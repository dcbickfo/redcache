package redcache_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

// clusterKeyCount spreads keys over every primary: 64 random keys miss one of
// three primaries with probability 3*(2/3)^64, about 2e-11.
const clusterKeyCount = 64

// makeClusterClient opens a Conn against the Redis Cluster named by
// REDCACHE_CLUSTER_ADDRS (comma-separated, e.g. 127.0.0.1:30001). It skips
// when unset, except under CI, where a missing cluster is a setup failure.
func makeClusterClient(t *testing.T) (*redcache.Cache, *redcache.Conn) {
	t.Helper()
	skipIfNoRedis(t)
	addrs := os.Getenv("REDCACHE_CLUSTER_ADDRS")
	if addrs == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("REDCACHE_CLUSTER_ADDRS must be set in CI")
		}
		t.Skip("REDCACHE_CLUSTER_ADDRS not set; run `docker compose up -d --wait redis-cluster`")
	}
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: strings.Split(addrs, ",")},
		redcache.WithLockTTL(3*time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return redcache.New(conn, redcache.StringCodec{}), conn
}

// clusterKeys returns keys spread across slots, plus two that share a hash tag
// so one slot carries more than one key.
func clusterKeys(t *testing.T) []string {
	t.Helper()
	tag := "{" + uuid.New().String() + "}"
	keys := []string{"cluster:" + tag + ":a", "cluster:" + tag + ":b"}
	for range clusterKeyCount {
		keys = append(keys, "cluster:"+uuid.New().String())
	}
	return keys
}

// requireSpansPrimaries fails unless keys map to at least three primaries, so
// a test can't pass by landing every key on one node.
func requireSpansPrimaries(t *testing.T, conn *redcache.Conn, keys []string) {
	t.Helper()
	client := conn.Client()
	ctx := t.Context()
	slots, err := client.Do(ctx, client.B().ClusterSlots().Build()).ToArray()
	require.NoError(t, err)
	owner := func(slot int64) string {
		for _, s := range slots {
			r, err := s.ToArray()
			require.NoError(t, err)
			lo, _ := r[0].AsInt64()
			hi, _ := r[1].AsInt64()
			if slot >= lo && slot <= hi {
				node, err := r[2].ToArray()
				require.NoError(t, err)
				port, _ := node[1].AsInt64()
				return fmt.Sprint(port)
			}
		}
		t.Fatalf("slot %d has no owner", slot)
		return ""
	}
	primaries := make(map[string]struct{})
	for _, k := range keys {
		slot, err := client.Do(ctx, client.B().ClusterKeyslot().Key(k).Build()).AsInt64()
		require.NoError(t, err)
		primaries[owner(slot)] = struct{}{}
	}
	require.GreaterOrEqual(t, len(primaries), 3, "keys must span every primary")
}

func valuesFor(keys []string, suffix string) map[string]string {
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		out[k] = "val:" + suffix + ":" + k
	}
	return out
}

func TestCluster_GetMultiAcrossSlots(t *testing.T) {
	t.Parallel()
	client, conn := makeClusterClient(t)
	ctx := t.Context()
	keys := clusterKeys(t)
	requireSpansPrimaries(t, conn, keys)
	want := valuesFor(keys, "load")

	var calls atomic.Int32
	loader := func(_ context.Context, ks []string) (map[string]string, error) {
		calls.Add(1)
		out := make(map[string]string, len(ks))
		for _, k := range ks {
			out[k] = want[k]
		}
		return out, nil
	}

	got, err := client.GetMulti(ctx, time.Minute, keys, loader)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, int32(1), calls.Load())

	got, err = client.GetMulti(ctx, time.Minute, keys, loader)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, int32(1), calls.Load(), "second GetMulti must hit")

	for _, k := range keys {
		v, err := conn.Client().Do(ctx, conn.Client().B().Get().Key(k).Build()).ToString()
		require.NoError(t, err)
		// Stored values carry an envelope header ahead of the payload.
		assert.True(t, strings.HasSuffix(v, ":"+want[k]), "Redis value for %s: %q", k, v)
	}
}

func TestCluster_GetMultiConcurrentLoadsEachKeyOnce(t *testing.T) {
	t.Parallel()
	client, conn := makeClusterClient(t)
	ctx := t.Context()
	keys := clusterKeys(t)
	requireSpansPrimaries(t, conn, keys)
	want := valuesFor(keys, "load")

	var mu sync.Mutex
	loads := make(map[string]int)
	loader := func(_ context.Context, ks []string) (map[string]string, error) {
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		out := make(map[string]string, len(ks))
		for _, k := range ks {
			loads[k]++
			out[k] = want[k]
		}
		return out, nil
	}

	const callers = 8
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := range callers {
		wg.Go(func() {
			got, err := client.GetMulti(ctx, time.Minute, keys, loader)
			if err == nil && !maps.Equal(want, got) {
				err = errors.New("value mismatch")
			}
			errs[i] = err
		})
	}
	start := time.Now()
	wg.Wait()
	require.NoError(t, errors.Join(errs...))
	// Waiters wake on invalidation from every primary, not on the 3s lock TTL.
	assert.Less(t, time.Since(start), 2*time.Second)
	for _, k := range keys {
		assert.Equal(t, 1, loads[k], "loads for %s", k)
	}
}

func TestCluster_GetMultiLoaderErrorReleasesLocks(t *testing.T) {
	t.Parallel()
	client, conn := makeClusterClient(t)
	ctx := t.Context()
	keys := clusterKeys(t)
	requireSpansPrimaries(t, conn, keys)
	errLoad := errors.New("load failed")

	_, err := client.GetMulti(ctx, time.Minute, keys,
		func(context.Context, []string) (map[string]string, error) { return nil, errLoad })
	require.ErrorIs(t, err, errLoad)

	for _, k := range keys {
		n, err := conn.Client().Do(ctx, conn.Client().B().Exists().Key(k).Build()).AsInt64()
		require.NoError(t, err)
		assert.Zero(t, n, "lock for %s must be released", k)
	}

	want := valuesFor(keys, "retry")
	start := time.Now()
	got, err := client.GetMulti(ctx, time.Minute, keys,
		func(_ context.Context, ks []string) (map[string]string, error) {
			out := make(map[string]string, len(ks))
			for _, k := range ks {
				out[k] = want[k]
			}
			return out, nil
		})
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Less(t, time.Since(start), time.Second, "retry must not wait out the lock TTL")
}

func TestCluster_SetMultiAndForceSetMultiAcrossSlots(t *testing.T) {
	t.Parallel()
	client, conn := makeClusterClient(t)
	ctx := t.Context()
	keys := clusterKeys(t)
	requireSpansPrimaries(t, conn, keys)
	noLoad := func(context.Context, []string) (map[string]string, error) {
		return nil, errors.New("loader must not run")
	}

	set := valuesFor(keys, "set")
	require.NoError(t, client.SetMulti(ctx, time.Minute, keys,
		func(_ context.Context, ks []string) (map[string]string, error) {
			out := make(map[string]string, len(ks))
			for _, k := range ks {
				out[k] = set[k]
			}
			return out, nil
		}))
	got, err := client.GetMulti(ctx, time.Minute, keys, noLoad)
	require.NoError(t, err)
	assert.Equal(t, set, got)

	forced := valuesFor(keys, "force")
	require.NoError(t, client.ForceSetMulti(ctx, time.Minute, forced))
	got, err = client.GetMulti(ctx, time.Minute, keys, noLoad)
	require.NoError(t, err)
	assert.Equal(t, forced, got)

	// A failed SetMulti callback restores every prior value on every primary.
	errSet := errors.New("set failed")
	err = client.SetMulti(ctx, time.Minute, keys,
		func(context.Context, []string) (map[string]string, error) { return nil, errSet })
	require.ErrorIs(t, err, errSet)
	got, err = client.GetMulti(ctx, time.Minute, keys, noLoad)
	require.NoError(t, err)
	assert.Equal(t, forced, got)
}

func TestCluster_TouchMultiAndDelMultiAcrossSlots(t *testing.T) {
	t.Parallel()
	client, conn := makeClusterClient(t)
	ctx := t.Context()
	keys := clusterKeys(t)
	requireSpansPrimaries(t, conn, keys)
	rc := conn.Client()

	require.NoError(t, client.ForceSetMulti(ctx, 10*time.Second, valuesFor(keys, "touch")))
	require.NoError(t, client.TouchMulti(ctx, time.Hour, keys))
	for _, k := range keys {
		pttl, err := rc.Do(ctx, rc.B().Pttl().Key(k).Build()).AsInt64()
		require.NoError(t, err)
		assert.Greater(t, pttl, int64(time.Minute/time.Millisecond), "TTL for %s", k)
	}

	require.NoError(t, client.DelMulti(ctx, keys))
	for _, k := range keys {
		n, err := rc.Do(ctx, rc.B().Exists().Key(k).Build()).AsInt64()
		require.NoError(t, err)
		assert.Zero(t, n, "%s must be deleted", k)
	}
}

func TestCluster_ScriptFlushFallsBackToEval(t *testing.T) {
	// Not parallel: SCRIPT FLUSH clears every primary's script cache.
	client, conn := makeClusterClient(t)
	ctx := t.Context()
	keys := clusterKeys(t)
	requireSpansPrimaries(t, conn, keys)

	for _, node := range conn.Client().Nodes() {
		require.NoError(t, node.Do(ctx, node.B().ScriptFlush().Build()).Error())
	}

	want := valuesFor(keys, "flushed")
	got, err := client.GetMulti(ctx, time.Minute, keys,
		func(_ context.Context, ks []string) (map[string]string, error) {
			out := make(map[string]string, len(ks))
			for _, k := range ks {
				out[k] = want[k]
			}
			return out, nil
		})
	require.NoError(t, err)
	assert.Equal(t, want, got)
}
