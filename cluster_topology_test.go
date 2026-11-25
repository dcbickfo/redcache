package redcache_test

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

// These tests change the cluster's topology, so none of them is parallel: Go
// runs every sequential top-level test before resuming the parallel ones, so
// the other cluster tests never see a half-moved slot or a failover. Each test
// restores the original layout in t.Cleanup.

// clusterNode is one line of CLUSTER NODES.
type clusterNode struct {
	id, addr, primaryID string
	primary             bool
}

// adminClient opens a plain, uncached connection to one cluster node for
// commands that must reach that node rather than be routed by slot.
func adminClient(t *testing.T, addr string) rueidis.Client {
	t.Helper()
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:       []string{addr},
		ForceSingleClient: true,
		DisableCache:      true,
	})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

func do(t *testing.T, c rueidis.Client, args ...string) rueidis.RedisResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.Do(ctx, c.B().Arbitrary(args...).Build())
}

// topologyTest opens a cluster client, fails fast unless the cluster is
// healthy with three primaries, and bounds the test's Redis calls so a broken
// topology fails the test instead of hanging it.
func topologyTest(t *testing.T, opts ...redcache.Option) (context.Context, *redcache.Cache, *redcache.Conn, []clusterNode) {
	t.Helper()
	client, conn := makeClusterClient(t, opts...)
	// A failover test's cleanup can leave the view settling for a few seconds.
	eventuallyWithStatus(t, 20*time.Second, "30001 to report cluster_state:ok (reset it with `docker compose up -d --force-recreate --wait redis-cluster`)",
		func() bool { return clusterStateOK("127.0.0.1:30001") },
		func() string { return clusterInfo(t, "127.0.0.1:30001") })
	nodes := clusterNodes(t, "127.0.0.1:30001")
	var primaries int
	for _, n := range nodes {
		if n.primary {
			primaries++
		}
	}
	require.Equal(t, 3, primaries, "cluster must have three primaries; reset it with `docker compose up -d --force-recreate --wait redis-cluster`")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx, client, conn, nodes
}

// clusterInfo returns node addr's CLUSTER INFO text, or the error as text.
func clusterInfo(t *testing.T, addr string) string {
	t.Helper()
	info, err := do(t, adminClient(t, addr), "CLUSTER", "INFO").ToString()
	if err != nil {
		return "CLUSTER INFO error: " + err.Error()
	}
	return info
}

func clusterStateOK(addr string) bool {
	c, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, ForceSingleClient: true, DisableCache: true})
	if err != nil {
		return false
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, err := c.Do(ctx, c.B().Arbitrary("CLUSTER", "INFO").Build()).ToString()
	return err == nil && strings.Contains(info, "cluster_state:ok")
}

// clusterNodes reads the topology as seen by node addr.
func clusterNodes(t *testing.T, addr string) []clusterNode {
	t.Helper()
	out, err := do(t, adminClient(t, addr), "CLUSTER", "NODES").ToString()
	require.NoError(t, err)
	// RESP3 sends CLUSTER NODES as a verbatim string, which keeps its "txt:"
	// format prefix.
	out = strings.TrimPrefix(out, "txt:")
	var nodes []clusterNode
	for line := range strings.Lines(out) {
		f := strings.Fields(line)
		if len(f) < 8 || strings.Contains(f[2], "fail") {
			continue
		}
		n := clusterNode{id: f[0], addr: strings.Split(f[1], "@")[0], primaryID: f[3]}
		n.primary = strings.Contains(f[2], "master")
		nodes = append(nodes, n)
	}
	return nodes
}

func keySlot(t *testing.T, conn *redcache.Conn, key string) int {
	t.Helper()
	c := conn.Client()
	slot, err := c.Do(t.Context(), c.B().ClusterKeyslot().Key(key).Build()).AsInt64()
	require.NoError(t, err)
	return int(slot)
}

// slotOwner returns the primary serving slot, by asking CLUSTER SLOTS.
func slotOwner(t *testing.T, nodes []clusterNode, slot int) clusterNode {
	t.Helper()
	slots, err := do(t, adminClient(t, nodes[0].addr), "CLUSTER", "SLOTS").ToArray()
	require.NoError(t, err)
	for _, s := range slots {
		r, err := s.ToArray()
		require.NoError(t, err)
		lo, _ := r[0].AsInt64()
		hi, _ := r[1].AsInt64()
		if int64(slot) < lo || int64(slot) > hi {
			continue
		}
		owner, err := r[2].ToArray()
		require.NoError(t, err)
		id, err := owner[2].ToString()
		require.NoError(t, err)
		for _, n := range nodes {
			if n.id == id {
				return n
			}
		}
	}
	t.Fatalf("slot %d has no owner", slot)
	return clusterNode{}
}

func otherPrimary(t *testing.T, nodes []clusterNode, not clusterNode) clusterNode {
	t.Helper()
	for _, n := range nodes {
		if n.primary && n.id != not.id {
			return n
		}
	}
	t.Fatal("no second primary")
	return clusterNode{}
}

func port(addr string) string { return addr[strings.LastIndex(addr, ":")+1:] }

// moveSlotKeys MIGRATEs every key in slot from one node to another.
func moveSlotKeys(t *testing.T, slot int, from, to clusterNode) {
	t.Helper()
	src := adminClient(t, from.addr)
	for {
		keys, err := do(t, src, "CLUSTER", "GETKEYSINSLOT", strconv.Itoa(slot), "100").AsStrSlice()
		require.NoError(t, err)
		if len(keys) == 0 {
			return
		}
		// Nodes share the container, so 127.0.0.1 reaches the destination.
		args := append([]string{"MIGRATE", "127.0.0.1", port(to.addr), "", "0", "5000", "KEYS"}, keys...)
		require.NoError(t, do(t, src, args...).Error())
	}
}

// beginSlotMigration marks slot as IMPORTING on to and MIGRATING on from.
func beginSlotMigration(t *testing.T, slot int, from, to clusterNode) {
	t.Helper()
	s := strconv.Itoa(slot)
	require.NoError(t, do(t, adminClient(t, to.addr), "CLUSTER", "SETSLOT", s, "IMPORTING", from.id).Error())
	require.NoError(t, do(t, adminClient(t, from.addr), "CLUSTER", "SETSLOT", s, "MIGRATING", to.id).Error())
}

// finishSlotMigration moves the remaining keys and assigns slot to on every
// primary.
func finishSlotMigration(t *testing.T, nodes []clusterNode, slot int, from, to clusterNode) {
	t.Helper()
	moveSlotKeys(t, slot, from, to)
	for _, n := range nodes {
		if n.primary {
			require.NoError(t, do(t, adminClient(t, n.addr), "CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", to.id).Error())
		}
	}
}

func migrateSlot(t *testing.T, nodes []clusterNode, slot int, from, to clusterNode) {
	t.Helper()
	beginSlotMigration(t, slot, from, to)
	finishSlotMigration(t, nodes, slot, from, to)
}

func flushScripts(t *testing.T, addr string) {
	t.Helper()
	require.NoError(t, do(t, adminClient(t, addr), "SCRIPT", "FLUSH").Error())
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	eventuallyWithStatus(t, timeout, what, cond, nil)
}

// eventuallyWithStatus is eventually that appends status() to the timeout
// message, so a failure names the last state observed.
func eventuallyWithStatus(t *testing.T, timeout time.Duration, what string, cond func() bool, status func() string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			if status != nil {
				t.Fatalf("timed out after %s waiting for %s; last seen: %s", timeout, what, status())
			}
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func role(t *testing.T, addr string) string {
	t.Helper()
	r, err := do(t, adminClient(t, addr), "ROLE").ToArray()
	if err != nil {
		return ""
	}
	s, _ := r[0].ToString()
	return s
}

// replicaSynced reports whether addr is a replica whose link to its primary
// is up. ROLE says "slave" before the link is up, and a CLUSTER FAILOVER
// issued then aborts silently.
func replicaSynced(t *testing.T, addr string) bool {
	t.Helper()
	synced, _ := replicaStatus(t, addr)
	return synced
}

// replicaStatus returns replicaSynced's verdict and a description of the ROLE
// reply (role and link state) for failure messages.
func replicaStatus(t *testing.T, addr string) (bool, string) {
	t.Helper()
	r, err := do(t, adminClient(t, addr), "ROLE").ToArray()
	if err != nil {
		return false, "ROLE error: " + err.Error()
	}
	if len(r) < 4 {
		s, _ := r[0].ToString()
		return false, "role " + s
	}
	s, _ := r[0].ToString()
	state, _ := r[3].ToString()
	return s == "slave" && state == "connected", "role " + s + ", link " + state
}

func taggedKeys(tag string, n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("topo:{%s}:%d", tag, i)
	}
	return keys
}

func exactLoader(want map[string]string) func(context.Context, []string) (map[string]string, error) {
	return func(_ context.Context, ks []string) (map[string]string, error) {
		out := make(map[string]string, len(ks))
		for _, k := range ks {
			out[k] = want[k]
		}
		return out, nil
	}
}

func noLoader(context.Context, []string) (map[string]string, error) {
	return nil, errors.New("loader must not run")
}

// TestCluster_SlotMigrationFollowsMOVED moves a populated slot to another
// primary, whose script cache is empty, and checks reads, writes and deletes
// follow the MOVED redirect.
func TestCluster_SlotMigrationFollowsMOVED(t *testing.T) {
	ctx, client, conn, nodes := topologyTest(t)
	keys := taggedKeys(uuid.New().String(), 4)
	slot := keySlot(t, conn, keys[0])
	from := slotOwner(t, nodes, slot)
	to := otherPrimary(t, nodes, from)

	want := valuesFor(keys, "before")
	got, err := client.GetMulti(ctx, time.Minute, keys, exactLoader(want))
	require.NoError(t, err)
	require.Equal(t, want, got)

	migrateSlot(t, nodes, slot, from, to)
	t.Cleanup(func() { migrateSlot(t, nodes, slot, to, from) })
	flushScripts(t, to.addr)
	require.Equal(t, to.id, slotOwner(t, nodes, slot).id)

	got, err = client.GetMulti(ctx, time.Minute, keys, noLoader)
	require.NoError(t, err)
	assert.Equal(t, want, got, "values must survive the move")

	after := valuesFor(keys, "after")
	require.NoError(t, client.SetMulti(ctx, time.Minute, keys, exactLoader(after)))
	got, err = client.GetMulti(ctx, time.Minute, keys, noLoader)
	require.NoError(t, err)
	assert.Equal(t, after, got)
	v, err := do(t, adminClient(t, to.addr), "GET", keys[0]).ToString()
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(v, ":"+after[keys[0]]), "the write must land on the new owner: %q", v)

	require.NoError(t, client.TouchMulti(ctx, time.Hour, keys))
	require.NoError(t, client.DelMulti(ctx, keys))
	n, err := do(t, adminClient(t, to.addr), "EXISTS", keys[0]).AsInt64()
	require.NoError(t, err)
	assert.Zero(t, n)
}

// TestCluster_SlotMigrationFollowsASK leaves a slot half-migrated: keys that
// still exist on the source are served there, and new keys are redirected to
// the target with ASK. The target's script cache is empty.
func TestCluster_SlotMigrationFollowsASK(t *testing.T) {
	metrics := &capturingMetrics{}
	ctx, client, conn, nodes := topologyTest(t, redcache.WithMetrics(metrics))
	tag := uuid.New().String()
	keys := taggedKeys(tag, 3)
	old, fresh := keys[0], keys[1:]
	slot := keySlot(t, conn, old)
	from := slotOwner(t, nodes, slot)
	to := otherPrimary(t, nodes, from)
	server, err := do(t, adminClient(t, from.addr), "INFO", "server").ToString()
	require.NoError(t, err)
	valkey9 := strings.Contains(server, "\nvalkey_version:9.")
	readErrorOps := func() []string {
		metrics.mu.Lock()
		defer metrics.mu.Unlock()
		return slices.Clone(metrics.redisErrOps)
	}
	unexpectedLoader := func(context.Context, string) (string, error) {
		return "", errors.New("loader must not run on a hit")
	}

	want := valuesFor(keys, "load")
	_, err = client.GetMulti(ctx, time.Minute, []string{old}, exactLoader(want))
	require.NoError(t, err)
	v, err := client.Get(ctx, time.Minute, old, unexpectedLoader)
	require.NoError(t, err)
	require.Equal(t, want[old], v)
	require.Empty(t, readErrorOps(), "normal reads must not emit read_fallback")

	beginSlotMigration(t, slot, from, to)
	t.Cleanup(func() {
		finishSlotMigration(t, nodes, slot, from, to)
		migrateSlot(t, nodes, slot, to, from)
	})
	flushScripts(t, to.addr)

	var loaded []string
	got, err := client.GetMulti(ctx, time.Minute, keys, func(c context.Context, ks []string) (map[string]string, error) {
		loaded = append(loaded, ks...)
		return exactLoader(want)(c, ks)
	})
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.ElementsMatch(t, fresh, loaded, "only the new keys are loaded")

	if valkey9 {
		before := len(readErrorOps())
		v, err := client.Get(ctx, time.Minute, fresh[0], unexpectedLoader)
		require.NoError(t, err)
		require.Equal(t, want[fresh[0]], v)
		assert.Equal(t, []string{"read_fallback"}, readErrorOps()[before:], "one event for a plain GET")
	}
	beforeBatch := len(readErrorOps())
	got, err = client.GetMulti(ctx, time.Minute, keys, noLoader)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	if valkey9 {
		assert.Equal(t, []string{"read_fallback"}, readErrorOps()[beforeBatch:], "one event for the batch of two plain GETs")
	}

	// New keys live on the importing node, which serves them only after ASKING.
	dst := adminClient(t, to.addr)
	resps := dst.DoMulti(ctx, dst.B().Arbitrary("ASKING").Build(), dst.B().Get().Key(fresh[0]).Build())
	v, err = resps[1].ToString()
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(v, ":"+want[fresh[0]]), "new key must be on the target: %q", v)

	after := valuesFor(keys, "after")
	require.NoError(t, client.SetMulti(ctx, time.Minute, keys, exactLoader(after)))
	got, err = client.GetMulti(ctx, time.Minute, keys, noLoader)
	require.NoError(t, err)
	assert.Equal(t, after, got)
	require.NoError(t, client.DelMulti(ctx, keys))
}

func askRefillTest(t *testing.T, keyCount int, opts ...redcache.Option) (context.Context, *redcache.Cache, *redcache.Cache, []string) {
	t.Helper()
	ctx, reader, conn, nodes := topologyTest(t, opts...)
	writer, _ := makeClusterClient(t)
	keys := taggedKeys(uuid.New().String(), keyCount)
	slot := keySlot(t, conn, keys[0])
	from := slotOwner(t, nodes, slot)
	to := otherPrimary(t, nodes, from)

	beginSlotMigration(t, slot, from, to)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		err := writer.DelMulti(cleanupCtx, keys)
		s := strconv.Itoa(slot)
		for _, node := range []clusterNode{from, to} {
			err = errors.Join(err, do(t, adminClient(t, node.addr), "CLUSTER", "SETSLOT", s, "STABLE").Error())
		}
		require.NoError(t, err)
	})
	return ctx, reader, writer, keys
}

func TestCluster_GetRefillsAfterASKHit(t *testing.T) {
	ctx, reader, writer, keys := askRefillTest(t, 1)
	key := keys[0]
	require.NoError(t, writer.ForceSet(ctx, time.Minute, key, "seed"))
	got, err := reader.Get(ctx, time.Minute, key, func(context.Context, string) (string, error) {
		return "", errors.New("loader must not run on a hit")
	})
	require.NoError(t, err)
	require.Equal(t, "seed", got)
	for round := 1; round <= 2; round++ {
		require.NoError(t, writer.Del(ctx, key))
		want := fmt.Sprintf("refill-%d", round)
		refillCtx, cancel := context.WithTimeout(ctx, time.Second)
		got, err := reader.Get(refillCtx, time.Minute, key, func(context.Context, string) (string, error) {
			return want, nil
		})
		cancel()
		require.NoError(t, err, "Get refill %d", round)
		require.Equal(t, want, got)

		stored, present, err := writer.Peek[string, string](ctx, time.Minute, key)
		require.NoError(t, err)
		require.True(t, present)
		require.Equal(t, want, stored)
	}
}

func TestCluster_GetMultiRefillsAfterASKHit(t *testing.T) {
	ctx, reader, writer, keys := askRefillTest(t, 2)
	seed := valuesFor(keys, "seed")
	require.NoError(t, writer.ForceSetMulti(ctx, time.Minute, seed))
	got, err := reader.GetMulti(ctx, time.Minute, keys, noLoader)
	require.NoError(t, err)
	require.Equal(t, seed, got)
	requireASKMultiRefills(t, ctx, reader, writer, keys)
}

func requireASKMultiRefills(t *testing.T, ctx context.Context, reader, writer *redcache.Cache, keys []string) {
	t.Helper()
	for round := 1; round <= 2; round++ {
		require.NoError(t, writer.DelMulti(ctx, keys))
		want := valuesFor(keys, fmt.Sprintf("refill-%d", round))
		refillCtx, cancel := context.WithTimeout(ctx, time.Second)
		got, err := reader.GetMulti(refillCtx, time.Minute, keys, exactLoader(want))
		cancel()
		require.NoError(t, err, "GetMulti refill %d", round)
		require.Equal(t, want, got)

		stored, err := writer.GetMulti(ctx, time.Minute, keys, noLoader)
		require.NoError(t, err)
		require.Equal(t, want, stored)
	}
}

func TestCluster_RefillsAfterASKPublication(t *testing.T) {
	for _, operation := range []string{"Get", "GetMulti", "Set", "SetMulti"} {
		t.Run(operation, func(t *testing.T) {
			keyCount := 2
			if operation == "Get" || operation == "Set" {
				keyCount = 1
			}
			ctx, reader, writer, keys := askRefillTest(t, keyCount)
			seed := valuesFor(keys, "seed")
			single := func(_ context.Context, key string) (string, error) { return seed[key], nil }
			switch operation {
			case "Get":
				_, err := reader.Get(ctx, time.Minute, keys[0], single)
				require.NoError(t, err)
			case "GetMulti":
				_, err := reader.GetMulti(ctx, time.Minute, keys, exactLoader(seed))
				require.NoError(t, err)
			case "Set":
				require.NoError(t, reader.Set(ctx, time.Minute, keys[0], single))
			case "SetMulti":
				require.NoError(t, reader.SetMulti(ctx, time.Minute, keys, exactLoader(seed)))
			}
			stored, err := writer.GetMulti(ctx, time.Minute, keys, noLoader)
			require.NoError(t, err)
			require.Equal(t, seed, stored)
			requireASKMultiRefills(t, ctx, reader, writer, keys)
		})
	}
}

func TestCluster_RefillsAfterCallbackPanic(t *testing.T) {
	const panicValue = "callback panic"
	panicSingle := func(context.Context, string) (string, error) { panic(panicValue) }
	panicMulti := func(context.Context, []string) (map[string]string, error) { panic(panicValue) }
	for _, operation := range []string{"Get", "GetMulti", "Set", "SetMulti"} {
		t.Run(operation, func(t *testing.T) {
			multi := operation == "GetMulti" || operation == "SetMulti"
			keyCount := 1
			if multi {
				keyCount = 2
			}
			ctx, reader, _, keys := askRefillTest(t, keyCount, redcache.WithLockTTL(30*time.Second))
			_, conn := makeClusterClient(t)
			require.PanicsWithValue(t, panicValue, func() {
				switch operation {
				case "Get":
					_, _ = reader.Get(ctx, time.Minute, keys[0], panicSingle)
				case "GetMulti":
					_, _ = reader.GetMulti(ctx, time.Minute, keys, panicMulti)
				case "Set":
					_ = reader.Set(ctx, time.Minute, keys[0], panicSingle)
				case "SetMulti":
					_ = reader.SetMulti(ctx, time.Minute, keys, panicMulti)
				}
			})
			client := conn.Client()
			for _, key := range keys {
				exists, err := client.Do(ctx, client.B().Exists().Key(key).Build()).AsInt64()
				require.NoError(t, err)
				require.Zero(t, exists, "the panic must roll back the Redis lock")
			}

			refillCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			if multi {
				want := valuesFor(keys, "refill")
				got, err := reader.GetMulti(refillCtx, time.Minute, keys, exactLoader(want))
				require.NoError(t, err, "GetMulti must refill on the same connection after the panic")
				require.Equal(t, want, got)
				return
			}
			got, err := reader.Get(refillCtx, time.Minute, keys[0], func(context.Context, string) (string, error) {
				return "refilled", nil
			})
			require.NoError(t, err, "Get must refill on the same connection after the panic")
			require.Equal(t, "refilled", got)
		})
	}
}

type stampedeMetrics struct {
	redcache.NoopMetrics
	waiting chan struct{}
}

func (m stampedeMetrics) LockContended(int64) {
	select {
	case m.waiting <- struct{}{}:
	default:
	}
}

func TestCluster_GetStampedeAfterASK(t *testing.T) {
	for _, primer := range []string{"hit", "miss"} {
		t.Run(primer, func(t *testing.T) {
			metrics := stampedeMetrics{waiting: make(chan struct{}, 1)}
			ctx, reader, writer, keys := askRefillTest(t, 1,
				redcache.WithLockTTL(30*time.Second), redcache.WithMetrics(metrics))
			key := keys[0]
			if primer == "hit" {
				require.NoError(t, writer.ForceSet(ctx, time.Minute, key, "seed"))
			}
			got, err := reader.Get(ctx, time.Minute, key, func(context.Context, string) (string, error) {
				if primer == "hit" {
					return "", errors.New("loader must not run on a hit")
				}
				return "seed", nil
			})
			require.NoError(t, err)
			require.Equal(t, "seed", got)
			require.NoError(t, writer.Del(ctx, key))

			const callers = 16
			callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			deadline, _ := callCtx.Deadline()
			loaderStarted := make(chan struct{})
			releaseLoader := make(chan struct{})
			type result struct {
				value    string
				err      error
				finished time.Time
			}
			results := make(chan result, callers)
			var loads atomic.Int32
			var workers sync.WaitGroup
			startCaller := func() {
				workers.Go(func() {
					value, err := reader.Get(callCtx, time.Minute, key, func(loaderCtx context.Context, _ string) (string, error) {
						if loads.Add(1) == 1 {
							close(loaderStarted)
						}
						select {
						case <-releaseLoader:
							return "refilled", nil
						case <-loaderCtx.Done():
							return "", loaderCtx.Err()
						}
					})
					results <- result{value, err, time.Now()}
				})
			}
			startCaller()
			select {
			case <-loaderStarted:
			case <-callCtx.Done():
				workers.Wait()
				t.Fatalf("loader did not start before the caller deadline: %v", callCtx.Err())
			}
			// Count contention only after the leader enters its loader.
			select {
			case <-metrics.waiting:
			default:
			}
			for range callers - 1 {
				startCaller()
			}
			select {
			case <-metrics.waiting:
				close(releaseLoader)
			case <-callCtx.Done():
				workers.Wait()
				t.Fatalf("no follower waited before the caller deadline: %v", callCtx.Err())
			}
			workers.Wait()
			for range callers {
				got := <-results
				require.NoError(t, got.err)
				require.True(t, got.finished.Before(deadline), "caller returned after its deadline")
				require.Equal(t, "refilled", got.value)
			}
			t.Logf("Loader count for %d concurrent callers: %d", callers, loads.Load())
			require.EqualValues(t, 1, loads.Load())
		})
	}
}

func retryNoSlot(ctx context.Context, read func() error) (int, error) {
	for retries := 0; ; retries++ {
		err := read()
		if !errors.Is(err, rueidis.ErrNoSlot) {
			return retries, err
		}
		select {
		case <-ctx.Done():
			return retries, err
		case <-time.After(100 * time.Millisecond):
			if ctx.Err() != nil {
				return retries, err
			}
		}
	}
}

func TestRetryNoSlot(t *testing.T) {
	t.Run("wrapped errors then success", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			const wantRetries = 3
			calls := 0
			retries, err := retryNoSlot(t.Context(), func() error {
				calls++
				if calls <= wantRetries {
					return fmt.Errorf("lock key: %w", rueidis.ErrNoSlot)
				}
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, wantRetries, retries)
			require.Equal(t, wantRetries+1, calls)
		})
	})
	t.Run("different error with same text", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			other := errors.New(rueidis.ErrNoSlot.Error())
			calls := 0
			retries, err := retryNoSlot(ctx, func() error {
				calls++
				return other
			})
			require.Same(t, other, err)
			require.Zero(t, retries)
			require.Equal(t, 1, calls)
		})
	})
	t.Run("deadline preserves latest no-slot error", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
			defer cancel()
			var lastErr error
			calls := 0
			retries, err := retryNoSlot(ctx, func() error {
				calls++
				lastErr = fmt.Errorf("attempt %d: %w", calls, rueidis.ErrNoSlot)
				return lastErr
			})
			require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
			require.Same(t, lastErr, err)
			require.ErrorIs(t, err, rueidis.ErrNoSlot)
			require.Equal(t, 2, retries)
			require.Equal(t, 3, calls)
		})
	})
}

// TestCluster_PrimaryFailover freezes a primary with DEBUG SLEEP until its
// replica takes over, then checks the multi-key paths against the promoted
// replica, whose script cache is empty. The frozen node rejoins as a replica
// and is failed back in cleanup.
func TestCluster_PrimaryFailover(t *testing.T) {
	ctx, client, conn, nodes := topologyTest(t)
	keys := taggedKeys(uuid.New().String(), 4)
	victim := slotOwner(t, nodes, keySlot(t, conn, keys[0]))
	var replica clusterNode
	for _, n := range nodes {
		if n.primaryID == victim.id {
			replica = n
		}
	}
	require.NotEmpty(t, replica.id, "victim %s has no replica", victim.addr)

	want := valuesFor(keys, "before")
	_, err := client.GetMulti(ctx, time.Minute, keys, exactLoader(want))
	require.NoError(t, err)
	// Make sure the replica has the values before the primary goes away.
	require.NoError(t, do(t, adminClient(t, victim.addr), "WAIT", "1", "2000").Error())

	frozen := adminClient(t, victim.addr)
	slept := make(chan error, 1)
	go func() {
		slept <- frozen.Do(context.Background(), frozen.B().Arbitrary("DEBUG", "SLEEP", "6").Build()).Error()
	}()
	t.Cleanup(func() {
		<-slept
		eventuallyWithStatus(t, 60*time.Second, victim.addr+" to sync as a replica",
			func() bool { return replicaSynced(t, victim.addr) },
			func() string { _, st := replicaStatus(t, victim.addr); return st })
		// A manual failover that cannot complete aborts after 5s without an
		// error, so reissue it until the promotion lands.
		var issued time.Time
		eventually(t, 30*time.Second, victim.addr+" to be primary again", func() bool {
			if role(t, victim.addr) == "master" {
				return true
			}
			if time.Since(issued) > 6*time.Second {
				require.NoError(t, do(t, adminClient(t, victim.addr), "CLUSTER", "FAILOVER").Error())
				issued = time.Now()
			}
			return false
		})
		// Later tests read the topology from 30001, so wait for its view too.
		eventually(t, 20*time.Second, "30001 to see the failback", func() bool {
			var victimPrimary, replicaPrimary bool
			for _, n := range clusterNodes(t, "127.0.0.1:30001") {
				switch n.id {
				case victim.id:
					victimPrimary = n.primary
				case replica.id:
					replicaPrimary = n.primary
				}
			}
			return victimPrimary && !replicaPrimary
		})
		// Leave every node reporting a healthy cluster, or fail here with the
		// state instead of poisoning the tests that share the cluster.
		for _, n := range nodes {
			eventuallyWithStatus(t, 30*time.Second, n.addr+" to report cluster_state:ok",
				func() bool { return clusterStateOK(n.addr) },
				func() string { return clusterInfo(t, n.addr) })
		}
	})
	eventually(t, 20*time.Second, replica.addr+" to be promoted", func() bool { return role(t, replica.addr) == "master" })

	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	start := time.Now()
	var got map[string]string
	// The role check can see promotion before rueidis's slot map includes the new primary.
	retries, err := retryNoSlot(opCtx, func() error {
		var err error
		got, err = client.GetMulti(opCtx, time.Minute, keys, noLoader)
		return err
	})
	t.Logf("first post-failover GetMulti took %s (%d retries)", time.Since(start), retries)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	after := valuesFor(keys, "after")
	require.NoError(t, client.SetMulti(opCtx, time.Minute, keys, exactLoader(after)))
	fresh := taggedKeys(uuid.New().String(), 2)
	freshWant := valuesFor(fresh, "load")
	// A broken cluster must fail in seconds, not block until opCtx expires.
	freshCtx, freshCancel := context.WithTimeout(opCtx, 10*time.Second)
	defer freshCancel()
	got, err = client.GetMulti(freshCtx, time.Minute, fresh, exactLoader(freshWant))
	require.NoError(t, err)
	assert.Equal(t, freshWant, got)
	require.NoError(t, client.DelMulti(opCtx, append(keys, fresh...)))
	t.Logf("multi-key ops after failover took %s", time.Since(start))
}

var cmdstatCalls = regexp.MustCompile(`(?m)^cmdstat_(script\|load|evalsha|eval):calls=(\d+)`)

func scriptCalls(t *testing.T, addr string) map[string]int {
	t.Helper()
	info, err := do(t, adminClient(t, addr), "INFO", "commandstats").ToString()
	require.NoError(t, err)
	out := make(map[string]int)
	for _, m := range cmdstatCalls.FindAllStringSubmatch(info, -1) {
		n, err := strconv.Atoi(m[2])
		require.NoError(t, err)
		out[m[1]] = n
	}
	return out
}

// TestCluster_NoScriptLoad checks the multi-key paths send no SCRIPT LOAD to
// any node, and no scripts at all to replicas. rueidis's Lua.ExecMulti sent
// one SCRIPT LOAD per call to every node, replicas included.
func TestCluster_NoScriptLoad(t *testing.T) {
	ctx, client, conn, nodes := topologyTest(t)
	keys := clusterKeys(t)
	requireSpansPrimaries(t, conn, keys)
	before := make(map[string]map[string]int, len(nodes))
	for _, n := range nodes {
		before[n.addr] = scriptCalls(t, n.addr)
	}

	want := valuesFor(keys, "load")
	_, err := client.GetMulti(ctx, time.Minute, keys, exactLoader(want))
	require.NoError(t, err)
	require.NoError(t, client.SetMulti(ctx, time.Minute, keys, exactLoader(valuesFor(keys, "set"))))
	require.NoError(t, client.TouchMulti(ctx, time.Hour, keys))

	var primaryScripts int
	for _, n := range nodes {
		after := scriptCalls(t, n.addr)
		assert.Equal(t, before[n.addr]["script|load"], after["script|load"], "SCRIPT LOAD on %s", n.addr)
		ran := after["evalsha"] + after["eval"] - before[n.addr]["evalsha"] - before[n.addr]["eval"]
		// ROLE is the node's own answer; CLUSTER NODES can lag a failover.
		if role(t, n.addr) == "master" {
			primaryScripts += ran
		} else {
			assert.Zero(t, ran, "scripts on replica %s", n.addr)
		}
	}
	assert.Positive(t, primaryScripts)
}
