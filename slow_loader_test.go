package redcache_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

// A loader slower than the lock TTL must keep its lock: Get returns after one
// loader call instead of losing the lock and re-running the loader forever.

func openShortLock(t *testing.T) (*redcache.Cache, *redcache.Conn) {
	t.Helper()
	skipIfNoRedis(t)
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: addr},
		redcache.WithLockTTL(200*time.Millisecond),
	)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return redcache.New(conn, redcache.StringCodec{}), conn
}

func TestGet_LoaderSlowerThanLockTTL(t *testing.T) {
	t.Parallel()
	client, _ := openShortLock(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	key := "slow:" + uuid.New().String()

	var calls atomic.Int32
	got, err := client.Get(ctx, time.Minute, key, func(context.Context, string) (string, error) {
		calls.Add(1)
		time.Sleep(700 * time.Millisecond)
		return "v", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "v", got)
	assert.EqualValues(t, 1, calls.Load(), "loader must run exactly once")

	v, ok, err := client.Peek[string, string](ctx, time.Minute, key)
	require.NoError(t, err)
	assert.True(t, ok, "Peek must hit after Get")
	assert.Equal(t, "v", v)
}

func TestGetMulti_LoaderSlowerThanLockTTL(t *testing.T) {
	t.Parallel()
	client, _ := openShortLock(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	id := uuid.New().String()
	keys := []string{"slowm:a:" + id, "slowm:b:" + id}

	var calls atomic.Int32
	got, err := client.GetMulti(ctx, time.Minute, keys, func(_ context.Context, ks []string) (map[string]string, error) {
		calls.Add(1)
		time.Sleep(700 * time.Millisecond)
		out := make(map[string]string, len(ks))
		for _, k := range ks {
			out[k] = "v-" + k
		}
		return out, nil
	})
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.EqualValues(t, 1, calls.Load(), "loader must run exactly once")

	for _, k := range keys {
		v, ok, err := client.Peek[string, string](ctx, time.Minute, k)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "v-"+k, v)
	}
}

// A Del during the load takes the lock away: the loader re-runs once and the
// fresh value is returned.
func TestGet_DelMidLoadRerunsLoaderOnce(t *testing.T) {
	t.Parallel()
	client, _ := openShortLock(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	key := "delmid:" + uuid.New().String()

	var calls atomic.Int32
	got, err := client.Get(ctx, time.Minute, key, func(context.Context, string) (string, error) {
		n := calls.Add(1)
		if n == 1 {
			require.NoError(t, client.Del(ctx, key))
		}
		return "v" + strconv.Itoa(int(n)), nil
	})
	require.NoError(t, err)
	assert.Equal(t, "v2", got)
	assert.EqualValues(t, 2, calls.Load())
}

func TestGetMulti_DelMidLoadRerunsLoaderOnce(t *testing.T) {
	t.Parallel()
	client, _ := openShortLock(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	id := uuid.New().String()
	keys := []string{"delmidm:a:" + id, "delmidm:b:" + id}

	var calls atomic.Int32
	got, err := client.GetMulti(ctx, time.Minute, keys, func(_ context.Context, ks []string) (map[string]string, error) {
		n := calls.Add(1)
		if n == 1 {
			require.NoError(t, client.Del(ctx, keys[0]))
		}
		out := make(map[string]string, len(ks))
		for _, k := range ks {
			out[k] = "v" + strconv.Itoa(int(n))
		}
		return out, nil
	})
	require.NoError(t, err)
	assert.Equal(t, "v2", got[keys[0]])
	assert.Len(t, got, 2)
	assert.EqualValues(t, 2, calls.Load())
}

// A lock lost on every attempt returns ErrLockLost within the retry cap.
func TestGet_RepeatedLockLossReturnsErrLockLost(t *testing.T) {
	t.Parallel()
	client, _ := openShortLock(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	key := "lost:" + uuid.New().String()

	var calls atomic.Int32
	_, err := client.Get(ctx, time.Minute, key, func(context.Context, string) (string, error) {
		calls.Add(1)
		require.NoError(t, client.Del(ctx, key))
		return "v", nil
	})
	require.ErrorIs(t, err, redcache.ErrLockLost)
	assert.LessOrEqual(t, calls.Load(), int32(10), "loader calls must be bounded")
	require.NoError(t, ctx.Err(), "must return before the context deadline")
}

func TestGetMulti_RepeatedLockLossReturnsErrLockLost(t *testing.T) {
	t.Parallel()
	client, _ := openShortLock(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	key := "lostm:" + uuid.New().String()

	var calls atomic.Int32
	_, err := client.GetMulti(ctx, time.Minute, []string{key}, func(_ context.Context, ks []string) (map[string]string, error) {
		calls.Add(1)
		require.NoError(t, client.Del(ctx, key))
		return map[string]string{key: "v"}, nil
	})
	require.ErrorIs(t, err, redcache.ErrLockLost)
	assert.LessOrEqual(t, calls.Load(), int32(10))
	require.NoError(t, ctx.Err())
}
