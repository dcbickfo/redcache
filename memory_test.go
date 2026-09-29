package redcache_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

// memCache returns a JSON-backed cache over an in-memory Conn plus a settable
// clock. Tests that do not care about time ignore the clock.
func memCache(t *testing.T) (*redcache.Cache, *time.Time) {
	t.Helper()
	now := time.Now()
	conn := redcache.OpenMemory(redcache.WithMemoryClock(func() time.Time { return now }))
	return redcache.New(conn, redcache.JSONCodec{}), &now
}

func countingLoader[V any](v V) (func(context.Context, string) (V, error), *atomic.Int64) {
	var calls atomic.Int64
	return func(context.Context, string) (V, error) {
		calls.Add(1)
		return v, nil
	}, &calls
}

func TestMemory_ConnShape(t *testing.T) {
	t.Parallel()
	conn := redcache.OpenMemory()
	assert.Nil(t, conn.Client())
	conn.Close() // no-op, must not panic
	raw := redcache.NewBytes(conn)
	require.NoError(t, raw.ForceSet(t.Context(), time.Minute, "k", []byte("v")))
	got, ok, err := raw.Peek[string, []byte](t.Context(), time.Minute, "k")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, []byte("v"), got)
}

func TestMemory_Get_MissCallsFnOnceThenHits(t *testing.T) {
	t.Parallel()
	c, _ := memCache(t)
	fn, calls := countingLoader(42)
	for range 3 {
		got, err := c.Get(t.Context(), time.Minute, "k", fn)
		require.NoError(t, err)
		assert.Equal(t, 42, got)
	}
	assert.Equal(t, int64(1), calls.Load())
}

func TestMemory_Get_ZeroValueIsAHit(t *testing.T) {
	t.Parallel()
	c, _ := memCache(t)
	require.NoError(t, c.ForceSet(t.Context(), time.Minute, "k", 0))
	var ran atomic.Bool
	got, err := c.Get(t.Context(), time.Minute, "k", func(context.Context, string) (int, error) {
		ran.Store(true)
		return 1, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 0, got)
	assert.False(t, ran.Load(), "presence of a zero value is still a hit")
}

func TestMemory_Get_FnErrorNotStored(t *testing.T) {
	t.Parallel()
	c, _ := memCache(t)
	wantErr := errors.New("boom")
	_, err := c.Get(t.Context(), time.Minute, "k", func(context.Context, string) (int, error) { return 0, wantErr })
	require.ErrorIs(t, err, wantErr)
	fn, calls := countingLoader(5)
	got, err := c.Get(t.Context(), time.Minute, "k", fn)
	require.NoError(t, err)
	assert.Equal(t, 5, got)
	assert.Equal(t, int64(1), calls.Load())
}

func TestMemory_Get_CodecsRun(t *testing.T) {
	t.Parallel()
	conn := redcache.OpenMemory()
	strs := redcache.New(conn, redcache.StringCodec{})
	err := strs.ForceSet(t.Context(), time.Minute, "k", 42)
	require.Error(t, err, "StringCodec must reject an int even in memory")
	require.NoError(t, strs.ForceSet(t.Context(), time.Minute, "k", "v"))
	_, err = strs.Get(t.Context(), time.Minute, "k", func(context.Context, string) (int, error) { return 0, nil })
	require.ErrorIs(t, err, redcache.ErrDecode)
}

func TestMemory_Get_PerOperationTypes(t *testing.T) {
	t.Parallel()
	c, _ := memCache(t)
	type userID string
	n, err := c.Get(t.Context(), time.Minute, userID("u1"), func(context.Context, userID) (int, error) { return 7, nil })
	require.NoError(t, err)
	assert.Equal(t, 7, n)
	s, err := c.Get(t.Context(), time.Minute, "s", func(context.Context, string) (string, error) { return "x", nil })
	require.NoError(t, err)
	assert.Equal(t, "x", s)
}

func TestMemory_DelAndDelMulti(t *testing.T) {
	t.Parallel()
	c, _ := memCache(t)
	for _, k := range []string{"a", "b", "c"} {
		require.NoError(t, c.ForceSet(t.Context(), time.Minute, k, 1))
	}
	require.NoError(t, c.Del(t.Context(), "a"))
	require.NoError(t, c.DelMulti(t.Context(), []string{"b", "missing"}))
	for k, want := range map[string]bool{"a": false, "b": false, "c": true} {
		_, ok, err := c.Peek[string, int](t.Context(), time.Minute, k)
		require.NoError(t, err)
		assert.Equal(t, want, ok, k)
	}
}

func TestMemory_Expiry_CausesRefetch(t *testing.T) {
	t.Parallel()
	c, now := memCache(t)
	fn, calls := countingLoader(7)
	ttl := time.Minute
	_, err := c.Get(t.Context(), ttl, "k", fn)
	require.NoError(t, err)
	*now = now.Add(ttl + time.Second)
	got, err := c.Get(t.Context(), ttl, "k", fn)
	require.NoError(t, err)
	assert.Equal(t, 7, got)
	assert.Equal(t, int64(2), calls.Load(), "expired entry should trigger a re-fetch")
}

func TestMemory_Touch_ExtendsExpiryAndNoOpsOnMissing(t *testing.T) {
	t.Parallel()
	c, now := memCache(t)
	require.NoError(t, c.ForceSet(t.Context(), time.Minute, "k", 1))
	*now = now.Add(50 * time.Second)
	require.NoError(t, c.Touch(t.Context(), time.Minute, "k"))
	require.NoError(t, c.Touch(t.Context(), time.Minute, "missing"))
	*now = now.Add(50 * time.Second) // 100s after store; alive only because of Touch
	_, ok, err := c.Peek[string, int](t.Context(), time.Minute, "k")
	require.NoError(t, err)
	assert.True(t, ok)
	_, ok, err = c.Peek[string, int](t.Context(), time.Minute, "missing")
	require.NoError(t, err)
	assert.False(t, ok, "Touch must not create entries")
	require.NoError(t, c.TouchMulti(t.Context(), time.Minute, []string{"k", "missing"}))
}

func TestMemory_GetMulti_PartialHitCallsFnWithMissingOnly(t *testing.T) {
	t.Parallel()
	c, _ := memCache(t)
	require.NoError(t, c.ForceSet(t.Context(), time.Minute, "a", 1))
	var asked []string
	got, err := c.GetMulti(t.Context(), time.Minute, []string{"a", "b", "c"},
		func(_ context.Context, keys []string) (map[string]int, error) {
			asked = keys
			out := make(map[string]int, len(keys))
			for _, k := range keys {
				out[k] = len(k) + 10
			}
			return out, nil
		})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"b", "c"}, asked)
	assert.Equal(t, map[string]int{"a": 1, "b": 11, "c": 11}, got)

	var ran atomic.Bool
	_, err = c.GetMulti(t.Context(), time.Minute, []string{"a", "b"},
		func(context.Context, []string) (map[string]int, error) { ran.Store(true); return nil, nil })
	require.NoError(t, err)
	assert.False(t, ran.Load(), "all hits must skip fn")

	wantErr := errors.New("boom")
	_, err = c.GetMulti(t.Context(), time.Minute, []string{"zz"},
		func(context.Context, []string) (map[string]int, error) { return nil, wantErr })
	require.ErrorIs(t, err, wantErr)
}

func TestMemory_SetAndSetMulti(t *testing.T) {
	t.Parallel()
	c, _ := memCache(t)
	require.NoError(t, c.Set(t.Context(), time.Minute, "k", func(context.Context, string) (int, error) { return 3, nil }))
	got, ok, err := c.Peek[string, int](t.Context(), time.Minute, "k")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 3, got)

	wantErr := errors.New("boom")
	require.ErrorIs(t, c.Set(t.Context(), time.Minute, "k2", func(context.Context, string) (int, error) { return 0, wantErr }), wantErr)
	_, ok, err = c.Peek[string, int](t.Context(), time.Minute, "k2")
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, c.SetMulti(t.Context(), time.Minute, []string{"m1", "m2"},
		func(_ context.Context, keys []string) (map[string]int, error) {
			out := map[string]int{}
			for _, k := range keys {
				out[k] = 1
			}
			return out, nil
		}))
	res, err := c.GetMulti(t.Context(), time.Minute, []string{"m1", "m2"},
		func(context.Context, []string) (map[string]int, error) { t.Fatal("must be hits"); return nil, nil })
	require.NoError(t, err)
	assert.Len(t, res, 2)
}

func TestMemory_InvalidTTLRejected(t *testing.T) {
	t.Parallel()
	c, _ := memCache(t)
	require.ErrorIs(t, c.ForceSet(t.Context(), 0, "k", 9), redcache.ErrInvalidTTL)
	_, err := c.Get(t.Context(), 0, "k", func(context.Context, string) (int, error) {
		t.Fatal("loader must not run for invalid ttl")
		return 0, nil
	})
	require.ErrorIs(t, err, redcache.ErrInvalidTTL)
	require.ErrorIs(t, c.Touch(t.Context(), -time.Second, "k"), redcache.ErrInvalidTTL)
}
