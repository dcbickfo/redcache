package redcache_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

func TestReadYourWritesAfterForceSetMulti(t *testing.T) {
	t.Run("standalone", func(t *testing.T) {
		var paths redcache.ReadPathCountsForTest
		cache, conn := makeClient(t, addr, paths.Option())
		checkReadYourWrites(t, cache, conn, &paths)
	})
	t.Run("cluster", func(t *testing.T) {
		var paths redcache.ReadPathCountsForTest
		cache, conn := makeClusterClient(t, paths.Option())
		checkReadYourWrites(t, cache, conn, &paths)
	})
}

func checkReadYourWrites(t *testing.T, cache *redcache.Cache, conn *redcache.Conn, paths *redcache.ReadPathCountsForTest) {
	t.Helper()
	const workers, iterations, nkeys = 4, 5000, 64
	var reads, stale atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			ctx := t.Context()
			keys := make([]string, nkeys)
			for i := range keys {
				keys[i] = "stale-read:" + uuid.New().String()
			}

			for i := range iterations {
				if err := runStaleReadIteration(ctx, cache, keys, i, &reads, &stale); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()

	stats := redcache.WriteMarkStatsForTest(conn)
	// Include writes that reused an existing mark in the denominator so a hot
	// key that stays marked across many writes cannot inflate the share.
	total := max(int64(workers*iterations*nkeys), stats.Invalidated+stats.Expired+stats.AtCap+stats.Live)
	t.Logf("reads=%d stale=%d cache_hits=%d bypass_reads=%d invalidated=%d expired=%d cap=%d live=%d",
		reads.Load(), stale.Load(), paths.CacheHits.Load(), paths.Bypasses.Load(), stats.Invalidated, stats.Expired, stats.AtCap, stats.Live)
	require.EqualValues(t, workers*iterations*nkeys, reads.Load())
	if stale.Load() != 0 {
		t.Errorf("read %d stale values after acknowledged writes", stale.Load())
	}
	if paths.Bypasses.Load() == 0 {
		t.Error("expected plain bypass reads")
	}
	if 10*stats.Invalidated < 9*total {
		t.Errorf("expected invalidations to end at least 90%% of marks, got %d/%d", stats.Invalidated, total)
	}
}

func runStaleReadIteration(ctx context.Context, cache *redcache.Cache, keys []string, iteration int, reads, stale *atomic.Int64) error {
	const ttl = time.Minute
	want := valuesFor(keys, strconv.Itoa(iteration))
	if err := cache.ForceSetMulti(ctx, ttl, want); err != nil {
		return err
	}
	got, err := cache.GetMulti(ctx, ttl, keys, func(context.Context, []string) (map[string]string, error) {
		return nil, errors.New("loader ran after an acknowledged ForceSetMulti")
	})
	if err != nil {
		return err
	}
	for _, key := range keys {
		if got[key] != want[key] {
			stale.Add(1)
		}
	}
	reads.Add(int64(len(keys)))
	return nil
}

func TestWriteMarkDeadlineAndFailure(t *testing.T) {
	var marks redcache.WriteMarksForTest
	now := time.Now()
	finish := marks.Begin([]string{"failed"}, now, 10)
	marks.Invalidate("failed")
	finish(now, false)
	marks.Invalidate("failed")
	require.True(t, marks.Bypass("failed", now.Add(time.Second-time.Nanosecond)))
	require.False(t, marks.Bypass("failed", now.Add(time.Second)))
	require.Equal(t, redcache.WriteMarkCountsForTest{Expired: 1}, marks.Counts())

	marks.Begin([]string{"unread"}, now, 10)(now, true)
	marks.Begin([]string{"new"}, now.Add(time.Second), 10)(now.Add(time.Second), true)
	require.Equal(t, redcache.WriteMarkCountsForTest{Expired: 2, Live: 1}, marks.Counts())
}

func TestWriteMarkOverlap(t *testing.T) {
	var marks redcache.WriteMarksForTest
	now := time.Now()
	first := marks.Begin([]string{"key"}, now, 10)
	second := marks.Begin([]string{"key"}, now, 10)
	marks.Invalidate("key")
	first(now, true)
	require.True(t, marks.Bypass("key", now))
	require.EqualValues(t, 1, marks.Counts().Live)

	// The push belonged to the first write. The second key was no longer
	// registered for client-side caching, so its write sends no push.
	second(now, true)
	require.True(t, marks.Bypass("key", now))
	require.False(t, marks.Bypass("key", now.Add(time.Second)))
	require.Equal(t, redcache.WriteMarkCountsForTest{Expired: 1}, marks.Counts())

	// A push for an ordinary single write can arrive before its reply.
	third := marks.Begin([]string{"key"}, now, 10)
	marks.Invalidate("key")
	require.True(t, marks.Bypass("key", now))
	third(now, true)
	require.False(t, marks.Bypass("key", now))
	require.Equal(t, redcache.WriteMarkCountsForTest{Invalidated: 1, Expired: 1}, marks.Counts())
}

func TestWriteMarkCap(t *testing.T) {
	var marks redcache.WriteMarksForTest
	now := time.Now()
	first := marks.Begin([]string{"first"}, now, 1)
	second := marks.Begin([]string{"second", "third"}, now, 1)
	require.Equal(t, redcache.WriteMarkCountsForTest{AtCap: 2, Live: 1}, marks.Counts())

	late := now.Add(2 * time.Second)
	require.True(t, marks.Bypass("unrelated", late), "overflowed writes are still in flight")
	first(now, true)
	second(late, true)
	require.True(t, marks.Bypass("unrelated", late.Add(time.Second-time.Nanosecond)))
	require.False(t, marks.Bypass("unrelated", late.Add(time.Second)))
	require.False(t, marks.Bypass("first", late.Add(time.Second)))
	require.Equal(t, redcache.WriteMarkCountsForTest{Expired: 1, AtCap: 2}, marks.Counts())
}

func TestWriteMarkNilInvalidation(t *testing.T) {
	var marks redcache.WriteMarksForTest
	now := time.Now()
	finish := marks.Begin([]string{"in-flight", "another-slot"}, now, 10)
	marks.Reset(now)
	require.Equal(t, redcache.WriteMarkCountsForTest{Invalidated: 2}, marks.Counts())

	// A nil callback can come from one node. Other nodes' active writes stay
	// covered even though the per-key table has been cleared.
	late := now.Add(2 * time.Second)
	require.True(t, marks.Bypass("another-slot", late))
	finish(late, true)
	require.False(t, marks.Bypass("another-slot", late.Add(time.Second)))
	require.Zero(t, marks.Counts().Live)
}

func TestWriteMarkBypassKeepsWaitEntry(t *testing.T) {
	for _, multi := range []bool{false, true} {
		t.Run(map[bool]string{false: "Get", true: "GetMulti"}[multi], func(t *testing.T) {
			var paths redcache.ReadPathCountsForTest
			cache, conn := makeClient(t, addr, paths.Option())
			ctx := t.Context()
			key := "bypass-wait:" + uuid.New().String()
			require.NoError(t, cache.ForceSet(ctx, time.Minute, key, "fresh"))

			if multi {
				other := "tracked:" + uuid.New().String()
				raw := conn.Client()
				require.NoError(t, raw.Do(ctx, raw.B().Set().Key(other).Value("other").Px(time.Minute).Build()).Error())
				got, err := cache.GetMulti(ctx, time.Minute, []string{other, key}, func(context.Context, []string) (map[string]string, error) {
					return nil, errors.New("loader ran for a present key")
				})
				require.NoError(t, err)
				require.Equal(t, map[string]string{key: "fresh", other: "other"}, got)
			} else {
				got, err := cache.Get(ctx, time.Minute, key, func(context.Context, string) (string, error) {
					return "", errors.New("loader ran for a present key")
				})
				require.NoError(t, err)
				require.Equal(t, "fresh", got)
			}
			require.EqualValues(t, 1, paths.Bypasses.Load())
			require.True(t, redcache.HasWaitEntryForTest(conn, key), "plain bypass GET must not release the tracked wait entry")
		})
	}
}

func TestWriteMarksOnValueChanges(t *testing.T) {
	withoutInvalidations := redcache.WithClientBuilder(func(opt rueidis.ClientOption) (rueidis.Client, error) {
		opt.OnInvalidations = nil
		return rueidis.NewClient(opt)
	})
	for _, name := range []string{"Set", "SetMulti", "ForceSet", "ForceSetMulti", "Del", "DelMulti", "Get", "GetMulti"} {
		t.Run(name, func(t *testing.T) {
			cache, conn := makeClient(t, addr, withoutInvalidations)
			ctx := t.Context()
			key := "write-mark:" + uuid.New().String()
			require.NoError(t, changeMarkedValue(ctx, cache, name, key))
			require.EqualValues(t, 1, redcache.WriteMarkStatsForTest(conn).Live)
		})
	}
}

func changeMarkedValue(ctx context.Context, cache *redcache.Cache, name, key string) error {
	const ttl = time.Minute
	load := func(context.Context, string) (string, error) { return "value", nil }
	values := map[string]string{key: "value"}
	keys := []string{key}
	switch name {
	case "Set":
		return cache.Set(ctx, ttl, key, load)
	case "SetMulti":
		return cache.SetMulti(ctx, ttl, keys, mapLoader(values))
	case "ForceSet":
		return cache.ForceSet(ctx, ttl, key, values[key])
	case "ForceSetMulti":
		return cache.ForceSetMulti(ctx, ttl, values)
	case "Del":
		return cache.Del(ctx, key)
	case "DelMulti":
		return cache.DelMulti(ctx, keys)
	case "Get":
		_, err := cache.Get(ctx, ttl, key, load)
		return err
	case "GetMulti":
		_, err := cache.GetMulti(ctx, ttl, keys, mapLoader(values))
		return err
	default:
		panic("unknown cache operation: " + name)
	}
}
