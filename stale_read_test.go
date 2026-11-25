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
				if err := runStaleReadIteration(ctx, cache, conn.Client(), keys, i, &reads, &stale); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()

	stats := redcache.WriteMarkStatsForTest(conn)
	total := stats.Invalidated + stats.Expired + stats.AtCap + stats.Live
	t.Logf("reads=%d stale=%d cache_hits=%d bypass_reads=%d invalidated=%d expired=%d cap=%d live=%d",
		reads.Load(), stale.Load(), paths.CacheHits.Load(), paths.Bypasses.Load(), stats.Invalidated, stats.Expired, stats.AtCap, stats.Live)
	require.EqualValues(t, workers*iterations*nkeys, reads.Load())
	if stale.Load() != 0 {
		t.Errorf("read %d stale values after acknowledged writes", stale.Load())
	}
	if paths.CacheHits.Load() == 0 || paths.Bypasses.Load() == 0 {
		t.Error("expected both cache hits and plain bypass reads")
	}
	if total == 0 || 10*stats.Invalidated < 9*total {
		t.Errorf("expected invalidations to end at least 90%% of marks, got %d/%d", stats.Invalidated, total)
	}
}

func runStaleReadIteration(ctx context.Context, cache *redcache.Cache, client rueidis.Client, keys []string, iteration int, reads, stale *atomic.Int64) error {
	const ttl = time.Minute
	// Keep the keys registered for invalidation, including after an iteration
	// whose post-write reads all bypassed client-side caching.
	cmds := make([]rueidis.CacheableTTL, len(keys))
	for i, key := range keys {
		cmds[i] = rueidis.CacheableTTL{Cmd: client.B().Get().Key(key).Cache(), TTL: ttl}
	}
	for _, resp := range client.DoMultiCache(ctx, cmds...) {
		if err := resp.Error(); err != nil && !rueidis.IsRedisNil(err) {
			return err
		}
	}

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
