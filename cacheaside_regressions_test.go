package redcache_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

// TestCacheAside_Close_SafeUnderConcurrentRefresh hammers Get while Close runs
// concurrently and is called twice. A regression in enqueueRefresh's
// closing-flag/recover guard or Close's closeOnce would surface as a panic.
func TestCacheAside_Close_SafeUnderConcurrentRefresh(t *testing.T) {
	t.Parallel()
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{
			LockTTL:              2 * time.Second,
			RefreshAfterFraction: 0.01, // refresh on virtually every Get
			RefreshBeta:          0,
			RefreshWorkers:       2,
			RefreshQueueSize:     4, // small queue maximizes the close-during-send window
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { client.Client().Close() })

	ctx := context.Background()
	key := "close-stress:" + uuid.New().String()

	_, err = client.Get(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
		return "v", nil
	})
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond) // PTTL drops below the 1% threshold

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = client.Get(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
					return "v", nil
				})
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	var closeWg sync.WaitGroup
	closeWg.Add(2)
	go func() {
		defer closeWg.Done()
		client.Close()
	}()
	go func() {
		defer closeWg.Done()
		client.Close() // double-close must be a no-op (closeOnce).
	}()
	closeWg.Wait()

	close(stop)
	wg.Wait()
	// Reaching here without a panic is the assertion.
}

// TestCacheAside_Get_CleanMissEmitsNoFalseLockLost guards against the bug where
// setKeyLua returned "OK" (unparseable by AsInt64), causing every successful CAS
// to be misreported as a lost lock.
func TestCacheAside_Get_CleanMissEmitsNoFalseLockLost(t *testing.T) {
	t.Parallel()
	metrics := &capturingMetrics{}
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{LockTTL: 2 * time.Second, Metrics: metrics},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
		client.Client().Close()
	})

	ctx := context.Background()
	key := "clean-miss:" + uuid.New().String()
	val := "val:" + uuid.New().String()

	res, err := client.Get(ctx, 10*time.Second, key, func(_ context.Context, _ string) (string, error) {
		return val, nil
	})
	require.NoError(t, err)
	assert.Equal(t, val, res)
	assert.Zero(t, metrics.lost.Load(), "uncontended cache miss must not emit LockLost")
	assert.Zero(t, metrics.contended.Load(), "uncontended cache miss must not emit LockContended")
	assert.Equal(t, int64(1), metrics.misses.Load(), "expected exactly one CacheMiss for a single populating Get")
}

// TestCacheAside_GetMulti_CleanMissEmitsNoFalseLockLost is the multi-key
// counterpart: catches runSlotSet CAS-interpretation regressions.
func TestCacheAside_GetMulti_CleanMissEmitsNoFalseLockLost(t *testing.T) {
	t.Parallel()
	metrics := &capturingMetrics{}
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{LockTTL: 2 * time.Second, Metrics: metrics},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
		client.Client().Close()
	})

	ctx := context.Background()
	suffix := uuid.New().String()
	keys := []string{"multi-clean:1:" + suffix, "multi-clean:2:" + suffix}
	want := map[string]string{keys[0]: "v1", keys[1]: "v2"}

	res, err := client.GetMulti(ctx, 10*time.Second, keys, func(_ context.Context, ks []string) (map[string]string, error) {
		out := make(map[string]string, len(ks))
		for _, k := range ks {
			out[k] = want[k]
		}
		return out, nil
	})
	require.NoError(t, err)
	assert.Equal(t, want, res)
	assert.Zero(t, metrics.lost.Load(), "uncontended GetMulti must not emit LockLost")
	assert.Zero(t, metrics.contended.Load(), "uncontended GetMulti must not emit LockContended")
}

// TestCacheAside_EmptyValueIsCacheHit verifies an empty-string cached value is
// returned as a hit rather than treated as a miss.
func TestCacheAside_EmptyValueIsCacheHit(t *testing.T) {
	t.Parallel()
	metrics := &capturingMetrics{}
	pca, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{LockTTL: 2 * time.Second, Metrics: metrics},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		pca.Close()
		pca.Client().Close()
	})

	ctx := context.Background()
	key := "empty-hit:" + uuid.New().String()

	require.NoError(t, pca.ForceSet(ctx, 10*time.Second, key, ""))

	res, err := pca.Get(ctx, 10*time.Second, key, func(_ context.Context, _ string) (string, error) {
		t.Fatal("callback must not be invoked when an empty value is cached")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "", res, "stored empty value should be returned as-is")
	assert.GreaterOrEqual(t, metrics.hits.Load(), int64(1), "expected CacheHit metric for empty value")
	assert.Zero(t, metrics.misses.Load(), "no CacheMiss should be recorded for an empty value")
}

// TestPrimeableCacheAside_Set_RollbackPreservesEmptyValue verifies a Set callback
// failure restores "" rather than DELing the key.
func TestPrimeableCacheAside_Set_RollbackPreservesEmptyValue(t *testing.T) {
	t.Parallel()
	pca := makePrimeableClient(t, addr)
	defer pca.Client().Close()
	ctx := context.Background()

	key := "rollback-empty:" + uuid.New().String()

	require.NoError(t, pca.ForceSet(ctx, 10*time.Second, key, ""))

	sentinel := errors.New("callback failed")
	err := pca.Set(ctx, 10*time.Second, key, func(_ context.Context, _ string) (string, error) {
		return "", sentinel
	})
	require.ErrorIs(t, err, sentinel)

	res, err := pca.Get(ctx, 10*time.Second, key, func(_ context.Context, _ string) (string, error) {
		t.Fatal("callback must not be invoked — rollback should have restored \"\"")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "", res, "rollback should preserve the empty-string value, not DEL the key")
}

// TestPrimeableCacheAside_Set_RollbackPreservesPTTL verifies the prior value's
// remaining TTL is preserved on rollback. Failure modes:
//   - PTTL == -2: rollback DELed instead of restoring.
//   - PTTL == -1: rollback SET without PX (now persistent).
//   - PTTL ~= ttl arg: rollback used the new ttl, not the captured PTTL.
func TestPrimeableCacheAside_Set_RollbackPreservesPTTL(t *testing.T) {
	t.Parallel()
	pca := makePrimeableClient(t, addr)
	defer pca.Client().Close()
	ctx := context.Background()

	key := "rollback-pttl:" + uuid.New().String()

	require.NoError(t, pca.ForceSet(ctx, 2*time.Second, key, "v"))

	// Captured PTTL on rollback should be well under 2000ms.
	time.Sleep(500 * time.Millisecond)

	sentinel := errors.New("callback failed")
	err := pca.Set(ctx, 10*time.Second, key, func(_ context.Context, _ string) (string, error) {
		return "", sentinel
	})
	require.ErrorIs(t, err, sentinel)

	pttl, err := pca.Client().Do(ctx, pca.Client().B().Pttl().Key(key).Build()).AsInt64()
	require.NoError(t, err)
	assert.Greater(t, pttl, int64(0), "key should exist with a finite TTL after rollback")
	assert.Less(t, pttl, int64(1800), "rollback must preserve the original remaining TTL, not refresh it")

	val, err := pca.Client().Do(ctx, pca.Client().B().Get().Key(key).Build()).ToString()
	require.NoError(t, err)
	// Raw GET sees the envelope; Get() unwraps to "v" for callers.
	assert.Equal(t, "__redcache:v1:0:v", val, "rollback should restore the original captured (envelope-wrapped) value")
}

// TestCacheAside_GetMulti_CASMismatchDropsKey verifies runSlotSet drops keys
// with a stolen lock (CAS Lua returned 0) and emits LockLost.
func TestCacheAside_GetMulti_CASMismatchDropsKey(t *testing.T) {
	t.Parallel()
	metrics := &capturingMetrics{}
	pca, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{LockTTL: 2 * time.Second, Metrics: metrics},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		pca.Close()
		pca.Client().Close()
	})

	ctx := context.Background()
	key1 := "cas:1:" + uuid.New().String()
	key2 := "cas:2:" + uuid.New().String()
	forcedVal := "forced:" + uuid.New().String()
	val1 := "val1:" + uuid.New().String()
	val2 := "val2:" + uuid.New().String()

	res, err := pca.GetMulti(ctx, 10*time.Second, []string{key1, key2}, func(_ context.Context, _ []string) (map[string]string, error) {
		// Steal key2's lock mid-callback so the subsequent CAS fails.
		if forceErr := pca.ForceSet(ctx, 10*time.Second, key2, forcedVal); forceErr != nil {
			return nil, forceErr
		}
		return map[string]string{key1: val1, key2: val2}, nil
	})
	require.NoError(t, err)

	assert.Equal(t, val1, res[key1], "key1's CAS-set should succeed")
	// Retry after CAS failure surfaces the ForceSet value, not val2.
	assert.Equal(t, forcedVal, res[key2], "key2 should reflect the ForceSet, not the callback's value")
	assert.NotEqual(t, val2, res[key2], "callback's value must not be returned after CAS mismatch")
	assert.GreaterOrEqual(t, metrics.lost.Load(), int64(1), "expected LockLost metric for the stolen key")
}

// TestPrimeableCacheAside_Set_RollbackSurvivesContextCancel verifies Set's rollback
// completes under a cancelled caller ctx (bestEffortRestore uses cleanupCtx).
func TestPrimeableCacheAside_Set_RollbackSurvivesContextCancel(t *testing.T) {
	t.Parallel()
	client, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{LockTTL: 5 * time.Second},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
		client.Client().Close()
	})

	bg := context.Background()
	key := "rollback-cancel:" + uuid.New().String()
	originalVal := "original:" + uuid.New().String()

	_, err = client.Get(bg, 10*time.Second, key, func(_ context.Context, _ string) (string, error) {
		return originalVal, nil
	})
	require.NoError(t, err)

	cbErr := errors.New("callback failed under cancelled context")
	cancelCtx, cancel := context.WithCancel(bg)
	err = client.Set(cancelCtx, 10*time.Second, key, func(_ context.Context, _ string) (string, error) {
		cancel() // cancel before returning so the rollback path sees a dead ctx.
		return "", cbErr
	})
	require.ErrorIs(t, err, cbErr)

	// Allow invalidation to propagate.
	time.Sleep(100 * time.Millisecond)

	res, err := client.Get(bg, 10*time.Second, key, func(_ context.Context, _ string) (string, error) {
		return "callback-fired-restore-failed", nil
	})
	require.NoError(t, err)
	assert.Equal(t, originalVal, res, "rollback must succeed under cancelled ctx so key is restored, not held by lock")
}
