package redcache_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

var errRefreshFailed = errors.New("simulated refresh failure")

type capturingMetrics struct {
	redcache.NoopMetrics
	hits, misses, contended, lost            atomic.Int64
	triggered, skipped, dropped, panic, errs atomic.Int64
	waitDurations                            atomic.Int64 // count of LockWaitDuration calls
	totalWait                                atomic.Int64 // sum of durations in nanoseconds
	mu                                       sync.Mutex
	panicKeys                                []string
	errKeys                                  []string
}

func (m *capturingMetrics) CacheHits(n int64)     { m.hits.Add(n) }
func (m *capturingMetrics) CacheMisses(n int64)   { m.misses.Add(n) }
func (m *capturingMetrics) LockContended(n int64) { m.contended.Add(n) }
func (m *capturingMetrics) LockWaitDuration(d time.Duration) {
	m.waitDurations.Add(1)
	m.totalWait.Add(int64(d))
}
func (m *capturingMetrics) LockLost(string)          { m.lost.Add(1) }
func (m *capturingMetrics) RefreshTriggered(n int64) { m.triggered.Add(n) }
func (m *capturingMetrics) RefreshSkipped(n int64)   { m.skipped.Add(n) }
func (m *capturingMetrics) RefreshDropped(n int64)   { m.dropped.Add(n) }
func (m *capturingMetrics) RefreshPanicked(key string) {
	m.panic.Add(1)
	m.mu.Lock()
	m.panicKeys = append(m.panicKeys, key)
	m.mu.Unlock()
}
func (m *capturingMetrics) RefreshError(key string) {
	m.errs.Add(1)
	m.mu.Lock()
	m.errKeys = append(m.errKeys, key)
	m.mu.Unlock()
}

func TestMetrics_HitAndMiss(t *testing.T) {
	t.Parallel()
	metrics := &capturingMetrics{}
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{
			LockTTL: time.Second,
			Metrics: metrics,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { client.Client().Close() })

	ctx := context.Background()
	key := "metrics:" + uuid.New().String()

	// First Get: miss + populate.
	_, err = client.Get(ctx, time.Second*10, key, func(ctx context.Context, _ string) (string, error) {
		return "v1", nil
	})
	require.NoError(t, err)

	// Second Get: hit.
	_, err = client.Get(ctx, time.Second*10, key, func(ctx context.Context, _ string) (string, error) {
		return "v1", nil
	})
	require.NoError(t, err)

	require.GreaterOrEqual(t, metrics.misses.Load(), int64(1), "expected at least 1 miss")
	require.GreaterOrEqual(t, metrics.hits.Load(), int64(1), "expected at least 1 hit")
}

func TestMetrics_RefreshTriggered(t *testing.T) {
	t.Parallel()
	metrics := &capturingMetrics{}
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{
			LockTTL:              time.Second * 2,
			RefreshAfterFraction: 0.01, // refresh almost immediately
			RefreshBeta:          0,    // disable XFetch sampling for deterministic tests
			Metrics:              metrics,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
		client.Client().Close()
	})

	ctx := context.Background()
	key := "refresh-metrics:" + uuid.New().String()

	// Populate.
	_, err = client.Get(ctx, time.Second, key, func(ctx context.Context, _ string) (string, error) {
		return "initial", nil
	})
	require.NoError(t, err)

	// Wait so PTTL crosses the very-low threshold.
	time.Sleep(50 * time.Millisecond)

	// Read again — should fire RefreshTriggered.
	_, err = client.Get(ctx, time.Second, key, func(ctx context.Context, _ string) (string, error) {
		return "refreshed", nil
	})
	require.NoError(t, err)

	// Poll until the refresh worker has emitted the metric (avoids hard-coded sleep).
	require.Eventually(t, func() bool {
		return metrics.triggered.Load() >= 1
	}, 2*time.Second, 5*time.Millisecond, "expected RefreshTriggered to fire")
}

func TestMetrics_RefreshPanickedIncludesKey(t *testing.T) {
	t.Parallel()
	metrics := &capturingMetrics{}
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{
			LockTTL:              time.Second * 2,
			RefreshAfterFraction: 0.01,
			RefreshBeta:          0,
			Metrics:              metrics,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
		client.Client().Close()
	})

	ctx := context.Background()
	key := "panic-metrics:" + uuid.New().String()
	var calls atomic.Int32

	cb := func(_ context.Context, _ string) (string, error) {
		n := calls.Add(1)
		if n == 1 {
			return "initial", nil
		}
		panic("simulated panic")
	}

	_, err = client.Get(ctx, time.Second, key, cb)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	_, err = client.Get(ctx, time.Second, key, cb)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return metrics.panic.Load() >= 1
	}, 2*time.Second, 5*time.Millisecond, "expected RefreshPanicked to fire")

	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	require.Contains(t, metrics.panicKeys, key, "RefreshPanicked should be tagged with the offending key")
}

// TestMetrics_RefreshErrorOnCallbackError verifies that a refresh-ahead callback
// returning an error emits RefreshError tagged with the affected key. Without
// this signal, operators can't distinguish healthy dedup contention
// (RefreshSkipped) from a broken upstream that's silently failing every refresh.
func TestMetrics_RefreshErrorOnCallbackError(t *testing.T) {
	t.Parallel()
	metrics := &capturingMetrics{}
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{
			LockTTL:              time.Second * 2,
			RefreshAfterFraction: 0.01,
			RefreshBeta:          0,
			Metrics:              metrics,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
		client.Client().Close()
	})

	ctx := context.Background()
	key := "refresh-err-metrics:" + uuid.New().String()
	var calls atomic.Int32

	cb := func(_ context.Context, _ string) (string, error) {
		n := calls.Add(1)
		if n == 1 {
			return "initial", nil
		}
		return "", errRefreshFailed
	}

	_, err = client.Get(ctx, time.Second, key, cb)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	_, err = client.Get(ctx, time.Second, key, cb)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return metrics.errs.Load() >= 1
	}, 2*time.Second, 5*time.Millisecond, "expected RefreshError to fire")

	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	require.Contains(t, metrics.errKeys, key, "RefreshError should be tagged with the offending key")
}

// TestMetrics_RefreshDroppedUnderBackpressure verifies that RefreshDropped fires
// when the refresh queue saturates. Without this signal, an operator running a
// queue too small for traffic sees only slowly-drifting cache freshness — they
// can't distinguish healthy contention (RefreshSkipped) from queue exhaustion
// (RefreshDropped) without the metric.
func TestMetrics_RefreshDroppedUnderBackpressure(t *testing.T) {
	t.Parallel()
	metrics := &capturingMetrics{}
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{
			LockTTL:              time.Second * 3,
			RefreshAfterFraction: 0.01, // refresh almost immediately, removes timing-margin flake
			RefreshBeta:          0,
			RefreshWorkers:       1,
			RefreshQueueSize:     1,
			Metrics:              metrics,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
		client.Client().Close()
	})
	ctx := context.Background()

	const numKeys = 20
	keys := make([]string, numKeys)
	for i := range numKeys {
		keys[i] = fmt.Sprintf("key:%d:%s", i, uuid.New().String())
	}

	ttl := time.Second
	populateCb := func(_ context.Context, _ string) (string, error) {
		return "initial", nil
	}
	for _, key := range keys {
		_, err := client.Get(ctx, ttl, key, populateCb)
		require.NoError(t, err)
	}

	time.Sleep(50 * time.Millisecond) // cross the very-low refresh threshold

	refreshCb := func(_ context.Context, _ string) (string, error) {
		time.Sleep(500 * time.Millisecond) // keep the single worker busy
		return "refreshed", nil
	}

	var wg sync.WaitGroup
	for _, key := range keys {
		k := key
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = client.Get(ctx, ttl, k, refreshCb)
		}()
	}
	wg.Wait()

	require.Eventually(t, func() bool {
		return metrics.dropped.Load() > 0
	}, 2*time.Second, 5*time.Millisecond, "expected RefreshDropped to fire under queue saturation")
}

func TestMetrics_LockWaitDuration(t *testing.T) {
	t.Parallel()
	metrics := &capturingMetrics{}
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{
			LockTTL: 2 * time.Second,
			Metrics: metrics,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
		client.Client().Close()
	})

	ctx := context.Background()
	key := "lockwait:" + uuid.New().String()

	// Two concurrent Gets — second one observes the lock and waits.
	holderInCb := make(chan struct{})
	holderProceed := make(chan struct{})
	holderCb := func(_ context.Context, _ string) (string, error) {
		close(holderInCb)
		<-holderProceed
		return "v", nil
	}
	waiterCb := func(_ context.Context, _ string) (string, error) {
		return "v", nil
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := client.Get(ctx, 5*time.Second, key, holderCb)
		require.NoError(t, err)
	}()

	// Wait for holder to actually be inside the callback (= it owns the lock).
	<-holderInCb

	go func() {
		defer wg.Done()
		_, err := client.Get(ctx, 5*time.Second, key, waiterCb)
		require.NoError(t, err)
	}()

	// Wait until the waiter is in the contended branch (about to enter
	// awaitLock). Without this barrier, the holder may release the lock
	// before the waiter has even started waiting, recording a near-zero
	// duration and hiding regressions in how the wait is timed.
	require.Eventually(t, func() bool {
		return metrics.contended.Load() >= 1
	}, time.Second, time.Millisecond, "waiter never entered contended branch")

	// Hold the lock for a measurable interval, then release.
	time.Sleep(100 * time.Millisecond)
	close(holderProceed)
	wg.Wait()

	require.Greater(t, metrics.waitDurations.Load(), int64(0), "expected LockWaitDuration to fire")
	// Waiter began waiting before the 100ms sleep; require the recorded
	// duration to reflect a meaningful wait. A regression that records
	// near-zero (e.g. metric emitted before the wait, or duration captured
	// at the wrong point) would pass a `> 0` assertion but fail this lower
	// bound. Margin: 50ms below the 100ms hold to absorb scheduling jitter.
	require.GreaterOrEqual(t, metrics.totalWait.Load(), int64(50*time.Millisecond),
		"LockWaitDuration must reflect actual wait time, not near-zero")
}
