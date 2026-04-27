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
	mu                                       sync.Mutex
	panicKeys                                []string
	errKeys                                  []string
}

func (m *capturingMetrics) CacheHit(string)         { m.hits.Add(1) }
func (m *capturingMetrics) CacheMiss(string)        { m.misses.Add(1) }
func (m *capturingMetrics) LockContended(string)    { m.contended.Add(1) }
func (m *capturingMetrics) LockLost(string)         { m.lost.Add(1) }
func (m *capturingMetrics) RefreshTriggered(string) { m.triggered.Add(1) }
func (m *capturingMetrics) RefreshSkipped(string)   { m.skipped.Add(1) }
func (m *capturingMetrics) RefreshDropped(string)   { m.dropped.Add(1) }
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
