package redcache_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

type capturingMetrics struct {
	redcache.NoopMetrics
	hits, misses, contended, lost      atomic.Int64
	triggered, skipped, dropped, panic atomic.Int64
}

func (m *capturingMetrics) CacheHit(string)         { m.hits.Add(1) }
func (m *capturingMetrics) CacheMiss(string)        { m.misses.Add(1) }
func (m *capturingMetrics) LockContended(string)    { m.contended.Add(1) }
func (m *capturingMetrics) LockLost(string)         { m.lost.Add(1) }
func (m *capturingMetrics) RefreshTriggered(string) { m.triggered.Add(1) }
func (m *capturingMetrics) RefreshSkipped(string)   { m.skipped.Add(1) }
func (m *capturingMetrics) RefreshDropped(string)   { m.dropped.Add(1) }
func (m *capturingMetrics) RefreshPanicked(any)     { m.panic.Add(1) }

func TestMetrics_HitAndMiss(t *testing.T) {
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
