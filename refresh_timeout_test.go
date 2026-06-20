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

// A refresh-ahead callback slower than LockTTL must still complete and persist
// its value when RefreshTimeout gives it a larger compute budget — instead of
// being cancelled at LockTTL and reported as an error.
func TestRefreshAhead_RefreshTimeoutExtendsCallbackBudget(t *testing.T) {
	t.Parallel()
	metrics := &capturingMetrics{}
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{
			LockTTL:              200 * time.Millisecond,
			RefreshAfterFraction: 0.01,
			RefreshTimeout:       3 * time.Second,
			Metrics:              metrics,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
		client.Client().Close()
	})

	ctx := context.Background()
	key := "refresh-timeout:" + uuid.New().String()
	var calls atomic.Int32

	// First call populates quickly; refresh calls take longer than LockTTL but
	// well under RefreshTimeout, and honour context cancellation.
	cb := func(ctx context.Context, _ string) (string, error) {
		if calls.Add(1) == 1 {
			return "initial", nil
		}
		select {
		case <-time.After(400 * time.Millisecond):
			return "refreshed", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	const dataTTL = 5 * time.Second
	_, err = client.Get(ctx, dataTTL, key, cb)
	require.NoError(t, err)

	// Age the value past the refresh floor, then trigger a refresh.
	time.Sleep(100 * time.Millisecond)
	_, err = client.Get(ctx, dataTTL, key, cb)
	require.NoError(t, err)

	// The slow refresh should land its new value (it would be cancelled at
	// LockTTL=200ms without RefreshTimeout).
	require.Eventually(t, func() bool {
		v, gErr := client.Get(ctx, dataTTL, key, cb)
		return gErr == nil && v == "refreshed"
	}, 2*time.Second, 20*time.Millisecond, "refresh should have persisted the new value")

	require.Zero(t, metrics.errs.Load(), "no RefreshError should fire when the callback finishes within RefreshTimeout")
}
