package redcache_test

import (
	"context"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

// Open builds a Conn that owns its client; Conn.Close shuts the underlying
// client cleanly and is idempotent (the engine guards on closeOnce).
func TestConn_ClosesCleanly(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires Redis")
	}

	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.WithLockTTL(time.Second),
	)
	require.NoError(t, err)
	c := redcache.New(conn, redcache.StringCodec{})

	ctx := t.Context()
	key := "conn:oneshot:" + uuid.New().String()
	got, err := c.Get(ctx, time.Second, key, func(context.Context, string) (string, error) {
		return "v", nil
	})
	require.NoError(t, err)
	require.Equal(t, "v", got)

	conn.Close()
	conn.Close() // idempotent
}

// Close must not leave engine goroutines (refresh workers, lock waiters) blocked
// forever. TestMain checks the whole run; this pins the check to Close with a
// refresh worker live.
func TestConn_Close_LeaksNoGoroutines(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Redis")
	}

	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.WithLockTTL(2*time.Second),
		redcache.WithRefreshAfterFraction(0.5),
	)
	require.NoError(t, err)

	cache := redcache.New(conn, redcache.StringCodec{})
	key := "leak:" + uuid.New().String()
	_, err = cache.Get(t.Context(), time.Minute, key, func(context.Context, string) (string, error) {
		return "v", nil
	})
	require.NoError(t, err)
	conn.Close()

	require.Empty(t, leakedGoroutines(), "leaked goroutines after Close")
}
