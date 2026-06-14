package redcache

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// Open builds a Conn; NewString and New derive typed views that share its engine
// (one client, one invalidation stream). This in-package test asserts the share
// is real via the unexported *cache.core, and that both views work.
func TestConn_SharesEngine(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires Redis")
	}

	conn, err := Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		WithLockTTL(2*time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	strs := NewString[string](conn, StringCodec{})
	ints := New[string, int](conn, StringKeyCodec{}, JSONCodec[int]{})

	require.Same(t, conn.core, strs.(*cache[string, string]).core, "view must share the Conn engine")
	require.Same(t, conn.core, ints.(*cache[string, int]).core, "views must share one engine")

	// nil codecs panic, they are not rejected with an error.
	require.Panics(t, func() { New[string, int](conn, nil, JSONCodec[int]{}) }, "New must panic on nil codec")

	// Both views are usable over the shared engine.
	ctx := context.Background()
	sKey := "conn:str:" + uuid.NewString()
	iKey := "conn:int:" + uuid.NewString()

	gotS, err := strs.Get(ctx, time.Second, sKey, func(context.Context, string) (string, error) {
		return "hello", nil
	})
	require.NoError(t, err)
	require.Equal(t, "hello", gotS)

	gotI, err := ints.Get(ctx, time.Second, iKey, func(context.Context, string) (int, error) {
		return 42, nil
	})
	require.NoError(t, err)
	require.Equal(t, 42, gotI)
}

// Open builds a Conn that owns its client; Conn.Close shuts the underlying
// client cleanly and is idempotent (the engine guards on closeOnce).
func TestConn_ClosesCleanly(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("requires Redis")
	}

	conn, err := Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		WithLockTTL(time.Second),
	)
	require.NoError(t, err)
	c := NewString[string](conn, StringCodec{})

	ctx := context.Background()
	key := "conn:oneshot:" + uuid.NewString()
	got, err := c.Get(ctx, time.Second, key, func(context.Context, string) (string, error) {
		return "v", nil
	})
	require.NoError(t, err)
	require.Equal(t, "v", got)

	conn.Close()
	conn.Close() // idempotent
}
