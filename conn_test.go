package redcache

import (
	"bytes"
	"context"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// Open builds a Conn; New and NewBytes derive caches that share its engine
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

	values := New(conn, JSONCodec{})
	strs := New(conn, StringCodec{})

	require.Same(t, conn.core, values.core, "cache must share the Conn engine")
	require.Same(t, conn.core, strs.core, "caches must share one engine")

	// nil codecs panic, they are not rejected with an error.
	require.Panics(t, func() { NewKeyed(conn, nil, JSONCodec{}) }, "New must panic on nil key codec")
	require.Panics(t, func() { New(conn, nil) }, "New must panic on nil value codec")

	// Both views are usable over the shared engine.
	ctx := t.Context()
	sKey := "conn:str:" + uuid.New().String()
	iKey := "conn:int:" + uuid.New().String()

	gotS, err := values.Get(ctx, time.Second, sKey, func(context.Context, string) (string, error) {
		return "hello", nil
	})
	require.NoError(t, err)
	require.Equal(t, "hello", gotS)

	gotI, err := values.Get(ctx, time.Second, iKey, func(context.Context, string) (int, error) {
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
	c := New(conn, StringCodec{})

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
// forever. Go 1.27's goroutineleak profile finds goroutines blocked on
// primitives no runnable goroutine can reach; we only fail on stacks that pass
// through this package so unrelated leaks elsewhere in the process are ignored.
func TestConn_Close_LeaksNoGoroutines(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Redis")
	}

	conn, err := Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		WithLockTTL(2*time.Second),
		WithRefreshAfterFraction(0.5),
	)
	require.NoError(t, err)

	cache := New(conn, StringCodec{})
	key := "leak:" + uuid.New().String()
	_, err = cache.Get(t.Context(), time.Minute, key, func(context.Context, string) (string, error) {
		return "v", nil
	})
	require.NoError(t, err)
	conn.Close()

	var buf bytes.Buffer
	require.NoError(t, pprof.Lookup("goroutineleak").WriteTo(&buf, 1))
	for stack := range strings.SplitSeq(buf.String(), "\n\n") {
		if strings.Contains(stack, "github.com/dcbickfo/redcache.") && !strings.Contains(stack, "_test.go") {
			t.Fatalf("leaked goroutine after Close:\n%s", stack)
		}
	}
}
