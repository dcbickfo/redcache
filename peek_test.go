package redcache_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

// Peek is a read-only lookup: it never runs a loader or takes a lock. A miss
// returns (_, false, nil); after a Get populates the key, Peek hits.
func TestPeek_MissThenHit(t *testing.T) {
	t.Parallel()
	skipIfNoRedis(t)

	client, closer, err := redcache.NewString[string](
		rueidis.ClientOption{InitAddress: addr},
		redcache.StringCodec{},
		redcache.WithLockTTL(time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(closer)

	ctx := context.Background()
	key := "peek:" + uuid.New().String()

	// Miss: no loader runs, no value present.
	v, ok, err := client.Peek(ctx, time.Second*10, key)
	require.NoError(t, err)
	require.False(t, ok, "Peek on an absent key must miss")
	require.Empty(t, v)

	// Populate via Get.
	got, err := client.Get(ctx, time.Second*10, key, func(context.Context, string) (string, error) {
		return "hello", nil
	})
	require.NoError(t, err)
	require.Equal(t, "hello", got)

	// Hit: Peek now sees the populated value.
	v, ok, err = client.Peek(ctx, time.Second*10, key)
	require.NoError(t, err)
	require.True(t, ok, "Peek must hit after Get populated the key")
	require.Equal(t, "hello", v)
}

// A key currently holding a lock value reads as a miss through Peek (the lock
// prefix is treated as not-found), and Peek does not run a loader to fill it.
func TestPeek_LockValueReadsAsMiss(t *testing.T) {
	t.Parallel()
	skipIfNoRedis(t)

	client, closer, err := redcache.NewString[string](
		rueidis.ClientOption{InitAddress: addr},
		redcache.StringCodec{},
		redcache.WithLockTTL(2*time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(closer)

	ctx := context.Background()
	key := "peek-lock:" + uuid.New().String()

	// Hold the key under a lock: a long-running Get callback owns the lock while
	// a concurrent Peek observes the lock value.
	inCb := make(chan struct{})
	proceed := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, gerr := client.Get(ctx, time.Second*10, key, func(context.Context, string) (string, error) {
			close(inCb)
			<-proceed
			return "real", nil
		})
		require.NoError(t, gerr)
	}()

	<-inCb // loader is running; the key currently holds a lock value
	v, ok, err := client.Peek(ctx, time.Second*10, key)
	require.NoError(t, err)
	require.False(t, ok, "a lock value must read as a miss through Peek")
	require.Empty(t, v)

	close(proceed)
	<-done
}
