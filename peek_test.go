package redcache_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

// Peek is a read-only lookup: it never runs a loader or takes a lock. A miss
// returns (_, false, nil); after a Get populates the key, Peek hits.
func TestPeek_MissThenHit(t *testing.T) {
	t.Parallel()
	skipIfNoRedis(t)

	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: addr},
		redcache.WithLockTTL(time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	client := redcache.New(conn, redcache.StringCodec{})

	ctx := t.Context()
	key := "peek:" + uuid.New().String()

	// Miss: no loader runs, no value present.
	v, ok, err := client.Peek[string, string](ctx, time.Second*10, key)
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
	v, ok, err = client.Peek[string, string](ctx, time.Second*10, key)
	require.NoError(t, err)
	require.True(t, ok, "Peek must hit after Get populated the key")
	require.Equal(t, "hello", v)
}

// A key currently holding a lock value reads as a miss through Peek (the lock
// prefix is treated as not-found), and Peek does not run a loader to fill it.
func TestPeek_LockValueReadsAsMiss(t *testing.T) {
	t.Parallel()
	skipIfNoRedis(t)

	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: addr},
		redcache.WithLockTTL(2*time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	client := redcache.New(conn, redcache.StringCodec{})

	ctx := t.Context()
	key := "peek-lock:" + uuid.New().String()

	// Hold the key under a lock: a long-running Get callback owns the lock while
	// a concurrent Peek observes the lock value.
	inCb := make(chan struct{})
	proceed := make(chan struct{})
	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		defer close(done)
		_, gerr := client.Get(ctx, time.Second*10, key, func(context.Context, string) (string, error) {
			close(inCb)
			<-proceed
			return "real", nil
		})
		errCh <- gerr
	}()

	<-inCb // loader is running; the key currently holds a lock value
	v, ok, err := client.Peek[string, string](ctx, time.Second*10, key)
	require.NoError(t, err)
	require.False(t, ok, "a lock value must read as a miss through Peek")
	require.Empty(t, v)

	close(proceed)
	<-done
	require.NoError(t, <-errCh)
}

func TestCachedReadRetriesOnlyRueidisAbort(t *testing.T) {
	t.Parallel()

	const key = "read:{3560}" // Slot zero matches the fake client's zero-value builder.
	const want = "retried value"
	client := &redcache.ReadTestClient{
		ClientMode: rueidis.ClientModeCluster,
		Single:     redcache.RedisResultForTest(t, redcache.RedisStringMessageForTest(t, want), nil),
	}
	client.Cached = []rueidis.RedisResult{redcache.RedisResultForTest(t, rueidis.RedisMessage{}, rueidis.ErrDoCacheAborted)}
	cache := openReadTestCache(t, client)
	val, hit, err := cache.Peek[string, string](t.Context(), time.Minute, key)
	require.NoError(t, err)
	require.True(t, hit)
	require.Equal(t, want, val)
	require.Equal(t, [][]string{{"GET", key}}, client.Commands)

	sameText := errors.New(rueidis.ErrDoCacheAborted.Error())
	client.Cached[0] = redcache.RedisResultForTest(t, rueidis.RedisMessage{}, sameText)
	_, _, err = cache.Peek[string, string](t.Context(), time.Minute, key)
	require.ErrorIs(t, err, sameText)
	require.Len(t, client.Commands, 1)
}

func TestCachedReadFallbackRequiresCluster(t *testing.T) {
	t.Parallel()

	const key = "read:{3560}"
	const want = "fallback value"
	abortErr := fmt.Errorf("cache transaction: %w", rueidis.ErrDoCacheAborted)
	abort := redcache.RedisResultForTest(t, rueidis.RedisMessage{}, abortErr)
	plain := redcache.RedisResultForTest(t, redcache.RedisStringMessageForTest(t, want), nil)
	for _, mode := range []rueidis.ClientMode{rueidis.ClientModeStandalone, rueidis.ClientModeSentinel, rueidis.ClientModeCluster} {
		t.Run(string(mode), func(t *testing.T) {
			client := &redcache.ReadTestClient{
				ClientMode: mode,
				Cached:     []rueidis.RedisResult{abort},
				Plain:      []rueidis.RedisResult{plain},
				Single:     plain,
			}
			cache := openReadTestCache(t, client)
			got, hit, singleErr := cache.Peek[string, string](t.Context(), time.Minute, key)
			results, multiErr := cache.GetMulti(t.Context(), time.Minute, []string{key}, unexpectedReadLoader(t))
			if mode == rueidis.ClientModeCluster {
				require.NoError(t, singleErr)
				require.NoError(t, multiErr)
				require.True(t, hit)
				require.Equal(t, want, got)
				require.Equal(t, map[string]string{key: want}, results)
				require.Equal(t, [][]string{{"GET", key}}, client.Commands)
				require.Equal(t, [][][]string{{{"GET", key}}}, client.MultiCommands)
				return
			}
			assert.Empty(t, got)
			assert.Empty(t, results)
			assert.Empty(t, client.Commands)
			assert.Empty(t, client.MultiCommands)
			require.Error(t, singleErr)
			require.Error(t, multiErr)
			require.Same(t, abortErr, errors.Unwrap(singleErr))
			require.Same(t, abortErr, errors.Unwrap(multiErr))
		})
	}
}

func TestCachedReadPreservesFallbackError(t *testing.T) {
	t.Parallel()

	const key = "read:{3560}"
	const serverError = "OOM command not allowed"
	abort := redcache.RedisResultForTest(t, rueidis.RedisMessage{}, rueidis.ErrDoCacheAborted)
	errReply := redcache.RedisResultForTest(t, redcache.RedisMessageForTest(t, '-', uint64(len(serverError)), []byte(serverError)), nil)
	client := &redcache.ReadTestClient{
		ClientMode: rueidis.ClientModeCluster,
		Cached:     []rueidis.RedisResult{abort},
		Plain:      []rueidis.RedisResult{errReply},
		Single:     errReply,
	}
	cache := openReadTestCache(t, client)

	_, _, singleErr := cache.Peek[string, string](t.Context(), time.Minute, key)
	_, multiErr := cache.GetMulti(t.Context(), time.Minute, []string{key}, unexpectedReadLoader(t))
	for name, err := range map[string]error{"single": singleErr, "batch": multiErr} {
		t.Run(name, func(t *testing.T) {
			var redisErr *rueidis.RedisError
			require.ErrorAs(t, err, &redisErr)
			require.EqualError(t, redisErr, serverError)
			require.ErrorIs(t, err, rueidis.ErrDoCacheAborted)
		})
	}
	require.Len(t, client.Commands, 1)
	require.Len(t, client.MultiCommands, 1)
}

func openReadTestCache(t *testing.T, client *redcache.ReadTestClient) *redcache.Cache {
	t.Helper()
	conn, err := redcache.Open(rueidis.ClientOption{InitAddress: addr}, redcache.WithClientBuilder(
		func(rueidis.ClientOption) (rueidis.Client, error) {
			return client, nil
		},
	))
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return redcache.New(conn, redcache.StringCodec{})
}

func unexpectedReadLoader(t *testing.T) func(context.Context, []string) (map[string]string, error) {
	t.Helper()
	return func(context.Context, []string) (map[string]string, error) {
		t.Fatal("loader should not run during a cached read")
		return nil, nil
	}
}
