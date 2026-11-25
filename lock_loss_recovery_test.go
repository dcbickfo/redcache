package redcache_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

const recoveryLockTTL = 30 * time.Second
const reloadedValue = "reloaded"

func openLockRecoveryCache(t *testing.T, opts ...redcache.Option) (*redcache.Cache, *redcache.Conn) {
	t.Helper()
	skipInShortMode(t)
	conn, err := redcache.Open(rueidis.ClientOption{InitAddress: addr},
		append([]redcache.Option{redcache.WithLockTTL(recoveryLockTTL)}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return redcache.New(conn, redcache.StringCodec{}), conn
}

func awaitLockRecoveryTest[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for test operation")
		var zero T
		return zero
	}
}

type recoveryGetResult struct {
	values map[string]string
	err    error
}

func startRecoveryGet(
	ctx context.Context,
	cache *redcache.Cache,
	keys []string,
	multi bool,
	load func(context.Context, []string) (map[string]string, error),
) <-chan recoveryGetResult {
	done := make(chan recoveryGetResult, 1)
	go func() {
		if multi {
			values, err := cache.GetMulti(ctx, time.Minute, keys, load)
			done <- recoveryGetResult{values: values, err: err}
			return
		}
		value, err := cache.Get(ctx, time.Minute, keys[0], func(ctx context.Context, key string) (string, error) {
			values, err := load(ctx, []string{key})
			return values[key], err
		})
		done <- recoveryGetResult{values: map[string]string{keys[0]: value}, err: err}
	}()
	return done
}

func TestLockRecovery_DeletedDuringLoad(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"Get", "GetMulti"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cache, _ := openLockRecoveryCache(t, redcache.WithClientBuilder(func(opt rueidis.ClientOption) (rueidis.Client, error) {
				opt.DisableCache = true
				return rueidis.NewClient(opt)
			}))
			other, _ := openLockRecoveryCache(t)
			key := "lost-lock:" + uuid.New().String()
			keys := []string{key}
			if name == "GetMulti" {
				keys = append(keys, key+":retained")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			started, resume := make(chan struct{}), make(chan struct{})
			var loads [][]string
			done := startRecoveryGet(ctx, cache, keys, name == "GetMulti", func(ctx context.Context, requested []string) (map[string]string, error) {
				loads = append(loads, slices.Clone(requested))
				value := reloadedValue
				if len(loads) == 1 {
					close(started)
					select {
					case <-resume:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					value = "first"
				}
				values := make(map[string]string, len(requested))
				for _, k := range requested {
					values[k] = value
				}
				return values, nil
			})
			awaitLockRecoveryTest(t, started)
			require.NoError(t, other.Del(ctx, key))
			close(resume)

			result := awaitLockRecoveryTest(t, done)
			require.NoError(t, result.err)
			require.NoError(t, ctx.Err())
			require.Equal(t, reloadedValue, result.values[key])
			require.Len(t, loads, 2)
			require.ElementsMatch(t, keys, loads[0])
			require.Equal(t, []string{key}, loads[1])
			if name == "GetMulti" {
				require.Equal(t, "first", result.values[keys[1]])
			}
		})
	}
}

type recoveryWaitGate struct {
	redcache.NoopMetrics
	ctx     context.Context
	waiting chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (g *recoveryWaitGate) LockContended(int64) {
	g.once.Do(func() {
		close(g.waiting)
		select {
		case <-g.resume:
		case <-g.ctx.Done():
		}
	})
}

func TestLockRecovery_FollowerAfterWriteAndDelete(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"Set", "SetMulti"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			gate := &recoveryWaitGate{ctx: ctx, waiting: make(chan struct{}), resume: make(chan struct{})}
			cache, _ := openLockRecoveryCache(t, redcache.WithMetrics(gate),
				redcache.WithClientBuilder(func(opt rueidis.ClientOption) (rueidis.Client, error) {
					// Keep client caching active and withhold engine wakeups.
					opt.OnInvalidations = nil
					return rueidis.NewClient(opt)
				}))
			other, _ := openLockRecoveryCache(t)
			key := "write-lock:" + uuid.New().String()
			require.NoError(t, other.ForceSet(ctx, time.Minute, key, "seed"))
			started, publish := make(chan struct{}), make(chan struct{})
			written := make(chan error, 1)
			write := func(ctx context.Context, _ string) (string, error) {
				close(started)
				select {
				case <-publish:
					return "published", nil
				case <-ctx.Done():
					return "", ctx.Err()
				}
			}
			go func() {
				if name == "Set" {
					written <- cache.Set(ctx, time.Minute, key, write)
					return
				}
				written <- cache.SetMulti(ctx, time.Minute, []string{key}, func(ctx context.Context, keys []string) (map[string]string, error) {
					value, err := write(ctx, keys[0])
					return map[string]string{key: value}, err
				})
			}()
			awaitLockRecoveryTest(t, started)
			done := startRecoveryGet(ctx, cache, []string{key}, false, func(context.Context, []string) (map[string]string, error) {
				return map[string]string{key: reloadedValue}, nil
			})
			awaitLockRecoveryTest(t, gate.waiting)
			close(publish)
			require.NoError(t, awaitLockRecoveryTest(t, written))
			value, found, err := other.Peek[string, string](ctx, time.Minute, key)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, "published", value)
			require.NoError(t, other.Del(ctx, key))
			close(gate.resume)

			result := awaitLockRecoveryTest(t, done)
			require.NoError(t, result.err)
			require.NoError(t, ctx.Err())
			require.Equal(t, reloadedValue, result.values[key])
		})
	}
}
