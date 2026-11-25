package redcache_test

import (
	"context"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

func TestReadYourWritesBeforeInvalidation(t *testing.T) {
	for _, mode := range []string{"standalone", "cluster"} {
		t.Run(mode, func(t *testing.T) {
			key := "stale-gate:" + uuid.New().String()
			gate := &invalidationGate{key: key, paused: make(chan struct{}), resume: make(chan struct{})}
			var cache *redcache.Cache
			var conn *redcache.Conn
			if mode == "cluster" {
				cache, conn = makeClusterClient(t, gate.option())
			} else {
				skipIfNoRedis(t)
				var err error
				conn, err = redcache.Open(rueidis.ClientOption{InitAddress: addr}, gate.option())
				require.NoError(t, err)
				t.Cleanup(conn.Close)
				cache = redcache.New(conn, redcache.StringCodec{})
			}
			defer gate.release()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			const ttl = time.Minute
			raw := conn.Client()
			require.NoError(t, raw.Do(ctx, raw.B().Set().Key(key).Value("old").Px(ttl).Build()).Error())
			old, err := cache.GetMulti(ctx, ttl, []string{key}, noLoader)
			require.NoError(t, err)
			require.Equal(t, "old", old[key])
			require.NoError(t, cache.ForceSetMulti(ctx, ttl, map[string]string{key: "new"}))
			select {
			case <-gate.paused:
			case <-ctx.Done():
				t.Fatal("the server did not push the primed key's invalidation")
			}
			got, err := cache.GetMulti(ctx, ttl, []string{key}, noLoader)
			require.NoError(t, err)
			require.Equal(t, "new", got[key], "stale value after an acknowledged write")
		})
	}
}

type invalidationGate struct {
	key        string
	paused     chan struct{}
	resume     chan struct{}
	pauseOnce  sync.Once
	resumeOnce sync.Once
}

func (g *invalidationGate) release() { g.resumeOnce.Do(func() { close(g.resume) }) }

func (g *invalidationGate) option() redcache.Option {
	return redcache.WithClientBuilder(func(opt rueidis.ClientOption) (rueidis.Client, error) {
		opt.NewCacheStoreFn = func(rueidis.CacheStoreOption) rueidis.CacheStore {
			return &pausedInvalidationStore{CacheStore: rueidis.NewSimpleCacheAdapter(testCacheStore{}), gate: g}
		}
		client, err := rueidis.NewClient(opt)
		if err != nil {
			return nil, err
		}
		return &releaseOnPlainGET{Client: client, gate: g}, nil
	})
}

type pausedInvalidationStore struct {
	rueidis.CacheStore
	gate *invalidationGate
}

func (s *pausedInvalidationStore) Delete(messages []rueidis.RedisMessage) {
	for _, msg := range messages {
		if key, err := msg.ToString(); err == nil && key == s.gate.key {
			s.gate.pauseOnce.Do(func() { close(s.gate.paused) })
			<-s.gate.resume
		}
	}
	s.CacheStore.Delete(messages)
}

// testCacheStore is accessed under the rueidis adapter's lock.
type testCacheStore map[string]rueidis.RedisMessage

func (s testCacheStore) Get(key string) rueidis.RedisMessage      { return s[key] }
func (s testCacheStore) Set(key string, val rueidis.RedisMessage) { s[key] = val }
func (s testCacheStore) Del(key string)                           { delete(s, key) }
func (s testCacheStore) Flush()                                   { clear(s) }

type releaseOnPlainGET struct {
	rueidis.Client
	gate *invalidationGate
}

func (c *releaseOnPlainGET) DoMulti(ctx context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	for _, cmd := range cmds {
		if cmd.Commands()[0] == "GET" {
			c.gate.release()
			break
		}
	}
	return c.Client.DoMulti(ctx, cmds...)
}
