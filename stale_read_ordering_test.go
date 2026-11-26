package redcache_test

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
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

// A delayed reader refills an older value after the push for an earlier write
// and before a later write is sent. These tests hold the later write's push in
// the cache store, before the pipe evicts the older value, and read after the
// write is acknowledged. A Conn's own write replies before its push, so one
// writer's back-to-back writes reproduce the interleaving of two writers.
func TestStandaloneNoStaleReadThroughBarrier(t *testing.T) {
	for _, tc := range []struct {
		name       string
		sameWriter bool
	}{
		{name: "two writers"},
		{name: "back-to-back writes", sameWriter: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			barrier := replayDelayedPush(t, tc.sameWriter, false)
			require.Equal(t, "V2", barrier.value, "stale value after an acknowledged write")
			require.Equal(t, [][]string{{"CLIENT", "CACHING", "YES"}, {"GET", barrier.key}}, barrier.commands)
			require.Equal(t, redcache.WriteMarkCountsForTest{Barrier: 1}, barrier.marks)

			// Without the barrier, the earlier write's push retires the mark and
			// the read is a local hit on the older value. This is the residual of
			// Conns that have no barrier, such as cluster.
			off := replayDelayedPush(t, tc.sameWriter, true)
			require.Equal(t, "V1", off.value)
			require.Empty(t, off.commands, "the read dispatched a barrier with the barrier off")
			require.Equal(t, redcache.WriteMarkCountsForTest{Invalidated: 1}, off.marks)
		})
	}
}

type delayedPushOutcome struct {
	key      string
	value    string
	commands [][]string
	marks    redcache.WriteMarkCountsForTest
}

type peekResult struct {
	value string
	hit   bool
	err   error
}

func replayDelayedPush(t *testing.T, sameWriter, barrierOff bool) delayedPushOutcome {
	t.Helper()
	skipIfNoRedis(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	const ttl = time.Minute
	key := "delayed-push:" + uuid.New().String()
	pushes := newHeldPushes(key)
	defer pushes.releaseAll()
	conn, err := redcache.Open(rueidis.ClientOption{
		InitAddress: addr,
		NewCacheStoreFn: func(rueidis.CacheStoreOption) rueidis.CacheStore {
			return &heldPushStore{CacheStore: rueidis.NewSimpleCacheAdapter(testCacheStore{}), pushes: pushes}
		},
	})
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	if barrierOff {
		redcache.DisableReadBarrierForTest(conn)
	}
	cache := redcache.New(conn, redcache.StringCodec{})
	raw := conn.Client()
	require.NoError(t, raw.Do(ctx, raw.B().Set().Key(key).Value("V0").Px(ttl).Build()).Error())
	requirePeekValue(t, ctx, cache, key, "V0")

	client := &delayedPushClient{
		Client:    raw,
		readReady: make(chan struct{}), readResume: make(chan struct{}),
		writeReady: make(chan struct{}), writeResume: make(chan struct{}),
		dispatched: make(chan [][]string, 4),
	}
	redcache.WrapClientForTest(conn, func(rueidis.Client) rueidis.Client { return client })

	// The reader chooses the cached path, then stalls before it contacts Redis.
	client.holdRead.Store(true)
	read := make(chan peekResult, 1)
	go func() {
		v, hit, err := cache.Peek[string, string](ctx, ttl, key)
		read <- peekResult{v, hit, err}
	}()
	awaitLockRecoveryTest(t, client.readReady)
	if sameWriter {
		require.NoError(t, cache.ForceSet(ctx, ttl, key, "V1"))
	}
	// The V2 write is marked and in flight, but not yet sent.
	client.holdWrite.Store(true)
	write := make(chan error, 1)
	go func() { write <- cache.ForceSet(ctx, ttl, key, "V2") }()
	awaitLockRecoveryTest(t, client.writeReady)
	if !sameWriter {
		other, _ := makeClient(t, addr)
		require.NoError(t, other.ForceSet(ctx, ttl, key, "V1"))
	}
	// The push for V1 is handled while the V2 mark is in flight. The delayed read
	// then caches V1 and re-arms tracking before V2 is sent.
	awaitLockRecoveryTest(t, pushes.reached[0])
	close(pushes.release[0])
	awaitLockRecoveryTest(t, pushes.evicted[0])
	close(client.readResume)
	require.Equal(t, peekResult{"V1", true, nil}, awaitLockRecoveryTest(t, read))
	close(client.writeResume)
	require.NoError(t, awaitLockRecoveryTest(t, write))
	// V2 is acknowledged. Its push is held before the pipe evicts V1.
	awaitLockRecoveryTest(t, pushes.reached[1])

	go func() {
		v, hit, err := cache.Peek[string, string](ctx, ttl, key)
		read <- peekResult{v, hit, err}
	}()
	if barrierOff {
		got := awaitLockRecoveryTest(t, read)
		require.NoError(t, got.err)
		// Record any barrier the read dispatched. It is a local hit, so none.
		var commands [][]string
		select {
		case commands = <-client.dispatched:
		default:
		}
		return delayedPushOutcome{key: key, value: got.value, commands: commands, marks: redcache.WriteMarkStatsForTest(conn)}
	}
	// The barrier reply queues behind V2's push, so the read cannot finish
	// before the pipe evicts V1.
	var commands [][]string
	select {
	case commands = <-client.dispatched:
	case got := <-read:
		t.Fatalf("stale read after an acknowledged write: returned %+v with no barrier dispatched", got)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(pushes.release[1])
	got := awaitLockRecoveryTest(t, read)
	require.NoError(t, got.err)
	// The local cache was cleared before the barrier reply, so a miss refills V2.
	for i := range 2 {
		resp := raw.DoCache(ctx, raw.B().Get().Key(key).Cache(), ttl)
		cached, err := resp.ToString()
		require.NoError(t, err)
		plain, _ := redcache.UnwrapEnvelope(cached)
		require.Equal(t, "V2", plain)
		require.Equal(t, i == 1, resp.IsCacheHit())
	}
	return delayedPushOutcome{key: key, value: got.value, commands: commands, marks: redcache.WriteMarkStatsForTest(conn)}
}

func requirePeekValue(t *testing.T, ctx context.Context, cache *redcache.Cache, key, want string) {
	t.Helper()
	got, hit, err := cache.Peek[string, string](ctx, time.Minute, key)
	require.NoError(t, err)
	require.True(t, hit)
	require.Equal(t, want, got)
}

// heldPushes pauses the first two evictions of one key, before the store
// removes the entry, until the test releases them.
type heldPushes struct {
	key     string
	seen    atomic.Int32
	reached [2]chan struct{} // the pipe holds the push, the old value still cached
	release [2]chan struct{}
	evicted [2]chan struct{} // the store removed the old value
	once    sync.Once
}

func newHeldPushes(key string) *heldPushes {
	p := &heldPushes{key: key}
	for i := range p.reached {
		p.reached[i], p.release[i], p.evicted[i] = make(chan struct{}), make(chan struct{}), make(chan struct{})
	}
	return p
}

func (p *heldPushes) releaseAll() {
	p.once.Do(func() {
		for _, ch := range p.release {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	})
}

type heldPushStore struct {
	rueidis.CacheStore
	pushes *heldPushes
}

func (s *heldPushStore) Delete(messages []rueidis.RedisMessage) {
	held := 0
	for _, msg := range messages {
		if key, err := msg.ToString(); err == nil && key == s.pushes.key {
			if n := int(s.pushes.seen.Add(1)); n <= len(s.pushes.reached) {
				held = n
				close(s.pushes.reached[n-1])
				<-s.pushes.release[n-1]
			}
		}
	}
	s.CacheStore.Delete(messages)
	if held != 0 {
		close(s.pushes.evicted[held-1])
	}
}

type delayedPushClient struct {
	rueidis.Client
	holdRead, holdWrite     atomic.Bool
	readReady, readResume   chan struct{}
	writeReady, writeResume chan struct{}
	dispatched              chan [][]string
}

func (c *delayedPushClient) DoCache(ctx context.Context, cmd rueidis.Cacheable, ttl time.Duration) rueidis.RedisResult {
	if c.holdRead.Swap(false) {
		holdUntil(ctx, c.readReady, c.readResume)
	}
	return c.Client.DoCache(ctx, cmd, ttl)
}

func (c *delayedPushClient) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	if cmd.Commands()[0] == "SET" && c.holdWrite.Swap(false) {
		holdUntil(ctx, c.writeReady, c.writeResume)
	}
	return c.Client.Do(ctx, cmd)
}

func (c *delayedPushClient) DoMulti(ctx context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	commands := make([][]string, len(cmds))
	for i, cmd := range cmds {
		commands[i] = slices.Clone(cmd.Commands())
	}
	select {
	case c.dispatched <- commands:
	default:
	}
	return c.Client.DoMulti(ctx, cmds...)
}

func holdUntil(ctx context.Context, ready, resume chan struct{}) {
	close(ready)
	select {
	case <-resume:
	case <-ctx.Done():
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
