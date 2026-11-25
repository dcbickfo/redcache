package redcache_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

func TestReadYourWritesAfterForceSetMulti(t *testing.T) {
	t.Run("standalone", func(t *testing.T) {
		var paths readPathCounts
		cache, conn := makeClient(t, addr)
		paths.Observe(conn)
		checkReadYourWrites(t, cache, conn, &paths)
	})
	t.Run("cluster", func(t *testing.T) {
		var paths readPathCounts
		cache, conn := makeClusterClient(t)
		paths.Observe(conn)
		checkReadYourWrites(t, cache, conn, &paths)
	})
}

func TestReadBarrierAfterFill(t *testing.T) {
	for _, operation := range []string{"Get", "GetMulti", "Set", "SetMulti"} {
		t.Run(operation, func(t *testing.T) {
			cache, conn := makeClient(t, addr)
			var paths readPathCounts
			paths.Observe(conn)
			ctx := t.Context()
			key := "barrier-fill:" + uuid.New().String()
			require.NoError(t, changeMarkedValue(ctx, cache, operation, key))
			for range 4 {
				got, hit, err := cache.Peek[string, string](ctx, time.Minute, key)
				require.NoError(t, err)
				require.True(t, hit)
				require.Equal(t, "value", got)
			}
			require.EqualValues(t, 1, paths.Bypasses.Load(), "one plain GET should end the mark")
			require.GreaterOrEqual(t, paths.CacheHits.Load(), int64(2))
		})
	}
}

func checkReadYourWrites(t *testing.T, cache *redcache.Cache, conn *redcache.Conn, paths *readPathCounts) {
	t.Helper()
	const workers, iterations, nkeys = 4, 5000, 64
	var reads, stale atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			ctx := t.Context()
			keys := make([]string, nkeys)
			for i := range keys {
				keys[i] = "stale-read:" + uuid.New().String()
			}

			for i := range iterations {
				if err := runStaleReadIteration(ctx, cache, keys, i, &reads, &stale); err != nil {
					t.Error(err)
					return
				}
			}
			// Keep the measured workload at one read per key-write. Once it
			// ends, read again to prove that the local cache is being reused.
			for range 2 {
				_, hit, err := cache.Peek[string, string](ctx, time.Minute, keys[0])
				if err != nil || !hit {
					t.Errorf("post-workload read of %q: hit=%t, error=%v", keys[0], hit, err)
				}
			}
		})
	}
	wg.Wait()

	stats := redcache.WriteMarkStatsForTest(conn)
	// Include writes that reused an existing mark in the denominator so a hot
	// key that stays marked across many writes cannot inflate the share.
	cleared := stats.Invalidated + stats.Barrier
	total := max(int64(workers*iterations*nkeys), cleared+stats.Expired+stats.AtCap+stats.Reset+stats.Live)
	t.Logf("reads=%d stale=%d cache_hits=%d bypass_reads=%d invalidated=%d barrier=%d expired=%d cap=%d reset=%d live=%d",
		reads.Load(), stale.Load(), paths.CacheHits.Load(), paths.Bypasses.Load(), stats.Invalidated, stats.Barrier, stats.Expired, stats.AtCap, stats.Reset, stats.Live)
	require.EqualValues(t, workers*iterations*nkeys, reads.Load())
	if stale.Load() != 0 {
		t.Errorf("read %d stale values after acknowledged writes", stale.Load())
	}
	if paths.Bypasses.Load() == 0 {
		t.Error("expected plain bypass reads")
	}
	require.Positive(t, paths.CacheHits.Load(), "reads must return to rueidis's local cache")
	require.Zero(t, stats.Expired, "the workload must clear marks before the deadline")
	if 100*cleared < 99*total {
		t.Errorf("expected invalidations and barriers to end at least 99%% of marks, got %d/%d", cleared, total)
	}
}

func runStaleReadIteration(ctx context.Context, cache *redcache.Cache, keys []string, iteration int, reads, stale *atomic.Int64) error {
	const ttl = time.Minute
	want := valuesFor(keys, strconv.Itoa(iteration))
	if err := cache.ForceSetMulti(ctx, ttl, want); err != nil {
		return err
	}
	got, err := cache.GetMulti(ctx, ttl, keys, func(context.Context, []string) (map[string]string, error) {
		return nil, errors.New("loader ran after an acknowledged ForceSetMulti")
	})
	if err != nil {
		return err
	}
	for _, key := range keys {
		if got[key] != want[key] {
			stale.Add(1)
		}
	}
	reads.Add(int64(len(keys)))
	return nil
}

func TestWriteMarkDeadlineAndFailure(t *testing.T) {
	var marks redcache.WriteMarksForTest
	now := time.Now()
	finish := marks.Begin([]string{"failed"}, now, 10)
	marks.Invalidate("failed")
	finish(now, false)
	marks.Invalidate("failed")
	require.True(t, marks.Bypass("failed", now.Add(time.Second-time.Nanosecond)))
	require.False(t, marks.Bypass("failed", now.Add(time.Second)))
	require.Equal(t, redcache.WriteMarkCountsForTest{Expired: 1}, marks.Counts())

	marks.Begin([]string{"unread"}, now, 10)(now, true)
	marks.Begin([]string{"new"}, now.Add(time.Second), 10)(now.Add(time.Second), true)
	require.Equal(t, redcache.WriteMarkCountsForTest{Expired: 2, Live: 1}, marks.Counts())
}

func TestWriteMarkOverlap(t *testing.T) {
	var marks redcache.WriteMarksForTest
	now := time.Now()
	first := marks.Begin([]string{"key"}, now, 10)
	second := marks.Begin([]string{"key"}, now, 10)
	marks.Invalidate("key")
	first(now, true)
	require.True(t, marks.Bypass("key", now))
	require.EqualValues(t, 1, marks.Counts().Live)

	// The push belonged to the first write. The second key was no longer
	// registered for client-side caching, so its write sends no push.
	second(now, true)
	require.True(t, marks.Bypass("key", now))
	require.False(t, marks.Bypass("key", now.Add(time.Second)))
	require.Equal(t, redcache.WriteMarkCountsForTest{Expired: 1}, marks.Counts())

	// A push for an ordinary single write can arrive before its reply.
	third := marks.Begin([]string{"key"}, now, 10)
	marks.Invalidate("key")
	require.True(t, marks.Bypass("key", now))
	third(now, true)
	require.False(t, marks.Bypass("key", now))
	require.Equal(t, redcache.WriteMarkCountsForTest{Invalidated: 1, Expired: 1}, marks.Counts())
}

func TestWriteMarkCap(t *testing.T) {
	var marks redcache.WriteMarksForTest
	now := time.Now()
	first := marks.Begin([]string{"first"}, now, 1)
	second := marks.Begin([]string{"second", "third"}, now, 1)
	require.Equal(t, redcache.WriteMarkCountsForTest{AtCap: 2, Live: 1}, marks.Counts())

	late := now.Add(2 * time.Second)
	require.True(t, marks.Bypass("unrelated", late), "overflowed writes are still in flight")
	first(now, true)
	second(late, true)
	require.True(t, marks.Bypass("unrelated", late.Add(time.Second-time.Nanosecond)))
	require.False(t, marks.Bypass("unrelated", late.Add(time.Second)))
	require.False(t, marks.Bypass("first", late.Add(time.Second)))
	require.Equal(t, redcache.WriteMarkCountsForTest{Expired: 1, AtCap: 2}, marks.Counts())
}

func TestWriteMarkNilInvalidation(t *testing.T) {
	var marks redcache.WriteMarksForTest
	now := time.Now()
	finish := marks.Begin([]string{"in-flight", "another-slot"}, now, 10)
	marks.Reset(now)
	require.Equal(t, redcache.WriteMarkCountsForTest{Reset: 2}, marks.Counts())

	// A nil callback can come from one node. Other nodes' active writes stay
	// covered even though the per-key table has been cleared.
	late := now.Add(2 * time.Second)
	require.True(t, marks.Bypass("another-slot", late))
	finish(late, true)
	require.False(t, marks.Bypass("another-slot", late.Add(time.Second)))
	require.Zero(t, marks.Counts().Live)
}

func TestWriteMarkBypassKeepsWaitEntry(t *testing.T) {
	for _, multi := range []bool{false, true} {
		t.Run(map[bool]string{false: "Get", true: "GetMulti"}[multi], func(t *testing.T) {
			var paths readPathCounts
			cache, conn := makeClient(t, addr)
			paths.Observe(conn)
			ctx := t.Context()
			key := "bypass-wait:" + uuid.New().String()
			require.NoError(t, cache.ForceSet(ctx, time.Minute, key, "fresh"))

			if multi {
				other := "tracked:" + uuid.New().String()
				raw := conn.Client()
				require.NoError(t, raw.Do(ctx, raw.B().Set().Key(other).Value("other").Px(time.Minute).Build()).Error())
				got, err := cache.GetMulti(ctx, time.Minute, []string{other, key}, func(context.Context, []string) (map[string]string, error) {
					return nil, errors.New("loader ran for a present key")
				})
				require.NoError(t, err)
				require.Equal(t, map[string]string{key: "fresh", other: "other"}, got)
			} else {
				got, err := cache.Get(ctx, time.Minute, key, func(context.Context, string) (string, error) {
					return "", errors.New("loader ran for a present key")
				})
				require.NoError(t, err)
				require.Equal(t, "fresh", got)
			}
			require.EqualValues(t, 1, paths.Bypasses.Load())
			require.True(t, redcache.HasWaitEntryForTest(conn, key), "plain bypass GET must not release the tracked wait entry")
		})
	}
}

func TestWriteMarksOnValueChanges(t *testing.T) {
	withoutInvalidations := redcache.WithClientBuilder(func(opt rueidis.ClientOption) (rueidis.Client, error) {
		opt.OnInvalidations = nil
		return rueidis.NewClient(opt)
	})
	for _, name := range []string{"Set", "SetMulti", "ForceSet", "ForceSetMulti", "Del", "DelMulti", "Get", "GetMulti"} {
		t.Run(name, func(t *testing.T) {
			cache, conn := makeClient(t, addr, withoutInvalidations)
			ctx := t.Context()
			key := "write-mark:" + uuid.New().String()
			require.NoError(t, changeMarkedValue(ctx, cache, name, key))
			require.EqualValues(t, 1, redcache.WriteMarkStatsForTest(conn).Live)
		})
	}
}

func changeMarkedValue(ctx context.Context, cache *redcache.Cache, name, key string) error {
	const ttl = time.Minute
	load := func(context.Context, string) (string, error) { return "value", nil }
	values := map[string]string{key: "value"}
	keys := []string{key}
	switch name {
	case "Set":
		return cache.Set(ctx, ttl, key, load)
	case "SetMulti":
		return cache.SetMulti(ctx, ttl, keys, mapLoader(values))
	case "ForceSet":
		return cache.ForceSet(ctx, ttl, key, values[key])
	case "ForceSetMulti":
		return cache.ForceSetMulti(ctx, ttl, values)
	case "Del":
		return cache.Del(ctx, key)
	case "DelMulti":
		return cache.DelMulti(ctx, keys)
	case "Get":
		_, err := cache.Get(ctx, ttl, key, load)
		return err
	case "GetMulti":
		_, err := cache.GetMulti(ctx, ttl, keys, mapLoader(values))
		return err
	default:
		panic("unknown cache operation: " + name)
	}
}

func TestWriteMarkBarrierOrdering(t *testing.T) {
	const key = "ordered"
	marks := redcache.NewWriteMarksForTest(10)
	now := time.Now()
	marks.Begin([]string{key}, now, 10)(now, true)
	oldRead := marks.ReadBarrier(key, 2, now)

	next := marks.Begin([]string{key}, now, 10)
	oldRead()
	require.True(t, marks.Bypass(key, now), "another write is in flight")
	readDuringWrite := marks.ReadBarrier(key, 2, now)
	next(now, true)

	oldRead()
	readDuringWrite()
	require.True(t, marks.Bypass(key, now), "both GETs preceded the last acknowledgement")
	marks.ReadBarrier(key, 2, now)()
	require.Equal(t, redcache.WriteMarkCountsForTest{Barrier: 1}, marks.Counts())
}

func TestWriteMarkBarrierAfterResetOrRedial(t *testing.T) {
	const key = "reset"
	for _, name := range []string{"nil invalidation", "redial before nil callback"} {
		t.Run(name, func(t *testing.T) {
			now := time.Now()
			marks := redcache.NewWriteMarksForTest(10)
			marks.Begin([]string{key}, now, 10)(now, true)
			oldRead := marks.ReadBarrier(key, 2, now)
			if name == "nil invalidation" {
				marks.Reset(now)
			} else {
				marks.Redial()
			}
			oldRead()
			require.True(t, marks.Bypass(key, now))
			require.Zero(t, marks.Counts().Barrier)
			require.False(t, marks.Bypass(key, now.Add(time.Second)))
		})
	}
}

func TestWriteMarkBarrierBlockingPipeline(t *testing.T) {
	now := time.Now()
	const key = "pooled"
	marks := redcache.NewWriteMarksForTest(2)
	marks.Begin([]string{key, "second-key"}, now, 10)(now, true)
	marks.ReadBarrier(key, 1, now)()
	require.True(t, marks.Bypass(key, now), "the write used the pool")
	marks.Begin([]string{key}, now, 10)(now, true)
	marks.ReadBarrier(key, 2, now)()
	require.True(t, marks.Bypass(key, now), "the GET used the pool")
	marks.ReadBarrier(key, 1, now)()
	require.False(t, marks.Bypass(key, now))
}

func TestReadBarrierFIFO(t *testing.T) {
	for _, mode := range []string{"standalone", "cluster"} {
		t.Run(mode, func(t *testing.T) {
			const iterations = 2000
			ctx := t.Context()
			key := "barrier-fifo:" + uuid.New().String()
			var pushes atomic.Int64
			opt := redcache.WithClientBuilder(func(opt rueidis.ClientOption) (rueidis.Client, error) {
				opt.OnInvalidations = func(messages []rueidis.RedisMessage) {
					for _, msg := range messages {
						if k, err := msg.ToString(); err == nil && k == key {
							pushes.Add(1)
						}
					}
				}
				return rueidis.NewClient(opt)
			})
			var conn *redcache.Conn
			if mode == "cluster" {
				_, conn = makeClusterClient(t, opt)
			} else {
				_, conn = makeClient(t, addr, opt)
			}
			client := conn.Client()
			require.NoError(t, client.Do(ctx, client.B().Set().Key(key).Value("initial").Px(time.Minute).Build()).Error())
			first := trackedGET(t, client, key, "initial")
			for i := range iterations {
				value := strconv.Itoa(i)
				replies := client.DoMulti(ctx,
					client.B().ClientId().Build().SetSlot(key),
					client.B().Set().Key(key).Value(value).Px(time.Minute).Build(),
				)
				writerID, err := replies[0].AsInt64()
				require.NoError(t, err)
				require.NoError(t, replies[1].Error())
				readerID := trackedGET(t, client, key, value)
				require.Equal(t, first, writerID)
				require.Equal(t, writerID, readerID)
				require.EqualValues(t, i+1, pushes.Load(), "push must precede the post-ack GET reply")
			}
			t.Logf("same-pipe post-ack GETs with preceding pushes = %d", pushes.Load())
		})
	}
}

func trackedGET(t *testing.T, client rueidis.Client, key, want string) int64 {
	t.Helper()
	replies := client.DoMulti(t.Context(),
		client.B().ClientId().Build().SetSlot(key),
		client.B().ClientCaching().Yes().Build().SetSlot(key),
		client.B().Get().Key(key).Build(),
	)
	id, err := replies[0].AsInt64()
	require.NoError(t, err)
	require.NoError(t, replies[1].Error())
	value, err := replies[2].ToString()
	require.NoError(t, err)
	require.Equal(t, want, value)
	return id
}

func TestReadBarrierIneligibleConnections(t *testing.T) {
	cases := []struct {
		name string
		opt  rueidis.ClientOption
	}{
		{name: "NOLOOP", opt: rueidis.ClientOption{ClientTrackingOptions: []string{"OPTIN", "NOLOOP"}}},
		{name: "BCAST", opt: rueidis.ClientOption{ClientTrackingOptions: []string{"BCAST"}}},
		{name: "OPTOUT", opt: rueidis.ClientOption{ClientTrackingOptions: []string{"OPTOUT"}}},
		{name: "cache disabled", opt: rueidis.ClientOption{DisableCache: true}},
		{name: "pipelining disabled", opt: rueidis.ClientOption{DisableAutoPipelining: true}},
		{name: "pooled GET", opt: rueidis.ClientOption{BlockingPipeline: 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			skipIfNoRedis(t)
			tc.opt.InitAddress = addr
			conn, err := redcache.Open(tc.opt)
			require.NoError(t, err)
			t.Cleanup(conn.Close)
			assertNoReadBarrier(t, redcache.New(conn, redcache.StringCodec{}), conn)
		})
	}
	t.Run("multiplexed builder", func(t *testing.T) {
		builder := redcache.WithClientBuilder(func(opt rueidis.ClientOption) (rueidis.Client, error) {
			opt.PipelineMultiplex = 2
			return rueidis.NewClient(opt)
		})
		cache, conn := makeClient(t, addr, builder)
		assertNoReadBarrier(t, cache, conn)
	})
}

func assertNoReadBarrier(t *testing.T, cache *redcache.Cache, conn *redcache.Conn) {
	t.Helper()
	ctx := t.Context()
	key := "barrier-config:" + uuid.New().String()
	require.NoError(t, cache.ForceSet(ctx, time.Minute, key, "value"))
	value, hit, err := cache.Peek[string, string](ctx, time.Minute, key)
	require.NoError(t, err)
	require.True(t, hit)
	require.Equal(t, "value", value)
	require.Zero(t, redcache.WriteMarkStatsForTest(conn).Barrier)
}

func TestReadBarrierErroredGET(t *testing.T) {
	cache, conn := makeClient(t, addr)
	raw := conn.Client()
	ctx := t.Context()
	key := "barrier-error:" + uuid.New().String()
	require.NoError(t, cache.ForceSet(ctx, time.Minute, key, "value"))
	require.NoError(t, raw.Do(ctx, raw.B().Del().Key(key).Build()).Error())
	require.NoError(t, raw.Do(ctx, raw.B().Lpush().Key(key).Element("list").Build()).Error())
	_, _, err := cache.Peek[string, string](ctx, time.Minute, key)
	require.ErrorContains(t, err, "WRONGTYPE")
	require.Equal(t, redcache.WriteMarkCountsForTest{Live: 1}, redcache.WriteMarkStatsForTest(conn))
}

func TestUncachedWriteReadDeleteRefill(t *testing.T) {
	for _, mode := range []string{"DisableCache", "RESP2", "builder"} {
		t.Run(mode, func(t *testing.T) {
			skipIfNoRedis(t)
			opt := rueidis.ClientOption{InitAddress: addr, DisableCache: mode != "builder", AlwaysRESP2: mode == "RESP2"}
			metrics := &capturingMetrics{}
			opts := []redcache.Option{redcache.WithMetrics(metrics)}
			if mode == "builder" {
				opts = append(opts, redcache.WithClientBuilder(func(opt rueidis.ClientOption) (rueidis.Client, error) {
					opt.DisableCache = true
					return rueidis.NewClient(opt)
				}))
			}
			conn, err := redcache.Open(opt, opts...)
			require.NoError(t, err)
			t.Cleanup(conn.Close)
			checkUncachedRefill(t, conn, metrics)
			if opt.DisableCache {
				require.Equal(t, redcache.WriteMarkCountsForTest{}, redcache.WriteMarkStatsForTest(conn))
			}
		})
	}
}

func checkUncachedRefill(t *testing.T, conn *redcache.Conn, metrics *capturingMetrics) {
	t.Helper()
	cache := redcache.New(conn, redcache.StringCodec{})
	ctx := t.Context()
	key := "uncached-refill:" + uuid.New().String()
	require.NoError(t, cache.ForceSet(ctx, time.Minute, key, "written"))
	load := func(context.Context, string) (string, error) { return "refilled", nil }
	got, err := cache.Get(ctx, time.Minute, key, load)
	require.NoError(t, err)
	require.Equal(t, "written", got)
	require.False(t, redcache.HasWaitEntryForTest(conn, key), "uncached hit must release its wait entry")
	require.NoError(t, cache.Del(ctx, key))
	got, err = cache.Get(ctx, time.Minute, key, load)
	require.NoError(t, err)
	require.Equal(t, "refilled", got)
	require.Zero(t, metrics.contended.Load(), "the refill must not wait behind its own prior hit")
	require.False(t, redcache.HasWaitEntryForTest(conn, key))
}

func TestPooledBypassReadsReleaseWaitEntries(t *testing.T) {
	cache, conn := makeClient(t, addr)
	ctx := t.Context()
	prefix := "pooled-bypass:" + uuid.New().String() + ":"
	keys := make([]string, rueidis.DefaultBlockingPipeline/2+1)
	for i := range keys {
		keys[i] = prefix + strconv.Itoa(i)
	}
	want := valuesFor(keys, "fresh")
	require.NoError(t, cache.ForceSetMulti(ctx, time.Minute, want))
	got, err := cache.GetMulti(ctx, time.Minute, keys, noLoader)
	require.NoError(t, err)
	require.Equal(t, want, got)
	for _, key := range keys {
		require.False(t, redcache.HasWaitEntryForTest(conn, key), "pooled GET cannot wake this wait entry")
	}
	stats := redcache.WriteMarkStatsForTest(conn)
	require.Zero(t, stats.Barrier, "pooled GET cannot end the mark")
	require.EqualValues(t, len(keys), stats.Live)
}

type readPathCounts struct {
	CacheHits atomic.Int64
	Bypasses  atomic.Int64
}

func (c *readPathCounts) Observe(conn *redcache.Conn) {
	redcache.WrapClientForTest(conn, func(client rueidis.Client) rueidis.Client {
		return &countReadPaths{Client: client, counts: c}
	})
}

type countReadPaths struct {
	rueidis.Client
	counts *readPathCounts
}

func (c *countReadPaths) DoCache(ctx context.Context, cmd rueidis.Cacheable, ttl time.Duration) rueidis.RedisResult {
	result := c.Client.DoCache(ctx, cmd, ttl)
	if result.IsCacheHit() {
		c.counts.CacheHits.Add(1)
	}
	return result
}

func (c *countReadPaths) DoMultiCache(ctx context.Context, cmds ...rueidis.CacheableTTL) []rueidis.RedisResult {
	results := c.Client.DoMultiCache(ctx, cmds...)
	var hits int64
	for _, result := range results {
		if result.IsCacheHit() {
			hits++
		}
	}
	c.counts.CacheHits.Add(hits)
	return results
}

func (c *countReadPaths) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	if cmd.Commands()[0] == "GET" {
		c.counts.Bypasses.Add(1)
	}
	return c.Client.Do(ctx, cmd)
}

func (c *countReadPaths) DoMulti(ctx context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	var reads int64
	for _, cmd := range cmds {
		if cmd.Commands()[0] == "GET" {
			reads++
		}
	}
	if reads != 0 {
		c.counts.Bypasses.Add(reads)
	}
	return c.Client.DoMulti(ctx, cmds...)
}
