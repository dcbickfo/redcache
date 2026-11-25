// This file stays in package redcache: it drives unexported engine state (wait entries, poll and refresh scheduling) that no public API reaches.
package redcache

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type readTestClient struct {
	rueidis.Client
	ClientMode    rueidis.ClientMode
	Commands      [][]string
	MultiCommands [][][]string
	Cached        []rueidis.RedisResult
	Plain         []rueidis.RedisResult
	Single        rueidis.RedisResult
}

func (*readTestClient) Close() {}

func (*readTestClient) B() rueidis.Builder { return rueidis.Builder{} }

func (c *readTestClient) Mode() rueidis.ClientMode { return c.ClientMode }

func (c *readTestClient) Do(_ context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	c.Commands = append(c.Commands, slices.Clone(cmd.Commands()))
	return c.Single
}

func (c *readTestClient) DoCache(context.Context, rueidis.Cacheable, time.Duration) rueidis.RedisResult {
	return c.Cached[0]
}

func (c *readTestClient) DoMultiCache(context.Context, ...rueidis.CacheableTTL) []rueidis.RedisResult {
	return slices.Clone(c.Cached)
}

func (c *readTestClient) DoMulti(_ context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	batch := make([][]string, len(cmds))
	for i, cmd := range cmds {
		batch[i] = slices.Clone(cmd.Commands())
	}
	c.MultiCommands = append(c.MultiCommands, batch)
	return c.Plain
}

func TestTrackedHitKeepsWaitEntry(t *testing.T) {
	t.Parallel()

	const key = "read:{3560}"
	const want = "tracked value"
	expiry := time.Now().Add(time.Minute).UnixMilli()
	message := redisStringMessage(t, want)
	encoded := message.CacheMarshal(nil)
	expiryBytes := binary.LittleEndian.AppendUint64(nil, uint64(expiry))
	copy(encoded[:7], expiryBytes[:7])
	require.NoError(t, message.CacheUnmarshalView(encoded))
	reply := redisResult(t, message, nil)
	require.Equal(t, expiry, reply.CachePXAT())

	for _, operation := range []string{"Get", "GetMulti"} {
		t.Run(operation, func(t *testing.T) {
			client := &readTestClient{Cached: []rueidis.RedisResult{reply}}
			rca := &cacheAside{client: client, lockTTL: time.Minute, lockPrefix: "lock:"}
			if operation == "Get" {
				got, err := rca.get(t.Context(), time.Minute, key, nil)
				require.NoError(t, err)
				require.Equal(t, want, got)
			} else {
				got, err := rca.getMulti(t.Context(), time.Minute, []string{key}, nil)
				require.NoError(t, err)
				require.Equal(t, map[string]string{key: want}, got)
			}
			entry, ok := rca.locks.Load(key)
			require.True(t, ok)
			defer rca.release(key, entry.done)
			select {
			case <-entry.done:
				t.Fatal("tracked hit closed its wait channel")
			default:
			}
		})
	}
}

func redisResult(t *testing.T, val rueidis.RedisMessage, err error) rueidis.RedisResult {
	t.Helper()
	var result rueidis.RedisResult
	// Use rueidis's result fields to supply both transport errors and server replies.
	fields := reflect.ValueOf(&result).Elem()
	*(*error)(fields.FieldByName("err").Addr().UnsafePointer()) = err
	*(*rueidis.RedisMessage)(fields.FieldByName("val").Addr().UnsafePointer()) = val
	return result
}

func TestCachedReadBatchFallback(t *testing.T) {
	t.Parallel()

	keys := []string{"read:{3560}:b", "read:{3560}:a", "read:{3560}:d", "read:{3560}:c", "read:{3560}:e"}
	abort := redisResult(t, rueidis.RedisMessage{}, rueidis.ErrDoCacheAborted)
	terminalErr := errors.New("cached read failed")
	terminal := redisResult(t, rueidis.RedisMessage{}, terminalErr)
	value := func(s string) rueidis.RedisResult { return redisResult(t, redisStringMessage(t, s), nil) }
	missing := redisResult(t, redisMessage(t, '_', 0, nil), nil)
	require.True(t, rueidis.IsRedisNil(missing.Error()))
	plain := []rueidis.RedisResult{value("first"), value("second"), missing, value("lock:held")}
	unlocked := slices.Clone(plain)
	unlocked[3] = missing
	cached := []rueidis.RedisResult{abort, value("cached"), abort, abort, abort}
	poll := func(t *testing.T, rca *cacheAside) (any, error) {
		return rca.anyWriteLocked(t.Context(), keys, nil)
	}
	readers := []struct {
		name  string
		plain []rueidis.RedisResult
		want  any
		read  func(*testing.T, *cacheAside) (any, error)
	}{
		{"GetMulti", plain, map[string]string{keys[0]: "first", keys[1]: "cached", keys[2]: "second"},
			func(t *testing.T, rca *cacheAside) (any, error) {
				got := make(map[string]string)
				_, _, err := rca.tryGetMulti(t.Context(), time.Minute, keys, got, nil, nil)
				return got, err
			}},
		{"SetMulti initial lock scan", unlocked, nil,
			func(t *testing.T, rca *cacheAside) (any, error) {
				var owned entryOwner
				defer func() { rca.releaseFinished(owned, true) }()
				return nil, rca.waitForReadLocks(t.Context(), keys, &owned)
			}},
		{"SetMulti lock poll with lock", plain, true, poll},
		{"SetMulti lock poll without lock", unlocked, false, poll},
	}
	for _, reader := range readers {
		t.Run(reader.name, func(t *testing.T) {
			client := &readTestClient{ClientMode: rueidis.ClientModeCluster, Cached: slices.Clone(cached), Plain: reader.plain}
			rca := &cacheAside{client: client, lockPrefix: "lock:", lockTTL: time.Minute, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			got, err := reader.read(t, rca)
			require.NoError(t, err)
			assert.Equal(t, reader.want, got)
			assert.Len(t, client.MultiCommands, 1)
			assert.Equal(t, [][][]string{{{"GET", keys[0]}, {"GET", keys[2]}, {"GET", keys[3]}, {"GET", keys[4]}}}, client.MultiCommands)
			assert.Empty(t, client.Commands)

			client.Cached[1] = terminal
			client.MultiCommands = nil
			client.Commands = nil
			_, err = reader.read(t, rca)
			require.ErrorIs(t, err, terminalErr)
			require.Empty(t, client.MultiCommands)
			require.Empty(t, client.Commands)
		})
	}
}

type invalidationMetrics struct {
	NoopMetrics
	count atomic.Int64
}

func (m *invalidationMetrics) InvalidationError() {
	m.count.Add(1)
}

func TestOnInvalidate_CancelsMatchingLockAndReportsMalformedMessages(t *testing.T) {
	t.Parallel()

	metrics := &invalidationMetrics{}
	rca := &cacheAside{
		lockTTL:        time.Second,
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics:        metrics,
		metricsEnabled: true,
	}

	matching, leader := rca.register("matching")
	require.True(t, leader)
	other, leader := rca.register("other")
	require.True(t, leader)
	t.Cleanup(func() {
		for _, key := range []string{"matching", "other"} {
			if entry, ok := rca.locks.LoadAndDelete(key); ok {
				entry.cancel()
			}
		}
	})

	rca.onInvalidate([]rueidis.RedisMessage{
		redisStringMessage(t, "matching"),
		redisIntMessage(t, 42),
	})

	select {
	case <-matching:
	default:
		t.Fatal("matching lock was not cancelled")
	}
	select {
	case <-other:
		t.Fatal("nonmatching lock was cancelled")
	default:
	}
	require.Equal(t, int64(1), metrics.count.Load())
}

func redisStringMessage(t *testing.T, s string) rueidis.RedisMessage {
	t.Helper()
	return redisMessage(t, '+', uint64(len(s)), []byte(s))
}

func redisIntMessage(t *testing.T, n int64) rueidis.RedisMessage {
	t.Helper()
	return redisMessage(t, ':', uint64(n), nil)
}

func redisMessage(t *testing.T, typ byte, length uint64, payload []byte) rueidis.RedisMessage {
	t.Helper()

	buf := make([]byte, 16+len(payload))
	buf[7] = typ
	binary.BigEndian.PutUint64(buf[8:], length)
	copy(buf[16:], payload)

	var m rueidis.RedisMessage
	require.NoError(t, m.CacheUnmarshalView(buf))
	return m
}

// rueidis signals "everything is invalid" (disconnect, FLUSHALL) with nil.
func TestOnInvalidate_NilCancelsEveryLock(t *testing.T) {
	t.Parallel()
	rca := &cacheAside{lockTTL: time.Minute, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), metrics: NoopMetrics{}}
	a, _ := rca.register("a")
	b, _ := rca.register("b")

	rca.onInvalidate(nil)

	for name, ch := range map[string]<-chan struct{}{"a": a, "b": b} {
		select {
		case <-ch:
		default:
			t.Fatalf("lock %q was not cancelled", name)
		}
		_, ok := rca.locks.Load(name)
		require.False(t, ok, "lock %q still registered", name)
	}
}

// TestConn_SharesEngine checks that both cache views share the Conn's engine
// and can read and write through it.
func TestConn_SharesEngine(t *testing.T) {
	t.Parallel()
	skipInShortMode(t)

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

func TestShouldRefresh_Deterministic(t *testing.T) {
	t.Parallel()
	// Cases that resolve without invoking the XFetch sampler.
	tests := []struct {
		name         string
		refreshAfter float64
		refreshBeta  float64
		cachePTTL    int64
		ttl          time.Duration
		delta        time.Duration
		want         bool
	}{
		{"disabled when refreshAfter is 0", 0, 1, 500, time.Second, time.Millisecond, false},
		{"disabled even with low pttl", 0, 1, 1, time.Second, time.Millisecond, false},
		{"false when cachePTTL is 0", 0.8, 1, 0, time.Second, time.Millisecond, false},
		{"false when cachePTTL is negative", 0.8, 1, -1, time.Second, time.Millisecond, false},
		{"above floor", 0.8, 1, 500, time.Second, time.Millisecond, false},
		{"at exact floor counts as above", 0.8, 1, 200, time.Second, time.Millisecond, false},
		{"above floor at 60% remaining (refreshAfter=0.5)", 0.5, 1, 600, time.Second, time.Millisecond, false},
		{"below floor with delta=0 falls back to always-refresh", 0.8, 1, 100, time.Second, 0, true},
		{"below floor with refreshBeta=0 falls back to always-refresh", 0.8, 0, 100, time.Second, time.Millisecond, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rca := &cacheAside{refreshAfter: tt.refreshAfter, refreshBeta: tt.refreshBeta}
			got := rca.shouldRefresh(tt.cachePTTL, tt.ttl, tt.delta)
			if got != tt.want {
				t.Errorf("shouldRefresh(pttl=%d ttl=%v delta=%v) = %v, want %v",
					tt.cachePTTL, tt.ttl, tt.delta, got, tt.want)
			}
		})
	}
}

// TestShouldRefresh_XFetch covers the probabilistic path (below floor, delta>0,
// beta>0) by asserting refresh rates fall in expected ranges over many trials.
func TestShouldRefresh_XFetch(t *testing.T) {
	t.Parallel()
	const trials = 5000
	rca := &cacheAside{refreshAfter: 0.8, refreshBeta: 1.0}
	ttl := time.Second

	t.Run("near expiry refreshes almost always", func(t *testing.T) {
		t.Parallel()
		// jitter ~ delta * Exp(1) (mean=100ms); P(jitter >= 1ms) ≈ 0.99.
		hits := 0
		for range trials {
			if rca.shouldRefresh(1, ttl, 100*time.Millisecond) {
				hits++
			}
		}
		rate := float64(hits) / trials
		if rate < 0.95 {
			t.Errorf("near-expiry refresh rate = %.3f, want >= 0.95", rate)
		}
	})

	t.Run("just below floor with tiny delta rarely refreshes", func(t *testing.T) {
		t.Parallel()
		// jitter mean = 1ms; P(jitter >= 199ms) ≈ exp(-199) ≈ 0.
		hits := 0
		for range trials {
			if rca.shouldRefresh(199, ttl, time.Millisecond) {
				hits++
			}
		}
		if hits > 5 {
			t.Errorf("tiny-delta refresh count = %d/%d, want ~0", hits, trials)
		}
	})

	t.Run("higher beta increases refresh rate", func(t *testing.T) {
		t.Parallel()
		low := &cacheAside{refreshAfter: 0.8, refreshBeta: 1.0}
		high := &cacheAside{refreshAfter: 0.8, refreshBeta: 10.0}
		var lowHits, highHits int
		for range trials {
			if low.shouldRefresh(50, ttl, 10*time.Millisecond) {
				lowHits++
			}
			if high.shouldRefresh(50, ttl, 10*time.Millisecond) {
				highHits++
			}
		}
		if highHits <= lowHits {
			t.Errorf("expected higher beta to refresh more: low=%d high=%d", lowHits, highHits)
		}
	})
}

func TestAwaitLockOrPoll_PollCanResolveBeforeWaitChannel(t *testing.T) {
	t.Parallel()
	rca := &cacheAside{lockTTL: 100 * time.Millisecond}
	wait := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	polls := 0
	start := time.Now()
	resolved, err := rca.awaitLockOrPoll(ctx, wait, func() (bool, error) {
		polls++
		return true, nil
	})

	if err != nil {
		t.Fatalf("awaitLockOrPoll returned error: %v", err)
	}
	if !resolved {
		t.Fatal("awaitLockOrPoll did not report poll resolution")
	}
	if polls != 1 {
		t.Fatalf("polls = %d, want 1", polls)
	}
	if elapsed := time.Since(start); elapsed >= rca.lockTTL {
		t.Fatalf("poll fallback took %v, want less than lockTTL %v", elapsed, rca.lockTTL)
	}
}

func TestAwaitLockOrPoll_WaitChannelWins(t *testing.T) {
	t.Parallel()
	rca := &cacheAside{lockTTL: time.Second}
	wait := make(chan struct{})
	close(wait)

	resolved, err := rca.awaitLockOrPoll(t.Context(), wait, func() (bool, error) {
		t.Fatal("poll should not run after wait channel closes")
		return false, nil
	})

	if err != nil {
		t.Fatalf("awaitLockOrPoll returned error: %v", err)
	}
	if resolved {
		t.Fatal("awaitLockOrPoll reported poll resolution after wait channel closed")
	}
}

func TestAwaitLockOrPoll_PollError(t *testing.T) {
	t.Parallel()
	rca := &cacheAside{lockTTL: 100 * time.Millisecond}
	wait := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	wantErr := errors.New("poll failed")

	resolved, err := rca.awaitLockOrPoll(ctx, wait, func() (bool, error) {
		return false, wantErr
	})

	if !errors.Is(err, wantErr) {
		t.Fatalf("awaitLockOrPoll error = %v, want %v", err, wantErr)
	}
	if resolved {
		t.Fatal("awaitLockOrPoll reported poll resolution on poll error")
	}
}

func TestAwaitLockMultiOrPoll_PollCanResolveBeforeAllWaitChannels(t *testing.T) {
	t.Parallel()
	rca := &cacheAside{lockTTL: 100 * time.Millisecond}
	wait1 := make(chan struct{})
	wait2 := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	resolved, err := rca.awaitLockMultiOrPoll(ctx, []<-chan struct{}{wait1, wait2}, func() (bool, error) {
		return true, nil
	})

	if err != nil {
		t.Fatalf("awaitLockMultiOrPoll returned error: %v", err)
	}
	if !resolved {
		t.Fatal("awaitLockMultiOrPoll did not report poll resolution")
	}
}

func TestRefreshKeyFor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		prefix string
		key    string
		want   string
	}{
		{"default prefix", "__redcache:refresh:", "user:123", "__redcache:refresh:{user:123}"},
		{"custom prefix", "rc:r:", "abc", "rc:r:{abc}"},
		{"empty key", "p:", "", "p:{}"},
		{"key with hash tag inside", "p:", "user:{tenant}:1", "p:{user:{tenant}:1}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rca := &cacheAside{refreshPrefix: tt.prefix}
			got := rca.refreshKeyFor(tt.key)
			if got != tt.want {
				t.Errorf("refreshKeyFor(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}

func TestShouldRunRefreshJob_DropsDequeuedJobAfterClose(t *testing.T) {
	t.Parallel()

	rca := &cacheAside{refreshDone: make(chan struct{})}
	key := "queued-after-close"
	rca.refreshing.Store(key, struct{}{})
	close(rca.refreshDone)

	if rca.shouldRunRefreshJob(refreshJob{keys: []string{key}}) {
		t.Fatal("refresh job should not run after Close begins")
	}
	if _, ok := rca.refreshing.Load(key); ok {
		t.Fatal("dropped refresh job did not clear local dedup marker")
	}
}

func TestEnqueueRefresh_DropsInsteadOfQueueingAfterClose(t *testing.T) {
	t.Parallel()

	for range 1000 {
		rca := &cacheAside{
			refreshQueue: make(chan refreshJob, 1),
			refreshDone:  make(chan struct{}),
		}
		key := "enqueue-after-close"
		rca.refreshing.Store(key, struct{}{})
		close(rca.refreshDone)

		rca.enqueueRefresh(refreshJob{keys: []string{key}}, []string{key})

		if len(rca.refreshQueue) != 0 {
			t.Fatal("refresh job was queued after Close began")
		}
		if _, ok := rca.refreshing.Load(key); ok {
			t.Fatal("dropped refresh job did not clear local dedup marker")
		}
	}
}

var helperAddr = []string{"127.0.0.1:6379"}

func skipInShortMode(tb testing.TB) {
	tb.Helper()
	if testing.Short() {
		tb.Skip("skipped by -short; without -short this test requires a server and fails if it is down")
	}
}

func newHelperPCA(t *testing.T) *cacheAside {
	t.Helper()
	skipInShortMode(t)
	pca, err := newCacheAside(
		rueidis.ClientOption{InitAddress: helperAddr},
		newConfig(WithLockTTL(2*time.Second)),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		pca.Close()
		pca.Client().Close()
	})
	return pca
}

// touchMultiLocks must keep lost-lock entries in lockValues so the eventual
// CAS-set surfaces ErrLockLost rather than overwriting the stealer's value.
func TestTouchMultiLocks_RetainsLostLocksForCASFailure(t *testing.T) {
	t.Parallel()
	pca := newHelperPCA(t)

	heldKey := "touch:held:" + uuid.New().String()
	lostKey := "touch:lost:" + uuid.New().String()
	heldLock := pca.lockPool.Generate()

	ctx := t.Context()
	ok, _, err := pca.tryAcquireWriteLock(ctx, heldKey, heldLock, "2000")
	require.NoError(t, err)
	require.True(t, ok)
	t.Cleanup(func() { pca.bestEffortUnlock(t.Context(), heldKey, heldLock) })

	lockValues := map[string]string{
		heldKey: heldLock,
		lostKey: "__redcache:lock:nonexistent", // never written to Redis
	}

	pca.touchMultiLocks(ctx, lockValues)

	require.Contains(t, lockValues, heldKey, "real lock should be retained")
	require.Contains(t, lockValues, lostKey, "lost lock retained so CAS-set surfaces ErrLockLost")
}

// waitForFailedKey returns ctx.Err() and triggers restoreMultiValues when the
// caller's ctx expires before the holder releases.
func TestWaitForFailedKey_ContextCancelled(t *testing.T) {
	t.Parallel()
	holder := newHelperPCA(t)
	waiter := newHelperPCA(t)

	key := "waitfail:" + uuid.New().String()
	holderLock := holder.lockPool.Generate()

	ok, _, err := holder.tryAcquireWriteLock(t.Context(), key, holderLock, "2000")
	require.NoError(t, err)
	require.True(t, ok)
	t.Cleanup(func() { holder.bestEffortUnlock(t.Context(), key, holderLock) })

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err = waiter.waitForFailedKey(ctx, key, map[string]string{}, map[string]savedValue{}, new(entryOwner))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
