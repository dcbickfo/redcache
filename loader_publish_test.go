package redcache_test

import (
	"context"
	"errors"
	"fmt"
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

// A successful SET NX proves the key was absent at Redis. With NOLOOP, it
// cannot prove this Conn's earlier user value was evicted from its local cache.
func TestLoaderPublishProtectsUninvalidatedValue(t *testing.T) {
	for _, name := range []string{"Get", "GetMulti"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cache, conn := openNoLoopClient(t, redcache.WithLockTTL(5*time.Second))
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			key := "publish-noloop:" + uuid.New().String()
			client := conn.Client()
			const ttl = time.Minute
			require.NoError(t, client.Do(ctx, client.B().Set().Key(key).Value("previous").Px(ttl).Build()).Error())
			require.NoError(t, client.DoCache(ctx, client.B().Get().Key(key).Cache(), ttl).Error())
			cached := client.DoCache(ctx, client.B().Get().Key(key).Cache(), ttl)
			require.NoError(t, cached.Error())
			require.True(t, cached.IsCacheHit(), "prime the earlier local user value")
			require.NoError(t, cache.Del(ctx, key))
			locked, publish := make(chan struct{}), make(chan struct{})
			keys := []string{key}
			if name == "GetMulti" {
				keys = append(keys, key+":second")
			}
			done := startRecoveryGet(ctx, cache, keys, len(keys) > 1, func(ctx context.Context, keys []string) (map[string]string, error) {
				close(locked)
				select {
				case <-publish:
					return filledValues(keys, "reloaded"), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			})
			awaitLockRecoveryTest(t, locked)
			// The loader already acquired the absent key. Let the older Del
			// mark expire while the prior cached user value remains alive.
			expiry := time.NewTimer(time.Second)
			defer expiry.Stop()
			awaitLockRecoveryTest(t, expiry.C)
			close(publish)
			written := awaitLockRecoveryTest(t, done)
			require.NoError(t, written.err)
			require.Equal(t, "reloaded", written.values[key])
			raw, err := client.Do(ctx, client.B().Get().Key(key).Build()).ToString()
			require.NoError(t, err)
			value, _ := redcache.UnwrapEnvelope(raw)
			require.Equal(t, "reloaded", value)

			got, hit, err := cache.Peek[string, string](ctx, ttl, key)
			require.NoError(t, err)
			require.True(t, hit)
			require.Equal(t, "reloaded", got, "stale user value after acknowledged loader publication")
		})
	}
}

type fillLoader func(context.Context, []string) (map[string]string, error)

func runLoaderFill(ctx context.Context, cache *redcache.Cache, keys []string, ttl time.Duration, load fillLoader) (map[string]string, error) {
	if len(keys) != 1 {
		return cache.GetMulti(ctx, ttl, keys, load)
	}
	key := keys[0]
	v, err := cache.Get(ctx, ttl, key, func(ctx context.Context, k string) (string, error) {
		got, err := load(ctx, []string{k})
		return got[k], err
	})
	return map[string]string{key: v}, err
}

func filledValues(keys []string, value string) map[string]string {
	values := make(map[string]string, len(keys))
	for _, key := range keys {
		values[key] = value
	}
	return values
}

type fillWaits struct {
	redcache.NoopMetrics
	contended chan struct{}
}

func (m *fillWaits) LockContended(int64) {
	select {
	case m.contended <- struct{}{}:
	default:
	}
}

type fillEvidence struct {
	fills, reads, stale atomic.Int64
	readerLoads         atomic.Int64 // keys a reader led and loaded because the publisher had not locked them
	maxWait             atomic.Int64
}

// Each worker has one fill goroutine and two concurrent readers using its Conn.
func checkConcurrentLoaderRefills(t *testing.T, mode string) {
	t.Helper()
	const workers = 4
	const rounds = 20
	const batchWidth = 16
	const totalFills int64 = rounds * workers * (1 + batchWidth)
	evidence := &fillEvidence{}
	t.Cleanup(func() {
		t.Logf("fills=%d reader_keys=%d stale=%d reader_loaded_keys=%d max_wait=%s",
			evidence.fills.Load(), evidence.reads.Load(), evidence.stale.Load(), evidence.readerLoads.Load(), time.Duration(evidence.maxWait.Load()))
		if !testing.Short() {
			require.Equal(t, totalFills, evidence.fills.Load())
			require.Equal(t, 2*totalFills, evidence.reads.Load())
			require.Zero(t, evidence.stale.Load())
		}
	})
	for worker := range workers {
		t.Run(strconv.Itoa(worker), func(t *testing.T) {
			t.Parallel()
			waiter := &fillWaits{contended: make(chan struct{}, 4)}
			cache, conn := openFillClient(t, mode, waiter)
			width := batchWidth
			if worker%2 == 0 {
				width = 1
			}
			for round := range rounds {
				keys := fillKeys(width)
				if err := checkLoaderFill(t.Context(), cache, waiter, evidence, keys, "initial"); err != nil {
					t.Fatal(err)
				}
				if err := retireFillKeys(t.Context(), cache, conn.Client(), keys, round%2 != 0); err != nil {
					t.Fatal(err)
				}
				if err := checkLoaderFill(t.Context(), cache, waiter, evidence, keys, "refilled"); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func openFillClient(t *testing.T, mode string, waiter *fillWaits) (*redcache.Cache, *redcache.Conn) {
	t.Helper()
	opts := []redcache.Option{redcache.WithMetrics(waiter)}
	if mode == "cluster" {
		return makeClusterClient(t, opts...)
	}
	// A builder keeps the same real RESP3 client and its invalidations while
	// making the standalone barrier ineligible.
	opts = append(opts, redcache.WithClientBuilder(rueidis.NewClient))
	return makeClient(t, addr, opts...)
}

func fillKeys(width int) []string {
	prefix := "concurrent-fill:" + uuid.New().String() + ":"
	keys := make([]string, width)
	for i := range keys {
		keys[i] = prefix + strconv.Itoa(i)
	}
	return keys
}

func checkLoaderFill(parent context.Context, cache *redcache.Cache, waiter *fillWaits, evidence *fillEvidence, keys []string, want string) error {
	const bound = 2 * time.Second
	ctx, cancel := context.WithTimeout(parent, bound)
	defer cancel()
	for len(waiter.contended) > 0 {
		<-waiter.contended
	}
	locked, publish := make(chan struct{}), make(chan struct{})
	var once sync.Once
	ledger := &loadLedger{loads: make(map[string][]string)}
	load := func(ctx context.Context, requested []string) (map[string]string, error) {
		ledger.record("publisher", requested)
		once.Do(func() { close(locked) })
		select {
		case <-publish:
			return filledValues(requested, want), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	// Name any double load in whichever error ends the phase first.
	fail := func(err error) error {
		if doubled := ledger.doubleLoads(keys); doubled != "" {
			return fmt.Errorf("%w; %s", err, doubled)
		}
		return err
	}
	// A call's loader gets only the keys that call locked, so one caller may
	// legitimately load a key another did not. The invariant is that no key is
	// loaded twice in one phase.
	followerLoad := func(who string) fillLoader {
		return func(_ context.Context, requested []string) (map[string]string, error) {
			ledger.record(who, requested)
			return filledValues(requested, want), nil
		}
	}
	multi := len(keys) > 1
	written := startRecoveryGet(ctx, cache, keys, multi, load)
	select {
	case <-locked:
	case <-ctx.Done():
		return fail(fmt.Errorf("loader did not acquire locks for %q: %w", want, ctx.Err()))
	}
	start := time.Now()
	readers := []<-chan recoveryGetResult{
		startRecoveryGet(ctx, cache, keys, multi, followerLoad("reader0")),
		startRecoveryGet(ctx, cache, keys, multi, followerLoad("reader1")),
	}
	select {
	case <-waiter.contended:
	case <-ctx.Done():
		return fail(fmt.Errorf("follower did not observe a held lock during %q: %w", want, ctx.Err()))
	}
	close(publish)
	if err := checkFillReply(ctx, written, keys, want, nil); err != nil {
		return fail(err)
	}
	for _, reader := range readers {
		if err := checkFillReply(ctx, reader, keys, want, evidence); err != nil {
			return fail(err)
		}
	}
	loaded, err := ledger.checkLoadedOnce(keys, want)
	if err != nil {
		return err
	}
	evidence.fills.Add(int64(loaded))
	evidence.readerLoads.Add(ledger.loadedByReaders())
	observed := int64(time.Since(start))
	for previous := evidence.maxWait.Load(); observed > previous; previous = evidence.maxWait.Load() {
		if evidence.maxWait.CompareAndSwap(previous, observed) {
			break
		}
	}
	return nil
}

// loadLedger records, per key, which callers' loaders were asked to load it
// within one phase.
type loadLedger struct {
	mu    sync.Mutex
	loads map[string][]string
}

func (l *loadLedger) record(who string, keys []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, key := range keys {
		l.loads[key] = append(l.loads[key], who)
	}
}

// doubleLoads describes every key loaded more than once, or "" when none was.
func (l *loadLedger) doubleLoads(keys []string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out string
	for _, key := range keys {
		if who := l.loads[key]; len(who) > 1 {
			out += fmt.Sprintf("key %q loaded %d times, by %v; ", key, len(who), who)
		}
	}
	return out
}

func (l *loadLedger) loadedByReaders() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var n int64
	for _, who := range l.loads {
		if len(who) == 1 && who[0] != "publisher" {
			n++
		}
	}
	return n
}

// checkLoadedOnce fails unless every key was loaded exactly once, by whichever
// caller won its lock. It returns the number of keys loaded.
func (l *loadLedger) checkLoadedOnce(keys []string, want string) (int, error) {
	if doubled := l.doubleLoads(keys); doubled != "" {
		return 0, fmt.Errorf("a key was loaded twice during %q: %s", want, doubled)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, key := range keys {
		if len(l.loads[key]) == 0 {
			return 0, fmt.Errorf("key %q was never loaded during %q", key, want)
		}
	}
	return len(l.loads), nil
}

func checkFillReply(ctx context.Context, done <-chan recoveryGetResult, keys []string, want string, evidence *fillEvidence) error {
	var result recoveryGetResult
	select {
	case result = <-done:
	case <-ctx.Done():
		return fmt.Errorf("fill or follower timed out during %q: %w", want, ctx.Err())
	}
	if result.err != nil {
		return result.err
	}
	for _, key := range keys {
		if evidence != nil {
			evidence.reads.Add(1)
		}
		if got := result.values[key]; got != want {
			if evidence != nil {
				evidence.stale.Add(1)
			}
			return fmt.Errorf("stale value for %q: got %q, want %q", key, got, want)
		}
	}
	return nil
}

func retireFillKeys(ctx context.Context, cache *redcache.Cache, client rueidis.Client, keys []string, expire bool) error {
	if !expire {
		return cache.DelMulti(ctx, keys)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	cmds := make(rueidis.Commands, len(keys))
	for i, key := range keys {
		cmds[i] = client.B().Pexpire().Key(key).Milliseconds(5).Build()
	}
	for _, reply := range client.DoMulti(ctx, cmds...) {
		if err := reply.Error(); err != nil {
			return err
		}
	}
	for {
		cmds = make(rueidis.Commands, len(keys))
		for i, key := range keys {
			cmds[i] = client.B().Get().Key(key).Build()
		}
		absent := 0
		for _, reply := range client.DoMulti(ctx, cmds...) {
			if rueidis.IsRedisNil(reply.Error()) {
				absent++
			} else if err := reply.Error(); err != nil {
				return err
			}
		}
		if absent == len(keys) {
			return nil
		}
	}
}

func openNoLoopClient(t *testing.T, opts ...redcache.Option) (*redcache.Cache, *redcache.Conn) {
	t.Helper()
	builder := redcache.WithClientBuilder(func(opt rueidis.ClientOption) (rueidis.Client, error) {
		opt.ClientTrackingOptions = []string{"OPTIN", "NOLOOP"}
		return rueidis.NewClient(opt)
	})
	return makeClient(t, addr, append(opts, builder)...)
}

func TestRefreshPublishProtectsPreviousValue(t *testing.T) {
	for _, width := range []int{1, 2} {
		t.Run(strconv.Itoa(width), func(t *testing.T) {
			cache, conn := openNoLoopClient(t, redcache.WithRefreshAfterFraction(0.25), redcache.WithRefreshBeta(0))
			ctx := t.Context()
			raw := conn.Client()
			keys := fillKeys(width)
			for _, key := range keys {
				require.NoError(t, raw.Do(ctx, raw.B().Set().Key(key).Value("previous").Px(time.Minute).Build()).Error())
			}
			fill, err := runLoaderFill(ctx, cache, keys, 2*time.Minute, func(_ context.Context, requested []string) (map[string]string, error) {
				return filledValues(requested, "refreshed"), nil
			})
			require.NoError(t, err)
			require.Equal(t, filledValues(keys, "previous"), fill)
			for _, key := range keys {
				require.Eventually(t, func() bool {
					reply, err := raw.Do(ctx, raw.B().Get().Key(key).Build()).ToString()
					value, _ := redcache.UnwrapEnvelope(reply)
					return err == nil && value == "refreshed"
				}, 2*time.Second, time.Millisecond, "refresh must publish to Redis")
				value, hit, err := cache.Peek[string, string](ctx, 2*time.Minute, key)
				require.NoError(t, err)
				require.True(t, hit)
				require.Equal(t, "refreshed", value, "refresh publish must protect the previous cached value")
			}
		})
	}
}

func TestWriteRestoreKeepsMark(t *testing.T) {
	for _, name := range []string{"Set", "SetMulti"} {
		t.Run(name, func(t *testing.T) {
			cache, conn := openNoLoopClient(t)
			ctx := t.Context()
			raw := conn.Client()
			key := "restore-mark:" + uuid.New().String()
			require.NoError(t, raw.Do(ctx, raw.B().Set().Key(key).Value("previous").Px(time.Minute).Build()).Error())
			loadErr := errors.New("rollback the write")
			var err error
			if name == "Set" {
				err = cache.Set(ctx, time.Minute, key, func(context.Context, string) (string, error) {
					return "", loadErr
				})
			} else {
				err = cache.SetMulti(ctx, time.Minute, []string{key}, func(context.Context, []string) (map[string]string, error) {
					return nil, loadErr
				})
			}
			require.ErrorIs(t, err, loadErr)
			value, err := raw.Do(ctx, raw.B().Get().Key(key).Build()).ToString()
			require.NoError(t, err)
			require.Equal(t, "previous", value)
			require.EqualValues(t, 1, redcache.WriteMarkStatsForTest(conn).Live)
		})
	}
}
