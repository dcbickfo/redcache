package redcache

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/lockpool"
	"github.com/dcbickfo/redcache/internal/poolx"
	"github.com/dcbickfo/redcache/internal/syncx"
)

const (
	minLockPollDelay = 25 * time.Millisecond
	maxLockPollDelay = time.Second
)

var (
	cacheableTTLPool = poolx.NewSlice(func() []rueidis.CacheableTTL { return make([]rueidis.CacheableTTL, 0, 16) })
	commandsPool     = poolx.NewSlice(func() []rueidis.Completed { return make([]rueidis.Completed, 0, 16) })
	luaExecPool      = poolx.NewSlice(func() []rueidis.LuaExec { return make([]rueidis.LuaExec, 0, 16) })
	stringPool       = poolx.NewSlice(func() []string { return make([]string, 0, 16) })
	chanPool         = poolx.NewSlice(func() []<-chan struct{} { return make([]<-chan struct{}, 0, 16) })
)

// lockEntry tracks a key's wait channel and its TTL timer. cancel and
// timerExpired each close done exactly once via once. timerExpired, run from
// the timer's own AfterFunc, doesn't touch le.timer: the timer has already
// fired (nothing to Stop), and reading the field could race its assignment in
// register.
type lockEntry struct {
	done  chan struct{}
	once  sync.Once
	timer *time.Timer
}

func (le *lockEntry) cancel() {
	le.once.Do(func() {
		if le.timer != nil {
			le.timer.Stop()
		}
		close(le.done)
	})
}

func (le *lockEntry) timerExpired() {
	le.once.Do(func() {
		close(le.done)
	})
}

// cacheAside is the string-typed cache-aside engine over a rueidis.Client. It
// owns the lock/refresh/pool machinery; the generic Cache[K,V] layer encodes
// K/V and delegates here.
type cacheAside struct {
	client         rueidis.Client
	locks          syncx.ShardedMap[*lockEntry]
	lockPool       *lockpool.Pool
	lockTTL        time.Duration
	lockTTLMs      string // pre-formatted lockTTL.Milliseconds() for Lua args.
	logger         Logger
	metrics        Metrics
	metricsEnabled bool // false when metrics is NoopMetrics{}, gates hot-path emits.
	lockPrefix     string
	refreshAfter   float64                    // 0 = disabled.
	refreshBeta    float64                    // XFetch beta; 0 = simple floor only.
	refreshTimeout time.Duration              // 0 = use the data ttl per call.
	refreshing     syncx.ShardedMap[struct{}] // local dedup of in-flight refreshes.
	refreshPrefix  string
	refreshQueue   chan refreshJob // worker pool job queue (nil when disabled).
	refreshDone    chan struct{}   // closed by Close to signal workers/senders.
	refreshCtx     context.Context
	refreshCancel  context.CancelFunc
	refreshWg      sync.WaitGroup
	closing        atomic.Bool // set true at the start of Close to gate refresh sends.
	closeOnce      sync.Once
}

// newCacheAside builds a cacheAside from a validated/defaulted config and the
// rueidis.ClientOption. It builds the underlying client internally and wires
// OnInvalidations to its own handler.
func newCacheAside(clientOption rueidis.ClientOption, cfg config) (*cacheAside, error) {
	if err := cfg.applyDefaults(clientOption); err != nil {
		return nil, err
	}

	lp, err := lockpool.New(cfg.lockPrefix)
	if err != nil {
		return nil, fmt.Errorf("lock pool: %w", err)
	}
	_, isNoop := cfg.metrics.(NoopMetrics)
	rca := &cacheAside{
		lockPool:       lp,
		lockTTL:        cfg.lockTTL,
		lockTTLMs:      strconv.FormatInt(cfg.lockTTL.Milliseconds(), 10),
		logger:         cfg.logger,
		metrics:        cfg.metrics,
		metricsEnabled: !isNoop,
		lockPrefix:     cfg.lockPrefix,
		refreshAfter:   cfg.refreshAfterFraction,
		refreshBeta:    cfg.refreshBeta,
		refreshTimeout: cfg.refreshTimeout,
		refreshPrefix:  cfg.refreshLockPrefix,
	}
	// PipelineMultiplex=-1: single connection per node so cache reads and the
	// invalidation stream share a pipe. ClientBuilder can override.
	clientOption.PipelineMultiplex = -1
	clientOption.OnInvalidations = rca.onInvalidate

	if cfg.clientBuilder != nil {
		rca.client, err = cfg.clientBuilder(clientOption)
	} else {
		rca.client, err = rueidis.NewClient(clientOption)
	}
	if err != nil {
		return nil, err
	}

	if rca.refreshAfter > 0 {
		//nolint:gosec // refreshCancel is retained on the engine and called by Close.
		rca.refreshCtx, rca.refreshCancel = context.WithCancel(context.Background())
		rca.refreshQueue = make(chan refreshJob, cfg.refreshQueueSize)
		rca.refreshDone = make(chan struct{})
		rca.startRefreshWorkers(cfg.refreshWorkers)
	}

	return rca, nil
}

// Client returns the underlying rueidis.Client. Bypasses cache-aside semantics.
func (rca *cacheAside) Client() rueidis.Client {
	return rca.client
}

// Close cancels pending lock entries, cancels refresh callback contexts, waits
// for in-flight refresh workers, and closes the underlying rueidis.Client.
// Refresh callbacks must observe their context for prompt shutdown. Idempotent.
//
// Shutdown signals workers via refreshDone; closing refreshQueue would race
// concurrent senders.
func (rca *cacheAside) Close() {
	rca.closeOnce.Do(func() {
		rca.closing.Store(true)
		rca.locks.Range(func(_ string, entry *lockEntry) bool {
			entry.cancel()
			return true
		})
		if rca.refreshQueue != nil {
			rca.refreshCancel()
			close(rca.refreshDone)
			rca.refreshWg.Wait()
		}
		rca.client.Close()
	})
}

// cleanupCtx returns ctx with cancellation/deadline stripped but bounded at
// lockTTL — so cleanup outlives a cancelled request without leaking forever.
// Callers must defer the returned cancel.
func (rca *cacheAside) cleanupCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), rca.lockTTL)
}

// awaitLock blocks on waitChan, a jittered poll fallback, or ctx. It returns
// polled=true when poll resolved the wait before Redis invalidation arrived.
func (rca *cacheAside) awaitLockOrPoll(ctx context.Context, waitChan <-chan struct{}, poll func() (bool, error)) (polled bool, err error) {
	start := time.Now()
	defer func() {
		rca.emitLockWaitDuration(time.Since(start))
	}()

	timer := time.NewTimer(rca.nextLockPollDelay())
	defer timer.Stop()
	for {
		select {
		case <-waitChan:
			return false, nil
		case <-ctx.Done():
			return false, ctx.Err()
		case <-timer.C:
			ok, err := poll()
			if ok || err != nil {
				return ok, err
			}
			timer.Reset(rca.nextLockPollDelay())
		}
	}
}

// awaitLockMultiOrPoll is awaitLockOrPoll for many channels. The WaitForAll
// goroutine is cancelled when poll resolves first, so fallback reads do not
// leave a waiter behind until every stale channel closes.
func (rca *cacheAside) awaitLockMultiOrPoll(ctx context.Context, chans []<-chan struct{}, poll func() (bool, error)) (polled bool, err error) {
	start := time.Now()
	defer func() {
		rca.emitLockWaitDuration(time.Since(start))
	}()

	waitCtx, waitCancel := context.WithCancel(ctx)
	defer waitCancel()
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- syncx.WaitForAll(waitCtx, chans)
	}()

	timer := time.NewTimer(rca.nextLockPollDelay())
	defer timer.Stop()
	for {
		select {
		case err := <-waitDone:
			return false, err
		case <-ctx.Done():
			return false, ctx.Err()
		case <-timer.C:
			ok, err := poll()
			if ok || err != nil {
				waitCancel()
				return ok, err
			}
			timer.Reset(rca.nextLockPollDelay())
		}
	}
}

func (rca *cacheAside) onInvalidate(messages []rueidis.RedisMessage) {
	for _, m := range messages {
		key, err := m.ToString()
		if err != nil {
			rca.logger.Error("failed to parse invalidation message", "error", err)
			rca.emitInvalidationError()
			continue
		}
		entry, loaded := rca.locks.LoadAndDelete(key)
		if loaded {
			entry.cancel()
		}
	}
}

func (rca *cacheAside) nextLockPollDelay() time.Duration {
	maxDelay := rca.lockTTL / 10
	if maxDelay < minLockPollDelay {
		maxDelay = minLockPollDelay
	}
	if maxDelay > maxLockPollDelay {
		maxDelay = maxLockPollDelay
	}
	if maxDelay >= rca.lockTTL && rca.lockTTL > 0 {
		maxDelay = rca.lockTTL / 2
		if maxDelay <= 0 {
			maxDelay = rca.lockTTL
		}
	}
	minDelay := maxDelay / 2
	if minDelay <= 0 || maxDelay <= minDelay {
		return maxDelay
	}
	//nolint:gosec // This jitter only spreads Redis polling; it is not security-sensitive.
	return minDelay + time.Duration(rand.Int64N(int64(maxDelay-minDelay)))
}

// register publishes a per-key lockEntry. leader=true means the caller drives
// Redis-side work (SET NX + fn + setWithLock); followers wait on the returned
// channel, saving N-1 round trips on a shared miss.
func (rca *cacheAside) register(key string) (<-chan struct{}, bool) {
retry:
	if actual, ok := rca.locks.Load(key); ok {
		select {
		case <-actual.done:
			rca.locks.CompareAndDelete(key, actual)
			goto retry
		default:
			return actual.done, false
		}
	}

	// timer must be assigned before LoadOrStore publishes newEntry: once
	// published, a concurrent cancel (via onInvalidate or Close) reads le.timer,
	// so the write must be ordered before the store to avoid a data race.
	newEntry := &lockEntry{done: make(chan struct{})}
	newEntry.timer = time.AfterFunc(rca.lockTTL, func() {
		newEntry.timerExpired()
		rca.locks.CompareAndDelete(key, newEntry)
	})
	actual, loaded := rca.locks.LoadOrStore(key, newEntry)
	if !loaded {
		return newEntry.done, true
	}

	// Lost the race — release our timer so the closure can be GCd.
	newEntry.cancel()

	select {
	case <-actual.done:
		rca.locks.CompareAndDelete(key, actual)
		goto retry
	default:
		return actual.done, false
	}
}
