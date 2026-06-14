package redcache

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/lockpool"
	"github.com/dcbickfo/redcache/internal/poolx"
	"github.com/dcbickfo/redcache/internal/syncx"
)

var (
	cacheableTTLPool = poolx.NewSlice(func() []rueidis.CacheableTTL { return make([]rueidis.CacheableTTL, 0, 16) })
	commandsPool     = poolx.NewSlice(func() []rueidis.Completed { return make([]rueidis.Completed, 0, 16) })
	luaExecPool      = poolx.NewSlice(func() []rueidis.LuaExec { return make([]rueidis.LuaExec, 0, 16) })
	stringPool       = poolx.NewSlice(func() []string { return make([]string, 0, 16) })
	chanPool         = poolx.NewSlice(func() []<-chan struct{} { return make([]<-chan struct{}, 0, 16) })
)

// lockEntry tracks a key's wait channel and its TTL timer. cancel and
// timerExpired close done idempotently; timerExpired skips reading le.timer
// because doing so would race the write in register's slow path.
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
	locks          syncx.Map[string, *lockEntry]
	lockPool       *lockpool.Pool
	lockTTL        time.Duration
	lockTTLMs      string // pre-formatted lockTTL.Milliseconds() for Lua args.
	logger         Logger
	metrics        Metrics
	metricsEnabled bool // false when metrics is NoopMetrics{}, gates hot-path emits.
	lockPrefix     string
	refreshAfter   float64                     // 0 = disabled.
	refreshBeta    float64                     // XFetch beta; 0 = simple floor only.
	refreshTimeout time.Duration               // 0 = use the data ttl per call.
	refreshing     syncx.Map[string, struct{}] // local dedup of in-flight refreshes.
	refreshPrefix  string
	refreshQueue   chan refreshJob // worker pool job queue (nil when disabled).
	refreshDone    chan struct{}   // closed by Close to signal workers/senders.
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

// Close cancels pending lock entries and drains refresh workers (bounded by
// LockTTL). The underlying rueidis.Client is closed too. Idempotent.
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

// awaitLock blocks on waitChan or ctx. Emits the wait duration regardless.
func (rca *cacheAside) awaitLock(ctx context.Context, waitChan <-chan struct{}) error {
	start := time.Now()
	select {
	case <-waitChan:
		rca.emitLockWaitDuration(time.Since(start))
		return nil
	case <-ctx.Done():
		rca.emitLockWaitDuration(time.Since(start))
		return ctx.Err()
	}
}

// awaitLockMulti is awaitLock for many channels.
func (rca *cacheAside) awaitLockMulti(ctx context.Context, chans []<-chan struct{}) error {
	start := time.Now()
	err := syncx.WaitForAll(ctx, chans)
	rca.emitLockWaitDuration(time.Since(start))
	return err
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

	// timer must be assigned before LoadOrStore publishes newEntry — otherwise
	// a concurrent cancel could see the field still nil.
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
