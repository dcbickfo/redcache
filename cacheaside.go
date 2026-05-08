// Package redcache provides a cache-aside implementation for Redis with distributed locking.
//
// This library builds on the rueidis Redis client to provide:
//   - Cache-aside pattern with automatic cache population
//   - Distributed locking to prevent thundering herd
//   - Client-side caching to reduce Redis round trips
//   - Redis cluster support with slot-aware batching
//   - Automatic cleanup of expired lock entries
//
// # Basic Usage
//
//	client, err := redcache.NewRedCacheAside(
//	    rueidis.ClientOption{InitAddress: []string{"localhost:6379"}},
//	    redcache.CacheAsideOption{LockTTL: 10 * time.Second},
//	)
//	if err != nil {
//	    return err
//	}
//	defer client.Client().Close()
//
//	// Get a single value with automatic cache population
//	value, err := client.Get(ctx, time.Minute, "user:123", func(ctx context.Context, key string) (string, error) {
//	    return fetchFromDatabase(ctx, key)
//	})
//
//	// Get multiple values with batched cache population
//	values, err := client.GetMulti(ctx, time.Minute, []string{"user:1", "user:2"}, func(ctx context.Context, keys []string) (map[string]string, error) {
//	    return fetchMultipleFromDatabase(ctx, keys)
//	})
//
// # Distributed Locking
//
// The library ensures that only one goroutine (across all instances of your application)
// executes the callback function for a given key at a time. Other goroutines will wait
// for the lock to be released and then return the cached value.
//
// Locks are implemented using Redis SET NX with a configurable TTL. Lock values use
// UUIDv7 for uniqueness and are prefixed (default: "__redcache:lock:") to avoid
// collisions with application data.
//
// # Context and Timeouts
//
// All operations respect context cancellation. The LockTTL option controls:
//   - Maximum time a lock can be held before automatic expiration
//   - Timeout for waiting on locks when handling invalidation messages
//   - Context timeout for cleanup operations
//
// Use context deadlines to control overall operation timeout:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	value, err := client.Get(ctx, time.Minute, key, callback)
//
// # Client-Side Caching
//
// The library uses rueidis client-side caching with Redis invalidation messages.
// When a key is modified in Redis, invalidation messages automatically clear the
// local cache, ensuring consistency across distributed instances.
package redcache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/cmdx"
	"github.com/dcbickfo/redcache/internal/lockpool"
	"github.com/dcbickfo/redcache/internal/mapsx"
	"github.com/dcbickfo/redcache/internal/poolx"
	"github.com/dcbickfo/redcache/internal/syncx"
)

// Pools for slice headers reused across multi-key call paths.
var (
	cacheableTTLPool = poolx.NewSlice(func() []rueidis.CacheableTTL { return make([]rueidis.CacheableTTL, 0, 16) })
	commandsPool     = poolx.NewSlice(func() []rueidis.Completed { return make([]rueidis.Completed, 0, 16) })
	luaExecPool      = poolx.NewSlice(func() []rueidis.LuaExec { return make([]rueidis.LuaExec, 0, 16) })
	stringPool       = poolx.NewSlice(func() []string { return make([]string, 0, 16) })
	chanPool         = poolx.NewSlice(func() []<-chan struct{} { return make([]<-chan struct{}, 0, 16) })
)

// Default prefixes used by CacheAside. Overridable via CacheAsideOption.LockPrefix
// and CacheAsideOption.RefreshLockPrefix.
const (
	DefaultLockPrefix    = "__redcache:lock:"
	DefaultRefreshPrefix = "__redcache:refresh:"
)

// lockEntry tracks a key's wait channel and TTL timer.
//
// cancel() and the timer's fire path both close done idempotently via sync.Once.
// Only the external cancel path reads le.timer; the timer-fire path does not,
// avoiding a data race against the write in register().
type lockEntry struct {
	done  chan struct{}
	once  sync.Once
	timer *time.Timer
}

// cancel stops the timer and closes done. Safe across concurrent invocations.
// Used by onInvalidate, Close, and register's race-loss path.
func (le *lockEntry) cancel() {
	le.once.Do(func() {
		if le.timer != nil {
			le.timer.Stop()
		}
		close(le.done)
	})
}

// timerExpired is the timer-fire path. It must not read le.timer: the timer is
// firing, so reading the field would race with the assignment in register's slow path.
func (le *lockEntry) timerExpired() {
	le.once.Do(func() {
		close(le.done)
	})
}

// Logger defines the logging interface used by CacheAside.
// Implementations must be safe for concurrent use and handle log levels internally.
type Logger interface {
	// Error logs unexpected failures or critical issues.
	Error(msg string, args ...any)
	// Debug logs verbose diagnostic information about internal state.
	Debug(msg string, args ...any)
}

// CacheAside provides a cache-aside pattern backed by Redis with distributed locking
// and client-side caching via rueidis invalidation messages.
type CacheAside struct {
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
	refreshing     syncx.Map[string, struct{}] // local dedup of in-flight refreshes.
	refreshPrefix  string
	refreshQueue   chan refreshJob // worker pool job queue (nil when disabled).
	refreshDone    chan struct{}   // closed by Close to signal workers/senders.
	refreshWg      sync.WaitGroup
	closing        atomic.Bool // set true at the start of Close to gate refresh sends.
	closeOnce      sync.Once
}

// CacheAsideOption configures a CacheAside instance.
type CacheAsideOption struct {
	// LockTTL is the maximum time a lock can be held, and the timeout for waiting
	// on locks when an invalidation message is lost. Defaults to 10 seconds.
	LockTTL time.Duration
	// ClientBuilder optionally overrides how the rueidis.Client is created.
	// When nil, rueidis.NewClient is used.
	ClientBuilder func(option rueidis.ClientOption) (rueidis.Client, error)
	// Logger for errors and debug output. Defaults to slog.Default().
	Logger Logger
	// Metrics receives observability events. Defaults to NoopMetrics.
	// Implementations must be concurrent-safe; methods run on the hot path.
	Metrics Metrics
	// LockPrefix for distributed locks. Defaults to DefaultLockPrefix.
	// Choose a prefix unlikely to conflict with your data keys.
	LockPrefix string
	// RefreshLockPrefix is the key prefix used for distributed refresh-ahead locks.
	// Defaults to DefaultRefreshPrefix. Refresh keys include a hash tag wrapping
	// the data key so they hash to the same cluster slot as the data key
	// (e.g. "__redcache:refresh:{user:123}").
	RefreshLockPrefix string
	// RefreshAfterFraction enables refresh-ahead caching. When a cached value
	// is returned and more than this fraction of its TTL has elapsed, a
	// background worker refreshes the value while the stale one is returned
	// immediately. For example, 0.8 means "refresh after 80% of TTL has passed".
	// Set to 0 (default) to disable. Must be in [0, 1).
	//
	// The threshold uses the client-side cache TTL (CachePTTL), which closely
	// approximates the server-side TTL when the same ttl is used consistently
	// for a key across Get calls.
	//
	// While remaining TTL is above (1 - RefreshAfterFraction) * ttl, refresh
	// never fires. Below that floor, XFetch sampling decides whether each
	// individual read triggers refresh (see RefreshBeta).
	RefreshAfterFraction float64
	// RefreshBeta scales the XFetch probabilistic-refresh window. Higher values
	// trigger refresh earlier within the floor; the per-read probability climbs
	// as the value approaches expiry, weighted by compute time (delta) so
	// slow-to-recompute values get more headroom. Default 0 (XFetch disabled,
	// always refresh below floor); 1.0 matches canonical XFetch from Vattani
	// et al. Only applies to envelope-wrapped values; legacy values (no delta
	// recorded) always use the simple floor-based behaviour.
	//
	// In multi-key writes the recorded delta is the total fn duration divided
	// evenly across returned values. Batches dominated by one slow key will
	// under-refresh it relative to a serial path.
	RefreshBeta float64
	// RefreshWorkers is the number of background workers that process refresh
	// jobs. Defaults to 4 when RefreshAfterFraction > 0. Must be > 0 when refresh
	// is enabled.
	RefreshWorkers int
	// RefreshQueueSize is the maximum number of pending refresh jobs. When full,
	// new requests are silently dropped (stale value continues to be served).
	// Defaults to 64 when RefreshAfterFraction > 0. Must be > 0 when refresh is enabled.
	RefreshQueueSize int
}

func validateAndApplyDefaults(clientOption rueidis.ClientOption, caOption *CacheAsideOption) error {
	if len(clientOption.InitAddress) == 0 {
		return errors.New("at least one Redis address must be provided in InitAddress")
	}
	if caOption.LockTTL < 0 {
		return errors.New("LockTTL must not be negative")
	}
	if caOption.LockTTL > 0 && caOption.LockTTL < 100*time.Millisecond {
		return errors.New("LockTTL should be at least 100ms to avoid excessive lock churn")
	}
	if caOption.LockTTL == 0 {
		caOption.LockTTL = 10 * time.Second
	}
	if caOption.Logger == nil {
		caOption.Logger = slog.Default()
	}
	if caOption.Metrics == nil {
		caOption.Metrics = NoopMetrics{}
	}
	if caOption.LockPrefix == "" {
		caOption.LockPrefix = DefaultLockPrefix
	}
	if caOption.RefreshLockPrefix == "" {
		caOption.RefreshLockPrefix = DefaultRefreshPrefix
	}
	// Reject any LockPrefix that would make the envelope-prefixed value read as a
	// lock — every cached value would then look like a lock and tryGet would
	// always return errNotFound, silently turning every read into a miss.
	if strings.HasPrefix(envelopePrefix, caOption.LockPrefix) {
		return fmt.Errorf("LockPrefix %q conflicts with envelope prefix %q (would mask all cached reads as locks)", caOption.LockPrefix, envelopePrefix)
	}
	return validateRefreshDefaults(caOption)
}

func validateRefreshDefaults(caOption *CacheAsideOption) error {
	if caOption.RefreshAfterFraction < 0 || caOption.RefreshAfterFraction >= 1 {
		return errors.New("RefreshAfterFraction must be in range [0, 1)")
	}
	if caOption.RefreshBeta < 0 {
		return errors.New("RefreshBeta must not be negative")
	}
	if caOption.RefreshAfterFraction == 0 {
		return nil
	}
	if caOption.RefreshWorkers < 0 {
		return errors.New("RefreshWorkers must not be negative")
	}
	if caOption.RefreshQueueSize < 0 {
		return errors.New("RefreshQueueSize must not be negative")
	}
	if caOption.RefreshWorkers == 0 {
		caOption.RefreshWorkers = 4
	}
	if caOption.RefreshQueueSize == 0 {
		caOption.RefreshQueueSize = 64
	}
	return nil
}

// NewRedCacheAside creates a CacheAside with the given Redis client and cache-aside options.
func NewRedCacheAside(clientOption rueidis.ClientOption, caOption CacheAsideOption) (*CacheAside, error) {
	if err := validateAndApplyDefaults(clientOption, &caOption); err != nil {
		return nil, err
	}

	lp, err := lockpool.New(caOption.LockPrefix)
	if err != nil {
		return nil, fmt.Errorf("lock pool: %w", err)
	}
	_, isNoop := caOption.Metrics.(NoopMetrics)
	rca := &CacheAside{
		lockPool:       lp,
		lockTTL:        caOption.LockTTL,
		lockTTLMs:      strconv.FormatInt(caOption.LockTTL.Milliseconds(), 10),
		logger:         caOption.Logger,
		metrics:        caOption.Metrics,
		metricsEnabled: !isNoop,
		lockPrefix:     caOption.LockPrefix,
		refreshAfter:   caOption.RefreshAfterFraction,
		refreshBeta:    caOption.RefreshBeta,
		refreshPrefix:  caOption.RefreshLockPrefix,
	}
	// Force a single connection per node so client-side cache reads and the
	// invalidation stream share the same pipe. Avoids cross-connection
	// invalidation routing. Users needing different behavior can override via
	// CacheAsideOption.ClientBuilder.
	clientOption.PipelineMultiplex = -1
	clientOption.OnInvalidations = rca.onInvalidate

	if caOption.ClientBuilder != nil {
		rca.client, err = caOption.ClientBuilder(clientOption)
	} else {
		rca.client, err = rueidis.NewClient(clientOption)
	}
	if err != nil {
		return nil, err
	}

	if rca.refreshAfter > 0 {
		rca.refreshQueue = make(chan refreshJob, caOption.RefreshQueueSize)
		rca.refreshDone = make(chan struct{})
		rca.startRefreshWorkers(caOption.RefreshWorkers)
	}

	return rca, nil
}

// Client returns the underlying rueidis.Client for advanced operations.
// Most users should not need direct client access. Use with caution as
// direct operations bypass the cache-aside pattern and distributed locking.
func (rca *CacheAside) Client() rueidis.Client {
	return rca.client
}

// Close cancels all pending lock entries and shuts down refresh workers.
// It does NOT close the underlying Redis client — that is the caller's responsibility.
// If refresh-ahead is enabled, Close waits for in-flight refresh jobs (bounded
// by LockTTL). Safe to call multiple times.
//
// Shutdown signals workers via refreshDone rather than closing refreshQueue:
// concurrent send + close on the same channel is a data race even with recover.
func (rca *CacheAside) Close() {
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
	})
}

// cleanupCtx returns a context derived from ctx with cancellation/deadline
// stripped (so cleanup runs even after the original request completes) but
// bounded at lockTTL. Callers MUST defer the returned cancel.
func (rca *CacheAside) cleanupCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), rca.lockTTL)
}

// Metric emit helpers. Each gates on rca.metricsEnabled; count-based helpers
// short-circuit on n <= 0 to avoid emitting zeros.

func (rca *CacheAside) emitCacheHits(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.CacheHits(int64(n))
	}
}

func (rca *CacheAside) emitCacheMisses(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.CacheMisses(int64(n))
	}
}

// awaitLock blocks until waitChan closes or ctx is done, then emits the
// resolved-wait duration. Returns nil on lock release, ctx.Err() on cancellation.
func (rca *CacheAside) awaitLock(ctx context.Context, waitChan <-chan struct{}) error {
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

// awaitLockMulti waits for every channel to close (or ctx to cancel) and emits
// the elapsed wait time once.
func (rca *CacheAside) awaitLockMulti(ctx context.Context, chans []<-chan struct{}) error {
	start := time.Now()
	err := syncx.WaitForAll(ctx, chans)
	rca.emitLockWaitDuration(time.Since(start))
	return err
}

func (rca *CacheAside) emitLockWaitDuration(d time.Duration) {
	if rca.metricsEnabled {
		rca.metrics.LockWaitDuration(d)
	}
}

func (rca *CacheAside) emitLockContended(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.LockContended(int64(n))
	}
}

func (rca *CacheAside) emitRefreshTriggered(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.RefreshTriggered(int64(n))
	}
}

func (rca *CacheAside) emitRefreshSkipped(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.RefreshSkipped(int64(n))
	}
}

func (rca *CacheAside) emitRefreshDropped(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.RefreshDropped(int64(n))
	}
}

func (rca *CacheAside) emitLockLost(key string) {
	if rca.metricsEnabled {
		rca.metrics.LockLost(key)
	}
}

func (rca *CacheAside) emitRefreshError(key string) {
	if rca.metricsEnabled {
		rca.metrics.RefreshError(key)
	}
}

func (rca *CacheAside) emitRefreshPanicked(key string) {
	if rca.metricsEnabled {
		rca.metrics.RefreshPanicked(key)
	}
}

func (rca *CacheAside) emitInvalidationError() {
	if rca.metricsEnabled {
		rca.metrics.InvalidationError()
	}
}

func (rca *CacheAside) onInvalidate(messages []rueidis.RedisMessage) {
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

// register publishes a per-key lockEntry and returns its done channel and a
// leader flag. leader=true: this caller created the entry and must drive
// Redis-side work (tryLock + fn + setWithLock). leader=false: another in-process
// caller owns that work and the follower waits on done instead of re-issuing
// SET NX, saving N-1 round trips when many goroutines miss the same key.
func (rca *CacheAside) register(key string) (<-chan struct{}, bool) {
retry:
	if actual, ok := rca.locks.Load(key); ok {
		select {
		case <-actual.done:
			// Stale entry — try to atomically delete and retry.
			rca.locks.CompareAndDelete(key, actual)
			goto retry
		default:
			return actual.done, false
		}
	}

	// The timer must be assigned BEFORE LoadOrStore publishes newEntry, otherwise
	// a concurrent cancel() could observe a partial struct (timer field not yet
	// written).
	newEntry := &lockEntry{done: make(chan struct{})}
	newEntry.timer = time.AfterFunc(rca.lockTTL, func() {
		newEntry.timerExpired()
		rca.locks.CompareAndDelete(key, newEntry)
	})
	actual, loaded := rca.locks.LoadOrStore(key, newEntry)
	if !loaded {
		return newEntry.done, true
	}

	// Lost the race — release our timer so it doesn't keep the closure alive.
	newEntry.cancel()

	select {
	case <-actual.done:
		rca.locks.CompareAndDelete(key, actual)
		goto retry
	default:
		return actual.done, false
	}
}

// Get returns the cached value for key, populating the cache by calling fn on a miss.
// Only one goroutine across all instances executes fn for a given key at a time;
// others wait for the result via Redis invalidation messages.
//
// Empty-string values are valid hits.
func (rca *CacheAside) Get(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (val string, err error),
) (string, error) {
retry:
	wait, leader := rca.register(key)
	res, err := rca.tryGet(ctx, ttl, key)

	if err == nil {
		rca.emitCacheHits(1)
		if rca.shouldRefresh(res.pttl, ttl, res.delta) {
			rca.triggerRefresh(ctx, ttl, key, fn)
		}
		return res.val, nil
	}
	if !errors.Is(err, errNotFound) {
		return "", err
	}

	rca.emitCacheMisses(1)

	if !leader {
		// Follower: leader will drive Redis population. Skip our SET NX and
		// wait for the leader's invalidation.
		rca.emitLockContended(1)
		if werr := rca.awaitLock(ctx, wait); werr != nil {
			return "", werr
		}
		goto retry
	}

	val, err := rca.trySetKeyFunc(ctx, ttl, key, fn)
	if err == nil {
		return val, nil
	}

	if errors.Is(err, errLockFailed) || errors.Is(err, ErrLockLost) {
		// errLockFailed: another process holds the Redis lock. Don't cancel our
		// local lockEntry — that would wake followers prematurely and they would
		// each become a new leader racing the same Redis NX. Wait alongside them
		// for the actual Redis holder's invalidation.
		// ErrLockLost: a ForceSet stole our lock — retry to read it.
		rca.emitLockContended(1)
		if werr := rca.awaitLock(ctx, wait); werr != nil {
			return "", werr
		}
		goto retry
	}

	return "", err
}

// Del removes a key from Redis, triggering invalidation on all clients.
func (rca *CacheAside) Del(ctx context.Context, key string) error {
	return rca.client.Do(ctx, rca.client.B().Del().Key(key).Build()).Error()
}

// DelMulti removes multiple keys from Redis, triggering invalidation on all clients.
//
// All commands are issued; on partial failure each per-key error is logged and
// the first is returned wrapped with key context.
func (rca *CacheAside) DelMulti(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	cmdsP := commandsPool.GetCap(len(keys))
	defer commandsPool.Put(cmdsP)
	for _, key := range keys {
		*cmdsP = append(*cmdsP, rca.client.B().Del().Key(key).Build())
	}
	resps := rca.client.DoMulti(ctx, *cmdsP...)
	var firstErr error
	var firstErrKey string
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			rca.logger.Error("DelMulti key failed", "key", keys[i], "error", err)
			if firstErr == nil {
				firstErr = err
				firstErrKey = keys[i]
			}
		}
	}
	if firstErr != nil {
		return fmt.Errorf("del key %q: %w", firstErrKey, firstErr)
	}
	return nil
}

// Touch sets the TTL of a cached value to ttl. No-op when the key is missing
// or holds a lock value, so it cannot accidentally extend an in-flight lock.
// PEXPIRE does not trigger client-side invalidation, so existing readers
// continue to serve from their local copy.
//
// Use Touch for sliding-TTL semantics (sessions, tokens) without re-running fn.
func (rca *CacheAside) Touch(ctx context.Context, ttl time.Duration, key string) error {
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	if err := touchScript.Exec(ctx, rca.client, []string{key}, []string{ttlMs, rca.lockPrefix}).Error(); err != nil {
		return fmt.Errorf("touch key %q: %w", key, err)
	}
	return nil
}

// TouchMulti sets the TTL of multiple cached values to ttl. Per-key extension
// is a no-op on missing or lock-held keys. Per-key errors are logged and the
// first is returned wrapped with key context.
func (rca *CacheAside) TouchMulti(ctx context.Context, ttl time.Duration, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	stmtsBySlot := rca.groupTouchExecs(ttl, keys)
	if firstErrKey, firstErr := rca.runTouchSlots(ctx, stmtsBySlot); firstErr != nil {
		return fmt.Errorf("touch key %q: %w", firstErrKey, firstErr)
	}
	return nil
}

type touchExec struct {
	key  string
	exec rueidis.LuaExec
}

func (rca *CacheAside) groupTouchExecs(ttl time.Duration, keys []string) map[uint16][]touchExec {
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	stmtsBySlot := make(map[uint16][]touchExec)
	for _, k := range keys {
		slot := cmdx.Slot(k)
		stmtsBySlot[slot] = append(stmtsBySlot[slot], touchExec{
			key: k,
			exec: rueidis.LuaExec{
				Keys: []string{k},
				Args: []string{ttlMs, rca.lockPrefix},
			},
		})
	}
	return stmtsBySlot
}

// runTouchSlots executes the touch script per slot in parallel.
func (rca *CacheAside) runTouchSlots(ctx context.Context, slots map[uint16][]touchExec) (string, error) {
	var (
		mu          sync.Mutex
		wg          sync.WaitGroup
		firstErr    error
		firstErrKey string
	)
	for _, stmts := range slots {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key, err := rca.touchSlot(ctx, stmts)
			if err == nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if firstErr == nil {
				firstErr, firstErrKey = err, key
			}
		}()
	}
	wg.Wait()
	return firstErrKey, firstErr
}

// touchSlot runs the touch script for one slot's keys, returning the first
// error and its key.
func (rca *CacheAside) touchSlot(ctx context.Context, stmts []touchExec) (string, error) {
	execsP := luaExecPool.Get(len(stmts))
	defer luaExecPool.Put(execsP)
	execs := *execsP
	for i, s := range stmts {
		execs[i] = s.exec
	}
	resps := touchScript.ExecMulti(ctx, rca.client, execs...)
	var firstErr error
	var firstErrKey string
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			rca.logger.Error("TouchMulti key failed", "key", stmts[i].key, "error", err)
			if firstErr == nil {
				firstErr, firstErrKey = err, stmts[i].key
			}
		}
	}
	return firstErrKey, firstErr
}

var (
	errNotFound   = errors.New("not found")
	errLockFailed = errors.New("lock failed")
)

// cacheReadResult is the return type of tryGet: cached value, client-side
// cache PTTL, and the envelope's recorded compute delta (0 for legacy values).
type cacheReadResult struct {
	val   string
	pttl  int64
	delta time.Duration
}

func (rca *CacheAside) tryGet(ctx context.Context, ttl time.Duration, key string) (cacheReadResult, error) {
	resp := rca.client.DoCache(ctx, rca.client.B().Get().Key(key).Cache(), ttl)
	val, err := resp.ToString()
	if rueidis.IsRedisNil(err) || strings.HasPrefix(val, rca.lockPrefix) {
		return cacheReadResult{}, errNotFound
	}
	if err != nil {
		return cacheReadResult{}, fmt.Errorf("read key %q: %w", key, err)
	}
	plain, delta := unwrapEnvelope(val)
	return cacheReadResult{val: plain, pttl: resp.CachePTTL(), delta: delta}, nil
}

func (rca *CacheAside) trySetKeyFunc(ctx context.Context, ttl time.Duration, key string, fn func(ctx context.Context, key string) (string, error)) (val string, err error) {
	setVal := false
	lockVal, err := rca.tryLock(ctx, key)
	if err != nil {
		return "", err
	}
	defer func() {
		if !setVal {
			toCtx, cancel := rca.cleanupCtx(ctx)
			defer cancel()
			// Best effort: lock TTL will clear it if unlock fails.
			if err := rca.unlock(toCtx, key, lockVal); err != nil {
				rca.logger.Error("failed to unlock key", "key", key, "error", err)
			}
		}
	}()
	start := time.Now()
	if val, err = fn(ctx, key); err == nil {
		// XFetch metadata: record fn duration so future reads can probabilistically
		// refresh slow-to-recompute values earlier.
		wrapped := wrapEnvelope(val, time.Since(start))
		if _, err = rca.setWithLock(ctx, ttl, key, valAndLock{wrapped, lockVal}); err == nil {
			setVal = true
		}
		return val, err
	}
	return "", err
}

func (rca *CacheAside) tryLock(ctx context.Context, key string) (string, error) {
	lockVal := rca.lockPool.Generate()
	err := rca.client.Do(ctx, rca.client.B().Set().Key(key).Value(lockVal).Nx().Get().Px(rca.lockTTL).Build()).Error()
	// SET NX GET reply semantics:
	//   IsRedisNil: NX fired, lock acquired.
	//   nil error:  key existed; NX rejected.
	//   other err:  real Redis error — propagate so the caller can fail fast
	//               instead of waiting on a contention channel that will never close.
	if rueidis.IsRedisNil(err) {
		return lockVal, nil
	}
	if err == nil {
		return "", fmt.Errorf("lock key %q: %w", key, errLockFailed)
	}
	return "", fmt.Errorf("lock key %q: %w", key, err)
}

func (rca *CacheAside) setWithLock(ctx context.Context, ttl time.Duration, key string, valLock valAndLock) (string, error) {
	resp := setKeyLua.Exec(ctx, rca.client, []string{key}, []string{valLock.lockVal, valLock.val, strconv.FormatInt(ttl.Milliseconds(), 10)})
	if err := resp.Error(); err != nil {
		if !rueidis.IsRedisNil(err) {
			return "", fmt.Errorf("set key %q: %w", key, err)
		}
		rca.emitLockLost(key)
		return "", fmt.Errorf("lock lost for key %q: %w", key, ErrLockLost)
	}
	// Lua returns 0 on CAS mismatch (lock lost), 1 on success. A non-integer
	// response means the script drifted.
	val, ierr := resp.AsInt64()
	if ierr != nil {
		rca.logger.Error("unexpected non-integer in CAS-set response", "key", key, "error", ierr)
		return "", fmt.Errorf("set key %q: parse response: %w", key, ierr)
	}
	if val == 0 {
		rca.emitLockLost(key)
		return "", fmt.Errorf("lock lost for key %q: %w", key, ErrLockLost)
	}
	return valLock.val, nil
}

func (rca *CacheAside) unlock(ctx context.Context, key string, lock string) error {
	return delKeyLua.Exec(ctx, rca.client, []string{key}, []string{lock}).Error()
}

// GetMulti returns cached values for the given keys, populating misses via fn.
// SET operations are grouped by Redis cluster slot for efficient batching.
func (rca *CacheAside) GetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, key []string) (val map[string]string, err error),
) (map[string]string, error) {
	if len(keys) == 0 {
		return map[string]string{}, nil
	}
	res := make(map[string]string, len(keys))

	// Parallel slices: pending[i] is an unresolved key, chans[i] its wait
	// channel from register(). Pooled so the dominant cache-hit path stays
	// alloc-free for these two slice headers.
	pendingP := stringPool.Get(len(keys))
	defer stringPool.Put(pendingP)
	pending := *pendingP
	copy(pending, keys)

	chansP := chanPool.Get(len(keys))
	defer chanPool.Put(chansP)
	chans := *chansP

	// Pooled scratch slice for refresh-ahead candidates. triggerMultiRefresh
	// copies into its own slice, so reusing the buffer across retries is safe.
	needRefreshP := stringPool.GetCap(len(keys))
	defer stringPool.Put(needRefreshP)

	// Leader keys: this caller created the lockEntry; followers stay out and
	// skip the Redis SET NX, waiting on chans[i] instead. Rebuilt each retry.
	leaderKeysP := stringPool.GetCap(len(keys))
	defer stringPool.Put(leaderKeysP)

retry:
	chans = chans[:len(pending)]
	leaderKeys := (*leaderKeysP)[:0]
	for i, key := range pending {
		var isLeader bool
		chans[i], isLeader = rca.register(key)
		if isLeader {
			leaderKeys = append(leaderKeys, key)
		}
	}
	*leaderKeysP = leaderKeys

	hitsBefore := len(res)
	*needRefreshP = (*needRefreshP)[:0]
	needRefresh, err := rca.tryGetMulti(ctx, ttl, pending, res, *needRefreshP)
	if err != nil {
		return nil, err
	}
	*needRefreshP = needRefresh
	rca.emitCacheHits(len(res) - hitsBefore)

	if len(needRefresh) > 0 {
		rca.triggerMultiRefresh(ctx, ttl, needRefresh, fn)
	}

	pending, chans = filterResolved(pending, chans, res)

	if len(pending) > 0 {
		rca.emitCacheMisses(len(pending))
		if err := rca.runLeaderSets(ctx, ttl, leaderKeys, fn, res); err != nil {
			return nil, err
		}
		pending, chans = filterResolved(pending, chans, res)
	}

	if len(pending) > 0 {
		// Followers plus leaders whose Redis NX lost — both wait for the actual
		// Redis lock holder's invalidation (or the lockTTL fallback).
		rca.emitLockContended(len(pending))
		if err = rca.awaitLockMulti(ctx, chans); err != nil {
			return nil, err
		}
		goto retry
	}
	return res, nil
}

// runLeaderSets drops leaderKeys already populated by tryGetMulti (e.g. via a
// CSC invalidation mid-call) then drives trySetMultiKeyFn for the rest.
func (rca *CacheAside) runLeaderSets(
	ctx context.Context,
	ttl time.Duration,
	leaderKeys []string,
	fn func(ctx context.Context, key []string) (val map[string]string, err error),
	res map[string]string,
) error {
	n := 0
	for _, k := range leaderKeys {
		if _, ok := res[k]; !ok {
			leaderKeys[n] = k
			n++
		}
	}
	leaderKeys = leaderKeys[:n]
	if len(leaderKeys) == 0 {
		return nil
	}
	return rca.trySetMultiKeyFn(ctx, ttl, leaderKeys, fn, res)
}

// filterResolved drops keys present in resolved from pending+chans in-place,
// keeping the two slices index-aligned so each remaining key keeps its wait channel.
func filterResolved(pending []string, chans []<-chan struct{}, resolved map[string]string) ([]string, []<-chan struct{}) {
	n := 0
	for i, k := range pending {
		if _, ok := resolved[k]; !ok {
			pending[n] = pending[i]
			chans[n] = chans[i]
			n++
		}
	}
	return pending[:n], chans[:n]
}

// tryGetMulti reads keys via DoMultiCache, writes non-lock values into res,
// and appends refresh-ahead candidates onto needRefresh (caller-supplied empty
// slice). Returns the appended slice so the caller can update its pool handle.
func (rca *CacheAside) tryGetMulti(ctx context.Context, ttl time.Duration, keys []string, res map[string]string, needRefresh []string) ([]string, error) {
	multiP := cacheableTTLPool.Get(len(keys))
	defer cacheableTTLPool.Put(multiP)
	multi := *multiP
	for i, key := range keys {
		multi[i] = rueidis.CacheableTTL{
			Cmd: rca.client.B().Get().Key(key).Cache(),
			TTL: ttl,
		}
	}
	resps := rca.client.DoMultiCache(ctx, multi...)

	for i, resp := range resps {
		val, err := resp.ToString()
		if rueidis.IsRedisNil(err) {
			continue
		}
		if err != nil {
			return needRefresh, fmt.Errorf("key %q: %w", keys[i], err)
		}
		if !strings.HasPrefix(val, rca.lockPrefix) {
			plain, delta := unwrapEnvelope(val)
			res[keys[i]] = plain
			if rca.shouldRefresh(resp.CachePTTL(), ttl, delta) {
				needRefresh = append(needRefresh, keys[i])
			}
		}
	}
	return needRefresh, nil
}

// trySetMultiKeyFn locks each pending key, calls fn, sets values in Redis,
// and writes successfully-set entries into res.
func (rca *CacheAside) trySetMultiKeyFn(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, key []string) (val map[string]string, err error),
	res map[string]string,
) error {
	lockVals, err := rca.tryLockMulti(ctx, keys)
	if err != nil {
		return err
	}

	defer func() {
		toUnlock := make(map[string]string)
		for key, lockVal := range lockVals {
			if _, ok := res[key]; !ok {
				toUnlock[key] = lockVal
			}
		}
		if len(toUnlock) > 0 {
			toCtx, cancel := rca.cleanupCtx(ctx)
			defer cancel()
			rca.unlockMulti(toCtx, toUnlock)
		}
	}()

	if len(lockVals) == 0 {
		return nil
	}

	start := time.Now()
	vals, err := fn(ctx, mapsx.Keys(lockVals))
	if err != nil {
		return err
	}
	// XFetch metadata: amortise fn time across the values produced. Skewed
	// (parallel batches report the same per-key delta as serial), but better
	// than 0 — slow batches get proportionally more refresh headroom.
	delta := perValueDelta(time.Since(start), len(vals))

	vL := make(map[string]valAndLock, len(vals))
	for k, v := range vals {
		vL[k] = valAndLock{wrapEnvelope(v, delta), lockVals[k]}
	}

	keysSet, err := rca.setMultiWithLock(ctx, ttl, vL)
	if err != nil {
		return err
	}

	for _, keySet := range keysSet {
		res[keySet] = vals[keySet]
	}

	return nil
}

// perValueDelta amortises a multi-key fn's total compute time across the
// values it produced. Returns 0 when n <= 0, which makes shouldRefresh fall
// back to the floor.
func perValueDelta(total time.Duration, n int) time.Duration {
	if n <= 0 {
		return 0
	}
	return total / time.Duration(n)
}

func (rca *CacheAside) tryLockMulti(ctx context.Context, keys []string) (map[string]string, error) {
	lockVals := make(map[string]string, len(keys))
	cmdsP := commandsPool.GetCap(len(keys))
	defer commandsPool.Put(cmdsP)
	for _, k := range keys {
		lockVals[k] = rca.lockPool.Generate()
		*cmdsP = append(*cmdsP, rca.client.B().Set().Key(k).Value(lockVals[k]).Nx().Get().Px(rca.lockTTL).Build())
	}
	resps := rca.client.DoMulti(ctx, *cmdsP...)
	// SET NX GET reply semantics (per-key, mirroring tryLock):
	//   IsRedisNil:  NX fired, lock acquired.
	//   nil error:   key existed; NX rejected — caller waits for release.
	//   other err:   real Redis error — propagate so the caller fails fast.
	//
	// DoMulti is a pipeline: every SET NX has already executed by the time we
	// read resps. Drain ALL responses so successes from earlier indices can be
	// released; an early return would leak them for the full lockTTL.
	var firstErr error
	var firstErrKey string
	for i, r := range resps {
		err := r.Error()
		if rueidis.IsRedisNil(err) {
			continue
		}
		if err == nil {
			delete(lockVals, keys[i])
			continue
		}
		delete(lockVals, keys[i])
		if firstErr == nil {
			firstErr = err
			firstErrKey = keys[i]
		} else {
			rca.logger.Error("additional tryLockMulti error", "key", keys[i], "error", err)
		}
	}
	if firstErr != nil {
		if len(lockVals) > 0 {
			cleanupCtx, cancel := rca.cleanupCtx(ctx)
			rca.unlockMulti(cleanupCtx, lockVals)
			cancel()
		}
		return nil, fmt.Errorf("lock key %q: %w", firstErrKey, firstErr)
	}
	return lockVals, nil
}

type valAndLock struct {
	val     string
	lockVal string
}

type keyOrderAndSet struct {
	keyOrder []string
	setStmts []rueidis.LuaExec
}

// groupBySlot groups keys by their Redis cluster slot for batched Lua execution.
func groupBySlot(keyValLock map[string]valAndLock, ttl time.Duration) map[uint16]keyOrderAndSet {
	stmts := make(map[uint16]keyOrderAndSet)
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)

	for k, vl := range keyValLock {
		slot := cmdx.Slot(k)
		kos := stmts[slot]
		kos.keyOrder = append(kos.keyOrder, k)
		kos.setStmts = append(kos.setStmts, rueidis.LuaExec{
			Keys: []string{k},
			Args: []string{vl.lockVal, vl.val, ttlMs},
		})
		stmts[slot] = kos
	}

	return stmts
}

type slotSetResult struct {
	keys []string
	err  error
}

// runSlotSet executes the Lua statements for one slot. All responses are
// inspected so successes in the same batch are not lost when reporting an
// error. Lua returning 0 (or a nil response, treated defensively as the same)
// indicates a CAS mismatch (lock lost): the key is dropped and LockLost emits.
func (rca *CacheAside) runSlotSet(ctx context.Context, kos keyOrderAndSet) slotSetResult {
	var keys []string
	var firstErr error
	setResps := setKeyLua.ExecMulti(ctx, rca.client, kos.setStmts...)
	for j, resp := range setResps {
		ok, err := rca.inspectSlotSetResponse(kos.keyOrder[j], resp)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if ok {
			keys = append(keys, kos.keyOrder[j])
		}
	}
	return slotSetResult{keys: keys, err: firstErr}
}

// inspectSlotSetResponse classifies one ExecMulti response into success /
// silent LockLost / surfaceable error. Parse errors are surfaced so script
// drift fails fast instead of triggering an infinite retry loop.
func (rca *CacheAside) inspectSlotSetResponse(key string, resp rueidis.RedisResult) (bool, error) {
	if err := resp.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			rca.emitLockLost(key)
			return false, nil
		}
		return false, fmt.Errorf("set key %q: %w", key, err)
	}
	val, ierr := resp.AsInt64()
	if ierr != nil {
		rca.logger.Error("unexpected non-integer in CAS-set response", "key", key, "error", ierr)
		return false, fmt.Errorf("set key %q: parse response: %w", key, ierr)
	}
	if val == 0 {
		rca.emitLockLost(key)
		return false, nil
	}
	return true, nil
}

// executeSetStatements runs the per-slot Lua set scripts and reduces results
// to (succeeded keys, first error). Slot work runs to completion before the
// reduce so a real Redis error in one slot can't mask successes in another.
func (rca *CacheAside) executeSetStatements(ctx context.Context, stmts map[uint16]keyOrderAndSet) ([]string, error) {
	return rca.collectSlotSetResults(rca.runSlotSets(ctx, stmts))
}

// runSlotSets executes each slot's Lua script, fanning out to goroutines only
// when there is real parallelism. Single-slot deployments hit the inline path;
// ExecMulti already pipelines per-slot scripts.
func (rca *CacheAside) runSlotSets(ctx context.Context, stmts map[uint16]keyOrderAndSet) []slotSetResult {
	results := make([]slotSetResult, 0, len(stmts))
	if len(stmts) <= 1 {
		for _, kos := range stmts {
			results = append(results, rca.runSlotSet(ctx, kos))
		}
		return results
	}
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, kos := range stmts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sr := rca.runSlotSet(ctx, kos)
			mu.Lock()
			results = append(results, sr)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return results
}

// collectSlotSetResults reduces per-slot outcomes to (keys, firstErr). On
// error the successfully written keys are logged for operator reconciliation.
func (rca *CacheAside) collectSlotSetResults(results []slotSetResult) ([]string, error) {
	var succeeded []string
	var firstErr error
	for _, sr := range results {
		succeeded = append(succeeded, sr.keys...)
		if sr.err != nil && firstErr == nil {
			firstErr = sr.err
		}
	}
	if firstErr != nil {
		if len(succeeded) > 0 {
			rca.logger.Error("setMulti partial completion before error", "completedKeys", succeeded, "error", firstErr)
		}
		return nil, firstErr
	}
	return succeeded, nil
}

func (rca *CacheAside) setMultiWithLock(ctx context.Context, ttl time.Duration, keyValLock map[string]valAndLock) ([]string, error) {
	stmts := groupBySlot(keyValLock, ttl)
	return rca.executeSetStatements(ctx, stmts)
}

func (rca *CacheAside) unlockMulti(ctx context.Context, lockVals map[string]string) {
	if len(lockVals) == 0 {
		return
	}
	type keyedExec struct {
		key  string
		exec rueidis.LuaExec
	}
	delStmts := make(map[uint16][]keyedExec)
	for key, lockVal := range lockVals {
		slot := cmdx.Slot(key)
		delStmts[slot] = append(delStmts[slot], keyedExec{
			key: key,
			exec: rueidis.LuaExec{
				Keys: []string{key},
				Args: []string{lockVal},
			},
		})
	}
	var wg sync.WaitGroup
	for slot, stmts := range delStmts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			execsP := luaExecPool.Get(len(stmts))
			defer luaExecPool.Put(execsP)
			execs := *execsP
			for i, s := range stmts {
				execs[i] = s.exec
			}
			// Best effort: locks will expire if unlock fails.
			resps := delKeyLua.ExecMulti(ctx, rca.client, execs...)
			for i, resp := range resps {
				if err := resp.Error(); err != nil {
					rca.logger.Error("failed to unlock key in batch", "key", stmts[i].key, "slot", slot, "error", err)
				}
			}
		}()
	}
	wg.Wait()
}
