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

// Pools for slice headers reused across multi-key call paths. Saves one alloc
// per call on the dominant hit path. Drops slices >1024 to avoid pinning
// oversized backing arrays.
var (
	cacheableTTLPool = poolx.NewSlice(func() []rueidis.CacheableTTL { return make([]rueidis.CacheableTTL, 0, 16) })
	commandsPool     = poolx.NewSlice(func() []rueidis.Completed { return make([]rueidis.Completed, 0, 16) })
	luaExecPool      = poolx.NewSlice(func() []rueidis.LuaExec { return make([]rueidis.LuaExec, 0, 16) })
	stringPool       = poolx.NewSlice(func() []string { return make([]string, 0, 16) })
	chanPool         = poolx.NewSlice(func() []<-chan struct{} { return make([]<-chan struct{}, 0, 16) })
)

// Default prefixes used by CacheAside. These can be overridden via
// CacheAsideOption.LockPrefix and CacheAsideOption.RefreshLockPrefix.
const (
	DefaultLockPrefix    = "__redcache:lock:"
	DefaultRefreshPrefix = "__redcache:refresh:"
)

// lockEntry tracks a key's wait channel and TTL timer.
//
// Compared to a context.WithTimeout-based design, this saves ~3 heap allocations
// per slow-path register (cancelCtx, timer struct, propagateCancel) — meaningful
// on multi-key paths where 10 registers happen per call. cancel() and the timer's
// fire path both close done idempotently via sync.Once, but only the external
// cancel path reads le.timer — the timer-fire path does not, since the timer
// expiring is itself the synchronization edge that publishes le.timer to no one
// (avoiding a data race against the write in register()).
type lockEntry struct {
	done  chan struct{}
	once  sync.Once
	timer *time.Timer
}

// cancel is for external callers (onInvalidate, Close, register race-loss).
// It Stops the timer and closes done. Safe across concurrent invocations.
func (le *lockEntry) cancel() {
	le.once.Do(func() {
		if le.timer != nil {
			le.timer.Stop()
		}
		close(le.done)
	})
}

// timerExpired is the timer-fire path. It does not read le.timer because the
// timer is itself firing, and reading the field would race with the assignment
// in register's slow path.
func (le *lockEntry) timerExpired() {
	le.once.Do(func() {
		close(le.done)
	})
}

// Logger defines the logging interface used by CacheAside.
// Implementations must be safe for concurrent use and should handle log levels internally.
type Logger interface {
	// Error logs error messages. Should be used for unexpected failures or critical issues.
	Error(msg string, args ...any)
	// Debug logs detailed diagnostic information useful for development and troubleshooting.
	// Call Debug to record verbose output about internal state, cache operations, or lock handling.
	// Debug messages should not include sensitive information and may be omitted in production.
	Debug(msg string, args ...any)
}

// CacheAside provides a cache-aside pattern backed by Redis with distributed locking
// and client-side caching via rueidis invalidation messages.
type CacheAside struct {
	client         rueidis.Client
	locks          syncx.Map[string, *lockEntry]
	lockPool       *lockpool.Pool // generates lock values without per-call uuid.NewV7
	lockTTL        time.Duration
	lockTTLMs      string // pre-formatted lockTTL.Milliseconds() — used as Lua arg on every Set/SetMulti
	logger         Logger
	metrics        Metrics
	metricsEnabled bool // false when metrics is NoopMetrics{} — gates hot-path emits
	lockPrefix     string
	refreshAfter   float64                     // 0 = disabled
	refreshBeta    float64                     // XFetch beta; 0 = simple floor only
	refreshing     syncx.Map[string, struct{}] // dedup in-flight refreshes (local)
	refreshPrefix  string                      // prefix for distributed refresh lock keys
	refreshQueue   chan refreshJob             // worker pool job queue (nil when disabled)
	refreshDone    chan struct{}               // closed by Close to signal workers/senders; never the data channel
	refreshWg      sync.WaitGroup              // tracks active refresh workers
	closing        atomic.Bool                 // set true at the start of Close to gate refresh sends
	closeOnce      sync.Once                   // guards Close() against double invocation
}

// CacheAsideOption configures a CacheAside instance.
type CacheAsideOption struct {
	// LockTTL is the maximum time a lock can be held, and also the timeout for waiting
	// on locks when handling lost Redis invalidation messages. Defaults to 10 seconds.
	LockTTL time.Duration
	// ClientBuilder optionally overrides how the rueidis.Client is created.
	// When nil, rueidis.NewClient is used.
	ClientBuilder func(option rueidis.ClientOption) (rueidis.Client, error)
	// Logger for logging errors and debug information. Defaults to slog.Default().
	// The logger should handle log levels internally (e.g., only log Debug if level is enabled).
	Logger Logger
	// Metrics receives observability events. Defaults to NoopMetrics.
	// Implementations must be concurrent-safe; methods run on the hot path.
	Metrics Metrics
	// LockPrefix for distributed locks. Defaults to DefaultLockPrefix.
	// Choose a prefix unlikely to conflict with your data keys.
	LockPrefix string
	// RefreshLockPrefix is the key prefix used for distributed refresh-ahead locks.
	// Defaults to DefaultRefreshPrefix. Refresh keys also include a hash tag wrapping
	// the data key so they hash to the same Redis cluster slot as the data key
	// (e.g. "__redcache:refresh:{user:123}").
	RefreshLockPrefix string
	// RefreshAfterFraction enables refresh-ahead caching. When a cached value
	// is returned and more than this fraction of its TTL has elapsed, a
	// background worker refreshes the value while the stale one is returned
	// immediately. For example, 0.8 means "refresh after 80% of TTL has passed"
	// (i.e., when 20% remains). Set to 0 (default) to disable. Must be in [0, 1).
	//
	// The refresh threshold is based on the client-side cache TTL (CachePTTL),
	// which tracks the remaining lifetime of the locally cached entry. This closely
	// approximates the server-side TTL when the same ttl parameter is used
	// consistently for a given key across Get calls.
	//
	// RefreshAfterFraction is the floor: while the remaining TTL is above
	// (1 - RefreshAfterFraction) * ttl, refresh never fires. Below the floor,
	// XFetch sampling decides whether each individual read triggers refresh
	// (see RefreshBeta).
	RefreshAfterFraction float64
	// RefreshBeta scales the XFetch probabilistic-refresh window. Higher values
	// trigger refresh earlier within the floor; the per-read probability of
	// firing climbs as the value approaches expiry, weighted by how long the
	// value took to compute (delta) so slow-to-recompute values get more
	// headroom. Default 0 (XFetch disabled — always refresh below floor); set
	// to 1.0 for the canonical XFetch behaviour from Vattani et al. XFetch only
	// applies to values written with envelope metadata; legacy values (no delta
	// recorded) always use the simple floor-based behaviour regardless of
	// RefreshBeta.
	RefreshBeta float64
	// RefreshWorkers is the number of background workers that process refresh-ahead
	// jobs. Defaults to 4 when RefreshAfterFraction > 0. Must be > 0 when refresh
	// is enabled.
	RefreshWorkers int
	// RefreshQueueSize is the maximum number of pending refresh jobs. When the queue
	// is full, new refresh requests are silently dropped — the stale value continues
	// to be served until the next access. Defaults to 64 when RefreshAfterFraction > 0.
	// Must be > 0 when refresh is enabled.
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
	// invalidation stream share the same pipe — same convention rueidislock
	// uses. Avoids cross-connection invalidation routing and keeps the
	// wait-and-retry loop tight. Unconditional override (matches the existing
	// OnInvalidations override pattern); users needing different behavior can
	// override via CacheAsideOption.ClientBuilder.
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
// If refresh-ahead is enabled, Close waits for in-flight refresh jobs to complete
// (bounded by LockTTL). Safe to call multiple times.
//
// Shutdown signals workers via the refreshDone channel rather than closing the
// data channel. Concurrent send + close on the same channel is a data race even
// when the panic is recovered; closing only the signal channel keeps refreshQueue
// senders race-free since closed channels are read-safe but write-unsafe.
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

// cleanupCtx returns a context derived from ctx that strips cancellation/deadline
// (so cleanup can run even after the original request completes) but bounds the
// total wait at lockTTL. Callers MUST defer the returned cancel.
func (rca *CacheAside) cleanupCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), rca.lockTTL)
}

// Metric emit helpers. Each gates on rca.metricsEnabled so call sites stay
// single-statement and the noop case compiles to a single bool check.
// Count-based helpers also short-circuit on n <= 0 to avoid emitting zeros.

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
// resolved-wait duration. Returns nil when the lock released (caller retries)
// and ctx.Err() on cancellation. The duration is the metric — short waits
// indicate healthy contention, waits at LockTTL indicate the lock holder
// stalled.
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
// the elapsed wait time once. The duration captures user-visible latency on a
// contended GetMulti — emitting per-channel would inflate histograms with
// sub-waits already covered by the worst-case channel.
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
			entry.cancel() // Cancel context, which closes the channel
		}
	}
}

// register publishes a per-key lockEntry and returns its done channel along
// with a leader flag. leader=true means this caller created the entry and is
// responsible for driving the Redis-side work (tryLock + fn + setWithLock);
// leader=false means another in-process caller already owns that work and the
// follower should wait on done instead of re-issuing SET NX. The follower path
// avoids N-1 SET NX commands per key when many goroutines miss the same key.
func (rca *CacheAside) register(key string) (<-chan struct{}, bool) {
retry:
	// Fast path: entry already registered. Avoid allocating a new lockEntry +
	// timer that would be immediately discarded when LoadOrStore loses the
	// race — the dominant cost for cache-hit Gets.
	if actual, ok := rca.locks.Load(key); ok {
		select {
		case <-actual.done:
			// Stale entry - try to atomically delete it and retry. On failure,
			// another goroutine raced us; retry to pick up whatever they did.
			rca.locks.CompareAndDelete(key, actual)
			goto retry
		default:
			return actual.done, false
		}
	}

	// Slow path: create new entry. The timer must be assigned BEFORE LoadOrStore
	// publishes newEntry, otherwise a concurrent cancel() could observe a partial
	// struct (newEntry.timer not yet written). On race-loss we Stop the timer
	// via cancel() — same parity cost as the old context.WithTimeout design.
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
		// Their entry is stale; remove and retry.
		rca.locks.CompareAndDelete(key, actual)
		goto retry
	default:
		return actual.done, false
	}
}

// Get returns the cached value for key, populating the cache by calling fn on a miss.
// Only one goroutine across all instances executes fn for a given key at a time;
// other callers wait for the result via Redis invalidation messages.
//
// Empty-string values are valid: an empty value present in Redis is returned as
// a cache hit, not treated as a miss.
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
		// Cache hit. Empty values are valid hits; tryGet returns errNotFound
		// for actually-missing or lock-prefixed entries.
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
		// Follower: another in-process caller created the lockEntry and will
		// drive the Redis-side population. Skip our own SET NX (saves one
		// round trip per follower) and wait for the leader's invalidation.
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
		// errLockFailed: another process holds the Redis lock — wait then retry.
		// We don't cancel our local lockEntry here; doing so would wake our
		// followers prematurely and they would busy-loop, each becoming a
		// new leader and racing the same Redis NX. Instead we wait alongside
		// them for the actual Redis holder's invalidation.
		// ErrLockLost: a ForceSet (or similar) stole our lock — retry to read it.
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
// the first error encountered is returned wrapped with key context. Some deletes
// may have succeeded.
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

// Touch extends the TTL of a cached value to ttl. It is a no-op when the key
// is missing or currently holds a lock value, so it cannot accidentally extend
// an in-flight Get/Set lock. PEXPIRE does not trigger client-side cache
// invalidation, so existing readers continue to serve from their local copy.
//
// Use Touch to implement sliding-TTL semantics (sessions, tokens) without
// re-running the origin function.
func (rca *CacheAside) Touch(ctx context.Context, ttl time.Duration, key string) error {
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	if err := touchScript.Exec(ctx, rca.client, []string{key}, []string{ttlMs, rca.lockPrefix}).Error(); err != nil {
		return fmt.Errorf("touch key %q: %w", key, err)
	}
	return nil
}

// TouchMulti extends the TTL of multiple cached values to ttl. Like Touch, each
// per-key extension is a no-op on missing or lock-held keys. Per-key errors
// are logged and the first one is returned wrapped with key context; some
// keys may have been extended.
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

// runTouchSlots executes the touch script per slot in parallel, logging
// per-key errors and returning the first error with its key.
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

// touchSlot runs the touch script for one slot's keys. Returns the first error
// encountered (with its key) for the slot; per-key errors are logged.
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

// ErrLockLost is defined in errors.go.

// cacheReadResult is the return type of tryGet: the cached value (when present),
// the client-side cache PTTL used to drive refresh-ahead decisions, and the
// recorded compute delta from the value's envelope (0 for legacy values).
type cacheReadResult struct {
	val   string
	pttl  int64
	delta time.Duration
}

func (rca *CacheAside) tryGet(ctx context.Context, ttl time.Duration, key string) (cacheReadResult, error) {
	resp := rca.client.DoCache(ctx, rca.client.B().Get().Key(key).Cache(), ttl)
	val, err := resp.ToString()
	if rueidis.IsRedisNil(err) || strings.HasPrefix(val, rca.lockPrefix) { // no response or is a lock value
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
			// Best effort unlock - errors are non-fatal as lock will expire
			if err := rca.unlock(toCtx, key, lockVal); err != nil {
				rca.logger.Error("failed to unlock key", "key", key, "error", err)
			}
		}
	}()
	start := time.Now()
	if val, err = fn(ctx, key); err == nil {
		// XFetch metadata: pair the user value with how long fn took so future
		// reads can probabilistically refresh slow-to-recompute values earlier.
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
	//   IsRedisNil: NX fired, lock acquired (no prior value).
	//   nil error:  key existed; NX rejected. Lock not acquired.
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
	// The Lua script returns 0 when the lock was lost (CAS mismatch), 1 on success.
	// A non-integer response means the script drifted (regression bait per setKeyLua).
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

// GetMulti returns cached values for the given keys, populating any misses by calling fn.
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

	// Parallel slices: pending[i] is an unresolved key, chans[i] is its wait
	// channel from register(). Backed by stack-friendly slices instead of a
	// map+channel-of-channels — avoids hash inserts and mapsx.Keys/Values
	// conversions on every iteration. Pooled so the dominant cache-hit path
	// stays alloc-free for these two slice headers.
	pendingP := stringPool.Get(len(keys))
	defer stringPool.Put(pendingP)
	pending := *pendingP
	copy(pending, keys)

	chansP := chanPool.Get(len(keys))
	defer chanPool.Put(chansP)
	chans := *chansP

	// Pooled scratch slice for refresh-ahead candidates. Caller-owned so each
	// retry-iteration's tryGetMulti reuses the same backing array — saves the
	// per-iteration alloc on the refresh-ahead hot path. triggerMultiRefresh
	// copies into its own toRefresh slice, so reusing the buffer is safe.
	needRefreshP := stringPool.GetCap(len(keys))
	defer stringPool.Put(needRefreshP)

	// Pooled scratch slice holding the keys for which this caller created the
	// in-process lockEntry (leaders) — followers stay out of this slice and
	// skip the Redis SET NX round trip, waiting on chans[i] instead. Rebuilt
	// each retry iteration so it always reflects the current iteration's
	// register() outcomes.
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
	if err != nil && !rueidis.IsRedisNil(err) {
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
		// Wait for lock releases (channels auto-close after lockTTL or on invalidation).
		// Pending here is followers + any leaders whose Redis NX lost the race —
		// both wait for the actual Redis lock holder's invalidation.
		rca.emitLockContended(len(pending))
		if err = rca.awaitLockMulti(ctx, chans); err != nil {
			return nil, ctx.Err()
		}
		goto retry
	}
	return res, nil
}

// runLeaderSets filters leaderKeys to drop entries already populated by
// tryGetMulti (e.g. via a CSC invalidation that landed mid-call) then drives
// trySetMultiKeyFn for the remaining leaders. Followers among pending stay
// out of leaderKeys entirely — they're handled by the caller's awaitLockMulti
// step.
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

// filterResolved drops keys present in resolved from pending+chans in-place
// via swap-keep-shrink, returning the trimmed views over the same backing
// arrays. The two slices stay index-aligned so each remaining key keeps its
// wait channel.
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

// tryGetMulti reads keys from the client-side cache, writing hit values
// directly into res. Returns the subset of hit keys whose remaining TTL has
// crossed the refresh threshold.
// tryGetMulti reads keys via DoMultiCache, populates `res` with non-lock
// values, and appends refresh-ahead candidates onto `needRefresh` (caller
// passes in a pooled empty slice with len=0, cap=len(keys)). Returns the
// appended slice so the caller can update its pool handle.
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

// trySetMultiKeyFn locks each pending key, calls fn for the values, sets them
// in Redis, and writes the successfully-set entries directly into res.
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
	// XFetch metadata: amortise total fn time across the values it produced.
	// Skewed estimate (computing 100 keys in parallel reports the same per-key
	// delta as a serial loop), but it's better than 0: it gives slow batches
	// proportionally more refresh headroom and remains well-defined when fn
	// returns fewer values than were requested.
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
// values it produced. Returns 0 when n <= 0 to avoid a divide and to keep the
// XFetch metadata "absent" (which makes shouldRefresh fall back to the floor).
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
	//   IsRedisNil:  NX fired, lock acquired (no prior value).
	//   nil error:   key existed; NX rejected. Drop so caller waits for release.
	//   other err:   real Redis error — propagate so the caller fails fast.
	//
	// DoMulti is a pipeline: by the time we read resps, every SET NX has
	// already executed in Redis. Drain ALL responses before returning so
	// successfully-acquired locks from earlier indices can be released; an
	// early return would leak them for the full lockTTL.
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

// groupBySlot groups keys by their Redis cluster slot for efficient batching.
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

// runSlotSet executes the Lua statements for one slot and reports its result.
// All responses are inspected even if one is a non-Redis-nil error, so successes
// in the same batch are not lost when reporting an error. Lua returning 0 (or a
// nil response, which the script never produces but we treat defensively the
// same way as setWithLock) indicates a CAS-mismatch (lock lost via TTL expiry
// or ForceSet) — the key is dropped from successes and a LockLost metric is
// emitted, matching setWithLock.
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

// inspectSlotSetResponse classifies one ExecMulti response into success / silent
// failure (LockLost) / surfaceable error. Parse errors are surfaced so a script
// drift fails fast instead of triggering an infinite retry loop in the caller's
// wait-and-retry path — matching setWithLock's contract.
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

// executeSetStatements runs the per-slot Lua set scripts and reduces their
// results to (succeeded keys, first error). Slot work runs to completion before
// the reduce step so a real Redis error in one slot cannot mask successful
// writes in another.
func (rca *CacheAside) executeSetStatements(ctx context.Context, stmts map[uint16]keyOrderAndSet) ([]string, error) {
	return rca.collectSlotSetResults(rca.runSlotSets(ctx, stmts))
}

// runSlotSets executes each slot's Lua script, fanning out to goroutines only
// when there is actually parallelism to exploit. Single-slot deployments
// (non-cluster Redis) hit the inline path and skip the goroutine + sync costs;
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

// collectSlotSetResults reduces per-slot outcomes to a single (keys, error)
// pair. On error the successfully written keys are logged so operators can
// reconcile partial state.
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
			// Best effort unlock - errors are non-fatal as locks will expire.
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
