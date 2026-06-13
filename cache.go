// Package redcache implements a Redis cache-aside built on rueidis. It uses
// rueidis client-side caching plus Redis SET NX locking so only one goroutine
// (across all processes) populates a missing key, with the rest waiting on the
// invalidation push for the populated value.
package redcache

import (
	"context"
	"errors"
	"fmt"
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

// Logger is the slog-shaped subset the cache calls into. *slog.Logger satisfies it.
type Logger interface {
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
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

func (rca *cacheAside) emitCacheHits(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.CacheHits(int64(n))
	}
}

func (rca *cacheAside) emitCacheMisses(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.CacheMisses(int64(n))
	}
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

func (rca *cacheAside) emitLockWaitDuration(d time.Duration) {
	if rca.metricsEnabled {
		rca.metrics.LockWaitDuration(d)
	}
}

func (rca *cacheAside) emitLockContended(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.LockContended(int64(n))
	}
}

func (rca *cacheAside) emitRefreshTriggered(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.RefreshTriggered(int64(n))
	}
}

func (rca *cacheAside) emitRefreshSkipped(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.RefreshSkipped(int64(n))
	}
}

func (rca *cacheAside) emitRefreshDropped(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.RefreshDropped(int64(n))
	}
}

func (rca *cacheAside) emitLockLost(key string) {
	if rca.metricsEnabled {
		rca.metrics.LockLost(key)
	}
}

func (rca *cacheAside) emitRefreshError(key string) {
	if rca.metricsEnabled {
		rca.metrics.RefreshError(key)
	}
}

func (rca *cacheAside) emitRefreshPanicked(key string) {
	if rca.metricsEnabled {
		rca.metrics.RefreshPanicked(key)
	}
}

func (rca *cacheAside) emitInvalidationError() {
	if rca.metricsEnabled {
		rca.metrics.InvalidationError()
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

// get returns the cached value for key, calling fn on a miss. Only one
// goroutine across all processes runs fn for a given key; others wait on the
// resulting invalidation. Empty strings are valid hits.
func (rca *cacheAside) get(
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
		// errLockFailed: another process holds the Redis lock — wait alongside
		// followers (cancelling our entry would wake them to race the same NX).
		// ErrLockLost: a ForceSet stole our lock; retry to read its value.
		rca.emitLockContended(1)
		if werr := rca.awaitLock(ctx, wait); werr != nil {
			return "", werr
		}
		goto retry
	}

	return "", err
}

// del removes a key, triggering invalidation on all subscribed clients.
func (rca *cacheAside) del(ctx context.Context, key string) error {
	return rca.client.Do(ctx, rca.client.B().Del().Key(key).Build()).Error()
}

// delMulti deletes keys. Per-key errors are logged; the first is returned.
func (rca *cacheAside) delMulti(ctx context.Context, keys ...string) error {
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

// touch extends a cached value's TTL via PEXPIRE. No-ops on missing key or
// lock value (so it can't extend an in-flight lock). PEXPIRE doesn't push
// invalidations, so existing readers keep serving from their local copy.
func (rca *cacheAside) touch(ctx context.Context, ttl time.Duration, key string) error {
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	if err := touchScript.Exec(ctx, rca.client, []string{key}, []string{ttlMs, rca.lockPrefix}).Error(); err != nil {
		return fmt.Errorf("touch key %q: %w", key, err)
	}
	return nil
}

// touchMulti is touch over many keys. Per-key errors are logged; the first
// is returned.
func (rca *cacheAside) touchMulti(ctx context.Context, ttl time.Duration, keys ...string) error {
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

func (rca *cacheAside) groupTouchExecs(ttl time.Duration, keys []string) map[uint16][]touchExec {
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

func (rca *cacheAside) runTouchSlots(ctx context.Context, slots map[uint16][]touchExec) (string, error) {
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

func (rca *cacheAside) touchSlot(ctx context.Context, stmts []touchExec) (string, error) {
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

// cacheReadResult is tryGet's return: value, client-side PTTL, recorded
// compute delta (0 for legacy values).
type cacheReadResult struct {
	val   string
	pttl  int64
	delta time.Duration
}

func (rca *cacheAside) tryGet(ctx context.Context, ttl time.Duration, key string) (cacheReadResult, error) {
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

func (rca *cacheAside) trySetKeyFunc(ctx context.Context, ttl time.Duration, key string, fn func(ctx context.Context, key string) (string, error)) (val string, err error) {
	setVal := false
	lockVal, err := rca.tryLock(ctx, key)
	if err != nil {
		return "", err
	}
	defer func() {
		if !setVal {
			toCtx, cancel := rca.cleanupCtx(ctx)
			defer cancel()
			if err := rca.unlock(toCtx, key, lockVal); err != nil {
				rca.logger.Error("failed to unlock key", "key", key, "error", err)
			}
		}
	}()
	start := time.Now()
	if val, err = fn(ctx, key); err == nil {
		wrapped := wrapEnvelope(val, time.Since(start))
		if _, err = rca.setWithLock(ctx, ttl, key, valAndLock{wrapped, lockVal}); err == nil {
			setVal = true
		}
		return val, err
	}
	return "", err
}

func (rca *cacheAside) tryLock(ctx context.Context, key string) (string, error) {
	lockVal := rca.lockPool.Generate()
	err := rca.client.Do(ctx, rca.client.B().Set().Key(key).Value(lockVal).Nx().Get().Px(rca.lockTTL).Build()).Error()
	// SET NX GET: IsRedisNil = lock acquired; nil = NX rejected; other = real
	// error — propagate so callers fail fast instead of waiting on a channel
	// that never closes.
	if rueidis.IsRedisNil(err) {
		return lockVal, nil
	}
	if err == nil {
		return "", fmt.Errorf("lock key %q: %w", key, errLockFailed)
	}
	return "", fmt.Errorf("lock key %q: %w", key, err)
}

func (rca *cacheAside) setWithLock(ctx context.Context, ttl time.Duration, key string, valLock valAndLock) (string, error) {
	resp := setKeyLua.Exec(ctx, rca.client, []string{key}, []string{valLock.lockVal, valLock.val, strconv.FormatInt(ttl.Milliseconds(), 10)})
	if err := resp.Error(); err != nil {
		if !rueidis.IsRedisNil(err) {
			return "", fmt.Errorf("set key %q: %w", key, err)
		}
		rca.emitLockLost(key)
		return "", fmt.Errorf("lock lost for key %q: %w", key, ErrLockLost)
	}
	// 0 = CAS lost; 1 = success. Anything else = script drift.
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

func (rca *cacheAside) unlock(ctx context.Context, key string, lock string) error {
	return delKeyLua.Exec(ctx, rca.client, []string{key}, []string{lock}).Error()
}

// getMulti returns cached values for keys, calling fn for misses. SETs are
// grouped by cluster slot.
func (rca *cacheAside) getMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, key []string) (val map[string]string, err error),
) (map[string]string, error) {
	if len(keys) == 0 {
		return map[string]string{}, nil
	}
	res := make(map[string]string, len(keys))

	// pending[i] / chans[i] are index-aligned: unresolved key + its wait channel.
	pendingP := stringPool.Get(len(keys))
	defer stringPool.Put(pendingP)
	pending := *pendingP
	copy(pending, keys)

	chansP := chanPool.Get(len(keys))
	defer chanPool.Put(chansP)
	chans := *chansP

	// triggerMultiRefresh copies into its own slice, so we can reuse the buffer.
	needRefreshP := stringPool.GetCap(len(keys))
	defer stringPool.Put(needRefreshP)

	// Leader keys are rebuilt each retry: a leader created the lockEntry and
	// drives Redis-side work; followers skip SET NX and wait on chans[i].
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
		// Followers + leaders whose NX lost: wait for the holder's invalidation
		// (or lockTTL).
		rca.emitLockContended(len(pending))
		if err = rca.awaitLockMulti(ctx, chans); err != nil {
			return nil, err
		}
		goto retry
	}
	return res, nil
}

// runLeaderSets filters out leaderKeys that tryGetMulti already populated
// (CSC invalidation can land mid-call), then SETs the rest.
func (rca *cacheAside) runLeaderSets(
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

// filterResolved drops keys present in resolved from pending+chans in place,
// keeping the slices index-aligned.
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
// and appends refresh-ahead candidates onto needRefresh (returned so callers
// can update their pool handle).
func (rca *cacheAside) tryGetMulti(ctx context.Context, ttl time.Duration, keys []string, res map[string]string, needRefresh []string) ([]string, error) {
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

// trySetMultiKeyFn locks each pending key, calls fn, writes the values, and
// records successes in res.
func (rca *cacheAside) trySetMultiKeyFn(
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
	// Amortise fn time across returned values for XFetch sampling. Skewed but
	// better than 0 — slow batches get proportionally more refresh headroom.
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

// perValueDelta divides total by n. Returns 0 when n<=0 (shouldRefresh then
// falls back to the floor check).
func perValueDelta(total time.Duration, n int) time.Duration {
	if n <= 0 {
		return 0
	}
	return total / time.Duration(n)
}

func (rca *cacheAside) tryLockMulti(ctx context.Context, keys []string) (map[string]string, error) {
	lockVals := make(map[string]string, len(keys))
	cmdsP := commandsPool.GetCap(len(keys))
	defer commandsPool.Put(cmdsP)
	for _, k := range keys {
		lockVals[k] = rca.lockPool.Generate()
		*cmdsP = append(*cmdsP, rca.client.B().Set().Key(k).Value(lockVals[k]).Nx().Get().Px(rca.lockTTL).Build())
	}
	resps := rca.client.DoMulti(ctx, *cmdsP...)
	// Drain every response: DoMulti has already executed all SET NXs in the
	// pipeline, so an early return would leak the later acquires for lockTTL.
	// IsRedisNil = acquired; nil = NX rejected; other = propagate.
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

// runSlotSet executes one slot's CAS-set scripts. Every response is inspected
// so successes survive a sibling error; Lua=0 (or nil) is a lock-lost.
func (rca *cacheAside) runSlotSet(ctx context.Context, kos keyOrderAndSet) slotSetResult {
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

// inspectSlotSetResponse classifies one CAS-set response: success, silent
// lock-lost, or surfaceable error. Parse errors are surfaced so script drift
// can't trigger an infinite retry loop.
func (rca *cacheAside) inspectSlotSetResponse(key string, resp rueidis.RedisResult) (bool, error) {
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

// executeSetStatements runs the per-slot scripts then reduces to (keys, err).
// Slot work runs to completion before the reduce so an error in one slot
// can't mask successes in another.
func (rca *cacheAside) executeSetStatements(ctx context.Context, stmts map[uint16]keyOrderAndSet) ([]string, error) {
	return rca.collectSlotSetResults(rca.runSlotSets(ctx, stmts))
}

// runSlotSets fans out to goroutines only when there's real parallelism;
// single-slot deployments hit the inline path (ExecMulti pipelines per-slot).
func (rca *cacheAside) runSlotSets(ctx context.Context, stmts map[uint16]keyOrderAndSet) []slotSetResult {
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

// collectSlotSetResults reduces per-slot outcomes. On error, succeeded keys
// are logged for operator reconciliation.
func (rca *cacheAside) collectSlotSetResults(results []slotSetResult) ([]string, error) {
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

func (rca *cacheAside) setMultiWithLock(ctx context.Context, ttl time.Duration, keyValLock map[string]valAndLock) ([]string, error) {
	stmts := groupBySlot(keyValLock, ttl)
	return rca.executeSetStatements(ctx, stmts)
}

func (rca *cacheAside) unlockMulti(ctx context.Context, lockVals map[string]string) {
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
			// Best effort — locks expire on lockTTL anyway.
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
