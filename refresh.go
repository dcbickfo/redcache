package redcache

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/redis/rueidis"
)

// refreshJob is a unit of work for the refresh worker pool. Exactly one of
// singleFn / multiFn is set; runRefreshJob dispatches on which is non-nil.
type refreshJob struct {
	ctx      context.Context
	ttl      time.Duration
	keys     []string
	expected map[string]string
	singleFn func(ctx context.Context, key string) (string, error)
	multiFn  func(ctx context.Context, keys []string) (map[string]string, error)
}

// startRefreshWorkers launches n workers that drain refreshQueue until Close
// begins. The data channel is never closed because concurrent send + close races
// even with recover.
func (rca *cacheAside) startRefreshWorkers(n int) {
	for range n {
		rca.refreshWg.Add(1)
		go func() {
			defer rca.refreshWg.Done()
			for {
				select {
				case <-rca.refreshDone:
					return
				case job := <-rca.refreshQueue:
					if !rca.shouldRunRefreshJob(job) {
						return
					}
					rca.runRefreshJob(job)
				}
			}
		}()
	}
}

// refreshKeyFor returns the distributed refresh-lock key for a data key.
// The data key is embedded for uniqueness. The wrapper also co-locates plain
// keys by cluster slot, but refresh-ahead correctness does not rely on
// co-location and keys containing Redis hash-tag metacharacters may hash by
// their first tag.
func (rca *cacheAside) refreshKeyFor(key string) string {
	return rca.refreshPrefix + "{" + key + "}"
}

func (rca *cacheAside) shouldRunRefreshJob(job refreshJob) bool {
	if rca.closing.Load() {
		rca.dropRefreshJob(job)
		return false
	}
	select {
	case <-rca.refreshDone:
		rca.dropRefreshJob(job)
		return false
	default:
		return true
	}
}

func (rca *cacheAside) dropRefreshJob(job refreshJob) {
	for _, key := range job.keys {
		rca.refreshing.Delete(key)
	}
	rca.emitRefreshDropped(len(job.keys))
}

// refreshFnTimeout returns the timeout bounding a refresh-ahead callback. It
// defaults to the data ttl (so a slow value gets the same compute budget as a
// cold Get) rather than lockTTL, which would silently cancel any fn slower than
// the lock. WithRefreshTimeout overrides it.
func (rca *cacheAside) refreshFnTimeout(ttl time.Duration) time.Duration {
	if rca.refreshTimeout > 0 {
		return rca.refreshTimeout
	}
	return ttl
}

func (rca *cacheAside) refreshCallbackCtx(ctx context.Context, ttl time.Duration) (context.Context, context.CancelFunc) {
	fnCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rca.refreshFnTimeout(ttl))
	if rca.refreshCtx == nil {
		return fnCtx, cancel
	}
	stopCloseHook := context.AfterFunc(rca.refreshCtx, cancel)
	return fnCtx, func() {
		stopCloseHook()
		cancel()
	}
}

// runRefreshJob runs a refresh-ahead job, recovering from any panic so a
// misbehaving callback cannot kill the worker goroutine. On panic,
// RefreshPanicked fires once per key.
//
// Defer order is LIFO: refreshing-map cleanup runs before panic recovery, so
// in-flight markers are released even when the callback panics.
func (rca *cacheAside) runRefreshJob(job refreshJob) {
	defer func() {
		if r := recover(); r != nil {
			rca.logger.Error("refresh worker panic recovered", "keys", job.keys, "panic", fmt.Sprintf("%v", r), "stack", string(debug.Stack()))
			for _, k := range job.keys {
				rca.emitRefreshPanicked(k)
			}
		}
	}()
	defer func() {
		for _, k := range job.keys {
			rca.refreshing.Delete(k)
		}
	}()
	if job.singleFn != nil {
		rca.doSingleRefresh(job.ctx, job.ttl, job.keys[0], job.expected[job.keys[0]], job.singleFn)
	} else {
		rca.doMultiRefresh(job.ctx, job.ttl, job.keys, job.expected, job.multiFn)
	}
}

// shouldRefresh reports whether the current read should trigger refresh-ahead.
// Above the floor (1-refreshAfter)*ttl, never refresh. Below the floor, if
// delta and RefreshBeta are both > 0, sample probabilistically per Vattani et
// al. (VLDB 2015): refresh when remaining_pttl <= delta * beta * Exp(1). Falls
// back to "always refresh below floor" when delta=0 (legacy values) or
// RefreshBeta=0 (XFetch disabled).
func (rca *cacheAside) shouldRefresh(cachePTTL int64, ttl time.Duration, delta time.Duration) bool {
	if rca.refreshAfter == 0 || cachePTTL <= 0 {
		return false
	}
	floor := time.Duration(float64(ttl) * (1 - rca.refreshAfter))
	cachePTTLd := time.Duration(cachePTTL) * time.Millisecond
	if cachePTTLd >= floor {
		return false
	}
	if delta <= 0 || rca.refreshBeta <= 0 {
		return true
	}
	jitter := time.Duration(float64(delta) * rca.refreshBeta * rand.ExpFloat64())
	return cachePTTLd <= jitter
}

// triggerRefresh enqueues a single-key refresh job to the worker pool.
// Two-level dedup: local syncx.Map + distributed SET NX on a separate refresh key.
// If the queue is full, the refresh is silently dropped.
//
// Safe against concurrent Close: the closing flag is a fast-exit optimization;
// correctness comes from enqueueRefresh's select on refreshDone.
func (rca *cacheAside) triggerRefresh(
	ctx context.Context,
	ttl time.Duration,
	key string,
	expected string,
	fn func(ctx context.Context, key string) (string, error),
) {
	if rca.closing.Load() {
		return
	}
	if _, loaded := rca.refreshing.LoadOrStore(key, struct{}{}); loaded {
		rca.emitRefreshSkipped(1)
		return
	}

	keys := []string{key}
	rca.enqueueRefresh(refreshJob{
		ctx:      ctx,
		ttl:      ttl,
		keys:     keys,
		expected: map[string]string{key: expected},
		singleFn: fn,
	}, keys)
}

// enqueueRefresh sends a job to the refresh queue. The select includes a
// refreshDone case so a concurrent Close unblocks senders without ever
// closing the data channel (sends on closed channels panic).
func (rca *cacheAside) enqueueRefresh(job refreshJob, keys []string) {
	if rca.closing.Load() {
		rca.dropRefreshJob(job)
		return
	}
	select {
	case <-rca.refreshDone:
		rca.dropRefreshJob(job)
		return
	default:
	}

	select {
	case rca.refreshQueue <- job:
		rca.emitRefreshTriggered(len(keys))
	case <-rca.refreshDone:
		rca.dropRefreshJob(job)
	default:
		rca.dropRefreshJob(job)
	}
}

// doSingleRefresh acquires a distributed refresh lock, calls fn, and writes the
// result. The fn runs under refreshFnTimeout (default = data ttl); the refresh
// lock SET NX uses lockTTL; the back-write and lock cleanup use cleanupCtx so a
// slow-but-successful fn still records its value.
func (rca *cacheAside) doSingleRefresh(
	ctx context.Context,
	ttl time.Duration,
	key string,
	expected string,
	fn func(ctx context.Context, key string) (string, error),
) {
	lockCtx, lockCancel := rca.cleanupCtx(ctx)
	defer lockCancel()

	// Distributed dedup: SET NX on a separate refresh lock key. IsRedisNil =
	// "another node is refreshing" (healthy); other errors = real Redis
	// problems, reported separately so operators can distinguish them.
	refreshKey := rca.refreshKeyFor(key)
	refreshLock := rca.lockPool.Generate()
	err := rca.client.Do(lockCtx, rca.client.B().Set().Key(refreshKey).Value(refreshLock).Nx().Px(rca.lockTTL).Build()).Error()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			rca.emitRefreshSkipped(1)
			return
		}
		rca.logger.Error("refresh-ahead lock acquisition failed", "key", key, "error", err)
		rca.emitRefreshError(key)
		return
	}
	defer func() {
		cleanupCtx, cleanupCancel := rca.cleanupCtx(ctx)
		defer cleanupCancel()
		if delErr := delKeyLua.Exec(cleanupCtx, rca.client, []string{refreshKey}, []string{refreshLock}).Error(); delErr != nil {
			rca.logger.Error("refresh-ahead lock release failed", "key", key, "refreshKey", refreshKey, "error", delErr)
		}
	}()

	// The fn runs under its own timeout, decoupled from the back-write below so
	// a fn that runs longer than lockTTL still has its result written.
	fnCtx, fnCancel := rca.refreshCallbackCtx(ctx, ttl)
	defer fnCancel()

	start := time.Now()
	val, err := fn(fnCtx, key)
	if err != nil {
		rca.logger.Error("refresh-ahead callback failed", "key", key, "error", err)
		rca.emitRefreshError(key)
		return
	}
	wrapped := wrapEnvelope(val, time.Since(start))

	setCtx, setCancel := rca.cleanupCtx(ctx)
	defer setCancel()
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	if err := refreshAheadSetScript.Exec(setCtx, rca.client, []string{key}, []string{wrapped, ttlMs, rca.lockPrefix, expected}).Error(); err != nil {
		rca.logger.Error("refresh-ahead set failed", "key", key, "error", err)
		rca.emitRefreshError(key)
	}
}

// triggerMultiRefresh enqueues a multi-key refresh job. Two-level dedup: local
// syncx.Map + distributed SET NX on separate refresh keys. Drops silently when
// the queue is full. Safe against concurrent Close (see triggerRefresh).
func (rca *cacheAside) triggerMultiRefresh(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	expected map[string]string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
) {
	if rca.closing.Load() {
		return
	}
	var toRefresh []string
	var skipped int
	for _, key := range keys {
		if _, loaded := rca.refreshing.LoadOrStore(key, struct{}{}); !loaded {
			toRefresh = append(toRefresh, key)
		} else {
			skipped++
		}
	}
	rca.emitRefreshSkipped(skipped)
	if len(toRefresh) == 0 {
		return
	}

	rca.enqueueRefresh(refreshJob{
		ctx:      ctx,
		ttl:      ttl,
		keys:     toRefresh,
		expected: expected,
		multiFn:  fn,
	}, toRefresh)
}

// doMultiRefresh acquires distributed refresh locks, calls fn, and writes
// results. The fn runs under refreshFnTimeout (default = data ttl); the locks
// use lockTTL and the back-write uses cleanupCtx so a slow-but-successful fn
// still records its values.
func (rca *cacheAside) doMultiRefresh(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	expected map[string]string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
) {
	lockCtx, lockCancel := rca.cleanupCtx(ctx)
	defer lockCancel()

	lockedKeys, refreshLocks := rca.acquireRefreshLocks(lockCtx, keys)
	if len(lockedKeys) == 0 {
		return
	}
	defer rca.deleteRefreshLocks(ctx, refreshLocks)

	fnCtx, fnCancel := rca.refreshCallbackCtx(ctx, ttl)
	defer fnCancel()

	start := time.Now()
	vals, err := fn(fnCtx, lockedKeys)
	if err != nil {
		rca.logger.Error("refresh-ahead multi callback failed", "error", err)
		for _, key := range lockedKeys {
			rca.emitRefreshError(key)
		}
		return
	}

	setCtx, setCancel := rca.cleanupCtx(ctx)
	defer setCancel()
	rca.setRefreshedValues(setCtx, ttl, vals, expected, perValueDelta(time.Since(start), len(vals)))
}

// acquireRefreshLocks batch-acquires distributed SET NX locks for refresh keys,
// distinguishing IsRedisNil (healthy dedup) from real Redis errors.
func (rca *cacheAside) acquireRefreshLocks(ctx context.Context, keys []string) ([]string, map[string]string) {
	cmdsP := commandsPool.Get(len(keys))
	defer commandsPool.Put(cmdsP)
	cmds := *cmdsP
	refreshLocks := make(map[string]string, len(keys))
	for i, key := range keys {
		refreshLock := rca.lockPool.Generate()
		refreshLocks[key] = refreshLock
		cmds[i] = rca.client.B().Set().Key(rca.refreshKeyFor(key)).Value(refreshLock).Nx().Px(rca.lockTTL).Build()
	}
	resps := rca.client.DoMulti(ctx, cmds...)

	var locked []string
	var skipped int
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			if rueidis.IsRedisNil(err) {
				skipped++
			} else {
				rca.logger.Error("refresh-ahead lock acquisition failed", "key", keys[i], "error", err)
				rca.emitRefreshError(keys[i])
			}
			delete(refreshLocks, keys[i])
			continue
		}
		locked = append(locked, keys[i])
	}
	rca.emitRefreshSkipped(skipped)
	return locked, refreshLocks
}

// deleteRefreshLocks removes distributed refresh lock keys (best effort).
// A stuck refresh lock disables refresh-ahead for that key for one lockTTL.
func (rca *cacheAside) deleteRefreshLocks(ctx context.Context, locks map[string]string) {
	cleanupCtx, cleanupCancel := rca.cleanupCtx(ctx)
	defer cleanupCancel()
	for key, refreshLock := range locks {
		refreshKey := rca.refreshKeyFor(key)
		if err := delKeyLua.Exec(cleanupCtx, rca.client, []string{refreshKey}, []string{refreshLock}).Error(); err != nil {
			rca.logger.Error("refresh-ahead lock release failed", "key", key, "refreshKey", refreshKey, "error", err)
		}
	}
}

// setRefreshedValues writes refreshed values via a CAS-style Lua script that
// skips keys currently holding a lock value (so concurrent Get/Set is not
// stomped) or missing entirely (let Get-on-miss handle population).
func (rca *cacheAside) setRefreshedValues(ctx context.Context, ttl time.Duration, vals map[string]string, expected map[string]string, delta time.Duration) {
	if len(vals) == 0 {
		return
	}
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	stmtsP := luaExecPool.GetCap(len(vals))
	defer luaExecPool.Put(stmtsP)
	keyOrder := make([]string, 0, len(vals))
	for key, val := range vals {
		expectedVal, ok := expected[key]
		if !ok {
			continue
		}
		keyOrder = append(keyOrder, key)
		*stmtsP = append(*stmtsP, rueidis.LuaExec{
			Keys: []string{key},
			Args: []string{wrapEnvelope(val, delta), ttlMs, rca.lockPrefix, expectedVal},
		})
	}
	if len(*stmtsP) == 0 {
		return
	}
	resps := refreshAheadSetScript.ExecMulti(ctx, rca.client, *stmtsP...)
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			rca.logger.Error("refresh-ahead multi set failed", "key", keyOrder[i], "error", err)
			rca.emitRefreshError(keyOrder[i])
		}
	}
}
