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
	singleFn func(ctx context.Context, key string) (string, error)
	multiFn  func(ctx context.Context, keys []string) (map[string]string, error)
}

// startRefreshWorkers launches n workers that drain refreshQueue, exiting when
// refreshDone is closed. The data channel is never closed because concurrent
// send + close races even with recover.
func (rca *CacheAside) startRefreshWorkers(n int) {
	for range n {
		rca.refreshWg.Add(1)
		go func() {
			defer rca.refreshWg.Done()
			for {
				select {
				case <-rca.refreshDone:
					return
				case job := <-rca.refreshQueue:
					rca.runRefreshJob(job)
				}
			}
		}()
	}
}

// refreshKeyFor returns the distributed refresh-lock key for a data key.
// The data key is wrapped in a hash tag ("{key}") so the refresh lock hashes
// to the same cluster slot as the data key.
func (rca *CacheAside) refreshKeyFor(key string) string {
	return rca.refreshPrefix + "{" + key + "}"
}

// runRefreshJob runs a refresh-ahead job, recovering from any panic so a
// misbehaving callback cannot kill the worker goroutine. On panic,
// RefreshPanicked fires once per key.
//
// Defer order is LIFO: refreshing-map cleanup runs before panic recovery, so
// in-flight markers are released even when the callback panics.
func (rca *CacheAside) runRefreshJob(job refreshJob) {
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
		rca.doSingleRefresh(job.ctx, job.ttl, job.keys[0], job.singleFn)
	} else {
		rca.doMultiRefresh(job.ctx, job.ttl, job.keys, job.multiFn)
	}
}

// shouldRefresh reports whether the current read should trigger refresh-ahead.
//
// Two stages:
//  1. Floor: while remaining TTL is at or above (1 - refreshAfter) * ttl, never
//     refresh.
//  2. Below floor: if delta and RefreshBeta are both > 0, sample probabilistically
//     per Vattani et al. (VLDB 2015): refresh when remaining_pttl <= delta * beta
//     * Exp(1). Per-read probability climbs to 1 at expiry.
//
// Falls back to "always refresh below floor" when delta is 0 (legacy values)
// or RefreshBeta is 0 (XFetch disabled).
func (rca *CacheAside) shouldRefresh(cachePTTL int64, ttl time.Duration, delta time.Duration) bool {
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
func (rca *CacheAside) triggerRefresh(
	ctx context.Context,
	ttl time.Duration,
	key string,
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
		singleFn: fn,
	}, keys)
}

// enqueueRefresh sends a job to the refresh queue. The select includes a
// refreshDone case so a concurrent Close unblocks senders without ever
// closing the data channel (sends on closed channels panic).
func (rca *CacheAside) enqueueRefresh(job refreshJob, keys []string) {
	select {
	case rca.refreshQueue <- job:
		rca.emitRefreshTriggered(len(keys))
	case <-rca.refreshDone:
		for _, key := range keys {
			rca.refreshing.Delete(key)
		}
		rca.emitRefreshDropped(len(keys))
	default:
		for _, key := range keys {
			rca.refreshing.Delete(key)
		}
		rca.emitRefreshDropped(len(keys))
	}
}

// doSingleRefresh acquires a distributed refresh lock, calls fn, and writes the result.
func (rca *CacheAside) doSingleRefresh(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (string, error),
) {
	refreshCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rca.lockTTL)
	defer cancel()

	// Distributed dedup: SET NX on a separate refresh lock key. IsRedisNil =
	// "another node is refreshing" (healthy); other errors = real Redis
	// problems, reported separately so operators can distinguish them.
	refreshKey := rca.refreshKeyFor(key)
	err := rca.client.Do(refreshCtx, rca.client.B().Set().Key(refreshKey).Value("1").Nx().Px(rca.lockTTL).Build()).Error()
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
		if delErr := rca.client.Do(cleanupCtx, rca.client.B().Del().Key(refreshKey).Build()).Error(); delErr != nil {
			rca.logger.Error("refresh-ahead lock release failed", "key", key, "refreshKey", refreshKey, "error", delErr)
		}
	}()

	start := time.Now()
	val, err := fn(refreshCtx, key)
	if err != nil {
		rca.logger.Error("refresh-ahead callback failed", "key", key, "error", err)
		rca.emitRefreshError(key)
		return
	}
	wrapped := wrapEnvelope(val, time.Since(start))

	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	if err := refreshAheadSetScript.Exec(refreshCtx, rca.client, []string{key}, []string{wrapped, ttlMs, rca.lockPrefix}).Error(); err != nil {
		rca.logger.Error("refresh-ahead set failed", "key", key, "error", err)
		rca.emitRefreshError(key)
	}
}

// triggerMultiRefresh enqueues a multi-key refresh job. Two-level dedup: local
// syncx.Map + distributed SET NX on separate refresh keys. Drops silently when
// the queue is full. Safe against concurrent Close (see triggerRefresh).
func (rca *CacheAside) triggerMultiRefresh(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
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
		ctx:     ctx,
		ttl:     ttl,
		keys:    toRefresh,
		multiFn: fn,
	}, toRefresh)
}

// doMultiRefresh acquires distributed refresh locks, calls fn, and writes results.
func (rca *CacheAside) doMultiRefresh(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
) {
	refreshCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rca.lockTTL)
	defer cancel()

	lockedKeys := rca.acquireRefreshLocks(refreshCtx, keys)
	if len(lockedKeys) == 0 {
		return
	}
	defer rca.deleteRefreshLocks(ctx, lockedKeys)

	start := time.Now()
	vals, err := fn(refreshCtx, lockedKeys)
	if err != nil {
		rca.logger.Error("refresh-ahead multi callback failed", "error", err)
		for _, key := range lockedKeys {
			rca.emitRefreshError(key)
		}
		return
	}

	rca.setRefreshedValues(refreshCtx, ttl, vals, perValueDelta(time.Since(start), len(vals)))
}

// acquireRefreshLocks batch-acquires distributed SET NX locks for refresh keys,
// distinguishing IsRedisNil (healthy dedup) from real Redis errors.
func (rca *CacheAside) acquireRefreshLocks(ctx context.Context, keys []string) []string {
	cmdsP := commandsPool.Get(len(keys))
	defer commandsPool.Put(cmdsP)
	cmds := *cmdsP
	for i, key := range keys {
		cmds[i] = rca.client.B().Set().Key(rca.refreshKeyFor(key)).Value("1").Nx().Px(rca.lockTTL).Build()
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
			continue
		}
		locked = append(locked, keys[i])
	}
	rca.emitRefreshSkipped(skipped)
	return locked
}

// deleteRefreshLocks removes distributed refresh lock keys (best effort).
// A stuck refresh lock disables refresh-ahead for that key for one lockTTL.
func (rca *CacheAside) deleteRefreshLocks(ctx context.Context, keys []string) {
	cleanupCtx, cleanupCancel := rca.cleanupCtx(ctx)
	defer cleanupCancel()
	delCmdsP := commandsPool.Get(len(keys))
	defer commandsPool.Put(delCmdsP)
	delCmds := *delCmdsP
	for i, key := range keys {
		delCmds[i] = rca.client.B().Del().Key(rca.refreshKeyFor(key)).Build()
	}
	resps := rca.client.DoMulti(cleanupCtx, delCmds...)
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			rca.logger.Error("refresh-ahead lock release failed", "key", keys[i], "error", err)
		}
	}
}

// setRefreshedValues writes refreshed values via a CAS-style Lua script that
// skips keys currently holding a lock value (so concurrent Get/Set is not
// stomped) or missing entirely (let Get-on-miss handle population).
func (rca *CacheAside) setRefreshedValues(ctx context.Context, ttl time.Duration, vals map[string]string, delta time.Duration) {
	if len(vals) == 0 {
		return
	}
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	stmtsP := luaExecPool.GetCap(len(vals))
	defer luaExecPool.Put(stmtsP)
	keyOrder := make([]string, 0, len(vals))
	for key, val := range vals {
		keyOrder = append(keyOrder, key)
		*stmtsP = append(*stmtsP, rueidis.LuaExec{
			Keys: []string{key},
			Args: []string{wrapEnvelope(val, delta), ttlMs, rca.lockPrefix},
		})
	}
	resps := refreshAheadSetScript.ExecMulti(ctx, rca.client, *stmtsP...)
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			rca.logger.Error("refresh-ahead multi set failed", "key", keyOrder[i], "error", err)
			rca.emitRefreshError(keyOrder[i])
		}
	}
}
