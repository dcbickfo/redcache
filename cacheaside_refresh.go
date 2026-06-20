package redcache

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/cmdx"
	"github.com/dcbickfo/redcache/internal/mapsx"
)

// refreshBudget returns how long a refresh-ahead callback may run. RefreshTimeout
// overrides the per-call ttl when set; otherwise the value's own ttl is the
// compute budget.
func (rca *CacheAside) refreshBudget(ttl time.Duration) time.Duration {
	if rca.refreshTimeout > 0 {
		return rca.refreshTimeout
	}
	return ttl
}

// refreshJob is a unit of work for the refresh worker pool. Holding fields
// directly (rather than a closure) avoids an allocation per trigger on the
// hot path. Exactly one of singleFn / multiFn is set; runRefreshJob dispatches
// based on which is non-nil.
type refreshJob struct {
	ctx      context.Context
	ttl      time.Duration
	keys     []string
	singleFn func(ctx context.Context, key string) (string, error)
	multiFn  func(ctx context.Context, keys []string) (map[string]string, error)
}

// startRefreshWorkers launches n background workers that drain refreshQueue.
// Each worker exits when refreshDone is closed (during Close). The data channel
// is never closed because concurrent send + close races even with recover.
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
// The data key is wrapped in a Redis cluster hash tag ("{key}") so the refresh
// lock always hashes to the same slot as the data key, keeping a key and its
// refresh lock co-located in cluster deployments.
func (rca *CacheAside) refreshKeyFor(key string) string {
	return rca.refreshPrefix + "{" + key + "}"
}

// runRefreshJob runs a refresh-ahead job, recovering from any panic so a
// misbehaving callback cannot kill the worker goroutine and degrade the pool.
// On panic, RefreshPanicked fires once per key the job was operating on.
//
// Defer order is LIFO, so refreshing-map cleanup runs before panic recovery —
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
// Two-stage decision:
//  1. Floor: while the remaining TTL is at or above (1 - refreshAfter) * ttl,
//     never refresh — fresh values are left alone regardless of XFetch noise.
//  2. Below floor: if XFetch metadata is available (delta from envelope) and
//     RefreshBeta > 0, sample probabilistically per Vattani et al. (VLDB 2015):
//     refresh when remaining_pttl <= delta * beta * Exp(1). Per-read
//     probability climbs to 1 at expiry, weighted by how slow the value is to
//     recompute.
//
// Falls back to "always refresh below floor" when delta is unknown (legacy
// values written before envelope wrapping) or RefreshBeta=0 (operator opted
// out of XFetch).
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
// If the queue is full, the refresh is silently dropped (stale value is still served).
//
// Safe against concurrent Close: the closing-flag check is a fast-exit
// optimization, not a correctness guarantee — Close can still flip the flag
// after we read it. Correctness comes from enqueueRefresh's select on
// refreshDone, which unblocks the sender if Close races us to send (we only
// ever close the signal channel refreshDone, never the data channel
// refreshQueue, so a closed-channel send is impossible).
func (rca *CacheAside) triggerRefresh(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (string, error),
) {
	if rca.closing.Load() {
		return
	}
	// Local dedup: skip if this process is already refreshing this key.
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
// refreshDone case so a concurrent Close unblocks senders without a data race —
// reads on a closed channel are safe, sends on a closed channel are not, so we
// only ever close refreshDone (the signal) and never refreshQueue (the data).
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
	budget := rca.refreshBudget(ttl)
	refreshCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
	defer cancel()

	// Distributed dedup: SET NX on a separate refresh lock key. IsRedisNil
	// signals "another node is refreshing" (healthy contention); other errors
	// are real Redis problems and must be reported separately so operators can
	// distinguish a healthy dedup signal from a broken Redis. The lock value is a
	// unique token (not a fixed "1") so release can CAS-check ownership, and its
	// TTL is the compute budget so it covers a long refresh without expiring.
	refreshKey := rca.refreshKeyFor(key)
	token := rca.lockPool.Generate()
	err := rca.client.Do(refreshCtx, rca.client.B().Set().Key(refreshKey).Value(token).Nx().Px(budget).Build()).Error()
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
		// CAS release: delete only if we still hold our own token, so a refresh
		// that overran its TTL can't delete a successor's lock.
		if delErr := delKeyLua.Exec(cleanupCtx, rca.client, []string{refreshKey}, []string{token}).Error(); delErr != nil {
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

	// Write under a fresh cleanup context, not refreshCtx: a callback that
	// consumed most of its budget would otherwise hit an expired context here and
	// lose its successfully-computed value.
	writeCtx, writeCancel := rca.cleanupCtx(ctx)
	defer writeCancel()
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	if err := refreshAheadSetScript.Exec(writeCtx, rca.client, []string{key}, []string{wrapped, ttlMs, rca.lockPrefix}).Error(); err != nil {
		rca.logger.Error("refresh-ahead set failed", "key", key, "error", err)
		rca.emitRefreshError(key)
	}
}

// triggerMultiRefresh enqueues a multi-key refresh job to the worker pool.
// Two-level dedup: local syncx.Map + distributed SET NX on separate refresh keys.
// If the queue is full, the refresh is silently dropped (stale values are still served).
//
// Safe against concurrent Close: see triggerRefresh for the closing+refreshDone pattern.
func (rca *CacheAside) triggerMultiRefresh(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
) {
	if rca.closing.Load() {
		return
	}
	// Local dedup: filter to keys not already being refreshed.
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
	budget := rca.refreshBudget(ttl)
	refreshCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
	defer cancel()

	lockedTokens := rca.acquireRefreshLocks(refreshCtx, keys, budget)
	if len(lockedTokens) == 0 {
		return
	}
	defer rca.deleteRefreshLocks(ctx, lockedTokens)
	lockedKeys := mapsx.Keys(lockedTokens)

	start := time.Now()
	vals, err := fn(refreshCtx, lockedKeys)
	if err != nil {
		rca.logger.Error("refresh-ahead multi callback failed", "error", err)
		for _, key := range lockedKeys {
			rca.emitRefreshError(key)
		}
		return
	}

	// Write under a fresh cleanup context (see doSingleRefresh) so a slow-but-
	// successful callback still persists its values.
	writeCtx, writeCancel := rca.cleanupCtx(ctx)
	defer writeCancel()
	rca.setRefreshedValues(writeCtx, ttl, vals, perValueDelta(time.Since(start), len(vals)))
}

// acquireRefreshLocks batch-acquires distributed SET NX locks for refresh keys,
// returning a map of data key to the unique token held for that key (used by
// deleteRefreshLocks to CAS-release). Each lock's TTL is the compute budget so a
// long refresh does not lose its dedup lock mid-flight. Distinguishes IsRedisNil
// (healthy dedup → RefreshSkipped) from real Redis errors (→ RefreshError + log).
func (rca *CacheAside) acquireRefreshLocks(ctx context.Context, keys []string, budget time.Duration) map[string]string {
	cmdsP := commandsPool.Get(len(keys))
	defer commandsPool.Put(cmdsP)
	cmds := *cmdsP
	tokens := make([]string, len(keys))
	for i, key := range keys {
		tokens[i] = rca.lockPool.Generate()
		cmds[i] = rca.client.B().Set().Key(rca.refreshKeyFor(key)).Value(tokens[i]).Nx().Px(budget).Build()
	}
	resps := rca.client.DoMulti(ctx, cmds...)

	locked := make(map[string]string, len(keys))
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
		locked[keys[i]] = tokens[i]
	}
	rca.emitRefreshSkipped(skipped)
	return locked
}

// deleteRefreshLocks CAS-releases distributed refresh locks (best effort),
// deleting each key only if it still holds our token so an overran refresh can't
// delete a successor's lock. Lua scripts run per cluster slot (refresh keys are
// hash-tagged to their data key's slot), fanning out across slots in parallel —
// mirroring unlockMulti. Failures are logged; a stuck lock disables refresh for
// that key until its TTL expires.
func (rca *CacheAside) deleteRefreshLocks(ctx context.Context, tokens map[string]string) {
	if len(tokens) == 0 {
		return
	}
	cleanupCtx, cleanupCancel := rca.cleanupCtx(ctx)
	defer cleanupCancel()
	type keyedExec struct {
		key  string
		exec rueidis.LuaExec
	}
	bySlot := make(map[uint16][]keyedExec)
	for key, token := range tokens {
		refreshKey := rca.refreshKeyFor(key)
		slot := cmdx.Slot(refreshKey)
		bySlot[slot] = append(bySlot[slot], keyedExec{
			key:  key,
			exec: rueidis.LuaExec{Keys: []string{refreshKey}, Args: []string{token}},
		})
	}
	var wg sync.WaitGroup
	for _, stmts := range bySlot {
		wg.Add(1)
		go func() {
			defer wg.Done()
			execs := make([]rueidis.LuaExec, len(stmts))
			for i, s := range stmts {
				execs[i] = s.exec
			}
			resps := delKeyLua.ExecMulti(cleanupCtx, rca.client, execs...)
			for i, resp := range resps {
				if err := resp.Error(); err != nil {
					rca.logger.Error("refresh-ahead lock release failed", "key", stmts[i].key, "error", err)
				}
			}
		}()
	}
	wg.Wait()
}

// setRefreshedValues writes refreshed values via a CAS-style Lua script that
// skips keys currently holding a lock value (so a concurrent Get/Set is not
// stomped) or missing entirely (let Get-on-miss handle population). Values
// are envelope-wrapped with the supplied delta so future reads can apply
// XFetch sampling.
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
