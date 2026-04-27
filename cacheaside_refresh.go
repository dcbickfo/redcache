package redcache

import (
	"context"
	"fmt"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/redis/rueidis"
)

// refreshJob is a unit of work for the refresh worker pool. It carries the
// keys it operates on so panic-recovery can attribute failures to specific keys.
type refreshJob struct {
	keys []string
	fn   func()
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
func (rca *CacheAside) runRefreshJob(job refreshJob) {
	defer func() {
		if r := recover(); r != nil {
			rca.logger.Error("refresh worker panic recovered", "keys", job.keys, "panic", fmt.Sprintf("%v", r), "stack", string(debug.Stack()))
			for _, k := range job.keys {
				rca.metrics.RefreshPanicked(k)
			}
		}
	}()
	job.fn()
}

// shouldRefresh reports whether the remaining client-side TTL has fallen below
// the configured refresh threshold. Returns false when refresh-ahead is
// disabled or the PTTL is unavailable (cachePTTL <= 0).
func (rca *CacheAside) shouldRefresh(cachePTTL int64, ttl time.Duration) bool {
	if rca.refreshAfter == 0 || cachePTTL <= 0 {
		return false
	}
	threshold := time.Duration(float64(ttl) * (1 - rca.refreshAfter))
	return time.Duration(cachePTTL)*time.Millisecond < threshold
}

// triggerRefresh enqueues a single-key refresh job to the worker pool.
// Two-level dedup: local syncx.Map + distributed SET NX on a separate refresh key.
// If the queue is full, the refresh is silently dropped (stale value is still served).
//
// Safe against concurrent Close: the closing flag short-circuits the common case,
// and a recover guards the unavoidable race where Close runs between the check
// and the send (Go has no atomic "send-if-not-closed" primitive).
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
		rca.metrics.RefreshSkipped(key)
		return
	}

	job := refreshJob{
		keys: []string{key},
		fn: func() {
			defer rca.refreshing.Delete(key)
			rca.doSingleRefresh(ctx, ttl, key, fn)
		},
	}

	rca.enqueueRefresh(job, []string{key})
}

// enqueueRefresh sends a job to the refresh queue. The select includes a
// refreshDone case so a concurrent Close unblocks senders without a data race —
// reads on a closed channel are safe, sends on a closed channel are not, so we
// only ever close refreshDone (the signal) and never refreshQueue (the data).
func (rca *CacheAside) enqueueRefresh(job refreshJob, keys []string) {
	select {
	case rca.refreshQueue <- job:
		for _, key := range keys {
			rca.metrics.RefreshTriggered(key)
		}
	case <-rca.refreshDone:
		for _, key := range keys {
			rca.refreshing.Delete(key)
			rca.metrics.RefreshDropped(key)
		}
	default:
		for _, key := range keys {
			rca.refreshing.Delete(key)
			rca.metrics.RefreshDropped(key)
		}
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

	// Distributed dedup: SET NX on a separate refresh lock key. IsRedisNil
	// signals "another node is refreshing" (healthy contention); other errors
	// are real Redis problems and must be reported separately so operators can
	// distinguish a healthy dedup signal from a broken Redis.
	refreshKey := rca.refreshKeyFor(key)
	err := rca.client.Do(refreshCtx, rca.client.B().Set().Key(refreshKey).Value("1").Nx().Px(rca.lockTTL).Build()).Error()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			rca.metrics.RefreshSkipped(key)
			return
		}
		rca.logger.Error("refresh-ahead lock acquisition failed", "key", key, "error", err)
		rca.metrics.RefreshError(key)
		return
	}
	defer func() {
		cleanupCtx, cleanupCancel := rca.cleanupCtx(ctx)
		defer cleanupCancel()
		if delErr := rca.client.Do(cleanupCtx, rca.client.B().Del().Key(refreshKey).Build()).Error(); delErr != nil {
			rca.logger.Error("refresh-ahead lock release failed", "key", key, "refreshKey", refreshKey, "error", delErr)
		}
	}()

	val, err := fn(refreshCtx, key)
	if err != nil {
		rca.logger.Error("refresh-ahead callback failed", "key", key, "error", err)
		rca.metrics.RefreshError(key)
		return
	}

	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	if err := refreshAheadSetScript.Exec(refreshCtx, rca.client, []string{key}, []string{val, ttlMs, rca.lockPrefix}).Error(); err != nil {
		rca.logger.Error("refresh-ahead set failed", "key", key, "error", err)
		rca.metrics.RefreshError(key)
	}
}

// triggerMultiRefresh enqueues a multi-key refresh job to the worker pool.
// Two-level dedup: local syncx.Map + distributed SET NX on separate refresh keys.
// If the queue is full, the refresh is silently dropped (stale values are still served).
//
// Safe against concurrent Close: see triggerRefresh for the closing+recover pattern.
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
	for _, key := range keys {
		if _, loaded := rca.refreshing.LoadOrStore(key, struct{}{}); !loaded {
			toRefresh = append(toRefresh, key)
		} else {
			rca.metrics.RefreshSkipped(key)
		}
	}
	if len(toRefresh) == 0 {
		return
	}

	job := refreshJob{
		keys: toRefresh,
		fn: func() {
			defer func() {
				for _, key := range toRefresh {
					rca.refreshing.Delete(key)
				}
			}()
			rca.doMultiRefresh(ctx, ttl, toRefresh, fn)
		},
	}

	rca.enqueueRefresh(job, toRefresh)
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

	vals, err := fn(refreshCtx, lockedKeys)
	if err != nil {
		rca.logger.Error("refresh-ahead multi callback failed", "error", err)
		for _, key := range lockedKeys {
			rca.metrics.RefreshError(key)
		}
		return
	}

	rca.setRefreshedValues(refreshCtx, ttl, vals)
}

// acquireRefreshLocks batch-acquires distributed SET NX locks for refresh keys.
// Distinguishes IsRedisNil (healthy dedup → RefreshSkipped) from real Redis
// errors (→ RefreshError + log).
func (rca *CacheAside) acquireRefreshLocks(ctx context.Context, keys []string) []string {
	cmds := make(rueidis.Commands, len(keys))
	for i, key := range keys {
		cmds[i] = rca.client.B().Set().Key(rca.refreshKeyFor(key)).Value("1").Nx().Px(rca.lockTTL).Build()
	}
	resps := rca.client.DoMulti(ctx, cmds...)

	var locked []string
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			if rueidis.IsRedisNil(err) {
				rca.metrics.RefreshSkipped(keys[i])
			} else {
				rca.logger.Error("refresh-ahead lock acquisition failed", "key", keys[i], "error", err)
				rca.metrics.RefreshError(keys[i])
			}
			continue
		}
		locked = append(locked, keys[i])
	}
	return locked
}

// deleteRefreshLocks removes distributed refresh lock keys (best effort).
// Failures are logged so operators can investigate; a stuck refresh lock
// disables refresh-ahead for that key for one lockTTL window.
func (rca *CacheAside) deleteRefreshLocks(ctx context.Context, keys []string) {
	cleanupCtx, cleanupCancel := rca.cleanupCtx(ctx)
	defer cleanupCancel()
	delCmds := make(rueidis.Commands, len(keys))
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
// skips keys currently holding a lock value (so a concurrent Get/Set is not
// stomped) or missing entirely (let Get-on-miss handle population).
func (rca *CacheAside) setRefreshedValues(ctx context.Context, ttl time.Duration, vals map[string]string) {
	if len(vals) == 0 {
		return
	}
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	stmts := make([]rueidis.LuaExec, 0, len(vals))
	keyOrder := make([]string, 0, len(vals))
	for key, val := range vals {
		keyOrder = append(keyOrder, key)
		stmts = append(stmts, rueidis.LuaExec{
			Keys: []string{key},
			Args: []string{val, ttlMs, rca.lockPrefix},
		})
	}
	resps := refreshAheadSetScript.ExecMulti(ctx, rca.client, stmts...)
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			rca.logger.Error("refresh-ahead multi set failed", "key", keyOrder[i], "error", err)
			rca.metrics.RefreshError(keyOrder[i])
		}
	}
}
