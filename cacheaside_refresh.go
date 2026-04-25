package redcache

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/rueidis"
)

// startRefreshWorkers launches n background workers that drain refreshQueue.
// Each worker exits when the queue is closed (during Close).
func (rca *CacheAside) startRefreshWorkers(n int) {
	for range n {
		rca.refreshWg.Add(1)
		go func() {
			defer rca.refreshWg.Done()
			for job := range rca.refreshQueue {
				rca.runRefreshJob(job)
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
func (rca *CacheAside) runRefreshJob(job func()) {
	defer func() {
		if r := recover(); r != nil {
			rca.logger.Error("refresh worker panic recovered", "panic", fmt.Sprintf("%v", r))
			rca.metrics.RefreshPanicked(r)
		}
	}()
	job()
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
func (rca *CacheAside) triggerRefresh(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (string, error),
) {
	// Local dedup: skip if this process is already refreshing this key.
	if _, loaded := rca.refreshing.LoadOrStore(key, struct{}{}); loaded {
		rca.metrics.RefreshSkipped(key)
		return
	}

	job := func() {
		defer rca.refreshing.Delete(key)
		rca.doSingleRefresh(ctx, ttl, key, fn)
	}

	select {
	case rca.refreshQueue <- job:
		rca.metrics.RefreshTriggered(key)
	default:
		// Queue full — drop refresh, stale value is fine.
		rca.refreshing.Delete(key)
		rca.metrics.RefreshDropped(key)
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

	// Distributed dedup: SET NX on a separate refresh lock key.
	refreshKey := rca.refreshKeyFor(key)
	err := rca.client.Do(refreshCtx, rca.client.B().Set().Key(refreshKey).Value("1").Nx().Px(rca.lockTTL).Build()).Error()
	if err != nil {
		// NX failed or Redis error — another process is refreshing.
		rca.metrics.RefreshSkipped(key)
		return
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), rca.lockTTL)
		defer cleanupCancel()
		rca.client.Do(cleanupCtx, rca.client.B().Del().Key(refreshKey).Build())
	}()

	val, err := fn(refreshCtx, key)
	if err != nil {
		rca.logger.Error("refresh-ahead callback failed", "key", key, "error", err)
		return
	}

	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	if err := refreshAheadSetScript.Exec(refreshCtx, rca.client, []string{key}, []string{val, ttlMs, rca.lockPrefix}).Error(); err != nil {
		rca.logger.Error("refresh-ahead set failed", "key", key, "error", err)
	}
}

// triggerMultiRefresh enqueues a multi-key refresh job to the worker pool.
// Two-level dedup: local syncx.Map + distributed SET NX on separate refresh keys.
// If the queue is full, the refresh is silently dropped (stale values are still served).
func (rca *CacheAside) triggerMultiRefresh(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
) {
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

	job := func() {
		defer func() {
			for _, key := range toRefresh {
				rca.refreshing.Delete(key)
			}
		}()
		rca.doMultiRefresh(ctx, ttl, toRefresh, fn)
	}

	select {
	case rca.refreshQueue <- job:
		for _, key := range toRefresh {
			rca.metrics.RefreshTriggered(key)
		}
	default:
		// Queue full — drop refresh, stale values are fine.
		for _, key := range toRefresh {
			rca.refreshing.Delete(key)
			rca.metrics.RefreshDropped(key)
		}
	}
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
		return
	}

	rca.setRefreshedValues(refreshCtx, ttl, vals)
}

// acquireRefreshLocks batch-acquires distributed SET NX locks for refresh keys.
func (rca *CacheAside) acquireRefreshLocks(ctx context.Context, keys []string) []string {
	cmds := make(rueidis.Commands, len(keys))
	for i, key := range keys {
		cmds[i] = rca.client.B().Set().Key(rca.refreshKeyFor(key)).Value("1").Nx().Px(rca.lockTTL).Build()
	}
	resps := rca.client.DoMulti(ctx, cmds...)

	var locked []string
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			continue
		}
		locked = append(locked, keys[i])
	}
	return locked
}

// deleteRefreshLocks removes distributed refresh lock keys (best effort).
func (rca *CacheAside) deleteRefreshLocks(ctx context.Context, keys []string) {
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), rca.lockTTL)
	defer cleanupCancel()
	delCmds := make(rueidis.Commands, len(keys))
	for i, key := range keys {
		delCmds[i] = rca.client.B().Del().Key(rca.refreshKeyFor(key)).Build()
	}
	rca.client.DoMulti(cleanupCtx, delCmds...)
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
	for key, val := range vals {
		stmts = append(stmts, rueidis.LuaExec{
			Keys: []string{key},
			Args: []string{val, ttlMs, rca.lockPrefix},
		})
	}
	resps := refreshAheadSetScript.ExecMulti(ctx, rca.client, stmts...)
	for _, resp := range resps {
		if err := resp.Error(); err != nil {
			rca.logger.Error("refresh-ahead multi set failed", "error", err)
		}
	}
}
