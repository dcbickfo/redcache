package redcache

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/cmdx"
)

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

	fnKeys := slices.Collect(maps.Keys(lockVals))
	start := time.Now()
	vals, err := fn(ctx, fnKeys)
	rca.emitLoaderDuration(time.Since(start))
	if err != nil {
		rca.emitLoaderErrors(len(fnKeys))
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
		rca.emitRedisError("lock")
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
