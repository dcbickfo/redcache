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
	fn func(ctx context.Context, keys []string) (map[string]string, error),
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

	var needRefreshP *[]string
	if rca.refreshAfter > 0 {
		// triggerMultiRefresh copies into its own slice, so we can reuse the buffer.
		needRefreshP = stringPool.GetCap(len(keys))
		defer stringPool.Put(needRefreshP)
	}

	// Leader keys are rebuilt each retry: a leader created the lockEntry and
	// drives Redis-side work; followers skip SET NX and wait on chans[i].
	leaderKeysP := stringPool.GetCap(len(keys))
	defer stringPool.Put(leaderKeysP)

retry:
	chans = chans[:len(pending)]
	leaderKeys := rca.registerPending(pending, chans, (*leaderKeysP)[:0])
	*leaderKeysP = leaderKeys

	if err := rca.readGetMultiHits(ctx, ttl, pending, res, needRefreshP, fn); err != nil {
		return nil, err
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
		var done bool
		var waitErr error
		pending, chans, done, waitErr = rca.waitForGetMulti(ctx, ttl, pending, chans, res, needRefreshP, fn)
		if waitErr != nil {
			return nil, waitErr
		}
		if done {
			return res, nil
		}
		goto retry
	}
	return res, nil
}

func (rca *cacheAside) registerPending(pending []string, chans []<-chan struct{}, leaderKeys []string) []string {
	for i, key := range pending {
		var isLeader bool
		chans[i], isLeader = rca.register(key)
		if isLeader {
			leaderKeys = append(leaderKeys, key)
		}
	}
	return leaderKeys
}

func (rca *cacheAside) readGetMultiHits(
	ctx context.Context,
	ttl time.Duration,
	pending []string,
	res map[string]string,
	needRefreshP *[]string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
) error {
	hitsBefore := len(res)
	needRefresh := resetNeedRefresh(needRefreshP)
	needRefresh, needRefreshExpected, err := rca.tryGetMulti(ctx, ttl, pending, res, needRefresh)
	if err != nil {
		return err
	}
	storeNeedRefresh(needRefreshP, needRefresh)
	rca.emitCacheHits(len(res) - hitsBefore)
	if len(needRefresh) > 0 {
		rca.triggerMultiRefresh(ctx, ttl, needRefresh, needRefreshExpected, fn)
	}
	return nil
}

func resetNeedRefresh(needRefreshP *[]string) []string {
	if needRefreshP == nil {
		return nil
	}
	*needRefreshP = (*needRefreshP)[:0]
	return *needRefreshP
}

func storeNeedRefresh(needRefreshP *[]string, needRefresh []string) {
	if needRefreshP != nil {
		*needRefreshP = needRefresh
	}
}

func (rca *cacheAside) waitForGetMulti(
	ctx context.Context,
	ttl time.Duration,
	pending []string,
	chans []<-chan struct{},
	res map[string]string,
	needRefreshP *[]string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
) ([]string, []<-chan struct{}, bool, error) {
	// Followers + leaders whose NX lost: wait for the holder's invalidation
	// (or the jittered poll fallback / lockTTL).
	rca.emitLockContended(len(pending))
	polled, err := rca.awaitGetMulti(ctx, ttl, pending, chans, res, needRefreshP, fn)
	if err != nil {
		return pending, chans, false, err
	}
	if !polled {
		return pending, chans, false, nil
	}
	pending, chans = filterResolved(pending, chans, res)
	return pending, chans, len(pending) == 0, nil
}

func (rca *cacheAside) awaitGetMulti(
	ctx context.Context,
	ttl time.Duration,
	pending []string,
	chans []<-chan struct{},
	res map[string]string,
	needRefreshP *[]string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
) (bool, error) {
	return rca.awaitLockMultiOrPoll(ctx, chans, func() (bool, error) {
		hitsBefore := len(res)
		needRefresh := resetNeedRefresh(needRefreshP)
		needRefresh, needRefreshExpected, err := rca.tryGetMulti(ctx, ttl, pending, res, needRefresh)
		if err != nil {
			return false, err
		}
		storeNeedRefresh(needRefreshP, needRefresh)
		hits := len(res) - hitsBefore
		rca.emitCacheHits(hits)
		if len(needRefresh) > 0 {
			rca.triggerMultiRefresh(ctx, ttl, needRefresh, needRefreshExpected, fn)
		}
		return hits > 0, nil
	})
}

// runLeaderSets filters out leaderKeys that tryGetMulti already populated
// (CSC invalidation can land mid-call), then SETs the rest.
func (rca *cacheAside) runLeaderSets(
	ctx context.Context,
	ttl time.Duration,
	leaderKeys []string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
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
func filterResolved(
	pending []string,
	chans []<-chan struct{},
	resolved map[string]string,
) ([]string, []<-chan struct{}) {
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
func (rca *cacheAside) tryGetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	res map[string]string,
	needRefresh []string,
) ([]string, map[string]string, error) {
	multiP := cacheableTTLPool.Get(len(keys))
	defer cacheableTTLPool.Put(multiP)
	multi := *multiP
	rca.fillCacheableTTLs(multi, ttl, keys)
	resps := rca.client.DoMultiCache(ctx, multi...)

	var needRefreshExpected map[string]string
	for i, resp := range resps {
		var err error
		needRefresh, needRefreshExpected, err = rca.collectGetMultiResponse(
			ttl,
			keys[i],
			resp,
			res,
			needRefresh,
			needRefreshExpected,
			len(keys),
		)
		if err != nil {
			return needRefresh, needRefreshExpected, err
		}
	}
	return needRefresh, needRefreshExpected, nil
}

func (rca *cacheAside) fillCacheableTTLs(
	dst []rueidis.CacheableTTL,
	ttl time.Duration,
	keys []string,
) {
	for i, key := range keys {
		dst[i] = rueidis.CacheableTTL{
			Cmd: rca.client.B().Get().Key(key).Cache(),
			TTL: ttl,
		}
	}
}

func (rca *cacheAside) collectGetMultiResponse(
	ttl time.Duration,
	key string,
	resp rueidis.RedisResult,
	res map[string]string,
	needRefresh []string,
	needRefreshExpected map[string]string,
	keyCount int,
) ([]string, map[string]string, error) {
	val, err := resp.ToString()
	if rueidis.IsRedisNil(err) {
		return needRefresh, needRefreshExpected, nil
	}
	if err != nil {
		rca.emitRedisError("read")
		return needRefresh, needRefreshExpected, fmt.Errorf("key %q: %w", key, err)
	}
	if strings.HasPrefix(val, rca.lockPrefix) {
		return needRefresh, needRefreshExpected, nil
	}

	plain, delta := unwrapEnvelope(val)
	res[key] = plain
	if rca.refreshAfter == 0 || !rca.shouldRefresh(resp.CachePTTL(), ttl, delta) {
		return needRefresh, needRefreshExpected, nil
	}
	if needRefreshExpected == nil {
		needRefreshExpected = make(map[string]string, keyCount)
	}
	needRefresh = append(needRefresh, key)
	needRefreshExpected[key] = val
	return needRefresh, needRefreshExpected, nil
}

// trySetMultiKeyFn locks each pending key, calls fn, writes the values, and
// records successes in res.
func (rca *cacheAside) trySetMultiKeyFn(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
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
		lockVal, ok := lockVals[k]
		if !ok {
			continue
		}
		vL[k] = valAndLock{val: wrapEnvelope(v, delta), lockVal: lockVal}
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
		rca.emitRedisError("set")
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
