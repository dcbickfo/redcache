package redcache

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"
)

// getMulti returns cached values for keys, calling fn for misses.
func (rca *cacheAside) getMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
) (map[string]string, error) {
	if len(keys) == 0 {
		return map[string]string{}, nil
	}
	var owned entryOwner
	failed := true
	defer func() {
		if failed {
			rca.releaseFinished(owned, true)
		}
	}()
	res, err := rca.getMultiLoop(ctx, ttl, keys, fn, &owned)
	failed = err != nil
	return res, err
}

func (rca *cacheAside) getMultiLoop(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
	owned *entryOwner,
) (map[string]string, error) {
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

	lockLossRounds := 0
retry:
	chans = chans[:len(pending)]
	leaderKeys := rca.registerPending(pending, chans, (*leaderKeysP)[:0], owned)
	*leaderKeysP = leaderKeys

	if err := rca.readGetMultiHits(ctx, ttl, pending, res, needRefreshP, *owned, fn); err != nil {
		return nil, err
	}

	pending, chans = filterResolved(pending, chans, res)

	if len(pending) > 0 {
		rca.emitCacheMisses(len(pending))
		lost, err := rca.runLeaderSets(ctx, ttl, leaderKeys, fn, res, *owned)
		if err != nil {
			return nil, err
		}
		// As in get: retry a lost lock a bounded number of times.
		if lost > 0 && lockLossExceeded(&lockLossRounds) {
			return nil, fmt.Errorf("lock lost for %d key(s): %w", lost, ErrLockLost)
		}
		pending, chans = filterResolved(pending, chans, res)
	}

	if len(pending) > 0 {
		var done bool
		var waitErr error
		pending, chans, done, waitErr = rca.waitForGetMulti(ctx, ttl, pending, chans, res, needRefreshP, *owned, fn)
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

// registerPending registers each pending key, appending the keys this call
// leads to leaderKeys and recording their entries in owned.
func (rca *cacheAside) registerPending(pending []string, chans []<-chan struct{}, leaderKeys []string, owned *entryOwner) []string {
	for i, key := range pending {
		var isLeader bool
		chans[i], isLeader = rca.registerOwned(owned, key)
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
	owned entryOwner,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
) error {
	hitsBefore := len(res)
	needRefresh := resetNeedRefresh(needRefreshP)
	needRefresh, needRefreshExpected, err := rca.tryGetMulti(ctx, ttl, pending, res, needRefresh, owned)
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
	owned entryOwner,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
) ([]string, []<-chan struct{}, bool, error) {
	// Followers + leaders whose NX lost: wait for the holder's invalidation
	// (or the jittered poll fallback / lockTTL).
	rca.emitLockContended(len(pending))
	polled, err := rca.awaitGetMulti(ctx, ttl, pending, chans, res, needRefreshP, owned, fn)
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
	owned entryOwner,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
) (bool, error) {
	return rca.awaitLockMultiOrPoll(ctx, chans, func() (bool, error) {
		hitsBefore := len(res)
		if err := rca.readGetMultiHits(ctx, ttl, pending, res, needRefreshP, owned, fn); err != nil {
			return false, err
		}
		return len(res) > hitsBefore, nil
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
	owned entryOwner,
) (lost int, err error) {
	n := 0
	for _, k := range leaderKeys {
		if _, ok := res[k]; !ok {
			leaderKeys[n] = k
			n++
		}
	}
	leaderKeys = leaderKeys[:n]
	if len(leaderKeys) == 0 {
		return 0, nil
	}
	return rca.trySetMultiKeyFn(ctx, ttl, leaderKeys, fn, res, owned)
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
	owned entryOwner,
) ([]string, map[string]string, error) {
	resps, err := rca.readMultiCache(ctx, ttl, keys)
	if err != nil {
		return needRefresh, nil, err
	}

	var needRefreshExpected map[string]string
	for i, key := range keys {
		needRefresh, needRefreshExpected, err = rca.collectGetMultiResponse(
			ttl,
			key,
			resps[i],
			res,
			needRefresh,
			needRefreshExpected,
			len(keys),
			owned,
		)
		if err != nil {
			return needRefresh, needRefreshExpected, err
		}
	}
	return needRefresh, needRefreshExpected, nil
}

func (rca *cacheAside) readMultiCache(ctx context.Context, ttl time.Duration, keys []string) ([]rueidis.RedisResult, error) {
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
	if len(resps) != len(keys) {
		return nil, fmt.Errorf("cached read: response/key length mismatch: %d resps vs %d keys", len(resps), len(keys))
	}

	var aborted []int
	// A terminal error in the cached batch precludes retrying any key.
	for i, resp := range resps {
		_, err := resp.ToString()
		if rca.shouldRetryCachedRead(err) {
			aborted = append(aborted, i)
		} else if err != nil && !rueidis.IsRedisNil(err) {
			rca.emitRedisError("read")
			return nil, fmt.Errorf("read key %q: %w", keys[i], err)
		}
	}
	if len(aborted) != 0 {
		return resps, rca.retryAbortedReads(ctx, keys, aborted, resps)
	}
	return resps, nil
}

func (rca *cacheAside) retryAbortedReads(ctx context.Context, keys []string, aborted []int, resps []rueidis.RedisResult) error {
	cmds := make(rueidis.Commands, len(aborted))
	for i, index := range aborted {
		cmds[i] = rca.client.B().Get().Key(keys[index]).Build()
	}
	rca.emitRedisError("read_fallback")
	plain := rca.client.DoMulti(ctx, cmds...)
	if len(plain) != len(aborted) {
		return fmt.Errorf("plain read: response/key length mismatch: %d resps vs %d keys", len(plain), len(aborted))
	}
	for i, index := range aborted {
		if _, err := plain[i].ToString(); err != nil && !rueidis.IsRedisNil(err) {
			rca.emitRedisError("read")
			return fmt.Errorf("read key %q: %w", keys[index], errors.Join(resps[index].Error(), err))
		}
		resps[index] = plain[i]
	}
	return nil
}

func (rca *cacheAside) collectGetMultiResponse(
	ttl time.Duration,
	key string,
	resp rueidis.RedisResult,
	res map[string]string,
	needRefresh []string,
	needRefreshExpected map[string]string,
	keyCount int,
	owned entryOwner,
) ([]string, map[string]string, error) {
	val, err := resp.ToString()
	owned.rememberRead(key, resp)
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

	rca.releaseUntracked(owned, key)
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

// unlockUnset releases the locks in lockVals whose keys never landed in res.
func (rca *cacheAside) unlockUnset(ctx context.Context, lockVals, res map[string]string) {
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
}

// trySetMultiKeyFn locks each pending key, calls fn, writes the values, and
// records successes in res.
func (rca *cacheAside) trySetMultiKeyFn(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
	res map[string]string,
	owned entryOwner,
) (lost int, err error) {
	lockVals, err := rca.tryLockMulti(ctx, keys)
	if err != nil {
		return 0, err
	}

	defer rca.unlockUnset(ctx, lockVals, res)

	if len(lockVals) == 0 {
		return 0, nil
	}

	fnKeys := slices.Collect(maps.Keys(lockVals))
	start := time.Now()
	stopKeepAlive := rca.holdLocks(ctx, lockVals)
	defer stopKeepAlive() // also on a loader panic; stop is idempotent
	vals, err := fn(ctx, fnKeys)
	stopKeepAlive()
	rca.emitLoaderDuration(time.Since(start))
	if err != nil {
		rca.emitLoaderErrors(len(fnKeys))
		return 0, err
	}
	// A key the loader omits would never land in res, so the retry loop would
	// re-lock it and re-run fn until ctx ends. Fail fast; the deferred unlock
	// releases every lock.
	if missing := missingKeys(lockVals, vals); len(missing) > 0 {
		return 0, fmt.Errorf("%w: %q", ErrMissingValue, missing)
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
		return 0, err
	}

	for _, keySet := range keysSet {
		res[keySet] = vals[keySet]
		rca.releaseUntracked(owned, keySet)
	}

	return len(vL) - len(keysSet), nil
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
	cmds := make(rueidis.Commands, 0, len(keys))
	for _, k := range keys {
		lockVals[k] = rca.lockPool.Generate()
		cmds = append(cmds, rca.client.B().Set().Key(k).Value(lockVals[k]).Nx().Get().Px(rca.lockTTL).Build())
	}
	resps := rca.client.DoMulti(ctx, cmds...)
	// Drain every response: DoMulti has already executed all SET NXs in the
	// pipeline, so an early return would leak the later acquires for lockTTL.
	// IsRedisNil = acquired; nil = NX rejected; other = propagate.
	var first firstKeyErr
	for i, r := range resps {
		err := r.Error()
		if rueidis.IsRedisNil(err) {
			continue
		}
		delete(lockVals, keys[i])
		if err == nil {
			continue
		}
		rca.emitRedisError("lock")
		if !first.add(keys[i], err) {
			rca.logger.Error("additional tryLockMulti error", "key", keys[i], "error", err)
		}
	}
	if err := first.wrap("lock"); err != nil {
		if len(lockVals) > 0 {
			cleanupCtx, cancel := rca.cleanupCtx(ctx)
			rca.unlockMulti(cleanupCtx, lockVals)
			cancel()
		}
		return nil, err
	}
	return lockVals, nil
}

type valAndLock struct {
	val     string
	lockVal string
}

// inspectSetResponse classifies one CAS-set response: success, silent
// lock-lost, or surfaceable error. Parse errors are surfaced so script drift
// can't trigger an infinite retry loop.
func (rca *cacheAside) inspectSetResponse(key string, resp rueidis.RedisResult) (bool, error) {
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

// setMultiWithLock CAS-replaces each held lock with its value in one pipeline;
// the client routes each single-key script to its slot's node. Every response
// is inspected so successes survive a sibling error, and on error the keys
// already written are logged for operator reconciliation. Lua=0 (or nil) is a
// lock-lost.
func (rca *cacheAside) setMultiWithLock(ctx context.Context, ttl time.Duration, keyValLock map[string]valAndLock) ([]string, error) {
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	keys, execs := mapExecs(keyValLock, func(k string, vl valAndLock) rueidis.LuaExec {
		return rueidis.LuaExec{Keys: []string{k}, Args: []string{vl.lockVal, vl.val, ttlMs}}
	})
	var succeeded []string
	var firstErr error
	for i, resp := range setKeyLua.ExecMulti(ctx, rca.client, execs...) {
		ok, err := rca.inspectSetResponse(keys[i], resp)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if ok {
			succeeded = append(succeeded, keys[i])
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

func (rca *cacheAside) unlockMulti(ctx context.Context, lockVals map[string]string) {
	if len(lockVals) == 0 {
		return
	}
	keys, execs := mapExecs(lockVals, func(key, lockVal string) rueidis.LuaExec {
		return rueidis.LuaExec{Keys: []string{key}, Args: []string{lockVal}}
	})
	// Best effort — locks expire on lockTTL anyway.
	for i, resp := range delKeyLua.ExecMulti(ctx, rca.client, execs...) {
		if err := resp.Error(); err != nil {
			rca.logger.Error("failed to unlock key in batch", "key", keys[i], "error", err)
		}
	}
}
