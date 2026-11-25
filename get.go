package redcache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"
)

// get returns the cached value for key, calling fn on a miss. Only one
// goroutine across all processes runs fn for a given key; others wait on the
// resulting invalidation. Empty strings are valid hits.
func (rca *cacheAside) get(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (val string, err error),
) (string, error) {
	var owned <-chan struct{}
	failed := true
	// Wake followers only after the inner call has unwound and attempted its Redis cleanup.
	defer func() {
		if failed && owned != nil {
			rca.release(key, owned)
		}
	}()
	val, err := rca.getLoop(ctx, ttl, key, fn, &owned)
	failed = err != nil
	return val, err
}

func (rca *cacheAside) getLoop(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (val string, err error),
	owned *<-chan struct{},
) (string, error) {
	lockLosses := 0
retry:
	wait, leader := rca.register(key)
	if leader {
		*owned = wait
	}
	res, err := rca.tryGet(ctx, ttl, key)

	if err == nil {
		return rca.returnGetHit(ctx, ttl, key, fn, res, *owned)
	}
	if !errors.Is(err, errNotFound) {
		return "", err
	}

	rca.emitCacheMisses(1)

	if !leader {
		val, done, err := rca.waitForGet(ctx, ttl, key, wait, *owned, fn)
		if done {
			return val, err
		}
		goto retry
	}

	val, err := rca.trySetKeyFunc(ctx, ttl, key, fn)
	if err == nil {
		rca.releaseIfUntracked(key, wait, res.untracked)
		return val, nil
	}

	// A ForceSet or Del took the lock, or Redis expired it despite the
	// keep-alive. Retry to read the new value, but a bounded number of times:
	// a lock lost over and over must surface, not loop.
	if errors.Is(err, ErrLockLost) && lockLossExceeded(&lockLosses) {
		return "", err
	}
	if errors.Is(err, errLockFailed) || errors.Is(err, ErrLockLost) {
		// errLockFailed: another process holds the Redis lock — wait alongside
		// followers (cancelling our entry would wake them to race the same NX).
		// ErrLockLost: retry to read the winner's value.
		val, done, err := rca.waitForGet(ctx, ttl, key, wait, *owned, fn)
		if done {
			return val, err
		}
		goto retry
	}

	return "", err
}

func (rca *cacheAside) waitForGet(
	ctx context.Context,
	ttl time.Duration,
	key string,
	wait, owned <-chan struct{},
	fn func(ctx context.Context, key string) (string, error),
) (val string, done bool, err error) {
	rca.emitLockContended(1)
	res, ok, err := rca.awaitGet(ctx, ttl, key, wait)
	if err != nil {
		return "", true, err
	}
	if ok {
		val, err := rca.returnGetHit(ctx, ttl, key, fn, res, owned)
		return val, true, err
	}
	return "", false, nil
}

func (rca *cacheAside) returnGetHit(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (string, error),
	res cacheReadResult,
	owned <-chan struct{},
) (string, error) {
	rca.releaseIfUntracked(key, owned, res.untracked)
	rca.emitCacheHits(1)
	if rca.refreshAfter > 0 && rca.shouldRefresh(res.pttl, ttl, res.delta) {
		rca.triggerRefresh(ctx, ttl, key, res.raw, fn)
	}
	return res.val, nil
}

func (rca *cacheAside) awaitGet(ctx context.Context, ttl time.Duration, key string, wait <-chan struct{}) (cacheReadResult, bool, error) {
	var polled cacheReadResult
	hit := false
	_, err := rca.awaitLockOrPoll(ctx, wait, func() (bool, error) {
		res, err := rca.tryGet(ctx, ttl, key)
		if errors.Is(err, errNotFound) {
			if res.locked {
				return false, nil
			}
			// Wake followers and let one retry SET NX against the absent key.
			rca.release(key, wait)
			return true, nil
		}
		if err != nil {
			return false, err
		}
		polled, hit = res, true
		return true, nil
	})
	return polled, hit, err
}

// peek is a read-only client-side-cached lookup: no loader, no lock. It reuses
// tryGet (which subscribes via DoCache and treats missing-or-lock values as
// errNotFound). Returns (val,true,nil) on a hit, ("",false,nil) on a miss or
// lock value, and ("",false,err) on a real Redis or decode error.
func (rca *cacheAside) peek(ctx context.Context, ttl time.Duration, key string) (string, bool, error) {
	res, err := rca.tryGet(ctx, ttl, key)
	if errors.Is(err, errNotFound) {
		return "", false, nil
	}
	if err != nil {
		// tryGet already emitted RedisError("read") for the real-read failure.
		return "", false, err
	}
	return res.val, true, nil
}

var (
	errNotFound   = errors.New("not found")
	errLockFailed = errors.New("lock failed")
)

type cacheReadResult struct {
	raw       string
	val       string
	pttl      int64
	delta     time.Duration
	untracked bool
	locked    bool
}

func (rca *cacheAside) tryGet(ctx context.Context, ttl time.Duration, key string) (cacheReadResult, error) {
	resp, read := rca.readKey(ctx, ttl, key, rca.refreshAfter > 0)
	val, err := resp.ToString()
	val, err = rca.cachedReadValue(ctx, key, val, err)
	untracked := read.untracked(resp)
	if rueidis.IsRedisNil(err) {
		return cacheReadResult{untracked: untracked}, errNotFound
	}
	if err != nil {
		rca.emitRedisError("read")
		return cacheReadResult{}, fmt.Errorf("read key %q: %w", key, err)
	}
	if strings.HasPrefix(val, rca.lockPrefix) {
		return cacheReadResult{untracked: untracked, locked: true}, errNotFound
	}
	plain, delta := unwrapEnvelope(val)
	var pttl int64
	if rca.refreshAfter > 0 {
		pttl = read.ttl(resp)
	}
	return cacheReadResult{raw: val, val: plain, pttl: pttl, delta: delta, untracked: untracked}, nil
}

func (rca *cacheAside) readKey(ctx context.Context, ttl time.Duration, key string, refreshTTL bool) (rueidis.RedisResult, bypassRead) {
	if rca.writeMarks.bypass(key) {
		if rca.trackBypasses || refreshTTL {
			replies, reads := rca.bypassReads(ctx, []string{key}, refreshTTL)
			if len(replies) == 1 {
				return replies[0], reads[0]
			}
		}
		return rca.client.Do(ctx, rca.client.B().Get().Key(key).Build()), bypassRead{bypassed: true}
	}
	return rca.client.DoCache(ctx, rca.client.B().Get().Key(key).Cache(), ttl), bypassRead{}
}

// cachedReadValue retries an aborted cached read in cluster mode with one plain
// GET to follow ASK when Valkey 9 returns it as the EXEC reply during slot migration.
// Fallback hits skip refresh-ahead because a plain GET has no cache TTL.
func (rca *cacheAside) cachedReadValue(ctx context.Context, key, val string, err error) (string, error) {
	if !rca.shouldRetryCachedRead(err) {
		return val, err
	}
	rca.emitRedisError("read_fallback")
	val, retryErr := rca.client.Do(ctx, rca.client.B().Get().Key(key).Build()).ToString()
	if retryErr != nil && !rueidis.IsRedisNil(retryErr) {
		return "", errors.Join(err, retryErr)
	}
	return val, retryErr
}

func (rca *cacheAside) shouldRetryCachedRead(err error) bool {
	// Only cluster clients can follow ASK, and an abort can also signal an EXEC permission failure.
	return errors.Is(err, rueidis.ErrDoCacheAborted) && rca.client.Mode() == rueidis.ClientModeCluster
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
	stopKeepAlive := rca.holdLocks(ctx, map[string]string{key: lockVal})
	defer stopKeepAlive() // also on a loader panic; stop is idempotent
	val, err = fn(ctx, key)
	stopKeepAlive()
	rca.emitLoaderDuration(time.Since(start))
	if err == nil {
		wrapped := wrapEnvelope(val, time.Since(start))
		if _, err = rca.setWithLock(ctx, ttl, key, valAndLock{val: wrapped, lockVal: lockVal}); err == nil {
			setVal = true
		}
		return val, err
	}
	rca.emitLoaderErrors(1)
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
	rca.emitRedisError("lock")
	return "", fmt.Errorf("lock key %q: %w", key, err)
}

func (rca *cacheAside) setWithLock(ctx context.Context, ttl time.Duration, key string, valLock valAndLock) (string, error) {
	keys := []string{key}
	mark := rca.writeMarks.begin(keys, time.Now(), writeMarkCap)
	resp := setKeyLua.Exec(ctx, rca.client, keys, []string{valLock.lockVal, valLock.val, strconv.FormatInt(ttl.Milliseconds(), 10)})
	rca.writeMarks.finish(keys, mark, time.Now(), func(int) bool { return casWrote(resp) })
	if err := resp.Error(); err != nil {
		if !rueidis.IsRedisNil(err) {
			rca.emitRedisError("set")
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
