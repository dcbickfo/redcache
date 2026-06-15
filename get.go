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
		rca.emitRedisError("read")
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
	val, err = fn(ctx, key)
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
	resp := setKeyLua.Exec(ctx, rca.client, []string{key}, []string{valLock.lockVal, valLock.val, strconv.FormatInt(ttl.Milliseconds(), 10)})
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
