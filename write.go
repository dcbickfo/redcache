package redcache

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/syncx"
)

// set acquires a write lock on key, calls fn, and atomically writes the
// returned value. Waits when another operation holds the lock. On callback
// error the prior value is restored only if set still holds the lock; a
// concurrent forceSet's value is preserved. The post-callback CAS may return
// ErrLockLost under the same race.
func (rca *cacheAside) set(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (string, error),
) error {
	lockVal := rca.lockPool.Generate()

	for {
		saved, retry, err := rca.acquireSingleWriteLock(ctx, key, lockVal, rca.lockTTLMs)
		if err != nil {
			return err
		}
		if retry {
			continue
		}

		start := time.Now()
		newVal, err := fn(ctx, key)
		rca.emitLoaderDuration(time.Since(start))
		if err != nil {
			rca.emitLoaderErrors(1)
			// bestEffortRestore so a cancelled request still rolls back the
			// lock instead of letting it linger until lockTTL expires.
			rca.bestEffortRestore(ctx, key, lockVal, saved)
			return err
		}
		wrapped := wrapEnvelope(newVal, time.Since(start))

		resp := setWithWriteLockScript.Exec(ctx, rca.client, []string{key}, []string{wrapped, strconv.FormatInt(ttl.Milliseconds(), 10), lockVal})
		if err := resp.Error(); err != nil {
			// CAS Lua errored mid-call; we may still hold the lock. Restore
			// the prior value (DEL if none) — bestEffortUnlock would wipe a
			// real prior value captured during acquire.
			rca.emitRedisError("set")
			rca.bestEffortRestore(ctx, key, lockVal, saved)
			return fmt.Errorf("set key %q: %w", key, err)
		}
		casResult, ierr := resp.AsInt64()
		if ierr != nil {
			rca.logger.Error("unexpected non-integer in CAS-set response", "key", key, "error", ierr)
			rca.bestEffortRestore(ctx, key, lockVal, saved)
			return fmt.Errorf("set key %q: parse response: %w", key, ierr)
		}
		if casResult == 0 {
			rca.emitLockLost(key)
			return fmt.Errorf("key %q: %w", key, ErrLockLost)
		}
		return nil
	}
}

// acquireSingleWriteLock subscribes to the key, waits out any existing lock
// holder, and tries to acquire a write lock. Returns:
//   - saved: previous real value (for callback-error rollback), if any
//   - retry: true when the caller should loop
//   - err: terminal error (context cancellation or Redis failure)
func (rca *cacheAside) acquireSingleWriteLock(
	ctx context.Context,
	key, lockVal, lockTTLMs string,
) (saved savedValue, retry bool, err error) {
	waitChan, _ := rca.register(key)

	resp := rca.client.DoCache(ctx, rca.client.B().Get().Key(key).Cache(), rca.lockTTL)
	val, rerr := resp.ToString()
	if rerr != nil && !rueidis.IsRedisNil(rerr) {
		rca.emitRedisError("read")
		return savedValue{}, false, fmt.Errorf("read key %q: %w", key, rerr)
	}

	if !rueidis.IsRedisNil(rerr) && strings.HasPrefix(val, rca.lockPrefix) {
		rca.emitLockContended(1)
		return savedValue{}, true, rca.awaitLock(ctx, waitChan)
	}

	acquired, saved, err := rca.tryAcquireWriteLock(ctx, key, lockVal, lockTTLMs)
	if err != nil {
		return savedValue{}, false, err
	}
	if !acquired {
		// Another lock appeared between DoCache and Exec.
		rca.emitLockContended(1)
		return savedValue{}, true, rca.awaitLock(ctx, waitChan)
	}
	return saved, false, nil
}

// setMulti acquires write locks for all keys (in sorted order to avoid
// deadlocks), calls fn once with the held keys (order is undefined; sort if
// you need stability), and atomically writes the returned values. Returns a
// *batchError on partial CAS failure.
func (rca *cacheAside) setMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
) error {
	if len(keys) == 0 {
		return nil
	}

	if err := rca.waitForReadLocks(ctx, keys); err != nil {
		return err
	}

	lockValues, savedValues, err := rca.acquireMultiWriteLocks(ctx, keys)
	if err != nil {
		return err
	}

	fnKeys := slices.Collect(maps.Keys(lockValues))
	start := time.Now()
	vals, err := fn(ctx, fnKeys)
	rca.emitLoaderDuration(time.Since(start))
	if err != nil {
		rca.emitLoaderErrors(len(fnKeys))
		rca.restoreMultiValues(ctx, lockValues, savedValues)
		return err
	}
	delta := perValueDelta(time.Since(start), len(vals))
	wrappedVals := make(map[string]string, len(vals))
	for k, v := range vals {
		wrappedVals[k] = wrapEnvelope(v, delta)
	}

	succeeded, failed := rca.setMultiValuesWithCAS(ctx, ttl, wrappedVals, lockValues)

	if len(succeeded) == len(lockValues) {
		return nil
	}

	// Restore (rather than unlock) keys not successfully written so a CAS
	// transport/parse error preserves the prior real value captured during
	// acquire. For lock-lost keys the restore Lua's CAS-check fails harmlessly.
	succeededSet := make(map[string]struct{}, len(succeeded))
	for _, s := range succeeded {
		succeededSet[s] = struct{}{}
	}
	toRestore := make(map[string]string)
	for key, lockVal := range lockValues {
		if _, ok := succeededSet[key]; !ok {
			toRestore[key] = lockVal
		}
	}
	if len(toRestore) > 0 {
		rca.restoreMultiValues(ctx, toRestore, savedValues)
	}

	return newBatchError(failed, succeeded)
}

// forceSet unconditionally writes value, bypassing locks. In-progress Get
// callers on the same key retry transparently and observe the force-set value;
// in-progress Set callers receive ErrLockLost and their pending set is
// abandoned (not retried). ttl must be > 0 (Redis rejects PX 0); use Del to
// remove. The value is envelope-wrapped with delta=0, so refresh-ahead falls
// back to the simple floor check. Prefer set when you need callback-error
// rollback.
func (rca *cacheAside) forceSet(ctx context.Context, ttl time.Duration, key, value string) error {
	if err := rca.client.Do(ctx, rca.client.B().Set().Key(key).Value(wrapEnvelope(value, 0)).Px(ttl).Build()).Error(); err != nil {
		rca.emitRedisError("set")
		return fmt.Errorf("force set key %q: %w", key, err)
	}
	return nil
}

// forceSetMulti unconditionally writes values, bypassing locks. In-progress
// Get callers on the same keys retry transparently and observe the force-set
// values; in-progress Set callers receive ErrLockLost and their pending sets
// are abandoned (not retried). ttl must be > 0. Returns a *batchError on
// partial failure.
func (rca *cacheAside) forceSetMulti(ctx context.Context, ttl time.Duration, values map[string]string) error {
	if len(values) == 0 {
		return nil
	}
	cmdsP := commandsPool.GetCap(len(values))
	defer commandsPool.Put(cmdsP)
	keyOrder := make([]string, 0, len(values))
	for key, val := range values {
		keyOrder = append(keyOrder, key)
		*cmdsP = append(*cmdsP, rca.client.B().Set().Key(key).Value(wrapEnvelope(val, 0)).Px(ttl).Build())
	}
	resps := rca.client.DoMulti(ctx, *cmdsP...)
	var failed map[string]error
	succeeded := make([]string, 0, len(resps))
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			rca.logger.Error("ForceSetMulti key failed", "key", keyOrder[i], "error", err)
			rca.emitRedisError("set")
			if failed == nil {
				failed = make(map[string]error)
			}
			failed[keyOrder[i]] = err
			continue
		}
		succeeded = append(succeeded, keyOrder[i])
	}
	return newBatchError(failed, succeeded)
}

// waitForReadLocks batch-reads keys and waits out any that currently hold a
// lock value. Registration must precede DoCache so onInvalidate can find the
// lockEntries.
func (rca *cacheAside) waitForReadLocks(ctx context.Context, keys []string) error {
	waitChansP := chanPool.Get(len(keys))
	defer chanPool.Put(waitChansP)
	waitChans := *waitChansP
	for i, key := range keys {
		waitChans[i], _ = rca.register(key)
	}

	multiP := cacheableTTLPool.Get(len(keys))
	defer cacheableTTLPool.Put(multiP)
	multi := *multiP
	for i, key := range keys {
		multi[i] = rueidis.CacheableTTL{
			Cmd: rca.client.B().Get().Key(key).Cache(),
			TTL: rca.lockTTL,
		}
	}
	resps := rca.client.DoMultiCache(ctx, multi...)

	// Distinguish redis-nil (no lock) from real Redis errors so the latter
	// surface to the caller instead of silently advancing against a broken cluster.
	if len(resps) != len(keys) {
		return fmt.Errorf("waitForReadLocks: response/key length mismatch: %d resps vs %d keys", len(resps), len(keys))
	}
	lockedChansP := chanPool.GetCap(len(keys))
	defer chanPool.Put(lockedChansP)
	var firstErr error
	var firstErrKey string
	for i := range keys {
		val, err := resps[i].ToString()
		if rueidis.IsRedisNil(err) {
			continue
		}
		if err != nil {
			rca.logger.Error("waitForReadLocks read failed", "key", keys[i], "error", err)
			rca.emitRedisError("read")
			if firstErr == nil {
				firstErr = err
				firstErrKey = keys[i]
			}
			continue
		}
		if strings.HasPrefix(val, rca.lockPrefix) {
			*lockedChansP = append(*lockedChansP, waitChans[i])
		}
	}
	if firstErr != nil {
		return fmt.Errorf("read key %q: %w", firstErrKey, firstErr)
	}

	if len(*lockedChansP) == 0 {
		return nil
	}
	return syncx.WaitForAll(ctx, *lockedChansP)
}
