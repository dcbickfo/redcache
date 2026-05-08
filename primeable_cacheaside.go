package redcache

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/mapsx"
	"github.com/dcbickfo/redcache/internal/syncx"
)

// PrimeableCacheAside extends CacheAside with explicit Set operations for
// cache priming and coordinated updates.
//
// Adds:
//   - Set/SetMulti for coordinated updates with write locking
//   - ForceSet/ForceSetMulti for unconditional writes bypassing locks
type PrimeableCacheAside struct {
	*CacheAside
}

// NewPrimeableCacheAside creates a PrimeableCacheAside that wraps a CacheAside
// with additional Set operations.
func NewPrimeableCacheAside(clientOption rueidis.ClientOption, caOption CacheAsideOption) (*PrimeableCacheAside, error) {
	rca, err := NewRedCacheAside(clientOption, caOption)
	if err != nil {
		return nil, err
	}
	return &PrimeableCacheAside{CacheAside: rca}, nil
}

// Close cancels all pending lock entries. It does NOT close the underlying Redis client.
func (pca *PrimeableCacheAside) Close() {
	pca.CacheAside.Close()
}

// Set acquires a write lock on key, calls fn to produce the value, and
// atomically sets it. If another operation holds a lock, Set waits.
//
// On callback error, the previous value is restored only if Set still holds
// the lock; a concurrent ForceSet's value is preserved otherwise. The
// post-callback CAS-set may return ErrLockLost under the same race.
func (pca *PrimeableCacheAside) Set(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (string, error),
) error {
	lockVal := pca.lockPool.Generate()

	for {
		saved, retry, err := pca.acquireSingleWriteLock(ctx, key, lockVal, pca.lockTTLMs)
		if err != nil {
			return err
		}
		if retry {
			continue
		}

		start := time.Now()
		newVal, err := fn(ctx, key)
		if err != nil {
			// bestEffortRestore so a cancelled request still rolls back the
			// lock instead of letting it linger until lockTTL expires.
			pca.bestEffortRestore(ctx, key, lockVal, saved)
			return err
		}
		wrapped := wrapEnvelope(newVal, time.Since(start))

		resp := setWithWriteLockScript.Exec(ctx, pca.client, []string{key}, []string{wrapped, strconv.FormatInt(ttl.Milliseconds(), 10), lockVal})
		if err := resp.Error(); err != nil {
			// CAS Lua errored mid-call; we may still hold the lock. Restore
			// the prior value (DEL if none) — bestEffortUnlock would wipe a
			// real prior value captured during acquire.
			pca.bestEffortRestore(ctx, key, lockVal, saved)
			return fmt.Errorf("set key %q: %w", key, err)
		}
		casResult, ierr := resp.AsInt64()
		if ierr != nil {
			pca.logger.Error("unexpected non-integer in CAS-set response", "key", key, "error", ierr)
			pca.bestEffortRestore(ctx, key, lockVal, saved)
			return fmt.Errorf("set key %q: parse response: %w", key, ierr)
		}
		if casResult == 0 {
			pca.emitLockLost(key)
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
func (pca *PrimeableCacheAside) acquireSingleWriteLock(
	ctx context.Context,
	key, lockVal, lockTTLMs string,
) (saved savedValue, retry bool, err error) {
	waitChan, _ := pca.register(key)

	resp := pca.client.DoCache(ctx, pca.client.B().Get().Key(key).Cache(), pca.lockTTL)
	val, rerr := resp.ToString()
	if rerr != nil && !rueidis.IsRedisNil(rerr) {
		return savedValue{}, false, fmt.Errorf("read key %q: %w", key, rerr)
	}

	if !rueidis.IsRedisNil(rerr) && strings.HasPrefix(val, pca.lockPrefix) {
		pca.emitLockContended(1)
		return savedValue{}, true, pca.awaitLock(ctx, waitChan)
	}

	acquired, saved, err := pca.tryAcquireWriteLock(ctx, key, lockVal, lockTTLMs)
	if err != nil {
		return savedValue{}, false, err
	}
	if !acquired {
		// Another lock appeared between DoCache and Exec.
		pca.emitLockContended(1)
		return savedValue{}, true, pca.awaitLock(ctx, waitChan)
	}
	return saved, false, nil
}

// SetMulti acquires write locks on all keys (in sorted order to prevent
// deadlocks), calls fn once with all keys, and atomically sets the returned
// values.
//
// The callback receives held keys in undefined order. Callers needing stable
// order should sort before use.
//
// On partial CAS failure, returns a *BatchError listing succeeded and failed keys.
func (pca *PrimeableCacheAside) SetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
) error {
	if len(keys) == 0 {
		return nil
	}

	if err := pca.waitForReadLocks(ctx, keys); err != nil {
		return err
	}

	lockValues, savedValues, err := pca.acquireMultiWriteLocks(ctx, keys)
	if err != nil {
		return err
	}

	start := time.Now()
	vals, err := fn(ctx, mapsx.Keys(lockValues))
	if err != nil {
		pca.restoreMultiValues(ctx, lockValues, savedValues)
		return err
	}
	delta := perValueDelta(time.Since(start), len(vals))
	wrappedVals := make(map[string]string, len(vals))
	for k, v := range vals {
		wrappedVals[k] = wrapEnvelope(v, delta)
	}

	succeeded, failed := pca.setMultiValuesWithCAS(ctx, ttl, wrappedVals, lockValues)

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
		pca.restoreMultiValues(ctx, toRestore, savedValues)
	}

	return NewBatchError(failed, succeeded)
}

// ForceSet unconditionally writes a value to Redis, bypassing all locks.
// Any in-progress Get or Set on this key will see ErrLockLost and retry.
//
// ttl must be > 0 (Redis rejects PX 0). Use Del to remove a key.
//
// Prefer Set when you need rollback semantics on callback failure.
//
// The value is envelope-wrapped with delta=0, so refresh-ahead falls back to
// the simple floor check.
func (pca *PrimeableCacheAside) ForceSet(ctx context.Context, ttl time.Duration, key, value string) error {
	return pca.client.Do(ctx, pca.client.B().Set().Key(key).Value(wrapEnvelope(value, 0)).Px(ttl).Build()).Error()
}

// ForceSetMulti unconditionally writes multiple values, bypassing all locks.
// Any in-progress Get or Set on these keys will see ErrLockLost and retry.
//
// ttl must be > 0 (Redis rejects PX 0).
//
// On partial failure, returns a *BatchError listing succeeded and failed keys.
func (pca *PrimeableCacheAside) ForceSetMulti(ctx context.Context, ttl time.Duration, values map[string]string) error {
	if len(values) == 0 {
		return nil
	}
	cmdsP := commandsPool.GetCap(len(values))
	defer commandsPool.Put(cmdsP)
	keyOrder := make([]string, 0, len(values))
	for key, val := range values {
		keyOrder = append(keyOrder, key)
		*cmdsP = append(*cmdsP, pca.client.B().Set().Key(key).Value(wrapEnvelope(val, 0)).Px(ttl).Build())
	}
	resps := pca.client.DoMulti(ctx, *cmdsP...)
	var failed map[string]error
	succeeded := make([]string, 0, len(resps))
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			pca.logger.Error("ForceSetMulti key failed", "key", keyOrder[i], "error", err)
			if failed == nil {
				failed = make(map[string]error)
			}
			failed[keyOrder[i]] = err
			continue
		}
		succeeded = append(succeeded, keyOrder[i])
	}
	return NewBatchError(failed, succeeded)
}

// waitForReadLocks registers all keys, batch-reads them, and waits on any
// holding a lock value. Register before DoCache so onInvalidate can find the
// lockEntries.
func (pca *PrimeableCacheAside) waitForReadLocks(ctx context.Context, keys []string) error {
	waitChansP := chanPool.Get(len(keys))
	defer chanPool.Put(waitChansP)
	waitChans := *waitChansP
	for i, key := range keys {
		waitChans[i], _ = pca.register(key)
	}

	multiP := cacheableTTLPool.Get(len(keys))
	defer cacheableTTLPool.Put(multiP)
	multi := *multiP
	for i, key := range keys {
		multi[i] = rueidis.CacheableTTL{
			Cmd: pca.client.B().Get().Key(key).Cache(),
			TTL: pca.lockTTL,
		}
	}
	resps := pca.client.DoMultiCache(ctx, multi...)

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
			pca.logger.Error("waitForReadLocks read failed", "key", keys[i], "error", err)
			if firstErr == nil {
				firstErr = err
				firstErrKey = keys[i]
			}
			continue
		}
		if strings.HasPrefix(val, pca.lockPrefix) {
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
