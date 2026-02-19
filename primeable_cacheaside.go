package redcache

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/lockpool"
	"github.com/dcbickfo/redcache/internal/mapsx"
	"github.com/dcbickfo/redcache/internal/syncx"
)

// PrimeableCacheAside extends CacheAside with explicit Set operations for cache
// priming and coordinated cache updates.
//
// It inherits all Get/GetMulti/Del/DelMulti capabilities and adds:
//   - Set/SetMulti for coordinated cache updates with write locking
//   - ForceSet/ForceSetMulti for unconditional writes bypassing locks
type PrimeableCacheAside struct {
	*CacheAside
	lockPool *lockpool.Pool
}

// NewPrimeableCacheAside creates a PrimeableCacheAside that wraps a CacheAside
// with additional Set operations.
func NewPrimeableCacheAside(clientOption rueidis.ClientOption, caOption CacheAsideOption) (*PrimeableCacheAside, error) {
	rca, err := NewRedCacheAside(clientOption, caOption)
	if err != nil {
		return nil, err
	}
	lp, err := lockpool.New(rca.lockPrefix)
	if err != nil {
		return nil, fmt.Errorf("lock pool: %w", err)
	}
	return &PrimeableCacheAside{
		CacheAside: rca,
		lockPool:   lp,
	}, nil
}

// Close cancels all pending lock entries. It does NOT close the underlying Redis client.
func (pca *PrimeableCacheAside) Close() {
	pca.CacheAside.Close()
}

// Set acquires a write lock on the key, calls fn to produce the value, and atomically
// sets it in Redis. If another operation holds a lock, Set waits for it to complete.
//
// The callback fn receives the key and should return the value to cache.
// Set respects context cancellation for timeouts.
func (pca *PrimeableCacheAside) Set(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (string, error),
) error {
	lockVal := pca.lockPool.Generate()
	lockTTLMs := strconv.FormatInt(pca.lockTTL.Milliseconds(), 10)

retry:
	waitChan := pca.register(key)

	// Subscribe + read current value.
	resp := pca.client.DoCache(ctx, pca.client.B().Get().Key(key).Cache(), pca.lockTTL)
	val, err := resp.ToString()
	if err != nil && !rueidis.IsRedisNil(err) {
		return fmt.Errorf("read key %q: %w", key, err)
	}

	// If current value is a lock, wait for it to be released.
	if !rueidis.IsRedisNil(err) && strings.HasPrefix(val, pca.lockPrefix) {
		select {
		case <-waitChan:
			goto retry
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Try to acquire write lock.
	result, err := acquireWriteLockScript.Exec(ctx, pca.client, []string{key}, []string{lockVal, lockTTLMs, pca.lockPrefix}).AsInt64()
	if err != nil {
		return fmt.Errorf("write lock for key %q: %w", key, err)
	}
	if result == 0 {
		// Another lock appeared between DoCache and Exec.
		select {
		case <-waitChan:
			goto retry
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Lock acquired — execute callback.
	newVal, err := fn(ctx, key)
	if err != nil {
		pca.bestEffortUnlock(ctx, key, lockVal)
		return err
	}

	// CAS set the value.
	casResult, err := setWithWriteLockScript.Exec(ctx, pca.client, []string{key}, []string{newVal, strconv.FormatInt(ttl.Milliseconds(), 10), lockVal}).AsInt64()
	if err != nil {
		return fmt.Errorf("set key %q: %w", key, err)
	}
	if casResult == 0 {
		return fmt.Errorf("key %q: %w", key, ErrLockLost)
	}
	return nil
}

// SetMulti acquires write locks on all keys, calls fn once with all keys,
// and atomically sets the returned values. Locks are acquired in sorted order
// to prevent deadlocks.
//
// On partial CAS failure, returns a *BatchError listing succeeded and failed keys.
// On full success, returns nil.
func (pca *PrimeableCacheAside) SetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, keys []string) (map[string]string, error),
) error {
	if len(keys) == 0 {
		return nil
	}

	// Wait for any existing read locks on these keys.
	if err := pca.waitForReadLocks(ctx, keys); err != nil {
		return err
	}

	// Acquire write locks in sorted order.
	lockValues, savedValues, err := pca.acquireMultiWriteLocks(ctx, keys)
	if err != nil {
		return err
	}

	// Execute the callback with all locked keys.
	vals, err := fn(ctx, mapsx.Keys(lockValues))
	if err != nil {
		pca.restoreMultiValues(ctx, lockValues, savedValues)
		return err
	}

	// CAS batch set.
	succeeded, failed := pca.setMultiValuesWithCAS(ctx, ttl, vals, lockValues)

	// Unlock any keys that weren't successfully written.
	toUnlock := make(map[string]string)
	for key, lockVal := range lockValues {
		found := false
		for _, s := range succeeded {
			if s == key {
				found = true
				break
			}
		}
		if !found {
			toUnlock[key] = lockVal
		}
	}
	if len(toUnlock) > 0 {
		pca.unlockMultiKeys(ctx, toUnlock)
	}

	return NewBatchError(failed, succeeded)
}

// ForceSet unconditionally writes a value to Redis, bypassing all locks.
// Any in-progress Get or Set on this key will see ErrLockLost and retry.
func (pca *PrimeableCacheAside) ForceSet(ctx context.Context, ttl time.Duration, key, value string) error {
	return pca.client.Do(ctx, pca.client.B().Set().Key(key).Value(value).Px(ttl).Build()).Error()
}

// ForceSetMulti unconditionally writes multiple values to Redis, bypassing all locks.
func (pca *PrimeableCacheAside) ForceSetMulti(ctx context.Context, ttl time.Duration, values map[string]string) error {
	if len(values) == 0 {
		return nil
	}
	cmds := make(rueidis.Commands, 0, len(values))
	for key, val := range values {
		cmds = append(cmds, pca.client.B().Set().Key(key).Value(val).Px(ttl).Build())
	}
	resps := pca.client.DoMulti(ctx, cmds...)
	for _, resp := range resps {
		if err := resp.Error(); err != nil {
			return err
		}
	}
	return nil
}

// waitForReadLocks registers all keys, batch-reads them, and waits for any that
// currently hold a lock value. Uses correct ordering: register first, then DoCache.
func (pca *PrimeableCacheAside) waitForReadLocks(ctx context.Context, keys []string) error {
	// 1. Register ALL keys first so onInvalidate can find the lockEntries.
	waitChans := make(map[string]<-chan struct{}, len(keys))
	for _, key := range keys {
		waitChans[key] = pca.register(key)
	}

	// 2. DoMultiCache to subscribe and read values.
	multi := make([]rueidis.CacheableTTL, len(keys))
	for i, key := range keys {
		multi[i] = rueidis.CacheableTTL{
			Cmd: pca.client.B().Get().Key(key).Cache(),
			TTL: pca.lockTTL,
		}
	}
	resps := pca.client.DoMultiCache(ctx, multi...)

	// 3. Collect channels for keys that have locks.
	var lockedChans []<-chan struct{}
	for i, resp := range resps {
		val, err := resp.ToString()
		if err != nil {
			continue // Redis nil or error — no lock.
		}
		if strings.HasPrefix(val, pca.lockPrefix) {
			lockedChans = append(lockedChans, waitChans[keys[i]])
		}
	}

	if len(lockedChans) == 0 {
		return nil
	}
	return syncx.WaitForAll(ctx, lockedChans)
}
