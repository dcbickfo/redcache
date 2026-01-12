package redcache

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/cachelock"
	"github.com/dcbickfo/redcache/internal/lockutil"
	"github.com/dcbickfo/redcache/internal/mapsx"
	"github.com/dcbickfo/redcache/internal/syncx"
)

const (
	// lockRetryInterval is the interval for periodic lock acquisition retries.
	// Used when waiting for locks to be released.
	lockRetryInterval = 50 * time.Millisecond
)

// PrimeableCacheAside extends CacheAside with explicit Set operations for cache priming
// and coordinated cache updates. Unlike the base CacheAside which only populates cache on
// misses, PrimeableCacheAside allows proactive cache updates and warming.
//
// The callback function controls backing store behavior - it can implement write-through
// patterns (update database then cache), cache warming (read from database), expensive
// computations, or any other value generation logic.
//
// It inherits all capabilities from CacheAside:
//   - Get/GetMulti for cache-aside pattern with automatic population
//   - Del/DelMulti for cache invalidation
//   - Distributed locking and retry mechanisms
//
// And adds cache priming operations:
//   - Set/SetMulti for coordinated cache updates with locking
//   - ForceSet/ForceSetMulti for bypassing locks (use with caution)
//
// For convenience patterns like SetValue, see the examples folder which demonstrates
// how to use Set/SetMulti with pre-computed values.
//
// This is particularly useful for:
//   - Cache warming during application startup
//   - Proactive cache updates after database writes
//   - Maintaining cache consistency in write-heavy scenarios
//   - Preventing stale reads immediately after writes
//
// # Distributed Lock Safety Notice
//
// The distributed locks used by Set/SetMulti are designed for CACHE COORDINATION,
// not correctness-critical operations. They prevent inefficiencies (duplicate work,
// thundering herd) but do NOT provide strong consistency guarantees.
//
// For critical operations (financial transactions, inventory), use database
// transactions with proper isolation levels instead. See DISTRIBUTED_LOCK_SAFETY.md
// for detailed analysis and recommendations.
type PrimeableCacheAside struct {
	*CacheAside
	lockChecker lockutil.LockChecker // Shared lock checking logic (interface)
}

// waitForLockRelease waits for a lock to be released via invalidation or timeout.
// This is a common pattern used throughout the code to wait for distributed locks.
// NOTE: The caller should have already subscribed to invalidations using DoCache.
func (pca *PrimeableCacheAside) waitForLockRelease(ctx context.Context, key string) error {
	// Don't subscribe again - the caller should have already used DoCache
	// This avoids duplicate subscriptions and ensures we get the invalidation

	// Register locally to wait for the invalidation
	waitStart := time.Now()
	waitChan := pca.register(key)
	pca.logger.Debug("waiting for lock release", "key", key, "start", waitStart)

	// Wait for lock release using shared utility
	if err := lockutil.WaitForSingleLock(ctx, waitChan, pca.lockTTL); err != nil {
		pca.logger.Debug("lock wait failed", "key", key, "duration", time.Since(waitStart), "error", err)
		return err
	}

	waitDuration := time.Since(waitStart)
	pca.logger.Debug("lock released", "key", key, "duration", waitDuration)

	// If we waited for nearly the full lockTTL, it means we timed out rather than got an invalidation
	if waitDuration > pca.lockTTL-100*time.Millisecond {
		pca.logger.Error("lock release likely timed out rather than received invalidation",
			"key", key, "duration", waitDuration, "lockTTL", pca.lockTTL)
	}

	return nil
}

// waitForReadLock waits for any active read lock on the key to be released.
func (pca *PrimeableCacheAside) waitForReadLock(ctx context.Context, key string) error {
	startTime := time.Now()

	// Check if there's a read lock on this key using DoCache
	// This ensures we're subscribed to invalidations if a lock exists
	resp := pca.client.DoCache(ctx, pca.client.B().Get().Key(key).Cache(), time.Second)
	val, err := resp.ToString()

	pca.logger.Debug("waitForReadLock check",
		"key", key,
		"hasValue", err == nil,
		"value", val,
		"isLock", err == nil && pca.lockChecker.HasLock(val))

	if err == nil && pca.lockChecker.HasLock(val) {
		pca.logger.Debug("read lock exists, waiting for it to complete", "key", key, "lockValue", val)
		// Since we used DoCache, we're guaranteed to get an invalidation when the lock is released
		if waitErr := pca.waitForLockRelease(ctx, key); waitErr != nil {
			return waitErr
		}
		pca.logger.Debug("read lock cleared", "key", key, "duration", time.Since(startTime))
	}

	return nil
}

// trySetKeyFuncForWrite performs coordinated cache update operation with distributed locking.
// Cache locks provide both write-write coordination and CAS protection.
func (pca *PrimeableCacheAside) trySetKeyFuncForWrite(ctx context.Context, ttl time.Duration, key string, fn func(ctx context.Context, key string) (string, error)) (val string, err error) {
	// Wait for any existing read locks to complete
	if waitErr := pca.waitForReadLock(ctx, key); waitErr != nil {
		return "", waitErr
	}

	// Acquire cache lock using lockManager
	var lockVal string
	for {
		result, acquireErr := pca.lockManager.TryAcquire(ctx, []string{key}, cachelock.LockModeWrite)
		if acquireErr != nil {
			return "", fmt.Errorf("failed to acquire cache lock for key %q: %w", key, acquireErr)
		}

		if len(result.Acquired) > 0 {
			lockVal = result.Acquired[key]
			break
		}

		// Wait for lock release
		if len(result.WaitChans) > 0 {
			if waitErr := pca.lockManager.WaitForRelease(ctx, result.WaitChans); waitErr != nil {
				return "", waitErr
			}
		}
	}

	pca.logger.Debug("acquired cache key lock", "key", key)

	// We have the lock, now execute the callback
	val, err = fn(ctx, key)
	if err != nil {
		// Release the cache lock on error
		pca.lockManager.Release(ctx, map[string]string{key: lockVal})
		return "", err
	}

	// Commit the value using CAS
	committed, commitErr := pca.lockManager.Commit(ctx, map[string]string{key: lockVal}, map[string]string{key: val}, ttl)
	if commitErr != nil {
		return "", fmt.Errorf("failed to set value for key %q: %w", key, commitErr)
	}

	if len(committed) == 0 {
		return "", fmt.Errorf("%w for key %q", ErrLockLost, key)
	}

	pca.logger.Debug("value set successfully", "key", key)
	return val, nil
}

// tryAcquireMultiCacheLocksBatched attempts to acquire cache locks for multiple keys in batches.
// Uses lockManager.TryAcquire which saves the previous value before acquiring the lock.
//
// Returns:
//   - acquired: map of keys to their lock values (successfully acquired)
//   - savedValues: map of keys to their previous values (to restore on rollback)
//   - failed: list of keys that failed to acquire
//   - error: if a critical error occurred
func (pca *PrimeableCacheAside) tryAcquireMultiCacheLocksBatched(
	ctx context.Context,
	keys []string,
) (acquired map[string]string, savedValues map[string]string, failed []string, err error) {
	if len(keys) == 0 {
		return make(map[string]string), make(map[string]string), nil, nil
	}

	result, err := pca.lockManager.TryAcquire(ctx, keys, cachelock.LockModeWrite)
	if err != nil {
		return nil, nil, nil, err
	}

	// Build failed list from keys not in Acquired
	failed = make([]string, 0, len(keys)-len(result.Acquired))
	for _, key := range keys {
		if _, ok := result.Acquired[key]; !ok {
			failed = append(failed, key)
		}
	}

	return result.Acquired, result.SavedValues, failed, nil
}

// releaseMultiCacheLocks releases multiple cache locks.
func (pca *PrimeableCacheAside) releaseMultiCacheLocks(ctx context.Context, lockValues map[string]string) {
	pca.lockManager.Release(ctx, lockValues)
}

// acquireMultiCacheLocks acquires cache locks for multiple keys using sequential acquisition
// with value preservation to prevent deadlocks and cache misses.
//
// Strategy:
//  1. Sort keys for consistent ordering (prevents deadlocks)
//  2. Try batch-acquire all remaining keys
//  3. On partial failure:
//     a. Restore original values for acquired keys (using saved values from backup script)
//     b. Wait for FIRST failed key in sorted order (sequential, not any)
//     c. Acquire that specific key and continue
//  4. Repeat until all locks acquired
//
// This approach prevents deadlocks by ensuring clients always wait for keys in the same order,
// and prevents cache misses by restoring original values when releasing locks.
//
// Returns a map of keys to their lock values, or an error if acquisition fails.
func (pca *PrimeableCacheAside) acquireMultiCacheLocks(ctx context.Context, keys []string) (map[string]string, error) {
	if len(keys) == 0 {
		return make(map[string]string), nil
	}

	// Sort keys for consistent lock ordering (prevents deadlocks)
	sortedKeys := pca.sortKeys(keys)
	lockValues := make(map[string]string)
	ticker := time.NewTicker(lockRetryInterval)
	defer ticker.Stop()

	remainingKeys := sortedKeys
	for len(remainingKeys) > 0 {
		done, err := pca.tryAcquireBatchAndProcess(ctx, sortedKeys, remainingKeys, lockValues, ticker)
		if err != nil {
			return nil, err
		}
		if done {
			return lockValues, nil
		}
		// Update remaining keys for next iteration
		remainingKeys = pca.keysNotIn(sortedKeys, lockValues)
	}

	return lockValues, nil
}

// tryAcquireBatchAndProcess attempts to acquire locks for remaining keys and processes the result.
// Returns true if all locks were acquired (done), or an error if acquisition failed.
func (pca *PrimeableCacheAside) tryAcquireBatchAndProcess(
	ctx context.Context,
	sortedKeys []string,
	remainingKeys []string,
	lockValues map[string]string,
	ticker *time.Ticker,
) (bool, error) {
	// Try to batch-acquire all remaining keys with backup
	acquired, savedValues, failed, err := pca.tryAcquireMultiCacheLocksBatched(ctx, remainingKeys)
	if err != nil {
		// Critical error - release all locks we've acquired so far
		pca.releaseMultiCacheLocks(ctx, lockValues)
		return false, err
	}

	// Success: All remaining keys acquired
	if len(failed) == 0 {
		// Add newly acquired locks to our collection
		for k, v := range acquired {
			lockValues[k] = v
		}
		pca.logger.Debug("acquireMultiCacheLocks completed", "keys", sortedKeys, "count", len(lockValues))
		return true, nil
	}

	// Handle partial failure
	return false, pca.handlePartialLockFailure(ctx, remainingKeys, acquired, savedValues, failed, lockValues, ticker)
}

// handlePartialLockFailure processes a partial lock acquisition failure.
// It keeps locks acquired in sequential order and restores out-of-order locks.
func (pca *PrimeableCacheAside) handlePartialLockFailure(
	ctx context.Context,
	remainingKeys []string,
	acquired map[string]string,
	savedValues map[string]string,
	failed []string,
	lockValues map[string]string,
	ticker *time.Ticker,
) error {
	pca.logger.Debug("partial acquisition, analyzing sequential locks",
		"acquired_this_batch", len(acquired),
		"failed", len(failed),
		"total_acquired_so_far", len(lockValues))

	// Find the first failed key in sorted order
	firstFailedKey := pca.findFirstKey(remainingKeys, failed)

	// Determine which acquired keys to keep vs restore
	toKeep, toRestore := pca.splitAcquiredBySequence(remainingKeys, acquired, firstFailedKey)

	// Restore keys that were acquired out of order
	if len(toRestore) > 0 {
		pca.logger.Debug("restoring out-of-order locks",
			"restore_count", len(toRestore),
			"first_failed", firstFailedKey)
		pca.restoreMultiValues(ctx, toRestore, savedValues)
	}

	// Add sequential locks to our permanent collection
	for k, v := range toKeep {
		lockValues[k] = v
	}

	// Touch/refresh TTL on all locks we're keeping to prevent expiration
	if len(lockValues) > 0 {
		pca.touchMultiLocks(ctx, lockValues)
	}

	// Wait for the first failed key to be released
	err := pca.waitForSingleLock(ctx, firstFailedKey, ticker)
	if err != nil {
		// Context cancelled or timeout - release all locks
		pca.releaseMultiCacheLocks(ctx, lockValues)
		return err
	}

	return nil
}

// sortKeys creates a sorted copy of the keys to ensure consistent lock ordering.
func (pca *PrimeableCacheAside) sortKeys(keys []string) []string {
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)
	return sorted
}

// restoreMultiValues restores original values for keys that were acquired.
// Uses lockManager.ReleaseWithRestore to atomically restore values or delete keys.
// This is critical for preventing cache misses when releasing locks on partial failure.
func (pca *PrimeableCacheAside) restoreMultiValues(
	ctx context.Context,
	lockValues map[string]string,
	savedValues map[string]string,
) {
	pca.lockManager.ReleaseWithRestore(ctx, lockValues, savedValues)
}

// findFirstKey finds the first key from sortedKeys that appears in targetKeys.
// This ensures sequential lock acquisition by always waiting for the first failed key.
func (pca *PrimeableCacheAside) findFirstKey(sortedKeys []string, targetKeys []string) string {
	// Convert targetKeys to a set for O(1) lookup
	targetSet := make(map[string]bool, len(targetKeys))
	for _, k := range targetKeys {
		targetSet[k] = true
	}

	// Find first key in sorted order
	for _, k := range sortedKeys {
		if targetSet[k] {
			return k
		}
	}

	// Should never happen if targetKeys is non-empty and derived from sortedKeys
	return targetKeys[0]
}

// splitAcquiredBySequence splits acquired keys into those that should be kept (sequential)
// vs those that should be restored (out of order after a gap).
//
// Example: sortedKeys=[key1,key2,key3], acquired=[key1,key3], firstFailedKey=key2.
// Result: keep=[key1], restore=[key3].
func (pca *PrimeableCacheAside) splitAcquiredBySequence(
	sortedKeys []string,
	acquired map[string]string,
	firstFailedKey string,
) (keep map[string]string, restore map[string]string) {
	keep = make(map[string]string)
	restore = make(map[string]string)

	foundFailedKey := false
	for _, key := range sortedKeys {
		if key == firstFailedKey {
			foundFailedKey = true
			continue
		}

		lockVal, wasAcquired := acquired[key]
		if !wasAcquired {
			continue
		}

		if foundFailedKey {
			// This key comes after the failed key - out of order, must restore
			restore[key] = lockVal
		} else {
			// This key comes before the failed key - sequential, can keep
			keep[key] = lockVal
		}
	}

	return keep, restore
}

// touchMultiLocks refreshes the TTL on multiple locks to prevent expiration.
// This is critical when waiting for locks, as we need to maintain our holds.
func (pca *PrimeableCacheAside) touchMultiLocks(ctx context.Context, lockValues map[string]string) {
	if len(lockValues) == 0 {
		return
	}

	// Build SET commands to refresh TTL on each lock
	cmds := make([]rueidis.Completed, 0, len(lockValues))
	for key, lockVal := range lockValues {
		cmds = append(cmds, pca.client.B().Set().
			Key(key).
			Value(lockVal).
			Px(pca.lockTTL).
			Build())
	}

	// Execute all SET commands in parallel
	_ = pca.client.DoMulti(ctx, cmds...)
}

// keysNotIn returns keys from sortedKeys that are not in the acquired map.
func (pca *PrimeableCacheAside) keysNotIn(sortedKeys []string, acquired map[string]string) []string {
	remaining := make([]string, 0, len(sortedKeys))
	for _, key := range sortedKeys {
		if _, ok := acquired[key]; !ok {
			remaining = append(remaining, key)
		}
	}
	return remaining
}

// waitForSingleLock waits for a specific key's lock to be released.
// Fetches the key using DoCache to register for invalidation notifications,
// then waits for the invalidation event or ticker timeout.
func (pca *PrimeableCacheAside) waitForSingleLock(ctx context.Context, key string, ticker *time.Ticker) error {
	// Fetch key using DoCache to register for invalidation notifications
	// This ensures we get notified when the lock is released
	waitChan := pca.register(key)
	_ = pca.client.DoCache(ctx, pca.client.B().Get().Key(key).Cache(), pca.lockTTL)

	select {
	case <-waitChan:
		// Lock was released (invalidation event)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-ticker.C:
		// Periodic retry timeout
		return nil
	}
}

// setMultiValuesWithCAS sets multiple values using CAS to verify we still hold the locks.
// Returns maps of succeeded and failed keys.
// Uses lockManager.Commit for batched CAS execution grouped by Redis cluster slot.
func (pca *PrimeableCacheAside) setMultiValuesWithCAS(
	ctx context.Context,
	ttl time.Duration,
	values map[string]string,
	lockValues map[string]string,
) (map[string]string, map[string]error) {
	if len(values) == 0 {
		return make(map[string]string), make(map[string]error)
	}

	// Use lockManager.Commit for CAS write
	committedKeys, err := pca.lockManager.Commit(ctx, lockValues, values, ttl)
	if err != nil {
		// On error, mark all keys as failed
		failed := make(map[string]error, len(values))
		for key := range values {
			failed[key] = err
		}
		return make(map[string]string), failed
	}

	// Build succeeded map from committed keys
	succeeded := make(map[string]string, len(committedKeys))
	for _, key := range committedKeys {
		succeeded[key] = values[key]
	}

	// Build failed map from keys not in committed
	failed := make(map[string]error)
	for key := range values {
		if _, ok := succeeded[key]; !ok {
			pca.logger.Debug("set CAS lock lost for key", "key", key)
			failed[key] = fmt.Errorf("%w", ErrLockLost)
		}
	}

	// Populate client-side cache for all successfully set values
	// This ensures other clients can see the values via CSC
	// Critical for empty strings and all cached values
	if len(succeeded) > 0 {
		cacheCommands := make([]rueidis.CacheableTTL, 0, len(succeeded))
		for key := range succeeded {
			cacheCommands = append(cacheCommands, rueidis.CacheableTTL{
				Cmd: pca.client.B().Get().Key(key).Cache(),
				TTL: ttl,
			})
		}
		_ = pca.client.DoMultiCache(ctx, cacheCommands...)
	}

	return succeeded, failed
}

// NewPrimeableCacheAside creates a new PrimeableCacheAside instance with the specified
// Redis client options and cache-aside configuration.
//
// This function creates a base CacheAside instance and wraps it with cache priming
// capabilities. All validation and defaults are handled by NewRedCacheAside.
//
// Parameters:
//   - clientOption: Redis client configuration (addresses, credentials, etc.)
//   - caOption: Cache-aside behavior configuration (TTLs, logging, etc.)
//
// Returns:
//   - A configured PrimeableCacheAside instance
//   - An error if initialization fails
//
// Example:
//
//	pca, err := NewPrimeableCacheAside(
//	    rueidis.ClientOption{InitAddress: []string{"localhost:6379"}},
//	    CacheAsideOption{LockTTL: 5 * time.Second},
//	)
//	if err != nil {
//	    return err
//	}
//	defer pca.Close()
func NewPrimeableCacheAside(clientOption rueidis.ClientOption, caOption CacheAsideOption) (*PrimeableCacheAside, error) {
	ca, err := NewRedCacheAside(clientOption, caOption)
	if err != nil {
		return nil, err
	}

	return &PrimeableCacheAside{
		CacheAside:  ca,
		lockChecker: &lockutil.PrefixLockChecker{Prefix: caOption.LockPrefix},
	}, nil
}

// Set performs a coordinated cache update operation with distributed locking.
// Unlike Get which only fills empty cache slots, Set can overwrite existing values
// while ensuring coordination across distributed processes.
//
// The callback function controls backing store behavior and can implement:
//   - Write-through patterns: Update database, then cache the result
//   - Cache warming: Read from database to populate cache
//   - Expensive computations: Calculate and cache the result
//   - Any other value generation logic
//
// The operation flow:
//  1. Check for and wait on any active read locks
//  2. Acquire a distributed write lock using rueidislock
//  3. Execute the provided callback function
//  4. If successful, cache the returned value with the specified TTL
//  5. Release the lock (happens automatically even on failure)
//
// The method coordinates with read operations by:
//   - Waiting for active Get operations to complete before writing
//   - Using client-side caching invalidations for efficient waiting
//   - Ensuring consistency between read and write paths
//
// IMPORTANT: The callback function must complete within the lock TTL period
// (default 10 seconds, configurable via CacheAsideOption.LockTTL). If the callback
// takes longer than the TTL, the lock will expire, CAS will fail, and the value
// won't be cached. For long-running operations, consider breaking into smaller
// batches or increasing the lock TTL.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - ttl: Time-to-live for the cached value
//   - key: The cache key to set
//   - fn: Function to generate the value
//
// Returns an error if:
//   - The callback function returns an error
//   - Lock acquisition fails
//   - Context is cancelled or deadline exceeded
//   - The lock expires before the callback completes (ErrLockLost)
//
// Example with database write:
//
//	err := pca.Set(ctx, 5*time.Minute, "user:123", func(ctx context.Context, key string) (string, error) {
//	    // Write to database first
//	    user := User{ID: "123", Name: "Alice"}
//	    if err := database.UpdateUser(ctx, user); err != nil {
//	        return "", err
//	    }
//	    // Return the serialized value to cache
//	    return json.Marshal(user)
//	})
//
// Example with pre-computed value:
//
//	value := "pre-computed-data"
//	err := pca.Set(ctx, ttl, key, func(_ context.Context, _ string) (string, error) {
//	    return value, nil
//	})
//
// For bypassing locks entirely, use ForceSet (use with extreme caution).
// See examples/cache_operations.go for more patterns.
func (pca *PrimeableCacheAside) Set(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (val string, err error),
) error {
	// With rueidislock, waiting and retrying is handled internally
	// We also check for read locks before acquiring the write lock
	_, err := pca.trySetKeyFuncForWrite(ctx, ttl, key, fn)
	return err
}

// SetMulti performs coordinated cache update operations for multiple keys with distributed locking.
// Using rueidislock, this method efficiently handles concurrent access with proper invalidations.
//
// The callback function controls backing store behavior and can implement:
//   - Write-through patterns: Batch update database, then cache the results
//   - Cache warming: Batch read from database to populate cache
//   - Expensive computations: Calculate and cache multiple values
//   - Any other batch value generation logic
//
// The operation flow:
//  1. Check for and wait on any active read locks for the keys
//  2. Acquire distributed write locks for all keys in parallel using rueidislock
//  3. Acquire cache locks for all keys (stores lock values in Redis)
//  4. Execute the callback once with all successfully locked keys
//  5. Use CAS to write values (only succeeds if we still hold the locks)
//  6. Release all locks (happens automatically even on failure)
//
// The method coordinates with read operations by:
//   - Waiting for active GetMulti operations to complete before writing
//   - Using client-side caching invalidations for efficient waiting
//   - Ensuring consistency between read and write paths
//
// The method protects against ForceSetMulti races by:
//   - Using strict CAS to verify we still hold our locks before writing
//   - Returning partial success if some locks are stolen
//   - Preserving ForceSetMulti values when locks are lost
//
// IMPORTANT: The callback function must complete within the lock TTL period
// (default 10 seconds, configurable via CacheAsideOption.LockTTL). If the callback
// takes longer than the TTL, locks will expire, CAS will fail, and values won't
// be cached. For long-running operations, consider breaking into smaller batches
// or increasing the lock TTL.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - ttl: Time-to-live for cached values
//   - keys: List of all keys to process
//   - fn: Callback that receives all keys and returns their values
//
// Returns a map of all successfully processed keys to their cached values.
//
// Example with database batch write:
//
//	userIDs := []string{"user:1", "user:2", "user:3"}
//	result, err := pca.SetMulti(ctx, 10*time.Minute, userIDs,
//	    func(ctx context.Context, keys []string) (map[string]string, error) {
//	        users := make(map[string]string)
//	        for _, key := range keys {
//	            userID := strings.TrimPrefix(key, "user:")
//	            userData, err := database.UpdateUser(ctx, userID)
//	            if err != nil {
//	                return nil, err
//	            }
//	            users[key] = userData
//	        }
//	        return users, nil
//	    })
//
// Example with pre-computed values:
//
//	values := map[string]string{
//	    "cache:a": valueA,
//	    "cache:b": valueB,
//	}
//	keys := []string{"cache:a", "cache:b"}
//	result, err := pca.SetMulti(ctx, ttl, keys, func(_ context.Context, lockedKeys []string) (map[string]string, error) {
//	    result := make(map[string]string)
//	    for _, key := range lockedKeys {
//	        result[key] = values[key]
//	    }
//	    return result, nil
//	})
//
// For operations that bypass locking, use ForceSetMulti (use with caution).
func (pca *PrimeableCacheAside) SetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, keys []string) (val map[string]string, err error),
) (map[string]string, error) {
	if len(keys) == 0 {
		return make(map[string]string), nil
	}

	// First, wait for any read locks to be released
	pca.waitForReadLocks(ctx, keys)

	// Acquire cache locks for all keys
	// Cache locks provide both write-write coordination (prevents concurrent Sets)
	// and CAS protection (verifies we still hold locks during setMultiValuesWithCAS)
	lockValues, acquireErr := pca.acquireMultiCacheLocks(ctx, keys)
	if acquireErr != nil {
		return nil, acquireErr
	}
	defer func() {
		// Release cache locks only for keys that weren't successfully set.
		// Successfully set keys no longer contain locks (they contain the real values),
		// so Release would fail the comparison check and do nothing, but it's wasteful
		// to attempt the unlock at all.
		pca.lockManager.Release(ctx, lockValues)
	}()

	// Execute the callback with locked keys
	vals, err := fn(ctx, keys)
	if err != nil {
		return nil, err
	}

	// Set all values using CAS to verify we still hold the locks
	succeeded, failed := pca.setMultiValuesWithCAS(ctx, ttl, vals, lockValues)

	// Remove successfully set keys from lockValues so defer won't try to unlock them.
	// This is an optimization: unlockKey would be safe (lock check would fail) but wasteful.
	for key := range succeeded {
		delete(lockValues, key)
	}

	if len(failed) > 0 {
		// Partial failure - some keys lost their locks
		pca.logger.Debug("SetMulti partial failure", "succeeded", len(succeeded), "failed", len(failed))
		return succeeded, NewBatchError(failed, mapsx.Keys(succeeded))
	}

	pca.logger.Debug("SetMulti completed", "keys", keys, "count", len(lockValues))
	return vals, nil
}

// waitForReadLocks checks for read locks on the given keys and waits for them to complete.
// Follows the same pattern as CacheAside's registerAll and WaitForAll usage.
func (pca *PrimeableCacheAside) waitForReadLocks(ctx context.Context, keys []string) {
	// Use shared utility to batch check locks
	// BatchCheckLocks now uses DoMultiCache, so we're already subscribed to invalidations
	lockedKeys := lockutil.BatchCheckLocks(ctx, pca.client, keys, pca.lockChecker)
	if len(lockedKeys) == 0 {
		return // No read locks to wait for
	}

	pca.logger.Debug("waiting for read locks to complete", "count", len(lockedKeys))

	// No need to subscribe again - BatchCheckLocks already used DoMultiCache

	// Register all keys and get their wait channels
	waitChannels := make(map[string]<-chan struct{}, len(lockedKeys))
	for _, key := range lockedKeys {
		waitChannels[key] = pca.register(key)
	}

	// Use syncx.WaitForAll like CacheAside does
	channels := mapsx.Values(waitChannels)
	if err := syncx.WaitForAll(ctx, channels); err != nil {
		pca.logger.Debug("context cancelled while waiting for read locks", "error", err)
		return
	}

	pca.logger.Debug("all read locks released")
}

// setMultiValues sets multiple values in Redis using DoMulti.
// rueidis automatically handles routing commands to appropriate cluster nodes based on slot.
func (pca *PrimeableCacheAside) setMultiValues(ctx context.Context, ttl time.Duration, values map[string]string) error {
	if len(values) == 0 {
		return nil
	}

	// Build individual SET commands for each key-value pair
	// Each command targets a single slot, and rueidis DoMulti automatically routes
	// them to the appropriate cluster nodes with auto-pipelining for efficiency.
	cmds := make(rueidis.Commands, 0, len(values))
	for key, value := range values {
		cmd := pca.client.B().Set().Key(key).Value(value).Px(ttl).Build()
		cmds = append(cmds, cmd)
	}

	// Execute all SET commands - rueidis handles slot-based routing automatically
	resps := pca.client.DoMulti(ctx, cmds...)

	// Check for errors
	for _, resp := range resps {
		if err := resp.Error(); err != nil {
			return err
		}
	}

	// Populate CSC for all set values
	cacheCommands := make([]rueidis.CacheableTTL, 0, len(values))
	for key := range values {
		cacheCommands = append(cacheCommands, rueidis.CacheableTTL{
			Cmd: pca.client.B().Get().Key(key).Cache(),
			TTL: ttl,
		})
	}
	_ = pca.client.DoMultiCache(ctx, cacheCommands...)

	return nil
}

// ForceSet unconditionally sets a value in the cache, bypassing all distributed locks.
// This operation immediately overwrites any existing value or lock without coordination.
//
// WARNING: This method can cause race conditions and should be used sparingly.
// It will:
//   - Override any existing value, even if locked (both read and write locks)
//   - Trigger invalidation messages causing waiting operations to retry
//   - Potentially cause inconsistency if used during concurrent updates
//
// Parameters:
//   - ctx: Context for cancellation control
//   - ttl: Time-to-live for the cached value
//   - key: The cache key to set
//   - value: The value to store in cache
//
// Returns an error if the Redis SET operation fails.
//
// Appropriate use cases:
//   - Emergency cache correction when locks are stuck
//   - Cache warming during startup when no other operations are running
//   - Administrative tools for manual cache management
//   - Testing scenarios where coordination isn't needed
//
// Example:
//
//	err := pca.ForceSet(ctx, 5*time.Minute, "config:app", emergencyConfigData)
//
// For normal operations with proper coordination, use Set instead.
func (pca *PrimeableCacheAside) ForceSet(ctx context.Context, ttl time.Duration, key string, value string) error {
	err := pca.client.Do(ctx, pca.client.B().Set().Key(key).Value(value).Px(ttl).Build()).Error()
	if err != nil {
		return err
	}

	// Note: No DoCache needed here. Redis automatically sends invalidation messages to all
	// clients currently tracking this key. Each client manages its own CSC subscriptions via DoCache.
	return nil
}

// ForceSetMulti unconditionally sets multiple values in the cache, bypassing all locks.
// This operation immediately overwrites any existing values or locks without coordination.
//
// WARNING: This method can cause race conditions and should be used sparingly.
// It will:
//   - Override all specified keys, even if locked (both read and write locks)
//   - Trigger invalidation messages for all affected keys
//   - Potentially cause inconsistency if used during concurrent operations
//
// The operation is optimized for Redis clusters by:
//   - Grouping keys by slot for efficient routing
//   - Executing updates in parallel per slot
//   - Minimizing round trips to Redis
//
// Parameters:
//   - ctx: Context for cancellation control
//   - ttl: Time-to-live for all cached values
//   - values: Map of cache keys to their values
//
// Returns an error if any SET operation fails.
// The operation is not atomic - some keys may be updated even if others fail.
//
// Appropriate use cases:
//   - Bulk cache warming during application startup
//   - Migration scripts when the application is offline
//   - Emergency bulk cache corrections
//   - Test data setup in isolated environments
//
// Example:
//
//	values := map[string]string{
//	    "config:db":    dbConfig,
//	    "config:cache": cacheConfig,
//	    "config:api":   apiConfig,
//	}
//	err := pca.ForceSetMulti(ctx, 1*time.Hour, values)
//
// For normal operations with proper coordination, use SetMulti instead.
func (pca *PrimeableCacheAside) ForceSetMulti(ctx context.Context, ttl time.Duration, values map[string]string) error {
	return pca.setMultiValues(ctx, ttl, values)
}

// Close closes both the underlying CacheAside client and the rueidislock locker.
// This should be called when the PrimeableCacheAside is no longer needed.
// Close cleans up all resources used by PrimeableCacheAside.
// It cleans up parent CacheAside resources and closes the Redis client.
func (pca *PrimeableCacheAside) Close() {
	// Clean up parent CacheAside resources (lock entries, etc)
	if pca.CacheAside != nil {
		pca.CacheAside.Close()
	}

	// Close Redis client
	if pca.CacheAside != nil && pca.Client() != nil {
		pca.Client().Close()
	}
}
