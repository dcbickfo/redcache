package redcache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/lockmanager"
	"github.com/dcbickfo/redcache/internal/mapsx"
	"github.com/dcbickfo/redcache/internal/writelock"
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
	writeLockManager writelock.WriteLockManager // Write lock coordination (interface)
}

// waitForReadLock waits for any active read lock on the key to be released.
// Uses lockManager to handle registration and subscription in the correct order.
func (pca *PrimeableCacheAside) waitForReadLock(ctx context.Context, key string) error {
	startTime := time.Now()

	// Register for invalidations and subscribe to Redis client-side cache in correct order
	// This prevents the race condition between registration and subscription
	waitHandle, val, err := pca.lockManager.WaitForKeyWithSubscription(ctx, key, time.Second)

	pca.logger.Debug("waitForReadLock check",
		"key", key,
		"hasValue", err == nil,
		"value", val,
		"isLock", err == nil && pca.lockManager.IsLockValue(val))

	// Only wait if there's actually a lock
	if err == nil && pca.lockManager.IsLockValue(val) {
		pca.logger.Debug("read lock exists, waiting for it to complete", "key", key, "lockValue", val)

		// Wait for lock release (already registered and subscribed above)
		if waitErr := waitHandle.Wait(ctx); waitErr != nil {
			pca.logger.Debug("lock wait failed", "key", key, "duration", time.Since(startTime), "error", waitErr)
			return waitErr
		}

		waitDuration := time.Since(startTime)
		pca.logger.Debug("read lock cleared", "key", key, "duration", waitDuration)

		// Log if timeout likely occurred
		if waitDuration > pca.lockTTL-100*time.Millisecond {
			pca.logger.Error("lock release likely timed out", "key", key, "duration", waitDuration, "lockTTL", pca.lockTTL)
		}
	}

	return nil
}

// trySetKeyFuncForWrite performs coordinated cache update operation with distributed locking.
// Cache locks provide both write-write coordination and CAS protection.
// Uses WriteLockManager which can overwrite existing values (but not other locks).
func (pca *PrimeableCacheAside) trySetKeyFuncForWrite(ctx context.Context, ttl time.Duration, key string, fn func(ctx context.Context, key string) (string, error)) (val string, err error) {
	// Wait for any existing read locks to complete
	if waitErr := pca.waitForReadLock(ctx, key); waitErr != nil {
		return "", waitErr
	}

	// Acquire write lock with retry logic (WriteLockManager can overwrite values, but not locks)
	var lockVal string
	for {
		lockVal, err = pca.writeLockManager.AcquireWriteLock(ctx, key)
		if err == nil {
			break // Successfully acquired lock
		}

		// Check if error is lock contention (retryable) or a real error
		if !errors.Is(err, ErrLockFailed) {
			return "", err
		}

		// Lock contention - wait for the lock to be released
		pca.logger.Debug("write lock contention, waiting for release", "key", key)
		// Register for invalidations and subscribe in correct order
		waitHandle, _, _ := pca.lockManager.WaitForKeyWithSubscription(ctx, key, time.Second)
		if waitErr := waitHandle.Wait(ctx); waitErr != nil {
			return "", waitErr
		}
		// Retry after wait
	}

	pca.logger.Debug("acquired cache key lock (blocking)", "key", key)

	// We have the lock, now execute the callback
	val, err = fn(ctx, key)
	if err != nil {
		// Release the cache lock on error
		_ = pca.writeLockManager.ReleaseWriteLock(ctx, key, lockVal)
		return "", err
	}

	// Set the value in Redis using a CAS to verify we still hold the lock
	// This uses strict compare-and-swap (CAS): only sets if we hold our exact lock value
	// This prevents Set from overwriting ForceSet values that may have stolen our lock
	succeeded, failed := pca.writeLockManager.CommitWriteLocks(ctx, ttl,
		map[string]string{key: lockVal},
		map[string]string{key: val})

	if len(failed) > 0 {
		return "", failed[key]
	}

	if len(succeeded) == 0 {
		return "", fmt.Errorf("failed to set value for key %q", key)
	}

	// Note: No DoCache needed here. Redis automatically sends invalidation messages to all
	// clients currently tracking this key when SET executes. Any Get operation will call
	// DoCache to both fetch the value and subscribe to future invalidations (cacheaside.go:493).
	// The Set-performing client doesn't need to track the key it just wrote.

	pca.logger.Debug("value set successfully", "key", key)
	return val, nil
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

	pca := &PrimeableCacheAside{
		CacheAside: ca,
	}

	// Create write lock manager for Set operations
	// IMPORTANT: Pass LockManager to ensure cohesive lock value generation and prefix checking
	pca.writeLockManager = writelock.NewCASWriteLockManager(writelock.Config{
		Client:      ca.client,
		LockTTL:     ca.lockTTL,
		LockManager: ca.lockManager, // Ensures consistent lock generation and checking
		Logger:      ca.logger,
	})

	return pca, nil
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
	// Use sequential acquisition strategy from WriteLockManager for deadlock prevention
	lockValues, savedValues, acquireErr := pca.writeLockManager.AcquireMultiWriteLocksSequential(ctx, keys)
	if acquireErr != nil {
		return nil, acquireErr
	}
	defer func() {
		// Release cache locks only for keys that weren't successfully set.
		// Successfully set keys no longer contain locks (they contain the real values),
		// so ReleaseWriteLock would fail the comparison check and do nothing, but it's wasteful
		// to attempt the unlock at all.
		for key, lockVal := range lockValues {
			_ = pca.writeLockManager.ReleaseWriteLock(ctx, key, lockVal)
		}
	}()

	// Execute the callback with locked keys
	vals, err := fn(ctx, keys)
	if err != nil {
		return nil, err
	}

	// Set all values using CAS to verify we still hold the locks
	// CommitWriteLocks handles CSC population internally
	var succeeded map[string]string
	var failed map[string]error
	if len(vals) == 0 {
		succeeded = make(map[string]string)
		failed = make(map[string]error)
	} else {
		succeeded, failed = pca.writeLockManager.CommitWriteLocks(ctx, ttl, lockValues, vals)
	}

	// Remove successfully set keys from lockValues so defer won't try to unlock them.
	// This is an optimization: unlockKey would be safe (lock check would fail) but wasteful.
	for key := range succeeded {
		delete(lockValues, key)
	}

	if len(failed) > 0 {
		// Partial failure - restore previous values for failed keys
		pca.restoreFailedKeys(ctx, failed, savedValues, lockValues)
		return succeeded, NewBatchError(failed, mapsx.Keys(succeeded))
	}

	pca.logger.Debug("SetMulti completed", "keys", keys, "count", len(lockValues))
	return vals, nil
}

// restoreFailedKeys restores previous values for keys that failed CAS during SetMulti.
// This prevents leaving lock placeholders in Redis when the final commit fails.
func (pca *PrimeableCacheAside) restoreFailedKeys(
	ctx context.Context,
	failed map[string]error,
	savedValues map[string]string,
	lockValues map[string]string,
) {
	pca.logger.Debug("SetMulti partial failure, restoring previous values for failed keys",
		"failed", len(failed))

	failedKeys := mapsx.Keys(failed)
	restoreMap := make(map[string]string)
	failedLockVals := make(map[string]string)
	for _, key := range failedKeys {
		if savedVal, ok := savedValues[key]; ok {
			restoreMap[key] = savedVal
			if lockVal, hasLock := lockValues[key]; hasLock {
				failedLockVals[key] = lockVal
			}
		}
	}
	if len(restoreMap) > 0 {
		pca.writeLockManager.RestoreValues(ctx, restoreMap, failedLockVals)
	}
}

// waitForReadLocks checks for read locks on the given keys and waits for them to complete.
// Uses lock manager's WaitHandle abstraction for consistent invalidation handling.
func (pca *PrimeableCacheAside) waitForReadLocks(ctx context.Context, keys []string) {
	// Register wait handles FIRST for all keys (before checking locks)
	// This prevents race where locks are released between check and registration
	waitHandleMap := make(map[string]lockmanager.WaitHandle, len(keys))
	for _, key := range keys {
		waitHandleMap[key] = pca.lockManager.WaitForKey(key)
	}

	// Now check which keys have locks (subscribes to Redis via DoMultiCache)
	lockedKeys := pca.lockManager.CheckMultiKeysLocked(ctx, keys)
	if len(lockedKeys) == 0 {
		return // No read locks to wait for
	}

	pca.logger.Debug("waiting for read locks to complete", "count", len(lockedKeys))

	// Wait for all locked keys (returns immediately if already released)
	waitHandles := make([]lockmanager.WaitHandle, 0, len(lockedKeys))
	for _, key := range lockedKeys {
		waitHandles = append(waitHandles, waitHandleMap[key])
	}

	// Wait for all handles concurrently using lock manager's helper
	if err := lockmanager.WaitForAll(ctx, waitHandles); err != nil {
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

	// Note: No DoMultiCache needed. Redis automatically sends invalidation
	// messages to all clients tracking these keys when SET executes.
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

// Close cleans up resources used by the PrimeableCacheAside instance.
// It cancels all pending lock wait operations and cleans up internal state.
// Note: This does NOT close the underlying Redis client, as that's owned by the caller.
// The caller is responsible for closing the Redis client when done.
func (pca *PrimeableCacheAside) Close() {
	// Clean up parent CacheAside resources (lock entries, etc)
	if pca.CacheAside != nil {
		pca.CacheAside.Close()
	}
}
