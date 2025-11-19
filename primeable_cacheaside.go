package redcache

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/cmdx"
	"github.com/dcbickfo/redcache/internal/lockutil"
	"github.com/dcbickfo/redcache/internal/mapsx"
	"github.com/dcbickfo/redcache/internal/syncx"
)

const (
	// lockRetryInterval is the interval for periodic lock acquisition retries.
	// Used when waiting for locks to be released.
	lockRetryInterval = 50 * time.Millisecond
)

// Pre-compiled Lua scripts for atomic operations.
var (
	unlockKeyScript = rueidis.NewLuaScript(`
		local key = KEYS[1]
		local expected = ARGV[1]
		if redis.call("GET", key) == expected then
			return redis.call("DEL", key)
		else
			return 0
		end
	`)

	// acquireWriteLockScript atomically acquires a write lock for Set operations.
	//
	// IMPORTANT: We CANNOT use SET NX here like CacheAside does for Get operations.
	//
	// Why SET NX doesn't work for Set operations:
	// - SET NX only succeeds if the key doesn't exist at all
	// - Set operations need to overwrite existing real values (not locks)
	// - If we used SET NX, Set would fail when trying to update an existing cached value
	//
	// This Lua script provides the correct behavior:
	// - Acquires lock if key is empty (like SET NX)
	// - Acquires lock if key contains a real value (overwrites to prepare for Set)
	// - REFUSES to acquire if there's an active lock from a Get operation
	//
	// This prevents the race condition where Set would overwrite Get's lock,
	// causing Get to lose its lock and retry, ultimately seeing Set's value
	// instead of its own callback result.
	//
	// Returns 0 if there's an existing lock (cannot acquire).
	// Returns 1 if lock was successfully acquired.
	acquireWriteLockScript = rueidis.NewLuaScript(`
		local key = KEYS[1]
		local lock_value = ARGV[1]
		local ttl = ARGV[2]
		local lock_prefix = ARGV[3]

		local current = redis.call("GET", key)

		-- If key is empty, we can set our lock
		if current == false then
			redis.call("SET", key, lock_value, "PX", ttl)
			return 1
		end

		-- If current value is a lock (has lock prefix), we cannot acquire
		if string.sub(current, 1, string.len(lock_prefix)) == lock_prefix then
			return 0
		end

		-- Current value is a real value (not a lock), we can overwrite with our lock
		redis.call("SET", key, lock_value, "PX", ttl)
		return 1
	`)

	// acquireWriteLockWithBackupScript acquires a lock and returns the previous value.
	// This is used for sequential lock acquisition where we need to restore values
	// if we can't acquire all locks.
	//
	// Returns: [success (0 or 1), previous_value or false]
	//   - [0, current] if lock exists (cannot acquire)
	//   - [1, false] if key was empty (acquired from nothing)
	//   - [1, current] if key had real value (acquired, can restore)
	acquireWriteLockWithBackupScript = rueidis.NewLuaScript(`
		local key = KEYS[1]
		local lock_value = ARGV[1]
		local ttl = ARGV[2]
		local lock_prefix = ARGV[3]

		local current = redis.call("GET", key)

		-- If key is empty, acquire and return false (nothing to restore)
		if current == false then
			redis.call("SET", key, lock_value, "PX", ttl)
			return {1, false}
		end

		-- If current value is a lock, cannot acquire
		if string.sub(current, 1, string.len(lock_prefix)) == lock_prefix then
			return {0, current}
		end

		-- Current value is real data - save it before overwriting
		redis.call("SET", key, lock_value, "PX", ttl)
		return {1, current}
	`)

	// restoreValueOrDeleteScript restores a saved value or deletes the key.
	// Used when releasing locks during sequential acquisition rollback.
	//
	// ARGV[2] can be:
	//   - false/nil: delete the key (was empty before)
	//   - string: restore the original value
	restoreValueOrDeleteScript = rueidis.NewLuaScript(`
		local key = KEYS[1]
		local expected_lock = ARGV[1]
		local restore_value = ARGV[2]

		-- Only restore if we still hold our lock
		if redis.call("GET", key) == expected_lock then
			if restore_value and restore_value ~= "" then
				-- Restore original value
				redis.call("SET", key, restore_value)
			else
				-- Was empty before, delete
				redis.call("DEL", key)
			end
			return 1
		else
			-- Someone else has the key now, don't touch it
			return 0
		end
	`)

	setWithLockScript = rueidis.NewLuaScript(`
		local key = KEYS[1]
		local value = ARGV[1]
		local ttl = ARGV[2]
		local expected_lock = ARGV[3]

		local current = redis.call("GET", key)

		-- STRICT CAS: We can ONLY set if we still hold our exact lock value
		-- This prevents Set from overwriting ForceSet values that stole our lock
		--
		-- If current is nil (false in Lua), our lock expired or was deleted
		-- If current is different, either:
		--   - Another Set operation acquired a different lock
		--   - A ForceSet operation overwrote our lock with a real value
		--   - Our lock naturally expired
		--
		-- In all cases where we don't hold our exact lock, we return 0 (failure)
		if current == expected_lock then
			redis.call("SET", key, value, "PX", ttl)
			return 1
		else
			return 0
		end
	`)
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

	// Acquire cache lock using acquireWriteLockScript
	// The Lua script provides write-write coordination by checking for existing locks
	// and refusing to acquire if another Set operation holds the lock.
	//
	// IMPORTANT: Do NOT use SET NX here (see acquireWriteLockScript comment for details).
	// The script ensures Set can overwrite real values but won't overwrite active locks.
	lockVal := pca.generateLockValue()
	ticker := time.NewTicker(lockRetryInterval)
	defer ticker.Stop()

	for {
		result := acquireWriteLockScript.Exec(ctx, pca.client,
			[]string{key},
			[]string{lockVal, strconv.FormatInt(pca.lockTTL.Milliseconds(), 10), pca.lockPrefix})

		success, execErr := result.AsInt64()
		if execErr != nil {
			return "", fmt.Errorf("failed to acquire cache lock for key %q: %w", key, execErr)
		}

		if success == 1 {
			// Successfully acquired the lock
			break
		}

		// There's an active lock (from Get or another Set)
		// Wait for it to be released via invalidation
		waitChan := pca.register(key)
		_ = pca.client.DoCache(ctx, pca.client.B().Get().Key(key).Cache(), pca.lockTTL)

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-waitChan:
			// Lock was released, retry acquisition
			continue
		case <-ticker.C:
			// Periodic retry
			continue
		}
	}

	pca.logger.Debug("acquired cache key lock", "key", key)

	// We have the lock, now execute the callback
	val, err = fn(ctx, key)
	if err != nil {
		// Release the cache lock on error
		pca.unlockKey(ctx, key, lockVal)
		return "", err
	}

	// Set the value in Redis using a Lua script that verifies we still hold the lock
	// This uses strict compare-and-swap (CAS): only sets if we hold our exact lock value
	// This prevents Set from overwriting ForceSet values that may have stolen our lock
	setResult := setWithLockScript.Exec(ctx, pca.client,
		[]string{key},
		[]string{val, strconv.FormatInt(ttl.Milliseconds(), 10), lockVal})

	setSuccess, err := setResult.AsInt64()
	if err != nil {
		return "", fmt.Errorf("failed to set value for key %q: %w", key, err)
	}

	if setSuccess == 0 {
		return "", fmt.Errorf("%w for key %q", ErrLockLost, key)
	}

	// Note: No DoCache needed here. Redis automatically sends invalidation messages to all
	// clients currently tracking this key when SET executes. Any Get operation will call
	// DoCache to both fetch the value and subscribe to future invalidations (cacheaside.go:493).
	// The Set-performing client doesn't need to track the key it just wrote.

	pca.logger.Debug("value set successfully", "key", key)
	return val, nil
}

// unlockKey releases a cache lock if it matches the expected value.
func (pca *PrimeableCacheAside) unlockKey(ctx context.Context, key string, lockVal string) {
	_ = unlockKeyScript.Exec(ctx, pca.client, []string{key}, []string{lockVal}).Error()
}

// lockAcquisitionResult holds the result of processing a single lock acquisition response.
type lockAcquisitionResult struct {
	acquired   bool
	lockValue  string
	savedValue string
	hasSaved   bool
}

// processLockAcquisitionResponse processes a single lock acquisition response.
// Returns the result or an error if the response is invalid.
func processLockAcquisitionResponse(resp rueidis.RedisResult, key, lockVal string) (lockAcquisitionResult, error) {
	// Response is [success, previous_value]
	result, err := resp.ToArray()
	if err != nil {
		return lockAcquisitionResult{}, fmt.Errorf("failed to acquire cache lock for key %q: %w", key, err)
	}

	if len(result) != 2 {
		return lockAcquisitionResult{}, fmt.Errorf("unexpected response length for key %q: got %d, expected 2", key, len(result))
	}

	success, err := result[0].AsInt64()
	if err != nil {
		return lockAcquisitionResult{}, fmt.Errorf("failed to parse success for key %q: %w", key, err)
	}

	if success == 1 {
		// Acquired successfully - check if there's a previous value to save
		prevValue, prevErr := result[1].ToString()
		if prevErr == nil && prevValue != "" {
			// Previous value exists and is not empty
			return lockAcquisitionResult{
				acquired:   true,
				lockValue:  lockVal,
				savedValue: prevValue,
				hasSaved:   true,
			}, nil
		}
		// No previous value (key was empty)
		return lockAcquisitionResult{
			acquired:  true,
			lockValue: lockVal,
		}, nil
	}

	// Failed to acquire
	return lockAcquisitionResult{acquired: false}, nil
}

// tryAcquireMultiCacheLocksBatched attempts to acquire cache locks for multiple keys in batches.
// Uses acquireWriteLockWithBackupScript which saves the previous value before acquiring the lock.
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

	acquired = make(map[string]string)
	savedValues = make(map[string]string)
	failed = make([]string, 0)

	// Group by slot and build Lua exec statements
	stmtsBySlot := pca.groupLockAcquisitionsBySlot(keys)

	// Execute all slots and collect results
	for _, stmts := range stmtsBySlot {
		// Batch execute all lock acquisitions for this slot
		resps := acquireWriteLockWithBackupScript.ExecMulti(ctx, pca.client, stmts.execStmts...)

		// Process responses in order
		for i, resp := range resps {
			key := stmts.keyOrder[i]
			lockVal := stmts.lockVals[i]

			result, respErr := processLockAcquisitionResponse(resp, key, lockVal)
			if respErr != nil {
				return nil, nil, nil, respErr
			}

			if result.acquired {
				acquired[key] = result.lockValue
				if result.hasSaved {
					savedValues[key] = result.savedValue
				}
			} else {
				failed = append(failed, key)
			}
		}
	}

	return acquired, savedValues, failed, nil
}

// slotLockStatements holds lock acquisition statements grouped by slot.
type slotLockStatements struct {
	keyOrder  []string
	lockVals  []string
	execStmts []rueidis.LuaExec
}

// estimateSlotDistribution estimates the number of Redis cluster slots and keys per slot
// for efficient pre-allocation when grouping operations by slot.
// Uses a heuristic of ~8 slots for typical Redis Cluster distributions.
func estimateSlotDistribution(itemCount int) (estimatedSlots, estimatedPerSlot int) {
	estimatedSlots = itemCount / 8
	if estimatedSlots < 1 {
		estimatedSlots = 1
	}
	estimatedPerSlot = (itemCount / estimatedSlots) + 1
	return
}

// groupLockAcquisitionsBySlot groups lock acquisition operations by Redis cluster slot.
// This is necessary for Lua scripts which must execute on a single node in Redis Cluster.
// Unlike regular commands, Lua scripts (LuaExec) require manual slot grouping.
func (pca *PrimeableCacheAside) groupLockAcquisitionsBySlot(keys []string) map[uint16]slotLockStatements {
	if len(keys) == 0 {
		return nil
	}

	// Pre-allocate with estimated capacity
	estimatedSlots, estimatedPerSlot := estimateSlotDistribution(len(keys))
	stmtsBySlot := make(map[uint16]slotLockStatements, estimatedSlots)

	// Pre-calculate lock TTL string once
	lockTTLStr := strconv.FormatInt(pca.lockTTL.Milliseconds(), 10)

	for _, key := range keys {
		lockVal := pca.generateLockValue()
		slot := cmdx.Slot(key)
		stmts := stmtsBySlot[slot]

		// Pre-allocate slices on first access to this slot
		if stmts.keyOrder == nil {
			stmts.keyOrder = make([]string, 0, estimatedPerSlot)
			stmts.lockVals = make([]string, 0, estimatedPerSlot)
			stmts.execStmts = make([]rueidis.LuaExec, 0, estimatedPerSlot)
		}

		stmts.keyOrder = append(stmts.keyOrder, key)
		stmts.lockVals = append(stmts.lockVals, lockVal)
		stmts.execStmts = append(stmts.execStmts, rueidis.LuaExec{
			Keys: []string{key},
			Args: []string{lockVal, lockTTLStr, pca.lockPrefix},
		})
		stmtsBySlot[slot] = stmts
	}

	return stmtsBySlot
}

// releaseMultiCacheLocks releases multiple cache locks.
func (pca *PrimeableCacheAside) releaseMultiCacheLocks(ctx context.Context, lockValues map[string]string) {
	for key, lockVal := range lockValues {
		pca.unlockKey(ctx, key, lockVal)
	}
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
// Uses the restoreValueOrDeleteScript to atomically restore values or delete keys.
// This is critical for preventing cache misses when releasing locks on partial failure.
func (pca *PrimeableCacheAside) restoreMultiValues(
	ctx context.Context,
	lockValues map[string]string,
	savedValues map[string]string,
) {
	if len(lockValues) == 0 {
		return
	}

	for key, lockVal := range lockValues {
		// Get the saved value (empty string if key didn't exist before)
		savedVal := savedValues[key] // Empty string if not in map

		// Use Lua script to restore value or delete key atomically
		_ = restoreValueOrDeleteScript.Exec(ctx, pca.client,
			[]string{key},
			[]string{lockVal, savedVal},
		).Error()
	}
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
// Uses batched Lua script execution grouped by Redis cluster slot for optimal performance.
func (pca *PrimeableCacheAside) setMultiValuesWithCAS(
	ctx context.Context,
	ttl time.Duration,
	values map[string]string,
	lockValues map[string]string,
) (map[string]string, map[string]error) {
	if len(values) == 0 {
		return make(map[string]string), make(map[string]error)
	}

	succeeded := make(map[string]string)
	failed := make(map[string]error)

	// Group by slot for efficient batching
	stmtsBySlot := pca.groupSetValuesBySlot(values, lockValues, ttl)

	// Execute all slots in parallel and collect results
	for slot, stmts := range stmtsBySlot {
		// Execute all Lua scripts for this slot in a single batch
		setResps := setWithLockScript.ExecMulti(ctx, pca.client, stmts.execStmts...)

		// Process responses in order
		for i, resp := range setResps {
			key := stmts.keyOrder[i]
			value := values[key]

			setSuccess, err := resp.AsInt64()
			if err != nil {
				pca.logger.Debug("set CAS failed for key", "key", key, "slot", slot, "error", err)
				failed[key] = fmt.Errorf("failed to set value: %w", err)
				continue
			}

			if setSuccess == 0 {
				pca.logger.Debug("set CAS lock lost for key", "key", key, "slot", slot)
				failed[key] = fmt.Errorf("%w", ErrLockLost)
				continue
			}

			succeeded[key] = value
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

// slotSetStatements holds Lua execution statements grouped by slot.
type slotSetStatements struct {
	keyOrder  []string
	execStmts []rueidis.LuaExec
}

// groupSetValuesBySlot groups set operations by Redis cluster slot for Lua script execution.
// This is necessary for CAS (compare-and-swap) Lua scripts which must execute on a single node.
// Unlike regular SET commands which rueidis.DoMulti routes automatically, Lua scripts
// (LuaExec) require manual slot grouping to ensure atomic operations on the correct node.
func (pca *PrimeableCacheAside) groupSetValuesBySlot(
	values map[string]string,
	lockValues map[string]string,
	ttl time.Duration,
) map[uint16]slotSetStatements {
	if len(values) == 0 {
		return nil
	}

	// Pre-allocate with estimated capacity
	estimatedSlots, estimatedPerSlot := estimateSlotDistribution(len(values))
	stmtsBySlot := make(map[uint16]slotSetStatements, estimatedSlots)

	// Pre-calculate TTL string once
	ttlStr := strconv.FormatInt(ttl.Milliseconds(), 10)

	for key, value := range values {
		lockVal, hasLock := lockValues[key]
		if !hasLock {
			// Skip keys without locks (shouldn't happen, but be defensive)
			pca.logger.Error("no lock value for key in groupSetValuesBySlot", "key", key)
			continue
		}

		slot := cmdx.Slot(key)
		stmts := stmtsBySlot[slot]

		// Pre-allocate slices on first access to this slot
		if stmts.keyOrder == nil {
			stmts.keyOrder = make([]string, 0, estimatedPerSlot)
			stmts.execStmts = make([]rueidis.LuaExec, 0, estimatedPerSlot)
		}

		stmts.keyOrder = append(stmts.keyOrder, key)
		stmts.execStmts = append(stmts.execStmts, rueidis.LuaExec{
			Keys: []string{key},
			Args: []string{value, ttlStr, lockVal},
		})
		stmtsBySlot[slot] = stmts
	}

	return stmtsBySlot
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
		// so unlockKey would fail the comparison check and do nothing, but it's wasteful
		// to attempt the unlock at all.
		for key, lockVal := range lockValues {
			pca.unlockKey(ctx, key, lockVal)
		}
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
