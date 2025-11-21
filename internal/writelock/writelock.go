// Package writelock provides write lock management for cache Set operations with CAS semantics.
// This package handles the complex lock acquisition, rollback, and slot grouping logic
// required for PrimeableCacheAside Set/SetMulti operations.
package writelock

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/cmdx"
	"github.com/dcbickfo/redcache/internal/errs"
	"github.com/dcbickfo/redcache/internal/lockmanager"
	"github.com/dcbickfo/redcache/internal/logger"
	"github.com/dcbickfo/redcache/internal/luascript"
)

// WriteLockManager defines the interface for write lock management with CAS semantics.
// Unlike simple read locks (LockManager), write locks need to:
// - Overwrite real values but not other locks
// - Support rollback via backup/restore
// - Handle partial acquisition failures.
type WriteLockManager interface {
	// AcquireWriteLock acquires a single write lock with retry.
	// Returns the lock value on success, or error if context cancelled.
	AcquireWriteLock(ctx context.Context, key string) (lockValue string, err error)

	// AcquireMultiWriteLocks attempts to acquire write locks for multiple keys.
	// Returns:
	//   - acquired: map of successfully locked keys to lock values
	//   - savedValues: map of keys to their previous values (for rollback)
	//   - failed: keys that couldn't be locked
	//   - error: critical errors (context cancellation, Redis errors)
	AcquireMultiWriteLocks(ctx context.Context, keys []string) (
		acquired map[string]string,
		savedValues map[string]string,
		failed []string,
		err error,
	)

	// ReleaseWriteLock releases a single write lock if it matches the expected value.
	ReleaseWriteLock(ctx context.Context, key string, lockValue string) error

	// ReleaseWriteLocks releases multiple write locks.
	ReleaseWriteLocks(ctx context.Context, lockValues map[string]string)

	// RestoreValues restores backed-up values (rollback on partial failure).
	RestoreValues(ctx context.Context, savedValues map[string]string, lockValues map[string]string)

	// TouchLocks refreshes TTLs on multiple locks (for long operations).
	TouchLocks(ctx context.Context, lockValues map[string]string)

	// CommitWriteLocks atomically replaces write lock values with real values using CAS.
	// Only succeeds for keys where we still hold the exact lock value.
	// This is for WRITE locks only - works with backup/restore mechanism.
	//
	// Returns:
	//   - succeeded: map of successfully committed keys to their values
	//   - failed: map of keys that failed with error details (lock lost or Redis errors)
	//
	// Usage: After acquiring write locks (with backup) and executing callback, use this
	// to atomically replace the lock placeholders with actual values. If some keys lost
	// their locks (e.g., due to ForceSet), they will be returned in failed map.
	// The caller should use RestoreValues() for rollback if needed.
	CommitWriteLocks(ctx context.Context, ttl time.Duration, lockValues map[string]string, actualValues map[string]string) (succeeded map[string]string, failed map[string]error)

	// AcquireMultiWriteLocksSequential acquires write locks for multiple keys using sequential
	// acquisition with automatic deadlock prevention and rollback.
	//
	// This method implements a sophisticated lock acquisition strategy:
	//  1. Sorts keys for consistent ordering (prevents deadlocks across processes)
	//  2. Attempts batch acquisition of all remaining keys
	//  3. On partial failure:
	//     - Keeps locks acquired sequentially (before first failure)
	//     - Restores values for out-of-order locks (after first failure)
	//     - Waits for the first failed key, then retries
	//  4. Repeats until all locks acquired or context cancelled
	//
	// This prevents deadlocks by ensuring all processes wait for keys in the same order.
	// It prevents cache misses by restoring original values when releasing early.
	//
	// Returns:
	//   - lockValues: map of keys to acquired lock values
	//   - savedValues: map of keys to their previous values (for rollback on error)
	//   - error: if context cancelled or critical Redis error
	AcquireMultiWriteLocksSequential(ctx context.Context, keys []string) (lockValues map[string]string, savedValues map[string]string, err error)
}

// CASWriteLockManager implements WriteLockManager with Compare-And-Swap semantics.
// It uses LockManager for cohesive lock value generation and prefix management.
type CASWriteLockManager struct {
	client      rueidis.Client
	lockTTL     time.Duration
	lockManager lockmanager.LockManager
	logger      Logger
}

// Logger is the logging interface used for write lock operations.
// This is a type alias for the shared logger interface.
type Logger = logger.Logger

// Config holds configuration for creating a CASWriteLockManager.
type Config struct {
	Client      rueidis.Client
	LockTTL     time.Duration
	LockManager lockmanager.LockManager // Required for cohesive lock generation and checking
	Logger      Logger
}

// NewCASWriteLockManager creates a new write lock manager with CAS semantics.
// The LockManager is used to ensure consistent lock value generation and prefix checking
// across both read and write lock operations.
func NewCASWriteLockManager(cfg Config) *CASWriteLockManager {
	return &CASWriteLockManager{
		client:      cfg.Client,
		lockTTL:     cfg.LockTTL,
		lockManager: cfg.LockManager,
		logger:      cfg.Logger,
	}
}

// AcquireWriteLock attempts to acquire a single write lock.
// Returns the lock value on success, or errs.ErrLockFailed if lock is held by another process.
// The caller is responsible for retry logic if needed.
func (wlm *CASWriteLockManager) AcquireWriteLock(ctx context.Context, key string) (string, error) {
	lockVal := wlm.lockManager.GenerateLockValue()

	result := acquireWriteLockScript.Exec(ctx, wlm.client,
		[]string{key},
		[]string{lockVal, strconv.FormatInt(wlm.lockTTL.Milliseconds(), 10), wlm.getLockPrefix()})

	success, err := result.AsInt64()
	if err != nil {
		return "", fmt.Errorf("failed to execute write lock script for key %q: %w", key, err)
	}

	if success != 1 {
		wlm.logger.Debug("write lock contention", "key", key)
		return "", fmt.Errorf("failed to acquire write lock for key %q: %w", key, errs.ErrLockFailed)
	}

	wlm.logger.Debug("write lock acquired", "key", key, "lockVal", lockVal)
	return lockVal, nil
}

// AcquireMultiWriteLocks attempts to acquire write locks for multiple keys in batches.
func (wlm *CASWriteLockManager) AcquireMultiWriteLocks(
	ctx context.Context,
	keys []string,
) (map[string]string, map[string]string, []string, error) {
	if len(keys) == 0 {
		return make(map[string]string), make(map[string]string), nil, nil
	}

	acquired := make(map[string]string)
	savedValues := make(map[string]string)
	failed := make([]string, 0)

	// Group by slot and build Lua exec statements
	stmtsBySlot := wlm.groupLockAcquisitionsBySlot(keys)

	// Execute all slots and collect results
	for _, stmts := range stmtsBySlot {
		// Batch execute all lock acquisitions for this slot
		resps := acquireWriteLockWithBackupScript.ExecMulti(ctx, wlm.client, stmts.execStmts...)

		// Process responses in order
		for i, resp := range resps {
			key := stmts.keyOrder[i]
			lockVal := stmts.lockVals[i]

			result, err := processLockAcquisitionResponse(resp, key, lockVal)
			if err != nil {
				return nil, nil, nil, err
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

// ReleaseWriteLock releases a single write lock if it matches the expected value.
func (wlm *CASWriteLockManager) ReleaseWriteLock(ctx context.Context, key string, lockValue string) error {
	result := unlockKeyScript.Exec(ctx, wlm.client, []string{key}, []string{lockValue})
	if err := result.Error(); err != nil {
		wlm.logger.Debug("failed to release write lock", "key", key, "error", err)
		return err
	}
	return nil
}

// ReleaseWriteLocks releases multiple write locks.
func (wlm *CASWriteLockManager) ReleaseWriteLocks(ctx context.Context, lockValues map[string]string) {
	if len(lockValues) == 0 {
		return
	}

	for key, lockVal := range lockValues {
		result := unlockKeyScript.Exec(ctx, wlm.client, []string{key}, []string{lockVal})
		if err := result.Error(); err != nil {
			wlm.logger.Debug("failed to release write lock", "key", key, "error", err)
		}
	}
}

// RestoreValues restores backed-up values for keys (rollback on partial failure).
func (wlm *CASWriteLockManager) RestoreValues(
	ctx context.Context,
	savedValues map[string]string,
	lockValues map[string]string,
) {
	if len(savedValues) == 0 {
		return
	}

	// Restore values using Lua script (CAS: only if our lock is still there)
	for key, val := range savedValues {
		if lockVal, hasLock := lockValues[key]; hasLock {
			result := setIfLockScript.Exec(ctx, wlm.client, []string{key}, []string{lockVal, val})
			if err := result.Error(); err != nil {
				wlm.logger.Debug("failed to restore value during rollback", "key", key, "error", err)
			}
		}
	}
}

// TouchLocks refreshes TTLs on multiple locks.
func (wlm *CASWriteLockManager) TouchLocks(ctx context.Context, lockValues map[string]string) {
	if len(lockValues) == 0 {
		return
	}

	cmds := make(rueidis.Commands, 0, len(lockValues))
	for key := range lockValues {
		cmds = append(cmds, wlm.client.B().Expire().Key(key).Seconds(int64(wlm.lockTTL.Seconds())).Build())
	}

	resps := wlm.client.DoMulti(ctx, cmds...)
	for _, resp := range resps {
		if err := resp.Error(); err != nil {
			wlm.logger.Debug("failed to touch lock", "error", err)
		}
	}
}

// slotLockStatements holds lock acquisition statements grouped by slot.
type slotLockStatements struct {
	keyOrder  []string
	lockVals  []string
	execStmts []rueidis.LuaExec
}

// groupLockAcquisitionsBySlot groups lock acquisition operations by Redis cluster slot.
func (wlm *CASWriteLockManager) groupLockAcquisitionsBySlot(keys []string) map[uint16]slotLockStatements {
	if len(keys) == 0 {
		return nil
	}

	estimatedSlots, estimatedPerSlot := cmdx.EstimateSlotDistribution(len(keys))
	stmtsBySlot := make(map[uint16]slotLockStatements, estimatedSlots)

	lockTTLStr := strconv.FormatInt(wlm.lockTTL.Milliseconds(), 10)

	for _, key := range keys {
		lockVal := wlm.lockManager.GenerateLockValue()
		slot := cmdx.Slot(key)
		stmts := stmtsBySlot[slot]

		if stmts.keyOrder == nil {
			stmts.keyOrder = make([]string, 0, estimatedPerSlot)
			stmts.lockVals = make([]string, 0, estimatedPerSlot)
			stmts.execStmts = make([]rueidis.LuaExec, 0, estimatedPerSlot)
		}

		stmts.keyOrder = append(stmts.keyOrder, key)
		stmts.lockVals = append(stmts.lockVals, lockVal)
		stmts.execStmts = append(stmts.execStmts, rueidis.LuaExec{
			Keys: []string{key},
			Args: []string{lockVal, lockTTLStr, wlm.getLockPrefix()},
		})

		stmtsBySlot[slot] = stmts
	}

	return stmtsBySlot
}

// lockAcquisitionResult holds the result of a lock acquisition attempt.
type lockAcquisitionResult struct {
	acquired   bool
	lockValue  string
	hasSaved   bool
	savedValue string
}

// processLockAcquisitionResponse processes the response from acquireWriteLockWithBackupScript.
func processLockAcquisitionResponse(resp rueidis.RedisResult, key, lockVal string) (lockAcquisitionResult, error) {
	arr, err := resp.ToArray()
	if err != nil {
		return lockAcquisitionResult{}, fmt.Errorf("failed to parse lock response for key %q: %w", key, err)
	}

	if len(arr) != 2 {
		return lockAcquisitionResult{}, fmt.Errorf("unexpected lock response length for key %q: got %d, want 2", key, len(arr))
	}

	success, err := arr[0].AsInt64()
	if err != nil {
		return lockAcquisitionResult{}, fmt.Errorf("failed to parse lock success for key %q: %w", key, err)
	}

	if success != 1 {
		return lockAcquisitionResult{acquired: false}, nil
	}

	// Successfully acquired - check if we have a saved value
	savedVal, err := arr[1].ToString()
	if err != nil {
		// No saved value (was false in Lua)
		return lockAcquisitionResult{
			acquired:  true,
			lockValue: lockVal,
			hasSaved:  false,
		}, nil
	}

	return lockAcquisitionResult{
		acquired:   true,
		lockValue:  lockVal,
		hasSaved:   true,
		savedValue: savedVal,
	}, nil
}

// CommitWriteLocks atomically replaces write lock values with real values using CAS.
func (wlm *CASWriteLockManager) CommitWriteLocks(
	ctx context.Context,
	ttl time.Duration,
	lockValues map[string]string,
	actualValues map[string]string,
) (map[string]string, map[string]error) {
	if len(lockValues) == 0 {
		return make(map[string]string), make(map[string]error)
	}

	succeeded := make(map[string]string)
	failed := make(map[string]error)

	// Group by slot for Redis Cluster compatibility
	stmtsBySlot := wlm.groupCommitsBySlot(lockValues, actualValues, ttl)

	// Execute each slot's statements
	for slot, stmt := range stmtsBySlot {
		setResps := commitWriteLockScript.ExecMulti(ctx, wlm.client, stmt.execStmts...)

		// Process responses in order
		for i, resp := range setResps {
			key := stmt.keyOrder[i]
			value := actualValues[key]

			// Check for Redis errors
			if err := resp.Error(); err != nil {
				wlm.logger.Debug("commit write lock failed for key", "key", key, "slot", slot, "error", err)
				failed[key] = fmt.Errorf("failed to commit write lock: %w", err)
				continue
			}

			// Check the Lua script return value (0 = lock lost)
			setSuccess, err := resp.AsInt64()
			if err != nil || setSuccess == 0 {
				wlm.logger.Debug("commit write lock CAS failed for key", "key", key, "slot", slot)
				failed[key] = fmt.Errorf("%w", errs.ErrLockLost)
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
				Cmd: wlm.client.B().Get().Key(key).Cache(),
				TTL: ttl,
			})
		}
		_ = wlm.client.DoMultiCache(ctx, cacheCommands...)
	}

	return succeeded, failed
}

// slotCommitStatements holds commit statements grouped by slot.
type slotCommitStatements struct {
	keyOrder  []string
	execStmts []rueidis.LuaExec
}

// groupCommitsBySlot groups commit operations by Redis cluster slot.
func (wlm *CASWriteLockManager) groupCommitsBySlot(
	lockValues map[string]string,
	actualValues map[string]string,
	ttl time.Duration,
) map[uint16]slotCommitStatements {
	estimatedSlots, estimatedPerSlot := cmdx.EstimateSlotDistribution(len(lockValues))
	stmtsBySlot := make(map[uint16]slotCommitStatements, estimatedSlots)

	// Pre-calculate TTL string once
	ttlStr := strconv.FormatInt(ttl.Milliseconds(), 10)

	for key, lockVal := range lockValues {
		actualVal, ok := actualValues[key]
		if !ok {
			// Skip keys without actual values (shouldn't happen, but be defensive)
			wlm.logger.Error("no actual value for key in CommitWriteLocks", "key", key)
			continue
		}

		slot := cmdx.Slot(key)
		stmt := stmtsBySlot[slot]

		// Pre-allocate slices on first access to this slot
		if stmt.keyOrder == nil {
			stmt.keyOrder = make([]string, 0, estimatedPerSlot)
			stmt.execStmts = make([]rueidis.LuaExec, 0, estimatedPerSlot)
		}

		stmt.keyOrder = append(stmt.keyOrder, key)
		stmt.execStmts = append(stmt.execStmts, rueidis.LuaExec{
			Keys: []string{key},
			Args: []string{actualVal, ttlStr, lockVal},
		})
		stmtsBySlot[slot] = stmt
	}

	return stmtsBySlot
}

// Lua scripts for write lock operations.
var (
	acquireWriteLockScript = luascript.New(`
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

	acquireWriteLockWithBackupScript = luascript.New(`
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

		-- Current value is real, acquire and save it for rollback
		redis.call("SET", key, lock_value, "PX", ttl)
		return {1, current}
	`)

	unlockKeyScript = luascript.New(`
		local key = KEYS[1]
		local expected = ARGV[1]
		if redis.call("GET", key) == expected then
			return redis.call("DEL", key)
		else
			return 0
		end
	`)

	setIfLockScript = luascript.New(`
		local key = KEYS[1]
		local expected_lock = ARGV[1]
		local new_value = ARGV[2]
		if redis.call("GET", key) == expected_lock then
			return redis.call("SET", key, new_value)
		else
			return 0
		end
	`)

	commitWriteLockScript = luascript.New(`
		local key = KEYS[1]
		local value = ARGV[1]
		local ttl = ARGV[2]
		local expected_lock = ARGV[3]

		local current = redis.call("GET", key)

		-- STRICT CAS: We can ONLY set if we still hold our exact lock value
		-- This prevents Set from overwriting ForceSet values that stole our lock
		if current == expected_lock then
			redis.call("SET", key, value, "PX", ttl)
			return 1
		else
			return 0
		end
	`)
)

// getLockPrefix returns the lock prefix from the LockManager.
// This ensures WriteLockManager and LockManager use the same prefix for cohesive lock checking.
func (wlm *CASWriteLockManager) getLockPrefix() string {
	return wlm.lockManager.LockPrefix()
}

const (
	// lockRetryInterval is the interval for periodic lock acquisition retries.
	// Used during sequential acquisition when waiting for contested locks.
	lockRetryInterval = 50 * time.Millisecond
)

// AcquireMultiWriteLocksSequential acquires write locks for multiple keys using sequential
// acquisition with automatic deadlock prevention and rollback.
// See WriteLockManager interface documentation for full details.
func (wlm *CASWriteLockManager) AcquireMultiWriteLocksSequential(ctx context.Context, keys []string) (map[string]string, map[string]string, error) {
	if len(keys) == 0 {
		return make(map[string]string), make(map[string]string), nil
	}

	// Sort keys for consistent lock ordering (prevents deadlocks)
	sortedKeys := wlm.sortKeys(keys)
	lockValues := make(map[string]string)
	allSavedValues := make(map[string]string) // Track all saved values for final restoration
	ticker := time.NewTicker(lockRetryInterval)
	defer ticker.Stop()

	remainingKeys := sortedKeys
	for len(remainingKeys) > 0 {
		done, savedValues, err := wlm.tryAcquireBatchAndProcess(ctx, sortedKeys, remainingKeys, lockValues, ticker)
		if err != nil {
			return nil, nil, err
		}
		// Merge saved values from this iteration
		for k, v := range savedValues {
			allSavedValues[k] = v
		}
		if done {
			return lockValues, allSavedValues, nil
		}
		// Update remaining keys for next iteration
		remainingKeys = wlm.keysNotIn(sortedKeys, lockValues)
	}

	return lockValues, allSavedValues, nil
}

// tryAcquireBatchAndProcess attempts to acquire locks for remaining keys and processes the result.
// Returns:
//   - done: true if all locks were acquired
//   - savedValues: map of keys to their previous values from this batch
//   - error: if acquisition failed
func (wlm *CASWriteLockManager) tryAcquireBatchAndProcess(
	ctx context.Context,
	sortedKeys []string,
	remainingKeys []string,
	lockValues map[string]string,
	ticker *time.Ticker,
) (bool, map[string]string, error) {
	// Try to batch-acquire all remaining keys with backup
	acquired, savedValues, failed, err := wlm.AcquireMultiWriteLocks(ctx, remainingKeys)
	if err != nil {
		// Critical error - release all locks we've acquired so far
		wlm.ReleaseWriteLocks(ctx, lockValues)
		return false, nil, err
	}

	// Success: All remaining keys acquired
	if len(failed) == 0 {
		// Add newly acquired locks to our collection
		for k, v := range acquired {
			lockValues[k] = v
		}
		wlm.logger.Debug("AcquireMultiWriteLocksSequential completed", "keys", sortedKeys, "count", len(lockValues))
		return true, savedValues, nil
	}

	// Handle partial failure
	err = wlm.handlePartialLockFailure(ctx, remainingKeys, acquired, savedValues, failed, lockValues, ticker)
	// Return saved values from keys we kept (sequential ones)
	keptSavedValues := make(map[string]string)
	for k := range lockValues {
		if v, ok := savedValues[k]; ok {
			keptSavedValues[k] = v
		}
	}
	return false, keptSavedValues, err
}

// handlePartialLockFailure processes a partial lock acquisition failure.
// It keeps locks acquired in sequential order and restores out-of-order locks.
func (wlm *CASWriteLockManager) handlePartialLockFailure(
	ctx context.Context,
	remainingKeys []string,
	acquired map[string]string,
	savedValues map[string]string,
	failed []string,
	lockValues map[string]string,
	ticker *time.Ticker,
) error {
	wlm.logger.Debug("partial acquisition, analyzing sequential locks",
		"acquired_this_batch", len(acquired),
		"failed", len(failed),
		"total_acquired_so_far", len(lockValues))

	// Find the first failed key in sorted order
	firstFailedKey := wlm.findFirstKey(remainingKeys, failed)

	// Determine which acquired keys to keep vs restore
	toKeep, toRestore := wlm.splitAcquiredBySequence(remainingKeys, acquired, firstFailedKey)

	// Restore keys that were acquired out of order
	if len(toRestore) > 0 {
		wlm.logger.Debug("restoring out-of-order locks",
			"restore_count", len(toRestore),
			"first_failed", firstFailedKey)
		wlm.RestoreValues(ctx, toRestore, savedValues)
	}

	// Add sequential locks to our permanent collection
	for k, v := range toKeep {
		lockValues[k] = v
	}

	// Touch/refresh TTL on all locks we're keeping to prevent expiration
	if len(lockValues) > 0 {
		wlm.TouchLocks(ctx, lockValues)
	}

	// Wait for the first failed key to be released
	err := wlm.lockManager.WaitForKeyWithRetry(ctx, firstFailedKey, ticker)
	if err != nil {
		// Context cancelled or timeout - release all locks
		wlm.ReleaseWriteLocks(ctx, lockValues)
		return err
	}

	return nil
}

// sortKeys creates a sorted copy of the keys to ensure consistent lock ordering.
func (wlm *CASWriteLockManager) sortKeys(keys []string) []string {
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)
	return sorted
}

// findFirstKey finds the first key from sortedKeys that appears in targetKeys.
// This ensures sequential lock acquisition by always waiting for the first failed key.
func (wlm *CASWriteLockManager) findFirstKey(sortedKeys []string, targetKeys []string) string {
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
func (wlm *CASWriteLockManager) splitAcquiredBySequence(
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

// keysNotIn returns keys from sortedKeys that are not in the acquired map.
func (wlm *CASWriteLockManager) keysNotIn(sortedKeys []string, acquired map[string]string) []string {
	remaining := make([]string, 0, len(sortedKeys))
	for _, key := range sortedKeys {
		if _, ok := acquired[key]; !ok {
			remaining = append(remaining, key)
		}
	}
	return remaining
}
