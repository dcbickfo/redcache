// Package lockmanager provides distributed lock management for cache-aside operations.
// This package extracts lock-related responsibilities from CacheAside to follow the
// Single Responsibility Principle (SOLID).
package lockmanager

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/cmdx"
	"github.com/dcbickfo/redcache/internal/invalidation"
	"github.com/dcbickfo/redcache/internal/lockpool"
	"github.com/dcbickfo/redcache/internal/logger"
	"github.com/dcbickfo/redcache/internal/luascript"
)

// WaitHandle represents a handle for waiting on lock release.
// It encapsulates the invalidation mechanism, hiding implementation details.
type WaitHandle interface {
	// Wait blocks until the lock is released or timeout/context cancellation occurs.
	Wait(ctx context.Context) error
}

// invalidationWaitHandle implements WaitHandle using Redis invalidation notifications.
type invalidationWaitHandle struct {
	waitChan <-chan struct{}
	lockTTL  time.Duration
}

// Wait blocks until the lock is released via invalidation or timeout.
func (h *invalidationWaitHandle) Wait(ctx context.Context) error {
	timer := time.NewTimer(h.lockTTL)
	defer timer.Stop()

	select {
	case <-h.waitChan:
		// Lock released via invalidation
		return nil
	case <-timer.C:
		// Lock TTL expired, safe to retry
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// LockManager defines the interface for distributed lock management.
// Implementations handle lock acquisition, release, and cleanup for cache-aside operations.
type LockManager interface {
	// TryAcquire attempts to acquire a lock optimistically (pre-registration pattern).
	// Pre-registers for invalidations BEFORE trying to acquire to avoid race conditions.
	//
	// Returns:
	//   - lockValue: non-empty if lock was acquired
	//   - retry: non-nil WaitHandle if lock contention occurred (caller should wait and retry)
	//   - err: real error (not lock contention)
	//
	// Usage pattern (CacheAside optimistic retry):
	//   retry:
	//     lockVal, waitHandle, err := lockMgr.TryAcquire(ctx, key)
	//     if lockVal != "" { /* got lock */ }
	//     if waitHandle != nil {
	//       waitHandle.Wait(ctx)
	//       goto retry
	//     }
	TryAcquire(ctx context.Context, key string) (lockValue string, retry WaitHandle, err error)

	// TryAcquireMulti attempts to acquire locks for multiple keys optimistically.
	// Pre-registers for invalidations before attempting acquisition.
	//
	// Returns:
	//   - acquired: map of successfully acquired locks
	//   - retry: map of keys that failed with wait handles for retry
	//   - err: critical error during acquisition
	TryAcquireMulti(ctx context.Context, keys []string) (acquired map[string]string, retry map[string]WaitHandle, err error)

	// AcquireLockBlocking acquires a lock, blocking until successful.
	// Handles all retry logic internally using invalidation notifications.
	//
	// This is the blocking pattern used by write operations that must eventually
	// acquire the lock before proceeding.
	//
	// Returns:
	//   - lockValue: the acquired lock value
	//   - err: context cancellation or critical error
	AcquireLockBlocking(ctx context.Context, key string) (lockValue string, err error)

	// AcquireMultiLocksBlocking acquires locks for multiple keys, blocking until successful.
	// Returns a map of all successfully acquired locks.
	// If context is cancelled, releases any acquired locks before returning.
	AcquireMultiLocksBlocking(ctx context.Context, keys []string) (map[string]string, error)

	// ReleaseLock releases a previously acquired lock.
	// The lock value must match to prevent releasing locks held by others.
	ReleaseLock(ctx context.Context, key, lockValue string) error

	// ReleaseMultiLocks releases multiple locks in a batch operation.
	ReleaseMultiLocks(ctx context.Context, lockValues map[string]string)

	// CleanupUnusedLocks releases locks that weren't used (e.g., partial failures).
	// Takes the full set of acquired locks and the subset that were successfully used.
	CleanupUnusedLocks(ctx context.Context, acquiredLocks, usedKeys map[string]string)

	// GenerateLockValue creates a unique lock identifier.
	// Uses pooling for performance in high-throughput scenarios.
	GenerateLockValue() string

	// CheckKeyLocked checks if a key currently has an active lock.
	// Returns true if the key holds a lock value (not a real cached value).
	CheckKeyLocked(ctx context.Context, key string) bool

	// CheckMultiKeysLocked checks multiple keys for active locks.
	// Returns a slice of keys that currently have locks.
	CheckMultiKeysLocked(ctx context.Context, keys []string) []string

	// IsLockValue checks if the given value is a lock (not a real cached value).
	// This is useful when you already have the value and want to check if it's a lock.
	IsLockValue(val string) bool

	// LockPrefix returns the lock prefix used by this manager.
	// This is used by WriteLockManager to ensure cohesive lock checking.
	LockPrefix() string

	// OnInvalidate processes Redis invalidation messages.
	// This is called by the Redis client when invalidation messages arrive.
	// Delegates to the internal invalidation handler.
	OnInvalidate(messages []rueidis.RedisMessage)

	// WaitForKey returns a WaitHandle for waiting on a key's invalidation.
	// This is used when you need to wait for a lock to be released without acquiring it yourself.
	// The caller should ensure they're subscribed to the key (via DoCache) before waiting.
	WaitForKey(key string) WaitHandle

	// WaitForKeyWithSubscription registers for lock invalidations and subscribes to
	// Redis client-side cache for the key in the correct order to avoid race conditions.
	//
	// This method combines two operations:
	//   1. Register for invalidations (WaitForKey) - MUST happen first
	//   2. Subscribe via DoCache and fetch current value
	//
	// This ordering ensures no invalidation messages are missed between registration
	// and subscription. Returns the wait handle and the current value.
	//
	// Returns:
	//   - waitHandle: handle for waiting on key invalidation
	//   - currentValue: the current value from Redis (may be empty if key doesn't exist)
	//   - err: error from DoCache operation
	WaitForKeyWithSubscription(ctx context.Context, key string, cacheTTL time.Duration) (waitHandle WaitHandle, currentValue string, err error)

	// WaitForKeyWithRetry waits for a key's lock to be released with periodic retry support.
	// This subscribes to the key, waits for invalidation, but also returns on ticker timeout
	// to allow the caller to retry lock acquisition.
	//
	// Returns nil if:
	//   - Lock was released (invalidation received)
	//   - Ticker fired (time to retry)
	//   - Context cancelled
	WaitForKeyWithRetry(ctx context.Context, key string, ticker *time.Ticker) error

	// CommitReadLocks atomically replaces read lock values with real values using CAS.
	// Only succeeds for keys where we still hold the exact lock value.
	// This is for READ locks only - no backup/restore capability needed.
	//
	// Returns:
	//   - succeeded: slice of keys that were successfully committed
	//   - needsRetry: slice of keys that lost their locks (CAS failed)
	//   - err: critical error during execution
	//
	// Usage: After acquiring read locks and executing callback, use this to atomically
	// replace the lock placeholders with actual values. If some keys lost their locks
	// (e.g., due to invalidation or ForceSet), they will be returned in needsRetry.
	CommitReadLocks(ctx context.Context, ttl time.Duration, lockValues map[string]string, actualValues map[string]string) (succeeded []string, needsRetry []string, err error)

	// CreateImmediateWaitHandle creates a WaitHandle that returns immediately without waiting.
	// Used for non-contention errors or when invalidation already occurred.
	// This allows callers to retry immediately rather than waiting for lock release.
	CreateImmediateWaitHandle() WaitHandle
}

// DistributedLockManager implements LockManager using Redis SET NX commands.
// This is a single-instance lock pattern suitable for cache coordination.
//
// Lock Safety Notes:
//   - Suitable for: Cache coordination, preventing thundering herd, efficiency optimizations
//   - NOT suitable for: Financial transactions, inventory management, correctness-critical operations
//   - See DISTRIBUTED_LOCK_SAFETY.md for detailed safety analysis
type DistributedLockManager struct {
	client              rueidis.Client
	lockTTL             time.Duration
	lockPrefix          string
	lockValPool         *lockpool.Pool
	logger              Logger
	invalidationHandler invalidation.Handler
}

// Logger is the logging interface used by LockManager.
// This is a type alias for the shared logger interface.
type Logger = logger.Logger

// Config holds configuration for creating a DistributedLockManager.
type Config struct {
	Client              rueidis.Client
	LockTTL             time.Duration
	LockPrefix          string
	Logger              Logger
	InvalidationHandler invalidation.Handler
}

// NewDistributedLockManager creates a new lock manager with the given configuration.
func NewDistributedLockManager(cfg Config) *DistributedLockManager {
	return &DistributedLockManager{
		client:              cfg.Client,
		lockTTL:             cfg.LockTTL,
		lockPrefix:          cfg.LockPrefix,
		lockValPool:         lockpool.New(cfg.LockPrefix, 10000),
		logger:              cfg.Logger,
		invalidationHandler: cfg.InvalidationHandler,
	}
}

// TryAcquire attempts to acquire a lock optimistically (pre-registration pattern).
// Pre-registers for invalidations BEFORE trying to acquire to avoid race conditions.
//
// Returns:
//   - lockValue: non-empty if lock was acquired
//   - retry: non-nil WaitHandle if lock contention occurred (caller should wait and retry)
//   - err: real error (not lock contention)
//
// Usage pattern (CacheAside optimistic retry):
//
//	retry:
//	  lockVal, waitHandle, err := lockMgr.TryAcquire(ctx, key)
//	  if lockVal != "" { /* got lock */ }
//	  if waitHandle != nil {
//	    waitHandle.Wait(ctx)
//	    goto retry
//	  }
func (dlm *DistributedLockManager) TryAcquire(ctx context.Context, key string) (lockValue string, retry WaitHandle, err error) {
	// Pre-register for invalidations BEFORE lock attempt to avoid race condition
	// where lock is released between our failed attempt and registration
	waitChan := dlm.invalidationHandler.Register(key)

	// Subscribe to key using DoCache to ensure we receive invalidation messages
	_ = dlm.client.DoCache(ctx, dlm.client.B().Get().Key(key).Cache(), dlm.lockTTL)

	// Generate lock value
	lockVal := dlm.GenerateLockValue()

	// Try to acquire lock: SET NX GET PX
	err = dlm.client.Do(
		ctx,
		dlm.client.B().Set().Key(key).Value(lockVal).Nx().Get().Px(dlm.lockTTL).Build(),
	).Error()

	// Success: err == redis.Nil (key didn't exist, lock acquired)
	if rueidis.IsRedisNil(err) {
		dlm.logger.Debug("lock acquired", "key", key, "lockVal", lockVal)
		return lockVal, nil, nil
	}

	// Check if this is a Redis error vs lock contention
	if err != nil {
		dlm.logger.Debug("lock acquisition error", "key", key, "error", err)
		return "", nil, fmt.Errorf("failed to acquire lock for key %q: %w", key, err)
	}

	// Lock contention (err == nil means key exists) - return wait handle
	dlm.logger.Debug("lock contention - failed to acquire lock", "key", key)
	return "", &invalidationWaitHandle{waitChan: waitChan, lockTTL: dlm.lockTTL}, nil
}

// TryAcquireMulti attempts to acquire locks for multiple keys optimistically.
// Pre-registers for invalidations before attempting acquisition.
//
// Returns:
//   - acquired: map of successfully acquired locks
//   - retry: map of keys that failed with wait handles for retry
//   - err: critical error during acquisition
func (dlm *DistributedLockManager) TryAcquireMulti(ctx context.Context, keys []string) (acquired map[string]string, retry map[string]WaitHandle, err error) {
	if len(keys) == 0 {
		return nil, nil, nil
	}

	// Check for context cancellation upfront
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}

	// Pre-register for all keys
	waitChans := dlm.invalidationHandler.RegisterAll(func(yield func(string) bool) {
		for _, k := range keys {
			if !yield(k) {
				return
			}
		}
	}, len(keys))

	// Subscribe to all keys using DoMultiCache
	cmds := make([]rueidis.CacheableTTL, 0, len(keys))
	for _, key := range keys {
		cmds = append(cmds, rueidis.CacheableTTL{
			Cmd: dlm.client.B().Get().Key(key).Cache(),
			TTL: dlm.lockTTL,
		})
	}
	_ = dlm.client.DoMultiCache(ctx, cmds...)

	// Build and execute lock commands
	lockVals, lockCmds := dlm.buildLockCommands(keys)
	resps := dlm.client.DoMulti(ctx, lockCmds...)

	// Process responses
	acquired = make(map[string]string)
	retry = make(map[string]WaitHandle)
	criticalErrors := 0

	for i, r := range resps {
		key := keys[i]
		respErr := r.Error()

		// Success: err == redis.Nil (key didn't exist, SET succeeded)
		if rueidis.IsRedisNil(respErr) {
			acquired[key] = lockVals[key]
			continue
		}

		// Redis error (not just contention)
		if respErr != nil {
			dlm.logger.Debug("lock acquisition error in batch", "key", key, "error", respErr)
			// Count critical errors (connection failures, context cancellation, etc.)
			// but don't fail immediately - try to acquire what we can
			criticalErrors++
			continue
		}

		// Lock contention (err == nil means key exists) - add to retry map
		dlm.logger.Debug("lock contention in batch", "key", key)
		retry[key] = &invalidationWaitHandle{
			waitChan: waitChans[key],
			lockTTL:  dlm.lockTTL,
		}
	}

	// If ALL operations failed with errors, return an error
	if criticalErrors > 0 && len(acquired) == 0 && len(retry) == 0 {
		return nil, nil, fmt.Errorf("failed to acquire any locks: %d/%d keys had errors", criticalErrors, len(keys))
	}

	dlm.logger.Debug("acquired locks in batch", "acquired", len(acquired), "retry", len(retry), "requested", len(keys))
	return acquired, retry, nil
}

// AcquireLockBlocking acquires a lock, blocking until successful.
// Handles all retry logic internally using invalidation notifications.
//
// This is the blocking pattern used by write operations that must eventually
// acquire the lock before proceeding.
//
// Returns:
//   - lockValue: the acquired lock value
//   - err: context cancellation or critical error
func (dlm *DistributedLockManager) AcquireLockBlocking(ctx context.Context, key string) (lockValue string, err error) {
	for {
		// Generate lock value
		lockVal := dlm.GenerateLockValue()

		// Try to acquire lock
		lockErr := dlm.client.Do(
			ctx,
			dlm.client.B().Set().Key(key).Value(lockVal).Nx().Get().Px(dlm.lockTTL).Build(),
		).Error()

		// Success: err == redis.Nil (key didn't exist, lock acquired)
		if rueidis.IsRedisNil(lockErr) {
			dlm.logger.Debug("lock acquired (blocking)", "key", key, "lockVal", lockVal)
			return lockVal, nil
		}

		// Check if this is a Redis error vs lock contention
		if lockErr != nil {
			dlm.logger.Debug("lock acquisition error (blocking)", "key", key, "error", lockErr)
			return "", fmt.Errorf("failed to acquire lock for key %q: %w", key, lockErr)
		}

		// Lock contention - register for invalidations (post-register pattern)
		dlm.logger.Debug("lock contention (blocking) - waiting for release", "key", key)
		waitChan := dlm.invalidationHandler.Register(key)

		// Subscribe to key using DoCache to ensure we receive invalidation messages
		_ = dlm.client.DoCache(ctx, dlm.client.B().Get().Key(key).Cache(), dlm.lockTTL)

		// Wait for invalidation or timeout
		timer := time.NewTimer(dlm.lockTTL)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-waitChan:
			timer.Stop()
			// Lock was released (invalidation event), retry
			continue
		case <-timer.C:
			// Lock TTL expired, safe to retry
			continue
		}
	}
}

// AcquireMultiLocksBlocking acquires locks for multiple keys, blocking until successful.
// Returns a map of all successfully acquired locks.
// If context is cancelled, releases any acquired locks before returning.
func (dlm *DistributedLockManager) AcquireMultiLocksBlocking(ctx context.Context, keys []string) (map[string]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	acquired := make(map[string]string, len(keys))
	remaining := keys

	for len(remaining) > 0 {
		// Check context before each attempt
		if err := dlm.checkContextAndCleanup(ctx, acquired); err != nil {
			return nil, err
		}

		// Pre-register and subscribe to remaining keys
		waitChans := dlm.registerAndSubscribe(ctx, remaining)

		// Try to acquire locks for remaining keys
		newRemaining, needsWait := dlm.tryAcquireBatch(ctx, remaining, acquired)
		remaining = newRemaining

		// If we acquired all locks, we're done
		if len(remaining) == 0 {
			dlm.logger.Debug("acquired all locks (blocking)", "count", len(acquired))
			return acquired, nil
		}

		// Wait for any lock to be released if we had contention
		if needsWait {
			if err := dlm.waitForAnyLockRelease(ctx, remaining, waitChans, acquired); err != nil {
				return nil, err
			}
		}
	}

	dlm.logger.Debug("acquired all locks (blocking)", "count", len(acquired))
	return acquired, nil
}

// checkContextAndCleanup checks if context is cancelled and releases acquired locks if so.
func (dlm *DistributedLockManager) checkContextAndCleanup(ctx context.Context, acquired map[string]string) error {
	if ctx.Err() != nil {
		if len(acquired) > 0 {
			dlm.ReleaseMultiLocks(ctx, acquired)
		}
		return ctx.Err()
	}
	return nil
}

// registerAndSubscribe pre-registers for invalidations and subscribes to all keys.
func (dlm *DistributedLockManager) registerAndSubscribe(ctx context.Context, keys []string) map[string]<-chan struct{} {
	// Pre-register for all remaining keys
	waitChans := dlm.invalidationHandler.RegisterAll(func(yield func(string) bool) {
		for _, k := range keys {
			if !yield(k) {
				return
			}
		}
	}, len(keys))

	// Subscribe to all remaining keys
	cmds := make([]rueidis.CacheableTTL, 0, len(keys))
	for _, key := range keys {
		cmds = append(cmds, rueidis.CacheableTTL{
			Cmd: dlm.client.B().Get().Key(key).Cache(),
			TTL: dlm.lockTTL,
		})
	}
	_ = dlm.client.DoMultiCache(ctx, cmds...)

	return waitChans
}

// tryAcquireBatch attempts to acquire locks for all keys and returns remaining keys and whether to wait.
func (dlm *DistributedLockManager) tryAcquireBatch(ctx context.Context, keys []string, acquired map[string]string) (remaining []string, needsWait bool) {
	// Try to acquire locks for remaining keys
	lockVals, lockCmds := dlm.buildLockCommands(keys)
	resps := dlm.client.DoMulti(ctx, lockCmds...)

	// Process responses
	newRemaining := make([]string, 0)

	for i, r := range resps {
		key := keys[i]
		respErr := r.Error()

		// Success: err == redis.Nil (key didn't exist, SET succeeded)
		if rueidis.IsRedisNil(respErr) {
			acquired[key] = lockVals[key]
			continue
		}

		// Redis error (not just contention)
		if respErr != nil {
			dlm.logger.Debug("lock acquisition error in blocking batch", "key", key, "error", respErr)
			newRemaining = append(newRemaining, key)
			continue
		}

		// Lock contention - need to wait and retry
		needsWait = true
		newRemaining = append(newRemaining, key)
	}

	return newRemaining, needsWait
}

// waitForAnyLockRelease waits for at least one lock to be released or timeout.
func (dlm *DistributedLockManager) waitForAnyLockRelease(
	ctx context.Context,
	remaining []string,
	waitChans map[string]<-chan struct{},
	acquired map[string]string,
) error {
	dlm.logger.Debug("waiting for locks (blocking)", "remaining", len(remaining))

	// Wait for the first lock to be released
	timer := time.NewTimer(dlm.lockTTL)
	defer timer.Stop()

	waitCases := make([]<-chan struct{}, 0, len(remaining)+1)
	waitCases = append(waitCases, ctx.Done())
	for _, key := range remaining {
		if ch, ok := waitChans[key]; ok {
			waitCases = append(waitCases, ch)
		}
	}

	// Wait for first signal
	select {
	case <-ctx.Done():
		if len(acquired) > 0 {
			dlm.ReleaseMultiLocks(ctx, acquired)
		}
		return ctx.Err()
	case <-timer.C:
		// Timeout, retry all remaining
		return nil
	default:
		// Check all wait channels
		if dlm.checkAnyChannelReady(waitCases[1:]) {
			return nil
		}
		// No releases yet, wait for timer
		<-timer.C
		return nil
	}
}

// checkAnyChannelReady checks if any of the channels are ready (non-blocking).
func (dlm *DistributedLockManager) checkAnyChannelReady(channels []<-chan struct{}) bool {
	for _, ch := range channels {
		select {
		case <-ch:
			return true
		default:
		}
	}
	return false
}

// ReleaseLock releases a single lock using a Lua script to ensure atomicity.
func (dlm *DistributedLockManager) ReleaseLock(ctx context.Context, key, lockValue string) error {
	return delKeyLua.Exec(ctx, dlm.client, []string{key}, []string{lockValue}).Error()
}

// ReleaseMultiLocks releases multiple locks in a batch operation.
func (dlm *DistributedLockManager) ReleaseMultiLocks(ctx context.Context, lockValues map[string]string) {
	if len(lockValues) == 0 {
		return
	}

	for key, lockVal := range lockValues {
		// Fire-and-forget: Don't wait for responses
		// Locks will expire via TTL if deletion fails
		if err := dlm.ReleaseLock(ctx, key, lockVal); err != nil {
			dlm.logger.Debug("failed to release lock", "key", key, "error", err)
		}
	}
}

// CleanupUnusedLocks releases locks that were acquired but not used.
// This handles partial failure scenarios where some operations succeed and others fail.
func (dlm *DistributedLockManager) CleanupUnusedLocks(
	ctx context.Context,
	acquiredLocks map[string]string,
	usedKeys map[string]string,
) {
	toUnlock := make(map[string]string)

	for key, lockVal := range acquiredLocks {
		if _, used := usedKeys[key]; !used {
			toUnlock[key] = lockVal
		}
	}

	if len(toUnlock) > 0 {
		dlm.ReleaseMultiLocks(ctx, toUnlock)
	}
}

// GenerateLockValue creates a unique lock identifier using a pool for performance.
func (dlm *DistributedLockManager) GenerateLockValue() string {
	// Use pool for better performance (~15% improvement in lock acquisition)
	return dlm.lockValPool.Get()
}

// CheckKeyLocked checks if a key currently has an active lock.
// Uses DoCache to subscribe to invalidations (consistent with CheckMultiKeysLocked).
func (dlm *DistributedLockManager) CheckKeyLocked(ctx context.Context, key string) bool {
	resp := dlm.client.DoCache(ctx, dlm.client.B().Get().Key(key).Cache(), dlm.lockTTL)
	val, err := resp.ToString()
	if err != nil {
		return false
	}
	return dlm.IsLockValue(val)
}

// CheckMultiKeysLocked checks multiple keys for active locks.
// Returns keys that currently have locks.
// Uses DoMultiCache to ensure invalidation subscriptions for locked keys.
func (dlm *DistributedLockManager) CheckMultiKeysLocked(ctx context.Context, keys []string) []string {
	if len(keys) == 0 {
		return nil
	}

	// Build cacheable commands to check for locks
	// Use lockTTL to ensure subscription lasts long enough to receive invalidations
	cmds := make([]rueidis.CacheableTTL, 0, len(keys))
	for _, key := range keys {
		cmds = append(cmds, rueidis.CacheableTTL{
			Cmd: dlm.client.B().Get().Key(key).Cache(),
			TTL: dlm.lockTTL,
		})
	}

	lockedKeys := make([]string, 0)
	resps := dlm.client.DoMultiCache(ctx, cmds...)
	for i, resp := range resps {
		val, err := resp.ToString()
		if err == nil && dlm.IsLockValue(val) {
			lockedKeys = append(lockedKeys, keys[i])
		}
	}

	return lockedKeys
}

// IsLockValue checks if the given value is a lock (has the lock prefix).
func (dlm *DistributedLockManager) IsLockValue(val string) bool {
	return strings.HasPrefix(val, dlm.lockPrefix)
}

// LockPrefix returns the lock prefix used by this manager.
// This is used by WriteLockManager to ensure cohesive lock checking.
func (dlm *DistributedLockManager) LockPrefix() string {
	return dlm.lockPrefix
}

// OnInvalidate processes Redis invalidation messages.
// Delegates to the internal invalidation handler.
func (dlm *DistributedLockManager) OnInvalidate(messages []rueidis.RedisMessage) {
	dlm.invalidationHandler.OnInvalidate(messages)
}

// WaitForKey returns a WaitHandle for waiting on a key's invalidation.
// This is used when you need to wait for a lock to be released without acquiring it yourself.
func (dlm *DistributedLockManager) WaitForKey(key string) WaitHandle {
	waitChan := dlm.invalidationHandler.Register(key)
	return &invalidationWaitHandle{
		waitChan: waitChan,
		lockTTL:  dlm.lockTTL,
	}
}

// WaitForKeyWithSubscription implements LockManager.WaitForKeyWithSubscription.
// Registers for invalidations first, then subscribes via DoCache and fetches the current value.
func (dlm *DistributedLockManager) WaitForKeyWithSubscription(
	ctx context.Context,
	key string,
	cacheTTL time.Duration,
) (WaitHandle, string, error) {
	// STEP 1: Register for invalidations FIRST (before DoCache subscription)
	// This ensures we won't miss any invalidation that arrives between DoCache and registration
	waitHandle := dlm.WaitForKey(key)

	// STEP 2: Subscribe to Redis client-side cache AND fetch current value
	// Since we already registered, we won't miss any invalidation messages
	resp := dlm.client.DoCache(ctx, dlm.client.B().Get().Key(key).Cache(), cacheTTL)
	val, err := resp.ToString()

	return waitHandle, val, err
}

// WaitForKeyWithRetry waits for a key's lock to be released with periodic retry support.
// Subscribes to the key via DoCache, then waits for either invalidation or ticker timeout.
func (dlm *DistributedLockManager) WaitForKeyWithRetry(ctx context.Context, key string, ticker *time.Ticker) error {
	// Get wait handle and subscribe to invalidations
	waitHandle := dlm.WaitForKey(key)
	_ = dlm.CheckKeyLocked(ctx, key) // Ensures subscription to invalidations

	// Wait with ticker support for periodic retries
	waitChan := make(chan struct{})
	go func() {
		_ = waitHandle.Wait(ctx)
		close(waitChan)
	}()

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

// WaitForAny waits for any of the provided WaitHandles to complete.
// Returns when the first handle completes or context is cancelled.
func WaitForAny(ctx context.Context, handles []WaitHandle) error {
	if len(handles) == 0 {
		return nil
	}

	// Create a channel to signal completion
	done := make(chan struct{})
	defer close(done)

	// Start goroutines for each handle
	for _, h := range handles {
		go func(handle WaitHandle) {
			_ = handle.Wait(ctx)
			select {
			case done <- struct{}{}:
			case <-ctx.Done():
			}
		}(h)
	}

	// Wait for first completion or context cancellation
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitForAll waits for all provided WaitHandles to complete concurrently.
// Returns when all handles complete or context is cancelled.
// Handles are waited on concurrently to support cases where multiple locks are released
// simultaneously via a single invalidation event.
func WaitForAll(ctx context.Context, handles []WaitHandle) error {
	if len(handles) == 0 {
		return nil
	}

	// Channel to collect completion signals
	done := make(chan error, len(handles))

	// Launch goroutine for each handle
	for _, h := range handles {
		go func(handle WaitHandle) {
			done <- handle.Wait(ctx)
		}(h)
	}

	// Wait for all handles to complete or context cancellation
	for i := 0; i < len(handles); i++ {
		select {
		case err := <-done:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// buildLockCommands generates lock values and builds SET NX GET commands for the given keys.
func (dlm *DistributedLockManager) buildLockCommands(keys []string) (map[string]string, rueidis.Commands) {
	lockVals := make(map[string]string, len(keys))
	cmds := make(rueidis.Commands, 0, len(keys))

	for _, k := range keys {
		lockVal := dlm.GenerateLockValue()
		lockVals[k] = lockVal
		// SET NX GET returns the old value if key exists, or nil if SET succeeded
		cmds = append(cmds, dlm.client.B().Set().Key(k).Value(lockVal).Nx().Get().Px(dlm.lockTTL).Build())
	}

	return lockVals, cmds
}

// LockChecker checks if a key has an active lock.
type LockChecker interface {
	// HasLock checks if the given value is a lock (not a real cached value).
	HasLock(val string) bool
	// CheckKeyLocked checks if a key currently has a lock in Redis.
	CheckKeyLocked(ctx context.Context, client rueidis.Client, key string) bool
}

// PrefixLockChecker checks locks based on a prefix.
type PrefixLockChecker struct {
	Prefix string
}

// HasLock checks if the value has the lock prefix.
func (p *PrefixLockChecker) HasLock(val string) bool {
	return strings.HasPrefix(val, p.Prefix)
}

// CheckKeyLocked checks if a key has an active lock.
func (p *PrefixLockChecker) CheckKeyLocked(ctx context.Context, client rueidis.Client, key string) bool {
	resp := client.Do(ctx, client.B().Get().Key(key).Build())
	val, err := resp.ToString()
	if err != nil {
		return false
	}
	return p.HasLock(val)
}

// BatchCheckLocks checks multiple keys for locks and returns those with locks.
// Uses DoMultiCache to ensure we're subscribed to invalidations for locked keys.
func BatchCheckLocks(ctx context.Context, client rueidis.Client, keys []string, checker LockChecker) []string {
	if len(keys) == 0 {
		return nil
	}

	// Build cacheable commands to check for locks
	cmds := make([]rueidis.CacheableTTL, 0, len(keys))
	for _, key := range keys {
		cmds = append(cmds, rueidis.CacheableTTL{
			Cmd: client.B().Get().Key(key).Cache(),
			TTL: time.Second, // Short TTL for lock checks
		})
	}

	lockedKeys := make([]string, 0)
	resps := client.DoMultiCache(ctx, cmds...)
	for i, resp := range resps {
		val, err := resp.ToString()
		if err == nil && checker.HasLock(val) {
			lockedKeys = append(lockedKeys, keys[i])
		}
	}

	return lockedKeys
}

// CommitReadLocks atomically replaces read lock values with real values using CAS.
func (dlm *DistributedLockManager) CommitReadLocks(
	ctx context.Context,
	ttl time.Duration,
	lockValues map[string]string,
	actualValues map[string]string,
) ([]string, []string, error) {
	if len(lockValues) == 0 {
		return nil, nil, nil
	}

	// Group by slot for Redis Cluster compatibility
	stmtsBySlot := dlm.groupCommitsBySlot(lockValues, actualValues, ttl)

	// Execute grouped statements and collect results
	succeeded, needsRetry, err := dlm.executeCommitStatements(ctx, stmtsBySlot)
	if err != nil {
		return nil, nil, err
	}

	return succeeded, needsRetry, nil
}

// CreateImmediateWaitHandle creates a WaitHandle that returns immediately.
func (dlm *DistributedLockManager) CreateImmediateWaitHandle() WaitHandle {
	return &immediateWaitHandle{}
}

// slotCommitStatements holds commit statements grouped by slot.
type slotCommitStatements struct {
	keyOrder  []string
	execStmts []rueidis.LuaExec
}

// groupCommitsBySlot groups commit operations by Redis cluster slot.
func (dlm *DistributedLockManager) groupCommitsBySlot(
	lockValues map[string]string,
	actualValues map[string]string,
	ttl time.Duration,
) map[uint16]slotCommitStatements {
	estimatedSlots := len(lockValues) / 8
	if estimatedSlots < 1 {
		estimatedSlots = 1
	}
	stmts := make(map[uint16]slotCommitStatements, estimatedSlots)

	// Pre-calculate TTL string once
	ttlStr := strconv.FormatInt(ttl.Milliseconds(), 10)

	for key, lockVal := range lockValues {
		actualVal, ok := actualValues[key]
		if !ok {
			// Skip keys without actual values (shouldn't happen, but be defensive)
			dlm.logger.Error("no actual value for key in CommitReadLocks", "key", key)
			continue
		}

		slot := cmdx.Slot(key)
		stmt := stmts[slot]

		// Pre-allocate slices on first access to this slot
		if stmt.keyOrder == nil {
			estimatedKeysPerSlot := (len(lockValues) / estimatedSlots) + 1
			stmt.keyOrder = make([]string, 0, estimatedKeysPerSlot)
			stmt.execStmts = make([]rueidis.LuaExec, 0, estimatedKeysPerSlot)
		}

		stmt.keyOrder = append(stmt.keyOrder, key)
		stmt.execStmts = append(stmt.execStmts, rueidis.LuaExec{
			Keys: []string{key},
			Args: []string{lockVal, actualVal, ttlStr},
		})
		stmts[slot] = stmt
	}

	return stmts
}

// executeCommitStatements executes commit statements and returns succeeded/failed keys.
func (dlm *DistributedLockManager) executeCommitStatements(
	ctx context.Context,
	stmtsBySlot map[uint16]slotCommitStatements,
) ([]string, []string, error) {
	succeeded := make([]string, 0)
	needsRetry := make([]string, 0)

	// Execute each slot's statements
	for _, stmt := range stmtsBySlot {
		setResps := commitReadLockScript.ExecMulti(ctx, dlm.client, stmt.execStmts...)

		// Process responses in order
		for i, resp := range setResps {
			key := stmt.keyOrder[i]

			// Check for Redis errors
			if err := resp.Error(); err != nil && !rueidis.IsRedisNil(err) {
				return nil, nil, fmt.Errorf("failed to commit lock for key %q: %w", key, err)
			}

			// Check the Lua script return value (0 = lock lost)
			returnValue, err := resp.AsInt64()
			if err != nil || returnValue == 0 {
				// Lock was lost for this key
				needsRetry = append(needsRetry, key)
				continue
			}

			succeeded = append(succeeded, key)
		}
	}

	return succeeded, needsRetry, nil
}

// immediateWaitHandle is a WaitHandle that returns immediately without waiting.
// Used for non-contention errors or when invalidation already occurred.
type immediateWaitHandle struct{}

// Wait returns immediately without blocking.
func (i *immediateWaitHandle) Wait(_ context.Context) error {
	return nil
}

// Lua script for committing read locks (replacing lock with actual value).
var commitReadLockScript = luascript.New(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3])
    return 1
else
    return 0
end
`)

// Lua script for atomic lock release (only delete if value matches).
var delKeyLua = luascript.New(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
`)
