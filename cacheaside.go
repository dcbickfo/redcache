// Package redcache provides a cache-aside implementation for Redis with distributed locking.
//
// This library builds on the rueidis Redis client to provide:
//   - Cache-aside pattern with automatic cache population
//   - Distributed locking to prevent thundering herd
//   - Client-side caching to reduce Redis round trips
//   - Redis cluster support with automatic slot routing
//   - Automatic cleanup of expired lock entries
//
// # Basic Usage
//
//	client, err := redcache.NewRedCacheAside(
//	    rueidis.ClientOption{InitAddress: []string{"localhost:6379"}},
//	    redcache.CacheAsideOption{LockTTL: 10 * time.Second},
//	)
//	if err != nil {
//	    return err
//	}
//	defer client.Client().Close()
//
//	// Get a single value with automatic cache population
//	value, err := client.Get(ctx, time.Minute, "user:123", func(ctx context.Context, key string) (string, error) {
//	    return fetchFromDatabase(ctx, key)
//	})
//
//	// Get multiple values with batched cache population
//	values, err := client.GetMulti(ctx, time.Minute, []string{"user:1", "user:2"}, func(ctx context.Context, keys []string) (map[string]string, error) {
//	    return fetchMultipleFromDatabase(ctx, keys)
//	})
//
// # Distributed Locking
//
// The library ensures that only one goroutine (across all instances of your application)
// executes the callback function for a given key at a time. Other goroutines will wait
// for the lock to be released and then return the cached value.
//
// Locks are implemented using Redis SET NX with a configurable TTL. Lock values use
// UUIDv7 for uniqueness and are prefixed (default: "__redcache:lock:") to avoid
// collisions with application data.
//
// # Context and Timeouts
//
// All operations respect context cancellation. The LockTTL option controls:
//   - Maximum time a lock can be held before automatic expiration
//   - Timeout for waiting on locks when handling invalidation messages
//   - Context timeout for cleanup operations
//
// Use context deadlines to control overall operation timeout:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	value, err := client.Get(ctx, time.Minute, key, callback)
//
// # Client-Side Caching
//
// The library uses rueidis client-side caching with Redis invalidation messages.
// When a key is modified in Redis, invalidation messages automatically clear the
// local cache, ensuring consistency across distributed instances.
package redcache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/contextx"
	"github.com/dcbickfo/redcache/internal/invalidation"
	"github.com/dcbickfo/redcache/internal/lockmanager"
	"github.com/dcbickfo/redcache/internal/logger"
	"github.com/dcbickfo/redcache/internal/mapsx"
)

// Logger is the logging interface used by CacheAside.
// This is a type alias for the shared logger interface.
type Logger = logger.Logger

// CacheAside implements the cache-aside pattern with distributed locking for Redis.
// It coordinates concurrent access to prevent cache stampedes and ensures only one
// process populates the cache for a given key at a time across distributed systems.
//
// Key features:
//   - Distributed locking prevents thundering herd on cache misses
//   - Client-side caching with Redis invalidation for consistency
//   - Automatic retry on lock contention with configurable timeouts
//   - Batch operations with automatic cluster slot routing via rueidis
//   - Context-aware cleanup ensures locks are released even on errors
type CacheAside struct {
	client      rueidis.Client
	lockManager lockmanager.LockManager
	lockTTL     time.Duration
	logger      Logger
	maxRetries  int
}

// CacheAsideOption configures the behavior of the CacheAside instance.
// All fields are optional with sensible defaults.
type CacheAsideOption struct {
	// LockTTL is the maximum time a lock can be held, and also the timeout for waiting
	// on locks when handling lost Redis invalidation messages. Defaults to 10 seconds.
	// This value should be longer than your typical callback execution time but short
	// enough to recover quickly from process failures.
	LockTTL time.Duration

	// Client allows injecting a pre-configured rueidis.Client for testing or custom implementations.
	// If provided, this takes precedence and ClientBuilder is ignored.
	// This is the primary mechanism for dependency injection in tests.
	// Since rueidis.Client is an interface, you can easily mock it for testing.
	// Example:
	//   caOption := CacheAsideOption{Client: mockRedisClient}
	Client rueidis.Client

	// ClientBuilder allows customizing the Redis client creation for advanced use cases.
	// If nil and Client is nil, rueidis.NewClient is used with the provided options.
	ClientBuilder func(option rueidis.ClientOption) (rueidis.Client, error)

	// LockManager allows injecting a custom lock manager for testing or alternative implementations.
	// If provided, this is used instead of creating a DistributedLockManager.
	// This enables mocking lock behavior in unit tests without Redis.
	// Example:
	//   caOption := CacheAsideOption{LockManager: mockLockManager}
	LockManager lockmanager.LockManager

	// Logger for logging errors and debug information. Defaults to slog.Default().
	// The logger should handle log levels internally (e.g., only log Debug if level is enabled).
	Logger Logger

	// LockPrefix for distributed locks. Defaults to "__redcache:lock:".
	// Choose a prefix unlikely to conflict with your data keys.
	// The prefix helps identify locks in Redis and prevents accidental data corruption.
	LockPrefix string

	// MaxRetries is the maximum number of retry attempts for SetMulti operations.
	// When acquiring locks for multiple keys, SetMulti may need to retry if locks
	// are held by other operations. This limit prevents indefinite retry loops.
	// Defaults to 100. Set to 0 for unlimited retries (relies only on context timeout).
	// Recommended: Set this to (context_timeout / LockTTL) * 2 for safety.
	MaxRetries int
}

// NewRedCacheAside creates a new CacheAside instance with the specified Redis client options
// and cache-aside configuration.
//
// The function validates all options and sets appropriate defaults:
//   - LockTTL defaults to 10 seconds if not specified
//   - Logger defaults to slog.Default() if not provided
//   - LockPrefix defaults to "__redcache:lock:" if empty
//
// Returns an error if:
//   - No Redis addresses are provided in InitAddress
//   - LockTTL is negative or less than 100ms
//   - Redis client creation fails
//
// Example:
//
//	ca, err := NewRedCacheAside(
//	    rueidis.ClientOption{InitAddress: []string{"localhost:6379"}},
//	    CacheAsideOption{LockTTL: 5 * time.Second},
//	)
//	if err != nil {
//	    return err
//	}
//	defer ca.Client().Close()
func NewRedCacheAside(clientOption rueidis.ClientOption, caOption CacheAsideOption) (*CacheAside, error) {
	// Validate client options
	if len(clientOption.InitAddress) == 0 {
		return nil, errors.New("at least one Redis address must be provided in InitAddress")
	}

	// Validate and set defaults for cache aside options
	if caOption.LockTTL < 0 {
		return nil, errors.New("LockTTL must not be negative")
	}
	if caOption.LockTTL > 0 && caOption.LockTTL < 100*time.Millisecond {
		return nil, errors.New("LockTTL should be at least 100ms to avoid excessive lock churn")
	}
	if caOption.LockTTL == 0 {
		caOption.LockTTL = 10 * time.Second
	}
	if caOption.Logger == nil {
		caOption.Logger = slog.Default()
	}
	if caOption.LockPrefix == "" {
		caOption.LockPrefix = "__redcache:lock:"
	}
	if caOption.MaxRetries == 0 {
		caOption.MaxRetries = 100
	}

	// Create temporary struct for initialization (needed for callbacks)
	rca := &CacheAside{
		lockTTL:    caOption.LockTTL,
		logger:     caOption.Logger,
		maxRetries: caOption.MaxRetries,
	}

	// Create invalidation handler (will be owned by lock manager)
	invalidHandler := invalidation.NewRedisInvalidationHandler(invalidation.Config{
		LockTTL: caOption.LockTTL,
		Logger:  caOption.Logger,
	})

	// Initialize Redis client with invalidation support
	// Pass invalidHandler directly to avoid race condition during initialization
	client, err := initializeClient(clientOption, caOption, rca, invalidHandler)
	if err != nil {
		return nil, err
	}
	rca.client = client

	// Initialize lock manager (injected or default) - pass invalidation handler
	rca.lockManager = initializeLockManager(caOption, rca, invalidHandler)

	return rca, nil
}

// initializeClient sets up the Redis client with invalidation callbacks.
func initializeClient(clientOption rueidis.ClientOption, caOption CacheAsideOption, _ *CacheAside, invalidHandler invalidation.Handler) (rueidis.Client, error) {
	// Priority: 1. Injected Client, 2. ClientBuilder, 3. Default rueidis.NewClient
	if caOption.Client != nil {
		return caOption.Client, nil
	}

	// Setup invalidation callback - delegates directly to handler to avoid race condition
	// (lockManager hasn't been assigned yet during initialization)
	clientOption.OnInvalidations = func(messages []rueidis.RedisMessage) {
		invalidHandler.OnInvalidate(messages)
	}

	if caOption.ClientBuilder != nil {
		return caOption.ClientBuilder(clientOption)
	}
	return rueidis.NewClient(clientOption)
}

// initializeLockManager creates or uses injected lock manager.
func initializeLockManager(caOption CacheAsideOption, rca *CacheAside, invalidHandler invalidation.Handler) lockmanager.LockManager {
	if caOption.LockManager != nil {
		return caOption.LockManager
	}
	return lockmanager.NewDistributedLockManager(lockmanager.Config{
		Client:              rca.client,
		LockTTL:             caOption.LockTTL,
		LockPrefix:          caOption.LockPrefix,
		Logger:              caOption.Logger,
		InvalidationHandler: invalidHandler,
	})
}

// Client returns the underlying rueidis.Client for advanced operations.
// Most users should not need direct client access. Use with caution as
// direct operations bypass the cache-aside pattern and distributed locking.
func (rca *CacheAside) Client() rueidis.Client {
	return rca.client
}

// Close cleans up resources used by the CacheAside instance.
// Note: Invalidation handler cleanup is automatic via context.AfterFunc.
// Note: This does NOT close the underlying Redis client, as that's owned by the caller.
func (rca *CacheAside) Close() {
	// Invalidation handler uses context.AfterFunc for automatic cleanup
	// No explicit cleanup needed
}

// Get retrieves a value from cache or computes it using the provided callback function.
// It implements the cache-aside pattern with distributed locking to prevent cache stampedes.
//
// The operation flow:
//  1. Check if the value exists in cache (including client-side cache)
//  2. If found and not a lock value, return it immediately
//  3. If not found or is a lock, try to acquire a distributed lock
//  4. If lock acquired, execute the callback to compute the value
//  5. Store the computed value in cache with the specified TTL
//  6. If lock not acquired, wait for the lock to be released and retry
//
// The method automatically retries when:
//   - Another process holds the lock (waits for completion)
//   - Redis invalidation is received (indicating the key was updated)
//   - A lock is lost during the set operation (e.g., overridden by ForceSet)
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - ttl: Time-to-live for the cached value
//   - key: The cache key to get or set
//   - fn: Callback function to compute the value if not in cache
//
// Returns:
//   - The cached or computed value
//   - An error if the operation fails or context is cancelled
//
// Example:
//
//	value, err := ca.Get(ctx, 5*time.Minute, "user:123", func(ctx context.Context, key string) (string, error) {
//	    // This function is only called if the value is not in cache
//	    // and this process successfully acquires the lock
//	    return database.GetUser(ctx, "123")
//	})
func (rca *CacheAside) Get(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (val string, err error),
) (string, error) {
	// Fast path: try to get from cache without registration
	// This avoids context creation for cache hits
	val, err := rca.tryGet(ctx, ttl, key)
	if err == nil && val != "" {
		// Cache hit - return immediately
		return val, nil
	}
	if err != nil && !errors.Is(err, errNotFound) {
		// Actual error (not just a cache miss)
		return "", err
	}

	// Slow path: cache miss or lock found, need full registration flow
	return rca.getWithRegistration(ctx, ttl, key, fn)
}

// getWithRegistration handles the full cache-aside flow with lock acquisition.
// This is used when we have a cache miss or need to wait for locks.
// Uses the new TryAcquire API which handles pre-registration internally.
func (rca *CacheAside) getWithRegistration(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (val string, err error),
) (string, error) {
retry:
	// Check if value was populated while we were waiting
	val, err := rca.tryGet(ctx, ttl, key)
	if err == nil && val != "" {
		return val, nil
	}
	if err != nil && !errors.Is(err, errNotFound) {
		return "", err
	}

	// Try to acquire lock (pre-registers for invalidations internally)
	lockVal, waitHandle, err := rca.lockManager.TryAcquire(ctx, key)
	if err != nil {
		return "", err
	}

	// If we got a wait handle, lock contention occurred - wait and retry
	if waitHandle != nil {
		if waitErr := waitHandle.Wait(ctx); waitErr != nil {
			return "", waitErr
		}
		goto retry
	}

	// We acquired the lock - populate the cache
	val, err = rca.populateAndCache(ctx, ttl, key, lockVal, fn)
	if err != nil {
		// Check if lock was lost (overridden by ForceSet)
		if errors.Is(err, ErrLockLost) {
			// Lock was lost, value may have been updated by another process
			// Retry immediately - the retry will check cache first
			goto retry
		}
		return "", err
	}
	return val, nil
}

// populateAndCache executes the callback and caches the result, handling lock cleanup.
// Returns ErrLockLost if the lock was overridden during the operation.
func (rca *CacheAside) populateAndCache(
	ctx context.Context,
	ttl time.Duration,
	key string,
	lockVal string,
	fn func(ctx context.Context, key string) (string, error),
) (string, error) {
	setVal := false
	defer func() {
		if !setVal {
			cleanupCtx, cancel := contextx.WithCleanupTimeout(ctx, rca.lockTTL)
			defer cancel()
			if unlockErr := rca.lockManager.ReleaseLock(cleanupCtx, key, lockVal); unlockErr != nil {
				rca.logger.Error("failed to unlock key", "key", key, "error", unlockErr)
			}
		}
	}()

	// Execute callback to get the value
	val, err := fn(ctx, key)
	if err != nil {
		return "", err
	}

	// Store value in cache
	val, err = rca.setWithLock(ctx, ttl, key, lockVal, val)
	if err == nil {
		setVal = true
		return val, nil
	}

	// Return error (caller will check for ErrLockLost and retry if needed)
	return "", err
}

// Del removes a key from Redis cache.
// This triggers invalidation messages to clear client-side caches across all connected instances.
//
// Parameters:
//   - ctx: Context for cancellation control
//   - key: The cache key to delete
//
// Returns an error if the deletion fails. Returns nil even if the key doesn't exist.
func (rca *CacheAside) Del(ctx context.Context, key string) error {
	return rca.client.Do(ctx, rca.client.B().Del().Key(key).Build()).Error()
}

// DelMulti removes multiple keys from Redis cache in a single batch operation.
// This triggers invalidation messages for all deleted keys to maintain cache consistency.
//
// The operation is optimized for Redis clusters by grouping keys by slot and
// executing deletions in parallel for better performance.
//
// Parameters:
//   - ctx: Context for cancellation control
//   - keys: Variable number of cache keys to delete
//
// Returns an error if any deletion fails. The operation is not atomic - some keys
// may be deleted even if others fail. Returns nil if all deletions succeed or if
// no keys are provided.
func (rca *CacheAside) DelMulti(ctx context.Context, keys ...string) error {
	cmds := make(rueidis.Commands, 0, len(keys))
	for _, key := range keys {
		cmds = append(cmds, rca.client.B().Del().Key(key).Build())
	}
	resps := rca.client.DoMulti(ctx, cmds...)
	for _, resp := range resps {
		if err := resp.Error(); err != nil {
			return err
		}
	}
	return nil
}

var (
	errNotFound = errors.New("not found")
)

// ErrLockLost is now defined in errors.go for consistency across the package.

func (rca *CacheAside) tryGet(ctx context.Context, ttl time.Duration, key string) (string, error) {
	resp := rca.client.DoCache(ctx, rca.client.B().Get().Key(key).Cache(), ttl)
	val, err := resp.ToString()
	if rueidis.IsRedisNil(err) || rca.lockManager.IsLockValue(val) { // no response or is a lock value
		if rueidis.IsRedisNil(err) {
			rca.logger.Debug("cache miss - key not found", "key", key)
		} else {
			rca.logger.Debug("cache miss - lock value found", "key", key)
		}
		return "", errNotFound
	}
	if err != nil {
		return "", err
	}
	rca.logger.Debug("cache hit", "key", key)
	return val, nil
}

func (rca *CacheAside) setWithLock(ctx context.Context, ttl time.Duration, key string, lockVal string, val string) (string, error) {
	// Commit read lock: atomically replace lock value with actual value
	succeeded, needsRetry, err := rca.lockManager.CommitReadLocks(ctx, ttl,
		map[string]string{key: lockVal},
		map[string]string{key: val})

	if err != nil {
		return "", fmt.Errorf("failed to commit lock for key %q: %w", key, err)
	}

	// Check if lock was lost
	if len(needsRetry) > 0 {
		rca.logger.Debug("lock lost during set operation", "key", key)
		return "", fmt.Errorf("lock lost for key %q: %w", key, ErrLockLost)
	}

	// Verify success
	if len(succeeded) == 0 {
		return "", fmt.Errorf("failed to set value for key %q", key)
	}

	rca.logger.Debug("value set successfully", "key", key)
	return val, nil
}

// GetMulti retrieves multiple values from cache or computes them using the provided callback.
// It extends the cache-aside pattern to handle batch operations efficiently with distributed locking.
//
// The operation flow:
//  1. Check which values exist in cache (including client-side cache)
//  2. For cache hits, return them immediately
//  3. For cache misses, attempt to acquire distributed locks
//  4. Execute the callback only for keys where locks were acquired
//  5. Store the computed values in cache with the specified TTL
//  6. For keys where locks couldn't be acquired, wait and retry
//
// The callback may be called multiple times with different subsets of keys as locks
// become available. This allows for partial progress even when some keys are locked.
//
// Performance optimizations:
//   - Batch Redis operations for efficiency
//   - Slot-aware grouping for Redis clusters
//   - Parallel execution where possible
//   - Client-side caching to minimize round trips
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - ttl: Time-to-live for cached values
//   - keys: List of cache keys to retrieve
//   - fn: Callback to compute values for keys not in cache
//
// The callback receives only the keys for which locks were successfully acquired.
// It should return a map containing values for those keys.
//
// Returns:
//   - A map of all requested keys to their values
//   - An error if the operation fails or context is cancelled
//
// Example:
//
//	values, err := ca.GetMulti(ctx, 5*time.Minute, []string{"user:1", "user:2", "user:3"},
//	    func(ctx context.Context, keys []string) (map[string]string, error) {
//	        // This may be called with a subset like ["user:2"] if only that key
//	        // needs to be fetched from the database
//	        return database.GetUsers(ctx, keys)
//	    })
func (rca *CacheAside) GetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, key []string) (val map[string]string, err error),
) (map[string]string, error) {
	res := make(map[string]string, len(keys))

	// Track which keys still need processing (use empty struct as marker)
	waitKeys := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		waitKeys[key] = struct{}{}
	}

retry:
	// No upfront registration needed - TryAcquireMulti handles it internally
	vals, err := rca.tryGetMulti(ctx, ttl, mapsx.Keys(waitKeys))
	if err != nil && !rueidis.IsRedisNil(err) {
		return nil, err
	}

	for k, v := range vals {
		res[k] = v
		delete(waitKeys, k)
	}

	if len(waitKeys) > 0 {
		var shouldRetry bool
		shouldRetry, err = rca.processRemainingKeys(ctx, ttl, waitKeys, res, fn)
		if err != nil {
			return nil, err
		}
		if shouldRetry {
			goto retry
		}
	}

	return res, nil
}

// processRemainingKeys handles the logic for keys that weren't in cache.
// Returns true if we should retry the operation, or an error if something went wrong.
func (rca *CacheAside) processRemainingKeys(
	ctx context.Context,
	ttl time.Duration,
	waitKeys map[string]struct{},
	res map[string]string,
	fn func(ctx context.Context, key []string) (val map[string]string, err error),
) (bool, error) {
	waitHandles, handleErr := rca.handleMissingKeys(ctx, ttl, waitKeys, res, fn)
	if handleErr != nil {
		// Check if locks expired (don't retry in this case)
		if errors.Is(handleErr, ErrLockLost) {
			return false, fmt.Errorf("locks expired during GetMulti callback: %w", handleErr)
		}
		return false, handleErr
	}

	if len(waitHandles) > 0 {
		// Wait for all handles to complete
		handles := mapsx.Values(waitHandles)
		err := lockmanager.WaitForAll(ctx, handles)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

// handleMissingKeys attempts to acquire locks and populate missing keys.
// Returns WaitHandles for keys that need retry.
func (rca *CacheAside) handleMissingKeys(
	ctx context.Context,
	ttl time.Duration,
	waitKeys map[string]struct{},
	res map[string]string,
	fn func(ctx context.Context, key []string) (val map[string]string, err error),
) (map[string]lockmanager.WaitHandle, error) {
	acquiredVals, waitHandles, err := rca.tryAcquireAndExecute(ctx, ttl, mapsx.Keys(waitKeys), fn)
	if err != nil {
		return nil, err
	}

	// Merge acquired values into result and remove from waitKeys
	for k, v := range acquiredVals {
		res[k] = v
		delete(waitKeys, k)
	}

	// Remove successfully acquired keys from wait handles as well
	for k := range acquiredVals {
		delete(waitHandles, k)
	}

	return waitHandles, nil
}

// tryAcquireAndExecute attempts to acquire locks and execute the callback for missing keys.
// Returns the values retrieved and WaitHandles for keys that need retry.
// It uses an optimistic approach: if not all locks can be acquired, it releases them
// and returns wait handles for the caller to wait on.
func (rca *CacheAside) tryAcquireAndExecute(
	ctx context.Context,
	ttl time.Duration,
	keysNeeded []string,
	fn func(ctx context.Context, key []string) (val map[string]string, err error),
) (map[string]string, map[string]lockmanager.WaitHandle, error) {
	// Try to acquire locks (pre-registers internally)
	acquired, retryHandles, err := rca.lockManager.TryAcquireMulti(ctx, keysNeeded)
	if err != nil {
		return nil, nil, err
	}

	// If we got all locks, execute callback
	if len(acquired) == len(keysNeeded) && len(retryHandles) == 0 {
		vals, execErr := rca.executeAndCacheMulti(ctx, ttl, keysNeeded, acquired, fn)
		if execErr != nil {
			return nil, nil, execErr
		}

		// Check if some keys lost their locks during execution
		if waitHandles := rca.buildWaitHandlesForLostLocks(keysNeeded, vals); waitHandles != nil {
			return vals, waitHandles, nil
		}

		return vals, nil, nil
	}

	// Didn't get all locks - release what we got and return retry handles
	if len(acquired) > 0 {
		cleanupCtx, cancel := contextx.WithCleanupTimeout(ctx, rca.lockTTL)
		defer cancel()
		rca.lockManager.ReleaseMultiLocks(cleanupCtx, acquired)
	}

	// For keys that had errors, add immediate-return handles
	rca.addImmediateHandlesForErrors(keysNeeded, retryHandles)

	return nil, retryHandles, nil
}

// buildWaitHandlesForLostLocks creates wait handles for keys that lost their locks during execution.
// Returns nil if all keys were successful.
func (rca *CacheAside) buildWaitHandlesForLostLocks(
	keysNeeded []string,
	vals map[string]string,
) map[string]lockmanager.WaitHandle {
	// All keys succeeded - no wait handles needed
	if len(vals) >= len(keysNeeded) {
		return nil
	}

	// Some keys lost their locks (e.g., due to invalidation/deletion)
	// These keys need immediate retry since the invalidation already happened
	waitHandles := make(map[string]lockmanager.WaitHandle, len(keysNeeded))
	immediateHandle := rca.lockManager.CreateImmediateWaitHandle()

	for _, key := range keysNeeded {
		if _, ok := vals[key]; !ok {
			// This key lost its lock - retry immediately (invalidation already happened)
			waitHandles[key] = immediateHandle
		}
	}

	return waitHandles
}

// addImmediateHandlesForErrors adds immediate-return wait handles for keys that had errors.
// Modifies retryHandles in-place.
func (rca *CacheAside) addImmediateHandlesForErrors(
	keysNeeded []string,
	retryHandles map[string]lockmanager.WaitHandle,
) {
	immediateHandle := rca.lockManager.CreateImmediateWaitHandle()

	for _, key := range keysNeeded {
		if _, hasHandle := retryHandles[key]; !hasHandle {
			// Key had error - add immediate handle so we retry right away
			retryHandles[key] = immediateHandle
		}
	}
}

func (rca *CacheAside) tryGetMulti(ctx context.Context, ttl time.Duration, keys []string) (map[string]string, error) {
	multi := make([]rueidis.CacheableTTL, len(keys))
	for i, key := range keys {
		cmd := rca.client.B().Get().Key(key).Cache()
		multi[i] = rueidis.CacheableTTL{
			Cmd: cmd,
			TTL: ttl,
		}
	}
	resps := rca.client.DoMultiCache(ctx, multi...)

	res := make(map[string]string, len(keys))
	for i, resp := range resps {
		val, err := resp.ToString()
		if err != nil && rueidis.IsRedisNil(err) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("failed to get key %q: %w", keys[i], err)
		}
		if !rca.lockManager.IsLockValue(val) {
			res[keys[i]] = val
			continue
		}
	}
	return res, nil
}

// executeAndCacheMulti executes the callback with all locked keys and caches the results.
func (rca *CacheAside) executeAndCacheMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	lockVals map[string]string,
	fn func(ctx context.Context, key []string) (val map[string]string, err error),
) (map[string]string, error) {
	res := make(map[string]string, len(keys))

	// Defer cleanup of locks that weren't successfully set
	defer func() {
		rca.lockManager.CleanupUnusedLocks(ctx, lockVals, res)
	}()

	// Execute callback
	vals, err := fn(ctx, keys)
	if err != nil {
		return nil, err
	}

	// Commit read locks: atomically replace lock values with actual values
	succeeded, _, err := rca.lockManager.CommitReadLocks(ctx, ttl, lockVals, vals)
	if err != nil {
		return nil, err
	}

	// Build result map from succeeded keys
	for _, key := range succeeded {
		res[key] = vals[key]
	}

	return res, nil
}
