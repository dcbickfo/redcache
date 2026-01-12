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
	"log/slog"
	"strings"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/cachelock"
	"github.com/dcbickfo/redcache/internal/mapsx"
)

// Logger defines the logging interface used by CacheAside.
// Implementations must be safe for concurrent use and should handle log levels internally.
type Logger interface {
	// Error logs error messages. Should be used for unexpected failures or critical issues.
	Error(msg string, args ...any)
	// Debug logs detailed diagnostic information useful for development and troubleshooting.
	// Call Debug to record verbose output about internal state, cache operations, or lock handling.
	// Debug messages should not include sensitive information and may be omitted in production.
	Debug(msg string, args ...any)
}

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
	lockTTL     time.Duration
	logger      Logger
	lockPrefix  string
	maxRetries  int
	lockManager *cachelock.RedisManager
}

// CacheAsideOption configures the behavior of the CacheAside instance.
// All fields are optional with sensible defaults.
type CacheAsideOption struct {
	// LockTTL is the maximum time a lock can be held, and also the timeout for waiting
	// on locks when handling lost Redis invalidation messages. Defaults to 10 seconds.
	// This value should be longer than your typical callback execution time but short
	// enough to recover quickly from process failures.
	LockTTL time.Duration

	// ClientBuilder allows customizing the Redis client creation.
	// If nil, rueidis.NewClient is used with the provided options.
	// This is useful for injecting mock clients in tests or adding middleware.
	ClientBuilder func(option rueidis.ClientOption) (rueidis.Client, error)

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

	rca := &CacheAside{
		lockTTL:    caOption.LockTTL,
		logger:     caOption.Logger,
		lockPrefix: caOption.LockPrefix,
		maxRetries: caOption.MaxRetries,
	}
	clientOption.OnInvalidations = rca.onInvalidate

	var err error
	if caOption.ClientBuilder != nil {
		rca.client, err = caOption.ClientBuilder(clientOption)
	} else {
		rca.client, err = rueidis.NewClient(clientOption)
	}
	if err != nil {
		return nil, err
	}

	// Initialize the lock manager after client is created
	rca.lockManager = cachelock.NewRedisManager(cachelock.RedisManagerConfig{
		Client:     rca.client,
		LockTTL:    caOption.LockTTL,
		LockPrefix: caOption.LockPrefix,
		Logger:     caOption.Logger,
	})

	return rca, nil
}

// Client returns the underlying rueidis.Client for advanced operations.
// Most users should not need direct client access. Use with caution as
// direct operations bypass the cache-aside pattern and distributed locking.
func (rca *CacheAside) Client() rueidis.Client {
	return rca.client
}

// Close cleans up resources used by the CacheAside instance.
// It cancels all pending lock wait operations and cleans up internal state.
// Note: This does NOT close the underlying Redis client, as that's owned by the caller.
func (rca *CacheAside) Close() {
	rca.lockManager.Close()
}

func (rca *CacheAside) onInvalidate(messages []rueidis.RedisMessage) {
	// Delegate to lock manager for proper invalidation handling
	rca.lockManager.OnInvalidate(messages)
}

// register creates or returns an existing wait channel for a key.
// Used by PrimeableCacheAside for CSC invalidation subscription.
func (rca *CacheAside) register(key string) <-chan struct{} {
	return rca.lockManager.Register(key)
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

// getWithRegistration handles the full cache-aside flow with registration
// This is used when we have a cache miss or need to wait for locks.
func (rca *CacheAside) getWithRegistration(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (val string, err error),
) (string, error) {
	for {
		val, done, err := rca.tryAcquireAndProcess(ctx, ttl, key, fn)
		if err != nil {
			return "", err
		}
		if done {
			return val, nil
		}
		// Retry the loop - lock was contended or lost
	}
}

// tryAcquireAndProcess attempts to acquire lock and process a single key.
// Returns (value, done, error) where done indicates if a value was obtained.
func (rca *CacheAside) tryAcquireAndProcess(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (val string, err error),
) (string, bool, error) {
	result, err := rca.lockManager.TryAcquire(ctx, []string{key}, cachelock.LockModeRead)
	if err != nil {
		return "", false, err
	}

	// Cache hit - return immediately
	if val, ok := result.CachedValues[key]; ok {
		return val, true, nil
	}

	// Lock acquired - run callback and commit
	if _, ok := result.Acquired[key]; ok {
		return rca.executeAndCommit(ctx, ttl, key, result.Acquired, fn)
	}

	// Need to wait for lock release
	if len(result.WaitChans) > 0 {
		if waitErr := rca.lockManager.WaitForRelease(ctx, result.WaitChans); waitErr != nil {
			return "", false, waitErr
		}
	}

	return "", false, nil
}

// executeAndCommit runs the callback and commits the result.
// Returns (value, done, error) where done indicates if the operation completed successfully.
func (rca *CacheAside) executeAndCommit(
	ctx context.Context,
	ttl time.Duration,
	key string,
	acquired map[string]string,
	fn func(ctx context.Context, key string) (val string, err error),
) (string, bool, error) {
	val, callbackErr := fn(ctx, key)
	if callbackErr != nil {
		// Release lock on callback error
		rca.lockManager.Release(ctx, acquired)
		return "", false, callbackErr
	}

	// Commit the value
	committed, commitErr := rca.lockManager.Commit(ctx, acquired, map[string]string{key: val}, ttl)
	if commitErr != nil {
		return "", false, commitErr
	}

	// Check if commit succeeded
	for _, k := range committed {
		if k == key {
			return val, true, nil
		}
	}

	// Lock was lost during commit
	rca.logger.Debug("lock lost during commit, retrying", "key", key)
	return "", false, nil
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

var errNotFound = errors.New("not found")

func (rca *CacheAside) tryGet(ctx context.Context, ttl time.Duration, key string) (string, error) {
	resp := rca.client.DoCache(ctx, rca.client.B().Get().Key(key).Cache(), ttl)
	val, err := resp.ToString()
	if rueidis.IsRedisNil(err) || strings.HasPrefix(val, rca.lockPrefix) { // no response or is a lock value
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
	remaining := make([]string, len(keys))
	copy(remaining, keys)

	for len(remaining) > 0 {
		done, err := rca.processMultiKeys(ctx, ttl, remaining, res, fn)
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
		// Update remaining keys for next iteration
		remaining = rca.getRemainingKeys(keys, res)
	}

	return res, nil
}

// getRemainingKeys returns keys not yet in the result map.
func (rca *CacheAside) getRemainingKeys(keys []string, res map[string]string) []string {
	remaining := make([]string, 0, len(keys)-len(res))
	for _, key := range keys {
		if _, ok := res[key]; !ok {
			remaining = append(remaining, key)
		}
	}
	return remaining
}

// processMultiKeys handles a single iteration of the GetMulti loop.
// Returns (done, error) where done indicates all keys are resolved.
func (rca *CacheAside) processMultiKeys(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	res map[string]string,
	fn func(ctx context.Context, key []string) (val map[string]string, err error),
) (bool, error) {
	result, err := rca.lockManager.TryAcquire(ctx, keys, cachelock.LockModeRead)
	if err != nil {
		return false, err
	}

	// Add cached values to result
	for k, v := range result.CachedValues {
		res[k] = v
	}

	// Process acquired locks
	if len(result.Acquired) > 0 {
		if commitErr := rca.executeAndCommitMulti(ctx, ttl, result.Acquired, res, fn); commitErr != nil {
			return false, commitErr
		}
	}

	// Check if all keys are resolved
	if rca.allKeysResolved(keys, res) {
		return true, nil
	}

	// Wait for locks to be released if there are wait channels
	if len(result.WaitChans) > 0 {
		if waitErr := rca.lockManager.WaitForRelease(ctx, result.WaitChans); waitErr != nil {
			return false, waitErr
		}
	}

	return false, nil
}

// allKeysResolved checks if all requested keys have been resolved.
func (rca *CacheAside) allKeysResolved(keys []string, res map[string]string) bool {
	for _, key := range keys {
		if _, ok := res[key]; !ok {
			return false
		}
	}
	return true
}

// executeAndCommitMulti executes callback for acquired locks and commits results.
func (rca *CacheAside) executeAndCommitMulti(
	ctx context.Context,
	ttl time.Duration,
	acquired map[string]string,
	res map[string]string,
	fn func(ctx context.Context, key []string) (val map[string]string, err error),
) error {
	// Get keys we need to fetch
	keysToFetch := mapsx.Keys(acquired)

	// Execute callback
	vals, err := fn(ctx, keysToFetch)
	if err != nil {
		// Release locks on callback error
		rca.lockManager.Release(ctx, acquired)
		return err
	}

	// Commit values
	committed, commitErr := rca.lockManager.Commit(ctx, acquired, vals, ttl)
	if commitErr != nil {
		return commitErr
	}

	// Add successfully committed values to result
	for _, key := range committed {
		if val, ok := vals[key]; ok {
			res[key] = val
		}
	}

	// Release any locks for uncommitted keys
	uncommitted := make(map[string]string)
	for key, lockVal := range acquired {
		found := false
		for _, k := range committed {
			if k == key {
				found = true
				break
			}
		}
		if !found {
			uncommitted[key] = lockVal
		}
	}
	if len(uncommitted) > 0 {
		rca.lockManager.Release(ctx, uncommitted)
	}

	return nil
}
