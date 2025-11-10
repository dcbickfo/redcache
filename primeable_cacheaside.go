package redcache

import (
	"context"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/redis/rueidis"
	"golang.org/x/sync/errgroup"

	"github.com/dcbickfo/redcache/internal/cmdx"
	"github.com/dcbickfo/redcache/internal/syncx"
)

// PrimeableCacheAside extends CacheAside with explicit Set operations for cache priming
// and write-through caching. Unlike the base CacheAside which only populates cache on
// misses, PrimeableCacheAside allows proactive cache updates and warming.
//
// It inherits all capabilities from CacheAside:
//   - Get/GetMulti for cache-aside pattern with automatic population
//   - Del/DelMulti for cache invalidation
//   - Distributed locking and retry mechanisms
//
// And adds write-through operations:
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
type PrimeableCacheAside struct {
	*CacheAside
}

// NewPrimeableCacheAside creates a new PrimeableCacheAside instance with the specified
// Redis client options and cache-aside configuration.
//
// This function creates a base CacheAside instance and wraps it with write-through
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
//	defer pca.Client().Close()
func NewPrimeableCacheAside(clientOption rueidis.ClientOption, caOption CacheAsideOption) (*PrimeableCacheAside, error) {
	ca, err := NewRedCacheAside(clientOption, caOption)
	if err != nil {
		return nil, err
	}
	return &PrimeableCacheAside{CacheAside: ca}, nil
}

// Set performs a write-through cache operation with distributed locking.
// Unlike Get which only fills empty cache slots, Set can overwrite existing values
// while ensuring coordination across distributed processes.
//
// The operation flow:
//  1. Register for local coordination within this process
//  2. Acquire a distributed lock in Redis
//  3. Execute the provided function (e.g., database write)
//  4. If successful, cache the returned value with the specified TTL
//  5. Release the lock (happens automatically even on failure)
//
// The method automatically retries when:
//   - Another process holds the lock (waits for completion)
//   - Redis invalidation is received (indicating concurrent modification)
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - ttl: Time-to-live for the cached value
//   - key: The cache key to set
//   - fn: Function to generate the value (typically performs a database write)
//
// Returns an error if:
//   - The callback function returns an error
//   - Lock acquisition fails after retries
//   - Context is cancelled or deadline exceeded
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
retry:
	// Register for local coordination
	wait := pca.register(key)

	// Try to acquire Redis lock and execute function using the base CacheAside method
	_, err := pca.trySetKeyFunc(ctx, ttl, key, fn)
	if err != nil {
		if errors.Is(err, errLockFailed) {
			// Failed to get Redis lock, wait for invalidation or timeout
			// The invalidation will cancel our context and close the channel
			select {
			case <-wait:
				// Either local operation completed or invalidation received
				goto retry
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return err
	}

	return nil
}

// SetMulti performs write-through cache operations for multiple keys with distributed locking.
// Each individual key's write is atomic (DB and cache will have the same value),
// but the batch as a whole is not atomic - keys may be processed in multiple subsets across retries.
//
// The operation flow:
//  1. Register local locks for all requested keys
//  2. Attempt to acquire distributed locks in Redis for those keys
//  3. Execute the callback ONLY with keys that were successfully locked
//  4. Cache the returned values and release the locks
//  5. Retry for any keys that couldn't be locked initially
//
// The callback may be invoked multiple times with different key subsets as locks
// become available. Each invocation should be idempotent and handle partial batches.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - ttl: Time-to-live for cached values
//   - keys: List of all keys to process
//   - fn: Callback that receives locked keys and returns their values
//
// Returns a map of all successfully processed keys to their cached values.
//
// Example with database batch write:
//
//	userIDs := []string{"user:1", "user:2", "user:3"}
//	result, err := pca.SetMulti(ctx, 10*time.Minute, userIDs,
//	    func(ctx context.Context, lockedKeys []string) (map[string]string, error) {
//	        // This might be called with ["user:1", "user:3"] if user:2 is locked
//	        users := make(map[string]string)
//	        for _, key := range lockedKeys {
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

	// Accumulate all successfully set values across retries
	allVals := make(map[string]string, len(keys))

	waitLock := make(map[string]<-chan struct{}, len(keys))
	for _, key := range keys {
		waitLock[key] = nil
	}

retry:
	waitLock = pca.registerAll(maps.Keys(waitLock), len(waitLock))

	// Try to set all keys using the callback - using base CacheAside method
	vals, err := pca.trySetMultiKeyFn(ctx, ttl, slices.Collect(maps.Keys(waitLock)), fn)
	if err != nil {
		return nil, err
	}

	// Add successfully set keys to accumulated result and remove from wait list
	for key, val := range vals {
		allVals[key] = val
		delete(waitLock, key)
	}

	// If there are still keys that failed due to lock contention, wait for invalidation
	if len(waitLock) > 0 {
		// Wait for ALL channels to signal - this allows us to potentially
		// acquire all remaining locks in one retry, reducing round trips
		err = syncx.WaitForAll(ctx, maps.Values(waitLock), len(waitLock))
		if err != nil {
			return nil, err
		}
		// All locks have been released, retry
		goto retry
	}

	return allVals, nil
}

// ForceSet unconditionally sets a value in the cache, bypassing all distributed locks.
// This operation immediately overwrites any existing value or lock without coordination.
//
// WARNING: This method can cause race conditions and should be used sparingly.
// It will:
//   - Override any existing value, even if locked
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
	return pca.client.Do(ctx, pca.client.B().Set().Key(key).Value(value).Px(ttl).Build()).Error()
}

// ForceSetMulti unconditionally sets multiple values in the cache, bypassing all locks.
// This operation immediately overwrites any existing values or locks without coordination.
//
// WARNING: This method can cause race conditions and should be used sparingly.
// It will:
//   - Override all specified keys, even if locked
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
	if len(values) == 0 {
		return nil
	}

	// Group by slot for efficient parallel execution in Redis cluster
	cmdsBySlot := make(map[uint16]rueidis.Commands)

	for k, v := range values {
		slot := cmdx.Slot(k)
		cmd := pca.client.B().Set().Key(k).Value(v).Px(ttl).Build()
		cmdsBySlot[slot] = append(cmdsBySlot[slot], cmd)
	}

	// Execute commands in parallel, one goroutine per slot
	eg, ctx := errgroup.WithContext(ctx)

	for _, cmds := range cmdsBySlot {
		cmds := cmds // Capture for goroutine
		eg.Go(func() error {
			resps := pca.client.DoMulti(ctx, cmds...)
			for _, resp := range resps {
				if respErr := resp.Error(); respErr != nil {
					return respErr
				}
			}
			return nil
		})
	}

	return eg.Wait()
}
