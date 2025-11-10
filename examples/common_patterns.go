//go:build examples

package examples

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
	"github.com/dcbickfo/redcache/examples/exampleutil"
)

// Example_CacheAsidePattern demonstrates the classic cache-aside pattern
// where cache is populated on miss. This pattern is ideal for read-heavy workloads.
func Example_CacheAsidePattern() {
	// Initialize cache with reasonable defaults
	ca, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: []string{"localhost:6379"}},
		redcache.CacheAsideOption{
			LockTTL: 5 * time.Second,
			// ClientOption can include more settings like timeout, retries, etc.
		},
	)
	if err != nil {
		fmt.Printf("Failed to create cache: %v\n", err)
		return
	}
	defer ca.Client().Close()

	db := exampleutil.NewMockDatabase()

	// Create a context with timeout to demonstrate proper context usage
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get a user - will fetch from database on cache miss
	userJSON, err := ca.Get(ctx, 5*time.Minute, "user:1", func(ctx context.Context, key string) (string, error) {
		// This callback is only executed on cache miss
		// Extract ID from key
		userID := key[5:] // Remove "user:" prefix

		// Fetch from database with context (respects cancellation)
		user, err := db.GetUser(ctx, userID)
		if err != nil {
			return "", fmt.Errorf("database error: %w", err)
		}

		// Serialize for caching
		return exampleutil.SerializeUser(user)
	})

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Parse and use the cached user data
	user, err := exampleutil.DeserializeUser(userJSON)
	if err != nil {
		fmt.Printf("Deserialization error: %v\n", err)
		return
	}

	fmt.Printf("Got user: %s <%s>\n", user.Name, user.Email)
	// Output:
	// Got user: Alice Smith <alice@example.com>
}

// Example_WriteThroughPattern demonstrates write-through caching where
// database writes are immediately reflected in cache. This ensures cache
// consistency after updates.
func Example_WriteThroughPattern() {
	pca, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: []string{"localhost:6379"}},
		redcache.CacheAsideOption{LockTTL: 5 * time.Second},
	)
	if err != nil {
		fmt.Printf("Failed to create cache: %v\n", err)
		return
	}
	defer pca.Client().Close()

	db := exampleutil.NewMockDatabase()

	// Context with deadline demonstrates timeout handling
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Prepare updated user data
	updatedUser := &exampleutil.User{
		ID:    "1",
		Name:  "Alice Smith-Johnson",
		Email: "alice.updated@example.com",
	}

	// Update user with write-through caching
	err = pca.Set(ctx, 5*time.Minute, fmt.Sprintf("user:%s", updatedUser.ID),
		func(ctx context.Context, key string) (string, error) {
			// Write to database first (respecting context)
			if err := db.UpdateUser(ctx, updatedUser); err != nil {
				// If database update fails, cache won't be updated
				return "", fmt.Errorf("failed to update database: %w", err)
			}

			// Return serialized value for cache
			return exampleutil.SerializeUser(updatedUser)
		})

	if err != nil {
		fmt.Printf("Write-through failed: %v\n", err)
		return
	}

	fmt.Println("User updated successfully")
	// Output:
	// User updated successfully
}

// Example_BatchOperations demonstrates efficient batch cache operations
// which minimize round trips to both cache and database.
func Example_BatchOperations() {
	ca, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: []string{"localhost:6379"}},
		redcache.CacheAsideOption{LockTTL: 5 * time.Second},
	)
	if err != nil {
		fmt.Printf("Failed to create cache: %v\n", err)
		return
	}
	defer ca.Client().Close()

	db := exampleutil.NewMockDatabase()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Batch get with automatic cache population
	userKeys := []string{"user:1", "user:2", "user:3"}

	results, err := ca.GetMulti(ctx, 5*time.Minute, userKeys,
		func(ctx context.Context, keys []string) (map[string]string, error) {
			// This is called only for keys not in cache
			// Extract IDs from keys
			ids := make([]string, len(keys))
			for i, key := range keys {
				ids[i] = key[5:] // Remove "user:" prefix
			}

			// Batch fetch from database (single query instead of N queries)
			users, err := db.GetUsers(ctx, ids)
			if err != nil {
				return nil, fmt.Errorf("database batch fetch failed: %w", err)
			}

			// Serialize results
			result := make(map[string]string, len(users))
			for id, user := range users {
				key := fmt.Sprintf("user:%s", id)
				serialized, err := exampleutil.SerializeUser(user)
				if err != nil {
					return nil, fmt.Errorf("serialization failed for user %s: %w", id, err)
				}
				result[key] = serialized
			}

			return result, nil
		})

	if err != nil {
		fmt.Printf("Batch get failed: %v\n", err)
		return
	}

	fmt.Printf("Retrieved %d users\n", len(results))
	// Output:
	// Retrieved 3 users
}

// Example_CacheWarming demonstrates how to proactively warm the cache
// during application startup to reduce initial latency.
func Example_CacheWarming() {
	pca, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: []string{"localhost:6379"}},
		redcache.CacheAsideOption{LockTTL: 5 * time.Second},
	)
	if err != nil {
		fmt.Printf("Failed to create cache: %v\n", err)
		return
	}
	defer pca.Client().Close()

	db := exampleutil.NewMockDatabase()

	// Use a longer timeout for startup operations
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Load frequently accessed users for cache warming
	userIDs := []string{"1", "2", "3"}

	// Fetch from database
	users, err := db.GetUsers(ctx, userIDs)
	if err != nil {
		fmt.Printf("Failed to load users: %v\n", err)
		return
	}

	// Prepare cache keys and serialize
	keys := make([]string, 0, len(users))
	serializedUsers := make(map[string]string)

	for id, user := range users {
		key := fmt.Sprintf("user:%s", id)
		keys = append(keys, key)

		serialized, err := exampleutil.SerializeUser(user)
		if err != nil {
			fmt.Printf("Failed to serialize user %s: %v\n", id, err)
			continue
		}
		serializedUsers[key] = serialized
	}

	// Warm the cache with pre-computed values
	cached, err := pca.SetMulti(ctx, 1*time.Hour, keys,
		func(ctx context.Context, lockedKeys []string) (map[string]string, error) {
			// Check context before processing
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			// Return values for successfully locked keys
			result := make(map[string]string, len(lockedKeys))
			for _, key := range lockedKeys {
				result[key] = serializedUsers[key]
			}
			return result, nil
		})

	if err != nil {
		fmt.Printf("Cache warming failed: %v\n", err)
		return
	}

	fmt.Printf("Successfully warmed cache with %d users\n", len(cached))
	// Output:
	// Successfully warmed cache with 3 users
}

// Example_ContextCancellation demonstrates how the cache respects context
// cancellation, preventing unnecessary work when requests are abandoned.
func Example_ContextCancellation() {
	ca, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: []string{"localhost:6379"}},
		redcache.CacheAsideOption{LockTTL: 5 * time.Second},
	)
	if err != nil {
		fmt.Printf("Failed to create cache: %v\n", err)
		return
	}
	defer ca.Client().Close()

	db := exampleutil.NewMockDatabase()

	// Create a context that will be cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Try to get user - should fail due to context timeout
	_, err = ca.Get(ctx, 5*time.Minute, "user:1", func(ctx context.Context, key string) (string, error) {
		// This might not even execute if context is already cancelled
		user, err := db.GetUser(ctx, "1")
		if err != nil {
			return "", err
		}
		return exampleutil.SerializeUser(user)
	})

	if err != nil {
		if err == context.DeadlineExceeded {
			fmt.Println("Operation cancelled due to timeout")
		} else {
			fmt.Printf("Operation failed: %v\n", err)
		}
	}
	// Output:
	// Operation cancelled due to timeout
}

// Example_SetPrecomputedValue demonstrates how to cache pre-computed values
// using the Set method with a simple callback pattern.
func Example_SetPrecomputedValue() {
	pca, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: []string{"localhost:6379"}},
		redcache.CacheAsideOption{LockTTL: 5 * time.Second},
	)
	if err != nil {
		fmt.Printf("Failed to create cache: %v\n", err)
		return
	}
	defer pca.Client().Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Pre-computed value (could be from any source)
	precomputedValue := `{"id":"99","name":"Computed User","email":"computed@example.com"}`

	// Cache the pre-computed value
	err = pca.Set(ctx, 10*time.Minute, "user:99", func(ctx context.Context, key string) (string, error) {
		// Simply return the pre-computed value
		// Still uses locking to coordinate with other operations
		return precomputedValue, nil
	})

	if err != nil {
		fmt.Printf("Failed to set value: %v\n", err)
		return
	}

	fmt.Println("Pre-computed value cached successfully")
	// Output:
	// Pre-computed value cached successfully
}

// Example_ForceSetEmergency demonstrates using ForceSet for emergency
// cache corrections. Use with extreme caution as it bypasses all locks!
func Example_ForceSetEmergency() {
	pca, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: []string{"localhost:6379"}},
		redcache.CacheAsideOption{LockTTL: 5 * time.Second},
	)
	if err != nil {
		fmt.Printf("Failed to create cache: %v\n", err)
		return
	}
	defer pca.Client().Close()

	ctx := context.Background()

	// Emergency correction data
	correctedData := `{"id":"1","name":"Emergency Fix","email":"fixed@example.com"}`

	// ForceSet bypasses all locks - use only for emergency corrections
	err = pca.ForceSet(ctx, 5*time.Minute, "user:1", correctedData)

	if err != nil {
		fmt.Printf("Force set failed: %v\n", err)
		return
	}

	fmt.Println("CAUTION: Cache forcefully updated")
	// Output:
	// CAUTION: Cache forcefully updated
}
