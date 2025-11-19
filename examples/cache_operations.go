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

// Example_ProductInventoryUpdate demonstrates a realistic use case:
// updating product inventory with write-through caching (callback handles database update)
// to ensure cache consistency after stock changes.
func Example_ProductInventoryUpdate() {
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

	// Context with timeout for the operation
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	productID := "prod-1"

	// Update product stock (e.g., after a sale)
	err = pca.Set(ctx, 10*time.Minute, fmt.Sprintf("product:%s", productID),
		func(ctx context.Context, _ string) (string, error) {
			// Update database first
			if stockErr := db.UpdateProductStock(ctx, productID, -1); stockErr != nil {
				return "", fmt.Errorf("failed to update stock: %w", stockErr)
			}

			// Fetch updated product to cache
			product, productErr := db.GetProduct(ctx, productID)
			if productErr != nil {
				return "", fmt.Errorf("failed to fetch updated product: %w", productErr)
			}

			return exampleutil.SerializeProduct(product)
		})

	if err != nil {
		fmt.Printf("Failed to update inventory: %v\n", err)
		return
	}

	fmt.Println("Product inventory updated")
	// Output:
	// Product inventory updated
}

// Example_CacheInvalidation demonstrates how to invalidate cache entries
// when data becomes stale or needs to be refreshed.
func Example_CacheInvalidation() {
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

	// Delete a single key
	err = pca.Del(ctx, "user:1")
	if err != nil {
		fmt.Printf("Failed to delete key: %v\n", err)
		return
	}

	// Delete multiple keys at once
	err = pca.DelMulti(ctx, "user:2", "user:3", "product:prod-1")
	if err != nil {
		fmt.Printf("Failed to delete keys: %v\n", err)
		return
	}

	fmt.Println("Cache entries invalidated")
	// Output:
	// Cache entries invalidated
}

// Example_ReadThroughCache demonstrates the read-through pattern where
// the cache automatically populates itself on misses.
func Example_ReadThroughCache() {
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

	// First call - cache miss, loads from database
	productJSON, err := ca.Get(ctx, 15*time.Minute, "product:prod-2",
		func(ctx context.Context, key string) (string, error) {
			productID := key[8:] // Remove "product:" prefix

			product, dbErr := db.GetProduct(ctx, productID)
			if dbErr != nil {
				return "", fmt.Errorf("database error: %w", dbErr)
			}

			return exampleutil.SerializeProduct(product)
		})

	if err != nil {
		fmt.Printf("Failed to get product: %v\n", err)
		return
	}

	product, _ := exampleutil.DeserializeProduct(productJSON)
	fmt.Printf("Product: %s - $%.2f\n", product.Name, product.Price)
	// Output:
	// Product: Mouse - $29.99
}

// Example_ConditionalCaching demonstrates caching only when certain
// conditions are met (e.g., only cache expensive computations).
func Example_ConditionalCaching() {
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

	userJSON, err := ca.Get(ctx, 5*time.Minute, "user:1",
		func(ctx context.Context, key string) (string, error) {
			userID := key[5:]

			user, dbErr := db.GetUser(ctx, userID)
			if dbErr != nil {
				// Don't cache errors - let them be retried
				return "", fmt.Errorf("database error: %w", dbErr)
			}

			// You could add conditional logic here:
			// - Only cache if user is active
			// - Only cache if data meets certain criteria
			// - Apply different TTLs based on user properties

			return exampleutil.SerializeUser(user)
		})

	if err != nil {
		fmt.Printf("Failed to get user: %v\n", err)
		return
	}

	user, _ := exampleutil.DeserializeUser(userJSON)
	fmt.Printf("User: %s\n", user.Name)
	// Output:
	// User: Alice Smith
}

// Example_BulkCachePopulation demonstrates efficiently populating cache
// with multiple items, useful for migration or cache rebuild scenarios.
func Example_BulkCachePopulation() {
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

	// Use a longer timeout for bulk operations
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	// Get all users to populate
	allUserIDs := []string{"1", "2", "3"}
	users, err := db.GetUsers(ctx, allUserIDs)
	if err != nil {
		fmt.Printf("Failed to fetch users: %v\n", err)
		return
	}

	// Prepare serialized data
	values := make(map[string]string)
	keys := make([]string, 0, len(users))

	for id, user := range users {
		key := fmt.Sprintf("user:%s", id)
		keys = append(keys, key)

		serialized, err := exampleutil.SerializeUser(user)
		if err != nil {
			fmt.Printf("Skipping user %s: serialization error\n", id)
			continue
		}
		values[key] = serialized
	}

	// Bulk populate cache
	result, err := pca.SetMulti(ctx, 30*time.Minute, keys,
		func(ctx context.Context, lockedKeys []string) (map[string]string, error) {
			// Return values for locked keys
			ret := make(map[string]string, len(lockedKeys))
			for _, key := range lockedKeys {
				ret[key] = values[key]
			}
			return ret, nil
		})

	if err != nil {
		fmt.Printf("Bulk population failed: %v\n", err)
		return
	}

	fmt.Printf("Populated cache with %d entries\n", len(result))
	// Output:
	// Populated cache with 3 entries
}

// Example_ErrorHandling demonstrates proper error handling patterns
// with context awareness and appropriate error wrapping.
func Example_ErrorHandling() {
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

	// Short timeout to demonstrate timeout handling
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = ca.Get(ctx, 5*time.Minute, "user:999",
		func(ctx context.Context, key string) (string, error) {
			// Check for context errors before expensive operations
			if ctx.Err() != nil {
				return "", ctx.Err()
			}

			userID := key[5:]
			user, dbErr := db.GetUser(ctx, userID)
			if dbErr != nil {
				// Wrap errors with context
				return "", fmt.Errorf("failed to fetch user %s: %w", userID, dbErr)
			}

			return exampleutil.SerializeUser(user)
		})

	if err != nil {
		// Handle different error types appropriately
		if ctx.Err() == context.DeadlineExceeded {
			fmt.Println("Operation timed out")
		} else if ctx.Err() == context.Canceled {
			fmt.Println("Operation was canceled")
		} else {
			fmt.Printf("Operation failed: %v\n", err)
		}
	}
	// Output:
	// Operation failed: failed to fetch user 999: sql: no rows in result set
}
