//go:build examples

package examples

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
	"github.com/dcbickfo/redcache/examples/exampleutil"
)

// TestProductInventoryUpdate verifies the inventory update example.
func TestProductInventoryUpdate(t *testing.T) {
	t.Parallel()

	pca, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: []string{redisAddr}},
		redcache.CacheAsideOption{LockTTL: 5 * time.Second},
	)
	require.NoError(t, err)
	defer pca.Client().Close()

	db := exampleutil.NewMockDatabase()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	productID := "prod-1"
	key := testKey(t, "test:ops:product:"+productID)

	// Get initial stock
	initialProduct, err := db.GetProduct(ctx, productID)
	require.NoError(t, err)
	initialStock := initialProduct.Stock

	// Update stock
	err = pca.Set(ctx, 1*time.Minute, key, func(ctx context.Context, _ string) (string, error) {
		if stockErr := db.UpdateProductStock(ctx, productID, -1); stockErr != nil {
			return "", stockErr
		}

		prod, productErr := db.GetProduct(ctx, productID)
		if productErr != nil {
			return "", productErr
		}

		return exampleutil.SerializeProduct(prod)
	})
	require.NoError(t, err)

	// Verify stock decreased
	updatedProduct, err := db.GetProduct(ctx, productID)
	require.NoError(t, err)
	assert.Equal(t, initialStock-1, updatedProduct.Stock)
}

// TestCacheInvalidation verifies cache invalidation works.
func TestCacheInvalidation(t *testing.T) {
	t.Parallel()

	pca, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: []string{redisAddr}},
		redcache.CacheAsideOption{LockTTL: 5 * time.Second},
	)
	require.NoError(t, err)
	defer pca.Client().Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Set a value first
	testValue := `{"id":"test","name":"Test"}`
	key := testKey(t, "test:ops:invalidate:user:1")
	err = pca.Set(ctx, 1*time.Minute, key, func(ctx context.Context, k string) (string, error) {
		return testValue, nil
	})
	require.NoError(t, err)

	// Delete it
	err = pca.Del(ctx, key)
	require.NoError(t, err)

	// Verify it's gone
	result := pca.Client().Do(ctx, pca.Client().B().Get().Key(key).Build())
	assert.True(t, rueidis.IsRedisNil(result.Error()))

	// Test DelMulti
	keyPrefix := fmt.Sprintf("test:ops:invalidate:multi:%s:%d:", t.Name(), time.Now().UnixNano())
	key1 := keyPrefix + "1"
	key2 := keyPrefix + "2"
	key3 := keyPrefix + "3"

	// Set multiple values
	for _, key := range []string{key1, key2, key3} {
		err = pca.Set(ctx, 1*time.Minute, key, func(ctx context.Context, k string) (string, error) {
			return "value", nil
		})
		require.NoError(t, err)
	}

	// Delete them all
	err = pca.DelMulti(ctx, key1, key2, key3)
	require.NoError(t, err)

	// Verify they're gone
	for _, key := range []string{key1, key2, key3} {
		result := pca.Client().Do(ctx, pca.Client().B().Get().Key(key).Build())
		assert.True(t, rueidis.IsRedisNil(result.Error()), "key %s should be deleted", key)
	}
}

// TestReadThroughCache verifies read-through pattern.
func TestReadThroughCache(t *testing.T) {
	t.Parallel()

	ca, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: []string{redisAddr}},
		redcache.CacheAsideOption{LockTTL: 5 * time.Second},
	)
	require.NoError(t, err)
	defer ca.Client().Close()

	db := exampleutil.NewMockDatabase()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key := testKey(t, "test:ops:product:prod-2")

	// First call - cache miss
	productJSON, err := ca.Get(ctx, 15*time.Minute, key,
		func(ctx context.Context, key string) (string, error) {
			// Just use the known product ID since key parsing is complex with test name
			productID := "prod-2"

			prod, dbErr := db.GetProduct(ctx, productID)
			if dbErr != nil {
				return "", dbErr
			}

			return exampleutil.SerializeProduct(prod)
		})

	require.NoError(t, err)

	retrievedProduct, err := exampleutil.DeserializeProduct(productJSON)
	require.NoError(t, err)
	assert.Equal(t, "prod-2", retrievedProduct.ID)
	assert.Equal(t, "Mouse", retrievedProduct.Name)
}

// TestConditionalCaching verifies conditional caching logic.
func TestConditionalCaching(t *testing.T) {
	t.Parallel()

	ca, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: []string{redisAddr}},
		redcache.CacheAsideOption{LockTTL: 5 * time.Second},
	)
	require.NoError(t, err)
	defer ca.Client().Close()

	db := exampleutil.NewMockDatabase()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key := testKey(t, "test:ops:conditional:user:1")

	userJSON, err := ca.Get(ctx, 5*time.Minute, key,
		func(ctx context.Context, key string) (string, error) {
			// Just use the known user ID since key parsing is complex with test name
			userID := "1"

			u, dbErr := db.GetUser(ctx, userID)
			if dbErr != nil {
				return "", dbErr
			}

			// In real code, you might have conditions here:
			// - Only cache if user is active
			// - Apply different TTLs based on user type
			// For now, always cache

			return exampleutil.SerializeUser(u)
		})

	require.NoError(t, err)

	retrievedUser, err := exampleutil.DeserializeUser(userJSON)
	require.NoError(t, err)
	assert.Equal(t, "1", retrievedUser.ID)
}

// TestBulkCachePopulation verifies bulk population.
func TestBulkCachePopulation(t *testing.T) {
	t.Parallel()

	pca, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: []string{redisAddr}},
		redcache.CacheAsideOption{LockTTL: 5 * time.Second},
	)
	require.NoError(t, err)
	defer pca.Client().Close()

	db := exampleutil.NewMockDatabase()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	allUserIDs := []string{"1", "2", "3"}

	userMap, err := db.GetUsers(ctx, allUserIDs)
	require.NoError(t, err)

	// Prepare serialized data
	values := make(map[string]string)
	keys := make([]string, 0, len(userMap))

	keyPrefix := fmt.Sprintf("test:ops:bulk:user:%s:%d:", t.Name(), time.Now().UnixNano())
	for id, user := range userMap {
		key := keyPrefix + id
		keys = append(keys, key)

		serialized, serErr := exampleutil.SerializeUser(user)
		require.NoError(t, serErr)
		values[key] = serialized
	}

	// Bulk populate cache
	result, err := pca.SetMulti(ctx, 30*time.Minute, keys,
		func(ctx context.Context, lockedKeys []string) (map[string]string, error) {
			ret := make(map[string]string, len(lockedKeys))
			for _, key := range lockedKeys {
				ret[key] = values[key]
			}
			return ret, nil
		})

	require.NoError(t, err)
	assert.Len(t, result, 3)
}

// TestErrorHandling verifies proper error handling.
func TestErrorHandling(t *testing.T) {
	t.Parallel()

	ca, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: []string{redisAddr}},
		redcache.CacheAsideOption{LockTTL: 5 * time.Second},
	)
	require.NoError(t, err)
	defer ca.Client().Close()

	db := exampleutil.NewMockDatabase()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key := testKey(t, "test:ops:error:user:999")

	// Try to get non-existent user
	_, err = ca.Get(ctx, 5*time.Minute, key,
		func(ctx context.Context, key string) (string, error) {
			if ctx.Err() != nil {
				return "", ctx.Err()
			}

			// Just use the known user ID since key parsing is complex with test name
			userID := "999"
			u, dbErr := db.GetUser(ctx, userID)
			if dbErr != nil {
				return "", dbErr
			}

			return exampleutil.SerializeUser(u)
		})

	require.Error(t, err)
	assert.Equal(t, sql.ErrNoRows, err)
}
