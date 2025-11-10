//go:build examples

package examples

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
	"github.com/dcbickfo/redcache/examples/exampleutil"
)

var redisAddr string

func TestMain(m *testing.M) {
	// Allow overriding Redis address via environment variable
	redisAddr = os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	os.Exit(m.Run())
}

// testKey generates a unique key for each test run to avoid interference.
func testKey(t *testing.T, base string) string {
	return fmt.Sprintf("%s:%s:%d", base, t.Name(), time.Now().UnixNano())
}

// TestCacheAsidePattern verifies the cache-aside example works.
func TestCacheAsidePattern(t *testing.T) {
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

	// Use unique key per test run to avoid interference
	key := testKey(t, "test:common:aside:user:1")

	// First call - cache miss
	userJSON, err := ca.Get(ctx, 1*time.Minute, key, func(ctx context.Context, k string) (string, error) {
		user, dbErr := db.GetUser(ctx, "1")
		if dbErr != nil {
			return "", dbErr
		}
		return exampleutil.SerializeUser(user)
	})

	require.NoError(t, err)
	user, err := exampleutil.DeserializeUser(userJSON)
	require.NoError(t, err)
	assert.Equal(t, "1", user.ID)

	// Second call - should hit cache (verify by checking result is same)
	userJSON2, err := ca.Get(ctx, 1*time.Minute, key, func(ctx context.Context, k string) (string, error) {
		t.Error("Callback should not be called on cache hit")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, userJSON, userJSON2)
}

// TestWriteThroughPattern verifies the write-through example works.
func TestWriteThroughPattern(t *testing.T) {
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

	updatedUser := &exampleutil.User{
		ID:    "1",
		Name:  "Updated Name",
		Email: "updated@example.com",
	}

	key := testKey(t, "test:common:write:user:1")

	err = pca.Set(ctx, 1*time.Minute, key, func(ctx context.Context, k string) (string, error) {
		if dbErr := db.UpdateUser(ctx, updatedUser); dbErr != nil {
			return "", dbErr
		}
		return exampleutil.SerializeUser(updatedUser)
	})
	require.NoError(t, err)

	// Verify value is in cache
	result := pca.Client().Do(ctx, pca.Client().B().Get().Key(key).Build())
	require.NoError(t, result.Error())

	cached, err := result.ToString()
	require.NoError(t, err)

	user, err := exampleutil.DeserializeUser(cached)
	require.NoError(t, err)
	assert.Equal(t, "Updated Name", user.Name)
}

// TestBatchOperations verifies the batch operations example works.
func TestBatchOperations(t *testing.T) {
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

	ids := []string{"1", "2", "3"}
	keyPrefix := fmt.Sprintf("test:common:batch:user:%s:%d:", t.Name(), time.Now().UnixNano())
	keys := []string{keyPrefix + "1", keyPrefix + "2", keyPrefix + "3"}

	results, err := ca.GetMulti(ctx, 1*time.Minute, keys, func(ctx context.Context, missedKeys []string) (map[string]string, error) {
		userMap, dbErr := db.GetUsers(ctx, ids)
		if dbErr != nil {
			return nil, dbErr
		}

		result := make(map[string]string)
		for i, key := range missedKeys {
			serialized, serErr := exampleutil.SerializeUser(userMap[ids[i]])
			if serErr != nil {
				return nil, serErr
			}
			result[key] = serialized
		}
		return result, nil
	})

	require.NoError(t, err)
	assert.Len(t, results, 3)
}

// TestContextCancellation verifies context cancellation is respected.
func TestContextCancellation(t *testing.T) {
	t.Parallel()

	ca, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: []string{redisAddr}},
		redcache.CacheAsideOption{LockTTL: 5 * time.Second},
	)
	require.NoError(t, err)
	defer ca.Client().Close()

	db := exampleutil.NewMockDatabase()

	// Context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	key := testKey(t, "test:common:cancel:user:1")

	_, err = ca.Get(ctx, 1*time.Minute, key, func(ctx context.Context, k string) (string, error) {
		user, dbErr := db.GetUser(ctx, "1")
		if dbErr != nil {
			return "", dbErr
		}
		return exampleutil.SerializeUser(user)
	})

	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// TestCacheWarming verifies cache warming example.
func TestCacheWarming(t *testing.T) {
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

	// Fetch from database
	userMap, err := db.GetUsers(ctx, allUserIDs)
	require.NoError(t, err)

	// Prepare cache keys and serialize
	keys := make([]string, 0, len(userMap))
	serializedUsers := make(map[string]string)

	keyPrefix := fmt.Sprintf("test:common:warm:user:%s:%d:", t.Name(), time.Now().UnixNano())
	for id, user := range userMap {
		key := keyPrefix + id
		keys = append(keys, key)

		serialized, serErr := exampleutil.SerializeUser(user)
		require.NoError(t, serErr)
		serializedUsers[key] = serialized
	}

	// Warm the cache
	cached, err := pca.SetMulti(ctx, 1*time.Hour, keys,
		func(ctx context.Context, lockedKeys []string) (map[string]string, error) {
			result := make(map[string]string, len(lockedKeys))
			for _, key := range lockedKeys {
				result[key] = serializedUsers[key]
			}
			return result, nil
		})

	require.NoError(t, err)
	assert.Len(t, cached, 3)
}

// TestSetPrecomputedValue verifies setting pre-computed values.
func TestSetPrecomputedValue(t *testing.T) {
	t.Parallel()

	pca, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: []string{redisAddr}},
		redcache.CacheAsideOption{LockTTL: 5 * time.Second},
	)
	require.NoError(t, err)
	defer pca.Client().Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	precomputedValue := `{"id":"99","name":"Computed User","email":"computed@example.com"}`
	key := testKey(t, "test:common:precomp:user:99")

	err = pca.Set(ctx, 10*time.Minute, key, func(ctx context.Context, k string) (string, error) {
		return precomputedValue, nil
	})

	require.NoError(t, err)

	// Verify it's in cache
	result := pca.Client().Do(ctx, pca.Client().B().Get().Key(key).Build())
	require.NoError(t, result.Error())

	cached, err := result.ToString()
	require.NoError(t, err)
	assert.Equal(t, precomputedValue, cached)
}
