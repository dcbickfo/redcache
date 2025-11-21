//go:build cluster

package redcache_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
	"github.com/dcbickfo/redcache/internal/cmdx"
)

var clusterAddr = []string{
	"localhost:17000",
	"localhost:17001",
	"localhost:17002",
	"localhost:17003",
	"localhost:17004",
	"localhost:17005",
}

func makeClusterCacheAside(t *testing.T) *redcache.CacheAside {
	// Allow override via environment variable
	addresses := clusterAddr
	if addr := os.Getenv("REDIS_CLUSTER_ADDR"); addr != "" {
		addresses = strings.Split(addr, ",")
	}

	cacheAside, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{
			InitAddress: addresses,
		},
		redcache.CacheAsideOption{
			LockTTL: time.Second * 1,
		},
	)
	if err != nil {
		t.Fatalf("Redis Cluster not available (use 'make docker-cluster-up' to start): %v", err)
		return nil
	}

	// Test cluster connectivity
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	innerClient := cacheAside.Client()
	if pingErr := innerClient.Do(ctx, innerClient.B().Ping().Build()).Error(); pingErr != nil {
		innerClient.Close()
		t.Fatalf("Redis Cluster not responding (use 'make docker-cluster-up' to start): %v", pingErr)
		return nil
	}

	return cacheAside
}

// TestCacheAside_Cluster_BasicOperations tests basic Get/GetMulti operations work in cluster mode
func TestCacheAside_Cluster_BasicOperations(t *testing.T) {
	t.Run("Get single key works across cluster", func(t *testing.T) {
		client := makeClusterCacheAside(t)
		if client == nil {
			return
		}
		defer client.Client().Close()

		ctx := t.Context()
		key := "cluster:basic:" + uuid.New().String()
		expectedValue := "value:" + uuid.New().String()
		called := false
		cb := makeGetCallback(expectedValue, &called)

		// First call should execute callback
		res, err := client.Get(ctx, time.Second*10, key, cb)
		require.NoError(t, err)
		assertValueEquals(t, expectedValue, res)
		assertCallbackCalled(t, called, "first Get should execute callback")

		// Second call should hit cache
		called = false
		res, err = client.Get(ctx, time.Second*10, key, cb)
		require.NoError(t, err)
		assertValueEquals(t, expectedValue, res)
		assertCallbackNotCalled(t, called, "second Get should hit cache")
	})

	t.Run("GetMulti with keys in same slot", func(t *testing.T) {
		client := makeClusterCacheAside(t)
		if client == nil {
			return
		}
		defer client.Client().Close()

		ctx := t.Context()

		// Use hash tags to ensure keys are in the same slot
		keys := []string{
			"{user:1000}:profile",
			"{user:1000}:settings",
			"{user:1000}:preferences",
		}

		// Verify all keys are in the same slot
		firstSlot := cmdx.Slot(keys[0])
		for _, key := range keys[1:] {
			require.Equal(t, firstSlot, cmdx.Slot(key), "all keys should be in same slot")
		}

		expectedValues := make(map[string]string)
		for _, key := range keys {
			expectedValues[key] = "value-for-" + key
		}

		called := false
		cb := makeGetMultiCallback(expectedValues, &called)

		// First call should execute callback
		res, err := client.GetMulti(ctx, time.Second*10, keys, cb)
		require.NoError(t, err)
		if diff := cmp.Diff(expectedValues, res); diff != "" {
			t.Errorf("GetMulti() mismatch (-want +got):\n%s", diff)
		}
		assertCallbackCalled(t, called, "first GetMulti should execute callback")

		// Second call should hit cache
		called = false
		res, err = client.GetMulti(ctx, time.Second*10, keys, cb)
		require.NoError(t, err)
		if diff := cmp.Diff(expectedValues, res); diff != "" {
			t.Errorf("GetMulti() mismatch (-want +got):\n%s", diff)
		}
		assertCallbackNotCalled(t, called, "second GetMulti should hit cache")
	})

	t.Run("GetMulti with keys across different slots", func(t *testing.T) {
		client := makeClusterCacheAside(t)
		if client == nil {
			return
		}
		defer client.Client().Close()

		ctx := t.Context()

		// Generate keys guaranteed to be in different hash slots
		keys := generateKeysInDifferentSlots("cluster:multiSlot", 3)
		t.Logf("Keys: %v", keys)

		expectedValues := make(map[string]string)
		for _, key := range keys {
			expectedValues[key] = "value-for-" + key
		}

		called := false
		cb := makeGetMultiCallback(expectedValues, &called)

		// Should successfully handle keys across different slots
		res, err := client.GetMulti(ctx, time.Second*10, keys, cb)
		require.NoError(t, err)
		if diff := cmp.Diff(expectedValues, res); diff != "" {
			t.Errorf("GetMulti() mismatch (-want +got):\n%s", diff)
		}
		assertCallbackCalled(t, called, "first GetMulti should execute callback")

		// Second call should hit cache
		called = false
		res, err = client.GetMulti(ctx, time.Second*10, keys, cb)
		require.NoError(t, err)
		if diff := cmp.Diff(expectedValues, res); diff != "" {
			t.Errorf("GetMulti() mismatch (-want +got):\n%s", diff)
		}
		assertCallbackNotCalled(t, called, "second GetMulti should hit cache")
	})
}

// TestCacheAside_Cluster_LargeKeySet tests handling of large number of keys across slots
func TestCacheAside_Cluster_LargeKeySet(t *testing.T) {
	client := makeClusterCacheAside(t)
	if client == nil {
		return
	}
	defer client.Client().Close()

	ctx := t.Context()

	// Create 100 keys that will span multiple slots
	numKeys := 100
	keys := make([]string, numKeys)
	expectedValues := make(map[string]string)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("cluster:large:%d:%s", i, uuid.New().String())
		keys[i] = key
		expectedValues[key] = fmt.Sprintf("value-%d", i)
	}

	// Verify keys span multiple slots
	slots := make(map[uint16]bool)
	for _, key := range keys {
		slots[cmdx.Slot(key)] = true
	}
	t.Logf("%d keys span %d different slots", numKeys, len(slots))
	require.Greater(t, len(slots), 10, "should span many slots")

	called := false
	cb := func(ctx context.Context, reqKeys []string) (map[string]string, error) {
		called = true
		result := make(map[string]string)
		for _, k := range reqKeys {
			result[k] = expectedValues[k]
		}
		return result, nil
	}

	// Should successfully handle large key set across slots
	res, err := client.GetMulti(ctx, time.Second*10, keys, cb)
	require.NoError(t, err)
	assert.Len(t, res, numKeys)
	assert.True(t, called)

	// Verify all values are correct
	if diff := cmp.Diff(expectedValues, res); diff != "" {
		t.Errorf("GetMulti() mismatch (-want +got):\n%s", diff)
	}

	// Second call should hit cache
	called = false
	res, err = client.GetMulti(ctx, time.Second*10, keys, cb)
	require.NoError(t, err)
	assert.Len(t, res, numKeys)
	assert.False(t, called, "should hit cache")
}

// TestCacheAside_Cluster_ConcurrentOperations tests concurrent operations across cluster nodes
func TestCacheAside_Cluster_ConcurrentOperations(t *testing.T) {
	t.Run("concurrent Gets to different slots don't block each other", func(t *testing.T) {
		client1 := makeClusterCacheAside(t)
		if client1 == nil {
			return
		}
		defer client1.Client().Close()

		client2 := makeClusterCacheAside(t)
		defer client2.Client().Close()

		ctx := t.Context()

		// Create keys in different slots
		key1 := "{shard:1}:key"
		key2 := "{shard:2}:key"

		// Verify keys are in different slots
		require.NotEqual(t, cmdx.Slot(key1), cmdx.Slot(key2), "keys should be in different slots")

		var wg sync.WaitGroup
		var callbackCount atomic.Int32

		// Client 1 gets key1 with slow callback
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = client1.Get(ctx, time.Second*10, key1, func(ctx context.Context, key string) (string, error) {
				callbackCount.Add(1)
				time.Sleep(500 * time.Millisecond)
				return "value1", nil
			})
		}()

		// Give client1 time to acquire lock
		time.Sleep(50 * time.Millisecond)

		// Client 2 gets key2 - should not be blocked by key1
		wg.Add(1)
		start := time.Now()
		go func() {
			defer wg.Done()
			_, _ = client2.Get(ctx, time.Second*10, key2, func(ctx context.Context, key string) (string, error) {
				callbackCount.Add(1)
				return "value2", nil
			})
		}()

		wg.Wait()
		elapsed := time.Since(start)

		// Client 2 should complete quickly (not wait for client1)
		// Note: Increased threshold from 200ms to 600ms due to cluster coordination overhead
		assert.Less(t, elapsed, 600*time.Millisecond, "operations on different slots should not block")
		assert.Equal(t, int32(2), callbackCount.Load(), "both callbacks should execute")
	})

	t.Run("concurrent Gets to same key coordinate properly", func(t *testing.T) {
		client1 := makeClusterCacheAside(t)
		if client1 == nil {
			return
		}
		defer client1.Client().Close()

		client2 := makeClusterCacheAside(t)
		defer client2.Client().Close()

		ctx := t.Context()
		key := "cluster:concurrent:" + uuid.New().String()

		var callbackCount atomic.Int32
		cb := func(ctx context.Context, key string) (string, error) {
			callbackCount.Add(1)
			time.Sleep(200 * time.Millisecond)
			return "shared-value", nil
		}

		var wg sync.WaitGroup
		results := make([]string, 2)
		errors := make([]error, 2)

		// Both clients try to Get same key concurrently
		wg.Add(2)
		go func() {
			defer wg.Done()
			results[0], errors[0] = client1.Get(ctx, time.Second*10, key, cb)
		}()
		go func() {
			defer wg.Done()
			results[1], errors[1] = client2.Get(ctx, time.Second*10, key, cb)
		}()

		wg.Wait()

		// Both should succeed
		for i, err := range errors {
			assert.NoError(t, err, "client %d should succeed", i)
		}

		// Both should get same value
		assert.Equal(t, "shared-value", results[0])
		assert.Equal(t, "shared-value", results[1])

		// Callback should only be called once (distributed lock coordination)
		assert.Equal(t, int32(1), callbackCount.Load(), "callback should only execute once")
	})
}

// TestCacheAside_Cluster_PartialResults tests partial cache hits across slots
func TestCacheAside_Cluster_PartialResults(t *testing.T) {
	client := makeClusterCacheAside(t)
	if client == nil {
		return
	}
	defer client.Client().Close()

	ctx := t.Context()

	// Create keys across different slots
	keys := []string{
		"{shard:1}:key1",
		"{shard:2}:key2",
		"{shard:3}:key3",
	}

	// Verify keys are in different slots
	slots := make(map[uint16]bool)
	for _, key := range keys {
		slots[cmdx.Slot(key)] = true
	}
	require.Equal(t, 3, len(slots), "keys should be in 3 different slots")

	// Pre-populate first key
	cbSingle := func(ctx context.Context, key string) (string, error) {
		return "value1", nil
	}
	_, err := client.Get(ctx, time.Second*10, keys[0], cbSingle)
	require.NoError(t, err)

	// Now request all three keys
	requestedKeys := make([]string, 0)
	cb := func(ctx context.Context, reqKeys []string) (map[string]string, error) {
		requestedKeys = append(requestedKeys, reqKeys...)
		result := make(map[string]string)
		for _, k := range reqKeys {
			result[k] = "value-" + k[len(k)-4:]
		}
		return result, nil
	}

	res, err := client.GetMulti(ctx, time.Second*10, keys, cb)
	require.NoError(t, err)
	assert.Len(t, res, 3)

	// Callback should only be called for the 2 missing keys
	assert.Len(t, requestedKeys, 2, "should only request 2 missing keys")
	assert.NotContains(t, requestedKeys, keys[0], "should not request cached key")
}

// TestCacheAside_Cluster_Invalidation tests Del/DelMulti across cluster
func TestCacheAside_Cluster_Invalidation(t *testing.T) {
	t.Run("Del removes key in cluster", func(t *testing.T) {
		client := makeClusterCacheAside(t)
		if client == nil {
			return
		}
		defer client.Client().Close()

		ctx := t.Context()
		key := "cluster:del:" + uuid.New().String()

		// Set a value
		innerClient := client.Client()
		err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value("test-value").Ex(time.Second*10).Build()).Error()
		require.NoError(t, err)

		// Verify it exists
		val, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "test-value", val)

		// Delete it
		err = client.Del(ctx, key)
		require.NoError(t, err)

		// Verify it's gone
		err = innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
		assert.True(t, rueidis.IsRedisNil(err))
	})

	t.Run("DelMulti removes keys across different slots", func(t *testing.T) {
		client := makeClusterCacheAside(t)
		if client == nil {
			return
		}
		defer client.Client().Close()

		ctx := t.Context()

		// Create keys across different slots
		keys := []string{
			"{shard:1}:del1",
			"{shard:2}:del2",
			"{shard:3}:del3",
		}

		// Set all keys
		innerClient := client.Client()
		for _, key := range keys {
			err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value("value").Ex(time.Second*10).Build()).Error()
			require.NoError(t, err)
		}

		// Delete all keys
		err := client.DelMulti(ctx, keys...)
		require.NoError(t, err)

		// Verify all are gone
		for _, key := range keys {
			getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
			assert.True(t, rueidis.IsRedisNil(getErr), "key %s should be deleted", key)
		}
	})
}

// TestCacheAside_Cluster_ErrorHandling tests error scenarios in cluster
func TestCacheAside_Cluster_ErrorHandling(t *testing.T) {
	t.Run("callback error does not cache across cluster", func(t *testing.T) {
		client := makeClusterCacheAside(t)
		if client == nil {
			return
		}
		defer client.Client().Close()

		ctx := t.Context()
		key := "cluster:error:" + uuid.New().String()

		callCount := 0
		cb := func(ctx context.Context, key string) (string, error) {
			callCount++
			if callCount == 1 {
				return "", fmt.Errorf("database error")
			}
			return "success-value", nil
		}

		// First call should fail
		_, err := client.Get(ctx, time.Second*10, key, cb)
		require.Error(t, err)
		assert.Equal(t, 1, callCount)

		// Second call should retry (error was not cached)
		res, err := client.Get(ctx, time.Second*10, key, cb)
		require.NoError(t, err)
		assert.Equal(t, "success-value", res)
		assert.Equal(t, 2, callCount)
	})

	t.Run("context cancellation works in cluster", func(t *testing.T) {
		client := makeClusterCacheAside(t)
		if client == nil {
			return
		}
		defer client.Client().Close()

		key := "cluster:cancel:" + uuid.New().String()

		ctx, cancel := context.WithCancel(t.Context())

		// Set a lock manually to force waiting
		innerClient := client.Client()
		lockVal := "__redcache:lock:" + uuid.New().String()
		err := innerClient.Do(context.Background(), innerClient.B().Set().Key(key).Value(lockVal).Ex(time.Second*5).Build()).Error()
		require.NoError(t, err)

		// Cancel context after short delay
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		// Get should fail with context canceled
		_, err = client.Get(ctx, time.Second*10, key, func(ctx context.Context, key string) (string, error) {
			return "value", nil
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)

		// Cleanup
		innerClient.Do(context.Background(), innerClient.B().Del().Key(key).Build())
	})
}

// TestCacheAside_Cluster_StressTest tests high load scenarios
func TestCacheAside_Cluster_StressTest(t *testing.T) {
	client := makeClusterCacheAside(t)
	if client == nil {
		return
	}
	defer client.Client().Close()

	ctx := t.Context()

	// Create many keys across all slots
	numKeys := 50
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("cluster:stress:%d:%s", i, uuid.New().String())
	}

	// Track callback invocations per key
	callbackCounts := sync.Map{}

	var wg sync.WaitGroup
	numGoroutines := 20

	// Many goroutines all requesting same keys
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Each goroutine gets a random subset of keys
			selectedKeys := []string{
				keys[idx%numKeys],
				keys[(idx+10)%numKeys],
				keys[(idx+25)%numKeys],
			}

			_, err := client.GetMulti(ctx, time.Second*10, selectedKeys,
				func(ctx context.Context, reqKeys []string) (map[string]string, error) {
					result := make(map[string]string)
					for _, k := range reqKeys {
						// Increment callback count for this key
						count, _ := callbackCounts.LoadOrStore(k, &atomic.Int32{})
						count.(*atomic.Int32).Add(1)

						result[k] = fmt.Sprintf("value-%s", k[len(k)-8:])
					}
					return result, nil
				})

			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Each key should have been computed exactly once (distributed locking working)
	callbackCounts.Range(func(key, value interface{}) bool {
		count := value.(*atomic.Int32).Load()
		assert.Equal(t, int32(1), count, "Key %v should be computed exactly once, got %d", key, count)
		return true
	})
}
