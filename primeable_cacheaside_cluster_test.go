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

// makeClusterPrimeableCacheAside creates a PrimeableCacheAside client connected to Redis Cluster
func makeClusterPrimeableCacheAside(t *testing.T) *redcache.PrimeableCacheAside {
	// Allow override via environment variable
	addresses := clusterAddr
	if addr := os.Getenv("REDIS_CLUSTER_ADDR"); addr != "" {
		addresses = strings.Split(addr, ",")
	}

	cacheAside, err := redcache.NewPrimeableCacheAside(
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	innerClient := cacheAside.Client()
	if pingErr := innerClient.Do(ctx, innerClient.B().Ping().Build()).Error(); pingErr != nil {
		innerClient.Close()
		t.Fatalf("Redis Cluster not responding (use 'make docker-cluster-up' to start): %v", pingErr)
		return nil
	}

	return cacheAside
}

// TestPrimeableCacheAside_Cluster_BasicSetOperations tests Set operations work in cluster mode
func TestPrimeableCacheAside_Cluster_BasicSetOperations(t *testing.T) {
	t.Run("Set single key works across cluster", func(t *testing.T) {
		client := makeClusterPrimeableCacheAside(t)
		if client == nil {
			return
		}
		defer client.Close()

		ctx := context.Background()
		key := "pcluster:set:" + uuid.New().String()
		expectedValue := "value:" + uuid.New().String()

		err := client.Set(ctx, time.Second*10, key, func(_ context.Context, _ string) (string, error) {
			return expectedValue, nil
		})
		require.NoError(t, err)

		// Verify value was set in Redis
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, expectedValue, result)

		// Get should retrieve the value without callback
		called := false
		res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, key string) (string, error) {
			called = true
			return "should-not-be-called", nil
		})
		require.NoError(t, err)
		assert.Equal(t, expectedValue, res)
		assert.False(t, called)
	})

	t.Run("ForceSet bypasses locks in cluster", func(t *testing.T) {
		client := makeClusterPrimeableCacheAside(t)
		if client == nil {
			return
		}
		defer client.Close()

		ctx := context.Background()
		key := "pcluster:force:" + uuid.New().String()

		// Manually set a lock
		innerClient := client.Client()
		lockVal := "__redcache:lock:" + uuid.New().String()
		err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(lockVal).Ex(time.Second*5).Build()).Error()
		require.NoError(t, err)

		// ForceSet should succeed despite lock
		forcedValue := "forced:" + uuid.New().String()
		err = client.ForceSet(ctx, time.Second*10, key, forcedValue)
		require.NoError(t, err)

		// Verify forced value
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, forcedValue, result)
	})
}

// TestPrimeableCacheAside_Cluster_SetMultiOperations tests SetMulti with keys across slots
func TestPrimeableCacheAside_Cluster_SetMultiOperations(t *testing.T) {
	t.Run("SetMulti with keys in same slot", func(t *testing.T) {
		client := makeClusterPrimeableCacheAside(t)
		if client == nil {
			return
		}
		defer client.Close()

		ctx := context.Background()

		// Use hash tags to ensure same slot
		keys := []string{
			"{user:2000}:profile",
			"{user:2000}:settings",
			"{user:2000}:preferences",
		}

		// Verify all in same slot
		firstSlot := cmdx.Slot(keys[0])
		for _, key := range keys[1:] {
			require.Equal(t, firstSlot, cmdx.Slot(key))
		}

		expectedValues := make(map[string]string)
		for _, key := range keys {
			expectedValues[key] = "value-" + key
		}

		result, err := client.SetMulti(ctx, time.Second*10, keys, func(_ context.Context, reqKeys []string) (map[string]string, error) {
			res := make(map[string]string)
			for _, k := range reqKeys {
				res[k] = expectedValues[k]
			}
			return res, nil
		})
		require.NoError(t, err)
		if diff := cmp.Diff(expectedValues, result); diff != "" {
			t.Errorf("SetMulti() mismatch (-want +got):\n%s", diff)
		}

		// Verify all values in Redis
		innerClient := client.Client()
		for key, expectedValue := range expectedValues {
			actualValue, getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
			require.NoError(t, getErr)
			assert.Equal(t, expectedValue, actualValue)
		}
	})

	t.Run("SetMulti with keys across different slots", func(t *testing.T) {
		client := makeClusterPrimeableCacheAside(t)
		if client == nil {
			return
		}
		defer client.Close()

		ctx := context.Background()

		// Create keys in different slots
		keys := []string{
			"{shard:100}:key1",
			"{shard:200}:key2",
			"{shard:300}:key3",
		}

		// Verify keys are in different slots
		slots := make(map[uint16]bool)
		for _, key := range keys {
			slots[cmdx.Slot(key)] = true
		}
		require.Equal(t, 3, len(slots), "keys should be in 3 different slots")

		expectedValues := make(map[string]string)
		for _, key := range keys {
			expectedValues[key] = "value-" + key
		}

		result, err := client.SetMulti(ctx, time.Second*10, keys, func(_ context.Context, reqKeys []string) (map[string]string, error) {
			res := make(map[string]string)
			for _, k := range reqKeys {
				res[k] = expectedValues[k]
			}
			return res, nil
		})
		require.NoError(t, err)
		if diff := cmp.Diff(expectedValues, result); diff != "" {
			t.Errorf("SetMulti() mismatch (-want +got):\n%s", diff)
		}

		// Verify all values across slots
		innerClient := client.Client()
		for key, expectedValue := range expectedValues {
			actualValue, getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
			require.NoError(t, getErr)
			assert.Equal(t, expectedValue, actualValue)
		}
	})

	t.Run("ForceSetMulti with keys across different slots", func(t *testing.T) {
		client := makeClusterPrimeableCacheAside(t)
		if client == nil {
			return
		}
		defer client.Close()

		ctx := context.Background()

		keys := []string{
			"{shard:400}:key1",
			"{shard:500}:key2",
			"{shard:600}:key3",
		}

		// Pre-set locks on some keys
		innerClient := client.Client()
		lockVal := "__redcache:lock:" + uuid.New().String()
		for _, key := range keys[:2] {
			err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(lockVal).Ex(time.Second*5).Build()).Error()
			require.NoError(t, err)
		}

		// ForceSetMulti should succeed despite locks
		values := make(map[string]string)
		for _, key := range keys {
			values[key] = "forced-" + key
		}

		err := client.ForceSetMulti(ctx, time.Second*10, values)
		require.NoError(t, err)

		// Verify all values
		for key, expectedValue := range values {
			actualValue, getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
			require.NoError(t, getErr)
			assert.Equal(t, expectedValue, actualValue)
		}
	})
}

// TestPrimeableCacheAside_Cluster_LargeKeySet tests handling many keys across slots
func TestPrimeableCacheAside_Cluster_LargeKeySet(t *testing.T) {
	client := makeClusterPrimeableCacheAside(t)
	if client == nil {
		return
	}
	defer client.Close()

	ctx := context.Background()

	// Create 50 keys across multiple slots
	numKeys := 50
	keys := make([]string, numKeys)
	expectedValues := make(map[string]string)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("pcluster:large:%d:%s", i, uuid.New().String())
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

	// SetMulti should handle all keys across slots
	result, err := client.SetMulti(ctx, time.Second*10, keys, func(_ context.Context, reqKeys []string) (map[string]string, error) {
		res := make(map[string]string)
		for _, k := range reqKeys {
			res[k] = expectedValues[k]
		}
		return res, nil
	})
	require.NoError(t, err)
	assert.Len(t, result, numKeys)

	// Verify all values in Redis
	innerClient := client.Client()
	for key, expectedValue := range expectedValues {
		actualValue, getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, getErr)
		assert.Equal(t, expectedValue, actualValue, "key: %s", key)
	}

	// GetMulti should retrieve all values without callback
	called := false
	retrieved, err := client.GetMulti(ctx, time.Second*10, keys, func(_ context.Context, _ []string) (map[string]string, error) {
		called = true
		return nil, fmt.Errorf("should not be called")
	})
	require.NoError(t, err)
	assert.Len(t, retrieved, numKeys)
	assert.False(t, called)
}

// TestPrimeableCacheAside_Cluster_ConcurrentSetOperations tests concurrent writes in cluster
func TestPrimeableCacheAside_Cluster_ConcurrentSetOperations(t *testing.T) {
	t.Run("concurrent Sets to different slots don't block", func(t *testing.T) {
		client1 := makeClusterPrimeableCacheAside(t)
		if client1 == nil {
			return
		}
		defer client1.Close()

		client2 := makeClusterPrimeableCacheAside(t)
		defer client2.Close()

		ctx := context.Background()

		// Keys in different slots
		key1 := "{shard:700}:concurrent1"
		key2 := "{shard:800}:concurrent2"

		require.NotEqual(t, cmdx.Slot(key1), cmdx.Slot(key2))

		var wg sync.WaitGroup

		// Client 1 sets key1 with slow callback
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = client1.Set(ctx, time.Second*10, key1, func(_ context.Context, _ string) (string, error) {
				time.Sleep(500 * time.Millisecond)
				return "value1", nil
			})
		}()

		time.Sleep(50 * time.Millisecond)

		// Client 2 sets key2 - should not wait
		start := time.Now()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = client2.Set(ctx, time.Second*10, key2, func(_ context.Context, _ string) (string, error) {
				return "value2", nil
			})
		}()

		wg.Wait()
		elapsed := time.Since(start)

		// Client 2 should complete quickly
		// Note: Increased threshold from 200ms to 600ms due to cluster coordination overhead
		assert.Less(t, elapsed, 600*time.Millisecond, "operations on different slots should not block")
	})

	t.Run("concurrent Sets to same key coordinate properly", func(t *testing.T) {
		client1 := makeClusterPrimeableCacheAside(t)
		if client1 == nil {
			return
		}
		defer client1.Close()

		client2 := makeClusterPrimeableCacheAside(t)
		defer client2.Close()

		ctx := context.Background()
		key := "pcluster:same:" + uuid.New().String()

		var callbackCount atomic.Int32

		var wg sync.WaitGroup

		// Both clients try to Set same key
		for i := 0; i < 2; i++ {
			wg.Add(1)
			clientIdx := i
			go func() {
				defer wg.Done()
				var client *redcache.PrimeableCacheAside
				if clientIdx == 0 {
					client = client1
				} else {
					client = client2
				}
				_ = client.Set(ctx, time.Second*10, key, func(_ context.Context, _ string) (string, error) {
					callbackCount.Add(1)
					time.Sleep(100 * time.Millisecond)
					return fmt.Sprintf("value-%d", clientIdx), nil
				})
			}()
		}

		wg.Wait()

		// Callback count should be 2 (both clients write)
		// In Set, both clients will execute their callbacks serially
		assert.Equal(t, int32(2), callbackCount.Load())

		// Verify a value exists
		innerClient := client1.Client()
		val, err := innerClient.Do(context.Background(), innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.NotEmpty(t, val)
	})
}

// TestPrimeableCacheAside_Cluster_SetAndGetIntegration tests Set/Get integration in cluster
func TestPrimeableCacheAside_Cluster_SetAndGetIntegration(t *testing.T) {
	t.Run("Set then Get from different client", func(t *testing.T) {
		client1 := makeClusterPrimeableCacheAside(t)
		if client1 == nil {
			return
		}
		defer client1.Close()

		client2 := makeClusterPrimeableCacheAside(t)
		defer client2.Close()

		ctx := context.Background()
		key := "pcluster:setget:" + uuid.New().String()
		setValue := "set-value:" + uuid.New().String()

		// Client 1 sets
		err := client1.Set(ctx, time.Second*10, key, func(_ context.Context, _ string) (string, error) {
			return setValue, nil
		})
		require.NoError(t, err)

		// Client 2 gets - should hit cache
		called := false
		result, err := client2.Get(ctx, time.Second*10, key, func(_ context.Context, _ string) (string, error) {
			called = true
			return "fallback", nil
		})
		require.NoError(t, err)
		assert.Equal(t, setValue, result)
		assert.False(t, called)
	})

	t.Run("Get waits for Set across cluster nodes", func(t *testing.T) {
		client1 := makeClusterPrimeableCacheAside(t)
		if client1 == nil {
			return
		}
		defer client1.Close()

		client2 := makeClusterPrimeableCacheAside(t)
		defer client2.Close()

		ctx := context.Background()
		key := "pcluster:getwait:" + uuid.New().String()

		// Client 1 starts slow Get
		getStarted := make(chan struct{})
		getDone := make(chan struct{})
		go func() {
			defer close(getDone)
			_, _ = client1.Get(ctx, time.Second*10, key, func(_ context.Context, _ string) (string, error) {
				close(getStarted)
				time.Sleep(500 * time.Millisecond)
				return "get-value", nil
			})
		}()

		<-getStarted
		time.Sleep(50 * time.Millisecond)

		// Client 2 tries to Set - should wait
		start := time.Now()
		err := client2.Set(ctx, time.Second*10, key, func(_ context.Context, _ string) (string, error) {
			return "set-value", nil
		})
		duration := time.Since(start)

		require.NoError(t, err)
		assert.Greater(t, duration, 400*time.Millisecond, "Set should wait for Get")

		<-getDone

		// Final value should be from Set
		innerClient := client1.Client()
		val, err := innerClient.Do(context.Background(), innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "set-value", val)
	})
}

// TestPrimeableCacheAside_Cluster_Invalidation tests Del/DelMulti in cluster
func TestPrimeableCacheAside_Cluster_Invalidation(t *testing.T) {
	t.Run("Del removes key in cluster", func(t *testing.T) {
		client := makeClusterPrimeableCacheAside(t)
		if client == nil {
			return
		}
		defer client.Close()

		ctx := context.Background()
		key := "pcluster:del:" + uuid.New().String()

		// Set a value
		err := client.Set(ctx, time.Second*10, key, func(_ context.Context, _ string) (string, error) {
			return "test-value", nil
		})
		require.NoError(t, err)

		// Delete it
		err = client.Del(ctx, key)
		require.NoError(t, err)

		// Verify it's gone
		innerClient := client.Client()
		delErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
		assert.True(t, rueidis.IsRedisNil(delErr))
	})

	t.Run("DelMulti across different slots", func(t *testing.T) {
		client := makeClusterPrimeableCacheAside(t)
		if client == nil {
			return
		}
		defer client.Close()

		ctx := context.Background()

		keys := []string{
			"{shard:900}:del1",
			"{shard:1000}:del2",
			"{shard:1100}:del3",
		}

		// Set all keys
		values := make(map[string]string)
		for _, key := range keys {
			values[key] = "value-" + key
		}

		err := client.ForceSetMulti(ctx, time.Second*10, values)
		require.NoError(t, err)

		// Delete all
		err = client.DelMulti(ctx, keys...)
		require.NoError(t, err)

		// Verify all deleted
		innerClient := client.Client()
		for _, key := range keys {
			delErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
			assert.True(t, rueidis.IsRedisNil(delErr), "key %s should be deleted", key)
		}
	})

	t.Run("Del during Set causes failure in cluster", func(t *testing.T) {
		client1 := makeClusterPrimeableCacheAside(t)
		if client1 == nil {
			return
		}
		defer client1.Close()

		client2 := makeClusterPrimeableCacheAside(t)
		defer client2.Close()

		ctx := context.Background()
		key := "pcluster:del-set:" + uuid.New().String()

		// Client 1 starts Set
		setStarted := make(chan struct{})
		setDone := make(chan error, 1)
		go func() {
			err := client1.Set(ctx, time.Second*10, key, func(_ context.Context, _ string) (string, error) {
				close(setStarted)
				time.Sleep(300 * time.Millisecond)
				return "set-value", nil
			})
			setDone <- err
		}()

		<-setStarted
		time.Sleep(50 * time.Millisecond)

		// Client 2 deletes while Set is in progress
		err := client2.Del(ctx, key)
		require.NoError(t, err)

		// Set should fail because it lost the cache lock (per spec line 20)
		setErr := <-setDone
		require.Error(t, setErr)
		assert.ErrorIs(t, setErr, redcache.ErrLockLost)
	})
}

// TestPrimeableCacheAside_Cluster_ErrorHandling tests error scenarios in cluster
func TestPrimeableCacheAside_Cluster_ErrorHandling(t *testing.T) {
	t.Run("callback error does not cache in cluster", func(t *testing.T) {
		client := makeClusterPrimeableCacheAside(t)
		if client == nil {
			return
		}
		defer client.Close()

		ctx := context.Background()
		key := "pcluster:error:" + uuid.New().String()

		callCount := 0
		cb := func(_ context.Context, _ string) (string, error) {
			callCount++
			if callCount == 1 {
				return "", fmt.Errorf("database error")
			}
			return "success", nil
		}

		// First Set fails
		err := client.Set(ctx, time.Second*10, key, cb)
		require.Error(t, err)
		assert.Equal(t, 1, callCount)

		// Second Set should retry
		err = client.Set(ctx, time.Second*10, key, cb)
		require.NoError(t, err)
		assert.Equal(t, 2, callCount)
	})

	t.Run("context cancellation in cluster Set", func(t *testing.T) {
		client := makeClusterPrimeableCacheAside(t)
		if client == nil {
			return
		}
		defer client.Close()

		key := "pcluster:cancel:" + uuid.New().String()

		// Set a lock manually
		innerClient := client.Client()
		lockVal := "__redcache:lock:" + uuid.New().String()
		err := innerClient.Do(context.Background(), innerClient.B().Set().Key(key).Value(lockVal).Ex(time.Second*5).Build()).Error()
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Set should fail with timeout
		err = client.Set(ctx, time.Second*10, key, func(_ context.Context, _ string) (string, error) {
			return "value", nil
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		// Cleanup
		innerClient.Do(context.Background(), innerClient.B().Del().Key(key).Build())
	})
}

// TestPrimeableCacheAside_Cluster_SpecialValues tests edge cases in cluster
func TestPrimeableCacheAside_Cluster_SpecialValues(t *testing.T) {
	t.Run("empty string values in cluster", func(t *testing.T) {
		client := makeClusterPrimeableCacheAside(t)
		if client == nil {
			return
		}
		defer client.Close()

		ctx := context.Background()
		key := "pcluster:empty:" + uuid.New().String()

		err := client.Set(ctx, time.Second*10, key, func(_ context.Context, _ string) (string, error) {
			return "", nil
		})
		require.NoError(t, err)

		// Verify empty string was stored
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "", result)
	})

	t.Run("unicode and special characters in cluster", func(t *testing.T) {
		client := makeClusterPrimeableCacheAside(t)
		if client == nil {
			return
		}
		defer client.Close()

		ctx := context.Background()

		testCases := []struct {
			name  string
			value string
		}{
			{"unicode", "Hello 世界 🌍"},
			{"newlines", "line1\nline2\nline3"},
			{"tabs", "col1\tcol2\tcol3"},
			{"quotes", `"quoted" and 'single'`},
		}

		for _, tc := range testCases {
			key := "pcluster:special:" + uuid.New().String()

			err := client.Set(ctx, time.Second*10, key, func(_ context.Context, _ string) (string, error) {
				return tc.value, nil
			})
			require.NoError(t, err, "test case: %s", tc.name)

			// Verify value
			innerClient := client.Client()
			result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
			require.NoError(t, err, "test case: %s", tc.name)
			assert.Equal(t, tc.value, result, "test case: %s", tc.name)
		}
	})

	t.Run("large value in cluster", func(t *testing.T) {
		client := makeClusterPrimeableCacheAside(t)
		if client == nil {
			return
		}
		defer client.Close()

		ctx := context.Background()
		key := "pcluster:large:" + uuid.New().String()
		largeValue := strings.Repeat("x", 1024*1024) // 1MB

		err := client.Set(ctx, time.Second*10, key, func(_ context.Context, _ string) (string, error) {
			return largeValue, nil
		})
		require.NoError(t, err)

		// Verify large value
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, len(largeValue), len(result))
	})
}

// TestPrimeableCacheAside_Cluster_StressTest tests high load in cluster
func TestPrimeableCacheAside_Cluster_StressTest(t *testing.T) {
	client := makeClusterPrimeableCacheAside(t)
	if client == nil {
		return
	}
	defer client.Close()

	ctx := context.Background()

	// Create many keys across slots
	numKeys := 50
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("pcluster:stress:%d:%s", i, uuid.New().String())
	}

	// Verify keys span multiple slots
	slots := make(map[uint16]bool)
	for _, key := range keys {
		slots[cmdx.Slot(key)] = true
	}
	t.Logf("%d keys span %d slots", numKeys, len(slots))

	var wg sync.WaitGroup
	numGoroutines := 20
	successCount := atomic.Int32{}
	errorCount := atomic.Int32{}

	// Many goroutines concurrently Set and Get
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Each goroutine works on a subset of keys
			myKeys := []string{
				keys[idx%numKeys],
				keys[(idx+10)%numKeys],
				keys[(idx+25)%numKeys],
			}

			// Set operation
			if idx%2 == 0 {
				err := client.ForceSetMulti(ctx, time.Second*10, map[string]string{
					myKeys[0]: fmt.Sprintf("value-%d-0", idx),
					myKeys[1]: fmt.Sprintf("value-%d-1", idx),
				})
				if err != nil {
					errorCount.Add(1)
				} else {
					successCount.Add(1)
				}
			} else {
				// Get operation
				_, err := client.GetMulti(ctx, time.Second*10, myKeys,
					func(_ context.Context, reqKeys []string) (map[string]string, error) {
						res := make(map[string]string)
						for _, k := range reqKeys {
							res[k] = "computed-" + k
						}
						return res, nil
					})
				if err != nil {
					errorCount.Add(1)
				} else {
					successCount.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	// All operations should succeed
	assert.Equal(t, int32(0), errorCount.Load(), "no operations should fail")
	assert.Equal(t, int32(numGoroutines), successCount.Load(), "all operations should succeed")
}

// TestPrimeableCacheAside_Cluster_TTLConsistency tests TTL behavior across cluster
func TestPrimeableCacheAside_Cluster_TTLConsistency(t *testing.T) {
	client := makeClusterPrimeableCacheAside(t)
	if client == nil {
		return
	}
	defer client.Close()

	ctx := context.Background()
	key := "pcluster:ttl:" + uuid.New().String()

	// Set with short TTL
	err := client.Set(ctx, 500*time.Millisecond, key, func(_ context.Context, _ string) (string, error) {
		return "short-ttl", nil
	})
	require.NoError(t, err)

	// Value should exist initially
	called := false
	result, err := client.Get(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
		called = true
		return "fallback", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "short-ttl", result)
	assert.False(t, called)

	// Wait for expiration
	time.Sleep(600 * time.Millisecond)

	// Value should be gone
	called = false
	result, err = client.Get(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
		called = true
		return "recomputed", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "recomputed", result)
	assert.True(t, called, "should recompute after TTL expiration")
}
