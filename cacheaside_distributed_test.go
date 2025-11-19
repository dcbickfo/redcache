package redcache_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

// TestCacheAside_DistributedCoordination tests that CacheAside Get/GetMulti operations
// coordinate correctly across multiple clients
func TestCacheAside_DistributedCoordination(t *testing.T) {
	t.Run("multiple clients Get same key - only one calls callback", func(t *testing.T) {
		ctx := context.Background()
		key := "dist:get:" + uuid.New().String()

		// Create multiple clients
		numClients := 5
		clients := make([]*redcache.CacheAside, numClients)
		for i := 0; i < numClients; i++ {
			client, err := redcache.NewRedCacheAside(
				rueidis.ClientOption{InitAddress: addr},
				redcache.CacheAsideOption{LockTTL: 2 * time.Second},
			)
			require.NoError(t, err)
			defer client.Client().Close()
			clients[i] = client
		}

		// Track how many times the callback is called
		var callbackCount atomic.Int32
		var computeTime = 500 * time.Millisecond

		// All clients try to Get the same key concurrently
		var wg sync.WaitGroup
		results := make([]string, numClients)
		errors := make([]error, numClients)

		for i := 0; i < numClients; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				val, err := clients[idx].Get(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
					callbackCount.Add(1)
					// Simulate expensive computation
					time.Sleep(computeTime)
					return fmt.Sprintf("computed-value-%d", callbackCount.Load()), nil
				})
				results[idx] = val
				errors[idx] = err
			}(i)
		}

		wg.Wait()

		// Check that all operations succeeded
		for i, err := range errors {
			assert.NoError(t, err, "Client %d should not have error", i)
		}

		// Only one callback should have been called
		assert.Equal(t, int32(1), callbackCount.Load(), "Callback should only be called once across all clients")

		// All clients should get the same value
		expectedValue := "computed-value-1"
		for i, val := range results {
			assert.Equal(t, expectedValue, val, "Client %d should get the same value", i)
		}
	})

	t.Run("multiple clients GetMulti with overlapping keys", func(t *testing.T) {
		ctx := context.Background()
		key1 := "dist:multi:1:" + uuid.New().String()
		key2 := "dist:multi:2:" + uuid.New().String()
		key3 := "dist:multi:3:" + uuid.New().String()

		// Create multiple clients
		client1, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer client1.Client().Close()

		client2, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer client2.Client().Close()

		// Track callback invocations per key
		keyCallbacks := sync.Map{}

		// Client 1 gets keys 1 and 2
		// Client 2 gets keys 2 and 3
		var wg sync.WaitGroup
		var result1, result2 map[string]string
		var err1, err2 error

		wg.Add(2)
		go func() {
			defer wg.Done()
			result1, err1 = client1.GetMulti(ctx, 10*time.Second, []string{key1, key2},
				func(ctx context.Context, keys []string) (map[string]string, error) {
					values := make(map[string]string)
					for _, k := range keys {
						if count, _ := keyCallbacks.LoadOrStore(k, int32(0)); count == int32(0) {
							keyCallbacks.Store(k, int32(1))
						}
						time.Sleep(200 * time.Millisecond)       // Simulate work
						values[k] = "value-for-" + k[len(k)-36:] // Use last 36 chars (UUID)
					}
					return values, nil
				})
		}()

		go func() {
			defer wg.Done()
			// Small delay to ensure some overlap
			time.Sleep(50 * time.Millisecond)
			result2, err2 = client2.GetMulti(ctx, 10*time.Second, []string{key2, key3},
				func(ctx context.Context, keys []string) (map[string]string, error) {
					values := make(map[string]string)
					for _, k := range keys {
						if count, _ := keyCallbacks.LoadOrStore(k, int32(0)); count == int32(0) {
							keyCallbacks.Store(k, int32(1))
						}
						time.Sleep(200 * time.Millisecond)       // Simulate work
						values[k] = "value-for-" + k[len(k)-36:] // Use last 36 chars (UUID)
					}
					return values, nil
				})
		}()

		wg.Wait()

		// Both operations should succeed
		require.NoError(t, err1)
		require.NoError(t, err2)

		// Check results
		assert.Len(t, result1, 2)
		assert.Len(t, result2, 2)

		// Values should be consistent for overlapping key (key2)
		assert.Equal(t, result1[key2], result2[key2], "Both clients should see same value for key2")

		// Each key's callback should only be called once total
		callCount := 0
		keyCallbacks.Range(func(key, value interface{}) bool {
			if value.(int32) > 0 {
				callCount++
			}
			return true
		})
		assert.Equal(t, 3, callCount, "Each unique key should only be computed once")
	})

	t.Run("client invalidation propagates across clients", func(t *testing.T) {
		ctx := context.Background()
		key := "dist:invalidate:" + uuid.New().String()

		// Create two clients
		client1, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: time.Second},
		)
		require.NoError(t, err)
		defer client1.Client().Close()

		client2, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: time.Second},
		)
		require.NoError(t, err)
		defer client2.Client().Close()

		// Client 1 gets a value
		val1, err := client1.Get(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
			return "initial-value", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "initial-value", val1)

		// Client 2 gets the same value (from cache, not callback)
		callbackCalled := false
		val2, err := client2.Get(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
			callbackCalled = true
			return "should-not-be-called", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "initial-value", val2)
		assert.False(t, callbackCalled, "Client 2 should get cached value")

		// Client 1 deletes the key
		err = client1.Del(ctx, key)
		require.NoError(t, err)

		// Give invalidation time to propagate
		time.Sleep(100 * time.Millisecond)

		// Client 2 should now need to recompute
		val3, err := client2.Get(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
			return "recomputed-value", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "recomputed-value", val3)
	})

	t.Run("lock expiration handled correctly across clients", func(t *testing.T) {
		ctx := context.Background()
		key := "dist:expire:" + uuid.New().String()

		// Create client with short lock TTL
		client1, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 500 * time.Millisecond},
		)
		require.NoError(t, err)
		defer client1.Client().Close()

		client2, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 500 * time.Millisecond},
		)
		require.NoError(t, err)
		defer client2.Client().Close()

		// Client 1 starts Get but hangs in callback
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			// This will timeout and lock will expire
			timeoutCtx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer cancel()
			_, getErr := client1.Get(timeoutCtx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
				// Hang until context cancels
				<-ctx.Done()
				return "", ctx.Err()
			})
			assert.Error(t, getErr)
		}()

		// Wait for client1 to acquire lock
		time.Sleep(100 * time.Millisecond)

		// Client 2 waits, then takes over after lock expires or client1 times out
		startTime := time.Now()
		val, err := client2.Get(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
			return "client2-value", nil
		})
		duration := time.Since(startTime)

		require.NoError(t, err)
		assert.Equal(t, "client2-value", val)
		// Client 2 should wait at least 300ms (since client1's context times out at 400ms)
		// but not more than the full lock TTL
		assert.Greater(t, duration, 200*time.Millisecond, "Should wait for client1")
		assert.Less(t, duration, 700*time.Millisecond, "Should not wait full lock TTL")

		wg.Wait()
	})

	t.Run("concurrent Gets from many clients - stress test", func(t *testing.T) {
		ctx := context.Background()

		// Create many clients
		numClients := 20
		numKeys := 10
		clients := make([]*redcache.CacheAside, numClients)

		for i := 0; i < numClients; i++ {
			client, err := redcache.NewRedCacheAside(
				rueidis.ClientOption{InitAddress: addr},
				redcache.CacheAsideOption{LockTTL: 2 * time.Second},
			)
			require.NoError(t, err)
			defer client.Client().Close()
			clients[i] = client
		}

		// Generate keys
		keys := make([]string, numKeys)
		for i := 0; i < numKeys; i++ {
			keys[i] = fmt.Sprintf("dist:stress:%d:%s", i, uuid.New().String())
		}

		// Track callback counts per key
		callbackCounts := sync.Map{}

		// Each client gets random keys
		var wg sync.WaitGroup
		for clientIdx := 0; clientIdx < numClients; clientIdx++ {
			wg.Add(1)
			go func(cIdx int) {
				defer wg.Done()

				// Get 3 random keys
				selectedKeys := []string{
					keys[cIdx%numKeys],
					keys[(cIdx+3)%numKeys],
					keys[(cIdx+7)%numKeys],
				}

				vals, err := clients[cIdx].GetMulti(ctx, 10*time.Second, selectedKeys,
					func(ctx context.Context, reqKeys []string) (map[string]string, error) {
						result := make(map[string]string)
						for _, k := range reqKeys {
							// Increment callback count for this key
							count, _ := callbackCounts.LoadOrStore(k, &atomic.Int32{})
							count.(*atomic.Int32).Add(1)

							// Simulate some work
							time.Sleep(10 * time.Millisecond)
							result[k] = fmt.Sprintf("value-%s", k[len(k)-8:])
						}
						return result, nil
					})

				assert.NoError(t, err)
				assert.Len(t, vals, len(selectedKeys))
			}(clientIdx)
		}

		wg.Wait()

		// Each key should have been computed exactly once
		callbackCounts.Range(func(key, value interface{}) bool {
			count := value.(*atomic.Int32).Load()
			assert.Equal(t, int32(1), count, "Key %v should be computed exactly once, got %d", key, count)
			return true
		})
	})
}
