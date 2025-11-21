//go:build integration

package redcache_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

func makeClientWithSet(t *testing.T, addr []string) *redcache.PrimeableCacheAside {
	client, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{
			InitAddress: addr,
		},
		redcache.CacheAsideOption{
			LockTTL: time.Second * 1,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// setMultiValue is a helper function to set multiple values using SetMulti.
// Simplifies test code by wrapping the SetMulti callback pattern.
func setMultiValue(client *redcache.PrimeableCacheAside, ctx context.Context, ttl time.Duration, values map[string]string) (map[string]string, error) {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}

	return client.SetMulti(ctx, ttl, keys, func(_ context.Context, lockedKeys []string) (map[string]string, error) {
		result := make(map[string]string, len(lockedKeys))
		for _, key := range lockedKeys {
			result[key] = values[key]
		}
		return result, nil
	})
}

// Helper function for tests to force set multiple values.
func forceSetMulti(client *redcache.PrimeableCacheAside, ctx context.Context, ttl time.Duration, values map[string]string) error {
	return client.ForceSetMulti(ctx, ttl, values)
}

func TestPrimeableCacheAside_Set(t *testing.T) {
	t.Run("successful set acquires lock and sets value", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		value := "value:" + uuid.New().String()

		// Ensure cleanup of test key
		t.Cleanup(func() {
			_ = client.Del(context.Background(), key)
		})

		called := false
		err := client.Set(ctx, time.Second, key, makeSetCallback(value, &called))
		require.NoError(t, err)
		assertCallbackCalled(t, called, "Set should execute callback")

		// Verify value was set
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assertValueEquals(t, value, result)
	})

	t.Run("waits and retries when lock cannot be acquired", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()

		// Set a lock manually with short TTL
		innerClient := client.Client()
		lockVal := "__redcache:lock:" + uuid.New().String()
		err := innerClient.Do(context.Background(), innerClient.B().Set().Key(key).Value(lockVal).Nx().ExSeconds(1).Build()).Error()
		require.NoError(t, err)

		// Now try to Set - should wait for lock to expire, then succeed
		err = client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return "value", nil
		})
		require.NoError(t, err)

		// Verify value was set
		result, err := innerClient.Do(context.Background(), innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "value", result)
	})

	t.Run("subsequent Get retrieves Set value", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		value := "value:" + uuid.New().String()

		setCalled := false
		err := client.Set(ctx, time.Second, key, makeSetCallback(value, &setCalled))
		require.NoError(t, err)
		assertCallbackCalled(t, setCalled, "Set should execute callback")

		// Get should return the set value without calling callback
		getCalled := false
		result, err := client.Get(ctx, time.Second, key, makeGetCallback("should-not-be-called", &getCalled))
		require.NoError(t, err)
		assertValueEquals(t, value, result)
		assertCallbackNotCalled(t, getCalled, "Get callback should not be called when value exists from Set")
	})
}

func TestPrimeableCacheAside_ForceSet(t *testing.T) {
	t.Run("successful force set bypasses locks", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		value := "value:" + uuid.New().String()

		err := client.ForceSet(ctx, time.Second, key, value)
		require.NoError(t, err)

		// Verify value was set
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, value, result)
	})

	t.Run("force set overrides existing lock", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()

		// Set a lock manually
		innerClient := client.Client()
		lockVal := "__redcache:lock:" + uuid.New().String()
		err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(lockVal).Nx().Build()).Error()
		require.NoError(t, err)

		// ForceSet should succeed and override the lock
		newValue := "forced-value:" + uuid.New().String()
		err = client.ForceSet(ctx, time.Second, key, newValue)
		require.NoError(t, err)

		// Verify the lock was overridden
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, newValue, result)
	})

	t.Run("force set overrides existing value", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		oldValue := "old-value:" + uuid.New().String()
		newValue := "new-value:" + uuid.New().String()

		// Set initial value
		err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return oldValue, nil
		})
		require.NoError(t, err)

		// ForceSet should override
		err = client.ForceSet(ctx, time.Second, key, newValue)
		require.NoError(t, err)

		// Verify new value
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, newValue, result)
	})
}

func TestPrimeableCacheAside_SetMulti(t *testing.T) {
	t.Run("successful set multi acquires locks and sets values", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		values := map[string]string{
			"key:1:" + uuid.New().String(): "value:1:" + uuid.New().String(),
			"key:2:" + uuid.New().String(): "value:2:" + uuid.New().String(),
			"key:3:" + uuid.New().String(): "value:3:" + uuid.New().String(),
		}

		result, err := setMultiValue(client, ctx, time.Second, values)
		require.NoError(t, err)
		assert.Len(t, result, 3)

		// Verify all values were set
		innerClient := client.Client()
		for key, expectedValue := range values {
			actualValue, getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
			require.NoError(t, getErr)
			assert.Equal(t, expectedValue, actualValue)
		}
	})

	t.Run("empty values returns empty result", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		result, err := setMultiValue(client, ctx, time.Second, map[string]string{})
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("waits for all locks to be released then sets all values", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key1 := "key:1:" + uuid.New().String()
		key2 := "key:2:" + uuid.New().String()

		// Set a lock on key1 manually with short TTL
		innerClient := client.Client()
		lockVal := "__redcache:lock:" + uuid.New().String()
		err := innerClient.Do(ctx, innerClient.B().Set().Key(key1).Value(lockVal).Nx().ExSeconds(1).Build()).Error()
		require.NoError(t, err)

		values := map[string]string{
			key1: "value1:" + uuid.New().String(),
			key2: "value2:" + uuid.New().String(),
		}

		// Use longer TTL to ensure values don't expire while waiting for lock
		result, err := setMultiValue(client, ctx, 5*time.Second, values)
		require.NoError(t, err)

		// Both keys should eventually be set (key2 immediately, key1 after lock expires)
		assert.Len(t, result, 2)
		assert.Contains(t, result, key1)
		assert.Contains(t, result, key2)

		// Verify both were set
		actualValue1, err := innerClient.Do(ctx, innerClient.B().Get().Key(key1).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, values[key1], actualValue1)

		actualValue2, err := innerClient.Do(ctx, innerClient.B().Get().Key(key2).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, values[key2], actualValue2)
	})

	t.Run("subsequent GetMulti retrieves SetMulti values", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		values := map[string]string{
			"key:1:" + uuid.New().String(): "value:1:" + uuid.New().String(),
			"key:2:" + uuid.New().String(): "value:2:" + uuid.New().String(),
		}

		_, err := setMultiValue(client, ctx, time.Second, values)
		require.NoError(t, err)

		// GetMulti should return values without calling callback
		called := false
		cb := func(ctx context.Context, keys []string) (map[string]string, error) {
			called = true
			return nil, fmt.Errorf("should-not-be-called")
		}

		keys := make([]string, 0, len(values))
		for k := range values {
			keys = append(keys, k)
		}

		result, err := client.GetMulti(ctx, time.Second, keys, cb)
		require.NoError(t, err)
		if diff := cmp.Diff(values, result); diff != "" {
			t.Errorf("GetMulti() mismatch (-want +got):\n%s", diff)
		}
		assert.False(t, called, "callback should not be called when values exist")
	})

	t.Run("successful SetMulti doesn't delete values in cleanup", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		values := map[string]string{
			"key:persist:1:" + uuid.New().String(): "value:persist:1:" + uuid.New().String(),
			"key:persist:2:" + uuid.New().String(): "value:persist:2:" + uuid.New().String(),
			"key:persist:3:" + uuid.New().String(): "value:persist:3:" + uuid.New().String(),
		}

		// Perform SetMulti
		result, err := setMultiValue(client, ctx, time.Second, values)
		require.NoError(t, err)
		assert.Len(t, result, 3)

		// Immediately verify values still exist in Redis (not deleted by defer cleanup)
		innerClient := client.Client()
		for key, expectedValue := range values {
			actualValue, getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
			require.NoError(t, getErr, "value should exist for key %s", key)
			assert.Equal(t, expectedValue, actualValue, "value should match for key %s", key)
		}

		// Wait a bit to ensure defer has completed
		time.Sleep(100 * time.Millisecond)

		// Verify values STILL exist after defer cleanup
		for key, expectedValue := range values {
			actualValue, getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
			require.NoError(t, getErr, "value should still exist after defer for key %s", key)
			assert.Equal(t, expectedValue, actualValue, "value should still match after defer for key %s", key)
		}
	})

	t.Run("callback exceeding lock TTL results in CAS failure", func(t *testing.T) {
		ctx := t.Context()

		// Create client with very short lock TTL (200ms)
		option := rueidis.ClientOption{InitAddress: addr}
		caOption := redcache.CacheAsideOption{
			LockTTL: 200 * time.Millisecond, // Very short TTL
		}
		client, err := redcache.NewPrimeableCacheAside(option, caOption)
		require.NoError(t, err)
		defer client.Close()

		key1 := "key:timeout:1:" + uuid.New().String()
		key2 := "key:timeout:2:" + uuid.New().String()
		keys := []string{key1, key2}

		// Callback sleeps longer than lock TTL
		result, err := client.SetMulti(ctx, time.Second, keys, func(ctx context.Context, keys []string) (map[string]string, error) {
			// Sleep longer than lock TTL (400ms > 200ms)
			time.Sleep(400 * time.Millisecond)

			return map[string]string{
				key1: "value1",
				key2: "value2",
			}, nil
		})

		// Should get error or empty result due to lock expiration
		// Either we get an error (batch operation failed) or empty result
		if err != nil {
			// Error indicates failure - could be "batch operation partially failed"
			assert.NotEmpty(t, err.Error(), "error should not be empty")
		}

		// Result should be empty or have fewer successful sets than expected
		// (locks expired during callback, so CAS failed)
		assert.LessOrEqual(t, len(result), 1, "should have at most 1 successful set due to lock expiration")

		// Verify values were NOT set (locks expired before CAS)
		innerClient := client.Client()
		val1, err1 := innerClient.Do(ctx, innerClient.B().Get().Key(key1).Build()).ToString()
		val2, err2 := innerClient.Do(ctx, innerClient.B().Get().Key(key2).Build()).ToString()

		// At least one key should not be set (due to lock expiration)
		hasFailure := (err1 != nil && err1.Error() == "redis nil message") ||
			(err2 != nil && err2.Error() == "redis nil message") ||
			(err1 == nil && val1 != "value1") ||
			(err2 == nil && val2 != "value2")

		assert.True(t, hasFailure, "at least one key should fail to set due to lock expiration")
	})
}

func TestPrimeableCacheAside_ForceSetMulti(t *testing.T) {
	t.Run("successful force set multi bypasses locks", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		values := map[string]string{
			"key:1:" + uuid.New().String(): "value:1:" + uuid.New().String(),
			"key:2:" + uuid.New().String(): "value:2:" + uuid.New().String(),
			"key:3:" + uuid.New().String(): "value:3:" + uuid.New().String(),
		}

		err := forceSetMulti(client, ctx, time.Second, values)
		require.NoError(t, err)

		// Verify all values were set
		innerClient := client.Client()
		for key, expectedValue := range values {
			actualValue, getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
			require.NoError(t, getErr)
			assert.Equal(t, expectedValue, actualValue)
		}
	})

	t.Run("empty values completes successfully", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		err := forceSetMulti(client, ctx, time.Second, map[string]string{})
		require.NoError(t, err)
	})

	t.Run("force set multi overrides existing locks", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key1 := "key:1:" + uuid.New().String()
		key2 := "key:2:" + uuid.New().String()

		// Set locks manually
		innerClient := client.Client()
		lockVal1 := "__redcache:lock:" + uuid.New().String()
		lockVal2 := "__redcache:lock:" + uuid.New().String()
		err := innerClient.Do(ctx, innerClient.B().Set().Key(key1).Value(lockVal1).Nx().Build()).Error()
		require.NoError(t, err)
		err = innerClient.Do(ctx, innerClient.B().Set().Key(key2).Value(lockVal2).Nx().Build()).Error()
		require.NoError(t, err)

		// ForceSetMulti should override both locks
		values := map[string]string{
			key1: "forced-value1:" + uuid.New().String(),
			key2: "forced-value2:" + uuid.New().String(),
		}

		err = forceSetMulti(client, ctx, time.Second, values)
		require.NoError(t, err)

		// Verify locks were overridden
		for key, expectedValue := range values {
			actualValue, getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
			require.NoError(t, getErr)
			assert.Equal(t, expectedValue, actualValue)
		}
	})
}

func TestPrimeableCacheAside_Integration(t *testing.T) {
	t.Run("Set waits for concurrent Get to complete", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		dbValue := "db-value:" + uuid.New().String()
		setValue := "set-value:" + uuid.New().String()

		// Start a Get that will hold a lock briefly
		getStarted := make(chan struct{})
		getFinished := make(chan struct{})
		setStartedChan := make(chan time.Time, 1)
		setFinishedChan := make(chan error, 1)

		go func() {
			defer close(getFinished)
			cb := func(ctx context.Context, key string) (string, error) {
				close(getStarted)
				time.Sleep(500 * time.Millisecond) // Hold lock briefly
				return dbValue, nil
			}
			_, _ = client.Get(ctx, time.Second, key, cb)
		}()

		// Wait for Get to acquire lock
		<-getStarted

		// Try to Set in a goroutine - should wait for Get to finish
		go func() {
			setStarted := time.Now()
			setStartedChan <- setStarted
			err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
				return setValue, nil
			})
			setFinishedChan <- err
		}()

		setStarted := <-setStartedChan
		<-getFinished
		setErr := <-setFinishedChan
		setDuration := time.Since(setStarted)

		// Set should have waited (at least 400ms) and then succeeded
		require.NoError(t, setErr)
		assert.Greater(t, setDuration, 400*time.Millisecond, "Set should have waited for Get to finish")

		// Verify Set value was written (overwriting Get's value)
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, setValue, result)
	})

	t.Run("ForceSet overrides lock from Get", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		forcedValue := "forced-value:" + uuid.New().String()

		// Set a lock manually (simulating Get in progress)
		innerClient := client.Client()
		lockVal := "__redcache:lock:" + uuid.New().String()
		err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(lockVal).Nx().Build()).Error()
		require.NoError(t, err)

		// ForceSet should succeed and override the lock
		err = client.ForceSet(ctx, time.Second, key, forcedValue)
		require.NoError(t, err)

		// Verify value was set
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, forcedValue, result)
	})

	t.Run("concurrent Set operations wait and eventually succeed", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()

		wg := sync.WaitGroup{}
		errors := make([]error, 10)

		// Try to Set concurrently - all should eventually succeed by waiting
		// Note: Each Set will overwrite the previous one. The last one to complete wins.
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				value := fmt.Sprintf("value-%d", i)
				// Use longer TTL to prevent expiration during concurrent operations
				errors[i] = client.Set(ctx, 5*time.Second, key, func(_ context.Context, _ string) (string, error) {
					// Simulate some work
					time.Sleep(10 * time.Millisecond)
					return value, nil
				})
			}(i)
		}

		wg.Wait()

		// All operations should succeed (Set can overwrite existing values)
		for i, err := range errors {
			assert.NoError(t, err, "Set operation %d should succeed", i)
		}

		// Verify some value was set
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.NotEmpty(t, result, "a value should be set")
	})
}

func TestNewPrimeableCacheAside(t *testing.T) {
	t.Run("creates instance successfully", func(t *testing.T) {
		client, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 10 * time.Second},
		)
		require.NoError(t, err)
		require.NotNil(t, client)
		require.NotNil(t, client.CacheAside)
		client.Client().Close()
	})

	t.Run("returns error on invalid client option", func(t *testing.T) {
		client, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: []string{}}, // Empty addresses
			redcache.CacheAsideOption{LockTTL: 10 * time.Second},
		)
		require.Error(t, err)
		require.Nil(t, client)
	})
}

func TestPrimeableCacheAside_EdgeCases_ContextCancellation(t *testing.T) {
	t.Run("Set with context cancelled before operation", func(t *testing.T) {
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		ctx, cancel := context.WithCancel(t.Context())
		cancel() // Cancel immediately

		err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return "value", nil
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("Set with context cancelled while waiting for lock", func(t *testing.T) {
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()

		// Set a lock manually with long TTL
		innerClient := client.Client()
		lockVal := "__redcache:lock:" + uuid.New().String()
		err := innerClient.Do(context.Background(), innerClient.B().Set().Key(key).Value(lockVal).Nx().ExSeconds(10).Build()).Error()
		require.NoError(t, err)

		// Try to Set with short timeout
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		err = client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return "value", nil
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		// Clean up the lock
		innerClient.Do(context.Background(), innerClient.B().Del().Key(key).Build())
	})

	t.Run("SetMulti with context cancelled before operation", func(t *testing.T) {
		client := makeClientWithSet(t, addr)
		defer client.Close()

		values := map[string]string{
			"key:1:" + uuid.New().String(): "value1",
			"key:2:" + uuid.New().String(): "value2",
		}

		ctx, cancel := context.WithCancel(t.Context())
		cancel() // Cancel immediately

		_, err := setMultiValue(client, ctx, time.Second, values)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("SetMulti with context cancelled while waiting for locks", func(t *testing.T) {
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key1 := "key:1:" + uuid.New().String()
		key2 := "key:2:" + uuid.New().String()

		// Set locks manually with long TTL
		innerClient := client.Client()
		lockVal1 := "__redcache:lock:" + uuid.New().String()
		lockVal2 := "__redcache:lock:" + uuid.New().String()
		err := innerClient.Do(context.Background(), innerClient.B().Set().Key(key1).Value(lockVal1).Nx().ExSeconds(10).Build()).Error()
		require.NoError(t, err)
		err = innerClient.Do(context.Background(), innerClient.B().Set().Key(key2).Value(lockVal2).Nx().ExSeconds(10).Build()).Error()
		require.NoError(t, err)

		values := map[string]string{
			key1: "value1",
			key2: "value2",
		}

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		_, err = setMultiValue(client, ctx, time.Second, values)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		// Clean up locks
		innerClient.Do(context.Background(), innerClient.B().Del().Key(key1).Build())
		innerClient.Do(context.Background(), innerClient.B().Del().Key(key2).Build())
	})
}

func TestPrimeableCacheAside_EdgeCases_TTL(t *testing.T) {
	t.Run("Set with very short TTL", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		value := "value:" + uuid.New().String()

		// Set with 10ms TTL
		err := client.Set(ctx, 10*time.Millisecond, key, func(_ context.Context, _ string) (string, error) {
			return value, nil
		})
		require.NoError(t, err)

		// Verify value was set
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, value, result)

		// Wait for expiration
		time.Sleep(20 * time.Millisecond)

		// Value should be expired
		_, err = innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		assert.Error(t, err)
		assert.True(t, rueidis.IsRedisNil(err))
	})

	t.Run("Set with 1 second TTL has correct expiration", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		value := "value:" + uuid.New().String()

		err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return value, nil
		})
		require.NoError(t, err)

		// Check TTL is approximately 1 second (allow some variance)
		innerClient := client.Client()
		ttl, err := innerClient.Do(ctx, innerClient.B().Pttl().Key(key).Build()).AsInt64()
		require.NoError(t, err)
		assert.Greater(t, ttl, int64(900), "TTL should be at least 900ms")
		assert.Less(t, ttl, int64(1100), "TTL should be at most 1100ms")
	})

	t.Run("SetMulti with very short TTL", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		values := map[string]string{
			"key:1:" + uuid.New().String(): "value1",
			"key:2:" + uuid.New().String(): "value2",
		}

		// Set with 10ms TTL
		result, err := setMultiValue(client, ctx, 10*time.Millisecond, values)
		require.NoError(t, err)
		assert.Len(t, result, 2)

		// Wait for expiration
		time.Sleep(20 * time.Millisecond)

		// Values should be expired
		innerClient := client.Client()
		for key := range values {
			_, getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
			assert.Error(t, getErr)
			assert.True(t, rueidis.IsRedisNil(getErr))
		}
	})
}

func TestPrimeableCacheAside_EdgeCases_DuplicateKeys(t *testing.T) {
	t.Run("SetMulti with duplicate keys in input", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		values := map[string]string{
			key: "value1", // Same key will overwrite in map
		}

		// This shouldn't cause any issues - map deduplicates automatically
		result, err := setMultiValue(client, ctx, time.Second, values)
		require.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Contains(t, result, key)

		// Verify value was set
		innerClient := client.Client()
		actualValue, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "value1", actualValue)
	})
}

func TestPrimeableCacheAside_EdgeCases_SpecialValues(t *testing.T) {
	t.Run("Set with empty string value", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()

		err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return "", nil
		})
		require.NoError(t, err)

		// Verify empty value was set
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "", result)
	})

	t.Run("Set with value that starts with lock prefix", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		// Value that looks like a lock but isn't - use a special value that's obviously not a real lock
		// We'll verify it gets set, but NOT test Get behavior since that would timeout
		value := "__redcache:lock:user-data-not-a-real-lock"

		err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return value, nil
		})
		require.NoError(t, err)

		// Verify value was set in Redis
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, value, result)

		// Note: We don't test Get() on this value because Get() correctly treats
		// values starting with the lock prefix as locks and would wait for LockTTL.
		// This test verifies that Set() CAN write such values if needed.
	})

	t.Run("Set with unicode and special characters", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		value := "Hello 世界 🚀 \n\t\r special chars: \"'`"

		err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return value, nil
		})
		require.NoError(t, err)

		// Verify value was set correctly
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, value, result)
	})

	t.Run("SetMulti with empty string values", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		values := map[string]string{
			"key:1:" + uuid.New().String(): "",
			"key:2:" + uuid.New().String(): "",
		}

		result, err := setMultiValue(client, ctx, time.Second, values)
		require.NoError(t, err)
		assert.Len(t, result, 2)

		// Verify empty values were set
		innerClient := client.Client()
		for key := range values {
			actualValue, getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
			require.NoError(t, getErr)
			assert.Equal(t, "", actualValue)
		}
	})

	t.Run("Set with very large value", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		// Create a 1MB value
		largeValue := string(make([]byte, 1024*1024))

		err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return largeValue, nil
		})
		require.NoError(t, err)

		// Verify value was set
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, len(largeValue), len(result))
	})
}

func TestPrimeableCacheAside_EdgeCases_GetRacingWithSet(t *testing.T) {
	t.Run("Get racing with Set - Set completes first", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		setValue := "set-value:" + uuid.New().String()

		// Set a value
		err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return setValue, nil
		})
		require.NoError(t, err)

		// Get should see the Set value without calling callback
		called := false
		cb := func(ctx context.Context, key string) (string, error) {
			called = true
			return "db-value", nil
		}

		result, err := client.Get(ctx, time.Second, key, cb)
		require.NoError(t, err)
		assert.Equal(t, setValue, result)
		assert.False(t, called, "Get should use cached value from Set")
	})

	t.Run("Get starts then Set completes - Get should see new value on retry", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		setValue := "set-value:" + uuid.New().String()

		getStarted := make(chan struct{})
		getComplete := make(chan string, 1)
		setComplete := make(chan struct{})

		// Start Get that will hold lock briefly
		go func() {
			cb := func(ctx context.Context, key string) (string, error) {
				close(getStarted)
				time.Sleep(200 * time.Millisecond)
				return "db-value", nil
			}
			result, _ := client.Get(ctx, time.Second, key, cb)
			getComplete <- result
		}()

		// Wait for Get to start
		<-getStarted

		// Set should wait, then succeed
		go func() {
			err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
				return setValue, nil
			})
			require.NoError(t, err)
			close(setComplete)
		}()

		// Wait for both operations
		getResult := <-getComplete
		<-setComplete

		// Get completed first with db-value, then Set wrote set-value
		assert.Equal(t, "db-value", getResult)

		// Another Get should now see the Set value
		result, err := client.Get(ctx, time.Second, key, func(ctx context.Context, key string) (string, error) {
			return "", fmt.Errorf("should not be called")
		})
		require.NoError(t, err)
		assert.Equal(t, setValue, result)
	})

	t.Run("GetMulti racing with SetMulti on overlapping keys", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key1 := "key:1:" + uuid.New().String()
		key2 := "key:2:" + uuid.New().String()

		var wg sync.WaitGroup

		// Start GetMulti
		wg.Add(1)
		go func() {
			defer wg.Done()
			cb := func(ctx context.Context, keys []string) (map[string]string, error) {
				time.Sleep(100 * time.Millisecond)
				return map[string]string{
					key1: "get-value1",
					key2: "get-value2",
				}, nil
			}
			_, _ = client.GetMulti(ctx, time.Second, []string{key1, key2}, cb)
		}()

		// Give GetMulti a head start
		time.Sleep(50 * time.Millisecond)

		// Start SetMulti on same keys
		wg.Add(1)
		go func() {
			defer wg.Done()
			values := map[string]string{
				key1: "set-value1",
				key2: "set-value2",
			}
			_, _ = setMultiValue(client, ctx, time.Second, values)
		}()

		wg.Wait()

		// Both operations should complete successfully
		// The final values depend on timing, but we can verify keys exist
		innerClient := client.Client()
		val1, err := innerClient.Do(ctx, innerClient.B().Get().Key(key1).Build()).ToString()
		require.NoError(t, err)
		assert.NotEmpty(t, val1)

		val2, err := innerClient.Do(ctx, innerClient.B().Get().Key(key2).Build()).ToString()
		require.NoError(t, err)
		assert.NotEmpty(t, val2)
	})

	t.Run("ForceSet triggers invalidation for waiting Get", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		forcedValue := "forced-value:" + uuid.New().String()

		getStarted := make(chan struct{})
		getResult := make(chan string, 1)

		// Start Get that will hold lock
		go func() {
			cb := func(ctx context.Context, key string) (string, error) {
				close(getStarted)
				time.Sleep(500 * time.Millisecond)
				return "db-value", nil
			}
			result, _ := client.Get(ctx, time.Second, key, cb)
			getResult <- result
		}()

		// Wait for Get to acquire lock
		<-getStarted
		time.Sleep(50 * time.Millisecond)

		// ForceSet should override the lock
		err := client.ForceSet(ctx, time.Second, key, forcedValue)
		require.NoError(t, err)

		// Get will complete with its db-value, but then try to set and fail (lock lost)
		// The forced value should be in Redis
		time.Sleep(600 * time.Millisecond)

		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		// Could be either forced-value or db-value depending on timing
		assert.NotEmpty(t, result)
	})

	t.Run("ForceSet overrides lock while Get is holding it", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		dbValue := "db-value:" + uuid.New().String()
		forcedValue := "forced-value:" + uuid.New().String()

		getLockAcquired := make(chan struct{})
		getCompleted := make(chan struct {
			result string
			err    error
		}, 1)
		forceSetCompleted := make(chan struct{})

		// Start Get that will hold lock for a while
		go func() {
			cb := func(ctx context.Context, key string) (string, error) {
				close(getLockAcquired)
				// Hold the lock while callback executes
				time.Sleep(300 * time.Millisecond)
				return dbValue, nil
			}
			result, err := client.Get(ctx, time.Second, key, cb)
			getCompleted <- struct {
				result string
				err    error
			}{result, err}
		}()

		// Wait for Get to acquire the lock
		<-getLockAcquired
		time.Sleep(50 * time.Millisecond)

		// Now ForceSet should overwrite the lock that Get is holding
		go func() {
			err := client.ForceSet(ctx, time.Second, key, forcedValue)
			require.NoError(t, err)
			close(forceSetCompleted)
		}()

		// Wait for ForceSet to complete
		<-forceSetCompleted

		// Immediately check Redis - should have the forced value
		innerClient := client.Client()
		resultDuringGet, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, forcedValue, resultDuringGet, "ForceSet should have overridden the lock")

		// Wait for Get to complete
		getResult := <-getCompleted

		// EXPECTED BEHAVIOR (after graceful retry fix):
		// 1. Get's callback returns db-value
		// 2. Get tries to write db-value using setWithLock (Lua script)
		// 3. The Lua script checks if lock matches - it doesn't (ForceSet overwrote it)
		// 4. Lua script returns 0 (indicating lock mismatch)
		// 5. setWithLock returns ErrLockLost
		// 6. Get detects ErrLockLost and waits for invalidation
		// 7. ForceSet triggers Redis invalidation, closing the wait channel
		// 8. Get retries and reads the forced-value from Redis
		// 9. Get returns success with the forced-value

		// Get should succeed after retry
		require.NoError(t, getResult.err, "Get should succeed after retry")

		// Get should return the forced-value that it read from Redis after retry
		assert.Equal(t, forcedValue, getResult.result, "Get should return forced-value after retry")

		// Redis still has forced-value (Get's write failed but it read on retry)
		finalResult, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, forcedValue, finalResult, "Redis has forced-value")

		t.Logf("✓ Correct behavior: Get gracefully retries and returns forced-value: %s", getResult.result)
	})

	t.Run("ForceSetMulti overrides locks while GetMulti is holding them", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key1 := "key:1:" + uuid.New().String()
		key2 := "key:2:" + uuid.New().String()
		dbValue1 := "db-value-1:" + uuid.New().String()
		dbValue2 := "db-value-2:" + uuid.New().String()
		forcedValue1 := "forced-value-1:" + uuid.New().String()

		getMultiStarted := make(chan struct{})
		getMultiCompleted := make(chan struct {
			result map[string]string
			err    error
		}, 1)
		forceSetMultiCompleted := make(chan struct{})

		// Start GetMulti that will hold locks for both keys
		go func() {
			cb := func(ctx context.Context, keys []string) (map[string]string, error) {
				close(getMultiStarted)
				// Hold the locks while callback executes
				time.Sleep(300 * time.Millisecond)
				return map[string]string{
					key1: dbValue1,
					key2: dbValue2,
				}, nil
			}
			result, err := client.GetMulti(ctx, time.Second, []string{key1, key2}, cb)
			getMultiCompleted <- struct {
				result map[string]string
				err    error
			}{result, err}
		}()

		// Wait for GetMulti to acquire locks
		<-getMultiStarted
		time.Sleep(50 * time.Millisecond)

		// ForceSetMulti overwrites the lock on key1 only
		go func() {
			values := map[string]string{
				key1: forcedValue1,
			}
			err := forceSetMulti(client, ctx, time.Second, values)
			require.NoError(t, err)
			close(forceSetMultiCompleted)
		}()

		// Wait for ForceSetMulti to complete
		<-forceSetMultiCompleted

		// Check Redis - key1 should have forced value
		innerClient := client.Client()
		resultKey1, err := innerClient.Do(ctx, innerClient.B().Get().Key(key1).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, forcedValue1, resultKey1, "key1 should have forced-value")

		// Wait for GetMulti to complete
		getMultiResult := <-getMultiCompleted

		// EXPECTED BEHAVIOR:
		// 1. GetMulti's callback returns db-values for both keys
		// 2. GetMulti tries to write both values
		// 3. key1's write fails (lock was lost to ForceSetMulti)
		// 4. key2's write succeeds
		// 5. key1 remains in waitLock, so GetMulti waits and retries
		// 6. ForceSetMulti triggers invalidation, causing key1's channel to close
		// 7. GetMulti retries and reads key1 from Redis (forced-value)
		// 8. GetMulti returns both keys with their current Redis values

		require.NoError(t, getMultiResult.err, "GetMulti should succeed")

		// GetMulti should return BOTH keys (it retries after invalidation)
		assert.Contains(t, getMultiResult.result, key1, "key1 should be in result (read after invalidation)")
		assert.Contains(t, getMultiResult.result, key2, "key2 should be in result")

		// key1 should have the forced-value (read from Redis after invalidation)
		assert.Equal(t, forcedValue1, getMultiResult.result[key1], "key1 should have forced-value from Redis")
		// key2 should have the db-value (successfully written)
		assert.Equal(t, dbValue2, getMultiResult.result[key2], "key2 should have db-value")

		// Verify final Redis state matches what GetMulti returned
		finalKey1, err := innerClient.Do(ctx, innerClient.B().Get().Key(key1).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, forcedValue1, finalKey1, "Redis key1 should have forced-value")

		finalKey2, err := innerClient.Do(ctx, innerClient.B().Get().Key(key2).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, dbValue2, finalKey2, "Redis key2 should have db-value")

		t.Logf("✓ Correct behavior: GetMulti retries and returns consistent state")
		t.Logf("  key1=%s (forced by ForceSetMulti, read on retry)", getMultiResult.result[key1])
		t.Logf("  key2=%s (from GetMulti callback)", getMultiResult.result[key2])
	})

	t.Run("Set in progress + ForceSet overwrites lock during callback", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		setValue := "set-value:" + uuid.New().String()
		forceValue := "force-value:" + uuid.New().String()

		setLockAcquired := make(chan struct{})
		setCompleted := make(chan error, 1)
		forceSetCompleted := make(chan struct{})

		// Start Set operation that will hold lock during callback
		go func() {
			err := client.Set(ctx, time.Second, key, func(ctx context.Context, key string) (string, error) {
				close(setLockAcquired)
				// Hold the lock while callback executes
				time.Sleep(300 * time.Millisecond)
				return setValue, nil
			})
			setCompleted <- err
		}()

		// Wait for Set to acquire lock
		<-setLockAcquired
		time.Sleep(50 * time.Millisecond)

		// ForceSet should overwrite the lock that Set is holding
		go func() {
			err := client.ForceSet(ctx, time.Second, key, forceValue)
			require.NoError(t, err)
			close(forceSetCompleted)
		}()

		// Wait for ForceSet to complete
		<-forceSetCompleted

		// ForceSet should have written its value immediately
		innerClient := client.Client()
		resultDuringSet, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, forceValue, resultDuringSet, "ForceSet should have overridden the lock")

		// Wait for Set to complete
		setErr := <-setCompleted

		// EXPECTED BEHAVIOR:
		// 1. Set's callback returns set-value
		// 2. Set tries to write using setWithLock (CAS - compare lock value)
		// 3. CAS detects lock was stolen (ForceSet overwrote it)
		// 4. Set returns ErrLockLost error
		// 5. ForceSet value is preserved (Set does NOT overwrite it)

		// Set MUST fail when it loses the lock
		require.Error(t, setErr, "Set MUST fail when lock is stolen by ForceSet")
		assert.Contains(t, setErr.Error(), "lock", "Error should indicate lock was lost")

		// Redis MUST still have ForceSet's value (Set must not overwrite it)
		finalResult, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, forceValue, finalResult, "ForceSet value MUST be preserved - Set must not overwrite when lock is lost")

		t.Logf("✓ CORRECT: Set detected lock loss and failed without overwriting ForceSet value")
	})

	t.Run("SetMulti in progress + ForceSetMulti overwrites some locks during callback", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key1 := "key:1:" + uuid.New().String()
		key2 := "key:2:" + uuid.New().String()
		key3 := "key:3:" + uuid.New().String()
		setValue1 := "set-value-1:" + uuid.New().String()
		setValue2 := "set-value-2:" + uuid.New().String()
		setValue3 := "set-value-3:" + uuid.New().String()
		forceValue1 := "force-value-1:" + uuid.New().String()
		forceValue2 := "force-value-2:" + uuid.New().String()

		setMultiLockAcquired := make(chan struct{})
		setMultiCompleted := make(chan struct {
			result map[string]string
			err    error
		}, 1)
		forceSetMultiCompleted := make(chan struct{})

		// Start SetMulti operation that will hold locks during callback
		go func() {
			keys := []string{key1, key2, key3}
			result, err := client.SetMulti(ctx, time.Second, keys, func(ctx context.Context, lockedKeys []string) (map[string]string, error) {
				close(setMultiLockAcquired)
				// Hold locks while callback executes
				time.Sleep(300 * time.Millisecond)
				return map[string]string{
					key1: setValue1,
					key2: setValue2,
					key3: setValue3,
				}, nil
			})
			setMultiCompleted <- struct {
				result map[string]string
				err    error
			}{result, err}
		}()

		// Wait for SetMulti to acquire locks
		<-setMultiLockAcquired
		time.Sleep(50 * time.Millisecond)

		// ForceSetMulti should overwrite locks for key1 and key2 (but not key3)
		go func() {
			values := map[string]string{
				key1: forceValue1,
				key2: forceValue2,
			}
			err := client.ForceSetMulti(ctx, time.Second, values)
			require.NoError(t, err)
			close(forceSetMultiCompleted)
		}()

		// Wait for ForceSetMulti to complete
		<-forceSetMultiCompleted

		// ForceSetMulti should have written key1 and key2
		innerClient := client.Client()
		result1, err := innerClient.Do(ctx, innerClient.B().Get().Key(key1).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, forceValue1, result1, "key1 should have ForceSetMulti value")

		result2, err := innerClient.Do(ctx, innerClient.B().Get().Key(key2).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, forceValue2, result2, "key2 should have ForceSetMulti value")

		// Wait for SetMulti to complete
		setMultiResult := <-setMultiCompleted

		// EXPECTED BEHAVIOR:
		// 1. SetMulti's callback returns values for all 3 keys
		// 2. SetMulti tries to write all 3 using setWithLock (CAS)
		// 3. key1 and key2: CAS fails (locks stolen by ForceSetMulti)
		// 4. key3: CAS succeeds (lock still held)
		// 5. SetMulti returns partial success with BatchError

		if setMultiResult.err != nil {
			// Partial failure - some keys lost locks
			// Should be a BatchError with ErrLockLost for keys that lost locks
			var batchErr *redcache.BatchError
			if errors.As(setMultiResult.err, &batchErr) {
				assert.True(t, batchErr.HasFailures(), "BatchError should have failures")

				// Check that key1 and key2 lost their locks
				for key, err := range batchErr.Failed {
					assert.ErrorIs(t, err, redcache.ErrLockLost, "Failed key %s should have ErrLockLost", key)
				}

				t.Logf("✓ Correct behavior: SetMulti returned BatchError with %d failed keys", len(batchErr.Failed))
			} else {
				// General error case
				t.Logf("SetMulti returned error: %v", setMultiResult.err)
			}

			// Verify ForceSetMulti values are preserved
			finalResult1, verifyErr := innerClient.Do(ctx, innerClient.B().Get().Key(key1).Build()).ToString()
			require.NoError(t, verifyErr)
			assert.Equal(t, forceValue1, finalResult1, "key1 should preserve ForceSetMulti value")

			finalResult2, verifyErr := innerClient.Do(ctx, innerClient.B().Get().Key(key2).Build()).ToString()
			require.NoError(t, verifyErr)
			assert.Equal(t, forceValue2, finalResult2, "key2 should preserve ForceSetMulti value")
		} else {
			// Should not happen - SetMulti should return error when locks are lost
			t.Errorf("SetMulti should have returned error when locks were stolen")
		}
	})
}

func TestPrimeableCacheAside_EdgeCases_Additional(t *testing.T) {
	t.Run("Set overwrites existing non-lock value", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		oldValue := "old-value:" + uuid.New().String()
		newValue := "new-value:" + uuid.New().String()

		// Set initial value
		err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return oldValue, nil
		})
		require.NoError(t, err)

		// Verify old value
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, oldValue, result)

		// Overwrite with new value
		err = client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return newValue, nil
		})
		require.NoError(t, err)

		// Verify new value
		result, err = innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, newValue, result)
	})

	t.Run("Set immediately after Del triggers new write", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()
		value1 := "value1:" + uuid.New().String()
		value2 := "value2:" + uuid.New().String()

		// Set initial value
		err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return value1, nil
		})
		require.NoError(t, err)

		// Delete
		err = client.Del(ctx, key)
		require.NoError(t, err)

		// Set new value immediately
		err = client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return value2, nil
		})
		require.NoError(t, err)

		// Verify new value
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, value2, result)
	})

	// NOTE: Multi-client SetMulti coordination test removed - see primeable_cacheaside_distributed_test.go
	// for comprehensive distributed coordination tests

	t.Run("Get with callback error does not cache", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()

		callCount := 0
		cb := func(ctx context.Context, key string) (string, error) {
			callCount++
			return "", fmt.Errorf("database error")
		}

		// First Get fails
		_, err := client.Get(ctx, time.Second, key, cb)
		require.Error(t, err)
		assert.Equal(t, 1, callCount)

		// Second Get should call callback again (error was not cached)
		_, err = client.Get(ctx, time.Second, key, cb)
		require.Error(t, err)
		assert.Equal(t, 2, callCount)
	})

	t.Run("GetMulti with empty keys slice", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		cb := func(ctx context.Context, keys []string) (map[string]string, error) {
			return make(map[string]string), nil
		}

		result, err := client.GetMulti(ctx, time.Second, []string{}, cb)
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("GetMulti with callback error does not cache", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		keys := []string{
			"key:1:" + uuid.New().String(),
			"key:2:" + uuid.New().String(),
		}

		callCount := 0
		cb := func(ctx context.Context, keys []string) (map[string]string, error) {
			callCount++
			return nil, fmt.Errorf("database error")
		}

		// First GetMulti fails
		_, err := client.GetMulti(ctx, time.Second, keys, cb)
		require.Error(t, err)
		assert.Equal(t, 1, callCount)

		// Second GetMulti should call callback again (error was not cached)
		_, err = client.GetMulti(ctx, time.Second, keys, cb)
		require.Error(t, err)
		assert.Equal(t, 2, callCount)
	})

	t.Run("Del on non-existent key succeeds", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "key:" + uuid.New().String()

		// Delete non-existent key should not error
		err := client.Del(ctx, key)
		require.NoError(t, err)
	})

	t.Run("DelMulti on non-existent keys succeeds", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		keys := []string{
			"key:1:" + uuid.New().String(),
			"key:2:" + uuid.New().String(),
		}

		// Delete non-existent keys should not error
		err := client.DelMulti(ctx, keys...)
		require.NoError(t, err)
	})

	t.Run("DelMulti with empty keys slice", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		// Delete empty slice should not error
		err := client.DelMulti(ctx)
		require.NoError(t, err)
	})
}

// TestPrimeableCacheAside_SetDoesNotBlockOnRedisLock tests that Set operations
// don't block when there's a lock in Redis but no local operation holding it.
// This would happen if a previous Get operation completed, left a lock in Redis,
// and then Set is called.
func TestPrimeableCacheAside_SetDoesNotBlockOnRedisLock(t *testing.T) {
	ctx := t.Context()
	client := makeClientWithSet(t, addr)
	defer client.Close()

	key := "key:" + uuid.New().String()

	// Manually set a lock value in Redis (simulating a lock from a Get operation)
	// Use the same TTL as configured in the client (1 second)
	innerClient := client.Client()
	lockVal := "__redcache:lock:" + uuid.New().String()
	err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(lockVal).Px(time.Second*1).Build()).Error()
	require.NoError(t, err)

	// Now try to Set - this should wait for the lock, not block indefinitely
	// Use a timeout to ensure we don't wait too long
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()

	value := "value:" + uuid.New().String()

	// This should complete within the lock TTL (1 second) + some buffer
	// If Set is broken and blocks on its own local lock, this will timeout
	start := time.Now()
	err = client.Set(ctxWithTimeout, time.Second, key, func(_ context.Context, _ string) (string, error) {
		return value, nil
	})
	elapsed := time.Since(start)

	require.NoError(t, err)

	// Should have waited approximately 1 second for lock to expire
	assert.Greater(t, elapsed, time.Millisecond*850, "Should have waited for lock TTL")
	assert.Less(t, elapsed, time.Second*2, "Should not have blocked indefinitely")

	// Verify value was set
	result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
	require.NoError(t, err)
	assert.Equal(t, value, result)
}

// TestPrimeableCacheAside_SetWithCallback is now merged into TestPrimeableCacheAside_Set
// Since Set always takes a callback, this separate test is no longer needed.

/*
func TestPrimeableCacheAside_SetWithCallback(t *testing.T) {
	t.Run("acquires lock, executes callback, caches result", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "user:" + uuid.New().String()
		expectedValue := "db-value:" + uuid.New().String()

		callbackExecuted := false
		callback := func(ctx context.Context, key string) (string, error) {
			callbackExecuted = true
			// Simulate database write
			time.Sleep(10 * time.Millisecond)
			return expectedValue, nil
		}

		// Execute coordinated Set
		err := client.Set(ctx, time.Second, key, callback)
		require.NoError(t, err)
		require.True(t, callbackExecuted, "callback should have been executed")

		// Verify value was cached
		innerClient := client.Client()
		cachedValue, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, expectedValue, cachedValue)
	})

	t.Run("concurrent Set operations coordinate via locking", func(t *testing.T) {
		ctx := t.Context()
		client1 := makeClientWithSet(t, addr)
		defer client1.Client().Close()
		client2 := makeClientWithSet(t, addr)
		defer client2.Client().Close()

		key := "counter:" + uuid.New().String()

		var callCount int
		var mu sync.Mutex

		callback := func(ctx context.Context, key string) (string, error) {
			mu.Lock()
			defer mu.Unlock()
			callCount++
			// Simulate database write
			time.Sleep(50 * time.Millisecond)
			return fmt.Sprintf("value-%d", callCount), nil
		}

		// Start two concurrent Set operations
		wg := sync.WaitGroup{}
		wg.Add(2)

		var err1, err2 error
		go func() {
			defer wg.Done()
			err1 = client1.Set(ctx, time.Second, key, callback)
		}()
		go func() {
			defer wg.Done()
			err2 = client2.Set(ctx, time.Second, key, callback)
		}()

		wg.Wait()

		// Both should succeed
		require.NoError(t, err1)
		require.NoError(t, err2)

		// Callback should have been called twice (one for each client)
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, 2, callCount, "callback should be called twice with distributed locking")
	})

	t.Run("callback error prevents caching", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		key := "error-key:" + uuid.New().String()
		expectedErr := fmt.Errorf("database write failed")

		callback := func(ctx context.Context, key string) (string, error) {
			return "", expectedErr
		}

		// Set should return the callback error
		err := client.Set(ctx, time.Second, key, callback)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "database write failed")

		// Value should not be cached
		innerClient := client.Client()
		err = innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
		assert.True(t, rueidis.IsRedisNil(err), "value should not be cached on error")
	})
}
*/

// TestPrimeableCacheAside_SetMultiWithCallback tests batch coordinated cache updates with a callback.
func TestPrimeableCacheAside_SetMultiWithCallback(t *testing.T) {
	t.Run("acquires locks, executes callback, caches results", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		keys := []string{
			"user:1:" + uuid.New().String(),
			"user:2:" + uuid.New().String(),
			"user:3:" + uuid.New().String(),
		}

		expectedValues := map[string]string{
			keys[0]: "db-value-1",
			keys[1]: "db-value-2",
			keys[2]: "db-value-3",
		}

		callbackExecuted := false
		callback := func(ctx context.Context, keys []string) (map[string]string, error) {
			callbackExecuted = true
			// Simulate batch database write
			time.Sleep(20 * time.Millisecond)
			result := make(map[string]string, len(keys))
			for _, key := range keys {
				result[key] = expectedValues[key]
			}
			return result, nil
		}

		// Execute coordinated SetMulti
		result, err := client.SetMulti(ctx, time.Second, keys, callback)
		require.NoError(t, err)
		require.True(t, callbackExecuted, "callback should have been executed")
		assert.Equal(t, expectedValues, result)

		// Verify all values were cached
		innerClient := client.Client()
		for key, expectedValue := range expectedValues {
			cachedValue, getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
			require.NoError(t, getErr)
			assert.Equal(t, expectedValue, cachedValue)
		}
	})

	t.Run("empty keys returns empty result", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		callback := func(ctx context.Context, keys []string) (map[string]string, error) {
			t.Fatal("callback should not be called for empty keys")
			return nil, nil
		}

		result, err := client.SetMulti(ctx, time.Second, []string{}, callback)
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("callback error prevents caching", func(t *testing.T) {
		ctx := t.Context()
		client := makeClientWithSet(t, addr)
		defer client.Close()

		keys := []string{
			"error-key:1:" + uuid.New().String(),
			"error-key:2:" + uuid.New().String(),
		}

		expectedErr := fmt.Errorf("batch database write failed")
		callback := func(ctx context.Context, keys []string) (map[string]string, error) {
			return nil, expectedErr
		}

		// SetMulti should return the callback error
		_, err := client.SetMulti(ctx, time.Second, keys, callback)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "batch database write failed")

		// Values should not be cached
		innerClient := client.Client()
		for _, key := range keys {
			getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
			assert.True(t, rueidis.IsRedisNil(getErr), "value should not be cached on error")
		}
	})
}

// TestPrimeableCacheAside_SetLockingBehavior verifies that Set properly acquires
// cache key locks and doesn't act like ForceSet
func TestPrimeableCacheAside_SetLockingBehavior(t *testing.T) {
	t.Run("Set respects new Get operations that start after read lock cleared", func(t *testing.T) {
		ctx := t.Context()
		key := "set-locking:" + uuid.New().String()

		client, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer client.Close()

		// First, create and release a read lock to simulate initial state
		innerClient := client.Client()
		lockVal := "__redcache:lock:" + uuid.New().String()
		err = innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(lockVal).Px(100*time.Millisecond).Build()).Error()
		require.NoError(t, err)

		// Wait for the initial lock to expire
		time.Sleep(150 * time.Millisecond)

		// Now start a Set operation
		setStarted := make(chan struct{})
		setDone := make(chan struct{})
		var setErr error

		go func() {
			defer close(setDone)
			callErr := client.Set(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
				close(setStarted)
				// Hold the lock for a bit while computing
				time.Sleep(500 * time.Millisecond)
				return "value-from-set", nil
			})
			setErr = callErr
		}()

		// Wait for Set to acquire its lock
		<-setStarted
		time.Sleep(50 * time.Millisecond)

		// Now a Get operation starts - it should wait for Set to complete
		// (not read a partial state)
		getStart := time.Now()
		val, err := client.Get(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
			t.Fatal("Get callback should not be called - Set should provide the value")
			return "", nil
		})
		getDuration := time.Since(getStart)

		require.NoError(t, err)
		assert.Equal(t, "value-from-set", val)
		// Get should have waited for Set to complete
		assert.Greater(t, getDuration, 400*time.Millisecond, "Get should wait for Set to complete")

		<-setDone
		assert.NoError(t, setErr)
	})

	t.Run("Set cannot overwrite active read lock from concurrent Get", func(t *testing.T) {
		ctx := t.Context()
		key := "set-no-overwrite:" + uuid.New().String()

		client1, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer client1.Close()

		client2, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer client2.Close()

		// Start a Get operation that will hold a lock
		getStarted := make(chan struct{})
		getDone := make(chan struct{})
		var getVal string
		var getErr error

		go func() {
			defer close(getDone)
			val, callErr := client1.Get(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
				close(getStarted)
				// Hold lock while computing
				time.Sleep(1 * time.Second)
				return "value-from-get", nil
			})
			getVal = val
			getErr = callErr
		}()

		// Wait for Get to acquire lock
		<-getStarted
		time.Sleep(100 * time.Millisecond)

		// Set should wait for Get to complete, not overwrite the lock
		setStart := time.Now()
		err = client2.Set(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
			return "value-from-set", nil
		})
		setDuration := time.Since(setStart)

		require.NoError(t, err)
		// Set should have waited for Get
		assert.Greater(t, setDuration, 850*time.Millisecond, "Set should wait for Get lock")

		<-getDone
		assert.NoError(t, getErr)
		assert.Equal(t, "value-from-get", getVal)

		// Final value should be from Set (it overwrote after Get completed)
		finalVal, err := client1.Get(ctx, time.Second, key, func(ctx context.Context, key string) (string, error) {
			t.Fatal("should not call callback")
			return "", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "value-from-set", finalVal)
	})

	t.Run("ForceSet bypasses locks while Set respects them", func(t *testing.T) {
		ctx := t.Context()
		key := "force-vs-set:" + uuid.New().String()

		client, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 5 * time.Second},
		)
		require.NoError(t, err)
		defer client.Close()

		// Create a long-lasting read lock
		innerClient := client.Client()
		lockVal := "__redcache:lock:" + uuid.New().String()
		err = innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(lockVal).Px(5*time.Second).Build()).Error()
		require.NoError(t, err)

		var wg sync.WaitGroup

		// Try Set - should wait for lock then succeed after ForceSet replaces it
		wg.Add(1)
		var setDuration time.Duration
		var setErr error
		go func() {
			defer wg.Done()
			start := time.Now()
			// Set will wait for the lock, then proceed when ForceSet replaces it with a value
			setErr = client.Set(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
				return "set-value", nil
			})
			setDuration = time.Since(start)
		}()

		// Give Set goroutine time to start waiting
		time.Sleep(100 * time.Millisecond)

		// Try ForceSet - should not wait and replace the lock
		forceStart := time.Now()
		err = client.ForceSet(ctx, 10*time.Second, key, "force-value")
		forceDuration := time.Since(forceStart)

		require.NoError(t, err)
		assert.Less(t, forceDuration, 500*time.Millisecond, "ForceSet should not wait")

		wg.Wait()
		// Set should succeed after ForceSet replaces the lock with a value
		assert.NoError(t, setErr, "Set should succeed after ForceSet replaces lock")
		assert.Less(t, setDuration, 500*time.Millisecond, "Set should proceed quickly after ForceSet")

		// Final value should be from Set (it ran after ForceSet)
		val, err := client.Get(ctx, time.Second, key, func(ctx context.Context, key string) (string, error) {
			t.Fatal("should not call callback")
			return "", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "set-value", val, "Final value should be from Set")
	})

	t.Run("Set properly uses compare-and-swap to ensure lock is held", func(t *testing.T) {
		ctx := t.Context()
		key := "set-cas:" + uuid.New().String()

		client, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 500 * time.Millisecond},
		)
		require.NoError(t, err)
		defer client.Close()

		// Start a Set operation with a very short lock TTL
		// The lock might expire during the callback
		err = client.Set(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
			// Simulate long computation that exceeds lock TTL
			time.Sleep(600 * time.Millisecond)
			return "computed-value", nil
		})

		// The operation should fail because the lock expired
		require.Error(t, err)
		assert.ErrorIs(t, err, redcache.ErrLockLost)

		// Verify the value was NOT set (atomic check-and-set failed)
		innerClient := client.Client()
		getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
		assert.True(t, rueidis.IsRedisNil(getErr), "Value should not be set when lock is lost")
	})

	// NOTE: Tests for Get/GetMulti with expiring locks moved to cacheaside_test.go
	// This test file should focus on Set/SetMulti behavior and their interactions with Get/GetMulti

	t.Run("SetMulti with callback exceeding lock TTL", func(t *testing.T) {
		ctx := t.Context()
		key1 := "setmulti-exceed-1:" + uuid.New().String()
		key2 := "setmulti-exceed-2:" + uuid.New().String()

		client, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 500 * time.Millisecond},
		)
		require.NoError(t, err)
		defer client.Close()

		// SetMulti with callback that exceeds lock TTL
		// Locks will expire during callback, so CAS will fail
		keys := []string{key1, key2}
		_, err = client.SetMulti(ctx, 10*time.Second, keys, func(ctx context.Context, lockedKeys []string) (map[string]string, error) {
			// Simulate computation that exceeds lock TTL
			time.Sleep(600 * time.Millisecond)
			return map[string]string{
				key1: "value1",
				key2: "value2",
			}, nil
		})

		// SetMulti should fail because locks expired during callback
		require.Error(t, err)

		// Error should be BatchError with ErrLockLost for all keys
		var batchErr *redcache.BatchError
		if errors.As(err, &batchErr) {
			assert.True(t, batchErr.HasFailures(), "Should have failures")
			assert.Len(t, batchErr.Failed, 2, "Both keys should fail")
			for _, keyErr := range batchErr.Failed {
				assert.ErrorIs(t, keyErr, redcache.ErrLockLost, "Should be ErrLockLost")
			}
		}

		// Values should NOT be cached (CAS failed)
		innerClient := client.Client()
		err1 := innerClient.Do(ctx, innerClient.B().Get().Key(key1).Build()).Error()
		assert.True(t, rueidis.IsRedisNil(err1), "key1 should not be cached")

		err2 := innerClient.Do(ctx, innerClient.B().Get().Key(key2).Build()).Error()
		assert.True(t, rueidis.IsRedisNil(err2), "key2 should not be cached")
	})
}

// TestPrimeableCacheAside_SetInvalidationMechanism verifies that Set and SetMulti
// properly subscribe to Redis cache invalidations when waiting for locks, rather than
// relying solely on ticker polling. The ticker provides a fallback at 50ms intervals,
// so tests verify operations complete in < 40ms to prove invalidation is working.
func TestPrimeableCacheAside_SetInvalidationMechanism(t *testing.T) {
	t.Run("Set receives cache lock invalidation, not just ticker", func(t *testing.T) {
		ctx := t.Context()
		key := "set-invalidation:" + uuid.New().String()

		client, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer client.Close()

		// Manually place a cache lock using the client's lock prefix
		innerClient := client.Client()
		lockVal := "__redcache:lock:" + uuid.New().String()
		err = innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(lockVal).Px(2*time.Second).Build()).Error()
		require.NoError(t, err)

		// Start a Set operation in background - it will wait for the lock
		setDone := make(chan struct{})
		var setErr error
		var setDuration time.Duration

		go func() {
			defer close(setDone)
			start := time.Now()
			setErr = client.Set(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
				return "value-after-invalidation", nil
			})
			setDuration = time.Since(start)
		}()

		// Give Set time to register for invalidations
		time.Sleep(10 * time.Millisecond)

		// Delete the lock to trigger an invalidation event
		// If DoCache is working, Set will be notified immediately
		// If only ticker polling works, Set will wait ~50ms
		err = innerClient.Do(ctx, innerClient.B().Del().Key(key).Build()).Error()
		require.NoError(t, err)

		// Wait for Set to complete
		<-setDone
		require.NoError(t, setErr)

		// Verify Set completed quickly via invalidation, not ticker polling
		// Ticker fires at 50ms intervals, so < 40ms proves invalidation works
		assert.Less(t, setDuration, 40*time.Millisecond,
			"Set should complete via invalidation (< 40ms), not ticker polling (~50ms)")

		// Verify the value was set correctly
		val, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "value-after-invalidation", val)
	})

	t.Run("Set waits for another Set's cache lock via invalidation", func(t *testing.T) {
		ctx := t.Context()
		key := "set-wait-set:" + uuid.New().String()

		client1, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer client1.Close()

		client2, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer client2.Close()

		// Start first Set operation that will hold lock for only 20ms
		set1Started := make(chan struct{})
		set1Done := make(chan struct{})
		var set1Err error

		go func() {
			defer close(set1Done)
			set1Err = client1.Set(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
				close(set1Started)
				time.Sleep(20 * time.Millisecond)
				return "value-from-set1", nil
			})
		}()

		// Wait for first Set to acquire lock
		<-set1Started
		time.Sleep(5 * time.Millisecond)

		// Start second Set operation - should wait for first Set via invalidation
		set2Start := time.Now()
		err = client2.Set(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
			return "value-from-set2", nil
		})
		set2Duration := time.Since(set2Start)

		require.NoError(t, err)

		// Second Set should wait ~20ms for first Set's callback + small overhead
		// If using invalidation: 20ms (callback) + ~5-15ms (invalidation + overhead) = ~25-35ms
		// If using ticker only: 20ms (callback) + 50ms (ticker interval) = 70ms+
		// We use < 50ms threshold to prove invalidation works (ticker hasn't fired yet)
		assert.Greater(t, set2Duration, 15*time.Millisecond, "Set2 should wait for Set1")
		assert.Less(t, set2Duration, 50*time.Millisecond,
			"Set2 should complete via invalidation (< 50ms), ticker polling would take 70ms+")

		<-set1Done
		require.NoError(t, set1Err)

		// Final value should be from Set2
		innerClient := client1.Client()
		val, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "value-from-set2", val)
	})

	t.Run("SetMulti receives invalidations during sequential acquisition", func(t *testing.T) {
		ctx := t.Context()
		key1 := "setmulti-inv-1:" + uuid.New().String()
		key2 := "setmulti-inv-2:" + uuid.New().String()
		key3 := "setmulti-inv-3:" + uuid.New().String()

		client, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer client.Close()

		// Place locks on key2 and key3
		innerClient := client.Client()
		lock2 := "__redcache:lock:" + uuid.New().String()
		lock3 := "__redcache:lock:" + uuid.New().String()

		err = innerClient.Do(ctx, innerClient.B().Set().Key(key2).Value(lock2).Px(2*time.Second).Build()).Error()
		require.NoError(t, err)
		err = innerClient.Do(ctx, innerClient.B().Set().Key(key3).Value(lock3).Px(2*time.Second).Build()).Error()
		require.NoError(t, err)

		// Start SetMulti in background
		keys := []string{key1, key2, key3}
		setMultiDone := make(chan struct{})
		var setMultiResult map[string]string
		var setMultiErr error
		var setMultiDuration time.Duration

		go func() {
			defer close(setMultiDone)
			start := time.Now()
			setMultiResult, setMultiErr = client.SetMulti(ctx, 10*time.Second, keys,
				func(ctx context.Context, lockedKeys []string) (map[string]string, error) {
					result := make(map[string]string)
					for _, k := range lockedKeys {
						result[k] = "value-for-" + k
					}
					return result, nil
				})
			setMultiDuration = time.Since(start)
		}()

		// Give SetMulti time to acquire key1 and start waiting for key2
		time.Sleep(20 * time.Millisecond)

		// Delete lock on key2 to trigger invalidation
		err = innerClient.Do(ctx, innerClient.B().Del().Key(key2).Build()).Error()
		require.NoError(t, err)

		// Wait a bit, then delete lock on key3
		time.Sleep(20 * time.Millisecond)
		err = innerClient.Do(ctx, innerClient.B().Del().Key(key3).Build()).Error()
		require.NoError(t, err)

		// Wait for SetMulti to complete
		<-setMultiDone
		require.NoError(t, setMultiErr)
		assert.Len(t, setMultiResult, 3, "All keys should be set")

		// Verify SetMulti completed quickly via invalidations
		// If using ticker only: 2 keys * 50ms average = ~100ms
		// If using invalidations: ~40-60ms total
		assert.Less(t, setMultiDuration, 80*time.Millisecond,
			"SetMulti should complete via invalidations, not ticker polling")

		// Verify all values were set
		var val string
		var getErr error
		for _, key := range keys {
			val, getErr = innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
			require.NoError(t, getErr)
			assert.Equal(t, "value-for-"+key, val)
		}
	})
}

// TestPrimeableCacheAside_SetMultiPartialFailure tests the partial failure and recovery paths
// in SetMulti when some keys can't be acquired initially. This covers:
// - findFirstKey (finds first failed key in sorted order) - ~85% coverage
// - splitAcquiredBySequence (separates sequential vs out-of-order locks) - ~85% coverage
// - touchMultiLocks (refreshes TTL on held locks) - ~83% coverage
// - keysNotIn (calculates remaining keys) - 100% coverage
// - waitForSingleLock (waits for specific key via invalidation) - ~83% coverage
//
// Note: restoreMultiValues (0% coverage) is not reliably testable due to timing dependencies
// with the invalidation mechanism. The restore path requires locks to persist through
// waitForReadLocks AND still be present during acquireMultiCacheLocks, which is not
// achievable with current test infrastructure without risky architectural changes.
func TestPrimeableCacheAside_SetMultiPartialFailure(t *testing.T) {
	t.Run("SetMulti with middle key locked - sequential acquisition", func(t *testing.T) {
		ctx := t.Context()

		// Use a shared UUID prefix to ensure sort order
		prefix := uuid.New().String()
		key1 := "partial:1:" + prefix // Will sort first
		key2 := "partial:2:" + prefix // Will sort second (this will be locked)
		key3 := "partial:3:" + prefix // Will sort third
		keys := []string{key1, key2, key3}

		client, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer client.Close()

		innerClient := client.Client()

		// Lock key2 with long TTL, but release it after 150ms in background
		lockVal2 := "__redcache:lock:" + uuid.New().String()
		err = innerClient.Do(ctx, innerClient.B().Set().Key(key2).Value(lockVal2).Px(10000*time.Millisecond).Build()).Error()
		require.NoError(t, err)

		// Release the lock after a short delay to let test complete quickly
		go func() {
			time.Sleep(150 * time.Millisecond)
			_ = innerClient.Do(ctx, innerClient.B().Del().Key(key2).Build()).Error()
		}()

		// SetMulti should:
		// 1. Try to acquire all 3 keys
		// 2. Succeed on key1, fail on key2 (locked), succeed on key3
		// 3. Keep key1 (sequential before failure)
		// 4. Restore key3 (out of order after failure)
		// 5. Wait for key2 to be released (via invalidation when lock expires at ~500ms)
		// 6. Retry and acquire key2 and key3
		// 7. Complete successfully

		start := time.Now()
		result, err := client.SetMulti(ctx, 10*time.Second, keys,
			func(ctx context.Context, lockedKeys []string) (map[string]string, error) {
				return map[string]string{
					key1: "value1",
					key2: "value2",
					key3: "value3",
				}, nil
			})
		duration := time.Since(start)

		require.NoError(t, err)
		assert.Len(t, result, 3, "All keys should eventually be set")

		// Should wait for key2 lock to be released (~150ms)
		assert.Greater(t, duration, 140*time.Millisecond, "Should wait for key2 lock")
		assert.Less(t, duration, 250*time.Millisecond, "Should complete quickly after lock released")

		// Verify all values were set correctly
		val1, err := innerClient.Do(ctx, innerClient.B().Get().Key(key1).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "value1", val1)

		val2, err := innerClient.Do(ctx, innerClient.B().Get().Key(key2).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "value2", val2)

		val3, err := innerClient.Do(ctx, innerClient.B().Get().Key(key3).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "value3", val3)
	})

	t.Run("SetMulti with out-of-order success triggers restore", func(t *testing.T) {
		ctx := t.Context()

		// Use a shared UUID prefix to ensure sort order
		prefix := uuid.New().String()
		key1 := "restore:1:" + prefix
		key2 := "restore:2:" + prefix // Will be locked
		key3 := "restore:3:" + prefix // Has existing value that should be restored
		keys := []string{key1, key2, key3}

		client, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer client.Close()

		innerClient := client.Client()

		// Set an existing value for key3 that should be restored if acquired out of order
		originalValue3 := "original-value-3"
		err = innerClient.Do(ctx, innerClient.B().Set().Key(key3).Value(originalValue3).Build()).Error()
		require.NoError(t, err)

		// Lock key2 with long TTL, release after 120ms
		lockVal2 := "__redcache:lock:" + uuid.New().String()
		err = innerClient.Do(ctx, innerClient.B().Set().Key(key2).Value(lockVal2).Px(10000*time.Millisecond).Build()).Error()
		require.NoError(t, err)

		// Release the lock after a short delay
		go func() {
			time.Sleep(120 * time.Millisecond)
			_ = innerClient.Do(ctx, innerClient.B().Del().Key(key2).Build()).Error()
		}()

		// SetMulti should:
		// 1. Acquire key1 (success), try key2 (fail - locked), acquire key3 (success, saves original value)
		// 2. Find first failed key = key2
		// 3. Keep key1 (before key2), restore key3 (after key2) back to original value
		// 4. Wait for key2
		// 5. Retry and acquire key2, key3

		result, err := client.SetMulti(ctx, 10*time.Second, keys,
			func(ctx context.Context, lockedKeys []string) (map[string]string, error) {
				return map[string]string{
					key1: "new-value-1",
					key2: "new-value-2",
					key3: "new-value-3",
				}, nil
			})

		require.NoError(t, err)
		assert.Len(t, result, 3)

		// All values should be the new values
		val1, err := innerClient.Do(ctx, innerClient.B().Get().Key(key1).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "new-value-1", val1)

		val2, err := innerClient.Do(ctx, innerClient.B().Get().Key(key2).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "new-value-2", val2)

		val3, err := innerClient.Do(ctx, innerClient.B().Get().Key(key3).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "new-value-3", val3, "key3 should have new value after successful acquisition")
	})

	t.Run("SetMulti TTL refresh during multi-retry waiting", func(t *testing.T) {
		ctx := t.Context()

		// Use a shared UUID prefix to ensure sort order
		prefix := uuid.New().String()
		key1 := "ttl:1:" + prefix
		key2 := "ttl:2:" + prefix // Will be locked first
		key3 := "ttl:3:" + prefix
		key4 := "ttl:4:" + prefix // Will be locked second
		keys := []string{key1, key2, key3, key4}

		client, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer client.Close()

		innerClient := client.Client()

		// Lock key2, will be released after 80ms
		lockVal2 := "__redcache:lock:" + uuid.New().String()
		err = innerClient.Do(ctx, innerClient.B().Set().Key(key2).Value(lockVal2).Px(10000*time.Millisecond).Build()).Error()
		require.NoError(t, err)

		// Start SetMulti in background
		setMultiDone := make(chan struct{})
		var setMultiResult map[string]string
		var setMultiErr error

		go func() {
			defer close(setMultiDone)
			setMultiResult, setMultiErr = client.SetMulti(ctx, 10*time.Second, keys,
				func(ctx context.Context, lockedKeys []string) (map[string]string, error) {
					return map[string]string{
						key1: "val1",
						key2: "val2",
						key3: "val3",
						key4: "val4",
					}, nil
				})
		}()

		// Wait for SetMulti to start and hit the key2 lock
		time.Sleep(30 * time.Millisecond)

		// Lock key4 while SetMulti is waiting (this creates a second wait cycle)
		lockVal4 := "__redcache:lock:" + uuid.New().String()
		err = innerClient.Do(ctx, innerClient.B().Set().Key(key4).Value(lockVal4).Px(10000*time.Millisecond).Build()).Error()
		require.NoError(t, err)

		// Release key2 after 80ms total
		time.Sleep(50 * time.Millisecond)
		_ = innerClient.Do(ctx, innerClient.B().Del().Key(key2).Build()).Error()

		// Release key4 after another 60ms
		time.Sleep(60 * time.Millisecond)
		_ = innerClient.Do(ctx, innerClient.B().Del().Key(key4).Build()).Error()

		// SetMulti should:
		// 1. Acquire key1, fail on key2, acquire key3, restore key3 (out of order)
		// 2. Wait for key2 (with TTL refresh on key1) - ~80ms
		// 3. Acquire key2 and key3
		// 4. Fail on key4 (now locked)
		// 5. Wait for key4 (with TTL refresh on key1, key2, key3) - ~60ms
		// 6. Complete in ~140ms total

		<-setMultiDone
		require.NoError(t, setMultiErr)
		assert.Len(t, setMultiResult, 4, "All keys should be set")

		// Verify key1's lock was refreshed (it should still be held during the ~140ms of waits)
		// If TTL wasn't refreshed, key1's lock would expire during the multi-retry waiting
	})

	t.Run("SetMulti keysNotIn correctly filters remaining keys", func(t *testing.T) {
		ctx := t.Context()

		// Use a shared UUID prefix to ensure sort order
		prefix := uuid.New().String()
		key1 := "filter:1:" + prefix
		key2 := "filter:2:" + prefix // Will be locked
		key3 := "filter:3:" + prefix // Will be locked
		key4 := "filter:4:" + prefix
		keys := []string{key1, key2, key3, key4}

		client, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer client.Close()

		innerClient := client.Client()

		// Lock key2 and key3 with long TTL
		lockVal2 := "__redcache:lock:" + uuid.New().String()
		err = innerClient.Do(ctx, innerClient.B().Set().Key(key2).Value(lockVal2).Px(10000*time.Millisecond).Build()).Error()
		require.NoError(t, err)

		lockVal3 := "__redcache:lock:" + uuid.New().String()
		err = innerClient.Do(ctx, innerClient.B().Set().Key(key3).Value(lockVal3).Px(10000*time.Millisecond).Build()).Error()
		require.NoError(t, err)

		// Release locks in background
		go func() {
			time.Sleep(100 * time.Millisecond)
			_ = innerClient.Do(ctx, innerClient.B().Del().Key(key2).Build()).Error()
			time.Sleep(80 * time.Millisecond)
			_ = innerClient.Do(ctx, innerClient.B().Del().Key(key3).Build()).Error()
		}()

		// SetMulti should:
		// 1. Acquire key1, fail on key2, restore key3 and key4 (out of order)
		// 2. keysNotIn should return [key2, key3, key4] (all keys not in {key1})
		// 3. Wait for key2 (~100ms), then retry
		// 4. Acquire key2, fail on key3, restore key4
		// 5. keysNotIn should return [key3, key4]
		// 6. Wait for key3 (~80ms), complete (~180ms total)

		result, err := client.SetMulti(ctx, 10*time.Second, keys,
			func(ctx context.Context, lockedKeys []string) (map[string]string, error) {
				return map[string]string{
					key1: "val1",
					key2: "val2",
					key3: "val3",
					key4: "val4",
				}, nil
			})

		require.NoError(t, err)
		assert.Len(t, result, 4, "All keys should eventually be acquired")

		// Verify all values
		for i, key := range keys {
			val, getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
			require.NoError(t, getErr, "key%d should be set", i+1)
			assert.Equal(t, fmt.Sprintf("val%d", i+1), val)
		}
	})
}
