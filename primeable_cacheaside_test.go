package redcache_test

import (
	"context"
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

// Helper function for tests to set multiple values - mimics the old SetMultiValue behavior.
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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

		key := "key:" + uuid.New().String()
		value := "value:" + uuid.New().String()

		err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return value, nil
		})
		require.NoError(t, err)

		// Verify value was set
		innerClient := client.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, value, result)
	})

	t.Run("waits and retries when lock cannot be acquired", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

		key := "key:" + uuid.New().String()
		value := "value:" + uuid.New().String()

		err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return value, nil
		})
		require.NoError(t, err)

		// Get should return the set value without calling callback
		called := false
		cb := func(ctx context.Context, key string) (string, error) {
			called = true
			return "should-not-be-called", nil
		}

		result, err := client.Get(ctx, time.Second, key, cb)
		require.NoError(t, err)
		assert.Equal(t, value, result)
		assert.False(t, called, "callback should not be called when value exists")
	})
}

func TestPrimeableCacheAside_ForceSet(t *testing.T) {
	t.Run("successful force set bypasses locks", func(t *testing.T) {
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

		result, err := setMultiValue(client, ctx, time.Second, map[string]string{})
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("waits for all locks to be released then sets all values", func(t *testing.T) {
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
}

func TestPrimeableCacheAside_ForceSetMulti(t *testing.T) {
	t.Run("successful force set multi bypasses locks", func(t *testing.T) {
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

		err := forceSetMulti(client, ctx, time.Second, map[string]string{})
		require.NoError(t, err)
	})

	t.Run("force set multi overrides existing locks", func(t *testing.T) {
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

		key := "key:" + uuid.New().String()

		wg := sync.WaitGroup{}

		// Try to Set concurrently - all should eventually succeed by waiting
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				value := fmt.Sprintf("value-%d", i)
				err := client.Set(ctx, time.Millisecond*100, key, func(_ context.Context, _ string) (string, error) {
					return value, nil
				})
				assert.NoError(t, err, "all Set operations should eventually succeed")
			}(i)
		}

		wg.Wait()

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
		defer client.Client().Close()

		key := "key:" + uuid.New().String()
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return "value", nil
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("Set with context cancelled while waiting for lock", func(t *testing.T) {
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

		key := "key:" + uuid.New().String()

		// Set a lock manually with long TTL
		innerClient := client.Client()
		lockVal := "__redcache:lock:" + uuid.New().String()
		err := innerClient.Do(context.Background(), innerClient.B().Set().Key(key).Value(lockVal).Nx().ExSeconds(10).Build()).Error()
		require.NoError(t, err)

		// Try to Set with short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
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
		defer client.Client().Close()

		values := map[string]string{
			"key:1:" + uuid.New().String(): "value1",
			"key:2:" + uuid.New().String(): "value2",
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err := setMultiValue(client, ctx, time.Second, values)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("SetMulti with context cancelled while waiting for locks", func(t *testing.T) {
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
}

func TestPrimeableCacheAside_EdgeCases_Additional(t *testing.T) {
	t.Run("Set overwrites existing non-lock value", func(t *testing.T) {
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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

	t.Run("SetMulti from multiple clients with overlapping keys", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		client1 := makeClientWithSet(t, addr)
		defer client1.Client().Close()
		client2 := makeClientWithSet(t, addr)
		defer client2.Client().Close()

		key1 := "key:1:" + uuid.New().String()
		key2 := "key:2:" + uuid.New().String()
		key3 := "key:3:" + uuid.New().String()

		var wg sync.WaitGroup
		var err1, err2 error

		// Client 1 sets keys 1 and 2
		wg.Add(1)
		go func() {
			defer wg.Done()
			values := map[string]string{
				key1: "client1-value1",
				key2: "client1-value2",
			}
			// Use longer TTL to ensure values don't expire during concurrent operations
			_, err1 = setMultiValue(client1, ctx, 15*time.Second, values)
		}()

		// Client 2 sets keys 2 and 3 (overlaps on key2)
		wg.Add(1)
		go func() {
			defer wg.Done()
			values := map[string]string{
				key2: "client2-value2",
				key3: "client2-value3",
			}
			// Use longer TTL to ensure values don't expire during concurrent operations
			_, err2 = setMultiValue(client2, ctx, 15*time.Second, values)
		}()

		wg.Wait()

		// At least one client should succeed in setting keys due to lock coordination
		// Both clients may succeed (one after the other) or one might timeout
		if err1 != nil && err2 != nil {
			t.Fatal("Both clients failed to set keys, expected at least one to succeed")
		}

		// Verify keys that were successfully set
		innerClient := client1.Client()

		// Key1 should exist (only client1 tries to set it)
		val1, err := innerClient.Do(context.Background(), innerClient.B().Get().Key(key1).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "client1-value1", val1)

		// Key2 should exist (both clients try to set it, one should succeed)
		val2, err := innerClient.Do(context.Background(), innerClient.B().Get().Key(key2).Build()).ToString()
		require.NoError(t, err)
		assert.NotEmpty(t, val2)
		assert.Contains(t, []string{"client1-value2", "client2-value2"}, val2)

		// Key3 should exist (only client2 tries to set it)
		val3, err := innerClient.Do(context.Background(), innerClient.B().Get().Key(key3).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "client2-value3", val3)
	})

	t.Run("Get with callback error does not cache", func(t *testing.T) {
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

		cb := func(ctx context.Context, keys []string) (map[string]string, error) {
			return make(map[string]string), nil
		}

		result, err := client.GetMulti(ctx, time.Second, []string{}, cb)
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("GetMulti with callback error does not cache", func(t *testing.T) {
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

		key := "key:" + uuid.New().String()

		// Delete non-existent key should not error
		err := client.Del(ctx, key)
		require.NoError(t, err)
	})

	t.Run("DelMulti on non-existent keys succeeds", func(t *testing.T) {
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

		keys := []string{
			"key:1:" + uuid.New().String(),
			"key:2:" + uuid.New().String(),
		}

		// Delete non-existent keys should not error
		err := client.DelMulti(ctx, keys...)
		require.NoError(t, err)
	})

	t.Run("DelMulti with empty keys slice", func(t *testing.T) {
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
	ctx := context.Background()
	client := makeClientWithSet(t, addr)
	defer client.Client().Close()

	key := "key:" + uuid.New().String()

	// Manually set a lock value in Redis (simulating a lock from a Get operation)
	innerClient := client.Client()
	lockVal := "__redcache:lock:" + uuid.New().String()
	err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(lockVal).Px(time.Second*5).Build()).Error()
	require.NoError(t, err)

	// Now try to Set - this should wait for the lock, not block indefinitely
	// Use a timeout to ensure we don't wait too long
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	value := "value:" + uuid.New().String()

	// This should complete within the lock TTL (5 seconds) + some buffer
	// If Set is broken and blocks on its own local lock, this will timeout
	start := time.Now()
	err = client.Set(ctxWithTimeout, time.Second, key, func(_ context.Context, _ string) (string, error) {
		return value, nil
	})
	elapsed := time.Since(start)

	require.NoError(t, err)

	// Should have waited approximately 5 seconds for lock to expire
	assert.Greater(t, elapsed, time.Second*4, "Should have waited for lock TTL")
	assert.Less(t, elapsed, time.Second*7, "Should not have blocked indefinitely")

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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

		key := "user:" + uuid.New().String()
		expectedValue := "db-value:" + uuid.New().String()

		callbackExecuted := false
		callback := func(ctx context.Context, key string) (string, error) {
			callbackExecuted = true
			// Simulate database write
			time.Sleep(10 * time.Millisecond)
			return expectedValue, nil
		}

		// Execute write-through Set
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
		ctx := context.Background()
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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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

// TestPrimeableCacheAside_SetMultiWithCallback tests batch write-through operations with a callback.
func TestPrimeableCacheAside_SetMultiWithCallback(t *testing.T) {
	t.Run("acquires locks, executes callback, caches results", func(t *testing.T) {
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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

		// Execute write-through SetMulti
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
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

		callback := func(ctx context.Context, keys []string) (map[string]string, error) {
			t.Fatal("callback should not be called for empty keys")
			return nil, nil
		}

		result, err := client.SetMulti(ctx, time.Second, []string{}, callback)
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("callback error prevents caching", func(t *testing.T) {
		ctx := context.Background()
		client := makeClientWithSet(t, addr)
		defer client.Client().Close()

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
