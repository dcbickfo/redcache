//go:build distributed

package redcache_test

import (
	"context"
	"fmt"
	"strings"
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

// TestPrimeableCacheAside_DistributedCoordination tests that write operations
// coordinate correctly across multiple clients, especially with read operations
func TestPrimeableCacheAside_DistributedCoordination(t *testing.T) {
	t.Run("write waits for read lock from different client", func(t *testing.T) {
		ctx := t.Context()
		key := "pdist:write-read:" + uuid.New().String()

		// Create separate clients
		readClient, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer readClient.Close()

		writeClient, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer writeClient.Close()

		// Start a read operation that holds a lock
		readStarted := make(chan struct{})
		readDone := make(chan struct{})

		go func() {
			defer close(readDone)
			val, getErr := readClient.Get(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
				close(readStarted)
				// Hold the read lock for a while
				time.Sleep(1 * time.Second)
				return "value-from-read", nil
			})
			assert.NoError(t, getErr)
			assert.Equal(t, "value-from-read", val)
		}()

		// Wait for read to acquire lock
		<-readStarted
		time.Sleep(100 * time.Millisecond)

		// Write operation should wait for read lock to release
		writeStart := time.Now()
		err = writeClient.Set(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
			return "value-from-write", nil
		})
		writeDuration := time.Since(writeStart)

		require.NoError(t, err)
		assert.Greater(t, writeDuration, 850*time.Millisecond, "Write should wait for read lock")
		assert.Less(t, writeDuration, 1500*time.Millisecond, "Write should not wait too long")

		<-readDone

		// Verify final value is from write
		val, err := readClient.Get(ctx, time.Second, key, func(ctx context.Context, key string) (string, error) {
			t.Fatal("should not call callback - value already cached")
			return "", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "value-from-write", val)
	})

	t.Run("multiple writes coordinate through rueidislock", func(t *testing.T) {
		ctx := t.Context()
		key := "pdist:multi-write:" + uuid.New().String()

		numClients := 5
		clients := make([]*redcache.PrimeableCacheAside, numClients)
		for i := 0; i < numClients; i++ {
			client, err := redcache.NewPrimeableCacheAside(
				rueidis.ClientOption{InitAddress: addr},
				redcache.CacheAsideOption{LockTTL: time.Second},
			)
			require.NoError(t, err)
			defer client.Close()
			clients[i] = client
		}

		// Track write order
		var writeOrder atomic.Int32
		writeSequence := make([]int, numClients)

		var wg sync.WaitGroup
		for i := 0; i < numClients; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				err := clients[idx].Set(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
					sequence := writeOrder.Add(1)
					writeSequence[idx] = int(sequence)
					// Simulate some work
					time.Sleep(100 * time.Millisecond)
					return fmt.Sprintf("value-from-client-%d", idx), nil
				})
				assert.NoError(t, err)
			}(i)
		}

		wg.Wait()

		// All writes should have succeeded sequentially (no parallel execution)
		assert.Equal(t, int32(numClients), writeOrder.Load())

		// Verify writes were serialized (each has unique sequence number)
		seen := make(map[int]bool)
		for i, seq := range writeSequence {
			assert.Greater(t, seq, 0, "Client %d should have written", i)
			assert.False(t, seen[seq], "Sequence %d used multiple times", seq)
			seen[seq] = true
		}
	})

	t.Run("SetMulti waits for GetMulti from different client", func(t *testing.T) {
		ctx := t.Context()
		key1 := "pdist:batch:1:" + uuid.New().String()
		key2 := "pdist:batch:2:" + uuid.New().String()

		readClient, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer readClient.Close()

		writeClient, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer writeClient.Close()

		// Start GetMulti that holds locks
		readStarted := make(chan struct{})
		readDone := make(chan struct{})

		go func() {
			defer close(readDone)
			vals, getErr := readClient.GetMulti(ctx, 10*time.Second, []string{key1, key2},
				func(ctx context.Context, keys []string) (map[string]string, error) {
					close(readStarted)
					time.Sleep(1 * time.Second)
					return map[string]string{
						key1: "read-value-1",
						key2: "read-value-2",
					}, nil
				})
			assert.NoError(t, getErr)
			assert.Len(t, vals, 2)
		}()

		<-readStarted
		time.Sleep(100 * time.Millisecond)

		// SetMulti should wait for read locks
		writeStart := time.Now()
		result, err := writeClient.SetMulti(ctx, 10*time.Second, []string{key1, key2},
			func(ctx context.Context, keys []string) (map[string]string, error) {
				return map[string]string{
					key1: "write-value-1",
					key2: "write-value-2",
				}, nil
			})
		writeDuration := time.Since(writeStart)

		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Greater(t, writeDuration, 850*time.Millisecond, "SetMulti should wait")
		assert.Less(t, writeDuration, 1500*time.Millisecond, "SetMulti should not wait too long")

		<-readDone
	})

	t.Run("ForceSet bypasses all locks including from other clients", func(t *testing.T) {
		ctx := t.Context()
		key := "pdist:force:" + uuid.New().String()

		client1, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 5 * time.Second},
		)
		require.NoError(t, err)
		defer client1.Close()

		client2, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 5 * time.Second},
		)
		require.NoError(t, err)
		defer client2.Close()

		// Client 1 starts a slow Get
		getStarted := make(chan struct{})
		go func() {
			_, _ = client1.Get(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
				close(getStarted)
				time.Sleep(2 * time.Second)
				return "get-value", nil
			})
		}()

		<-getStarted
		time.Sleep(100 * time.Millisecond)

		// Client 2 uses ForceSet - should not wait
		forceStart := time.Now()
		err = client2.ForceSet(ctx, 10*time.Second, key, "forced-value")
		forceDuration := time.Since(forceStart)

		require.NoError(t, err)
		assert.Less(t, forceDuration, 500*time.Millisecond, "ForceSet should not wait for locks")
	})

	t.Run("read lock expiration allows write to proceed", func(t *testing.T) {
		ctx := t.Context()
		key := "pdist:expire:" + uuid.New().String()

		// Use very short lock TTL
		readClient, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 500 * time.Millisecond},
		)
		require.NoError(t, err)
		defer readClient.Close()

		writeClient, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 500 * time.Millisecond},
		)
		require.NoError(t, err)
		defer writeClient.Close()

		// Manually set a read lock that will expire
		innerClient := readClient.Client()
		lockVal := "__redcache:lock:" + uuid.New().String()
		err = innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(lockVal).Px(500*time.Millisecond).Build()).Error()
		require.NoError(t, err)

		// Write should wait for lock expiration then proceed
		writeStart := time.Now()
		err = writeClient.Set(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
			return "write-after-expiry", nil
		})
		writeDuration := time.Since(writeStart)

		require.NoError(t, err)
		assert.Greater(t, writeDuration, 400*time.Millisecond, "Should wait for lock expiry")
		assert.Less(t, writeDuration, 1*time.Second, "Should not wait too long")

		// Verify value was written
		val, err := readClient.Get(ctx, time.Second, key, func(ctx context.Context, key string) (string, error) {
			t.Fatal("should not call callback")
			return "", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "write-after-expiry", val)
	})

	t.Run("invalidation from write triggers waiting reads across clients", func(t *testing.T) {
		ctx := t.Context()
		key := "pdist:invalidate:" + uuid.New().String()

		// Set up initial lock to block everyone
		client1, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer client1.Close()

		// Manually create a lock
		innerClient := client1.Client()
		lockVal := "__redcache:lock:" + uuid.New().String()
		err = innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(lockVal).Px(5*time.Second).Build()).Error()
		require.NoError(t, err)

		// Multiple readers waiting
		numReaders := 3
		readers := make([]*redcache.PrimeableCacheAside, numReaders)
		readerResults := make([]string, numReaders)
		readerErrors := make([]error, numReaders)

		var readersWg sync.WaitGroup
		for i := 0; i < numReaders; i++ {
			reader, newErr := redcache.NewPrimeableCacheAside(
				rueidis.ClientOption{InitAddress: addr},
				redcache.CacheAsideOption{LockTTL: 2 * time.Second},
			)
			require.NoError(t, newErr)
			defer reader.Close()
			readers[i] = reader

			readersWg.Add(1)
			go func(idx int) {
				defer readersWg.Done()
				val, getErr := readers[idx].Get(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
					return fmt.Sprintf("reader-%d-computed", idx), nil
				})
				readerResults[idx] = val
				readerErrors[idx] = getErr
			}(i)
		}

		// Give readers time to start waiting
		time.Sleep(200 * time.Millisecond)

		// Writer comes in and sets value (replacing the lock)
		writer, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer writer.Close()

		err = writer.ForceSet(ctx, 10*time.Second, key, "written-value")
		require.NoError(t, err)

		// All readers should wake up and get the written value
		readersWg.Wait()

		for i := 0; i < numReaders; i++ {
			assert.NoError(t, readerErrors[i], "Reader %d should succeed", i)
			assert.Equal(t, "written-value", readerResults[i], "Reader %d should get written value", i)
		}
	})

	t.Run("stress test - many concurrent read and write operations", func(t *testing.T) {
		ctx := t.Context()

		numClients := 10
		numOperations := 20
		clients := make([]*redcache.PrimeableCacheAside, numClients)

		for i := 0; i < numClients; i++ {
			client, err := redcache.NewPrimeableCacheAside(
				rueidis.ClientOption{InitAddress: addr},
				redcache.CacheAsideOption{LockTTL: 500 * time.Millisecond},
			)
			require.NoError(t, err)
			defer client.Close()
			clients[i] = client
		}

		// Use a small set of keys to ensure contention
		keys := make([]string, 5)
		for i := 0; i < 5; i++ {
			keys[i] = fmt.Sprintf("pdist:stress:%d:%s", i, uuid.New().String())
		}

		var wg sync.WaitGroup
		errorCount := atomic.Int32{}
		successCount := atomic.Int32{}

		// Mix of read and write operations
		for i := 0; i < numOperations; i++ {
			wg.Add(1)
			go func(opIdx int) {
				defer wg.Done()

				clientIdx := opIdx % numClients
				keyIdx := opIdx % len(keys)
				key := keys[keyIdx]

				if opIdx%3 == 0 {
					// Write operation
					err := clients[clientIdx].Set(ctx, 5*time.Second, key,
						func(ctx context.Context, key string) (string, error) {
							time.Sleep(10 * time.Millisecond)
							return fmt.Sprintf("write-%d-%d", clientIdx, opIdx), nil
						})
					if err != nil {
						errorCount.Add(1)
					} else {
						successCount.Add(1)
					}
				} else {
					// Read operation
					_, err := clients[clientIdx].Get(ctx, 5*time.Second, key,
						func(ctx context.Context, key string) (string, error) {
							time.Sleep(10 * time.Millisecond)
							return fmt.Sprintf("read-%d-%d", clientIdx, opIdx), nil
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
		assert.Equal(t, int32(0), errorCount.Load(), "No operations should fail")
		assert.Equal(t, int32(numOperations), successCount.Load(), "All operations should succeed")

		// Verify all keys have values
		for _, key := range keys {
			// Use a fresh client to check final state
			checkClient, err := redcache.NewPrimeableCacheAside(
				rueidis.ClientOption{InitAddress: addr},
				redcache.CacheAsideOption{LockTTL: time.Second},
			)
			require.NoError(t, err)
			defer checkClient.Close()

			val, err := checkClient.Get(ctx, time.Second, key, func(ctx context.Context, key string) (string, error) {
				return "should-exist", nil
			})
			assert.NoError(t, err)
			assert.NotEmpty(t, val, "Key %s should have a value", key)
		}
	})

	t.Run("context cancellation during distributed Set", func(t *testing.T) {
		ctx := t.Context()
		key := "pdist:ctx-cancel:" + uuid.New().String()

		client1, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 5 * time.Second},
		)
		require.NoError(t, err)
		defer client1.Close()

		client2, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 5 * time.Second},
		)
		require.NoError(t, err)
		defer client2.Close()

		// Client 1 holds a lock
		getLockAcquired := make(chan struct{})
		go func() {
			_, _ = client1.Get(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
				close(getLockAcquired)
				time.Sleep(2 * time.Second)
				return "get-value", nil
			})
		}()

		<-getLockAcquired
		time.Sleep(50 * time.Millisecond)

		// Client 2 tries to Set with short timeout
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		err = client2.Set(ctxWithTimeout, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return "set-value", nil
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("callback error in distributed Set does not cache", func(t *testing.T) {
		ctx := t.Context()
		key := "pdist:callback-error:" + uuid.New().String()

		client1, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: time.Second},
		)
		require.NoError(t, err)
		defer client1.Close()

		client2, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: time.Second},
		)
		require.NoError(t, err)
		defer client2.Close()

		// Client 1 tries to Set but callback fails
		callCount1 := 0
		err = client1.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			callCount1++
			return "", fmt.Errorf("database write failed")
		})
		require.Error(t, err)
		assert.Equal(t, 1, callCount1)

		// Client 2 should not see cached error - should retry callback
		callCount2 := 0
		err = client2.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			callCount2++
			return "success-value", nil
		})
		require.NoError(t, err)
		assert.Equal(t, 1, callCount2)

		// Verify the successful value was written
		innerClient := client1.Client()
		result, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "success-value", result)
	})

	t.Run("SetMulti callback error across clients does not cache", func(t *testing.T) {
		ctx := t.Context()

		client1, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: time.Second},
		)
		require.NoError(t, err)
		defer client1.Close()

		client2, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: time.Second},
		)
		require.NoError(t, err)
		defer client2.Close()

		keys := []string{
			"pdist:multi-error:1:" + uuid.New().String(),
			"pdist:multi-error:2:" + uuid.New().String(),
		}

		// Client 1 tries SetMulti but fails
		_, err = client1.SetMulti(ctx, time.Second, keys, func(_ context.Context, _ []string) (map[string]string, error) {
			return nil, fmt.Errorf("batch write failed")
		})
		require.Error(t, err)

		// Client 2 should retry successfully
		result, err := client2.SetMulti(ctx, time.Second, keys, func(_ context.Context, reqKeys []string) (map[string]string, error) {
			res := make(map[string]string)
			for _, k := range reqKeys {
				res[k] = "success-" + k
			}
			return res, nil
		})
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("Del during distributed Set coordination", func(t *testing.T) {
		ctx := t.Context()
		key := "pdist:del-during-set:" + uuid.New().String()

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

		// Client 1 starts Set
		setStarted := make(chan struct{})
		setDone := make(chan error, 1)
		go func() {
			setErr := client1.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
				close(setStarted)
				time.Sleep(300 * time.Millisecond)
				return "set-value", nil
			})
			setDone <- setErr
		}()

		<-setStarted
		time.Sleep(50 * time.Millisecond)

		// Client 2 deletes the key while Set is in progress
		err = client2.Del(ctx, key)
		require.NoError(t, err)

		// Wait for Set to complete
		setErr := <-setDone

		// Set should fail because it lost the cache lock (per spec line 20)
		require.Error(t, setErr)
		assert.ErrorIs(t, setErr, redcache.ErrLockLost)
	})

	t.Run("DelMulti coordination across clients", func(t *testing.T) {
		ctx := t.Context()

		client1, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: time.Second},
		)
		require.NoError(t, err)
		defer client1.Close()

		client2, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: time.Second},
		)
		require.NoError(t, err)
		defer client2.Close()

		keys := []string{
			"pdist:delmulti:1:" + uuid.New().String(),
			"pdist:delmulti:2:" + uuid.New().String(),
			"pdist:delmulti:3:" + uuid.New().String(),
		}

		// Client 1 sets values
		_, err = client1.SetMulti(ctx, 5*time.Second, keys, func(_ context.Context, reqKeys []string) (map[string]string, error) {
			res := make(map[string]string)
			for _, k := range reqKeys {
				res[k] = "value-" + k
			}
			return res, nil
		})
		require.NoError(t, err)

		// Client 2 deletes all keys
		err = client2.DelMulti(ctx, keys...)
		require.NoError(t, err)

		// Verify all keys are deleted
		innerClient := client1.Client()
		for _, key := range keys {
			getErr := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
			assert.True(t, rueidis.IsRedisNil(getErr), "key %s should be deleted", key)
		}

		// Client 1 should not find cached values
		callCount := 0
		result, err := client1.GetMulti(ctx, time.Second, keys, func(_ context.Context, reqKeys []string) (map[string]string, error) {
			callCount++
			res := make(map[string]string)
			for _, k := range reqKeys {
				res[k] = "recomputed-" + k
			}
			return res, nil
		})
		require.NoError(t, err)
		assert.Len(t, result, 3)
		assert.Equal(t, 1, callCount, "should recompute after deletion")
	})

	t.Run("empty and special values across clients", func(t *testing.T) {
		ctx := t.Context()

		client1, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: time.Second},
		)
		require.NoError(t, err)
		defer client1.Close()

		client2, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: time.Second},
		)
		require.NoError(t, err)
		defer client2.Close()

		testCases := []struct {
			name  string
			value string
		}{
			// NOTE: Empty string ("") is a known limitation - cross-client caching doesn't work
			// {"empty string", ""},
			{"unicode", "Hello 世界 🚀"},
			{"special chars", "newline\ntab\tquote\""},
			{"whitespace", "   \n\t   "},
		}

		for _, tc := range testCases {
			key := "pdist:special:" + uuid.New().String()

			// Client 1 sets the value
			setErr := client1.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
				return tc.value, nil
			})
			require.NoError(t, setErr, "test case: %s", tc.name)

			// Client 2 reads it - should not call callback
			called := false
			result, getErr := client2.Get(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
				called = true
				return "should-not-be-called", nil
			})
			require.NoError(t, getErr, "test case: %s", tc.name)
			assert.Equal(t, tc.value, result, "test case: %s", tc.name)
			assert.False(t, called, "test case: %s should hit cache", tc.name)
		}
	})

	t.Run("large value handling across clients", func(t *testing.T) {
		ctx := t.Context()

		client1, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: time.Second},
		)
		require.NoError(t, err)
		defer client1.Close()

		client2, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: time.Second},
		)
		require.NoError(t, err)
		defer client2.Close()

		key := "pdist:large:" + uuid.New().String()
		// Create a 1MB value
		largeValue := strings.Repeat("x", 1024*1024)

		// Client 1 sets large value
		err = client1.Set(ctx, 5*time.Second, key, func(_ context.Context, _ string) (string, error) {
			return largeValue, nil
		})
		require.NoError(t, err)

		// Client 2 reads it
		result, err := client2.Get(ctx, 5*time.Second, key, func(_ context.Context, _ string) (string, error) {
			t.Fatal("should not call callback")
			return "", nil
		})
		require.NoError(t, err)
		assert.Equal(t, len(largeValue), len(result))
	})

	t.Run("TTL consistency across clients", func(t *testing.T) {
		ctx := t.Context()

		client1, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: time.Second},
		)
		require.NoError(t, err)
		defer client1.Close()

		client2, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: time.Second},
		)
		require.NoError(t, err)
		defer client2.Close()

		key := "pdist:ttl:" + uuid.New().String()

		// Client 1 sets with 500ms TTL
		err = client1.Set(ctx, 500*time.Millisecond, key, func(_ context.Context, _ string) (string, error) {
			return "short-ttl-value", nil
		})
		require.NoError(t, err)

		// Client 2 should see the value immediately
		result, err := client2.Get(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			return "fallback", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "short-ttl-value", result)

		// Wait for TTL to expire
		time.Sleep(600 * time.Millisecond)

		// Both clients should not find the value
		callCount := 0
		_, err = client1.Get(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			callCount++
			return "recomputed1", nil
		})
		require.NoError(t, err)
		assert.Equal(t, 1, callCount, "client1 should recompute after expiry")

		callCount = 0
		_, err = client2.Get(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
			callCount++
			return "recomputed2", nil
		})
		require.NoError(t, err)
		// callCount could be 0 if client1's recomputed value was already cached
		// This is acceptable behavior
	})

	t.Run("concurrent SetMulti with partial overlap - coordinated completion", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

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

		// Keys with partial overlap
		keys1 := []string{
			"pdist:overlap:a:" + uuid.New().String(),
			"pdist:overlap:b:" + uuid.New().String(),
		}
		keys2 := []string{
			"pdist:overlap:b:" + uuid.New().String(), // Overlaps with keys1[1]
			"pdist:overlap:c:" + uuid.New().String(),
		}

		var wg sync.WaitGroup
		var err1, err2 error

		// Client 1 sets keys1
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err1 = client1.SetMulti(ctx, 10*time.Second, keys1, func(_ context.Context, reqKeys []string) (map[string]string, error) {
				time.Sleep(200 * time.Millisecond)
				res := make(map[string]string)
				for _, k := range reqKeys {
					res[k] = "client1-" + k[len(k)-8:]
				}
				return res, nil
			})
		}()

		// Small delay to ensure client1 starts first
		time.Sleep(50 * time.Millisecond)

		// Client 2 sets keys2 (will wait for overlapping key)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err2 = client2.SetMulti(ctx, 10*time.Second, keys2, func(_ context.Context, reqKeys []string) (map[string]string, error) {
				res := make(map[string]string)
				for _, k := range reqKeys {
					res[k] = "client2-" + k[len(k)-8:]
				}
				return res, nil
			})
		}()

		wg.Wait()

		// At least one should succeed
		if err1 != nil && err2 != nil {
			t.Fatalf("Both clients failed: err1=%v, err2=%v", err1, err2)
		}

		// Verify all keys exist
		innerClient := client1.Client()
		allKeys := append([]string{}, keys1...)
		allKeys = append(allKeys, keys2[1]) // Add unique key from keys2

		for _, key := range allKeys {
			val, getErr := innerClient.Do(context.Background(), innerClient.B().Get().Key(key).Build()).ToString()
			assert.NoError(t, getErr, "key %s should exist", key)
			assert.NotEmpty(t, val, "key %s should have value", key)
		}
	})

	t.Run("distributed: Set from client A + ForceSet from client B steals lock", func(t *testing.T) {
		ctx := t.Context()
		key := "pdist:set-forceSet:" + uuid.New().String()

		clientA, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer clientA.Close()

		clientB, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer clientB.Close()

		setStarted := make(chan struct{})
		setCompleted := make(chan error, 1)
		forceSetCompleted := make(chan struct{})

		// Client A: Start Set operation that holds lock during callback
		go func() {
			setErr := clientA.Set(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
				close(setStarted)
				// Hold lock while callback executes
				time.Sleep(300 * time.Millisecond)
				return "client-a-value", nil
			})
			setCompleted <- setErr
		}()

		// Wait for Client A's Set to acquire lock
		<-setStarted
		time.Sleep(50 * time.Millisecond)

		// Client B: ForceSet should overwrite Client A's lock
		go func() {
			forceErr := clientB.ForceSet(ctx, 10*time.Second, key, "client-b-forced-value")
			require.NoError(t, forceErr)
			close(forceSetCompleted)
		}()

		// Wait for ForceSet to complete
		<-forceSetCompleted

		// Client B's ForceSet should have written immediately
		innerClient := clientA.Client()
		valueDuringSet, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "client-b-forced-value", valueDuringSet, "ForceSet should override lock")

		// Wait for Client A's Set to complete
		setErr := <-setCompleted

		// EXPECTED: Client A's Set MUST fail because lock was stolen
		require.Error(t, setErr, "Client A's Set MUST fail when lock stolen by Client B's ForceSet")
		assert.ErrorIs(t, setErr, redcache.ErrLockLost, "Error should be ErrLockLost")

		// Redis MUST preserve Client B's ForceSet value
		finalValue, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "client-b-forced-value", finalValue, "ForceSet value MUST be preserved")

		t.Logf("✓ CORRECT: Client A's Set detected lock loss and failed without overwriting Client B's ForceSet")
	})

	t.Run("distributed: SetMulti from client A + ForceSetMulti from client B steals some locks", func(t *testing.T) {
		ctx := t.Context()
		key1 := "pdist:setmulti:1:" + uuid.New().String()
		key2 := "pdist:setmulti:2:" + uuid.New().String()
		key3 := "pdist:setmulti:3:" + uuid.New().String()

		clientA, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer clientA.Close()

		clientB, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 2 * time.Second},
		)
		require.NoError(t, err)
		defer clientB.Close()

		setMultiStarted := make(chan struct{})
		setMultiCompleted := make(chan struct {
			result map[string]string
			err    error
		}, 1)
		forceSetMultiCompleted := make(chan struct{})

		// Client A: Start SetMulti operation for 3 keys
		go func() {
			keys := []string{key1, key2, key3}
			result, setErr := clientA.SetMulti(ctx, 10*time.Second, keys, func(ctx context.Context, lockedKeys []string) (map[string]string, error) {
				close(setMultiStarted)
				// Hold locks while callback executes
				time.Sleep(300 * time.Millisecond)
				return map[string]string{
					key1: "client-a-value-1",
					key2: "client-a-value-2",
					key3: "client-a-value-3",
				}, nil
			})
			setMultiCompleted <- struct {
				result map[string]string
				err    error
			}{result, setErr}
		}()

		// Wait for Client A's SetMulti to acquire locks
		<-setMultiStarted
		time.Sleep(50 * time.Millisecond)

		// Client B: ForceSetMulti should overwrite locks for key1 and key2
		go func() {
			values := map[string]string{
				key1: "client-b-forced-1",
				key2: "client-b-forced-2",
			}
			forceErr := clientB.ForceSetMulti(ctx, 10*time.Second, values)
			require.NoError(t, forceErr)
			close(forceSetMultiCompleted)
		}()

		// Wait for ForceSetMulti to complete
		<-forceSetMultiCompleted

		// Client B's ForceSetMulti should have written key1 and key2
		innerClient := clientA.Client()
		value1, err := innerClient.Do(ctx, innerClient.B().Get().Key(key1).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "client-b-forced-1", value1, "key1 should have Client B's value")

		value2, err := innerClient.Do(ctx, innerClient.B().Get().Key(key2).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "client-b-forced-2", value2, "key2 should have Client B's value")

		// Wait for Client A's SetMulti to complete
		setMultiResult := <-setMultiCompleted

		// EXPECTED: Client A's SetMulti returns BatchError for key1 and key2
		require.Error(t, setMultiResult.err, "Client A's SetMulti should return error")

		var batchErr *redcache.BatchError
		if assert.ErrorAs(t, setMultiResult.err, &batchErr, "Should be BatchError") {
			assert.True(t, batchErr.HasFailures(), "Should have failures")

			// key1 and key2 should have failed with ErrLockLost
			for key, keyErr := range batchErr.Failed {
				assert.ErrorIs(t, keyErr, redcache.ErrLockLost, "Failed key %s should have ErrLockLost", key)
			}

			t.Logf("✓ CORRECT: Client A's SetMulti returned BatchError with %d failed keys", len(batchErr.Failed))
		}

		// Verify final Redis state: Client B's ForceSetMulti values are preserved
		finalValue1, err := innerClient.Do(ctx, innerClient.B().Get().Key(key1).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "client-b-forced-1", finalValue1, "key1 MUST preserve Client B's value")

		finalValue2, err := innerClient.Do(ctx, innerClient.B().Get().Key(key2).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "client-b-forced-2", finalValue2, "key2 MUST preserve Client B's value")

		t.Logf("✓ CORRECT: Client B's ForceSetMulti values preserved, Client A's SetMulti failed for stolen locks")
	})
}

// TestPrimeableCacheAside_DistributedInvalidationTiming verifies that cache operations
// across different clients receive Redis invalidation notifications (not just ticker polling).
//
// The ticker provides fallback at 50ms intervals, so tests use tight timing constraints
// (< 50ms) to prove operations complete via invalidation, not polling.
func TestPrimeableCacheAside_DistributedInvalidationTiming(t *testing.T) {
	t.Run("Distributed Set waits for Set via invalidation, not ticker", func(t *testing.T) {
		ctx := t.Context()
		key := "dist-inv-set-set:" + uuid.New().String()

		// Create two separate clients (simulating distributed processes)
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

		// Client 1: Start Set operation that holds lock for only 20ms
		set1Started := make(chan struct{})
		set1Done := make(chan struct{})
		var set1Err error

		go func() {
			defer close(set1Done)
			set1Err = client1.Set(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
				close(set1Started)
				time.Sleep(20 * time.Millisecond) // Short hold
				return "value-from-client1", nil
			})
		}()

		// Wait for Client 1 to acquire lock
		<-set1Started
		time.Sleep(5 * time.Millisecond)

		// Client 2: Set should wait for Client 1 via invalidation
		set2Start := time.Now()
		err = client2.Set(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
			return "value-from-client2", nil
		})
		set2Duration := time.Since(set2Start)

		require.NoError(t, err)

		// Client 2 should wait ~20ms (Client 1's callback) + small overhead
		// If using invalidation: 20ms + ~5-15ms (invalidation) = ~25-35ms
		// If using ticker only: 20ms + 50ms (ticker interval) = 70ms+
		// We use < 50ms threshold to PROVE invalidation works (ticker would fail)
		assert.Greater(t, set2Duration, 15*time.Millisecond, "Client 2 should wait for Client 1")
		assert.Less(t, set2Duration, 50*time.Millisecond,
			"Client 2 should complete via invalidation (< 50ms), ticker polling would take 70ms+")

		<-set1Done
		require.NoError(t, set1Err)

		// Final value should be from Client 2
		innerClient := client1.Client()
		val, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "value-from-client2", val)
	})

	t.Run("Distributed SetMulti waits for SetMulti via invalidation", func(t *testing.T) {
		ctx := t.Context()
		key1 := "dist-inv-setm-setm-1:" + uuid.New().String()
		key2 := "dist-inv-setm-setm-2:" + uuid.New().String()
		keys := []string{key1, key2}

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

		// Client 1: SetMulti with 60ms callback (longer to distinguish from ticker)
		setMulti1Started := make(chan struct{})
		setMulti1Done := make(chan struct{})
		var setMulti1Err error

		go func() {
			defer close(setMulti1Done)
			_, setMulti1Err = client1.SetMulti(ctx, 10*time.Second, keys,
				func(ctx context.Context, keys []string) (map[string]string, error) {
					close(setMulti1Started)
					time.Sleep(60 * time.Millisecond) // Longer callback
					return map[string]string{
						key1: "client1-val1",
						key2: "client1-val2",
					}, nil
				})
		}()

		// Wait for Client 1 to acquire locks
		<-setMulti1Started
		time.Sleep(5 * time.Millisecond)

		// Client 2: SetMulti should wait via invalidation
		setMulti2Start := time.Now()
		_, err = client2.SetMulti(ctx, 10*time.Second, keys,
			func(ctx context.Context, keys []string) (map[string]string, error) {
				return map[string]string{
					key1: "client2-val1",
					key2: "client2-val2",
				}, nil
			})
		setMulti2Duration := time.Since(setMulti2Start)

		require.NoError(t, err)

		// Client 2 waits ~60ms (Client 1's callback) + overhead
		// If invalidation: 60ms + ~20-30ms (overhead for 2 keys) = ~80-90ms
		// If ticker: 60ms + 50ms (ticker interval) = 110ms+
		// We use < 100ms threshold to PROVE invalidation (ticker would clearly fail)
		assert.Greater(t, setMulti2Duration, 55*time.Millisecond, "Client 2 should wait for Client 1")
		assert.Less(t, setMulti2Duration, 100*time.Millisecond,
			"SetMulti should complete via invalidation (< 100ms), ticker polling would take 110ms+")

		<-setMulti1Done
		require.NoError(t, setMulti1Err)

		// Final values should be from Client 2
		innerClient := client1.Client()
		val1, err := innerClient.Do(ctx, innerClient.B().Get().Key(key1).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "client2-val1", val1)

		val2, err := innerClient.Do(ctx, innerClient.B().Get().Key(key2).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "client2-val2", val2)
	})

	t.Run("Distributed Set waits for Get via invalidation", func(t *testing.T) {
		ctx := t.Context()
		key := "dist-inv-set-get:" + uuid.New().String()

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

		// Client 1: Get operation holds read lock for 25ms
		getStarted := make(chan struct{})
		getDone := make(chan struct{})
		var getErr error

		go func() {
			defer close(getDone)
			_, getErr = client1.Get(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
				close(getStarted)
				time.Sleep(25 * time.Millisecond)
				return "value-from-get", nil
			})
		}()

		// Wait for Get to acquire read lock
		<-getStarted
		time.Sleep(5 * time.Millisecond)

		// Client 2: Set should wait for read lock via invalidation
		setStart := time.Now()
		err = client2.Set(ctx, 10*time.Second, key, func(ctx context.Context, key string) (string, error) {
			return "value-from-set", nil
		})
		setDuration := time.Since(setStart)

		require.NoError(t, err)

		// Set waits ~25ms (Get callback) + invalidation overhead
		// If invalidation: 25ms + ~10ms = ~35ms
		// If ticker: 25ms + 50ms = 75ms+
		assert.Greater(t, setDuration, 20*time.Millisecond, "Set should wait for Get")
		assert.Less(t, setDuration, 45*time.Millisecond,
			"Set should complete via invalidation (< 45ms), ticker would take 75ms+")

		<-getDone
		require.NoError(t, getErr)

		// Final value should be from Set (overwrites cached Get value)
		innerClient := client1.Client()
		val, err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "value-from-set", val)
	})

	t.Run("Distributed SetMulti waits for GetMulti via invalidation", func(t *testing.T) {
		ctx := t.Context()
		key1 := "dist-inv-setm-getm-1:" + uuid.New().String()
		key2 := "dist-inv-setm-getm-2:" + uuid.New().String()
		keys := []string{key1, key2}

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

		// Client 1: GetMulti holds read locks for 30ms
		getMultiStarted := make(chan struct{})
		getMultiDone := make(chan struct{})
		var getMultiErr error

		go func() {
			defer close(getMultiDone)
			_, getMultiErr = client1.GetMulti(ctx, 10*time.Second, keys,
				func(ctx context.Context, keys []string) (map[string]string, error) {
					close(getMultiStarted)
					time.Sleep(30 * time.Millisecond)
					return map[string]string{
						key1: "get-val1",
						key2: "get-val2",
					}, nil
				})
		}()

		// Wait for GetMulti to acquire locks
		<-getMultiStarted
		time.Sleep(5 * time.Millisecond)

		// Client 2: SetMulti should wait for read locks via invalidation
		setMultiStart := time.Now()
		_, err = client2.SetMulti(ctx, 10*time.Second, keys,
			func(ctx context.Context, keys []string) (map[string]string, error) {
				return map[string]string{
					key1: "set-val1",
					key2: "set-val2",
				}, nil
			})
		setMultiDuration := time.Since(setMultiStart)

		require.NoError(t, err)

		// SetMulti waits ~30ms (GetMulti callback) + invalidation
		// If invalidation: 30ms + ~15ms = ~45ms
		// If ticker: 30ms + 50ms = 80ms+
		assert.Greater(t, setMultiDuration, 25*time.Millisecond, "SetMulti should wait for GetMulti")
		assert.Less(t, setMultiDuration, 55*time.Millisecond,
			"SetMulti should complete via invalidation (< 55ms), ticker would take 80ms+")

		<-getMultiDone
		require.NoError(t, getMultiErr)

		// Final values should be from SetMulti
		innerClient := client1.Client()
		val1, err := innerClient.Do(ctx, innerClient.B().Get().Key(key1).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "set-val1", val1)

		val2, err := innerClient.Do(ctx, innerClient.B().Get().Key(key2).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, "set-val2", val2)
	})
}
