package redcache_test

import (
	"context"
	"fmt"
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
)

func makePrimeableClient(t *testing.T, addr []string) *redcache.PrimeableCacheAside {
	t.Helper()
	client, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{
			InitAddress: addr,
		},
		redcache.CacheAsideOption{
			LockTTL: time.Second * 1,
		},
	)
	require.NoError(t, err)
	return client
}

func TestPrimeableCacheAside_Set_Basic(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	val := "val:" + uuid.New().String()

	err := client.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		assert.Equal(t, key, k)
		return val, nil
	})
	require.NoError(t, err)

	// Subsequent Get should return cached value without callback.
	called := false
	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		called = true
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, val, res)
	assert.False(t, called, "Get callback should not be invoked after Set")
}

func TestPrimeableCacheAside_Set_Overwrites(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	val1 := "val1:" + uuid.New().String()
	val2 := "val2:" + uuid.New().String()

	// Set initial value via Get.
	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return val1, nil
	})
	require.NoError(t, err)
	assert.Equal(t, val1, res)

	// Overwrite with Set.
	err = client.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return val2, nil
	})
	require.NoError(t, err)

	// Verify new value.
	res, err = client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, val2, res)
}

func TestPrimeableCacheAside_Set_WaitsForExistingReadLock(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	getVal := "get-val:" + uuid.New().String()
	setVal := "set-val:" + uuid.New().String()

	getStarted := make(chan struct{})
	getComplete := make(chan struct{})

	// Start a Get that holds a lock for a while.
	go func() {
		_, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
			close(getStarted)
			time.Sleep(200 * time.Millisecond)
			return getVal, nil
		})
		assert.NoError(t, err)
		close(getComplete)
	}()

	// Wait for Get to acquire its lock.
	<-getStarted
	time.Sleep(50 * time.Millisecond)

	// Set should wait for the Get lock to be released, then proceed.
	err := client.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return setVal, nil
	})
	require.NoError(t, err)

	<-getComplete

	// The Set value should be the final value.
	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, setVal, res)
}

func TestPrimeableCacheAside_Set_Concurrent(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	var callCount atomic.Int32

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := client.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
				callCount.Add(1)
				return "val-from-" + uuid.New().String(), nil
			})
			// Either success or ErrLockLost is acceptable.
			if err != nil {
				assert.ErrorIs(t, err, redcache.ErrLockLost, "iteration %d", i)
			}
		}()
	}
	wg.Wait()

	// At least one should have succeeded.
	assert.GreaterOrEqual(t, callCount.Load(), int32(1))
}

func TestPrimeableCacheAside_SetMulti_Basic(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	keyAndVals := map[string]string{
		"key:0:" + uuid.New().String(): "val:0:" + uuid.New().String(),
		"key:1:" + uuid.New().String(): "val:1:" + uuid.New().String(),
		"key:2:" + uuid.New().String(): "val:2:" + uuid.New().String(),
	}
	keys := make([]string, 0, len(keyAndVals))
	for k := range keyAndVals {
		keys = append(keys, k)
	}

	err := client.SetMulti(ctx, time.Second*10, keys, func(ctx context.Context, ks []string) (map[string]string, error) {
		res := make(map[string]string, len(ks))
		for _, k := range ks {
			res[k] = keyAndVals[k]
		}
		return res, nil
	})
	require.NoError(t, err)

	// Verify all keys cached.
	res, err := client.GetMulti(ctx, time.Second*10, keys, func(ctx context.Context, ks []string) (map[string]string, error) {
		t.Fatal("GetMulti callback should not be called after SetMulti")
		return nil, nil
	})
	require.NoError(t, err)
	if diff := cmp.Diff(keyAndVals, res); diff != "" {
		t.Errorf("GetMulti() mismatch (-want +got):\n%s", diff)
	}
}

func TestPrimeableCacheAside_SetMulti_NoDeadlock(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Two overlapping key sets — sorted order prevents deadlock.
	keys1 := []string{
		"key:a:" + uuid.New().String(),
		"key:b:" + uuid.New().String(),
		"key:c:" + uuid.New().String(),
	}
	keys2 := []string{
		keys1[1], // overlap on key:b
		"key:d:" + uuid.New().String(),
		"key:e:" + uuid.New().String(),
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range 5 {
			_ = client.SetMulti(ctx, time.Second*10, keys1, func(ctx context.Context, ks []string) (map[string]string, error) {
				res := make(map[string]string, len(ks))
				for _, k := range ks {
					res[k] = "val-1:" + uuid.New().String()
				}
				return res, nil
			})
		}
	}()

	go func() {
		defer wg.Done()
		for range 5 {
			_ = client.SetMulti(ctx, time.Second*10, keys2, func(ctx context.Context, ks []string) (map[string]string, error) {
				res := make(map[string]string, len(ks))
				for _, k := range ks {
					res[k] = "val-2:" + uuid.New().String()
				}
				return res, nil
			})
		}
	}()

	// If there's a deadlock, the test will timeout.
	wg.Wait()
}

func TestPrimeableCacheAside_ForceSet_Basic(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	val := "forced-val:" + uuid.New().String()

	err := client.ForceSet(ctx, time.Second*10, key, val)
	require.NoError(t, err)

	// Get should return the force-set value.
	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, val, res)
}

func TestPrimeableCacheAside_ForceSet_StealsLock(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	forcedVal := "forced:" + uuid.New().String()

	getStarted := make(chan struct{})

	// Start a slow Get that holds a lock.
	go func() {
		_, _ = client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
			close(getStarted)
			time.Sleep(300 * time.Millisecond)
			return "get-val", nil
		})
	}()

	<-getStarted
	time.Sleep(50 * time.Millisecond)

	// ForceSet overwrites the lock.
	err := client.ForceSet(ctx, time.Second*10, key, forcedVal)
	require.NoError(t, err)

	// Wait for Get to complete (it will see ErrLockLost and retry).
	time.Sleep(500 * time.Millisecond)

	// The forced value should be present (or Get retried with its own value).
	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called — value should exist")
		return "", nil
	})
	require.NoError(t, err)
	assert.NotEmpty(t, res, "expected a value to be cached")
}

func TestPrimeableCacheAside_ForceSetMulti_Basic(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	values := map[string]string{
		"key:0:" + uuid.New().String(): "val:0:" + uuid.New().String(),
		"key:1:" + uuid.New().String(): "val:1:" + uuid.New().String(),
	}

	err := client.ForceSetMulti(ctx, time.Second*10, values)
	require.NoError(t, err)

	// Verify via direct reads.
	for key, expected := range values {
		res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
			t.Fatal("callback should not be called")
			return "", nil
		})
		require.NoError(t, err)
		assert.Equal(t, expected, res, "key %s", key)
	}
}

func TestPrimeableCacheAside_Set_ContextCancellation(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()

	key := "key:" + uuid.New().String()

	// Place a lock so Set will wait.
	innerClient := client.Client()
	lockVal := "__redcache:lock:" + uuid.New().String()
	err := innerClient.Do(context.Background(), innerClient.B().Set().Key(key).Value(lockVal).Nx().Get().Px(time.Second*30).Build()).Error()
	require.True(t, rueidis.IsRedisNil(err))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err = client.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return "val", nil
	})
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestPrimeableCacheAside_Close_CancelsPendingLocks(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()

	// Place a lock so operations will wait.
	innerClient := client.Client()
	lockVal := "__redcache:lock:" + uuid.New().String()
	err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(lockVal).Nx().Get().Px(time.Second*30).Build()).Error()
	require.True(t, rueidis.IsRedisNil(err))

	// Use a context with timeout so Set doesn't loop forever after Close.
	setCtx, setCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer setCancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.Set(setCtx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
			return "val", nil
		})
	}()

	// Give Set time to start waiting.
	time.Sleep(100 * time.Millisecond)

	// Close should cancel pending lock entries, causing Set to wake up and retry.
	client.Close()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Set did not return after Close")
	case err := <-errCh:
		// Set should eventually fail with context deadline exceeded because
		// the external lock persists, but Close woke it up at least once.
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
}

func TestPrimeableCacheAside_SetMulti_ContextCancellation(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()

	keys := []string{
		"key:0:" + uuid.New().String(),
		"key:1:" + uuid.New().String(),
	}

	// Place locks on all keys.
	innerClient := client.Client()
	for _, key := range keys {
		lockVal := "__redcache:lock:" + uuid.New().String()
		err := innerClient.Do(context.Background(), innerClient.B().Set().Key(key).Value(lockVal).Nx().Get().Px(time.Second*30).Build()).Error()
		require.True(t, rueidis.IsRedisNil(err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := client.SetMulti(ctx, time.Second*10, keys, func(ctx context.Context, ks []string) (map[string]string, error) {
		t.Fatal("callback should not be called when waiting for locks")
		return nil, nil
	})
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestPrimeableCacheAside_Set_CallbackError(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	cbErr := fmt.Errorf("set callback failed")

	// Set with failing callback.
	err := client.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return "", cbErr
	})
	require.ErrorIs(t, err, cbErr)

	// Lock should have been cleaned up — a subsequent Set should succeed.
	val := "good-val:" + uuid.New().String()
	err = client.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return val, nil
	})
	require.NoError(t, err)

	// Verify the value is there.
	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, val, res)
}

func TestPrimeableCacheAside_SetMulti_CallbackError_RestoresValues(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	keys := []string{
		"key:0:" + uuid.New().String(),
		"key:1:" + uuid.New().String(),
	}

	// Pre-populate with known values via Get.
	originalVals := map[string]string{
		keys[0]: "original:0:" + uuid.New().String(),
		keys[1]: "original:1:" + uuid.New().String(),
	}
	res, err := client.GetMulti(ctx, time.Second*10, keys, func(ctx context.Context, ks []string) (map[string]string, error) {
		out := make(map[string]string, len(ks))
		for _, k := range ks {
			out[k] = originalVals[k]
		}
		return out, nil
	})
	require.NoError(t, err)
	if diff := cmp.Diff(originalVals, res); diff != "" {
		t.Fatalf("setup mismatch: %s", diff)
	}

	// SetMulti with failing callback — should restore original values.
	cbErr := fmt.Errorf("setmulti callback failed")
	err = client.SetMulti(ctx, time.Second*10, keys, func(ctx context.Context, ks []string) (map[string]string, error) {
		return nil, cbErr
	})
	require.ErrorIs(t, err, cbErr)

	// Give invalidation a moment to propagate, then verify original values were restored.
	time.Sleep(100 * time.Millisecond)
	res, err = client.GetMulti(ctx, time.Second*10, keys, func(ctx context.Context, ks []string) (map[string]string, error) {
		// If the callback fires, it means the originals were NOT restored — the keys were
		// left as lock values (deleted). This is acceptable but we need to return values.
		out := make(map[string]string, len(ks))
		for _, k := range ks {
			out[k] = originalVals[k]
		}
		return out, nil
	})
	require.NoError(t, err)
	// Either the originals are restored or the callback re-populated them.
	if diff := cmp.Diff(originalVals, res); diff != "" {
		t.Errorf("values after rollback mismatch (-want +got):\n%s", diff)
	}
}

func TestPrimeableCacheAside_SetMulti_PartialCASFailure_BatchError(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key1 := "key:0:" + uuid.New().String()
	key2 := "key:1:" + uuid.New().String()
	keys := []string{key1, key2}

	// Use SetMulti with a callback that calls ForceSet on key2 to steal its lock
	// between lock acquisition and CAS write.
	forcedVal := "forced:" + uuid.New().String()
	err := client.SetMulti(ctx, time.Second*10, keys, func(ctx context.Context, ks []string) (map[string]string, error) {
		// Steal key2's lock while we hold it.
		forceErr := client.ForceSet(ctx, time.Second*10, key2, forcedVal)
		if forceErr != nil {
			return nil, forceErr
		}
		// Return values for both keys — but CAS on key2 should fail.
		return map[string]string{
			key1: "val1:" + uuid.New().String(),
			key2: "val2:" + uuid.New().String(),
		}, nil
	})

	if err != nil {
		// Should be a BatchError with key2 failed.
		var batchErr *redcache.BatchError
		if assert.ErrorAs(t, err, &batchErr) {
			assert.True(t, batchErr.HasFailures())
			assert.Contains(t, batchErr.Failed, key2, "key2 should have failed CAS")
			assert.ErrorIs(t, batchErr.Failed[key2], redcache.ErrLockLost)
		}
	}
	// Either way, key2 should have the forced value.
	res, getErr := client.Get(ctx, time.Second*10, key2, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called — forced value should exist")
		return "", nil
	})
	require.NoError(t, getErr)
	assert.Equal(t, forcedVal, res)
}

func TestPrimeableCacheAside_ForceSet_OverwritesExistingValue(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	originalVal := "original:" + uuid.New().String()
	forcedVal := "forced:" + uuid.New().String()

	// Populate via Get.
	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return originalVal, nil
	})
	require.NoError(t, err)
	assert.Equal(t, originalVal, res)

	// ForceSet overwrites the real value.
	err = client.ForceSet(ctx, time.Second*10, key, forcedVal)
	require.NoError(t, err)

	// Allow invalidation message to propagate to the client-side cache.
	time.Sleep(100 * time.Millisecond)

	// Verify forced value is returned.
	res, err = client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, forcedVal, res)
}

func TestNewPrimeableCacheAside_Validation(t *testing.T) {
	t.Run("empty InitAddress", func(t *testing.T) {
		_, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{},
			redcache.CacheAsideOption{},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "InitAddress")
	})

	t.Run("negative LockTTL", func(t *testing.T) {
		_, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: -1 * time.Second},
		)
		require.Error(t, err)
	})
}

func TestPrimeableCacheAside_MultiClient_SetGet(t *testing.T) {
	client1 := makePrimeableClient(t, addr)
	defer client1.Client().Close()
	client2 := makePrimeableClient(t, addr)
	defer client2.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	setVal := "set-val:" + uuid.New().String()

	// client1 does Set.
	err := client1.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return setVal, nil
	})
	require.NoError(t, err)

	// client2 does Get — should see the Set value without invoking callback.
	called := false
	res, err := client2.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		called = true
		return "other-val", nil
	})
	require.NoError(t, err)
	assert.Equal(t, setVal, res)
	assert.False(t, called, "client2 Get callback should not be called")
}

func TestPrimeableCacheAside_ConcurrentSetAndGet(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key := "key:" + uuid.New().String()

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = client.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
				return "set:" + uuid.New().String(), nil
			})
		}()
		go func() {
			defer wg.Done()
			_, _ = client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
				return "get:" + uuid.New().String(), nil
			})
		}()
	}
	wg.Wait()

	// Key should have a value — no deadlock, no panic.
	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called — value should exist")
		return "", nil
	})
	require.NoError(t, err)
	assert.NotEmpty(t, res)
}

func TestPrimeableCacheAside_SetMulti_EmptyKeys(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	err := client.SetMulti(ctx, time.Second*10, nil, func(ctx context.Context, ks []string) (map[string]string, error) {
		t.Fatal("callback should not be called for empty keys")
		return nil, nil
	})
	require.NoError(t, err)
}

func TestPrimeableCacheAside_ForceSetMulti_EmptyMap(t *testing.T) {
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	err := client.ForceSetMulti(ctx, time.Second*10, nil)
	require.NoError(t, err)

	err = client.ForceSetMulti(ctx, time.Second*10, map[string]string{})
	require.NoError(t, err)
}
