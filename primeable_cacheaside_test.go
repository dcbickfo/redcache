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
	t.Parallel()
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
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	val1 := "val1:" + uuid.New().String()
	val2 := "val2:" + uuid.New().String()

	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return val1, nil
	})
	require.NoError(t, err)
	assert.Equal(t, val1, res)

	err = client.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return val2, nil
	})
	require.NoError(t, err)

	res, err = client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, val2, res)
}

func TestPrimeableCacheAside_Set_WaitsForExistingReadLock(t *testing.T) {
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	getVal := "get-val:" + uuid.New().String()
	setVal := "set-val:" + uuid.New().String()

	getStarted := make(chan struct{})
	getComplete := make(chan struct{})

	go func() {
		_, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
			close(getStarted)
			time.Sleep(200 * time.Millisecond)
			return getVal, nil
		})
		assert.NoError(t, err)
		close(getComplete)
	}()

	<-getStarted
	time.Sleep(50 * time.Millisecond)

	err := client.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return setVal, nil
	})
	require.NoError(t, err)

	<-getComplete

	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, setVal, res)
}

func TestPrimeableCacheAside_Set_Concurrent(t *testing.T) {
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	var callCount atomic.Int32
	var successCount atomic.Int32

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := client.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
				callCount.Add(1)
				return "val-from-" + uuid.New().String(), nil
			})
			if err == nil {
				successCount.Add(1)
				return
			}
			// Only ErrLockLost is acceptable on error.
			assert.ErrorIs(t, err, redcache.ErrLockLost, "iteration %d", i)
		}()
	}
	wg.Wait()

	assert.GreaterOrEqual(t, callCount.Load(), int32(1), "at least one callback should fire")
	assert.GreaterOrEqual(t, successCount.Load(), int32(1), "at least one Set must succeed (otherwise concurrent Sets are silently broken)")
}

func TestPrimeableCacheAside_SetMulti_Basic(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Sorted key order is what prevents deadlock under overlap.
	keys1 := []string{
		"key:a:" + uuid.New().String(),
		"key:b:" + uuid.New().String(),
		"key:c:" + uuid.New().String(),
	}
	keys2 := []string{
		keys1[1], // overlap on key:b.
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

	wg.Wait()
}

func TestPrimeableCacheAside_ForceSet_Basic(t *testing.T) {
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	val := "forced-val:" + uuid.New().String()

	err := client.ForceSet(ctx, time.Second*10, key, val)
	require.NoError(t, err)

	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, val, res)
}

func TestPrimeableCacheAside_ForceSet_StealsLock(t *testing.T) {
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	forcedVal := "forced:" + uuid.New().String()

	getStarted := make(chan struct{})
	getDone := make(chan struct{})

	go func() {
		defer close(getDone)
		_, _ = client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
			close(getStarted)
			time.Sleep(300 * time.Millisecond)
			return "get-val", nil
		})
	}()

	<-getStarted
	time.Sleep(50 * time.Millisecond)

	err := client.ForceSet(ctx, time.Second*10, key, forcedVal)
	require.NoError(t, err)

	// Get sees ErrLockLost and retries.
	select {
	case <-getDone:
	case <-time.After(5 * time.Second):
		t.Fatal("background Get did not complete after ForceSet")
	}

	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called — value should exist")
		return "", nil
	})
	require.NoError(t, err)
	assert.NotEmpty(t, res, "expected a value to be cached")
}

func TestPrimeableCacheAside_ForceSetMulti_Basic(t *testing.T) {
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	values := map[string]string{
		"key:0:" + uuid.New().String(): "val:0:" + uuid.New().String(),
		"key:1:" + uuid.New().String(): "val:1:" + uuid.New().String(),
	}

	err := client.ForceSetMulti(ctx, time.Second*10, values)
	require.NoError(t, err)

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
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()

	key := "key:" + uuid.New().String()

	// Lock the key so Set waits.
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
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()

	innerClient := client.Client()
	lockVal := "__redcache:lock:" + uuid.New().String()
	err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(lockVal).Nx().Get().Px(time.Second*30).Build()).Error()
	require.True(t, rueidis.IsRedisNil(err))

	// Bound Set so it can't loop forever after Close.
	setCtx, setCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer setCancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.Set(setCtx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
			return "val", nil
		})
	}()

	time.Sleep(100 * time.Millisecond)

	client.Close()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Set did not return after Close")
	case err := <-errCh:
		// External lock persists; Set must surface the deadline.
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
}

func TestPrimeableCacheAside_SetMulti_ContextCancellation(t *testing.T) {
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()

	keys := []string{
		"key:0:" + uuid.New().String(),
		"key:1:" + uuid.New().String(),
	}

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
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	cbErr := fmt.Errorf("set callback failed")

	err := client.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return "", cbErr
	})
	require.ErrorIs(t, err, cbErr)

	// Lock cleanup lets a follow-up Set succeed.
	val := "good-val:" + uuid.New().String()
	err = client.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return val, nil
	})
	require.NoError(t, err)

	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, val, res)
}

func TestPrimeableCacheAside_Set_CallbackError_RestoresValue(t *testing.T) {
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	originalVal := "original:" + uuid.New().String()

	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return originalVal, nil
	})
	require.NoError(t, err)
	assert.Equal(t, originalVal, res)

	cbErr := fmt.Errorf("set callback failed")
	err = client.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return "", cbErr
	})
	require.ErrorIs(t, err, cbErr)

	// Allow invalidation to propagate.
	time.Sleep(100 * time.Millisecond)

	res, err = client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		// Callback firing here means the rollback DELed instead of restoring.
		return originalVal, nil
	})
	require.NoError(t, err)
	assert.Equal(t, originalVal, res)
}

func TestPrimeableCacheAside_SetMulti_CallbackError_RestoresValues(t *testing.T) {
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	keys := []string{
		"key:0:" + uuid.New().String(),
		"key:1:" + uuid.New().String(),
	}

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

	cbErr := fmt.Errorf("setmulti callback failed")
	err = client.SetMulti(ctx, time.Second*10, keys, func(ctx context.Context, ks []string) (map[string]string, error) {
		return nil, cbErr
	})
	require.ErrorIs(t, err, cbErr)

	// Use a t.Fatal callback so a buggy DEL-and-recover rollback is detected
	// rather than masked by a re-populate.
	time.Sleep(100 * time.Millisecond)
	res, err = client.GetMulti(ctx, time.Second*10, keys, func(_ context.Context, _ []string) (map[string]string, error) {
		t.Fatal("rollback failed: callback fired, meaning the original value was DELed, not restored")
		return nil, nil
	})
	require.NoError(t, err)
	if diff := cmp.Diff(originalVals, res); diff != "" {
		t.Errorf("values after rollback mismatch (-want +got):\n%s", diff)
	}
}

func TestPrimeableCacheAside_SetMulti_PartialCASFailure_BatchError(t *testing.T) {
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key1 := "key:0:" + uuid.New().String()
	key2 := "key:1:" + uuid.New().String()
	keys := []string{key1, key2}

	forcedVal := "forced:" + uuid.New().String()
	err := client.SetMulti(ctx, time.Second*10, keys, func(ctx context.Context, ks []string) (map[string]string, error) {
		// Steal key2's lock between acquisition and CAS write.
		forceErr := client.ForceSet(ctx, time.Second*10, key2, forcedVal)
		if forceErr != nil {
			return nil, forceErr
		}
		return map[string]string{
			key1: "val1:" + uuid.New().String(),
			key2: "val2:" + uuid.New().String(),
		}, nil
	})

	// Stolen lock must surface as a BatchError; silently returning nil would
	// mask the partial failure.
	require.Error(t, err, "SetMulti must report partial CAS failure")
	var batchErr *redcache.BatchError
	require.ErrorAs(t, err, &batchErr)
	assert.True(t, batchErr.HasFailures())
	assert.Contains(t, batchErr.Failed, key2, "key2 should have failed CAS")
	assert.ErrorIs(t, batchErr.Failed[key2], redcache.ErrLockLost)
	res, getErr := client.Get(ctx, time.Second*10, key2, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called — forced value should exist")
		return "", nil
	})
	require.NoError(t, getErr)
	assert.Equal(t, forcedVal, res)
}

func TestPrimeableCacheAside_ForceSet_OverwritesExistingValue(t *testing.T) {
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	originalVal := "original:" + uuid.New().String()
	forcedVal := "forced:" + uuid.New().String()

	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return originalVal, nil
	})
	require.NoError(t, err)
	assert.Equal(t, originalVal, res)

	err = client.ForceSet(ctx, time.Second*10, key, forcedVal)
	require.NoError(t, err)

	// Allow invalidation to propagate to the client-side cache.
	time.Sleep(100 * time.Millisecond)

	res, err = client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, forcedVal, res)
}

func TestNewPrimeableCacheAside_Validation(t *testing.T) {
	t.Parallel()
	t.Run("empty InitAddress", func(t *testing.T) {
		t.Parallel()
		_, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{},
			redcache.CacheAsideOption{},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "InitAddress")
	})

	t.Run("negative LockTTL", func(t *testing.T) {
		t.Parallel()
		_, err := redcache.NewPrimeableCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: -1 * time.Second},
		)
		require.Error(t, err)
	})
}

func TestPrimeableCacheAside_MultiClient_SetGet(t *testing.T) {
	t.Parallel()
	client1 := makePrimeableClient(t, addr)
	defer client1.Client().Close()
	client2 := makePrimeableClient(t, addr)
	defer client2.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	setVal := "set-val:" + uuid.New().String()

	err := client1.Set(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return setVal, nil
	})
	require.NoError(t, err)

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
	t.Parallel()
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

	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called — value should exist")
		return "", nil
	})
	require.NoError(t, err)
	assert.NotEmpty(t, res)
}

func TestPrimeableCacheAside_SetMulti_EmptyKeys(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	err := client.ForceSetMulti(ctx, time.Second*10, nil)
	require.NoError(t, err)

	err = client.ForceSetMulti(ctx, time.Second*10, map[string]string{})
	require.NoError(t, err)
}
