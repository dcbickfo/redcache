package redcache_test

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
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

var addr = []string{"127.0.0.1:6379"}

func makeClient(t *testing.T, addr []string) *redcache.CacheAside {
	client, err := redcache.NewRedCacheAside(
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

func TestCacheAside_Get(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()
	key := "key:" + uuid.New().String()
	val := "val:" + uuid.New().String()
	called := false

	cb := func(ctx context.Context, key string) (string, error) {
		called = true
		return val, nil
	}

	res, err := client.Get(ctx, time.Second*10, key, cb)
	require.NoError(t, err)
	if diff := cmp.Diff(val, res); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.True(t, called)

	called = false
	res, err = client.Get(ctx, time.Second*10, key, cb)
	require.NoError(t, err)
	if diff := cmp.Diff(val, res); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.False(t, called)
}

func TestCacheAside_GetMulti(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()
	keyAndVals := make(map[string]string)
	for i := range 3 {
		keyAndVals[fmt.Sprintf("key:%d:%s", i, uuid.New().String())] = fmt.Sprintf("val:%d:%s", i, uuid.New().String())
	}
	keys := make([]string, 0, len(keyAndVals))
	for k := range keyAndVals {
		keys = append(keys, k)
	}
	called := false

	cb := func(ctx context.Context, keys []string) (map[string]string, error) {
		called = true
		res := make(map[string]string, len(keys))
		for _, key := range keys {
			res[key] = keyAndVals[key]
		}
		return res, nil
	}

	res, err := client.GetMulti(ctx, time.Second*10, keys, cb)
	require.NoError(t, err)
	if diff := cmp.Diff(keyAndVals, res); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.True(t, called)

	called = false
	res, err = client.GetMulti(ctx, time.Second*10, keys, cb)
	require.NoError(t, err)
	if diff := cmp.Diff(keyAndVals, res); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.False(t, called)
}

func TestCacheAside_GetMulti_Partial(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()
	keyAndVals := make(map[string]string)
	for i := range 3 {
		keyAndVals[fmt.Sprintf("key:%d:%s", i, uuid.New().String())] = fmt.Sprintf("val:%d:%s", i, uuid.New().String())
	}
	keys := make([]string, 0, len(keyAndVals))
	for k := range keyAndVals {
		keys = append(keys, k)
	}
	called := false

	cbSingle := func(ctx context.Context, key string) (string, error) {
		called = true
		return keyAndVals[key], nil
	}

	cbRes := make(map[string]string)
	cb := func(ctx context.Context, keys []string) (map[string]string, error) {
		called = true
		for _, key := range keys {
			cbRes[key] = keyAndVals[key]
		}
		return cbRes, nil
	}

	res, err := client.Get(ctx, time.Second*10, keys[0], cbSingle)
	require.NoError(t, err)
	if diff := cmp.Diff(keyAndVals[keys[0]], res); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.True(t, called)

	called = false
	resMulti, err := client.GetMulti(ctx, time.Second*10, keys, cb)
	require.NoError(t, err)
	if diff := cmp.Diff(keyAndVals, resMulti); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	wantReq := make(map[string]string)
	for i, key := range keys {
		if i == 0 {
			continue
		}
		wantReq[key] = keyAndVals[key]
	}
	if diff := cmp.Diff(cbRes, wantReq); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.True(t, called)

	called = false
	resMulti, err = client.GetMulti(ctx, time.Second*10, keys, cb)
	require.NoError(t, err)
	if diff := cmp.Diff(keyAndVals, resMulti); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.False(t, called)
}

func TestCacheAside_GetMulti_PartLock(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()
	keyAndVals := make(map[string]string)
	for i := range 3 {
		keyAndVals[fmt.Sprintf("key:%d:%s", i, uuid.New().String())] = fmt.Sprintf("val:%d:%s", i, uuid.New().String())
	}
	keys := make([]string, 0, len(keyAndVals))
	for k := range keyAndVals {
		keys = append(keys, k)
	}
	called := false

	cb := func(ctx context.Context, keys []string) (map[string]string, error) {
		called = true
		res := make(map[string]string, len(keys))
		for _, key := range keys {
			res[key] = keyAndVals[key]
		}
		return res, nil
	}

	innerClient := client.Client()
	lockVal := "__redcache:lock:" + uuid.New().String()
	err := innerClient.Do(ctx, innerClient.B().Set().Key(keys[0]).Value(lockVal).Nx().Get().Px(time.Millisecond*100).Build()).Error()
	require.True(t, rueidis.IsRedisNil(err))

	res, err := client.GetMulti(ctx, time.Second*10, keys, cb)
	require.NoError(t, err)
	if diff := cmp.Diff(keyAndVals, res); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.True(t, called)

	called = false
	res, err = client.GetMulti(ctx, time.Second*10, keys, cb)
	require.NoError(t, err)
	if diff := cmp.Diff(keyAndVals, res); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.False(t, called)
}

func TestCacheAside_Del(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	val := "val:" + uuid.New().String()

	innerClient := client.Client()
	err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(val).Nx().Get().Px(time.Millisecond*100).Build()).Error()
	require.True(t, rueidis.IsRedisNil(err))

	err = innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
	require.NoErrorf(t, err, "expected no error, got %v", err)

	err = client.Del(ctx, key)
	require.NoError(t, err)

	err = innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
	require.True(t, rueidis.IsRedisNil(err))
}

func TestCBWrapper_GetMultiCheckConcurrent(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()
	client2 := makeClient(t, addr)
	defer client2.Client().Close()

	ctx := context.Background()
	keyAndVals := make(map[string]string)
	for i := range 6 {
		keyAndVals[fmt.Sprintf("key:%d:%s", i, uuid.New().String())] = fmt.Sprintf("val:%d:%s", i, uuid.New().String())
	}
	keys := make([]string, 0, len(keyAndVals))
	for k := range keyAndVals {
		keys = append(keys, k)
	}

	cb := func(ctx context.Context, keys []string) (map[string]string, error) {
		res := make(map[string]string, len(keys))
		for _, key := range keys {
			res[key] = keyAndVals[key]
		}
		return res, nil
	}

	wg := sync.WaitGroup{}

	expected1 := map[string]string{
		keys[0]: keyAndVals[keys[0]],
		keys[1]: keyAndVals[keys[1]],
		keys[2]: keyAndVals[keys[2]],
	}

	expected2 := map[string]string{
		keys[3]: keyAndVals[keys[3]],
		keys[4]: keyAndVals[keys[4]],
		keys[5]: keyAndVals[keys[5]],
	}

	for i := 0; i < 100; i++ {
		wg.Add(4)
		go func() {
			defer wg.Done()
			out, err := client.GetMulti(
				ctx,
				time.Second*10,
				keys[:3],
				cb,
			)
			assert.NoError(t, err)
			if diff := cmp.Diff(expected1, out); diff != "" {
				t.Errorf("GetMulti() mismatch (-want +got):\n%s", diff)
			}
		}()
		go func() {
			defer wg.Done()
			out, err := client2.GetMulti(
				ctx,
				time.Second*10,
				keys[:3],
				cb,
			)
			assert.NoError(t, err)
			if diff := cmp.Diff(expected1, out); diff != "" {
				t.Errorf("GetMulti() mismatch (-want +got):\n%s", diff)
			}
		}()
		go func() {
			defer wg.Done()
			out, err := client.GetMulti(
				context.Background(),
				time.Second*10,
				keys[3:],
				cb)
			assert.NoError(t, err)
			if diff := cmp.Diff(expected2, out); diff != "" {
				t.Errorf("GetMulti() mismatch (-want +got):\n%s", diff)
			}
		}()
		go func() {
			defer wg.Done()
			out, err := client2.GetMulti(
				context.Background(),
				time.Second*10,
				keys[3:],
				cb)
			assert.NoError(t, err)
			if diff := cmp.Diff(expected2, out); diff != "" {
				t.Errorf("GetMulti() mismatch (-want +got):\n%s", diff)
			}
		}()
	}
	wg.Wait()
}

func TestCBWrapper_GetMultiCheckConcurrentOverlapDifferentClients(t *testing.T) {
	client1 := makeClient(t, addr)
	defer client1.Client().Close()
	client2 := makeClient(t, addr)
	defer client2.Client().Close()
	client3 := makeClient(t, addr)
	defer client3.Client().Close()
	client4 := makeClient(t, addr)
	defer client4.Client().Close()

	ctx := context.Background()
	keyAndVals := make(map[string]string)
	for i := range 6 {
		keyAndVals[fmt.Sprintf("key:%d:%s", i, uuid.New().String())] = fmt.Sprintf("val:%d:%s", i, uuid.New().String())
	}
	keys := make([]string, 0, len(keyAndVals))
	for k := range keyAndVals {
		keys = append(keys, k)
	}

	cb := func(ctx context.Context, keys []string) (map[string]string, error) {
		res := make(map[string]string, len(keys))
		for _, key := range keys {
			res[key] = keyAndVals[key]
		}
		return res, nil
	}

	wg := sync.WaitGroup{}

	keys1 := []string{keys[0], keys[1], keys[3]}
	expected1 := map[string]string{
		keys[0]: keyAndVals[keys[0]],
		keys[1]: keyAndVals[keys[1]],
		keys[3]: keyAndVals[keys[3]],
	}

	keys2 := []string{keys[2], keys[4], keys[5]}
	expected2 := map[string]string{
		keys[2]: keyAndVals[keys[2]],
		keys[4]: keyAndVals[keys[4]],
		keys[5]: keyAndVals[keys[5]],
	}

	keys3 := []string{keys[0], keys[2], keys[4]}
	expected3 := map[string]string{
		keys[0]: keyAndVals[keys[0]],
		keys[2]: keyAndVals[keys[2]],
		keys[4]: keyAndVals[keys[4]],
	}

	keys4 := []string{keys[1], keys[3], keys[5]}
	expected4 := map[string]string{
		keys[1]: keyAndVals[keys[1]],
		keys[3]: keyAndVals[keys[3]],
		keys[5]: keyAndVals[keys[5]],
	}

	for i := 0; i < 100; i++ {
		wg.Add(4)
		go func() {
			defer wg.Done()
			localKeys := slices.Clone(keys1)
			rand.Shuffle(len(localKeys), func(i, j int) {
				localKeys[i], localKeys[j] = localKeys[j], localKeys[i]
			})
			out, err := client1.GetMulti(
				ctx,
				time.Second*10,
				localKeys,
				cb,
			)
			assert.NoError(t, err)
			if diff := cmp.Diff(expected1, out); diff != "" {
				t.Errorf("GetMulti() mismatch in 1 (-want +got):\n%s", diff)
				t.Errorf("got: %v", out)
			}
		}()
		go func() {
			defer wg.Done()
			localKeys := slices.Clone(keys2)
			rand.Shuffle(len(localKeys), func(i, j int) {
				localKeys[i], localKeys[j] = localKeys[j], localKeys[i]
			})
			out, err := client2.GetMulti(
				ctx,
				time.Second*10,
				localKeys,
				cb,
			)
			assert.NoError(t, err)
			if diff := cmp.Diff(expected2, out); diff != "" {
				t.Errorf("GetMulti() mismatch in 2 (-want +got):\n%s", diff)
				t.Errorf("got: %v", out)
			}
		}()
		go func() {
			defer wg.Done()
			localKeys := slices.Clone(keys3)
			rand.Shuffle(len(localKeys), func(i, j int) {
				localKeys[i], localKeys[j] = localKeys[j], localKeys[i]
			})
			out, err := client3.GetMulti(
				context.Background(),
				time.Second*10,
				localKeys,
				cb)
			assert.NoError(t, err)
			if diff := cmp.Diff(expected3, out); diff != "" {
				t.Errorf("GetMulti() mismatch in 3 (-want +got):\n%s", diff)
				t.Errorf("got: %v", out)
			}
		}()
		go func() {
			defer wg.Done()
			localKeys := slices.Clone(keys4)
			rand.Shuffle(len(localKeys), func(i, j int) {
				localKeys[i], localKeys[j] = localKeys[j], localKeys[i]
			})
			out, err := client4.GetMulti(
				context.Background(),
				time.Second*10,
				localKeys,
				cb)
			assert.NoError(t, err)
			if diff := cmp.Diff(expected4, out); diff != "" {
				t.Errorf("GetMulti() mismatch in 4 (-want +got):\n%s", diff)
				t.Errorf("got: %v", out)
			}
		}()
	}
	wg.Wait()
}

func TestCBWrapper_GetMultiCheckConcurrentOverlap(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()

	ctx := context.Background()
	keyAndVals := make(map[string]string)
	for i := range 6 {
		keyAndVals[fmt.Sprintf("key:%d:%s", i, uuid.New().String())] = fmt.Sprintf("val:%d:%s", i, uuid.New().String())
	}
	keys := make([]string, 0, len(keyAndVals))
	for k := range keyAndVals {
		keys = append(keys, k)
	}

	cb := func(ctx context.Context, keys []string) (map[string]string, error) {
		res := make(map[string]string, len(keys))
		for _, key := range keys {
			res[key] = keyAndVals[key]
		}
		return res, nil
	}

	wg := sync.WaitGroup{}

	keys1 := []string{keys[0], keys[1], keys[3]}
	expected1 := map[string]string{
		keys[0]: keyAndVals[keys[0]],
		keys[1]: keyAndVals[keys[1]],
		keys[3]: keyAndVals[keys[3]],
	}

	keys2 := []string{keys[2], keys[4], keys[5]}
	expected2 := map[string]string{
		keys[2]: keyAndVals[keys[2]],
		keys[4]: keyAndVals[keys[4]],
		keys[5]: keyAndVals[keys[5]],
	}

	keys3 := []string{keys[0], keys[2], keys[4]}
	expected3 := map[string]string{
		keys[0]: keyAndVals[keys[0]],
		keys[2]: keyAndVals[keys[2]],
		keys[4]: keyAndVals[keys[4]],
	}

	keys4 := []string{keys[1], keys[3], keys[5]}
	expected4 := map[string]string{
		keys[1]: keyAndVals[keys[1]],
		keys[3]: keyAndVals[keys[3]],
		keys[5]: keyAndVals[keys[5]],
	}

	for i := 0; i < 100; i++ {
		wg.Add(4)
		go func() {
			defer wg.Done()
			localKeys := slices.Clone(keys1)
			rand.Shuffle(len(localKeys), func(i, j int) {
				localKeys[i], localKeys[j] = localKeys[j], localKeys[i]
			})
			out, err := client.GetMulti(
				ctx,
				time.Second*10,
				localKeys,
				cb,
			)
			assert.NoError(t, err)
			if diff := cmp.Diff(expected1, out); diff != "" {
				t.Errorf("GetMulti() mismatch in 1 (-want +got):\n%s", diff)
				t.Errorf("got: %v", out)
			}
		}()
		go func() {
			defer wg.Done()
			localKeys := slices.Clone(keys2)
			rand.Shuffle(len(localKeys), func(i, j int) {
				localKeys[i], localKeys[j] = localKeys[j], localKeys[i]
			})
			out, err := client.GetMulti(
				ctx,
				time.Second*10,
				localKeys,
				cb,
			)
			assert.NoError(t, err)
			if diff := cmp.Diff(expected2, out); diff != "" {
				t.Errorf("GetMulti() mismatch in 2 (-want +got):\n%s", diff)
				t.Errorf("got: %v", out)
			}
		}()
		go func() {
			defer wg.Done()
			localKeys := slices.Clone(keys3)
			rand.Shuffle(len(localKeys), func(i, j int) {
				localKeys[i], localKeys[j] = localKeys[j], localKeys[i]
			})
			out, err := client.GetMulti(
				context.Background(),
				time.Second*10,
				localKeys,
				cb)
			assert.NoError(t, err)
			if diff := cmp.Diff(expected3, out); diff != "" {
				t.Errorf("GetMulti() mismatch in 3 (-want +got):\n%s", diff)
				t.Errorf("got: %v", out)
			}
		}()
		go func() {
			defer wg.Done()
			localKeys := slices.Clone(keys4)
			rand.Shuffle(len(localKeys), func(i, j int) {
				localKeys[i], localKeys[j] = localKeys[j], localKeys[i]
			})
			out, err := client.GetMulti(
				context.Background(),
				time.Second*10,
				localKeys,
				cb)
			assert.NoError(t, err)
			if diff := cmp.Diff(expected4, out); diff != "" {
				t.Errorf("GetMulti() mismatch in 4 (-want +got):\n%s", diff)
				t.Errorf("got: %v", out)
			}
		}()
	}
	wg.Wait()
}

func TestCacheAside_DelMulti(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	keyAndVals := make(map[string]string)
	for i := range 3 {
		keyAndVals[fmt.Sprintf("key:%d:%s", i, uuid.New().String())] = fmt.Sprintf("val:%d:%s", i, uuid.New().String())
	}

	innerClient := client.Client()
	for key, val := range keyAndVals {
		err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(val).Nx().Get().Px(time.Millisecond*100).Build()).Error()
		require.True(t, rueidis.IsRedisNil(err))
	}

	for key := range keyAndVals {
		err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
		require.NoErrorf(t, err, "expected no error, got %v", err)
	}

	err := client.DelMulti(ctx, slices.Collect(maps.Keys(keyAndVals))...)
	require.NoError(t, err)

	for key := range keyAndVals {
		err = innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
		require.True(t, rueidis.IsRedisNil(err))
	}
}

func TestCacheAside_GetParentContextCancellation(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()

	ctx, cancel := context.WithCancel(context.Background())
	key := "key:" + uuid.New().String()
	val := "val:" + uuid.New().String()

	// Set a lock on the key so Get will wait
	innerClient := client.Client()
	lockVal := "__redcache:lock:" + uuid.New().String()
	err := innerClient.Do(context.Background(), innerClient.B().Set().Key(key).Value(lockVal).Nx().Get().Px(time.Second*30).Build()).Error()
	require.True(t, rueidis.IsRedisNil(err))

	// Cancel the parent context after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	cb := func(ctx context.Context, key string) (string, error) {
		return val, nil
	}

	// Should get parent context cancelled error, not a timeout
	_, err = client.Get(ctx, time.Second*10, key, cb)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

// TestConcurrentRegisterRace tests the register() method under high contention
// to ensure the CompareAndDelete race condition fix works correctly.
func TestConcurrentRegisterRace(t *testing.T) {
	// Use minimum allowed lock TTL to force lock expirations during concurrent access
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{
			InitAddress: addr,
		},
		redcache.CacheAsideOption{
			LockTTL: 100 * time.Millisecond,
		},
	)
	require.NoError(t, err)
	defer client.Client().Close()

	ctx := context.Background()
	key := "key:" + uuid.New().String()
	val := "val:" + uuid.New().String()

	callCount := 0
	var mu sync.Mutex
	cb := func(ctx context.Context, key string) (string, error) {
		mu.Lock()
		callCount++
		mu.Unlock()
		// Very short sleep to keep test fast while still triggering some lock expirations
		time.Sleep(5 * time.Millisecond)
		return val, nil
	}

	// Run concurrent goroutines to stress test the register race condition fix
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(4)
		go func() {
			defer wg.Done()
			res, getErr := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, getErr)
			assert.Equal(t, val, res)
		}()
		go func() {
			defer wg.Done()
			res, getErr := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, getErr)
			assert.Equal(t, val, res)
		}()
		go func() {
			defer wg.Done()
			res, getErr := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, getErr)
			assert.Equal(t, val, res)
		}()
		go func() {
			defer wg.Done()
			res, getErr := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, getErr)
			assert.Equal(t, val, res)
		}()
	}
	wg.Wait()

	// The callback should be called, but we might get multiple calls due to lock expiration
	mu.Lock()
	defer mu.Unlock()
	assert.Greater(t, callCount, 0, "callback should be called at least once")
}

// TestConcurrentGetSameKeySingleClient tests that multiple goroutines getting
// the same key from a single client instance only triggers one callback when locks don't expire.
func TestConcurrentGetSameKeySingleClient(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()

	ctx := context.Background()
	key := "key:" + uuid.New().String()
	val := "val:" + uuid.New().String()

	callCount := 0
	var mu sync.Mutex

	cb := func(ctx context.Context, key string) (string, error) {
		mu.Lock()
		callCount++
		mu.Unlock()
		return val, nil
	}

	// Run multiple iterations with concurrent goroutines, matching existing test pattern
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(4)
		go func() {
			defer wg.Done()
			res, err := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, err)
			assert.Equal(t, val, res)
		}()
		go func() {
			defer wg.Done()
			res, err := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, err)
			assert.Equal(t, val, res)
		}()
		go func() {
			defer wg.Done()
			res, err := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, err)
			assert.Equal(t, val, res)
		}()
		go func() {
			defer wg.Done()
			res, err := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, err)
			assert.Equal(t, val, res)
		}()
	}

	wg.Wait()

	// Callback should only be called once due to distributed locking
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, callCount, "callback should only be called once")
}

// TestConcurrentInvalidation tests that cache invalidation works correctly
// when multiple goroutines are accessing the same keys.
func TestConcurrentInvalidation(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()

	ctx := context.Background()
	key := "key:" + uuid.New().String()

	callCount := 0
	var mu sync.Mutex
	cb := func(ctx context.Context, key string) (string, error) {
		mu.Lock()
		callCount++
		mu.Unlock()
		return "value", nil
	}

	// Populate cache
	_, err := client.Get(ctx, time.Second*10, key, cb)
	require.NoError(t, err)

	mu.Lock()
	initialCount := callCount
	mu.Unlock()

	// Delete the key
	err = client.Del(ctx, key)
	require.NoError(t, err)

	// Run multiple iterations with concurrent reads after deletion, matching existing test pattern
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(4)
		go func() {
			defer wg.Done()
			_, getErr := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, getErr)
		}()
		go func() {
			defer wg.Done()
			_, getErr := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, getErr)
		}()
		go func() {
			defer wg.Done()
			_, getErr := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, getErr)
		}()
		go func() {
			defer wg.Done()
			_, getErr := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, getErr)
		}()
	}
	wg.Wait()

	// Callback should have been invoked at least once more due to invalidation
	mu.Lock()
	defer mu.Unlock()
	assert.Greater(t, callCount, initialCount, "callbacks should be invoked after invalidation")
}

// TestDeleteDuringGetWithLock tests that Delete called while Get holds a lock
// triggers graceful retry behavior via Redis invalidation messages.
func TestDeleteDuringGetWithLock(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()

	ctx := context.Background()
	key := "key:" + uuid.New().String()
	expectedValue := "val:" + uuid.New().String()

	callCount := 0
	var mu sync.Mutex
	var lockAcquiredOnce sync.Once
	getStarted := make(chan struct{})
	lockAcquired := make(chan struct{})

	cb := func(ctx context.Context, key string) (string, error) {
		mu.Lock()
		callCount++
		mu.Unlock()

		// Signal that we've started executing (only once)
		lockAcquiredOnce.Do(func() {
			close(lockAcquired)
		})

		// Simulate some work while holding the lock
		time.Sleep(50 * time.Millisecond)

		return expectedValue, nil
	}

	// Start Get operation in background
	var getResult string
	var getErr error
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(getStarted)
		getResult, getErr = client.Get(ctx, time.Second*10, key, cb)
	}()

	// Wait for Get to acquire lock
	<-getStarted
	<-lockAcquired

	// Now delete while Get is holding the lock
	err := client.Del(ctx, key)
	require.NoError(t, err)

	// Wait for Get to complete
	wg.Wait()

	// Get should have completed successfully with graceful retry
	require.NoError(t, getErr)
	require.Equal(t, expectedValue, getResult)

	// Callback should have been called twice:
	// 1. First call sets the lock and value
	// 2. Delete triggers invalidation, causing ErrLockLost
	// 3. Get retries and calls callback again
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 2, callCount, "callback should be called twice due to Delete invalidation")
}

// TestDeleteDuringGetMultiWithLocks tests that Delete called while GetMulti
// holds locks triggers graceful retry behavior via Redis invalidation messages.
func TestDeleteDuringGetMultiWithLocks(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()

	ctx := context.Background()
	keyAndVals := make(map[string]string)
	for i := range 3 {
		keyAndVals[fmt.Sprintf("key:%d:%s", i, uuid.New().String())] = fmt.Sprintf("val:%d:%s", i, uuid.New().String())
	}
	keys := make([]string, 0, len(keyAndVals))
	for k := range keyAndVals {
		keys = append(keys, k)
	}

	callCount := 0
	var mu sync.Mutex
	var lockAcquiredOnce sync.Once
	getStarted := make(chan struct{})
	lockAcquired := make(chan struct{})

	cb := func(ctx context.Context, keys []string) (map[string]string, error) {
		mu.Lock()
		callCount++
		mu.Unlock()

		// Signal that we've started executing (only once)
		lockAcquiredOnce.Do(func() {
			close(lockAcquired)
		})

		// Simulate some work while holding locks
		time.Sleep(50 * time.Millisecond)

		res := make(map[string]string, len(keys))
		for _, key := range keys {
			res[key] = keyAndVals[key]
		}
		return res, nil
	}

	// Start GetMulti operation in background
	var getResult map[string]string
	var getErr error
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(getStarted)
		getResult, getErr = client.GetMulti(ctx, time.Second*10, keys, cb)
	}()

	// Wait for GetMulti to acquire locks
	<-getStarted
	<-lockAcquired

	// Now delete one of the keys while GetMulti is holding locks
	err := client.Del(ctx, keys[0])
	require.NoError(t, err)

	// Wait for GetMulti to complete
	wg.Wait()

	// GetMulti should have completed successfully with graceful retry
	require.NoError(t, getErr)
	require.Equal(t, keyAndVals, getResult)

	// Callback should have been called twice:
	// 1. First call sets locks and values
	// 2. Delete triggers invalidation on one key, causing retry
	// 3. GetMulti retries and calls callback again
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 2, callCount, "callback should be called twice due to Delete invalidation")
}
