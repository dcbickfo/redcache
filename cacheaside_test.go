package redcache_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
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
	"github.com/dcbickfo/redcache/internal/mapsx"
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

	err := client.DelMulti(ctx, mapsx.Keys(keyAndVals)...)
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
// to ensure the CompareAndDelete race condition fix works correctly
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

	// The callback should be called, but we might get multiple calls due to lock expiration
	mu.Lock()
	defer mu.Unlock()
	assert.Greater(t, callCount, 0, "callback should be called at least once")
}

// TestConcurrentGetSameKeySingleClient tests that multiple goroutines getting
// the same key from a single client instance only triggers one callback when locks don't expire
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
// when multiple goroutines are accessing the same keys
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
			_, err := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, err)
		}()
		go func() {
			defer wg.Done()
			_, err := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, err)
		}()
		go func() {
			defer wg.Done()
			_, err := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, err)
		}()
		go func() {
			defer wg.Done()
			_, err := client.Get(ctx, time.Second*10, key, cb)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	// Callback should have been invoked at least once more due to invalidation
	mu.Lock()
	defer mu.Unlock()
	assert.Greater(t, callCount, initialCount, "callbacks should be invoked after invalidation")
}

func TestCacheAside_Get_CallbackError(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	cbErr := fmt.Errorf("callback failed")

	// First Get: callback returns error.
	_, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return "", cbErr
	})
	require.ErrorIs(t, err, cbErr)

	// Second Get: lock should have been cleaned up, so a fresh callback succeeds.
	val := "good-val:" + uuid.New().String()
	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return val, nil
	})
	require.NoError(t, err)
	assert.Equal(t, val, res)
}

func TestCacheAside_GetMulti_CallbackError(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	keys := []string{
		"key:0:" + uuid.New().String(),
		"key:1:" + uuid.New().String(),
	}
	cbErr := fmt.Errorf("multi callback failed")

	// Callback returns error — locks should be cleaned up.
	_, err := client.GetMulti(ctx, time.Second*10, keys, func(ctx context.Context, ks []string) (map[string]string, error) {
		return nil, cbErr
	})
	require.ErrorIs(t, err, cbErr)

	// Retry should succeed — locks were released.
	vals := map[string]string{
		keys[0]: "val:0:" + uuid.New().String(),
		keys[1]: "val:1:" + uuid.New().String(),
	}
	res, err := client.GetMulti(ctx, time.Second*10, keys, func(ctx context.Context, ks []string) (map[string]string, error) {
		out := make(map[string]string, len(ks))
		for _, k := range ks {
			out[k] = vals[k]
		}
		return out, nil
	})
	require.NoError(t, err)
	if diff := cmp.Diff(vals, res); diff != "" {
		t.Errorf("GetMulti() mismatch (-want +got):\n%s", diff)
	}
}

func TestCacheAside_Close(t *testing.T) {
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()

	// Place a long-lived lock.
	innerClient := client.Client()
	lockVal := "__redcache:lock:" + uuid.New().String()
	err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(lockVal).Nx().Get().Px(time.Second*30).Build()).Error()
	require.True(t, rueidis.IsRedisNil(err))

	getCtx, getCancel := context.WithTimeout(ctx, 2*time.Second)
	defer getCancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := client.Get(getCtx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
			return "val", nil
		})
		errCh <- err
	}()

	time.Sleep(100 * time.Millisecond)
	client.Close()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Get did not return after Close")
	case err := <-errCh:
		// Close wakes up the waiter; since the lock persists it will eventually timeout.
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
}

func makeRefreshClient(t *testing.T, addr []string, fraction float64) *redcache.CacheAside {
	t.Helper()
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{
			InitAddress: addr,
		},
		redcache.CacheAsideOption{
			LockTTL:              time.Second * 2,
			RefreshAfterFraction: fraction,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.Close()
		client.Client().Close()
	})
	return client
}

func TestRefreshAhead_TriggersBackgroundRefresh(t *testing.T) {
	// fraction=0.5 means refresh when >50% of TTL elapsed (i.e. <50% remaining).
	client := makeRefreshClient(t, addr, 0.5)
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	callCount := 0
	var mu sync.Mutex

	cb := func(_ context.Context, _ string) (string, error) {
		mu.Lock()
		callCount++
		c := callCount
		mu.Unlock()
		return fmt.Sprintf("val-%d", c), nil
	}

	ttl := 2 * time.Second

	// First call: populates cache — fn called once.
	res, err := client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)
	assert.Equal(t, "val-1", res)

	// Wait until >50% of TTL has elapsed so remaining < threshold.
	time.Sleep(1200 * time.Millisecond)

	// This Get should return the stale value and trigger a background refresh.
	res, err = client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)
	assert.Equal(t, "val-1", res) // stale value returned immediately

	// Give the background goroutine time to complete.
	time.Sleep(500 * time.Millisecond)

	// Next Get should see the refreshed value.
	res, err = client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)
	assert.Equal(t, "val-2", res)

	mu.Lock()
	assert.Equal(t, 2, callCount, "fn should have been called exactly twice")
	mu.Unlock()
}

func TestRefreshAhead_Dedup(t *testing.T) {
	client := makeRefreshClient(t, addr, 0.5)
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	var refreshCount int64
	var mu sync.Mutex
	firstCall := true

	cb := func(_ context.Context, _ string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if firstCall {
			firstCall = false
			return "initial", nil
		}
		refreshCount++
		time.Sleep(200 * time.Millisecond) // slow enough to overlap concurrent Gets
		return "refreshed", nil
	}

	ttl := 2 * time.Second

	// Populate cache.
	_, err := client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)

	// Wait until threshold is crossed.
	time.Sleep(1200 * time.Millisecond)

	// Fire many concurrent Gets — all should return stale and trigger at most one refresh.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := client.Get(ctx, ttl, key, cb)
			assert.NoError(t, err)
			assert.Equal(t, "initial", res) // stale returned
		}()
	}
	wg.Wait()

	// Wait for refresh goroutine to finish.
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, int64(1), refreshCount, "refresh callback should be called exactly once")
	mu.Unlock()
}

func TestRefreshAhead_Disabled(t *testing.T) {
	// Default (fraction=0) — no refresh-ahead.
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	callCount := 0
	var mu sync.Mutex

	cb := func(_ context.Context, _ string) (string, error) {
		mu.Lock()
		callCount++
		mu.Unlock()
		return "val", nil
	}

	_, err := client.Get(ctx, 2*time.Second, key, cb)
	require.NoError(t, err)

	// Wait until TTL is nearly expired.
	time.Sleep(1500 * time.Millisecond)

	_, err = client.Get(ctx, 2*time.Second, key, cb)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, 1, callCount, "fn should only be called once with refresh-ahead disabled")
	mu.Unlock()
}

func TestRefreshAhead_ErrorLogged(t *testing.T) {
	client := makeRefreshClient(t, addr, 0.5)
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	firstCall := true
	var mu sync.Mutex

	cb := func(_ context.Context, _ string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if firstCall {
			firstCall = false
			return "initial", nil
		}
		return "", fmt.Errorf("refresh failed")
	}

	ttl := 2 * time.Second

	// Populate cache.
	res, err := client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)
	assert.Equal(t, "initial", res)

	// Wait until threshold is crossed.
	time.Sleep(1200 * time.Millisecond)

	// Get triggers background refresh which will fail — stale value returned.
	res, err = client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)
	assert.Equal(t, "initial", res)

	// Wait for refresh goroutine to complete (error is logged, not returned).
	time.Sleep(500 * time.Millisecond)

	// Stale value should still be present — no panic.
	res, err = client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)
	assert.Equal(t, "initial", res)
}

func TestRefreshAhead_PanicRecovered(t *testing.T) {
	client := makeRefreshClient(t, addr, 0.5)
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	var calls atomic.Int32

	cb := func(_ context.Context, _ string) (string, error) {
		n := calls.Add(1)
		if n == 1 {
			return "initial", nil
		}
		if n == 2 {
			panic("simulated callback panic")
		}
		return "refreshed", nil
	}

	ttl := 2 * time.Second

	res, err := client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)
	assert.Equal(t, "initial", res)

	// First refresh: callback panics. Worker must recover and stay alive.
	time.Sleep(1200 * time.Millisecond)
	res, err = client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)
	assert.Equal(t, "initial", res)

	// Wait for the panicking refresh to land.
	time.Sleep(400 * time.Millisecond)
	assert.GreaterOrEqual(t, calls.Load(), int32(2), "refresh callback should have been invoked")

	// Wait past dedup TTL so a new refresh can be triggered.
	time.Sleep(2500 * time.Millisecond)

	// Re-populate so we can trigger another refresh.
	key2 := "key:" + uuid.New().String()
	res, err = client.Get(ctx, ttl, key2, cb)
	require.NoError(t, err)
	assert.Equal(t, "refreshed", res)

	time.Sleep(1200 * time.Millisecond)
	res, err = client.Get(ctx, ttl, key2, cb)
	require.NoError(t, err)
	assert.Equal(t, "refreshed", res)

	// If the worker had died, the refresh wouldn't process and calls.Load() would stall.
	time.Sleep(400 * time.Millisecond)
	assert.GreaterOrEqual(t, calls.Load(), int32(4), "worker should still be alive to process refreshes")
}

func TestRefreshAhead_DoesNotStompLockValue(t *testing.T) {
	client := makeRefreshClient(t, addr, 0.5)
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	callbackStarted := make(chan struct{})
	callbackProceed := make(chan struct{})
	var calls atomic.Int32

	cb := func(_ context.Context, _ string) (string, error) {
		n := calls.Add(1)
		if n == 1 {
			return "initial", nil
		}
		close(callbackStarted)
		<-callbackProceed
		return "refreshed", nil
	}

	ttl := 2 * time.Second

	_, err := client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)

	time.Sleep(1200 * time.Millisecond)

	_, err = client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)

	select {
	case <-callbackStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh callback did not start")
	}

	// Simulate a concurrent Get-on-miss winning the distributed lock by
	// swapping the stale real value for a lock-prefixed value while the
	// refresh callback is mid-flight.
	lockVal := "__redcache:lock:race-winner"
	require.NoError(t, client.Client().Do(ctx,
		client.Client().B().Set().Key(key).Value(lockVal).Px(ttl).Build()).Error())

	close(callbackProceed)

	time.Sleep(400 * time.Millisecond)

	got, gErr := client.Client().Do(ctx, client.Client().B().Get().Key(key).Build()).ToString()
	require.NoError(t, gErr)
	assert.Equal(t, lockVal, got, "refresh-ahead must skip the SET when a lock value is present")
}

func TestRefreshAhead_GetMulti(t *testing.T) {
	client := makeRefreshClient(t, addr, 0.5)
	ctx := context.Background()

	keys := []string{
		"key:0:" + uuid.New().String(),
		"key:1:" + uuid.New().String(),
	}
	callCount := 0
	var mu sync.Mutex

	cb := func(_ context.Context, ks []string) (map[string]string, error) {
		mu.Lock()
		callCount++
		c := callCount
		mu.Unlock()
		res := make(map[string]string, len(ks))
		for _, k := range ks {
			res[k] = fmt.Sprintf("val-%d", c)
		}
		return res, nil
	}

	ttl := 2 * time.Second

	// Populate cache.
	res, err := client.GetMulti(ctx, ttl, keys, cb)
	require.NoError(t, err)
	for _, k := range keys {
		assert.Equal(t, "val-1", res[k])
	}

	// Wait until threshold is crossed.
	time.Sleep(1200 * time.Millisecond)

	// GetMulti returns stale values and triggers background refresh.
	res, err = client.GetMulti(ctx, ttl, keys, cb)
	require.NoError(t, err)
	for _, k := range keys {
		assert.Equal(t, "val-1", res[k]) // stale
	}

	// Wait for refresh.
	time.Sleep(500 * time.Millisecond)

	// Next GetMulti should see refreshed values.
	res, err = client.GetMulti(ctx, ttl, keys, cb)
	require.NoError(t, err)
	for _, k := range keys {
		assert.Equal(t, "val-2", res[k])
	}

	mu.Lock()
	assert.Equal(t, 2, callCount, "fn should have been called exactly twice")
	mu.Unlock()
}

func TestRefreshAhead_Backpressure(t *testing.T) {
	// Tiny pool: 1 worker, queue size 1.
	// The worker sleeps during refresh, so the queue fills fast.
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{
			LockTTL:              time.Second * 3,
			RefreshAfterFraction: 0.5,
			RefreshWorkers:       1,
			RefreshQueueSize:     1,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Close()
		client.Client().Close()
	})
	ctx := context.Background()

	// Create many distinct keys so each triggers a separate refresh.
	const numKeys = 20
	keys := make([]string, numKeys)
	for i := range numKeys {
		keys[i] = fmt.Sprintf("key:%d:%s", i, uuid.New().String())
	}

	ttl := 3 * time.Second
	var refreshCount atomic.Int64

	populateCb := func(_ context.Context, _ string) (string, error) {
		return "initial", nil
	}

	// Populate all keys.
	for _, key := range keys {
		_, err := client.Get(ctx, ttl, key, populateCb)
		require.NoError(t, err)
	}

	// Wait until >50% of TTL has elapsed so refresh triggers.
	time.Sleep(1700 * time.Millisecond)

	refreshCb := func(_ context.Context, _ string) (string, error) {
		refreshCount.Add(1)
		time.Sleep(500 * time.Millisecond) // slow — keeps the single worker busy
		return "refreshed", nil
	}

	// Fire concurrent Gets on all 20 keys. With 1 worker and queue size 1,
	// at most ~2 refresh jobs can be accepted (1 executing + 1 queued).
	// The rest are silently dropped.
	var wg sync.WaitGroup
	for _, key := range keys {
		k := key
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := client.Get(ctx, ttl, k, refreshCb)
			assert.NoError(t, err)
			assert.Equal(t, "initial", res) // stale value always returned
		}()
	}
	wg.Wait()

	// Wait for all enqueued refreshes to finish.
	time.Sleep(1500 * time.Millisecond)

	// With 1 worker processing a 500ms job and queue size 1, far fewer than
	// 20 refreshes should have executed.
	count := refreshCount.Load()
	assert.Less(t, count, int64(numKeys),
		"expected fewer than %d refreshes, got %d — backpressure should drop excess jobs", numKeys, count)
	assert.Greater(t, count, int64(0), "at least one refresh should have executed")
}

func TestRefreshAhead_FractionValidation(t *testing.T) {
	t.Run("negative fraction", func(t *testing.T) {
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{RefreshAfterFraction: -0.1},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "RefreshAfterFraction")
	})
	t.Run("fraction equals 1", func(t *testing.T) {
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{RefreshAfterFraction: 1.0},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "RefreshAfterFraction")
	})
	t.Run("fraction greater than 1", func(t *testing.T) {
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{RefreshAfterFraction: 1.5},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "RefreshAfterFraction")
	})
	t.Run("valid fraction", func(t *testing.T) {
		client, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{RefreshAfterFraction: 0.8},
		)
		require.NoError(t, err)
		client.Close()
		client.Client().Close()
	})
	t.Run("negative RefreshWorkers", func(t *testing.T) {
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{RefreshAfterFraction: 0.8, RefreshWorkers: -1},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "RefreshWorkers")
	})
	t.Run("negative RefreshQueueSize", func(t *testing.T) {
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{RefreshAfterFraction: 0.8, RefreshQueueSize: -1},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "RefreshQueueSize")
	})
	t.Run("custom workers and queue", func(t *testing.T) {
		client, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{RefreshAfterFraction: 0.8, RefreshWorkers: 2, RefreshQueueSize: 16},
		)
		require.NoError(t, err)
		client.Close()
		client.Client().Close()
	})
}

func TestNewRedCacheAside_Validation(t *testing.T) {
	t.Run("empty InitAddress", func(t *testing.T) {
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{},
			redcache.CacheAsideOption{},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "InitAddress")
	})

	t.Run("negative LockTTL", func(t *testing.T) {
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: -1 * time.Second},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "negative")
	})

	t.Run("too small LockTTL", func(t *testing.T) {
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 10 * time.Millisecond},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "100ms")
	})
}

// TestCacheAside_Get_ErrLockLostRetry verifies that when a ForceSet steals the lock
// during a Get callback, Get retries and eventually sees the ForceSet value.
func TestCacheAside_Get_ErrLockLostRetry(t *testing.T) {
	client, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{LockTTL: time.Second * 2},
	)
	require.NoError(t, err)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	forcedVal := "forced:" + uuid.New().String()

	getStarted := make(chan struct{})

	go func() {
		// Get acquires lock, then we steal it with ForceSet during callback.
		_, _ = client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
			close(getStarted)
			time.Sleep(300 * time.Millisecond)
			return "get-val", nil
		})
	}()

	<-getStarted
	time.Sleep(50 * time.Millisecond)

	// ForceSet steals the lock — Get's setWithLock will see CAS mismatch (ErrLockLost).
	err = client.ForceSet(ctx, time.Second*10, key, forcedVal)
	require.NoError(t, err)

	// Wait for Get to complete its retry.
	time.Sleep(500 * time.Millisecond)

	// Key should have a value (either the forced value or a re-populated value).
	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called — value should exist")
		return "", nil
	})
	require.NoError(t, err)
	assert.NotEmpty(t, res)
}
