package redcache_test

import (
	"context"
	"errors"
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	val := "val:" + uuid.New().String()

	innerClient := client.Client()
	err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(val).Nx().Get().Px(time.Second*30).Build()).Error()
	require.True(t, rueidis.IsRedisNil(err))

	err = innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
	require.NoErrorf(t, err, "expected no error, got %v", err)

	err = client.Del(ctx, key)
	require.NoError(t, err)

	err = innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
	require.True(t, rueidis.IsRedisNil(err))
}

func TestCBWrapper_GetMultiCheckConcurrent(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	keyAndVals := make(map[string]string)
	for i := range 3 {
		keyAndVals[fmt.Sprintf("key:%d:%s", i, uuid.New().String())] = fmt.Sprintf("val:%d:%s", i, uuid.New().String())
	}

	innerClient := client.Client()
	for key, val := range keyAndVals {
		err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(val).Nx().Get().Px(time.Second*30).Build()).Error()
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

func TestCacheAside_Touch_ExtendsTTL(t *testing.T) {
	t.Parallel()
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "touch:" + uuid.New().String()

	_, err := client.Get(ctx, 500*time.Millisecond, key, func(_ context.Context, _ string) (string, error) {
		return "v", nil
	})
	require.NoError(t, err)

	require.NoError(t, client.Touch(ctx, 5*time.Second, key))

	pttl, err := client.Client().Do(ctx, client.Client().B().Pttl().Key(key).Build()).AsInt64()
	require.NoError(t, err)
	assert.Greater(t, pttl, int64(2000), "TTL should have been extended well past the original 500ms")
}

func TestCacheAside_Touch_NoOpOnMissing(t *testing.T) {
	t.Parallel()
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "touch-missing:" + uuid.New().String()
	require.NoError(t, client.Touch(ctx, 5*time.Second, key), "Touch on missing key must succeed silently")

	err := client.Client().Do(ctx, client.Client().B().Get().Key(key).Build()).Error()
	require.True(t, rueidis.IsRedisNil(err), "Touch must not create the key")
}

func TestCacheAside_Touch_NoOpOnLockValue(t *testing.T) {
	t.Parallel()
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "touch-lock:" + uuid.New().String()
	lockVal := "__redcache:lock:abc"
	require.NoError(t, client.Client().Do(ctx,
		client.Client().B().Set().Key(key).Value(lockVal).Px(time.Second).Build()).Error())

	require.NoError(t, client.Touch(ctx, 10*time.Second, key))

	pttl, err := client.Client().Do(ctx, client.Client().B().Pttl().Key(key).Build()).AsInt64()
	require.NoError(t, err)
	// Bound by the original 1000ms PX so partial regressions (e.g. extension to 2s) are also caught.
	assert.LessOrEqual(t, pttl, int64(1000), "lock TTL must NOT have been extended by Touch")

	got, err := client.Client().Do(ctx, client.Client().B().Get().Key(key).Build()).ToString()
	require.NoError(t, err)
	assert.Equal(t, lockVal, got, "lock value must be preserved")
}

func TestCacheAside_TouchMulti_ExtendsTTLs(t *testing.T) {
	t.Parallel()
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	keys := []string{
		"touchm:0:" + uuid.New().String(),
		"touchm:1:" + uuid.New().String(),
		"touchm:2:" + uuid.New().String(),
	}
	for _, k := range keys {
		_, err := client.Get(ctx, 500*time.Millisecond, k, func(_ context.Context, _ string) (string, error) {
			return "v", nil
		})
		require.NoError(t, err)
	}

	require.NoError(t, client.TouchMulti(ctx, 5*time.Second, keys...))

	for _, k := range keys {
		pttl, err := client.Client().Do(ctx, client.Client().B().Pttl().Key(k).Build()).AsInt64()
		require.NoError(t, err)
		assert.Greater(t, pttl, int64(2000), "key %q TTL should have been extended", k)
	}
}

func TestCacheAside_TouchMulti_EmptyKeysIsNoOp(t *testing.T) {
	t.Parallel()
	client := makeClient(t, addr)
	defer client.Client().Close()
	require.NoError(t, client.TouchMulti(context.Background(), 5*time.Second))
}

func TestCacheAside_GetParentContextCancellation(t *testing.T) {
	t.Parallel()
	client := makeClient(t, addr)
	defer client.Client().Close()

	ctx, cancel := context.WithCancel(context.Background())
	key := "key:" + uuid.New().String()
	val := "val:" + uuid.New().String()

	// Lock the key so Get will wait.
	innerClient := client.Client()
	lockVal := "__redcache:lock:" + uuid.New().String()
	err := innerClient.Do(context.Background(), innerClient.B().Set().Key(key).Value(lockVal).Nx().Get().Px(time.Second*30).Build()).Error()
	require.True(t, rueidis.IsRedisNil(err))

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	cb := func(ctx context.Context, key string) (string, error) {
		return val, nil
	}

	_, err = client.Get(ctx, time.Second*10, key, cb)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

// TestConcurrentRegisterRace stresses register() to guard the CompareAndDelete race.
func TestConcurrentRegisterRace(t *testing.T) {
	t.Parallel()
	// Minimum lock TTL forces expirations under contention.
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
		// Brief sleep triggers lock expirations without slowing the test.
		time.Sleep(5 * time.Millisecond)
		return val, nil
	}

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

	// Cap above: a register-race regression would let every Get re-fire the callback unboundedly.
	mu.Lock()
	defer mu.Unlock()
	assert.Greater(t, callCount, 0, "callback should be called at least once")
	assert.Less(t, callCount, 4000, "callback fired far more than expected — possible register-race regression")
}

// TestConcurrentGetSameKeySingleClient verifies a single client dedupes concurrent
// same-key Gets down to one callback.
func TestConcurrentGetSameKeySingleClient(t *testing.T) {
	t.Parallel()
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

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, callCount, "callback should only be called once")
}

// TestCacheAside_Get_LeaderNXFailure_WaitsForInvalidation verifies the
// leader/follower discipline when an in-process leader's SET NX is rejected.
// The leader must wait for the actual holder's invalidation rather than
// cancelling its own lockEntry and waking followers into a busy-loop NX race.
func TestCacheAside_Get_LeaderNXFailure_WaitsForInvalidation(t *testing.T) {
	t.Parallel()
	client := makeClient(t, addr)
	defer client.Client().Close()

	ctx := context.Background()
	key := "key:" + uuid.New().String()
	val := "val:" + uuid.New().String()

	// Plant a synthetic Redis lock from "another process" so SET NX is rejected.
	inner := client.Client()
	syntheticLock := "__redcache:lock:other-process-" + uuid.New().String()
	require.NoError(t, inner.Do(ctx, inner.B().Set().Key(key).Value(syntheticLock).Px(time.Second*5).Build()).Error())

	// fn must NEVER fire: the synthetic lock will be replaced via invalidation.
	cb := func(ctx context.Context, key string) (string, error) {
		t.Errorf("fn unexpectedly invoked for key %q", key)
		return "", errors.New("fn should not run")
	}

	// At most one Get becomes leader, attempts SET NX, fails, and waits; the
	// rest are followers and wait directly.
	const n = 50
	results := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			results[i], errs[i] = client.Get(ctx, time.Second*10, key, cb)
		}()
	}

	// Give all Gets time to enter the await path. The test fails closed if
	// they resolve early — fn would fire and t.Errorf above catches it.
	time.Sleep(100 * time.Millisecond)

	// Replace the synthetic lock with a real envelope-wrapped value; the
	// resulting invalidation wakes the leader and all followers.
	wrapped := "__redcache:v1:0:" + val
	require.NoError(t, inner.Do(ctx, inner.B().Set().Key(key).Value(wrapped).Px(time.Second*10).Build()).Error())

	wg.Wait()
	for i := range n {
		require.NoError(t, errs[i], "goroutine %d failed", i)
		require.Equal(t, val, results[i], "goroutine %d got wrong value", i)
	}
}

// TestConcurrentInvalidation verifies invalidation under concurrent reads.
func TestConcurrentInvalidation(t *testing.T) {
	t.Parallel()
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

	_, err := client.Get(ctx, time.Second*10, key, cb)
	require.NoError(t, err)

	mu.Lock()
	initialCount := callCount
	mu.Unlock()

	err = client.Del(ctx, key)
	require.NoError(t, err)

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

	mu.Lock()
	defer mu.Unlock()
	assert.Greater(t, callCount, initialCount, "callbacks should be invoked after invalidation")
}

func TestCacheAside_Get_CallbackError(t *testing.T) {
	t.Parallel()
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	cbErr := fmt.Errorf("callback failed")

	_, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return "", cbErr
	})
	require.ErrorIs(t, err, cbErr)

	// Lock should have been cleaned up, so a retry's callback can run.
	val := "good-val:" + uuid.New().String()
	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		return val, nil
	})
	require.NoError(t, err)
	assert.Equal(t, val, res)
}

func TestCacheAside_GetMulti_CallbackError(t *testing.T) {
	t.Parallel()
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	keys := []string{
		"key:0:" + uuid.New().String(),
		"key:1:" + uuid.New().String(),
	}
	cbErr := fmt.Errorf("multi callback failed")

	_, err := client.GetMulti(ctx, time.Second*10, keys, func(ctx context.Context, ks []string) (map[string]string, error) {
		return nil, cbErr
	})
	require.ErrorIs(t, err, cbErr)

	// Retry should succeed once locks are released.
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
	t.Parallel()
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "key:" + uuid.New().String()

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
		// Close wakes the waiter; the persistent lock then forces a deadline timeout.
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
			// Disable XFetch sampling so tests assert exact refresh-vs-no-refresh
			// outcomes; sampling is exercised by TestShouldRefresh_XFetch.
			RefreshBeta: 0,
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
	t.Parallel()
	// fraction=0.5 -> refresh once <50% TTL remaining.
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

	res, err := client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)
	assert.Equal(t, "val-1", res)

	// Cross the refresh threshold.
	time.Sleep(1200 * time.Millisecond)

	res, err = client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)
	assert.Equal(t, "val-1", res)

	require.Eventually(t, func() bool {
		v, e := client.Get(ctx, ttl, key, cb)
		return e == nil && v == "val-2"
	}, 2*time.Second, 10*time.Millisecond, "expected refreshed value after background refresh")

	mu.Lock()
	assert.Equal(t, 2, callCount, "fn should have been called exactly twice")
	mu.Unlock()
}

func TestRefreshAhead_Dedup(t *testing.T) {
	t.Parallel()
	client := makeRefreshClient(t, addr, 0.5)
	ctx := context.Background()

	key := "key:" + uuid.New().String()
	var refreshCount int64
	var mu sync.Mutex
	firstCall := true

	// Block the refresh callback so every concurrent Get is forced through
	// the dedup decision before any refresh can complete.
	refreshUnblock := make(chan struct{})

	cb := func(_ context.Context, _ string) (string, error) {
		mu.Lock()
		if firstCall {
			firstCall = false
			mu.Unlock()
			return "initial", nil
		}
		refreshCount++
		mu.Unlock()
		<-refreshUnblock
		return "refreshed", nil
	}

	ttl := 2 * time.Second

	_, err := client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)

	time.Sleep(1200 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := client.Get(ctx, ttl, key, cb)
			assert.NoError(t, err)
			assert.Equal(t, "initial", res)
		}()
	}
	wg.Wait()

	// All 20 Gets have made their dedup decision; release the worker.
	close(refreshUnblock)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return refreshCount >= 1
	}, 2*time.Second, 10*time.Millisecond, "refresh callback should fire")

	mu.Lock()
	assert.Equal(t, int64(1), refreshCount, "refresh callback should be called exactly once")
	mu.Unlock()
}

func TestRefreshAhead_Disabled(t *testing.T) {
	t.Parallel()
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

	time.Sleep(1500 * time.Millisecond)

	_, err = client.Get(ctx, 2*time.Second, key, cb)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, 1, callCount, "fn should only be called once with refresh-ahead disabled")
	mu.Unlock()
}

func TestRefreshAhead_ErrorLogged(t *testing.T) {
	t.Parallel()
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

	res, err := client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)
	assert.Equal(t, "initial", res)

	time.Sleep(1200 * time.Millisecond)

	// Background refresh fails; stale value is still returned.
	res, err = client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)
	assert.Equal(t, "initial", res)

	// Refresh goroutine logs the error rather than returning it.
	time.Sleep(500 * time.Millisecond)

	res, err = client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)
	assert.Equal(t, "initial", res)
}

func TestRefreshAhead_PanicRecovered(t *testing.T) {
	t.Parallel()
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

	// First refresh panics; worker must recover and stay alive.
	time.Sleep(1200 * time.Millisecond)
	res, err = client.Get(ctx, ttl, key, cb)
	require.NoError(t, err)
	assert.Equal(t, "initial", res)

	require.Eventually(t, func() bool {
		return calls.Load() >= 2
	}, 2*time.Second, 10*time.Millisecond, "refresh callback should have been invoked")

	// Wait past dedup TTL so the next refresh is not deduped.
	time.Sleep(2500 * time.Millisecond)

	key2 := "key:" + uuid.New().String()
	res, err = client.Get(ctx, ttl, key2, cb)
	require.NoError(t, err)
	assert.Equal(t, "refreshed", res)

	time.Sleep(1200 * time.Millisecond)
	res, err = client.Get(ctx, ttl, key2, cb)
	require.NoError(t, err)
	assert.Equal(t, "refreshed", res)

	// A dead worker would stall calls.Load() here.
	require.Eventually(t, func() bool {
		return calls.Load() >= 4
	}, 2*time.Second, 10*time.Millisecond, "worker should still be alive to process refreshes")
}

func TestRefreshAhead_DoesNotStompLockValue(t *testing.T) {
	t.Parallel()
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

	// Swap the stale value for a lock-prefixed value mid-refresh, simulating
	// a concurrent Get-on-miss winning the distributed lock.
	lockVal := "__redcache:lock:race-winner"
	require.NoError(t, client.Client().Do(ctx,
		client.Client().B().Set().Key(key).Value(lockVal).Px(ttl).Build()).Error())

	close(callbackProceed)

	// Drain the refresh worker so its post-callback Lua-CAS has either run or
	// definitively skipped before we assert. Avoids a wall-clock race on slow CI.
	client.Close()

	got, gErr := client.Client().Do(ctx, client.Client().B().Get().Key(key).Build()).ToString()
	require.NoError(t, gErr)
	assert.Equal(t, lockVal, got, "refresh-ahead must skip the SET when a lock value is present")
}

func TestRefreshAhead_GetMulti(t *testing.T) {
	t.Parallel()
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

	res, err := client.GetMulti(ctx, ttl, keys, cb)
	require.NoError(t, err)
	for _, k := range keys {
		assert.Equal(t, "val-1", res[k])
	}

	time.Sleep(1200 * time.Millisecond)

	res, err = client.GetMulti(ctx, ttl, keys, cb)
	require.NoError(t, err)
	for _, k := range keys {
		assert.Equal(t, "val-1", res[k])
	}

	require.Eventually(t, func() bool {
		v, e := client.GetMulti(ctx, ttl, keys, cb)
		if e != nil {
			return false
		}
		for _, k := range keys {
			if v[k] != "val-2" {
				return false
			}
		}
		return true
	}, 2*time.Second, 10*time.Millisecond, "expected refreshed values after background refresh")

	mu.Lock()
	assert.Equal(t, 2, callCount, "fn should have been called exactly twice")
	mu.Unlock()
}

func TestRefreshAhead_Backpressure(t *testing.T) {
	t.Parallel()
	// Tiny pool (1 worker, queue size 1) plus a sleeping callback so the queue fills fast.
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{
			LockTTL:              time.Second * 3,
			RefreshAfterFraction: 0.5,
			RefreshBeta:          0,
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

	// Distinct keys so each triggers its own refresh.
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

	time.Sleep(1700 * time.Millisecond)

	refreshCb := func(_ context.Context, _ string) (string, error) {
		refreshCount.Add(1)
		time.Sleep(500 * time.Millisecond) // keeps the single worker busy
		return "refreshed", nil
	}

	// 1 worker + queue size 1 accepts ~2 jobs; the rest are dropped.
	var wg sync.WaitGroup
	for _, key := range keys {
		k := key
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := client.Get(ctx, ttl, k, refreshCb)
			assert.NoError(t, err)
			assert.Equal(t, "initial", res)
		}()
	}
	wg.Wait()

	time.Sleep(1500 * time.Millisecond)

	count := refreshCount.Load()
	assert.Less(t, count, int64(numKeys),
		"expected fewer than %d refreshes, got %d — backpressure should drop excess jobs", numKeys, count)
	assert.Greater(t, count, int64(0), "at least one refresh should have executed")
}

func TestRefreshAhead_FractionValidation(t *testing.T) {
	t.Parallel()
	t.Run("negative fraction", func(t *testing.T) {
		t.Parallel()
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{RefreshAfterFraction: -0.1},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "RefreshAfterFraction")
	})
	t.Run("fraction equals 1", func(t *testing.T) {
		t.Parallel()
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{RefreshAfterFraction: 1.0},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "RefreshAfterFraction")
	})
	t.Run("fraction greater than 1", func(t *testing.T) {
		t.Parallel()
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{RefreshAfterFraction: 1.5},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "RefreshAfterFraction")
	})
	t.Run("valid fraction", func(t *testing.T) {
		t.Parallel()
		client, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{RefreshAfterFraction: 0.8},
		)
		require.NoError(t, err)
		client.Close()
		client.Client().Close()
	})
	t.Run("negative RefreshWorkers", func(t *testing.T) {
		t.Parallel()
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{RefreshAfterFraction: 0.8, RefreshWorkers: -1},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "RefreshWorkers")
	})
	t.Run("negative RefreshQueueSize", func(t *testing.T) {
		t.Parallel()
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{RefreshAfterFraction: 0.8, RefreshQueueSize: -1},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "RefreshQueueSize")
	})
	t.Run("custom workers and queue", func(t *testing.T) {
		t.Parallel()
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
	t.Parallel()
	t.Run("empty InitAddress", func(t *testing.T) {
		t.Parallel()
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{},
			redcache.CacheAsideOption{},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "InitAddress")
	})

	t.Run("negative LockTTL", func(t *testing.T) {
		t.Parallel()
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: -1 * time.Second},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "negative")
	})

	t.Run("too small LockTTL", func(t *testing.T) {
		t.Parallel()
		_, err := redcache.NewRedCacheAside(
			rueidis.ClientOption{InitAddress: addr},
			redcache.CacheAsideOption{LockTTL: 10 * time.Millisecond},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "100ms")
	})
}

// TestCacheAside_Get_ErrLockLostRetry verifies a ForceSet during a Get callback
// causes Get's CAS-set to fail and a subsequent read returns the forced value.
func TestCacheAside_Get_ErrLockLostRetry(t *testing.T) {
	t.Parallel()
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
		// Get acquires the lock; ForceSet steals it during the callback.
		_, _ = client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
			close(getStarted)
			time.Sleep(300 * time.Millisecond)
			return "get-val", nil
		})
	}()

	<-getStarted
	time.Sleep(50 * time.Millisecond)

	err = client.ForceSet(ctx, time.Second*10, key, forcedVal)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	res, err := client.Get(ctx, time.Second*10, key, func(ctx context.Context, k string) (string, error) {
		t.Fatal("callback should not be called — value should exist")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, forcedVal, res, "ForceSet's value must survive Get's CAS failure")
}
