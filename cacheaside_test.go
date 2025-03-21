package redcache_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dcbickfo/redcache"

	"github.com/dcbickfo/redcache/internal/mapsx"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	lockVal := "redcache:" + uuid.New().String()
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
