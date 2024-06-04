package redcache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

var addr = []string{"127.0.0.1:6379"}

func makeClient(t *testing.T, addr []string) *CacheAside {
	client, err := NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestCacheAside_Get(t *testing.T) {
	client := makeClient(t, addr)
	defer client.client.Close()
	ctx := context.Background()
	key := "key:" + uuid.New().String()
	val := "val:" + uuid.New().String()
	called := false

	cb := func(ctx context.Context, key string) (string, error) {
		called = true
		return val, nil
	}

	res, err := client.Get(ctx, key, cb)
	require.NoError(t, err)
	if diff := cmp.Diff(val, res); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.True(t, called)

	called = false
	res, err = client.Get(ctx, key, cb)
	require.NoError(t, err)
	if diff := cmp.Diff(val, res); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.False(t, called)

}

func TestCacheAside_GetMulti(t *testing.T) {
	client := makeClient(t, addr)
	defer client.client.Close()
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

	res, err := client.GetMulti(ctx, keys, cb)
	require.NoError(t, err)
	if diff := cmp.Diff(keyAndVals, res); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.True(t, called)

	called = false
	res, err = client.GetMulti(ctx, keys, cb)
	require.NoError(t, err)
	if diff := cmp.Diff(keyAndVals, res); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.False(t, called)
}

func TestCacheAside_GetMulti_Partial(t *testing.T) {
	client := makeClient(t, addr)
	defer client.client.Close()
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

	res, err := client.Get(ctx, keys[0], cbSingle)
	require.NoError(t, err)
	if diff := cmp.Diff(keyAndVals[keys[0]], res); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.True(t, called)

	called = false
	resMulti, err := client.GetMulti(ctx, keys, cb)
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
	resMulti, err = client.GetMulti(ctx, keys, cb)
	require.NoError(t, err)
	if diff := cmp.Diff(keyAndVals, resMulti); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.False(t, called)
}

func TestCacheAside_GetMulti_PartLock(t *testing.T) {
	client := makeClient(t, addr)
	defer client.client.Close()
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

	innerClient := client.client
	lockVal := prefix + uuid.New().String()
	err := innerClient.Do(ctx, innerClient.B().Set().Key(keys[0]).Value(lockVal).Nx().Get().Px(time.Millisecond + 100).Build()).Error()
	require.True(t, rueidis.IsRedisNil(err))

	res, err := client.GetMulti(ctx, keys, cb)
	require.NoError(t, err)
	if diff := cmp.Diff(keyAndVals, res); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.True(t, called)

	called = false
	res, err = client.GetMulti(ctx, keys, cb)
	require.NoError(t, err)
	if diff := cmp.Diff(keyAndVals, res); diff != "" {
		t.Errorf("Get() mismatch (-want +got):\n%s", diff)
	}
	require.False(t, called)
}
