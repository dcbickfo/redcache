package redcache_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/google/go-cmp/cmp"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

var addr = []string{"127.0.0.1:6379"}

// skipIfNoRedis skips integration tests that need a live Redis when run under
// -short, so `go test -short ./...` exercises only the no-Redis unit subset.
func skipIfNoRedis(tb testing.TB) {
	tb.Helper()
	if testing.Short() {
		tb.Skip("requires Redis")
	}
}

// makeClient opens a Conn and derives a string view over it. The Conn is closed
// via t.Cleanup, so callers need no teardown of their own. The Conn is returned
// too for tests that inspect Redis directly (conn.Client()).
func makeClient(t *testing.T, addr []string) (*redcache.Cache, *redcache.Conn) {
	t.Helper()
	skipIfNoRedis(t)
	conn, err := redcache.Open(
		rueidis.ClientOption{
			InitAddress: addr,
		},
		redcache.WithLockTTL(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	return redcache.New(conn, redcache.StringCodec{}), conn
}

// mapLoader returns a multi-key loader that serves each requested key from vals.
func mapLoader(vals map[string]string) func(context.Context, []string) (map[string]string, error) {
	return func(_ context.Context, keys []string) (map[string]string, error) {
		res := make(map[string]string, len(keys))
		for _, k := range keys {
			res[k] = vals[k]
		}
		return res, nil
	}
}

func TestCache_Get(t *testing.T) {
	t.Parallel()
	client, _ := makeClient(t, addr)
	ctx := t.Context()
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

func TestCache_GetMulti(t *testing.T) {
	t.Parallel()
	client, _ := makeClient(t, addr)
	ctx := t.Context()
	keyAndVals := make(map[string]string)
	for i := range 3 {
		keyAndVals[fmt.Sprintf("key:%d:%s", i, uuid.New().String())] = fmt.Sprintf("val:%d:%s", i, uuid.New().String())
	}
	keys := make([]string, 0, len(keyAndVals))
	for k := range keyAndVals {
		keys = append(keys, k)
	}
	called := false

	load := mapLoader(keyAndVals)
	cb := func(ctx context.Context, keys []string) (map[string]string, error) {
		called = true
		return load(ctx, keys)
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

func TestCache_GetMulti_Partial(t *testing.T) {
	t.Parallel()
	client, _ := makeClient(t, addr)
	ctx := t.Context()
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

func TestCache_GetMulti_IgnoresLoaderExtraKeys(t *testing.T) {
	t.Parallel()
	skipIfNoRedis(t)

	metrics := &capturingMetrics{}
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: addr},
		redcache.WithLockTTL(time.Second),
		redcache.WithMetrics(metrics),
	)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	client := redcache.New(conn, redcache.StringCodec{})

	ctx := t.Context()
	key := "key:" + uuid.New().String()
	extra := "extra:" + uuid.New().String()

	res, err := client.GetMulti(ctx, 10*time.Second, []string{key}, func(_ context.Context, _ []string) (map[string]string, error) {
		return map[string]string{
			key:   "wanted",
			extra: "ignored",
		}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{key: "wanted"}, res)
	assert.Zero(t, metrics.lost.Load(), "extra loader keys must not be attempted as lock-owned writes")

	err = conn.Client().Do(ctx, conn.Client().B().Get().Key(extra).Build()).Error()
	require.True(t, rueidis.IsRedisNil(err), "extra loader keys must not be written")
}

func TestCache_GetMulti_PartLock(t *testing.T) {
	t.Parallel()
	client, conn := makeClient(t, addr)
	ctx := t.Context()
	keyAndVals := make(map[string]string)
	for i := range 3 {
		keyAndVals[fmt.Sprintf("key:%d:%s", i, uuid.New().String())] = fmt.Sprintf("val:%d:%s", i, uuid.New().String())
	}
	keys := make([]string, 0, len(keyAndVals))
	for k := range keyAndVals {
		keys = append(keys, k)
	}
	called := false

	load := mapLoader(keyAndVals)
	cb := func(ctx context.Context, keys []string) (map[string]string, error) {
		called = true
		return load(ctx, keys)
	}

	innerClient := conn.Client()
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

func TestCache_Del(t *testing.T) {
	t.Parallel()
	client, conn := makeClient(t, addr)
	ctx := t.Context()

	key := "key:" + uuid.New().String()
	val := "val:" + uuid.New().String()

	innerClient := conn.Client()
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

	client, _ := makeClient(t, addr)
	client2, _ := makeClient(t, addr)

	ctx := t.Context()
	keyAndVals := make(map[string]string)
	for i := range 6 {
		keyAndVals[fmt.Sprintf("key:%d:%s", i, uuid.New().String())] = fmt.Sprintf("val:%d:%s", i, uuid.New().String())
	}
	keys := make([]string, 0, len(keyAndVals))
	for k := range keyAndVals {
		keys = append(keys, k)
	}

	cb := mapLoader(keyAndVals)

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

	for range 100 {
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
				t.Context(),
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
				t.Context(),
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

// TestCBWrapper_GetMultiCheckConcurrentOverlap hammers overlapping GetMulti key
// sets, spread across separate clients or all on one client.
func TestCBWrapper_GetMultiCheckConcurrentOverlap(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		clients int
	}{
		{"DifferentClients", 4},
		{"SingleClient", 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clients := make([]*redcache.Cache, tt.clients)
			for i := range clients {
				clients[i], _ = makeClient(t, addr)
			}

			ctx := t.Context()
			keyAndVals := make(map[string]string)
			for i := range 6 {
				keyAndVals[fmt.Sprintf("key:%d:%s", i, uuid.New().String())] = fmt.Sprintf("val:%d:%s", i, uuid.New().String())
			}
			keys := make([]string, 0, len(keyAndVals))
			for k := range keyAndVals {
				keys = append(keys, k)
			}

			cb := mapLoader(keyAndVals)

			// Each group of key indexes overlaps the others pairwise.
			groups := [][]int{{0, 1, 3}, {2, 4, 5}, {0, 2, 4}, {1, 3, 5}}

			wg := sync.WaitGroup{}
			for range 100 {
				for g, idxs := range groups {
					groupKeys := make([]string, len(idxs))
					expected := make(map[string]string, len(idxs))
					for i, idx := range idxs {
						groupKeys[i] = keys[idx]
						expected[keys[idx]] = keyAndVals[keys[idx]]
					}
					client := clients[g%len(clients)]

					wg.Go(func() {
						localKeys := slices.Clone(groupKeys)
						rand.Shuffle(len(localKeys), func(i, j int) {
							localKeys[i], localKeys[j] = localKeys[j], localKeys[i]
						})
						out, err := client.GetMulti(ctx, time.Second*10, localKeys, cb)
						if err != nil {
							t.Errorf("GetMulti() error in %d: %v", g+1, err)
							return
						}
						if diff := cmp.Diff(expected, out); diff != "" {
							t.Errorf("GetMulti() mismatch in %d (-want +got):\n%s", g+1, diff)
							t.Errorf("got: %v", out)
						}
					})
				}
			}
			wg.Wait()
		})
	}
}

func TestCache_DelMulti(t *testing.T) {
	t.Parallel()
	client, conn := makeClient(t, addr)
	ctx := t.Context()

	keyAndVals := make(map[string]string)
	for i := range 3 {
		keyAndVals[fmt.Sprintf("key:%d:%s", i, uuid.New().String())] = fmt.Sprintf("val:%d:%s", i, uuid.New().String())
	}

	innerClient := conn.Client()
	for key, val := range keyAndVals {
		err := innerClient.Do(ctx, innerClient.B().Set().Key(key).Value(val).Nx().Get().Px(time.Second*30).Build()).Error()
		require.True(t, rueidis.IsRedisNil(err))
	}

	for key := range keyAndVals {
		err := innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
		require.NoErrorf(t, err, "expected no error, got %v", err)
	}

	err := client.DelMulti(ctx, slices.Collect(maps.Keys(keyAndVals)))
	require.NoError(t, err)

	for key := range keyAndVals {
		err = innerClient.Do(ctx, innerClient.B().Get().Key(key).Build()).Error()
		require.True(t, rueidis.IsRedisNil(err))
	}
}

func TestCache_Touch_ExtendsTTL(t *testing.T) {
	t.Parallel()
	client, conn := makeClient(t, addr)
	ctx := t.Context()

	key := "touch:" + uuid.New().String()

	_, err := client.Get(ctx, 500*time.Millisecond, key, func(_ context.Context, _ string) (string, error) {
		return "v", nil
	})
	require.NoError(t, err)

	require.NoError(t, client.Touch(ctx, 5*time.Second, key))

	pttl, err := conn.Client().Do(ctx, conn.Client().B().Pttl().Key(key).Build()).AsInt64()
	require.NoError(t, err)
	assert.Greater(t, pttl, int64(2000), "TTL should have been extended well past the original 500ms")
}

func TestCache_Touch_NoOpOnMissing(t *testing.T) {
	t.Parallel()
	client, conn := makeClient(t, addr)
	ctx := t.Context()

	key := "touch-missing:" + uuid.New().String()
	require.NoError(t, client.Touch(ctx, 5*time.Second, key), "Touch on missing key must succeed silently")

	err := conn.Client().Do(ctx, conn.Client().B().Get().Key(key).Build()).Error()
	require.True(t, rueidis.IsRedisNil(err), "Touch must not create the key")
}

func TestCache_Touch_NoOpOnLockValue(t *testing.T) {
	t.Parallel()
	client, conn := makeClient(t, addr)
	ctx := t.Context()

	key := "touch-lock:" + uuid.New().String()
	lockVal := "__redcache:lock:abc"
	require.NoError(t, conn.Client().Do(ctx,
		conn.Client().B().Set().Key(key).Value(lockVal).Px(time.Second).Build()).Error())

	require.NoError(t, client.Touch(ctx, 10*time.Second, key))

	pttl, err := conn.Client().Do(ctx, conn.Client().B().Pttl().Key(key).Build()).AsInt64()
	require.NoError(t, err)
	// Bound by the original 1000ms PX so partial regressions (e.g. extension to 2s) are also caught.
	assert.LessOrEqual(t, pttl, int64(1000), "lock TTL must NOT have been extended by Touch")

	got, err := conn.Client().Do(ctx, conn.Client().B().Get().Key(key).Build()).ToString()
	require.NoError(t, err)
	assert.Equal(t, lockVal, got, "lock value must be preserved")
}

func TestCache_TouchMulti_ExtendsTTLs(t *testing.T) {
	t.Parallel()
	client, conn := makeClient(t, addr)
	ctx := t.Context()

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

	require.NoError(t, client.TouchMulti(ctx, 5*time.Second, keys))

	for _, k := range keys {
		pttl, err := conn.Client().Do(ctx, conn.Client().B().Pttl().Key(k).Build()).AsInt64()
		require.NoError(t, err)
		assert.Greater(t, pttl, int64(2000), "key %q TTL should have been extended", k)
	}
}

func TestCache_TouchMulti_EmptyKeysIsNoOp(t *testing.T) {
	t.Parallel()
	client, _ := makeClient(t, addr)
	require.NoError(t, client.TouchMulti[string](t.Context(), 5*time.Second, nil))
}

func TestCache_GetParentContextCancellation(t *testing.T) {
	t.Parallel()
	client, conn := makeClient(t, addr)

	ctx, cancel := context.WithCancel(t.Context())
	key := "key:" + uuid.New().String()
	val := "val:" + uuid.New().String()

	// Lock the key so Get will wait.
	innerClient := conn.Client()
	lockVal := "__redcache:lock:" + uuid.New().String()
	err := innerClient.Do(t.Context(), innerClient.B().Set().Key(key).Value(lockVal).Nx().Get().Px(time.Second*30).Build()).Error()
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

// getConcurrently runs 400 concurrent Gets of key and asserts each returns want.
func getConcurrently(
	ctx context.Context,
	t *testing.T,
	client *redcache.Cache,
	key, want string,
	cb func(context.Context, string) (string, error),
) {
	t.Helper()
	var wg sync.WaitGroup
	for range 400 {
		wg.Go(func() {
			res, err := client.Get(ctx, time.Second*10, key, cb)
			if err != nil {
				t.Errorf("Get() error: %v", err)
				return
			}
			assert.Equal(t, want, res)
		})
	}
	wg.Wait()
}

// TestConcurrentRegisterRace stresses register() to guard the CompareAndDelete race.
func TestConcurrentRegisterRace(t *testing.T) {
	t.Parallel()
	skipIfNoRedis(t)
	// Minimum lock TTL forces expirations under contention.
	conn, err := redcache.Open(
		rueidis.ClientOption{
			InitAddress: addr,
		},
		redcache.WithLockTTL(100*time.Millisecond),
	)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	client := redcache.New(conn, redcache.StringCodec{})

	ctx := t.Context()
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

	getConcurrently(ctx, t, client, key, val, cb)

	// Cap above: a register-race regression would let every Get re-fire the callback unboundedly.
	mu.Lock()
	defer mu.Unlock()
	assert.Positive(t, callCount, "callback should be called at least once")
	assert.Less(t, callCount, 4000, "callback fired far more than expected — possible register-race regression")
}

// TestConcurrentGetSameKeySingleClient verifies a single client dedupes concurrent
// same-key Gets down to one callback.
func TestConcurrentGetSameKeySingleClient(t *testing.T) {
	t.Parallel()
	client, _ := makeClient(t, addr)

	ctx := t.Context()
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

	getConcurrently(ctx, t, client, key, val, cb)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, callCount, "callback should only be called once")
}

// TestCache_Get_LeaderNXFailure_WaitsForInvalidation verifies the
// leader/follower discipline when an in-process leader's SET NX is rejected.
// The leader must wait for the actual holder's invalidation rather than
// cancelling its own lockEntry and waking followers into a busy-loop NX race.
func TestCache_Get_LeaderNXFailure_WaitsForInvalidation(t *testing.T) {
	t.Parallel()
	client, conn := makeClient(t, addr)

	ctx := t.Context()
	key := "key:" + uuid.New().String()
	val := "val:" + uuid.New().String()

	// Plant a synthetic Redis lock from "another process" so SET NX is rejected.
	inner := conn.Client()
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
	client, _ := makeClient(t, addr)

	ctx := t.Context()
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
	for range 100 {
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

func TestCache_Get_CallbackError(t *testing.T) {
	t.Parallel()
	client, _ := makeClient(t, addr)
	ctx := t.Context()

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

func TestCache_GetMulti_CallbackError(t *testing.T) {
	t.Parallel()
	client, _ := makeClient(t, addr)
	ctx := t.Context()

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
	res, err := client.GetMulti(ctx, time.Second*10, keys, mapLoader(vals))
	require.NoError(t, err)
	if diff := cmp.Diff(vals, res); diff != "" {
		t.Errorf("GetMulti() mismatch (-want +got):\n%s", diff)
	}
}

func TestCache_Close(t *testing.T) {
	t.Parallel()
	client, conn := makeClient(t, addr)
	ctx := t.Context()

	key := "key:" + uuid.New().String()

	innerClient := conn.Client()
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
	conn.Close()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Get did not return after Close")
	case err := <-errCh:
		// Close cancels the pending lock entry and tears down the client, so the
		// waiter wakes and returns promptly instead of hanging on the persistent
		// lock until its 30s TTL. The exact error is teardown-ordering dependent:
		// the deadline (2s) may fire first, or the now-closing client surfaces on
		// retry — either proves Close unblocked the waiter.
		require.Error(t, err)
	}
}

func TestCache_CloseCancelsRefreshCallback(t *testing.T) {
	t.Parallel()
	skipIfNoRedis(t)

	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: addr},
		redcache.WithLockTTL(150*time.Millisecond),
		redcache.WithRefreshAfterFraction(0.5),
		redcache.WithRefreshBeta(0),
		redcache.WithRefreshWorkers(1),
		redcache.WithRefreshQueueSize(4),
		redcache.WithRefreshTimeout(2*time.Second),
	)
	require.NoError(t, err)
	client := redcache.New(conn, redcache.StringCodec{})

	ctx := t.Context()
	key := "close-refresh:" + uuid.New().String()
	// Wide TTL: the second Get must land past the refresh threshold (half the
	// TTL) and well before expiry, or it takes the miss path instead.
	ttl := 3 * time.Second

	_, err = client.Get(ctx, ttl, key, func(context.Context, string) (string, error) {
		return "initial", nil
	})
	require.NoError(t, err)

	time.Sleep(1600 * time.Millisecond)

	refreshStarted := make(chan struct{})
	_, err = client.Get(ctx, ttl, key, func(ctx context.Context, _ string) (string, error) {
		close(refreshStarted)
		<-ctx.Done()
		return "", ctx.Err()
	})
	require.NoError(t, err)

	select {
	case <-refreshStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh callback did not start")
	}

	done := make(chan struct{})
	start := time.Now()
	go func() {
		conn.Close()
		close(done)
	}()

	select {
	case <-done:
		require.Less(t, time.Since(start), 500*time.Millisecond)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close did not cancel the refresh callback promptly")
	}
}

func TestOpen_Validation(t *testing.T) {
	t.Parallel()
	t.Run("empty InitAddress", func(t *testing.T) {
		t.Parallel()
		_, err := redcache.Open(
			rueidis.ClientOption{},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "InitAddress")
	})

	t.Run("negative LockTTL", func(t *testing.T) {
		t.Parallel()
		_, err := redcache.Open(
			rueidis.ClientOption{InitAddress: addr},
			redcache.WithLockTTL(-1*time.Second),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "negative")
	})

	t.Run("too small LockTTL", func(t *testing.T) {
		t.Parallel()
		_, err := redcache.Open(
			rueidis.ClientOption{InitAddress: addr},
			redcache.WithLockTTL(10*time.Millisecond),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "100ms")
	})

	t.Run("client builder receives tracking options", func(t *testing.T) {
		t.Parallel()
		wantErr := errors.New("builder failed")
		called := false
		_, err := redcache.Open(
			rueidis.ClientOption{InitAddress: addr},
			redcache.WithClientBuilder(func(option rueidis.ClientOption) (rueidis.Client, error) {
				called = true
				if option.PipelineMultiplex != -1 {
					t.Fatalf("PipelineMultiplex = %d, want -1", option.PipelineMultiplex)
				}
				if option.OnInvalidations == nil {
					t.Fatal("OnInvalidations was not configured")
				}
				return nil, wantErr
			}),
		)
		require.ErrorIs(t, err, wantErr)
		require.True(t, called, "custom client builder was not called")
	})
}

// TestCache_Get_ErrLockLostRetry verifies a ForceSet during a Get callback
// causes Get's CAS-set to fail and a subsequent read returns the forced value.
func TestCache_Get_ErrLockLostRetry(t *testing.T) {
	t.Parallel()
	skipIfNoRedis(t)
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: addr},
		redcache.WithLockTTL(time.Second*2),
	)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	client := redcache.New(conn, redcache.StringCodec{})
	ctx := t.Context()

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
