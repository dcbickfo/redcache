package redcache_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

func makeRefreshClient(t *testing.T, addr []string, fraction float64) (*redcache.Cache, *redcache.Conn) {
	t.Helper()
	skipIfNoRedis(t)
	conn, err := redcache.Open(
		rueidis.ClientOption{
			InitAddress: addr,
		},
		redcache.WithLockTTL(time.Second*2),
		redcache.WithRefreshAfterFraction(fraction),
		// Disable XFetch sampling so tests assert exact refresh-vs-no-refresh
		// outcomes; sampling is exercised by TestShouldRefresh_XFetch.
		redcache.WithRefreshBeta(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	return redcache.New(conn, redcache.StringCodec{}), conn
}

// Refresh-ahead integration tests use wall-clock Redis TTL transitions. Keep
// this group serial so package-wide load cannot expire data before an expected
// stale-value assertion.
func TestRefreshAhead_TriggersBackgroundRefresh(t *testing.T) {
	// fraction=0.5 -> refresh once <50% TTL remaining.
	client, _ := makeRefreshClient(t, addr, 0.5)
	ctx := t.Context()

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
	client, _ := makeRefreshClient(t, addr, 0.5)
	ctx := t.Context()

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
	results := make(chan struct {
		value string
		err   error
	}, 20)
	for range 20 {
		wg.Go(func() {
			res, err := client.Get(ctx, ttl, key, cb)
			results <- struct {
				value string
				err   error
			}{value: res, err: err}
		})
	}
	wg.Wait()
	close(results)
	for result := range results {
		require.NoError(t, result.err)
		assert.Equal(t, "initial", result.value)
	}

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
	client, _ := makeClient(t, addr)
	ctx := t.Context()

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
	client, _ := makeRefreshClient(t, addr, 0.5)
	ctx := t.Context()

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
	client, _ := makeRefreshClient(t, addr, 0.5)
	ctx := t.Context()

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
	client, conn := makeRefreshClient(t, addr, 0.5)
	ctx := t.Context()

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
	require.NoError(t, conn.Client().Do(ctx,
		conn.Client().B().Set().Key(key).Value(lockVal).Px(ttl).Build()).Error())

	close(callbackProceed)

	// Independent client for the post-Close read: Close tears down client's own
	// connection, so we verify the stored value through a separate one.
	verify, vErr := rueidis.NewClient(rueidis.ClientOption{InitAddress: addr})
	require.NoError(t, vErr)
	defer verify.Close()

	// Drain the refresh worker so its post-callback Lua-CAS has either run or
	// definitively skipped before we assert. Avoids a wall-clock race on slow CI.
	conn.Close()

	got, gErr := verify.Do(ctx, verify.B().Get().Key(key).Build()).ToString()
	require.NoError(t, gErr)
	assert.Equal(t, lockVal, got, "refresh-ahead must skip the SET when a lock value is present")
}

func TestRefreshAhead_DoesNotStompNewerRealValue(t *testing.T) {
	client, conn := makeRefreshClient(t, addr, 0.5)
	ctx := t.Context()

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

	require.NoError(t, client.ForceSet(ctx, ttl, key, "newer"))

	close(callbackProceed)

	verify, vErr := rueidis.NewClient(rueidis.ClientOption{InitAddress: addr})
	require.NoError(t, vErr)
	defer verify.Close()

	conn.Close()

	got, gErr := verify.Do(ctx, verify.B().Get().Key(key).Build()).ToString()
	require.NoError(t, gErr)
	assert.Equal(t, "__redcache:v1:0:newer", got, "refresh-ahead must not overwrite a newer real value")
}

func TestRefreshAhead_DoesNotReleaseNewerRefreshLock(t *testing.T) {
	skipIfNoRedis(t)

	open := func(t *testing.T, lockTTL time.Duration) (*redcache.Cache, *redcache.Conn) {
		t.Helper()
		conn, err := redcache.Open(
			rueidis.ClientOption{InitAddress: addr},
			redcache.WithLockTTL(lockTTL),
			redcache.WithRefreshAfterFraction(0.5),
			redcache.WithRefreshBeta(0),
			redcache.WithRefreshWorkers(1),
			redcache.WithRefreshQueueSize(4),
			redcache.WithRefreshTimeout(2*time.Second),
		)
		require.NoError(t, err)
		return redcache.New(conn, redcache.StringCodec{}), conn
	}

	client1, conn1 := open(t, 150*time.Millisecond)
	client2, conn2 := open(t, 2*time.Second)
	client3, conn3 := open(t, 2*time.Second)
	t.Cleanup(conn1.Close)
	t.Cleanup(conn2.Close)
	t.Cleanup(conn3.Close)

	ctx := t.Context()
	key := "refresh-lock:" + uuid.New().String()
	ttl := time.Second

	_, err := client1.Get(ctx, ttl, key, func(context.Context, string) (string, error) {
		return "initial", nil
	})
	require.NoError(t, err)

	triggerRefresh := func(
		t *testing.T,
		client *redcache.Cache,
		started <-chan struct{},
		fn func(context.Context, string) (string, error),
		failureMsg string,
	) {
		t.Helper()
		var lastErr error
		require.Eventually(t, func() bool {
			_, lastErr = client.Get(ctx, ttl, key, fn)
			if lastErr != nil {
				return true
			}
			select {
			case <-started:
				return true
			default:
				return false
			}
		}, 3*time.Second, 20*time.Millisecond, failureMsg)
		require.NoError(t, lastErr)
		select {
		case <-started:
		default:
			t.Fatal(failureMsg)
		}
	}

	firstStarted := make(chan struct{})
	startFirst := sync.OnceFunc(func() { close(firstStarted) })
	firstProceed := make(chan struct{})
	releaseFirst := sync.OnceFunc(func() { close(firstProceed) })
	defer releaseFirst()
	firstErr := errors.New("first refresh failed")
	triggerRefresh(t, client1, firstStarted, func(context.Context, string) (string, error) {
		startFirst()
		<-firstProceed
		return "", firstErr
	}, "first refresh did not start")

	secondStarted := make(chan struct{})
	startSecond := sync.OnceFunc(func() { close(secondStarted) })
	secondProceed := make(chan struct{})
	releaseSecond := sync.OnceFunc(func() { close(secondProceed) })
	defer releaseSecond()
	triggerRefresh(t, client2, secondStarted, func(context.Context, string) (string, error) {
		startSecond()
		<-secondProceed
		return "second", nil
	}, "second refresh did not start")

	releaseFirst()
	time.Sleep(50 * time.Millisecond)

	thirdStarted := make(chan struct{})
	_, err = client3.Get(ctx, ttl, key, func(context.Context, string) (string, error) {
		close(thirdStarted)
		return "third", nil
	})
	require.NoError(t, err)

	select {
	case <-thirdStarted:
		releaseSecond()
		t.Fatal("third refresh started while the second refresh lock should still be owned")
	case <-time.After(300 * time.Millisecond):
	}

	releaseSecond()
}

func TestRefreshAhead_GetMulti(t *testing.T) {
	client, _ := makeRefreshClient(t, addr, 0.5)
	ctx := t.Context()

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
	skipIfNoRedis(t)
	// Tiny pool (1 worker, queue size 1) plus a sleeping callback so the queue fills fast.
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: addr},
		redcache.WithLockTTL(time.Second*3),
		redcache.WithRefreshAfterFraction(0.5),
		redcache.WithRefreshBeta(0),
		redcache.WithRefreshWorkers(1),
		redcache.WithRefreshQueueSize(1),
	)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	client := redcache.New(conn, redcache.StringCodec{})
	ctx := t.Context()

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
	results := make(chan struct {
		value string
		err   error
	}, len(keys))
	for _, key := range keys {
		k := key
		wg.Go(func() {
			res, err := client.Get(ctx, ttl, k, refreshCb)
			results <- struct {
				value string
				err   error
			}{value: res, err: err}
		})
	}
	wg.Wait()
	close(results)
	for result := range results {
		require.NoError(t, result.err)
		assert.Equal(t, "initial", result.value)
	}

	time.Sleep(1500 * time.Millisecond)

	count := refreshCount.Load()
	assert.Less(t, count, int64(numKeys),
		"expected fewer than %d refreshes, got %d — backpressure should drop excess jobs", numKeys, count)
	assert.Positive(t, count, "at least one refresh should have executed")
}

func TestRefreshAhead_FractionValidation(t *testing.T) {
	t.Parallel()
	invalid := []struct {
		name string
		opts []redcache.Option
		want string
	}{
		{"negative fraction", []redcache.Option{redcache.WithRefreshAfterFraction(-0.1)}, "RefreshAfterFraction"},
		{"fraction equals 1", []redcache.Option{redcache.WithRefreshAfterFraction(1.0)}, "RefreshAfterFraction"},
		{"fraction greater than 1", []redcache.Option{redcache.WithRefreshAfterFraction(1.5)}, "RefreshAfterFraction"},
		{
			"negative RefreshWorkers",
			[]redcache.Option{redcache.WithRefreshAfterFraction(0.8), redcache.WithRefreshWorkers(-1)},
			"RefreshWorkers",
		},
		{
			"negative RefreshQueueSize",
			[]redcache.Option{redcache.WithRefreshAfterFraction(0.8), redcache.WithRefreshQueueSize(-1)},
			"RefreshQueueSize",
		},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := redcache.Open(rueidis.ClientOption{InitAddress: addr}, tc.opts...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
	t.Run("valid fraction", func(t *testing.T) {
		t.Parallel()
		skipIfNoRedis(t)
		conn, err := redcache.Open(
			rueidis.ClientOption{InitAddress: addr},
			redcache.WithRefreshAfterFraction(0.8),
		)
		require.NoError(t, err)
		t.Cleanup(conn.Close)
	})
	t.Run("custom workers and queue", func(t *testing.T) {
		t.Parallel()
		skipIfNoRedis(t)
		conn, err := redcache.Open(
			rueidis.ClientOption{InitAddress: addr},
			redcache.WithRefreshAfterFraction(0.8),
			redcache.WithRefreshWorkers(2),
			redcache.WithRefreshQueueSize(16),
		)
		require.NoError(t, err)
		t.Cleanup(conn.Close)
	})
}
