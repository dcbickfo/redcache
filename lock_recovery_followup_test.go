package redcache_test

import (
	"context"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

type followupRecoveryMetrics struct {
	redcache.NoopMetrics
	contended atomic.Int64
	lost      atomic.Int64
}

func (m *followupRecoveryMetrics) LockContended(n int64) { m.contended.Add(n) }
func (m *followupRecoveryMetrics) LockLost(string)       { m.lost.Add(1) }

type recoveryAcquisitionClient struct {
	rueidis.Client
	attempts atomic.Int32
}

func (c *recoveryAcquisitionClient) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	args := cmd.Commands()
	if args[0] == "SET" && len(args) > 3 && slices.Contains(args[3:], "NX") {
		c.attempts.Add(1)
	}
	return c.Client.Do(ctx, cmd)
}

func openUntrackedRecoveryClient(opt rueidis.ClientOption) (rueidis.Client, error) {
	opt.DisableCache = true
	return rueidis.NewClient(opt)
}

func TestLockRecovery_ManyLocalFollowersAfterDelete(t *testing.T) {
	// Run serially so the process-wide goroutine check can settle after cleanup.
	const callers = 8
	baseline := runtime.NumGoroutine()
	metrics := &followupRecoveryMetrics{}
	var observed *recoveryAcquisitionClient
	cache, conn := openLockRecoveryCache(t, redcache.WithMetrics(metrics),
		redcache.WithClientBuilder(func(opt rueidis.ClientOption) (rueidis.Client, error) {
			client, err := openUntrackedRecoveryClient(opt)
			if err != nil {
				return nil, err
			}
			observed = &recoveryAcquisitionClient{Client: client}
			return observed, nil
		}))
	other, otherConn := openLockRecoveryCache(t)
	key := "many-followers:" + uuid.New().String()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	started, resume, replacement := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var loads atomic.Int32
	load := func(ctx context.Context, _ []string) (map[string]string, error) {
		switch loads.Add(1) {
		case 1:
			close(started)
			select {
			case <-resume:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return map[string]string{key: "stale"}, nil
		case 2:
			close(replacement)
		}
		return map[string]string{key: reloadedValue}, nil
	}
	keys := []string{key}
	results := make([]<-chan recoveryGetResult, 0, callers)
	results = append(results, startRecoveryGet(ctx, cache, keys, false, load))
	awaitLockRecoveryTest(t, started)
	for range callers - 1 {
		results = append(results, startRecoveryGet(ctx, cache, keys, false, load))
	}
	require.Eventually(t, func() bool { return metrics.contended.Load() >= callers-1 },
		5*time.Second, 20*time.Millisecond, "all followers must contend before deletion")
	require.EqualValues(t, 1, observed.attempts.Load(), "only the original leader should attempt SET NX")

	require.NoError(t, other.Del(ctx, key))
	awaitLockRecoveryTest(t, replacement)
	close(resume)

	for _, done := range results {
		result := awaitLockRecoveryTest(t, done)
		require.NoError(t, result.err)
		require.Equal(t, reloadedValue, result.values[key])
	}
	require.EqualValues(t, 2, loads.Load(), "only one replacement loader should run")
	require.EqualValues(t, 2, observed.attempts.Load(), "only one local caller should attempt SET NX after deletion")
	require.EqualValues(t, 1, metrics.lost.Load(), "the original loader must lose its publish CAS")

	conn.Close()
	otherConn.Close()
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= baseline+2 },
		5*time.Second, 20*time.Millisecond, "goroutine count should settle near the pre-test count (%d)", baseline)
}

func TestLockRecovery_GetMultiAbsentAndRemoteLocked(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	gate := &recoveryWaitGate{ctx: ctx, waiting: make(chan struct{}), resume: make(chan struct{})}
	cache, _ := openLockRecoveryCache(t, redcache.WithLockPrefix(recoveryLockPrefix),
		redcache.WithMetrics(gate), redcache.WithClientBuilder(openUntrackedRecoveryClient))
	holderCache, holder := openLockRecoveryCache(t)
	other, _ := openLockRecoveryCache(t)
	keyA := "mixed-locks:" + uuid.New().String() + ":a"
	keyB := keyA + ":b"
	client := holder.Client()
	for _, key := range []string{keyA, keyB} {
		cmd := client.B().Set().Key(key).Value(recoveryHolderValue).Nx().Px(recoveryLockTTL).Build()
		require.NoError(t, client.Do(ctx, cmd).Error())
	}

	var loadsA, loadsB atomic.Int32
	done := startRecoveryGet(ctx, cache, []string{keyA, keyB}, true,
		func(_ context.Context, keys []string) (map[string]string, error) {
			values := make(map[string]string, len(keys))
			for _, key := range keys {
				switch key {
				case keyA:
					loadsA.Add(1)
				case keyB:
					loadsB.Add(1)
				}
				values[key] = reloadedValue
			}
			return values, nil
		})
	awaitLockRecoveryTest(t, gate.waiting)
	require.NoError(t, other.Del(ctx, keyA))
	close(gate.resume)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		val, found, err := other.Peek[string, string](ctx, time.Minute, keyA)
		require.NoError(c, err)
		require.True(c, found, "A should be published while B is still locked")
		require.Equal(c, reloadedValue, val)
	}, 5*time.Second, 20*time.Millisecond)
	require.EqualValues(t, 1, loadsA.Load())
	require.Zero(t, loadsB.Load(), "B's remote lock must prevent loading")
	locked, err := client.Do(ctx, client.B().Get().Key(keyB).Build()).ToString()
	require.NoError(t, err)
	require.Equal(t, recoveryHolderValue, locked)
	select {
	case result := <-done:
		t.Fatalf("GetMulti returned before B was published: %+v", result)
	default:
	}

	const published = "published by holder"
	require.NoError(t, holderCache.ForceSet(ctx, time.Minute, keyB, published))
	result := awaitLockRecoveryTest(t, done)
	require.NoError(t, result.err)
	require.Equal(t, map[string]string{keyA: reloadedValue, keyB: published}, result.values)
	require.EqualValues(t, 1, loadsA.Load())
	require.Zero(t, loadsB.Load())
}

func TestLockRecovery_RepeatedLossAndCanceledRetry(t *testing.T) {
	t.Run("RetryCap", func(t *testing.T) {
		metrics := &followupRecoveryMetrics{}
		cache, _ := openLockRecoveryCache(t, redcache.WithMetrics(metrics),
			redcache.WithClientBuilder(openUntrackedRecoveryClient))
		other, _ := openLockRecoveryCache(t)
		key := "retry-cap:" + uuid.New().String()
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()

		var loads int
		_, err := cache.Get(ctx, time.Minute, key, func(ctx context.Context, key string) (string, error) {
			loads++
			return reloadedValue, other.Del(ctx, key)
		})
		require.ErrorIs(t, err, redcache.ErrLockLost, "repeated CAS failures must reach the retry cap")
		require.Equal(t, 4, loads, "one initial load plus three retries")
		require.EqualValues(t, loads, metrics.lost.Load(), "every publish CAS must lose its lock")
	})

	t.Run("CanceledRetry", func(t *testing.T) {
		metrics := &followupRecoveryMetrics{}
		cache, _ := openLockRecoveryCache(t, redcache.WithMetrics(metrics),
			redcache.WithClientBuilder(openUntrackedRecoveryClient))
		other, _ := openLockRecoveryCache(t)
		key := "canceled-retry:" + uuid.New().String()
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		request, cancelRequest := context.WithCancel(ctx)
		defer cancelRequest()

		retrying := make(chan struct{})
		var loads int
		done := startRecoveryGet(request, cache, []string{key}, false,
			func(ctx context.Context, _ []string) (map[string]string, error) {
				loads++
				if loads == 1 {
					return map[string]string{key: "stale"}, other.Del(ctx, key)
				}
				close(retrying)
				<-ctx.Done()
				return nil, ctx.Err()
			})
		awaitLockRecoveryTest(t, retrying)
		cancelRequest()
		require.ErrorIs(t, awaitLockRecoveryTest(t, done).err, context.Canceled)
		require.Equal(t, 2, loads)
		require.EqualValues(t, 1, metrics.lost.Load())

		// A stale wait entry would cause local contention even if polling
		// eventually let this independent caller recover and load.
		before := metrics.contended.Load()
		var laterLoads int
		value, err := cache.Get(ctx, time.Minute, key, func(context.Context, string) (string, error) {
			laterLoads++
			return reloadedValue, nil
		})
		require.NoError(t, err)
		require.Equal(t, reloadedValue, value)
		require.Equal(t, 1, laterLoads)
		require.Equal(t, before, metrics.contended.Load(), "the canceled retry must leave no wait entry")
	})
}
