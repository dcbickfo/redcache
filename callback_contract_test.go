package redcache_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

// GetMulti must fail fast with a key-specific error when the callback omits a
// requested key, rather than spinning (re-locking + re-invoking fn) until the
// caller's context deadline expires.
func TestCacheAside_GetMulti_CallbackOmitsKey_FailsFast(t *testing.T) {
	t.Parallel()
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	present := "k:present:" + uuid.New().String()
	omitted := "k:omitted:" + uuid.New().String()
	keys := []string{present, omitted}

	start := time.Now()
	_, err := client.GetMulti(ctx, 10*time.Second, keys, func(ctx context.Context, ks []string) (map[string]string, error) {
		res := make(map[string]string)
		for _, k := range ks {
			if k == omitted {
				continue // violate the contract: omit a requested key
			}
			res[k] = "val:" + k
		}
		return res, nil
	})
	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded, "must not hang until the deadline")
	require.ErrorContains(t, err, omitted, "error should name the omitted key")
	require.Less(t, time.Since(start), 1500*time.Millisecond, "should fail fast, not spin to the deadline")
}

// SetMulti must surface keys the callback omits (locked but never returned) in
// the BatchError rather than silently reverting them and returning nil.
func TestPrimeableCacheAside_SetMulti_CallbackOmitsKey_ReportedInBatchError(t *testing.T) {
	t.Parallel()
	client := makePrimeableClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	present := "k:present:" + uuid.New().String()
	omitted := "k:omitted:" + uuid.New().String()
	keys := []string{present, omitted}

	err := client.SetMulti(ctx, 10*time.Second, keys, func(ctx context.Context, ks []string) (map[string]string, error) {
		res := make(map[string]string)
		for _, k := range ks {
			if k == omitted {
				continue // violate the contract: omit a locked key
			}
			res[k] = "val:" + k
		}
		return res, nil
	})
	require.Error(t, err)
	var be *redcache.BatchError
	require.ErrorAs(t, err, &be)
	require.True(t, be.HasError(omitted), "omitted key must be reported as failed")
	require.False(t, be.HasError(present), "present key must not be reported as failed")

	// The present key was cached (Get is a hit); the omitted key was reverted
	// (Get is a miss that runs the callback).
	presentCalled := false
	v, err := client.Get(ctx, 10*time.Second, present, func(ctx context.Context, k string) (string, error) {
		presentCalled = true
		return "x", nil
	})
	require.NoError(t, err)
	require.False(t, presentCalled, "present key should be a cache hit")
	require.Equal(t, "val:"+present, v)

	omittedCalled := false
	_, err = client.Get(ctx, 10*time.Second, omitted, func(ctx context.Context, k string) (string, error) {
		omittedCalled = true
		return "y", nil
	})
	require.NoError(t, err)
	require.True(t, omittedCalled, "omitted key should be a cache miss (it was reverted)")
}

// Get must reject a non-positive ttl up front with a clear error rather than
// running the callback and surfacing an opaque Redis "invalid expire time".
func TestCacheAside_Get_RejectsNonPositiveTTL(t *testing.T) {
	t.Parallel()
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	called := false
	_, err := client.Get(ctx, 0, "k:"+uuid.New().String(), func(ctx context.Context, k string) (string, error) {
		called = true
		return "v", nil
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "ttl")
	require.False(t, called, "callback must not run when ttl is invalid")
}

// Touch must reject a non-positive ttl rather than issuing PEXPIRE 0, which
// would delete the key.
func TestCacheAside_Touch_RejectsNonPositiveTTL(t *testing.T) {
	t.Parallel()
	client := makeClient(t, addr)
	defer client.Client().Close()
	ctx := context.Background()

	key := "k:" + uuid.New().String()
	_, err := client.Get(ctx, 10*time.Second, key, func(ctx context.Context, k string) (string, error) {
		return "v", nil
	})
	require.NoError(t, err)

	err = client.Touch(ctx, 0, key)
	require.Error(t, err)
	require.ErrorContains(t, err, "ttl")

	raw := client.Client()
	getErr := raw.Do(ctx, raw.B().Get().Key(key).Build()).Error()
	require.False(t, rueidis.IsRedisNil(getErr), "Touch(ttl=0) must not delete the key")
}
