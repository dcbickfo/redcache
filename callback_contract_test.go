package redcache_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

// omitLoader returns a value for every key except omitted, violating the
// loader contract.
func omitLoader(omitted string) func(context.Context, []string) (map[string]string, error) {
	return func(_ context.Context, ks []string) (map[string]string, error) {
		res := make(map[string]string, len(ks))
		for _, k := range ks {
			if k != omitted {
				res[k] = "val:" + k
			}
		}
		return res, nil
	}
}

// A loader that omits a requested key must fail GetMulti fast with an error
// naming the key, not re-lock and re-run the loader until the deadline.
func TestGetMulti_LoaderOmitsKey_FailsFast(t *testing.T) {
	t.Parallel()
	cache, _ := makeClient(t, []string{"127.0.0.1:6379"})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	present := "omit:present:" + uuid.New().String()
	omitted := "omit:omitted:" + uuid.New().String()

	var calls int
	loader := omitLoader(omitted)
	start := time.Now()
	_, err := cache.GetMulti(ctx, 10*time.Second, []string{present, omitted}, func(ctx context.Context, ks []string) (map[string]string, error) {
		calls++
		return loader(ctx, ks)
	})
	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, redcache.ErrMissingValue)
	require.ErrorContains(t, err, omitted)
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, 1, calls)
}

// SetMulti must report a key the loader omitted (locked, never written) as
// failed instead of silently reverting it and returning nil.
func TestSetMulti_LoaderOmitsKey_ReportedAsFailed(t *testing.T) {
	t.Parallel()
	cache, _ := makeClient(t, []string{"127.0.0.1:6379"})
	ctx := t.Context()
	present := "omit:present:" + uuid.New().String()
	omitted := "omit:omitted:" + uuid.New().String()

	err := cache.SetMulti(ctx, 10*time.Second, []string{present, omitted}, omitLoader(omitted))
	var bke *redcache.BatchKeyError[string]
	require.ErrorAs(t, err, &bke)
	require.ErrorIs(t, bke.ErrorFor(omitted), redcache.ErrMissingValue)
	require.False(t, bke.HasError(present))

	v, ok, err := cache.Peek(ctx, 10*time.Second, present)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "val:"+present, v)
	_, ok, err = cache.Peek(ctx, 10*time.Second, omitted)
	require.NoError(t, err)
	require.False(t, ok, "omitted key must be reverted")
}
