package redcachetest_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
	"github.com/dcbickfo/redcache/redcachetest"
)

// countingLoader returns a single-key loader that records how many times it ran
// and which keys it was asked for.
func countingLoader[V any](v V) (func(context.Context, string) (V, error), *atomic.Int64) {
	var calls atomic.Int64
	fn := func(_ context.Context, _ string) (V, error) {
		calls.Add(1)
		return v, nil
	}
	return fn, &calls
}

func TestFake_SatisfiesInterface(t *testing.T) {
	var _ redcache.Cache[string, []byte] = redcachetest.New[string, []byte]()
}

func TestGet_MissCallsFnOnce(t *testing.T) {
	f := redcachetest.New[string, int]()
	fn, calls := countingLoader(42)

	for i := 0; i < 3; i++ {
		got, err := f.Get(context.Background(), time.Minute, "k", fn)
		require.NoError(t, err)
		assert.Equal(t, 42, got)
	}
	assert.Equal(t, int64(1), calls.Load(), "fn should run exactly once across repeated Gets")
}

func TestGet_HitSkipsFn(t *testing.T) {
	f := redcachetest.New[string, int]()
	require.NoError(t, f.ForceSet(context.Background(), time.Minute, "k", 7))

	var ran atomic.Bool
	got, err := f.Get(context.Background(), time.Minute, "k", func(context.Context, string) (int, error) {
		ran.Store(true)
		return 999, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 7, got)
	assert.False(t, ran.Load(), "fn must not run on a hit")
}

func TestGet_ZeroValueIsAHit(t *testing.T) {
	tests := []struct {
		name  string
		store func(f *redcachetest.Fake[string, int])
	}{
		{
			name:  "force set zero",
			store: func(f *redcachetest.Fake[string, int]) { _ = f.ForceSet(context.Background(), time.Minute, "k", 0) },
		},
		{
			name: "get loads zero",
			store: func(f *redcachetest.Fake[string, int]) {
				_, _ = f.Get(context.Background(), time.Minute, "k", func(context.Context, string) (int, error) {
					return 0, nil
				})
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := redcachetest.New[string, int]()
			tc.store(f)

			var ran atomic.Bool
			got, err := f.Get(context.Background(), time.Minute, "k", func(context.Context, string) (int, error) {
				ran.Store(true)
				return 1, nil
			})
			require.NoError(t, err)
			assert.Equal(t, 0, got, "stored zero value should be returned")
			assert.False(t, ran.Load(), "presence of a zero value is still a hit")
		})
	}
}

func TestGet_FnErrorNotStored(t *testing.T) {
	f := redcachetest.New[string, int]()
	wantErr := errors.New("boom")

	_, err := f.Get(context.Background(), time.Minute, "k", func(context.Context, string) (int, error) {
		return 0, wantErr
	})
	require.ErrorIs(t, err, wantErr)

	// A second Get must re-invoke fn — the failed load was not cached.
	fn, calls := countingLoader(5)
	got, err := f.Get(context.Background(), time.Minute, "k", fn)
	require.NoError(t, err)
	assert.Equal(t, 5, got)
	assert.Equal(t, int64(1), calls.Load())
}

func TestDel_Evicts(t *testing.T) {
	f := redcachetest.New[string, int]()
	require.NoError(t, f.ForceSet(context.Background(), time.Minute, "k", 3))
	require.NoError(t, f.Del(context.Background(), "k"))

	fn, calls := countingLoader(11)
	got, err := f.Get(context.Background(), time.Minute, "k", fn)
	require.NoError(t, err)
	assert.Equal(t, 11, got)
	assert.Equal(t, int64(1), calls.Load(), "Del should force a re-fetch")
}

func TestDelMulti_Evicts(t *testing.T) {
	f := redcachetest.New[string, int]()
	require.NoError(t, f.ForceSetMulti(context.Background(), time.Minute, map[string]int{"a": 1, "b": 2, "c": 3}))
	require.NoError(t, f.DelMulti(context.Background(), []string{"a", "c", "missing"}))

	var missing []string
	out, err := f.GetMulti(context.Background(), time.Minute, []string{"a", "b", "c"}, func(_ context.Context, m []string) (map[string]int, error) {
		missing = append(missing, m...)
		res := make(map[string]int, len(m))
		for _, k := range m {
			res[k] = -1
		}
		return res, nil
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "c"}, missing, "only deleted keys should miss")
	assert.Equal(t, map[string]int{"a": -1, "b": 2, "c": -1}, out)
}

func TestTouch_ExtendsExpiry(t *testing.T) {
	f := redcachetest.New[string, int]()
	require.NoError(t, f.ForceSet(context.Background(), 20*time.Millisecond, "k", 8))

	time.Sleep(10 * time.Millisecond)
	require.NoError(t, f.Touch(context.Background(), time.Minute, "k"))
	time.Sleep(20 * time.Millisecond) // past the original deadline, within the new one.

	var ran atomic.Bool
	got, err := f.Get(context.Background(), time.Minute, "k", func(context.Context, string) (int, error) {
		ran.Store(true)
		return 0, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 8, got)
	assert.False(t, ran.Load(), "Touch should have kept the entry alive")
}

func TestTouch_NoOpOnMissingKey(t *testing.T) {
	f := redcachetest.New[string, int]()
	// Touching an absent key must not create it.
	require.NoError(t, f.Touch(context.Background(), time.Minute, "absent"))

	fn, calls := countingLoader(99)
	got, err := f.Get(context.Background(), time.Minute, "absent", fn)
	require.NoError(t, err)
	assert.Equal(t, 99, got)
	assert.Equal(t, int64(1), calls.Load(), "Touch must not resurrect/create a missing key")
}

func TestTouchMulti_ExtendsPresentKeysOnly(t *testing.T) {
	f := redcachetest.New[string, int]()
	require.NoError(t, f.ForceSet(context.Background(), 20*time.Millisecond, "live", 1))

	time.Sleep(10 * time.Millisecond)
	require.NoError(t, f.TouchMulti(context.Background(), time.Minute, []string{"live", "absent"}))
	time.Sleep(20 * time.Millisecond)

	// "live" survives.
	var liveRan atomic.Bool
	got, err := f.Get(context.Background(), time.Minute, "live", func(context.Context, string) (int, error) {
		liveRan.Store(true)
		return 0, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, got)
	assert.False(t, liveRan.Load())

	// "absent" was never created.
	fn, calls := countingLoader(2)
	_, err = f.Get(context.Background(), time.Minute, "absent", fn)
	require.NoError(t, err)
	assert.Equal(t, int64(1), calls.Load())
}

func TestGetMulti_PartialHitCallsFnWithMissingOnly(t *testing.T) {
	f := redcachetest.New[string, int]()
	require.NoError(t, f.ForceSet(context.Background(), time.Minute, "a", 1))
	require.NoError(t, f.ForceSet(context.Background(), time.Minute, "c", 3))

	var gotMissing []string
	var calls atomic.Int64
	out, err := f.GetMulti(context.Background(), time.Minute, []string{"a", "b", "c", "d"}, func(_ context.Context, missing []string) (map[string]int, error) {
		calls.Add(1)
		gotMissing = append(gotMissing, missing...)
		res := make(map[string]int, len(missing))
		for _, k := range missing {
			res[k] = 100
		}
		return res, nil
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), calls.Load(), "fn called once")
	assert.ElementsMatch(t, []string{"b", "d"}, gotMissing, "fn receives only the missing keys")
	assert.Equal(t, map[string]int{"a": 1, "b": 100, "c": 3, "d": 100}, out, "hits and loaded values merge")

	// Loaded values are now cached.
	var ran atomic.Bool
	out2, err := f.GetMulti(context.Background(), time.Minute, []string{"b", "d"}, func(_ context.Context, _ []string) (map[string]int, error) {
		ran.Store(true)
		return nil, nil
	})
	require.NoError(t, err)
	assert.False(t, ran.Load(), "previously loaded keys are hits")
	assert.Equal(t, map[string]int{"b": 100, "d": 100}, out2)
}

func TestGetMulti_AllHitsSkipsFn(t *testing.T) {
	f := redcachetest.New[string, int]()
	require.NoError(t, f.ForceSetMulti(context.Background(), time.Minute, map[string]int{"a": 1, "b": 2}))

	var ran atomic.Bool
	out, err := f.GetMulti(context.Background(), time.Minute, []string{"a", "b"}, func(context.Context, []string) (map[string]int, error) {
		ran.Store(true)
		return nil, nil
	})
	require.NoError(t, err)
	assert.False(t, ran.Load())
	assert.Equal(t, map[string]int{"a": 1, "b": 2}, out)
}

func TestGetMulti_FnErrorPropagates(t *testing.T) {
	f := redcachetest.New[string, int]()
	wantErr := errors.New("loader down")
	_, err := f.GetMulti(context.Background(), time.Minute, []string{"a"}, func(context.Context, []string) (map[string]int, error) {
		return nil, wantErr
	})
	require.ErrorIs(t, err, wantErr)
}

func TestForceSet_Overwrites(t *testing.T) {
	f := redcachetest.New[string, int]()
	require.NoError(t, f.ForceSet(context.Background(), time.Minute, "k", 1))
	require.NoError(t, f.ForceSet(context.Background(), time.Minute, "k", 2))

	got, err := f.Get(context.Background(), time.Minute, "k", func(context.Context, string) (int, error) {
		return -1, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, got, "ForceSet should overwrite the prior value")
}

func TestSet_StoresViaFn(t *testing.T) {
	f := redcachetest.New[string, int]()
	require.NoError(t, f.Set(context.Background(), time.Minute, "k", func(context.Context, string) (int, error) {
		return 55, nil
	}))

	var ran atomic.Bool
	got, err := f.Get(context.Background(), time.Minute, "k", func(context.Context, string) (int, error) {
		ran.Store(true)
		return 0, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 55, got)
	assert.False(t, ran.Load(), "value written by Set should be a hit")
}

func TestSet_FnErrorNotStored(t *testing.T) {
	f := redcachetest.New[string, int]()
	wantErr := errors.New("nope")
	err := f.Set(context.Background(), time.Minute, "k", func(context.Context, string) (int, error) {
		return 0, wantErr
	})
	require.ErrorIs(t, err, wantErr)

	fn, calls := countingLoader(1)
	_, err = f.Get(context.Background(), time.Minute, "k", fn)
	require.NoError(t, err)
	assert.Equal(t, int64(1), calls.Load(), "failed Set must not have cached anything")
}

func TestSetMulti_StoresAllViaFn(t *testing.T) {
	f := redcachetest.New[string, int]()
	require.NoError(t, f.SetMulti(context.Background(), time.Minute, []string{"a", "b"}, func(_ context.Context, keys []string) (map[string]int, error) {
		res := make(map[string]int, len(keys))
		for i, k := range keys {
			res[k] = i + 1
		}
		return res, nil
	}))

	out, err := f.GetMulti(context.Background(), time.Minute, []string{"a", "b"}, func(context.Context, []string) (map[string]int, error) {
		t.Fatal("fn should not run; both keys should be hits")
		return nil, nil
	})
	require.NoError(t, err)
	assert.Len(t, out, 2)
}

func TestExpiry_CausesRefetch(t *testing.T) {
	clk := &redcachetest.Clock{}
	f := redcachetest.NewWithClock[string, int](clk)
	fn, calls := countingLoader(7)

	ttl := time.Minute
	got, err := f.Get(context.Background(), ttl, "k", fn)
	require.NoError(t, err)
	assert.Equal(t, 7, got)
	assert.Equal(t, int64(1), calls.Load())

	clk.Advance(ttl + time.Second) // step past the deadline; no real sleep.

	got, err = f.Get(context.Background(), ttl, "k", fn)
	require.NoError(t, err)
	assert.Equal(t, 7, got)
	assert.Equal(t, int64(2), calls.Load(), "expired entry should trigger a re-fetch")
}

func TestForceSet_TTLZeroRejected(t *testing.T) {
	f := redcachetest.New[string, int]()
	require.ErrorIs(t, f.ForceSet(context.Background(), 0, "k", 9), redcache.ErrInvalidTTL)
}

func TestInvalidTTLRejected(t *testing.T) {
	f := redcachetest.New[string, int]()
	ctx := context.Background()

	_, err := f.Get(ctx, 0, "k", func(context.Context, string) (int, error) {
		t.Fatal("Get loader must not run for invalid ttl")
		return 0, nil
	})
	require.ErrorIs(t, err, redcache.ErrInvalidTTL)
	_, err = f.GetMulti(ctx, 0, []string{"k"}, func(context.Context, []string) (map[string]int, error) {
		t.Fatal("GetMulti loader must not run for invalid ttl")
		return nil, nil
	})
	require.ErrorIs(t, err, redcache.ErrInvalidTTL)
	_, _, err = f.Peek(ctx, 0, "k")
	require.ErrorIs(t, err, redcache.ErrInvalidTTL)
	require.ErrorIs(t, f.Set(ctx, 0, "k", func(context.Context, string) (int, error) { return 1, nil }), redcache.ErrInvalidTTL)
	require.ErrorIs(t, f.SetMulti(ctx, 0, []string{"k"}, func(context.Context, []string) (map[string]int, error) {
		return map[string]int{"k": 1}, nil
	}), redcache.ErrInvalidTTL)
	require.ErrorIs(t, f.ForceSet(ctx, 0, "k", 1), redcache.ErrInvalidTTL)
	require.ErrorIs(t, f.ForceSetMulti(ctx, 0, map[string]int{"k": 1}), redcache.ErrInvalidTTL)
	require.ErrorIs(t, f.Touch(ctx, 0, "k"), redcache.ErrInvalidTTL)
	require.ErrorIs(t, f.TouchMulti(ctx, 0, []string{"k"}), redcache.ErrInvalidTTL)
}

func TestPeek_HitAfterStoreMissWhenAbsent(t *testing.T) {
	f := redcachetest.New[string, int]()

	// Absent key: miss, zero value, no error, no loader.
	v, ok, err := f.Peek(context.Background(), time.Minute, "k")
	require.NoError(t, err)
	assert.False(t, ok, "absent key is a Peek miss")
	assert.Equal(t, 0, v)

	require.NoError(t, f.ForceSet(context.Background(), time.Minute, "k", 13))

	// Present key: hit returning the stored value.
	v, ok, err = f.Peek(context.Background(), time.Minute, "k")
	require.NoError(t, err)
	assert.True(t, ok, "stored key is a Peek hit")
	assert.Equal(t, 13, v)
}

func TestPeek_ExpiredIsMiss(t *testing.T) {
	clk := &redcachetest.Clock{}
	f := redcachetest.NewWithClock[string, int](clk)

	ttl := time.Minute
	require.NoError(t, f.ForceSet(context.Background(), ttl, "k", 21))

	v, ok, err := f.Peek(context.Background(), ttl, "k")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, 21, v)

	clk.Advance(ttl + time.Second) // expire deterministically; no real sleep.

	v, ok, err = f.Peek(context.Background(), ttl, "k")
	require.NoError(t, err)
	assert.False(t, ok, "expired entry is a Peek miss")
	assert.Equal(t, 0, v)
}

func TestPeek_DoesNotMutate(t *testing.T) {
	f := redcachetest.New[string, int]()

	// Peeking an absent key must not create it: a following Get still runs fn.
	_, ok, err := f.Peek(context.Background(), time.Minute, "k")
	require.NoError(t, err)
	require.False(t, ok)

	fn, calls := countingLoader(5)
	got, err := f.Get(context.Background(), time.Minute, "k", fn)
	require.NoError(t, err)
	assert.Equal(t, 5, got)
	assert.Equal(t, int64(1), calls.Load(), "Peek must not populate the cache")
}
