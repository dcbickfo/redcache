package redcache_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

type accountID string

type orderID int64

func (o orderID) MarshalText() ([]byte, error) {
	return []byte("order:" + strconv.FormatInt(int64(o), 10)), nil
}

// One Cache serves string, ~string, and TextMarshaler keys with the default key
// codec, and each key type may carry a different value type.
func TestCache_PerOperationKeyTypes(t *testing.T) {
	t.Parallel()
	_, conn := makeClient(t, addr)
	cache := redcache.New(conn, redcache.JSONCodec{})
	ctx := t.Context()
	suffix := uuid.New().String()

	s, err := cache.Get(ctx, time.Minute, "plain:"+suffix,
		func(context.Context, string) (string, error) { return "v1", nil })
	require.NoError(t, err)
	require.Equal(t, "v1", s)

	a := accountID("acct:" + suffix)
	n, err := cache.Get(ctx, time.Minute, a,
		func(_ context.Context, got accountID) (int, error) {
			require.Equal(t, a, got)
			return 7, nil
		})
	require.NoError(t, err)
	require.Equal(t, 7, n)
	// The ~string key is stored under its raw string value.
	raw, err := conn.Client().Do(ctx, conn.Client().B().Get().Key(string(a)).Build()).ToString()
	require.NoError(t, err)
	require.NotEmpty(t, raw)

	o := orderID(42)
	got, err := cache.Get(ctx, time.Minute, o,
		func(context.Context, orderID) (accountID, error) { return "owner", nil })
	require.NoError(t, err)
	require.Equal(t, accountID("owner"), got)
	raw, err = conn.Client().Do(ctx, conn.Client().B().Get().Key("order:42").Build()).ToString()
	require.NoError(t, err)
	require.NotEmpty(t, raw)
	require.NoError(t, cache.Del(ctx, o))

	// Multi-key ops on a ~string key type take the alias fast path and round-trip.
	keys := []accountID{accountID("m1:" + suffix), accountID("m2:" + suffix)}
	res, err := cache.GetMulti(ctx, time.Minute, keys,
		func(_ context.Context, ks []accountID) (map[accountID]int, error) {
			out := make(map[accountID]int, len(ks))
			for _, k := range ks {
				out[k] = len(k)
			}
			return out, nil
		})
	require.NoError(t, err)
	require.Equal(t, map[accountID]int{keys[0]: len(keys[0]), keys[1]: len(keys[1])}, res)
	require.NoError(t, cache.DelMulti(ctx, keys))
}

func TestCache_DefaultKeyCodecRejectsUnsupportedKey(t *testing.T) {
	t.Parallel()
	cache, _ := makeClient(t, addr)
	_, err := cache.Get(t.Context(), time.Minute, 12345,
		func(context.Context, int) (string, error) { return "", nil })
	require.Error(t, err)
	require.ErrorContains(t, err, "StringKeyCodec cannot encode key of type int")
}

func TestKeyCodecFunc_RejectsOtherKeyTypes(t *testing.T) {
	t.Parallel()
	codec := redcache.KeyCodecFunc[int](func(i int) (string, error) { return strconv.Itoa(i), nil })
	_, err := codec.EncodeKey("not an int")
	require.Error(t, err)
	require.ErrorContains(t, err, "cannot encode key of type string")
}

func TestStringKeyCodec_Kinds(t *testing.T) {
	t.Parallel()
	c := redcache.StringKeyCodec{}
	for _, tc := range []struct {
		in   any
		want string
	}{
		{"s", "s"},
		{accountID("a"), "a"},
		{orderID(3), "order:3"},
	} {
		got, err := c.EncodeKey(tc.in)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
	_, err := c.EncodeKey(3.5)
	require.ErrorContains(t, err, "cannot encode key of type float64")
}

// keyOpCase is one cache operation expected to fail before reaching the core.
type keyOpCase struct {
	name string
	run  func(t *testing.T) error
}

// multiKeyCases returns the multi-key operations over keys 1 and 2, each of
// which fails the test if its callback runs after the stop described by reason.
func multiKeyCases(ctx context.Context, c *redcache.Cache, reason string) []keyOpCase {
	return []keyOpCase{
		{
			name: "get multi",
			run: func(t *testing.T) error {
				t.Helper()
				_, err := c.GetMulti(ctx, time.Second, []int{1, 2}, func(context.Context, []int) (map[int]string, error) {
					t.Fatal("loader should not run after " + reason)
					return nil, nil
				})
				return err
			},
		},
		{
			name: "set multi",
			run: func(t *testing.T) error {
				t.Helper()
				return c.SetMulti(ctx, time.Second, []int{1, 2}, func(context.Context, []int) (map[int]string, error) {
					t.Fatal("setter should not run after " + reason)
					return nil, nil
				})
			},
		},
		{
			name: "del multi",
			run: func(t *testing.T) error {
				t.Helper()
				return c.DelMulti(ctx, []int{1, 2})
			},
		},
		{
			name: "touch multi",
			run: func(t *testing.T) error {
				t.Helper()
				return c.TouchMulti(ctx, time.Second, []int{1, 2})
			},
		},
	}
}

func TestCache_KeyEncodeErrorStopsBeforeCore(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("bad key")
	cache := redcache.NewKeyed(new(redcache.Conn), redcache.KeyCodecFunc[int](func(k int) (string, error) {
		if k == 2 {
			return "", wantErr
		}
		return strconv.Itoa(k), nil
	}), redcache.StringCodec{})
	ctx := t.Context()

	tests := append(multiKeyCases(ctx, cache, "key encode failure"), []keyOpCase{
		{
			name: "get",
			run: func(t *testing.T) error {
				t.Helper()
				_, err := cache.Get(ctx, time.Second, 2, func(context.Context, int) (string, error) {
					t.Fatal("loader should not run after key encode failure")
					return "", nil
				})
				return err
			},
		},
		{
			name: "set",
			run: func(t *testing.T) error {
				t.Helper()
				return cache.Set(ctx, time.Second, 2, func(context.Context, int) (string, error) {
					t.Fatal("setter should not run after key encode failure")
					return "", nil
				})
			},
		},
		{
			name: "peek",
			run: func(t *testing.T) error {
				t.Helper()
				_, _, err := cache.Peek[int, string](ctx, time.Second, 2)
				return err
			},
		},
		{
			name: "del",
			run: func(t *testing.T) error {
				t.Helper()
				return cache.Del(ctx, 2)
			},
		},
		{
			name: "touch",
			run: func(t *testing.T) error {
				t.Helper()
				return cache.Touch(ctx, time.Second, 2)
			},
		},
	}...)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, tt.run(t), wantErr)
		})
	}
}

func TestCache_DuplicateEncodedKeysStopBeforeCore(t *testing.T) {
	t.Parallel()

	cache := redcache.NewKeyed(new(redcache.Conn), redcache.KeyCodecFunc[int](func(int) (string, error) {
		return "same", nil
	}), redcache.StringCodec{})
	ctx := t.Context()

	tests := multiKeyCases(ctx, cache, "duplicate key encoding")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorContains(t, tt.run(t), "duplicate encoded key")
		})
	}
}

func TestCache_EncodeMultiResultRejectsDuplicateEncodedKeys(t *testing.T) {
	t.Parallel()

	cache := redcache.NewKeyed(redcache.OpenMemory(), redcache.KeyCodecFunc[int](func(int) (string, error) {
		return "same", nil
	}), redcache.StringCodec{})

	err := cache.SetMulti(t.Context(), time.Second, []int{1}, func(context.Context, []int) (map[int]string, error) {
		return map[int]string{1: "one", 2: "two"}, nil
	})
	require.ErrorContains(t, err, "duplicate encoded key")
}
