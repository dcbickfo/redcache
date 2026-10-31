package redcache

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// keyOpCase is one cache operation expected to fail before reaching the core.
type keyOpCase struct {
	name string
	run  func(t *testing.T) error
}

// multiKeyCases returns the multi-key operations over keys 1 and 2, each of
// which fails the test if its callback runs after the stop described by reason.
func multiKeyCases(ctx context.Context, c *cache[int, string], reason string) []keyOpCase {
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
	cache := &cache[int, string]{
		keyCodec: KeyCodecFunc[int](func(k int) (string, error) {
			if k == 2 {
				return "", wantErr
			}
			return strconv.Itoa(k), nil
		}),
		valCodec: StringCodec{},
	}
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
				_, _, err := cache.Peek(ctx, time.Second, 2)
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

	cache := &cache[int, string]{
		keyCodec: KeyCodecFunc[int](func(int) (string, error) {
			return "same", nil
		}),
		valCodec: StringCodec{},
	}
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

	cache := &cache[int, string]{
		keyCodec: KeyCodecFunc[int](func(int) (string, error) {
			return "same", nil
		}),
		valCodec: StringCodec{},
	}

	_, err := cache.encodeMultiResult(map[int]string{
		1: "one",
		2: "two",
	})
	require.ErrorContains(t, err, "duplicate encoded key")
}
