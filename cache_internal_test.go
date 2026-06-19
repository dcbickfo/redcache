package redcache

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
	ctx := context.Background()

	tests := []struct {
		name string
		run  func(t *testing.T) error
	}{
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
			name: "get multi",
			run: func(t *testing.T) error {
				t.Helper()
				_, err := cache.GetMulti(ctx, time.Second, []int{1, 2}, func(context.Context, []int) (map[int]string, error) {
					t.Fatal("loader should not run after key encode failure")
					return nil, nil
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
			name: "set multi",
			run: func(t *testing.T) error {
				t.Helper()
				return cache.SetMulti(ctx, time.Second, []int{1, 2}, func(context.Context, []int) (map[int]string, error) {
					t.Fatal("setter should not run after key encode failure")
					return nil, nil
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
			name: "del multi",
			run: func(t *testing.T) error {
				t.Helper()
				return cache.DelMulti(ctx, []int{1, 2})
			},
		},
		{
			name: "touch",
			run: func(t *testing.T) error {
				t.Helper()
				return cache.Touch(ctx, time.Second, 2)
			},
		},
		{
			name: "touch multi",
			run: func(t *testing.T) error {
				t.Helper()
				return cache.TouchMulti(ctx, time.Second, []int{1, 2})
			},
		},
	}

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
	ctx := context.Background()

	tests := []struct {
		name string
		run  func(t *testing.T) error
	}{
		{
			name: "get multi",
			run: func(t *testing.T) error {
				t.Helper()
				_, err := cache.GetMulti(ctx, time.Second, []int{1, 2}, func(context.Context, []int) (map[int]string, error) {
					t.Fatal("loader should not run after duplicate key encoding")
					return nil, nil
				})
				return err
			},
		},
		{
			name: "set multi",
			run: func(t *testing.T) error {
				t.Helper()
				return cache.SetMulti(ctx, time.Second, []int{1, 2}, func(context.Context, []int) (map[int]string, error) {
					t.Fatal("setter should not run after duplicate key encoding")
					return nil, nil
				})
			},
		},
		{
			name: "del multi",
			run: func(t *testing.T) error {
				t.Helper()
				return cache.DelMulti(ctx, []int{1, 2})
			},
		},
		{
			name: "touch multi",
			run: func(t *testing.T) error {
				t.Helper()
				return cache.TouchMulti(ctx, time.Second, []int{1, 2})
			},
		},
	}

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
