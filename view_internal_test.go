package redcache

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// View derives a sibling typed cache that shares the parent's engine (one
// client, one invalidation stream). This test asserts the share is real: both
// views observe the same underlying *cacheAside and the same rueidis.Client.
func TestView_SharesEngine(t *testing.T) {
	t.Parallel()

	base, err := New[string, string](
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		StringKeyCodec{},
		StringCodec{},
		WithLockTTL(2*time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(base.Close)

	// A concrete *cache satisfies interface{ engine() *cacheAside }.
	parent, ok := base.(*cache[string, string])
	require.True(t, ok, "New should return *cache[string, string]")

	sibling := View[string, int](parent, StringKeyCodec{}, JSONCodec[int]{})
	siblingCache, ok := sibling.(*cache[string, int])
	require.True(t, ok)

	require.Same(t, parent.engine(), siblingCache.engine(), "view must share the parent engine")
	require.Equal(t, parent.Client(), sibling.Client(), "view must share the parent client")

	// Both views are usable over the shared engine.
	ctx := context.Background()
	sKey := "view:str:" + uuid.NewString()
	iKey := "view:int:" + uuid.NewString()

	gotS, err := base.Get(ctx, time.Second, sKey, func(context.Context, string) (string, error) {
		return "hello", nil
	})
	require.NoError(t, err)
	require.Equal(t, "hello", gotS)

	gotI, err := sibling.Get(ctx, time.Second, iKey, func(context.Context, string) (int, error) {
		return 42, nil
	})
	require.NoError(t, err)
	require.Equal(t, 42, gotI)
}
