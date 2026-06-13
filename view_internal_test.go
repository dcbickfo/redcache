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

	// An external caller passes the Cache interface value straight to View.
	sibling, err := View[string, int](base, StringKeyCodec{}, JSONCodec[int]{})
	require.NoError(t, err)

	baseCache := base.(*cache[string, string])
	siblingCache := sibling.(*cache[string, int])
	require.Same(t, baseCache.engine(), siblingCache.engine(), "view must share the parent engine")
	require.Equal(t, base.Client(), sibling.Client(), "view must share the parent client")

	// A non-redcache Cache (or any other value) is rejected, not panicked on.
	_, viewErr := View[string, int]("not a cache", StringKeyCodec{}, JSONCodec[int]{})
	require.Error(t, viewErr)

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
