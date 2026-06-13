package redcache_test

import (
	"context"
	"testing"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

// New and View reject nil codecs at construction rather than panicking on the
// first hot-path call. The nil check runs before any client is built, so this
// needs no Redis.
func TestNew_NilCodecRejected(t *testing.T) {
	t.Parallel()
	opt := rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}}

	_, err := redcache.New[string, string](opt, nil, redcache.StringCodec{})
	require.Error(t, err)

	_, err = redcache.New[string, string](opt, redcache.StringKeyCodec{}, nil)
	require.Error(t, err)
}

// Write methods reject a non-positive ttl with ErrInvalidTTL instead of leaking
// the raw Redis "PX 0" error.
func TestWrite_InvalidTTLRejected(t *testing.T) {
	t.Parallel()
	cache := makeClient(t, []string{"127.0.0.1:6379"})
	defer cache.Close()
	ctx := context.Background()

	require.ErrorIs(t, cache.ForceSet(ctx, 0, "k", "v"), redcache.ErrInvalidTTL)
	require.ErrorIs(t, cache.Set(ctx, -1, "k",
		func(context.Context, string) (string, error) { return "v", nil }),
		redcache.ErrInvalidTTL)
	require.ErrorIs(t, cache.ForceSetMulti(ctx, 0, map[string]string{"k": "v"}), redcache.ErrInvalidTTL)
	require.ErrorIs(t, cache.SetMulti(ctx, 0, []string{"k"},
		func(context.Context, []string) (map[string]string, error) { return nil, nil }),
		redcache.ErrInvalidTTL)
}
