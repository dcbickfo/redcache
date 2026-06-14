package redcache_test

import (
	"context"
	"testing"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

// Of derives a view over an existing Conn and panics on a nil codec, so a nil
// keyCodec or valCodec fails fast at derivation rather than on the first
// hot-path call. Deriving needs a live Conn, so this needs Redis.
func TestOf_NilCodecPanics(t *testing.T) {
	t.Parallel()
	skipIfNoRedis(t)
	opt := rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}}

	conn, err := redcache.Open(opt)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	require.Panics(t, func() { redcache.Of[string, int](conn, nil, redcache.JSONCodec[int]{}) },
		"Of must panic on a nil keyCodec")
}

// Write methods reject a non-positive ttl with ErrInvalidTTL instead of leaking
// the raw Redis "PX 0" error.
func TestWrite_InvalidTTLRejected(t *testing.T) {
	t.Parallel()
	cache, _ := makeClient(t, []string{"127.0.0.1:6379"})
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
