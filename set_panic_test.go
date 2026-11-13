package redcache_test

import (
	"context"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A Set callback that panics, with the panic recovered by the caller, must
// roll the key back: the prior value is readable at once, not a lock, and the
// panic still propagates.

func TestSet_PanickingCallbackRestoresPriorValue(t *testing.T) {
	t.Parallel()
	client, conn := makeClient(t, addr)
	ctx := t.Context()
	key := "set-panic:" + uuid.New().String()
	require.NoError(t, client.ForceSet(ctx, time.Minute, key, "prior"))

	assert.PanicsWithValue(t, "boom", func() {
		_ = client.Set(ctx, time.Minute, key, func(context.Context, string) (string, error) {
			panic("boom")
		})
	})

	raw, err := conn.Client().Do(ctx, conn.Client().B().Get().Key(key).Build()).ToString()
	require.NoError(t, err)
	assert.Equal(t, "__redcache:v1:0:prior", raw, "prior value must be back, not a lock")
	ttl, err := conn.Client().Do(ctx, conn.Client().B().Pttl().Key(key).Build()).AsInt64()
	require.NoError(t, err)
	assert.Positive(t, ttl)
}

func TestSet_PanickingCallbackOnAbsentKeyRemovesLock(t *testing.T) {
	t.Parallel()
	client, conn := makeClient(t, addr)
	ctx := t.Context()
	key := "set-panic-absent:" + uuid.New().String()

	assert.Panics(t, func() {
		_ = client.Set(ctx, time.Minute, key, func(context.Context, string) (string, error) {
			panic("boom")
		})
	})

	n, err := conn.Client().Do(ctx, conn.Client().B().Exists().Key(key).Build()).AsInt64()
	require.NoError(t, err)
	assert.Zero(t, n, "no lock may be left behind")
}

func TestSetMulti_PanickingCallbackRestoresPriorValues(t *testing.T) {
	t.Parallel()
	client, conn := makeClient(t, addr)
	ctx := t.Context()
	id := uuid.New().String()
	keys := []string{"setm-panic:a:" + id, "setm-panic:b:" + id, "setm-panic:c:" + id}
	require.NoError(t, client.ForceSet(ctx, time.Minute, keys[0], "prior-a"))
	require.NoError(t, client.ForceSet(ctx, time.Minute, keys[1], "prior-b"))
	// keys[2] has no prior value.

	assert.PanicsWithValue(t, "boom", func() {
		_ = client.SetMulti(ctx, time.Minute, keys, func(context.Context, []string) (map[string]string, error) {
			panic("boom")
		})
	})

	for i, want := range []string{"__redcache:v1:0:prior-a", "__redcache:v1:0:prior-b"} {
		raw, err := conn.Client().Do(ctx, conn.Client().B().Get().Key(keys[i]).Build()).ToString()
		require.NoError(t, err)
		assert.Equal(t, want, raw)
	}
	n, err := conn.Client().Do(ctx, conn.Client().B().Exists().Key(keys[2]).Build()).AsInt64()
	require.NoError(t, err)
	assert.Zero(t, n, "key without a prior value must be deleted, not left locked")
}
