package redcache_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dcbickfo/redcache"
)

func TestTyped_GetMulti_LoadsAndCaches(t *testing.T) {
	cache := newTestCacheAside(t)
	users := redcache.NewStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	prefix := uuid.NewString() + ":"
	keys := []string{prefix + "a", prefix + "b", prefix + "c"}

	var calls int
	loader := func(_ context.Context, missing []string) (map[string]tUser, error) {
		calls++
		out := make(map[string]tUser, len(missing))
		for i, k := range missing {
			out[k] = tUser{ID: i + 1, Name: k}
		}
		return out, nil
	}

	got, err := users.GetMulti(context.Background(), time.Second, keys, loader)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("first get len %d, want 3", len(got))
	}
	for _, k := range keys {
		u, ok := got[k]
		if !ok || u.Name != k {
			t.Fatalf("first get key %q: got %+v ok=%v", k, u, ok)
		}
	}

	got2, err := users.GetMulti(context.Background(), time.Second, keys, loader)
	if err != nil {
		t.Fatalf("second get: %v", err)
	}
	if len(got2) != 3 || calls != 1 {
		t.Fatalf("second get triggered loader (calls=%d) or short result %d", calls, len(got2))
	}
}

func TestTyped_GetMulti_IntKeys(t *testing.T) {
	cache := newTestCacheAside(t)
	prefix := uuid.NewString() + ":"
	codec := redcache.KeyCodecFunc[int](func(i int) (string, error) {
		return prefix + strconv.Itoa(i), nil
	})
	users := redcache.NewTyped[int, tUser](cache, codec, redcache.JSONCodec[tUser]{})

	loader := func(_ context.Context, missing []int) (map[int]tUser, error) {
		out := make(map[int]tUser, len(missing))
		for _, i := range missing {
			out[i] = tUser{ID: i, Name: strconv.Itoa(i)}
		}
		return out, nil
	}
	got, err := users.GetMulti(context.Background(), time.Second, []int{10, 20, 30}, loader)
	if err != nil {
		t.Fatalf("getmulti: %v", err)
	}
	if got[10].Name != "10" || got[20].Name != "20" || got[30].Name != "30" {
		t.Fatalf("typed int keys not preserved: %+v", got)
	}
}
