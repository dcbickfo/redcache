package redcache_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

type tUser struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func newTestCacheAside(t *testing.T) *redcache.CacheAside {
	t.Helper()
	c, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.CacheAsideOption{LockTTL: 2 * time.Second},
	)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestTyped_Get_LoadsAndCaches(t *testing.T) {
	cache := newTestCacheAside(t)
	users := redcache.NewStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	key := "u:" + uuid.NewString()

	var calls int
	loader := func(_ context.Context, _ string) (tUser, error) {
		calls++
		return tUser{ID: 1, Name: "alice"}, nil
	}

	got, err := users.Get(context.Background(), time.Second, key, loader)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	if got.ID != 1 || got.Name != "alice" {
		t.Fatalf("first get value: %+v", got)
	}
	got2, err := users.Get(context.Background(), time.Second, key, loader)
	if err != nil {
		t.Fatalf("second get: %v", err)
	}
	if got2 != got {
		t.Fatalf("second get value: %+v", got2)
	}
	if calls != 1 {
		t.Fatalf("loader called %d times, want 1", calls)
	}
}

func TestTyped_Get_DecodeErrorIsWrapped(t *testing.T) {
	cache := newTestCacheAside(t)
	// Seed garbage so the typed Get's decode call surfaces ErrDecode.
	key := "decode:" + uuid.NewString()
	if err := cache.Client().Do(context.Background(),
		cache.Client().B().Set().Key(key).Value("not json").Px(time.Second).Build()).Error(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	users := redcache.NewStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})

	_, err := users.Get(context.Background(), time.Second, key, func(context.Context, string) (tUser, error) {
		return tUser{}, errors.New("loader should not be called on decode failure of cache hit")
	})
	if !errors.Is(err, redcache.ErrDecode) {
		t.Fatalf("expected ErrDecode, got %v", err)
	}
}

func TestTyped_Get_DecodeErrorPreservesUnderlying(t *testing.T) {
	cache := newTestCacheAside(t)
	key := "decode-chain:" + uuid.NewString()
	if err := cache.Client().Do(context.Background(),
		cache.Client().B().Set().Key(key).Value("not json").Px(time.Second).Build()).Error(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	users := redcache.NewStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	_, err := users.Get(context.Background(), time.Second, key,
		func(context.Context, string) (tUser, error) { return tUser{}, nil },
	)
	if err == nil {
		t.Fatal("expected decode error")
	}
	if !errors.Is(err, redcache.ErrDecode) {
		t.Fatalf("expected ErrDecode in chain, got %v", err)
	}
	var syntaxErr *json.SyntaxError
	if !errors.As(err, &syntaxErr) {
		t.Fatalf("expected *json.SyntaxError in chain, got %v (%T)", err, err)
	}
}

func TestTyped_Del_RemovesEntry(t *testing.T) {
	cache := newTestCacheAside(t)
	users := redcache.NewStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	key := "del:" + uuid.NewString()

	loader := func(context.Context, string) (tUser, error) { return tUser{ID: 9, Name: "x"}, nil }
	if _, err := users.Get(context.Background(), time.Second, key, loader); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := users.Del(context.Background(), key); err != nil {
		t.Fatalf("del: %v", err)
	}
	var calls int
	wrapped := func(ctx context.Context, k string) (tUser, error) {
		calls++
		return loader(ctx, k)
	}
	if _, err := users.Get(context.Background(), time.Second, key, wrapped); err != nil {
		t.Fatalf("get after del: %v", err)
	}
	if calls != 1 {
		t.Fatalf("loader called %d times after del, want 1", calls)
	}
}

func TestTyped_Touch_ExtendsTTL(t *testing.T) {
	cache := newTestCacheAside(t)
	users := redcache.NewStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	key := "touch:" + uuid.NewString()

	loader := func(context.Context, string) (tUser, error) { return tUser{ID: 9, Name: "x"}, nil }
	if _, err := users.Get(context.Background(), 200*time.Millisecond, key, loader); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := users.Touch(context.Background(), 5*time.Second, key); err != nil {
		t.Fatalf("touch: %v", err)
	}
	// Sleep past the original TTL; Touch must have extended it.
	time.Sleep(400 * time.Millisecond)
	var calls int
	wrapped := func(ctx context.Context, k string) (tUser, error) {
		calls++
		return loader(ctx, k)
	}
	if _, err := users.Get(context.Background(), time.Second, key, wrapped); err != nil {
		t.Fatalf("get after touch: %v", err)
	}
	if calls != 0 {
		t.Fatalf("loader called %d times after touch, want 0 (entry should still be cached)", calls)
	}
}

func TestTyped_RefreshAhead_FiresThroughTypedView(t *testing.T) {
	c, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.CacheAsideOption{
			LockTTL:              500 * time.Millisecond,
			RefreshAfterFraction: 0.1,
			RefreshWorkers:       1,
			RefreshQueueSize:     8,
		},
	)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	defer c.Close()

	users := redcache.NewStringTyped[tUser](c, redcache.JSONCodec[tUser]{})
	key := "refresh:" + uuid.NewString()

	var calls int32
	loader := func(_ context.Context, _ string) (tUser, error) {
		n := atomic.AddInt32(&calls, 1)
		return tUser{ID: int(n), Name: "v"}, nil
	}

	first, err := users.Get(context.Background(), 500*time.Millisecond, key, loader)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	if first.ID != 1 {
		t.Fatalf("first ID %d, want 1", first.ID)
	}
	// Cross the RefreshAfterFraction floor (0.1 * 500ms = 50ms).
	time.Sleep(150 * time.Millisecond)

	if _, err := users.Get(context.Background(), 500*time.Millisecond, key, loader); err != nil {
		t.Fatalf("trigger get: %v", err)
	}
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&calls) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&calls); got < 2 {
		t.Fatalf("loader call count %d; expected refresh-ahead to have fired (>=2)", got)
	}
}

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

func TestTyped_DelMulti_RemovesAll(t *testing.T) {
	cache := newTestCacheAside(t)
	users := redcache.NewStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	prefix := uuid.NewString() + ":"
	keys := []string{prefix + "a", prefix + "b"}

	loader := func(_ context.Context, missing []string) (map[string]tUser, error) {
		out := make(map[string]tUser, len(missing))
		for _, k := range missing {
			out[k] = tUser{ID: 1, Name: k}
		}
		return out, nil
	}
	if _, err := users.GetMulti(context.Background(), time.Second, keys, loader); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := users.DelMulti(context.Background(), keys...); err != nil {
		t.Fatalf("delmulti: %v", err)
	}
	calls := 0
	wrapped := func(ctx context.Context, missing []string) (map[string]tUser, error) {
		calls++
		return loader(ctx, missing)
	}
	if _, err := users.GetMulti(context.Background(), time.Second, keys, wrapped); err != nil {
		t.Fatalf("get after del: %v", err)
	}
	if calls != 1 {
		t.Fatalf("loader called %d times after del, want 1", calls)
	}
}

func TestTyped_TouchMulti_ExtendsTTL(t *testing.T) {
	cache := newTestCacheAside(t)
	users := redcache.NewStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	prefix := uuid.NewString() + ":"
	keys := []string{prefix + "a", prefix + "b"}

	loader := func(_ context.Context, missing []string) (map[string]tUser, error) {
		out := make(map[string]tUser, len(missing))
		for _, k := range missing {
			out[k] = tUser{ID: 1, Name: k}
		}
		return out, nil
	}
	if _, err := users.GetMulti(context.Background(), 200*time.Millisecond, keys, loader); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := users.TouchMulti(context.Background(), 5*time.Second, keys...); err != nil {
		t.Fatalf("touchmulti: %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	calls := 0
	wrapped := func(ctx context.Context, missing []string) (map[string]tUser, error) {
		calls++
		return loader(ctx, missing)
	}
	if _, err := users.GetMulti(context.Background(), time.Second, keys, wrapped); err != nil {
		t.Fatalf("get after touch: %v", err)
	}
	if calls != 0 {
		t.Fatalf("loader called %d times after touch, want 0", calls)
	}
}
