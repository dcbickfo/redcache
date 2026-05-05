package redcache_test

import (
	"context"
	"encoding/json"
	"errors"
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
	// Write a non-JSON payload directly via the underlying client so the
	// typed Get reads garbage and surfaces ErrDecode.
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
	// Underlying json error must also be in the chain.
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
	// Wait past the original TTL — Touch should have extended it.
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
