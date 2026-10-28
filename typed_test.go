package redcache_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

type tUser struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestCache_InfersValueTypesPerOperation(t *testing.T) {
	var conn redcache.Conn
	cache := redcache.New(&conn, redcache.JSONCodec{})
	if cache == nil {
		t.Fatal("New returned nil")
	}

	// Compile-time coverage: Get infers V from each loader; Peek names V because
	// it has no value argument from which type inference could work.
	compile := func() {
		_, _ = cache.Get(t.Context(), time.Second, "int", func(context.Context, string) (int, error) {
			return 1, nil
		})
		_, _ = cache.Get(t.Context(), time.Second, "user", func(context.Context, string) (tUser, error) {
			return tUser{}, nil
		})
		_, _, _ = cache.Peek[string, tUser](t.Context(), time.Second, "user")
	}
	_ = compile
}

func TestCache_StoresDifferentValueTypesWithOneJSONCodec(t *testing.T) {
	skipIfNoRedis(t)
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.WithLockTTL(2*time.Second),
	)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(conn.Close)

	cache := redcache.New(conn, redcache.JSONCodec{})
	userKey := "codec-user:" + uuid.New().String()
	countKey := "codec-count:" + uuid.New().String()
	wantUser := tUser{ID: 7, Name: "alice"}

	if err := cache.ForceSet(t.Context(), time.Second, userKey, wantUser); err != nil {
		t.Fatalf("force set user: %v", err)
	}
	if err := cache.ForceSet(t.Context(), time.Second, countKey, 42); err != nil {
		t.Fatalf("force set count: %v", err)
	}
	gotUser, err := cache.Get(t.Context(), time.Second, userKey, func(context.Context, string) (tUser, error) {
		t.Fatal("user loader should not run")
		return tUser{}, nil
	})
	if err != nil {
		t.Fatalf("get user: %v", err)
	}
	if gotUser != wantUser {
		t.Fatalf("user = %+v, want %+v", gotUser, wantUser)
	}
	gotCount, err := cache.Get(t.Context(), time.Second, countKey, func(context.Context, string) (int, error) {
		t.Fatal("count loader should not run")
		return 0, nil
	})
	if err != nil {
		t.Fatalf("get count: %v", err)
	}
	if gotCount != 42 {
		t.Fatalf("count = %d, want 42", gotCount)
	}
}

func TestCache_ReturnsCodecTypeMismatch(t *testing.T) {
	skipIfNoRedis(t)
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.WithLockTTL(2*time.Second),
	)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(conn.Close)

	cache := redcache.New(conn, redcache.StringCodec{})
	key := "codec-mismatch:" + uuid.New().String()
	if err := cache.ForceSet(t.Context(), time.Second, key, 42); err == nil {
		t.Fatal("expected StringCodec to reject an int write")
	}
	if err := conn.Client().Do(t.Context(),
		conn.Client().B().Set().Key(key).Value("42").Px(time.Second).Build()).Error(); err != nil {
		t.Fatalf("seed string payload: %v", err)
	}

	_, err = cache.Get(t.Context(), time.Second, key, func(context.Context, string) (int, error) {
		t.Fatal("loader should not run for a cached payload")
		return 0, nil
	})
	if !errors.Is(err, redcache.ErrDecode) {
		t.Fatalf("expected codec mismatch wrapped with ErrDecode, got %v", err)
	}
}

func TestTyped_Get_LoadsAndCaches(t *testing.T) {
	users := newTypedCache(t, redcache.JSONCodec{})
	key := "u:" + uuid.New().String()

	var calls int
	loader := func(_ context.Context, _ string) (tUser, error) {
		calls++
		return tUser{ID: 1, Name: "alice"}, nil
	}

	got, err := users.Get(t.Context(), time.Second, key, loader)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	if got.ID != 1 || got.Name != "alice" {
		t.Fatalf("first get value: %+v", got)
	}
	got2, err := users.Get(t.Context(), time.Second, key, loader)
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
	skipIfNoRedis(t)
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.WithLockTTL(2*time.Second),
	)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(conn.Close)
	users := redcache.New(conn, redcache.JSONCodec{})

	// Seed garbage so the typed Get's decode call surfaces ErrDecode.
	key := "decode:" + uuid.New().String()
	if err := conn.Client().Do(t.Context(),
		conn.Client().B().Set().Key(key).Value("not json").Px(time.Second).Build()).Error(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err = users.Get(t.Context(), time.Second, key, func(context.Context, string) (tUser, error) {
		return tUser{}, errors.New("loader should not be called on decode failure of cache hit")
	})
	if !errors.Is(err, redcache.ErrDecode) {
		t.Fatalf("expected ErrDecode, got %v", err)
	}
}

func TestTyped_Get_DecodeErrorPreservesUnderlying(t *testing.T) {
	skipIfNoRedis(t)
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.WithLockTTL(2*time.Second),
	)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(conn.Close)
	users := redcache.New(conn, redcache.JSONCodec{})

	key := "decode-chain:" + uuid.New().String()
	if err := conn.Client().Do(t.Context(),
		conn.Client().B().Set().Key(key).Value("not json").Px(time.Second).Build()).Error(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, err = users.Get(t.Context(), time.Second, key,
		func(context.Context, string) (tUser, error) { return tUser{}, nil },
	)
	if err == nil {
		t.Fatal("expected decode error")
	}
	if !errors.Is(err, redcache.ErrDecode) {
		t.Fatalf("expected ErrDecode in chain, got %v", err)
	}
	if _, ok := errors.AsType[*json.SyntaxError](err); !ok {
		t.Fatalf("expected *json.SyntaxError in chain, got %v (%T)", err, err)
	}
}

func TestNewBytes_EmptyPayloadRoundTrip(t *testing.T) {
	skipIfNoRedis(t)
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.WithLockTTL(2*time.Second),
	)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(conn.Close)
	cache := redcache.NewBytes(conn)

	key := "bytes-empty:" + uuid.New().String()
	if err := cache.ForceSet(t.Context(), time.Second, key, []byte{}); err != nil {
		t.Fatalf("force set empty bytes: %v", err)
	}

	got, err := cache.Get(t.Context(), time.Second, key, func(context.Context, string) ([]byte, error) {
		t.Fatal("loader should not run for cached empty byte payload")
		return nil, nil
	})
	if err != nil {
		t.Fatalf("get empty bytes: %v", err)
	}
	if got == nil {
		t.Fatal("empty byte payload round-tripped as nil")
	}
	if len(got) != 0 {
		t.Fatalf("empty byte payload length = %d, want 0", len(got))
	}
}

func TestTyped_Del_RemovesEntry(t *testing.T) {
	users := newTypedCache(t, redcache.JSONCodec{})
	key := "del:" + uuid.New().String()

	loader := func(context.Context, string) (tUser, error) { return tUser{ID: 9, Name: "x"}, nil }
	if _, err := users.Get(t.Context(), time.Second, key, loader); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := users.Del(t.Context(), key); err != nil {
		t.Fatalf("del: %v", err)
	}
	var calls int
	wrapped := func(ctx context.Context, k string) (tUser, error) {
		calls++
		return loader(ctx, k)
	}
	if _, err := users.Get(t.Context(), time.Second, key, wrapped); err != nil {
		t.Fatalf("get after del: %v", err)
	}
	if calls != 1 {
		t.Fatalf("loader called %d times after del, want 1", calls)
	}
}

func TestTyped_Touch_ExtendsTTL(t *testing.T) {
	users := newTypedCache(t, redcache.JSONCodec{})
	key := "touch:" + uuid.New().String()

	loader := func(context.Context, string) (tUser, error) { return tUser{ID: 9, Name: "x"}, nil }
	if _, err := users.Get(t.Context(), 200*time.Millisecond, key, loader); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := users.Touch(t.Context(), 5*time.Second, key); err != nil {
		t.Fatalf("touch: %v", err)
	}
	// Sleep past the original TTL; Touch must have extended it.
	time.Sleep(400 * time.Millisecond)
	var calls int
	wrapped := func(ctx context.Context, k string) (tUser, error) {
		calls++
		return loader(ctx, k)
	}
	if _, err := users.Get(t.Context(), time.Second, key, wrapped); err != nil {
		t.Fatalf("get after touch: %v", err)
	}
	if calls != 0 {
		t.Fatalf("loader called %d times after touch, want 0 (entry should still be cached)", calls)
	}
}

func TestTyped_RefreshAhead_FiresThroughTypedView(t *testing.T) {
	skipIfNoRedis(t)
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.WithLockTTL(500*time.Millisecond),
		redcache.WithRefreshAfterFraction(0.1),
		redcache.WithRefreshWorkers(1),
		redcache.WithRefreshQueueSize(8),
	)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	t.Cleanup(conn.Close)
	users := redcache.New(conn, redcache.JSONCodec{})

	key := "refresh:" + uuid.New().String()

	var calls atomic.Int32
	loader := func(_ context.Context, _ string) (tUser, error) {
		n := calls.Add(1)
		return tUser{ID: int(n), Name: "v"}, nil
	}

	first, err := users.Get(t.Context(), 500*time.Millisecond, key, loader)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	if first.ID != 1 {
		t.Fatalf("first ID %d, want 1", first.ID)
	}
	// Cross the RefreshAfterFraction floor (0.1 * 500ms = 50ms).
	time.Sleep(150 * time.Millisecond)

	if _, err := users.Get(t.Context(), 500*time.Millisecond, key, loader); err != nil {
		t.Fatalf("trigger get: %v", err)
	}
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if calls.Load() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := calls.Load(); got < 2 {
		t.Fatalf("loader call count %d; expected refresh-ahead to have fired (>=2)", got)
	}
}

func TestTyped_GetMulti_LoadsAndCaches(t *testing.T) {
	users := newTypedCache(t, redcache.JSONCodec{})
	prefix := uuid.New().String() + ":"
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

	got, err := users.GetMulti(t.Context(), time.Second, keys, loader)
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

	got2, err := users.GetMulti(t.Context(), time.Second, keys, loader)
	if err != nil {
		t.Fatalf("second get: %v", err)
	}
	if len(got2) != 3 || calls != 1 {
		t.Fatalf("second get triggered loader (calls=%d) or short result %d", calls, len(got2))
	}
}

func TestTyped_GetMulti_IntKeys(t *testing.T) {
	skipIfNoRedis(t)
	prefix := uuid.New().String() + ":"
	codec := redcache.KeyCodecFunc[int](func(i int) (string, error) {
		return prefix + strconv.Itoa(i), nil
	})
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.WithLockTTL(2*time.Second),
	)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	t.Cleanup(conn.Close)
	users := redcache.NewKeyed(conn, codec, redcache.JSONCodec{})

	loader := func(_ context.Context, missing []int) (map[int]tUser, error) {
		out := make(map[int]tUser, len(missing))
		for _, i := range missing {
			out[i] = tUser{ID: i, Name: strconv.Itoa(i)}
		}
		return out, nil
	}
	got, err := users.GetMulti(t.Context(), time.Second, []int{10, 20, 30}, loader)
	if err != nil {
		t.Fatalf("getmulti: %v", err)
	}
	if got[10].Name != "10" || got[20].Name != "20" || got[30].Name != "30" {
		t.Fatalf("typed int keys not preserved: %+v", got)
	}
}

func TestTyped_DelMulti_RemovesAll(t *testing.T) {
	users := newTypedCache(t, redcache.JSONCodec{})
	prefix := uuid.New().String() + ":"
	keys := []string{prefix + "a", prefix + "b"}

	loader := func(_ context.Context, missing []string) (map[string]tUser, error) {
		out := make(map[string]tUser, len(missing))
		for _, k := range missing {
			out[k] = tUser{ID: 1, Name: k}
		}
		return out, nil
	}
	if _, err := users.GetMulti(t.Context(), time.Second, keys, loader); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := users.DelMulti(t.Context(), keys); err != nil {
		t.Fatalf("delmulti: %v", err)
	}
	calls := 0
	wrapped := func(ctx context.Context, missing []string) (map[string]tUser, error) {
		calls++
		return loader(ctx, missing)
	}
	if _, err := users.GetMulti(t.Context(), time.Second, keys, wrapped); err != nil {
		t.Fatalf("get after del: %v", err)
	}
	if calls != 1 {
		t.Fatalf("loader called %d times after del, want 1", calls)
	}
}

func TestTyped_TouchMulti_ExtendsTTL(t *testing.T) {
	users := newTypedCache(t, redcache.JSONCodec{})
	prefix := uuid.New().String() + ":"
	keys := []string{prefix + "a", prefix + "b"}

	loader := func(_ context.Context, missing []string) (map[string]tUser, error) {
		out := make(map[string]tUser, len(missing))
		for _, k := range missing {
			out[k] = tUser{ID: 1, Name: k}
		}
		return out, nil
	}
	if _, err := users.GetMulti(t.Context(), 200*time.Millisecond, keys, loader); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := users.TouchMulti(t.Context(), 5*time.Second, keys); err != nil {
		t.Fatalf("touchmulti: %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	calls := 0
	wrapped := func(ctx context.Context, missing []string) (map[string]tUser, error) {
		calls++
		return loader(ctx, missing)
	}
	if _, err := users.GetMulti(t.Context(), time.Second, keys, wrapped); err != nil {
		t.Fatalf("get after touch: %v", err)
	}
	if calls != 0 {
		t.Fatalf("loader called %d times after touch, want 0", calls)
	}
}
