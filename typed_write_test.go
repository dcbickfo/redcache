package redcache_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

func newTypedCache[V any](t *testing.T, valCodec redcache.Codec[V]) redcache.Cache[string, V] {
	t.Helper()
	skipIfNoRedis(t)
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.WithLockTTL(2*time.Second),
	)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(conn.Close)
	return redcache.NewString[V](conn, valCodec)
}

func TestTyped_Set_PopulatesAndCaches(t *testing.T) {
	users := newTypedCache[tUser](t, redcache.JSONCodec[tUser]{})
	key := "set:" + uuid.NewString()

	if err := users.Set(context.Background(), time.Second, key,
		func(context.Context, string) (tUser, error) { return tUser{ID: 1, Name: "a"}, nil },
	); err != nil {
		t.Fatalf("set: %v", err)
	}

	got, err := users.Get(context.Background(), time.Second, key,
		func(context.Context, string) (tUser, error) {
			t.Fatal("loader should not run after Set")
			return tUser{}, nil
		},
	)
	if err != nil {
		t.Fatalf("get after set: %v", err)
	}
	if got.ID != 1 || got.Name != "a" {
		t.Fatalf("got %+v", got)
	}
}

func TestTyped_ForceSet_OverwritesUnconditionally(t *testing.T) {
	users := newTypedCache[tUser](t, redcache.JSONCodec[tUser]{})
	key := "force:" + uuid.NewString()

	if err := users.ForceSet(context.Background(), time.Second, key, tUser{ID: 1, Name: "a"}); err != nil {
		t.Fatalf("force set 1: %v", err)
	}
	if err := users.ForceSet(context.Background(), time.Second, key, tUser{ID: 2, Name: "b"}); err != nil {
		t.Fatalf("force set 2: %v", err)
	}

	got, err := users.Get(context.Background(), time.Second, key,
		func(context.Context, string) (tUser, error) { return tUser{}, errors.New("nope") },
	)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != 2 || got.Name != "b" {
		t.Fatalf("got %+v, want overwritten value", got)
	}
}

func TestTyped_Set_EncodeFailureReleasesLock(t *testing.T) {
	users := newTypedCache[badEncode](t, badEncodeCodec{})
	key := "encfail:" + uuid.NewString()

	want := errors.New("nope")
	err := users.Set(context.Background(), time.Second, key,
		func(context.Context, string) (badEncode, error) { return badEncode{err: want}, nil },
	)
	if !errors.Is(err, want) {
		t.Fatalf("expected encode error, got %v", err)
	}

	// Lock must be released — a follow-up ForceSet should succeed immediately.
	if err := users.ForceSet(context.Background(), time.Second, key, badEncode{}); err != nil {
		t.Fatalf("force set after encode failure: %v", err)
	}
}

type badEncode struct{ err error }
type badEncodeCodec struct{}

func (badEncodeCodec) Encode(b badEncode) ([]byte, error) {
	if b.err != nil {
		return nil, b.err
	}
	return []byte("ok"), nil
}
func (badEncodeCodec) Decode(b []byte) (badEncode, error) { return badEncode{}, nil }

type maybeString struct {
	val string
	err error
}

type maybeStringCodec struct{}

func (maybeStringCodec) Encode(v maybeString) ([]byte, error) {
	if v.err != nil {
		return nil, v.err
	}
	return []byte(v.val), nil
}

func (maybeStringCodec) Decode(b []byte) (maybeString, error) {
	return maybeString{val: string(b)}, nil
}

func newIntKeyCache[V any](t *testing.T, valCodec redcache.Codec[V]) redcache.Cache[int, V] {
	t.Helper()
	skipIfNoRedis(t)
	prefix := uuid.NewString() + ":"
	codec := redcache.KeyCodecFunc[int](func(i int) (string, error) {
		return prefix + strconv.Itoa(i), nil
	})
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.WithLockTTL(2*time.Second),
	)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(conn.Close)
	return redcache.New[int, V](conn, codec, valCodec)
}

func TestTyped_SetMulti_PopulatesAll(t *testing.T) {
	users := newTypedCache[tUser](t, redcache.JSONCodec[tUser]{})
	prefix := uuid.NewString() + ":"
	keys := []string{prefix + "a", prefix + "b"}

	if err := users.SetMulti(context.Background(), time.Second, keys,
		func(_ context.Context, keys []string) (map[string]tUser, error) {
			out := make(map[string]tUser, len(keys))
			for i, k := range keys {
				out[k] = tUser{ID: i, Name: k}
			}
			return out, nil
		},
	); err != nil {
		t.Fatalf("setmulti: %v", err)
	}

	got, err := users.GetMulti(context.Background(), time.Second, keys,
		func(context.Context, []string) (map[string]tUser, error) {
			t.Fatal("loader should not run after SetMulti")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatalf("get after setmulti: %v", err)
	}
	if len(got) != 2 || got[keys[0]].Name != keys[0] || got[keys[1]].Name != keys[1] {
		t.Fatalf("got %+v", got)
	}
}

func TestTyped_SetMulti_IntKeys_PopulatesAll(t *testing.T) {
	users := newIntKeyCache[tUser](t, redcache.JSONCodec[tUser]{})
	keys := []int{101, 202}

	if err := users.SetMulti(context.Background(), time.Second, keys,
		func(_ context.Context, keys []int) (map[int]tUser, error) {
			out := make(map[int]tUser, len(keys))
			for _, k := range keys {
				out[k] = tUser{ID: k, Name: strconv.Itoa(k)}
			}
			return out, nil
		},
	); err != nil {
		t.Fatalf("setmulti int keys: %v", err)
	}

	got, err := users.GetMulti(context.Background(), time.Second, keys,
		func(context.Context, []int) (map[int]tUser, error) {
			t.Fatal("loader should not run after SetMulti with int keys")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatalf("get after int-key setmulti: %v", err)
	}
	if got[101].Name != "101" || got[202].Name != "202" {
		t.Fatalf("got %+v", got)
	}
}

// TestTyped_SetMulti_BatchKeyError_Surfaces verifies the typed
// wrapper converts *batchError to *BatchKeyError[string] on partial CAS failure.
func TestTyped_SetMulti_BatchKeyError_Surfaces(t *testing.T) {
	users := newTypedCache[tUser](t, redcache.JSONCodec[tUser]{})
	prefix := uuid.NewString() + ":"
	keys := []string{prefix + "a", prefix + "b"}

	err := users.SetMulti(context.Background(), time.Second, keys,
		func(_ context.Context, gotKeys []string) (map[string]tUser, error) {
			// Steal the lock on keys[1] before our CAS-set runs.
			if serr := users.ForceSet(context.Background(), time.Second, keys[1], tUser{Name: "stolen"}); serr != nil {
				t.Fatalf("steal force set: %v", serr)
			}
			out := make(map[string]tUser, len(gotKeys))
			for _, k := range gotKeys {
				out[k] = tUser{ID: 1, Name: k}
			}
			return out, nil
		},
	)
	if err == nil {
		t.Fatal("expected partial failure")
	}
	var bke *redcache.BatchKeyError[string]
	if !errors.As(err, &bke) {
		t.Fatalf("expected *BatchKeyError[string], got %T: %v", err, err)
	}
	if !bke.HasFailures() {
		t.Fatalf("expected failures, got %+v", bke)
	}
	if !bke.HasError(keys[1]) {
		t.Fatalf("expected failure for stolen key %q; got %+v", keys[1], bke.Failed)
	}
}

func TestTyped_SetMulti_IntKeys_BatchKeyErrorPreservesTypedKey(t *testing.T) {
	users := newIntKeyCache[tUser](t, redcache.JSONCodec[tUser]{})
	keys := []int{1, 2}

	err := users.SetMulti(context.Background(), time.Second, keys,
		func(_ context.Context, gotKeys []int) (map[int]tUser, error) {
			if serr := users.ForceSet(context.Background(), time.Second, 2, tUser{Name: "stolen"}); serr != nil {
				t.Fatalf("steal force set: %v", serr)
			}
			out := make(map[int]tUser, len(gotKeys))
			for _, k := range gotKeys {
				out[k] = tUser{ID: k, Name: strconv.Itoa(k)}
			}
			return out, nil
		},
	)
	if err == nil {
		t.Fatal("expected partial failure")
	}
	var bke *redcache.BatchKeyError[int]
	if !errors.As(err, &bke) {
		t.Fatalf("expected *BatchKeyError[int], got %T: %v", err, err)
	}
	if !bke.HasError(2) {
		t.Fatalf("expected int key 2 to fail; got %+v", bke.Failed)
	}
	if !errors.Is(bke.ErrorFor(2), redcache.ErrLockLost) {
		t.Fatalf("key 2 error = %v, want ErrLockLost", bke.ErrorFor(2))
	}
}

func TestTyped_ForceSetMulti_OverwritesAll(t *testing.T) {
	users := newTypedCache[tUser](t, redcache.JSONCodec[tUser]{})
	prefix := uuid.NewString() + ":"
	in := map[string]tUser{
		prefix + "a": {ID: 1, Name: "a"},
		prefix + "b": {ID: 2, Name: "b"},
	}

	if err := users.ForceSetMulti(context.Background(), time.Second, in); err != nil {
		t.Fatalf("force set multi: %v", err)
	}

	keys := []string{prefix + "a", prefix + "b"}
	got, err := users.GetMulti(context.Background(), time.Second, keys,
		func(context.Context, []string) (map[string]tUser, error) {
			t.Fatal("loader should not run after ForceSetMulti")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatalf("get after force set multi: %v", err)
	}
	if len(got) != 2 || got[prefix+"a"].ID != 1 || got[prefix+"b"].ID != 2 {
		t.Fatalf("got %+v", got)
	}
}

func TestTyped_ForceSetMulti_IntKeys_PartialEncodeFailure(t *testing.T) {
	cache := newIntKeyCache[maybeString](t, maybeStringCodec{})
	wantErr := errors.New("encode failed")

	err := cache.ForceSetMulti(context.Background(), time.Second, map[int]maybeString{
		1: {val: "one"},
		2: {err: wantErr},
	})
	if err == nil {
		t.Fatal("expected partial encode failure")
	}
	var bke *redcache.BatchKeyError[int]
	if !errors.As(err, &bke) {
		t.Fatalf("expected *BatchKeyError[int], got %T: %v", err, err)
	}
	if !errors.Is(bke.ErrorFor(2), wantErr) {
		t.Fatalf("key 2 error = %v, want %v", bke.ErrorFor(2), wantErr)
	}
	if bke.HasError(1) {
		t.Fatalf("key 1 should have succeeded; failures: %+v", bke.Failed)
	}

	got, err := cache.Get(context.Background(), time.Second, 1, func(context.Context, int) (maybeString, error) {
		t.Fatal("loader should not run for successfully encoded key")
		return maybeString{}, nil
	})
	if err != nil {
		t.Fatalf("get successful key after partial ForceSetMulti: %v", err)
	}
	if got.val != "one" {
		t.Fatalf("key 1 value = %q, want one", got.val)
	}
}

func TestTyped_ForceSetMulti_IntKeys_PartialKeyEncodeFailure(t *testing.T) {
	skipIfNoRedis(t)
	wantErr := errors.New("key encode failed")
	prefix := uuid.NewString() + ":"
	codec := redcache.KeyCodecFunc[int](func(i int) (string, error) {
		if i == 2 {
			return "", wantErr
		}
		return prefix + strconv.Itoa(i), nil
	})
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.WithLockTTL(2*time.Second),
	)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(conn.Close)
	cache := redcache.New[int, string](conn, codec, redcache.StringCodec{})

	err = cache.ForceSetMulti(context.Background(), time.Second, map[int]string{
		1: "one",
		2: "two",
	})
	if err == nil {
		t.Fatal("expected partial key encode failure")
	}
	var bke *redcache.BatchKeyError[int]
	if !errors.As(err, &bke) {
		t.Fatalf("expected *BatchKeyError[int], got %T: %v", err, err)
	}
	if !errors.Is(bke.ErrorFor(2), wantErr) {
		t.Fatalf("key 2 error = %v, want %v", bke.ErrorFor(2), wantErr)
	}
	if bke.HasError(1) {
		t.Fatalf("key 1 should have succeeded; failures: %+v", bke.Failed)
	}

	got, err := cache.Get(context.Background(), time.Second, 1, func(context.Context, int) (string, error) {
		t.Fatal("loader should not run for successfully encoded key")
		return "", nil
	})
	if err != nil {
		t.Fatalf("get successful key after partial ForceSetMulti: %v", err)
	}
	if got != "one" {
		t.Fatalf("key 1 value = %q, want one", got)
	}
}

func TestTyped_ForceSetMulti_IntKeys_DuplicateEncodedKeyFailure(t *testing.T) {
	skipIfNoRedis(t)
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.WithLockTTL(2*time.Second),
	)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(conn.Close)
	encoded := "typed-collision:" + uuid.NewString()
	cache := redcache.New[int, string](conn, redcache.KeyCodecFunc[int](func(int) (string, error) {
		return encoded, nil
	}), redcache.StringCodec{})

	err = cache.ForceSetMulti(context.Background(), time.Second, map[int]string{
		1: "one",
		2: "two",
	})
	if err == nil {
		t.Fatal("expected duplicate encoded key failure")
	}
	var bke *redcache.BatchKeyError[int]
	if !errors.As(err, &bke) {
		t.Fatalf("expected *BatchKeyError[int], got %T: %v", err, err)
	}
	if !bke.HasFailures() {
		t.Fatalf("expected duplicate encoded key failure, got %+v", bke)
	}
	if !bke.HasError(1) || !bke.HasError(2) {
		t.Fatalf("both colliding keys should fail; got %+v", bke.Failed)
	}
	if len(bke.Succeeded) != 0 {
		t.Fatalf("duplicate collision should abort before any key succeeds; got %+v", bke.Succeeded)
	}

	_, err = conn.Client().Do(context.Background(), conn.Client().B().Get().Key(encoded).Build()).ToString()
	if !rueidis.IsRedisNil(err) {
		t.Fatalf("colliding Redis key was written; err=%v", err)
	}
}
