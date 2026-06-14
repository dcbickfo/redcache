package redcache_test

import (
	"context"
	"errors"
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
