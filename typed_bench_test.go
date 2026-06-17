package redcache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

var (
	benchTUser = tUser{ID: 1, Name: "alice"}

	benchTypedPrimeFn = func(_ context.Context, _ string) (tUser, error) {
		return benchTUser, nil
	}
	benchTypedPrimeMultiFn = func(_ context.Context, keys []string) (map[string]tUser, error) {
		out := make(map[string]tUser, len(keys))
		for _, k := range keys {
			out[k] = benchTUser
		}
		return out, nil
	}
	benchTypedUnreachableFn = func(_ context.Context, _ string) (tUser, error) {
		panic("callback should not run on cache hit")
	}
	benchTypedUnreachableMultiFn = func(_ context.Context, _ []string) (map[string]tUser, error) {
		panic("callback should not run on cache hit")
	}
)

func BenchmarkTypedGet(b *testing.B) {
	b.ReportAllocs()
	users := newBenchTypedJSON(b)
	ctx := context.Background()
	key := "bench:typed:get:" + uuid.NewString()

	if err := users.ForceSet(ctx, time.Minute, key, benchTUser); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for range b.N {
		if _, err := users.Get(ctx, time.Minute, key, benchTypedUnreachableFn); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedGet_Parallel(b *testing.B) {
	b.ReportAllocs()
	users := newBenchTypedJSON(b)
	ctx := context.Background()
	key := "bench:typed:get:parallel:" + uuid.NewString()

	if err := users.ForceSet(ctx, time.Minute, key, benchTUser); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	runBenchParallel(b, func(pb *testing.PB) error {
		for pb.Next() {
			if _, err := users.Get(ctx, time.Minute, key, benchTypedUnreachableFn); err != nil {
				return err
			}
		}
		return nil
	})
}

func BenchmarkTypedGetMulti(b *testing.B) {
	b.ReportAllocs()
	users := newBenchTypedJSON(b)
	ctx := context.Background()

	keys := make([]string, 10)
	seed := make(map[string]tUser, len(keys))
	for i := range keys {
		keys[i] = fmt.Sprintf("bench:typed:getmulti:%d:%s", i, uuid.NewString())
		seed[keys[i]] = benchTUser
	}
	if err := users.ForceSetMulti(ctx, time.Minute, seed); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for range b.N {
		if _, err := users.GetMulti(ctx, time.Minute, keys, benchTypedUnreachableMultiFn); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedGetMulti_Parallel(b *testing.B) {
	b.ReportAllocs()
	users := newBenchTypedJSON(b)
	ctx := context.Background()

	keys := make([]string, 10)
	seed := make(map[string]tUser, len(keys))
	for i := range keys {
		keys[i] = fmt.Sprintf("bench:typed:getmulti:parallel:%d:%s", i, uuid.NewString())
		seed[keys[i]] = benchTUser
	}
	if err := users.ForceSetMulti(ctx, time.Minute, seed); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	runBenchParallel(b, func(pb *testing.PB) error {
		for pb.Next() {
			if _, err := users.GetMulti(ctx, time.Minute, keys, benchTypedUnreachableMultiFn); err != nil {
				return err
			}
		}
		return nil
	})
}

func BenchmarkTypedSet(b *testing.B) {
	b.ReportAllocs()
	users := newBenchTypedJSON(b)
	ctx := context.Background()
	key := "bench:typed:set:" + uuid.NewString()

	b.ResetTimer()
	for range b.N {
		if err := users.Set(ctx, time.Minute, key, benchTypedPrimeFn); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedSetMulti(b *testing.B) {
	b.ReportAllocs()
	users := newBenchTypedJSON(b)
	ctx := context.Background()

	keys := make([]string, 10)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench:typed:setmulti:%d:%s", i, uuid.NewString())
	}

	b.ResetTimer()
	for range b.N {
		if err := users.SetMulti(ctx, time.Minute, keys, benchTypedPrimeMultiFn); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedForceSet(b *testing.B) {
	b.ReportAllocs()
	users := newBenchTypedJSON(b)
	ctx := context.Background()
	key := "bench:typed:forceset:" + uuid.NewString()

	b.ResetTimer()
	for range b.N {
		if err := users.ForceSet(ctx, time.Minute, key, benchTUser); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedForceSetMulti(b *testing.B) {
	b.ReportAllocs()
	users := newBenchTypedJSON(b)
	ctx := context.Background()

	values := make(map[string]tUser, 10)
	for i := range 10 {
		values[fmt.Sprintf("bench:typed:forcesetmulti:%d:%s", i, uuid.NewString())] = benchTUser
	}

	b.ResetTimer()
	for range b.N {
		if err := users.ForceSetMulti(ctx, time.Minute, values); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTypedGet_Codec isolates decode cost across codecs on the hit path.
// JSON pays json.Unmarshal; Bytes/String are zero-/single-copy.
func BenchmarkTypedGet_Codec(b *testing.B) {
	b.ReportAllocs()
	skipIfNoRedis(b)
	ctx := context.Background()
	payload := `{"id":1,"name":"alice"}`

	b.Run("json", func(b *testing.B) {
		users := newBenchBase(b)
		key := "bench:typed:codec:json:" + uuid.NewString()
		if err := users.ForceSet(ctx, time.Minute, key, benchTUser); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for range b.N {
			if _, err := users.Get(ctx, time.Minute, key, benchTypedUnreachableFn); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("bytes", func(b *testing.B) {
		conn, err := redcache.Open(rueidisOptForBench(), redcache.WithLockTTL(2*time.Second))
		if err != nil {
			b.Fatalf("new cache: %v", err)
		}
		b.Cleanup(conn.Close)
		users := redcache.NewBytes(conn)
		key := "bench:typed:codec:bytes:" + uuid.NewString()
		if err := users.ForceSet(ctx, time.Minute, key, []byte(payload)); err != nil {
			b.Fatal(err)
		}
		unreachable := func(_ context.Context, _ string) ([]byte, error) {
			panic("callback should not run on cache hit")
		}
		b.ResetTimer()
		for range b.N {
			if _, err := users.Get(ctx, time.Minute, key, unreachable); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("string", func(b *testing.B) {
		conn, err := redcache.Open(rueidisOptForBench(), redcache.WithLockTTL(2*time.Second))
		if err != nil {
			b.Fatalf("new cache: %v", err)
		}
		b.Cleanup(conn.Close)
		users := redcache.NewString[string](conn, redcache.StringCodec{})
		key := "bench:typed:codec:string:" + uuid.NewString()
		if err := users.ForceSet(ctx, time.Minute, key, payload); err != nil {
			b.Fatal(err)
		}
		unreachable := func(_ context.Context, _ string) (string, error) {
			panic("callback should not run on cache hit")
		}
		b.ResetTimer()
		for range b.N {
			if _, err := users.Get(ctx, time.Minute, key, unreachable); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func newBenchTypedJSON(b *testing.B) redcache.Cache[string, tUser] {
	b.Helper()
	return newBenchBase(b)
}

func newBenchBase(b *testing.B) redcache.Cache[string, tUser] {
	b.Helper()
	skipIfNoRedis(b)
	conn, err := redcache.Open(
		rueidisOptForBench(),
		redcache.WithLockTTL(2*time.Second),
	)
	if err != nil {
		b.Fatalf("new cache: %v", err)
	}
	b.Cleanup(conn.Close)
	c := redcache.NewString[tUser](conn, redcache.JSONCodec[tUser]{})
	return c
}

func rueidisOptForBench() rueidis.ClientOption {
	return rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}}
}
