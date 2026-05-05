package redcache_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

func BenchmarkTypedGet(b *testing.B) {
	cache := newBenchPrimeable(b)
	users := redcache.NewPrimeableStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	key := "bench:" + uuid.NewString()

	if err := users.ForceSet(context.Background(), time.Minute, key, tUser{ID: 1, Name: "alice"}); err != nil {
		b.Fatalf("seed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := users.Get(context.Background(), time.Minute, key,
			func(context.Context, string) (tUser, error) { return tUser{}, nil },
		); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkTypedGetMulti(b *testing.B) {
	cache := newBenchPrimeable(b)
	users := redcache.NewPrimeableStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	prefix := uuid.NewString() + ":"
	keys := []string{prefix + "a", prefix + "b", prefix + "c", prefix + "d", prefix + "e"}

	for _, k := range keys {
		if err := users.ForceSet(context.Background(), time.Minute, k, tUser{ID: 1, Name: k}); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := users.GetMulti(context.Background(), time.Minute, keys,
			func(context.Context, []string) (map[string]tUser, error) { return nil, nil },
		); err != nil {
			b.Fatalf("getmulti: %v", err)
		}
	}
}

func newBenchPrimeable(b *testing.B) *redcache.PrimeableCacheAside {
	b.Helper()
	c, err := redcache.NewPrimeableCacheAside(
		rueidisOptForBench(),
		redcache.CacheAsideOption{LockTTL: 2 * time.Second},
	)
	if err != nil {
		b.Fatalf("new primeable: %v", err)
	}
	b.Cleanup(c.Close)
	return c
}

func rueidisOptForBench() rueidis.ClientOption {
	return rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}}
}
