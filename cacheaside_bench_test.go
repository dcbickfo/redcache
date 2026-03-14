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

func makeBenchClient(b *testing.B) *redcache.CacheAside {
	b.Helper()
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{
			InitAddress: []string{"127.0.0.1:6379"},
		},
		redcache.CacheAsideOption{
			LockTTL: 5 * time.Second,
		},
	)
	if err != nil {
		b.Fatal(err)
	}
	return client
}

// BenchmarkCacheAside_Get measures hot-path performance for a single cached key.
func BenchmarkCacheAside_Get(b *testing.B) {
	client := makeBenchClient(b)
	defer client.Client().Close()
	ctx := context.Background()
	key := "bench:get:" + uuid.New().String()
	val := "bench-value"

	// Prime the cache.
	_, err := client.Get(ctx, time.Minute, key, func(ctx context.Context, key string) (string, error) {
		return val, nil
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for range b.N {
		_, err := client.Get(ctx, time.Minute, key, func(ctx context.Context, key string) (string, error) {
			b.Fatal("callback should not be called on cache hit")
			return "", nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCacheAside_Get_Parallel measures hot-path performance under contention.
func BenchmarkCacheAside_Get_Parallel(b *testing.B) {
	client := makeBenchClient(b)
	defer client.Client().Close()
	ctx := context.Background()
	key := "bench:get:parallel:" + uuid.New().String()
	val := "bench-value"

	// Prime the cache.
	_, err := client.Get(ctx, time.Minute, key, func(ctx context.Context, key string) (string, error) {
		return val, nil
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := client.Get(ctx, time.Minute, key, func(ctx context.Context, key string) (string, error) {
				b.Fatal("callback should not be called on cache hit")
				return "", nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkCacheAside_GetMulti measures hot-path performance for multiple cached keys.
func BenchmarkCacheAside_GetMulti(b *testing.B) {
	client := makeBenchClient(b)
	defer client.Client().Close()
	ctx := context.Background()

	keys := make([]string, 10)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench:getmulti:%d:%s", i, uuid.New().String())
	}

	// Prime the cache.
	_, err := client.GetMulti(ctx, time.Minute, keys, func(ctx context.Context, keys []string) (map[string]string, error) {
		result := make(map[string]string, len(keys))
		for _, k := range keys {
			result[k] = "bench-value"
		}
		return result, nil
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for range b.N {
		_, err := client.GetMulti(ctx, time.Minute, keys, func(ctx context.Context, keys []string) (map[string]string, error) {
			b.Fatal("callback should not be called on cache hit")
			return nil, nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCacheAside_GetMulti_Parallel measures hot-path multi-key performance under contention.
func BenchmarkCacheAside_GetMulti_Parallel(b *testing.B) {
	client := makeBenchClient(b)
	defer client.Client().Close()
	ctx := context.Background()

	keys := make([]string, 10)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench:getmulti:parallel:%d:%s", i, uuid.New().String())
	}

	// Prime the cache.
	_, err := client.GetMulti(ctx, time.Minute, keys, func(ctx context.Context, keys []string) (map[string]string, error) {
		result := make(map[string]string, len(keys))
		for _, k := range keys {
			result[k] = "bench-value"
		}
		return result, nil
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := client.GetMulti(ctx, time.Minute, keys, func(ctx context.Context, keys []string) (map[string]string, error) {
				b.Fatal("callback should not be called on cache hit")
				return nil, nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
