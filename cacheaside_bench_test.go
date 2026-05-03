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

// Hoisted callbacks reused across benchmarks. Defining them as package-level
// vars keeps the per-iteration loop free of closure-allocation noise that
// would otherwise dominate hot-path measurements.
var (
	benchValue        = "bench-value"
	benchPrimeFn      = func(ctx context.Context, key string) (string, error) { return benchValue, nil }
	benchPrimeMultiFn = func(ctx context.Context, keys []string) (map[string]string, error) {
		out := make(map[string]string, len(keys))
		for _, k := range keys {
			out[k] = benchValue
		}
		return out, nil
	}
	benchUnreachableFn      = func(ctx context.Context, key string) (string, error) { panic("callback should not run on cache hit") }
	benchUnreachableMultiFn = func(ctx context.Context, keys []string) (map[string]string, error) {
		panic("callback should not run on cache hit")
	}
)

// BenchmarkCacheAside_Get measures hot-path performance for a single cached key.
func BenchmarkCacheAside_Get(b *testing.B) {
	client := makeBenchClient(b)
	defer client.Client().Close()
	ctx := context.Background()
	key := "bench:get:" + uuid.New().String()

	if _, err := client.Get(ctx, time.Minute, key, benchPrimeFn); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for range b.N {
		if _, err := client.Get(ctx, time.Minute, key, benchUnreachableFn); err != nil {
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

	if _, err := client.Get(ctx, time.Minute, key, benchPrimeFn); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := client.Get(ctx, time.Minute, key, benchUnreachableFn); err != nil {
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

	if _, err := client.GetMulti(ctx, time.Minute, keys, benchPrimeMultiFn); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for range b.N {
		if _, err := client.GetMulti(ctx, time.Minute, keys, benchUnreachableMultiFn); err != nil {
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

	if _, err := client.GetMulti(ctx, time.Minute, keys, benchPrimeMultiFn); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := client.GetMulti(ctx, time.Minute, keys, benchUnreachableMultiFn); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkCacheAside_Del measures the single-key delete path.
func BenchmarkCacheAside_Del(b *testing.B) {
	client := makeBenchClient(b)
	defer client.Client().Close()
	ctx := context.Background()
	key := "bench:del:" + uuid.New().String()

	b.ResetTimer()
	for range b.N {
		if err := client.Del(ctx, key); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCacheAside_DelMulti measures the multi-key delete path with N=10 keys.
func BenchmarkCacheAside_DelMulti(b *testing.B) {
	client := makeBenchClient(b)
	defer client.Client().Close()
	ctx := context.Background()

	keys := make([]string, 10)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench:delmulti:%d:%s", i, uuid.New().String())
	}

	b.ResetTimer()
	for range b.N {
		if err := client.DelMulti(ctx, keys...); err != nil {
			b.Fatal(err)
		}
	}
}

func makePrimeableBenchClient(b *testing.B) *redcache.PrimeableCacheAside {
	b.Helper()
	client, err := redcache.NewPrimeableCacheAside(
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

// BenchmarkPrimeable_Set measures the single-key Set hot path.
func BenchmarkPrimeable_Set(b *testing.B) {
	client := makePrimeableBenchClient(b)
	defer client.Client().Close()
	ctx := context.Background()
	key := "bench:set:" + uuid.New().String()

	b.ResetTimer()
	for range b.N {
		if err := client.Set(ctx, time.Minute, key, benchPrimeFn); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPrimeable_SetMulti measures multi-key Set with N=10 keys.
func BenchmarkPrimeable_SetMulti(b *testing.B) {
	client := makePrimeableBenchClient(b)
	defer client.Client().Close()
	ctx := context.Background()

	keys := make([]string, 10)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench:setmulti:%d:%s", i, uuid.New().String())
	}

	b.ResetTimer()
	for range b.N {
		if err := client.SetMulti(ctx, time.Minute, keys, benchPrimeMultiFn); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPrimeable_ForceSet measures the unconditional ForceSet path.
func BenchmarkPrimeable_ForceSet(b *testing.B) {
	client := makePrimeableBenchClient(b)
	defer client.Client().Close()
	ctx := context.Background()
	key := "bench:forceset:" + uuid.New().String()

	b.ResetTimer()
	for range b.N {
		if err := client.ForceSet(ctx, time.Minute, key, "bench-value"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPrimeable_ForceSetMulti measures unconditional multi-key writes.
func BenchmarkPrimeable_ForceSetMulti(b *testing.B) {
	client := makePrimeableBenchClient(b)
	defer client.Client().Close()
	ctx := context.Background()

	values := make(map[string]string, 10)
	for i := range 10 {
		key := fmt.Sprintf("bench:forcesetmulti:%d:%s", i, uuid.New().String())
		values[key] = "bench-value"
	}

	b.ResetTimer()
	for range b.N {
		if err := client.ForceSetMulti(ctx, time.Minute, values); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCacheAside_Get_Refresh measures the refresh-ahead-triggering path.
// The cache is primed with a short TTL so the per-iteration Get crosses the
// refresh threshold and enqueues a background job.
func BenchmarkCacheAside_Get_Refresh(b *testing.B) {
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.CacheAsideOption{
			LockTTL:              5 * time.Second,
			RefreshAfterFraction: 0.001, // any TTL elapsed → trigger refresh
			RefreshWorkers:       8,
			RefreshQueueSize:     1024,
		},
	)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Client().Close()
	ctx := context.Background()
	key := "bench:get:refresh:" + uuid.New().String()
	const val = "bench-value"

	// Hoist the callback out of the loop so we measure the refresh-ahead path
	// itself, not the per-iteration closure allocation a captured-variable
	// callback would produce.
	cb := func(ctx context.Context, key string) (string, error) { return val, nil }

	if _, err := client.Get(ctx, time.Minute, key, cb); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for range b.N {
		if _, err := client.Get(ctx, time.Minute, key, cb); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCacheAside_GetMulti_Refresh measures the refresh-ahead-triggering
// multi-key path. RefreshAfterFraction is set so every iteration crosses the
// threshold and enqueues a background refresh job for each key.
func BenchmarkCacheAside_GetMulti_Refresh(b *testing.B) {
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.CacheAsideOption{
			LockTTL:              5 * time.Second,
			RefreshAfterFraction: 0.001, // any TTL elapsed → trigger refresh
			RefreshWorkers:       8,
			RefreshQueueSize:     1024,
		},
	)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Client().Close()
	ctx := context.Background()

	keys := make([]string, 10)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench:getmulti:refresh:%d:%s", i, uuid.New().String())
	}
	cb := func(ctx context.Context, ks []string) (map[string]string, error) {
		out := make(map[string]string, len(ks))
		for _, k := range ks {
			out[k] = "bench-value"
		}
		return out, nil
	}

	if _, err := client.GetMulti(ctx, time.Minute, keys, cb); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for range b.N {
		if _, err := client.GetMulti(ctx, time.Minute, keys, cb); err != nil {
			b.Fatal(err)
		}
	}
}
