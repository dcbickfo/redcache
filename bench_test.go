package redcache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

// =============================================================================
// Benchmark Helpers
// =============================================================================

func makeBenchClient(b *testing.B, addr []string) *redcache.CacheAside {
	b.Helper()
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{
			InitAddress: addr,
		},
		redcache.CacheAsideOption{
			LockTTL: time.Second,
		},
	)
	if err != nil {
		b.Fatal(err)
	}
	return client
}

func makeBenchClientWithSet(b *testing.B, addr []string) *redcache.PrimeableCacheAside {
	b.Helper()
	client, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{
			InitAddress: addr,
		},
		redcache.CacheAsideOption{
			LockTTL: time.Second,
		},
	)
	if err != nil {
		b.Fatal(err)
	}
	return client
}

// =============================================================================
// Basic Operation Benchmarks
// =============================================================================

// BenchmarkCacheAside_Get benchmarks single key cache operations.
func BenchmarkCacheAside_Get(b *testing.B) {
	client := makeBenchClient(b, addr)
	defer client.Client().Close()

	ctx := context.Background()
	key := "bench:get:" + uuid.New().String()

	b.Run("CacheHit", func(b *testing.B) {
		// Pre-populate cache
		_, err := client.Get(ctx, time.Minute, key, func(_ context.Context, _ string) (string, error) {
			return "cached-value", nil
		})
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, getErr := client.Get(ctx, time.Minute, key, func(_ context.Context, _ string) (string, error) {
				b.Fatal("callback should not be called on cache hit")
				return "", nil
			})
			if getErr != nil {
				b.Fatal(getErr)
			}
		}
	})

	b.Run("CacheMiss", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			uniqueKey := fmt.Sprintf("bench:miss:%d:%s", i, uuid.New().String())
			_, err := client.Get(ctx, time.Minute, uniqueKey, func(_ context.Context, _ string) (string, error) {
				return "new-value", nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkCacheAside_GetMulti benchmarks batch cache operations.
func BenchmarkCacheAside_GetMulti(b *testing.B) {
	client := makeBenchClient(b, addr)
	defer client.Client().Close()

	ctx := context.Background()

	sizes := []int{10, 50, 100}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d_AllHits", size), func(b *testing.B) {
			// Pre-populate cache
			keys := make([]string, size)
			values := make(map[string]string, size)
			for i := 0; i < size; i++ {
				key := fmt.Sprintf("bench:multi:hit:%d:%s", i, uuid.New().String())
				keys[i] = key
				values[key] = fmt.Sprintf("value-%d", i)
			}

			_, err := client.GetMulti(ctx, time.Minute, keys, func(_ context.Context, _ []string) (map[string]string, error) {
				return values, nil
			})
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, getErr := client.GetMulti(ctx, time.Minute, keys, func(_ context.Context, _ []string) (map[string]string, error) {
					b.Fatal("callback should not be called on cache hit")
					return nil, nil
				})
				if getErr != nil {
					b.Fatal(getErr)
				}
			}
		})

		b.Run(fmt.Sprintf("Size%d_AllMisses", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				keys := make([]string, size)
				values := make(map[string]string, size)
				for j := 0; j < size; j++ {
					key := fmt.Sprintf("bench:multi:miss:%d:%d:%s", i, j, uuid.New().String())
					keys[j] = key
					values[key] = fmt.Sprintf("value-%d", j)
				}

				_, err := client.GetMulti(ctx, time.Minute, keys, func(_ context.Context, _ []string) (map[string]string, error) {
					return values, nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCacheAside_GetMulti_PartialHits benchmarks GetMulti with varying cache hit rates.
// This represents realistic production scenarios where some keys are cached and others need to be fetched.
func BenchmarkCacheAside_GetMulti_PartialHits(b *testing.B) {
	client := makeBenchClient(b, addr)
	defer client.Client().Close()

	ctx := context.Background()
	size := 100

	hitRates := []struct {
		name    string
		percent int
	}{
		{"25PercentCached", 25},
		{"50PercentCached", 50},
		{"75PercentCached", 75},
	}

	for _, hr := range hitRates {
		b.Run(hr.name, func(b *testing.B) {
			// Pre-populate a percentage of keys
			allKeys := make([]string, size)
			allValues := make(map[string]string, size)
			cachedKeys := make([]string, 0, size*hr.percent/100)

			for i := 0; i < size; i++ {
				key := fmt.Sprintf("bench:partial:%s:%d:%s", hr.name, i, uuid.New().String())
				allKeys[i] = key
				allValues[key] = fmt.Sprintf("value-%d", i)

				// Cache only the specified percentage
				if i < size*hr.percent/100 {
					cachedKeys = append(cachedKeys, key)
				}
			}

			// Pre-populate the cached keys
			if len(cachedKeys) > 0 {
				cachedValues := make(map[string]string, len(cachedKeys))
				for _, k := range cachedKeys {
					cachedValues[k] = allValues[k]
				}
				_, err := client.GetMulti(ctx, time.Minute, cachedKeys, func(_ context.Context, keys []string) (map[string]string, error) {
					return cachedValues, nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, getErr := client.GetMulti(ctx, time.Minute, allKeys, func(_ context.Context, missedKeys []string) (map[string]string, error) {
					result := make(map[string]string, len(missedKeys))
					for _, k := range missedKeys {
						result[k] = allValues[k]
					}
					return result, nil
				})
				if getErr != nil {
					b.Fatal(getErr)
				}
			}
		})
	}
}

// BenchmarkPrimeableCacheAside_Set benchmarks coordinated cache update operations.
func BenchmarkPrimeableCacheAside_Set(b *testing.B) {
	client := makeBenchClientWithSet(b, addr)
	defer client.Client().Close()

	ctx := context.Background()

	b.Run("NewKey", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench:set:%d:%s", i, uuid.New().String())
			err := client.Set(ctx, time.Minute, key, func(_ context.Context, _ string) (string, error) {
				return "value", nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("OverwriteExisting", func(b *testing.B) {
		key := "bench:set:overwrite:" + uuid.New().String()
		// Pre-populate
		err := client.Set(ctx, time.Minute, key, func(_ context.Context, _ string) (string, error) {
			return "initial", nil
		})
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			setErr := client.Set(ctx, time.Minute, key, func(_ context.Context, _ string) (string, error) {
				return fmt.Sprintf("value-%d", i), nil
			})
			if setErr != nil {
				b.Fatal(setErr)
			}
		}
	})
}

// BenchmarkSet_vs_ForceSet compares Set (with locking) vs ForceSet (lock bypass).
// This helps users understand the performance trade-off between safety and speed.
func BenchmarkSet_vs_ForceSet(b *testing.B) {
	client := makeBenchClientWithSet(b, addr)
	defer client.Client().Close()

	ctx := context.Background()

	b.Run("Set_WithLocking", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench:comparison:set:%d", i)
			err := client.Set(ctx, time.Minute, key, func(_ context.Context, _ string) (string, error) {
				return "value", nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ForceSet_NoLocking", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench:comparison:force:%d", i)
			err := client.ForceSet(ctx, time.Minute, key, "value")
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Set_Overwrite_ExistingValue", func(b *testing.B) {
		key := "bench:comparison:set:existing"
		// Pre-populate
		err := client.ForceSet(ctx, time.Minute, key, "initial")
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			setErr := client.Set(ctx, time.Minute, key, func(_ context.Context, _ string) (string, error) {
				return "value", nil
			})
			if setErr != nil {
				b.Fatal(setErr)
			}
		}
	})

	b.Run("ForceSet_Overwrite_ExistingValue", func(b *testing.B) {
		key := "bench:comparison:force:existing"
		// Pre-populate
		err := client.ForceSet(ctx, time.Minute, key, "initial")
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			forceErr := client.ForceSet(ctx, time.Minute, key, "value")
			if forceErr != nil {
				b.Fatal(forceErr)
			}
		}
	})
}

// BenchmarkPrimeableCacheAside_SetMulti benchmarks batch write operations.
func BenchmarkPrimeableCacheAside_SetMulti(b *testing.B) {
	client := makeBenchClientWithSet(b, addr)
	defer client.Client().Close()

	ctx := context.Background()

	sizes := []int{10, 50, 100}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				keys := make([]string, size)
				for j := 0; j < size; j++ {
					keys[j] = fmt.Sprintf("bench:setmulti:%d:%d:%s", i, j, uuid.New().String())
				}

				_, err := client.SetMulti(ctx, time.Minute, keys, func(_ context.Context, lockedKeys []string) (map[string]string, error) {
					result := make(map[string]string, len(lockedKeys))
					for _, key := range lockedKeys {
						result[key] = "value"
					}
					return result, nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkLockAcquisition benchmarks the lock acquisition overhead.
func BenchmarkLockAcquisition(b *testing.B) {
	client := makeBenchClientWithSet(b, addr)
	defer client.Client().Close()

	ctx := context.Background()

	b.Run("NoContention", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench:lock:%d:%s", i, uuid.New().String())
			err := client.Set(ctx, time.Millisecond*100, key, func(_ context.Context, _ string) (string, error) {
				return "value", nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkDelOperations benchmarks delete operations.
func BenchmarkDelOperations(b *testing.B) {
	client := makeBenchClient(b, addr)
	defer client.Client().Close()

	ctx := context.Background()

	b.Run("Del_Single", func(b *testing.B) {
		// Pre-populate keys
		keys := make([]string, b.N)
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench:del:%d:%s", i, uuid.New().String())
			keys[i] = key
			_, err := client.Get(ctx, time.Minute, key, func(_ context.Context, _ string) (string, error) {
				return "value", nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			err := client.Del(ctx, keys[i])
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("DelMulti_Batch10", func(b *testing.B) {
		// Pre-populate keys
		allKeys := make([][]string, b.N)
		for i := 0; i < b.N; i++ {
			batch := make([]string, 10)
			values := make(map[string]string, 10)
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("bench:delmulti:%d:%d:%s", i, j, uuid.New().String())
				batch[j] = key
				values[key] = "value"
			}
			allKeys[i] = batch

			_, err := client.GetMulti(ctx, time.Minute, batch, func(_ context.Context, _ []string) (map[string]string, error) {
				return values, nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			err := client.DelMulti(ctx, allKeys[i]...)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// =============================================================================
// Optimization Benchmarks
// =============================================================================

// BenchmarkSmallBatchOptimization benchmarks PrimeableCacheAside multi-operations
// which use fast path for small batches (< 10 keys)
func BenchmarkSmallBatchOptimization(b *testing.B) {
	ctx := context.Background()
	pca, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{LockTTL: time.Second},
	)
	require.NoError(b, err)
	defer pca.Close()

	sizes := []int{3, 5, 10, 20, 50}

	for _, size := range sizes {
		// Prepare test data
		values := make(map[string]string, size)
		keys := make([]string, 0, size)
		for i := 0; i < size; i++ {
			key := fmt.Sprintf("bench:opt:%d:%s", i, uuid.New().String())
			keys = append(keys, key)
			values[key] = fmt.Sprintf("value-%d", i)
		}

		b.Run(fmt.Sprintf("ForceSetMulti_Size%d", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				setErr := pca.ForceSetMulti(ctx, time.Minute, values)
				require.NoError(b, setErr)
			}
		})

		b.Run(fmt.Sprintf("GetMulti_Size%d", size), func(b *testing.B) {
			// Pre-populate
			_ = pca.ForceSetMulti(ctx, time.Minute, values)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, getErr := pca.GetMulti(ctx, time.Minute, keys, func(ctx context.Context, missingKeys []string) (map[string]string, error) {
					result := make(map[string]string)
					for _, k := range missingKeys {
						result[k] = values[k]
					}
					return result, nil
				})
				require.NoError(b, getErr)
			}
		})
	}
}

// BenchmarkLockAcquisitionMethods compares different lock acquisition strategies
func BenchmarkLockAcquisitionMethods(b *testing.B) {
	ctx := context.Background()
	ca, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{LockTTL: time.Second},
	)
	require.NoError(b, err)
	defer ca.Client().Close()

	b.Run("SingleLock", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench:lock:single:%d", i)
			_, getErr := ca.Get(ctx, time.Minute, key, func(ctx context.Context, key string) (string, error) {
				return "value", nil
			})
			require.NoError(b, getErr)
		}
	})

	b.Run("BatchLock_10Keys", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			keys := make([]string, 10)
			for j := 0; j < 10; j++ {
				keys[j] = fmt.Sprintf("bench:lock:batch10:%d:%d", i, j)
			}

			_, getErr := ca.GetMulti(ctx, time.Minute, keys, func(ctx context.Context, keys []string) (map[string]string, error) {
				result := make(map[string]string, len(keys))
				for _, k := range keys {
					result[k] = "value"
				}
				return result, nil
			})
			require.NoError(b, getErr)
		}
	})

	b.Run("BatchLock_50Keys", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			keys := make([]string, 50)
			for j := 0; j < 50; j++ {
				keys[j] = fmt.Sprintf("bench:lock:batch50:%d:%d", i, j)
			}

			_, getErr := ca.GetMulti(ctx, time.Minute, keys, func(ctx context.Context, keys []string) (map[string]string, error) {
				result := make(map[string]string, len(keys))
				for _, k := range keys {
					result[k] = "value"
				}
				return result, nil
			})
			require.NoError(b, getErr)
		}
	})
}

// BenchmarkContextCreation measures the overhead of context creation
func BenchmarkContextCreation(b *testing.B) {
	baseCtx := context.Background()

	b.Run("DirectContextWithTimeout", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ctx, cancel := context.WithTimeout(baseCtx, 10*time.Second)
			cancel()
			_ = ctx
		}
	})

	b.Run("LazyContextCreation", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Simulate lazy creation - only create if needed
			var ctx context.Context
			var cancel context.CancelFunc
			needsContext := false // Simulate cache hit scenario

			if needsContext {
				ctx, cancel = context.WithTimeout(baseCtx, 10*time.Second)
				cancel()
			}
			_ = ctx
		}
	})
}

// BenchmarkSlotBatching compares different batching strategies
func BenchmarkSlotBatching(b *testing.B) {
	ctx := context.Background()
	pca, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{LockTTL: time.Second},
	)
	require.NoError(b, err)
	defer pca.Close()

	sizes := []int{10, 50, 100, 500}

	for _, size := range sizes {
		values := make(map[string]string, size)
		for i := 0; i < size; i++ {
			// Use keys that will distribute across slots
			key := fmt.Sprintf("{slot%d}:key:%d", i%16, i)
			values[key] = fmt.Sprintf("value-%d", i)
		}

		b.Run(fmt.Sprintf("ForceSetMulti_%dKeys", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				setErr := pca.ForceSetMulti(ctx, time.Minute, values)
				require.NoError(b, setErr)
			}
		})
	}
}

// BenchmarkConcurrentOperations measures performance under high concurrency
func BenchmarkConcurrentOperations(b *testing.B) {
	ctx := context.Background()
	ca, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{LockTTL: time.Second},
	)
	require.NoError(b, err)
	defer ca.Client().Close()

	concurrencyLevels := []int{10, 50, 100}

	for _, level := range concurrencyLevels {
		b.Run(fmt.Sprintf("Get_Concurrency%d", level), func(b *testing.B) {
			b.SetParallelism(level)
			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					key := fmt.Sprintf("bench:concurrent:%d:%d", level, i%100)
					_, _ = ca.Get(ctx, time.Minute, key, func(ctx context.Context, key string) (string, error) {
						return "value", nil
					})
					i++
				}
			})
		})
	}
}

// BenchmarkCacheHitVsMiss compares performance of cache hits vs misses
func BenchmarkCacheHitVsMiss(b *testing.B) {
	ctx := context.Background()
	ca, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{LockTTL: time.Second},
	)
	require.NoError(b, err)
	defer ca.Client().Close()

	// Pre-populate for cache hit test
	hitKey := "bench:hit:" + uuid.New().String()
	_, err = ca.Get(ctx, time.Minute, hitKey, func(ctx context.Context, key string) (string, error) {
		return "cached-value", nil
	})
	require.NoError(b, err)

	b.Run("CacheHit_FastPath", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			val, getErr := ca.Get(ctx, time.Minute, hitKey, func(ctx context.Context, key string) (string, error) {
				b.Fatal("callback should not be called on cache hit")
				return "", nil
			})
			require.NoError(b, getErr)
			require.NotEmpty(b, val)
		}
	})

	b.Run("CacheMiss_FullPath", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			missKey := fmt.Sprintf("bench:miss:%d:%s", i, uuid.New().String())
			val, getErr := ca.Get(ctx, time.Minute, missKey, func(ctx context.Context, key string) (string, error) {
				return "computed-value", nil
			})
			require.NoError(b, getErr)
			require.NotEmpty(b, val)
		}
	})
}

// BenchmarkWriteCoordination measures write-write coordination overhead
func BenchmarkWriteCoordination(b *testing.B) {
	ctx := context.Background()
	pca, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: addr},
		redcache.CacheAsideOption{LockTTL: time.Second},
	)
	require.NoError(b, err)
	defer pca.Close()

	b.Run("Set_NoContention", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench:write:nocontention:%d", i)
			setErr := pca.Set(ctx, time.Minute, key, func(ctx context.Context, key string) (string, error) {
				return "value", nil
			})
			require.NoError(b, setErr)
		}
	})

	b.Run("ForceSet_NoLocking", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench:write:force:%d", i)
			setErr := pca.ForceSet(ctx, time.Minute, key, "value")
			require.NoError(b, setErr)
		}
	})
}

// =============================================================================
// Allocation Profiling Benchmarks
// =============================================================================

// BenchmarkSetMulti_Allocations focuses on allocation hotspots in SetMulti
func BenchmarkSetMulti_Allocations(b *testing.B) {
	client := makeBenchClientWithSet(b, addr)
	defer client.Close()

	sizes := []int{10, 50, 100}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			// Pre-generate keys and values to isolate SetMulti performance
			keys := make([]string, size)
			values := make(map[string]string, size)
			for i := 0; i < size; i++ {
				key := fmt.Sprintf("bench_key_%d", i)
				keys[i] = key
				values[key] = fmt.Sprintf("value_%d", i)
			}

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := client.SetMulti(ctx, time.Minute, keys, func(_ context.Context, lockedKeys []string) (map[string]string, error) {
					return values, nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGetMulti_Allocations focuses on allocation hotspots in GetMulti
func BenchmarkGetMulti_Allocations(b *testing.B) {
	client := makeBenchClientWithSet(b, addr)
	defer client.Close()

	sizes := []int{10, 50, 100}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			// Pre-populate cache
			keys := make([]string, size)
			values := make(map[string]string, size)
			for i := 0; i < size; i++ {
				key := fmt.Sprintf("bench_get_key_%d", i)
				keys[i] = key
				values[key] = fmt.Sprintf("value_%d", i)
			}

			ctx := context.Background()
			err := client.ForceSetMulti(ctx, time.Minute, values)
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, getErr := client.GetMulti(ctx, time.Minute, keys, func(_ context.Context, missedKeys []string) (map[string]string, error) {
					b.Fatal("should all be cached")
					return nil, nil
				})
				if getErr != nil {
					b.Fatal(getErr)
				}
			}
		})
	}
}

// BenchmarkWriteLock_Allocations focuses on lock acquisition allocations
func BenchmarkWriteLock_Allocations(b *testing.B) {
	client := makeBenchClientWithSet(b, addr)
	defer client.Close()

	ctx := context.Background()

	b.Run("single_key", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("lock_key_%d", i)
			err := client.Set(ctx, time.Second, key, func(_ context.Context, _ string) (string, error) {
				return "value", nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("multi_key_10", func(b *testing.B) {
		keys := make([]string, 10)
		values := make(map[string]string, 10)
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("multi_lock_%d", i)
			keys[i] = key
			values[key] = fmt.Sprintf("value_%d", i)
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := client.SetMulti(ctx, time.Second, keys, func(_ context.Context, lockedKeys []string) (map[string]string, error) {
				return values, nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSliceOperations focuses on slice allocation patterns
func BenchmarkSliceOperations(b *testing.B) {
	sizes := []int{10, 50, 100, 500}

	b.Run("mapKeys_extraction", func(b *testing.B) {
		for _, size := range sizes {
			m := make(map[string]string, size)
			for i := 0; i < size; i++ {
				m[fmt.Sprintf("key_%d", i)] = fmt.Sprintf("value_%d", i)
			}

			b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					keys := make([]string, 0, len(m))
					for k := range m {
						keys = append(keys, k)
					}
					_ = keys
				}
			})
		}
	})

	b.Run("slice_filtering", func(b *testing.B) {
		for _, size := range sizes {
			slice := make([]string, size)
			exclude := make(map[string]bool, size/2)
			for i := 0; i < size; i++ {
				slice[i] = fmt.Sprintf("key_%d", i)
				if i%2 == 0 {
					exclude[slice[i]] = true
				}
			}

			b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					result := make([]string, 0, len(slice))
					for _, item := range slice {
						if !exclude[item] {
							result = append(result, item)
						}
					}
					_ = result
				}
			})
		}
	})
}

// BenchmarkUUIDGeneration focuses on UUID generation patterns
func BenchmarkUUIDGeneration(b *testing.B) {
	b.Run("single", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = uuid.New().String()
		}
	})

	b.Run("batch_10", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			ids := make([]string, 10)
			for j := 0; j < 10; j++ {
				ids[j] = uuid.New().String()
			}
			_ = ids
		}
	})

	b.Run("batch_100", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			ids := make([]string, 100)
			for j := 0; j < 100; j++ {
				ids[j] = uuid.New().String()
			}
			_ = ids
		}
	})
}

// BenchmarkMapOperations focuses on map allocation patterns
func BenchmarkMapOperations(b *testing.B) {
	sizes := []int{10, 50, 100}

	b.Run("map_creation_with_capacity", func(b *testing.B) {
		for _, size := range sizes {
			b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					m := make(map[string]string, size)
					for j := 0; j < size; j++ {
						m[fmt.Sprintf("key_%d", j)] = fmt.Sprintf("value_%d", j)
					}
					_ = m
				}
			})
		}
	})

	b.Run("map_creation_without_capacity", func(b *testing.B) {
		for _, size := range sizes {
			b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					m := make(map[string]string)
					for j := 0; j < size; j++ {
						m[fmt.Sprintf("key_%d", j)] = fmt.Sprintf("value_%d", j)
					}
					_ = m
				}
			})
		}
	})
}

// BenchmarkStringBuilding focuses on string concatenation patterns
func BenchmarkStringBuilding(b *testing.B) {
	const prefix = "redcache:writelock:"
	const key = "user:1000"

	b.Run("concatenation", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = prefix + key
		}
	})

	b.Run("sprintf", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = fmt.Sprintf("%s%s", prefix, key)
		}
	})
}

// BenchmarkContextOperations focuses on context creation patterns
func BenchmarkContextOperations(b *testing.B) {
	b.Run("background_context", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			ctx := context.Background()
			_ = ctx
		}
	})

	b.Run("with_cancel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_ = ctx
		}
	})

	b.Run("with_timeout", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			cancel()
			_ = ctx
		}
	})
}

// BenchmarkTickerOperations focuses on ticker usage patterns
func BenchmarkTickerOperations(b *testing.B) {
	b.Run("ticker_create_stop", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			ticker := time.NewTicker(time.Second)
			ticker.Stop()
		}
	})
}

// BenchmarkSlotGrouping benchmarks the hash slot grouping operation
func BenchmarkSlotGrouping(b *testing.B) {
	sizes := []int{10, 50, 100}

	for _, size := range sizes {
		keys := make([]string, size)
		for i := 0; i < size; i++ {
			// Mix of keys that will go to different slots
			keys[i] = fmt.Sprintf("key_%d", i)
		}

		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Simulate groupBySlot operation
				slotGroups := make(map[uint16][]string)
				for _, key := range keys {
					slot := uint16(i % 16384) // Simplified hash
					slotGroups[slot] = append(slotGroups[slot], key)
				}
				_ = slotGroups
			}
		})
	}
}
