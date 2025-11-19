package lockpool

import (
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestLockValuePool_Get(t *testing.T) {
	t.Run("returns values with correct prefix", func(t *testing.T) {
		pool := New("test:", 100)

		val := pool.Get()
		assert.True(t, strings.HasPrefix(val, "test:"))
		assert.Greater(t, len(val), len("test:"))
	})

	t.Run("returns unique values", func(t *testing.T) {
		pool := New("test:", 100)

		seen := make(map[string]bool)
		for i := 0; i < 100; i++ {
			val := pool.Get()
			assert.False(t, seen[val], "value should be unique: %s", val)
			seen[val] = true
		}
	})

	t.Run("generates valid values continuously", func(t *testing.T) {
		pool := New("test:", 10)

		// sync.Pool doesn't have a fixed size, so just verify it continues
		// generating valid values indefinitely
		for i := 0; i < 100; i++ {
			val := pool.Get()
			assert.True(t, strings.HasPrefix(val, "test:"))
			assert.Greater(t, len(val), len("test:"))
		}
	})

	t.Run("is safe for concurrent use", func(t *testing.T) {
		pool := New("test:", 1000)

		var wg sync.WaitGroup
		seen := sync.Map{}
		const goroutines = 100
		const valuesPerGoroutine = 100

		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < valuesPerGoroutine; j++ {
					val := pool.Get()
					_, loaded := seen.LoadOrStore(val, true)
					assert.False(t, loaded, "concurrent access produced duplicate: %s", val)
				}
			}()
		}

		wg.Wait()

		// Should have gotten 10,000 unique values
		count := 0
		seen.Range(func(key, value any) bool {
			count++
			return true
		})
		assert.Equal(t, goroutines*valuesPerGoroutine, count)
	})
}

func BenchmarkLockValuePool_Get(b *testing.B) {
	pool := New("__redcache:lock:", 10000)

	b.Run("Sequential", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = pool.Get()
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = pool.Get()
			}
		})
	})
}

func BenchmarkLockValueGeneration(b *testing.B) {
	b.Run("DirectUUID", func(b *testing.B) {
		prefix := "__redcache:lock:"
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			id, _ := uuid.NewV7()
			_ = prefix + id.String()
		}
	})

	b.Run("PooledValues", func(b *testing.B) {
		pool := New("__redcache:lock:", 10000)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = pool.Get()
		}
	})
}
