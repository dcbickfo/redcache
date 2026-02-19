package lockpool_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache/internal/lockpool"
)

func TestPool_Generate_Prefix(t *testing.T) {
	prefix := "__redcache:lock:"
	pool, err := lockpool.New(prefix)
	require.NoError(t, err)

	val := pool.Generate()
	assert.True(t, strings.HasPrefix(val, prefix), "expected prefix %q, got %q", prefix, val)
}

func TestPool_Generate_Uniqueness(t *testing.T) {
	pool, err := lockpool.New("lock:")
	require.NoError(t, err)

	seen := make(map[string]struct{})
	for range 1000 {
		val := pool.Generate()
		_, exists := seen[val]
		assert.False(t, exists, "duplicate lock value: %s", val)
		seen[val] = struct{}{}
	}
}

func TestPool_Generate_ConcurrentSafety(t *testing.T) {
	pool, err := lockpool.New("lock:")
	require.NoError(t, err)

	const goroutines = 100
	const perGoroutine = 100

	results := make(chan string, goroutines*perGoroutine)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range perGoroutine {
				results <- pool.Generate()
			}
		}()
	}
	wg.Wait()
	close(results)

	seen := make(map[string]struct{})
	for val := range results {
		_, exists := seen[val]
		assert.False(t, exists, "duplicate lock value under concurrency: %s", val)
		seen[val] = struct{}{}
	}
	assert.Len(t, seen, goroutines*perGoroutine)
}
