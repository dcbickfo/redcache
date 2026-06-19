package syncx_test

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/dcbickfo/redcache/internal/syncx"
)

type benchLockMap interface {
	Load(key string) (*int, bool)
	LoadAndDelete(key string) (*int, bool)
	LoadOrStore(key string, value *int) (*int, bool)
}

func BenchmarkLockMapReadMostly(b *testing.B) {
	keys := benchKeys(4096)
	value := new(int)
	b.Run("syncx", func(b *testing.B) {
		var sm syncx.Map[string, *int]
		for _, key := range keys {
			sm.LoadOrStore(key, value)
		}
		benchmarkMapLoads(b, &sm, keys)
	})
	b.Run("sharded", func(b *testing.B) {
		var sm syncx.ShardedMap[*int]
		for _, key := range keys {
			sm.LoadOrStore(key, value)
		}
		benchmarkMapLoads(b, &sm, keys)
	})
}

func BenchmarkLockMapChurn(b *testing.B) {
	keys := benchKeys(4096)
	value := new(int)
	b.Run("syncx", func(b *testing.B) {
		var sm syncx.Map[string, *int]
		benchmarkMapChurn(b, &sm, keys, value)
	})
	b.Run("sharded", func(b *testing.B) {
		var sm syncx.ShardedMap[*int]
		benchmarkMapChurn(b, &sm, keys, value)
	})
}

func BenchmarkLockMapHotKeyChurn(b *testing.B) {
	value := new(int)
	b.Run("syncx", func(b *testing.B) {
		var sm syncx.Map[string, *int]
		benchmarkMapChurn(b, &sm, []string{"hot"}, value)
	})
	b.Run("sharded", func(b *testing.B) {
		var sm syncx.ShardedMap[*int]
		benchmarkMapChurn(b, &sm, []string{"hot"}, value)
	})
}

func benchmarkMapLoads(b *testing.B, m benchLockMap, keys []string) {
	b.ReportAllocs()
	var n atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := int(n.Add(1)-1) & (len(keys) - 1)
			if _, ok := m.Load(keys[idx]); !ok {
				b.Fatal("missing key")
			}
		}
	})
}

func benchmarkMapChurn(b *testing.B, m benchLockMap, keys []string, value *int) {
	b.ReportAllocs()
	var n atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := int(n.Add(1)-1) % len(keys)
			key := keys[idx]
			m.LoadOrStore(key, value)
			m.LoadAndDelete(key)
		}
	})
}

func benchKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
	}
	return keys
}
