// Package syncx provides generic typed wrappers around standard library sync primitives.
package syncx

import (
	"hash/maphash"
	"sync"
)

const shardedMapShardCount = 32

// Map is a generic typed wrapper around sync.Map that avoids interface{} casts at call sites.
type Map[K comparable, V any] struct {
	m sync.Map
}

// CompareAndDelete deletes the entry for key if its value equals old.
func (sm *Map[K, V]) CompareAndDelete(key K, old V) bool {
	return sm.m.CompareAndDelete(key, old)
}

// CompareAndSwap swaps the value for key to new if the stored value equals old.
func (sm *Map[K, V]) CompareAndSwap(key K, old, new V) bool {
	return sm.m.CompareAndSwap(key, old, new)
}

// Delete deletes the value for key.
func (sm *Map[K, V]) Delete(key K) {
	sm.m.Delete(key)
}

// Load returns the value stored for key, or the zero value if absent.
func (sm *Map[K, V]) Load(key K) (V, bool) {
	val, ok := sm.m.Load(key)
	if val == nil {
		var zero V
		return zero, ok
	}
	return val.(V), ok
}

// LoadAndDelete deletes the value for key, returning the previous value if present.
func (sm *Map[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	val, loaded := sm.m.LoadAndDelete(key)
	if val == nil {
		var zero V
		return zero, loaded
	}
	return val.(V), loaded
}

// LoadOrStore returns the existing value for key if present, otherwise stores and returns value.
func (sm *Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	val, loaded := sm.m.LoadOrStore(key, value)
	if val == nil {
		var zero V
		return zero, loaded
	}
	return val.(V), loaded
}

// Range calls f for each key/value in the map, stopping early if f returns false.
func (sm *Map[K, V]) Range(f func(key K, value V) bool) {
	sm.m.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}

// Store sets the value for key.
func (sm *Map[K, V]) Store(key K, value V) {
	sm.m.Store(key, value)
}

// Swap stores value for key and returns the previous value, if present.
func (sm *Map[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	prev, loaded := sm.m.Swap(key, value)
	if prev == nil {
		var zero V
		return zero, loaded
	}
	return prev.(V), loaded
}

// ShardedMap is a string-keyed map optimized for Store/Delete churn.
type ShardedMap[V comparable] struct {
	seedOnce sync.Once
	seed     maphash.Seed
	shards   [shardedMapShardCount]shardedMapShard[V]
}

type shardedMapShard[V comparable] struct {
	mu sync.RWMutex
	m  map[string]V
}

// CompareAndDelete deletes the entry for key if its value equals old.
func (sm *ShardedMap[V]) CompareAndDelete(key string, old V) bool {
	shard := sm.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.m == nil {
		return false
	}
	val, ok := shard.m[key]
	if !ok || val != old {
		return false
	}
	delete(shard.m, key)
	return true
}

// Delete deletes the value for key.
func (sm *ShardedMap[V]) Delete(key string) {
	shard := sm.shard(key)
	shard.mu.Lock()
	if shard.m != nil {
		delete(shard.m, key)
	}
	shard.mu.Unlock()
}

// Load returns the value stored for key, or the zero value if absent.
func (sm *ShardedMap[V]) Load(key string) (V, bool) {
	shard := sm.shard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	if shard.m == nil {
		var zero V
		return zero, false
	}
	val, ok := shard.m[key]
	return val, ok
}

// LoadAndDelete deletes the value for key, returning the previous value if present.
func (sm *ShardedMap[V]) LoadAndDelete(key string) (value V, loaded bool) {
	shard := sm.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.m == nil {
		return value, false
	}
	value, loaded = shard.m[key]
	delete(shard.m, key)
	return value, loaded
}

// LoadOrStore returns the existing value for key if present, otherwise stores and returns value.
func (sm *ShardedMap[V]) LoadOrStore(key string, value V) (actual V, loaded bool) {
	shard := sm.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.m == nil {
		shard.m = make(map[string]V)
	}
	if actual, loaded = shard.m[key]; loaded {
		return actual, true
	}
	shard.m[key] = value
	return value, false
}

// Range calls f for each key/value in the map, stopping early if f returns false.
func (sm *ShardedMap[V]) Range(f func(key string, value V) bool) {
	type entry struct {
		key   string
		value V
	}
	for i := range sm.shards {
		shard := &sm.shards[i]
		shard.mu.RLock()
		entries := make([]entry, 0, len(shard.m))
		for key, value := range shard.m {
			entries = append(entries, entry{key: key, value: value})
		}
		shard.mu.RUnlock()
		for _, entry := range entries {
			if !f(entry.key, entry.value) {
				return
			}
		}
	}
}

// Store sets the value for key.
func (sm *ShardedMap[V]) Store(key string, value V) {
	shard := sm.shard(key)
	shard.mu.Lock()
	if shard.m == nil {
		shard.m = make(map[string]V)
	}
	shard.m[key] = value
	shard.mu.Unlock()
}

func (sm *ShardedMap[V]) shard(key string) *shardedMapShard[V] {
	sm.seedOnce.Do(func() {
		sm.seed = maphash.MakeSeed()
	})
	return &sm.shards[int(maphash.String(sm.seed, key)%shardedMapShardCount)]
}
