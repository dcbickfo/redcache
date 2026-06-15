// Package syncx provides generic typed wrappers around standard library sync primitives.
package syncx

import "sync"

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
