// Package syncx provides generic typed wrappers around standard library sync primitives.
package syncx

import "sync"

// Map is a generic typed wrapper around sync.Map that avoids interface{} casts at call sites.
type Map[K comparable, V any] struct {
	m sync.Map
}

func (sm *Map[K, V]) CompareAndDelete(key K, old V) bool {
	return sm.m.CompareAndDelete(key, old)
}

func (sm *Map[K, V]) CompareAndSwap(key K, old, new V) bool {
	return sm.m.CompareAndSwap(key, old, new)
}

func (sm *Map[K, V]) Delete(key K) {
	sm.m.Delete(key)
}

func (sm *Map[K, V]) Load(key K) (V, bool) {
	val, ok := sm.m.Load(key)
	if val == nil {
		var zero V
		return zero, ok
	}
	return val.(V), ok
}

func (sm *Map[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	val, loaded := sm.m.LoadAndDelete(key)
	if val == nil {
		var zero V
		return zero, loaded
	}
	return val.(V), loaded
}

func (sm *Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	val, loaded := sm.m.LoadOrStore(key, value)
	if val == nil {
		var zero V
		return zero, loaded
	}
	return val.(V), loaded
}

func (sm *Map[K, V]) Range(f func(key K, value V) bool) {
	sm.m.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}

func (sm *Map[K, V]) Store(key K, value V) {
	sm.m.Store(key, value)
}

func (sm *Map[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	prev, loaded := sm.m.Swap(key, value)
	if prev == nil {
		var zero V
		return zero, loaded
	}
	return prev.(V), loaded
}
