// Package mapsx provides generic map utility functions optimized for performance.
package mapsx

// Keys extracts keys from a map into a slice with pre-allocated capacity.
// This is more efficient than using maps.Keys() + slices.Collect() for hot paths.
//
// Benchmarks (50 keys):
//   - Keys():                    ~476ns/op, 896 B/op, 1 alloc/op
//   - maps.Keys + slices.Collect: ~1045ns/op, 2224 B/op, 10 allocs/op
//
// The stdlib approach is ~2.2x slower with ~2.5x more memory and 10x more allocations.
// For cold paths or readability, stdlib is fine. For hot paths (GetMulti, SetMulti), use this.
func Keys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Values extracts values from a map into a slice with pre-allocated capacity.
// This is more efficient than using maps.Values() + slices.Collect() for hot paths.
// Same performance characteristics as Keys().
func Values[K comparable, V any](m map[K]V) []V {
	values := make([]V, 0, len(m))
	for _, v := range m {
		values = append(values, v)
	}
	return values
}

// ToSet converts map keys to a set (map with bool values).
// This is useful for creating exclusion sets or membership tests.
func ToSet[K comparable, V any](m map[K]V) map[K]bool {
	result := make(map[K]bool, len(m))
	for key := range m {
		result[key] = true
	}
	return result
}
