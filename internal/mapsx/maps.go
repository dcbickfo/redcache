// Package mapsx provides generic helpers for map operations.
package mapsx

// Keys returns the keys of the map m in unspecified order.
func Keys[M ~map[K]V, K comparable, V any](m M) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
