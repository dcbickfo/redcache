// Package poolx provides typed sync.Pool wrappers for reusing slice headers
// across multi-key call paths.
//
// Slices are stored as *[]T to avoid the per-Get interface-boxing allocation
// that plain []T values would incur — sync.Pool's Get returns any, and a
// slice header is three words wide, so storing it as a value escapes via the
// interface. Callers receive a *[]T handle, dereference it to read the slice,
// and reassign through the pointer when appending: *h = append(*h, ...).
//
// Capacity is capped at maxCap so a single oversized request does not pin a
// giant backing array in the pool.
package poolx

import "sync"

const maxCap = 1024

// Slice is a typed sync.Pool of []T. Callers receive a *[]T from Get / GetCap
// and return it via Put. The pointer indirection avoids the alloc that would
// occur if []T values were boxed into sync.Pool's any.
type Slice[T any] struct {
	p sync.Pool
}

// NewSlice creates a Slice pool. The newFn is invoked when the pool is empty.
func NewSlice[T any](newFn func() []T) *Slice[T] {
	return &Slice[T]{p: sync.Pool{New: func() any {
		v := newFn()
		return &v
	}}}
}

// Get returns a *[]T whose slice has length n and capacity ≥ n. The slice
// contents are zero-valued for the requested length. The returned pointer
// must be returned via Put when the caller is done.
func (s *Slice[T]) Get(n int) *[]T {
	h := s.p.Get().(*[]T)
	v := *h
	if cap(v) < n {
		v = make([]T, n)
	} else {
		v = v[:n]
		var zero T
		for i := range v {
			v[i] = zero
		}
	}
	*h = v
	return h
}

// GetCap returns a *[]T whose slice is empty (len=0) with capacity ≥ n. Use
// when callers will append rather than index. If a subsequent append exceeds
// the returned capacity, the reallocated slice replaces the pooled one when
// the caller writes back via *h = append(*h, ...).
func (s *Slice[T]) GetCap(n int) *[]T {
	h := s.p.Get().(*[]T)
	v := *h
	if cap(v) < n {
		v = make([]T, 0, n)
	} else {
		v = v[:0]
	}
	*h = v
	return h
}

// Put returns a *[]T to the pool. If the underlying slice grew past maxCap,
// it is dropped to avoid pinning an oversized backing array.
func (s *Slice[T]) Put(h *[]T) {
	if cap(*h) > maxCap {
		return
	}
	*h = (*h)[:0]
	s.p.Put(h)
}
