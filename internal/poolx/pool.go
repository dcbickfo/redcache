// Package poolx provides typed sync.Pool wrappers for reusing slice headers
// across multi-key call paths.
//
// Slices are stored as *[]T to avoid the per-Get interface-boxing allocation
// that plain []T values would incur. Callers reassign through the pointer
// when appending: *h = append(*h, ...). Capacity is capped at maxCap so a
// single oversized request does not pin a giant backing array.
package poolx

import "sync"

const maxCap = 1024

// Slice is a typed sync.Pool of []T. Callers receive a *[]T from Get / GetCap
// and return it via Put.
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

// Get returns a *[]T whose slice has length n and capacity ≥ n, zero-valued.
// The returned pointer must be returned via Put when the caller is done.
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
// when callers will append rather than index.
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

// Put returns a *[]T to the pool. Slices grown past maxCap are dropped.
func (s *Slice[T]) Put(h *[]T) {
	if cap(*h) > maxCap {
		return
	}
	*h = (*h)[:0]
	s.p.Put(h)
}
