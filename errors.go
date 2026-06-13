package redcache

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrLockLost is returned when the distributed lock was stolen or expired
// before the value could be written.
var ErrLockLost = errors.New("lock was lost or expired before value could be set")

// BatchError carries per-key results of a multi-key operation. All accessors
// are nil-safe.
type BatchError struct {
	Failed    map[string]error
	Succeeded []string
}

// Error formats the batch outcome with failed keys in sorted order.
func (e *BatchError) Error() string {
	if e == nil {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "batch operation partially failed: %d succeeded, %d failed", len(e.Succeeded), len(e.Failed))
	keys := make([]string, 0, len(e.Failed))
	for key := range e.Failed {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(&b, "; key %q: %s", key, e.Failed[key])
	}
	return b.String()
}

// HasFailures reports whether any key failed.
func (e *BatchError) HasFailures() bool {
	if e == nil {
		return false
	}
	return len(e.Failed) > 0
}

// ErrorFor returns the error for key, or nil.
func (e *BatchError) ErrorFor(key string) error {
	if e == nil {
		return nil
	}
	return e.Failed[key]
}

// HasError reports whether key failed.
func (e *BatchError) HasError(key string) bool {
	if e == nil {
		return false
	}
	_, ok := e.Failed[key]
	return ok
}

// NewBatchError returns a *BatchError as error, or untyped nil when failed
// is empty (so call sites can return it directly).
func NewBatchError(failed map[string]error, succeeded []string) error {
	if len(failed) == 0 {
		return nil
	}
	return &BatchError{
		Failed:    failed,
		Succeeded: succeeded,
	}
}

// ErrDecode is returned (wrapped) from typed reads when a stored value
// cannot be decoded. The library does not auto-evict — the caller decides
// whether to log, Del, or retry.
var ErrDecode = errors.New("redcache: decode failed")

// BatchKeyError is the typed counterpart of BatchError, returned via errors.As
// from PrimeableTyped multi-set methods on partial failure. All accessors are
// nil-safe.
type BatchKeyError[K comparable] struct {
	Failed    map[K]error
	Succeeded []K
}

// Error formats the batch outcome with failed keys sorted by their %v
// rendering for stable output.
func (e *BatchKeyError[K]) Error() string {
	if e == nil {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "batch operation partially failed: %d succeeded, %d failed", len(e.Succeeded), len(e.Failed))
	type kv struct {
		k K
		s string
	}
	pairs := make([]kv, 0, len(e.Failed))
	for k := range e.Failed {
		pairs = append(pairs, kv{k: k, s: fmt.Sprintf("%v", k)})
	}
	sort.SliceStable(pairs, func(i, j int) bool { return pairs[i].s < pairs[j].s })
	for _, p := range pairs {
		fmt.Fprintf(&b, "; key %q: %s", p.s, e.Failed[p.k])
	}
	return b.String()
}

// HasFailures reports whether any key failed.
func (e *BatchKeyError[K]) HasFailures() bool {
	if e == nil {
		return false
	}
	return len(e.Failed) > 0
}

// ErrorFor returns the error for k, or nil.
func (e *BatchKeyError[K]) ErrorFor(k K) error {
	if e == nil {
		return nil
	}
	return e.Failed[k]
}

// HasError reports whether k failed.
func (e *BatchKeyError[K]) HasError(k K) bool {
	if e == nil {
		return false
	}
	_, ok := e.Failed[k]
	return ok
}

// NewBatchKeyError returns a *BatchKeyError as error, or untyped nil when
// failed is empty (so call sites can return it directly).
func NewBatchKeyError[K comparable](failed map[K]error, succeeded []K) error {
	if len(failed) == 0 {
		return nil
	}
	return &BatchKeyError[K]{Failed: failed, Succeeded: succeeded}
}
