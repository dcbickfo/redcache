package redcache

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrLockLost indicates the distributed lock was lost or expired before the value could be set.
// This can occur if the lock TTL expires during callback execution or if Redis invalidates the lock.
var ErrLockLost = errors.New("lock was lost or expired before value could be set")

// BatchError represents partial failures in a multi-key operation.
// Some keys may have succeeded while others failed.
//
// Currently only PrimeableCacheAside.SetMulti returns this type via errors.As;
// Get/GetMulti and Del/DelMulti return plain wrapped errors.
type BatchError struct {
	// Failed maps each failed key to its error.
	Failed map[string]error
	// Succeeded lists the keys that were set successfully.
	Succeeded []string
}

// Error returns a human-readable summary of the batch failure. Failed keys are
// emitted in sorted order so the string is stable across calls (map iteration
// would otherwise scramble per-key entries). Safe to call on a nil receiver.
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

// HasFailures returns true if any keys failed. Safe to call on a nil receiver.
func (e *BatchError) HasFailures() bool {
	if e == nil {
		return false
	}
	return len(e.Failed) > 0
}

// ErrorFor returns the error recorded for key, or nil if the key did not fail.
// Safe to call on a nil receiver, so callers can chain after errors.As without
// a nil-check.
func (e *BatchError) ErrorFor(key string) error {
	if e == nil {
		return nil
	}
	return e.Failed[key]
}

// HasError reports whether the given key failed. Safe to call on a nil receiver.
func (e *BatchError) HasError(key string) bool {
	if e == nil {
		return false
	}
	_, ok := e.Failed[key]
	return ok
}

// NewBatchError creates a BatchError from the given failures and successes.
// Returns nil (untyped) if there are no failures, so it is safe to return
// directly as an error interface value.
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
// cannot be decoded by the configured Codec. The library does not auto-evict
// — the caller decides whether to log, Del, or retry.
var ErrDecode = errors.New("redcache: decode failed")

// BatchKeyError is the typed counterpart of BatchError, returned (via errors.As)
// from PrimeableTyped's multi-set methods on partial failure. Same semantics
// as BatchError; the only difference is that keys are typed K instead of string.
//
// Note: the Error() formatter is intentionally a structural duplicate of
// BatchError.Error() (~13 lines). The format is stable and the duplication
// is preferable to a generic helper that would either constrain K to ordered
// types or sort by fmt.Sprintf("%v", k).
type BatchKeyError[K comparable] struct {
	Failed    map[K]error
	Succeeded []K
}

// Error returns a human-readable summary. Failed keys are emitted in the order
// produced by sorting their %v formatting so the string is stable across calls.
// Safe to call on a nil receiver.
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

// HasFailures returns true if any keys failed. Safe to call on a nil receiver.
func (e *BatchKeyError[K]) HasFailures() bool {
	if e == nil {
		return false
	}
	return len(e.Failed) > 0
}

// ErrorFor returns the error recorded for k, or nil if the key did not fail.
// Safe to call on a nil receiver.
func (e *BatchKeyError[K]) ErrorFor(k K) error {
	if e == nil {
		return nil
	}
	return e.Failed[k]
}

// HasError reports whether the given key failed. Safe to call on a nil receiver.
func (e *BatchKeyError[K]) HasError(k K) bool {
	if e == nil {
		return false
	}
	_, ok := e.Failed[k]
	return ok
}

// NewBatchKeyError mirrors NewBatchError. Returns untyped nil when there
// are no failures, so call sites can return it directly as `error`.
func NewBatchKeyError[K comparable](failed map[K]error, succeeded []K) error {
	if len(failed) == 0 {
		return nil
	}
	return &BatchKeyError[K]{Failed: failed, Succeeded: succeeded}
}
