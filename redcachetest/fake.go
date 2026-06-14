// Package redcachetest provides an in-memory test double for redcache.Cache.
//
// Fake[K, V] satisfies redcache.Cache[K, V] backed by a plain map guarded by a
// mutex. It exists so adopters can unit-test their loaders, wiring, and
// cache-aside call shape with no Redis dependency.
//
// What the Fake models: the OBSERVABLE single-process cache-aside contract.
// A present, unexpired key is a hit and skips fn; a miss calls fn exactly once,
// stores the result, and returns it. Hits are presence-based, so a stored
// zero/empty value is still a hit.
//
// What the Fake does NOT model: distributed single-flight, the SET NX lock
// layer, client-side-cache invalidation pushes, the stored envelope format, or
// refresh-ahead. Those behaviours only emerge against a real Redis. To exercise
// them in tests, drive the real redcache.Cache against rueidis/mock (NOT
// miniredis — miniredis cannot emulate RESP3 client-side invalidation).
package redcachetest

import (
	"context"
	"sync"
	"time"

	"github.com/dcbickfo/redcache"
)

// entry is a stored value with its expiry deadline. A zero deadline means the
// entry never expires.
type entry[V any] struct {
	value    V
	deadline time.Time // zero == no expiry
}

// Clock is a manually-advanced time source for deterministic expiry tests. Pass
// one to NewWithClock and move time forward with Advance instead of sleeping.
// Its zero value starts at the wall-clock time of the first Now call. A Clock is
// safe for concurrent use.
type Clock struct {
	mu   sync.Mutex
	base time.Time     // wall-clock anchor, set lazily on first use.
	off  time.Duration // accumulated Advance.
}

// Now returns the Clock's current time: its anchor plus all advances so far. The
// anchor is captured on the first Now call, so a fresh Clock reads as "now".
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.base.IsZero() {
		c.base = time.Now()
	}
	return c.base.Add(c.off)
}

// Advance moves the Clock forward by d. Negative durations move it backward.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.base.IsZero() {
		c.base = time.Now()
	}
	c.off += d
}

// Fake is an in-memory redcache.Cache[K, V] for unit tests. It honours the
// observable single-process cache-aside contract (see package docs) and is safe
// for concurrent use. The zero value is not usable; construct one with New.
type Fake[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]entry[V]
	now  func() time.Time // time source for TTL deadlines; defaults to time.Now.
}

var _ redcache.Cache[string, []byte] = (*Fake[string, []byte])(nil)

// New returns an empty Fake[K, V] backed by the real wall clock.
func New[K comparable, V any]() *Fake[K, V] {
	return &Fake[K, V]{data: make(map[K]entry[V]), now: time.Now}
}

// NewWithClock returns an empty Fake[K, V] whose TTL deadlines are measured
// against clk. Advance clk to expire entries deterministically, with no sleeps.
func NewWithClock[K comparable, V any](clk *Clock) *Fake[K, V] {
	return &Fake[K, V]{data: make(map[K]entry[V]), now: clk.Now}
}

// deadlineFor converts a ttl to an absolute deadline against the Fake's clock. A
// ttl <= 0 yields a zero deadline, meaning the entry never expires.
func (f *Fake[K, V]) deadlineFor(ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return f.now().Add(ttl)
}

// load returns the live value for k. It treats an expired entry as a miss and
// returns ok == false. Callers must hold at least a read lock.
func (f *Fake[K, V]) load(k K) (V, bool) {
	e, ok := f.data[k]
	if !ok {
		var zero V
		return zero, false
	}
	if !e.deadline.IsZero() && f.now().After(e.deadline) {
		var zero V
		return zero, false
	}
	return e.value, true
}

// Get returns the cached value for k, calling fn exactly once on a miss and
// storing its result under ttl. A present, unexpired entry is a hit (even if
// its stored value is the zero value) and fn is not called.
func (f *Fake[K, V]) Get(
	ctx context.Context,
	ttl time.Duration,
	k K,
	fn func(ctx context.Context, k K) (V, error),
) (V, error) {
	f.mu.RLock()
	if v, ok := f.load(k); ok {
		f.mu.RUnlock()
		return v, nil
	}
	f.mu.RUnlock()

	v, err := fn(ctx, k)
	if err != nil {
		var zero V
		return zero, err
	}

	f.mu.Lock()
	f.data[k] = entry[V]{value: v, deadline: f.deadlineFor(ttl)}
	f.mu.Unlock()
	return v, nil
}

// GetMulti returns cached values for the present keys and calls fn exactly once
// with the missing keys (if any). Results from fn are stored under ttl and
// merged into the returned map. fn is not called when every key is a hit.
func (f *Fake[K, V]) GetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, missing []K) (map[K]V, error),
) (map[K]V, error) {
	out := make(map[K]V, len(keys))
	var missing []K

	f.mu.RLock()
	for _, k := range keys {
		if v, ok := f.load(k); ok {
			out[k] = v
		} else {
			missing = append(missing, k)
		}
	}
	f.mu.RUnlock()

	if len(missing) == 0 {
		return out, nil
	}

	loaded, err := fn(ctx, missing)
	if err != nil {
		return nil, err
	}

	deadline := f.deadlineFor(ttl)
	f.mu.Lock()
	for k, v := range loaded {
		f.data[k] = entry[V]{value: v, deadline: deadline}
		out[k] = v
	}
	f.mu.Unlock()
	return out, nil
}

// Peek reports whether k is present and unexpired without a loader. It returns
// (value, true, nil) on a hit (a stored zero value still counts) and
// (zero, false, nil) on a miss. ttl is accepted to satisfy the Cache contract
// but the Fake never mutates state on a Peek.
func (f *Fake[K, V]) Peek(_ context.Context, _ time.Duration, k K) (V, bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.load(k)
	return v, ok, nil
}

// Set calls fn and stores its result under k with ttl, unconditionally
// overwriting any existing entry.
func (f *Fake[K, V]) Set(
	ctx context.Context,
	ttl time.Duration,
	k K,
	fn func(ctx context.Context, k K) (V, error),
) error {
	v, err := fn(ctx, k)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.data[k] = entry[V]{value: v, deadline: f.deadlineFor(ttl)}
	f.mu.Unlock()
	return nil
}

// SetMulti calls fn once with all keys and stores every returned entry under
// ttl, overwriting any existing entries.
func (f *Fake[K, V]) SetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, keys []K) (map[K]V, error),
) error {
	if len(keys) == 0 {
		return nil
	}
	loaded, err := fn(ctx, keys)
	if err != nil {
		return err
	}
	deadline := f.deadlineFor(ttl)
	f.mu.Lock()
	for k, v := range loaded {
		f.data[k] = entry[V]{value: v, deadline: deadline}
	}
	f.mu.Unlock()
	return nil
}

// ForceSet stores v under k with ttl, bypassing any loader.
func (f *Fake[K, V]) ForceSet(_ context.Context, ttl time.Duration, k K, v V) error {
	f.mu.Lock()
	f.data[k] = entry[V]{value: v, deadline: f.deadlineFor(ttl)}
	f.mu.Unlock()
	return nil
}

// ForceSetMulti stores every key/value in values under ttl, bypassing any
// loader.
func (f *Fake[K, V]) ForceSetMulti(_ context.Context, ttl time.Duration, values map[K]V) error {
	if len(values) == 0 {
		return nil
	}
	deadline := f.deadlineFor(ttl)
	f.mu.Lock()
	for k, v := range values {
		f.data[k] = entry[V]{value: v, deadline: deadline}
	}
	f.mu.Unlock()
	return nil
}

// Del removes k from the cache. Missing keys are a no-op.
func (f *Fake[K, V]) Del(_ context.Context, k K) error {
	f.mu.Lock()
	delete(f.data, k)
	f.mu.Unlock()
	return nil
}

// DelMulti removes the given keys from the cache. Missing keys are a no-op.
func (f *Fake[K, V]) DelMulti(_ context.Context, keys ...K) error {
	f.mu.Lock()
	for _, k := range keys {
		delete(f.data, k)
	}
	f.mu.Unlock()
	return nil
}

// Touch resets the expiry of k to now+ttl. It is a no-op when k is absent or
// already expired (it does not resurrect an expired entry).
func (f *Fake[K, V]) Touch(_ context.Context, ttl time.Duration, k K) error {
	f.mu.Lock()
	if v, ok := f.load(k); ok {
		f.data[k] = entry[V]{value: v, deadline: f.deadlineFor(ttl)}
	}
	f.mu.Unlock()
	return nil
}

// TouchMulti resets the expiry of each present key to now+ttl. Absent or expired
// keys are skipped.
func (f *Fake[K, V]) TouchMulti(_ context.Context, ttl time.Duration, keys ...K) error {
	deadline := f.deadlineFor(ttl)
	f.mu.Lock()
	for _, k := range keys {
		if v, ok := f.load(k); ok {
			f.data[k] = entry[V]{value: v, deadline: deadline}
		}
	}
	f.mu.Unlock()
	return nil
}
