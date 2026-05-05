package redcache

import (
	"context"
	"fmt"
	"time"
	"unsafe"
)

// Typed is a type-safe view over a *CacheAside.
//
// One *CacheAside can be shared across many Typed views with different
// K / V instantiations and codecs. The wrapper itself is stateless beyond
// its codec and underlying-client pointers and is safe for concurrent use
// to the same extent as the underlying *CacheAside and codecs.
type Typed[K comparable, V any] struct {
	cache    *CacheAside
	keyCodec KeyCodec[K]
	valCodec Codec[V]
}

// NewTyped constructs a typed view over cache.
func NewTyped[K comparable, V any](cache *CacheAside, keyCodec KeyCodec[K], valCodec Codec[V]) *Typed[K, V] {
	return &Typed[K, V]{cache: cache, keyCodec: keyCodec, valCodec: valCodec}
}

// NewStringTyped is sugar for NewTyped[string, V] with StringKeyCodec{} preset.
func NewStringTyped[V any](cache *CacheAside, valCodec Codec[V]) *Typed[string, V] {
	return NewTyped[string, V](cache, StringKeyCodec{}, valCodec)
}

// Get returns the cached value for k, populating the cache via fn on a miss.
// See (*CacheAside).Get for stampede / lock semantics — Typed.Get is a thin
// codec wrapper around it.
//
// Errors:
//   - keyCodec.EncodeKey error: returned before any Redis I/O.
//   - fn error: forwarded as-is (lock released, nothing cached).
//   - valCodec.Encode error inside fn: forwarded as a callback error.
//   - valCodec.Decode error on read: returned wrapped with ErrDecode; the
//     cached entry is left intact.
func (t *Typed[K, V]) Get(
	ctx context.Context,
	ttl time.Duration,
	k K,
	fn func(ctx context.Context, k K) (V, error),
) (V, error) {
	var zero V
	encKey, err := t.keyCodec.EncodeKey(k)
	if err != nil {
		return zero, fmt.Errorf("redcache: encode key: %w", err)
	}

	raw, err := t.cache.Get(ctx, ttl, encKey, func(ctx context.Context, _ string) (string, error) {
		v, ferr := fn(ctx, k)
		if ferr != nil {
			return "", ferr
		}
		b, eerr := t.valCodec.Encode(v)
		if eerr != nil {
			return "", fmt.Errorf("redcache: encode value: %w", eerr)
		}
		return bytesToString(b), nil
	})
	if err != nil {
		return zero, err
	}

	v, derr := t.valCodec.Decode(stringToBytes(raw))
	if derr != nil {
		return zero, fmt.Errorf("redcache: decode key %q: %w: %w", encKey, ErrDecode, derr)
	}
	return v, nil
}

// Del removes a key from Redis, triggering invalidation on all clients.
func (t *Typed[K, V]) Del(ctx context.Context, k K) error {
	encKey, err := t.keyCodec.EncodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	return t.cache.Del(ctx, encKey)
}

// Touch sets the TTL of a cached value to ttl (this can shorten or extend
// the remaining lifetime). See (*CacheAside).Touch for no-op-on-lock and
// no-op-on-missing-key semantics.
func (t *Typed[K, V]) Touch(ctx context.Context, ttl time.Duration, k K) error {
	encKey, err := t.keyCodec.EncodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	return t.cache.Touch(ctx, ttl, encKey)
}

// stringToBytes returns the bytes backing s. Callers must not mutate the
// returned slice — codecs are documented to treat their Decode input as
// borrowed.
func stringToBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// bytesToString returns a string viewing the bytes of b. Callers (the library)
// own b after Encode returns and never mutate it, so the returned string is
// safe for the lifetime of b.
func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// PrimeableTyped is a type-safe view over a *PrimeableCacheAside, adding
// Set / ForceSet / SetMulti / ForceSetMulti to the read/delete/touch surface
// from Typed.
type PrimeableTyped[K comparable, V any] struct {
	Typed[K, V]
	primeable *PrimeableCacheAside
}

// NewPrimeableTyped constructs a typed view over a *PrimeableCacheAside.
func NewPrimeableTyped[K comparable, V any](
	cache *PrimeableCacheAside,
	keyCodec KeyCodec[K],
	valCodec Codec[V],
) *PrimeableTyped[K, V] {
	return &PrimeableTyped[K, V]{
		Typed:     Typed[K, V]{cache: cache.CacheAside, keyCodec: keyCodec, valCodec: valCodec},
		primeable: cache,
	}
}

// NewPrimeableStringTyped is sugar for NewPrimeableTyped[string, V] with
// StringKeyCodec{} preset.
func NewPrimeableStringTyped[V any](
	cache *PrimeableCacheAside,
	valCodec Codec[V],
) *PrimeableTyped[string, V] {
	return NewPrimeableTyped[string, V](cache, StringKeyCodec{}, valCodec)
}

// Set explicitly populates the cache via fn under a write lock.
// See (*PrimeableCacheAside).Set.
func (p *PrimeableTyped[K, V]) Set(
	ctx context.Context,
	ttl time.Duration,
	k K,
	fn func(ctx context.Context, k K) (V, error),
) error {
	encKey, err := p.keyCodec.EncodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	return p.primeable.Set(ctx, ttl, encKey, func(ctx context.Context, _ string) (string, error) {
		v, ferr := fn(ctx, k)
		if ferr != nil {
			return "", ferr
		}
		b, eerr := p.valCodec.Encode(v)
		if eerr != nil {
			return "", fmt.Errorf("redcache: encode value: %w", eerr)
		}
		return bytesToString(b), nil
	})
}

// ForceSet unconditionally writes v to Redis. See (*PrimeableCacheAside).ForceSet.
func (p *PrimeableTyped[K, V]) ForceSet(ctx context.Context, ttl time.Duration, k K, v V) error {
	encKey, err := p.keyCodec.EncodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	b, err := p.valCodec.Encode(v)
	if err != nil {
		return fmt.Errorf("redcache: encode value: %w", err)
	}
	return p.primeable.ForceSet(ctx, ttl, encKey, bytesToString(b))
}
