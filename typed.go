package redcache

import (
	"context"
	"fmt"
	"time"
	"unsafe"
)

// Typed is a type-safe view over a *CacheAside. One *CacheAside can be shared
// across many Typed views with different K/V instantiations and codecs.
type Typed[K comparable, V any] struct {
	cache    *CacheAside
	keyCodec KeyCodec[K]
	valCodec Codec[V]
	// keyIsString is true iff keyCodec is StringKeyCodec, which guarantees
	// K=string (StringKeyCodec only implements KeyCodec[string]). Multi-key
	// paths use this to alias []K↔[]string and skip a reverse-lookup map.
	keyIsString bool
}

// NewTyped constructs a typed view over cache.
func NewTyped[K comparable, V any](cache *CacheAside, keyCodec KeyCodec[K], valCodec Codec[V]) *Typed[K, V] {
	t := &Typed[K, V]{cache: cache, keyCodec: keyCodec, valCodec: valCodec}
	if _, ok := any(keyCodec).(StringKeyCodec); ok {
		t.keyIsString = true
	}
	return t
}

// NewStringTyped is sugar for NewTyped[string, V] with StringKeyCodec{} preset.
func NewStringTyped[V any](cache *CacheAside, valCodec Codec[V]) *Typed[string, V] {
	return NewTyped[string, V](cache, StringKeyCodec{}, valCodec)
}

// Get returns the cached value for k, populating the cache via fn on a miss.
// See (*CacheAside).Get for stampede / lock semantics.
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

// Touch sets the TTL of a cached value to ttl. See (*CacheAside).Touch for
// no-op-on-lock and no-op-on-missing-key semantics.
func (t *Typed[K, V]) Touch(ctx context.Context, ttl time.Duration, k K) error {
	encKey, err := t.keyCodec.EncodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	return t.cache.Touch(ctx, ttl, encKey)
}

// GetMulti returns cached values for keys, populating misses via fn.
// See (*CacheAside).GetMulti for slot-batching and stampede semantics.
//
// Errors:
//   - keyCodec.EncodeKey error: returned before any Redis I/O.
//   - fn error: forwarded as-is.
//   - valCodec.Encode error inside fn: forwarded as a callback error.
//   - valCodec.Decode error on a read result: returned wrapped with ErrDecode;
//     no successful entries are returned in that case.
func (t *Typed[K, V]) GetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, missing []K) (map[K]V, error),
) (map[K]V, error) {
	if len(keys) == 0 {
		return map[K]V{}, nil
	}
	if t.keyIsString {
		return t.getMultiString(ctx, ttl, keys, fn)
	}
	return t.getMultiKeyed(ctx, ttl, keys, fn)
}

// getMultiString is the K=string fast path. Aliases keys to []string and skips
// the byEnc reverse-lookup map.
func (t *Typed[K, V]) getMultiString(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, missing []K) (map[K]V, error),
) (map[K]V, error) {
	encKeys := asStringSlice(keys)

	raw, err := t.cache.GetMulti(ctx, ttl, encKeys, func(ctx context.Context, missingEnc []string) (map[string]string, error) {
		result, ferr := fn(ctx, asKSlice[K](missingEnc))
		if ferr != nil {
			return nil, ferr
		}
		return t.encodeMultiResult(result)
	})
	if err != nil {
		return nil, err
	}

	out := make(map[K]V, len(raw))
	for s, payload := range raw {
		v, derr := t.valCodec.Decode(stringToBytes(payload))
		if derr != nil {
			return nil, fmt.Errorf("redcache: decode key %q: %w: %w", s, ErrDecode, derr)
		}
		out[asK[K](s)] = v
	}
	return out, nil
}

// getMultiKeyed is the general path for non-string keys, building a byEnc map
// to recover K from each encoded key.
func (t *Typed[K, V]) getMultiKeyed(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, missing []K) (map[K]V, error),
) (map[K]V, error) {
	encKeys := make([]string, len(keys))
	byEnc := make(map[string]K, len(keys))
	for i, k := range keys {
		s, err := t.keyCodec.EncodeKey(k)
		if err != nil {
			return nil, fmt.Errorf("redcache: encode key: %w", err)
		}
		encKeys[i] = s
		byEnc[s] = k
	}

	raw, err := t.cache.GetMulti(ctx, ttl, encKeys, func(ctx context.Context, missingEnc []string) (map[string]string, error) {
		missingK := make([]K, len(missingEnc))
		for i, s := range missingEnc {
			missingK[i] = byEnc[s]
		}
		result, ferr := fn(ctx, missingK)
		if ferr != nil {
			return nil, ferr
		}
		return t.encodeMultiResult(result)
	})
	if err != nil {
		return nil, err
	}

	out := make(map[K]V, len(raw))
	for s, payload := range raw {
		k, ok := byEnc[s]
		if !ok {
			continue
		}
		v, derr := t.valCodec.Decode(stringToBytes(payload))
		if derr != nil {
			return nil, fmt.Errorf("redcache: decode key %q: %w: %w", s, ErrDecode, derr)
		}
		out[k] = v
	}
	return out, nil
}

// DelMulti removes multiple keys from Redis, triggering invalidation. See
// (*CacheAside).DelMulti.
func (t *Typed[K, V]) DelMulti(ctx context.Context, keys ...K) error {
	if len(keys) == 0 {
		return nil
	}
	encKeys, err := t.encodeKeys(keys)
	if err != nil {
		return err
	}
	return t.cache.DelMulti(ctx, encKeys...)
}

// TouchMulti sets the TTL of multiple cached values to ttl. See
// (*CacheAside).TouchMulti for no-op-on-lock and no-op-on-missing-key semantics.
func (t *Typed[K, V]) TouchMulti(ctx context.Context, ttl time.Duration, keys ...K) error {
	if len(keys) == 0 {
		return nil
	}
	encKeys, err := t.encodeKeys(keys)
	if err != nil {
		return err
	}
	return t.cache.TouchMulti(ctx, ttl, encKeys...)
}

// encodeKeys returns keys as []string. Aliases for the StringKeyCodec fast path,
// otherwise allocates and runs EncodeKey per key.
func (t *Typed[K, V]) encodeKeys(keys []K) ([]string, error) {
	if t.keyIsString {
		return asStringSlice(keys), nil
	}
	encKeys := make([]string, len(keys))
	for i, k := range keys {
		s, err := t.keyCodec.EncodeKey(k)
		if err != nil {
			return nil, fmt.Errorf("redcache: encode key: %w", err)
		}
		encKeys[i] = s
	}
	return encKeys, nil
}

// encodeMultiResult converts a typed map[K]V from the user's loader into the
// map[string]string that (*CacheAside).GetMulti stores in Redis.
func (t *Typed[K, V]) encodeMultiResult(result map[K]V) (map[string]string, error) {
	out := make(map[string]string, len(result))
	for k, v := range result {
		var s string
		if t.keyIsString {
			s = asString(k)
		} else {
			ks, kerr := t.keyCodec.EncodeKey(k)
			if kerr != nil {
				return nil, fmt.Errorf("redcache: encode key: %w", kerr)
			}
			s = ks
		}
		b, eerr := t.valCodec.Encode(v)
		if eerr != nil {
			return nil, fmt.Errorf("redcache: encode value for key %q: %w", s, eerr)
		}
		out[s] = bytesToString(b)
	}
	return out, nil
}

// asStringSlice reinterprets a []K as []string. Callers must guarantee K=string
// (e.g., via Typed.keyIsString).
func asStringSlice[K comparable](keys []K) []string {
	return *(*[]string)(unsafe.Pointer(&keys))
}

// asKSlice reinterprets a []string as []K. Callers must guarantee K=string.
func asKSlice[K comparable](s []string) []K {
	return *(*[]K)(unsafe.Pointer(&s))
}

// asK reinterprets a string as K. Callers must guarantee K=string.
func asK[K comparable](s string) K {
	return *(*K)(unsafe.Pointer(&s))
}

// asString reinterprets a K as string. Callers must guarantee K=string.
func asString[K comparable](k K) string {
	return *(*string)(unsafe.Pointer(&k))
}

// stringToBytes returns the bytes backing s. Callers must not mutate the
// returned slice — codecs treat Decode input as borrowed.
func stringToBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// bytesToString returns a string viewing the bytes of b. The library owns b
// after Encode returns and never mutates it.
func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}
