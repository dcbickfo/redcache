package redcache

import (
	"context"
	"fmt"
	"time"
	"unsafe"
)

// Typed is a type-safe view over a *CacheAside. One *CacheAside may be shared
// across many Typed views with different K/V and codecs.
type Typed[K comparable, V any] struct {
	cache    *CacheAside
	keyCodec KeyCodec[K]
	valCodec Codec[V]
	// Set when keyCodec is StringKeyCodec; multi-key paths then alias
	// []K↔[]string instead of building a reverse-lookup map.
	keyIsString bool
}

func NewTyped[K comparable, V any](cache *CacheAside, keyCodec KeyCodec[K], valCodec Codec[V]) *Typed[K, V] {
	t := &Typed[K, V]{cache: cache, keyCodec: keyCodec, valCodec: valCodec}
	if _, ok := any(keyCodec).(StringKeyCodec); ok {
		t.keyIsString = true
	}
	return t
}

// NewStringTyped is NewTyped[string, V] with StringKeyCodec preset.
func NewStringTyped[V any](cache *CacheAside, valCodec Codec[V]) *Typed[string, V] {
	return NewTyped[string, V](cache, StringKeyCodec{}, valCodec)
}

// Get returns the cached value for k, calling fn on a miss. Decode errors on
// read are wrapped with ErrDecode and leave the cached entry intact. See
// (*CacheAside).Get for stampede / lock semantics.
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

// Del removes a key, triggering invalidation on all subscribed clients.
func (t *Typed[K, V]) Del(ctx context.Context, k K) error {
	encKey, err := t.keyCodec.EncodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	return t.cache.Del(ctx, encKey)
}

// Touch sets the TTL of a cached value. See (*CacheAside).Touch.
func (t *Typed[K, V]) Touch(ctx context.Context, ttl time.Duration, k K) error {
	encKey, err := t.keyCodec.EncodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	return t.cache.Touch(ctx, ttl, encKey)
}

// GetMulti returns cached values for keys, calling fn for misses. A decode
// error on any read returns wrapped with ErrDecode and aborts the batch. See
// (*CacheAside).GetMulti for slot-batching and stampede semantics.
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

// K=string fast path: aliases keys to []string, skips the reverse-lookup map.
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

// DelMulti removes keys, triggering invalidation. See (*CacheAside).DelMulti.
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

// TouchMulti extends the TTL of cached values. See (*CacheAside).TouchMulti.
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

// The asK / asString family aliases between K and string under the invariant
// that K=string — callers (gated on Typed.keyIsString) must guarantee that.

func asStringSlice[K comparable](keys []K) []string {
	return *(*[]string)(unsafe.Pointer(&keys))
}

func asKSlice[K comparable](s []string) []K {
	return *(*[]K)(unsafe.Pointer(&s))
}

func asK[K comparable](s string) K {
	return *(*K)(unsafe.Pointer(&s))
}

func asString[K comparable](k K) string {
	return *(*string)(unsafe.Pointer(&k))
}

// stringToBytes / bytesToString alias without copying. The result is read-only;
// codecs treat both Decode input and post-Encode bytes as borrowed.

func stringToBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}
