package redcache

import (
	"context"
	"fmt"
	"time"
)

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
			continue // unexpected; underlying returned a key we did not request
		}
		v, derr := t.valCodec.Decode(stringToBytes(payload))
		if derr != nil {
			return nil, fmt.Errorf("redcache: decode key %q: %w: %w", s, ErrDecode, derr)
		}
		out[k] = v
	}
	return out, nil
}

// encodeMultiResult converts a typed map[K]V from the user's loader into the
// map[string]string that (*CacheAside).GetMulti stores in Redis.
func (t *Typed[K, V]) encodeMultiResult(result map[K]V) (map[string]string, error) {
	out := make(map[string]string, len(result))
	for k, v := range result {
		s, kerr := t.keyCodec.EncodeKey(k)
		if kerr != nil {
			return nil, fmt.Errorf("redcache: encode key: %w", kerr)
		}
		b, eerr := t.valCodec.Encode(v)
		if eerr != nil {
			return nil, fmt.Errorf("redcache: encode value for key %q: %w", s, eerr)
		}
		out[s] = bytesToString(b)
	}
	return out, nil
}
