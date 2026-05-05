package redcache

import (
	"context"
	"errors"
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

// DelMulti removes multiple keys from Redis, triggering invalidation on all
// clients. See (*CacheAside).DelMulti.
func (t *Typed[K, V]) DelMulti(ctx context.Context, keys ...K) error {
	if len(keys) == 0 {
		return nil
	}
	encKeys := make([]string, len(keys))
	for i, k := range keys {
		s, err := t.keyCodec.EncodeKey(k)
		if err != nil {
			return fmt.Errorf("redcache: encode key: %w", err)
		}
		encKeys[i] = s
	}
	return t.cache.DelMulti(ctx, encKeys...)
}

// TouchMulti sets the TTL of multiple cached values to ttl (this can shorten
// or extend the remaining lifetime). See (*CacheAside).TouchMulti for
// no-op-on-lock and no-op-on-missing-key semantics.
func (t *Typed[K, V]) TouchMulti(ctx context.Context, ttl time.Duration, keys ...K) error {
	if len(keys) == 0 {
		return nil
	}
	encKeys := make([]string, len(keys))
	for i, k := range keys {
		s, err := t.keyCodec.EncodeKey(k)
		if err != nil {
			return fmt.Errorf("redcache: encode key: %w", err)
		}
		encKeys[i] = s
	}
	return t.cache.TouchMulti(ctx, ttl, encKeys...)
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

// SetMulti explicitly populates the cache via fn under write locks.
// See (*PrimeableCacheAside).SetMulti.
//
// On partial failure the returned error wraps a *BatchKeyError[K] (reachable
// via errors.As) listing per-key Failed / Succeeded entries.
func (p *PrimeableTyped[K, V]) SetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, keys []K) (map[K]V, error),
) error {
	if len(keys) == 0 {
		return nil
	}
	encKeys := make([]string, len(keys))
	byEnc := make(map[string]K, len(keys))
	for i, k := range keys {
		s, err := p.keyCodec.EncodeKey(k)
		if err != nil {
			return fmt.Errorf("redcache: encode key: %w", err)
		}
		encKeys[i] = s
		byEnc[s] = k
	}

	err := p.primeable.SetMulti(ctx, ttl, encKeys, func(ctx context.Context, encArg []string) (map[string]string, error) {
		argK := make([]K, len(encArg))
		for i, s := range encArg {
			argK[i] = byEnc[s]
		}
		result, ferr := fn(ctx, argK)
		if ferr != nil {
			return nil, ferr
		}
		return p.encodeMultiResult(result)
	})
	if err == nil {
		return nil
	}

	// Convert *BatchError (string-keyed) to *BatchKeyError[K] using byEnc.
	var be *BatchError
	if !errors.As(err, &be) {
		return err
	}
	return convertBatchErrorToTyped(be, byEnc)
}

// ForceSetMulti unconditionally writes values to Redis. See
// (*PrimeableCacheAside).ForceSetMulti.
//
// Encode failures are collected per-key and returned as *BatchKeyError[K];
// successfully encoded entries are still written.
//
// If the underlying ForceSetMulti returns a non-nil error, the wrapper
// cannot determine which keys succeeded and which failed — every encoded
// key is reported as failed with that same error. If you need per-key
// success/failure beyond encode-time, use Set/SetMulti instead.
func (p *PrimeableTyped[K, V]) ForceSetMulti(
	ctx context.Context,
	ttl time.Duration,
	values map[K]V,
) error {
	if len(values) == 0 {
		return nil
	}
	encVals := make(map[string]string, len(values))
	failed := make(map[K]error)
	byEnc := make(map[string]K, len(values))
	for k, v := range values {
		s, err := p.keyCodec.EncodeKey(k)
		if err != nil {
			failed[k] = fmt.Errorf("redcache: encode key: %w", err)
			continue
		}
		b, err := p.valCodec.Encode(v)
		if err != nil {
			failed[k] = fmt.Errorf("redcache: encode value: %w", err)
			continue
		}
		encVals[s] = bytesToString(b)
		byEnc[s] = k
	}

	if len(encVals) == 0 {
		// Every key failed encoding — return BatchKeyError without touching Redis.
		return NewBatchKeyError(failed, nil)
	}

	err := p.primeable.ForceSetMulti(ctx, ttl, encVals)
	if err == nil && len(failed) == 0 {
		return nil
	}

	// Build per-key result. Underlying ForceSetMulti returns plain error
	// (not *BatchError); on success all encVals were written.
	succeeded := make([]K, 0, len(byEnc))
	if err == nil {
		for _, k := range byEnc {
			succeeded = append(succeeded, k)
		}
	} else {
		// Underlying returned a non-batch error; treat all encVals as failed
		// with the same underlying error so the caller still sees per-key info.
		for _, k := range byEnc {
			failed[k] = err
		}
	}
	return NewBatchKeyError(failed, succeeded)
}

// convertBatchErrorToTyped maps a *BatchError's string keys back to typed K
// using byEnc. Keys not in byEnc are silently skipped: their presence would
// indicate an internal invariant violation, and surfacing a slightly-incomplete
// BatchKeyError is safer than panicking.
func convertBatchErrorToTyped[K comparable](be *BatchError, byEnc map[string]K) error {
	failedK := make(map[K]error, len(be.Failed))
	for s, ferr := range be.Failed {
		if k, ok := byEnc[s]; ok {
			failedK[k] = ferr
		}
	}
	succeededK := make([]K, 0, len(be.Succeeded))
	for _, s := range be.Succeeded {
		if k, ok := byEnc[s]; ok {
			succeededK = append(succeededK, k)
		}
	}
	return NewBatchKeyError(failedK, succeededK)
}
