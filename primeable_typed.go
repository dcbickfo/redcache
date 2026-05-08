package redcache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// PrimeableTyped is a type-safe view over a *PrimeableCacheAside, adding
// Set / ForceSet / SetMulti / ForceSetMulti on top of Typed.
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
	t := NewTyped[K, V](cache.CacheAside, keyCodec, valCodec)
	return &PrimeableTyped[K, V]{Typed: *t, primeable: cache}
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
	if p.keyIsString {
		return p.setMultiString(ctx, ttl, keys, fn)
	}
	return p.setMultiKeyed(ctx, ttl, keys, fn)
}

// setMultiString is the K=string fast path. Aliases keys to []string and skips
// the byEnc reverse-lookup map.
func (p *PrimeableTyped[K, V]) setMultiString(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, keys []K) (map[K]V, error),
) error {
	encKeys := asStringSlice(keys)

	err := p.primeable.SetMulti(ctx, ttl, encKeys, func(ctx context.Context, encArg []string) (map[string]string, error) {
		result, ferr := fn(ctx, asKSlice[K](encArg))
		if ferr != nil {
			return nil, ferr
		}
		return p.encodeMultiResult(result)
	})
	if err == nil {
		return nil
	}
	var be *BatchError
	if !errors.As(err, &be) {
		return err
	}
	return convertBatchErrorToTypedString[K](be)
}

// setMultiKeyed is the general path for non-string keys, building a byEnc map
// to recover K from each encoded key.
func (p *PrimeableTyped[K, V]) setMultiKeyed(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, keys []K) (map[K]V, error),
) error {
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
// successfully encoded entries are still written. On partial Redis failure,
// the per-key results from the upstream *BatchError are merged into the
// returned *BatchKeyError[K].
func (p *PrimeableTyped[K, V]) ForceSetMulti(
	ctx context.Context,
	ttl time.Duration,
	values map[K]V,
) error {
	if len(values) == 0 {
		return nil
	}
	if p.keyIsString {
		return p.forceSetMultiString(ctx, ttl, values)
	}
	return p.forceSetMultiKeyed(ctx, ttl, values)
}

// forceSetMultiString is the K=string fast path. Aliases each K to string and
// skips the byEnc reverse-lookup map.
func (p *PrimeableTyped[K, V]) forceSetMultiString(
	ctx context.Context,
	ttl time.Duration,
	values map[K]V,
) error {
	encVals := make(map[string]string, len(values))
	failed := make(map[K]error)
	for k, v := range values {
		s := asString(k)
		b, err := p.valCodec.Encode(v)
		if err != nil {
			failed[k] = fmt.Errorf("redcache: encode value: %w", err)
			continue
		}
		encVals[s] = bytesToString(b)
	}
	if len(encVals) == 0 {
		return NewBatchKeyError(failed, nil)
	}
	err := p.primeable.ForceSetMulti(ctx, ttl, encVals)
	if err == nil && len(failed) == 0 {
		return nil
	}
	succeeded := mergeForceSetResultString[K](err, encVals, failed)
	return NewBatchKeyError(failed, succeeded)
}

// forceSetMultiKeyed is the general path for non-string keys, building a byEnc
// map to recover K from each encoded key.
func (p *PrimeableTyped[K, V]) forceSetMultiKeyed(
	ctx context.Context,
	ttl time.Duration,
	values map[K]V,
) error {
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
		return NewBatchKeyError(failed, nil)
	}
	err := p.primeable.ForceSetMulti(ctx, ttl, encVals)
	if err == nil && len(failed) == 0 {
		return nil
	}
	succeeded := mergeForceSetResult(err, byEnc, failed)
	return NewBatchKeyError(failed, succeeded)
}

// mergeForceSetResult merges per-key outcomes from (*PrimeableCacheAside).ForceSetMulti
// into failed (encode errors are preserved) and returns the succeeded slice.
// A non-*BatchError err is treated as a total failure.
func mergeForceSetResult[K comparable](err error, byEnc map[string]K, failed map[K]error) []K {
	succeeded := make([]K, 0, len(byEnc))
	if err == nil {
		for _, k := range byEnc {
			succeeded = append(succeeded, k)
		}
		return succeeded
	}
	var be *BatchError
	if !errors.As(err, &be) {
		for _, k := range byEnc {
			failed[k] = err
		}
		return succeeded
	}
	for s, ferr := range be.Failed {
		if k, ok := byEnc[s]; ok {
			failed[k] = ferr
		}
	}
	for _, s := range be.Succeeded {
		if k, ok := byEnc[s]; ok {
			succeeded = append(succeeded, k)
		}
	}
	return succeeded
}

// convertBatchErrorToTyped maps a *BatchError's string keys back to typed K
// using byEnc. Keys not in byEnc are silently skipped — surfacing a partial
// BatchKeyError is safer than panicking on an invariant violation.
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

// convertBatchErrorToTypedString is the K=string fast path: cast each encoded
// key directly to K via asK.
func convertBatchErrorToTypedString[K comparable](be *BatchError) error {
	failedK := make(map[K]error, len(be.Failed))
	for s, ferr := range be.Failed {
		failedK[asK[K](s)] = ferr
	}
	succeededK := make([]K, 0, len(be.Succeeded))
	for _, s := range be.Succeeded {
		succeededK = append(succeededK, asK[K](s))
	}
	return NewBatchKeyError(failedK, succeededK)
}

// mergeForceSetResultString is the K=string fast path of mergeForceSetResult.
// encVals provides the successfully-encoded set (its keys are encoded keys, K
// is the same string).
func mergeForceSetResultString[K comparable](err error, encVals map[string]string, failed map[K]error) []K {
	succeeded := make([]K, 0, len(encVals))
	if err == nil {
		for s := range encVals {
			succeeded = append(succeeded, asK[K](s))
		}
		return succeeded
	}
	var be *BatchError
	if !errors.As(err, &be) {
		for s := range encVals {
			failed[asK[K](s)] = err
		}
		return succeeded
	}
	for s, ferr := range be.Failed {
		failed[asK[K](s)] = ferr
	}
	for _, s := range be.Succeeded {
		succeeded = append(succeeded, asK[K](s))
	}
	return succeeded
}
