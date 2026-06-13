package redcache

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/redis/rueidis"
)

// Cache is the primary cache-aside surface: a generic interface over a key type
// K and value type V. It mirrors rueidis.Client in being an interface so callers
// can fake it in tests. All read methods run the stampede-protected lock loop;
// all write methods populate every subscribed client's cache.
type Cache[K comparable, V any] interface {
	// Get returns the cached value for k, calling fn on a miss. Only one caller
	// across all processes runs fn for a given key; the rest wait on the
	// resulting invalidation. Decode errors on read are wrapped with ErrDecode.
	Get(ctx context.Context, ttl time.Duration, k K, fn func(context.Context, K) (V, error)) (V, error)
	// GetMulti returns cached values for keys, calling fn for misses. SETs are
	// grouped by Redis cluster slot. A decode error aborts the batch (wrapped
	// with ErrDecode).
	GetMulti(ctx context.Context, ttl time.Duration, keys []K, fn func(context.Context, []K) (map[K]V, error)) (map[K]V, error)
	// Set populates k via fn under a write lock, writing the value to every
	// subscribed client. On callback error the prior value is restored.
	Set(ctx context.Context, ttl time.Duration, k K, fn func(context.Context, K) (V, error)) error
	// SetMulti populates keys via fn under write locks. Partial failures surface
	// as *BatchKeyError[K] via errors.As.
	SetMulti(ctx context.Context, ttl time.Duration, keys []K, fn func(context.Context, []K) (map[K]V, error)) error
	// ForceSet writes v unconditionally, bypassing locks. In-progress Get/Set
	// callers on the same key see ErrLockLost and retry.
	ForceSet(ctx context.Context, ttl time.Duration, k K, v V) error
	// ForceSetMulti writes values unconditionally. Encode failures are collected
	// per-key; successfully-encoded entries are still written. Partial failures
	// surface as *BatchKeyError[K].
	ForceSetMulti(ctx context.Context, ttl time.Duration, values map[K]V) error
	// Del removes a key, triggering invalidation on all subscribed clients.
	Del(ctx context.Context, k K) error
	// DelMulti removes keys, triggering invalidation.
	DelMulti(ctx context.Context, keys ...K) error
	// Touch sets the TTL of a cached value. No-ops on a missing key or lock value.
	Touch(ctx context.Context, ttl time.Duration, k K) error
	// TouchMulti extends the TTL of cached values.
	TouchMulti(ctx context.Context, ttl time.Duration, keys ...K) error
	// Client returns the underlying rueidis.Client. Bypasses cache-aside
	// semantics; do NOT raw-SET cached keys (it skips the envelope).
	Client() rueidis.Client
	// Close cancels pending lock entries, drains refresh workers, and closes the
	// underlying client. Idempotent.
	Close()
}

// cache is the concrete generic implementation of Cache[K, V]. It encodes K/V
// and delegates to the unexported string-typed engine (*cacheAside). One engine
// may back many cache views with different K/V and codecs (see View).
type cache[K comparable, V any] struct {
	core     *cacheAside
	keyCodec KeyCodec[K]
	valCodec Codec[V]
	// keyIsString is set when keyCodec is StringKeyCodec; multi-key paths then
	// alias []K↔[]string instead of building a reverse-lookup map.
	keyIsString bool
}

var _ Cache[string, []byte] = (*cache[string, []byte])(nil)

// New builds a self-contained Cache[K, V] with its own rueidis.Client (wired for
// invalidation). keyCodec maps K to the Redis key; valCodec maps V to the
// envelope payload.
func New[K comparable, V any](
	clientOption rueidis.ClientOption,
	keyCodec KeyCodec[K],
	valCodec Codec[V],
	opts ...Option,
) (Cache[K, V], error) {
	if keyCodec == nil || valCodec == nil {
		return nil, errors.New("redcache: keyCodec and valCodec must not be nil")
	}
	cfg := newConfig(opts...)
	core, err := newCacheAside(clientOption, cfg)
	if err != nil {
		return nil, err
	}
	return &cache[K, V]{
		core:        core,
		keyCodec:    keyCodec,
		valCodec:    valCodec,
		keyIsString: isStringKeyCodec[K](keyCodec),
	}, nil
}

// NewString is New[string, V] with StringKeyCodec preset (enabling the K=string
// fast path).
func NewString[V any](
	clientOption rueidis.ClientOption,
	valCodec Codec[V],
	opts ...Option,
) (Cache[string, V], error) {
	return New[string, V](clientOption, StringKeyCodec{}, valCodec, opts...)
}

// NewBytes is NewString[[]byte] with UnsafeBytesCodec — a zero-copy raw []byte
// cache. The decoded slice aliases borrowed memory; do not mutate or retain it.
func NewBytes(clientOption rueidis.ClientOption, opts ...Option) (Cache[string, []byte], error) {
	return NewString[[]byte](clientOption, UnsafeBytesCodec{}, opts...)
}

// View derives a sibling typed cache that shares parent's client, connection,
// and invalidation stream, with its own key/value types and codecs. Use it to
// cache multiple value types over a single Redis connection.
//
// parent must be a Cache built by New, NewString, or NewBytes; View returns an
// error for any other value (e.g. a third-party or test implementation), since
// only redcache's own caches expose a shareable engine. Closing any view — or
// the parent — closes the shared client, so derive views from one long-lived
// parent and close it last.
func View[K comparable, V any](
	parent any,
	keyCodec KeyCodec[K],
	valCodec Codec[V],
) (Cache[K, V], error) {
	if keyCodec == nil || valCodec == nil {
		return nil, errors.New("redcache: keyCodec and valCodec must not be nil")
	}
	shared, ok := parent.(interface{ engine() *cacheAside })
	if !ok {
		return nil, errors.New("redcache: View requires a *Cache built by New, NewString, or NewBytes")
	}
	return &cache[K, V]{
		core:        shared.engine(),
		keyCodec:    keyCodec,
		valCodec:    valCodec,
		keyIsString: isStringKeyCodec[K](keyCodec),
	}, nil
}

// engine exposes the shared *cacheAside for View. Unexported so it stays a
// power-user seam rather than public surface.
func (c *cache[K, V]) engine() *cacheAside { return c.core }

// isStringKeyCodec reports whether keyCodec is StringKeyCodec, which guarantees
// K=string and so gates the unsafe []K↔[]string fast path.
func isStringKeyCodec[K comparable](keyCodec KeyCodec[K]) bool {
	_, ok := any(keyCodec).(StringKeyCodec)
	return ok
}

// Client returns the underlying rueidis.Client.
func (c *cache[K, V]) Client() rueidis.Client { return c.core.Client() }

// Close closes the underlying engine and client.
func (c *cache[K, V]) Close() { c.core.Close() }

// Get returns the cached value for k, calling fn on a miss. Decode errors on
// read are wrapped with ErrDecode and leave the cached entry intact.
func (c *cache[K, V]) Get(
	ctx context.Context,
	ttl time.Duration,
	k K,
	fn func(ctx context.Context, k K) (V, error),
) (V, error) {
	var zero V
	encKey, err := c.keyCodec.EncodeKey(k)
	if err != nil {
		return zero, fmt.Errorf("redcache: encode key: %w", err)
	}

	raw, err := c.core.get(ctx, ttl, encKey, func(ctx context.Context, _ string) (string, error) {
		v, ferr := fn(ctx, k)
		if ferr != nil {
			return "", ferr
		}
		b, eerr := c.valCodec.Encode(v)
		if eerr != nil {
			return "", fmt.Errorf("redcache: encode value: %w", eerr)
		}
		return bytesToString(b), nil
	})
	if err != nil {
		return zero, err
	}

	v, derr := c.valCodec.Decode(stringToBytes(raw))
	if derr != nil {
		return zero, fmt.Errorf("redcache: decode key %q: %w: %w", encKey, ErrDecode, derr)
	}
	return v, nil
}

// Del removes a key, triggering invalidation on all subscribed clients.
func (c *cache[K, V]) Del(ctx context.Context, k K) error {
	encKey, err := c.keyCodec.EncodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	return c.core.del(ctx, encKey)
}

// Touch sets the TTL of a cached value.
func (c *cache[K, V]) Touch(ctx context.Context, ttl time.Duration, k K) error {
	encKey, err := c.keyCodec.EncodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	return c.core.touch(ctx, ttl, encKey)
}

// GetMulti returns cached values for keys, calling fn for misses. A decode
// error on any read returns wrapped with ErrDecode and aborts the batch.
func (c *cache[K, V]) GetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, missing []K) (map[K]V, error),
) (map[K]V, error) {
	if len(keys) == 0 {
		return map[K]V{}, nil
	}
	if c.keyIsString {
		return c.getMultiString(ctx, ttl, keys, fn)
	}
	return c.getMultiKeyed(ctx, ttl, keys, fn)
}

// K=string fast path: aliases keys to []string, skips the reverse-lookup map.
func (c *cache[K, V]) getMultiString(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, missing []K) (map[K]V, error),
) (map[K]V, error) {
	encKeys := asStringSlice(keys)

	raw, err := c.core.getMulti(ctx, ttl, encKeys, func(ctx context.Context, missingEnc []string) (map[string]string, error) {
		result, ferr := fn(ctx, asKSlice[K](missingEnc))
		if ferr != nil {
			return nil, ferr
		}
		return c.encodeMultiResult(result)
	})
	if err != nil {
		return nil, err
	}

	out := make(map[K]V, len(raw))
	for s, payload := range raw {
		v, derr := c.valCodec.Decode(stringToBytes(payload))
		if derr != nil {
			return nil, fmt.Errorf("redcache: decode key %q: %w: %w", s, ErrDecode, derr)
		}
		out[asK[K](s)] = v
	}
	return out, nil
}

func (c *cache[K, V]) getMultiKeyed(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, missing []K) (map[K]V, error),
) (map[K]V, error) {
	encKeys := make([]string, len(keys))
	byEnc := make(map[string]K, len(keys))
	for i, k := range keys {
		s, err := c.keyCodec.EncodeKey(k)
		if err != nil {
			return nil, fmt.Errorf("redcache: encode key: %w", err)
		}
		encKeys[i] = s
		byEnc[s] = k
	}

	raw, err := c.core.getMulti(ctx, ttl, encKeys, func(ctx context.Context, missingEnc []string) (map[string]string, error) {
		missingK := make([]K, len(missingEnc))
		for i, s := range missingEnc {
			missingK[i] = byEnc[s]
		}
		result, ferr := fn(ctx, missingK)
		if ferr != nil {
			return nil, ferr
		}
		return c.encodeMultiResult(result)
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
		v, derr := c.valCodec.Decode(stringToBytes(payload))
		if derr != nil {
			return nil, fmt.Errorf("redcache: decode key %q: %w: %w", s, ErrDecode, derr)
		}
		out[k] = v
	}
	return out, nil
}

// DelMulti removes keys, triggering invalidation.
func (c *cache[K, V]) DelMulti(ctx context.Context, keys ...K) error {
	if len(keys) == 0 {
		return nil
	}
	encKeys, err := c.encodeKeys(keys)
	if err != nil {
		return err
	}
	return c.core.delMulti(ctx, encKeys...)
}

// TouchMulti extends the TTL of cached values.
func (c *cache[K, V]) TouchMulti(ctx context.Context, ttl time.Duration, keys ...K) error {
	if len(keys) == 0 {
		return nil
	}
	encKeys, err := c.encodeKeys(keys)
	if err != nil {
		return err
	}
	return c.core.touchMulti(ctx, ttl, encKeys...)
}

func (c *cache[K, V]) encodeKeys(keys []K) ([]string, error) {
	if c.keyIsString {
		return asStringSlice(keys), nil
	}
	encKeys := make([]string, len(keys))
	for i, k := range keys {
		s, err := c.keyCodec.EncodeKey(k)
		if err != nil {
			return nil, fmt.Errorf("redcache: encode key: %w", err)
		}
		encKeys[i] = s
	}
	return encKeys, nil
}

func (c *cache[K, V]) encodeMultiResult(result map[K]V) (map[string]string, error) {
	out := make(map[string]string, len(result))
	for k, v := range result {
		var s string
		if c.keyIsString {
			s = asString(k)
		} else {
			ks, kerr := c.keyCodec.EncodeKey(k)
			if kerr != nil {
				return nil, fmt.Errorf("redcache: encode key: %w", kerr)
			}
			s = ks
		}
		b, eerr := c.valCodec.Encode(v)
		if eerr != nil {
			return nil, fmt.Errorf("redcache: encode value for key %q: %w", s, eerr)
		}
		out[s] = bytesToString(b)
	}
	return out, nil
}

// Set populates the cache via fn under a write lock.
func (c *cache[K, V]) Set(
	ctx context.Context,
	ttl time.Duration,
	k K,
	fn func(ctx context.Context, k K) (V, error),
) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	encKey, err := c.keyCodec.EncodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	return c.core.set(ctx, ttl, encKey, func(ctx context.Context, _ string) (string, error) {
		v, ferr := fn(ctx, k)
		if ferr != nil {
			return "", ferr
		}
		b, eerr := c.valCodec.Encode(v)
		if eerr != nil {
			return "", fmt.Errorf("redcache: encode value: %w", eerr)
		}
		return bytesToString(b), nil
	})
}

// ForceSet writes v unconditionally.
func (c *cache[K, V]) ForceSet(ctx context.Context, ttl time.Duration, k K, v V) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	encKey, err := c.keyCodec.EncodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	b, err := c.valCodec.Encode(v)
	if err != nil {
		return fmt.Errorf("redcache: encode value: %w", err)
	}
	return c.core.forceSet(ctx, ttl, encKey, bytesToString(b))
}

// SetMulti populates the cache via fn under write locks. Partial failures
// surface as *BatchKeyError[K].
func (c *cache[K, V]) SetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, keys []K) (map[K]V, error),
) error {
	if len(keys) == 0 {
		return nil
	}
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	if c.keyIsString {
		return c.setMultiString(ctx, ttl, keys, fn)
	}
	return c.setMultiKeyed(ctx, ttl, keys, fn)
}

// K=string fast path: aliases keys to []string, skips the reverse-lookup map.
func (c *cache[K, V]) setMultiString(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, keys []K) (map[K]V, error),
) error {
	encKeys := asStringSlice(keys)

	err := c.core.setMulti(ctx, ttl, encKeys, func(ctx context.Context, encArg []string) (map[string]string, error) {
		result, ferr := fn(ctx, asKSlice[K](encArg))
		if ferr != nil {
			return nil, ferr
		}
		return c.encodeMultiResult(result)
	})
	if err == nil {
		return nil
	}
	var be *batchError
	if !errors.As(err, &be) {
		return err
	}
	return convertBatchErrorToTypedString[K](be)
}

func (c *cache[K, V]) setMultiKeyed(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, keys []K) (map[K]V, error),
) error {
	encKeys := make([]string, len(keys))
	byEnc := make(map[string]K, len(keys))
	for i, k := range keys {
		s, err := c.keyCodec.EncodeKey(k)
		if err != nil {
			return fmt.Errorf("redcache: encode key: %w", err)
		}
		encKeys[i] = s
		byEnc[s] = k
	}

	err := c.core.setMulti(ctx, ttl, encKeys, func(ctx context.Context, encArg []string) (map[string]string, error) {
		argK := make([]K, len(encArg))
		for i, s := range encArg {
			argK[i] = byEnc[s]
		}
		result, ferr := fn(ctx, argK)
		if ferr != nil {
			return nil, ferr
		}
		return c.encodeMultiResult(result)
	})
	if err == nil {
		return nil
	}
	var be *batchError
	if !errors.As(err, &be) {
		return err
	}
	return convertBatchErrorToTyped(be, byEnc)
}

// ForceSetMulti writes values unconditionally. Encode failures are collected
// per-key; successfully-encoded entries are still written. Partial failures
// (encode or write) surface as *BatchKeyError[K].
func (c *cache[K, V]) ForceSetMulti(
	ctx context.Context,
	ttl time.Duration,
	values map[K]V,
) error {
	if len(values) == 0 {
		return nil
	}
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	if c.keyIsString {
		return c.forceSetMultiString(ctx, ttl, values)
	}
	return c.forceSetMultiKeyed(ctx, ttl, values)
}

// K=string fast path: aliases each K to string, skips the reverse-lookup map.
func (c *cache[K, V]) forceSetMultiString(
	ctx context.Context,
	ttl time.Duration,
	values map[K]V,
) error {
	encVals := make(map[string]string, len(values))
	failed := make(map[K]error)
	for k, v := range values {
		s := asString(k)
		b, err := c.valCodec.Encode(v)
		if err != nil {
			failed[k] = fmt.Errorf("redcache: encode value: %w", err)
			continue
		}
		encVals[s] = bytesToString(b)
	}
	if len(encVals) == 0 {
		return newBatchKeyError(failed, nil)
	}
	err := c.core.forceSetMulti(ctx, ttl, encVals)
	if err == nil && len(failed) == 0 {
		return nil
	}
	succeeded := mergeForceSetResultString[K](err, encVals, failed)
	return newBatchKeyError(failed, succeeded)
}

func (c *cache[K, V]) forceSetMultiKeyed(
	ctx context.Context,
	ttl time.Duration,
	values map[K]V,
) error {
	encVals := make(map[string]string, len(values))
	failed := make(map[K]error)
	byEnc := make(map[string]K, len(values))
	for k, v := range values {
		s, err := c.keyCodec.EncodeKey(k)
		if err != nil {
			failed[k] = fmt.Errorf("redcache: encode key: %w", err)
			continue
		}
		b, err := c.valCodec.Encode(v)
		if err != nil {
			failed[k] = fmt.Errorf("redcache: encode value: %w", err)
			continue
		}
		encVals[s] = bytesToString(b)
		byEnc[s] = k
	}
	if len(encVals) == 0 {
		return newBatchKeyError(failed, nil)
	}
	err := c.core.forceSetMulti(ctx, ttl, encVals)
	if err == nil && len(failed) == 0 {
		return nil
	}
	succeeded := mergeForceSetResult(err, byEnc, failed)
	return newBatchKeyError(failed, succeeded)
}

// mergeForceSetResult merges per-key outcomes from forceSetMulti into failed
// (encode errors are preserved) and returns the succeeded slice. A non-*batchError
// err is treated as a total failure.
func mergeForceSetResult[K comparable](err error, byEnc map[string]K, failed map[K]error) []K {
	succeeded := make([]K, 0, len(byEnc))
	if err == nil {
		for _, k := range byEnc {
			succeeded = append(succeeded, k)
		}
		return succeeded
	}
	var be *batchError
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

// convertBatchErrorToTyped maps a *batchError's string keys back to typed K
// using byEnc. Keys not in byEnc are silently skipped — surfacing a partial
// BatchKeyError is safer than panicking on an invariant violation.
func convertBatchErrorToTyped[K comparable](be *batchError, byEnc map[string]K) error {
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
	return newBatchKeyError(failedK, succeededK)
}

// convertBatchErrorToTypedString is the K=string fast path: cast each encoded
// key directly to K via asK.
func convertBatchErrorToTypedString[K comparable](be *batchError) error {
	failedK := make(map[K]error, len(be.Failed))
	for s, ferr := range be.Failed {
		failedK[asK[K](s)] = ferr
	}
	succeededK := make([]K, 0, len(be.Succeeded))
	for _, s := range be.Succeeded {
		succeededK = append(succeededK, asK[K](s))
	}
	return newBatchKeyError(failedK, succeededK)
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
	var be *batchError
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

// The asK / asString family aliases between K and string under the invariant
// that K=string — callers (gated on cache.keyIsString) must guarantee that.

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
