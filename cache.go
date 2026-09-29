package redcache

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"github.com/redis/rueidis"
)

// Conn owns one rueidis client, its invalidation stream, and a lock namespace.
// Every Cache derived from it shares the single client and invalidation
// subscription. Lifecycle stays on the Conn: open it once and close it when
// done with all derived caches.
type Conn struct {
	core engine
}

// engine is the string-typed cache-aside backend. cacheAside implements it over
// Redis; memEngine (OpenMemory) implements it in-process for tests.
type engine interface {
	Client() rueidis.Client
	Close()
	get(ctx context.Context, ttl time.Duration, key string, fn func(context.Context, string) (string, error)) (string, error)
	getMulti(ctx context.Context, ttl time.Duration, keys []string, fn func(context.Context, []string) (map[string]string, error)) (map[string]string, error)
	peek(ctx context.Context, ttl time.Duration, key string) (string, bool, error)
	set(ctx context.Context, ttl time.Duration, key string, fn func(context.Context, string) (string, error)) error
	setMulti(ctx context.Context, ttl time.Duration, keys []string, fn func(context.Context, []string) (map[string]string, error)) error
	forceSet(ctx context.Context, ttl time.Duration, key, value string) error
	forceSetMulti(ctx context.Context, ttl time.Duration, values map[string]string) error
	del(ctx context.Context, key string) error
	delMulti(ctx context.Context, keys ...string) error
	touch(ctx context.Context, ttl time.Duration, key string) error
	touchMulti(ctx context.Context, ttl time.Duration, keys ...string) error
}

// Open builds a Conn with its own rueidis.Client (wired for invalidation).
// Construct caches over it with New, NewKeyed, or NewBytes.
func Open(clientOption rueidis.ClientOption, opts ...Option) (*Conn, error) {
	cfg := newConfig(opts...)
	core, err := newCacheAside(clientOption, cfg)
	if err != nil {
		return nil, err
	}
	return &Conn{core: core}, nil
}

// Close closes the underlying engine and client. Idempotent. It also closes
// every cache constructed over this Conn because they share the client.
func (c *Conn) Close() { c.core.Close() }

// Client returns the underlying rueidis.Client, shared by every cache. It is
// nil for a Conn from OpenMemory.
func (c *Conn) Client() rueidis.Client { return c.core.Client() }

// Cache is a cache-aside handle. It owns one KeyCodec and one value Codec;
// every operation infers its key and value types independently, so a single
// Cache can store different Go types under keys of different Go types. Codec
// compatibility with each operation's K and V is checked by the codecs at call
// time. Lifecycle and raw-client access remain on the owning Conn.
type Cache struct {
	core     engine
	keyCodec KeyCodec
	valCodec Codec
	// keyCodecIsString is set when keyCodec is StringKeyCodec; operations whose
	// K has underlying type string then alias keys instead of encoding them.
	keyCodecIsString bool
	// valCodecIsString is set when valCodec is StringCodec; operations whose V
	// is string then skip the codec and pass the immutable payload through.
	valCodecIsString bool
}

// New constructs a Cache over c with StringKeyCodec and the given value codec.
// It does no I/O. Passing a nil codec panics because it is a programmer error.
func New(c *Conn, valCodec Codec) *Cache {
	return NewKeyed(c, StringKeyCodec{}, valCodec)
}

// NewKeyed is New with an explicit KeyCodec, for key types StringKeyCodec does
// not handle.
func NewKeyed(c *Conn, keyCodec KeyCodec, valCodec Codec) *Cache {
	if keyCodec == nil || valCodec == nil {
		panic("redcache: keyCodec and valCodec must not be nil")
	}
	_, keyCodecIsString := keyCodec.(StringKeyCodec)
	_, valCodecIsString := valCodec.(StringCodec)
	return &Cache{
		core:             c.core,
		keyCodec:         keyCodec,
		valCodec:         valCodec,
		keyCodecIsString: keyCodecIsString,
		valCodecIsString: valCodecIsString,
	}
}

// NewBytes is New with UnsafeBytesCodec preset. Decoded byte slices alias
// borrowed cache memory and must not be mutated or retained after the call.
func NewBytes(c *Conn) *Cache {
	return New(c, UnsafeBytesCodec{})
}

// cache is an operation-scoped typed view over Cache. It encodes K/V and
// delegates to the unexported string-typed engine (*cacheAside).
type cache[K comparable, V any] struct {
	core     engine
	keyCodec KeyCodec
	valCodec Codec
	// keyIsString is set when keyCodec is StringKeyCodec and K's underlying
	// type is string; key paths then alias K↔string instead of encoding.
	keyIsString bool
	// valueIsString is set when valCodec is StringCodec and V is string; value
	// encode/decode then return the immutable string payload without copies.
	valueIsString bool
}

func viewFor[K comparable, V any](c *Cache) cache[K, V] {
	var zero V
	_, vIsString := any(zero).(string)
	return cache[K, V]{
		core:          c.core,
		keyCodec:      c.keyCodec,
		valCodec:      c.valCodec,
		keyIsString:   c.keyCodecIsString && reflect.TypeFor[K]().Kind() == reflect.String,
		valueIsString: c.valCodecIsString && vIsString,
	}
}

// Get returns the cached value for k, calling fn on a miss. Only one caller
// across all processes runs fn for a given key; the rest wait on invalidation.
// V is inferred from fn. Decode errors are wrapped with ErrDecode.
func (c *Cache) Get[K comparable, V any](
	ctx context.Context,
	ttl time.Duration,
	k K,
	fn func(context.Context, K) (V, error),
) (V, error) {
	return viewFor[K, V](c).Get(ctx, ttl, k, fn)
}

// GetMulti returns cached values for keys, calling fn for misses. V is inferred
// from fn. SETs are grouped by Redis cluster slot; a decode error aborts the
// batch and is wrapped with ErrDecode.
func (c *Cache) GetMulti[K comparable, V any](
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(context.Context, []K) (map[K]V, error),
) (map[K]V, error) {
	return viewFor[K, V](c).GetMulti(ctx, ttl, keys, fn)
}

// Peek is a read-only, client-side-cached lookup with no loader and no lock.
// The caller must supply both K and V (as Peek[K, V]) because Peek has no
// value argument from which Go can infer V. It returns (value, true, nil) on a hit, (zero, false, nil) on a miss
// or lock value, and (zero, false, err) on a Redis or decode error. ttl is the
// client-side-cache subscription TTL, like Get.
func (c *Cache) Peek[K comparable, V any](ctx context.Context, ttl time.Duration, k K) (V, bool, error) {
	return viewFor[K, V](c).Peek(ctx, ttl, k)
}

// Set populates k via fn under a write lock, writing the value to every
// subscribed client. V is inferred from fn. On callback error the prior value
// is restored.
func (c *Cache) Set[K comparable, V any](
	ctx context.Context,
	ttl time.Duration,
	k K,
	fn func(context.Context, K) (V, error),
) error {
	return viewFor[K, V](c).Set(ctx, ttl, k, fn)
}

// SetMulti populates keys via fn under write locks. V is inferred from fn.
// Partial failures surface as *BatchKeyError[K] via errors.As.
func (c *Cache) SetMulti[K comparable, V any](
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(context.Context, []K) (map[K]V, error),
) error {
	return viewFor[K, V](c).SetMulti(ctx, ttl, keys, fn)
}

// ForceSet writes v unconditionally, bypassing locks. V is inferred from v.
// In-progress Get callers on the same key retry and observe the new value;
// in-progress Set callers receive ErrLockLost and abandon their pending write.
func (c *Cache) ForceSet[K comparable, V any](ctx context.Context, ttl time.Duration, k K, v V) error {
	return viewFor[K, V](c).ForceSet(ctx, ttl, k, v)
}

// ForceSetMulti writes values unconditionally. K and V are inferred from
// values; pass them explicitly when values is an untyped nil map.
// Encode failures are collected per key and successful entries are still
// written; partial failures surface as *BatchKeyError[K].
func (c *Cache) ForceSetMulti[K comparable, V any](ctx context.Context, ttl time.Duration, values map[K]V) error {
	return viewFor[K, V](c).ForceSetMulti(ctx, ttl, values)
}

// Del removes k and triggers invalidation on subscribed clients.
func (c *Cache) Del[K comparable](ctx context.Context, k K) error {
	return viewFor[K, struct{}](c).Del(ctx, k)
}

// DelMulti removes keys and triggers invalidation on subscribed clients.
func (c *Cache) DelMulti[K comparable](ctx context.Context, keys []K) error {
	return viewFor[K, struct{}](c).DelMulti(ctx, keys)
}

// Touch updates the TTL of a cached value. It is a no-op for a missing key or
// lock value. Clients caching the key are invalidated and re-fetch it with the
// new TTL on their next read.
func (c *Cache) Touch[K comparable](ctx context.Context, ttl time.Duration, k K) error {
	return viewFor[K, struct{}](c).Touch(ctx, ttl, k)
}

// TouchMulti updates the TTL of cached values, with the same behavior as Touch.
func (c *Cache) TouchMulti[K comparable](ctx context.Context, ttl time.Duration, keys []K) error {
	return viewFor[K, struct{}](c).TouchMulti(ctx, ttl, keys)
}

func validateTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	return nil
}

func (c cache[K, V]) encodeKey(k K) (string, error) {
	if c.keyIsString {
		return asString(k), nil
	}
	return c.keyCodec.EncodeKey(k)
}

func (c cache[K, V]) encodeValue(v V) (string, error) {
	if c.valueIsString {
		return any(v).(string), nil
	}
	b, err := c.valCodec.Encode(v)
	if err != nil {
		return "", err
	}
	return bytesToString(b), nil
}

func (c cache[K, V]) decodeValue(payload string) (V, error) {
	if c.valueIsString {
		return any(payload).(V), nil
	}
	var v V
	if err := c.valCodec.Decode(stringToBytes(payload), &v); err != nil {
		var zero V
		return zero, err
	}
	return v, nil
}

// Get returns the cached value for k, calling fn on a miss. Decode errors on
// read are wrapped with ErrDecode and leave the cached entry intact.
func (c cache[K, V]) Get(
	ctx context.Context,
	ttl time.Duration,
	k K,
	fn func(ctx context.Context, k K) (V, error),
) (V, error) {
	var zero V
	if err := validateTTL(ttl); err != nil {
		return zero, err
	}
	encKey, err := c.encodeKey(k)
	if err != nil {
		return zero, fmt.Errorf("redcache: encode key: %w", err)
	}

	raw, err := c.core.get(ctx, ttl, encKey, func(ctx context.Context, _ string) (string, error) {
		v, ferr := fn(ctx, k)
		if ferr != nil {
			return "", ferr
		}
		enc, eerr := c.encodeValue(v)
		if eerr != nil {
			return "", fmt.Errorf("redcache: encode value: %w", eerr)
		}
		return enc, nil
	})
	if err != nil {
		return zero, err
	}

	v, derr := c.decodeValue(raw)
	if derr != nil {
		return zero, fmt.Errorf("redcache: decode key %q: %w: %w", encKey, ErrDecode, derr)
	}
	return v, nil
}

// Peek is a read-only, client-side-cached lookup with no loader and no lock.
// Returns (value, true, nil) on a cached hit, (zero, false, nil) on a miss or a
// lock value, and (zero, false, err) on a real Redis or decode error. Decode
// errors are wrapped with ErrDecode like Get.
func (c cache[K, V]) Peek(ctx context.Context, ttl time.Duration, k K) (V, bool, error) {
	var zero V
	if err := validateTTL(ttl); err != nil {
		return zero, false, err
	}
	encKey, err := c.encodeKey(k)
	if err != nil {
		return zero, false, fmt.Errorf("redcache: encode key: %w", err)
	}

	raw, ok, err := c.core.peek(ctx, ttl, encKey)
	if err != nil {
		return zero, false, err
	}
	if !ok {
		return zero, false, nil
	}

	v, derr := c.decodeValue(raw)
	if derr != nil {
		return zero, false, fmt.Errorf("redcache: decode key %q: %w: %w", encKey, ErrDecode, derr)
	}
	return v, true, nil
}

// Del removes a key, triggering invalidation on all subscribed clients.
func (c cache[K, V]) Del(ctx context.Context, k K) error {
	encKey, err := c.encodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	return c.core.del(ctx, encKey)
}

// Touch sets the TTL of a cached value.
func (c cache[K, V]) Touch(ctx context.Context, ttl time.Duration, k K) error {
	if err := validateTTL(ttl); err != nil {
		return err
	}
	encKey, err := c.encodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	return c.core.touch(ctx, ttl, encKey)
}

// GetMulti returns cached values for keys, calling fn for misses. A decode
// error on any read returns wrapped with ErrDecode and aborts the batch.
func (c cache[K, V]) GetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, missing []K) (map[K]V, error),
) (map[K]V, error) {
	if err := validateTTL(ttl); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return map[K]V{}, nil
	}
	dst := make(map[K]V, len(keys))
	if c.keyIsString {
		return c.getMultiStringInto(ctx, ttl, keys, nil, dst, fn)
	}
	return c.getMultiKeyedInto(ctx, ttl, keys, dst, fn)
}

// K=string fast path: aliases keys to []string, skips the reverse-lookup map.
func (c cache[K, V]) getMultiStringInto(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	encKeys []string,
	dst map[K]V,
	fn func(ctx context.Context, missing []K) (map[K]V, error),
) (map[K]V, error) {
	if encKeys == nil {
		encKeys = asStringSlice(keys)
	}

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

	for s, payload := range raw {
		v, derr := c.decodeValue(payload)
		if derr != nil {
			return nil, fmt.Errorf("redcache: decode key %q: %w: %w", s, ErrDecode, derr)
		}
		dst[asK[K](s)] = v
	}
	return dst, nil
}

func (c cache[K, V]) getMultiKeyedInto(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	dst map[K]V,
	fn func(ctx context.Context, missing []K) (map[K]V, error),
) (map[K]V, error) {
	encKeys, byEnc, err := c.encodeKeysWithLookup(keys)
	if err != nil {
		return nil, err
	}
	return c.getMultiEncodedInto(ctx, ttl, encKeys, byEnc, dst, fn)
}

func (c cache[K, V]) getMultiEncodedInto(
	ctx context.Context,
	ttl time.Duration,
	encKeys []string,
	byEnc map[string]K,
	dst map[K]V,
	fn func(ctx context.Context, missing []K) (map[K]V, error),
) (map[K]V, error) {
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

	for s, payload := range raw {
		k, ok := byEnc[s]
		if !ok {
			continue
		}
		v, derr := c.decodeValue(payload)
		if derr != nil {
			return nil, fmt.Errorf("redcache: decode key %q: %w: %w", s, ErrDecode, derr)
		}
		dst[k] = v
	}
	return dst, nil
}

// DelMulti removes keys, triggering invalidation.
func (c cache[K, V]) DelMulti(ctx context.Context, keys []K) error {
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
func (c cache[K, V]) TouchMulti(ctx context.Context, ttl time.Duration, keys []K) error {
	if err := validateTTL(ttl); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	encKeys, err := c.encodeKeys(keys)
	if err != nil {
		return err
	}
	return c.core.touchMulti(ctx, ttl, encKeys...)
}

func (c cache[K, V]) encodeKeys(keys []K) ([]string, error) {
	if c.keyIsString {
		return asStringSlice(keys), nil
	}
	encKeys, _, err := c.encodeKeysWithLookup(keys)
	return encKeys, err
}

func (c cache[K, V]) encodeKeysWithLookup(keys []K) ([]string, map[string]K, error) {
	encKeys := make([]string, len(keys))
	byEnc := make(map[string]K, len(keys))
	for i, k := range keys {
		s, err := c.encodeKey(k)
		if err != nil {
			return nil, nil, fmt.Errorf("redcache: encode key: %w", err)
		}
		if prev, ok := byEnc[s]; ok && prev != k {
			return nil, nil, duplicateEncodedKeyError(s, prev, k)
		}
		encKeys[i] = s
		byEnc[s] = k
	}
	return encKeys, byEnc, nil
}

func (c cache[K, V]) encodeMultiResult(result map[K]V) (map[string]string, error) {
	out := make(map[string]string, len(result))
	var byEnc map[string]K
	if !c.keyIsString {
		byEnc = make(map[string]K, len(result))
	}
	for k, v := range result {
		var s string
		if c.keyIsString {
			s = asString(k)
		} else {
			ks, kerr := c.encodeKey(k)
			if kerr != nil {
				return nil, fmt.Errorf("redcache: encode key: %w", kerr)
			}
			s = ks
			if prev, ok := byEnc[s]; ok && prev != k {
				return nil, duplicateEncodedKeyError(s, prev, k)
			}
			byEnc[s] = k
		}
		enc, eerr := c.encodeValue(v)
		if eerr != nil {
			return nil, fmt.Errorf("redcache: encode value for key %q: %w", s, eerr)
		}
		out[s] = enc
	}
	return out, nil
}

// Set populates the cache via fn under a write lock.
func (c cache[K, V]) Set(
	ctx context.Context,
	ttl time.Duration,
	k K,
	fn func(ctx context.Context, k K) (V, error),
) error {
	if err := validateTTL(ttl); err != nil {
		return err
	}
	encKey, err := c.encodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	return c.core.set(ctx, ttl, encKey, func(ctx context.Context, _ string) (string, error) {
		v, ferr := fn(ctx, k)
		if ferr != nil {
			return "", ferr
		}
		enc, eerr := c.encodeValue(v)
		if eerr != nil {
			return "", fmt.Errorf("redcache: encode value: %w", eerr)
		}
		return enc, nil
	})
}

// ForceSet writes v unconditionally.
func (c cache[K, V]) ForceSet(ctx context.Context, ttl time.Duration, k K, v V) error {
	if err := validateTTL(ttl); err != nil {
		return err
	}
	encKey, err := c.encodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	enc, err := c.encodeValue(v)
	if err != nil {
		return fmt.Errorf("redcache: encode value: %w", err)
	}
	return c.core.forceSet(ctx, ttl, encKey, enc)
}

// SetMulti populates the cache via fn under write locks. Partial failures
// surface as *BatchKeyError[K].
func (c cache[K, V]) SetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, keys []K) (map[K]V, error),
) error {
	if err := validateTTL(ttl); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	if c.keyIsString {
		return c.setMultiString(ctx, ttl, keys, fn)
	}
	return c.setMultiKeyed(ctx, ttl, keys, fn)
}

// K=string fast path: aliases keys to []string, skips the reverse-lookup map.
func (c cache[K, V]) setMultiString(
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
	be, ok := errors.AsType[*batchError](err)
	if !ok {
		return err
	}
	return convertBatchErrorToTypedString[K](be)
}

func (c cache[K, V]) setMultiKeyed(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, keys []K) (map[K]V, error),
) error {
	encKeys, byEnc, err := c.encodeKeysWithLookup(keys)
	if err != nil {
		return err
	}

	err = c.core.setMulti(ctx, ttl, encKeys, func(ctx context.Context, encArg []string) (map[string]string, error) {
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
	be, ok := errors.AsType[*batchError](err)
	if !ok {
		return err
	}
	return convertBatchErrorToTyped(be, byEnc)
}

// ForceSetMulti writes values unconditionally. Encode failures are collected
// per-key; successfully-encoded entries are still written. Partial failures
// (encode or write) surface as *BatchKeyError[K].
func (c cache[K, V]) ForceSetMulti(
	ctx context.Context,
	ttl time.Duration,
	values map[K]V,
) error {
	if err := validateTTL(ttl); err != nil {
		return err
	}
	if len(values) == 0 {
		return nil
	}
	if c.keyIsString {
		return c.forceSetMultiString(ctx, ttl, values)
	}
	return c.forceSetMultiKeyed(ctx, ttl, values)
}

// K=string fast path: aliases each K to string, skips the reverse-lookup map.
func (c cache[K, V]) forceSetMultiString(
	ctx context.Context,
	ttl time.Duration,
	values map[K]V,
) error {
	encVals := make(map[string]string, len(values))
	failed := make(map[K]error)
	for k, v := range values {
		s := asString(k)
		enc, err := c.encodeValue(v)
		if err != nil {
			failed[k] = fmt.Errorf("redcache: encode value: %w", err)
			continue
		}
		encVals[s] = enc
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

func (c cache[K, V]) forceSetMultiKeyed(
	ctx context.Context,
	ttl time.Duration,
	values map[K]V,
) error {
	encVals := make(map[string]string, len(values))
	failed := make(map[K]error)
	seen := make(map[string]K, len(values))
	byEnc := make(map[string]K, len(values))
	duplicate := false
	for k, v := range values {
		s, err := c.encodeKey(k)
		if err != nil {
			failed[k] = fmt.Errorf("redcache: encode key: %w", err)
			continue
		}
		if prev, ok := seen[s]; ok && prev != k {
			err := duplicateEncodedKeyError(s, prev, k)
			failed[prev] = err
			failed[k] = err
			delete(encVals, s)
			delete(byEnc, s)
			duplicate = true
			continue
		}
		seen[s] = k
		enc, err := c.encodeValue(v)
		if err != nil {
			failed[k] = fmt.Errorf("redcache: encode value: %w", err)
			continue
		}
		encVals[s] = enc
		byEnc[s] = k
	}
	if duplicate {
		return newBatchKeyError(failed, nil)
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
	be, ok := errors.AsType[*batchError](err)
	if !ok {
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

func duplicateEncodedKeyError[K comparable](encoded string, first K, second K) error {
	return fmt.Errorf("redcache: encode key: duplicate encoded key %q for keys %v and %v", encoded, first, second)
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
	be, ok := errors.AsType[*batchError](err)
	if !ok {
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
		return []byte{}
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}
