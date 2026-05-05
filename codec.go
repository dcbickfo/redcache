package redcache

import "encoding/json"

// Codec encodes and decodes typed values to and from byte slices stored
// inside the existing envelope payload.
//
// Implementations must be safe for concurrent use; the library invokes
// Encode and Decode from arbitrary goroutines (callbacks, refresh workers,
// multi-set fan-out).
//
// Slice-ownership contract: the slice returned from Encode is owned by the
// library after return — implementations must not retain or mutate it. The
// slice passed to Decode is owned by the caller (the library) — implementations
// must not retain or mutate it past the Decode call.
type Codec[V any] interface {
	Encode(V) ([]byte, error)
	Decode([]byte) (V, error)
}

// KeyCodec converts a typed key to the Redis key string used to address
// the entry. Implementations must be deterministic and safe for concurrent
// use. The output must be a valid non-empty Redis key (the library does
// not validate further).
type KeyCodec[K any] interface {
	EncodeKey(K) (string, error)
}

// KeyCodecFunc adapts a function value to a KeyCodec[K].
type KeyCodecFunc[K any] func(K) (string, error)

// EncodeKey implements KeyCodec.
func (f KeyCodecFunc[K]) EncodeKey(k K) (string, error) { return f(k) }

// JSONCodec serializes V via encoding/json. The default value codec for
// NewStringTyped / NewPrimeableStringTyped users who do not supply their own.
type JSONCodec[V any] struct{}

// Encode implements Codec.
func (JSONCodec[V]) Encode(v V) ([]byte, error) { return json.Marshal(v) }

// Decode implements Codec.
func (JSONCodec[V]) Decode(b []byte) (V, error) {
	var v V
	if err := json.Unmarshal(b, &v); err != nil {
		var zero V
		return zero, err
	}
	return v, nil
}

// BytesCodec is the identity codec for V = []byte.
type BytesCodec struct{}

// Encode implements Codec.
func (BytesCodec) Encode(v []byte) ([]byte, error) { return v, nil }

// Decode implements Codec.
func (BytesCodec) Decode(b []byte) ([]byte, error) { return b, nil }

// StringCodec is the identity codec for V = string.
type StringCodec struct{}

// Encode implements Codec.
func (StringCodec) Encode(v string) ([]byte, error) { return []byte(v), nil }

// Decode implements Codec.
func (StringCodec) Decode(b []byte) (string, error) { return string(b), nil }

// StringKeyCodec is the identity codec for K = string.
type StringKeyCodec struct{}

// EncodeKey implements KeyCodec.
func (StringKeyCodec) EncodeKey(k string) (string, error) { return k, nil }
