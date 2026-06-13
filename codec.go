package redcache

import "encoding/json"

// Codec encodes and decodes V into the envelope payload. Implementations must
// be concurrent-safe.
//
// Ownership of return values:
//   - Encode's returned slice is handed to the library and must not be mutated
//     by the caller afterward. The library may alias it without copying, so the
//     codec must not retain or later modify it either.
//   - Decode's input slice is borrowed from library-internal memory and is only
//     valid for the duration of the call; it must not be retained.
//
// Identity codecs (UnsafeBytesCodec) alias this borrowed memory directly and so
// trade safety for zero copies — the decoded []byte must not outlive the call or
// be mutated. JSON/string codecs (JSONCodec, StringCodec) instead return fresh,
// caller-owned copies and are safe to retain.
type Codec[V any] interface {
	Encode(V) ([]byte, error)
	Decode([]byte) (V, error)
}

// KeyCodec encodes a typed key K into the Redis key string. Must be
// deterministic, concurrent-safe, and produce a non-empty key.
type KeyCodec[K any] interface {
	EncodeKey(K) (string, error)
}

// KeyCodecFunc adapts a func to KeyCodec[K].
type KeyCodecFunc[K any] func(K) (string, error)

// EncodeKey calls f.
func (f KeyCodecFunc[K]) EncodeKey(k K) (string, error) { return f(k) }

// JSONCodec encodes V via encoding/json.
type JSONCodec[V any] struct{}

// Encode marshals v to JSON.
func (JSONCodec[V]) Encode(v V) ([]byte, error) { return json.Marshal(v) }

// Decode unmarshals b into V.
func (JSONCodec[V]) Decode(b []byte) (V, error) {
	var v V
	if err := json.Unmarshal(b, &v); err != nil {
		var zero V
		return zero, err
	}
	return v, nil
}

// UnsafeBytesCodec is the zero-copy identity codec for []byte. It aliases
// library-internal memory in both directions: Encode hands its input straight
// to the library (which may alias it), and Decode returns a []byte backed by
// the cache's borrowed read buffer. The decoded slice must not be mutated or
// retained past the call. Use a copying codec if you need an owned value.
type UnsafeBytesCodec struct{}

// Encode returns v unchanged (no copy).
func (UnsafeBytesCodec) Encode(v []byte) ([]byte, error) { return v, nil }

// Decode returns b unchanged (no copy); the result aliases borrowed memory.
func (UnsafeBytesCodec) Decode(b []byte) ([]byte, error) { return b, nil }

// StringCodec is the identity codec for string.
type StringCodec struct{}

// Encode returns v as bytes.
func (StringCodec) Encode(v string) ([]byte, error) { return []byte(v), nil }

// Decode returns b as a string.
func (StringCodec) Decode(b []byte) (string, error) { return string(b), nil }

// StringKeyCodec is the identity KeyCodec for string keys.
type StringKeyCodec struct{}

// EncodeKey returns k.
func (StringKeyCodec) EncodeKey(k string) (string, error) { return k, nil }
