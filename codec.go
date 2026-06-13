package redcache

import "encoding/json"

// Codec encodes and decodes V into the envelope payload. Implementations
// must be concurrent-safe. Encode's returned slice is owned by the library;
// Decode's input is borrowed and must not be retained.
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

// BytesCodec is the identity codec for []byte.
type BytesCodec struct{}

// Encode returns v.
func (BytesCodec) Encode(v []byte) ([]byte, error) { return v, nil }

// Decode returns b.
func (BytesCodec) Decode(b []byte) ([]byte, error) { return b, nil }

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
