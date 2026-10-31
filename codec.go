package redcache

import (
	"encoding"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"fmt"
	"reflect"
)

// Codec encodes and decodes values into the envelope payload. A Cache owns one
// Codec and may use it with different value types across operations. Decode
// writes into the supplied destination; the cache never asserts a decoded
// interface value back to the operation's type. Implementations must be
// concurrent-safe and return an error for unsupported value or destination
// types.
//
// Ownership of return values:
//   - Encode's returned slice is handed to the library and must not be mutated
//     by the caller afterward. The library may alias it without copying, so the
//     codec must not retain or later modify it either.
//   - Decode receives a pointer to the operation's value type. Its input slice
//     is borrowed from library-internal memory and is only valid for the
//     duration of the call; it must not be retained.
//
// Identity codecs (UnsafeBytesCodec) alias this borrowed memory directly and so
// trade safety for zero copies — the decoded []byte must not outlive the call or
// be mutated. JSONCodec returns owned decoded values. StringCodec returns
// immutable strings that are safe to retain.
type Codec interface {
	Encode(any) ([]byte, error)
	Decode([]byte, any) error
}

// KeyCodec encodes a key into the Redis key string. A Cache owns one KeyCodec
// and may use it with different key types across operations; implementations
// must be concurrent-safe and return an error for unsupported key types.
// StringKeyCodec is the default; KeyCodecFunc adapts a typed function.
type KeyCodec interface {
	EncodeKey(any) (string, error)
}

// KeyCodecFunc adapts a typed function into a KeyCodec. Keys of any type other
// than K are rejected with an error.
type KeyCodecFunc[K any] func(K) (string, error)

// EncodeKey calls f when k is a K.
func (f KeyCodecFunc[K]) EncodeKey(k any) (string, error) {
	kk, ok := k.(K)
	if !ok {
		var zero K
		return "", fmt.Errorf("redcache: KeyCodecFunc[%T] cannot encode key of type %T", zero, k)
	}
	return f(kk)
}

// JSONCodec encodes values via encoding/json (v1 semantics: nil slices and
// maps encode as null, case-insensitive field matching, duplicate object names
// tolerated). Its wire format is unchanged from earlier releases, so caches
// written by older binaries stay readable.
type JSONCodec struct{}

// Encode marshals v to JSON.
func (JSONCodec) Encode(v any) ([]byte, error) { return json.Marshal(v) }

// Decode unmarshals b into dst, which must be a non-nil pointer.
func (JSONCodec) Decode(b []byte, dst any) error { return json.Unmarshal(b, dst) }

// JSONV2Codec encodes values via encoding/json/v2 with its stricter defaults:
// nil slices and maps encode as [] and {}, field names match case-sensitively,
// and duplicate object names and invalid UTF-8 are rejected on decode. Use it
// for new caches; switching an existing cache from JSONCodec changes the stored
// representation of nil containers.
type JSONV2Codec struct{}

// Encode marshals v to JSON.
func (JSONV2Codec) Encode(v any) ([]byte, error) { return jsonv2.Marshal(v) }

// Decode unmarshals b into dst, which must be a non-nil pointer.
func (JSONV2Codec) Decode(b []byte, dst any) error { return jsonv2.Unmarshal(b, dst) }

// UnsafeBytesCodec is the zero-copy identity codec for []byte. It aliases
// library-internal memory in both directions: Encode hands its input straight
// to the library (which may alias it), and Decode returns a []byte backed by
// the cache's borrowed read buffer. The decoded slice must not be mutated or
// retained past the call. Use a copying codec if you need an owned value.
type UnsafeBytesCodec struct{}

// Encode returns v unchanged (no copy).
func (UnsafeBytesCodec) Encode(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("redcache: UnsafeBytesCodec cannot encode %T", v)
	}
	return b, nil
}

// Decode assigns b unchanged (no copy); the result aliases borrowed memory.
func (UnsafeBytesCodec) Decode(b []byte, dst any) error {
	p, ok := dst.(*[]byte)
	if !ok {
		return fmt.Errorf("redcache: UnsafeBytesCodec cannot decode into %T", dst)
	}
	*p = b
	return nil
}

// StringCodec is the identity codec for string.
type StringCodec struct{}

// Encode returns v as bytes.
func (StringCodec) Encode(v any) ([]byte, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("redcache: StringCodec cannot encode %T", v)
	}
	return []byte(s), nil
}

// Decode assigns b as a string.
func (StringCodec) Decode(b []byte, dst any) error {
	p, ok := dst.(*string)
	if !ok {
		return fmt.Errorf("redcache: StringCodec cannot decode into %T", dst)
	}
	*p = string(b)
	return nil
}

// StringKeyCodec is the default KeyCodec. A string, or any type whose
// underlying type is string, is used as-is (this is what enables the
// zero-allocation multi-key fast path); an [encoding.TextMarshaler] is encoded
// with MarshalText; any other type is an error.
type StringKeyCodec struct{}

// EncodeKey returns k's string value, or its MarshalText output.
func (StringKeyCodec) EncodeKey(k any) (string, error) {
	if s, ok := k.(string); ok {
		return s, nil
	}
	if rv := reflect.ValueOf(k); rv.Kind() == reflect.String {
		return rv.String(), nil
	}
	if tm, ok := k.(encoding.TextMarshaler); ok {
		b, err := tm.MarshalText()
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
	return "", fmt.Errorf("redcache: StringKeyCodec cannot encode key of type %T", k)
}
