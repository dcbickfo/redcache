package redcache_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/dcbickfo/redcache"
)

func TestJSONCodec_RoundTripStruct(t *testing.T) {
	type user struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	c := redcache.JSONCodec{}
	in := user{ID: 7, Name: "alice"}

	b, err := c.Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out user
	if err := c.Decode(b, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("got %+v, want %+v", out, in)
	}
}

func TestJSONCodec_DecodeInvalidJSON(t *testing.T) {
	c := redcache.JSONCodec{}
	var out map[string]int
	if err := c.Decode([]byte("not json"), &out); err == nil {
		t.Fatal("expected decode error for invalid json")
	}
}

func TestJSONCodec_NilContainersEncodeAsNull(t *testing.T) {
	c := redcache.JSONCodec{}
	for _, in := range []any{[]string(nil), map[string]int(nil)} {
		b, err := c.Encode(in)
		if err != nil {
			t.Fatalf("encode nil %T: %v", in, err)
		}
		if string(b) != "null" {
			t.Fatalf("encoded nil %T = %s, want null", in, b)
		}
	}
}

func TestJSONV2Codec_RoundTripStruct(t *testing.T) {
	type user struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	c := redcache.JSONV2Codec{}
	in := user{ID: 7, Name: "alice"}
	b, err := c.Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out user
	if err := c.Decode(b, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("got %+v, want %+v", out, in)
	}
}

func TestJSONV2Codec_NilContainerDefaults(t *testing.T) {
	c := redcache.JSONV2Codec{}
	tests := []struct {
		name string
		in   any
		want string
	}{
		{name: "slice", in: []string(nil), want: "[]"},
		{name: "map", in: map[string]int(nil), want: "{}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := c.Encode(tt.in)
			if err != nil {
				t.Fatalf("encode nil %s: %v", tt.name, err)
			}
			if string(b) != tt.want {
				t.Fatalf("encoded nil %s = %s, want %s", tt.name, b, tt.want)
			}
		})
	}
}

func TestJSONV2Codec_RejectsDuplicateObjectNames(t *testing.T) {
	c := redcache.JSONV2Codec{}
	var out map[string]int
	if err := c.Decode([]byte(`{"count":1,"count":2}`), &out); err == nil {
		t.Fatal("expected duplicate object name to be rejected")
	}
}

func TestBytesCodec_Identity(t *testing.T) {
	c := redcache.UnsafeBytesCodec{}
	in := []byte("hello \x00 world")
	b, err := c.Encode(in)
	if err != nil || !bytes.Equal(b, in) {
		t.Fatalf("encode mismatch: got %q err %v", b, err)
	}
	var out []byte
	err = c.Decode(in, &out)
	if err != nil || !bytes.Equal(out, in) {
		t.Fatalf("decode mismatch: got %q err %v", out, err)
	}
}

func TestStringCodec_Identity(t *testing.T) {
	c := redcache.StringCodec{}
	const in = "hello"
	b, err := c.Encode(in)
	if err != nil || string(b) != in {
		t.Fatalf("encode mismatch: got %q err %v", b, err)
	}
	var out string
	err = c.Decode([]byte(in), &out)
	if err != nil || out != in {
		t.Fatalf("decode mismatch: got %q err %v", out, err)
	}
}

func TestStringCodec_RejectsOtherValueTypes(t *testing.T) {
	c := redcache.StringCodec{}
	if _, err := c.Encode(42); err == nil {
		t.Fatal("expected encoding int with StringCodec to fail")
	}
	var out int
	if err := c.Decode([]byte("42"), &out); err == nil {
		t.Fatal("expected decoding into *int with StringCodec to fail")
	}
}

func TestStringKeyCodec_Identity(t *testing.T) {
	c := redcache.StringKeyCodec{}
	out, err := c.EncodeKey("user-123")
	if err != nil || out != "user-123" {
		t.Fatalf("got %q err %v", out, err)
	}
}

func TestKeyCodecFunc_AdaptsFunction(t *testing.T) {
	sentinel := errors.New("nope")
	bad := redcache.KeyCodecFunc[int](func(int) (string, error) { return "", sentinel })
	if _, err := bad.EncodeKey(0); !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel, got %v", err)
	}
	good := redcache.KeyCodecFunc[int](func(i int) (string, error) {
		return "i:" + itoa(i), nil
	})
	out, err := good.EncodeKey(42)
	if err != nil || out != "i:42" {
		t.Fatalf("got %q err %v", out, err)
	}
}

func itoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = digits[i%10]
		i /= 10
	}
	return string(b[pos:])
}
