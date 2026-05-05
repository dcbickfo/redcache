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
	c := redcache.JSONCodec[user]{}
	in := user{ID: 7, Name: "alice"}

	b, err := c.Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := c.Decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("got %+v, want %+v", out, in)
	}
}

func TestJSONCodec_DecodeInvalidJSON(t *testing.T) {
	c := redcache.JSONCodec[map[string]int]{}
	if _, err := c.Decode([]byte("not json")); err == nil {
		t.Fatal("expected decode error for invalid json")
	}
}

func TestBytesCodec_Identity(t *testing.T) {
	c := redcache.BytesCodec{}
	in := []byte("hello \x00 world")
	b, err := c.Encode(in)
	if err != nil || !bytes.Equal(b, in) {
		t.Fatalf("encode mismatch: got %q err %v", b, err)
	}
	out, err := c.Decode(in)
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
	out, err := c.Decode([]byte(in))
	if err != nil || out != in {
		t.Fatalf("decode mismatch: got %q err %v", out, err)
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
