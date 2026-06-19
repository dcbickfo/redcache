package redcache

import (
	"testing"
	"time"
)

func TestEnvelope_RoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		val   string
		delta time.Duration
	}{
		{"simple value", "hello", 100 * time.Millisecond},
		{"empty value", "", 250 * time.Microsecond},
		{"value with colons", "user:42:name", 1500 * time.Microsecond},
		{"value with envelope-looking prefix", "__redcache:v1:99:body", 5 * time.Millisecond},
		{"zero delta", "raw", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wrapped := wrapEnvelope(tc.val, tc.delta)
			gotVal, gotDelta := unwrapEnvelope(wrapped)
			if gotVal != tc.val {
				t.Errorf("val = %q, want %q", gotVal, tc.val)
			}
			if gotDelta != tc.delta {
				t.Errorf("delta = %v, want %v", gotDelta, tc.delta)
			}
		})
	}
}

func TestEnvelope_LegacyValuePassthrough(t *testing.T) {
	t.Parallel()
	// Pre-envelope values must round-trip with delta=0 so shouldRefresh falls
	// back to floor-based refresh.
	cases := []string{
		"plain",
		"",
		"with:colons",
		"__redcache:lock:1234", // lock-shaped values pass through as-is
	}
	for _, val := range cases {
		gotVal, gotDelta := unwrapEnvelope(val)
		if gotVal != val {
			t.Errorf("legacy %q lost: got %q", val, gotVal)
		}
		if gotDelta != 0 {
			t.Errorf("legacy %q got delta=%v, want 0", val, gotDelta)
		}
	}
}

func TestEnvelope_MalformedFallsBack(t *testing.T) {
	t.Parallel()
	// Prefixed-but-unparseable inputs must pass through with delta=0 rather
	// than panicking or returning garbage.
	cases := []string{
		"__redcache:v1:notanumber:body",
		"__redcache:v1:-5:body", // negative delta is rejected
		"__redcache:v1:nocolons",
	}
	for _, val := range cases {
		gotVal, gotDelta := unwrapEnvelope(val)
		if gotVal != val {
			t.Errorf("malformed %q got %q, want passthrough", val, gotVal)
		}
		if gotDelta != 0 {
			t.Errorf("malformed %q got delta=%v, want 0", val, gotDelta)
		}
	}
}
