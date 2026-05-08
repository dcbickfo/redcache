package redcache

import (
	"strconv"
	"strings"
	"time"
)

// envelopePrefix wraps stored values so refresh-ahead can read the original
// compute time (delta) and apply XFetch-style probabilistic refresh.
//
// Format: __redcache:v1:<delta_ns>:<payload>
//
// A custom LockPrefix must not be a prefix of envelopePrefix. Values without
// the prefix are returned with delta=0 so shouldRefresh falls back to the
// floor-based check.
const envelopePrefix = "__redcache:v1:"

// wrapEnvelope wraps a user value with the v1 envelope so the compute delta
// can be recovered on read.
func wrapEnvelope(val string, delta time.Duration) string {
	if delta < 0 {
		delta = 0
	}
	var b strings.Builder
	b.Grow(len(envelopePrefix) + 20 + 1 + len(val))
	b.WriteString(envelopePrefix)
	b.WriteString(strconv.FormatInt(int64(delta), 10))
	b.WriteByte(':')
	b.WriteString(val)
	return b.String()
}

// unwrapEnvelope returns the inner value and the recorded delta. For
// non-envelope or malformed input it returns the input unchanged with delta=0,
// which shouldRefresh treats as "no XFetch metadata".
func unwrapEnvelope(s string) (val string, delta time.Duration) {
	if !strings.HasPrefix(s, envelopePrefix) {
		return s, 0
	}
	rest := s[len(envelopePrefix):]
	colonIdx := strings.IndexByte(rest, ':')
	if colonIdx < 0 {
		return s, 0
	}
	deltaNs, err := strconv.ParseInt(rest[:colonIdx], 10, 64)
	if err != nil || deltaNs < 0 {
		return s, 0
	}
	return rest[colonIdx+1:], time.Duration(deltaNs)
}
