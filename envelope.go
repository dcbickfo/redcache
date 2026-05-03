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
// Constraints:
//   - The prefix must not collide with the lock prefix. The default lock prefix
//     ("__redcache:lock:") is distinct, but operators choosing a custom
//     LockPrefix must avoid one that is itself a prefix of envelopePrefix.
//   - Values stored before envelope wrapping (legacy) are returned with delta=0,
//     which makes shouldRefresh fall back to the simple floor-based check.
const envelopePrefix = "__redcache:v1:"

// wrapEnvelope wraps a user value with the v1 envelope so the compute delta
// travels with the value and can be recovered on read.
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

// unwrapEnvelope returns the inner value and the recorded delta. For values
// that were not envelope-wrapped (legacy data, or malformed envelope) it
// returns the input unchanged and delta=0. shouldRefresh treats delta=0 as
// "no XFetch metadata available" and falls back to the simple floor check.
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
