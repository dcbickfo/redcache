package redcache

import "time"

// Metrics receives observability events. Methods run on the hot path and
// must be concurrent-safe. High-volume events (hits/misses/contention/refresh
// counts) are batched per operation and emitted once with n. Diagnostics
// (LockLost, RefreshError, RefreshPanicked) carry the affected key. Embed
// NoopMetrics to opt in to a subset.
type Metrics interface {
	CacheHits(n int64)
	CacheMisses(n int64)
	LockContended(n int64)
	// LockWaitDuration fires once per resolved wait, regardless of outcome
	// (invalidation, ctx cancel, or lockTTL). Typically histogrammed.
	LockWaitDuration(d time.Duration)
	RefreshTriggered(n int64)
	// RefreshSkipped covers both local and distributed dedup.
	RefreshSkipped(n int64)
	// RefreshDropped fires when the refresh queue is full.
	RefreshDropped(n int64)
	// LockLost fires when a CAS detected a stolen lock (e.g. via ForceSet).
	LockLost(key string)
	RefreshError(key string)
	// RefreshPanicked fires once per key when a refresh callback panicked.
	RefreshPanicked(key string)
	// InvalidationError fires when an invalidation message couldn't be parsed.
	InvalidationError()
}

// NoopMetrics is the zero-value Metrics implementation. Embed and override.
type NoopMetrics struct{}

func (NoopMetrics) CacheHits(int64)                {}
func (NoopMetrics) CacheMisses(int64)              {}
func (NoopMetrics) LockContended(int64)            {}
func (NoopMetrics) LockWaitDuration(time.Duration) {}
func (NoopMetrics) RefreshTriggered(int64)         {}
func (NoopMetrics) RefreshSkipped(int64)           {}
func (NoopMetrics) RefreshDropped(int64)           {}
func (NoopMetrics) LockLost(string)                {}
func (NoopMetrics) RefreshError(string)            {}
func (NoopMetrics) RefreshPanicked(string)         {}
func (NoopMetrics) InvalidationError()             {}
