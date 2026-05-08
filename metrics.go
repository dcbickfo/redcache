package redcache

import "time"

// Metrics receives observability events from CacheAside operations. All methods
// must be safe for concurrent use and run on the hot path.
//
// High-volume events (CacheHits, CacheMisses, LockContended, RefreshTriggered,
// RefreshSkipped, RefreshDropped) are aggregated per operation and emitted
// once with a count. Low-volume diagnostic events (LockLost, RefreshError,
// RefreshPanicked) carry the affected key.
//
// Implementations can embed NoopMetrics and override only the methods of interest.
type Metrics interface {
	// CacheHits fires once per Get/GetMulti with the number of values served
	// from the client-side cache.
	CacheHits(n int64)
	// CacheMisses fires once per Get/GetMulti with the number of keys that
	// had to be populated via the user callback.
	CacheMisses(n int64)
	// LockContended fires once per Get/GetMulti with the number of keys that
	// observed an existing lock and waited.
	LockContended(n int64)
	// LockWaitDuration fires once per resolved lock wait with the elapsed
	// time. Emitted regardless of how the wait ended (invalidation, context
	// cancellation, or LockTTL timeout). Typically histogrammed.
	LockWaitDuration(d time.Duration)
	// RefreshTriggered fires once per refresh enqueue with the number of keys
	// the job covers.
	RefreshTriggered(n int64)
	// RefreshSkipped fires with the number of keys skipped due to local or
	// distributed dedup.
	RefreshSkipped(n int64)
	// RefreshDropped fires with the number of keys whose refresh was dropped
	// because the worker queue was full.
	RefreshDropped(n int64)
	// LockLost fires when a CAS detected the operation's lock was no longer
	// held (typically because a ForceSet overwrote it).
	LockLost(key string)
	// RefreshError fires when a refresh-ahead operation failed due to a Redis
	// error or callback error.
	RefreshError(key string)
	// RefreshPanicked fires when a refresh worker recovered from a panic in
	// the callback.
	RefreshPanicked(key string)
	// InvalidationError fires when a Redis invalidation message could not be
	// parsed. The key is unknown.
	InvalidationError()
}

// NoopMetrics is a Metrics implementation that does nothing. Embed it to opt
// in to a subset of events:
//
//	type myMetrics struct {
//	    redcache.NoopMetrics
//	}
//
//	func (myMetrics) CacheMisses(n int64) { /* count miss */ }
type NoopMetrics struct{}

// CacheHits implements Metrics.
func (NoopMetrics) CacheHits(int64) {}

// CacheMisses implements Metrics.
func (NoopMetrics) CacheMisses(int64) {}

// LockContended implements Metrics.
func (NoopMetrics) LockContended(int64) {}

// LockWaitDuration implements Metrics.
func (NoopMetrics) LockWaitDuration(time.Duration) {}

// RefreshTriggered implements Metrics.
func (NoopMetrics) RefreshTriggered(int64) {}

// RefreshSkipped implements Metrics.
func (NoopMetrics) RefreshSkipped(int64) {}

// RefreshDropped implements Metrics.
func (NoopMetrics) RefreshDropped(int64) {}

// LockLost implements Metrics.
func (NoopMetrics) LockLost(string) {}

// RefreshError implements Metrics.
func (NoopMetrics) RefreshError(string) {}

// RefreshPanicked implements Metrics.
func (NoopMetrics) RefreshPanicked(string) {}

// InvalidationError implements Metrics.
func (NoopMetrics) InvalidationError() {}
