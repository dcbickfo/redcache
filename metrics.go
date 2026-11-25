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
	// (invalidation, ctx cancel, lockTTL, or a poll that found a hit or an
	// absent key). Typically histogrammed.
	LockWaitDuration(d time.Duration)
	// LoaderDuration measures foreground origin-loader latency (Get/GetMulti
	// miss, Set/SetMulti). Background refresh is excluded. Typically histogrammed.
	LoaderDuration(d time.Duration)
	// LoaderErrors fires when a foreground loader returns an error; n is the
	// number of keys the loader was responsible for (1 for single, len(missing)
	// for multi).
	LoaderErrors(n int64)
	// RedisError records command failures with op "read", "lock", "set",
	// "del", or "touch". "read_fallback" counts an attempted uncached retry
	// after a clustered cached read aborted. It emits one event per uncached
	// retry attempt, whether for one key or a batch. A polling Get or
	// GetMulti can make more than one attempt. The count includes retries
	// that fail, return Redis nil, or are canceled before sending. Missing
	// PTTL permission can trigger an abort. A sustained rate alone cannot
	// identify a server-side EXEC abort because rueidis also reports
	// ErrDoCacheAborted when a connection closes.
	// Lock-lost, Redis nil, and script-drift parse failures are excluded
	// from the command-failure labels.
	RedisError(op string)
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
func (NoopMetrics) LoaderDuration(time.Duration)   {}
func (NoopMetrics) LoaderErrors(int64)             {}
func (NoopMetrics) RedisError(string)              {}
func (NoopMetrics) RefreshTriggered(int64)         {}
func (NoopMetrics) RefreshSkipped(int64)           {}
func (NoopMetrics) RefreshDropped(int64)           {}
func (NoopMetrics) LockLost(string)                {}
func (NoopMetrics) RefreshError(string)            {}
func (NoopMetrics) RefreshPanicked(string)         {}
func (NoopMetrics) InvalidationError()             {}
