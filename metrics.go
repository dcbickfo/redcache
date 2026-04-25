package redcache

// Metrics receives observability events from CacheAside operations.
//
// All methods must be safe for concurrent use; callers may invoke them from
// background workers and request goroutines simultaneously. Implementations
// should be cheap — they run on the hot path.
//
// Implementations that only care about a subset of events can embed
// NoopMetrics and override the methods of interest.
type Metrics interface {
	// CacheHit fires when a Get/GetMulti served a value from the client-side cache.
	CacheHit(key string)
	// CacheMiss fires when a Get/GetMulti had to populate via the user callback.
	CacheMiss(key string)
	// LockContended fires when an operation observed an existing lock and waited.
	LockContended(key string)
	// LockLost fires when a CAS detected the operation's lock was no longer held
	// (typically because a ForceSet or similar overwrote it).
	LockLost(key string)
	// RefreshTriggered fires when a refresh-ahead job was enqueued.
	RefreshTriggered(key string)
	// RefreshSkipped fires when a refresh was skipped due to local or distributed dedup.
	RefreshSkipped(key string)
	// RefreshDropped fires when a refresh was dropped because the worker queue was full.
	RefreshDropped(key string)
	// RefreshPanicked fires when a refresh worker recovered from a panic in the callback.
	RefreshPanicked(panicValue any)
}

// NoopMetrics is a Metrics implementation that does nothing. Embed it to opt
// in to a subset of events:
//
//	type myMetrics struct {
//	    redcache.NoopMetrics
//	}
//
//	func (myMetrics) CacheMiss(key string) { /* count miss */ }
type NoopMetrics struct{}

// CacheHit implements Metrics.
func (NoopMetrics) CacheHit(string) {}

// CacheMiss implements Metrics.
func (NoopMetrics) CacheMiss(string) {}

// LockContended implements Metrics.
func (NoopMetrics) LockContended(string) {}

// LockLost implements Metrics.
func (NoopMetrics) LockLost(string) {}

// RefreshTriggered implements Metrics.
func (NoopMetrics) RefreshTriggered(string) {}

// RefreshSkipped implements Metrics.
func (NoopMetrics) RefreshSkipped(string) {}

// RefreshDropped implements Metrics.
func (NoopMetrics) RefreshDropped(string) {}

// RefreshPanicked implements Metrics.
func (NoopMetrics) RefreshPanicked(any) {}
