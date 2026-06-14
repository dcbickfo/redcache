package redcacheotel

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/dcbickfo/redcache"
)

// scopeName identifies this instrumentation scope in emitted metrics.
const scopeName = "github.com/dcbickfo/redcache/redcacheotel"

// Metrics implements redcache.Metrics by recording OpenTelemetry instruments.
// Construct it with NewMetrics. The zero value is not usable.
type Metrics struct {
	ctx context.Context

	cacheHits     metric.Int64Counter
	cacheMisses   metric.Int64Counter
	lockContended metric.Int64Counter

	refreshTriggered metric.Int64Counter
	refreshSkipped   metric.Int64Counter
	refreshDropped   metric.Int64Counter

	lockLost           metric.Int64Counter
	refreshErrors      metric.Int64Counter
	refreshPanicked    metric.Int64Counter
	invalidationErrors metric.Int64Counter
	loaderErrors       metric.Int64Counter
	redisErrors        metric.Int64Counter

	lockWaitDuration metric.Float64Histogram
	loaderDuration   metric.Float64Histogram
}

// Verify the adapter satisfies the redcache contract at compile time.
var _ redcache.Metrics = (*Metrics)(nil)

// NewMetrics builds a Metrics from a MeterProvider. It returns an error only on
// instrument-construction failure, which indicates programmer error (e.g. an
// invalid unit). The returned *Metrics satisfies redcache.Metrics and is safe
// for concurrent use.
func NewMetrics(mp metric.MeterProvider) (*Metrics, error) {
	meter := mp.Meter(scopeName)
	m := &Metrics{ctx: context.Background()}

	// Construction continues past a failed instrument and returns the first error
	// encountered, so one bad build doesn't short-circuit the rest of the wiring.
	var firstErr error
	counter := func(name, desc string) metric.Int64Counter {
		c, err := meter.Int64Counter(name, metric.WithDescription(desc))
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("redcacheotel: create counter %q: %w", name, err)
		}
		return c
	}
	histogram := func(name, desc string) metric.Float64Histogram {
		h, err := meter.Float64Histogram(name,
			metric.WithDescription(desc),
			metric.WithUnit("s"),
		)
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("redcacheotel: create histogram %q: %w", name, err)
		}
		return h
	}

	m.cacheHits = counter("cache.hits", "Cache reads served from cache.")
	m.cacheMisses = counter("cache.misses", "Cache reads that fell through to a loader.")
	m.lockContended = counter("lock.contended", "Waits on a lock held by another caller.")

	m.refreshTriggered = counter("refresh.triggered", "Background refreshes started.")
	m.refreshSkipped = counter("refresh.skipped", "Background refreshes deduped (local or distributed).")
	m.refreshDropped = counter("refresh.dropped", "Background refreshes dropped because the queue was full.")

	m.lockLost = counter("lock.lost", "Locks lost to a concurrent writer (CAS detected a stolen lock).")
	m.refreshErrors = counter("refresh.errors", "Background refresh callbacks that returned an error.")
	m.refreshPanicked = counter("refresh.panicked", "Background refresh callbacks that panicked.")
	m.invalidationErrors = counter("invalidation.errors", "Invalidation messages that failed to parse.")
	m.loaderErrors = counter("loader.errors", "Keys whose foreground loader returned an error.")
	m.redisErrors = counter("redis.errors", "Redis commands that failed, by operation.")

	m.lockWaitDuration = histogram("lock.wait.duration", "Time spent waiting on a contended lock, in seconds.")
	m.loaderDuration = histogram("loader.duration", "Foreground origin-loader latency, in seconds.")

	if firstErr != nil {
		return nil, firstErr
	}
	return m, nil
}

// CacheHits records n cache reads served from cache.
func (m *Metrics) CacheHits(n int64) { m.cacheHits.Add(m.ctx, n) }

// CacheMisses records n cache reads that fell through to a loader.
func (m *Metrics) CacheMisses(n int64) { m.cacheMisses.Add(m.ctx, n) }

// LockContended records n waits on a lock held by another caller.
func (m *Metrics) LockContended(n int64) { m.lockContended.Add(m.ctx, n) }

// LockWaitDuration records the time spent waiting on a contended lock.
func (m *Metrics) LockWaitDuration(d time.Duration) {
	m.lockWaitDuration.Record(m.ctx, d.Seconds())
}

// LoaderDuration records foreground origin-loader latency (Get/GetMulti miss,
// Set/SetMulti). It excludes background refresh.
func (m *Metrics) LoaderDuration(d time.Duration) {
	m.loaderDuration.Record(m.ctx, d.Seconds())
}

// LoaderErrors records that a foreground loader returned an error; n is the
// number of keys it was responsible for.
func (m *Metrics) LoaderErrors(n int64) { m.loaderErrors.Add(m.ctx, n) }

// RefreshTriggered records n background refreshes started.
func (m *Metrics) RefreshTriggered(n int64) { m.refreshTriggered.Add(m.ctx, n) }

// RefreshSkipped records n background refreshes deduped (local or distributed).
func (m *Metrics) RefreshSkipped(n int64) { m.refreshSkipped.Add(m.ctx, n) }

// RefreshDropped records n background refreshes dropped because the queue was full.
func (m *Metrics) RefreshDropped(n int64) { m.refreshDropped.Add(m.ctx, n) }

// RedisError records a failed Redis command, labelled by op (one of read,
// lock, set, del, touch). op is a small bounded set, so it is safe as a label.
func (m *Metrics) RedisError(op string) {
	m.redisErrors.Add(m.ctx, 1, metric.WithAttributes(attribute.String("op", op)))
}

// LockLost records a lock lost to a concurrent writer. The key is deliberately
// not attached as an attribute to avoid unbounded label cardinality.
func (m *Metrics) LockLost(string) { m.lockLost.Add(m.ctx, 1) }

// RefreshError records a background refresh callback error. The key is
// deliberately not attached as an attribute to avoid unbounded cardinality.
func (m *Metrics) RefreshError(string) { m.refreshErrors.Add(m.ctx, 1) }

// RefreshPanicked records a background refresh callback panic. The key is
// deliberately not attached as an attribute to avoid unbounded cardinality.
func (m *Metrics) RefreshPanicked(string) { m.refreshPanicked.Add(m.ctx, 1) }

// InvalidationError records an invalidation message that failed to parse.
func (m *Metrics) InvalidationError() { m.invalidationErrors.Add(m.ctx, 1) }
