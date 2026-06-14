// Package redcacheotel provides a drop-in OpenTelemetry adapter for the
// redcache.Metrics interface.
//
// Wire it into a Conn with redcache.WithMetrics, then derive typed views:
//
//	mp := otel.GetMeterProvider() // or your own *sdkmetric.MeterProvider
//	m, err := redcacheotel.NewMetrics(mp)
//	if err != nil {
//		return err
//	}
//	conn, err := redcache.Open(opt, redcache.WithMetrics(m))
//	if err != nil {
//		return err
//	}
//	defer conn.Close()
//
//	cache := redcache.NewString[User](conn, redcache.JSONCodec[User]{})
//
// If you prefer to panic on construction failure (instruments only fail to
// build on programmer error, e.g. a bad unit string), wrap it in a must helper:
//
//	func must[T any](v T, err error) T {
//		if err != nil {
//			panic(err)
//		}
//		return v
//	}
//
//	conn, err := redcache.Open(opt, redcache.WithMetrics(must(redcacheotel.NewMetrics(mp))))
//
// # Instruments
//
// Counters (Int64Counter, monotonic):
//
//	cache.hits             cache reads served from cache
//	cache.misses           cache reads that fell through to a loader
//	lock.contended         waits on a lock held by another caller
//	refresh.triggered      background refreshes started
//	refresh.skipped        background refreshes deduped (local or distributed)
//	refresh.dropped        background refreshes dropped (queue full)
//	lock.lost              CAS detected a stolen lock
//	refresh.errors         background refresh callbacks returned an error
//	refresh.panicked       background refresh callbacks panicked
//	invalidation.errors    invalidation messages that failed to parse
//	loader.errors          foreground loader callbacks returned an error
//	redis.errors           Redis commands that failed (attribute: op)
//
// Histograms (Float64Histogram, unit "s" — seconds):
//
//	lock.wait.duration     time spent waiting on a contended lock
//	loader.duration        foreground origin-loader latency
//
// # Cardinality
//
// redis.errors carries a bounded op attribute (one of read, lock, set, del,
// touch). All other counters are emitted without per-key labels. The redcache
// diagnostics that carry a key (LockLost, RefreshError, RefreshPanicked) would
// produce unbounded label cardinality if the key were attached, so they are
// counted without the key. Use logs/traces to attribute individual keys.
package redcacheotel
