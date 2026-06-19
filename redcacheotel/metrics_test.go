package redcacheotel

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collect builds a Metrics backed by a manual reader, runs record, then returns
// the single scope's collected metrics keyed by instrument name.
func collect(t *testing.T, record func(m *Metrics)) map[string]metricdata.Metrics {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	m, err := NewMetrics(mp)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	record(m)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	out := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			out[md.Name] = md
		}
	}
	return out
}

func TestCountersRecord(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		instrument string
		record     func(*Metrics)
		want       int64
	}{
		{
			name:       "cache hits",
			instrument: "cache.hits",
			record: func(m *Metrics) {
				m.CacheHits(3)
				m.CacheHits(2)
			},
			want: 5,
		},
		{
			name:       "cache misses",
			instrument: "cache.misses",
			record:     func(m *Metrics) { m.CacheMisses(4) },
			want:       4,
		},
		{
			name:       "lock contended",
			instrument: "lock.contended",
			record:     func(m *Metrics) { m.LockContended(3) },
			want:       3,
		},
		{
			name:       "loader errors",
			instrument: "loader.errors",
			record:     func(m *Metrics) { m.LoaderErrors(7) },
			want:       7,
		},
		{
			name:       "refresh triggered",
			instrument: "refresh.triggered",
			record:     func(m *Metrics) { m.RefreshTriggered(2) },
			want:       2,
		},
		{
			name:       "refresh skipped",
			instrument: "refresh.skipped",
			record:     func(m *Metrics) { m.RefreshSkipped(2) },
			want:       2,
		},
		{
			name:       "refresh dropped",
			instrument: "refresh.dropped",
			record:     func(m *Metrics) { m.RefreshDropped(2) },
			want:       2,
		},
		{
			name:       "refresh errors",
			instrument: "refresh.errors",
			record: func(m *Metrics) {
				m.RefreshError("user:1")
				m.RefreshError("user:2")
			},
			want: 2,
		},
		{
			name:       "refresh panicked",
			instrument: "refresh.panicked",
			record: func(m *Metrics) {
				m.RefreshPanicked("user:1")
				m.RefreshPanicked("user:2")
			},
			want: 2,
		},
		{
			name:       "invalidation errors",
			instrument: "invalidation.errors",
			record: func(m *Metrics) {
				m.InvalidationError()
				m.InvalidationError()
			},
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := collect(t, tt.record)
			assertCounterValue(t, got, tt.instrument, tt.want)
		})
	}
}

func TestRedisErrorAttribute(t *testing.T) {
	t.Parallel()
	got := collect(t, func(m *Metrics) {
		m.RedisError("read")
		m.RedisError("read")
		m.RedisError("set")
	})

	md, ok := got["redis.errors"]
	if !ok {
		t.Fatalf("redis.errors not recorded; got %v", keys(got))
	}
	sum := md.Data.(metricdata.Sum[int64])
	// One data point per distinct op attribute value.
	if len(sum.DataPoints) != 2 {
		t.Fatalf("redis.errors: got %d data points, want 2 (read, set)", len(sum.DataPoints))
	}
	byOp := map[string]int64{}
	for _, dp := range sum.DataPoints {
		op, ok := dp.Attributes.Value("op")
		if !ok {
			t.Fatalf("redis.errors data point missing op attribute")
		}
		byOp[op.AsString()] = dp.Value
	}
	if byOp["read"] != 2 || byOp["set"] != 1 {
		t.Fatalf("redis.errors by op = %v, want read=2 set=1", byOp)
	}
}

func TestLockLostNoKeyLabel(t *testing.T) {
	t.Parallel()
	got := collect(t, func(m *Metrics) {
		m.LockLost("user:1")
		m.LockLost("user:2")
	})

	md, ok := got["lock.lost"]
	if !ok {
		t.Fatalf("lock.lost not recorded; got %v", keys(got))
	}
	sum := md.Data.(metricdata.Sum[int64])
	// Keys must NOT become attributes: both calls collapse to one data point.
	if len(sum.DataPoints) != 1 {
		t.Fatalf("lock.lost: got %d data points, want 1 (no per-key labels)", len(sum.DataPoints))
	}
	if v := sum.DataPoints[0].Value; v != 2 {
		t.Fatalf("lock.lost value = %d, want 2", v)
	}
}

func TestHistogramRecords(t *testing.T) {
	t.Parallel()
	got := collect(t, func(m *Metrics) {
		m.LockWaitDuration(20 * time.Millisecond)
		m.LoaderDuration(150 * time.Millisecond)
	})

	assertHistogramValue(t, got, "lock.wait.duration", 0.02)
	assertHistogramValue(t, got, "loader.duration", 0.15)
}

func assertCounterValue(t *testing.T, got map[string]metricdata.Metrics, name string, want int64) {
	t.Helper()

	md, ok := got[name]
	if !ok {
		t.Fatalf("%s not recorded; got %v", name, keys(got))
	}
	sum, ok := md.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s is %T, want metricdata.Sum[int64]", name, md.Data)
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("%s: got %d data points, want 1", name, len(sum.DataPoints))
	}
	if v := sum.DataPoints[0].Value; v != want {
		t.Fatalf("%s value = %d, want %d", name, v, want)
	}
}

func assertHistogramValue(t *testing.T, got map[string]metricdata.Metrics, name string, want float64) {
	t.Helper()

	md, ok := got[name]
	if !ok {
		t.Fatalf("%s not recorded; got %v", name, keys(got))
	}
	if md.Unit != "s" {
		t.Fatalf("%s unit = %q, want %q", name, md.Unit, "s")
	}
	h, ok := md.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("%s is %T, want metricdata.Histogram[float64]", name, md.Data)
	}
	if len(h.DataPoints) != 1 {
		t.Fatalf("%s: got %d data points, want 1", name, len(h.DataPoints))
	}
	dp := h.DataPoints[0]
	if dp.Count != 1 {
		t.Fatalf("%s count = %d, want 1", name, dp.Count)
	}
	if dp.Sum < want-0.001 || dp.Sum > want+0.001 {
		t.Fatalf("%s sum = %v seconds, want ~%v", name, dp.Sum, want)
	}
}

func keys(m map[string]metricdata.Metrics) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
