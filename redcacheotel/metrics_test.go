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

func TestCounterRecords(t *testing.T) {
	got := collect(t, func(m *Metrics) {
		m.CacheHits(3)
		m.CacheHits(2)
	})

	md, ok := got["cache.hits"]
	if !ok {
		t.Fatalf("cache.hits not recorded; got %v", keys(got))
	}
	sum, ok := md.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("cache.hits is %T, want metricdata.Sum[int64]", md.Data)
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("cache.hits: got %d data points, want 1", len(sum.DataPoints))
	}
	if v := sum.DataPoints[0].Value; v != 5 {
		t.Fatalf("cache.hits value = %d, want 5", v)
	}
}

func TestRedisErrorAttribute(t *testing.T) {
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
	got := collect(t, func(m *Metrics) {
		m.LoaderDuration(150 * time.Millisecond)
	})

	md, ok := got["loader.duration"]
	if !ok {
		t.Fatalf("loader.duration not recorded; got %v", keys(got))
	}
	if md.Unit != "s" {
		t.Fatalf("loader.duration unit = %q, want %q", md.Unit, "s")
	}
	h, ok := md.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("loader.duration is %T, want metricdata.Histogram[float64]", md.Data)
	}
	if len(h.DataPoints) != 1 {
		t.Fatalf("loader.duration: got %d data points, want 1", len(h.DataPoints))
	}
	dp := h.DataPoints[0]
	if dp.Count != 1 {
		t.Fatalf("loader.duration count = %d, want 1", dp.Count)
	}
	if dp.Sum < 0.149 || dp.Sum > 0.151 {
		t.Fatalf("loader.duration sum = %v seconds, want ~0.15", dp.Sum)
	}
}

func keys(m map[string]metricdata.Metrics) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
