package redcache_test

import (
	"testing"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

// Compile-time interface check.
var _ redcache.Metrics = redcache.NoopMetrics{}

func TestNoopMetrics_AllMethodsSafe(t *testing.T) {
	t.Parallel()
	m := redcache.NoopMetrics{}
	m.CacheHits(1)
	m.CacheMisses(1)
	m.LockContended(1)
	m.LockWaitDuration(time.Millisecond)
	m.LoaderDuration(time.Millisecond)
	m.LoaderErrors(1)
	m.RedisError("read")
	m.LockLost("x")
	m.RefreshTriggered(1)
	m.RefreshSkipped(1)
	m.RefreshDropped(1)
	m.RefreshPanicked("x")
	m.RefreshError("x")
	m.InvalidationError()
}

func TestValidateAndApplyDefaults_DefaultsToNoopMetrics(t *testing.T) {
	t.Parallel()
	clientOpt := rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}}
	metrics, err := redcache.DefaultedMetrics(clientOpt)
	if err != nil {
		t.Fatalf("applyDefaults: %v", err)
	}
	if metrics == nil {
		t.Fatal("metrics not defaulted")
	}
	if _, ok := metrics.(redcache.NoopMetrics); !ok {
		t.Fatalf("default metrics is %T, want NoopMetrics", metrics)
	}
}

type metricsTestCounter struct {
	redcache.NoopMetrics
	Hits, Misses, Contended, Lost int
	Triggered, Skipped, Dropped   int
	Panics                        int
	RefreshErrors                 int
	InvalidationErrors            int
}

func (c *metricsTestCounter) CacheHits(n int64)        { c.Hits += int(n) }
func (c *metricsTestCounter) CacheMisses(n int64)      { c.Misses += int(n) }
func (c *metricsTestCounter) LockContended(n int64)    { c.Contended += int(n) }
func (c *metricsTestCounter) LockLost(string)          { c.Lost++ }
func (c *metricsTestCounter) RefreshTriggered(n int64) { c.Triggered += int(n) }
func (c *metricsTestCounter) RefreshSkipped(n int64)   { c.Skipped += int(n) }
func (c *metricsTestCounter) RefreshDropped(n int64)   { c.Dropped += int(n) }
func (c *metricsTestCounter) RefreshPanicked(string)   { c.Panics++ }
func (c *metricsTestCounter) RefreshError(string)      { c.RefreshErrors++ }
func (c *metricsTestCounter) InvalidationError()       { c.InvalidationErrors++ }

func TestCountingMetrics_ImplementsInterface(t *testing.T) {
	t.Parallel()
	var _ redcache.Metrics = &metricsTestCounter{}
}
