package redcache

import (
	"testing"
	"time"

	"github.com/redis/rueidis"
)

// Compile-time interface check.
var _ Metrics = NoopMetrics{}

func TestNoopMetrics_AllMethodsSafe(t *testing.T) {
	t.Parallel()
	m := NoopMetrics{}
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
	cfg := config{}
	clientOpt := rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}}
	if err := cfg.applyDefaults(clientOpt); err != nil {
		t.Fatalf("applyDefaults: %v", err)
	}
	if cfg.metrics == nil {
		t.Fatal("metrics not defaulted")
	}
	if _, ok := cfg.metrics.(NoopMetrics); !ok {
		t.Fatalf("default metrics is %T, want NoopMetrics", cfg.metrics)
	}
}

type countingMetrics struct {
	NoopMetrics
	Hits, Misses, Contended, Lost int
	Triggered, Skipped, Dropped   int
	Panics                        int
	RefreshErrors                 int
	InvalidationErrors            int
}

func (c *countingMetrics) CacheHits(n int64)        { c.Hits += int(n) }
func (c *countingMetrics) CacheMisses(n int64)      { c.Misses += int(n) }
func (c *countingMetrics) LockContended(n int64)    { c.Contended += int(n) }
func (c *countingMetrics) LockLost(string)          { c.Lost++ }
func (c *countingMetrics) RefreshTriggered(n int64) { c.Triggered += int(n) }
func (c *countingMetrics) RefreshSkipped(n int64)   { c.Skipped += int(n) }
func (c *countingMetrics) RefreshDropped(n int64)   { c.Dropped += int(n) }
func (c *countingMetrics) RefreshPanicked(string)   { c.Panics++ }
func (c *countingMetrics) RefreshError(string)      { c.RefreshErrors++ }
func (c *countingMetrics) InvalidationError()       { c.InvalidationErrors++ }

func TestCountingMetrics_ImplementsInterface(t *testing.T) {
	t.Parallel()
	var _ Metrics = &countingMetrics{}
}
