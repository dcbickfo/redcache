package redcache

import (
	"testing"

	"github.com/redis/rueidis"
)

// Compile-time interface check.
var _ Metrics = NoopMetrics{}

func TestNoopMetrics_AllMethodsSafe(t *testing.T) {
	// All methods must be safe to call and never panic.
	m := NoopMetrics{}
	m.CacheHit("x")
	m.CacheMiss("x")
	m.LockContended("x")
	m.LockLost("x")
	m.RefreshTriggered("x")
	m.RefreshSkipped("x")
	m.RefreshDropped("x")
	m.RefreshPanicked("x")
}

func TestValidateAndApplyDefaults_DefaultsToNoopMetrics(t *testing.T) {
	opt := CacheAsideOption{}
	clientOpt := rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}}
	if err := validateAndApplyDefaults(clientOpt, &opt); err != nil {
		t.Fatalf("validateAndApplyDefaults: %v", err)
	}
	if opt.Metrics == nil {
		t.Fatal("Metrics not defaulted")
	}
	if _, ok := opt.Metrics.(NoopMetrics); !ok {
		t.Fatalf("default Metrics is %T, want NoopMetrics", opt.Metrics)
	}
}

// countingMetrics records events for assertions in tests.
type countingMetrics struct {
	NoopMetrics
	Hits, Misses, Contended, Lost int
	Triggered, Skipped, Dropped   int
	Panics                        int
}

func (c *countingMetrics) CacheHit(string)         { c.Hits++ }
func (c *countingMetrics) CacheMiss(string)        { c.Misses++ }
func (c *countingMetrics) LockContended(string)    { c.Contended++ }
func (c *countingMetrics) LockLost(string)         { c.Lost++ }
func (c *countingMetrics) RefreshTriggered(string) { c.Triggered++ }
func (c *countingMetrics) RefreshSkipped(string)   { c.Skipped++ }
func (c *countingMetrics) RefreshDropped(string)   { c.Dropped++ }
func (c *countingMetrics) RefreshPanicked(any)     { c.Panics++ }

func TestCountingMetrics_ImplementsInterface(t *testing.T) {
	var _ Metrics = &countingMetrics{}
}
