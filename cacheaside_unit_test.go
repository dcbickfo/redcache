package redcache

import (
	"testing"
	"time"
)

func TestShouldRefresh_Deterministic(t *testing.T) {
	t.Parallel()
	// These cases all resolve without invoking the XFetch sampler:
	// either refresh is disabled, the value is above the floor, or the
	// value is below the floor with no XFetch metadata (delta=0) so the
	// fallback "always refresh below floor" kicks in.
	tests := []struct {
		name         string
		refreshAfter float64
		refreshBeta  float64
		cachePTTL    int64
		ttl          time.Duration
		delta        time.Duration
		want         bool
	}{
		{"disabled when refreshAfter is 0", 0, 1, 500, time.Second, time.Millisecond, false},
		{"disabled even with low pttl", 0, 1, 1, time.Second, time.Millisecond, false},
		{"false when cachePTTL is 0", 0.8, 1, 0, time.Second, time.Millisecond, false},
		{"false when cachePTTL is negative", 0.8, 1, -1, time.Second, time.Millisecond, false},
		{"above floor", 0.8, 1, 500, time.Second, time.Millisecond, false},
		{"at exact floor counts as above", 0.8, 1, 200, time.Second, time.Millisecond, false},
		{"above floor at 60% remaining (refreshAfter=0.5)", 0.5, 1, 600, time.Second, time.Millisecond, false},
		{"below floor with delta=0 falls back to always-refresh", 0.8, 1, 100, time.Second, 0, true},
		{"below floor with refreshBeta=0 falls back to always-refresh", 0.8, 0, 100, time.Second, time.Millisecond, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rca := &CacheAside{refreshAfter: tt.refreshAfter, refreshBeta: tt.refreshBeta}
			got := rca.shouldRefresh(tt.cachePTTL, tt.ttl, tt.delta)
			if got != tt.want {
				t.Errorf("shouldRefresh(pttl=%d ttl=%v delta=%v) = %v, want %v",
					tt.cachePTTL, tt.ttl, tt.delta, got, tt.want)
			}
		})
	}
}

// TestShouldRefresh_XFetch covers the probabilistic path: below floor, delta>0,
// beta>0. Asserts rates fall in expected ranges over many trials rather than
// exact thresholds.
func TestShouldRefresh_XFetch(t *testing.T) {
	t.Parallel()
	const trials = 5000
	rca := &CacheAside{refreshAfter: 0.8, refreshBeta: 1.0}
	ttl := time.Second

	t.Run("near expiry refreshes almost always", func(t *testing.T) {
		t.Parallel()
		// cachePTTL=1ms, delta=100ms → jitter ~ delta * Exp(1) (mean=100ms);
		// P(jitter >= 1ms) ≈ 0.99.
		hits := 0
		for range trials {
			if rca.shouldRefresh(1, ttl, 100*time.Millisecond) {
				hits++
			}
		}
		rate := float64(hits) / trials
		if rate < 0.95 {
			t.Errorf("near-expiry refresh rate = %.3f, want >= 0.95", rate)
		}
	})

	t.Run("just below floor with tiny delta rarely refreshes", func(t *testing.T) {
		t.Parallel()
		// cachePTTL=199ms (just below floor=200ms), delta=1ms → jitter mean = 1ms;
		// P(jitter >= 199ms) ≈ exp(-199) ≈ 0.
		hits := 0
		for range trials {
			if rca.shouldRefresh(199, ttl, time.Millisecond) {
				hits++
			}
		}
		if hits > 5 {
			t.Errorf("tiny-delta refresh count = %d/%d, want ~0", hits, trials)
		}
	})

	t.Run("higher beta increases refresh rate", func(t *testing.T) {
		t.Parallel()
		// Same cachePTTL/delta, beta=10 should refresh substantially more than beta=1.
		low := &CacheAside{refreshAfter: 0.8, refreshBeta: 1.0}
		high := &CacheAside{refreshAfter: 0.8, refreshBeta: 10.0}
		var lowHits, highHits int
		for range trials {
			if low.shouldRefresh(50, ttl, 10*time.Millisecond) {
				lowHits++
			}
			if high.shouldRefresh(50, ttl, 10*time.Millisecond) {
				highHits++
			}
		}
		if highHits <= lowHits {
			t.Errorf("expected higher beta to refresh more: low=%d high=%d", lowHits, highHits)
		}
	})
}

func TestValidateRefreshDefaults(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		opt         CacheAsideOption
		wantErr     bool
		wantWorkers int
		wantQSize   int
	}{
		{
			name:    "disabled (RefreshAfterFraction=0)",
			opt:     CacheAsideOption{RefreshAfterFraction: 0},
			wantErr: false,
		},
		{
			name:    "negative fraction rejected",
			opt:     CacheAsideOption{RefreshAfterFraction: -0.1},
			wantErr: true,
		},
		{
			name:    "fraction of 1.0 rejected",
			opt:     CacheAsideOption{RefreshAfterFraction: 1.0},
			wantErr: true,
		},
		{
			name:    "fraction above 1.0 rejected",
			opt:     CacheAsideOption{RefreshAfterFraction: 1.5},
			wantErr: true,
		},
		{
			name:        "fraction near upper bound",
			opt:         CacheAsideOption{RefreshAfterFraction: 0.999},
			wantErr:     false,
			wantWorkers: 4,
			wantQSize:   64,
		},
		{
			name:    "negative workers rejected",
			opt:     CacheAsideOption{RefreshAfterFraction: 0.8, RefreshWorkers: -1},
			wantErr: true,
		},
		{
			name:    "negative queue size rejected",
			opt:     CacheAsideOption{RefreshAfterFraction: 0.8, RefreshQueueSize: -1},
			wantErr: true,
		},
		{
			name:        "defaults applied when enabled",
			opt:         CacheAsideOption{RefreshAfterFraction: 0.8},
			wantErr:     false,
			wantWorkers: 4,
			wantQSize:   64,
		},
		{
			name:        "explicit workers/queue size honored",
			opt:         CacheAsideOption{RefreshAfterFraction: 0.8, RefreshWorkers: 8, RefreshQueueSize: 128},
			wantErr:     false,
			wantWorkers: 8,
			wantQSize:   128,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opt := tt.opt
			err := validateRefreshDefaults(&opt)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateRefreshDefaults() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if opt.RefreshWorkers != tt.wantWorkers {
				t.Errorf("RefreshWorkers = %d, want %d", opt.RefreshWorkers, tt.wantWorkers)
			}
			if opt.RefreshQueueSize != tt.wantQSize {
				t.Errorf("RefreshQueueSize = %d, want %d", opt.RefreshQueueSize, tt.wantQSize)
			}
		})
	}
}

func TestRefreshKeyFor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		prefix string
		key    string
		want   string
	}{
		{"default prefix", "__redcache:refresh:", "user:123", "__redcache:refresh:{user:123}"},
		{"custom prefix", "rc:r:", "abc", "rc:r:{abc}"},
		{"empty key", "p:", "", "p:{}"},
		{"key with hash tag inside", "p:", "user:{tenant}:1", "p:{user:{tenant}:1}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rca := &CacheAside{refreshPrefix: tt.prefix}
			got := rca.refreshKeyFor(tt.key)
			if got != tt.want {
				t.Errorf("refreshKeyFor(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}
