package redcache

import (
	"testing"
	"time"
)

func TestShouldRefresh(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		refreshAfter float64
		cachePTTL    int64
		ttl          time.Duration
		want         bool
	}{
		{"disabled when refreshAfter is 0", 0, 500, time.Second, false},
		{"disabled even with low pttl", 0, 1, time.Second, false},
		{"false when cachePTTL is 0", 0.8, 0, time.Second, false},
		{"false when cachePTTL is negative", 0.8, -1, time.Second, false},
		{"false when cachePTTL above threshold (refreshAfter=0.8)", 0.8, 500, time.Second, false},
		{"true when cachePTTL below threshold (refreshAfter=0.8)", 0.8, 100, time.Second, true},
		{"false when cachePTTL equals threshold", 0.8, 200, time.Second, false},
		{"true at 40% remaining with refreshAfter=0.5", 0.5, 400, time.Second, true},
		{"false at 60% remaining with refreshAfter=0.5", 0.5, 600, time.Second, false},
		{"true near expiration with refreshAfter=0.9", 0.9, 50, time.Second, true},
		{"false just above 10% remaining with refreshAfter=0.9", 0.9, 150, time.Second, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rca := &CacheAside{refreshAfter: tt.refreshAfter}
			got := rca.shouldRefresh(tt.cachePTTL, tt.ttl)
			if got != tt.want {
				t.Errorf("shouldRefresh(cachePTTL=%d, ttl=%v) = %v, want %v (refreshAfter=%v)",
					tt.cachePTTL, tt.ttl, got, tt.want, tt.refreshAfter)
			}
		})
	}
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
