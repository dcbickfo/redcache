package redcache

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestShouldRefresh_Deterministic(t *testing.T) {
	t.Parallel()
	// Cases that resolve without invoking the XFetch sampler.
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
			rca := &cacheAside{refreshAfter: tt.refreshAfter, refreshBeta: tt.refreshBeta}
			got := rca.shouldRefresh(tt.cachePTTL, tt.ttl, tt.delta)
			if got != tt.want {
				t.Errorf("shouldRefresh(pttl=%d ttl=%v delta=%v) = %v, want %v",
					tt.cachePTTL, tt.ttl, tt.delta, got, tt.want)
			}
		})
	}
}

// TestShouldRefresh_XFetch covers the probabilistic path (below floor, delta>0,
// beta>0) by asserting refresh rates fall in expected ranges over many trials.
func TestShouldRefresh_XFetch(t *testing.T) {
	t.Parallel()
	const trials = 5000
	rca := &cacheAside{refreshAfter: 0.8, refreshBeta: 1.0}
	ttl := time.Second

	t.Run("near expiry refreshes almost always", func(t *testing.T) {
		t.Parallel()
		// jitter ~ delta * Exp(1) (mean=100ms); P(jitter >= 1ms) ≈ 0.99.
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
		// jitter mean = 1ms; P(jitter >= 199ms) ≈ exp(-199) ≈ 0.
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
		low := &cacheAside{refreshAfter: 0.8, refreshBeta: 1.0}
		high := &cacheAside{refreshAfter: 0.8, refreshBeta: 10.0}
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
		cfg         config
		wantErr     bool
		wantWorkers int
		wantQSize   int
	}{
		{
			name:    "disabled (refreshAfterFraction=0)",
			cfg:     config{refreshAfterFraction: 0},
			wantErr: false,
		},
		{
			name:    "negative fraction rejected",
			cfg:     config{refreshAfterFraction: -0.1},
			wantErr: true,
		},
		{
			name:    "fraction of 1.0 rejected",
			cfg:     config{refreshAfterFraction: 1.0},
			wantErr: true,
		},
		{
			name:    "fraction above 1.0 rejected",
			cfg:     config{refreshAfterFraction: 1.5},
			wantErr: true,
		},
		{
			name:        "fraction near upper bound",
			cfg:         config{refreshAfterFraction: 0.999},
			wantErr:     false,
			wantWorkers: 4,
			wantQSize:   64,
		},
		{
			name:    "negative workers rejected",
			cfg:     config{refreshAfterFraction: 0.8, refreshWorkers: -1},
			wantErr: true,
		},
		{
			name:    "negative queue size rejected",
			cfg:     config{refreshAfterFraction: 0.8, refreshQueueSize: -1},
			wantErr: true,
		},
		{
			name:        "defaults applied when enabled",
			cfg:         config{refreshAfterFraction: 0.8},
			wantErr:     false,
			wantWorkers: 4,
			wantQSize:   64,
		},
		{
			name:        "explicit workers/queue size honored",
			cfg:         config{refreshAfterFraction: 0.8, refreshWorkers: 8, refreshQueueSize: 128},
			wantErr:     false,
			wantWorkers: 8,
			wantQSize:   128,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := tt.cfg
			err := cfg.applyRefreshDefaults()
			if (err != nil) != tt.wantErr {
				t.Fatalf("applyRefreshDefaults() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if cfg.refreshWorkers != tt.wantWorkers {
				t.Errorf("refreshWorkers = %d, want %d", cfg.refreshWorkers, tt.wantWorkers)
			}
			if cfg.refreshQueueSize != tt.wantQSize {
				t.Errorf("refreshQueueSize = %d, want %d", cfg.refreshQueueSize, tt.wantQSize)
			}
		})
	}
}

func TestAwaitLockOrPoll_PollCanResolveBeforeWaitChannel(t *testing.T) {
	t.Parallel()
	rca := &cacheAside{lockTTL: 100 * time.Millisecond}
	wait := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	polls := 0
	start := time.Now()
	resolved, err := rca.awaitLockOrPoll(ctx, wait, func() (bool, error) {
		polls++
		return true, nil
	})

	if err != nil {
		t.Fatalf("awaitLockOrPoll returned error: %v", err)
	}
	if !resolved {
		t.Fatal("awaitLockOrPoll did not report poll resolution")
	}
	if polls != 1 {
		t.Fatalf("polls = %d, want 1", polls)
	}
	if elapsed := time.Since(start); elapsed >= rca.lockTTL {
		t.Fatalf("poll fallback took %v, want less than lockTTL %v", elapsed, rca.lockTTL)
	}
}

func TestAwaitLockOrPoll_WaitChannelWins(t *testing.T) {
	t.Parallel()
	rca := &cacheAside{lockTTL: time.Second}
	wait := make(chan struct{})
	close(wait)

	resolved, err := rca.awaitLockOrPoll(context.Background(), wait, func() (bool, error) {
		t.Fatal("poll should not run after wait channel closes")
		return false, nil
	})

	if err != nil {
		t.Fatalf("awaitLockOrPoll returned error: %v", err)
	}
	if resolved {
		t.Fatal("awaitLockOrPoll reported poll resolution after wait channel closed")
	}
}

func TestAwaitLockOrPoll_PollError(t *testing.T) {
	t.Parallel()
	rca := &cacheAside{lockTTL: 100 * time.Millisecond}
	wait := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wantErr := errors.New("poll failed")

	resolved, err := rca.awaitLockOrPoll(ctx, wait, func() (bool, error) {
		return false, wantErr
	})

	if !errors.Is(err, wantErr) {
		t.Fatalf("awaitLockOrPoll error = %v, want %v", err, wantErr)
	}
	if resolved {
		t.Fatal("awaitLockOrPoll reported poll resolution on poll error")
	}
}

func TestAwaitLockMultiOrPoll_PollCanResolveBeforeAllWaitChannels(t *testing.T) {
	t.Parallel()
	rca := &cacheAside{lockTTL: 100 * time.Millisecond}
	wait1 := make(chan struct{})
	wait2 := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resolved, err := rca.awaitLockMultiOrPoll(ctx, []<-chan struct{}{wait1, wait2}, func() (bool, error) {
		return true, nil
	})

	if err != nil {
		t.Fatalf("awaitLockMultiOrPoll returned error: %v", err)
	}
	if !resolved {
		t.Fatal("awaitLockMultiOrPoll did not report poll resolution")
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
			rca := &cacheAside{refreshPrefix: tt.prefix}
			got := rca.refreshKeyFor(tt.key)
			if got != tt.want {
				t.Errorf("refreshKeyFor(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}

func TestShouldRunRefreshJob_DropsDequeuedJobAfterClose(t *testing.T) {
	t.Parallel()

	rca := &cacheAside{refreshDone: make(chan struct{})}
	key := "queued-after-close"
	rca.refreshing.Store(key, struct{}{})
	close(rca.refreshDone)

	if rca.shouldRunRefreshJob(refreshJob{keys: []string{key}}) {
		t.Fatal("refresh job should not run after Close begins")
	}
	if _, ok := rca.refreshing.Load(key); ok {
		t.Fatal("dropped refresh job did not clear local dedup marker")
	}
}

func TestEnqueueRefresh_DropsInsteadOfQueueingAfterClose(t *testing.T) {
	t.Parallel()

	for range 1000 {
		rca := &cacheAside{
			refreshQueue: make(chan refreshJob, 1),
			refreshDone:  make(chan struct{}),
		}
		key := "enqueue-after-close"
		rca.refreshing.Store(key, struct{}{})
		close(rca.refreshDone)

		rca.enqueueRefresh(refreshJob{keys: []string{key}}, []string{key})

		if len(rca.refreshQueue) != 0 {
			t.Fatal("refresh job was queued after Close began")
		}
		if _, ok := rca.refreshing.Load(key); ok {
			t.Fatal("dropped refresh job did not clear local dedup marker")
		}
	}
}
