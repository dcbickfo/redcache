package redcache_test

import (
	"errors"
	"testing"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

type testLogger struct{}

func (testLogger) Error(string, ...any) {}
func (testLogger) Debug(string, ...any) {}

func TestNewConfig_OptionSetters(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("builder failed")
	builder := func(rueidis.ClientOption) (rueidis.Client, error) {
		return nil, wantErr
	}
	logger := testLogger{}

	gotLogger, lockPrefix, refreshLockPrefix, clientBuilder := redcache.ConfiguredOptions(
		redcache.WithLogger(logger),
		redcache.WithLockPrefix("lock:"),
		redcache.WithRefreshLockPrefix("refresh:"),
		redcache.WithClientBuilder(builder),
	)

	if gotLogger != logger {
		t.Fatalf("logger = %T, want %T", gotLogger, logger)
	}
	if lockPrefix != "lock:" {
		t.Fatalf("lockPrefix = %q, want lock:", lockPrefix)
	}
	if refreshLockPrefix != "refresh:" {
		t.Fatalf("refreshLockPrefix = %q, want refresh:", refreshLockPrefix)
	}
	if clientBuilder == nil {
		t.Fatal("clientBuilder not configured")
	}
	_, err := clientBuilder(rueidis.ClientOption{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("clientBuilder error = %v, want %v", err, wantErr)
	}
}

func TestApplyDefaults_RejectsLockPrefixThatMasksEnvelope(t *testing.T) {
	t.Parallel()

	_, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.WithLockPrefix("__redcache:v1:"),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflicts with envelope prefix")
}

func TestValidateRefreshDefaults(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		fraction    float64
		workers     int
		queueSize   int
		wantErr     bool
		wantWorkers int
		wantQSize   int
	}{
		{
			name:     "disabled (refreshAfterFraction=0)",
			fraction: 0,
			wantErr:  false,
		},
		{
			name:     "negative fraction rejected",
			fraction: -0.1,
			wantErr:  true,
		},
		{
			name:     "fraction of 1.0 rejected",
			fraction: 1.0,
			wantErr:  true,
		},
		{
			name:     "fraction above 1.0 rejected",
			fraction: 1.5,
			wantErr:  true,
		},
		{
			name:        "fraction near upper bound",
			fraction:    0.999,
			wantErr:     false,
			wantWorkers: 4,
			wantQSize:   64,
		},
		{
			name:     "negative workers rejected",
			fraction: 0.8,
			workers:  -1,
			wantErr:  true,
		},
		{
			name:      "negative queue size rejected",
			fraction:  0.8,
			queueSize: -1,
			wantErr:   true,
		},
		{
			name:        "defaults applied when enabled",
			fraction:    0.8,
			wantErr:     false,
			wantWorkers: 4,
			wantQSize:   64,
		},
		{
			name:        "explicit workers/queue size honored",
			fraction:    0.8,
			workers:     8,
			queueSize:   128,
			wantErr:     false,
			wantWorkers: 8,
			wantQSize:   128,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			workers, queueSize, err := redcache.ApplyRefreshDefaultsForTest(tt.fraction, tt.workers, tt.queueSize)
			if (err != nil) != tt.wantErr {
				t.Fatalf("applyRefreshDefaults() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if workers != tt.wantWorkers {
				t.Errorf("refreshWorkers = %d, want %d", workers, tt.wantWorkers)
			}
			if queueSize != tt.wantQSize {
				t.Errorf("refreshQueueSize = %d, want %d", queueSize, tt.wantQSize)
			}
		})
	}
}
