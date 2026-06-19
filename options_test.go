package redcache

import (
	"errors"
	"testing"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
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

	cfg := newConfig(
		WithLogger(logger),
		WithLockPrefix("lock:"),
		WithRefreshLockPrefix("refresh:"),
		WithClientBuilder(builder),
	)

	if cfg.logger != logger {
		t.Fatalf("logger = %T, want %T", cfg.logger, logger)
	}
	if cfg.lockPrefix != "lock:" {
		t.Fatalf("lockPrefix = %q, want lock:", cfg.lockPrefix)
	}
	if cfg.refreshLockPrefix != "refresh:" {
		t.Fatalf("refreshLockPrefix = %q, want refresh:", cfg.refreshLockPrefix)
	}
	if cfg.clientBuilder == nil {
		t.Fatal("clientBuilder not configured")
	}
	_, err := cfg.clientBuilder(rueidis.ClientOption{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("clientBuilder error = %v, want %v", err, wantErr)
	}
}

func TestApplyDefaults_RejectsLockPrefixThatMasksEnvelope(t *testing.T) {
	t.Parallel()

	cfg := newConfig(WithLockPrefix("__redcache:v1:"))

	err := cfg.applyDefaults(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflicts with envelope prefix")
}
