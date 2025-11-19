package lockutil_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache/internal/lockutil"
)

func TestPrefixLockChecker_HasLock(t *testing.T) {
	checker := &lockutil.PrefixLockChecker{Prefix: "__lock:"}

	tests := []struct {
		name     string
		value    string
		expected bool
	}{
		{"lock value", "__lock:abc123", true},
		{"real value", "some-data", false},
		{"empty value", "", false},
		{"partial prefix", "__loc", false},
		{"prefix at end", "data__lock:", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := checker.HasLock(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestWaitForSingleLock_ImmediateRelease(t *testing.T) {
	ctx := context.Background()
	waitChan := make(chan struct{})

	// Close channel immediately to simulate lock release
	close(waitChan)

	err := lockutil.WaitForSingleLock(ctx, waitChan, time.Second)
	require.NoError(t, err)
}

func TestWaitForSingleLock_Timeout(t *testing.T) {
	ctx := context.Background()
	waitChan := make(chan struct{})

	// Don't close channel, let it timeout
	err := lockutil.WaitForSingleLock(ctx, waitChan, 50*time.Millisecond)
	require.NoError(t, err, "timeout should not return error")
}

func TestWaitForSingleLock_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	waitChan := make(chan struct{})

	// Cancel context immediately
	cancel()

	err := lockutil.WaitForSingleLock(ctx, waitChan, time.Second)
	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestWaitForSingleLock_ContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	waitChan := make(chan struct{})

	err := lockutil.WaitForSingleLock(ctx, waitChan, time.Second)
	require.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
}
