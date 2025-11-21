package contextx_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache/internal/contextx"
)

func TestWithCleanupTimeout(t *testing.T) {
	t.Run("creates context that survives parent cancellation", func(t *testing.T) {
		// Create parent context that we'll cancel
		parentCtx, parentCancel := context.WithCancel(context.Background())
		defer parentCancel()

		// Create cleanup context
		cleanupCtx, cleanupCancel := contextx.WithCleanupTimeout(parentCtx, 5*time.Second)
		defer cleanupCancel()

		// Cancel parent immediately
		parentCancel()

		// Cleanup context should still be valid
		select {
		case <-cleanupCtx.Done():
			t.Fatal("Cleanup context should not be cancelled when parent is cancelled")
		case <-time.After(10 * time.Millisecond):
			// Success - cleanup context is still active
		}
	})

	t.Run("applies timeout to cleanup context", func(t *testing.T) {
		parentCtx := context.Background()

		// Create cleanup context with very short timeout
		cleanupCtx, cleanupCancel := contextx.WithCleanupTimeout(parentCtx, 50*time.Millisecond)
		defer cleanupCancel()

		// Wait for timeout
		select {
		case <-cleanupCtx.Done():
			// Success - context timed out
			assert.ErrorIs(t, cleanupCtx.Err(), context.DeadlineExceeded)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Cleanup context should have timed out")
		}
	})

	t.Run("cancel function releases resources", func(t *testing.T) {
		parentCtx := context.Background()

		cleanupCtx, cleanupCancel := contextx.WithCleanupTimeout(parentCtx, 5*time.Second)

		// Cancel explicitly
		cleanupCancel()

		// Context should be done
		select {
		case <-cleanupCtx.Done():
			// Success - context was cancelled
			assert.ErrorIs(t, cleanupCtx.Err(), context.Canceled)
		case <-time.After(10 * time.Millisecond):
			t.Fatal("Cleanup context should be cancelled after calling cancel function")
		}
	})

	t.Run("inherits values from parent context", func(t *testing.T) {
		type contextKey string
		const key contextKey = "test-key"
		expectedValue := "test-value"

		// Create parent with value
		parentCtx := context.WithValue(context.Background(), key, expectedValue)

		// Create cleanup context
		cleanupCtx, cleanupCancel := contextx.WithCleanupTimeout(parentCtx, 5*time.Second)
		defer cleanupCancel()

		// Value should be accessible in cleanup context
		actualValue := cleanupCtx.Value(key)
		require.NotNil(t, actualValue)
		assert.Equal(t, expectedValue, actualValue.(string))
	})

	t.Run("timeout occurs before parent cancellation", func(t *testing.T) {
		// Create parent that will be cancelled later
		parentCtx, parentCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer parentCancel()

		// Create cleanup context with shorter timeout
		cleanupCtx, cleanupCancel := contextx.WithCleanupTimeout(parentCtx, 50*time.Millisecond)
		defer cleanupCancel()

		// Wait for cleanup context to timeout (should happen first)
		select {
		case <-cleanupCtx.Done():
			// Success - cleanup context timed out before parent
			assert.ErrorIs(t, cleanupCtx.Err(), context.DeadlineExceeded)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Cleanup context should have timed out")
		}

		// Parent should still be active at this point
		select {
		case <-parentCtx.Done():
			t.Fatal("Parent context should still be active")
		default:
			// Success - parent is still active
		}
	})
}
