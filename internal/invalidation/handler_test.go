//go:build integration

package invalidation_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache/internal/invalidation"
)

// noopLogger is a no-op logger for testing
type noopLogger struct{}

func (noopLogger) Debug(msg string, args ...any) {}
func (noopLogger) Error(msg string, args ...any) {}

// makeHandler creates an invalidation handler for testing
func makeHandler(t *testing.T) invalidation.Handler {
	t.Helper()

	return invalidation.NewRedisInvalidationHandler(invalidation.Config{
		LockTTL: 5 * time.Second,
		Logger:  noopLogger{},
	})
}

// TestHandler_Register tests single key registration
func TestHandler_Register(t *testing.T) {
	handler := makeHandler(t)

	t.Run("returns channel for key", func(t *testing.T) {
		ch := handler.Register("key1")
		require.NotNil(t, ch)

		// Channel should be open initially
		select {
		case <-ch:
			t.Fatal("Channel should not be closed yet")
		default:
			// Expected - channel is still open
		}
	})

	t.Run("multiple registrations for same key share wait channel", func(t *testing.T) {
		ch1 := handler.Register("key2")
		ch2 := handler.Register("key2")

		assert.NotNil(t, ch1)
		assert.NotNil(t, ch2)
		// Both channels should be open initially
		select {
		case <-ch1:
			t.Fatal("ch1 should not be closed yet")
		default:
		}
		select {
		case <-ch2:
			t.Fatal("ch2 should not be closed yet")
		default:
		}
	})
}

// TestHandler_RegisterAll tests batch key registration
func TestHandler_RegisterAll(t *testing.T) {
	handler := makeHandler(t)

	t.Run("registers multiple keys", func(t *testing.T) {
		keys := []string{"m1", "m2", "m3"}
		keysSeq := func(yield func(string) bool) {
			for _, k := range keys {
				if !yield(k) {
					return
				}
			}
		}

		channels := handler.RegisterAll(keysSeq, len(keys))
		assert.Len(t, channels, 3)

		for _, key := range keys {
			ch, exists := channels[key]
			assert.True(t, exists)
			assert.NotNil(t, ch)
		}
	})

	t.Run("handles empty keys", func(t *testing.T) {
		emptySeq := func(yield func(string) bool) {
			// Empty iterator
		}

		channels := handler.RegisterAll(emptySeq, 0)
		assert.Empty(t, channels)
	})
}

// TestHandler_OnInvalidate tests invalidation message processing
func TestHandler_OnInvalidate(t *testing.T) {
	handler := makeHandler(t)

	t.Run("handles empty invalidation messages", func(t *testing.T) {
		// Should not panic
		handler.OnInvalidate(nil)
		handler.OnInvalidate([]rueidis.RedisMessage{})
	})

	// Note: We cannot easily test invalidation message processing without
	// a real Redis connection, as RedisMessage is an opaque type from rueidis.
	// The actual invalidation behavior is tested via integration tests in
	// the main package (cacheaside_test.go, primeable_cacheaside_test.go).
}

// TestWaitForSingleLock tests the wait helper function
func TestWaitForSingleLock(t *testing.T) {
	t.Run("returns immediately when channel closed", func(t *testing.T) {
		ch := make(chan struct{})
		close(ch)

		ctx := context.Background()
		err := invalidation.WaitForSingleLock(ctx, ch, 5*time.Second)
		assert.NoError(t, err)
	})

	t.Run("waits for channel close", func(t *testing.T) {
		ch := make(chan struct{})

		// Close after delay
		go func() {
			time.Sleep(50 * time.Millisecond)
			close(ch)
		}()

		ctx := context.Background()
		start := time.Now()
		err := invalidation.WaitForSingleLock(ctx, ch, 5*time.Second)
		duration := time.Since(start)

		assert.NoError(t, err)
		assert.Greater(t, duration, 40*time.Millisecond)
	})

	t.Run("returns on context cancellation", func(t *testing.T) {
		ch := make(chan struct{})

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := invalidation.WaitForSingleLock(ctx, ch, 5*time.Second)
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("returns on lock TTL timeout", func(t *testing.T) {
		ch := make(chan struct{})

		ctx := context.Background()
		err := invalidation.WaitForSingleLock(ctx, ch, 50*time.Millisecond)
		assert.NoError(t, err) // TTL timeout is not an error, just a signal to retry
	})
}

// TestHandler_TTLExpiration tests channel timeout behavior
func TestHandler_TTLExpiration(t *testing.T) {
	// Use longer TTL for testing timeout (buffer is 20% with 200ms minimum)
	shortHandler := invalidation.NewRedisInvalidationHandler(invalidation.Config{
		LockTTL: 100 * time.Millisecond,
		Logger:  noopLogger{},
	})

	t.Run("channel closes on TTL timeout", func(t *testing.T) {
		ch := shortHandler.Register("timeout1")

		// Wait for channel to close due to TTL
		// TTL is 100ms, buffer is 200ms (minimum), so total is 300ms
		start := time.Now()
		select {
		case <-ch:
			duration := time.Since(start)
			// Should close after TTL + buffer (100ms + 200ms = 300ms)
			assert.Greater(t, duration, 250*time.Millisecond)
			assert.Less(t, duration, 400*time.Millisecond)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Channel should have closed due to TTL timeout")
		}
	})

	t.Run("multiple registrations share timeout", func(t *testing.T) {
		ch1 := shortHandler.Register("timeout2")
		// Give it a moment to establish the entry
		time.Sleep(10 * time.Millisecond)
		ch2 := shortHandler.Register("timeout2")
		ch3 := shortHandler.Register("timeout2")

		// All should close at roughly the same time
		timeout := time.After(500 * time.Millisecond)

		select {
		case <-ch1:
			// Expected
		case <-timeout:
			t.Fatal("ch1 should have closed")
		}

		// ch2 and ch3 should close shortly after (or already be closed)
		select {
		case <-ch2:
			// Expected
		case <-time.After(50 * time.Millisecond):
			t.Fatal("ch2 should have closed")
		}

		select {
		case <-ch3:
			// Expected
		case <-time.After(50 * time.Millisecond):
			t.Fatal("ch3 should have closed")
		}
	})
}

// TestHandler_ConcurrentAccess tests thread safety
func TestHandler_ConcurrentAccess(t *testing.T) {
	handler := makeHandler(t)

	t.Run("concurrent registrations", func(t *testing.T) {
		done := make(chan bool)

		// Spawn multiple goroutines registering keys
		for i := 0; i < 10; i++ {
			go func(id int) {
				for j := 0; j < 100; j++ {
					handler.Register("concurrent")
				}
				done <- true
			}(i)
		}

		// Wait for all to complete
		for i := 0; i < 10; i++ {
			<-done
		}

		// Should not panic or race
	})

	t.Run("concurrent register and onInvalidate", func(t *testing.T) {
		done := make(chan bool)

		// Spawn registerers
		go func() {
			for i := 0; i < 50; i++ {
				handler.Register("race")
				time.Sleep(1 * time.Millisecond)
			}
			done <- true
		}()

		// Spawn invalidators calling OnInvalidate with empty messages
		go func() {
			for i := 0; i < 50; i++ {
				handler.OnInvalidate(nil)
				time.Sleep(1 * time.Millisecond)
			}
			done <- true
		}()

		// Wait for both
		<-done
		<-done

		// Should not panic or race
	})
}

// TestHandler_IteratorConversion tests iter.Seq usage
func TestHandler_IteratorConversion(t *testing.T) {
	handler := makeHandler(t)

	t.Run("slice to iterator conversion", func(t *testing.T) {
		keys := []string{"s1", "s2", "s3"}

		// Convert slice to iter.Seq
		keysSeq := func(yield func(string) bool) {
			for _, k := range keys {
				if !yield(k) {
					return
				}
			}
		}

		channels := handler.RegisterAll(keysSeq, len(keys))
		assert.Len(t, channels, 3)
	})

	t.Run("generator iterator", func(t *testing.T) {
		// Create iterator that generates keys
		genSeq := func(yield func(string) bool) {
			for i := 0; i < 5; i++ {
				if !yield("gen" + string(rune('0'+i))) {
					return
				}
			}
		}

		channels := handler.RegisterAll(genSeq, 5)
		assert.Len(t, channels, 5)
	})
}
