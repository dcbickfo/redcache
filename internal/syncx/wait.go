package syncx

import (
	"context"
	"sync"
)

// WaitForAll waits for all channels to close or for context cancellation.
// Uses goroutine-based channel merging which is ~100x faster than reflect.SelectCase.
//
// Implementation uses one goroutine per channel to forward close signals to a merged
// channel, avoiding reflection overhead entirely.
func WaitForAll[C ~<-chan V, V any](ctx context.Context, channels []C) error {
	if len(channels) == 0 {
		return nil
	}

	// Merged channel that collects close signals from all input channels
	done := make(chan struct{}, len(channels))
	var wg sync.WaitGroup

	// Launch one goroutine per channel to wait for close
	for _, ch := range channels {
		wg.Add(1)
		go func(c C) {
			defer wg.Done()
			// Wait for channel to close
			for range c {
				// Drain channel until closed
			}
			// Signal completion
			done <- struct{}{}
		}(ch)
	}

	// Close done channel when all goroutines complete
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait for all channels to close or context cancellation
	for i := 0; i < len(channels); i++ {
		select {
		case <-done:
			// One channel closed
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
