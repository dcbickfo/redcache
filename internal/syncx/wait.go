package syncx

import (
	"context"
	"sync"
)

// WaitForAll blocks until all channels are closed (or receive a value) or the context is cancelled.
// Returns nil if all channels signaled, or the context error if cancelled first.
func WaitForAll[C ~<-chan V, V any](ctx context.Context, channels []C) error {
	if len(channels) == 0 {
		return nil
	}

	// Merged channel: each goroutine sends one signal when its channel fires.
	done := make(chan struct{}, len(channels))
	var wg sync.WaitGroup
	wg.Add(len(channels))

	for _, ch := range channels {
		go func() {
			defer wg.Done()
			select {
			case <-ch:
				done <- struct{}{}
			case <-ctx.Done():
			}
		}()
	}

	// Wait for all signals or context cancellation.
	for range len(channels) {
		select {
		case <-done:
		case <-ctx.Done():
			// Ensure all goroutines finish before returning.
			wg.Wait()
			return ctx.Err()
		}
	}
	return nil
}
