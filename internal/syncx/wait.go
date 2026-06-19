package syncx

import (
	"context"
	"sync"
)

// WaitForAll blocks until all channels are closed (or receive a value) or the
// context is cancelled. Returns nil on success, or the context error.
func WaitForAll[C ~<-chan V, V any](ctx context.Context, channels []C) error {
	if len(channels) == 0 {
		return nil
	}

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

	for range len(channels) {
		select {
		case <-done:
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		}
	}
	return nil
}
