package syncx

import "context"

// WaitForAll blocks until all channels are closed (or receive a value) or the
// context is cancelled. Returns nil on success, or the context error. Channels
// are waited on in turn: the total wait is the slowest channel either way, and
// no goroutines are started.
func WaitForAll[C ~<-chan V, V any](ctx context.Context, channels []C) error {
	for _, ch := range channels {
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
