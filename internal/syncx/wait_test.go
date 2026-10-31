package syncx_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dcbickfo/redcache/internal/syncx"
)

func delayedSend[T any](ch chan T, val T, delay time.Duration) {
	go func() {
		time.Sleep(delay)
		ch <- val
		close(ch)
	}()
}

func delayedClose[T any](ch chan T, delay time.Duration) {
	go func() {
		time.Sleep(delay)
		close(ch)
	}()
}

func TestWaitForAll_Success(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		ch1 := make(chan struct{})
		ch2 := make(chan struct{})

		delayedSend(ch1, struct{}{}, 100*time.Millisecond)
		delayedSend(ch2, struct{}{}, 200*time.Millisecond)

		waitLock := []<-chan struct{}{ch1, ch2}

		err := syncx.WaitForAll(ctx, waitLock)
		assert.NoErrorf(t, err, "expected no error, got %v", err)
	})
}

func TestWaitForAll_SuccessClosed(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		ch1 := make(chan struct{})
		ch2 := make(chan struct{})

		delayedClose(ch1, 100*time.Millisecond)
		delayedClose(ch2, 200*time.Millisecond)

		waitLock := []<-chan struct{}{ch1, ch2}

		err := syncx.WaitForAll(ctx, waitLock)
		assert.NoErrorf(t, err, "expected no error, got %v", err)
	})
}

// assertDeadlineExceeded waits on two delayed senders under a context that
// times out first, and asserts WaitForAll reports context.DeadlineExceeded.
func assertDeadlineExceeded(t *testing.T, timeout, delay1, delay2 time.Duration) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), timeout)
		defer cancel()

		ch1 := make(chan int)
		ch2 := make(chan int)

		delayedSend(ch1, 1, delay1)
		delayedSend(ch2, 2, delay2)

		waitLock := []<-chan int{ch1, ch2}

		err := syncx.WaitForAll(ctx, waitLock)
		assert.ErrorIsf(t, err, context.DeadlineExceeded, "expected context.DeadlineExceeded, got %v", err)
		// Drain so the late senders exit inside the bubble.
		<-ch1
		<-ch2
	})
}

func TestWaitForAll_ContextCancelled(t *testing.T) {
	t.Parallel()
	assertDeadlineExceeded(t, 150*time.Millisecond, 200*time.Millisecond, 300*time.Millisecond)
}

func TestWaitForAll_PartialCompleteContextCancelled(t *testing.T) {
	t.Parallel()
	assertDeadlineExceeded(t, 200*time.Millisecond, 100*time.Millisecond, 300*time.Millisecond)
}

func TestWaitForAll_NoChannels(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var waitLock []<-chan int

	err := syncx.WaitForAll(ctx, waitLock)
	assert.NoErrorf(t, err, "expected no error, got %v", err)
}

func TestWaitForAll_ImmediateContextCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	ch1 := make(chan int)
	ch2 := make(chan int)

	waitLock := []<-chan int{ch1, ch2}

	err := syncx.WaitForAll(ctx, waitLock)
	assert.ErrorIsf(t, err, context.Canceled, "expected context.Canceled, got %v", err)
}

func TestWaitForAll_ChannelAlreadyClosed(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	ch1 := make(chan int)
	ch2 := make(chan int)

	close(ch1)
	close(ch2)

	waitLock := []<-chan int{ch1, ch2}

	err := syncx.WaitForAll(ctx, waitLock)
	assert.NoErrorf(t, err, "expected no error, got %v", err)
}
