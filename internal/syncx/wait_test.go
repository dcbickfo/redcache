package syncx_test

import (
	"context"
	"testing"
	"time"

	"github.com/dcbickfo/redcache/internal/syncx"
	"github.com/stretchr/testify/assert"
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
	ctx := context.Background()
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})

	delayedSend(ch1, struct{}{}, 100*time.Millisecond)
	delayedSend(ch2, struct{}{}, 200*time.Millisecond)

	waitLock := []<-chan struct{}{ch1, ch2}

	err := syncx.WaitForAll(ctx, waitLock)
	assert.NoErrorf(t, err, "expected no error, got %v", err)
}

func TestWaitForAll_SuccessClosed(t *testing.T) {
	ctx := context.Background()
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})

	delayedClose(ch1, 100*time.Millisecond)
	delayedClose(ch2, 200*time.Millisecond)

	waitLock := []<-chan struct{}{ch1, ch2}

	err := syncx.WaitForAll(ctx, waitLock)
	assert.NoErrorf(t, err, "expected no error, got %v", err)
}

func TestWaitForAll_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	ch1 := make(chan int)
	ch2 := make(chan int)

	delayedSend(ch1, 1, 200*time.Millisecond)
	delayedSend(ch2, 2, 300*time.Millisecond)

	waitLock := []<-chan int{ch1, ch2}

	err := syncx.WaitForAll(ctx, waitLock)
	assert.ErrorIsf(t, err, context.DeadlineExceeded, "expected context.DeadlineExceeded, got %v", err)
}

func TestWaitForAll_PartialCompleteContextCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	ch1 := make(chan int)
	ch2 := make(chan int)

	delayedSend(ch1, 1, 100*time.Millisecond)
	delayedSend(ch2, 2, 300*time.Millisecond)

	waitLock := []<-chan int{ch1, ch2}

	err := syncx.WaitForAll(ctx, waitLock)
	assert.ErrorIsf(t, err, context.DeadlineExceeded, "expected context.DeadlineExceeded, got %v", err)
}

func TestWaitForAll_NoChannels(t *testing.T) {
	ctx := context.Background()
	var waitLock []<-chan int

	err := syncx.WaitForAll(ctx, waitLock)
	assert.NoErrorf(t, err, "expected no error, got %v", err)
}

func TestWaitForAll_ImmediateContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch1 := make(chan int)
	ch2 := make(chan int)

	waitLock := []<-chan int{ch1, ch2}

	err := syncx.WaitForAll(ctx, waitLock)
	assert.ErrorIsf(t, err, context.Canceled, "expected context.Canceled, got %v", err)
}

func TestWaitForAll_ChannelAlreadyClosed(t *testing.T) {
	ctx := context.Background()
	ch1 := make(chan int)
	ch2 := make(chan int)

	close(ch1)
	close(ch2)

	waitLock := []<-chan int{ch1, ch2}

	err := syncx.WaitForAll(ctx, waitLock)
	assert.NoErrorf(t, err, "expected no error, got %v", err)
}
