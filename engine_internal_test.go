package redcache

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

type uncachedGetClient struct {
	rueidis.Client
	commands [][]string
}

func (*uncachedGetClient) B() rueidis.Builder { return rueidis.Builder{} }

func (c *uncachedGetClient) Do(_ context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	c.commands = append(c.commands, slices.Clone(cmd.Commands()))
	return rueidis.RedisResult{} // empty value
}

func TestCachedReadRetriesOnlyRueidisAbort(t *testing.T) {
	t.Parallel()

	client := &uncachedGetClient{}
	rca := &cacheAside{client: client}
	const key = "read:{3560}" // Slot zero matches the fake client's zero-value builder.
	val, err := rca.cachedReadValue(t.Context(), key, "", rueidis.ErrDoCacheAborted)
	require.NoError(t, err)
	require.Empty(t, val)
	require.Equal(t, [][]string{{"GET", key}}, client.commands)

	sameText := errors.New(rueidis.ErrDoCacheAborted.Error())
	_, err = rca.cachedReadValue(t.Context(), key, "", sameText)
	require.ErrorIs(t, err, sameText)
	require.Len(t, client.commands, 1)
}

type invalidationMetrics struct {
	NoopMetrics
	count atomic.Int64
}

func (m *invalidationMetrics) InvalidationError() {
	m.count.Add(1)
}

func TestOnInvalidate_CancelsMatchingLockAndReportsMalformedMessages(t *testing.T) {
	t.Parallel()

	metrics := &invalidationMetrics{}
	rca := &cacheAside{
		lockTTL:        time.Second,
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics:        metrics,
		metricsEnabled: true,
	}

	matching, leader := rca.register("matching")
	require.True(t, leader)
	other, leader := rca.register("other")
	require.True(t, leader)
	t.Cleanup(func() {
		for _, key := range []string{"matching", "other"} {
			if entry, ok := rca.locks.LoadAndDelete(key); ok {
				entry.cancel()
			}
		}
	})

	rca.onInvalidate([]rueidis.RedisMessage{
		redisStringMessage(t, "matching"),
		redisIntMessage(t, 42),
	})

	select {
	case <-matching:
	default:
		t.Fatal("matching lock was not cancelled")
	}
	select {
	case <-other:
		t.Fatal("nonmatching lock was cancelled")
	default:
	}
	require.Equal(t, int64(1), metrics.count.Load())
}

func redisStringMessage(t *testing.T, s string) rueidis.RedisMessage {
	t.Helper()
	return redisMessage(t, '+', uint64(len(s)), []byte(s))
}

func redisIntMessage(t *testing.T, n int64) rueidis.RedisMessage {
	t.Helper()
	return redisMessage(t, ':', uint64(n), nil)
}

func redisMessage(t *testing.T, typ byte, length uint64, payload []byte) rueidis.RedisMessage {
	t.Helper()

	buf := make([]byte, 16+len(payload))
	buf[7] = typ
	binary.BigEndian.PutUint64(buf[8:], length)
	copy(buf[16:], payload)

	var m rueidis.RedisMessage
	require.NoError(t, m.CacheUnmarshalView(buf))
	return m
}

// rueidis signals "everything is invalid" (disconnect, FLUSHALL) with nil.
func TestOnInvalidate_NilCancelsEveryLock(t *testing.T) {
	t.Parallel()
	rca := &cacheAside{lockTTL: time.Minute, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), metrics: NoopMetrics{}}
	a, _ := rca.register("a")
	b, _ := rca.register("b")

	rca.onInvalidate(nil)

	for name, ch := range map[string]<-chan struct{}{"a": a, "b": b} {
		select {
		case <-ch:
		default:
			t.Fatalf("lock %q was not cancelled", name)
		}
		_, ok := rca.locks.Load(name)
		require.False(t, ok, "lock %q still registered", name)
	}
}
