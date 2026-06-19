package redcache

import (
	"encoding/binary"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

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
