package redcache

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

type readTestClient struct {
	rueidis.Client
	commands [][]string
	cached   []rueidis.RedisResult
}

func (*readTestClient) B() rueidis.Builder { return rueidis.Builder{} }

func (c *readTestClient) Do(_ context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	c.commands = append(c.commands, slices.Clone(cmd.Commands()))
	return rueidis.RedisResult{}
}

func (c *readTestClient) DoCache(context.Context, rueidis.Cacheable, time.Duration) rueidis.RedisResult {
	return c.cached[0]
}

func (c *readTestClient) DoMultiCache(context.Context, ...rueidis.CacheableTTL) []rueidis.RedisResult {
	return slices.Clone(c.cached)
}

func TestTrackedHitKeepsWaitEntry(t *testing.T) {
	t.Parallel()

	const key = "read:{3560}"
	const want = "tracked value"
	expiry := time.Now().Add(time.Minute).UnixMilli()
	message := redisStringMessage(t, want)
	encoded := message.CacheMarshal(nil)
	expiryBytes := binary.LittleEndian.AppendUint64(nil, uint64(expiry))
	copy(encoded[:7], expiryBytes[:7])
	require.NoError(t, message.CacheUnmarshalView(encoded))
	reply := redisResult(t, message, nil)
	require.Equal(t, expiry, reply.CachePXAT())

	for _, operation := range []string{"Get", "GetMulti"} {
		t.Run(operation, func(t *testing.T) {
			client := &readTestClient{cached: []rueidis.RedisResult{reply}}
			rca := &cacheAside{client: client, lockTTL: time.Minute, lockPrefix: "lock:"}
			if operation == "Get" {
				got, err := rca.get(t.Context(), time.Minute, key, nil)
				require.NoError(t, err)
				require.Equal(t, want, got)
			} else {
				got, err := rca.getMulti(t.Context(), time.Minute, []string{key}, nil)
				require.NoError(t, err)
				require.Equal(t, map[string]string{key: want}, got)
			}
			entry, ok := rca.locks.Load(key)
			require.True(t, ok)
			defer rca.release(key, entry.done)
			select {
			case <-entry.done:
				t.Fatal("tracked hit closed its wait channel")
			default:
			}
		})
	}
}

func redisResult(t *testing.T, val rueidis.RedisMessage, err error) rueidis.RedisResult {
	t.Helper()
	var result rueidis.RedisResult
	// Use rueidis's result fields to supply both transport errors and server replies.
	fields := reflect.ValueOf(&result).Elem()
	*(*error)(fields.FieldByName("err").Addr().UnsafePointer()) = err
	*(*rueidis.RedisMessage)(fields.FieldByName("val").Addr().UnsafePointer()) = val
	return result
}

func TestCachedReadRetriesOnlyRueidisAbort(t *testing.T) {
	t.Parallel()

	client := &readTestClient{}
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
