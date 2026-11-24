package redcache

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type readTestClient struct {
	rueidis.Client
	mode          rueidis.ClientMode
	commands      [][]string
	multiCommands [][][]string
	cached        []rueidis.RedisResult
	plain         []rueidis.RedisResult
	single        rueidis.RedisResult
}

func (*readTestClient) B() rueidis.Builder { return rueidis.Builder{} }

func (c *readTestClient) Mode() rueidis.ClientMode { return c.mode }

func (c *readTestClient) Do(_ context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	c.commands = append(c.commands, slices.Clone(cmd.Commands()))
	return c.single
}

func (c *readTestClient) DoCache(context.Context, rueidis.Cacheable, time.Duration) rueidis.RedisResult {
	return c.cached[0]
}

func (c *readTestClient) DoMultiCache(context.Context, ...rueidis.CacheableTTL) []rueidis.RedisResult {
	return slices.Clone(c.cached)
}

func (c *readTestClient) DoMulti(_ context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	batch := make([][]string, len(cmds))
	for i, cmd := range cmds {
		batch[i] = slices.Clone(cmd.Commands())
	}
	c.multiCommands = append(c.multiCommands, batch)
	return c.plain
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

	const key = "read:{3560}" // Slot zero matches the fake client's zero-value builder.
	const want = "retried value"
	client := &readTestClient{
		mode:   rueidis.ClientModeCluster,
		single: redisResult(t, redisStringMessage(t, want), nil),
	}
	rca := &cacheAside{client: client}
	val, err := rca.cachedReadValue(t.Context(), key, "", rueidis.ErrDoCacheAborted)
	require.NoError(t, err)
	require.Equal(t, want, val)
	require.Equal(t, [][]string{{"GET", key}}, client.commands)

	sameText := errors.New(rueidis.ErrDoCacheAborted.Error())
	_, err = rca.cachedReadValue(t.Context(), key, "", sameText)
	require.ErrorIs(t, err, sameText)
	require.Len(t, client.commands, 1)
}

func TestCachedReadFallbackRequiresCluster(t *testing.T) {
	t.Parallel()

	const key = "read:{3560}"
	const want = "fallback value"
	abortErr := fmt.Errorf("cache transaction: %w", rueidis.ErrDoCacheAborted)
	abort := redisResult(t, rueidis.RedisMessage{}, abortErr)
	plain := redisResult(t, redisStringMessage(t, want), nil)
	for _, mode := range []rueidis.ClientMode{rueidis.ClientModeStandalone, rueidis.ClientModeSentinel, rueidis.ClientModeCluster} {
		t.Run(string(mode), func(t *testing.T) {
			client := &readTestClient{
				mode:   mode,
				cached: []rueidis.RedisResult{abort},
				plain:  []rueidis.RedisResult{plain},
				single: plain,
			}
			rca := &cacheAside{client: client, lockPrefix: "lock:"}
			got, singleErr := rca.cachedReadValue(t.Context(), key, "", abortErr)
			results := make(map[string]string)
			_, _, multiErr := rca.tryGetMulti(t.Context(), time.Minute, []string{key}, results, nil, nil)
			if mode == rueidis.ClientModeCluster {
				require.NoError(t, singleErr)
				require.NoError(t, multiErr)
				require.Equal(t, want, got)
				require.Equal(t, map[string]string{key: want}, results)
				require.Equal(t, [][]string{{"GET", key}}, client.commands)
				require.Equal(t, [][][]string{{{"GET", key}}}, client.multiCommands)
				return
			}
			assert.Empty(t, got)
			assert.Empty(t, results)
			assert.Empty(t, client.commands)
			assert.Empty(t, client.multiCommands)
			require.Error(t, singleErr)
			require.Error(t, multiErr)
			require.Same(t, abortErr, singleErr)
			require.Same(t, abortErr, errors.Unwrap(multiErr))
		})
	}
}

func TestCachedReadBatchFallback(t *testing.T) {
	t.Parallel()

	keys := []string{"read:{3560}:b", "read:{3560}:a", "read:{3560}:d", "read:{3560}:c", "read:{3560}:e"}
	abort := redisResult(t, rueidis.RedisMessage{}, rueidis.ErrDoCacheAborted)
	terminalErr := errors.New("cached read failed")
	terminal := redisResult(t, rueidis.RedisMessage{}, terminalErr)
	value := func(s string) rueidis.RedisResult { return redisResult(t, redisStringMessage(t, s), nil) }
	missing := redisResult(t, redisMessage(t, '_', 0, nil), nil)
	require.True(t, rueidis.IsRedisNil(missing.Error()))
	plain := []rueidis.RedisResult{value("first"), value("second"), missing, value("lock:held")}
	unlocked := slices.Clone(plain)
	unlocked[3] = missing
	cached := []rueidis.RedisResult{abort, value("cached"), abort, abort, abort}
	poll := func(t *testing.T, rca *cacheAside) (any, error) {
		return rca.anyWriteLocked(t.Context(), keys, nil)
	}
	readers := []struct {
		name  string
		plain []rueidis.RedisResult
		want  any
		read  func(*testing.T, *cacheAside) (any, error)
	}{
		{"GetMulti", plain, map[string]string{keys[0]: "first", keys[1]: "cached", keys[2]: "second"},
			func(t *testing.T, rca *cacheAside) (any, error) {
				got := make(map[string]string)
				_, _, err := rca.tryGetMulti(t.Context(), time.Minute, keys, got, nil, nil)
				return got, err
			}},
		{"SetMulti initial lock scan", unlocked, nil,
			func(t *testing.T, rca *cacheAside) (any, error) {
				var owned entryOwner
				defer func() { rca.releaseFinished(owned, true) }()
				return nil, rca.waitForReadLocks(t.Context(), keys, &owned)
			}},
		{"SetMulti lock poll with lock", plain, true, poll},
		{"SetMulti lock poll without lock", unlocked, false, poll},
	}
	for _, reader := range readers {
		t.Run(reader.name, func(t *testing.T) {
			client := &readTestClient{mode: rueidis.ClientModeCluster, cached: slices.Clone(cached), plain: reader.plain}
			rca := &cacheAside{client: client, lockPrefix: "lock:", lockTTL: time.Minute, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			got, err := reader.read(t, rca)
			require.NoError(t, err)
			assert.Equal(t, reader.want, got)
			assert.Len(t, client.multiCommands, 1)
			assert.Equal(t, [][][]string{{{"GET", keys[0]}, {"GET", keys[2]}, {"GET", keys[3]}, {"GET", keys[4]}}}, client.multiCommands)
			assert.Empty(t, client.commands)

			client.cached[1] = terminal
			client.multiCommands = nil
			client.commands = nil
			_, err = reader.read(t, rca)
			require.ErrorIs(t, err, terminalErr)
			require.Empty(t, client.multiCommands)
			require.Empty(t, client.commands)
		})
	}
}

func TestCachedReadPreservesFallbackError(t *testing.T) {
	t.Parallel()

	const key = "read:{3560}"
	const serverError = "OOM command not allowed"
	abort := redisResult(t, rueidis.RedisMessage{}, rueidis.ErrDoCacheAborted)
	errReply := redisResult(t, redisMessage(t, '-', uint64(len(serverError)), []byte(serverError)), nil)
	client := &readTestClient{
		mode:   rueidis.ClientModeCluster,
		cached: []rueidis.RedisResult{abort},
		plain:  []rueidis.RedisResult{errReply},
		single: errReply,
	}
	rca := &cacheAside{client: client, lockPrefix: "lock:"}

	_, singleErr := rca.tryGet(t.Context(), time.Minute, key)
	_, _, multiErr := rca.tryGetMulti(t.Context(), time.Minute, []string{key}, make(map[string]string), nil, nil)
	for name, err := range map[string]error{"single": singleErr, "batch": multiErr} {
		t.Run(name, func(t *testing.T) {
			var redisErr *rueidis.RedisError
			require.ErrorAs(t, err, &redisErr)
			require.EqualError(t, redisErr, serverError)
			require.ErrorIs(t, err, rueidis.ErrDoCacheAborted)
		})
	}
	require.Len(t, client.commands, 1)
	require.Len(t, client.multiCommands, 1)
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
