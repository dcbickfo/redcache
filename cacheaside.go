// Package redcache provides a cache-aside implementation for Redis with distributed locking.
//
// This library builds on the rueidis Redis client to provide:
//   - Cache-aside pattern with automatic cache population
//   - Distributed locking to prevent thundering herd
//   - Client-side caching to reduce Redis round trips
//   - Redis cluster support with slot-aware batching
//   - Automatic cleanup of expired lock entries
//
// # Basic Usage
//
//	client, err := redcache.NewRedCacheAside(
//	    rueidis.ClientOption{InitAddress: []string{"localhost:6379"}},
//	    redcache.CacheAsideOption{LockTTL: 10 * time.Second},
//	)
//	if err != nil {
//	    return err
//	}
//	defer client.Client().Close()
//
//	// Get a single value with automatic cache population
//	value, err := client.Get(ctx, time.Minute, "user:123", func(ctx context.Context, key string) (string, error) {
//	    return fetchFromDatabase(ctx, key)
//	})
//
//	// Get multiple values with batched cache population
//	values, err := client.GetMulti(ctx, time.Minute, []string{"user:1", "user:2"}, func(ctx context.Context, keys []string) (map[string]string, error) {
//	    return fetchMultipleFromDatabase(ctx, keys)
//	})
//
// # Distributed Locking
//
// The library ensures that only one goroutine (across all instances of your application)
// executes the callback function for a given key at a time. Other goroutines will wait
// for the lock to be released and then return the cached value.
//
// Locks are implemented using Redis SET NX with a configurable TTL. Lock values use
// UUIDv7 for uniqueness and are prefixed (default: "__redcache:lock:") to avoid
// collisions with application data.
//
// # Context and Timeouts
//
// All operations respect context cancellation. The LockTTL option controls:
//   - Maximum time a lock can be held before automatic expiration
//   - Timeout for waiting on locks when handling invalidation messages
//   - Context timeout for cleanup operations
//
// Use context deadlines to control overall operation timeout:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	value, err := client.Get(ctx, time.Minute, key, callback)
//
// # Client-Side Caching
//
// The library uses rueidis client-side caching with Redis invalidation messages.
// When a key is modified in Redis, invalidation messages automatically clear the
// local cache, ensuring consistency across distributed instances.
package redcache

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"golang.org/x/sync/errgroup"

	"github.com/dcbickfo/redcache/internal/cmdx"
	"github.com/dcbickfo/redcache/internal/mapsx"
	"github.com/dcbickfo/redcache/internal/syncx"
)

type lockEntry struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// Logger defines the logging interface used by CacheAside.
// Implementations must be safe for concurrent use and should handle log levels internally.
type Logger interface {
	// Error logs error messages. Should be used for unexpected failures or critical issues.
	Error(msg string, args ...any)
	// Debug logs detailed diagnostic information useful for development and troubleshooting.
	// Call Debug to record verbose output about internal state, cache operations, or lock handling.
	// Debug messages should not include sensitive information and may be omitted in production.
	Debug(msg string, args ...any)
}

type CacheAside struct {
	client     rueidis.Client
	locks      syncx.Map[string, *lockEntry]
	lockTTL    time.Duration
	logger     Logger
	lockPrefix string
}

type CacheAsideOption struct {
	// LockTTL is the maximum time a lock can be held, and also the timeout for waiting
	// on locks when handling lost Redis invalidation messages. Defaults to 10 seconds.
	LockTTL       time.Duration
	ClientBuilder func(option rueidis.ClientOption) (rueidis.Client, error)
	// Logger for logging errors and debug information. Defaults to slog.Default().
	// The logger should handle log levels internally (e.g., only log Debug if level is enabled).
	Logger Logger
	// LockPrefix for distributed locks. Defaults to "__redcache:lock:".
	// Choose a prefix unlikely to conflict with your data keys.
	LockPrefix string
}

func NewRedCacheAside(clientOption rueidis.ClientOption, caOption CacheAsideOption) (*CacheAside, error) {
	// Validate client options
	if len(clientOption.InitAddress) == 0 {
		return nil, errors.New("at least one Redis address must be provided in InitAddress")
	}

	// Validate and set defaults for cache aside options
	if caOption.LockTTL < 0 {
		return nil, errors.New("LockTTL must not be negative")
	}
	if caOption.LockTTL > 0 && caOption.LockTTL < 100*time.Millisecond {
		return nil, errors.New("LockTTL should be at least 100ms to avoid excessive lock churn")
	}
	if caOption.LockTTL == 0 {
		caOption.LockTTL = 10 * time.Second
	}
	if caOption.Logger == nil {
		caOption.Logger = slog.Default()
	}
	if caOption.LockPrefix == "" {
		caOption.LockPrefix = "__redcache:lock:"
	}

	rca := &CacheAside{
		lockTTL:    caOption.LockTTL,
		logger:     caOption.Logger,
		lockPrefix: caOption.LockPrefix,
	}
	clientOption.OnInvalidations = rca.onInvalidate

	var err error
	if caOption.ClientBuilder != nil {
		rca.client, err = caOption.ClientBuilder(clientOption)
	} else {
		rca.client, err = rueidis.NewClient(clientOption)
	}
	if err != nil {
		return nil, err
	}
	return rca, nil
}

// Client returns the underlying rueidis.Client for advanced operations.
// Most users should not need direct client access. Use with caution as
// direct operations bypass the cache-aside pattern and distributed locking.
func (rca *CacheAside) Client() rueidis.Client {
	return rca.client
}

func (rca *CacheAside) onInvalidate(messages []rueidis.RedisMessage) {
	for _, m := range messages {
		key, err := m.ToString()
		if err != nil {
			rca.logger.Error("failed to parse invalidation message", "error", err)
			continue
		}
		entry, loaded := rca.locks.LoadAndDelete(key)
		if loaded {
			entry.cancel() // Cancel context, which closes the channel
		}
	}
}

var (
	delKeyLua = rueidis.NewLuaScript(`if redis.call("GET",KEYS[1]) == ARGV[1] then return redis.call("DEL",KEYS[1]) else return 0 end`)
	setKeyLua = rueidis.NewLuaScript(`if redis.call("GET",KEYS[1]) == ARGV[1] then return redis.call("SET",KEYS[1],ARGV[2],"PX",ARGV[3]) else return 0 end`)
)

func (rca *CacheAside) register(key string) <-chan struct{} {
retry:
	// Create new entry with context that auto-cancels after lockTTL
	ctx, cancel := context.WithTimeout(context.Background(), rca.lockTTL)

	newEntry := &lockEntry{
		ctx:    ctx,
		cancel: cancel,
	}

	// Store or get existing entry atomically
	actual, loaded := rca.locks.LoadOrStore(key, newEntry)

	// If we successfully stored, schedule automatic cleanup on expiration
	if !loaded {
		// Use context.AfterFunc to clean up expired entry without blocking goroutine
		context.AfterFunc(ctx, func() {
			rca.locks.CompareAndDelete(key, newEntry)
		})
		return ctx.Done()
	}

	// Another goroutine stored first, cancel our context to prevent leak
	cancel()

	// Check if their context is still active (not cancelled/timed out)
	select {
	case <-actual.ctx.Done():
		// Context is done - try to atomically delete it and retry
		if rca.locks.CompareAndDelete(key, actual) {
			// We successfully deleted the expired entry, retry
			goto retry
		}
		// CompareAndDelete failed - another goroutine modified it
		// Load the new entry and use it
		newEntry, loaded := rca.locks.Load(key)
		if !loaded {
			// Entry was deleted by another goroutine, retry registration
			goto retry
		}
		// Use the new entry's context
		return newEntry.ctx.Done()
	default:
		// Context is still active - use it
		return actual.ctx.Done()
	}
}

func (rca *CacheAside) Get(
	ctx context.Context,
	ttl time.Duration,
	key string,
	fn func(ctx context.Context, key string) (val string, err error),
) (string, error) {
retry:
	wait := rca.register(key)
	val, err := rca.tryGet(ctx, ttl, key)

	if err != nil && !errors.Is(err, errNotFound) {
		return "", err
	}

	if err == nil && val != "" {
		return val, nil
	}

	if val == "" {
		val, err = rca.trySetKeyFunc(ctx, ttl, key, fn)
	}

	if err != nil && !errors.Is(err, errLockFailed) {
		return "", err
	}

	if val == "" {
		// Wait for lock release (channel auto-closes after lockTTL or on invalidation)
		select {
		case <-wait:
			goto retry
		case <-ctx.Done():
			// Parent context cancelled
			return "", ctx.Err()
		}
	}

	return val, err
}

func (rca *CacheAside) Del(ctx context.Context, key string) error {
	return rca.client.Do(ctx, rca.client.B().Del().Key(key).Build()).Error()
}

func (rca *CacheAside) DelMulti(ctx context.Context, keys ...string) error {
	cmds := make(rueidis.Commands, 0, len(keys))
	for _, key := range keys {
		cmds = append(cmds, rca.client.B().Del().Key(key).Build())
	}
	resps := rca.client.DoMulti(ctx, cmds...)
	for _, resp := range resps {
		if err := resp.Error(); err != nil {
			return err
		}
	}
	return nil
}

var (
	errNotFound   = errors.New("not found")
	errLockFailed = errors.New("lock failed")
)

// ErrLockLost indicates the distributed lock was lost or expired before the value could be set.
// This can occur if the lock TTL expires during callback execution or if Redis invalidates the lock.
var ErrLockLost = errors.New("lock was lost or expired before value could be set")

func (rca *CacheAside) tryGet(ctx context.Context, ttl time.Duration, key string) (string, error) {
	resp := rca.client.DoCache(ctx, rca.client.B().Get().Key(key).Cache(), ttl)
	val, err := resp.ToString()
	if rueidis.IsRedisNil(err) || strings.HasPrefix(val, rca.lockPrefix) { // no response or is a lock value
		if rueidis.IsRedisNil(err) {
			rca.logger.Debug("cache miss - key not found", "key", key)
		} else {
			rca.logger.Debug("cache miss - lock value found", "key", key)
		}
		return "", errNotFound
	}
	if err != nil {
		return "", err
	}
	rca.logger.Debug("cache hit", "key", key)
	return val, nil
}

func (rca *CacheAside) trySetKeyFunc(ctx context.Context, ttl time.Duration, key string, fn func(ctx context.Context, key string) (string, error)) (val string, err error) {
	setVal := false
	lockVal, err := rca.tryLock(ctx, key)
	if err != nil {
		return "", err
	}
	defer func() {
		if !setVal {
			// Use context.WithoutCancel to preserve tracing/request context while allowing cleanup
			cleanupCtx := context.WithoutCancel(ctx)
			toCtx, cancel := context.WithTimeout(cleanupCtx, rca.lockTTL)
			defer cancel()
			// Best effort unlock - errors are non-fatal as lock will expire
			if err := rca.unlock(toCtx, key, lockVal); err != nil {
				rca.logger.Error("failed to unlock key", "key", key, "error", err)
			}
		}
	}()
	if val, err = fn(ctx, key); err == nil {
		val, err = rca.setWithLock(ctx, ttl, key, valAndLock{val, lockVal})
		if err == nil {
			setVal = true
		}
		return val, err
	}
	return "", err
}

func (rca *CacheAside) tryLock(ctx context.Context, key string) (string, error) {
	uuidv7, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("failed to generate lock UUID for key %q: %w", key, err)
	}
	lockVal := rca.lockPrefix + uuidv7.String()
	err = rca.client.Do(ctx, rca.client.B().Set().Key(key).Value(lockVal).Nx().Get().Px(rca.lockTTL).Build()).Error()
	if !rueidis.IsRedisNil(err) {
		rca.logger.Debug("lock contention - failed to acquire lock", "key", key)
		return "", fmt.Errorf("failed to acquire lock for key %q: %w", key, errLockFailed)
	}
	rca.logger.Debug("lock acquired", "key", key, "lockVal", lockVal)
	return lockVal, nil
}

func (rca *CacheAside) setWithLock(ctx context.Context, ttl time.Duration, key string, valLock valAndLock) (string, error) {
	err := setKeyLua.Exec(ctx, rca.client, []string{key}, []string{valLock.lockVal, valLock.val, strconv.FormatInt(ttl.Milliseconds(), 10)}).Error()
	if err != nil {
		if !rueidis.IsRedisNil(err) {
			return "", fmt.Errorf("failed to set value for key %q: %w", key, err)
		}
		rca.logger.Debug("lock lost during set operation", "key", key)
		return "", fmt.Errorf("lock lost for key %q: %w", key, ErrLockLost)
	}
	rca.logger.Debug("value set successfully", "key", key)
	return valLock.val, nil
}

func (rca *CacheAside) unlock(ctx context.Context, key string, lock string) error {
	return delKeyLua.Exec(ctx, rca.client, []string{key}, []string{lock}).Error()
}

func (rca *CacheAside) GetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, key []string) (val map[string]string, err error),
) (map[string]string, error) {
	res := make(map[string]string, len(keys))

	waitLock := make(map[string]<-chan struct{}, len(keys))
	for _, key := range keys {
		waitLock[key] = nil
	}

retry:
	waitLock = rca.registerAll(maps.Keys(waitLock), len(waitLock))

	vals, err := rca.tryGetMulti(ctx, ttl, mapsx.Keys(waitLock))
	if err != nil && !rueidis.IsRedisNil(err) {
		return nil, err
	}

	for k, v := range vals {
		res[k] = v
		delete(waitLock, k)
	}

	if len(waitLock) > 0 {
		vals, err := rca.trySetMultiKeyFn(ctx, ttl, mapsx.Keys(waitLock), fn)
		if err != nil {
			return nil, err
		}
		for k, v := range vals {
			res[k] = v
			delete(waitLock, k)
		}
	}

	if len(waitLock) > 0 {
		// Wait for lock releases (channels auto-close after lockTTL or on invalidation)
		err = syncx.WaitForAll(ctx, maps.Values(waitLock), len(waitLock))
		if err != nil {
			// Parent context cancelled or deadline exceeded
			return nil, ctx.Err()
		}
		goto retry
	}
	return res, err
}

func (rca *CacheAside) registerAll(keys iter.Seq[string], length int) map[string]<-chan struct{} {
	res := make(map[string]<-chan struct{}, length)
	for key := range keys {
		res[key] = rca.register(key)
	}
	return res
}

func (rca *CacheAside) tryGetMulti(ctx context.Context, ttl time.Duration, keys []string) (map[string]string, error) {
	multi := make([]rueidis.CacheableTTL, len(keys))
	for i, key := range keys {
		cmd := rca.client.B().Get().Key(key).Cache()
		multi[i] = rueidis.CacheableTTL{
			Cmd: cmd,
			TTL: ttl,
		}
	}
	resps := rca.client.DoMultiCache(ctx, multi...)

	res := make(map[string]string)
	for i, resp := range resps {
		val, err := resp.ToString()
		if err != nil && rueidis.IsRedisNil(err) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("failed to get key %q: %w", keys[i], err)
		}
		if !strings.HasPrefix(val, rca.lockPrefix) {
			res[keys[i]] = val
			continue
		}
	}
	return res, nil
}

func (rca *CacheAside) trySetMultiKeyFn(
	ctx context.Context,
	ttl time.Duration,
	keys []string,
	fn func(ctx context.Context, key []string) (val map[string]string, err error),
) (map[string]string, error) {
	res := make(map[string]string)

	lockVals, err := rca.tryLockMulti(ctx, keys)
	if err != nil {
		return nil, err
	}

	defer func() {
		toUnlock := make(map[string]string)
		for key, lockVal := range lockVals {
			if _, ok := res[key]; !ok {
				toUnlock[key] = lockVal
			}
		}
		if len(toUnlock) > 0 {
			// Use context.WithoutCancel to preserve tracing/request context while allowing cleanup
			cleanupCtx := context.WithoutCancel(ctx)
			toCtx, cancel := context.WithTimeout(cleanupCtx, rca.lockTTL)
			defer cancel()
			rca.unlockMulti(toCtx, toUnlock)
		}
	}()

	// Case where we were unable to get any locks
	if len(lockVals) == 0 {
		return res, nil
	}

	vals, err := fn(ctx, mapsx.Keys(lockVals))
	if err != nil {
		return nil, err
	}

	vL := make(map[string]valAndLock, len(vals))

	for k, v := range vals {
		vL[k] = valAndLock{v, lockVals[k]}
	}

	keysSet, err := rca.setMultiWithLock(ctx, ttl, vL)
	if err != nil {
		return nil, err
	}

	for _, keySet := range keysSet {
		res[keySet] = vals[keySet]
	}

	return res, err
}

func (rca *CacheAside) tryLockMulti(ctx context.Context, keys []string) (map[string]string, error) {
	lockVals := make(map[string]string, len(keys))
	cmds := make(rueidis.Commands, 0, len(keys))
	for _, k := range keys {
		uuidv7, err := uuid.NewV7()
		if err != nil {
			return nil, err
		}
		lockVals[k] = rca.lockPrefix + uuidv7.String()
		cmds = append(cmds, rca.client.B().Set().Key(k).Value(lockVals[k]).Nx().Get().Px(rca.lockTTL).Build())
	}
	resps := rca.client.DoMulti(ctx, cmds...)
	for i, r := range resps {
		err := r.Error()
		if !rueidis.IsRedisNil(err) {
			if err != nil {
				rca.logger.Error("failed to acquire lock", "key", keys[i], "error", err)
			}
			delete(lockVals, keys[i])
		}
	}
	return lockVals, nil
}

type valAndLock struct {
	val     string
	lockVal string
}

type keyOrderAndSet struct {
	keyOrder []string
	setStmts []rueidis.LuaExec
}

// groupBySlot groups keys by their Redis cluster slot for efficient batching.
func groupBySlot(keyValLock map[string]valAndLock, ttl time.Duration) map[uint16]keyOrderAndSet {
	stmts := make(map[uint16]keyOrderAndSet)

	for k, vl := range keyValLock {
		slot := cmdx.Slot(k)
		kos := stmts[slot]
		kos.keyOrder = append(kos.keyOrder, k)
		kos.setStmts = append(kos.setStmts, rueidis.LuaExec{
			Keys: []string{k},
			Args: []string{vl.lockVal, vl.val, strconv.FormatInt(ttl.Milliseconds(), 10)},
		})
		stmts[slot] = kos
	}

	return stmts
}

// executeSetStatements executes Lua set statements in parallel, grouped by slot.
func (rca *CacheAside) executeSetStatements(ctx context.Context, stmts map[uint16]keyOrderAndSet) ([]string, error) {
	keyByStmt := make([][]string, len(stmts))
	i := 0
	eg, ctx := errgroup.WithContext(ctx)

	for _, kos := range stmts {
		ii := i
		eg.Go(func() error {
			setResps := setKeyLua.ExecMulti(ctx, rca.client, kos.setStmts...)
			for j, resp := range setResps {
				err := resp.Error()
				if err != nil {
					if !rueidis.IsRedisNil(err) {
						return err
					}
					continue
				}
				keyByStmt[ii] = append(keyByStmt[ii], kos.keyOrder[j])
			}
			return nil
		})
		i++
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	out := make([]string, 0)
	for _, keys := range keyByStmt {
		out = append(out, keys...)
	}
	return out, nil
}

func (rca *CacheAside) setMultiWithLock(ctx context.Context, ttl time.Duration, keyValLock map[string]valAndLock) ([]string, error) {
	stmts := groupBySlot(keyValLock, ttl)
	return rca.executeSetStatements(ctx, stmts)
}

func (rca *CacheAside) unlockMulti(ctx context.Context, lockVals map[string]string) {
	if len(lockVals) == 0 {
		return
	}
	delStmts := make(map[uint16][]rueidis.LuaExec)
	for key, lockVal := range lockVals {
		slot := cmdx.Slot(key)
		delStmts[slot] = append(delStmts[slot], rueidis.LuaExec{
			Keys: []string{key},
			Args: []string{lockVal},
		})
	}
	wg := sync.WaitGroup{}
	for _, stmts := range delStmts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Best effort unlock - errors are non-fatal as locks will expire
			resps := delKeyLua.ExecMulti(ctx, rca.client, stmts...)
			for _, resp := range resps {
				if err := resp.Error(); err != nil {
					rca.logger.Error("failed to unlock key in batch", "error", err)
				}
			}
		}()
	}
	wg.Wait()
}
