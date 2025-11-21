package invalidation

import (
	"context"
	"iter"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/logger"
	"github.com/dcbickfo/redcache/internal/syncx"
)

// Handler manages cache invalidation tracking and coordination.
// It tracks pending requests waiting for cache updates and notifies them
// when invalidation messages arrive from Redis.
//
// This is used to coordinate multiple concurrent requests for the same key:
// - When a cache miss occurs, the first request acquires a lock
// - Subsequent requests register themselves as waiters
// - When the value is computed and stored, Redis sends invalidation messages
// - All waiters are notified to retry their cache lookups.
type Handler interface {
	// OnInvalidate processes Redis invalidation messages.
	// It notifies all registered waiters for the invalidated keys.
	OnInvalidate(messages []rueidis.RedisMessage)

	// Register registers interest in a key and returns a channel that will be closed
	// when the key is invalidated or times out.
	// Multiple goroutines can register for the same key safely.
	Register(key string) <-chan struct{}

	// RegisterAll registers interest in multiple keys at once.
	// Returns a map of key -> wait channel.
	RegisterAll(keys iter.Seq[string], length int) map[string]<-chan struct{}
}

// Logger is the logging interface used for invalidation operations.
// This is a type alias for the shared logger interface.
type Logger = logger.Logger

// lockEntry tracks a registered waiter for a key.
type lockEntry struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// Config holds configuration for the invalidation handler.
type Config struct {
	// LockTTL is the timeout for waiting on invalidations.
	// This should match the lock TTL used for distributed locking.
	LockTTL time.Duration

	// Logger for error reporting.
	Logger Logger
}

// RedisInvalidationHandler implements Handler using a sync.Map to track waiters.
type RedisInvalidationHandler struct {
	lockTTL time.Duration
	logger  Logger
	locks   *lockEntryMap // map[string]*lockEntry
}

// lockEntryMap is a type-safe wrapper around syncx.Map for lock entries.
type lockEntryMap struct {
	m *syncx.Map[string, *lockEntry]
}

// NewRedisInvalidationHandler creates a new invalidation handler.
func NewRedisInvalidationHandler(cfg Config) *RedisInvalidationHandler {
	return &RedisInvalidationHandler{
		lockTTL: cfg.LockTTL,
		logger:  cfg.Logger,
		locks: &lockEntryMap{
			m: syncx.NewMap[string, *lockEntry](),
		},
	}
}

// OnInvalidate processes Redis invalidation messages.
func (h *RedisInvalidationHandler) OnInvalidate(messages []rueidis.RedisMessage) {
	for _, m := range messages {
		key, err := m.ToString()
		if err != nil {
			h.logger.Error("failed to parse invalidation message", "error", err)
			continue
		}
		entry, loaded := h.locks.m.LoadAndDelete(key)
		if loaded {
			entry.cancel() // Cancel context, which closes the channel
		}
	}
}

// Register registers interest in a key and returns a channel that will be closed
// when the key is invalidated or times out.
//
//nolint:gocognit // Complex due to atomic operations and retry logic
func (h *RedisInvalidationHandler) Register(key string) <-chan struct{} {
retry:
	// First check if an entry already exists (common case for concurrent requests)
	// This avoids creating a context unnecessarily
	if existing, ok := h.locks.m.Load(key); ok {
		// Check if the existing context is still active
		select {
		case <-existing.ctx.Done():
			// Context is done - try to atomically delete it and retry
			if h.locks.m.CompareAndDelete(key, existing) {
				goto retry
			}
			// Another goroutine modified it, try loading again
			if newEntry, found := h.locks.m.Load(key); found {
				return newEntry.ctx.Done()
			}
			// Entry was deleted, retry
			goto retry
		default:
			// Context is still active, use it
			return existing.ctx.Done()
		}
	}

	// No existing entry or it was expired, create new one
	// The extra time allows the invalidation message to arrive (primary flow)
	// while still providing a fallback timeout for missed messages.
	// We use a proportional buffer (20% of lockTTL) with a minimum of 200ms
	// to account for network delays, ensuring the timeout scales appropriately
	// with different lock durations.
	buffer := h.lockTTL / 5 // 20%
	if buffer < 200*time.Millisecond {
		buffer = 200 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(context.Background(), h.lockTTL+buffer)

	newEntry := &lockEntry{
		ctx:    ctx,
		cancel: cancel,
	}

	// Store or get existing entry atomically
	actual, loaded := h.locks.m.LoadOrStore(key, newEntry)

	// If we successfully stored, schedule automatic cleanup on expiration
	if !loaded {
		// Use context.AfterFunc to clean up expired entry without blocking goroutine
		context.AfterFunc(ctx, func() {
			h.locks.m.CompareAndDelete(key, newEntry)
		})
		return ctx.Done()
	}

	// Another goroutine stored first, cancel our context to prevent leak
	cancel()

	// Check if their context is still active (not cancelled/timed out)
	select {
	case <-actual.ctx.Done():
		// Context is done - try to atomically delete it and retry
		if h.locks.m.CompareAndDelete(key, actual) {
			// We successfully deleted the expired entry, retry
			goto retry
		}
		// CompareAndDelete failed - another goroutine modified it
		// Load the new entry and use it
		waitEntry, ok := h.locks.m.Load(key)
		if !ok {
			// Entry was deleted by another goroutine, retry registration
			goto retry
		}
		return waitEntry.ctx.Done()
	default:
		// Context is still active, use it
		return actual.ctx.Done()
	}
}

// RegisterAll registers interest in multiple keys at once.
func (h *RedisInvalidationHandler) RegisterAll(keys iter.Seq[string], length int) map[string]<-chan struct{} {
	res := make(map[string]<-chan struct{}, length)
	for key := range keys {
		res[key] = h.Register(key)
	}
	return res
}

// WaitForSingleLock waits for a single lock to be released via invalidation or timeout.
// This is a helper function for waiting on invalidation channels with proper timeout handling.
func WaitForSingleLock(ctx context.Context, waitChan <-chan struct{}, lockTTL time.Duration) error {
	timer := time.NewTimer(lockTTL)
	defer timer.Stop()

	select {
	case <-waitChan:
		// Lock released via invalidation
		return nil
	case <-timer.C:
		// Lock TTL expired
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
