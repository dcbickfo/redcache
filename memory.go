package redcache

import (
	"context"
	"sync"
	"time"

	"github.com/redis/rueidis"
)

// MemoryOption configures OpenMemory.
type MemoryOption func(*memEngine)

// WithMemoryClock sets the time source used for TTL expiry, so tests can
// advance time without sleeping. The default is time.Now.
func WithMemoryClock(now func() time.Time) MemoryOption {
	return func(m *memEngine) { m.now = now }
}

// OpenMemory returns a Conn backed by an in-process map instead of Redis, for
// unit tests and local development. Caches built over it (New, NewKeyed,
// NewBytes) behave identically at the typed layer — codecs run, TTLs expire,
// a present entry is a hit and a miss calls the loader once — but there is no
// distributed locking, invalidation, refresh-ahead, or metrics, and Client()
// returns nil. It never fails and needs no Close.
func OpenMemory(opts ...MemoryOption) *Conn {
	m := &memEngine{data: make(map[string]memEntry), now: time.Now}
	for _, opt := range opts {
		opt(m)
	}
	return &Conn{core: m}
}

type memEntry struct {
	val      string
	deadline time.Time
}

type memEngine struct {
	mu   sync.Mutex
	data map[string]memEntry
	now  func() time.Time
}

func (m *memEngine) Client() rueidis.Client { return nil }

func (m *memEngine) Close() {}

// load returns the live value for key; an expired entry is evicted and a miss.
// Callers hold m.mu.
func (m *memEngine) load(key string) (string, bool) {
	e, ok := m.data[key]
	if !ok {
		return "", false
	}
	if m.now().After(e.deadline) {
		delete(m.data, key)
		return "", false
	}
	return e.val, true
}

func (m *memEngine) store(ttl time.Duration, key, val string) {
	m.data[key] = memEntry{val: val, deadline: m.now().Add(ttl)}
}

func (m *memEngine) get(ctx context.Context, ttl time.Duration, key string, fn func(context.Context, string) (string, error)) (string, error) {
	m.mu.Lock()
	val, ok := m.load(key)
	m.mu.Unlock()
	if ok {
		return val, nil
	}
	val, err := fn(ctx, key)
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	m.store(ttl, key, val)
	m.mu.Unlock()
	return val, nil
}

func (m *memEngine) getMulti(ctx context.Context, ttl time.Duration, keys []string, fn func(context.Context, []string) (map[string]string, error)) (map[string]string, error) {
	out := make(map[string]string, len(keys))
	var missing []string
	m.mu.Lock()
	for _, k := range keys {
		if v, ok := m.load(k); ok {
			out[k] = v
		} else {
			missing = append(missing, k)
		}
	}
	m.mu.Unlock()
	if len(missing) == 0 {
		return out, nil
	}
	loaded, err := fn(ctx, missing)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	for k, v := range loaded {
		m.store(ttl, k, v)
		out[k] = v
	}
	m.mu.Unlock()
	return out, nil
}

func (m *memEngine) peek(_ context.Context, _ time.Duration, key string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.load(key)
	return v, ok, nil
}

func (m *memEngine) set(ctx context.Context, ttl time.Duration, key string, fn func(context.Context, string) (string, error)) error {
	val, err := fn(ctx, key)
	if err != nil {
		return err
	}
	return m.forceSet(ctx, ttl, key, val)
}

func (m *memEngine) setMulti(ctx context.Context, ttl time.Duration, keys []string, fn func(context.Context, []string) (map[string]string, error)) error {
	vals, err := fn(ctx, keys)
	if err != nil {
		return err
	}
	return m.forceSetMulti(ctx, ttl, vals)
}

func (m *memEngine) forceSet(_ context.Context, ttl time.Duration, key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store(ttl, key, value)
	return nil
}

func (m *memEngine) forceSetMulti(_ context.Context, ttl time.Duration, values map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range values {
		m.store(ttl, k, v)
	}
	return nil
}

func (m *memEngine) del(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *memEngine) delMulti(_ context.Context, keys ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		delete(m.data, k)
	}
	return nil
}

func (m *memEngine) touch(_ context.Context, ttl time.Duration, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.load(key); ok {
		m.store(ttl, key, v)
	}
	return nil
}

func (m *memEngine) touchMulti(_ context.Context, ttl time.Duration, keys ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		if v, ok := m.load(k); ok {
			m.store(ttl, k, v)
		}
	}
	return nil
}
