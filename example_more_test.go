package redcache_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

// failOnEmpty is a value codec that refuses to encode the empty string, used to
// force a deterministic per-key failure in the ForceSetMulti example.
type failOnEmpty struct{}

func (failOnEmpty) Encode(s string) ([]byte, error) {
	if s == "" {
		return nil, errors.New("empty value not allowed")
	}
	return []byte(s), nil
}

func (failOnEmpty) Decode(b []byte) (string, error) { return string(b), nil }

// Multi-key writes report per-key partial failures as *BatchKeyError[K],
// reachable via errors.As. Note the generic type argument on the target pointer:
// it must match the cache's key type (string here).
func ExampleCache_ForceSetMulti() {
	cache, err := redcache.NewString[string](
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		failOnEmpty{},
	)
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	err = cache.ForceSetMulti(context.Background(), time.Minute, map[string]string{
		"a": "alpha",
		"b": "", // fails to encode
	})

	var be *redcache.BatchKeyError[string]
	if errors.As(err, &be) {
		fmt.Println("b failed:", be.HasError("b"))
		fmt.Println("a succeeded:", !be.HasError("a"))
		for k, kerr := range be.Failed {
			_ = k
			_ = kerr // handle each failed key
		}
	}
}

// Set primes a key under a write lock without a prior read. If the callback
// returns an error, the cache restores the value that was there before, so a
// failed refresh never leaves the key empty. Redis-dependent, so it omits an
// Output: directive and is compiled but not run by `go test`.
func ExampleCache_Set() {
	cache, err := redcache.NewString[string](
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.StringCodec{},
		redcache.WithLockTTL(5*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	err = cache.Set(context.Background(), time.Minute, "config:greeting",
		func(ctx context.Context, key string) (string, error) {
			// Compute the value to cache; runs under a write lock.
			return "hello", nil
		},
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("primed")
}

// ForceSet writes a value unconditionally, bypassing the lock. Any in-progress
// Get or Set on the same key sees ErrLockLost and retries. Redis-dependent, so
// it omits an Output: directive.
func ExampleCache_ForceSet() {
	cache, err := redcache.NewString[string](
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.StringCodec{},
		redcache.WithLockTTL(5*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	if err := cache.ForceSet(context.Background(), time.Minute, "config:greeting", "hola"); err != nil {
		panic(err)
	}
	fmt.Println("forced")
}

// New keys the cache by a domain type via a KeyCodec. KeyCodecFunc adapts a
// plain function into a KeyCodec.
func ExampleNew() {
	type UserID int64
	type User struct {
		ID   UserID
		Name string
	}

	userIDCodec := redcache.KeyCodecFunc[UserID](func(id UserID) (string, error) {
		return fmt.Sprintf("user:%d", id), nil
	})

	cache, err := redcache.New[UserID, User](
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		userIDCodec,
		redcache.JSONCodec[User]{},
		redcache.WithLockTTL(5*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	u, err := cache.Get(context.Background(), time.Minute, UserID(123),
		func(ctx context.Context, id UserID) (User, error) {
			// Load from your data source; runs only on a miss.
			return User{ID: id, Name: "alice"}, nil
		},
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(u.Name)
}

// A Conn owns one Redis client and invalidation stream; Of/StringOf derive
// typed views over it — one client backing multiple value types.
func ExampleOf() {
	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.WithLockTTL(5*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer conn.Close() // closing the Conn closes the shared client (and all views)

	// users and loginCounts share one client, connection, and invalidation stream.
	users, err := redcache.StringOf[string](conn, redcache.StringCodec{})
	if err != nil {
		panic(err)
	}
	loginCounts, err := redcache.Of[string, int](conn, redcache.StringKeyCodec{}, redcache.JSONCodec[int]{})
	if err != nil {
		panic(err)
	}
	_ = users

	n, err := loginCounts.Get(context.Background(), time.Minute, "u-123",
		func(ctx context.Context, key string) (int, error) {
			return 7, nil
		},
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(n)
}

// countingMetrics shows the embed-and-override pattern: embed NoopMetrics and
// implement only the events you care about.
type countingMetrics struct {
	redcache.NoopMetrics
	hits, misses atomic.Int64
}

func (m *countingMetrics) CacheHits(n int64)   { m.hits.Add(n) }
func (m *countingMetrics) CacheMisses(n int64) { m.misses.Add(n) }

// Pass a Metrics implementation via WithMetrics to count cache events. The cache
// calls these on the hot path, batched per operation. This example invokes them
// directly (no Redis needed) to show the counting shape.
func ExampleNoopMetrics() {
	m := &countingMetrics{}
	// In real use: redcache.NewString[string](opt, codec, redcache.WithMetrics(m)).
	m.CacheHits(3)
	m.CacheMisses(1)
	fmt.Println(m.hits.Load(), m.misses.Load())
	// Output: 3 1
}
