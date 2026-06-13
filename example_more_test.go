package redcache_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

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

// View derives a second typed cache that shares the first cache's Redis
// connection and invalidation stream — one client backing multiple value types.
func ExampleView() {
	users, err := redcache.NewString[string](
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.StringCodec{},
		redcache.WithLockTTL(5*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer users.Close() // closing the parent closes the shared client

	// loginCounts shares users' client, connection, and invalidation stream.
	loginCounts, err := redcache.View[string, int](users, redcache.StringKeyCodec{}, redcache.JSONCodec[int]{})
	if err != nil {
		panic(err)
	}

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
