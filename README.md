# redcache

A production-ready cache-aside implementation for Redis with distributed locking, client-side caching, and full Redis Cluster support.

Built on [rueidis](https://github.com/redis/rueidis) for optimal performance and automatic client-side caching.

## Features

- **Cache-Aside Pattern**: Automatically handles cache misses with callback functions
- **Distributed Locking**: Prevents thundering herd with coordinated cache updates
- **Client-Side Caching**: Leverages Redis client-side caching to reduce network round trips
- **Redis Cluster Support**: Full support for Redis Cluster with automatic hash slot grouping
- **Batch Operations**: Efficient `GetMulti`/`SetMulti` operations
- **Write Support**: `PrimeableCacheAside` allows cache writes with `Set`/`ForceSet`
- **Type Safe**: Generic implementations for type safety
- **Production Ready**: Comprehensive test coverage including distributed and cluster scenarios

## Installation

```bash
go get github.com/dcbickfo/redcache
```

## Quick Start

### Read-Only Cache (CacheAside)

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "time"

    "github.com/redis/rueidis"
    "github.com/dcbickfo/redcache"
)

func main() {
    // Create cache client
    cache, err := redcache.NewRedCacheAside(
        rueidis.ClientOption{
            InitAddress: []string{"127.0.0.1:6379"},
        },
        redcache.CacheAsideOption{
            LockTTL: time.Second,
        },
    )
    if err != nil {
        log.Fatal(err)
    }
    defer cache.Close()

    // Get single value with automatic cache-aside
    val, err := cache.Get(context.Background(), time.Minute, "user:123",
        func(ctx context.Context, key string) (string, error) {
            // This callback only executes on cache miss
            return fetchFromDatabase(key)
        })

    // Get multiple values efficiently
    vals, err := cache.GetMulti(context.Background(), time.Minute,
        []string{"user:1", "user:2", "user:3"},
        func(ctx context.Context, keys []string) (map[string]string, error) {
            // Batch fetch from database
            return batchFetchFromDatabase(keys)
        })
}
```

### Read-Write Cache (PrimeableCacheAside)

```go
// Create cache with write support
cache, err := redcache.NewPrimeableCacheAside(
    rueidis.ClientOption{
        InitAddress: []string{"127.0.0.1:6379"},
    },
    redcache.CacheAsideOption{
        LockTTL: time.Second,
    },
)
if err != nil {
    log.Fatal(err)
}
defer cache.Close()

// Set a value (with distributed locking)
err = cache.Set(context.Background(), time.Minute, "user:123",
    func(ctx context.Context, key string) (string, error) {
        // Write to database then return value to cache
        return writeToDatabase(key, data)
    })

// Force set without locking (use cautiously)
err = cache.ForceSet(context.Background(), time.Minute, "user:123", "new-value")

// Invalidate cache
err = cache.Del(context.Background(), "user:123")
```

## Redis Cluster Support

redcache fully supports Redis Cluster with automatic hash slot grouping for efficient batch operations:

```go
// Single client works with both standalone and cluster
cache, err := redcache.NewPrimeableCacheAside(
    rueidis.ClientOption{
        InitAddress: []string{
            "localhost:17000",  // Using ports 17000-17005 to avoid conflicts
            "localhost:17001",
            "localhost:17002",
        },
    },
    redcache.CacheAsideOption{
        LockTTL: time.Second,
    },
)

// Use hash tags to colocate related keys in same slot
keys := []string{
    "{user:1000}:profile",
    "{user:1000}:settings",
    "{user:1000}:preferences",
}

// Batch operations automatically group by hash slot
vals, err := cache.GetMulti(ctx, time.Minute, keys, fetchCallback)
```

### Redis Cluster Best Practices

**Hash Tags for Related Data:**
- Use hash tags to colocate related keys: `{user:ID}:field`
- Improves batch operation efficiency
- Reduces cross-slot operations

**Performance:**
- Keys in same slot: 1 Redis call
- Keys across 3 slots: 3 Redis calls
- Use hash tags strategically for frequently accessed together data

## Distributed Locking

redcache uses distributed locks to coordinate cache updates across multiple application instances:

### Use Cases (Where Locks Are Appropriate)

✅ **Cache Coordination** (Primary Use Case)
```go
// Prevents thundering herd - multiple clients coordinate to ensure
// only one performs expensive operation
result, err := cache.Get(ctx, key, func(ctx context.Context, key string) (string, error) {
    return expensiveDatabaseQuery(key) // Only one caller executes this
})
```

✅ **Preventing Duplicate Work**
- Background job coordination
- Rate limiting coordination
- Deduplication of expensive operations

### When NOT to Use These Locks

❌ **Critical Correctness Guarantees**

**Don't use for:**
- Financial transactions
- Inventory management
- Distributed state machines
- Any operation where safety violations are unacceptable

**Why?** The distributed locks in redcache are designed for **efficiency/optimization**, not **correctness guarantees**. They:
- Depend on a single Redis instance (or cluster node per key)
- Don't provide fencing tokens
- Can be lost during network partitions or node failures
- Lost if Redis master crashes before replication completes
- Vulnerable to clock skew (NTP corrections can cause early expiration)
- NOT Redlock (no multi-instance quorum)

**Failure scenarios:**
- Master crashes → lock lost before replica sync → duplicate locks possible
- Network partition → split-brain → both sides can acquire same lock
- Failover window → brief period where locks can be lost

**Use instead:**
- Database transactions with proper isolation levels (e.g., `SELECT ... FOR UPDATE`)
- Optimistic locking with version numbers
- Distributed consensus systems (etcd, Consul, ZooKeeper) for critical coordination

### Example: Safe vs Unsafe

```go
// ✅ SAFE: Cache coordination (efficiency optimization)
cache.Get(ctx, "expensive-report", func(ctx context.Context, key string) (string, error) {
    return generateReport() // Expensive but idempotent
})

// ❌ UNSAFE: Financial transaction (correctness required)
// DON'T DO THIS - use database transactions instead
lock := acquireLock("account:123")
balance := getBalance("account:123")
setBalance("account:123", balance - amount) // NOT SAFE - lock can expire

// ✅ SAFE: Database transaction
tx.Exec("UPDATE accounts SET balance = balance - $1 WHERE id = $2 AND balance >= $1",
    amount, accountID)
```

## Testing

Run tests with different configurations:

```bash
# View all available test targets
make help

# Run unit tests (requires single Redis instance)
make docker-up
make test

# Run distributed tests (multi-client coordination)
make test-distributed

# Run Redis cluster tests
make docker-cluster-up
make test-cluster

# Run everything (unit + distributed + cluster + examples)
make docker-up && make docker-cluster-up
make test-complete

# Cleanup
make docker-down && make docker-cluster-down
```

## Architecture

### Cache-Aside Pattern

redcache implements the cache-aside (lazy loading) pattern with distributed coordination:

1. **Cache Hit**: Return value immediately from Redis
2. **Cache Miss**:
   - Acquire distributed lock for the key
   - Execute callback to fetch/compute value
   - Store in Redis with TTL
   - Release lock and return value
3. **Concurrent Requests**: Other clients wait for lock, then read cached value

### Client-Side Caching

Leverages Redis client-side caching (RESP3) for frequently accessed keys:
- Automatic invalidation notifications
- Reduced network round trips
- Transparent to application code

### Distributed Locking

Uses Redis-based distributed locks with:
- Automatic lock refresh to prevent expiration during long operations
- Lock release on cleanup
- Proper error handling and timeout support
- Hash slot awareness for Redis Cluster

## Configuration

### CacheAsideOption

```go
type CacheAsideOption struct {
    // LockTTL is the TTL for distributed locks
    // Should be longer than expected callback duration
    LockTTL time.Duration
}
```

### Client Configuration

```go
// Basic configuration
rueidis.ClientOption{
    InitAddress: []string{"127.0.0.1:6379"},
}

// Production configuration
rueidis.ClientOption{
    InitAddress: []string{
        "redis-1:6379",
        "redis-2:6379",
        "redis-3:6379",
    },
    ShuffleInit: true,                    // Load balance initial connections
    DisableCache: false,                  // Enable client-side caching
    ConnWriteTimeout: 10 * time.Second,
}
```

## Examples

See the [examples](examples/) directory for complete working examples:

- Basic cache-aside usage
- Multi-key batch operations
- Redis Cluster deployment
- Error handling patterns
- Testing strategies

## Performance Considerations

1. **Batch Operations**: Use `GetMulti`/`SetMulti` for multiple keys
2. **Hash Tags**: Use hash tags (`{user:ID}`) to colocate related data in cluster
3. **TTL Selection**: Balance freshness vs database load
4. **Lock TTL**: Set longer than expected callback duration to prevent lock expiration
5. **Client-Side Caching**: Most effective for hot keys accessed by many clients

## Contributing

Contributions welcome! Please ensure:
- Tests pass: `make test-complete`
- Linter passes: `make lint`
- Code follows existing patterns
- New features include tests

## License

MIT License - see LICENSE file for details

## Credits

Built on [rueidis](https://github.com/redis/rueidis) by Redis.