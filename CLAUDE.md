# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`redcache` is a production-ready cache-aside implementation for Redis with distributed locking, client-side caching, and full Redis Cluster support. Built on [rueidis](https://github.com/redis/rueidis) for optimal performance.

**Key Features:**
- Cache-aside pattern with automatic cache population
- Distributed locking to prevent thundering herd
- Redis Cluster support with automatic hash slot grouping
- Batch operations (GetMulti/SetMulti)
- Client-side caching via RESP3
- Two implementations: read-only (`CacheAside`) and read-write (`PrimeableCacheAside`)

## Essential Commands

### Testing

redcache uses Go build tags to organize tests by category:

```bash
# Unit tests only (no Redis required, <0.3s)
make test-unit
go test -short ./...

# Integration tests (requires Redis on localhost:6379)
make test-integration
go test -tags=integration ./...

# Distributed tests (multi-client coordination)
make test-distributed
go test -tags=distributed ./...

# Redis Cluster tests (requires cluster on ports 17000-17005)
make test-cluster
go test -tags=cluster ./...

# Run multiple test suites
go test -tags="integration,distributed" ./...

# All tests (comprehensive: unit + integration + distributed + cluster + examples)
make test
go test -tags="integration,distributed,cluster,examples" ./...

# Run specific test with race detector
go test -tags=integration -run TestName -race -count=1 -v

# Run specific package tests
go test -tags=integration ./internal/writelock -v
```

### Docker/Redis

```bash
# Start single Redis instance
make docker-up

# Start Redis Cluster (6 nodes, ports 7000-7005)
make docker-cluster-up

# Stop instances
make docker-down
make docker-cluster-down
```

### Linting

```bash
# Run linter
make lint
golangci-lint run

# Run linter with auto-fix
make lint-fix

# Check single file
golangci-lint run internal/writelock/writelock.go
```

### Benchmarking

```bash
# Run benchmarks
go test -bench=. -benchmem

# Run specific benchmark
go test -bench=BenchmarkPrimeableCacheAside_SetMulti -benchmem -count=10

# Profile memory allocations
go test -bench=BenchmarkSetMulti -benchmem -memprofile=mem.prof
go tool pprof mem.prof

# Compare benchmarks
benchstat baseline.txt optimized.txt
```

## Architecture Overview

### Core Components

**1. CacheAside (cacheaside.go)**
- Read-only cache implementation
- Handles Get/GetMulti operations with automatic cache population
- Uses read locks (prevents concurrent callback execution for same key)
- Simpler locking model: SET NX for lock acquisition

**2. PrimeableCacheAside (primeable_cacheaside.go)**
- Read-write cache implementation
- Adds Set/SetMulti/ForceSet operations
- Uses both read locks (Get) and write locks (Set)
- Complex locking: custom Lua script prevents race between Get and Set
- **Critical distinction**: Set operations must be able to overwrite existing cached values but NOT overwrite active Get locks

**3. WriteLock (internal/writelock/writelock.go)**
- Distributed lock implementation for Set/SetMulti operations
- Uses Redis SET NX with automatic TTL refresh
- **Redis Cluster aware**: Automatically groups keys by hash slot before Lua script execution
- Object pooling for reduced allocations (slotGroupPool, lockInfoMapPool, stringSlicePool)
- Single-instance lock pattern (NOT Redlock) - suitable for cache coordination, not correctness

### Key Design Patterns

**Hash Slot Grouping (Redis Cluster Compatibility)**
- Lua scripts can only operate on keys in same slot
- `internal/writelock/writelock.go` automatically handles this in `groupKeysBySlot()`
- `groupLockAcquisitionsBySlot()` and `groupSetValuesBySlot()` handle slot grouping for CAS Lua scripts
- Regular SET commands use `rueidis.DoMulti` which automatically routes to correct nodes

**Distributed Lock Coordination**
- Read locks (Get): SET NX - simple, fail if any lock exists
- Write locks (Set): Custom Lua script - can overwrite real values but NOT active locks
- Lock values use UUIDv7 for uniqueness
- Automatic lock refresh via goroutines (TTL/2 interval)
- Lock release uses Lua scripts to verify ownership before deletion

**Object Pooling (Performance Optimization)**
- `sync.Pool` used in hot paths to reduce allocations
- Three pools in writelock: slotGroupPool, lockInfoMapPool, stringSlicePool
- **IMPORTANT**: Pooled objects must be cleared before returning to pool
- Helper function `clearAndReturnLockInfoMap()` ensures proper cleanup

**Client-Side Caching**
- Uses rueidis RESP3 client-side caching
- Automatic invalidation via Redis pub/sub
- Transparent to application code

### Internal Packages

**internal/writelock/**
- Distributed write lock implementation
- Hash slot grouping for cluster support
- Implements `Locker` interface

**internal/lockpool/**
- Pool of reusable lock tracking structures
- Reduces allocations in high-concurrency scenarios

**internal/cmdx/**
- Redis command utilities
- Hash slot calculation: `Slot(key string) uint16`

**internal/syncx/**
- Concurrency utilities
- Thread-safe map wrapper
- Wait group helpers

**internal/mapsx/**
- Generic map utilities (Keys, Values, Merge)

**internal/slicesx/**
- Generic slice utilities (Filter, FilterExclude, Contains, Dedupe)

## Important Implementation Details

### Cognitive Complexity Limit

The linter enforces cognitive complexity < 15 per function. When implementing complex logic:
1. Extract helper functions
2. Use early returns to reduce nesting
3. Avoid deeply nested conditionals
4. See `internal/writelock/writelock.go` for examples of proper decomposition

### Redis Cluster Considerations

**CROSSSLOT Errors**
- Lua scripts fail if keys are in different slots
- Always group by hash slot before executing Lua scripts
- Use `cmdx.Slot(redisKey)` to calculate slot (CRC16 % 16384)

**Hash Tags**
- Keys like `{user:1000}:profile` hash only the part in braces
- Allows colocating related keys in same slot
- Document this in examples and tests

### Lock Safety Guarantees

**What These Locks ARE Good For:**
- Cache coordination (preventing thundering herd)
- Preventing duplicate background work
- Rate limiting coordination
- **Use case: Efficiency/optimization, not correctness**

**What These Locks Are NOT Good For:**
- Financial transactions
- Inventory management
- Distributed state machines
- **Reason: Single Redis instance dependency, no fencing tokens, vulnerable to clock skew and network partitions**

**Failure Scenarios:**
- Master crashes before replica sync → lock lost → duplicate locks possible
- Network partition → split-brain → both sides can acquire same lock
- Clock skew (NTP) → early lock expiration

**Better Alternatives for Correctness:**
- Database transactions with proper isolation levels
- Optimistic locking with version numbers
- Distributed consensus systems (etcd, Consul, ZooKeeper)

### Testing Patterns

**Test Categories with Build Tags:**
1. **Unit tests** (`*_unit_test.go`): No build tag, use `-short` flag. Fast isolated tests with mocks, no Redis required.
2. **Integration tests** (`*_test.go`): `//go:build integration` tag. Test with real Redis for end-to-end functionality.
3. **Distributed tests** (`*_distributed_test.go`): `//go:build distributed` tag. Multi-client coordination tests.
4. **Cluster tests** (`*_cluster_test.go`): `//go:build cluster` tag. Redis Cluster specific tests (ports 17000-17005).
5. **Examples** (`examples/*_test.go`): `//go:build examples` tag. Runnable documentation examples.

**Test Organization - IMPORTANT:**
- **Always use subtests** with `t.Run()` for test organization
- **Always use `t.Context()`** instead of `context.Background()` in tests
- **Unit tests must check `testing.Short()`**: Skip when NOT in short mode (`if !testing.Short() { t.Skip(...) }`)

**Test Naming Convention:**
- Parent test: `Test<Component>_<Operation>`
- Subtests: Descriptive names like `"CacheHit"`, `"CacheMiss_LockAcquired"`, `"CallbackError"`
- Example: `TestCacheAside_Get` with subtests `"CacheHit"` and `"CacheMiss_LockAcquired"`

**Test Structure Pattern:**
```go
func TestCacheAside_Get(t *testing.T) {
    if !testing.Short() {
        t.Skip("Skipping unit test in non-short mode")
    }

    // Shared setup here if needed

    t.Run("CacheHit", func(t *testing.T) {
        ctx := t.Context()  // Use test context
        // ... test logic
    })

    t.Run("CacheMiss_LockAcquired", func(t *testing.T) {
        ctx := t.Context()
        // ... test logic
    })
}
```

**Redis Setup:**
- Single Redis: Tests assume `localhost:6379`
- Cluster: Tests use ports 17000-17005
- Always use `makeClient(t)` helper for setup
- Always defer `client.Close()`
- Always use `t.Context()` for automatic cleanup

### Benchmarking Best Practices

**Baseline Capture:**
```bash
go test -bench=BenchmarkName -benchmem -count=10 > baseline.txt
# Make changes
go test -bench=BenchmarkName -benchmem -count=10 > optimized.txt
benchstat baseline.txt optimized.txt
```

**Memory Profiling:**
```bash
go test -bench=BenchmarkName -memprofile=mem.prof
go tool pprof -alloc_space mem.prof
go tool pprof -inuse_space mem.prof
```

**Success Criteria:**
- ≥6 samples for statistical confidence (use -count=10)
- Report allocs/op and B/op with -benchmem
- No significant performance regression (within 5%)

## Common Development Workflows

### Adding New Features with Distributed Locking

1. **Determine lock type needed**:
   - Read-only operation → Use CacheAside pattern (SET NX)
   - Write operation → Use WriteLock or custom Lua script

2. **Check Redis Cluster compatibility**:
   - Batch operations → Group by hash slot first
   - Lua scripts → Ensure keys in same slot
   - Use `cmdx.Slot()` for slot calculation

3. **Test sequence**:
   ```bash
   # Unit tests (with mocks, no Redis)
   go test -short -run TestNewFeature -v

   # Integration tests (with real Redis)
   go test -tags=integration -run TestNewFeature -v

   # Race detector
   go test -tags=integration -run TestNewFeature -race -count=1

   # Distributed coordination
   go test -tags=distributed -run TestNewFeature -v

   # Cluster support
   make docker-cluster-up
   go test -tags=cluster -run TestNewFeature -v
   ```

4. **Linter verification**:
   ```bash
   golangci-lint run
   # Fix cognitive complexity if > 15
   ```

### Debugging Lock Issues

**Common Symptoms:**
- Tests hang → Deadlock or lock not released
- Race detector warnings → Unsafe concurrent access
- Flaky tests → Race condition in lock acquisition

**Debugging Steps:**
1. Enable verbose logging: Tests print lock IDs and durations
2. Check for missing cleanup: Ensure `defer cleanup()` is called
3. Verify lock TTL: Should be > expected operation duration
4. Check Redis directly:
   ```bash
   redis-cli KEYS "__redcache:lock:*"
   redis-cli GET "__redcache:lock:keyname"
   redis-cli TTL "__redcache:lock:keyname"
   ```

### Performance Optimization

**Profiling Workflow:**
1. Create baseline benchmarks
2. Run memory profiler to identify hotspots
3. Target top allocation sites (>3% of total)
4. Consider object pooling (sync.Pool) for:
   - Frequently allocated maps/slices
   - Clear lifecycle (easy to clean and return to pool)
   - NOT for: contexts, tickers, channels (complex lifecycle)

**Object Pooling Rules:**
1. Always clear before returning to pool
2. Use defer for pool cleanup
3. Document pool ownership in comments
4. Validate with race detector

**See:**
- `PROFILING_ANALYSIS.md` - Detailed profiling methodology
- `OPTIMIZATION_RESULTS.md` - Object pooling case study

## Repository Structure Highlights

```
redcache/
├── cacheaside.go              # Read-only cache implementation
├── primeable_cacheaside.go    # Read-write cache implementation
├── internal/
│   ├── writelock/             # Distributed write locks (cluster-aware)
│   ├── lockpool/              # Lock structure pooling
│   ├── cmdx/                  # Redis command utilities (hash slots)
│   ├── syncx/                 # Concurrency primitives
│   ├── mapsx/                 # Generic map utilities
│   └── slicesx/               # Generic slice utilities
├── examples/                  # Working examples with build tag
├── Makefile                   # All build/test commands
├── docker-compose.yml         # Redis single + cluster setup
├── DISTRIBUTED_LOCK_SAFETY.md # Lock safety analysis
├── REDIS_CLUSTER.md           # Cluster compatibility guide
├── PROFILING_ANALYSIS.md      # Performance optimization guide
└── OPTIMIZATION_RESULTS.md    # Object pooling case study
```

## Anti-Patterns to Avoid

1. **Don't use locks for correctness-critical operations** - These are optimization locks, not safety locks
2. **Don't forget to group by hash slot** - Lua scripts will fail with CROSSSLOT errors
3. **Don't pool complex objects** - Tickers, contexts, channels have complex lifecycles
4. **Don't shadow variables in tests** - Linter will fail (use different names)
5. **Don't create high cognitive complexity** - Extract helper functions to stay under 15
6. **Don't forget cleanup in tests** - Always `defer client.Close()` and `defer cleanup()`
7. **Don't assume keys are in same slot** - Even similar-looking keys may hash differently
8. **Don't use `context.Background()` in tests** - Always use `t.Context()` for automatic cleanup
9. **Don't skip organizing tests with subtests** - Always use `t.Run()` for better structure and isolation
10. **Don't forget build tags** - Unit tests need `testing.Short()` check, integration tests need `//go:build integration` tag

## Additional Resources

- See `TESTING.md` for comprehensive testing guide (build tags, mocks, best practices)
- See `README.md` for user-facing documentation
- See `DISTRIBUTED_LOCK_SAFETY.md` for lock safety analysis
- See `REDIS_CLUSTER.md` for cluster deployment guide
- See `examples/` for working code examples
- See `internal/writelock/writelock.go` for object pooling implementation
