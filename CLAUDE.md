# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

redcache is a Go library that provides a cache-aside implementation for Redis, built on the [rueidis](https://github.com/redis/rueidis) client. It uses client-side caching with Redis invalidation messages to reduce round trips, and distributed locking (SET NX with UUIDv7) to prevent thundering herd problems.

## Commands

Go and golangci-lint are pinned in `.tool-versions`; with
[asdf](https://asdf-vm.com) installed, run `make setup` to install them.
`make help` lists repository tasks, and `make check` runs the full
build/lint/coverage/test gate.

```bash
# Run all tests (requires Redis on localhost:6379)
go test ./...

# Run a single test
go test -run TestCache_Get ./...

# Run tests with race detector
go test -race ./...

# Formatting and static checks
make lint

# Format
make fmt

# Start Redis for local development
docker compose up -d
```

## Architecture

The library is a single-package Go module (`package redcache`) with internal helpers.

**Public surface: `Cache`** (`cache.go`) — a concrete, non-generic cache whose generic methods (Go 1.27) infer `K` and `V` independently on each operation. Build a `Conn` with `Open`, then construct caches with `New` (default `StringKeyCodec`), `NewKeyed`, or `NewBytes`; each cache owns a `KeyCodec` and `Codec` (both `any`-based) and shares the `Conn`'s `rueidis.Client` and invalidation stream. There is no interface form (generic methods cannot satisfy interfaces); `OpenMemory` returns a map-backed `Conn` (`memory.go`) for tests. `Conn`/`Cache` hold the unexported `engine` interface, implemented by `cacheAside` (Redis) and `memEngine`. The unexported `cacheAside` engine backs the single-key path (`get.go`), multi-key path (`getmulti.go`), del/touch operations (`ops.go`), and metric emitters (`emit.go`). Core operations:
- `Get(ctx, ttl, key, fn)` — single-key cache-aside with distributed lock
- `GetMulti(ctx, ttl, keys, fn)` — multi-key cache-aside; groups SET operations by Redis cluster slot for efficient batching
- `Peek` — read-only client-side-cached lookup (no loader, no lock)
- `Set` / `SetMulti` / `ForceSet` / `ForceSetMulti` — write-through priming
- `Del` / `DelMulti` — cache invalidation
- `Touch` / `TouchMulti` — sliding-TTL extension

**How it works — the Get loop:**

1. **Register** (`register`) — creates a local `lockEntry` with a context that auto-expires after `lockTTL`. Returns the context's `Done()` channel, which serves as the "wait" channel.
2. **Try cached get** (`tryGet`) — calls `DoCache` (rueidis client-side caching) to GET the key. This serves a **dual purpose**: it returns any cached value *and* subscribes the client to Redis invalidation notifications for that key. If the value is missing or is a lock (has the lock prefix), it returns `errNotFound`.
3. **Try lock + set** (`trySetKeyFunc`) — attempts `SET NX GET PX` to acquire a distributed lock. `NX` ensures only one caller wins; `GET` returns the old value (nil on success); `PX` sets lock TTL in milliseconds. On success, calls the user's callback, then atomically replaces the lock with the real value via `setKeyLua` (Lua script that verifies lock ownership before SET).
4. **Wait** — if another caller holds the lock, wait on the channel from step 1. The channel closes when either:
   - **Redis invalidation arrives** — the lock holder SETs the real value, Redis notifies all clients that cached the key (from step 2's `DoCache`), `onInvalidate` fires and cancels the lock entry's context
   - **Lock TTL expires** — the context times out automatically
5. **Retry** — go back to step 1; `tryGet` now finds the real value via client-side cache.

The key insight: because `tryGet` uses `DoCache`, any client that reads a lock value automatically subscribes to invalidation for that key. When the lock holder replaces the lock with the real value, Redis pushes an invalidation message, which unblocks all waiters.

**Lock mechanism:** Lock values are prefixed UUIDv7 strings (default prefix `__redcache:lock:`). `tryGet` recognizes these by prefix and treats them as cache misses. Lua scripts (`delKeyLua`, `setKeyLua`) atomically verify lock ownership before deleting or overwriting values. Lock entries are tracked locally via `syncx.Map` with context-based auto-expiration via `context.AfterFunc`.

**Multi-key operations:** `GetMulti` follows the same pattern but batches operations. `tryGetMulti` uses `DoMultiCache` for batch reads. `tryLockMulti` acquires locks in batch. `setMultiWithLock` groups SET Lua scripts by Redis cluster slot (via `cmdx.Slot`) and executes each group in parallel via goroutines coordinated with `sync.WaitGroup` (results merged under a `sync.Mutex`). `syncx.WaitForAll` waits on multiple channels simultaneously by spawning one goroutine per channel coordinated via a buffered channel and `sync.WaitGroup`, returning the context error on cancellation.

**Internal packages:**
- `internal/cmdx` — Redis cluster slot calculation (CRC16) for grouping multi-key operations
- `internal/lockpool` — fast lock-value generation: a per-instance UUIDv7 prefix plus an atomic counter, avoiding a per-lock `uuid.NewV7()` call
- `internal/poolx` — typed `sync.Pool` wrappers that reuse `[]T` slice headers (stored as `*[]T` to avoid interface-boxing allocations) across multi-key paths; capacity-capped, with live elements cleared on return
- `internal/syncx` — Generic typed wrapper around `sync.Map`; `WaitForAll` waits on multiple channels by spawning one goroutine per channel coordinated via a buffered channel and `sync.WaitGroup`, returning the context error on cancellation

## Code Conventions

- **Import ordering** (enforced by `gci`): standard library, third-party, then `github.com/dcbickfo/redcache` internal packages
- Tests are integration tests requiring a running Redis instance at `127.0.0.1:6379`
- Tests use UUID-based keys to avoid collisions between test runs
- Linting config is in `.golangci.yml` (v2 format); `make lint` runs the pinned
  golangci-lint v2.13.1 with Go 1.27 generic-method support.
