# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

redcache is a Go library that provides a cache-aside implementation for Redis, built on the [rueidis](https://github.com/redis/rueidis) client. It uses client-side caching with Redis invalidation messages to reduce round trips, and distributed locking (SET NX with UUIDv7) to prevent thundering herd problems.

## Commands

```bash
# Run all tests (requires Redis on localhost:6379)
go test ./...

# Run a single test
go test -run TestCacheAside_Get ./...

# Run tests with race detector
go test -race ./...

# Lint
golangci-lint run

# Format (v2 formatters section)
golangci-lint fmt

# Start Redis for local development
docker compose up -d
```

## Architecture

The library is a single-package Go module (`package redcache`) with internal helpers.

**Core type: `CacheAside`** (`cacheaside.go`) — wraps a `rueidis.Client` and provides:
- `Get(ctx, ttl, key, fn)` — single-key cache-aside with distributed lock
- `GetMulti(ctx, ttl, keys, fn)` — multi-key cache-aside; groups SET operations by Redis cluster slot for efficient batching
- `Del` / `DelMulti` — cache invalidation

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

**Multi-key operations:** `GetMulti` follows the same pattern but batches operations. `tryGetMulti` uses `DoMultiCache` for batch reads. `tryLockMulti` acquires locks in batch. `setMultiWithLock` groups SET Lua scripts by Redis cluster slot (via `cmdx.Slot`) and executes each group in parallel using `errgroup`. `syncx.WaitForAll` waits on multiple channels simultaneously using `reflect.Select`.

**Internal packages:**
- `internal/cmdx` — Redis cluster slot calculation (CRC16) for grouping multi-key operations
- `internal/syncx` — Generic typed wrapper around `sync.Map`; `WaitForAll` uses `reflect.Select` to wait on multiple channels with context cancellation
- `internal/mapsx` — Generic map helpers (`Keys`, `Values`)

## Code Conventions

- **Import ordering** (enforced by `gci`): standard library, third-party, then `github.com/dcbickfo/redcache` internal packages
- Tests are integration tests requiring a running Redis instance at `127.0.0.1:6379`
- Tests use UUID-based keys to avoid collisions between test runs
- Linting config is in `.golangci.yml` (v2 format) — comments must end in a period (`godot`), naked returns limited to functions under 30 lines
- **Claude Code hooks** (`.claude/settings.json`): auto-lint/format `.go` files after Edit/Write; tests run automatically before Claude stops
