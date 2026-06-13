# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.3.0] - 2026-06-13

Generics-first collapse: the public surface is now a single generic `Cache[K, V]`
interface. The stampede/lock engine and the on-disk value format are unchanged —
this is a surface redesign only. Stay v0.x; a v1 will be cut once the surface
settles.

### Breaking
- **Single generic `Cache[K, V]` interface** replaces the four concrete types.
  Removed `CacheAside`, `PrimeableCacheAside`, `Typed[K, V]`, and
  `PrimeableTyped[K, V]`. Read methods (`Get`/`GetMulti`), write/prime methods
  (`Set`/`SetMulti`/`ForceSet`/`ForceSetMulti`), invalidation (`Del`/`DelMulti`),
  TTL extension (`Touch`/`TouchMulti`), `Client()`, and `Close()` are all methods
  on `Cache[K, V]`.
- **New constructors; old ones removed.** Removed `NewRedCacheAside`,
  `NewPrimeableCacheAside`, `NewTyped`, `NewStringTyped`, `NewPrimeableTyped`, and
  `NewPrimeableStringTyped`. Added:
  - `New[K, V](clientOption, keyCodec, valCodec, opts...)`
  - `NewString[V](clientOption, valCodec, opts...)` — `StringKeyCodec` preset.
  - `NewBytes(clientOption, opts...)` — zero-copy `Cache[string, []byte]`.
  - `View[K, V](parent, keyCodec, valCodec)` — derive a sibling typed view sharing
    one client/engine/invalidation stream, for caching multiple value types over a
    single connection.
  Constructors take a `rueidis.ClientOption` and build the client internally.
- **Functional options replace the `CacheAsideOption` struct.** Removed
  `CacheAsideOption`. Configure with `WithLockTTL`, `WithLogger`, `WithMetrics`,
  `WithLockPrefix`, `WithRefreshLockPrefix`, `WithRefreshAfterFraction`,
  `WithRefreshBeta`, `WithRefreshWorkers`, `WithRefreshQueueSize`, and
  `WithClientBuilder`.
- **Typed batch error only.** Removed `BatchError`, `NewBatchError`, and
  `NewBatchKeyError` from the public surface. Multi-key write partial failures
  surface as `*BatchKeyError[K]` via `errors.As` (with nil-safe `HasFailures`,
  `ErrorFor`, `HasError` accessors).
- **`BytesCodec` renamed to `UnsafeBytesCodec`** — the name now states the
  zero-copy retention hazard: the decoded slice aliases borrowed library memory
  and must not be mutated or retained past the call.
- **`New` and `View` reject nil codecs at construction** rather than panicking on
  the first call.

### Added
- `WithRefreshTimeout(d)` — bounds how long a refresh-ahead callback may run,
  defaulting to the data `ttl` passed to `Get`/`GetMulti` rather than `LockTTL`.
- `ErrInvalidTTL` — write methods (`Set`/`SetMulti`/`ForceSet`/`ForceSetMulti`)
  now reject a non-positive `ttl` with this sentinel instead of leaking the raw
  Redis `PX 0` error.
- `redcachetest.Fake[K, V]` (constructed with `redcachetest.New[K, V]()`) — an
  in-memory `Cache[K, V]` for adopter unit tests. Models the observable
  single-process cache-aside contract (loader-once-per-miss, presence-based hits,
  TTL expiry); does not model distributed single-flight, the lock layer,
  invalidation pushes, the envelope, or refresh-ahead.

### Fixed
- **Refresh-ahead callbacks were silently capped at `LockTTL`.** A refresh
  function that legitimately ran longer than `LockTTL` (e.g. a 20s fn under a 10s
  lock) was cancelled and its result lost. `WithRefreshTimeout` decouples the
  refresh compute budget from `LockTTL`, and the back-write that records a
  slow-but-successful refresh is decoupled from the timeout so the write is kept.

## [v0.2.0] - 2026-05-04

### Breaking
- **On-disk value format changed.** Stored values are now wrapped in an envelope (`__redcache:v1:<delta_ns>:<payload>`) so refresh-ahead can read the original compute time for XFetch sampling. Reads transparently unwrap this envelope, and legacy un-wrapped values continue to be served (with `delta=0`, falling back to the simple floor refresh check). **Rollback warning:** if a deployment writes values under v0.2.0+ and then rolls back to v0.1.7 or earlier, those clients will return the raw envelope string as the user value. Flush affected keys (or run a full cache invalidation) before rolling back.
- `LockPrefix` is now validated at construction. If `LockPrefix` is set such that the envelope prefix `__redcache:v1:` would itself look like a lock value, `NewRedCacheAside` returns an error rather than silently making every read miss. The default lock prefix (`__redcache:lock:`) is unaffected.

### Added
- `RefreshBeta` for XFetch probabilistic refresh: weights per-read refresh probability by recorded compute time so slow-to-recompute values are refreshed earlier. Default `0` keeps XFetch disabled (simple floor only).
- `Touch` and `TouchMulti` for sliding-TTL extensions without re-running the origin function.
- `LockWaitDuration` metric capturing time spent waiting on contended locks.
- In-process leader/follower coordination so concurrent goroutines missing the same key issue only one Redis `SET NX` instead of one per goroutine; followers wait on the in-process leader's invalidation.

### Changed
- Cut allocations on `Get`/`GetMulti` hot path via pooled slice headers and slice reuse.
- High-volume metrics (cache hit/miss) switched to count-based emission and short-circuit when no metrics sink is configured.
- Stripped hot-path `Debug` log calls and avoided map reallocation in `registerAll`.
- `BatchError.Error()` emits failed keys in sorted order so the string is stable.

## [v0.1.7] - 2026-04-27

### Added
- `PrimeableCacheAside` with `Set` and `SetMulti` for write-through caching with CAS-based lock ownership.
- Refresh-ahead caching via `RefreshAfterFraction`, `RefreshWorkers`, and `RefreshQueueSize`. Background workers refresh stale-but-valid entries while the previous value is still served.
- `Metrics` interface with `NoopMetrics` default. Hooks for cache hit/miss, lock contention, lock loss, and refresh triggered/skipped/dropped/panicked events.
- `BatchError.ErrorFor(key)` and `BatchError.HasError(key)` helpers (nil-safe) for inspecting per-key failures from multi-key operations.
- `RefreshLockPrefix` option for customizing the distributed refresh lock prefix; defaults to `__redcache:refresh:`.
- Runnable `ExampleCacheAside_Get` and `ExampleCacheAside_GetMulti` in `example_test.go`.
- Benchmarks in `cacheaside_bench_test.go`.
- CI hardening: PR triggers, concurrency-group cancellation of stale runs, golangci-lint pinned to v2.11.4.

### Changed
- Library-internal errors in `tryGet`, `tryLockMulti`, and `executeSetStatements` are wrapped with key context for easier debugging.
- `RefreshPanicked` is emitted per affected key (multi-key refreshes now tag each key).
- Integration tests now run with `t.Parallel()` for faster CI.

## [v0.1.6] - 2025-10-06

### Fixed
- Fix `Register` race condition with `CompareAndDelete` for expired lock entries.

### Added
- golangci-lint configuration with `godot`, `gci`, and `nakedret` linters.
- Additional concurrency tests for lock contention scenarios.

## [v0.1.5] - 2025-10-06

### Fixed
- Address race condition in lock entry cleanup using `context.AfterFunc`.

## [v0.1.4] - 2025-04-04

### Fixed
- Fix indefinite wait when lock context expires without Redis invalidation.

## [v0.1.3] - 2025-03-21

### Fixed
- Fix refresh bug in client-side cache invalidation handling.

## [v0.1.2] - 2025-03-13

### Changed
- Use `iter.Seq` iterator for key iteration in multi-key operations.
- Add `ClientBuilder` option for custom `rueidis.Client` construction.

## [v0.1.1] - 2025-02-04

### Changed
- Handle multi-slot operations for keys in Redis cluster mode (#1).
- Group SET Lua scripts by Redis cluster slot for efficient batching.

## [v0.1.0] - 2025-02-04

### Added
- Initial release of redcache.
- Cache-aside pattern with `Get` and `GetMulti` operations.
- Distributed locking with `SET NX GET PX` and UUIDv7 lock values.
- Client-side caching via rueidis `DoCache` with Redis invalidation.
- `Del` and `DelMulti` for cache invalidation.
- Lua scripts for atomic lock verification on SET and DEL.
- CI workflow with GitHub Actions.

[v0.3.0]: https://github.com/dcbickfo/redcache/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/dcbickfo/redcache/compare/v0.1.7...v0.2.0
[v0.1.7]: https://github.com/dcbickfo/redcache/compare/v0.1.6...v0.1.7
[v0.1.6]: https://github.com/dcbickfo/redcache/compare/v0.1.5...v0.1.6
[v0.1.5]: https://github.com/dcbickfo/redcache/compare/v0.1.4...v0.1.5
[v0.1.4]: https://github.com/dcbickfo/redcache/compare/v0.1.3...v0.1.4
[v0.1.3]: https://github.com/dcbickfo/redcache/compare/v0.1.2...v0.1.3
[v0.1.2]: https://github.com/dcbickfo/redcache/compare/v0.1.1...v0.1.2
[v0.1.1]: https://github.com/dcbickfo/redcache/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/dcbickfo/redcache/releases/tag/v0.1.0
