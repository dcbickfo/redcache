# redcache

[![CI](https://github.com/dcbickfo/redcache/actions/workflows/CI.yml/badge.svg)](https://github.com/dcbickfo/redcache/actions/workflows/CI.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/dcbickfo/redcache.svg)](https://pkg.go.dev/github.com/dcbickfo/redcache)
[![Go Report Card](https://goreportcard.com/badge/github.com/dcbickfo/redcache)](https://goreportcard.com/report/github.com/dcbickfo/redcache)
[![codecov](https://codecov.io/gh/dcbickfo/redcache/branch/main/graph/badge.svg)](https://codecov.io/gh/dcbickfo/redcache)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A typed cache-aside for Redis, built on the [rueidis](https://github.com/redis/rueidis) client. It combines rueidis client-side caching with distributed `SET NX` locking so that, across every process, only one caller populates a missing key while the rest wait on the invalidation push for the populated value. The result is a stampede-resistant cache behind a single generic `Cache[K, V]` interface.

## Features

- **One generic interface** — `Cache[K, V]` for typed keys and values; fakeable in tests.
- **Stampede protection** — in-process leader/follower coordination plus a distributed `SET NX` lock-and-wait, so a single caller runs your loader per key and the rest wait on the invalidation rather than piling onto the origin.
- **Client-side caching** — rueidis client-side cache with Redis invalidation messages cuts round trips and unblocks waiters the moment the value lands.
- **Multi-key batching** — `GetMulti` groups operations by Redis cluster slot and executes the per-slot groups concurrently.
- **Refresh-ahead + XFetch** — optional background refresh of stale-but-valid entries, with XFetch-style probabilistic early expiration to smear reload moments across the keyspace.
- **Write-through priming** — `Set` / `ForceSet` / `Touch` (and their multi variants) populate or extend entries on every subscribed client without a read-through miss.
- **Typed keys *and* values** — pluggable `Codec[V]` and `KeyCodec[K]`; per-key partial failures surface as `*BatchKeyError[K]`.
- **Pluggable metrics** — a `Metrics` interface for hits/misses, lock contention, lock-wait duration, and refresh events.

## Requirements

- Go 1.24+
- Redis 7+

## Installation

```bash
go get github.com/dcbickfo/redcache
```

## Quickstart

`Open` builds a `Conn` that owns a rueidis client; `NewString[V]` derives a `Cache[string, V]` view over it. Pair it with `JSONCodec[V]` to store JSON-encoded values. `Get` returns the cached value, calling your loader only on a miss — and only on one caller per key.

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/redis/rueidis"

    "github.com/dcbickfo/redcache"
)

type User struct {
    ID   string `json:"id"`
    Name string `json:"name"`
}

func main() {
    conn, err := redcache.Open(
        rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
        redcache.WithLockTTL(5*time.Second),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    cache := redcache.NewString[User](conn, redcache.JSONCodec[User]{})

    ctx := context.Background()

    // Get-with-loader: fn runs only on a cache miss, and only on one caller
    // per key across all processes. The loader receives the key being missed.
    u, err := cache.Get(ctx, time.Minute, "u-123",
        func(ctx context.Context, key string) (User, error) {
            // load from the database / upstream service
            return User{ID: "u-123", Name: "alice"}, nil
        },
    )
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("user: %+v", u)
}
```

Absence semantics are the caller's to own — there is no `ErrNotFound` sentinel. Use a `*T`, `sql.Null[T]`, or a domain sentinel inside `V` to cache "not found" and avoid cache penetration.

`GetMulti` follows the same pattern in batch: it returns the cached values and calls the loader once with just the missing keys.

```go
users, err := cache.GetMulti(ctx, time.Minute, []string{"u-1", "u-2", "u-3"},
    func(ctx context.Context, missing []string) (map[string]User, error) {
        out := make(map[string]User, len(missing))
        // load the missing keys in one round trip
        for _, id := range missing {
            out[id] = User{ID: id}
        }
        return out, nil
    },
)
```

`Peek` is a read-only, client-side-cached lookup — no loader, no lock. It returns
`(value, true, nil)` on a cached hit and `(zero, false, nil)` on a miss (or when
the key currently holds a lock value), for warm-cache checks without populating:

```go
u, ok, err := cache.Peek(ctx, time.Minute, "u-123")
// ok == false means not currently cached; Peek never runs your loader.
```

## Typed keys

Derive a view with `New[K, V]` and a `KeyCodec[K]` to key the cache by a domain type. `KeyCodecFunc[K]` adapts a plain function into a `KeyCodec[K]`.

```go
type UserID int64

userIDCodec := redcache.KeyCodecFunc[UserID](func(id UserID) (string, error) {
    return fmt.Sprintf("user:%d", id), nil
})

conn, err := redcache.Open(
    rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
)
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

cache := redcache.New[UserID, User](conn, userIDCodec, redcache.JSONCodec[User]{})

u, err := cache.Get(ctx, time.Minute, UserID(123),
    func(ctx context.Context, id UserID) (User, error) {
        return loadUser(ctx, id)
    },
)
```

The key codec must be deterministic, concurrent-safe, and produce a non-empty key.

## Raw bytes

`NewBytes` derives a zero-copy `Cache[string, []byte]` for opaque payloads — `NewString[[]byte]` preset with `UnsafeBytesCodec`. The decoded slice aliases the cache's borrowed read buffer; do not mutate or retain it past the callback. Copy it out if you need an owned value.

```go
conn, err := redcache.Open(
    rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
)
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

cache := redcache.NewBytes(conn)

b, err := cache.Get(ctx, time.Minute, "blob:42",
    func(ctx context.Context, key string) ([]byte, error) {
        return loadBlob(ctx, key)
    },
)
// b aliases borrowed memory — copy it before retaining or mutating.
```

## Codecs

A `Codec[V]` maps values to and from the stored envelope payload; a `KeyCodec[K]` maps typed keys to the Redis key string. Both must be concurrent-safe.

| Codec | For | Allocation behavior |
|---|---|---|
| `JSONCodec[V]` | any `V` (default) | Encode/Decode via `encoding/json`; returns fresh, caller-owned copies — safe to retain. |
| `StringCodec` | `V = string` | Identity; returns a fresh copy — safe to retain. |
| `UnsafeBytesCodec` | `V = []byte` | Zero-copy. The decoded slice **aliases borrowed library memory**; do not mutate or retain it past the call. |
| `StringKeyCodec` | `K = string` | Identity key codec; enables the `K=string` fast path. |
| `KeyCodecFunc[K]` | any `K` | Adapts `func(K) (string, error)` into a `KeyCodec[K]`. |

The allocation tradeoff is explicit: `JSONCodec` and `StringCodec` copy, so the values they hand back are yours to keep. `UnsafeBytesCodec` skips the copy for throughput, but the `[]byte` it returns borrows the cache's internal buffer — it is only valid for the duration of the call and must not be mutated. Likewise, a slice handed to `Encode` is given to the library and must not be mutated afterward. Choose the copying codecs unless you have measured a reason not to.

Decode failures on read are returned wrapped with `redcache.ErrDecode` (`errors.Is`-checkable). The library does not auto-evict on a decode failure; the caller decides whether to log, `Del`, or retry.

## Options

Construction takes functional options. They are applied in order; later wins.

| Option | Default | Description |
|---|---|---|
| `WithLockTTL(d)` | `10s` | Bounds both how long a Redis lock survives and how long callers wait for one. Values below 100ms are rejected. |
| `WithLogger(l)` | `slog.Default()` | Logger for errors and debug output. Must be concurrent-safe. |
| `WithMetrics(m)` | `NoopMetrics{}` | Observability sink. Runs on the hot path; must be concurrent-safe. |
| `WithLockPrefix(p)` | `__redcache:lock:` | Prefix tagged onto in-Redis lock values so reads recognise a lock as a miss. |
| `WithRefreshLockPrefix(p)` | `__redcache:refresh:` | Prefix for refresh-ahead dedup keys. |
| `WithRefreshAfterFraction(f)` | `0` (disabled) | Enables refresh-ahead. Reads with low remaining TTL may trigger a background refresh. Must be in `[0, 1)`. |
| `WithRefreshBeta(b)` | `0` (XFetch off) | Enables XFetch probabilistic sampling within the refresh window, weighted by recorded compute time. `1.0` matches canonical XFetch. |
| `WithRefreshTimeout(d)` | data `ttl` | Bounds how long a refresh-ahead callback may run. Decoupled from `LockTTL`. |
| `WithRefreshWorkers(n)` | `4` | Refresh worker pool size. |
| `WithRefreshQueueSize(n)` | `64` | Pending-refresh queue capacity; over-full drops silently and the stale value keeps serving. |
| `WithClientBuilder(b)` | `rueidis.NewClient` | Overrides how the internal client is built. A test seam (see Testing). |

## Refresh-ahead and XFetch

Set `WithRefreshAfterFraction` to enable background refreshes. When a `Get`/`GetMulti` returns a value whose remaining TTL has crossed the configured threshold, the stale value is returned immediately and a background worker repopulates the entry. Distributed and local dedup ensure only one refresh runs per key.

```go
conn, err := redcache.Open(
    rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
    redcache.WithLockTTL(5*time.Second),
    redcache.WithRefreshAfterFraction(0.8), // refresh once 80% of TTL has elapsed
    redcache.WithRefreshTimeout(20*time.Second),
    redcache.WithRefreshWorkers(4),
    redcache.WithRefreshQueueSize(64),
)
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

cache := redcache.NewString[User](conn, redcache.JSONCodec[User]{})
```

### XFetch probabilistic refresh

Set `WithRefreshBeta(b)` with `b > 0` to add the [XFetch](https://en.wikipedia.org/wiki/Cache_stampede#Probabilistic_early_expiration) sampling layer on top of the floor. Below the floor, each read fires a refresh with probability proportional to how close the value is to expiry, weighted by how long the previous value took to compute. Slow-to-compute values get more headroom and refresh earlier; cheap values defer until closer to expiry. This smooths refresh load instead of bunching it at the floor crossing. `1.0` matches the canonical XFetch beta (Vattani et al.); for multi-key writes the recorded compute time is divided evenly across the returned values.

### Decoupled refresh budget

By default `WithRefreshTimeout` is the data `ttl` passed to `Get`/`GetMulti`, **not** `LockTTL`. This matters: the refresh callback's compute budget is independent of how long the lock is held. A refresh function that legitimately takes 20s is no longer silently cancelled by a 10s `LockTTL`. The refresh lock itself still uses `LockTTL`, and the back-write that records a slow-but-successful result is decoupled from the timeout so a successful-but-slow refresh keeps its write.

### Value envelope and rollback

Values are stored with a small envelope (`__redcache:v1:<delta_ns>:<payload>`) capturing the compute time XFetch needs. Reads transparently unwrap it, and legacy un-enveloped values are served with `delta=0`, falling back to plain floor-based refresh.

**Rollback warning:** if a deployment writes values under the enveloped format and then rolls back to a pre-envelope release, those older clients will return the raw envelope string as the user value. Flush affected keys (or run a full cache invalidation) before rolling back.

## Comparison to rueidisaside

rueidis already ships [`rueidisaside`](https://github.com/redis/rueidis/tree/main/rueidisaside) and [`rueidislock`](https://github.com/redis/rueidis/tree/main/rueidislock), and the **core lock-and-wait stampede technique is shared** between them and redcache: one caller wins a distributed lock, populates the key, and everyone else waits on the client-side-cache invalidation. If that single-key, single-value-type contract is all you need, `rueidisaside` is an excellent, well-maintained choice and you should reach for it first.

redcache adds, on top of that shared foundation:

- **`GetMulti` with cluster-slot batching** — multi-key reads and writes grouped by Redis cluster slot and executed concurrently per slot.
- **Refresh-ahead + XFetch** — probabilistic early refresh of stale-but-valid entries, decoupling reload latency from request latency.
- **Typed keys, not just values** — a `KeyCodec[K]` maps a domain key type to the Redis key, and multi-key write failures come back as a typed, per-key `*BatchKeyError[K]`.
- **Write-through priming** — `Set` / `ForceSet` / `Touch` (and multi variants) populate or extend entries without a read-through miss.
- **`Conn` + `New`** — open one connection and derive sibling typed caches that share its client, engine, and invalidation stream, so you can cache multiple value types over a single connection.

This is an honest superset for those specific needs, not a claim that `rueidisaside` is deficient — it deliberately keeps a smaller surface.

## Sharing one client across value types

Open a `Conn` once, then derive typed `Cache[K, V]` views over it with `New` (or `NewString` / `NewBytes`). Every view shares the `Conn`'s rueidis client and invalidation stream, with its own key/value types and codecs. Use it to cache several value types over a single Redis connection instead of opening one connection per type. Lifecycle lives on the `Conn`: the views are operations-only (no `Close` / `Client`), so closing happens in exactly one place. Open the `Conn`, derive all the views you need, and close the `Conn` when done.

```go
// One connection backs several typed views.
conn, err := redcache.Open(
    rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
)
if err != nil {
    log.Fatal(err)
}
defer conn.Close() // closes the shared client and all views

sessions := redcache.NewString[Session](conn, redcache.JSONCodec[Session]{})

orders := redcache.New[OrderID, Order](
    conn,
    orderIDCodec,
    redcache.JSONCodec[Order]{},
)
```

`New` (and `NewString` / `NewBytes`) does no I/O — it returns a `Cache[K, V]` with no error, and panics on a nil codec. The constructors are exactly `Open` (which builds the `Conn` and owns the client) plus the `New` / `NewString` / `NewBytes` derive-funcs; close the `Conn` to tear down the underlying client and every view derived from it.

### Long-lived service caching multiple value types

For a service that caches several value types, open one `Conn`, derive a view per type, and inject the views. Because views are operations-only, the injected code can read and write the cache but cannot close the shared connection — lifecycle stays with whoever holds the `Conn`:

```go
conn, err := redcache.Open(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

users := redcache.NewString[User](conn, redcache.JSONCodec[User]{})
sessions := redcache.NewString[Session](conn, redcache.JSONCodec[Session]{})

// Inject `users` and `sessions` (operations-only Cache values) into services.
userSvc := NewUserService(users)
sessionSvc := NewSessionService(sessions)
```

## Metrics

Implement `Metrics`, or embed `NoopMetrics` and override only the methods you care about, to wire counters into Prometheus, OpenTelemetry, or any other backend. Methods run on the hot path and must be concurrent-safe.

High-volume events (`CacheHits`, `CacheMisses`, `LockContended`, `RefreshTriggered`, `RefreshSkipped`, `RefreshDropped`) are aggregated per operation and emitted once with a count rather than once per key. `LockWaitDuration` fires once per resolved wait, and `LoaderDuration` once per foreground origin-loader call (`Get`/`GetMulti` miss, `Set`/`SetMulti` — background refresh excluded). `LoaderErrors(n)` fires when a foreground loader returns an error, with `n` the number of keys it was responsible for. `RedisError(op)` fires when a Redis command fails, tagged with `op` (`"read"`, `"lock"`, `"set"`, `"del"`, `"touch"`). Diagnostic events (`LockLost`, `RefreshError`, `RefreshPanicked`, `InvalidationError`) carry the affected key where applicable.

```go
type myMetrics struct {
    redcache.NoopMetrics
    hits, misses atomic.Int64
}

func (m *myMetrics) CacheHits(n int64)   { m.hits.Add(n) }
func (m *myMetrics) CacheMisses(n int64) { m.misses.Add(n) }

conn, err := redcache.Open(
    rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
    redcache.WithMetrics(&myMetrics{}),
)
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

cache := redcache.NewString[User](conn, redcache.JSONCodec[User]{})
```

### OpenTelemetry

For OpenTelemetry, use the `redcacheotel` subpackage — a drop-in `Metrics` adapter you import as `github.com/dcbickfo/redcache/redcacheotel`:

```go
import "github.com/dcbickfo/redcache/redcacheotel"

m, err := redcacheotel.NewMetrics(meterProvider) // metric.MeterProvider (e.g. your own *sdkmetric.MeterProvider)
if err != nil {
    log.Fatal(err)
}
conn, err := redcache.Open(opt, redcache.WithMetrics(m))
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

cache := redcache.NewString[User](conn, redcache.JSONCodec[User]{})
```

It records counters (hits, misses, lock contention, refresh and error events) and histograms (`lock.wait.duration`, `loader.duration`, in seconds). High-cardinality keys are deliberately not attached as labels; `RedisError`'s bounded `op` is.

Importing redcache's core does **not** pull OpenTelemetry into your binary (verified: zero otel symbols linked) — OTel is only compiled in if you import `redcacheotel`. It does appear in the module graph, since it lives in the main module. OpenTelemetry is pinned at v1.41 because it is the last release supporting Go 1.24; v1.42+ require Go 1.25.

## Testing code that depends on redcache

Depend on the `redcache.Cache[K, V]` interface in your own code, not on a concrete type. Then in unit tests substitute the in-memory fake from the `redcachetest` subpackage — no Redis required:

```go
import "github.com/dcbickfo/redcache/redcachetest"

func TestUserService(t *testing.T) {
    cache := redcachetest.New[string, User]() // satisfies redcache.Cache[string, User]
    svc := NewUserService(cache)
    // exercise svc; the fake calls your loader, stores results, and honours TTL.
}
```

`redcachetest.New[K, V]()` returns a `*Fake[K, V]` that satisfies `redcache.Cache[K, V]`, backed by a map with TTL semantics. It validates the observable single-process contract — your loader runs once per miss, a present unexpired entry is a hit, TTLs expire — which is enough to test loaders, wiring, and call shape. For deterministic expiry tests, construct it with `redcachetest.NewWithClock[K, V](clk)` and advance a `redcachetest.Clock` by hand instead of sleeping.

What the fake does **not** model: distributed single-flight, the `SET NX` lock layer, client-side-cache invalidation pushes, the stored envelope, or refresh-ahead. Those only emerge against real Redis. For fuller fidelity, drive the real `redcache.Cache` against [`rueidis/mock`](https://github.com/redis/rueidis/tree/main/mock) via `WithClientBuilder` (note: miniredis cannot emulate RESP3 client-side invalidation, so it is unsuitable here).

## Stampede mitigation without refresh-ahead

If you can't or don't want to enable refresh-ahead, the lock layer already prevents per-key thundering herd: only one caller per key runs the origin function while peers wait on the cached invalidation. The remaining stampede risk is *cross-key simultaneous expiry* — many keys SET in the same window (deploys, batch imports, cold-start backfill) all expire together.

This library deliberately does not jitter the `ttl` you pass to `Get`/`GetMulti`/`Set`/`ForceSet`: the contract is that you get the TTL you asked for. Jitter expiries at the call site instead, so callers retain control over the policy:

```go
ttl := baseTTL + time.Duration(rand.Int64N(int64(baseTTL/10))) // ±10%
val, err := cache.Get(ctx, ttl, key, fetch)
```

For workloads that already use `WithRefreshAfterFraction` + `WithRefreshBeta`, XFetch handles this naturally — the probabilistic refresh window smears reload moments across the keyspace without changing observable TTLs.

## Local Development

```bash
# Start Redis
docker compose up -d

# Run tests (requires Redis on localhost:6379)
go test -race ./...

# Lint
golangci-lint run

# Benchmarks
go test -bench=. -benchtime=3s ./...
```
