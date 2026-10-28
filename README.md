# redcache

[![Build](https://github.com/dcbickfo/redcache/actions/workflows/CI.yml/badge.svg)](https://github.com/dcbickfo/redcache/actions/workflows/CI.yml)
[![CodeQL](https://github.com/dcbickfo/redcache/actions/workflows/codeql.yml/badge.svg)](https://github.com/dcbickfo/redcache/actions/workflows/codeql.yml)
[![Govulncheck](https://github.com/dcbickfo/redcache/actions/workflows/govulncheck.yml/badge.svg)](https://github.com/dcbickfo/redcache/actions/workflows/govulncheck.yml)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/dcbickfo/redcache/badge)](https://scorecard.dev/viewer/?uri=github.com/dcbickfo/redcache)
[![Go Reference](https://pkg.go.dev/badge/github.com/dcbickfo/redcache.svg)](https://pkg.go.dev/github.com/dcbickfo/redcache)
[![Go Version](https://img.shields.io/github/go-mod/go-version/dcbickfo/redcache)](go.mod)
[![Latest Release](https://img.shields.io/github/v/release/dcbickfo/redcache?sort=semver)](https://github.com/dcbickfo/redcache/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/dcbickfo/redcache)](https://goreportcard.com/report/github.com/dcbickfo/redcache)
[![codecov](https://codecov.io/gh/dcbickfo/redcache/branch/main/graph/badge.svg)](https://codecov.io/gh/dcbickfo/redcache)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A typed cache-aside for Redis, built on the [rueidis](https://github.com/redis/rueidis) client. It combines rueidis client-side caching with distributed `SET NX` locking so that, across every process, only one caller populates a missing key while the rest wait on the invalidation push. A single `Cache` serves every key and value type: each operation infers `K` and `V` from its arguments.

## Features

- **Per-operation key and value types** — one JSON-backed `Cache` stores different Go types under keys of different Go types; `K` and `V` are inferred per call.
- **Stampede protection** — in-process leader/follower coordination plus a distributed `SET NX` lock-and-wait, so a single caller runs your loader per key and the rest wait on the invalidation rather than piling onto the origin.
- **Client-side caching** — rueidis client-side cache with Redis invalidation messages cuts round trips and unblocks waiters the moment the value lands.
- **Multi-key batching** — `GetMulti` groups operations by Redis cluster slot and executes the per-slot groups concurrently.
- **Refresh-ahead + XFetch** — optional background refresh of stale-but-valid entries, with XFetch-style probabilistic early expiration to smear reload moments across the keyspace.
- **Write-through priming** — `Set` / `ForceSet` / `Touch` (and their multi variants) populate or extend entries on every subscribed client without a read-through miss.
- **Typed keys and values** — pluggable `Codec` and `KeyCodec`; per-key partial failures surface as `*BatchKeyError[K]`.
- **Pluggable metrics** — a `Metrics` interface for hits/misses, lock contention, lock-wait duration, and refresh events.

## Requirements

- Go 1.27+
- Redis 7+ with RESP3 and client-side caching (tracking) enabled

RESP3 client-side caching is load-bearing, not optional: redcache wakes waiters
through Redis invalidation pushes. Without RESP3 (or with tracking disabled)
there are no pushes — waiters fall back to jittered polling until the lock TTL,
raising tail latency and Redis read traffic under contention.

## Installation

```bash
go get github.com/dcbickfo/redcache
```

## Runnable examples

Standalone examples live under [`examples/`](examples/). They compile with the
module and can be run directly against a local Redis:

```bash
go run ./examples/string-cache
go run ./examples/typed-cache
go run ./examples/metrics
```

Set `REDIS_ADDR` to point them at a non-default Redis address.

## Migrating from v0.3.x

The next release requires Go 1.27 and moves both type parameters from cache
construction to the operations (Go 1.27 generic methods). `Cache` is no longer
generic:

```go
// v0.3.x
users  := redcache.NewString[User](conn, redcache.JSONCodec[User]{})
orders := redcache.New[OrderID, Order](conn, orderIDCodec, redcache.JSONCodec[Order]{})

// next release — one cache, K and V inferred per call
cache := redcache.New(conn, redcache.JSONCodec{})
user, err  := cache.Get(ctx, ttl, "u-1", loadUser)      // K=string, V=User
order, err := cache.Get(ctx, ttl, OrderID(7), loadOrder) // K=OrderID, V=Order
profile, ok, err := cache.Peek[string, Profile](ctx, ttl, profileKey)
```

`New(conn, valCodec)` uses `StringKeyCodec`, which accepts `string`, any type
with underlying type `string`, and `encoding.TextMarshaler`. For other key types
use `NewKeyed(conn, keyCodec, valCodec)`; `KeyCodecFunc[K]` still adapts a
typed function. `Codec[V]` and `KeyCodec[K]` become `Codec` and `KeyCodec`
(their methods take `any`); `JSONCodec[V]{}` becomes `JSONCodec{}`.

There is no interface form anymore — generic methods cannot satisfy Go
interfaces — so code that depended on the `Cache[K, V]` interface now takes
`*redcache.Cache`. For tests, `redcache.OpenMemory()` returns a `Conn` backed by
an in-process map; `redcachetest` is removed:

```go
// v0.3.x
func NewUserService(c redcache.Cache[string, User]) *UserService
svc := NewUserService(redcachetest.New[string, User]())

// next release
func NewUserService(c *redcache.Cache) *UserService
svc := NewUserService(redcache.New(redcache.OpenMemory(), redcache.JSONCodec{}))
```

`JSONCodec` keeps `encoding/json` (v1) semantics, so stored payloads are
unchanged. `JSONV2Codec` is new and opts in to `encoding/json/v2` defaults.

## Quickstart

`Open` builds a `Conn` that owns a rueidis client. `New` constructs a `Cache`
with one value codec (string-ish keys by default). `Get` infers its key type
from the key and its value type from the loader, which runs only on a miss and
only on one caller per key.

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

    cache := redcache.New(conn, redcache.JSONCodec{})

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
u, ok, err := cache.Peek[string, User](ctx, time.Minute, "u-123")
// ok == false means not currently cached; Peek never runs your loader.
```

## Typed keys

Keys whose underlying type is `string` (e.g. `type UserID string`) and types
implementing `encoding.TextMarshaler` work with `New` as-is. For anything else,
construct the cache with `NewKeyed`, a `KeyCodec`, and a value `Codec`;
`KeyCodecFunc[K]` adapts a plain function into a `KeyCodec` that accepts only `K`.

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

cache := redcache.NewKeyed(conn, userIDCodec, redcache.JSONCodec{})

u, err := cache.Get(ctx, time.Minute, UserID(123),
    func(ctx context.Context, id UserID) (User, error) {
        return loadUser(ctx, id)
    },
)
```

The key codec must be deterministic, concurrent-safe, produce a non-empty key,
and encode distinct logical keys to distinct Redis keys. Multi-key operations
reject typed-key collisions before touching Redis.

## Raw bytes

`NewBytes` constructs a zero-copy `Cache` for opaque `[]byte` payloads. The
decoded slice aliases the cache's borrowed read buffer; do not mutate or retain
it past the callback. Copy it out if you need an owned value.

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

A `Codec` maps operation values to and from the stored envelope payload; a
`KeyCodec` maps keys to Redis key strings. Both must be concurrent-safe.
`Decode` receives a pointer to the operation's value type, like
`json.Unmarshal`. A codec that does not support that type returns an error.

| Codec | For | Allocation behavior |
|---|---|---|
| `JSONCodec` | any JSON-supported value | Encode/Decode via `encoding/json` (v1 semantics); decoded values are caller-owned. |
| `JSONV2Codec` | any JSON-supported value | `encoding/json/v2`: nil slices/maps encode as `[]`/`{}`, case-sensitive field matching, duplicate names rejected. Changes the stored form of nil containers relative to `JSONCodec`. |
| `StringCodec` | `V = string` | Identity for immutable strings; safe to retain. |
| `UnsafeBytesCodec` | `V = []byte` | Zero-copy. The decoded slice **aliases borrowed library memory**; do not mutate or retain it past the call. |
| `StringKeyCodec` | `string`, `~string`, `encoding.TextMarshaler` | Default key codec. String-underlying keys take the zero-copy multi-key fast path. |
| `KeyCodecFunc[K]` | exactly `K` | Adapts `func(K) (string, error)` into a `KeyCodec`; other key types are rejected. |

`JSONCodec` returns owned decoded values, and `StringCodec` returns immutable
strings that are safe to retain. `UnsafeBytesCodec` skips the copy, so its
decoded `[]byte` borrows the cache's internal buffer and must not be mutated or
retained. A slice returned by any codec's `Encode` is also owned by the library
after the call.

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
| `WithClientBuilder(b)` | `rueidis.NewClient` | Overrides how the internal client is built, including for tests. |

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

cache := redcache.New(conn, redcache.JSONCodec{})
```

### XFetch probabilistic refresh

Set `WithRefreshBeta(b)` with `b > 0` to add the [XFetch](https://en.wikipedia.org/wiki/Cache_stampede#Probabilistic_early_expiration) sampling layer on top of the floor. Below the floor, each read fires a refresh with probability proportional to how close the value is to expiry, weighted by how long the previous value took to compute. Slow-to-compute values get more headroom and refresh earlier; cheap values defer until closer to expiry. This smooths refresh load instead of bunching it at the floor crossing. `1.0` matches the canonical XFetch beta (Vattani et al.); for multi-key writes the recorded compute time is divided evenly across the returned values. That is an approximation: if one key in a batch dominates loader time and per-key XFetch precision matters, split that workload into separate `Get` calls or smaller `GetMulti` groups.

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
- **Typed keys, not just values** — a `KeyCodec` maps a domain key type to the Redis key, and multi-key write failures come back as a typed, per-key `*BatchKeyError[K]`.
- **Write-through priming** — `Set` / `ForceSet` / `Touch` (and multi variants) populate or extend entries without a read-through miss. This is the hardest piece for `rueidisaside` to absorb rather than just a missing feature: `rueidisaside` claims only *missing* keys, with a single per-client placeholder and no value backup, whereas `Set` overwrites an already-live value under a per-call lock token and restores the prior value if the write fails. In-place locking, per-call lock identity, and a backup/restore path are structural to redcache's model, not a flag on the read-miss-only one.
- **Per-operation value types** — a JSON-backed cache can read and write
  unrelated Go types without constructing a cache per type.

This is an honest superset for those specific needs, not a claim that `rueidisaside` is deficient — it deliberately keeps a smaller surface.

## Sharing one client across caches

Open one `Conn`, then construct one cache per codec pair the service needs.
Every cache shares the Conn's client and invalidation stream, and a single
JSON-backed cache serves any key and value types on each operation.

```go
conn, err := redcache.Open(
    rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
)
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

values := redcache.New(conn, redcache.JSONCodec{})
raw := redcache.NewBytes(conn)

session, err := values.Get(ctx, ttl, sessionKey, loadSession)  // K=string,  V=Session
order, err   := values.Get(ctx, ttl, OrderID("o-9"), loadOrder) // K=OrderID, V=Order
payload, err := raw.Get(ctx, ttl, "blob", loadBlob)             // V=[]byte
```

`New`, `NewKeyed`, and `NewBytes` do no I/O and panic on a nil codec. Close the `Conn` to tear down
the shared client and every cache built over it.

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

cache := redcache.New(conn, redcache.JSONCodec{})
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

cache := redcache.New(conn, redcache.JSONCodec{})
```

It records counters (hits, misses, lock contention, refresh and error events) and histograms (`lock.wait.duration`, `loader.duration`, in seconds). High-cardinality keys are deliberately not attached as labels; `RedisError`'s bounded `op` is.

Importing redcache's core does **not** pull OpenTelemetry into your binary (verified: zero otel symbols linked) — OTel is only compiled in if you import `redcacheotel`. It does appear in the module graph, since it lives in the main module.

## Testing

`Cache` is concrete because generic methods cannot satisfy a Go interface, so
code under test takes `*redcache.Cache`. To unit-test without Redis, build the
cache over `redcache.OpenMemory()` — a `Conn` backed by an in-process map:

```go
func TestUserService(t *testing.T) {
    cache := redcache.New(redcache.OpenMemory(), redcache.JSONCodec{})
    svc := NewUserService(cache)
    // exercise svc; the real codecs run, a miss calls your loader once,
    // a present unexpired entry is a hit, and TTLs expire.
}
```

For deterministic expiry pass `redcache.WithMemoryClock(func() time.Time)` and
move the returned time forward instead of sleeping. The memory Conn does not
model distributed single-flight, invalidation pushes, refresh-ahead, or
metrics; for those, construct the real cache with `WithClientBuilder` and
[`rueidis/mock`](https://github.com/redis/rueidis/tree/main/mock), or run against
Redis. Miniredis cannot emulate RESP3 client-side invalidation.

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
# Install the pinned Go and golangci-lint versions via asdf
make setup

# Start Redis
docker compose up -d

# Run tests (requires Redis on localhost:6379)
make test-race

# Check coverage against the current baseline
make cover-check

# Lint
make lint

# Benchmarks
make bench
```
