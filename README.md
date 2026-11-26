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
- **Multi-key batching** — `GetMulti` and the other multi-key calls send each phase as one pipeline; in cluster mode rueidis splits it per node and runs the nodes in parallel.
- **Refresh-ahead + XFetch** — optional background refresh of stale-but-valid entries, with XFetch-style probabilistic early expiration to smear reload moments across the keyspace.
- **Write-through priming** — `Set` / `ForceSet` / `Touch` (and their multi variants) populate or extend entries on every subscribed client without a read-through miss.
- **Typed keys and values** — pluggable `Codec` and `KeyCodec`; per-key partial failures surface as `*BatchKeyError[K]`.
- **Pluggable metrics** — a `Metrics` interface for hits/misses, lock contention, lock-wait duration, and refresh events.

## Requirements

- Go 1.27+
- Redis 7+ with RESP3 and client-side caching (tracking) enabled

RESP3 client-side caching lets redcache wake waiters through Redis invalidation
pushes. Without tracking, waiters rely on jittered polling to see a published
value or a deletion. A deletion prompts another lock acquisition attempt.
Polling increases latency and Redis read traffic under contention.

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

v0.4 requires Go 1.27 and moves both type parameters from cache
construction to the operations (Go 1.27 generic methods). `Cache` is no longer
generic:

```go
// v0.3.x
users  := redcache.NewString[User](conn, redcache.JSONCodec[User]{})
orders := redcache.New[OrderID, Order](conn, orderIDCodec, redcache.JSONCodec[Order]{})

// v0.4 — one cache, K and V inferred per call
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

// v0.4
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

`GetMulti` follows the same pattern in batch: it returns the cached values and calls the loader once with just the missing keys. The loader must return a value for every key it is given; an omitted key fails the call with `ErrMissingValue`.

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

## Write-through with Set

`Set` takes a per-call write lock on the key, runs your callback, and writes the
callback's value under that lock; `SetMulti` does the same for a batch. Use it
to update the source of truth and the cache together:

```go
err := cache.Set(ctx, time.Minute, "u-123",
    func(ctx context.Context, key string) (User, error) {
        return db.UpdateUser(ctx, key) // write to the database, return the new value
    },
)
```

**Restore-on-error precondition.** If the callback returns an error, `Set`
puts the prior cached value back with the remaining TTL captured when `Set`
took the key (not reduced by the callback's run time); a key with no prior
value is deleted. If another writer replaced the lock in the meantime, that
writer's value is kept, and if Redis is unreachable the lock stays until
`LockTTL` expires. Restoring is only correct when a failed callback did not
change the source of truth. If the callback can commit and then return an error
(a timeout waiting for the acknowledgement of a write that did land), the restored value is stale.
Make such callbacks return success once the write is committed, or follow a
failed `Set` with `Del` so the next `Get` reloads. A callback that panics is
rolled back the same way before the panic continues, so a caller that recovers
(an HTTP server, for example) does not leave the key locked.

**`ErrLockLost`.** The write lock is held for `LockTTL` and is not extended
while your callback runs. `Set` returns `ErrLockLost` (test with
`errors.Is`; `SetMulti` wraps it in a `*BatchKeyError` per key) when the lock
was removed or expired before the value was written: a concurrent `ForceSet` or
`Del` took the key, or the callback outlasted `LockTTL`. The callback's value
was not written and the key holds whatever the other writer left, so the
source of truth and the cache may disagree. Do not blindly retry a callback with
side effects; reconcile with `Del` (the next `Get` reloads from the source) and
size `LockTTL` above your slowest write callback.

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
| `WithLockTTL(d)` | `10s` | Bounds how long a lock survives without its holder (the holder extends it every `LockTTL/3` while its loader runs, so loaders may take longer than `LockTTL`) and how long callers wait for one. Values below 100ms are rejected. |
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

- **`GetMulti` with cluster-aware batching** — multi-key reads and writes pipelined, split per cluster node by rueidis.
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

Reads through the same `Conn` see its acknowledged writes. For a recently
written key, redcache bypasses the local cache and reads directly from Redis
until the key's mark ends. A mark also expires one second after the write
completes. The table shows the cost for reads started after a fill or explicit
write completes.

| Connection | After a `Get` or `GetMulti` loader fill | After `Set`, `SetMulti`, `ForceSet*`, or `Del*` |
| --- | --- | --- |
| Standalone, default tracking | One direct Redis GET, then local caching resumes. | One direct Redis GET, including a missing-key reply after `Del` or `DelMulti`, then local caching resumes. |
| Cluster | Direct Redis reads until an invalidation for the key arrives or one second passes. Measured at the full second. | The same rule. Measured at the full second after `Set*`, and after `ForceSet*` on a key this Conn is not tracking. About one round trip after `ForceSet*` on a key it is tracking. `Del*` takes the `ForceSet*` code path, unmeasured. |
| Sentinel | Direct Redis GETs until invalidation or the one-second deadline. | Direct Redis GETs until invalidation or the one-second deadline. |
| Custom `ClientBuilder` | Direct Redis GETs until invalidation or the one-second deadline. | Direct Redis GETs until invalidation or the one-second deadline. |
| Explicit `DisableCache` or RESP2 | Ordinary Redis reads with no marks. | Ordinary Redis reads with no marks. |
| `NOLOOP` or `SendToReplicas` | No read-your-writes guarantee; see below. | No read-your-writes guarantee; see below. |

**Standalone with default tracking** promises read-your-writes on a `Conn`
while the mark is live. The read that ends the mark is sent on the connection
that receives invalidations. Redis queues any pending invalidation for the key
ahead of that read's reply, and the client evicts the local entry before it
delivers the reply. An invalidation never ends the mark on this connection,
because it can belong to an earlier write, by this `Conn` or another client, and
the invalidation for a `Conn`'s own write follows its reply. Concurrent first
reads can each issue a GET before the first reply ends the mark. Pool routing,
an error reply, or a reconnect leaves the deadline in effect. If a write's
invalidation arrives after the one-second deadline, the mark has expired, and
every local hit returns the older entry until that invalidation evicts it.

**Cluster** is supported. A mark ends when an invalidation push for the key
arrives or at the one-second deadline, whichever comes first. A read-through
fill and every `Set*` take a lock by writing the key. That lock write uses up
this connection's tracking of the key before the mark begins, so the value
write sends this connection no invalidation. A `ForceSet*` on a key this
connection is not tracking sends none either. In practice the key is read
directly from the server for the full second, one network round trip per read,
and then served from the local cache. A `ForceSet*` on a key this connection is
tracking, such as after a tracked read just before it, ends the mark in about
one round trip. `Del*` sets and ends marks by the same code path as
`ForceSet*`, and was not measured separately. Standalone with default tracking
ends the window after one read.

Measured on 2026-11-25 on a Redis 7.2 cluster, 500 fills per case, on a loaded
shared host, on a build whose cluster path matches this release (PR #45). The
baseline is main at 4cc10985, which has no marks.

| Measurement | Median time to first local hit | How the mark ended |
| --- | --- | --- |
| Read-through fill, this version | 1.000 s | Deadline, 500 of 500 (483 at a read, 17 by sweep) |
| Read-through fill, baseline (no marks) | 123 µs | No mark |
| `ForceSet` on a key this Conn is not tracking, this version | 1.000 s | Deadline, 500 of 500 (482 at a read, 18 by sweep) |
| `Set` with a loader, this version | 1.000 s | Deadline 496; invalidation 4, from the lock write's push after the mark began |
| Tracked read, then `ForceSet`, this version | 119 µs | Invalidation, 500 of 500 (497 in flight, 3 after the reply) |

Arming tracking before the write is being evaluated; its safety is not yet proven.

**`NOLOOP` and `SendToReplicas` give no read-your-writes guarantee**, on
standalone or cluster. With `NOLOOP`, Redis sends this `Conn` no invalidation
for its own writes, so its marks end only at the one-second deadline. A key
this `Conn` had cached locally before a `Set*`, `ForceSet*`, or `Del*` keeps
the pre-write value in the local cache, and a read more than one second after
the write returns that value until the local entry's TTL expires or another
client's write invalidates it. With `SendToReplicas`, the direct GET a mark
causes can go to a replica that has not yet applied the write, so even a read
inside the mark can return the pre-write value.

**Other connections without the barrier read** keep a residual: cluster,
Sentinel, a custom `ClientBuilder`, and standalone settings that change
tracking or routing (`BCAST`, `OPTOUT`, `DisableAutoPipelining`,
`ConnLifetime`, redirects). Their marks end when an invalidation arrives or the
deadline passes. A read that began before a write
can refill an older value after that write's invalidation. That invalidation
can then end a later write's mark, and the older value stays visible until the
later invalidation arrives. This takes a delayed read and two close writes,
from concurrent writers or from one writer's back-to-back calls. Cluster is
supported with this residual. A second `Conn`, even in the same process, is
eventually consistent and sees the write when its own invalidation arrives.

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

High-volume events (`CacheHits`, `CacheMisses`, `LockContended`, `RefreshTriggered`, `RefreshSkipped`, `RefreshDropped`) are aggregated per operation and emitted once with a count rather than once per key. `LockWaitDuration` fires once per resolved wait, and `LoaderDuration` once per foreground origin-loader call (`Get`/`GetMulti` miss, `Set`/`SetMulti` — background refresh excluded). `LoaderErrors(n)` fires when a foreground loader returns an error, with `n` the number of keys it was responsible for. Diagnostic events (`LockLost`, `RefreshError`, `RefreshPanicked`, `InvalidationError`) carry the affected key where applicable.

`RedisError(op)` uses `read`, `lock`, `set`, `del`, and `touch` for command
failures, and `read_fallback` for an attempted uncached retry after a
clustered cached read aborted. It emits one event per uncached retry attempt,
whether for one key or a batch. A polling `Get` or `GetMulti` can make more
than one attempt. The count includes retries that fail, return Redis nil,
or are canceled before sending. Missing `PTTL` permission can trigger an
abort. A sustained rate alone cannot identify a server-side `EXEC` abort
because rueidis also reports `ErrDoCacheAborted` when a connection closes.

Metrics implementations and dashboards that enumerate operation labels need
to accept `read_fallback`. Unfiltered `redis.errors` alerts include successful
retries. Exclude `op="read_fallback"` from alerts that count only command
failures.

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
    // exercise svc; the real codecs run, a miss calls your loader,
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
