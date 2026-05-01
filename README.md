# redcache

[![CI](https://github.com/dcbickfo/redcache/actions/workflows/CI.yml/badge.svg)](https://github.com/dcbickfo/redcache/actions/workflows/CI.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/dcbickfo/redcache.svg)](https://pkg.go.dev/github.com/dcbickfo/redcache)
[![Go Report Card](https://goreportcard.com/badge/github.com/dcbickfo/redcache)](https://goreportcard.com/report/github.com/dcbickfo/redcache)

A cache-aside implementation for Redis, built on the [rueidis](https://github.com/redis/rueidis) client.

## Features

- **Cache-aside pattern** — automatic cache population with `Get` and `GetMulti`.
- **Distributed locking** — `SET NX GET PX` with UUIDv7 prevents thundering herd.
- **Client-side caching** — rueidis client-side cache with Redis invalidation messages reduces round trips.
- **Cluster support** — multi-key operations are grouped by slot for efficient batching.
- **Lock ownership verification** — Lua scripts atomically verify lock ownership before SET/DEL.

## Requirements

- Go 1.23+
- Redis 7+

## Installation

```bash
go get github.com/dcbickfo/redcache
```

## Usage

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
    if err := run(); err != nil {
        log.Fatal(err)
    }
}

func run() error {
    var db *sql.DB
    // initialize db
    client, err := redcache.NewRedCacheAside(
        rueidis.ClientOption{
            InitAddress: []string{"127.0.0.1:6379"},
        },
        redcache.CacheAsideOption{
            LockTTL:   time.Second * 1,
        },
    )
    if err != nil {
        return err
    }

    repo := Repository{
        client: client,
        db:     &db,
    }

    val, err := repo.GetByID(context.Background(), "key")
    if err != nil {
        return err
    }

    vals, err := repo.GetByIDs(context.Background(), []string{"key1", "key2"})
    if err != nil {
        return err
    }
    _, _ = val, vals
    return nil
}

type Repository struct {
    client  *redcache.CacheAside
    db      *sql.DB
}

func (r Repository) GetByID(ctx context.Context, key string) (string, error) {
    val, err := r.client.Get(ctx, time.Minute, key, func(ctx context.Context, key string) (val string, err error) {
        if err = r.db.QueryRowContext(ctx, "SELECT val FROM mytab WHERE id = ?", key).Scan(&val); err == sql.ErrNoRows {
            val = "NULL" // cache null to avoid penetration.
            err = nil     // clear err in case of sql.ErrNoRows.
        }
        return
    })
    if err != nil {
       return "", err
    } else if val == "NULL" {
        val = ""
        err = sql.ErrNoRows
    }
    return val, err
}

func (r Repository) GetByIDs(ctx context.Context, keys []string) (map[string]string, error) {
    val, err := r.client.GetMulti(ctx, time.Minute, keys, func(ctx context.Context, keys []string) (val map[string]string, err error) {
        val = make(map[string]string)
        rows, err := r.db.QueryContext(ctx, "SELECT id, val FROM mytab WHERE id IN (?)", keys)
        if err != nil {
            return nil, err
        }
        defer rows.Close()
        for rows.Next() {
            var id, rowVal string
            if err = rows.Scan(&id, &rowVal); err != nil {
                return nil, err
            }
            val[id] = rowVal
        }
        if len(val) != len(keys) {
            for _, k := range keys {
                if _, ok := val[k]; !ok {
                    val[k] = "NULL" // cache null to avoid penetration.
                }
            }
        }
        return val, nil
    })
    if err != nil {
        return nil, err
    }
    // handle any NULL vals if desired
    // ...

    return val, nil
}
```

## Configuration

`CacheAsideOption` controls the behavior of the cache-aside client:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `LockTTL` | `time.Duration` | `10s` | Maximum time a lock can be held. Also the timeout for waiting on lost invalidation messages. Must be at least 100ms. |
| `ClientBuilder` | `func(rueidis.ClientOption) (rueidis.Client, error)` | `nil` | Custom builder for the underlying rueidis client. Uses `rueidis.NewClient` when nil. |
| `Logger` | `Logger` | `slog.Default()` | Logger for errors and debug information. Must be safe for concurrent use. |
| `Metrics` | `Metrics` | `NoopMetrics{}` | Receives observability events. Must be concurrent-safe — methods run on the hot path. |
| `LockPrefix` | `string` | `"__redcache:lock:"` | Prefix for distributed lock values. Choose a prefix unlikely to conflict with your data keys. |
| `RefreshLockPrefix` | `string` | `"__redcache:refresh:"` | Prefix for distributed refresh-ahead locks. |
| `RefreshAfterFraction` | `float64` | `0` (disabled) | Fraction of TTL after which a refresh-ahead is triggered. Must be in `[0, 1)`. |
| `RefreshWorkers` | `int` | `4` (when refresh enabled) | Background workers processing refresh jobs. |
| `RefreshQueueSize` | `int` | `64` (when refresh enabled) | Capacity of the refresh job queue. Jobs are silently dropped when full; the stale value continues to serve. |

## Refresh-Ahead

Set `RefreshAfterFraction` to enable background refreshes. When a `Get`/`GetMulti` returns a value whose TTL has crossed the configured threshold, the stale value is returned immediately and a background worker repopulates the entry. Distributed and local dedup ensure only one refresh runs per key.

```go
client, err := redcache.NewRedCacheAside(
    rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
    redcache.CacheAsideOption{
        LockTTL:              5 * time.Second,
        RefreshAfterFraction: 0.8, // refresh once 80% of TTL has elapsed
        RefreshWorkers:       4,
        RefreshQueueSize:     64,
    },
)
```

## Metrics

Implement `Metrics` (or embed `NoopMetrics` and override the methods you care about) to wire counters into Prometheus, OpenTelemetry, or any other backend. Methods are called on the hot path and must be concurrent-safe.

High-volume events (`CacheHits`, `CacheMisses`, `LockContended`, `RefreshTriggered`, `RefreshSkipped`, `RefreshDropped`) are aggregated per operation and emitted once with a count rather than once per key. Diagnostic events (`LockLost`, `RefreshError`, `RefreshPanicked`) carry the affected key.

```go
type myMetrics struct {
    redcache.NoopMetrics
    hits, misses atomic.Int64
}

func (m *myMetrics) CacheHits(n int64)   { m.hits.Add(n) }
func (m *myMetrics) CacheMisses(n int64) { m.misses.Add(n) }

client, err := redcache.NewRedCacheAside(
    rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
    redcache.CacheAsideOption{
        LockTTL: 5 * time.Second,
        Metrics: &myMetrics{},
    },
)
```

## Local Development

```bash
# Start Redis
docker compose up -d

# Run tests
go test -race ./...

# Lint
golangci-lint run

# Benchmarks
go test -bench=. -benchtime=3s ./...
```
