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
| `LockPrefix` | `string` | `"__redcache:lock:"` | Prefix for distributed lock values. Choose a prefix unlikely to conflict with your data keys. |

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
