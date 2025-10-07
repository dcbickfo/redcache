# redcache

redcache provides a cache aside implementation for Redis. It's based on the rueidis library and uses client side caching to reduce the number of round trips to the Redis server.


### Example
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
