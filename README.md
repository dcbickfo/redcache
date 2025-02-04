# redcache

redcache provides a cache aside implementation for Redis. It's based on the rueidis library and uses client side caching to reduce the number of round trips to the Redis server.


### Example
```go
package main

import (
    "context"
    "database/sql"
    "github.com/google/go-cmp/cmp/internal/value"
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
    var db sql.DB
    // initialize db
    client, err := redcache.NewRedCacheAside(
        rueidis.ClientOption{
            InitAddress: addr,
        },
        redcache.CacheAsideOption{
            ServerTTL: time.Second * 10,
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
	
    val, err := Repository.GetByID(context.Background(), "key")
    if err != nil {
    return err
    }
	
    vals, err := Repository.GetByIDs(context.Background(), map[string]string{"key1": "val1", "key2": "val2"})
    if err != nil {
        return err
    }
}

type Repository struct {
    client  redcache.CacheAside
    db      *sql.DB
}

func (r Repository) GetByID(ctx context.Context, key string) (string, error) {
    val, err := r.client.Get(ctx, time.Minute, key, func(ctx context.Context, key string) (val string, err error) {
        if err = db.QueryRowContext(ctx, "SELECT val FROM mytab WHERE id = ?", key).Scan(&val); err == sql.ErrNoRows {
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
    // ...
}

func (r Repository) GetByIDs(ctx context.Context, key []string) (map[string]string, error) {
    val, err := r.client.GetMulti(ctx, time.Minute, key, func(ctx context.Context, key []string) (val map[string]string, err error) {
        rows := db.QueryContext(ctx, "SELECT id, val FROM mytab WHERE id = ?", key)
        defer rows.Close()
        for rows.Next() {
            var id, rowVal string
            if err = rows.Scan(&id, &rowVal); err != nil {
                return
            }
            val[id] = rowVal
        }
        if len(val) != len(key) {
            for _, k := range key {
                if _, ok := val[k]; !ok {
                    val[k] = "NULL" // cache null to avoid penetration.
                }
            }
        }
        return
    })
    if err != nil {
        return nil, err
    } 
    // handle any NULL vals if desired
    // ...

    return val, nil
}

```