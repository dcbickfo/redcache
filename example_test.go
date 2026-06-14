package redcache_test

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

func ExampleNewString() {
	client, err := redcache.NewString[string](
		rueidis.ClientOption{
			InitAddress: []string{"127.0.0.1:6379"},
		},
		redcache.StringCodec{},
		redcache.WithLockTTL(5*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer client.Client().Close()

	val, err := client.Get(context.Background(), time.Minute, "example:get", func(ctx context.Context, key string) (string, error) {
		// Called only on cache miss — fetch from your data source.
		return "hello", nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(val)
	// This example dials Redis, so it omits an Output: directive (which would
	// make `go test` run it). It is compiled to keep the snippet honest.
}

func ExampleNewString_getMulti() {
	client, err := redcache.NewString[string](
		rueidis.ClientOption{
			InitAddress: []string{"127.0.0.1:6379"},
		},
		redcache.StringCodec{},
		redcache.WithLockTTL(5*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer client.Client().Close()

	keys := []string{"example:multi:a", "example:multi:b"}
	vals, err := client.GetMulti(context.Background(), time.Minute, keys, func(ctx context.Context, keys []string) (map[string]string, error) {
		// Called only for keys not in cache — fetch from your data source.
		result := make(map[string]string, len(keys))
		for _, k := range keys {
			result[k] = "value-for-" + k
		}
		return result, nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(len(vals))
	// Redis-dependent; omits Output: so `go test` does not run it.
}
