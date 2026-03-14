package redcache_test

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

func ExampleCacheAside_Get() {
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{
			InitAddress: []string{"127.0.0.1:6379"},
		},
		redcache.CacheAsideOption{
			LockTTL: 5 * time.Second,
		},
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
	// Output: hello
}

func ExampleCacheAside_GetMulti() {
	client, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{
			InitAddress: []string{"127.0.0.1:6379"},
		},
		redcache.CacheAsideOption{
			LockTTL: 5 * time.Second,
		},
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
	// Output: 2
}
