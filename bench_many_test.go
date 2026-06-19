package redcache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

func BenchmarkCache_Get_ManyKeys(b *testing.B) {
	conn, err := redcache.Open(rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	cache := redcache.NewString[string](conn, redcache.StringCodec{})
	ctx := context.Background()

	// Prime 100,000 keys
	b.Log("Priming keys...")
	for i := 0; i < 100000; i++ {
		if err := cache.ForceSet(ctx, time.Minute, fmt.Sprintf("k:%d", i), "v"); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	runBenchParallel(b, func(pb *testing.PB) error {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("k:%d", i%100000)
			if _, err := cache.Get(ctx, time.Minute, key, benchUnreachableFn); err != nil {
				return err
			}
			i++
		}
		return nil
	})
}
