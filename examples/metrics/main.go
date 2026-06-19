// Package main demonstrates wiring a custom Metrics implementation.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

type counters struct {
	redcache.NoopMetrics
	hits   atomic.Int64
	misses atomic.Int64
}

func (c *counters) CacheHits(n int64) {
	c.hits.Add(n)
}

func (c *counters) CacheMisses(n int64) {
	c.misses.Add(n)
}

func main() {
	ctx := context.Background()
	metrics := &counters{}
	runID := fmt.Sprintf("%d", time.Now().UnixNano())

	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{redisAddr()}},
		redcache.WithMetrics(metrics),
	)
	if err != nil {
		log.Fatalf("open redcache: %v", err)
	}
	defer conn.Close()

	cache := redcache.NewString[string](conn, redcache.StringCodec{})
	key := "redcache:examples:metrics:" + runID

	load := func(_ context.Context, _ string) (string, error) {
		return "value from origin", nil
	}
	if _, err := cache.Get(ctx, time.Minute, key, load); err != nil {
		log.Fatalf("get miss: %v", err)
	}
	if _, err := cache.Get(ctx, time.Minute, key, load); err != nil {
		log.Fatalf("get hit: %v", err)
	}

	if err := cache.Set(ctx, time.Minute, key+":primed", func(_ context.Context, _ string) (string, error) {
		return "primed value", nil
	}); err != nil {
		log.Fatalf("set primed value: %v", err)
	}

	fmt.Printf("cache hits=%d misses=%d\n", metrics.hits.Load(), metrics.misses.Load())
}

func redisAddr() string {
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:6379"
}
