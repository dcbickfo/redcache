// Package main demonstrates a migration-friendly string-key/string-value cache.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

func main() {
	ctx := context.Background()
	runID := fmt.Sprintf("%d", time.Now().UnixNano())

	conn, err := redcache.Open(
		rueidis.ClientOption{InitAddress: []string{redisAddr()}},
		redcache.WithLockTTL(5*time.Second),
	)
	if err != nil {
		log.Fatalf("open redcache: %v", err)
	}
	defer conn.Close()

	cache := redcache.NewString[string](conn, redcache.StringCodec{})
	origin := map[string]string{
		"user:1": "Ada Lovelace",
		"user:2": "Grace Hopper",
		"user:3": "Katherine Johnson",
	}

	key := "redcache:examples:string:" + runID + ":user:1"
	loaderCalls := 0
	loadOne := func(_ context.Context, _ string) (string, error) {
		loaderCalls++
		return origin["user:1"], nil
	}

	name, err := cache.Get(ctx, time.Minute, key, loadOne)
	if err != nil {
		log.Fatalf("get first value: %v", err)
	}
	fmt.Printf("first get: %s (loader calls: %d)\n", name, loaderCalls)

	name, err = cache.Get(ctx, time.Minute, key, loadOne)
	if err != nil {
		log.Fatalf("get cached value: %v", err)
	}
	fmt.Printf("second get: %s (loader calls: %d)\n", name, loaderCalls)

	keys := []string{
		"redcache:examples:string:" + runID + ":user:1",
		"redcache:examples:string:" + runID + ":user:2",
		"redcache:examples:string:" + runID + ":user:3",
	}
	users, err := cache.GetMulti(ctx, time.Minute, keys, func(_ context.Context, missing []string) (map[string]string, error) {
		values := make(map[string]string, len(missing))
		for _, k := range missing {
			userID := k[len("redcache:examples:string:"+runID+":"):]
			values[k] = origin[userID]
		}
		return values, nil
	})
	if err != nil {
		log.Fatalf("get multi: %v", err)
	}
	fmt.Printf("get multi returned %d users\n", len(users))
}

func redisAddr() string {
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:6379"
}
