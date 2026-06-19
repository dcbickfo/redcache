// Package main demonstrates typed keys and JSON-encoded values.
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

type userID string

type user struct {
	ID    userID `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

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

	keyCodec := redcache.KeyCodecFunc[userID](func(id userID) (string, error) {
		return "redcache:examples:typed:" + runID + ":user:" + string(id), nil
	})
	cache := redcache.New[userID, user](conn, keyCodec, redcache.JSONCodec[user]{})

	loadUsers := func(ids []userID) map[userID]user {
		users := make(map[userID]user, len(ids))
		for _, id := range ids {
			users[id] = user{
				ID:    id,
				Name:  "User " + string(id),
				Email: fmt.Sprintf("user-%s@example.com", id),
			}
		}
		return users
	}

	one, err := cache.Get(ctx, time.Minute, userID("42"), func(_ context.Context, id userID) (user, error) {
		users := loadUsers([]userID{id})
		return users[id], nil
	})
	if err != nil {
		log.Fatalf("get typed user: %v", err)
	}
	fmt.Printf("single typed user: %+v\n", one)

	many, err := cache.GetMulti(ctx, time.Minute, []userID{"101", "102"}, func(_ context.Context, ids []userID) (map[userID]user, error) {
		return loadUsers(ids), nil
	})
	if err != nil {
		log.Fatalf("get typed users: %v", err)
	}
	fmt.Printf("typed batch returned %d users\n", len(many))
}

func redisAddr() string {
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:6379"
}
