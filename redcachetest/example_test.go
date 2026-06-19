package redcachetest_test

import (
	"context"
	"fmt"
	"time"

	"github.com/dcbickfo/redcache"
	"github.com/dcbickfo/redcache/redcachetest"
)

// ExampleFake shows the in-memory Fake standing in for a real redcache.Cache in
// a unit test: no Redis, no network. The loader runs once on the miss, then the
// second Get is a hit and skips it. Because the Fake needs no Redis, this
// example carries an Output: directive and is run by `go test`.
func ExampleFake() {
	// A function under test that caches lookups through any redcache.Cache.
	greet := func(c redcache.Cache[string, string], calls *int) (string, error) {
		return c.Get(context.Background(), time.Minute, "user:1",
			func(ctx context.Context, key string) (string, error) {
				*calls++
				return "alice", nil
			},
		)
	}

	cache := redcachetest.New[string, string]()
	var calls int

	v1, _ := greet(cache, &calls)
	v2, _ := greet(cache, &calls) // served from the Fake, loader not re-run

	fmt.Println(v1, v2, "loader calls:", calls)
	// Output: alice alice loader calls: 1
}
