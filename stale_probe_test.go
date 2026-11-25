package redcache_test

// PROBE, DO NOT MERGE. Loops ForceSetMulti then GetMulti and logs any stale
// read with the evidence needed to name the mechanism. Always passes.

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

func probeOwner(ctx context.Context, c rueidis.Client, key string) (int64, string) {
	slot, err := c.Do(ctx, c.B().ClusterKeyslot().Key(key).Build()).AsInt64()
	if err != nil {
		return -1, ""
	}
	slots, err := c.Do(ctx, c.B().ClusterSlots().Build()).ToArray()
	if err != nil {
		return slot, ""
	}
	for _, s := range slots {
		r, err := s.ToArray()
		if err != nil || len(r) < 3 {
			continue
		}
		lo, _ := r[0].AsInt64()
		hi, _ := r[1].AsInt64()
		if slot >= lo && slot <= hi {
			node, _ := r[2].ToArray()
			host, _ := node[0].ToString()
			port, _ := node[1].AsInt64()
			return slot, host + ":" + strconv.FormatInt(port, 10)
		}
	}
	return slot, ""
}

func probeRun(t *testing.T, mode string, cache *redcache.Cache, conn *redcache.Conn, cluster bool) {
	iters := 20000
	if v, err := strconv.Atoi(os.Getenv("PROBE_ITERS")); err == nil {
		iters = v
	}
	const workers = 4
	noLoad := func(context.Context, []string) (map[string]string, error) {
		return nil, errors.New("loader must not run")
	}
	var stale, reads atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		keys := make([]string, 64)
		for i := range keys {
			keys[i] = "probe:" + uuid.New().String()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			c := conn.Client()
			for i := 0; i < iters; i++ {
				gen := "g" + strconv.Itoa(i)
				if err := cache.ForceSetMulti(ctx, time.Minute, valuesFor(keys, gen)); err != nil {
					t.Logf("PROBE_ERR mode=%s set: %v", mode, err)
					return
				}
				setDone := time.Now()
				got, err := cache.GetMulti(ctx, time.Minute, keys, noLoad)
				if err != nil {
					t.Logf("PROBE_ERR mode=%s get: %v", mode, err)
					return
				}
				reads.Add(int64(len(keys)))
				for _, k := range keys {
					want := "val:" + gen + ":" + k
					if got[k] == want {
						continue
					}
					n := stale.Add(1)
					if n > 20 {
						continue
					}
					slot, node := int64(-1), ""
					if cluster {
						slot, node = probeOwner(ctx, c, k)
					}
					cached := c.DoCache(ctx, c.B().Get().Key(k).Cache(), time.Minute)
					plain, _ := c.Do(ctx, c.B().Get().Key(k).Build()).ToString()
					pttl, _ := c.Do(ctx, c.B().Pttl().Key(k).Build()).AsInt64()
					// How long until the cached read becomes fresh?
					start := time.Now()
					tries := 0
					for ; tries < 2000; tries++ {
						g2, _ := cache.GetMulti(ctx, time.Minute, []string{k}, noLoad)
						if g2[k] == want {
							break
						}
						time.Sleep(50 * time.Microsecond)
					}
					t.Logf("PROBE_STALE mode=%s iter=%d key=%s slot=%d node=%s sinceSet=%s got=%q want=%q recheckCacheHit=%v recheckPTTL=%d serverPlain=%q healedAfter=%s tries=%d",
						mode, i, k, slot, node, time.Since(setDone), got[k], want, cached.IsCacheHit(), cached.CachePTTL(), plain, time.Since(start), tries)
					_ = pttl
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("PROBE_SUMMARY mode=%s image=%s iters=%d workers=%d reads=%d stale=%d gomaxprocs_env=%q",
		mode, os.Getenv("REDCACHE_SERVER_IMAGE"), iters, workers, reads.Load(), stale.Load(), os.Getenv("GOMAXPROCS"))
}

func TestProbe_StaleReadAfterForceSetMulti(t *testing.T) {
	t.Run("cluster", func(t *testing.T) {
		if os.Getenv("REDCACHE_CLUSTER_ADDRS") == "" {
			t.Skip("probe: no cluster")
		}
		cache, conn := makeClusterClient(t)
		probeRun(t, "cluster", cache, conn, true)
	})
	t.Run("standalone", func(t *testing.T) {
		cache, conn := makeClient(t, strings.Split("127.0.0.1:6379", ","))
		probeRun(t, "standalone", cache, conn, false)
	})
}
