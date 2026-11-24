package redcache_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

type untrackedRecoveryClient struct {
	rueidis.Client
	reads     atomic.Int32
	firstPoll chan rueidis.RedisResult
}

func (c *untrackedRecoveryClient) DoCache(ctx context.Context, cmd rueidis.Cacheable, ttl time.Duration) rueidis.RedisResult {
	result := c.Client.DoCache(ctx, cmd, ttl)
	c.recordRead(result)
	return result
}

func (c *untrackedRecoveryClient) DoMultiCache(ctx context.Context, cmds ...rueidis.CacheableTTL) []rueidis.RedisResult {
	results := c.Client.DoMultiCache(ctx, cmds...)
	c.recordRead(results[0])
	return results
}

func (c *untrackedRecoveryClient) recordRead(result rueidis.RedisResult) {
	if c.reads.Add(1) == 2 {
		c.firstPoll <- result
	}
}

func TestLockRecovery_UntrackedWaiter(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"Get", "GetMulti"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const lockPrefix = "recovery-lock:"
			const holderValue = lockPrefix + "remote-holder"
			firstPoll := make(chan rueidis.RedisResult, 1)
			waiter, _ := openLockRecoveryCache(t, redcache.WithLockPrefix(lockPrefix),
				redcache.WithClientBuilder(func(opt rueidis.ClientOption) (rueidis.Client, error) {
					opt.DisableCache = true
					client, err := rueidis.NewClient(opt)
					if err != nil {
						return nil, err
					}
					return &untrackedRecoveryClient{Client: client, firstPoll: firstPoll}, nil
				}))
			holderCache, holder := openLockRecoveryCache(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			key := "remote-lock:" + uuid.New().String()
			client := holder.Client()
			cmd := client.B().Set().Key(key).Value(holderValue).Nx().Px(recoveryLockTTL).Build()
			require.NoError(t, client.Do(ctx, cmd).Error())
			var loads atomic.Int32
			start := time.Now()
			done := startRecoveryGet(ctx, waiter, []string{key}, name == "GetMulti", func(context.Context, []string) (map[string]string, error) {
				loads.Add(1)
				return map[string]string{key: reloadedValue}, nil
			})
			polled := awaitLockRecoveryTest(t, firstPoll)
			locked, err := polled.ToString()
			require.NoError(t, err)
			require.Equal(t, holderValue, locked)
			require.EqualValues(t, -1, polled.CachePXAT())
			require.Zero(t, loads.Load(), "waiter must respect the remote lock")
			require.NoError(t, holderCache.Del(ctx, key))

			result := awaitLockRecoveryTest(t, done)
			require.NoError(t, result.err)
			require.Less(t, time.Since(start), 3*time.Second)
			require.Equal(t, reloadedValue, result.values[key])
			require.EqualValues(t, 1, loads.Load())
		})
	}
}
