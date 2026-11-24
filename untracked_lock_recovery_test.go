package redcache_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

const recoveryLockPrefix = "recovery-lock:"
const recoveryHolderValue = recoveryLockPrefix + "remote-holder"

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

func (c *untrackedRecoveryClient) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	get := cmd.Commands()[0] == "GET"
	result := c.Client.Do(ctx, cmd)
	if get {
		c.recordRead(result)
	}
	return result
}

func (c *untrackedRecoveryClient) DoMulti(ctx context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	get := len(cmds) == 1 && cmds[0].Commands()[0] == "GET"
	results := c.Client.DoMulti(ctx, cmds...)
	if get {
		c.recordRead(results[0])
	}
	return results
}

func (c *untrackedRecoveryClient) recordRead(result rueidis.RedisResult) {
	// During ASK, some servers abort the cached read and redcache retries it
	// as a plain GET. Count that reply as the read.
	if errors.Is(result.Error(), rueidis.ErrDoCacheAborted) {
		return
	}
	if c.reads.Add(1) == 2 {
		c.firstPoll <- result
	}
}

func observeUntrackedRecoveryReads(firstPoll chan rueidis.RedisResult, disableCache bool) redcache.Option {
	return redcache.WithClientBuilder(func(opt rueidis.ClientOption) (rueidis.Client, error) {
		opt.DisableCache = disableCache
		client, err := rueidis.NewClient(opt)
		if err != nil {
			return nil, err
		}
		return &untrackedRecoveryClient{Client: client, firstPoll: firstPoll}, nil
	})
}

func TestLockRecovery_UntrackedWaiter(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"Get", "GetMulti"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			firstPoll := make(chan rueidis.RedisResult, 1)
			waiter, _ := openLockRecoveryCache(t, redcache.WithLockPrefix(recoveryLockPrefix), observeUntrackedRecoveryReads(firstPoll, true))
			holderCache, holder := openLockRecoveryCache(t)
			key := "remote-lock:" + uuid.New().String()
			testUntrackedWaiter(t, waiter, holderCache, holder.Client(), firstPoll, key, name == "GetMulti")
		})
	}
}

// Slot migration tests run sequentially because they change the cluster topology.
func TestLockRecovery_ASKWaiter(t *testing.T) {
	for _, name := range []string{"Get", "GetMulti"} {
		t.Run(name, func(t *testing.T) {
			firstPoll := make(chan rueidis.RedisResult, 1)
			_, waiter, _, keys := askRefillTest(t, 1, redcache.WithLockTTL(recoveryLockTTL),
				redcache.WithLockPrefix(recoveryLockPrefix), observeUntrackedRecoveryReads(firstPoll, false))
			holderCache, holder := makeClusterClient(t)
			testUntrackedWaiter(t, waiter, holderCache, holder.Client(), firstPoll, keys[0], name == "GetMulti")
		})
	}
}

func testUntrackedWaiter(t *testing.T, waiter, holderCache *redcache.Cache, holder rueidis.Client,
	firstPoll <-chan rueidis.RedisResult, key string, multi bool,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	cmd := holder.B().Set().Key(key).Value(recoveryHolderValue).Nx().Px(recoveryLockTTL).Build()
	require.NoError(t, holder.Do(ctx, cmd).Error())
	var loads atomic.Int32
	done := startRecoveryGet(ctx, waiter, []string{key}, multi, func(context.Context, []string) (map[string]string, error) {
		loads.Add(1)
		return map[string]string{key: reloadedValue}, nil
	})
	polled := awaitLockRecoveryTest(t, firstPoll)
	locked, err := polled.ToString()
	require.NoError(t, err)
	require.Equal(t, recoveryHolderValue, locked)
	require.EqualValues(t, -1, polled.CachePXAT())
	require.Zero(t, loads.Load(), "waiter must respect the remote lock")
	require.NoError(t, holderCache.Del(ctx, key))

	result := awaitLockRecoveryTest(t, done)
	require.NoError(t, result.err)
	require.NoError(t, ctx.Err())
	require.Equal(t, reloadedValue, result.values[key])
	require.EqualValues(t, 1, loads.Load())
}
