// This file stays in package redcache: it exposes unexported functions, types and scripts to the external tests as thin aliases and accessors.
package redcache

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"
)

type EngineBatchError = batchError

var (
	WrapEnvelope   = wrapEnvelope
	UnwrapEnvelope = unwrapEnvelope

	NewEngineBatchError       = newBatchError
	NewIntBatchKeyError       = newBatchKeyError[int]
	NewStringBatchKeyError    = newBatchKeyError[string]
	MergeForceSetStringResult = mergeForceSetResult[string]

	TouchScriptExecMulti           = touchScript.ExecMulti
	RestoreValueOrDeleteScriptExec = restoreValueOrDeleteScript.Exec
	DelScriptRueidisExecMulti      = delKeyLua.Lua.ExecMulti
	DelScriptExecMulti             = delKeyLua.ExecMulti
)

func DefaultedMetrics(clientOption rueidis.ClientOption) (Metrics, error) {
	var cfg config
	err := cfg.applyDefaults(clientOption)
	return cfg.metrics, err
}

func ConfiguredOptions(opts ...Option) (Logger, string, string, func(rueidis.ClientOption) (rueidis.Client, error)) {
	cfg := newConfig(opts...)
	return cfg.logger, cfg.lockPrefix, cfg.refreshLockPrefix, cfg.clientBuilder
}

func ApplyRefreshDefaultsForTest(fraction float64, workers, queueSize int) (int, int, error) {
	cfg := config{refreshAfterFraction: fraction, refreshWorkers: workers, refreshQueueSize: queueSize}
	err := cfg.applyRefreshDefaults()
	return cfg.refreshWorkers, cfg.refreshQueueSize, err
}

// Share the cached-read fixtures between the two test packages.
type ReadTestClient = readTestClient

var (
	RedisResultForTest        = redisResult
	RedisMessageForTest       = redisMessage
	RedisStringMessageForTest = redisStringMessage
)

type ReadPathCountsForTest struct {
	CacheHits atomic.Int64
	Bypasses  atomic.Int64
}

func (c *ReadPathCountsForTest) Option() Option {
	return WithClientBuilder(func(opt rueidis.ClientOption) (rueidis.Client, error) {
		client, err := rueidis.NewClient(opt)
		if err != nil {
			return nil, err
		}
		return &countReadPaths{Client: client, counts: c}, nil
	})
}

type countReadPaths struct {
	rueidis.Client
	counts *ReadPathCountsForTest
}

func (c *countReadPaths) DoCache(ctx context.Context, cmd rueidis.Cacheable, ttl time.Duration) rueidis.RedisResult {
	result := c.Client.DoCache(ctx, cmd, ttl)
	if result.IsCacheHit() {
		c.counts.CacheHits.Add(1)
	}
	return result
}

func (c *countReadPaths) DoMultiCache(ctx context.Context, cmds ...rueidis.CacheableTTL) []rueidis.RedisResult {
	results := c.Client.DoMultiCache(ctx, cmds...)
	var hits int64
	for _, result := range results {
		if result.IsCacheHit() {
			hits++
		}
	}
	c.counts.CacheHits.Add(hits)
	return results
}

func (c *countReadPaths) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	if cmd.Commands()[0] == "GET" {
		c.counts.Bypasses.Add(1)
	}
	return c.Client.Do(ctx, cmd)
}

func (c *countReadPaths) DoMulti(ctx context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	var reads int64
	for _, cmd := range cmds {
		if cmd.Commands()[0] == "GET" {
			reads++
		}
	}
	if reads != 0 {
		c.counts.Bypasses.Add(reads)
	}
	return c.Client.DoMulti(ctx, cmds...)
}

type WriteMarkCountsForTest struct {
	Invalidated int64
	Expired     int64
	AtCap       int64
	Live        int64
}

func WriteMarkStatsForTest(_ *Conn) WriteMarkCountsForTest {
	return WriteMarkCountsForTest{}
}
