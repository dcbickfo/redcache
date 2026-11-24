package redcache

import "github.com/redis/rueidis"

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
