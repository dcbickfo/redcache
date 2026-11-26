// This file stays in package redcache: it exposes unexported functions, types and scripts to the external tests as thin aliases and accessors.
package redcache

import (
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

func WrapClientForTest(conn *Conn, wrap func(rueidis.Client) rueidis.Client) {
	engine := conn.core.(*cacheAside)
	engine.client = wrap(engine.client)
}

type WriteMarkCountsForTest struct {
	Invalidated int64
	Barrier     int64
	Expired     int64
	AtCap       int64
	Reset       int64
	Live        int64
}

func WriteMarkStatsForTest(conn *Conn) WriteMarkCountsForTest {
	return writeMarkCounts(&conn.core.(*cacheAside).writeMarks)
}

func writeMarkCounts(m *writeMarks) WriteMarkCountsForTest {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return WriteMarkCountsForTest{
		Invalidated: m.invalidated, Barrier: m.barrier, Expired: m.expired,
		AtCap: m.atCap, Reset: m.resetCount, Live: int64(len(m.marks)),
	}
}

type WriteMarksForTest struct{ state writeMarks }

func (m *WriteMarksForTest) Begin(keys []string, now time.Time, cap int) func(time.Time, bool) {
	ticket := m.state.begin(keys, now, cap)
	return func(end time.Time, success bool) {
		m.state.finish(keys, ticket, end, func(int) bool { return success })
	}
}

func (m *WriteMarksForTest) Bypass(key string, now time.Time) bool {
	return m.state.bypassAt(key, now)
}

func (m *WriteMarksForTest) Invalidate(key string)          { m.state.invalidate(key) }
func (m *WriteMarksForTest) Reset(now time.Time)            { m.state.reset(now) }
func (m *WriteMarksForTest) Counts() WriteMarkCountsForTest { return writeMarkCounts(&m.state) }

func NewWriteMarksForTest(pipeLimit int) *WriteMarksForTest {
	return &WriteMarksForTest{state: writeMarks{pipeLimit: pipeLimit}}
}

func (m *WriteMarksForTest) ReadBarrier(key string, commands int, now time.Time) func() {
	barriers := m.state.readBarriers([]string{key}, commands, now)
	return func() {
		if len(barriers) != 0 {
			m.state.finishBarrier(key, barriers[0])
		}
	}
}

func (m *WriteMarksForTest) Redial() { m.state.wire.Add(1) }

// DisableReadBarrierForTest makes a Conn retire marks by invalidation, as a
// cluster or custom-builder Conn does. Call it before issuing traffic.
func DisableReadBarrierForTest(conn *Conn) {
	marks := &conn.core.(*cacheAside).writeMarks
	marks.mu.Lock()
	defer marks.mu.Unlock()
	marks.pipeLimit = 0
}

// ReadBarrierForOptionsForTest reports whether a standalone Conn opened with
// opt would get the read barrier, for options a test server cannot accept.
func ReadBarrierForOptionsForTest(opt rueidis.ClientOption) bool {
	rca := &cacheAside{trackBypasses: canTrackBypasses(opt), poolCutoff: rueidis.DefaultBlockingPipeline}
	return rca.readBarrierPipeLimit(opt, config{}) != 0
}

func HasWaitEntryForTest(conn *Conn, key string) bool {
	_, ok := conn.core.(*cacheAside).locks.Load(key)
	return ok
}
