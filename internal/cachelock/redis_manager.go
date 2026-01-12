package cachelock

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/cmdx"
	"github.com/dcbickfo/redcache/internal/lockpool"
	"github.com/dcbickfo/redcache/internal/mapsx"
	"github.com/dcbickfo/redcache/internal/syncx"
)

// Lua scripts for atomic operations.
var (
	// delKeyLua deletes a key only if it matches the expected value.
	delKeyLua = rueidis.NewLuaScript(`if redis.call("GET",KEYS[1]) == ARGV[1] then return redis.call("DEL",KEYS[1]) else return 0 end`)

	// setKeyLua sets a value only if the current value matches the expected lock.
	setKeyLua = rueidis.NewLuaScript(`if redis.call("GET",KEYS[1]) == ARGV[1] then return redis.call("SET",KEYS[1],ARGV[2],"PX",ARGV[3]) else return 0 end`)

	// acquireWriteLockWithBackupScript acquires a lock and returns the previous value.
	// Used for sequential lock acquisition where we need to restore values on rollback.
	//
	// Returns: [success (0 or 1), previous_value or false].
	acquireWriteLockWithBackupScript = rueidis.NewLuaScript(`
		local key = KEYS[1]
		local lock_value = ARGV[1]
		local ttl = ARGV[2]
		local lock_prefix = ARGV[3]

		local current = redis.call("GET", key)

		-- If key is empty, acquire and return false (nothing to restore)
		if current == false then
			redis.call("SET", key, lock_value, "PX", ttl)
			return {1, false}
		end

		-- If current value is a lock, cannot acquire
		if string.sub(current, 1, string.len(lock_prefix)) == lock_prefix then
			return {0, current}
		end

		-- Current value is real data - save it before overwriting
		redis.call("SET", key, lock_value, "PX", ttl)
		return {1, current}
	`)

	// restoreValueOrDeleteScript restores a saved value or deletes the key.
	restoreValueOrDeleteScript = rueidis.NewLuaScript(`
		local key = KEYS[1]
		local expected_lock = ARGV[1]
		local restore_value = ARGV[2]

		-- Only restore if we still hold our lock
		if redis.call("GET", key) == expected_lock then
			if restore_value and restore_value ~= "" then
				redis.call("SET", key, restore_value)
			else
				redis.call("DEL", key)
			end
			return 1
		else
			return 0
		end
	`)
)

// lockEntry tracks a pending lock wait operation.
type lockEntry struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// Logger defines the logging interface.
type Logger interface {
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
}

// RedisManagerConfig contains configuration for RedisManager.
type RedisManagerConfig struct {
	Client     rueidis.Client
	LockTTL    time.Duration
	LockPrefix string
	Logger     Logger
}

// RedisManager implements Manager using Redis for distributed locking.
type RedisManager struct {
	client      rueidis.Client
	locks       syncx.Map[string, *lockEntry]
	lockTTL     time.Duration
	lockPrefix  string
	logger      Logger
	lockValPool *lockpool.Pool
}

// NewRedisManager creates a new RedisManager with the given configuration.
func NewRedisManager(cfg RedisManagerConfig) *RedisManager {
	return &RedisManager{
		client:      cfg.Client,
		lockTTL:     cfg.LockTTL,
		lockPrefix:  cfg.LockPrefix,
		logger:      cfg.Logger,
		lockValPool: lockpool.New(cfg.LockPrefix, 10000),
	}
}

// OnInvalidate should be called when Redis invalidation messages are received.
// This triggers waiting operations to retry.
func (rm *RedisManager) OnInvalidate(messages []rueidis.RedisMessage) {
	for _, m := range messages {
		key, err := m.ToString()
		if err != nil {
			rm.logger.Error("failed to parse invalidation message", "error", err)
			continue
		}
		entry, loaded := rm.locks.LoadAndDelete(key)
		if loaded {
			entry.cancel()
		}
	}
}

// Close cancels all pending wait operations.
func (rm *RedisManager) Close() {
	rm.locks.Range(func(_ string, entry *lockEntry) bool {
		if entry != nil {
			entry.cancel()
		}
		return true
	})
}

// TryAcquire implements Manager.TryAcquire.
func (rm *RedisManager) TryAcquire(ctx context.Context, keys []string, mode LockMode) (*LockResult, error) {
	if len(keys) == 0 {
		return &LockResult{
			Acquired:     make(map[string]string),
			CachedValues: make(map[string]string),
			WaitChans:    make(map[string]<-chan struct{}),
			SavedValues:  make(map[string]string),
		}, nil
	}

	switch mode {
	case LockModeRead:
		return rm.tryAcquireRead(ctx, keys)
	case LockModeWrite:
		return rm.tryAcquireWrite(ctx, keys)
	default:
		return rm.tryAcquireRead(ctx, keys)
	}
}

// tryAcquireRead handles LockModeRead acquisition (for Get operations).
func (rm *RedisManager) tryAcquireRead(ctx context.Context, keys []string) (*LockResult, error) {
	result := &LockResult{
		Acquired:     make(map[string]string),
		CachedValues: make(map[string]string),
		WaitChans:    make(map[string]<-chan struct{}),
		SavedValues:  make(map[string]string),
	}

	// Register all keys first to get wait channels
	for _, key := range keys {
		result.WaitChans[key] = rm.register(key)
	}

	// Try to get cached values
	cachedVals, err := rm.tryGetMulti(ctx, keys)
	if err != nil {
		return nil, err
	}

	// Separate cache hits from keys needing locks
	keysToLock := make([]string, 0, len(keys))
	for _, key := range keys {
		if val, ok := cachedVals[key]; ok {
			result.CachedValues[key] = val
			delete(result.WaitChans, key)
		} else {
			keysToLock = append(keysToLock, key)
		}
	}

	// Try to acquire locks for remaining keys
	if len(keysToLock) > 0 {
		lockVals := rm.tryLockMultiRead(ctx, keysToLock)
		for key, lockVal := range lockVals {
			result.Acquired[key] = lockVal
			delete(result.WaitChans, key)
		}
	}

	return result, nil
}

// tryAcquireWrite handles LockModeWrite acquisition (for Set operations).
func (rm *RedisManager) tryAcquireWrite(ctx context.Context, keys []string) (*LockResult, error) {
	result := &LockResult{
		Acquired:     make(map[string]string),
		CachedValues: make(map[string]string),
		WaitChans:    make(map[string]<-chan struct{}),
		SavedValues:  make(map[string]string),
	}

	// First check for existing locks and subscribe to invalidations
	for _, key := range keys {
		result.WaitChans[key] = rm.register(key)
	}

	// Try to acquire write locks
	acquired, savedValues, failed, err := rm.tryAcquireMultiWrite(ctx, keys)
	if err != nil {
		return nil, err
	}

	result.Acquired = acquired
	result.SavedValues = savedValues

	// Update WaitChans - remove acquired, keep failed
	for key := range acquired {
		delete(result.WaitChans, key)
	}
	for _, key := range failed {
		// Ensure wait channel exists for failed keys
		if _, ok := result.WaitChans[key]; !ok {
			result.WaitChans[key] = rm.register(key)
		}
	}

	return result, nil
}

// Register creates or returns an existing wait channel for a key.
// This is useful for subscribing to CSC invalidation notifications.
func (rm *RedisManager) Register(key string) <-chan struct{} {
	return rm.register(key)
}

// GenerateLockValue creates a unique lock identifier.
func (rm *RedisManager) GenerateLockValue() string {
	return rm.lockValPool.Get()
}

// register creates or returns an existing wait channel for a key.
func (rm *RedisManager) register(key string) <-chan struct{} {
	for {
		// Try to use existing entry
		if ch := rm.tryUseExistingEntry(key); ch != nil {
			return ch
		}

		// Create and store new entry
		ch, shouldRetry := rm.createAndStoreEntry(key)
		if !shouldRetry {
			return ch
		}
		// Retry the loop
	}
}

// tryUseExistingEntry attempts to use an existing lock entry.
// Returns the wait channel if successful, nil if we should create a new entry.
func (rm *RedisManager) tryUseExistingEntry(key string) <-chan struct{} {
	existing, ok := rm.locks.Load(key)
	if !ok {
		return nil
	}

	select {
	case <-existing.ctx.Done():
		// Context is done - try to clean up and return nil to create new
		rm.locks.CompareAndDelete(key, existing)
		return nil
	default:
		return existing.ctx.Done()
	}
}

// createAndStoreEntry creates a new lock entry and attempts to store it.
// Returns (channel, shouldRetry) - if shouldRetry is true, caller should loop.
func (rm *RedisManager) createAndStoreEntry(key string) (<-chan struct{}, bool) {
	buffer := rm.lockTTL / 5
	if buffer < 200*time.Millisecond {
		buffer = 200 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(context.Background(), rm.lockTTL+buffer)

	newEntry := &lockEntry{ctx: ctx, cancel: cancel}
	actual, loaded := rm.locks.LoadOrStore(key, newEntry)

	if !loaded {
		// We successfully stored our entry
		context.AfterFunc(ctx, func() {
			rm.locks.CompareAndDelete(key, newEntry)
		})
		return ctx.Done(), false
	}

	// Another goroutine stored first, cancel ours
	cancel()

	// Check if their entry is still valid
	select {
	case <-actual.ctx.Done():
		// Their entry expired, retry
		rm.locks.CompareAndDelete(key, actual)
		return nil, true
	default:
		return actual.ctx.Done(), false
	}
}

// tryGetMulti tries to get multiple values from cache.
func (rm *RedisManager) tryGetMulti(ctx context.Context, keys []string) (map[string]string, error) {
	multi := make([]rueidis.CacheableTTL, len(keys))
	for i, key := range keys {
		cmd := rm.client.B().Get().Key(key).Cache()
		multi[i] = rueidis.CacheableTTL{
			Cmd: cmd,
			TTL: rm.lockTTL,
		}
	}
	resps := rm.client.DoMultiCache(ctx, multi...)

	res := make(map[string]string, len(keys))
	for i, resp := range resps {
		val, err := resp.ToString()
		if err != nil && rueidis.IsRedisNil(err) {
			continue
		} else if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(val, rm.lockPrefix) {
			res[keys[i]] = val
		}
	}
	return res, nil
}

// tryLockMultiRead attempts to acquire read locks using SET NX.
func (rm *RedisManager) tryLockMultiRead(ctx context.Context, keys []string) map[string]string {
	lockVals := make(map[string]string, len(keys))
	cmds := make(rueidis.Commands, 0, len(keys))

	for _, k := range keys {
		lockVal := rm.lockValPool.Get()
		lockVals[k] = lockVal
		cmds = append(cmds, rm.client.B().Set().Key(k).Value(lockVal).Nx().Get().Px(rm.lockTTL).Build())
	}

	resps := rm.client.DoMulti(ctx, cmds...)

	for i, r := range resps {
		key := keys[i]
		err := r.Error()
		if !rueidis.IsRedisNil(err) {
			delete(lockVals, key)
		}
	}

	return lockVals
}

// tryAcquireMultiWrite attempts to acquire write locks with backup.
func (rm *RedisManager) tryAcquireMultiWrite(
	ctx context.Context,
	keys []string,
) (acquired map[string]string, savedValues map[string]string, failed []string, err error) {
	if len(keys) == 0 {
		return make(map[string]string), make(map[string]string), nil, nil
	}

	acquired = make(map[string]string)
	savedValues = make(map[string]string)
	failed = make([]string, 0)

	stmtsBySlot := rm.groupWriteLocksBySlot(keys)

	for _, stmts := range stmtsBySlot {
		resps := acquireWriteLockWithBackupScript.ExecMulti(ctx, rm.client, stmts.execStmts...)

		for i, resp := range resps {
			key := stmts.keyOrder[i]
			lockVal := stmts.lockVals[i]

			result, respErr := rm.processWriteLockResponse(resp, lockVal)
			if respErr != nil {
				return nil, nil, nil, respErr
			}

			if result.acquired {
				acquired[key] = result.lockValue
				if result.hasSaved {
					savedValues[key] = result.savedValue
				}
			} else {
				failed = append(failed, key)
			}
		}
	}

	return acquired, savedValues, failed, nil
}

type writeLockResult struct {
	acquired   bool
	lockValue  string
	savedValue string
	hasSaved   bool
}

func (rm *RedisManager) processWriteLockResponse(resp rueidis.RedisResult, lockVal string) (writeLockResult, error) {
	result, err := resp.ToArray()
	if err != nil {
		return writeLockResult{}, err
	}

	if len(result) != 2 {
		return writeLockResult{}, nil
	}

	success, err := result[0].AsInt64()
	if err != nil {
		return writeLockResult{}, err
	}

	if success == 1 {
		prevValue, prevErr := result[1].ToString()
		if prevErr == nil && prevValue != "" {
			return writeLockResult{
				acquired:   true,
				lockValue:  lockVal,
				savedValue: prevValue,
				hasSaved:   true,
			}, nil
		}
		return writeLockResult{
			acquired:  true,
			lockValue: lockVal,
		}, nil
	}

	return writeLockResult{acquired: false}, nil
}

type slotLockStatements struct {
	keyOrder  []string
	lockVals  []string
	execStmts []rueidis.LuaExec
}

func (rm *RedisManager) groupWriteLocksBySlot(keys []string) map[uint16]slotLockStatements {
	if len(keys) == 0 {
		return nil
	}

	estimatedSlots := len(keys) / 8
	if estimatedSlots < 1 {
		estimatedSlots = 1
	}
	stmtsBySlot := make(map[uint16]slotLockStatements, estimatedSlots)
	lockTTLStr := strconv.FormatInt(rm.lockTTL.Milliseconds(), 10)

	for _, key := range keys {
		lockVal := rm.lockValPool.Get()
		slot := cmdx.Slot(key)
		stmts := stmtsBySlot[slot]

		if stmts.keyOrder == nil {
			estimatedPerSlot := (len(keys) / estimatedSlots) + 1
			stmts.keyOrder = make([]string, 0, estimatedPerSlot)
			stmts.lockVals = make([]string, 0, estimatedPerSlot)
			stmts.execStmts = make([]rueidis.LuaExec, 0, estimatedPerSlot)
		}

		stmts.keyOrder = append(stmts.keyOrder, key)
		stmts.lockVals = append(stmts.lockVals, lockVal)
		stmts.execStmts = append(stmts.execStmts, rueidis.LuaExec{
			Keys: []string{key},
			Args: []string{lockVal, lockTTLStr, rm.lockPrefix},
		})
		stmtsBySlot[slot] = stmts
	}

	return stmtsBySlot
}

// Commit implements Manager.Commit.
func (rm *RedisManager) Commit(ctx context.Context, lockValues map[string]string, values map[string]string, ttl time.Duration) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}

	stmts := rm.groupSetValuesBySlot(values, lockValues, ttl)
	return rm.executeSetStatements(ctx, stmts)
}

type slotSetStatements struct {
	keyOrder  []string
	execStmts []rueidis.LuaExec
}

func (rm *RedisManager) groupSetValuesBySlot(values map[string]string, lockValues map[string]string, ttl time.Duration) map[uint16]slotSetStatements {
	if len(values) == 0 {
		return nil
	}

	estimatedSlots := len(values) / 8
	if estimatedSlots < 1 {
		estimatedSlots = 1
	}
	stmtsBySlot := make(map[uint16]slotSetStatements, estimatedSlots)
	ttlStr := strconv.FormatInt(ttl.Milliseconds(), 10)

	for key, value := range values {
		lockVal, hasLock := lockValues[key]
		if !hasLock {
			continue
		}

		slot := cmdx.Slot(key)
		stmts := stmtsBySlot[slot]

		if stmts.keyOrder == nil {
			estimatedPerSlot := (len(values) / estimatedSlots) + 1
			stmts.keyOrder = make([]string, 0, estimatedPerSlot)
			stmts.execStmts = make([]rueidis.LuaExec, 0, estimatedPerSlot)
		}

		stmts.keyOrder = append(stmts.keyOrder, key)
		stmts.execStmts = append(stmts.execStmts, rueidis.LuaExec{
			Keys: []string{key},
			Args: []string{lockVal, value, ttlStr},
		})
		stmtsBySlot[slot] = stmts
	}

	return stmtsBySlot
}

func (rm *RedisManager) executeSetStatements(ctx context.Context, stmts map[uint16]slotSetStatements) ([]string, error) {
	totalKeys := 0
	for _, kos := range stmts {
		totalKeys += len(kos.keyOrder)
	}

	keyByStmt := make([][]string, len(stmts))
	var mu sync.Mutex
	var wg sync.WaitGroup

	i := 0
	for _, kos := range stmts {
		keyByStmt[i] = make([]string, 0, len(kos.keyOrder))
		wg.Add(1)
		go rm.executeSlotStatements(ctx, kos, &keyByStmt[i], &mu, &wg)
		i++
	}

	wg.Wait()

	out := make([]string, 0, totalKeys)
	for _, keys := range keyByStmt {
		out = append(out, keys...)
	}
	return out, nil
}

// executeSlotStatements executes Lua statements for a single slot.
func (rm *RedisManager) executeSlotStatements(
	ctx context.Context,
	kos slotSetStatements,
	result *[]string,
	mu *sync.Mutex,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	setResps := setKeyLua.ExecMulti(ctx, rm.client, kos.execStmts...)
	for j, resp := range setResps {
		if rm.isSetSuccessful(resp) {
			mu.Lock()
			*result = append(*result, kos.keyOrder[j])
			mu.Unlock()
		}
	}
}

// isSetSuccessful checks if a set operation succeeded.
func (rm *RedisManager) isSetSuccessful(resp rueidis.RedisResult) bool {
	if err := resp.Error(); err != nil && !rueidis.IsRedisNil(err) {
		return false
	}
	returnValue, err := resp.AsInt64()
	return err != nil || returnValue != 0
}

// Release implements Manager.Release.
func (rm *RedisManager) Release(ctx context.Context, lockValues map[string]string) {
	if len(lockValues) == 0 {
		return
	}

	cleanupCtx := context.WithoutCancel(ctx)
	toCtx, cancel := context.WithTimeout(cleanupCtx, rm.lockTTL)
	defer cancel()

	rm.unlockMulti(toCtx, lockValues)
}

// ReleaseWithRestore implements Manager.ReleaseWithRestore.
func (rm *RedisManager) ReleaseWithRestore(ctx context.Context, lockValues map[string]string, savedValues map[string]string) {
	if len(lockValues) == 0 {
		return
	}

	cleanupCtx := context.WithoutCancel(ctx)
	toCtx, cancel := context.WithTimeout(cleanupCtx, rm.lockTTL)
	defer cancel()

	for key, lockVal := range lockValues {
		savedVal := savedValues[key]
		_ = restoreValueOrDeleteScript.Exec(toCtx, rm.client,
			[]string{key},
			[]string{lockVal, savedVal},
		).Error()
	}
}

func (rm *RedisManager) unlockMulti(ctx context.Context, lockVals map[string]string) {
	if len(lockVals) == 0 {
		return
	}

	delStmts := make(map[uint16][]rueidis.LuaExec)
	for key, lockVal := range lockVals {
		slot := cmdx.Slot(key)
		delStmts[slot] = append(delStmts[slot], rueidis.LuaExec{
			Keys: []string{key},
			Args: []string{lockVal},
		})
	}

	var wg sync.WaitGroup
	for _, stmts := range delStmts {
		wg.Add(1)
		go func(stmts []rueidis.LuaExec) {
			defer wg.Done()
			_ = delKeyLua.ExecMulti(ctx, rm.client, stmts...)
		}(stmts)
	}
	wg.Wait()
}

// WaitForRelease implements Manager.WaitForRelease.
func (rm *RedisManager) WaitForRelease(ctx context.Context, waitChans map[string]<-chan struct{}) error {
	if len(waitChans) == 0 {
		return nil
	}

	channels := mapsx.Values(waitChans)
	return syncx.WaitForAll(ctx, channels)
}
