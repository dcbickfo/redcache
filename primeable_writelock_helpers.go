package redcache

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/cmdx"
)

// savedValue captures the previous value of a key for rollback after a Set
// callback failure. The pttl is preserved so a restored value retains its
// original TTL rather than becoming persistent.
type savedValue struct {
	val     string
	pttl    int64 // -1 = persistent, positive = ms remaining
	present bool  // distinguishes "no prior value" from "prior value was empty"
}

// tryAcquireWriteLock attempts to acquire a write lock on a single key, returning the
// previous value (with its PTTL) for rollback. Returns (acquired, saved, error).
func (pca *PrimeableCacheAside) tryAcquireWriteLock(ctx context.Context, key, lockVal, lockTTLMs string) (bool, savedValue, error) {
	resp := acquireWriteLockWithBackupScript.Exec(ctx, pca.client, []string{key}, []string{lockVal, lockTTLMs, pca.lockPrefix})
	arr, err := resp.ToArray()
	if err != nil {
		return false, savedValue{}, fmt.Errorf("write lock for key %q: %w", key, err)
	}
	if len(arr) != 3 {
		return false, savedValue{}, fmt.Errorf("write lock for key %q: malformed response (len=%d)", key, len(arr))
	}
	success, ierr := arr[0].AsInt64()
	if ierr != nil {
		pca.logger.Error("unexpected non-integer in lock-acquire response", "key", key, "error", ierr)
		return false, savedValue{}, fmt.Errorf("write lock for key %q: parse success: %w", key, ierr)
	}
	if success == 0 {
		return false, savedValue{}, nil
	}
	saved := parseBackup(pca.logger, key, arr[1], arr[2])
	return true, saved, nil
}

// parseBackup converts the (value, pttl) pair from the acquire script into a
// savedValue. A redis-nil value means "no prior value" (present=false). A real
// parse error is logged and surfaces as present=false — Set will then DEL on
// rollback rather than restoring an indeterminate value, matching the Lua's
// "could not capture backup" intent.
func parseBackup(logger Logger, key string, valMsg, pttlMsg rueidis.RedisMessage) savedValue {
	backupVal, bErr := valMsg.ToString()
	if bErr != nil {
		if !rueidis.IsRedisNil(bErr) {
			logger.Error("unexpected backup value in lock-acquire response", "key", key, "error", bErr)
		}
		return savedValue{}
	}
	pttl, pErr := pttlMsg.AsInt64()
	if pErr != nil {
		// PTTL is unparseable — we'd otherwise restore the value as persistent
		// (pttl=0 → SET without PX in restore script). Better to drop the
		// backup so rollback DELs the key than risk a permanent stale entry.
		logger.Error("unexpected non-integer pttl in lock-acquire response", "key", key, "error", pErr)
		return savedValue{}
	}
	return savedValue{val: backupVal, pttl: pttl, present: true}
}

// acquireMultiWriteLocks acquires write locks on all keys in sorted order with rollback.
// Returns lockValues map and savedValues map (for rollback of previous real values).
//
// savedValues is lazy-allocated — first-time keys with no prior value never
// trigger the alloc, keeping the cold-cache happy path lighter. Callers must
// treat savedValues as nil-safe (read/delete on nil map are no-ops; see
// restoreMultiValues, rollbackAfterFirstFailure).
func (pca *PrimeableCacheAside) acquireMultiWriteLocks(
	ctx context.Context,
	keys []string,
) (lockValues map[string]string, savedValues map[string]savedValue, err error) {
	sortedP := stringPool.Get(len(keys))
	defer stringPool.Put(sortedP)
	sorted := *sortedP
	copy(sorted, keys)
	sort.Strings(sorted)

	lockValues = make(map[string]string, len(sorted))
	remaining := sorted

	for len(remaining) > 0 {
		firstFailed, err := pca.tryAcquireRemaining(ctx, remaining, lockValues, &savedValues)
		if err != nil {
			pca.restoreMultiValues(ctx, lockValues, savedValues)
			return nil, nil, err
		}

		if firstFailed == "" {
			break
		}

		if err := pca.waitForFailedKey(ctx, firstFailed, lockValues, savedValues); err != nil {
			return nil, nil, err
		}

		remaining = pca.computeRemaining(sorted, lockValues)
	}

	return lockValues, savedValues, nil
}

// tryAcquireRemaining generates lock values and batch-acquires locks for remaining keys.
// On partial failure, rolls back locks after the first failed key and returns firstFailed.
//
// savedValues is a pointer so the caller's map can be lazy-allocated on first
// prior-value capture — keeps the no-prior-value happy path alloc-free.
func (pca *PrimeableCacheAside) tryAcquireRemaining(
	ctx context.Context,
	remaining []string,
	lockValues map[string]string,
	savedValues *map[string]savedValue,
) (firstFailed string, err error) {
	// remaining is filtered by computeRemaining to exclude already-locked keys,
	// so every entry needs a freshly-generated lock value. Skipping the prior
	// batchLocks map saves an alloc and a redundant rebuild loop.
	entries := make([]lockAcquireEntry, len(remaining))
	for i, key := range remaining {
		entries[i] = lockAcquireEntry{key: key, lockVal: pca.lockPool.Generate()}
	}

	acquired, backups, firstFailed, err := pca.batchAcquireWithBackup(ctx, entries, pca.lockTTLMs)
	if err != nil {
		return "", err
	}

	for key, lockVal := range acquired {
		lockValues[key] = lockVal
		if backup, ok := backups[key]; ok {
			if *savedValues == nil {
				*savedValues = make(map[string]savedValue)
			}
			(*savedValues)[key] = backup
		}
	}

	if firstFailed != "" {
		pca.rollbackAfterFirstFailure(ctx, remaining, firstFailed, lockValues, *savedValues, acquired)
		pca.touchMultiLocks(ctx, lockValues)
	}

	return firstFailed, nil
}

// waitForFailedKey registers, subscribes, and waits for a failed key's lock to release.
//
// The DoCache response is inspected: if the key already shows a non-lock value
// (or is absent), the lock is gone and the caller can retry immediately rather
// than blocking on a wait channel for a missed invalidation that already fired.
func (pca *PrimeableCacheAside) waitForFailedKey(
	ctx context.Context,
	firstFailed string,
	lockValues map[string]string,
	savedValues map[string]savedValue,
) error {
	waitChan, _ := pca.register(firstFailed)
	resp := pca.client.DoCache(ctx, pca.client.B().Get().Key(firstFailed).Cache(), pca.lockTTL)
	val, rerr := resp.ToString()
	if rueidis.IsRedisNil(rerr) {
		return nil
	}
	if rerr != nil {
		// Real Redis error — fail fast rather than blocking on a wait channel
		// for the full lockTTL while the cluster is unhealthy.
		pca.restoreMultiValues(ctx, lockValues, savedValues)
		return fmt.Errorf("read key %q: %w", firstFailed, rerr)
	}
	if !strings.HasPrefix(val, pca.lockPrefix) {
		return nil
	}

	select {
	case <-waitChan:
		return nil
	case <-ctx.Done():
		pca.restoreMultiValues(ctx, lockValues, savedValues)
		return ctx.Err()
	}
}

// computeRemaining returns the sorted keys that haven't been locked yet.
func (pca *PrimeableCacheAside) computeRemaining(sorted []string, lockValues map[string]string) []string {
	remaining := make([]string, 0, len(sorted))
	for _, key := range sorted {
		if _, ok := lockValues[key]; !ok {
			remaining = append(remaining, key)
		}
	}
	return remaining
}

type lockAcquireEntry struct {
	key     string
	lockVal string
}

// batchAcquireWithBackup attempts to acquire write locks on entries, grouped by slot.
// Returns acquired locks, saved backups, the first failed key (in entry order), and any error.
// backups is lazily allocated by execSlotAcquire so the no-prior-value path stays alloc-free.
func (pca *PrimeableCacheAside) batchAcquireWithBackup(
	ctx context.Context,
	entries []lockAcquireEntry,
	lockTTLMs string,
) (acquired map[string]string, backups map[string]savedValue, firstFailed string, err error) {
	acquired = make(map[string]string, len(entries))

	slotGroups := cmdx.GroupBySlot(entries, func(e lockAcquireEntry) string { return e.key })

	for _, group := range slotGroups {
		var slotErr error
		backups, slotErr = pca.execSlotAcquire(ctx, group, lockTTLMs, acquired, backups)
		if slotErr != nil {
			return nil, nil, "", slotErr
		}
	}

	// Find first failed key in entry order.
	for _, e := range entries {
		if _, ok := acquired[e.key]; !ok {
			return acquired, backups, e.key, nil
		}
	}

	return acquired, backups, "", nil
}

// execSlotAcquire executes lock acquisitions for a single slot group and populates acquired/backups.
// execSlotAcquire executes lock acquisitions for a single slot group. backups
// is lazy-initialized via the returned map (callers must use the return value)
// so the no-prior-value happy path stays alloc-free.
func (pca *PrimeableCacheAside) execSlotAcquire(
	ctx context.Context,
	group []lockAcquireEntry,
	lockTTLMs string,
	acquired map[string]string,
	backups map[string]savedValue,
) (map[string]savedValue, error) {
	n := len(group)
	stmtsP := luaExecPool.Get(n)
	defer luaExecPool.Put(stmtsP)
	stmts := *stmtsP
	// Shared backing arrays for per-entry Keys/Args slices: 2 pool gets per
	// group instead of 2N small make() allocs.
	keysBufP := stringPool.Get(n)
	defer stringPool.Put(keysBufP)
	keysBuf := *keysBufP
	argsBufP := stringPool.Get(3 * n)
	defer stringPool.Put(argsBufP)
	argsBuf := *argsBufP
	for i, entry := range group {
		keysBuf[i] = entry.key
		argsBuf[i*3] = entry.lockVal
		argsBuf[i*3+1] = lockTTLMs
		argsBuf[i*3+2] = pca.lockPrefix
		stmts[i] = rueidis.LuaExec{
			Keys: keysBuf[i : i+1 : i+1],
			Args: argsBuf[i*3 : i*3+3 : i*3+3],
		}
	}
	resps := acquireWriteLockWithBackupScript.ExecMulti(ctx, pca.client, stmts...)
	// ExecMulti is a pipeline: every script has already executed in Redis
	// regardless of how we handle responses. Drain all responses to record
	// successes into `acquired` before bailing on any error — otherwise
	// later-index acquires that ran in Redis would leak for the full lockTTL.
	backups, firstErrKey, firstErr := pca.drainSlotAcquireResponses(group, resps, acquired, backups)
	if firstErr != nil {
		// Restore prior values where the acquire script captured one. Plain
		// unlock would DEL the lock and silently drop a real cached entry that
		// the acquire just overwrote. Mirrors rollbackAfterFirstFailure.
		for k, v := range acquired {
			if saved, ok := backups[k]; ok {
				pca.bestEffortRestore(ctx, k, v, saved)
				delete(backups, k)
			} else {
				pca.bestEffortUnlock(ctx, k, v)
			}
			delete(acquired, k)
		}
		return backups, fmt.Errorf("lock key %q: %w", firstErrKey, firstErr)
	}
	return backups, nil
}

// drainSlotAcquireResponses parses all responses from a pipelined slot acquire,
// recording successes into acquired/backups and returning the first error
// encountered. Subsequent errors are logged but do not stop the drain — every
// response must be inspected so the script's already-applied side-effects can
// be reconciled.
func (pca *PrimeableCacheAside) drainSlotAcquireResponses(
	group []lockAcquireEntry,
	resps []rueidis.RedisResult,
	acquired map[string]string,
	backups map[string]savedValue,
) (map[string]savedValue, string, error) {
	var firstErrKey string
	var firstErr error
	capture := func(key string, err error) {
		if firstErr == nil {
			firstErr = err
			firstErrKey = key
			return
		}
		pca.logger.Error("additional execSlotAcquire error", "key", key, "error", err)
	}
	for i, resp := range resps {
		arr, err := resp.ToArray()
		if err != nil {
			capture(group[i].key, err)
			continue
		}
		if len(arr) != 3 {
			capture(group[i].key, fmt.Errorf("malformed response (len=%d)", len(arr)))
			continue
		}
		var rerr error
		backups, rerr = pca.recordSlotAcquireResult(group[i], arr, acquired, backups)
		if rerr != nil {
			capture(group[i].key, rerr)
		}
	}
	return backups, firstErrKey, firstErr
}

// recordSlotAcquireResult parses one acquire response and updates acquired/backups.
// backups is lazy-allocated on first prior-value capture and returned to the
// caller, keeping the no-prior-value happy path alloc-free.
//
// Returns an error on script-drift parse failure so the caller can surface it via
// drainSlotAcquireResponses' firstErr channel — without this, an unparseable
// success-int gets silently treated as "not acquired" and acquireMultiWriteLocks
// loops forever recomputing remaining against the same broken response.
func (pca *PrimeableCacheAside) recordSlotAcquireResult(
	entry lockAcquireEntry,
	arr []rueidis.RedisMessage,
	acquired map[string]string,
	backups map[string]savedValue,
) (map[string]savedValue, error) {
	success, ierr := arr[0].AsInt64()
	if ierr != nil {
		pca.logger.Error("unexpected non-integer in lock-acquire response", "key", entry.key, "error", ierr)
		return backups, fmt.Errorf("parse success: %w", ierr)
	}
	if success != 1 {
		return backups, nil
	}
	acquired[entry.key] = entry.lockVal
	saved := parseBackup(pca.logger, entry.key, arr[1], arr[2])
	if saved.present {
		if backups == nil {
			backups = make(map[string]savedValue)
		}
		backups[entry.key] = saved
	}
	return backups, nil
}

// rollbackAfterFirstFailure releases locks acquired AFTER the first failed key
// (in sorted order), keeping locks before it. Uses cleanupCtx so a cancelled
// caller still rolls back rather than leaving locks live for the full TTL.
func (pca *PrimeableCacheAside) rollbackAfterFirstFailure(
	ctx context.Context,
	sorted []string,
	firstFailed string,
	lockValues map[string]string,
	savedValues map[string]savedValue,
	justAcquired map[string]string,
) {
	cleanupCtx, cancel := pca.cleanupCtx(ctx)
	defer cancel()
	pastFailure := false
	for _, key := range sorted {
		if key == firstFailed {
			pastFailure = true
			continue
		}
		if !pastFailure {
			continue
		}
		// Release locks acquired in this batch that are after the first failure.
		if lockVal, ok := justAcquired[key]; ok {
			if saved, hasSaved := savedValues[key]; hasSaved {
				pca.restoreValue(cleanupCtx, key, lockVal, saved)
				delete(savedValues, key)
			} else {
				pca.bestEffortUnlock(cleanupCtx, key, lockVal)
			}
			delete(lockValues, key)
		}
	}
}

// touchMultiLocks refreshes TTL on held locks using CAS PEXPIRE.
//
// When a lock is lost (CAS returned 0), the entry is left in lockValues so the
// eventual CAS-set returns ErrLockLost for that key — preserving the stealer's
// value rather than letting a re-acquire overwrite it. Lost-lock keys still
// emit metrics.LockLost so operators see the contention.
//
// Real Redis errors (network/timeout) are logged but the entry is also retained
// so the eventual CAS-set surfaces the failure to the caller via BatchError.
func (pca *PrimeableCacheAside) touchMultiLocks(ctx context.Context, lockValues map[string]string) {
	for key, lockVal := range lockValues {
		result, err := refreshLockScript.Exec(ctx, pca.client, []string{key}, []string{lockVal, pca.lockTTLMs}).AsInt64()
		if err != nil {
			pca.logger.Error("lock refresh script error", "key", key, "error", err)
			continue
		}
		if result == 0 {
			pca.logger.Debug("lock lost during refresh", "key", key)
			pca.emitLockLost(key)
		}
	}
}

type casSetEntry struct {
	key     string
	val     string
	lockVal string
}

// casSlotResult pairs a slot's entries with the per-entry script responses so
// the reduce step can emit per-key success/failure without re-grouping.
type casSlotResult struct {
	entries []casSetEntry
	resps   []rueidis.RedisResult
}

// setMultiValuesWithCAS batch-sets values using CAS, grouped by Redis cluster slot.
// Returns succeeded keys and a map of failed keys to their errors. The failed
// map is lazily allocated on first error so the all-success path stays alloc-free.
//
// `succeeded` is pre-sized to len(values) so the all-success path appends in
// place rather than triggering 4× slice grows from a nil starting point.
func (pca *PrimeableCacheAside) setMultiValuesWithCAS(
	ctx context.Context,
	ttl time.Duration,
	values map[string]string,
	lockValues map[string]string,
) (succeeded []string, failed map[string]error) {
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	entries := make([]casSetEntry, 0, len(values))
	for key, val := range values {
		lockVal, ok := lockValues[key]
		if !ok {
			continue
		}
		entries = append(entries, casSetEntry{key: key, val: val, lockVal: lockVal})
	}
	succeeded = make([]string, 0, len(entries))
	slotGroups := cmdx.GroupBySlot(entries, func(e casSetEntry) string { return e.key })

	for _, sr := range pca.runCASSlots(ctx, slotGroups, ttlMs) {
		pca.collectCASResults(sr.entries, sr.resps, &succeeded, &failed)
	}
	return succeeded, failed
}

// runCASSlots executes each slot's CAS-set script, fanning out to goroutines
// only when there is actually parallelism to exploit. Single-slot deployments
// (non-cluster Redis) hit the inline path and skip the goroutine + sync costs;
// ExecMulti already pipelines per-slot scripts. Mirrors CacheAside.runSlotSets.
func (pca *PrimeableCacheAside) runCASSlots(ctx context.Context, slotGroups map[uint16][]casSetEntry, ttlMs string) []casSlotResult {
	results := make([]casSlotResult, 0, len(slotGroups))
	if len(slotGroups) <= 1 {
		for _, group := range slotGroups {
			results = append(results, casSlotResult{entries: group, resps: pca.execSlotGroup(ctx, group, ttlMs)})
		}
		return results
	}
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, group := range slotGroups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sr := casSlotResult{entries: group, resps: pca.execSlotGroup(ctx, group, ttlMs)}
			mu.Lock()
			results = append(results, sr)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return results
}

// execSlotGroup pipelines the CAS-set Lua script for a single slot's entries.
//
// Per-entry Keys/Args slices share two pooled backing arrays (one for keys,
// one for the {val, ttlMs, lockVal} triples) instead of being allocated fresh
// — 2 pool gets per slot rather than 2N small make() allocs.
func (pca *PrimeableCacheAside) execSlotGroup(ctx context.Context, group []casSetEntry, ttlMs string) []rueidis.RedisResult {
	n := len(group)
	stmtsP := luaExecPool.Get(n)
	defer luaExecPool.Put(stmtsP)
	stmts := *stmtsP
	keysBufP := stringPool.Get(n)
	defer stringPool.Put(keysBufP)
	keysBuf := *keysBufP
	argsBufP := stringPool.Get(3 * n)
	defer stringPool.Put(argsBufP)
	argsBuf := *argsBufP
	for i, e := range group {
		keysBuf[i] = e.key
		argsBuf[i*3] = e.val
		argsBuf[i*3+1] = ttlMs
		argsBuf[i*3+2] = e.lockVal
		stmts[i] = rueidis.LuaExec{
			Keys: keysBuf[i : i+1 : i+1],
			Args: argsBuf[i*3 : i*3+3 : i*3+3],
		}
	}
	return setWithWriteLockScript.ExecMulti(ctx, pca.client, stmts...)
}

// collectCASResults processes Lua CAS responses, populating succeeded/failed.
// The failed map is allocated lazily on first error to keep the happy path
// allocation-free.
func (pca *PrimeableCacheAside) collectCASResults(
	entries []casSetEntry,
	resps []rueidis.RedisResult,
	succeeded *[]string,
	failed *map[string]error,
) {
	addFail := func(key string, err error) {
		if *failed == nil {
			*failed = make(map[string]error)
		}
		(*failed)[key] = err
	}
	for i, resp := range resps {
		key := entries[i].key
		if err := resp.Error(); err != nil {
			addFail(key, fmt.Errorf("CAS set key %q: %w", key, err))
			continue
		}
		val, ierr := resp.AsInt64()
		if ierr != nil {
			pca.logger.Error("unexpected non-integer in CAS-set response", "key", key, "error", ierr)
			addFail(key, fmt.Errorf("CAS set key %q: %w", key, ierr))
			continue
		}
		if val == 0 {
			addFail(key, ErrLockLost)
			pca.emitLockLost(key)
			continue
		}
		*succeeded = append(*succeeded, key)
	}
}

// restoreMultiValues restores saved values or deletes keys for all held locks.
func (pca *PrimeableCacheAside) restoreMultiValues(ctx context.Context, lockValues map[string]string, savedValues map[string]savedValue) {
	toCtx, cancel := pca.cleanupCtx(ctx)
	defer cancel()

	for key, lockVal := range lockValues {
		pca.restoreValue(toCtx, key, lockVal, savedValues[key])
	}
}

// restoreValue restores a single key's previous value (with original TTL) or
// deletes the key if no prior value was saved. Empty saved values are preserved
// when saved.present is true; "absent" is signaled by saved.present == false.
func (pca *PrimeableCacheAside) restoreValue(ctx context.Context, key, lockVal string, saved savedValue) {
	hadSaved := "0"
	if saved.present {
		hadSaved = "1"
	}
	pttlStr := strconv.FormatInt(saved.pttl, 10)
	err := restoreValueOrDeleteScript.Exec(ctx, pca.client, []string{key}, []string{lockVal, hadSaved, saved.val, pttlStr}).Error()
	if err != nil {
		pca.logger.Error("failed to restore value", "key", key, "error", err)
	}
}

// bestEffortUnlock releases a lock using delKeyLua.
func (pca *PrimeableCacheAside) bestEffortUnlock(ctx context.Context, key, lockVal string) {
	toCtx, cancel := pca.cleanupCtx(ctx)
	defer cancel()
	if err := pca.unlock(toCtx, key, lockVal); err != nil {
		pca.logger.Error("failed to unlock key", "key", key, "error", err)
	}
}

// bestEffortRestore wraps restoreValue with cleanupCtx + deferred cancel so a
// panic in the restore script cannot leak the rollback context. Mirrors the
// shape of bestEffortUnlock for consistency.
func (pca *PrimeableCacheAside) bestEffortRestore(ctx context.Context, key, lockVal string, saved savedValue) {
	toCtx, cancel := pca.cleanupCtx(ctx)
	defer cancel()
	pca.restoreValue(toCtx, key, lockVal, saved)
}
