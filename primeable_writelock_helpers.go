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
		return false, savedValue{}, nil
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
func (pca *PrimeableCacheAside) acquireMultiWriteLocks(
	ctx context.Context,
	keys []string,
) (lockValues map[string]string, savedValues map[string]savedValue, err error) {
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)

	lockValues = make(map[string]string, len(sorted))
	savedValues = make(map[string]savedValue, len(sorted))
	remaining := sorted

	for len(remaining) > 0 {
		firstFailed, err := pca.tryAcquireRemaining(ctx, remaining, lockValues, savedValues)
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
func (pca *PrimeableCacheAside) tryAcquireRemaining(
	ctx context.Context,
	remaining []string,
	lockValues map[string]string,
	savedValues map[string]savedValue,
) (firstFailed string, err error) {
	lockTTLMs := strconv.FormatInt(pca.lockTTL.Milliseconds(), 10)
	batchLocks := make(map[string]string, len(remaining))
	for _, key := range remaining {
		if _, ok := lockValues[key]; !ok {
			batchLocks[key] = pca.lockPool.Generate()
		}
	}

	acquired, backups, firstFailed, err := pca.batchAcquireWithBackup(ctx, remaining, batchLocks, lockTTLMs)
	if err != nil {
		return "", err
	}

	for key, lockVal := range acquired {
		lockValues[key] = lockVal
		if backup, ok := backups[key]; ok {
			savedValues[key] = backup
		}
	}

	if firstFailed != "" {
		pca.rollbackAfterFirstFailure(ctx, remaining, firstFailed, lockValues, savedValues, acquired)
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
	waitChan := pca.register(firstFailed)
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

// batchAcquireWithBackup attempts to acquire write locks on keys, grouped by slot.
// Returns acquired locks, saved backups, the first failed key (in input order), and any error.
func (pca *PrimeableCacheAside) batchAcquireWithBackup(
	ctx context.Context,
	keys []string,
	batchLocks map[string]string,
	lockTTLMs string,
) (acquired map[string]string, backups map[string]savedValue, firstFailed string, err error) {
	acquired = make(map[string]string, len(keys))
	backups = make(map[string]savedValue)

	entries := make([]lockAcquireEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, lockAcquireEntry{key: key, lockVal: batchLocks[key]})
	}
	slotGroups := cmdx.GroupBySlot(entries, func(e lockAcquireEntry) string { return e.key })

	for _, group := range slotGroups {
		if err := pca.execSlotAcquire(ctx, group, lockTTLMs, acquired, backups); err != nil {
			return nil, nil, "", err
		}
	}

	// Find first failed key in input order.
	for _, key := range keys {
		if _, ok := acquired[key]; !ok {
			return acquired, backups, key, nil
		}
	}

	return acquired, backups, "", nil
}

// execSlotAcquire executes lock acquisitions for a single slot group and populates acquired/backups.
func (pca *PrimeableCacheAside) execSlotAcquire(
	ctx context.Context,
	group []lockAcquireEntry,
	lockTTLMs string,
	acquired map[string]string,
	backups map[string]savedValue,
) error {
	stmts := make([]rueidis.LuaExec, len(group))
	for i, entry := range group {
		stmts[i] = rueidis.LuaExec{
			Keys: []string{entry.key},
			Args: []string{entry.lockVal, lockTTLMs, pca.lockPrefix},
		}
	}
	resps := acquireWriteLockWithBackupScript.ExecMulti(ctx, pca.client, stmts...)
	// ExecMulti is a pipeline: every script has already executed in Redis
	// regardless of how we handle responses. Drain all responses to record
	// successes into `acquired` before bailing on any error — otherwise
	// later-index acquires that ran in Redis would leak for the full lockTTL.
	firstErrKey, firstErr := pca.drainSlotAcquireResponses(group, resps, acquired, backups)
	if firstErr != nil {
		for k, v := range acquired {
			pca.bestEffortUnlock(ctx, k, v)
			delete(acquired, k)
		}
		return fmt.Errorf("lock key %q: %w", firstErrKey, firstErr)
	}
	return nil
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
) (firstErrKey string, firstErr error) {
	for i, resp := range resps {
		arr, err := resp.ToArray()
		if err != nil {
			if firstErr == nil {
				firstErr = err
				firstErrKey = group[i].key
			} else {
				pca.logger.Error("additional execSlotAcquire error", "key", group[i].key, "error", err)
			}
			continue
		}
		if len(arr) != 3 {
			if firstErr == nil {
				firstErr = fmt.Errorf("malformed response (len=%d)", len(arr))
				firstErrKey = group[i].key
			}
			continue
		}
		pca.recordSlotAcquireResult(group[i], arr, acquired, backups)
	}
	return firstErrKey, firstErr
}

// recordSlotAcquireResult parses one acquire response and updates acquired/backups.
func (pca *PrimeableCacheAside) recordSlotAcquireResult(
	entry lockAcquireEntry,
	arr []rueidis.RedisMessage,
	acquired map[string]string,
	backups map[string]savedValue,
) {
	success, ierr := arr[0].AsInt64()
	if ierr != nil {
		pca.logger.Error("unexpected non-integer in lock-acquire response", "key", entry.key, "error", ierr)
		return
	}
	if success != 1 {
		return
	}
	acquired[entry.key] = entry.lockVal
	saved := parseBackup(pca.logger, entry.key, arr[1], arr[2])
	if saved.present {
		backups[entry.key] = saved
	}
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
	lockTTLMs := strconv.FormatInt(pca.lockTTL.Milliseconds(), 10)
	for key, lockVal := range lockValues {
		result, err := refreshLockScript.Exec(ctx, pca.client, []string{key}, []string{lockVal, lockTTLMs}).AsInt64()
		if err != nil {
			pca.logger.Error("lock refresh script error", "key", key, "error", err)
			continue
		}
		if result == 0 {
			pca.logger.Debug("lock lost during refresh", "key", key)
			pca.metrics.LockLost(key)
		}
	}
}

type casSetEntry struct {
	key     string
	setStmt rueidis.LuaExec
}

// setMultiValuesWithCAS batch-sets values using CAS, grouped by Redis cluster slot.
// Returns succeeded keys and a map of failed keys to their errors.
func (pca *PrimeableCacheAside) setMultiValuesWithCAS(
	ctx context.Context,
	ttl time.Duration,
	values map[string]string,
	lockValues map[string]string,
) (succeeded []string, failed map[string]error) {
	failed = make(map[string]error)

	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	entries := make([]casSetEntry, 0, len(values))
	for key, val := range values {
		lockVal, ok := lockValues[key]
		if !ok {
			continue
		}
		entries = append(entries, casSetEntry{
			key: key,
			setStmt: rueidis.LuaExec{
				Keys: []string{key},
				Args: []string{val, ttlMs, lockVal},
			},
		})
	}
	slotGroups := cmdx.GroupBySlot(entries, func(e casSetEntry) string { return e.key })

	type slotResult struct {
		entries []casSetEntry
		resps   []rueidis.RedisResult
	}

	var wg sync.WaitGroup
	resultsCh := make(chan slotResult, len(slotGroups))

	for _, group := range slotGroups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stmts := make([]rueidis.LuaExec, len(group))
			for i, e := range group {
				stmts[i] = e.setStmt
			}
			resps := setWithWriteLockScript.ExecMulti(ctx, pca.client, stmts...)
			resultsCh <- slotResult{entries: group, resps: resps}
		}()
	}

	wg.Wait()
	close(resultsCh)

	for sr := range resultsCh {
		pca.collectCASResults(sr.entries, sr.resps, &succeeded, failed)
	}

	return succeeded, failed
}

// collectCASResults processes Lua CAS responses, populating succeeded/failed.
func (pca *PrimeableCacheAside) collectCASResults(
	entries []casSetEntry,
	resps []rueidis.RedisResult,
	succeeded *[]string,
	failed map[string]error,
) {
	for i, resp := range resps {
		key := entries[i].key
		if err := resp.Error(); err != nil {
			failed[key] = fmt.Errorf("CAS set key %q: %w", key, err)
			continue
		}
		val, ierr := resp.AsInt64()
		if ierr != nil {
			pca.logger.Error("unexpected non-integer in CAS-set response", "key", key, "error", ierr)
			failed[key] = fmt.Errorf("CAS set key %q: %w", key, ierr)
			continue
		}
		if val == 0 {
			failed[key] = ErrLockLost
			pca.metrics.LockLost(key)
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

// unlockMultiKeys releases multiple locks.
func (pca *PrimeableCacheAside) unlockMultiKeys(ctx context.Context, lockVals map[string]string) {
	toCtx, cancel := pca.cleanupCtx(ctx)
	defer cancel()
	pca.unlockMulti(toCtx, lockVals)
}
