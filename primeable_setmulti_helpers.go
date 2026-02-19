package redcache

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/redis/rueidis"
	"golang.org/x/sync/errgroup"

	"github.com/dcbickfo/redcache/internal/cmdx"
)

// acquireMultiWriteLocks acquires write locks on all keys in sorted order with rollback.
// Returns lockValues map and savedValues map (for rollback of previous real values).
func (pca *PrimeableCacheAside) acquireMultiWriteLocks(
	ctx context.Context,
	keys []string,
) (lockValues map[string]string, savedValues map[string]string, err error) {
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)

	lockValues = make(map[string]string, len(sorted))
	savedValues = make(map[string]string, len(sorted))
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
	savedValues map[string]string,
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
func (pca *PrimeableCacheAside) waitForFailedKey(
	ctx context.Context,
	firstFailed string,
	lockValues map[string]string,
	savedValues map[string]string,
) error {
	waitChan := pca.register(firstFailed)
	pca.client.DoCache(ctx, pca.client.B().Get().Key(firstFailed).Cache(), pca.lockTTL)

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
) (acquired map[string]string, backups map[string]string, firstFailed string, err error) {
	acquired = make(map[string]string, len(keys))
	backups = make(map[string]string)

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
	backups map[string]string,
) error {
	stmts := make([]rueidis.LuaExec, len(group))
	for i, entry := range group {
		stmts[i] = rueidis.LuaExec{
			Keys: []string{entry.key},
			Args: []string{entry.lockVal, lockTTLMs, pca.lockPrefix},
		}
	}
	resps := acquireWriteLockWithBackupScript.ExecMulti(ctx, pca.client, stmts...)
	for i, resp := range resps {
		arr, err := resp.ToArray()
		if err != nil {
			// Release any locks we've already acquired in previous slots.
			for k, v := range acquired {
				pca.bestEffortUnlock(ctx, k, v)
			}
			return fmt.Errorf("lock key %q: %w", group[i].key, err)
		}
		success, _ := arr[0].AsInt64()
		if success != 1 {
			continue
		}
		acquired[group[i].key] = group[i].lockVal
		if backupVal, bErr := arr[1].ToString(); bErr == nil && backupVal != "" {
			backups[group[i].key] = backupVal
		}
	}
	return nil
}

// rollbackAfterFirstFailure releases locks acquired AFTER the first failed key
// (in sorted order), keeping locks before it.
func (pca *PrimeableCacheAside) rollbackAfterFirstFailure(
	ctx context.Context,
	sorted []string,
	firstFailed string,
	lockValues map[string]string,
	savedValues map[string]string,
	justAcquired map[string]string,
) {
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
				pca.restoreValue(ctx, key, lockVal, saved)
				delete(savedValues, key)
			} else {
				pca.bestEffortUnlock(ctx, key, lockVal)
			}
			delete(lockValues, key)
		}
	}
}

// touchMultiLocks refreshes TTL on held locks using CAS PEXPIRE.
// Removes any locks that were lost (stolen by another operation).
func (pca *PrimeableCacheAside) touchMultiLocks(ctx context.Context, lockValues map[string]string) {
	lockTTLMs := strconv.FormatInt(pca.lockTTL.Milliseconds(), 10)
	for key, lockVal := range lockValues {
		result, err := refreshLockScript.Exec(ctx, pca.client, []string{key}, []string{lockVal, lockTTLMs}).AsInt64()
		if err != nil || result == 0 {
			// Lock was lost — remove from our set.
			pca.logger.Debug("lock refresh failed, removing from held locks", "key", key)
			delete(lockValues, key)
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

	eg, egCtx := errgroup.WithContext(ctx)
	resultsCh := make(chan slotResult, len(slotGroups))

	for _, group := range slotGroups {
		eg.Go(func() error {
			stmts := make([]rueidis.LuaExec, len(group))
			for i, e := range group {
				stmts[i] = e.setStmt
			}
			resps := setWithWriteLockScript.ExecMulti(egCtx, pca.client, stmts...)
			resultsCh <- slotResult{entries: group, resps: resps}
			return nil
		})
	}

	_ = eg.Wait()
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
		val, _ := resp.AsInt64()
		if val == 0 {
			failed[key] = ErrLockLost
			continue
		}
		*succeeded = append(*succeeded, key)
	}
}

// restoreMultiValues restores saved values or deletes keys for all held locks.
func (pca *PrimeableCacheAside) restoreMultiValues(ctx context.Context, lockValues, savedValues map[string]string) {
	cleanupCtx := context.WithoutCancel(ctx)
	toCtx, cancel := context.WithTimeout(cleanupCtx, pca.lockTTL)
	defer cancel()

	for key, lockVal := range lockValues {
		saved := savedValues[key]
		pca.restoreValue(toCtx, key, lockVal, saved)
	}
}

// restoreValue restores a single key's previous value or deletes it.
func (pca *PrimeableCacheAside) restoreValue(ctx context.Context, key, lockVal, savedValue string) {
	err := restoreValueOrDeleteScript.Exec(ctx, pca.client, []string{key}, []string{lockVal, savedValue}).Error()
	if err != nil {
		pca.logger.Error("failed to restore value", "key", key, "error", err)
	}
}

// bestEffortUnlock releases a lock using delKeyLua.
func (pca *PrimeableCacheAside) bestEffortUnlock(ctx context.Context, key, lockVal string) {
	cleanupCtx := context.WithoutCancel(ctx)
	toCtx, cancel := context.WithTimeout(cleanupCtx, pca.lockTTL)
	defer cancel()
	if err := pca.unlock(toCtx, key, lockVal); err != nil {
		pca.logger.Error("failed to unlock key", "key", key, "error", err)
	}
}

// unlockMultiKeys releases multiple locks.
func (pca *PrimeableCacheAside) unlockMultiKeys(ctx context.Context, lockVals map[string]string) {
	cleanupCtx := context.WithoutCancel(ctx)
	toCtx, cancel := context.WithTimeout(cleanupCtx, pca.lockTTL)
	defer cancel()
	pca.unlockMulti(toCtx, lockVals)
}
