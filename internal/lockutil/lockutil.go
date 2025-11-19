// Package lockutil provides shared utilities for lock management in cache-aside operations.
package lockutil

import (
	"context"
	"strings"
	"time"

	"github.com/redis/rueidis"
)

// LockChecker checks if a key has an active lock.
type LockChecker interface {
	// HasLock checks if the given value is a lock (not a real cached value).
	HasLock(val string) bool
	// CheckKeyLocked checks if a key currently has a lock in Redis.
	CheckKeyLocked(ctx context.Context, client rueidis.Client, key string) bool
}

// PrefixLockChecker checks locks based on a prefix.
type PrefixLockChecker struct {
	Prefix string
}

// HasLock checks if the value has the lock prefix.
func (p *PrefixLockChecker) HasLock(val string) bool {
	return strings.HasPrefix(val, p.Prefix)
}

// CheckKeyLocked checks if a key has an active lock.
func (p *PrefixLockChecker) CheckKeyLocked(ctx context.Context, client rueidis.Client, key string) bool {
	resp := client.Do(ctx, client.B().Get().Key(key).Build())
	val, err := resp.ToString()
	if err != nil {
		return false
	}
	return p.HasLock(val)
}

// BatchCheckLocks checks multiple keys for locks and returns those with locks.
// Uses DoMultiCache to ensure we're subscribed to invalidations for locked keys.
func BatchCheckLocks(ctx context.Context, client rueidis.Client, keys []string, checker LockChecker) []string {
	if len(keys) == 0 {
		return nil
	}

	// Build cacheable commands to check for locks
	cmds := make([]rueidis.CacheableTTL, 0, len(keys))
	for _, key := range keys {
		cmds = append(cmds, rueidis.CacheableTTL{
			Cmd: client.B().Get().Key(key).Cache(),
			TTL: time.Second, // Short TTL for lock checks
		})
	}

	lockedKeys := make([]string, 0)
	resps := client.DoMultiCache(ctx, cmds...)
	for i, resp := range resps {
		val, err := resp.ToString()
		if err == nil && checker.HasLock(val) {
			lockedKeys = append(lockedKeys, keys[i])
		}
	}

	return lockedKeys
}

// WaitForSingleLock waits for a single lock to be released via invalidation or timeout.
func WaitForSingleLock(ctx context.Context, waitChan <-chan struct{}, lockTTL time.Duration) error {
	timer := time.NewTimer(lockTTL)
	defer timer.Stop()

	select {
	case <-waitChan:
		// Lock released via invalidation
		return nil
	case <-timer.C:
		// Lock TTL expired
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
