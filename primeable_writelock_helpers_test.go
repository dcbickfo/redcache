package redcache

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

var helperAddr = []string{"127.0.0.1:6379"}

func newHelperPCA(t *testing.T) *PrimeableCacheAside {
	t.Helper()
	pca, err := NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: helperAddr},
		CacheAsideOption{LockTTL: 2 * time.Second},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		pca.Close()
		pca.Client().Close()
	})
	return pca
}

// touchMultiLocks must NOT remove lost-lock entries from lockValues. The
// stale entry stays so the eventual CAS-set surfaces ErrLockLost — preserving
// the stealer's value rather than letting a re-acquire overwrite it.
func TestTouchMultiLocks_RetainsLostLocksForCASFailure(t *testing.T) {
	t.Parallel()
	pca := newHelperPCA(t)

	heldKey := "touch:held:" + uuid.New().String()
	lostKey := "touch:lost:" + uuid.New().String()
	heldLock := pca.lockPool.Generate()

	// Acquire a real lock for heldKey; we'll claim a fake lock for lostKey.
	ctx := context.Background()
	ok, _, err := pca.tryAcquireWriteLock(ctx, heldKey, heldLock, "2000")
	require.NoError(t, err)
	require.True(t, ok)
	t.Cleanup(func() { pca.bestEffortUnlock(context.Background(), heldKey, heldLock) })

	lockValues := map[string]string{
		heldKey: heldLock,
		lostKey: "__redcache:lock:nonexistent", // never set in Redis
	}

	pca.touchMultiLocks(ctx, lockValues)

	require.Contains(t, lockValues, heldKey, "real lock should be retained")
	require.Contains(t, lockValues, lostKey, "lost lock retained so CAS-set surfaces ErrLockLost")
}

// restoreValue logs (but does not propagate) an error when the underlying script
// errors. Closing the client is the simplest way to trigger an Exec error.
func TestRestoreValue_LogsErrorOnClientFailure(t *testing.T) {
	t.Parallel()
	pca := newHelperPCA(t)

	pca.Client().Close() // force subsequent Lua exec to fail

	// Should return without panicking even though the script call errors.
	pca.restoreValue(context.Background(), "restore:"+uuid.New().String(), "lock", savedValue{val: "saved", present: true})
}

// bestEffortUnlock should swallow errors from the unlock script.
func TestBestEffortUnlock_LogsErrorOnClientFailure(t *testing.T) {
	t.Parallel()
	pca := newHelperPCA(t)

	pca.Client().Close()

	pca.bestEffortUnlock(context.Background(), "unlock:"+uuid.New().String(), "lock")
}

// waitForReadLocks must surface a real Redis error (with key context) instead
// of silently advancing SetMulti against a broken cluster. Regression guard for
// the silent-failure fix that distinguishes redis-nil ("key absent") from real
// errors. Closing the client is the simplest way to make DoMultiCache fail.
func TestWaitForReadLocks_SurfacesRedisError(t *testing.T) {
	t.Parallel()
	pca := newHelperPCA(t)

	pca.Client().Close()

	key := "wait-readlock-err:" + uuid.New().String()
	err := pca.waitForReadLocks(context.Background(), []string{key})
	require.Error(t, err, "broken client should surface error from waitForReadLocks")
	require.Contains(t, err.Error(), "read key", "error should be wrapped with read key context")
	require.Contains(t, err.Error(), key, "error should be tagged with the offending key")
}

// waitForFailedKey returns ctx.Err() and triggers restoreMultiValues when the
// caller's context expires before the holder releases.
func TestWaitForFailedKey_ContextCancelled(t *testing.T) {
	t.Parallel()
	holder := newHelperPCA(t)
	waiter := newHelperPCA(t)

	key := "waitfail:" + uuid.New().String()
	holderLock := holder.lockPool.Generate()

	ok, _, err := holder.tryAcquireWriteLock(context.Background(), key, holderLock, "2000")
	require.NoError(t, err)
	require.True(t, ok)
	t.Cleanup(func() { holder.bestEffortUnlock(context.Background(), key, holderLock) })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = waiter.waitForFailedKey(ctx, key, map[string]string{}, map[string]savedValue{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
