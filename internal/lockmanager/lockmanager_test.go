//go:build integration

package lockmanager_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache/internal/invalidation"
	"github.com/dcbickfo/redcache/internal/lockmanager"
)

// noopLogger is a no-op logger for testing
type noopLogger struct{}

func (noopLogger) Debug(msg string, args ...any) {}
func (noopLogger) Error(msg string, args ...any) {}

// makeClient creates a Redis client for testing
func makeClient(t *testing.T) rueidis.Client {
	t.Helper()
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	require.NoError(t, err, "Failed to connect to Redis")
	return client
}

// makeLockManager creates a LockManager for testing
func makeLockManager(t *testing.T, client rueidis.Client) lockmanager.LockManager {
	t.Helper()

	invHandler := invalidation.NewRedisInvalidationHandler(invalidation.Config{
		LockTTL: 5 * time.Second,
		Logger:  noopLogger{},
	})

	return lockmanager.NewDistributedLockManager(lockmanager.Config{
		Client:              client,
		LockPrefix:          "__test:lock:",
		LockTTL:             5 * time.Second,
		Logger:              noopLogger{},
		InvalidationHandler: invHandler,
	})
}

// TestLockManager_TryAcquire tests optimistic lock acquisition
func TestLockManager_TryAcquire(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	lm := makeLockManager(t, client)
	ctx := context.Background()

	t.Run("acquires lock for new key", func(t *testing.T) {
		lockVal, retry, err := lm.TryAcquire(ctx, "key1")
		require.NoError(t, err)
		assert.NotEmpty(t, lockVal)
		assert.Nil(t, retry)

		// Clean up
		err = lm.ReleaseLock(ctx, "key1", lockVal)
		assert.NoError(t, err)
	})

	t.Run("returns retry handle when lock exists", func(t *testing.T) {
		// Acquire lock first
		lockVal1, _, err := lm.TryAcquire(ctx, "key2")
		require.NoError(t, err)
		defer lm.ReleaseLock(ctx, "key2", lockVal1)

		// Try to acquire again
		lockVal2, retry, err := lm.TryAcquire(ctx, "key2")
		require.NoError(t, err)
		assert.Empty(t, lockVal2)
		assert.NotNil(t, retry)
	})

	t.Run("context cancellation", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := lm.TryAcquire(cancelCtx, "key3")
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// TestLockManager_TryAcquireMulti tests batch optimistic lock acquisition
func TestLockManager_TryAcquireMulti(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	lm := makeLockManager(t, client)
	ctx := context.Background()

	t.Run("acquires locks for multiple keys", func(t *testing.T) {
		keys := []string{"m1", "m2", "m3"}

		acquired, retry, err := lm.TryAcquireMulti(ctx, keys)
		require.NoError(t, err)
		assert.Len(t, acquired, 3)
		assert.Empty(t, retry)

		// Clean up
		lm.ReleaseMultiLocks(ctx, acquired)
	})

	t.Run("returns retry handles for contested keys", func(t *testing.T) {
		keys := []string{"m4", "m5", "m6"}

		// Acquire some locks first
		lock1, _, err := lm.TryAcquire(ctx, "m4")
		require.NoError(t, err)
		defer lm.ReleaseLock(ctx, "m4", lock1)

		// Try to acquire all - should get retry for m4
		acquired, retry, err := lm.TryAcquireMulti(ctx, keys)
		require.NoError(t, err)
		assert.Len(t, acquired, 2) // m5, m6
		assert.Len(t, retry, 1)    // m4
		assert.Contains(t, retry, "m4")

		// Clean up
		lm.ReleaseMultiLocks(ctx, acquired)
	})
}

// TestLockManager_AcquireLockBlocking tests blocking lock acquisition
func TestLockManager_AcquireLockBlocking(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	lm := makeLockManager(t, client)
	ctx := context.Background()

	t.Run("acquires lock immediately if available", func(t *testing.T) {
		lockVal, err := lm.AcquireLockBlocking(ctx, "b1")
		require.NoError(t, err)
		assert.NotEmpty(t, lockVal)

		err = lm.ReleaseLock(ctx, "b1", lockVal)
		assert.NoError(t, err)
	})

	t.Run("waits for lock release", func(t *testing.T) {
		// Acquire lock first
		lock1, err := lm.AcquireLockBlocking(ctx, "b2")
		require.NoError(t, err)

		// Launch goroutine to release after delay
		go func() {
			time.Sleep(100 * time.Millisecond)
			lm.ReleaseLock(context.Background(), "b2", lock1)
		}()

		// Try to acquire - should wait and then succeed
		start := time.Now()
		lock2, err := lm.AcquireLockBlocking(ctx, "b2")
		duration := time.Since(start)

		require.NoError(t, err)
		assert.NotEmpty(t, lock2)
		assert.Greater(t, duration, 50*time.Millisecond)

		err = lm.ReleaseLock(ctx, "b2", lock2)
		assert.NoError(t, err)
	})

	t.Run("context cancellation", func(t *testing.T) {
		cancelCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		// Hold lock in another goroutine
		lock1, err := lm.AcquireLockBlocking(context.Background(), "b3")
		require.NoError(t, err)
		defer lm.ReleaseLock(context.Background(), "b3", lock1)

		// Try to acquire with short timeout
		_, err = lm.AcquireLockBlocking(cancelCtx, "b3")
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

// TestLockManager_AcquireMultiLocksBlocking tests batch blocking lock acquisition
func TestLockManager_AcquireMultiLocksBlocking(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	lm := makeLockManager(t, client)
	ctx := context.Background()

	t.Run("acquires all locks", func(t *testing.T) {
		keys := []string{"mb1", "mb2", "mb3"}

		acquired, err := lm.AcquireMultiLocksBlocking(ctx, keys)
		require.NoError(t, err)
		assert.Len(t, acquired, 3)

		lm.ReleaseMultiLocks(ctx, acquired)
	})

	t.Run("waits for all locks to become available", func(t *testing.T) {
		keys := []string{"mb4", "mb5"}

		// Hold one lock
		lock1, err := lm.AcquireLockBlocking(ctx, "mb4")
		require.NoError(t, err)

		// Release after delay
		go func() {
			time.Sleep(100 * time.Millisecond)
			lm.ReleaseLock(context.Background(), "mb4", lock1)
		}()

		// Try to acquire both - should wait
		start := time.Now()
		acquired, err := lm.AcquireMultiLocksBlocking(ctx, keys)
		duration := time.Since(start)

		require.NoError(t, err)
		assert.Len(t, acquired, 2)
		assert.Greater(t, duration, 50*time.Millisecond)

		lm.ReleaseMultiLocks(ctx, acquired)
	})
}

// TestLockManager_ReleaseLock tests lock release
func TestLockManager_ReleaseLock(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	lm := makeLockManager(t, client)
	ctx := context.Background()

	t.Run("releases owned lock", func(t *testing.T) {
		lockVal, err := lm.AcquireLockBlocking(ctx, "r1")
		require.NoError(t, err)

		err = lm.ReleaseLock(ctx, "r1", lockVal)
		assert.NoError(t, err)

		// Verify lock is released
		locked := lm.CheckKeyLocked(ctx, "r1")
		assert.False(t, locked)
	})

	t.Run("fails to release with wrong lock value", func(t *testing.T) {
		lockVal, err := lm.AcquireLockBlocking(ctx, "r2")
		require.NoError(t, err)
		defer lm.ReleaseLock(ctx, "r2", lockVal)

		err = lm.ReleaseLock(ctx, "r2", "wrong-value")
		assert.NoError(t, err) // Doesn't error, just doesn't release

		// Verify lock is still held
		locked := lm.CheckKeyLocked(ctx, "r2")
		assert.True(t, locked)
	})
}

// TestLockManager_CheckKeyLocked tests lock status checking
func TestLockManager_CheckKeyLocked(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	lm := makeLockManager(t, client)
	ctx := context.Background()

	t.Run("returns false for unlocked key", func(t *testing.T) {
		locked := lm.CheckKeyLocked(ctx, "c1")
		assert.False(t, locked)
	})

	t.Run("returns true for locked key", func(t *testing.T) {
		lockVal, err := lm.AcquireLockBlocking(ctx, "c2")
		require.NoError(t, err)
		defer lm.ReleaseLock(ctx, "c2", lockVal)

		locked := lm.CheckKeyLocked(ctx, "c2")
		assert.True(t, locked)
	})

	t.Run("returns false for real cached value", func(t *testing.T) {
		// Set a real value (not a lock)
		client.Do(ctx, client.B().Set().Key("c3").Value("real-value").Build())

		locked := lm.CheckKeyLocked(ctx, "c3")
		assert.False(t, locked)
	})
}

// TestLockManager_CheckMultiKeysLocked tests batch lock checking
func TestLockManager_CheckMultiKeysLocked(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	lm := makeLockManager(t, client)
	ctx := context.Background()

	t.Run("identifies locked keys", func(t *testing.T) {
		// Set up: lock some keys, leave others unlocked
		lock1, err := lm.AcquireLockBlocking(ctx, "cm1")
		require.NoError(t, err)
		defer lm.ReleaseLock(ctx, "cm1", lock1)

		lock3, err := lm.AcquireLockBlocking(ctx, "cm3")
		require.NoError(t, err)
		defer lm.ReleaseLock(ctx, "cm3", lock3)

		keys := []string{"cm1", "cm2", "cm3", "cm4"}
		locked := lm.CheckMultiKeysLocked(ctx, keys)

		assert.Len(t, locked, 2)
		assert.Contains(t, locked, "cm1")
		assert.Contains(t, locked, "cm3")
	})
}

// TestLockManager_CommitReadLocks tests CAS commit for read locks
func TestLockManager_CommitReadLocks(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	lm := makeLockManager(t, client)
	ctx := context.Background()

	t.Run("commits locks to real values", func(t *testing.T) {
		// Acquire locks
		lock1, err := lm.AcquireLockBlocking(ctx, "co1")
		require.NoError(t, err)

		lock2, err := lm.AcquireLockBlocking(ctx, "co2")
		require.NoError(t, err)

		lockValues := map[string]string{
			"co1": lock1,
			"co2": lock2,
		}

		actualValues := map[string]string{
			"co1": "value1",
			"co2": "value2",
		}

		succeeded, needsRetry, err := lm.CommitReadLocks(ctx, 10*time.Second, lockValues, actualValues)
		require.NoError(t, err)
		assert.Len(t, succeeded, 2)
		assert.Empty(t, needsRetry)

		// Verify values are set
		resp := client.Do(ctx, client.B().Get().Key("co1").Build())
		val, _ := resp.ToString()
		assert.Equal(t, "value1", val)
	})

	t.Run("returns needsRetry if lock lost", func(t *testing.T) {
		lock1, err := lm.AcquireLockBlocking(ctx, "co3")
		require.NoError(t, err)

		// Manually overwrite the lock
		client.Do(ctx, client.B().Set().Key("co3").Value("stolen").Build())

		lockValues := map[string]string{"co3": lock1}
		actualValues := map[string]string{"co3": "new-value"}

		succeeded, needsRetry, err := lm.CommitReadLocks(ctx, 10*time.Second, lockValues, actualValues)
		require.NoError(t, err)
		assert.Empty(t, succeeded)
		assert.Len(t, needsRetry, 1)
		assert.Contains(t, needsRetry, "co3")
	})
}

// TestLockManager_CleanupUnusedLocks tests cleanup of unused locks
func TestLockManager_CleanupUnusedLocks(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	lm := makeLockManager(t, client)
	ctx := context.Background()

	t.Run("releases unused locks only", func(t *testing.T) {
		// Acquire 3 locks
		acquired := make(map[string]string)
		for _, key := range []string{"cu1", "cu2", "cu3"} {
			lock, err := lm.AcquireLockBlocking(ctx, key)
			require.NoError(t, err)
			acquired[key] = lock
		}

		// Mark only 2 as used
		used := map[string]string{
			"cu1": acquired["cu1"],
			"cu2": acquired["cu2"],
		}

		// Cleanup should release cu3
		lm.CleanupUnusedLocks(ctx, acquired, used)

		// Verify cu1 and cu2 still locked
		assert.True(t, lm.CheckKeyLocked(ctx, "cu1"))
		assert.True(t, lm.CheckKeyLocked(ctx, "cu2"))

		// Verify cu3 released
		assert.False(t, lm.CheckKeyLocked(ctx, "cu3"))

		// Clean up remaining
		lm.ReleaseLock(ctx, "cu1", acquired["cu1"])
		lm.ReleaseLock(ctx, "cu2", acquired["cu2"])
	})
}

// TestLockManager_IsLockValue tests lock value identification
func TestLockManager_IsLockValue(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	lm := makeLockManager(t, client)

	t.Run("identifies lock values", func(t *testing.T) {
		lockVal := lm.GenerateLockValue()
		assert.True(t, lm.IsLockValue(lockVal))
	})

	t.Run("rejects non-lock values", func(t *testing.T) {
		assert.False(t, lm.IsLockValue("regular-value"))
		assert.False(t, lm.IsLockValue(""))
	})
}

// TestLockManager_GenerateLockValue tests lock value generation
func TestLockManager_GenerateLockValue(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	lm := makeLockManager(t, client)

	t.Run("generates unique lock values", func(t *testing.T) {
		lock1 := lm.GenerateLockValue()
		lock2 := lm.GenerateLockValue()

		assert.NotEmpty(t, lock1)
		assert.NotEmpty(t, lock2)
		assert.NotEqual(t, lock1, lock2)
	})

	t.Run("generated values are valid locks", func(t *testing.T) {
		lockVal := lm.GenerateLockValue()
		assert.True(t, lm.IsLockValue(lockVal))
	})
}
