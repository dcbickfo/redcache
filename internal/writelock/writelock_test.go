//go:build integration

package writelock_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache/internal/lockmanager"
	"github.com/dcbickfo/redcache/internal/writelock"
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

// makeWriteLockManager creates a WriteLockManager for testing
func makeWriteLockManager(t *testing.T, client rueidis.Client) writelock.WriteLockManager {
	t.Helper()

	lockMgr := lockmanager.NewDistributedLockManager(lockmanager.Config{
		Client:     client,
		LockPrefix: "__test:lock:",
		LockTTL:    5 * time.Second,
		Logger:     noopLogger{},
	})

	return writelock.NewCASWriteLockManager(writelock.Config{
		Client:      client,
		LockTTL:     5 * time.Second,
		LockManager: lockMgr,
		Logger:      noopLogger{},
	})
}

// TestWriteLockManager_AcquireWriteLock tests single write lock acquisition
func TestWriteLockManager_AcquireWriteLock(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	wlm := makeWriteLockManager(t, client)
	ctx := t.Context()

	t.Run("acquires lock for new key", func(t *testing.T) {
		lockVal, err := wlm.AcquireWriteLock(ctx, "key1")
		require.NoError(t, err)
		assert.NotEmpty(t, lockVal)

		// Clean up
		err = wlm.ReleaseWriteLock(ctx, "key1", lockVal)
		assert.NoError(t, err)
	})

	t.Run("context cancellation", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := wlm.AcquireWriteLock(cancelCtx, "key2")
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// TestWriteLockManager_AcquireMultiWriteLocks tests batch write lock acquisition
func TestWriteLockManager_AcquireMultiWriteLocks(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	wlm := makeWriteLockManager(t, client)
	ctx := t.Context()

	t.Run("acquires locks for multiple keys", func(t *testing.T) {
		keys := []string{"k1", "k2", "k3"}

		acquired, savedValues, failed, err := wlm.AcquireMultiWriteLocks(ctx, keys)
		require.NoError(t, err)
		assert.Len(t, acquired, 3)
		assert.Empty(t, failed)
		assert.NotNil(t, savedValues)

		// Clean up
		wlm.ReleaseWriteLocks(ctx, acquired)
	})

	t.Run("handles partial acquisition failure", func(t *testing.T) {
		keys := []string{"p1", "p2", "p3"}

		// Acquire first batch
		acquired1, _, _, err := wlm.AcquireMultiWriteLocks(ctx, keys)
		require.NoError(t, err)
		defer wlm.ReleaseWriteLocks(ctx, acquired1)

		// Try to acquire again - should fail
		acquired2, _, failed2, err := wlm.AcquireMultiWriteLocks(ctx, keys)
		require.NoError(t, err)
		assert.Empty(t, acquired2)
		assert.Len(t, failed2, 3)
	})
}

// TestWriteLockManager_ReleaseWriteLock tests lock release
func TestWriteLockManager_ReleaseWriteLock(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	wlm := makeWriteLockManager(t, client)
	ctx := t.Context()

	t.Run("releases owned lock", func(t *testing.T) {
		lockVal, err := wlm.AcquireWriteLock(ctx, "rel1")
		require.NoError(t, err)

		err = wlm.ReleaseWriteLock(ctx, "rel1", lockVal)
		assert.NoError(t, err)
	})

	t.Run("fails to release with wrong lock value", func(t *testing.T) {
		lockVal, err := wlm.AcquireWriteLock(ctx, "rel2")
		require.NoError(t, err)
		defer wlm.ReleaseWriteLock(ctx, "rel2", lockVal)

		err = wlm.ReleaseWriteLock(ctx, "rel2", "wrong-value")
		assert.NoError(t, err) // Doesn't error, just doesn't release
	})
}

// TestWriteLockManager_CommitWriteLocks tests CAS commit operation
func TestWriteLockManager_CommitWriteLocks(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	wlm := makeWriteLockManager(t, client)
	ctx := t.Context()

	t.Run("commits locks to real values", func(t *testing.T) {
		keys := []string{"c1", "c2"}
		acquired, _, _, err := wlm.AcquireMultiWriteLocks(ctx, keys)
		require.NoError(t, err)

		actualValues := map[string]string{
			"c1": "value1",
			"c2": "value2",
		}

		succeeded, failed := wlm.CommitWriteLocks(ctx, 10*time.Second, acquired, actualValues)
		assert.Len(t, succeeded, 2)
		assert.Empty(t, failed)

		// Verify values are set
		resp := client.Do(ctx, client.B().Get().Key("c1").Build())
		val, _ := resp.ToString()
		assert.Equal(t, "value1", val)
	})

	t.Run("fails commit if lock lost", func(t *testing.T) {
		lockVal, err := wlm.AcquireWriteLock(ctx, "c3")
		require.NoError(t, err)

		// Manually overwrite the lock
		client.Do(ctx, client.B().Set().Key("c3").Value("stolen").Build())

		lockValues := map[string]string{"c3": lockVal}
		actualValues := map[string]string{"c3": "new-value"}

		succeeded, failed := wlm.CommitWriteLocks(ctx, 10*time.Second, lockValues, actualValues)
		assert.Empty(t, succeeded)
		assert.Len(t, failed, 1)
	})
}

// TestWriteLockManager_AcquireMultiWriteLocksSequential tests sequential lock acquisition
func TestWriteLockManager_AcquireMultiWriteLocksSequential(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	wlm := makeWriteLockManager(t, client)
	ctx := t.Context()

	t.Run("acquires all locks sequentially", func(t *testing.T) {
		keys := []string{"s1", "s2", "s3"}

		lockValues, savedValues, err := wlm.AcquireMultiWriteLocksSequential(ctx, keys)
		require.NoError(t, err)
		assert.Len(t, lockValues, 3)
		assert.NotNil(t, savedValues)

		// Verify locks are held
		for key := range lockValues {
			assert.Contains(t, keys, key)
		}

		// Clean up
		wlm.ReleaseWriteLocks(ctx, lockValues)
	})

	t.Run("context cancellation", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := wlm.AcquireMultiWriteLocksSequential(cancelCtx, []string{"s4", "s5"})
		assert.Error(t, err)
	})
}

// TestWriteLockManager_TouchLocks tests lock TTL refresh
func TestWriteLockManager_TouchLocks(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	wlm := makeWriteLockManager(t, client)
	ctx := t.Context()

	t.Run("refreshes lock TTL", func(t *testing.T) {
		lockVal, err := wlm.AcquireWriteLock(ctx, "touch1")
		require.NoError(t, err)
		defer wlm.ReleaseWriteLock(ctx, "touch1", lockVal)

		lockValues := map[string]string{"touch1": lockVal}

		// Touch the lock
		wlm.TouchLocks(ctx, lockValues)

		// Verify lock still exists
		resp := client.Do(ctx, client.B().Ttl().Key("touch1").Build())
		ttl, err := resp.AsInt64()
		require.NoError(t, err)
		assert.Greater(t, ttl, int64(0))
	})
}

// TestWriteLockManager_RestoreValues tests backup restoration
func TestWriteLockManager_RestoreValues(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	wlm := makeWriteLockManager(t, client)
	ctx := t.Context()

	t.Run("restores backed up values", func(t *testing.T) {
		// Set initial value
		client.Do(ctx, client.B().Set().Key("restore1").Value("original").Build())

		keys := []string{"restore1"}
		lockValues, savedValues, err := wlm.AcquireMultiWriteLocksSequential(ctx, keys)
		require.NoError(t, err)

		// Verify original value was saved
		assert.Equal(t, "original", savedValues["restore1"])

		// Restore the value
		wlm.RestoreValues(ctx, savedValues, lockValues)

		// Verify value was restored
		resp := client.Do(ctx, client.B().Get().Key("restore1").Build())
		val, _ := resp.ToString()
		assert.Equal(t, "original", val)
	})
}
