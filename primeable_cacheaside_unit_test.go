package redcache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/dcbickfo/redcache/internal/lockmanager"
	mocklockmanager "github.com/dcbickfo/redcache/mocks/lockmanager"
	mocklogger "github.com/dcbickfo/redcache/mocks/logger"
	mockwritelock "github.com/dcbickfo/redcache/mocks/writelock"
)

// TestPrimeableCacheAside_Set tests all Set() scenarios using subtests
func TestPrimeableCacheAside_Set(t *testing.T) {
	mockLogger := &mocklogger.MockLogger{
		DebugFunc: func(msg string, keysAndValues ...interface{}) {},
		ErrorFunc: func(msg string, keysAndValues ...interface{}) {},
	}

	t.Run("SuccessfulSet_NoReadLock", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		key := "test:key"
		lockValue := "lock-123"
		computedValue := "computed-value"

		mockClient := mock.NewClient(ctrl)

		// WaitForKeyWithSubscription handles Redis internally, no DoCache expectation needed

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string { return "__redcache:lock:" },
			IsLockValueFunc: func(value string) bool {
				return len(value) > 0 && value[0] == 0x01
			},
			WaitForKeyWithSubscriptionFunc: func(ctx context.Context, key string, timeout time.Duration) (lockmanager.WaitHandle, string, error) {
				// No lock value found
				return nil, "", rueidis.Nil
			},
		}

		mockWriteLockManager := &mockwritelock.MockWriteLockManager{
			AcquireWriteLockFunc: func(ctx context.Context, key string) (string, error) {
				return lockValue, nil
			},
			CommitWriteLocksFunc: func(ctx context.Context, ttl time.Duration, lockValues map[string]string, actualValues map[string]string) (map[string]string, map[string]error) {
				// Return success for all keys
				return actualValues, nil
			},
		}

		pca := &PrimeableCacheAside{
			CacheAside: &CacheAside{
				client:      mockClient,
				lockManager: mockLockManager,
				lockTTL:     5 * time.Second,
				logger:      mockLogger,
				maxRetries:  3,
			},
			writeLockManager: mockWriteLockManager,
		}

		err := pca.Set(ctx, time.Minute, key, func(ctx context.Context, key string) (string, error) {
			return computedValue, nil
		})

		require.NoError(t, err)

		// Verify write lock was acquired
		assert.Len(t, mockWriteLockManager.AcquireWriteLockCalls(), 1)
		assert.Equal(t, key, mockWriteLockManager.AcquireWriteLockCalls()[0].Key)

		// Verify commit was called
		assert.Len(t, mockWriteLockManager.CommitWriteLocksCalls(), 1)
		commitCall := mockWriteLockManager.CommitWriteLocksCalls()[0]
		assert.Equal(t, time.Minute, commitCall.TTL)
		assert.Equal(t, lockValue, commitCall.LockValues[key])
		assert.Equal(t, computedValue, commitCall.ActualValues[key])
	})

	t.Run("WaitForReadLock_ThenSuccessfulSet", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		key := "test:key"
		lockValue := "lock-123"
		readLockValue := "\x01read-lock-value"
		computedValue := "computed-value"

		mockClient := mock.NewClient(ctrl)

		mockWaitHandle := &mocklockmanager.MockWaitHandle{
			WaitFunc: func(ctx context.Context) error {
				// Simulate successful wait for read lock to complete
				return nil
			},
		}

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string { return "__redcache:lock:" },
			IsLockValueFunc: func(value string) bool {
				return len(value) > 0 && value[0] == 0x01
			},
			WaitForKeyWithSubscriptionFunc: func(ctx context.Context, key string, timeout time.Duration) (lockmanager.WaitHandle, string, error) {
				// Read lock exists
				return mockWaitHandle, readLockValue, nil
			},
		}

		mockWriteLockManager := &mockwritelock.MockWriteLockManager{
			AcquireWriteLockFunc: func(ctx context.Context, key string) (string, error) {
				return lockValue, nil
			},
			CommitWriteLocksFunc: func(ctx context.Context, ttl time.Duration, lockValues map[string]string, actualValues map[string]string) (map[string]string, map[string]error) {
				return actualValues, nil
			},
		}

		pca := &PrimeableCacheAside{
			CacheAside: &CacheAside{
				client:      mockClient,
				lockManager: mockLockManager,
				lockTTL:     5 * time.Second,
				logger:      mockLogger,
				maxRetries:  3,
			},
			writeLockManager: mockWriteLockManager,
		}

		err := pca.Set(ctx, time.Minute, key, func(ctx context.Context, key string) (string, error) {
			return computedValue, nil
		})

		require.NoError(t, err)

		// Verify wait was called
		assert.Len(t, mockWaitHandle.WaitCalls(), 1)

		// Verify write lock was acquired after wait
		assert.Len(t, mockWriteLockManager.AcquireWriteLockCalls(), 1)
	})

	t.Run("CallbackError_ReleasesLock", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		key := "test:key"
		lockValue := "lock-123"
		callbackErr := errors.New("callback failed")

		mockClient := mock.NewClient(ctrl)

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string { return "__redcache:lock:" },
			IsLockValueFunc: func(value string) bool {
				return len(value) > 0 && value[0] == 0x01
			},
			WaitForKeyWithSubscriptionFunc: func(ctx context.Context, key string, timeout time.Duration) (lockmanager.WaitHandle, string, error) {
				return nil, "", rueidis.Nil
			},
		}

		mockWriteLockManager := &mockwritelock.MockWriteLockManager{
			AcquireWriteLockFunc: func(ctx context.Context, key string) (string, error) {
				return lockValue, nil
			},
			ReleaseWriteLockFunc: func(ctx context.Context, key string, lockVal string) error {
				return nil
			},
		}

		pca := &PrimeableCacheAside{
			CacheAside: &CacheAside{
				client:      mockClient,
				lockManager: mockLockManager,
				lockTTL:     5 * time.Second,
				logger:      mockLogger,
				maxRetries:  3,
			},
			writeLockManager: mockWriteLockManager,
		}

		err := pca.Set(ctx, time.Minute, key, func(ctx context.Context, key string) (string, error) {
			return "", callbackErr
		})

		require.Error(t, err)
		assert.Equal(t, callbackErr, err)

		// Verify lock was released (not committed) on error
		assert.Len(t, mockWriteLockManager.ReleaseWriteLockCalls(), 1)
		releaseCall := mockWriteLockManager.ReleaseWriteLockCalls()[0]
		assert.Equal(t, key, releaseCall.Key)
		assert.Equal(t, lockValue, releaseCall.LockValue)

		// Verify commit was NOT called
		assert.Len(t, mockWriteLockManager.CommitWriteLocksCalls(), 0)
	})

	t.Run("AcquireLockFailure", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		key := "test:key"
		lockErr := errors.New("lock acquisition failed")

		mockClient := mock.NewClient(ctrl)

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string { return "__redcache:lock:" },
			IsLockValueFunc: func(value string) bool {
				return len(value) > 0 && value[0] == 0x01
			},
			WaitForKeyWithSubscriptionFunc: func(ctx context.Context, key string, timeout time.Duration) (lockmanager.WaitHandle, string, error) {
				return nil, "", rueidis.Nil
			},
		}

		mockWriteLockManager := &mockwritelock.MockWriteLockManager{
			AcquireWriteLockFunc: func(ctx context.Context, key string) (string, error) {
				return "", lockErr
			},
		}

		pca := &PrimeableCacheAside{
			CacheAside: &CacheAside{
				client:      mockClient,
				lockManager: mockLockManager,
				lockTTL:     5 * time.Second,
				logger:      mockLogger,
				maxRetries:  3,
			},
			writeLockManager: mockWriteLockManager,
		}

		err := pca.Set(ctx, time.Minute, key, func(ctx context.Context, key string) (string, error) {
			t.Error("Callback should not be called when lock acquisition fails")
			return "", nil
		})

		require.Error(t, err)
		assert.Equal(t, lockErr, err)
	})
}

// TestPrimeableCacheAside_SetMulti tests all SetMulti() scenarios using subtests
func TestPrimeableCacheAside_SetMulti(t *testing.T) {
	mockLogger := &mocklogger.MockLogger{
		DebugFunc: func(msg string, keysAndValues ...interface{}) {},
		ErrorFunc: func(msg string, keysAndValues ...interface{}) {},
	}

	t.Run("AllKeysSuccess", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		keys := []string{"key1", "key2", "key3"}
		lockValues := map[string]string{
			"key1": "lock-1",
			"key2": "lock-2",
			"key3": "lock-3",
		}
		computedValues := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		mockClient := mock.NewClient(ctrl)

		// SetMulti uses waitForReadLocks which calls WaitForKey and CheckMultiKeysLocked
		mockWaitHandle := &mocklockmanager.MockWaitHandle{
			WaitFunc: func(ctx context.Context) error {
				return nil
			},
		}

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string { return "__redcache:lock:" },
			IsLockValueFunc: func(value string) bool {
				return len(value) > 0 && value[0] == 0x01
			},
			WaitForKeyFunc: func(key string) lockmanager.WaitHandle {
				return mockWaitHandle
			},
			CheckMultiKeysLockedFunc: func(ctx context.Context, keys []string) []string {
				// No keys are locked
				return []string{}
			},
		}

		mockWriteLockManager := &mockwritelock.MockWriteLockManager{
			AcquireMultiWriteLocksSequentialFunc: func(ctx context.Context, keys []string) (map[string]string, map[string]string, error) {
				// All locks acquired successfully, no saved values
				return lockValues, map[string]string{}, nil
			},
			ReleaseWriteLockFunc: func(ctx context.Context, key string, lockVal string) error {
				// Release called for cleanup
				return nil
			},
			CommitWriteLocksFunc: func(ctx context.Context, ttl time.Duration, lockVals map[string]string, actualVals map[string]string) (map[string]string, map[string]error) {
				// All commits successful
				return actualVals, nil
			},
		}

		pca := &PrimeableCacheAside{
			CacheAside: &CacheAside{
				client:      mockClient,
				lockManager: mockLockManager,
				lockTTL:     5 * time.Second,
				logger:      mockLogger,
				maxRetries:  3,
			},
			writeLockManager: mockWriteLockManager,
		}

		result, err := pca.SetMulti(ctx, time.Minute, keys, func(ctx context.Context, keys []string) (map[string]string, error) {
			return computedValues, nil
		})

		require.NoError(t, err)
		assert.Equal(t, computedValues, result)

		// Verify acquire multi sequential was called
		assert.Len(t, mockWriteLockManager.AcquireMultiWriteLocksSequentialCalls(), 1)

		// Verify commit was called with all values
		assert.Len(t, mockWriteLockManager.CommitWriteLocksCalls(), 1)
		commitCall := mockWriteLockManager.CommitWriteLocksCalls()[0]
		assert.Equal(t, time.Minute, commitCall.TTL)
		assert.Equal(t, lockValues, commitCall.LockValues)
		assert.Equal(t, computedValues, commitCall.ActualValues)
	})

	t.Run("AcquireLockError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		keys := []string{"key1", "key2", "key3"}
		lockErr := errors.New("lock acquisition failed")

		mockClient := mock.NewClient(ctrl)

		mockWaitHandle := &mocklockmanager.MockWaitHandle{
			WaitFunc: func(ctx context.Context) error {
				return nil
			},
		}

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string { return "__redcache:lock:" },
			IsLockValueFunc: func(value string) bool {
				return len(value) > 0 && value[0] == 0x01
			},
			WaitForKeyFunc: func(key string) lockmanager.WaitHandle {
				return mockWaitHandle
			},
			CheckMultiKeysLockedFunc: func(ctx context.Context, keys []string) []string {
				return []string{}
			},
		}

		mockWriteLockManager := &mockwritelock.MockWriteLockManager{
			AcquireMultiWriteLocksSequentialFunc: func(ctx context.Context, keys []string) (map[string]string, map[string]string, error) {
				// Lock acquisition failed
				return nil, nil, lockErr
			},
		}

		pca := &PrimeableCacheAside{
			CacheAside: &CacheAside{
				client:      mockClient,
				lockManager: mockLockManager,
				lockTTL:     5 * time.Second,
				logger:      mockLogger,
				maxRetries:  3,
			},
			writeLockManager: mockWriteLockManager,
		}

		result, err := pca.SetMulti(ctx, time.Minute, keys, func(ctx context.Context, keys []string) (map[string]string, error) {
			t.Error("Callback should not be called when lock acquisition fails")
			return nil, nil
		})

		require.Error(t, err)
		assert.Equal(t, lockErr, err)
		assert.Nil(t, result)
	})

	t.Run("CallbackError_ReleasesLocks", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		keys := []string{"key1", "key2"}
		lockValues := map[string]string{
			"key1": "lock-1",
			"key2": "lock-2",
		}
		callbackErr := errors.New("callback failed")

		mockClient := mock.NewClient(ctrl)

		mockWaitHandle := &mocklockmanager.MockWaitHandle{
			WaitFunc: func(ctx context.Context) error {
				return nil
			},
		}

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string { return "__redcache:lock:" },
			IsLockValueFunc: func(value string) bool {
				return len(value) > 0 && value[0] == 0x01
			},
			WaitForKeyFunc: func(key string) lockmanager.WaitHandle {
				return mockWaitHandle
			},
			CheckMultiKeysLockedFunc: func(ctx context.Context, keys []string) []string {
				return []string{}
			},
		}

		mockWriteLockManager := &mockwritelock.MockWriteLockManager{
			AcquireMultiWriteLocksSequentialFunc: func(ctx context.Context, keys []string) (map[string]string, map[string]string, error) {
				return lockValues, map[string]string{}, nil
			},
			ReleaseWriteLockFunc: func(ctx context.Context, key string, lockVal string) error {
				// Release called for cleanup
				return nil
			},
		}

		pca := &PrimeableCacheAside{
			CacheAside: &CacheAside{
				client:      mockClient,
				lockManager: mockLockManager,
				lockTTL:     5 * time.Second,
				logger:      mockLogger,
				maxRetries:  3,
			},
			writeLockManager: mockWriteLockManager,
		}

		result, err := pca.SetMulti(ctx, time.Minute, keys, func(ctx context.Context, keys []string) (map[string]string, error) {
			return nil, callbackErr
		})

		require.Error(t, err)
		assert.Equal(t, callbackErr, err)
		assert.Nil(t, result)

		// Verify locks were released (not committed)
		// ReleaseWriteLock is called once for each key (2 keys)
		assert.Len(t, mockWriteLockManager.ReleaseWriteLockCalls(), 2)

		// Verify commit was NOT called
		assert.Len(t, mockWriteLockManager.CommitWriteLocksCalls(), 0)
	})
}

// TestPrimeableCacheAside_ForceSet tests ForceSet() scenarios using subtests
func TestPrimeableCacheAside_ForceSet(t *testing.T) {
	mockLogger := &mocklogger.MockLogger{
		DebugFunc: func(msg string, keysAndValues ...interface{}) {},
		ErrorFunc: func(msg string, keysAndValues ...interface{}) {},
	}

	t.Run("SuccessfulForceSet", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		key := "test:key"
		value := "force-value"

		mockClient := mock.NewClient(ctrl)

		// ForceSet uses Do to set the value directly
		mockClient.EXPECT().
			Do(gomock.Any(), gomock.Any()).
			Return(mock.Result(mock.RedisString("OK")))

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string { return "__redcache:lock:" },
		}

		pca := &PrimeableCacheAside{
			CacheAside: &CacheAside{
				client:      mockClient,
				lockManager: mockLockManager,
				lockTTL:     5 * time.Second,
				logger:      mockLogger,
				maxRetries:  3,
			},
		}

		err := pca.ForceSet(ctx, time.Minute, key, value)

		require.NoError(t, err)
	})
}

// TestPrimeableCacheAside_ForceSetMulti tests ForceSetMulti() scenarios using subtests
func TestPrimeableCacheAside_ForceSetMulti(t *testing.T) {
	mockLogger := &mocklogger.MockLogger{
		DebugFunc: func(msg string, keysAndValues ...interface{}) {},
		ErrorFunc: func(msg string, keysAndValues ...interface{}) {},
	}

	t.Run("SuccessfulForceSetMulti", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		values := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		mockClient := mock.NewClient(ctrl)

		// ForceSetMulti uses DoMulti for batch SET
		mockClient.EXPECT().
			DoMulti(gomock.Any(), gomock.Any()).
			Return([]rueidis.RedisResult{
				mock.Result(mock.RedisString("OK")),
				mock.Result(mock.RedisString("OK")),
				mock.Result(mock.RedisString("OK")),
			})

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string { return "__redcache:lock:" },
		}

		pca := &PrimeableCacheAside{
			CacheAside: &CacheAside{
				client:      mockClient,
				lockManager: mockLockManager,
				lockTTL:     5 * time.Second,
				logger:      mockLogger,
				maxRetries:  3,
			},
		}

		err := pca.ForceSetMulti(ctx, time.Minute, values)

		require.NoError(t, err)
	})

	t.Run("PartialFailure", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		values := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		mockClient := mock.NewClient(ctrl)

		// Some keys succeed, one fails
		mockClient.EXPECT().
			DoMulti(gomock.Any(), gomock.Any()).
			Return([]rueidis.RedisResult{
				mock.Result(mock.RedisString("OK")),
				mock.ErrorResult(errors.New("redis error")),
				mock.Result(mock.RedisString("OK")),
			})

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string { return "__redcache:lock:" },
		}

		pca := &PrimeableCacheAside{
			CacheAside: &CacheAside{
				client:      mockClient,
				lockManager: mockLockManager,
				lockTTL:     5 * time.Second,
				logger:      mockLogger,
				maxRetries:  3,
			},
		}

		err := pca.ForceSetMulti(ctx, time.Minute, values)

		// ForceSetMulti should return error if any key fails
		require.Error(t, err)
	})
}
