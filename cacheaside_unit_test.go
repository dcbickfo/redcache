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
)

// TestCacheAside_Get tests all Get() scenarios using subtests.
func TestCacheAside_Get(t *testing.T) {
	t.Run("CacheHit", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		key := "test:key"
		cachedValue := "cached-value"

		// Create mocked logger
		mockLogger := &mocklogger.MockLogger{
			DebugFunc: func(msg string, args ...any) {},
			ErrorFunc: func(msg string, args ...any) {
				t.Errorf("Unexpected error log: %s %v", msg, args)
			},
		}

		// Create mocked Redis client
		mockClient := mock.NewClient(ctrl)

		// Expect DoCache call for cache hit
		mockClient.EXPECT().
			DoCache(gomock.Any(), mock.Match("GET", key), gomock.Any()).
			Return(mock.Result(mock.RedisString(cachedValue)))

		// Create mocked lock manager
		tryAcquireCalled := false
		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string {
				return "__redcache:lock:"
			},
			IsLockValueFunc: func(value string) bool {
				return false
			},
			TryAcquireFunc: func(ctx context.Context, key string) (string, lockmanager.WaitHandle, error) {
				tryAcquireCalled = true
				return "", nil, nil
			},
		}

		// Create CacheAside instance
		ca := &CacheAside{
			client:      mockClient,
			lockManager: mockLockManager,
			lockTTL:     5 * time.Second,
			logger:      mockLogger,
			maxRetries:  3,
		}

		// Callback should not be called
		callbackCalled := false
		callback := func(ctx context.Context, key string) (string, error) {
			callbackCalled = true
			return "should-not-be-called", nil
		}

		// Execute Get
		result, err := ca.Get(ctx, time.Minute, key, callback)

		// Verify results
		require.NoError(t, err)
		assert.Equal(t, cachedValue, result)
		assert.False(t, callbackCalled, "Callback should not be called on cache hit")
		assert.False(t, tryAcquireCalled, "Lock should not be acquired on cache hit")
	})

	t.Run("CacheMiss_LockAcquired", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		key := "test:key"
		lockValue := "lock-value-123"
		callbackValue := "fetched-from-db"

		mockLogger := &mocklogger.MockLogger{
			DebugFunc: func(msg string, args ...any) {},
			ErrorFunc: func(msg string, args ...any) {
				t.Errorf("Unexpected error log: %s %v", msg, args)
			},
		}

		mockClient := mock.NewClient(ctrl)
		mockClient.EXPECT().
			DoCache(gomock.Any(), mock.Match("GET", key), gomock.Any()).
			Return(mock.Result(mock.RedisNil())).
			Times(2)

		lockAcquired := false
		lockCommitted := false
		lockReleased := false

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string {
				return "__redcache:lock:"
			},
			IsLockValueFunc: func(value string) bool {
				return false
			},
			TryAcquireFunc: func(ctx context.Context, k string) (string, lockmanager.WaitHandle, error) {
				lockAcquired = true
				assert.Equal(t, key, k)
				return lockValue, nil, nil
			},
			CommitReadLocksFunc: func(ctx context.Context, ttl time.Duration, lockValues map[string]string, actualValues map[string]string) (succeeded []string, needsRetry []string, err error) {
				lockCommitted = true
				require.Contains(t, lockValues, key)
				require.Contains(t, actualValues, key)
				assert.Equal(t, lockValue, lockValues[key])
				assert.Equal(t, callbackValue, actualValues[key])
				return []string{key}, nil, nil
			},
			ReleaseLockFunc: func(ctx context.Context, k string, lv string) error {
				lockReleased = true
				t.Errorf("Lock should be committed, not released")
				return nil
			},
		}

		ca := &CacheAside{
			client:      mockClient,
			lockManager: mockLockManager,
			lockTTL:     5 * time.Second,
			logger:      mockLogger,
			maxRetries:  3,
		}

		callbackCalled := false
		callback := func(ctx context.Context, k string) (string, error) {
			callbackCalled = true
			assert.Equal(t, key, k)
			return callbackValue, nil
		}

		result, err := ca.Get(ctx, time.Minute, key, callback)

		require.NoError(t, err)
		assert.Equal(t, callbackValue, result)
		assert.True(t, lockAcquired)
		assert.True(t, callbackCalled)
		assert.True(t, lockCommitted)
		assert.False(t, lockReleased)
	})

	t.Run("LockContention", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		key := "test:key"
		lockValue := "lock-value-456"
		callbackValue := "fetched-after-retry"

		mockLogger := &mocklogger.MockLogger{
			DebugFunc: func(msg string, args ...any) {},
			ErrorFunc: func(msg string, args ...any) {
				t.Errorf("Unexpected error log: %s %v", msg, args)
			},
		}

		mockClient := mock.NewClient(ctrl)
		mockClient.EXPECT().
			DoCache(gomock.Any(), mock.Match("GET", key), gomock.Any()).
			Return(mock.Result(mock.RedisNil())).
			MinTimes(2).
			MaxTimes(6)

		waitChan := make(chan struct{})
		close(waitChan)

		mockWaitHandle := &mocklockmanager.MockWaitHandle{
			WaitFunc: func(ctx context.Context) error {
				<-waitChan
				return nil
			},
		}

		retryAttempts := 0
		lockCommitted := false
		callbackCalled := false

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string {
				return "__redcache:lock:"
			},
			IsLockValueFunc: func(value string) bool {
				return false
			},
			TryAcquireFunc: func(ctx context.Context, k string) (string, lockmanager.WaitHandle, error) {
				retryAttempts++
				if retryAttempts == 1 {
					return "", mockWaitHandle, nil
				}
				return lockValue, nil, nil
			},
			CommitReadLocksFunc: func(ctx context.Context, ttl time.Duration, lockValues map[string]string, actualValues map[string]string) (succeeded []string, needsRetry []string, err error) {
				lockCommitted = true
				require.Contains(t, lockValues, key)
				require.Contains(t, actualValues, key)
				assert.Equal(t, lockValue, lockValues[key])
				assert.Equal(t, callbackValue, actualValues[key])
				return []string{key}, nil, nil
			},
		}

		ca := &CacheAside{
			client:      mockClient,
			lockManager: mockLockManager,
			lockTTL:     5 * time.Second,
			logger:      mockLogger,
			maxRetries:  3,
		}

		callback := func(ctx context.Context, k string) (string, error) {
			callbackCalled = true
			assert.Equal(t, key, k)
			return callbackValue, nil
		}

		result, err := ca.Get(ctx, time.Minute, key, callback)

		require.NoError(t, err)
		assert.Equal(t, callbackValue, result)
		assert.Equal(t, 2, retryAttempts, "Expected 2 lock acquisition attempts (1 retry)")
		assert.True(t, callbackCalled)
		assert.True(t, lockCommitted)
	})

	t.Run("CallbackError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		key := "test:key"
		lockValue := "lock-value-789"
		expectedErr := errors.New("database connection failed")

		mockLogger := &mocklogger.MockLogger{
			DebugFunc: func(msg string, args ...any) {},
			ErrorFunc: func(msg string, args ...any) {},
		}

		mockClient := mock.NewClient(ctrl)
		mockClient.EXPECT().
			DoCache(gomock.Any(), mock.Match("GET", key), gomock.Any()).
			Return(mock.Result(mock.RedisNil())).
			Times(2)

		lockAcquired := false
		lockReleased := false
		lockCommitted := false
		callbackCalled := false

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string {
				return "__redcache:lock:"
			},
			IsLockValueFunc: func(value string) bool {
				return false
			},
			TryAcquireFunc: func(ctx context.Context, k string) (string, lockmanager.WaitHandle, error) {
				lockAcquired = true
				assert.Equal(t, key, k)
				return lockValue, nil, nil
			},
			ReleaseLockFunc: func(ctx context.Context, k string, lv string) error {
				lockReleased = true
				assert.Equal(t, key, k)
				assert.Equal(t, lockValue, lv)
				return nil
			},
			CommitReadLocksFunc: func(ctx context.Context, ttl time.Duration, lockValues map[string]string, actualValues map[string]string) (succeeded []string, needsRetry []string, err error) {
				lockCommitted = true
				t.Errorf("Lock should be released, not committed, when callback returns error")
				return nil, nil, nil
			},
		}

		ca := &CacheAside{
			client:      mockClient,
			lockManager: mockLockManager,
			lockTTL:     5 * time.Second,
			logger:      mockLogger,
			maxRetries:  3,
		}

		callback := func(ctx context.Context, k string) (string, error) {
			callbackCalled = true
			assert.Equal(t, key, k)
			return "", expectedErr
		}

		result, err := ca.Get(ctx, time.Minute, key, callback)

		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
		assert.Empty(t, result)
		assert.True(t, lockAcquired)
		assert.True(t, callbackCalled)
		assert.True(t, lockReleased)
		assert.False(t, lockCommitted)
	})
}

// TestCacheAside_GetMulti tests all GetMulti() scenarios using subtests.
func TestCacheAside_GetMulti(t *testing.T) {
	t.Run("AllCacheHits", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		keys := []string{"key1", "key2", "key3"}

		mockLogger := &mocklogger.MockLogger{
			DebugFunc: func(msg string, args ...any) {},
			ErrorFunc: func(msg string, args ...any) {
				t.Errorf("Unexpected error log: %s %v", msg, args)
			},
		}

		mockClient := mock.NewClient(ctrl)
		mockClient.EXPECT().
			DoMultiCache(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, commands ...rueidis.CacheableTTL) []rueidis.RedisResult {
				// Return cached values for all keys in order
				return []rueidis.RedisResult{
					mock.Result(mock.RedisString("value1")),
					mock.Result(mock.RedisString("value2")),
					mock.Result(mock.RedisString("value3")),
				}
			})

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string {
				return "__redcache:lock:"
			},
			IsLockValueFunc: func(value string) bool {
				return false
			},
		}

		ca := &CacheAside{
			client:      mockClient,
			lockManager: mockLockManager,
			lockTTL:     5 * time.Second,
			logger:      mockLogger,
			maxRetries:  3,
		}

		callbackCalled := false
		callback := func(ctx context.Context, keys []string) (map[string]string, error) {
			callbackCalled = true
			return nil, errors.New("callback should not be called")
		}

		result, err := ca.GetMulti(ctx, time.Minute, keys, callback)

		require.NoError(t, err)
		assert.Len(t, result, 3)
		assert.Equal(t, "value1", result["key1"])
		assert.Equal(t, "value2", result["key2"])
		assert.Equal(t, "value3", result["key3"])
		assert.False(t, callbackCalled)
	})

	t.Run("AllMiss", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		keys := []string{"miss1", "miss2", "miss3"}
		callbackValues := map[string]string{
			"miss1": "fetched1",
			"miss2": "fetched2",
			"miss3": "fetched3",
		}
		lockValues := map[string]string{
			"miss1": "lock1",
			"miss2": "lock2",
			"miss3": "lock3",
		}

		mockLogger := &mocklogger.MockLogger{
			DebugFunc: func(msg string, args ...any) {},
			ErrorFunc: func(msg string, args ...any) {
				t.Errorf("Unexpected error log: %s %v", msg, args)
			},
		}

		mockClient := mock.NewClient(ctrl)
		mockClient.EXPECT().
			DoMultiCache(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, commands ...rueidis.CacheableTTL) []rueidis.RedisResult {
				results := make([]rueidis.RedisResult, len(commands))
				for i := range commands {
					results[i] = mock.Result(mock.RedisNil())
				}
				return results
			})

		tryAcquireCalled := false
		callbackCalled := false
		commitCalled := false

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string {
				return "__redcache:lock:"
			},
			IsLockValueFunc: func(value string) bool {
				return false
			},
			TryAcquireMultiFunc: func(ctx context.Context, acquireKeys []string) (map[string]string, map[string]lockmanager.WaitHandle, error) {
				tryAcquireCalled = true
				return lockValues, nil, nil
			},
			CommitReadLocksFunc: func(ctx context.Context, ttl time.Duration, lockVals map[string]string, actualValues map[string]string) (succeeded []string, needsRetry []string, err error) {
				commitCalled = true
				for key, lockVal := range lockValues {
					assert.Equal(t, lockVal, lockVals[key])
				}
				for key, expectedVal := range callbackValues {
					assert.Equal(t, expectedVal, actualValues[key])
				}
				return keys, nil, nil
			},
			CleanupUnusedLocksFunc: func(ctx context.Context, acquired map[string]string, used map[string]string) {},
		}

		ca := &CacheAside{
			client:      mockClient,
			lockManager: mockLockManager,
			lockTTL:     5 * time.Second,
			logger:      mockLogger,
			maxRetries:  3,
		}

		callback := func(ctx context.Context, cbKeys []string) (map[string]string, error) {
			callbackCalled = true
			assert.ElementsMatch(t, keys, cbKeys)
			return callbackValues, nil
		}

		result, err := ca.GetMulti(ctx, time.Minute, keys, callback)

		require.NoError(t, err)
		assert.Len(t, result, 3)
		assert.Equal(t, "fetched1", result["miss1"])
		assert.Equal(t, "fetched2", result["miss2"])
		assert.Equal(t, "fetched3", result["miss3"])
		assert.True(t, tryAcquireCalled)
		assert.True(t, callbackCalled)
		assert.True(t, commitCalled)
	})

	t.Run("PartialHits", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		keys := []string{"cached1", "miss1", "cached2", "miss2"}
		missedKeys := []string{"miss1", "miss2"}
		fetchedValues := map[string]string{
			"miss1": "fetched1",
			"miss2": "fetched2",
		}
		lockValues := map[string]string{
			"miss1": "lock1",
			"miss2": "lock2",
		}

		mockLogger := &mocklogger.MockLogger{
			DebugFunc: func(msg string, args ...any) {},
			ErrorFunc: func(msg string, args ...any) {
				t.Errorf("Unexpected error log: %s %v", msg, args)
			},
		}

		mockClient := mock.NewClient(ctrl)
		mockClient.EXPECT().
			DoMultiCache(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, commands ...rueidis.CacheableTTL) []rueidis.RedisResult {
				// Return: hit, miss, hit, miss (in order of keys: cached1, miss1, cached2, miss2)
				return []rueidis.RedisResult{
					mock.Result(mock.RedisString("value1")), // cached1
					mock.Result(mock.RedisNil()),            // miss1
					mock.Result(mock.RedisString("value2")), // cached2
					mock.Result(mock.RedisNil()),            // miss2
				}
			})

		tryAcquireCalled := false
		callbackCalled := false
		commitCalled := false

		mockLockManager := &mocklockmanager.MockLockManager{
			LockPrefixFunc: func() string {
				return "__redcache:lock:"
			},
			IsLockValueFunc: func(value string) bool {
				return false
			},
			TryAcquireMultiFunc: func(ctx context.Context, acquireKeys []string) (map[string]string, map[string]lockmanager.WaitHandle, error) {
				tryAcquireCalled = true
				assert.ElementsMatch(t, missedKeys, acquireKeys)
				result := make(map[string]string)
				for _, key := range acquireKeys {
					result[key] = lockValues[key]
				}
				return result, nil, nil
			},
			CommitReadLocksFunc: func(ctx context.Context, ttl time.Duration, lockVals map[string]string, actualValues map[string]string) (succeeded []string, needsRetry []string, err error) {
				commitCalled = true
				assert.Len(t, lockVals, 2)
				assert.Len(t, actualValues, 2)
				for key, expectedVal := range fetchedValues {
					assert.Equal(t, expectedVal, actualValues[key])
				}
				return missedKeys, nil, nil
			},
			CleanupUnusedLocksFunc: func(ctx context.Context, acquired map[string]string, used map[string]string) {},
		}

		ca := &CacheAside{
			client:      mockClient,
			lockManager: mockLockManager,
			lockTTL:     5 * time.Second,
			logger:      mockLogger,
			maxRetries:  3,
		}

		callback := func(ctx context.Context, cbKeys []string) (map[string]string, error) {
			callbackCalled = true
			assert.ElementsMatch(t, missedKeys, cbKeys)
			return fetchedValues, nil
		}

		result, err := ca.GetMulti(ctx, time.Minute, keys, callback)

		require.NoError(t, err)
		assert.Len(t, result, 4)
		assert.Equal(t, "value1", result["cached1"])
		assert.Equal(t, "value2", result["cached2"])
		assert.Equal(t, "fetched1", result["miss1"])
		assert.Equal(t, "fetched2", result["miss2"])
		assert.True(t, tryAcquireCalled)
		assert.True(t, callbackCalled)
		assert.True(t, commitCalled)
	})
}

// TestCacheAside_Del tests deletion operations using subtests.
func TestCacheAside_Del(t *testing.T) {
	t.Run("SingleKey", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		key := "test:key"

		mockLogger := &mocklogger.MockLogger{
			DebugFunc: func(msg string, args ...any) {},
			ErrorFunc: func(msg string, args ...any) {
				t.Errorf("Unexpected error log: %s %v", msg, args)
			},
		}

		mockClient := mock.NewClient(ctrl)
		mockClient.EXPECT().
			Do(gomock.Any(), mock.Match("DEL", key)).
			Return(mock.Result(mock.RedisInt64(1)))

		ca := &CacheAside{
			client: mockClient,
			lockManager: &mocklockmanager.MockLockManager{
				LockPrefixFunc: func() string {
					return "__redcache:lock:"
				},
			},
			lockTTL:    5 * time.Second,
			logger:     mockLogger,
			maxRetries: 3,
		}

		err := ca.Del(ctx, key)
		require.NoError(t, err)
	})

	t.Run("MultipleKeys", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := t.Context()
		keys := []string{"key1", "key2", "key3"}

		mockLogger := &mocklogger.MockLogger{
			DebugFunc: func(msg string, args ...any) {},
			ErrorFunc: func(msg string, args ...any) {
				t.Errorf("Unexpected error log: %s %v", msg, args)
			},
		}

		mockClient := mock.NewClient(ctrl)
		mockClient.EXPECT().
			DoMulti(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, commands ...rueidis.Completed) []rueidis.RedisResult {
				results := make([]rueidis.RedisResult, len(commands))
				for i := range commands {
					results[i] = mock.Result(mock.RedisInt64(1))
				}
				return results
			})

		ca := &CacheAside{
			client: mockClient,
			lockManager: &mocklockmanager.MockLockManager{
				LockPrefixFunc: func() string {
					return "__redcache:lock:"
				},
			},
			lockTTL:    5 * time.Second,
			logger:     mockLogger,
			maxRetries: 3,
		}

		err := ca.DelMulti(ctx, keys...)
		require.NoError(t, err)
	})
}
