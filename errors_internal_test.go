package redcache

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchError_Error(t *testing.T) {
	t.Parallel()
	be := &batchError{
		Failed:    map[string]error{"key1": errors.New("timeout"), "key2": errors.New("lock lost")},
		Succeeded: []string{"key3"},
	}
	msg := be.Error()
	assert.Contains(t, msg, "1 succeeded")
	assert.Contains(t, msg, "2 failed")
	assert.Contains(t, msg, "key1")
	assert.Contains(t, msg, "key2")
}

func TestBatchError_HasFailures(t *testing.T) {
	t.Parallel()
	be := &batchError{
		Failed:    map[string]error{"key1": errors.New("err")},
		Succeeded: []string{"key2"},
	}
	assert.True(t, be.HasFailures())

	beNoFail := &batchError{
		Failed:    map[string]error{},
		Succeeded: []string{"key1"},
	}
	assert.False(t, beNoFail.HasFailures())
}

func TestNewBatchError_NilWhenNoFailures(t *testing.T) {
	t.Parallel()
	be := newBatchError(map[string]error{}, []string{"key1"})
	assert.NoError(t, be)
}

func TestNewBatchError_ReturnsErrorWhenFailures(t *testing.T) {
	t.Parallel()
	failed := map[string]error{"key1": errors.New("oops")}
	succeeded := []string{"key2"}
	err := newBatchError(failed, succeeded)
	require.Error(t, err)
	var be *batchError
	require.ErrorAs(t, err, &be)
	assert.Equal(t, failed, be.Failed)
	assert.Equal(t, succeeded, be.Succeeded)
}

func TestBatchError_ErrorForAndHasError(t *testing.T) {
	t.Parallel()
	keyErr := errors.New("oops")
	be := &batchError{
		Failed:    map[string]error{"key1": keyErr},
		Succeeded: []string{"key2"},
	}

	assert.True(t, be.HasError("key1"))
	assert.False(t, be.HasError("key2"))
	assert.False(t, be.HasError("unknown"))

	require.ErrorIs(t, be.ErrorFor("key1"), keyErr)
	assert.NoError(t, be.ErrorFor("key2"))
	assert.NoError(t, be.ErrorFor("unknown"))
}

func TestBatchError_NilReceiverSafe(t *testing.T) {
	t.Parallel()
	var be *batchError
	assert.False(t, be.HasError("anything"))
	assert.NoError(t, be.ErrorFor("anything"))
}

func TestBatchError_NilReceiverHasFailuresAndError(t *testing.T) {
	t.Parallel()
	var be *batchError
	if be.HasFailures() {
		t.Fatal("nil receiver HasFailures should be false")
	}
	if got := be.Error(); got != "" {
		t.Fatalf("nil receiver Error should be empty, got %q", got)
	}
}

func TestNewBatchKeyError_NilWhenNoFailures(t *testing.T) {
	t.Parallel()
	if newBatchKeyError(map[int]error{}, []int{1}) != nil {
		t.Fatal("expected untyped-nil error")
	}
}

func TestNewBatchKeyError_ReturnsErrorWhenFailures(t *testing.T) {
	t.Parallel()
	failed := map[string]error{"key1": errors.New("oops")}
	succeeded := []string{"key2"}
	err := newBatchKeyError(failed, succeeded)
	require.Error(t, err)
	var bke *BatchKeyError[string]
	require.ErrorAs(t, err, &bke)
	assert.Equal(t, failed, bke.Failed)
	assert.Equal(t, succeeded, bke.Succeeded)
}

func TestMergeForceSetResultString(t *testing.T) {
	t.Parallel()
	values := map[string]string{"a": "1", "b": "2"}

	t.Run("all encoded values succeed when core succeeds", func(t *testing.T) {
		t.Parallel()
		failed := map[string]error{}
		succeeded := mergeForceSetResultString[string](nil, values, failed)

		assert.Empty(t, failed)
		assert.ElementsMatch(t, []string{"a", "b"}, succeeded)
	})

	t.Run("non batch error fails every encoded value", func(t *testing.T) {
		t.Parallel()
		wantErr := errors.New("redis failed")
		failed := map[string]error{}
		succeeded := mergeForceSetResultString[string](wantErr, values, failed)

		assert.Empty(t, succeeded)
		require.ErrorIs(t, failed["a"], wantErr)
		require.ErrorIs(t, failed["b"], wantErr)
	})

	t.Run("batch error preserves partial success", func(t *testing.T) {
		t.Parallel()
		wantErr := errors.New("lock lost")
		failed := map[string]error{}
		err := &batchError{
			Failed:    map[string]error{"b": wantErr},
			Succeeded: []string{"a"},
		}

		succeeded := mergeForceSetResultString[string](err, values, failed)

		assert.ElementsMatch(t, []string{"a"}, succeeded)
		require.ErrorIs(t, failed["b"], wantErr)
		assert.NotContains(t, failed, "a")
	})
}
