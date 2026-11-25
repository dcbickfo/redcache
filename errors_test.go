package redcache_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

func TestBatchKeyError_Int_AccessorsAndFormat(t *testing.T) {
	t.Parallel()
	bke := &redcache.BatchKeyError[int]{
		Failed:    map[int]error{1: errors.New("boom"), 2: errors.New("bad")},
		Succeeded: []int{3, 4},
	}
	var err error = bke
	var got *redcache.BatchKeyError[int]
	if !errors.As(err, &got) {
		t.Fatalf("errors.As failed for *BatchKeyError[int]; got %T", err)
	}
	if !got.HasFailures() || !got.HasError(1) || got.HasError(99) {
		t.Fatalf("HasFailures/HasError wrong: %+v", got)
	}
	if got.ErrorFor(1) == nil || got.ErrorFor(99) != nil {
		t.Fatal("ErrorFor wrong")
	}
	if msg := got.Error(); !strings.Contains(msg, "2 succeeded, 2 failed") {
		t.Fatalf("missing summary: %s", msg)
	}
}

func TestBatchKeyError_String_AccessorsAndFormat(t *testing.T) {
	t.Parallel()
	keyErr := errors.New("timeout")
	bke := &redcache.BatchKeyError[string]{
		Failed:    map[string]error{"key1": keyErr, "key2": errors.New("lock lost")},
		Succeeded: []string{"key3"},
	}

	assert.True(t, bke.HasFailures())
	assert.True(t, bke.HasError("key1"))
	assert.False(t, bke.HasError("key3"))
	assert.False(t, bke.HasError("unknown"))

	require.ErrorIs(t, bke.ErrorFor("key1"), keyErr)
	assert.NoError(t, bke.ErrorFor("key3"))
	assert.NoError(t, bke.ErrorFor("unknown"))

	msg := bke.Error()
	assert.Contains(t, msg, "1 succeeded")
	assert.Contains(t, msg, "2 failed")
	assert.Contains(t, msg, "key1")
	assert.Contains(t, msg, "key2")
}

func TestBatchKeyError_HasFailures(t *testing.T) {
	t.Parallel()
	bke := &redcache.BatchKeyError[string]{
		Failed:    map[string]error{"key1": errors.New("err")},
		Succeeded: []string{"key2"},
	}
	assert.True(t, bke.HasFailures())

	bkeNoFail := &redcache.BatchKeyError[string]{
		Failed:    map[string]error{},
		Succeeded: []string{"key1"},
	}
	assert.False(t, bkeNoFail.HasFailures())
}

func TestBatchKeyError_Nil_SafeAccessors(t *testing.T) {
	t.Parallel()
	var bke *redcache.BatchKeyError[string]
	if bke.HasError("x") || bke.ErrorFor("x") != nil {
		t.Fatal("nil receiver should be safe and return zero values")
	}
}

func TestErrDecode_IsSentinel(t *testing.T) {
	t.Parallel()
	wrapped := fmt.Errorf("decoding user: %w", redcache.ErrDecode)
	if !errors.Is(wrapped, redcache.ErrDecode) {
		t.Fatal("ErrDecode should be reachable via errors.Is")
	}
}

func TestBatchKeyError_NilReceiverHasFailuresAndError(t *testing.T) {
	t.Parallel()
	var bke *redcache.BatchKeyError[int]
	if bke.HasFailures() {
		t.Fatal("nil receiver HasFailures should be false")
	}
	if got := bke.Error(); got != "" {
		t.Fatalf("nil receiver Error should be empty, got %q", got)
	}
}

func TestBatchKeyError_UnwrapExposesPerKeyErrors(t *testing.T) {
	t.Parallel()
	var err error = &redcache.BatchKeyError[int]{
		Failed: map[int]error{
			1: fmt.Errorf("write: %w", context.Canceled),
			2: redcache.ErrLockLost,
		},
	}
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, redcache.ErrLockLost)
	require.NotErrorIs(t, err, redcache.ErrDecode)
	require.Nil(t, (*redcache.BatchKeyError[int])(nil).Unwrap())
}

func TestBatchError_Error(t *testing.T) {
	t.Parallel()
	be := &redcache.EngineBatchError{
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
	be := &redcache.EngineBatchError{
		Failed:    map[string]error{"key1": errors.New("err")},
		Succeeded: []string{"key2"},
	}
	assert.True(t, be.HasFailures())

	beNoFail := &redcache.EngineBatchError{
		Failed:    map[string]error{},
		Succeeded: []string{"key1"},
	}
	assert.False(t, beNoFail.HasFailures())
}

func TestNewBatchError_NilWhenNoFailures(t *testing.T) {
	t.Parallel()
	be := redcache.NewEngineBatchError(map[string]error{}, []string{"key1"})
	assert.NoError(t, be)
}

func TestNewBatchError_ReturnsErrorWhenFailures(t *testing.T) {
	t.Parallel()
	failed := map[string]error{"key1": errors.New("oops")}
	succeeded := []string{"key2"}
	err := redcache.NewEngineBatchError(failed, succeeded)
	require.Error(t, err)
	var be *redcache.EngineBatchError
	require.ErrorAs(t, err, &be)
	assert.Equal(t, failed, be.Failed)
	assert.Equal(t, succeeded, be.Succeeded)
}

func TestBatchError_ErrorForAndHasError(t *testing.T) {
	t.Parallel()
	keyErr := errors.New("oops")
	be := &redcache.EngineBatchError{
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
	var be *redcache.EngineBatchError
	assert.False(t, be.HasError("anything"))
	assert.NoError(t, be.ErrorFor("anything"))
}

func TestBatchError_NilReceiverHasFailuresAndError(t *testing.T) {
	t.Parallel()
	var be *redcache.EngineBatchError
	if be.HasFailures() {
		t.Fatal("nil receiver HasFailures should be false")
	}
	if got := be.Error(); got != "" {
		t.Fatalf("nil receiver Error should be empty, got %q", got)
	}
}

func TestNewBatchKeyError_NilWhenNoFailures(t *testing.T) {
	t.Parallel()
	if redcache.NewIntBatchKeyError(map[int]error{}, []int{1}) != nil {
		t.Fatal("expected untyped-nil error")
	}
}

func TestNewBatchKeyError_ReturnsErrorWhenFailures(t *testing.T) {
	t.Parallel()
	failed := map[string]error{"key1": errors.New("oops")}
	succeeded := []string{"key2"}
	err := redcache.NewStringBatchKeyError(failed, succeeded)
	require.Error(t, err)
	var bke *redcache.BatchKeyError[string]
	require.ErrorAs(t, err, &bke)
	assert.Equal(t, failed, bke.Failed)
	assert.Equal(t, succeeded, bke.Succeeded)
}

func TestMergeForceSetResult_StringKeys(t *testing.T) {
	t.Parallel()
	values := map[string]string{"a": "1", "b": "2"}

	t.Run("all encoded values succeed when core succeeds", func(t *testing.T) {
		t.Parallel()
		failed := map[string]error{}
		succeeded := redcache.MergeForceSetStringResult(nil, values, nil, failed)

		assert.Empty(t, failed)
		assert.ElementsMatch(t, []string{"a", "b"}, succeeded)
	})

	t.Run("non batch error fails every encoded value", func(t *testing.T) {
		t.Parallel()
		wantErr := errors.New("redis failed")
		failed := map[string]error{}
		succeeded := redcache.MergeForceSetStringResult(wantErr, values, nil, failed)

		assert.Empty(t, succeeded)
		require.ErrorIs(t, failed["a"], wantErr)
		require.ErrorIs(t, failed["b"], wantErr)
	})

	t.Run("batch error preserves partial success", func(t *testing.T) {
		t.Parallel()
		wantErr := errors.New("lock lost")
		failed := map[string]error{}
		err := &redcache.EngineBatchError{
			Failed:    map[string]error{"b": wantErr},
			Succeeded: []string{"a"},
		}

		succeeded := redcache.MergeForceSetStringResult(err, values, nil, failed)

		assert.ElementsMatch(t, []string{"a"}, succeeded)
		require.ErrorIs(t, failed["b"], wantErr)
		assert.NotContains(t, failed, "a")
	})
}
