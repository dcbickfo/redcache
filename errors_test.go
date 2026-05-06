package redcache_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

func TestBatchError_Error(t *testing.T) {
	t.Parallel()
	be := &redcache.BatchError{
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
	be := &redcache.BatchError{
		Failed:    map[string]error{"key1": errors.New("err")},
		Succeeded: []string{"key2"},
	}
	assert.True(t, be.HasFailures())

	beNoFail := &redcache.BatchError{
		Failed:    map[string]error{},
		Succeeded: []string{"key1"},
	}
	assert.False(t, beNoFail.HasFailures())
}

func TestNewBatchError_NilWhenNoFailures(t *testing.T) {
	t.Parallel()
	be := redcache.NewBatchError(map[string]error{}, []string{"key1"})
	assert.Nil(t, be)
}

func TestNewBatchError_ReturnsErrorWhenFailures(t *testing.T) {
	t.Parallel()
	failed := map[string]error{"key1": errors.New("oops")}
	succeeded := []string{"key2"}
	err := redcache.NewBatchError(failed, succeeded)
	require.NotNil(t, err)
	var be *redcache.BatchError
	require.ErrorAs(t, err, &be)
	assert.Equal(t, failed, be.Failed)
	assert.Equal(t, succeeded, be.Succeeded)
}

func TestBatchError_ErrorForAndHasError(t *testing.T) {
	t.Parallel()
	keyErr := errors.New("oops")
	be := &redcache.BatchError{
		Failed:    map[string]error{"key1": keyErr},
		Succeeded: []string{"key2"},
	}

	assert.True(t, be.HasError("key1"))
	assert.False(t, be.HasError("key2"))
	assert.False(t, be.HasError("unknown"))

	assert.ErrorIs(t, be.ErrorFor("key1"), keyErr)
	assert.NoError(t, be.ErrorFor("key2"))
	assert.NoError(t, be.ErrorFor("unknown"))
}

func TestBatchError_NilReceiverSafe(t *testing.T) {
	t.Parallel()
	var be *redcache.BatchError
	assert.False(t, be.HasError("anything"))
	assert.NoError(t, be.ErrorFor("anything"))
}

func TestBatchKeyError_Int_AccessorsAndFormat(t *testing.T) {
	t.Parallel()
	err := redcache.NewBatchKeyError(
		map[int]error{1: errors.New("boom"), 2: errors.New("bad")},
		[]int{3, 4},
	)
	if err == nil {
		t.Fatal("expected non-nil error")
	}
	var bke *redcache.BatchKeyError[int]
	if !errors.As(err, &bke) {
		t.Fatalf("errors.As failed for *BatchKeyError[int]; got %T", err)
	}
	if !bke.HasFailures() || !bke.HasError(1) || bke.HasError(99) {
		t.Fatalf("HasFailures/HasError wrong: %+v", bke)
	}
	if bke.ErrorFor(1) == nil || bke.ErrorFor(99) != nil {
		t.Fatal("ErrorFor wrong")
	}
	got := bke.Error()
	if !strings.Contains(got, "2 succeeded, 2 failed") {
		t.Fatalf("missing summary: %s", got)
	}
}

func TestBatchKeyError_Nil_SafeAccessors(t *testing.T) {
	t.Parallel()
	var bke *redcache.BatchKeyError[string]
	if bke.HasError("x") || bke.ErrorFor("x") != nil {
		t.Fatal("nil receiver should be safe and return zero values")
	}
}

func TestNewBatchKeyError_NilWhenNoFailures(t *testing.T) {
	t.Parallel()
	if redcache.NewBatchKeyError(map[int]error{}, []int{1}) != nil {
		t.Fatal("expected untyped-nil error")
	}
}

func TestErrDecode_IsSentinel(t *testing.T) {
	t.Parallel()
	wrapped := fmt.Errorf("decoding user: %w", redcache.ErrDecode)
	if !errors.Is(wrapped, redcache.ErrDecode) {
		t.Fatal("ErrDecode should be reachable via errors.Is")
	}
}

func TestBatchError_NilReceiverHasFailuresAndError(t *testing.T) {
	t.Parallel()
	var be *redcache.BatchError
	if be.HasFailures() {
		t.Fatal("nil receiver HasFailures should be false")
	}
	if got := be.Error(); got != "" {
		t.Fatalf("nil receiver Error should be empty, got %q", got)
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
