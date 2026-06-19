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
