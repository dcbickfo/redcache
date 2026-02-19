package redcache_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dcbickfo/redcache"
)

func TestBatchError_Error(t *testing.T) {
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
	be := redcache.NewBatchError(map[string]error{}, []string{"key1"})
	assert.Nil(t, be)
}

func TestNewBatchError_ReturnsErrorWhenFailures(t *testing.T) {
	failed := map[string]error{"key1": errors.New("oops")}
	succeeded := []string{"key2"}
	err := redcache.NewBatchError(failed, succeeded)
	require.NotNil(t, err)
	var be *redcache.BatchError
	require.ErrorAs(t, err, &be)
	assert.Equal(t, failed, be.Failed)
	assert.Equal(t, succeeded, be.Succeeded)
}
