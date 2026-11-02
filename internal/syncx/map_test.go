package syncx_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dcbickfo/redcache/internal/syncx"
)

func TestShardedMap_ZeroValueOperations(t *testing.T) {
	t.Parallel()
	var sm syncx.ShardedMap[int]

	actual, loaded := sm.LoadOrStore("key", 1)
	assert.False(t, loaded)
	assert.Equal(t, 1, actual)

	actual, loaded = sm.LoadOrStore("key", 2)
	assert.True(t, loaded)
	assert.Equal(t, 1, actual)

	actual, loaded = sm.Load("key")
	assert.True(t, loaded)
	assert.Equal(t, 1, actual)

	deleted := sm.CompareAndDelete("key", 2)
	assert.False(t, deleted)

	deleted = sm.CompareAndDelete("key", 1)
	assert.True(t, deleted)

	_, loaded = sm.Load("key")
	assert.False(t, loaded)
}

func TestShardedMap_RangeStopsEarly(t *testing.T) {
	t.Parallel()
	var sm syncx.ShardedMap[int]
	sm.Store("a", 1)
	sm.Store("b", 2)

	seen := 0
	sm.Range(func(_ string, _ int) bool {
		seen++
		return false
	})

	assert.Equal(t, 1, seen)
}

func TestShardedMap_LoadAndDelete(t *testing.T) {
	t.Parallel()
	var sm syncx.ShardedMap[int]
	sm.Store("key", 7)

	actual, loaded := sm.LoadAndDelete("key")
	assert.True(t, loaded)
	assert.Equal(t, 7, actual)

	_, loaded = sm.Load("key")
	assert.False(t, loaded)
}

func TestShardedMap_Delete(t *testing.T) {
	t.Parallel()
	var sm syncx.ShardedMap[int]
	sm.Store("key", 7)

	sm.Delete("missing")
	sm.Delete("key")

	_, loaded := sm.Load("key")
	assert.False(t, loaded)
}
