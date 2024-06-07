package syncx_test

import (
	"testing"

	"github.com/dcbickfo/redcache/internal/syncx"
	"github.com/stretchr/testify/assert"
)

func TestMap_CompareAndDelete(t *testing.T) {
	var sm syncx.Map[string, int]
	key := "key"
	value := 1
	sm.Store(key, value)

	deleted := sm.CompareAndDelete(key, value)
	assert.Truef(t, deleted, "expected key %s to be deleted", key)

	_, ok := sm.Load(key)
	assert.Falsef(t, ok, "expected key %s to be absent", key)
}

func TestMap_CompareAndSwap(t *testing.T) {
	var sm syncx.Map[string, int]
	key := "key"
	oldValue := 1
	newValue := 2
	sm.Store(key, oldValue)

	swapped := sm.CompareAndSwap(key, oldValue, newValue)
	assert.Truef(t, swapped, "expected key %s value to be swapped", key)

	value, _ := sm.Load(key)
	assert.Equalf(t, newValue, value, "expected key %s value to be %d, got %d", key, newValue, value)
}

func TestMap_Delete(t *testing.T) {
	var sm syncx.Map[string, int]
	key := "key"
	value := 1
	sm.Store(key, value)

	sm.Delete(key)

	_, ok := sm.Load(key)
	assert.Falsef(t, ok, "expected key %s to be absent", key)
}

func TestMap_Load(t *testing.T) {
	var sm syncx.Map[string, int]
	key := "key"
	value := 1
	sm.Store(key, value)

	loadedValue, ok := sm.Load(key)
	assert.Truef(t, ok, "expected key %s to be present", key)
	assert.Equalf(t, value, loadedValue, "expected key %s value to be %d, got %d", key, value, loadedValue)
}

func TestMap_LoadAndDelete(t *testing.T) {
	var sm syncx.Map[string, int]
	key := "key"
	value := 1
	sm.Store(key, value)

	loadedValue, loaded := sm.LoadAndDelete(key)
	assert.Truef(t, loaded, "expected key %s to be loaded", key)
	assert.Equalf(t, value, loadedValue, "expected key %s value to be %d, got %d", key, value, loadedValue)

	_, ok := sm.Load(key)
	assert.Falsef(t, ok, "expected key %s to be absent", key)
}

func TestMap_LoadOrStore(t *testing.T) {
	var sm syncx.Map[string, int]
	key := "key"
	initialValue := 1
	newValue := 2

	value, loaded := sm.LoadOrStore(key, initialValue)
	assert.Falsef(t, loaded, "expected key %s not to be loaded initially", key)
	assert.Equalf(t, initialValue, value, "expected stored value for key %s to be %d, got %d", key, initialValue, value)

	value, loaded = sm.LoadOrStore(key, newValue)
	assert.Truef(t, loaded, "expected key %s to be loaded", key)
	assert.Equalf(t, initialValue, value, "expected loaded value for key %s to be %d, got %d", key, initialValue, value)
}

func TestMap_Range(t *testing.T) {
	var sm syncx.Map[string, int]
	key1 := "key1"
	value1 := 1
	key2 := "key2"
	value2 := 2

	sm.Store(key1, value1)
	sm.Store(key2, value2)

	found := map[string]int{}
	sm.Range(func(key string, value int) bool {
		found[key] = value
		return true
	})

	assert.Equalf(t, 2, len(found), "expected map to contain %d elements, got %d", 2, len(found))
	assert.Equalf(t, value1, found[key1], "expected map to contain key %s with value %d, got %d", key1, value1, found[key1])
	assert.Equalf(t, value2, found[key2], "expected map to contain key %s with value %d, got %d", key2, value2, found[key2])
}

func TestMap_Store(t *testing.T) {
	var sm syncx.Map[string, int]
	key := "key"
	value := 1
	sm.Store(key, value)

	loadedValue, ok := sm.Load(key)
	assert.Truef(t, ok, "expected key %s to be present", key)
	assert.Equalf(t, value, loadedValue, "expected key %s value to be %d, got %d", key, value, loadedValue)
}

func TestMap_Swap(t *testing.T) {
	var sm syncx.Map[string, int]
	key := "key"
	initialValue := 1
	newValue := 2
	sm.Store(key, initialValue)

	previousValue, loaded := sm.Swap(key, newValue)
	assert.Truef(t, loaded, "expected key %s to be loaded", key)
	assert.Equalf(t, initialValue, previousValue, "expected previous value for key %s to be %d, got %d", key, initialValue, previousValue)

	loadedValue, _ := sm.Load(key)
	assert.Equalf(t, newValue, loadedValue, "expected key %s value to be %d, got %d", key, newValue, loadedValue)
}
