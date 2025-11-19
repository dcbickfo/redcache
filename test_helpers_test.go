package redcache_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dcbickfo/redcache/internal/cmdx"
)

// assertValueEquals checks if the actual value matches the expected value.
// This helper reduces boilerplate in tests that need to verify returned values.
func assertValueEquals(t *testing.T, expected, actual string, msgAndArgs ...interface{}) bool {
	t.Helper()
	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// assertCallbackCalled verifies that a callback was called exactly once.
// This is the most common assertion pattern in cache-aside tests.
func assertCallbackCalled(t *testing.T, called bool, msgAndArgs ...interface{}) bool {
	t.Helper()
	return assert.True(t, called, msgAndArgs...)
}

// assertCallbackNotCalled verifies that a callback was NOT called (cache hit).
// This is used to verify cache hit scenarios.
func assertCallbackNotCalled(t *testing.T, called bool, msgAndArgs ...interface{}) bool {
	t.Helper()
	return assert.False(t, called, msgAndArgs...)
}

// generateKeysInDifferentSlots generates n keys that hash to different Redis Cluster slots.
// This is critical for testing multi-slot operations in cluster mode.
// The function keeps trying different suffixes until it finds keys that hash to different slots.
func generateKeysInDifferentSlots(prefix string, count int) []string {
	if count <= 0 {
		return []string{}
	}

	keys := make([]string, 0, count)
	slots := make(map[uint16]bool)

	// Try up to 1000 iterations to find keys in different slots
	for i := 0; len(keys) < count && i < 1000; i++ {
		key := fmt.Sprintf("%s_%d", prefix, i)
		slot := cmdx.Slot(key)

		if !slots[slot] {
			keys = append(keys, key)
			slots[slot] = true
		}
	}

	if len(keys) < count {
		panic(fmt.Sprintf("could not generate %d keys in different slots after 1000 attempts (only got %d)", count, len(keys)))
	}

	return keys
}

// makeGetCallback creates a simple Get callback that returns a fixed value.
// This reduces boilerplate in tests that don't need complex callback logic.
func makeGetCallback(expectedValue string, called *bool) func(context.Context, string) (string, error) {
	return func(ctx context.Context, key string) (string, error) {
		*called = true
		return expectedValue, nil
	}
}

// makeGetMultiCallback creates a simple GetMulti callback that returns fixed values.
// This reduces boilerplate in tests that don't need complex callback logic.
func makeGetMultiCallback(expectedValues map[string]string, called *bool) func(context.Context, []string) (map[string]string, error) {
	return func(ctx context.Context, keys []string) (map[string]string, error) {
		*called = true
		result := make(map[string]string)
		for _, k := range keys {
			if v, ok := expectedValues[k]; ok {
				result[k] = v
			}
		}
		return result, nil
	}
}

// makeSetCallback creates a simple Set callback that returns a fixed value.
func makeSetCallback(valueToSet string, called *bool) func(context.Context, string) (string, error) {
	return func(ctx context.Context, key string) (string, error) {
		*called = true
		return valueToSet, nil
	}
}
