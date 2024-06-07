package mapsx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeys(t *testing.T) {
	// Test with an empty map
	emptyMap := map[string]int{}
	keys := Keys(emptyMap)
	assert.Lenf(t, keys, 0, "expected no keys for empty map")

	// Test with a map with some elements
	sampleMap := map[string]int{"a": 1, "b": 2, "c": 3}
	keys = Keys(sampleMap)
	expectedKeys := []string{"a", "b", "c"}

	assert.ElementsMatch(t, expectedKeys, keys, "expected keys to match")

	// Test with a map with different key types
	intKeyMap := map[int]string{1: "one", 2: "two", 3: "three"}
	intKeys := Keys(intKeyMap)
	expectedIntKeys := []int{1, 2, 3}

	assert.ElementsMatch(t, expectedIntKeys, intKeys, "expected keys to match")
}

func TestValues(t *testing.T) {
	// Test with an empty map
	emptyMap := map[string]int{}
	values := Values(emptyMap)
	assert.Lenf(t, values, 0,"expected no values for empty map")

	// Test with a map with some elements
	sampleMap := map[string]int{"a": 1, "b": 2, "c": 3}
	values = Values(sampleMap)
	expectedValues := []int{1, 2, 3}

	assert.ElementsMatch(t, expectedValues, values, "expected values to match")

	// Test with a map with different value types
	intKeyMap := map[int]string{1: "one", 2: "two", 3: "three"}
	strValues := Values(intKeyMap)
	expectedStrValues := []string{"one", "two", "three"}

	assert.ElementsMatch(t, expectedStrValues, strValues, "expected values to match")
}
