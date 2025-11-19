package mapsx_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dcbickfo/redcache/internal/mapsx"
)

func TestKeys(t *testing.T) {
	t.Run("empty map returns empty slice", func(t *testing.T) {
		m := make(map[string]int)
		keys := mapsx.Keys(m)
		assert.Empty(t, keys)
		assert.NotNil(t, keys) // Should return empty slice, not nil
	})

	t.Run("extracts all keys from map", func(t *testing.T) {
		m := map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		}
		keys := mapsx.Keys(m)
		assert.Len(t, keys, 3)
		assert.ElementsMatch(t, []string{"a", "b", "c"}, keys)
	})

	t.Run("works with different types", func(t *testing.T) {
		m := map[int]string{
			1: "one",
			2: "two",
			3: "three",
		}
		keys := mapsx.Keys(m)
		assert.Len(t, keys, 3)
		assert.ElementsMatch(t, []int{1, 2, 3}, keys)
	})
}

func TestToSet(t *testing.T) {
	t.Run("converts string map to set", func(t *testing.T) {
		input := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		result := mapsx.ToSet(input)

		expected := map[string]bool{
			"key1": true,
			"key2": true,
			"key3": true,
		}
		assert.Equal(t, expected, result)
	})

	t.Run("converts int map to set", func(t *testing.T) {
		input := map[int]string{
			1: "one",
			2: "two",
			3: "three",
		}

		result := mapsx.ToSet(input)

		expected := map[int]bool{
			1: true,
			2: true,
			3: true,
		}
		assert.Equal(t, expected, result)
	})

	t.Run("returns empty set for empty map", func(t *testing.T) {
		input := map[string]int{}

		result := mapsx.ToSet(input)

		assert.Empty(t, result)
	})

	t.Run("returns empty set for nil map", func(t *testing.T) {
		var input map[string]string

		result := mapsx.ToSet(input)

		assert.Empty(t, result)
	})

	t.Run("works with struct values", func(t *testing.T) {
		type Value struct {
			Name string
			Age  int
		}
		input := map[string]Value{
			"alice": {Name: "Alice", Age: 30},
			"bob":   {Name: "Bob", Age: 25},
		}

		result := mapsx.ToSet(input)

		expected := map[string]bool{
			"alice": true,
			"bob":   true,
		}
		assert.Equal(t, expected, result)
	})

	t.Run("preserves all keys with different value types", func(t *testing.T) {
		input := map[string]interface{}{
			"key1": "string",
			"key2": 123,
			"key3": true,
		}

		result := mapsx.ToSet(input)

		expected := map[string]bool{
			"key1": true,
			"key2": true,
			"key3": true,
		}
		assert.Equal(t, expected, result)
	})
}
