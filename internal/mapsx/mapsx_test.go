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
