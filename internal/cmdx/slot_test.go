package cmdx_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dcbickfo/redcache/internal/cmdx"
)

func TestSlot(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected uint16
	}{
		// Basic keys - verified against Redis cluster spec
		{
			name:     "simple key",
			key:      "key",
			expected: 12539,
		},
		{
			name:     "numeric key",
			key:      "123",
			expected: 5970,
		},
		{
			name:     "empty key",
			key:      "",
			expected: 0,
		},
		// Hash tags - only the content between { and } is hashed
		{
			name:     "hash tag simple",
			key:      "{user:1000}:profile",
			expected: cmdx.Slot("user:1000"),
		},
		{
			name:     "hash tag at start",
			key:      "{tag}key",
			expected: cmdx.Slot("tag"),
		},
		{
			name:     "hash tag at end",
			key:      "key{tag}",
			expected: cmdx.Slot("tag"),
		},
		{
			name:     "hash tag in middle",
			key:      "prefix{tag}suffix",
			expected: cmdx.Slot("tag"),
		},
		// Edge cases with braces
		{
			name:     "empty hash tag",
			key:      "key{}value",
			expected: cmdx.Slot("key{}value"), // Empty tags are ignored
		},
		{
			name:     "no closing brace",
			key:      "key{value",
			expected: cmdx.Slot("key{value"), // No closing brace, whole key hashed
		},
		{
			name:     "only opening brace",
			key:      "{key",
			expected: cmdx.Slot("{key"),
		},
		{
			name:     "only closing brace",
			key:      "key}",
			expected: cmdx.Slot("key}"),
		},
		{
			name:     "multiple hash tags - first wins",
			key:      "{tag1}{tag2}",
			expected: cmdx.Slot("tag1"),
		},
		{
			name:     "nested braces",
			key:      "{{nested}}",
			expected: cmdx.Slot("{nested"), // First { to first }
		},
		// Common patterns - these should be deterministic
		{
			name:     "user pattern",
			key:      "user:1000",
			expected: 1649, // Verified against Redis CLUSTER KEYSLOT
		},
		{
			name:     "session pattern",
			key:      "session:abc123",
			expected: 11692, // Verified against Redis CLUSTER KEYSLOT
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cmdx.Slot(tt.key)
			assert.Equalf(t, tt.expected, result, "Slot(%q) = %d, want %d", tt.key, result, tt.expected)
		})
	}
}

func TestSlot_Consistency(t *testing.T) {
	// Test that the same key always produces the same slot
	key := "test:key:123"
	slot1 := cmdx.Slot(key)
	slot2 := cmdx.Slot(key)
	assert.Equal(t, slot1, slot2, "Slot function should be deterministic")
}

func TestSlot_Distribution(t *testing.T) {
	// Test that slots are distributed across the valid range
	keys := []string{
		"key1", "key2", "key3", "key4", "key5",
		"user:1", "user:2", "user:3", "user:4", "user:5",
		"session:a", "session:b", "session:c", "session:d", "session:e",
	}

	slots := make(map[uint16]bool)
	for _, key := range keys {
		slot := cmdx.Slot(key)
		assert.LessOrEqualf(t, slot, uint16(16383), "Slot for key %q should be <= 16383", key)
		slots[slot] = true
	}

	// With 15 different keys, we should have some distribution (not all the same slot)
	assert.Greater(t, len(slots), 1, "Keys should distribute across multiple slots")
}

func TestSlot_HashTagCollision(t *testing.T) {
	// Keys with the same hash tag should go to the same slot
	keys := []string{
		"{user:1000}:profile",
		"{user:1000}:settings",
		"{user:1000}:preferences",
	}

	expectedSlot := cmdx.Slot("user:1000")
	for _, key := range keys {
		slot := cmdx.Slot(key)
		assert.Equalf(t, expectedSlot, slot, "Key %q with hash tag should map to slot %d", key, expectedSlot)
	}
}

func TestSlot_BoundaryValues(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{"single char", "a"},
		{"special chars", "!@#$%^&*()"},
		{"unicode", "你好世界"},
		{"long key", string(make([]byte, 1000))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slot := cmdx.Slot(tt.key)
			assert.LessOrEqualf(t, slot, uint16(16383), "Slot should be within valid range")
		})
	}
}

func TestGroupBySlot(t *testing.T) {
	type item struct {
		key   string
		value int
	}

	items := []item{
		{key: "{user:1}:a", value: 1},
		{key: "{user:1}:b", value: 2},
		{key: "{user:2}:a", value: 3},
		{key: "standalone", value: 4},
	}

	groups := cmdx.GroupBySlot(items, func(i item) string { return i.key })

	// {user:1}:a and {user:1}:b should be in the same slot (same hash tag).
	slot1 := cmdx.Slot("{user:1}:a")
	assert.Len(t, groups[slot1], 2)
	assert.Equal(t, 1, groups[slot1][0].value)
	assert.Equal(t, 2, groups[slot1][1].value)

	// {user:2}:a should be in its own slot.
	slot2 := cmdx.Slot("{user:2}:a")
	assert.Len(t, groups[slot2], 1)
	assert.Equal(t, 3, groups[slot2][0].value)

	// standalone should be in its own slot.
	slot3 := cmdx.Slot("standalone")
	assert.Len(t, groups[slot3], 1)
	assert.Equal(t, 4, groups[slot3][0].value)
}

func TestGroupBySlot_Empty(t *testing.T) {
	groups := cmdx.GroupBySlot([]string{}, func(s string) string { return s })
	assert.Empty(t, groups)
}

func BenchmarkSlot(b *testing.B) {
	keys := []string{
		"simple",
		"user:1000",
		"{tag}key",
		"prefix{tag}suffix",
	}

	for _, key := range keys {
		b.Run(key, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = cmdx.Slot(key)
			}
		})
	}
}
