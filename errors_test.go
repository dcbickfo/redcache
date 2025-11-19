package redcache_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dcbickfo/redcache"
)

func TestBatchError_Error(t *testing.T) {
	tests := []struct {
		name      string
		failed    map[string]error
		succeeded []string
		expected  string
	}{
		{
			name:      "no failures",
			failed:    map[string]error{},
			succeeded: []string{"key1", "key2"},
			expected:  "no failures in batch operation",
		},
		{
			name: "partial failure",
			failed: map[string]error{
				"key1": errors.New("error 1"),
				"key2": errors.New("error 2"),
			},
			succeeded: []string{"key3", "key4", "key5"},
			expected:  "batch operation partially failed: 2/5 keys failed",
		},
		{
			name: "all failed",
			failed: map[string]error{
				"key1": errors.New("error 1"),
				"key2": errors.New("error 2"),
			},
			succeeded: []string{},
			expected:  "batch operation partially failed: 2/2 keys failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redcache.NewBatchError(tt.failed, tt.succeeded)
			assert.Equal(t, tt.expected, err.Error())
		})
	}
}

func TestBatchError_HasFailures(t *testing.T) {
	tests := []struct {
		name     string
		failed   map[string]error
		expected bool
	}{
		{
			name:     "no failures",
			failed:   map[string]error{},
			expected: false,
		},
		{
			name: "has failures",
			failed: map[string]error{
				"key1": errors.New("error"),
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redcache.NewBatchError(tt.failed, []string{"key2"})
			assert.Equal(t, tt.expected, err.HasFailures())
		})
	}
}

func TestBatchError_AllSucceeded(t *testing.T) {
	tests := []struct {
		name     string
		failed   map[string]error
		expected bool
	}{
		{
			name:     "all succeeded",
			failed:   map[string]error{},
			expected: true,
		},
		{
			name: "some failed",
			failed: map[string]error{
				"key1": errors.New("error"),
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redcache.NewBatchError(tt.failed, []string{"key2"})
			assert.Equal(t, tt.expected, err.AllSucceeded())
		})
	}
}

func TestBatchError_FailureRate(t *testing.T) {
	tests := []struct {
		name      string
		failed    map[string]error
		succeeded []string
		expected  float64
	}{
		{
			name:      "no operations",
			failed:    map[string]error{},
			succeeded: []string{},
			expected:  0.0,
		},
		{
			name:      "all succeeded",
			failed:    map[string]error{},
			succeeded: []string{"key1", "key2", "key3"},
			expected:  0.0,
		},
		{
			name: "all failed",
			failed: map[string]error{
				"key1": errors.New("error 1"),
				"key2": errors.New("error 2"),
			},
			succeeded: []string{},
			expected:  1.0,
		},
		{
			name: "50% failure rate",
			failed: map[string]error{
				"key1": errors.New("error 1"),
				"key2": errors.New("error 2"),
			},
			succeeded: []string{"key3", "key4"},
			expected:  0.5,
		},
		{
			name: "25% failure rate",
			failed: map[string]error{
				"key1": errors.New("error 1"),
			},
			succeeded: []string{"key2", "key3", "key4"},
			expected:  0.25,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redcache.NewBatchError(tt.failed, tt.succeeded)
			assert.InDelta(t, tt.expected, err.FailureRate(), 0.001)
		})
	}
}

func TestNewBatchError(t *testing.T) {
	failed := map[string]error{
		"key1": errors.New("error 1"),
	}
	succeeded := []string{"key2", "key3"}

	err := redcache.NewBatchError(failed, succeeded)

	assert.NotNil(t, err)
	assert.Equal(t, failed, err.Failed)
	assert.Equal(t, succeeded, err.Succeeded)
}
