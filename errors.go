package redcache

import (
	"errors"
	"fmt"
)

// Common errors returned by redcache operations.
var (
	// ErrNoKeys is returned when an operation is called with an empty key list.
	ErrNoKeys = errors.New("no keys provided")

	// ErrLockFailed is returned when a lock cannot be acquired.
	ErrLockFailed = errors.New("failed to acquire lock")

	// ErrLockLost indicates the distributed lock was lost or expired before the value could be set.
	// This can occur if the lock TTL expires during callback execution or if Redis invalidates the lock.
	ErrLockLost = errors.New("lock was lost or expired before value could be set")

	// ErrInvalidTTL is returned when a TTL value is invalid (e.g., negative or zero).
	ErrInvalidTTL = errors.New("invalid TTL value")

	// ErrNilCallback is returned when a required callback function is nil.
	ErrNilCallback = errors.New("callback function cannot be nil")
)

// BatchError represents an error that occurred during a batch operation.
// It tracks which keys succeeded and which failed, along with the specific errors.
type BatchError struct {
	// Failed maps failed keys to their specific errors
	Failed map[string]error

	// Succeeded lists keys that completed successfully
	Succeeded []string
}

// Error implements the error interface for BatchError.
func (e *BatchError) Error() string {
	if len(e.Failed) == 0 {
		return "no failures in batch operation"
	}

	total := len(e.Failed) + len(e.Succeeded)
	return fmt.Sprintf("batch operation partially failed: %d/%d keys failed", len(e.Failed), total)
}

// HasFailures returns true if any keys failed in the batch operation.
func (e *BatchError) HasFailures() bool {
	return len(e.Failed) > 0
}

// AllSucceeded returns true if all keys in the batch succeeded.
func (e *BatchError) AllSucceeded() bool {
	return len(e.Failed) == 0
}

// FailureRate returns the percentage of keys that failed (0.0 to 1.0).
func (e *BatchError) FailureRate() float64 {
	total := len(e.Failed) + len(e.Succeeded)
	if total == 0 {
		return 0.0
	}
	return float64(len(e.Failed)) / float64(total)
}

// NewBatchError creates a new BatchError with the provided failed and succeeded keys.
func NewBatchError(failed map[string]error, succeeded []string) *BatchError {
	return &BatchError{
		Failed:    failed,
		Succeeded: succeeded,
	}
}
