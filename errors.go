package redcache

import (
	"errors"
	"fmt"
	"strings"
)

// ErrLockLost indicates the distributed lock was lost or expired before the value could be set.
// This can occur if the lock TTL expires during callback execution or if Redis invalidates the lock.
var ErrLockLost = errors.New("lock was lost or expired before value could be set")

// BatchError represents partial failures in a multi-key operation.
// Some keys may have succeeded while others failed.
type BatchError struct {
	// Failed maps each failed key to its error.
	Failed map[string]error
	// Succeeded lists the keys that were set successfully.
	Succeeded []string
}

// Error returns a human-readable summary of the batch failure.
func (e *BatchError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "batch operation partially failed: %d succeeded, %d failed", len(e.Succeeded), len(e.Failed))
	for key, err := range e.Failed {
		fmt.Fprintf(&b, "; key %q: %s", key, err)
	}
	return b.String()
}

// HasFailures returns true if any keys failed.
func (e *BatchError) HasFailures() bool {
	return len(e.Failed) > 0
}

// NewBatchError creates a BatchError from the given failures and successes.
// Returns nil (untyped) if there are no failures, so it is safe to return
// directly as an error interface value.
func NewBatchError(failed map[string]error, succeeded []string) error {
	if len(failed) == 0 {
		return nil
	}
	return &BatchError{
		Failed:    failed,
		Succeeded: succeeded,
	}
}
