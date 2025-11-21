// Package errs provides common error definitions used throughout redcache.
package errs

import (
	"errors"
)

// Common errors used by lock management operations.
var (
	// ErrLockFailed is returned when a lock cannot be acquired due to contention.
	ErrLockFailed = errors.New("lock acquisition failed")

	// ErrLockLost indicates the distributed lock was lost or expired before the value could be set.
	// This can occur if the lock TTL expires during callback execution or if Redis invalidates the lock.
	ErrLockLost = errors.New("lock was lost or expired before value could be set")
)
