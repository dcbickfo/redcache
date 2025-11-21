// Package contextx provides context utilities for the redcache package.
package contextx

import (
	"context"
	"time"
)

// WithCleanupTimeout creates a context for cleanup operations that will not be
// cancelled when the parent context is cancelled. This is useful for ensuring
// cleanup operations (like releasing locks) complete even if the parent context
// times out or is cancelled.
//
// The returned context will have a timeout applied to prevent indefinite blocking.
// The caller must call the returned cancel function to release resources.
//
// Example usage:
//
//	ctx, cancel := contextx.WithCleanupTimeout(parentCtx, 5*time.Second)
//	defer cancel()
//	if err := releaseLock(ctx, lockID); err != nil {
//	    log.Error("failed to release lock", "error", err)
//	}
func WithCleanupTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	// Create a new context that won't be cancelled when parent is cancelled
	cleanupCtx := context.WithoutCancel(parent)

	// Add a timeout to prevent indefinite blocking
	return context.WithTimeout(cleanupCtx, timeout)
}
