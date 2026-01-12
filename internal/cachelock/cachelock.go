// Package cachelock provides lock management for cache-aside operations.
// It abstracts the "register + try-lock + CAS commit" pattern used in cache coordination.
package cachelock

import (
	"context"
	"time"
)

// LockMode specifies the type of lock to acquire.
type LockMode int

const (
	// LockModeRead is used for Get operations.
	// Uses SET NX - fails if any value (lock or real) exists.
	// This ensures only one process populates the cache on miss.
	LockModeRead LockMode = iota

	// LockModeWrite is used for Set operations.
	// Uses Lua script that can overwrite real values but not active locks.
	// This allows Set to update existing cache entries while respecting ongoing operations.
	LockModeWrite
)

// LockResult contains the result of a TryAcquire operation.
type LockResult struct {
	// Acquired maps keys to their lock values for successfully acquired locks.
	// These locks must be either Committed or Released.
	Acquired map[string]string

	// CachedValues contains values for keys that had cache hits.
	// These keys don't need lock handling - values are already available.
	CachedValues map[string]string

	// WaitChans provides channels for keys that couldn't be acquired.
	// Wait on these channels to be notified when locks are released.
	WaitChans map[string]<-chan struct{}

	// SavedValues contains previous values for keys acquired in LockModeWrite.
	// Used for rollback if lock acquisition partially fails.
	// Only populated when using LockModeWrite.
	SavedValues map[string]string
}

// Manager handles distributed lock coordination for cache operations.
// It manages CSC (Client-Side Caching) subscriptions and lock lifecycle.
//
// The typical flow is:
//  1. TryAcquire - attempt to lock keys, get cache hits, subscribe to locked keys
//  2. If acquired: execute callback, then Commit
//  3. If not acquired: WaitForRelease, then retry TryAcquire
//  4. On error: Release any acquired locks
type Manager interface {
	// TryAcquire attempts to acquire locks for the given keys.
	//
	// For LockModeRead:
	//   - Returns cached values for cache hits
	//   - Acquires locks using SET NX for cache misses
	//   - Returns wait channels for keys locked by other processes
	//
	// For LockModeWrite:
	//   - Waits for any read locks to complete first
	//   - Acquires write locks (can overwrite values, not locks)
	//   - Saves previous values for potential rollback
	//
	// The caller MUST either Commit or Release all acquired locks.
	TryAcquire(ctx context.Context, keys []string, mode LockMode) (*LockResult, error)

	// Commit atomically writes values for keys where we still hold locks.
	// Uses CAS (compare-and-swap) to verify lock ownership before writing.
	//
	// Returns the list of keys that were successfully committed.
	// Keys not in the returned list had their locks stolen (e.g., by ForceSet).
	//
	// After Commit, the locks are consumed - no need to call Release for
	// successfully committed keys.
	Commit(ctx context.Context, lockValues map[string]string, values map[string]string, ttl time.Duration) ([]string, error)

	// Release abandons locks without writing values.
	// For LockModeWrite, optionally restores previous values.
	//
	// This should be called when:
	//   - Callback returns an error
	//   - Context is cancelled
	//   - Partial lock acquisition in multi-key operations
	Release(ctx context.Context, lockValues map[string]string)

	// ReleaseWithRestore releases locks and restores saved values.
	// Used when rolling back LockModeWrite acquisitions to prevent cache misses.
	ReleaseWithRestore(ctx context.Context, lockValues map[string]string, savedValues map[string]string)

	// WaitForRelease waits for any of the given channels to signal.
	// Returns when at least one lock is released or context is cancelled.
	WaitForRelease(ctx context.Context, waitChans map[string]<-chan struct{}) error

	// Close cleans up resources (cancels pending wait channels, etc).
	Close()
}
