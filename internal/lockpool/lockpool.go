package lockpool

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

// Pool manages lock value generation using atomic counter + instance ID.
// This is significantly faster than UUID generation while still guaranteeing uniqueness.
//
// Lock values have the format: prefix + instanceID + ":" + counter
// Example: "__redcache:lock:550e8400-e29b-41d4-a716-446655440000:42"
//
// Uniqueness guarantees:
// - instanceID is a full UUID generated once at process startup (128-bit uniqueness)
// - counter is monotonically increasing within that process
// - Combined, they provide globally unique lock values even across millions of instances.
//
// Counter overflow:
// The atomic.Uint64 counter will wrap to 0 after reaching MaxUint64 (18,446,744,073,709,551,615).
// This is not a practical concern because even at extreme load (100M locks/sec), it would take
// 5,850+ years to overflow. In practice, processes restart regularly (deployments, crashes),
// and each restart generates a new UUID instance ID, ensuring continued uniqueness.
type Pool struct {
	prefix     string
	instanceID string // Full UUID string (36 chars)
	counter    atomic.Uint64
	pool       sync.Pool // Pool of strings.Builder for string construction
}

// New creates a new pool of lock values.
// The poolSize parameter is ignored but kept for API compatibility.
func New(prefix string, _ int) *Pool {
	// Generate full UUID instance ID once at startup for uniqueness
	id, _ := uuid.NewV7()
	instanceID := id.String() // Full 36-char UUID

	p := &Pool{
		prefix:     prefix,
		instanceID: instanceID,
	}
	p.pool.New = func() interface{} {
		// Pre-allocate for prefix + full UUID (36) + ":" + counter (max 20 digits)
		var sb strings.Builder
		sb.Grow(len(prefix) + 36 + 1 + 20)
		return &sb
	}
	return p
}

// Get returns a lock value from the pool.
// This is ~10x faster than UUID generation while maintaining uniqueness.
func (p *Pool) Get() string {
	sb := p.pool.Get().(*strings.Builder)
	sb.Reset()

	// Build: prefix + instanceID + ":" + counter
	sb.WriteString(p.prefix)
	sb.WriteString(p.instanceID)
	sb.WriteString(":")
	sb.WriteString(strconv.FormatUint(p.counter.Add(1), 10))

	result := sb.String()

	// Return builder to pool
	p.pool.Put(sb)

	return result
}
