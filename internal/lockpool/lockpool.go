// Package lockpool provides fast lock value generation using an atomic counter
// and a per-instance UUID prefix.
package lockpool

import (
	"strconv"
	"sync/atomic"

	"github.com/google/uuid"
)

// Pool generates unique lock values by combining a fixed instance UUID with an
// atomic counter. This avoids calling uuid.NewV7() per lock, which is expensive
// under high concurrency.
type Pool struct {
	prefix     string
	instanceID string
	counter    atomic.Uint64
}

// New creates a Pool with the given lock prefix (e.g., "__redcache:lock:").
func New(prefix string) (*Pool, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	return &Pool{
		prefix:     prefix,
		instanceID: id.String(),
	}, nil
}

// Generate returns a unique lock value: prefix + instanceID + ":" + counter.
func (p *Pool) Generate() string {
	n := p.counter.Add(1)
	return p.prefix + p.instanceID + ":" + strconv.FormatUint(n, 10)
}
