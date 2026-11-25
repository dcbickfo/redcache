package redcache

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"
)

const (
	writeMarkTTL = time.Second
	writeMarkCap = 1 << 20
)

type writeMark struct {
	inFlight    int
	deadline    time.Time
	invalidated bool
	overlapped  bool
	failed      bool
}

// writeTicket describes a batch's tracked prefix. The remaining keys use the
// global bypass. A reset moves every outstanding ticket to the global bypass.
type writeTicket struct {
	epoch uint64
	keys  int
}

type writeMarks struct {
	active atomic.Bool
	mu     sync.RWMutex
	marks  map[string]writeMark

	epoch       uint64
	inFlight    int
	global      int
	bypassUntil time.Time
	nextSweep   time.Time

	invalidated int64
	expired     int64
	atCap       int64
}

func (m *writeMarks) begin(keys []string, now time.Time, limit int) writeTicket {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.active.Store(true)
	m.inFlight++
	if m.marks == nil {
		m.marks = make(map[string]writeMark)
	}
	if !now.Before(m.nextSweep) {
		m.sweep(now)
	}
	for i, key := range keys {
		mark, exists := m.marks[key]
		if !exists && len(m.marks) == limit {
			m.global++
			m.atCap += int64(len(keys) - i)
			return writeTicket{epoch: m.epoch, keys: i}
		}
		if mark.inFlight == 0 {
			mark = writeMark{}
		} else {
			mark.overlapped = true
		}
		mark.inFlight++
		m.marks[key] = mark
	}
	return writeTicket{epoch: m.epoch, keys: len(keys)}
}

func (m *writeMarks) finish(keys []string, ticket writeTicket, now time.Time, succeeded func(int) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inFlight--
	if ticket.epoch != m.epoch || ticket.keys != len(keys) {
		m.global--
		m.extendGlobalBypass(now)
	}
	if ticket.epoch == m.epoch {
		for i, key := range keys[:ticket.keys] {
			m.finishKey(key, now, succeeded(i))
		}
	}
	m.updateActive()
}

func (m *writeMarks) finishKey(key string, now time.Time, succeeded bool) {
	mark := m.marks[key]
	mark.inFlight--
	mark.failed = mark.failed || !succeeded
	if mark.inFlight == 0 {
		if mark.invalidated && !mark.overlapped && !mark.failed {
			m.removeInvalidated(key)
			return
		}
		mark.deadline = now.Add(writeMarkTTL)
	}
	m.marks[key] = mark
}

func (m *writeMarks) invalidate(key string) {
	if !m.active.Load() {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	mark, exists := m.marks[key]
	switch {
	case !exists || mark.failed:
	case mark.inFlight == 0:
		m.removeInvalidated(key)
		m.updateActive()
	default:
		// rueidis already evicted the old value. Remember the push until the
		// reply arrives. If writes overlap, a push may belong to an earlier one.
		mark.invalidated = true
		m.marks[key] = mark
	}
}

func (m *writeMarks) reset(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.marks) > 0 {
		// A cluster node or a multiplexed pipe can flush its own cache only.
		// Keep the same grace period for keys past the cleared pipe.
		m.extendGlobalBypass(now)
	}
	m.invalidated += int64(len(m.marks))
	m.marks = nil
	m.epoch++
	m.global = m.inFlight
	m.updateActive()
}

func (m *writeMarks) bypass(key string) bool {
	if !m.active.Load() {
		return false
	}
	return m.bypassAt(key, time.Now())
}

func (m *writeMarks) bypassAt(key string, now time.Time) bool {
	m.mu.RLock()
	until := m.bypassUntil
	global := m.global > 0 || now.Before(until)
	mark, exists := m.marks[key]
	m.mu.RUnlock()
	if global || (exists && (mark.inFlight > 0 || now.Before(mark.deadline))) {
		return true
	}
	if exists || !until.IsZero() {
		return m.expireForRead(key, now)
	}
	return false
}

func (m *writeMarks) expireForRead(key string, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.global > 0 || now.Before(m.bypassUntil) {
		return true
	}
	m.bypassUntil = time.Time{}
	mark, exists := m.marks[key]
	if exists && (mark.inFlight > 0 || now.Before(mark.deadline)) {
		return true
	}
	if exists {
		delete(m.marks, key)
		m.expired++
	}
	m.updateActive()
	return false
}

func (m *writeMarks) sweep(now time.Time) {
	for key, mark := range m.marks {
		if mark.inFlight == 0 && !now.Before(mark.deadline) {
			delete(m.marks, key)
			m.expired++
		}
	}
	m.nextSweep = now.Add(writeMarkTTL)
}

func (m *writeMarks) removeInvalidated(key string) {
	delete(m.marks, key)
	m.invalidated++
}

func (m *writeMarks) updateActive() {
	m.active.Store(len(m.marks) > 0 || m.global > 0 || !m.bypassUntil.IsZero())
}

func (m *writeMarks) extendGlobalBypass(now time.Time) {
	if until := now.Add(writeMarkTTL); until.After(m.bypassUntil) {
		m.bypassUntil = until
	}
}

func successfulReplies(resps []rueidis.RedisResult, wrote func(rueidis.RedisResult) bool) func(int) bool {
	return func(i int) bool { return i < len(resps) && wrote(resps[i]) }
}

func writeReplied(resp rueidis.RedisResult) bool { return resp.Error() == nil }

func casWrote(resp rueidis.RedisResult) bool {
	n, err := resp.AsInt64()
	return err == nil && n != 0
}
