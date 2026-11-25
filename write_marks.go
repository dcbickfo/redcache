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
	noBarrier   bool
	ack         uint64
	wire        uint64
}

// writeTicket describes a batch's tracked prefix. The remaining keys use the
// global bypass. A reset moves every outstanding ticket to the global bypass.
type writeTicket struct {
	epoch uint64
	wire  uint64
	keys  int
}

type readBarrier struct {
	epoch uint64
	wire  uint64
	ack   uint64
}

type writeMarks struct {
	active atomic.Bool
	wire   atomic.Uint64
	mu     sync.RWMutex
	marks  map[string]writeMark
	// Zero disables barriers. A negative limit means rueidis has no batch cutoff.
	pipeLimit int

	epoch       uint64
	ack         uint64
	inFlight    int
	global      int
	bypassUntil time.Time
	nextSweep   time.Time

	invalidated int64
	barrier     int64
	expired     int64
	atCap       int64
	resetCount  int64
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
	ticket := writeTicket{epoch: m.epoch, wire: m.wire.Load(), keys: len(keys)}
	noBarrier := !m.onTrackingPipe(len(keys))
	for i, key := range keys {
		mark, exists := m.marks[key]
		if !exists && len(m.marks) == limit {
			m.global++
			m.atCap += int64(len(keys) - i)
			ticket.keys = i
			return ticket
		}
		if mark.inFlight == 0 {
			mark = writeMark{}
		} else {
			mark.overlapped = true
		}
		mark.inFlight++
		mark.noBarrier = mark.noBarrier || noBarrier
		m.marks[key] = mark
	}
	return ticket
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
		m.ack++
		for i, key := range keys[:ticket.keys] {
			m.finishKey(key, now, succeeded(i), ticket.wire)
		}
	}
	m.updateActive()
}

func (m *writeMarks) finishKey(key string, now time.Time, succeeded bool, wire uint64) {
	mark := m.marks[key]
	mark.inFlight--
	mark.failed = mark.failed || !succeeded
	mark.noBarrier = mark.noBarrier || wire != m.wire.Load()
	if mark.inFlight == 0 {
		if mark.invalidated && !mark.overlapped && !mark.failed {
			m.removeInvalidated(key)
			return
		}
		mark.deadline = now.Add(writeMarkTTL)
		mark.ack, mark.wire = m.ack, wire
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
	m.resetCount += int64(len(m.marks))
	m.marks = nil
	m.epoch++
	m.global = m.inFlight
	m.updateActive()
}

func (m *writeMarks) onTrackingPipe(commands int) bool {
	// At the cutoff either a writer or a bypass batch can use the blocking pool.
	return m.pipeLimit < 0 || commands < m.pipeLimit
}

func (m *writeMarks) readBarriers(keys []string, commands int, now time.Time) []readBarrier {
	if !m.onTrackingPipe(commands) {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.global > 0 || now.Before(m.bypassUntil) {
		return nil
	}
	wire := m.wire.Load()
	var barriers []readBarrier
	for i, key := range keys {
		mark, exists := m.marks[key]
		if exists && mark.inFlight == 0 && !mark.failed && !mark.noBarrier && mark.wire == wire && now.Before(mark.deadline) {
			if barriers == nil {
				barriers = make([]readBarrier, len(keys))
			}
			barriers[i] = readBarrier{epoch: m.epoch, wire: wire, ack: mark.ack}
		}
	}
	return barriers
}

func (m *writeMarks) finishBarrier(key string, barrier readBarrier) {
	if barrier.ack == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	mark, exists := m.marks[key]
	if exists && mark.inFlight == 0 && mark.ack == barrier.ack && m.epoch == barrier.epoch && m.wire.Load() == barrier.wire {
		delete(m.marks, key)
		m.barrier++
		m.updateActive()
	}
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
