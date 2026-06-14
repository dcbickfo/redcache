package redcache

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache/internal/cmdx"
)

// del removes a key, triggering invalidation on all subscribed clients.
func (rca *cacheAside) del(ctx context.Context, key string) error {
	if err := rca.client.Do(ctx, rca.client.B().Del().Key(key).Build()).Error(); err != nil {
		rca.emitRedisError("del")
		return fmt.Errorf("del key %q: %w", key, err)
	}
	return nil
}

// delMulti deletes keys. Per-key errors are logged; the first is returned.
func (rca *cacheAside) delMulti(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	cmdsP := commandsPool.GetCap(len(keys))
	defer commandsPool.Put(cmdsP)
	for _, key := range keys {
		*cmdsP = append(*cmdsP, rca.client.B().Del().Key(key).Build())
	}
	resps := rca.client.DoMulti(ctx, *cmdsP...)
	var firstErr error
	var firstErrKey string
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			rca.logger.Error("DelMulti key failed", "key", keys[i], "error", err)
			rca.emitRedisError("del")
			if firstErr == nil {
				firstErr = err
				firstErrKey = keys[i]
			}
		}
	}
	if firstErr != nil {
		return fmt.Errorf("del key %q: %w", firstErrKey, firstErr)
	}
	return nil
}

// touch extends a cached value's TTL via PEXPIRE. No-ops on missing key or
// lock value (so it can't extend an in-flight lock). PEXPIRE emits a CSC
// invalidation, so clients caching the key re-fetch it (observing the new
// TTL) on their next read.
func (rca *cacheAside) touch(ctx context.Context, ttl time.Duration, key string) error {
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	if err := touchScript.Exec(ctx, rca.client, []string{key}, []string{ttlMs, rca.lockPrefix}).Error(); err != nil {
		rca.emitRedisError("touch")
		return fmt.Errorf("touch key %q: %w", key, err)
	}
	return nil
}

// touchMulti is touch over many keys. Per-key errors are logged; the first
// is returned.
func (rca *cacheAside) touchMulti(ctx context.Context, ttl time.Duration, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	stmtsBySlot := rca.groupTouchExecs(ttl, keys)
	if firstErrKey, firstErr := rca.runTouchSlots(ctx, stmtsBySlot); firstErr != nil {
		return fmt.Errorf("touch key %q: %w", firstErrKey, firstErr)
	}
	return nil
}

type touchExec struct {
	key  string
	exec rueidis.LuaExec
}

func (rca *cacheAside) groupTouchExecs(ttl time.Duration, keys []string) map[uint16][]touchExec {
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	stmtsBySlot := make(map[uint16][]touchExec)
	for _, k := range keys {
		slot := cmdx.Slot(k)
		stmtsBySlot[slot] = append(stmtsBySlot[slot], touchExec{
			key: k,
			exec: rueidis.LuaExec{
				Keys: []string{k},
				Args: []string{ttlMs, rca.lockPrefix},
			},
		})
	}
	return stmtsBySlot
}

func (rca *cacheAside) runTouchSlots(ctx context.Context, slots map[uint16][]touchExec) (string, error) {
	var (
		mu          sync.Mutex
		wg          sync.WaitGroup
		firstErr    error
		firstErrKey string
	)
	for _, stmts := range slots {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key, err := rca.touchSlot(ctx, stmts)
			if err == nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if firstErr == nil {
				firstErr, firstErrKey = err, key
			}
		}()
	}
	wg.Wait()
	return firstErrKey, firstErr
}

func (rca *cacheAside) touchSlot(ctx context.Context, stmts []touchExec) (string, error) {
	execsP := luaExecPool.Get(len(stmts))
	defer luaExecPool.Put(execsP)
	execs := *execsP
	for i, s := range stmts {
		execs[i] = s.exec
	}
	resps := touchScript.ExecMulti(ctx, rca.client, execs...)
	var firstErr error
	var firstErrKey string
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			rca.logger.Error("TouchMulti key failed", "key", stmts[i].key, "error", err)
			rca.emitRedisError("touch")
			if firstErr == nil {
				firstErr, firstErrKey = err, stmts[i].key
			}
		}
	}
	return firstErrKey, firstErr
}
