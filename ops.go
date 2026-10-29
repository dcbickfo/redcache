package redcache

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/rueidis"
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
	cmds := make(rueidis.Commands, 0, len(keys))
	for _, key := range keys {
		cmds = append(cmds, rca.client.B().Del().Key(key).Build())
	}
	var first firstKeyErr
	for i, resp := range rca.client.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			rca.logger.Error("DelMulti key failed", "key", keys[i], "error", err)
			rca.emitRedisError("del")
			first.add(keys[i], err)
		}
	}
	return first.wrap("del")
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

// touchMulti is touch over many keys in one pipeline. Per-key errors are
// logged; the first is returned.
func (rca *cacheAside) touchMulti(ctx context.Context, ttl time.Duration, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	execsP := luaExecPool.Get(len(keys))
	defer luaExecPool.Put(execsP)
	execs := *execsP
	for i, k := range keys {
		execs[i] = rueidis.LuaExec{Keys: []string{k}, Args: []string{ttlMs, rca.lockPrefix}}
	}
	var first firstKeyErr
	for i, resp := range touchScript.ExecMulti(ctx, rca.client, execs...) {
		if err := resp.Error(); err != nil {
			rca.logger.Error("TouchMulti key failed", "key", keys[i], "error", err)
			rca.emitRedisError("touch")
			first.add(keys[i], err)
		}
	}
	return first.wrap("touch")
}
