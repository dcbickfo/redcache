package redcache

import (
	"context"
	"time"

	"github.com/redis/rueidis"
)

// bypassTTLs contains Redis TTLs for the plain reads, in their dispatch order.
// A nil slice means refresh-ahead did not ask for TTLs.
type bypassTTLs []int64

func (t bypassTTLs) at(i int) int64 {
	if t == nil {
		return 0
	}
	return t[i]
}

// bypassReads gets values directly from Redis. Opting in on the same pipe
// re-arms the invalidation for a later write, even if all current reads bypass
// the local cache. No bypass reply is installed in rueidis's cache.
func (rca *cacheAside) bypassReads(ctx context.Context, keys []string, refreshTTL bool) ([]rueidis.RedisResult, bypassTTLs) {
	getOffset := 0
	if rca.trackBypasses {
		getOffset = 1
	}
	stride := 1 + getOffset
	if refreshTTL {
		stride++
	}
	cmds := make(rueidis.Commands, 0, stride*len(keys))
	for _, key := range keys {
		if rca.trackBypasses {
			cmds = append(cmds, rca.client.B().ClientCaching().Yes().Build().SetSlot(key))
		}
		cmds = append(cmds, rca.client.B().Get().Key(key).Build())
		if refreshTTL {
			cmds = append(cmds, rca.client.B().Pttl().Key(key).Build())
		}
	}
	var barriers []readBarrier
	if rca.trackBypasses && rca.writeMarks.onTrackingPipe(len(cmds)) {
		barriers = rca.writeMarks.readBarriers(keys, len(cmds), time.Now())
	}
	// CLIENT CACHING YES is not retryable in rueidis. Its presence makes the
	// standalone pipeline non-retryable. The config check excludes ConnLifetime
	// (which otherwise retries expired pipes) and all implicit routing changes.
	replies := rca.client.DoMulti(ctx, cmds...)
	if len(replies) != len(cmds) {
		return nil, nil
	}
	return rca.unpackBypassReads(keys, replies, barriers, stride, getOffset, refreshTTL)
}

func (rca *cacheAside) unpackBypassReads(keys []string, replies []rueidis.RedisResult, barriers []readBarrier, stride, getOffset int, refreshTTL bool) ([]rueidis.RedisResult, bypassTTLs) {
	var ttls bypassTTLs
	if refreshTTL {
		ttls = make(bypassTTLs, len(keys))
	}
	for i := range keys {
		index := i*stride + getOffset
		if barriers != nil && replies[index-1].Error() == nil && replies[index].Error() == nil {
			// The write had completed before dispatch. The read loop delivered
			// any preceding push on this pipe before delivering the GET reply.
			rca.writeMarks.finishBarrier(keys[i], barriers[i])
		}
		replies[i] = replies[index]
		if refreshTTL {
			ttl, err := replies[index+1].AsInt64()
			if err == nil {
				ttls[i] = ttl
			}
		}
	}
	return replies[:len(keys)], ttls
}
