package redcache

import (
	"context"

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
	replies := rca.client.DoMulti(ctx, cmds...)
	if len(replies) != len(cmds) {
		return nil, nil
	}
	var ttls bypassTTLs
	if refreshTTL {
		ttls = make(bypassTTLs, len(keys))
	}
	for i := range keys {
		index := i*stride + getOffset
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
