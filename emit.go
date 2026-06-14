package redcache

import (
	"time"
)

func (rca *cacheAside) emitCacheHits(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.CacheHits(int64(n))
	}
}

func (rca *cacheAside) emitCacheMisses(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.CacheMisses(int64(n))
	}
}

func (rca *cacheAside) emitLockWaitDuration(d time.Duration) {
	if rca.metricsEnabled {
		rca.metrics.LockWaitDuration(d)
	}
}

func (rca *cacheAside) emitLoaderDuration(d time.Duration) {
	if rca.metricsEnabled {
		rca.metrics.LoaderDuration(d)
	}
}

func (rca *cacheAside) emitLoaderErrors(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.LoaderErrors(int64(n))
	}
}

func (rca *cacheAside) emitRedisError(op string) {
	if rca.metricsEnabled {
		rca.metrics.RedisError(op)
	}
}

func (rca *cacheAside) emitLockContended(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.LockContended(int64(n))
	}
}

func (rca *cacheAside) emitRefreshTriggered(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.RefreshTriggered(int64(n))
	}
}

func (rca *cacheAside) emitRefreshSkipped(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.RefreshSkipped(int64(n))
	}
}

func (rca *cacheAside) emitRefreshDropped(n int) {
	if rca.metricsEnabled && n > 0 {
		rca.metrics.RefreshDropped(int64(n))
	}
}

func (rca *cacheAside) emitLockLost(key string) {
	if rca.metricsEnabled {
		rca.metrics.LockLost(key)
	}
}

func (rca *cacheAside) emitRefreshError(key string) {
	if rca.metricsEnabled {
		rca.metrics.RefreshError(key)
	}
}

func (rca *cacheAside) emitRefreshPanicked(key string) {
	if rca.metricsEnabled {
		rca.metrics.RefreshPanicked(key)
	}
}

func (rca *cacheAside) emitInvalidationError() {
	if rca.metricsEnabled {
		rca.metrics.InvalidationError()
	}
}
