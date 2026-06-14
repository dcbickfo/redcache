package redcache

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/rueidis"
)

const (
	// DefaultLockPrefix tags in-Redis lock values so reads recognise a lock as
	// a miss.
	DefaultLockPrefix = "__redcache:lock:"
	// DefaultRefreshPrefix prefixes refresh-ahead dedup lock keys.
	DefaultRefreshPrefix = "__redcache:refresh:"
)

// config is the internal source of truth for cache construction. It is
// populated by the functional Option values and then validated/defaulted by
// applyDefaults. Field meanings match the With* option docs.
type config struct {
	lockTTL              time.Duration
	clientBuilder        func(option rueidis.ClientOption) (rueidis.Client, error)
	logger               Logger
	metrics              Metrics
	lockPrefix           string
	refreshLockPrefix    string
	refreshAfterFraction float64
	refreshBeta          float64
	refreshTimeout       time.Duration
	refreshWorkers       int
	refreshQueueSize     int
}

// Option configures a cache. Options are applied in order; later wins.
type Option func(*config)

// WithLockTTL bounds both how long a Redis lock survives and how long callers
// wait for one. Defaults to 10s; values below 100ms are rejected.
func WithLockTTL(d time.Duration) Option {
	return func(c *config) { c.lockTTL = d }
}

// WithLogger sets the logger. Defaults to slog.Default().
func WithLogger(l Logger) Option {
	return func(c *config) { c.logger = l }
}

// Logger is the slog-shaped subset the cache calls into. *slog.Logger satisfies it.
type Logger interface {
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
}

// WithMetrics sets the metrics sink. Defaults to NoopMetrics. Methods run on
// the hot path; impls must be concurrent-safe.
func WithMetrics(m Metrics) Option {
	return func(c *config) { c.metrics = m }
}

// WithLockPrefix sets the in-Redis prefix tagged onto lock values so reads can
// recognise a lock as a miss. Defaults to DefaultLockPrefix.
func WithLockPrefix(p string) Option {
	return func(c *config) { c.lockPrefix = p }
}

// WithRefreshLockPrefix sets the prefix for refresh-ahead dedup keys. Defaults
// to DefaultRefreshPrefix. The data key is wrapped in a hash tag so the refresh
// lock hashes to the same cluster slot.
func WithRefreshLockPrefix(p string) Option {
	return func(c *config) { c.refreshLockPrefix = p }
}

// WithRefreshAfterFraction enables refresh-ahead. Reads with remaining TTL
// below (1 - fraction) * ttl may trigger a background refresh while still
// returning the cached value. Must be in [0, 1); 0 disables.
func WithRefreshAfterFraction(f float64) Option {
	return func(c *config) { c.refreshAfterFraction = f }
}

// WithRefreshBeta enables XFetch-style probabilistic sampling within the
// refresh window, weighting by recorded compute time so slow values get more
// headroom. 0 (default) = always refresh below the floor; 1.0 matches
// canonical XFetch (Vattani et al). Multi-key writes record fn duration
// divided evenly across returned values.
func WithRefreshBeta(b float64) Option {
	return func(c *config) { c.refreshBeta = b }
}

// WithRefreshTimeout bounds how long a refresh-ahead callback may run. Defaults
// to the data ttl passed to Get/GetMulti (not LockTTL). The refresh lock itself
// still uses LockTTL; the back-write that records a slow-but-successful result
// is decoupled from this timeout so it is not lost.
func WithRefreshTimeout(d time.Duration) Option {
	return func(c *config) { c.refreshTimeout = d }
}

// WithRefreshWorkers sets the refresh worker pool size. Defaults to 4.
func WithRefreshWorkers(n int) Option {
	return func(c *config) { c.refreshWorkers = n }
}

// WithRefreshQueueSize bounds pending refresh jobs; over-full drops silently.
// Defaults to 64.
func WithRefreshQueueSize(n int) Option {
	return func(c *config) { c.refreshQueueSize = n }
}

// WithClientBuilder overrides rueidis.NewClient when building the internal
// client. Useful as a test seam.
func WithClientBuilder(b func(option rueidis.ClientOption) (rueidis.Client, error)) Option {
	return func(c *config) { c.clientBuilder = b }
}

// newConfig applies opts onto a zero config (defaults filled in later by
// applyDefaults).
func newConfig(opts ...Option) config {
	var c config
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

func (c *config) applyDefaults(clientOption rueidis.ClientOption) error {
	if len(clientOption.InitAddress) == 0 {
		return errors.New("at least one Redis address must be provided in InitAddress")
	}
	if c.lockTTL < 0 {
		return errors.New("LockTTL must not be negative")
	}
	if c.lockTTL > 0 && c.lockTTL < 100*time.Millisecond {
		return errors.New("LockTTL should be at least 100ms to avoid excessive lock churn")
	}
	if c.lockTTL == 0 {
		c.lockTTL = 10 * time.Second
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}
	if c.metrics == nil {
		c.metrics = NoopMetrics{}
	}
	if c.lockPrefix == "" {
		c.lockPrefix = DefaultLockPrefix
	}
	if c.refreshLockPrefix == "" {
		c.refreshLockPrefix = DefaultRefreshPrefix
	}
	// Reject any LockPrefix that would make the envelope-prefixed value read as a
	// lock — every cached value would then look like a lock and reads would
	// always miss, silently turning every read into a cache miss.
	if strings.HasPrefix(envelopePrefix, c.lockPrefix) {
		return fmt.Errorf("LockPrefix %q conflicts with envelope prefix %q (would mask all cached reads as locks)", c.lockPrefix, envelopePrefix)
	}
	return c.applyRefreshDefaults()
}

func (c *config) applyRefreshDefaults() error {
	if c.refreshAfterFraction < 0 || c.refreshAfterFraction >= 1 {
		return errors.New("RefreshAfterFraction must be in range [0, 1)")
	}
	if c.refreshBeta < 0 {
		return errors.New("RefreshBeta must not be negative")
	}
	if c.refreshTimeout < 0 {
		return errors.New("RefreshTimeout must not be negative")
	}
	if c.refreshAfterFraction == 0 {
		return nil
	}
	if c.refreshWorkers < 0 {
		return errors.New("RefreshWorkers must not be negative")
	}
	if c.refreshQueueSize < 0 {
		return errors.New("RefreshQueueSize must not be negative")
	}
	if c.refreshWorkers == 0 {
		c.refreshWorkers = 4
	}
	if c.refreshQueueSize == 0 {
		c.refreshQueueSize = 64
	}
	return nil
}
