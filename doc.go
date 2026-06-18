// Package redcache is a typed cache-aside for Redis, built on the rueidis
// client. It combines rueidis client-side caching with distributed SET NX
// locking so that, across every process, only one caller populates a missing
// key while the rest wait on the invalidation push for the populated value —
// a stampede-resistant cache behind a single generic Cache[K, V] interface.
//
// # How it works
//
// A read registers an in-process lock entry, then issues a client-side-cached
// GET. That GET serves a dual purpose: it returns any cached value and
// subscribes the connection to Redis invalidation for the key. On a miss, one
// caller wins a distributed lock (SET NX with a UUIDv7 value), runs the loader,
// and atomically replaces the lock with the real value via a Lua CAS. Every
// other caller — in this process or another — waits for the resulting
// invalidation, with jittered polling as a fallback until the lock TTL, and then
// reads the populated value. In-process leader/follower coordination collapses a
// thundering herd on one key to a single Redis SET NX.
//
// # Choosing a constructor
//
// Open one [Conn] (it owns the rueidis client and invalidation stream), then
// derive typed views over it that all share that single client:
//
//   - [NewString] — Cache[string, V]: string keys, typed values. The common case.
//   - [NewBytes]  — Cache[string, []byte]: zero-copy opaque payloads.
//   - [New]       — Cache[K, V]: typed keys via a [KeyCodec] (and typed values).
//
// The views are operations-only; lifecycle (Close) and the raw-client escape
// hatch (Client) live on the [Conn]. Deriving a view does no I/O, returns no
// error, and panics on a nil codec.
//
// # Minimal example
//
//	conn, err := redcache.Open(
//		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
//	)
//	if err != nil {
//		return err
//	}
//	defer conn.Close()
//
//	cache := redcache.NewString[string](conn, redcache.StringCodec{})
//
//	v, err := cache.Get(ctx, time.Minute, "k", func(ctx context.Context, key string) (string, error) {
//		return loadFromUpstream(ctx, key) // runs only on a miss, once per key
//	})
//
// # Beyond read-through
//
// Every Cache also supports write-through priming — Set, ForceSet, and Touch
// (with Multi variants) — for populating or extending entries without a
// read-through miss. Opt into background refresh of stale-but-valid entries with
// [WithRefreshAfterFraction] (and [WithRefreshBeta] for XFetch-style
// probabilistic early expiration). Wire counters through [WithMetrics].
//
// # Values, codecs, and absence
//
// Values are stored through a [Codec]; keys through a [KeyCodec]. JSONCodec is
// the typical choice; StringCodec and UnsafeBytesCodec are identity codecs (the
// latter zero-copy, returning borrowed memory). Decode failures on read are
// wrapped with [ErrDecode]. Absence semantics are the caller's to own — there is
// no ErrNotFound sentinel; cache a *T, sql.Null[T], or a domain sentinel inside
// V to represent "not found" and avoid cache penetration.
package redcache
