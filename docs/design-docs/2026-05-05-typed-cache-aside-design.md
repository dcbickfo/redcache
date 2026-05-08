# Typed Cache-Aside (Generics + Codec)

**Status:** Draft — pending implementation
**Author:** David
**Date:** 2026-05-05

## Summary

Add typed views over `*CacheAside` and `*PrimeableCacheAside` so callers can store and retrieve arbitrary value types (and arbitrary key types) without hand-serializing at every call site. The existing string-typed public API is preserved unchanged. Generics live at the wrapper boundary only; the hot path inside the library still operates on strings. The lock / refresh / multi-slot machinery is unchanged. The only internal change is a small refactor of the multi-set error collector so it can serve both the legacy `BatchError` and the new `BatchKeyError[K]` from a single source — see **Implementation notes**.

## Goals

- Let users hold typed values (`User`, `Order`, etc.) and typed keys (`UserID`, composite ID structs, etc.) in cache-aside calls.
- Provide a sensible default value codec (JSON) so users who don't care about codecs get a working setup with zero configuration.
- Allow users to plug in their own value or key codec when JSON is wrong (msgpack, protobuf, custom binary, etc.).
- Keep the existing string-based public surface (`*CacheAside`, `*PrimeableCacheAside`, `BatchError`) byte-for-byte source-compatible.
- Keep the existing hot path (lock acquisition, Lua CAS-set, multi-slot batching, refresh-ahead) untouched.

## Non-goals

- Library-level absence / negative-cache semantics. Callers handle missing values via `*T`, `sql.Null[T]`, or a domain-specific sentinel inside `V`. The library does not introduce an `ErrNotFound` sentinel.
- Generic-ifying the underlying `CacheAside` type itself. Methods on the existing string types remain string-typed; users who want type safety construct a `Typed[K,V]` view.
- Any architecture / refactoring work in the existing string code. That work is captured in the **Follow-up work** section and lands after this PR.
- A rich type-safe key model beyond what `KeyCodec[K]` already provides. Users who want algebraic key types build them in their own packages.

## Constraints and prior decisions

The design pins down each of these explicitly. Each was settled during brainstorming.

| Decision | Choice |
|---|---|
| API shape | Wrapper view (`Typed[K,V]` / `PrimeableTyped[K,V]`) over the existing string clients |
| Codec payload | `[]byte` |
| Default value codec | JSON |
| Absence semantics | Caller-owned (no library sentinel) |
| String API after this lands | Keeps both string and typed views exported |
| Generics depth | Boundary only — hot path stays string-based |
| Type parameter count | Two: `K` and `V` |
| Mapping to existing types | Two parallel wrappers (`Typed`, `PrimeableTyped`) mirror `CacheAside` / `PrimeableCacheAside` |
| `KeyCodec.EncodeKey` signature | Returns `(string, error)` |
| Codec shape | Interface (with provided concrete types) |
| String-key convenience | Provide `NewStringTyped` / `NewPrimeableStringTyped` |
| Encode-fail in user fn | Returned as callback error |
| Decode-fail on read | Returned wrapped (`ErrDecode` sentinel); no auto-evict |
| Encode-fail on `ForceSet` | Returned before any Redis call |
| `BatchError` evolution | Keep `BatchError` non-generic for the string API; add new `BatchKeyError[K comparable]` for typed; share internals |
| `KeyPrefix` view option | Dropped — users put prefix logic in their `KeyCodec` |

## Public surface

All new types live in the existing `redcache` package alongside `CacheAside` and `PrimeableCacheAside`.

### Codec interfaces

```go
// Codec encodes and decodes typed values to and from byte slices that the
// library stores inside the existing envelope payload.
//
// Implementations must be safe for concurrent use; the library invokes
// Encode and Decode from arbitrary goroutines (callbacks, refresh workers,
// multi-set fan-out).
//
// Slice-ownership contract: the slice returned from Encode is owned by the
// library after return — implementations must not retain or mutate it. The
// slice passed to Decode is owned by the caller (the library) — implementations
// must not retain or mutate it past the Decode call.
type Codec[V any] interface {
    Encode(V) ([]byte, error)
    Decode([]byte) (V, error)
}

// KeyCodec converts a typed key to the Redis key string used to address
// the entry. Implementations must be deterministic: the same K must always
// produce the same string within the lifetime of a process. The output
// must be a valid Redis key (non-empty; the library does not validate
// further). Implementations must be safe for concurrent use.
type KeyCodec[K any] interface {
    EncodeKey(K) (string, error)
}
```

### Provided concrete codecs

```go
// JSONCodec serializes V via encoding/json. The default value codec.
type JSONCodec[V any] struct{}

// BytesCodec is the identity codec for V = []byte.
type BytesCodec struct{}

// StringCodec is the identity codec for V = string.
type StringCodec struct{}

// StringKeyCodec is the identity codec for K = string.
type StringKeyCodec struct{}

// KeyCodecFunc adapts a function value to a KeyCodec[K]. Callers supply
// `redcache.KeyCodecFunc[UserID](func(u UserID) (string, error) { ... })`.
type KeyCodecFunc[K any] func(K) (string, error)
func (f KeyCodecFunc[K]) EncodeKey(k K) (string, error) { return f(k) }
```

### Typed wrappers

```go
// Typed is a type-safe view over a *CacheAside. Construct with NewTyped.
// One *CacheAside can be shared across many Typed views with different
// K / V instantiations and codecs.
type Typed[K comparable, V any] struct {
    cache    *CacheAside
    keyCodec KeyCodec[K]
    valCodec Codec[V]
}

// PrimeableTyped is a type-safe view over a *PrimeableCacheAside, adding
// the Set/ForceSet methods to the read/delete/touch surface from Typed.
type PrimeableTyped[K comparable, V any] struct {
    Typed[K, V]
    primeable *PrimeableCacheAside
}
```

### Constructors

```go
func NewTyped[K comparable, V any](
    c *CacheAside,
    keyCodec KeyCodec[K],
    valCodec Codec[V],
) *Typed[K, V]

func NewPrimeableTyped[K comparable, V any](
    c *PrimeableCacheAside,
    keyCodec KeyCodec[K],
    valCodec Codec[V],
) *PrimeableTyped[K, V]

// String-key convenience constructors. Equivalent to passing
// StringKeyCodec{} explicitly.
func NewStringTyped[V any](c *CacheAside, valCodec Codec[V]) *Typed[string, V]
func NewPrimeableStringTyped[V any](c *PrimeableCacheAside, valCodec Codec[V]) *PrimeableTyped[string, V]
```

### Typed methods (read / delete / touch — `*Typed[K,V]`)

```go
func (t *Typed[K, V]) Get(
    ctx context.Context, ttl time.Duration, k K,
    fn func(ctx context.Context, k K) (V, error),
) (V, error)

func (t *Typed[K, V]) GetMulti(
    ctx context.Context, ttl time.Duration, keys []K,
    fn func(ctx context.Context, missing []K) (map[K]V, error),
) (map[K]V, error)

func (t *Typed[K, V]) Del(ctx context.Context, k K) error
func (t *Typed[K, V]) DelMulti(ctx context.Context, keys ...K) error
func (t *Typed[K, V]) Touch(ctx context.Context, ttl time.Duration, k K) error
func (t *Typed[K, V]) TouchMulti(ctx context.Context, ttl time.Duration, keys ...K) error
```

### Primeable methods (write — `*PrimeableTyped[K,V]`)

```go
func (p *PrimeableTyped[K, V]) Set(
    ctx context.Context, ttl time.Duration, k K,
    fn func(ctx context.Context, k K) (V, error),
) error

func (p *PrimeableTyped[K, V]) SetMulti(
    ctx context.Context, ttl time.Duration, keys []K,
    fn func(ctx context.Context, keys []K) (map[K]V, error),
) error

func (p *PrimeableTyped[K, V]) ForceSet(
    ctx context.Context, ttl time.Duration, k K, v V,
) error

func (p *PrimeableTyped[K, V]) ForceSetMulti(
    ctx context.Context, ttl time.Duration, values map[K]V,
) error
```

### Errors

```go
// BatchError is unchanged. The string-API SetMulti / ForceSetMulti continue
// to return *BatchError for source compatibility.
type BatchError struct {
    Failed    map[string]error
    Succeeded []string
}

// BatchKeyError is the typed counterpart returned from PrimeableTyped's
// multi-set methods. Same method shape as BatchError but keyed by K.
// Reachable via errors.As, paralleling how BatchError is exposed today.
type BatchKeyError[K comparable] struct {
    Failed    map[K]error
    Succeeded []K
}
func (e *BatchKeyError[K]) Error() string
func (e *BatchKeyError[K]) HasFailures() bool
func (e *BatchKeyError[K]) ErrorFor(k K) error
func (e *BatchKeyError[K]) HasError(k K) bool

// NewBatchKeyError mirrors NewBatchError: returns nil (untyped) when there
// are no failures, so call sites can return it directly as `error`.
func NewBatchKeyError[K comparable](failed map[K]error, succeeded []K) error

// Sentinel returned (wrapped) from typed reads when a stored value cannot
// be decoded. Library does not auto-evict; the caller decides whether to
// log, Del, or retry.
var ErrDecode = errors.New("redcache: decode failed")
```

The internal multi-set collector that powers both `BatchError` and `BatchKeyError` is shared. Two presentations, one source of truth — see **Implementation notes** below.

## Data flow

### Single-key Get

```
caller k
  → EncodeKey(k)             // returns (s string, err); err short-circuits, no Redis I/O
  → cache.Get(ctx, ttl, s, wrappedFn)
       wrappedFn:            // closes over original k, valCodec
          v, err := userFn(ctx, k)
          if err != nil { return "", err }
          b, err := valCodec.Encode(v)
          if err != nil { return "", err }   // surfaces as Get-callback error
          return unsafeBytesToString(b), nil
  ← raw string from cache
  → valCodec.Decode(unsafeStringToBytes(raw))
  ← (V, ErrDecode-wrapped or nil)
```

### Multi-key Get

```
caller keys []K
  → for each k: encode → encKeys []string, byEnc map[string]K
       (any EncodeKey error short-circuits before Redis I/O)
  → cache.GetMulti(ctx, ttl, encKeys, wrappedFn)
       wrappedFn(missing []string):
          missingK := look up each missing string in byEnc
          result, err := userFn(ctx, missingK)
          if err != nil { return nil, err }
          out := make(map[string]string, len(result))
          for k, v := range result {
              s, err := keyCodec.EncodeKey(k); if err != nil { return nil, err }
              b, err := valCodec.Encode(v);     if err != nil { return nil, err }
              out[s] = unsafeBytesToString(b)
          }
          return out, nil
  ← map[string]string from cache
  → for each (s, raw): byEnc[s] → original K, valCodec.Decode(raw) → V
  ← map[K]V (or first ErrDecode-wrapped error)
```

### Set / SetMulti / ForceSet / ForceSetMulti

Same encode-on-write pattern. The public return type is `error`; multi-key partial failures expose `*BatchKeyError[K]` reachable via `errors.As`, paralleling how `*BatchError` is exposed in the existing string API. Encode failures inside the user `fn` are propagated as callback errors (lock released cleanly, nothing cached). Encode failures in `ForceSet*` short-circuit before any Redis I/O.

### Del / Touch / DelMulti / TouchMulti

Key-only operations. Encode each `K`, forward to the underlying string method.

### Refresh-ahead transparency

Refresh-ahead operates inside `*CacheAside` on the envelope-wrapped string payload. The typed wrapper's user `fn` is captured in the closure passed to the underlying `Get` / `GetMulti` call, and that closure is what the refresh worker invokes when the floor / XFetch fires. The wrapper does not need any refresh-specific code path — encoding happens inside the closure regardless of who triggers it.

## Error semantics

| Failure | Behavior |
|---|---|
| `KeyCodec.EncodeKey` returns error (any path) | Return error to caller before any Redis I/O. No lock attempt, no partial state. |
| User `fn` returns error in `Get` / `Set` / their multi variants | Forwarded as callback error from the underlying string call. Lock released; nothing cached. |
| `Codec.Encode` returns error inside the user `fn` wrapper | Treated identically to a callback error: surfaced to the caller, lock released, nothing cached. |
| `Codec.Encode` returns error in `ForceSet` / `ForceSetMulti` | Returned before any Redis I/O. `ForceSetMulti` collects per-key encode failures into `BatchKeyError[K].Failed` and proceeds with the rest. |
| `Codec.Decode` returns error on read (`Get` / `GetMulti`) | Returned wrapped: `fmt.Errorf("...: %w", ErrDecode)`. Library does not call `Del`. The cached value remains. |
| Multi-key partial failure (some keys CAS-mismatch / lock-lost) | Reported through `BatchKeyError[K]` exactly as the existing string `SetMulti` does through `BatchError`. |

## Implementation notes

### Same package

Everything new lives in `package redcache` next to the existing files. New files:

- `typed.go` — `Typed`, `PrimeableTyped`, constructors, single-key methods.
- `typed_multi.go` — multi-key methods (Get/Set/Del/Touch).
- `codec.go` — `Codec`, `KeyCodec`, `KeyCodecFunc`, `JSONCodec`, `BytesCodec`, `StringCodec`, `StringKeyCodec`.
- `errors.go` — extend with `BatchKeyError[K]` and `ErrDecode`.

Tests:

- `typed_test.go` — typed Get/GetMulti hot path through real Redis, mirroring the existing `cacheaside_test.go` structure.
- `typed_primeable_test.go` — Set / SetMulti / ForceSet / ForceSetMulti, including partial-failure → `BatchKeyError`.
- `typed_unit_test.go` — codec round-trip, `BatchKeyError` accessors, key-codec error short-circuit, decode-error wrapping.
- `typed_bench_test.go` — at least one Get + one GetMulti benchmark for codec overhead measurement.

### Shared multi-set error collection

Internally there is exactly one multi-set error collection helper. It builds a `BatchKeyError[string]` (the typed form is used internally because the type system makes it the natural choice; `K=string` for the legacy path is just an instantiation). At the legacy string-API boundary (in the existing `PrimeableCacheAside.SetMulti` / `ForceSetMulti` returns), the helper output is converted to `*BatchError` so the public type does not change. The typed wrapper uses `BatchKeyError[K]` directly.

The conversion at the boundary is structural: `BatchError{Failed: bke.Failed, Succeeded: bke.Succeeded}`. Both types share the same `Error()` formatter logic via an unexported helper to avoid drift.

### Unsafe string ↔ []byte at the codec boundary

The library converts between `string` (rueidis) and `[]byte` (codec) at the typed wrapper boundary using `unsafe.String` / `unsafe.Slice`. Each conversion is read-only on the source side, so this is sound: the codec promises not to mutate the byte slice it receives, and the library never mutates a returned `string`. This avoids two allocations per Get and two per Set on the hot path. Document the contract on `Codec.Decode` ("must not mutate the input slice").

### Key constraint

`K comparable` is required to use `K` as a Go map key (`map[K]V`, `map[string]K`). This excludes types containing slices, maps, or functions. Most realistic key types (numeric IDs, strings, struct compositions of comparable fields) satisfy this.

### Concurrency

`Typed[K,V]` and `PrimeableTyped[K,V]` are stateless apart from their codec and underlying client pointers. They are safe for concurrent use to the same extent the underlying `*CacheAside` and codec implementations are. No new goroutines, no new locks.

### Allocation budget

Per single-key Get, additional allocations relative to the string API: one for `EncodeKey` if it allocates, plus whatever `Codec.Encode` / `Codec.Decode` allocate. The wrapper itself adds zero. Per multi-key Get with `n` keys, additional allocations are `O(n)` for the `byEnc` map and the encoded-key slice, plus codec costs.

## Testing strategy

- **Codec unit tests.** Round-trip JSON for primitives, structs, slices, nested maps. Identity codecs verify byte equality. `KeyCodecFunc` adapter exercise.
- **Integration tests against real Redis** mirroring the structure of the existing `cacheaside_test.go`. Two parameterizations: `Typed[string, exampleStruct]` and `Typed[int64, exampleStruct]` to exercise both string-key and numeric-key paths.
- **Targeted regression tests:**
  - Encode-fail in `fn` releases the lock (next caller acquires immediately) and does not cache anything.
  - Decode-fail on read returns wrapped `ErrDecode` and leaves the key alive in Redis.
  - GetMulti with a partial encode failure inside `fn` propagates the error and leaves no partial cache state.
  - Key-codec error short-circuits before any Redis I/O (verified by checking Redis was not contacted via a mock or by call counts).
  - Refresh-ahead through a `Typed` view works end-to-end (configure `RefreshAfterFraction`, observe re-fetch firing).
- **`BatchKeyError[K]` tests** mirroring `errors_test.go`: nil-safe accessors, `HasFailures`, `ErrorFor`, formatted output, sorted determinism.
- **Benchmarks.** Add `BenchmarkTypedGet` and `BenchmarkTypedGetMulti` for codec overhead measurement; compare against the existing string-API benchmarks.

## Migration

For external users of the library, this PR is **fully source-compatible**. The existing `*CacheAside`, `*PrimeableCacheAside`, `BatchError`, every option, every method signature, and every Lua script is unchanged. A user who never touches the new typed surface sees no diff.

A user who wants to adopt the typed API does:

```go
client, err := redcache.NewPrimeableCacheAside(ropts, caopts)
if err != nil { /* ... */ }

users := redcache.NewPrimeableStringTyped[User](client, redcache.JSONCodec[User]{})

u, err := users.Get(ctx, time.Minute, "u-123",
    func(ctx context.Context, key string) (User, error) {
        // load from db
        return user, nil
    },
)
```

## Follow-up work (out of scope for this PR)

These were surfaced during the architecture audit during brainstorming and are deliberately **not** included in this PR. Each is independently shippable after the typed work lands.

### Worth doing — clear ROI

- **Metric-emit boilerplate consolidation.** `cacheaside.go` defines 13 near-identical 6-line helpers (`emitCacheHits`, `emitCacheMisses`, etc.) totaling ~80 lines. Collapse into a single helper or wrap the metrics interface. Pure refactor; no behavior change.
- **Lua-script contract tests.** Lua scripts (`lua_scripts.go`) are string literals; `KEYS[N]` / `ARGV[N]` drift between script body and Go-side wiring is only caught at runtime. Add a static test that scans each script for `KEYS[N]` / `ARGV[N]` references and asserts the call sites pass exactly that count. Cheap insurance against future bugs.
- **Slot-aware command executor extraction.** `primeable_writelock_helpers.go` repeats the slot-group → per-slot Lua exec → result-collect pattern across roughly five functions and ~130 LoC. Extract into a dedicated `slotExec` helper that consolidates retry / error parsing in one place. Estimated ~50 LoC saved; touches the multi-set hot path so requires careful regression coverage.

### Lower priority

- **Split `cacheaside.go`** (currently 1,322 lines) into single-key, multi-key, and lifecycle files. Pure reorganization; muddies git blame for limited gain.
- **Replace `goto retry` with a `for` loop** in `Get`. Cosmetic; the existing form is clear and efficient.

### Acceptable as-is — no action

- `syncx.Map[K,V]` interface{}-cast overhead. Intrinsic to `sync.Map`; the typed wrapper is already a net cohesion win.
- `lockEntry`'s `sync.Once` race-avoidance pattern. Already correct and well-commented.
- `internal/poolx` slice-reuse helpers. Well-bounded; no smell.

## Open questions

None. All decisions captured in the **Constraints and prior decisions** table above.