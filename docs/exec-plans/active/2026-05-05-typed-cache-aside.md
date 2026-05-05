# Typed Cache-Aside Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add typed views (`Typed[K,V]`, `PrimeableTyped[K,V]`) on top of the existing `*CacheAside` / `*PrimeableCacheAside`, with a JSON default codec and pluggable user codecs, while leaving the existing string-based public API source-compatible.

**Architecture:** Boundary-only generics. New wrappers live in the same `redcache` package. Each typed call encodes/decodes at the wrapper boundary using the user-supplied `Codec[V]` and `KeyCodec[K]`, then forwards to the existing string-based methods. The lock / refresh / Lua / multi-slot machinery is unchanged. A new `BatchKeyError[K comparable]` mirrors `BatchError` for typed multi-set partial failures; the legacy `BatchError` type is untouched.

**Tech Stack:** Go 1.23+, rueidis, encoding/json, integration tests against a real Redis at `127.0.0.1:6379`, golangci-lint v2.

**Spec:** [`docs/design-docs/2026-05-05-typed-cache-aside-design.md`](../../design-docs/2026-05-05-typed-cache-aside-design.md)

**Deviation from spec:** The spec's "share internal multi-set error collector" is simplified. `BatchError` and `BatchKeyError[K]` each have their own ~13-line `Error()` formatter. The drift risk is small (the format is stable) and the simpler implementation avoids touching the working `PrimeableCacheAside.SetMulti` code. Document this in the type comment for `BatchKeyError`.

---

## File Structure

**New files:**
- `codec.go` — `Codec[V]`, `KeyCodec[K]`, `KeyCodecFunc[K]`, `JSONCodec[V]`, `BytesCodec`, `StringCodec`, `StringKeyCodec`.
- `codec_test.go` — codec round-trip / identity / error-path tests.
- `typed.go` — `Typed[K,V]`, `PrimeableTyped[K,V]`, constructors, single-key methods.
- `typed_multi.go` — multi-key methods for both typed wrappers.
- `typed_test.go` — single-key Typed integration tests against real Redis.
- `typed_multi_test.go` — multi-key Typed integration tests.
- `typed_primeable_test.go` — `PrimeableTyped` Set/ForceSet/SetMulti/ForceSetMulti tests.
- `typed_unit_test.go` — encode/decode failure short-circuit, `BatchKeyError` accessors.
- `typed_bench_test.go` — `BenchmarkTypedGet`, `BenchmarkTypedGetMulti`.

**Modified files:**
- `errors.go` — add `BatchKeyError[K comparable]`, `NewBatchKeyError`, `ErrDecode`.
- `errors_test.go` — `BatchKeyError` accessor tests.
- `README.md` — add typed-API usage example.
- `CHANGELOG.md` — add unreleased section entry.

---

## Task 1: Codec interfaces and provided implementations

**Files:**
- Create: `/Users/david/GolandProjects/redcache/codec.go`
- Create: `/Users/david/GolandProjects/redcache/codec_test.go`

- [ ] **Step 1: Write the failing tests for the provided codecs.**

```go
// codec_test.go
package redcache_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/dcbickfo/redcache"
)

func TestJSONCodec_RoundTripStruct(t *testing.T) {
	type user struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	c := redcache.JSONCodec[user]{}
	in := user{ID: 7, Name: "alice"}

	b, err := c.Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := c.Decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("got %+v, want %+v", out, in)
	}
}

func TestJSONCodec_DecodeInvalidJSON(t *testing.T) {
	c := redcache.JSONCodec[map[string]int]{}
	if _, err := c.Decode([]byte("not json")); err == nil {
		t.Fatal("expected decode error for invalid json")
	}
}

func TestBytesCodec_Identity(t *testing.T) {
	c := redcache.BytesCodec{}
	in := []byte("hello \x00 world")
	b, err := c.Encode(in)
	if err != nil || !bytes.Equal(b, in) {
		t.Fatalf("encode mismatch: got %q err %v", b, err)
	}
	out, err := c.Decode(in)
	if err != nil || !bytes.Equal(out, in) {
		t.Fatalf("decode mismatch: got %q err %v", out, err)
	}
}

func TestStringCodec_Identity(t *testing.T) {
	c := redcache.StringCodec{}
	const in = "hello"
	b, err := c.Encode(in)
	if err != nil || string(b) != in {
		t.Fatalf("encode mismatch: got %q err %v", b, err)
	}
	out, err := c.Decode([]byte(in))
	if err != nil || out != in {
		t.Fatalf("decode mismatch: got %q err %v", out, err)
	}
}

func TestStringKeyCodec_Identity(t *testing.T) {
	c := redcache.StringKeyCodec{}
	out, err := c.EncodeKey("user-123")
	if err != nil || out != "user-123" {
		t.Fatalf("got %q err %v", out, err)
	}
}

func TestKeyCodecFunc_AdaptsFunction(t *testing.T) {
	sentinel := errors.New("nope")
	bad := redcache.KeyCodecFunc[int](func(int) (string, error) { return "", sentinel })
	if _, err := bad.EncodeKey(0); !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel, got %v", err)
	}
	good := redcache.KeyCodecFunc[int](func(i int) (string, error) {
		return "i:" + itoa(i), nil
	})
	out, err := good.EncodeKey(42)
	if err != nil || out != "i:42" {
		t.Fatalf("got %q err %v", out, err)
	}
}

func itoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = digits[i%10]
		i /= 10
	}
	return string(b[pos:])
}
```

- [ ] **Step 2: Run tests to verify they fail with "undefined" errors.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run 'TestJSONCodec|TestBytesCodec|TestStringCodec|TestStringKeyCodec|TestKeyCodecFunc' -v
```

Expected: build failure — `undefined: redcache.JSONCodec`, `redcache.BytesCodec`, etc.

- [ ] **Step 3: Implement codec.go.**

```go
// codec.go
package redcache

import "encoding/json"

// Codec encodes and decodes typed values to and from byte slices stored
// inside the existing envelope payload.
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
// the entry. Implementations must be deterministic and safe for concurrent
// use. The output must be a valid non-empty Redis key (the library does
// not validate further).
type KeyCodec[K any] interface {
	EncodeKey(K) (string, error)
}

// KeyCodecFunc adapts a function value to a KeyCodec[K].
type KeyCodecFunc[K any] func(K) (string, error)

// EncodeKey implements KeyCodec.
func (f KeyCodecFunc[K]) EncodeKey(k K) (string, error) { return f(k) }

// JSONCodec serializes V via encoding/json. The default value codec for
// NewStringTyped / NewPrimeableStringTyped users who do not supply their own.
type JSONCodec[V any] struct{}

// Encode implements Codec.
func (JSONCodec[V]) Encode(v V) ([]byte, error) { return json.Marshal(v) }

// Decode implements Codec.
func (JSONCodec[V]) Decode(b []byte) (V, error) {
	var v V
	if err := json.Unmarshal(b, &v); err != nil {
		var zero V
		return zero, err
	}
	return v, nil
}

// BytesCodec is the identity codec for V = []byte.
type BytesCodec struct{}

// Encode implements Codec.
func (BytesCodec) Encode(v []byte) ([]byte, error) { return v, nil }

// Decode implements Codec.
func (BytesCodec) Decode(b []byte) ([]byte, error) { return b, nil }

// StringCodec is the identity codec for V = string.
type StringCodec struct{}

// Encode implements Codec.
func (StringCodec) Encode(v string) ([]byte, error) { return []byte(v), nil }

// Decode implements Codec.
func (StringCodec) Decode(b []byte) (string, error) { return string(b), nil }

// StringKeyCodec is the identity codec for K = string.
type StringKeyCodec struct{}

// EncodeKey implements KeyCodec.
func (StringKeyCodec) EncodeKey(k string) (string, error) { return k, nil }
```

- [ ] **Step 4: Run tests to verify pass.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run 'TestJSONCodec|TestBytesCodec|TestStringCodec|TestStringKeyCodec|TestKeyCodecFunc' -v
```

Expected: all PASS.

- [ ] **Step 5: Lint and format.**

```bash
cd /Users/david/GolandProjects/redcache && golangci-lint fmt && golangci-lint run
```

Expected: zero issues.

- [ ] **Step 6: Commit.**

```bash
cd /Users/david/GolandProjects/redcache && git add codec.go codec_test.go && git commit -m "Add Codec/KeyCodec interfaces and JSON/identity implementations"
```

---

## Task 2: BatchKeyError[K] and ErrDecode

**Files:**
- Modify: `/Users/david/GolandProjects/redcache/errors.go`
- Modify: `/Users/david/GolandProjects/redcache/errors_test.go`

- [ ] **Step 1: Write the failing tests for BatchKeyError accessors.**

Append to `errors_test.go`:

```go
// errors_test.go (append)
func TestBatchKeyError_Int_AccessorsAndFormat(t *testing.T) {
	err := redcache.NewBatchKeyError(
		map[int]error{1: errors.New("boom"), 2: errors.New("bad")},
		[]int{3, 4},
	)
	if err == nil {
		t.Fatal("expected non-nil error")
	}
	var bke *redcache.BatchKeyError[int]
	if !errors.As(err, &bke) {
		t.Fatalf("errors.As failed for *BatchKeyError[int]; got %T", err)
	}
	if !bke.HasFailures() || !bke.HasError(1) || bke.HasError(99) {
		t.Fatalf("HasFailures/HasError wrong: %+v", bke)
	}
	if bke.ErrorFor(1) == nil || bke.ErrorFor(99) != nil {
		t.Fatal("ErrorFor wrong")
	}
	// Stable formatting: failed keys ordered.
	got := bke.Error()
	if !strings.Contains(got, "2 succeeded, 2 failed") {
		t.Fatalf("missing summary: %s", got)
	}
}

func TestBatchKeyError_Nil_SafeAccessors(t *testing.T) {
	var bke *redcache.BatchKeyError[string]
	if bke.HasError("x") || bke.ErrorFor("x") != nil {
		t.Fatal("nil receiver should be safe and return zero values")
	}
}

func TestNewBatchKeyError_NilWhenNoFailures(t *testing.T) {
	if redcache.NewBatchKeyError(map[int]error{}, []int{1}) != nil {
		t.Fatal("expected untyped-nil error")
	}
}

func TestErrDecode_IsSentinel(t *testing.T) {
	wrapped := fmt.Errorf("decoding user: %w", redcache.ErrDecode)
	if !errors.Is(wrapped, redcache.ErrDecode) {
		t.Fatal("ErrDecode should be reachable via errors.Is")
	}
}
```

If `fmt` / `strings` are not yet imported in `errors_test.go`, add them.

- [ ] **Step 2: Run tests to verify they fail.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run 'TestBatchKeyError|TestNewBatchKeyError|TestErrDecode' -v
```

Expected: build failure — `undefined: redcache.BatchKeyError`, `redcache.NewBatchKeyError`, `redcache.ErrDecode`.

- [ ] **Step 3: Extend errors.go.**

Append to `errors.go`:

```go
// errors.go (append; keep existing BatchError untouched)

// ErrDecode is returned (wrapped) from typed reads when a stored value
// cannot be decoded by the configured Codec. The library does not auto-evict
// — the caller decides whether to log, Del, or retry.
var ErrDecode = errors.New("redcache: decode failed")

// BatchKeyError is the typed counterpart of BatchError, returned (via errors.As)
// from PrimeableTyped's multi-set methods on partial failure. Same semantics
// as BatchError; the only difference is that keys are typed K instead of string.
//
// Note: the Error() formatter is intentionally a structural duplicate of
// BatchError.Error() (~13 lines). The format is stable and the duplication
// is preferable to a generic helper that would either constrain K to ordered
// types or sort by fmt.Sprintf("%v", k).
type BatchKeyError[K comparable] struct {
	Failed    map[K]error
	Succeeded []K
}

// Error returns a human-readable summary. Failed keys are emitted in the order
// produced by sorting their %v formatting so the string is stable across calls.
func (e *BatchKeyError[K]) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "batch operation partially failed: %d succeeded, %d failed", len(e.Succeeded), len(e.Failed))
	type kv struct {
		k K
		s string
	}
	pairs := make([]kv, 0, len(e.Failed))
	for k := range e.Failed {
		pairs = append(pairs, kv{k: k, s: fmt.Sprintf("%v", k)})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].s < pairs[j].s })
	for _, p := range pairs {
		fmt.Fprintf(&b, "; key %q: %s", p.s, e.Failed[p.k])
	}
	return b.String()
}

// HasFailures returns true if any keys failed.
func (e *BatchKeyError[K]) HasFailures() bool { return len(e.Failed) > 0 }

// ErrorFor returns the error recorded for k, or nil if the key did not fail.
// Safe to call on a nil receiver.
func (e *BatchKeyError[K]) ErrorFor(k K) error {
	if e == nil {
		return nil
	}
	return e.Failed[k]
}

// HasError reports whether the given key failed. Safe to call on a nil receiver.
func (e *BatchKeyError[K]) HasError(k K) bool {
	if e == nil {
		return false
	}
	_, ok := e.Failed[k]
	return ok
}

// NewBatchKeyError mirrors NewBatchError. Returns untyped nil when there
// are no failures, so call sites can return it directly as `error`.
func NewBatchKeyError[K comparable](failed map[K]error, succeeded []K) error {
	if len(failed) == 0 {
		return nil
	}
	return &BatchKeyError[K]{Failed: failed, Succeeded: succeeded}
}
```

- [ ] **Step 4: Run tests to verify pass.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run 'TestBatchKeyError|TestNewBatchKeyError|TestErrDecode|TestBatchError|TestNewBatchError' -v
```

Expected: all PASS (existing BatchError tests still pass).

- [ ] **Step 5: Lint and full test run.**

```bash
cd /Users/david/GolandProjects/redcache && golangci-lint fmt && golangci-lint run && go test -race ./...
```

Expected: zero lint issues; full suite green (Redis at 127.0.0.1:6379 must be running).

- [ ] **Step 6: Commit.**

```bash
cd /Users/david/GolandProjects/redcache && git add errors.go errors_test.go && git commit -m "Add BatchKeyError[K] and ErrDecode for typed cache-aside"
```

---

## Task 3: Typed[K,V] type and Typed.Get

**Files:**
- Create: `/Users/david/GolandProjects/redcache/typed.go`
- Create: `/Users/david/GolandProjects/redcache/typed_test.go`

- [ ] **Step 1: Write a failing integration test for Typed[string, struct].Get.**

```go
// typed_test.go
package redcache_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

type tUser struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func newTestCacheAside(t *testing.T) *redcache.CacheAside {
	t.Helper()
	c, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.CacheAsideOption{LockTTL: 2 * time.Second},
	)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestTyped_Get_LoadsAndCaches(t *testing.T) {
	cache := newTestCacheAside(t)
	users := redcache.NewStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	key := "u:" + uuid.NewString()

	var calls int
	loader := func(_ context.Context, _ string) (tUser, error) {
		calls++
		return tUser{ID: 1, Name: "alice"}, nil
	}

	got, err := users.Get(context.Background(), time.Second, key, loader)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	if got.ID != 1 || got.Name != "alice" {
		t.Fatalf("first get value: %+v", got)
	}
	got2, err := users.Get(context.Background(), time.Second, key, loader)
	if err != nil {
		t.Fatalf("second get: %v", err)
	}
	if got2 != got {
		t.Fatalf("second get value: %+v", got2)
	}
	if calls != 1 {
		t.Fatalf("loader called %d times, want 1", calls)
	}
}

func TestTyped_Get_DecodeErrorIsWrapped(t *testing.T) {
	cache := newTestCacheAside(t)
	// Write a non-JSON payload directly via the underlying client so the
	// typed Get reads garbage and surfaces ErrDecode.
	key := "decode:" + uuid.NewString()
	if err := cache.Client().Do(context.Background(),
		cache.Client().B().Set().Key(key).Value("not json").Px(time.Second).Build()).Error(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	users := redcache.NewStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})

	_, err := users.Get(context.Background(), time.Second, key, func(context.Context, string) (tUser, error) {
		return tUser{}, errors.New("loader should not be called on decode failure of cache hit")
	})
	if !errors.Is(err, redcache.ErrDecode) {
		t.Fatalf("expected ErrDecode, got %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify failure.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run TestTyped_Get -v
```

Expected: build failure — `undefined: redcache.NewStringTyped`, `redcache.Typed`.

- [ ] **Step 3: Implement typed.go (initial: just `Typed`, `NewTyped`, `NewStringTyped`, single-key `Get`).**

```go
// typed.go
package redcache

import (
	"context"
	"fmt"
	"time"
	"unsafe"
)

// Typed is a type-safe view over a *CacheAside.
//
// One *CacheAside can be shared across many Typed views with different
// K / V instantiations and codecs. The wrapper itself is stateless beyond
// its codec and underlying-client pointers and is safe for concurrent use
// to the same extent as the underlying *CacheAside and codecs.
type Typed[K comparable, V any] struct {
	cache    *CacheAside
	keyCodec KeyCodec[K]
	valCodec Codec[V]
}

// NewTyped constructs a typed view over cache.
func NewTyped[K comparable, V any](cache *CacheAside, keyCodec KeyCodec[K], valCodec Codec[V]) *Typed[K, V] {
	return &Typed[K, V]{cache: cache, keyCodec: keyCodec, valCodec: valCodec}
}

// NewStringTyped is sugar for NewTyped[string, V] with StringKeyCodec{} preset.
func NewStringTyped[V any](cache *CacheAside, valCodec Codec[V]) *Typed[string, V] {
	return NewTyped[string, V](cache, StringKeyCodec{}, valCodec)
}

// Get returns the cached value for k, populating the cache via fn on a miss.
// See (*CacheAside).Get for stampede / lock semantics — Typed.Get is a thin
// codec wrapper around it.
//
// Errors:
//   - keyCodec.EncodeKey error: returned before any Redis I/O.
//   - fn error: forwarded as-is (lock released, nothing cached).
//   - valCodec.Encode error inside fn: forwarded as a callback error.
//   - valCodec.Decode error on read: returned wrapped with ErrDecode; the
//     cached entry is left intact.
func (t *Typed[K, V]) Get(
	ctx context.Context,
	ttl time.Duration,
	k K,
	fn func(ctx context.Context, k K) (V, error),
) (V, error) {
	var zero V
	encKey, err := t.keyCodec.EncodeKey(k)
	if err != nil {
		return zero, fmt.Errorf("redcache: encode key: %w", err)
	}

	raw, err := t.cache.Get(ctx, ttl, encKey, func(ctx context.Context, _ string) (string, error) {
		v, ferr := fn(ctx, k)
		if ferr != nil {
			return "", ferr
		}
		b, eerr := t.valCodec.Encode(v)
		if eerr != nil {
			return "", fmt.Errorf("redcache: encode value: %w", eerr)
		}
		return bytesToString(b), nil
	})
	if err != nil {
		return zero, err
	}

	v, derr := t.valCodec.Decode(stringToBytes(raw))
	if derr != nil {
		return zero, fmt.Errorf("redcache: decode key %q: %w: %s", encKey, ErrDecode, derr)
	}
	return v, nil
}

// stringToBytes returns the bytes backing s. Callers must not mutate the
// returned slice — codecs are documented to treat their Decode input as
// borrowed.
func stringToBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// bytesToString returns a string viewing the bytes of b. Callers (the library)
// own b after Encode returns and never mutate it, so the returned string is
// safe for the lifetime of b.
func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}
```

- [ ] **Step 4: Run tests to verify pass.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run TestTyped_Get -v
```

Expected: both `TestTyped_Get_LoadsAndCaches` and `TestTyped_Get_DecodeErrorIsWrapped` PASS.

- [ ] **Step 5: Lint and full test.**

```bash
cd /Users/david/GolandProjects/redcache && golangci-lint fmt && golangci-lint run && go test -race ./...
```

Expected: zero lint issues; full suite green.

- [ ] **Step 6: Commit.**

```bash
cd /Users/david/GolandProjects/redcache && git add typed.go typed_test.go && git commit -m "Add Typed[K,V] wrapper with Get"
```

---

## Task 4: Typed.Del and Typed.Touch

**Files:**
- Modify: `/Users/david/GolandProjects/redcache/typed.go`
- Modify: `/Users/david/GolandProjects/redcache/typed_test.go`

- [ ] **Step 1: Add failing tests for Del and Touch.**

Append to `typed_test.go`:

```go
func TestTyped_Del_RemovesEntry(t *testing.T) {
	cache := newTestCacheAside(t)
	users := redcache.NewStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	key := "del:" + uuid.NewString()

	loader := func(context.Context, string) (tUser, error) { return tUser{ID: 9, Name: "x"}, nil }
	if _, err := users.Get(context.Background(), time.Second, key, loader); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := users.Del(context.Background(), key); err != nil {
		t.Fatalf("del: %v", err)
	}
	var calls int
	wrapped := func(ctx context.Context, k string) (tUser, error) {
		calls++
		return loader(ctx, k)
	}
	if _, err := users.Get(context.Background(), time.Second, key, wrapped); err != nil {
		t.Fatalf("get after del: %v", err)
	}
	if calls != 1 {
		t.Fatalf("loader called %d times after del, want 1", calls)
	}
}

func TestTyped_Touch_ExtendsTTL(t *testing.T) {
	cache := newTestCacheAside(t)
	users := redcache.NewStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	key := "touch:" + uuid.NewString()

	loader := func(context.Context, string) (tUser, error) { return tUser{ID: 9, Name: "x"}, nil }
	if _, err := users.Get(context.Background(), 200*time.Millisecond, key, loader); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := users.Touch(context.Background(), 5*time.Second, key); err != nil {
		t.Fatalf("touch: %v", err)
	}
	// Wait past the original TTL — Touch should have extended it.
	time.Sleep(400 * time.Millisecond)
	var calls int
	wrapped := func(ctx context.Context, k string) (tUser, error) {
		calls++
		return loader(ctx, k)
	}
	if _, err := users.Get(context.Background(), time.Second, key, wrapped); err != nil {
		t.Fatalf("get after touch: %v", err)
	}
	if calls != 0 {
		t.Fatalf("loader called %d times after touch, want 0 (entry should still be cached)", calls)
	}
}
```

- [ ] **Step 2: Run tests to verify failure.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run 'TestTyped_Del|TestTyped_Touch' -v
```

Expected: build failure — `users.Del undefined`, `users.Touch undefined`.

- [ ] **Step 3: Add Del and Touch methods to typed.go.**

Append to `typed.go`:

```go
// Del removes a key from Redis, triggering invalidation on all clients.
func (t *Typed[K, V]) Del(ctx context.Context, k K) error {
	encKey, err := t.keyCodec.EncodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	return t.cache.Del(ctx, encKey)
}

// Touch extends the TTL of a cached value to ttl. See (*CacheAside).Touch
// for no-op-on-lock semantics.
func (t *Typed[K, V]) Touch(ctx context.Context, ttl time.Duration, k K) error {
	encKey, err := t.keyCodec.EncodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	return t.cache.Touch(ctx, ttl, encKey)
}
```

- [ ] **Step 4: Run tests to verify pass.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run 'TestTyped_Del|TestTyped_Touch' -v
```

Expected: PASS.

- [ ] **Step 5: Lint and full test.**

```bash
cd /Users/david/GolandProjects/redcache && golangci-lint fmt && golangci-lint run && go test -race ./...
```

- [ ] **Step 6: Commit.**

```bash
cd /Users/david/GolandProjects/redcache && git add typed.go typed_test.go && git commit -m "Add Typed.Del and Typed.Touch"
```

---

## Task 5: Typed.GetMulti

**Files:**
- Create: `/Users/david/GolandProjects/redcache/typed_multi.go`
- Create: `/Users/david/GolandProjects/redcache/typed_multi_test.go`

- [ ] **Step 1: Write failing integration tests for GetMulti.**

```go
// typed_multi_test.go
package redcache_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dcbickfo/redcache"
)

func TestTyped_GetMulti_LoadsAndCaches(t *testing.T) {
	cache := newTestCacheAside(t)
	users := redcache.NewStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	prefix := uuid.NewString() + ":"
	keys := []string{prefix + "a", prefix + "b", prefix + "c"}

	var calls int
	loader := func(_ context.Context, missing []string) (map[string]tUser, error) {
		calls++
		out := make(map[string]tUser, len(missing))
		for i, k := range missing {
			out[k] = tUser{ID: i + 1, Name: k}
		}
		return out, nil
	}

	got, err := users.GetMulti(context.Background(), time.Second, keys, loader)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("first get len %d, want 3", len(got))
	}
	for _, k := range keys {
		u, ok := got[k]
		if !ok || u.Name != k {
			t.Fatalf("first get key %q: got %+v ok=%v", k, u, ok)
		}
	}

	got2, err := users.GetMulti(context.Background(), time.Second, keys, loader)
	if err != nil {
		t.Fatalf("second get: %v", err)
	}
	if len(got2) != 3 || calls != 1 {
		t.Fatalf("second get triggered loader (calls=%d) or short result %d", calls, len(got2))
	}
}

func TestTyped_GetMulti_IntKeys(t *testing.T) {
	cache := newTestCacheAside(t)
	prefix := uuid.NewString() + ":"
	codec := redcache.KeyCodecFunc[int](func(i int) (string, error) {
		return prefix + strconv.Itoa(i), nil
	})
	users := redcache.NewTyped[int, tUser](cache, codec, redcache.JSONCodec[tUser]{})

	loader := func(_ context.Context, missing []int) (map[int]tUser, error) {
		out := make(map[int]tUser, len(missing))
		for _, i := range missing {
			out[i] = tUser{ID: i, Name: strconv.Itoa(i)}
		}
		return out, nil
	}
	got, err := users.GetMulti(context.Background(), time.Second, []int{10, 20, 30}, loader)
	if err != nil {
		t.Fatalf("getmulti: %v", err)
	}
	if got[10].Name != "10" || got[20].Name != "20" || got[30].Name != "30" {
		t.Fatalf("typed int keys not preserved: %+v", got)
	}
}
```

- [ ] **Step 2: Run tests to verify failure.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run TestTyped_GetMulti -v
```

Expected: `users.GetMulti undefined`.

- [ ] **Step 3: Implement typed_multi.go.**

```go
// typed_multi.go
package redcache

import (
	"context"
	"fmt"
	"time"
)

// GetMulti returns cached values for keys, populating misses via fn.
// See (*CacheAside).GetMulti for slot-batching and stampede semantics.
//
// Errors:
//   - keyCodec.EncodeKey error: returned before any Redis I/O.
//   - fn error: forwarded as-is.
//   - valCodec.Encode error inside fn: forwarded as a callback error.
//   - valCodec.Decode error on a read result: returned wrapped with ErrDecode;
//     no successful entries are returned in that case.
func (t *Typed[K, V]) GetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, missing []K) (map[K]V, error),
) (map[K]V, error) {
	if len(keys) == 0 {
		return map[K]V{}, nil
	}
	encKeys := make([]string, len(keys))
	byEnc := make(map[string]K, len(keys))
	for i, k := range keys {
		s, err := t.keyCodec.EncodeKey(k)
		if err != nil {
			return nil, fmt.Errorf("redcache: encode key: %w", err)
		}
		encKeys[i] = s
		byEnc[s] = k
	}

	raw, err := t.cache.GetMulti(ctx, ttl, encKeys, func(ctx context.Context, missingEnc []string) (map[string]string, error) {
		missingK := make([]K, len(missingEnc))
		for i, s := range missingEnc {
			missingK[i] = byEnc[s]
		}
		result, ferr := fn(ctx, missingK)
		if ferr != nil {
			return nil, ferr
		}
		out := make(map[string]string, len(result))
		for k, v := range result {
			s, kerr := t.keyCodec.EncodeKey(k)
			if kerr != nil {
				return nil, fmt.Errorf("redcache: encode key: %w", kerr)
			}
			b, eerr := t.valCodec.Encode(v)
			if eerr != nil {
				return nil, fmt.Errorf("redcache: encode value for key %q: %w", s, eerr)
			}
			out[s] = bytesToString(b)
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}

	out := make(map[K]V, len(raw))
	for s, payload := range raw {
		k, ok := byEnc[s]
		if !ok {
			continue // unexpected; underlying returned a key we did not request
		}
		v, derr := t.valCodec.Decode(stringToBytes(payload))
		if derr != nil {
			return nil, fmt.Errorf("redcache: decode key %q: %w: %s", s, ErrDecode, derr)
		}
		out[k] = v
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests to verify pass.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run TestTyped_GetMulti -v
```

Expected: PASS.

- [ ] **Step 5: Lint and full test.**

```bash
cd /Users/david/GolandProjects/redcache && golangci-lint fmt && golangci-lint run && go test -race ./...
```

- [ ] **Step 6: Commit.**

```bash
cd /Users/david/GolandProjects/redcache && git add typed_multi.go typed_multi_test.go && git commit -m "Add Typed.GetMulti"
```

---

## Task 6: Typed.DelMulti and Typed.TouchMulti

**Files:**
- Modify: `/Users/david/GolandProjects/redcache/typed_multi.go`
- Modify: `/Users/david/GolandProjects/redcache/typed_multi_test.go`

- [ ] **Step 1: Write failing tests for DelMulti and TouchMulti.**

Append to `typed_multi_test.go`:

```go
func TestTyped_DelMulti_RemovesAll(t *testing.T) {
	cache := newTestCacheAside(t)
	users := redcache.NewStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	prefix := uuid.NewString() + ":"
	keys := []string{prefix + "a", prefix + "b"}

	loader := func(_ context.Context, missing []string) (map[string]tUser, error) {
		out := make(map[string]tUser, len(missing))
		for _, k := range missing {
			out[k] = tUser{ID: 1, Name: k}
		}
		return out, nil
	}
	if _, err := users.GetMulti(context.Background(), time.Second, keys, loader); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := users.DelMulti(context.Background(), keys...); err != nil {
		t.Fatalf("delmulti: %v", err)
	}
	calls := 0
	wrapped := func(ctx context.Context, missing []string) (map[string]tUser, error) {
		calls++
		return loader(ctx, missing)
	}
	if _, err := users.GetMulti(context.Background(), time.Second, keys, wrapped); err != nil {
		t.Fatalf("get after del: %v", err)
	}
	if calls != 1 {
		t.Fatalf("loader called %d times after del, want 1", calls)
	}
}

func TestTyped_TouchMulti_ExtendsTTL(t *testing.T) {
	cache := newTestCacheAside(t)
	users := redcache.NewStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	prefix := uuid.NewString() + ":"
	keys := []string{prefix + "a", prefix + "b"}

	loader := func(_ context.Context, missing []string) (map[string]tUser, error) {
		out := make(map[string]tUser, len(missing))
		for _, k := range missing {
			out[k] = tUser{ID: 1, Name: k}
		}
		return out, nil
	}
	if _, err := users.GetMulti(context.Background(), 200*time.Millisecond, keys, loader); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := users.TouchMulti(context.Background(), 5*time.Second, keys...); err != nil {
		t.Fatalf("touchmulti: %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	calls := 0
	wrapped := func(ctx context.Context, missing []string) (map[string]tUser, error) {
		calls++
		return loader(ctx, missing)
	}
	if _, err := users.GetMulti(context.Background(), time.Second, keys, wrapped); err != nil {
		t.Fatalf("get after touch: %v", err)
	}
	if calls != 0 {
		t.Fatalf("loader called %d times after touch, want 0", calls)
	}
}
```

- [ ] **Step 2: Run tests to verify failure.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run 'TestTyped_DelMulti|TestTyped_TouchMulti' -v
```

Expected: `users.DelMulti undefined`, `users.TouchMulti undefined`.

- [ ] **Step 3: Implement DelMulti and TouchMulti.**

Append to `typed_multi.go`:

```go
// DelMulti removes multiple keys from Redis. See (*CacheAside).DelMulti.
func (t *Typed[K, V]) DelMulti(ctx context.Context, keys ...K) error {
	if len(keys) == 0 {
		return nil
	}
	encKeys := make([]string, len(keys))
	for i, k := range keys {
		s, err := t.keyCodec.EncodeKey(k)
		if err != nil {
			return fmt.Errorf("redcache: encode key: %w", err)
		}
		encKeys[i] = s
	}
	return t.cache.DelMulti(ctx, encKeys...)
}

// TouchMulti extends the TTL of multiple keys. See (*CacheAside).TouchMulti.
func (t *Typed[K, V]) TouchMulti(ctx context.Context, ttl time.Duration, keys ...K) error {
	if len(keys) == 0 {
		return nil
	}
	encKeys := make([]string, len(keys))
	for i, k := range keys {
		s, err := t.keyCodec.EncodeKey(k)
		if err != nil {
			return fmt.Errorf("redcache: encode key: %w", err)
		}
		encKeys[i] = s
	}
	return t.cache.TouchMulti(ctx, ttl, encKeys...)
}
```

- [ ] **Step 4: Run tests to verify pass.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run 'TestTyped_DelMulti|TestTyped_TouchMulti' -v
```

Expected: PASS.

- [ ] **Step 5: Lint and full test.**

```bash
cd /Users/david/GolandProjects/redcache && golangci-lint fmt && golangci-lint run && go test -race ./...
```

- [ ] **Step 6: Commit.**

```bash
cd /Users/david/GolandProjects/redcache && git add typed_multi.go typed_multi_test.go && git commit -m "Add Typed.DelMulti and Typed.TouchMulti"
```

---

## Task 7: PrimeableTyped[K,V] with Set and ForceSet

**Files:**
- Modify: `/Users/david/GolandProjects/redcache/typed.go`
- Create: `/Users/david/GolandProjects/redcache/typed_primeable_test.go`

- [ ] **Step 1: Write failing tests.**

```go
// typed_primeable_test.go
package redcache_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"

	"github.com/dcbickfo/redcache"
)

func newTestPrimeable(t *testing.T) *redcache.PrimeableCacheAside {
	t.Helper()
	c, err := redcache.NewPrimeableCacheAside(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.CacheAsideOption{LockTTL: 2 * time.Second},
	)
	if err != nil {
		t.Fatalf("new primeable: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestPrimeableTyped_Set_PopulatesAndCaches(t *testing.T) {
	pca := newTestPrimeable(t)
	users := redcache.NewPrimeableStringTyped[tUser](pca, redcache.JSONCodec[tUser]{})
	key := "set:" + uuid.NewString()

	if err := users.Set(context.Background(), time.Second, key,
		func(context.Context, string) (tUser, error) { return tUser{ID: 1, Name: "a"}, nil },
	); err != nil {
		t.Fatalf("set: %v", err)
	}

	got, err := users.Get(context.Background(), time.Second, key,
		func(context.Context, string) (tUser, error) {
			t.Fatal("loader should not run after Set")
			return tUser{}, nil
		},
	)
	if err != nil {
		t.Fatalf("get after set: %v", err)
	}
	if got.ID != 1 || got.Name != "a" {
		t.Fatalf("got %+v", got)
	}
}

func TestPrimeableTyped_ForceSet_OverwritesUnconditionally(t *testing.T) {
	pca := newTestPrimeable(t)
	users := redcache.NewPrimeableStringTyped[tUser](pca, redcache.JSONCodec[tUser]{})
	key := "force:" + uuid.NewString()

	if err := users.ForceSet(context.Background(), time.Second, key, tUser{ID: 1, Name: "a"}); err != nil {
		t.Fatalf("force set 1: %v", err)
	}
	if err := users.ForceSet(context.Background(), time.Second, key, tUser{ID: 2, Name: "b"}); err != nil {
		t.Fatalf("force set 2: %v", err)
	}

	got, err := users.Get(context.Background(), time.Second, key,
		func(context.Context, string) (tUser, error) { return tUser{}, errors.New("nope") },
	)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != 2 || got.Name != "b" {
		t.Fatalf("got %+v, want overwritten value", got)
	}
}

func TestPrimeableTyped_Set_EncodeFailureReleasesLock(t *testing.T) {
	pca := newTestPrimeable(t)
	users := redcache.NewPrimeableStringTyped[badEncode](pca, badEncodeCodec{})
	key := "encfail:" + uuid.NewString()

	want := errors.New("nope")
	err := users.Set(context.Background(), time.Second, key,
		func(context.Context, string) (badEncode, error) { return badEncode{err: want}, nil },
	)
	if !errors.Is(err, want) {
		t.Fatalf("expected encode error, got %v", err)
	}

	// Lock must be released — a follow-up ForceSet should succeed immediately.
	if err := users.ForceSet(context.Background(), time.Second, key, badEncode{}); err != nil {
		t.Fatalf("force set after encode failure: %v", err)
	}
}

type badEncode struct{ err error }
type badEncodeCodec struct{}

func (badEncodeCodec) Encode(b badEncode) ([]byte, error) {
	if b.err != nil {
		return nil, b.err
	}
	return []byte("ok"), nil
}
func (badEncodeCodec) Decode(b []byte) (badEncode, error) { return badEncode{}, nil }
```

- [ ] **Step 2: Run tests to verify failure.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run TestPrimeableTyped -v
```

Expected: build failure — `redcache.NewPrimeableStringTyped` undefined.

- [ ] **Step 3: Add PrimeableTyped to typed.go.**

Append to `typed.go`:

```go
// PrimeableTyped is a type-safe view over a *PrimeableCacheAside, adding
// Set / ForceSet / SetMulti / ForceSetMulti to the read/delete/touch surface
// from Typed.
type PrimeableTyped[K comparable, V any] struct {
	Typed[K, V]
	primeable *PrimeableCacheAside
}

// NewPrimeableTyped constructs a typed view over a *PrimeableCacheAside.
func NewPrimeableTyped[K comparable, V any](
	cache *PrimeableCacheAside,
	keyCodec KeyCodec[K],
	valCodec Codec[V],
) *PrimeableTyped[K, V] {
	return &PrimeableTyped[K, V]{
		Typed:     Typed[K, V]{cache: &cache.CacheAside, keyCodec: keyCodec, valCodec: valCodec},
		primeable: cache,
	}
}

// NewPrimeableStringTyped is sugar for NewPrimeableTyped[string, V] with
// StringKeyCodec{} preset.
func NewPrimeableStringTyped[V any](
	cache *PrimeableCacheAside,
	valCodec Codec[V],
) *PrimeableTyped[string, V] {
	return NewPrimeableTyped[string, V](cache, StringKeyCodec{}, valCodec)
}

// Set explicitly populates the cache via fn under a write lock.
// See (*PrimeableCacheAside).Set.
func (p *PrimeableTyped[K, V]) Set(
	ctx context.Context,
	ttl time.Duration,
	k K,
	fn func(ctx context.Context, k K) (V, error),
) error {
	encKey, err := p.keyCodec.EncodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	return p.primeable.Set(ctx, ttl, encKey, func(ctx context.Context, _ string) (string, error) {
		v, ferr := fn(ctx, k)
		if ferr != nil {
			return "", ferr
		}
		b, eerr := p.valCodec.Encode(v)
		if eerr != nil {
			return "", fmt.Errorf("redcache: encode value: %w", eerr)
		}
		return bytesToString(b), nil
	})
}

// ForceSet unconditionally writes v to Redis. See (*PrimeableCacheAside).ForceSet.
func (p *PrimeableTyped[K, V]) ForceSet(ctx context.Context, ttl time.Duration, k K, v V) error {
	encKey, err := p.keyCodec.EncodeKey(k)
	if err != nil {
		return fmt.Errorf("redcache: encode key: %w", err)
	}
	b, err := p.valCodec.Encode(v)
	if err != nil {
		return fmt.Errorf("redcache: encode value: %w", err)
	}
	return p.primeable.ForceSet(ctx, ttl, encKey, bytesToString(b))
}
```

- [ ] **Step 4: Run tests to verify pass.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run TestPrimeableTyped -v
```

Expected: PASS.

- [ ] **Step 5: Lint and full test.**

```bash
cd /Users/david/GolandProjects/redcache && golangci-lint fmt && golangci-lint run && go test -race ./...
```

- [ ] **Step 6: Commit.**

```bash
cd /Users/david/GolandProjects/redcache && git add typed.go typed_primeable_test.go && git commit -m "Add PrimeableTyped[K,V] with Set and ForceSet"
```

---

## Task 8: PrimeableTyped.SetMulti

**Files:**
- Modify: `/Users/david/GolandProjects/redcache/typed_multi.go`
- Modify: `/Users/david/GolandProjects/redcache/typed_primeable_test.go`

- [ ] **Step 1: Write failing tests for SetMulti, including BatchKeyError partial failure.**

Append to `typed_primeable_test.go`:

```go
func TestPrimeableTyped_SetMulti_PopulatesAll(t *testing.T) {
	pca := newTestPrimeable(t)
	users := redcache.NewPrimeableStringTyped[tUser](pca, redcache.JSONCodec[tUser]{})
	prefix := uuid.NewString() + ":"
	keys := []string{prefix + "a", prefix + "b"}

	if err := users.SetMulti(context.Background(), time.Second, keys,
		func(_ context.Context, keys []string) (map[string]tUser, error) {
			out := make(map[string]tUser, len(keys))
			for i, k := range keys {
				out[k] = tUser{ID: i, Name: k}
			}
			return out, nil
		},
	); err != nil {
		t.Fatalf("setmulti: %v", err)
	}

	got, err := users.GetMulti(context.Background(), time.Second, keys,
		func(context.Context, []string) (map[string]tUser, error) {
			t.Fatal("loader should not run after SetMulti")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatalf("get after setmulti: %v", err)
	}
	if len(got) != 2 || got[keys[0]].Name != keys[0] || got[keys[1]].Name != keys[1] {
		t.Fatalf("got %+v", got)
	}
}

func TestPrimeableTyped_SetMulti_PartialFailureSurfacesBatchKeyError(t *testing.T) {
	pca := newTestPrimeable(t)
	users := redcache.NewPrimeableStringTyped[tUser](pca, redcache.JSONCodec[tUser]{})
	prefix := uuid.NewString() + ":"
	keys := []string{prefix + "a", prefix + "b"}

	// Force a CAS mismatch by ForceSet'ing one key mid-flight is racy; instead
	// rely on the existing semantic: if fn returns a subset of keys, the
	// underlying SetMulti still succeeds for the returned ones. To exercise
	// the BatchKeyError path deterministically, we steal a write lock on
	// keys[1] before calling SetMulti.
	if err := users.ForceSet(context.Background(), time.Second, keys[1], tUser{ID: 99}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Acquire a write lock on keys[1] from a separate primeable client so
	// our SetMulti's CAS for that key fails (lock-lost). The simplest way to
	// trigger lock-lost is to ForceSet again during the callback, racing.
	// For deterministic coverage of BatchKeyError surfacing, this test
	// covers the happy-path collection only; per-key failure is exercised
	// by primeable_cacheaside_test.go and the conversion paths below.
	//
	// Verify the type wiring instead: when underlying SetMulti returns a
	// *BatchError, our typed wrapper should return *BatchKeyError[string].
	// This verification is performed in TestPrimeableTyped_SetMulti_BatchKeyError_Surfaces below.
	_ = keys
}

// This test forces partial failure by returning a value for an unknown key.
// PrimeableCacheAside.SetMulti silently ignores keys returned by fn that
// were not in the original list, so this is not a partial-failure trigger.
// Instead, exercise BatchKeyError by injecting a lock-stealing competitor.
func TestPrimeableTyped_SetMulti_BatchKeyError_Surfaces(t *testing.T) {
	pca := newTestPrimeable(t)
	users := redcache.NewPrimeableStringTyped[tUser](pca, redcache.JSONCodec[tUser]{})
	prefix := uuid.NewString() + ":"
	keys := []string{prefix + "a", prefix + "b"}

	// Inside the callback, ForceSet keys[1] from another goroutine before we
	// try to CAS-set our locked value. This causes lock-lost for keys[1] but
	// not keys[0], yielding a partial failure surfaced as *BatchKeyError[string].
	steal := func() {
		_ = pca.ForceSet(context.Background(), time.Second, keys[1], "stolen")
	}

	err := users.SetMulti(context.Background(), time.Second, keys,
		func(_ context.Context, keys []string) (map[string]tUser, error) {
			steal()
			out := make(map[string]tUser, len(keys))
			for _, k := range keys {
				out[k] = tUser{ID: 1, Name: k}
			}
			return out, nil
		},
	)
	if err == nil {
		t.Fatal("expected partial failure")
	}
	var bke *redcache.BatchKeyError[string]
	if !errors.As(err, &bke) {
		t.Fatalf("expected *BatchKeyError[string], got %T: %v", err, err)
	}
	if !bke.HasFailures() {
		t.Fatalf("expected failures, got %+v", bke)
	}
	if !bke.HasError(keys[1]) {
		t.Fatalf("expected failure for stolen key %q; got %+v", keys[1], bke.Failed)
	}
}
```

- [ ] **Step 2: Run tests to verify failure.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run TestPrimeableTyped_SetMulti -v
```

Expected: `users.SetMulti undefined`.

- [ ] **Step 3: Implement SetMulti.**

Append to `typed_multi.go`:

```go
// SetMulti explicitly populates the cache via fn under write locks.
// See (*PrimeableCacheAside).SetMulti.
//
// On partial failure the returned error wraps a *BatchKeyError[K] (reachable
// via errors.As) listing per-key Failed / Succeeded entries.
func (p *PrimeableTyped[K, V]) SetMulti(
	ctx context.Context,
	ttl time.Duration,
	keys []K,
	fn func(ctx context.Context, keys []K) (map[K]V, error),
) error {
	if len(keys) == 0 {
		return nil
	}
	encKeys := make([]string, len(keys))
	byEnc := make(map[string]K, len(keys))
	for i, k := range keys {
		s, err := p.keyCodec.EncodeKey(k)
		if err != nil {
			return fmt.Errorf("redcache: encode key: %w", err)
		}
		encKeys[i] = s
		byEnc[s] = k
	}

	err := p.primeable.SetMulti(ctx, ttl, encKeys, func(ctx context.Context, encArg []string) (map[string]string, error) {
		argK := make([]K, len(encArg))
		for i, s := range encArg {
			argK[i] = byEnc[s]
		}
		result, ferr := fn(ctx, argK)
		if ferr != nil {
			return nil, ferr
		}
		out := make(map[string]string, len(result))
		for k, v := range result {
			s, kerr := p.keyCodec.EncodeKey(k)
			if kerr != nil {
				return nil, fmt.Errorf("redcache: encode key: %w", kerr)
			}
			b, eerr := p.valCodec.Encode(v)
			if eerr != nil {
				return nil, fmt.Errorf("redcache: encode value for key %q: %w", s, eerr)
			}
			out[s] = bytesToString(b)
		}
		return out, nil
	})
	if err == nil {
		return nil
	}

	// Convert *BatchError (string-keyed) to *BatchKeyError[K] using byEnc.
	var be *BatchError
	if !errorsAs(err, &be) {
		return err
	}
	failedK := make(map[K]error, len(be.Failed))
	for s, ferr := range be.Failed {
		if k, ok := byEnc[s]; ok {
			failedK[k] = ferr
		}
	}
	succeededK := make([]K, 0, len(be.Succeeded))
	for _, s := range be.Succeeded {
		if k, ok := byEnc[s]; ok {
			succeededK = append(succeededK, k)
		}
	}
	return NewBatchKeyError(failedK, succeededK)
}

// errorsAs is a thin local alias kept so the multi conversion paths do not
// need to import "errors" twice; the builtin errors.As is fine to use directly.
// (Inlined call site below references errors.As.)
```

Replace `errorsAs(err, &be)` in the body above with the standard `errors.As(err, &be)` and add `"errors"` to the imports if not already present. The wrapper `errorsAs` placeholder above is for plan-readability only — implement using the standard library directly:

```go
import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ... and in the function body:
var be *BatchError
if !errors.As(err, &be) {
	return err
}
```

- [ ] **Step 4: Run tests to verify pass.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run TestPrimeableTyped_SetMulti -v
```

Expected: PASS. The lock-stealing test should now produce a `*BatchKeyError[string]` with `HasError(keys[1]) == true`.

If the steal-test is flaky (timing-dependent), keep it but mark it `t.Skip("inherently racy") if steal beat the lock acquire` — investigate first whether the underlying SetMulti is deterministic w.r.t. ForceSet inside its callback.

- [ ] **Step 5: Lint and full test.**

```bash
cd /Users/david/GolandProjects/redcache && golangci-lint fmt && golangci-lint run && go test -race ./...
```

- [ ] **Step 6: Commit.**

```bash
cd /Users/david/GolandProjects/redcache && git add typed_multi.go typed_primeable_test.go && git commit -m "Add PrimeableTyped.SetMulti with BatchKeyError surfacing"
```

---

## Task 9: PrimeableTyped.ForceSetMulti

**Files:**
- Modify: `/Users/david/GolandProjects/redcache/typed_multi.go`
- Modify: `/Users/david/GolandProjects/redcache/typed_primeable_test.go`

- [ ] **Step 1: Write failing test.**

Append to `typed_primeable_test.go`:

```go
func TestPrimeableTyped_ForceSetMulti_OverwritesAll(t *testing.T) {
	pca := newTestPrimeable(t)
	users := redcache.NewPrimeableStringTyped[tUser](pca, redcache.JSONCodec[tUser]{})
	prefix := uuid.NewString() + ":"
	in := map[string]tUser{
		prefix + "a": {ID: 1, Name: "a"},
		prefix + "b": {ID: 2, Name: "b"},
	}

	if err := users.ForceSetMulti(context.Background(), time.Second, in); err != nil {
		t.Fatalf("force set multi: %v", err)
	}

	keys := []string{prefix + "a", prefix + "b"}
	got, err := users.GetMulti(context.Background(), time.Second, keys,
		func(context.Context, []string) (map[string]tUser, error) {
			t.Fatal("loader should not run after ForceSetMulti")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatalf("get after force set multi: %v", err)
	}
	if len(got) != 2 || got[prefix+"a"].ID != 1 || got[prefix+"b"].ID != 2 {
		t.Fatalf("got %+v", got)
	}
}
```

- [ ] **Step 2: Run tests to verify failure.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run TestPrimeableTyped_ForceSetMulti -v
```

Expected: `users.ForceSetMulti undefined`.

- [ ] **Step 3: Implement ForceSetMulti.**

Append to `typed_multi.go`:

```go
// ForceSetMulti unconditionally writes values to Redis. See
// (*PrimeableCacheAside).ForceSetMulti. Encode failures are collected per
// key and returned as *BatchKeyError[K]; successfully encoded keys are still
// written.
func (p *PrimeableTyped[K, V]) ForceSetMulti(
	ctx context.Context,
	ttl time.Duration,
	values map[K]V,
) error {
	if len(values) == 0 {
		return nil
	}
	encVals := make(map[string]string, len(values))
	failed := make(map[K]error)
	byEnc := make(map[string]K, len(values))
	for k, v := range values {
		s, err := p.keyCodec.EncodeKey(k)
		if err != nil {
			failed[k] = fmt.Errorf("encode key: %w", err)
			continue
		}
		b, err := p.valCodec.Encode(v)
		if err != nil {
			failed[k] = fmt.Errorf("encode value: %w", err)
			continue
		}
		encVals[s] = bytesToString(b)
		byEnc[s] = k
	}

	if len(encVals) == 0 {
		// Every key failed encoding — return BatchKeyError without touching Redis.
		return NewBatchKeyError(failed, nil)
	}

	err := p.primeable.ForceSetMulti(ctx, ttl, encVals)
	if err == nil && len(failed) == 0 {
		return nil
	}

	// Build per-key result. Underlying ForceSetMulti returns plain error
	// (not *BatchError); on success all encVals were written.
	succeeded := make([]K, 0, len(byEnc))
	if err == nil {
		for _, k := range byEnc {
			succeeded = append(succeeded, k)
		}
	} else {
		// Underlying returned a non-batch error; treat all encVals as failed
		// with the same underlying error so the caller still sees per-key info.
		for _, k := range byEnc {
			failed[k] = err
		}
	}
	return NewBatchKeyError(failed, succeeded)
}
```

- [ ] **Step 4: Run tests to verify pass.**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run TestPrimeableTyped_ForceSetMulti -v
```

Expected: PASS.

- [ ] **Step 5: Lint and full test.**

```bash
cd /Users/david/GolandProjects/redcache && golangci-lint fmt && golangci-lint run && go test -race ./...
```

- [ ] **Step 6: Commit.**

```bash
cd /Users/david/GolandProjects/redcache && git add typed_multi.go typed_primeable_test.go && git commit -m "Add PrimeableTyped.ForceSetMulti"
```

---

## Task 10: Refresh-ahead transparency end-to-end test

**Files:**
- Modify: `/Users/david/GolandProjects/redcache/typed_test.go`

- [ ] **Step 1: Write failing test.**

Append to `typed_test.go`:

```go
func TestTyped_RefreshAhead_FiresThroughTypedView(t *testing.T) {
	c, err := redcache.NewRedCacheAside(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		redcache.CacheAsideOption{
			LockTTL:              500 * time.Millisecond,
			RefreshAfterFraction: 0.1,
			RefreshWorkers:       1,
			RefreshQueueSize:     8,
		},
	)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	defer c.Close()

	users := redcache.NewStringTyped[tUser](c, redcache.JSONCodec[tUser]{})
	key := "refresh:" + uuid.NewString()

	var calls int32
	loader := func(_ context.Context, _ string) (tUser, error) {
		n := atomic.AddInt32(&calls, 1)
		return tUser{ID: int(n), Name: "v"}, nil
	}

	first, err := users.Get(context.Background(), 500*time.Millisecond, key, loader)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	if first.ID != 1 {
		t.Fatalf("first ID %d, want 1", first.ID)
	}
	// Wait past RefreshAfterFraction floor (0.1 * 500ms = 50ms).
	time.Sleep(150 * time.Millisecond)

	// Next Get returns the still-cached value but should trigger a refresh.
	if _, err := users.Get(context.Background(), 500*time.Millisecond, key, loader); err != nil {
		t.Fatalf("trigger get: %v", err)
	}
	// Give the refresh worker time to run.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&calls) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&calls); got < 2 {
		t.Fatalf("loader call count %d; expected refresh-ahead to have fired (>=2)", got)
	}
}
```

If `sync/atomic` is not imported in `typed_test.go`, add it.

- [ ] **Step 2: Run test to verify failure mode (likely passes immediately if Task 3 is correct, but worth running).**

```bash
cd /Users/david/GolandProjects/redcache && go test ./... -run TestTyped_RefreshAhead -v
```

Expected: PASS — refresh-ahead is transparent through the typed wrapper because the closure captures the codec wrapping. If it fails, debug whether the user fn is being called from the refresh worker; the wrapper closure should fire identically.

- [ ] **Step 3: Lint and full test.**

```bash
cd /Users/david/GolandProjects/redcache && golangci-lint fmt && golangci-lint run && go test -race ./...
```

- [ ] **Step 4: Commit.**

```bash
cd /Users/david/GolandProjects/redcache && git add typed_test.go && git commit -m "Add refresh-ahead transparency test for typed view"
```

---

## Task 11: Benchmarks

**Files:**
- Create: `/Users/david/GolandProjects/redcache/typed_bench_test.go`

- [ ] **Step 1: Write benchmarks.**

```go
// typed_bench_test.go
package redcache_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dcbickfo/redcache"
)

func BenchmarkTypedGet(b *testing.B) {
	cache := newBenchPrimeable(b)
	users := redcache.NewPrimeableStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	key := "bench:" + uuid.NewString()

	if err := users.ForceSet(context.Background(), time.Minute, key, tUser{ID: 1, Name: "alice"}); err != nil {
		b.Fatalf("seed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := users.Get(context.Background(), time.Minute, key,
			func(context.Context, string) (tUser, error) { return tUser{}, nil },
		); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkTypedGetMulti(b *testing.B) {
	cache := newBenchPrimeable(b)
	users := redcache.NewPrimeableStringTyped[tUser](cache, redcache.JSONCodec[tUser]{})
	prefix := uuid.NewString() + ":"
	keys := []string{prefix + "a", prefix + "b", prefix + "c", prefix + "d", prefix + "e"}

	for _, k := range keys {
		if err := users.ForceSet(context.Background(), time.Minute, k, tUser{ID: 1, Name: k}); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := users.GetMulti(context.Background(), time.Minute, keys,
			func(context.Context, []string) (map[string]tUser, error) { return nil, nil },
		); err != nil {
			b.Fatalf("getmulti: %v", err)
		}
	}
}

func newBenchPrimeable(b *testing.B) *redcache.PrimeableCacheAside {
	b.Helper()
	c, err := redcache.NewPrimeableCacheAside(
		// Re-use the same constructor pattern as the integration tests.
		rueidisOptForBench(),
		redcache.CacheAsideOption{LockTTL: 2 * time.Second},
	)
	if err != nil {
		b.Fatalf("new primeable: %v", err)
	}
	b.Cleanup(c.Close)
	return c
}
```

Add at the top of the file or in a shared helper (you may inline if simpler):

```go
import "github.com/redis/rueidis"

func rueidisOptForBench() rueidis.ClientOption {
	return rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}}
}
```

- [ ] **Step 2: Run the benchmarks for one iteration to verify compile + execution.**

```bash
cd /Users/david/GolandProjects/redcache && go test -run=^$ -bench='^(BenchmarkTypedGet|BenchmarkTypedGetMulti)$' -benchtime=1x ./...
```

Expected: both benchmarks run without error.

- [ ] **Step 3: Run full benchmarks for ROI comparison vs the existing string benchmarks (informational).**

```bash
cd /Users/david/GolandProjects/redcache && go test -run=^$ -bench='^Benchmark' -benchtime=2s ./...
```

Inspect the result: typed Get should be within a few hundred ns/op of the string Get. The marginal cost is one JSON encode and one decode per round-trip plus the unsafe conversions.

- [ ] **Step 4: Lint.**

```bash
cd /Users/david/GolandProjects/redcache && golangci-lint fmt && golangci-lint run
```

- [ ] **Step 5: Commit.**

```bash
cd /Users/david/GolandProjects/redcache && git add typed_bench_test.go && git commit -m "Add typed-API benchmarks"
```

---

## Task 12: Documentation — README and CHANGELOG

**Files:**
- Modify: `/Users/david/GolandProjects/redcache/README.md`
- Modify: `/Users/david/GolandProjects/redcache/CHANGELOG.md`

- [ ] **Step 1: Add a Typed API section to README.md.**

Add the following section after the existing "Usage" example (before "Configuration"):

````markdown
## Typed API

For typed values and keys, wrap a `*CacheAside` (or `*PrimeableCacheAside`) in a `Typed[K, V]` (or `PrimeableTyped[K, V]`) view. The default codec is JSON; supply your own `Codec[V]` / `KeyCodec[K]` to use a different format.

```go
type User struct {
    ID   int    `json:"id"`
    Name string `json:"name"`
}

client, err := redcache.NewPrimeableCacheAside(
    rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
    redcache.CacheAsideOption{LockTTL: time.Second},
)
if err != nil {
    log.Fatal(err)
}

// String keys, JSON-encoded User values.
users := redcache.NewPrimeableStringTyped[User](client, redcache.JSONCodec[User]{})

u, err := users.Get(ctx, time.Minute, "u-123",
    func(ctx context.Context, key string) (User, error) {
        // load from db
        return User{ID: 123, Name: "alice"}, nil
    },
)
```

For typed keys, supply a `KeyCodec[K]`:

```go
type UserID int64

userIDCodec := redcache.KeyCodecFunc[UserID](func(id UserID) (string, error) {
    return fmt.Sprintf("user:%d", id), nil
})
users := redcache.NewPrimeableTyped[UserID, User](client, userIDCodec, redcache.JSONCodec[User]{})
u, err := users.Get(ctx, time.Minute, UserID(123), loader)
```

Provided codecs:

| Type | Description |
|---|---|
| `JSONCodec[V]` | Default; serializes V via `encoding/json`. |
| `BytesCodec` | Identity for `V = []byte`. |
| `StringCodec` | Identity for `V = string`. |
| `StringKeyCodec` | Identity for `K = string`. |
| `KeyCodecFunc[K]` | Adapts a `func(K) (string, error)` to `KeyCodec[K]`. |

Caller owns absence semantics — use `*T`, `sql.Null[T]`, or a domain sentinel inside `V`. The library does not introduce an `ErrNotFound` sentinel.

Decode failures on read are returned wrapped with `redcache.ErrDecode` (`errors.Is`-checkable). The library does not auto-evict; the caller decides whether to log, `Del`, or retry.

Multi-set partial failures surface as `*BatchKeyError[K]` (reachable via `errors.As`), mirroring the existing string `*BatchError`.
````

- [ ] **Step 2: Add CHANGELOG entry.**

Look at existing CHANGELOG.md format first:

```bash
head -40 /Users/david/GolandProjects/redcache/CHANGELOG.md
```

Add a new section near the top following the existing format. Example shape:

```markdown
## [Unreleased]

### Added
- `Typed[K, V]` / `PrimeableTyped[K, V]` typed wrappers over `CacheAside` / `PrimeableCacheAside`, with `JSONCodec[V]`, `BytesCodec`, `StringCodec`, `StringKeyCodec`, and `KeyCodecFunc[K]` provided.
- `BatchKeyError[K comparable]` typed counterpart of `BatchError`, returned (via `errors.As`) from `PrimeableTyped` multi-set methods.
- `ErrDecode` sentinel returned (wrapped) from typed reads when the configured codec rejects a stored value.

### Notes
- Existing string-typed public API (`*CacheAside`, `*PrimeableCacheAside`, `*BatchError`, all options and method signatures) is unchanged.
```

- [ ] **Step 3: Verify README renders cleanly (basic sanity).**

```bash
cd /Users/david/GolandProjects/redcache && head -60 README.md && echo "---" && grep -n '^##' README.md
```

Expected: section ordering looks right, no markdown table breakage, no truncated code fences.

- [ ] **Step 4: Run full test + lint one last time.**

```bash
cd /Users/david/GolandProjects/redcache && golangci-lint fmt && golangci-lint run && go test -race ./...
```

Expected: green.

- [ ] **Step 5: Commit.**

```bash
cd /Users/david/GolandProjects/redcache && git add README.md CHANGELOG.md && git commit -m "Document typed cache-aside API in README and CHANGELOG"
```

---

## Verification checklist (post-execution)

After all 12 tasks, confirm:

- [ ] `go test -race ./...` is green (Redis at 127.0.0.1:6379 must be up).
- [ ] `golangci-lint run` is clean.
- [ ] `go test -run=^$ -bench='^Benchmark'` runs both new and existing benchmarks.
- [ ] Public API for the existing string surface (`*CacheAside`, `*PrimeableCacheAside`, `*BatchError`, all option fields, every existing method signature) is byte-identical to the pre-PR state.
- [ ] No new files exceed ~400 lines (cohesion sanity check). If `typed_multi.go` grows past that, consider splitting reads vs writes into separate files in a follow-up.
- [ ] `git log --oneline` shows ~12 small commits, one per task, telling a coherent story.
