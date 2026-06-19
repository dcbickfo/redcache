# Standalone examples

These examples are runnable programs that show complete redcache setup and usage
outside package-level godoc snippets.

They expect Redis on `127.0.0.1:6379` by default. Override that with
`REDIS_ADDR` if needed:

```bash
REDIS_ADDR=localhost:6379 go run ./examples/string-cache
```

From the repository root:

```bash
go run ./examples/string-cache
go run ./examples/typed-cache
go run ./examples/metrics
```

## Examples

- `string-cache` uses `NewString[string]` and `StringCodec`, which is the direct
  migration path for string-key/string-value integrations.
- `typed-cache` uses a domain key type, `KeyCodecFunc`, and `JSONCodec` for
  typed values.
- `metrics` wires a custom `Metrics` implementation and shows hit/miss counters
  around cache operations.
