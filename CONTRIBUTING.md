# Contributing

Thanks for your interest in redcache. It's a small, solo-maintained library, so
contributions are welcome but please open an issue to discuss anything
non-trivial before sending a large PR.

## Prerequisites

- Go 1.24+ (the module targets `go 1.24.9`; rueidis requires it — don't lower it).
- Docker, to run a local Redis. The tests are integration tests that need a
  real Redis at `127.0.0.1:6379` — they exercise RESP3 client-side
  invalidation, which can't be faked.

The Go and golangci-lint versions are pinned in `.tool-versions`. With
[asdf](https://asdf-vm.com) installed, `make setup` installs any missing asdf
plugins and then gets the exact toolchain CI uses. Common tasks are wrapped in
the `Makefile`; run
`make help` to list them.

## Running Redis

```bash
docker compose up -d
```

This starts the Redis defined in `docker-compose.yml` on port 6379. Stop it with
`docker compose down`. (`make redis-up` / `make redis-down` wrap these.)

## Tests, lint, format, bench

These are wrapped as `make` targets (`make test-race`, `make lint`, `make fmt`,
`make bench`, or `make check` for the full gate). The raw commands:

```bash
# Tests (race detector on; needs Redis running)
go test -race ./...

# Lint
golangci-lint run

# Format (gofmt + goimports + gci, per the v2 formatters config)
golangci-lint fmt

# Benchmarks
go test -bench=. -benchmem ./...
```

`golangci-lint fmt` applies the formatting the linter expects, so run it before
`golangci-lint run` if the linter complains about formatting.

## Conventions

These are enforced by `.golangci.yml`; CI runs the same linter, so matching them
locally saves a round trip.

- **Import ordering** (`gci`): three groups in this order — standard library,
  third-party, then `github.com/dcbickfo/redcache` (internal) packages.
  `golangci-lint fmt` sorts them for you.
- **Comments end in a period** (`godot`), including doc comments.
- **Exported symbols need doc comments** (`revive`'s `exported` rule).
- Naked returns are only allowed in functions under 30 lines (`nakedret`).
- Cyclomatic and cognitive complexity are capped (`gocyclo` / `gocognit`,
  min-complexity 15) — split large functions rather than suppressing.

## Git hooks (lefthook)

The repo ships a `lefthook.yml` that mirrors CI locally:

- **pre-commit**: `golangci-lint run` and `go build ./...`
- **pre-push**: `go test -race -count=1 ./...`

If you use [lefthook](https://github.com/evilmartians/lefthook), run
`lefthook install` once to wire these up. The hooks are optional — CI runs the
same checks — but they catch problems before you push.

## Pull requests

- Keep the change focused; one logical change per PR.
- Add or update tests for behavior changes.
- Make sure `go test -race ./...` and `golangci-lint run` are both green.
  CI (`.github/workflows/CI.yml`) runs lint and the race test suite against Redis
  on every PR, so a green local run should mean a green CI run.
- Update `README.md` / `CHANGELOG.md` if you change the public API.
