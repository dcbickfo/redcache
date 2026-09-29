# Contributing

Thanks for your interest in redcache. It's a small, solo-maintained library, so
contributions are welcome but please open an issue to discuss anything
non-trivial before sending a large PR.

## Prerequisites

- Go 1.27+ (the module and `.tool-versions` both target Go 1.27.0).
- Docker, to run a local Redis. The tests are integration tests that need a
  real Redis at `127.0.0.1:6379` — they exercise RESP3 client-side
  invalidation, which can't be faked.

Go and golangci-lint are pinned in `.tool-versions`. With
[asdf](https://asdf-vm.com) installed, `make setup` installs the exact versions
CI uses. Run `make help` to list the repository tasks.

## Running Redis

```bash
docker compose up -d
```

This starts the Redis defined in `docker-compose.yml` on port 6379. Stop it with
`docker compose down`. (`make redis-up` / `make redis-down` wrap these.)

## Tests, lint, format, bench

These are wrapped as `make` targets (`make test-race`, `make cover-check`,
`make lint`, `make fmt`, `make bench`, or `make check` for the full gate). The
raw commands:

```bash
# Tests (race detector on; needs Redis running)
go test -race -count=1 ./...

# Coverage gate
go test -coverprofile=coverage.out ./...
go tool cover -func=coverage.out

# Lint
golangci-lint run

# Format
golangci-lint fmt

# Benchmarks
go test -bench=. -benchmem ./...
```

## Conventions

These are enforced by `.golangci.yml`; CI runs the same linter, so matching them
locally saves a round trip.

- **Import ordering** (`gci`): three groups in this order — standard library,
  third-party, then `github.com/dcbickfo/redcache` packages.
- Comments end in a period, including doc comments.
- Exported symbols need doc comments.
- Naked returns are only allowed in functions under 30 lines.
- Cyclomatic and cognitive complexity are capped at 15; split large functions
  instead of suppressing the checks.

## Git hooks (lefthook)

The repo ships a `lefthook.yml` that mirrors CI locally:

- **pre-commit**: `make lint` and `go build ./...`
- **pre-push**: `go test -race -count=1 ./...`

If you use [lefthook](https://github.com/evilmartians/lefthook), run
`lefthook install` once to wire these up. The hooks are optional — CI runs the
same checks — but they catch problems before you push.

## Pull requests

- Keep the change focused; one logical change per PR.
- Add or update tests for behavior changes.
- Make sure `make test-race`, `make cover-check`, and `make lint` are green.
  CI (`.github/workflows/CI.yml`) runs lint, coverage, and the race test suite
  against Redis on every PR, so a green local run should mean a green CI run.
- Update `README.md` / `CHANGELOG.md` if you change the public API.
