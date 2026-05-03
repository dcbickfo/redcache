# scripts

Local-only helpers. Not part of the public API.

## bench.sh — capture a benchmark baseline

```bash
scripts/bench.sh             # bench/<git-sha>-<utc-timestamp>.txt
scripts/bench.sh v0.3.0      # bench/v0.3.0.txt
```

Runs `go test -bench=. -benchmem -count=10 -benchtime=2s` and writes the
output to `bench/`. The `bench/` directory is gitignored — these baselines
stay on your machine.

## bench-compare.sh — diff two runs

```bash
scripts/bench-compare.sh v0.3.0           # compares v0.3.0 vs most-recent
scripts/bench-compare.sh v0.3.0 v0.4.0    # compares two named runs
```

Runs `benchstat` over the two files and prints a delta table with
p-values. Install benchstat once:

```bash
go install golang.org/x/perf/cmd/benchstat@latest
```

## Recommended release workflow

1. Before the release branch: `scripts/bench.sh <prev-version>` (only if you
   don't already have a baseline file for it).
2. On the release candidate: `scripts/bench.sh <new-version>-rc`.
3. `scripts/bench-compare.sh <prev-version> <new-version>-rc`.
4. Investigate any row where `allocs/op` or `B/op` regressed with
   `p<0.05`. `ns/op` is noisier — only act on multi-percent shifts.
5. After releasing, keep the file as the next baseline.
