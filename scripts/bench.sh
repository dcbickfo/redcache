#!/usr/bin/env bash
# Run the benchmark suite and save results to bench/ for later comparison.
#
# Usage:
#   scripts/bench.sh                 # save as bench/<git-short-sha>-<timestamp>.txt
#   scripts/bench.sh <label>         # save as bench/<label>.txt (e.g. v0.3.0)
#
# Compare two runs:
#   benchstat bench/<old>.txt bench/<new>.txt
#
# Requires Redis on 127.0.0.1:6379 and benchstat:
#   go install golang.org/x/perf/cmd/benchstat@latest

set -euo pipefail

cd "$(dirname "$0")/.."

mkdir -p bench

if [[ $# -ge 1 ]]; then
    label="$1"
else
    sha=$(git rev-parse --short HEAD 2>/dev/null || echo "nogit")
    ts=$(date -u +%Y%m%dT%H%M%SZ)
    label="${sha}-${ts}"
fi

out="bench/${label}.txt"

echo "Running benchmarks → ${out}"
echo "  (count=10, benchtime=2s — takes a few minutes)"

# -count=10 + benchstat smooths out cloud / shared-runner noise.
# -run=^$ disables the test suite so only Benchmarks run.
# -benchmem reports allocs/op + B/op (much more stable signal than ns/op).
go test -bench=. -benchmem -count=10 -benchtime=2s -run=^$ -timeout=20m . | tee "${out}"

echo
echo "Saved: ${out}"
echo "Compare with:  benchstat <baseline>.txt ${out}"
