#!/usr/bin/env bash
# Compare two saved benchmark runs with benchstat.
#
# Usage:
#   scripts/bench-compare.sh <baseline> [<new>]
#
#   <baseline>  filename in bench/ (with or without .txt)
#   <new>       filename in bench/ (defaults to most-recent file)
#
# Examples:
#   scripts/bench-compare.sh v0.3.0
#   scripts/bench-compare.sh v0.3.0 v0.4.0-rc1

set -euo pipefail

cd "$(dirname "$0")/.."

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <baseline> [<new>]" >&2
    echo "  files are looked up in bench/ (extension optional)" >&2
    exit 1
fi

resolve() {
    local name="$1"
    if [[ -f "bench/${name}" ]]; then
        echo "bench/${name}"
    elif [[ -f "bench/${name}.txt" ]]; then
        echo "bench/${name}.txt"
    elif [[ -f "${name}" ]]; then
        echo "${name}"
    else
        echo "not found: ${name}" >&2
        exit 1
    fi
}

baseline=$(resolve "$1")

if [[ $# -ge 2 ]]; then
    new=$(resolve "$2")
else
    # Most recent file in bench/, excluding the resolved baseline.
    new=$(ls -t bench/*.txt 2>/dev/null | grep -v "^${baseline}$" | head -n 1 || true)
    if [[ -z "${new}" ]]; then
        echo "no second benchmark file found in bench/" >&2
        exit 1
    fi
fi

if ! command -v benchstat >/dev/null 2>&1; then
    echo "benchstat not installed. Install with:" >&2
    echo "  go install golang.org/x/perf/cmd/benchstat@latest" >&2
    exit 1
fi

echo "baseline: ${baseline}"
echo "new:      ${new}"
echo

benchstat "${baseline}" "${new}"
