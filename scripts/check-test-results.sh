#!/usr/bin/env bash
# Fail unless a `go test -json` file exists, executed tests, and skipped none.
# A skipped test means a server it needs was not reached, which CI must not
# report as a pass.
set -euo pipefail
file=${1:?usage: check-test-results.sh TestResults.json}
[ -s "$file" ] || { echo "no test results in $file" >&2; exit 1; }
passed=$(jq -s '[.[] | select(.Test != null and .Action == "pass")] | length' "$file")
skipped=$(jq -rs '[.[] | select(.Test != null and .Action == "skip") | "\(.Package) \(.Test)"] | unique | .[]' "$file")
[ "$passed" -gt 0 ] || { echo "no tests passed in $file" >&2; exit 1; }
if [ -n "$skipped" ]; then
  echo "skipped tests (a server was unreachable or -short was set):" >&2
  echo "$skipped" >&2
  exit 1
fi
echo "ok: $passed tests passed, none skipped"
