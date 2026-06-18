# Support

redcache is a small, solo-maintained open source library. Please use the
lightest-weight support path that fits the issue.

## Bugs

Open a GitHub issue with the bug report template. Include:

- the redcache version or commit,
- `go version`,
- Redis version and whether it is standalone or clustered, and
- a minimal reproduction or failing test if possible.

## Questions

Open a GitHub issue with the question template. For API usage questions, include
the constructor and codecs you are using, for example `Open` plus `NewString`
with `StringCodec` or `JSONCodec`.

## Feature requests

Open a GitHub issue with the feature request template. API sketches are useful,
especially for changes to `Cache[K, V]`, codecs, metrics, or refresh behavior.

## Security

Do not open public issues for vulnerabilities. Use the process in
[SECURITY.md](SECURITY.md).

## Expectations

There is no paid support or SLA. The current maintained line is the latest
`v0.x` release until the project reaches `v1.0`.
