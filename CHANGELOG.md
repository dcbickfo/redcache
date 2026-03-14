# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.1.6] - 2025-10-06

### Fixed
- Fix `Register` race condition with `CompareAndDelete` for expired lock entries.

### Added
- golangci-lint configuration with `godot`, `gci`, and `nakedret` linters.
- Additional concurrency tests for lock contention scenarios.

## [v0.1.5] - 2025-10-06

### Fixed
- Address race condition in lock entry cleanup using `context.AfterFunc`.

## [v0.1.4] - 2025-04-04

### Fixed
- Fix indefinite wait when lock context expires without Redis invalidation.

## [v0.1.3] - 2025-03-21

### Fixed
- Fix refresh bug in client-side cache invalidation handling.

## [v0.1.2] - 2025-03-13

### Changed
- Use `iter.Seq` iterator for key iteration in multi-key operations.
- Add `ClientBuilder` option for custom `rueidis.Client` construction.

## [v0.1.1] - 2025-02-04

### Changed
- Handle multi-slot operations for keys in Redis cluster mode (#1).
- Group SET Lua scripts by Redis cluster slot for efficient batching.

## [v0.1.0] - 2025-02-04

### Added
- Initial release of redcache.
- Cache-aside pattern with `Get` and `GetMulti` operations.
- Distributed locking with `SET NX GET PX` and UUIDv7 lock values.
- Client-side caching via rueidis `DoCache` with Redis invalidation.
- `Del` and `DelMulti` for cache invalidation.
- Lua scripts for atomic lock verification on SET and DEL.
- CI workflow with GitHub Actions.

[Unreleased]: https://github.com/dcbickfo/redcache/compare/v0.1.6...HEAD
[v0.1.6]: https://github.com/dcbickfo/redcache/compare/v0.1.5...v0.1.6
[v0.1.5]: https://github.com/dcbickfo/redcache/compare/v0.1.4...v0.1.5
[v0.1.4]: https://github.com/dcbickfo/redcache/compare/v0.1.3...v0.1.4
[v0.1.3]: https://github.com/dcbickfo/redcache/compare/v0.1.2...v0.1.3
[v0.1.2]: https://github.com/dcbickfo/redcache/compare/v0.1.1...v0.1.2
[v0.1.1]: https://github.com/dcbickfo/redcache/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/dcbickfo/redcache/releases/tag/v0.1.0
