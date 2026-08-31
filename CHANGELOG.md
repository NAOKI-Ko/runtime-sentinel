# Changelog

All notable changes follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions
follow semantic versioning.

## [Unreleased]

## [0.1.0] - 2026-08-31

### Added

- Initial asyncio supervisor with structured concurrency and graceful shutdown.
- Immutable worker state machine, heartbeat monitoring, retries, and failure propagation.
- Priority-aware DAG scheduling with cycle detection.
- Circuit breaker, token bucket, async resource pool, and fault injection.
- Typed event bus, metrics abstraction, SQLite snapshots/event log, and status CLI.
- Property, integration, concurrency, timeout, and failure tests.

[Unreleased]: https://github.com/NAOKI-Ko/runtime-sentinel/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/NAOKI-Ko/runtime-sentinel/releases/tag/v0.1.0
