# Contributing

Thank you for improving Runtime Sentinel.

1. Open an issue for behavior changes or significant new APIs.
2. Fork the repository and branch from `main`.
3. Add focused tests that cover success, failure, cancellation, and relevant races.
4. Run `ruff check src tests examples benchmarks`, `mypy src/runtime_sentinel`, and `pytest`.
5. Update documentation and `CHANGELOG.md` when behavior changes.
6. Open a pull request explaining the problem, design, trade-offs, and verification.

Keep the event loop non-blocking and preserve the immutable domain model. New infrastructure
must implement a narrow protocol and include an integration test. Benchmarks must describe the
environment and must never be presented as universal performance claims.

By participating, you agree to treat other contributors with respect and focus review on the
work rather than the person.

