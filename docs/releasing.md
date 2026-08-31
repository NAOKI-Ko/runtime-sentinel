# Releasing

This project follows semantic versioning. Releases are built from an annotated tag on `main`; the
GitHub workflow is the source of release artifacts. PyPI publication is optional until trusted
publishing or a repository secret is deliberately configured.

## Prepare and review

1. Start `release/vX.Y.Z` from the latest `main`.
2. Update the version in `pyproject.toml` and move completed entries from `[Unreleased]` to a dated
   Keep a Changelog section.
3. Run the local gate:

   ```bash
   python -m pip install '.[dev]'
   ruff check src tests examples benchmarks
   mypy src/runtime_sentinel
   pytest --cov-report=xml
   python -m build
   python -m twine check dist/*
   ```

4. Inspect the wheel and sdist, including `runtime_sentinel/py.typed`.
5. Open a pull request describing quality gates, packaging, release behavior, and breaking changes.
6. Merge only after required CI succeeds, then update local `main` with a fast-forward pull.

## Tag and publish

Create an annotated `vX.Y.Z` tag on the reviewed `main` commit and push it. The release workflow
checks that the tag exactly matches `project.version`, reruns all quality gates, builds wheel and
sdist, validates them with Twine, and creates a non-draft GitHub Release with both artifacts.

Registry publication requires preconfigured trusted publishing or credentials. Never add tokens to
the repository, workflow, documentation, or release artifacts.

## Failed release or rollback

Do not move or force-push a published tag. If the workflow fails before a GitHub Release exists,
fix the cause through a new pull request and decide whether the unused tag can be safely deleted or
whether to issue the next patch version. If artifacts are already public, preserve the release for
auditability, document the defect, and publish a corrected patch release. Mark a genuinely unsafe
artifact as deprecated in the release notes rather than silently replacing it.
