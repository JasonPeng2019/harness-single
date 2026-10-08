---
paths:
  - "tests/**/*.py"
---

# Testing rule

Tests mirror the source layout: `tests/unit/test_<layer>/...`,
`tests/integration/...`, `tests/e2e/...`.

- **Structure**: `tests/unit/` mirrors `src/everos/` package-for-package. Put a test
  next to where its subject lives in the mirror.
- **Async**: `pytest-asyncio` is in `auto` mode — write `async def test_*` directly,
  no marker needed.
- **Markers** (default run excludes both — `-m "not slow and not live_llm"`):
  - `@pytest.mark.slow` — tests ≥ ~10s.
  - `@pytest.mark.live_llm` — needs real LLM/embedder credentials.
  Keep unit tests fast and credential-free; push anything needing real services
  behind a marker or into `integration`/`e2e`.
- **Fixtures**: shared fixtures live in the nearest `conftest.py`. The root conftest
  resets module caches (settings/logging/datetime) per test — rely on that for
  isolation rather than mutating globals.
- **Module docstring** on each test file stating what contract it pins (see existing
  tests for the style).
- **Coverage gate**: `make cov` enforces 80% (`--cov-fail-under=80`). New code should
  not drop coverage below the gate.
- Run `make test` (unit) and `make integration` before pushing; both run in CI.
