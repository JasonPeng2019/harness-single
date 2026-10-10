"""Shared pytest fixtures.

Cache invalidation:
    ``load_settings`` (and the timezone helper that reads it) are
    ``functools.cache``-d for hot paths in production. Tests that
    monkeypatch ``EVEROS_*`` env vars must see fresh settings on each
    function — clear both caches around every test to keep results
    deterministic regardless of declaration order.

"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _isolate_everos_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Pin ``EVEROS_ROOT`` so no test reads the developer's own ``everos.toml``.

    ``Settings`` resolves its TOML source through ``resolve_root()``, so any
    field a test does not pass explicitly is filled from the real config file
    on the machine running the suite. A developer who has, say, enabled
    ``[observability]`` locally then sees failures in tests that assert the
    default-off behaviour — green in CI, red on their machine, and unrelated to
    whatever they were changing.

    Tests that exercise root resolution itself override this: their own
    ``setenv`` / ``delenv`` runs inside the test body, after this fixture.
    """
    monkeypatch.setenv("EVEROS_ROOT", str(tmp_path))


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> Iterator[None]:
    import structlog

    from everos.component.utils import datetime as dt_module
    from everos.config import load_settings
    from everos.memory import _partition_locks

    # ``configure_logging`` (called by some e2e fixtures / the CLI entry)
    # sets ``cache_logger_on_first_use=True``; once a logger is cached,
    # ``structlog.testing.capture_logs`` can no longer intercept events,
    # which silently breaks log-assertion tests that run *after* it in the
    # same process. Reset structlog to defaults around every test so that
    # global config never leaks across the suite.
    # ``_partition_locks`` caches one ``asyncio.Lock`` per (strategy,
    # partition key) and never evicts it — correct for a single-loop
    # production process, wrong across tests: pytest-asyncio gives each
    # test its own event loop, so the second test to touch a partition
    # gets a lock bound to a dead loop and the strategy dies with
    # ``RuntimeError: ... is bound to a different event loop``. It hits
    # the parametrised suites hardest, where both backends replay the
    # same agent ids, and it surfaces as a dead-lettered OME run rather
    # than a test error — reported as a flake on whichever parameter
    # happens to run second.
    structlog.reset_defaults()
    load_settings.cache_clear()
    dt_module._display_tz.cache_clear()
    _partition_locks._reset_for_tests()
    yield
    structlog.reset_defaults()
    load_settings.cache_clear()
    dt_module._display_tz.cache_clear()
    _partition_locks._reset_for_tests()


@pytest.fixture(autouse=True)
def _reset_embedding_capability_singleton() -> Iterator[None]:
    """Force embedding capability to ``available=False`` for every test.

    ``get_embedding_capability()`` lazily builds a process-wide singleton
    on first call (see ``component.embedding.accessor``). Leaving the
    cache as ``None`` between tests lets the accessor read ambient
    settings — ``~/.everos/everos.toml`` on a developer machine, ``.env``
    when loaded — which turns test outcomes into a function of the host
    environment. Pre-seeding a ``Capability(provider=None)`` keeps the
    suite hermetic: any body-guard that reads ``.available`` sees
    ``False`` unless the test explicitly opts in by re-assigning
    ``acc._capability`` (or patching the strategy module's own
    ``get_embedding_capability`` reference).
    """
    import everos.component.embedding.accessor as acc
    from everos.component.embedding import EmbeddingCapability

    acc._capability = EmbeddingCapability(provider=None)
    yield
    acc._capability = None


@pytest.fixture(autouse=True)
def _reset_rerank_capability_singleton() -> Iterator[None]:
    """Force rerank capability to ``available=False`` for every test.

    See :func:`_reset_embedding_capability_singleton` for rationale — the
    rerank accessor has the same lazy-singleton shape.
    """
    import everos.component.rerank.accessor as acc
    from everos.component.rerank import RerankCapability

    acc._capability = RerankCapability(provider=None)
    yield
    acc._capability = None


@pytest.fixture(autouse=True)
def _reset_multimodal_capability_singleton() -> Iterator[None]:
    """Force multimodal capability to ``available=False`` for every test.

    See :func:`_reset_embedding_capability_singleton` for rationale — the
    multimodal accessor has the same lazy-singleton shape.
    """
    import everos.component.multimodal.accessor as acc
    from everos.component.multimodal import MultimodalLLMCapability

    acc._capability = MultimodalLLMCapability(provider=None)
    yield
    acc._capability = None


@pytest.fixture(scope="session")
def long_conversation() -> dict:
    """Original multi-topic project conversation for persistence coverage."""
    from tests.fixtures.project_data import project_conversation

    return project_conversation()
