"""Agent pipeline e2e: 5 original project-maintenance sessions drive /add + /flush.

Drives the full HTTP route through to storage, exercising the agent-track
pipeline (boundary → memcell → extract_agent_case → trigger_skill_clustering
→ extract_agent_skill) with real LLM and real embedder credentials — this
module's own ``_opt_in_real_embedding`` fixture opts the embedding
capability back in (see its docstring for why that is necessary and why
it does not weaken the global hermeticity fixture). Rerank is
deliberately left at its hermetic default: nothing on this write path
touches rerank, so opting it in would only widen the credential surface
with no coverage benefit.

Mixed tenancy by design (sender_id alignment from fixture):

    agent_parser   (1 project session)                 ┐ independent
    agent_math     (1 project session)                 ┘ owners
    agent_backend  (3 project sessions)                  shared

Concurrency strategy (workaround for the known
``trigger_skill_clustering`` read-modify-write race on a shared owner_id):

    Phase 1: parser + math concurrent via asyncio.gather (disjoint owners)
    Phase 2: 3 backend sessions sequential (same owner, would race)

Once the cluster race is fixed in production, Phase 2 can collapse into
the same gather and the test will still pass — the assertions are
race-free, only the driver is conservative.

White-box assertions (audit trail of internal surfaces touched):
    - sqlite ``memcell`` rows per session_id
    - filesystem ``<root>/agents/<agent>/.cases/*.md`` presence
    - LanceDB ``agent_case`` rows by ``owner_id`` (count + session_id set)
    - LanceDB ``agent_skill`` rows by ``owner_id`` (aggregate floor — see
      ``test_agent_pipeline_e2e_mixed_tenancy``'s section 4.5)
    - OME ``run_record``: no dead-lettered ``extract_agent_skill`` run
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator
from pathlib import Path

import httpx
import pytest

import everos.component.embedding.accessor as _embedding_accessor
from everos.infra.ome.records import RunStatus
from everos.infra.persistence.index import agent_case_repo, agent_skill_repo, eq
from everos.infra.persistence.markdown import AgentCaseDailyFrontmatter
from everos.service.memorize import _get_engine
from tests.fixtures.project_data import project_agent_session

_PARSER_SESSION = "project_parser"
_MATH_SESSION = "project_math"
_BACKEND_SESSIONS = (
    "project_backend_reset",
    "project_backend_increment",
    "project_backend_validation",
)

_AGENT_PARSER = "agent_parser"
_AGENT_MATH = "agent_math"
_AGENT_BACKEND = "agent_backend"

# Phase 3 drain budget: OME chain (case → cluster → skill) writes md in
# stages, each picked up by cascade. Multiple drain rounds with brief
# sleeps let the chain quiesce without false-positive completion.
_DRAIN_ROUNDS = 4
_DRAIN_TIMEOUT_SECONDS = 300.0
_DRAIN_INTER_ROUND_SLEEP_SECONDS = 5.0


@pytest.fixture(autouse=True)
def _opt_in_real_embedding(
    _reset_embedding_capability_singleton: None,
) -> Iterator[None]:
    """Opt this module's test into a real embedding capability.

    ``tests/conftest.py``'s ``_reset_embedding_capability_singleton``
    autouse fixture pins the capability to unavailable for every test
    (hermeticity); its docstring says a test may "explicitly opt in by
    re-assigning ``acc._capability``". This test needs it:
    ``trigger_skill_clustering`` and ``extract_agent_skill`` both
    body-guard on ``get_embedding_capability().available`` and return
    early when it is false, so without opting in here the skill chain
    would never run — exactly the coverage gap this fixture closes.
    Scoped to this file only (not the global fixture) so every other
    test keeps its hermetic default.

    Requesting ``_reset_embedding_capability_singleton`` as a parameter
    — rather than relying on collection/declaration order between this
    file's conftest chain and the root conftest — makes pytest's
    dependency graph guarantee this fixture's setup runs after it and its
    teardown before it. Rerank is left untouched (see the module
    docstring): this fixture reads and writes only the embedding
    capability, so there is nothing to order it against.

    Setting ``_capability = None`` (rather than constructing a capability
    object directly) makes the accessor rebuild lazily from
    ``load_settings()`` on next call, picking up the real ``.env``
    credentials ``tests/e2e/conftest.py`` loads at import time.
    """
    _embedding_accessor._capability = None
    yield
    _embedding_accessor._capability = None


def _load_fixture(session_id: str) -> dict:
    agent = (
        _AGENT_PARSER
        if session_id == _PARSER_SESSION
        else (_AGENT_MATH if session_id == _MATH_SESSION else _AGENT_BACKEND)
    )
    return project_agent_session(session_id, agent)


async def _drive_session(
    client: httpx.AsyncClient, session_data: dict
) -> tuple[str, str]:
    """Run /add followed by /flush for one trajectory; return status."""
    sid = session_data["everos_session_id"]
    msgs = session_data["messages"]
    # MessageItemDTO.max_length=500; project sessions stay below the request limit.
    r = await client.post(
        "/api/v1/memory/add",
        json={"session_id": sid, "messages": msgs},
        timeout=600.0,
    )
    assert r.status_code == 200, (
        f"{sid}: /add returned {r.status_code} — {r.text[:300]}"
    )
    r = await client.post(
        "/api/v1/memory/flush",
        json={"session_id": sid},
        timeout=600.0,
    )
    assert r.status_code == 200, (
        f"{sid}: /flush returned {r.status_code} — {r.text[:300]}"
    )
    return sid, r.json()["data"]["status"]


@pytest.mark.slow
@pytest.mark.live_llm
async def test_agent_pipeline_e2e_mixed_tenancy(
    async_client: httpx.AsyncClient,
    core_pipeline_runtime: Path,
    pipeline_done_poll: Callable[..., Awaitable[None]],
    memcell_count: Callable[..., Awaitable[int]],
) -> None:
    """Five project sessions yield agent_case + agent_skill on three agents."""
    memory_root = core_pipeline_runtime

    parser_fx = _load_fixture(_PARSER_SESSION)
    math_fx = _load_fixture(_MATH_SESSION)
    backend_fxs = [_load_fixture(s) for s in _BACKEND_SESSIONS]

    # ── Phase 1: independent owners concurrent ────────────────────────────
    await asyncio.gather(
        _drive_session(async_client, parser_fx),
        _drive_session(async_client, math_fx),
    )

    # ── Phase 2: shared owner_id, sequential to dodge cluster race ────────
    for fx in backend_fxs:
        await _drive_session(async_client, fx)

    # ── Phase 3: drain OME chain + cascade ────────────────────────────────
    for _ in range(_DRAIN_ROUNDS):
        await pipeline_done_poll(deadline_seconds=_DRAIN_TIMEOUT_SECONDS)
        await asyncio.sleep(_DRAIN_INTER_ROUND_SLEEP_SECONDS)

    # ── Phase 4: assertions ───────────────────────────────────────────────

    # 4.1 every session produced ≥1 memcell
    all_sessions = (_PARSER_SESSION, _MATH_SESSION, *_BACKEND_SESSIONS)
    for sid in all_sessions:
        n = await memcell_count(sid)
        assert n >= 1, f"no memcell for session {sid!r} (got {n})"

    # 4.2 each agent has a .cases dir with ≥1 .md file
    agents_dir = memory_root / "default_app" / "default_project" / "agents"
    case_dir_name = AgentCaseDailyFrontmatter.DIR_NAME
    for agent_id in (_AGENT_PARSER, _AGENT_MATH, _AGENT_BACKEND):
        case_dir = agents_dir / agent_id / case_dir_name
        assert case_dir.is_dir(), f"missing {case_dir!s} for agent={agent_id!r}"
        md_files = list(case_dir.glob("*.md"))
        assert md_files, f"no agent_case md under {case_dir!s}"

    # 4.3 LanceDB agent_case rows per owner
    parser_cases = await agent_case_repo.find_where(eq("owner_id", _AGENT_PARSER))
    math_cases = await agent_case_repo.find_where(eq("owner_id", _AGENT_MATH))
    backend_cases = await agent_case_repo.find_where(eq("owner_id", _AGENT_BACKEND))

    assert len(parser_cases) >= 1, (
        f"no agent_parser rows in LanceDB (got {len(parser_cases)})"
    )
    assert len(math_cases) >= 1, (
        f"no agent_math rows in LanceDB (got {len(math_cases)})"
    )
    # Each backend session writes at least one cell → at least one case per
    # session. Lower bound 3 covers the minimum; LLM may produce more.
    assert len(backend_cases) >= 3, (
        "agent_backend expected ≥3 LanceDB cases (3 sessions), "
        f"got {len(backend_cases)}"
    )

    # 4.4 cross-owner isolation — each agent's cases trace back only to
    # its own sessions
    parser_session_ids = {c.session_id for c in parser_cases}
    assert parser_session_ids == {_PARSER_SESSION}, (
        f"agent_parser cases leaked across sessions: {parser_session_ids}"
    )
    math_session_ids = {c.session_id for c in math_cases}
    assert math_session_ids == {_MATH_SESSION}, (
        f"agent_math cases leaked across sessions: {math_session_ids}"
    )
    backend_session_ids = {c.session_id for c in backend_cases}
    assert backend_session_ids == set(_BACKEND_SESSIONS), (
        f"agent_backend session set mismatch — got {backend_session_ids}, "
        f"want {set(_BACKEND_SESSIONS)}"
    )

    # 4.5 agent_skill — aggregate floor across all three agents. Per-agent
    # emission depends on everalgo's per-case quality gate
    # (skip_quality_threshold, see everalgo/agent_memory/skill_ops.py) —
    # extract_agent_skill itself has no cluster-size gate, so a per-agent
    # floor would be genuinely flaky (a single low-quality trajectory can
    # legitimately yield 0 skills for that agent). The aggregate floor
    # checks that the complete five-session workload exercises the skill
    # chain even if an individual agent's cluster is quality-gated to 0.
    parser_skills = await agent_skill_repo.find_where(eq("owner_id", _AGENT_PARSER))
    math_skills = await agent_skill_repo.find_where(eq("owner_id", _AGENT_MATH))
    backend_skills = await agent_skill_repo.find_where(eq("owner_id", _AGENT_BACKEND))
    total_skills = len(parser_skills) + len(math_skills) + len(backend_skills)
    assert total_skills >= 1, (
        "agent-skill chain produced nothing — the strategy chain "
        "(extract_agent_case → trigger_skill_clustering → extract_agent_skill) "
        "is broken or gated off "
        f"(parser={len(parser_skills)}, math={len(math_skills)}, "
        f"backend={len(backend_skills)})"
    )

    # 4.6 no dead-lettered extract_agent_skill run. Sharper signal than
    # the skill-count floor above and targets this branch's actual defect
    # directly: a dead-letter means the chain attempted and failed
    # (exhausted retries), as opposed to a quality-gated 0-skill outcome,
    # which is not a failure.
    #
    # The dead-letter check alone is vacuous if the strategy never ran at
    # all — zero dead-letters is also what a never-executed strategy
    # looks like. Assert (any status) runs exist first, so the dead-letter
    # assertion below is only non-vacuous because of this check, not
    # because the skill-count floor in 4.5 happened to run first.
    engine = _get_engine()
    all_skill_runs = await engine.list_runs("extract_agent_skill")
    assert all_skill_runs, (
        "extract_agent_skill never ran at all — the dead-letter check "
        "below would be vacuously satisfied by a strategy that never "
        "executed"
    )

    dead_letters = await engine.list_runs(
        "extract_agent_skill", status=RunStatus.DEAD_LETTER
    )
    assert not dead_letters, (
        "extract_agent_skill dead-lettered "
        f"{len(dead_letters)} run(s): {[r.error for r in dead_letters]}"
    )

    # 4.7 strict md ↔ LanceDB parity across every cascade kind
    #
    # The per-owner counts above are loose (LLM-emission-dependent); this
    # check enforces byte-exact id-set + content_sha256 parity across
    # every md the agent pipeline wrote.
    #
    # ``expect_at_least`` pins agent_case (every session writes ≥1 case)
    # so an empty glob would fail loudly. agent_skill is NOT pinned —
    # emission depends on the LLM clustering quality gate per 4.5; a
    # legitimately empty agent_skill md set is still a passing run.
    from tests._consistency_assertions import assert_md_lance_strict_consistent

    await assert_md_lance_strict_consistent(
        memory_root,
        expect_at_least={"agent_case": 1},
    )
