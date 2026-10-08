"""STEP-04: scoped read-only EverOS skill-hit discovery on the accepted adapter.

These tests pin the one narrow boundary this slice adds to ``EverOSAdapter``:
``search_skill_candidates`` is the smallest sanitized read-only scoped public
keyword query, following the accepted ``search_case_candidates`` public-surface
pattern.

- the accepted public search surface receives exactly the bound owner,
  application, and project filters plus the sanitized query, the keyword
  method, and a bounded ``top_k``; no session filter or receipt token is
  appended anywhere;
- only raw ``agent_skills`` mappings of this exact scope are returned: EverOS
  presence, similarity (``score``), and any Step-02 generated-skill approval
  stay discovery data only, never procedural guidance, and nothing local is
  written;
- one foreign or malformed neighbor hit is omitted by itself without
  discarding an unrelated in-scope hit, while an invalid query, an invalid
  ``top_k``, a rebound root, and a malformed whole response fail closed.

Whether one discovered hit may later rejoin an exact durable Step-02/Step-03
trust chain is deliberately a separate follow-up decision; this file contains
no store, preparation, or approval wiring.
"""

from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import experience, privacy


class _FakeModel:
    """One deterministic public-model double, dumpable exactly like EverOS."""

    def __init__(self, payload: dict) -> None:
        self.payload = dict(payload)

    def model_dump(self, **_: object) -> dict:
        return dict(self.payload)


class _FakeEverOS:
    """Deterministic public EverOS surface double; records every search call."""

    def __init__(self) -> None:
        self.memorize_calls: list[dict] = []
        self.search_calls: list[dict] = []
        self.skill_results: list[object] = []
        self.case_results: list[object] = []
        self.search_error: Exception | None = None
        self.response_override: object | None = None

    async def memorize(self, payload: dict, **_: object) -> dict:
        self.memorize_calls.append(dict(payload))
        return {"status": "extracted", "message_count": len(payload["messages"])}

    def make_search_request(self, **kwargs: object) -> dict:
        return dict(kwargs)

    async def search(self, request: object) -> object:
        self.search_calls.append(dict(request))
        if self.search_error is not None:
            raise self.search_error
        if self.response_override is not None:
            return self.response_override
        return {
            "request_id": "fake-everos-skill-search",
            "data": {
                "agent_cases": list(self.case_results),
                "agent_skills": list(self.skill_results),
            },
        }


class Step04EverOSSkillHitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.policy = privacy.PrivacyPolicy(
            known_secrets=(experience.SYNTHETIC_SECRET,)
        )
        self.scope = experience.ExperienceScope(
            application="harness",
            project="product",
            namespace="everos-skill-hits",
            owner="root-agent",
        )
        self.fake = _FakeEverOS()
        self.adapter = self._adapter(self.fake)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    # -- helpers -----------------------------------------------------------

    def _adapter(self, fake, *, resolve=None) -> experience.EverOSAdapter:
        base_root = self.root / "everos"
        memory_root = experience.EverOSAdapter.memory_root_for_scope(
            base_root, self.scope
        )
        return experience.EverOSAdapter(
            scope=self.scope,
            base_root=base_root,
            surface=experience.EverOSPublicSurface.from_object(
                fake,
                memory_root=memory_root,
                resolve_memory_root=resolve or (lambda: memory_root),
            ),
            privacy_policy=self.policy,
        )

    def _skill(
        self, *, skill_id: str = "skill-parser-lock", **overrides: object
    ) -> dict:
        skill: dict = {
            "id": skill_id,
            "agent_id": self.adapter.everos_owner_id,
            "app_id": self.adapter.everos_application_id,
            "project_id": self.adapter.everos_project_id,
            "name": "parser lock recovery",
            "description": "Inspect the local parser lock before the network path.",
            "content": "Check the local parser lock, then rerun the discriminating test.",
            "confidence": 0.8,
            "maturity_score": 0.6,
            "source_case_ids": ["case-parser-lock"],
            "score": 0.5,
        }
        skill.update(overrides)
        return skill

    def _search(self, *, query: str = "parser lock recovery", top_k: int = 100):
        return asyncio.run(
            self.adapter.search_skill_candidates(query=query, top_k=top_k)
        )

    def _assert_read_only(self) -> None:
        # The query writes nothing local and promotes nothing: no memorize
        # call happens and the bound memory root is never created on disk.
        self.assertEqual([], self.fake.memorize_calls)
        self.assertFalse(self.adapter.memory_root.exists())

    # -- the valid scoped hit ----------------------------------------------

    def test_valid_scoped_skill_hit_returns_the_raw_mapping(self) -> None:
        skill = self._skill()
        self.fake.skill_results = [skill]
        # A case hit in the same response never leaks into the skill surface.
        self.fake.case_results = [
            {
                "id": "case-not-a-skill",
                "agent_id": self.adapter.everos_owner_id,
                "app_id": self.adapter.everos_application_id,
                "project_id": self.adapter.everos_project_id,
            }
        ]

        result = self._search()

        # The hit is the exact raw remote mapping: presence, similarity, and
        # every other field stay discovery data, with nothing promoted.
        self.assertEqual([skill], result)
        self.assertIsInstance(result[0], dict)
        self.assertEqual(1, len(self.fake.search_calls))
        self.assertEqual(
            {
                "agent_id": self.adapter.everos_owner_id,
                "app_id": self.adapter.everos_application_id,
                "project_id": self.adapter.everos_project_id,
                "query": "parser lock recovery",
                "method": "keyword",
                "top_k": 100,
            },
            self.fake.search_calls[-1],
        )
        self._assert_read_only()

    def test_public_request_sanitizes_the_query_and_honors_top_k(self) -> None:
        skill = self._skill()
        self.fake.skill_results = [skill]

        result = self._search(
            query=f"parser lock {experience.SYNTHETIC_SECRET} recovery", top_k=7
        )

        self.assertEqual([skill], result)
        request = self.fake.search_calls[-1]
        self.assertEqual(
            f"parser lock {privacy.REDACTION_MARKER} recovery", request["query"]
        )
        self.assertEqual(7, request["top_k"])
        self.assertNotIn(experience.SYNTHETIC_SECRET, str(request))
        self._assert_read_only()

    def test_empty_agent_skills_is_an_honest_empty_result(self) -> None:
        self.assertEqual([], self._search())
        self.assertEqual(1, len(self.fake.search_calls))
        self._assert_read_only()

    def test_skill_query_uses_no_session_filter_or_appended_token(self) -> None:
        first = self._skill(skill_id="skill-one", source_case_ids=["case-one"])
        second = self._skill(skill_id="skill-two", source_case_ids=["case-two"])
        self.fake.skill_results = [first, second]

        result = self._search(query="parser lock recovery")

        # Both same-scope hits stay discoverable: no session or receipt token
        # narrows the query, because a skill is discoverable before any local
        # ingestion session exists.
        self.assertEqual([first, second], result)
        request = self.fake.search_calls[-1]
        self.assertNotIn("session_id", request)
        self.assertNotIn("session_id", str(request))
        self.assertEqual("parser lock recovery", request["query"])
        self.assertEqual(
            {"agent_id", "app_id", "project_id", "query", "method", "top_k"},
            set(request),
        )
        self._assert_read_only()

    # -- foreign and malformed neighbors -----------------------------------

    def test_foreign_or_malformed_neighbor_skills_are_omitted_individually(
        self,
    ) -> None:
        skill = self._skill()
        model_skill = self._skill(skill_id="skill-from-public-model")
        self.fake.skill_results = [
            "not-an-object",
            {},
            {"id": "skill-no-scope"},
            self._skill(skill_id="skill-foreign-owner", agent_id="other-owner"),
            self._skill(skill_id="skill-foreign-app", app_id="other-app"),
            self._skill(skill_id="skill-foreign-project", project_id="other-project"),
            self._skill(id=""),
            self._skill(id=None),
            skill,
            _FakeModel(model_skill),
        ]

        result = self._search()

        self.assertEqual([skill, model_skill], result)
        self.assertEqual(1, len(self.fake.search_calls))
        self._assert_read_only()

    # -- fail-closed boundaries --------------------------------------------

    def test_invalid_query_fails_before_any_remote_call(self) -> None:
        for query in ("", "   ", "\t\n"):
            with self.assertRaises(experience.ExperienceError):
                self._search(query=query)
        self.assertEqual([], self.fake.search_calls)
        self._assert_read_only()

    def test_invalid_top_k_fails_before_any_remote_call(self) -> None:
        for top_k in (0, -1, True, False, 2.5, "10", None):
            with self.assertRaises(experience.ExperienceError):
                self._search(top_k=top_k)
        self.assertEqual([], self.fake.search_calls)
        self._assert_read_only()

    def test_rebound_root_fails_closed_and_recovers_when_it_matches(self) -> None:
        memory_root = self.adapter.memory_root
        holder = {"value": memory_root}
        adapter = self._adapter(self.fake, resolve=lambda: holder["value"])
        skill = self._skill()
        self.fake.skill_results = [skill]

        holder["value"] = self.root / "everos-elsewhere"
        with self.assertRaises(experience.ScopeBoundaryError):
            asyncio.run(adapter.search_skill_candidates(query="parser lock recovery"))
        self.assertEqual([], self.fake.search_calls)

        holder["value"] = memory_root
        self.assertEqual(
            [skill],
            asyncio.run(adapter.search_skill_candidates(query="parser lock recovery")),
        )
        self.assertEqual(1, len(self.fake.search_calls))
        self._assert_read_only()

    def test_malformed_whole_responses_fail_closed(self) -> None:
        malformed = (
            "not-an-object",
            {"request_id": "no-data"},
            {"data": []},
            {"data": {"agent_skills": "not-a-list"}},
            {"data": {"agent_skills": {"id": "skill-object-not-list"}}},
        )
        for response in malformed:
            self.fake.response_override = response
            with self.assertRaises(experience.ExperienceError):
                self._search()
            self.fake.response_override = None
        # Each malformed whole response failed after its own request, and no
        # optional guidance or local state came out of any of them.
        self.assertEqual(len(malformed), len(self.fake.search_calls))
        self._assert_read_only()

    def test_remote_search_failure_propagates_without_local_effect(self) -> None:
        self.fake.search_error = RuntimeError("everos skill search unavailable")

        with self.assertRaises(RuntimeError):
            self._search()

        self.assertEqual(1, len(self.fake.search_calls))
        self._assert_read_only()


if __name__ == "__main__":
    unittest.main()
