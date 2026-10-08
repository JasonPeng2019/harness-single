"""STEP-04: the future EverOS generated-skill SearchStore call gate.

These tests pin the one narrow slice that admits a *future* EverOS
generated-skill procedure store to the bounded preparation search: the finite
``everos_generated_skill`` source marker, its mandatory
``requires_network=True`` declaration, and the admission branch that decides
whether the store's real remote query starts at all.

- the marker is admitted only while ``generated_skill_use`` is true outside
  ``restricted_local``: the recording store's query starts in that exact
  configuration and never starts otherwise;
- ``atlas_shared_retrieval`` alone never enables the marker: with
  generated-skill use off the query is suppressed even while shared retrieval
  is on, and with shared retrieval off the query still starts while
  generated-skill use is on;
- a disabled admission records a truthful ``disabled`` trace attempt and the
  bounded search never runs the store, so no remote work occurs to be filtered
  afterwards;
- the marker cannot declare a local/non-network call, and the accepted local
  generated, local curated, Atlas, caller, and EverOS case-store gates keep
  their observed behavior.

No remote adapter is implemented here and no candidate is ever delivered by
the recording store: these tests prove the call gate, not a trust join.
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import (
    config,
    contracts,
    preparation,
    privacy,
    search,
    templates,
)

ROOT_REPLAN = {
    "requested_by": "ROOT",
    "reason": "the tests exercise the explicit ROOT replan path",
}

MARKER = "everos_generated_skill"
STORE_ID = "everos-generated-skill-procedures"


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.value = float(start)

    def __call__(self) -> float:
        return self.value


class RecordingStore:
    """One future EverOS generated-skill store that records every query start.

    Its query performs no remote call by itself: the recorded call count is
    exactly the number of times the accepted bounded search started the store,
    so suppression is proven on the call gate instead of on filtered output.
    """

    def __init__(self, *, source_kind: str = MARKER, requires_network: bool = True,
                 kind: str = "procedure", store_id: str = STORE_ID) -> None:
        self.calls: list[dict] = []
        self.store = search.SearchStore(
            store_id=store_id,
            kind=kind,
            query=self.query,
            freshness="live",
            specificity="project",
            requires_network=requires_network,
            source_kind=source_kind,
        )

    def query(self, payload):
        self.calls.append(dict(payload))
        return []

    @property
    def call_count(self) -> int:
        return len(self.calls)


class Step04EverosGeneratedSkillCallGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.clock = FakeClock()
        self.limits = config.resolve_limits(
            {
                "default_deadline_seconds": 300.0,
                "execution_reserve_seconds": 60.0,
                "store_seconds": 8.0,
                "minimum_optional_slice_seconds": 1.0,
            }
        )
        self.policy = privacy.PrivacyPolicy()
        self.service = preparation.PreparationService(
            limits=self.limits, clock=self.clock
        )
        self.card = contracts.make_task_card(
            task="Repair the parser regression", base_commit="base-1"
        )
        self.candidate = contracts.make_plan(
            plan_id="candidate-plan",
            objective_id="parser-regression-repair",
            route="ordinary",
            state="candidate",
            content={"steps": ["draft"]},
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    # -- helpers -----------------------------------------------------------

    def _prepare(self, stores, **overrides):
        arguments = {
            "task_card": self.card,
            "plan": self.candidate,
            "objective_id": "parser-regression-repair",
            "route": "ordinary",
            "stores": list(stores),
            "root_replan": ROOT_REPLAN,
        }
        arguments.update(overrides)
        return self.service.prepare(**arguments)

    @staticmethod
    def _attempts(outcome):
        return {entry["store_id"]: entry for entry in outcome.trace["attempts"]}

    # -- the marker declaration rule ---------------------------------------

    def test_marker_cannot_declare_a_local_non_network_call(self) -> None:
        with self.assertRaises(search.SearchError) as caught:
            search.SearchStore(
                store_id=STORE_ID,
                kind="procedure",
                query=lambda payload: [],
                requires_network=False,
                source_kind=MARKER,
            )
        self.assertIn("shared/remote", str(caught.exception))

        # The honest declaration is accepted, and the accepted local sources
        # keep their own opposite rule: a local procedure source never
        # declares a shared/remote call.
        store = search.SearchStore(
            store_id=STORE_ID,
            kind="procedure",
            query=lambda payload: [],
            requires_network=True,
            source_kind=MARKER,
        )
        self.assertTrue(store.requires_network)
        self.assertEqual(MARKER, store.source_kind)
        with self.assertRaises(search.SearchError):
            search.SearchStore(
                store_id="local-curated",
                kind="procedure",
                query=lambda payload: [],
                requires_network=True,
                source_kind="curated_local_procedure",
            )
        with self.assertRaises(search.SearchError):
            search.SearchStore(
                store_id="local-generated",
                kind="procedure",
                query=lambda payload: [],
                requires_network=True,
                source_kind="generated_local_procedure",
            )
        with self.assertRaises(search.SearchError):
            search.SearchStore(
                store_id="unknown-marker",
                kind="procedure",
                query=lambda payload: [],
                requires_network=True,
                source_kind="everos_skill",
            )

    # -- the admitted configuration starts the real query ------------------

    def test_marker_query_starts_only_with_generated_skill_use_outside_restricted_local(self) -> None:
        recording = RecordingStore()
        outcome = self._prepare([recording.store])
        attempts = self._attempts(outcome)
        self.assertEqual("completed", attempts[STORE_ID]["status"])
        self.assertEqual(1, recording.call_count)
        # The query began on the bounded search's own worker and consumed the
        # sanitized bounded payload only.
        payload = recording.calls[0]
        self.assertEqual("ordinary", payload["route"])
        self.assertTrue(payload["tokens"])

        # shared retrieval off, generated-skill use on: the marker still
        # starts, because its authority is generated-skill use, not Atlas.
        recording = RecordingStore()
        self._prepare(
            [recording.store],
            request={"generated_skill_use": True, "atlas_shared_retrieval": False},
        )
        self.assertEqual(1, recording.call_count)

    def test_generated_skill_use_off_suppresses_the_query_even_with_shared_retrieval_on(self) -> None:
        recording = RecordingStore()
        outcome = self._prepare(
            [recording.store],
            request={"generated_skill_use": False, "atlas_shared_retrieval": True},
        )
        attempts = self._attempts(outcome)
        self.assertEqual("disabled", attempts[STORE_ID]["status"])
        self.assertIn("generated_skill_use", attempts[STORE_ID]["reason"])
        self.assertEqual(0, recording.call_count)
        self.assertEqual([], outcome.trace["candidates"])

    def test_restricted_local_never_starts_the_query(self) -> None:
        recording = RecordingStore()
        outcome = self._prepare(
            [recording.store],
            network_mode="restricted_local",
            request={"generated_skill_use": True, "atlas_shared_retrieval": True},
        )
        attempts = self._attempts(outcome)
        self.assertEqual("disabled", attempts[STORE_ID]["status"])
        self.assertIn("restricted-local", attempts[STORE_ID]["reason"])
        self.assertEqual(0, recording.call_count)
        self.assertEqual([], outcome.trace["candidates"])

    # -- the accepted neighboring gates keep their observed behavior -------

    def test_neighboring_source_gates_keep_their_observed_behavior(self) -> None:
        # Curated local procedures stay eligible with shared retrieval and
        # generated-skill use both off; restricted-local never suppresses them.
        curated = RecordingStore(
            source_kind="curated_local_procedure",
            requires_network=False,
            store_id="local-curated-procedures",
        )
        outcome = self._prepare(
            [curated.store],
            network_mode="restricted_local",
            request={"generated_skill_use": False, "atlas_shared_retrieval": False},
        )
        self.assertEqual(
            "completed", self._attempts(outcome)["local-curated-procedures"]["status"]
        )
        self.assertEqual(1, curated.call_count)

        # Generated-origin local sources still make no query at all while
        # generated_skill_use is off.
        generated = RecordingStore(
            source_kind="generated_local_procedure",
            requires_network=False,
            store_id="local-generated-procedures",
        )
        outcome = self._prepare(
            [generated.store],
            request={"generated_skill_use": False, "atlas_shared_retrieval": True},
        )
        self.assertEqual(
            "disabled", self._attempts(outcome)["local-generated-procedures"]["status"]
        )
        self.assertEqual(0, generated.call_count)

        # A caller-supplied remote store keeps the accepted generic gate:
        # atlas_shared_retrieval enables it, restricted_local suppresses it.
        caller = RecordingStore(
            source_kind="caller",
            requires_network=True,
            store_id="caller-remote-store",
        )
        self._prepare([caller.store], request={"atlas_shared_retrieval": True})
        self.assertEqual(1, caller.call_count)
        caller_off = RecordingStore(
            source_kind="caller",
            requires_network=True,
            store_id="caller-remote-store",
        )
        self._prepare([caller_off.store], request={"atlas_shared_retrieval": False})
        self.assertEqual(0, caller_off.call_count)
        caller_local = RecordingStore(
            source_kind="caller",
            requires_network=True,
            store_id="caller-remote-store",
        )
        self._prepare([caller_local.store], network_mode="restricted_local")
        self.assertEqual(0, caller_local.call_count)


if __name__ == "__main__":
    unittest.main()
