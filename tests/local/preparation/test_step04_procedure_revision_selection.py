"""STEP-04 procedure revision selection: one delivered revision per procedure.

These regressions execute the bounded search selection unit directly.  Two
eligible revisions of one logical procedure compete: the narrower authorized
scope wins before the discovery score, exactly like the accepted
``TrustedProcedureService.resolve_atlas`` policy, with the revision id as the
deterministic tie.  The nonselected revision stays visible as an explicit
rejection with its selection reason, a rejected or invalid competitor never
suppresses an independently eligible procedure, and duplicate sightings of one
logical revision merge provenance without a relevance bonus.
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import config, contracts, preparation, search, store, templates


class FakeClock:
    """One injected clock shared by the service so time can be controlled."""

    def __init__(self, start: float = 1000.0) -> None:
        self.value = float(start)

    def __call__(self) -> float:
        return self.value


def representation(limits: config.PreparationLimits, tokens: list[str]) -> dict:
    return templates.representation_identity(limits=limits) | {
        "tokens": tokens,
        "route": "ordinary",
    }


class ProcedureRevisionSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.clock = FakeClock()
        self.limits = config.resolve_limits(
            {
                "default_deadline_seconds": 300.0,
                "execution_reserve_seconds": 10.0,
                "store_seconds": 8.0,
                "minimum_optional_slice_seconds": 1.0,
                "standard_stage_seconds": 20.0,
            }
        )
        self.memory_store = store.MemoryStore(self.root / "memory-state.sqlite3")
        self.memory_store.initialize()
        self.service = preparation.PreparationService(
            store=self.memory_store, limits=self.limits, clock=self.clock
        )
        self.card = contracts.make_task_card(
            task="Fix the regression failure in the parser test", base_commit="base-1"
        )
        self.candidate = contracts.make_plan(
            plan_id="candidate-plan",
            objective_id="objective-1",
            route="ordinary",
            state="candidate",
            content={"steps": ["draft"]},
        )

    def tearDown(self) -> None:
        self.memory_store.close()
        self.temporary.cleanup()

    # -- helpers -----------------------------------------------------------

    def _procedure(self, logical_id, revision="rev-1", **overrides):
        item = {
            "kind": "procedure",
            "logical_id": logical_id,
            "revision_id": revision,
            "payload": {"steps": [f"{logical_id}:{revision}"]},
            "scope": {
                "app": "demo",
                "project": "p",
                "namespace": "shared",
                "owner": "owner-1",
            },
            "origin": "atlas",
            "representation": representation(self.limits, ["regression", "failure"]),
            "score": 0.5,
            "specificity": "project",
            "freshness": "live",
            "approval_status": "approved",
            "designation": "current",
            "predicates_ok": True,
        }
        item.update(overrides)
        item["payload_digest"] = contracts.sha256_hex(item["payload"])
        return item

    def _store(self, store_id, items, *, scope=None):
        return search.SearchStore(
            store_id=store_id,
            kind="procedure",
            query=lambda query, items=items: items,
            scope=scope,
        )

    def _prepare(self, stores):
        return self.service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            route="ordinary",
            stores=list(stores),
        )

    def _procedures(self, outcome):
        return [
            item
            for item in outcome.trace["candidates"]
            if item["kind"] == "procedure"
        ]

    def _by_revision(self, outcome):
        return {item["revision_id"]: item for item in self._procedures(outcome)}

    def _selected_revisions(self, outcome):
        return sorted(
            item["revision_id"]
            for item in self._procedures(outcome)
            if item["disposition"] == "selected"
        )

    def _selected_per_logical(self, outcome):
        counts: dict[str, int] = {}
        for item in self._procedures(outcome):
            if item["disposition"] == "selected":
                counts[item["logical_id"]] = counts.get(item["logical_id"], 0) + 1
        return counts

    # -- one delivered revision per logical procedure ----------------------

    def test_narrower_authorized_scope_beats_a_higher_scored_revision(self) -> None:
        local = self._procedure("procedure-1", "rev-local", specificity="local", score=0.2)
        shared = self._procedure("procedure-1", "rev-shared", specificity="shared", score=0.9)
        outcome = self._prepare(
            [self._store("atlas-primary", [shared]), self._store("atlas-mirror", [local])]
        )
        records = self._by_revision(outcome)
        self.assertEqual(["rev-local"], self._selected_revisions(outcome))
        self.assertEqual({"procedure-1": 1}, self._selected_per_logical(outcome))
        rejected = records["rev-shared"]
        self.assertEqual("rejected", rejected["disposition"])
        reason_text = " ".join(rejected["reasons"])
        self.assertIn("rev-local", reason_text)
        self.assertIn("scope", reason_text)
        # The losing revision stays visible at its recorded discovery score.
        self.assertEqual(0.9, rejected["score"])

    def test_equal_specificity_and_score_resolve_by_a_deterministic_tie(self) -> None:
        alpha = self._procedure("procedure-1", "rev-alpha", specificity="project", score=0.5)
        beta = self._procedure("procedure-1", "rev-beta", specificity="project", score=0.5)
        first = self._prepare([self._store("atlas", [alpha, beta])])
        reversed_order = self._prepare([self._store("atlas", [beta, alpha])])
        self.assertEqual(["rev-alpha"], self._selected_revisions(first))
        self.assertEqual(["rev-alpha"], self._selected_revisions(reversed_order))

    def test_duplicate_sightings_merge_provenance_without_a_score_bonus(self) -> None:
        first = self._procedure("procedure-1", "rev-1", specificity="project", score=0.2)
        second = self._procedure("procedure-1", "rev-1", specificity="project", score=0.35)
        outcome = self._prepare(
            [self._store("atlas", [first]), self._store("local-atlas-mirror", [second])]
        )
        records = self._by_revision(outcome)
        self.assertEqual(["rev-1"], list(records))
        record = records["rev-1"]
        self.assertEqual("selected", record["disposition"])
        store_ids = {entry["store_id"] for entry in record["provenance"]}
        self.assertEqual({"atlas", "local-atlas-mirror"}, store_ids)
        # One sighting never adds relevance: the maximum observed score stands.
        self.assertEqual(0.35, record["score"])

    def test_mixed_scope_duplicate_sightings_keep_the_narrower_scope(self) -> None:
        """One revision seen at two authorized scopes keeps the narrower one.

        The shared sighting was discovered at a higher score, but the narrower
        authorized scope wins before the discovery score, and no sighting may
        borrow relevance from another: the representative keeps the score and
        the source identity of its own narrower sighting, in either sighting
        order.
        """

        for order in ("shared-first", "local-first"):
            with self.subTest(order=order):
                shared = self._procedure(
                    "procedure-1", "rev-a", specificity="shared", score=0.8
                )
                local = self._procedure(
                    "procedure-1", "rev-a", specificity="local", score=0.1
                )
                competitor = self._procedure(
                    "procedure-1", "rev-b", specificity="project", score=0.7
                )
                stores = (
                    [
                        self._store("atlas-shared", [shared, competitor]),
                        self._store("local-atlas", [local]),
                    ]
                    if order == "shared-first"
                    else [
                        self._store("local-atlas", [local]),
                        self._store("atlas-shared", [shared, competitor]),
                    ]
                )
                outcome = self._prepare(stores)
                records = self._by_revision(outcome)
                self.assertEqual(["rev-a"], self._selected_revisions(outcome))
                representative = records["rev-a"]
                self.assertEqual("selected", representative["disposition"])
                # The representative is the narrower local sighting: it keeps
                # its own discovered score and source identity instead of the
                # shared sighting's 0.8.
                self.assertEqual("local-atlas", representative["source_id"])
                self.assertEqual(0.1, representative["score"])
                store_ids = {
                    entry["store_id"] for entry in representative["provenance"]
                }
                self.assertEqual({"atlas-shared", "local-atlas"}, store_ids)
                loser = records["rev-b"]
                self.assertEqual("rejected", loser["disposition"])
                self.assertIn("rev-a", " ".join(loser["reasons"]))

    def test_revoked_duplicate_sighting_cannot_boost_an_eligible_revision(self) -> None:
        """An ineligible duplicate never changes the eligible representative.

        The revoked sighting carries a much higher score and a different
        payload; the eligible representative keeps its own score, source
        identity, and payload in either sighting order, the revoked value
        stays attributable in the trace, and the independently eligible
        competitor is still delivered.
        """

        for order in ("eligible-first", "revoked-first"):
            with self.subTest(order=order):
                eligible = self._procedure(
                    "procedure-1", "rev-a", specificity="local", score=0.2
                )
                revoked = self._procedure(
                    "procedure-1",
                    "rev-a",
                    specificity="local",
                    score=0.99,
                    revoked=True,
                    payload={"steps": ["revoked-copy"]},
                )
                competitor = self._procedure(
                    "procedure-1", "rev-b", specificity="local", score=0.8
                )
                unrelated = self._procedure(
                    "procedure-2", "rev-c", specificity="local", score=0.3
                )
                eligible_store = self._store("atlas-mirror", [eligible, unrelated])
                revoked_store = self._store("atlas", [revoked, competitor])
                outcome = self._prepare(
                    [eligible_store, revoked_store]
                    if order == "eligible-first"
                    else [revoked_store, eligible_store]
                )
                records = self._by_revision(outcome)
                self.assertEqual(["rev-b", "rev-c"], self._selected_revisions(outcome))
                self.assertEqual(
                    {"procedure-1": 1, "procedure-2": 1},
                    self._selected_per_logical(outcome),
                )
                duplicate = records["rev-a"]
                self.assertEqual("rejected", duplicate["disposition"])
                self.assertEqual(0.2, duplicate["score"])
                self.assertEqual("atlas-mirror", duplicate["source_id"])
                self.assertEqual(
                    {"steps": ["procedure-1:rev-a"]}, duplicate["payload"]
                )
                reason_text = " ".join(duplicate["reasons"])
                self.assertIn("revoked", reason_text)
                self.assertIn("rev-b", reason_text)

    def test_a_rejected_revision_never_suppresses_an_independent_procedure(self) -> None:
        contested_local = self._procedure(
            "procedure-1", "rev-local", specificity="local", score=0.2
        )
        contested_shared = self._procedure(
            "procedure-1", "rev-shared", specificity="shared", score=0.9
        )
        revoked = self._procedure("procedure-1", "rev-revoked", revoked=True)
        independent = self._procedure(
            "procedure-2", "rev-independent", specificity="project", score=0.4
        )
        outcome = self._prepare(
            [self._store("atlas", [contested_shared, revoked, contested_local, independent])]
        )
        self.assertEqual(["rev-independent", "rev-local"], self._selected_revisions(outcome))
        self.assertEqual(
            {"procedure-1": 1, "procedure-2": 1}, self._selected_per_logical(outcome)
        )
        records = self._by_revision(outcome)
        self.assertEqual("rejected", records["rev-shared"]["disposition"])
        self.assertTrue(records["rev-shared"]["reasons"])
        self.assertEqual("rejected", records["rev-revoked"]["disposition"])
        self.assertTrue(
            any("revoked" in reason for reason in records["rev-revoked"]["reasons"])
        )


if __name__ == "__main__":
    unittest.main()