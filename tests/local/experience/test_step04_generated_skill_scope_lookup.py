"""STEP-04: the durable Step-02 candidate lookup a remote-skill rejoin reads.

These tests pin the one narrow read this slice adds to ``store.MemoryStore``:
``read_generated_skill_candidates_for_scope(skill_id, scope)`` is the smallest
read-only lookup the later EverOS approved-skill rejoin needs, and it returns
durable candidates as discovery data only.

- an exact stable skill id plus the exact normalized four-part scope returns
  the intact durable generated candidates of that skill and scope through one
  read-only connection created and closed by the calling thread;
- the returned records stay the non-authoritative, proposed Step-02
  candidates: no approval or guidance exists or is derived, nothing is
  written, and nothing is promoted;
- a foreign-scope row, an altered row whose retained content hash no longer
  matches, and a row outside the candidate contract are each omitted by
  themselves, so a corrupted or foreign neighbor never hides an intact
  same-scope candidate;
- the read works on a bounded worker thread -- the thread the accepted
  bounded search hands a store -- and closes its own connection there while
  the caller-thread connection stays usable.

This file deliberately does not repeat the accepted Step-02 approval matrix
(``tests/local/experience/test_generated_trust.py``); it pins only the join
this slice adds.
"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
import threading
import traceback
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import contracts, store


class LookupProbeStore(store.MemoryStore):
    """Record the exact read-only connection the lookup opened and closed."""

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.read_only_connections: list[sqlite3.Connection] = []

    def _open_read_only_connection(self) -> sqlite3.Connection:
        connection = super()._open_read_only_connection()
        self.read_only_connections.append(connection)
        return connection


class Step04GeneratedSkillScopeLookupTests(unittest.TestCase):
    """The exact read-only skill-candidate lookup over durable Step-02 rows."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.memory_store = LookupProbeStore(self.root / "memory-state.sqlite3")
        self.memory_store.initialize()
        self.scope = {
            "application": "harness",
            "project": "product-a",
            "namespace": "everos-skill-lookup",
            "owner": "root-agent",
        }
        self.foreign_scope = dict(self.scope, owner="another-agent")

    def tearDown(self) -> None:
        self.memory_store.close()
        self.temporary.cleanup()

    # -- helpers -----------------------------------------------------------

    def _source_case(self, suffix: str) -> dict:
        """One retained source-case provenance object of a durable candidate."""

        return {
            "case_id": f"case-{suffix}",
            "case_receipt_id": f"case-receipt-{suffix}",
            "trajectory_id": f"trajectory-{suffix}",
            "review_receipt_id": f"review-receipt-{suffix}",
            "review_receipt_digest": f"review-receipt-digest-{suffix}",
        }

    def _candidate(
        self,
        *,
        skill_id: str = "skill-parser-lock",
        suffix: str = "one",
        scope: dict | None = None,
        content: str = "Inspect the parser lock before network recovery.",
        metadata: dict | None = None,
    ) -> dict:
        return contracts.make_generated_skill_candidate(
            scope=scope or self.scope,
            skill_id=skill_id,
            content=content,
            source_cases=[self._source_case(suffix)],
            metadata=metadata if metadata is not None else {"source": "synthetic"},
            created_at="2026-09-23T00:00:00Z",
        )

    def _record(self, candidate: dict) -> dict:
        return self.memory_store.record_generated_skill_candidate(candidate)

    def _mutate(self, statement: str, parameters: tuple) -> None:
        writer = sqlite3.connect(str(self.memory_store.path))
        try:
            writer.execute(statement, parameters)
            writer.commit()
        finally:
            writer.close()

    def _lookup(
        self, skill_id: str = "skill-parser-lock", scope: dict | None = None
    ) -> list[dict]:
        return self.memory_store.read_generated_skill_candidates_for_scope(
            skill_id, scope or self.scope
        )

    def _approval_rows(self) -> int:
        reader = sqlite3.connect(str(self.memory_store.path))
        try:
            return reader.execute(
                "SELECT COUNT(*) FROM generated_skill_approvals"
            ).fetchone()[0]
        finally:
            reader.close()

    # -- the exact scope read ---------------------------------------------

    def test_exact_skill_and_scope_returns_only_intact_durable_candidates(self) -> None:
        recorded = self._record(self._candidate())

        candidates = self._lookup()

        self.assertEqual([recorded], candidates)
        # The read is discovery data only: the exact stored record comes back
        # as the non-authoritative, proposed candidate it was stored as, and
        # no approval or guidance is attached to it.
        self.assertEqual(contracts.GENERATED_SKILL_SCHEMA, candidates[0]["schema"])
        self.assertEqual("generated", candidates[0]["origin"])
        self.assertEqual("proposed", candidates[0]["state"])
        self.assertEqual(recorded["content_hash"], candidates[0]["content_hash"])
        self.assertNotIn("approval_id", candidates[0])
        self.assertEqual(0, self._approval_rows())
        # The lookup opened exactly one read-only connection on this calling
        # thread and closed it before returning.
        self.assertEqual(1, len(self.memory_store.read_only_connections))
        closed = self.memory_store.read_only_connections[-1]
        with self.assertRaises(sqlite3.ProgrammingError):
            closed.execute("SELECT 1")
        # The durable state is unchanged: the accepted shared-connection
        # reader still resolves the exact same record.
        self.assertEqual(
            recorded, self.memory_store.get_generated_skill_candidate(recorded["candidate_id"])
        )

    def test_other_skill_and_other_scope_rows_are_foreign_and_omitted(self) -> None:
        intact = self._record(self._candidate(suffix="intact"))
        # Same skill, different scope: the row was never this scope's evidence.
        self._record(self._candidate(suffix="other-scope", scope=self.foreign_scope))
        # Same scope, different stable skill id: never this skill's evidence.
        self._record(self._candidate(skill_id="skill-other-lock", suffix="other-skill"))

        self.assertEqual([intact], self._lookup())

    def test_altered_content_hash_row_is_omitted_without_hiding_an_intact_row(self) -> None:
        intact = self._record(self._candidate(suffix="intact"))
        altered = self._record(self._candidate(suffix="altered"))
        self._mutate(
            "UPDATE generated_skill_candidates SET content_hash = ? "
            "WHERE candidate_id = ?",
            ("tampered-content-hash", altered["candidate_id"]),
        )

        candidates = self._lookup()

        self.assertEqual([intact], candidates)
        self.assertNotIn(altered["candidate_id"], [item["candidate_id"] for item in candidates])

    def test_row_outside_the_candidate_contract_is_omitted_by_itself(self) -> None:
        intact = self._record(self._candidate(suffix="intact"))
        broken = self._record(self._candidate(suffix="broken"))
        # The stored scope JSON is altered to another exact four-part scope
        # while the row keeps the requested scope digest: the row can no
        # longer revalidate its own contract identity.
        self._mutate(
            "UPDATE generated_skill_candidates SET scope = ? WHERE candidate_id = ?",
            (
                contracts.canonical_json(dict(self.foreign_scope)).decode("utf-8"),
                broken["candidate_id"],
            ),
        )

        self.assertEqual([intact], self._lookup())

    def test_missing_skill_id_is_rejected_before_any_connection_opens(self) -> None:
        self._record(self._candidate())
        opened_before = len(self.memory_store.read_only_connections)
        with self.assertRaises(store.StoreError):
            self.memory_store.read_generated_skill_candidates_for_scope("", self.scope)
        self.assertEqual(opened_before, len(self.memory_store.read_only_connections))

    def test_inexact_scope_is_rejected(self) -> None:
        self._record(self._candidate())
        for broken_scope in (
            {"application": "harness", "project": "product-a", "owner": "root-agent"},
            dict(self.scope, owner=""),
            "harness/product-a/everos-skill-lookup/root-agent",
        ):
            with self.assertRaises(contracts.ContractError):
                self.memory_store.read_generated_skill_candidates_for_scope(
                    "skill-parser-lock", broken_scope
                )

    # -- the bounded worker thread -----------------------------------------

    def test_lookup_runs_on_a_worker_thread_and_closes_its_own_connection(self) -> None:
        recorded = self._record(self._candidate())
        caller_thread = threading.get_ident()
        outcome: dict = {}

        def worker() -> None:
            try:
                outcome["candidates"] = self.memory_store.read_generated_skill_candidates_for_scope(
                    "skill-parser-lock", self.scope
                )
            except BaseException:  # pragma: no cover - surfaced through outcome
                outcome["traceback"] = traceback.format_exc()

        thread = threading.Thread(target=worker, name="bounded-search-worker")
        thread.start()
        thread.join(timeout=30.0)
        self.assertFalse(thread.is_alive(), "the bounded worker read must finish")
        self.assertNotIn("traceback", outcome, outcome.get("traceback"))
        self.assertEqual([recorded], outcome["candidates"])
        # The worker read really ran on the worker thread, and its read-only
        # connection was created and closed there.
        self.assertNotEqual(caller_thread, thread.ident)
        self.assertTrue(self.memory_store.read_only_connections)
        for connection in self.memory_store.read_only_connections:
            with self.assertRaises(sqlite3.ProgrammingError):
                connection.execute("SELECT 1")
        # The caller-thread connection stays usable after the worker read.
        self.assertEqual(
            recorded,
            self.memory_store.get_generated_skill_candidate(recorded["candidate_id"]),
        )

    def test_lookup_returns_every_intact_candidate_in_a_deterministic_order(self) -> None:
        first = self._record(self._candidate(suffix="alpha", content="Inspect the lock first."))
        second = self._record(self._candidate(suffix="beta", content="Escalate the lock second."))

        expected = sorted([first, second], key=lambda item: item["candidate_id"])
        self.assertEqual(expected, self._lookup())


if __name__ == "__main__":
    unittest.main()
