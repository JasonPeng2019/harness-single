"""STEP-04 correction: one real bounded search-stage deadline.

These regressions execute the enforceable deadline unit directly.  A store
callable that never returns and a lazy iterator that yields once and then
blocks must both return control inside their assigned slice; their late values
never enter the packet; a blocked store never removes a later store's own
bounded chance inside the same stage or lets the stage overrun its allowance.
"""

from __future__ import annotations

import sys
import tempfile
import threading
import time
import unittest
from collections.abc import Mapping
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from memory_harness import (
    config,
    contracts,
    preparation,
    search,
    store,
    templates,
)


class FakeClock:
    """One injected clock shared by the service so time can be controlled."""

    def __init__(self, start: float = 1000.0) -> None:
        self.value = float(start)

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += float(seconds)


class SlowScope(Mapping):
    """A candidate scope that consumes controlled clock time when normalized."""

    def __init__(self, data, clock, advance):
        self._data = dict(data)
        self._clock = clock
        self._advance = float(advance)

    def __getitem__(self, key):
        self._clock.advance(self._advance)
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)


def representation(limits: config.PreparationLimits, tokens: list[str]) -> dict:
    return templates.representation_identity(limits=limits) | {
        "tokens": tokens,
        "route": "ordinary",
    }


class SearchStageDeadlineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.clock = FakeClock()
        self.memory_store = store.MemoryStore(self.root / "memory-state.sqlite3")
        self.memory_store.initialize()
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
        self.release = threading.Event()

    def tearDown(self) -> None:
        # Let anything the deadline abandoned finish, so no test leaves live
        # helper work behind (the abandoned workers are daemon threads).
        self.release.set()
        self.memory_store.close()
        self.temporary.cleanup()

    # -- helpers -----------------------------------------------------------

    def _limits(self, **overrides) -> config.PreparationLimits:
        raw = {
            "default_deadline_seconds": 300.0,
            "execution_reserve_seconds": 10.0,
            "standard_stage_seconds": 2.0,
            "store_seconds": 0.5,
            "minimum_optional_slice_seconds": 0.05,
        }
        raw.update(overrides)
        return config.resolve_limits(raw)

    def _evidence(self, logical_id: str = "case-1", revision: str = "r1", **overrides):
        item = {
            "kind": "historical_evidence",
            "logical_id": logical_id,
            "revision_id": revision,
            "payload": {"summary": "prior regression evidence"},
            "scope": {
                "app": "demo",
                "project": "p",
                "namespace": "reviewed",
                "owner": "owner-1",
            },
            "origin": "everos",
            "representation": representation(self.limits(), ["regression", "failure"]),
            "score": 0.5,
            "freshness": "live",
        }
        item.update(overrides)
        return item

    def limits(self) -> config.PreparationLimits:
        return self._limits()

    def _store(self, store_id, kind, items, *, scope=None, freshness="live"):
        return search.SearchStore(
            store_id=store_id,
            kind=kind,
            query=lambda query, items=items: items,
            scope=scope,
            freshness=freshness,
        )

    def _store_of(self, store_id, kind, query):
        return search.SearchStore(store_id=store_id, kind=kind, query=query)

    def _prepare(self, stores, *, limits=None, clock=None):
        service = preparation.PreparationService(
            store=self.memory_store,
            limits=limits or self._limits(),
            clock=clock or time.monotonic,
        )
        return service.prepare(
            task_card=self.card,
            plan=self.candidate,
            objective_id="objective-1",
            route="ordinary",
            stores=list(stores),
        )

    def _attempts(self, outcome):
        return {entry["store_id"]: entry for entry in outcome.trace["attempts"]}

    def _logical_ids(self, outcome):
        return [item["logical_id"] for item in outcome.trace["candidates"]]

    def _selected_ids(self, outcome):
        return [
            item["logical_id"]
            for item in outcome.trace["candidates"]
            if item["disposition"] == "selected"
        ]

    def _assert_abandoned_work_is_daemon(self) -> None:
        workers = [item for item in threading.enumerate() if item.name.startswith("memory-store:")]
        self.assertTrue(workers, "the abandoned store work must stay attributable")
        self.assertTrue(all(item.daemon for item in workers))

    # -- real wall-clock bounds --------------------------------------------

    def test_forever_blocking_store_call_returns_control_inside_its_slice(self) -> None:
        def blocking(query):
            self.release.wait()
            return [self._evidence(logical_id="case-late")]

        started = time.monotonic()
        outcome = self._prepare(
            [
                self._store_of("slow", "historical_evidence", blocking),
                self._store("everos", "historical_evidence", [self._evidence()]),
            ]
        )
        elapsed = time.monotonic() - started
        attempts = self._attempts(outcome)
        self.assertLess(elapsed, 1.5, "a call that never returns must not consume the stage")
        self.assertEqual("timed-out", attempts["slow"]["status"])
        # The store really waited out its own slice before control returned.
        self.assertGreaterEqual(attempts["slow"]["elapsed_seconds"], 0.4)
        self.assertEqual("completed", attempts["everos"]["status"])
        self.assertEqual(["case-1"], self._selected_ids(outcome))
        self.assertNotIn("case-late", self._logical_ids(outcome))
        self._assert_abandoned_work_is_daemon()

    def test_lazy_iterable_blocking_during_materialization_is_bounded(self) -> None:
        def lazy(query):
            def results():
                yield self._evidence(logical_id="case-late")
                self.release.wait()
                yield self._evidence(logical_id="case-later")

            return results()

        started = time.monotonic()
        outcome = self._prepare(
            [
                self._store_of("lazy", "historical_evidence", lazy),
                self._store("everos", "historical_evidence", [self._evidence()]),
            ]
        )
        elapsed = time.monotonic() - started
        attempts = self._attempts(outcome)
        self.assertLess(elapsed, 1.5, "lazy iteration must stay inside the bounded slice")
        self.assertEqual("timed-out", attempts["lazy"]["status"])
        self.assertEqual("completed", attempts["everos"]["status"])
        self.assertEqual(["case-1"], self._selected_ids(outcome))
        self.assertNotIn("case-late", self._logical_ids(outcome))
        self.assertNotIn("case-later", self._logical_ids(outcome))
        self._assert_abandoned_work_is_daemon()

    def test_stage_deadline_bounds_a_sequence_of_blocked_stores(self) -> None:
        def blocking(query):
            self.release.wait()
            return [self._evidence(logical_id="case-late")]

        limits = self._limits(standard_stage_seconds=1.0, store_seconds=0.5)
        started = time.monotonic()
        outcome = self._prepare(
            [
                self._store_of(f"slow-{index}", "historical_evidence", blocking)
                for index in range(4)
            ],
            limits=limits,
        )
        elapsed = time.monotonic() - started
        attempts = self._attempts(outcome)
        self.assertLess(elapsed, 1.5, "the one stage deadline bounds every store together")
        self.assertEqual("timed-out", attempts["slow-0"]["status"])
        self.assertEqual("timed-out", attempts["slow-1"]["status"])
        # Nothing is left of the stage allowance: the stores behind it are
        # honestly recorded as unattempted rather than silently squeezed in.
        self.assertEqual("unattempted-by-budget", attempts["slow-2"]["status"])
        self.assertTrue(attempts["slow-2"]["reason"])
        self.assertEqual([], self._logical_ids(outcome))
        self.assertEqual("no_optional_memory", outcome.trace["outcome"])
        self._assert_abandoned_work_is_daemon()

    def test_a_blocked_store_never_removes_a_later_stores_bounded_chance(self) -> None:
        def blocking(query):
            self.release.wait()
            return [self._evidence(logical_id="case-late")]

        def slowish(query):
            time.sleep(0.15)
            return [self._evidence(logical_id="case-slowish")]

        started = time.monotonic()
        outcome = self._prepare(
            [
                self._store_of("slow", "historical_evidence", blocking),
                self._store_of("slowish", "historical_evidence", slowish),
                self._store("everos", "historical_evidence", [self._evidence()]),
            ]
        )
        elapsed = time.monotonic() - started
        attempts = self._attempts(outcome)
        self.assertLess(elapsed, 2.5)
        self.assertEqual("timed-out", attempts["slow"]["status"])
        # Every enabled store that fits the stage gets its own positive bounded
        # chance, even behind a store that never returns.
        self.assertEqual("completed", attempts["slowish"]["status"])
        self.assertEqual("completed", attempts["everos"]["status"])
        self.assertEqual(["case-1", "case-slowish"], sorted(self._selected_ids(outcome)))
        self.assertNotIn("case-late", self._logical_ids(outcome))

    # -- controlled-clock late work ----------------------------------------

    def test_controlled_clock_late_normalization_adds_no_candidates(self) -> None:
        on_time = self._evidence(logical_id="case-early")
        late = self._evidence(
            logical_id="case-late",
            scope=SlowScope(self._evidence()["scope"], self.clock, 500.0),
        )
        never_reached = self._evidence(logical_id="case-never-reached")
        outcome = self._prepare(
            [self._store("everos", "historical_evidence", [on_time, late, never_reached])],
            clock=self.clock,
        )
        attempts = self._attempts(outcome)
        self.assertEqual("timed-out", attempts["everos"]["status"])
        self.assertIn("late", attempts["everos"]["reason"])
        # Normalization and scoring ran past the assigned slice: neither the
        # late value nor anything behind it may contribute to the packet.
        self.assertEqual([], self._logical_ids(outcome))
        self.assertEqual(0, attempts["everos"]["candidates"])
        self.assertEqual(0, attempts["everos"]["accepted"])
        self.assertEqual("no_optional_memory", outcome.trace["outcome"])
        durable = self.memory_store.get_search_trace(
            outcome.preparation["preparation_id"]
        )
        self.assertEqual("timed-out", durable["attempts"][0]["status"])
        self.assertTrue(durable["attempts"][0]["reason"])


    # -- blocking normalization and finalization ---------------------------

    def test_blocking_normalization_returns_at_the_deadline(self) -> None:
        """ROOT reproduction: a 2.0s normalizer must not hold a 0.1s stage."""

        class BlockingPayload(Mapping):
            def __getitem__(self, key):
                time.sleep(2.0)
                return {"summary": "late evidence"}[key]

            def __iter__(self):
                return iter(("summary",))

            def __len__(self) -> int:
                return 1

        item = self._evidence(logical_id="case-blocking")
        item["payload"] = BlockingPayload()
        limits = self._limits(
            standard_stage_seconds=0.1,
            store_seconds=0.1,
            minimum_optional_slice_seconds=0.01,
        )
        started = time.monotonic()
        outcome = self._prepare(
            [self._store("everos", "historical_evidence", [item])], limits=limits
        )
        elapsed = time.monotonic() - started
        attempts = self._attempts(outcome)
        self.assertLess(elapsed, 1.0, "a blocking normalizer must not hold the stage")
        self.assertEqual("timed-out", attempts["everos"]["status"])
        self.assertEqual([], self._logical_ids(outcome))
        self._assert_abandoned_work_is_daemon()
        published = outcome.trace["candidates"]
        # Let the abandoned normalization finish: its late values must never
        # reach the packet that was already published.
        time.sleep(2.5)
        self.assertEqual([], published, "late normalization must not publish")
        self.assertEqual([], self._logical_ids(outcome))

    def test_static_injected_clock_cannot_replenish_the_real_stage_allowance(self) -> None:
        """ROOT reproduction: one real stage bound covers materialization and normalization.

        With the injected clock frozen at 1000.0, a 0.1-second stage that
        spends about 0.08 seconds materializing a store and about 0.08 seconds
        normalizing its single candidate used to return after roughly 0.157
        real seconds and still report the attempt as completed, because the
        normalization allowance was recomputed from the injected clock.  Both
        phases must consume one real absolute stage deadline: the frozen clock
        may record logical facts but may never replenish real phase time.
        """

        class SlowPayload(Mapping):
            """A payload that consumes real wall-clock time when normalized."""

            def __getitem__(self, key):
                time.sleep(0.08)
                return {"summary": "late evidence"}[key]

            def __iter__(self):
                return iter(("summary",))

            def __len__(self) -> int:
                return 1

        def slow_store(query):
            # About 0.08s of real materialization inside a 0.1s stage.
            time.sleep(0.08)
            item = self._evidence(logical_id="case-two-phase")
            item["payload"] = SlowPayload()
            return [item]

        # The normalization phase must always be admitted into whatever real
        # slice is left, so the abandoned-worker evidence below is
        # deterministic even when the materialization sleep overshoots.
        limits = self._limits(
            standard_stage_seconds=0.1,
            store_seconds=0.1,
            minimum_optional_slice_seconds=0.0,
        )
        engine = search.BoundedSearch(limits=limits, clock=self.clock)
        objective = templates.objective_representation(
            "Fix the regression failure in the parser test",
            route="ordinary",
            limits=limits,
        )
        started = time.monotonic()
        result = engine.run(
            objective=objective,
            stores=[
                search.SearchStore(
                    store_id="everos", kind="historical_evidence", query=slow_store
                )
            ],
            route="ordinary",
            stage_seconds=0.1,
            rounds=1,
        )
        elapsed = time.monotonic() - started
        attempts = {entry["store_id"]: entry for entry in result.attempts}
        # The injected clock must not hand normalization a second fresh slice:
        # the stage costs about its one real 0.1s allowance, never the ~0.16s
        # sum of two independent phase allowances.
        self.assertGreaterEqual(elapsed, 0.05, "the store really consumed wall-clock time")
        self.assertLess(
            elapsed,
            0.14,
            "materialization and normalization must share one real stage allowance",
        )
        self.assertEqual("timed-out", attempts["everos"]["status"])
        # Either the normalization phase overran the remaining real slice or
        # the store consumed the whole slice itself; both are late, and both
        # must be recorded truthfully instead of passing as completed.
        self.assertTrue(attempts["everos"]["reason"], "the late attempt needs its real reason")
        self.assertEqual(0, attempts["everos"]["accepted"])
        self.assertEqual(0, attempts["everos"]["candidates"])
        self.assertEqual([], result.candidates)
        self.assertEqual([], result.delivered)
        self.assertEqual("no_optional_memory", result.outcome)
        # Daemon ownership of abandoned workers is asserted by the
        # blocking-store, lazy-materialization, and blocking-normalizer
        # regressions; here only the shared real bound is asserted.
        published = result.candidates
        # Let the abandoned normalization finish: its late values must never
        # reach the packet that was already published.
        time.sleep(0.4)
        self.assertEqual([], published, "late normalization must not publish")

    def test_blocking_finalization_returns_at_the_deadline(self) -> None:
        """Deduplication, ranking, and capacity selection share the same bound."""

        class BlockingFinalization(search.BoundedSearch):
            def _finalize(self, *args, **kwargs):
                time.sleep(2.0)
                finalized, delivered = super()._finalize(*args, **kwargs)
                return finalized, delivered

        limits = self._limits(
            standard_stage_seconds=0.1,
            store_seconds=0.1,
            minimum_optional_slice_seconds=0.01,
        )
        engine = BlockingFinalization(limits=limits)
        objective = templates.objective_representation(
            "Fix the regression failure in the parser test",
            route="ordinary",
            limits=limits,
        )
        started = time.monotonic()
        result = engine.run(
            objective=objective,
            stores=[
                search.SearchStore(
                    store_id="everos",
                    kind="historical_evidence",
                    query=lambda query: [self._evidence()],
                )
            ],
            route="ordinary",
            stage_seconds=0.1,
            rounds=1,
        )
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 1.0, "blocking finalization must not hold the stage")
        self.assertEqual([], result.candidates)
        self.assertEqual([], result.delivered)
        self.assertEqual("no_optional_memory", result.outcome)
        self.assertTrue(result.reason, "the overrun must be recorded truthfully")
        attempts = {entry["store_id"]: entry for entry in result.attempts}
        self.assertEqual("completed", attempts["everos"]["status"])
        published = result.candidates
        # The abandoned finalization may only write to its own discarded copy.
        time.sleep(2.5)
        self.assertEqual([], published, "late finalization must not publish")


if __name__ == "__main__":
    unittest.main()
