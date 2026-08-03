from __future__ import annotations

import io
import json
import unittest
from datetime import timedelta
from unittest.mock import patch

from orchestrator_harness.notifications import (
    admit_deferred_handoffs,
    preempt_pending_with_higher_priority,
    select_actionable_with_deferred,
)
from orchestrator_harness.tests.support import NOW
from orchestrator_harness.tests.support import SuiteFixture
from orchestrator_harness.stable_io import SafeOutput
from orchestrator_harness.cli import EXIT_OK, ack_command, stop_command, watch_managed
from orchestrator_harness.models import ProcessInfo, ProcessSnapshot


class CleanMHandoffTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = SuiteFixture.create()

    def tearDown(self) -> None:
        self.fixture.close()
    def _signal(self) -> dict[str, object]:
        return {
            "identity": "manager-signal:delta-help", "event_id": "delta-help",
            "type": "MANAGER_SIGNAL", "severity": "info",
            "data": {"lane_id": "delta", "signal_id": "delta-help"},
        }

    def test_deferred_blocked_help_retains_delivery_deadline_order(self) -> None:
        first = self._signal()
        first["event_id"] = "later-deadline"
        first["identity"] = "manager-signal:later-deadline"
        first["data"] = {
            "lane_id": "delta", "signal_id": "later-deadline", "kind": "HELP",
            "agent_blocked": True, "delivery_deadline_utc": "2026-07-30T12:02:00Z",
        }
        second = self._signal()
        second["event_id"] = "earlier-deadline"
        second["identity"] = "manager-signal:earlier-deadline"
        second["data"] = {
            "lane_id": "delta", "signal_id": "earlier-deadline", "kind": "HELP",
            "agent_blocked": True, "delivery_deadline_utc": "2026-07-30T12:01:00Z",
        }
        live = {"lanes": [{"lane_id": "delta", "process_state": "RUNNING_CODEX"}], "requests": [], "helpers": [], "mcps": []}
        deferred = admit_deferred_handoffs(
            {first["identity"]: first, second["identity"]: second}, live,
            observed_at=NOW, acknowledged_event_ids=set(),
            newly_observed_event_ids={"later-deadline", "earlier-deadline"}, existing=[],
        )
        self.assertEqual(["earlier-deadline", "later-deadline"], [item["event"]["event_id"] for item in deferred])
        self.assertEqual([1.5, 1.5], [item["priority"] for item in deferred])

    def test_admitted_signal_uses_stored_rank_after_lane_exit(self) -> None:
        signal = self._signal()
        live_snapshot = {"lanes": [{"lane_id": "delta", "process_state": "RUNNING_CODEX"}], "requests": [], "helpers": [], "mcps": []}
        deferred = admit_deferred_handoffs(
            {signal["identity"]: signal}, live_snapshot, observed_at=NOW,
            acknowledged_event_ids=set(), newly_observed_event_ids={signal["event_id"]},
            existing=[],
        )
        self.assertEqual(1, len(deferred))
        self.assertEqual(3, deferred[0]["priority"])
        exited_snapshot = {"lanes": [{"lane_id": "delta", "process_state": "EXITED"}], "requests": [], "helpers": [], "mcps": []}
        selected, remaining = select_actionable_with_deferred(
            {}, exited_snapshot, observed_at=NOW + timedelta(seconds=1),
            acknowledged_event_ids=set(), newly_observed_event_ids=set(), deferred=deferred,
        )
        self.assertEqual("delta-help", selected["event_id"])
        self.assertEqual([], remaining)

    def test_only_exact_answered_correlated_signal_is_pruned(self) -> None:
        signal = self._signal()
        signal["data"] = {"lane_id": "delta", "signal_id": "delta-help", "correlated_request_answered": False}
        snapshot = {"lanes": [], "requests": [], "helpers": [], "mcps": []}
        deferred = [{
            "event": signal, "priority": 3, "deadline_order": "9999-12-31T23:59:59Z",
            "identity": signal["identity"], "admitted_utc": NOW.isoformat(),
        }]
        current = {signal["identity"]: {**signal, "data": {**signal["data"], "correlated_request_answered": True}}}
        selected, remaining = select_actionable_with_deferred(current, snapshot, observed_at=NOW, acknowledged_event_ids=set(), newly_observed_event_ids=set(), deferred=deferred)
        self.assertIsNone(selected)
        self.assertEqual([], remaining)

    def test_same_output_ack_preserves_deferred_for_post_exit_delivery(self) -> None:
        signal = self._signal()
        live = {"lanes": [{"lane_id": "delta", "process_state": "RUNNING_CODEX"}], "requests": [], "helpers": [], "mcps": []}
        deferred = admit_deferred_handoffs({signal["identity"]: signal}, live, observed_at=NOW, acknowledged_event_ids=set(), newly_observed_event_ids={signal["event_id"]}, existing=[])
        high = {"event_id": "high", "identity": "resource:high", "type": "RESOURCE_CONFLICT", "data": {}}
        store = SafeOutput(
            harness_root=self.fixture.harness_root,
            output_root=self.fixture.config.output_dir,
            forbidden_roots=(self.fixture.suite_root,),
        )
        store.prepare()
        store.save_notification_state(pending=high, acknowledged_event_ids=[], deferred=deferred)
        self.assertTrue(store.acknowledge_notification("high"))
        restored = store.load_notification_state()
        self.assertEqual(1, len(restored["deferred"]))
        selected, remaining = select_actionable_with_deferred(
            {}, {"lanes": [{"lane_id": "delta", "process_state": "EXITED"}], "requests": [], "helpers": [], "mcps": []},
            observed_at=NOW, acknowledged_event_ids={"high"}, newly_observed_event_ids=set(),
            deferred=restored["deferred"],
        )
        self.assertEqual("delta-help", selected["event_id"])
        self.assertEqual([], remaining)

    def test_fresh_output_historical_exited_signal_is_not_admitted(self) -> None:
        signal = self._signal()
        deferred = admit_deferred_handoffs(
            {signal["identity"]: signal},
            {"lanes": [{"lane_id": "delta", "process_state": "EXITED"}], "requests": [], "helpers": [], "mcps": []},
            observed_at=NOW, acknowledged_event_ids=set(), newly_observed_event_ids=set(), existing=[],
        )
        self.assertEqual([], deferred)

    def test_urgent_deferred_help_preempts_checkpoint_and_checkpoint_remains_selectable(self) -> None:
        checkpoint = {
            "identity": "lane:delta:checkpoint", "event_id": "checkpoint",
            "type": "CHECKPOINT_UPDATED", "severity": "info", "data": {},
            "observed_utc": NOW.isoformat(), "notification": "MANAGER_ACTION_REQUIRED",
        }
        help_event = self._signal()
        help_event["data"] = {
            "lane_id": "delta", "signal_id": "delta-help", "kind": "HELP",
            "agent_blocked": True, "delivery_deadline_utc": "2026-07-30T12:01:00Z",
        }
        deferred = [{
            "event": help_event, "priority": 1.5,
            "deadline_order": "2026-07-30T12:01:00Z",
            "identity": help_event["identity"], "admitted_utc": NOW.isoformat(),
        }]
        live = {"lanes": [{"lane_id": "delta", "process_state": "RUNNING_CODEX"}], "requests": [], "helpers": [], "mcps": []}

        selected, remaining, preempted = preempt_pending_with_higher_priority(
            checkpoint, deferred, live, observed_at=NOW,
        )
        self.assertTrue(preempted)
        self.assertEqual("delta-help", selected["event_id"])
        self.assertEqual(["checkpoint"], [item["event"]["event_id"] for item in remaining])
        self.assertEqual(5, remaining[0]["priority"])
        self.assertEqual(checkpoint["observed_utc"], remaining[0]["admitted_utc"])

        restored, after_restore = select_actionable_with_deferred(
            {}, live, observed_at=NOW + timedelta(seconds=1),
            acknowledged_event_ids={"delta-help"}, newly_observed_event_ids=set(),
            deferred=remaining,
        )
        self.assertEqual("checkpoint", restored["event_id"])
        self.assertEqual([], after_restore)

    def test_admitted_priority_allows_urgent_help_to_preempt_non_live_pending_signal(self) -> None:
        pending = self._signal()
        pending.update({
            "admitted_priority": 3,
            "observed_utc": NOW.isoformat(),
            "notification": "MANAGER_ACTION_REQUIRED",
        })
        urgent = self._signal()
        urgent["event_id"] = "urgent-help"
        urgent["identity"] = "manager-signal:urgent-help"
        urgent["data"] = {
            "lane_id": "delta", "signal_id": "urgent-help", "kind": "HELP",
            "agent_blocked": True, "delivery_deadline_utc": "2026-07-30T12:01:00Z",
        }
        deferred = [{
            "event": urgent, "priority": 1.5,
            "deadline_order": "2026-07-30T12:01:00Z",
            "identity": urgent["identity"], "admitted_utc": NOW.isoformat(),
        }]
        non_live = {"lanes": [{"lane_id": "delta", "process_state": "EXITED"}], "requests": [], "helpers": [], "mcps": []}

        selected, remaining, preempted = preempt_pending_with_higher_priority(
            pending, deferred, non_live, observed_at=NOW + timedelta(seconds=1),
        )

        self.assertTrue(preempted)
        self.assertEqual("urgent-help", selected["event_id"])
        self.assertEqual(1.5, selected["admitted_priority"])
        self.assertEqual(["delta-help"], [item["event"]["event_id"] for item in remaining])
        self.assertEqual(3, remaining[0]["priority"])

    def test_equal_or_lower_priority_deferred_work_does_not_preempt(self) -> None:
        pending = self._signal()
        pending["observed_utc"] = NOW.isoformat()
        live = {"lanes": [{"lane_id": "delta", "process_state": "RUNNING_CODEX"}], "requests": [], "helpers": [], "mcps": []}
        equal = self._signal()
        equal["event_id"] = "equal"
        equal["identity"] = "manager-signal:equal"
        checkpoint = {
            "identity": "lane:delta:checkpoint", "event_id": "lower",
            "type": "CHECKPOINT_UPDATED", "severity": "info", "data": {},
        }
        for label, deferred in (
            ("equal", [{"event": equal, "priority": 3, "deadline_order": "9999-12-31T23:59:59Z", "identity": equal["identity"], "admitted_utc": NOW.isoformat()}]),
            ("lower", [{"event": checkpoint, "priority": 5, "deadline_order": "9999-12-31T23:59:59Z", "identity": checkpoint["identity"], "admitted_utc": NOW.isoformat()}]),
        ):
            with self.subTest(label=label):
                selected, remaining, preempted = preempt_pending_with_higher_priority(
                    pending, deferred, live, observed_at=NOW,
                )
                self.assertFalse(preempted)
                self.assertEqual(pending, selected)
                self.assertEqual(deferred, remaining)

    def test_repeated_priority_checks_do_not_duplicate_or_lose_preempted_work(self) -> None:
        checkpoint = {
            "identity": "lane:delta:checkpoint", "event_id": "checkpoint",
            "type": "CHECKPOINT_UPDATED", "severity": "info", "data": {},
            "observed_utc": NOW.isoformat(),
        }
        help_event = self._signal()
        help_event["data"] = {
            "lane_id": "delta", "signal_id": "delta-help", "kind": "HELP",
            "agent_blocked": True, "delivery_deadline_utc": "2026-07-30T12:01:00Z",
        }
        deferred = [{"event": help_event, "priority": 1.5, "deadline_order": "2026-07-30T12:01:00Z", "identity": help_event["identity"], "admitted_utc": NOW.isoformat()}]
        live = {"lanes": [{"lane_id": "delta", "process_state": "RUNNING_CODEX"}], "requests": [], "helpers": [], "mcps": []}
        pending, deferred, _ = preempt_pending_with_higher_priority(checkpoint, deferred, live, observed_at=NOW)

        for _ in range(2):
            pending, deferred, preempted = preempt_pending_with_higher_priority(
                pending, deferred, live, observed_at=NOW,
            )
            self.assertFalse(preempted)
            self.assertEqual("delta-help", pending["event_id"])
            self.assertEqual(["checkpoint"], [item["event"]["event_id"] for item in deferred])

    def test_managed_watch_preempts_checkpoint_for_new_urgent_help_without_checkpoint_ack(self) -> None:
        lane = {
            "lane_id": "delta", "process_state": "RUNNING_CODEX", "operational_state": "RUNNING_CODEX",
            "run_root": "run", "doer": "Delta", "task": "D", "phase": "wait",
        }
        baseline = {
            "process_snapshot_complete": True, "lanes": [lane], "requests": [], "helpers": [], "mcps": [],
            "manager_signals": [], "resource_conflicts": [], "observation_errors": [],
        }
        checkpoint = {**baseline, "lanes": [{**lane, "checkpoint_path": "checkpoint.md", "checkpoint_sha256": "sha"}]}
        urgent_help = {
            **checkpoint,
            "manager_signals": [{
                "signal_id": "urgent-help", "lane_id": "delta", "kind": "HELP",
                "summary": "blocked", "agent_blocked": True,
                "delivery_deadline_utc": "2026-07-30T12:01:00Z",
            }],
        }
        snapshots = [baseline, checkpoint, urgent_help, urgent_help]
        watcher = ProcessInfo(9001, 9000, "python", "watcher", NOW)
        owner = ProcessInfo(9000, 1, "python", "owner", NOW)
        processes = ProcessSnapshot(True, (watcher, owner), (), "fake")
        output = io.StringIO()
        sleeps = 0

        def sleeper(_: float) -> None:
            nonlocal sleeps
            sleeps += 1
            store = SafeOutput(harness_root=self.fixture.harness_root, output_root=self.fixture.config.output_dir, forbidden_roots=(self.fixture.suite_root,))
            store.prepare()
            state = store.load_notification_state()
            if sleeps == 2:
                self.assertEqual("CHECKPOINT_UPDATED", state["pending"]["type"])
            elif sleeps == 3:
                self.assertEqual("urgent-help", state["pending"]["data"]["signal_id"])
                self.assertEqual(["CHECKPOINT_UPDATED"], [item["event"]["type"] for item in state["deferred"]])
                ack_command(self.fixture.config, event_id=state["pending"]["event_id"], clock=lambda: NOW, stream=io.StringIO())
            elif sleeps == 4:
                self.assertEqual("CHECKPOINT_UPDATED", state["pending"]["type"])
                self.assertEqual([], state["deferred"])
                stop_command(self.fixture.config, clock=lambda: NOW, stream=io.StringIO())

        with patch("orchestrator_harness.cli.reconcile", side_effect=snapshots):
            self.assertEqual(EXIT_OK, watch_managed(
                self.fixture.config, process_provider=lambda: processes, clock=lambda: NOW,
                sleeper=sleeper, stream=output, watcher=watcher, owner=owner,
            ))
        notices = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(
            ["CHECKPOINT_UPDATED", "MANAGER_SIGNAL", "CHECKPOINT_UPDATED"],
            [item["type"] for item in notices],
        )

    def test_managed_watch_defers_then_delivers_after_exit_and_exact_ack(self) -> None:
        delta_live = {
            "lane_id": "delta", "process_state": "RUNNING_CODEX", "operational_state": "RUNNING_CODEX",
            "run_root": "run", "doer": "Delta", "task": "D", "phase": "wait",
        }
        signal = {"signal_id": "delta-help", "lane_id": "delta", "kind": "HELP", "summary": "handoff"}
        first = {
            "process_snapshot_complete": True, "lanes": [delta_live], "requests": [], "helpers": [], "mcps": [],
            "manager_signals": [signal], "resource_conflicts": [{"resource": "board:high"}], "observation_errors": [],
        }
        second = {**first, "lanes": [{**delta_live, "process_state": "EXITED", "operational_state": "EXITED"}]}
        snapshots = [first, second, second]
        watcher = ProcessInfo(9001, 9000, "python", "watcher", NOW)
        owner = ProcessInfo(9000, 1, "python", "owner", NOW)
        processes = ProcessSnapshot(True, (watcher, owner), (), "fake")
        output = io.StringIO()
        sleeps = 0

        def sleeper(_: float) -> None:
            nonlocal sleeps
            sleeps += 1
            store = SafeOutput(harness_root=self.fixture.harness_root, output_root=self.fixture.config.output_dir, forbidden_roots=(self.fixture.suite_root,))
            store.prepare()
            if sleeps == 1:
                high = store.load_notification_state()["pending"]
                self.assertEqual("RESOURCE_CONFLICT", high["type"])
                self.assertEqual(1, len(store.load_notification_state()["deferred"]))
                ack_command(self.fixture.config, event_id=high["event_id"], clock=lambda: NOW, stream=io.StringIO())
            elif sleeps == 2:
                stop_command(self.fixture.config, clock=lambda: NOW, stream=io.StringIO())

        with patch("orchestrator_harness.cli.reconcile", side_effect=snapshots):
            self.assertEqual(EXIT_OK, watch_managed(
                self.fixture.config, process_provider=lambda: processes, clock=lambda: NOW,
                sleeper=sleeper, stream=output, watcher=watcher, owner=owner,
            ))
        notices = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(["RESOURCE_CONFLICT", "MANAGER_SIGNAL"], [item["type"] for item in notices])

    def test_managed_restart_preserves_before_ack_and_delivers_after_ack(self) -> None:
        delta_live = {
            "lane_id": "delta", "process_state": "RUNNING_CODEX", "operational_state": "RUNNING_CODEX",
            "run_root": "run", "doer": "Delta", "task": "D", "phase": "wait",
        }
        signal = {"signal_id": "delta-restart-help", "lane_id": "delta", "kind": "HELP", "summary": "handoff"}
        live = {
            "process_snapshot_complete": True, "lanes": [delta_live], "requests": [], "helpers": [], "mcps": [],
            "manager_signals": [signal], "resource_conflicts": [{"resource": "board:high"}], "observation_errors": [],
        }
        exited = {**live, "lanes": [{**delta_live, "process_state": "EXITED", "operational_state": "EXITED"}]}
        owner = ProcessInfo(9000, 1, "python", "owner", NOW)

        def run_once(watcher_id: int, snapshot: dict[str, object], output: io.StringIO) -> None:
            watcher = ProcessInfo(watcher_id, 9000, "python", f"watcher-{watcher_id}", NOW)
            processes = ProcessSnapshot(True, (watcher, owner), (), "fake")
            def stop(_: float) -> None:
                stop_command(self.fixture.config, clock=lambda: NOW, stream=io.StringIO())
            with patch("orchestrator_harness.cli.reconcile", return_value=snapshot):
                self.assertEqual(EXIT_OK, watch_managed(
                    self.fixture.config, process_provider=lambda: processes, clock=lambda: NOW,
                    sleeper=stop, stream=output, watcher=watcher, owner=owner,
                ))

        first_output = io.StringIO()
        run_once(9101, live, first_output)
        store = SafeOutput(harness_root=self.fixture.harness_root, output_root=self.fixture.config.output_dir, forbidden_roots=(self.fixture.suite_root,))
        store.prepare()
        high = store.load_notification_state()["pending"]
        self.assertEqual("RESOURCE_CONFLICT", high["type"])
        self.assertEqual(1, len(store.load_notification_state()["deferred"]))

        second_output = io.StringIO()
        run_once(9102, exited, second_output)
        self.assertEqual(high["event_id"], store.load_notification_state()["pending"]["event_id"])
        self.assertEqual(1, len(store.load_notification_state()["deferred"]))

        self.assertEqual(0, ack_command(self.fixture.config, event_id=high["event_id"], clock=lambda: NOW, stream=io.StringIO()))
        third_output = io.StringIO()
        run_once(9103, exited, third_output)
        notices = [json.loads(line) for line in third_output.getvalue().splitlines()]
        self.assertEqual(["MANAGER_SIGNAL"], [item["type"] for item in notices])
        self.assertEqual("delta-restart-help", notices[0]["data"]["signal_id"])


if __name__ == "__main__":
    unittest.main()
