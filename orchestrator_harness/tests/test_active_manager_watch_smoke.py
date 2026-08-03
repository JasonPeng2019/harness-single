from __future__ import annotations

# pyright: reportImplicitRelativeImport=false

import io
import json
import unittest
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable

from orchestrator_harness.active_management import (
    _request_is_stage_complete,
    acknowledge_management_event,
    project_active_lanes,
    transition_active_management,
)
from orchestrator_harness.cli import (
    EXIT_OK,
    EXIT_TIMEOUT,
    ack_command,
    scan_command,
    stop_command,
    watch_managed,
    watch_once,
    watch_until_actionable,
    watch_until_event,
)
from orchestrator_harness.config import ConfigError, load_config
from orchestrator_harness.events import conditions_from_snapshot
from orchestrator_harness.models import ProcessInfo, ProcessSnapshot, iso_utc
from orchestrator_harness.notifications import select_actionable
from orchestrator_harness.stable_io import (
    ManagedWatcherClaimError,
    PathSafetyError,
    SafeOutput,
)
from orchestrator_harness.tests.support import NOW, SuiteFixture, write_json


class _StopManagedWatch(Exception):
    """Abort a deliberately interrupted managed watcher without starting a process."""


def _lane(
    lane_id: str = "lane-a",
    *,
    state: str = "RUNNING_CODEX",
    doer: str = "Atlas",
    task: str = "A00",
    phase: str = "boundary",
    started: Any = NOW,
    checkpoint_path: str | None = None,
    checkpoint_sha256: str | None = None,
    result_path: str | None = None,
    result_sha256: str | None = None,
    board_tokens: list[str] | None = None,
    mcp_servers: list[str] | None = None,
    resource_ambiguity: list[str] | None = None,
    checkpoint_observed_utc: Any = None,
) -> dict[str, Any]:
    lane = {
        "lane_id": lane_id,
        "process_state": state,
        "operational_state": state,
        "doer": doer,
        "task": task,
        "phase": phase,
        "started_utc": iso_utc(started),
        "thread_id": f"thread-{lane_id}",
        "board_tokens": board_tokens or [],
        "mcp_servers": mcp_servers or [],
        "resources": [],
        "resource_ambiguity": resource_ambiguity or [],
    }
    if checkpoint_path is not None:
        lane["checkpoint_path"] = checkpoint_path
        lane["checkpoint_sha256"] = checkpoint_sha256 or "checkpoint-hash"
    if checkpoint_observed_utc is not None:
        lane["checkpoint_observed_utc"] = iso_utc(checkpoint_observed_utc)
    if result_path is not None:
        lane["result_path"] = result_path
        lane["result_sha256"] = result_sha256 or "result-hash"
    return lane


def _request(
    lane_id: str,
    request_id: str,
    kind: str,
    created: Any,
    *,
    relay_state: str = "UNBOUND",
    lifetime_state: str = "LIVE",
    operational_state: str = "REQUEST_AMBIGUOUS",
    relay_path: str | None = None,
) -> dict[str, Any]:
    return {
        "path": f"/disposable/{request_id}.json",
        "sha256": f"hash-{request_id}",
        "request_id": request_id,
        "request_kind": kind,
        "created_utc": iso_utc(created),
        "deadline_utc": iso_utc(created + timedelta(seconds=30)),
        "declared_lane_id": lane_id,
        "relay_state": relay_state,
        "lifetime_state": lifetime_state,
        "operational_state": operational_state,
        "relay_path": relay_path,
        "resources": [],
        "resource_ambiguity": [],
        "producer_identities": {"session_id": f"session-{lane_id}"},
    }


def _snapshot(
    lanes: list[dict[str, Any]],
    *,
    requests: list[dict[str, Any]] | None = None,
    helpers: list[dict[str, Any]] | None = None,
    mcps: list[dict[str, Any]] | None = None,
    signals: list[dict[str, Any]] | None = None,
    conflicts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "process_snapshot_complete": True,
        "process_provider": "synthetic",
        "process_errors": [],
        "lanes": lanes,
        "requests": requests or [],
        "helpers": helpers or [],
        "mcps": mcps or [],
        "manager_signals": signals or [],
        "resource_conflicts": conflicts or [],
        "observation_errors": [],
    }


class ActiveManagerWatchSmokeTests(unittest.TestCase):
    fixture: SuiteFixture  # pyright: ignore[reportUninitializedInstanceVariable]

    def test_bound_expired_is_a_completed_review_stage_not_execution_authority(self) -> None:
        self.assertTrue(_request_is_stage_complete({"relay_state": "BOUND_EXPIRED"}))

    def setUp(self) -> None:
        self.fixture = SuiteFixture.create()

    def tearDown(self) -> None:
        self.fixture.close()

    def _short_config(self, **values: Any):
        raw = json.loads(self.fixture.config_path.read_text(encoding="utf-8"))
        raw.update(values)
        write_json(self.fixture.config_path, raw)
        self.fixture.config = load_config(
            self.fixture.config_path, harness_root=self.fixture.harness_root
        )
        return self.fixture.config

    def _store(self, config=None) -> SafeOutput:
        config = config or self.fixture.config
        store = SafeOutput(
            harness_root=config.harness_root,
            output_root=config.output_dir,
            forbidden_roots=config.forbidden_output_roots,
        )
        store.prepare()
        return store

    def _signal(
        self,
        signal_id: str,
        *,
        lane_id: str = "A00_test:Atlas:A00",
        created: Any = NOW,
        kind: str = "HELP",
    ) -> None:
        write_json(
            self.fixture.workspace() / "manager-signals" / f"{signal_id}.json",
            {
                "schema": "manager-signal/v1",
                "signal_id": signal_id,
                "kind": kind,
                "created_utc": iso_utc(created),
                "lane_id": lane_id,
                "task": "A00",
                "phase": "boundary",
                "summary": f"synthetic manager signal {signal_id}",
                "evidence_paths": [],
            },
        )

    def _managed_processes(
        self, watcher: ProcessInfo, owner: ProcessInfo
    ) -> ProcessSnapshot:
        base = self.fixture.process_snapshot()
        return ProcessSnapshot(
            True,
            tuple(base.processes) + (watcher, owner),
            (),
            "synthetic",
        )

    def test_AM_SMOKE_001_pending_urgent_does_not_consume_lower_warning(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=10,
            lane_no_progress_seconds=1,
            manager_heartbeat_timeout_seconds=20,
        )
        lane = _lane()
        request = _request(
            "lane-a",
            "urgent",
            "BOARD_SETUP",
            NOW,
            operational_state="RELAY_READY",
        )
        snapshot = _snapshot([lane], requests=[request])
        history, _ = transition_active_management(
            snapshot, None, config, observed_at=NOW
        )
        history, due = transition_active_management(
            snapshot, history, config, observed_at=NOW + timedelta(seconds=1)
        )
        self.assertIn("LANE_NO_PROGRESS", {item["type"] for item in due})
        base = conditions_from_snapshot(snapshot)
        conditions = {**base, **{item["identity"]: item for item in due}}
        urgent = select_actionable(
            conditions, snapshot, observed_at=NOW + timedelta(seconds=1), acknowledged_event_ids=set()
        )
        self.assertEqual("RELAY_READY", urgent["type"])
        history, after_ack = transition_active_management(
            snapshot,
            history,
            config,
            observed_at=NOW + timedelta(seconds=1, milliseconds=100),
        )
        available = {
            **base,
            **{item["identity"]: item for item in after_ack},
        }
        selected = select_actionable(
            available,
            snapshot,
            observed_at=NOW + timedelta(seconds=1, milliseconds=100),
            acknowledged_event_ids={urgent["event_id"]},
        )
        self.assertIsNotNone(selected)
        self.assertEqual("LANE_NO_PROGRESS", selected["type"])

    def test_AM_SMOKE_002_review_digest_uses_latest_activity_and_age(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=1,
            lane_no_progress_seconds=30,
            manager_heartbeat_timeout_seconds=10,
        )
        lane = _lane(
            checkpoint_path="/disposable/checkpoint.md",
            checkpoint_sha256="new-checkpoint",
            checkpoint_observed_utc=NOW + timedelta(seconds=1),
        )
        request = _request("lane-a", "old-request", "BOARD_SETUP", NOW)
        snapshot = _snapshot([lane], requests=[request])
        history, _ = transition_active_management(
            snapshot, None, config, observed_at=NOW
        )
        _, conditions = transition_active_management(
            snapshot, history, config, observed_at=NOW + timedelta(seconds=2)
        )
        review = next(item for item in conditions if item["type"] == "MANAGER_REVIEW_DUE")
        entry = review["data"]["lanes"][0]
        self.assertEqual("checkpoint", entry["latest_activity"]["kind"])
        self.assertEqual("/disposable/checkpoint.md", entry["latest_activity"]["identity"]["path"])
        self.assertIsNotNone(entry["latest_activity"]["age_seconds"])

    def test_AM_SMOKE_003_stage_repeat_detects_A_B_A(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=30,
            lane_no_progress_seconds=30,
            manager_heartbeat_timeout_seconds=60,
        )
        lane = _lane()
        first = _request("lane-a", "a-1", "BOARD_SETUP", NOW, relay_state="BOUND")
        second = _request(
            "lane-a", "b-1", "DIAGNOSTIC", NOW + timedelta(seconds=1), relay_state="UNBOUND"
        )
        third = _request(
            "lane-a", "a-2", "BOARD_SETUP", NOW + timedelta(seconds=2), relay_state="UNBOUND"
        )
        h1, _ = transition_active_management(
            _snapshot([lane], requests=[first]), None, config, observed_at=NOW
        )
        h2, _ = transition_active_management(
            _snapshot([lane], requests=[first, second]), h1, config, observed_at=NOW + timedelta(seconds=1)
        )
        _, conditions = transition_active_management(
            _snapshot([lane], requests=[first, second, third]), h2, config, observed_at=NOW + timedelta(seconds=2)
        )
        self.assertTrue(any(item["type"] == "LANE_STAGE_REPEAT" for item in conditions))

    def test_AM_SMOKE_004_stage_repeat_survives_inactive_relaunch(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=30,
            lane_no_progress_seconds=30,
            manager_heartbeat_timeout_seconds=60,
        )
        first_lane = _lane()
        first = _request("lane-a", "old", "BOARD_SETUP", NOW, relay_state="BOUND")
        h1, _ = transition_active_management(
            _snapshot([first_lane], requests=[first]), None, config, observed_at=NOW
        )
        exited = _lane(state="EXITED")
        h2, _ = transition_active_management(
            _snapshot([exited], requests=[first]), h1, config, observed_at=NOW + timedelta(seconds=1)
        )
        self.assertEqual({}, h2["lanes"])
        relaunched = _lane()
        repeated = _request(
            "lane-a", "new", "BOARD_SETUP", NOW + timedelta(seconds=2), relay_state="UNBOUND"
        )
        _, conditions = transition_active_management(
            _snapshot([relaunched], requests=[first, repeated]),
            h2,
            config,
            observed_at=NOW + timedelta(seconds=2),
        )
        self.assertTrue(any(item["type"] == "LANE_STAGE_REPEAT" for item in conditions))

    def test_AM_SMOKE_005_resource_ambiguity_is_actionable(self) -> None:
        snapshot = _snapshot(
            [
                _lane(
                    board_tokens=["STM-A"],
                    resource_ambiguity=["missing canonical probe ownership"],
                )
            ]
        )
        conditions = conditions_from_snapshot(snapshot)
        selected = select_actionable(
            conditions, snapshot, observed_at=NOW, acknowledged_event_ids=set()
        )
        self.assertIsNotNone(selected)
        self.assertEqual("RESOURCE_AMBIGUOUS", selected["type"])

    def test_AM_SMOKE_006_unknown_lane_state_is_actionable(self) -> None:
        snapshot = _snapshot([_lane(state="UNKNOWN")])
        conditions = conditions_from_snapshot(snapshot)
        selected = select_actionable(
            conditions, snapshot, observed_at=NOW, acknowledged_event_ids=set()
        )
        self.assertIsNotNone(selected)
        self.assertEqual("LANE_STATE_UNKNOWN", selected["type"])

    def test_historical_helper_exit_is_not_actionable_during_unrelated_live_lane(self) -> None:
        snapshot = _snapshot([_lane()])
        snapshot["helpers"] = [
            {
                "path": "historical-helper.json",
                "sha256": "historical",
                "operational_state": "HELPER_EXITED",
                "processes": [{"pid": 10, "state": "absent"}],
            }
        ]
        conditions = conditions_from_snapshot(snapshot)
        selected = select_actionable(
            conditions,
            snapshot,
            observed_at=NOW,
            acknowledged_event_ids=set(),
            newly_observed_event_ids=set(),
        )
        self.assertIsNone(selected)

    def test_new_helper_exit_transition_remains_actionable(self) -> None:
        snapshot = _snapshot([_lane()])
        snapshot["helpers"] = [
            {
                "path": "current-helper.json",
                "sha256": "current",
                "operational_state": "HELPER_EXITED",
                "processes": [{"pid": 10, "state": "absent"}],
            }
        ]
        conditions = conditions_from_snapshot(snapshot)
        helper = next(
            condition
            for condition in conditions.values()
            if condition["type"] == "HELPER_EXITED"
        )
        selected = select_actionable(
            conditions,
            snapshot,
            observed_at=NOW,
            acknowledged_event_ids=set(),
            newly_observed_event_ids={helper["event_id"]},
        )
        self.assertIsNotNone(selected)
        self.assertEqual("HELPER_EXITED", selected["type"])

    def test_AM_SMOKE_007_configuration_accepts_short_intervals_and_rejects_invalid(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=0.25,
            lane_no_progress_seconds=0.5,
            manager_heartbeat_timeout_seconds=1.0,
        )
        self.assertEqual(0.25, config.manager_review_interval_seconds)
        self.assertEqual(0.5, config.lane_no_progress_seconds)
        self.assertEqual(1.0, config.manager_heartbeat_timeout_seconds)
        raw = json.loads(self.fixture.config_path.read_text(encoding="utf-8"))
        raw["manager_heartbeat_timeout_seconds"] = raw["manager_review_interval_seconds"]
        write_json(self.fixture.config_path, raw)
        with self.assertRaises(ConfigError):
            load_config(self.fixture.config_path, harness_root=self.fixture.harness_root)

    def test_active_canary_config_reviews_at_three_minutes(self) -> None:
        path = Path(__file__).resolve().parents[1] / "canary-20260731-s1.json"
        config = load_config(path, harness_root=path.parent)
        self.assertEqual(180, config.manager_review_interval_seconds)

    def test_exited_checkpointed_lane_with_mcp_exited_is_not_no_progress_active_work(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=30,
            lane_no_progress_seconds=1,
            manager_heartbeat_timeout_seconds=60,
        )
        snapshot = _snapshot(
            [_lane("lane-a", state="CHECKPOINTED", checkpoint_path="/disposable/checkpoint.md")],
            mcps=[{"declared_lane_id": "lane-a", "operational_state": "MCP_EXITED"}],
        )
        history, first = transition_active_management(snapshot, None, config, observed_at=NOW)
        _, later = transition_active_management(
            snapshot, history, config, observed_at=NOW + timedelta(seconds=2)
        )
        self.assertFalse(any(item["type"] == "LANE_NO_PROGRESS" for item in first + later))

    def test_AM_SMOKE_008_review_timing_active_inactive_and_unique_digest(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=1,
            lane_no_progress_seconds=30,
            manager_heartbeat_timeout_seconds=10,
        )
        active = _snapshot([_lane("lane-a"), _lane("lane-b", doer="Boreal", task="A01")])
        history, first = transition_active_management(active, None, config, observed_at=NOW)
        self.assertFalse(any(item["type"] == "MANAGER_REVIEW_DUE" for item in first))
        history, due = transition_active_management(
            active, history, config, observed_at=NOW + timedelta(seconds=1)
        )
        review = next(item for item in due if item["type"] == "MANAGER_REVIEW_DUE")
        entries = review["data"]["lanes"]
        self.assertEqual(2, review["data"]["active_lane_count"])
        self.assertEqual({"lane-a", "lane-b"}, {entry["lane_id"] for entry in entries})
        self.assertEqual(2, len({entry["lane_id"] for entry in entries}))
        acknowledged = acknowledge_management_event(
            history, review, acknowledged_at=NOW + timedelta(seconds=1)
        )
        _, before_next = transition_active_management(
            active, acknowledged, config, observed_at=NOW + timedelta(seconds=1, milliseconds=500)
        )
        self.assertFalse(any(item["type"] == "MANAGER_REVIEW_DUE" for item in before_next))
        _, next_due = transition_active_management(
            active, acknowledged, config, observed_at=NOW + timedelta(seconds=2, milliseconds=100)
        )
        self.assertTrue(any(item["type"] == "MANAGER_REVIEW_DUE" for item in next_due))
        inactive = _snapshot([_lane("lane-a", state="EXITED"), _lane("lane-b", state="EXITED")])
        _, inactive_conditions = transition_active_management(
            inactive, acknowledged, config, observed_at=NOW + timedelta(seconds=10)
        )
        self.assertFalse(any(item["type"] == "MANAGER_REVIEW_DUE" for item in inactive_conditions))

    def test_AM_SMOKE_009_exact_review_ack_only_resets_review_baseline(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=1,
            lane_no_progress_seconds=30,
            manager_heartbeat_timeout_seconds=10,
        )
        snapshot = _snapshot([_lane()])
        history, _ = transition_active_management(snapshot, None, config, observed_at=NOW)
        history, due = transition_active_management(
            snapshot, history, config, observed_at=NOW + timedelta(seconds=1)
        )
        review = next(item for item in due if item["type"] == "MANAGER_REVIEW_DUE")
        store = self._store(config)
        store.save_active_management_history(history)
        store.save_notification_state(pending=review, acknowledged_event_ids=[])
        with self.assertRaises(ValueError):
            ack_command(config, event_id="not-the-review", stream=io.StringIO())
        self.assertEqual(review["event_id"], store.load_notification_state()["pending"]["event_id"])
        self.assertEqual(history["review_baseline_utc"], store.load_active_management_history()["review_baseline_utc"])
        ack_time = NOW + timedelta(seconds=1, milliseconds=200)
        self.assertEqual(
            EXIT_OK,
            ack_command(config, event_id=review["event_id"], clock=lambda: ack_time, stream=io.StringIO()),
        )
        self.assertIsNone(store.load_notification_state()["pending"])
        self.assertEqual(iso_utc(ack_time), store.load_active_management_history()["review_baseline_utc"])

    def test_AM_SMOKE_010_no_progress_warns_once_and_resets_on_real_progress(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=30,
            lane_no_progress_seconds=1,
            manager_heartbeat_timeout_seconds=60,
        )
        initial = _snapshot([_lane(phase="one")])
        history, first = transition_active_management(initial, None, config, observed_at=NOW)
        self.assertFalse(any(item["type"] == "LANE_NO_PROGRESS" for item in first))
        history, warning = transition_active_management(
            initial, history, config, observed_at=NOW + timedelta(seconds=1)
        )
        self.assertEqual(1, sum(item["type"] == "LANE_NO_PROGRESS" for item in warning))
        warning_event = next(item for item in warning if item["type"] == "LANE_NO_PROGRESS")
        history, eligible_again = transition_active_management(
            initial, history, config, observed_at=NOW + timedelta(seconds=2)
        )
        repeated_event = next(item for item in eligible_again if item["type"] == "LANE_NO_PROGRESS")
        self.assertEqual(warning_event["event_id"], repeated_event["event_id"])

        self.fixture.status()
        watcher = ProcessInfo(9001, 9000, "python", "managed watcher", NOW)
        owner = ProcessInfo(9000, 1, "python", "manager", NOW)
        processes = self._managed_processes(watcher, owner)
        output = io.StringIO()
        times = iter(
            [
                NOW,
                NOW,
                NOW + timedelta(seconds=1),
                NOW + timedelta(seconds=2),
                NOW + timedelta(seconds=2, milliseconds=100),
                NOW + timedelta(seconds=2, milliseconds=200),
            ]
        )
        sleep_calls: list[float] = []
        managed_warning_id: str | None = None

        def sleeper(delay: float) -> None:
            nonlocal managed_warning_id
            sleep_calls.append(delay)
            if len(sleep_calls) == 3:
                delivered = [json.loads(line) for line in output.getvalue().splitlines()]
                self.assertEqual(1, len(delivered))
                self.assertEqual("LANE_NO_PROGRESS", delivered[0]["type"])
                managed_warning_id = delivered[0]["event_id"]
                pending = self._store(config).load_notification_state()["pending"]
                self.assertEqual(managed_warning_id, pending["event_id"])
                ack_command(
                    config,
                    event_id=managed_warning_id,
                    clock=lambda: NOW + timedelta(seconds=2),
                    stream=io.StringIO(),
                )
            elif len(sleep_calls) == 4:
                self.assertEqual(1, len(output.getvalue().splitlines()))
                stop_command(config, clock=lambda: NOW, stream=io.StringIO())

        self.assertEqual(
            EXIT_OK,
            watch_managed(
                config,
                process_provider=lambda: processes,
                clock=lambda: next(times),
                sleeper=sleeper,
                stream=output,
                watcher=watcher,
                owner=owner,
            ),
        )
        self.assertEqual(4, len(sleep_calls))
        self.assertIsNone(self._store(config).load_notification_state()["pending"])

        acknowledged = acknowledge_management_event(
            history,
            warning_event,
            acknowledged_at=NOW + timedelta(seconds=2),
        )
        progressed = _snapshot([_lane(phase="two")])
        history, after_progress = transition_active_management(
            progressed, acknowledged, config, observed_at=NOW + timedelta(seconds=2)
        )
        self.assertFalse(any(item["type"] == "LANE_NO_PROGRESS" for item in after_progress))
        _, warning_again = transition_active_management(
            progressed, history, config, observed_at=NOW + timedelta(seconds=3)
        )
        self.assertEqual(1, sum(item["type"] == "LANE_NO_PROGRESS" for item in warning_again))

    def test_AM_SMOKE_011_stage_repeat_is_prevented_by_phase_or_checkpoint_progress(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=30,
            lane_no_progress_seconds=30,
            manager_heartbeat_timeout_seconds=60,
        )
        lane = _lane(phase="setup")
        first = _request("lane-a", "first", "BOARD_SETUP", NOW, relay_state="BOUND")
        repeated = _request("lane-a", "repeated", "BOARD_SETUP", NOW + timedelta(seconds=1))
        history, _ = transition_active_management(
            _snapshot([lane], requests=[first]), None, config, observed_at=NOW
        )
        _, phase_progress = transition_active_management(
            _snapshot([_lane(phase="diagnostic")], requests=[first, repeated]),
            history,
            config,
            observed_at=NOW + timedelta(seconds=1),
        )
        self.assertFalse(any(item["type"] == "LANE_STAGE_REPEAT" for item in phase_progress))
        _, checkpoint_progress = transition_active_management(
            _snapshot(
                [_lane(checkpoint_path="/disposable/checkpoint.md", checkpoint_sha256="new")],
                requests=[first, repeated],
            ),
            history,
            config,
            observed_at=NOW + timedelta(seconds=1),
        )
        self.assertFalse(any(item["type"] == "LANE_STAGE_REPEAT" for item in checkpoint_progress))

    def test_AM_SMOKE_012_same_managed_watcher_rearms_after_exact_ack(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=1,
            lane_no_progress_seconds=30,
            manager_heartbeat_timeout_seconds=10,
        )
        self.fixture.status()
        watcher = ProcessInfo(9001, 9000, "python", "managed watcher", NOW)
        owner = ProcessInfo(9000, 1, "python", "manager", NOW)
        processes = self._managed_processes(watcher, owner)
        output = io.StringIO()
        sleep_calls: list[float] = []

        def sleeper(delay: float) -> None:
            self.assertGreater(delay, 0)
            sleep_calls.append(delay)
            if len(sleep_calls) == 1:
                self._signal("first")
            elif len(sleep_calls) == 2:
                pending = self._store(config).load_notification_state()["pending"]
                self.assertIsNotNone(pending)
                ack_command(config, event_id=pending["event_id"], clock=lambda: NOW, stream=io.StringIO())
                self._signal("second", created=NOW + timedelta(seconds=1))
            elif len(sleep_calls) == 3:
                stop_command(config, clock=lambda: NOW, stream=io.StringIO())

        code = watch_managed(
            config,
            process_provider=lambda: processes,
            clock=lambda: NOW,
            sleeper=sleeper,
            stream=output,
            watcher=watcher,
            owner=owner,
        )
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(EXIT_OK, code)
        self.assertEqual(["MANAGER_SIGNAL", "MANAGER_SIGNAL"], [item["type"] for item in events])
        self.assertNotEqual(events[0]["event_id"], events[1]["event_id"])
        self.assertEqual(3, len(sleep_calls))
        self.assertEqual("stop-requested", self._store(config).load_managed_runtime()["exit_reason"])

    def test_AM_SMOKE_013_unacknowledged_notification_redelivers_after_restart(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=1,
            lane_no_progress_seconds=30,
            manager_heartbeat_timeout_seconds=10,
        )
        self.fixture.status()
        self._signal("durable")
        owner = ProcessInfo(9000, 1, "python", "manager", NOW)
        first_watcher = ProcessInfo(9001, 9000, "python", "managed watcher one", NOW)
        first_processes = self._managed_processes(first_watcher, owner)
        first_output = io.StringIO()

        def interrupt(_: float) -> None:
            raise _StopManagedWatch()

        with self.assertRaises(_StopManagedWatch):
            watch_managed(
                config,
                process_provider=lambda: first_processes,
                clock=lambda: NOW,
                sleeper=interrupt,
                stream=first_output,
                watcher=first_watcher,
                owner=owner,
            )
        pending = self._store(config).load_notification_state()["pending"]
        self.assertIsNotNone(pending)
        second_watcher = ProcessInfo(9002, 9000, "python", "managed watcher two", NOW)
        second_processes = self._managed_processes(second_watcher, owner)
        second_output = io.StringIO()

        def stop_after_redelivery(_: float) -> None:
            stop_command(config, clock=lambda: NOW, stream=io.StringIO())

        self.assertEqual(
            EXIT_OK,
            watch_managed(
                config,
                process_provider=lambda: second_processes,
                clock=lambda: NOW,
                sleeper=stop_after_redelivery,
                stream=second_output,
                watcher=second_watcher,
                owner=owner,
            ),
        )
        redelivered = json.loads(second_output.getvalue())
        self.assertEqual(pending["event_id"], redelivered["event_id"])
        self.assertEqual(pending["event_id"], json.loads(first_output.getvalue())["event_id"])

    def test_AM_SMOKE_014_heartbeat_expiry_records_reason_and_exits(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=0.2,
            lane_no_progress_seconds=30,
            manager_heartbeat_timeout_seconds=0.5,
        )
        watcher = ProcessInfo(9001, 9000, "python", "managed watcher", NOW)
        owner = ProcessInfo(9000, 1, "python", "manager", NOW)
        processes = self._managed_processes(watcher, owner)
        times = iter([NOW, NOW + timedelta(seconds=1)])
        self.assertEqual(
            EXIT_OK,
            watch_managed(
                config,
                process_provider=lambda: processes,
                clock=lambda: next(times),
                sleeper=lambda _: self.fail("expired lease must exit before sleeping"),
                stream=io.StringIO(),
                watcher=watcher,
                owner=owner,
            ),
        )
        self.assertEqual("manager-heartbeat-expired", self._store(config).load_managed_runtime()["exit_reason"])

    def test_AM_SMOKE_015_owner_identity_loss_exits_cleanly(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=1,
            manager_heartbeat_timeout_seconds=10,
        )
        watcher = ProcessInfo(9001, 9000, "python", "managed watcher", NOW)
        owner = ProcessInfo(9000, 1, "python", "manager", NOW)
        first = self._managed_processes(watcher, owner)
        second = ProcessSnapshot(True, tuple(self.fixture.process_snapshot().processes) + (watcher,), (), "synthetic")
        processes = iter([first, second])
        self.assertEqual(
            EXIT_OK,
            watch_managed(
                config,
                process_provider=lambda: next(processes),
                clock=lambda: NOW,
                sleeper=lambda _: self.fail("owner loss must exit before sleeping"),
                stream=io.StringIO(),
                watcher=watcher,
                owner=owner,
            ),
        )
        self.assertEqual("owner-identity-lost", self._store(config).load_managed_runtime()["exit_reason"])

    def test_AM_SMOKE_016_duplicate_live_and_stale_ownership_are_safe(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=1,
            manager_heartbeat_timeout_seconds=10,
        )
        watcher_one = ProcessInfo(9001, 9000, "python", "watcher one", NOW)
        watcher_two = ProcessInfo(9002, 9000, "python", "watcher two", NOW)
        owner = ProcessInfo(9000, 1, "python", "manager", NOW)
        base = self.fixture.process_snapshot()
        live = ProcessSnapshot(True, tuple(base.processes) + (watcher_one, watcher_two, owner), (), "synthetic")
        store = self._store(config)
        store.claim_managed_watcher(
            watcher=watcher_one,
            owner=owner,
            processes=live,
            heartbeat_timeout_seconds=10,
            started_at=NOW,
        )
        store.save_notification_state(pending={"event_id": "durable"}, acknowledged_event_ids=[])
        with self.assertRaises(ManagedWatcherClaimError):
            store.claim_managed_watcher(
                watcher=watcher_two,
                owner=owner,
                processes=live,
                heartbeat_timeout_seconds=10,
                started_at=NOW,
            )
        stale = ProcessSnapshot(True, tuple(base.processes) + (watcher_two, owner), (), "synthetic")
        store.claim_managed_watcher(
            watcher=watcher_two,
            owner=owner,
            processes=stale,
            heartbeat_timeout_seconds=10,
            started_at=NOW + timedelta(seconds=1),
        )
        self.assertEqual("durable", store.load_notification_state()["pending"]["event_id"])

    def test_AM_SMOKE_017_explicit_stop_exits_managed_watch_cleanly(self) -> None:
        config = self._short_config(
            manager_review_interval_seconds=1,
            manager_heartbeat_timeout_seconds=10,
        )
        watcher = ProcessInfo(9001, 9000, "python", "managed watcher", NOW)
        owner = ProcessInfo(9000, 1, "python", "manager", NOW)
        processes = self._managed_processes(watcher, owner)
        calls = 0

        def request_stop(delay: float) -> None:
            nonlocal calls
            self.assertGreater(delay, 0)
            calls += 1
            stop_command(config, clock=lambda: NOW, stream=io.StringIO())

        self.assertEqual(
            EXIT_OK,
            watch_managed(
                config,
                process_provider=lambda: processes,
                clock=lambda: NOW,
                sleeper=request_stop,
                stream=io.StringIO(),
                watcher=watcher,
                owner=owner,
            ),
        )
        self.assertEqual(1, calls)
        self.assertEqual("stop-requested", self._store(config).load_managed_runtime()["exit_reason"])

    def test_AM_SMOKE_018_output_writes_are_confined_to_harness_root(self) -> None:
        self.fixture.status()
        status_path = self.fixture.workspace() / "atlas_boundary_001_controller.status.json"
        before = status_path.read_bytes()
        output = io.StringIO()
        watch_once(
            self.fixture.config,
            no_write=False,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
            stream=output,
        )
        self.assertEqual(before, status_path.read_bytes())
        output_files = list(self.fixture.config.output_dir.iterdir())
        self.assertTrue(output_files)
        self.assertTrue(all(path.is_relative_to(self.fixture.harness_root) for path in output_files))
        self.assertFalse((self.fixture.suite_root / "snapshot.json").exists())
        raw = json.loads(self.fixture.config_path.read_text(encoding="utf-8"))
        raw["output_dir"] = str(self.fixture.suite_root)
        bad_path = self.fixture.harness_root / "bad-config.json"
        write_json(bad_path, raw)
        bad_config = load_config(bad_path, harness_root=self.fixture.harness_root)
        with self.assertRaises(PathSafetyError):
            self._store(bad_config)

    def test_AM_SMOKE_019_compatibility_modes_remain_usable(self) -> None:
        self.fixture.status()
        scan_output = io.StringIO()
        self.assertEqual(
            EXIT_OK,
            scan_command(
                self.fixture.config,
                process_provider=self.fixture.process_snapshot,
                clock=lambda: NOW,
                stream=scan_output,
            ),
        )
        self.assertEqual("orchestrator-watcher-snapshot/v1", json.loads(scan_output.getvalue())["schema"])
        once_output = io.StringIO()
        code, events = watch_once(
            self.fixture.config,
            no_write=True,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
            stream=once_output,
        )
        self.assertEqual(EXIT_OK, code)
        self.assertTrue(events)
        until_event_output = io.StringIO()
        self.assertEqual(
            EXIT_OK,
            watch_until_event(
                self.fixture.config,
                no_write=True,
                timeout_seconds=0.1,
                process_provider=self.fixture.process_snapshot,
                clock=lambda: NOW,
                stream=until_event_output,
            ),
        )
        self._signal("compatibility")
        actionable_output = io.StringIO()
        self.assertEqual(
            EXIT_OK,
            watch_until_actionable(
                self.fixture.config,
                timeout_seconds=0.1,
                process_provider=self.fixture.process_snapshot,
                clock=lambda: NOW,
                stream=actionable_output,
            ),
        )
        notification = json.loads(actionable_output.getvalue())
        self.assertEqual("MANAGER_SIGNAL", notification["type"])
        ack_command(self.fixture.config, event_id=notification["event_id"], stream=io.StringIO())

    def test_AM_SMOKE_020_polling_sleeps_between_observations(self) -> None:
        delays: list[float] = []
        monotonic_values = iter([0.0, 0.0, 0.01, 0.2])
        empty = ProcessSnapshot(True, (), (), "synthetic")
        code = watch_until_event(
            self.fixture.config,
            no_write=True,
            timeout_seconds=0.1,
            process_provider=lambda: empty,
            clock=lambda: NOW,
            sleeper=lambda delay: delays.append(delay),
            monotonic=lambda: next(monotonic_values),
            stream=io.StringIO(),
        )
        self.assertEqual(EXIT_TIMEOUT, code)
        self.assertTrue(delays)
        self.assertTrue(all(delay > 0 for delay in delays))

    def test_clean_i_explicit_lane_authority_prevents_reused_session_projection(self) -> None:
        lane = _lane("clean-i-current")
        lane["thread_id"] = "reused-persistent-session"
        historical = _request("clean-h-historical", "historical", "BOARD_SETUP", NOW)
        historical["producer_identities"] = {"session_id": "reused-persistent-session"}
        current = _request("clean-i-current", "current", "BOARD_SETUP", NOW + timedelta(seconds=1))
        current["producer_identities"] = {"session_id": "reused-persistent-session"}
        help_signal = {
            "schema": "manager-signal/v1", "signal_id": "clean-i-help",
            "kind": "HELP", "lane_id": "clean-i-current",
            "created_utc": iso_utc(NOW + timedelta(seconds=1)),
        }

        historical_projection = project_active_lanes(
            _snapshot([lane], requests=[historical]), observed_at=NOW
        )
        current_projection = project_active_lanes(
            _snapshot([lane], requests=[historical, current], signals=[help_signal]),
            observed_at=NOW,
        )

        self.assertIsNone(historical_projection[0]["request"])
        self.assertEqual("current", current_projection[0]["request"]["identity"])
        self.assertEqual(["clean-i-help"], [item["signal_id"] for item in current_projection[0]["manager_signals"]])

    def test_clean_i_unlabelled_legacy_session_record_still_projects(self) -> None:
        lane = _lane("clean-i-current")
        lane["thread_id"] = "legacy-session"
        legacy = _request("other-lane", "legacy", "BOARD_SETUP", NOW)
        legacy.pop("declared_lane_id")
        legacy["producer_identities"] = {"session_id": "legacy-session"}

        projection = project_active_lanes(
            _snapshot([lane], requests=[legacy]), observed_at=NOW
        )

        self.assertEqual("legacy", projection[0]["request"]["identity"])


if __name__ == "__main__":
    unittest.main()
