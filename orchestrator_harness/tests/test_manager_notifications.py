from __future__ import annotations
# pyright: reportImplicitRelativeImport=false

import io
from dataclasses import replace
import json
import unittest
from datetime import timedelta

from orchestrator_harness.cli import (
    EXIT_TIMEOUT,
    ack_command,
    observe,
    _managed_consumer_runtime,
    _consume_managed_notification,
    _deliver_blocking_wake,
    _pending_work_snapshot,
    watch_until_actionable,
)
from orchestrator_harness.events import conditions_from_snapshot, diff_conditions
from orchestrator_harness.notifications import _manager_signal_ineligibility_reason, select_actionable
from orchestrator_harness.tests.support import NOW, SuiteFixture, write_json


class ManagerNotificationTests(unittest.TestCase):
    fixture: SuiteFixture  # pyright: ignore[reportUninitializedInstanceVariable]

    def setUp(self) -> None:
        self.fixture = SuiteFixture.create()

    def tearDown(self) -> None:
        self.fixture.close()

    def _signal(
        self, signal_id: str, *, summary: str = "Manager input needed", **metadata: object
    ) -> None:
        write_json(
            self.fixture.workspace() / "manager-signals" / f"{signal_id}.json",
            {
                "schema": "manager-signal/v1",
                "signal_id": signal_id,
                "kind": "HELP",
                "created_utc": "2026-07-30T12:00:00Z",
                "lane_id": "A00_test:Atlas:A00",
                "task": "A00",
                "phase": "synthetic",
                "summary": summary,
                "evidence_paths": ["logs/claim.txt"],
                **metadata,
            },
        )

    @staticmethod
    def _manager_signal_snapshot(
        signal_ids: list[str],
        *,
        requests: list[dict[str, object]] | None = None,
        lanes: list[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        lane_id = "A00_test:Atlas:A00"
        return {
            "process_snapshot_complete": True,
            "lanes": lanes or [],
            "helpers": [],
            "mcps": [],
            "requests": requests or [],
            "manager_signals": [
                {"signal_id": signal_id, "lane_id": lane_id}
                for signal_id in signal_ids
            ],
            "resource_conflicts": [],
            "observation_errors": [],
        }

    @staticmethod
    def _manager_request(
        *,
        lifetime_state: str,
        expiry_bucket: str | None = None,
        deadline_utc: str | None = None,
    ) -> dict[str, object]:
        request: dict[str, object] = {
            "path": "manager-requests/request.json",
            "operational_state": "REQUEST_AMBIGUOUS",
            "declared_lane_id": "A00_test:Atlas:A00",
            "lifetime_state": lifetime_state,
        }
        if expiry_bucket is not None:
            request["expiry_bucket"] = expiry_bucket
        if deadline_utc is not None:
            request["deadline_utc"] = deadline_utc
        return request

    def _wait(self, output: io.StringIO) -> int:
        ticks = iter([0.0, 0.0, 1.0, 1.0])
        return watch_until_actionable(
            self.fixture.config,
            timeout_seconds=0.5,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
            sleeper=lambda _: None,
            monotonic=lambda: next(ticks, 1.0),
            stream=output,
        )

    def test_attention_enabled_wake_attempts_are_correlated_and_fail_honestly(self) -> None:
        config = replace(self.fixture.config, attention_logging_enabled=True, attention_epoch_id="A00_test")
        self.fixture.status()
        baseline = io.StringIO()
        self.assertEqual(EXIT_TIMEOUT, watch_until_actionable(config, timeout_seconds=0, process_provider=self.fixture.process_snapshot, clock=lambda: NOW, sleeper=lambda _: None, monotonic=lambda: 0.0, stream=baseline, manager_session_id="session", manager_invocation_id="invocation"))
        self._signal("signal-1")
        signal_path = self.fixture.workspace() / "manager-signals" / "signal-1.json"
        signal = json.loads(signal_path.read_text(encoding="utf-8")); signal["attention_epoch_id"] = "A00_test"; write_json(signal_path, signal)
        output = io.StringIO()
        ticks = iter([0.0, 0.0, 1.0])
        self.assertEqual(0, watch_until_actionable(config, timeout_seconds=0.5, process_provider=self.fixture.process_snapshot, clock=lambda: NOW, sleeper=lambda _: None, monotonic=lambda: next(ticks, 1.0), stream=output, manager_session_id="session", manager_invocation_id="invocation"))
        delivered = json.loads(output.getvalue())
        self.assertEqual("blocking_harness_wait_stdout", delivered["wake_transport"])
        self.assertEqual("orchestrator_harness.watch_until_actionable", delivered["wake_component"])
        first_wake_id = delivered["wake_id"]
        redelivery = io.StringIO()
        self.assertEqual(0, watch_until_actionable(config, timeout_seconds=0.5, process_provider=self.fixture.process_snapshot, clock=lambda: NOW, sleeper=lambda _: None, monotonic=lambda: 0.0, stream=redelivery, manager_session_id="session", manager_invocation_id="invocation"))
        self.assertNotEqual(first_wake_id, json.loads(redelivery.getvalue())["wake_id"])
        records = [json.loads(line) for line in (config.output_dir / "attention-events.jsonl").read_text(encoding="utf-8").splitlines()]
        self.assertEqual(["MANAGER_WAKE_ATTEMPTED", "MANAGER_WAKE_DELIVERED", "MANAGER_WAKE_ATTEMPTED", "MANAGER_WAKE_DELIVERED"], [record["kind"] for record in records if record["kind"].startswith("MANAGER_WAKE_")])
        with self.assertRaises(ValueError):
            watch_until_actionable(config, timeout_seconds=0, manager_session_id=None, manager_invocation_id="invocation")
        class BrokenStream:
            def write(self, _: str) -> None: raise BrokenPipeError("broken")
            def flush(self) -> None: raise AssertionError("flush must not run after write failure")
        with self.assertRaises(BrokenPipeError):
            watch_until_actionable(config, timeout_seconds=0.5, process_provider=self.fixture.process_snapshot, clock=lambda: NOW, sleeper=lambda _: None, monotonic=lambda: 0.0, stream=BrokenStream(), manager_session_id="session", manager_invocation_id="invocation")
        records = [json.loads(line) for line in (config.output_dir / "attention-events.jsonl").read_text(encoding="utf-8").splitlines()]
        failed = [record for record in records if record["kind"] == "MANAGER_WAKE_FAILED"]
        self.assertEqual(1, len(failed)); self.assertFalse(failed[0]["delivery_succeeded"]); self.assertEqual("BrokenPipeError", failed[0]["failure_kind"])
        self.assertFalse(any(record["kind"] == "MANAGER_WAKE_DELIVERED" and record["wake_id"] == failed[0]["wake_id"] for record in records))

    def test_wake_write_error_is_preserved_when_failure_recording_also_fails(self) -> None:
        class Store:
            attention_epoch_id = None
            def append_attention(self, *, kind, **_):
                if kind == "MANAGER_WAKE_FAILED": raise OSError("persistence failed")
        class Broken:
            def write(self, _: str): raise BrokenPipeError("stdout broken")
            def flush(self): raise AssertionError("unreachable")
        with self.assertRaisesRegex(BrokenPipeError, "stdout broken"):
            _deliver_blocking_wake(Store(), event={"event_id":"event","type":"x"}, stream=Broken(), timestamp=NOW, manager_session_id="s", manager_invocation_id="i")

    def test_managed_consumer_delivers_later_pending_without_state_mutation(self) -> None:
        pending={"event_id":"exact","type":"HELP"}; calls=iter([{"pending":None},{"pending":pending}])
        class Store:
            attention_epoch_id=None
            def __init__(self): self.records=[]
            def load_notification_state(self): return next(calls)
            def append_attention(self, **kwargs): self.records.append(kwargs)
        store=Store(); before=b"snapshot"; snapshot=before; cursor=b"cursor"; notification=b"notification"
        from unittest.mock import patch
        ticks=iter([0,0,1])
        with patch("orchestrator_harness.cli._managed_consumer_runtime"):
            out=io.StringIO(); self.assertEqual(0,_consume_managed_notification(store,process_provider=lambda:None,poll_interval_seconds=.01,timeout=1,clock=lambda:NOW,sleeper=lambda _:None,monotonic=lambda:next(ticks),stream=out,manager_session_id="s",manager_invocation_id="i",attention_enabled=True))
        delivered=json.loads(out.getvalue()); self.assertEqual("exact",delivered["event_id"]); self.assertEqual("blocking_harness_wait_stdout",delivered["wake_transport"]); self.assertTrue(delivered["wake_id"])
        self.assertEqual(["MANAGER_WAKE_ATTEMPTED","MANAGER_WAKE_DELIVERED"],[r["kind"] for r in store.records]); self.assertEqual(before,snapshot); self.assertEqual(b"cursor",cursor); self.assertEqual(b"notification",notification)

    def test_managed_runtime_invalid_states_fail_closed(self) -> None:
        from orchestrator_harness.models import ProcessSnapshot
        class Store:
            def __init__(self,runtime,status): self.runtime=runtime; self.status=status
            def load_managed_runtime(self): return self.runtime
            def _process_identity_status(self,*_): return self.status
        base={"stop_requested":False,"exit_reason":None,"lease_expires_utc":"2999-01-01T00:00:00+00:00"}
        from unittest.mock import patch
        for label,runtime,status,complete in (("stopped",{**base,"stop_requested":True},"live",True),("expired",{**base,"lease_expires_utc":"2000-01-01T00:00:00+00:00"},"live",True),("owner",base,"absent",True),("watcher",base,"reused",True),("coverage",base,"live",False)):
            with self.subTest(label=label), patch("orchestrator_harness.cli.process_snapshot",return_value=ProcessSnapshot(complete,())):
                with self.assertRaises(ValueError): _managed_consumer_runtime(Store(runtime,status),NOW,lambda: ProcessSnapshot(complete,()))

    def test_managed_consumer_runtime_disappearance_before_delivery_fails_closed(self) -> None:
        class Store:
            attention_epoch_id=None
            def __init__(self): self.records=[]
            def load_notification_state(self): return {"pending":{"event_id":"exact","type":"HELP"}}
            def append_attention(self, **kwargs): self.records.append(kwargs)
        store=Store()
        from unittest.mock import patch
        with patch("orchestrator_harness.cli._managed_consumer_runtime",side_effect=[True,ValueError("managed watcher runtime disappeared")]):
            with self.assertRaisesRegex(ValueError,"disappeared"):
                _consume_managed_notification(store,process_provider=lambda:None,poll_interval_seconds=.01,timeout=1,clock=lambda:NOW,sleeper=lambda _:None,monotonic=lambda:0,stream=io.StringIO(),manager_session_id="s",manager_invocation_id="i",attention_enabled=True)
        self.assertEqual([],store.records)

    def test_managed_consumer_quiet_timeout_has_no_wake(self) -> None:
        class Store:
            def load_notification_state(self): return {"pending":None}
        from unittest.mock import patch
        with patch("orchestrator_harness.cli._managed_consumer_runtime"):
            out=io.StringIO(); self.assertEqual(EXIT_TIMEOUT,_consume_managed_notification(Store(),process_provider=lambda: None,poll_interval_seconds=.01,timeout=0,clock=lambda:NOW,sleeper=lambda _:None,monotonic=lambda:0,stream=out,manager_session_id=None,manager_invocation_id=None,attention_enabled=False))
        self.assertNotIn("wake_id",out.getvalue())

    def test_valid_signal_is_reconciled_without_opening_evidence(self) -> None:
        self._signal("signal-1")
        snapshot, _ = observe(
            self.fixture.config,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
        )
        signal = snapshot["manager_signals"][0]
        self.assertEqual("signal-1", signal["signal_id"])
        self.assertEqual(["logs/claim.txt"], signal["evidence_paths"])

    def test_delivery_metadata_is_validated_and_propagated(self) -> None:
        self._signal(
            "delivery-help",
            delivery_deadline_utc="2026-07-30T12:01:00Z",
            agent_blocked=True,
            attention_epoch_id="A00_test",
        )
        snapshot, _ = observe(
            self.fixture.config,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
        )
        signal = snapshot["manager_signals"][0]
        self.assertEqual("2026-07-30T12:01:00Z", signal["delivery_deadline_utc"])
        self.assertIs(True, signal["agent_blocked"])
        self.assertEqual("A00_test", signal["attention_epoch_id"])

    def test_invalid_optional_delivery_metadata_is_an_observation_error(self) -> None:
        cases = (
            {"delivery_deadline_utc": "not-utc"},
            {"delivery_deadline_utc": "2026-07-30T11:59:59Z"},
            {"agent_blocked": "true"},
            {"attention_epoch_id": "  "},
        )
        for index, metadata in enumerate(cases):
            with self.subTest(metadata=metadata):
                self.fixture.close()
                self.fixture = SuiteFixture.create()
                self._signal(f"invalid-{index}", **metadata)
                snapshot, _ = observe(
                    self.fixture.config,
                    process_provider=self.fixture.process_snapshot,
                    clock=lambda: NOW,
                )
                self.assertEqual([], snapshot["manager_signals"])
                self.assertEqual(
                    ["MANAGER_SIGNAL_READ_ERROR"],
                    [error["code"] for error in snapshot["observation_errors"]],
                )

    def test_blocked_help_delivery_deadline_precedes_stale_status(self) -> None:
        lane_id = "A00_test:Atlas:A00"
        snapshot = {
            "process_snapshot_complete": True,
            "lanes": [{"lane_id": lane_id, "process_state": "RUNNING_CODEX", "operational_state": "RUNNING_CODEX"}],
            "helpers": [], "mcps": [], "requests": [],
            "manager_signals": [{
                "signal_id": "blocked-help", "kind": "HELP", "lane_id": lane_id,
                "created_utc": "2026-07-30T12:00:00Z",
                "delivery_deadline_utc": "2026-07-30T12:01:00Z",
                "agent_blocked": True,
            }],
            "resource_conflicts": [], "observation_errors": [],
        }
        conditions = conditions_from_snapshot(snapshot)
        conditions["lane:stale"] = {
            "identity": "lane:stale", "event_id": "stale", "type": "STALE_STATUS",
            "severity": "warning", "data": {"created_utc": "2026-07-30T12:00:00Z"},
        }
        selected = select_actionable(
            conditions, snapshot, observed_at=NOW, acknowledged_event_ids=set()
        )
        self.assertIsNotNone(selected)
        self.assertEqual("MANAGER_SIGNAL", selected["type"])
        self.assertEqual("blocked-help", selected["data"]["signal_id"])

    def test_delivery_deadline_is_response_by_time_in_pending_snapshot(self) -> None:
        event = {
            "event_id": "blocked-help", "identity": "manager-signal:blocked-help",
            "type": "MANAGER_SIGNAL", "data": {
                "kind": "HELP", "signal_id": "blocked-help",
                "lane_id": "A00_test:Atlas:A00", "created_utc": "2026-07-30T12:00:00Z",
                "delivery_deadline_utc": "2026-07-30T12:01:00Z", "agent_blocked": True,
            },
        }
        snapshot = {"lanes": [{"lane_id": "A00_test:Atlas:A00", "process_state": "RUNNING_CODEX"}]}
        pending = _pending_work_snapshot(
            selected=event, deferred=[], snapshot=snapshot, observed=NOW,
            selection_reason="SELECT_ACTIONABLE",
        )
        self.assertTrue(pending["complete"])
        self.assertEqual(
            "2026-07-30T12:01:00Z",
            pending["events"][0]["response_deadline_utc"],
        )

    def test_non_blocking_help_with_delivery_deadline_does_not_outrank_stale_status(self) -> None:
        lane_id = "A00_test:Atlas:A00"
        snapshot = {
            "process_snapshot_complete": True,
            "lanes": [{"lane_id": lane_id, "process_state": "RUNNING_CODEX", "operational_state": "RUNNING_CODEX"}],
            "helpers": [], "mcps": [], "requests": [],
            "manager_signals": [{
                "signal_id": "ordinary-help", "kind": "HELP", "lane_id": lane_id,
                "created_utc": "2026-07-30T12:00:00Z",
                "delivery_deadline_utc": "2026-07-30T12:01:00Z",
                "agent_blocked": False,
            }],
            "resource_conflicts": [], "observation_errors": [],
        }
        conditions = conditions_from_snapshot(snapshot)
        conditions["lane:stale"] = {
            "identity": "lane:stale", "event_id": "stale", "type": "STALE_STATUS",
            "severity": "warning", "data": {"created_utc": "2026-07-30T12:00:00Z"},
        }
        selected = select_actionable(
            conditions, snapshot, observed_at=NOW, acknowledged_event_ids=set()
        )
        self.assertIsNotNone(selected)
        self.assertEqual("STALE_STATUS", selected["type"])

    def test_malformed_and_conflicting_signals_are_observation_errors(self) -> None:
        self._signal("same", summary="one")
        self._signal("same-copy", summary="two")
        copy = self.fixture.workspace() / "manager-signals" / "same-copy.json"
        raw = json.loads(copy.read_text(encoding="utf-8"))
        raw["signal_id"] = "same"
        write_json(copy, raw)
        (self.fixture.workspace() / "manager-signals" / "broken.json").write_text("{", encoding="utf-8")
        self._signal("bad-time")
        invalid = self.fixture.workspace() / "manager-signals" / "bad-time.json"
        raw = json.loads(invalid.read_text(encoding="utf-8"))
        raw["created_utc"] = "2026-07-30T12:00:00"
        write_json(invalid, raw)
        snapshot, _ = observe(
            self.fixture.config,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
        )
        self.assertEqual([], snapshot["manager_signals"])
        self.assertEqual(
            {"MANAGER_SIGNAL_READ_ERROR"},
            {error["code"] for error in snapshot["observation_errors"]},
        )

    def test_identical_duplicate_signals_collapse_to_one_record(self) -> None:
        self._signal("same")
        source = self.fixture.workspace() / "manager-signals" / "same.json"
        duplicate = self.fixture.workspace() / "manager-signals" / "same-copy.json"
        duplicate.write_bytes(source.read_bytes())
        snapshot, _ = observe(
            self.fixture.config,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
        )
        self.assertEqual(1, len(snapshot["manager_signals"]))
        self.assertEqual([], snapshot["observation_errors"])

    def test_sole_stale_status_is_actionable(self) -> None:
        self.fixture.status()
        output = io.StringIO()
        base = self.fixture.process_snapshot()
        ticks = iter([0.0, 0.0, 1.0, 1.0])
        code = watch_until_actionable(
            self.fixture.config,
            timeout_seconds=0.5,
            process_provider=lambda: type(base)(True, (base.processes[0],), (), "fake"),
            clock=lambda: NOW,
            sleeper=lambda _: None,
            monotonic=lambda: next(ticks, 1.0),
            stream=output,
        )
        self.assertEqual(0, code)
        self.assertEqual("STALE_STATUS", json.loads(output.getvalue())["type"])

    def test_incomplete_inventory_without_lanes_is_actionable(self) -> None:
        output = io.StringIO()
        base = self.fixture.process_snapshot()
        ticks = iter([0.0, 0.0, 1.0, 1.0])
        code = watch_until_actionable(
            self.fixture.config,
            timeout_seconds=0.5,
            process_provider=lambda: type(base)(False, (), ("incomplete",), "fake"),
            clock=lambda: NOW,
            sleeper=lambda _: None,
            monotonic=lambda: next(ticks, 1.0),
            stream=output,
        )
        self.assertEqual(0, code)
        self.assertEqual(
            "PROCESS_INVENTORY_INCOMPLETE", json.loads(output.getvalue())["type"]
        )

    def test_routine_churn_times_out_then_signal_ack_and_next_signal_flow(self) -> None:
        self.fixture.status()
        first = io.StringIO()
        self.assertEqual(EXIT_TIMEOUT, self._wait(first))
        self.assertEqual("WATCH_TIMEOUT", json.loads(first.getvalue())["type"])

        self._signal("signal-1")
        second = io.StringIO()
        self.assertEqual(0, self._wait(second))
        notification = json.loads(second.getvalue())
        self.assertEqual("MANAGER_SIGNAL", notification["type"])

        redelivery = io.StringIO()
        self.assertEqual(0, self._wait(redelivery))
        self.assertEqual(notification["event_id"], json.loads(redelivery.getvalue())["event_id"])

        with self.assertRaises(ValueError):
            ack_command(self.fixture.config, event_id="wrong", stream=io.StringIO())
        still_pending = io.StringIO()
        self.assertEqual(0, self._wait(still_pending))
        self.assertEqual(notification["event_id"], json.loads(still_pending.getvalue())["event_id"])

        self.assertEqual(0, ack_command(self.fixture.config, event_id=notification["event_id"], stream=io.StringIO()))
        self.assertEqual(0, ack_command(self.fixture.config, event_id=notification["event_id"], stream=io.StringIO()))
        self._signal("signal-2")
        third = io.StringIO()
        self.assertEqual(0, self._wait(third))
        self.assertEqual("signal-2", json.loads(third.getvalue())["data"]["signal_id"])
        self.assertTrue((self.fixture.config.output_dir / "pending-notification.json").exists())

    def test_bootstrap_ignores_historical_clean_controller_exit(self) -> None:
        self.fixture.status(state="exited")
        base = self.fixture.process_snapshot()
        output = io.StringIO()
        ticks = iter([0.0, 0.0, 1.0, 1.0])
        code = watch_until_actionable(
            self.fixture.config,
            timeout_seconds=0.5,
            process_provider=lambda: type(base)(True, (), (), "fake"),
            clock=lambda: NOW,
            sleeper=lambda _: None,
            monotonic=lambda: next(ticks, 1.0),
            stream=output,
        )
        self.assertEqual(EXIT_TIMEOUT, code)

    def test_historical_manager_signal_is_retained_but_not_actionable(self) -> None:
        self._signal("historical-help")
        snapshot, _ = observe(self.fixture.config, process_provider=self.fixture.process_snapshot, clock=lambda: NOW)
        self.assertEqual("historical-help", snapshot["manager_signals"][0]["signal_id"])
        self.assertIsNone(select_actionable(conditions_from_snapshot(snapshot), snapshot, observed_at=NOW, acknowledged_event_ids=set()))

    def test_non_live_signal_emits_passive_exact_ineligibility_evidence(self) -> None:
        config = replace(self.fixture.config, attention_logging_enabled=True, attention_epoch_id="A00_test")
        self._signal("historical-help", attention_epoch_id="A00_test")
        self.assertEqual(
            EXIT_TIMEOUT,
            watch_until_actionable(
                config, timeout_seconds=0, process_provider=self.fixture.process_snapshot,
                clock=lambda: NOW, sleeper=lambda _: None, monotonic=lambda: 0.0,
                stream=io.StringIO(), manager_session_id="session", manager_invocation_id="invocation",
            ),
        )
        records = [json.loads(line) for line in (config.output_dir / "attention-events.jsonl").read_text(encoding="utf-8").splitlines()]
        observed = [record for record in records if record["kind"] == "HARNESS_SIGNAL_OBSERVED"]
        ineligible = [record for record in records if record["kind"] == "HARNESS_EVENT_INELIGIBLE"]
        self.assertEqual(1, len(observed))
        self.assertEqual(1, len(ineligible))
        self.assertEqual(observed[0]["event_id"], ineligible[0]["event_id"])
        self.assertEqual(observed[0]["harness_event_id"], ineligible[0]["harness_event_id"])
        self.assertEqual("LANE_NOT_LIVE", ineligible[0]["ineligibility_reason"])

    def test_live_lane_manager_signal_remains_actionable(self) -> None:
        self.fixture.status(declared_lane_id="A00_test:Atlas:A00")
        self._signal("live-help")
        snapshot, _ = observe(self.fixture.config, process_provider=self.fixture.process_snapshot, clock=lambda: NOW)
        selected = select_actionable(conditions_from_snapshot(snapshot), snapshot, observed_at=NOW, acknowledged_event_ids=set())
        self.assertIsNotNone(selected)
        self.assertEqual("MANAGER_SIGNAL", selected["type"])
        self.assertEqual("live-help", selected["data"]["signal_id"])

    def test_stopped_lane_mcp_unknown_does_not_reactivate_historical_signal(self) -> None:
        self._signal("stopped-unknown")
        snapshot = {
            "process_snapshot_complete": True,
            "lanes": [{"lane_id": "A00_test:Atlas:A00", "process_state": "EXITED", "operational_state": "EXITED"}],
            "helpers": [],
            "mcps": [{"path": "historical-mcp.json", "declared_lane_id": "A00_test:Atlas:A00", "operational_state": "MCP_STATE_UNKNOWN", "processes": []}],
            "requests": [],
            "manager_signals": [{"signal_id": "stopped-unknown", "lane_id": "A00_test:Atlas:A00"}],
        }
        self.assertIsNone(select_actionable(conditions_from_snapshot(snapshot), snapshot, observed_at=NOW, acknowledged_event_ids=set()))

    def test_expired_unknown_request_signal_is_retained_but_not_actionable(self) -> None:
        request = self._manager_request(
            lifetime_state="UNKNOWN", expiry_bucket="EXPIRED",
            deadline_utc="2026-07-30T11:59:00Z",
        )
        snapshot = self._manager_signal_snapshot(["expired-help"], requests=[request])
        conditions = conditions_from_snapshot(snapshot)

        self.assertEqual(["expired-help"], [item["signal_id"] for item in snapshot["manager_signals"]])
        self.assertIn("manager-signal:expired-help", conditions)
        self.assertEqual(
            ["MANAGER_SIGNAL"],
            [item["type"] for item in diff_conditions(None, conditions, observed_at=NOW) if item["type"] == "MANAGER_SIGNAL"],
        )
        self.assertIsNone(select_actionable(conditions, snapshot, observed_at=NOW, acknowledged_event_ids=set()))

    def test_past_deadline_without_expiry_bucket_does_not_reactivate_unknown_request_signal(self) -> None:
        request = self._manager_request(
            lifetime_state="UNKNOWN", deadline_utc="2026-07-30T11:59:00Z"
        )
        snapshot = self._manager_signal_snapshot(["past-deadline-help"], requests=[request])

        self.assertIsNone(select_actionable(conditions_from_snapshot(snapshot), snapshot, observed_at=NOW, acknowledged_event_ids=set()))

    def test_acknowledging_one_expired_signal_does_not_expose_another_before_live_review(self) -> None:
        request = self._manager_request(lifetime_state="UNKNOWN", expiry_bucket="EXPIRED")
        snapshot = self._manager_signal_snapshot(["expired-one", "expired-two"], requests=[request])
        conditions = conditions_from_snapshot(snapshot)
        first_expired = conditions["manager-signal:expired-one"]
        conditions["manager-review:live"] = {
            "identity": "manager-review:live",
            "type": "MANAGER_REVIEW_DUE",
            "severity": "info",
            "data": {},
            "event_id": "live-review-event",
        }

        selected = select_actionable(
            conditions, snapshot, observed_at=NOW,
            acknowledged_event_ids={first_expired["event_id"]},
        )
        self.assertIsNotNone(selected)
        self.assertEqual("MANAGER_REVIEW_DUE", selected["type"])

    def test_unexpired_unknown_request_signal_remains_actionable(self) -> None:
        request = self._manager_request(
            lifetime_state="UNKNOWN", expiry_bucket="OK",
            deadline_utc=(NOW + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
        )
        snapshot = self._manager_signal_snapshot(["unknown-but-current"], requests=[request])

        selected = select_actionable(conditions_from_snapshot(snapshot), snapshot, observed_at=NOW, acknowledged_event_ids=set())
        self.assertIsNotNone(selected)
        self.assertEqual("MANAGER_SIGNAL", selected["type"])

    def test_unknown_request_event_uses_reconciled_actionability_boundary(self) -> None:
        def snapshot(actionable: bool) -> dict[str, object]:
            return {
                "process_snapshot_complete": True, "lanes": [], "helpers": [], "mcps": [],
                "requests": [{
                    "path": "manager-requests/unknown.json",
                    "operational_state": "REQUEST_AMBIGUOUS",
                    "lifetime_state": "UNKNOWN", "manager_actionable": actionable,
                    "expiry_bucket": "OK" if actionable else "EXPIRED",
                }],
                "manager_signals": [], "resource_conflicts": [], "observation_errors": [],
            }

        current = snapshot(True)
        selected = select_actionable(
            conditions_from_snapshot(current), current, observed_at=NOW,
            acknowledged_event_ids=set(),
        )
        self.assertIsNotNone(selected)
        self.assertEqual("REQUEST_AMBIGUOUS", selected["type"])
        expired = snapshot(False)
        self.assertIsNone(select_actionable(
            conditions_from_snapshot(expired), expired, observed_at=NOW,
            acknowledged_event_ids=set(),
        ))

    def test_live_request_signal_remains_actionable_even_when_expired(self) -> None:
        request = self._manager_request(lifetime_state="LIVE", expiry_bucket="EXPIRED")
        snapshot = self._manager_signal_snapshot(["live-request"], requests=[request])
        conditions = conditions_from_snapshot(snapshot)

        selected = select_actionable(
            conditions, snapshot, observed_at=NOW,
            acknowledged_event_ids={conditions["request:manager-requests/request.json:state"]["event_id"]},
        )
        self.assertIsNotNone(selected)
        self.assertEqual("MANAGER_SIGNAL", selected["type"])

    def test_exact_live_lane_signal_remains_actionable_without_a_request(self) -> None:
        snapshot = self._manager_signal_snapshot(
            ["live-lane"],
            lanes=[{
                "lane_id": "A00_test:Atlas:A00",
                "process_state": "RUNNING_CODEX",
                "operational_state": "RUNNING_CODEX",
            }],
        )

        selected = select_actionable(conditions_from_snapshot(snapshot), snapshot, observed_at=NOW, acknowledged_event_ids=set())
        self.assertIsNotNone(selected)
        self.assertEqual("MANAGER_SIGNAL", selected["type"])

    def test_inactive_resource_ambiguity_is_retained_but_does_not_wake_bootstrap(self) -> None:
        snapshot = {
            "process_snapshot_complete": True,
            "lanes": [{
                "lane_id": "clean-h:Atlas:A22", "process_state": "EXITED",
                "operational_state": "EXITED", "resource_ambiguity": ["historical ambiguity"],
            }],
            "requests": [], "helpers": [], "mcps": [], "manager_signals": [],
            "resource_conflicts": [], "observation_errors": [],
        }
        conditions = conditions_from_snapshot(snapshot)

        self.assertIn("lane:clean-h:Atlas:A22:resource-ambiguity", conditions)
        self.assertIn(
            "RESOURCE_AMBIGUOUS",
            [item["type"] for item in diff_conditions(None, conditions, observed_at=NOW)],
        )
        self.assertIsNone(select_actionable(conditions, snapshot, observed_at=NOW, acknowledged_event_ids=set()))

    def test_active_resource_ambiguity_remains_actionable(self) -> None:
        snapshot = {
            "process_snapshot_complete": True,
            "lanes": [{
                "lane_id": "clean-i:Cygnus:D31", "process_state": "RUNNING_CODEX",
                "operational_state": "RUNNING_CODEX", "resource_ambiguity": ["current ambiguity"],
            }],
            "requests": [], "helpers": [], "mcps": [], "manager_signals": [],
            "resource_conflicts": [], "observation_errors": [],
        }

        selected = select_actionable(conditions_from_snapshot(snapshot), snapshot, observed_at=NOW, acknowledged_event_ids=set())
        self.assertIsNotNone(selected)
        self.assertEqual("RESOURCE_AMBIGUOUS", selected["type"])

    def test_inactive_resource_ambiguity_with_current_request_or_live_mcp_is_actionable(self) -> None:
        lane = {
            "lane_id": "clean-i:Cygnus:A24", "process_state": "EXITED",
            "operational_state": "EXITED", "resource_ambiguity": ["current evidence ambiguity"],
        }
        for evidence in (
            {"requests": [{
                "path": "requests/current.json", "operational_state": "REQUEST_STALE",
                "declared_lane_id": lane["lane_id"], "lifetime_state": "LIVE",
            }]},
            {"mcps": [{
                "path": "mcp/current.json", "declared_lane_id": lane["lane_id"],
                "operational_state": "MCP_RUNNING",
            }]},
        ):
            with self.subTest(evidence=evidence):
                snapshot = {
                    "process_snapshot_complete": True, "lanes": [lane],
                    "requests": evidence.get("requests", []), "helpers": [],
                    "mcps": evidence.get("mcps", []), "manager_signals": [],
                    "resource_conflicts": [], "observation_errors": [],
                }
                selected = select_actionable(
                    conditions_from_snapshot(snapshot), snapshot,
                    observed_at=NOW, acknowledged_event_ids=set(),
                )
                self.assertIsNotNone(selected)
                self.assertEqual("RESOURCE_AMBIGUOUS", selected["type"])

    def test_ineligibility_reasons_are_truthful_without_changing_selection(self) -> None:
        lane_id = "clean-k:Atlas:A24"
        live_snapshot = self._manager_signal_snapshot(
            [], lanes=[{"lane_id": lane_id, "process_state": "RUNNING_CODEX", "operational_state": "RUNNING_CODEX"}],
        )
        self.assertEqual(
            "ALREADY_ANSWERED",
            _manager_signal_ineligibility_reason(
                {"lane_id": lane_id, "correlated_request_answered": True}, live_snapshot, NOW,
            ),
        )
        self.assertEqual(
            "INVALID_LANE_ID",
            _manager_signal_ineligibility_reason({"lane_id": None}, live_snapshot, NOW),
        )
        self.assertEqual(
            "LANE_NOT_LIVE",
            _manager_signal_ineligibility_reason({"lane_id": lane_id}, self._manager_signal_snapshot([]), NOW),
        )

    def test_answered_expired_relay_and_correlated_help_remain_observed_but_do_not_wake(self) -> None:
        lane_id = "clean-k:Atlas:A24"
        snapshot = {
            "process_snapshot_complete": True,
            "lanes": [{"lane_id": lane_id, "process_state": "RUNNING_CODEX", "operational_state": "RUNNING_CODEX"}],
            "helpers": [], "mcps": [],
            "requests": [{
                "path": "manager-requests/a24.json", "request_id": "a24-request",
                "declared_lane_id": lane_id, "lifetime_state": "LIVE",
                "relay_state": "BOUND_EXPIRED", "operational_state": "RELAYED_EXPIRED",
                "manager_actionable": False, "expiry_bucket": "EXPIRED",
            }],
            "manager_signals": [{
                "signal_id": "a24-help", "lane_id": lane_id,
                "correlated_request_id": "a24-request",
                "correlated_request_answered": True,
            }],
            "resource_conflicts": [], "observation_errors": [],
        }
        self.assertIsNone(select_actionable(
            conditions_from_snapshot(snapshot), snapshot, observed_at=NOW,
            acknowledged_event_ids=set(),
        ))


if __name__ == "__main__":
    unittest.main()

