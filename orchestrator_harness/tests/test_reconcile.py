from __future__ import annotations
# pyright: reportImplicitRelativeImport=false

import hashlib
import json
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from orchestrator_harness.discovery import discover_suite
from orchestrator_harness.events import conditions_from_snapshot
from orchestrator_harness.models import ProcessInfo, ProcessSnapshot
from orchestrator_harness.notifications import select_actionable
from orchestrator_harness.reconcile import _lane_is_active_or_unknown, reconcile
from orchestrator_harness.tests.support import NOW, SuiteFixture, write_json


class ReconcileTests(unittest.TestCase):
    fixture: SuiteFixture  # pyright: ignore[reportUninitializedInstanceVariable]

    def setUp(self) -> None:
        self.fixture = SuiteFixture.create()

    def tearDown(self) -> None:
        self.fixture.close()

    def observe(self, snapshot):
        runs = discover_suite(self.fixture.config)
        return reconcile(runs, snapshot, self.fixture.config, now=NOW)

    def test_live_controller_parent_identity(self) -> None:
        self.fixture.status()
        self.fixture.jsonl()
        observed = self.observe(self.fixture.process_snapshot())
        lane = observed["lanes"][0]
        self.assertEqual("RUNNING_CODEX", lane["operational_state"])
        self.assertEqual("match", lane["parent_state"])

    def test_explicit_declared_lane_id_is_public_identity(self) -> None:
        self.fixture.status(declared_lane_id="canary:atlas:A22:B14")
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual("canary:atlas:A22:B14", observed["lanes"][0]["lane_id"])

    def test_missing_child_makes_running_stale(self) -> None:
        self.fixture.status()
        observed = self.observe(self.fixture.process_snapshot(missing_codex=True))
        self.assertEqual("STALE_STATUS", observed["lanes"][0]["operational_state"])

    def test_missing_child_makes_new_running_codex_state_stale(self) -> None:
        self.fixture.status(state="RUNNING_CODEX")
        observed = self.observe(self.fixture.process_snapshot(missing_codex=True))
        self.assertEqual("STALE_STATUS", observed["lanes"][0]["operational_state"])

    def test_incomplete_process_inventory_is_unknown(self) -> None:
        self.fixture.status()
        observed = self.observe(self.fixture.process_snapshot(complete=False))
        self.assertEqual(
            "PROCESS_STATE_UNKNOWN", observed["lanes"][0]["operational_state"]
        )

    def test_partial_linux_inventory_uses_observed_identity_but_not_absence(
        self,
    ) -> None:
        self.fixture.status()
        base = self.fixture.process_snapshot()
        partial_live = ProcessSnapshot(
            False,
            base.processes,
            ("one proc entry disappeared",),
            "linux-proc",
        )
        observed = self.observe(partial_live)
        self.assertEqual("RUNNING_CODEX", observed["lanes"][0]["operational_state"])

        missing_child = ProcessSnapshot(
            False,
            (ProcessInfo(101, 1, "python", "controller", NOW),),
            ("one proc entry disappeared",),
            "linux-proc",
        )
        unknown = self.observe(missing_child)
        self.assertEqual(
            "PROCESS_STATE_UNKNOWN", unknown["lanes"][0]["operational_state"]
        )

    def test_missing_creation_time_is_unknown(self) -> None:
        self.fixture.status()
        observed = self.observe(self.fixture.process_snapshot(missing_created=True))
        self.assertEqual(
            "PROCESS_STATE_UNKNOWN", observed["lanes"][0]["operational_state"]
        )

    def test_legacy_shared_start_time_is_not_exact_process_identity(self) -> None:
        status = self.fixture.status()
        value = json.loads(status.read_text(encoding="utf-8"))
        value.pop("controller_started_utc")
        value.pop("codex_started_utc")
        write_json(status, value)
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual(
            "PROCESS_STATE_UNKNOWN", observed["lanes"][0]["operational_state"]
        )

    def test_parent_pid_reuse_is_stale(self) -> None:
        self.fixture.status()
        observed = self.observe(self.fixture.process_snapshot(codex_parent=999))
        self.assertEqual("STALE_STATUS", observed["lanes"][0]["operational_state"])

    def test_pid_creation_mismatch_is_stale(self) -> None:
        self.fixture.status()
        observed = self.observe(
            self.fixture.process_snapshot(created=NOW + timedelta(hours=1))
        )
        self.assertEqual("STALE_STATUS", observed["lanes"][0]["operational_state"])

    def test_terminal_jsonl_contradicts_running(self) -> None:
        self.fixture.status()
        self.fixture.jsonl(terminal="turn.completed")
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual("STALE_STATUS", observed["lanes"][0]["operational_state"])
        self.assertEqual("turn.completed", observed["lanes"][0]["terminal_event"])

    def test_exited_with_checkpoint(self) -> None:
        self.fixture.status(state="exited")
        checkpoint = self.fixture.workspace() / "PARALLEL_CHECKPOINT.md"
        checkpoint.write_text("# checkpoint\n", encoding="utf-8")
        snapshot = self.fixture.process_snapshot()
        observed = self.observe(type(snapshot)(True, (), (), "fake"))
        self.assertEqual("CHECKPOINTED", observed["lanes"][0]["operational_state"])
        self.assertEqual("EXITED", observed["lanes"][0]["process_state"])

    def test_result_is_terminal(self) -> None:
        self.fixture.status(state="exited")
        write_json(self.fixture.workspace() / "RESULT.json", {"status": "PASS"})
        snapshot = self.fixture.process_snapshot()
        observed = self.observe(type(snapshot)(True, (), (), "fake"))
        self.assertEqual("TERMINAL_RESULT", observed["lanes"][0]["operational_state"])

    def test_declared_exited_with_live_tree_is_stale(self) -> None:
        self.fixture.status(state="exited")
        observed = self.observe(self.fixture.process_snapshot())
        lane = observed["lanes"][0]
        self.assertEqual("STALE_STATUS", lane["process_state"])
        self.assertFalse(lane["resource_release_possible"])

    def test_declared_exited_with_incomplete_inventory_is_unknown(self) -> None:
        self.fixture.status(state="exited")
        observed = self.observe(self.fixture.process_snapshot(complete=False))
        self.assertEqual("PROCESS_STATE_UNKNOWN", observed["lanes"][0]["process_state"])

    def test_duplicate_live_controllers(self) -> None:
        self.fixture.status(label="atlas_001", controller_pid=101, codex_pid=102)
        self.fixture.status(
            label="atlas_002",
            controller_pid=201,
            codex_pid=202,
            started=NOW + timedelta(seconds=1),
        )
        snapshot = self.fixture.process_snapshot()
        processes = list(snapshot.processes) + [
            type(snapshot.processes[0])(201, 1, "python", "controller", NOW),
            type(snapshot.processes[0])(202, 201, "codex", "codex", NOW),
        ]
        observed = self.observe(type(snapshot)(True, tuple(processes), (), "fake"))
        self.assertEqual(2, observed["lanes"][0]["duplicate_live_attempts"])

    def test_named_current_attempt_collapses_unnamed_same_thread_without_conflict(
        self,
    ) -> None:
        old = self.fixture.status(
            label="boreal_old",
            doer="Boreal",
            task="D31",
            state="exited",
            board_tokens=["stm-a"],
            controller_pid=101,
            codex_pid=102,
        )
        old_value = json.loads(old.read_text(encoding="utf-8"))
        old_value.pop("doer")
        write_json(old, old_value)
        self.fixture.status(
            label="boreal_current",
            doer="Boreal",
            task="D31",
            board_tokens=["stm-a"],
            controller_pid=201,
            codex_pid=202,
            started=NOW + timedelta(seconds=1),
        )
        base = self.fixture.process_snapshot()
        info = type(base.processes[0])
        observed = self.observe(
            type(base)(
                True,
                (
                    info(201, 1, "python", "controller", NOW + timedelta(seconds=1)),
                    info(202, 201, "codex", "codex", NOW + timedelta(seconds=1)),
                ),
                (),
                "fake",
            )
        )
        self.assertEqual(1, len(observed["lanes"]))
        self.assertEqual("A00_test:Boreal:D31", observed["lanes"][0]["lane_id"])
        self.assertEqual([], observed["resource_conflicts"])

    def test_same_named_doer_and_task_with_distinct_threads_remain_separate(
        self,
    ) -> None:
        first = self.fixture.status(
            label="boreal_one",
            doer="Boreal",
            task="D31",
            board_tokens=["stm-a"],
            controller_pid=101,
            codex_pid=102,
        )
        second = self.fixture.status(
            label="boreal_two",
            doer="Boreal",
            task="D31",
            board_tokens=["stm-a"],
            controller_pid=201,
            codex_pid=202,
            started=NOW + timedelta(seconds=1),
        )
        for path, thread_id in ((first, "thread-one"), (second, "thread-two")):
            value = json.loads(path.read_text(encoding="utf-8"))
            value["thread_id"] = thread_id
            write_json(path, value)
        base = self.fixture.process_snapshot()
        info = type(base.processes[0])
        observed = self.observe(
            type(base)(
                True,
                (
                    info(101, 1, "python", "controller", NOW),
                    info(102, 101, "codex", "codex", NOW),
                    info(201, 1, "python", "controller", NOW + timedelta(seconds=1)),
                    info(202, 201, "codex", "codex", NOW + timedelta(seconds=1)),
                ),
                (),
                "fake",
            )
        )
        self.assertEqual(2, len(observed["lanes"]))
        self.assertEqual(2, len({lane["lane_id"] for lane in observed["lanes"]}))
        self.assertTrue(
            all(":Boreal:D31" in lane["lane_id"] for lane in observed["lanes"])
        )
        self.assertEqual("board:stm-a", observed["resource_conflicts"][0]["resource"])

    def test_current_named_attempt_wins_same_thread_historical_terminal_state(
        self,
    ) -> None:
        old = self.fixture.status(
            label="boreal_old",
            doer="Boreal",
            task="D31",
            state="exited",
            controller_pid=101,
            codex_pid=102,
        )
        old_value = json.loads(old.read_text(encoding="utf-8"))
        old_value.pop("doer")
        write_json(old, old_value)
        self.fixture.status(
            label="boreal_current",
            doer="Boreal",
            task="D31",
            controller_pid=201,
            codex_pid=202,
            started=NOW + timedelta(seconds=1),
        )
        base = self.fixture.process_snapshot()
        info = type(base.processes[0])
        observed = self.observe(
            type(base)(
                True,
                (
                    info(201, 1, "python", "controller", NOW + timedelta(seconds=1)),
                    info(202, 201, "codex", "codex", NOW + timedelta(seconds=1)),
                ),
                (),
                "fake",
            )
        )
        self.assertEqual(1, len(observed["lanes"]))
        self.assertEqual("Boreal", observed["lanes"][0]["doer"])
        self.assertEqual("RUNNING_CODEX", observed["lanes"][0]["operational_state"])

    def _request(
        self, *, pid: int = 101, include_start: bool = True
    ) -> tuple[Path, dict[str, Any]]:
        workspace = self.fixture.workspace()
        process_value: dict[str, Any] = {"pid": pid}
        lifetime_value: dict[str, Any] = {
            "run_id": "run-1",
            "process": process_value,
        }
        value: dict[str, Any] = {
            "schema": "synthetic/v1",
            "request_id": "req-1",
            "created_utc": NOW.isoformat().replace("+00:00", "Z"),
            "run": {"session_id": "thread-atlas"},
            "live_lifetime": lifetime_value,
            "board": {
                "board_id": "stm_a",
                "probe_uid": "probe-1",
                "vcom": "COM1",
            },
            "roots": {"project": ".agent-workspace/runtime/project"},
            "relay_path": ".agent-workspace/permission-requests/req.relay.json",
        }
        if include_start:
            process_value["started_utc"] = NOW.isoformat().replace("+00:00", "Z")
        path = workspace / "permission-requests" / "req.json"
        write_json(path, value)
        return path, value

    def _manager_request(self, *, sidecar: bool = False) -> tuple[Path, dict[str, Any]]:
        workspace = self.fixture.workspace()
        call = {"tool": "board_setup-plan", "arguments": {"board_id": "stm_b"}}
        value: dict[str, Any] = {
            "schema": "suite-manager-request/v1",
            "request_id": "manager-req-1",
            "created_utc": NOW.isoformat().replace("+00:00", "Z"),
            "deadline_utc": (NOW + timedelta(minutes=10))
            .isoformat()
            .replace("+00:00", "Z"),
            "declared_lane_id": "clean-f:Boreal:D31",
            "session_id": "thread-atlas",
            "live_lifetime": {
                "run_id": "manager-run-1",
                "process": {
                    "pid": 101,
                    "creation_time_utc": NOW.isoformat().replace("+00:00", "Z"),
                },
            },
            "board": {
                "board_token": "STM-B",
                "board_id": "stm_b",
                "probe_uid": "probe-b",
                "vcom": "COM17",
            },
            "roots": {"project": ".agent-workspace/runtime/project"},
            "server_snapshot": {"head": "snapshot"},
            "tool": call["tool"],
            "arguments": call["arguments"],
            "tool_argument_sha256": "a" * 64,
            "relay": {"path": ".agent-workspace/manager-relays/manager-req-1.json"},
        }
        path = workspace / "manager-requests" / "manager-req-1.json"
        write_json(path, value)
        if sidecar:
            path.with_name(path.name + ".sha256").write_text(
                hashlib.sha256(path.read_bytes()).hexdigest() + "  " + path.name + "\n",
                encoding="ascii",
            )
        return path, value

    def _manager_relay(
        self, request_path: Path, request: dict[str, Any], *, exact: bool = True
    ) -> Path:
        relay = self.fixture.workspace() / "manager-relays" / "manager-req-1.json"
        write_json(
            relay,
            {
                "schema": "suite-manager-relay/v1",
                "decision": "approved",
                "request_id": request["request_id"],
                "request_sha256": hashlib.sha256(request_path.read_bytes()).hexdigest(),
                "declared_lane_id": request["declared_lane_id"],
                "tool_argument_sha256": request["tool_argument_sha256"]
                if exact
                else "b" * 64,
                "server_snapshot": request["server_snapshot"],
                "exact_approved_call": {
                    "tool": request["tool"],
                    "arguments": request["arguments"],
                },
                "expires_utc": (NOW + timedelta(minutes=5))
                .isoformat()
                .replace("+00:00", "Z"),
            },
        )
        return relay

    def _mcp_observation(self, value: dict[str, Any], snapshot):
        write_json(self.fixture.workspace() / "mcp-lifetime.json", value)
        return self.observe(snapshot)["mcps"][0]

    def _snapshot_with_process(self, pid: int, created: datetime):
        base = self.fixture.process_snapshot()
        info = type(base.processes[0])
        return type(base)(
            True,
            tuple(base.processes) + (info(pid, 1, "python", "synthetic mcp", created),),
            (),
            "fake",
        )

    def test_clean_f_manager_request_is_discovered_lane_correlated_and_removes_false_ambiguity(
        self,
    ) -> None:
        self.fixture.status(
            board_tokens=["STM-B"], declared_lane_id="clean-f:Boreal:D31"
        )
        path, _ = self._manager_request(sidecar=True)
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual([str(path)], [item["path"] for item in observed["requests"]])
        self.assertEqual(
            "clean-f:Boreal:D31", observed["requests"][0]["declared_lane_id"]
        )
        self.assertEqual("RELAY_READY", observed["requests"][0]["operational_state"])
        self.assertNotIn(
            "active hardware lane has no current request identity for probe/serial/root audit",
            observed["lanes"][0]["resource_ambiguity"],
        )

    def test_manager_relay_without_sidecar_is_observed_and_exact_binding_relays(
        self,
    ) -> None:
        self.fixture.status(declared_lane_id="clean-f:Boreal:D31")
        path, request = self._manager_request()
        relay = self._manager_relay(path, request)
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual(
            [str(relay)],
            [str(item.path) for item in discover_suite(self.fixture.config)[0].relays],
        )
        self.assertEqual("RELAYED", observed["requests"][0]["operational_state"])
        self.assertFalse(
            any(
                item["code"] == "RELAY_READ_ERROR"
                for item in observed["observation_errors"]
            )
        )

    def test_manager_relay_without_sidecar_still_requires_exact_content_binding(
        self,
    ) -> None:
        self.fixture.status(declared_lane_id="clean-f:Boreal:D31")
        path, request = self._manager_request()
        self._manager_relay(path, request, exact=False)
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual("RELAY_UNBOUND", observed["requests"][0]["operational_state"])

    def test_expired_exact_manager_relay_is_retained_but_not_actionable(self) -> None:
        self.fixture.status(declared_lane_id="clean-f:Boreal:D31")
        path, request = self._manager_request()
        relay = self._manager_relay(path, request)
        value = json.loads(relay.read_text(encoding="utf-8"))
        value["expires_utc"] = (
            (NOW - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
        )
        write_json(relay, value)
        observed = self.observe(self.fixture.process_snapshot())
        request_observation = observed["requests"][0]
        self.assertEqual("BOUND_EXPIRED", request_observation["relay_state"])
        self.assertEqual("RELAYED_EXPIRED", request_observation["operational_state"])
        self.assertFalse(request_observation["manager_actionable"])

    def test_suite_lifetime_binding_proves_only_exact_live_server_pid_and_creation(
        self,
    ) -> None:
        self.fixture.status(declared_lane_id="clean-f:Boreal:D31")
        path, request = self._manager_request()
        request.pop("live_lifetime")
        request["mcp_server"] = {"name": "clean-k-mcp"}
        request["lifetime_binding"] = {
            "server_name": "clean-k-mcp",
            "pid": 101,
            "creation_utc": NOW.isoformat().replace("+00:00", "Z"),
        }
        write_json(path, request)
        self._manager_relay(path, request)
        current = self.observe(self.fixture.process_snapshot())["requests"][0]
        self.assertEqual("PROVEN", current["mcp_lifetime_state"])
        self.assertEqual("BOUND", current["relay_state"])
        self.assertEqual("RELAYED", current["operational_state"])
        self.assertFalse(current["manager_actionable"])

        request["lifetime_binding"]["server_name"] = "wrong-server"
        write_json(path, request)
        mismatched = self.observe(self.fixture.process_snapshot())["requests"][0]
        self.assertEqual("UNPROVEN", mismatched["mcp_lifetime_state"])
        self.assertEqual("REQUEST_AMBIGUOUS", mismatched["operational_state"])
        self.assertTrue(mismatched["manager_actionable"])

    def test_invalid_manager_request_sidecar_is_an_observation_error(self) -> None:
        self.fixture.status(declared_lane_id="clean-f:Boreal:D31")
        path, _ = self._manager_request()
        path.with_name(path.name + ".sha256").write_text(
            "0" * 64 + "  " + path.name + "\n", encoding="ascii"
        )
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual([], observed["requests"])
        self.assertTrue(
            any(
                item["code"] == "REQUEST_READ_ERROR"
                for item in observed["observation_errors"]
            )
        )

    def test_invalid_present_manager_relay_sidecar_is_an_observation_error(
        self,
    ) -> None:
        self.fixture.status(declared_lane_id="clean-f:Boreal:D31")
        path, request = self._manager_request()
        relay = self._manager_relay(path, request)
        relay.with_name(relay.name + ".sha256").write_text(
            "0" * 64 + "  " + relay.name + "\n", encoding="ascii"
        )
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual("RELAY_READY", observed["requests"][0]["operational_state"])
        self.assertTrue(
            any(
                item["code"] == "RELAY_READ_ERROR"
                for item in observed["observation_errors"]
            )
        )

    def test_legacy_permission_request_discovery_is_unchanged(self) -> None:
        self.fixture.status()
        path, _ = self._request()
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual(str(path), observed["requests"][0]["path"])
        self.assertEqual("RELAY_READY", observed["requests"][0]["operational_state"])

    def test_live_request_is_relay_ready(self) -> None:
        self.fixture.status()
        path, _ = self._request()
        observed = self.observe(self.fixture.process_snapshot())
        request = observed["requests"][0]
        self.assertEqual("RELAY_READY", request["operational_state"])
        self.assertEqual(
            hashlib.sha256(path.read_bytes()).hexdigest(), request["sha256"]
        )
        self.assertEqual([], observed["mcps"])

    def test_request_facts_are_orthogonal_and_summary_is_derived(self) -> None:
        self.fixture.status()
        path, value = self._request()
        value["live_lifetime"]["metadata"] = {
            "pid": 999,
            "created_utc": NOW.isoformat().replace("+00:00", "Z"),
        }
        write_json(path, value)
        request = self.observe(self.fixture.process_snapshot())["requests"][0]
        self.assertEqual("LIVE", request["request_facts"]["lifetime_state"])
        self.assertEqual("ABSENT", request["request_facts"]["relay_state"])
        self.assertEqual(request["operational_state"], request["operator_summary"])
        self.assertEqual([101], [item["pid"] for item in request["processes"]])

    def test_unscoped_request_is_not_lane_owned(self) -> None:
        self.fixture.status()
        path, value = self._request()
        value["run"].pop("session_id")
        write_json(path, value)
        observed = self.observe(self.fixture.process_snapshot())
        lane = observed["lanes"][0]
        self.assertEqual("RUNNING_CODEX", lane["operational_state"])
        self.assertNotIn("producer-lifetime:run-1", lane["resources"])
        self.assertIsNone(
            observed["requests"][0]["producer_identities"].get("session_id")
        )

    def test_unscoped_live_request_does_not_block_lane_release(self) -> None:
        self.fixture.status(state="CODEX_EXITED")
        path, value = self._request(pid=301)
        value["run"].pop("session_id")
        write_json(path, value)
        base = self.fixture.process_snapshot()
        info = type(base.processes[0])
        snapshot = type(base)(
            True,
            (info(301, 1, "helper", "helper", NOW),),
            (),
            "fake",
        )
        observed = self.observe(snapshot)
        lane = observed["lanes"][0]
        self.assertEqual("EXITED", lane["operational_state"])
        self.assertTrue(lane["resource_release_possible"])

    def test_exited_declared_board_token_is_not_an_operational_lease(self) -> None:
        self.fixture.status(state="CODEX_EXITED", board_tokens=["STM-A"])
        base = self.fixture.process_snapshot()
        observed = self.observe(type(base)(True, (), (), "fake"))
        lane = observed["lanes"][0]
        self.assertEqual(["STM-A"], lane["board_tokens"])
        self.assertNotIn("board:stm-a", lane["resources"])
        self.assertNotIn(
            "active hardware lane has no current request identity for probe/serial/root audit",
            lane["resource_ambiguity"],
        )
        self.assertTrue(lane["resource_release_possible"])

    def test_explicit_lane_id_owns_request_without_session_id(self) -> None:
        self.fixture.status()
        path, value = self._request()
        value["run"].pop("session_id")
        value["lane_id"] = "A00_test:Atlas:A00"
        write_json(path, value)
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual("WAITING_RELAY", observed["lanes"][0]["operational_state"])

    def test_explicit_lane_id_wins_over_reused_session_identity(self) -> None:
        current = self.fixture.status(
            label="atlas_current",
            doer="Atlas",
            task="A00",
            controller_pid=101,
            codex_pid=102,
        )
        historical = self.fixture.status(
            label="boreal_historical",
            doer="Boreal",
            task="D36",
            state="exited",
            controller_pid=201,
            codex_pid=202,
        )
        for path in (current, historical):
            value = json.loads(path.read_text(encoding="utf-8"))
            value["thread_id"] = "shared-persistent-session"
            write_json(path, value)
        request_path, request = self._request()
        request["run"]["session_id"] = "shared-persistent-session"
        request["lane_id"] = "A00_test:Atlas:A00"
        write_json(request_path, request)

        observed = self.observe(self.fixture.process_snapshot())
        lanes = {lane["lane_id"]: lane for lane in observed["lanes"]}
        current_lane = lanes["A00_test:Atlas:A00"]
        historical_lane = lanes["A00_test:Boreal:D36"]

        self.assertEqual("WAITING_RELAY", current_lane["operational_state"])
        self.assertIn("producer-lifetime:run-1", current_lane["resources"])
        self.assertEqual("EXITED", historical_lane["operational_state"])
        self.assertNotIn("producer-lifetime:run-1", historical_lane["resources"])
        self.assertTrue(historical_lane["resource_release_possible"])
        self.assertEqual([], observed["resource_conflicts"])

    def test_historical_checkpoint_prose_does_not_invent_provider_wait(self) -> None:
        self.fixture.status(state="exited")
        checkpoint = self.fixture.workspace() / "PARALLEL_CHECKPOINT.md"
        checkpoint.write_text(
            "# Current checkpoint\n\n"
            "Current state: complete and waiting for manager review.\n\n"
            "## Historical section\n\nwaiting_for_provider\n",
            encoding="utf-8",
        )
        snapshot = self.fixture.process_snapshot()
        observed = self.observe(type(snapshot)(True, (), (), "fake"))

        lane = observed["lanes"][0]
        self.assertEqual("CHECKPOINTED", lane["operational_state"])
        self.assertFalse(lane["provider_wait"])

    def test_two_owned_relay_requests_transition_without_watcher_writes(self) -> None:
        self.fixture.status(run="A00_one", doer="Atlas", task="A00")
        self.fixture.status(
            run="A01_two",
            label="boreal_boundary_001",
            doer="Boreal",
            task="A01",
            controller_pid=201,
            codex_pid=202,
        )
        first_workspace = self.fixture.workspace("A00_one")
        first_path = first_workspace / "permission-requests" / "first.json"
        first = {
            "request_id": "first",
            "run": {"session_id": "thread-atlas"},
            "live_lifetime": {
                "run_id": "first-run",
                "process": {
                    "pid": 101,
                    "started_utc": NOW.isoformat().replace("+00:00", "Z"),
                },
            },
            "board": {"board_id": "stm-a", "probe_uid": "probe-a"},
            "relay_path": ".agent-workspace/permission-requests/first.relay.json",
        }
        second_workspace = self.fixture.workspace("A01_two")
        second_path = second_workspace / "permission-requests" / "second.json"
        second = {
            "request_id": "second",
            "run": {"session_id": "thread-boreal"},
            "live_lifetime": {
                "run_id": "second-run",
                "process": {
                    "pid": 201,
                    "started_utc": NOW.isoformat().replace("+00:00", "Z"),
                },
            },
            "board": {"board_id": "stm-b", "probe_uid": "probe-b"},
            "relay_path": ".agent-workspace/permission-requests/second.relay.json",
        }
        write_json(first_path, first)
        write_json(second_path, second)
        base = self.fixture.process_snapshot()
        info = type(base.processes[0])
        snapshot = type(base)(
            True,
            tuple(base.processes)
            + (
                info(201, 1, "python", "controller", NOW),
                info(202, 201, "codex", "codex", NOW),
            ),
            (),
            "fake",
        )
        ready = self.observe(snapshot)
        self.assertEqual(
            {"RELAY_READY"}, {item["operational_state"] for item in ready["requests"]}
        )
        request_bytes = {path: path.read_bytes() for path in (first_path, second_path)}
        for path, request, session in (
            (first_path, first, "thread-atlas"),
            (second_path, second, "thread-boreal"),
        ):
            relay = path.with_name(path.stem + ".relay.json")
            write_json(
                relay,
                {
                    "decision": "approved",
                    "request_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "run_id": request["live_lifetime"]["run_id"],
                    "session_id": session,
                },
            )
        relayed = self.observe(snapshot)
        self.assertEqual(
            {"RELAYED"}, {item["operational_state"] for item in relayed["requests"]}
        )
        self.assertEqual(
            request_bytes, {path: path.read_bytes() for path in request_bytes}
        )

    def test_missing_request_creation_identity_is_ambiguous(self) -> None:
        self.fixture.status()
        self._request(include_start=False)
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual(
            "REQUEST_AMBIGUOUS", observed["requests"][0]["operational_state"]
        )

    def test_missing_creation_unknown_request_is_actionable_only_before_deadline(
        self,
    ) -> None:
        self.fixture.status()
        path, value = self._request(include_start=False)
        value["deadline_utc"] = (
            (NOW + timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        )
        write_json(path, value)
        current = self.observe(self.fixture.process_snapshot())
        request = current["requests"][0]
        self.assertEqual("UNKNOWN", request["lifetime_state"])
        self.assertTrue(request["manager_actionable"])
        selected = select_actionable(
            conditions_from_snapshot(current),
            current,
            observed_at=NOW,
            acknowledged_event_ids=set(),
        )
        self.assertIsNotNone(selected)
        self.assertIn(selected["type"], {"REQUEST_AMBIGUOUS", "REQUEST_EXPIRY_WARNING"})

        value["deadline_utc"] = (
            (NOW - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        )
        write_json(path, value)
        expired = self.observe(self.fixture.process_snapshot())
        self.assertFalse(expired["requests"][0]["manager_actionable"])
        self.assertIsNone(
            select_actionable(
                conditions_from_snapshot(expired),
                expired,
                observed_at=NOW,
                acknowledged_event_ids=set(),
            )
        )

    def test_absent_request_producer_is_stale(self) -> None:
        self.fixture.status()
        self._request(pid=999)
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual("REQUEST_STALE", observed["requests"][0]["operational_state"])
        self.assertEqual([], observed["mcps"])

    def test_explicit_mcp_request_role_produces_mcp_lifecycle(self) -> None:
        self.fixture.status(mcp_servers=["byo-firmware-stm-a"])
        path, value = self._request()
        value["live_lifetime"]["role"] = "mcp"
        value["live_lifetime"]["mcp_server"] = "byo-firmware-stm-a"
        value["live_lifetime"]["session_id"] = "thread-atlas"
        write_json(path, value)
        write_json(
            self.fixture.workspace() / "permission-requests" / "req.relay.json",
            {
                "decision": "approved",
                "request_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "run_id": "run-1",
                "session_id": "thread-atlas",
            },
        )
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual("RELAYED", observed["requests"][0]["operational_state"])
        self.assertEqual("PROVEN", observed["requests"][0]["mcp_lifetime_state"])
        self.assertEqual("MCP_RUNNING", observed["mcps"][0]["operational_state"])
        self.assertEqual("byo-firmware-stm-a", observed["mcps"][0]["server_name"])

    def test_object_shaped_mcp_declaration_without_mcp_lifetime_never_relays(
        self,
    ) -> None:
        self.fixture.status()
        path, value = self._request()
        value["mcp_server"] = {"name": "byo-firmware-stm-b"}
        write_json(path, value)
        write_json(
            self.fixture.workspace() / "permission-requests" / "req.relay.json",
            {
                "decision": "approved",
                "request_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "run_id": "run-1",
                "session_id": "thread-atlas",
            },
        )
        observed = self.observe(self.fixture.process_snapshot())
        request = observed["requests"][0]
        self.assertEqual("REQUEST_AMBIGUOUS", request["operational_state"])
        self.assertEqual("UNPROVEN", request["mcp_lifetime_state"])
        self.assertEqual("BOUND", request["relay_state"])

    def test_creation_time_utc_proves_live_mcp_launcher_server_provider_lifetime(
        self,
    ) -> None:
        self.fixture.status()
        path, value = self._request()
        timestamp = NOW.isoformat().replace("+00:00", "Z")
        value["live_lifetime"] = {
            "run_id": "run-1",
            "session_id": "thread-atlas",
            "role": "mcp",
            "mcp_server": {"name": "byo-firmware-stm-a"},
            "launcher": {"pid": 301, "creation_time_utc": timestamp},
            "server": {"pid": 302, "creation_time_utc": timestamp},
            "provider": {"pid": 303, "creation_time_utc": timestamp},
        }
        write_json(path, value)
        base = self.fixture.process_snapshot()
        info = type(base.processes[0])
        snapshot = type(base)(
            True,
            tuple(base.processes)
            + (
                info(301, 1, "powershell", "launcher", NOW),
                info(302, 301, "python", "mcp server", NOW),
                info(303, 302, "python", "provider", NOW),
            ),
            (),
            "fake",
        )
        observed = self.observe(snapshot)
        request = observed["requests"][0]
        self.assertEqual("LIVE", request["lifetime_state"])
        self.assertEqual("PROVEN", request["mcp_lifetime_state"])
        self.assertEqual("RELAY_READY", request["operational_state"])

    def test_mismatched_or_absent_creation_time_mcp_lifetimes_fail_closed(self) -> None:
        for name, timestamp, include_processes in (
            (
                "mismatched",
                (NOW + timedelta(seconds=10)).isoformat().replace("+00:00", "Z"),
                True,
            ),
            ("absent", NOW.isoformat().replace("+00:00", "Z"), False),
        ):
            with self.subTest(name=name):
                self.fixture.status()
                path, value = self._request()
                value["live_lifetime"] = {
                    "run_id": "run-1",
                    "session_id": "thread-atlas",
                    "role": "mcp",
                    "mcp_server": {"name": "byo-firmware-stm-a"},
                    "server": {"pid": 301, "creation_time_utc": timestamp},
                }
                write_json(path, value)
                base = self.fixture.process_snapshot()
                info = type(base.processes[0])
                snapshot = type(base)(
                    True,
                    tuple(base.processes)
                    + (
                        (info(301, 1, "python", "mcp server", NOW),)
                        if include_processes
                        else ()
                    ),
                    (),
                    "fake",
                )
                request = self.observe(snapshot)["requests"][0]
                self.assertIn(request["lifetime_state"], {"ABSENT", "UNKNOWN"})
                self.assertEqual("UNPROVEN", request["mcp_lifetime_state"])
                self.assertNotEqual("RELAY_READY", request["operational_state"])

    def test_a26_closed_before_board_action_stays_exited_when_nested_parent_pid_is_reused(
        self,
    ) -> None:
        record = {
            "schema": "mcp-lifetime/v1",
            "server_name": "a26_counter_s1d_mcp",
            "declared_lane_id": "20260731-s1-clean-d:Delta:A26",
            "persistent_session_id": "synthetic-a26-session",
            "launcher_pid": 186400,
            "mcp_process": {
                "pid": 186656,
                "parent_pid": 194236,
                "name": "python.exe",
                "created_utc": "/Date(1785514961305)/",
            },
            "created_utc": NOW.isoformat().replace("+00:00", "Z"),
            "lifetime_status": "closed_before_board_action",
            "board_action_started": False,
        }
        reused_parent = self._snapshot_with_process(194236, NOW + timedelta(minutes=1))

        while_reused = self._mcp_observation(record, reused_parent)
        self.assertEqual("MCP_EXITED", while_reused["operational_state"])
        self.assertEqual(
            {"absent"}, {item["state"] for item in while_reused["processes"]}
        )

    def test_exact_pid_absent_after_cleanup_stays_terminal_when_pid_is_reused(
        self,
    ) -> None:
        record = {
            "server_name": "historical-mcp",
            "pid": 301,
            "terminal_state": "exact_pid_absent_after_cleanup",
        }
        observed = self._mcp_observation(record, self._snapshot_with_process(301, NOW))

        self.assertEqual("MCP_EXITED", observed["operational_state"])

    def test_a24_creation_utc_marks_recycled_pid_mismatched_and_exited(self) -> None:
        record = {
            "schema": "mcp-lifetime/v1",
            "server_name": "a24_pair_a_s1d_mcp",
            "declared_lane_id": "20260731-s1-clean-d:Cygnus:A24",
            "persistent_session_id": "synthetic-a24-session",
            "pid": 194892,
            "creation_utc": NOW.isoformat().replace("+00:00", "Z"),
            "creation_time_raw": f"/Date({int(NOW.timestamp() * 1000)})/",
        }
        observed = self._mcp_observation(
            record, self._snapshot_with_process(194892, NOW + timedelta(minutes=1))
        )

        self.assertEqual("MCP_EXITED", observed["operational_state"])
        self.assertEqual("mismatch", observed["processes"][0]["state"])

    def test_windows_creation_time_raw_milliseconds_are_usable_for_live_mcp_identity(
        self,
    ) -> None:
        record = {
            "server_name": "a24_pair_b_s1d_mcp",
            "pid": 195828,
            "creation_utc": None,
            "creation_time_raw": f"/Date({int(NOW.timestamp() * 1000)})/",
        }
        observed = self._mcp_observation(
            record, self._snapshot_with_process(195828, NOW)
        )

        self.assertEqual("MCP_RUNNING", observed["operational_state"])
        self.assertEqual("live", observed["processes"][0]["state"])

    def test_a24_creation_utc_matching_live_pid_remains_running(self) -> None:
        record = {
            "server_name": "a24_pair_a_s1d_mcp",
            "pid": 194892,
            "creation_utc": NOW.isoformat().replace("+00:00", "Z"),
        }
        observed = self._mcp_observation(
            record, self._snapshot_with_process(194892, NOW)
        )

        self.assertEqual("MCP_RUNNING", observed["operational_state"])

    def test_nonterminal_mcp_pid_without_creation_evidence_remains_unknown(
        self,
    ) -> None:
        record = {"server_name": "current-mcp", "pid": 301}
        observed = self._mcp_observation(record, self._snapshot_with_process(301, NOW))

        self.assertEqual("MCP_STATE_UNKNOWN", observed["operational_state"])

    def test_nested_created_utc_mcp_identity_remains_live(self) -> None:
        record = {
            "server_name": "current-mcp",
            "mcp_process": {
                "pid": 301,
                "created_utc": NOW.isoformat().replace("+00:00", "Z"),
            },
        }
        observed = self._mcp_observation(record, self._snapshot_with_process(301, NOW))

        self.assertEqual("MCP_RUNNING", observed["operational_state"])

    def test_versioned_mcp_processes_container_covers_live_and_exited(self) -> None:
        self.fixture.status()
        timestamp = NOW.isoformat().replace("+00:00", "Z")
        for name, pid, snapshot, expected_operational, expected_process in (
            (
                "live",
                301,
                self._snapshot_with_process(301, NOW),
                "MCP_RUNNING",
                "live",
            ),
            (
                "exited",
                999,
                self.fixture.process_snapshot(),
                "MCP_EXITED",
                "absent",
            ),
        ):
            with self.subTest(name=name):
                observed = self._mcp_observation(
                    {
                        "schema": "mcp-lifetime/v1",
                        "server_name": "versioned-plural-mcp",
                        "mcp_processes": [{"pid": pid, "created_utc": timestamp}],
                    },
                    snapshot,
                )
                self.assertEqual(expected_operational, observed["operational_state"])
                self.assertEqual(expected_process, observed["processes"][0]["state"])

    def test_unreadable_declared_mcp_record_blocks_terminal_release_and_preserves_error(
        self,
    ) -> None:
        status = self.fixture.status(state="exited")
        value = json.loads(status.read_text(encoding="utf-8"))
        value["record_paths"] = {"mcp": ["declared/missing-mcp.json"]}
        write_json(status, value)

        observed = self.observe(ProcessSnapshot(True, (), (), "fake"))
        lane = observed["lanes"][0]
        self.assertEqual("EXITED", lane["process_state"])
        self.assertFalse(lane["resource_release_possible"])
        self.assertIn(
            "declared helper/MCP record observation is incomplete",
            lane["resource_ambiguity"],
        )
        self.assertTrue(
            any(
                item["code"] == "MCP_READ_ERROR"
                and item["path"].endswith("declared\\missing-mcp.json")
                for item in observed["observation_errors"]
            )
        )

    def test_partial_multi_pid_lifetime_is_ambiguous_not_relay_ready(self) -> None:
        self.fixture.status()
        path, value = self._request()
        value["live_lifetime"]["required_helper"] = {
            "pid": 999,
            "started_utc": NOW.isoformat().replace("+00:00", "Z"),
        }
        write_json(path, value)
        observed = self.observe(self.fixture.process_snapshot())
        request = observed["requests"][0]
        self.assertEqual("UNKNOWN", request["lifetime_state"])
        self.assertEqual("REQUEST_AMBIGUOUS", request["operational_state"])

    def test_partial_creation_unknown_request_is_actionable_only_before_deadline(
        self,
    ) -> None:
        self.fixture.status()
        path, value = self._request()
        value["live_lifetime"]["required_helper"] = {"pid": 999}
        value["deadline_utc"] = (
            (NOW + timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        )
        write_json(path, value)
        current = self.observe(self.fixture.process_snapshot())
        self.assertEqual("UNKNOWN", current["requests"][0]["lifetime_state"])
        self.assertTrue(current["requests"][0]["manager_actionable"])
        self.assertIsNotNone(
            select_actionable(
                conditions_from_snapshot(current),
                current,
                observed_at=NOW,
                acknowledged_event_ids=set(),
            )
        )

        value["deadline_utc"] = (
            (NOW - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        )
        write_json(path, value)
        expired = self.observe(self.fixture.process_snapshot())
        self.assertFalse(expired["requests"][0]["manager_actionable"])
        self.assertIsNone(
            select_actionable(
                conditions_from_snapshot(expired),
                expired,
                observed_at=NOW,
                acknowledged_event_ids=set(),
            )
        )

    def test_path_only_or_wrong_hash_relay_is_unbound(self) -> None:
        self.fixture.status()
        self._request()
        write_json(
            self.fixture.workspace() / "permission-requests" / "req.relay.json",
            {
                "decision": "approved",
                "request_sha256": "0" * 64,
                "run_id": "run-1",
                "session_id": "thread-atlas",
            },
        )
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual("RELAY_UNBOUND", observed["requests"][0]["operational_state"])

    def test_exact_hash_and_identity_relay_binds(self) -> None:
        self.fixture.status()
        path, _ = self._request()
        write_json(
            self.fixture.workspace() / "permission-requests" / "req.relay.json",
            {
                "decision": "approved",
                "request_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "run_id": "run-1",
                "session_id": "thread-atlas",
            },
        )
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual("RELAYED", observed["requests"][0]["operational_state"])

    def test_bound_relay_with_exited_producer_is_inactive(self) -> None:
        self.fixture.status()
        path, _ = self._request(pid=999)
        write_json(
            self.fixture.workspace() / "permission-requests" / "req.relay.json",
            {
                "decision": "approved",
                "request_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "run_id": "run-1",
                "session_id": "thread-atlas",
            },
        )
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual(
            "RELAYED_INACTIVE", observed["requests"][0]["operational_state"]
        )

    def test_changed_request_invalidates_existing_relay(self) -> None:
        self.fixture.status()
        path, value = self._request()
        old_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        relay = self.fixture.workspace() / "permission-requests" / "req.relay.json"
        write_json(
            relay,
            {
                "decision": "approved",
                "request_sha256": old_hash,
                "run_id": "run-1",
                "session_id": "thread-atlas",
            },
        )
        value["changed"] = True
        write_json(path, value)
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual("RELAY_UNBOUND", observed["requests"][0]["operational_state"])

    def test_sidecar_mismatch_is_ambiguous(self) -> None:
        self.fixture.status()
        path, _ = self._request()
        (path.parent / (path.name + ".sha256")).write_text("0" * 64, encoding="ascii")
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual(
            "REQUEST_AMBIGUOUS", observed["requests"][0]["operational_state"]
        )

    def test_matching_sidecar_preserves_readiness(self) -> None:
        self.fixture.status()
        path, _ = self._request()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        (path.parent / (path.name + ".sha256")).write_text(
            digest + "  req.json\n", encoding="ascii"
        )
        observed = self.observe(self.fixture.process_snapshot())
        self.assertTrue(observed["requests"][0]["sidecar_matches"])
        self.assertEqual("RELAY_READY", observed["requests"][0]["operational_state"])

    def test_expiry_warning_is_bucketed(self) -> None:
        self.fixture.status()
        path, value = self._request()
        value["deadline_utc"] = (
            (NOW + timedelta(seconds=60)).isoformat().replace("+00:00", "Z")
        )
        write_json(path, value)
        observed = self.observe(self.fixture.process_snapshot())
        self.assertEqual("WARNING", observed["requests"][0]["expiry_bucket"])

    def test_malformed_request_becomes_observation_error(self) -> None:
        self.fixture.status()
        path = self.fixture.workspace() / "permission-requests" / "broken.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"partial":', encoding="utf-8")
        observed = self.observe(self.fixture.process_snapshot())
        self.assertTrue(
            any(
                item["code"] == "REQUEST_READ_ERROR"
                for item in observed["observation_errors"]
            )
        )

    def test_provider_wait_checkpoint_is_reported(self) -> None:
        self.fixture.status(state="exited")
        (self.fixture.workspace() / "PARALLEL_CHECKPOINT.md").write_text(
            "# State\n\nWAITING_FOR_PROVIDER\n", encoding="utf-8"
        )
        snapshot = self.fixture.process_snapshot()
        observed = self.observe(type(snapshot)(True, (), (), "fake"))
        self.assertTrue(observed["lanes"][0]["provider_wait"])

    def test_terminal_checkpoint_before_mcp_creation_has_no_invented_mcp_or_ambiguity(
        self,
    ) -> None:
        self.fixture.status(state="exited", mcp_servers=["byo-firmware-stm-a"])
        (self.fixture.workspace() / "PARALLEL_CHECKPOINT.md").write_text(
            "# complete\n", encoding="utf-8"
        )
        snapshot = self.fixture.process_snapshot()
        observed = self.observe(type(snapshot)(True, (), (), "fake"))
        lane = observed["lanes"][0]
        self.assertEqual("CHECKPOINTED", lane["operational_state"])
        self.assertTrue(lane["resource_release_possible"])
        self.assertNotIn(
            "declared MCP server has no lane/session-correlated PID lifetime evidence",
            lane["resource_ambiguity"],
        )
        self.assertEqual([], observed["mcps"])

    def test_running_lane_with_missing_declared_mcp_still_synthesizes_unknown(
        self,
    ) -> None:
        self.fixture.status(mcp_servers=["byo-firmware-stm-a"])
        observed = self.observe(self.fixture.process_snapshot())
        lane = observed["lanes"][0]
        self.assertEqual("RUNNING_CODEX", lane["operational_state"])
        self.assertIn(
            "declared MCP server has no lane/session-correlated PID lifetime evidence",
            lane["resource_ambiguity"],
        )
        self.assertEqual("MCP_STATE_UNKNOWN", observed["mcps"][0]["operational_state"])

    def test_waiting_relay_lane_with_missing_declared_mcp_still_synthesizes_unknown(
        self,
    ) -> None:
        self.fixture.status(mcp_servers=["byo-firmware-stm-a"])
        self._request()
        observed = self.observe(self.fixture.process_snapshot())
        lane = observed["lanes"][0]
        self.assertEqual("WAITING_RELAY", lane["operational_state"])
        self.assertIn(
            "declared MCP server has no lane/session-correlated PID lifetime evidence",
            lane["resource_ambiguity"],
        )
        self.assertEqual("MCP_STATE_UNKNOWN", observed["mcps"][0]["operational_state"])

    def test_process_unknown_lane_with_missing_declared_mcp_still_synthesizes_unknown(
        self,
    ) -> None:
        self.fixture.status(mcp_servers=["byo-firmware-stm-a"])
        observed = self.observe(self.fixture.process_snapshot(complete=False))
        lane = observed["lanes"][0]
        self.assertEqual("PROCESS_STATE_UNKNOWN", lane["operational_state"])
        self.assertIn(
            "declared MCP server has no lane/session-correlated PID lifetime evidence",
            lane["resource_ambiguity"],
        )
        self.assertEqual("MCP_STATE_UNKNOWN", observed["mcps"][0]["operational_state"])

    def test_unknown_lane_and_helper_running_are_active_mcp_expectations(self) -> None:
        self.fixture.status(state="unknown", mcp_servers=["byo-firmware-stm-a"])
        base = self.fixture.process_snapshot()
        observed = self.observe(type(base)(True, (), (), "fake"))
        lane = observed["lanes"][0]
        self.assertEqual("UNKNOWN", lane["operational_state"])
        self.assertIn(
            "declared MCP server has no lane/session-correlated PID lifetime evidence",
            lane["resource_ambiguity"],
        )
        self.assertEqual("MCP_STATE_UNKNOWN", observed["mcps"][0]["operational_state"])
        self.assertTrue(
            _lane_is_active_or_unknown({"operational_state": "HELPER_RUNNING"})
        )

    def test_explicit_terminal_mcp_lifetime_releases_exited_checkpointed_lane(
        self,
    ) -> None:
        self.fixture.status(
            state="exited",
            mcp_servers=["byo-firmware-stm-a"],
            declared_lane_id="canary:atlas:A22:B14",
        )
        (self.fixture.workspace() / "PARALLEL_CHECKPOINT.md").write_text(
            "# checkpoint\n", encoding="utf-8"
        )
        write_json(
            self.fixture.workspace() / "mcp-lifetime.json",
            {
                "server_name": "byo-firmware-stm-a",
                "declared_lane_id": "canary:atlas:A22:B14",
                "session_id": "thread-atlas",
                "pid": 301,
                "creation_time_utc": NOW.isoformat().replace("+00:00", "Z"),
            },
        )
        snapshot = self.fixture.process_snapshot()
        observed = self.observe(type(snapshot)(True, (), (), "fake"))
        lane = observed["lanes"][0]
        self.assertEqual("canary:atlas:A22:B14", lane["lane_id"])
        self.assertEqual("CHECKPOINTED", lane["operational_state"])
        self.assertEqual("MCP_EXITED", observed["mcps"][0]["operational_state"])
        self.assertTrue(lane["resource_release_possible"])

    def test_terminal_lane_ignores_uncorrelated_mcp_lifetime_without_synthesizing_unknown(
        self,
    ) -> None:
        self.fixture.status(
            state="exited",
            mcp_servers=["byo-firmware-stm-a"],
            declared_lane_id="canary:atlas:A22:B14",
        )
        write_json(
            self.fixture.workspace() / "mcp-lifetime.json",
            {
                "server_name": "byo-firmware-stm-a",
                "declared_lane_id": "canary:boreal:D31",
                "session_id": "thread-atlas",
                "pid": 301,
                "creation_time_utc": NOW.isoformat().replace("+00:00", "Z"),
            },
        )
        snapshot = self.fixture.process_snapshot()
        observed = self.observe(type(snapshot)(True, (), (), "fake"))
        lane = observed["lanes"][0]
        self.assertTrue(lane["resource_release_possible"])
        self.assertEqual("MCP_EXITED", observed["mcps"][0]["operational_state"])

    def test_terminal_lane_with_correlated_unknown_mcp_remains_nonreleasable(
        self,
    ) -> None:
        self.fixture.status(
            state="exited",
            mcp_servers=["byo-firmware-stm-a"],
            declared_lane_id="canary:atlas:A22:B14",
        )
        write_json(
            self.fixture.workspace() / "mcp-lifetime.json",
            {
                "server_name": "byo-firmware-stm-a",
                "declared_lane_id": "canary:atlas:A22:B14",
                "session_id": "thread-atlas",
                "pid": 301,
            },
        )
        base = self.fixture.process_snapshot()
        info = type(base.processes[0])
        observed = self.observe(
            type(base)(True, (info(301, 1, "mcp", "synthetic mcp", NOW),), (), "fake")
        )
        lane = observed["lanes"][0]
        self.assertFalse(lane["resource_release_possible"])
        self.assertEqual("MCP_STATE_UNKNOWN", observed["mcps"][0]["operational_state"])

    def test_absent_controller_helper_and_mcp_tree_allows_release_candidate(
        self,
    ) -> None:
        self.fixture.status(state="exited")
        self._request(pid=999)
        snapshot = self.fixture.process_snapshot()
        observed = self.observe(type(snapshot)(True, (), (), "fake"))
        lane = observed["lanes"][0]
        self.assertEqual("EXITED", lane["process_state"])
        self.assertEqual([], observed["mcps"])
        self.assertTrue(lane["resource_release_possible"])

    def test_unrelated_mcp_evidence_does_not_satisfy_another_lane(self) -> None:
        self.fixture.status(
            label="atlas",
            state="exited",
            doer="Atlas",
            task="A00",
            controller_pid=101,
            codex_pid=102,
            mcp_servers=["server-a"],
        )
        self.fixture.status(
            label="boreal",
            state="exited",
            doer="Boreal",
            task="A01",
            controller_pid=201,
            codex_pid=202,
            mcp_servers=["server-b"],
        )
        write_json(
            self.fixture.workspace() / "mcp_process.json",
            {
                "server_name": "server-a",
                "session_id": "thread-atlas",
                "pid": 301,
                "started_utc": NOW.isoformat().replace("+00:00", "Z"),
            },
        )
        base = self.fixture.process_snapshot()
        info = type(base.processes[0])
        snapshot = type(base)(
            True,
            (info(301, 1, "mcp", "synthetic mcp", NOW),),
            (),
            "fake",
        )
        observed = self.observe(snapshot)
        lanes = {lane["doer"]: lane for lane in observed["lanes"]}
        self.assertNotIn(
            "declared MCP server has no lane/session-correlated PID lifetime evidence",
            lanes["Atlas"]["resource_ambiguity"],
        )
        self.assertNotIn(
            "declared MCP server has no lane/session-correlated PID lifetime evidence",
            lanes["Boreal"]["resource_ambiguity"],
        )
        self.assertTrue(lanes["Boreal"]["resource_release_possible"])

    def test_unscoped_mcp_record_does_not_correlate_to_lane(self) -> None:
        status = self.fixture.status(state="exited", mcp_servers=["byo-firmware-stm-a"])
        value = json.loads(status.read_text(encoding="utf-8"))
        value.pop("thread_id")
        write_json(status, value)
        write_json(
            self.fixture.workspace() / "mcp_process.json",
            {
                "server_name": "byo-firmware-stm-a",
                "pid": 301,
                "started_utc": NOW.isoformat().replace("+00:00", "Z"),
            },
        )
        base = self.fixture.process_snapshot()
        info = type(base.processes[0])
        snapshot = type(base)(
            True,
            (info(301, 1, "mcp", "synthetic mcp", NOW),),
            (),
            "fake",
        )
        observed = self.observe(snapshot)
        lane = observed["lanes"][0]
        self.assertNotIn(
            "declared MCP server has no lane/session-correlated PID lifetime evidence",
            lane["resource_ambiguity"],
        )
        self.assertTrue(lane["resource_release_possible"])

    def test_correlated_live_mcp_blocks_release_without_server_declaration(
        self,
    ) -> None:
        self.fixture.status(state="exited", mcp_servers=["byo-firmware-stm-a"])
        write_json(
            self.fixture.workspace() / "mcp_process.json",
            {
                "server_name": "byo-firmware-stm-a",
                "session_id": "thread-atlas",
                "pid": 301,
                "started_utc": NOW.isoformat().replace("+00:00", "Z"),
            },
        )
        base = self.fixture.process_snapshot()
        info = type(base.processes[0])
        snapshot = type(base)(
            True,
            (info(301, 1, "mcp", "synthetic mcp", NOW),),
            (),
            "fake",
        )
        observed = self.observe(snapshot)
        self.assertEqual("MCP_RUNNING", observed["mcps"][0]["operational_state"])
        self.assertFalse(observed["lanes"][0]["resource_release_possible"])

    def test_generic_run_id_is_not_an_mcp_resource(self) -> None:
        self.fixture.status()
        self._request()
        observed = self.observe(self.fixture.process_snapshot())
        resources = observed["lanes"][0]["resources"]
        self.assertIn("producer-lifetime:run-1", resources)
        self.assertFalse(any(item.startswith("mcp-lifetime:") for item in resources))
        self.assertEqual([], observed["mcps"])

    def test_resource_conflict_uses_board_alias_normalization(self) -> None:
        self.fixture.status(
            run="A00_one",
            doer="Atlas",
            board_tokens=["STM-A"],
            controller_pid=101,
            codex_pid=102,
        )
        self.fixture.status(
            run="A01_two",
            doer="Boreal",
            task="A01",
            board_tokens=["stm_a"],
            controller_pid=201,
            codex_pid=202,
        )
        snapshot = self.fixture.process_snapshot()
        info = type(snapshot.processes[0])
        processes = tuple(snapshot.processes) + (
            info(201, 1, "python", "controller", NOW),
            info(202, 201, "codex", "codex", NOW),
        )
        observed = self.observe(type(snapshot)(True, processes, (), "fake"))
        conflicts = observed["resource_conflicts"]
        self.assertEqual("board:stm-a", conflicts[0]["resource"])
        self.assertEqual(2, len(conflicts[0]["owners"]))


if __name__ == "__main__":
    unittest.main()
