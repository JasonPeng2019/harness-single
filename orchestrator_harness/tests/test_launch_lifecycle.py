from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from orchestrator_harness import controller, launch
from orchestrator_harness.controller import ControllerError
from orchestrator_harness.core import content_hash
from orchestrator_harness.leases import LeaseError


def bind_invocation(lane, invocation):
    git = {"fixture": "launch-lifecycle", "bootstrap_tip": "a" * 40}
    lane["git"] = git
    lane["provider"] = invocation["provider"]
    invocation["git"] = git
    invocation["content_hash"] = content_hash(invocation)
    lane["invocation_hash"] = invocation["content_hash"]


class LaunchHandshakeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.runtime = self.root / "runtime"
        self.worktree = self.root / "worktree"
        (self.worktree / ".agent-workspace").mkdir(parents=True)
        binding = self.root / "orchestrator_harness" / "provider_adapters" / "codex" / "launcher_binding.py"
        binding.parent.mkdir(parents=True)
        binding.write_text("# binding\n", encoding="utf-8")
        self.lane = {
            "schema": "lane/v1", "lane_id": "lane-1", "run_id": "run-1",
            "lifecycle": "prepared", "process": {}, "worktree_path": str(self.worktree),
            "controller_status_path": str(self.worktree / ".agent-workspace" / "controller.status.json"),
            "controller_events_path": str(self.worktree / ".agent-workspace" / "controller.events.jsonl"),
        }
        self.invocation = {
            "schema": "controller-invocation/v1", "lane_id": "lane-1",
            "run_id": "run-1", "provider": {"id": "codex"},
        }
        bind_invocation(self.lane, self.invocation)

    def test_fast_terminal_status_is_a_successful_handshake(self) -> None:
        child = MagicMock(pid=41)
        child.poll.return_value = 0
        terminal = {
            "schema": "controller-status/v1", "lane_id": "lane-1", "run_id": "run-1",
            "controller_state": "exited", "provider_state": {"state": "exited", "exit_code": 0},
            "cleanup_proven": True, "recorded_status": "result_invalid",
        }
        running_lane = {**self.lane, "lifecycle": "running", "process": {"pid": 41, "creation_time": "created-1"}}
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(launch, "load_config", return_value=SimpleNamespace(runtime_root=self.runtime)),
            patch.object(launch, "read_runtime_state", return_value={"state": "OPEN"}),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", self.lane)),
            patch.object(launch, "read_record", return_value=self.invocation),
            patch.object(launch.processes, "spawn_detached", return_value=child),
            patch.object(launch.processes, "process_identity", return_value={"pid": 41, "creation_time": "created-1"}),
            patch.object(launch, "update_lane", return_value=running_lane) as update,
            patch.object(launch, "_read_controller_status", return_value=terminal),
            patch.object(launch, "read_lane", return_value=running_lane),
        ):
            result = launch.run_launch("lane-1")
        self.assertTrue(result["ok"], result)
        self.assertEqual("LAUNCH_OK", result["code"])
        self.assertIn("result_invalid", result["summary"])
        update.assert_called_once()

    def test_unrecordable_controller_identity_stops_exact_child(self) -> None:
        child = MagicMock(pid=41)
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(launch, "load_config", return_value=SimpleNamespace(runtime_root=self.runtime)),
            patch.object(launch, "read_runtime_state", return_value={"state": "OPEN"}),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", self.lane)),
            patch.object(launch, "read_record", return_value=self.invocation),
            patch.object(launch.processes, "spawn_detached", return_value=child),
            patch.object(launch.processes, "process_identity", return_value=None),
        ):
            result = launch.run_launch("lane-1")
        self.assertFalse(result["ok"])
        child.terminate.assert_called_once_with()
        child.wait.assert_called_once_with(timeout=10.0)

    def test_lane_persistence_failure_stops_exact_child(self) -> None:
        child = MagicMock(pid=41)
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(launch, "load_config", return_value=SimpleNamespace(runtime_root=self.runtime)),
            patch.object(launch, "read_runtime_state", return_value={"state": "OPEN"}),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", self.lane)),
            patch.object(launch, "read_record", return_value=self.invocation),
            patch.object(launch.processes, "spawn_detached", return_value=child),
            patch.object(launch.processes, "process_identity", return_value={"pid": 41, "creation_time": "created-1"}),
            patch.object(launch, "update_lane", side_effect=OSError("write failed")),
        ):
            result = launch.run_launch("lane-1")
        self.assertFalse(result["ok"])
        child.terminate.assert_called_once_with()
        child.wait.assert_called_once_with(timeout=10.0)


class CleanupDecisionTests(unittest.TestCase):
    def test_never_launched_prepared_lane_is_clean(self) -> None:
        lane = {"lifecycle": "prepared", "process": {}, "controller_status_path": "missing"}
        with patch.object(launch, "_read_controller_status", return_value=None):
            self.assertTrue(launch._terminate_lane_processes(lane))

    def test_launched_lane_without_status_or_boundary_fails_closed(self) -> None:
        lane = {"lifecycle": "running", "process": {}, "controller_status_path": "missing"}
        with patch.object(launch, "_read_controller_status", return_value=None):
            self.assertFalse(launch._terminate_lane_processes(lane))
        lane["process"] = {"pid": 41, "creation_time": "created-1"}
        with (
            patch.object(launch, "_read_controller_status", return_value={"cleanup_proven": False, "provider_state": {}}),
            patch.object(launch.processes, "terminate_process", return_value=True),
        ):
            self.assertFalse(launch._terminate_lane_processes(lane))


class ControllerLeaseTests(unittest.TestCase):
    def test_lease_busy_is_terminal_clean_and_restores_prepared(self) -> None:
        lane = {"lane_id": "lane-1", "run_id": "run-1", "worktree_path": "worktree", "controller_status_path": "status", "controller_events_path": "events"}
        invocation = {"lane_id": "lane-1", "run_id": "run-1", "provider": {"id": "codex"}, "exclusive_resources": ["shared"]}
        bind_invocation(lane, invocation)
        updates: list[dict[str, object]] = []
        def update(_rt, _epoch, _lane, mutate):
            value = mutate(dict(lane))
            updates.append(value)
            return {**lane, **value}
        with (
            patch.object(controller, "find_harness_root", return_value=Path("root")),
            patch.object(controller, "load_config", return_value=SimpleNamespace(runtime_root=Path("runtime"))),
            patch.object(controller, "find_active_lane", return_value=("epoch-1", lane)),
            patch.object(controller, "read_record", return_value=invocation),
            patch.object(controller, "_write_status") as status,
            patch.object(controller, "_append_event"),
            patch.object(controller.processes, "process_identity", return_value={"pid": 41, "creation_time": "created-1"}),
            patch.object(controller, "update_lane", side_effect=update),
            patch.object(controller, "_load_binding", return_value=object()),
            patch.object(controller, "acquire_leases", side_effect=LeaseError("LAUNCH_LEASE_BUSY", "held")),
        ):
            self.assertEqual(2, controller.run_controller("lane-1"))
        self.assertEqual("prepared", updates[-1]["lifecycle"])
        self.assertEqual({}, updates[-1]["process"])
        terminal = status.call_args_list[-1].args[1]
        self.assertEqual("exited", terminal["controller_state"])
        self.assertEqual("not_started", terminal["provider_state"]["state"])
        self.assertTrue(terminal["cleanup_proven"])

    def test_provider_not_created_releases_only_current_run_lease(self) -> None:
        lane = {"lane_id": "lane-1", "run_id": "run-1", "worktree_path": "worktree", "controller_status_path": "status", "controller_events_path": "events"}
        invocation = {"lane_id": "lane-1", "run_id": "run-1", "provider": {"id": "codex"}, "exclusive_resources": ["shared"]}
        bind_invocation(lane, invocation)
        with (
            patch.object(controller, "find_harness_root", return_value=Path("root")),
            patch.object(controller, "load_config", return_value=SimpleNamespace(runtime_root=Path("runtime"))),
            patch.object(controller, "find_active_lane", return_value=("epoch-1", lane)),
            patch.object(controller, "read_record", side_effect=[invocation, {}]),
            patch.object(controller, "_write_status"),
            patch.object(controller, "_append_event"),
            patch.object(controller.processes, "process_identity", return_value={"pid": 41, "creation_time": "created-1"}),
            patch.object(controller, "update_lane", return_value=lane),
            patch.object(controller, "_load_binding", return_value=object()),
            patch.object(controller, "acquire_leases"),
            patch.object(controller, "_run_provider", side_effect=ControllerError("LAUNCH_PROVIDER_START_FAILED", "not created", no_provider_started=True)),
            patch.object(controller, "release_leases") as release,
        ):
            self.assertEqual(4, controller.run_controller("lane-1"))
        release.assert_called_once_with(Path("runtime"), "lane-1", "run-1")


if __name__ == "__main__":
    unittest.main()
