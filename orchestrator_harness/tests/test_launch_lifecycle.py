from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from orchestrator_harness import controller, launch, shutdown
from orchestrator_harness.bootstrap import BOOTSTRAP_REQUEST_INVALID, BootstrapError
from orchestrator_harness.controller import ControllerError
from orchestrator_harness.leases import LeaseError
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.tests.support import prepare_lane_fixture, seed_provider_history


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
            "run_id": "run-1", "provider": {
                "id": "codex", "model": "configured-model",
                "launch_config": {"reasoning_effort": "high", "service_tier": "priority"},
            },
        }
        self.lane["provider"] = self.invocation["provider"]
        self.lane = prepare_lane_fixture(self.runtime, "epoch-1", self.lane)
        seed_provider_history(self.runtime, self.lane)
        atomic_write_json(self.runtime / 'epochs/epoch-1/lanes/lane-1/lane.json', self.lane)

    def test_missing_provider_preferences_refuse_launch_before_spawn(self) -> None:
        invocation = {
            **self.invocation,
            "provider": {"id": "codex", "model": "configured-model"},
        }
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(launch, "load_config", return_value=SimpleNamespace(runtime_root=self.runtime)),
            patch.object(launch, "read_runtime_state", return_value={"state": "OPEN"}),
            patch.object(launch, "read_current_epoch", return_value={"epoch_id": "epoch-1"}),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", self.lane)),
            patch.object(launch, "read_record", return_value=invocation),
            patch.object(
                launch,
                "_validate_provider_launch_config",
                side_effect=BootstrapError(
                    BOOTSTRAP_REQUEST_INVALID,
                    "Codex launch_config must be an object",
                ),
            ),
            patch.object(launch.processes, "spawn_detached") as spawn,
        ):
            result = launch.run_launch("lane-1")
        self.assertFalse(result["ok"])
        self.assertEqual(launch.LAUNCH_INVOCATION_INVALID, result["code"])
        spawn.assert_not_called()

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
            patch.object(launch, "read_current_epoch", return_value={"epoch_id": "epoch-1"}),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", self.lane)),
            patch.object(launch, "read_record", return_value=self.invocation),
            patch.object(
                launch,
                "_validate_provider_launch_config",
                return_value=self.invocation["provider"]["launch_config"],
            ),
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
        self.assertEqual(2, update.call_count)
        self.assertEqual([self.lane["controller_status_path"]], result["evidence_paths"])

    def test_unrecordable_controller_identity_is_not_targeted_by_pid(self) -> None:
        child = MagicMock(pid=41)
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(launch, "load_config", return_value=SimpleNamespace(runtime_root=self.runtime)),
            patch.object(launch, "read_runtime_state", return_value={"state": "OPEN"}),
            patch.object(launch, "read_current_epoch", return_value={"epoch_id": "epoch-1"}),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", self.lane)),
            patch.object(launch, "read_record", return_value=self.invocation),
            patch.object(
                launch,
                "_validate_provider_launch_config",
                return_value=self.invocation["provider"]["launch_config"],
            ),
            patch.object(launch.processes, "spawn_detached", return_value=child),
            patch.object(launch.processes, "process_identity", return_value=None),
            patch.object(launch, "update_lane", return_value=self.lane),
        ):
            result = launch.run_launch("lane-1")
        self.assertFalse(result["ok"])
        child.terminate.assert_not_called()
        child.wait.assert_not_called()

    def test_lane_persistence_failure_stops_exact_child(self) -> None:
        child = MagicMock(pid=41)
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(launch, "load_config", return_value=SimpleNamespace(runtime_root=self.runtime)),
            patch.object(launch, "read_runtime_state", return_value={"state": "OPEN"}),
            patch.object(launch, "read_current_epoch", return_value={"epoch_id": "epoch-1"}),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", self.lane)),
            patch.object(launch, "read_record", return_value=self.invocation),
            patch.object(
                launch,
                "_validate_provider_launch_config",
                return_value=self.invocation["provider"]["launch_config"],
            ),
            patch.object(launch.processes, "spawn_detached", return_value=child),
            patch.object(launch.processes, "process_identity", return_value={"pid": 41, "creation_time": "created-1"}),
            patch.object(launch.processes, "terminate_process", return_value=True) as terminate,
            patch.object(launch.processes, "identity_matches", return_value=False),
            patch.object(launch, "update_lane", side_effect=[self.lane, OSError("write failed")]),
        ):
            result = launch.run_launch("lane-1")
        self.assertFalse(result["ok"])
        terminate.assert_called_once_with(41, "created-1", force=True)
        child.wait.assert_called_once_with(timeout=10.0)

    def test_failed_registration_and_cleanup_cannot_look_unlaunched(self) -> None:
        for lifecycle in ("prepared", "running"):
            for failure in ("terminate", "wait"):
                with self.subTest(lifecycle=lifecycle, failure=failure):
                    lane = {**self.lane, "lifecycle": lifecycle, "launch_pending": True}
                    child = MagicMock(pid=41)
                    if failure == "wait":
                        child.wait.side_effect = OSError("cleanup unproven")
                    events = []
                    writes = 0

                    def persist(_runtime, _epoch, _lane, mutate):
                        nonlocal writes
                        writes += 1
                        if writes == 2:
                            raise OSError("identity registration failed")
                        lane.update(mutate(dict(lane)))
                        events.append("persist")
                        return dict(lane)

                    def spawn(*_args, **_kwargs):
                        events.append("spawn")
                        return child

                    with (
                        patch.object(launch, "find_harness_root", return_value=self.root),
                        patch.object(launch, "load_config", return_value=SimpleNamespace(runtime_root=self.runtime)),
                        patch.object(launch, "read_runtime_state", return_value={"state": "OPEN"}),
                        patch.object(launch, "read_current_epoch", return_value={"epoch_id": "epoch-1"}),
                        patch.object(launch, "find_active_lane", return_value=("epoch-1", lane)),
                        patch.object(launch, "read_record", return_value=self.invocation),
                        patch.object(launch, "_validate_provider_launch_config", return_value=self.invocation["provider"]["launch_config"]),
                        patch.object(launch, "_spawn_controller_logged", side_effect=spawn),
                        patch.object(launch.processes, "process_identity", return_value={"pid": 41, "creation_time": "created-1"}),
                        patch.object(launch.processes, "identity_matches", return_value=False),
                        patch.object(launch.processes, "terminate_process", return_value=True, side_effect=OSError("cleanup unproven") if failure == "terminate" else None),
                        patch.object(launch, "update_lane", side_effect=persist),
                    ):
                        result = launch.run_launch("lane-1")
                    self.assertFalse(result["ok"])
                    with patch.object(shutdown.processes, "identity_matches", return_value=False):
                        self.assertFalse(shutdown._lane_clean(self.runtime, lane))
                    self.assertEqual(["persist", "spawn", "persist"], events)
                    self.assertEqual("running", lane["lifecycle"])
                    self.assertIs(False, lane["launch_pending"])


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
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.runtime = Path(temporary.name) / 'runtime'
        worktree = Path(temporary.name) / 'worktree'
        worktree.mkdir()
        self.fixture_lane = {"lane_id": "lane-1", "run_id": "run-1", "worktree_path": str(worktree),
                             "controller_status_path": str(worktree / "status"),
                             "controller_events_path": str(worktree / "events"),
                             "provider": {"id": "codex"}, "lifecycle": "prepared", "process": {}}
        self.fixture_lane["provider"]["launch_config"] = {"permission_mode": "yolo"}
        self.fixture_lane["exclusive_resources"] = ["shared"]
        self.fixture_lane = prepare_lane_fixture(self.runtime, "epoch-1", self.fixture_lane)
        seed_provider_history(self.runtime, self.fixture_lane)
        gate = patch.object(controller, 'require_ready', return_value=None)
        gate.start()
        self.addCleanup(gate.stop)

    def test_lease_busy_is_terminal_clean_and_restores_prepared(self) -> None:
        lane = self.fixture_lane
        invocation = {"lane_id": "lane-1", "run_id": "run-1", "provider": {"id": "codex"}, "exclusive_resources": ["shared"]}
        updates: list[dict[str, object]] = []
        def update(_rt, _epoch, _lane, mutate):
            value = mutate(dict(lane))
            updates.append(value)
            return {**lane, **value}
        with (
            patch.object(controller, "find_harness_root", return_value=Path("root")),
            patch.object(controller, "load_config", return_value=SimpleNamespace(runtime_root=self.runtime)),
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
        lane = self.fixture_lane
        invocation = {"lane_id": "lane-1", "run_id": "run-1", "provider": {"id": "codex"}, "exclusive_resources": ["shared"]}
        with (
            patch.object(controller, "find_harness_root", return_value=Path("root")),
            patch.object(controller, "load_config", return_value=SimpleNamespace(runtime_root=self.runtime)),
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
        release.assert_called_once_with(self.runtime, "lane-1", "run-1")

    def test_run_block_before_provider_creation_still_proves_cleanup(self) -> None:
        from orchestrator_harness.windows_sandbox_preflight import BLOCK
        lane = self.fixture_lane
        invocation = {"lane_id": "lane-1", "run_id": "run-1",
                      "provider": {"id": "codex", "launch_config": {"permission_mode": "yolo"}},
                      "exclusive_resources": ["shared"]}
        atomic_write_json(self.runtime.parent / BLOCK, {"reason": "first failure"})
        with (
            patch.object(controller, "find_harness_root", return_value=self.runtime.parent / "harness"),
            patch.object(controller, "load_config", return_value=SimpleNamespace(runtime_root=self.runtime)),
            patch.object(controller, "find_active_lane", return_value=("epoch-1", lane)),
            patch.object(controller, "read_record", side_effect=[invocation, {}]),
            patch.object(controller, "_write_status") as status,
            patch.object(controller, "_append_event"),
            patch.object(controller.processes, "process_identity", return_value={"pid": 41, "creation_time": "created-1"}),
            patch.object(controller, "update_lane", return_value=lane),
            patch.object(controller, "_load_binding", return_value=object()),
            patch.object(controller, "acquire_leases"),
            patch.object(controller, "_run_provider") as provider,
            patch.object(controller, "release_leases") as release,
        ):
            self.assertEqual(4, controller.run_controller("lane-1"))
        provider.assert_not_called()
        terminal = status.call_args_list[-1].args[1]
        self.assertTrue(terminal["cleanup_proven"])
        self.assertEqual({"state": "not_started"}, terminal["provider_state"])
        release.assert_called_once_with(self.runtime, "lane-1", "run-1")
        from orchestrator_harness.lane_cleanup import cleanup_is_proven
        atomic_write_json(Path(lane["controller_status_path"]), {
            "schema": "controller-status/v1", "lane_id": "lane-1", "run_id": "run-1",
            "controller_identity": {"pid": 41, "creation_time": "2026-10-11T06:00:00Z"}, **terminal,
        })
        with patch.object(controller.processes, "identity_exit_proven", return_value=True):
            self.assertTrue(cleanup_is_proven(lane))


if __name__ == "__main__":
    unittest.main()
