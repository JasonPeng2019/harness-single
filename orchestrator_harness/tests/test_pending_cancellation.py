"""Never-launched cancellation, exact ownership, and public-command closure."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from orchestrator_harness import launch, processes, root_hook_dispatch, setup, shutdown
from orchestrator_harness.config import HarnessConfig, ResourceManifest
from orchestrator_harness.epochs import open_epoch, read_current_epoch, read_epoch_state, write_active_lanes
from orchestrator_harness.lane_cleanup import observe_controller_status
from orchestrator_harness.lanes import LaneError, read_lane, update_lane, write_lane
from orchestrator_harness.leases import acquire_leases, lease_path
from orchestrator_harness.records import atomic_write_json


class CancellationFixture(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        self.config = HarnessConfig(self.root, self.workspace, "enabled")
        self.rt = self.config.runtime_root
        self.epoch = open_epoch(self.rt, self.config, ResourceManifest())['epoch_id']
        atomic_write_json(setup.runtime_state_path(self.rt), {"schema": "runtime-state/v1", "state": "OPEN"})
        self.worktree = self.root / "worktree"
        (self.worktree / ".agent-workspace").mkdir(parents=True)
        self.status_path = self.worktree / ".agent-workspace" / "controller.status.json"
        self.lane = write_lane(self.rt, self.epoch, "lane", {
            "run_id": "resumed-run", "lifecycle": "running", "launch_pending": True,
            "process": {}, "worktree_path": str(self.worktree),
            "controller_status_path": str(self.status_path),
            "controller_events_path": str(self.worktree / ".agent-workspace" / "controller.events.jsonl"),
        })
        write_active_lanes(self.rt, self.epoch, [{"lane_id": "lane"}])

    def force_stop(self) -> dict:
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(launch, "load_config", return_value=self.config),
            patch.object(launch, "_prune_worktrees"),
        ):
            return launch.run_force_stop("lane")

    def lease(self, resource: str, run_id: str) -> Path:
        acquire_leases(self.rt, [resource], lane_id="lane", run_id=run_id, pid=123, creation_time="fixture")
        return lease_path(self.rt, resource)


class PendingCancellationTests(CancellationFixture):
    def test_force_stop_retires_resumed_pending_and_releases_exact_owner(self) -> None:
        own = self.lease("own", "resumed-run")
        other = self.lease("other", "newer-run")
        with patch.object(processes, "terminate_process") as terminate:
            result = self.force_stop()
        self.assertTrue(result["ok"], result)
        self.assertEqual("retired", read_lane(self.rt, self.epoch, "lane")["lifecycle"])
        self.assertFalse(own.exists())
        self.assertTrue(other.exists())
        terminate.assert_not_called()

    def test_same_proof_rejects_unknown_launches_and_process_records(self) -> None:
        for changes in (
            {"launch_pending": False}, {"launch_pending": "true"},
            {"process": None}, {"process": {"pid": None}},
            {"process": {"pid": 42, "creation_time": "created"}},
        ):
            with self.subTest(changes=changes), patch.object(processes, "identity_matches", return_value=False):
                lane = {**self.lane, **changes}
                self.assertFalse(shutdown._lane_clean(self.rt, lane))
                self.assertEqual(launch.FORCE_STOP_CLEANUP_UNPROVEN, launch._cleanup_lane_processes(lane)[1])

    def test_invalid_status_is_never_absence(self) -> None:
        for content in ("not-json", json.dumps({"schema": "controller-status/v1", "lane_id": "lane", "run_id": "old"})):
            self.status_path.write_text(content, encoding="utf-8")
            self.assertEqual("invalid", observe_controller_status(self.lane).kind)
            self.assertFalse(shutdown._lane_clean(self.rt, self.lane))
            self.assertEqual(launch.FORCE_STOP_CLEANUP_UNPROVEN, self.force_stop()["code"])
        self.status_path.unlink()
        self.status_path.mkdir()
        self.assertEqual("invalid", observe_controller_status(self.lane).kind)
        self.assertFalse(launch._terminate_lane_processes(self.lane))

    def test_unreadable_status_fails_closed(self) -> None:
        with patch.object(Path, "lstat", side_effect=PermissionError("denied")):
            self.assertEqual("unreadable", observe_controller_status(self.lane).kind)
            self.assertFalse(shutdown._lane_clean(self.rt, self.lane))
            self.assertFalse(launch._terminate_lane_processes(self.lane))

    def test_status_identity_is_checked_even_with_empty_lane_process(self) -> None:
        atomic_write_json(self.status_path, {
            "schema": "controller-status/v1", "lane_id": "lane", "run_id": "resumed-run",
            "controller_identity": {"pid": 42, "creation_time": "created"},
            "controller_state": "exited", "provider_state": {"state": "not_started"}, "cleanup_proven": True,
        })
        with (
            patch.object(processes, "identity_matches", return_value=True),
            patch.object(processes, "terminate_process", return_value=False),
        ):
            self.assertFalse(shutdown._lane_clean(self.rt, self.lane))
            self.assertEqual(launch.FORCE_STOP_PROCESS_SURVIVED, self.force_stop()["code"])
        with (
            patch.object(processes, "identity_matches", return_value=False),
            patch.object(processes, "terminate_process", return_value=False),
        ):
            self.assertEqual(launch.FORCE_STOP_CLEANUP_UNPROVEN, self.force_stop()["code"])

    def test_stale_force_stop_preserves_newer_lane_and_lease(self) -> None:
        lease = self.lease("newer", "newer-run")
        write_lane(self.rt, self.epoch, "lane", {**self.lane, "run_id": "newer-run"})
        with patch.object(launch, "find_active_lane", return_value=(self.epoch, self.lane)):
            result = self.force_stop()
        self.assertEqual(launch.FORCE_STOP_OWNERSHIP_CHANGED, result["code"])
        current = read_lane(self.rt, self.epoch, "lane")
        self.assertEqual("newer-run", current["run_id"])
        self.assertEqual("running", current["lifecycle"])
        self.assertTrue(lease.exists())

    def test_stale_shutdown_retirement_preserves_newer_lane_and_lease(self) -> None:
        lease = self.lease("newer", "newer-run")
        write_lane(self.rt, self.epoch, "lane", {**self.lane, "run_id": "newer-run"})
        with self.assertRaises(LaneError):
            shutdown._retire_lane(self.rt, self.epoch, "lane", self.lane)
        self.assertTrue(lease.exists())
        self.assertEqual("running", read_lane(self.rt, self.epoch, "lane")["lifecycle"])

    def test_expired_allowance_has_no_retirement_or_lease_effect(self) -> None:
        lease = self.lease("own", "resumed-run")
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(launch, "load_config", return_value=self.config),
        ):
            result = launch.run_force_stop("lane", allowance_seconds=0)
        self.assertEqual(launch.FORCE_STOP_ALLOWANCE_EXPIRED, result["code"])
        self.assertTrue(lease.exists())
        self.assertEqual("running", read_lane(self.rt, self.epoch, "lane")["lifecycle"])

    def test_epoch_change_prevents_cancellation_effects(self) -> None:
        lease = self.lease("own", "resumed-run")
        atomic_write_json(self.rt / "CURRENT_EPOCH.json", {"schema": "current-epoch/v1", "epoch_id": "new-epoch"})
        with (
            patch.object(launch, "find_active_lane", return_value=(self.epoch, self.lane)),
            patch.object(launch, "_cleanup_lane_processes") as cleanup,
        ):
            result = self.force_stop()
        self.assertEqual(launch.FORCE_STOP_OWNERSHIP_CHANGED, result["code"])
        cleanup.assert_not_called()
        self.assertTrue(lease.exists())

    def test_lease_delete_failure_keeps_invocation_visible(self) -> None:
        lease = self.lease("own", "resumed-run")
        with patch.object(Path, "unlink", side_effect=PermissionError("lease is locked")):
            result = self.force_stop()
        self.assertEqual(launch.FORCE_STOP_LEASE_RELEASE_FAILED, result["code"])
        self.assertTrue(lease.exists())
        self.assertEqual("running", read_lane(self.rt, self.epoch, "lane")["lifecycle"])

    def test_changed_cleanup_proof_prevents_shutdown_retirement(self) -> None:
        lease = self.lease("own", "resumed-run")
        self.status_path.write_text("invalid", encoding="utf-8")
        with self.assertRaises(shutdown.ShutdownError):
            shutdown._retire_lane(self.rt, self.epoch, "lane", self.lane)
        self.assertTrue(lease.exists())
        self.assertEqual("running", read_lane(self.rt, self.epoch, "lane")["lifecycle"])


class FailedStartRecoveryTests(CancellationFixture):
    def setUp(self) -> None:
        super().setUp()
        self.invocation = {
            "schema": "controller-invocation/v1", "lane_id": "lane", "run_id": "resumed-run",
            "provider": {"id": "codex", "model": "fixture", "launch_config": {}},
        }
        atomic_write_json(self.worktree / ".agent-workspace" / "invocation.json", self.invocation)
        binding = self.root / "orchestrator_harness" / "provider_adapters" / "codex" / "launcher_binding.py"
        binding.parent.mkdir(parents=True)
        binding.write_text("# fixture", encoding="utf-8")
        for target, name, value in (
            (launch, "find_harness_root", self.root), (launch, "load_config", self.config),
            (launch, "_validate_provider_launch_config", {}),
        ):
            patcher = patch.object(target, name, return_value=value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_failed_native_spawn_has_durable_no_controller_proof(self) -> None:
        with patch.object(processes, "spawn_detached", side_effect=OSError("spawn refused")):
            result = launch.run_launch("lane")
        self.assertFalse(result["ok"])
        lane = read_lane(self.rt, self.epoch, "lane")
        self.assertIs(False, lane["launch_pending"])
        self.assertIs(True, lane["launch_failure"]["controller_not_created"])
        self.assertTrue(shutdown._lane_clean(self.rt, lane))
        self.assertTrue(self.force_stop()["ok"])

    def test_stale_launch_refuses_before_spawn(self) -> None:
        def replace_before_effect(*args):
            write_lane(self.rt, self.epoch, "lane", {**self.lane, "run_id": "newer-run"})
            return update_lane(*args)

        with (
            patch.object(launch, "update_lane", side_effect=replace_before_effect),
            patch.object(processes, "spawn_detached") as spawn,
        ):
            self.assertFalse(launch.run_launch("lane")["ok"])
        spawn.assert_not_called()
        current = read_lane(self.rt, self.epoch, "lane")
        self.assertEqual("newer-run", current["run_id"])
        self.assertIs(True, current["launch_pending"])

    def test_prepared_label_does_not_allow_duplicate_live_controller(self) -> None:
        write_lane(self.rt, self.epoch, "lane", {**self.lane, "lifecycle": "prepared"})
        atomic_write_json(self.status_path, {
            "schema": "controller-status/v1", "lane_id": "lane", "run_id": "resumed-run",
            "controller_identity": {"pid": 42, "creation_time": "created"},
            "controller_state": "exited", "provider_state": {"state": "not_started"}, "cleanup_proven": True,
        })
        with (
            patch.object(processes, "identity_matches", return_value=True),
            patch.object(processes, "spawn_detached") as spawn,
        ):
            self.assertFalse(launch.run_launch("lane")["ok"])
        spawn.assert_not_called()

    def test_unrecordable_spawn_is_unknown_and_not_a_pending_launch(self) -> None:
        child = MagicMock(pid=42)
        with (
            patch.object(processes, "spawn_detached", return_value=child),
            patch.object(processes, "process_identity", return_value=None),
        ):
            self.assertFalse(launch.run_launch("lane")["ok"])
        lane = read_lane(self.rt, self.epoch, "lane")
        self.assertIs(False, lane["launch_pending"])
        self.assertIs(False, lane["launch_failure"]["controller_not_created"])
        self.assertFalse(shutdown._lane_clean(self.rt, lane))
        self.assertEqual(launch.FORCE_STOP_CLEANUP_UNPROVEN, self.force_stop()["code"])
        child.terminate.assert_not_called()

    def test_failed_registration_records_exact_reap_without_inventing_provider_cleanup(self) -> None:
        for terminal in (False, True):
            with self.subTest(terminal=terminal):
                write_lane(self.rt, self.epoch, "lane", self.lane)
                child = MagicMock(pid=42)
                calls = 0

                def fail_registration(*args):
                    nonlocal calls
                    calls += 1
                    if calls == 2:
                        if terminal:
                            atomic_write_json(self.status_path, {
                                "schema": "controller-status/v1", "lane_id": "lane", "run_id": "resumed-run",
                                "controller_state": "exited", "provider_state": {"state": "not_started"}, "cleanup_proven": True,
                            })
                        raise OSError("identity registration failed")
                    return update_lane(*args)

                with (
                    patch.object(launch, "update_lane", side_effect=fail_registration),
                    patch.object(processes, "spawn_detached", return_value=child),
                    patch.object(processes, "process_identity", return_value={"pid": 42, "creation_time": "created"}),
                    patch.object(processes, "terminate_process", return_value=True),
                    patch.object(processes, "identity_matches", return_value=False),
                    patch.object(processes, "process_alive", return_value=False),
                ):
                    self.assertFalse(launch.run_launch("lane")["ok"])
                    lane = read_lane(self.rt, self.epoch, "lane")
                    self.assertTrue(lane["launch_failure"]["controller_reaped"])
                    self.assertEqual(terminal, shutdown._lane_clean(self.rt, lane))
                child.wait.assert_called_once_with(timeout=10.0)


class PublicPendingShutdownTests(unittest.TestCase):
    """Real CLI processes and monitor; fixture session history, no real provider."""

    def test_public_resume_then_shutdown_and_force_stop_then_shutdown(self) -> None:
        source = Path(__file__).resolve().parents[2]
        (source / "runtime").mkdir(exist_ok=True)
        for mode in ("resume-shutdown", "resume-force-stop", "initial-prepared"):
            # Keep native Git paths short on Windows while staying in version-b.
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(prefix="p", dir=source / "runtime") as raw:
                root = Path(raw)
                harness = root / "local-config" / "harness"
                workspace = root / "runtime" / "project"
                harness.mkdir(parents=True)
                workspace.mkdir(parents=True)
                for name in ("adapters", "super-cache", "orchestrator_harness", "harness_common"):
                    shutil.copytree(source / name, harness / name, ignore=shutil.ignore_patterns("__pycache__"))
                atomic_write_json(harness / "harness-config.json", {"root_workspace": str(workspace), "managed_coordination": "enabled"})
                atomic_write_json(harness / "resource-manifest.json", {"schema": "resource-manifest/v1", "resources": [{"id": "shared", "exclusive": True}]})
                subprocess.run(["git", "init", str(workspace)], check=True, capture_output=True)
                subprocess.run(["git", "-C", str(workspace), "config", "core.longpaths", "true"], check=True, capture_output=True)
                (workspace / "example.txt").write_text("fixture\n", encoding="utf-8")
                subprocess.run(["git", "-C", str(workspace), "add", "example.txt"], check=True, capture_output=True)
                subprocess.run(["git", "-C", str(workspace), "-c", "user.name=Harness Test", "-c", "user.email=harness@example.invalid", "commit", "-m", "fixture"], check=True, capture_output=True)
                card = harness / "task.json"
                atomic_write_json(card, {
                    "schema": "project-task-card/v1", "task": "Exercise pending cancellation",
                    "acceptance_criteria": ["Runtime closes"], "deliverables": ["Cleanup evidence"],
                    "reason_for_acceptance_and_deliverables": "Prove public cleanup of an unlaunched invocation",
                })
                rt = workspace / ".harness-runtime"
                env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}

                def public(*args: str) -> dict:
                    completed = subprocess.run([sys.executable, "-B", "-m", "orchestrator_harness.operator_launch", "--json", *args], cwd=harness, env=env, capture_output=True, text=True, timeout=45)
                    self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
                    return json.loads(completed.stdout)

                monitor = None
                try:
                    public("harness", "setup")
                    monitor = setup.read_monitor_record(rt)
                    self.assertIsNotNone(monitor)
                    public("lane", "bootstrap", "--lane-id", "pending", "--provider", "codex", "--model", "fixture-model", "--provider-option", "reasoning_effort=high", "--provider-option", "service_tier=priority", "--exclusive-resource", "shared", "--task-card", str(card))
                    epoch = read_current_epoch(rt)["epoch_id"]
                    lane = read_lane(rt, epoch, "pending")
                    if mode != "initial-prepared":
                        # A stopped fixture with native session metadata is enough
                        # to exercise resume's real file preparation and CLI route.
                        lane = update_lane(rt, epoch, "pending", lambda current: {**current, "lifecycle": "blocked", "session": {"session_id": "fixture-session"}})
                        prior_run = lane["run_id"]
                        public("resume-lane", "--lane-id", "pending", "--resume-task-card", str(card))
                        lane = read_lane(rt, epoch, "pending")
                        self.assertNotEqual(prior_run, lane["run_id"])
                        self.assertIs(True, lane["launch_pending"])
                        self.assertEqual({}, lane["process"])
                        self.assertFalse(Path(lane["controller_status_path"]).exists())
                    acquire_leases(rt, ["shared"], lane_id="pending", run_id=lane["run_id"], pid=123, creation_time="fixture")
                    if mode == "resume-force-stop":
                        public("lane", "force-stop", "--lane-id", "pending")
                    public("harness", "shutdown")
                    self.assertEqual("retired", read_lane(rt, epoch, "pending")["lifecycle"])
                    self.assertFalse(lease_path(rt, "shared").exists())
                    self.assertIsNone(read_current_epoch(rt))
                    self.assertEqual("closed", read_epoch_state(rt, epoch)["lifecycle"])
                    self.assertEqual("CLOSED", setup.read_runtime_state(rt)["state"])
                    self.assertEqual("STOPPED", setup.read_monitor_record(rt)["health"])
                    deadline = time.monotonic() + 5
                    while processes.identity_matches(monitor["pid"], monitor["creation_time"]) and time.monotonic() < deadline:
                        time.sleep(0.05)
                    self.assertFalse(processes.identity_matches(monitor["pid"], monitor["creation_time"]))
                    self.assertEqual({"decision": "ALLOW"}, root_hook_dispatch.dispatch(harness, "stop", "codex"))
                finally:
                    if monitor is not None and processes.identity_matches(monitor["pid"], monitor["creation_time"]):
                        # Test failure cleanup still targets only this fixture's
                        # recorded PID plus creation identity.
                        shutdown._stop_monitor(rt)


if __name__ == "__main__":
    unittest.main()
