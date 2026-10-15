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

from orchestrator_harness import lane_cleanup, launch, processes, root_hook_dispatch, setup, shutdown
from orchestrator_harness.config import HarnessConfig, ResourceManifest
from orchestrator_harness.epochs import open_epoch, read_current_epoch, read_epoch_state, write_active_lanes
from orchestrator_harness.lane_cleanup import observe_controller_status
from orchestrator_harness.lanes import LaneError, read_lane, update_lane, write_lane
from orchestrator_harness.leases import acquire_leases, lease_path
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.tests.support import exited_lease_holder_fixture, prepare_lane_fixture


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
        self.worktree = self.workspace / "worktree"
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
        exited_lease_holder_fixture(self.rt, [resource], lane_id="lane", run_id=run_id)
        return lease_path(self.rt, resource)


class PendingCancellationTests(CancellationFixture):
    def test_cleanup_reference_is_durable_before_lease_release(self) -> None:
        actual_release = launch.force_release_leases
        records = []
        def checked_release(rt, lane_id, run_id):
            current = read_lane(rt, self.epoch, lane_id)
            self.assertTrue(lane_cleanup.cleanup_is_proven(current))
            ref = current["forced_cleanup_ref"]
            record = json.loads(Path(ref["path"]).read_bytes())
            self.assertEqual(run_id, record["run_id"])
            self.assertEqual("forced_cancellation", record["kind"])
            self.assertEqual("never_launched", record["observations"]["kind"])
            self.assertIsNone(record["controller"])
            self.assertIsNone(record["provider"])
            self.assertIsNone(record["boundary"])
            records.append(ref)
            actual_release(rt, lane_id, run_id)
        with patch.object(launch, "force_release_leases", side_effect=checked_release):
            result = self.force_stop()
        self.assertTrue(result["ok"], result)
        retired = read_lane(self.rt, self.epoch, "lane")
        self.assertEqual([retired["forced_cleanup_ref"]], records)
        self.assertTrue(lane_cleanup.cleanup_is_proven(retired))
        self.assertFalse(self.status_path.exists())

    def test_failed_lease_release_retains_proof_for_exact_explicit_retry(self) -> None:
        own = self.lease("own", self.lane["run_id"])
        with patch.object(launch, "force_release_leases", side_effect=OSError("interrupted release")):
            result = self.force_stop()
        self.assertEqual(launch.FORCE_STOP_LEASE_RELEASE_FAILED, result["code"])
        current = read_lane(self.rt, self.epoch, "lane")
        self.assertEqual("running", current["lifecycle"])
        self.assertTrue(own.exists())
        self.assertTrue(lane_cleanup.cleanup_is_proven(current))
        ref = current["forced_cleanup_ref"]
        original = Path(ref["path"]).read_bytes()
        self.assertTrue(self.force_stop()["ok"])
        retired = read_lane(self.rt, self.epoch, "lane")
        self.assertEqual(ref, retired["forced_cleanup_ref"])
        self.assertEqual(original, Path(ref["path"]).read_bytes())
        self.assertFalse(own.exists())
        self.assertTrue(lane_cleanup.cleanup_is_proven(retired))

    def test_byte_identical_foreign_cleanup_record_cannot_reuse_proof(self) -> None:
        self.assertTrue(self.force_stop()["ok"])
        retired = read_lane(self.rt, self.epoch, "lane")
        path = Path(retired["forced_cleanup_ref"]["path"])
        raw = path.read_bytes()
        retained = path.with_name("original-cleanup-retained.json")
        path.rename(retained)
        path.write_bytes(raw)
        self.assertFalse(lane_cleanup.cleanup_is_proven(retired))
        self.assertEqual(raw, retained.read_bytes())
        self.assertEqual(raw, path.read_bytes())

    def test_unknown_cleanup_reference_cannot_fall_back_to_never_launched(self) -> None:
        lane = {**self.lane, "forced_cleanup_ref": {"path": str(self.root / "unknown"),
                                                   "content_hash": "unknown", "native_identity": "unknown"}}
        self.assertFalse(lane_cleanup.cleanup_is_proven(lane))
        self.assertEqual(launch.FORCE_STOP_CLEANUP_UNPROVEN, launch._cleanup_lane_processes(lane)[1])

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
            "controller_identity": {"pid": 42, "creation_time": "2026-10-11T06:00:00Z"},
            "controller_state": "exited", "provider_state": {"state": "not_started"}, "cleanup_proven": True,
        })
        with (
            patch.object(processes, "identity_matches", return_value=True),
            patch.object(processes, "identity_exit_proven", return_value=False),
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
        from orchestrator_harness.tests.support import seed_provider_history
        self.lane["provider"] = self.invocation["provider"]
        self.lane = prepare_lane_fixture(self.rt, self.epoch, self.lane)
        self.status_path = Path(self.lane["controller_status_path"])
        seed_provider_history(self.rt, self.lane)
        write_lane(self.rt, self.epoch, "lane", self.lane)
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

    def test_fast_provider_exit_is_a_successful_launch_only_with_cleanup_proof(self) -> None:
        for cleanup in (True, False):
            with self.subTest(cleanup=cleanup):
                write_lane(self.rt, self.epoch, "lane", self.lane)
                child = MagicMock(pid=os.getpid())
                child.poll.return_value = 0
                status = {
                    "controller_state": "exited", "provider_state": {"state": "exited"},
                    "cleanup_proven": cleanup, "recorded_status": "provider_exited_no_result",
                }
                with (patch.object(processes, "spawn_detached", return_value=child),
                      patch.object(launch, "_read_controller_status", return_value=status)):
                    result = launch.run_launch("lane")
                self.assertEqual(cleanup, result["ok"], result)
                if cleanup:
                    self.assertEqual("LAUNCH_OK", result["code"])
                    self.assertIn("provider_exited_no_result", result["summary"])
                child.terminate.assert_not_called()

    def test_failed_registration_records_exact_reap_without_inventing_provider_cleanup(self) -> None:
        for terminal in (False, True):
            with self.subTest(terminal=terminal):
                write_lane(self.rt, self.epoch, "lane", self.lane)
                with (open(os.devnull, "r") as stdin, open(os.devnull, "w") as stdout,
                      open(os.devnull, "w") as stderr):
                    child = processes.spawn_provider([sys.executable, "-B", "-c", "import time; time.sleep(60)"],
                                                     cwd=self.workspace, stdin=stdin,
                                                     stdout=stdout, stderr=stderr,
                                                     env=dict(os.environ))
                job = child.take_job_handle() if os.name == "nt" else None
                boundary = processes.ProcessBoundary.for_process(child.pid, windows_job_handle=job)
                self.assertTrue(boundary.observe())
                self.assertIsNotNone(boundary.root_creation_time)
                identity = {"pid": child.pid, "creation_time": boundary.root_creation_time}
                if os.name == "nt":
                    child.resume()
                calls = 0
                boundary_cleaned = False

                def fail_registration(*args):
                    nonlocal calls, boundary_cleaned
                    calls += 1
                    if calls == 2:
                        if terminal:
                            self.assertTrue(boundary.cleanup(force=True))
                            boundary_cleaned = True
                            atomic_write_json(self.status_path, {
                                "schema": "controller-status/v1", "lane_id": "lane", "run_id": "resumed-run",
                                "controller_identity": identity,
                                "controller_state": "exited", "provider_state": {"state": "not_started"}, "cleanup_proven": True,
                            })
                        raise OSError("identity registration failed")
                    return update_lane(*args)

                try:
                    with (patch.object(launch, "update_lane", side_effect=fail_registration),
                          patch.object(processes, "spawn_detached", return_value=child),
                          patch.object(child, "wait", wraps=child.wait) as original_wait):
                        self.assertFalse(launch.run_launch("lane")["ok"])
                        lane = read_lane(self.rt, self.epoch, "lane")
                        self.assertTrue(lane["launch_failure"]["controller_reaped"])
                        self.assertEqual(terminal, shutdown._lane_clean(self.rt, lane))
                        original_wait.assert_called_once_with(timeout=10.0)
                finally:
                    # The terminal branch already consumed/closed the Job.
                    # Do not repeat its cleanup through global process discovery.
                    if not boundary_cleaned:
                        self.assertTrue(boundary.cleanup(force=True), boundary.errors)
                    self.assertTrue(processes.process_boundary_is_gone(boundary.record()))
                    self.assertTrue(processes.reap_created_process(child))
                    close = getattr(child, "close", None)
                    if close is not None:
                        close()

    def test_early_controller_failure_retains_stderr_for_public_diagnosis(self) -> None:
        children = []
        original_spawn = processes.spawn_detached

        def spawn(*args, **kwargs):
            child = original_spawn(*args, **kwargs)
            children.append(child)
            return child

        try:
            with (patch.object(processes, "spawn_detached", side_effect=spawn),
                  patch.object(processes, "python_argv", return_value=[sys.executable, "-B", "-c",
                      "import sys,time; time.sleep(0.3); sys.stderr.write('fixture controller import failed\\n'); sys.exit(1)"])):
                result = launch.run_launch("lane")
            self.assertFalse(result["ok"], result)
            self.assertEqual(launch.LAUNCH_CONTROLLER_START_FAILED, result["code"])
            stderr = self.status_path.with_name("controller-stderr.txt")
            self.assertIn(str(stderr), result["evidence_paths"])
            self.assertIn("fixture controller import failed", stderr.read_text(encoding="utf-8"))
        finally:
            for child in children:
                # Original handles remain authoritative even if registration failed.
                if child.poll() is None:
                    child.terminate()
                self.assertTrue(processes.reap_created_process(child))


class PublicPendingShutdownTests(unittest.TestCase):
    """Public CLIs, controller/provider processes and optional service in owned fixtures."""

    def test_public_resume_then_shutdown_and_force_stop_then_shutdown(self, *,
            modes: tuple[str, ...] = ("resume-shutdown", "resume-force-stop", "initial-prepared", "retry-limit")) -> None:
        source = Path(__file__).resolve().parents[2]
        for mode, enabled in ((mode, enabled) for mode in modes
                for enabled in (False, True)):
            with self.subTest(mode=mode, service=enabled), tempfile.TemporaryDirectory(prefix="p") as raw:
                root = Path(raw)
                harness = root / "local-config" / "harness"
                workspace = root / "runtime" / "project"
                harness.mkdir(parents=True)
                workspace.mkdir(parents=True)
                for name in ("adapters", "super-cache", "orchestrator_harness", "harness_common", "tools", "scripts"):
                    shutil.copytree(source / name, harness / name, ignore=shutil.ignore_patterns("__pycache__"))
                # The copied binding runs an owned deterministic native process,
                # retaining real launch/controller/session/receipt boundaries.
                binding = harness / "orchestrator_harness/provider_adapters/codex/launcher_binding.py"
                with binding.open("a", encoding="utf-8") as stream:
                    stream.write('\ndef build_argv(**kwargs):\n    import sys\n'
                                 '    return [sys.executable, "-B", str(Path(__file__).resolve().parents[3] / "fixture-provider.py"), kwargs.get("session_id") or "fixture-session"]\n')
                shutil.copyfile(binding, harness / "adapters/codex/harness/launcher_binding.py")
                (harness / "fixture-provider.py").write_text(
                    'import json, sys\nfrom pathlib import Path\n'
                    'Path("RESULT.json").write_text("invalid result")\n'
                    'print(json.dumps({"type": "thread.started", "thread_id": sys.argv[1]}), flush=True)\n'
                    'print(json.dumps({"type": "turn.completed", "usage": {"input_tokens": 1, "output_tokens": 1}}), flush=True)\n', encoding="utf-8")
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

                def startup_diagnostics() -> str:
                    diagnostics = []
                    for path in (rt / "epochs").glob("*/lanes/pending/lane.json"):
                        try:
                            current = json.loads(path.read_text(encoding="utf-8"))
                            diagnostics.append({"lane": str(path), "launch_failure": current.get("launch_failure")})
                            status = Path(current["controller_status_path"])
                            paths = {status, Path(current["controller_events_path"]),
                                     status.with_name("controller-stderr.txt")}
                            paths.update(status.parent.rglob("provider-stderr.txt"))
                            for evidence in sorted(paths):
                                if evidence.is_file():
                                    diagnostics.append({"path": str(evidence),
                                                        "tail": evidence.read_text(encoding="utf-8", errors="replace")[-8000:]})
                        except (OSError, ValueError, KeyError) as error:
                            diagnostics.append({"path": str(path), "diagnostic_error": str(error)})
                    return "PUBLIC_PENDING_STARTUP_DIAGNOSTICS " + json.dumps(diagnostics)

                def public(*args: str, success: bool = True) -> dict:
                    # A full-catalog Windows setup took 68.609s and bootstrap
                    # took 61.297s in a disposable Git project. Allow both
                    # startup commands 120s; ordinary commands retain 45s.
                    setup_command = args == ("harness", "setup")
                    bootstrap_command = args[:2] == ("lane", "bootstrap")
                    started = time.monotonic()
                    completed = subprocess.run([sys.executable, "-B", "-m", "orchestrator_harness.operator_launch", "--json", *args], cwd=harness, env=env, capture_output=True, text=True, timeout=120 if setup_command or bootstrap_command else 45)
                    if setup_command or bootstrap_command:
                        print(("PUBLIC_PENDING_SETUP_TIMING " if setup_command else "PUBLIC_PENDING_BOOTSTRAP_TIMING ") + json.dumps({"mode": mode, "service": enabled,
                              "elapsed_seconds": round(time.monotonic() - started, 3), "exit_code": completed.returncode}), flush=True)
                    self.assertEqual(success, completed.returncode == 0,
                                     completed.stdout + completed.stderr + "\n" +
                                     (startup_diagnostics() if success != (completed.returncode == 0) else ""))
                    return json.loads(completed.stdout)

                def queue_snapshot() -> dict:
                    from orchestrator_harness.epochs import manager_queue_path
                    from orchestrator_harness.manager_queue import read_manager_queue
                    from orchestrator_harness.records import RecordLock, runtime_admission_lock
                    with runtime_admission_lock(rt), RecordLock(manager_queue_path(rt)):
                        return read_manager_queue(rt)

                def wait_for_exit(lane):
                    deadline = time.monotonic() + 35
                    while time.monotonic() < deadline:
                        status = observe_controller_status(lane).status
                        try:
                            current = read_lane(rt, epoch, "pending")
                        except LaneError as exc:
                            if not isinstance(exc.__cause__, PermissionError):
                                raise
                            time.sleep(0.1)  # Windows may be replacing this owned record.
                            continue
                        if (status and status.get("controller_state") == "exited"
                                and status.get("cleanup_proven") is True
                                and not processes.identity_matches(current["process"]["pid"], current["process"]["creation_time"])):
                            self.assertEqual("provider_exited_no_result", status["recorded_status"])
                            self.assertEqual("fixture-session", current["session"]["session_id"])
                            self.assertFalse(lease_path(rt, "shared").exists())
                            return current
                        time.sleep(0.1)
                    self.fail("owned controller/provider did not complete with proven cleanup\n" + startup_diagnostics())

                monitor = None
                try:
                    public("harness", "setup")
                    monitor = setup.read_monitor_record(rt)
                    self.assertIsNotNone(monitor)
                    from orchestrator_harness.config import load_config
                    from orchestrator_harness.root_session import publish_root_session
                    root_session = publish_root_session(load_config(harness), run_id="pending-public-fixture", provider_id="codex")
                    env["HARNESS_ROOT_SESSION_ID"] = root_session["session_id"]
                    env["HARNESS_BINDING_ID"] = root_session["provider_id"]
                    if enabled:
                        import hashlib
                        from orchestrator_harness.tests.test_public_checks import ADAPTER_SOURCE
                        adapter = harness / "fixture-adapter.py"
                        adapter.write_text(ADAPTER_SOURCE, encoding="utf-8")
                        atomic_write_json(harness / "local-config/public-check-policy.json", {
                            "schema": "public-check-policy/v2", "adapter": {"path": str(adapter), "sha256": hashlib.sha256(adapter.read_bytes()).hexdigest()},
                            "input_paths": ["example.txt"], "optional_input_paths": [], "fixed_paths": [],
                            "commands": {key: [sys.executable, "-c", "print('public fixture')"] for key in ("build", "public")},
                            "allow_focused": False, "timeout_seconds": 10, "max_parallel_checks": 1,
                        })
                        started = subprocess.run([sys.executable, "-B", "-c",
                            'from pathlib import Path; from orchestrator_harness.config import load_config; from orchestrator_harness.public_checks import start_service; h=Path.cwd(); start_service(h, load_config(h).root_workspace)'],
                            cwd=harness, env=env, capture_output=True, text=True, timeout=35)
                        self.assertEqual(0, started.returncode, started.stderr)
                    public("lane", "bootstrap", "--lane-id", "pending", "--provider", "codex", "--model", "fixture-model", "--provider-option", "reasoning_effort=high", "--provider-option", "service_tier=priority", "--provider-option", "permission_mode=yolo", "--exclusive-resource", "shared", "--task-card", str(card))
                    epoch = read_current_epoch(rt)["epoch_id"]
                    lane = read_lane(rt, epoch, "pending")
                    if mode != "initial-prepared":
                        public("lane", "launch", "--lane-id", "pending")
                        lane = wait_for_exit(lane)
                        from orchestrator_harness.retry_policy import completed_provider_invocations
                        self.assertEqual(1, completed_provider_invocations(lane, rt))
                        prior_run = lane["run_id"]
                        public("resume-lane", "--lane-id", "pending", "--resume-task-card", str(card))
                        lane = read_lane(rt, epoch, "pending")
                        self.assertNotEqual(prior_run, lane["run_id"])
                        self.assertIs(True, lane["launch_pending"])
                        self.assertEqual({}, lane["process"])
                        self.assertFalse(Path(lane["controller_status_path"]).exists())
                        if mode == "retry-limit":
                            public("lane", "launch", "--lane-id", "pending")
                            lane = wait_for_exit(lane)
                            self.assertEqual(2, completed_provider_invocations(lane, rt))
                            evidence = [Path(lane["provider_history_path"]), rt / "epochs" / epoch / "lanes/pending/lane.json"]
                            evidence += list((rt / "operator-only/public-check-service/routes").glob("*.json"))
                            before = {path: path.read_bytes() for path in evidence}
                            refused = public("resume-lane", "--lane-id", "pending", "--resume-task-card", str(card), success=False)
                            self.assertEqual("WORKER_RETRY_LIMIT", refused["code"])
                            self.assertEqual(before, {path: path.read_bytes() for path in evidence})
                    # Retain a real, exactly identified dead holder. A fabricated
                    # PID/CID cannot establish native absence or exercise strict
                    # lease cleanup. The helper belongs only to this fixture.
                    holder = subprocess.Popen(
                        [sys.executable, "-B", "-c", "import sys; sys.stdin.buffer.read(1)"],
                        cwd=root, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL, env=env,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                    try:
                        holder_identity = processes.process_identity(holder.pid)
                        self.assertIsNotNone(holder_identity)
                        acquire_leases(rt, ["shared"], lane_id="pending", run_id=lane["run_id"],
                                       pid=holder.pid, creation_time=holder_identity["creation_time"])
                        holder.communicate(b"x", timeout=10)
                        self.assertTrue(processes.identity_exit_proven(holder.pid, holder_identity["creation_time"]))
                    finally:
                        if holder.poll() is None:
                            holder.terminate()
                            holder.wait(timeout=10)
                    if mode == "resume-force-stop":
                        public("lane", "force-stop", "--lane-id", "pending")
                    else:
                        # Admission is asynchronous. Wait for this deliberately
                        # orphaned lease's actual monitor event before taking the
                        # queue snapshot; an empty earlier snapshot is not proof
                        # that the next monitor pass has no fresh obligation.
                        from orchestrator_harness.monitor import PASS_INTERVAL_SECONDS
                        deadline = time.monotonic() + 2 * PASS_INTERVAL_SECONDS
                        while True:
                            public("watch", "--until-actionable", "--timeout", str(int(2 * PASS_INTERVAL_SECONDS)) + "s")
                            observed = queue_snapshot()
                            if any(event.get("dedup_key") == "orphan-lease:shared" for event in observed["events"]):
                                break
                            self.assertLess(time.monotonic(), deadline, "native monitor did not report the retained dead lease")
                            time.sleep(0.1)
                    queue = queue_snapshot()
                    for event in queue["events"]:
                        if event["state"] == "PENDING":
                            public("manager", "acknowledge", "--event-id", event["event_id"])
                        if event["state"] in ("PENDING", "ACKNOWLEDGED"):
                            public("manager", "close", "--event-id", event["event_id"], "--outcome", "BLOCKED", "--summary", "fixture cancellation is intentional")
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
                    if enabled:
                        from orchestrator_harness.public_checks import home, validate_service
                        stopped = validate_service(workspace, harness, runtime_closed=True)
                        self.assertEqual("STOPPED", stopped["status"])
                        self.assertTrue(json.loads((home(workspace) / "service.json").read_text())["cleanup_proven"])
                except BaseException:
                    print(startup_diagnostics(), flush=True)
                    # Optional owned validation output preserves the authority
                    # behind a failure before temporary-fixture cleanup removes
                    # it. This is test evidence, never a product recovery path.
                    record_name = os.environ.get("MERGE_VALIDATION_RECORD")
                    if record_name:
                        destination = Path(record_name).absolute().with_suffix(".fixtures") / f"{mode}-service-{enabled}-{root.name}"
                        destination.mkdir(parents=True, exist_ok=True)
                        try:
                            for name in ("epochs", "operator-only", "monitor", "resources", "manager"):
                                if (rt / name).is_dir():
                                    shutil.copytree(rt / name, destination / name)
                            for name in ("RUNTIME_STATE.json", "CURRENT_EPOCH.json"):
                                if (rt / name).is_file():
                                    shutil.copyfile(rt / name, destination / name)
                            from orchestrator_harness.lane_cleanup import cleanup_is_proven, observe_controller_status as observe_failure_status
                            from orchestrator_harness.retry_policy import current_invocation_facts
                            observations = []
                            for path in (rt / "epochs").glob("*/lanes/*/lane.json"):
                                current = json.loads(path.read_text(encoding="utf-8"))
                                status = observe_failure_status(current)
                                facts = current_invocation_facts(current)
                                boundaries = [fact.get("process_boundary") for fact in facts]
                                boundaries.append((status.status or {}).get("process_boundary"))
                                probes = []
                                for boundary in boundaries:
                                    if not isinstance(boundary, dict):
                                        continue
                                    members = []
                                    for identity in boundary.get("processes", []):
                                        pid, creation = identity.get("pid"), identity.get("creation_time")
                                        members.append({"recorded": identity, "current": processes.process_identity(pid),
                                                        "alive": processes.process_alive(pid),
                                                        "exit_proven": processes.identity_exit_proven(pid, creation)})
                                    probes.append({"record": boundary, "gone": processes.process_boundary_is_gone(boundary),
                                                   "members": members})
                                observations.append({"lane": current, "status": status.status,
                                                     "status_kind": status.kind, "facts": facts,
                                                     "cleanup_proven_now": cleanup_is_proven(current), "boundaries": probes})
                            atomic_write_json(destination / "cleanup-observations.json", {"observations": observations})
                        except Exception as capture_error:
                            atomic_write_json(destination / "capture-error.json", {"error": str(capture_error)})
                    raise
                finally:
                    if enabled:
                        from orchestrator_harness.public_checks import home, stop_service
                        if (home(workspace) / "service.json").is_file():
                            stop_service(workspace, harness)
                    if monitor is not None and processes.identity_matches(monitor["pid"], monitor["creation_time"]):
                        # Test failure cleanup still targets only this fixture's
                        # recorded PID plus creation identity.
                        shutdown._stop_monitor(rt)


if __name__ == "__main__":
    unittest.main()
