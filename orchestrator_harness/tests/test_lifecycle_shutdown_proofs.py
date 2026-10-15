"""Independent finite lifecycle probes. All fixtures belong to this role."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from orchestrator_harness import bootstrap, controller, lane_authority, processes, public_checks, shutdown
from orchestrator_harness.config import HarnessConfig, ResourceManifest
from orchestrator_harness.models import ProcessInfo, ProcessQuery, ProcessSnapshot
from orchestrator_harness.lanes import lane_operation_lock, write_lane, read_lane
from orchestrator_harness.epochs import write_active_lanes, open_epoch
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.tests.support import prepare_lane_fixture, seed_provider_history
from orchestrator_harness.windows_sandbox_preflight import BLOCK, require_not_blocked


class ProvenNoProvider(RuntimeError):
    no_provider_created = True


@unittest.skipUnless(sys.platform.startswith("linux"), "native Linux terminal-child observation")
class NativeLinuxTerminalChildTests(unittest.TestCase):
    def child(self):
        child = subprocess.Popen(
            [sys.executable, "-B", "-c", "import sys;sys.stdin.buffer.read(1)"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        self.addCleanup(self.reap, child)
        identity = processes.process_identity(child.pid)
        self.assertIsNotNone(identity)
        return child, identity

    def reap(self, child):
        self.assertTrue(processes.reap_created_process(child), "original child cleanup is unproven")
        if child.stdin is not None and not child.stdin.closed:
            child.stdin.close()

    def exit_without_reaping(self, child, identity):
        child.stdin.write(b"x")
        child.stdin.close()
        path = processes.procfs_root() / str(child.pid) / "stat"
        deadline = time.monotonic() + 5
        while True:
            record = path.read_text(encoding="ascii")
            fields = record.rsplit(")", 1)[1].split()
            if fields[0] == "Z":
                self.assertEqual(identity["creation_time"], f"linux-boot:{processes._linux_boot_id()}:{fields[19]}")
                return
            self.assertLess(time.monotonic(), deadline, "original child did not reach native terminal state")
            time.sleep(.01)

    def test_original_live_then_unreaped_child_has_exact_terminal_proof_without_signal(self):
        child, identity = self.child()
        self.assertTrue(processes.process_alive(child.pid))
        self.assertTrue(processes.identity_matches(child.pid, identity["creation_time"]))
        self.assertFalse(processes.identity_exit_proven(child.pid, identity["creation_time"]))
        legacy = processes._identity_time(identity["creation_time"]).isoformat()
        self.exit_without_reaping(child, identity)
        with patch.object(processes.os, "kill", side_effect=AssertionError("terminal child must not be signaled")):
            self.assertFalse(processes.process_alive(child.pid))
            self.assertFalse(processes.identity_matches(child.pid, identity["creation_time"]))
            self.assertTrue(processes.identity_exit_proven(child.pid, identity["creation_time"]))
            self.assertTrue(processes.identity_exit_proven(child.pid, legacy))
            self.assertTrue(processes.terminate_process(child.pid, identity["creation_time"], force=True))
        self.assertEqual(0, child.wait(timeout=5))
        self.assertTrue(processes.identity_exit_proven(child.pid, identity["creation_time"]))

    def test_original_process_group_is_gone_with_unreaped_terminal_root(self):
        child, identity = self.child()
        boundary = processes.ProcessBoundary.for_process(child.pid)
        self.assertTrue(boundary.observe())
        self.assertFalse(boundary.is_gone())
        self.exit_without_reaping(child, identity)
        self.assertTrue(boundary.is_gone())
        self.assertTrue(boundary.cleanup(force=True, timeout_seconds=1))
        self.assertEqual(0, child.wait(timeout=5))

    def test_unreadable_or_malformed_terminal_observation_cannot_prove_live_child_exit(self):
        child, identity = self.child()
        for error in (PermissionError("native state unreadable"), ValueError("malformed state"),
                      RuntimeError("proc mount unknown")):
            with self.subTest(error=type(error).__name__):
                with patch.object(processes, "_linux_process_stat", side_effect=error):
                    self.assertTrue(processes.process_alive(child.pid))
                    self.assertTrue(processes.identity_matches(child.pid, identity["creation_time"]))
                    self.assertFalse(processes.identity_exit_proven(child.pid, identity["creation_time"]))


class LinuxBootIdentityTests(unittest.TestCase):
    old_boot = "11111111-1111-1111-1111-111111111111"
    new_boot = "22222222-2222-2222-2222-222222222222"

    def test_new_identity_distinguishes_equal_pid_and_ticks_after_reboot(self):
        with (patch.object(processes.sys, "platform", "linux"),
              patch.object(processes, "exact_process_identity", return_value={"pid": 123, "created_utc": "linux-start-ticks:500"}),
              patch.object(processes, "_linux_boot_id", side_effect=[self.old_boot, self.new_boot])):
            old, new = processes.process_identity(123), processes.process_identity(123)
        self.assertNotEqual(old, new)
        self.assertTrue(processes.valid_creation_identity(old["creation_time"]))
        self.assertTrue(processes.valid_creation_identity(new["creation_time"]))

    def test_unknown_boot_cannot_publish_signal_identity(self):
        with (patch.object(processes.sys, "platform", "linux"),
              patch.object(processes, "exact_process_identity", return_value={"pid": 123, "created_utc": "linux-start-ticks:500"}),
              patch.object(processes, "_linux_boot_id", side_effect=OSError("boot ID unreadable"))):
            self.assertIsNone(processes.process_identity(123))

    def test_old_boot_proves_exit_without_adopting_reused_group_or_signaling(self):
        old = {"pid": 123, "creation_time": f"linux-boot:{self.old_boot}:500"}
        record = {"kind": "posix-process-group", "root": old, "processes": [old],
                  "process_group_id": 123, "session_id": 123}
        with (patch.object(processes.sys, "platform", "linux"),
              patch.object(processes, "_linux_boot_id", return_value=self.new_boot),
              patch.object(processes, "process_snapshot", side_effect=AssertionError("old group cannot grant membership")),
              patch.object(processes.os, "kill", side_effect=AssertionError("reused PID cannot be signaled"))):
            self.assertTrue(processes.identity_exit_proven(**old))
            self.assertTrue(processes.terminate_process(**old, force=True))
            self.assertTrue(processes.process_boundary_is_gone(record))
            self.assertTrue(processes.cleanup_recorded_process_boundary(record))

    def test_legacy_ticks_cannot_signal_live_alias_but_can_prove_old_pid_exit(self):
        current = {"pid": 123, "creation_time": f"linux-boot:{self.new_boot}:500"}
        with (patch.object(processes.sys, "platform", "linux"),
              patch.object(processes, "_linux_boot_id", return_value=self.new_boot),
              patch.object(processes, "_linux_process_stat", return_value=("S", 500)),
              patch.object(processes, "process_identity", return_value=current),
              patch.object(processes, "process_alive", return_value=True),
              patch.object(processes.os, "kill", side_effect=AssertionError("unscoped PID cannot be signaled"))):
            self.assertTrue(processes.identity_matches(**current))
            self.assertFalse(processes.identity_matches(123, "linux-start-ticks:500"))
            self.assertFalse(processes.terminate_process(123, "linux-start-ticks:500", force=True))
            self.assertFalse(processes.identity_exit_proven(123, "linux-start-ticks:500"))
            self.assertTrue(processes.identity_exit_proven(123, "linux-start-ticks:499"))
            self.assertTrue(processes.terminate_process(123, "linux-start-ticks:499", force=True))
            record = {"kind": "posix-process-group", "root": {"pid": 123, "creation_time": "linux-start-ticks:500"},
                      "processes": [], "process_group_id": 123, "session_id": 123}
            self.assertFalse(processes.cleanup_recorded_process_boundary(record))

    def test_reused_group_without_original_root_cannot_grant_signal_authority(self):
        from datetime import datetime, timezone
        original = {"pid": 123, "creation_time": f"linux-boot:{self.new_boot}:100"}
        record = {"kind": "posix-process-group", "root": original, "processes": [original],
                  "process_group_id": 123, "session_id": 123}
        created = datetime(2026, 10, 15, tzinfo=timezone.utc)
        snapshot = ProcessSnapshot(True, (ProcessInfo(456, 1, "unrelated", "unrelated", created,
                                                     process_group_id=123, session_id=123),))
        with (patch.object(processes.sys, "platform", "linux"),
              patch.object(processes, "_linux_boot_id", return_value=self.new_boot),
              patch.object(processes, "_identity_time", return_value=created),
              patch.object(processes, "process_identity", side_effect=lambda pid: None if pid == 123 else
                           {"pid": 456, "creation_time": f"linux-boot:{self.new_boot}:500"}),
              patch.object(processes, "process_alive", side_effect=lambda pid: pid == 456),
              patch.object(processes, "terminate_process") as terminate):
            boundary = processes.ProcessBoundary.from_record(record, snapshot_provider=lambda: snapshot)
            self.assertFalse(boundary.cleanup(force=True, timeout_seconds=.01))
            self.assertEqual({(123, original["creation_time"])}, set(boundary._owned))
        terminate.assert_not_called()


class LifecycleAdversarialTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name) / self._testMethodName
        self.base.mkdir(parents=True)
        self.harness = self.base / "harness"
        self.root = self.base / "workspace"
        self.harness.mkdir()
        self.root.mkdir()
        self.rt = self.root / ".harness-runtime"
        atomic_write_json(self.rt / "RUNTIME_STATE.json", {"schema": "runtime-state/v1", "state": "OPEN"})

    def fixture_shutdown(self):
        config = HarnessConfig(self.harness, self.root, "enabled")
        with (patch.object(shutdown, "find_harness_root", return_value=self.harness),
              patch.object(shutdown, "load_config", return_value=config),
              patch.object(shutdown, "_prune_worktrees"),
              patch.object(shutdown, "MONITOR_WAIT_SECONDS", 0.05)):
            return shutdown.run_shutdown()

    @unittest.skipUnless(os.name == "nt", "native Windows repeated owned Job cleanup")
    def test_owned_job_cleanup_is_reusable_without_global_inventory(self):
        with open(os.devnull, "rb") as stdin, open(os.devnull, "wb") as stdout, open(os.devnull, "wb") as stderr:
            child = processes.spawn_provider([sys.executable, "-B", "-c", "import time; time.sleep(30)"],
                                             cwd=self.root, stdin=stdin, stdout=stdout, stderr=stderr)
        boundary = None
        try:
            boundary = processes.ProcessBoundary.for_process(child.pid, windows_job_handle=child.take_job_handle())
            child.resume()
            self.assertTrue(boundary.cleanup(force=True))
            with patch.object(processes, "process_snapshot", side_effect=AssertionError("already-clean Job needs no inventory")):
                self.assertTrue(boundary.cleanup(force=True))
            self.assertTrue(processes.identity_exit_proven(child.pid, boundary.root_creation_time))
        finally:
            if boundary is not None:
                boundary._close_job()
            self.assertTrue(processes.reap_created_process(child))
            child.close()

    def test_shutdown_checks_run_with_a_nonempty_transcript_backlog(self):
        worktree = self.root / "worker"
        agent = worktree / ".agent-workspace"
        agent.mkdir(parents=True)
        prompt = agent / "prompt.md"
        prompt.write_text("fixture", encoding="utf-8")
        lane = {"lane_id": "worker", "run_id": "run", "worktree_path": str(worktree),
                "provider": {"id": "fixture"}, "lifecycle": "running", "process": {},
                "last_message_path": str(agent / "last.txt"),
                "controller_status_path": str(agent / "status.json"),
                "controller_events_path": str(agent / "events.jsonl")}
        child = MagicMock(pid=123, returncode=-1)
        child.poll.side_effect = [None, None, None, 0]
        boundary = MagicMock(root_creation_time="2026-10-15T00:00:00Z", process_group_id=None, session_id=None)
        boundary.record.return_value = {"kind": "fixture"}
        state = {"state": "OPEN"}
        parsed, stopped_at = [], []

        def spawn(*args, **kwargs):
            # The reader always has another line while the fake child is live.
            (agent / "provider-transcript.jsonl").write_text("{}\n" * 20, encoding="utf-8")
            return child

        def parse(line):
            parsed.append(line)
            state["state"] = "SHUTTING_DOWN"
            return {}

        boundary.cleanup.side_effect = lambda **_: stopped_at.append(len(parsed)) or True
        clock = iter(range(100))
        with (patch.object(controller, "read_lane", return_value=lane),
              patch.object(lane_authority, "read_invocation"),
              patch.object(controller, "read_current_epoch", return_value={"epoch_id": "epoch"}),
              patch.object(controller, "read_runtime_state", side_effect=lambda _: dict(state)),
              patch.object(controller, "reserve_invocation", return_value="token"),
              patch.object(controller, "record_started"),
              patch.object(controller.time, "monotonic", side_effect=lambda: next(clock)),
              patch.object(processes, "spawn_provider", side_effect=spawn),
              patch.object(processes.ProcessBoundary, "for_process", return_value=boundary)):
            controller._run_provider(self.rt, "epoch", lane, {"provider": {"model": "fixture", "launch_config": {}}},
                                     SimpleNamespace(build_argv=lambda **_: ["fixture"], parse_line=parse), prompt)
        self.assertEqual([1], stopped_at, "cancellation must occur before draining the live transcript")
        child.wait.assert_called_once_with(timeout=10.0)

    def test_shutdown_does_not_close_with_exact_live_stopped_monitor(self):
        identity = processes.process_identity(os.getpid())
        self.assertIsNotNone(identity)
        atomic_write_json(shutdown.monitor_record_path(self.rt), {
            "schema": shutdown.MONITOR_SCHEMA, **identity,
            "health": "STOPPED", "stop_requested": False,
        })
        result = self.fixture_shutdown()
        self.assertTrue(processes.identity_matches(identity["pid"], identity["creation_time"]))
        self.assertFalse(result["ok"], result)

    def test_shutdown_does_not_treat_invalid_monitor_record_as_absence(self):
        path = shutdown.monitor_record_path(self.rt)
        path.parent.mkdir()
        path.write_text("{invalid", encoding="utf-8")
        result = self.fixture_shutdown()
        self.assertFalse(result["ok"], result)

    def test_provider_rechecks_block_created_after_outer_gate(self):
        worktree = self.root / "lane"
        agent = worktree / ".agent-workspace"
        agent.mkdir(parents=True)
        lane = {"lane_id": "lane", "run_id": "initial", "worktree_path": str(worktree),
                "provider": {"id": "codex"}, "session": {},
                "controller_status_path": str(agent / "status.json"),
                "controller_events_path": str(agent / "events.jsonl"),
                "isolation_config_path": str(self.rt / "operator-only/isolation.toml"),
                "last_message_path": str(agent / "last-message.txt")}
        prompt = agent / "prompt.md"
        prompt.write_text("finite fixture", encoding="utf-8")
        (agent / "worker-prompt.md").write_text("finite fixture", encoding="utf-8")
        (worktree / ".codex").mkdir()
        (worktree / ".codex/config.toml").write_text('default_permissions = ":workspace"\n', encoding="utf-8")
        bootstrap._write_invocation(worktree, lane_id=lane["lane_id"], run_id=lane["run_id"],
                                    provider_id="codex", model="fixture",
                                    launch_config={"permission_mode": "yolo"}, exclusive_resources=[])
        generated = public_checks.read(agent / "invocation.json")
        lane["provider"] = generated["provider"]
        lane["exclusive_resources"] = []
        card = {"schema": "project-task-card/v1", "task": "fixture", "acceptance_criteria": ["fixture"],
                "deliverables": ["fixture"], "reason_for_acceptance_and_deliverables": "fixture"}
        lane = prepare_lane_fixture(self.rt, "epoch", lane, task_card=card)
        seed_provider_history(self.rt, lane, epoch_id="epoch")
        write_lane(self.rt, "epoch", lane["lane_id"], lane)
        prompt = Path(lane["prompt_path"])
        require_not_blocked(self.root)  # Same initial gate as run_controller.

        def build_argv(**_):
            # Deterministic interleaving: another lane latches failure after
            # run_controller's gate and before the last process-create boundary.
            atomic_write_json(self.root / BLOCK, {"schema": "sandbox-launch-block/v1", "reason": "another lane failed"})
            return [sys.executable, "-B", "-c", "pass"]

        def denied_spawn(*_, **__):
            self.assertTrue((self.root / BLOCK).is_file())
            raise ProvenNoProvider("probe never creates a provider")

        invocation = lane_authority.read_invocation(lane)
        binding = SimpleNamespace(build_argv=build_argv, parse_line=json.loads)
        with patch.object(processes, "spawn_provider", side_effect=denied_spawn) as spawn:
            with self.assertRaises(controller.ControllerError):
                controller._run_provider(self.rt, "epoch", lane, invocation, binding, prompt)
            self.assertEqual(0, spawn.call_count, "provider creation reached after run-wide block became durable")

    def service_record(self, **updates):
        identity = processes.process_identity(os.getpid())
        self.assertIsNotNone(identity)
        record = {"schema": "worker-public-check-service/v1", **identity,
                  "root": str(self.root), "harness": str(self.harness),
                  "status": "STOPPED", "cleanup_proven": True,
                  "policy_sha256": hashlib.sha256(json.dumps({"fixture": True}, sort_keys=True).encode()).hexdigest()}
        record.update(updates)
        atomic_write_json(public_checks.home(self.root) / "service.json", record)
        atomic_write_json(public_checks.home(self.root) / "config.json", {"policy": {"fixture": True}})
        return record

    def test_stopped_service_with_garbage_creation_time_is_unproven(self):
        self.service_record(creation_time="not-a-process-creation-time")
        with (patch.object(public_checks, "policy", return_value={"fixture": True}),
              patch.object(public_checks, "_adapter_path", return_value=self.harness / "adapter.py")):
            with self.assertRaises(ValueError):
                public_checks.validate_service(self.root, self.harness, runtime_closed=True)

    def test_stop_service_with_garbage_creation_time_is_unproven(self):
        self.service_record(creation_time="not-a-process-creation-time")
        with self.assertRaises(ValueError):
            public_checks.stop_service(self.root, self.harness)

    def test_unknown_process_query_is_not_exit_proof(self):
        identity = processes.process_identity(os.getpid())
        with (patch.object(processes, "process_identity", return_value=None),
              patch.object(processes, "targeted_process_query", return_value=ProcessQuery(False, None, ("access denied",)))):
            self.assertFalse(processes.identity_exit_proven(identity["pid"], identity["creation_time"]))

    def test_complete_absence_query_proves_exit(self):
        identity = processes.process_identity(os.getpid())
        with (patch.object(processes, "process_identity", return_value=None),
              patch.object(processes, "_windows_identity_exit_proven", return_value=None),
              patch.object(processes, "targeted_process_query", return_value=ProcessQuery(True, None))):
            self.assertTrue(processes.identity_exit_proven(identity["pid"], identity["creation_time"]))

    def test_invalid_creation_identity_cannot_become_absence(self):
        with patch.object(processes, "targeted_process_query") as query:
            for value in (None, "garbage", "windows-filetime:-1", "linux-start-ticks:nan", "darwin-start-time:1:1000000"):
                self.assertFalse(processes.identity_exit_proven(123, value))
            query.assert_not_called()

    def test_exact_live_process_has_no_exit_proof(self):
        identity = processes.process_identity(os.getpid())
        self.assertFalse(processes.identity_exit_proven(identity["pid"], identity["creation_time"]))

    def shutdown_lane(self):
        self.epoch = open_epoch(self.rt, HarnessConfig(self.harness, self.root, "enabled"),
                                ResourceManifest())["epoch_id"]
        agent = self.root / "worker/.agent-workspace"
        agent.mkdir(parents=True)
        lane = {"schema": "lane/v1", "lane_id": "worker", "run_id": "initial",
                "provider": {"id": "codex"}, "worktree_path": str(agent.parent),
                "lifecycle": "prepared", "process": {},
                "controller_status_path": str(agent / "status.json")}
        write_lane(self.rt, self.epoch, "worker", lane)
        write_active_lanes(self.rt, self.epoch, [{"lane_id": "worker", "run_id": "initial"}])
        return lane

    def test_shutdown_wait_does_not_hold_controller_startup_lock(self):
        self.shutdown_lane()
        acquired = threading.Event()
        threads = []
        def wait(*_):
            def startup():
                with lane_operation_lock(self.rt, self.epoch, "worker"):
                    acquired.set()
            thread = threading.Thread(target=startup)
            threads.append(thread)
            thread.start()
            return acquired.wait(1)
        with patch.object(shutdown, "_wait_for_controller_exit", side_effect=wait):
            result = self.fixture_shutdown()
        for thread in threads:
            thread.join(2)
        self.assertTrue(result["ok"], result)
        self.assertTrue(acquired.is_set())

    def test_shutdown_revalidates_run_after_unlocked_wait(self):
        lane = self.shutdown_lane()
        def change_run(*_):
            write_lane(self.rt, self.epoch, "worker", {**lane, "run_id": "new-owner"})
            return True
        with patch.object(shutdown, "_wait_for_controller_exit", side_effect=change_run):
            result = self.fixture_shutdown()
        self.assertFalse(result["ok"], result)
        current = read_lane(self.rt, self.epoch, "worker")
        self.assertEqual(("new-owner", "prepared"), (current["run_id"], current["lifecycle"]))


if __name__ == "__main__":
    unittest.main()
