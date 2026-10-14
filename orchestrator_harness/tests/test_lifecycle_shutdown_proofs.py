"""Independent finite lifecycle probes. All fixtures belong to this role."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from orchestrator_harness import bootstrap, controller, lane_authority, processes, public_checks, shutdown
from orchestrator_harness.config import HarnessConfig, ResourceManifest
from orchestrator_harness.models import ProcessQuery
from orchestrator_harness.lanes import lane_operation_lock, write_lane, read_lane
from orchestrator_harness.epochs import write_active_lanes, open_epoch
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.tests.support import prepare_lane_fixture, seed_provider_history
from orchestrator_harness.windows_sandbox_preflight import BLOCK, require_not_blocked


class ProvenNoProvider(RuntimeError):
    no_provider_created = True


class LifecycleAdversarialTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name) / self._testMethodName
        self.base.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.base)
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
