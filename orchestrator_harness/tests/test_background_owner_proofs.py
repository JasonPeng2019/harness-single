"""Exact background ownership survives replacement and CLOSED re-observation."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from orchestrator_harness import config, epochs, lanes, monitor, processes, setup, shutdown
from orchestrator_harness.records import atomic_write_json


class BackgroundOwnerProofTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.harness = Path(temporary.name) / "harness"
        self.workspace = Path(temporary.name) / "workspace"
        self.harness.mkdir()
        self.workspace.mkdir()
        self.config = config.HarnessConfig(self.harness, self.workspace, "enabled")
        self.rt = self.config.runtime_root
        setup.set_runtime_state(self.rt, "OPEN")

    def shutdown(self):
        with (patch.object(shutdown, "find_harness_root", return_value=self.harness),
              patch.object(shutdown, "load_config", return_value=self.config),
              patch.object(shutdown, "_prune_worktrees")):
            return shutdown.run_shutdown()

    def record(self, identity=None, startup_id="3" * 32, **fields):
        record = {"schema": setup.MONITOR_SCHEMA,
                  **(identity or processes.process_identity(os.getpid())),
                  "config_identity": "fixture", "startup_id": startup_id,
                  "health": "healthy", "stop_requested": False, **fields}
        atomic_write_json(setup.monitor_record_path(self.rt), record)
        return record

    def dead_child(self):
        child = subprocess.Popen([sys.executable, "-B", "-c", "import sys; sys.stdin.buffer.read(1)"],
                                 stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL,
                                 creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        try:
            identity = processes.process_identity(child.pid)
            self.assertIsNotNone(identity)
            child.communicate(b"x", timeout=10)
            self.assertTrue(processes.identity_exit_proven(identity["pid"], identity["creation_time"]))
            return identity
        finally:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=10)

    def test_replaced_monitor_nonce_prevents_heartbeat_and_pass(self):
        old = self.record()
        owner = monitor._monitor_owner(old)
        self.record(startup_id="4" * 32)
        path = setup.monitor_record_path(self.rt)
        original = path.read_bytes()
        monitor._heartbeat(self.rt, "fixture", 9, [{"error": "old owner"}], monitor_owner=owner)
        with self.assertRaisesRegex(ValueError, "ownership changed"):
            monitor._monitor_pass(self.rt, "fixture", monitor_owner=owner)
        self.assertEqual(original, path.read_bytes())
        self.assertFalse((self.rt / "manager/QUEUE.json").exists())

    def test_monitor_replacement_during_stop_keeps_new_record(self):
        old = self.record(stop_requested=True)
        new = {**old, "startup_id": "4" * 32, "stop_requested": False}
        original_require = monitor._require_monitor_owner
        calls = []

        def observe(rt, owner):
            calls.append(owner)
            if len(calls) == 2:
                atomic_write_json(setup.monitor_record_path(rt), new)
            return original_require(rt, owner)

        with (patch.object(monitor, "find_harness_root", return_value=self.harness),
              patch.object(monitor, "load_config", return_value=self.config),
              patch.object(monitor, "load_resource_manifest", return_value=config.ResourceManifest()),
              patch.object(monitor, "compute_config_identity", return_value="fixture"),
              patch.object(monitor, "_require_monitor_owner", side_effect=observe)):
            # Setup hands the reserved nonce to the exact monitor it starts.
            with patch.dict(os.environ, {"HARNESS_MONITOR_STARTUP_ID": old["startup_id"]}):
                self.assertEqual(1, monitor.main())
            self.assertEqual(2, len(calls))
            self.assertEqual(new, setup.read_monitor_record(self.rt))
            # A process without the reserved nonce never adopts the record.
            calls.clear()
            with patch.dict(os.environ, {"HARNESS_MONITOR_STARTUP_ID": ""}):
                self.assertEqual(1, monitor.main())
            self.assertEqual([], calls)
        self.assertEqual(new, setup.read_monitor_record(self.rt))

    def test_closed_receipt_rejects_a_new_live_monitor(self):
        self.assertTrue(self.shutdown()["ok"])
        shutdown.prove_runtime_closed(self.rt, self.workspace, self.harness)
        self.record(health="STOPPED")
        with self.assertRaises(shutdown.ShutdownError):
            shutdown.prove_runtime_closed(self.rt, self.workspace, self.harness)
        self.assertFalse(self.shutdown()["ok"])

    def test_final_closed_compares_exact_dead_incarnations(self):
        old = self.record(identity=self.dead_child())
        replacement = {**old, **self.dead_child(), "startup_id": "4" * 32}

        def stop_and_replace(rt):
            atomic_write_json(setup.monitor_record_path(rt), replacement)
            return old

        with patch.object(shutdown, "_stop_monitor", side_effect=stop_and_replace):
            result = self.shutdown()
        self.assertFalse(result["ok"], result)
        self.assertIn("ownership changed", result["summary"])
        self.assertEqual("SHUTTING_DOWN", setup.read_runtime_state(self.rt)["state"])
        self.assertEqual(replacement, setup.read_monitor_record(self.rt))

    def test_legacy_closed_without_retained_owner_is_not_proof(self):
        setup.set_runtime_state(self.rt, "CLOSED")
        with self.assertRaisesRegex(shutdown.ShutdownError, "no proven CLOSED"):
            shutdown.prove_runtime_closed(self.rt, self.workspace, self.harness)
        self.assertFalse(self.shutdown()["ok"])

    def test_retired_preparation_retains_run_bound_cleanup_proof(self):
        epoch = epochs.open_epoch(self.rt, self.config, config.ResourceManifest())["epoch_id"]
        worktree = self.workspace / "worker"
        worktree.mkdir()
        lane = lanes.write_lane(self.rt, epoch, "worker", {
            "run_id": "initial", "provider": {"id": "qwen-code"}, "lifecycle": "prepared",
            "worktree_path": str(worktree), "process": {},
            "controller_status_path": str(worktree / "status.json")})
        epochs.write_active_lanes(self.rt, epoch, [{"lane_id": "worker", "run_id": "initial"}])
        self.assertTrue(self.shutdown()["ok"])
        retired = lanes.read_lane(self.rt, epoch, "worker")
        self.assertEqual(lane["run_id"], retired["retirement_proof"]["run_id"])
        shutdown.prove_runtime_closed(self.rt, self.workspace, self.harness)
        retired["run_id"] = "different"
        lanes.write_lane(self.rt, epoch, "worker", retired)
        with self.assertRaises(shutdown.ShutdownError):
            shutdown.prove_runtime_closed(self.rt, self.workspace, self.harness)

    def test_closed_retains_no_resource_lease(self):
        self.assertTrue(self.shutdown()["ok"])
        path = self.rt / "resources/leases/unobserved.lease"
        atomic_write_json(path, {"schema": "resource-lease/v1", "lane_id": "unknown"})
        with self.assertRaisesRegex(shutdown.ShutdownError, "resource ownership"):
            shutdown.prove_runtime_closed(self.rt, self.workspace, self.harness)
        self.assertTrue(path.exists())
