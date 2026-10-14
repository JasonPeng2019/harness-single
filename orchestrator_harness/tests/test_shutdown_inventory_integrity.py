"""Admission and closure reconcile durable native ownership."""
from __future__ import annotations

import json
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import bootstrap, config, epochs, lanes, setup, shutdown
from orchestrator_harness.records import atomic_write_json


class ShutdownInventoryIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.harness = self.root / "harness"
        self.workspace = self.root / "workspace"
        self.harness.mkdir()
        self.workspace.mkdir()
        for argv in (["init", "-q"], ["config", "core.autocrlf", "false"]):
            subprocess.run(["git", "-C", str(self.workspace), *argv], check=True, capture_output=True)
        (self.workspace / ".gitignore").write_text(".harness-runtime/\n", encoding="utf-8")
        (self.workspace / "source.txt").write_text("owned fixture source\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(self.workspace), "add", ".gitignore", "source.txt"], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(self.workspace), "-c", "user.name=Fixture", "-c", "user.email=fixture@local",
                        "commit", "-qm", "fixture base"], check=True, capture_output=True)
        self.config = config.HarnessConfig(self.harness, self.workspace, "enabled")
        self.rt = self.config.runtime_root
        setup.set_runtime_state(self.rt, "OPEN")
        self.card = self.harness / "task.json"
        atomic_write_json(self.card, {"schema": "project-task-card/v1", "task": "fixture",
            "acceptance_criteria": ["fixture"], "deliverables": ["fixture"],
            "reason_for_acceptance_and_deliverables": "fixture"})

    def lane(self):
        epoch = epochs.open_epoch(self.rt, self.config, config.ResourceManifest())["epoch_id"]
        worktree = self.workspace / "worker"
        worktree.mkdir()
        lane = lanes.write_lane(self.rt, epoch, "worker", {
            "run_id": "initial", "provider": {"id": "qwen-code"}, "lifecycle": "prepared",
            "worktree_path": str(worktree), "process": {}, "controller_status_path": str(worktree / "status.json")})
        epochs.write_active_lanes(self.rt, epoch, [{"lane_id": "worker", "run_id": "initial"}])
        return epoch, lane

    def shutdown(self):
        with (patch.object(shutdown, "find_harness_root", return_value=self.harness),
              patch.object(shutdown, "load_config", return_value=self.config),
              patch.object(shutdown, "_stop_monitor", return_value=None), patch.object(shutdown, "_prune_worktrees")):
            return shutdown.run_shutdown()

    def test_corrupt_native_control_records_refuse_closure(self):
        epoch, _ = self.lane()
        for path in (self.rt / "CURRENT_EPOCH.json", self.rt / "epochs" / epoch / "epoch-state.json",
                     self.rt / "epochs" / epoch / "active-lanes.json", setup.runtime_state_path(self.rt)):
            with self.subTest(record=path.name):
                original = path.read_bytes()
                path.write_text("{invalid", encoding="utf-8")
                result = self.shutdown()
                self.assertFalse(result["ok"], result)
                path.write_bytes(original)
                setup.set_runtime_state(self.rt, "OPEN")

    def test_absent_marker_does_not_hide_durable_active_epoch(self):
        self.lane()
        (self.rt / "CURRENT_EPOCH.json").unlink()
        self.assertFalse(self.shutdown()["ok"])

    def test_durable_lane_omitted_from_index_is_still_retired(self):
        epoch, _ = self.lane()
        epochs.write_active_lanes(self.rt, epoch, [])
        result = self.shutdown()
        self.assertTrue(result["ok"], result)
        self.assertEqual("retired", lanes.read_lane(self.rt, epoch, "worker")["lifecycle"])

    def test_missing_or_stale_indexed_lane_is_ambiguous(self):
        epoch, _ = self.lane()
        for entries in ([{"lane_id": "missing", "run_id": "initial"}],
                        [{"lane_id": "worker", "run_id": "older"}]):
            epochs.write_active_lanes(self.rt, epoch, entries)
            self.assertFalse(self.shutdown()["ok"])
            setup.set_runtime_state(self.rt, "OPEN")

    def test_bootstrap_requires_open_before_any_git_effect(self):
        for state in ("CLOSED", "SHUTTING_DOWN"):
            with self.subTest(state=state):
                setup.set_runtime_state(self.rt, state)
                with patch.object(bootstrap, "_git_worktree_add") as create:
                    result = self.bootstrap()
                self.assertFalse(result["ok"], result)
                create.assert_not_called()
                self.assertFalse((self.rt / "CURRENT_EPOCH.json").exists())

    def bootstrap(self):
        with (patch.object(config, "find_harness_root", return_value=self.harness),
              patch.object(bootstrap, "load_config", return_value=self.config),
              patch.object(bootstrap, "load_resource_manifest", return_value=config.ResourceManifest()),
              patch.object(bootstrap, "_validate_provider_launch_config", return_value={}),
              patch.object(bootstrap, "_managed_payload", return_value=self.harness),
              patch.object(bootstrap, "_install_managed_material")):
            return bootstrap.run_bootstrap(lane_id="worker", provider="qwen-code", model="fixture",
                launch_config={}, exclusive_resources=[], task_card_path=str(self.card))

    def test_shutdown_waits_for_admitted_bootstrap_publication(self):
        creating = threading.Event()
        finish_create = threading.Event()
        shutdown_started = threading.Event()
        shutdown_done = threading.Event()
        results = {}
        real_create = bootstrap._git_worktree_add
        def create(root, branch, worktree, base):
            creating.set()
            if not finish_create.wait(5):
                raise AssertionError("fixture did not release bootstrap")
            real_create(root, branch, worktree, base)
        def create_lane():
            results["bootstrap"] = self.bootstrap()
        def close():
            shutdown_started.set()
            try:
                results["shutdown"] = self.shutdown()
            finally:
                shutdown_done.set()
        with patch.object(bootstrap, "_git_worktree_add", side_effect=create):
            creator = threading.Thread(target=create_lane)
            closer = threading.Thread(target=close)
            creator.start()
            try:
                self.assertTrue(creating.wait(5))
                closer.start()
                self.assertTrue(shutdown_started.wait(5))
                self.assertFalse(shutdown_done.wait(0.2), results)
                self.assertEqual("OPEN", setup.read_runtime_state(self.rt)["state"])
            finally:
                finish_create.set()
                creator.join(5)
                if closer.ident is not None:
                    closer.join(5)
        self.assertFalse(creator.is_alive() or closer.is_alive())
        self.assertTrue(results["bootstrap"]["ok"], results)
        self.assertTrue(results["shutdown"]["ok"], results)
        self.assertEqual("CLOSED", setup.read_runtime_state(self.rt)["state"])


if __name__ == "__main__":
    unittest.main()
