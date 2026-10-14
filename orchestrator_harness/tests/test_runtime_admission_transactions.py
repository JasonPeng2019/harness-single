"""Whole ownership transactions survive concurrency, corruption and interruption."""
from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import bootstrap, config, epochs, lane_authority, lane_cleanup, lanes, launch, leases, monitor, retry_policy, setup
from orchestrator_harness.records import RecordLock, atomic_write_json, runtime_admission_lock
from orchestrator_harness.tests.support import prepare_lane_fixture

CREATION = "windows-filetime:134361777213898726"


class RuntimeAdmissionTransactionsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        self.config = config.HarnessConfig(self.root / "harness", self.workspace, "enabled")
        self.rt = self.config.runtime_root
        setup.set_runtime_state(self.rt, "OPEN")
        self.manifest = config.ResourceManifest()
        self.epoch = epochs.open_epoch(self.rt, self.config, self.manifest)["epoch_id"]

    def lane(self):
        worktree = self.rt / "worktrees" / self.epoch / "worker"
        worktree.mkdir(parents=True)
        record = lanes.write_lane(self.rt, self.epoch, "worker", {
            "run_id": "initial", "lifecycle": "prepared", "process": {},
            "provider": {"id": "qwen-code", "model": "fixture", "launch_config": {}},
            "exclusive_resources": ["device"], "worktree_path": str(worktree),
            "controller_status_path": str(worktree / ".agent-workspace" / "controller.status.json")})
        lanes.reconcile_active_lanes(self.rt, self.epoch)
        return record

    def test_nested_admission_reuses_the_lock_and_other_threads_wait(self):
        started, finished = threading.Event(), threading.Event()
        def contender():
            started.set()
            with runtime_admission_lock(self.rt):
                finished.set()
        with runtime_admission_lock(self.rt):
            with runtime_admission_lock(self.rt):
                child = threading.Thread(target=contender)
                child.start()
                self.assertTrue(started.wait(2))
                self.assertFalse(finished.wait(0.1))
        child.join(2)
        self.assertFalse(child.is_alive())
        self.assertTrue(finished.is_set())

    def test_automatic_close_waits_for_admission_and_finds_omitted_durable_lane(self):
        lane = self.lane()
        epochs.write_active_lanes(self.rt, self.epoch, [])
        started, finished = threading.Event(), threading.Event()
        def close():
            started.set()
            try:
                launch._maybe_close_epoch(self.rt, self.epoch)
            finally:
                finished.set()
        with RecordLock(setup.runtime_state_path(self.rt)):
            child = threading.Thread(target=close)
            child.start()
            self.assertTrue(started.wait(2))
            self.assertFalse(finished.wait(0.1))
            self.assertIsNotNone(epochs.read_current_epoch(self.rt))
        child.join(2)
        self.assertFalse(child.is_alive())
        self.assertEqual("active", epochs.read_epoch_state(self.rt, self.epoch)["lifecycle"])
        self.assertEqual(lane["run_id"], epochs.read_active_lanes(self.rt, self.epoch)[0]["run_id"])

    def test_corrupt_durable_lane_preserves_index_and_mutation_evidence(self):
        self.lane()
        index = epochs.epoch_dir(self.rt, self.epoch) / "active-lanes.json"
        before = index.read_bytes()
        path = epochs.lane_record_path(self.rt, self.epoch, "worker")
        path.write_text("{invalid", encoding="utf-8")
        for operation in (lambda: monitor.reconcile_active_lanes(self.rt, self.epoch),
                          lambda: lanes.update_lane(self.rt, self.epoch, "worker", lambda row: {**row, "lifecycle": "retired"})):
            with self.assertRaises(lanes.LaneError):
                operation()
        self.assertEqual(before, index.read_bytes())
        self.assertEqual("{invalid", path.read_text(encoding="utf-8"))

    def test_current_run_inventory_is_rebuilt_without_losing_other_owners(self):
        self.lane()
        with runtime_admission_lock(self.rt):
            lanes.update_lane(self.rt, self.epoch, "worker", lambda row: {**row, "run_id": "resumed"})
            lanes.reconcile_active_lanes(self.rt, self.epoch)
        self.assertEqual("resumed", epochs.read_active_lanes(self.rt, self.epoch)[0]["run_id"])

    def test_close_intent_survives_marker_removal_and_retries(self):
        real_write = epochs.atomic_write_json
        def interrupted(path, record):
            if record.get("schema") == epochs.EPOCH_STATE_SCHEMA and record.get("lifecycle") == "closed":
                raise OSError("fixture interrupted final close write")
            real_write(path, record)
        with patch.object(epochs, "atomic_write_json", side_effect=interrupted):
            with self.assertRaises(OSError):
                epochs.close_epoch(self.rt, self.epoch)
        self.assertIsNone(epochs.read_current_epoch(self.rt))
        self.assertEqual("closing", epochs.read_epoch_state(self.rt, self.epoch)["lifecycle"])
        epochs.close_epoch(self.rt, self.epoch)
        self.assertEqual("closed", epochs.read_epoch_state(self.rt, self.epoch)["lifecycle"])

    def test_open_intent_survives_missing_marker_and_reuses_its_identity(self):
        epochs.close_epoch(self.rt, self.epoch)
        real_write = epochs.atomic_write_json
        def interrupted(path, record):
            if record.get("schema") == epochs.CURRENT_EPOCH_SCHEMA:
                raise OSError("fixture interrupted marker publication")
            real_write(path, record)
        with patch.object(epochs, "atomic_write_json", side_effect=interrupted):
            with self.assertRaises(OSError):
                epochs.open_epoch(self.rt, self.config, self.manifest)
        pending = [path.name for path in (self.rt / "epochs").iterdir() if epochs.read_epoch_state(self.rt, path.name)["lifecycle"] == "opening"]
        self.assertEqual(1, len(pending))
        result = epochs.open_epoch(self.rt, self.config, self.manifest)
        self.assertEqual(pending[0], result["epoch_id"])

    def test_unknown_lease_holder_is_not_released_by_lifecycle_labels(self):
        for lifecycle, run in (("running", "initial"), ("retired", "initial"), ("abandoned", "initial"), ("running", "later")):
            with self.subTest(lifecycle=lifecycle, run=run):
                path = leases.lease_path(self.rt, "device")
                atomic_write_json(path, {"schema": leases.LEASE_SCHEMA, "resource_id": "device",
                    "lane_id": "worker", "run_id": "initial", "pid": 987, "creation_time": CREATION})
                with patch.object(leases, "identity_matches", return_value=False), patch.object(leases, "identity_exit_proven", return_value=False):
                    with self.assertRaises(leases.LeaseError):
                        leases.force_release_lease(self.rt, "device", lane={"lane_id": "worker", "run_id": run, "lifecycle": lifecycle})
                self.assertTrue(path.is_file())

    def test_strict_release_preserves_corrupt_lease_and_reports_failure(self):
        path = leases.lease_path(self.rt, "device")
        path.parent.mkdir(parents=True)
        path.write_text("{invalid", encoding="utf-8")
        with self.assertRaises(leases.LeaseError):
            leases.release_leases(self.rt, "worker", "initial", strict=True)
        self.assertEqual("{invalid", path.read_text(encoding="utf-8"))

    def test_monitor_ambiguity_and_stopped_live_owner_never_spawn_replacement(self):
        atomic_write_json(setup.monitor_record_path(self.rt), {"schema": setup.MONITOR_SCHEMA,
            "pid": 987, "creation_time": CREATION, "config_identity": "fixture", "health": "STOPPED", "stop_requested": True})
        for live in (False, True):
            with patch.object(setup.processes, "identity_matches", return_value=live), patch.object(setup.processes, "identity_exit_proven", return_value=False), patch.object(setup, "_start_monitor_locked") as spawn:
                with self.assertRaises(config.ConfigError):
                    setup._start_monitor(self.root, self.rt, self.config, "fixture")
                spawn.assert_not_called()

    def test_native_inputs_ignore_worker_resource_and_provider_rewrites(self):
        lane = self.lane()
        worktree = Path(lane["worktree_path"])
        bootstrap._write_invocation(worktree, lane_id="worker", run_id="initial", provider_id="qwen-code", model="fixture", launch_config={}, exclusive_resources=["device"])
        (worktree / ".agent-workspace" / "worker-prompt.md").write_text("fixture prompt", encoding="utf-8")
        card = {"schema": "project-task-card/v1", "task": "fixture", "acceptance_criteria": ["fixture"], "deliverables": ["fixture"], "reason_for_acceptance_and_deliverables": "fixture"}
        lane = prepare_lane_fixture(self.rt, self.epoch, lane, task_card=card)
        atomic_write_json(worktree / ".agent-workspace" / "invocation.json", {"schema": bootstrap.INVOCATION_SCHEMA, "provider": {"id": "untrusted"}, "exclusive_resources": []})
        invocation = lane_authority.read_invocation(lane)
        self.assertEqual(["device"], invocation["exclusive_resources"])
        self.assertEqual("qwen-code", invocation["provider"]["id"])
        self.assertFalse(Path(lane["controller_status_path"]).is_relative_to(worktree))
        Path(lane["prompt_path"]).write_text("changed", encoding="utf-8")
        with self.assertRaises(ValueError):
            lane_authority.read_invocation(lane)

    def test_worker_not_started_status_cannot_override_protected_started_provider(self):
        lane = self.lane()
        retry_policy.initialize_history(self.rt, lane)
        token = retry_policy.reserve_invocation(self.rt, lane)
        retry_policy.record_started(self.rt, lane, token, process_boundary={"kind": "windows-job", "root": {"pid": 987, "creation_time": CREATION}, "processes": [{"pid": 987, "creation_time": CREATION}]})
        atomic_write_json(Path(lane["controller_status_path"]), {"schema": "controller-status/v1", "lane_id": "worker", "run_id": "initial", "controller_state": "exited", "controller_identity": {"pid": 988, "creation_time": CREATION}, "cleanup_proven": True, "provider_state": {"state": "not_started"}})
        with patch.object(lane_cleanup.processes, "identity_exit_proven", return_value=True), patch.object(lane_cleanup.processes, "process_boundary_is_gone", return_value=False):
            self.assertFalse(lane_cleanup.cleanup_is_proven(lane))
        with patch.object(launch.processes, "terminate_process") as terminate:
            clean, code, _ = launch._cleanup_lane_processes(lane)
            self.assertFalse(clean)
            self.assertEqual(launch.FORCE_STOP_CLEANUP_UNPROVEN, code)
            terminate.assert_not_called()


if __name__ == "__main__":
    unittest.main()
