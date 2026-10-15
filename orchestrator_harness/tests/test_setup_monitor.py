"""Monitor readiness uses real owned children and the native record/lock path."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import processes, setup
from orchestrator_harness.config import ConfigError, compute_config_identity, load_config, load_resource_manifest
from orchestrator_harness.core import iso_utc
from orchestrator_harness.records import atomic_write_json, runtime_admission_lock


class SetupMonitorIdentityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.harness = Path(self.temporary.name)
        self.workspace = self.harness / "workspace"
        self.workspace.mkdir()
        (self.harness / "harness-config.json").write_text(json.dumps({"root_workspace": str(self.workspace)}), encoding="utf-8")
        (self.harness / "resource-manifest.json").write_text(json.dumps({"schema": "resource-manifest/v1", "resources": []}), encoding="utf-8")
        self.config = load_config(self.harness)
        self.identity = compute_config_identity(self.config, load_resource_manifest(self.harness))
        self.rt = self.config.runtime_root
        setup.set_runtime_state(self.rt, "OPEN")
        self.children = []
        self.addCleanup(self.stop_children)
        self.environment = {"PYTHONPATH": str(Path(__file__).resolve().parents[2])}

    def stop_children(self) -> None:
        setup.set_runtime_state(self.rt, "SHUTTING_DOWN")
        for child in self.children:
            if child.poll() is None:
                try:
                    child.wait(timeout=3)
                except Exception:
                    self.assertTrue(processes.reap_created_process(child))
            self.assertIsNotNone(child.poll(), "original monitor child must be reaped")
        setup.set_runtime_state(self.rt, "CLOSED")

    def start(self):
        real_spawn = processes.spawn_detached
        def spawn(*args, **kwargs):
            child = real_spawn(*args, **kwargs)
            self.children.append(child)
            return child
        with patch.dict(os.environ, self.environment), patch.object(processes, "spawn_detached", side_effect=spawn):
            return setup._start_monitor(self.harness, self.rt, self.config, self.identity)

    def ready(self):
        startup = self.start()
        return setup._wait_monitor_ready(self.rt, self.identity, startup)

    def test_monitor_reserves_before_spawn_and_publishes_ready_after_first_pass(self) -> None:
        real_spawn = processes.spawn_detached
        def observe_reservation(*args, **kwargs):
            reservation = setup.read_monitor_record(self.rt)
            self.assertEqual("starting", reservation["health"])
            self.assertIsNone(reservation["pid"])
            self.assertIs(reservation["initialized"], False)
            self.assertEqual(reservation["startup_id"], kwargs["env"]["HARNESS_MONITOR_STARTUP_ID"])
            return real_spawn(*args, **kwargs)
        with runtime_admission_lock(self.rt), patch.object(processes, "spawn_detached", side_effect=observe_reservation):
            startup = self.start()
            record = setup.read_monitor_record(self.rt)
            self.assertEqual(self.identity, record["config_identity"])
            self.assertIs(record["initialized"], False)
        ready = setup._wait_monitor_ready(self.rt, self.identity, startup)
        self.assertIs(ready["initialized"], True)
        self.assertEqual("healthy", ready["health"])
        self.assertEqual(startup["startup_id"], ready["startup_id"])
        self.assertTrue(processes.identity_matches(ready["pid"], ready["creation_time"]))
        self.assertEqual(ready, setup.require_monitor_ready(self.rt, self.identity))

    def test_live_ready_monitor_is_reused_without_another_spawn(self) -> None:
        ready = self.ready()
        with patch.object(processes, "spawn_detached") as spawn:
            with self.assertRaisesRegex(ConfigError, setup.SETUP_MONITOR_ALREADY_RUNNING):
                setup._start_monitor(self.harness, self.rt, self.config, self.identity)
        spawn.assert_not_called()
        self.assertEqual(ready["startup_id"], setup.require_monitor_ready(self.rt, self.identity)["startup_id"])

    def test_readiness_gate_rejects_starting_future_stale_naive_and_degraded(self) -> None:
        ready = self.ready()
        invalids = [
            {"initialized": False}, {"health": "starting"}, {"health": "degraded"},
            {"last_heartbeat_at": iso_utc(datetime.now(timezone.utc) + timedelta(seconds=60))},
            {"last_heartbeat_at": iso_utc(datetime.now(timezone.utc) - timedelta(seconds=181))},
            {"last_heartbeat_at": datetime.now().isoformat()},
            {"ready_at": None}, {"ready_at": iso_utc(datetime.now(timezone.utc) + timedelta(seconds=60))},
        ]
        for changed in invalids:
            with self.subTest(changed=changed):
                atomic_write_json(setup.monitor_record_path(self.rt), {**ready, **changed})
                with self.assertRaises(ConfigError):
                    setup.require_monitor_ready(self.rt, self.identity)
        atomic_write_json(setup.monitor_record_path(self.rt), ready)

    def test_failed_first_pass_is_not_ready_and_original_child_is_reaped(self) -> None:
        (self.rt / "CURRENT_EPOCH.json").write_bytes(b"damaged epoch marker")
        startup = self.start()
        with self.assertRaisesRegex(ConfigError, "initialization failed"):
            setup._wait_monitor_ready(self.rt, self.identity, startup)
        self.assertIsNotNone(startup.child.poll())
        self.assertTrue(processes.identity_exit_proven(startup["pid"], startup["creation_time"]))
        record = setup.read_monitor_record(self.rt)
        self.assertIs(record["initialized"], False)
        self.assertIs(record["diagnostics"][0]["cleanup_proven"], True)

    def test_early_child_exit_captures_real_stderr_and_exit(self) -> None:
        argv = [sys.executable, "-c", "import time,sys; time.sleep(0.5); print('owned startup diagnostic',file=sys.stderr,flush=True); sys.exit(7)"]
        with patch.object(processes, "python_argv", return_value=argv):
            startup = self.start()
        with self.assertRaisesRegex(ConfigError, "owned startup diagnostic"):
            setup._wait_monitor_ready(self.rt, self.identity, startup)
        diagnostic = setup.read_monitor_record(self.rt)["diagnostics"][0]
        self.assertEqual(7, diagnostic["child_exit_code"])
        self.assertIs(diagnostic["cleanup_proven"], True)
        self.assertIsNotNone(startup.child.poll())

    def test_publication_failure_reaps_original_child_and_preserves_diagnosis(self) -> None:
        real_write = setup.atomic_write_json
        publications = 0
        def publish(path, value):
            nonlocal publications
            if path == setup.monitor_record_path(self.rt):
                publications += 1
                if publications == 2:
                    raise OSError("injected ownership publication failure")
            return real_write(path, value)
        with patch.object(setup, "atomic_write_json", side_effect=publish):
            with self.assertRaisesRegex(ConfigError, "publication failure"):
                self.start()
        self.assertEqual(1, len(self.children))
        self.assertIsNotNone(self.children[0].poll())
        record = setup.read_monitor_record(self.rt)
        self.assertEqual("STOPPED", record["health"])
        self.assertIs(record["diagnostics"][0]["cleanup_proven"], True)

    def test_another_operation_cannot_adopt_starting_owner(self) -> None:
        with runtime_admission_lock(self.rt):
            startup = self.start()
            with patch.object(processes, "spawn_detached") as spawn:
                with self.assertRaisesRegex(ConfigError, "not initialized"):
                    setup._start_monitor(self.harness, self.rt, self.config, self.identity)
            spawn.assert_not_called()
        setup._wait_monitor_ready(self.rt, self.identity, startup)

    def test_monitor_recovery_waits_for_real_initialized_child(self) -> None:
        real_spawn = processes.spawn_detached
        def spawn(*args, **kwargs):
            child = real_spawn(*args, **kwargs)
            self.children.append(child)
            return child
        with patch.dict(os.environ, self.environment), patch.object(processes, "spawn_detached", side_effect=spawn):
            result = setup.run_monitor_recover(self.harness)
        self.assertTrue(result["ok"], result)
        ready = setup.require_monitor_ready(self.rt, self.identity)
        self.assertIs(ready["initialized"], True)
        # Recovery's Popen handle belongs to its operation; durable cleanup here
        # targets its exact native incarnation, never a synthetic PID.
        from orchestrator_harness.shutdown import _stop_monitor
        record = _stop_monitor(self.rt)
        self.assertTrue(processes.identity_exit_proven(record["pid"], record["creation_time"]))


if __name__ == "__main__":
    unittest.main()
