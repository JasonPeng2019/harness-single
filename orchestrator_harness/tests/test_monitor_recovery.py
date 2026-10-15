"""Recovery exercises live, exited and unobservable native monitor ownership."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from orchestrator_harness import operator_launch, processes, setup
from orchestrator_harness.config import compute_config_identity, load_config, load_resource_manifest
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.tests.test_v2_materialization import MaterializationFixture


class MonitorRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = MaterializationFixture()
        self.addCleanup(self.fixture.close)
        self.root = self.fixture.harness
        self.config = load_config(self.root)
        self.identity = compute_config_identity(self.config, load_resource_manifest(self.root))
        self.rt = self.config.runtime_root
        setup.set_runtime_state(self.rt, "OPEN")

    def record(self, **overrides):
        with self.fixture.native_setup():
            startup = setup._start_monitor(self.root, self.rt, self.config, self.identity)
        ready = dict(setup._wait_monitor_ready(self.rt, self.identity, startup))
        ready.update(overrides)
        atomic_write_json(setup.monitor_record_path(self.rt), ready)
        return ready

    def call(self):
        with self.fixture.native_setup():
            return setup.run_monitor_recover(self.root)

    def test_public_cli_exposes_monitor_recover(self) -> None:
        args = operator_launch._build_parser().parse_args(["health", "monitor-recover"])
        self.assertEqual((args.command, args.health_command), ("health", "monitor-recover"))
        with patch.object(setup, "run_monitor_recover", return_value={"ok": True}) as recover:
            self.assertEqual(operator_launch._dispatch(args), {"ok": True})
        recover.assert_called_once_with()

    def test_missing_monitor_starts_one_exact_record(self) -> None:
        result = self.call()
        self.assertTrue(result["ok"], result)
        self.assertEqual(setup.MONITOR_RECOVERED, result["code"])
        self.assertEqual(1, len(self.fixture.monitor_children))
        ready = setup.require_monitor_ready(self.rt, self.identity)
        self.assertIs(ready["initialized"], True)
        self.assertEqual(self.fixture.monitor_children[0][0].pid, ready["pid"])

    def test_fresh_live_monitor_is_unchanged(self) -> None:
        before = self.record()
        result = self.call()
        self.assertEqual(setup.MONITOR_HEALTHY, result["code"])
        self.assertEqual(1, len(self.fixture.monitor_children))
        self.assertEqual(before, setup.read_monitor_record(self.rt))

    def test_dead_unstopped_monitor_restarts(self) -> None:
        before = self.record()
        self.assertTrue(processes.terminate_process(before["pid"], before["creation_time"], force=True))
        self.fixture.monitor_children[0][0].wait(timeout=3)
        result = self.call()
        self.assertTrue(result["ok"], result)
        self.assertEqual(setup.MONITOR_RECOVERED, result["code"])
        ready = setup.require_monitor_ready(self.rt, self.identity)
        self.assertNotEqual(before["startup_id"], ready["startup_id"])
        self.assertTrue(processes.identity_exit_proven(before["pid"], before["creation_time"]))

    def test_dead_deliberately_stopped_monitor_is_not_restarted(self) -> None:
        self.record(stop_requested=True)
        self.fixture.monitor_children[0][0].wait(timeout=3)
        result = self.call()
        self.assertEqual(setup.MONITOR_DELIBERATELY_STOPPED, result["code"])
        self.assertEqual(1, len(self.fixture.monitor_children))

    def test_marked_stopped_but_alive_does_not_double_start(self) -> None:
        before = self.record()
        # Own admission prevents the actual child from consuming the stop flag
        # until recovery has observed the exact still-live owner.
        from orchestrator_harness.records import runtime_admission_lock
        with runtime_admission_lock(self.rt):
            atomic_write_json(setup.monitor_record_path(self.rt), {**before, "stop_requested": True})
            result = self.call()
        self.assertEqual(setup.MONITOR_STOP_INCOMPLETE, result["code"])
        self.assertFalse(result["ok"])
        self.assertEqual(1, len(self.fixture.monitor_children))

    def test_stale_live_monitor_is_exactly_stopped_before_replacement(self) -> None:
        before = self.record(last_heartbeat_at="2020-01-01T00:00:00Z")
        real_terminate = processes.terminate_process
        with patch.object(processes, "terminate_process", wraps=real_terminate) as terminate:
            result = self.call()
        self.assertTrue(result["ok"], result)
        self.assertEqual(setup.MONITOR_RECOVERED, result["code"])
        terminate.assert_called_once_with(before["pid"], before["creation_time"], force=True)
        self.assertTrue(processes.identity_exit_proven(before["pid"], before["creation_time"]))
        setup.require_monitor_ready(self.rt, self.identity)

    def test_unproven_hung_cleanup_never_starts_replacement(self) -> None:
        before = self.record(last_heartbeat_at="not-a-time")
        with patch.object(processes, "terminate_process", return_value=False) as terminate:
            result = self.call()
        self.assertEqual(setup.MONITOR_CLEANUP_UNPROVEN, result["code"])
        terminate.assert_called_once_with(before["pid"], before["creation_time"], force=True)
        self.assertEqual(1, len(self.fixture.monitor_children))
        self.assertTrue(processes.identity_matches(before["pid"], before["creation_time"]))

    def test_plain_profile_refuses_automatic_recovery(self) -> None:
        self.fixture._write_json(self.root / "harness-config.json", {
            "root_workspace": str(self.fixture.root_workspace), "managed_coordination": "disabled"})
        result = self.call()
        self.assertEqual(setup.MONITOR_RECOVERY_DISABLED, result["code"])
        self.assertEqual([], self.fixture.monitor_children)

    def test_closed_runtime_refuses_recovery(self) -> None:
        setup.set_runtime_state(self.rt, "CLOSED")
        result = self.call()
        self.assertEqual(setup.MONITOR_RUNTIME_NOT_OPEN, result["code"])
        self.assertEqual([], self.fixture.monitor_children)

    def test_unknown_owner_is_preserved_without_replacement(self) -> None:
        before = self.record()
        with patch.object(processes, "identity_matches", return_value=False), patch.object(processes, "identity_exit_proven", return_value=False):
            result = self.call()
        self.assertEqual(setup.MONITOR_IDENTITY_UNPROVEN, result["code"])
        self.assertEqual(before, setup.read_monitor_record(self.rt))
        self.assertEqual(1, len(self.fixture.monitor_children))


if __name__ == "__main__":
    unittest.main()
