"""Regression: provider-start failure must establish exact controller exit.

CHECK-LIVE-11 observes all Windows profiles reaching ``provider_start_failed``
with ``controller_state=exited`` and ``cleanup_proven=true`` while the exact
controller identity stays observable, so the monitor-derived
``controller_exited`` event never appears within the transition window.  The
launch-owned controller handle must be reaped and the exact lane identity
cleared only after terminal cleanup proof *and* actual process exit.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from orchestrator_harness import launch, processes


_CONTROLLER_EVENTS_SCRIPT = r"""
import sys
import time
from pathlib import Path

Path(sys.argv[1]).write_text(
    '{"event_type": "provider_start_failed"}\n', encoding="utf-8"
)
time.sleep(180)
""".strip()


class ProviderStartFailureExitRaceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.runtime = self.root / "runtime"
        self.worktree = self.root / "worktree"
        workspace = self.worktree / ".agent-workspace"
        workspace.mkdir(parents=True)
        binding = (
            self.root
            / "orchestrator_harness"
            / "provider_adapters"
            / "codex"
            / "launcher_binding.py"
        )
        binding.parent.mkdir(parents=True)
        binding.write_text("# binding\n", encoding="utf-8")
        self.lane = {
            "schema": "lane/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "lifecycle": "prepared",
            "process": {},
            "worktree_path": str(self.worktree),
            "controller_status_path": str(workspace / "controller.status.json"),
            "controller_events_path": str(workspace / "controller.events.jsonl"),
        }
        self.invocation = {
            "schema": "controller-invocation/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "provider": {
                "id": "codex",
                "model": "configured-model",
                "launch_config": {"reasoning_effort": "high", "service_tier": "priority"},
            },
        }
        self.terminal_status = {
            "schema": "controller-status/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "controller_state": "exited",
            "provider_state": {"state": "not_started"},
            "cleanup_proven": True,
            "recorded_status": "provider_start_failed",
        }

    def _spawn_controller(self) -> tuple[subprocess.Popen, dict[str, object]]:
        """Spawn a real controller process and record its exact identity.

        The child writes the provider-start-failure event, then stays
        observable just like the live controllers whose status already proved
        terminal cleanup while the process was still detectable.
        """
        child = processes.spawn_detached(
            [
                sys.executable,
                "-c",
                _CONTROLLER_EVENTS_SCRIPT,
                str(Path(self.lane["controller_events_path"])),
            ],
            cwd=str(self.root),
        )
        identity = processes.process_identity(child.pid)
        self.assertIsNotNone(
            identity, "the real controller identity must be recordable"
        )
        assert identity is not None
        self.addCleanup(self._stop_exact, child, identity)
        return child, identity

    @staticmethod
    def _stop_exact(
        child: subprocess.Popen, identity: dict[str, object]
    ) -> None:
        """Terminate the exact incarnation and reap the handle on cleanup."""
        if child.poll() is None and isinstance(identity.get("pid"), int):
            processes.terminate_process(
                identity["pid"], identity.get("creation_time"), force=True
            )
        try:
            child.wait(timeout=10.0)
        except OSError:
            pass

    def _recording_updates(
        self,
    ) -> tuple[callable, list[dict[str, object]]]:
        updates: list[dict[str, object]] = []
        current = dict(self.lane)

        def update(
            _rt: object,
            _epoch: object,
            _lane_id: object,
            mutate: object,
        ) -> dict[str, object]:
            value = mutate(dict(current))
            updates.append(value)
            current.update(value)
            return dict(current)

        return update, updates

    def test_provider_start_failure_proves_exact_exit_then_clears_identity(
        self,
    ) -> None:
        child, identity = self._spawn_controller()
        update, updates = self._recording_updates()
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(
                launch,
                "load_config",
                return_value=SimpleNamespace(runtime_root=self.runtime),
            ),
            patch.object(launch, "read_runtime_state", return_value={"state": "OPEN"}),
            patch.object(
                launch, "find_active_lane", return_value=("epoch-1", self.lane)
            ),
            patch.object(launch, "read_record", return_value=self.invocation),
            patch.object(
                launch,
                "_validate_provider_launch_config",
                return_value=self.invocation["provider"]["launch_config"],
            ),
            patch.object(
                launch.processes, "spawn_detached", return_value=child
            ),
            patch.object(launch, "update_lane", side_effect=update),
            patch.object(
                launch, "_read_controller_status", return_value=self.terminal_status
            ),
            patch.object(launch, "HANDSHAKE_TIMEOUT_SECONDS", 1.0),
        ):
            result = launch.run_launch("lane-1")

        self.assertFalse(result["ok"], result)
        self.assertEqual(launch.LAUNCH_PROVIDER_START_FAILED, result["code"])
        recorded = {
            "pid": identity["pid"],
            "creation_time": identity["creation_time"],
        }
        self.assertEqual(recorded, updates[0]["process"])
        self.assertEqual(
            {},
            updates[-1]["process"],
            "exact controller identity must be cleared only after actual process exit",
        )
        self.assertIsNotNone(
            child.poll(), "the launch-owned controller handle must be reaped"
        )
        self.assertFalse(
            processes.identity_matches(identity["pid"], identity["creation_time"]),
            "the exact controller incarnation must be absent after terminal cleanup proof",
        )

    def test_terminal_facts_never_clear_unprovable_identity(self) -> None:
        child, identity = self._spawn_controller()
        update, updates = self._recording_updates()
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(
                launch,
                "load_config",
                return_value=SimpleNamespace(runtime_root=self.runtime),
            ),
            patch.object(launch, "read_runtime_state", return_value={"state": "OPEN"}),
            patch.object(
                launch, "find_active_lane", return_value=("epoch-1", self.lane)
            ),
            patch.object(launch, "read_record", return_value=self.invocation),
            patch.object(
                launch,
                "_validate_provider_launch_config",
                return_value=self.invocation["provider"]["launch_config"],
            ),
            patch.object(
                launch.processes, "spawn_detached", return_value=child
            ),
            patch.object(launch, "update_lane", side_effect=update),
            patch.object(
                launch, "_read_controller_status", return_value=self.terminal_status
            ),
            patch.object(launch, "HANDSHAKE_TIMEOUT_SECONDS", 1.0),
            patch.object(launch, "RETIRE_CONTROLLER_EXIT_WAIT_SECONDS", 0.2),
            patch.object(launch.processes, "terminate_process", return_value=False),
        ):
            result = launch.run_launch("lane-1")

        self.assertFalse(result["ok"], result)
        self.assertEqual(launch.LAUNCH_PROVIDER_START_FAILED, result["code"])
        self.assertEqual(
            {
                "pid": identity["pid"],
                "creation_time": identity["creation_time"],
            },
            updates[-1]["process"],
            "a live or unprovable controller identity must never be cleared",
        )


if __name__ == "__main__":
    unittest.main()
