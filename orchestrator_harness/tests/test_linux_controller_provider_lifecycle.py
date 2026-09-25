"""Linux controller survival and exact recovery after an abrupt controller exit."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from orchestrator_harness import launch, processes
from orchestrator_harness.models import ProcessInfo, ProcessQuery
from orchestrator_harness.records import atomic_write_json, read_record


@unittest.skipUnless(sys.platform.startswith("linux"), "Linux process lifecycle")
class LinuxControllerProviderLifecycleTests(unittest.TestCase):
    def test_detached_controller_has_its_own_session(self) -> None:
        child = processes.spawn_detached(
            [sys.executable, "-c", "import time; time.sleep(20)"]
        )
        try:
            self.assertEqual(child.pid, os.getsid(child.pid))
            self.assertEqual(child.pid, os.getpgid(child.pid))
        finally:
            child.kill()
            child.wait(timeout=5)

    def test_exited_unreaped_process_is_not_a_live_boundary_member(self) -> None:
        child = subprocess.Popen([sys.executable, "-c", "pass"])
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                stat = Path(f"/proc/{child.pid}/stat").read_text(encoding="ascii")
                if stat[stat.rfind(")") + 2] == "Z":
                    break
                time.sleep(0.01)
            else:
                self.fail("child did not reach exited, unreaped state")
            self.assertFalse(processes.process_alive(child.pid))
        finally:
            child.wait(timeout=5)

    def test_non_ascii_proc_name_does_not_hide_live_identity(self) -> None:
        with (
            patch.object(processes.os, "kill"),
            patch.object(processes.Path, "read_text", side_effect=UnicodeDecodeError(
                "ascii", b"\xff", 0, 1, "non-ASCII process name"
            )),
            patch.object(processes.Path, "read_bytes", return_value=b"41001 (\xff) R 1 1"),
        ):
            self.assertTrue(processes.process_alive(41001))

    def test_vanished_unrelated_proc_entry_does_not_block_exact_boundary_cleanup(self) -> None:
        created = datetime(2026, 9, 25, tzinfo=timezone.utc)
        identity = "2026-09-25T00:00:00+00:00"
        root_pid = 41001
        vanished_pid = 42002
        root = ProcessInfo(
            root_pid, 1, "provider", "provider", created,
            process_group_id=root_pid, session_id=root_pid,
        )
        live = {root_pid: identity}
        record = {
            "kind": "posix-process-group",
            "root": {"pid": root_pid, "creation_time": identity},
            "process_group_id": root_pid,
            "session_id": root_pid,
            "processes": [{"pid": root_pid, "creation_time": identity}],
        }

        def query(pid, **_kwargs):
            return ProcessQuery(True, root if pid == root_pid and pid in live else None)

        def terminate(pid, creation, **_kwargs):
            self.assertEqual((root_pid, identity), (pid, creation))
            live.pop(pid)
            return True

        with (
            patch.object(processes.Path, "iterdir", return_value=[
                Path(f"/proc/{root_pid}"), Path(f"/proc/{vanished_pid}")
            ]),
            patch.object(processes, "_linux_boot_time", return_value=created),
            patch.object(processes, "_linux_clock_ticks", return_value=100),
            patch.object(processes, "_linux_process_query", side_effect=query),
            patch.object(processes, "process_alive", side_effect=lambda pid: pid in live),
            patch.object(
                processes, "process_identity",
                side_effect=lambda pid: (
                    {"pid": pid, "creation_time": live[pid]} if pid in live else None
                ),
            ),
            patch.object(processes, "terminate_process", side_effect=terminate) as stop,
        ):
            self.assertTrue(processes.cleanup_recorded_process_boundary(record))
        stop.assert_called_once()

    def test_live_unreadable_proc_entry_still_blocks_boundary_cleanup(self) -> None:
        with (
            patch.object(processes.Path, "iterdir", return_value=[Path("/proc/42002")]),
            patch.object(
                processes, "_linux_boot_time",
                return_value=datetime(2026, 9, 25, tzinfo=timezone.utc),
            ),
            patch.object(processes, "_linux_clock_ticks", return_value=100),
            patch.object(processes, "_linux_process_query", return_value=ProcessQuery(True, None)),
            patch.object(processes, "process_alive", return_value=True),
        ):
            self.assertFalse(processes.linux_process_snapshot().complete)

    def test_force_stop_reconciles_durable_status_after_controller_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary) / "worktree" / ".agent-workspace"
            workspace.mkdir(parents=True)
            status_path = workspace / "controller.status.json"
            boundary = {
                "kind": "posix-process-group",
                "root": {"pid": 41001, "creation_time": "provider-created"},
                "process_group_id": 41001,
                "session_id": 41001,
                "processes": [{"pid": 41001, "creation_time": "provider-created"}],
            }
            lane = {
                "lane_id": "lane-1", "run_id": "run-1", "lifecycle": "running",
                "process": {"pid": 40001, "creation_time": "controller-created"},
                "worktree_path": str(workspace.parent),
                "controller_status_path": str(status_path),
            }
            atomic_write_json(status_path, {
                "schema": "controller-status/v1", "lane_id": "lane-1", "run_id": "run-1",
                "controller_state": "running",
                "provider_state": {"state": "running", "pid": 41001,
                                   "creation_time": "provider-created"},
                "process_boundary": boundary, "cleanup_proven": False,
            })
            retired = []

            def update(_rt, _epoch, _lane_id, mutate):
                retired.append(mutate(dict(lane)))

            with (
                patch.object(launch, "find_harness_root", return_value=Path(temporary)),
                patch.object(launch, "load_config", return_value=SimpleNamespace(
                    runtime_root=Path(temporary), root_workspace=Path(temporary)
                )),
                patch.object(launch, "find_active_lane", return_value=("epoch-1", lane)),
                patch.object(launch.processes, "terminate_process", return_value=True) as stop_controller,
                patch.object(launch.processes, "cleanup_recorded_process_boundary", return_value=True) as stop_boundary,
                patch.object(launch, "force_release_leases"),
                patch.object(launch, "update_lane", side_effect=update),
                patch.object(launch, "read_active_lanes", return_value=[{"lane_id": "lane-1"}]),
                patch.object(launch, "write_active_lanes"),
                patch.object(launch, "_prune_worktrees"),
            ):
                result = launch.run_force_stop("lane-1")

            self.assertEqual("FORCE_STOP_OK", result["code"], result)
            stop_controller.assert_called_once_with(
                40001, "controller-created", force=True
            )
            stop_boundary.assert_called_once_with(boundary)
            self.assertEqual("retired", retired[0]["lifecycle"])
            status = read_record(status_path, "controller-status/v1")
            self.assertEqual("exited", status["controller_state"])
            self.assertEqual("exited", status["provider_state"]["state"])
            self.assertTrue(status["cleanup_proven"])


if __name__ == "__main__":
    unittest.main()
