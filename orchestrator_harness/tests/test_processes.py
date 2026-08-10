from __future__ import annotations
# pyright: reportImplicitRelativeImport=false

import json
import subprocess
import unittest

from datetime import datetime, timezone

from orchestrator_harness.models import ProcessInfo, ProcessSnapshot
from orchestrator_harness.processes import (
    WINDOWS_CIM_SCRIPT,
    windows_process_query,
    windows_process_snapshot,
)


class ProcessProviderTests(unittest.TestCase):
    def test_snapshot_indexes_once_and_supports_direct_parent_queries(self) -> None:
        created = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)
        snapshot = ProcessSnapshot(
            True,
            (
                ProcessInfo(10, 1, "controller", "controller", created),
                ProcessInfo(11, 10, "child", "child", created),
            ),
            (),
            "fake",
        )

        index = snapshot.by_pid
        self.assertIs(index, snapshot.by_pid)
        self.assertIs(index, snapshot.pid_index)
        self.assertIs(snapshot.process_for(11), index[11])
        self.assertTrue(snapshot.parent_matches(11, 10))
        self.assertFalse(snapshot.parent_matches(11, 1))
        self.assertIsNone(
            ProcessSnapshot(False, snapshot.processes, ("partial",), "linux-proc")
            .parent_matches(99, 10)
        )

    def test_windows_known_pid_query_does_not_use_full_inventory(self) -> None:
        captured = {}

        def runner(argv, **kwargs):
            captured["argv"] = argv
            payload = {
                "pid": 11,
                "ppid": 10,
                "name": "child.exe",
                "command_line": "child",
                "created_utc": "2026-07-30T12:00:00Z",
            }
            return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

        query = windows_process_query(11, runner=runner)
        self.assertTrue(query.complete)
        self.assertEqual(11, query.process.pid if query.process else None)
        self.assertIn("ProcessId = 11", captured["argv"][-1])
        self.assertNotIn("Get-CimInstance Win32_Process | ForEach-Object", captured["argv"][-1])

    def test_windows_provider_uses_only_fixed_command(self) -> None:
        captured = {}

        def runner(argv, **kwargs):
            captured["argv"] = argv
            captured["kwargs"] = kwargs
            payload = [
                {
                    "pid": 10,
                    "ppid": 1,
                    "name": "python.exe",
                    "command_line": "python worker.py",
                    "created_utc": "2026-07-30T12:00:00Z",
                }
            ]
            return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

        snapshot = windows_process_snapshot(runner=runner)
        self.assertTrue(snapshot.complete)
        self.assertEqual(10, snapshot.processes[0].pid)
        self.assertEqual(WINDOWS_CIM_SCRIPT, captured["argv"][-1])
        self.assertEqual("-Command", captured["argv"][-2])
        self.assertNotIn("shell", captured["kwargs"])

    def test_windows_provider_fails_unknown_on_cim_error(self) -> None:
        def runner(argv, **kwargs):
            return subprocess.CompletedProcess(argv, 7, "", "CIM unavailable")

        snapshot = windows_process_snapshot(runner=runner)
        self.assertFalse(snapshot.complete)
        self.assertIn("CIM returned 7", snapshot.errors[0])

    def test_windows_provider_preserves_missing_creation_time(self) -> None:
        def runner(argv, **kwargs):
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps(
                    {
                        "pid": 10,
                        "ppid": 1,
                        "name": "x",
                        "command_line": "",
                        "created_utc": None,
                    }
                ),
                "",
            )

        snapshot = windows_process_snapshot(runner=runner)
        self.assertTrue(snapshot.complete)
        self.assertIsNone(snapshot.processes[0].created_utc)
        self.assertTrue(snapshot.errors)


if __name__ == "__main__":
    unittest.main()
