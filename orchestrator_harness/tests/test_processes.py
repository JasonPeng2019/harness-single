from __future__ import annotations
# pyright: reportImplicitRelativeImport=false

import json
import subprocess
import unittest

from orchestrator_harness.processes import WINDOWS_CIM_SCRIPT, windows_process_snapshot


class ProcessProviderTests(unittest.TestCase):
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
