"""Distribution and host discovery must not select a developer's paths."""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from harness_common import platform_paths, process_identity
from orchestrator_harness import processes
from orchestrator_harness.config import ConfigError, load_config


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


class PathPortabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        platform_paths._mount_candidates.cache_clear()
        self.addCleanup(platform_paths._mount_candidates.cache_clear)

    def test_shipped_configuration_requires_operator_workspace(self) -> None:
        record = json.loads((REPOSITORY_ROOT / "harness-config.json").read_text())
        self.assertEqual("", record["root_workspace"])
        with self.assertRaises(ConfigError):
            load_config(REPOSITORY_ROOT)

    def test_cim_uses_discovered_powershell_for_snapshot_and_query(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            executable = str(Path(temporary) / "tools" / "powershell.exe")
            runner = Mock(return_value=subprocess.CompletedProcess([], 0, "[]", ""))
            with patch("shutil.which", return_value=executable):
                self.assertTrue(processes.windows_process_snapshot(runner=runner).complete)
                self.assertTrue(processes.windows_process_query(11, runner=runner).complete)
            for call in runner.call_args_list:
                self.assertEqual(executable, call.args[0][0])

    def test_darwin_library_uses_os_discovery(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            library = str(Path(temporary) / "native" / "libproc.dylib")
            native = Mock()
            native.proc_listpids.return_value = 0
            with patch("ctypes.util.find_library", return_value=library), patch(
                "ctypes.CDLL", return_value=native
            ) as loader:
                self.assertEqual((), process_identity.darwin_process_ids())
            loader.assert_called_once_with(library)

    def test_missing_os_tools_are_unknown_not_safe_absence(self) -> None:
        with patch("shutil.which", return_value=None):
            runner = Mock()
            self.assertFalse(processes.windows_process_snapshot(runner=runner).complete)
            self.assertFalse(processes.windows_process_query(11, runner=runner).complete)
            runner.assert_not_called()
            self.assertIsNone(process_identity._linux_process_identity(11))
            self.assertFalse(processes._linux_process_query(11).complete)
        with patch("ctypes.util.find_library", return_value=None), patch("ctypes.CDLL") as loader:
            self.assertIsNone(process_identity.darwin_process_ids())
            loader.assert_not_called()

    def test_linux_reads_discovered_mount_and_preserves_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "kernel-processes"
            (root / "self").mkdir(parents=True)
            (root / "self" / "stat").write_text(f"{os.getpid()} (self) S")
            (root / "stat").write_text("btime 1000\n")
            (root / "11").mkdir()
            fields = ["S", "10", "11", "11", *(["0"] * 15), "500"]
            (root / "11" / "stat").write_text("11 (name with spaces) " + " ".join(fields))
            (root / "11" / "cmdline").write_bytes(b"worker\0--example\0")
            output = json.dumps({"filesystems": [{"target": str(root), "fstype": "proc"}]})
            with patch("shutil.which", return_value="findmnt"), patch(
                "harness_common.platform_paths.subprocess.run",
                return_value=subprocess.CompletedProcess([], 0, output, ""),
            ), patch.object(processes, "_linux_clock_ticks", return_value=100):
                self.assertEqual(root, platform_paths.procfs_root())
                self.assertEqual({"pid": 11, "created_utc": "linux-start-ticks:500"},
                                 process_identity._linux_process_identity(11))
                query = processes._linux_process_query(11)
                self.assertTrue(query.complete, query.errors)
                self.assertEqual(10, query.process.ppid)
                self.assertEqual("worker --example", query.process.command_line)
                self.assertEqual(1005, query.process.created_utc.timestamp())
                self.assertTrue(processes._linux_process_query(99).complete)
                (root / "self" / "stat").unlink()
                self.assertFalse(processes._linux_process_query(99).complete)


if __name__ == "__main__":
    unittest.main()
