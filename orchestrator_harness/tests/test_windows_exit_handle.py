"""Termination proof uses the exact process object even with retained handles."""
from __future__ import annotations

import ctypes
import os
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch

from harness_common import process_identity as common
from orchestrator_harness import processes
from orchestrator_harness.models import ProcessQuery


@unittest.skipUnless(os.name == "nt", "Windows native process handles")
class WindowsExitHandleTests(unittest.TestCase):
    def test_exited_owned_child_is_proven_with_retained_popen_handle_and_no_wmi(self):
        child = processes.spawn_detached([sys.executable, "-B", "-c", "import time; time.sleep(.1)"],
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            identity = processes.process_identity(child.pid)
            self.assertIsNotNone(identity)
            child.communicate(timeout=10)
            self.assertEqual(0, child.returncode)
            with patch.object(processes, "targeted_process_query", return_value=ProcessQuery(False, None)) as query:
                self.assertTrue(processes.identity_exit_proven(child.pid, identity["creation_time"]))
                query.assert_not_called()
        finally:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=10)

    def test_exact_live_self_is_not_exit_even_if_inventory_claims_absence(self):
        identity = processes.process_identity(os.getpid())
        with patch.object(processes, "targeted_process_query", return_value=ProcessQuery(True, None)) as query:
            self.assertFalse(processes.identity_exit_proven(identity["pid"], identity["creation_time"]))
            query.assert_not_called()

    def test_different_exact_creation_does_not_adopt_live_self(self):
        identity = processes.process_identity(os.getpid())
        old_creation = f"windows-filetime:{int(identity['creation_time'].split(':')[1]) - 100000}"
        with patch.object(processes, "targeted_process_query") as query:
            self.assertTrue(processes.identity_exit_proven(identity["pid"], old_creation))
            query.assert_not_called()
        self.assertTrue(processes.identity_matches(identity["pid"], identity["creation_time"]))

    def test_dword_overflow_cannot_wrap_to_an_existing_process(self):
        identity = processes.process_identity(os.getpid())
        invalid = 2**32 + identity["pid"]
        with patch.object(common, "_windows_process_identity") as query:
            self.assertIsNone(common.exact_process_identity(invalid))
            query.assert_not_called()
        with patch.object(processes, "_windows_identity_exit_proven") as probe:
            self.assertFalse(processes.identity_exit_proven(invalid, identity["creation_time"]))
            self.assertFalse(processes.terminate_process(invalid, identity["creation_time"], force=True))
            probe.assert_not_called()

    def test_denied_native_handle_remains_unknown(self):
        identity = processes.process_identity(os.getpid())
        library = Mock()
        library.OpenProcess.return_value = None
        with (patch.object(ctypes, "WinDLL", return_value=library),
              patch.object(ctypes, "get_last_error", return_value=5)):
            self.assertIsNone(processes._windows_identity_exit_proven(identity["pid"], identity["creation_time"]))
        library.CloseHandle.assert_not_called()

    def test_valid_native_pid_missing_from_kernel_is_absent(self):
        library = Mock()
        library.OpenProcess.return_value = None
        with (patch.object(ctypes, "WinDLL", return_value=library),
              patch.object(ctypes, "get_last_error", return_value=87)):
            self.assertTrue(processes._windows_identity_exit_proven(123, "windows-filetime:134361000000000000"))
        library.CloseHandle.assert_not_called()
