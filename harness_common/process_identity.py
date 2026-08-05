"""Exact process-incarnation identities for host-only continuity checks."""
from __future__ import annotations

import os
import shlex
from typing import Any


def exact_process_identity(pid: int) -> dict[str, Any] | None:
    """Return PID plus exact creation identity, or None when the OS cannot prove it."""
    if pid <= 0:
        return None
    if os.name == "nt":
        try:
            import ctypes
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.OpenProcess.argtypes = (ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32)
            kernel32.OpenProcess.restype = ctypes.c_void_p
            kernel32.GetProcessTimes.argtypes = (ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p)
            kernel32.GetProcessTimes.restype = ctypes.c_int
            kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)
            kernel32.CloseHandle.restype = ctypes.c_int
            handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
            if not handle:
                return None
            try:
                creation = ctypes.c_ulonglong()
                exit_time = ctypes.c_ulonglong(); kernel = ctypes.c_ulonglong(); user = ctypes.c_ulonglong()
                if not kernel32.GetProcessTimes(handle, ctypes.byref(creation), ctypes.byref(exit_time), ctypes.byref(kernel), ctypes.byref(user)):
                    return None
                return {"pid": pid, "created_utc": f"windows-filetime:{creation.value}"}
            finally:
                kernel32.CloseHandle(handle)
        except (AttributeError, OSError):
            return None
    try:
        stat = open(f"/proc/{pid}/stat", encoding="utf-8").read()
        fields = stat[stat.rfind(")") + 2:].split()
        start_ticks = fields[19]  # /proc/<pid>/stat field 22
        if not start_ticks.isdigit():
            return None
        return {"pid": pid, "created_utc": f"linux-start-ticks:{start_ticks}"}
    except (OSError, IndexError):
        return None


def lane_controller_argv_matches(command_line: str, invocation_path: str) -> bool:
    """Recognize only the exact Python module invocation C3 constructs."""
    if not isinstance(command_line, str) or not command_line or not isinstance(invocation_path, str) or not invocation_path:
        return False
    try:
        if os.name == "nt":
            import ctypes
            argc = ctypes.c_int()
            shell32 = ctypes.windll.shell32
            shell32.CommandLineToArgvW.argtypes = (ctypes.c_wchar_p, ctypes.POINTER(ctypes.c_int))
            shell32.CommandLineToArgvW.restype = ctypes.POINTER(ctypes.c_wchar_p)
            argv_ptr = shell32.CommandLineToArgvW(command_line, ctypes.byref(argc))
            if not argv_ptr:
                return False
            try:
                argv = [argv_ptr[index] for index in range(argc.value)]
            finally:
                ctypes.windll.kernel32.LocalFree(argv_ptr)
        else:
            argv = shlex.split(command_line, posix=True)
    except (AttributeError, OSError, ValueError):
        return False
    return (len(argv) == 4 and isinstance(argv[0], str) and bool(argv[0])
            and argv[1:] == ["-m", "orchestrator_harness.lane_controller", invocation_path])

