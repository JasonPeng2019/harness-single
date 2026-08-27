"""Exact process-incarnation identities for host-only continuity checks."""

from __future__ import annotations

import os
from typing import Any


def exact_process_identity(pid: int) -> dict[str, Any] | None:
    """Return PID plus exact creation identity, or None when the OS cannot prove it."""
    if pid <= 0:
        return None
    if os.name == "nt":
        try:
            import ctypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.OpenProcess.argtypes = (
                ctypes.c_uint32,
                ctypes.c_int,
                ctypes.c_uint32,
            )
            kernel32.OpenProcess.restype = ctypes.c_void_p
            kernel32.GetProcessTimes.argtypes = (
                ctypes.c_void_p,
                ctypes.c_void_p,
                ctypes.c_void_p,
                ctypes.c_void_p,
                ctypes.c_void_p,
            )
            kernel32.GetProcessTimes.restype = ctypes.c_int
            kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)
            kernel32.CloseHandle.restype = ctypes.c_int
            handle = kernel32.OpenProcess(
                0x1000, False, pid
            )  # PROCESS_QUERY_LIMITED_INFORMATION
            if not handle:
                return None
            try:
                creation = ctypes.c_ulonglong()
                exit_time = ctypes.c_ulonglong()
                kernel = ctypes.c_ulonglong()
                user = ctypes.c_ulonglong()
                if not kernel32.GetProcessTimes(
                    handle,
                    ctypes.byref(creation),
                    ctypes.byref(exit_time),
                    ctypes.byref(kernel),
                    ctypes.byref(user),
                ):
                    return None
                return {"pid": pid, "created_utc": f"windows-filetime:{creation.value}"}
            finally:
                kernel32.CloseHandle(handle)
        except (AttributeError, OSError):
            return None
    try:
        stat = open(f"/proc/{pid}/stat", encoding="utf-8").read()
        fields = stat[stat.rfind(")") + 2 :].split()
        start_ticks = fields[19]  # /proc/<pid>/stat field 22
        if not start_ticks.isdigit():
            return None
        return {"pid": pid, "created_utc": f"linux-start-ticks:{start_ticks}"}
    except (OSError, IndexError):
        return None
