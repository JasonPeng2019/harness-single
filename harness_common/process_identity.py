"""Exact process-incarnation identities for host-only continuity checks.

The public helper deliberately returns ``None`` when the operating system cannot
prove the process identity.  Callers must treat that as unknown, never as a
safe absence.  The three supported live paths use the platform's native process
identity source: Windows process times, macOS ``libproc``, and Linux ``/proc``.
"""

from __future__ import annotations

import ctypes
import os
import sys
from datetime import datetime, timezone
from typing import Any


def _valid_pid(pid: object) -> bool:
    return isinstance(pid, int) and not isinstance(pid, bool) and pid > 0


def _windows_process_identity(pid: int) -> dict[str, Any] | None:
    try:
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
        handle = kernel32.OpenProcess(0x1000, False, pid)  # QUERY_LIMITED_INFORMATION
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


class _DarwinProcBsdInfo(ctypes.Structure):
    """The stable prefix of macOS ``struct proc_bsdinfo``."""

    _fields_ = [
        ("flags", ctypes.c_uint32),
        ("status", ctypes.c_uint32),
        ("xstatus", ctypes.c_uint32),
        ("pid", ctypes.c_uint32),
        ("ppid", ctypes.c_uint32),
        ("uid", ctypes.c_uint32),
        ("gid", ctypes.c_uint32),
        ("ruid", ctypes.c_uint32),
        ("rgid", ctypes.c_uint32),
        ("svuid", ctypes.c_uint32),
        ("svgid", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
        ("comm", ctypes.c_char * 16),
        ("name", ctypes.c_char * 32),
        ("nfiles", ctypes.c_uint32),
        ("pgid", ctypes.c_uint32),
        ("pjobc", ctypes.c_uint32),
        ("tdev", ctypes.c_uint32),
        ("tpgid", ctypes.c_uint32),
        ("nice", ctypes.c_int32),
        ("start_tvsec", ctypes.c_uint64),
        ("start_tvusec", ctypes.c_uint64),
    ]


def _darwin_proc_bsd_info(pid: int) -> _DarwinProcBsdInfo | None:
    try:
        libproc = ctypes.CDLL("/usr/lib/libproc.dylib")
        libproc.proc_pidinfo.argtypes = (
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        )
        libproc.proc_pidinfo.restype = ctypes.c_int
        info = _DarwinProcBsdInfo()
        size = ctypes.sizeof(info)
        received = libproc.proc_pidinfo(pid, 3, 0, ctypes.byref(info), size)
        if received < size:
            return None
        return info
    except (AttributeError, OSError):
        return None


def darwin_process_info(pid: int) -> dict[str, Any] | None:
    """Return native macOS process details, or ``None`` when unavailable."""

    info = _darwin_proc_bsd_info(pid)
    if info is None or info.pid != pid or info.start_tvsec <= 0:
        return None
    try:
        created = datetime.fromtimestamp(
            info.start_tvsec + info.start_tvusec / 1_000_000,
            tz=timezone.utc,
        )
    except (OverflowError, OSError, ValueError):
        return None
    try:
        session_id = os.getsid(pid)
    except (AttributeError, OSError, PermissionError):
        session_id = None
    name = bytes(info.comm).split(b"\0", 1)[0].decode("utf-8", errors="replace")
    return {
        "pid": pid,
        "ppid": int(info.ppid),
        "pgid": int(info.pgid),
        "session_id": session_id,
        "name": name,
        "command_line": name,
        "created_utc": created,
        "creation_identity": f"darwin-start-time:{info.start_tvsec}:{info.start_tvusec}",
    }


def darwin_process_ids() -> tuple[int, ...] | None:
    """Return the macOS process inventory, or ``None`` when unavailable."""

    try:
        libproc = ctypes.CDLL("/usr/lib/libproc.dylib")
        libproc.proc_listpids.argtypes = (
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_int,
        )
        libproc.proc_listpids.restype = ctypes.c_int
        required = libproc.proc_listpids(1, 0, None, 0)  # PROC_ALL_PIDS
        if required < 0:
            return None
        count = max(1, (required + ctypes.sizeof(ctypes.c_uint32) - 1) // 4)
        pids = (ctypes.c_uint32 * count)()
        received = libproc.proc_listpids(1, 0, ctypes.byref(pids), ctypes.sizeof(pids))
        if received < 0 or received % ctypes.sizeof(ctypes.c_uint32):
            return None
        return tuple(sorted({int(pid) for pid in pids[: received // 4] if pid}))
    except (AttributeError, OSError):
        return None


def _linux_process_identity(pid: int) -> dict[str, Any] | None:
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as handle:
            stat = handle.read()
        close = stat.rfind(")")
        fields = stat[close + 2 :].split()
        start_ticks = fields[19]  # /proc/<pid>/stat field 22
        if not start_ticks.isdigit():
            return None
        return {"pid": pid, "created_utc": f"linux-start-ticks:{start_ticks}"}
    except (OSError, IndexError):
        return None


def exact_process_identity(pid: int) -> dict[str, Any] | None:
    """Return PID plus exact creation identity, or ``None`` when unprovable."""

    if not _valid_pid(pid):
        return None
    if os.name == "nt":
        return _windows_process_identity(pid)
    if sys.platform == "darwin":
        info = darwin_process_info(pid)
        if info is None:
            return None
        return {"pid": pid, "created_utc": str(info["creation_identity"])}
    if sys.platform.startswith("linux"):
        return _linux_process_identity(pid)
    return None
