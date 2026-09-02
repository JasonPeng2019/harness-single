"""Cross-platform process identity, liveness, containment, and termination.

Every destructive process operation is bound to a PID and its recorded
creation/start identity.  Provider cleanup uses a controller-owned boundary:
POSIX providers run in a fresh process group/session and Windows providers run
in a Job Object.  A cleanup result is proven only after the boundary's exact
identities are gone; unknown process observations remain unproven.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Sequence, cast

from harness_common.process_identity import (
    darwin_process_ids,
    darwin_process_info,
    exact_process_identity,
)

from .models import (
    ProcessInfo,
    ProcessQuery,
    ProcessSnapshot,
    parse_utc,
)

WINDOWS_CREATE_NO_WINDOW = 0x08000000
WINDOWS_CREATE_SUSPENDED = 0x00000004
BOUNDARY_WAIT_SECONDS = 5.0


def _valid_pid(pid: object) -> bool:
    return isinstance(pid, int) and not isinstance(pid, bool) and pid > 0


def process_identity(pid: int) -> dict[str, Any] | None:
    """Return ``{"pid", "creation_time"}`` or ``None`` when unprovable."""

    identity = exact_process_identity(pid)
    if identity is None:
        return None
    return {"pid": int(identity["pid"]), "creation_time": str(identity["created_utc"])}


def process_alive(pid: int) -> bool:
    """Return whether the operating system still reports a live PID.

    This is only a preliminary liveness probe.  Any ownership or termination
    decision must also compare the exact creation identity.
    """

    if not _valid_pid(pid):
        return False
    if os.name == "nt":
        try:
            import ctypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.OpenProcess.argtypes = (ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32)
            kernel32.OpenProcess.restype = ctypes.c_void_p
            kernel32.GetExitCodeProcess.argtypes = (ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32))
            kernel32.GetExitCodeProcess.restype = ctypes.c_int
            kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)
            kernel32.CloseHandle.restype = ctypes.c_int
            handle = kernel32.OpenProcess(0x1000, False, pid)
            if not handle:
                return False
            try:
                exit_code = ctypes.c_uint32()
                return bool(kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))) and exit_code.value == 259
            finally:
                kernel32.CloseHandle(handle)
        except (AttributeError, OSError):
            return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def identity_matches(pid: int, creation_time: str | None) -> bool:
    """Return whether the recorded PID is the same live process incarnation."""

    if not _valid_pid(pid) or not creation_time:
        return False
    if not process_alive(pid):
        return False
    current = process_identity(pid)
    return current is not None and current["creation_time"] == creation_time


def terminate_process(
    pid: int,
    creation_time: str | None,
    *,
    force: bool = False,
    timeout_seconds: float = BOUNDARY_WAIT_SECONDS,
) -> bool:
    """Terminate one exact process incarnation and verify that it exited.

    A live PID without a creation identity is never targeted.  ``force`` uses
    the platform's hard termination primitive after the caller has already
    supplied the same exact identity.
    """

    if not _valid_pid(pid):
        return True
    if not process_alive(pid):
        return True
    if not creation_time or not identity_matches(pid, creation_time):
        return False
    if os.name == "nt":
        try:
            import ctypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.OpenProcess.argtypes = (ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32)
            kernel32.OpenProcess.restype = ctypes.c_void_p
            kernel32.TerminateProcess.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
            kernel32.TerminateProcess.restype = ctypes.c_int
            kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)
            handle = kernel32.OpenProcess(0x0001, False, pid)  # PROCESS_TERMINATE
            if not handle:
                return False
            try:
                if not kernel32.TerminateProcess(handle, 1):
                    return False
            finally:
                kernel32.CloseHandle(handle)
        except (AttributeError, OSError):
            return False
    else:
        try:
            os.kill(pid, signal.SIGKILL if force else signal.SIGTERM)
        except ProcessLookupError:
            return True
        except OSError:
            return False
    return wait_for_exit(pid, timeout_seconds=timeout_seconds)


def wait_for_exit(pid: int, timeout_seconds: float = BOUNDARY_WAIT_SECONDS) -> bool:
    """Poll until a PID is gone or the bounded wait elapses."""

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if not process_alive(pid):
            return True
        time.sleep(0.1)
    return not process_alive(pid)


def spawn_detached(
    argv: Sequence[str],
    *,
    cwd: str | Path | None = None,
    stdout: Any = subprocess.DEVNULL,
    stderr: Any = subprocess.DEVNULL,
) -> subprocess.Popen[Any]:
    """Start one detached monitor/helper process and return its Popen handle."""

    creationflags = WINDOWS_CREATE_NO_WINDOW if os.name == "nt" else 0
    return subprocess.Popen(
        list(argv),
        cwd=str(cwd) if cwd is not None else None,
        stdout=stdout,
        stderr=stderr,
        stdin=subprocess.DEVNULL,
        creationflags=creationflags,
        close_fds=True,
    )


class _WindowsSuspendedProcess:
    """Small ``Popen``-shaped handle for a natively suspended process."""

    def __init__(self, argv: Sequence[str], process_handle: int, thread_handle: int, pid: int) -> None:
        self.args = list(argv)
        self._handle = process_handle
        self._thread_handle: int | None = thread_handle
        self.pid = pid
        self.returncode: int | None = None

    def resume(self) -> None:
        """Resume the primary thread after its Job Object has been attached."""

        if self._thread_handle is None:
            return
        try:
            import ctypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.ResumeThread.argtypes = (ctypes.c_void_p,)
            kernel32.ResumeThread.restype = ctypes.c_uint32
            if kernel32.ResumeThread(self._thread_handle) == 0xFFFFFFFF:
                raise ctypes.WinError(ctypes.get_last_error())
        finally:
            import _winapi

            _winapi.CloseHandle(self._thread_handle)
            self._thread_handle = None

    def poll(self) -> int | None:
        if self.returncode is not None:
            return self.returncode
        import _winapi

        code = _winapi.GetExitCodeProcess(self._handle)
        if code == 259:  # STILL_ACTIVE
            return None
        self.returncode = int(code)
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        import _winapi

        if self.poll() is None:
            milliseconds = 0xFFFFFFFF if timeout is None else max(0, int(timeout * 1000))
            result = _winapi.WaitForSingleObject(self._handle, milliseconds)
            if result == 0x102:  # WAIT_TIMEOUT
                raise subprocess.TimeoutExpired(self.args, timeout)
            if result == 0xFFFFFFFF:  # WAIT_FAILED
                raise OSError("WaitForSingleObject failed")
        result = self.poll()
        if result is None:
            raise OSError("process signaled without an exit code")
        return result

    def terminate(self) -> None:
        import _winapi

        if self.poll() is None:
            _winapi.TerminateProcess(self._handle, 1)

    kill = terminate

    def close(self) -> None:
        import _winapi

        if self._thread_handle is not None:
            _winapi.CloseHandle(self._thread_handle)
            self._thread_handle = None
        if self._handle is not None:
            _winapi.CloseHandle(self._handle)
            self._handle = None

    def __del__(self) -> None:
        try:
            self.close()
        except (AttributeError, OSError):
            pass


def _spawn_windows_suspended(
    argv: Sequence[str],
    *,
    cwd: str | Path | None,
    stdin: Any,
    stdout: Any,
    stderr: Any,
) -> _WindowsSuspendedProcess:
    """Create a provider suspended until the caller has attached its Job."""

    import _winapi
    import msvcrt

    handles: list[int] = []
    previous_inheritability: dict[int, bool] = {}
    process_handle: int | None = None
    thread_handle: int | None = None
    try:
        for stream in (stdin, stdout, stderr):
            handle = int(msvcrt.get_osfhandle(stream.fileno()))
            if handle == -1:
                raise OSError("provider standard stream has no native handle")
            handles.append(handle)
            if handle not in previous_inheritability:
                previous_inheritability[handle] = os.get_handle_inheritable(handle)
                os.set_handle_inheritable(handle, True)

        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= _winapi.STARTF_USESTDHANDLES
        startupinfo.hStdInput, startupinfo.hStdOutput, startupinfo.hStdError = handles
        process_handle, thread_handle, pid, _tid = _winapi.CreateProcess(
            None,
            subprocess.list2cmdline(list(argv)),
            None,
            None,
            True,
            WINDOWS_CREATE_NO_WINDOW | WINDOWS_CREATE_SUSPENDED,
            None,
            str(cwd) if cwd is not None else None,
            startupinfo,
        )
        return _WindowsSuspendedProcess(argv, process_handle, thread_handle, pid)
    except BaseException:
        if thread_handle is not None:
            _winapi.CloseHandle(thread_handle)
        if process_handle is not None:
            _winapi.CloseHandle(process_handle)
        raise
    finally:
        for handle, inheritable in previous_inheritability.items():
            os.set_handle_inheritable(handle, inheritable)


def spawn_provider(
    argv: Sequence[str],
    *,
    cwd: str | Path,
    stdin: Any,
    stdout: Any,
    stderr: Any,
) -> subprocess.Popen[Any] | _WindowsSuspendedProcess:
    """Start a provider, suspended on Windows until its boundary is complete."""

    if os.name == "nt":
        return _spawn_windows_suspended(
            argv,
            cwd=cwd,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
        )
    return subprocess.Popen(
        list(argv),
        cwd=str(cwd),
        stdin=stdin,
        stdout=stdout,
        stderr=stderr,
        text=True,
        encoding="utf-8",
        errors="replace",
        creationflags=0,
        start_new_session=True,
    )


def python_argv(module: str, *args: str) -> list[str]:
    """Return the argv for ``python -m <module> <args>``."""

    return [sys.executable, "-m", module, *args]


WINDOWS_CIM_SCRIPT = r"""
$ErrorActionPreference='Stop'
@(Get-CimInstance Win32_Process | ForEach-Object {
  [pscustomobject]@{
    pid=[int]$_.ProcessId
    ppid=[int]$_.ParentProcessId
    name=[string]$_.Name
    command_line=[string]$_.CommandLine
    created_utc=if($_.CreationDate){$_.CreationDate.ToUniversalTime().ToString('o')}else{$null}
  }
}) | ConvertTo-Json -Compress -Depth 4
""".strip()


def _windows_cim_identity_script(pid: int) -> str:
    """Return the fixed-shape, integer-filtered known-PID query."""

    return f"""
$ErrorActionPreference='Stop'
@(Get-CimInstance Win32_Process -Filter 'ProcessId = {int(pid)}' | ForEach-Object {{
  [pscustomobject]@{{
    pid=[int]$_.ProcessId
    ppid=[int]$_.ParentProcessId
    name=[string]$_.Name
    command_line=[string]$_.CommandLine
    created_utc=if($_.CreationDate){{$_.CreationDate.ToUniversalTime().ToString('o')}}else{{$null}}
  }}
}}) | ConvertTo-Json -Compress -Depth 4
""".strip()


def windows_process_snapshot(
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    timeout_seconds: float = 12.0,
) -> ProcessSnapshot:
    """Read one bounded Windows process snapshot through CIM."""

    powershell = r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
    argv = [
        powershell,
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        WINDOWS_CIM_SCRIPT,
    ]
    try:
        completed = runner(
            argv,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            check=False,
            creationflags=WINDOWS_CREATE_NO_WINDOW,
        )
    except Exception as exc:
        return ProcessSnapshot(False, (), (f"CIM invocation failed: {exc}",), "windows-cim")
    if completed.returncode != 0:
        return ProcessSnapshot(
            False,
            (),
            (f"CIM returned {completed.returncode}: {completed.stderr.strip()}",),
            "windows-cim",
        )
    try:
        raw = json.loads(completed.stdout.lstrip("\ufeff") or "[]")
        if isinstance(raw, dict):
            raw = [raw]
        if not isinstance(raw, list):
            raise ValueError("CIM JSON root is not a list")
        processes: list[ProcessInfo] = []
        errors: list[str] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            created = parse_utc(item.get("created_utc"))
            if created is None:
                errors.append(f"PID {item.get('pid')!r} lacks a creation identity")
            processes.append(
                ProcessInfo(
                    pid=int(item["pid"]),
                    ppid=int(item.get("ppid", 0)),
                    name=str(item.get("name") or ""),
                    command_line=str(item.get("command_line") or ""),
                    created_utc=created,
                )
            )
        return ProcessSnapshot(
            not errors,
            tuple(sorted(processes, key=lambda p: p.pid)),
            tuple(errors),
            "windows-cim",
        )
    except Exception as exc:
        return ProcessSnapshot(False, (), (f"invalid CIM output: {exc}",), "windows-cim")


def windows_process_query(
    pid: int,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    timeout_seconds: float = 12.0,
) -> ProcessQuery:
    """Query one known Windows PID without taking a full inventory."""

    if not _valid_pid(pid):
        return ProcessQuery(True, None, ("PID is invalid",))
    powershell = r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
    argv = [
        powershell,
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        _windows_cim_identity_script(pid),
    ]
    try:
        completed = runner(
            argv,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            check=False,
            creationflags=WINDOWS_CREATE_NO_WINDOW,
        )
    except Exception as exc:
        return ProcessQuery(False, None, (f"CIM identity query failed: {exc}",))
    if completed.returncode != 0:
        return ProcessQuery(
            False,
            None,
            (f"CIM identity query returned {completed.returncode}: {completed.stderr.strip()}",),
        )
    try:
        raw = json.loads(completed.stdout.lstrip("\ufeff") or "[]")
        if isinstance(raw, dict):
            raw = [raw]
        if not isinstance(raw, list):
            raise ValueError("CIM JSON root is not a list")
        if not raw:
            return ProcessQuery(True, None)
        item = raw[0]
        if not isinstance(item, dict):
            raise ValueError("CIM identity record is not an object")
        if int(item["pid"]) != pid:
            raise ValueError("CIM identity record PID does not match the requested PID")
        created = parse_utc(item.get("created_utc"))
        return ProcessQuery(
            created is not None,
            ProcessInfo(
                pid=int(item["pid"]),
                ppid=int(item.get("ppid", 0)),
                name=str(item.get("name") or ""),
                command_line=str(item.get("command_line") or ""),
                created_utc=created,
            ),
            ("process creation time is unavailable",) if created is None else (),
        )
    except Exception as exc:
        return ProcessQuery(False, None, (f"invalid CIM identity output: {exc}",))


def _linux_boot_time() -> datetime:
    for line in Path("/proc/stat").read_text(encoding="ascii").splitlines():
        if line.startswith("btime "):
            return datetime.fromtimestamp(int(line.split()[1]), tz=timezone.utc)
    raise RuntimeError("/proc/stat has no btime")


def _linux_clock_ticks() -> int:
    sysconf = getattr(os, "sysconf", None)
    if not callable(sysconf):
        raise RuntimeError("os.sysconf is unavailable")
    read_sysconf = cast(Callable[[str], int], sysconf)
    return int(read_sysconf("SC_CLK_TCK"))


def _linux_process_query(
    pid: int,
    *,
    boot: datetime | None = None,
    ticks: int | None = None,
) -> ProcessQuery:
    if not _valid_pid(pid):
        return ProcessQuery(True, None, ("PID is invalid",))
    try:
        boot = boot or _linux_boot_time()
        ticks = ticks or _linux_clock_ticks()
        entry = Path("/proc") / str(pid)
        stat_text = (entry / "stat").read_text(encoding="ascii")
        close = stat_text.rfind(")")
        fields = stat_text[close + 2 :].split()
        if close < 0 or len(fields) <= 19:
            raise ValueError("/proc stat record is incomplete")
        ppid = int(fields[1])
        process_group_id = int(fields[2])
        session_id = int(fields[3])
        start_ticks = int(fields[19])
        name = stat_text[stat_text.find("(") + 1 : close]
        raw_cmd = (entry / "cmdline").read_bytes().replace(b"\0", b" ").strip()
        command = raw_cmd.decode("utf-8", errors="replace")
        created = boot + timedelta(seconds=start_ticks / ticks)
        return ProcessQuery(
            True,
            ProcessInfo(
                pid,
                ppid,
                name,
                command,
                created,
                process_group_id=process_group_id,
                session_id=session_id,
            ),
        )
    except (FileNotFoundError, ProcessLookupError):
        return ProcessQuery(True, None)
    except PermissionError as exc:
        return ProcessQuery(False, None, (f"/proc/{pid}: {exc}",))
    except Exception as exc:
        return ProcessQuery(False, None, (f"/proc/{pid}: {exc}",))


def _darwin_process_query(pid: int) -> ProcessQuery:
    if not _valid_pid(pid):
        return ProcessQuery(True, None, ("PID is invalid",))
    info = darwin_process_info(pid)
    if info is None:
        if not process_alive(pid):
            return ProcessQuery(True, None)
        return ProcessQuery(False, None, ("libproc process identity is unavailable",))
    return ProcessQuery(
        True,
        ProcessInfo(
            pid=pid,
            ppid=int(info["ppid"]),
            name=str(info["name"]),
            command_line=str(info["command_line"]),
            created_utc=cast(datetime, info["created_utc"]),
            process_group_id=int(info["pgid"]) if info.get("pgid") else None,
            session_id=(
                int(info["session_id"])
                if info.get("session_id") is not None
                else None
            ),
        ),
    )


def targeted_process_query(
    pid: int,
    *,
    expected_parent_pid: int | None = None,
) -> ProcessQuery:
    """Query one known PID through the current platform's native provider."""

    if os.name == "nt":
        query = windows_process_query(pid)
    elif sys.platform == "darwin":
        query = _darwin_process_query(pid)
    elif sys.platform.startswith("linux"):
        query = _linux_process_query(pid)
    else:
        return ProcessQuery(False, None, ("unsupported process platform",))
    if expected_parent_pid is not None and query.parent_matches(expected_parent_pid) is False:
        return ProcessQuery(
            query.complete,
            query.process,
            (*query.errors, f"PID {pid} parent does not match {expected_parent_pid}"),
        )
    return query


def linux_process_snapshot() -> ProcessSnapshot:
    """Read one Linux ``/proc`` process snapshot."""

    errors: list[str] = []
    processes: list[ProcessInfo] = []
    try:
        boot = _linux_boot_time()
        ticks = _linux_clock_ticks()
    except Exception as exc:
        return ProcessSnapshot(False, (), (f"/proc setup failed: {exc}",), "linux-proc")
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        query = _linux_process_query(int(entry.name), boot=boot, ticks=ticks)
        if query.process is not None:
            processes.append(query.process)
        if not query.complete:
            errors.extend(query.errors)
        elif query.process is None:
            errors.append(f"{entry}: process disappeared during observation")
    return ProcessSnapshot(
        complete=not errors,
        processes=tuple(sorted(processes, key=lambda p: p.pid)),
        errors=tuple(errors),
        provider="linux-proc",
    )


def darwin_process_snapshot() -> ProcessSnapshot:
    """Read one macOS ``libproc`` process snapshot without using ``/proc``."""

    pids = darwin_process_ids()
    if pids is None:
        return ProcessSnapshot(False, (), ("libproc process inventory is unavailable",), "darwin-libproc")
    errors: list[str] = []
    processes: list[ProcessInfo] = []
    for pid in pids:
        query = _darwin_process_query(pid)
        if query.process is not None:
            processes.append(query.process)
        elif not query.complete:
            errors.extend(query.errors)
    return ProcessSnapshot(
        complete=not errors,
        processes=tuple(sorted(processes, key=lambda p: p.pid)),
        errors=tuple(errors),
        provider="darwin-libproc",
    )


def process_snapshot() -> ProcessSnapshot:
    """Read one complete process snapshot through the current platform path."""

    if os.name == "nt":
        return windows_process_snapshot()
    if sys.platform == "darwin":
        return darwin_process_snapshot()
    if sys.platform.startswith("linux"):
        return linux_process_snapshot()
    return ProcessSnapshot(False, (), ("unsupported process platform",), "unsupported")


def _identity_key(pid: int, creation_time: str) -> tuple[int, str]:
    return pid, creation_time


class ProcessBoundary:
    """Controller-owned provider/helper process boundary.

    The boundary remembers exact identities observed while the provider lives.
    A POSIX process group/session supplies containment for descendants; a
    Windows Job Object supplies kernel containment.  The object is also
    serializable so a force-stop route can continue exact cleanup after a
    controller has exited.
    """

    def __init__(
        self,
        root_pid: int,
        root_creation_time: str | None,
        *,
        root_process: ProcessInfo | None = None,
        process_group_id: int | None = None,
        session_id: int | None = None,
        boundary_kind: str | None = None,
        snapshot_provider: Callable[[], ProcessSnapshot] | None = None,
    ) -> None:
        self.root_pid = root_pid
        self.root_creation_time = root_creation_time
        self.root_process = root_process
        self.process_group_id = process_group_id
        self.session_id = session_id
        self.boundary_kind = boundary_kind or (
            "windows-job" if os.name == "nt" else "posix-process-group"
        )
        self.snapshot_provider = snapshot_provider or process_snapshot
        self._owned: dict[tuple[int, str], ProcessInfo | None] = {}
        self.errors: list[str] = []
        if not _valid_pid(root_pid):
            self.errors.append("provider root PID is invalid")
        if not isinstance(root_creation_time, str) or not root_creation_time:
            self.errors.append("provider root creation identity is unavailable")
        if _valid_pid(root_pid) and isinstance(root_creation_time, str) and root_creation_time:
            self._owned[_identity_key(root_pid, root_creation_time)] = root_process
        self._job_handle: Any = None
        self._last_observation_complete = not self.errors

    @classmethod
    def for_process(
        cls,
        pid: int,
        *,
        snapshot_provider: Callable[[], ProcessSnapshot] | None = None,
    ) -> "ProcessBoundary":
        identity = process_identity(pid)
        query = targeted_process_query(pid) if identity is not None else None
        process = query.process if query is not None and query.complete else None
        return cls(
            pid,
            identity["creation_time"] if identity is not None else None,
            root_process=process,
            process_group_id=process.process_group_id if process else None,
            session_id=process.session_id if process else None,
            snapshot_provider=snapshot_provider,
        )

    @classmethod
    def from_record(
        cls,
        record: dict[str, Any],
        *,
        snapshot_provider: Callable[[], ProcessSnapshot] | None = None,
    ) -> "ProcessBoundary":
        root = record.get("root") if isinstance(record.get("root"), dict) else {}
        boundary = cls(
            root.get("pid"),
            root.get("creation_time"),
            process_group_id=record.get("process_group_id"),
            session_id=record.get("session_id"),
            boundary_kind=record.get("kind"),
            snapshot_provider=snapshot_provider,
        )
        for item in record.get("processes", []):
            if not isinstance(item, dict) or not _valid_pid(item.get("pid")):
                boundary.errors.append("recorded boundary identity is malformed")
                continue
            creation = item.get("creation_time")
            if not isinstance(creation, str) or not creation:
                boundary.errors.append("recorded boundary creation identity is missing")
                continue
            boundary._owned[_identity_key(item["pid"], creation)] = None
        return boundary

    def attach_windows_process_handle(self, handle: Any) -> bool:
        """Attach a Windows provider to a kill-on-close Job Object."""

        if os.name != "nt":
            return True
        try:
            import ctypes

            class IoCounters(ctypes.Structure):
                _fields_ = [(name, ctypes.c_uint64) for name in (
                    "read_operations", "write_operations", "other_operations",
                    "read_bytes", "write_bytes", "other_bytes",
                )]

            class BasicLimits(ctypes.Structure):
                _fields_ = [
                    ("per_process_user_time", ctypes.c_int64),
                    ("per_job_user_time", ctypes.c_int64),
                    ("limit_flags", ctypes.c_uint32),
                    ("minimum_working_set_size", ctypes.c_void_p),
                    ("maximum_working_set_size", ctypes.c_void_p),
                    ("active_process_limit", ctypes.c_uint32),
                    ("affinity", ctypes.c_void_p),
                    ("priority_class", ctypes.c_uint32),
                    ("scheduling_class", ctypes.c_uint32),
                ]

            class ExtendedLimits(ctypes.Structure):
                _fields_ = [
                    ("basic", BasicLimits),
                    ("io", IoCounters),
                    ("process_memory_limit", ctypes.c_void_p),
                    ("job_memory_limit", ctypes.c_void_p),
                    ("peak_process_memory_used", ctypes.c_void_p),
                    ("peak_job_memory_used", ctypes.c_void_p),
                ]

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.CreateJobObjectW.argtypes = (ctypes.c_void_p, ctypes.c_wchar_p)
            kernel32.CreateJobObjectW.restype = ctypes.c_void_p
            kernel32.SetInformationJobObject.argtypes = (
                ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32
            )
            kernel32.SetInformationJobObject.restype = ctypes.c_int
            kernel32.AssignProcessToJobObject.argtypes = (ctypes.c_void_p, ctypes.c_void_p)
            kernel32.AssignProcessToJobObject.restype = ctypes.c_int
            job = kernel32.CreateJobObjectW(None, None)
            if not job:
                return False
            limits = ExtendedLimits()
            limits.basic.limit_flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            if not kernel32.SetInformationJobObject(
                job, 9, ctypes.byref(limits), ctypes.sizeof(limits)
            ) or not kernel32.AssignProcessToJobObject(job, handle):
                kernel32.CloseHandle(job)
                return False
            self._job_handle = job
            self.boundary_kind = "windows-job"
            return True
        except (AttributeError, OSError):
            return False

    def _close_job(self) -> None:
        if self._job_handle is None or os.name != "nt":
            return
        try:
            import ctypes

            ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle(self._job_handle)
        except (AttributeError, OSError):
            pass
        finally:
            self._job_handle = None

    def _job_process_ids(self) -> set[int] | None:
        """Return the current native Job Object membership, or unknown."""

        if self._job_handle is None or os.name != "nt":
            return set()
        try:
            import ctypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.QueryInformationJobObject.argtypes = (
                ctypes.c_void_p,
                ctypes.c_int,
                ctypes.c_void_p,
                ctypes.c_uint32,
                ctypes.POINTER(ctypes.c_uint32),
            )
            kernel32.QueryInformationJobObject.restype = ctypes.c_int
            pointer_size = ctypes.sizeof(ctypes.c_void_p)
            size = 8 + pointer_size
            while True:
                buffer = ctypes.create_string_buffer(size)
                returned = ctypes.c_uint32()
                if kernel32.QueryInformationJobObject(
                    self._job_handle,
                    3,  # JobObjectBasicProcessIdList
                    ctypes.byref(buffer),
                    size,
                    ctypes.byref(returned),
                ):
                    count = ctypes.c_uint32.from_buffer(buffer, 4).value
                    required = 8 + count * pointer_size
                    if required > size:
                        size = required
                        continue
                    ids = []
                    for offset in range(8, required, pointer_size):
                        value = int.from_bytes(
                            buffer.raw[offset : offset + pointer_size],
                            byteorder=sys.byteorder,
                        )
                        if value:
                            ids.append(value)
                    return set(ids)
                if returned.value <= size:
                    return None
                size = returned.value
        except (AttributeError, OSError, ValueError):
            return None

    def _observe_job(self) -> bool:
        pids = self._job_process_ids()
        if pids is None:
            self.errors.append("Windows Job Object membership is unavailable")
            self._last_observation_complete = False
            return False
        for pid in pids:
            identity = process_identity(pid)
            if identity is None:
                if process_alive(pid):
                    self.errors.append(f"Job Object PID {pid} identity is unavailable")
                    self._last_observation_complete = False
                continue
            creation = identity["creation_time"]
            self._owned[_identity_key(pid, creation)] = None
            if pid == self.root_pid and creation != self.root_creation_time:
                self.errors.append(f"root PID {pid} creation identity was reused")
                self._last_observation_complete = False
        self._last_observation_complete = not self.errors
        return self._last_observation_complete

    def __del__(self) -> None:
        self._close_job()

    def _snapshot_identity(self, item: ProcessInfo) -> str | None:
        if item.created_utc is None:
            self.errors.append(f"PID {item.pid} lacks a snapshot creation identity")
            return None
        identity = process_identity(item.pid)
        if identity is None:
            if process_alive(item.pid):
                self.errors.append(f"PID {item.pid} identity is unavailable")
            return None
        return identity["creation_time"]

    def observe(self) -> bool:
        """Observe and retain exact members of the provider boundary."""

        if self._job_handle is not None:
            return self._observe_job()
        snapshot = self.snapshot_provider()
        if not isinstance(snapshot, ProcessSnapshot) or not snapshot.complete:
            self._last_observation_complete = False
            self.errors.extend(
                list(getattr(snapshot, "errors", ("process snapshot is incomplete",)))
            )
            return False
        by_pid = snapshot.by_pid
        root = by_pid.get(self.root_pid)
        if root is not None:
            root_identity = self._snapshot_identity(root)
            if root_identity != self.root_creation_time:
                self.errors.append(f"root PID {self.root_pid} creation identity was reused")
                self._last_observation_complete = False
                return False
            if self.process_group_id is None:
                self.process_group_id = root.process_group_id
            if self.session_id is None:
                self.session_id = root.session_id
        selected: list[ProcessInfo] = []
        known_pids = {pid for pid, _ in self._owned}
        for item in snapshot.processes:
            in_boundary = (
                self.process_group_id is not None
                and item.process_group_id == self.process_group_id
            ) or (
                self.session_id is not None and item.session_id == self.session_id
            )
            if item.pid in known_pids or in_boundary:
                selected.append(item)
        changed = True
        while changed:
            changed = False
            selected_pids = {item.pid for item in selected}
            owned_current_pids = {
                item.pid
                for item in selected
                if self._snapshot_identity(item) is not None
            }
            for item in snapshot.processes:
                if item.pid in selected_pids or item.ppid not in owned_current_pids:
                    continue
                selected.append(item)
                changed = True
        for item in selected:
            creation = self._snapshot_identity(item)
            if creation is not None:
                self._owned[_identity_key(item.pid, creation)] = item
        self._last_observation_complete = not self.errors
        return self._last_observation_complete

    def record(self) -> dict[str, Any]:
        """Return the exact identities needed by a later force-stop route."""

        return {
            "kind": self.boundary_kind,
            "root": {"pid": self.root_pid, "creation_time": self.root_creation_time},
            "process_group_id": self.process_group_id,
            "session_id": self.session_id,
            "processes": [
                {"pid": pid, "creation_time": creation}
                for pid, creation in sorted(self._owned)
            ],
        }

    def _remaining(self) -> list[tuple[int, str]] | None:
        remaining: list[tuple[int, str]] = []
        for pid, creation in self._owned:
            if not process_alive(pid):
                continue
            current = process_identity(pid)
            if current is None:
                return None
            if current["creation_time"] == creation:
                remaining.append((pid, creation))
        return remaining

    def cleanup(self, *, force: bool = False, timeout_seconds: float = BOUNDARY_WAIT_SECONDS) -> bool:
        """Terminate every recorded/contained member and prove the boundary gone."""

        if not self.root_creation_time:
            self.errors.append("provider root creation identity is unavailable")
            return False
        if self._job_handle is not None:
            try:
                import ctypes

                if not self._observe_job():
                    return False
                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                kernel32.TerminateJobObject.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
                kernel32.TerminateJobObject.restype = ctypes.c_int
                if not kernel32.TerminateJobObject(self._job_handle, 1):
                    return False
                if not self._wait_job_empty(timeout_seconds):
                    return False
            except (AttributeError, OSError):
                return False
            self._close_job()
            return self._wait_exact_members(timeout_seconds)

        self.observe()
        if not self._last_observation_complete:
            return False
        members = list(self._owned)
        members.sort(key=lambda value: value[0] == self.root_pid)
        for pid, creation in members:
            terminate_process(
                pid,
                creation,
                force=force,
                timeout_seconds=min(timeout_seconds, 1.0),
            )
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            self.observe()
            if not self._last_observation_complete:
                return False
            remaining = self._remaining()
            if remaining is None:
                return False
            if not remaining:
                return True
            for pid, creation in remaining:
                terminate_process(pid, creation, force=True, timeout_seconds=0.5)
            time.sleep(0.1)
        self.observe()
        return self._last_observation_complete and self._remaining() == []

    def _wait_job_empty(self, timeout_seconds: float) -> bool:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            pids = self._job_process_ids()
            if pids is None:
                return False
            if not pids:
                return True
            if not self._observe_job():
                return False
            time.sleep(0.1)
        pids = self._job_process_ids()
        return pids == set()

    def _wait_exact_members(self, timeout_seconds: float) -> bool:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            remaining = self._remaining()
            if remaining == []:
                return True
            if remaining is None:
                return False
            time.sleep(0.1)
        return self._remaining() == []


def cleanup_recorded_process_boundary(
    record: dict[str, Any],
    *,
    force: bool = True,
    timeout_seconds: float = BOUNDARY_WAIT_SECONDS,
) -> bool:
    """Force-clean one serialized exact process boundary."""

    return ProcessBoundary.from_record(record).cleanup(
        force=force,
        timeout_seconds=timeout_seconds,
    )


def process_boundary_is_gone(record: dict[str, Any]) -> bool:
    """Prove a serialized boundary is absent without terminating anything."""

    boundary = ProcessBoundary.from_record(record)
    if not boundary.root_creation_time or not boundary.observe():
        return False
    return boundary._remaining() == []
