"""Exact, reusable child cleanup for coding lane controllers."""
from __future__ import annotations

import ctypes
import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Mapping

from .models import ProcessBoundaryInventory, ProcessInfo, ProcessQuery, ProcessSnapshot, iso_utc, parse_utc
from .processes import process_group_inventory, process_snapshot, targeted_process_query, windows_process_query


PROCESS_CLEANUP_SCHEMA = "orchestrator-process-cleanup/v1"
PROCESS_BOUNDARY_SCHEMA = "orchestrator-process-boundary/v1"


class ProcessBoundaryUnsupported(RuntimeError):
    """The host cannot establish or enumerate a complete owned boundary."""


def _windows_job_api() -> tuple[Any, ...]:
    if os.name != "nt":
        raise ProcessBoundaryUnsupported("Windows Job Objects are unavailable")
    import ctypes.wintypes as wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create = kernel32.CreateJobObjectW
    create.argtypes = [wintypes.HANDLE, wintypes.LPCWSTR]
    create.restype = wintypes.HANDLE
    assign = kernel32.AssignProcessToJobObject
    assign.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    assign.restype = wintypes.BOOL
    set_info = kernel32.SetInformationJobObject
    set_info.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD]
    set_info.restype = wintypes.BOOL
    query = kernel32.QueryInformationJobObject
    query.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
    query.restype = wintypes.BOOL
    terminate = kernel32.TerminateJobObject
    terminate.argtypes = [wintypes.HANDLE, wintypes.UINT]
    terminate.restype = wintypes.BOOL
    close = kernel32.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    close.restype = wintypes.BOOL
    ntdll = ctypes.WinDLL("ntdll", use_last_error=True)
    resume = ntdll.NtResumeProcess
    resume.argtypes = [wintypes.HANDLE]
    resume.restype = ctypes.c_long
    return create, assign, set_info, query, terminate, close, resume, wintypes


class ProcessBoundary:
    """A controller-owned kernel/session boundary with complete inventory evidence."""

    def __init__(self, *, kind: str, identity: str | None = None) -> None:
        self.kind = kind
        self.identity = identity
        self._job_handle: Any = None
        self._group_id: int | None = None
        self._root_pid: int | None = None
        self._history: dict[tuple[int, str], ProcessInfo] = {}
        self._inventory_errors: list[str] = []
        self._closed = False
        self._cleanup_result: str | None = None

    @classmethod
    def prepare(cls) -> "ProcessBoundary":
        if os.name == "nt":
            boundary = cls(kind="windows-job")
            try:
                create, _, set_info, _, _, close, _, wintypes = _windows_job_api()
                handle = create(None, None)
                if not handle or handle == ctypes.c_void_p(-1).value:
                    raise ProcessBoundaryUnsupported(f"CreateJobObjectW failed ({ctypes.get_last_error()})")

                class _BasicLimitInformation(ctypes.Structure):
                    _fields_ = [
                        ("PerProcessUserTimeLimit", ctypes.c_longlong),
                        ("PerJobUserTimeLimit", ctypes.c_longlong),
                        ("LimitFlags", wintypes.DWORD),
                        ("MinimumWorkingSetSize", ctypes.c_size_t),
                        ("MaximumWorkingSetSize", ctypes.c_size_t),
                        ("ActiveProcessLimit", wintypes.DWORD),
                        ("Affinity", ctypes.c_size_t),
                        ("PriorityClass", wintypes.DWORD),
                        ("SchedulingClass", wintypes.DWORD),
                    ]

                class _IoCounters(ctypes.Structure):
                    _fields_ = [("ReadOperationCount", ctypes.c_ulonglong), ("WriteOperationCount", ctypes.c_ulonglong), ("OtherOperationCount", ctypes.c_ulonglong), ("ReadTransferCount", ctypes.c_ulonglong), ("WriteTransferCount", ctypes.c_ulonglong), ("OtherTransferCount", ctypes.c_ulonglong)]

                class _ExtendedLimitInformation(ctypes.Structure):
                    _fields_ = [
                        ("BasicLimitInformation", _BasicLimitInformation),
                        ("IoInfo", _IoCounters),
                        ("ProcessMemoryLimit", ctypes.c_size_t),
                        ("JobMemoryLimit", ctypes.c_size_t),
                        ("PeakProcessMemoryUsed", ctypes.c_size_t),
                        ("PeakJobMemoryUsed", ctypes.c_size_t),
                    ]

                limits = _ExtendedLimitInformation()
                limits.BasicLimitInformation.LimitFlags = 0x00002000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
                if not set_info(handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
                    error = ctypes.get_last_error()
                    close(handle)
                    raise ProcessBoundaryUnsupported(f"SetInformationJobObject failed ({error})")
                boundary._job_handle = handle
                boundary.identity = f"job:{int(handle):x}"
                return boundary
            except ProcessBoundaryUnsupported:
                raise
            except Exception as exc:
                if boundary._job_handle is not None:
                    try:
                        _windows_job_api()[5](boundary._job_handle)
                    except Exception:
                        pass
                raise ProcessBoundaryUnsupported(f"Windows Job Object setup failed: {exc}") from exc
        if os.name == "posix" and os.path.isdir("/proc"):
            return cls(kind="linux-process-group")
        raise ProcessBoundaryUnsupported("no complete process boundary is supported on this host")

    @property
    def popen_kwargs(self) -> dict[str, Any]:
        if self.kind == "windows-job":
            return {"creationflags": 0x00000004}  # CREATE_SUSPENDED
        if self.kind == "linux-process-group":
            return {"start_new_session": True}
        raise ProcessBoundaryUnsupported("process boundary has no launch contract")

    def attach(self, process: Any, identity: ProcessInfo) -> None:
        if self._closed:
            raise ProcessBoundaryUnsupported("process boundary is already closed")
        pid = getattr(process, "pid", None)
        if not isinstance(pid, int) or pid <= 0:
            raise ProcessBoundaryUnsupported("provider PID is invalid")
        if self.kind == "windows-job":
            if self._job_handle is None:
                raise ProcessBoundaryUnsupported("Windows Job Object handle is unavailable")
            _, assign, _, _, _, _, resume, wintypes = _windows_job_api()
            process_handle = getattr(process, "_handle", None)
            if process_handle is None or not assign(self._job_handle, wintypes.HANDLE(process_handle)):
                raise ProcessBoundaryUnsupported(f"AssignProcessToJobObject failed ({ctypes.get_last_error()})")
            status = resume(wintypes.HANDLE(process_handle))
            if status != 0:
                raise ProcessBoundaryUnsupported(f"NtResumeProcess failed ({status})")
            self._root_pid = pid
        elif self.kind == "linux-process-group":
            try:
                self._group_id = os.getpgid(pid)
            except OSError as exc:
                raise ProcessBoundaryUnsupported(f"process group identity unavailable: {exc}") from exc
            self.identity = f"pgid:{self._group_id}"
            self._root_pid = pid
        else:
            raise ProcessBoundaryUnsupported("unknown process boundary kind")
        self._record(identity)

    def _record(self, item: ProcessInfo) -> None:
        if item.created_utc is not None:
            self._history[(item.pid, iso_utc(item.created_utc) or "")] = item

    @property
    def history(self) -> tuple[ProcessInfo, ...]:
        return tuple(sorted(self._history.values(), key=lambda item: (item.pid, iso_utc(item.created_utc) or "")))

    def inventory(self) -> ProcessBoundaryInventory:
        if self._closed:
            return ProcessBoundaryInventory(False, self.kind, self.identity, errors=("boundary is closed",), source="controller")
        if self.kind == "windows-job":
            try:
                _, _, _, query, _, _, _, wintypes = _windows_job_api()
                capacity = 64
                while capacity <= 65536:
                    size = 8 + ctypes.sizeof(ctypes.c_void_p) * capacity
                    buffer = ctypes.create_string_buffer(size)
                    returned = wintypes.DWORD()
                    if query(self._job_handle, 3, buffer, size, ctypes.byref(returned)):
                        count = int.from_bytes(buffer.raw[:4], "little")
                        ids_count = int.from_bytes(buffer.raw[4:8], "little")
                        if ids_count > capacity:
                            capacity = ids_count + 16
                            continue
                        pointer_size = ctypes.sizeof(ctypes.c_void_p)
                        pids = [
                            int.from_bytes(buffer.raw[8 + index * pointer_size:8 + (index + 1) * pointer_size], "little")
                            for index in range(ids_count)
                        ]
                        snapshot = process_snapshot()
                        if not snapshot.complete:
                            self._inventory_errors.extend(snapshot.errors)
                            return ProcessBoundaryInventory(False, self.kind, self.identity, errors=tuple(snapshot.errors), source="job-object+CIM")
                        by_pid = snapshot.by_pid
                        member_pids = set(pids)
                        # CIM's Job Object list is authoritative for
                        # containment.  The complete snapshot also closes the
                        # short observation gap for descendants created after
                        # the first job query; every such descendant must be
                        # admitted to the same exact ownership set.
                        changed = True
                        while changed:
                            changed = False
                            for item in snapshot.processes:
                                if item.ppid in member_pids and item.pid not in member_pids:
                                    member_pids.add(item.pid)
                                    changed = True
                        members: list[ProcessInfo] = []
                        errors: list[str] = []
                        history_pids = {item.pid for item in self.history}
                        for pid in sorted(member_pids):
                            item = by_pid.get(pid)
                            if item is None and os.name == "nt":
                                direct = windows_process_query(pid)
                                item = direct.process if direct.complete else None
                            if item is None or item.created_utc is None:
                                if item is None and pid in history_pids:
                                    # A job can retain a recently reaped PID
                                    # for one query.  It is safe to classify
                                    # that member absent only after this
                                    # boundary observed its exact creation
                                    # identity earlier.
                                    continue
                                errors.append(f"job member {pid} is absent or lacks creation identity")
                            else:
                                marked = ProcessInfo(item.pid, item.ppid, item.name, item.command_line, item.created_utc, item.process_group_id, item.session_id, self.identity)
                                members.append(marked)
                                self._record(marked)
                        if errors or count != ids_count:
                            failure_errors = tuple(errors or ("job inventory count is inconsistent",))
                            self._inventory_errors.extend(failure_errors)
                            return ProcessBoundaryInventory(False, self.kind, self.identity, tuple(members), self.history, failure_errors, "job-object+CIM")
                        if self._inventory_errors:
                            return ProcessBoundaryInventory(False, self.kind, self.identity, tuple(sorted(members, key=lambda item: item.pid)), self.history, tuple(self._inventory_errors), "job-object+CIM")
                        return ProcessBoundaryInventory(True, self.kind, self.identity, tuple(sorted(members, key=lambda item: item.pid)), self.history, (), "job-object+CIM")
                    if ctypes.get_last_error() not in {24, 122}:  # insufficient buffer variants
                        break
                    capacity *= 2
                error = f"QueryInformationJobObject failed ({ctypes.get_last_error()})"
                self._inventory_errors.append(error)
                return ProcessBoundaryInventory(False, self.kind, self.identity, errors=(error,), source="job-object")
            except ProcessBoundaryUnsupported as exc:
                self._inventory_errors.append(str(exc))
                return ProcessBoundaryInventory(False, self.kind, self.identity, errors=(str(exc),), source="job-object")
            except Exception as exc:
                self._inventory_errors.append(f"job inventory failed: {exc}")
                return ProcessBoundaryInventory(False, self.kind, self.identity, errors=(f"job inventory failed: {exc}",), source="job-object")
        if self.kind == "linux-process-group" and self._group_id is not None:
            inventory = process_group_inventory(self._group_id, boundary_identity=self.identity)
            for item in inventory.processes:
                marked = ProcessInfo(item.pid, item.ppid, item.name, item.command_line, item.created_utc, item.process_group_id, item.session_id, self.identity)
                self._record(marked)
            if inventory.errors:
                self._inventory_errors.extend(inventory.errors)
            return ProcessBoundaryInventory(inventory.complete and not self._inventory_errors, inventory.boundary_kind, inventory.boundary_identity, inventory.processes, self.history, tuple(self._inventory_errors) or inventory.errors, inventory.source, self._cleanup_result)
        return ProcessBoundaryInventory(False, self.kind, self.identity, errors=("boundary identity is unavailable",), source="controller")

    def terminate_owned(self) -> str:
        if self._closed:
            return "already-closed"
        if self.kind == "windows-job":
            if self._job_handle is None:
                return "job-handle-unavailable"
            try:
                _, _, _, _, terminate, _, _, _ = _windows_job_api()
                if not terminate(self._job_handle, 1):
                    self._cleanup_result = f"job-termination-failed:{ctypes.get_last_error()}"
                else:
                    self._cleanup_result = "job-terminated"
            except Exception as exc:
                self._cleanup_result = f"job-termination-failed:{exc}"
            return self._cleanup_result
        if self.kind == "linux-process-group" and self._group_id is not None:
            try:
                os.killpg(self._group_id, signal.SIGTERM)
                self._cleanup_result = "process-group-terminated"
            except ProcessLookupError:
                self._cleanup_result = "process-group-absent"
            except OSError as exc:
                self._cleanup_result = f"process-group-termination-failed:{exc}"
            return self._cleanup_result
        self._cleanup_result = "boundary-identity-unavailable"
        return self._cleanup_result

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self.kind == "windows-job" and self._job_handle is not None:
            try:
                _windows_job_api()[5](self._job_handle)
            except Exception:
                pass
            self._job_handle = None

    def to_record(self, inventory: ProcessBoundaryInventory | None = None) -> dict[str, Any]:
        observed = inventory or self.inventory()
        return {
            "schema": PROCESS_BOUNDARY_SCHEMA,
            "kind": self.kind,
            "identity": self.identity,
            "complete": observed.complete,
            "inventory_source": observed.source,
            "errors": list(observed.errors),
            "cleanup": observed.cleanup or self._cleanup_result,
            "root_pid": self._root_pid,
            "group_id": self._group_id,
            "members": [
                {"pid": item.pid, "created_utc": iso_utc(item.created_utc), "name": item.name}
                for item in observed.observed_processes
            ],
            "live_members": [
                {"pid": item.pid, "created_utc": iso_utc(item.created_utc), "name": item.name}
                for item in observed.processes
            ],
        }


def _identity_from(value: ProcessInfo | Mapping[str, Any] | None) -> ProcessInfo | None:
    if isinstance(value, ProcessInfo):
        return value if value.pid > 0 and value.created_utc is not None else None
    if not isinstance(value, Mapping):
        return None
    pid = value.get("pid")
    ppid = value.get("ppid", 0)
    created = parse_utc(value.get("created_utc"))
    if (
        not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0
        or not isinstance(ppid, int) or isinstance(ppid, bool)
        or created is None
    ):
        return None
    return ProcessInfo(pid, ppid, str(value.get("name") or ""), str(value.get("command_line") or ""), created)


@dataclass(frozen=True)
class CleanupResult:
    """Typed evidence for every cleanup stage and the final reap proof."""

    pid: int
    expected_created_utc: str | None
    status: str
    stages: tuple[str, ...] = ()
    terminate_attempted: bool = False
    kill_attempted: bool = False
    final_reap: bool = False
    cleanup_confirmed: bool = False
    identity_verified: bool = False
    identity_uncertain: bool = False
    exit_code: int | None = None
    reaped_after: str | None = None
    errors: tuple[str, ...] = ()

    @property
    def proved_reap(self) -> bool:
        return self.cleanup_confirmed and self.final_reap and not self.identity_uncertain

    def to_record(self) -> dict[str, Any]:
        record: dict[str, Any] = {
            "schema": PROCESS_CLEANUP_SCHEMA,
            "pid": self.pid,
            "provider_pid": self.pid,
            "codex_pid": self.pid,
            "expected_created_utc": self.expected_created_utc,
            "status": self.status,
            "stages": list(self.stages),
            "terminate_attempted": self.terminate_attempted,
            "kill_attempted": self.kill_attempted,
            "final_reap": self.final_reap,
            "cleanup_confirmed": self.cleanup_confirmed,
            "identity_verified": self.identity_verified,
            "identity_uncertain": self.identity_uncertain,
            "exit_code": self.exit_code,
            "reaped_after": self.reaped_after,
            "errors": list(self.errors),
        }
        return record


@dataclass
class ProcessSupervisor:
    """Own one Popen handle and prove reap before a caller releases resources."""

    process: Any
    identity: ProcessInfo | Mapping[str, Any] | None
    graceful_timeout_seconds: float = 5.0
    force_timeout_seconds: float | None = None
    parent_pid: int | None = None
    observer: Callable[..., ProcessQuery] | None = targeted_process_query
    boundary: ProcessBoundary | None = None
    _identity_record: ProcessInfo | None = field(init=False, repr=False)
    _reaped: bool = field(init=False, default=False, repr=False)
    _exit_code: int | None = field(init=False, default=None, repr=False)
    _result: CleanupResult | None = field(init=False, default=None, repr=False)

    def __post_init__(self) -> None:
        if self.graceful_timeout_seconds < 0:
            raise ValueError("graceful timeout cannot be negative")
        if self.force_timeout_seconds is None:
            self.force_timeout_seconds = self.graceful_timeout_seconds
        if self.force_timeout_seconds < 0:
            raise ValueError("force timeout cannot be negative")
        self._identity_record = _identity_from(self.identity)

    @property
    def pid(self) -> int:
        value = getattr(self.process, "pid", 0)
        return int(value) if isinstance(value, int) and not isinstance(value, bool) else 0

    @property
    def creation_identity(self) -> str | None:
        return iso_utc(self._identity_record.created_utc) if self._identity_record is not None else None

    def _observation(self) -> tuple[bool, str | None]:
        """Return whether external exact observation corroborates the bound child."""
        identity = self._identity_record
        if identity is None or identity.pid != self.pid:
            return False, "owned process creation identity is unavailable"
        if self.observer is None:
            return True, None
        try:
            query = self.observer(self.pid, expected_parent_pid=self.parent_pid)
        except TypeError:
            query = self.observer(self.pid)
        except BaseException as exc:
            return False, f"exact process observation failed: {type(exc).__name__}: {exc}"
        if not isinstance(query, ProcessQuery):
            return False, "exact process observation returned an invalid result"
        if not query.complete:
            # An incomplete inventory is uncertainty, not safe absence.  The
            # owned handle is not enough to release an external claim when
            # creation identity cannot be independently revalidated.
            return False, "exact process observation is incomplete"
        if query.process is None:
            try:
                if self.process.poll() is None:
                    return False, "exact observation says child is absent while owned handle is live"
            except BaseException as exc:
                return False, f"cannot reconcile absent child observation: {type(exc).__name__}: {exc}"
            return True, None
        if query.process.pid != identity.pid:
            return False, "observed child PID does not match owned Popen PID"
        if query.process.created_utc != identity.created_utc:
            return False, "observed child creation identity does not match owned Popen identity"
        if self.parent_pid is not None and query.process.ppid != self.parent_pid:
            return False, "observed child parent does not match controller identity"
        return True, None

    def _result_for(
        self,
        *,
        status: str,
        stages: list[str],
        terminate_attempted: bool,
        kill_attempted: bool,
        final_reap: bool,
        identity_verified: bool,
        identity_uncertain: bool,
        reaped_after: str | None,
        errors: list[str],
    ) -> CleanupResult:
        return CleanupResult(
            pid=self.pid,
            expected_created_utc=self.creation_identity,
            status=status,
            stages=tuple(stages),
            terminate_attempted=terminate_attempted,
            kill_attempted=kill_attempted,
            final_reap=final_reap,
            cleanup_confirmed=final_reap and not identity_uncertain,
            identity_verified=identity_verified,
            identity_uncertain=identity_uncertain,
            exit_code=self._exit_code,
            reaped_after=reaped_after,
            errors=tuple(errors),
        )

    def wait_for_exit(self) -> int:
        """Wait normally and bind the observed exit to the owned handle."""
        if self._reaped:
            return int(self._exit_code or 0)
        identity_ok, reason = self._observation()
        if not identity_ok:
            raise RuntimeError(reason or "child identity is uncertain")
        if self.boundary is None:
            result = self.process.wait()
        else:
            # Polling is deliberate: it records every real member observed in
            # the controller-owned boundary, including helpers that exit before
            # the direct worker does.
            while True:
                result = self.process.poll()
                if result is not None:
                    break
                self.boundary.inventory()
                time.sleep(0.05)
            result = self.process.wait()
        self._exit_code = int(result) if isinstance(result, int) else None
        self._reaped = True
        return int(result)

    def boundary_inventory(self) -> ProcessBoundaryInventory:
        if self.boundary is None:
            return ProcessBoundaryInventory(
                False,
                "none",
                None,
                errors=("no controller-owned process boundary is attached",),
                source="controller",
            )
        return self.boundary.inventory()

    def terminate_owned_boundary(self) -> str:
        if self.boundary is None:
            return "no-boundary"
        return self.boundary.terminate_owned()

    def cleanup(self) -> CleanupResult:
        """Gracefully stop, force stop if needed, then perform a final reap."""
        if self._result is not None:
            return self._result
        stages: list[str] = []
        errors: list[str] = []
        terminate_attempted = False
        kill_attempted = False
        identity_ok, observation_note = self._observation()
        identity_uncertain = not identity_ok or self._identity_record is None
        identity_verified = identity_ok and self._identity_record is not None
        if observation_note is not None:
            errors.append(observation_note)
        if self._identity_record is None or self.pid <= 0:
            self._result = self._result_for(
                status="IDENTITY_UNCERTAIN", stages=stages,
                terminate_attempted=False, kill_attempted=False, final_reap=False,
                identity_verified=False, identity_uncertain=True, reaped_after=None, errors=errors,
            )
            return self._result
        if not identity_ok:
            self._result = self._result_for(
                status="IDENTITY_UNCERTAIN", stages=stages,
                terminate_attempted=False, kill_attempted=False, final_reap=False,
                identity_verified=False, identity_uncertain=True, reaped_after=None, errors=errors,
            )
            return self._result
        try:
            already_exited = self.process.poll()
        except BaseException as exc:
            already_exited = None
            errors.append(f"initial poll failed: {type(exc).__name__}: {exc}")
        if already_exited is not None:
            stages.append("ALREADY_EXITED")
            try:
                self._exit_code = self.process.wait(timeout=0)
                self._reaped = True
                stages.append("FINAL_REAP")
                self._result = self._result_for(
                    status="REAPED", stages=stages,
                    terminate_attempted=False, kill_attempted=False, final_reap=True,
                    identity_verified=identity_verified, identity_uncertain=identity_uncertain,
                    reaped_after="already_exited", errors=errors,
                )
                return self._result
            except BaseException as exc:
                errors.append(f"already-exited reap failed: {type(exc).__name__}: {exc}")

        stages.append("GRACEFUL_STOP_REQUESTED")
        terminate_attempted = True
        try:
            self.process.terminate()
        except BaseException as exc:
            errors.append(f"graceful stop failed: {type(exc).__name__}: {exc}")
        try:
            self._exit_code = self.process.wait(timeout=self.graceful_timeout_seconds)
            self._reaped = True
            stages.append("GRACEFUL_WAIT")
            stages.append("FINAL_REAP")
            self._result = self._result_for(
                status="REAPED", stages=stages,
                terminate_attempted=terminate_attempted, kill_attempted=False, final_reap=True,
                identity_verified=identity_verified, identity_uncertain=identity_uncertain,
                reaped_after="graceful_stop", errors=errors,
            )
            return self._result
        except subprocess.TimeoutExpired:
            stages.append("GRACEFUL_WAIT_TIMEOUT")
        except BaseException as exc:
            errors.append(f"graceful wait failed: {type(exc).__name__}: {exc}")

        stages.append("FORCE_STOP_REQUESTED")
        kill_attempted = True
        try:
            self.process.kill()
        except BaseException as exc:
            errors.append(f"force stop failed: {type(exc).__name__}: {exc}")
        try:
            self._exit_code = self.process.wait(timeout=self.force_timeout_seconds)
            self._reaped = True
            stages.append("FINAL_REAP")
            self._result = self._result_for(
                status="REAPED", stages=stages,
                terminate_attempted=terminate_attempted, kill_attempted=kill_attempted, final_reap=True,
                identity_verified=identity_verified, identity_uncertain=identity_uncertain,
                reaped_after="force_stop", errors=errors,
            )
            return self._result
        except subprocess.TimeoutExpired:
            stages.append("FINAL_REAP_TIMEOUT")
        except BaseException as exc:
            errors.append(f"final reap failed: {type(exc).__name__}: {exc}")
        self._result = self._result_for(
            status="IDENTITY_UNCERTAIN" if identity_uncertain else "UNREAPED",
            stages=stages, terminate_attempted=terminate_attempted, kill_attempted=kill_attempted,
            final_reap=False, identity_verified=identity_verified, identity_uncertain=identity_uncertain,
            reaped_after=None, errors=errors,
        )
        return self._result


def supervise_process(
    process: Any,
    identity: ProcessInfo | Mapping[str, Any] | None,
    *,
    graceful_timeout_seconds: float = 5.0,
    observer: Callable[..., ProcessQuery] | None = targeted_process_query,
    parent_pid: int | None = None,
    boundary: ProcessBoundary | None = None,
) -> CleanupResult:
    return ProcessSupervisor(
        process,
        identity,
        graceful_timeout_seconds=graceful_timeout_seconds,
        observer=observer,
        parent_pid=parent_pid,
        boundary=boundary,
    ).cleanup()


__all__ = [
    "CleanupResult",
    "PROCESS_CLEANUP_SCHEMA",
    "PROCESS_BOUNDARY_SCHEMA",
    "ProcessBoundary",
    "ProcessBoundaryInventory",
    "ProcessBoundaryUnsupported",
    "ProcessSupervisor",
    "supervise_process",
]
