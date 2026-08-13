"""One focused real-agent journey with a Windows-native public controller.

The synthetic Git repository, invocation, controller state, lifecycle and
result stay on Windows.  Ubuntu WSL prepares an isolated provider side and
the lane controller launches only ``wsl.exe`` as its provider child.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from ctypes import wintypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from orchestrator_harness.cli import watch_once
from orchestrator_harness.config import load_config
from orchestrator_harness.lane_controller import load_invocation
from orchestrator_harness.lane_lifecycle import lifecycle_registry_path
from orchestrator_harness.models import ProcessQuery, iso_utc
from orchestrator_harness.processes import (
    WINDOWS_CREATE_NO_WINDOW,
    targeted_process_query,
)
from orchestrator_harness.public_launch import launch_lane_controller
from orchestrator_harness.tests.wsl_identity import validate_cross_os_identity_relation

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SUPPORT = Path(__file__).resolve().parent / "support"
DEFAULT_DISTRO = "Ubuntu"
DEFAULT_CODEX_ROOT = "/opt/orchestrator-harness-codex"
LANE_ID = "real-agent"
WORKER_ID = "real-agent-001"
DEFAULT_EVIDENCE_DIRECTORY = "orchestrator-harness-real-agent-evidence"
if str(SUPPORT) not in sys.path:
    sys.path.insert(0, str(SUPPORT))
from wsl_real_agent_driver import (
    validate_prepared_state,  # type: ignore[import-not-found]
)


def wsl_path(_distro: str, path: Path) -> str:
    """Translate one Windows path for WSL's explicit host bind source."""

    resolved = path.resolve()
    drive = resolved.drive
    if len(drive) != 2 or drive[1] != ":":
        raise RuntimeError(f"real-agent test requires a drive-letter path: {resolved}")
    return f"/mnt/{drive[0].lower()}/{resolved.as_posix()[2:].lstrip('/')}"


def _json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _run(cwd: Path, *argv: str) -> str:
    result = subprocess.run(
        list(argv), cwd=cwd, stdin=subprocess.DEVNULL, capture_output=True,
        text=True, encoding="utf-8", errors="replace",
        check=False, timeout=30, shell=False,
        creationflags=WINDOWS_CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Git command failed: {argv[0]} {argv[1:]}")
    return result.stdout.strip()


def _read_object(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _resolve_evidence_root(
    value: str | Path | None, *, repository_root: Path = REPOSITORY_ROOT,
) -> Path:
    """Resolve one writable evidence root that cannot be inside source."""

    candidate = (
        Path(value).expanduser()
        if value is not None
        else Path(tempfile.gettempdir()) / DEFAULT_EVIDENCE_DIRECTORY
    )
    resolved = candidate.resolve(strict=False)
    source = repository_root.resolve(strict=True)
    try:
        resolved.relative_to(source)
    except ValueError:
        pass
    else:
        raise RuntimeError("real-agent evidence root must be outside the source checkout")
    if resolved.exists() and not resolved.is_dir():
        raise RuntimeError("real-agent evidence root must be a directory")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def _validate_controller_receipt_identity(
    receipt: dict[str, Any], status: dict[str, Any],
) -> tuple[int, str]:
    """Require the public receipt and controller status to name one process."""

    receipt_pid = receipt.get("pid")
    receipt_created = receipt.get("created_utc")
    if not isinstance(receipt_pid, int) or not isinstance(receipt_created, str):
        raise TypeError("Windows controller receipt lacks exact identity")
    if status.get("controller_pid") != receipt_pid:
        raise RuntimeError("controller status PID does not match the public receipt")
    if status.get("controller_created_utc") != receipt_created:
        raise RuntimeError("controller status creation identity does not match the public receipt")
    return receipt_pid, receipt_created


def _provider_identity_from_query(
    query: ProcessQuery, *, provider_pid: int, provider_created: str,
    controller_pid: int, nonce: str, invocation_id: str,
) -> dict[str, Any]:
    """Fail closed unless one targeted query proves the exact direct child."""

    if not query.complete:
        raise RuntimeError("Windows provider identity query is incomplete")
    if query.errors:
        raise RuntimeError("Windows provider identity query returned errors")
    process = query.process
    if process is None:
        raise RuntimeError("Windows provider identity query found no process")
    if process.pid != provider_pid:
        raise RuntimeError("Windows provider identity query returned the wrong PID")
    if process.created_utc is None or iso_utc(process.created_utc) != provider_created:
        raise RuntimeError("Windows wsl.exe provider creation identity changed")
    if process.ppid != controller_pid:
        raise RuntimeError("Windows provider shim is not a direct controller child")
    command_line = process.command_line.lower()
    if "wsl.exe" not in command_line and "wslhost" not in command_line:
        raise RuntimeError("provider identity is not the controller-owned wsl.exe child")
    return {
        "platform": "windows", "pid": provider_pid,
        "created_utc": provider_created, "nonce": nonce,
        "invocation_id": invocation_id, "parent_pid": process.ppid,
    }


def _artifact_fact(path: Path) -> dict[str, Any]:
    """Describe a disposable artifact without retaining its content or path."""

    try:
        content = path.read_bytes()
    except OSError:
        return {"present": False, "byte_count": 0, "sha256": None}
    return {
        "present": True, "byte_count": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def _safe_controller_summary(status: dict[str, Any] | None) -> dict[str, Any]:
    if status is None:
        return {"available": False}
    validation = status.get("result_validation")
    boundary = status.get("process_boundary")
    return {
        "available": True,
        "state": status.get("state"),
        "exit_code": status.get("exit_code"),
        "result_valid": status.get("result_valid"),
        "result_validation_state": validation.get("state") if isinstance(validation, dict) else None,
        "helpers_complete": status.get("helpers_complete"),
        "direct_child_reaped": status.get("direct_child_reaped"),
        "resource_claim_release_safe": status.get("resource_claim_release_safe"),
        "held_resource_claim_count": len(status["held_resource_claims"])
        if isinstance(status.get("held_resource_claims"), list) else None,
        "process_boundary_complete": boundary.get("complete")
        if isinstance(boundary, dict) else None,
        "process_boundary_live_member_count": len(boundary["live_members"])
        if isinstance(boundary, dict) and isinstance(boundary.get("live_members"), list)
        else None,
    }


def _safe_bridge_summary(bridge: dict[str, Any] | None) -> dict[str, Any]:
    if bridge is None:
        return {"available": False}
    sandbox = bridge.get("sandbox")
    return {
        "available": True,
        "status": bridge.get("status"),
        "cleanup_complete": bridge.get("cleanup_complete"),
        "mnt_c_exposed": sandbox.get("mnt_c_exposed") if isinstance(sandbox, dict) else None,
        "usb_exposed": sandbox.get("usb_exposed") if isinstance(sandbox, dict) else None,
    }


def _safe_prepared_summary(prepared: dict[str, Any] | None) -> dict[str, Any]:
    if prepared is None:
        return {"available": False}
    return {
        "available": True,
        "status": prepared.get("status"),
        "cleanup_complete": prepared.get("cleanup_complete"),
        "credentials_in_state": prepared.get("credentials_in_state"),
    }


def _safe_failure_record(
    temp_root: Path, failure: BaseException,
) -> dict[str, Any]:
    workspace = temp_root / "synthetic-repository" / ".agent-workspace"
    return {
        "schema": "orchestrator-real-agent-evidence/v1",
        "status": "FAIL",
        "completed_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "failure_type": type(failure).__name__,
        "controller_summary": _safe_controller_summary(
            _read_object(workspace / "real_agent_controller.status.json")
        ),
        "bridge_summary": _safe_bridge_summary(_read_object(temp_root / "bridge-evidence.json")),
        "prepared_summary": _safe_prepared_summary(_read_object(temp_root / "prepared-state.json")),
        "source_artifact_facts": {
            "controller_status": _artifact_fact(workspace / "real_agent_controller.status.json"),
            "operator_receipt": _artifact_fact(workspace / "real_agent_operator.receipt.json"),
            "controller_result": _artifact_fact(workspace / "RESULT.json"),
            "provider_event_stream": _artifact_fact(workspace / "real_agent_codex.jsonl"),
            "provider_standard_error": _artifact_fact(workspace / "real_agent_codex.stderr.log"),
            "provider_last_message": _artifact_fact(workspace / "real_agent_last_message.txt"),
        },
        "retained_file_count": 1,
        "credentials_persisted": False,
        "transcript_persisted": False,
    }


def _safe_success_record(
    *,
    completed_utc: str,
    route: str,
    distro: str,
    codex_root: str,
    nonce: str,
    invocation_id: str,
    controller_pid: int,
    controller_created: str,
    provider_identity: dict[str, Any],
    linux_bridge_identity: dict[str, Any],
    status: dict[str, Any],
    result_value: dict[str, Any],
    lifecycle: dict[str, Any],
    bridge: dict[str, Any],
    prepared: dict[str, Any],
    event_types: list[str],
    workspace: Path,
    receipt_path: Path,
    result_path: Path,
    lifecycle_path: Path,
    bridge_evidence: Path,
    state_path: Path,
    claim_path: Path,
    prompt_path: Path,
) -> dict[str, Any]:
    """Build the fixed allowlisted success record.

    The accepted provider result is represented only by its validated fixed
    identity fields and a SHA-256 of the discarded RESULT.json; provider
    summaries, per-check command/summary text, transcripts, and complete
    controller/lifecycle/bridge/preparation objects are never copied.
    """

    source_artifact_facts = {
        name: _artifact_fact(artifact_path)
        for name, artifact_path in (
            ("controller_status", workspace / "real_agent_controller.status.json"),
            ("operator_receipt", receipt_path),
            ("controller_result", result_path),
            ("provider_event_stream", workspace / "real_agent_codex.jsonl"),
            ("provider_standard_error", workspace / "real_agent_codex.stderr.log"),
            ("provider_last_message", workspace / "real_agent_last_message.txt"),
            ("prompt", prompt_path),
            ("lifecycle_registry", lifecycle_path),
            ("bridge_evidence", bridge_evidence),
            ("prepared_state", state_path),
            ("prepared_claim", claim_path),
        )
    }
    return {
        "schema": "orchestrator-real-agent-evidence/v1",
        "status": "PASS",
        "completed_utc": completed_utc,
        "route": route,
        "distro": distro,
        "codex_root": codex_root,
        "attempt_identity": {"nonce": nonce, "invocation_id": invocation_id},
        "controller_identity": {
            "platform": "windows", "pid": controller_pid, "created_utc": controller_created,
        },
        "provider_identity": provider_identity,
        "linux_bridge_identity": linux_bridge_identity,
        "cross_os_relation": "nonce-and-invocation-bound-independent-identities",
        "controller_summary": _safe_controller_summary(status),
        "bridge_summary": _safe_bridge_summary(bridge),
        "prepared_summary": _safe_prepared_summary(prepared),
        "lifecycle_complete": bool(
            isinstance(lifecycle.get("lifecycle"), dict)
            and lifecycle["lifecycle"].get("complete") is True
        ),
        "result_identity": {
            "lane_id": result_value.get("lane_id"),
            "worker_invocation_id": result_value.get("worker_invocation_id"),
            "branch": result_value.get("branch"),
            "commit": result_value.get("commit"),
            "outcome": result_value.get("outcome"),
        },
        "event_types": sorted(set(event_types)),
        "source_artifact_facts": source_artifact_facts,
        "retained_file_count": 1,
        "credentials_persisted": False,
        "transcript_persisted": False,
    }


# --- Windows handle-bound evidence publication --------------------------------

_FILE_READ_DATA = 0x0001
_FILE_READ_ATTRIBUTES = 0x0080
_FILE_WRITE_ATTRIBUTES = 0x0100
_FILE_DELETE = 0x00010000
_FILE_SYNCHRONIZE = 0x00100000
_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_FILE_SHARE_READ = 0x00000001
_FILE_SHARE_WRITE = 0x00000002
_FILE_SHARE_DELETE = 0x00000004
_FILE_ATTRIBUTE_DIRECTORY = 0x00000010
_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_FILE_RENAME_INFORMATION_CLASS = 10
_FILE_DIRECTORY_INFORMATION_CLASS = 1
_FILE_DISPOSITION_INFORMATION_CLASS = 13
_INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

_WINDOWS_API_READY = False
_KERNEL32: Any = None
_NTDLL: Any = None


class _IO_STATUS_BLOCK(ctypes.Structure):
    _fields_ = [
        ("Status", wintypes.LONG),
        ("Information", ctypes.c_void_p),
    ]


class _FILETIME(ctypes.Structure):
    _fields_ = [
        ("dwLowDateTime", wintypes.DWORD),
        ("dwHighDateTime", wintypes.DWORD),
    ]


class _BY_HANDLE_FILE_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("dwFileAttributes", wintypes.DWORD),
        ("ftCreationTime", _FILETIME),
        ("ftLastAccessTime", _FILETIME),
        ("ftLastWriteTime", _FILETIME),
        ("dwVolumeSerialNumber", wintypes.DWORD),
        ("nFileSizeHigh", wintypes.DWORD),
        ("nFileSizeLow", wintypes.DWORD),
        ("nNumberOfLinks", wintypes.DWORD),
        ("nFileIndexHigh", wintypes.DWORD),
        ("nFileIndexLow", wintypes.DWORD),
    ]


class _LARGE_INTEGER(ctypes.Structure):
    _fields_ = [
        ("QuadPart", ctypes.c_longlong),
    ]


class _FILE_DIRECTORY_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("NextEntryOffset", wintypes.ULONG),
        ("FileIndex", wintypes.ULONG),
        ("CreationTime", _LARGE_INTEGER),
        ("LastAccessTime", _LARGE_INTEGER),
        ("LastWriteTime", _LARGE_INTEGER),
        ("ChangeTime", _LARGE_INTEGER),
        ("EndOfFile", _LARGE_INTEGER),
        ("AllocationSize", _LARGE_INTEGER),
        ("FileAttributes", wintypes.ULONG),
        ("FileNameLength", wintypes.ULONG),
        ("FileName", wintypes.WCHAR * 1),
    ]


class _FILE_RENAME_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("ReplaceIfExists", wintypes.BOOL),
        ("RootDirectory", wintypes.HANDLE),
        ("FileNameLength", wintypes.DWORD),
        ("FileName", wintypes.WCHAR * 1),
    ]


class _FILE_DISPOSITION_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("DeleteFile", wintypes.BOOLEAN),
        ("_reserved", wintypes.BYTE * 3),
    ]


# The filename payload begins at the native FileName member offset: 20 bytes
# on 64-bit Windows and 12 bytes on 32-bit Windows.  sizeof() of the
# structure is not used because trailing array alignment would overestimate
# the header.
_FILE_RENAME_INFO_HEADER_SIZE = _FILE_RENAME_INFORMATION.FileName.offset


def _windows_api() -> None:
    """Configure the exact Windows APIs used by handle-bound publication once."""

    global _WINDOWS_API_READY, _KERNEL32, _NTDLL
    if _WINDOWS_API_READY:
        return
    if os.name != "nt":
        raise RuntimeError(
            "real-agent evidence publication requires the Windows controller host"
        )
    _KERNEL32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _NTDLL = ctypes.WinDLL("ntdll", use_last_error=True)
    _KERNEL32.CreateFileW.argtypes = [
        wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
        wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE,
    ]
    _KERNEL32.CreateFileW.restype = wintypes.HANDLE
    _KERNEL32.GetFileInformationByHandle.argtypes = [
        wintypes.HANDLE, ctypes.POINTER(_BY_HANDLE_FILE_INFORMATION),
    ]
    _KERNEL32.GetFileInformationByHandle.restype = wintypes.BOOL
    _KERNEL32.CloseHandle.argtypes = [wintypes.HANDLE]
    _KERNEL32.CloseHandle.restype = wintypes.BOOL
    _NTDLL.NtSetInformationFile.argtypes = [
        wintypes.HANDLE, ctypes.POINTER(_IO_STATUS_BLOCK), ctypes.c_void_p,
        wintypes.ULONG, ctypes.c_int,
    ]
    _NTDLL.NtSetInformationFile.restype = wintypes.LONG
    _KERNEL32.ReadFile.argtypes = [
        wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD), ctypes.c_void_p,
    ]
    _KERNEL32.ReadFile.restype = wintypes.BOOL
    _KERNEL32.SetFilePointerEx.argtypes = [
        wintypes.HANDLE, ctypes.c_longlong, ctypes.POINTER(ctypes.c_longlong),
        wintypes.DWORD,
    ]
    _KERNEL32.SetFilePointerEx.restype = wintypes.BOOL
    _KERNEL32.GetFileSizeEx.argtypes = [
        wintypes.HANDLE, ctypes.POINTER(_LARGE_INTEGER),
    ]
    _KERNEL32.GetFileSizeEx.restype = wintypes.BOOL
    _NTDLL.NtQueryDirectoryFile.argtypes = [
        wintypes.HANDLE, wintypes.HANDLE, ctypes.c_void_p, ctypes.c_void_p,
        ctypes.POINTER(_IO_STATUS_BLOCK), ctypes.c_void_p, wintypes.ULONG,
        ctypes.c_int, wintypes.BOOLEAN, ctypes.c_void_p, wintypes.BOOLEAN,
    ]
    _NTDLL.NtQueryDirectoryFile.restype = wintypes.LONG
    _WINDOWS_API_READY = True


def _open_directory_handle(path: Path, *, access: int, share: int) -> int:
    """Open one directory no-follow and return the exact Windows handle."""

    _windows_api()
    handle = _KERNEL32.CreateFileW(
        str(path), access, share, None, 3,  # OPEN_EXISTING
        _FILE_FLAG_BACKUP_SEMANTICS | _FILE_FLAG_OPEN_REPARSE_POINT, None,
    )
    if handle == _INVALID_HANDLE_VALUE:
        raise OSError(ctypes.get_last_error(), f"failed to open directory handle: {path}")
    return int(handle)


def _close_handle(handle: int) -> None:
    if handle is not None and handle != _INVALID_HANDLE_VALUE:
        _windows_api()
        _KERNEL32.CloseHandle(handle)


def _open_regular_file_handle(path: Path, *, access: int, share: int) -> int:
    """Open one file no-follow and return the exact Windows handle."""

    _windows_api()
    handle = _KERNEL32.CreateFileW(
        str(path), access, share, None, 3,  # OPEN_EXISTING
        _FILE_FLAG_OPEN_REPARSE_POINT, None,
    )
    if handle == _INVALID_HANDLE_VALUE:
        raise OSError(ctypes.get_last_error(), f"failed to open file handle: {path}")
    return int(handle)


def _read_file_handle(handle: int, size: int) -> bytes:
    """Read exactly the requested byte count through one retained handle."""

    _windows_api()
    distance = ctypes.c_longlong(0)
    if not _KERNEL32.SetFilePointerEx(handle, 0, ctypes.byref(distance), 0):
        raise OSError(ctypes.get_last_error(), "SetFilePointerEx failed")
    buffer = ctypes.create_string_buffer(size)
    read = wintypes.DWORD(0)
    if not _KERNEL32.ReadFile(handle, buffer, size, ctypes.byref(read), None):
        raise OSError(ctypes.get_last_error(), "ReadFile failed")
    return buffer.raw[: read.value]


def _file_size_handle(handle: int) -> int:
    """Return the exact byte size of one retained file handle."""

    _windows_api()
    size = _LARGE_INTEGER()
    if not _KERNEL32.GetFileSizeEx(handle, ctypes.byref(size)):
        raise OSError(ctypes.get_last_error(), "GetFileSizeEx failed")
    return int(size.QuadPart)


def _enumerate_directory_handle(handle: int) -> list[dict[str, Any]]:
    """List one directory through its retained handle; no pathname is used."""

    _windows_api()
    buffer = ctypes.create_string_buffer(65536)
    status_block = _IO_STATUS_BLOCK()
    status = _NTDLL.NtQueryDirectoryFile(
        handle, None, None, None, ctypes.byref(status_block),
        buffer, len(buffer), _FILE_DIRECTORY_INFORMATION_CLASS,
        False, None, True,
    )
    if (status & 0xFFFFFFFF) == 0x80000006:  # STATUS_NO_MORE_FILES
        return []
    if status != 0:
        raise RuntimeError(
            f"directory enumeration failed closed (0x{status & 0xFFFFFFFF:08X})"
        )
    entries: list[dict[str, Any]] = []
    offset = 0
    while True:
        entry = ctypes.cast(
            ctypes.byref(buffer, offset), ctypes.POINTER(_FILE_DIRECTORY_INFORMATION)
        ).contents
        name_length = int(entry.FileNameLength)
        name = ctypes.string_at(
            ctypes.addressof(entry) + _FILE_DIRECTORY_INFORMATION.FileName.offset,
            name_length,
        ).decode("utf-16-le")
        entries.append({"name": name, "attributes": int(entry.FileAttributes)})
        if entry.NextEntryOffset == 0:
            break
        offset += int(entry.NextEntryOffset)
    return entries


def _set_file_disposition(handle: int, delete: bool) -> None:
    """Mark one retained handle for native deletion at close; no pathname."""

    _windows_api()
    info = _FILE_DISPOSITION_INFORMATION(bool(delete))
    status_block = _IO_STATUS_BLOCK()
    status = _NTDLL.NtSetInformationFile(
        handle, ctypes.byref(status_block), ctypes.byref(info),
        ctypes.sizeof(_FILE_DISPOSITION_INFORMATION),
        _FILE_DISPOSITION_INFORMATION_CLASS,
    )
    if status != 0:
        raise RuntimeError(
            f"native disposition failed closed (0x{status & 0xFFFFFFFF:08X})"
        )


def _handle_file_information(handle: int) -> dict[str, Any]:
    """Return attributes, volume serial, and file index by handle."""

    _windows_api()
    info = _BY_HANDLE_FILE_INFORMATION()
    if not _KERNEL32.GetFileInformationByHandle(handle, ctypes.byref(info)):
        raise OSError(ctypes.get_last_error(), "GetFileInformationByHandle failed")
    return {
        "attributes": int(info.dwFileAttributes),
        "volume_serial": int(info.dwVolumeSerialNumber),
        "file_index": (int(info.nFileIndexHigh) << 32) | int(info.nFileIndexLow),
    }


def _capture_directory_identity(path: Path) -> dict[str, Any]:
    """Capture one no-follow non-reparse directory identity by Windows handle."""

    handle = _open_directory_handle(
        path,
        access=_FILE_READ_ATTRIBUTES | _FILE_SYNCHRONIZE,
        share=_FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,
    )
    try:
        info = _handle_file_information(handle)
    finally:
        _close_handle(handle)
    if not (info["attributes"] & _FILE_ATTRIBUTE_DIRECTORY):
        raise RuntimeError(f"expected a directory: {path}")
    if info["attributes"] & _FILE_ATTRIBUTE_REPARSE_POINT:
        raise RuntimeError(f"reparse point is not allowed: {path}")
    return {
        "path": str(path),
        "volume_serial": info["volume_serial"],
        "file_index": info["file_index"],
        "attributes": info["attributes"],
    }


def _open_evidence_root_handle(path: Path) -> dict[str, Any]:
    """Open and bind the exact external evidence root for one attempt.

    The returned binding holds a no-follow Windows directory handle whose
    sharing mode denies delete sharing, so the bound root cannot be renamed,
    deleted, or replaced while the attempt is active.  The identity is
    captured by handle and the lexical path is never resolved.
    """

    if os.name != "nt":
        raise RuntimeError("evidence-root handle binding requires Windows")
    if path.is_symlink() or (os.name == "nt" and os.path.isjunction(path)):
        raise RuntimeError(f"evidence root must not be a link: {path}")
    handle = _open_directory_handle(
        path,
        access=(
            _FILE_READ_DATA | _FILE_READ_ATTRIBUTES
            | _FILE_WRITE_ATTRIBUTES | _FILE_SYNCHRONIZE
        ),
        share=_FILE_SHARE_READ | _FILE_SHARE_WRITE,  # delete sharing denied
    )
    try:
        info = _handle_file_information(handle)
    except BaseException:
        _close_handle(handle)
        raise
    if not (info["attributes"] & _FILE_ATTRIBUTE_DIRECTORY):
        _close_handle(handle)
        raise RuntimeError(f"evidence root is not a directory: {path}")
    if info["attributes"] & _FILE_ATTRIBUTE_REPARSE_POINT:
        _close_handle(handle)
        raise RuntimeError(f"evidence root must not be a reparse point: {path}")
    return {
        "handle": int(handle),
        "path": str(path),
        "volume_serial": info["volume_serial"],
        "file_index": info["file_index"],
        "attributes": info["attributes"],
    }


def _close_evidence_root_handle(binding: dict[str, Any]) -> None:
    """Close the bound evidence-root handle exactly once per terminal route."""

    handle = binding.get("handle")
    if handle is not None:
        _close_handle(handle)
        binding["handle"] = None


def _revalidate_evidence_root_binding(binding: dict[str, Any]) -> None:
    """Fail closed unless the bound root is still the exact verified directory."""

    if binding.get("handle") is None:
        raise RuntimeError("evidence-root handle is already closed")
    current = _capture_directory_identity(Path(binding["path"]))
    if (current["volume_serial"], current["file_index"]) != (
        binding["volume_serial"],
        binding["file_index"],
    ):
        raise RuntimeError(f"evidence-root identity changed: {binding['path']}")


def _validate_attempt_name(name: str) -> str:
    if not isinstance(name, str) or not name:
        raise RuntimeError("attempt name is invalid")
    if any(ch in name for ch in '/\\:*?"<>|'):
        raise RuntimeError("attempt name is invalid")
    if name in {".", ".."} or name.strip() != name or name.endswith((" ", ".")):
        raise RuntimeError("attempt name is invalid")
    return name


# Test-only interposition point invoked with the staging binding inside the
# fail-closed handle-closing owner of exact staging disposal; a hook fault
# closes and clears the retained staging handle and leaves the private stage
# untouched.  Production runs never set it.
_CLEANUP_INTERPOSITION_HOOK: Any = None


def _verify_staging_via_handles(binding: dict[str, Any]) -> None:
    """Fail closed unless the retained staging object is exact and current.

    All checks use only the retained no-follow staging-directory handle and
    transient no-follow record opens; the staging pathname is never resolved
    or reopened in the final check-use interval.  The record handle is never
    held across the native directory rename (Windows refuses to rename a
    directory that contains an open child handle), so the exact record bytes
    and identity are re-verified immediately before publication.
    """

    staging_handle = binding.get("handle")
    if staging_handle is None:
        raise RuntimeError("staging binding is not retained")
    info = _handle_file_information(staging_handle)
    if (info["volume_serial"], info["file_index"]) != (
        binding["volume_serial"],
        binding["file_index"],
    ):
        raise RuntimeError("evidence staging identity changed")
    entries = _enumerate_directory_handle(staging_handle)
    names = sorted(
        entry["name"] for entry in entries if entry["name"] not in (".", "..")
    )
    if names != [binding["record_name"]]:
        raise RuntimeError(
            f"staging directory must contain exactly one record: {names}"
        )
    for entry in entries:
        if entry["name"] in (".", ".."):
            continue
        if entry["attributes"] & _FILE_ATTRIBUTE_REPARSE_POINT:
            raise RuntimeError("staging record must not be a reparse point")
        if entry["attributes"] & _FILE_ATTRIBUTE_DIRECTORY:
            raise RuntimeError("staging record must be an ordinary file")
    record_handle = _open_record_handle(binding, delete=False)
    try:
        record_info = _handle_file_information(record_handle)
        if (record_info["volume_serial"], record_info["file_index"]) != (
            binding["record_volume_serial"],
            binding["record_file_index"],
        ):
            raise RuntimeError("staging record identity changed")
        if _file_size_handle(record_handle) != len(binding["record_bytes"]):
            raise RuntimeError("staging record size changed")
        actual = _read_file_handle(record_handle, len(binding["record_bytes"]))
        if actual != binding["record_bytes"]:
            raise RuntimeError("staging record bytes changed")
    finally:
        _close_handle(record_handle)


def _dispose_staging_evidence(binding: dict[str, Any]) -> None:
    """Dispose exactly the retained staging object by handle or fail closed.

    Verifies through the retained staging-directory handle that it still
    contains exactly the retained record with the expected bytes, then opens
    the exact record no-follow with DELETE access, verifies its identity
    again, marks it for native deletion, marks the now-empty directory for
    native deletion, and closes every owned handle.  The test-only cleanup
    interposition hook runs inside this fail-closed handle-closing owner, so
    a hook fault still closes and clears the exact retained staging handle
    and leaves the private staging directory untouched.  If the exact object
    cannot be verified, every owned handle is still closed and the private
    staging directory is left untouched with a RuntimeError so the terminal
    rules preserve the cleanup failure.  No mutable staging pathname is ever
    traversed, unlinked, or removed.
    """

    staging_handle = binding.get("handle")
    if staging_handle is None:
        return
    try:
        hook = _CLEANUP_INTERPOSITION_HOOK
        if hook is not None:
            hook(binding)
        _verify_staging_via_handles(binding)
        record_handle = _open_record_handle(binding, delete=True)
        try:
            record_info = _handle_file_information(record_handle)
            if (record_info["volume_serial"], record_info["file_index"]) != (
                binding["record_volume_serial"],
                binding["record_file_index"],
            ):
                raise RuntimeError("staging record identity changed during cleanup")
            _set_file_disposition(record_handle, True)
        finally:
            _close_handle(record_handle)
        entries = _enumerate_directory_handle(staging_handle)
        if any(entry["name"] not in (".", "..") for entry in entries):
            raise RuntimeError("staging directory gained entries during cleanup")
        _set_file_disposition(staging_handle, True)
        _close_handle(staging_handle)
        binding["handle"] = None
    except BaseException as exc:
        _close_handle(staging_handle)
        binding["handle"] = None
        raise RuntimeError(f"exact staging cleanup failed closed: {exc}") from exc


def _release_staging_handles(binding: dict[str, Any]) -> None:
    """Close the retained staging handle after a successful publication."""

    _close_handle(binding.get("handle"))
    binding["handle"] = None


def _open_record_handle(binding: dict[str, Any], *, delete: bool) -> int:
    """Open the exact record file no-follow inside the bound staging directory.

    The parent directory is retained with delete sharing denied, so the
    staging pathname cannot be replaced while this open is performed; the
    record itself is opened no-follow and verified by identity.
    """

    access = _FILE_READ_ATTRIBUTES | _FILE_READ_DATA | _FILE_SYNCHRONIZE
    if delete:
        access |= _FILE_DELETE
    return _open_regular_file_handle(
        Path(binding["path"]) / binding["record_name"],
        access=access,
        share=_FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,
    )


def _build_staging_evidence(
    root_binding: dict[str, Any], record: dict[str, Any],
) -> dict[str, Any]:
    """Build one private same-volume staging directory with exactly the record.

    The fresh staging directory is bound immediately after creation with a
    no-follow handle opened without delete sharing; that exact handle and its
    volume/file identity are retained through record construction, final
    source validation, native publication, and safe cleanup.  The record file
    is created inside the bound directory and its identity and serialized
    bytes are captured through transient no-follow opens.  The staging
    pathname is never resolved or reopened after construction, and the record
    handle is never held across the native directory rename.
    """

    staging_path = Path(tempfile.mkdtemp(prefix="orchestrator-s6-evidence-staging-"))
    staging_handle: int | None = None
    try:
        staging_handle = _open_directory_handle(
            staging_path,
            access=(
                _FILE_READ_DATA | _FILE_READ_ATTRIBUTES | _FILE_WRITE_ATTRIBUTES
                | _FILE_SYNCHRONIZE | _FILE_DELETE
            ),
            share=_FILE_SHARE_READ | _FILE_SHARE_WRITE,  # delete sharing denied
        )
        info = _handle_file_information(staging_handle)
        if not (info["attributes"] & _FILE_ATTRIBUTE_DIRECTORY):
            raise RuntimeError(f"evidence staging is not a directory: {staging_path}")
        if info["attributes"] & _FILE_ATTRIBUTE_REPARSE_POINT:
            raise RuntimeError(
                f"evidence staging must not be a reparse point: {staging_path}"
            )
        if info["volume_serial"] != root_binding["volume_serial"]:
            raise RuntimeError("evidence staging is not on the evidence-root volume")
        record_bytes = (json.dumps(record, indent=2, sort_keys=True) + "\n").encode("utf-8")
        record_path = staging_path / "REAL_AGENT_TEST_RESULT.json"
        with open(record_path, "x", encoding="utf-8", newline="\n") as handle:
            handle.write(record_bytes.decode("utf-8"))
        record_handle = _open_regular_file_handle(
            record_path,
            access=_FILE_DELETE | _FILE_READ_ATTRIBUTES | _FILE_READ_DATA | _FILE_SYNCHRONIZE,
            share=_FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,
        )
        try:
            record_info = _handle_file_information(record_handle)
            if record_info["attributes"] & _FILE_ATTRIBUTE_DIRECTORY:
                raise RuntimeError("staging record is not an ordinary file")
            if record_info["attributes"] & _FILE_ATTRIBUTE_REPARSE_POINT:
                raise RuntimeError("staging record must not be a reparse point")
            actual = _read_file_handle(record_handle, len(record_bytes))
            if actual != record_bytes:
                raise RuntimeError("staging record bytes do not match the constructed record")
        finally:
            _close_handle(record_handle)
        return {
            "handle": staging_handle,
            "path": str(staging_path),
            "volume_serial": info["volume_serial"],
            "file_index": info["file_index"],
            "record_volume_serial": record_info["volume_serial"],
            "record_file_index": record_info["file_index"],
            "record_bytes": record_bytes,
            "record_name": "REAL_AGENT_TEST_RESULT.json",
        }
    except BaseException:
        # A partially built fresh private directory is left untouched after
        # the owned handles close; the construction failure is the terminal
        # failure and no mutable staging pathname is ever cleaned.
        _close_handle(staging_handle)
        raise


# Test-only interposition point invoked with the staging binding before the
# final retained-object staging verification.  Record-substitution tests use
# this seam so the actual final verification detects the swap.  Production
# runs never set it.
_PRE_FINAL_STAGING_VALIDATION_HOOK: Any = None

# Test-only interposition point invoked with (root_binding, attempt_name,
# staging-binding) after the final full retained-object staging verification
# and immediately before the native handle-relative publication.  Production
# runs never set it.
_PUBLICATION_INTERPOSITION_HOOK: Any = None


def _rename_attempt_relative(
    source_handle: int, root_handle: int, attempt_name: str,
) -> None:
    """One no-replace handle-relative native atomic rename.

    Uses NtSetInformationFile with FileRenameInformation whose RootDirectory
    is the retained evidence-root handle; the destination is a fresh
    unpredictable attempt name and ReplaceIfExists is always FALSE.  The
    FILE_RENAME_INFORMATION filename payload offset is derived from the
    active ctypes native layout (FileName.offset), never hard-coded.
    """

    _windows_api()
    name_bytes = attempt_name.encode("utf-16-le")
    header_size = _FILE_RENAME_INFO_HEADER_SIZE
    info = _FILE_RENAME_INFORMATION()
    info.ReplaceIfExists = False
    info.RootDirectory = root_handle
    info.FileNameLength = len(name_bytes)
    buffer_size = header_size + len(name_bytes)
    buffer = ctypes.create_string_buffer(buffer_size)
    ctypes.memmove(buffer, ctypes.byref(info), header_size)
    ctypes.memmove(
        ctypes.addressof(buffer) + header_size,
        name_bytes,
        len(name_bytes),
    )
    status_block = _IO_STATUS_BLOCK()
    status = _NTDLL.NtSetInformationFile(
        source_handle,
        ctypes.byref(status_block),
        buffer,
        buffer_size,
        _FILE_RENAME_INFORMATION_CLASS,
    )
    if status == 0:
        return
    if (status & 0xFFFFFFFF) == 0xC0000035:  # STATUS_OBJECT_NAME_COLLISION
        raise RuntimeError(
            f"attempt destination already exists; native publication failed "
            f"closed: {attempt_name}"
        )
    raise RuntimeError(
        f"native handle-relative publication failed closed "
        f"(0x{status & 0xFFFFFFFF:08X}): {attempt_name}"
    )


def _publish_attempt_directory(
    root_binding: dict[str, Any], attempt_name: str, staging: dict[str, Any],
) -> Path:
    """Publish one complete one-file attempt via handle-relative atomic rename.

    The final external attempt path never exists and is never announced
    before this rename.  The root binding is revalidated and the retained
    staging object is verified entirely through its no-follow handles, then
    the publication interposition hook runs immediately before the native
    rename; no staging validation runs between that hook and the rename.
    The native rename source is the retained staging handle itself, so the
    staging pathname is never resolved or reopened in the final check-use
    interval.
    """

    _validate_attempt_name(attempt_name)
    _revalidate_evidence_root_binding(root_binding)
    pre_final_hook = _PRE_FINAL_STAGING_VALIDATION_HOOK
    if pre_final_hook is not None:
        pre_final_hook(staging)
    _verify_staging_via_handles(staging)
    hook = _PUBLICATION_INTERPOSITION_HOOK
    if hook is not None:
        hook(root_binding, attempt_name, staging)
    _rename_attempt_relative(staging["handle"], root_binding["handle"], attempt_name)
    return Path(root_binding["path"]) / attempt_name


def _finalize_attempt_evidence(
    root_binding: dict[str, Any],
    attempt_name: str,
    record: dict[str, Any],
) -> Path:
    """Publish exactly one allowlisted record through one handle-bound rename.

    Every terminal route (success and failure) publishes through this single
    finalization owner.  The retained record is constructed only from fixed
    identity and outcome scalars plus safe hashes/counts; provider-controlled
    text and full controller/provider objects are never copied into it.
    """

    if record.get("status") not in {"PASS", "FAIL"} or record.get("retained_file_count") != 1:
        raise RuntimeError("attempt evidence record violates the fixed allowlist")
    _revalidate_evidence_root_binding(root_binding)
    staging = _build_staging_evidence(root_binding, record)
    try:
        published = _publish_attempt_directory(root_binding, attempt_name, staging)
    except BaseException:
        _dispose_staging_evidence(staging)
        raise
    _release_staging_handles(staging)
    return published


def _remove_disposable_temp_root(
    temp_root: Path, *, timeout_seconds: float = 15.0,
) -> None:
    """Remove the exact mkdtemp root or fail the live oracle."""

    expected_parent = Path(tempfile.gettempdir()).resolve(strict=True)
    resolved = temp_root.resolve(strict=False)
    if (
        resolved.parent != expected_parent
        or not resolved.name.startswith("orchestrator-s6-real-agent-")
    ):
        raise RuntimeError(f"refusing unexpected real-agent temp root: {resolved}")
    deadline = time.monotonic() + timeout_seconds
    last_error: OSError | None = None

    def clear_readonly_and_retry(
        function: Any, path: str, failure: BaseException,
    ) -> None:
        candidate = Path(path).resolve(strict=False)
        try:
            candidate.relative_to(resolved)
        except ValueError as exc:
            raise RuntimeError(
                f"refusing cleanup outside the real-agent temp root: {candidate}"
            ) from exc
        if not isinstance(failure, PermissionError):
            raise failure
        os.chmod(path, stat.S_IWRITE)
        function(path)

    def clear_readonly_legacy(
        function: Any, path: str, failure_info: Any,
    ) -> None:
        clear_readonly_and_retry(function, path, failure_info[1])

    while resolved.exists():
        try:
            remove_tree: Any = shutil.rmtree
            if sys.version_info >= (3, 12):
                remove_tree(resolved, onexc=clear_readonly_and_retry)
            else:
                remove_tree(resolved, onerror=clear_readonly_legacy)
        except OSError as exc:
            last_error = exc
        if not resolved.exists():
            return
        if time.monotonic() >= deadline:
            detail = f": {last_error}" if last_error is not None else ""
            raise RuntimeError(
                f"real-agent host temp cleanup did not complete{detail}"
            ) from last_error
        time.sleep(0.1)


def _release_preparation_process(
    prep_process: subprocess.Popen[bytes] | None,
    release_signal: Any,
) -> BaseException | None:
    """Release the exact preparation process and return the first cleanup fault.

    Owns only the exact Popen handle and the exact release signal; no process
    scans, name matching, or broad cleanup.  Every ordinary release-write,
    initial wait, timeout-kill, and post-kill-wait fault is captured while
    safe exact-handle fallback steps are still attempted.  The first captured
    fault is returned so the terminal seam can convert it into the existing
    failure state.
    """

    if prep_process is None:
        return None
    first_fault: BaseException | None = None

    def capture(fault: BaseException) -> None:
        nonlocal first_fault
        if first_fault is None:
            first_fault = fault

    if prep_process.poll() is None:
        if release_signal is None:
            try:
                prep_process.kill()
            except BaseException as exc:  # noqa: BLE001
                capture(exc)
        else:
            try:
                release_signal.write_text("release\n", encoding="utf-8")
            except BaseException as exc:  # noqa: BLE001
                capture(exc)
                try:
                    prep_process.kill()
                except BaseException as kill_exc:  # noqa: BLE001
                    capture(kill_exc)
    try:
        prep_process.wait(timeout=90)
    except subprocess.TimeoutExpired:
        try:
            prep_process.kill()
        except BaseException as exc:  # noqa: BLE001
            capture(exc)
        try:
            prep_process.wait(timeout=10)
        except BaseException as exc:  # noqa: BLE001
            capture(exc)
    except BaseException as exc:  # noqa: BLE001
        capture(exc)
        try:
            prep_process.kill()
        except BaseException as kill_exc:  # noqa: BLE001
            capture(kill_exc)
        try:
            prep_process.wait(timeout=10)
        except BaseException as post_exc:  # noqa: BLE001
            capture(post_exc)
    return first_fault


def _terminal_finalize(
    *,
    root_binding: dict[str, Any],
    attempt_name: str,
    temp_root: Path,
    failure: BaseException | None,
    result: dict[str, Any] | None,
    prep_process: subprocess.Popen[bytes] | None,
    release_signal: Any,
) -> Path | None:
    """Production terminal seam: publish one record and close the root handle.

    Every terminal route (success and failure) passes through this single
    orchestration owner, which main()'s finally block calls.  Preparation-
    process cleanup faults are captured and converted into the existing
    failure state while safe exact-handle fallback steps still run; safe
    failure-record construction, exact disposable-root removal, and exactly
    one construction-only attempt-evidence finalizer call cannot be bypassed
    by those faults.  The evidence-root handle is closed exactly once on every
    route.  The terminal fault is re-raised only after the retained record is
    published, unless finalization itself fails closed.
    """

    published: Path | None = None
    try:
        prep_cleanup_failure = _release_preparation_process(prep_process, release_signal)
        if prep_cleanup_failure is not None and failure is None:
            failure = prep_cleanup_failure
        failure_record = (
            _safe_failure_record(temp_root, failure) if failure is not None else None
        )
        cleanup_failure: RuntimeError | None = None
        try:
            _remove_disposable_temp_root(temp_root)
        except RuntimeError as exc:
            cleanup_failure = exc
            if failure is None:
                failure = exc
                failure_record = _safe_failure_record(temp_root, failure)
        if failure is not None:
            assert failure_record is not None
            failure_record["host_temp_cleanup_complete"] = cleanup_failure is None
            if cleanup_failure is not None:
                failure_record["host_temp_cleanup_failure_type"] = type(
                    cleanup_failure
                ).__name__
            if prep_cleanup_failure is not None:
                failure_record["preparation_cleanup_failure_type"] = type(
                    prep_cleanup_failure
                ).__name__
            _finalize_attempt_evidence(root_binding, attempt_name, failure_record)
            raise failure
        elif result is not None:
            result["host_temp_cleanup_complete"] = True
            published = _finalize_attempt_evidence(
                root_binding, attempt_name, result,
            )
        else:
            raise RuntimeError("terminal seam requires a result or a failure")
    finally:
        _close_evidence_root_handle(root_binding)
    return published


def _wait_json(path: Path, predicate: Any, *, timeout: float, process: subprocess.Popen[bytes] | None = None) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = _read_object(path)
        if value is not None and predicate(value):
            return value
        if process is not None and process.poll() is not None and not path.exists():
            raise RuntimeError(f"prepared Linux side exited before publishing {path.name}")
        time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for {path}")


def _make_repository(root: Path) -> tuple[Path, str, str]:
    repo = root / "synthetic-repository"
    repo.mkdir()
    _run(repo, "git", "init", "-b", LANE_ID)
    _run(repo, "git", "config", "user.email", "real-agent@example.invalid")
    _run(repo, "git", "config", "user.name", "Synthetic Real Agent")
    (repo / ".gitignore").write_text(".agent-workspace/\n", encoding="utf-8")
    (repo / "task.txt").write_text(
        "Create marker.txt, commit it, and publish the exact coding result.\n",
        encoding="utf-8",
    )
    _run(repo, "git", "add", ".gitignore", "task.txt")
    _run(repo, "git", "commit", "-m", "Initialize synthetic real-agent repository")
    common_dir = _run(repo, "git", "rev-parse", "--git-common-dir")
    return repo, _run(repo, "git", "rev-parse", "HEAD"), str((repo / common_dir).resolve())


def _make_invocation(
    repo: Path, base_commit: str, common_dir: str, runtime: Path, prompt: Path,
    provider_command: list[str],
) -> Path:
    workspace = repo / ".agent-workspace"
    workspace.mkdir(exist_ok=True)
    status = workspace / "real_agent_controller.status.json"
    invocation = {
        "schema": "orchestrator-coding-invocation/v1",
        "action": "start",
        "runtime_root": str(runtime),
        "resource_lock_root": str(runtime / "coding-resource-locks"),
        "run_root": str(repo),
        "repository": {
            "common_dir": common_dir,
            "worktree_root": str(repo),
            "branch": LANE_ID,
            "base_commit": base_commit,
        },
        "prompt_path": str(prompt),
        "prompt_sha256": hashlib.sha256(prompt.read_bytes()).hexdigest(),
        "output_paths": {
            "status": str(status),
            "jsonl": str(workspace / "real_agent_codex.jsonl"),
            "stderr": str(workspace / "real_agent_codex.stderr.log"),
            "last_message": str(workspace / "real_agent_last_message.txt"),
        },
        "event_log_path": str(runtime / "LANE_EVENTS.jsonl"),
        "lane_id": LANE_ID,
        "worker_invocation_id": WORKER_ID,
        "task": "Complete the public real-agent release route in the synthetic repository",
        "phase": "public-route",
        "exclusive_resources": [],
        "codex": {
            "command": provider_command,
            "model": "gpt-5.6-terra",
            "reasoning_effort": "high",
            "service_tier": "priority",
            "sandbox": "danger-full-access",
            "approval_policy": "never",
            "config_overrides": ["mcp_servers={}"],
        },
    }
    path = workspace / "invocation.json"
    _json(path, invocation)
    return path


def _prompt(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """Work only in the synthetic repository at the current working directory.
Do not inspect the parent, host authentication, source checkout, hardware, MCP,
USB, or any external service except the already configured provider session.

Create marker.txt with exactly one line: public route passed. Commit it with
message Complete public route task. Then write .agent-workspace/RESULT.json
with exactly this coding result shape and the exact branch/tip after your
commit:
{
  "schema": "orchestrator-lane-result/v1",
  "lane_id": "real-agent",
  "worker_invocation_id": "real-agent-001",
  "branch": "real-agent",
  "commit": "<exact git rev-parse HEAD>",
  "outcome": "PASS",
  "summary": "public route task passed",
  "checks": [{"name": "synthetic public route", "outcome": "PASS", "summary": "marker committed"}]
}
Leave the repository clean and reply PUBLIC_ROUTE_COMPLETE.
""",
        encoding="utf-8",
    )


def _event_types(event_log: Path) -> list[str]:
    types: list[str] = []
    try:
        for line in event_log.read_text(encoding="utf-8").splitlines():
            value = json.loads(line)
            if isinstance(value, dict) and isinstance(value.get("type"), str):
                types.append(value["type"])
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        pass
    return types


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Windows public route with one isolated Ubuntu provider")
    parser.add_argument("--distro", default=os.environ.get("ORCH_HARNESS_WSL_DISTRO", DEFAULT_DISTRO))
    parser.add_argument("--codex-root", default=os.environ.get("ORCH_HARNESS_WSL_CODEX_ROOT", DEFAULT_CODEX_ROOT))
    parser.add_argument("--evidence-root", default=os.environ.get("ORCH_HARNESS_REAL_AGENT_EVIDENCE_ROOT"))
    parser.add_argument("--model", default="gpt-5.6-terra")
    args = parser.parse_args()
    if os.name != "nt":
        raise RuntimeError("real-agent public route requires the Windows controller host")
    if args.distro != "Ubuntu":
        raise RuntimeError("repair-003 real-agent oracle is pinned to Ubuntu")
    if args.codex_root != DEFAULT_CODEX_ROOT:
        raise RuntimeError("real-agent oracle requires the isolated pinned Codex root")
    auth = Path.home() / ".codex" / "auth.json"
    if not auth.is_file():
        raise RuntimeError("Codex authentication file is unavailable")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    attempt_name = stamp
    evidence_root = _resolve_evidence_root(args.evidence_root)
    temp_name = tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-")
    temp_root = Path(temp_name).resolve()
    try:
        root_binding = _open_evidence_root_handle(evidence_root)
    except BaseException:
        try:
            _remove_disposable_temp_root(temp_root)
        except BaseException:
            pass
        raise
    run_id = uuid.uuid4().hex
    nonce = uuid.uuid4().hex + uuid.uuid4().hex
    prep_process: subprocess.Popen[bytes] | None = None
    result: dict[str, Any] | None = None
    failure: BaseException | None = None
    status: dict[str, Any] | None = None
    release_signal: Path | None = None
    try:
        repo, base_commit, common_dir = _make_repository(temp_root)
        runtime = temp_root / "runtime"
        runtime.mkdir()
        state_path = temp_root / "prepared-state.json"
        release_signal = temp_root / "release-signal.json"
        bridge_evidence = temp_root / "bridge-evidence.json"
        claim_path = temp_root / "prepared-claim.json"
        prompt_path = repo / ".agent-workspace" / "real_agent_prompt.md"
        _prompt(prompt_path)
        driver = wsl_path(args.distro, SUPPORT / "wsl_real_agent_driver.py")
        guarded = wsl_path(args.distro, SUPPORT / "wsl_guarded_entry.py")
        provider = wsl_path(args.distro, SUPPORT / "wsl_codex_provider.py")
        cgroup_launcher = wsl_path(args.distro, SUPPORT / "cgroup_exec.py")
        evidence_dir = temp_root / "prepared-evidence"
        prep_command = [
            "wsl.exe", "-d", args.distro, "-u", "root", "--", "python3", guarded,
            "--run-id", run_id, "--driver", driver, "--",
            "--mode", "prepare", "--nonce", nonce, "--invocation-id", WORKER_ID,
            "--state", wsl_path(args.distro, state_path),
            "--release-signal", wsl_path(args.distro, release_signal),
            "--evidence", wsl_path(args.distro, evidence_dir),
            "--auth-json", wsl_path(args.distro, auth),
            "--workspace", wsl_path(args.distro, repo),
            "--codex-root", args.codex_root,
            "--cgroup-launcher", cgroup_launcher,
            "--provider-entry", provider,
            "--wait-seconds", "900",
        ]
        prep_process = subprocess.Popen(
            prep_command, cwd=str(REPOSITORY_ROOT), stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=False,
            creationflags=WINDOWS_CREATE_NO_WINDOW,
        )
        prepared = _wait_json(state_path, lambda value: value.get("status") == "READY", timeout=90, process=prep_process)
        validate_prepared_state(prepared, nonce=nonce, invocation_id=WORKER_ID)
        if prepared.get("credentials_in_state") is not False:
            raise RuntimeError("prepared state claims to contain credentials")
        provider_command = [
            "wsl.exe", "-d", args.distro, "-u", "root", "--", "python3", provider,
            "--state", wsl_path(args.distro, state_path), "--nonce", nonce,
            "--invocation-id", WORKER_ID, "--evidence", wsl_path(args.distro, bridge_evidence),
            "--claim", wsl_path(args.distro, claim_path),
        ]
        invocation = _make_invocation(repo, base_commit, common_dir, runtime, prompt_path, provider_command)
        # Deterministic preflight: the controller must accept the invocation before it is launched.
        load_invocation(invocation)
        receipt_path = repo / ".agent-workspace" / "real_agent_operator.receipt.json"
        status_path = repo / ".agent-workspace" / "real_agent_controller.status.json"
        receipt = launch_lane_controller(
            invocation, receipt=receipt_path, cwd=REPOSITORY_ROOT,
            label="real-agent-coding-controller", role="coding-lane-controller",
            expected_state_path=status_path,
        )
        controller_pid = receipt.get("pid")
        controller_created = receipt.get("created_utc")
        if not isinstance(controller_pid, int) or not isinstance(controller_created, str):
            raise TypeError("Windows controller receipt lacks exact identity")
        provider_observed = False
        provider_identity: dict[str, Any] | None = None
        event_types: list[str] = []
        watcher_failures: list[str] = []
        config_path = temp_root / "watcher.json"
        watcher_root = temp_root / "runtime" / "watcher-state"
        _json(config_path, {
            "suite_root": str(temp_root), "run_globs": ["synthetic-repository"],
            "workspace_relpath": ".agent-workspace", "output_dir": str(watcher_root),
            "poll_interval_seconds": 0.1, "watch_timeout_seconds": 900,
            "request_warning_seconds": 900, "request_critical_seconds": 899,
            "process_start_tolerance_seconds": 2, "stable_read_delay_seconds": 0.02,
        })
        config = load_config(config_path)
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline:
            candidate = _read_object(status_path)
            if candidate is not None:
                status = candidate
                _validate_controller_receipt_identity(receipt, status)
                provider_pid = candidate.get("provider_pid")
                provider_created = candidate.get("provider_created_utc")
                if isinstance(provider_pid, int) and isinstance(provider_created, str) and not provider_observed:
                    query = targeted_process_query(provider_pid, expected_parent_pid=controller_pid)
                    provider_identity = _provider_identity_from_query(
                        query, provider_pid=provider_pid, provider_created=provider_created,
                        controller_pid=controller_pid, nonce=nonce, invocation_id=WORKER_ID,
                    )
                    provider_observed = True
            try:
                _, observed_events = watch_once(config, no_write=False)
                for item in observed_events:
                    if not isinstance(item, dict):
                        continue
                    event_type = item.get("type")
                    if isinstance(event_type, str):
                        event_types.append(event_type)
            except Exception as watcher_error:  # noqa: BLE001
                # A watcher observation is diagnostic; native controller state
                # and lifecycle evidence remain decisive for this route.
                watcher_failures.append(f"{type(watcher_error).__name__}: {watcher_error}")
            controller_query = targeted_process_query(controller_pid)
            terminal = (
                status is not None
                and status.get("state") in {"CODEX_EXITED", "PROVIDER_EXITED", "CONTROLLER_FAILED", "LAUNCH_FAILED"}
                and status.get("ended_utc") is not None
            )
            if terminal and controller_query.process is None:
                break
            time.sleep(0.1)
        else:
            raise TimeoutError("Windows public controller did not reach terminal state")
        if status is None:
            raise RuntimeError("controller status was never published")
        _validate_controller_receipt_identity(receipt, status)
        if not provider_observed or provider_identity is None:
            raise RuntimeError("exact Windows wsl.exe provider identity was not observed")
        if status.get("state") not in {"CODEX_EXITED", "PROVIDER_EXITED"} or status.get("exit_code") != 0:
            raise RuntimeError(f"public controller did not exit successfully: {status.get('state')}")
        if status.get("result_valid") is not True or not isinstance(status.get("result_validation"), dict) or status["result_validation"].get("state") != "VALID":
            raise RuntimeError("controller did not validate a distinct successful result")
        if status.get("held_resource_claims") != [] or not all(status.get(key) is True for key in ("helpers_complete", "direct_child_reaped", "resource_claim_release_safe")):
            raise RuntimeError("native lifecycle/claim cleanup evidence is incomplete")
        boundary = status.get("process_boundary")
        if not isinstance(boundary, dict) or boundary.get("complete") is not True or boundary.get("live_members"):
            raise RuntimeError("native controller process boundary is incomplete")
        result_path = repo / ".agent-workspace" / "RESULT.json"
        result_value = _read_object(result_path)
        if result_value is None:
            raise RuntimeError("controller result is missing")
        actual_branch = _run(repo, "git", "branch", "--show-current")
        actual_head = _run(repo, "git", "rev-parse", "HEAD")
        dirty = _run(repo, "git", "status", "--porcelain=v1", "--untracked-files=all")
        validation = status["result_validation"]
        if result_value.get("lane_id") != LANE_ID or result_value.get("worker_invocation_id") != WORKER_ID or result_value.get("branch") != actual_branch or result_value.get("commit") != actual_head or validation.get("commit") != actual_head or actual_branch != LANE_ID or dirty:
            raise RuntimeError("result identity is not exact and distinct from stale state")
        lifecycle_path = lifecycle_registry_path(repo, LANE_ID, WORKER_ID)
        lifecycle = _read_object(lifecycle_path)
        if lifecycle is None or lifecycle.get("lifecycle", {}).get("complete") is not True or lifecycle.get("lifecycle", {}).get("helpers_complete") is not True:
            raise RuntimeError("native lifecycle registry is incomplete")
        bridge = _wait_json(bridge_evidence, lambda value: value.get("status") == "PASS", timeout=30)
        prepared_cleaned = _wait_json(state_path, lambda value: value.get("status") == "CLEANED", timeout=60, process=prep_process)
        if prepared_cleaned.get("cleanup_complete") is not True or prepared_cleaned.get("credentials_in_state") is not False:
            raise RuntimeError("prepared Linux cleanup was not complete")
        linux_bridge = bridge.get("linux_bridge")
        if not isinstance(linux_bridge, dict):
            raise TypeError("Linux bridge identity evidence is missing")
        validate_cross_os_identity_relation(
            provider_identity, linux_bridge, nonce=nonce, invocation_id=WORKER_ID
        )
        if bridge.get("sandbox", {}).get("mnt_c_exposed") is not False or bridge.get("sandbox", {}).get("usb_exposed") is not False or bridge.get("cleanup_complete") is not True:
            raise RuntimeError("Linux sandbox/cleanup evidence is incomplete")
        required_events = {"CONTROLLER_ACTIVE", "CONTROLLER_EXITED", "RESOURCE_RELEASE_POSSIBLE"}
        if not required_events.issubset(set(event_types)):
            raise RuntimeError(
                f"native watcher events are incomplete: {sorted(set(event_types))}"
                + (f"; watcher failures: {watcher_failures}" if watcher_failures else "")
            )
        result = _safe_success_record(
            completed_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            route="public_launch -> operator_launch -> lane_controller -> wsl.exe provider bridge",
            distro=args.distro, codex_root=args.codex_root,
            nonce=nonce, invocation_id=WORKER_ID,
            controller_pid=controller_pid, controller_created=controller_created,
            provider_identity=provider_identity, linux_bridge_identity=linux_bridge,
            status=status, result_value=result_value, lifecycle=lifecycle,
            bridge=bridge, prepared=prepared_cleaned, event_types=event_types,
            workspace=repo / ".agent-workspace",
            receipt_path=receipt_path, result_path=result_path,
            lifecycle_path=lifecycle_path, bridge_evidence=bridge_evidence,
            state_path=state_path, claim_path=claim_path, prompt_path=prompt_path,
        )
    except BaseException as exc:  # noqa: BLE001
        failure = exc
    finally:
        published = _terminal_finalize(
            root_binding=root_binding,
            attempt_name=attempt_name,
            temp_root=temp_root,
            failure=failure,
            result=result,
            prep_process=prep_process,
            release_signal=release_signal,
        )
        print(f"REAL_AGENT_EVIDENCE={published}", flush=True)
        # No runtime, authentication material, or provider transcript is
        # retained in source or copied from the disposable repository.
    assert result is not None
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
