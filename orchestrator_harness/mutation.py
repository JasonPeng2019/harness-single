"""Identity-bound local mutation primitives used by S4-managed state.

The public callers provide a verified root, a single relative destination, and
the target state they authorized.  This module owns the last-use proof and the
temporary/replace/delete mechanics.  A pathname check is never treated as an
atomic boundary. POSIX uses descriptor-relative, no-follow operations and
revalidates every held ancestor; Windows holds no-delete directory handles
opened with ``FILE_FLAG_OPEN_REPARSE_POINT``. Neither advisory locks nor POSIX
directory descriptors prevent a hostile process from renaming directories.
An observed change is a conflict, never permission to follow the new path.
"""

from __future__ import annotations

import ctypes
import errno
import hashlib
import os
import stat
import struct
import sys
import tempfile
import uuid
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath
from typing import BinaryIO

from .portable_paths import component as portable_component


class MutationError(OSError):
    """Base class for a failed identity-bound mutation."""


class MutationConflict(MutationError):
    """The captured parent or expected target no longer matches."""


class MutationUnsupported(MutationError):
    """The host cannot provide the required anchored no-follow operation."""


@dataclass(frozen=True)
class TargetState:
    """The exact state authorized for one destination entry."""

    present: bool
    kind: str
    identity: tuple[int, int] | None
    content_sha256: str | None
    size: int | None
    content: bytes | None = None

    @classmethod
    def absent(cls) -> "TargetState":
        return cls(False, "absent", None, None, None, None)


@dataclass(frozen=True)
class CapturedTree:
    """A complete plain-entry census carried from validation into removal."""

    root: Path
    relative: Path
    root_identity: tuple[int, int]
    entries: tuple[tuple[Path, TargetState], ...]


DirectoryExpectations = tuple[tuple[Path, TargetState], ...]


@dataclass(frozen=True)
class FileMetadata:
    mode: int
    atime_ns: int
    mtime_ns: int


@dataclass(frozen=True)
class MutationReceipt:
    operation: str
    root: str
    parent: str
    target: str
    expected: TargetState
    resulting: TargetState
    temporary: str | None = None


def _before_commit() -> None:
    """Internal adversarial seam; production callers cannot select it."""


def _after_anchor() -> None:
    """Internal seam immediately after the root/parent capability is acquired."""


def _after_target_proof() -> None:
    """Internal seam immediately after the final expected-target proof."""


def _lexical(value: str | Path) -> Path:
    _validate_directory_input(value)
    return Path(os.path.abspath(os.fspath(value)))


def _is_reparse(path: Path) -> bool:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(info.st_mode):
        return True
    return bool(
        getattr(info, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _identity(info: os.stat_result) -> tuple[int, int]:
    return int(info.st_dev), int(info.st_ino)


def _windows_close(handle: object) -> None:
    if not _CloseHandle(handle):
        raise MutationError(f"native handle close failed ({ctypes.get_last_error()})")


def _windows_identity(handle: object) -> tuple[int, int]:
    # Python >=3.12 uses the full volume serial and 128-bit FILE_ID_INFO.
    if sys.version_info < (3, 12):
        legacy = _ByHandleFileInformation()
        if not _GetFileInformationByHandle(handle, ctypes.byref(legacy)):
            raise MutationUnsupported("native file identity is unavailable")
        return int(legacy.dwVolumeSerialNumber), (int(legacy.nFileIndexHigh) << 32) | int(legacy.nFileIndexLow)
    info = _FileIdInfo()
    if not _GetFileInformationByHandleEx(handle, 18, ctypes.byref(info), ctypes.sizeof(info)):
        raise MutationUnsupported("native full-width file identity is unavailable")
    return int(info.VolumeSerialNumber), int.from_bytes(bytes(info.FileId), "little")


def _has_ads(path: Path) -> bool:
    drive = path.drive
    for part in path.parts:
        if part in {drive, path.anchor, "\\", "/"}:
            continue
        if ":" in part:
            return True
    return False


def safe_relative_path(value: str | os.PathLike[str]) -> Path:
    """Return one conservative path safe for use below a verified root.

    Both host and Windows grammar are evaluated so a Windows-rooted or
    drive-relative spelling cannot become an escape when the same manifest or
    test is handled on another host.
    """

    try:
        raw = os.fspath(value)
    except TypeError as exc:
        raise MutationConflict("mutation target must be a safe relative path") from exc
    if not isinstance(raw, str) or not raw:
        raise MutationConflict("mutation target must be a safe relative path")
    if any(part in {"", ".", ".."} for part in raw.replace("\\", "/").split("/")):
        raise MutationConflict("mutation target contains an ambiguous component")
    try:
        for part in raw.replace("\\", "/").split("/"):
            portable_component(part)
    except ValueError as exc:
        raise MutationConflict(str(exc)) from exc
    if ":" in raw:
        raise MutationConflict(
            "mutation target contains alternate-stream or drive syntax"
        )

    host = Path(raw)
    windows = PureWindowsPath(raw)
    for candidate in (host, windows):
        if (
            candidate.is_absolute()
            or candidate.anchor
            or candidate.root
            or candidate.drive
            or not candidate.parts
            or any(part in {"", ".", ".."} for part in candidate.parts)
        ):
            raise MutationConflict("mutation target must be a safe relative path")

    return Path(*windows.parts)


def _validate_directory_input(value: str | os.PathLike[str]) -> None:
    """Reject ambiguous Windows spellings before observing a full path."""

    try:
        raw = os.fspath(value)
    except TypeError as exc:
        raise MutationConflict("directory path is not safe") from exc
    if not isinstance(raw, str) or not raw:
        raise MutationConflict("directory path is not safe")
    components = raw.replace("\\", "/").split("/")
    if any(part in {".", ".."} for part in components):
        raise MutationConflict("directory path contains an ambiguous component")
    host = Path(raw)
    windows = PureWindowsPath(raw)
    if windows.drive.startswith("\\\\") or (not host.is_absolute() and windows.anchor):
        raise MutationConflict("directory path is not safe")
    if not host.is_absolute():
        safe_relative_path(raw)
    else:
        try:
            for part in host.parts[1:]:
                portable_component(part)
        except ValueError as exc:
            raise MutationConflict(str(exc)) from exc


def _relative(root: Path, value: str | Path) -> Path:
    raw = os.fspath(value)
    candidate = Path(raw)
    if candidate.is_absolute():
        _validate_directory_input(raw)
        try:
            candidate = candidate.relative_to(root)
        except ValueError as exc:
            raise MutationConflict(
                "mutation target is outside its verified root"
            ) from exc
        return safe_relative_path(candidate)
    return safe_relative_path(raw)


def _require_exact_entry(parent: Path, name: str) -> None:
    """Reject case and native short-name aliases before opening an entry."""
    matches = [item for item in os.listdir(parent) if item.casefold() == name.casefold()]
    if matches and matches != [name]:
        raise MutationConflict(f"entry has an ambiguous portable spelling: {parent / name}")
    if os.name == "nt" and not matches and os.path.lexists(parent / name):
        raise MutationConflict(f"entry uses a native path alias: {parent / name}")


def _validate_chain(root: Path, relative: Path) -> tuple[Path, str]:
    root = _lexical(root)
    root_info = root.lstat()
    if not stat.S_ISDIR(root_info.st_mode) or _is_reparse(root):
        raise MutationConflict(f"mutation root is not a regular directory: {root}")
    target = root / relative
    current = root
    for part in relative.parts[:-1]:
        _require_exact_entry(current, part)
        current = current / part
        info = current.lstat()
        if _is_reparse(current) or not stat.S_ISDIR(info.st_mode):
            raise MutationConflict(f"mutation parent is unsafe: {current}")
    parent = target.parent
    if not parent.is_dir() or _is_reparse(parent):
        raise MutationConflict(f"mutation parent is unsafe: {parent}")
    _require_exact_entry(parent, target.name)
    return parent, target.name


def _capture_path(path: Path, *, include_content: bool = True) -> TargetState:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return TargetState.absent()
    if _is_reparse(path) or stat.S_ISLNK(info.st_mode):
        raise MutationConflict(f"mutation target is a reparse point: {path}")
    identity = _identity(info)
    if stat.S_ISDIR(info.st_mode):
        return TargetState(True, "directory", identity, None, None, None)
    if not stat.S_ISREG(info.st_mode):
        raise MutationConflict(f"mutation target is not a regular file: {path}")
    if info.st_nlink != 1:
        raise MutationConflict(f"mutation target has multiple hard links: {path}")
    if not include_content:
        return TargetState(True, "file", identity, None, int(info.st_size), None)
    try:
        content = read_bytes(path.parent, path.name)
        after = path.lstat()
    except OSError as exc:
        raise MutationConflict(f"mutation target cannot be read: {path}") from exc
    if (
        _is_reparse(path)
        or _identity(after) != identity
        or int(after.st_size) != len(content)
    ):
        raise MutationConflict(f"mutation target changed while it was captured: {path}")
    return TargetState(
        True,
        "file",
        identity,
        hashlib.sha256(content).hexdigest(),
        len(content),
        content,
    )


def capture_target(root: str | Path, relative: str | Path) -> TargetState:
    """Capture one safe target, including exact regular-file bytes when present."""

    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    parent, name = _validate_chain(root_path, rel)
    with _DirectoryAnchor(root_path, parent) as anchor:
        anchor.assert_stable()
        return _capture_anchored(anchor, name, include_content=True)


def _same_state(actual: TargetState, expected: TargetState) -> bool:
    if actual.present != expected.present or actual.kind != expected.kind:
        return False
    if not actual.present:
        return True
    return (
        actual.identity == expected.identity
        and (expected.content_sha256 is None or actual.content_sha256 == expected.content_sha256)
        and (expected.size is None or actual.size == expected.size)
    )


def _require_expected(path: Path, expected: TargetState) -> TargetState:
    actual = _capture_path(path, include_content=True)
    if not _same_state(actual, expected):
        raise MutationConflict(f"mutation target changed after authorization: {path}")
    return actual


def _require_supported_root_boundary() -> None:
    """Require actual no-follow and descriptor-relative kernel capabilities."""
    if os.name == "nt":
        return
    required = (os.open, os.stat, os.mkdir, os.unlink, os.rmdir, os.rename, os.readlink)
    if (os.name != "posix" or not hasattr(os, "O_NOFOLLOW")
            or not hasattr(os, "O_DIRECTORY")
            or any(call not in os.supports_dir_fd for call in required)):
        raise MutationUnsupported("host lacks anchored no-follow filesystem primitives")


def _windows_file_handle(
    path: Path,
    *,
    access: int,
    share: int,
    creation: int | None = None,
) -> tuple[object, tuple[int, int]]:
    if os.name != "nt":
        raise MutationUnsupported("Windows file backend is unavailable")
    handle = _CreateFileW(
        str(path),
        access,
        share,
        None,
        _OPEN_EXISTING if creation is None else creation,
        _FILE_ATTRIBUTE_NORMAL | _FILE_FLAG_OPEN_REPARSE_POINT,
        None,
    )
    if handle in (None, _INVALID_HANDLE_VALUE):
        error = ctypes.get_last_error()
        raise MutationUnsupported(f"Windows file handle unavailable ({error}): {path}")
    info = _ByHandleFileInformation()
    if not _GetFileInformationByHandle(handle, ctypes.byref(info)):
        error = ctypes.get_last_error()
        _windows_close(handle)
        raise MutationUnsupported(
            f"Windows file identity unavailable ({error}): {path}"
        )
    if info.dwFileAttributes & _FILE_ATTRIBUTE_REPARSE_POINT:
        _windows_close(handle)
        raise MutationConflict(f"Windows target is a reparse point: {path}")
    if info.dwFileAttributes & 0x00000010:
        _windows_close(handle)
        raise MutationConflict(
            f"Windows target is a directory, not a regular file: {path}"
        )
    if info.nNumberOfLinks != 1:
        _windows_close(handle)
        raise MutationConflict(f"Windows target has multiple hard links: {path}")
    try:
        return handle, _windows_identity(handle)
    except BaseException:
        _windows_close(handle)
        raise


def _windows_directory_handle(path: Path, expected: TargetState) -> object:
    if os.name != "nt":
        raise MutationUnsupported("Windows directory backend is unavailable")
    handle = _CreateFileW(
        str(path),
        _GENERIC_READ | _DELETE | _FILE_LIST_DIRECTORY,
        _FILE_SHARE_READ | _FILE_SHARE_WRITE,
        None,
        _OPEN_EXISTING,
        _FILE_FLAG_BACKUP_SEMANTICS | _FILE_FLAG_OPEN_REPARSE_POINT,
        None,
    )
    if handle in (None, _INVALID_HANDLE_VALUE):
        error = ctypes.get_last_error()
        raise MutationUnsupported(
            f"Windows directory delete handle unavailable ({error}): {path}"
        )
    try:
        info = _ByHandleFileInformation()
        if not _GetFileInformationByHandle(handle, ctypes.byref(info)):
            error = ctypes.get_last_error()
            raise MutationUnsupported(
                f"Windows directory identity unavailable ({error}): {path}"
            )
        if info.dwFileAttributes & _FILE_ATTRIBUTE_REPARSE_POINT:
            raise MutationConflict(f"Windows directory is a reparse point: {path}")
        if not (info.dwFileAttributes & 0x00000010):
            raise MutationConflict(f"Windows target is not a directory: {path}")
        actual_info = path.stat()
        if expected.identity != _identity(actual_info):
            raise MutationConflict(
                f"mutation directory changed while acquiring its handle: {path}"
            )
        return handle
    except Exception:
        _windows_close(handle)
        raise


def _windows_mark_handle_deleted(handle: object, path: Path) -> None:
    disposition = _FileDispositionInfo(True)
    if not _SetFileInformationByHandle(
        handle,
        _FILE_DISPOSITION_INFO,
        ctypes.byref(disposition),
        ctypes.sizeof(disposition),
    ):
        error = ctypes.get_last_error()
        raise MutationUnsupported(
            f"Windows handle-bound delete unavailable ({error}): {path}"
        )


def _windows_remove_directory(path: Path, expected: TargetState) -> None:
    _remove_anchored_tree(path.parent, Path(path.name), expected)


def _windows_python_file(native: object, mode: str, flags: int) -> object:
    import msvcrt
    try:
        descriptor = msvcrt.open_osfhandle(int(native), flags | os.O_BINARY)
    except BaseException:
        _windows_close(native)
        raise
    try:
        return os.fdopen(descriptor, mode, closefd=True)
    except BaseException:
        os.close(descriptor)
        raise


def _capture_open_file(handle: object) -> TargetState:
    before = os.fstat(handle.fileno())
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise MutationConflict("opened target is not one plain regular file")
    handle.seek(0)
    content = handle.read()
    after = os.fstat(handle.fileno())
    fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_nlink")
    if len(content) != after.st_size or any(getattr(before, key) != getattr(after, key) for key in fields):
        raise MutationConflict("opened target changed while observing its bytes")
    return TargetState(True, "file", _identity(after), hashlib.sha256(content).hexdigest(), len(content), content)


def _windows_open_existing_for_update(path: Path, expected: TargetState) -> object:
    """Bind an update handle to the exact authorized regular file."""
    native, _ = _windows_file_handle(path, access=_GENERIC_READ | _GENERIC_WRITE, share=_FILE_SHARE_READ)
    handle = _windows_python_file(native, "r+b", os.O_RDWR)
    try:
        actual = _capture_open_file(handle)
        if not _same_state(actual, expected):
            raise MutationConflict("mutation target changed while acquiring its handle")
        handle.seek(0)
        return handle
    except BaseException:
        handle.close()
        raise


def _windows_delete_existing(path: Path, expected: TargetState) -> None:
    """Delete only the exact opened regular file, always closing its handle."""
    native, _ = _windows_file_handle(path, access=_GENERIC_READ | _DELETE, share=_FILE_SHARE_READ)
    with _windows_python_file(native, "rb", os.O_RDONLY) as handle:
        actual = _capture_open_file(handle)
        if not _same_state(actual, expected):
            raise MutationConflict("mutation target changed while acquiring delete handle")
        import msvcrt
        _windows_mark_handle_deleted(msvcrt.get_osfhandle(handle.fileno()), path)


class AnchoredAppendFile:
    """A file handle whose parent/root capability remains held until close."""

    def __init__(self, handle: object, anchor: "_DirectoryAnchor") -> None:
        self.handle = handle
        self.anchor = anchor

    def __enter__(self) -> "AnchoredAppendFile":
        return self

    def __getattr__(self, name: str) -> object:
        return getattr(self.handle, name)

    def close(self) -> None:
        try:
            self.handle.close()
        finally:
            self.anchor.close()

    def __enter_file__(self) -> object:
        return self.handle

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


if os.name == "nt":
    import ctypes.wintypes as _wintypes

    _GENERIC_READ = 0x80000000
    _GENERIC_WRITE = 0x40000000
    _DELETE = 0x00010000
    _FILE_LIST_DIRECTORY = 0x0001
    _FILE_READ_ATTRIBUTES = 0x0080
    _FILE_SHARE_READ = 0x00000001
    _FILE_SHARE_WRITE = 0x00000002
    _FILE_SHARE_DELETE = 0x00000004
    _OPEN_EXISTING = 3
    _OPEN_ALWAYS = 4
    _CREATE_NEW = 1
    _FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
    _FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
    _FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
    _FILE_ATTRIBUTE_NORMAL = 0x00000080
    _INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
    _MOVEFILE_FAIL_IF_EXISTS = 0x00000001
    _MOVEFILE_WRITE_THROUGH = 0x00000008

    class _ByHandleFileInformation(ctypes.Structure):
        _fields_ = [
            ("dwFileAttributes", ctypes.wintypes.DWORD),
            ("ftCreationTime", ctypes.wintypes.FILETIME),
            ("ftLastAccessTime", ctypes.wintypes.FILETIME),
            ("ftLastWriteTime", ctypes.wintypes.FILETIME),
            ("dwVolumeSerialNumber", ctypes.wintypes.DWORD),
            ("nFileSizeHigh", ctypes.wintypes.DWORD),
            ("nFileSizeLow", ctypes.wintypes.DWORD),
            ("nNumberOfLinks", ctypes.wintypes.DWORD),
            ("nFileIndexHigh", ctypes.wintypes.DWORD),
            ("nFileIndexLow", ctypes.wintypes.DWORD),
        ]

    class _FileIdInfo(ctypes.Structure):
        _fields_ = [("VolumeSerialNumber", ctypes.c_ulonglong), ("FileId", ctypes.c_ubyte * 16)]

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _CreateFileW = _kernel32.CreateFileW
    _CreateFileW.argtypes = [
        ctypes.wintypes.LPCWSTR,
        ctypes.wintypes.DWORD,
        ctypes.wintypes.DWORD,
        ctypes.c_void_p,
        ctypes.wintypes.DWORD,
        ctypes.wintypes.DWORD,
        ctypes.wintypes.HANDLE,
    ]
    _CreateFileW.restype = ctypes.wintypes.HANDLE
    _GetFileInformationByHandle = _kernel32.GetFileInformationByHandle
    _GetFileInformationByHandle.argtypes = [
        ctypes.wintypes.HANDLE,
        ctypes.POINTER(_ByHandleFileInformation),
    ]
    _GetFileInformationByHandle.restype = ctypes.wintypes.BOOL
    _GetFileInformationByHandleEx = _kernel32.GetFileInformationByHandleEx
    _GetFileInformationByHandleEx.argtypes = [ctypes.wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, ctypes.wintypes.DWORD]
    _GetFileInformationByHandleEx.restype = ctypes.wintypes.BOOL
    _GetFinalPathNameByHandleW = _kernel32.GetFinalPathNameByHandleW
    _GetFinalPathNameByHandleW.argtypes = [ctypes.wintypes.HANDLE, ctypes.wintypes.LPWSTR,
                                         ctypes.wintypes.DWORD, ctypes.wintypes.DWORD]
    _GetFinalPathNameByHandleW.restype = ctypes.wintypes.DWORD
    _QueryDosDeviceW = _kernel32.QueryDosDeviceW
    _QueryDosDeviceW.argtypes = [ctypes.wintypes.LPCWSTR, ctypes.wintypes.LPWSTR, ctypes.wintypes.DWORD]
    _QueryDosDeviceW.restype = ctypes.wintypes.DWORD
    _DeviceIoControl = _kernel32.DeviceIoControl
    _DeviceIoControl.argtypes = [ctypes.wintypes.HANDLE, ctypes.wintypes.DWORD,
        ctypes.c_void_p, ctypes.wintypes.DWORD, ctypes.c_void_p, ctypes.wintypes.DWORD,
        ctypes.POINTER(ctypes.wintypes.DWORD), ctypes.c_void_p]
    _DeviceIoControl.restype = ctypes.wintypes.BOOL
    _CloseHandle = _kernel32.CloseHandle
    _CloseHandle.argtypes = [ctypes.wintypes.HANDLE]
    _CloseHandle.restype = ctypes.wintypes.BOOL
    _SetFileInformationByHandle = _kernel32.SetFileInformationByHandle
    _SetFileInformationByHandle.argtypes = [
        ctypes.wintypes.HANDLE,
        ctypes.wintypes.DWORD,
        ctypes.c_void_p,
        ctypes.wintypes.DWORD,
    ]
    _SetFileInformationByHandle.restype = ctypes.wintypes.BOOL
    _MoveFileExW = _kernel32.MoveFileExW
    _MoveFileExW.argtypes = [
        ctypes.wintypes.LPCWSTR,
        ctypes.wintypes.LPCWSTR,
        ctypes.wintypes.DWORD,
    ]
    _MoveFileExW.restype = ctypes.wintypes.BOOL

    class _FileDispositionInfo(ctypes.Structure):
        _fields_ = [("DeleteFile", ctypes.wintypes.BOOL)]

    _FILE_DISPOSITION_INFO = 4

    class _UnicodeString(ctypes.Structure):
        _fields_ = [("Length", ctypes.c_ushort), ("MaximumLength", ctypes.c_ushort),
                    ("Buffer", ctypes.c_void_p)]

    class _ObjectAttributes(ctypes.Structure):
        _fields_ = [("Length", ctypes.c_ulong), ("RootDirectory", ctypes.c_void_p),
                    ("ObjectName", ctypes.POINTER(_UnicodeString)), ("Attributes", ctypes.c_ulong),
                    ("SecurityDescriptor", ctypes.c_void_p), ("SecurityQualityOfService", ctypes.c_void_p)]

    class _IoStatusBlock(ctypes.Structure):
        _fields_ = [("Status", ctypes.c_void_p), ("Information", ctypes.c_size_t)]

    _ntdll = ctypes.WinDLL("ntdll")
    _NtCreateFile = _ntdll.NtCreateFile
    _NtCreateFile.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_ulong,
        ctypes.POINTER(_ObjectAttributes), ctypes.POINTER(_IoStatusBlock), ctypes.c_void_p,
        ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_void_p, ctypes.c_ulong]
    _NtCreateFile.restype = ctypes.c_long
    _NtSetInformationFile = _ntdll.NtSetInformationFile
    _NtSetInformationFile.argtypes = [ctypes.c_void_p, ctypes.POINTER(_IoStatusBlock),
                                     ctypes.c_void_p, ctypes.c_ulong, ctypes.c_ulong]
    _NtSetInformationFile.restype = ctypes.c_long


def _windows_open_directory(path: Path, *, parent: object = None,
                            create: bool = False) -> tuple[object, tuple[int, int]]:
    """NtCreateFile refuses reparse traversal, including above an absolute root.

    Outside ancestors need no extra access grants or enumeration. Below the
    configured root, lookup is relative to an already held no-delete handle.
    There is no pathname-open fallback when the native capability fails.
    """
    if parent is not None:
        name = str(path)
    else:
        device = ctypes.create_unicode_buffer(32768)
        if not _QueryDosDeviceW(path.drive, device, len(device)):
            raise MutationUnsupported("cannot discover the configured root's native volume")
        if not device.value.startswith("\\Device\\"):
            raise MutationUnsupported("configured root uses an indirect native volume mapping")
        suffix = path.relative_to(path.anchor)
        name = device.value + ("\\" + str(suffix) if suffix.parts else "\\")
    buffer = ctypes.create_unicode_buffer(name)
    length = len(name.encode("utf-16-le"))
    if length > 65532:
        raise MutationConflict("native directory path is too long")
    unicode = _UnicodeString(length, length + 2, ctypes.cast(buffer, ctypes.c_void_p))
    attributes = _ObjectAttributes(ctypes.sizeof(_ObjectAttributes), parent,
                                  ctypes.pointer(unicode), 0x1000, None, None)  # OBJ_DONT_REPARSE
    handle = ctypes.c_void_p()
    status = _IoStatusBlock()
    result = _NtCreateFile(ctypes.byref(handle), 0x00100000 | _FILE_READ_ATTRIBUTES, ctypes.byref(attributes),
        ctypes.byref(status), None, 0, _FILE_SHARE_READ | _FILE_SHARE_WRITE,
        2 if create else 1, 0x00000001 | 0x00000020 | 0x00200000, None, 0)  # CREATE/OPEN, DIRECTORY, SYNC, NOFOLLOW
    if result < 0:
        if create and result & 0xffffffff == 0xc0000035:
            raise MutationConflict(f"directory appeared before native creation: {path}")
        if result & 0xffffffff == 0xc000050b:
            raise MutationConflict(f"native directory ancestry contains a reparse point: {path}")
        raise MutationUnsupported(f"native no-reparse directory open failed (0x{result & 0xffffffff:08x}): {path}")
    try:
        info = _ByHandleFileInformation()
        if not _GetFileInformationByHandle(handle, ctypes.byref(info)):
            raise MutationUnsupported("native directory identity unavailable")
        if info.dwFileAttributes & _FILE_ATTRIBUTE_REPARSE_POINT or not info.dwFileAttributes & 0x10:
            raise MutationConflict(f"native directory is redirected or not a directory: {path}")
        final = ctypes.create_unicode_buffer(32768)
        received = _GetFinalPathNameByHandleW(handle, final, len(final), 0)
        if not received or received >= len(final):
            raise MutationUnsupported("native canonical directory name unavailable")
        actual = Path(final.value.removeprefix("\\\\?\\"))
        expected = path.name if parent is not None else str(path).rstrip("\\")
        observed = actual.name if parent is not None else str(actual).rstrip("\\")
        if (observed != expected if parent is not None else os.path.normcase(observed) != os.path.normcase(expected)):
            raise MutationConflict(f"native directory has a path alias: {path}")
        return handle.value, _windows_identity(handle)
    except BaseException:
        _windows_close(handle)
        raise


class _DirectoryAnchor:
    """Hold the root/parent identity while a mutation is committed."""

    def __init__(self, root: Path, parent: Path) -> None:
        self.root = root
        self.parent = parent
        self.expected_root_identity: tuple[int, int] | None = None
        self.expected_parent_identity: tuple[int, int] | None = None
        self.root_identity: tuple[int, int] | None = None
        self.parent_identity: tuple[int, int] | None = None
        self.parent_fd: int | None = None
        self._fds: list[int] = []
        self._paths: list[Path] = []
        self._handles: list[object] = []
        self._temporaries: dict[str, tuple[int, int]] = {}
        try:
            self.expected_root_identity = _identity(self.root.lstat())
            self.expected_parent_identity = _identity(self.parent.lstat())
        except OSError as exc:
            raise MutationConflict("mutation directory disappeared before anchoring") from exc

    def __enter__(self) -> "_DirectoryAnchor":
        if self._fds or self._handles:
            raise MutationConflict("directory anchor already holds native handles")
        try:
            if os.name == "nt":
                self._enter_windows()
            else:
                self._enter_posix()
            if (
                self.root_identity != self.expected_root_identity
                or self.parent_identity != self.expected_parent_identity
            ):
                raise MutationConflict(
                    "mutation directory identity changed before anchoring"
                )
            return self
        except BaseException:
            self.close()
            raise

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def _enter_posix(self) -> None:
        _require_supported_root_boundary()
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
        root_fd = os.open(self.root.anchor, flags)
        self._fds.append(root_fd)
        current = Path(self.root.anchor)
        self._paths.append(current)
        for part in self.root.relative_to(self.root.anchor).parts:
            root_fd = os.open(part, flags, dir_fd=root_fd)
            self._fds.append(root_fd)
            current = current / part
            self._paths.append(current)
        root_info = os.fstat(root_fd)
        if not stat.S_ISDIR(root_info.st_mode):
            raise MutationUnsupported("POSIX root handle is not a directory")
        self.root_identity = _identity(root_info)
        current_fd = root_fd
        relative_parent = self.parent.relative_to(self.root)
        for part in relative_parent.parts:
            names = [name for name in os.listdir(current_fd) if name.casefold() == part.casefold()]
            if names != [part]:
                raise MutationConflict("mutation parent has an ambiguous portable spelling")
            next_fd = os.open(part, flags, dir_fd=current_fd)
            self._fds.append(next_fd)
            current_fd = next_fd
            current = current / part
            self._paths.append(current)
        parent_info = os.fstat(current_fd)
        if not stat.S_ISDIR(parent_info.st_mode):
            raise MutationUnsupported("POSIX parent handle is not a directory")
        self.parent_fd = current_fd
        self.parent_identity = _identity(parent_info)

    @staticmethod
    def _windows_open(path: Path) -> tuple[object, tuple[int, int]]:
        if os.name != "nt":
            raise MutationUnsupported("Windows directory backend is unavailable")
        return _windows_open_directory(path)

    def _enter_windows(self) -> None:
        root_handle, self.root_identity = self._windows_open(self.root)
        self._handles.append(root_handle)
        self.parent_identity = self.root_identity
        current = self.root
        for part in self.parent.relative_to(self.root).parts:
            _require_exact_entry(current, part)
            current = current / part
            handle, identity = _windows_open_directory(Path(part), parent=self._handles[-1])
            self._handles.append(handle)
            self.parent_identity = identity

    def assert_stable(self) -> None:
        if self.root_identity is None or self.parent_identity is None:
            raise MutationUnsupported("mutation directory anchor was not established")
        if _is_reparse(self.root) or _is_reparse(self.parent):
            raise MutationConflict("mutation directory became a reparse point")
        if os.name == "nt":
            # The no-delete handles are the authoritative anti-substitution
            # anchor.  A current identity check makes the evidence explicit.
            root_handle, root_identity = self._windows_open(self.root)
            try:
                parent_handle, parent_identity = self._windows_open(self.parent)
                _windows_close(parent_handle)
            finally:
                _windows_close(root_handle)
            if (
                root_identity != self.root_identity
                or parent_identity != self.parent_identity
            ):
                raise MutationConflict("Windows mutation directory identity changed")
            return
        for index, (path, descriptor) in enumerate(zip(self._paths, self._fds)):
            held = os.fstat(descriptor)
            named = (os.lstat(path) if index == 0 else
                     os.stat(path.name, dir_fd=self._fds[index - 1], follow_symlinks=False))
            if not stat.S_ISDIR(named.st_mode) or _identity(named) != _identity(held):
                raise MutationConflict("POSIX mutation directory identity changed")

    def stat(self, name: str) -> os.stat_result:
        self.assert_stable()
        if os.name != "nt":
            matches = [entry for entry in os.listdir(self.parent_fd)
                       if entry.casefold() == name.casefold()]
            if matches and matches != [name]:
                raise MutationConflict("target has an ambiguous portable spelling")
            return os.stat(name, dir_fd=self.parent_fd, follow_symlinks=False)
        _require_exact_entry(self.parent, name)
        return (self.parent / name).lstat()

    def close(self) -> None:
        errors: list[OSError] = []
        failed_fds: list[int] = []
        for fd in reversed(self._fds):
            try:
                os.close(fd)
            except OSError as exc:
                errors.append(exc)
                failed_fds.append(fd)
        self._fds = list(reversed(failed_fds))
        self._paths.clear()
        failed_handles: list[object] = []
        for handle in reversed(self._handles):
            try:
                _windows_close(handle)
            except OSError as exc:
                errors.append(exc)
                failed_handles.append(handle)
        self._handles = list(reversed(failed_handles))
        self.parent_fd = None
        self.root_identity = None
        self.parent_identity = None
        if errors:
            raise errors[0]


def read_bytes(
    root: str | Path, relative: str | Path, *, max_bytes: int | None = None,
    expected_root_identity: tuple[int, int] | None = None,
) -> bytes:
    """Read a regular file through held no-follow ancestors and one file handle.

    The size bound applies to that handle, including growth after the initial
    pathname observation. This primitive performs no filesystem mutation.
    """
    return _read_file_observation(root, relative, max_bytes=max_bytes,
                                  expected_root_identity=expected_root_identity)[0]


def _read_file_observation(
    root: str | Path, relative: str | Path, *, max_bytes: int | None = None,
    expected_root_identity: tuple[int, int] | None = None, tail: bool = False,
    hooks: bool = True,
) -> tuple[bytes, os.stat_result]:
    if max_bytes is not None and (type(max_bytes) is not int or max_bytes < 0):
        raise ValueError("read size limit must be a nonnegative integer")
    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    parent, name = _validate_chain(root_path, rel)
    target = root_path / rel
    with _DirectoryAnchor(root_path, parent) as anchor:
        if hooks:
            _after_anchor()
        anchor.assert_stable()
        if (expected_root_identity is not None
                and _identity(root_path.lstat()) != expected_root_identity):
            raise MutationConflict("read root identity changed after preparation")
        before = anchor.stat(name)
        if _is_reparse(target) or not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise MutationConflict(f"read target is not one plain regular file: {target}")
        if hooks:
            _after_target_proof()
        if os.name == "nt":
            import msvcrt
            native, _ = _windows_file_handle(target, access=_GENERIC_READ,
                share=_FILE_SHARE_READ | (_FILE_SHARE_WRITE if tail else 0))
            try:
                descriptor = msvcrt.open_osfhandle(int(native), os.O_RDONLY | os.O_BINARY)
            except BaseException:
                _windows_close(native)
                raise
        else:
            descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
                                 | getattr(os, "O_CLOEXEC", 0), dir_fd=anchor.parent_fd)
        try:
            file_handle = os.fdopen(descriptor, "rb")
        except BaseException:
            os.close(descriptor)
            raise
        with file_handle as handle:
            opened = os.fstat(handle.fileno())
            if (_identity(opened) != _identity(before) or not stat.S_ISREG(opened.st_mode)
                    or opened.st_nlink != 1):
                raise MutationConflict("read target changed before opening")
            if not tail and max_bytes is not None and opened.st_size > max_bytes:
                raise ValueError(f"read target exceeds its {max_bytes}-byte limit")
            offset = max(0, opened.st_size - max_bytes) if tail and max_bytes is not None else 0
            handle.seek(offset)
            data = handle.read() if max_bytes is None else handle.read(max_bytes + 1)
            after = os.fstat(handle.fileno())
            fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
            if (any(getattr(opened, field) != getattr(after, field) for field in fields)
                    or len(data) != opened.st_size - offset
                    or _identity(anchor.stat(name)) != _identity(opened)
                    or after.st_nlink != 1):
                raise MutationConflict("read target changed while reading")
            if max_bytes is not None and len(data) > max_bytes:
                raise ValueError(f"read target exceeds its {max_bytes}-byte limit")
            anchor.assert_stable()
            return data, after


def _capture_anchored(anchor: _DirectoryAnchor, name: str, *, include_content: bool) -> TargetState:
    try:
        info = anchor.stat(name)
    except FileNotFoundError:
        return TargetState.absent()
    target = anchor.parent / name
    if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
        raise MutationConflict(f"mutation target is a reparse point: {target}")
    if stat.S_ISDIR(info.st_mode):
        return TargetState(True, "directory", _identity(info), None, None)
    if not stat.S_ISREG(info.st_mode):
        raise MutationConflict(f"mutation target is not a regular file: {target}")
    if info.st_nlink != 1:
        raise MutationConflict(f"mutation target has multiple hard links: {target}")
    if not include_content:
        return TargetState(True, "file", _identity(info), None, info.st_size)
    data, after = _read_file_observation(anchor.root, target.relative_to(anchor.root),
                                       expected_root_identity=_identity(anchor.root.lstat()), hooks=False)
    if _identity(after) != _identity(info):
        raise MutationConflict(f"mutation target changed while it was captured: {target}")
    return TargetState(True, "file", _identity(after), hashlib.sha256(data).hexdigest(), len(data), data)


def _require_anchored_expected(anchor: _DirectoryAnchor, name: str, expected: TargetState) -> TargetState:
    actual = _capture_anchored(anchor, name, include_content=expected.content_sha256 is not None)
    if not _same_state(actual, expected):
        raise MutationConflict(f"mutation target changed after authorization: {anchor.parent / name}")
    return actual


def durable_identity(
    root: str | Path, relative: str | Path, *, expected: TargetState,
    expected_root_identity: tuple[int, int] | None = None,
) -> str:
    """Observe a version-independent native identity against the original proof.

    This identifies an object; it does not establish who created or owns it.
    Windows FILE_ID_INFO is used even when Python stat exposes a legacy ID.
    """
    root_path = _lexical(root)
    if os.fspath(relative) == ".":
        if not expected.present or expected.kind != "directory":
            raise MutationConflict("authority identity requires a captured plain directory")
        parent, name = root_path, None
    else:
        rel = _relative(root_path, relative)
        parent, name = _validate_chain(root_path, rel)
    with _DirectoryAnchor(root_path, parent) as anchor:
        if expected_root_identity is not None and anchor.root_identity != expected_root_identity:
            raise MutationConflict("durable identity authority changed")
        if name is None:
            if expected.identity != anchor.root_identity:
                raise MutationConflict("durable authority identity changed")
            handle = anchor._handles[0] if os.name == "nt" else anchor.parent_fd
            actual = expected
        else:
            actual = _require_anchored_expected(anchor, name, expected)
            if not actual.present:
                raise MutationConflict("durable identity target is absent")
        if os.name == "nt":
            close = False
            if name is not None:
                if actual.kind == "directory":
                    handle, identity = _windows_open_directory(Path(name), parent=anchor._handles[-1])
                else:
                    handle, _ = _windows_file_handle(parent / name, access=_GENERIC_READ, share=_FILE_SHARE_READ)
                    identity = _windows_identity(handle)
                close = True
            try:
                if name is not None and identity != actual.identity:
                    raise MutationConflict("durable identity target changed while opening")
                info = _FileIdInfo()
                if not _GetFileInformationByHandleEx(handle, 18, ctypes.byref(info), ctypes.sizeof(info)):
                    raise MutationUnsupported("stable native FILE_ID_INFO is unavailable")
                value = f"windows-file-id/v1:{int(info.VolumeSerialNumber):016x}:{bytes(info.FileId).hex()}"
            finally:
                if close:
                    _windows_close(handle)
        else:
            if name is None:
                info = os.fstat(handle)
            else:
                info = anchor.stat(name)
                if _identity(info) != actual.identity:
                    raise MutationConflict("durable identity target changed while observing")
            value = f"posix-inode/v1:{int(info.st_dev):x}:{int(info.st_ino):x}"
        if name is not None:
            _require_anchored_expected(anchor, name, expected)
        anchor.assert_stable()
        return value


def read_link_value(
    root: str | Path, relative: str | Path, *,
    expected_root_identity: tuple[int, int] | None = None,
) -> bytes:
    """Read the symbolic link's own value without opening its referent."""
    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    parent, name = _validate_chain(root_path, rel)
    with _DirectoryAnchor(root_path, parent) as anchor:
        _after_anchor()
        anchor.assert_stable()
        if expected_root_identity is not None and _identity(root_path.lstat()) != expected_root_identity:
            raise MutationConflict("link read root identity changed after preparation")
        before = anchor.stat(name)
        if not stat.S_ISLNK(before.st_mode) or before.st_nlink != 1:
            raise MutationConflict("link-value target is not one symbolic link")
        _after_target_proof()
        if os.name == "nt":
            handle = _CreateFileW(str(parent / name), _FILE_READ_ATTRIBUTES, _FILE_SHARE_READ,
                None, _OPEN_EXISTING, _FILE_FLAG_OPEN_REPARSE_POINT | _FILE_FLAG_BACKUP_SEMANTICS, None)
            if handle in (None, _INVALID_HANDLE_VALUE):
                raise MutationUnsupported(f"cannot open symbolic link ({ctypes.get_last_error()})")
            try:
                info = _ByHandleFileInformation()
                if not _GetFileInformationByHandle(handle, ctypes.byref(info)):
                    raise MutationUnsupported("cannot observe symbolic link identity")
                native_id = _windows_identity(handle)
                if native_id != _identity(before):
                    raise MutationConflict("symbolic link changed before opening")
                buffer = ctypes.create_string_buffer(16384)
                received = ctypes.wintypes.DWORD()
                if not _DeviceIoControl(handle, 0x000900A8, None, 0, buffer, len(buffer),
                                        ctypes.byref(received), None):
                    raise MutationUnsupported("cannot read symbolic link reparse value")
                raw = buffer.raw[:received.value]
                if len(raw) < 20 or struct.unpack_from("<I", raw)[0] != 0xA000000C:
                    raise MutationConflict("link-value target is not a native symbolic link")
                length = struct.unpack_from("<H", raw, 4)[0]
                sub_offset, sub_length, print_offset, print_length = struct.unpack_from("<HHHH", raw, 8)
                offset, count = (print_offset, print_length) if print_length else (sub_offset, sub_length)
                if length + 8 > len(raw) or offset % 2 or count % 2 or offset + count > length - 12:
                    raise MutationConflict("malformed native symbolic link value")
                value = os.fsencode(raw[20 + offset:20 + offset + count].decode("utf-16-le"))
            finally:
                _windows_close(handle)
        else:
            value = os.fsencode(os.readlink(name, dir_fd=anchor.parent_fd))
        after = anchor.stat(name)
        fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_nlink")
        if any(getattr(before, key) != getattr(after, key) for key in fields):
            raise MutationConflict("symbolic link changed during its value read")
        anchor.assert_stable()
        return value


def publish_bytes(
    root: str | Path, relative: str | Path, data: bytes, *,
    expected_root_identity: tuple[int, int] | None = None,
) -> None:
    """Atomically publish bytes; reject redirected, aliased or shared targets."""
    _publish(root, relative, data, expected=None, expected_root_identity=expected_root_identity)


def open_append_file(root: str | Path, relative: str | Path, *,
                     expected: TargetState | None = None,
                     expected_root_identity: tuple[int, int] | None = None,
                     expected_directories: DirectoryExpectations | None = None) -> AnchoredAppendFile:
    """Open/create one plain append file and retain all its ancestor handles."""
    _require_supported_root_boundary()
    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    parent, name = _validate_chain(root_path, rel)
    anchor = _DirectoryAnchor(root_path, parent)
    file_handle = None
    _before_commit()
    try:
        anchor.__enter__()
        _after_anchor()
        anchor.assert_stable()
        if expected_root_identity is not None and anchor.root_identity != expected_root_identity:
            raise MutationConflict("append authority changed after preparation")
        _require_anchored_directories(anchor, expected_directories)
        before = expected if expected is not None else _capture_anchored(anchor, name, include_content=False)
        _require_anchored_expected(anchor, name, before)
        if before.present and before.kind != "file":
            raise MutationConflict("append target is not a regular file")
        _after_target_proof()
        _require_anchored_directories(anchor, expected_directories)
        _require_anchored_expected(anchor, name, before)
        if os.name == "nt":
            native, _ = _windows_file_handle(parent / name,
                access=_GENERIC_READ | _GENERIC_WRITE,
                share=_FILE_SHARE_READ | _FILE_SHARE_WRITE,
                creation=_OPEN_EXISTING if before.present else _CREATE_NEW)
            import msvcrt
            try:
                descriptor = msvcrt.open_osfhandle(int(native), os.O_RDWR | os.O_BINARY)
            except BaseException:
                _windows_close(native)
                raise
        else:
            flags = os.O_RDWR | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
            if not before.present:
                flags |= os.O_CREAT | os.O_EXCL
            descriptor = os.open(name, flags, 0o600, dir_fd=anchor.parent_fd)
        try:
            file_handle = os.fdopen(descriptor, "a+b", closefd=True)
        except BaseException:
            os.close(descriptor)
            raise
        opened = os.fstat(file_handle.fileno())
        if (not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1
                or (before.present and _identity(opened) != before.identity)
                or _identity(anchor.stat(name)) != _identity(opened)):
            raise MutationConflict("append target changed while opening")
        if expected is not None and expected.present and not _same_state(_capture_open_file(file_handle), expected):
            raise MutationConflict("opened append bytes differ from authorization")
        anchor.assert_stable()
        return AnchoredAppendFile(file_handle, anchor)
    except BaseException:
        try:
            if file_handle is not None:
                file_handle.close()
        finally:
            anchor.close()
        raise


def _new_temp(anchor: _DirectoryAnchor, name: str) -> tuple[int, Path]:
    """Create the temporary entry through the captured parent when possible."""

    if os.name != "nt" and anchor.parent_fd is not None:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        for _ in range(32):
            candidate = f".{name}.{uuid.uuid4().hex}.tmp"
            try:
                descriptor = os.open(candidate, flags, 0o600, dir_fd=anchor.parent_fd)
                anchor._temporaries[candidate] = _identity(os.fstat(descriptor))
                return descriptor, Path(candidate)
            except FileExistsError:
                continue
        raise MutationUnsupported("could not allocate an anchored temporary entry")
    descriptor, raw = tempfile.mkstemp(prefix=f".{name}.", suffix=".tmp", dir=str(anchor.parent))
    path = Path(raw)
    anchor._temporaries[path.name] = _identity(os.fstat(descriptor))
    return descriptor, path


def _unlink_temp(anchor: _DirectoryAnchor, temporary: Path) -> None:
    try:
        expected = anchor._temporaries.get(temporary.name)
        if expected is None or _identity(anchor.stat(temporary.name)) != expected:
            return
        if os.name != "nt" and anchor.parent_fd is not None:
            os.unlink(temporary.name, dir_fd=anchor.parent_fd)
        else:
            temporary.unlink(missing_ok=True)
    except (FileNotFoundError, OSError):
        pass


def _posix_rename(source_fd: int, source: str, target_fd: int, target: str, *, replace_existing: bool) -> None:
    if replace_existing:
        os.replace(source, target, src_dir_fd=source_fd, dst_dir_fd=target_fd)
        return
    native = ctypes.CDLL(None, use_errno=True)
    if sys.platform.startswith("linux"):
        function = getattr(native, "renameat2", None)
        flag = 1  # RENAME_NOREPLACE
    elif sys.platform == "darwin":
        function = getattr(native, "renameatx_np", None)
        flag = 4  # RENAME_EXCL
    else:
        function = None
        flag = 0
    if function is None:
        raise MutationUnsupported("host lacks an atomic no-replace rename capability")
    function.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    function.restype = ctypes.c_int
    if function(source_fd, os.fsencode(source), target_fd, os.fsencode(target), flag):
        error = ctypes.get_errno()
        if error in {errno.ENOSYS, errno.EOPNOTSUPP, errno.EINVAL}:
            raise MutationUnsupported("filesystem lacks atomic no-replace rename")
        raise OSError(error, os.strerror(error), target)


def _windows_rename_handle(handle: object, destination: _DirectoryAnchor, name: str, *, replace_existing: bool) -> None:
    length = len(name.encode("utf-16-le"))
    class RenameInfo(ctypes.Structure):
        _fields_ = [("ReplaceIfExists", ctypes.wintypes.BOOL), ("RootDirectory", ctypes.wintypes.HANDLE),
                    ("FileNameLength", ctypes.wintypes.DWORD), ("FileName", ctypes.c_wchar * (length // 2 + 1))]
    info = RenameInfo(bool(replace_existing), destination._handles[-1], length, name)
    status = _IoStatusBlock()
    from . import records
    for attempt in range(records._WINDOWS_REPLACE_MAX_ATTEMPTS):
        result = _NtSetInformationFile(handle, ctypes.byref(status), ctypes.byref(info), ctypes.sizeof(info), 10)
        if result >= 0:
            return
        error = result & 0xffffffff
        if error not in {0xc0000022, 0xc0000043} or attempt + 1 >= records._WINDOWS_REPLACE_MAX_ATTEMPTS:
            raise MutationConflict(f"native handle-bound rename failed (0x{error:08x}): {name}")
        records.time.sleep(records._WINDOWS_REPLACE_RETRY_DELAY_SECONDS * (attempt + 1))


def _publish(root: str | Path, relative: str | Path, data: bytes, *,
             expected: TargetState | None, expected_root_identity: tuple[int, int] | None,
             expected_directories: DirectoryExpectations | None = None,
             metadata: FileMetadata | None = None) -> MutationReceipt:
    _require_supported_root_boundary()
    _validate_metadata(metadata)
    if not isinstance(data, bytes):
        raise TypeError("publication data must be bytes")
    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    parent, name = _validate_chain(root_path, rel)
    anchor = _DirectoryAnchor(root_path, parent)
    _before_commit()
    with anchor:
        temporary = None
        try:
            _after_anchor()
            anchor.assert_stable()
            if expected_root_identity is not None and _identity(root_path.lstat()) != expected_root_identity:
                raise MutationConflict("publication root identity changed after preparation")
            _require_anchored_directories(anchor, expected_directories)
            authorized = expected if expected is not None else _capture_anchored(anchor, name, include_content=True)
            _require_anchored_expected(anchor, name, authorized)
            if authorized.present and authorized.kind != "file":
                raise MutationConflict("replacement target is not a regular file")
            descriptor, raw = _new_temp(anchor, name)
            temporary = parent / Path(raw).name
            try:
                file_handle = os.fdopen(descriptor, "wb")
            except BaseException:
                os.close(descriptor)
                raise
            with file_handle as handle:
                if handle.write(data) != len(data):
                    raise MutationConflict("publication did not write all bytes")
                handle.flush()
                os.fsync(handle.fileno())
                if metadata is not None and os.name != "nt":
                    _apply_file_metadata(handle, metadata)
            _after_target_proof()
            anchor.assert_stable()
            _require_anchored_directories(anchor, expected_directories)
            _require_anchored_expected(anchor, name, authorized)
            staged = _capture_anchored(anchor, temporary.name, include_content=True)
            if staged.identity != anchor._temporaries[temporary.name] or staged.content != data:
                raise MutationConflict("publication temporary identity or bytes changed")
            if os.name == "nt":
                if authorized.present:
                    # Atomic named output replacement while the parent is pinned.
                    from .records import _replace_with_retry
                    _replace_with_retry(temporary, parent / name)
                else:
                    native, _ = _windows_file_handle(temporary, access=_GENERIC_READ | _DELETE | (0x0100 if metadata is not None else 0),
                                                     share=_FILE_SHARE_READ)
                    try:
                        _windows_rename_handle(native, anchor, name, replace_existing=False)
                        if metadata is not None:
                            _apply_windows_metadata(native, metadata)
                    finally:
                        _windows_close(native)
            else:
                _posix_rename(anchor.parent_fd, temporary.name, anchor.parent_fd, name,
                              replace_existing=authorized.present)
                os.fsync(anchor.parent_fd)
            temporary = None
            anchor.assert_stable()
            resulting = _capture_anchored(anchor, name, include_content=True)
            if resulting.identity != staged.identity or resulting.content != data:
                raise MutationConflict("publication result identity or bytes changed")
            return MutationReceipt("replace", str(root_path), str(parent), str(parent / name), authorized, resulting)
        finally:
            if temporary is not None:
                _unlink_temp(anchor, temporary)


def replace(
    root: str | Path,
    relative: str | Path,
    data: bytes,
    *,
    expected: TargetState | None = None,
    expected_root_identity: tuple[int, int] | None = None,
    expected_directories: DirectoryExpectations | None = None,
    metadata: FileMetadata | None = None,
) -> MutationReceipt:
    """Write the authorized file, preserving Windows target handle protection.

    An existing Windows file is updated through a no-write/no-delete-sharing
    handle, retaining its identity. This update is not atomic on write failure.
    Complete atomic named record publication uses publish_bytes separately.
    POSIX replacement is atomic but has no kernel expected-inode comparison;
    cooperating writers must hold their resource lock.
    """
    if os.name == "nt":
        return _windows_replace_bound(root, relative, data, expected=expected,
                                      expected_root_identity=expected_root_identity,
                                      expected_directories=expected_directories, metadata=metadata)
    return _publish(root, relative, data, expected=expected,
                    expected_root_identity=expected_root_identity,
                    expected_directories=expected_directories, metadata=metadata)


def _windows_replace_bound(root: str | Path, relative: str | Path, data: bytes, *,
                           expected: TargetState | None,
                           expected_root_identity: tuple[int, int] | None,
                           expected_directories: DirectoryExpectations | None = None,
                           metadata: FileMetadata | None = None) -> MutationReceipt:
    _validate_metadata(metadata)
    if not isinstance(data, bytes):
        raise TypeError("replacement data must be bytes")
    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    parent, name = _validate_chain(root_path, rel)
    anchor = _DirectoryAnchor(root_path, parent)
    _before_commit()
    with anchor:
        _after_anchor()
        anchor.assert_stable()
        if expected_root_identity is not None and anchor.root_identity != expected_root_identity:
            raise MutationConflict("replacement root identity changed after preparation")
        _require_anchored_directories(anchor, expected_directories)
        authorized = expected if expected is not None else _capture_anchored(anchor, name, include_content=True)
        _require_anchored_expected(anchor, name, authorized)
        _after_target_proof()
        _require_anchored_directories(anchor, expected_directories)
        _require_anchored_expected(anchor, name, authorized)
        if not authorized.present:
            # The native publication is NO_REPLACE, even if another entry
            # appears after this observation. The outer ancestors remain held.
            return _publish(root_path, rel, data, expected=authorized,
                            expected_root_identity=anchor.root_identity,
                            expected_directories=expected_directories, metadata=metadata)
        if authorized.kind != "file":
            raise MutationConflict("replacement target is not a regular file")
        target = parent / name
        with _windows_open_existing_for_update(target, authorized) as handle:
            # The acquisition validates every captured fact on this handle.
            # Its share mode excludes both foreign writes and target renames
            # throughout the actual write, truncate, flush and verification.
            handle.seek(0)
            if handle.write(data) != len(data):
                raise MutationConflict("handle-bound replacement did not write all bytes")
            handle.truncate(len(data))
            handle.flush()
            os.fsync(handle.fileno())
            resulting = _capture_open_file(handle)
            if (resulting.identity != authorized.identity or resulting.content != data
                    or _identity(anchor.stat(name)) != resulting.identity):
                raise MutationConflict("handle-bound replacement identity or bytes changed")
            if metadata is not None:
                _apply_file_metadata(handle, metadata)
            anchor.assert_stable()
            return MutationReceipt("replace", str(root_path), str(parent), str(target), authorized, resulting)


def delete(
    root: str | Path,
    relative: str | Path,
    *,
    expected: TargetState | None = None,
    expected_root_identity: tuple[int, int] | None = None,
) -> MutationReceipt:
    _require_supported_root_boundary()
    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    parent, name = _validate_chain(root_path, rel)
    anchor = _DirectoryAnchor(root_path, parent)
    authorized = expected if expected is not None else capture_target(root_path, rel)
    _before_commit()
    with anchor:
        _after_anchor()
        anchor.assert_stable()
        if expected_root_identity is not None and _identity(root_path.lstat()) != expected_root_identity:
            raise MutationConflict("deletion root identity changed after preparation")
        _require_anchored_expected(anchor, name, authorized)
        _after_target_proof()
        _require_anchored_expected(anchor, name, authorized)
        if authorized.present:
            if authorized.kind != "file":
                raise MutationConflict("file deletion requires a regular file; use remove_tree for directories")
            if os.name == "nt":
                _windows_delete_existing(parent / name, authorized)
            else:
                os.unlink(name, dir_fd=anchor.parent_fd)
        anchor.assert_stable()
        resulting = _capture_anchored(anchor, name, include_content=False)
        if resulting.present:
            raise MutationConflict("deletion could not prove target absence")
        return MutationReceipt("delete", str(root_path), str(parent), str(parent / name), authorized, resulting)


def _require_anchored_directories(anchor: _DirectoryAnchor,
                                  expected: DirectoryExpectations | None) -> None:
    if expected is None:
        return
    chain = [Path(".")]
    current = Path(".")
    for part in anchor.parent.relative_to(anchor.root).parts:
        current /= part
        chain.append(current)
    states = dict(expected)
    if len(states) != len(expected) or set(states) != set(chain):
        raise MutationConflict("captured directory chain is incomplete or belongs to another parent")
    anchor.assert_stable()
    for relative in chain:
        state = states[relative]
        if (not isinstance(relative, Path) or not isinstance(state, TargetState)
                or not state.present or state.kind != "directory" or state.identity is None):
            raise MutationConflict("captured parent state is not a plain directory")
        path = anchor.root / relative
        info = path.lstat()
        if (not stat.S_ISDIR(info.st_mode) or _is_reparse(path)
                or _identity(info) != state.identity):
            raise MutationConflict("publication directory changed after preflight")
    anchor.assert_stable()


def _validate_metadata(metadata: FileMetadata | None) -> None:
    if metadata is not None and (not isinstance(metadata, FileMetadata)
            or type(metadata.mode) is not int or not 0 <= metadata.mode <= 0o7777
            or type(metadata.atime_ns) is not int or type(metadata.mtime_ns) is not int):
        raise MutationConflict("file metadata is invalid")
    if metadata is not None and os.name == "nt" and any(
            not 0 < value // 100 + 116444736000000000 < 2**63
            for value in (metadata.atime_ns, metadata.mtime_ns)):
        raise MutationConflict("file time is outside the native Windows range")


def _apply_windows_metadata(native: object, metadata: FileMetadata) -> None:
    class BasicInformation(ctypes.Structure):
        _fields_ = [("CreationTime", ctypes.c_longlong), ("LastAccessTime", ctypes.c_longlong),
                    ("LastWriteTime", ctypes.c_longlong), ("ChangeTime", ctypes.c_longlong),
                    ("FileAttributes", ctypes.c_ulong)]
    before = _ByHandleFileInformation()
    if not _GetFileInformationByHandle(native, ctypes.byref(before)):
        raise MutationUnsupported("bound file metadata is unavailable")
    access = metadata.atime_ns // 100 + 116444736000000000
    modified = metadata.mtime_ns // 100 + 116444736000000000
    if not 0 < access < 2**63 or not 0 < modified < 2**63:
        raise MutationConflict("file time is outside the native Windows range")
    attributes = before.dwFileAttributes & ~1
    if not metadata.mode & stat.S_IWRITE:
        attributes |= 1
    basic = BasicInformation(0, access, modified, 0, attributes or 0x80)
    if not _SetFileInformationByHandle(native, 0, ctypes.byref(basic), ctypes.sizeof(basic)):
        raise MutationError(f"bound file metadata update failed ({ctypes.get_last_error()})")
    after = _ByHandleFileInformation()
    if not _GetFileInformationByHandle(native, ctypes.byref(after)):
        raise MutationUnsupported("updated bound file metadata is unavailable")
    actual_modified = (int(after.ftLastWriteTime.dwHighDateTime) << 32) | int(after.ftLastWriteTime.dwLowDateTime)
    if actual_modified != modified or bool(after.dwFileAttributes & 1) != (not bool(metadata.mode & stat.S_IWRITE)):
        raise MutationConflict("bound file metadata update could not be verified")


def _apply_file_metadata(handle: BinaryIO, metadata: FileMetadata) -> None:
    if os.name == "nt":
        import msvcrt
        _apply_windows_metadata(msvcrt.get_osfhandle(handle.fileno()), metadata)
    else:
        os.fchmod(handle.fileno(), metadata.mode)
        os.utime(handle.fileno(), ns=(metadata.atime_ns, metadata.mtime_ns))
        after = os.fstat(handle.fileno())
        if stat.S_IMODE(after.st_mode) != metadata.mode or after.st_mtime_ns != metadata.mtime_ns:
            raise MutationConflict("bound file metadata update could not be verified")


def create_directory(root: str | Path, relative: str | Path, *,
                     expected_root_identity: tuple[int, int] | None = None,
                     expected_directories: DirectoryExpectations | None = None) -> TargetState:
    """Create one absent child without adopting a late existing directory."""
    _require_supported_root_boundary()
    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    parent, name = _validate_chain(root_path, rel)
    anchor = _DirectoryAnchor(root_path, parent)
    _before_commit()
    with anchor:
        _after_anchor()
        if expected_root_identity is not None and anchor.root_identity != expected_root_identity:
            raise MutationConflict("directory creation authority changed after preflight")
        _require_anchored_directories(anchor, expected_directories)
        _require_anchored_expected(anchor, name, TargetState.absent())
        _after_target_proof()
        _require_anchored_directories(anchor, expected_directories)
        _require_anchored_expected(anchor, name, TargetState.absent())
        if os.name == "nt":
            native, identity = _windows_open_directory(Path(name), parent=anchor._handles[-1], create=True)
            try:
                result = _capture_anchored(anchor, name, include_content=False)
                if result.kind != "directory" or result.identity != identity:
                    raise MutationConflict("created directory identity changed")
                anchor.assert_stable()
                return result
            finally:
                _windows_close(native)
        try:
            os.mkdir(name, dir_fd=anchor.parent_fd)
        except FileExistsError as exc:
            raise MutationConflict("directory appeared before native creation") from exc
        os.fsync(anchor.parent_fd)
        result = _capture_anchored(anchor, name, include_content=False)
        if result.kind != "directory":
            raise MutationConflict("created directory is not plain")
        anchor.assert_stable()
        return result


def ensure_directory_path(path: str | Path) -> Path:
    """Create plain directories through held, no-follow ancestor capabilities."""
    _validate_directory_input(path)
    _require_supported_root_boundary()
    target = _lexical(path)
    missing = []
    current = target
    while not os.path.lexists(current):
        missing.append(current)
        if current.parent == current:
            raise MutationUnsupported("no existing directory anchor")
        current = current.parent
    if not current.is_dir() or _is_reparse(current):
        raise MutationConflict(f"directory anchor is unsafe: {current}")
    # Even an existing target must validate its whole ancestry.
    with _DirectoryAnchor(current, current) as existing:
        existing.assert_stable()
    for directory in reversed(missing):
        anchor = _DirectoryAnchor(current, current)
        _before_commit()
        with anchor:
            _after_anchor()
            anchor.assert_stable()
            _require_exact_entry(current, directory.name)
            _after_target_proof()
            anchor.assert_stable()
            try:
                if os.name == "nt":
                    directory.mkdir()
                else:
                    os.mkdir(directory.name, dir_fd=anchor.parent_fd)
                    os.fsync(anchor.parent_fd)
            except FileExistsError:
                pass
            result = _capture_anchored(anchor, directory.name, include_content=False)
            if not result.present or result.kind != "directory":
                raise MutationConflict("directory creation result is unsafe")
        current = directory
    with _DirectoryAnchor(target, target) as result:
        result.assert_stable()
    return target


def make_temporary_directory(root: str | Path, *, prefix: str) -> Path:
    _validate_directory_input(root)
    if not isinstance(prefix, str):
        raise MutationConflict("temporary directory prefix must be text")
    try:
        portable_component(prefix + "0" * 32)
    except ValueError as exc:
        raise MutationConflict("temporary directory prefix is unsafe") from exc
    _require_supported_root_boundary()
    root_path = _lexical(root)
    if not root_path.is_dir() or _is_reparse(root_path):
        raise MutationConflict(f"temporary root is unsafe: {root_path}")
    anchor = _DirectoryAnchor(root_path, root_path)
    _before_commit()
    with anchor:
        _after_anchor()
        anchor.assert_stable()
        _after_target_proof()
        anchor.assert_stable()
        if (
            os.name != "nt" and anchor.parent_fd is not None
        ):
            path = None
            for _ in range(32):
                candidate = f"{prefix}{uuid.uuid4().hex}"
                try:
                    os.mkdir(candidate, 0o700, dir_fd=anchor.parent_fd)
                except FileExistsError:
                    continue
                path = root_path / candidate
                break
            if path is None:
                raise MutationUnsupported(
                    "could not allocate an anchored temporary directory"
                )
            os.fsync(anchor.parent_fd)
        else:
            path = None
            for _ in range(32):
                candidate = root_path / f"{prefix}{uuid.uuid4().hex}"
                try:
                    candidate.mkdir()
                except FileExistsError:
                    continue
                path = candidate
                break
            if path is None:
                raise MutationUnsupported(
                    "could not allocate an anchored temporary directory"
                )
        anchor.assert_stable()
        if _is_reparse(path) or not path.is_dir():
            raise MutationConflict(f"temporary directory is unsafe: {path}")
        return path


def _windows_rename_entries(source_anchor: _DirectoryAnchor, source_name: str, source_state: TargetState,
                            target_anchor: _DirectoryAnchor, target_name: str, target_state: TargetState) -> None:
    """Bind both entries before moving; never overwrite an unbound destination.

    Replacing an existing file needs two namespace moves. The bound old target
    is retained under a unique sibling name, then the bound source is published
    with native NO_REPLACE. A failed publication restores the original only
    into an absent name; if a foreign entry won that name, both it and the
    retained original survive and the error identifies the retained path.
    This is deliberately not an atomic existing-target rename.
    """
    import msvcrt
    retired = None
    with ExitStack() as opened:
        source_path = source_anchor.parent / source_name
        source_file = None
        if source_state.kind == "directory":
            source_native = _windows_directory_handle(source_path, source_state)
            opened.callback(_windows_close, source_native)
            if _windows_identity(source_native) != source_state.identity:
                raise MutationConflict("rename directory source changed while acquiring its handle")
        else:
            source_native, _ = _windows_file_handle(source_path, access=_GENERIC_READ | _DELETE,
                                                    share=_FILE_SHARE_READ)
            source_file = opened.enter_context(_windows_python_file(source_native, "rb", os.O_RDONLY))
            source_native = msvcrt.get_osfhandle(source_file.fileno())
            if not _same_state(_capture_open_file(source_file), source_state):
                raise MutationConflict("rename source facts changed while acquiring its handle")
        if _identity(source_anchor.stat(source_name)) != source_state.identity:
            raise MutationConflict("rename opened source no longer occupies its authorized name")

        target_file = None
        target_native = None
        retained = None
        if target_state.present:
            if target_state.kind != "file" or source_state.kind != "file":
                raise MutationUnsupported("Windows existing-target rename requires two regular files")
            target_path = target_anchor.parent / target_name
            target_native, _ = _windows_file_handle(target_path, access=_GENERIC_READ | _DELETE,
                                                    share=_FILE_SHARE_READ)
            target_file = opened.enter_context(_windows_python_file(target_native, "rb", os.O_RDONLY))
            target_native = msvcrt.get_osfhandle(target_file.fileno())
            if (not _same_state(_capture_open_file(target_file), target_state)
                    or _identity(target_anchor.stat(target_name)) != target_state.identity):
                raise MutationConflict("rename destination facts changed while acquiring its handle")
        source_anchor.assert_stable()
        target_anchor.assert_stable()
        try:
            if target_native is not None:
                retained_name = f".rename.{uuid.uuid4().hex}.retained"
                _windows_rename_handle(target_native, target_anchor, retained_name, replace_existing=False)
                retained = target_anchor.parent / retained_name
            _windows_rename_handle(source_native, target_anchor, target_name, replace_existing=False)
            # Verify on the still-held source before destroying any retained
            # original. Neither source nor retained target permits new writers.
            if source_file is not None and not _same_state(_capture_open_file(source_file), source_state):
                raise MutationConflict("rename source facts changed during native publication")
            if _identity(target_anchor.stat(target_name)) != source_state.identity:
                raise MutationConflict("rename destination does not name the bound source")
            try:
                source_anchor.stat(source_name)
            except FileNotFoundError:
                pass
            else:
                raise MutationConflict("rename source name was repopulated during publication")
            if retained is not None:
                if (not _same_state(_capture_open_file(target_file), target_state)
                        or _identity(target_anchor.stat(retained.name)) != target_state.identity):
                    raise MutationConflict("retained rename destination changed")
                _windows_mark_handle_deleted(target_native, retained)
                retired = retained
                retained = None
        except BaseException as failure:
            if retained is not None:
                try:
                    _windows_rename_handle(target_native, target_anchor, target_name, replace_existing=False)
                except OSError as restore_error:
                    raise MutationConflict(
                        f"rename failed; original destination {target_state.identity} retained at {retained}; "
                        f"no-replace restoration failed: {restore_error}"
                    ) from failure
            raise
    if retired is not None:
        try:
            target_anchor.stat(retired.name)
        except FileNotFoundError:
            pass
        else:
            raise MutationConflict(f"rename published; retained destination cleanup is unproven: {retired}")


def rename(
    root: str | Path,
    source: str | Path,
    target: str | Path,
    *,
    expected_source: TargetState | None = None,
    expected_target: TargetState | None = None,
    expected_root_identity: tuple[int, int] | None = None,
) -> MutationReceipt:
    """Move an authorized entry between two anchored parents within one root.

    Windows binds and validates source bytes before mutation. An existing
    destination is retained through a bound handle before NO_REPLACE publication
    and can be restored only if its name remains absent. POSIX lacks an atomic
    expected-inode comparison; resource locks serialize cooperating writers.
    """
    _require_supported_root_boundary()
    root_path = _lexical(root)
    source_rel, target_rel = _relative(root_path, source), _relative(root_path, target)
    if source_rel == target_rel or source_rel in target_rel.parents or target_rel in source_rel.parents:
        raise MutationConflict("rename source and target overlap")
    source_parent, source_name = _validate_chain(root_path, source_rel)
    target_parent, target_name = _validate_chain(root_path, target_rel)
    source_state = expected_source if expected_source is not None else capture_target(root_path, source_rel)
    target_state = expected_target if expected_target is not None else capture_target(root_path, target_rel)
    if not source_state.present:
        raise MutationConflict("rename source is absent")
    source_anchor = _DirectoryAnchor(root_path, source_parent)
    target_anchor = _DirectoryAnchor(root_path, target_parent)
    _before_commit()
    with source_anchor, target_anchor:
        _after_anchor()
        source_anchor.assert_stable(); target_anchor.assert_stable()
        if expected_root_identity is not None and (
                source_anchor.root_identity != expected_root_identity
                or target_anchor.root_identity != expected_root_identity):
            raise MutationConflict("rename root identity changed after preparation")
        _require_anchored_expected(source_anchor, source_name, source_state)
        _require_anchored_expected(target_anchor, target_name, target_state)
        _after_target_proof()
        source_anchor.assert_stable(); target_anchor.assert_stable()
        _require_anchored_expected(source_anchor, source_name, source_state)
        _require_anchored_expected(target_anchor, target_name, target_state)
        if os.name == "nt":
            _windows_rename_entries(source_anchor, source_name, source_state,
                                    target_anchor, target_name, target_state)
        else:
            _posix_rename(source_anchor.parent_fd, source_name, target_anchor.parent_fd, target_name,
                          replace_existing=target_state.present)
            os.fsync(source_anchor.parent_fd)
            if source_parent != target_parent:
                os.fsync(target_anchor.parent_fd)
        source_anchor.assert_stable(); target_anchor.assert_stable()
        resulting = _capture_anchored(target_anchor, target_name, include_content=source_state.content_sha256 is not None)
        if not _same_state(resulting, source_state) or _capture_anchored(source_anchor, source_name, include_content=False).present:
            raise MutationConflict("rename could not prove exact destination and source absence")
        return MutationReceipt("rename", str(root_path), str(target_parent), str(target_parent / target_name), source_state, resulting)


def _tree_plan(captured: CapturedTree) -> tuple[dict[Path, TargetState], dict[Path, list[str]]]:
    if any(not isinstance(entry, Path) or not isinstance(state, TargetState)
           for entry, state in captured.entries):
        raise MutationConflict("captured tree entries have invalid types")
    plan = dict(captured.entries)
    if len(plan) != len(captured.entries) or captured.relative not in plan:
        raise MutationConflict("captured tree has duplicate entries or no selected root")
    if not plan[captured.relative].present:
        if len(plan) != 1 or plan[captured.relative] != TargetState.absent():
            raise MutationConflict("absent captured tree contains entries")
        return plan, {}
    if plan[captured.relative].kind != "directory":
        raise MutationConflict("captured tree root is not a plain directory")
    names = {entry: [] for entry, state in plan.items() if state.kind == "directory"}
    for entry, state in plan.items():
        if (not isinstance(entry, Path) or entry.is_absolute()
                or entry != safe_relative_path(entry) or not entry.is_relative_to(captured.relative)
                or not isinstance(state, TargetState) or not state.present
                or state.kind not in ("file", "directory")):
            raise MutationConflict("captured tree entry is outside its plain selected tree")
        if state.kind == "file" and (not isinstance(state.content, bytes)
                or state.size != len(state.content)
                or state.content_sha256 != hashlib.sha256(state.content).hexdigest()):
            raise MutationConflict("captured tree file lacks its complete validated bytes")
        if entry != captured.relative:
            if entry.parent not in names:
                raise MutationConflict("captured tree entry has no captured directory parent")
            names[entry.parent].append(entry.name)
    for children in names.values():
        if len({name.casefold() for name in children}) != len(children):
            raise MutationConflict("captured tree contains aliased portable names")
        children.sort()
    return plan, names


def _revalidate_tree(captured: CapturedTree) -> None:
    plan, names = _tree_plan(captured)
    root = captured.root
    with _DirectoryAnchor(root, root) as root_anchor:
        if root_anchor.root_identity != captured.root_identity:
            raise MutationConflict("captured tree authority root changed")
        for entry, state in plan.items():
            if not _same_state(capture_target(root, entry), state):
                raise MutationConflict("captured tree entry identity or bytes changed")
            if entry in names:
                with _DirectoryAnchor(root, root / entry) as directory:
                    if directory.parent_identity != state.identity:
                        raise MutationConflict("captured tree directory changed")
                    actual = sorted(os.listdir(directory.parent_fd if os.name != "nt" else root / entry))
                    if actual != names[entry]:
                        raise MutationConflict("captured tree membership changed")
                    directory.assert_stable()
            root_anchor.assert_stable()


def capture_tree(root: str | Path, relative: str | Path) -> CapturedTree:
    """Capture and revalidate every plain entry without authorizing later substitutions."""
    _require_supported_root_boundary()
    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    parent, name = _validate_chain(root_path, rel)
    plan: dict[Path, TargetState] = {}

    def inventory(entry: Path) -> None:
        state = capture_target(root_path, entry)
        if not state.present:
            raise MutationConflict("tree entry disappeared during capture")
        plan[entry] = state
        if state.kind == "directory":
            with _DirectoryAnchor(root_path, root_path / entry) as directory:
                if directory.parent_identity != state.identity:
                    raise MutationConflict("tree directory changed during capture")
                children = sorted(os.listdir(directory.parent_fd if os.name != "nt" else root_path / entry))
                seen = set()
                for child in children:
                    try:
                        portable_component(child)
                    except ValueError as exc:
                        raise MutationConflict("tree contains an unsafe portable name") from exc
                    if child.casefold() in seen:
                        raise MutationConflict("tree contains aliased portable names")
                    seen.add(child.casefold())
                    inventory(entry / child)
                directory.assert_stable()

    with _DirectoryAnchor(root_path, parent) as outer:
        selected = _capture_anchored(outer, name, include_content=True)
        if not selected.present:
            plan[rel] = selected
        elif selected.kind != "directory":
            raise MutationConflict("tree capture requires one plain directory")
        else:
            inventory(rel)
            if not _same_state(plan[rel], selected):
                raise MutationConflict("tree root changed during capture")
        captured = CapturedTree(root_path, rel, outer.root_identity, tuple(sorted(plan.items())))
        _revalidate_tree(captured)
        outer.assert_stable()
        return captured


def remove_tree(
    root: str | Path,
    relative: str | Path,
    *,
    expected: TargetState | None = None,
    captured: CapturedTree | None = None,
) -> None:
    """Remove captured entries only; changed identity/bytes/membership is a conflict."""
    _require_supported_root_boundary()
    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    snapshot = captured if captured is not None else capture_tree(root_path, rel)
    if not isinstance(snapshot, CapturedTree) or snapshot.root != root_path or snapshot.relative != rel:
        raise MutationConflict("captured tree belongs to another authority root or target")
    plan, _ = _tree_plan(snapshot)
    authorized = plan[rel]
    if expected is not None and not _same_state(authorized, expected):
        raise MutationConflict("captured tree root differs from the expected target")
    if not authorized.present:
        _revalidate_tree(snapshot)
        return
    _remove_anchored_tree(snapshot)


def _remove_anchored_tree(captured: CapturedTree) -> None:
    root, relative = captured.root, captured.relative
    plan, _ = _tree_plan(captured)
    parent, name = _validate_chain(root, relative)
    outer = _DirectoryAnchor(root, parent)
    _before_commit()
    with outer:
        _after_anchor()
        if outer.root_identity != captured.root_identity:
            raise MutationConflict("captured tree authority root changed before removal")
        _require_anchored_expected(outer, name, plan[relative])
        _after_target_proof()
        # Validate the caller's entire original census before the first deletion.
        _revalidate_tree(captured)
        for entry in sorted(plan, key=lambda item: len(item.parts), reverse=True):
            state = plan[entry]
            with _DirectoryAnchor(root, (root / entry).parent) as anchor:
                for ancestor in entry.parents:
                    if ancestor in plan and not _same_state(capture_target(root, ancestor), plan[ancestor]):
                        raise MutationConflict("tree ancestor changed during removal")
                _require_anchored_expected(anchor, entry.name, state)
                if state.kind == "directory":
                    if os.name == "nt":
                        native = _windows_directory_handle(root / entry, state)
                        try:
                            if list(os.scandir(root / entry)):
                                raise MutationConflict("directory gained entries during removal")
                            _windows_mark_handle_deleted(native, root / entry)
                        finally:
                            _windows_close(native)
                    else:
                        os.rmdir(entry.name, dir_fd=anchor.parent_fd)
                elif os.name == "nt":
                    _windows_delete_existing(root / entry, state)
                else:
                    os.unlink(entry.name, dir_fd=anchor.parent_fd)
                anchor.assert_stable()
                if _capture_anchored(anchor, entry.name, include_content=False).present:
                    raise MutationConflict("tree entry removal could not prove absence")
        outer.assert_stable()
        if _capture_anchored(outer, name, include_content=False).present:
            raise MutationConflict("tree removal could not prove root absence")


def append_bytes(root: str | Path, relative: str | Path, data: bytes, *,
                 expected: TargetState | None = None,
                 expected_root_identity: tuple[int, int] | None = None,
                 expected_directories: DirectoryExpectations | None = None) -> TargetState:
    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    target = root_path / rel
    anchored = open_append_file(root_path, rel, expected=expected,
                               expected_root_identity=expected_root_identity,
                               expected_directories=expected_directories)
    try:
        handle = anchored.handle
        before = os.fstat(handle.fileno())
        if before.st_nlink != 1:
            raise MutationConflict("append target gained hard links")
        handle.seek(0, os.SEEK_END)
        if handle.write(data) != len(data):
            raise MutationConflict("anchored append did not write all requested bytes")
        handle.flush()
        os.fsync(handle.fileno())
        anchored.anchor.assert_stable()
        # Read back through the held writer handle. A second Windows reader
        # opened without write sharing conflicts with this very handle, even
        # though the append succeeded. Keep the same exact object through proof.
        written = os.fstat(handle.fileno())
        handle.seek(0)
        content = handle.read(written.st_size + 1)
        after = os.fstat(handle.fileno())
        named = target.lstat()
        fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if (any(getattr(written, field) != getattr(after, field) for field in fields)
                or _identity(named) != _identity(after) or _is_reparse(target)
                or after.st_nlink != 1 or len(content) != before.st_size + len(data)
                or content[before.st_size:] != data):
            raise MutationConflict("append target changed before verification")
        anchored.anchor.assert_stable()
        return TargetState(True, "file", _identity(after),
                           hashlib.sha256(content).hexdigest(), len(content), content)
    finally:
        anchored.close()


__all__ = [
    "MutationConflict",
    "MutationError",
    "MutationReceipt",
    "MutationUnsupported",
    "AnchoredAppendFile",
    "CapturedTree",
    "DirectoryExpectations",
    "FileMetadata",
    "TargetState",
    "append_bytes",
    "capture_target",
    "capture_tree",
    "create_directory",
    "delete",
    "durable_identity",
    "ensure_directory_path",
    "make_temporary_directory",
    "open_append_file",
    "publish_bytes",
    "read_bytes",
    "read_link_value",
    "remove_tree",
    "rename",
    "replace",
    "safe_relative_path",
]
