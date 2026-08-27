"""Identity-bound local mutation primitives used by S4-managed state.

The public callers provide a verified root, a single relative destination, and
the target state they authorized.  This module owns the last-use proof and the
temporary/replace/delete mechanics.  A pathname check is never treated as an
atomic boundary: POSIX returns typed ``MutationUnsupported`` because a movable
directory descriptor cannot prove root containment through use; Windows holds
no-delete directory handles opened with ``FILE_FLAG_OPEN_REPARSE_POINT`` and
uses handle-bound target operations.
"""

from __future__ import annotations

import ctypes
import hashlib
import os
import stat
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath


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
    return Path(os.path.abspath(str(Path(value).expanduser())))


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


def _relative(root: Path, value: str | Path) -> Path:
    candidate = Path(value)
    if candidate.is_absolute():
        try:
            candidate = candidate.relative_to(root)
        except ValueError as exc:
            raise MutationConflict(
                "mutation target is outside its verified root"
            ) from exc
    return safe_relative_path(candidate)


def _validate_chain(root: Path, relative: Path) -> tuple[Path, str]:
    root = _lexical(root)
    if not root.is_dir() or _is_reparse(root):
        raise MutationConflict(f"mutation root is not a regular directory: {root}")
    target = root / relative
    current = root
    for part in relative.parts[:-1]:
        current = current / part
        if not os.path.lexists(current):
            raise MutationConflict(f"mutation parent is missing: {current}")
        if _is_reparse(current) or not current.is_dir():
            raise MutationConflict(f"mutation parent is unsafe: {current}")
    parent = target.parent
    if not parent.is_dir() or _is_reparse(parent):
        raise MutationConflict(f"mutation parent is unsafe: {parent}")
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
    if not include_content:
        return TargetState(True, "file", identity, None, int(info.st_size), None)
    try:
        content = path.read_bytes()
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
    del parent
    return _capture_path(root_path / rel, include_content=True)


def _same_state(actual: TargetState, expected: TargetState) -> bool:
    if actual.present != expected.present or actual.kind != expected.kind:
        return False
    if not actual.present:
        return True
    return (
        actual.identity == expected.identity
        and actual.content_sha256 == expected.content_sha256
        and actual.size == expected.size
    )


def _require_expected(path: Path, expected: TargetState) -> TargetState:
    actual = _capture_path(path, include_content=True)
    if not _same_state(actual, expected):
        raise MutationConflict(f"mutation target changed after authorization: {path}")
    return actual


def _require_supported_root_boundary() -> None:
    """Reject POSIX descriptor-relative writes whose root can be relocated.

    A POSIX directory descriptor protects name lookup from symlink traversal, but
    it does not pin the directory to its admitted pathname/root relationship:
    the directory can be renamed outside the admitted root while the descriptor
    remains usable.  This package has no portable kernel primitive that proves
    both facts through the write, so POSIX mutations fail closed before opening
    a temporary, lock, payload, or destructive target.
    """

    if os.name != "nt":
        raise MutationUnsupported(
            "POSIX mutation backend cannot prove root containment through use"
        )


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
        _CloseHandle(handle)
        raise MutationUnsupported(
            f"Windows file identity unavailable ({error}): {path}"
        )
    if info.dwFileAttributes & _FILE_ATTRIBUTE_REPARSE_POINT:
        _CloseHandle(handle)
        raise MutationConflict(f"Windows target is a reparse point: {path}")
    if info.dwFileAttributes & 0x00000010:
        _CloseHandle(handle)
        raise MutationConflict(
            f"Windows target is a directory, not a regular file: {path}"
        )
    file_id = (int(info.nFileIndexHigh) << 32) | int(info.nFileIndexLow)
    return handle, (int(info.dwVolumeSerialNumber), file_id)


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
        _CloseHandle(handle)
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
    """Recursively remove one exact directory while its delete handle is held."""

    handle = _windows_directory_handle(path, expected)
    try:
        children = sorted(list(os.scandir(path)), key=lambda item: item.name)
        for entry in children:
            child = path / entry.name
            if _is_reparse(child):
                raise MutationConflict(
                    f"refusing recursive cleanup through a reparse point: {child}"
                )
            child_state = _capture_path(child, include_content=True)
            if not child_state.present:
                continue
            if child_state.kind == "directory":
                _windows_remove_directory(child, child_state)
            else:
                _windows_delete_existing(child, child_state)
        with os.scandir(path) as remaining:
            if next(remaining, None) is not None:
                raise MutationConflict(
                    f"directory changed during handle-bound cleanup: {path}"
                )
        _windows_mark_handle_deleted(handle, path)
    finally:
        _CloseHandle(handle)


def _windows_open_existing_for_update(path: Path, expected: TargetState) -> object:
    """Open an existing file exclusively enough to prevent a target swap/write."""

    handle, _ = _windows_file_handle(
        path,
        access=_GENERIC_READ | _GENERIC_WRITE,
        share=_FILE_SHARE_READ,
    )
    try:
        import msvcrt

        descriptor = msvcrt.open_osfhandle(int(handle), os.O_RDWR | os.O_BINARY)
        handle = None
        file_handle = os.fdopen(descriptor, "r+b", closefd=True)
        file_handle.seek(0)
        content = file_handle.read()
        info = path.stat()
        actual = TargetState(
            True,
            "file",
            _identity(info),
            hashlib.sha256(content).hexdigest(),
            len(content),
            content,
        )
        if not _same_state(actual, expected):
            file_handle.close()
            raise MutationConflict(
                f"mutation target changed while acquiring its handle: {path}"
            )
        return file_handle
    finally:
        if handle is not None:
            _CloseHandle(handle)


def _windows_delete_existing(path: Path, expected: TargetState) -> None:
    """Mark the exact opened target for deletion, without a pathname unlink."""

    handle, _ = _windows_file_handle(
        path,
        access=_GENERIC_READ | _DELETE,
        share=_FILE_SHARE_READ,
    )
    try:
        import msvcrt

        descriptor = msvcrt.open_osfhandle(int(handle), os.O_RDONLY | os.O_BINARY)
        handle = None
        file_handle = os.fdopen(descriptor, "rb", closefd=True)
        content = file_handle.read()
        info = path.stat()
        actual = TargetState(
            True,
            "file",
            _identity(info),
            hashlib.sha256(content).hexdigest(),
            len(content),
            content,
        )
        if not _same_state(actual, expected):
            file_handle.close()
            raise MutationConflict(
                f"mutation target changed while acquiring delete handle: {path}"
            )
        disposition = _FileDispositionInfo(True)
        if not _SetFileInformationByHandle(
            msvcrt.get_osfhandle(file_handle.fileno()),
            _FILE_DISPOSITION_INFO,
            ctypes.byref(disposition),
            ctypes.sizeof(disposition),
        ):
            error = ctypes.get_last_error()
            file_handle.close()
            raise MutationUnsupported(
                f"Windows handle-bound delete unavailable ({error}): {path}"
            )
        file_handle.close()
    finally:
        if handle is not None:
            _CloseHandle(handle)


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
        self._handles: list[object] = []
        if os.name == "nt":
            root_handle, self.expected_root_identity = self._windows_open(self.root)
            parent_handle, self.expected_parent_identity = self._windows_open(
                self.parent
            )
            _CloseHandle(root_handle)
            _CloseHandle(parent_handle)
        else:
            try:
                self.expected_root_identity = _identity(self.root.lstat())
                self.expected_parent_identity = _identity(self.parent.lstat())
            except OSError as exc:
                raise MutationConflict(
                    "mutation directory disappeared before anchoring"
                ) from exc

    def __enter__(self) -> "_DirectoryAnchor":
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
        except (MutationError, OSError):
            self.close()
            raise

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def _enter_posix(self) -> None:
        flags = (
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        )
        root_fd = os.open(self.root, flags)
        self._fds.append(root_fd)
        root_info = os.fstat(root_fd)
        if not stat.S_ISDIR(root_info.st_mode):
            raise MutationUnsupported("POSIX root handle is not a directory")
        self.root_identity = _identity(root_info)
        current_fd = root_fd
        relative_parent = self.parent.relative_to(self.root)
        for part in relative_parent.parts:
            next_fd = os.open(part, flags, dir_fd=current_fd)
            self._fds.append(next_fd)
            current_fd = next_fd
        parent_info = os.fstat(current_fd)
        if not stat.S_ISDIR(parent_info.st_mode):
            raise MutationUnsupported("POSIX parent handle is not a directory")
        self.parent_fd = current_fd
        self.parent_identity = _identity(parent_info)

    @staticmethod
    def _windows_open(path: Path) -> tuple[object, tuple[int, int]]:
        if os.name != "nt":
            raise MutationUnsupported("Windows directory backend is unavailable")
        handle = _CreateFileW(
            str(path),
            _FILE_READ_ATTRIBUTES | _FILE_LIST_DIRECTORY,
            _FILE_SHARE_READ | _FILE_SHARE_WRITE,
            None,
            _OPEN_EXISTING,
            _FILE_FLAG_BACKUP_SEMANTICS | _FILE_FLAG_OPEN_REPARSE_POINT,
            None,
        )
        if handle in (None, _INVALID_HANDLE_VALUE):
            error = ctypes.get_last_error()
            raise MutationUnsupported(
                f"Windows directory handle unavailable ({error}): {path}"
            )
        info = _ByHandleFileInformation()
        if not _GetFileInformationByHandle(handle, ctypes.byref(info)):
            error = ctypes.get_last_error()
            _CloseHandle(handle)
            raise MutationUnsupported(
                f"Windows directory identity unavailable ({error}): {path}"
            )
        if info.dwFileAttributes & _FILE_ATTRIBUTE_REPARSE_POINT:
            _CloseHandle(handle)
            raise MutationConflict(f"Windows directory is a reparse point: {path}")
        file_id = (int(info.nFileIndexHigh) << 32) | int(info.nFileIndexLow)
        return handle, (int(info.dwVolumeSerialNumber), file_id)

    def _enter_windows(self) -> None:
        paths = [self.root]
        relative_parent = self.parent.relative_to(self.root)
        current = self.root
        for part in relative_parent.parts:
            current = current / part
            paths.append(current)
        for path in paths:
            handle, identity = self._windows_open(path)
            self._handles.append(handle)
            if path == self.root:
                self.root_identity = identity
            if path == self.parent:
                self.parent_identity = identity
        if self.parent_identity is None:
            self.parent_identity = self.root_identity

    def assert_stable(self) -> None:
        if self.root_identity is None or self.parent_identity is None:
            raise MutationUnsupported("mutation directory anchor was not established")
        if _is_reparse(self.root) or _is_reparse(self.parent):
            raise MutationConflict("mutation directory became a reparse point")
        if os.name == "nt":
            # The no-delete handles are the authoritative anti-substitution
            # anchor.  A current identity check makes the evidence explicit.
            root_handle, root_identity = self._windows_open(self.root)
            parent_handle, parent_identity = self._windows_open(self.parent)
            _CloseHandle(root_handle)
            _CloseHandle(parent_handle)
            if (
                root_identity != self.root_identity
                or parent_identity != self.parent_identity
            ):
                raise MutationConflict("Windows mutation directory identity changed")
            return
        if (
            _identity(os.lstat(self.root)) != self.root_identity
            or _identity(os.lstat(self.parent)) != self.parent_identity
        ):
            raise MutationConflict("POSIX mutation directory identity changed")

    def close(self) -> None:
        for fd in reversed(self._fds):
            try:
                os.close(fd)
            except OSError:
                pass
        self._fds.clear()
        for handle in reversed(self._handles):
            try:
                _CloseHandle(handle)  # type: ignore[name-defined]
            except OSError:
                pass
        self._handles.clear()
        self.parent_fd = None


def open_append_file(root: str | Path, relative: str | Path) -> AnchoredAppendFile:
    """Open/create an append destination while retaining its root capability."""

    _require_supported_root_boundary()
    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    parent, name = _validate_chain(root_path, rel)
    target = root_path / rel
    anchor = _DirectoryAnchor(root_path, parent)
    handed_off = False
    _before_commit()
    try:
        anchor.__enter__()
        _after_anchor()
        anchor.assert_stable()
        if os.name == "nt":
            handle, _ = _windows_file_handle(
                target,
                access=_GENERIC_READ | _GENERIC_WRITE,
                share=_FILE_SHARE_READ | _FILE_SHARE_WRITE,
                creation=_OPEN_ALWAYS,
            )
            try:
                import msvcrt

                descriptor = msvcrt.open_osfhandle(int(handle), os.O_RDWR | os.O_BINARY)
                handle = None
                file_handle = os.fdopen(descriptor, "a+b", closefd=True)
            finally:
                if handle is not None:
                    _CloseHandle(handle)
        else:  # pragma: no cover - guarded by _require_supported_root_boundary
            raise MutationUnsupported("POSIX append boundary is unsupported")
        anchor.assert_stable()
        handed_off = True
        return AnchoredAppendFile(file_handle, anchor)
    except MutationError:
        raise
    except OSError as exc:
        raise MutationConflict(f"anchored append open failed: {target}") from exc
    finally:
        if not handed_off:
            anchor.close()


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
                return descriptor, Path(candidate)
            except FileExistsError:
                continue
        raise MutationUnsupported("could not allocate an anchored temporary entry")
    return tempfile.mkstemp(prefix=f".{name}.", suffix=".tmp", dir=str(anchor.parent))


def _unlink_temp(anchor: _DirectoryAnchor, temporary: Path) -> None:
    try:
        if os.name != "nt" and anchor.parent_fd is not None:
            os.unlink(temporary.name, dir_fd=anchor.parent_fd)
        else:
            temporary.unlink(missing_ok=True)
    except (FileNotFoundError, OSError):
        pass


def replace(
    root: str | Path,
    relative: str | Path,
    data: bytes,
    *,
    expected: TargetState | None = None,
) -> MutationReceipt:
    _require_supported_root_boundary()
    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    parent, name = _validate_chain(root_path, rel)
    target = root_path / rel
    authorized = (
        expected
        if expected is not None
        else _capture_path(target, include_content=True)
    )
    temporary: Path | None = None
    anchor = _DirectoryAnchor(root_path, parent)
    _before_commit()
    try:
        with anchor:
            _after_anchor()
            anchor.assert_stable()
            _require_expected(target, authorized)
            _after_target_proof()
            _require_expected(target, authorized)
            if authorized.present and authorized.kind != "file":
                raise MutationConflict(
                    f"mutation replacement target is not a regular file: {target}"
                )
            if os.name == "nt" and authorized.present:
                with _windows_open_existing_for_update(target, authorized) as handle:
                    handle.seek(0)
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
                    handle.truncate()
                    handle.seek(0)
                    written = handle.read()
                    if written != data:
                        raise MutationConflict(
                            f"handle-bound replacement could not verify bytes: {target}"
                        )
                anchor.assert_stable()
                resulting = _capture_path(target, include_content=True)
                expected_result = TargetState(
                    True,
                    "file",
                    authorized.identity,
                    hashlib.sha256(data).hexdigest(),
                    len(data),
                    data,
                )
                if not _same_state(resulting, expected_result):
                    raise MutationConflict(
                        "handle-bound replacement identity or bytes could not be verified"
                    )
                return MutationReceipt(
                    "replace",
                    str(root_path),
                    str(parent),
                    str(target),
                    authorized,
                    resulting,
                )
            descriptor, raw_name = _new_temp(anchor, name)
            raw_path = Path(raw_name)
            temporary = raw_path if raw_path.is_absolute() else anchor.parent / raw_path
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            anchor.assert_stable()
            _require_expected(target, authorized)
            _after_target_proof()
            _require_expected(target, authorized)
            if _is_reparse(temporary) or not temporary.is_file():
                raise MutationConflict("mutation temporary is not a regular file")
            if (
                os.name != "nt" and anchor.parent_fd is not None
            ):  # pragma: no cover - unsupported above
                os.link(
                    temporary.name,
                    name,
                    src_dir_fd=anchor.parent_fd,
                    dst_dir_fd=anchor.parent_fd,
                    follow_symlinks=False,
                )
                os.unlink(temporary.name, dir_fd=anchor.parent_fd)
            else:
                try:
                    os.link(temporary, target)
                except FileExistsError as exc:
                    raise MutationConflict(
                        f"mutation target appeared during no-replace publication: {target}"
                    ) from exc
                temporary.unlink(missing_ok=True)
            temporary = None
            anchor.assert_stable()
            resulting = _capture_path(target, include_content=True)
            expected_result = TargetState(
                True,
                "file",
                resulting.identity,
                hashlib.sha256(data).hexdigest(),
                len(data),
                data,
            )
            if not _same_state(resulting, expected_result):
                raise MutationConflict(
                    "mutation result identity or bytes could not be verified"
                )
            return MutationReceipt(
                "replace",
                str(root_path),
                str(parent),
                str(target),
                authorized,
                resulting,
            )
    except MutationError:
        raise
    except OSError as exc:
        raise MutationConflict(f"anchored replacement failed: {target}") from exc
    finally:
        if temporary is not None:
            # Cleanup is restricted to the captured parent/operation.  If the
            # anchor could not be established, do not follow a substituted path.
            try:
                if "anchor" in locals() and anchor.parent_identity is not None:
                    _unlink_temp(anchor, temporary)
            except OSError:
                pass


def delete(
    root: str | Path,
    relative: str | Path,
    *,
    expected: TargetState | None = None,
) -> MutationReceipt:
    _require_supported_root_boundary()
    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    parent, name = _validate_chain(root_path, rel)
    target = root_path / rel
    authorized = (
        expected
        if expected is not None
        else _capture_path(target, include_content=True)
    )
    anchor = _DirectoryAnchor(root_path, parent)
    _before_commit()
    with anchor:
        _after_anchor()
        anchor.assert_stable()
        _require_expected(target, authorized)
        _after_target_proof()
        _require_expected(target, authorized)
        if authorized.present:
            if os.name == "nt":
                _windows_delete_existing(target, authorized)
            elif anchor.parent_fd is not None:  # pragma: no cover - unsupported above
                os.unlink(name, dir_fd=anchor.parent_fd)
            else:  # pragma: no cover - unsupported above
                raise MutationUnsupported(
                    "anchored delete requires a supported parent capability"
                )
        anchor.assert_stable()
        resulting = _capture_path(target, include_content=True)
        if resulting.present:
            raise MutationConflict(
                f"mutation delete could not verify absence: {target}"
            )
        return MutationReceipt(
            "delete", str(root_path), str(parent), str(target), authorized, resulting
        )


def ensure_directory_path(path: str | Path) -> Path:
    """Create missing regular directories without following a reparse chain."""

    _validate_directory_input(path)
    target = _lexical(path)
    if _has_ads(target):
        raise MutationConflict(
            f"directory path contains alternate-stream syntax: {target}"
        )
    if target.exists():
        if not target.is_dir() or _is_reparse(target):
            raise MutationConflict(f"directory target is unsafe: {target}")
        return target
    _require_supported_root_boundary()
    missing: list[Path] = []
    current = target
    while not current.exists():
        missing.append(current)
        parent = current.parent
        if parent == current:
            raise MutationUnsupported(f"no existing directory anchor for {target}")
        current = parent
    if not current.is_dir() or _is_reparse(current):
        raise MutationConflict(f"directory anchor is unsafe: {current}")
    for directory in reversed(missing):
        rel = Path(directory.name)
        anchor = _DirectoryAnchor(current, current)
        _before_commit()
        with anchor:
            _after_anchor()
            anchor.assert_stable()
            try:
                if os.name != "nt" and anchor.parent_fd is not None:
                    os.mkdir(rel.name, dir_fd=anchor.parent_fd)
                else:
                    directory.mkdir()
            except FileExistsError:
                pass
            anchor.assert_stable()
        if not directory.is_dir() or _is_reparse(directory):
            raise MutationConflict(f"directory creation result is unsafe: {directory}")
        current = directory
    return target


def make_temporary_directory(root: str | Path, *, prefix: str) -> Path:
    _validate_directory_input(root)
    _require_supported_root_boundary()
    root_path = _lexical(root)
    if not root_path.is_dir() or _is_reparse(root_path):
        raise MutationConflict(f"temporary root is unsafe: {root_path}")
    anchor = _DirectoryAnchor(root_path, root_path)
    _before_commit()
    with anchor:
        _after_anchor()
        anchor.assert_stable()
        if (
            os.name != "nt" and anchor.parent_fd is not None
        ):  # pragma: no cover - unsupported above
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


def rename(
    root: str | Path,
    source: str | Path,
    target: str | Path,
    *,
    expected_source: TargetState | None = None,
    expected_target: TargetState | None = None,
) -> MutationReceipt:
    _require_supported_root_boundary()
    root_path = _lexical(root)
    source_rel = _relative(root_path, source)
    target_rel = _relative(root_path, target)
    if source_rel.parent != target_rel.parent:
        raise MutationUnsupported("anchored rename requires one verified parent")
    parent, source_name = _validate_chain(root_path, source_rel)
    _, target_name = _validate_chain(root_path, target_rel)
    source_path = root_path / source_rel
    target_path = root_path / target_rel
    source_state = (
        expected_source
        if expected_source is not None
        else _capture_path(source_path, include_content=False)
    )
    target_state = (
        expected_target
        if expected_target is not None
        else _capture_path(target_path, include_content=False)
    )
    anchor = _DirectoryAnchor(root_path, parent)
    _before_commit()
    with anchor:
        _after_anchor()
        anchor.assert_stable()
        _require_expected(source_path, source_state)
        _require_expected(target_path, target_state)
        _after_target_proof()
        _require_expected(source_path, source_state)
        _require_expected(target_path, target_state)
        if (
            os.name != "nt" and anchor.parent_fd is not None
        ):  # pragma: no cover - unsupported above
            os.rename(
                source_name,
                target_name,
                src_dir_fd=anchor.parent_fd,
                dst_dir_fd=anchor.parent_fd,
            )
        else:
            if target_state.present:
                raise MutationUnsupported(
                    "Windows rename cannot prove an existing target through use"
                )
            if not _MoveFileExW(
                str(source_path),
                str(target_path),
                _MOVEFILE_FAIL_IF_EXISTS | _MOVEFILE_WRITE_THROUGH,
            ):
                error = ctypes.get_last_error()
                raise MutationConflict(
                    f"handle-bound no-replace rename failed ({error}): {target_path}"
                )
        anchor.assert_stable()
        resulting = _capture_path(target_path, include_content=False)
        if not resulting.present or resulting.identity != source_state.identity:
            raise MutationConflict(
                "anchored rename result identity could not be verified"
            )
        return MutationReceipt(
            "rename",
            str(root_path),
            str(parent),
            str(target_path),
            source_state,
            resulting,
        )


def remove_tree(
    root: str | Path,
    relative: str | Path,
    *,
    expected: TargetState | None = None,
) -> None:
    _require_supported_root_boundary()
    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    parent, _ = _validate_chain(root_path, rel)
    target = root_path / rel
    authorized = (
        expected
        if expected is not None
        else _capture_path(target, include_content=False)
    )
    if not authorized.present or authorized.kind != "directory":
        return
    anchor = _DirectoryAnchor(root_path, parent)
    _before_commit()
    with anchor:
        _after_anchor()
        anchor.assert_stable()
        _require_expected(target, authorized)
        _after_target_proof()
        _require_expected(target, authorized)
        if os.name == "nt":
            _windows_remove_directory(target, authorized)
        else:  # pragma: no cover - unsupported above
            raise MutationUnsupported(
                "recursive directory removal requires a supported directory capability"
            )
        anchor.assert_stable()
        if _capture_path(target, include_content=False).present:
            raise MutationConflict(f"directory cleanup did not remove {target}")


def append_bytes(root: str | Path, relative: str | Path, data: bytes) -> TargetState:
    root_path = _lexical(root)
    rel = _relative(root_path, relative)
    target = root_path / rel
    anchored = open_append_file(root_path, rel)
    try:
        handle = anchored.handle
        handle.seek(0, os.SEEK_END)
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
        anchored.anchor.assert_stable()
        state = _capture_path(target, include_content=True)
        return state
    finally:
        anchored.close()


__all__ = [
    "MutationConflict",
    "MutationError",
    "MutationReceipt",
    "MutationUnsupported",
    "AnchoredAppendFile",
    "TargetState",
    "append_bytes",
    "capture_target",
    "delete",
    "ensure_directory_path",
    "make_temporary_directory",
    "open_append_file",
    "remove_tree",
    "rename",
    "replace",
    "safe_relative_path",
]
