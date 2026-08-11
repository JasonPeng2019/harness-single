"""Immutable source allocation and archive-first terminal lane lifecycle.

These public operations are intentionally conservative.  A failed proof leaves
the lane and its evidence visible.  Only a clean linked worktree with an exact
retained commit, complete process absence, and explicit no-unmerged proof is
closed through ``git worktree remove``.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import stat
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .models import ProcessInfo, ProcessSnapshot, iso_utc, utc_now


IMMUTABLE_VIEW_SCHEMA = "orchestrator-immutable-source-view/v1"
LANE_ARCHIVE_SCHEMA = "orchestrator-lane-archive/v1"
_HEX_COMMIT = re.compile(r"^[0-9a-fA-F]{40,64}$")
_GIT_ENV_KEYS = (
    "PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "SYSTEMDRIVE",
    "TEMP", "TMP", "TMPDIR",
)


class LaneLifecycleError(ValueError):
    """A lifecycle request is invalid or cannot be proved safe."""


class ImmutableViewError(LaneLifecycleError):
    pass


class RetirementBlocked(LaneLifecycleError):
    """The terminal lane remains visible because one proof is missing."""

    def __init__(self, reason: str, *, result: "RetirementResult | None" = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.result = result


class ArchiveFailed(LaneLifecycleError):
    pass


def _git_env() -> dict[str, str]:
    env = {key: os.environ[key] for key in _GIT_ENV_KEYS if key in os.environ}
    env.update({"GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0", "LC_ALL": "C"})
    return env


def _git(cwd: Path, *args: str, check: bool = False) -> subprocess.CompletedProcess[bytes]:
    try:
        completed = subprocess.run(
            ["git", "-C", str(cwd), *args],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=15,
            env=_git_env(),
            shell=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LaneLifecycleError(f"Git lifecycle inspection failed: {exc}") from exc
    if check and completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()[:500]
        raise LaneLifecycleError(f"Git lifecycle operation failed: {detail}")
    return completed


def _git_text(cwd: Path, *args: str) -> str:
    result = _git(cwd, *args, check=True)
    return result.stdout.decode("utf-8", errors="strict").strip()


def _regular_directory(path: str | Path, *, create: bool = False) -> Path:
    original = Path(path).expanduser()
    if original.exists() and original.is_symlink():
        raise LaneLifecycleError(f"expected a regular directory: {original}")
    value = original.resolve(strict=False)
    if value.exists() and not value.is_dir():
        raise LaneLifecycleError(f"expected a regular directory: {value}")
    if create:
        value.mkdir(parents=True, exist_ok=True)
    if not value.is_dir():
        raise LaneLifecycleError(f"directory is missing: {value}")
    return value


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _separate_root(path: Path, *, source: Path, other: Sequence[Path] = ()) -> None:
    if _inside(path, source) or _inside(source, path):
        raise ImmutableViewError("result/cache/view roots must be outside the source root")
    for sibling in other:
        if _inside(path, sibling) or _inside(sibling, path):
            raise ImmutableViewError("result and cache roots must be separate")


def _exact_commit(source: Path, revision: str) -> str:
    if not isinstance(revision, str) or _HEX_COMMIT.fullmatch(revision) is None:
        raise ImmutableViewError("revision must be a full hexadecimal commit identity")
    resolved = _git_text(source, "rev-parse", "--verify", f"{revision}^{{commit}}")
    if resolved.lower() != revision.lower():
        raise ImmutableViewError("declared revision does not resolve to the exact retained commit")
    _git(source, "cat-file", "-e", f"{revision}^{{commit}}", check=True)
    return resolved.lower()


def _safe_member(name: str) -> bool:
    path = Path(name)
    return not path.is_absolute() and ".." not in path.parts and not any(part in {"", "."} for part in path.parts)


def _set_read_only(root: Path) -> None:
    for path in sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        if path.is_symlink():
            raise ImmutableViewError(f"source view contains an indirection: {path}")
        try:
            if path.is_dir():
                path.chmod(stat.S_IREAD | stat.S_IEXEC)
            else:
                path.chmod(stat.S_IREAD)
        except OSError as exc:
            raise ImmutableViewError(f"cannot establish source read-only boundary: {path}") from exc
    root.chmod(stat.S_IREAD | stat.S_IEXEC)


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        descriptor, raw_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
        temporary = Path(raw_name)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write((json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink(missing_ok=True)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class ImmutableSourceView:
    view_id: str
    source_root: Path
    view_root: Path
    result_root: Path
    cache_root: Path
    retained_commit: str
    manifest_path: Path
    ready_path: Path
    read_only: bool = True

    def as_record(self) -> dict[str, Any]:
        return {
            "schema": IMMUTABLE_VIEW_SCHEMA,
            "view_id": self.view_id,
            "source_root": str(self.source_root),
            "view_root": str(self.view_root),
            "result_root": str(self.result_root),
            "cache_root": str(self.cache_root),
            "retained_commit": self.retained_commit,
            "manifest_path": str(self.manifest_path),
            "ready_path": str(self.ready_path),
            "read_only": self.read_only,
        }

    def write_source(self, relative: str | Path, data: bytes | str) -> None:
        del relative, data
        raise ImmutableViewError("immutable source view rejects writes")

    def assert_read_only(self) -> bool:
        if not self.read_only or not self.ready_path.is_file():
            raise ImmutableViewError("immutable source view is not admitted ready")
        return True


def allocate_immutable_source_view(
    source_root: str | Path,
    *,
    revision: str,
    view_root: str | Path,
    result_root: str | Path,
    cache_root: str | Path,
    view_id: str = "immutable-view",
) -> ImmutableSourceView:
    """Allocate a read-only exact-revision view with separate writable roots."""

    source = _regular_directory(source_root)
    view = Path(view_root).expanduser().resolve(strict=False)
    result = Path(result_root).expanduser().resolve(strict=False)
    cache = Path(cache_root).expanduser().resolve(strict=False)
    if not isinstance(view_id, str) or not view_id.strip() or len(view_id) > 200:
        raise ImmutableViewError("view_id must be a bounded non-empty string")
    if view.exists():
        raise ImmutableViewError("view_root must not already exist")
    _separate_root(view, source=source, other=(result, cache))
    _separate_root(result, source=source, other=(view, cache))
    _separate_root(cache, source=source, other=(view, result))
    commit = _exact_commit(source, revision)
    result.mkdir(parents=True, exist_ok=True)
    cache.mkdir(parents=True, exist_ok=True)
    if result.is_symlink() or cache.is_symlink():
        raise ImmutableViewError("result/cache roots may not be symlinks")
    manifest = result / "IMMUTABLE_VIEW.json"
    ready = result / "VIEW_READY.json"
    if manifest.exists() or ready.exists():
        raise ImmutableViewError("result root already contains an allocation record")
    view.mkdir(parents=True)
    admitted = False
    try:
        archive = _git(source, "archive", "--format=tar", revision, check=True)
        with tarfile.open(fileobj=io.BytesIO(archive.stdout), mode="r:") as tar:
            members = tar.getmembers()
            if any(not _safe_member(member.name) or member.issym() or member.islnk() for member in members):
                raise ImmutableViewError("source archive contains an unsafe member")
            for member in members:
                destination = (view / member.name).resolve(strict=False)
                if not _inside(destination, view.resolve()):
                    raise ImmutableViewError("source archive member escapes the view")
                if member.isdir():
                    destination.mkdir(parents=True, exist_ok=True)
                    continue
                source_handle = tar.extractfile(member)
                if source_handle is None:
                    raise ImmutableViewError(f"source archive member cannot be read: {member.name}")
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(source_handle.read())
        _set_read_only(view)
        record = {
            "schema": IMMUTABLE_VIEW_SCHEMA,
            "view_id": view_id,
            "source_root": str(source),
            "view_root": str(view),
            "result_root": str(result),
            "cache_root": str(cache),
            "retained_commit": commit,
            "created_utc": iso_utc(utc_now()),
            "read_only_boundary": {
                "mechanism": "filesystem-read-only-plus-write-api-refusal",
                "source_output_forbidden": True,
                "result_root_writable": True,
                "cache_root_writable": True,
            },
            "ready": False,
        }
        _atomic_json(manifest, record)
        ready_record = {
            "schema": IMMUTABLE_VIEW_SCHEMA,
            "view_id": view_id,
            "retained_commit": commit,
            "manifest_sha256": _sha256(manifest),
            "admitted_utc": iso_utc(utc_now()),
            "ready": True,
        }
        _atomic_json(ready, ready_record)
        record["ready"] = True
        admitted = True
        return ImmutableSourceView(view_id, source, view, result, cache, commit, manifest, ready)
    except Exception:
        # There is deliberately no ready record after a partial allocation.  A
        # newly-created view is removed through Git-independent exact paths;
        # it is an archive extraction, not a registered worktree.
        ready.unlink(missing_ok=True)
        manifest.unlink(missing_ok=True)
        if not admitted:
            try:
                for child in sorted(view.rglob("*"), key=lambda item: len(item.parts), reverse=True):
                    if child.is_file() or child.is_symlink():
                        child.unlink(missing_ok=True)
                    elif child.is_dir():
                        child.rmdir()
                view.rmdir()
            except OSError:
                pass
        raise


allocate_source_view = allocate_immutable_source_view
allocate_immutable_view = allocate_immutable_source_view


def _reference(value: object, *, name: str) -> dict[str, Any]:
    if value is None:
        return {"name": name, "present": False, "path": None, "sha256": None}
    if isinstance(value, Mapping):
        path_value = value.get("path")
        expected = value.get("sha256")
    else:
        path_value = value
        expected = None
    if not isinstance(path_value, (str, Path)):
        raise ArchiveFailed(f"{name} reference path is invalid")
    path = Path(path_value).expanduser().resolve(strict=False)
    if path.is_symlink() or not path.is_file():
        raise ArchiveFailed(f"{name} reference is not a regular file: {path}")
    actual = _sha256(path)
    if expected is not None and expected != actual:
        raise ArchiveFailed(f"{name} reference hash does not match: {path}")
    return {"name": name, "present": True, "path": str(path), "sha256": actual}


def _process_absence(
    *,
    process_snapshot: ProcessSnapshot | None,
    live_processes: Iterable[ProcessInfo | Mapping[str, Any]] | None,
) -> tuple[bool, str, list[dict[str, Any]]]:
    if process_snapshot is None:
        return False, "LIVE_USE_UNPROVEN", []
    if not process_snapshot.complete:
        return False, "LIVE_USE_AMBIGUOUS", []
    if live_processes is None:
        return False, "LIVE_USE_UNPROVEN", []
    observed: list[dict[str, Any]] = []
    for item in live_processes or ():
        if isinstance(item, ProcessInfo):
            pid = item.pid
            created = iso_utc(item.created_utc)
        elif isinstance(item, Mapping):
            pid = item.get("pid")
            created = item.get("created_utc")
        else:
            return False, "LIVE_USE_AMBIGUOUS", observed
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0 or not isinstance(created, str):
            return False, "LIVE_USE_AMBIGUOUS", observed
        observed.append({"pid": pid, "created_utc": created})
        actual = process_snapshot.by_pid.get(pid)
        if actual is not None:
            if iso_utc(actual.created_utc) != created:
                return False, "LIVE_USE_AMBIGUOUS", observed
            return False, "LIVE_USE_PROVEN", observed
    return True, "NO_LIVE_USE_PROVED", observed


@dataclass(frozen=True)
class RetirementResult:
    outcome: str
    reason: str
    lane_root: Path
    archive_path: Path | None
    archive_sha256: str | None = None
    retained_revision: str | None = None
    worktree_closed: bool = False
    visible: bool = True

    def as_record(self) -> dict[str, Any]:
        return {
            "schema": LANE_ARCHIVE_SCHEMA,
            "outcome": self.outcome,
            "reason": self.reason,
            "lane_root": str(self.lane_root),
            "archive_path": str(self.archive_path) if self.archive_path else None,
            "archive_sha256": self.archive_sha256,
            "retained_revision": self.retained_revision,
            "worktree_closed": self.worktree_closed,
            "visible": self.visible,
        }


def _visible_result(lane: Path, reason: str, *, archive: Path | None = None, revision: str | None = None) -> RetirementResult:
    return RetirementResult("VISIBLE", reason, lane, archive, retained_revision=revision, visible=True)


def _archive_digest(value: Mapping[str, Any]) -> str:
    normalized = dict(value)
    normalized["archive_content_sha256"] = ""
    return hashlib.sha256((json.dumps(normalized, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")).hexdigest()


def retire_terminal_lane(
    lane_root: str | Path,
    archive_root: str | Path,
    *,
    lane_id: str,
    retained_revision: str,
    task_ref: object = None,
    result_ref: object = None,
    findings_ref: object = None,
    acceptance_ref: object = None,
    transcript_ref: object = None,
    dependency_ref: object = None,
    process_snapshot: ProcessSnapshot | None = None,
    live_processes: Iterable[ProcessInfo | Mapping[str, Any]] | None = None,
    unmerged_work_proof: bool | None = None,
    target_revision: str | None = None,
    discarded_cache: Sequence[object] = (),
) -> RetirementResult:
    """Archive complete terminal evidence, then close one safe linked worktree."""

    lane = _regular_directory(lane_root)
    archive_base = _regular_directory(archive_root, create=True)
    if _inside(archive_base, lane) or _inside(lane, archive_base):
        return _visible_result(lane, "ARCHIVE_ROOT_OVERLAPS_LANE")
    if (
        not isinstance(lane_id, str)
        or not lane_id.strip()
        or Path(lane_id).is_absolute()
        or ".." in Path(lane_id).parts
        or Path(lane_id).name != lane_id
    ):
        return _visible_result(lane, "LANE_ID_INVALID")
    try:
        actual_head = _git_text(lane, "rev-parse", "--verify", "HEAD^{commit}")
        if _HEX_COMMIT.fullmatch(retained_revision or "") is None or actual_head.lower() != retained_revision.lower():
            return _visible_result(lane, "RETAINED_REVISION_UNPROVEN", revision=retained_revision)
        status = _git(lane, "status", "--porcelain=v1", "-z", "--untracked-files=all")
        if status.returncode != 0 or status.stdout:
            return _visible_result(lane, "DIRTY_WORKTREE", revision=actual_head)
        branch = _git(lane, "symbolic-ref", "--quiet", "--short", "HEAD")
        if branch.returncode != 0 or not branch.stdout.strip():
            return _visible_result(lane, "AMBIGUOUS_BRANCH", revision=actual_head)
    except LaneLifecycleError:
        return _visible_result(lane, "GIT_STATE_UNKNOWN", revision=retained_revision)

    if unmerged_work_proof is False:
        return _visible_result(lane, "UNMERGED_WORK_PRESENT", revision=actual_head)
    if target_revision is not None:
        ancestry = _git(lane, "merge-base", "--is-ancestor", actual_head, target_revision)
        if ancestry.returncode != 0:
            return _visible_result(lane, "UNMERGED_WORK_PRESENT", revision=actual_head)
    elif unmerged_work_proof is not True:
        return _visible_result(lane, "UNMERGED_WORK_UNPROVEN", revision=actual_head)

    absent, live_reason, process_evidence = _process_absence(
        process_snapshot=process_snapshot,
        live_processes=live_processes,
    )
    if not absent:
        return _visible_result(lane, live_reason, revision=actual_head)

    required_refs = {
        "task": task_ref,
        "result": result_ref,
        "findings": findings_ref,
        "acceptance": acceptance_ref,
        "transcript": transcript_ref,
        "dependency": dependency_ref,
    }
    if any(value is None for value in required_refs.values()):
        return _visible_result(lane, "ARCHIVE_EVIDENCE_INCOMPLETE", revision=actual_head)

    archive_dir = archive_base / lane_id
    if archive_dir.exists():
        if archive_dir.is_symlink() or not archive_dir.is_dir():
            return _visible_result(lane, "ARCHIVE_DESTINATION_AMBIGUOUS", revision=actual_head)
        archive_path = archive_dir / "LANE_ARCHIVE.json"
        if archive_path.exists():
            return _visible_result(lane, "ARCHIVE_ALREADY_EXISTS", archive=archive_path, revision=actual_head)
    archive_dir.mkdir(parents=True, exist_ok=False)
    archive_path = archive_dir / "LANE_ARCHIVE.json"
    try:
        references = {
            "task": _reference(task_ref, name="task"),
            "result": _reference(result_ref, name="result"),
            "findings": _reference(findings_ref, name="findings"),
            "acceptance": _reference(acceptance_ref, name="acceptance"),
            "transcript": _reference(transcript_ref, name="transcript"),
            "dependency_map": _reference(dependency_ref, name="dependency_map"),
        }
        discarded = []
        for item in discarded_cache:
            if isinstance(item, Mapping):
                discarded.append(dict(item))
            else:
                discarded.append({"path": str(item)})
        archive: dict[str, Any] = {
            "schema": LANE_ARCHIVE_SCHEMA,
            "lane_id": lane_id,
            "archived_utc": iso_utc(utc_now()),
            "task": references["task"],
            "result": references["result"],
            "findings": references["findings"],
            "acceptance": references["acceptance"],
            "transcript": references["transcript"],
            "dependency": references["dependency_map"],
            "references": references,
            "content_identities": {
                name: row.get("sha256") for name, row in references.items()
            },
            "retained_revision": actual_head,
            "worktree": {
                "path": str(lane),
                "clean": True,
                "branch": branch.stdout.decode("utf-8", errors="replace").strip(),
            },
            "no_live_process_proof": {
                "complete": True,
                "state": live_reason,
                "processes": process_evidence,
            },
            "no_unmerged_work_proof": {
                "provided": True,
                "target_revision": target_revision,
                "retained_revision": actual_head,
            },
            "close_result": "PENDING",
            "discarded_cache_inventory": discarded,
            "archive_content_sha256": "",
        }
        archive["archive_content_sha256"] = _archive_digest(archive)
        _atomic_json(archive_path, archive)
    except Exception as exc:
        try:
            archive_path.unlink(missing_ok=True)
            archive_dir.rmdir()
        except OSError:
            pass
        return _visible_result(
            lane,
            f"ARCHIVE_FAILED:{type(exc).__name__}",
            revision=actual_head,
        )

    common_dir_text = _git_text(lane, "rev-parse", "--git-common-dir")
    common_dir = Path(common_dir_text)
    if not common_dir.is_absolute():
        common_dir = (lane / common_dir).resolve()
    worktrees = _git_text(lane, "worktree", "list", "--porcelain")
    admin_root: Path | None = None
    for line in worktrees.splitlines():
        if line.startswith("worktree "):
            candidate = Path(line[len("worktree ") :]).resolve(strict=False)
            if candidate != lane:
                admin_root = candidate
                break
    if admin_root is None:
        return _visible_result(lane, "NO_GIT_ADMIN_WORKTREE", archive=archive_path, revision=actual_head)
    removal = _git(admin_root, "worktree", "remove", str(lane))
    if removal.returncode != 0:
        detail = removal.stderr.decode("utf-8", errors="replace").strip()[:300]
        return _visible_result(lane, f"ARCHIVE_SUCCEEDED_CLOSE_FAILED:{detail or 'git worktree remove failed'}", archive=archive_path, revision=actual_head)
    try:
        closed_archive = json.loads(archive_path.read_text(encoding="utf-8"))
        if not isinstance(closed_archive, dict):
            raise ArchiveFailed("archive changed shape before close result publication")
        closed_archive["close_result"] = "CLOSED"
        closed_archive["closed_utc"] = iso_utc(utc_now())
        closed_archive["archive_content_sha256"] = ""
        closed_archive["archive_content_sha256"] = _archive_digest(closed_archive)
        _atomic_json(archive_path, closed_archive)
    except Exception as exc:
        return RetirementResult("VISIBLE", f"ARCHIVE_CLOSE_RESULT_FAILED:{type(exc).__name__}", lane, archive_path, None, actual_head, True, True)
    retained = _git(common_dir, "cat-file", "-e", f"{actual_head}^{{commit}}")
    if retained.returncode != 0:
        return RetirementResult("VISIBLE", "RETAINED_REVISION_LOST_AFTER_CLOSE", lane, archive_path, _sha256(archive_path), actual_head, False, True)
    return RetirementResult("CLOSED", "ARCHIVED_AND_CLOSED", lane, archive_path, _sha256(archive_path), actual_head, True, False)


retire_lane = retire_terminal_lane
archive_and_retire_terminal_lane = retire_terminal_lane


def validate_lane_archive(path: str | Path) -> dict[str, Any]:
    archive_path = Path(path).expanduser().resolve(strict=False)
    if archive_path.is_symlink() or not archive_path.is_file():
        raise ArchiveFailed("lane archive is not a regular file")
    value = json.loads(archive_path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema") != LANE_ARCHIVE_SCHEMA:
        raise ArchiveFailed("lane archive schema is invalid")
    if value.get("archive_content_sha256") != _archive_digest(value):
        raise ArchiveFailed("lane archive content identity is invalid")
    for name in ("task", "result", "findings", "acceptance", "transcript", "dependency"):
        if name not in value:
            raise ArchiveFailed(f"lane archive missing {name} reference")
        reference = value.get(name)
        if (
            not isinstance(reference, Mapping)
            or reference.get("present") is not True
            or not isinstance(reference.get("path"), str)
            or not isinstance(reference.get("sha256"), str)
        ):
            raise ArchiveFailed(f"lane archive {name} reference is incomplete")
    if value.get("retained_revision") is None or value.get("close_result") not in {"PENDING", "CLOSED"}:
        raise ArchiveFailed("lane archive retained revision/close result is incomplete")
    return value


__all__ = [
    "ArchiveFailed",
    "IMMUTABLE_VIEW_SCHEMA",
    "ImmutableSourceView",
    "ImmutableViewError",
    "LANE_ARCHIVE_SCHEMA",
    "LaneLifecycleError",
    "RetirementBlocked",
    "RetirementResult",
    "allocate_immutable_source_view",
    "allocate_immutable_view",
    "allocate_source_view",
    "archive_and_retire_terminal_lane",
    "retire_lane",
    "retire_terminal_lane",
    "validate_lane_archive",
]
