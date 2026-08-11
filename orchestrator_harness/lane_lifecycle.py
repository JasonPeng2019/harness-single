"""Retained immutable source views and archive-first terminal retirement."""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import ProcessInfo, ProcessSnapshot, iso_utc, parse_utc, utc_now


IMMUTABLE_VIEW_SCHEMA = "orchestrator-immutable-source-view/v1"
LANE_ARCHIVE_SCHEMA = "orchestrator-lane-archive/v1"
PROCESS_EVIDENCE_SCHEMA = "orchestrator-process-evidence/v1"
_HEX_COMMIT = re.compile(r"^[0-9a-fA-F]{40}|[0-9a-fA-F]{64}$")
_GIT_ENV_KEYS = (
    "PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "SYSTEMDRIVE",
    "TEMP", "TMP", "TMPDIR",
)


class LaneLifecycleError(ValueError):
    """A lifecycle request is invalid or cannot be proved safe."""


class ImmutableViewError(LaneLifecycleError):
    pass


class RetirementBlocked(LaneLifecycleError):
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
        result = subprocess.run(
            ["git", "-C", str(cwd), *args], stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
            timeout=15, env=_git_env(), shell=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LaneLifecycleError(f"Git lifecycle inspection failed: {exc}") from exc
    if check and result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()[:500]
        raise LaneLifecycleError(f"Git lifecycle operation failed: {detail}")
    return result


def _git_text(cwd: Path, *args: str) -> str:
    result = _git(cwd, *args, check=True)
    return result.stdout.decode("utf-8", errors="strict").strip()


def _is_reparse(path: Path) -> bool:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(info.st_mode):
        return True
    return bool(getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _lexical(path: str | Path) -> Path:
    return Path(os.path.abspath(str(Path(path).expanduser())))


def _reject_reparse_chain(path: Path) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        if os.path.lexists(current) and _is_reparse(current):
            raise LaneLifecycleError(f"path contains a symlink or reparse point: {current}")


def _regular_directory(path: str | Path, *, create: bool = False) -> Path:
    value = _lexical(path)
    _reject_reparse_chain(value)
    if create and not value.exists():
        parent = value.parent
        _regular_directory(parent, create=True)
        value.mkdir()
    if not value.is_dir() or _is_reparse(value):
        raise LaneLifecycleError(f"expected a regular directory: {value}")
    return value


def _regular_file(path: str | Path, *, name: str) -> tuple[Path, bytes]:
    value = _lexical(path)
    _reject_reparse_chain(value)
    if _is_reparse(value) or not value.is_file():
        raise ArchiveFailed(f"{name} reference is not a regular file: {value}")
    try:
        data = value.read_bytes()
    except OSError as exc:
        raise ArchiveFailed(f"{name} reference cannot be read: {value}") from exc
    if len(data) > 8_000_000:
        raise ArchiveFailed(f"{name} reference is oversized")
    return value, data


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


def _exact_revision(repo: Path, value: str, *, name: str) -> str:
    if not isinstance(value, str) or _HEX_COMMIT.fullmatch(value) is None:
        raise LaneLifecycleError(f"{name} must be a full hexadecimal commit identity")
    resolved = _git_text(repo, "rev-parse", "--verify", f"{value}^{{commit}}")
    if resolved.lower() != value.lower():
        raise LaneLifecycleError(f"{name} does not resolve to its exact declared identity")
    _git(repo, "cat-file", "-e", f"{value}^{{commit}}", check=True)
    return resolved.lower()


def _safe_ref(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 512 or value.startswith("-") or ".." in value:
        raise LaneLifecycleError(f"{name} is not a supported retained Git ref")
    return value.strip()


def _retained_commit(repo: Path, revision: str, retained_ref: str) -> str:
    ref = _safe_ref(retained_ref, name="retained_ref")
    retained = _git_text(repo, "rev-parse", "--verify", f"{ref}^{{commit}}")
    if _git(repo, "merge-base", "--is-ancestor", revision, ref).returncode != 0:
        raise ImmutableViewError("declared revision is not reachable from retained_ref")
    return retained.lower()


def _assert_retained(repo: Path, revision: str, retained_ref: str, retained_commit: str) -> None:
    current_revision = _git_text(repo, "rev-parse", "--verify", f"{revision}^{{commit}}")
    current_retained = _git_text(repo, "rev-parse", "--verify", f"{retained_ref}^{{commit}}")
    if current_revision.lower() != revision.lower() or current_retained.lower() != retained_commit.lower():
        raise ImmutableViewError("retained revision or ref changed during allocation")
    if _git(repo, "merge-base", "--is-ancestor", revision, retained_ref).returncode != 0:
        raise ImmutableViewError("retained ref no longer reaches the requested commit")


def _safe_member(name: str) -> bool:
    path = Path(name)
    return not path.is_absolute() and ".." not in path.parts and not any(part in {"", "."} for part in path.parts)


def _set_read_only(root: Path) -> None:
    for path in sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        if _is_reparse(path):
            raise ImmutableViewError(f"source view contains an indirection: {path}")
        try:
            path.chmod(stat.S_IREAD | stat.S_IEXEC if path.is_dir() else stat.S_IREAD)
        except OSError as exc:
            raise ImmutableViewError(f"cannot establish source read-only boundary: {path}") from exc
    root.chmod(stat.S_IREAD | stat.S_IEXEC)


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    parent = path.parent
    if not parent.is_dir() or _is_reparse(parent):
        raise LaneLifecycleError(f"publication parent is unsafe: {parent}")
    parent_identity = (parent.stat().st_dev, parent.stat().st_ino)
    temporary: Path | None = None
    try:
        descriptor, raw_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(parent))
        temporary = Path(raw_name)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write((json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
        if (parent.stat().st_dev, parent.stat().st_ino) != parent_identity or _is_reparse(parent):
            raise LaneLifecycleError("publication parent identity changed")
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            try:
                if (parent.stat().st_dev, parent.stat().st_ino) == parent_identity and not _is_reparse(parent):
                    temporary.unlink(missing_ok=True)
            except OSError:
                pass


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
    member_hashes: Mapping[str, str] = field(default_factory=dict)

    def as_record(self) -> dict[str, Any]:
        return {
            "schema": IMMUTABLE_VIEW_SCHEMA, "view_id": self.view_id,
            "source_root": str(self.source_root), "view_root": str(self.view_root),
            "result_root": str(self.result_root), "cache_root": str(self.cache_root),
            "retained_commit": self.retained_commit, "manifest_path": str(self.manifest_path),
            "ready_path": str(self.ready_path), "read_only": self.read_only,
        }

    def write_source(self, relative: str | Path, data: bytes | str) -> None:
        del relative, data
        raise ImmutableViewError("immutable source view rejects writes")

    def assert_read_only(self) -> bool:
        if not self.read_only or not self.manifest_path.is_file() or not self.ready_path.is_file():
            raise ImmutableViewError("immutable source view is not admitted ready")
        try:
            manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
            ready = json.loads(self.ready_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ImmutableViewError("immutable readiness evidence is unreadable") from exc
        if not isinstance(manifest, dict) or manifest.get("ready") is not True:
            raise ImmutableViewError("immutable manifest is not finally ready")
        if not isinstance(ready, dict) or ready.get("ready") is not True:
            raise ImmutableViewError("immutable READY record is not final")
        if ready.get("retained_commit") != self.retained_commit or ready.get("manifest_sha256") != _sha256(self.manifest_path):
            raise ImmutableViewError("READY does not bind the final immutable manifest")
        for relative, digest in dict(self.member_hashes or manifest.get("member_sha256", {})).items():
            path = self.view_root / relative
            if not path.is_file() or _is_reparse(path) or _sha256(path) != digest:
                raise ImmutableViewError("immutable source bytes changed after admission")
        return True


def _allocation_root(path: str | Path, *, name: str) -> Path:
    value = _lexical(path)
    _reject_reparse_chain(value)
    if os.path.lexists(value) and _is_reparse(value):
        raise ImmutableViewError(f"{name} may not be a reparse point")
    _regular_directory(value.parent, create=True)
    return value


def allocate_immutable_source_view(
    source_root: str | Path,
    *,
    revision: str,
    retained_ref: str,
    view_root: str | Path,
    result_root: str | Path,
    cache_root: str | Path,
    view_id: str = "immutable-view",
) -> ImmutableSourceView:
    """Extract one exact commit only while its declared retained ref reaches it."""

    source = _regular_directory(source_root)
    view = _allocation_root(view_root, name="view_root")
    result = _regular_directory(result_root, create=True)
    cache = _regular_directory(cache_root, create=True)
    if not isinstance(view_id, str) or not view_id.strip() or len(view_id) > 200:
        raise ImmutableViewError("view_id must be a bounded non-empty string")
    if os.path.lexists(view):
        raise ImmutableViewError("view_root must not already exist")
    _separate_root(view, source=source, other=(result, cache))
    _separate_root(result, source=source, other=(view, cache))
    _separate_root(cache, source=source, other=(view, result))
    commit = _exact_revision(source, revision, name="revision")
    retained_commit = _retained_commit(source, commit, retained_ref)
    manifest = result / "IMMUTABLE_VIEW.json"
    ready = result / "VIEW_READY.json"
    if manifest.exists() or ready.exists():
        raise ImmutableViewError("result root already contains an allocation record")
    view.mkdir()
    view_identity = (view.stat().st_dev, view.stat().st_ino)
    admitted = False
    try:
        _assert_retained(source, commit, retained_ref, retained_commit)
        archive = _git(source, "archive", "--format=tar", commit, check=True)
        member_hashes: dict[str, str] = {}
        with tarfile.open(fileobj=io.BytesIO(archive.stdout), mode="r:") as tar:
            members = tar.getmembers()
            if any(not _safe_member(item.name) or item.issym() or item.islnk() or item.isdev() or item.isfifo() for item in members):
                raise ImmutableViewError("source archive contains an unsafe member")
            for member in members:
                destination = view / member.name
                if not _inside(destination.absolute(), view.absolute()):
                    raise ImmutableViewError("source archive member escapes the view")
                if member.isdir():
                    destination.mkdir(parents=True, exist_ok=True)
                    continue
                handle = tar.extractfile(member)
                if handle is None:
                    raise ImmutableViewError(f"source archive member cannot be read: {member.name}")
                destination.parent.mkdir(parents=True, exist_ok=True)
                _reject_reparse_chain(destination.parent)
                if _is_reparse(destination.parent):
                    raise ImmutableViewError("source view extraction encountered an indirection")
                data = handle.read()
                destination.write_bytes(data)
                member_hashes[member.name] = hashlib.sha256(data).hexdigest()
        _set_read_only(view)
        _assert_retained(source, commit, retained_ref, retained_commit)
        record = {
            "schema": IMMUTABLE_VIEW_SCHEMA, "view_id": view_id,
            "source_root": str(source), "view_root": str(view),
            "result_root": str(result), "cache_root": str(cache),
            "retained_commit": commit, "retained_ref": retained_ref,
            "created_utc": iso_utc(utc_now()),
            "read_only_boundary": {
                "mechanism": "filesystem-read-only-plus-hash-admission-and-write-api-refusal",
                "source_output_forbidden": True, "result_root_writable": True,
                "cache_root_writable": True,
            },
            "member_sha256": dict(sorted(member_hashes.items())),
            "ready": True,
        }
        # The manifest is final before READY is published.  READY binds the
        # exact bytes of this final ready:true document.
        _atomic_json(manifest, record)
        _assert_retained(source, commit, retained_ref, retained_commit)
        manifest_hash = _sha256(manifest)
        _atomic_json(ready, {
            "schema": IMMUTABLE_VIEW_SCHEMA, "view_id": view_id,
            "retained_commit": commit, "retained_ref": retained_ref,
            "manifest_sha256": manifest_hash, "admitted_utc": iso_utc(utc_now()),
            "ready": True,
        })
        if _sha256(manifest) != manifest_hash:
            raise ImmutableViewError("immutable manifest changed before READY publication")
        admitted = True
        return ImmutableSourceView(view_id, source, view, result, cache, commit, manifest, ready, True, member_hashes)
    except Exception:
        ready.unlink(missing_ok=True)
        manifest.unlink(missing_ok=True)
        if not admitted:
            try:
                if view.exists() and not _is_reparse(view) and (view.stat().st_dev, view.stat().st_ino) == view_identity:
                    children = list(view.rglob("*"))
                    for child in children:
                        if _is_reparse(child):
                            raise ImmutableViewError("refusing cleanup through a source-view link")
                    for child in children:
                        try:
                            child.chmod(stat.S_IWRITE | stat.S_IREAD | (stat.S_IEXEC if child.is_dir() else 0))
                        except OSError:
                            pass
                    view.chmod(stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
                    shutil.rmtree(view)
            except OSError:
                pass
        raise


allocate_source_view = allocate_immutable_source_view
allocate_immutable_view = allocate_immutable_source_view


def _source_reference(value: object, *, name: str) -> tuple[dict[str, Any], bytes]:
    if isinstance(value, Mapping):
        source_value = value.get("path")
        expected = value.get("sha256")
    else:
        source_value = value
        expected = None
    if not isinstance(source_value, (str, Path)):
        raise ArchiveFailed(f"{name} reference path is invalid")
    path, data = _regular_file(source_value, name=name)
    digest = hashlib.sha256(data).hexdigest()
    if expected is not None and expected != digest:
        raise ArchiveFailed(f"{name} reference hash does not match")
    return {
        "name": name, "present": True, "member": None, "sha256": digest,
        "size": len(data), "source_identity": {"path": str(path), "sha256": digest},
    }, data


def _load_process_evidence(value: object) -> tuple[dict[str, Any], bytes, ProcessSnapshot, dict[str, dict[str, Any]]]:
    if not isinstance(value, (str, Path)):
        raise ArchiveFailed("process evidence must be a persisted evidence file")
    reference, data = _source_reference(value, name="process_evidence")
    try:
        raw = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArchiveFailed("process evidence is malformed") from exc
    if not isinstance(raw, dict) or raw.get("schema") != PROCESS_EVIDENCE_SCHEMA or raw.get("complete") is not True:
        raise ArchiveFailed("process evidence is not a complete admitted snapshot")
    identities = raw.get("identities")
    processes = raw.get("processes")
    if not isinstance(identities, dict) or set(identities) != {"controller", "worker", "helper"}:
        raise ArchiveFailed("process evidence lacks exact controller/worker/helper identities")
    if not isinstance(processes, list) or len(processes) > 4096:
        raise ArchiveFailed("process evidence snapshot is incomplete")
    rows: list[ProcessInfo] = []
    seen: set[int] = set()
    for item in processes:
        if not isinstance(item, Mapping) or set(item) != {"pid", "ppid", "name", "command_line", "created_utc"}:
            raise ArchiveFailed("process evidence contains an incomplete process row")
        pid, ppid = item.get("pid"), item.get("ppid")
        created = parse_utc(item.get("created_utc"))
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0 or not isinstance(ppid, int) or ppid < 0 or created is None or not isinstance(item.get("name"), str) or not isinstance(item.get("command_line"), str) or pid in seen:
            raise ArchiveFailed("process evidence contains an invalid process identity")
        seen.add(pid)
        rows.append(ProcessInfo(pid, ppid, item["name"], item["command_line"], created))
    normalized: dict[str, dict[str, Any]] = {}
    role_pids: set[int] = set()
    for role in ("controller", "worker", "helper"):
        item = identities[role]
        if not isinstance(item, Mapping) or set(item) != {"pid", "created_utc"}:
            raise ArchiveFailed(f"persisted {role} identity is missing or ambiguous")
        pid, created = item.get("pid"), parse_utc(item.get("created_utc"))
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0 or created is None:
            raise ArchiveFailed(f"persisted {role} identity is invalid")
        if pid in role_pids:
            raise ArchiveFailed("persisted process identities are ambiguous")
        role_pids.add(pid)
        normalized[role] = {"pid": pid, "created_utc": iso_utc(created)}
    return reference, data, ProcessSnapshot(True, tuple(rows), provider=str(raw.get("provider", "persisted"))), normalized


def _process_proof(snapshot: ProcessSnapshot, identities: Mapping[str, Mapping[str, Any]]) -> tuple[bool, str, dict[str, Any]]:
    states: dict[str, str] = {}
    for role, identity in identities.items():
        pid = identity["pid"]
        expected = parse_utc(identity["created_utc"])
        actual = snapshot.by_pid.get(pid)
        if actual is None:
            states[role] = "ABSENT"
        elif actual.created_utc is None:
            states[role] = "AMBIGUOUS"
        elif actual.created_utc != expected:
            states[role] = "REUSED"
        else:
            states[role] = "LIVE"
    if "AMBIGUOUS" in states.values():
        return False, "LIVE_USE_AMBIGUOUS", {"complete": True, "states": states}
    if "LIVE" in states.values():
        return False, "LIVE_USE_PROVEN", {"complete": True, "states": states}
    return True, "NO_LIVE_USE_PROVED", {"complete": True, "states": states}


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
            "schema": LANE_ARCHIVE_SCHEMA, "outcome": self.outcome, "reason": self.reason,
            "lane_root": str(self.lane_root), "archive_path": str(self.archive_path) if self.archive_path else None,
            "archive_sha256": self.archive_sha256, "retained_revision": self.retained_revision,
            "worktree_closed": self.worktree_closed, "visible": self.visible,
        }


def _visible_result(lane: Path, reason: str, *, archive: Path | None = None, revision: str | None = None) -> RetirementResult:
    return RetirementResult("VISIBLE", reason, lane, archive, retained_revision=revision, visible=True)


def _archive_digest(value: Mapping[str, Any]) -> str:
    normalized = dict(value)
    normalized["archive_content_sha256"] = ""
    return hashlib.sha256((json.dumps(normalized, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")).hexdigest()


def _copy_member(stage: Path, name: str, data: bytes) -> dict[str, Any]:
    if not _safe_member(name):
        raise ArchiveFailed(f"unsafe archive member name: {name}")
    destination = stage / name
    destination.parent.mkdir(parents=True, exist_ok=True)
    _reject_reparse_chain(destination.parent)
    if _is_reparse(destination.parent):
        raise ArchiveFailed("archive staging parent became a reparse point")
    temporary: Path | None = None
    try:
        fd, raw_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent))
        temporary = Path(raw_name)
        with os.fdopen(fd, "wb", closefd=True) as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    digest = hashlib.sha256(data).hexdigest()
    if not destination.is_file() or _sha256(destination) != digest or destination.stat().st_size != len(data):
        raise ArchiveFailed(f"archive member validation failed: {name}")
    return {"member": name, "sha256": digest, "size": len(data)}


def retire_terminal_lane(
    lane_root: str | Path,
    archive_root: str | Path,
    *,
    lane_id: str,
    retained_revision: str,
    retained_ref: str,
    target_revision: str,
    process_evidence: str | Path,
    task_ref: object = None,
    result_ref: object = None,
    findings_ref: object = None,
    acceptance_ref: object = None,
    transcript_ref: object = None,
    dependency_ref: object = None,
    discarded_cache: Sequence[object] = (),
) -> RetirementResult:
    """Copy and validate all evidence, then close only a mechanically safe lane."""

    try:
        lane = _regular_directory(lane_root)
        archive_base = _regular_directory(archive_root, create=True)
    except LaneLifecycleError as exc:
        return _visible_result(_lexical(lane_root), f"PATH_UNSAFE:{type(exc).__name__}")
    if _inside(archive_base, lane) or _inside(lane, archive_base):
        return _visible_result(lane, "ARCHIVE_ROOT_OVERLAPS_LANE")
    if not isinstance(lane_id, str) or not lane_id.strip() or Path(lane_id).is_absolute() or ".." in Path(lane_id).parts or Path(lane_id).name != lane_id:
        return _visible_result(lane, "LANE_ID_INVALID")
    try:
        actual_head = _exact_revision(lane, retained_revision, name="retained_revision")
        target = _exact_revision(lane, target_revision, name="target_revision")
        ref = _safe_ref(retained_ref, name="retained_ref")
        retained_ref_commit = _git_text(lane, "rev-parse", "--verify", f"{ref}^{{commit}}").lower()
        if _git(lane, "merge-base", "--is-ancestor", actual_head, ref).returncode != 0:
            return _visible_result(lane, "RETAINED_REVISION_UNPROVEN", revision=actual_head)
        if _git(lane, "merge-base", "--is-ancestor", actual_head, target).returncode != 0:
            return _visible_result(lane, "UNMERGED_WORK_PRESENT", revision=actual_head)
        status = _git(lane, "status", "--porcelain=v1", "-z", "--untracked-files=all")
        if status.returncode != 0 or status.stdout:
            return _visible_result(lane, "DIRTY_WORKTREE", revision=actual_head)
        branch = _git(lane, "symbolic-ref", "--quiet", "--short", "HEAD")
        if branch.returncode != 0 or not branch.stdout.strip():
            return _visible_result(lane, "AMBIGUOUS_BRANCH", revision=actual_head)
    except LaneLifecycleError as exc:
        return _visible_result(lane, f"GIT_STATE_UNKNOWN:{type(exc).__name__}", revision=retained_revision)

    required = {
        "task": task_ref, "result": result_ref, "findings": findings_ref,
        "acceptance": acceptance_ref, "transcript": transcript_ref, "dependency": dependency_ref,
    }
    if any(value is None for value in required.values()):
        return _visible_result(lane, "ARCHIVE_EVIDENCE_INCOMPLETE", revision=actual_head)
    try:
        refs: dict[str, tuple[dict[str, Any], bytes]] = {name: _source_reference(value, name=name) for name, value in required.items()}
        process_ref, process_bytes, process_snapshot, identities = _load_process_evidence(process_evidence)
        absent, live_reason, process_proof = _process_proof(process_snapshot, identities)
        if not absent:
            return _visible_result(lane, live_reason, revision=actual_head)
    except ArchiveFailed as exc:
        return _visible_result(lane, f"ARCHIVE_EVIDENCE_INCOMPLETE:{str(exc)[:160]}", revision=actual_head)

    archive_dir = archive_base / lane_id
    if os.path.lexists(archive_dir):
        return _visible_result(lane, "ARCHIVE_ALREADY_EXISTS", archive=archive_dir / "LANE_ARCHIVE.json", revision=actual_head)
    staging = Path(tempfile.mkdtemp(prefix=f".{lane_id}.staging-", dir=str(archive_base)))
    archive_path = staging / "LANE_ARCHIVE.json"
    try:
        archive_refs: dict[str, dict[str, Any]] = {}
        for name, (reference, data) in refs.items():
            member = _copy_member(staging, f"evidence/{name}.json", data)
            archive_refs[name] = {**reference, **member}
        process_member = _copy_member(staging, "evidence/process_evidence.json", process_bytes)
        archive_refs["process_evidence"] = {**process_ref, **process_member}
        discarded = [dict(item) if isinstance(item, Mapping) else {"path": str(item)} for item in discarded_cache]
        archive: dict[str, Any] = {
            "schema": LANE_ARCHIVE_SCHEMA, "lane_id": lane_id, "archived_utc": iso_utc(utc_now()),
            "task": archive_refs["task"], "result": archive_refs["result"],
            "findings": archive_refs["findings"], "acceptance": archive_refs["acceptance"],
            "transcript": archive_refs["transcript"], "dependency": archive_refs["dependency"],
            "process_evidence": archive_refs["process_evidence"], "references": archive_refs,
            "content_identities": {name: item["sha256"] for name, item in archive_refs.items()},
            "retained_revision": actual_head, "retained_ref": ref,
            "retained_ref_commit": retained_ref_commit, "target_revision": target,
            "worktree": {"path": str(lane), "clean": True, "branch": branch.stdout.decode("utf-8", errors="replace").strip()},
            "no_live_process_proof": {**process_proof, "state": live_reason},
            "no_unmerged_work_proof": {"target_revision": target, "ancestry_verified": True},
            "close_result": "PENDING", "discarded_cache_inventory": discarded,
            "archive_content_sha256": "",
        }
        archive["archive_content_sha256"] = _archive_digest(archive)
        _atomic_json(archive_path, archive)
        validate_lane_archive(archive_path)
        os.replace(staging, archive_dir)
        staging = archive_dir
        archive_path = archive_dir / "LANE_ARCHIVE.json"
        validate_lane_archive(archive_path)
    except Exception as exc:
        try:
            if staging.exists() and not _is_reparse(staging):
                shutil.rmtree(staging)
        except OSError:
            pass
        return _visible_result(lane, f"ARCHIVE_FAILED:{type(exc).__name__}", revision=actual_head)

    blocks = _git_text(lane, "worktree", "list", "--porcelain").split("\n\n")
    admin_root: Path | None = None
    for block in blocks:
        candidate = next((line[len("worktree "):] for line in block.splitlines() if line.startswith("worktree ")), None)
        if candidate is not None and _lexical(candidate) != lane:
            admin_root = _lexical(candidate)
            break
    if admin_root is None:
        return _visible_result(lane, "NO_GIT_ADMIN_WORKTREE", archive=archive_path, revision=actual_head)
    removal = _git(admin_root, "worktree", "remove", str(lane))
    if removal.returncode != 0:
        detail = removal.stderr.decode("utf-8", errors="replace").strip()[:300]
        return _visible_result(lane, f"ARCHIVE_SUCCEEDED_CLOSE_FAILED:{detail or 'git worktree remove failed'}", archive=archive_path, revision=actual_head)
    archive_digest = _sha256(archive_path)
    try:
        closed = json.loads(archive_path.read_text(encoding="utf-8"))
        if not isinstance(closed, dict):
            raise ArchiveFailed("archive changed shape after Git close")
        closed["close_result"] = "CLOSED"
        closed["closed_utc"] = iso_utc(utc_now())
        closed["archive_content_sha256"] = ""
        closed["archive_content_sha256"] = _archive_digest(closed)
        _atomic_json(archive_path, closed)
        validate_lane_archive(archive_path)
    except Exception as exc:
        # Git close is already proven.  Preserve the self-contained PENDING
        # archive and report closed metadata uncertainty honestly.
        return RetirementResult("CLOSED_UNCERTAIN", f"ARCHIVE_CLOSE_RESULT_FAILED:{type(exc).__name__}", lane, archive_path, archive_digest, actual_head, True, False)
    return RetirementResult("CLOSED", "ARCHIVED_AND_CLOSED", lane, archive_path, _sha256(archive_path), actual_head, True, False)


retire_lane = retire_terminal_lane
archive_and_retire_terminal_lane = retire_terminal_lane


def validate_lane_archive(path: str | Path) -> dict[str, Any]:
    archive_path = _lexical(path)
    if _is_reparse(archive_path) or not archive_path.is_file():
        raise ArchiveFailed("lane archive is not a regular file")
    try:
        value = json.loads(archive_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArchiveFailed("lane archive cannot be read") from exc
    if not isinstance(value, dict) or value.get("schema") != LANE_ARCHIVE_SCHEMA:
        raise ArchiveFailed("lane archive schema is invalid")
    if value.get("archive_content_sha256") != _archive_digest(value):
        raise ArchiveFailed("lane archive content identity is invalid")
    refs = value.get("references")
    if not isinstance(refs, Mapping) or set(refs) != {"task", "result", "findings", "acceptance", "transcript", "dependency", "process_evidence"}:
        raise ArchiveFailed("lane archive evidence member set is incomplete")
    for name, reference in refs.items():
        if not isinstance(reference, Mapping) or reference.get("present") is not True or not isinstance(reference.get("member"), str) or not _safe_member(reference["member"]) or not isinstance(reference.get("sha256"), str) or not isinstance(reference.get("size"), int):
            raise ArchiveFailed(f"lane archive {name} member identity is incomplete")
        member = archive_path.parent / reference["member"]
        if _is_reparse(member) or not member.is_file() or member.stat().st_size != reference["size"] or _sha256(member) != reference["sha256"]:
            raise ArchiveFailed(f"lane archive {name} member bytes are unavailable or invalid")
    if value.get("retained_revision") is None or value.get("target_revision") is None or value.get("close_result") not in {"PENDING", "CLOSED"}:
        raise ArchiveFailed("lane archive revision/close result is incomplete")
    return value


__all__ = [
    "ArchiveFailed", "IMMUTABLE_VIEW_SCHEMA", "ImmutableSourceView", "ImmutableViewError",
    "LANE_ARCHIVE_SCHEMA", "LaneLifecycleError", "RetirementBlocked", "RetirementResult",
    "allocate_immutable_source_view", "allocate_immutable_view", "allocate_source_view",
    "archive_and_retire_terminal_lane", "retire_lane", "retire_terminal_lane", "validate_lane_archive",
]
