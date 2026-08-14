"""Retained immutable source views and archive-first terminal retirement."""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import stat
import subprocess
import tarfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import ProcessInfo, ProcessSnapshot, iso_utc, parse_utc, utc_now
from .mutation import (
    MutationConflict,
    MutationReceipt,
    MutationUnsupported,
    TargetState,
    capture_target,
    ensure_directory_path,
    make_temporary_directory,
    remove_tree,
)
from .mutation import (
    delete as mutation_delete,
)
from .mutation import (
    rename as mutation_rename,
)
from .mutation import (
    replace as mutation_replace,
)
from .processes import WINDOWS_CREATE_NO_WINDOW, process_snapshot
from .workspace_overlay import (
    OVERLAY_RECEIPT_SCHEMA,
    restore_worktree,
    verify_overlay_receipt,
)

IMMUTABLE_VIEW_SCHEMA = "orchestrator-immutable-source-view/v1"
LANE_ARCHIVE_SCHEMA = "orchestrator-lane-archive/v1"
PROCESS_EVIDENCE_SCHEMA = "orchestrator-process-evidence/v1"
LIFECYCLE_REGISTRY_SCHEMA = "orchestrator-lifecycle-registry/v1"
_LIFECYCLE_REGISTRY_DIR = ".orchestrator-lifecycle-registry"
_HEX_COMMIT = re.compile(r"^[0-9a-fA-F]{40}|[0-9a-fA-F]{64}$")
_GIT_ENV_KEYS = (
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "WINDIR",
    "COMSPEC",
    "SYSTEMDRIVE",
    "TEMP",
    "TMP",
    "TMPDIR",
)


class LaneLifecycleError(ValueError):
    """A lifecycle request is invalid or cannot be proved safe."""


class ImmutableViewError(LaneLifecycleError):
    pass


class RetirementBlocked(LaneLifecycleError):
    def __init__(
        self, reason: str, *, result: "RetirementResult | None" = None
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.result = result


class ArchiveFailed(LaneLifecycleError):
    pass


def _git_env() -> dict[str, str]:
    env = {key: os.environ[key] for key in _GIT_ENV_KEYS if key in os.environ}
    env.update({"GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0", "LC_ALL": "C"})
    return env


def _git(
    cwd: Path, *args: str, check: bool = False
) -> subprocess.CompletedProcess[bytes]:
    try:
        result = subprocess.run(
            ["git", "-C", str(cwd), *args],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=15,
            env=_git_env(),
            shell=False,
            creationflags=WINDOWS_CREATE_NO_WINDOW if os.name == "nt" else 0,
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
    return bool(
        getattr(info, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _lexical(path: str | Path) -> Path:
    return Path(os.path.abspath(str(Path(path).expanduser())))


def _reject_reparse_chain(path: Path) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        if os.path.lexists(current) and _is_reparse(current):
            raise LaneLifecycleError(
                f"path contains a symlink or reparse point: {current}"
            )


def _regular_directory(path: str | Path, *, create: bool = False) -> Path:
    value = _lexical(path)
    _reject_reparse_chain(value)
    if create and not value.exists():
        try:
            ensure_directory_path(value)
        except (MutationConflict, MutationUnsupported) as exc:
            raise LaneLifecycleError(str(exc)) from exc
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
        raise ImmutableViewError(
            "result/cache/view roots must be outside the source root"
        )
    for sibling in other:
        if _inside(path, sibling) or _inside(sibling, path):
            raise ImmutableViewError("result and cache roots must be separate")


def _exact_revision(repo: Path, value: str, *, name: str) -> str:
    if not isinstance(value, str) or _HEX_COMMIT.fullmatch(value) is None:
        raise LaneLifecycleError(f"{name} must be a full hexadecimal commit identity")
    resolved = _git_text(repo, "rev-parse", "--verify", f"{value}^{{commit}}")
    if resolved.lower() != value.lower():
        raise LaneLifecycleError(
            f"{name} does not resolve to its exact declared identity"
        )
    _git(repo, "cat-file", "-e", f"{value}^{{commit}}", check=True)
    return resolved.lower()


def _safe_ref(value: object, *, name: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > 512
        or value.startswith("-")
        or ".." in value
    ):
        raise LaneLifecycleError(f"{name} is not a supported retained Git ref")
    return value.strip()


def _retained_commit(repo: Path, revision: str, retained_ref: str) -> str:
    ref = _safe_ref(retained_ref, name="retained_ref")
    retained = _git_text(repo, "rev-parse", "--verify", f"{ref}^{{commit}}")
    if _git(repo, "merge-base", "--is-ancestor", revision, ref).returncode != 0:
        raise ImmutableViewError("declared revision is not reachable from retained_ref")
    return retained.lower()


def _assert_retained(
    repo: Path, revision: str, retained_ref: str, retained_commit: str
) -> None:
    current_revision = _git_text(
        repo, "rev-parse", "--verify", f"{revision}^{{commit}}"
    )
    current_retained = _git_text(
        repo, "rev-parse", "--verify", f"{retained_ref}^{{commit}}"
    )
    if (
        current_revision.lower() != revision.lower()
        or current_retained.lower() != retained_commit.lower()
    ):
        raise ImmutableViewError("retained revision or ref changed during allocation")
    if (
        _git(repo, "merge-base", "--is-ancestor", revision, retained_ref).returncode
        != 0
    ):
        raise ImmutableViewError("retained ref no longer reaches the requested commit")


def _safe_member(name: str) -> bool:
    path = Path(name)
    return (
        not path.is_absolute()
        and ".." not in path.parts
        and not any(part in {"", "."} for part in path.parts)
    )


def _set_read_only(root: Path) -> None:
    for path in sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        if _is_reparse(path):
            raise ImmutableViewError(f"source view contains an indirection: {path}")
        try:
            path.chmod(stat.S_IREAD | stat.S_IEXEC if path.is_dir() else stat.S_IREAD)
        except OSError as exc:
            raise ImmutableViewError(
                f"cannot establish source read-only boundary: {path}"
            ) from exc
    root.chmod(stat.S_IREAD | stat.S_IEXEC)


def _atomic_json(path: Path, value: Mapping[str, Any]) -> MutationReceipt:
    parent = path.parent
    if not parent.is_dir() or _is_reparse(parent):
        raise LaneLifecycleError(f"publication parent is unsafe: {parent}")
    try:
        data = (
            json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
        ).encode("utf-8")
        expected = capture_target(parent, path.name)
        return mutation_replace(parent, path.name, data, expected=expected)
    except (MutationConflict, MutationUnsupported) as exc:
        raise LaneLifecycleError(str(exc)) from exc


def _registry_lane_id(lane_id: object) -> str:
    if (
        not isinstance(lane_id, str)
        or not lane_id.strip()
        or Path(lane_id).is_absolute()
        or ".." in Path(lane_id).parts
        or Path(lane_id).name != lane_id
    ):
        raise LaneLifecycleError("lane_id is not a supported lifecycle coordinate")
    return lane_id.strip()


def _path_identity(path: Path) -> tuple[int, int]:
    try:
        info = path.lstat()
    except OSError as exc:
        raise LaneLifecycleError(
            f"canonical Git identity is unavailable: {path}"
        ) from exc
    if _is_reparse(path) or not stat.S_ISDIR(info.st_mode):
        raise LaneLifecycleError(
            f"canonical Git identity is not a regular directory: {path}"
        )
    return int(info.st_dev), int(info.st_ino)


def _canonical_registry_identity(lane_root: str | Path, lane_id: str) -> dict[str, Any]:
    """Derive registry provenance only from the live Git target."""

    lane = _regular_directory(lane_root)
    lane_value = _registry_lane_id(lane_id)
    try:
        worktree_raw = Path(_git_text(lane, "rev-parse", "--show-toplevel"))
        worktree = _lexical(
            worktree_raw if worktree_raw.is_absolute() else lane / worktree_raw
        )
        common = _common_directory(lane)
        branch = _git_text(lane, "symbolic-ref", "--quiet", "--short", "HEAD")
    except LaneLifecycleError:
        raise
    except Exception as exc:
        raise LaneLifecycleError(
            f"canonical Git identity is unavailable: {exc}"
        ) from exc
    if not _same_path(worktree, lane) or not branch:
        raise LaneLifecycleError(
            "canonical Git worktree or attached branch is ambiguous"
        )
    return {
        "worktree_root": str(worktree),
        "worktree_identity": list(_path_identity(worktree)),
        "common_dir": str(common),
        "common_dir_identity": list(_path_identity(common)),
        "lane_id": lane_value,
        "branch": branch,
    }


def _registry_coordinate_seed(identity: Mapping[str, Any]) -> str:
    return json.dumps(
        {
            "worktree_root": identity["worktree_root"],
            "worktree_identity": identity["worktree_identity"],
            "common_dir": identity["common_dir"],
            "common_dir_identity": identity["common_dir_identity"],
            "lane_id": identity["lane_id"],
            "branch": identity["branch"],
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def lifecycle_registry_root(lane_root: str | Path, lane_id: str) -> Path:
    """Return the canonical common-Git registry directory for one lane."""

    identity = _canonical_registry_identity(lane_root, lane_id)
    digest = hashlib.sha256(
        _registry_coordinate_seed(identity).encode("utf-8")
    ).hexdigest()
    return Path(identity["common_dir"]) / _LIFECYCLE_REGISTRY_DIR / digest


def lifecycle_registry_path(
    lane_root: str | Path,
    lane_id: str,
    worker_invocation_id: str | None = None,
) -> Path:
    """Return the one lane-scoped controller admission coordinate.

    ``worker_invocation_id`` remains an ignored compatibility argument for
    callers from the preceding repair.  It must never participate in the
    coordinate: all legitimate workers for one canonical Git lane contend on
    this exact owner file.
    """

    if worker_invocation_id is not None and (
        not isinstance(worker_invocation_id, str) or not worker_invocation_id.strip()
    ):
        raise LaneLifecycleError("worker invocation identity is invalid")
    return lifecycle_registry_root(lane_root, lane_id) / "LIFECYCLE.json"


def _registry_digest(value: Mapping[str, Any]) -> str:
    normalized = dict(value)
    normalized["record_sha256"] = ""
    return hashlib.sha256(
        (
            json.dumps(normalized, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
        ).encode("utf-8")
    ).hexdigest()


def _registry_process_identity(value: object, *, name: str) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, ProcessInfo):
        if value.created_utc is None:
            raise LaneLifecycleError(f"{name} has no creation identity")
        return {"pid": value.pid, "created_utc": iso_utc(value.created_utc)}
    if isinstance(value, Mapping):
        if set(value) != {"pid", "created_utc"}:
            raise LaneLifecycleError(f"{name} identity shape is not closed")
        pid = value.get("pid")
        created = parse_utc(value.get("created_utc"))
        if (
            not isinstance(pid, int)
            or isinstance(pid, bool)
            or pid <= 0
            or created is None
        ):
            raise LaneLifecycleError(f"{name} identity is invalid")
        return {"pid": pid, "created_utc": iso_utc(created)}
    raise LaneLifecycleError(f"{name} identity is unsupported")


@dataclass
class _LifecycleAdmission:
    path: Path
    record: dict[str, Any]
    record_bytes: bytes
    coordinate: dict[str, Any]
    generation: str
    worker_invocation_id: str


def _record_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _record_with_digest(value: dict[str, Any]) -> tuple[dict[str, Any], bytes]:
    record = dict(value)
    record["record_sha256"] = ""
    record["record_sha256"] = _registry_digest(record)
    return record, _record_bytes(record)


def _process_boundary_record(boundary: Mapping[str, Any] | None) -> dict[str, Any]:
    if boundary is None:
        return {
            "schema": "orchestrator-process-boundary/v1",
            "kind": "unsupported",
            "identity": None,
            "complete": False,
            "inventory_source": "controller",
            "errors": ["controller process boundary has not been established"],
            "cleanup": None,
            "root_pid": None,
            "group_id": None,
            "session_id": None,
            "members": [],
            "live_members": [],
        }
    value = dict(boundary)
    required = {
        "schema",
        "kind",
        "identity",
        "complete",
        "inventory_source",
        "errors",
        "cleanup",
        "root_pid",
        "group_id",
        "session_id",
        "members",
        "live_members",
    }
    if (
        set(value) != required
        or value.get("schema") != "orchestrator-process-boundary/v1"
    ):
        raise LaneLifecycleError("controller process boundary evidence is not closed")
    if (
        value.get("complete") is not True
        or not isinstance(value.get("kind"), str)
        or not value.get("kind")
        or not isinstance(value.get("inventory_source"), str)
        or not value.get("inventory_source")
    ):
        raise LaneLifecycleError("controller process boundary is incomplete")
    if not isinstance(value.get("members"), list) or not isinstance(
        value.get("live_members"), list
    ):
        raise LaneLifecycleError("controller process boundary members are invalid")
    return value


def _helper_records(helpers: Sequence[object]) -> list[dict[str, Any]]:
    helper_records: list[dict[str, Any]] = []
    helper_names: set[str] = set()
    for index, item in enumerate(helpers):
        name = f"helper-{index}"
        identity = item
        if isinstance(item, Mapping) and "identity" in item:
            raw_name = item.get("name")
            if not isinstance(raw_name, str) or not raw_name.strip():
                raise LaneLifecycleError("helper name is invalid")
            name = raw_name.strip()
            identity = item.get("identity")
        if name in helper_names:
            raise LaneLifecycleError("owned helper collection is ambiguous")
        helper_names.add(name)
        normalized = _registry_process_identity(identity, name=name)
        if normalized is None:
            raise LaneLifecycleError("owned helper identity is missing")
        helper_records.append({"name": name, **normalized})
    helper_records.sort(key=lambda item: item["name"])
    return helper_records


def _build_lifecycle_record(
    coordinate: Mapping[str, Any],
    *,
    lane_id: str,
    run_root: str | Path,
    invocation_path: str | Path,
    status_path: str | Path,
    invocation_schema: str | None,
    worker_invocation_id: str,
    generation: str,
    state: str,
    repository: Mapping[str, Any],
    controller: object,
    worker: object = None,
    helpers: Sequence[object] = (),
    boundary: Mapping[str, Any] | None = None,
    retained_ref: str | None = None,
    target_revision: str | None = None,
) -> tuple[dict[str, Any], bytes]:
    lane = _registry_lane_id(lane_id)
    run = _regular_directory(run_root)
    invocation, invocation_bytes = _regular_file(invocation_path, name="invocation")
    status_value = _lexical(status_path)
    _reject_reparse_chain(status_value)
    status_bytes: bytes | None = None
    status: Mapping[str, Any] | None = None
    if os.path.lexists(status_value):
        status_path_checked, status_bytes = _regular_file(status_value, name="status")
        del status_path_checked
        try:
            parsed_status = json.loads(status_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LaneLifecycleError("controller status is not valid JSON") from exc
        if not isinstance(parsed_status, Mapping):
            raise LaneLifecycleError("controller status must be an object")
        status = parsed_status
    if not isinstance(invocation_schema, (str, type(None))):
        raise LaneLifecycleError("invocation schema is invalid")
    if not isinstance(worker_invocation_id, str) or not worker_invocation_id.strip():
        raise LaneLifecycleError("worker invocation identity is invalid")
    if not isinstance(generation, str) or not generation.strip():
        raise LaneLifecycleError("lifecycle registry generation is invalid")
    required_repository = {"worktree_root", "common_dir", "branch", "expected_head"}
    if not required_repository.issubset(repository):
        raise LaneLifecycleError("controller repository binding is incomplete")
    expected_head = repository.get("expected_head")
    if (
        not isinstance(expected_head, str)
        or _HEX_COMMIT.fullmatch(expected_head) is None
    ):
        raise LaneLifecycleError("controller expected HEAD is invalid")
    branch = repository.get("branch")
    common_dir = repository.get("common_dir")
    worktree_root = repository.get("worktree_root")
    if not all(
        isinstance(item, str) and item for item in (branch, common_dir, worktree_root)
    ):
        raise LaneLifecycleError("controller Git identity is incomplete")
    retained = retained_ref or repository.get("retained_ref") or "HEAD"
    target = target_revision or repository.get("target_revision") or expected_head
    if not isinstance(retained, str) or not isinstance(target, str):
        raise LaneLifecycleError("controller retirement claims are incomplete")
    controller_identity = _registry_process_identity(controller, name="controller")
    worker_identity = _registry_process_identity(worker, name="worker")
    helper_records = _helper_records(helpers)
    status_identity = {
        "schema": status.get("schema") if status is not None else None,
        "lane_id": status.get("lane_id") if status is not None else None,
        "worker_invocation_id": status.get("worker_invocation_id")
        if status is not None
        else None,
        "state": status.get("state") if status is not None else None,
        "generation": status.get("lifecycle_registry_generation")
        if status is not None
        else None,
    }
    complete_states = {"CODEX_EXITED", "PROVIDER_EXITED"}
    boundary_record = _process_boundary_record(boundary)
    complete = (
        worker_identity is not None
        and state in complete_states
        and status_bytes is not None
        and status_identity["schema"] == "orchestrator-lane-controller/v1"
        and status_identity["lane_id"] == lane
        and status_identity["worker_invocation_id"] == worker_invocation_id
        and status_identity["state"] == state
        and status_identity["generation"] == generation
        and boundary_record["complete"] is True
        and not boundary_record.get("live_members")
    )
    record: dict[str, Any] = {
        "schema": LIFECYCLE_REGISTRY_SCHEMA,
        "record_version": 1,
        "record_sha256": "",
        "authority": "controller-admitted-canonical-coordinate",
        "coordinate": dict(coordinate),
        "run": {
            "run_root": str(run),
            "lane_id": lane,
            "worker_invocation_id": worker_invocation_id,
            "invocation_schema": invocation_schema,
            "invocation_path": str(invocation),
            "invocation_sha256": hashlib.sha256(invocation_bytes).hexdigest(),
            "status_path": str(status_value),
            "status_sha256": hashlib.sha256(status_bytes).hexdigest()
            if status_bytes is not None
            else None,
            "status_identity": status_identity,
            "generation": generation,
        },
        "repository": {
            "worktree_root": str(_lexical(worktree_root)),
            "common_dir": str(_lexical(common_dir)),
            "branch": branch,
            "expected_head": expected_head.lower(),
            "starting_head": str(
                repository.get("starting_head") or expected_head
            ).lower(),
            "retained_ref": retained,
            "target_revision": target.lower() if isinstance(target, str) else target,
        },
        "identities": {
            "controller": controller_identity,
            "worker": worker_identity,
            "helpers": helper_records,
        },
        "boundary": boundary_record,
        "lifecycle": {
            "state": state,
            "complete": complete,
            "helpers_complete": complete,
        },
    }
    return _record_with_digest(record)


def _admit_lifecycle_registry(
    lane_root: str | Path,
    *,
    lane_id: str,
    run_root: str | Path,
    invocation_path: str | Path,
    status_path: str | Path,
    invocation_schema: str | None,
    worker_invocation_id: str,
    generation: str,
    state: str,
    repository: Mapping[str, Any],
    controller: object,
    worker: object = None,
    helpers: Sequence[object] = (),
    boundary: Mapping[str, Any] | None = None,
    retained_ref: str | None = None,
    target_revision: str | None = None,
    resume: bool = False,
) -> _LifecycleAdmission:
    """Atomically claim or extend the one controller admission for a Git lane."""

    coordinate = _canonical_registry_identity(lane_root, lane_id)
    if (
        coordinate["worktree_root"]
        != str(_lexical(repository.get("worktree_root", "")))
        or coordinate["common_dir"] != str(_lexical(repository.get("common_dir", "")))
        or coordinate["branch"] != repository.get("branch")
    ):
        raise LaneLifecycleError(
            "controller Git identity does not match canonical admission"
        )
    if not isinstance(worker_invocation_id, str) or not worker_invocation_id.strip():
        raise LaneLifecycleError("worker invocation identity is invalid")
    coordinate = {
        **coordinate,
        "coordinate_hash": hashlib.sha256(
            _registry_coordinate_seed(coordinate).encode("utf-8")
        ).hexdigest(),
    }
    path = lifecycle_registry_path(lane_root, lane_id)
    if not resume:
        record, data = _build_lifecycle_record(
            coordinate,
            lane_id=lane_id,
            run_root=run_root,
            invocation_path=invocation_path,
            status_path=status_path,
            invocation_schema=invocation_schema,
            worker_invocation_id=worker_invocation_id,
            generation=generation,
            state=state,
            repository=repository,
            controller=controller,
            worker=worker,
            helpers=helpers,
            boundary=boundary,
            retained_ref=retained_ref,
            target_revision=target_revision,
        )
        try:
            ensure_directory_path(path.parent)
            mutation_replace(
                path.parent, path.name, data, expected=TargetState.absent()
            )
        except (MutationConflict, MutationUnsupported) as exc:
            raise LaneLifecycleError(str(exc)) from exc
        return _LifecycleAdmission(
            path, record, data, dict(coordinate), generation, worker_invocation_id
        )

    if not path.is_file() or _is_reparse(path):
        raise LaneLifecycleError("resume lifecycle admission is missing")
    try:
        existing_bytes = path.read_bytes()
    except OSError as exc:
        raise LaneLifecycleError("resume lifecycle admission cannot be read") from exc
    try:
        existing = json.loads(existing_bytes.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LaneLifecycleError("resume lifecycle admission is malformed") from exc
    if (
        not isinstance(existing, dict)
        or existing.get("authority") != "controller-admitted-canonical-coordinate"
        or existing.get("coordinate") != coordinate
    ):
        raise LaneLifecycleError("resume lifecycle admission is foreign")
    if existing.get("record_sha256") != _registry_digest(
        existing
    ) or existing_bytes != _record_bytes(existing):
        raise LaneLifecycleError("resume lifecycle admission integrity is invalid")
    run_record = existing.get("run")
    if not isinstance(run_record, Mapping):
        raise LaneLifecycleError("resume lifecycle admission run binding is invalid")
    if run_record.get("worker_invocation_id") != worker_invocation_id:
        raise LaneLifecycleError("resume lifecycle admission worker identity mismatch")
    # Validate the admitted source bindings before allowing a continuation to
    # publish a new status.  A legitimate resume may use a new invocation and
    # status path; those are compare-and-swap updated into this same owner
    # after the resumed status exists.
    old_invocation_path = run_record.get("invocation_path")
    if not isinstance(old_invocation_path, str):
        raise LaneLifecycleError("resume lifecycle invocation binding is incomplete")
    try:
        _old_invocation, old_invocation_bytes = _regular_file(
            old_invocation_path, name="admitted invocation"
        )
    except (LaneLifecycleError, ArchiveFailed) as exc:
        raise LaneLifecycleError(str(exc)) from exc
    if (
        run_record.get("invocation_sha256")
        != hashlib.sha256(old_invocation_bytes).hexdigest()
    ):
        raise LaneLifecycleError("admitted invocation bytes changed")
    old_status_path = run_record.get("status_path")
    if not isinstance(old_status_path, str):
        raise LaneLifecycleError("resume lifecycle status binding is incomplete")
    old_status = _lexical(old_status_path)
    _reject_reparse_chain(old_status)
    if run_record.get("status_sha256") is not None:
        try:
            _, old_status_bytes = _regular_file(old_status, name="admitted status")
        except (LaneLifecycleError, ArchiveFailed) as exc:
            raise LaneLifecycleError(str(exc)) from exc
        if (
            run_record.get("status_sha256")
            != hashlib.sha256(old_status_bytes).hexdigest()
        ):
            raise LaneLifecycleError("admitted status bytes changed")
    existing_generation = run_record.get("generation")
    if not isinstance(existing_generation, str) or not existing_generation:
        raise LaneLifecycleError("resume lifecycle admission generation is missing")
    return _LifecycleAdmission(
        path,
        existing,
        existing_bytes,
        dict(coordinate),
        existing_generation,
        worker_invocation_id,
    )


def _update_lifecycle_registry(
    admission: _LifecycleAdmission,
    *,
    lane_id: str,
    run_root: str | Path,
    invocation_path: str | Path,
    status_path: str | Path,
    invocation_schema: str | None,
    worker_invocation_id: str,
    generation: str,
    state: str,
    repository: Mapping[str, Any],
    controller: object,
    worker: object = None,
    helpers: Sequence[object] = (),
    boundary: Mapping[str, Any] | None = None,
    retained_ref: str | None = None,
    target_revision: str | None = None,
) -> _LifecycleAdmission:
    if (
        worker_invocation_id != admission.worker_invocation_id
        or generation != admission.generation
    ):
        raise LaneLifecycleError(
            "controller lifecycle update is outside its admitted generation"
        )
    current = admission.path.read_bytes()
    if current != admission.record_bytes:
        raise LaneLifecycleError(
            "controller lifecycle admission changed outside its owner"
        )
    record, data = _build_lifecycle_record(
        admission.coordinate,
        lane_id=lane_id,
        run_root=run_root,
        invocation_path=invocation_path,
        status_path=status_path,
        invocation_schema=invocation_schema,
        worker_invocation_id=worker_invocation_id,
        generation=generation,
        state=state,
        repository=repository,
        controller=controller,
        worker=worker,
        helpers=helpers,
        boundary=boundary,
        retained_ref=retained_ref,
        target_revision=target_revision,
    )
    try:
        expected = capture_target(admission.path.parent, admission.path.name)
        if (
            expected.content_sha256
            != hashlib.sha256(admission.record_bytes).hexdigest()
        ):
            raise MutationConflict("controller lifecycle admission bytes changed")
        mutation_replace(
            admission.path.parent, admission.path.name, data, expected=expected
        )
    except (MutationConflict, MutationUnsupported) as exc:
        raise LaneLifecycleError(str(exc)) from exc
    return _LifecycleAdmission(
        admission.path,
        record,
        data,
        admission.coordinate,
        admission.generation,
        admission.worker_invocation_id,
    )


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
        if (
            not self.read_only
            or not self.manifest_path.is_file()
            or not self.ready_path.is_file()
        ):
            raise ImmutableViewError("immutable source view is not admitted ready")
        try:
            manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
            ready = json.loads(self.ready_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ImmutableViewError(
                "immutable readiness evidence is unreadable"
            ) from exc
        if not isinstance(manifest, dict) or manifest.get("ready") is not True:
            raise ImmutableViewError("immutable manifest is not finally ready")
        if not isinstance(ready, dict) or ready.get("ready") is not True:
            raise ImmutableViewError("immutable READY record is not final")
        if ready.get("retained_commit") != self.retained_commit or ready.get(
            "manifest_sha256"
        ) != _sha256(self.manifest_path):
            raise ImmutableViewError("READY does not bind the final immutable manifest")
        for relative, digest in dict(
            self.member_hashes or manifest.get("member_sha256", {})
        ).items():
            path = self.view_root / relative
            if not path.is_file() or _is_reparse(path) or _sha256(path) != digest:
                raise ImmutableViewError(
                    "immutable source bytes changed after admission"
                )
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
    try:
        ensure_directory_path(view)
    except (MutationConflict, MutationUnsupported) as exc:
        raise ImmutableViewError(str(exc)) from exc
    view_identity = (view.stat().st_dev, view.stat().st_ino)
    admitted = False
    manifest_receipt: MutationReceipt | None = None
    ready_receipt: MutationReceipt | None = None
    try:
        _assert_retained(source, commit, retained_ref, retained_commit)
        archive = _git(source, "archive", "--format=tar", commit, check=True)
        member_hashes: dict[str, str] = {}
        with tarfile.open(fileobj=io.BytesIO(archive.stdout), mode="r:") as tar:
            members = tar.getmembers()
            if any(
                not _safe_member(item.name)
                or item.issym()
                or item.islnk()
                or item.isdev()
                or item.isfifo()
                for item in members
            ):
                raise ImmutableViewError("source archive contains an unsafe member")
            for member in members:
                destination = view / member.name
                if not _inside(destination.absolute(), view.absolute()):
                    raise ImmutableViewError("source archive member escapes the view")
                if member.isdir():
                    try:
                        ensure_directory_path(destination)
                    except (MutationConflict, MutationUnsupported) as exc:
                        raise ImmutableViewError(str(exc)) from exc
                    continue
                handle = tar.extractfile(member)
                if handle is None:
                    raise ImmutableViewError(
                        f"source archive member cannot be read: {member.name}"
                    )
                try:
                    ensure_directory_path(destination.parent)
                except (MutationConflict, MutationUnsupported) as exc:
                    raise ImmutableViewError(str(exc)) from exc
                data = handle.read()
                try:
                    mutation_replace(
                        view,
                        Path(member.name),
                        data,
                        expected=capture_target(view, Path(member.name)),
                    )
                except (MutationConflict, MutationUnsupported) as exc:
                    raise ImmutableViewError(str(exc)) from exc
                member_hashes[member.name] = hashlib.sha256(data).hexdigest()
        _set_read_only(view)
        _assert_retained(source, commit, retained_ref, retained_commit)
        record = {
            "schema": IMMUTABLE_VIEW_SCHEMA,
            "view_id": view_id,
            "source_root": str(source),
            "view_root": str(view),
            "result_root": str(result),
            "cache_root": str(cache),
            "retained_commit": commit,
            "retained_ref": retained_ref,
            "created_utc": iso_utc(utc_now()),
            "read_only_boundary": {
                "mechanism": "filesystem-read-only-plus-hash-admission-and-write-api-refusal",
                "source_output_forbidden": True,
                "result_root_writable": True,
                "cache_root_writable": True,
            },
            "member_sha256": dict(sorted(member_hashes.items())),
            "ready": True,
        }
        # The manifest is final before READY is published.  READY binds the
        # exact bytes of this final ready:true document.
        manifest_receipt = _atomic_json(manifest, record)
        _assert_retained(source, commit, retained_ref, retained_commit)
        manifest_hash = _sha256(manifest)
        ready_receipt = _atomic_json(
            ready,
            {
                "schema": IMMUTABLE_VIEW_SCHEMA,
                "view_id": view_id,
                "retained_commit": commit,
                "retained_ref": retained_ref,
                "manifest_sha256": manifest_hash,
                "admitted_utc": iso_utc(utc_now()),
                "ready": True,
            },
        )
        if _sha256(manifest) != manifest_hash:
            raise ImmutableViewError(
                "immutable manifest changed before READY publication"
            )
        admitted = True
        return ImmutableSourceView(
            view_id,
            source,
            view,
            result,
            cache,
            commit,
            manifest,
            ready,
            True,
            member_hashes,
        )
    except Exception:
        try:
            if ready_receipt is not None:
                mutation_delete(result, ready.name, expected=ready_receipt.resulting)
            else:
                mutation_delete(result, ready.name, expected=TargetState.absent())
        except (MutationConflict, MutationUnsupported):
            pass
        try:
            if manifest_receipt is not None:
                mutation_delete(
                    result, manifest.name, expected=manifest_receipt.resulting
                )
            else:
                mutation_delete(result, manifest.name, expected=TargetState.absent())
        except (MutationConflict, MutationUnsupported):
            pass
        if not admitted:
            try:
                if (
                    view.exists()
                    and not _is_reparse(view)
                    and (view.stat().st_dev, view.stat().st_ino) == view_identity
                ):
                    children = list(view.rglob("*"))
                    for child in children:
                        if _is_reparse(child):
                            raise ImmutableViewError(
                                "refusing cleanup through a source-view link"
                            )
                    for child in children:
                        try:
                            child.chmod(
                                stat.S_IWRITE
                                | stat.S_IREAD
                                | (stat.S_IEXEC if child.is_dir() else 0)
                            )
                        except OSError:
                            pass
                    view.chmod(stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
                    remove_tree(
                        view.parent,
                        view.name,
                        expected=TargetState(
                            True, "directory", view_identity, None, None
                        ),
                    )
            except (OSError, MutationConflict, MutationUnsupported):
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
        "name": name,
        "present": True,
        "member": None,
        "sha256": digest,
        "size": len(data),
        "source_identity": {"path": str(path), "sha256": digest},
    }, data


def _load_canonical_lifecycle_registry(
    lane: Path,
    *,
    lane_id: str,
) -> tuple[
    dict[str, Any],
    bytes,
    dict[str, dict[str, Any]],
    dict[str, Any],
    bytes,
    dict[str, Any],
    Path,
]:
    """Load the unique admission derived from the target's live Git identity."""

    try:
        root = lifecycle_registry_root(lane, lane_id)
    except LaneLifecycleError as exc:
        raise ArchiveFailed(str(exc)) from exc
    # A normal non-worktree repository legitimately has its canonical Git
    # common directory under the worktree's ``.git`` directory.  The
    # coordinate is still derived from live Git identity; only indirections
    # and missing roots are unsafe here.
    if _is_reparse(root) or not root.is_dir():
        raise ArchiveFailed("canonical lifecycle registry root is missing or unsafe")
    registry_path = lifecycle_registry_path(lane, lane_id)
    if not registry_path.is_file() or _is_reparse(registry_path):
        raise ArchiveFailed("canonical lifecycle admission is missing or ambiguous")
    reference, data = _source_reference(registry_path, name="lifecycle_registry")
    try:
        raw = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArchiveFailed("canonical lifecycle registry record is malformed") from exc
    if not isinstance(raw, dict):
        raise ArchiveFailed("canonical lifecycle registry record is not an object")
    required = {
        "schema",
        "record_version",
        "record_sha256",
        "authority",
        "coordinate",
        "run",
        "repository",
        "identities",
        "boundary",
        "lifecycle",
    }
    if (
        set(raw) != required
        or raw.get("schema") != LIFECYCLE_REGISTRY_SCHEMA
        or raw.get("record_version") != 1
        or raw.get("authority") != "controller-admitted-canonical-coordinate"
    ):
        raise ArchiveFailed("canonical lifecycle registry record shape is invalid")
    if raw.get("record_sha256") != _registry_digest(raw):
        raise ArchiveFailed("canonical lifecycle registry record was modified")
    coordinate = raw.get("coordinate")
    run_record = raw.get("run")
    repository = raw.get("repository")
    identities_record = raw.get("identities")
    boundary = raw.get("boundary")
    lifecycle = raw.get("lifecycle")
    if not all(
        isinstance(item, Mapping)
        for item in (
            coordinate,
            run_record,
            repository,
            identities_record,
            boundary,
            lifecycle,
        )
    ):
        raise ArchiveFailed(
            "canonical lifecycle registry record has incomplete sections"
        )
    assert isinstance(coordinate, Mapping)
    assert isinstance(run_record, Mapping)
    assert isinstance(repository, Mapping)
    assert isinstance(identities_record, Mapping)
    assert isinstance(lifecycle, Mapping)
    try:
        actual_coordinate = _canonical_registry_identity(lane, lane_id)
    except LaneLifecycleError as exc:
        raise ArchiveFailed(str(exc)) from exc
    worker_id = run_record.get("worker_invocation_id")
    if not isinstance(worker_id, str) or not worker_id:
        raise ArchiveFailed("canonical lifecycle worker identity is incomplete")
    expected_coordinate = {
        **actual_coordinate,
        "coordinate_hash": hashlib.sha256(
            _registry_coordinate_seed(actual_coordinate).encode("utf-8")
        ).hexdigest(),
    }
    if dict(coordinate) != expected_coordinate or not _same_path(
        registry_path, lifecycle_registry_path(lane, lane_id)
    ):
        raise ArchiveFailed("canonical lifecycle coordinate is foreign")
    if run_record.get("lane_id") != lane_id or run_record.get("run_root") != str(lane):
        raise ArchiveFailed("canonical lifecycle run binding is foreign")
    if (
        run_record.get("status_path") is None
        or run_record.get("invocation_path") is None
        or not isinstance(run_record.get("generation"), str)
        or not run_record.get("generation")
    ):
        raise ArchiveFailed("canonical lifecycle source binding is incomplete")
    status_path = _lexical(run_record["status_path"])
    if (
        not _inside(status_path, lane)
        or status_path.parent != lane / ".agent-workspace"
    ):
        raise ArchiveFailed("canonical lifecycle status binding is foreign")
    invocation_path = _lexical(run_record["invocation_path"])
    invocation, invocation_bytes = _regular_file(invocation_path, name="invocation")
    if hashlib.sha256(invocation_bytes).hexdigest() != run_record.get(
        "invocation_sha256"
    ):
        raise ArchiveFailed("canonical lifecycle invocation bytes changed")
    status_path_checked, status_bytes = _regular_file(status_path, name="status")
    del status_path_checked
    if not isinstance(run_record.get("status_sha256"), str) or hashlib.sha256(
        status_bytes
    ).hexdigest() != run_record.get("status_sha256"):
        raise ArchiveFailed("canonical lifecycle status bytes changed")
    try:
        status = json.loads(status_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArchiveFailed("canonical lifecycle status is malformed") from exc
    if not isinstance(status, Mapping):
        raise ArchiveFailed("canonical lifecycle status is not an object")
    status_identity = run_record.get("status_identity")
    if (
        not isinstance(status_identity, Mapping)
        or any(
            status.get(key) != status_identity.get(key)
            for key in ("schema", "lane_id", "worker_invocation_id", "state")
        )
        or status_identity.get("generation") != run_record.get("generation")
        or status.get("lifecycle_registry_generation") != run_record.get("generation")
    ):
        raise ArchiveFailed(
            "canonical lifecycle status identity does not match admission"
        )
    if status.get("invocation_schema") != run_record.get("invocation_schema"):
        raise ArchiveFailed(
            "canonical lifecycle invocation schema does not match admission"
        )
    if (
        status.get("worktree_root") != repository.get("worktree_root")
        or status.get("repository_common_dir") != repository.get("common_dir")
        or status.get("branch") != repository.get("branch")
    ):
        raise ArchiveFailed(
            "canonical lifecycle status Git identity does not match admission"
        )
    expected_repository = {
        "worktree_root",
        "common_dir",
        "branch",
        "expected_head",
        "starting_head",
        "retained_ref",
        "target_revision",
    }
    if set(repository) != expected_repository or not all(
        isinstance(repository.get(key), str) and repository.get(key)
        for key in expected_repository
    ):
        raise ArchiveFailed("canonical lifecycle Git binding is not closed")
    binding = {
        "lane_id": lane_id,
        "worktree": repository["worktree_root"],
        "git_common_dir": repository["common_dir"],
        "branch": repository["branch"],
        "expected_head": repository["expected_head"],
        "retained_ref": repository["retained_ref"],
        "target_revision": repository["target_revision"],
    }
    if not isinstance(identities_record, Mapping):
        raise ArchiveFailed("canonical lifecycle identities are incomplete")
    controller = identities_record.get("controller")
    worker = identities_record.get("worker")
    helpers = identities_record.get("helpers")
    if (
        not isinstance(controller, Mapping)
        or not isinstance(worker, Mapping)
        or not isinstance(helpers, list)
    ):
        raise ArchiveFailed("canonical lifecycle process collection is incomplete")
    identity_map: dict[str, dict[str, Any]] = {}
    for role, item in (("controller", controller), ("worker", worker)):
        try:
            normalized = _registry_process_identity(item, name=role)
        except LaneLifecycleError as exc:
            raise ArchiveFailed(str(exc)) from exc
        if normalized is None:
            raise ArchiveFailed(f"canonical lifecycle {role} identity is missing")
        identity_map[role] = normalized
    helper_records: list[dict[str, Any]] = []
    names: set[str] = set()
    for item in helpers:
        if not isinstance(item, Mapping) or set(item) != {"name", "pid", "created_utc"}:
            raise ArchiveFailed("canonical lifecycle helper collection is ambiguous")
        name = item.get("name")
        if not isinstance(name, str) or not name or name in names:
            raise ArchiveFailed("canonical lifecycle helper collection is ambiguous")
        names.add(name)
        normalized = _registry_process_identity(
            {"pid": item.get("pid"), "created_utc": item.get("created_utc")}, name=name
        )
        if normalized is None:
            raise ArchiveFailed("canonical lifecycle helper identity is missing")
        identity_map[f"helper:{name}"] = normalized
        helper_records.append({"name": name, **normalized})
    try:
        boundary_record = _process_boundary_record(dict(boundary))
    except LaneLifecycleError as exc:
        raise ArchiveFailed(str(exc)) from exc
    if (
        lifecycle.get("complete") is not True
        or lifecycle.get("helpers_complete") is not True
        or lifecycle.get("state") not in {"CODEX_EXITED", "PROVIDER_EXITED"}
    ):
        raise ArchiveFailed(
            "canonical lifecycle registry is incomplete or not terminal"
        )
    if boundary_record.get("live_members"):
        raise ArchiveFailed(
            "canonical lifecycle boundary still contains live owned processes"
        )
    process_bytes = _record_bytes(
        {
            "schema": PROCESS_EVIDENCE_SCHEMA,
            "complete": True,
            "provider": "controller-boundary",
            "identities": {
                "controller": identity_map["controller"],
                "worker": identity_map["worker"],
                "helpers": helper_records,
            },
            "boundary": boundary_record,
            "processes": boundary_record.get("members", []),
            "lifecycle_registry": {
                "path": str(registry_path),
                "sha256": reference["sha256"],
            },
        }
    )
    return (
        raw,
        data,
        identity_map,
        binding,
        process_bytes,
        boundary_record,
        registry_path,
    )


def _process_proof(
    snapshot: ProcessSnapshot,
    identities: Mapping[str, Mapping[str, Any]],
    boundary: Mapping[str, Any] | None = None,
) -> tuple[bool, str, dict[str, Any]]:
    if (
        not isinstance(snapshot, ProcessSnapshot)
        or not snapshot.complete
        or not snapshot.provider
        or snapshot.provider in {"unknown", "persisted"}
    ):
        return (
            False,
            "PROCESS_SNAPSHOT_INCOMPLETE",
            {
                "complete": bool(getattr(snapshot, "complete", False)),
                "provider": str(getattr(snapshot, "provider", "unknown")),
                "errors": list(getattr(snapshot, "errors", ())),
                "states": {},
            },
        )
    if len(snapshot.by_pid) != len(snapshot.processes):
        return (
            False,
            "PROCESS_SNAPSHOT_AMBIGUOUS",
            {"complete": True, "provider": snapshot.provider, "states": {}},
        )
    if (
        boundary is None
        or boundary.get("complete") is not True
        or not isinstance(boundary.get("kind"), str)
        or not boundary.get("kind")
        or not boundary.get("identity")
    ):
        return (
            False,
            "PROCESS_BOUNDARY_INCOMPLETE",
            {
                "complete": True,
                "provider": snapshot.provider,
                "states": {},
                "boundary": dict(boundary or {}),
            },
        )
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
    known_pids = {item["pid"] for item in identities.values()}
    owned_parent_pids = set(known_pids)
    group_id = boundary.get("group_id")
    session_id = boundary.get("session_id")
    boundary_id = boundary.get("identity")
    boundary_identity_keys: set[tuple[int, str]] = set()
    raw_members = boundary.get("members")
    if not isinstance(raw_members, list):
        return (
            False,
            "PROCESS_BOUNDARY_MEMBERS_INCOMPLETE",
            {
                "complete": True,
                "provider": snapshot.provider,
                "states": {},
                "boundary": dict(boundary),
            },
        )
    for item in raw_members:
        if (
            not isinstance(item, Mapping)
            or not isinstance(item.get("pid"), int)
            or not isinstance(item.get("created_utc"), str)
        ):
            return (
                False,
                "PROCESS_BOUNDARY_MEMBERS_AMBIGUOUS",
                {
                    "complete": True,
                    "provider": snapshot.provider,
                    "states": {},
                    "boundary": dict(boundary),
                },
            )
        boundary_identity_keys.add((item["pid"], item["created_utc"]))
    changed = True
    while changed:
        changed = False
        for item in snapshot.processes:
            if item.pid not in owned_parent_pids and (
                item.ppid in owned_parent_pids
                or item.boundary_id == boundary_id
                or (isinstance(group_id, int) and item.process_group_id == group_id)
                or (isinstance(session_id, int) and item.session_id == session_id)
                or (item.pid, iso_utc(item.created_utc) or "") in boundary_identity_keys
            ):
                owned_parent_pids.add(item.pid)
                changed = True
    for item in snapshot.processes:
        belongs = (
            item.boundary_id == boundary_id
            or (isinstance(group_id, int) and item.process_group_id == group_id)
            or (isinstance(session_id, int) and item.session_id == session_id)
            or (item.pid, iso_utc(item.created_utc) or "") in boundary_identity_keys
            or item.pid in owned_parent_pids
        )
        if belongs and item.pid not in known_pids:
            states[f"undeclared:{item.pid}"] = "UNDECLARED_LIVE"
    proof = {
        "complete": True,
        "provider": snapshot.provider,
        "states": states,
        "boundary": {
            "kind": boundary.get("kind"),
            "identity": boundary.get("identity"),
            "inventory_source": boundary.get("inventory_source"),
        },
    }
    if "AMBIGUOUS" in states.values():
        return False, "LIVE_USE_AMBIGUOUS", proof
    if "LIVE" in states.values() or "UNDECLARED_LIVE" in states.values():
        return False, "LIVE_USE_PROVEN", proof
    return True, "NO_LIVE_USE_PROVED", proof


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


def _visible_result(
    lane: Path, reason: str, *, archive: Path | None = None, revision: str | None = None
) -> RetirementResult:
    return RetirementResult(
        "VISIBLE", reason, lane, archive, retained_revision=revision, visible=True
    )


def _after_initial_retirement_proof() -> None:
    """Internal late seam before the final registry/Git/process proof."""


def _before_git_close() -> None:
    """Internal late seam immediately before the public Git close."""


def _same_path(left: str | Path, right: str | Path) -> bool:
    return os.path.normcase(str(_lexical(left))) == os.path.normcase(
        str(_lexical(right))
    )


def _registered_worktree_matches(lane: Path) -> bool:
    blocks = [
        block
        for block in _git_text(lane, "worktree", "list", "--porcelain").split("\n\n")
        if block.strip()
    ]
    matches = []
    for block in blocks:
        candidate = next(
            (
                line[len("worktree ") :].strip()
                for line in block.splitlines()
                if line.startswith("worktree ")
            ),
            None,
        )
        if candidate is not None and _same_path(candidate, lane):
            matches.append(candidate)
    return len(matches) == 1


def _common_directory(lane: Path) -> Path:
    raw = Path(_git_text(lane, "rev-parse", "--git-common-dir"))
    return _lexical(raw if raw.is_absolute() else lane / raw)


def _validate_lane_binding(
    lane: Path,
    *,
    lane_id: str,
    binding: Mapping[str, Any],
    retained_revision: str | None = None,
    retained_ref: str | None = None,
    target_revision: str | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    required = {
        "lane_id",
        "worktree",
        "git_common_dir",
        "branch",
        "expected_head",
        "retained_ref",
        "target_revision",
    }
    if set(binding) != required:
        return None, "LANE_BINDING_SHAPE_INVALID"
    if binding.get("lane_id") != lane_id or not _same_path(
        str(binding.get("worktree")), lane
    ):
        return None, "LANE_BINDING_FOREIGN_WORKTREE"
    if not isinstance(binding.get("branch"), str) or not isinstance(
        binding.get("git_common_dir"), str
    ):
        return None, "LANE_BINDING_IDENTITY_INVALID"
    try:
        actual_common = _common_directory(lane)
    except LaneLifecycleError as exc:
        return None, f"GIT_STATE_UNKNOWN:{type(exc).__name__}"
    if not _same_path(str(binding["git_common_dir"]), actual_common):
        return None, "LANE_BINDING_FOREIGN_COMMON_DIR"
    if (
        binding.get("retained_ref") != retained_ref
        or binding.get("target_revision") != target_revision
    ):
        return None, "LANE_BINDING_CLAIM_MISMATCH"
    if not isinstance(binding.get("expected_head"), str):
        return None, "LANE_BINDING_EXPECTED_HEAD_INVALID"
    try:
        expected_head = _exact_revision(
            lane, binding["expected_head"], name="binding expected_head"
        )
        target = _exact_revision(
            lane, binding["target_revision"], name="binding target_revision"
        )
        caller_retained = _exact_revision(
            lane, retained_revision, name="retained_revision claim"
        )
        caller_target = _exact_revision(
            lane, target_revision, name="target_revision claim"
        )
    except LaneLifecycleError as exc:
        return None, f"LANE_BINDING_REVISION_INVALID:{type(exc).__name__}"
    if (
        caller_retained.lower() != expected_head.lower()
        or caller_target.lower() != target.lower()
    ):
        return None, "LANE_BINDING_CLAIM_MISMATCH"
    branch = _git(lane, "symbolic-ref", "--quiet", "--short", "HEAD")
    if branch.returncode != 0 or not branch.stdout.strip():
        return None, "AMBIGUOUS_BRANCH"
    branch_name = branch.stdout.decode("utf-8", errors="replace").strip()
    if binding["branch"] != branch_name:
        return None, "LANE_BINDING_BRANCH_MISMATCH"
    if not _registered_worktree_matches(lane):
        return None, "LANE_BINDING_WORKTREE_NOT_REGISTERED"
    actual_head = _git_text(lane, "rev-parse", "--verify", "HEAD^{commit}").lower()
    state: dict[str, Any] = {
        "path": str(lane),
        "common_dir": str(actual_common),
        "branch": branch_name,
        "actual_head": actual_head,
        "expected_head": expected_head,
        "retained_ref": retained_ref,
        "target_revision": target,
    }
    if actual_head != expected_head.lower():
        return state, "ACTUAL_HEAD_CHANGED_AFTER_AUTHORIZATION"
    status = _git(lane, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    if status.returncode != 0:
        return state, "GIT_STATE_UNKNOWN:STATUS_FAILED"
    state["clean"] = not bool(status.stdout)
    if status.stdout:
        return state, "DIRTY_WORKTREE"
    try:
        retained_ref_commit = _git_text(
            lane,
            "rev-parse",
            "--verify",
            f"{_safe_ref(retained_ref, name='retained_ref')}^{{commit}}",
        ).lower()
    except LaneLifecycleError as exc:
        return state, f"RETAINED_REF_UNKNOWN:{type(exc).__name__}"
    state["retained_ref_commit"] = retained_ref_commit
    if (
        _git(lane, "merge-base", "--is-ancestor", actual_head, retained_ref).returncode
        != 0
    ):
        return state, "RETAINED_REVISION_UNPROVEN"
    if _git(lane, "merge-base", "--is-ancestor", actual_head, target).returncode != 0:
        return state, "UNMERGED_WORK_PRESENT"
    state["ancestry_verified"] = True
    return state, None


def _archive_digest(value: Mapping[str, Any]) -> str:
    normalized = dict(value)
    normalized["archive_content_sha256"] = ""
    return hashlib.sha256(
        (
            json.dumps(normalized, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
        ).encode("utf-8")
    ).hexdigest()


def _copy_member(stage: Path, name: str, data: bytes) -> dict[str, Any]:
    if not _safe_member(name):
        raise ArchiveFailed(f"unsafe archive member name: {name}")
    destination = stage / name
    try:
        ensure_directory_path(destination.parent)
        relative = destination.relative_to(stage)
        mutation_replace(
            stage,
            relative,
            data,
            expected=capture_target(stage, relative),
        )
    except (MutationConflict, MutationUnsupported) as exc:
        raise ArchiveFailed(str(exc)) from exc
    digest = hashlib.sha256(data).hexdigest()
    if (
        not destination.is_file()
        or _sha256(destination) != digest
        or destination.stat().st_size != len(data)
    ):
        raise ArchiveFailed(f"archive member validation failed: {name}")
    return {"member": name, "sha256": digest, "size": len(data)}


def retire_terminal_lane(
    lane_root: str | Path,
    archive_root: str | Path,
    *,
    lane_id: str,
    task_ref: object = None,
    result_ref: object = None,
    findings_ref: object = None,
    acceptance_ref: object = None,
    transcript_ref: object = None,
    dependency_ref: object = None,
    discarded_cache: Sequence[object] = (),
    overlay_receipt: object = None,
) -> RetirementResult:
    """Copy and validate all evidence, then close only a mechanically safe lane."""

    try:
        lane = _regular_directory(lane_root)
        archive_base = _regular_directory(archive_root, create=True)
    except LaneLifecycleError as exc:
        return _visible_result(_lexical(lane_root), f"PATH_UNSAFE:{type(exc).__name__}")
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
    required = {
        "task": task_ref,
        "result": result_ref,
        "findings": findings_ref,
        "acceptance": acceptance_ref,
        "transcript": transcript_ref,
        "dependency": dependency_ref,
    }
    if any(value is None for value in required.values()):
        return _visible_result(lane, "ARCHIVE_EVIDENCE_INCOMPLETE")
    try:
        refs: dict[str, tuple[dict[str, Any], bytes]] = {
            name: _source_reference(value, name=name)
            for name, value in required.items()
        }
        (
            registry,
            registry_bytes,
            identities,
            binding,
            process_bytes,
            boundary,
            registry_path,
        ) = _load_canonical_lifecycle_registry(
            lane,
            lane_id=lane_id,
        )
        process_ref, _ = _source_reference(registry_path, name="process_evidence")
        retained_revision = binding["expected_head"]
        retained_ref = binding["retained_ref"]
        target_revision = binding["target_revision"]
    except ArchiveFailed as exc:
        return _visible_result(lane, f"ARCHIVE_EVIDENCE_INCOMPLETE:{str(exc)[:160]}")

    # Overlay restoration precedes the git-clean binding proof: preparation
    # leaves receipt-recorded untracked files in the worktree until
    # retirement.  The receipt must first prove it is completed and targets
    # this exact lane with role subagent; a foreign/orchestrator receipt is
    # rejected before any restoration so no worktree outside the lane's
    # ownership boundary can be mutated.  Restore only after that proof; any
    # later edit, missing/malformed receipt, or unsafe target keeps the lane
    # visible with its later work intact.
    overlay_restoration: dict[str, Any] | None = None
    if overlay_receipt is not None:
        try:
            overlay_verification = verify_overlay_receipt(
                receipt_path=_lexical(overlay_receipt),
                expected_target_worktree_id=lane,
                role="subagent",
            )
        except Exception as exc:
            overlay_verification = {
                "present": True,
                "verified": False,
                "reason": f"receipt verification failed: {type(exc).__name__}: {exc}",
            }
        if not overlay_verification.get("verified"):
            return _visible_result(
                lane,
                "OVERLAY_RESTORE_BLOCKED:"
                + str(overlay_verification.get("reason", "unknown")),
                revision=retained_revision,
            )
        try:
            overlay_restoration = restore_worktree(
                receipt_path=_lexical(overlay_receipt)
            )
        except Exception as exc:
            overlay_restoration = {
                "schema": OVERLAY_RECEIPT_SCHEMA,
                "receipt_path": str(_lexical(overlay_receipt)),
                "outcome": "BLOCKED",
                "reason": f"restore failed: {type(exc).__name__}: {exc}",
                "restored_paths": [],
                "removed_paths": [],
                "preserved_paths": [],
                "left_directories": [],
            }
        if overlay_restoration.get("outcome") != "RESTORED":
            return _visible_result(
                lane,
                "OVERLAY_RESTORE_BLOCKED:"
                + str(overlay_restoration.get("reason", "unknown")),
                revision=retained_revision,
            )

    try:
        git_state, git_reason = _validate_lane_binding(
            lane,
            lane_id=lane_id,
            binding=binding,
            retained_revision=retained_revision,
            retained_ref=retained_ref,
            target_revision=target_revision,
        )
    except LaneLifecycleError as exc:
        return _visible_result(
            lane, f"GIT_STATE_UNKNOWN:{type(exc).__name__}", revision=retained_revision
        )
    actual_head = str((git_state or {}).get("actual_head") or retained_revision)
    if git_reason is not None:
        return _visible_result(lane, git_reason, revision=actual_head)

    try:
        fresh_process = process_snapshot()
    except Exception as exc:
        return _visible_result(
            lane, f"PROCESS_SNAPSHOT_UNKNOWN:{type(exc).__name__}", revision=actual_head
        )
    absent, live_reason, process_proof = _process_proof(
        fresh_process, identities, boundary
    )
    if not absent:
        return _visible_result(lane, live_reason, revision=actual_head)
    _after_initial_retirement_proof()

    archive_dir = archive_base / lane_id
    if os.path.lexists(archive_dir):
        return _visible_result(
            lane,
            "ARCHIVE_ALREADY_EXISTS",
            archive=archive_dir / "LANE_ARCHIVE.json",
            revision=actual_head,
        )
    try:
        staging = make_temporary_directory(archive_base, prefix=f".{lane_id}.staging-")
    except (MutationConflict, MutationUnsupported) as exc:
        return _visible_result(
            lane, f"ARCHIVE_FAILED:{type(exc).__name__}", revision=actual_head
        )
    archive_path = staging / "LANE_ARCHIVE.json"
    staging_state = capture_target(archive_base, staging.name)
    try:
        archive_refs: dict[str, dict[str, Any]] = {}
        for name, (reference, data) in refs.items():
            member = _copy_member(staging, f"evidence/{name}.json", data)
            archive_refs[name] = {**reference, **member}
        process_member = _copy_member(
            staging, "evidence/process_evidence.json", process_bytes
        )
        archive_refs["process_evidence"] = {**process_ref, **process_member}
        discarded = [
            dict(item) if isinstance(item, Mapping) else {"path": str(item)}
            for item in discarded_cache
        ]
        archive: dict[str, Any] = {
            "schema": LANE_ARCHIVE_SCHEMA,
            "lane_id": lane_id,
            "archived_utc": iso_utc(utc_now()),
            "task": archive_refs["task"],
            "result": archive_refs["result"],
            "findings": archive_refs["findings"],
            "acceptance": archive_refs["acceptance"],
            "transcript": archive_refs["transcript"],
            "dependency": archive_refs["dependency"],
            "process_evidence": archive_refs["process_evidence"],
            "references": archive_refs,
            "content_identities": {
                name: item["sha256"] for name, item in archive_refs.items()
            },
            "retained_revision": git_state["actual_head"],
            "retained_ref": git_state["retained_ref"],
            "retained_ref_commit": git_state["retained_ref_commit"],
            "target_revision": git_state["target_revision"],
            "worktree": {
                "path": git_state["path"],
                "common_dir": git_state["common_dir"],
                "clean": True,
                "branch": git_state["branch"],
                "actual_head": git_state["actual_head"],
            },
            "lane_binding": dict(binding),
            "lifecycle_registry": {
                "path": str(registry_path),
                "sha256": hashlib.sha256(registry_bytes).hexdigest(),
                "record": registry,
            },
            "no_live_process_proof": {**process_proof, "state": live_reason},
            "no_unmerged_work_proof": {
                "target_revision": git_state["target_revision"],
                "ancestry_verified": True,
            },
            "close_result": "PENDING",
            "discarded_cache_inventory": discarded,
            "archive_content_sha256": "",
        }
        archive["archive_content_sha256"] = _archive_digest(archive)
        _atomic_json(archive_path, archive)
        validate_lane_archive(archive_path)
        mutation_rename(
            archive_base,
            staging.name,
            archive_dir.name,
            expected_source=staging_state,
            expected_target=TargetState.absent(),
        )
        staging = archive_dir
        archive_path = archive_dir / "LANE_ARCHIVE.json"
        validate_lane_archive(archive_path)
    except Exception as exc:
        try:
            if (
                staging.parent == archive_base
                and staging.exists()
                and not _is_reparse(staging)
            ):
                remove_tree(archive_base, staging.name, expected=staging_state)
        except (OSError, MutationConflict, MutationUnsupported):
            pass
        return _visible_result(
            lane, f"ARCHIVE_FAILED:{type(exc).__name__}", revision=actual_head
        )

    if overlay_restoration is not None:
        try:
            archived = json.loads(archive_path.read_text(encoding="utf-8"))
            archived["overlay_restoration"] = overlay_restoration
            archived["archive_content_sha256"] = ""
            archived["archive_content_sha256"] = _archive_digest(archived)
            _atomic_json(archive_path, archived)
            validate_lane_archive(archive_path)
        except Exception as exc:
            return _visible_result(
                lane,
                f"OVERLAY_RECORD_FAILED:{type(exc).__name__}",
                archive=archive_path,
                revision=actual_head,
            )

    blocks = [
        block
        for block in _git_text(lane, "worktree", "list", "--porcelain").split("\n\n")
        if block.strip()
    ]
    admin_roots: list[Path] = []
    for block in blocks:
        candidate = next(
            (
                line[len("worktree ") :]
                for line in block.splitlines()
                if line.startswith("worktree ")
            ),
            None,
        )
        if candidate is not None and not _same_path(candidate, lane):
            admin_roots.append(_lexical(candidate))
    if len(admin_roots) != 1:
        return _visible_result(
            lane, "NO_GIT_ADMIN_WORKTREE", archive=archive_path, revision=actual_head
        )
    admin_root = admin_roots[0]

    try:
        _before_git_close()
        (
            final_registry,
            final_registry_bytes,
            final_identities,
            final_binding,
            _,
            final_boundary,
            final_registry_path,
        ) = _load_canonical_lifecycle_registry(
            lane,
            lane_id=lane_id,
        )
        if (
            hashlib.sha256(final_registry_bytes).hexdigest()
            != hashlib.sha256(registry_bytes).hexdigest()
        ):
            return _visible_result(
                lane,
                "PRE_CLOSE_LIFECYCLE_REGISTRY_CHANGED",
                archive=archive_path,
                revision=actual_head,
            )
        final_retained_revision = final_binding["expected_head"]
        final_retained_ref = final_binding["retained_ref"]
        final_target_revision = final_binding["target_revision"]
        final_state, final_git_reason = _validate_lane_binding(
            lane,
            lane_id=lane_id,
            binding=final_binding,
            retained_revision=final_retained_revision,
            retained_ref=final_retained_ref,
            target_revision=final_target_revision,
        )
        final_snapshot = process_snapshot()
        final_absent, final_live_reason, final_process_proof = _process_proof(
            final_snapshot, final_identities, final_boundary
        )
        if not _same_path(final_registry_path, registry_path):
            return _visible_result(
                lane,
                "PRE_CLOSE_LIFECYCLE_COORDINATE_CHANGED",
                archive=archive_path,
                revision=actual_head,
            )
    except Exception as exc:
        return _visible_result(
            lane,
            f"PRE_CLOSE_PROOF_UNKNOWN:{type(exc).__name__}",
            archive=archive_path,
            revision=actual_head,
        )
    final_revision = str((final_state or {}).get("actual_head") or actual_head)
    if final_git_reason is not None:
        return _visible_result(
            lane,
            f"PRE_CLOSE_{final_git_reason}",
            archive=archive_path,
            revision=final_revision,
        )
    if not final_absent:
        return _visible_result(
            lane,
            f"PRE_CLOSE_{final_live_reason}",
            archive=archive_path,
            revision=final_revision,
        )

    removal = _git(admin_root, "worktree", "remove", str(lane))
    if removal.returncode != 0:
        detail = removal.stderr.decode("utf-8", errors="replace").strip()[:300]
        return _visible_result(
            lane,
            f"ARCHIVE_SUCCEEDED_CLOSE_FAILED:{detail or 'git worktree remove failed'}",
            archive=archive_path,
            revision=final_revision,
        )
    archive_digest = _sha256(archive_path)
    try:
        closed = json.loads(archive_path.read_text(encoding="utf-8"))
        if not isinstance(closed, dict):
            raise ArchiveFailed("archive changed shape after Git close")
        closed["pre_close_proof"] = {
            "git": final_state,
            "process": {**final_process_proof, "state": final_live_reason},
        }
        closed["close_result"] = "CLOSED"
        closed["closed_utc"] = iso_utc(utc_now())
        closed["archive_content_sha256"] = ""
        closed["archive_content_sha256"] = _archive_digest(closed)
        _atomic_json(archive_path, closed)
        validate_lane_archive(archive_path)
    except Exception as exc:
        # Git close is already proven.  Preserve the self-contained PENDING
        # archive and report closed metadata uncertainty honestly.
        return RetirementResult(
            "CLOSED_UNCERTAIN",
            f"ARCHIVE_CLOSE_RESULT_FAILED:{type(exc).__name__}",
            lane,
            archive_path,
            archive_digest,
            final_revision,
            True,
            False,
        )
    return RetirementResult(
        "CLOSED",
        "ARCHIVED_AND_CLOSED",
        lane,
        archive_path,
        _sha256(archive_path),
        final_revision,
        True,
        False,
    )


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
    if not isinstance(refs, Mapping) or set(refs) != {
        "task",
        "result",
        "findings",
        "acceptance",
        "transcript",
        "dependency",
        "process_evidence",
    }:
        raise ArchiveFailed("lane archive evidence member set is incomplete")
    for name, reference in refs.items():
        if (
            not isinstance(reference, Mapping)
            or reference.get("present") is not True
            or not isinstance(reference.get("member"), str)
            or not _safe_member(reference["member"])
            or not isinstance(reference.get("sha256"), str)
            or not isinstance(reference.get("size"), int)
        ):
            raise ArchiveFailed(f"lane archive {name} member identity is incomplete")
        member = archive_path.parent / reference["member"]
        if (
            _is_reparse(member)
            or not member.is_file()
            or member.stat().st_size != reference["size"]
            or _sha256(member) != reference["sha256"]
        ):
            raise ArchiveFailed(
                f"lane archive {name} member bytes are unavailable or invalid"
            )
    if (
        value.get("retained_revision") is None
        or value.get("target_revision") is None
        or value.get("close_result") not in {"PENDING", "CLOSED"}
    ):
        raise ArchiveFailed("lane archive revision/close result is incomplete")
    return value


__all__ = [
    "ArchiveFailed",
    "IMMUTABLE_VIEW_SCHEMA",
    "ImmutableSourceView",
    "ImmutableViewError",
    "LANE_ARCHIVE_SCHEMA",
    "LIFECYCLE_REGISTRY_SCHEMA",
    "LaneLifecycleError",
    "RetirementBlocked",
    "RetirementResult",
    "allocate_immutable_source_view",
    "allocate_immutable_view",
    "allocate_source_view",
    "archive_and_retire_terminal_lane",
    "lifecycle_registry_path",
    "lifecycle_registry_root",
    "retire_lane",
    "retire_terminal_lane",
    "validate_lane_archive",
]
