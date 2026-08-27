"""Provider-neutral editable super-cache ingestion and worktree overlay lifecycle.

REQ-O40..REQ-O42: contents-only refresh of an ordinary editable
``super-cache`` folder, preflight-before-mutation worktree preparation with
create/merge/exact-append behavior, a minimal no-hash operation receipt, and
one shared exact-byte restoration function used by subagent lane retirement
and by the external owner of an orchestrator worktree.

The cache and its optional ``.super-cache.json`` control declaration are
ordinary mutable data.  They are never locked, frozen, hashed, snapshotted, or
used as invalidation keys.  A later re-ingest or direct cache edit affects only
later preparations and never invalidates an already prepared worktree.  The
harness does not create or launch an orchestrator and owns no worktree manager.
"""

from __future__ import annotations

import base64
import json
import os
import stat
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any

from .models import iso_utc, utc_now
from .mutation import (
    MutationError,
    TargetState,
    capture_target,
    ensure_directory_path,
    make_temporary_directory,
    remove_tree,
    safe_relative_path,
)
from .mutation import (
    append_bytes as mutation_append_bytes,
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

OVERLAY_RECEIPT_SCHEMA = "orchestrator-workspace-overlay-receipt/v1"
SUPER_CACHE_CONTROL_SCHEMA = "orchestrator-super-cache-control/v1"
SUPER_CACHE_NAME = "super-cache"
DECLARATION_NAME = ".super-cache.json"
SUPPORTED_ROLES = frozenset({"orchestrator", "subagent"})
_APPLIED_OPERATIONS = frozenset({"create_dir", "merge_dir", "create_file", "append"})
WORKSPACE_RULES_BEGIN = "<!-- BEGIN ORCHESTRATOR-HARNESS QUICK RULES -->"
WORKSPACE_RULES_END = "<!-- END ORCHESTRATOR-HARNESS QUICK RULES -->"


class WorkspaceOverlayError(ValueError):
    """A workspace overlay operation failed closed before or during mutation."""


class OverlayCollisionError(WorkspaceOverlayError):
    """Preparation rejected an existing-file collision without mutating."""


def _workspace_rules_block() -> bytes:
    """Return the packaged full rules block for a manager workspace."""

    try:
        rules = files("orchestrator_harness.assets.rules").joinpath(
            "quick_rules.md"
        ).read_bytes()
    except (FileNotFoundError, ModuleNotFoundError) as exc:
        raise WorkspaceOverlayError("packaged workspace rules are unavailable") from exc
    return (
        f"{WORKSPACE_RULES_BEGIN}\n".encode()
        + rules.rstrip(b"\r\n")
        + f"\n{WORKSPACE_RULES_END}\n".encode()
    )


def install_workspace_rules(*, workspace: str | Path) -> dict[str, Any]:
    """Append the packaged manager rules to ``AGENTS.md`` exactly once."""

    root = _regular_directory(workspace, name="workspace")
    agents = root / "AGENTS.md"
    if _is_reparse(agents) or (agents.exists() and not agents.is_file()):
        raise WorkspaceOverlayError("workspace AGENTS.md must be a regular file")
    try:
        existing = agents.read_bytes() if agents.exists() else b""
        existing.decode("utf-8", errors="strict")
    except (OSError, UnicodeDecodeError) as exc:
        raise WorkspaceOverlayError("workspace AGENTS.md must be readable UTF-8") from exc
    block = _workspace_rules_block()
    begin = WORKSPACE_RULES_BEGIN.encode()
    end = WORKSPACE_RULES_END.encode()
    begin_count = existing.count(begin)
    end_count = existing.count(end)
    if begin_count or end_count:
        if begin_count != 1 or end_count != 1 or block not in existing:
            raise WorkspaceOverlayError(
                "workspace AGENTS.md contains a modified or incomplete managed rules block"
            )
        return {
            "schema": "orchestrator-workspace-rules-install/v1",
            "workspace": str(root),
            "agents_path": str(agents),
            "installed": True,
            "idempotent": True,
        }
    separator = b"" if not existing or existing.endswith((b"\n", b"\r")) else b"\n"
    payload = existing + separator + (b"\n" if existing else b"") + block
    try:
        mutation_replace(
            root,
            "AGENTS.md",
            payload,
            expected=capture_target(root, "AGENTS.md"),
        )
    except (MutationError, OSError) as exc:
        raise WorkspaceOverlayError(f"could not install workspace rules: {exc}") from exc
    return {
        "schema": "orchestrator-workspace-rules-install/v1",
        "workspace": str(root),
        "agents_path": str(agents),
        "installed": True,
        "idempotent": False,
    }


def _lexical(value: str | Path) -> Path:
    return Path(os.path.abspath(str(Path(value).expanduser())))


def _is_reparse(path: Path) -> bool:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    if stat.S_ISLNK(info.st_mode):
        return True
    attributes = getattr(info, "st_file_attributes", 0)
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _regular_directory(value: str | Path, *, name: str) -> Path:
    path = _lexical(value)
    if _is_reparse(path) or not path.is_dir():
        raise WorkspaceOverlayError(
            f"{name} must be an existing regular directory: {path}"
        )
    return path


def _path_identity(path: Path) -> str:
    return os.path.normcase(os.path.abspath(str(path)))


def _walk_contents(root: Path) -> list[tuple[str, str]]:
    """Return sorted ``(relative_posix, kind)`` entries under ``root``.

    Reparse points, symlinks, and non-regular files are rejected so the
    refresh/prepare plan is complete and mechanical.
    """
    entries: list[tuple[str, str]] = []

    def walk(directory: Path) -> None:
        try:
            children = sorted(os.scandir(directory), key=lambda item: item.name)
        except OSError as exc:
            raise WorkspaceOverlayError(
                f"cannot read overlay source {directory}: {exc}"
            ) from exc
        for child in children:
            child_path = directory / child.name
            if _is_reparse(child_path):
                raise WorkspaceOverlayError(
                    f"overlay source contains a reparse point: {child_path}"
                )
            try:
                info = child_path.lstat()
            except OSError as exc:
                raise WorkspaceOverlayError(
                    f"cannot inspect overlay source {child_path}: {exc}"
                ) from exc
            relative = child_path.relative_to(root).as_posix()
            if stat.S_ISDIR(info.st_mode):
                entries.append((relative, "dir"))
                walk(child_path)
            elif stat.S_ISREG(info.st_mode):
                entries.append((relative, "file"))
            else:
                raise WorkspaceOverlayError(
                    f"overlay source contains a non-regular file: {child_path}"
                )

    walk(root)
    return sorted(entries)


def _verify_contents_match(source: Path, mirror: Path) -> None:
    source_entries = _walk_contents(source)
    mirror_entries = _walk_contents(mirror)
    if [kind for _, kind in source_entries] != [kind for _, kind in mirror_entries]:
        raise WorkspaceOverlayError(
            "refreshed cache does not match the supplied folder structure"
        )
    for (relative, kind), (mirror_relative, mirror_kind) in zip(
        source_entries, mirror_entries, strict=True
    ):
        if relative != mirror_relative or kind != mirror_kind:
            raise WorkspaceOverlayError(
                "refreshed cache ordering differs from the supplied folder"
            )
        if kind == "file":
            try:
                left = (source / relative).read_bytes()
                right = (mirror / relative).read_bytes()
            except OSError as exc:
                raise WorkspaceOverlayError(
                    f"cannot verify refreshed cache entry {relative}: {exc}"
                ) from exc
            if left != right:
                raise WorkspaceOverlayError(
                    f"refreshed cache entry differs from source: {relative}"
                )


def ingest_super_cache(
    *,
    source_folder: str | Path,
    harness_worktree: str | Path,
) -> dict[str, Any]:
    """Refresh ``HARNESS_WORKTREE/super-cache`` to exactly the folder contents.

    The refresh stages a complete mirror beside the cache first, verifies it
    byte-for-byte, then swaps it into place.  A failed refresh is never
    reported complete; the previous cache remains visible and the caller
    receives an error.
    """
    source = _regular_directory(source_folder, name="source folder")
    harness = _regular_directory(harness_worktree, name="harness worktree")
    cache = _lexical(harness / SUPER_CACHE_NAME)
    if _is_reparse(cache):
        raise WorkspaceOverlayError("super-cache destination is a reparse point")
    if cache.exists() and not cache.is_dir():
        raise WorkspaceOverlayError("super-cache destination is not a directory")
    source_identity = _path_identity(source)
    cache_identity = _path_identity(cache)
    if source_identity == cache_identity:
        raise WorkspaceOverlayError(
            "source folder and super-cache must be different directories"
        )
    if _inside(cache, source):
        raise WorkspaceOverlayError("super-cache must not be inside the source folder")
    if _inside(source, cache):
        raise WorkspaceOverlayError("source folder must not be inside the super-cache")

    staging = make_temporary_directory(harness, prefix=f".{SUPER_CACHE_NAME}.ingest-")
    previous: Path | None = None
    committed = False
    try:
        entries = _walk_contents(source)
        for relative, kind in entries:
            if kind == "dir":
                ensure_directory_path(staging / relative)
            else:
                data = (source / relative).read_bytes()
                mutation_replace(
                    staging,
                    relative,
                    data,
                    expected=TargetState.absent(),
                )
        _verify_contents_match(source, staging)
        if cache.exists():
            previous = _lexical(
                harness / f".{SUPER_CACHE_NAME}.previous-{uuid.uuid4().hex}"
            )
            mutation_rename(
                harness,
                cache.name,
                previous.name,
                expected_source=capture_target(harness, cache.name),
                expected_target=TargetState.absent(),
            )
        mutation_rename(
            harness,
            staging.name,
            cache.name,
            expected_source=capture_target(harness, staging.name),
            expected_target=TargetState.absent(),
        )
        committed = True
        _verify_contents_match(source, cache)
    except (MutationError, OSError, WorkspaceOverlayError) as exc:
        if committed:
            # The swap completed but the final verification failed (for example
            # a concurrent source edit).  Remove the half-new cache and restore
            # the prior contents so no partially successful refresh remains.
            try:
                if cache.exists() and not _is_reparse(cache):
                    remove_tree(
                        harness,
                        cache.name,
                        expected=capture_target(harness, cache.name),
                    )
            except (MutationError, OSError, WorkspaceOverlayError):
                pass
            if previous is not None and previous.exists() and not cache.exists():
                try:
                    mutation_rename(
                        harness,
                        previous.name,
                        cache.name,
                        expected_source=capture_target(harness, previous.name),
                        expected_target=TargetState.absent(),
                    )
                    previous = None
                except (MutationError, OSError, WorkspaceOverlayError):
                    pass
        else:
            try:
                if staging.exists() and not _is_reparse(staging):
                    remove_tree(
                        harness,
                        staging.name,
                        expected=capture_target(harness, staging.name),
                    )
            except (MutationError, OSError, WorkspaceOverlayError):
                pass
        if previous is not None and previous.exists() and not cache.exists():
            try:
                mutation_rename(
                    harness,
                    previous.name,
                    cache.name,
                    expected_source=capture_target(harness, previous.name),
                    expected_target=TargetState.absent(),
                )
            except (MutationError, OSError, WorkspaceOverlayError):
                pass
        raise WorkspaceOverlayError(
            f"super-cache refresh failed and was not reported complete: {exc}"
        ) from exc

    removed_previous = True
    if previous is not None and previous.exists():
        try:
            remove_tree(
                harness, previous.name, expected=capture_target(harness, previous.name)
            )
        except (MutationError, OSError, WorkspaceOverlayError):
            removed_previous = False
    return {
        "schema": "orchestrator-workspace-overlay-ingest/v1",
        "complete": True,
        "source_folder": str(source),
        "harness_worktree": str(harness),
        "super_cache": str(cache),
        "entry_count": len(entries),
        "replaced_prior_contents": previous is not None,
        "removed_previous": removed_previous,
    }


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _read_declaration(cache: Path) -> dict[str, Any] | None:
    declaration = cache / DECLARATION_NAME
    if not os.path.lexists(declaration):
        return None
    if _is_reparse(declaration) or not declaration.is_file():
        raise WorkspaceOverlayError(f"{DECLARATION_NAME} must be a regular file")
    try:
        data = declaration.read_bytes()
        value = json.loads(data.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WorkspaceOverlayError(
            f"{DECLARATION_NAME} is not valid UTF-8 JSON: {exc}"
        ) from exc
    if not isinstance(value, dict) or set(value) != {"schema", "append_text"}:
        raise WorkspaceOverlayError(
            f"{DECLARATION_NAME} must contain only schema and append_text"
        )
    if value.get("schema") != SUPER_CACHE_CONTROL_SCHEMA:
        raise WorkspaceOverlayError(f"{DECLARATION_NAME} schema is invalid")
    append_text = value.get("append_text")
    if not isinstance(append_text, list) or any(
        not isinstance(item, str) or not item.strip() for item in append_text
    ):
        raise WorkspaceOverlayError(
            f"{DECLARATION_NAME} append_text must be a list of paths"
        )
    normalized: list[str] = []
    for item in append_text:
        relative = safe_relative_path(item.strip())
        normalized.append(relative.as_posix())
    if len(set(normalized)) != len(normalized):
        raise WorkspaceOverlayError(
            f"{DECLARATION_NAME} append_text contains duplicates"
        )
    return {"schema": SUPER_CACHE_CONTROL_SCHEMA, "append_text": normalized}


@dataclass(frozen=True)
class _PlanEntry:
    relative: str
    kind: str
    operation: str
    payload: bytes | None = None


def _build_plan(
    cache: Path,
    target: Path,
    declaration: Mapping[str, Any] | None,
) -> list[_PlanEntry]:
    append_targets = set((declaration or {}).get("append_text", []))
    plan: list[_PlanEntry] = []
    for relative, kind in _walk_contents(cache):
        if relative == DECLARATION_NAME:
            continue
        target_path = target / relative
        exists = os.path.lexists(target_path)
        if kind == "dir":
            if exists:
                if _is_reparse(target_path) or not target_path.is_dir():
                    raise OverlayCollisionError(
                        f"existing-file collision: cache directory {relative} overlaps a non-directory"
                    )
                operation = "merge_dir"
            else:
                operation = "create_dir"
            plan.append(_PlanEntry(relative, kind, operation))
            continue
        if exists:
            if _is_reparse(target_path) or not target_path.is_file():
                raise OverlayCollisionError(
                    f"existing-file collision: cache file {relative} overlaps a non-file"
                )
            if relative in append_targets:
                operation = "append"
            else:
                raise OverlayCollisionError(
                    f"existing-file collision: {relative} exists in the target and is not declared "
                    f"for append in {DECLARATION_NAME}"
                )
        else:
            operation = "create_file"
        payload = (cache / relative).read_bytes()
        plan.append(_PlanEntry(relative, kind, operation, payload))
    return plan


def _validate_append_targets(plan: list[_PlanEntry], target: Path) -> None:
    for entry in plan:
        if entry.operation != "append":
            continue
        target_path = target / entry.relative
        try:
            data = target_path.read_bytes()
            data.decode("utf-8", errors="strict")
        except (OSError, UnicodeDecodeError) as exc:
            raise OverlayCollisionError(
                f"declared append target is not an existing regular UTF-8 text file: {entry.relative}"
            ) from exc


def _encode_bytes(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _decode_bytes(value: object, *, name: str) -> bytes:
    if not isinstance(value, str):
        raise WorkspaceOverlayError(f"receipt {name} must be a base64 string")
    try:
        return base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as exc:
        raise WorkspaceOverlayError(f"receipt {name} is not valid base64") from exc


def _rollback_applied(
    target: Path,
    applied_files: list[tuple[str, str, bytes]],
    created_dirs: list[str],
) -> list[str]:
    """Reverse every recorded target mutation; returns human-readable errors."""
    errors: list[str] = []
    for relative, operation, pre_bytes in reversed(applied_files):
        try:
            if operation == "create_file":
                mutation_delete(
                    target, relative, expected=capture_target(target, relative)
                )
            else:
                mutation_replace(
                    target,
                    relative,
                    pre_bytes,
                    expected=capture_target(target, relative),
                )
        except (MutationError, OSError) as exc:
            errors.append(f"{relative}: {type(exc).__name__}")
    for directory in sorted(
        created_dirs, key=lambda item: len(item.split("/")), reverse=True
    ):
        directory_path = target / directory
        if (
            directory_path.exists()
            and not _is_reparse(directory_path)
            and directory_path.is_dir()
        ):
            try:
                if next(os.scandir(directory_path), None) is None:
                    remove_tree(
                        target, directory, expected=capture_target(target, directory)
                    )
            except (MutationError, OSError):
                pass
    return errors


def prepare_worktree(
    *,
    super_cache: str | Path,
    target_worktree: str | Path,
    role: str,
    receipt_path: str | Path,
) -> dict[str, Any]:
    """Prepare one identified worktree from the cache contents current for this call.

    The complete operation is preflighted before any mutation.  Missing paths
    are created; directories merge recursively without overwriting existing
    files; an existing-file collision is rejected unless its relative path is
    declared in the optional root ``.super-cache.json`` ``append_text`` list.
    The declaration file is control data and is never copied to the target.
    """
    if role not in SUPPORTED_ROLES:
        raise WorkspaceOverlayError("role must be orchestrator or subagent")
    cache = _regular_directory(super_cache, name="super-cache")
    target = _regular_directory(target_worktree, name="target worktree")
    cache_identity = _path_identity(cache)
    target_identity = _path_identity(target)
    if (
        cache_identity == target_identity
        or _inside(cache, target)
        or _inside(target, cache)
    ):
        raise WorkspaceOverlayError(
            "super-cache and target worktree must be separate directories"
        )
    receipt = _lexical(receipt_path)

    declaration = _read_declaration(cache)
    plan = _build_plan(cache, target, declaration)
    _validate_append_targets(plan, target)
    for entry in plan:
        current = target
        for part in Path(entry.relative).parts[:-1]:
            current = current / part
            if os.path.lexists(current):
                if _is_reparse(current):
                    raise OverlayCollisionError(
                        f"target path component is a reparse point: {current}"
                    )
                if not current.is_dir():
                    raise OverlayCollisionError(
                        f"target path parent is not a directory: {current}"
                    )

    # Complete collision preflight passed; only now may any mutation occur.
    try:
        receipt.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise WorkspaceOverlayError(f"cannot create receipt parent: {exc}") from exc
    if _is_reparse(receipt.parent):
        raise WorkspaceOverlayError("receipt parent must not be a reparse point")

    pre_overlay_bytes: dict[str, str] = {}
    post_prepare_bytes: dict[str, str] = {}
    created_paths: list[str] = []
    affected_paths: list[str] = []
    operations: dict[str, str] = {}
    applied_files: list[tuple[str, str, bytes]] = []  # relative, operation, pre_bytes
    created_dirs: list[str] = []

    try:
        for entry in plan:
            operations[entry.relative] = entry.operation
            affected_paths.append(entry.relative)
            if entry.operation == "create_dir":
                ensure_directory_path(target / entry.relative)
                created_paths.append(entry.relative)
                created_dirs.append(entry.relative)
            elif entry.operation == "merge_dir":
                continue
            elif entry.operation == "create_file":
                assert entry.payload is not None
                mutation_replace(
                    target,
                    entry.relative,
                    entry.payload,
                    expected=TargetState.absent(),
                )
                created_paths.append(entry.relative)
                applied_files.append((entry.relative, "create_file", b""))
                post_prepare_bytes[entry.relative] = _encode_bytes(
                    (target / entry.relative).read_bytes()
                )
            else:  # append
                assert entry.payload is not None
                pre_bytes = (target / entry.relative).read_bytes()
                mutation_append_bytes(target, entry.relative, entry.payload)
                applied_files.append((entry.relative, "append", pre_bytes))
                pre_overlay_bytes[entry.relative] = _encode_bytes(pre_bytes)
                post_prepare_bytes[entry.relative] = _encode_bytes(
                    (target / entry.relative).read_bytes()
                )
    except (MutationError, OSError) as exc:
        rollback_errors = _rollback_applied(target, applied_files, created_dirs)
        detail = f"worktree preparation failed and was rolled back after {type(exc).__name__}: {exc}"
        if rollback_errors:
            detail += "; rollback errors: " + ", ".join(rollback_errors)
        raise WorkspaceOverlayError(detail) from exc
    receipt_record: dict[str, Any] = {
        "schema": OVERLAY_RECEIPT_SCHEMA,
        "target_worktree_id": _path_identity(target),
        "role": role,
        "completed": True,
        "prepared_utc": iso_utc(utc_now()) or "",
        "affected_paths": sorted(affected_paths),
        "operations": {key: operations[key] for key in sorted(operations)},
        "created_paths": sorted(created_paths),
        "pre_overlay_bytes": dict(sorted(pre_overlay_bytes.items())),
        "post_prepare_bytes": dict(sorted(post_prepare_bytes.items())),
    }
    try:
        mutation_replace(
            receipt.parent,
            receipt.name,
            (
                json.dumps(receipt_record, sort_keys=True, indent=2, ensure_ascii=False)
                + "\n"
            ).encode("utf-8"),
            expected=capture_target(receipt.parent, receipt.name),
        )
    except (MutationError, OSError) as exc:
        rollback_errors = _rollback_applied(target, applied_files, created_dirs)
        detail = (
            "preparation applied but the receipt could not be published and "
            f"target changes were rolled back: {exc}"
        )
        if rollback_errors:
            detail += "; rollback errors: " + ", ".join(rollback_errors)
        raise WorkspaceOverlayError(detail) from exc
    return {
        "schema": "orchestrator-workspace-overlay-prepare/v1",
        "complete": True,
        "target_worktree_id": _path_identity(target),
        "role": role,
        "receipt_path": str(receipt),
        "affected_paths": sorted(affected_paths),
        "operations": operations,
        "created_paths": sorted(created_paths),
        "appended_paths": sorted(pre_overlay_bytes),
    }


def _read_receipt(receipt_path: str | Path) -> dict[str, Any]:
    path = _lexical(receipt_path)
    if _is_reparse(path) or not path.is_file():
        raise WorkspaceOverlayError(
            "overlay receipt is missing or is not a regular file"
        )
    try:
        data = path.read_bytes()
        value = json.loads(data.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WorkspaceOverlayError(
            f"overlay receipt is not valid UTF-8 JSON: {exc}"
        ) from exc
    if not isinstance(value, dict) or value.get("schema") != OVERLAY_RECEIPT_SCHEMA:
        raise WorkspaceOverlayError("overlay receipt schema is invalid")
    return value


def _validate_receipt(
    value: Mapping[str, Any],
) -> tuple[
    str, str, list[str], dict[str, str], list[str], dict[str, str], dict[str, str]
]:
    required = {
        "schema",
        "target_worktree_id",
        "role",
        "completed",
        "prepared_utc",
        "affected_paths",
        "operations",
        "created_paths",
        "pre_overlay_bytes",
        "post_prepare_bytes",
    }
    if set(value) != required:
        raise WorkspaceOverlayError("overlay receipt has an invalid closed shape")
    if value.get("completed") is not True:
        raise WorkspaceOverlayError("overlay receipt is not completed")
    role = value.get("role")
    if role not in SUPPORTED_ROLES:
        raise WorkspaceOverlayError("overlay receipt role is invalid")
    target_id = value.get("target_worktree_id")
    if not isinstance(target_id, str) or not target_id.strip():
        raise WorkspaceOverlayError("overlay receipt target worktree ID is missing")
    affected = value.get("affected_paths")
    operations = value.get("operations")
    created = value.get("created_paths")
    if (
        not isinstance(affected, list)
        or not isinstance(operations, dict)
        or not isinstance(created, list)
        or any(not isinstance(item, str) or not item for item in affected)
        or any(not isinstance(item, str) or not item for item in created)
    ):
        raise WorkspaceOverlayError("overlay receipt path lists are invalid")
    normalized_affected: list[str] = []
    for item in affected:
        normalized_affected.append(safe_relative_path(item).as_posix())
    if len(set(normalized_affected)) != len(normalized_affected):
        raise WorkspaceOverlayError("overlay receipt affected paths contain duplicates")
    if set(operations) != set(normalized_affected) or any(
        operation not in _APPLIED_OPERATIONS for operation in operations.values()
    ):
        raise WorkspaceOverlayError(
            "overlay receipt operations do not match affected paths"
        )
    normalized_created: list[str] = []
    for item in created:
        normalized_created.append(safe_relative_path(item).as_posix())
    if not set(normalized_created).issubset(normalized_affected):
        raise WorkspaceOverlayError(
            "overlay receipt created paths are not affected paths"
        )
    pre_bytes = value.get("pre_overlay_bytes")
    post_bytes = value.get("post_prepare_bytes")
    if not isinstance(pre_bytes, dict) or not isinstance(post_bytes, dict):
        raise WorkspaceOverlayError("overlay receipt byte maps are invalid")
    if not set(pre_bytes).issubset(normalized_affected) or not set(post_bytes).issubset(
        normalized_affected
    ):
        raise WorkspaceOverlayError(
            "overlay receipt byte map keys are not affected paths"
        )
    for relative in pre_bytes:
        _decode_bytes(pre_bytes[relative], name="pre_overlay_bytes")
        if operations[relative] != "append":
            raise WorkspaceOverlayError(
                "overlay receipt pre-overlay bytes exist for a non-append path"
            )
    for relative in post_bytes:
        _decode_bytes(post_bytes[relative], name="post_prepare_bytes")
        if operations[relative] not in {"append", "create_file"}:
            raise WorkspaceOverlayError(
                "overlay receipt post-prepare bytes exist for a non-file path"
            )
    for relative, operation in operations.items():
        if operation in {"append", "create_file"} and relative not in post_bytes:
            raise WorkspaceOverlayError(
                "overlay receipt is missing post-prepare bytes for a file path"
            )
    return (
        target_id,
        str(role),
        normalized_affected,
        {key: operations[key] for key in normalized_affected},
        normalized_created,
        dict(pre_bytes),
        dict(post_bytes),
    )


def restore_worktree(*, receipt_path: str | Path) -> dict[str, Any]:
    """Restore exactly the changes recorded by one preparation receipt.

    Every affected file's current bytes are compared directly with the
    receipt's exact post-prepare bytes.  Unchanged appended files get their
    exact pre-overlay bytes back; unchanged overlay-created files are removed;
    then only empty receipt-created directories are removed.  Any later edit,
    missing/malformed/mismatched/ambiguous receipt, or unsafe target leaves the
    target visible and blocks only retirement/reuse -- never broad deletion.
    """
    blocked: list[str] = []
    restored_paths: list[str] = []
    removed_paths: list[str] = []
    preserved_paths: list[str] = []
    left_directories: list[str] = []
    try:
        raw = _read_receipt(receipt_path)
        (
            target_id,
            role,
            affected,
            operations,
            created,
            pre_bytes,
            post_bytes,
        ) = _validate_receipt(raw)
    except WorkspaceOverlayError as exc:
        return {
            "schema": OVERLAY_RECEIPT_SCHEMA,
            "receipt_path": str(_lexical(receipt_path)),
            "outcome": "BLOCKED",
            "reason": f"receipt invalid: {exc}",
            "restored_paths": [],
            "removed_paths": [],
            "preserved_paths": [],
            "left_directories": [],
        }
    target = _lexical(target_id)
    if _is_reparse(target) or not target.is_dir():
        return {
            "schema": OVERLAY_RECEIPT_SCHEMA,
            "receipt_path": str(_lexical(receipt_path)),
            "target_worktree_id": target_id,
            "role": role,
            "outcome": "BLOCKED",
            "reason": "target worktree is missing or unsafe",
            "restored_paths": [],
            "removed_paths": [],
            "preserved_paths": [],
            "left_directories": [],
        }

    def blocked_entry(relative: str, reason: str) -> None:
        preserved_paths.append(relative)
        blocked.append(f"{relative}: {reason}")

    for relative in affected:
        operation = operations[relative]
        if operation in {"create_dir", "merge_dir"}:
            continue
        target_path = target / relative
        if not os.path.lexists(target_path):
            blocked_entry(relative, "missing (not equal to post-prepare bytes)")
            continue
        if _is_reparse(target_path) or not target_path.is_file():
            blocked_entry(relative, "not a regular file")
            continue
        try:
            current = target_path.read_bytes()
        except OSError as exc:
            blocked_entry(relative, f"unreadable: {exc}")
            continue
        expected_post = _decode_bytes(post_bytes[relative], name="post_prepare_bytes")
        if current != expected_post:
            blocked_entry(
                relative,
                "later edit detected (current bytes differ from post-prepare bytes)",
            )
            continue
        try:
            if operation == "append":
                mutation_replace(
                    target,
                    relative,
                    _decode_bytes(pre_bytes[relative], name="pre_overlay_bytes"),
                    expected=capture_target(target, relative),
                )
                restored_paths.append(relative)
            else:
                mutation_delete(
                    target, relative, expected=capture_target(target, relative)
                )
                removed_paths.append(relative)
        except (MutationError, OSError) as exc:
            blocked_entry(relative, f"restoration failed: {exc}")

    for directory in sorted(
        (relative for relative in created if operations[relative] == "create_dir"),
        key=lambda item: len(item.split("/")),
        reverse=True,
    ):
        directory_path = target / directory
        if not os.path.lexists(directory_path):
            continue
        if _is_reparse(directory_path) or not directory_path.is_dir():
            left_directories.append(directory)
            blocked.append(f"{directory}: unsafe directory")
            continue
        try:
            if next(os.scandir(directory_path), None) is not None:
                left_directories.append(directory)
                blocked.append(
                    f"{directory}: non-empty created directory (later work preserved)"
                )
                continue
            remove_tree(target, directory, expected=capture_target(target, directory))
            removed_paths.append(directory)
        except (MutationError, OSError) as exc:
            left_directories.append(directory)
            blocked.append(f"{directory}: removal failed: {exc}")

    outcome = "BLOCKED" if blocked else "RESTORED"
    reason = "; ".join(blocked) if blocked else "exact byte restoration completed"
    return {
        "schema": OVERLAY_RECEIPT_SCHEMA,
        "receipt_path": str(_lexical(receipt_path)),
        "target_worktree_id": target_id,
        "role": role,
        "outcome": outcome,
        "reason": reason,
        "restored_paths": sorted(set(restored_paths)),
        "removed_paths": sorted(set(removed_paths)),
        "preserved_paths": sorted(set(preserved_paths)),
        "left_directories": sorted(set(left_directories)),
    }


def verify_overlay_receipt(
    *,
    receipt_path: str | Path | None,
    expected_target_worktree_id: str | Path,
    role: str = "subagent",
) -> dict[str, Any]:
    """Prelaunch lane-controller verification (REQ-O41).

    Absence means no overlay was requested and is allowed.  A present receipt
    must be completed and name the expected target worktree ID and role.  The
    receipt is never compared with the current cache and no cache work is
    performed here.
    """
    if receipt_path is None:
        return {
            "present": False,
            "verified": True,
            "reason": "no overlay was requested",
        }
    expected = _path_identity(Path(expected_target_worktree_id))
    try:
        raw = _read_receipt(receipt_path)
        target_id, receipt_role, _, _, _, _, _ = _validate_receipt(raw)
    except WorkspaceOverlayError as exc:
        return {
            "present": True,
            "verified": False,
            "reason": str(exc),
        }
    if receipt_role != role:
        return {
            "present": True,
            "verified": False,
            "reason": f"overlay receipt role {receipt_role!r} is not {role!r}",
        }
    if _path_identity(Path(target_id)) != expected:
        return {
            "present": True,
            "verified": False,
            "reason": "overlay receipt target worktree ID does not match the launch worktree",
        }
    return {
        "present": True,
        "verified": True,
        "target_worktree_id": target_id,
        "role": receipt_role,
        "reason": "completed overlay receipt matches the identified worktree and role",
    }


__all__ = [
    "DECLARATION_NAME",
    "OVERLAY_RECEIPT_SCHEMA",
    "SUPER_CACHE_CONTROL_SCHEMA",
    "SUPER_CACHE_NAME",
    "SUPPORTED_ROLES",
    "OverlayCollisionError",
    "WorkspaceOverlayError",
    "ingest_super_cache",
    "install_workspace_rules",
    "prepare_worktree",
    "restore_worktree",
    "verify_overlay_receipt",
]
