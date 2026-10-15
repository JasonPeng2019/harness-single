"""``harness setup``: idempotent one-time integration.

Validates the stored config and resource manifest, preflights the entire
catalog (active cache, ROOT payloads, launcher bindings) before writing
anything, then creates only the required runtime parents/state/idle managed
queue, atomically stages and byte-verifies the active super-cache, installs
the ROOT payloads, writes the active resource manifest + lease dir, and
starts the persistent monitor.  It starts no lane or provider and never
creates an epoch or worktree.

Launcher bindings are shipped/read-only: setup validates the catalog binding
bytes against the matching registered binding and imports the registered
binding; it never copies a binding into product source.
"""

from __future__ import annotations

# Setuptools executes this filename as a build script. Native harness setup
# imports it as a package module; keep those two entry points explicit.
if __name__ == "__main__" and not __package__:
    from setuptools import setup as build_package
    build_package()
    raise SystemExit(0)

import hashlib
import json
import os
import shutil
import stat
import sys
import time
import tomllib
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable

from . import processes
from .mutation import (
    CapturedTree, DirectoryExpectations, FileMetadata, MutationConflict, TargetState,
    _DirectoryAnchor, _capture_anchored, _require_anchored_directories,
    _require_anchored_expected, _same_state, capture_target, capture_tree,
    _revalidate_tree, append_bytes, create_directory, delete, durable_identity,
    remove_tree, rename, replace, safe_relative_path,
)
from .stable_io import PreparedOutputTransaction
from .activity_log import append_activity
from .config import (
    ConfigError,
    HarnessConfig,
    compute_config_identity,
    find_harness_root,
    load_config,
    load_resource_manifest,
    path_identity,
)
from .core import iso_utc, read_json
from .epochs import MANAGER_QUEUE_SCHEMA, manager_queue_path, read_current_epoch
from .records import (
    RecordLock,
    _replace_with_retry,
    atomic_write_json,
    read_record,
)
from .records import runtime_admission_lock

RUNTIME_STATE_SCHEMA = "runtime-state/v1"
MONITOR_SCHEMA = "monitor/v1"
RESOURCE_MANIFEST_SCHEMA = "resource-manifest/v1"
ROOT_HOOK_BINDING_SCHEMA = "harness-hook-binding/v1"
ROOT_HOOK_BINDING_NAME = "orchestrator-harness-binding.json"
CODEX_ROOT_CONFIG = Path(".codex") / "config.toml"
SHARED_ROOT_HOOK_CONFIGS = {
    Path(".codex") / "hooks.json",
    Path(".claude") / "settings.json",
    Path(".qwen") / "settings.json",
}
COMPOSED_PAYLOADS = Path("composed-payloads")
CACHE_TRANSACTION_SCHEMA = "super-cache-transaction/v2"
LEGACY_CACHE_TRANSACTION_SCHEMA = "super-cache-transaction/v1"
CACHE_TRANSACTION_NAME = "super-cache.transaction.json"
SHARED_WORKER_FILES = (
    Path(".agent-workspace") / "hook-dispatch.py",
    Path(".agent-workspace") / "lane-queue.py",
    Path(".agent-workspace") / "manager-notify.py",
    Path(".agent-workspace") / "result-stop-check.py",
)
PROVIDER_WORKER_FILES = (
    Path("orchestrator-harness-binding.json"),
    Path("skills") / "lane-assignment" / "SKILL.md",
    Path("skills") / "manager-notify" / "SKILL.md",
)

SETUP_CONFIG_INVALID = "SETUP_CONFIG_INVALID"
SETUP_CACHE_INVALID = "SETUP_CACHE_INVALID"
SETUP_RESOURCE_MANIFEST_INVALID = "SETUP_RESOURCE_MANIFEST_INVALID"
SETUP_ADAPTER_COLLISION = "SETUP_ADAPTER_COLLISION"
SETUP_OVERWRITE_FAILED = "SETUP_OVERWRITE_FAILED"
SETUP_MONITOR_ALREADY_RUNNING = "SETUP_MONITOR_ALREADY_RUNNING"

MONITOR_RECOVERED = "MONITOR_RECOVERED"
MONITOR_HEALTHY = "MONITOR_HEALTHY"
MONITOR_DELIBERATELY_STOPPED = "MONITOR_DELIBERATELY_STOPPED"
MONITOR_STOP_INCOMPLETE = "MONITOR_STOP_INCOMPLETE"
MONITOR_CLEANUP_UNPROVEN = "MONITOR_CLEANUP_UNPROVEN"
MONITOR_IDENTITY_UNPROVEN = "MONITOR_IDENTITY_UNPROVEN"
MONITOR_RECOVERY_DISABLED = "MONITOR_RECOVERY_DISABLED"
MONITOR_RUNTIME_NOT_OPEN = "MONITOR_RUNTIME_NOT_OPEN"


class SetupError(ConfigError):
    """A setup failure carrying a stable result code."""

    def __init__(self, code: str, message: str, *, next_action: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.next_action = next_action


class _CacheRecoveryRequired(SetupError):
    """Preserve failed rollback/cleanup until the next explicit setup call."""


def runtime_state_path(rt: Path) -> Path:
    return rt / "RUNTIME_STATE.json"


def read_runtime_state(rt: Path) -> dict[str, Any] | None:
    path = runtime_state_path(rt)
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(metadata.st_mode) or _is_reparse(metadata):
        raise ConfigError(f"runtime state is not a plain record: {path}")
    try:
        record = read_record(path, RUNTIME_STATE_SCHEMA)
    except (OSError, ValueError) as exc:
        raise ConfigError(f"runtime state is unreadable or invalid: {path}: {exc}") from exc
    if record.get("state") not in {"OPEN", "SHUTTING_DOWN", "CLOSED"}:
        raise ConfigError(f"runtime state is invalid: {path}")
    return record


def set_runtime_state(rt: Path, state: str) -> dict[str, Any]:
    if state not in {"OPEN", "SHUTTING_DOWN", "CLOSED"}:
        raise ConfigError(f"invalid runtime state: {state}")
    record = {"schema": RUNTIME_STATE_SCHEMA, "state": state, "updated_at": iso_utc()}
    with runtime_admission_lock(rt):
        atomic_write_json(runtime_state_path(rt), record)
    return record


def monitor_record_path(rt: Path) -> Path:
    return rt / "monitor" / "MONITOR.json"


def read_monitor_record(rt: Path) -> dict[str, Any] | None:
    path = monitor_record_path(rt)
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(metadata.st_mode) or _is_reparse(metadata):
        raise ConfigError(f"monitor ownership is not a plain record: {path}")
    try:
        return read_record(path, MONITOR_SCHEMA)
    except (OSError, ValueError) as exc:
        raise ConfigError(f"monitor ownership is unreadable or invalid: {path}: {exc}") from exc


def _plan_tree(source: Path) -> list[tuple[Path, Path]]:
    """Return ``(source_file, relative_path)`` pairs for one source tree.

    Bytecode caches are never part of a shipped plan.
    """
    if not source.is_dir():
        raise ConfigError(f"source tree missing: {source}")
    planned: list[tuple[Path, Path]] = []
    for item in sorted(source.rglob("*")):
        if not item.is_file() or "__pycache__" in item.parts:
            continue
        planned.append((item, item.relative_to(source)))
    return planned


def _claim_destination(claims: dict[str, str], relative: Path, owner: str) -> None:
    """Claim one installed path, including its parent directories."""
    parts = [part.casefold() for part in relative.parts]
    key = "/".join(parts)
    if key in claims:
        raise SetupError(
            SETUP_ADAPTER_COLLISION,
            f"worker destination {relative} is claimed by {claims[key]} and {owner}",
        )
    for index in range(1, len(parts)):
        parent = "/".join(parts[:index])
        if parent in claims:
            raise SetupError(
                SETUP_ADAPTER_COLLISION,
                f"worker destination {relative} is below a file owned by {claims[parent]}",
            )
    if any(existing.startswith(key + "/") for existing in claims):
        raise SetupError(
            SETUP_ADAPTER_COLLISION,
            f"worker destination {relative} replaces a planned directory",
        )
    claims[key] = owner


@dataclass(frozen=True)
class _CacheCatalog:
    harness: Path
    trees: tuple[CapturedTree, ...]
    sources: tuple[_RootPayloadSource, ...]


def _capture_cache_catalog(harness_root: Path) -> _CacheCatalog:
    """Capture native catalog bytes/metadata before any catalog validation."""
    harness = Path(os.path.abspath(harness_root))
    try:
        trees = tuple(capture_tree(harness, name) for name in ("super-cache", "adapters"))
        files = {}
        for tree in trees:
            for relative, state in tree.entries:
                if state.kind == "file":
                    path = tree.root / relative
                    source = _capture_root_source(path)
                    if not _same_state(source.state, state):
                        raise MutationConflict(f"catalog source changed during capture: {path}")
                    files[path] = source
        result = _CacheCatalog(harness, trees, tuple(files.values()))
        _revalidate_cache_catalog(result)
        return result
    except (OSError, ValueError, TypeError) as exc:
        raise SetupError(SETUP_CACHE_INVALID, f"cache catalog capture rejected: {exc}") from exc


def _revalidate_cache_catalog(catalog: _CacheCatalog) -> None:
    for tree in catalog.trees:
        _revalidate_tree(tree)
    for source in catalog.sources:
        _revalidate_root_source(source)


def _catalog_state(catalog: _CacheCatalog, path: Path) -> TargetState:
    path = path.absolute()
    for tree in catalog.trees:
        for relative, state in tree.entries:
            if tree.root / relative == path:
                return state
    return TargetState.absent()


def _catalog_is_dir(path: Path, catalog: _CacheCatalog | None) -> bool:
    return path.is_dir() if catalog is None else _catalog_state(catalog, path).kind == "directory"


def _catalog_is_file(path: Path, catalog: _CacheCatalog | None) -> bool:
    return path.is_file() if catalog is None else _catalog_state(catalog, path).kind == "file"


def _catalog_tree(path: Path, catalog: _CacheCatalog | None) -> list[tuple[Path, Path]]:
    if catalog is None:
        return _plan_tree(path)
    if not _catalog_is_dir(path, catalog):
        return []
    path = path.absolute()
    return sorted(((source.path, source.path.relative_to(path)) for source in catalog.sources
                   if source.path.is_relative_to(path)), key=lambda item: item[1].as_posix())


def _catalog_adapters(path: Path, catalog: _CacheCatalog | None) -> list[Path]:
    if catalog is None:
        return sorted(path.iterdir())
    path = path.absolute()
    return sorted({tree.root / relative for tree in catalog.trees for relative, state in tree.entries
                   if state.kind == "directory" and (tree.root / relative).parent == path})


def _catalog_bytes(path: Path, catalog: _CacheCatalog | None) -> bytes:
    if catalog is None:
        return path.read_bytes()
    source = next((source for source in catalog.sources if source.path == path.absolute()), None)
    if source is None:
        raise SetupError(SETUP_CACHE_INVALID, f"uncaptured cache source: {path}")
    return source.state.content

def _validate_worker_commands(
    provider_id: str, dotdir: str, files: dict[Path, Path], *, catalog: _CacheCatalog | None = None
) -> None:
    """Shipped hook commands must name files owned by the same worker payload."""
    for config_name in ("hooks.json", "settings.json"):
        config = files.get(Path(dotdir) / config_name)
        if config is None:
            continue
        try:
            record = json.loads(_catalog_bytes(config, catalog).decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SetupError(SETUP_CONFIG_INVALID, f"invalid {provider_id} worker hooks: {config}: {exc}") from exc
        hooks = record.get("hooks") if isinstance(record, dict) else None
        if not isinstance(hooks, dict):
            raise SetupError(SETUP_CONFIG_INVALID, f"worker hook commands missing for {provider_id}: {config}")
        seen: set[str] = set()
        owned = {path.as_posix().casefold() for path in files}
        for groups in hooks.values():
            if not isinstance(groups, list):
                raise SetupError(SETUP_CONFIG_INVALID, f"malformed worker hook groups for {provider_id}: {config}")
            for group in groups:
                entries = group.get("hooks") if isinstance(group, dict) else None
                if not isinstance(entries, list) or not entries:
                    raise SetupError(SETUP_CONFIG_INVALID, f"malformed worker hook command for {provider_id}: {config}")
                for entry in entries:
                    command = entry.get("command") if isinstance(entry, dict) else None
                    if not isinstance(command, str):
                        raise SetupError(SETUP_CONFIG_INVALID, f"malformed worker hook command for {provider_id}: {config}")
                    parts = command.split()
                    relative = PurePosixPath(parts[-1].replace("\\", "/")) if len(parts) == 2 and parts[0] == "python" else None
                    destination = relative.as_posix().casefold() if relative is not None else None
                    if (
                        relative is None
                        or not relative.parts
                        or relative.is_absolute()
                        or ".." in relative.parts
                        or relative.parts[0].casefold() != dotdir.casefold()
                        or destination not in owned
                        or destination in seen
                    ):
                        raise SetupError(
                            SETUP_ADAPTER_COLLISION,
                            f"duplicate or unknown {provider_id} worker command: {command}",
                        )
                    seen.add(destination)


def _plan_worker_compositions(
    harness_root: Path,
    *,
    root_plan: list[tuple[Path, Path]] | None = None,
    catalog: _CacheCatalog | None = None,
) -> dict[str, list[tuple[Path, Path]]]:
    """Resolve every provider's shared and provider-owned worker destination."""
    harness_root = Path(os.path.abspath(harness_root))
    catalog = catalog if catalog is not None else _capture_cache_catalog(harness_root)
    workspace = harness_root / "super-cache" / "workspace"
    if not _catalog_is_dir(workspace, catalog):
        raise SetupError(SETUP_CACHE_INVALID, f"shipped super-cache workspace missing: {workspace}")
    base = _catalog_tree(workspace, catalog)
    base_paths = {relative for _, relative in base}
    missing = next((path for path in SHARED_WORKER_FILES if path not in base_paths), None)
    if missing is not None:
        raise SetupError(SETUP_CACHE_INVALID, f"shared worker lifecycle file missing: {missing}")
    if any(not _catalog_bytes(workspace / path, catalog) for path in SHARED_WORKER_FILES):
        raise SetupError(SETUP_CACHE_INVALID, "shared worker lifecycle file is empty")
    root_owners: dict[str, str] = {}
    root_dotdirs: dict[str, set[str]] = {}
    adapters_dir = harness_root / "adapters"
    for source, relative in root_plan if root_plan is not None else _plan_root_payloads(harness_root, catalog=catalog):
        owner = source.absolute().relative_to(adapters_dir).parts[0]
        root_dotdirs.setdefault(owner, set()).add(relative.parts[0].casefold())
        key = relative.as_posix().casefold()
        if key in root_owners and root_owners[key] != owner:
            raise SetupError(SETUP_ADAPTER_COLLISION, f"ROOT destination {relative} has multiple provider owners")
        root_owners[key] = owner
    compositions: dict[str, list[tuple[Path, Path]]] = {}
    dotdir_owners: dict[str, str] = {}
    for adapter in _catalog_adapters(adapters_dir, catalog):
        if not _catalog_is_dir(adapter, catalog):
            continue
        provider_id = adapter.name
        payload = adapter / "super-cache"
        if _catalog_is_dir(payload, catalog) and not _catalog_is_file(adapter / "harness" / "launcher_binding.py", catalog):
            raise SetupError(SETUP_ADAPTER_COLLISION, f"unknown worker provider owns {payload}")
        if not _catalog_is_file(adapter / "harness" / "launcher_binding.py", catalog):
            continue
        if not _catalog_is_dir(payload, catalog):
            raise SetupError(SETUP_CACHE_INVALID, f"worker payload missing for {provider_id}: {payload}")
        worker = _catalog_tree(payload, catalog)
        dotdirs = {relative.parts[0] for _, relative in worker}
        if len(dotdirs) != 1 or not next(iter(dotdirs)).startswith(".") or ".agent-workspace" in dotdirs:
            raise SetupError(SETUP_ADAPTER_COLLISION, f"unknown provider-owned destination in {payload}")
        dotdir = next(iter(dotdirs))
        if dotdir.casefold() not in root_dotdirs.get(provider_id, set()):
            raise SetupError(SETUP_ADAPTER_COLLISION, f"unknown {provider_id} worker destination: {dotdir}")
        prior_owner = dotdir_owners.setdefault(dotdir.casefold(), provider_id)
        if prior_owner != provider_id:
            raise SetupError(SETUP_ADAPTER_COLLISION, f"provider destination {dotdir} is owned by {prior_owner} and {provider_id}")
        worker_files = {relative: source for source, relative in worker}
        missing = next((Path(dotdir) / path for path in PROVIDER_WORKER_FILES if Path(dotdir) / path not in worker_files), None)
        if missing is not None:
            raise SetupError(SETUP_CACHE_INVALID, f"{provider_id} worker tool missing: {missing}")
        for skill, helper in (("lane-assignment", "lane-queue.py"), ("manager-notify", "manager-notify.py")):
            skill_path = worker_files[Path(dotdir) / "skills" / skill / "SKILL.md"]
            if f".agent-workspace/{helper}" not in _catalog_bytes(skill_path, catalog).decode("utf-8"):
                raise SetupError(SETUP_CACHE_INVALID, f"{provider_id} worker skill does not call {helper}: {skill_path}")
        binding_path = worker_files[Path(dotdir) / PROVIDER_WORKER_FILES[0]]
        binding = _captured_json_object(_catalog_bytes(binding_path, catalog), binding_path, description="worker binding")
        if binding.get("schema") != ROOT_HOOK_BINDING_SCHEMA or binding.get("role") != "worker" or binding.get("provider_id") != provider_id:
            raise SetupError(SETUP_CONFIG_INVALID, f"worker binding identity invalid for {provider_id}")
        _validate_worker_commands(provider_id, dotdir, worker_files, catalog=catalog)
        claims: dict[str, str] = {}
        for source, relative in [*base, *worker]:
            if relative.name.casefold() in {"agents.md", "claude.md"}:
                raise SetupError(SETUP_ADAPTER_COLLISION, f"repository instruction destination is checkout-owned: {relative}")
            owner = "shared lifecycle" if source.is_relative_to(workspace) else provider_id
            _claim_destination(claims, relative, owner)
            if owner == provider_id:
                root_owner = root_owners.get(relative.as_posix().casefold())
                if root_owner is not None and root_owner != provider_id:
                    raise SetupError(SETUP_ADAPTER_COLLISION, f"worker destination {relative} belongs to ROOT provider {root_owner}")
        compositions[provider_id] = [*base, *worker]
    if not compositions:
        raise SetupError(SETUP_CACHE_INVALID, f"no managed worker payloads in {adapters_dir}")
    return compositions


def _plan_active_cache(
    harness_root: Path, *, root_plan: list[tuple[Path, Path]] | None = None,
    catalog: _CacheCatalog | None = None,
) -> list[tuple[Path, Path]]:
    """Plan the shipped cache and each provider's exact composed worker tree."""
    harness_root = Path(os.path.abspath(harness_root))
    catalog = catalog if catalog is not None else _capture_cache_catalog(harness_root)
    source_cache = harness_root / "super-cache"
    if not _catalog_is_dir(source_cache, catalog):
        raise SetupError(SETUP_CACHE_INVALID, f"shipped super-cache missing: {source_cache}")
    workspace = source_cache / "workspace"
    if not _catalog_is_dir(workspace, catalog):
        raise SetupError(
            SETUP_CACHE_INVALID, f"shipped super-cache workspace missing: {workspace}"
        )
    planned: list[tuple[Path, Path]] = []
    for source_file, relative in _catalog_tree(workspace, catalog):
        planned.append((source_file, Path("workspace") / relative))
    custom = source_cache / "custom"
    if _catalog_is_dir(custom, catalog):
        for source_file, relative in _catalog_tree(custom, catalog):
            planned.append((source_file, Path("custom") / relative))
    adapters_dir = harness_root / "adapters"
    if _catalog_is_dir(adapters_dir, catalog):
        for adapter in _catalog_adapters(adapters_dir, catalog):
            if not _catalog_is_dir(adapter, catalog):
                continue
            payload = adapter / "super-cache"
            if not _catalog_is_dir(payload, catalog):
                continue
            for source_file, relative in _catalog_tree(payload, catalog):
                planned.append(
                    (source_file, Path("adapter-payloads") / adapter.name / relative)
                )
    for provider_id, files in _plan_worker_compositions(harness_root, root_plan=root_plan, catalog=catalog).items():
        for source_file, relative in files:
            planned.append((source_file, COMPOSED_PAYLOADS / provider_id / relative))
    return planned


def _cache_entries(root: Path) -> tuple[set[str], set[str]]:
    """List a cache without following links or Windows reparse points."""
    directories: set[str] = set()
    files: set[str] = set()
    try:
        root_stat = root.lstat()
        if not stat.S_ISDIR(root_stat.st_mode) or _is_reparse(root_stat):
            raise SetupError(SETUP_CACHE_INVALID, f"cache is not a plain directory: {root}")
        pending = [root]
        while pending:
            parent = pending.pop()
            with os.scandir(parent) as entries:
                for entry in entries:
                    path = Path(entry.path)
                    relative = path.relative_to(root).as_posix()
                    entry_stat = entry.stat(follow_symlinks=False)
                    if _is_reparse(entry_stat):
                        raise SetupError(SETUP_CACHE_INVALID, f"cache contains a link or reparse point: {path}")
                    if stat.S_ISDIR(entry_stat.st_mode):
                        directories.add(relative)
                        pending.append(path)
                    elif stat.S_ISREG(entry_stat.st_mode):
                        files.add(relative)
                    else:
                        raise SetupError(SETUP_CACHE_INVALID, f"cache contains an unknown path: {path}")
    except OSError as exc:
        raise SetupError(SETUP_CACHE_INVALID, f"cache cannot be inspected: {root}: {exc}") from exc
    return directories, files


def _is_reparse(info: os.stat_result) -> bool:
    return stat.S_ISLNK(info.st_mode) or bool(
        getattr(info, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _require_plain_workspace_boundary(rt: Path, *, code: str) -> None:
    """Reject redirects at the configured workspace and runtime roots."""
    for path in (rt.parent, rt):
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise SetupError(code, f"workspace boundary cannot be inspected: {path}: {exc}") from exc
        if _is_reparse(info):
            raise SetupError(code, f"workspace boundary is a link or reparse point: {path}")


def _require_plain_root_destination(root_workspace: Path, relative: Path) -> None:
    """Check each existing component of a planned ROOT payload destination."""
    path = root_workspace
    for part in (None, *relative.parts):
        if part is not None:
            path = path / part
        try:
            info = path.lstat()
        except FileNotFoundError:
            break
        except OSError as exc:
            raise SetupError(SETUP_ADAPTER_COLLISION, f"ROOT payload path cannot be inspected: {path}: {exc}") from exc
        if _is_reparse(info):
            raise SetupError(
                SETUP_ADAPTER_COLLISION,
                f"ROOT payload path is a link or reparse point: {path}",
                next_action=f"inspect and restore a plain ROOT payload path at {path}, then re-run setup",
            )


def _manifest_for_tree(root: Path) -> dict[str, Any]:
    tree = capture_tree(root.parent, root.name)
    manifest = _tree_manifest(tree)
    if manifest is None:
        raise SetupError(SETUP_CACHE_INVALID, f"cache tree is absent: {root}")
    _revalidate_tree(tree)
    return manifest


def _manifest_for_plan(plan: list[tuple[Path, Path]]) -> dict[str, Any]:
    return _cache_plan_manifest(plan)


def _validate_active_cache(
    plan: list[tuple[Path, Path]], destination: Path, *, catalog: _CacheCatalog | None = None,
) -> None:
    """Validate original native membership/bytes against captured source bytes."""
    expected_manifest = _cache_plan_manifest(plan, catalog)
    original = capture_tree(destination.parent, destination.name)
    if _tree_manifest(original) != expected_manifest:
        raise SetupError(SETUP_CACHE_INVALID, f"active cache has missing, unknown or changed paths: {destination}")
    _revalidate_tree(original)
    if catalog is not None:
        _revalidate_cache_catalog(catalog)


def _validated_cache_for_dispatch(harness_root: Path, rt: Path, *, catalog: _CacheCatalog | None = None) -> None:
    """Keep both bootstrap profiles outside an unresolved cache transaction."""
    _require_plain_workspace_boundary(rt, code=SETUP_CACHE_INVALID)
    catalog = catalog if catalog is not None else _capture_cache_catalog(harness_root)
    live_valid, _ = _inspect_active_cache(
        _plan_active_cache(harness_root, catalog=catalog), rt, overwrite=False, for_dispatch=True, catalog=catalog
    )
    if not live_valid:
        raise SetupError(
            SETUP_CACHE_INVALID, f"installed active cache is missing: {rt / 'super-cache'}"
        )
    _revalidate_cache_catalog(catalog)


def _validated_worker_payload(harness_root: Path, rt: Path, provider_id: str) -> Path:
    """Return only the installed composed tree whose bytes match its planned owners."""
    catalog = _capture_cache_catalog(harness_root)
    _validated_cache_for_dispatch(harness_root, rt, catalog=catalog)
    compositions = _plan_worker_compositions(harness_root, catalog=catalog)
    files = compositions.get(provider_id)
    if files is None:
        raise SetupError(SETUP_CACHE_INVALID, f"unsupported worker provider: {provider_id}")
    payload = rt / "super-cache" / COMPOSED_PAYLOADS / provider_id
    for source, relative in files:
        installed = payload / relative
        if not installed.is_file():
            raise SetupError(SETUP_CACHE_INVALID, f"installed worker composition missing: {installed}")
        if installed.read_bytes() != _catalog_bytes(source, catalog):
            raise SetupError(SETUP_CACHE_INVALID, f"installed worker composition changed: {installed}")
    expected = {relative.as_posix() for _, relative in files}
    actual = {path.relative_to(payload).as_posix() for path in payload.rglob("*") if path.is_file()}
    if actual != expected:
        raise SetupError(SETUP_CACHE_INVALID, f"installed worker composition has unknown files: {payload}")
    _revalidate_cache_catalog(catalog)
    return payload


def _path_present(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise SetupError(SETUP_CACHE_INVALID, f"cache path cannot be inspected: {path}: {exc}") from exc
    return True


def _transaction_path(rt: Path) -> Path:
    return rt / CACHE_TRANSACTION_NAME


def _valid_cache_relative(name: object) -> bool:
    return (
        isinstance(name, str)
        and bool(name)
        and "\\" not in name
        and ":" not in name
        and not name.startswith("/")
        and all(part not in ("", ".", "..") for part in name.split("/"))
    )


def _valid_manifest(value: object) -> bool:
    if not isinstance(value, dict) or set(value) != {"directories", "files"}:
        return False
    directories = value["directories"]
    files = value["files"]
    if not isinstance(directories, list) or not isinstance(files, dict):
        return False
    if not all(_valid_cache_relative(name) for name in directories + list(files)):
        return False
    if len(set(directories)) != len(directories) or set(directories) & set(files):
        return False
    if not all(
        isinstance(digest, str)
        and len(digest) == 64
        and all(character in "0123456789abcdef" for character in digest)
        for digest in files.values()
    ):
        return False
    return all(
        all(parent.as_posix() in directories for parent in Path(name).parents if parent != Path("."))
        for name in [*directories, *files]
    )


@dataclass(frozen=True)
class _CacheBoundary:
    runtime: Path
    authority: Path
    authority_identity: tuple[int, int]
    directories: DirectoryExpectations
    runtime_state: TargetState
    journal: TargetState
    trees: tuple[tuple[str, CapturedTree | None], ...]


def _capture_cache_boundary(rt: Path) -> _CacheBoundary:
    """Capture journal, all cache slots and the runtime route before parsing."""
    rt = Path(os.path.abspath(rt))
    try:
        authority = rt
        while not os.path.lexists(authority):
            authority = authority.parent
        with _DirectoryAnchor(authority, authority) as anchor:
            identity = anchor.root_identity
            directories = {Path("."): TargetState(True, "directory", identity, None, None)}
            current = Path(".")
            for part in rt.relative_to(authority).parts:
                parent = current
                current /= part
                state = (capture_target(authority, current) if directories[parent].present else TargetState.absent())
                if state.present and state.kind != "directory":
                    raise MutationConflict(f"runtime route is not a directory: {authority / current}")
                directories[current] = state
            runtime_state = directories[rt.relative_to(authority)]
            if runtime_state.present:
                journal = capture_target(rt, CACHE_TRANSACTION_NAME)
                if journal.present and journal.kind != "file":
                    raise MutationConflict("cache journal is not a plain file")
                with _DirectoryAnchor(rt, rt) as runtime_anchor:
                    if runtime_anchor.root_identity != runtime_state.identity:
                        raise MutationConflict("cache runtime changed during capture")
                    names = sorted(name for name in os.listdir(runtime_anchor.parent_fd if os.name != "nt" else rt)
                                   if name.startswith(".super-cache."))
                    trees = tuple((name, capture_tree(rt, name))
                                  for name in ("super-cache", "super-cache.old", *names))
                    runtime_anchor.assert_stable()
            else:
                journal = TargetState.absent()
                trees = (("super-cache", None), ("super-cache.old", None))
            anchor.assert_stable()
        result = _CacheBoundary(rt, authority, identity, tuple(directories.items()), runtime_state, journal, trees)
        _revalidate_cache_boundary(result)
        return result
    except (OSError, ValueError, TypeError) as exc:
        raise SetupError(SETUP_CACHE_INVALID, f"cache boundary capture rejected: {exc}") from exc


def _revalidate_cache_boundary(boundary: _CacheBoundary) -> None:
    _check_root_targets(boundary.authority, boundary.authority_identity, boundary.directories, ())
    if not boundary.runtime_state.present:
        return
    with _DirectoryAnchor(boundary.runtime, boundary.runtime) as anchor:
        if anchor.root_identity != boundary.runtime_state.identity:
            raise MutationConflict("cache runtime changed after capture")
        _require_anchored_expected(anchor, CACHE_TRANSACTION_NAME, boundary.journal)
        actual = {name for name in os.listdir(anchor.parent_fd if os.name != "nt" else boundary.runtime)
                  if name.startswith(".super-cache.")}
        expected = {name for name, _ in boundary.trees if name.startswith(".super-cache.")}
        if actual != expected:
            raise MutationConflict("cache stage membership changed after capture")
        for _, tree in boundary.trees:
            if tree is not None:
                _revalidate_tree(tree)
        anchor.assert_stable()


def _prepare_cache_runtime(boundary: _CacheBoundary) -> _CacheBoundary:
    """Create only originally absent runtime ancestors with kernel NO_REPLACE."""
    _revalidate_cache_boundary(boundary)
    if boundary.runtime_state.present:
        return boundary
    states = dict(boundary.directories)
    for relative in sorted(states, key=lambda path: (len(path.parts), str(path))):
        if not states[relative].present:
            states[relative] = create_directory(
                boundary.authority, relative, expected_root_identity=boundary.authority_identity,
                expected_directories=_root_directory_chain(relative.parent, states))
    runtime_state = states[boundary.runtime.relative_to(boundary.authority)]
    trees = tuple((name, CapturedTree(boundary.runtime, Path(name), runtime_state.identity,
                                    ((Path(name), TargetState.absent()),))) for name, _ in boundary.trees)
    return _CacheBoundary(boundary.runtime, boundary.authority, boundary.authority_identity,
                          tuple(states.items()), runtime_state, boundary.journal, trees)


def _valid_durable_identity(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    fields = value.split(":")
    if len(fields) != 3 or fields[0] not in {"windows-file-id/v1", "posix-inode/v1"}:
        return False
    if not all(field and all(character in "0123456789abcdef" for character in field) for field in fields[1:]):
        return False
    return fields[0] != "windows-file-id/v1" or (len(fields[1]) == 16 and len(fields[2]) == 32)


def _cache_tree_proof(tree: CapturedTree, purpose: str) -> dict[str, Any]:
    entries = {}
    for path, state in tree.entries:
        if not state.present:
            raise MutationConflict("absent cache object cannot become an ownership proof")
        name = path.relative_to(tree.relative).as_posix()
        entries[name] = {"kind": state.kind,
                         "identity": durable_identity(tree.root, path, expected=state,
                                                       expected_root_identity=tree.root_identity),
                         "sha256": state.content_sha256}
    _revalidate_tree(tree)
    return {"purpose": purpose, "entries": entries}


def _valid_cache_object(value: Any) -> bool:
    if not isinstance(value, dict) or set(value) != {"purpose", "entries"} or value["purpose"] not in {"previous", "target"}:
        return False
    entries = value["entries"]
    if not isinstance(entries, dict) or not entries or "." not in entries:
        return False
    if len({name.casefold() for name in entries}) != len(entries):
        return False
    for name, entry in entries.items():
        if name != ".":
            try:
                safe_relative_path(name)
            except (OSError, ValueError, TypeError):
                return False
        if not isinstance(entry, dict) or set(entry) != {"kind", "identity", "sha256"}:
            return False
        if not _valid_durable_identity(entry["identity"]) or entry["kind"] not in {"directory", "file"}:
            return False
        digest = entry["sha256"]
        if entry["kind"] == "directory":
            if digest is not None:
                return False
        elif not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            return False
        if name != "." and (Path(name).parent.as_posix() not in entries
                            or entries[Path(name).parent.as_posix()]["kind"] != "directory"):
            return False
    return entries["."]["kind"] == "directory"


def _decode_cache_transaction(data: bytes, rt: Path) -> dict[str, Any]:
    """Use complete append snapshots; an incomplete final update grants no credit."""
    try:
        try:
            single = json.loads(data.decode("utf-8"))
        except (UnicodeError, ValueError):
            single = None
        records = [single] if isinstance(single, dict) else [
            json.loads(line.decode("utf-8")) for line in data.split(b"\n")[:-1]]
        if not records:
            raise ValueError("no complete cache transaction snapshot")
        previous_record = None
        for record in records:
            base = {"schema", "runtime_root", "transaction_id", "stage_name", "previous", "target"}
            legacy = record.get("schema") == LEGACY_CACHE_TRANSACTION_SCHEMA if isinstance(record, dict) else False
            fields = base if legacy else base | {"runtime_identity", "config_identity", "objects", "state", "error"}
            if not isinstance(record, dict) or set(record) != fields:
                raise ValueError("invalid closed cache transaction fields")
            transaction_id = record["transaction_id"]
            if (record["schema"] not in {CACHE_TRANSACTION_SCHEMA, LEGACY_CACHE_TRANSACTION_SCHEMA}
                    or not isinstance(record["runtime_root"], str)
                    or os.path.normcase(os.path.abspath(record["runtime_root"])) != os.path.normcase(os.path.abspath(rt))
                    or not isinstance(transaction_id, str) or len(transaction_id) != 32
                    or any(c not in "0123456789abcdef" for c in transaction_id)
                    or record["stage_name"] != f".super-cache.{transaction_id}"
                    or not _valid_manifest(record["target"])
                    or (record["previous"] is not None and not _valid_manifest(record["previous"]))):
                raise ValueError("invalid cache transaction identity or manifest")
            if not legacy:
                objects = record["objects"]
                if (not _valid_durable_identity(record["runtime_identity"])
                        or not isinstance(record["config_identity"], str) or len(record["config_identity"]) != 64
                        or any(c not in "0123456789abcdef" for c in record["config_identity"])
                        or not isinstance(objects, list) or len(objects) > 3
                        or not all(_valid_cache_object(value) for value in objects)
                        or len({value["entries"]["."]["identity"] for value in objects}) != len(objects)
                        or record["state"] not in {"building", "prepared", "promoting", "cleanup", "cleanup_unproven"}
                        or (record["error"] is not None and not isinstance(record["error"], str))):
                    raise ValueError("invalid cache transaction ownership or state")
            if previous_record is not None:
                immutable = base | {"runtime_identity", "config_identity"}
                if legacy or any(record.get(key) != previous_record.get(key) for key in immutable):
                    raise ValueError("cache transaction immutable fields changed in append history")
                newer = {value["entries"]["."]["identity"]: value for value in record["objects"]}
                for old in previous_record["objects"]:
                    new = newer.get(old["entries"]["."]["identity"])
                    if new is None or new["purpose"] != old["purpose"]:
                        raise ValueError("cache append history dropped original ownership")
                    if any(new["entries"].get(name) != entry for name, entry in old["entries"].items()):
                        raise ValueError("cache append history changed recorded ownership")
            previous_record = record
        return records[-1]
    except (UnicodeError, ValueError, TypeError, KeyError) as exc:
        raise SetupError(SETUP_CACHE_INVALID, f"invalid cache transaction {_transaction_path(rt)}: {exc}") from exc


def _read_cache_transaction(rt: Path, *, captured: _CacheBoundary | None = None) -> dict[str, Any] | None:
    rt = Path(os.path.abspath(rt))
    boundary = captured if captured is not None else _capture_cache_boundary(rt)
    if boundary.runtime != rt.absolute():
        raise SetupError(SETUP_CACHE_INVALID, "cache journal capture belongs to another runtime")
    return _decode_cache_transaction(boundary.journal.content, rt) if boundary.journal.present else None


def _tree_manifest(tree: CapturedTree | None) -> dict[str, Any] | None:
    if tree is None or not dict(tree.entries)[tree.relative].present:
        return None
    entries = {path.relative_to(tree.relative).as_posix(): state
               for path, state in tree.entries if path != tree.relative}
    return {"directories": sorted(name for name, state in entries.items() if state.kind == "directory"),
            "files": {name: state.content_sha256 for name, state in sorted(entries.items()) if state.kind == "file"}}


def _match_cache_objects(boundary: _CacheBoundary, record: dict[str, Any]) -> dict[str, tuple[CapturedTree, str]]:
    """Validate durable original IDs against the complete pre-validation census."""
    found = {}
    if record["schema"] == LEGACY_CACHE_TRANSACTION_SCHEMA:
        return found
    actual_root = durable_identity(boundary.runtime, ".", expected=boundary.runtime_state,
                                   expected_root_identity=boundary.runtime_state.identity)
    if record["runtime_identity"] != actual_root:
        raise SetupError(SETUP_CACHE_INVALID, "cache transaction runtime native identity changed")
    objects = {value["entries"]["."]["identity"]: value for value in record["objects"]}
    for name, tree in boundary.trees:
        if _tree_manifest(tree) is None:
            continue
        proof = _cache_tree_proof(tree, "target")
        original = objects.get(proof["entries"]["."]["identity"])
        if original is None:
            raise SetupError(SETUP_CACHE_INVALID, f"unregistered cache object is preserved: {boundary.runtime / name}")
        for relative, entry in proof["entries"].items():
            expected = original["entries"].get(relative)
            if expected is None or (entry["kind"], entry["identity"]) != (expected["kind"], expected["identity"]):
                raise SetupError(SETUP_CACHE_INVALID, f"cache object has an unknown birth: {boundary.runtime / name / relative}")
            if original["purpose"] == "previous" and entry["sha256"] != expected["sha256"]:
                raise SetupError(SETUP_CACHE_INVALID, f"prior cache object bytes changed: {boundary.runtime / name / relative}")
        if name == "super-cache.old" and original["purpose"] != "previous":
            raise SetupError(SETUP_CACHE_INVALID, "cache backup is not a registered prior object")
        if name.startswith(".super-cache.") and original["purpose"] != "target":
            raise SetupError(SETUP_CACHE_INVALID, "cache stage is not a registered target object")
        found[name] = (tree, original["purpose"])
    return found


def _journal_snapshot_bytes(record: dict[str, Any]) -> bytes:
    return (json.dumps(record, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


def _publish_cache_journal(rt: Path, record: dict[str, Any], expected: TargetState,
                           root_state: TargetState) -> TargetState:
    chain = ((Path("."), root_state),)
    with _DirectoryAnchor(rt, rt) as anchor:
        if anchor.root_identity != root_state.identity:
            raise MutationConflict("cache journal runtime changed before lock creation")
        with RecordLock(rt / "cache-transaction-journal", root=rt):
            if expected.present:
                return append_bytes(rt, CACHE_TRANSACTION_NAME, _journal_snapshot_bytes(record), expected=expected,
                                    expected_root_identity=root_state.identity, expected_directories=chain)
            return replace(rt, CACHE_TRANSACTION_NAME, _journal_snapshot_bytes(record), expected=expected,
                           expected_root_identity=root_state.identity, expected_directories=chain).resulting


def _delete_cache_journal(rt: Path, expected: TargetState, root_state: TargetState) -> None:
    with _DirectoryAnchor(rt, rt) as anchor:
        if anchor.root_identity != root_state.identity:
            raise MutationConflict("cache journal runtime changed")
        with RecordLock(rt / "cache-transaction-journal", root=rt):
            delete(rt, CACHE_TRANSACTION_NAME, expected=expected, expected_root_identity=root_state.identity)


def _rebase_cache_tree(tree: CapturedTree, relative: str | Path) -> CapturedTree:
    relative = safe_relative_path(relative)
    return CapturedTree(tree.root, relative, tree.root_identity,
                        tuple((relative / path.relative_to(tree.relative), state) for path, state in tree.entries))


def _move_cache_tree(tree: CapturedTree, target: str | Path) -> CapturedTree:
    """Move only this original object into an absent same-runtime slot."""
    target = safe_relative_path(target)
    with _DirectoryAnchor(tree.root, tree.root) as anchor:
        if anchor.root_identity != tree.root_identity:
            raise MutationConflict("cache rename runtime changed")
        _revalidate_tree(tree)
        receipt = rename(tree.root, tree.relative, target, expected_source=dict(tree.entries)[tree.relative],
                         expected_target=TargetState.absent(), expected_root_identity=tree.root_identity)
        if receipt.resulting.identity != dict(tree.entries)[tree.relative].identity:
            raise MutationConflict("cache rename resulting native identity changed")
        result = _rebase_cache_tree(tree, target)
        _revalidate_tree(result)
        return result


def _manifest_matches(path: Path, manifest: dict[str, Any] | None) -> bool:
    return manifest is not None and _path_present(path) and _manifest_for_tree(path) == manifest


def _require_exact_manifest(path: Path, manifest: dict[str, Any] | None) -> None:
    if not _manifest_matches(path, manifest):
        raise SetupError(SETUP_CACHE_INVALID, f"transaction backup differs from recorded prior cache: {path}")


def _check_owned_tree(path: Path, manifest: dict[str, Any], *, verify_bytes: bool,
                      captured: CapturedTree | None = None) -> CapturedTree:
    snapshot = captured if captured is not None else capture_tree(path.parent, path.name)
    if snapshot.root != path.parent.absolute() or snapshot.relative != Path(path.name):
        raise SetupError(SETUP_CACHE_INVALID, f"captured cache tree belongs to another path: {path}")
    entries = {entry.relative_to(snapshot.relative).as_posix(): state
               for entry, state in snapshot.entries if entry != snapshot.relative}
    directories = {name for name, state in entries.items() if state.kind == "directory"}
    files = {name for name, state in entries.items() if state.kind == "file"}
    if not directories <= set(manifest["directories"]) or not files <= set(manifest["files"]):
        raise SetupError(SETUP_CACHE_INVALID, f"transaction artifact has unknown paths: {path}")
    if verify_bytes:
        try:
            for name in files:
                if entries[name].content_sha256 != manifest["files"][name]:
                    raise SetupError(SETUP_CACHE_INVALID, f"transaction artifact has changed bytes: {path / name}")
        except OSError as exc:
            raise SetupError(SETUP_CACHE_INVALID, f"transaction artifact cannot be verified: {path}: {exc}") from exc
    return snapshot


def _remove_owned_tree(path: Path, manifest: dict[str, Any], *, verify_bytes: bool) -> None:
    snapshot = capture_tree(path.parent, path.name)
    _check_owned_tree(path, manifest, verify_bytes=verify_bytes, captured=snapshot)
    remove_tree(snapshot.root, snapshot.relative, captured=snapshot)


def _cache_plan_manifest(plan: list[tuple[Path, Path]], catalog: _CacheCatalog | None = None) -> dict[str, Any]:
    relatives = [safe_relative_path(relative) for _, relative in plan]
    aliases = [path.as_posix().casefold() for path in relatives]
    if len(set(aliases)) != len(aliases) or any(
            parent.as_posix().casefold() in aliases for path in relatives for parent in path.parents):
        raise SetupError(SETUP_ADAPTER_COLLISION, "cache plan has duplicate/aliased file or directory destinations")
    sources = ({source.path: source for source in catalog.sources} if catalog is not None else
               {path.absolute(): _capture_root_source(path) for path, _ in plan})
    return {"directories": sorted({parent.as_posix() for path in relatives for parent in path.parents
                                   if parent != Path(".")}),
            "files": {relative.as_posix(): sources[source.absolute()].state.content_sha256
                      for (source, _), relative in zip(plan, relatives)}}


def _cache_config_identity(catalog: _CacheCatalog) -> str:
    return compute_config_identity(load_config(catalog.harness), load_resource_manifest(catalog.harness))


def _inspect_active_cache(
    plan: list[tuple[Path, Path]], rt: Path, *, overwrite: bool, for_dispatch: bool = False,
    catalog: _CacheCatalog | None = None, captured: _CacheBoundary | None = None,
    config_identity: str | None = None,
) -> tuple[bool, dict[str, Any] | None]:
    """Validate original catalog/boundary snapshots without adopting later entries."""
    rt = Path(os.path.abspath(rt))
    boundary = captured if captured is not None else _capture_cache_boundary(rt)
    if boundary.runtime != rt.absolute():
        raise SetupError(SETUP_CACHE_INVALID, "cache boundary belongs to another runtime")
    target = _cache_plan_manifest(plan, catalog)
    transaction = _read_cache_transaction(rt, captured=boundary)
    physical = dict(boundary.trees)
    stages = [name for name in physical if name.startswith(".super-cache.")]
    backup = _tree_manifest(physical.get("super-cache.old"))
    live = _tree_manifest(physical.get("super-cache"))
    if transaction is None:
        if backup is not None or stages:
            names = (["super-cache.old"] if backup is not None else []) + stages
            paths = ", ".join(str(rt / name) for name in names)
            raise SetupError(SETUP_CACHE_INVALID, f"unowned cache backup or stage needs resolution: {paths}",
                             next_action=f"inspect and preserve bytes at {paths}; after confirming ownership, move each artifact outside the runtime root and re-run setup")
        if live is not None:
            unknown = (set(live["directories"]) - set(target["directories"])) | (set(live["files"]) - set(target["files"]))
            if unknown:
                paths = ", ".join(str(rt / "super-cache" / name) for name in sorted(unknown))
                raise SetupError(SETUP_CACHE_INVALID, f"active cache contains unknown paths: {paths}",
                                 next_action=f"inspect and preserve any needed bytes at {paths}; resolve unknown paths before re-running setup")
            if live != target and not overwrite:
                raise SetupError(SETUP_CACHE_INVALID, f"active cache has missing or changed planned bytes: {rt / 'super-cache'}")
        return live == target, None
    if for_dispatch:
        raise SetupError(SETUP_CACHE_INVALID, f"cache transaction needs setup recovery: {rt}")
    extra = [name for name in stages if name != transaction["stage_name"]]
    if extra:
        raise SetupError(SETUP_CACHE_INVALID, f"unowned cache stage needs resolution: {', '.join(str(rt / name) for name in extra)}",
                         next_action="inspect and preserve the named unowned artifacts; resolve their ownership outside the runtime before re-running setup")
    previous = transaction["previous"]
    recorded_target = transaction["target"]
    live_target = live is not None and live == recorded_target
    live_previous = live is not None and live == previous
    if transaction["schema"] == LEGACY_CACHE_TRANSACTION_SCHEMA:
        if backup is not None or stages:
            raise SetupError(SETUP_CACHE_INVALID, f"legacy cache journal has no native birth proof; preserve artifacts at {rt}")
        if not (live_target or live_previous or (previous is None and live is None)):
            raise SetupError(SETUP_CACHE_INVALID, f"legacy cache transaction is ambiguous: {rt}")
        return False, transaction
    current_identity = config_identity if config_identity is not None else (
        _cache_config_identity(catalog) if catalog is not None else None)
    if current_identity is not None and transaction["config_identity"] != current_identity:
        raise SetupError(SETUP_CACHE_INVALID, "cache transaction configuration identity differs from the admitted runtime")
    known = _match_cache_objects(boundary, transaction)
    if backup is not None:
        if previous is None or not (backup == previous or live_target):
            raise SetupError(SETUP_CACHE_INVALID, f"partial cache backup lacks a valid promoted cache: {rt / 'super-cache.old'}")
        if live is not None and not (live_target or live_previous) and known["super-cache"][1] != "target":
            raise SetupError(SETUP_CACHE_INVALID, f"transaction live cache is ambiguous: {rt / 'super-cache'}")
    elif live is not None and not (live_target or live_previous):
        raise SetupError(SETUP_CACHE_INVALID, f"transaction live cache is ambiguous: {rt / 'super-cache'}")
    elif live is None and previous is not None:
        raise SetupError(SETUP_CACHE_INVALID, f"transaction prior cache is missing: {rt / 'super-cache'}")
    return False, transaction


def _recover_cache_transaction(
    rt: Path, transaction: dict[str, Any], *, captured: _CacheBoundary | None = None,
) -> None:
    """Reconcile only original native births, preserving unknown replacements."""
    rt = Path(os.path.abspath(rt))
    boundary = captured if captured is not None else _capture_cache_boundary(rt)
    if _read_cache_transaction(rt, captured=boundary) != transaction:
        raise SetupError(SETUP_CACHE_INVALID, "cache recovery journal differs from the captured transaction")
    _revalidate_cache_boundary(boundary)
    if transaction["schema"] == LEGACY_CACHE_TRANSACTION_SCHEMA:
        physical = dict(boundary.trees)
        if any(name.startswith(".super-cache.") or (
                name == "super-cache.old" and _tree_manifest(tree) is not None) for name, tree in boundary.trees):
            raise SetupError(SETUP_CACHE_INVALID, "legacy cache transaction has no native artifact birth proof")
        live = _tree_manifest(physical.get("super-cache"))
        if not (live in (transaction["previous"], transaction["target"])
                or (live is None and transaction["previous"] is None)):
            raise SetupError(SETUP_CACHE_INVALID, "legacy cache transaction cannot be safely cleared")
        _delete_cache_journal(rt, boundary.journal, boundary.runtime_state)
        return
    known = _match_cache_objects(boundary, transaction)
    stage_name = transaction["stage_name"]
    stage = known.get(stage_name)
    backup = known.get("super-cache.old")
    live = known.get("super-cache")
    live_manifest = _tree_manifest(live[0]) if live else None
    valid_target = live_manifest is not None and live_manifest == transaction["target"]
    valid_previous = live_manifest is not None and live_manifest == transaction["previous"]
    if backup and not (_tree_manifest(backup[0]) == transaction["previous"] or valid_target):
        raise SetupError(SETUP_CACHE_INVALID, "partial cache backup cannot be recovered")
    if stage:
        remove_tree(stage[0].root, stage[0].relative, captured=stage[0])
    if backup and not (valid_target or valid_previous):
        if live:
            isolated = _move_cache_tree(live[0], stage_name)
            remove_tree(isolated.root, isolated.relative, captured=isolated)
        _move_cache_tree(backup[0], "super-cache")
    elif backup:
        remove_tree(backup[0].root, backup[0].relative, captured=backup[0])
    # Revalidate all remaining original files and the original journal at last use.
    expected_live = (live[0] if valid_target or valid_previous else
                     _rebase_cache_tree(backup[0], "super-cache") if backup else None)
    if expected_live is not None:
        _revalidate_tree(expected_live)
    elif transaction["previous"] is not None:
        raise SetupError(SETUP_CACHE_INVALID, "cache recovery lost the recorded prior cache")
    for name in (stage_name, "super-cache.old"):
        if capture_target(rt, name).present:
            raise SetupError(SETUP_CACHE_INVALID, f"cache transaction artifact remains unresolved: {rt / name}")
    _delete_cache_journal(rt, boundary.journal, boundary.runtime_state)


def _replace_tree(
    staging: Path, destination: Path, *, validate: Callable[[Path], None] | None = None,
    validate_backup: Callable[[Path], None] | None = None,
    captured_stage: CapturedTree | None = None, captured_destination: CapturedTree | None = None,
) -> None:
    """Promote original complete trees; rollback only their original native IDs."""
    staging = Path(os.path.abspath(staging))
    destination = Path(os.path.abspath(destination))
    if staging.parent.absolute() != destination.parent.absolute():
        raise SetupError(SETUP_CACHE_INVALID, "cache promotion must use one runtime authority")
    stage = captured_stage if captured_stage is not None else capture_tree(staging.parent, staging.name)
    prior = captured_destination if captured_destination is not None else capture_tree(destination.parent, destination.name)
    if (stage.root != staging.parent or stage.relative != Path(staging.name)
            or prior.root != destination.parent or prior.relative != Path(destination.name)):
        raise SetupError(SETUP_CACHE_INVALID, "cache promotion capture belongs to another path")
    _revalidate_tree(stage); _revalidate_tree(prior)
    had_destination = dict(prior.entries)[prior.relative].present
    backup_name = destination.name + ".old"
    moved_prior = None
    promoted = None
    try:
        if had_destination:
            moved_prior = _move_cache_tree(prior, backup_name)
        promoted = _move_cache_tree(stage, destination.name)
        if validate is not None:
            validate(destination)
        _revalidate_tree(promoted)
    except BaseException as failure:
        # A native move can succeed before its caller reports a failure.
        # Rebased original proofs cover that outcome; foreign objects never do.
        try:
            if capture_target(stage.root, destination.name).identity == dict(stage.entries)[stage.relative].identity:
                promoted = _rebase_cache_tree(stage, destination.name)
            if had_destination and capture_target(prior.root, backup_name).identity == dict(prior.entries)[prior.relative].identity:
                moved_prior = _rebase_cache_tree(prior, backup_name)
            if promoted is not None:
                _move_cache_tree(promoted, staging.name)
            if moved_prior is not None:
                _move_cache_tree(moved_prior, destination.name)
        except BaseException as rollback:
            raise _CacheRecoveryRequired(SETUP_OVERWRITE_FAILED, f"active cache rollback needs recovery at {destination}: {rollback}") from failure
        raise
    if moved_prior is not None:
        try:
            if validate_backup is not None:
                validate_backup(destination.with_name(backup_name))
            _revalidate_tree(moved_prior)
            remove_tree(moved_prior.root, moved_prior.relative, captured=moved_prior)
        except (OSError, ValueError, TypeError) as cleanup:
            # Keep the original mutation/config exception contract for callers.
            # The installer still defers another attempt until explicit setup.
            setattr(cleanup, "_cache_recovery_required", True)
            raise


def _install_active_cache(
    harness_root: Path, rt: Path, *, plan: list[tuple[Path, Path]], overwrite: bool,
    catalog: _CacheCatalog | None = None, captured: _CacheBoundary | None = None,
    config_identity: str | None = None,
) -> None:
    """Publish captured bytes and recover only durably recorded native objects."""
    harness_root = Path(os.path.abspath(harness_root))
    rt = Path(os.path.abspath(rt))
    catalog = catalog if catalog is not None else _capture_cache_catalog(harness_root)
    boundary = captured if captured is not None else _capture_cache_boundary(rt)
    if catalog.harness != harness_root.absolute() or boundary.runtime != rt.absolute():
        raise SetupError(SETUP_CACHE_INVALID, "cache capture belongs to another harness/runtime")
    identity = config_identity if config_identity is not None else _cache_config_identity(catalog)
    expected_plan = _plan_active_cache(harness_root, catalog=catalog)
    if [(p.absolute(), r) for p, r in plan] != [(p.absolute(), r) for p, r in expected_plan]:
        raise SetupError(SETUP_CACHE_INVALID, "cache plan differs from the original captured catalog")
    try:
        _revalidate_cache_catalog(catalog)
        live_valid, transaction = _inspect_active_cache(plan, rt, overwrite=overwrite, catalog=catalog,
                                                        captured=boundary, config_identity=identity)
        boundary = _prepare_cache_runtime(boundary)
        if transaction is not None:
            _recover_cache_transaction(rt, transaction, captured=boundary)
            boundary = _capture_cache_boundary(rt)
            live_valid, _ = _inspect_active_cache(plan, rt, overwrite=overwrite, catalog=catalog,
                                                  captured=boundary, config_identity=identity)
        if live_valid:
            _revalidate_cache_catalog(catalog); _revalidate_cache_boundary(boundary)
            return
        _revalidate_cache_boundary(boundary)
        rt = boundary.runtime
        root_state = boundary.runtime_state
        prior = dict(boundary.trees)["super-cache"]
        previous = _tree_manifest(prior)
        target = _cache_plan_manifest(plan, catalog)
        transaction_id = uuid.uuid4().hex
        stage_name = f".super-cache.{transaction_id}"
        if capture_target(rt, stage_name).present:
            raise MutationConflict("allocated cache stage already exists")
        transaction = {"schema": CACHE_TRANSACTION_SCHEMA, "runtime_root": str(rt),
                       "runtime_identity": durable_identity(rt, ".", expected=root_state,
                                                            expected_root_identity=root_state.identity),
                       "config_identity": identity, "transaction_id": transaction_id, "stage_name": stage_name,
                       "previous": previous, "target": target,
                       "objects": [_cache_tree_proof(prior, "previous")] if previous is not None else [],
                       "state": "building", "error": None}
        journal = _publish_cache_journal(rt, transaction, boundary.journal, root_state)
        stage_entries = {}
        promotion_started = False
        try:
            state = create_directory(rt, stage_name, expected_root_identity=root_state.identity,
                                     expected_directories=((Path("."), root_state),))
            stage_entries[Path(stage_name)] = state
            staged_object = {"purpose": "target", "entries": {".": {
                "kind": "directory", "identity": durable_identity(rt, stage_name, expected=state,
                                                                expected_root_identity=root_state.identity),
                "sha256": None}}}
            transaction["objects"].append(staged_object)
            journal = _publish_cache_journal(rt, transaction, journal, root_state)
            directories = {Path("."): root_state, Path(stage_name): state}
            sources = {source.path: source for source in catalog.sources}
            for source_path, relative in plan:
                source = sources[source_path.absolute()]
                _revalidate_root_source(source)
                relative = safe_relative_path(relative)
                current = Path(stage_name)
                for part in relative.parent.parts:
                    current /= part
                    if current not in directories:
                        created = create_directory(rt, current, expected_root_identity=root_state.identity,
                                                   expected_directories=_root_directory_chain(current.parent, directories))
                        directories[current] = stage_entries[current] = created
                        staged_object["entries"][current.relative_to(stage_name).as_posix()] = {
                            "kind": "directory", "identity": durable_identity(rt, current, expected=created,
                                                                             expected_root_identity=root_state.identity),
                            "sha256": None}
                        journal = _publish_cache_journal(rt, transaction, journal, root_state)
                selected = Path(stage_name) / relative
                receipt = replace(rt, selected, source.state.content, expected=TargetState.absent(),
                                  expected_root_identity=root_state.identity,
                                  expected_directories=_root_directory_chain(selected.parent, directories),
                                  metadata=source.metadata)
                stage_entries[selected] = receipt.resulting
                staged_object["entries"][relative.as_posix()] = {
                    "kind": "file", "identity": durable_identity(rt, selected, expected=receipt.resulting,
                                                               expected_root_identity=root_state.identity),
                    "sha256": receipt.resulting.content_sha256}
                journal = _publish_cache_journal(rt, transaction, journal, root_state)
                _revalidate_root_source(source)
            stage = CapturedTree(rt, Path(stage_name), root_state.identity, tuple(sorted(stage_entries.items())))
            _revalidate_tree(stage)
            _revalidate_cache_catalog(catalog)
            if _tree_manifest(stage) != target:
                raise SetupError(SETUP_CACHE_INVALID, "staged cache differs from the captured target")
            transaction["state"] = "prepared"
            journal = _publish_cache_journal(rt, transaction, journal, root_state)
            transaction["state"] = "promoting"
            journal = _publish_cache_journal(rt, transaction, journal, root_state)
            promotion_started = True
            _replace_tree(rt / stage_name, rt / "super-cache", captured_stage=stage, captured_destination=prior,
                          validate=lambda path: _validate_active_cache(plan, path, catalog=catalog),
                          validate_backup=lambda path: _require_exact_manifest(path, previous))
            transaction["state"] = "cleanup"
            journal = _publish_cache_journal(rt, transaction, journal, root_state)
            _revalidate_tree(_rebase_cache_tree(stage, "super-cache"))
            _revalidate_cache_catalog(catalog)
            _delete_cache_journal(rt, journal, root_state)
        except BaseException as failure:
            needs_recovery = (isinstance(failure, _CacheRecoveryRequired)
                              or getattr(failure, "_cache_recovery_required", False))
            if stage_entries and not promotion_started:
                try:
                    original_stage = CapturedTree(rt, Path(stage_name), root_state.identity,
                                                  tuple(sorted(stage_entries.items())))
                    remove_tree(rt, stage_name, captured=original_stage)
                except (OSError, ValueError, TypeError):
                    needs_recovery = True
            try:
                transaction["state"] = "cleanup_unproven"; transaction["error"] = str(failure)
                journal = _publish_cache_journal(rt, transaction, journal, root_state)
                if not needs_recovery:
                    recovery = _capture_cache_boundary(rt)
                    _inspect_active_cache(plan, rt, overwrite=True, catalog=catalog, captured=recovery,
                                          config_identity=identity)
                    _recover_cache_transaction(rt, transaction, captured=recovery)
            except (OSError, ValueError, TypeError, SetupError):
                # Retain original records/artifacts; never erase uncertainty.
                pass
            raise failure
    except SetupError:
        raise
    except (OSError, ValueError, TypeError) as exc:
        raise SetupError(SETUP_OVERWRITE_FAILED, f"active cache transaction failed: {exc}") from exc


def _plan_root_payloads(harness_root: Path, *, catalog: _CacheCatalog | None = None) -> list[tuple[Path, Path]]:
    """Plan every shipped adapter's ROOT payload, relative to the root
    workspace."""
    harness_root = Path(os.path.abspath(harness_root))
    adapters_dir = harness_root / "adapters"
    if not _catalog_is_dir(adapters_dir, catalog):
        raise SetupError(SETUP_CONFIG_INVALID, f"adapter catalog missing: {adapters_dir}")
    planned: list[tuple[Path, Path]] = []
    for adapter in _catalog_adapters(adapters_dir, catalog):
        if not _catalog_is_dir(adapter, catalog):
            continue
        root_payload = adapter / "root"
        if not _catalog_is_dir(root_payload, catalog):
            continue
        planned.extend(_catalog_tree(root_payload, catalog))
    return planned


def _materialized_binding(
    template: dict[str, Any],
    harness_root: Path,
    runtime_root: Path,
) -> dict[str, Any]:
    """Return the shipped binding template bound to this exact runtime."""
    bound = dict(template)
    bound["harness_root"] = str(harness_root.resolve())
    bound["runtime_root"] = str(runtime_root.resolve())
    config = load_config(harness_root)
    if path_identity(config.runtime_root) != path_identity(runtime_root):
        raise SetupError(SETUP_CONFIG_INVALID, "ROOT binding runtime differs from configured product runtime")
    bound["config_identity"] = compute_config_identity(config, load_resource_manifest(harness_root))
    bound["manager_workspace"] = path_identity(config.manager_workspace)
    bound["root_workspace"] = path_identity(config.root_workspace)
    return bound




def _read_json_object(path: Path, *, description: str) -> dict[str, Any]:
    try:
        value = read_json(path)
    except (OSError, ValueError) as exc:
        raise SetupError(
            SETUP_ADAPTER_COLLISION, f"{description} is unreadable: {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise SetupError(
            SETUP_ADAPTER_COLLISION, f"{description} must be a JSON object: {path}"
        )
    return value


def _hook_commands(group: Any) -> set[str]:
    if not isinstance(group, dict):
        return set()
    hooks = group.get("hooks")
    if not isinstance(hooks, list):
        return set()
    return {
        hook["command"]
        for hook in hooks
        if isinstance(hook, dict) and isinstance(hook.get("command"), str)
    }


def _merge_root_hook_config(source: Path, target: Path) -> dict[str, Any]:
    """Add shipped harness hook groups to an existing provider config.

    Existing top-level fields, permissions, event order, and hook groups remain
    authoritative.  A shipped command that is already present with a changed
    group is treated as a collision instead of being duplicated or overwritten.
    """
    shipped = _read_json_object(source, description="shipped hook configuration")
    existing = _read_json_object(target, description="existing hook configuration")
    return _merge_root_hook_objects(shipped, existing, source=source, target=target)


def _merge_root_hook_objects(
    shipped: dict[str, Any], existing: dict[str, Any], *, source: Path, target: Path,
) -> dict[str, Any]:
    """Merge only the original captured configuration objects."""
    shipped_hooks = shipped.get("hooks")
    if not isinstance(shipped_hooks, dict):
        raise SetupError(
            SETUP_CONFIG_INVALID,
            f"shipped hook configuration has no hooks object: {source}",
        )
    existing_hooks = existing.get("hooks")
    if existing_hooks is None:
        existing_hooks = {}
    if not isinstance(existing_hooks, dict):
        raise SetupError(
            SETUP_ADAPTER_COLLISION,
            f"existing hook configuration has a non-object hooks field: {target}",
        )

    merged = deepcopy(existing)
    merged_hooks = deepcopy(existing_hooks)
    merged["hooks"] = merged_hooks
    existing_commands = {
        command
        for groups in existing_hooks.values()
        if isinstance(groups, list)
        for group in groups
        for command in _hook_commands(group)
    }
    for event_name, shipped_groups in shipped_hooks.items():
        if not isinstance(event_name, str) or not isinstance(shipped_groups, list):
            raise SetupError(
                SETUP_CONFIG_INVALID,
                f"shipped hook event is malformed in {source}",
            )
        existing_groups = merged_hooks.get(event_name)
        if existing_groups is None:
            existing_groups = []
            merged_hooks[event_name] = existing_groups
        if not isinstance(existing_groups, list):
            raise SetupError(
                SETUP_ADAPTER_COLLISION,
                f"existing hook event {event_name!r} is not a list: {target}",
            )
        for shipped_group in shipped_groups:
            if not isinstance(shipped_group, dict) or not _hook_commands(shipped_group):
                raise SetupError(
                    SETUP_CONFIG_INVALID,
                    f"shipped hook group is malformed in {source}",
                )
            if shipped_group in existing_groups:
                continue
            shipped_commands = _hook_commands(shipped_group)
            if shipped_commands.intersection(existing_commands):
                raise SetupError(
                    SETUP_ADAPTER_COLLISION,
                    f"existing hook configuration changes a harness-owned "
                    f"{event_name!r} command: {target}",
                )
            existing_groups.append(deepcopy(shipped_group))
    return merged


def _validate_existing_codex_config(target: Path) -> None:
    """Preserve an existing Codex config after proving hooks are enabled."""
    try:
        data = target.read_bytes()
    except OSError as exc:
        raise SetupError(SETUP_ADAPTER_COLLISION,
                         f"existing Codex configuration is unreadable: {target}: {exc}") from exc
    _validate_captured_codex_config(target, data)


def _validate_captured_codex_config(target: Path, data: bytes) -> None:
    try:
        value = tomllib.loads(data.decode("utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise SetupError(
            SETUP_ADAPTER_COLLISION,
            f"existing Codex configuration is unreadable: {target}: {exc}",
        ) from exc
    features = value.get("features")
    if not isinstance(features, dict) or features.get("hooks") is not True:
        raise SetupError(
            SETUP_ADAPTER_COLLISION,
            f"existing Codex configuration must enable [features] hooks = true: {target}",
        )




@dataclass(frozen=True)
class _RootPayloadSource:
    path: Path
    parent_identity: tuple[int, int]
    state: TargetState
    metadata: FileMetadata


@dataclass(frozen=True)
class _RootPayloadFile:
    relative: Path
    source: _RootPayloadSource
    target: TargetState


@dataclass(frozen=True)
class _RootPayloadCapture:
    workspace: Path
    authority: Path
    authority_identity: tuple[int, int]
    directories: DirectoryExpectations
    files: tuple[_RootPayloadFile, ...]


@dataclass(frozen=True)
class _RootPayloadAction:
    file: _RootPayloadFile
    data: bytes | None
    metadata: FileMetadata | None
    merged: bool = False


@dataclass(frozen=True)
class _RootPayloadPlan:
    capture: _RootPayloadCapture
    overwrite: bool
    actions: tuple[_RootPayloadAction, ...]


@dataclass(frozen=True)
class _RootPublication:
    workspace: Path
    authority: Path
    authority_identity: tuple[int, int]
    directories: DirectoryExpectations
    files: tuple[tuple[Path, TargetState], ...]


def _root_directory_chain(relative: Path, directories: dict[Path, TargetState]) -> DirectoryExpectations:
    current = Path(".")
    chain = [current]
    for part in relative.parts:
        current /= part
        chain.append(current)
    return tuple((path, directories[path]) for path in chain)


def _capture_root_source(source: Path) -> _RootPayloadSource:
    source = source.absolute()
    safe_relative_path(source.name)
    with _DirectoryAnchor(source.parent, source.parent) as anchor:
        state = _capture_anchored(anchor, source.name, include_content=True)
        if not state.present or state.kind != "file":
            raise MutationConflict(f"ROOT payload source is not a plain file: {source}")
        info = anchor.stat(source.name)
        metadata = FileMetadata(stat.S_IMODE(info.st_mode), info.st_atime_ns, info.st_mtime_ns)
        _require_anchored_expected(anchor, source.name, state)
        return _RootPayloadSource(source, anchor.root_identity, state, metadata)


def _revalidate_root_source(source: _RootPayloadSource) -> None:
    with _DirectoryAnchor(source.path.parent, source.path.parent) as anchor:
        if anchor.root_identity != source.parent_identity:
            raise MutationConflict(f"ROOT payload source parent changed: {source.path.parent}")
        _require_anchored_expected(anchor, source.path.name, source.state)
        info = anchor.stat(source.path.name)
        if (stat.S_IMODE(info.st_mode), info.st_mtime_ns) != (source.metadata.mode, source.metadata.mtime_ns):
            raise MutationConflict(f"ROOT payload source metadata changed: {source.path}")
        _require_anchored_expected(anchor, source.path.name, source.state)


def _check_root_targets(
    authority: Path, identity: tuple[int, int], directories: DirectoryExpectations,
    files: tuple[tuple[Path, TargetState], ...],
) -> None:
    states = dict(directories)
    with _DirectoryAnchor(authority, authority) as anchor:
        if anchor.root_identity != identity:
            raise MutationConflict("ROOT payload publication authority changed")
        for relative, state in sorted(states.items(), key=lambda item: (len(item[0].parts), str(item[0]))):
            if relative == Path("."):
                if state.identity != identity or state.kind != "directory":
                    raise MutationConflict("ROOT payload authority proof is inconsistent")
                continue
            parent = states[relative.parent]
            if parent.present:
                if not _same_state(capture_target(authority, relative), state):
                    raise MutationConflict(f"ROOT payload directory changed: {authority / relative}")
            elif state.present:
                raise MutationConflict("ROOT payload directory capture is inconsistent")
        for relative, state in files:
            if states[relative.parent].present:
                with _DirectoryAnchor(authority, (authority / relative).parent) as parent_anchor:
                    _require_anchored_directories(parent_anchor, _root_directory_chain(relative.parent, states))
                    _require_anchored_expected(parent_anchor, relative.name, state)
            elif state.present:
                raise MutationConflict("ROOT payload file capture is inconsistent")
        anchor.assert_stable()


def _capture_root_payloads(
    plan: list[tuple[Path, Path]], root_workspace: Path,
    *, sources: dict[Path, _RootPayloadSource] | None = None,
) -> _RootPayloadCapture:
    """Capture the complete selected source/destination state before validation."""
    workspace = root_workspace.absolute()
    try:
        relatives = [safe_relative_path(relative) for _, relative in plan]
        aliases = [relative.as_posix().casefold() for relative in relatives]
        if len(set(aliases)) != len(aliases):
            raise MutationConflict("catalog payload collision at duplicate or aliased destination")
        spellings: dict[str, Path] = {}
        for relative in relatives:
            for path in (relative, *relative.parents):
                key = path.as_posix().casefold()
                if key in spellings and spellings[key] != path:
                    raise MutationConflict("catalog payload paths use aliased directory spellings")
                spellings[key] = path
            if any(parent.as_posix().casefold() in aliases for parent in relative.parents):
                raise MutationConflict("catalog payload file collides with a required directory")
        authority = workspace
        while not os.path.lexists(authority):
            authority = authority.parent
        with _DirectoryAnchor(authority, authority) as anchor:
            identity = anchor.root_identity
            directories = {Path("."): TargetState(True, "directory", identity, None, None)}
            files = []
            for (source, _), relative in zip(plan, relatives):
                if sources is None:
                    captured_source = _capture_root_source(source)
                else:
                    captured_source = sources.get(source.absolute())
                    if (not isinstance(captured_source, _RootPayloadSource)
                            or captured_source.path != source.absolute()):
                        raise MutationConflict(f"missing or misbound ROOT source capture: {source}")
                target_relative = (workspace / relative).relative_to(authority)
                current = Path(".")
                for part in target_relative.parent.parts:
                    parent = current
                    current /= part
                    if current not in directories:
                        state = (capture_target(authority, current) if directories[parent].present
                                 else TargetState.absent())
                        if state.present and state.kind != "directory":
                            raise MutationConflict(f"ROOT payload parent is not a directory: {authority / current}")
                        directories[current] = state
                target = (capture_target(authority, target_relative)
                          if directories[target_relative.parent].present else TargetState.absent())
                if target.present and target.kind != "file":
                    raise MutationConflict(f"ROOT payload destination is not a plain file: {workspace / relative}")
                files.append(_RootPayloadFile(relative, captured_source, target))
            anchor.assert_stable()
        result = _RootPayloadCapture(workspace, authority, identity, tuple(directories.items()), tuple(files))
        _check_root_targets(authority, identity, result.directories,
                            tuple(((workspace / file.relative).relative_to(authority), file.target) for file in files))
        for file in files:
            _revalidate_root_source(file.source)
        return result
    except (OSError, ValueError, TypeError) as exc:
        raise SetupError(SETUP_ADAPTER_COLLISION, f"ROOT payload capture rejected: {exc}") from exc


def _require_root_capture(
    captured: _RootPayloadCapture, plan: list[tuple[Path, Path]], workspace: Path,
) -> None:
    if captured.workspace != workspace.absolute() or tuple(
        (file.source.path, file.relative) for file in captured.files
    ) != tuple((source.absolute(), safe_relative_path(relative)) for source, relative in plan):
        raise MutationConflict("ROOT payload capture belongs to a different workspace or catalog")


def _captured_json_object(data: bytes, path: Path, *, description: str) -> dict[str, Any]:
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise SetupError(SETUP_ADAPTER_COLLISION, f"{description} is unreadable: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SetupError(SETUP_ADAPTER_COLLISION, f"{description} must be a JSON object: {path}")
    return value


def _render_root_json(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def _preflight_root_payloads(
    plan: list[tuple[Path, Path]],
    root_workspace: Path,
    *,
    overwrite: bool,
    harness_root: Path | None = None,
    runtime_root: Path | None = None,
    binding_templates: dict[Path, dict[str, Any]] | None = None,
    captured: _RootPayloadCapture | None = None,
) -> _RootPayloadPlan:
    """Render original captured bytes, preserving shared configs and valid installs."""
    snapshot = captured if captured is not None else _capture_root_payloads(plan, root_workspace)
    try:
        _require_root_capture(snapshot, plan, root_workspace)
        actions = []
        sources = {file.source.path for file in snapshot.files}
        for file in snapshot.files:
            target = snapshot.workspace / file.relative
            data = file.source.state.content
            metadata = file.source.metadata
            merged = False
            if file.target.present:
                if file.relative == CODEX_ROOT_CONFIG:
                    _validate_captured_codex_config(target, file.target.content)
                    data = None
                elif file.relative in SHARED_ROOT_HOOK_CONFIGS:
                    shipped = _captured_json_object(data, file.source.path, description="shipped hook configuration")
                    current = _captured_json_object(file.target.content, target, description="existing hook configuration")
                    combined = _merge_root_hook_objects(shipped, current, source=file.source.path, target=target)
                    merged = combined != current
                    data = _render_root_json(combined) if merged else None
                    metadata = None
                elif not overwrite:
                    valid = file.target.content == data
                    template = (binding_templates or {}).get(root_workspace / file.relative)
                    if not valid and template is not None and harness_root is not None and runtime_root is not None:
                        try:
                            valid = json.loads(file.target.content.decode("utf-8")) == _materialized_binding(
                                template, harness_root, runtime_root)
                        except (UnicodeDecodeError, ValueError):
                            valid = False
                    if valid:
                        data = None
                    else:
                        raise SetupError(SETUP_ADAPTER_COLLISION,
                                         f"destination collision (re-run with --overwrite to replace): {target}")
            if data is not None and target in sources:
                raise MutationConflict("ROOT payload publication would mutate its observed source")
            actions.append(_RootPayloadAction(file, data, metadata, merged))
        return _RootPayloadPlan(snapshot, overwrite, tuple(actions))
    except (OSError, ValueError, TypeError) as exc:
        raise SetupError(SETUP_ADAPTER_COLLISION, f"ROOT payload preflight rejected: {exc}") from exc


def _plan_root_hook_bindings(
    harness_root: Path,
    plan: list[tuple[Path, Path]],
    *,
    captured_sources: dict[Path, bytes] | None = None,
) -> list[tuple[str, Path, dict[str, Any]]]:
    """Validate portable ROOT hook bindings before setup writes anything."""
    adapters_dir = harness_root / "adapters"
    provider_files: dict[str, list[tuple[Path, Path]]] = {}
    for source, relative in plan:
        try:
            source_parts = source.relative_to(adapters_dir).parts
        except ValueError as exc:
            raise SetupError(
                SETUP_CONFIG_INVALID,
                f"ROOT payload source is outside the adapter catalog: {source}",
            ) from exc
        if len(source_parts) < 3 or source_parts[1] != "root":
            raise SetupError(
                SETUP_CONFIG_INVALID,
                f"ROOT payload source has an invalid adapter location: {source}",
            )
        provider_files.setdefault(source_parts[0], []).append((source, relative))

    bindings: list[tuple[str, Path, dict[str, Any]]] = []
    for provider_id, files in sorted(provider_files.items()):
        has_python_hook = any(
            source.suffix == ".py" and "hooks" in relative.parts
            for source, relative in files
        )
        if not has_python_hook:
            continue
        candidates = [
            (source, relative)
            for source, relative in files
            if relative.name == ROOT_HOOK_BINDING_NAME
        ]
        if len(candidates) != 1:
            raise SetupError(
                SETUP_CONFIG_INVALID,
                f"installed {provider_id} hook has no harness binding",
            )
        source, relative = candidates[0]
        try:
            record = (read_json(source) if captured_sources is None
                      else json.loads(captured_sources[source.absolute()].decode("utf-8")))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, KeyError) as exc:
            raise SetupError(
                SETUP_CONFIG_INVALID,
                f"ROOT hook binding for {provider_id} is unreadable: {exc}",
            ) from exc
        if not isinstance(record, dict):
            raise SetupError(
                SETUP_CONFIG_INVALID,
                f"ROOT hook binding for {provider_id} must be a JSON object",
            )
        expected = {
            "schema": ROOT_HOOK_BINDING_SCHEMA,
            "role": "root",
            "provider_id": provider_id,
            "harness_root": None,
            "runtime_root": None,
        }
        for field, value in expected.items():
            if record.get(field) != value:
                raise SetupError(
                    SETUP_CONFIG_INVALID,
                    f"ROOT hook binding for {provider_id} has invalid {field}",
                )
        bindings.append((provider_id, relative, record))
    return bindings


def _materialize_root_hook_bindings(
    root_workspace: Path,
    harness_root: Path,
    runtime_root: Path,
    bindings: list[tuple[str, Path, dict[str, Any]]],
    *,
    publication: _RootPublication | None = None,
) -> list[Path]:
    """Bind the original installed identities, refusing later substitutions."""
    if publication is None:
        try:
            capture = _capture_root_payloads(
                [(root_workspace / relative, relative) for _, relative, _ in bindings], root_workspace)
        except SetupError as exc:
            raise SetupError(SETUP_CONFIG_INVALID, f"installed ROOT hook has no harness binding: {exc}") from exc
        publication = _RootPublication(
            capture.workspace, capture.authority, capture.authority_identity, capture.directories,
            tuple(((capture.workspace / file.relative).relative_to(capture.authority), file.target)
                  for file in capture.files))
    try:
        if publication.workspace != root_workspace.absolute():
            raise MutationConflict("ROOT binding publication belongs to a different workspace")
        _check_root_targets(publication.authority, publication.authority_identity,
                            publication.directories, publication.files)
        states = dict(publication.files)
        directories = dict(publication.directories)
        actions = []
        seen = set()
        for provider_id, relative, template in bindings:
            relative = safe_relative_path(relative)
            if relative.as_posix().casefold() in seen:
                raise MutationConflict("duplicate or aliased ROOT binding destination")
            seen.add(relative.as_posix().casefold())
            target = publication.workspace / relative
            selected = target.relative_to(publication.authority)
            state = states.get(selected)
            if state is None or not state.present or state.kind != "file":
                raise MutationConflict(f"installed {provider_id} binding is missing from the publication")
            try:
                copied = json.loads(state.content.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as exc:
                raise SetupError(SETUP_CONFIG_INVALID,
                                 f"installed {provider_id} hook has no harness binding: {exc}") from exc
            expected = _materialized_binding(template, harness_root, runtime_root)
            if copied != template and copied != expected:
                raise SetupError(SETUP_ADAPTER_COLLISION,
                                 f"installed ROOT hook binding for {provider_id} is neither its "
                                 "verified template nor a binding for this exact harness/runtime")
            actions.append((target, selected, state, _render_root_json(expected) if copied != expected else None))
        output = PreparedOutputTransaction(publication.authority, allowed_roots=(publication.authority.parent,))
        output.prepare()
        if output.root_identity != publication.authority_identity:
            raise MutationConflict("ROOT binding prepared authority changed")
        # Parsing/config validation cannot substitute for the original native proof.
        _check_root_targets(publication.authority, publication.authority_identity,
                            publication.directories, publication.files)
        for target, selected, state, data in actions:
            chain = _root_directory_chain(selected.parent, directories)
            if data is None:
                with _DirectoryAnchor(publication.authority, target.parent) as anchor:
                    _require_anchored_directories(anchor, chain)
                    _require_anchored_expected(anchor, target.name, state)
            else:
                output.admit(target)
                receipt = replace(publication.authority, selected, data, expected=state,
                                  expected_root_identity=publication.authority_identity, expected_directories=chain)
                states[selected] = receipt.resulting
        _check_root_targets(publication.authority, publication.authority_identity,
                            publication.directories, tuple(states.items()))
        return [target for target, _, _, _ in actions]
    except (OSError, ValueError, TypeError) as exc:
        raise SetupError(SETUP_ADAPTER_COLLISION, f"ROOT binding publication rejected: {exc}") from exc


def _install_root_payloads(
    harness_root: Path,
    root_workspace: Path,
    *,
    plan: list[tuple[Path, Path]],
    overwrite: bool,
    runtime_root: Path | None = None,
    binding_templates: dict[Path, dict[str, Any]] | None = None,
    captured: _RootPayloadPlan | None = None,
    publications: list[_RootPublication] | None = None,
) -> tuple[list[Path], list[Path], list[Path]]:
    """Install captured bytes against original states and return the existing three lists."""
    harness_identity = path_identity(harness_root)
    workspace_identity = path_identity(root_workspace)
    if workspace_identity == harness_identity or workspace_identity.startswith(harness_identity + os.sep):
        raise SetupError(SETUP_CONFIG_INVALID,
                         f"root workspace must not be inside the harness root: {root_workspace}")
    prepared = captured if captured is not None else _preflight_root_payloads(
        plan, root_workspace, overwrite=overwrite, harness_root=harness_root,
        runtime_root=runtime_root, binding_templates=binding_templates)
    snapshot = prepared.capture
    try:
        _require_root_capture(snapshot, plan, root_workspace)
        if prepared.overwrite != overwrite:
            raise MutationConflict("ROOT payload preflight has a different overwrite policy")
        directories = dict(snapshot.directories)
        original_files = tuple(((snapshot.workspace / file.relative).relative_to(snapshot.authority), file.target)
                               for file in snapshot.files)
        _check_root_targets(snapshot.authority, snapshot.authority_identity, snapshot.directories, original_files)
        for file in snapshot.files:
            _revalidate_root_source(file.source)
        output = PreparedOutputTransaction(snapshot.authority, allowed_roots=(snapshot.authority.parent,))
        output.prepare()
        if output.root_identity != snapshot.authority_identity:
            raise MutationConflict("ROOT payload prepared authority changed")
        for relative in sorted(directories, key=lambda path: (len(path.parts), str(path))):
            if not directories[relative].present:
                directories[relative] = create_directory(
                    snapshot.authority, relative, expected_root_identity=snapshot.authority_identity,
                    expected_directories=_root_directory_chain(relative.parent, directories))
        installed, overwritten, merged = [], [], []
        resulting = {}
        for action in prepared.actions:
            file = action.file
            target = snapshot.workspace / file.relative
            relative = target.relative_to(snapshot.authority)
            chain = _root_directory_chain(relative.parent, directories)
            _revalidate_root_source(file.source)
            if action.data is None:
                with _DirectoryAnchor(snapshot.authority, target.parent) as anchor:
                    _require_anchored_directories(anchor, chain)
                    _require_anchored_expected(anchor, target.name, file.target)
                state = file.target
            else:
                output.admit(target)
                receipt = replace(snapshot.authority, relative, action.data, expected=file.target,
                                  expected_root_identity=snapshot.authority_identity,
                                  expected_directories=chain, metadata=action.metadata)
                state = receipt.resulting
                if action.merged:
                    merged.append(target)
                elif file.target.present:
                    overwritten.append(target)
            _revalidate_root_source(file.source)
            resulting[relative] = state
            installed.append(target)
        _check_root_targets(snapshot.authority, snapshot.authority_identity,
                            tuple(directories.items()), tuple(resulting.items()))
        for file in snapshot.files:
            _revalidate_root_source(file.source)
        if publications is not None:
            publications.append(_RootPublication(snapshot.workspace, snapshot.authority, snapshot.authority_identity,
                                                 tuple(directories.items()), tuple(resulting.items())))
        return installed, overwritten, merged
    except (OSError, ValueError, TypeError) as exc:
        raise SetupError(SETUP_ADAPTER_COLLISION, f"ROOT payload publication rejected: {exc}") from exc


def _load_binding(path: Path) -> Any:
    """Import one registered launcher binding without writing bytecode back
    into the shipped tree."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("_harness_binding", path)
    if spec is None or spec.loader is None:
        raise ConfigError(f"cannot load launcher binding: {path}")
    module = importlib.util.module_from_spec(spec)
    previous = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous
    return module


def _validate_binding_module(path: Path, provider_id: str) -> None:
    """Require exact identity plus the complete configurable-launch contract."""
    try:
        module = _load_binding(path)
    except Exception as exc:
        raise SetupError(
            SETUP_CONFIG_INVALID,
            f"launcher binding {path} cannot be imported: {exc}",
        ) from exc
    if getattr(module, "PROVIDER_ID", None) != provider_id:
        raise SetupError(
            SETUP_CONFIG_INVALID,
            f"launcher binding {path} PROVIDER_ID {getattr(module, 'PROVIDER_ID', None)!r} "
            f"does not match {provider_id!r}",
        )
    version = getattr(module, "ADAPTER_VERSION", None)
    if not isinstance(version, str) or not version:
        raise SetupError(
            SETUP_CONFIG_INVALID,
            f"launcher binding {path} lacks a non-empty ADAPTER_VERSION",
        )
    for symbol in ("validate_launch_config", "build_argv", "parse_line"):
        if not callable(getattr(module, symbol, None)):
            raise SetupError(
                SETUP_CONFIG_INVALID,
                f"launcher binding {path} lacks required callable {symbol}",
            )


def _check_launcher_bindings(harness_root: Path) -> list[Path]:
    """Validate the shipped/read-only launcher bindings.

    Every registered binding must import and expose the strict symbols with
    an exact PROVIDER_ID; every catalog binding must byte-match its matching
    registered binding.  Setup never copies a binding into product source.
    """
    registered_dir = harness_root / "orchestrator_harness" / "provider_adapters"
    adapters_dir = harness_root / "adapters"
    if not adapters_dir.is_dir():
        raise SetupError(SETUP_CONFIG_INVALID, f"adapter catalog missing: {adapters_dir}")
    checked: list[Path] = []
    registered: dict[str, Path] = {}
    if registered_dir.is_dir():
        for binding in sorted(registered_dir.rglob("launcher_binding.py")):
            provider_id = binding.parent.name
            _validate_binding_module(binding, provider_id)
            registered[provider_id] = binding
            checked.append(binding)
    for adapter in sorted(adapters_dir.iterdir()):
        if not adapter.is_dir():
            continue
        source_binding = adapter / "harness" / "launcher_binding.py"
        if not source_binding.is_file():
            continue
        target_binding = registered.get(adapter.name)
        if target_binding is None:
            raise SetupError(
                SETUP_CONFIG_INVALID,
                f"registered binding missing for {adapter.name}: "
                f"{registered_dir / adapter.name / 'launcher_binding.py'} "
                f"(bindings are shipped/read-only; setup never copies them)",
            )
        if target_binding.read_bytes() != source_binding.read_bytes():
            raise SetupError(
                SETUP_CONFIG_INVALID,
                f"registered binding drift for {adapter.name}: {target_binding} "
                f"differs from the catalog binding",
            )
    return checked


class _MonitorStartup(dict[str, Any]):
    """An operation's record plus its original child handle; never serialized."""

    def __init__(self, record: dict[str, Any], child: Any, stderr_path: Path) -> None:
        super().__init__(record)
        self.child = child
        self.stderr_path = stderr_path


def _start_monitor_locked(
    harness_root: Path,
    rt: Path,
    config_identity: str,
) -> dict[str, Any]:
    """Reserve STARTING before spawn; caller holds admission and record locks."""
    record = {
        "schema": MONITOR_SCHEMA,
        "config_identity": config_identity,
        "startup_id": uuid.uuid4().hex,
        "pid": None,
        "creation_time": None,
        "started_at": iso_utc(),
        "health": "starting",
        "last_heartbeat_at": None,
        "initialized": False,
        "ready_at": None,
        "watched_lane_count": 0,
        "diagnostics": [],
        "stop_requested": False,
    }
    atomic_write_json(monitor_record_path(rt), record)
    argv = processes.python_argv("orchestrator_harness.monitor")
    env = dict(os.environ)
    env["HARNESS_MONITOR_STARTUP_ID"] = record["startup_id"]
    stderr_path = rt / "monitor" / (record["startup_id"] + ".stderr.log")
    from .mutation import open_append_file
    child = None
    try:
        append_activity(rt, "process.spawn_requested", component="setup", argv=argv,
                        startup_id=record["startup_id"], stderr_path=str(stderr_path))
        with open_append_file(stderr_path.parent, stderr_path.name) as stderr:
            child = processes.spawn_detached(argv, cwd=str(harness_root), stderr=stderr, env=env)
        identity = processes.process_identity(child.pid)
        if (identity is None or identity.get("pid") != child.pid
                or not processes.valid_creation_identity(identity.get("creation_time"))):
            raise ConfigError("cannot record monitor process identity")
        record.update(pid=identity["pid"], creation_time=identity["creation_time"])
        atomic_write_json(monitor_record_path(rt), record)
        append_activity(rt, "process.spawned", component="setup", pid=identity["pid"],
                        creation_time=identity["creation_time"], startup_id=record["startup_id"])
    except Exception as exc:
        cleanup = child is None or processes.reap_created_process(child)
        record.update(health="STOPPED" if cleanup else "cleanup_unproven", stop_requested=True)
        record["diagnostics"] = [{"kind": "startup_failure", "error": str(exc),
                                  "child_created": child is not None, "cleanup_proven": cleanup,
                                  "stderr_path": str(stderr_path)}]
        # Preserve a failed reservation even if identity publication failed.
        # Its nullable identity is not adopted as a new live monitor.
        atomic_write_json(monitor_record_path(rt), record)
        raise ConfigError(f"monitor startup failed; exact child cleanup {'proven' if cleanup else 'unproven'}: {exc}") from exc
    return _MonitorStartup(record, child, stderr_path)


def _start_monitor(
    harness_root: Path,
    rt: Path,
    config: HarnessConfig,
    config_identity: str,
) -> dict[str, Any]:
    """Start the one persistent monitor under the monitor-record lock."""
    record_path = monitor_record_path(rt)
    with runtime_admission_lock(rt), RecordLock(record_path):
        state = read_runtime_state(rt)
        if not state or state["state"] != "OPEN":
            raise ConfigError("monitor startup requires an OPEN runtime")
        existing = read_monitor_record(rt)
        if existing is not None:
            pid = existing.get("pid")
            creation = existing.get("creation_time")
            if processes.identity_matches(pid, creation):
                if existing.get("stop_requested") or existing.get("health") == "STOPPED":
                    raise ConfigError("monitor stop is incomplete; its exact owner remains live")
                if existing.get("config_identity") != config_identity:
                    raise ConfigError("live monitor configuration identity differs")
                _validate_monitor_ready(rt, config_identity, existing)
                raise ConfigError(SETUP_MONITOR_ALREADY_RUNNING)
            if not processes.identity_exit_proven(pid, creation):
                raise ConfigError("monitor owner exit is unproven; preserve its ownership record")
        return _start_monitor_locked(harness_root, rt, config_identity)


def _heartbeat_is_fresh(record: dict[str, Any]) -> bool:
    """Return whether the recorded heartbeat is a fresh, valid ISO UTC time.

    Missing, malformed, naive, or stale timestamps are not fresh.  The
    staleness bound is imported locally to avoid the monitor -> setup cycle.
    """
    from .monitor import HEARTBEAT_STALENESS_SECONDS

    raw = record.get("last_heartbeat_at")
    if not isinstance(raw, str) or not raw:
        return False
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None:
        return False
    return (parsed.utcoffset().total_seconds() == 0
            and 0 <= (datetime.now(timezone.utc) - parsed).total_seconds() <= HEARTBEAT_STALENESS_SECONDS)


def _validate_monitor_ready(rt: Path, config_identity: str, record: dict[str, Any] | None) -> dict[str, Any]:
    state = read_runtime_state(rt)
    if not state or state.get("state") != "OPEN":
        raise ConfigError("monitor readiness requires an OPEN runtime; explicitly run harness setup")
    if (record is None or record.get("config_identity") != config_identity
            or not isinstance(record.get("startup_id"), str) or not record["startup_id"]):
        raise ConfigError("monitor readiness has no matching configuration/startup ownership; explicitly prepare ROOT")
    if (record.get("stop_requested") or record.get("initialized") is not True
            or record.get("health") != "healthy" or not _heartbeat_is_fresh(record)):
        raise ConfigError("monitor is not initialized and healthy with a fresh heartbeat; inspect diagnostics and run health monitor-recover")
    # ready_at is a birth fact and may be older than the current heartbeat.
    try:
        ready = datetime.fromisoformat(record["ready_at"].replace("Z", "+00:00"))
        if (ready.tzinfo is None or ready.utcoffset().total_seconds() != 0
                or ready > datetime.now(timezone.utc)):
            raise ValueError("invalid readiness timestamp")
    except (KeyError, AttributeError, TypeError, ValueError) as exc:
        raise ConfigError("monitor has no valid child-published ready_at") from exc
    if not processes.identity_matches(record.get("pid"), record.get("creation_time")):
        raise ConfigError("initialized monitor incarnation is not live; explicitly run health monitor-recover")
    return record


def require_monitor_ready(rt: Path, config_identity: str) -> dict[str, Any]:
    """Validate initialized live monitoring without installing or restarting it."""
    with runtime_admission_lock(rt), RecordLock(monitor_record_path(rt)):
        return _validate_monitor_ready(rt, config_identity, read_monitor_record(rt))


def _wait_monitor_ready(rt: Path, config_identity: str, startup: _MonitorStartup) -> dict[str, Any]:
    """Wait outside all caller locks and retain/reap only this operation's child."""
    from .mutation import read_bytes
    deadline = time.monotonic() + 30.0  # Existing controller handshake bound.
    try:
        while True:
            with runtime_admission_lock(rt), RecordLock(monitor_record_path(rt)):
                state = read_runtime_state(rt)
                current = read_monitor_record(rt)
                if (not state or state.get("state") != "OPEN" or current is None
                        or any(current.get(key) != startup.get(key)
                               for key in ("startup_id", "pid", "creation_time", "config_identity"))):
                    raise ConfigError("monitor startup lost its OPEN runtime or exact reservation")
                if current.get("health") == "healthy":
                    return _validate_monitor_ready(rt, config_identity, current)
                if current.get("health") != "starting" or current.get("stop_requested"):
                    raise ConfigError(f"monitor initialization failed: {current.get('diagnostics')}")
            if startup.child.poll() is not None:
                raise ConfigError(f"monitor exited before READY (exit {startup.child.returncode})")
            if time.monotonic() >= deadline:
                raise ConfigError("monitor did not publish initialized READY within 30 seconds")
            time.sleep(0.2)
    except Exception as exc:
        cleanup = processes.reap_created_process(startup.child)
        try:
            stderr = read_bytes(startup.stderr_path.parent, startup.stderr_path.name).decode("utf-8", errors="replace")
        except (OSError, ValueError) as read_error:
            stderr = f"stderr unavailable: {read_error}"
        diagnostic = {"kind": "startup_failure", "error": str(exc), "cleanup_proven": cleanup,
                      "child_exit_code": startup.child.poll(), "stderr_path": str(startup.stderr_path),
                      "stderr": stderr}
        with runtime_admission_lock(rt), RecordLock(monitor_record_path(rt)):
            current = read_monitor_record(rt)
            if current is not None and all(current.get(key) == startup.get(key)
                    for key in ("startup_id", "pid", "creation_time", "config_identity")):
                current.update(health="STOPPED" if cleanup else "cleanup_unproven", stop_requested=True)
                current["diagnostics"] = [diagnostic]
                atomic_write_json(monitor_record_path(rt), current)
        raise ConfigError(f"monitor readiness failed; exact child cleanup {'proven' if cleanup else 'unproven'}: {exc}; {stderr}") from exc


def run_monitor_recover(harness_root: Path | None = None) -> dict[str, Any]:
    """Execute ``health monitor-recover`` and return the structured result.

    Automatic recovery is managed-only and requires the runtime state OPEN.
    Under the single monitor-record lock it starts a missing monitor, leaves
    a fresh live monitor untouched, restarts a dead one, and force-stops a
    hung one before replacement.  A deliberately stopped monitor is never
    restarted.
    """
    try:
        harness_root = (
            Path(harness_root).resolve()
            if harness_root is not None
            else find_harness_root()
        )
        config = load_config(harness_root)
        manifest = load_resource_manifest(harness_root)
    except (ConfigError, OSError, ValueError) as exc:
        return _failure(
            SETUP_CONFIG_INVALID,
            str(exc),
            "fix harness-config.json and resource-manifest.json, then re-run setup",
        )

    rt = config.runtime_root
    if config.profile != "managed":
        return _failure(
            MONITOR_RECOVERY_DISABLED,
            "automatic monitor recovery requires managed coordination",
            "enable managed_coordination or start the monitor manually",
        )
    record_path = monitor_record_path(rt)
    try:
        config_identity = compute_config_identity(config, manifest)
        with runtime_admission_lock(rt), RecordLock(record_path):
            state = read_runtime_state(rt)
            if not state or state.get("state") != "OPEN":
                return _failure(MONITOR_RUNTIME_NOT_OPEN, "monitor recovery requires an OPEN runtime",
                                "open the runtime with harness setup before recovery")
            record = read_monitor_record(rt)
            if record is not None:
                pid, creation = record.get("pid"), record.get("creation_time")
                if not processes._valid_pid(pid) or not processes.valid_creation_identity(creation):
                    return _failure(MONITOR_IDENTITY_UNPROVEN,
                                    "monitor has no usable native incarnation; no replacement started",
                                    "reconcile the exact monitor ownership evidence before recovery")
                alive = processes.identity_matches(pid, creation)
                gone = processes.identity_exit_proven(pid, creation)
                if not alive and not gone:
                    return _failure(MONITOR_IDENTITY_UNPROVEN, "monitor owner is unobservable; no replacement started",
                                    "reconcile exact ownership before recovery")
                if record.get("stop_requested") or record.get("health") == "STOPPED":
                    if alive:
                        return _failure(MONITOR_STOP_INCOMPLETE, "stopped monitor's exact owner remains live",
                                        "stop the exact owner before recovery")
                    return {"ok": True, "code": MONITOR_DELIBERATELY_STOPPED,
                            "summary": "monitor is deliberately stopped; nothing started",
                            "evidence_paths": [str(record_path)], "next_action": "explicitly run harness setup when needed"}
                if alive:
                    if record.get("config_identity") != config_identity:
                        raise ConfigError("live monitor configuration identity differs; preserve its owner")
                    if record.get("health") == "starting":
                        raise ConfigError("monitor STARTING belongs to another operation; no replacement or readiness adoption")
                    if (record.get("initialized") is True and record.get("health") == "healthy"
                            and _heartbeat_is_fresh(record)):
                        _validate_monitor_ready(rt, config_identity, record)
                        return {"ok": True, "code": MONITOR_HEALTHY,
                                "summary": "monitor is initialized, live and healthy; nothing changed",
                                "evidence_paths": [str(record_path)], "next_action": "no action"}
                    if (not processes.terminate_process(pid, creation, force=True)
                            or not processes.identity_exit_proven(pid, creation)):
                        return _failure(MONITOR_CLEANUP_UNPROVEN, "hung monitor termination could not be proven",
                                        "verify exact owner exit before recovery")
            startup = _start_monitor_locked(harness_root, rt, config_identity)
        # The child needs admission for its first pass. This wait must remain
        # outside both the monitor record and every transitive admission scope.
        ready = _wait_monitor_ready(rt, config_identity, startup)
        return {"ok": True, "code": MONITOR_RECOVERED,
                "summary": "monitor completed its first healthy pass and published initialized READY",
                "evidence_paths": [str(record_path)], "monitor": ready, "next_action": "proceed"}
    except (ConfigError, OSError) as exc:
        return _failure(SETUP_CONFIG_INVALID, str(exc), "inspect retained diagnostics and explicitly recover the monitor")


def _failure(code: str, summary: str, next_action: str) -> dict[str, Any]:
    return {
        "ok": False,
        "code": code,
        "summary": summary,
        "evidence_paths": [],
        "next_action": next_action,
    }


def run_setup(*, overwrite: bool = False) -> dict[str, Any]:
    """Execute ``harness setup`` and return the structured result.

    Phase one validates and preflights the entire catalog with no writes;
    phase two performs the writes.  A ROOT payload collision in normal mode
    therefore causes no partial copy of any kind.
    """
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
        manifest = load_resource_manifest(harness_root)
    except (ConfigError, OSError, ValueError) as exc:
        return _failure(
            SETUP_CONFIG_INVALID,
            str(exc),
            "fix harness-config.json and resource-manifest.json, then re-run setup",
        )

    rt = config.runtime_root
    try:
        if config.root_workspace is None or config.manager_workspace is None:
            raise SetupError(
                SETUP_CONFIG_INVALID,
                "harness config does not declare root_workspace",
            )
        _require_plain_workspace_boundary(rt, code=SETUP_CONFIG_INVALID)
        harness_identity = path_identity(harness_root)
        workspace_identity = path_identity(config.manager_workspace)
        if workspace_identity == harness_identity or workspace_identity.startswith(
            harness_identity + os.sep
        ):
            raise SetupError(
                SETUP_CONFIG_INVALID,
                f"manager workspace must not be inside the harness root: {config.manager_workspace}",
            )
        cache_catalog = _capture_cache_catalog(harness_root)
        root_plan = _plan_root_payloads(harness_root, catalog=cache_catalog)
        root_capture = _capture_root_payloads(
            root_plan, config.manager_workspace,
            sources={source.path: source for source in cache_catalog.sources},
        )
        root_bindings = _plan_root_hook_bindings(
            harness_root, root_plan,
            captured_sources={file.source.path: file.source.state.content for file in root_capture.files},
        )
        binding_templates = {
            config.manager_workspace / relative: template
            for _, relative, template in root_bindings
        }
        root_preflight = _preflight_root_payloads(
            root_plan,
            config.manager_workspace,
            overwrite=overwrite,
            harness_root=harness_root,
            runtime_root=rt,
            binding_templates=binding_templates,
            captured=root_capture,
        )
        cache_plan = _plan_active_cache(harness_root, root_plan=root_plan, catalog=cache_catalog)
        cache_boundary = _capture_cache_boundary(rt)
        cache_identity = compute_config_identity(config, manifest)
        _inspect_active_cache(cache_plan, rt, overwrite=overwrite, catalog=cache_catalog,
                              captured=cache_boundary, config_identity=cache_identity)
        _check_launcher_bindings(harness_root)
    except SetupError as exc:
        return _failure(
            exc.code,
            str(exc),
            exc.next_action or "resolve the named target; --overwrite replaces only harness-owned payloads",
        )
    except ConfigError as exc:
        return _failure(
            SETUP_CONFIG_INVALID,
            str(exc),
            "fix harness-config.json and resource-manifest.json, then re-run setup",
        )

    try:
        try:
            _revalidate_cache_catalog(cache_catalog)
            cache_boundary = _prepare_cache_runtime(cache_boundary)
        except (OSError, ValueError, TypeError) as exc:
            raise SetupError(SETUP_CACHE_INVALID, f"runtime/cache pre-publication proof changed: {exc}") from exc
        with runtime_admission_lock(rt):
            state = read_runtime_state(rt)
            if state is None:
                from .public_checks import home
                ownership_paths = (rt / "CURRENT_EPOCH.json", monitor_record_path(rt),
                                   home(config.root_workspace) / "service.json",
                                   home(config.root_workspace) / "config.json")
                if any(path.exists() or path.is_symlink() for path in ownership_paths):
                    raise SetupError(SETUP_CONFIG_INVALID, "runtime state is missing while native ownership remains")
                for directory in (rt / "epochs", rt / "resources/leases"):
                    if directory.exists() and any(directory.iterdir()):
                        raise SetupError(SETUP_CONFIG_INVALID, "runtime state is missing while durable ownership remains")
            if state is not None and state.get("state") == "CLOSED":
                from .shutdown import prove_runtime_closed
                try:
                    prove_runtime_closed(rt, config.root_workspace, harness_root)
                except (RuntimeError, OSError, ValueError) as exc:
                    raise SetupError(SETUP_CONFIG_INVALID, f"cannot reopen unproven CLOSED runtime: {exc}") from exc
            for parent in ("worktrees", "monitor", "epochs", "resources"):
                (rt / parent).mkdir(parents=True, exist_ok=True)
            if config.profile == "managed":
                (rt / "manager").mkdir(parents=True, exist_ok=True)

            if state is None or state.get("state") == "CLOSED":
                set_runtime_state(rt, "OPEN")
            elif state.get("state") != "OPEN":
                raise SetupError(
                    SETUP_CONFIG_INVALID, f"unexpected runtime state: {state.get('state')}"
                )

            if config.profile == "managed":
                queue_path = manager_queue_path(rt)
                if not queue_path.is_file():
                    from .epochs import _stage_manager_queue

                    _stage_manager_queue(rt, "idle")
                else:
                    try:
                        queue = read_record(queue_path, MANAGER_QUEUE_SCHEMA)
                        if queue.get("epoch_id") != "idle":
                            # A live epoch owns the queue; leave it alone.
                            pass
                    except (OSError, ValueError):
                        raise SetupError(
                            SETUP_CONFIG_INVALID,
                            "manager queue is malformed; remove it and re-run setup",
                        )

            _install_active_cache(harness_root, rt, plan=cache_plan, overwrite=overwrite,
                                  catalog=cache_catalog, captured=cache_boundary, config_identity=cache_identity)
            root_publications: list[_RootPublication] = []
            installed, overwritten, merged = _install_root_payloads(
                harness_root,
                config.manager_workspace,
                plan=root_plan,
                overwrite=overwrite,
                runtime_root=rt,
                binding_templates=binding_templates,
                captured=root_preflight,
                publications=root_publications,
            )
            installed_set = set(installed)
            if any(config.manager_workspace / relative not in installed_set for _, relative, _ in root_bindings):
                raise SetupError(
                    SETUP_CONFIG_INVALID,
                    "installed ROOT hook payload is missing its planned harness binding",
                )
            _materialize_root_hook_bindings(
                config.manager_workspace,
                harness_root,
                rt,
                root_bindings,
                publication=root_publications[0],
            )

            manifest_path = rt / "resources" / "RESOURCE_MANIFEST.json"
            if manifest_path.is_file():
                try:
                    active = read_record(manifest_path, RESOURCE_MANIFEST_SCHEMA)
                    if active.get("resources") != [dict(item) for item in manifest.resources]:
                        if read_current_epoch(rt) is not None:
                            raise SetupError(
                                SETUP_RESOURCE_MANIFEST_INVALID,
                                "resource manifest changed while an epoch is active; "
                                "shut down the runtime before changing it",
                            )
                        if any((rt / "resources" / "leases").glob("*.lease")):
                            raise SetupError(
                                SETUP_RESOURCE_MANIFEST_INVALID,
                                "resource manifest changed while a lease is live; "
                                "shut down the runtime before changing it",
                            )
                        atomic_write_json(
                            manifest_path,
                            {
                                "schema": RESOURCE_MANIFEST_SCHEMA,
                                "resources": [dict(item) for item in manifest.resources],
                            },
                        )
                except (OSError, ValueError):
                    raise SetupError(
                        SETUP_RESOURCE_MANIFEST_INVALID,
                        "active resource manifest is malformed",
                    )
            else:
                atomic_write_json(
                    manifest_path,
                    {
                        "schema": RESOURCE_MANIFEST_SCHEMA,
                        "resources": [dict(item) for item in manifest.resources],
                    },
                )
            (rt / "resources" / "leases").mkdir(parents=True, exist_ok=True)

            config_identity = compute_config_identity(config, manifest)
            monitor_already_running = False
            try:
                monitor = _start_monitor(harness_root, rt, config, config_identity)
            except ConfigError as exc:
                if str(exc) != SETUP_MONITOR_ALREADY_RUNNING:
                    raise
                monitor_already_running = True
        # No outer admission/record lock may surround initialization waiting.
        if monitor_already_running:
            require_monitor_ready(rt, config_identity)
            return {"ok": True, "code": SETUP_MONITOR_ALREADY_RUNNING,
                    "summary": "the exact initialized healthy monitor already exists",
                    "evidence_paths": [str(monitor_record_path(rt))],
                    "next_action": "proceed; the runtime is monitored"}
        else:
            _wait_monitor_ready(rt, config_identity, monitor)
    except SetupError as exc:
        return _failure(
            exc.code,
            str(exc),
            exc.next_action or "resolve the named target; --overwrite replaces only harness-owned payloads",
        )
    except ConfigError as exc:
        return _failure(
            SETUP_CONFIG_INVALID,
            str(exc),
            "resolve the named target; --overwrite replaces only harness-owned payloads",
        )

    result: dict[str, Any] = {
        "ok": True,
        "code": "SETUP_OK",
        "summary": "runtime is OPEN and the active resource manifest exists",
        "evidence_paths": [
            str(rt / "resources" / "RESOURCE_MANIFEST.json"),
            str(monitor_record_path(rt)),
        ],
        "next_action": "bootstrap a lane with `lane bootstrap`",
    }
    if overwritten:
        result["overwritten_paths"] = [str(path) for path in overwritten]
    if merged:
        result["merged_paths"] = [str(path) for path in merged]
    return result
