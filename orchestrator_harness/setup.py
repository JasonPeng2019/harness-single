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

import hashlib
import json
import os
import shutil
import stat
import sys
import tomllib
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable

from . import processes
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
from .core import iso_utc
from .epochs import MANAGER_QUEUE_SCHEMA, manager_queue_path, read_current_epoch
from .records import (
    RecordLock,
    _replace_with_retry,
    atomic_write_bytes,
    atomic_write_json,
    read_record,
)

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
CACHE_TRANSACTION_SCHEMA = "super-cache-transaction/v1"
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


def runtime_state_path(rt: Path) -> Path:
    return rt / "RUNTIME_STATE.json"


def read_runtime_state(rt: Path) -> dict[str, Any] | None:
    path = runtime_state_path(rt)
    if not path.is_file():
        return None
    try:
        return read_record(path, RUNTIME_STATE_SCHEMA)
    except (OSError, ValueError):
        return None


def set_runtime_state(rt: Path, state: str) -> dict[str, Any]:
    record = {"schema": RUNTIME_STATE_SCHEMA, "state": state, "updated_at": iso_utc()}
    with RecordLock(runtime_state_path(rt)):
        atomic_write_json(runtime_state_path(rt), record)
    return record


def monitor_record_path(rt: Path) -> Path:
    return rt / "monitor" / "MONITOR.json"


def read_monitor_record(rt: Path) -> dict[str, Any] | None:
    path = monitor_record_path(rt)
    if not path.is_file():
        return None
    try:
        return read_record(path, MONITOR_SCHEMA)
    except (OSError, ValueError):
        return None


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


def _validate_worker_commands(
    provider_id: str, dotdir: str, files: dict[Path, Path]
) -> None:
    """Shipped hook commands must name files owned by the same worker payload."""
    for config_name in ("hooks.json", "settings.json"):
        config = files.get(Path(dotdir) / config_name)
        if config is None:
            continue
        try:
            record = json.loads(config.read_text(encoding="utf-8"))
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
) -> dict[str, list[tuple[Path, Path]]]:
    """Resolve every provider's shared and provider-owned worker destination."""
    workspace = harness_root / "super-cache" / "workspace"
    if not workspace.is_dir():
        raise SetupError(SETUP_CACHE_INVALID, f"shipped super-cache workspace missing: {workspace}")
    base = _plan_tree(workspace)
    base_paths = {relative for _, relative in base}
    missing = next((path for path in SHARED_WORKER_FILES if path not in base_paths), None)
    if missing is not None:
        raise SetupError(SETUP_CACHE_INVALID, f"shared worker lifecycle file missing: {missing}")
    if any(not (workspace / path).read_bytes() for path in SHARED_WORKER_FILES):
        raise SetupError(SETUP_CACHE_INVALID, "shared worker lifecycle file is empty")
    root_owners: dict[str, str] = {}
    root_dotdirs: dict[str, set[str]] = {}
    adapters_dir = harness_root / "adapters"
    for source, relative in root_plan if root_plan is not None else _plan_root_payloads(harness_root):
        owner = source.relative_to(adapters_dir).parts[0]
        root_dotdirs.setdefault(owner, set()).add(relative.parts[0].casefold())
        key = relative.as_posix().casefold()
        if key in root_owners and root_owners[key] != owner:
            raise SetupError(SETUP_ADAPTER_COLLISION, f"ROOT destination {relative} has multiple provider owners")
        root_owners[key] = owner
    compositions: dict[str, list[tuple[Path, Path]]] = {}
    dotdir_owners: dict[str, str] = {}
    for adapter in sorted(adapters_dir.iterdir()):
        if not adapter.is_dir():
            continue
        provider_id = adapter.name
        payload = adapter / "super-cache"
        if payload.is_dir() and not (adapter / "harness" / "launcher_binding.py").is_file():
            raise SetupError(SETUP_ADAPTER_COLLISION, f"unknown worker provider owns {payload}")
        if not (adapter / "harness" / "launcher_binding.py").is_file():
            continue
        if not payload.is_dir():
            raise SetupError(SETUP_CACHE_INVALID, f"worker payload missing for {provider_id}: {payload}")
        worker = _plan_tree(payload)
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
            if f".agent-workspace/{helper}" not in skill_path.read_text(encoding="utf-8"):
                raise SetupError(SETUP_CACHE_INVALID, f"{provider_id} worker skill does not call {helper}: {skill_path}")
        binding = _read_json_object(worker_files[Path(dotdir) / PROVIDER_WORKER_FILES[0]], description="worker binding")
        if binding.get("schema") != ROOT_HOOK_BINDING_SCHEMA or binding.get("role") != "worker" or binding.get("provider_id") != provider_id:
            raise SetupError(SETUP_CONFIG_INVALID, f"worker binding identity invalid for {provider_id}")
        _validate_worker_commands(provider_id, dotdir, worker_files)
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
    harness_root: Path, *, root_plan: list[tuple[Path, Path]] | None = None
) -> list[tuple[Path, Path]]:
    """Plan the shipped cache and each provider's exact composed worker tree."""
    source_cache = harness_root / "super-cache"
    if not source_cache.is_dir():
        raise SetupError(SETUP_CACHE_INVALID, f"shipped super-cache missing: {source_cache}")
    workspace = source_cache / "workspace"
    if not workspace.is_dir():
        raise SetupError(
            SETUP_CACHE_INVALID, f"shipped super-cache workspace missing: {workspace}"
        )
    planned: list[tuple[Path, Path]] = []
    for source_file, relative in _plan_tree(workspace):
        planned.append((source_file, Path("workspace") / relative))
    custom = source_cache / "custom"
    if custom.is_dir():
        for source_file, relative in _plan_tree(custom):
            planned.append((source_file, Path("custom") / relative))
    adapters_dir = harness_root / "adapters"
    if adapters_dir.is_dir():
        for adapter in sorted(adapters_dir.iterdir()):
            if not adapter.is_dir():
                continue
            payload = adapter / "super-cache"
            if not payload.is_dir():
                continue
            for source_file, relative in _plan_tree(payload):
                planned.append(
                    (source_file, Path("adapter-payloads") / adapter.name / relative)
                )
    for provider_id, files in _plan_worker_compositions(harness_root, root_plan=root_plan).items():
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
    directories, files = _cache_entries(root)
    try:
        hashes = {
            name: hashlib.sha256((root / name).read_bytes()).hexdigest()
            for name in sorted(files)
        }
    except OSError as exc:
        raise SetupError(SETUP_CACHE_INVALID, f"cache bytes cannot be read: {root}: {exc}") from exc
    return {"directories": sorted(directories), "files": hashes}


def _manifest_for_plan(plan: list[tuple[Path, Path]]) -> dict[str, Any]:
    directories = {
        parent.as_posix()
        for _, relative in plan
        for parent in relative.parents
        if parent != Path(".")
    }
    try:
        files = {
            relative.as_posix(): hashlib.sha256(source.read_bytes()).hexdigest()
            for source, relative in plan
        }
    except OSError as exc:
        raise SetupError(SETUP_CACHE_INVALID, f"shipped cache bytes cannot be read: {exc}") from exc
    return {"directories": sorted(directories), "files": files}


def _validate_active_cache(plan: list[tuple[Path, Path]], destination: Path) -> None:
    """Validate every installed path before reading any cache file bytes."""
    directories, files = _cache_entries(destination)
    expected_files = {relative.as_posix() for _, relative in plan}
    expected_directories = {
        parent.as_posix()
        for _, relative in plan
        for parent in relative.parents
        if parent != Path(".")
    }
    if directories != expected_directories or files != expected_files:
        raise SetupError(SETUP_CACHE_INVALID, f"active cache has missing or unknown paths: {destination}")
    try:
        for source, relative in plan:
            if (destination / relative).read_bytes() != source.read_bytes():
                raise SetupError(
                    SETUP_CACHE_INVALID,
                    f"active cache malformed: {relative} differs from the shipped source",
                )
    except OSError as exc:
        raise SetupError(SETUP_CACHE_INVALID, f"active cache cannot be verified: {destination}: {exc}") from exc


def _validated_cache_for_dispatch(harness_root: Path, rt: Path) -> None:
    """Keep both bootstrap profiles outside an unresolved cache transaction."""
    _require_plain_workspace_boundary(rt, code=SETUP_CACHE_INVALID)
    live_valid, _ = _inspect_active_cache(
        _plan_active_cache(harness_root), rt, overwrite=False, for_dispatch=True
    )
    if not live_valid:
        raise SetupError(
            SETUP_CACHE_INVALID, f"installed active cache is missing: {rt / 'super-cache'}"
        )


def _validated_worker_payload(harness_root: Path, rt: Path, provider_id: str) -> Path:
    """Return only the installed composed tree whose bytes match its planned owners."""
    _validated_cache_for_dispatch(harness_root, rt)
    compositions = _plan_worker_compositions(harness_root)
    files = compositions.get(provider_id)
    if files is None:
        raise SetupError(SETUP_CACHE_INVALID, f"unsupported worker provider: {provider_id}")
    payload = rt / "super-cache" / COMPOSED_PAYLOADS / provider_id
    for source, relative in files:
        installed = payload / relative
        if not installed.is_file():
            raise SetupError(SETUP_CACHE_INVALID, f"installed worker composition missing: {installed}")
        if installed.read_bytes() != source.read_bytes():
            raise SetupError(SETUP_CACHE_INVALID, f"installed worker composition changed: {installed}")
    expected = {relative.as_posix() for _, relative in files}
    actual = {path.relative_to(payload).as_posix() for path in payload.rglob("*") if path.is_file()}
    if actual != expected:
        raise SetupError(SETUP_CACHE_INVALID, f"installed worker composition has unknown files: {payload}")
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


def _read_cache_transaction(rt: Path) -> dict[str, Any] | None:
    path = _transaction_path(rt)
    if not _path_present(path):
        return None
    try:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or _is_reparse(info) or info.st_nlink != 1:
            raise ValueError("transaction record is not a plain file")
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise SetupError(SETUP_CACHE_INVALID, f"invalid cache transaction {path}: {exc}") from exc
    required = {"schema", "runtime_root", "transaction_id", "stage_name", "previous", "target"}
    if not isinstance(record, dict) or set(record) != required:
        raise SetupError(SETUP_CACHE_INVALID, f"invalid cache transaction fields: {path}")
    transaction_id = record["transaction_id"]
    if (
        record["schema"] != CACHE_TRANSACTION_SCHEMA
        or not isinstance(record["runtime_root"], str)
        or os.path.normcase(os.path.abspath(record["runtime_root"]))
        != os.path.normcase(os.path.abspath(rt))
        or not isinstance(transaction_id, str)
        or len(transaction_id) != 32
        or any(character not in "0123456789abcdef" for character in transaction_id)
        or record["stage_name"] != f".super-cache.{transaction_id}"
        or not _valid_manifest(record["target"])
        or (record["previous"] is not None and not _valid_manifest(record["previous"]))
    ):
        raise SetupError(SETUP_CACHE_INVALID, f"invalid cache transaction identity or manifest: {path}")
    return record


def _manifest_matches(path: Path, manifest: dict[str, Any] | None) -> bool:
    return manifest is not None and _path_present(path) and _manifest_for_tree(path) == manifest


def _require_exact_manifest(path: Path, manifest: dict[str, Any] | None) -> None:
    if not _manifest_matches(path, manifest):
        raise SetupError(SETUP_CACHE_INVALID, f"transaction backup differs from recorded prior cache: {path}")


def _check_owned_tree(path: Path, manifest: dict[str, Any], *, verify_bytes: bool) -> None:
    directories, files = _cache_entries(path)
    if not directories <= set(manifest["directories"]) or not files <= set(manifest["files"]):
        raise SetupError(SETUP_CACHE_INVALID, f"transaction artifact has unknown paths: {path}")
    if verify_bytes:
        try:
            for name in files:
                if hashlib.sha256((path / name).read_bytes()).hexdigest() != manifest["files"][name]:
                    raise SetupError(SETUP_CACHE_INVALID, f"transaction artifact has changed bytes: {path / name}")
        except OSError as exc:
            raise SetupError(SETUP_CACHE_INVALID, f"transaction artifact cannot be verified: {path}: {exc}") from exc


def _remove_owned_tree(path: Path, manifest: dict[str, Any], *, verify_bytes: bool) -> None:
    _check_owned_tree(path, manifest, verify_bytes=verify_bytes)
    shutil.rmtree(path)


def _inspect_active_cache(
    plan: list[tuple[Path, Path]], rt: Path, *, overwrite: bool, for_dispatch: bool = False
) -> tuple[bool, dict[str, Any] | None]:
    """Preflight the exact live, backup, and staged cache paths without mutation."""
    destination = rt / "super-cache"
    backup = destination.with_name(f"{destination.name}.old")
    transaction = _read_cache_transaction(rt)
    try:
        stages = sorted(
            (path for path in rt.iterdir() if path.name.startswith(".super-cache.")),
            key=lambda path: path.name,
        ) if rt.is_dir() else []
    except OSError as exc:
        raise SetupError(
            SETUP_CACHE_INVALID, f"active cache stages cannot be inspected: {rt}: {exc}"
        ) from exc
    backup_present = _path_present(backup)
    if transaction is None and (backup_present or stages):
        artifacts = ([backup] if backup_present else []) + stages
        paths = ", ".join(str(path) for path in artifacts)
        raise SetupError(
            SETUP_CACHE_INVALID,
            f"unowned cache backup or stage needs resolution: {paths}",
            next_action=f"inspect and preserve bytes at {paths}; after confirming ownership, move each artifact outside the runtime root and re-run setup",
        )
    if for_dispatch and (transaction is not None or backup_present or stages):
        raise SetupError(SETUP_CACHE_INVALID, f"cache transaction needs setup recovery: {rt}")
    if transaction is not None:
        stage = rt / transaction["stage_name"]
        extra_stages = [path for path in stages if path != stage]
        if extra_stages:
            paths = ", ".join(str(path) for path in extra_stages)
            raise SetupError(
                SETUP_CACHE_INVALID,
                f"unowned cache stage needs resolution: {paths}",
                next_action=f"inspect and preserve bytes at {paths}; after confirming ownership, move each artifact outside the runtime root and re-run setup",
            )
        if _path_present(stage):
            _check_owned_tree(stage, transaction["target"], verify_bytes=False)
        previous = transaction["previous"]
        if backup_present:
            if previous is None:
                raise SetupError(SETUP_CACHE_INVALID, f"unexpected cache backup: {backup}")
            _check_owned_tree(backup, previous, verify_bytes=True)
        live_present = _path_present(destination)
        live_target = _manifest_matches(destination, transaction["target"])
        live_previous = _manifest_matches(destination, previous)
        backup_exact = _manifest_matches(backup, previous)
        if backup_present:
            if not (backup_exact or live_target):
                raise SetupError(SETUP_CACHE_INVALID, f"partial cache backup lacks a valid promoted cache: {backup}")
            if live_present and not (live_target or live_previous):
                _check_owned_tree(destination, transaction["target"], verify_bytes=False)
        elif live_present and not (live_target or live_previous):
            raise SetupError(SETUP_CACHE_INVALID, f"transaction live cache is ambiguous: {destination}")
        elif not live_present and previous is not None:
            raise SetupError(SETUP_CACHE_INVALID, f"transaction prior cache is missing: {destination}")
        return False, transaction
    live_valid = False
    if _path_present(destination):
        directories, files = _cache_entries(destination)
        planned_files = {relative.as_posix() for _, relative in plan}
        planned_directories = {
            parent.as_posix()
            for _, relative in plan
            for parent in relative.parents
            if parent != Path(".")
        }
        unknown = sorted((directories - planned_directories) | (files - planned_files))
        if unknown:
            paths = ", ".join(str(destination / name) for name in unknown)
            raise SetupError(
                SETUP_CACHE_INVALID,
                f"active cache contains unknown paths: {paths}",
                next_action=f"inspect and preserve any needed bytes at {paths}; resolve unknown paths before re-running setup",
            )
        try:
            _validate_active_cache(plan, destination)
            live_valid = True
        except SetupError:
            if not overwrite:
                raise
    return live_valid, None


def _recover_cache_transaction(rt: Path, transaction: dict[str, Any]) -> None:
    """Finish only the transaction whose exact paths and old bytes were recorded."""
    destination = rt / "super-cache"
    backup = rt / "super-cache.old"
    stage = rt / transaction["stage_name"]
    previous = transaction["previous"]
    target = transaction["target"]
    backup_present = _path_present(backup)
    live_target = _manifest_matches(destination, target)
    if backup_present and not (_manifest_matches(backup, previous) or live_target):
        raise SetupError(SETUP_CACHE_INVALID, f"partial cache backup cannot be recovered: {backup}")
    if _path_present(stage):
        _remove_owned_tree(stage, target, verify_bytes=False)
    if backup_present and not live_target and not _manifest_matches(destination, previous):
        if _path_present(destination):
            _check_owned_tree(destination, target, verify_bytes=False)
            _replace_with_retry(destination, stage)
            _remove_owned_tree(stage, target, verify_bytes=False)
        _replace_with_retry(backup, destination)
    elif backup_present:
        _remove_owned_tree(backup, previous, verify_bytes=True)
    if _path_present(stage) or _path_present(backup) or not (
        _manifest_matches(destination, target)
        or _manifest_matches(destination, previous)
        or (previous is None and not _path_present(destination))
    ):
        raise SetupError(SETUP_CACHE_INVALID, f"cache transaction remains unresolved: {rt}")
    _transaction_path(rt).unlink()


def _replace_tree(
    staging: Path,
    destination: Path,
    *,
    validate: Callable[[Path], None] | None = None,
    validate_backup: Callable[[Path], None] | None = None,
) -> None:
    """Promote a staged tree, retaining the old destination through validation."""
    backup = destination.with_name(f"{destination.name}.old")
    if _path_present(backup):
        raise SetupError(
            SETUP_CACHE_INVALID, f"active cache backup needs recovery: {backup}"
        )
    had_destination = _path_present(destination)
    try:
        if had_destination:
            _replace_with_retry(destination, backup)
        _replace_with_retry(staging, destination)
        if validate is not None:
            validate(destination)
    except BaseException:
        try:
            if _path_present(backup):
                if _path_present(destination):
                    if _path_present(staging):
                        raise SetupError(
                            SETUP_OVERWRITE_FAILED,
                            f"cannot isolate promoted cache: {destination}",
                        )
                    _replace_with_retry(destination, staging)
                _replace_with_retry(backup, destination)
            elif (
                not had_destination
                and _path_present(destination)
                and not _path_present(staging)
            ):
                _replace_with_retry(destination, staging)
        except BaseException as exc:
            raise SetupError(
                SETUP_OVERWRITE_FAILED,
                f"active cache rollback needs recovery at {destination} and {backup}: {exc}",
            ) from exc
        raise
    if _path_present(backup):
        if validate_backup is not None:
            validate_backup(backup)
        shutil.rmtree(backup)


def _install_active_cache(
    harness_root: Path,
    rt: Path,
    *,
    plan: list[tuple[Path, Path]],
    overwrite: bool,
) -> None:
    """Stage and byte-verify the active cache, then place it atomically.

    An existing valid cache is preserved exactly; a malformed or incomplete
    cache is rejected unless ``overwrite`` rebuilds it from the catalog.
    """
    destination = rt / "super-cache"
    live_valid, transaction = _inspect_active_cache(plan, rt, overwrite=overwrite)
    if transaction is not None:
        try:
            _recover_cache_transaction(rt, transaction)
        except OSError as exc:
            raise SetupError(SETUP_OVERWRITE_FAILED, f"active cache recovery failed: {exc}") from exc
        live_valid, _ = _inspect_active_cache(plan, rt, overwrite=overwrite)
    if live_valid:
        return
    previous = _manifest_for_tree(destination) if _path_present(destination) else None
    target = _manifest_for_plan(plan)
    transaction_id = uuid.uuid4().hex
    staging = rt / f".super-cache.{transaction_id}"
    while _path_present(staging):
        transaction_id = uuid.uuid4().hex
        staging = rt / f".super-cache.{transaction_id}"
    transaction = {
        "schema": CACHE_TRANSACTION_SCHEMA,
        "runtime_root": str(rt.absolute()),
        "transaction_id": transaction_id,
        "stage_name": staging.name,
        "previous": previous,
        "target": target,
    }
    try:
        atomic_write_json(_transaction_path(rt), transaction)
        staging.mkdir()
    except OSError as exc:
        raise SetupError(SETUP_OVERWRITE_FAILED, f"active cache staging failed: {exc}") from exc
    try:
        for source_file, relative in plan:
            staged_file = staging / relative
            staged_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_file, staged_file)
            if staged_file.read_bytes() != source_file.read_bytes():
                raise SetupError(
                    SETUP_CACHE_INVALID, f"byte verification failed for {relative}"
                )
        _validate_active_cache(plan, staging)
        if _manifest_for_tree(staging) != target:
            raise SetupError(SETUP_CACHE_INVALID, f"staged cache changed during transaction: {staging}")
        _replace_tree(
            staging,
            destination,
            validate=lambda target: _validate_active_cache(plan, target),
            validate_backup=lambda path: _require_exact_manifest(path, previous),
        )
        _transaction_path(rt).unlink()
    except BaseException as exc:
        if _path_present(staging):
            try:
                _remove_owned_tree(staging, target, verify_bytes=False)
            except (OSError, SetupError):
                pass
        if not _path_present(rt / "super-cache.old") and (
            (previous is None and not _path_present(destination))
            or _manifest_matches(destination, previous)
        ) and not _path_present(staging):
            try:
                _transaction_path(rt).unlink()
            except OSError:
                pass
        if isinstance(exc, OSError):
            raise SetupError(SETUP_OVERWRITE_FAILED, f"active cache replacement failed: {exc}") from exc
        raise


def _plan_root_payloads(harness_root: Path) -> list[tuple[Path, Path]]:
    """Plan every shipped adapter's ROOT payload, relative to the root
    workspace."""
    adapters_dir = harness_root / "adapters"
    if not adapters_dir.is_dir():
        raise SetupError(SETUP_CONFIG_INVALID, f"adapter catalog missing: {adapters_dir}")
    planned: list[tuple[Path, Path]] = []
    for adapter in sorted(adapters_dir.iterdir()):
        if not adapter.is_dir():
            continue
        root_payload = adapter / "root"
        if not root_payload.is_dir():
            continue
        planned.extend(_plan_tree(root_payload))
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
    return bound


def _is_valid_installed_payload(
    target: Path,
    source_file: Path,
    *,
    harness_root: Path | None,
    runtime_root: Path | None,
    binding_templates: dict[Path, dict[str, Any]] | None,
) -> bool:
    """Return whether an existing destination is the valid installed form of
    a planned payload: byte-identical to the shipped source, or a ROOT hook
    binding correctly materialized for the exact current harness/runtime."""
    if target.is_file():
        try:
            if target.read_bytes() == source_file.read_bytes():
                return True
        except OSError:
            return False
    if harness_root is None or runtime_root is None or binding_templates is None:
        return False
    template = binding_templates.get(target)
    if template is None:
        return False
    try:
        copied = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    return copied == _materialized_binding(template, harness_root, runtime_root)


def _read_json_object(path: Path, *, description: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
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
        value = tomllib.loads(target.read_text(encoding="utf-8"))
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


def _preflight_shared_root_config(source: Path, target: Path, relative: Path) -> bool:
    """Validate one existing shared provider config; return whether handled."""
    if relative == CODEX_ROOT_CONFIG:
        _validate_existing_codex_config(target)
        return True
    if relative in SHARED_ROOT_HOOK_CONFIGS:
        _merge_root_hook_config(source, target)
        return True
    return False


def _write_root_hook_config(path: Path, value: dict[str, Any]) -> None:
    rendered = (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    atomic_write_bytes(path, rendered)


def _preflight_root_payloads(
    plan: list[tuple[Path, Path]],
    root_workspace: Path,
    *,
    overwrite: bool,
    harness_root: Path | None = None,
    runtime_root: Path | None = None,
    binding_templates: dict[Path, dict[str, Any]] | None = None,
) -> None:
    """Reject destination collisions before anything is written.

    ``--overwrite`` replaces only harness-owned catalog files. Existing Codex
    and Claude shared configuration is validated and merged or preserved. An
    existing destination that is the valid installed form of its planned
    payload is preserved, not a collision.
    """
    seen: set[Path] = set()
    plan_collisions: list[Path] = []
    for _, relative in plan:
        if relative in seen:
            plan_collisions.append(relative)
        seen.add(relative)
    if plan_collisions:
        raise SetupError(
            SETUP_ADAPTER_COLLISION,
            f"catalog payload collision at {plan_collisions[0]}",
        )
    collisions: list[Path] = []
    for source, relative in plan:
        target = root_workspace / relative
        _require_plain_root_destination(root_workspace, relative)
        if not target.exists():
            continue
        if _preflight_shared_root_config(source, target, relative):
            continue
        if overwrite:
            continue
        if _is_valid_installed_payload(
            target,
            source,
            harness_root=harness_root,
            runtime_root=runtime_root,
            binding_templates=binding_templates,
        ):
            continue
        collisions.append(target)
    if collisions:
        raise SetupError(
            SETUP_ADAPTER_COLLISION,
            f"destination collision (re-run with --overwrite to replace): {collisions[0]}",
        )


def _plan_root_hook_bindings(
    harness_root: Path,
    plan: list[tuple[Path, Path]],
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
            record = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
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
) -> list[Path]:
    """Atomically bind verified installed ROOT hook payloads to this runtime.

    The installed binding is accepted as the raw shipped template or as the
    template already materialized for this exact harness/runtime; a foreign or
    changed installed binding is a collision."""
    bound_paths: list[Path] = []
    for provider_id, relative, template in bindings:
        target = root_workspace / relative
        _require_plain_root_destination(root_workspace, relative)
        try:
            copied = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SetupError(
                SETUP_CONFIG_INVALID,
                f"installed {provider_id} hook has no harness binding: {exc}",
            ) from exc
        expected = _materialized_binding(template, harness_root, runtime_root)
        if copied != template and copied != expected:
            raise SetupError(
                SETUP_ADAPTER_COLLISION,
                f"installed ROOT hook binding for {provider_id} is neither its "
                "verified template nor a binding for this exact harness/runtime",
            )
        if copied != expected:
            try:
                atomic_write_json(target, expected)
            except OSError as exc:
                raise SetupError(
                    SETUP_CONFIG_INVALID,
                    f"cannot bind installed ROOT hook for {provider_id}: {exc}",
                ) from exc
        bound_paths.append(target)
    return bound_paths


def _install_root_payloads(
    harness_root: Path,
    root_workspace: Path,
    *,
    plan: list[tuple[Path, Path]],
    overwrite: bool,
    runtime_root: Path | None = None,
    binding_templates: dict[Path, dict[str, Any]] | None = None,
) -> tuple[list[Path], list[Path], list[Path]]:
    """Install ROOT payloads and return ``(installed, overwritten, merged)``.

    Never writes back into the shipped harness trees. Shared provider
    configuration is merged or preserved even under ``--overwrite``.
    An already-installed valid payload is preserved exactly on an unchanged
    re-run; ``--overwrite`` re-copies every harness-owned planned payload."""
    harness_identity = path_identity(harness_root)
    workspace_identity = path_identity(root_workspace)
    if workspace_identity == harness_identity or workspace_identity.startswith(
        harness_identity + os.sep
    ):
        raise SetupError(
            SETUP_CONFIG_INVALID,
            f"root workspace must not be inside the harness root: {root_workspace}",
        )
    installed: list[Path] = []
    overwritten: list[Path] = []
    merged: list[Path] = []
    for source_file, relative in plan:
        target = root_workspace / relative
        _require_plain_root_destination(root_workspace, relative)
        if target.exists() and relative == CODEX_ROOT_CONFIG:
            _validate_existing_codex_config(target)
            installed.append(target)
            continue
        if target.exists() and relative in SHARED_ROOT_HOOK_CONFIGS:
            combined = _merge_root_hook_config(source_file, target)
            current = _read_json_object(
                target, description="existing hook configuration"
            )
            if combined != current:
                _write_root_hook_config(target, combined)
                merged.append(target)
            installed.append(target)
            continue
        if not overwrite and _is_valid_installed_payload(
            target,
            source_file,
            harness_root=harness_root,
            runtime_root=runtime_root,
            binding_templates=binding_templates,
        ):
            installed.append(target)
            continue
        replaced = target.exists()
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, target)
        if target.read_bytes() != source_file.read_bytes():
            raise SetupError(
                SETUP_OVERWRITE_FAILED if overwrite else SETUP_CONFIG_INVALID,
                f"byte verification failed for {target}",
            )
        installed.append(target)
        if replaced:
            overwritten.append(target)
    return installed, overwritten, merged


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


def _start_monitor_locked(
    harness_root: Path,
    rt: Path,
    config_identity: str,
) -> dict[str, Any]:
    """Start and record the monitor; the caller must hold the monitor-record lock."""
    argv = processes.python_argv("orchestrator_harness.monitor")
    append_activity(rt, "process.spawn_requested", component="setup", argv=argv)
    child = processes.spawn_detached(argv, cwd=str(harness_root))
    identity = processes.process_identity(child.pid)
    if identity is None:
        raise ConfigError("cannot record monitor process identity")
    record = {
        "schema": MONITOR_SCHEMA,
        "config_identity": config_identity,
        "pid": identity["pid"],
        "creation_time": identity["creation_time"],
        "started_at": iso_utc(),
        "health": "starting",
        "last_heartbeat_at": iso_utc(),
        "watched_lane_count": 0,
        "diagnostics": [],
        "stop_requested": False,
    }
    atomic_write_json(monitor_record_path(rt), record)
    append_activity(
        rt, "process.spawned", component="setup", pid=identity["pid"],
        creation_time=identity["creation_time"],
    )
    return record


def _start_monitor(
    harness_root: Path,
    rt: Path,
    config: HarnessConfig,
    config_identity: str,
) -> dict[str, Any]:
    """Start the one persistent monitor under the monitor-record lock."""
    record_path = monitor_record_path(rt)
    with RecordLock(record_path):
        existing = read_monitor_record(rt)
        if existing is not None:
            pid = existing.get("pid")
            creation = existing.get("creation_time")
            if (
                isinstance(pid, int)
                and processes.identity_matches(pid, creation)
                and not existing.get("stop_requested", False)
            ):
                raise ConfigError(SETUP_MONITOR_ALREADY_RUNNING)
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
    return (datetime.now(timezone.utc) - parsed).total_seconds() <= HEARTBEAT_STALENESS_SECONDS


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
    state = read_runtime_state(rt)
    if state is None or state.get("state") != "OPEN":
        current = state.get("state") if state is not None else "missing"
        return _failure(
            MONITOR_RUNTIME_NOT_OPEN,
            f"runtime state is not OPEN: {current}",
            "open the runtime with `harness setup` before recovering the monitor",
        )

    record_path = monitor_record_path(rt)
    try:
        config_identity = compute_config_identity(config, manifest)
        with RecordLock(record_path):
            record = read_monitor_record(rt)
            if record is None:
                _start_monitor_locked(harness_root, rt, config_identity)
                return {
                    "ok": True,
                    "code": MONITOR_RECOVERED,
                    "summary": "monitor record was missing; a fresh monitor started",
                    "evidence_paths": [str(record_path)],
                    "next_action": "confirm the monitor heartbeat in MONITOR.json",
                }

            pid = record.get("pid")
            creation = record.get("creation_time")
            stop_requested = record.get("stop_requested", False)
            health = record.get("health")

            if not isinstance(pid, int) or not creation:
                return {
                    "ok": False,
                    "code": MONITOR_IDENTITY_UNPROVEN,
                    "summary": "recorded monitor PID has no usable creation identity",
                    "evidence_paths": [str(record_path)],
                    "next_action": "remove the unproven MONITOR.json and re-run monitor-recover",
                }

            alive = processes.identity_matches(pid, creation)

            if stop_requested or health == "STOPPED":
                if alive:
                    return {
                        "ok": False,
                        "code": MONITOR_STOP_INCOMPLETE,
                        "summary": "monitor is marked stopped but the recorded process is still alive",
                        "evidence_paths": [str(record_path)],
                        "next_action": "stop the exact recorded process, then re-run monitor-recover",
                    }
                return {
                    "ok": True,
                    "code": MONITOR_DELIBERATELY_STOPPED,
                    "summary": "monitor is deliberately stopped; nothing started",
                    "evidence_paths": [str(record_path)],
                    "next_action": "start the monitor manually when it is needed again",
                }

            if alive:
                if _heartbeat_is_fresh(record):
                    return {
                        "ok": True,
                        "code": MONITOR_HEALTHY,
                        "summary": "monitor is alive with a fresh heartbeat; nothing changed",
                        "evidence_paths": [str(record_path)],
                        "next_action": "no action; the monitor is healthy",
                    }
                terminated = processes.terminate_process(pid, creation, force=True)
                if not terminated or processes.identity_matches(pid, creation):
                    return {
                        "ok": False,
                        "code": MONITOR_CLEANUP_UNPROVEN,
                        "summary": "hung monitor termination could not be proven",
                        "evidence_paths": [str(record_path)],
                        "next_action": "verify the exact recorded process is gone, then re-run monitor-recover",
                    }
                _start_monitor_locked(harness_root, rt, config_identity)
                return {
                    "ok": True,
                    "code": MONITOR_RECOVERED,
                    "summary": "hung monitor was force-stopped and replaced",
                    "evidence_paths": [str(record_path)],
                    "next_action": "confirm the replacement monitor heartbeat in MONITOR.json",
                }

            _start_monitor_locked(harness_root, rt, config_identity)
            return {
                "ok": True,
                "code": MONITOR_RECOVERED,
                "summary": "dead monitor was replaced with a fresh one",
                "evidence_paths": [str(record_path)],
                "next_action": "confirm the replacement monitor heartbeat in MONITOR.json",
            }
    except ConfigError as exc:
        return _failure(
            SETUP_CONFIG_INVALID,
            str(exc),
            "resolve the error and re-run monitor-recover",
        )
    except OSError as exc:
        return _failure(
            SETUP_CONFIG_INVALID,
            str(exc),
            "resolve the error and re-run monitor-recover",
        )


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
        if config.root_workspace is None:
            raise SetupError(
                SETUP_CONFIG_INVALID,
                "harness config does not declare root_workspace",
            )
        _require_plain_workspace_boundary(rt, code=SETUP_CONFIG_INVALID)
        harness_identity = path_identity(harness_root)
        workspace_identity = path_identity(config.root_workspace)
        if workspace_identity == harness_identity or workspace_identity.startswith(
            harness_identity + os.sep
        ):
            raise SetupError(
                SETUP_CONFIG_INVALID,
                f"root workspace must not be inside the harness root: {config.root_workspace}",
            )
        root_plan = _plan_root_payloads(harness_root)
        root_bindings = _plan_root_hook_bindings(harness_root, root_plan)
        binding_templates = {
            config.root_workspace / relative: template
            for _, relative, template in root_bindings
        }
        _preflight_root_payloads(
            root_plan,
            config.root_workspace,
            overwrite=overwrite,
            harness_root=harness_root,
            runtime_root=rt,
            binding_templates=binding_templates,
        )
        cache_plan = _plan_active_cache(harness_root, root_plan=root_plan)
        _inspect_active_cache(cache_plan, rt, overwrite=overwrite)
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
        for parent in ("worktrees", "monitor", "epochs", "resources"):
            (rt / parent).mkdir(parents=True, exist_ok=True)
        if config.profile == "managed":
            (rt / "manager").mkdir(parents=True, exist_ok=True)

        state = read_runtime_state(rt)
        if state is None or state.get("state") == "CLOSED":
            set_runtime_state(rt, "OPEN")
        elif state.get("state") not in ("OPEN", "SHUTTING_DOWN"):
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

        _install_active_cache(harness_root, rt, plan=cache_plan, overwrite=overwrite)
        installed, overwritten, merged = _install_root_payloads(
            harness_root,
            config.root_workspace,
            plan=root_plan,
            overwrite=overwrite,
            runtime_root=rt,
            binding_templates=binding_templates,
        )
        installed_set = set(installed)
        if any(config.root_workspace / relative not in installed_set for _, relative, _ in root_bindings):
            raise SetupError(
                SETUP_CONFIG_INVALID,
                "installed ROOT hook payload is missing its planned harness binding",
            )
        _materialize_root_hook_bindings(
            config.root_workspace,
            harness_root,
            rt,
            root_bindings,
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

        try:
            monitor = _start_monitor(
                harness_root,
                rt,
                config,
                compute_config_identity(config, manifest),
            )
        except ConfigError as exc:
            if str(exc) == SETUP_MONITOR_ALREADY_RUNNING:
                return {
                    "ok": True,
                    "code": SETUP_MONITOR_ALREADY_RUNNING,
                    "summary": "a live monitor already exists; nothing started",
                    "evidence_paths": [str(monitor_record_path(rt))],
                    "next_action": "proceed; the runtime is already monitored",
                }
            raise
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
