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

import json
import os
import shutil
import sys
import tempfile
import tomllib
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import processes
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
}

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

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


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
    for item in source.rglob("*"):
        if not item.is_file() or "__pycache__" in item.parts:
            continue
        planned.append((item, item.relative_to(source)))
    return planned


def _plan_active_cache(harness_root: Path) -> list[tuple[Path, Path]]:
    """Plan the active cache: ``super-cache/workspace`` plus every
    ``adapters/<provider-id>/super-cache`` payload, each relative to
    ``<rt>/super-cache``."""
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
    return planned


def _validate_active_cache(
    plan: list[tuple[Path, Path]], destination: Path
) -> None:
    """Reject an existing active cache that is incomplete or differs from the
    shipped catalog; a valid cache is preserved exactly."""
    for source_file, relative in plan:
        target = destination / relative
        if not target.is_file():
            raise SetupError(
                SETUP_CACHE_INVALID, f"active cache incomplete: missing {relative}"
            )
        if target.read_bytes() != source_file.read_bytes():
            raise SetupError(
                SETUP_CACHE_INVALID,
                f"active cache malformed: {relative} differs from the shipped source",
            )


def _replace_tree(staging: Path, destination: Path) -> None:
    """Atomically replace ``destination`` with the fully staged ``staging``."""
    backup = destination.with_name(f"{destination.name}.old")
    if backup.exists():
        shutil.rmtree(backup, ignore_errors=True)
    if destination.exists():
        _replace_with_retry(destination, backup)
    try:
        _replace_with_retry(staging, destination)
    except BaseException:
        if backup.exists() and not destination.exists():
            _replace_with_retry(backup, destination)
        raise
    if backup.exists():
        shutil.rmtree(backup, ignore_errors=True)


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
    if destination.is_dir():
        try:
            _validate_active_cache(plan, destination)
            return
        except SetupError:
            if not overwrite:
                raise
    staging = Path(tempfile.mkdtemp(prefix=".super-cache.", dir=str(rt)))
    try:
        for source_file, relative in plan:
            target = staging / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_file, target)
            if target.read_bytes() != source_file.read_bytes():
                raise SetupError(
                    SETUP_CACHE_INVALID, f"byte verification failed for {relative}"
                )
        _replace_tree(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
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
        if config.root_workspace.is_symlink() or rt.is_symlink():
            raise SetupError(
                SETUP_CONFIG_INVALID,
                f"workspace and runtime root must not be symbolic links: {rt}",
            )
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
        cache_plan = _plan_active_cache(harness_root)
        destination = rt / "super-cache"
        if destination.is_dir():
            try:
                _validate_active_cache(cache_plan, destination)
            except SetupError:
                if not overwrite:
                    raise
        _check_launcher_bindings(harness_root)
    except SetupError as exc:
        return _failure(
            exc.code,
            str(exc),
            "resolve the named target; --overwrite replaces only harness-owned payloads",
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
                    if any((rt / "resources" / "leases").glob("*")):
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
            "resolve the named target; --overwrite replaces only harness-owned payloads",
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
