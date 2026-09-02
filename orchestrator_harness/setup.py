"""``harness setup``: idempotent one-time integration.

Installs the ROOT payloads, builds the active cache, writes the active
resource manifest + lease dir, and starts the persistent monitor.  It starts
no lane or provider.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from . import processes
from .config import (
    ConfigError,
    HarnessConfig,
    ResourceManifest,
    find_harness_root,
    load_config,
    load_resource_manifest,
)
from .core import iso_utc, read_json, require_schema
from .epochs import MANAGER_QUEUE_SCHEMA, manager_queue_path, read_current_epoch
from .records import RecordLock, atomic_write_json, read_record, write_record

RUNTIME_STATE_SCHEMA = "runtime-state/v1"
MONITOR_SCHEMA = "monitor/v1"
RESOURCE_MANIFEST_SCHEMA = "resource-manifest/v1"

SETUP_CONFIG_INVALID = "SETUP_CONFIG_INVALID"
SETUP_CACHE_INVALID = "SETUP_CACHE_INVALID"
SETUP_RESOURCE_MANIFEST_INVALID = "SETUP_RESOURCE_MANIFEST_INVALID"
SETUP_ADAPTER_COLLISION = "SETUP_ADAPTER_COLLISION"
SETUP_OVERWRITE_FAILED = "SETUP_OVERWRITE_FAILED"
SETUP_MONITOR_ALREADY_RUNNING = "SETUP_MONITOR_ALREADY_RUNNING"


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


def _copy_tree(source: Path, destination: Path, *, overwrite: bool) -> list[Path]:
    """Copy one directory tree; a destination-file collision rejects the whole
    copy unless ``overwrite`` is set.  Returns the copied file paths."""
    if not source.is_dir():
        raise ConfigError(f"source tree missing: {source}")
    planned: list[tuple[Path, Path]] = []
    for item in source.rglob("*"):
        if item.is_file():
            relative = item.relative_to(source)
            planned.append((item, destination / relative))
    if not overwrite:
        collisions = [target for _, target in planned if target.exists()]
        if collisions:
            raise ConfigError(
                f"destination collision (re-run with --overwrite to replace): {collisions[0]}"
            )
    copied: list[Path] = []
    for source_file, target in planned:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, target)
        copied.append(target)
    return copied


def _byte_verify_copy(source: Path, destination: Path) -> None:
    """Copy a tree and verify every destination file matches its source bytes."""
    if not source.is_dir():
        raise ConfigError(f"source tree missing: {source}")
    for item in source.rglob("*"):
        if not item.is_file():
            continue
        relative = item.relative_to(source)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(item, target)
        if target.read_bytes() != item.read_bytes():
            raise ConfigError(f"byte verification failed for {target}")


def _install_active_cache(harness_root: Path, rt: Path, *, overwrite: bool) -> None:
    """Stage and byte-verify the active super-cache, then place it atomically."""
    source_cache = harness_root / "super-cache"
    if not source_cache.is_dir():
        raise ConfigError(f"shipped super-cache missing: {source_cache}")
    destination = rt / "super-cache"
    if destination.is_dir():
        # Preserve an existing valid cache; never silently overwrite it.
        return
    staging = Path(tempfile.mkdtemp(prefix=".super-cache.", dir=str(rt)))
    try:
        _byte_verify_copy(source_cache / "workspace", staging / "workspace")
        if (source_cache / "custom").is_dir():
            _byte_verify_copy(source_cache / "custom", staging / "custom")
        adapters_dir = harness_root / "adapters"
        if adapters_dir.is_dir():
            for adapter in sorted(adapters_dir.iterdir()):
                payload = adapter / "super-cache"
                if not payload.is_dir():
                    continue
                _byte_verify_copy(
                    payload, staging / "adapter-payloads" / adapter.name
                )
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _install_root_payloads(
    harness_root: Path, root_workspace: Path, *, overwrite: bool
) -> list[Path]:
    """Materialize each shipped adapter's ROOT payload into root_workspace."""
    adapters_dir = harness_root / "adapters"
    if not adapters_dir.is_dir():
        raise ConfigError(f"adapter catalog missing: {adapters_dir}")
    installed: list[Path] = []
    for adapter in sorted(adapters_dir.iterdir()):
        root_payload = adapter / "root"
        if not root_payload.is_dir():
            continue
        try:
            installed.extend(_copy_tree(root_payload, root_workspace, overwrite=overwrite))
        except ConfigError as exc:
            raise ConfigError(f"{SETUP_ADAPTER_COLLISION}: {exc}") from exc
    return installed


def _check_launcher_bindings(harness_root: Path) -> list[Path]:
    """Verify the registered launcher bindings exist and expose the strict
    symbols; register any custom adapter binding found in the catalog."""
    registered_dir = harness_root / "orchestrator_harness" / "provider_adapters"
    registered_dir.mkdir(parents=True, exist_ok=True)
    adapters_dir = harness_root / "adapters"
    checked: list[Path] = []
    if not adapters_dir.is_dir():
        return checked
    for adapter in sorted(adapters_dir.iterdir()):
        source_binding = adapter / "harness" / "launcher_binding.py"
        if not source_binding.is_file():
            continue
        target_binding = registered_dir / adapter.name / "launcher_binding.py"
        if not target_binding.is_file() or (
            target_binding.read_bytes() != source_binding.read_bytes()
        ):
            target_binding.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_binding, target_binding)
        checked.append(target_binding)
    for binding in sorted(registered_dir.rglob("launcher_binding.py")):
        module = _load_binding(binding)
        for symbol in ("PROVIDER_ID", "ADAPTER_VERSION", "build_argv", "parse_line"):
            if not hasattr(module, symbol):
                raise ConfigError(
                    f"launcher binding {binding} lacks required symbol {symbol}"
                )
    return checked


def _load_binding(path: Path) -> Any:
    import importlib.util

    spec = importlib.util.spec_from_file_location("_harness_binding", path)
    if spec is None or spec.loader is None:
        raise ConfigError(f"cannot load launcher binding: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _start_monitor(harness_root: Path, rt: Path, config: HarnessConfig) -> dict[str, Any]:
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
        argv = processes.python_argv("orchestrator_harness.monitor")
        child = processes.spawn_detached(argv, cwd=str(harness_root))
        identity = processes.process_identity(child.pid)
        if identity is None:
            raise ConfigError("cannot record monitor process identity")
        record = {
            "schema": MONITOR_SCHEMA,
            "config_identity": config.profile,
            "pid": identity["pid"],
            "creation_time": identity["creation_time"],
            "started_at": iso_utc(),
            "health": "starting",
            "last_heartbeat_at": iso_utc(),
            "stop_requested": False,
        }
        atomic_write_json(record_path, record)
    return record


def run_setup(*, overwrite: bool = False) -> dict[str, Any]:
    """Execute ``harness setup`` and return the structured result."""
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
        manifest = load_resource_manifest(harness_root)
    except (ConfigError, OSError, ValueError) as exc:
        return {"ok": False, "code": SETUP_CONFIG_INVALID, "summary": str(exc),
                "evidence_paths": [], "next_action": "fix harness-config.json and resource-manifest.json, then re-run setup"}

    rt = config.runtime_root
    try:
        if rt.is_symlink():
            raise ConfigError(
                f"runtime root must not be a symbolic link: {rt}"
            )
        for parent in ("worktrees", "monitor", "epochs", "resources"):
            (rt / parent).mkdir(parents=True, exist_ok=True)
        if config.profile == "managed":
            (rt / "manager").mkdir(parents=True, exist_ok=True)

        state = read_runtime_state(rt)
        if state is None or state.get("state") == "CLOSED":
            set_runtime_state(rt, "OPEN")
        elif state.get("state") not in ("OPEN", "SHUTTING_DOWN"):
            raise ConfigError(f"unexpected runtime state: {state.get('state')}")

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
                    raise ConfigError("manager queue is malformed; remove it and re-run setup")

        _install_active_cache(harness_root, rt, overwrite=overwrite)
        _install_root_payloads(harness_root, config.root_workspace, overwrite=overwrite)
        _check_launcher_bindings(harness_root)

        manifest_path = rt / "resources" / "RESOURCE_MANIFEST.json"
        source_manifest = harness_root / "resource-manifest.json"
        if manifest_path.is_file():
            try:
                active = read_record(manifest_path, RESOURCE_MANIFEST_SCHEMA)
                if active.get("resources") != [dict(item) for item in manifest.resources]:
                    if read_current_epoch(rt) is not None:
                        raise ConfigError(
                            "resource manifest changed while an epoch is active; "
                            "shut down the runtime before changing it"
                        )
                    if any((rt / "resources" / "leases").glob("*")):
                        raise ConfigError(
                            "resource manifest changed while a lease is live; "
                            "shut down the runtime before changing it"
                        )
                    atomic_write_json(manifest_path, {
                        "schema": RESOURCE_MANIFEST_SCHEMA,
                        "resources": [dict(item) for item in manifest.resources],
                    })
            except (OSError, ValueError):
                raise ConfigError("active resource manifest is malformed")
        else:
            atomic_write_json(manifest_path, {
                "schema": RESOURCE_MANIFEST_SCHEMA,
                "resources": [dict(item) for item in manifest.resources],
            })
        (rt / "resources" / "leases").mkdir(parents=True, exist_ok=True)

        try:
            monitor = _start_monitor(harness_root, rt, config)
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
    except ConfigError as exc:
        return {"ok": False, "code": SETUP_CONFIG_INVALID, "summary": str(exc),
                "evidence_paths": [], "next_action": "resolve the named target or re-run with --overwrite"}

    return {
        "ok": True,
        "code": "SETUP_OK",
        "summary": "runtime is OPEN and the active resource manifest exists",
        "evidence_paths": [
            str(rt / "resources" / "RESOURCE_MANIFEST.json"),
            str(monitor_record_path(rt)),
        ],
        "next_action": "bootstrap a lane with `lane bootstrap`",
    }
