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

import os
import shutil
import sys
import tempfile
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
from .records import RecordLock, atomic_write_json, read_record

RUNTIME_STATE_SCHEMA = "runtime-state/v1"
MONITOR_SCHEMA = "monitor/v1"
RESOURCE_MANIFEST_SCHEMA = "resource-manifest/v1"

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
        os.replace(destination, backup)
    try:
        os.replace(staging, destination)
    except BaseException:
        if backup.exists() and not destination.exists():
            os.replace(backup, destination)
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


def _preflight_root_payloads(
    plan: list[tuple[Path, Path]],
    root_workspace: Path,
    *,
    overwrite: bool,
) -> None:
    """Reject any destination collision across the whole ROOT payload plan
    before anything is written; ``--overwrite`` replaces only catalog-planned
    files."""
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
    if overwrite:
        return
    collisions = [
        root_workspace / relative
        for _, relative in plan
        if (root_workspace / relative).exists()
    ]
    if collisions:
        raise SetupError(
            SETUP_ADAPTER_COLLISION,
            f"destination collision (re-run with --overwrite to replace): {collisions[0]}",
        )


def _install_root_payloads(
    harness_root: Path,
    root_workspace: Path,
    *,
    plan: list[tuple[Path, Path]],
    overwrite: bool,
) -> tuple[list[Path], list[Path]]:
    """Install the preflighted ROOT payloads and return ``(installed,
    overwritten)`` paths.  Never writes back into the shipped harness trees."""
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
    for source_file, relative in plan:
        target = root_workspace / relative
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
    return installed, overwritten


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
    """Require exact PROVIDER_ID plus ADAPTER_VERSION/build_argv/parse_line."""
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
    for symbol in ("build_argv", "parse_line"):
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
        _preflight_root_payloads(root_plan, config.root_workspace, overwrite=overwrite)
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
            "resolve the named target or re-run with --overwrite",
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
        _, overwritten = _install_root_payloads(
            harness_root,
            config.root_workspace,
            plan=root_plan,
            overwrite=overwrite,
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
            "resolve the named target or re-run with --overwrite",
        )
    except ConfigError as exc:
        return _failure(
            SETUP_CONFIG_INVALID,
            str(exc),
            "resolve the named target or re-run with --overwrite",
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
    return result
