"""Configuration: harness-config/v1, resource-manifest/v1, and config identity.

The configuration is a closed two-key set in ``<harness-root>/harness-config.json``
plus the ROOT-authored resource manifest.  ROOT never passes paths, feature
flags, or a profile again on any later command; the stored config selects
everything.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .core import read_json, sha256_hex

CONFIG_SCHEMA = "harness-config/v1"  # logical schema name; never a literal field
MANIFEST_SCHEMA = "resource-manifest/v1"  # literal schema field in the manifest
RUNTIME_DIR_NAME = ".harness-runtime"
PROFILE_MANAGED = "managed"
PROFILE_PLAIN = "plain"


class ConfigError(ValueError):
    """The harness configuration or resource manifest is invalid."""


def path_identity(path: str | Path) -> str:
    """Return a platform-native, normalized identity for a filesystem path."""

    resolved = Path(path).resolve(strict=False)
    return os.path.normcase(os.path.normpath(str(resolved)))


def same_path(left: str | Path, right: str | Path) -> bool:
    return path_identity(left) == path_identity(right)


@dataclass(frozen=True)
class HarnessConfig:
    harness_root: Path
    root_workspace: Path | None = None
    managed_coordination: str = "enabled"
    config_path: Path | None = None
    suite_root: Path | None = None
    run_globs: tuple[str, ...] = ()
    workspace_relpath: str = ".agent-workspace"
    output_dir: Path | None = None
    poll_interval_seconds: float = 1.0
    watch_timeout_seconds: float = 60.0
    request_warning_seconds: int = 120
    request_critical_seconds: int = 30
    process_start_tolerance_seconds: int = 2
    max_json_bytes: int = 4_000_000
    max_jsonl_tail_bytes: int = 512_000
    stable_read_retries: int = 4
    stable_read_delay_seconds: float = 0.03
    record_paths: tuple[str, ...] = ()
    record_manifests: tuple[str, ...] = ()
    legacy_config_diagnostics: tuple[str, ...] = ()

    @property
    def profile(self) -> str:
        return PROFILE_MANAGED if self.managed_coordination == "enabled" else PROFILE_PLAIN

    @property
    def runtime_root(self) -> Path:
        return self.root_workspace / RUNTIME_DIR_NAME

    @property
    def migration_diagnostics(self) -> tuple[str, ...]:
        """Explicit diagnostics for removed policy keys, never live settings."""

        return self.legacy_config_diagnostics

    @property
    def forbidden_output_roots(self) -> tuple[Path, ...]:
        return (
            self.suite_root / ".agent-workspace",
            self.suite_root / "fresh-experiments",
            self.suite_root / "BYO-Firmware-MCP",
        )


@dataclass(frozen=True)
class ResourceManifest:
    resources: tuple[dict[str, Any], ...] = ()

    def resource_ids(self) -> tuple[str, ...]:
        return tuple(str(item["id"]) for item in self.resources)

    def is_declared(self, resource_id: str) -> bool:
        return resource_id in self.resource_ids()


def find_harness_root(start: str | os.PathLike[str] | None = None) -> Path:
    """Walk upward from ``start`` (default: the current directory) to the
    directory that holds ``harness-config.json``."""
    current = Path(start or os.getcwd()).absolute()
    if not current.is_dir():
        current = current.parent
    for candidate in (current, *current.parents):
        if (candidate / "harness-config.json").is_file():
            return candidate
    raise ConfigError(
        f"harness root not found: no harness-config.json at or above {current}"
    )


def _number(raw: dict[str, Any], key: str, default: float, *, minimum: float) -> float:
    value = raw.get(key, default)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ConfigError(f"{key} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ConfigError(f"{key} must be finite")
    if result < minimum:
        raise ConfigError(f"{key} must be >= {minimum}")
    return result


def _integer(raw: dict[str, Any], key: str, default: int, *, minimum: int) -> int:
    value = raw.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{key} must be an integer")
    if value < minimum:
        raise ConfigError(f"{key} must be >= {minimum}")
    return value


def _declared_relative_paths(
    raw: dict[str, Any], key: str, aliases: tuple[str, ...] = ()
) -> tuple[str, ...]:
    value: object = raw.get(key)
    if value is None:
        for alias in aliases:
            if alias in raw:
                value = raw[alias]
                break
    if value is None:
        return ()
    if not isinstance(value, list) or any(
        not isinstance(item, str)
        or not item.strip()
        or Path(item).is_absolute()
        or ".." in Path(item).parts
        for item in value
    ):
        raise ConfigError(f"{key} must be a list of safe relative paths")
    return tuple(item.strip() for item in value)


def _load_legacy_config(
    path: str | Path,
    *,
    harness_root: Path | None = None,
) -> HarnessConfig:
    """Read a legacy config.json and derive the legacy settings."""
    config_path = Path(path).resolve()
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"cannot load config {config_path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError("config root must be a JSON object")

    root = (harness_root or Path(__file__).resolve().parent).resolve()
    base = config_path.parent
    suite_value = raw.get("suite_root", "..")
    if not isinstance(suite_value, str) or not suite_value:
        raise ConfigError("suite_root must be a non-empty path string")
    suite_root = (base / suite_value).resolve()

    globs = raw.get("run_globs", ["worktrees/*"])
    if (
        not isinstance(globs, list)
        or not globs
        or any(not isinstance(item, str) or not item for item in globs)
    ):
        raise ConfigError("run_globs must be a non-empty list of strings")
    if any(Path(item).is_absolute() or ".." in Path(item).parts for item in globs):
        raise ConfigError("run_globs must be relative and may not contain '..'")

    workspace = raw.get("workspace_relpath", ".agent-workspace")
    if (
        not isinstance(workspace, str)
        or not workspace
        or Path(workspace).is_absolute()
        or ".." in Path(workspace).parts
    ):
        raise ConfigError("workspace_relpath must be a safe relative path")

    record_paths = _declared_relative_paths(raw, "record_paths", ("observation_paths",))
    record_manifests = _declared_relative_paths(
        raw, "record_manifests", ("record_manifest_paths",)
    )

    output_value = raw.get("output_dir", ".state")
    if not isinstance(output_value, str) or not output_value:
        raise ConfigError("output_dir must be a non-empty path string")
    output = Path(output_value)
    if not output.is_absolute():
        output = root / output

    warning = _integer(raw, "request_warning_seconds", 120, minimum=1)
    critical = _integer(raw, "request_critical_seconds", 30, minimum=0)
    if critical >= warning:
        raise ConfigError("request_critical_seconds must be less than warning")

    process_tolerance = _integer(raw, "process_start_tolerance_seconds", 2, minimum=0)
    if process_tolerance > 2:
        raise ConfigError("process_start_tolerance_seconds must be <= 2")

    max_json_bytes = _integer(raw, "max_json_bytes", 4_000_000, minimum=1024)
    max_jsonl_tail_bytes = _integer(raw, "max_jsonl_tail_bytes", 512_000, minimum=1024)
    stable_read_retries = _integer(raw, "stable_read_retries", 4, minimum=1)
    removed_keys = (
        "manager_heartbeat_timeout_seconds",
        "attention_logging_enabled",
        "attention_epoch_id",
        "attention_sprint_lifetime_seconds",
        "attention_tolerance_seconds",
        "manager_review_interval_seconds",
        "lane_no_progress_seconds",
        "watcher_ack_policy",
        "managed_runtime_path",
        "pending_notification_path",
    )
    legacy_diagnostics = tuple(
        f"legacy configuration key {key!r} is retained only for read compatibility and has no S4 runtime effect"
        for key in removed_keys
        if key in raw
    )

    return HarnessConfig(
        harness_root=root,
        config_path=config_path,
        suite_root=suite_root,
        run_globs=tuple(globs),
        workspace_relpath=workspace,
        output_dir=output,
        poll_interval_seconds=_number(raw, "poll_interval_seconds", 1.0, minimum=0.05),
        watch_timeout_seconds=_number(raw, "watch_timeout_seconds", 60.0, minimum=0.1),
        request_warning_seconds=warning,
        request_critical_seconds=critical,
        process_start_tolerance_seconds=process_tolerance,
        max_json_bytes=max_json_bytes,
        max_jsonl_tail_bytes=max_jsonl_tail_bytes,
        stable_read_retries=stable_read_retries,
        stable_read_delay_seconds=_number(
            raw, "stable_read_delay_seconds", 0.03, minimum=0
        ),
        record_paths=record_paths,
        record_manifests=record_manifests,
        legacy_config_diagnostics=legacy_diagnostics,
    )


def _load_v2_config(harness_root: str | os.PathLike[str]) -> HarnessConfig:
    """Read and validate the stored harness-config.json.

    The stored record is the closed two-key ``harness-config/v1`` shape:
    required absolute ``root_workspace`` and optional ``managed_coordination``
    (``enabled`` or ``disabled``, default ``enabled``).  The record carries no
    literal ``schema`` field and no other keys are legal.
    """
    root = Path(harness_root).absolute()
    path = root / "harness-config.json"
    if not path.is_file():
        raise ConfigError(f"harness config missing: {path}")
    try:
        record = read_json(path)
    except (OSError, ValueError) as exc:
        raise ConfigError(f"harness config unreadable: {path}: {exc}") from exc
    unknown = sorted(set(record) - {"root_workspace", "managed_coordination"})
    if unknown:
        raise ConfigError(
            f"harness config unknown key {unknown[0]!r} "
            f"(closed keys: root_workspace, managed_coordination): {path}"
        )
    root_workspace = record.get("root_workspace")
    if not isinstance(root_workspace, str) or not root_workspace:
        raise ConfigError(f"harness config root_workspace missing: {path}")
    workspace = Path(root_workspace).expanduser()
    if not workspace.is_absolute():
        raise ConfigError(f"harness config root_workspace must be absolute: {path}")
    if workspace.is_symlink():
        raise ConfigError(
            f"harness config root_workspace must not be a symbolic link: {path}"
        )
    managed = record.get("managed_coordination", "enabled")
    if managed not in ("enabled", "disabled"):
        raise ConfigError(
            f"harness config managed_coordination must be enabled or disabled: {path}"
        )
    return HarnessConfig(
        harness_root=root,
        root_workspace=workspace,
        managed_coordination=managed,
    )


def load_config(
    path: str | os.PathLike[str],
    *,
    harness_root: str | os.PathLike[str] | None = None,
) -> HarnessConfig:
    """Load the harness configuration in either supported form.

    `load_config(<harness-root>)` reads the stored `harness-config.json`
    (harness-config/v1).  `load_config(<config-file>, harness_root=<root>)`
    (or a bare legacy config file path) reads a legacy `config.json` and
    derives the legacy settings.
    """
    candidate = Path(path)
    if harness_root is not None or candidate.is_file():
        root = Path(harness_root) if harness_root is not None else None
        return _load_legacy_config(candidate, harness_root=root)
    return _load_v2_config(candidate)


def load_resource_manifest(harness_root: str | os.PathLike[str]) -> ResourceManifest:
    """Read and validate ``<harness-root>/resource-manifest.json``.

    The stored record is the closed ``resource-manifest/v1`` shape: a literal
    ``schema`` field equal to ``resource-manifest/v1`` plus a single
    ``resources`` list.  Every nonempty entry is a closed object with a
    nonempty literal ``id`` and ``exclusive: true``; duplicate IDs and unknown
    fields are invalid.
    """
    root = Path(harness_root).absolute()
    path = root / "resource-manifest.json"
    if not path.is_file():
        raise ConfigError(f"resource manifest missing: {path}")
    try:
        record = read_json(path)
    except (OSError, ValueError) as exc:
        raise ConfigError(f"resource manifest unreadable: {path}: {exc}") from exc
    unknown = sorted(set(record) - {"schema", "resources"})
    if unknown:
        raise ConfigError(
            f"resource manifest unknown key {unknown[0]!r} "
            f"(closed keys: schema, resources): {path}"
        )
    if record.get("schema") != MANIFEST_SCHEMA:
        raise ConfigError(
            f"resource manifest schema must be {MANIFEST_SCHEMA!r}: {path}"
        )
    resources = record.get("resources")
    if not isinstance(resources, list):
        raise ConfigError(f"resource manifest resources must be a list: {path}")
    validated: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in resources:
        if not isinstance(item, dict):
            raise ConfigError(f"resource manifest entry must be an object: {path}")
        entry_unknown = sorted(set(item) - {"id", "exclusive"})
        if entry_unknown:
            raise ConfigError(
                f"resource manifest entry unknown key {entry_unknown[0]!r} "
                f"(closed keys: id, exclusive): {path}"
            )
        resource_id = item.get("id")
        if not isinstance(resource_id, str) or not resource_id:
            raise ConfigError(f"resource manifest entry id missing: {path}")
        if resource_id in seen:
            raise ConfigError(f"resource manifest duplicate id: {resource_id}")
        seen.add(resource_id)
        exclusive = item.get("exclusive")
        if exclusive is not True:
            raise ConfigError(
                f"resource manifest entry exclusive must be true: {resource_id}"
            )
        validated.append({"id": resource_id, "exclusive": exclusive})
    return ResourceManifest(tuple(validated))


def compute_config_identity(
    config: HarnessConfig, manifest: ResourceManifest
) -> str:
    """Return the epoch-immutable configuration identity.

    Changing any epoch-immutable fact (root_workspace, profile, the declared
    exclusive-resource set, or the record schema version) changes this digest
    and forces a new epoch.
    """
    payload = {
        "root_workspace": str(config.root_workspace),
        "managed_coordination": config.managed_coordination,
        "resource_manifest": [dict(item) for item in manifest.resources],
        "runtime_record_schema_version": 1,
    }
    return sha256_hex(payload)
