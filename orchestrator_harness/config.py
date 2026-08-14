from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ConfigError(ValueError):
    pass


def path_identity(path: str | Path) -> str:
    """Return a platform-native, normalized identity for a filesystem path."""

    resolved = Path(path).resolve(strict=False)
    return os.path.normcase(os.path.normpath(str(resolved)))


def same_path(left: str | Path, right: str | Path) -> bool:
    return path_identity(left) == path_identity(right)


@dataclass(frozen=True)
class HarnessConfig:
    config_path: Path
    harness_root: Path
    suite_root: Path
    run_globs: tuple[str, ...]
    workspace_relpath: str
    output_dir: Path
    poll_interval_seconds: float
    watch_timeout_seconds: float
    request_warning_seconds: int
    request_critical_seconds: int
    process_start_tolerance_seconds: int
    max_json_bytes: int
    max_jsonl_tail_bytes: int
    stable_read_retries: int
    stable_read_delay_seconds: float
    record_paths: tuple[str, ...] = ()
    record_manifests: tuple[str, ...] = ()
    legacy_config_diagnostics: tuple[str, ...] = ()

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


def load_config(
    path: str | Path,
    *,
    harness_root: Path | None = None,
) -> HarnessConfig:
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
        config_path=config_path,
        harness_root=root,
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
