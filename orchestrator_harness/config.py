from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ConfigError(ValueError):
    pass


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
    manager_review_interval_seconds: float
    lane_no_progress_seconds: float
    manager_heartbeat_timeout_seconds: float
    attention_logging_enabled: bool
    attention_epoch_id: str
    attention_sprint_lifetime_seconds: float | None = None

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
    if result < minimum:
        raise ConfigError(f"{key} must be >= {minimum}")
    return result


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

    globs = raw.get("run_globs", ["fresh-experiments/*"])
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

    output_value = raw.get("output_dir", ".state")
    if not isinstance(output_value, str) or not output_value:
        raise ConfigError("output_dir must be a non-empty path string")
    output = Path(output_value)
    if not output.is_absolute():
        output = root / output

    warning = int(_number(raw, "request_warning_seconds", 120, minimum=1))
    critical = int(_number(raw, "request_critical_seconds", 30, minimum=0))
    if critical >= warning:
        raise ConfigError("request_critical_seconds must be less than warning")

    process_tolerance = _number(raw, "process_start_tolerance_seconds", 2, minimum=0)
    if process_tolerance > 2:
        raise ConfigError("process_start_tolerance_seconds must be <= 2")

    manager_review_interval = _number(
        raw, "manager_review_interval_seconds", 300, minimum=0
    )
    lane_no_progress = _number(raw, "lane_no_progress_seconds", 600, minimum=0)
    manager_heartbeat_timeout = _number(
        raw, "manager_heartbeat_timeout_seconds", 420, minimum=0
    )
    if manager_review_interval <= 0:
        raise ConfigError("manager_review_interval_seconds must be positive")
    if lane_no_progress <= 0:
        raise ConfigError("lane_no_progress_seconds must be positive")
    if manager_heartbeat_timeout <= 0:
        raise ConfigError("manager_heartbeat_timeout_seconds must be positive")
    attention_enabled = raw.get("attention_logging_enabled", False)
    if not isinstance(attention_enabled, bool):
        raise ConfigError("attention_logging_enabled must be boolean")
    attention_epoch = raw.get("attention_epoch_id", f"harness-{config_path.stem}")
    if not isinstance(attention_epoch, str) or not attention_epoch:
        raise ConfigError("attention_epoch_id must be a non-empty string")

    if manager_heartbeat_timeout <= manager_review_interval:
        raise ConfigError(
            "manager_heartbeat_timeout_seconds must be greater than "
            "manager_review_interval_seconds"
        )
    sprint_lifetime = raw.get("attention_sprint_lifetime_seconds")
    if sprint_lifetime is not None:
        sprint_lifetime = _number(raw, "attention_sprint_lifetime_seconds", 0, minimum=0)
        if sprint_lifetime <= 0:
            raise ConfigError("attention_sprint_lifetime_seconds must be positive when provided")
        # Loading a declared attention-sprint config is its launcher boundary:
        # fail before any managed watcher can be started.
        from .attention_sprint import AttentionSprintError, validate_sprint_boundary
        try:
            validate_sprint_boundary(epoch_id=attention_epoch, heartbeat_timeout_seconds=manager_heartbeat_timeout, formal_review_interval_seconds=manager_review_interval, bounded_lifetime_seconds=sprint_lifetime)
        except AttentionSprintError as exc:
            raise ConfigError(str(exc)) from exc

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
        process_start_tolerance_seconds=int(process_tolerance),
        max_json_bytes=int(_number(raw, "max_json_bytes", 4_000_000, minimum=1024)),
        max_jsonl_tail_bytes=int(
            _number(raw, "max_jsonl_tail_bytes", 512_000, minimum=1024)
        ),
        stable_read_retries=int(_number(raw, "stable_read_retries", 4, minimum=1)),
        stable_read_delay_seconds=_number(
            raw, "stable_read_delay_seconds", 0.03, minimum=0
        ),
        manager_review_interval_seconds=manager_review_interval,
        lane_no_progress_seconds=lane_no_progress,
        manager_heartbeat_timeout_seconds=manager_heartbeat_timeout,
        attention_logging_enabled=attention_enabled,
        attention_epoch_id=attention_epoch,
        attention_sprint_lifetime_seconds=sprint_lifetime,
    )
