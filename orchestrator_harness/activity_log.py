"""Append-only, line-oriented activity logging for the native monitor."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from .core import iso_utc
from .records import RecordLock


LOG_NAME = "MONITOR.log"
DETAIL_LOG_NAME = "MONITOR_DETAIL.log"
IMPORTANT_LOG_NAME = "MONITOR_IMPORTANT.log"

_SENSITIVE_KEYS = {
    "authorization",
    "credential",
    "credentials",
    "password",
    "secret",
    "token",
    "api_key",
    "apikey",
}
_SENSITIVE_PATTERNS = (
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+"),
    re.compile(r"(?i)(mongodb(?:\+srv)?://[^:/@\s]+:)[^@/\s]+(@)"),
    re.compile(r"(?i)(\b(?:sk|rk|pk)-)[A-Za-z0-9_-]{12,}"),
)


def monitor_log_path(runtime_root: str | Path) -> Path:
    return Path(runtime_root) / "monitor" / LOG_NAME


def detail_log_path(runtime_root: str | Path) -> Path:
    return Path(runtime_root) / "monitor" / DETAIL_LOG_NAME


def important_log_path(runtime_root: str | Path) -> Path:
    return Path(runtime_root) / "monitor" / IMPORTANT_LOG_NAME


def is_important_record(record: dict[str, Any]) -> bool:
    """Select events useful for following a run; the detailed log keeps everything."""

    event = str(record.get("event", ""))
    if event.startswith("memory."):
        if record.get("actor") == "operator":
            return False
        return (
            ".recall." in event
            or event in {
                "memory.always_context.materialized",
                "memory.always_context.injected",
                "memory.worker.context.injected",
                "memory.preparation.started",
                "memory.preparation.completed",
                "memory.preparation.failed",
            }
        )
    if event in {"prompt.root_to_worker", "launch.worker.requested", "queue.manager.added"}:
        return True
    if event == "worker.lifecycle":
        return record.get("worker_event") in {
            "provider_started", "provider_exited", "result_valid", "result_invalid",
            "provider_exited_no_result", "acceptance_copied",
        }
    if event == "worker.native_event":
        native = record.get("native_event") or {}
        if not isinstance(native, dict) or native.get("type") != "item.completed":
            return False
        item = native.get("item") or {}
        return isinstance(item, dict) and item.get("type") in {"agent_message", "error"}
    return event.endswith(".failed") or event in {
        "process.stop_request.completed", "process.stopped",
    }


def _append_important(runtime_root: str | Path, record: dict[str, Any]) -> None:
    if is_important_record(record):
        try:
            _append_json_line(important_log_path(runtime_root), _redact(record))
        except OSError:
            # The trace remains available if the optional human view cannot be written.
            pass


def _redact(value: Any, *, key: str = "") -> Any:
    """Retain task/memory text while removing obvious authorization material."""

    lowered = key.lower().replace("-", "_")
    if lowered and (
        lowered in _SENSITIVE_KEYS
        or any(
            lowered.endswith(f"_{marker}")
            for marker in _SENSITIVE_KEYS
        )
    ):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {str(item_key): _redact(item, key=str(item_key)) for item_key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_redact(item) for item in value]
    if isinstance(value, str):
        redacted = value
        for pattern in _SENSITIVE_PATTERNS:
            if pattern.groups == 2:
                redacted = pattern.sub(r"\1[REDACTED]\2", redacted)
            else:
                redacted = pattern.sub(r"\1[REDACTED]", redacted)
        return redacted
    return value


def _append_json_line(path: Path, record: dict[str, Any]) -> Path:
    line = json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)
    with RecordLock(path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(line + "\n")
            stream.flush()
            os.fsync(stream.fileno())
    return path


def append_activity(
    runtime_root: str | Path,
    event: str,
    *,
    component: str = "monitor",
    **details: Any,
) -> Path:
    """Append one flushed JSON record without changing monitor authority."""

    path = monitor_log_path(runtime_root)
    record = {
        "schema": "harness-activity/v1",
        "timestamp": iso_utc(),
        "pid": os.getpid(),
        "component": component,
        "event": event,
        **details,
    }
    written = _append_json_line(path, record)
    try:
        _append_json_line(
            detail_log_path(runtime_root),
            {**_redact(record), "schema": "harness-detail/v1"},
        )
    except OSError:
        # The primary monitor log remains authoritative if the richer trace fails.
        pass
    _append_important(runtime_root, record)
    return written


def append_detail(
    runtime_root: str | Path,
    event: str,
    *,
    component: str = "trace",
    **details: Any,
) -> Path:
    """Append one full-fidelity trace record with credential-shaped values redacted."""

    path = detail_log_path(runtime_root)
    record = {
        "schema": "harness-detail/v1",
        "timestamp": iso_utc(),
        "pid": os.getpid(),
        "component": component,
        "event": event,
        **_redact(details),
    }
    try:
        written = _append_json_line(path, record)
        _append_important(runtime_root, record)
        return written
    except OSError:
        # Tracing must not acquire control over queue or launch behavior.
        return path


def append_current_activity(
    event: str, *, component: str = "memory", **details: Any
) -> Path | None:
    """Best-effort logging for a harness command that already owns config."""

    try:
        from .config import find_harness_root, load_config

        config = load_config(find_harness_root())
        return append_activity(
            config.runtime_root, event, component=component, **details
        )
    except Exception:
        # Observability must never become execution authority or break a run.
        return None


def append_current_detail(
    event: str, *, component: str = "trace", **details: Any
) -> Path | None:
    """Best-effort detailed logging for a harness command that owns config."""

    try:
        from .config import find_harness_root, load_config

        config = load_config(find_harness_root())
        return append_detail(
            config.runtime_root, event, component=component, **details
        )
    except Exception:
        # Detailed observability must never become execution authority.
        return None


__all__ = [
    "LOG_NAME",
    "DETAIL_LOG_NAME",
    "IMPORTANT_LOG_NAME",
    "append_activity",
    "append_current_activity",
    "append_current_detail",
    "append_detail",
    "detail_log_path",
    "important_log_path",
    "is_important_record",
    "monitor_log_path",
]
