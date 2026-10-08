"""Best-effort product-memory events for the harness monitor log."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_LOG_ENV = "MEMORY_HARNESS_MONITOR_LOG_PATH"
_DETAIL_LOG_ENV = "MEMORY_HARNESS_DETAIL_LOG_PATH"
_IMPORTANT_LOG_ENV = "MEMORY_HARNESS_IMPORTANT_LOG_PATH"
_OPERATOR_LOG_ENV = "MEMORY_HARNESS_OPERATOR_LOG_PATH"
_configured_path: Path | None = None
_configured_detail_path: Path | None = None
_configured_important_path: Path | None = None

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


def configure_activity_log(
    path: str | Path, *, detail_path: str | Path | None = None,
    important_path: str | Path | None = None,
) -> Path:
    global _configured_path, _configured_detail_path, _configured_important_path
    _configured_path = Path(path).resolve()
    _configured_detail_path = (
        Path(detail_path).resolve()
        if detail_path is not None
        else _configured_path.with_name("MONITOR_DETAIL.log")
    )
    _configured_important_path = (
        Path(important_path).resolve()
        if important_path is not None
        else _configured_path.with_name("MONITOR_IMPORTANT.log")
    )
    os.environ[_LOG_ENV] = str(_configured_path)
    os.environ[_DETAIL_LOG_ENV] = str(_configured_detail_path)
    os.environ[_IMPORTANT_LOG_ENV] = str(_configured_important_path)
    return _configured_path


def activity_log_path() -> Path | None:
    raw = os.environ.get(_LOG_ENV)
    return Path(raw).resolve() if raw else _configured_path


def detail_log_path() -> Path | None:
    raw = os.environ.get(_DETAIL_LOG_ENV)
    return Path(raw).resolve() if raw else _configured_detail_path


def important_log_path() -> Path | None:
    raw = os.environ.get(_IMPORTANT_LOG_ENV)
    if raw:
        return Path(raw).resolve()
    if _configured_important_path is not None:
        return _configured_important_path
    primary = activity_log_path()
    return primary.with_name("MONITOR_IMPORTANT.log") if primary is not None else None


def operator_log_path() -> Path | None:
    raw = os.environ.get(_OPERATOR_LOG_ENV)
    if raw:
        return Path(raw).resolve()
    primary = activity_log_path()
    return primary.with_name("MONITOR_OPERATOR_MEMORY.log") if primary is not None else None


def _write_important(record: dict[str, Any]) -> None:
    if record.get("actor") == "operator":
        return
    event = str(record.get("event", ""))
    if ".recall." in event or event in {
        "memory.preparation.started", "memory.preparation.completed", "memory.preparation.failed",
    }:
        _write(important_log_path(), _redact(record))


def _write_operator(record: dict[str, Any]) -> None:
    if record.get("actor") == "operator" and str(record.get("event", "")).startswith("memory."):
        _write(operator_log_path(), _redact(record))


def _redact(value: Any, *, key: str = "") -> Any:
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


def _write(path: Path | None, record: dict[str, Any]) -> None:
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)
        descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        try:
            os.write(descriptor, (line + "\n").encode("utf-8"))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        pass


def write_activity(event: str, *, component: str = "memory", **details: Any) -> None:
    """Write one immediately visible JSON line; logging never breaks memory."""

    path = activity_log_path()
    if path is None:
        return
    record = {
        "schema": "harness-activity/v1",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(),
        "component": component,
        "event": event,
        **({"actor": os.environ["MEMORY_HARNESS_ACTOR"]} if os.environ.get("MEMORY_HARNESS_ACTOR") else {}),
        **details,
    }
    _write(path, record)
    detail = detail_log_path()
    if detail is not None:
        _write(detail, {**_redact(record), "schema": "harness-detail/v1"})
    _write_important(record)
    _write_operator(record)


def write_detail(event: str, *, component: str = "memory", **details: Any) -> None:
    """Write expanded memory/query/payload evidence only to the detailed trace."""

    record = {
        "schema": "harness-detail/v1",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(),
        "component": component,
        "event": event,
        **({"actor": os.environ["MEMORY_HARNESS_ACTOR"]} if os.environ.get("MEMORY_HARNESS_ACTOR") else {}),
        **_redact(details),
    }
    _write(detail_log_path(), record)
    _write_important(record)
    _write_operator(record)


__all__ = [
    "activity_log_path",
    "configure_activity_log",
    "detail_log_path",
    "important_log_path",
    "write_activity",
    "write_detail",
]
