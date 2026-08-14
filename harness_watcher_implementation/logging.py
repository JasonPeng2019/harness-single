from __future__ import annotations
import json, os, re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SAFE = re.compile(r"[^A-Za-z0-9_.-]+")


def utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_id(value: str) -> str:
    result = SAFE.sub("_", value).strip("._")
    return result[:128] or "unknown"


def route(root: Path, category: str, source_id: str = "default") -> Path:
    if category not in {"orchestrator", "harness", "watcher", "subagents"}:
        raise ValueError("invalid source category")
    target = root / category
    if category == "subagents":
        target /= safe_id(source_id)
    target.mkdir(parents=True, exist_ok=True)
    return target / "events.jsonl"


def log(
    root: Path,
    category: str,
    event_type: str,
    data: dict[str, Any],
    source_id: str = "default",
) -> None:
    record = {
        "schema": "harness-watcher-log/v1",
        "timestamp_utc": utc(),
        "source_category": category,
        "source_id": safe_id(source_id),
        "event_type": event_type,
        "data": data,
    }
    encoded = (
        json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode()
    if len(encoded) > 65536:
        raise ValueError("log record too large")
    path = route(root, category, source_id)
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(fd, encoded)
        os.fsync(fd)
    finally:
        os.close(fd)
