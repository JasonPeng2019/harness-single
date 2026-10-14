from __future__ import annotations
import json, re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from .io import output, lock
from . import settings

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
    path = target / "events.jsonl"
    output(root, path)
    return path


def log(
    root: Path,
    category: str,
    event_type: str,
    data: dict[str, Any],
    source_id: str = "default",
    *,
    event_id: str | None = None,
) -> None:
    if not settings.harness_watcher_active:
        return
    record = {
        "schema": "harness-watcher-log/v1",
        "timestamp_utc": utc(),
        "source_category": category,
        "source_id": safe_id(source_id),
        "event_type": event_type,
        "data": data,
    }
    if event_id is not None:
        record["event_id"] = event_id
    encoded = (
        json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode()
    if len(encoded) > 65536:
        raise ValueError("log record too large")
    path = route(root, category, source_id)
    # A replay after output fsync but before source-cursor publication must not
    # manufacture another routed event. The stable identity binds raw source
    # bytes and offsets; timestamps are diagnostic rather than dedup authority.
    with lock(root, path):
        transaction = output(root, path)
        if event_id is not None:
            try:
                existing = transaction.read_bytes(path)
            except FileNotFoundError:
                existing = b""
            for line in existing.splitlines():
                try:
                    prior = json.loads(line)
                except (ValueError, UnicodeDecodeError):
                    continue
                if isinstance(prior, dict) and prior.get("event_id") == event_id:
                    fields = ("source_category", "source_id", "event_type", "data")
                    if any(prior.get(field) != record[field] for field in fields):
                        raise ValueError("routed watcher event identity has conflicting content")
                    return
        # The path-keyed kernel lock is already held; use the anchored append
        # directly rather than recursively acquiring that same lock.
        from orchestrator_harness.mutation import append_bytes
        append_bytes(transaction.root, path.relative_to(transaction.root), encoded)
