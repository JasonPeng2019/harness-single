"""Shared primitives for the v2 harness: time, ids, canonical JSON, hashing."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    """Return the current UTC time."""
    return datetime.now(timezone.utc)


def iso_utc(value: datetime | None = None) -> str:
    """Return an ISO-8601 UTC timestamp string (e.g. 2026-09-02T12:00:00.123456Z)."""
    moment = value if value is not None else utc_now()
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def new_id() -> str:
    """Return one opaque, harness-generated unique id (never reused across epochs)."""
    return uuid.uuid4().hex


def canonical_json(value: Any) -> bytes:
    """Serialize a value as canonical (sorted-key, compact, UTF-8) JSON bytes."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def sha256_hex(value: Any) -> str:
    """Return the sha256 hex digest of a value's canonical JSON."""
    return hashlib.sha256(canonical_json(value)).hexdigest()


def content_hash(record: dict[str, Any]) -> str:
    """Return the record's content hash over its canonical JSON without the
    ``content_hash`` field itself (the field is the digest's storage slot)."""
    payload = {key: item for key, item in record.items() if key != "content_hash"}
    return sha256_hex(payload)


def read_json(path: Any) -> dict[str, Any]:
    """Read and parse one JSON record file."""
    import os

    from pathlib import Path

    target = Path(path)
    with target.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"record is not a JSON object: {target}")
    return value


def require_schema(record: dict[str, Any], schema: str, path: Any) -> None:
    """Validate that a record carries the expected schema string."""
    if record.get("schema") != schema:
        raise ValueError(
            f"record schema mismatch at {path}: expected {schema}, got {record.get('schema')!r}"
        )
