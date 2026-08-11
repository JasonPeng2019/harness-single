"""Bounded historical decoder for the removed attention-sprint policy.

There is intentionally no validator, selector, timeline writer, integrity
chain, heartbeat, or finalization policy in this module.  The decoder is useful
only when an operator explicitly inspects old evidence and cannot construct
current SafeOutput state or influence S3 actionability.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


HISTORICAL_ATTENTION_SCHEMA = "orchestrator-historical-attention-record/v1"
_MAX_RECORD_KEYS = 32


def decode_historical_attention_record(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return a bounded inert projection of one legacy record."""

    if not isinstance(value, Mapping) or len(value) > _MAX_RECORD_KEYS:
        raise ValueError("historical attention record is oversized or malformed")
    event_id = value.get("event_id")
    kind = value.get("kind")
    if not isinstance(event_id, str) or not event_id.strip() or not isinstance(kind, str) or not kind.strip():
        raise ValueError("historical attention record has no bounded identity")
    return {
        "schema": HISTORICAL_ATTENTION_SCHEMA,
        "event_id": event_id,
        "kind": kind,
        "observed_utc": value.get("source_timestamp_utc") or value.get("observed_utc"),
        "historical_only": True,
        "actionable": False,
        "acknowledgeable": False,
    }


__all__ = ["HISTORICAL_ATTENTION_SCHEMA", "decode_historical_attention_record"]
