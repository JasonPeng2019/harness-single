"""Diagnostic watcher compatibility boundary.

The optional watcher can contribute facts to a diagnostic snapshot, but it is
not a second manager attention state machine.  In particular, watcher alerts do
not become harness queue events and acknowledgement is not routed back through
the watcher.  Recovery is a small open/acknowledged/resolved projection only.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from .events import conditions_from_snapshot


WATCHER_RECOVERY_SCHEMA = "orchestrator-watcher-recovery/v1"
WATCHER_RECOVERY_STATES = frozenset({"open", "acknowledged", "resolved"})


def watcher_recovery_projection(records: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, Mapping):
            continue
        state = record.get("state", "open")
        alert_id = record.get("alert_id")
        if not isinstance(alert_id, str) or not alert_id.strip() or state not in WATCHER_RECOVERY_STATES:
            continue
        rows.append({
            "alert_id": alert_id,
            "state": state,
            "observed_utc": record.get("observed_utc"),
        })
    return {
        "schema": WATCHER_RECOVERY_SCHEMA,
        "states": rows,
        "actionable": [row["alert_id"] for row in rows if row["state"] == "open"],
    }


def merge_watcher_conditions(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Return native diagnostic conditions without creating watcher wake events."""

    return conditions_from_snapshot(snapshot)


def acknowledge_watcher_event(event_id: str) -> bool:
    """Compatibility no-op: watcher evidence is not a manager acknowledgement."""

    del event_id
    return False


__all__ = [
    "WATCHER_RECOVERY_SCHEMA",
    "WATCHER_RECOVERY_STATES",
    "acknowledge_watcher_event",
    "merge_watcher_conditions",
    "watcher_recovery_projection",
]
