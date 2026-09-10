"""Provider-neutral ROOT PostToolUse and Stop boundary decisions.

The installed provider wrappers supply the setup-bound harness root and their
provider identity.  This module owns ROOT's monitor-liveness check and its
content-free view of unresolved manager-queue obligations; it never creates
an event or advances event state.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import processes
from .config import ConfigError, load_config
from .core import iso_utc
from .epochs import current_epoch_path, read_current_epoch
from .manager_queue import ManagerQueueError, read_manager_queue
from .setup import monitor_record_path, read_monitor_record, read_runtime_state

BOUNDARIES = frozenset({"post-tool-use", "stop"})
UNRESOLVED_STATES = frozenset({"PENDING", "ACKNOWLEDGED"})


def _reject(reason: str) -> dict[str, Any]:
    return {"decision": "REJECT", "reason": reason}


def _notice(
    provider_id: str,
    *,
    unresolved: list[dict[str, Any]] | None = None,
    monitor_code: str | None = None,
    message: str | None = None,
    epoch_id: str | None = None,
    queue_id: str | None = None,
) -> dict[str, Any]:
    events = unresolved or []
    severity_order = {"info": 0, "warning": 1, "error": 2, "blocking": 3}
    severities = [str(event.get("severity") or "warning") for event in events]
    diagnostic_class = (
        str(monitor_code or "HARNESS_DIAGNOSTIC")
        if (monitor_code or (message and not events))
        else None
    )
    if diagnostic_class:
        severities.append("error")
    highest_severity = max(
        severities,
        key=lambda item: severity_order.get(item, 1),
        default="info",
    )
    highest_classes = sorted(
        {
            str(event.get("event_class") or event.get("type"))
            for event in events
            if str(event.get("severity") or "warning") == highest_severity
            and (event.get("event_class") or event.get("type"))
        }
    )
    if diagnostic_class and severity_order.get(highest_severity, 1) <= severity_order["error"]:
        highest_classes.append(diagnostic_class)
        highest_classes = sorted(set(highest_classes))
    payload: dict[str, Any] = {
        "provider_id": provider_id,
        "binding_id": provider_id,
        "binding_identity": provider_id,
        "unresolved_count": len(events),
        "event_classes": sorted(
            {
                str(event.get("event_class") or event.get("type"))
                for event in events
                if isinstance(event.get("event_class") or event.get("type"), str)
                and (event.get("event_class") or event.get("type"))
            }
            | ({diagnostic_class} if diagnostic_class else set())
        ),
        "highest_class": highest_classes[0] if highest_classes else None,
        "highest_severity": highest_severity,
        "at": iso_utc(),
    }
    if epoch_id is not None:
        payload["epoch_id"] = epoch_id
    if queue_id is not None:
        payload["queue_id"] = queue_id
    if monitor_code:
        payload["monitor_code"] = monitor_code
    if message:
        payload["message"] = message
    return {"decision": "NOTICE", "notice": payload}


def _unresolved_events(rt: Path) -> tuple[list[dict[str, Any]] | None, str | None]:
    marker_path = current_epoch_path(rt)
    marker = read_current_epoch(rt)
    if marker is None:
        if marker_path.exists():
            return None, f"active epoch marker is invalid: {marker_path}"
        return [], None
    try:
        queue = read_manager_queue(rt)
    except (ManagerQueueError, OSError, ValueError) as exc:
        return None, f"manager queue is unreadable: {exc}"
    events = queue.get("events")
    if not isinstance(events, list) or any(not isinstance(item, dict) for item in events):
        return None, "manager queue events is not a list of objects"
    return [
        event for event in events if event.get("state") in UNRESOLVED_STATES
    ], None


def unresolved_event_ids(rt: Path) -> list[str]:
    """Return unresolved event IDs for normal ROOT delivery receipts."""
    events, error = _unresolved_events(rt)
    if error is not None or events is None:
        return []
    return [
        str(event["event_id"])
        for event in events
        if isinstance(event.get("event_id"), str) and event["event_id"]
    ]


def _heartbeat_fresh(value: object) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None:
        return False
    from .monitor import HEARTBEAT_STALENESS_SECONDS

    return (
        datetime.now(timezone.utc) - parsed
    ).total_seconds() <= HEARTBEAT_STALENESS_SECONDS


def _monitor_health(rt: Path) -> tuple[str | None, str | None]:
    """Inspect monitor liveness without starting, stopping, or replacing it."""
    record = read_monitor_record(rt)
    if record is None:
        return "MONITOR_UNHEALTHY", f"monitor record is missing: {monitor_record_path(rt)}"
    pid = record.get("pid")
    creation = record.get("creation_time")
    try:
        alive = isinstance(pid, int) and processes.identity_matches(pid, creation)
    except Exception as exc:
        return "MONITOR_UNHEALTHY", f"monitor identity could not be checked: {exc}"
    deliberately_stopped = bool(record.get("stop_requested")) or record.get("health") == "STOPPED"
    if deliberately_stopped and not alive:
        return None, None
    if not alive:
        return "MONITOR_UNHEALTHY", "recorded monitor process is not alive"
    if not _heartbeat_fresh(record.get("last_heartbeat_at")):
        return "MONITOR_UNHEALTHY", "monitor heartbeat is stale; it may be hung"
    if record.get("health") == "degraded":
        return (
            "MONITOR_DEGRADED",
            "monitor reports degraded health; ROOT must inspect and correct the recorded diagnostics",
        )
    if deliberately_stopped:
        return "MONITOR_STOP_INCOMPLETE", "monitor is marked stopped but its process is still alive"
    return None, None


def dispatch(harness_root: Path, boundary: str, provider_id: str) -> dict[str, Any]:
    """Return an ALLOW, NOTICE, or REJECT for one ROOT hook boundary."""
    if boundary not in BOUNDARIES:
        return _reject(f"unknown ROOT hook boundary: {boundary!r}")
    if not isinstance(provider_id, str) or not provider_id:
        return _reject("ROOT hook provider binding identity is missing")
    root = Path(harness_root).resolve()
    try:
        config = load_config(root)
    except (ConfigError, OSError, ValueError) as exc:
        reason = f"ROOT hook binding cannot load harness config: {exc}"
        return _reject(reason) if boundary == "stop" else _notice(
            provider_id, message=reason
        )
    if config.profile != "managed":
        return {"decision": "ALLOW"}
    runtime = read_runtime_state(config.runtime_root)
    if runtime is not None and runtime.get("state") == "CLOSED":
        return {"decision": "ALLOW"}
    if runtime is None or runtime.get("state") != "OPEN":
        reason = "ROOT hook cannot prove that the runtime is OPEN"
        return _reject(reason) if boundary == "stop" else _notice(
            provider_id, message=reason
        )

    recovery_code: str | None = None
    recovery_message: str | None = None
    if boundary == "post-tool-use":
        recovery_code, health_message = _monitor_health(config.runtime_root)
        if recovery_code is not None:
            recovery_message = health_message
            if recovery_code != "MONITOR_DEGRADED":
                recovery_message = f"{health_message}; ROOT must run `health monitor-recover`"

    unresolved, queue_error = _unresolved_events(config.runtime_root)
    if queue_error is not None:
        return _reject(queue_error) if boundary == "stop" else _notice(
            provider_id,
            monitor_code=recovery_code,
            message=queue_error,
        )
    assert unresolved is not None
    marker = read_current_epoch(config.runtime_root)
    epoch_id = str(marker.get("epoch_id")) if marker else None
    queue_id = str(marker.get("queue_id")) if marker and marker.get("queue_id") else None
    if boundary == "stop":
        if unresolved:
            return _reject(
                f"ROOT has {len(unresolved)} unresolved manager queue obligation(s)"
            )
        return {"decision": "ALLOW"}
    if unresolved or recovery_message:
        return _notice(
            provider_id,
            unresolved=unresolved,
            monitor_code=recovery_code,
            message=recovery_message,
            epoch_id=epoch_id,
            queue_id=queue_id,
        )
    return {"decision": "ALLOW"}
