"""Provider-neutral ROOT PostToolUse and Stop boundary decisions.

The installed provider wrappers supply the setup-bound harness root and their
provider identity.  This module owns ROOT's monitor-liveness check and its
content-free view of unresolved manager-queue obligations; it never creates
an event or advances event state.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .config import ConfigError, load_config
from .core import iso_utc
from .epochs import current_epoch_path, read_current_epoch
from .manager_queue import ManagerQueueError, read_manager_queue
from .setup import (
    MONITOR_DELIBERATELY_STOPPED,
    MONITOR_HEALTHY,
    MONITOR_RECOVERED,
    MONITOR_RECOVERY_DISABLED,
    MONITOR_RUNTIME_NOT_OPEN,
    read_runtime_state,
    run_monitor_recover,
)

BOUNDARIES = frozenset({"post-tool-use", "stop"})
UNRESOLVED_STATES = frozenset({"PENDING", "ACKNOWLEDGED"})
HEALTHY_RECOVERY_CODES = frozenset({MONITOR_HEALTHY, MONITOR_RECOVERED})
INACTIVE_RECOVERY_CODES = frozenset(
    {
        MONITOR_DELIBERATELY_STOPPED,
        MONITOR_RECOVERY_DISABLED,
        MONITOR_RUNTIME_NOT_OPEN,
    }
)


def _reject(reason: str) -> dict[str, Any]:
    return {"decision": "REJECT", "reason": reason}


def _notice(
    provider_id: str,
    *,
    unresolved: list[dict[str, Any]] | None = None,
    monitor_code: str | None = None,
    message: str | None = None,
) -> dict[str, Any]:
    events = unresolved or []
    payload: dict[str, Any] = {
        "provider_id": provider_id,
        "unresolved_count": len(events),
        "event_classes": sorted(
            {
                str(event.get("type"))
                for event in events
                if isinstance(event.get("type"), str) and event.get("type")
            }
        ),
        "at": iso_utc(),
    }
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
        recovery = run_monitor_recover(root)
        recovery_code = str(recovery.get("code") or "MONITOR_RECOVERY_UNKNOWN")
        if recovery_code not in HEALTHY_RECOVERY_CODES | INACTIVE_RECOVERY_CODES:
            recovery_message = (
                str(recovery.get("next_action") or recovery.get("summary") or "")
                or "run `health monitor-recover` and inspect MONITOR.json"
            )

    unresolved, queue_error = _unresolved_events(config.runtime_root)
    if queue_error is not None:
        return _reject(queue_error) if boundary == "stop" else _notice(
            provider_id,
            monitor_code=recovery_code,
            message=queue_error,
        )
    assert unresolved is not None
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
        )
    return {"decision": "ALLOW"}
