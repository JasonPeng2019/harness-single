from __future__ import annotations

from datetime import datetime
from typing import Any

from .models import iso_utc, parse_utc


_TRANSITION_ONLY_TYPES = {
    "CHECKPOINT_UPDATED",
    "HELPER_EXITED",
    "HELPER_STATE_UNKNOWN",
    "MCP_EXITED",
    "MCP_STATE_UNKNOWN",
    "RESULT_AVAILABLE",
}

_DEFERRED_HANDOFF_TYPES = {
    "MANAGER_SIGNAL",
    "CHECKPOINT_UPDATED",
    "RESULT_AVAILABLE",
}
_MUTABLE_HANDOFF_TYPES = {"CHECKPOINT_UPDATED", "RESULT_AVAILABLE"}


def _active_lanes(snapshot: dict[str, Any]) -> bool:
    return any(
        lane.get("process_state", lane.get("operational_state"))
        in {"RUNNING_CODEX", "WAITING_RELAY", "PROCESS_STATE_UNKNOWN"}
        for lane in snapshot.get("lanes", [])
    )


def _request_is_live(data: dict[str, Any]) -> bool:
    return data.get("lifetime_state") == "LIVE"


def _event_manager_actionable(data: dict[str, Any]) -> bool:
    """New snapshots carry an explicit authority bit; old fixtures fail closed."""
    value = data.get("manager_actionable")
    if isinstance(value, bool):
        return value
    return _request_is_live(data) and data.get("relay_state") != "BOUND_EXPIRED"


def _manager_request_is_actionable(
    request: dict[str, Any], lane_id: str, observed_at: datetime
) -> bool:
    if request.get("declared_lane_id") != lane_id:
        return False
    if request.get("lifetime_state") == "LIVE":
        return True
    if request.get("lifetime_state") != "UNKNOWN":
        return False
    if request.get("expiry_bucket") == "EXPIRED":
        return False
    if not request.get("expiry_bucket"):
        deadline = parse_utc(request.get("deadline_utc"))
        if deadline is not None and deadline <= observed_at:
            return False
    return True


def _manager_signal_ineligibility_reason(
    data: dict[str, Any], snapshot: dict[str, Any], observed_at: datetime
) -> str | None:
    """Return the sole passive reason a retained manager signal cannot wake.

    The native selection decision remains exactly this liveness check.  The
    returned value is diagnostic-only and is emitted after observation; it does
    not alter admission, pending work, or wake delivery.
    """
    if data.get("correlated_request_answered") is True:
        return "ALREADY_ANSWERED"
    lane_id = data.get("lane_id")
    if not isinstance(lane_id, str) or not lane_id:
        return "INVALID_LANE_ID"
    lane_current = any(
        lane.get("lane_id") == lane_id
        and lane.get("process_state", lane.get("operational_state"))
        in {"RUNNING_CODEX", "WAITING_RELAY", "PROCESS_STATE_UNKNOWN"}
        for lane in snapshot.get("lanes", [])
    )
    if lane_current:
        return None
    request_current = any(
        _manager_request_is_actionable(item, lane_id, observed_at)
        for item in snapshot.get("requests", [])
    )
    if request_current:
        return None
    helper_current = any(
        item.get("declared_lane_id") == lane_id
        and item.get("operational_state")
        in {"HELPER_RUNNING", "MCP_RUNNING"}
        for group in ("helpers", "mcps")
        for item in snapshot.get(group, [])
    )
    return None if helper_current else "LANE_NOT_LIVE"


def _manager_signal_is_live(
    data: dict[str, Any], snapshot: dict[str, Any], observed_at: datetime
) -> bool:
    """Historical signals remain observable, but do not wake a fresh epoch."""
    return _manager_signal_ineligibility_reason(data, snapshot, observed_at) is None


def _resource_ambiguity_is_current(
    data: dict[str, Any], snapshot: dict[str, Any], observed_at: datetime
) -> bool:
    lane_id = data.get("lane_id")
    if not isinstance(lane_id, str) or not lane_id:
        return False
    if any(
        lane.get("lane_id") == lane_id
        and lane.get("operational_state", lane.get("process_state"))
        in {
            "RUNNING_CODEX",
            "WAITING_RELAY",
            "HELPER_RUNNING",
            "PROCESS_STATE_UNKNOWN",
            "UNKNOWN",
        }
        for lane in snapshot.get("lanes", [])
    ):
        return True
    if any(
        _manager_request_is_actionable(item, lane_id, observed_at)
        for item in snapshot.get("requests", [])
    ):
        return True
    return any(
        item.get("declared_lane_id") == lane_id
        and item.get("operational_state") in {"HELPER_RUNNING", "MCP_RUNNING"}
        for group in ("helpers", "mcps")
        for item in snapshot.get(group, [])
    )


def _manager_signal_has_delivery_urgency(data: dict[str, Any]) -> bool:
    return (
        data.get("kind") == "HELP"
        and data.get("agent_blocked") is True
        and parse_utc(data.get("delivery_deadline_utc")) is not None
    )


def _notification_deadline(data: dict[str, Any]) -> datetime | None:
    """Use a HELP delivery deadline for manager ordering when one was declared."""
    return parse_utc(data.get("delivery_deadline_utc")) or parse_utc(data.get("deadline_utc"))


def _priority(
    event: dict[str, Any], snapshot: dict[str, Any], observed_at: datetime
) -> float | None:
    kind = event.get("type")
    data = event.get("data", {})
    if kind == "HARNESS_WATCHER_ALERT":
        return 0
    if kind in {"RELAY_READY", "REQUEST_AMBIGUOUS", "RELAY_UNBOUND"}:
        return 1 if _event_manager_actionable(data) else None
    if kind == "REQUEST_EXPIRY_WARNING":
        return 1 if _event_manager_actionable(data) and data.get("expiry_bucket") in {"WARNING", "CRITICAL"} else None
    if kind in {
        "DUPLICATE_CONTROLLER",
        "RESOURCE_CONFLICT",
        "LANE_STATE_UNKNOWN",
    }:
        return 2
    if kind == "RESOURCE_AMBIGUOUS":
        return 2 if _resource_ambiguity_is_current(data, snapshot, observed_at) else None
    if kind == "STALE_STATUS":
        return 2
    if kind == "PROCESS_STATE_UNKNOWN":
        return 2 if _active_lanes(snapshot) else None
    if kind == "PROCESS_INVENTORY_INCOMPLETE":
        return 2
    if kind == "OBSERVATION_ERROR":
        return 2 if _active_lanes(snapshot) or any(item.get("lifetime_state") == "LIVE" for item in snapshot.get("requests", [])) else None
    if kind == "MANAGER_SIGNAL":
        if not _manager_signal_is_live(data, snapshot, observed_at):
            return None
        return 1.5 if _manager_signal_has_delivery_urgency(data) else 3
    if kind == "CONTROLLER_EXITED":
        lane = next(
            (item for item in snapshot.get("lanes", []) if item.get("lane_id") == data.get("lane_id")),
            {},
        )
        if lane.get("checkpoint_path") or lane.get("result_path"):
            return None
        declared = str(lane.get("declared_state", "")).lower()
        if declared in {"controller_failed", "launch_failed", "error", "failed", "terminated", "killed", "cancelled"}:
            return 4
        ended = parse_utc(lane.get("ended_utc"))
        return 4 if ended is not None and (observed_at - ended).total_seconds() <= 2 else None
    if kind in {"HELPER_EXITED", "MCP_EXITED", "HELPER_STATE_UNKNOWN", "MCP_STATE_UNKNOWN", "PROVIDER_WAIT"}:
        if kind == "PROVIDER_WAIT":
            return 4
        lane_id = data.get("declared_lane_id")
        session_id = data.get("session_id")
        if isinstance(lane_id, str) and lane_id:
            return 4 if any(
                lane.get("lane_id") == lane_id
                and lane.get("process_state", lane.get("operational_state"))
                in {"RUNNING_CODEX", "WAITING_RELAY", "PROCESS_STATE_UNKNOWN"}
                for lane in snapshot.get("lanes", [])
            ) else None
        if isinstance(session_id, str) and session_id:
            return 4 if any(
                lane.get("thread_id") == session_id
                and lane.get("process_state", lane.get("operational_state"))
                in {"RUNNING_CODEX", "WAITING_RELAY", "PROCESS_STATE_UNKNOWN"}
                for lane in snapshot.get("lanes", [])
            ) else None
        return 4 if _active_lanes(snapshot) else None
    if kind == "LANE_STAGE_REPEAT":
        return 4.1
    if kind == "LANE_NO_PROGRESS":
        return 4.2
    if kind == "MANAGER_REVIEW_DUE":
        return 4.3
    if kind in {"CHECKPOINT_UPDATED", "RESULT_AVAILABLE"}:
        return 5
    return None


def select_actionable(
    conditions: dict[str, dict[str, Any]],
    snapshot: dict[str, Any],
    *,
    observed_at: datetime,
    acknowledged_event_ids: set[str],
    newly_observed_event_ids: set[str] | None = None,
) -> dict[str, Any] | None:
    """Select one durable manager notification from current conditions."""
    candidates: list[tuple[float, datetime, str, dict[str, Any]]] = []
    for condition in conditions.values():
        if condition["event_id"] in acknowledged_event_ids:
            continue
        if (
            newly_observed_event_ids is not None
            and condition.get("type") in _TRANSITION_ONLY_TYPES
            and condition["event_id"] not in newly_observed_event_ids
        ):
            continue
        priority = _priority(condition, snapshot, observed_at)
        if priority is None:
            continue
        deadline = _notification_deadline(condition.get("data", {}))
        candidates.append((priority, deadline or datetime.max.replace(tzinfo=observed_at.tzinfo), condition["identity"], condition))
    if not candidates:
        return None
    priority, _, _, condition = min(candidates, key=lambda item: (item[0], item[1], item[2]))
    return {
        **condition,
        "admitted_priority": priority,
        "observed_utc": iso_utc(observed_at),
        "notification": "MANAGER_ACTION_REQUIRED",
    }


def _order_key(condition: dict[str, Any], observed_at: datetime) -> tuple[str, str]:
    """The stable deadline/identity portion of notification selection ordering."""
    deadline = _notification_deadline(condition.get("data", {}))
    return (iso_utc(deadline) or "9999-12-31T23:59:59Z", str(condition["identity"]))


def admit_deferred_handoffs(
    conditions: dict[str, dict[str, Any]],
    snapshot: dict[str, Any],
    *,
    observed_at: datetime,
    acknowledged_event_ids: set[str],
    newly_observed_event_ids: set[str] | None,
    existing: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Capture only newly eligible file-backed handoffs at their admission rank.

    Entries deliberately retain the original condition and ranking facts.  A later
    selection must not re-evaluate liveness for a deferred handoff.
    """
    by_event = {
        item.get("event", {}).get("event_id")
        for item in existing
        if isinstance(item, dict) and isinstance(item.get("event"), dict)
    }
    admitted = list(existing)
    for condition in conditions.values():
        if condition.get("type") not in _DEFERRED_HANDOFF_TYPES:
            continue
        event_id = condition.get("event_id")
        if not isinstance(event_id, str) or event_id in acknowledged_event_ids or event_id in by_event:
            continue
        if (
            newly_observed_event_ids is not None
            and condition.get("type") in _TRANSITION_ONLY_TYPES
            and event_id not in newly_observed_event_ids
        ):
            continue
        priority = _priority(condition, snapshot, observed_at)
        if priority is None:
            continue
        deadline_key, identity_key = _order_key(condition, observed_at)
        admitted.append({
            "event": condition,
            "priority": priority,
            "deadline_order": deadline_key,
            "identity": identity_key,
            "admitted_utc": iso_utc(observed_at),
        })
        by_event.add(event_id)
    return sorted(admitted, key=lambda item: (item["priority"], item["deadline_order"], item["identity"]))


def _prune_deferred(
    deferred: list[dict[str, Any]], conditions: dict[str, dict[str, Any]], acknowledged: set[str]
) -> list[dict[str, Any]]:
    retained: list[dict[str, Any]] = []
    for item in deferred:
        event = item.get("event") if isinstance(item, dict) else None
        if (
            not isinstance(event, dict)
            or (
                event.get("type") not in _DEFERRED_HANDOFF_TYPES
                and item.get("preempted_pending") is not True
            )
            or event.get("event_id") in acknowledged
        ):
            continue
        # Only the exact signal identity may prove this stored signal superseded.
        current = conditions.get(event.get("identity"))
        if (
            event.get("type") == "MANAGER_SIGNAL"
            and isinstance(current, dict)
            and current.get("data", {}).get("correlated_request_answered") is True
        ):
            continue
        retained.append(item)
    return retained


def select_actionable_with_deferred(
    conditions: dict[str, dict[str, Any]],
    snapshot: dict[str, Any],
    *,
    observed_at: datetime,
    acknowledged_event_ids: set[str],
    newly_observed_event_ids: set[str] | None,
    deferred: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Select current work or an admitted handoff without re-gating stored work."""
    retained = _prune_deferred(deferred, conditions, acknowledged_event_ids)
    current = select_actionable(
        conditions, snapshot, observed_at=observed_at,
        acknowledged_event_ids=acknowledged_event_ids,
        newly_observed_event_ids=newly_observed_event_ids,
    )
    choices: list[tuple[float, str, str, dict[str, Any], bool]] = []
    if current is not None:
        priority = _priority(current, snapshot, observed_at)
        assert priority is not None
        deadline, identity = _order_key(current, observed_at)
        choices.append((priority, deadline, identity, current, False))
    for item in retained:
        event = item["event"]
        choices.append((item["priority"], item["deadline_order"], item["identity"], event, True))
    if not choices:
        return None, retained
    _, _, _, selected, stored = min(choices, key=lambda item: item[:3])
    if stored:
        retained = [item for item in retained if item.get("event", {}).get("event_id") != selected.get("event_id")]
        source = next(item for item in deferred if item.get("event", {}).get("event_id") == selected.get("event_id"))
        selected = {**selected, "admitted_priority": source["priority"], "admitted_utc": source["admitted_utc"], "observed_utc": iso_utc(observed_at), "notification": "MANAGER_ACTION_REQUIRED"}
    elif selected.get("type") in _MUTABLE_HANDOFF_TYPES:
        source = next((item for item in retained if item.get("event", {}).get("event_id") == selected.get("event_id")), None)
        if source is not None:
            retained = [item for item in retained if item is not source]
            selected = {**selected, "admitted_priority": source["priority"], "admitted_utc": source["admitted_utc"]}
    return selected, retained


def preempt_pending_with_higher_priority(
    pending: dict[str, Any] | None,
    deferred: list[dict[str, Any]],
    snapshot: dict[str, Any],
    *,
    observed_at: datetime,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], bool]:
    """Select strictly higher-priority deferred work without losing pending work."""
    if not isinstance(pending, dict) or not deferred:
        return pending, deferred, False
    stored_priority = pending.get("admitted_priority")
    pending_priority = (
        float(stored_priority)
        if isinstance(stored_priority, (int, float)) and not isinstance(stored_priority, bool)
        else _priority(pending, snapshot, observed_at)
    )
    if pending_priority is None:
        return pending, deferred, False
    ranked = sorted(
        deferred,
        key=lambda item: (item["priority"], item["deadline_order"], item["identity"]),
    )
    selected_item = ranked[0]
    if selected_item["priority"] >= pending_priority:
        return pending, deferred, False

    selected_event = selected_item["event"]
    selected_id = selected_event.get("event_id")
    pending_deadline, pending_identity = _order_key(pending, observed_at)
    admitted_utc = pending.get("admitted_utc") or pending.get("observed_utc") or iso_utc(observed_at)
    displaced = {
        "event": pending,
        "priority": pending_priority,
        "deadline_order": pending_deadline,
        "identity": pending_identity,
        "admitted_utc": admitted_utc,
        "preempted_pending": True,
    }
    remaining = [
        item for item in ranked
        if item.get("event", {}).get("event_id") not in {selected_id, pending.get("event_id")}
    ]
    remaining.append(displaced)
    remaining.sort(
        key=lambda item: (item["priority"], item["deadline_order"], item["identity"])
    )
    selected = {
        **selected_event,
        "admitted_priority": selected_item["priority"],
        "admitted_utc": selected_item["admitted_utc"],
        "observed_utc": iso_utc(observed_at),
        "notification": "MANAGER_ACTION_REQUIRED",
    }
    return selected, remaining, True


def coalesce_mutable_handoffs(
    pending: dict[str, Any] | None,
    deferred: list[dict[str, Any]],
    *,
    observed_at: datetime,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Keep exactly the newest mutable file version per (type, identity)."""
    entries: list[tuple[dict[str, Any], dict[str, Any] | None, str]] = []
    if isinstance(pending, dict) and pending.get("type") in _MUTABLE_HANDOFF_TYPES:
        # Missing admission is legacy state and deterministically ranks before explicit facts.
        entries.append((pending, None, str(pending.get("admitted_utc") or "")))
    for item in deferred:
        event = item.get("event") if isinstance(item, dict) else None
        if isinstance(event, dict) and event.get("type") in _MUTABLE_HANDOFF_TYPES:
            entries.append((event, item, str(item.get("admitted_utc") or "")))
    groups: dict[tuple[str, str], list[tuple[dict[str, Any], dict[str, Any] | None, str]]] = {}
    for entry in entries:
        event = entry[0]
        groups.setdefault((str(event.get("type")), str(event.get("identity"))), []).append(entry)
    kept_deferred = list(deferred)
    result_pending = pending
    for _, group in groups.items():
        if len(group) < 2:
            continue
        winner, winner_item, _ = max(group, key=lambda item: (item[2], str(item[0].get("event_id") or "")))
        superseded = sorted(
            str(event.get("event_id")) for event, _, _ in group if event.get("event_id") != winner.get("event_id")
        )
        pending_member = next((event for event, item, _ in group if item is None), None)
        group_ids = {event.get("event_id") for event, _, _ in group}
        kept_deferred = [
            item for item in kept_deferred
            if item.get("event", {}).get("event_id") not in group_ids
        ]
        if pending_member is not None:
            data = dict(winner.get("data", {}))
            prior = data.get("superseded_event_ids", [])
            data["superseded_event_ids"] = sorted(set(superseded + [str(item) for item in prior]))
            admission = winner_item.get("admitted_utc") if winner_item is not None else winner.get("admitted_utc")
            result_pending = {**winner, "data": data, "admitted_utc": admission, "observed_utc": iso_utc(observed_at), "notification": "MANAGER_ACTION_REQUIRED"}
            if winner_item is not None:
                result_pending["admitted_priority"] = winner_item["priority"]
        elif winner_item is not None:
            kept_deferred.append(winner_item)
    return result_pending, sorted(kept_deferred, key=lambda item: (item.get("priority", 99), item.get("deadline_order", ""), item.get("identity", "")))

