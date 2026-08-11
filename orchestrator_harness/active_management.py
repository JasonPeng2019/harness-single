"""Pure active-lane timer and status projections.

This module only derives advisory conditions from a reconciled snapshot.  It neither
observes files/processes nor persists history, which keeps its decisions repeatable for
the native manager/coordinator boundary that consumes it.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from .config import HarnessConfig
from .events import stable_condition
from .models import iso_utc, parse_utc
from .stable_io import canonical_json


HISTORY_SCHEMA = "orchestrator-active-management-history/v1"
_LIVE_CONTROLLER_STATES = {"RUNNING_CODEX", "WAITING_RESOURCE", "WAITING_RELAY", "HELPER_RUNNING"}
_UNKNOWN_CONTROLLER_STATES = {"PROCESS_STATE_UNKNOWN", "UNKNOWN"}
_LIVE_HELPER_STATES = {"HELPER_RUNNING"}
_UNKNOWN_HELPER_STATES = {"HELPER_STATE_UNKNOWN"}
_LIVE_MCP_STATES = {"MCP_RUNNING"}
_UNKNOWN_MCP_STATES = {"MCP_STATE_UNKNOWN"}


def empty_history() -> dict[str, Any]:
    """Return the compact, JSON-serializable active-management history schema."""
    return {
        "schema": HISTORY_SCHEMA,
        "review_baseline_utc": None,
        "lanes": {},
        "inactive_lanes": {},
    }


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _seconds_since(now: datetime, then: str | None) -> float | None:
    parsed = parse_utc(then)
    if parsed is None:
        return None
    return max(0.0, round((_as_utc(now) - parsed).total_seconds(), 3))


def _is_lane_match(item: Mapping[str, Any], lane: Mapping[str, Any]) -> bool:
    lane_id = lane.get("lane_id")
    if not isinstance(lane_id, str) or not lane_id:
        return False
    for key in ("declared_lane_id", "lane_id"):
        explicit = item.get(key)
        if isinstance(explicit, str) and explicit:
            return explicit == lane_id
    session = item.get("session_id")
    if not isinstance(session, str):
        identities = item.get("producer_identities")
        if isinstance(identities, Mapping):
            session = identities.get("session_id")
    return bool(session and session == lane.get("thread_id"))


def _classification(
    state: object,
    *,
    live: set[str],
    unknown: set[str],
) -> str:
    if state in live:
        return "live"
    if state in unknown:
        return "unknown"
    return "exited"


def _request_sort_key(request: Mapping[str, Any]) -> tuple[datetime, str, str]:
    created = parse_utc(request.get("created_utc"))
    return (
        created or datetime.min.replace(tzinfo=timezone.utc),
        str(request.get("request_id") or ""),
        str(request.get("path") or ""),
    )


def _request_entry(request: Mapping[str, Any] | None, now: datetime) -> dict[str, Any] | None:
    if request is None:
        return None
    request_id = request.get("request_id")
    path = request.get("path")
    identity = request_id if isinstance(request_id, str) and request_id else path
    return {
        "identity": identity,
        "kind": request.get("request_kind"),
        "deadline_utc": request.get("deadline_utc"),
        "relay_state": request.get("relay_state"),
        "lifetime_state": request.get("lifetime_state"),
        "age_seconds": _seconds_since(now, request.get("created_utc")),
        "created_utc": request.get("created_utc"),
        # This is only used to distinguish a later publication when no request ID exists.
        "instance": request_id
        if isinstance(request_id, str) and request_id
        else canonical_json(
            {
                "path": path,
                "kind": request.get("request_kind"),
                "created_utc": request.get("created_utc"),
            }
        ),
    }


def _latest_activity(
    lane: Mapping[str, Any], request: Mapping[str, Any] | None, snapshot: Mapping[str, Any], now: datetime
) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    for kind, path_key, hash_key in (
        ("checkpoint", "checkpoint_path", "checkpoint_sha256"),
        ("result", "result_path", "result_sha256"),
    ):
        if lane.get(path_key):
            candidates.append(
                {
                    "kind": kind,
                    "identity": {"path": lane.get(path_key), "sha256": lane.get(hash_key)},
                    "observed_utc": lane.get(f"{kind}_observed_utc"),
                    "age_seconds": _seconds_since(
                        now, lane.get(f"{kind}_observed_utc")
                    ),
                }
            )
    if request is not None:
        candidates.append(
            {
                "kind": "request",
                "identity": request.get("request_id") or request.get("path"),
                "observed_utc": request.get("created_utc"),
                "age_seconds": _seconds_since(now, request.get("created_utc")),
            }
        )
        if request.get("relay_path"):
            candidates.append(
                {
                    "kind": "relay",
                    "identity": {
                        "path": request.get("relay_path"),
                        "sha256": request.get("relay_sha256"),
                    },
                    "observed_utc": request.get("relay_observed_utc"),
                    "age_seconds": _seconds_since(
                        now, request.get("relay_observed_utc")
                    ),
                }
            )
    for signal in snapshot.get("manager_signals", []):
        if not isinstance(signal, Mapping) or not _is_lane_match(signal, lane):
            continue
        candidates.append(
            {
                "kind": "manager_signal",
                "identity": {"signal_id": signal.get("signal_id"), "sha256": signal.get("sha256")},
                "observed_utc": signal.get("created_utc"),
                "age_seconds": _seconds_since(now, signal.get("created_utc")),
            }
        )
    dated = [item for item in candidates if parse_utc(item.get("observed_utc"))]
    if dated:
        return max(dated, key=lambda item: parse_utc(item["observed_utc"]))
    return candidates[0] if candidates else None


def _progress_material(lane: Mapping[str, Any], projection: Mapping[str, Any]) -> dict[str, Any]:
    request = projection.get("request") or {}
    latest = projection.get("latest_activity") or {}
    signals = sorted(
        {
            (str(signal.get("signal_id") or ""), str(signal.get("sha256") or ""))
            for signal in projection.get("manager_signals", [])
        }
    )
    return {
        "task": lane.get("task"),
        "phase": lane.get("phase"),
        "controller": projection.get("controller_state"),
        "helpers": projection.get("helper_states"),
        "mcps": projection.get("mcp_states"),
        "request": {
            "identity": request.get("identity"),
            "kind": request.get("kind"),
            "relay_state": request.get("relay_state"),
        },
        "checkpoint": lane.get("checkpoint_sha256"),
        "result": lane.get("result_sha256"),
        "signals": signals,
        "terminal": lane.get("operational_state") == "TERMINAL_RESULT",
        "provider_wait": bool(lane.get("provider_wait")),
        # Kept separate so request repetition can ignore request publication itself.
        "activity_kind": latest.get("kind"),
    }


def progress_fingerprint(lane: Mapping[str, Any], projection: Mapping[str, Any]) -> str:
    """Hash only meaningful reconciled facts; never timestamps, PIDs, or run roots."""
    return hashlib.sha256(
        canonical_json(_progress_material(lane, projection)).encode("utf-8")
    ).hexdigest()


def _stage_context(lane: Mapping[str, Any], projection: Mapping[str, Any]) -> str:
    """Facts whose advance breaks the no-progress stage-repetition sequence."""
    return hashlib.sha256(
        canonical_json(
            {
                "task": lane.get("task"),
                "phase": lane.get("phase"),
                "checkpoint": lane.get("checkpoint_sha256"),
                "result": lane.get("result_sha256"),
                "terminal": lane.get("operational_state") == "TERMINAL_RESULT",
            }
        ).encode("utf-8")
    ).hexdigest()


def project_active_lanes(snapshot: Mapping[str, Any], *, observed_at: datetime) -> list[dict[str, Any]]:
    """Project each currently active reconciled lane exactly once."""
    now = _as_utc(observed_at)
    projections: list[dict[str, Any]] = []
    helpers = [item for item in snapshot.get("helpers", []) if isinstance(item, Mapping)]
    mcps = [item for item in snapshot.get("mcps", []) if isinstance(item, Mapping)]
    requests = [item for item in snapshot.get("requests", []) if isinstance(item, Mapping)]
    signals = [item for item in snapshot.get("manager_signals", []) if isinstance(item, Mapping)]
    conflicts = [item for item in snapshot.get("resource_conflicts", []) if isinstance(item, Mapping)]

    for lane in snapshot.get("lanes", []):
        if not isinstance(lane, Mapping) or not isinstance(lane.get("lane_id"), str):
            continue
        related_helpers = [item for item in helpers if _is_lane_match(item, lane)]
        related_mcps = [item for item in mcps if _is_lane_match(item, lane)]
        controller_state = _classification(
            lane.get("process_state", lane.get("operational_state")),
            live=_LIVE_CONTROLLER_STATES,
            unknown=_UNKNOWN_CONTROLLER_STATES,
        )
        helper_states = sorted(
            _classification(item.get("operational_state"), live=_LIVE_HELPER_STATES, unknown=_UNKNOWN_HELPER_STATES)
            for item in related_helpers
        )
        mcp_states = sorted(
            _classification(item.get("operational_state"), live=_LIVE_MCP_STATES, unknown=_UNKNOWN_MCP_STATES)
            for item in related_mcps
        )
        active = controller_state in {"live", "unknown"} or any(
            state in {"live", "unknown"} for state in helper_states + mcp_states
        )
        if not active:
            continue
        related_requests = [item for item in requests if _is_lane_match(item, lane)]
        request = max(related_requests, key=_request_sort_key) if related_requests else None
        related_signals = [item for item in signals if _is_lane_match(item, lane)]
        resource_conflicts = [
            item for item in conflicts if lane["lane_id"] in item.get("owners", [])
        ]
        projection: dict[str, Any] = {
            "lane_id": lane["lane_id"],
            "doer": lane.get("doer"),
            "task": lane.get("task"),
            "phase": lane.get("phase"),
            "controller_operational_state": lane.get(
                "process_state", lane.get("operational_state")
            ),
            "controller_state": controller_state,
            "helper_states": helper_states,
            "mcp_states": mcp_states,
            "elapsed_seconds": _seconds_since(now, lane.get("started_utc")),
            "request": _request_entry(request, now),
            "resources": list(lane.get("resources", [])),
            "declared_resources": {
                "boards": list(lane.get("board_tokens", [])),
                "mcp_servers": list(lane.get("mcp_servers", [])),
            },
            "resource_ambiguity": list(lane.get("resource_ambiguity", [])),
            "resource_conflicts": resource_conflicts,
            "manager_signals": related_signals,
        }
        projection["latest_activity"] = _latest_activity(lane, request, snapshot, now)
        projection["fingerprint"] = progress_fingerprint(lane, projection)
        projections.append(projection)
    return sorted(projections, key=lambda item: item["lane_id"])


def _normalized_history(history: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(history, Mapping):
        return empty_history()
    lanes = history.get("lanes")
    return {
        "schema": HISTORY_SCHEMA,
        "review_baseline_utc": history.get("review_baseline_utc"),
        "lanes": dict(lanes) if isinstance(lanes, Mapping) else {},
        "inactive_lanes": (
            dict(history.get("inactive_lanes"))
            if isinstance(history.get("inactive_lanes"), Mapping)
            else {}
        ),
    }


def _review_condition(lanes: list[dict[str, Any]], baseline: str, now: datetime) -> dict[str, Any]:
    due = parse_utc(baseline)
    review_lanes = [_review_lane_entry(lane) for lane in lanes]
    return stable_condition(
        "manager:review-due",
        "MANAGER_REVIEW_DUE",
        "info",
        {
            "review_baseline_utc": baseline,
            "due_utc": iso_utc(due) if due else None,
            "active_lane_count": len(review_lanes),
            "lanes": review_lanes,
            "observed_utc": iso_utc(now),
        },
        event_id_data={"review_baseline_utc": baseline},
    )


def _review_lane_entry(lane: Mapping[str, Any]) -> dict[str, Any]:
    request = lane.get("request")
    return {
        "lane_id": lane.get("lane_id"),
        "doer": lane.get("doer"),
        "task": lane.get("task"),
        "phase": lane.get("phase"),
        "controller": {
            "operational_state": lane.get("controller_operational_state"),
            "classification": lane.get("controller_state"),
        },
        "elapsed_seconds": lane.get("elapsed_seconds"),
        "latest_activity": lane.get("latest_activity"),
        "request": (
            {
                "kind": request.get("kind"),
                "deadline_utc": request.get("deadline_utc"),
            }
            if isinstance(request, Mapping)
            else None
        ),
        "declared_resources": lane.get("declared_resources"),
        "resource_conflicts": lane.get("resource_conflicts"),
        "resource_ambiguity": lane.get("resource_ambiguity"),
        "fingerprint_age_seconds": lane.get("fingerprint_age_seconds"),
    }


def _no_progress_condition(
    lane: Mapping[str, Any], lane_history: Mapping[str, Any]
) -> dict[str, Any]:
    fingerprint = lane["fingerprint"]
    generation = int(lane_history.get("fingerprint_generation", 1))
    since = lane_history.get("fingerprint_since_utc")
    return stable_condition(
        f"manager:lane:{lane['lane_id']}:no-progress",
        "LANE_NO_PROGRESS",
        "warning",
        {
            "lane": dict(lane),
            "fingerprint": fingerprint,
            "unchanged_since_utc": since,
        },
        event_id_data={
            "lane_id": lane["lane_id"],
            "fingerprint": fingerprint,
            "generation": generation,
            "unchanged_since_utc": since,
        },
    )


def _repeat_condition(
    lane: Mapping[str, Any],
    lane_history: Mapping[str, Any],
    request: Mapping[str, Any],
    prior: Mapping[str, Any],
) -> dict[str, Any]:
    request_kind = str(request.get("kind") or "unknown")
    return stable_condition(
        f"manager:lane:{lane['lane_id']}:stage-repeat:{request_kind}",
        "LANE_STAGE_REPEAT",
        "warning",
        {
            "lane": dict(lane),
            "request_kind": request_kind,
            "prior_request": prior,
            "current_request": dict(request),
        },
        event_id_data={
            "lane_id": lane["lane_id"],
            "stage_context": lane_history.get("stage_context"),
            "request_kind": request_kind,
            "prior_instance": prior.get("instance"),
            "current_instance": request.get("instance"),
        },
    )


def _request_is_stage_complete(request: Mapping[str, Any]) -> bool:
    return request.get("relay_state") in {"BOUND", "BOUND_EXPIRED"} or request.get("lifetime_state") == "ABSENT"


def transition_active_management(
    snapshot: Mapping[str, Any],
    history: Mapping[str, Any] | None,
    config: HarnessConfig,
    *,
    observed_at: datetime,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Advance timer history and return advisory manager conditions.

    Callers persist the returned history atomically with their own watcher state.  This
    transition intentionally has no I/O and does not acknowledge notifications.
    """
    now = _as_utc(observed_at)
    updated = _normalized_history(history)
    active_lanes = project_active_lanes(snapshot, observed_at=now)
    conditions: list[dict[str, Any]] = []
    if active_lanes and parse_utc(updated.get("review_baseline_utc")) is None:
        updated["review_baseline_utc"] = iso_utc(now)

    current_lanes: dict[str, dict[str, Any]] = {}
    inactive_lanes = dict(updated["inactive_lanes"])
    for lane in active_lanes:
        lane_id = lane["lane_id"]
        previous = updated["lanes"].get(lane_id)
        if previous is None:
            previous = inactive_lanes.pop(lane_id, None)
        else:
            inactive_lanes.pop(lane_id, None)
        entry = dict(previous) if isinstance(previous, Mapping) else {}
        fingerprint = lane["fingerprint"]
        if entry.get("fingerprint") != fingerprint:
            entry["fingerprint"] = fingerprint
            entry["fingerprint_since_utc"] = iso_utc(now)
            entry["fingerprint_generation"] = int(entry.get("fingerprint_generation", 0)) + 1
        since = _seconds_since(now, entry.get("fingerprint_since_utc"))
        lane["fingerprint_since_utc"] = entry.get("fingerprint_since_utc")
        lane["fingerprint_age_seconds"] = since
        acknowledged_ids = {
            item
            for item in entry.get("acknowledged_management_event_ids", [])
            if isinstance(item, str)
        }
        if since is not None and since >= config.lane_no_progress_seconds:
            condition = _no_progress_condition(lane, entry)
            if condition["event_id"] not in acknowledged_ids:
                conditions.append(condition)

        stage_context = _stage_context(
            next(
                item for item in snapshot.get("lanes", [])
                if isinstance(item, Mapping) and item.get("lane_id") == lane_id
            ),
            lane,
        )
        if entry.get("stage_context") != stage_context:
            entry["stage_context"] = stage_context
            entry["stage_requests_by_kind"] = {}
            entry["stage_repeat_occurrences"] = {}
            entry["acknowledged_management_event_ids"] = []
            acknowledged_ids = set()
        request = lane.get("request")
        if isinstance(request, Mapping) and request.get("kind") and request.get("instance"):
            kind = str(request["kind"])
            prior_by_kind = entry.get("stage_requests_by_kind")
            prior_by_kind = dict(prior_by_kind) if isinstance(prior_by_kind, Mapping) else {}
            occurrences = entry.get("stage_repeat_occurrences")
            occurrences = dict(occurrences) if isinstance(occurrences, Mapping) else {}
            prior = prior_by_kind.get(kind)
            if isinstance(prior, Mapping) and prior.get("instance") != request.get("instance"):
                if prior.get("complete") and kind not in occurrences:
                    occurrences[kind] = {
                        "prior": dict(prior),
                        "current": dict(request),
                    }
                prior_by_kind[kind] = {
                    "instance": request.get("instance"),
                    "identity": request.get("identity"),
                    "kind": kind,
                    "complete": _request_is_stage_complete(request),
                }
            elif not isinstance(prior, Mapping):
                prior_by_kind[kind] = {
                    "instance": request.get("instance"),
                    "identity": request.get("identity"),
                    "kind": kind,
                    "complete": _request_is_stage_complete(request),
                }
            elif _request_is_stage_complete(request):
                prior_by_kind[kind] = {**prior, "complete": True}
            entry["stage_requests_by_kind"] = prior_by_kind
            entry["stage_repeat_occurrences"] = occurrences
        for occurrence in entry.get("stage_repeat_occurrences", {}).values():
            if not isinstance(occurrence, Mapping):
                continue
            prior = occurrence.get("prior")
            repeated = occurrence.get("current")
            if not isinstance(prior, Mapping) or not isinstance(repeated, Mapping):
                continue
            condition = _repeat_condition(lane, entry, repeated, prior)
            if condition["event_id"] not in acknowledged_ids:
                conditions.append(condition)
        current_lanes[lane_id] = entry
    for lane_id, entry in updated["lanes"].items():
        if lane_id not in current_lanes and isinstance(entry, Mapping):
            inactive_lanes[lane_id] = dict(entry)
    updated["lanes"] = current_lanes
    updated["inactive_lanes"] = inactive_lanes

    baseline = parse_utc(updated.get("review_baseline_utc"))
    if active_lanes and baseline is not None:
        if now >= baseline + timedelta(seconds=config.manager_review_interval_seconds):
            conditions.append(_review_condition(active_lanes, iso_utc(baseline) or "", now))
    return updated, sorted(conditions, key=lambda item: (item["identity"], item["type"]))


def acknowledge_management_event(
    history: Mapping[str, Any] | None,
    event: Mapping[str, Any],
    *,
    acknowledged_at: datetime,
) -> dict[str, Any]:
    """Record the exact acknowledgement without consuming unrelated warnings."""
    updated = _normalized_history(history)
    if event.get("type") == "MANAGER_REVIEW_DUE":
        updated["review_baseline_utc"] = iso_utc(_as_utc(acknowledged_at))
        return updated
    if event.get("type") not in {"LANE_NO_PROGRESS", "LANE_STAGE_REPEAT"}:
        return updated
    data = event.get("data")
    lane = data.get("lane") if isinstance(data, Mapping) else None
    lane_id = lane.get("lane_id") if isinstance(lane, Mapping) else None
    if not isinstance(lane_id, str) or not lane_id:
        return updated
    for bucket in (updated["lanes"], updated["inactive_lanes"]):
        entry = bucket.get(lane_id)
        if not isinstance(entry, Mapping):
            continue
        revised = dict(entry)
        acknowledged = [
            item
            for item in revised.get("acknowledged_management_event_ids", [])
            if isinstance(item, str)
        ]
        event_id = event.get("event_id")
        if isinstance(event_id, str) and event_id not in acknowledged:
            revised["acknowledged_management_event_ids"] = (acknowledged + [event_id])[-100:]
            bucket[lane_id] = revised
        break
    return updated
