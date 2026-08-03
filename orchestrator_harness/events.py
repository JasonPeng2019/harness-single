from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

from .models import iso_utc
from .stable_io import canonical_json


LANE_EVENT_TYPES = {
    "RUNNING_CODEX": "CONTROLLER_ACTIVE",
    "WAITING_RESOURCE": "LANE_WAITING_RESOURCE",
    "WAITING_RELAY": "LANE_WAITING_RELAY",
    "HELPER_RUNNING": "HELPER_ACTIVE",
    "STALE_STATUS": "STALE_STATUS",
    "PROCESS_STATE_UNKNOWN": "PROCESS_STATE_UNKNOWN",
    "CHECKPOINTED": "CHECKPOINT_UPDATED",
    "TERMINAL_RESULT": "RESULT_AVAILABLE",
    "EXITED": "CONTROLLER_EXITED",
    "UNKNOWN": "LANE_STATE_UNKNOWN",
}

PROCESS_EVENT_TYPES = {
    "RUNNING_CODEX": "CONTROLLER_ACTIVE",
    "WAITING_RESOURCE": "LANE_WAITING_RESOURCE",
    "STALE_STATUS": "STALE_STATUS",
    "PROCESS_STATE_UNKNOWN": "PROCESS_STATE_UNKNOWN",
    "EXITED": "CONTROLLER_EXITED",
    "UNKNOWN": "LANE_STATE_UNKNOWN",
}


def _stable_data(data: dict[str, Any]) -> dict[str, Any]:
    ignored = {"remaining_seconds", "wait_seconds", "observed_utc"}
    return {key: value for key, value in data.items() if key not in ignored}


def stable_condition(
    identity: str,
    kind: str,
    severity: str,
    data: dict[str, Any],
    *,
    event_id_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a condition with a stable ID independent of observation-only fields."""
    stable = {
        "identity": identity,
        "type": kind,
        "severity": severity,
        "data": _stable_data(event_id_data if event_id_data is not None else data),
    }
    event_id = hashlib.sha256(canonical_json(stable).encode("utf-8")).hexdigest()
    return {
        "identity": identity,
        "type": kind,
        "severity": severity,
        "data": _stable_data(data),
        "event_id": event_id,
    }


def _condition(
    identity: str, kind: str, severity: str, data: dict[str, Any]
) -> dict[str, Any]:
    return stable_condition(identity, kind, severity, data)


def conditions_from_snapshot(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    conditions: dict[str, dict[str, Any]] = {}
    for lane in snapshot.get("lanes", []):
        lane_id = lane["lane_id"]
        state = lane.get("process_state", lane["operational_state"])
        kind = PROCESS_EVENT_TYPES.get(state, "LANE_STATE_UNKNOWN")
        severity = (
            "error"
            if state == "STALE_STATUS"
            else "warning"
            if state in {"PROCESS_STATE_UNKNOWN", "UNKNOWN"}
            else "info"
        )
        data = {
            "lane_id": lane_id,
            "run_root": lane.get("run_root"),
            "doer": lane.get("doer"),
            "task": lane.get("task"),
            "state": state,
            "reason": lane.get("process_reason", lane.get("reason")),
            "controller_pid": lane.get("controller_pid"),
            "codex_pid": lane.get("codex_pid"),
            "controller_started_utc": lane.get("controller_started_utc"),
            "codex_started_utc": lane.get("codex_started_utc"),
            "invocation_schema": lane.get("invocation_schema"),
            "worker_invocation_id": lane.get("worker_invocation_id"),
            "repository": lane.get("repository"),
            "resources": lane.get("declared_resources", []),
            "held_resource_claims": lane.get("held_resource_claims", []),
            "result_validation": lane.get("result_validation"),
            "result_valid": lane.get("result_valid"),
            "coordination_failure": lane.get("coordination_failure"),
        }
        identity = f"lane:{lane_id}:process"
        conditions[identity] = _condition(identity, kind, severity, data)
        coordination_failure = lane.get("coordination_failure")
        if (
            lane.get("invocation_schema") == "orchestrator-coding-invocation/v1"
            and (
                lane.get("declared_state") == "coordination_failed"
                or isinstance(coordination_failure, dict)
            )
        ):
            identity = f"lane:{lane_id}:coordination-failure"
            conditions[identity] = _condition(
                identity,
                "COORDINATION_FAILED",
                "error",
                {
                    "lane_id": lane_id,
                    "worker_invocation_id": lane.get("worker_invocation_id"),
                    "declared_state": lane.get("declared_state"),
                    "coordination_failure": coordination_failure,
                },
            )
        wait = lane.get("waiting_resource_claim")
        if isinstance(wait, dict):
            identity = f"lane:{lane_id}:resource-wait"
            conditions[identity] = _condition(
                identity,
                "RESOURCE_WAIT",
                "warning" if wait.get("actionable") is True else "info",
                {
                    "lane_id": lane_id,
                    "worker_invocation_id": lane.get("worker_invocation_id"),
                    **wait,
                },
            )
        for finding in lane.get("resource_claim_findings", []):
            if not isinstance(finding, dict):
                continue
            resource = finding.get("resource")
            identity = f"lane:{lane_id}:resource-claim:{resource}"
            conditions[identity] = _condition(
                identity, "RESOURCE_CLAIM_STALE", "warning",
                {"lane_id": lane_id, **finding},
            )
        invalid_result = lane.get("invalid_result")
        if isinstance(invalid_result, dict):
            identity = f"lane:{lane_id}:invalid-result"
            conditions[identity] = _condition(
                identity, "CODING_RESULT_INVALID", "error",
                {
                    "lane_id": lane_id,
                    "worker_invocation_id": lane.get("worker_invocation_id"),
                    **invalid_result,
                },
            )
        if lane.get("operational_state") == "WAITING_RELAY":
            identity = f"lane:{lane_id}:relay-wait"
            conditions[identity] = _condition(
                identity,
                "LANE_WAITING_RELAY",
                "info",
                {
                    "lane_id": lane_id,
                    "state": "WAITING_RELAY",
                    "phase": lane.get("phase"),
                },
            )
        if lane.get("checkpoint_path"):
            identity = f"lane:{lane_id}:checkpoint"
            conditions[identity] = _condition(
                identity,
                "CHECKPOINT_UPDATED",
                "info",
                {
                    "lane_id": lane_id,
                    "checkpoint_path": lane.get("checkpoint_path"),
                    "checkpoint_sha256": lane.get("checkpoint_sha256"),
                },
            )
        if lane.get("result_path"):
            identity = f"lane:{lane_id}:result"
            conditions[identity] = _condition(
                identity,
                "RESULT_AVAILABLE",
                "info",
                {
                    "lane_id": lane_id,
                    "result_path": lane.get("result_path"),
                    "result_sha256": lane.get("result_sha256"),
                },
            )
        if lane.get("provider_wait"):
            identity = f"lane:{lane_id}:provider-wait"
            conditions[identity] = _condition(
                identity,
                "PROVIDER_WAIT",
                "warning",
                {
                    "lane_id": lane_id,
                    "phase": lane.get("phase"),
                    "checkpoint_path": lane.get("checkpoint_path"),
                },
            )
        if lane.get("resource_release_possible"):
            identity = f"lane:{lane_id}:resource-release-possible"
            conditions[identity] = _condition(
                identity,
                "RESOURCE_RELEASE_POSSIBLE",
                "info",
                {
                    "lane_id": lane_id,
                    "process_state": lane.get("process_state"),
                    "resources": lane.get("resources", []),
                },
            )
        if lane.get("duplicate_live_attempts", 0) > 1:
            identity = f"lane:{lane_id}:duplicate"
            conditions[identity] = _condition(
                identity,
                "DUPLICATE_CONTROLLER",
                "error",
                {
                    "lane_id": lane_id,
                    "attempts": lane.get("live_attempt_labels", []),
                },
            )
        if lane.get("resource_ambiguity"):
            identity = f"lane:{lane_id}:resource-ambiguity"
            conditions[identity] = _condition(
                identity,
                "RESOURCE_AMBIGUOUS",
                "warning",
                {
                    "lane_id": lane_id,
                    "reasons": lane["resource_ambiguity"],
                },
            )

    for request in snapshot.get("requests", []):
        path = request["path"]
        state = request["operational_state"]
        kind = state
        severity = (
            "error"
            if state in {"REQUEST_STALE", "RELAY_UNBOUND"}
            else "warning"
            if state == "REQUEST_AMBIGUOUS"
            else "info"
        )
        identity = f"request:{path}:state"
        conditions[identity] = _condition(
            identity,
            kind,
            severity,
            {
                "path": path,
                "sha256": request.get("sha256"),
                "request_id": request.get("request_id"),
                "state": state,
                "lifetime_state": request.get("lifetime_state"),
                "relay_state": request.get("relay_state"),
                "declared_lane_id": request.get("declared_lane_id"),
                "manager_actionable": request.get("manager_actionable"),
                "relay_path": request.get("relay_path"),
                "relay_reason": request.get("relay_reason"),
                "sidecar_matches": request.get("sidecar_matches"),
            },
        )
        if request.get("expiry_bucket") in {"WARNING", "CRITICAL", "EXPIRED"}:
            identity = f"request:{path}:expiry"
            conditions[identity] = _condition(
                identity,
                "REQUEST_EXPIRY_WARNING",
                "error" if request["expiry_bucket"] == "EXPIRED" else "warning",
                {
                    "path": path,
                    "sha256": request.get("sha256"),
                    "lifetime_state": request.get("lifetime_state"),
                    "relay_state": request.get("relay_state"),
                    "declared_lane_id": request.get("declared_lane_id"),
                    "manager_actionable": request.get("manager_actionable"),
                    "expiry_bucket": request["expiry_bucket"],
                    "deadline_utc": request.get("deadline_utc"),
                },
            )

    for helper in snapshot.get("helpers", []):
        path = helper["path"]
        state = helper["operational_state"]
        kind = {
            "HELPER_RUNNING": "HELPER_ACTIVE",
            "HELPER_EXITED": "HELPER_EXITED",
            "HELPER_STATE_UNKNOWN": "HELPER_STATE_UNKNOWN",
        }.get(state, "HELPER_STATE_UNKNOWN")
        identity = f"helper:{path}:state"
        conditions[identity] = _condition(
            identity,
            kind,
            "warning" if state == "HELPER_STATE_UNKNOWN" else "info",
            {
                "path": path,
                "sha256": helper.get("sha256"),
                "state": state,
                "declared_lane_id": helper.get("declared_lane_id"),
                "session_id": helper.get("session_id"),
                "processes": helper.get("processes", []),
            },
        )

    for mcp in snapshot.get("mcps", []):
        path = mcp["path"]
        state = mcp["operational_state"]
        kind = {
            "MCP_RUNNING": "MCP_ACTIVE",
            "MCP_EXITED": "MCP_EXITED",
            "MCP_STATE_UNKNOWN": "MCP_STATE_UNKNOWN",
        }.get(state, "MCP_STATE_UNKNOWN")
        identity = f"mcp:{path}:state"
        conditions[identity] = _condition(
            identity,
            kind,
            "warning" if state == "MCP_STATE_UNKNOWN" else "info",
            {
                "path": path,
                "source_path": mcp.get("source_path"),
                "sha256": mcp.get("sha256"),
                "run_id": mcp.get("run_id"),
                "server_name": mcp.get("server_name"),
                "declared_lane_id": mcp.get("declared_lane_id"),
                "session_id": mcp.get("session_id"),
                "state": state,
                "processes": mcp.get("processes", []),
            },
        )

    for conflict in snapshot.get("resource_conflicts", []):
        identity = f"resource:{conflict['resource']}:conflict"
        conditions[identity] = _condition(
            identity,
            "RESOURCE_CONFLICT",
            "error",
            conflict,
        )

    for conflict in snapshot.get("coding_conflicts", []):
        identity = f"coding:{conflict['type']}:{conflict['identity']}"
        conditions[identity] = _condition(
            identity, conflict["type"], "error", conflict,
        )

    for signal in snapshot.get("manager_signals", []):
        identity = f"manager-signal:{signal['signal_id']}"
        conditions[identity] = _condition(
            identity,
            "MANAGER_SIGNAL",
            "info",
            signal,
        )

    if not snapshot.get("process_snapshot_complete", False):
        identity = "process-provider:incomplete"
        conditions[identity] = _condition(
            identity,
            "PROCESS_INVENTORY_INCOMPLETE",
            "error",
            {
                "provider": snapshot.get("process_provider"),
                "errors": snapshot.get("process_errors", []),
            },
        )

    for index, error in enumerate(snapshot.get("observation_errors", [])):
        identity = f"observation-error:{error.get('path')}:{error.get('code')}"
        conditions[identity] = _condition(
            identity,
            "OBSERVATION_ERROR",
            "warning",
            error,
        )
    return dict(sorted(conditions.items()))


def diff_conditions(
    previous: dict[str, dict[str, Any]] | None,
    current: dict[str, dict[str, Any]],
    *,
    observed_at: datetime,
) -> list[dict[str, Any]]:
    prior = previous or {}
    events: list[dict[str, Any]] = []
    for identity, condition in current.items():
        if prior.get(identity, {}).get("event_id") == condition["event_id"]:
            continue
        events.append({**condition, "observed_utc": iso_utc(observed_at)})
    for identity, old in prior.items():
        if identity in current:
            continue
        cleared = _condition(
            identity,
            "CONDITION_CLEARED",
            "info",
            {
                "cleared_type": old.get("type"),
                "cleared_event_id": old.get("event_id"),
            },
        )
        events.append({**cleared, "observed_utc": iso_utc(observed_at)})
    return sorted(events, key=lambda event: (event["identity"], event["type"]))
