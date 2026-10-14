"""Shared, fail-closed observations of one invocation's cleanup evidence."""

from __future__ import annotations

import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import processes
from .records import read_record


@dataclass(frozen=True)
class ControllerStatusObservation:
    kind: str
    status: dict[str, Any] | None = None
    reason: str = ""


def observe_controller_status(lane: dict[str, Any]) -> ControllerStatusObservation:
    try:
        path = Path(lane["controller_status_path"])
        metadata = path.lstat()
    except FileNotFoundError:
        return ControllerStatusObservation("absent")
    except (OSError, KeyError, TypeError, ValueError) as exc:
        return ControllerStatusObservation("unreadable", reason=str(exc))
    if not stat.S_ISREG(metadata.st_mode):
        return ControllerStatusObservation("invalid", reason="status path is not a regular file")
    try:
        status = read_record(path, "controller-status/v1")
    except OSError as exc:
        return ControllerStatusObservation("unreadable", reason=str(exc))
    except ValueError as exc:
        return ControllerStatusObservation("invalid", reason=str(exc))
    if any(status.get(field) != lane.get(field) for field in ("lane_id", "run_id")):
        return ControllerStatusObservation("invalid", reason="status belongs to another invocation")
    return ControllerStatusObservation("valid", status)


def never_launched(lane: dict[str, Any], observation: ControllerStatusObservation) -> bool:
    proof = lane.get("retirement_proof")
    retired_preparation = (
        lane.get("lifecycle") == "retired"
        and isinstance(proof, dict)
        and proof == {"schema": "lane-retirement-proof/v1", "lane_id": lane.get("lane_id"),
                      "run_id": lane.get("run_id"), "kind": "never_launched"}
    )
    return (
        observation.kind == "absent"
        and lane.get("process") == {}
        and (
            lane.get("lifecycle") == "prepared"
            or (lane.get("lifecycle") == "running" and lane.get("launch_pending") is True)
            or retired_preparation
        )
    )


def controller_not_created(lane: dict[str, Any], observation: ControllerStatusObservation) -> bool:
    """A failed spawn has separate proof; it never restores launch_pending."""
    failure = lane.get("launch_failure")
    return (
        observation.kind == "absent"
        and lane.get("process") == {}
        and isinstance(failure, dict)
        and failure.get("run_id") == lane.get("run_id")
        and failure.get("controller_not_created") is True
    )


def retired_lane_record(lane: dict[str, Any]) -> dict[str, Any]:
    """Retain pre-launch proof when a proven-clean native owner is retired.

    Callers must establish cleanup under runtime/lane admission before applying
    this record. Retirement changes the lifecycle, so the original prepared or
    pending state cannot itself serve as later CLOSED cleanup evidence.
    """
    retired = {**lane, "lifecycle": "retired", "launch_pending": False}
    if never_launched(lane, observe_controller_status(lane)):
        retired["retirement_proof"] = {"schema": "lane-retirement-proof/v1", "lane_id": lane["lane_id"],
                                       "run_id": lane["run_id"], "kind": "never_launched"}
    return retired


def cleanup_is_proven(lane: dict[str, Any]) -> bool:
    """Return whether controller and provider boundary are proven gone."""
    facts = []
    if lane.get("provider_history_path"):
        try:
            from .retry_policy import current_invocation_facts
            facts = current_invocation_facts(lane)
        except (OSError, ValueError, KeyError, TypeError):
            return False
        for fact in facts:
            if fact["kind"] == "RESERVED":
                return False
            boundary = fact.get("process_boundary")
            if fact["kind"] in {"STARTED", "COMPLETED"} and (not isinstance(boundary, dict) or not processes.process_boundary_is_gone(boundary)):
                return False
    process = lane.get("process")
    if not isinstance(process, dict):
        return False
    if process and (type(process.get("pid")) is not int or not processes.valid_creation_identity(process.get("creation_time"))):
        return False
    if process and not processes.identity_exit_proven(process.get("pid"), process.get("creation_time")):
        return False
    observation = observe_controller_status(lane)
    status = observation.status
    if status is None:
        return (not any(fact["kind"] in {"STARTED", "COMPLETED"} for fact in facts)
                and (never_launched(lane, observation) or controller_not_created(lane, observation)))
    if status.get("cleanup_proven") is not True:
        return False
    controller_identity = status.get("controller_identity") or {}
    if not isinstance(controller_identity, dict):
        return False
    if not process and not controller_identity:
        return False
    for identity in (process, controller_identity):
        if identity and (
            type(identity.get("pid")) is not int
            or not processes.identity_exit_proven(identity.get("pid"), identity.get("creation_time"))
        ):
            return False
    boundary = status.get("process_boundary")
    provider_state = status.get("provider_state") or {}
    if not isinstance(provider_state, dict):
        return False
    if boundary is None and status.get("controller_state") == "exited" and provider_state == {"state": "not_started"}:
        return not any(fact["kind"] in {"STARTED", "COMPLETED"} for fact in facts)
    if not isinstance(boundary, dict) or not processes.process_boundary_is_gone(boundary):
        return False
    provider_pid = provider_state.get("pid")
    provider_creation = provider_state.get("creation_time")
    if provider_pid is not None and not processes.identity_exit_proven(provider_pid, provider_creation):
        return False
    return True
