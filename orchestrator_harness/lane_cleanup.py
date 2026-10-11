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
    return (
        observation.kind == "absent"
        and lane.get("process") == {}
        and (
            lane.get("lifecycle") == "prepared"
            or (lane.get("lifecycle") == "running" and lane.get("launch_pending") is True)
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


def cleanup_is_proven(lane: dict[str, Any]) -> bool:
    """Return whether controller and provider boundary are proven gone."""
    process = lane.get("process")
    if not isinstance(process, dict):
        return False
    if process and (type(process.get("pid")) is not int or not process.get("creation_time")):
        return False
    if processes.identity_matches(
        process.get("pid"), process.get("creation_time")
    ):
        return False
    observation = observe_controller_status(lane)
    status = observation.status
    if status is None:
        return never_launched(lane, observation) or controller_not_created(lane, observation)
    if status.get("cleanup_proven") is not True:
        return False
    controller_identity = status.get("controller_identity") or {}
    if not isinstance(controller_identity, dict):
        return False
    if not process and not controller_identity:
        return False
    for identity in (process, controller_identity):
        if identity and (
            type(identity.get("pid")) is not int or not identity.get("creation_time")
            or processes.identity_matches(identity["pid"], identity["creation_time"])
            or (processes.process_alive(identity["pid"]) and processes.process_identity(identity["pid"]) is None)
        ):
            return False
    boundary = status.get("process_boundary")
    provider_state = status.get("provider_state") or {}
    if boundary is None and status.get("controller_state") == "exited" and provider_state == {"state": "not_started"}:
        return True
    if not isinstance(boundary, dict) or not processes.process_boundary_is_gone(boundary):
        return False
    provider_pid = provider_state.get("pid")
    provider_creation = provider_state.get("creation_time")
    if isinstance(provider_pid, int):
        if processes.identity_matches(provider_pid, provider_creation):
            return False
        if processes.process_alive(provider_pid) and processes.process_identity(provider_pid) is None:
            return False
    return True
