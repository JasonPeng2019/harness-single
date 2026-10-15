"""Shared, fail-closed observations of one invocation's cleanup evidence."""

from __future__ import annotations

import stat
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import processes
from .core import content_hash, iso_utc
from . import mutation
from .records import read_record

FORCED_CLEANUP_SCHEMA = "lane-cleanup/v1"


def _bound_history_hash(raw: bytes, recorded: object, run_id: str) -> str:
    """Keep a receipt's history binding when only other runs appended rows.

    The shared lane journal grows when a later saved-session retry runs. The
    recorded journal must remain a byte-identical row prefix; any later row for
    the receipt's own run, or any rewritten byte, still breaks the binding.
    """
    digest = hashlib.sha256(raw).hexdigest()
    if not isinstance(recorded, str) or digest == recorded:
        return digest
    rows = raw.splitlines(keepends=True)
    prefix = hashlib.sha256()
    for index, row in enumerate(rows):
        prefix.update(row)
        if prefix.hexdigest() == recorded:
            later = [json.loads(line) for line in rows[index + 1:]]
            if all(isinstance(line, dict) and line.get("run_id") != run_id for line in later):
                return recorded
            break
    return digest


def _history_proof(lane: dict[str, Any], recorded: object = None) -> tuple[str | None, str | None, list[dict[str, Any]]]:
    if not lane.get("provider_history_path"):
        return None, None, []
    from .retry_policy import _path, current_invocation_facts
    path = _path(lane)
    original = mutation.capture_target(path.parent, path.name)
    raw = mutation.read_bytes(path.parent, path.name)
    if original.kind != "file" or raw != original.content:
        raise ValueError("protected cleanup history changed while reading")
    native = mutation.durable_identity(path.parent, path.name, expected=original)
    facts = current_invocation_facts(lane)
    mutation.durable_identity(path.parent, path.name, expected=original)
    return _bound_history_hash(raw, recorded, lane["run_id"]), native, facts


def _native_cleanup_proof(lane: dict[str, Any]) -> dict[str, Any]:
    observation = observe_controller_status(lane)
    if never_launched(lane, observation):
        return {"kind": "never_launched", "controller": None, "provider": None, "boundary": None}
    if controller_not_created(lane, observation):
        return {"kind": "controller_not_created", "controller": None, "provider": None, "boundary": None}
    if observation.kind != "valid" or not isinstance(lane.get("process"), dict):
        raise ValueError("forced cleanup requires an exact original controller observation")
    status = observation.status
    assert status is not None
    controller = lane["process"] or status.get("controller_identity")
    if (not isinstance(controller, dict) or not processes.identity_exit_proven(
            controller.get("pid"), controller.get("creation_time"))):
        raise ValueError("original controller exit is unproven")
    if status.get("controller_identity") and status["controller_identity"] != controller:
        raise ValueError("original controller registrations disagree")
    provider = status.get("provider_state")
    boundary = status.get("process_boundary")
    if not isinstance(provider, dict):
        raise ValueError("original provider creation is unknown")
    if isinstance(boundary, dict):
        if not processes.process_boundary_is_gone(boundary):
            raise ValueError("original provider boundary exit is unproven")
        provider_identity = boundary.get("root")
        if not isinstance(provider_identity, dict):
            raise ValueError("original provider root is missing")
    elif provider == {"state": "not_started"} and status.get("cleanup_proven") is True:
        provider_identity = None
    else:
        raise ValueError("original provider boundary is unknown")
    return {"kind": "controller_created", "controller": controller,
            "provider": provider_identity, "boundary": boundary}


def publish_forced_cleanup(rt: Path, epoch_id: str, lane: dict[str, Any]) -> dict[str, Any]:
    """Persist exact cancellation proof before releasing the original run."""
    from .epochs import lane_record_dir
    if "forced_cleanup_ref" in lane:
        if not _forced_cleanup_is_proven(lane):
            raise ValueError("retained forced cleanup is invalid; preserve it for reconciliation")
        return lane
    proof = _native_cleanup_proof(lane)
    history_hash, history_identity, facts = _history_proof(lane)
    if history_hash is None and proof["kind"] == "controller_created":
        raise ValueError("created controller cleanup requires protected invocation history")
    for fact in facts:
        if fact["kind"] == "RESERVED":
            raise ValueError("provider reservation has no native cleanup proof")
        if fact["kind"] in {"STARTED", "COMPLETED"} and (
                not isinstance(proof["boundary"], dict)
                or not isinstance(fact.get("process_boundary"), dict)
                or fact.get("process_boundary", {}).get("root") != proof["provider"]):
            raise ValueError("forced boundary differs from protected provider creation")
    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    original_folder = mutation.capture_target(folder.parent, folder.name)
    folder_identity = mutation.durable_identity(folder.parent, folder.name, expected=original_folder)
    record = {"schema": FORCED_CLEANUP_SCHEMA, "lane_id": lane["lane_id"], "run_id": lane["run_id"],
              "kind": "forced_cancellation", "controller": proof["controller"], "provider": proof["provider"],
              "boundary": proof["boundary"], "invocation_ref": history_hash,
              "observations": {"kind": proof["kind"], "directory_identity": folder_identity,
                               "history_identity": history_identity},
              "cleanup_proven": True, "recorded_at": iso_utc()}
    record["content_hash"] = content_hash(record)
    raw = (json.dumps(record, sort_keys=True, indent=2) + "\n").encode("utf-8")
    receipt = mutation.replace(folder, "CLEANUP.json", raw, expected=mutation.TargetState.absent(),
                               expected_root_identity=original_folder.identity)
    native = mutation.durable_identity(folder, "CLEANUP.json", expected=receipt.resulting,
                                       expected_root_identity=original_folder.identity)
    return {**lane, "forced_cleanup_ref": {"path": str(folder / "CLEANUP.json"),
                                           "content_hash": record["content_hash"], "native_identity": native}}


def _forced_cleanup_is_proven(lane: dict[str, Any]) -> bool:
    try:
        ref = lane["forced_cleanup_ref"]
        if not isinstance(ref, dict) or set(ref) != {"path", "content_hash", "native_identity"}:
            return False
        path = Path(ref["path"])
        if (not path.is_absolute() or path.resolve() != path or path.name != "CLEANUP.json"
                or path.is_relative_to(Path(lane["worktree_path"]).absolute())):
            return False
        folder = mutation.capture_target(path.parent.parent, path.parent.name)
        original = mutation.capture_target(path.parent, path.name)
        raw = mutation.read_bytes(path.parent, path.name, expected_root_identity=folder.identity)
        if original.kind != "file" or original.content != raw:
            return False
        if mutation.durable_identity(path.parent, path.name, expected=original,
                expected_root_identity=folder.identity) != ref["native_identity"]:
            return False
        record = json.loads(raw)
        if (not isinstance(record, dict) or set(record) != {"schema", "lane_id", "run_id", "kind",
                "controller", "provider", "boundary", "invocation_ref", "observations", "cleanup_proven",
                "recorded_at", "content_hash"} or record.get("schema") != FORCED_CLEANUP_SCHEMA
                or record.get("lane_id") != lane["lane_id"] or record.get("run_id") != lane["run_id"]
                or record.get("kind") != "forced_cancellation" or record.get("cleanup_proven") is not True
                or record.get("content_hash") != content_hash(record)
                or record["content_hash"] != ref["content_hash"]):
            return False
        observations = record["observations"]
        if mutation.durable_identity(path.parent.parent, path.parent.name,
                expected=folder) != observations["directory_identity"]:
            return False
        history_hash, history_identity, facts = _history_proof(lane, record["invocation_ref"])
        if history_hash != record["invocation_ref"] or history_identity != observations["history_identity"]:
            return False
        if any(fact["kind"] == "RESERVED" for fact in facts):
            return False
        def revalidated(result: bool) -> bool:
            if not result:
                return False
            mutation.durable_identity(path.parent, path.name, expected=original,
                                      expected_root_identity=folder.identity)
            mutation.durable_identity(path.parent.parent, path.parent.name, expected=folder)
            return _history_proof(lane, record["invocation_ref"]) == (history_hash, history_identity, facts)
        if observations["kind"] in {"never_launched", "controller_not_created"}:
            proof = _native_cleanup_proof(lane)
            return revalidated(proof["kind"] == observations["kind"] and record["controller"] is None
                    and record["provider"] is None and record["boundary"] is None
                    and not any(fact["kind"] in {"STARTED", "COMPLETED"} for fact in facts))
        if observations["kind"] != "controller_created" or history_hash is None:
            return False
        controller = record["controller"]
        current = lane.get("process")
        if not isinstance(current, dict) or (current and current != controller):
            return False
        if not current:
            status = observe_controller_status(lane)
            if status.kind != "valid" or status.status.get("controller_identity") != controller:
                return False
        if not processes.identity_exit_proven(controller.get("pid"), controller.get("creation_time")):
            return False
        boundary = record["boundary"]
        if boundary is None:
            return revalidated(record["provider"] is None and not any(fact["kind"] in {"STARTED", "COMPLETED"} for fact in facts))
        return revalidated(isinstance(boundary, dict) and boundary.get("root") == record["provider"]
                and all(fact.get("process_boundary", {}).get("root") == record["provider"]
                        for fact in facts if fact["kind"] in {"STARTED", "COMPLETED"})
                and processes.process_boundary_is_gone(boundary))
    except (OSError, ValueError, KeyError, TypeError, AttributeError, RuntimeError):
        return False


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
    if "forced_cleanup_ref" in lane:
        return _forced_cleanup_is_proven(lane)
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
