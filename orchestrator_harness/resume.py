"""``resume-lane``: re-run a stopped, unaccepted lane in its same worktree and
provider session with a fresh ``run_id``.

Resume is a short program, not an agent.  It re-does work: it clears the prior
run's obsolete current state, writes a fresh invocation for the new run, and
leaves the lane ready for ``lane launch``.  It never reconstructs a session,
PID, worktree, or amendment/hash record.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from . import processes
from .setup import COMPOSED_PAYLOADS
from .bootstrap import (
    _worker_output,
    _write_invocation,
    _write_result_template,
    _write_worker_binding,
    _write_worker_prompt,
)
from .config import find_harness_root, load_config
from .core import content_hash, iso_utc, new_id, read_json, require_schema
from .epochs import lane_record_dir, read_current_epoch
from .setup import read_runtime_state
from .lanes import find_active_lane, lane_operation_lock, read_lane, update_lane
from .lane_cleanup import controller_not_created, never_launched, observe_controller_status
from .records import atomic_write_json, remove_record
from .records import runtime_admission_lock
from .manager_queue import acknowledge_event, close_event, read_manager_queue
from .task_cards import validate_task_card
from .retry_policy import MAX_LANE_PROVIDER_INVOCATIONS, WORKER_RETRY_LIMIT, completed_provider_invocations

INVOCATION_SCHEMA = "controller-invocation/v1"
OVERLAY_RECEIPT_SCHEMA = "overlay-receipt/v1"
LANE_INBOX_SCHEMA = "lane-inbox/v1"
COMPLETION_REVIEW_SCHEMA = "completion-review/v1"
ACCEPTANCE_SCHEMA = "orchestrator-acceptance/v1"

ALREADY_ACCEPTED = "ALREADY_ACCEPTED"
LANE_RUNNING = "LANE_RUNNING"
RESUME_WORKTREE_MISSING = "RESUME_WORKTREE_MISSING"
NO_SAVED_SESSION_ID = "NO_SAVED_SESSION_ID"
INVALID_RESUME_TASK_CARD = "INVALID_RESUME_TASK_CARD"
RESUME_LANE_WRITE_FAILED = "RESUME_LANE_WRITE_FAILED"

_RESUMABLE_LIFECYCLES = frozenset(
    {"review_pending", "result_invalid", "blocked", "abandoned", "resuming"}
)


def _live_controller(lane: dict[str, Any]) -> bool:
    """Check both the lane process and the controller's exact attestation."""
    status_value = lane.get("controller_status_path")
    if not isinstance(status_value, str) or not status_value:
        raise ValueError("controller status path is missing; resume cannot prove prior ownership")
    observation = observe_controller_status(lane)
    if observation.kind in ("invalid", "unreadable"):
        raise ValueError("controller status is invalid or unreadable; resume cannot prove prior ownership")
    identities = [lane.get("process")]
    status = observation.status
    if status is not None:
        identities.append(status.get("controller_identity", {}))
    for identity in identities:
        if not isinstance(identity, dict) or (identity and (
            type(identity.get("pid")) is not int or not processes.valid_creation_identity(identity.get("creation_time"))
        )):
            raise ValueError("controller identity is unknown; resume cannot replace its run")
        if identity:
            pid, creation = identity["pid"], identity["creation_time"]
            if processes.identity_matches(pid, creation):
                return True
            if not processes.identity_exit_proven(pid, creation):
                raise ValueError("controller identity is unobservable; resume cannot replace its run")
    if (
        status is None and lane.get("lifecycle") == "running"
        and not never_launched(lane, observation) and not controller_not_created(lane, observation)
    ):
        raise ValueError("started invocation cleanup is unknown; resume cannot discard its evidence")
    return False


def _consume_resume_signal(rt: Path, lane_id: str, prior_run_id: str) -> None:
    """Close the rejected-review signal after the fresh run is prepared."""
    try:
        queue = read_manager_queue(rt)
    except Exception:
        return
    for event in queue.get("events", []):
        if not (
            event.get("type") == "LANE_RESUME_REQUIRED"
            and event.get("lane_id") == lane_id
            and event.get("run_id") == prior_run_id
            and event.get("state") in {"PENDING", "ACKNOWLEDGED"}
        ):
            continue
        event_id = str(event["event_id"])
        if event.get("state") == "PENDING":
            acknowledge_event(rt, event_id)
        close_event(
            rt,
            event_id,
            "COMPLETE",
            summary=f"lane {lane_id} resumed under a fresh run",
        )


def _read_task_card(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"resume task card missing: {path}")
    record = read_json(path)
    validate_task_card(record, path)
    return record


def _has_valid_acceptance_chain(
    rt: Path, epoch_id: str, lane: dict[str, Any]
) -> bool:
    """Return whether a complete, linked ACCEPTED chain exists for this run."""
    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    review_path = folder / "COMPLETION_REVIEW.json"
    acceptance_path = folder / "ORCHESTRATOR_ACCEPTANCE.json"
    import stat
    present = []
    for path in (review_path, acceptance_path):
        try:
            info = path.lstat()
        except FileNotFoundError:
            present.append(False)
            continue
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or getattr(info, "st_file_attributes", 0) & 0x400):
            raise ValueError("ROOT decision is not a plain record; preserve it for explicit reconciliation")
        present.append(True)
    if not any(present):
        return False
    try:
        review = read_json(review_path)
        acceptance = read_json(acceptance_path)
        require_schema(review, COMPLETION_REVIEW_SCHEMA, review_path)
        require_schema(acceptance, ACCEPTANCE_SCHEMA, acceptance_path)
    except (OSError, ValueError) as exc:
        raise ValueError("ROOT decision is unreadable or incomplete; preserve it for explicit reconciliation") from exc
    from .review import validate_acceptance_chain
    if not validate_acceptance_chain(review, acceptance, lane_id=lane["lane_id"], run_id=lane.get("run_id")):
        raise ValueError("ROOT decision is invalid or stale; preserve it for explicit reconciliation")
    if (acceptance["source_assignment_hash"] != lane.get("result_assignment_hash")
            or acceptance["task_card_hash"] != lane.get("task_card_hash")):
        raise ValueError("ROOT decision differs from the protected assignment; reconcile before resume")
    if acceptance["approval"] == "REJECTED":
        return False
    from .launch import LaunchError, _validate_current_acceptance
    try:
        _validate_current_acceptance(rt, epoch_id, lane, acceptance)
    except LaunchError as exc:
        raise ValueError(f"accepted source is no longer valid; reconcile before resume: {exc}") from exc
    return True


def _clear_prior_run(rt: Path, epoch_id: str, lane: dict[str, Any], *, output=None) -> None:
    """Remove the prior run's obsolete current state (best-effort, honest)."""
    worktree = Path(lane["worktree_path"])
    output = _worker_output(worktree, output)
    output.delete(worktree / "RESULT.json")
    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    remove_record(folder / "COMPLETION_REVIEW.json")
    remove_record(folder / "ORCHESTRATOR_ACCEPTANCE.json")
    output.delete(worktree / ".agent-workspace" / "controller.status.json")


def _reset_worker_inbox(worktree: Path, lane_id: str, run_id: str, *, output=None) -> None:
    """Reset the managed worker inbox to a valid empty queue for the new run."""
    inbox = {
        "schema": LANE_INBOX_SCHEMA,
        "lane_id": lane_id,
        "run_id": run_id,
        "assignments": [],
    }
    output = _worker_output(worktree, output)
    output.publish_json(output.admit(worktree / ".agent-workspace" / "QUEUE.json"), inbox)


def _rewrite_overlay_receipt(
    worktree: Path, lane: dict[str, Any], run_id: str, *, managed: bool, output=None
) -> None:
    receipt = {
        "schema": OVERLAY_RECEIPT_SCHEMA,
        "lane_id": lane["lane_id"],
        "run_id": run_id,
        "profile": "managed" if managed else "plain",
        "base_cache_ref": "super-cache/workspace",
        "applied_at": iso_utc(),
    }
    if managed:
        receipt["provider_payload"] = f"{COMPOSED_PAYLOADS.as_posix()}/{lane['provider']['id']}"
    output = _worker_output(worktree, output)
    output.publish_json(output.admit(worktree / ".agent-workspace" / "overlay-receipt.json"), receipt)


def run_resume(
    *,
    lane_id: str,
    resume_task_card: str,
    rationale: str | None = None,
) -> dict[str, Any]:
    """Execute ``resume-lane`` and return the structured result."""
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": RESUME_LANE_WRITE_FAILED,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "fix the configuration and re-run setup",
        }
    rt = config.runtime_root
    try:
        epoch_id, lane = find_active_lane(rt, lane_id)
    except Exception as exc:
        return {
            "ok": False,
            "code": RESUME_LANE_WRITE_FAILED,
            "summary": f"lane not found: {lane_id}",
            "evidence_paths": [],
            "next_action": "check the lane id or bootstrap a fresh lane",
        }

    try:
        if _has_valid_acceptance_chain(rt, epoch_id, lane):
            return {
                "ok": False,
                "code": ALREADY_ACCEPTED,
                "summary": f"lane {lane_id} already has a valid ACCEPTED chain; it is not re-resumed",
                "evidence_paths": [
                    str(lane_record_dir(rt, epoch_id, lane_id) / "ORCHESTRATOR_ACCEPTANCE.json")
                ],
                "next_action": "retire the lane with `lane retire --acceptance-ref <file>`",
            }
        lifecycle = lane.get("lifecycle")
        if lifecycle == "accepted":
            return {
                "ok": False,
                "code": RESUME_LANE_WRITE_FAILED,
                "summary": f"lane {lane_id} is marked accepted without a validated current decision; reconcile it before resume",
                "evidence_paths": [],
                "next_action": "preserve evidence and reconcile the ROOT decision",
            }
        if lifecycle == "retired":
            return {
                "ok": False,
                "code": RESUME_LANE_WRITE_FAILED,
                "summary": f"lane {lane_id} is retired; resume is not a cleanup tool",
                "evidence_paths": [],
                "next_action": "bootstrap a fresh lane",
            }
        if _live_controller(lane):
            return {
                "ok": False,
                "code": LANE_RUNNING,
                "summary": f"lane {lane_id} is still active; it is not resumed",
                "evidence_paths": [],
                "next_action": "wait for the lane to stop, or force-stop it first",
            }
        try:
            prior_invocations = completed_provider_invocations(lane, rt)
        except (OSError, ValueError, TypeError) as exc:
            return {
                "ok": False, "code": WORKER_RETRY_LIMIT, "summary": str(exc),
                "evidence_paths": [str(lane.get("provider_history_path") or "")],
                "next_action": "bootstrap a fresh lane and native session; preserve prior invocation evidence",
            }
        if prior_invocations >= MAX_LANE_PROVIDER_INVOCATIONS:
            return {
                "ok": False, "code": WORKER_RETRY_LIMIT,
                "summary": f"lane {lane_id} used its initial assignment plus one retry",
                "evidence_paths": [str(lane.get("provider_history_path") or "")],
                "next_action": "settle the previous lane and bootstrap a fresh lane and native session",
            }
        if lifecycle not in _RESUMABLE_LIFECYCLES and lifecycle != "running":
            return {
                "ok": False,
                "code": RESUME_LANE_WRITE_FAILED,
                "summary": f"lane {lane_id} is not resumable (lifecycle={lifecycle})",
                "evidence_paths": [],
                "next_action": "bootstrap a fresh lane",
            }

        worktree = Path(lane["worktree_path"])
        if not worktree.is_dir():
            return {
                "ok": False,
                "code": RESUME_WORKTREE_MISSING,
                "summary": f"lane worktree is missing: {worktree}",
                "evidence_paths": [],
                "next_action": "bootstrap a fresh lane (resume cannot rebuild a worktree)",
            }
        session = lane.get("session") or {}
        session_id = session.get("session_id")
        if not isinstance(session_id, str) or not session_id:
            return {
                "ok": False,
                "code": NO_SAVED_SESSION_ID,
                "summary": f"lane {lane_id} has no saved provider session to resume",
                "evidence_paths": [],
                "next_action": "bootstrap a fresh lane (resume requires a native session)",
            }

        try:
            task_card = _read_task_card(Path(resume_task_card))
        except (OSError, ValueError) as exc:
            return {
                "ok": False,
                "code": INVALID_RESUME_TASK_CARD,
                "summary": str(exc),
                "evidence_paths": [],
                "next_action": "supply a valid project-task-card/v1 resume card",
            }

        prior_run_id = str(lane.get("run_id") or "")
        run_id = new_id()
        managed = config.profile == "managed"
        def mark_resuming(current: dict[str, Any]) -> dict[str, Any]:
            if any(current.get(field) != lane.get(field) for field in ("lane_id", "run_id", "worktree_path", "provider")) or current.get("lifecycle") != lifecycle:
                raise ValueError("lane run or status changed before resume mutation")
            if _live_controller(current):
                raise ValueError("an exact controller is still live; resume cannot replace its run")
            if completed_provider_invocations(current, rt) >= MAX_LANE_PROVIDER_INVOCATIONS:
                raise ValueError("initial provider invocation plus one explicit retry exhausted")
            marker = read_current_epoch(rt)
            runtime = read_runtime_state(rt)
            if not marker or marker.get("epoch_id") != epoch_id or not runtime or runtime.get("state") != "OPEN":
                raise ValueError("runtime or epoch changed before resume")
            return {
                **current,
                "lifecycle": "resuming",
                "resume_started_at": iso_utc(),
                "resume_from_run_id": prior_run_id,
            }

        with runtime_admission_lock(rt), lane_operation_lock(rt, epoch_id, lane_id):
            from .lane_authority import read_invocation, stage_authority
            from .lanes import require_same_run
            require_same_run(read_lane(rt, epoch_id, lane_id), lane)
            if _has_valid_acceptance_chain(rt, epoch_id, lane):
                raise ValueError("ROOT accepted this run before resume; retire it instead")
            prior_invocation = read_invocation(lane)
            output = _worker_output(worktree)
            if output.root_identity != tuple(lane["worktree_identity"]):
                raise ValueError("assigned worktree changed before resume mutation")
            output.revalidate(worktree / ".agent-workspace")
            if managed:
                from .public_checks import home, policy, validate_service
                if (policy(harness_root) is not None
                        or (home(config.root_workspace) / "config.json").exists()
                        or (home(config.root_workspace) / "service.json").exists()):
                    validate_service(config.root_workspace, harness_root)
            update_lane(rt, epoch_id, lane_id, mark_resuming)
            current = read_lane(rt, epoch_id, lane_id)
            if any(current.get(field) != lane.get(field) for field in ("lane_id", "run_id", "worktree_path", "provider")) or current.get("lifecycle") != "resuming":
                raise ValueError("lane run or status changed before prior run cleanup")
            _clear_prior_run(rt, epoch_id, lane, output=output)
            if managed:
                _reset_worker_inbox(worktree, lane_id, run_id, output=output)
                from .public_checks import install_route
                install_route(harness_root, config.root_workspace, worktree, lane_id, run_id, output=output)
            _write_worker_prompt(
                worktree, task_card, managed=managed, rationale=rationale, output=output
            )
            _write_result_template(worktree, lane_id, run_id, output=output)
            exclusive_resources = list(prior_invocation["exclusive_resources"])
            invocation = _write_invocation(
                worktree,
                lane_id=lane_id,
                run_id=run_id,
                provider_id=lane["provider"]["id"],
                model=lane["provider"]["model"],
                launch_config=lane["provider"]["launch_config"],
                exclusive_resources=exclusive_resources,
                output=output,
            )
            invocation_path = worktree / ".agent-workspace" / "invocation.json"
            import json
            written_invocation = json.loads(output.read_bytes(invocation_path))
            require_schema(written_invocation, INVOCATION_SCHEMA, invocation_path)
            if (
                written_invocation.get("lane_id") != lane_id
                or written_invocation.get("run_id") != run_id
                or written_invocation.get("provider", {}).get("id") != lane["provider"]["id"]
                or written_invocation.get("content_hash") != content_hash(written_invocation)
            ):
                raise ValueError("fresh resume invocation failed identity or hash validation")
            _rewrite_overlay_receipt(worktree, lane, run_id, managed=managed, output=output)
            if managed:
                _write_worker_binding(worktree, rt, lane_id, run_id, output=output)
            # The resume task card is a per-lane input; keep the current copy.
            output.publish_json(output.admit(worktree / ".agent-workspace" / "task-card.json"), task_card)
            replacement = stage_authority(rt, epoch_id, {**lane, "run_id": run_id}, task_card)

            def replace_run(current: dict[str, Any]) -> dict[str, Any]:
                if any(current.get(field) != lane.get(field) for field in ("lane_id", "run_id", "worktree_path", "provider")) or current.get("lifecycle") != "resuming":
                    raise ValueError("lane run or status changed before fresh run ownership")
                marker = read_current_epoch(rt)
                runtime = read_runtime_state(rt)
                if not marker or marker.get("epoch_id") != epoch_id or not runtime or runtime.get("state") != "OPEN":
                    raise ValueError("runtime or epoch changed before fresh run ownership")
                return {
                    **current,
                    **{key: value for key, value in replacement.items() if key in {
                        "control_dir", "worktree_identity", "result_assignment_hash", "invocation_path", "invocation_hash", "prompt_path", "prompt_sha256",
                        "task_card_path", "task_card_hash", "controller_status_path", "controller_events_path",
                        "transcript_path", "stderr_path", "attempts_path", "last_message_path"}},
                    **{key: replacement[key] for key in ("isolation_config_path", "isolation_config_sha256") if key in replacement},
                    "run_id": run_id,
                    "lifecycle": "running",
                    "process": {},
                    "launch_pending": True,
                    "launch_failure": None,
                    "acceptance_advancement": None,
                    "last_reported_actionable_status": None,
                    "resume_from_run_id": prior_run_id,
                }

            update_lane(rt, epoch_id, lane_id, replace_run)
            from .lanes import reconcile_active_lanes
            reconcile_active_lanes(rt, epoch_id)
            if managed:
                _consume_resume_signal(rt, lane_id, prior_run_id)
    except Exception as exc:
        return {
            "ok": False,
            "code": RESUME_LANE_WRITE_FAILED,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "resolve the error and retry resume",
        }

    return {
        "ok": True,
        "code": "RESUME_OK",
        "summary": f"lane {lane_id} resumed under a fresh run_id; launch it to start the provider",
        "evidence_paths": [
            str(worktree / ".agent-workspace" / "invocation.json"),
            str(lane_record_dir(rt, epoch_id, lane_id) / "lane.json"),
        ],
        "next_action": "run `lane launch --lane-id <id>` to start the resumed run",
    }
