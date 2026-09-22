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

from .bootstrap import (
    _write_invocation,
    _write_result_template,
    _write_worker_binding,
    _write_worker_prompt,
)
from .config import find_harness_root, load_config
from .core import content_hash, iso_utc, new_id, read_json, require_schema
from .epochs import lane_record_dir
from .lanes import find_active_lane, update_lane
from .records import atomic_write_json, remove_record
from .manager_queue import acknowledge_event, close_event, read_manager_queue
from .task_cards import validate_task_card

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
    if not review_path.is_file() or not acceptance_path.is_file():
        return False
    try:
        review = read_json(review_path)
        acceptance = read_json(acceptance_path)
        require_schema(review, COMPLETION_REVIEW_SCHEMA, review_path)
        require_schema(acceptance, ACCEPTANCE_SCHEMA, acceptance_path)
    except (OSError, ValueError):
        return False
    if review.get("run_id") != lane.get("run_id"):
        return False
    if acceptance.get("run_id") != lane.get("run_id"):
        return False
    if acceptance.get("review_ref") != review.get("content_hash"):
        return False
    return acceptance.get("approval") == "ACCEPTED"


def _clear_prior_run(rt: Path, epoch_id: str, lane: dict[str, Any]) -> None:
    """Remove the prior run's obsolete current state (best-effort, honest)."""
    worktree = Path(lane["worktree_path"])
    remove_record(worktree / "RESULT.json")
    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    remove_record(folder / "COMPLETION_REVIEW.json")
    remove_record(folder / "ORCHESTRATOR_ACCEPTANCE.json")
    remove_record(worktree / ".agent-workspace" / "controller.status.json")


def _reset_worker_inbox(worktree: Path, lane_id: str, run_id: str) -> None:
    """Reset the managed worker inbox to a valid empty queue for the new run."""
    inbox = {
        "schema": LANE_INBOX_SCHEMA,
        "lane_id": lane_id,
        "run_id": run_id,
        "assignments": [],
    }
    atomic_write_json(worktree / ".agent-workspace" / "QUEUE.json", inbox)


def _rewrite_overlay_receipt(
    worktree: Path, lane: dict[str, Any], run_id: str, *, managed: bool
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
        receipt["provider_payload"] = f"adapter-payloads/{lane['provider']['id']}"
    atomic_write_json(worktree / ".agent-workspace" / "overlay-receipt.json", receipt)


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
                "code": ALREADY_ACCEPTED,
                "summary": f"lane {lane_id} is accepted; it is not re-resumed",
                "evidence_paths": [],
                "next_action": "retire the lane with `lane retire --acceptance-ref <file>`",
            }
        if lifecycle == "retired":
            return {
                "ok": False,
                "code": RESUME_LANE_WRITE_FAILED,
                "summary": f"lane {lane_id} is retired; resume is not a cleanup tool",
                "evidence_paths": [],
                "next_action": "bootstrap a fresh lane",
            }
        process = lane.get("process") or {}
        from . import processes

        if lifecycle == "running" and processes.identity_matches(
            process.get("pid"), process.get("creation_time")
        ):
            return {
                "ok": False,
                "code": LANE_RUNNING,
                "summary": f"lane {lane_id} is still active; it is not resumed",
                "evidence_paths": [],
                "next_action": "wait for the lane to stop, or force-stop it first",
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
        update_lane(
            rt,
            epoch_id,
            lane_id,
            lambda current: {
                **current,
                "lifecycle": "resuming",
                "resume_started_at": iso_utc(),
                "resume_from_run_id": prior_run_id,
            },
        )
        _clear_prior_run(rt, epoch_id, lane)
        if managed:
            _reset_worker_inbox(worktree, lane_id, run_id)
        _write_worker_prompt(
            worktree, task_card, managed=managed, rationale=rationale
        )
        _write_result_template(worktree, lane_id, run_id)
        invocation_path = worktree / ".agent-workspace" / "invocation.json"
        try:
            prior_invocation = read_json(invocation_path)
            require_schema(prior_invocation, INVOCATION_SCHEMA, invocation_path)
            exclusive_resources = [
                str(item) for item in prior_invocation.get("exclusive_resources", [])
            ]
        except (OSError, ValueError):
            exclusive_resources = []
        invocation = _write_invocation(
            worktree,
            lane_id=lane_id,
            run_id=run_id,
            provider_id=lane["provider"]["id"],
            model=lane["provider"]["model"],
            launch_config=lane["provider"]["launch_config"],
            exclusive_resources=exclusive_resources,
        )
        invocation_path = worktree / ".agent-workspace" / "invocation.json"
        written_invocation = read_json(invocation_path)
        require_schema(written_invocation, INVOCATION_SCHEMA, invocation_path)
        if (
            written_invocation.get("lane_id") != lane_id
            or written_invocation.get("run_id") != run_id
            or written_invocation.get("provider", {}).get("id") != lane["provider"]["id"]
            or written_invocation.get("content_hash") != content_hash(written_invocation)
        ):
            raise ValueError("fresh resume invocation failed identity or hash validation")
        _rewrite_overlay_receipt(worktree, lane, run_id, managed=managed)
        if managed:
            _write_worker_binding(worktree, rt, lane_id, run_id)
        # The resume task card is a per-lane input; keep the current copy.
        atomic_write_json(worktree / ".agent-workspace" / "task-card.json", task_card)

        update_lane(
            rt,
            epoch_id,
            lane_id,
            lambda current, value=run_id: {
                **current,
                "run_id": value,
                "lifecycle": "running",
                "process": {},
                "launch_pending": True,
                "acceptance_advancement": None,
                "last_reported_actionable_status": None,
                "resume_from_run_id": prior_run_id,
            },
        )
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
