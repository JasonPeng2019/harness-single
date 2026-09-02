"""``lane completion-review``: the only writer of the review/acceptance pair.

The lifecycle is task card -> RESULT.json -> COMPLETION_REVIEW.json ->
ORCHESTRATOR_ACCEPTANCE.json.  This command records ROOT's factual finding
(``--review-outcome``) and ROOT's separate accept/reject decision
(``--approval``) as a linked pair outside the worktree, and closes its own
managed review event automatically.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from .config import find_harness_root, load_config
from .core import content_hash, iso_utc, read_json, require_schema
from .epochs import lane_record_dir
from .lanes import find_active_lane, read_lane
from .manager_queue import (
    MANAGER_ACK_EVENT_NOT_FOUND,
    ManagerQueueError,
    close_event,
    read_manager_queue,
)
from .records import RecordLock, atomic_write_json

COMPLETION_REVIEW_SCHEMA = "completion-review/v1"
ACCEPTANCE_SCHEMA = "orchestrator-acceptance/v1"
RESULT_SCHEMA = "result/v1"
TASK_CARD_SCHEMA = "project-task-card/v1"

REVIEW_OUTCOMES = frozenset({"PASS", "FAIL", "BLOCKED"})
APPROVALS = frozenset({"ACCEPTED", "REJECTED"})
_REVIEW_EVENT_TYPES = frozenset({"COMPLETION_REVIEW_REQUIRED", "LANE_RESULT_INVALID"})

COMPLETION_REVIEW_EVENT_INVALID = "COMPLETION_REVIEW_EVENT_INVALID"
COMPLETION_REVIEW_NOT_ACKNOWLEDGED = "COMPLETION_REVIEW_NOT_ACKNOWLEDGED"
COMPLETION_REVIEW_STALE_SOURCE = "COMPLETION_REVIEW_STALE_SOURCE"
COMPLETION_REVIEW_FORCE_REASON_INVALID = "COMPLETION_REVIEW_FORCE_REASON_INVALID"
COMPLETION_REVIEW_OUTPUT_CONFLICT = "COMPLETION_REVIEW_OUTPUT_CONFLICT"
COMPLETION_REVIEW_WRITE_FAILED = "COMPLETION_REVIEW_WRITE_FAILED"


class ReviewError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def validate_acceptance_chain(
    review: dict[str, Any],
    acceptance: dict[str, Any],
    *,
    lane_id: str,
    run_id: str | None = None,
) -> bool:
    """Validate the complete review/acceptance link before it is honored.

    Both records are integrity checked and every task, result, lane, run, and
    commit identifier must match.  In particular, ``review_ref`` must point to
    the actual review content hash; matching copied fields alone are not a
    valid acceptance chain.
    """

    if review.get("content_hash") != content_hash(review):
        return False
    if acceptance.get("content_hash") != content_hash(acceptance):
        return False
    if acceptance.get("review_ref") != review.get("content_hash"):
        return False
    required = (
        "lane_id",
        "run_id",
        "task_card_id",
        "task_card_hash",
        "result_id",
        "result_hash",
        "commit",
    )
    for field in required:
        review_value = review.get(field)
        acceptance_value = acceptance.get(field)
        if not isinstance(review_value, str) or not review_value:
            return False
        if acceptance_value != review_value:
            return False
    if review.get("lane_id") != lane_id or acceptance.get("lane_id") != lane_id:
        return False
    if run_id is not None and review.get("run_id") != run_id:
        return False
    if acceptance.get("approval") not in APPROVALS:
        return False
    if not isinstance(acceptance.get("accepted_by"), str) or not acceptance["accepted_by"]:
        return False
    return review.get("review_outcome") in REVIEW_OUTCOMES


def _read_task_card(worktree: Path) -> dict[str, Any]:
    path = worktree / ".agent-workspace" / "task-card.json"
    if not path.is_file():
        raise ReviewError(
            COMPLETION_REVIEW_STALE_SOURCE, f"task card copy missing: {path}"
        )
    try:
        record = read_json(path)
        require_schema(record, TASK_CARD_SCHEMA, path)
    except (OSError, ValueError) as exc:
        raise ReviewError(COMPLETION_REVIEW_STALE_SOURCE, str(exc)) from exc
    return record


def _read_result(worktree: Path, lane: dict[str, Any]) -> dict[str, Any]:
    path = worktree / "RESULT.json"
    if not path.is_file():
        raise ReviewError(COMPLETION_REVIEW_STALE_SOURCE, f"result missing: {path}")
    try:
        record = read_json(path)
        require_schema(record, RESULT_SCHEMA, path)
    except (OSError, ValueError) as exc:
        raise ReviewError(COMPLETION_REVIEW_STALE_SOURCE, str(exc)) from exc
    if record.get("lane_id") != lane["lane_id"] or record.get("run_id") != lane.get("run_id"):
        raise ReviewError(
            COMPLETION_REVIEW_STALE_SOURCE,
            "result does not match the lane's current run",
        )
    if record.get("outcome") not in {"PASS", "FAIL", "BLOCKED"}:
        raise ReviewError(COMPLETION_REVIEW_STALE_SOURCE, "result outcome is invalid")
    if record.get("content_hash") != content_hash(record):
        raise ReviewError(COMPLETION_REVIEW_STALE_SOURCE, "result content hash mismatch")
    return record


def _worktree_commit(worktree: Path, task_card: dict[str, Any]) -> str:
    completed = subprocess.run(
        ["git", "-C", str(worktree), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode == 0 and completed.stdout.strip():
        return completed.stdout.strip()
    base = task_card.get("base_commit")
    return str(base) if isinstance(base, str) and base else ""


def _resolve_lane_managed(
    rt: Path, event_id: str
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    """Resolve the lane and its review event from the manager queue."""
    try:
        queue = read_manager_queue(rt)
    except ManagerQueueError as exc:
        raise ReviewError(COMPLETION_REVIEW_EVENT_INVALID, str(exc)) from exc
    event = None
    for candidate in queue.get("events", []):
        if candidate.get("event_id") == event_id:
            event = candidate
            break
    if event is None:
        raise ReviewError(
            COMPLETION_REVIEW_EVENT_INVALID, f"review event not found: {event_id}"
        )
    if event.get("type") not in _REVIEW_EVENT_TYPES:
        raise ReviewError(
            COMPLETION_REVIEW_EVENT_INVALID,
            f"event {event_id} is not a review event (type={event.get('type')})",
        )
    if event.get("state") != "ACKNOWLEDGED":
        raise ReviewError(
            COMPLETION_REVIEW_NOT_ACKNOWLEDGED,
            f"event {event_id} is not ACKNOWLEDGED (state={event.get('state')})",
        )
    lane_id = str(event.get("lane_id") or "")
    epoch_id, lane = find_active_lane(rt, lane_id)
    if lane.get("run_id") != event.get("run_id"):
        raise ReviewError(
            COMPLETION_REVIEW_EVENT_INVALID,
            f"event {event_id} does not match the lane's current run",
        )
    return epoch_id, lane, event


def _write_pair(
    rt: Path,
    epoch_id: str,
    lane: dict[str, Any],
    *,
    review_outcome: str,
    review_summary: str,
    evidence: list[str],
    approval: str,
    force_accept_reason: str | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    review_path = folder / "COMPLETION_REVIEW.json"
    acceptance_path = folder / "ORCHESTRATOR_ACCEPTANCE.json"
    if review_path.is_file() or acceptance_path.is_file():
        raise ReviewError(
            COMPLETION_REVIEW_OUTPUT_CONFLICT,
            "a review/acceptance pair already exists for this lane",
        )
    worktree = Path(lane["worktree_path"])
    task_card = _read_task_card(worktree)
    result = _read_result(worktree, lane)
    commit = _worktree_commit(worktree, task_card)
    task_card_id = str(task_card.get("card_id") or task_card.get("id") or "")
    if not task_card_id:
        task_card_id = content_hash(task_card)
    task_card_hash = content_hash(task_card)
    result_id = str(lane.get("run_id") or "")
    result_hash = content_hash(result)
    reviewed_at = iso_utc()
    review = {
        "schema": COMPLETION_REVIEW_SCHEMA,
        "lane_id": lane["lane_id"],
        "run_id": lane["run_id"],
        "review_outcome": review_outcome,
        "review_summary": review_summary,
        "evidence": list(evidence),
        "task_card_id": task_card_id,
        "task_card_hash": task_card_hash,
        "result_id": result_id,
        "result_hash": result_hash,
        "commit": commit,
        "reviewed_at": reviewed_at,
    }
    review["content_hash"] = content_hash(review)
    acceptance = {
        "schema": ACCEPTANCE_SCHEMA,
        "lane_id": lane["lane_id"],
        "run_id": lane["run_id"],
        "approval": approval,
        "accepted_by": "ROOT",
        "review_ref": review["content_hash"],
        "task_card_id": task_card_id,
        "task_card_hash": task_card_hash,
        "result_id": result_id,
        "result_hash": result_hash,
        "commit": commit,
        "decided_at": reviewed_at,
    }
    if force_accept_reason is not None:
        acceptance["force_accept_reason"] = force_accept_reason
    acceptance["content_hash"] = content_hash(acceptance)
    with RecordLock(review_path):
        atomic_write_json(review_path, review)
        atomic_write_json(acceptance_path, acceptance)
    return review, acceptance


def run_completion_review(
    *,
    event_id: str | None,
    lane_id: str | None,
    review_outcome: str,
    approval: str,
    review_summary: str,
    evidence: list[str],
    force_accept: bool,
    force_reason: str | None,
) -> dict[str, Any]:
    """Execute ``lane completion-review`` and return the structured result."""
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": COMPLETION_REVIEW_WRITE_FAILED,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "fix the configuration and re-run setup",
        }
    rt = config.runtime_root
    try:
        if review_outcome not in REVIEW_OUTCOMES:
            raise ReviewError(
                COMPLETION_REVIEW_WRITE_FAILED,
                f"invalid review outcome: {review_outcome}",
            )
        if approval not in APPROVALS:
            raise ReviewError(
                COMPLETION_REVIEW_WRITE_FAILED, f"invalid approval: {approval}"
            )
        if approval == "ACCEPTED" and review_outcome != "PASS":
            if not force_accept:
                raise ReviewError(
                    COMPLETION_REVIEW_FORCE_REASON_INVALID,
                    "ACCEPTED requires a PASS finding unless --force-accept is used",
                )
            if not force_reason or not force_reason.strip():
                raise ReviewError(
                    COMPLETION_REVIEW_FORCE_REASON_INVALID,
                    "--force-accept requires a non-empty --force-reason",
                )
        if event_id is not None:
            epoch_id, lane, event = _resolve_lane_managed(rt, event_id)
        elif lane_id is not None:
            epoch_id, lane = find_active_lane(rt, lane_id)
            event = None
        else:
            raise ReviewError(
                COMPLETION_REVIEW_EVENT_INVALID,
                "select the lane with --event-id (managed) or --lane-id (plain)",
            )
        if lane.get("lifecycle") not in ("review_pending", "result_invalid"):
            raise ReviewError(
                COMPLETION_REVIEW_STALE_SOURCE,
                f"lane {lane['lane_id']} is not terminal (lifecycle={lane.get('lifecycle')})",
            )
        review, acceptance = _write_pair(
            rt,
            epoch_id,
            lane,
            review_outcome=review_outcome,
            review_summary=review_summary,
            evidence=evidence,
            approval=approval,
            force_accept_reason=force_reason if (force_accept and approval == "ACCEPTED" and review_outcome != "PASS") else None,
        )
        if event is not None:
            try:
                close_event(rt, event["event_id"], "COMPLETE")
            except ManagerQueueError as exc:
                raise ReviewError(
                    COMPLETION_REVIEW_WRITE_FAILED,
                    f"review pair written but the event could not be closed: {exc}",
                ) from exc
    except ReviewError as exc:
        return {
            "ok": False,
            "code": exc.code,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "resolve the error and retry the review",
        }
    except Exception as exc:
        return {
            "ok": False,
            "code": COMPLETION_REVIEW_WRITE_FAILED,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "resolve the error and retry the review",
        }

    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    next_action = (
        "retire the lane with `lane retire --acceptance-ref <file>` when done"
        if approval == "ACCEPTED"
        else "resume the lane with `resume-lane` to redo the work"
    )
    return {
        "ok": True,
        "code": "COMPLETION_REVIEW_OK",
        "summary": f"review recorded: {review_outcome} / {approval}",
        "evidence_paths": [
            str(folder / "COMPLETION_REVIEW.json"),
            str(folder / "ORCHESTRATOR_ACCEPTANCE.json"),
        ],
        "next_action": next_action,
    }
