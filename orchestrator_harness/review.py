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
from .core import content_hash, iso_utc, read_json, require_schema, sha256_hex
from .epochs import lane_record_dir
from .lanes import LaneError, find_active_lane, read_lane
from . import memory_handoff, terminal_evidence
from .manager_queue import (
    MANAGER_ACK_EVENT_NOT_FOUND,
    ManagerQueueError,
    close_event,
    read_manager_queue,
)
from .records import RecordLock, atomic_write_json, read_record

COMPLETION_REVIEW_SCHEMA = "completion-review/v1"
ACCEPTANCE_SCHEMA = "orchestrator-acceptance/v1"
RESULT_SCHEMA = "result/v1"
TASK_CARD_SCHEMA = "project-task-card/v1"

REVIEW_OUTCOMES = frozenset({"PASS", "FAIL", "BLOCKED", "UNKNOWN"})
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
    outcome = review.get("review_outcome")
    if outcome == "UNKNOWN":
        proof = review.get("terminal_proof_digest")
        return (
            review.get("result_id") is None
            and review.get("result_hash") is None
            and acceptance.get("result_id") is None
            and acceptance.get("result_hash") is None
            and isinstance(proof, str) and bool(proof)
            and acceptance.get("terminal_proof_digest") == proof
            and acceptance.get("approval") == "ACCEPTED"
            and isinstance(acceptance.get("force_accept_reason"), str)
            and bool(acceptance["force_accept_reason"].strip())
        )
    if outcome not in REVIEW_OUTCOMES:
        return False
    for field in ("result_id", "result_hash"):
        value = review.get(field)
        if not isinstance(value, str) or not value or acceptance.get(field) != value:
            return False
    return True


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
    if not isinstance(record.get("summary"), str) or not record["summary"].strip():
        raise ReviewError(COMPLETION_REVIEW_STALE_SOURCE, "result summary is empty")
    if not isinstance(record.get("evidence"), list):
        raise ReviewError(COMPLETION_REVIEW_STALE_SOURCE, "result evidence is invalid")
    if not isinstance(record.get("completed_at"), str) or not record["completed_at"].strip():
        raise ReviewError(COMPLETION_REVIEW_STALE_SOURCE, "result completed_at is empty")
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
    if event.get("type") not in _REVIEW_EVENT_TYPES and not (
        event.get("type") == "LANE_STATUS_CHANGED"
        and event.get("actionable_status") == "provider_exited_no_result"
    ):
        raise ReviewError(
            COMPLETION_REVIEW_EVENT_INVALID,
            f"event {event_id} is not a review event (type={event.get('type')})",
        )
    if event.get("state") not in {"ACKNOWLEDGED", "COMPLETE"}:
        raise ReviewError(
            COMPLETION_REVIEW_NOT_ACKNOWLEDGED,
            f"event {event_id} is not ACKNOWLEDGED (state={event.get('state')})",
        )
    lane_id = str(event.get("lane_id") or "")
    try:
        epoch_id, lane = find_active_lane(rt, lane_id)
    except LaneError:
        # An older publication may already have advanced and retired its lane
        # before manager close was acknowledged. The queue still names its
        # exact epoch/run; replay can finish from retained publication.
        epoch_id = str(queue["epoch_id"])
        lane = read_lane(rt, epoch_id, lane_id)
    if lane.get("run_id") != event.get("run_id"):
        raise ReviewError(
            COMPLETION_REVIEW_EVENT_INVALID,
            f"event {event_id} does not match the lane's current run",
        )
    return epoch_id, lane, event


def _native_source(
    lane: dict[str, Any], task_card: dict[str, Any], worktree: Path,
) -> dict[str, Any] | None:
    """Resolve only an enhanced parent's exact delivered STEP-07 observation."""
    try:
        state = memory_handoff.lane_handoff_state(task_card, lane)
        if state is None:
            return None
        if state != "execution_accepted" or memory_handoff.enabled_memory_handoff(task_card) is None:
            raise memory_handoff.MemoryHandoffError(
                "the lane is not an execution-accepted enhanced parent"
            )
        plan = memory_handoff.require_accepted_handoff(task_card)
        envelope = memory_handoff.load_envelope(worktree)
        if envelope is None:
            raise memory_handoff.MemoryHandoffError("enhanced parent has no finalized dispatch envelope")
        memory_handoff.validate_envelope_for_launch(
            envelope=envelope, task_card=task_card, lane_id=lane["lane_id"],
            run_id=lane["run_id"], worktree_path=worktree,
            base_commit=task_card["base_commit"],
        )
        context = memory_handoff.load_final_context(
            worktree_path=worktree, envelope=envelope,
        )
        memory_handoff.validate_final_context_for_launch(
            context=context, envelope=envelope, task_card=task_card,
            lane_id=lane["lane_id"], run_id=lane["run_id"],
            worktree_path=worktree, base_commit=task_card["base_commit"],
        )
        operation = memory_handoff.get_dispatch_operation(
            worktree_path=worktree, envelope=envelope,
        )
        decision = memory_handoff.get_dispatch_decision(
            worktree_path=worktree, envelope=envelope,
        )
        if not isinstance(operation, dict) or operation.get("status") != "delivered":
            raise memory_handoff.MemoryHandoffError(
                "enhanced parent has no delivered native dispatch observation"
            )
        observed = operation.get("observed_invocation")
        if not isinstance(observed, dict):
            raise memory_handoff.MemoryHandoffError("delivered dispatch has no native invocation")
        expected = memory_handoff.native_observation(
            envelope=envelope, context=context, controller_identity=observed,
        )
        process = lane.get("process") or {}
        if (
            operation.get("run_id") != lane["run_id"]
            or operation.get("decision_id") != envelope["decision_id"]
            or operation.get("envelope_digest") != envelope["content_hash"]
            or observed != expected
            or any(process.get(field) != observed[field] for field in ("pid", "creation_time"))
        ):
            raise memory_handoff.MemoryHandoffError(
                "delivered native observation conflicts with this lane and run"
            )
        return {
            "plan": plan, "decision": decision, "envelope": envelope,
            "context": context, "operation": operation, "observed": observed,
        }
    except (OSError, ValueError, memory_handoff.MemoryHandoffError) as exc:
        raise ReviewError(COMPLETION_REVIEW_STALE_SOURCE, str(exc)) from exc


def _unknown_proof(lane: dict[str, Any], worktree: Path) -> dict[str, Any]:
    """Require controller-owned, same-run no-result and cleanup proof."""
    result_path = worktree / "RESULT.json"
    if result_path.exists():
        try:
            candidate = read_json(result_path)
        except (OSError, ValueError):
            candidate = None
        if isinstance(candidate, dict):
            if candidate.get("lane_id") != lane["lane_id"] or candidate.get("run_id") != lane["run_id"]:
                raise ReviewError(COMPLETION_REVIEW_STALE_SOURCE, "wrong-run result cannot establish UNKNOWN")
            if candidate.get("content_hash") == content_hash(candidate) and candidate.get("outcome") in {"PASS", "FAIL", "BLOCKED"}:
                raise ReviewError(COMPLETION_REVIEW_STALE_SOURCE, "a valid result exists; UNKNOWN is not justified")
    path_value = lane.get("controller_status_path")
    if not isinstance(path_value, str) or not path_value:
        raise ReviewError(COMPLETION_REVIEW_STALE_SOURCE, "controller proof path is missing")
    try:
        status = read_record(Path(path_value), "controller-status/v1")
    except (OSError, ValueError) as exc:
        raise ReviewError(COMPLETION_REVIEW_STALE_SOURCE, f"controller proof is missing or invalid: {exc}") from exc
    if not (
        status.get("lane_id") == lane["lane_id"]
        and status.get("run_id") == lane["run_id"]
        and status.get("controller_state") == "exited"
        and isinstance(status.get("provider_state"), dict)
        and status["provider_state"].get("state") == "exited"
        and status.get("result_state") in {"absent", "invalid"}
        and status.get("recorded_status") == "provider_exited_no_result"
        and status.get("cleanup_proven") is True
    ):
        raise ReviewError(
            COMPLETION_REVIEW_STALE_SOURCE,
            "exact same-run provider_exited_no_result and cleanup proof are required",
        )
    return status


def _existing_record(path: Path, schema: str) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        value = read_record(path, schema)
    except (OSError, ValueError) as exc:
        raise ReviewError(
            COMPLETION_REVIEW_OUTPUT_CONFLICT,
            f"existing publication at {path} is invalid and was preserved: {exc}",
        ) from exc
    if value.get("content_hash") != content_hash(value):
        raise ReviewError(
            COMPLETION_REVIEW_OUTPUT_CONFLICT,
            f"existing publication at {path} has an invalid hash and was preserved",
        )
    return value


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
    managed_event: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    folder = terminal_evidence.publication_dir(rt, epoch_id, lane)
    root_folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    if folder != root_folder:
        for name, schema in (
            ("COMPLETION_REVIEW.json", COMPLETION_REVIEW_SCHEMA),
            ("ORCHESTRATOR_ACCEPTANCE.json", ACCEPTANCE_SCHEMA),
            (terminal_evidence.TERMINAL_EVIDENCE_NAME, terminal_evidence.TERMINAL_EVIDENCE_SCHEMA),
        ):
            historical = _existing_record(root_folder / name, schema)
            if historical is not None and historical.get("run_id") == lane["run_id"]:
                raise ReviewError(
                    COMPLETION_REVIEW_OUTPUT_CONFLICT,
                    "the root publication already owns this exact run",
                )
    review_path = folder / "COMPLETION_REVIEW.json"
    acceptance_path = folder / "ORCHESTRATOR_ACCEPTANCE.json"
    terminal_path = folder / terminal_evidence.TERMINAL_EVIDENCE_NAME
    worktree = Path(lane["worktree_path"])
    task_card = _read_task_card(worktree)
    native = _native_source(lane, task_card, worktree)
    if review_outcome == "UNKNOWN":
        if native is None or approval != "ACCEPTED" or force_accept_reason is None:
            raise ReviewError(
                COMPLETION_REVIEW_FORCE_REASON_INVALID,
                "UNKNOWN requires an enhanced parent and explicit exceptional ROOT acceptance",
            )
        result = None
        terminal_proof = _unknown_proof(lane, worktree)
    else:
        result = _read_result(worktree, lane)
        terminal_proof = None
    commit = _worktree_commit(worktree, task_card)
    task_card_id = str(task_card.get("card_id") or task_card.get("id") or "")
    if not task_card_id:
        task_card_id = content_hash(task_card)
    task_card_hash = content_hash(task_card)
    result_id = lane["run_id"] if result is not None else None
    result_hash = content_hash(result) if result is not None else None
    with RecordLock(review_path):
        existing_review = _existing_record(review_path, COMPLETION_REVIEW_SCHEMA)
        existing_acceptance = _existing_record(acceptance_path, ACCEPTANCE_SCHEMA)
        existing_terminal = _existing_record(
            terminal_path, terminal_evidence.TERMINAL_EVIDENCE_SCHEMA,
        )
        if existing_terminal is not None and existing_review is None:
            raise ReviewError(
                COMPLETION_REVIEW_OUTPUT_CONFLICT,
                "terminal evidence exists without its review and was preserved",
            )
        reviewed_at = (
            existing_review.get("reviewed_at") if existing_review is not None
            else existing_acceptance.get("decided_at") if existing_acceptance is not None
            else iso_utc()
        )
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
        if terminal_proof is not None:
            review["terminal_proof_digest"] = sha256_hex(terminal_proof)
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
        if terminal_proof is not None:
            acceptance["terminal_proof_digest"] = sha256_hex(terminal_proof)
        if force_accept_reason is not None:
            acceptance["force_accept_reason"] = force_accept_reason
        acceptance["content_hash"] = content_hash(acceptance)
        if not validate_acceptance_chain(
            review, acceptance, lane_id=lane["lane_id"], run_id=lane["run_id"],
        ):
            raise ReviewError(COMPLETION_REVIEW_WRITE_FAILED, "constructed review chain is invalid")
        if native is not None:
            envelope = native["envelope"]
            terminal = terminal_evidence.make_terminal_evidence(
                epoch_id=epoch_id, lane_id=lane["lane_id"], run_id=lane["run_id"],
                task_card=task_card, accepted_plan=native["plan"],
                objective_id=envelope["objective_id"], decision_id=envelope["decision_id"],
                decision=native["decision"], envelope=envelope,
                final_context=native["context"],
                dispatch_operation=native["operation"],
                envelope_digest=envelope["content_hash"],
                observed_invocation=native["observed"],
                configuration=envelope["configuration"],
                configuration_digest=envelope["configuration_digest"],
                result=result, review=review, acceptance=acceptance,
                terminal_proof=terminal_proof,
            )
        else:
            terminal = None
        for old, new, name in (
            (existing_review, review, "review"),
            (existing_acceptance, acceptance, "acceptance"),
            (existing_terminal, terminal, "terminal evidence"),
        ):
            if old is not None and old != new:
                raise ReviewError(
                    COMPLETION_REVIEW_OUTPUT_CONFLICT,
                    f"existing {name} conflicts with this exact review and was preserved",
                )
        if existing_review is None:
            atomic_write_json(review_path, review)
        if terminal is not None and existing_terminal is None:
            atomic_write_json(terminal_path, terminal)
        if terminal is not None:
            terminal_evidence.record_domain_review(rt, epoch_id, lane, terminal)
        if terminal is not None and managed_event is not None and managed_event.get("state") != "COMPLETE":
            try:
                close_event(
                    rt, managed_event["event_id"], "COMPLETE",
                    summary=f"completion review recorded: {review_outcome} / {approval}",
                )
            except ManagerQueueError as exc:
                raise ReviewError(
                    COMPLETION_REVIEW_WRITE_FAILED,
                    f"review prepared but the event could not be closed: {exc}",
                ) from exc
        # Acceptance is the existing harness advancement signal. Publish it
        # only after the enhanced evidence and managed close are durable.
        if existing_acceptance is None:
            atomic_write_json(acceptance_path, acceptance)
    return review, acceptance


def _replay_retained_pair(
    rt: Path, epoch_id: str, lane: dict[str, Any], *, review_outcome: str,
    review_summary: str, evidence: list[str], approval: str,
    force_accept_reason: str | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Retry manager close from an exact publication, even after retirement."""
    folder = terminal_evidence.publication_dir(rt, epoch_id, lane)
    review = _existing_record(folder / "COMPLETION_REVIEW.json", COMPLETION_REVIEW_SCHEMA)
    acceptance = _existing_record(folder / "ORCHESTRATOR_ACCEPTANCE.json", ACCEPTANCE_SCHEMA)
    if review is None or acceptance is None or not validate_acceptance_chain(
        review, acceptance, lane_id=lane["lane_id"], run_id=lane["run_id"],
    ):
        raise ReviewError(COMPLETION_REVIEW_OUTPUT_CONFLICT, "retained review pair is absent or invalid")
    if (
        review.get("review_outcome") != review_outcome
        or review.get("review_summary") != review_summary
        or review.get("evidence") != evidence
        or acceptance.get("approval") != approval
        or acceptance.get("force_accept_reason") != force_accept_reason
    ):
        raise ReviewError(COMPLETION_REVIEW_OUTPUT_CONFLICT, "retained review conflicts with this retry")
    if lane.get("memory_plan_state") == "execution_accepted":
        try:
            terminal = terminal_evidence.read_terminal_evidence(
                rt, epoch_id, lane["lane_id"], run_id=lane["run_id"],
            )
        except terminal_evidence.TerminalEvidenceError as exc:
            raise ReviewError(COMPLETION_REVIEW_OUTPUT_CONFLICT, str(exc)) from exc
        if terminal is None or terminal["review"] != review or terminal["acceptance"] != acceptance:
            raise ReviewError(COMPLETION_REVIEW_OUTPUT_CONFLICT, "retained native evidence is absent or conflicts")
        terminal_evidence.record_domain_review(rt, epoch_id, lane, terminal)
    return review, acceptance


def _replay_retained_preparation(
    rt: Path, epoch_id: str, lane: dict[str, Any], *, review_outcome: str,
    review_summary: str, evidence: list[str], approval: str,
    force_accept_reason: str | None, managed_event: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Finish only the missing acceptance from validated native preparation."""
    folder = terminal_evidence.publication_dir(rt, epoch_id, lane)
    review_path = folder / "COMPLETION_REVIEW.json"
    acceptance_path = folder / "ORCHESTRATOR_ACCEPTANCE.json"
    with RecordLock(review_path):
        review = _existing_record(review_path, COMPLETION_REVIEW_SCHEMA)
        terminal = _existing_record(
            folder / terminal_evidence.TERMINAL_EVIDENCE_NAME,
            terminal_evidence.TERMINAL_EVIDENCE_SCHEMA,
        )
        if review is None or terminal is None or acceptance_path.exists():
            raise ReviewError(COMPLETION_REVIEW_OUTPUT_CONFLICT, "retained preparation is absent or already published")
        try:
            terminal_evidence.validate_terminal_evidence(
                terminal, lane_id=lane["lane_id"], run_id=lane["run_id"],
            )
        except terminal_evidence.TerminalEvidenceError as exc:
            raise ReviewError(COMPLETION_REVIEW_OUTPUT_CONFLICT, str(exc)) from exc
        acceptance = terminal["acceptance"]
        if (
            terminal["epoch_id"] != epoch_id
            or terminal["review"] != review
            or not validate_acceptance_chain(
                review, acceptance, lane_id=lane["lane_id"], run_id=lane["run_id"],
            )
            or review.get("review_outcome") != review_outcome
            or review.get("review_summary") != review_summary
            or review.get("evidence") != evidence
            or acceptance.get("approval") != approval
            or acceptance.get("force_accept_reason") != force_accept_reason
        ):
            raise ReviewError(COMPLETION_REVIEW_OUTPUT_CONFLICT, "retained preparation conflicts with this retry")
        if folder != lane_record_dir(rt, epoch_id, lane["lane_id"]):
            try:
                terminal_evidence.validate_root_siblings(
                    rt, epoch_id, lane["lane_id"], lane["run_id"], terminal,
                )
            except terminal_evidence.TerminalEvidenceError as exc:
                raise ReviewError(COMPLETION_REVIEW_OUTPUT_CONFLICT, str(exc)) from exc
        terminal_evidence.record_domain_review(rt, epoch_id, lane, terminal)
        if managed_event.get("state") != "COMPLETE":
            try:
                close_event(
                    rt, managed_event["event_id"], "COMPLETE",
                    summary=f"completion review recorded: {review_outcome} / {approval}",
                )
            except ManagerQueueError as exc:
                raise ReviewError(
                    COMPLETION_REVIEW_WRITE_FAILED,
                    f"review prepared but the event could not be closed: {exc}",
                ) from exc
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
        if review_outcome == "UNKNOWN" and approval != "ACCEPTED":
            raise ReviewError(
                COMPLETION_REVIEW_FORCE_REASON_INVALID,
                "UNKNOWN requires explicit exceptional ROOT acceptance",
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
        folder = terminal_evidence.publication_dir(rt, epoch_id, lane)
        retained_preparation = (
            event is not None
            and lane.get("memory_plan_state") == "execution_accepted"
            and (folder / "COMPLETION_REVIEW.json").is_file()
            and (folder / terminal_evidence.TERMINAL_EVIDENCE_NAME).is_file()
            and not (folder / "ORCHESTRATOR_ACCEPTANCE.json").exists()
        )
        if lane.get("lifecycle") not in ("review_pending", "result_invalid"):
            if not (
                (lane.get("lifecycle") == "accepted" or (
                    lane.get("lifecycle") == "retired" and event is not None
                ))
                and (folder / "COMPLETION_REVIEW.json").is_file()
                and ((folder / "ORCHESTRATOR_ACCEPTANCE.json").is_file() or retained_preparation)
            ):
                raise ReviewError(
                    COMPLETION_REVIEW_STALE_SOURCE,
                    f"lane {lane['lane_id']} is not terminal (lifecycle={lane.get('lifecycle')})",
                )
        if event is not None and event.get("state") == "COMPLETE":
            if not (folder / "COMPLETION_REVIEW.json").is_file() or (
                lane.get("memory_plan_state") == "execution_accepted"
                and not (folder / terminal_evidence.TERMINAL_EVIDENCE_NAME).is_file()
            ) or (
                lane.get("memory_plan_state") != "execution_accepted"
                and not (folder / "ORCHESTRATOR_ACCEPTANCE.json").is_file()
            ):
                raise ReviewError(
                    COMPLETION_REVIEW_OUTPUT_CONFLICT,
                    "completed review event has no durable review preparation",
                )
        exact_reason = force_reason if (force_accept and approval == "ACCEPTED" and review_outcome != "PASS") else None
        if retained_preparation:
            review, acceptance = _replay_retained_preparation(
                rt, epoch_id, lane, review_outcome=review_outcome,
                review_summary=review_summary, evidence=evidence,
                approval=approval, force_accept_reason=exact_reason,
                managed_event=event,
            )
        elif event is not None and (folder / "ORCHESTRATOR_ACCEPTANCE.json").is_file():
            review, acceptance = _replay_retained_pair(
                rt, epoch_id, lane, review_outcome=review_outcome,
                review_summary=review_summary, evidence=evidence,
                approval=approval, force_accept_reason=exact_reason,
            )
            if event.get("state") != "COMPLETE":
                try:
                    close_event(
                        rt, event["event_id"], "COMPLETE",
                        summary=f"completion review recorded: {review_outcome} / {approval}",
                    )
                except ManagerQueueError as exc:
                    raise ReviewError(
                        COMPLETION_REVIEW_WRITE_FAILED,
                        f"retained review is valid but the event could not be closed: {exc}",
                    ) from exc
        else:
            review, acceptance = _write_pair(
                rt, epoch_id, lane, review_outcome=review_outcome,
                review_summary=review_summary, evidence=evidence,
                approval=approval, force_accept_reason=exact_reason,
                managed_event=event,
            )
            if event is not None and event.get("state") != "COMPLETE" and lane.get("memory_plan_state") != "execution_accepted":
                try:
                    close_event(
                        rt, event["event_id"], "COMPLETE",
                        summary=f"completion review recorded: {review_outcome} / {approval}",
                    )
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

    folder = terminal_evidence.publication_dir(rt, epoch_id, lane)
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
        ] + ([str(folder / terminal_evidence.TERMINAL_EVIDENCE_NAME)]
             if (folder / terminal_evidence.TERMINAL_EVIDENCE_NAME).is_file() else []),
        "next_action": next_action,
    }
