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
INVOCATION_SCHEMA = "controller-invocation/v1"

FRESH_LANE_INTEGRITY_DIAGNOSTIC = (
    "lane uses the pre-integrity runtime contract; bootstrap a fresh lane "
    "in a fresh epoch"
)

REVIEW_OUTCOMES = frozenset({"PASS", "FAIL", "BLOCKED"})
APPROVALS = frozenset({"ACCEPTED", "REJECTED"})
_REVIEW_EVENT_TYPES = frozenset({"COMPLETION_REVIEW_REQUIRED", "LANE_RESULT_INVALID"})

COMPLETION_REVIEW_EVENT_INVALID = "COMPLETION_REVIEW_EVENT_INVALID"
COMPLETION_REVIEW_NOT_ACKNOWLEDGED = "COMPLETION_REVIEW_NOT_ACKNOWLEDGED"
COMPLETION_REVIEW_STALE_SOURCE = "COMPLETION_REVIEW_STALE_SOURCE"
COMPLETION_REVIEW_FORCE_REASON_INVALID = "COMPLETION_REVIEW_FORCE_REASON_INVALID"
COMPLETION_REVIEW_OUTPUT_CONFLICT = "COMPLETION_REVIEW_OUTPUT_CONFLICT"
COMPLETION_REVIEW_WRITE_FAILED = "COMPLETION_REVIEW_WRITE_FAILED"


class GitStateError(RuntimeError):
    """The lane's current Git state cannot be proven merge-ready."""


class ReviewError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def lane_integrity_contract_error(lane: dict[str, Any]) -> str | None:
    """Return the stable migration diagnostic for a pre-integrity lane."""

    git = lane.get("git")
    if (
        not isinstance(git, dict)
        or "current_tip" in git
        or not isinstance(git.get("bootstrap_tip"), str)
        or not git.get("bootstrap_tip")
        or not isinstance(lane.get("invocation_hash"), str)
        or not lane.get("invocation_hash")
    ):
        return FRESH_LANE_INTEGRITY_DIAGNOSTIC
    return None


def validate_invocation_binding(
    lane: dict[str, Any], invocation: dict[str, Any]
) -> None:
    """Fail unless an invocation is the immutable one published by the lane."""

    legacy = lane_integrity_contract_error(lane)
    if legacy is not None:
        raise GitStateError(legacy)
    invocation_hash = invocation.get("content_hash")
    if not isinstance(invocation_hash, str) or invocation_hash != content_hash(invocation):
        raise GitStateError("invocation content hash mismatch")
    if invocation_hash != lane.get("invocation_hash"):
        raise GitStateError(
            "invocation no longer matches the authoritative lane invocation hash"
        )
    if (
        invocation.get("lane_id") != lane.get("lane_id")
        or invocation.get("run_id") != lane.get("run_id")
        or invocation.get("provider") != lane.get("provider")
        or invocation.get("git") != lane.get("git")
    ):
        raise GitStateError("invocation identity does not match the authoritative lane")


def _git(
    worktree: Path,
    *args: str,
    allowed_returncodes: frozenset[int] = frozenset({0}),
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        ["git", "-C", str(worktree), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode not in allowed_returncodes:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise GitStateError(
            f"git {' '.join(args)} failed: {detail or f'exit {completed.returncode}'}"
        )
    return completed


def validate_merge_ready_git(lane: dict[str, Any]) -> dict[str, Any]:
    """Return the exact current Git identity or fail closed.

    Tracked, staged, and unmerged changes are always dirty.  Untracked files
    are allowed only for the explicit bootstrap-owned paths recorded in the
    lane: the reserved ``.agent-workspace`` tree, ``RESULT.json``, and exact
    provider payload files.  Ignored files retain normal Git-clean semantics.
    """

    git = lane.get("git")
    if not isinstance(git, dict):
        raise GitStateError("lane has no authoritative Git identity")
    if "current_tip" in git or "bootstrap_tip" not in git:
        raise GitStateError(FRESH_LANE_INTEGRITY_DIAGNOSTIC)
    required = (
        "source_root",
        "common_dir",
        "branch",
        "base_commit",
        "origin_tip",
        "bootstrap_tip",
    )
    if any(not isinstance(git.get(field), str) or not git[field] for field in required):
        raise GitStateError("lane Git identity is incomplete")
    worktree = Path(str(lane.get("worktree_path") or ""))
    if not worktree.is_dir():
        raise GitStateError(f"lane worktree is missing: {worktree}")

    branch = _git(worktree, "symbolic-ref", "--quiet", "--short", "HEAD").stdout.strip()
    if branch != git["branch"]:
        raise GitStateError(
            f"lane branch mismatch: expected {git['branch']}, got {branch or '(detached)'}"
        )
    commit = _git(worktree, "rev-parse", "--verify", "HEAD^{commit}").stdout.strip()
    if len(commit) != 40:
        raise GitStateError("lane HEAD did not resolve to a full commit")

    common_text = _git(worktree, "rev-parse", "--git-common-dir").stdout.strip()
    common_dir = Path(common_text)
    if not common_dir.is_absolute():
        common_dir = worktree / common_dir
    if common_dir.resolve() != Path(git["common_dir"]).resolve():
        raise GitStateError("lane Git common directory no longer matches bootstrap")

    ancestry = _git(
        worktree,
        "merge-base",
        "--is-ancestor",
        str(git["origin_tip"]),
        commit,
        allowed_returncodes=frozenset({0, 1}),
    )
    if ancestry.returncode != 0:
        raise GitStateError("lane HEAD no longer descends from its recorded origin tip")

    unmerged = _git(worktree, "ls-files", "-u", "-z").stdout
    if unmerged:
        raise GitStateError("lane worktree contains unmerged paths")
    unstaged = _git(
        worktree,
        "diff",
        "--quiet",
        "--ignore-submodules=none",
        "--",
        allowed_returncodes=frozenset({0, 1}),
    )
    if unstaged.returncode != 0:
        raise GitStateError("lane worktree has modified tracked files")
    staged = _git(
        worktree,
        "diff",
        "--cached",
        "--quiet",
        "--ignore-submodules=none",
        "--",
        allowed_returncodes=frozenset({0, 1}),
    )
    if staged.returncode != 0:
        raise GitStateError("lane worktree has staged but uncommitted files")

    owned = git.get("harness_owned_paths")
    if not isinstance(owned, list) or not all(isinstance(item, str) for item in owned):
        raise GitStateError("lane Git identity lacks its harness-owned path inventory")
    exact_owned = {item for item in owned if item != ".agent-workspace/**"}
    untracked_raw = _git(
        worktree, "ls-files", "--others", "--exclude-standard", "-z"
    ).stdout
    untracked = [item for item in untracked_raw.split("\0") if item]
    unexpected = sorted(
        item
        for item in untracked
        if not item.startswith(".agent-workspace/") and item not in exact_owned
    )
    if unexpected:
        raise GitStateError(
            "lane worktree has unexpected untracked files: " + ", ".join(unexpected[:10])
        )
    return {
        "branch": branch,
        "commit": commit,
        "common_dir": str(common_dir.resolve()),
        "origin_tip": str(git["origin_tip"]),
        "clean": True,
    }


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
        "invocation_hash",
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
    if outcome not in REVIEW_OUTCOMES:
        return False
    if acceptance.get("approval") == "ACCEPTED" and outcome != "PASS":
        reason = acceptance.get("force_accept_reason")
        if not isinstance(reason, str) or not reason.strip():
            return False
    return True


def validate_lane_acceptance_chain(
    review: dict[str, Any],
    acceptance: dict[str, Any],
    lane: dict[str, Any],
) -> bool:
    """Validate a review pair against the lane's current authoritative run."""

    lane_id = lane.get("lane_id")
    run_id = lane.get("run_id")
    if not isinstance(lane_id, str) or not isinstance(run_id, str):
        return False
    if not validate_acceptance_chain(
        review, acceptance, lane_id=lane_id, run_id=run_id
    ):
        return False
    if review.get("task_card_hash") != lane.get("task_card_hash"):
        return False
    if review.get("result_id") != run_id:
        return False
    if review.get("invocation_hash") != lane.get("invocation_hash"):
        return False
    git = lane.get("git")
    validation = lane.get("result_validation")
    if not isinstance(git, dict) or not isinstance(validation, dict):
        return False
    if "current_tip" in git or not isinstance(git.get("bootstrap_tip"), str):
        return False
    expected_validation = {
        "run_id": run_id,
        "result_hash": review.get("result_hash"),
        "invocation_hash": lane.get("invocation_hash"),
        "branch": git.get("branch"),
        "commit": review.get("commit"),
    }
    if any(
        validation.get(field) != value
        for field, value in expected_validation.items()
    ) or validation.get("clean") is not True:
        return False
    advancement = lane.get("acceptance_advancement")
    if advancement is not None and advancement != acceptance:
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
    if completed.returncode != 0 or len(completed.stdout.strip()) != 40:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ReviewError(
            COMPLETION_REVIEW_STALE_SOURCE,
            f"cannot resolve the worktree's current commit: {detail or completed.returncode}",
        )
    return completed.stdout.strip()


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
    invocation_path = worktree / ".agent-workspace" / "invocation.json"
    try:
        invocation = read_json(invocation_path)
        require_schema(invocation, INVOCATION_SCHEMA, invocation_path)
        validate_invocation_binding(lane, invocation)
    except (OSError, ValueError, GitStateError) as exc:
        raise ReviewError(COMPLETION_REVIEW_STALE_SOURCE, str(exc)) from exc
    if lane.get("task_card_hash") != content_hash(task_card):
        raise ReviewError(
            COMPLETION_REVIEW_STALE_SOURCE,
            "task card copy no longer matches the bootstrapped task card",
        )
    try:
        git_state = validate_merge_ready_git(lane)
    except GitStateError as exc:
        raise ReviewError(COMPLETION_REVIEW_STALE_SOURCE, str(exc)) from exc
    commit = _worktree_commit(worktree, task_card)
    if commit != git_state["commit"]:
        raise ReviewError(
            COMPLETION_REVIEW_STALE_SOURCE,
            "worktree HEAD changed during completion review",
        )
    task_card_id = str(task_card.get("card_id") or task_card.get("id") or "")
    if not task_card_id:
        task_card_id = content_hash(task_card)
    task_card_hash = content_hash(task_card)
    result_id = str(lane.get("run_id") or "")
    result_hash = content_hash(result)
    validation = lane.get("result_validation")
    expected_validation = {
        "run_id": lane["run_id"],
        "result_hash": result_hash,
        "branch": git_state["branch"],
        "commit": commit,
    }
    if not isinstance(validation, dict) or any(
        validation.get(field) != value
        for field, value in expected_validation.items()
    ) or validation.get("clean") is not True:
        raise ReviewError(
            COMPLETION_REVIEW_STALE_SOURCE,
            "result is not bound to the lane's current clean branch tip",
        )
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
        "invocation_hash": lane["invocation_hash"],
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
        "invocation_hash": lane["invocation_hash"],
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
                close_event(
                    rt,
                    event["event_id"],
                    "COMPLETE",
                    summary=(
                        f"completion review recorded: {review_outcome} / {approval}"
                    ),
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
