"""``resume-lane``: re-run a stopped, unaccepted lane in its same worktree and
provider session with a fresh ``run_id``.

Resume is a short program, not an agent.  It re-does work: it clears the prior
run's obsolete current state, writes a fresh invocation for the new run, and
leaves the lane ready for ``lane launch``.  It never reconstructs a session,
PID, worktree, or amendment/hash record.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from .bootstrap import (
    _build_invocation,
    _write_result_template,
    _write_worker_prompt,
)
from .config import find_harness_root, load_config
from .core import content_hash, iso_utc, new_id, read_json, require_schema
from .epochs import lane_record_dir, read_active_lanes, write_active_lanes
from .lanes import find_active_lane, update_lane
from .records import RecordLock, atomic_write_bytes, atomic_write_json, remove_record
from .manager_queue import acknowledge_event, close_event, read_manager_queue
from .review import (
    lane_integrity_contract_error,
    validate_invocation_binding,
    validate_lane_acceptance_chain,
)

TASK_CARD_SCHEMA = "project-task-card/v1"
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
PENDING_RESUME_SCHEMA = "pending-resume/v1"

_RESUMABLE_LIFECYCLES = frozenset(
    {"review_pending", "result_invalid", "blocked", "abandoned", "resuming"}
)


def _consume_resume_signal(rt: Path, lane_id: str, prior_run_id: str) -> bool:
    """Close the rejected-review signal after the fresh run is prepared."""
    try:
        queue = read_manager_queue(rt)
    except Exception:
        return False
    for event in queue.get("events", []):
        if not (
            event.get("type") == "LANE_RESUME_REQUIRED"
            and event.get("lane_id") == lane_id
            and event.get("run_id") == prior_run_id
            and event.get("state") in {"PENDING", "ACKNOWLEDGED"}
        ):
            continue
        event_id = str(event["event_id"])
        try:
            if event.get("state") == "PENDING":
                acknowledge_event(rt, event_id)
            close_event(
                rt,
                event_id,
                "COMPLETE",
                summary=f"lane {lane_id} resumed under a fresh run",
            )
        except Exception:
            return False
    return True


def _resume_success(
    rt: Path,
    epoch_id: str,
    lane: dict[str, Any],
    *,
    already_committed: bool = False,
) -> dict[str, Any]:
    lane_id = str(lane["lane_id"])
    summary = (
        f"lane {lane_id} resume was already committed; launch it to start the provider"
        if already_committed
        else f"lane {lane_id} resumed under a fresh run_id; launch it to start the provider"
    )
    return {
        "ok": True,
        "code": "RESUME_OK",
        "summary": summary,
        "evidence_paths": [
            str(Path(lane["worktree_path"]) / ".agent-workspace" / "invocation.json"),
            str(lane_record_dir(rt, epoch_id, lane_id) / "lane.json"),
        ],
        "next_action": "run `lane launch --lane-id <id>` to start the resumed run",
    }


def _read_task_card(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"resume task card missing: {path}")
    record = read_json(path)
    require_schema(record, TASK_CARD_SCHEMA, path)
    task = record.get("task")
    if not isinstance(task, str) or not task.strip():
        raise ValueError("resume task card has no task text")
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
    return (
        acceptance.get("approval") == "ACCEPTED"
        and validate_lane_acceptance_chain(review, acceptance, lane)
    )


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


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _pending_with_hash(fields: dict[str, Any]) -> dict[str, Any]:
    pending = dict(fields)
    pending["content_hash"] = content_hash(pending)
    return pending


def _validate_pending_resume(
    pending: Any,
    *,
    lane: dict[str, Any],
    folder: Path,
    task_card_hash: str,
    rationale: str | None,
    managed: bool,
) -> dict[str, Any]:
    if not isinstance(pending, dict) or pending.get("schema") != PENDING_RESUME_SCHEMA:
        raise ValueError("pending resume transaction is invalid; bootstrap a fresh lane")
    if pending.get("content_hash") != content_hash(pending):
        raise ValueError("pending resume transaction hash mismatch; bootstrap a fresh lane")
    run_id = pending.get("new_run_id")
    expected_stage = folder / "pending-resume" / str(run_id)
    normalized_rationale = rationale.strip() if rationale and rationale.strip() else None
    if (
        not isinstance(run_id, str)
        or not run_id
        or pending.get("prior_run_id") != lane.get("run_id")
        or pending.get("prior_invocation_hash") != lane.get("invocation_hash")
        or pending.get("task_card_hash") != task_card_hash
        or pending.get("rationale") != normalized_rationale
        or pending.get("managed") is not managed
        or Path(str(pending.get("stage_path") or "")).resolve()
        != expected_stage.resolve()
        or pending.get("phase") not in {"planned", "staged"}
    ):
        raise ValueError(
            "resume retry does not match the durable pending transaction; "
            "retry the exact same resume request"
        )
    return pending


def _stage_resume(
    stage: Path,
    *,
    worktree: Path,
    rt: Path,
    lane: dict[str, Any],
    run_id: str,
    task_card: dict[str, Any],
    rationale: str | None,
    managed: bool,
    exclusive_resources: list[str],
) -> tuple[dict[str, Any], dict[str, str]]:
    """Materialize the next run outside the worktree and hash every artifact."""

    stage.mkdir(parents=True, exist_ok=True)
    (stage / ".agent-workspace").mkdir(parents=True, exist_ok=True)
    _write_worker_prompt(stage, task_card, managed=managed, rationale=rationale)
    _write_result_template(stage, lane["lane_id"], run_id)
    invocation = _build_invocation(
        worktree,
        lane_id=lane["lane_id"],
        run_id=run_id,
        provider_id=lane["provider"]["id"],
        model=lane["provider"]["model"],
        exclusive_resources=exclusive_resources,
        git_identity=lane["git"],
    )
    atomic_write_json(stage / ".agent-workspace" / "invocation.json", invocation)
    _rewrite_overlay_receipt(stage, lane, run_id, managed=managed)
    atomic_write_json(stage / ".agent-workspace" / "task-card.json", task_card)
    if managed:
        _reset_worker_inbox(stage, lane["lane_id"], run_id)
        binding = {
            "schema": "harness-hook-binding/v1",
            "role": "worker",
            "lane_id": lane["lane_id"],
            "run_id": run_id,
            "manager_queue_path": str(rt / "manager" / "QUEUE.json"),
            "inbox_path": str(worktree / ".agent-workspace" / "QUEUE.json"),
            "outbox_dir": str(worktree / ".agent-workspace" / "manager-notifications"),
            "result_path": str(worktree / "RESULT.json"),
            "result_stop_check": str(
                worktree / ".agent-workspace" / "result-stop-check.py"
            ),
        }
        atomic_write_json(
            stage / ".agent-workspace" / "harness-hook-binding.json", binding
        )
    files: dict[str, str] = {}
    for path in sorted(item for item in stage.rglob("*") if item.is_file()):
        files[path.relative_to(stage).as_posix()] = _sha256(path)
    return invocation, files


def _validate_staged_resume(stage: Path, files: Any) -> dict[str, Path]:
    if not isinstance(files, dict) or not files:
        raise ValueError("pending resume has no staged artifact inventory")
    actual: dict[str, Path] = {}
    for name, expected_hash in files.items():
        relative = Path(str(name))
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise ValueError("pending resume contains an unsafe staged path")
        path = stage.joinpath(*relative.parts)
        if not path.is_file() or path.is_symlink() or _sha256(path) != expected_hash:
            raise ValueError(f"pending resume staged artifact changed: {name}")
        actual[relative.as_posix()] = path
    disk_files = {
        item.relative_to(stage).as_posix()
        for item in stage.rglob("*")
        if item.is_file()
    }
    if disk_files != set(actual):
        raise ValueError("pending resume staged artifact inventory is not exact")
    return actual


def _publish_staged_resume(worktree: Path, staged: dict[str, Path]) -> None:
    for relative, source in staged.items():
        destination = worktree.joinpath(*Path(relative).parts)
        atomic_write_bytes(destination, source.read_bytes())


def _publish_resume_active_index(
    rt: Path,
    epoch_id: str,
    lane: dict[str, Any],
    pending: dict[str, Any],
) -> None:
    """Advance the exact active entry before final lane publication.

    ``find_active_lane`` recognizes only this explicit pending transition, so
    an interruption between the two durable records can be retried safely.
    """

    lane_id = str(lane["lane_id"])
    entries = read_active_lanes(rt, epoch_id)
    matches = [entry for entry in entries if entry.get("lane_id") == lane_id]
    expected_path = lane_record_dir(rt, epoch_id, lane_id) / "lane.json"
    if (
        len(matches) != 1
        or Path(str(matches[0].get("lane_record_path") or "")).resolve()
        != expected_path.resolve()
        or matches[0].get("run_id")
        not in {pending.get("prior_run_id"), pending.get("new_run_id")}
    ):
        raise ValueError("active lane index changed during resume publication")
    if matches[0].get("run_id") == pending.get("new_run_id"):
        return
    replacement = [
        {**entry, "run_id": pending["new_run_id"]}
        if entry is matches[0]
        else entry
        for entry in entries
    ]
    write_active_lanes(rt, epoch_id, replacement)


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
        legacy = lane_integrity_contract_error(lane)
        if legacy is not None:
            return {
                "ok": False,
                "code": RESUME_LANE_WRITE_FAILED,
                "summary": legacy,
                "evidence_paths": [],
                "next_action": "bootstrap a fresh lane in a fresh epoch",
            }
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

        managed = config.profile == "managed"
        task_hash = content_hash(task_card)
        if lifecycle == "running":
            committed_resume = (
                lane.get("launch_pending") is True
                and lane.get("pending_resume") is None
                and isinstance(lane.get("resume_from_run_id"), str)
                and bool(lane.get("resume_from_run_id"))
                and lane.get("task_card_hash") == task_hash
            )
            if committed_resume:
                invocation_path = worktree / ".agent-workspace" / "invocation.json"
                committed_invocation = read_json(invocation_path)
                require_schema(
                    committed_invocation, INVOCATION_SCHEMA, invocation_path
                )
                validate_invocation_binding(lane, committed_invocation)
                if managed:
                    try:
                        _consume_resume_signal(
                            rt, lane_id, str(lane["resume_from_run_id"])
                        )
                    except Exception:
                        # The lane and active index are already authoritative.
                        # Queue cleanup is retryable bookkeeping, never a
                        # reason to mint another run or report failure.
                        pass
                return _resume_success(
                    rt, epoch_id, lane, already_committed=True
                )
            if (
                lane.get("launch_pending") is True
                and isinstance(lane.get("resume_from_run_id"), str)
                and bool(lane.get("resume_from_run_id"))
            ):
                return {
                    "ok": False,
                    "code": LANE_RUNNING,
                    "summary": f"lane {lane_id} already has a committed resume prepared to launch",
                    "evidence_paths": [],
                    "next_action": "launch the current run; do not replace its resume task card",
                }

        # The previous invocation is immutable lane authority, including the
        # exclusive-resource set carried into a same-session resume.  A
        # durable pending transaction permits an exact retry after any crash
        # between staging and the final lane-record publication.
        invocation_path = worktree / ".agent-workspace" / "invocation.json"
        prior_run_id = str(lane.get("run_id") or "")
        folder = lane_record_dir(rt, epoch_id, lane_id)
        normalized_rationale = rationale.strip() if rationale and rationale.strip() else None
        with RecordLock(folder / "resume.transaction"):
            pending_value = lane.get("pending_resume")
            if pending_value is None:
                prior_invocation = read_json(invocation_path)
                require_schema(prior_invocation, INVOCATION_SCHEMA, invocation_path)
                validate_invocation_binding(lane, prior_invocation)
                exclusive_resources = [
                    str(item)
                    for item in prior_invocation.get("exclusive_resources", [])
                ]
                run_id = new_id()
                stage = folder / "pending-resume" / run_id
                pending = _pending_with_hash(
                    {
                        "schema": PENDING_RESUME_SCHEMA,
                        "phase": "planned",
                        "prior_run_id": prior_run_id,
                        "new_run_id": run_id,
                        "prior_invocation_hash": lane["invocation_hash"],
                        "task_card_hash": task_hash,
                        "rationale": normalized_rationale,
                        "managed": managed,
                        "stage_path": str(stage),
                        "exclusive_resources": exclusive_resources,
                        "created_at": iso_utc(),
                    }
                )
                lane = update_lane(
                    rt,
                    epoch_id,
                    lane_id,
                    lambda current, value=pending: {
                        **current,
                        "lifecycle": "resuming",
                        "resume_started_at": iso_utc(),
                        "resume_from_run_id": prior_run_id,
                        "pending_resume": value,
                    },
                )
            else:
                pending = _validate_pending_resume(
                    pending_value,
                    lane=lane,
                    folder=folder,
                    task_card_hash=task_hash,
                    rationale=rationale,
                    managed=managed,
                )
                run_id = str(pending["new_run_id"])
                stage = Path(str(pending["stage_path"]))
                exclusive_resources = [
                    str(item) for item in pending.get("exclusive_resources", [])
                ]
                # The live invocation can be either the old record or the
                # staged new record after a publication interruption.
                live_invocation = read_json(invocation_path)
                require_schema(live_invocation, INVOCATION_SCHEMA, invocation_path)
                live_hash = live_invocation.get("content_hash")
                if live_hash not in {
                    pending.get("prior_invocation_hash"),
                    pending.get("new_invocation_hash"),
                } or live_hash != content_hash(live_invocation):
                    raise ValueError(
                        "live invocation does not match the pending resume transaction"
                    )

            if pending["phase"] == "planned":
                invocation, files = _stage_resume(
                    stage,
                    worktree=worktree,
                    rt=rt,
                    lane=lane,
                    run_id=run_id,
                    task_card=task_card,
                    rationale=normalized_rationale,
                    managed=managed,
                    exclusive_resources=exclusive_resources,
                )
                pending = _pending_with_hash(
                    {
                        **pending,
                        "phase": "staged",
                        "new_invocation_hash": invocation["content_hash"],
                        "files": files,
                        "staged_at": iso_utc(),
                    }
                )
                lane = update_lane(
                    rt,
                    epoch_id,
                    lane_id,
                    lambda current, value=pending: {
                        **current,
                        "pending_resume": value,
                    },
                )
            staged = _validate_staged_resume(stage, pending.get("files"))
            invocation = read_json(staged[".agent-workspace/invocation.json"])
            if (
                invocation.get("content_hash") != pending.get("new_invocation_hash")
                or invocation.get("content_hash") != content_hash(invocation)
                or invocation.get("lane_id") != lane_id
                or invocation.get("run_id") != run_id
                or invocation.get("provider") != lane.get("provider")
                or invocation.get("git") != lane.get("git")
            ):
                raise ValueError("staged resume invocation failed identity validation")

            _clear_prior_run(rt, epoch_id, lane)
            _publish_staged_resume(worktree, staged)
            _publish_resume_active_index(rt, epoch_id, lane, pending)

            def finalize(current: dict[str, Any]) -> dict[str, Any]:
                current_pending = current.get("pending_resume")
                if (
                    current.get("run_id") != prior_run_id
                    or not isinstance(current_pending, dict)
                    or current_pending.get("content_hash") != pending["content_hash"]
                ):
                    raise ValueError("pending resume lane authority changed before publication")
                return {
                    **current,
                    "run_id": run_id,
                    "invocation_hash": invocation["content_hash"],
                    "lifecycle": "running",
                    "process": {},
                    "launch_pending": True,
                    "acceptance_advancement": None,
                    "task_card_hash": task_hash,
                    "result_validation": None,
                    "last_reported_actionable_status": None,
                    "resume_from_run_id": prior_run_id,
                    "pending_resume": None,
                }

            update_lane(rt, epoch_id, lane_id, finalize)
        if managed:
            try:
                _consume_resume_signal(rt, lane_id, prior_run_id)
            except Exception:
                # Resume is already durably committed.  A manager-queue
                # acknowledgement can be retried without changing run state.
                pass
    except Exception as exc:
        return {
            "ok": False,
            "code": RESUME_LANE_WRITE_FAILED,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "resolve the error and retry resume",
        }

    return _resume_success(rt, epoch_id, {**lane, "worktree_path": str(worktree)})
