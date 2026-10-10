"""``resume-lane``: re-run a stopped, unaccepted lane in its same worktree
with a fresh ``run_id``. A proven pre-provider failure has no session to reuse.

Resume is a short program, not an agent.  It re-does work: it clears the prior
run's obsolete current state, writes a fresh invocation for the new run, and
leaves the lane ready for ``lane launch``.  It never reconstructs a session,
PID, worktree, or amendment/hash record.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

from . import memory_handoff, processes, terminal_evidence
from .bootstrap import (
    _write_invocation,
    _write_result_template,
    _write_worker_binding,
    _write_worker_prompt,
)
from .config import find_harness_root, load_config
from .core import content_hash, iso_utc, new_id, read_json, require_schema
from .epochs import lane_record_dir
from .lanes import find_active_lane, read_lane, update_lane
from .records import RecordLock, atomic_write_json, remove_record
from .manager_queue import acknowledge_event, close_event, read_manager_queue
from .setup import COMPOSED_PAYLOADS
from .retry_policy import MAX_LANE_PROVIDER_INVOCATIONS, WORKER_RETRY_LIMIT, completed_provider_invocations

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
RESUME_PLAN_PENDING = "RESUME_PLAN_PENDING"

_RESUMABLE_LIFECYCLES = frozenset(
    {"review_pending", "result_invalid", "blocked", "abandoned", "resuming"}
)


def _live_controller(lane: dict[str, Any]) -> bool:
    """Check both the lane process and the controller's exact attestation."""
    identities = [lane.get("process") or {}]
    status_path = Path(lane["controller_status_path"])
    if status_path.is_file():
        try:
            status = read_json(status_path)
            require_schema(status, "controller-status/v1", status_path)
        except (OSError, ValueError) as exc:
            raise memory_handoff.MemoryHandoffError(
                "controller status is unreadable; resume cannot prove prior ownership"
            ) from exc
        if status.get("lane_id") == lane["lane_id"] and status.get("run_id") == lane["run_id"]:
            identities.append(status.get("controller_identity") or {})
    return any(
        isinstance(identity, dict)
        and processes.identity_matches(identity.get("pid"), identity.get("creation_time"))
        for identity in identities
    )


def _recheck_resume_owner(
    rt: Path, epoch_id: str, lane: dict[str, Any], *, lifecycle: str
) -> dict[str, Any]:
    current = read_lane(rt, epoch_id, lane["lane_id"])
    if any(
        current.get(field) != lane.get(field)
        for field in ("lane_id", "run_id", "worktree_path", "provider")
    ) or current.get("lifecycle") != lifecycle:
        raise memory_handoff.MemoryHandoffError(
            "lane run or status changed before resume could replace it"
        )
    if _live_controller(current):
        raise memory_handoff.MemoryHandoffError(
            "an exact native controller is still live; resume cannot replace its run"
        )
    return current


def _exact_controller_exited(identity: dict[str, Any]) -> bool:
    pid = identity.get("pid")
    creation = identity.get("creation_time")
    if not isinstance(pid, int) or not isinstance(creation, str) or not creation:
        return False
    if not processes.process_alive(pid):
        return True
    current = processes.process_identity(pid)
    return current is not None and current["creation_time"] != creation


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
    require_schema(record, TASK_CARD_SCHEMA, path)
    task = record.get("task")
    if not isinstance(task, str) or not task.strip():
        raise ValueError("resume task card has no task text")
    return record


def _has_valid_acceptance_chain(
    rt: Path, epoch_id: str, lane: dict[str, Any]
) -> bool:
    """Return whether a complete, linked ACCEPTED chain exists for this run."""
    folder = terminal_evidence.publication_dir(rt, epoch_id, lane)
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
    if acceptance.get("approval") != "ACCEPTED":
        return False
    if lane.get("memory_plan_state") == "execution_accepted":
        try:
            terminal = terminal_evidence.read_terminal_evidence(
                rt, epoch_id, lane["lane_id"], run_id=lane["run_id"],
            )
        except terminal_evidence.TerminalEvidenceError:
            return False
        return terminal is not None and terminal["review"] == review and terminal["acceptance"] == acceptance
    return True


def _clear_prior_run(rt: Path, epoch_id: str, lane: dict[str, Any]) -> None:
    """Remove the prior run's obsolete current state (best-effort, honest)."""
    worktree = Path(lane["worktree_path"])
    remove_record(worktree / "RESULT.json")
    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    if lane.get("memory_plan_state") != "execution_accepted":
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
        receipt["provider_payload"] = f"{COMPOSED_PAYLOADS.as_posix()}/{lane['provider']['id']}"
    atomic_write_json(worktree / ".agent-workspace" / "overlay-receipt.json", receipt)


def run_resume(
    *,
    lane_id: str,
    resume_task_card: str,
    rationale: str | None = None,
    search_stores: Sequence[Any] = (),
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

    dispatch_lock: RecordLock | None = None
    try:
        if _has_valid_acceptance_chain(rt, epoch_id, lane):
            return {
                "ok": False,
                "code": ALREADY_ACCEPTED,
                "summary": f"lane {lane_id} already has a valid ACCEPTED chain; it is not re-resumed",
                "evidence_paths": [
                    str(terminal_evidence.publication_dir(rt, epoch_id, lane) / "ORCHESTRATOR_ACCEPTANCE.json")
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
        if _live_controller(lane):
            return {
                "ok": False,
                "code": LANE_RUNNING,
                "summary": f"lane {lane_id} is still active; it is not resumed",
                "evidence_paths": [],
                "next_action": "wait for the lane to stop, or force-stop it first",
            }
        try:
            prior_invocations = completed_provider_invocations(lane)
        except (OSError, ValueError, TypeError) as exc:
            return {
                "ok": False, "code": WORKER_RETRY_LIMIT, "summary": str(exc),
                "evidence_paths": [str(lane.get("attempts_path") or "")],
                "next_action": "bootstrap a fresh lane and native session; preserve prior invocation evidence",
            }
        if prior_invocations >= MAX_LANE_PROVIDER_INVOCATIONS:
            return {
                "ok": False, "code": WORKER_RETRY_LIMIT,
                "summary": f"lane {lane_id} used its initial assignment plus one retry (including automatic result corrections)",
                "evidence_paths": [str(lane.get("attempts_path") or "")],
                "next_action": "settle the previous lane and bootstrap a fresh lane and native session",
            }
        if lifecycle not in _RESUMABLE_LIFECYCLES and lifecycle not in ("running", "prepared"):
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

        try:
            task_card = _read_task_card(Path(resume_task_card))
            memory_handoff.validate_task_card(task_card)
        except (OSError, ValueError, memory_handoff.MemoryHandoffError) as exc:
            return {
                "ok": False,
                "code": INVALID_RESUME_TASK_CARD,
                "summary": str(exc),
                "evidence_paths": [],
                "next_action": "supply a valid project-task-card/v1 resume card",
            }
        declared_environment = task_card.get("worker_environment")
        if (
            lane.get("worker_environment") == "scrubbed"
            and declared_environment != "scrubbed"
        ):
            raise memory_handoff.MemoryHandoffError(
                "resume task card cannot weaken the lane's durable scrubbed worker environment"
            )

        prior_run_id = str(lane.get("run_id") or "")
        recorded_memory_state = lane.get("memory_plan_state")
        current_memory_state = memory_handoff.enabled_handoff_state(task_card)
        if recorded_memory_state and current_memory_state != recorded_memory_state:
            raise memory_handoff.MemoryHandoffError(
                "resume task card changed the lane's accepted memory plan state"
            )
        if current_memory_state == "execution_accepted":
            # Launch uses this same lock for the final intent/spawn transition.
            # Keep it through the new run record so the old run cannot dispatch
            # after resume has checked its operation.
            dispatch_lock = RecordLock(memory_handoff.memory_paths(worktree)[1])
            dispatch_lock.__enter__()
            lane = _recheck_resume_owner(rt, epoch_id, lane, lifecycle=lifecycle)
        memory_handoff.validate_resume_handoff(
            task_card=task_card,
            lane_id=lane_id,
            prior_run_id=prior_run_id,
            worktree_path=worktree,
            base_commit=str(task_card.get("base_commit") or "HEAD"),
        )
        supersession = None
        required_sources: frozenset[tuple[str, str, str]] = frozenset()
        if current_memory_state == "execution_accepted":
            prior_envelope = memory_handoff.load_envelope(worktree)
            assert prior_envelope is not None
            prior_context = memory_handoff.load_final_context(
                worktree_path=worktree, envelope=prior_envelope,
            )
            required_sources = memory_handoff.required_resume_sources(prior_context)
            if not search_stores:
                if prior_context["optional_content"]:
                    raise memory_handoff.MemoryHandoffError(
                        "selected optional content requires live search stores for resume"
                    )
            prior_operation = memory_handoff.get_dispatch_operation(
                worktree_path=worktree, envelope=prior_envelope
            )
            if prior_operation is not None and prior_operation["status"] in ("pending", "ambiguous"):
                from . import launch

                prior_context = memory_handoff.load_final_context(
                    worktree_path=worktree, envelope=prior_envelope
                )
                try:
                    native = launch._lookup_native_invocation(
                        rt, epoch_id, lane,
                        expected_binding=memory_handoff.dispatch_binding(
                            envelope=prior_envelope, context=prior_context
                        ),
                    )
                except launch.LaunchError as exc:
                    raise memory_handoff.MemoryHandoffError(
                        "prior dispatch has conflicting native ownership; reconcile before resume"
                    ) from exc
                if native is None:
                    raise memory_handoff.MemoryHandoffError(
                        "prior dispatch intent has unresolved native ownership; resume cannot replace its run"
                    )
                memory_handoff.record_observed_invocation(
                    worktree_path=worktree,
                    envelope=prior_envelope,
                    observed_invocation=memory_handoff.native_observation(
                        envelope=prior_envelope,
                        context=prior_context,
                        controller_identity=native,
                    ),
                )
                raise memory_handoff.MemoryHandoffError(
                    "prior dispatch was reconciled; review its native lifecycle before resume"
                )
            if prior_operation is not None and prior_operation["status"] == "delivered":
                retained = terminal_evidence.read_terminal_evidence(
                    rt, epoch_id, lane_id, run_id=prior_run_id,
                )
                if retained is None or retained["acceptance"]["approval"] != "REJECTED":
                    raise memory_handoff.MemoryHandoffError(
                        "delivered native run has no exact ROOT-rejected review for correction"
                    )
                authorization = terminal_evidence.record_domain_review(
                    rt, epoch_id, lane, retained,
                )
                assert authorization is not None
                if (authorization["operation_id"] != prior_operation["operation_id"]
                        or authorization["decision_id"] != prior_envelope["decision_id"]):
                    raise memory_handoff.MemoryHandoffError(
                        "rejected native attempt differs from the prior dispatch"
                    )
                supersession = {
                    field: authorization[field] for field in (
                        "rejected_attempt_id", "run_id", "decision_id", "operation_id", "evidence_digest",
                    )
                }
            elif lane.get("native_supersession") is not None:
                if prior_operation is not None and prior_operation["status"] != "failed_pre_spawn":
                    raise memory_handoff.MemoryHandoffError(
                        "native supersession belongs to an unresolved prior dispatch"
                    )
                memory_handoff.supersession_id_for_launch(
                    worktree_path=worktree, lane=lane, envelope=prior_envelope,
                )
                supersession = dict(lane["native_supersession"])
            if not isinstance(session_id, str) or not session_id:
                from . import launch

                observed = (prior_operation or {}).get("observed_invocation")
                if prior_operation is None or prior_operation["status"] != "delivered" or not isinstance(observed, dict):
                    raise memory_handoff.MemoryHandoffError(
                        "no saved provider session or delivered no-provider failure permits a fresh run"
                    )
                context = memory_handoff.load_final_context(
                    worktree_path=worktree, envelope=prior_envelope
                )
                expected = memory_handoff.native_observation(
                    envelope=prior_envelope,
                    context=context,
                    controller_identity=observed,
                )
                if observed != expected:
                    raise memory_handoff.MemoryHandoffError(
                        "prior native controller observation conflicts with this run"
                    )
                current_process = lane.get("process") or {}
                if current_process and current_process != {
                    "pid": observed["pid"], "creation_time": observed["creation_time"]
                }:
                    raise memory_handoff.MemoryHandoffError(
                        "a different native controller owns the prior run"
                    )
                outcome = launch._delivered_provider_outcome(
                    lane, observed,
                    memory_handoff.dispatch_binding(envelope=prior_envelope, context=context),
                )
                if (
                    outcome is None or outcome[0] == "LAUNCH_OK"
                    or not _exact_controller_exited(observed)
                ):
                    raise memory_handoff.MemoryHandoffError(
                        "no-provider cleanup and exact controller exit are not proven; resume cannot replace the run"
                    )
        elif not isinstance(session_id, str) or not session_id:
            return {
                "ok": False,
                "code": NO_SAVED_SESSION_ID,
                "summary": f"lane {lane_id} has no saved provider session to resume",
                "evidence_paths": [],
                "next_action": "bootstrap a fresh lane (resume requires a native session)",
            }
        if lifecycle == "prepared" and (not current_memory_state or session_id):
            raise memory_handoff.MemoryHandoffError(
                "prepared lane has no proven pre-provider failure to resume"
            )
        run_id = new_id()
        managed = config.profile == "managed"
        # Resume is the same logical decision as the original bootstrap: it
        # runs one bounded preparation for the exact current-plan state and
        # only an exact ROOT-accepted plan produces a dispatchable envelope.
        # An explicitly absent plan keeps its fresh ROOT-planning disposition
        # and a pending candidate keeps its exact review identity, so neither
        # one creates, prepares, or launches an execution worker here.
        memory = memory_handoff.prepare_lane_memory(
            task_card=task_card,
            lane_id=lane_id,
            run_id=run_id,
            worktree_path=worktree,
            base_commit=str(task_card.get("base_commit") or "HEAD"),
            search_stores=search_stores,
            required_sources=required_sources,
        )
        if memory.pending_plan:
            update_lane(
                rt,
                epoch_id,
                lane_id,
                lambda current, value=memory: {
                    **current,
                    "memory_plan_state": value.state,
                    "dispatchable": False,
                    "memory_pending_reason": memory_handoff.plan_state_summary(
                        str(value.state)
                    ),
                },
            )
            return {
                "ok": False,
                "code": RESUME_PLAN_PENDING,
                "summary": (
                    f"lane {lane_id} has no ROOT-accepted execution plan: "
                    + memory_handoff.plan_state_summary(str(memory.state))
                ),
                "evidence_paths": [
                    str(lane_record_dir(rt, epoch_id, lane_id) / "lane.json"),
                    str(memory_handoff.memory_paths(worktree)[0]),
                ],
                "next_action": (
                    "ROOT must review and accept the exact current plan, then "
                    "resume the lane again; no worker was created or launched"
                ),
            }
        if dispatch_lock is not None:
            lane = _recheck_resume_owner(rt, epoch_id, lane, lifecycle=lifecycle)
        def mark_resuming(current: dict[str, Any]) -> dict[str, Any]:
            if current.get("run_id") != prior_run_id or current.get("lifecycle") != lifecycle:
                raise memory_handoff.MemoryHandoffError(
                    "lane run or status changed before resume mutation"
                )
            return {
                **{key: value for key, value in current.items() if key != "native_supersession"},
                "lifecycle": "resuming",
                "resume_started_at": iso_utc(),
                "resume_from_run_id": prior_run_id,
                **({"native_supersession": supersession} if supersession is not None else {}),
            }

        update_lane(
            rt,
            epoch_id,
            lane_id,
            mark_resuming,
        )
        if dispatch_lock is not None:
            _recheck_resume_owner(rt, epoch_id, lane, lifecycle="resuming")
        _clear_prior_run(rt, epoch_id, lane)
        if managed:
            _reset_worker_inbox(worktree, lane_id, run_id)
            from .public_checks import install_route
            install_route(harness_root, config.root_workspace, worktree, lane_id, run_id)
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
            memory_envelope=memory.envelope,
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

        def replace_run(current: dict[str, Any]) -> dict[str, Any]:
            if current.get("run_id") != prior_run_id or current.get("lifecycle") != "resuming":
                raise memory_handoff.MemoryHandoffError(
                    "lane run or status changed before fresh run ownership"
                )
            if (
                current.get("worker_environment") == "scrubbed"
                and declared_environment != "scrubbed"
            ):
                raise memory_handoff.MemoryHandoffError(
                    "resume task card cannot weaken the lane's durable scrubbed worker environment"
                )
            return {
                **current,
                "run_id": run_id,
                "lifecycle": "running",
                "process": {},
                "launch_pending": True,
                "acceptance_advancement": None,
                "last_reported_actionable_status": None,
                "resume_from_run_id": prior_run_id,
                **(
                    {"worker_environment": declared_environment}
                    if declared_environment is not None
                    else {}
                ),
                **(
                    {
                        "memory_plan_state": memory.state,
                        "dispatchable": memory.dispatchable,
                    }
                    if memory.state is not None
                    else {}
                ),
            }

        update_lane(rt, epoch_id, lane_id, replace_run)
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
    finally:
        if dispatch_lock is not None:
            dispatch_lock.__exit__(None, None, None)

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
