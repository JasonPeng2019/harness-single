"""The lane controller: one long-lived supervisor process per lane.

The controller starts and cleans up the lane's provider process, captures the
transcript/stderr/last message, writes the worktree execution records,
validates the worker's result, holds the lane's exclusive lease(s), proves its
own process cleanup, and copies a valid ACCEPTED advancement into its status.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import processes
from .config import find_harness_root, load_config
from .core import content_hash, iso_utc, read_json, require_schema
from .epochs import lane_record_dir
from .lanes import find_active_lane, update_lane
from .leases import acquire_leases, release_leases
from .records import (
    RecordLock,
    append_jsonl,
    atomic_write_bytes,
    atomic_write_json,
    read_record,
)
from .review import validate_acceptance_chain
from .setup import read_runtime_state

CONTROLLER_STATUS_SCHEMA = "controller-status/v1"
CONTROLLER_EVENTS_SCHEMA = "controller-events/v1"
INVOCATION_SCHEMA = "controller-invocation/v1"
RESULT_SCHEMA = "result/v1"
COMPLETION_REVIEW_SCHEMA = "completion-review/v1"
ACCEPTANCE_SCHEMA = "orchestrator-acceptance/v1"

LAUNCH_INVOCATION_INVALID = "LAUNCH_INVOCATION_INVALID"
LAUNCH_BINDING_FAILED = "LAUNCH_BINDING_FAILED"
LAUNCH_LEASE_BUSY = "LAUNCH_LEASE_BUSY"
LAUNCH_PROVIDER_START_FAILED = "LAUNCH_PROVIDER_START_FAILED"
LAUNCH_CONTROLLER_START_FAILED = "LAUNCH_CONTROLLER_START_FAILED"

RESULT_OUTCOMES = frozenset({"PASS", "FAIL", "BLOCKED"})
ACCEPTANCE_POLL_SECONDS = 2.0


class ControllerError(RuntimeError):
    def __init__(
        self, code: str, message: str, *, no_provider_started: bool = False
    ) -> None:
        super().__init__(message)
        self.code = code
        self.no_provider_started = no_provider_started


@dataclass(frozen=True)
class ProviderExecution:
    """The provider exit code plus its controller-owned process boundary."""

    exit_code: int
    boundary: processes.ProcessBoundary
    session_id: str | None = None
    argv: tuple[str, ...] = ()
    non_retryable_failure: bool = False


def _load_binding(harness_root: Path, provider_id: str) -> Any:
    from .setup import _load_binding as load_module

    binding_path = (
        harness_root
        / "orchestrator_harness"
        / "provider_adapters"
        / provider_id
        / "launcher_binding.py"
    )
    if not binding_path.is_file():
        raise ControllerError(LAUNCH_BINDING_FAILED, f"binding missing: {binding_path}")
    module = load_module(binding_path)
    if getattr(module, "PROVIDER_ID", None) != provider_id:
        raise ControllerError(
            LAUNCH_BINDING_FAILED,
            f"binding PROVIDER_ID {getattr(module, 'PROVIDER_ID', None)!r} does not match {provider_id}",
        )
    return module


def _write_status(lane: dict[str, Any], fields: dict[str, Any]) -> None:
    path = Path(lane["controller_status_path"])
    record = {
        "schema": CONTROLLER_STATUS_SCHEMA,
        "lane_id": lane["lane_id"],
        "run_id": lane["run_id"],
        "controller_state": "running",
        "provider_state": {"state": "starting"},
        "result_state": "absent",
        "cleanup_proven": False,
        "recorded_status": None,
        "acceptance_advancement": None,
        "updated_at": iso_utc(),
    }
    with RecordLock(path):
        if path.is_file():
            try:
                existing = read_record(path, CONTROLLER_STATUS_SCHEMA)
            except (OSError, ValueError):
                existing = None
            if (
                isinstance(existing, dict)
                and existing.get("lane_id") == lane["lane_id"]
                and existing.get("run_id") == lane["run_id"]
            ):
                record.update(existing)
        record.update(fields)
        record["updated_at"] = iso_utc()
        atomic_write_json(path, record)


def _append_event(lane: dict[str, Any], event_type: str, detail: str) -> None:
    append_jsonl(
        Path(lane["controller_events_path"]),
        {"ts": iso_utc(), "run_id": lane["run_id"], "event_type": event_type, "detail": detail},
        header={"schema": CONTROLLER_EVENTS_SCHEMA},
    )


def _validate_result(lane: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
    """Return (result_state, result_record) for the lane's RESULT.json."""
    path = Path(lane["result_path"])
    if not path.is_file():
        return "invalid", None
    try:
        record = read_json(path)
        require_schema(record, RESULT_SCHEMA, path)
        if record.get("lane_id") != lane["lane_id"]:
            return "invalid", None
        if record.get("run_id") != lane["run_id"]:
            return "invalid", None
        if record.get("outcome") not in RESULT_OUTCOMES:
            return "invalid", None
        if not isinstance(record.get("summary"), str) or not record["summary"].strip():
            return "invalid", None
        if not isinstance(record.get("evidence"), list):
            return "invalid", None
        if not isinstance(record.get("completed_at"), str) or not record["completed_at"].strip():
            return "invalid", None
        if record.get("content_hash") != content_hash(record):
            return "invalid", None
        return "valid", record
    except (OSError, ValueError):
        return "invalid", None


def _attempt_paths(lane: dict[str, Any], attempt_number: int) -> dict[str, Path]:
    """Return isolated transcript/evidence paths for one provider attempt."""
    workspace = Path(lane["worktree_path"]) / ".agent-workspace"
    if attempt_number == 1:
        return {
            "transcript": Path(
                lane.get("transcript_path")
                or workspace / "provider-transcript.jsonl"
            ),
            "stderr": Path(
                lane.get("stderr_path") or workspace / "provider-stderr.txt"
            ),
        }
    folder = workspace / "attempts" / f"attempt-{attempt_number}"
    return {
        "transcript": folder / "provider-transcript.jsonl",
        "stderr": folder / "provider-stderr.txt",
    }


def _write_correction_prompt(
    lane: dict[str, Any], attempt_number: int, reason: str
) -> Path:
    """Persist one bounded native-continuation correction prompt."""
    workspace = Path(lane["worktree_path"]) / ".agent-workspace"
    path = workspace / "attempts" / f"correction-prompt-{attempt_number}.md"
    text = f"""The prior provider response did not produce a valid proposed result/v1 record ({reason}).

Continue this same lane and native provider session. Write a valid RESULT.json at the worktree root, not inside .agent-workspace, matching:

{{
  \"schema\": \"result/v1\",
  \"lane_id\": \"{lane['lane_id']}\",
  \"run_id\": \"{lane['run_id']}\",
  \"outcome\": \"PASS|FAIL|BLOCKED\",
  \"summary\": \"nonempty factual summary\",
  \"evidence\": [],
  \"completed_at\": \"ISO-8601 UTC timestamp\",
  \"content_hash\": \"sha256 of canonical JSON excluding content_hash\"
}}

The outcome must be exactly PASS, FAIL, or BLOCKED; summary must be nonempty; evidence must be a list; completed_at must be nonempty; and content_hash must be correct. Do not answer with prose alone. Do not change lane_id or run_id.
"""
    atomic_write_bytes(path, text.encode("utf-8"))
    return path


def _append_attempt(
    lane: dict[str, Any],
    *,
    attempt_number: int,
    argv: list[str] | None,
    session_id: str | None,
    prompt_path: Path,
    paths: dict[str, Path],
    exit_code: int | None,
    result_state: str,
    cleanup_proven: bool,
    validation_error: str | None = None,
) -> None:
    """Append immutable provider-attempt evidence."""
    append_jsonl(
        Path(
            lane.get("attempts_path")
            or Path(lane["worktree_path"]) / ".agent-workspace" / "controller.attempts.jsonl"
        ),
        {
            "attempt": attempt_number,
            "provider": lane.get("provider", {}),
            "argv": list(argv or []),
            "session_id": session_id,
            "prompt_path": str(prompt_path),
            "transcript_path": str(paths["transcript"]),
            "stderr_path": str(paths["stderr"]),
            "exit_code": exit_code,
            "result_state": result_state,
            "validation_error": validation_error,
            "cleanup_proven": cleanup_proven,
            "at": iso_utc(),
        },
        header={"schema": "controller-attempts/v1"},
    )


def _read_acceptance_chain(
    rt: Path, epoch_id: str, lane: dict[str, Any]
) -> dict[str, Any] | None:
    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    review_path = folder / "COMPLETION_REVIEW.json"
    acceptance_path = folder / "ORCHESTRATOR_ACCEPTANCE.json"
    if not review_path.is_file() or not acceptance_path.is_file():
        return None
    try:
        review = read_record(review_path, COMPLETION_REVIEW_SCHEMA)
        acceptance = read_record(acceptance_path, ACCEPTANCE_SCHEMA)
    except (OSError, ValueError):
        return None
    if not validate_acceptance_chain(
        review,
        acceptance,
        lane_id=lane["lane_id"],
        run_id=lane["run_id"],
    ):
        return None
    return {"review": review, "acceptance": acceptance}


def _run_provider(
    rt: Path,
    epoch_id: str,
    lane: dict[str, Any],
    invocation: dict[str, Any],
    binding: Any,
    prompt_path: Path,
    *,
    attempt_number: int = 1,
    resume: bool | None = None,
) -> ProviderExecution:
    """Start the provider, stream output, and return its exact process boundary."""
    worktree = Path(lane["worktree_path"])
    attempt_paths = _attempt_paths(lane, attempt_number)
    attempt_paths["transcript"].parent.mkdir(parents=True, exist_ok=True)
    attempt_paths["stderr"].parent.mkdir(parents=True, exist_ok=True)
    transcript_path = attempt_paths["transcript"]
    stderr_path = attempt_paths["stderr"]
    last_message_path = Path(lane["last_message_path"])
    session_id = (lane.get("session") or {}).get("session_id")
    if resume is None:
        resume = bool(session_id)
    argv = binding.build_argv(
        model=invocation["provider"]["model"],
        launch_config=invocation["provider"]["launch_config"],
        worktree=str(worktree),
        prompt_path=str(prompt_path),
        session_id=session_id,
        resume=resume,
    )
    _append_event(lane, "provider_started", " ".join(argv))
    pre_spawn_offset: int = 0
    with prompt_path.open("r", encoding="utf-8") as prompt_handle, \
         transcript_path.open("a", encoding="utf-8") as transcript_handle, \
         stderr_path.open("a", encoding="utf-8") as stderr_handle:
        pre_spawn_offset = transcript_handle.tell()
        try:
            child = processes.spawn_provider(
                argv,
                cwd=str(worktree),
                stdin=prompt_handle,
                stdout=transcript_handle,
                stderr=stderr_handle,
            )
        except Exception as exc:
            raise ControllerError(
                LAUNCH_PROVIDER_START_FAILED,
                f"provider process was not created: {exc}",
                no_provider_started=True,
            ) from exc
    boundary: processes.ProcessBoundary | None = None
    try:
        take_job_handle = getattr(child, "take_job_handle", None)
        job_handle = (
            take_job_handle()
            if os.name == "nt" and callable(take_job_handle)
            else None
        )
        if os.name == "nt" and job_handle is None:
            raise ControllerError(
                LAUNCH_PROVIDER_START_FAILED,
                "provider was created without a preassigned Job Object",
            )
        boundary = processes.ProcessBoundary.for_process(
            child.pid,
            windows_job_handle=job_handle,
        )
        if boundary.root_creation_time is None:
            raise ControllerError(
                LAUNCH_PROVIDER_START_FAILED,
                "cannot record provider process identity",
            )
        if os.name == "nt":
            # The record is durable before the suspended provider is allowed to
            # execute. The provider was already a Job member when native
            # process creation returned, so every point before this record is
            # also covered by kill-on-close.
            _write_status(
                lane,
                {
                    "provider_state": {
                        "state": "starting",
                        "pid": child.pid,
                        "creation_time": boundary.root_creation_time,
                        "process_group_id": boundary.process_group_id,
                        "session_id": boundary.session_id,
                        "provider_session_id": session_id,
                    },
                    "process_boundary": boundary.record(),
                },
            )
            getattr(child, "resume")()
    except BaseException:
        cleanup_proven = boundary is not None and boundary.cleanup(
            force=True,
            timeout_seconds=10.0,
        )
        if not cleanup_proven:
            try:
                child.terminate()
                child.wait(timeout=10.0)
            finally:
                close = getattr(child, "close", None)
                if callable(close):
                    close()
        else:
            close = getattr(child, "close", None)
            if callable(close):
                close()
        raise
    assert boundary is not None
    _write_status(
        lane,
        {
            "provider_state": {
                "state": "running",
                "pid": child.pid,
                "creation_time": boundary.root_creation_time,
                "process_group_id": boundary.process_group_id,
                "session_id": boundary.session_id,
                "provider_session_id": session_id,
            },
            "process_boundary": boundary.record(),
        },
    )
    boundary.observe()
    last_message: str | None = None
    session: str | None = None
    non_retryable_failure = False
    with transcript_path.open("r", encoding="utf-8", errors="replace") as handle:
        handle.seek(pre_spawn_offset)
        next_observation = time.monotonic()
        while child.poll() is None:
            if time.monotonic() >= next_observation:
                boundary.observe()
                next_observation = time.monotonic() + 0.5
            line = handle.readline()
            if line:
                parsed = binding.parse_line(line.rstrip("\n"))
                if isinstance(parsed, dict):
                    if parsed.get("message"):
                        last_message = str(parsed["message"])
                    if parsed.get("session_id"):
                        session = str(parsed["session_id"])
                    if parsed.get("non_retryable_failure") is True:
                        non_retryable_failure = True
            else:
                # The runtime is shutting down: this controller cleans its own
                # provider by exact identity (its direct child handle), then
                # the normal cleanup-proof path below runs.
                state = read_runtime_state(rt)
                if state is not None and state.get("state") == "SHUTTING_DOWN":
                    _append_event(
                        lane,
                        "shutdown_stop",
                        "runtime shutting down; terminating provider boundary",
                    )
                    boundary.cleanup(force=True, timeout_seconds=10.0)
                    try:
                        child.wait(timeout=10.0)
                    except subprocess.TimeoutExpired:
                        child.kill()  # Popen handle for this exact child
                        child.wait(timeout=10.0)
                    break
                time.sleep(0.2)
        for line in handle:
            parsed = binding.parse_line(line.rstrip("\n"))
            if isinstance(parsed, dict):
                if parsed.get("message"):
                    last_message = str(parsed["message"])
                if parsed.get("session_id"):
                    session = str(parsed["session_id"])
                if parsed.get("non_retryable_failure") is True:
                    non_retryable_failure = True
    exit_code = child.returncode if child.returncode is not None else -1
    if last_message is not None:
        last_message_path.write_text(last_message, encoding="utf-8")
    if session is not None:
        update_lane(
            rt,
            epoch_id,
            lane["lane_id"],
            lambda current, value=session: {**current, "session": {"session_id": value}},
        )
    provider_session_id = session or session_id
    return ProviderExecution(
        exit_code,
        boundary,
        provider_session_id,
        tuple(argv),
        non_retryable_failure or exit_code != 0,
    )


def run_controller(lane_id: str) -> int:
    """Execute one controller lifetime; returns the process exit code."""
    harness_root = find_harness_root()
    config = load_config(harness_root)
    rt = config.runtime_root
    epoch_id, lane = find_active_lane(rt, lane_id)
    invocation_path = Path(lane["worktree_path"]) / ".agent-workspace" / "invocation.json"
    try:
        invocation = read_record(invocation_path, INVOCATION_SCHEMA)
        if invocation.get("lane_id") != lane_id or invocation.get("run_id") != lane.get("run_id"):
            raise ControllerError(
                LAUNCH_INVOCATION_INVALID,
                "invocation does not match the lane's current run",
            )
    except (OSError, ValueError) as exc:
        raise ControllerError(LAUNCH_INVOCATION_INVALID, str(exc)) from exc
    provider_id = invocation["provider"]["id"]

    _write_status(lane, {"controller_state": "starting"})
    _append_event(lane, "controller_started", lane_id)

    identity = processes.process_identity(os.getpid())
    if identity is None:
        raise ControllerError(
            LAUNCH_CONTROLLER_START_FAILED, "cannot record controller process identity"
        )
    lane = update_lane(
        rt,
        epoch_id,
        lane_id,
        lambda current: {
            **current,
            "lifecycle": "running",
            "process": {"pid": identity["pid"], "creation_time": identity["creation_time"]},
        },
    )
    try:
        binding = _load_binding(harness_root, provider_id)
    except Exception as exc:
        _write_status(
            lane,
            {
                "controller_state": "exited",
                "provider_state": {"state": "not_started"},
                "cleanup_proven": True,
                "cleanup_error": None,
            },
        )
        _append_event(lane, "binding_failed", str(exc))
        update_lane(
            rt,
            epoch_id,
            lane_id,
            lambda current: {**current, "lifecycle": "prepared", "process": {}},
        )
        return 4

    declared = [str(item) for item in invocation.get("exclusive_resources", [])]
    try:
        acquire_leases(
            rt,
            declared,
            lane_id=lane_id,
            run_id=lane["run_id"],
            pid=identity["pid"],
            creation_time=identity["creation_time"],
        )
    except Exception as exc:
        code = getattr(exc, "code", LAUNCH_LEASE_BUSY)
        _write_status(
            lane,
            {
                "controller_state": "exited",
                "provider_state": {"state": "not_started"},
                "cleanup_proven": True,
                "cleanup_error": None,
            },
        )
        _append_event(lane, "lease_busy", str(exc))
        update_lane(
            rt,
            epoch_id,
            lane_id,
            lambda current: {**current, "lifecycle": "prepared", "process": {}},
        )
        return 2 if code == LAUNCH_LEASE_BUSY else 3
    _append_event(lane, "leases_acquired", ",".join(declared) or "(none)")
    _write_status(lane, {"controller_state": "running"})

    prompt_path = Path(lane["worktree_path"]) / ".agent-workspace" / "worker-prompt.md"
    if not prompt_path.is_file():
        prompt_path = Path(lane["worktree_path"]) / ".agent-workspace" / "prompt.md"

    attempt_number = 1
    correction_count = 0
    while True:
        attempt_paths = _attempt_paths(lane, attempt_number)
        try:
            execution = _run_provider(
                rt,
                epoch_id,
                lane,
                invocation,
                binding,
                prompt_path,
                attempt_number=attempt_number,
                resume=attempt_number > 1 or bool((lane.get("session") or {}).get("session_id")),
            )
        except Exception as exc:
            status_path = Path(lane["controller_status_path"])
            try:
                status = read_record(status_path, CONTROLLER_STATUS_SCHEMA)
            except (OSError, ValueError):
                status = {}
            boundary_record = status.get("process_boundary") if isinstance(status, dict) else None
            no_provider_started = bool(
                isinstance(exc, ControllerError) and exc.no_provider_started
            )
            cleanup_proven = no_provider_started or (
                isinstance(boundary_record, dict)
                and processes.cleanup_recorded_process_boundary(boundary_record)
            )
            # A provider adapter that cannot build its native resume vector is
            # a terminal no-result outcome, not permission to start fresh.
            if attempt_number > 1 and not no_provider_started:
                _append_attempt(
                    lane,
                    attempt_number=attempt_number,
                    argv=None,
                    session_id=(lane.get("session") or {}).get("session_id"),
                    prompt_path=prompt_path,
                    paths=attempt_paths,
                    exit_code=None,
                    result_state="invalid",
                    cleanup_proven=cleanup_proven,
                    validation_error=f"native resume unavailable: {exc}",
                )
                _write_status(
                    lane,
                    {
                        "controller_state": "exited",
                        "provider_state": {"state": "exited", "exit_code": -1},
                        "result_state": "invalid",
                        "recorded_status": "provider_exited_no_result",
                        "cleanup_proven": cleanup_proven,
                    },
                )
                _append_event(
                    lane,
                    "provider_exited_no_result",
                    f"native resume unavailable: {exc}",
                )
                if cleanup_proven:
                    release_leases(rt, lane_id, lane["run_id"])
                    _append_event(lane, "leases_released", ",".join(declared) or "(none)")
                update_lane(
                    rt,
                    epoch_id,
                    lane_id,
                    lambda current: {**current, "lifecycle": "result_invalid"},
                )
                return 0
            provider_state = dict(status.get("provider_state") or {}) if isinstance(status, dict) else {}
            provider_state.update(
                {"state": "not_started" if no_provider_started else "exited", "exit_code": -1}
            )
            _append_attempt(
                lane,
                attempt_number=attempt_number,
                argv=None,
                session_id=(lane.get("session") or {}).get("session_id"),
                prompt_path=prompt_path,
                paths=attempt_paths,
                exit_code=None,
                result_state="invalid",
                cleanup_proven=cleanup_proven,
                validation_error=str(exc),
            )
            _write_status(
                lane,
                {
                    "controller_state": "exited",
                    "provider_state": provider_state,
                    "cleanup_proven": cleanup_proven,
                    "recorded_status": "provider_start_failed",
                    **({"cleanup_error": "provider/helper process boundary remains unknown or live"} if not cleanup_proven else {}),
                },
            )
            _append_event(lane, "provider_start_failed", str(exc))
            if cleanup_proven:
                release_leases(rt, lane_id, lane["run_id"])
                _append_event(lane, "leases_released", ",".join(declared) or "(none)")
            return 4

        exit_code = execution.exit_code
        provider_state = {
            "state": "exited",
            "pid": execution.boundary.root_pid,
            "creation_time": execution.boundary.root_creation_time,
            "exit_code": exit_code,
            "process_group_id": execution.boundary.process_group_id,
            "session_id": execution.boundary.session_id,
            "provider_session_id": execution.session_id,
            "non_retryable_failure": execution.non_retryable_failure,
        }
        _write_status(
            lane,
            {
                "provider_state": provider_state,
                "process_boundary": execution.boundary.record(),
                "cleanup_proven": False,
                "attempt": attempt_number,
            },
        )
        _append_event(lane, "provider_exited", f"attempt={attempt_number}; exit_code={exit_code}")

        cleanup_proven = execution.boundary.cleanup(force=True)
        result_state, _result = _validate_result(lane)
        effective_result_state = (
            result_state
            if result_state == "valid"
            else ("invalid" if execution.non_retryable_failure else result_state)
        )
        if not cleanup_proven:
            _append_attempt(
                lane,
                attempt_number=attempt_number,
                argv=list(execution.argv),
                session_id=execution.session_id,
                prompt_path=prompt_path,
                paths=attempt_paths,
                exit_code=exit_code,
                result_state=effective_result_state,
                cleanup_proven=False,
                validation_error="provider/helper process boundary remains unknown or live",
            )
            _write_status(
                lane,
                {
                    "provider_state": provider_state,
                    "process_boundary": execution.boundary.record(),
                    "cleanup_proven": False,
                    "cleanup_error": "provider/helper process boundary remains unknown or live",
                },
            )
            _append_event(
                lane,
                "cleanup_unproven",
                "provider/helper process boundary was not proven gone; leases remain held",
            )
            return 5

        _append_event(lane, "cleanup_proven", "provider/helper process boundary confirmed gone")
        _append_attempt(
            lane,
            attempt_number=attempt_number,
            argv=list(execution.argv),
            session_id=execution.session_id,
            prompt_path=prompt_path,
            paths=attempt_paths,
            exit_code=exit_code,
            result_state=effective_result_state,
            cleanup_proven=True,
            validation_error=(
                None
                if result_state == "valid"
                else (
                    "provider startup/auth/process failure"
                    if execution.non_retryable_failure
                    else "missing or invalid RESULT.json"
                )
            ),
        )
        if execution.session_id:
            lane = {**lane, "session": {"session_id": execution.session_id}}

        if effective_result_state == "valid":
            # Cleanup proof covers the complete provider/helper boundary; only
            # now may the controller release the lane's exclusive leases.
            _write_status(
                lane,
                {
                    "cleanup_proven": True,
                    "result_state": "valid",
                    "recorded_status": "review_pending",
                },
            )
            release_leases(rt, lane_id, lane["run_id"])
            _append_event(lane, "leases_released", ",".join(declared) or "(none)")
            _append_event(lane, "result_valid", "review_pending")
            update_lane(
                rt,
                epoch_id,
                lane_id,
                lambda current: {**current, "lifecycle": "review_pending"},
            )
            recorded = "review_pending"
            break

        # The invalid result is kept as evidence while the provider continues
        # in its saved native session.  The original run_id and leases remain.
        if execution.session_id:
            update_lane(
                rt,
                epoch_id,
                lane_id,
                lambda current, value=execution.session_id: {
                    **current,
                    "session": {"session_id": value},
                },
            )
        if (
            correction_count >= 5
            or not execution.session_id
            or execution.non_retryable_failure
        ):
            _write_status(
                lane,
                {
                    "controller_state": "exited",
                    "provider_state": provider_state,
                    "result_state": "invalid",
                    "recorded_status": "provider_exited_no_result",
                    "cleanup_proven": True,
                    "correction_attempts": correction_count,
                },
            )
            release_leases(rt, lane_id, lane["run_id"])
            reason = (
                "provider failure"
                if execution.non_retryable_failure
                else "correction limit or native session unavailable"
            )
            _append_event(lane, "provider_exited_no_result", reason)
            _append_event(lane, "leases_released", ",".join(declared) or "(none)")
            update_lane(
                rt,
                epoch_id,
                lane_id,
                lambda current: {**current, "lifecycle": "result_invalid"},
            )
            return 0

        correction_count += 1
        correction_prompt = _write_correction_prompt(
            lane, correction_count, "missing or invalid RESULT.json"
        )
        _write_status(
            lane,
            {
                "cleanup_proven": True,
                "result_state": "invalid",
                "recorded_status": "correction_pending",
                "correction_attempts": correction_count,
            },
        )
        _append_event(
            lane,
            "correction_requested",
            f"attempt={correction_count}; prompt={correction_prompt}",
        )
        prompt_path = correction_prompt
        attempt_number += 1

    # Wait for the acceptance chain (ACCEPTED -> copy + exit; REJECTED -> exit).
    while True:
        chain = _read_acceptance_chain(rt, epoch_id, lane)
        if chain is not None:
            acceptance = chain["acceptance"]
            if acceptance.get("approval") == "ACCEPTED":
                _write_status(
                    lane,
                    {
                        "acceptance_advancement": acceptance,
                        "controller_state": "exited",
                        "recorded_status": recorded,
                    },
                )
                _append_event(lane, "acceptance_copied", "ACCEPTED")
                update_lane(
                    rt,
                    epoch_id,
                    lane_id,
                    lambda current, value=acceptance: {
                        **current,
                        "lifecycle": "accepted",
                        "acceptance_advancement": value,
                    },
                )
                return 0
            if acceptance.get("approval") == "REJECTED":
                _write_status(lane, {"controller_state": "exited", "recorded_status": recorded})
                _append_event(lane, "acceptance_rejected", "REJECTED")
                return 0
        state = read_runtime_state(rt)
        if state is not None and state.get("state") == "SHUTTING_DOWN":
            _write_status(lane, {"controller_state": "exited", "recorded_status": recorded})
            _append_event(lane, "shutdown_stop", "runtime shutting down")
            return 0
        time.sleep(ACCEPTANCE_POLL_SECONDS)


def main() -> int:
    if len(sys.argv) != 2:
        return 1
    try:
        return run_controller(sys.argv[1])
    except ControllerError as exc:
        print(f"{exc.code}: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"CONTROLLER_FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
