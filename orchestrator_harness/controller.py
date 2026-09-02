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
from pathlib import Path
from typing import Any

from . import processes
from .config import find_harness_root, load_config
from .core import content_hash, iso_utc, read_json, require_schema
from .epochs import lane_record_dir
from .lanes import find_active_lane, update_lane
from .leases import acquire_leases, release_leases
from .records import RecordLock, append_jsonl, atomic_write_json, read_record
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
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


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
    record.update(fields)
    with RecordLock(path):
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
        if not isinstance(record.get("summary"), str) or not record["summary"]:
            return "invalid", None
        if not isinstance(record.get("evidence"), list):
            return "invalid", None
        if record.get("content_hash") != content_hash(record):
            return "invalid", None
        return "valid", record
    except (OSError, ValueError):
        return "invalid", None


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
    if review.get("run_id") != lane["run_id"] or acceptance.get("run_id") != lane["run_id"]:
        return None
    if acceptance.get("review_ref") != review.get("content_hash"):
        return None
    return {"review": review, "acceptance": acceptance}


def _run_provider(
    rt: Path,
    epoch_id: str,
    lane: dict[str, Any],
    invocation: dict[str, Any],
    binding: Any,
    prompt_path: Path,
) -> int:
    """Start the provider, stream its output, and return its exit code."""
    worktree = Path(lane["worktree_path"])
    transcript_path = Path(lane["transcript_path"])
    stderr_path = Path(lane["stderr_path"])
    last_message_path = Path(lane["last_message_path"])
    session_id = (lane.get("session") or {}).get("session_id")
    resume = bool(session_id)
    argv = binding.build_argv(
        model=invocation["provider"]["model"],
        worktree=str(worktree),
        prompt_path=str(prompt_path),
        session_id=session_id,
        resume=resume,
    )
    _append_event(lane, "provider_started", " ".join(argv))
    with prompt_path.open("r", encoding="utf-8") as prompt_handle, \
         transcript_path.open("a", encoding="utf-8") as transcript_handle, \
         stderr_path.open("a", encoding="utf-8") as stderr_handle:
        child = subprocess.Popen(
            argv,
            cwd=str(worktree),
            stdin=prompt_handle,
            stdout=transcript_handle,
            stderr=stderr_handle,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=processes.WINDOWS_CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
    _write_status(lane, {"provider_state": {"state": "running", "pid": child.pid}})
    last_message: str | None = None
    session: str | None = None
    with transcript_path.open("r", encoding="utf-8", errors="replace") as handle:
        handle.seek(0, os.SEEK_END)
        while child.poll() is None:
            line = handle.readline()
            if line:
                parsed = binding.parse_line(line.rstrip("\n"))
                if isinstance(parsed, dict):
                    if parsed.get("message"):
                        last_message = str(parsed["message"])
                    if parsed.get("session_id"):
                        session = str(parsed["session_id"])
            else:
                # The runtime is shutting down: this controller cleans its own
                # provider by exact identity (its direct child handle), then
                # the normal cleanup-proof path below runs.
                state = read_runtime_state(rt)
                if state is not None and state.get("state") == "SHUTTING_DOWN":
                    _append_event(lane, "shutdown_stop", "runtime shutting down; terminating provider")
                    child.terminate()
                    try:
                        child.wait(timeout=10.0)
                    except subprocess.TimeoutExpired:
                        child.kill()
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
    return exit_code


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
    binding = _load_binding(harness_root, provider_id)

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
        _write_status(lane, {"controller_state": "exited"})
        _append_event(lane, "lease_busy", str(exc))
        return 2 if code == LAUNCH_LEASE_BUSY else 3
    _append_event(lane, "leases_acquired", ",".join(declared) or "(none)")

    prompt_path = Path(lane["worktree_path"]) / ".agent-workspace" / "worker-prompt.md"
    if not prompt_path.is_file():
        prompt_path = Path(lane["worktree_path"]) / ".agent-workspace" / "prompt.md"
    try:
        exit_code = _run_provider(rt, epoch_id, lane, invocation, binding, prompt_path)
    except Exception as exc:
        _write_status(
            lane,
            {"controller_state": "exited", "provider_state": {"state": "exited", "exit_code": -1}},
        )
        _append_event(lane, "provider_start_failed", str(exc))
        return 4

    _write_status(lane, {"provider_state": {"state": "exited", "exit_code": exit_code}})
    _append_event(lane, "provider_exited", f"exit_code={exit_code}")

    # Cleanup proof: the provider process is gone; release the leases.
    _write_status(lane, {"cleanup_proven": True})
    _append_event(lane, "cleanup_proven", "provider process confirmed gone")
    release_leases(rt, lane_id, lane["run_id"])
    _append_event(lane, "leases_released", ",".join(declared) or "(none)")

    result_state, _result = _validate_result(lane)
    recorded = "review_pending" if result_state == "valid" else "result_invalid"
    _write_status(
        lane,
        {
            "result_state": result_state,
            "recorded_status": recorded,
            "cleanup_proven": True,
        },
    )
    _append_event(lane, "result_" + result_state, recorded)
    update_lane(rt, epoch_id, lane_id, lambda current: {**current, "lifecycle": recorded})

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
