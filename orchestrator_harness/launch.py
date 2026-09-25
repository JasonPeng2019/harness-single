"""``lane launch``, ``lane force-stop``, and ``lane retire``.

Launch consumes the prepared invocation and starts the lane controller (which
starts the provider and owns the leases).  Force-stop is the targeted single-
lane hard stop.  Retire is the graceful end of an accepted lane.
"""

from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Any

from . import processes
from .bootstrap import BootstrapError, _validate_provider_launch_config
from .config import find_harness_root, load_config
from .core import iso_utc, read_json, require_schema
from .epochs import (
    close_epoch,
    epoch_dir,
    lane_record_dir,
    read_active_lanes,
    read_current_epoch,
    read_epoch_state,
    write_active_lanes,
)
from .lanes import find_active_lane, read_lane, update_lane
from .leases import force_release_leases, release_leases
from .manager_queue import read_manager_queue
from .records import RecordLock, atomic_write_json, read_record
from .setup import read_runtime_state

INVOCATION_SCHEMA = "controller-invocation/v1"
CONTROLLER_STATUS_SCHEMA = "controller-status/v1"
ACCEPTANCE_SCHEMA = "orchestrator-acceptance/v1"
COMPLETION_REVIEW_SCHEMA = "completion-review/v1"

LAUNCH_INVOCATION_INVALID = "LAUNCH_INVOCATION_INVALID"
LAUNCH_BINDING_FAILED = "LAUNCH_BINDING_FAILED"
LAUNCH_LEASE_BUSY = "LAUNCH_LEASE_BUSY"
LAUNCH_CONTROLLER_START_FAILED = "LAUNCH_CONTROLLER_START_FAILED"
LAUNCH_PROVIDER_START_FAILED = "LAUNCH_PROVIDER_START_FAILED"
FORCE_STOP_LANE_NOT_FOUND = "FORCE_STOP_LANE_NOT_FOUND"
FORCE_STOP_PROCESS_SURVIVED = "FORCE_STOP_PROCESS_SURVIVED"
FORCE_STOP_LEASE_RELEASE_FAILED = "FORCE_STOP_LEASE_RELEASE_FAILED"
RETIRE_LANE_NOT_FOUND = "RETIRE_LANE_NOT_FOUND"
RETIRE_ACCEPTANCE_INVALID = "RETIRE_ACCEPTANCE_INVALID"
RETIRE_CLEANUP_UNPROVEN = "RETIRE_CLEANUP_UNPROVEN"
RETIRE_LEASE_RELEASE_FAILED = "RETIRE_LEASE_RELEASE_FAILED"

HANDSHAKE_TIMEOUT_SECONDS = 30.0
RETIRE_CONTROLLER_EXIT_WAIT_SECONDS = 5.0
RETIRE_CONTROLLER_EXIT_POLL_SECONDS = 0.1


class LaunchError(RuntimeError):
    def __init__(
        self, code: str, message: str, *, evidence_paths: list[str] | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.evidence_paths = evidence_paths or []


def _read_controller_status(lane: dict[str, Any]) -> dict[str, Any] | None:
    path = Path(lane["controller_status_path"])
    if not path.is_file():
        return None
    try:
        status = read_record(path, CONTROLLER_STATUS_SCHEMA)
        if (
            status.get("lane_id") != lane.get("lane_id")
            or status.get("run_id") != lane.get("run_id")
        ):
            return None
        return status
    except (OSError, ValueError):
        return None


def _clear_exited_controller_identity(
    rt: Path,
    epoch_id: str,
    lane_id: str,
    identity: dict[str, Any],
) -> None:
    """Clear the launch-owned controller identity after its handle exits.

    The controller writes terminal status before returning, so the detached
    launch handle is the authority that proves the controller has actually
    exited.  The mutation is conditional on the same PID-plus-creation pair
    still being recorded, preserving a newer owner if one was installed.
    """
    pid = identity.get("pid")
    creation_time = identity.get("creation_time")

    def clear(current: dict[str, Any]) -> dict[str, Any]:
        process = current.get("process") or {}
        if (
            process.get("pid") == pid
            and process.get("creation_time") == creation_time
        ):
            return {**current, "process": {}}
        return current

    update_lane(rt, epoch_id, lane_id, clear)


def _wait_for_controller_exit(
    lane: dict[str, Any], timeout_seconds: float
) -> bool:
    """Wait for the recorded controller incarnation to disappear exactly."""
    process = lane.get("process") or {}
    return _wait_for_pid_exit(
        process.get("pid"), process.get("creation_time"), timeout_seconds
    )


def _wait_for_pid_exit(
    pid: int, creation: str | None, timeout_seconds: float
) -> bool:
    """Wait until the exact PID-plus-creation incarnation is unobservable."""
    if not (isinstance(pid, int) and processes.identity_matches(pid, creation)):
        return True
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if not processes.identity_matches(pid, creation):
            return True
        time.sleep(RETIRE_CONTROLLER_EXIT_POLL_SECONDS)
    return not processes.identity_matches(pid, creation)


def _reap_controller_and_prove_exit(
    child: subprocess.Popen[Any],
    identity: dict[str, Any],
) -> bool:
    """Reap the launch-owned controller handle and prove the exact exit.

    The controller already declared terminal cleanup in its own status, so a
    launch-owned controller that is still observable after that proof is only
    the residual process of this launch.  Give the exact incarnation a bounded
    grace period for a natural exit, terminate only that exact incarnation when
    it lingers, reap the Popen handle, and return True only when the exact
    identity is no longer observable.  A live or unprovable identity is never
    reported as exited.
    """
    pid = identity.get("pid")
    creation = identity.get("creation_time")
    if not isinstance(pid, int):
        return False
    if processes.identity_matches(pid, creation):
        if not _wait_for_pid_exit(
            pid, creation, RETIRE_CONTROLLER_EXIT_WAIT_SECONDS
        ):
            if not processes.terminate_process(pid, creation, force=True):
                return False
    try:
        child.wait(timeout=RETIRE_CONTROLLER_EXIT_WAIT_SECONDS)
    except subprocess.TimeoutExpired:
        return False
    return not processes.identity_matches(pid, creation)


def run_launch(lane_id: str) -> dict[str, Any]:
    """Execute ``lane launch`` and return the structured result."""
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": LAUNCH_CONTROLLER_START_FAILED,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "fix the configuration and re-run setup",
        }
    rt = config.runtime_root
    try:
        state = read_runtime_state(rt)
        if state is None or state.get("state") != "OPEN":
            raise LaunchError(
                LAUNCH_CONTROLLER_START_FAILED,
                "runtime is not OPEN; run setup first",
            )
        epoch_id, lane = find_active_lane(rt, lane_id)
        if lane.get("lifecycle") not in ("prepared", "running"):
            raise LaunchError(
                LAUNCH_CONTROLLER_START_FAILED,
                f"lane {lane_id} is not prepared (lifecycle={lane.get('lifecycle')})",
            )
        invocation_path = Path(lane["worktree_path"]) / ".agent-workspace" / "invocation.json"
        try:
            invocation = read_record(invocation_path, INVOCATION_SCHEMA)
            if invocation.get("lane_id") != lane_id or invocation.get("run_id") != lane.get("run_id"):
                raise LaunchError(LAUNCH_INVOCATION_INVALID, "invocation does not match the lane's current run")
        except (OSError, ValueError) as exc:
            raise LaunchError(LAUNCH_INVOCATION_INVALID, str(exc)) from exc
        provider_id = invocation["provider"]["id"]
        try:
            configured_launch = _validate_provider_launch_config(
                harness_root,
                provider_id=provider_id,
                model=invocation["provider"].get("model"),
                launch_config=invocation["provider"].get("launch_config"),
            )
        except BootstrapError as exc:
            raise LaunchError(LAUNCH_INVOCATION_INVALID, str(exc)) from exc
        if configured_launch != invocation["provider"].get("launch_config"):
            raise LaunchError(
                LAUNCH_INVOCATION_INVALID,
                "provider launch configuration is not in its validated canonical form",
            )
        binding_path = (
            harness_root / "orchestrator_harness" / "provider_adapters" / provider_id / "launcher_binding.py"
        )
        if not binding_path.is_file():
            raise LaunchError(LAUNCH_BINDING_FAILED, f"binding missing: {binding_path}")

        child = processes.spawn_detached(
            processes.python_argv("orchestrator_harness.controller", lane_id),
            cwd=str(harness_root),
        )
        controller_identity = processes.process_identity(child.pid)
        if controller_identity is None:
            child.terminate()
            child.wait(timeout=10.0)
            raise LaunchError(
                LAUNCH_CONTROLLER_START_FAILED,
                "cannot record the launched controller process identity",
            )
        try:
            lane = update_lane(
                rt,
                epoch_id,
                lane_id,
                lambda current: {
                    **current,
                    "lifecycle": "running",
                    "process": {
                        "pid": controller_identity["pid"],
                        "creation_time": controller_identity["creation_time"],
                    },
                    "launch_pending": False,
                },
            )
        except Exception as exc:
            child.terminate()
            child.wait(timeout=10.0)
            raise LaunchError(
                LAUNCH_CONTROLLER_START_FAILED,
                f"cannot persist the launched controller process identity: {exc}",
            ) from exc
        deadline = time.monotonic() + HANDSHAKE_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            status = _read_controller_status(lane)
            provider_state = (status or {}).get("provider_state") or {}
            running = (
                status is not None
                and status.get("controller_state") == "running"
                and provider_state.get("state") == "running"
            )
            terminal = (
                status is not None
                and provider_state.get("state") == "exited"
                and status.get("cleanup_proven") is True
                and status.get("recorded_status") in ("review_pending", "result_invalid")
            )
            if running or terminal:
                lane = read_lane(rt, epoch_id, lane_id)
                state = status.get("recorded_status") if terminal else "running"
                return {
                    "ok": True,
                    "code": "LAUNCH_OK",
                    "summary": f"lane {lane_id} reached {state}",
                    "evidence_paths": [str(Path(lane["worktree_path"]) / ".agent-workspace" / "controller.status.json")],
                    "next_action": "wait for the worker result; the monitor reports actionable status",
                }
            if child.poll() is not None:
                break
            time.sleep(0.2)
        # The controller exited before the handshake: map its last event to a code.
        events_path = Path(lane["controller_events_path"])
        code = LAUNCH_CONTROLLER_START_FAILED
        summary = "controller exited before the provider started"
        if events_path.is_file():
            for line in events_path.read_text(encoding="utf-8", errors="replace").splitlines():
                if "lease_busy" in line:
                    code = LAUNCH_LEASE_BUSY
                    summary = "a declared resource is held; no lane started"
                elif "binding_failed" in line:
                    code = LAUNCH_BINDING_FAILED
                    summary = "the provider launcher binding could not be loaded"
                elif "provider_start_failed" in line:
                    code = LAUNCH_PROVIDER_START_FAILED
                    summary = "the provider process could not be started"
        status = _read_controller_status(lane)
        evidence = [str(Path(lane["controller_status_path"]))] if status else []
        if (
            code == LAUNCH_PROVIDER_START_FAILED
            and status is not None
            and status.get("controller_state") == "exited"
            and status.get("cleanup_proven") is True
        ):
            # Terminal cleanup is proven; the controller handle is still
            # launch-owned, so reap it and prove the exact incarnation exited
            # before clearing the recorded identity.
            controller_exited = _reap_controller_and_prove_exit(
                child, controller_identity
            )
            if controller_exited:
                _clear_exited_controller_identity(
                    rt,
                    epoch_id,
                    lane_id,
                    controller_identity,
                )
        raise LaunchError(code, summary, evidence_paths=evidence)
    except LaunchError as exc:
        return {
            "ok": False,
            "code": exc.code,
            "summary": str(exc),
            "evidence_paths": exc.evidence_paths,
            "next_action": "on LAUNCH_LEASE_BUSY, wait for the holder to finish and re-launch",
        }
    except Exception as exc:
        return {
            "ok": False,
            "code": LAUNCH_CONTROLLER_START_FAILED,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "resolve the error and re-launch",
        }


def _terminate_lane_processes(lane: dict[str, Any]) -> bool:
    """Terminate the lane's complete provider boundary and controller exactly."""
    process = lane.get("process") or {}
    controller_pid = process.get("pid")
    controller_creation = process.get("creation_time")
    status = _read_controller_status(lane)
    if not isinstance(controller_pid, int) and status is None:
        return lane.get("lifecycle") == "prepared"
    provider_pid = None
    provider_creation = None
    ok = True
    if isinstance(controller_pid, int):
        if not processes.terminate_process(
            controller_pid,
            controller_creation,
            force=True,
        ):
            ok = False
    boundary_ok = False
    if status is not None:
        provider_state = status.get("provider_state") or {}
        provider_pid = provider_state.get("pid")
        provider_creation = provider_state.get("creation_time")
        boundary = status.get("process_boundary")
        if isinstance(boundary, dict):
            boundary_ok = processes.cleanup_recorded_process_boundary(boundary)
        elif isinstance(provider_pid, int):
            boundary_ok = processes.terminate_process(provider_pid, provider_creation, force=True)
        elif status.get("cleanup_proven") is True and provider_state.get("state") == "not_started":
            boundary_ok = True
    return ok and boundary_ok


def _record_force_stopped(lane: dict[str, Any]) -> None:
    """Make a proven forced cleanup visible in the durable controller status."""
    path = Path(lane["controller_status_path"])
    if not path.is_file():
        return  # A prepared lane may never have started a controller.
    with RecordLock(path):
        status = read_record(path, CONTROLLER_STATUS_SCHEMA)
        if (
            status.get("lane_id") != lane["lane_id"]
            or status.get("run_id") != lane["run_id"]
        ):
            raise ValueError("controller status changed ownership during force-stop")
        provider_state = dict(status.get("provider_state") or {})
        if provider_state.get("state") in ("starting", "running"):
            provider_state["state"] = "exited"
        status.update(
            controller_state="exited",
            provider_state=provider_state,
            cleanup_proven=True,
            cleanup_error=None,
            updated_at=iso_utc(),
        )
        atomic_write_json(path, status)


def run_force_stop(lane_id: str) -> dict[str, Any]:
    """Execute ``lane force-stop`` and return the structured result."""
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": FORCE_STOP_LANE_NOT_FOUND,
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
            "code": FORCE_STOP_LANE_NOT_FOUND,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "check the lane id",
        }
    try:
        if not _terminate_lane_processes(lane):
            return {
                "ok": False,
                "code": FORCE_STOP_PROCESS_SURVIVED,
                "summary": "a lane process could not be terminated even forcibly",
                "evidence_paths": [str(Path(lane["worktree_path"]) / ".agent-workspace" / "controller.status.json")],
                "next_action": "escalate to the operator/host; an unkillable process is outside the harness's authority",
            }
        _record_force_stopped(lane)
        try:
            force_release_leases(rt, lane_id)
        except Exception as exc:
            return {
                "ok": False,
                "code": FORCE_STOP_LEASE_RELEASE_FAILED,
                "summary": str(exc),
                "evidence_paths": [],
                "next_action": "force-release the lease manually or retry force-stop",
            }
        update_lane(rt, epoch_id, lane_id, lambda current: {**current, "lifecycle": "retired"})
        entries = [e for e in read_active_lanes(rt, epoch_id) if e.get("lane_id") != lane_id]
        write_active_lanes(rt, epoch_id, entries)
        _prune_worktrees(config.root_workspace)
    except Exception as exc:
        return {
            "ok": False,
            "code": FORCE_STOP_LANE_NOT_FOUND,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "resolve the error and retry",
        }
    return {
        "ok": True,
        "code": "FORCE_STOP_OK",
        "summary": f"lane {lane_id} force-stopped and retired",
        "evidence_paths": [str(lane_record_dir(rt, epoch_id, lane_id) / "lane.json")],
        "next_action": "reuse the freed resource or bootstrap a fresh lane",
    }


def _prune_worktrees(root_workspace: Path) -> None:
    subprocess.run(
        ["git", "-C", str(root_workspace), "worktree", "prune"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def _validate_acceptance_ref(path: Path) -> dict[str, Any]:
    try:
        acceptance = read_record(path, ACCEPTANCE_SCHEMA)
    except (OSError, ValueError) as exc:
        raise LaunchError(RETIRE_ACCEPTANCE_INVALID, str(exc)) from exc
    if acceptance.get("approval") != "ACCEPTED":
        raise LaunchError(RETIRE_ACCEPTANCE_INVALID, "acceptance is not ACCEPTED")
    return acceptance


def run_retire(acceptance_ref: str) -> dict[str, Any]:
    """Execute ``lane retire`` and return the structured result."""
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": RETIRE_LANE_NOT_FOUND,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "fix the configuration and re-run setup",
        }
    rt = config.runtime_root
    try:
        acceptance = _validate_acceptance_ref(Path(acceptance_ref))
        lane_id = str(acceptance["lane_id"])
        epoch_id, lane = find_active_lane(rt, lane_id)
        status = _read_controller_status(lane)
        process = lane.get("process") or {}
        pid = process.get("pid")
        creation = process.get("creation_time")
        controller_gone = not processes.identity_matches(pid, creation)
        deadline = time.monotonic() + RETIRE_CONTROLLER_EXIT_WAIT_SECONDS
        while (
            not controller_gone
            and (status or {}).get("controller_state") != "exited"
            and time.monotonic() < deadline
        ):
            time.sleep(RETIRE_CONTROLLER_EXIT_POLL_SECONDS)
            status = _read_controller_status(lane)
            if status is None:
                break
            controller_gone = not processes.identity_matches(pid, creation)
        if (
            not controller_gone
            and (status or {}).get("controller_state") == "exited"
            and (status or {}).get("cleanup_proven") is True
        ):
            controller_gone = _wait_for_controller_exit(
                lane, RETIRE_CONTROLLER_EXIT_WAIT_SECONDS
            )
        cleanup_proven = bool((status or {}).get("cleanup_proven", False))
        boundary = (status or {}).get("process_boundary")
        boundary_gone = (
            isinstance(boundary, dict)
            and processes.process_boundary_is_gone(boundary)
        )
        provider_state = (status or {}).get("provider_state") or {}
        provider_pid = provider_state.get("pid")
        provider_creation = provider_state.get("creation_time")
        provider_gone = not (
            isinstance(provider_pid, int)
            and (
                processes.identity_matches(provider_pid, provider_creation)
                or (
                    processes.process_alive(provider_pid)
                    and processes.process_identity(provider_pid) is None
                )
            )
        )
        if not controller_gone or not cleanup_proven or not boundary_gone or not provider_gone:
            return {
                "ok": False,
                "code": RETIRE_CLEANUP_UNPROVEN,
                "summary": "lane cleanup is not proven; force-stop the lane first",
                "evidence_paths": [str(Path(lane["worktree_path"]) / ".agent-workspace" / "controller.status.json")],
                "next_action": "run `lane force-stop --lane-id <id>` then treat as done",
            }
        try:
            release_leases(rt, lane_id, lane["run_id"])
        except Exception as exc:
            return {
                "ok": False,
                "code": RETIRE_LEASE_RELEASE_FAILED,
                "summary": str(exc),
                "evidence_paths": [],
                "next_action": "retry retire after resolving the lease error",
            }
        update_lane(rt, epoch_id, lane_id, lambda current: {**current, "lifecycle": "retired"})
        entries = [e for e in read_active_lanes(rt, epoch_id) if e.get("lane_id") != lane_id]
        write_active_lanes(rt, epoch_id, entries)
        _prune_worktrees(config.root_workspace)
        if not entries:
            _maybe_close_epoch(rt, epoch_id)
    except LaunchError as exc:
        return {
            "ok": False,
            "code": exc.code,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "resolve the error and retry retire",
        }
    except Exception as exc:
        return {
            "ok": False,
            "code": RETIRE_LANE_NOT_FOUND,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "resolve the error and retry retire",
        }
    return {
        "ok": True,
        "code": "RETIRE_OK",
        "summary": f"lane {lane_id} retired",
        "evidence_paths": [str(Path(acceptance_ref))],
        "next_action": "none; the lane branch is retained",
    }


def _maybe_close_epoch(rt: Path, epoch_id: str) -> None:
    """Close the epoch when no active lane remains and no unresolved ROOT
    manager event remains (managed)."""
    try:
        state = read_epoch_state(rt, epoch_id)
    except (OSError, ValueError):
        return
    if state.get("lifecycle") != "active":
        return
    if state.get("lane_mode") == "managed":
        try:
            queue = read_manager_queue(rt)
        except Exception:
            return
        if any(event.get("state") in ("PENDING", "ACKNOWLEDGED") for event in queue.get("events", [])):
            return
    close_epoch(rt, epoch_id)
