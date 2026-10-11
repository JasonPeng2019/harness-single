"""``lane launch``, ``lane force-stop``, and ``lane retire``.

Launch consumes the prepared invocation and starts the lane controller (which
starts the provider and owns the leases).  Force-stop is the targeted single-
lane hard stop.  Retire is the graceful end of an accepted lane.
"""

from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Any, Mapping

from . import processes
from .activity_log import append_detail
from .bootstrap import BootstrapError, _validate_provider_launch_config
from .config import find_harness_root, load_config
from .core import read_json, require_schema
from .epochs import (
    close_epoch,
    epoch_dir,
    lane_record_dir,
    read_active_lanes,
    read_current_epoch,
    read_epoch_state,
    write_active_lanes,
)
from .lanes import LaneError, find_active_lane, lane_operation_lock, read_lane, require_same_run, update_lane
from .lane_cleanup import cleanup_is_proven, controller_not_created, never_launched, observe_controller_status
from .leases import force_release_leases, release_leases
from .manager_queue import read_manager_queue
from .records import read_record
from .setup import read_runtime_state

INVOCATION_SCHEMA = "controller-invocation/v1"
CONTROLLER_STATUS_SCHEMA = "controller-status/v1"
ACCEPTANCE_SCHEMA = "orchestrator-acceptance/v1"
COMPLETION_REVIEW_SCHEMA = "completion-review/v1"

LAUNCH_INVOCATION_INVALID = "LAUNCH_INVOCATION_INVALID"
LAUNCH_ALLOWANCE_EXPIRED = "LAUNCH_ALLOWANCE_EXPIRED"
LAUNCH_BINDING_FAILED = "LAUNCH_BINDING_FAILED"
LAUNCH_LEASE_BUSY = "LAUNCH_LEASE_BUSY"
LAUNCH_CONTROLLER_START_FAILED = "LAUNCH_CONTROLLER_START_FAILED"
LAUNCH_PROVIDER_START_FAILED = "LAUNCH_PROVIDER_START_FAILED"
FORCE_STOP_LANE_NOT_FOUND = "FORCE_STOP_LANE_NOT_FOUND"
FORCE_STOP_ALLOWANCE_EXPIRED = "FORCE_STOP_ALLOWANCE_EXPIRED"
FORCE_STOP_PROCESS_SURVIVED = "FORCE_STOP_PROCESS_SURVIVED"
FORCE_STOP_CLEANUP_UNPROVEN = "FORCE_STOP_CLEANUP_UNPROVEN"
FORCE_STOP_OWNERSHIP_CHANGED = "FORCE_STOP_OWNERSHIP_CHANGED"
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


class ControllerNotCreated(LaunchError):
    """The native spawn failed before returning any controller handle."""


def _spawn_controller_logged(
    rt: Path,
    lane: Mapping[str, Any],
    argv: list[str],
    spawn_options: Mapping[str, Any],
) -> subprocess.Popen[Any]:
    try:
        append_detail(
            rt, "launch.controller.requested", component="launch",
            lane_id=lane.get("lane_id"), run_id=lane.get("run_id"),
            argv=list(argv), cwd=spawn_options.get("cwd"),
            environment_mode="scrubbed" if "env" in spawn_options else "inherited",
        )
        return processes.spawn_detached(argv, **dict(spawn_options))
    except (OSError, ValueError) as exc:
        raise ControllerNotCreated(LAUNCH_CONTROLLER_START_FAILED, str(exc)) from exc


def _read_controller_status(lane: dict[str, Any]) -> dict[str, Any] | None:
    return observe_controller_status(lane).status


def _clear_exited_controller_identity(
    rt: Path,
    epoch_id: str,
    lane_id: str,
    identity: dict[str, Any],
    run_id: str,
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
            current.get("run_id") == run_id
            and process.get("pid") == pid
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


def _stop_failed_controller(child: subprocess.Popen[Any], identity: dict[str, Any]) -> bool:
    """Record only what exact termination plus the owned handle actually proves."""
    try:
        if not processes.terminate_process(identity["pid"], identity["creation_time"], force=True):
            return False
        child.wait(timeout=10.0)
        return not processes.identity_matches(identity["pid"], identity["creation_time"])
    except (OSError, subprocess.TimeoutExpired):
        return False


def _record_launch_failure(
    rt: Path, epoch_id: str, lane: dict[str, Any], *,
    controller_not_created: bool, identity: dict[str, Any] | None = None,
    controller_reaped: bool = False,
) -> None:
    def record(current: dict[str, Any]) -> dict[str, Any]:
        require_same_run(current, lane)
        if current.get("lifecycle") in ("retired", "resuming") or current.get("launch_pending") is not False:
            raise LaunchError(LAUNCH_CONTROLLER_START_FAILED, "lane changed before failed-start recovery")
        if identity is not None and current.get("process") not in ({}, identity):
            raise LaunchError(LAUNCH_CONTROLLER_START_FAILED, "controller owner changed before failed-start recovery")
        return {
            **current,
            "process": dict(identity) if identity is not None else current.get("process"),
            "launch_failure": {
                "run_id": lane["run_id"],
                "controller_not_created": controller_not_created,
                "controller_reaped": controller_reaped,
            },
        }
    update_lane(rt, epoch_id, lane["lane_id"], record)


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
        with lane_operation_lock(rt, epoch_id, lane_id):
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

            # Consume the never-launched state before any process can be created.
            # Registration or cleanup failures must not look like a pending launch.
            captured = dict(lane)

            def consume_launch(current: dict[str, Any]) -> dict[str, Any]:
                require_same_run(current, captured)
                if current.get("lifecycle") != captured.get("lifecycle") or (
                    current.get("lifecycle") == "running" and current.get("launch_pending") is not True
                ):
                    raise LaunchError(LAUNCH_CONTROLLER_START_FAILED, "lane is no longer pending launch")
                if current.get("process") != {}:
                    raise LaunchError(LAUNCH_CONTROLLER_START_FAILED, "controller ownership is already recorded")
                if not cleanup_is_proven(current):
                    raise LaunchError(LAUNCH_CONTROLLER_START_FAILED, "prior controller cleanup is unproven")
                state = read_runtime_state(rt)
                marker = read_current_epoch(rt)
                if not state or state.get("state") != "OPEN" or not marker or marker.get("epoch_id") != epoch_id:
                    raise LaunchError(LAUNCH_CONTROLLER_START_FAILED, "runtime or epoch changed before launch")
                return {**current, "lifecycle": "running", "launch_pending": False, "launch_failure": None}

            lane = update_lane(rt, epoch_id, lane_id, consume_launch)
            # Recheck immediately at the spawn boundary, while the native operation
            # lock prevents resume or cancellation from replacing this invocation.
            state = read_runtime_state(rt)
            marker = read_current_epoch(rt)
            if not state or state.get("state") != "OPEN" or not marker or marker.get("epoch_id") != epoch_id:
                _record_launch_failure(rt, epoch_id, lane, controller_not_created=True)
                raise LaunchError(LAUNCH_CONTROLLER_START_FAILED, "runtime or epoch changed before spawn")
            try:
                child = _spawn_controller_logged(
                    rt, lane, processes.python_argv("orchestrator_harness.controller", lane_id),
                    {"cwd": str(harness_root)},
                )
            except ControllerNotCreated:
                _record_launch_failure(rt, epoch_id, lane, controller_not_created=True)
                raise
            controller_identity = processes.process_identity(child.pid)
            if controller_identity is None:
                # Without creation identity, a numeric PID is not a safe target.
                # Preserve the uncertain start instead of claiming no controller.
                _record_launch_failure(rt, epoch_id, lane, controller_not_created=False)
                raise LaunchError(
                    LAUNCH_CONTROLLER_START_FAILED,
                    "cannot record the launched controller process identity",
                )
            def register_controller(current: dict[str, Any]) -> dict[str, Any]:
                require_same_run(current, lane)
                if current.get("lifecycle") in ("retired", "resuming") or current.get("launch_pending") is not False:
                    raise LaunchError(LAUNCH_CONTROLLER_START_FAILED, "lane changed before controller registration")
                if current.get("process") not in ({}, controller_identity):
                    raise LaunchError(LAUNCH_CONTROLLER_START_FAILED, "controller owner changed before registration")
                return {
                    **current, "process": {
                        "pid": controller_identity["pid"], "creation_time": controller_identity["creation_time"],
                    },
                }

            try:
                lane = update_lane(
                    rt, epoch_id, lane_id, register_controller,
                )
            except Exception as exc:
                reaped = _stop_failed_controller(child, controller_identity)
                _record_launch_failure(
                    rt, epoch_id, lane, controller_not_created=False,
                    identity=controller_identity, controller_reaped=reaped,
                )
                raise LaunchError(
                    LAUNCH_CONTROLLER_START_FAILED,
                    f"cannot persist the launched controller process identity: {exc}",
                ) from exc
        append_detail(
            rt, "launch.controller.spawned", component="launch",
            lane_id=lane_id, run_id=lane.get("run_id"), pid=child.pid,
        )
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
                    lane["run_id"],
                )
        raise LaunchError(code, summary, evidence_paths=evidence)
    except LaunchError as exc:
        return {
            "ok": False,
            "code": exc.code,
            "summary": str(exc),
            "evidence_paths": exc.evidence_paths,
            "next_action": "on LAUNCH_LEASE_BUSY, wait and re-launch; otherwise inspect the exact failed-start cleanup evidence",
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
    """Compatibility boolean for callers that only need cleanup success."""
    return _cleanup_lane_processes(lane)[0]


def _cleanup_lane_processes(lane: dict[str, Any]) -> tuple[bool, str, str]:
    """Distinguish missing proof from an exact observed surviving identity."""
    observation = observe_controller_status(lane)
    if never_launched(lane, observation) or controller_not_created(lane, observation):
        return True, "", ""
    if observation.kind != "valid":
        return False, FORCE_STOP_CLEANUP_UNPROVEN, f"controller status is {observation.kind}: {observation.reason}"
    process = lane.get("process")
    if not isinstance(process, dict):
        return False, FORCE_STOP_CLEANUP_UNPROVEN, "controller process metadata is unknown"
    controller_pid = process.get("pid")
    controller_creation = process.get("creation_time")
    status = observation.status
    assert status is not None
    if process and (type(controller_pid) is not int or controller_pid <= 0 or not controller_creation):
        return False, FORCE_STOP_CLEANUP_UNPROVEN, "controller identity is incomplete"
    status_identity = status.get("controller_identity") or {}
    if not isinstance(status_identity, dict):
        return False, FORCE_STOP_CLEANUP_UNPROVEN, "status controller identity is invalid"
    if not process and not status_identity:
        return False, FORCE_STOP_CLEANUP_UNPROVEN, "controller exit identity is missing"
    if status_identity and not process:
        controller_pid = status_identity.get("pid")
        controller_creation = status_identity.get("creation_time")
        if type(controller_pid) is not int or controller_pid <= 0 or not controller_creation:
            return False, FORCE_STOP_CLEANUP_UNPROVEN, "status controller identity is incomplete"
    if status_identity and process and status_identity != process:
        return False, FORCE_STOP_CLEANUP_UNPROVEN, "controller identities disagree"
    provider_pid = None
    provider_creation = None
    provider_state = status.get("provider_state")
    if not isinstance(provider_state, dict):
        return False, FORCE_STOP_CLEANUP_UNPROVEN, "provider process metadata is unknown"
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
        provider_pid = provider_state.get("pid")
        provider_creation = provider_state.get("creation_time")
        boundary = status.get("process_boundary")
        if isinstance(boundary, dict):
            boundary_ok = processes.cleanup_recorded_process_boundary(boundary)
        elif isinstance(provider_pid, int):
            boundary_ok = processes.terminate_process(provider_pid, provider_creation, force=True)
        elif status.get("cleanup_proven") is True and status.get("controller_state") == "exited" and provider_state == {"state": "not_started"}:
            boundary_ok = True
    if ok and boundary_ok:
        return True, "", ""
    identities = [(controller_pid, controller_creation), (provider_pid, provider_creation)]
    boundary = status.get("process_boundary")
    if isinstance(boundary, dict):
        for member in [boundary.get("root"), *boundary.get("processes", [])]:
            if isinstance(member, dict):
                identities.append((member.get("pid"), member.get("creation_time")))
    if any(processes.identity_matches(pid, creation) for pid, creation in identities):
        return False, FORCE_STOP_PROCESS_SURVIVED, "an exact recorded lane process remains live after cleanup"
    return False, FORCE_STOP_CLEANUP_UNPROVEN, "controller or provider boundary cleanup could not be proven"


def run_force_stop(
    lane_id: str, *, allowance_seconds: float | None = None
) -> dict[str, Any]:
    """Execute ``lane force-stop`` and return the structured result.

    ``allowance_seconds`` is the caller's remaining share of one enclosing
    absolute deadline.  A spent allowance refuses before any termination,
    lease, or lane mutation effect, and the same absolute instant is
    re-checked immediately before termination, so a call that started inside
    the allowance and then ran long can never terminate or retire the lane
    after the caller stopped waiting.  ``None`` keeps the ordinary caller's
    unbounded behaviour.
    """
    force_stop_deadline: float | None = None
    if allowance_seconds is not None:
        force_stop_deadline = time.monotonic() + float(allowance_seconds)
    if force_stop_deadline is not None and force_stop_deadline <= time.monotonic():
        return {
            "ok": False,
            "code": FORCE_STOP_ALLOWANCE_EXPIRED,
            "summary": (
                "the enclosing allowance is spent; no termination or lease "
                "effect was started and the lane's ownership stays visible"
            ),
            "evidence_paths": [],
            "next_action": (
                "reconcile the exact lane inside the remaining decision time"
            ),
        }
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
    if force_stop_deadline is not None and time.monotonic() >= force_stop_deadline:
        # The call entered inside the allowance and ran long; the termination
        # phase is the first effect, so it may not start now.  The exact lane
        # keeps its ownership and no termination or lease effect was started.
        return {
            "ok": False,
            "code": FORCE_STOP_ALLOWANCE_EXPIRED,
            "summary": (
                "the enclosing allowance expired before termination started; "
                "no termination or lease effect was started and the lane's "
                "ownership stays visible"
            ),
            "evidence_paths": [],
            "next_action": (
                "reconcile the exact lane inside the remaining decision time"
            ),
        }
    try:
        with lane_operation_lock(rt, epoch_id, lane_id):
            def cancel(current: dict[str, Any]) -> dict[str, Any]:
                require_same_run(current, lane)
                marker = read_current_epoch(rt)
                if not marker or marker.get("epoch_id") != epoch_id or current.get("lifecycle") == "resuming":
                    raise LaneError(FORCE_STOP_OWNERSHIP_CHANGED, "epoch or lane ownership changed before cancellation")
                if force_stop_deadline is not None and time.monotonic() >= force_stop_deadline:
                    raise LaunchError(FORCE_STOP_ALLOWANCE_EXPIRED, "allowance expired before cancellation effects")
                clean, code, reason = _cleanup_lane_processes(current)
                if not clean:
                    raise LaunchError(code, reason, evidence_paths=[current["controller_status_path"]])
                if force_stop_deadline is not None and time.monotonic() >= force_stop_deadline:
                    raise LaunchError(FORCE_STOP_ALLOWANCE_EXPIRED, "allowance expired before lease release and retirement")
                # Still under the lane-record lock: no newer run can take ownership
                # between proof, exact-owner lease release, and retirement.
                try:
                    force_release_leases(rt, lane_id, current["run_id"])
                except Exception as exc:
                    raise LaunchError(FORCE_STOP_LEASE_RELEASE_FAILED, str(exc)) from exc
                return {**current, "lifecycle": "retired", "launch_pending": False}

            update_lane(rt, epoch_id, lane_id, cancel)
            entries = [e for e in read_active_lanes(rt, epoch_id) if e.get("lane_id") != lane_id]
            write_active_lanes(rt, epoch_id, entries)
            _prune_worktrees(config.root_workspace)
    except (LaunchError, LaneError) as exc:
        return {
            "ok": False,
            "code": FORCE_STOP_OWNERSHIP_CHANGED if isinstance(exc, LaneError) else exc.code,
            "summary": str(exc),
            "evidence_paths": getattr(exc, "evidence_paths", []),
            "next_action": "reconcile the exact invocation and its cleanup evidence before retrying",
        }
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
