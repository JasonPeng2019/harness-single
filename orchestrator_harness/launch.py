"""``lane launch``, ``lane force-stop``, and ``lane retire``.

Launch consumes the prepared invocation and starts the lane controller (which
starts the provider and owns the leases).  Force-stop is the targeted single-
lane hard stop.  Retire is the graceful end of an accepted lane.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Mapping

from . import memory_handoff, processes, terminal_evidence
from .activity_log import append_detail
from .bootstrap import BootstrapError, _validate_provider_launch_config
from .config import find_harness_root, load_config
from .core import content_hash, read_json, require_schema
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
from .records import RecordLock, read_record
from .setup import read_runtime_state

INVOCATION_SCHEMA = "controller-invocation/v1"
CONTROLLER_STATUS_SCHEMA = "controller-status/v1"
ACCEPTANCE_SCHEMA = "orchestrator-acceptance/v1"
COMPLETION_REVIEW_SCHEMA = "completion-review/v1"

LAUNCH_INVOCATION_INVALID = "LAUNCH_INVOCATION_INVALID"
LAUNCH_PLAN_PENDING = "LAUNCH_PLAN_PENDING"
LAUNCH_ALLOWANCE_EXPIRED = "LAUNCH_ALLOWANCE_EXPIRED"
LAUNCH_BINDING_FAILED = "LAUNCH_BINDING_FAILED"
LAUNCH_LEASE_BUSY = "LAUNCH_LEASE_BUSY"
LAUNCH_CONTROLLER_START_FAILED = "LAUNCH_CONTROLLER_START_FAILED"
LAUNCH_PROVIDER_START_FAILED = "LAUNCH_PROVIDER_START_FAILED"
LAUNCH_DISPATCH_AMBIGUOUS = "LAUNCH_DISPATCH_AMBIGUOUS"
FORCE_STOP_LANE_NOT_FOUND = "FORCE_STOP_LANE_NOT_FOUND"
FORCE_STOP_PROCESS_SURVIVED = "FORCE_STOP_PROCESS_SURVIVED"
FORCE_STOP_ALLOWANCE_EXPIRED = "FORCE_STOP_ALLOWANCE_EXPIRED"
FORCE_STOP_LEASE_RELEASE_FAILED = "FORCE_STOP_LEASE_RELEASE_FAILED"
RETIRE_LANE_NOT_FOUND = "RETIRE_LANE_NOT_FOUND"
RETIRE_ACCEPTANCE_INVALID = "RETIRE_ACCEPTANCE_INVALID"
RETIRE_CLEANUP_UNPROVEN = "RETIRE_CLEANUP_UNPROVEN"
RETIRE_LEASE_RELEASE_FAILED = "RETIRE_LEASE_RELEASE_FAILED"

# The explicit worker-environment boundary a durable task card may
# declare.  A "scrubbed" card starts its controller (and, through it,
# its provider) without the product control credentials; legacy and
# all-off cards declare nothing and keep the inherited environment.
WORKER_ENVIRONMENT_SCRUBBED = "scrubbed"
ROOT_MEMORY_SERVICE_KEYS = frozenset({"MONGODB_URI", "DEEPINFRA_API_KEY", "EVEROS_ROOT"})
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


def _spawn_controller_logged(
    rt: Path,
    lane: Mapping[str, Any],
    argv: list[str],
    spawn_options: Mapping[str, Any],
) -> subprocess.Popen[Any]:
    append_detail(
        rt,
        "launch.controller.requested",
        component="launch",
        lane_id=lane.get("lane_id"),
        run_id=lane.get("run_id"),
        argv=list(argv),
        cwd=spawn_options.get("cwd"),
        environment_mode=(
            "scrubbed" if "env" in spawn_options else "inherited"
        ),
    )
    child = processes.spawn_detached(argv, **dict(spawn_options))
    append_detail(
        rt,
        "launch.controller.spawned",
        component="launch",
        lane_id=lane.get("lane_id"),
        run_id=lane.get("run_id"),
        pid=child.pid,
        argv=list(argv),
    )
    return child


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


def _declared_worker_environment(task_card: Mapping[str, Any] | None) -> str | None:
    """Resolve one lane's explicit worker-environment boundary.

    The durable task card supplies the ordinary boundary. A card that declares
    nothing keeps the harness's inherited environment unless worker recall
    is enabled, and an enhanced lane
    with its own finalized dispatch envelope keeps the existing scrubbed
    worker environment.  A card that declares the scrubbed boundary is
    honored even without an enhanced memory handoff, so a restricted child
    never starts with the product control credentials.  An unknown mode is
    refused instead of silently degrading to inherited.
    """

    if not isinstance(task_card, Mapping):
        return None
    mode = task_card.get("worker_environment")
    if mode is None:
        return None
    from memory_harness.contracts import WORKER_ENVIRONMENT_MODES

    if not isinstance(mode, str) or mode not in WORKER_ENVIRONMENT_MODES:
        raise LaunchError(
            LAUNCH_INVOCATION_INVALID,
            f"task card declares an unknown worker environment mode: {mode!r}",
        )
    return mode


def _lookup_native_invocation(
    rt: Path, epoch_id: str, lane: Mapping[str, Any], *,
    expected_binding: Mapping[str, Any], expected_pid: int | None = None,
) -> dict[str, Any] | None:
    """Look up the controller's exact lane/run and PID/creation incarnation.

    The controller writes its process identity to the lane and attests the
    binding in its status. Both are needed before a lost acknowledgement can
    be reconciled to this dispatch.
    """

    current = read_lane(rt, epoch_id, str(lane["lane_id"]))
    if any(current.get(field) != lane.get(field) for field in ("lane_id", "run_id", "worktree_path")):
        raise LaunchError(
            LAUNCH_DISPATCH_AMBIGUOUS,
            "native lane ownership changed during dispatch reconciliation",
        )
    process = current.get("process") or {}
    pid = process.get("pid")
    creation = process.get("creation_time")
    if not isinstance(pid, int) or pid <= 0 or not isinstance(creation, str) or not creation:
        return None
    if expected_pid is not None and pid != expected_pid:
        raise LaunchError(
            LAUNCH_DISPATCH_AMBIGUOUS,
            "a different native controller owns this lane and run",
        )
    status = _read_controller_status(current)
    if status is not None and status.get("dispatch_binding") is not None and status["dispatch_binding"] != expected_binding:
        raise LaunchError(
            LAUNCH_DISPATCH_AMBIGUOUS,
            "native controller attests a conflicting dispatch binding",
        )
    if status is None or status.get("dispatch_binding") != expected_binding:
        return None
    if status.get("controller_identity") != {"pid": pid, "creation_time": creation}:
        raise LaunchError(
            LAUNCH_DISPATCH_AMBIGUOUS,
            "native controller attests a conflicting process identity",
        )
    if status.get("lane_id") != lane["lane_id"] or status.get("run_id") != lane["run_id"]:
        raise LaunchError(
            LAUNCH_DISPATCH_AMBIGUOUS,
            "native controller attests a conflicting lane or run",
        )
    live = processes.identity_matches(pid, creation)
    terminal = bool(
        status is not None
        and status.get("run_id") == lane["run_id"]
        and status.get("controller_state") == "exited"
        and status.get("cleanup_proven") is True
    )
    if not live and not terminal:
        return None
    return {"pid": pid, "creation_time": creation}


def _wait_for_spawn_attestation(
    rt: Path, epoch_id: str, lane: Mapping[str, Any], child: subprocess.Popen[Any],
    *, expected_binding: Mapping[str, Any], identity: dict[str, Any] | None,
    deadline: float,
) -> dict[str, Any] | None:
    """Wait within the handshake allowance for the exact native controller."""
    while True:
        if identity is None:
            identity = _lookup_native_invocation(
                rt, epoch_id, lane,
                expected_binding=expected_binding, expected_pid=child.pid,
            )
            if identity is not None:
                return identity
        else:
            current = read_lane(rt, epoch_id, str(lane["lane_id"]))
            if any(current.get(field) != lane.get(field) for field in ("lane_id", "run_id", "worktree_path")):
                raise LaunchError(
                    LAUNCH_DISPATCH_AMBIGUOUS,
                    "native lane ownership changed before controller attestation",
                )
            recorded_process = current.get("process") or {}
            if recorded_process and recorded_process != identity:
                raise LaunchError(
                    LAUNCH_DISPATCH_AMBIGUOUS,
                    "a different native process occupied this lane and run",
                )
            status = _read_controller_status(current)
            if status is not None:
                if status.get("dispatch_binding") not in (None, expected_binding):
                    raise LaunchError(
                        LAUNCH_DISPATCH_AMBIGUOUS,
                        "native controller attests a conflicting dispatch binding",
                    )
                if status.get("controller_identity") not in (None, identity):
                    raise LaunchError(
                        LAUNCH_DISPATCH_AMBIGUOUS,
                        "native controller attests a conflicting process identity",
                    )
                if (
                    status.get("lane_id") == lane["lane_id"]
                    and status.get("run_id") == lane["run_id"]
                    and status.get("dispatch_binding") == expected_binding
                    and status.get("controller_identity") == identity
                ):
                    return identity
        if child.poll() is not None or time.monotonic() >= deadline:
            return None
        time.sleep(0.1)


def _delivered_provider_outcome(
    lane: Mapping[str, Any], identity: Mapping[str, Any],
    expected_binding: Mapping[str, Any],
    *, status: Mapping[str, Any] | None = None,
) -> tuple[str, str] | None:
    """Read the exact controller's provider outcome, not just its delivery.

    ``None`` means the provider outcome is still unresolved. A controller can
    attest its PID and then fail while acquiring a lease or starting a provider.
    The existing status and run-scoped events retain that failure without a
    second dispatch receipt.
    """
    if status is None:
        status = _read_controller_status(dict(lane))
    if (
        status is None
        or status.get("dispatch_binding") != expected_binding
        or status.get("controller_identity") != {
            "pid": identity.get("pid"), "creation_time": identity.get("creation_time")
        }
    ):
        return None
    provider = status.get("provider_state") or {}
    if not isinstance(provider, Mapping):
        return None
    if status.get("controller_state") == "running" and provider.get("state") == "running":
        return "LAUNCH_OK", "the provider started"
    if (
        provider.get("state") == "exited"
        and status.get("cleanup_proven") is True
        and status.get("recorded_status") in ("review_pending", "result_invalid")
    ):
        return "LAUNCH_OK", f"the provider reached {status['recorded_status']}"
    if not (
        status.get("controller_state") == "exited"
        and provider.get("state") == "not_started"
        and status.get("cleanup_proven") is True
        and not any(provider.get(key) for key in ("pid", "creation_time", "provider_session_id"))
        and not status.get("process_boundary")
        and not (lane.get("session") or {}).get("session_id")
    ):
        return None
    failure = status.get("recorded_status")
    if failure not in ("lease_busy", "binding_failed", "provider_start_failed"):
        events_path = Path(lane["controller_events_path"])
        try:
            lines = events_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            lines = []
        for line in lines:
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("run_id") == lane["run_id"] and event.get("event_type") in (
                "lease_busy", "binding_failed", "provider_start_failed"
            ):
                failure = event["event_type"]
    return {
        "lease_busy": (LAUNCH_LEASE_BUSY, "a declared resource is held; no provider started"),
        "binding_failed": (LAUNCH_BINDING_FAILED, "the provider binding failed before start"),
        "provider_start_failed": (LAUNCH_PROVIDER_START_FAILED, "the provider could not be started"),
    }.get(failure)


def run_launch(
    lane_id: str, *, allowance_seconds: float | None = None
) -> dict[str, Any]:
    """Execute ``lane launch`` and return the structured result.

    ``allowance_seconds`` is the caller's remaining share of one enclosing
    absolute deadline. A spent allowance refuses before durable intent. Once
    intent commits, the native spawn is attempted so the operation cannot be
    stranded as a proven zero-spawn pending intent. The following handshake
    remains bounded. ``None`` keeps the ordinary caller's unbounded behaviour.
    """
    if allowance_seconds is not None and allowance_seconds <= 0:
        return {
            "ok": False,
            "code": LAUNCH_ALLOWANCE_EXPIRED,
            "summary": (
                "the enclosing allowance is spent; no controller was started "
                "and the prepared lane keeps its exact ownership"
            ),
            "evidence_paths": [],
            "next_action": (
                "reconcile the exact prepared lane, or prepare again inside "
                "the remaining decision time"
            ),
        }
    launch_deadline = (
        None
        if allowance_seconds is None
        else time.monotonic() + float(allowance_seconds)
    )
    memory_envelope: dict[str, Any] | None = None
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": LAUNCH_CONTROLLER_START_FAILED,
            "summary": memory_handoff.redact_control_diagnostic(str(exc)),
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
        # Resolve the lane's governing enhanced-handoff state from the durable
        # task card and the lane record.  ``None`` is the inherited ordinary
        # path: a legacy card or an all-off enhanced card never initializes or
        # validates optional memory here.  Every other value is an enabled
        # enhanced lane, and it must already own its exact finalized dispatch
        # envelope and durable final context before any dispatch intent exists.
        task_card_path = (
            Path(lane["worktree_path"]) / ".agent-workspace" / "task-card.json"
        )
        task_card: dict[str, Any] | None = None
        if task_card_path.is_file():
            try:
                task_card = read_json(task_card_path)
            except (OSError, ValueError) as exc:
                raise LaunchError(
                    LAUNCH_INVOCATION_INVALID,
                    f"cannot read the task card for memory dispatch: {exc}",
                ) from exc
        elif lane.get("memory_plan_state"):
            raise LaunchError(
                LAUNCH_PLAN_PENDING,
                "the enhanced lane's durable task card is missing; "
                "nothing may dispatch",
            )
        # The lane's durable record is the authority for a restricted worker
        # environment; the copied card only carries the same requirement
        # forward.  A prepared scrubbed lane whose card is missing or no
        # longer declares the scrubbed boundary must fail closed instead of
        # launching its controller with the inherited control credentials.
        worker_environment = _declared_worker_environment(task_card)
        if (
            lane.get("worker_environment") == WORKER_ENVIRONMENT_SCRUBBED
            and worker_environment != WORKER_ENVIRONMENT_SCRUBBED
        ):
            raise LaunchError(
                LAUNCH_INVOCATION_INVALID,
                "the prepared lane's durable scrubbed worker environment is not "
                "confirmed by its copied task card; nothing may launch with the "
                "inherited product control environment",
            )
        try:
            memory_state = memory_handoff.lane_handoff_state(task_card, lane)
        except memory_handoff.PendingPlanError as exc:
            raise LaunchError(LAUNCH_PLAN_PENDING, str(exc)) from exc
        except memory_handoff.MemoryHandoffError as exc:
            raise LaunchError(LAUNCH_INVOCATION_INVALID, str(exc)) from exc
        final_context: dict[str, Any] | None = None
        if memory_state is not None:
            memory_envelope = memory_handoff.load_envelope(lane["worktree_path"])
            if memory_envelope is None:
                raise LaunchError(
                    LAUNCH_PLAN_PENDING,
                    f"lane {lane_id} has no finalized dispatch envelope for its "
                    f"{memory_state!r} enhanced plan state; ROOT must accept the "
                    "exact current plan and finalize it before anything launches",
                )
            base_commit = str((task_card or {}).get("base_commit") or "HEAD")
            try:
                memory_handoff.validate_envelope_for_launch(
                    envelope=memory_envelope,
                    task_card=task_card,
                    lane_id=lane_id,
                    run_id=str(lane.get("run_id")),
                    worktree_path=lane["worktree_path"],
                    base_commit=base_commit,
                )
                final_context = memory_handoff.load_final_context(
                    worktree_path=lane["worktree_path"],
                    envelope=memory_envelope,
                )
                memory_handoff.validate_final_context_for_launch(
                    context=final_context,
                    envelope=memory_envelope,
                    task_card=task_card,
                    lane_id=lane_id,
                    run_id=str(lane.get("run_id")),
                    worktree_path=lane["worktree_path"],
                    base_commit=base_commit,
                )
                if invocation.get("content_hash") != content_hash(invocation):
                    raise memory_handoff.MemoryHandoffError("controller invocation integrity changed")
                if invocation.get("dispatch_binding") != memory_handoff.dispatch_binding(
                    envelope=memory_envelope, context=final_context
                ):
                    raise memory_handoff.MemoryHandoffError(
                        "controller invocation dispatch identity changed"
                    )
                prompt_path = Path(lane["worktree_path"]) / ".agent-workspace" / "worker-prompt.md"
                if invocation.get("prompt_digest") != hashlib.sha256(prompt_path.read_bytes()).hexdigest():
                    raise memory_handoff.MemoryHandoffError(
                        "controller invocation worker prompt changed"
                    )
                if (
                    invocation.get("provider") != lane.get("provider")
                    or invocation.get("cwd") != lane["worktree_path"]
                    or invocation.get("paths", {}).get("prompt")
                    != str(Path(lane["worktree_path"]) / ".agent-workspace" / "worker-prompt.md")
                ):
                    raise memory_handoff.MemoryHandoffError(
                        "controller invocation differs from the prepared lane"
                    )
            except memory_handoff.PendingPlanError as exc:
                raise LaunchError(LAUNCH_PLAN_PENDING, str(exc)) from exc
            except memory_handoff.MemoryHandoffError as exc:
                raise LaunchError(LAUNCH_INVOCATION_INVALID, str(exc)) from exc
        binding_path = (
            harness_root / "orchestrator_harness" / "provider_adapters" / provider_id / "launcher_binding.py"
        )
        if not binding_path.is_file():
            raise LaunchError(LAUNCH_BINDING_FAILED, f"binding missing: {binding_path}")
        if launch_deadline is not None and time.monotonic() >= launch_deadline:
            raise LaunchError(
                LAUNCH_ALLOWANCE_EXPIRED,
                "the enclosing allowance was spent before the controller could "
                "start; the prepared lane keeps its exact ownership",
            )

        spawn_options: dict[str, Any] = {"cwd": str(harness_root)}
        if memory_envelope is not None:
            try:
                spawn_options["env"] = memory_handoff.worker_environment(
                    task_card, provider_id=provider_id
                )
                memory_handoff.validate_worker_material(
                    worktree_path=lane["worktree_path"],
                    invocation=invocation,
                    environment=spawn_options["env"],
                    task_card=task_card,
                )
            except memory_handoff.MemoryHandoffError as exc:
                raise LaunchError(LAUNCH_INVOCATION_INVALID, str(exc)) from exc
        elif (
            worker_environment == WORKER_ENVIRONMENT_SCRUBBED
            or os.environ.get("MEMORY_HARNESS_WORKER_RECALL") == "1"
        ):
            try:
                spawn_options["env"] = memory_handoff.worker_environment(
                    task_card, provider_id=provider_id
                )
                memory_handoff.validate_worker_material(
                    worktree_path=lane["worktree_path"],
                    invocation=invocation,
                    environment=spawn_options["env"],
                    task_card=task_card,
                )
            except memory_handoff.MemoryHandoffError as exc:
                raise LaunchError(LAUNCH_INVOCATION_INVALID, str(exc)) from exc
        if os.environ.get("MEMORY_HARNESS_WORKER_RECALL") == "1" and "env" in spawn_options:
            spawn_options["env"] = {
                key: value for key, value in spawn_options["env"].items()
                if key.upper() not in ROOT_MEMORY_SERVICE_KEYS
                and not key.upper().startswith(("MEMORY_HARNESS_", "EVEROS_EMBEDDING__"))
            }
        argv = processes.python_argv("orchestrator_harness.controller", lane_id)
        if memory_envelope is not None:
            assert final_context is not None
            # Serialize only the final intent/native-ownership transition.
            # The operation row is the durable intent; this lock adds no
            # second receipt or launcher and closes concurrent launch races.
            with RecordLock(memory_handoff.memory_paths(lane["worktree_path"])[1]):
                try:
                    locked_lane = read_lane(rt, epoch_id, lane_id)
                    if any(
                        locked_lane.get(field) != lane.get(field)
                        for field in (
                            "lane_id", "run_id", "worktree_path", "provider",
                            "memory_plan_state", "worker_environment", "dispatchable",
                            "launch_pending", "lifecycle", "native_supersession",
                        )
                    ):
                        raise memory_handoff.MemoryHandoffError(
                            "lane dispatch identity changed before native launch"
                        )
                    current_card = read_json(task_card_path)
                    current_envelope = memory_handoff.load_envelope(lane["worktree_path"])
                    if current_card != task_card or current_envelope != memory_envelope:
                        raise memory_handoff.MemoryHandoffError(
                            "accepted task or dispatch envelope changed before native launch"
                        )
                    if read_json(invocation_path) != invocation:
                        raise memory_handoff.MemoryHandoffError(
                            "controller invocation changed before native launch"
                        )
                    if invocation["prompt_digest"] != hashlib.sha256(
                        (Path(lane["worktree_path"]) / ".agent-workspace" / "worker-prompt.md").read_bytes()
                    ).hexdigest():
                        raise memory_handoff.MemoryHandoffError(
                            "worker prompt changed before native launch"
                        )
                    memory_handoff.validate_envelope_for_launch(
                        envelope=current_envelope,
                        task_card=current_card,
                        lane_id=lane_id,
                        run_id=str(lane["run_id"]),
                        worktree_path=lane["worktree_path"],
                        base_commit=base_commit,
                    )
                    current_context = memory_handoff.load_final_context(
                        worktree_path=lane["worktree_path"], envelope=memory_envelope
                    )
                    memory_handoff.validate_final_context_for_launch(
                        context=current_context,
                        envelope=memory_envelope,
                        task_card=task_card,
                        lane_id=lane_id,
                        run_id=str(lane["run_id"]),
                        worktree_path=lane["worktree_path"],
                        base_commit=base_commit,
                    )
                    if current_context != final_context:
                        raise memory_handoff.MemoryHandoffError(
                            "durable final context changed before native launch"
                        )
                    memory_handoff.validate_worker_material(
                        worktree_path=lane["worktree_path"],
                        invocation=invocation,
                        environment=spawn_options["env"],
                        task_card=current_card,
                    )
                    existing = memory_handoff.get_dispatch_operation(
                        worktree_path=lane["worktree_path"], envelope=memory_envelope
                    )
                except (OSError, ValueError, memory_handoff.MemoryHandoffError) as exc:
                    raise LaunchError(LAUNCH_INVOCATION_INVALID, str(exc)) from exc
                if existing is not None:
                    observed = existing.get("observed_invocation")
                    if existing["status"] == "delivered" and isinstance(observed, Mapping):
                        expected = memory_handoff.native_observation(
                            envelope=memory_envelope,
                            context=final_context,
                            controller_identity=observed,
                        )
                        if observed != expected:
                            raise LaunchError(
                                LAUNCH_DISPATCH_AMBIGUOUS,
                                "stored native owner conflicts with this exact dispatch",
                            )
                        recorded_process = read_lane(rt, epoch_id, lane_id).get("process") or {}
                        if recorded_process and any(
                            recorded_process.get(field) != observed.get(field)
                            for field in ("pid", "creation_time")
                        ):
                            raise LaunchError(
                                LAUNCH_DISPATCH_AMBIGUOUS,
                                "a conflicting native controller occupies this lane and run",
                            )
                    else:
                        controller_identity = _lookup_native_invocation(
                            rt, epoch_id, lane,
                            expected_binding=invocation["dispatch_binding"],
                        )
                        if controller_identity is None:
                            raise LaunchError(
                                LAUNCH_DISPATCH_AMBIGUOUS,
                                "dispatch intent has unresolved native ownership; reconcile the exact lane and run before retry",
                            )
                        observed = memory_handoff.native_observation(
                            envelope=memory_envelope,
                            context=final_context,
                            controller_identity=controller_identity,
                        )
                        memory_handoff.record_observed_invocation(
                            worktree_path=lane["worktree_path"],
                            envelope=memory_envelope,
                            observed_invocation=observed,
                        )
                    current_lane = read_lane(rt, epoch_id, lane_id)
                    outcome = _delivered_provider_outcome(
                        current_lane, observed, invocation["dispatch_binding"]
                    )
                    def clear_pending(current: dict[str, Any]) -> dict[str, Any]:
                        if current.get("run_id") != lane["run_id"]:
                            raise LaunchError(
                                LAUNCH_DISPATCH_AMBIGUOUS,
                                "lane run changed during exact dispatch reconciliation",
                            )
                        current_process = current.get("process") or {}
                        if current_process and current_process != {
                            "pid": observed["pid"], "creation_time": observed["creation_time"]
                        }:
                            raise LaunchError(
                                LAUNCH_DISPATCH_AMBIGUOUS,
                                "native controller changed during exact dispatch reconciliation",
                            )
                        return {**current, "launch_pending": False}

                    try:
                        update_lane(rt, epoch_id, lane_id, clear_pending)
                    except LaunchError:
                        raise
                    except Exception as exc:
                        raise LaunchError(
                            LAUNCH_DISPATCH_AMBIGUOUS,
                            "native invocation is delivered but lane persistence is unresolved; reconcile this exact run",
                        ) from exc
                    evidence = [str(memory_handoff.memory_paths(lane["worktree_path"])[0])]
                    if outcome is None:
                        raise LaunchError(
                            LAUNCH_DISPATCH_AMBIGUOUS,
                            "native controller is delivered but provider-start outcome is unresolved; retry exact reconciliation",
                            evidence_paths=evidence,
                        )
                    code, detail = outcome
                    return {
                        "ok": code == "LAUNCH_OK",
                        "code": code,
                        "summary": f"lane {lane_id}: {detail}",
                        "evidence_paths": evidence + [str(current_lane["controller_status_path"])],
                        "next_action": (
                            "continue the existing harness review and cleanup lifecycle"
                            if code == "LAUNCH_OK" else
                            "after exact controller exit and no-provider proof, use resume-lane for a fresh run"
                        ),
                    }
                if launch_deadline is not None and time.monotonic() >= launch_deadline:
                    raise LaunchError(
                        LAUNCH_ALLOWANCE_EXPIRED,
                        "the enclosing allowance expired before dispatch intent; no native controller started",
                    )
                try:
                    supersedes_rejected_attempt_id = memory_handoff.supersession_id_for_launch(
                        worktree_path=lane["worktree_path"], lane=locked_lane,
                        envelope=memory_envelope,
                    )
                    memory_handoff.record_dispatch_intent(
                        worktree_path=lane["worktree_path"], envelope=memory_envelope,
                        supersedes_rejected_attempt_id=supersedes_rejected_attempt_id,
                    )
                except memory_handoff.MemoryHandoffError as exc:
                    raise LaunchError(LAUNCH_DISPATCH_AMBIGUOUS, str(exc)) from exc
                # The allowance is checked immediately before durable intent.
                # Once intent commits, attempt the native spawn even if that
                # commit crossed the deadline: a zero-spawn pending intent has
                # no durable fact from which an exact retry can recover.
                try:
                    child = _spawn_controller_logged(rt, lane, argv, spawn_options)
                except Exception as exc:
                    memory_handoff.record_ambiguous_dispatch(
                        worktree_path=lane["worktree_path"], envelope=memory_envelope
                    )
                    raise LaunchError(
                        LAUNCH_DISPATCH_AMBIGUOUS,
                        "native launch acknowledgement was lost; reconcile exact ownership before retry",
                    ) from exc
                attestation_deadline = time.monotonic() + HANDSHAKE_TIMEOUT_SECONDS
                if launch_deadline is not None:
                    attestation_deadline = min(attestation_deadline, launch_deadline)
                try:
                    controller_identity = _wait_for_spawn_attestation(
                        rt, epoch_id, lane, child,
                        expected_binding=invocation["dispatch_binding"],
                        identity=processes.process_identity(child.pid),
                        deadline=attestation_deadline,
                    )
                except LaunchError:
                    memory_handoff.record_ambiguous_dispatch(
                        worktree_path=lane["worktree_path"], envelope=memory_envelope
                    )
                    raise
                if controller_identity is None:
                    memory_handoff.record_ambiguous_dispatch(
                        worktree_path=lane["worktree_path"], envelope=memory_envelope
                    )
                    raise LaunchError(
                        LAUNCH_DISPATCH_AMBIGUOUS,
                        "native controller identity is unresolved; reconcile exact ownership before retry",
                    )
                try:
                    memory_handoff.record_observed_invocation(
                        worktree_path=lane["worktree_path"],
                        envelope=memory_envelope,
                        observed_invocation=memory_handoff.native_observation(
                            envelope=memory_envelope,
                            context=final_context,
                            controller_identity=controller_identity,
                        ),
                    )
                except Exception as exc:
                    raise LaunchError(
                        LAUNCH_DISPATCH_AMBIGUOUS,
                        "native observation is unresolved; reconcile exact ownership before retry",
                    ) from exc
        else:
            child = _spawn_controller_logged(rt, lane, argv, spawn_options)
            controller_identity = processes.process_identity(child.pid)
            if controller_identity is None:
                child.terminate()
                child.wait(timeout=10.0)
                raise LaunchError(
                    LAUNCH_CONTROLLER_START_FAILED,
                    "cannot record the launched controller process identity",
                )
        def mark_launched(current: dict[str, Any]) -> dict[str, Any]:
            if memory_envelope is not None:
                if current.get("run_id") != lane.get("run_id"):
                    raise LaunchError(
                        LAUNCH_DISPATCH_AMBIGUOUS,
                        "lane run changed after native observation",
                    )
                current_process = current.get("process") or {}
                if current_process and current_process != controller_identity:
                    raise LaunchError(
                        LAUNCH_DISPATCH_AMBIGUOUS,
                        "a conflicting native process owns the observed run",
                    )
            return {
                **current,
                "lifecycle": "running",
                "process": {
                    "pid": controller_identity["pid"],
                    "creation_time": controller_identity["creation_time"],
                },
                "launch_pending": False,
            }

        try:
            lane = update_lane(
                rt,
                epoch_id,
                lane_id,
                mark_launched,
            )
        except LaunchError:
            raise
        except Exception as exc:
            if memory_envelope is not None:
                raise LaunchError(
                    LAUNCH_DISPATCH_AMBIGUOUS,
                    "native invocation is delivered but lane persistence is unresolved; reconcile this exact run",
                    evidence_paths=[str(memory_handoff.memory_paths(lane["worktree_path"])[0])],
                ) from exc
            child.terminate()
            child.wait(timeout=10.0)
            raise LaunchError(
                LAUNCH_CONTROLLER_START_FAILED,
                f"cannot persist the launched controller process identity: {exc}",
            ) from exc
        deadline = time.monotonic() + HANDSHAKE_TIMEOUT_SECONDS
        if launch_deadline is not None:
            # The handshake is a blocking native phase, so it may not outlive
            # the caller's one enclosing allowance.
            deadline = min(deadline, launch_deadline)
        while True:
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
                if memory_envelope is not None and lane.get("run_id") != invocation["run_id"]:
                    raise LaunchError(
                        LAUNCH_DISPATCH_AMBIGUOUS,
                        "lane run changed during native launch handshake",
                    )
                if memory_envelope is not None:
                    outcome = _delivered_provider_outcome(
                        lane, controller_identity, invocation["dispatch_binding"],
                        status=status,
                    )
                    if outcome is None or outcome[0] != "LAUNCH_OK":
                        raise LaunchError(
                            LAUNCH_DISPATCH_AMBIGUOUS,
                            "native controller provider-success proof changed during launch handshake",
                        )
                state = status.get("recorded_status") if terminal else "running"
                return {
                    "ok": True,
                    "code": "LAUNCH_OK",
                    "summary": f"lane {lane_id} reached {state}",
                    "evidence_paths": [str(Path(lane["worktree_path"]) / ".agent-workspace" / "controller.status.json")],
                    "next_action": "wait for the worker result; the monitor reports actionable status",
                }
            if child.poll() is not None or time.monotonic() >= deadline:
                break
            time.sleep(0.2)
        if memory_envelope is not None and child.poll() is None:
            raise LaunchError(
                LAUNCH_DISPATCH_AMBIGUOUS,
                "native controller was delivered but its provider outcome is still pending; reconcile this exact run",
                evidence_paths=[str(memory_handoff.memory_paths(lane["worktree_path"])[0])],
            )
        if memory_envelope is not None:
            outcome = _delivered_provider_outcome(
                read_lane(rt, epoch_id, lane_id),
                controller_identity,
                invocation["dispatch_binding"],
            )
            evidence = [str(Path(lane["controller_status_path"]))]
            if outcome is None:
                raise LaunchError(
                    LAUNCH_DISPATCH_AMBIGUOUS,
                    "native controller was delivered but provider-start outcome is unresolved; reconcile this exact run",
                    evidence_paths=evidence,
                )
            code, summary = outcome
            if code == "LAUNCH_OK":
                return {
                    "ok": True,
                    "code": code,
                    "summary": f"lane {lane_id}: {summary}",
                    "evidence_paths": evidence,
                    "next_action": "continue the existing harness review and cleanup lifecycle",
                }
            if code == LAUNCH_PROVIDER_START_FAILED and _reap_controller_and_prove_exit(
                child, controller_identity
            ):
                _clear_exited_controller_identity(rt, epoch_id, lane_id, controller_identity)
            raise LaunchError(code, summary, evidence_paths=evidence)
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
            "summary": memory_handoff.redact_control_diagnostic(str(exc)),
            "evidence_paths": exc.evidence_paths,
            "next_action": (
                "reconcile this exact dispatch; after proven no-provider exit, use resume-lane for a fresh run"
                if memory_envelope is not None else
                "on LAUNCH_LEASE_BUSY, wait for the holder to finish and re-launch"
            ),
        }
    except Exception as exc:
        return {
            "ok": False,
            "code": LAUNCH_CONTROLLER_START_FAILED,
            "summary": memory_handoff.redact_control_diagnostic(str(exc)),
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
        if not _terminate_lane_processes(lane):
            return {
                "ok": False,
                "code": FORCE_STOP_PROCESS_SURVIVED,
                "summary": "a lane process could not be terminated even forcibly",
                "evidence_paths": [str(Path(lane["worktree_path"]) / ".agent-workspace" / "controller.status.json")],
                "next_action": "escalate to the operator/host; an unkillable process is outside the harness's authority",
            }
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
        if lane.get("memory_plan_state") == "execution_accepted":
            from .review import validate_acceptance_chain

            folder = terminal_evidence.publication_dir(rt, epoch_id, lane)
            expected_ref = folder / "ORCHESTRATOR_ACCEPTANCE.json"
            if Path(acceptance_ref).resolve() != expected_ref.resolve():
                raise LaunchError(RETIRE_ACCEPTANCE_INVALID, "acceptance is not the current run's publication")
            try:
                review = read_record(folder / "COMPLETION_REVIEW.json", COMPLETION_REVIEW_SCHEMA)
                terminal = terminal_evidence.read_terminal_evidence(
                    rt, epoch_id, lane_id, run_id=lane["run_id"],
                )
            except (OSError, ValueError) as exc:
                raise LaunchError(RETIRE_ACCEPTANCE_INVALID, str(exc)) from exc
            if (
                terminal is None
                or not validate_acceptance_chain(
                    review, acceptance, lane_id=lane_id, run_id=lane["run_id"],
                )
                or terminal["review"] != review
                or terminal["acceptance"] != acceptance
            ):
                raise LaunchError(RETIRE_ACCEPTANCE_INVALID, "current run lacks exact valid native evidence")
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
