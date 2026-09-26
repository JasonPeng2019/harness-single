"""``lane launch``, ``lane force-stop``, and ``lane retire``.

Launch consumes the prepared invocation and starts the lane controller (which
starts the provider and owns the leases).  Force-stop is the targeted single-
lane hard stop.  Retire is the graceful end of an accepted lane.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import stat
import subprocess
import time
from pathlib import Path, PurePosixPath
from typing import Any

from . import processes
from .config import find_harness_root, load_config
from .core import content_hash, iso_utc, new_id, read_json, require_schema
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
from .records import atomic_write_json, read_record
from .review import (
    FRESH_LANE_INTEGRITY_DIAGNOSTIC,
    GitStateError,
    lane_integrity_contract_error,
    validate_lane_acceptance_chain,
    validate_invocation_binding,
    validate_merge_ready_git,
)
from .setup import read_runtime_state

INVOCATION_SCHEMA = "controller-invocation/v1"
CONTROLLER_STATUS_SCHEMA = "controller-status/v1"
ACCEPTANCE_SCHEMA = "orchestrator-acceptance/v1"
COMPLETION_REVIEW_SCHEMA = "completion-review/v1"
RESULT_SCHEMA = "result/v1"
TASK_CARD_SCHEMA = "project-task-card/v1"

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
RETIRE_GIT_INVALID = "RETIRE_GIT_INVALID"
RETIRE_ARCHIVE_FAILED = "RETIRE_ARCHIVE_FAILED"
RETIRE_WORKTREE_REMOVE_FAILED = "RETIRE_WORKTREE_REMOVE_FAILED"

RETIREMENT_ARCHIVE_SCHEMA = "retirement-archive/v1"

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
    state = processes.exact_identity_state(pid, creation)
    if state == processes.IDENTITY_GONE_OR_REUSED:
        return True
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        state = processes.exact_identity_state(pid, creation)
        if state == processes.IDENTITY_GONE_OR_REUSED:
            return True
        time.sleep(RETIRE_CONTROLLER_EXIT_POLL_SECONDS)
    return (
        processes.exact_identity_state(pid, creation)
        == processes.IDENTITY_GONE_OR_REUSED
    )


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
            # POSIX can keep the exact child observable as a zombie until its
            # Popen handle is reaped.  The exact termination call may thus
            # time out after delivering its signal; reap first, then reconcile
            # the same PID-plus-creation identity again below.
            processes.terminate_process(pid, creation, force=True)
    try:
        child.wait(timeout=RETIRE_CONTROLLER_EXIT_WAIT_SECONDS)
    except subprocess.TimeoutExpired:
        return False
    return _wait_for_pid_exit(pid, creation, RETIRE_CONTROLLER_EXIT_WAIT_SECONDS)


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
        legacy = lane_integrity_contract_error(lane)
        if legacy is not None:
            raise LaunchError(LAUNCH_INVOCATION_INVALID, legacy)
        if lane.get("lifecycle") not in ("prepared", "running"):
            raise LaunchError(
                LAUNCH_CONTROLLER_START_FAILED,
                f"lane {lane_id} is not prepared (lifecycle={lane.get('lifecycle')})",
            )
        invocation_path = Path(lane["worktree_path"]) / ".agent-workspace" / "invocation.json"
        try:
            invocation = read_record(invocation_path, INVOCATION_SCHEMA)
            validate_invocation_binding(lane, invocation)
        except (OSError, ValueError, GitStateError) as exc:
            raise LaunchError(LAUNCH_INVOCATION_INVALID, str(exc)) from exc
        provider_id = invocation["provider"]["id"]
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
            "next_action": (
                "bootstrap a fresh lane in a fresh epoch"
                if FRESH_LANE_INTEGRITY_DIAGNOSTIC in str(exc)
                else "on LAUNCH_LEASE_BUSY, wait for the holder to finish and re-launch"
            ),
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


def _validate_acceptance_ref(
    path: Path, *, expected_path: Path | None = None
) -> dict[str, Any]:
    try:
        acceptance = read_record(path, ACCEPTANCE_SCHEMA)
    except (OSError, ValueError) as exc:
        raise LaunchError(RETIRE_ACCEPTANCE_INVALID, str(exc)) from exc
    if acceptance.get("content_hash") != content_hash(acceptance):
        raise LaunchError(RETIRE_ACCEPTANCE_INVALID, "acceptance content hash mismatch")
    if acceptance.get("approval") != "ACCEPTED":
        raise LaunchError(RETIRE_ACCEPTANCE_INVALID, "acceptance is not ACCEPTED")
    if expected_path is not None:
        try:
            actual = path.resolve(strict=True)
            expected = expected_path.resolve(strict=True)
        except OSError as exc:
            raise LaunchError(RETIRE_ACCEPTANCE_INVALID, str(exc)) from exc
        if actual != expected:
            raise LaunchError(
                RETIRE_ACCEPTANCE_INVALID,
                f"acceptance ref is not the authoritative lane record: {path}",
            )
    return acceptance


def _validate_retirement_chain(
    rt: Path,
    epoch_id: str,
    lane: dict[str, Any],
    acceptance_path: Path,
    acceptance: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    expected_acceptance = folder / "ORCHESTRATOR_ACCEPTANCE.json"
    acceptance = _validate_acceptance_ref(
        acceptance_path, expected_path=expected_acceptance
    )
    review_path = folder / "COMPLETION_REVIEW.json"
    try:
        review = read_record(review_path, COMPLETION_REVIEW_SCHEMA)
    except (OSError, ValueError) as exc:
        raise LaunchError(RETIRE_ACCEPTANCE_INVALID, str(exc)) from exc
    legacy = lane_integrity_contract_error(lane)
    if legacy is not None:
        raise LaunchError(
            RETIRE_ACCEPTANCE_INVALID,
            legacy,
        )
    if not validate_lane_acceptance_chain(review, acceptance, lane):
        raise LaunchError(
            RETIRE_ACCEPTANCE_INVALID,
            "acceptance does not form the authoritative review/result chain",
        )
    if lane.get("lifecycle") not in {"accepted", "retired"}:
        raise LaunchError(
            RETIRE_ACCEPTANCE_INVALID,
            f"lane is not currently accepted (lifecycle={lane.get('lifecycle')})",
        )
    validation = lane.get("result_validation")
    if not isinstance(validation, dict):
        raise LaunchError(
            RETIRE_ACCEPTANCE_INVALID, "lane has no validated result Git binding"
        )
    return review, acceptance


def _prove_lane_processes_gone(
    lane: dict[str, Any], status: dict[str, Any] | None
) -> tuple[bool, dict[str, Any] | None, dict[str, Any]]:
    process = lane.get("process") or {}
    pid = process.get("pid")
    creation = process.get("creation_time")
    controller_gone = (
        processes.exact_identity_state(pid, creation)
        == processes.IDENTITY_GONE_OR_REUSED
    )
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
        controller_gone = (
            processes.exact_identity_state(pid, creation)
            == processes.IDENTITY_GONE_OR_REUSED
        )
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
        isinstance(boundary, dict) and processes.process_boundary_is_gone(boundary)
    )
    provider_state = (status or {}).get("provider_state") or {}
    provider_pid = provider_state.get("pid")
    provider_creation = provider_state.get("creation_time")
    provider_gone = (
        processes.exact_identity_state(provider_pid, provider_creation)
        == processes.IDENTITY_GONE_OR_REUSED
    )
    proof = {
        "controller_gone": controller_gone,
        "provider_boundary_gone": boundary_gone,
        "provider_gone": provider_gone,
        "cleanup_proven": cleanup_proven,
        "proved_at": iso_utc(),
    }
    return all((controller_gone, cleanup_proven, boundary_gone, provider_gone)), status, proof


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_relative_path(value: object, *, code: str, context: str) -> PurePosixPath:
    """Return one canonical, portable relative path or fail closed."""

    if not isinstance(value, str) or not value or "\\" in value or "\0" in value:
        raise LaunchError(code, f"unsafe {context}: {value!r}")
    relative = PurePosixPath(value)
    if (
        relative.is_absolute()
        or any(part in {"", ".", ".."} for part in relative.parts)
        or relative.as_posix() != value
        or (relative.parts and relative.parts[0].endswith(":"))
    ):
        raise LaunchError(code, f"unsafe {context}: {value!r}")
    return relative


def _path_beneath(
    root: Path,
    relative: PurePosixPath,
    *,
    code: str,
    context: str,
) -> Path:
    candidate = root.joinpath(*relative.parts)
    try:
        candidate.resolve(strict=False).relative_to(root.resolve())
    except (OSError, ValueError) as exc:
        raise LaunchError(code, f"{context} escapes its root: {relative}") from exc
    current = root
    for part in relative.parts[:-1]:
        current /= part
        if not os.path.lexists(current):
            continue
        try:
            info = current.lstat()
        except OSError as exc:
            raise LaunchError(code, f"cannot inspect {context}: {current}: {exc}") from exc
        if not stat.S_ISDIR(info.st_mode):
            raise LaunchError(
                code, f"{context} crosses a symlink or special directory: {current}"
            )
    return candidate


def _require_regular_file(path: Path, *, code: str, context: str) -> os.stat_result:
    try:
        info = path.lstat()
    except OSError as exc:
        raise LaunchError(code, f"cannot inspect {context}: {path}: {exc}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise LaunchError(code, f"{context} is not a regular file: {path}")
    return info


def _copy_regular_file(source: Path, destination: Path, *, context: str) -> None:
    """Copy one non-symlink regular file while binding the opened inode."""

    before = _require_regular_file(
        source, code=RETIRE_ARCHIVE_FAILED, context=context
    )
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(source, flags)
    except OSError as exc:
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED, f"cannot open {context}: {source}: {exc}"
        ) from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise LaunchError(
                RETIRE_ARCHIVE_FAILED,
                f"{context} changed identity while it was being archived: {source}",
            )
        destination.parent.mkdir(parents=True, exist_ok=True)
        with os.fdopen(descriptor, "rb", closefd=False) as source_handle:
            with destination.open("xb") as destination_handle:
                shutil.copyfileobj(source_handle, destination_handle)
    except LaunchError:
        raise
    except OSError as exc:
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED, f"cannot archive {context}: {source}: {exc}"
        ) from exc
    finally:
        os.close(descriptor)


def _tracked_inventory(
    worktree: Path, *, code: str = RETIRE_ARCHIVE_FAILED
) -> tuple[set[str], set[str]]:
    completed = subprocess.run(
        ["git", "-C", str(worktree), "ls-files", "--stage", "-z"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        raise LaunchError(
            code,
            completed.stderr.strip() or "cannot inventory tracked worktree paths",
        )
    tracked: set[str] = set()
    gitlinks: set[str] = set()
    for entry in completed.stdout.split("\0"):
        if not entry:
            continue
        try:
            header, path = entry.split("\t", 1)
            mode, _object_id, _stage = header.split(" ", 2)
        except ValueError as exc:
            raise LaunchError(code, "cannot parse the Git index inventory") from exc
        tracked.add(path)
        if mode == "160000":
            gitlinks.add(path)
    return tracked, gitlinks


def _tracked_paths(
    worktree: Path, *, code: str = RETIRE_ARCHIVE_FAILED
) -> set[str]:
    return _tracked_inventory(worktree, code=code)[0]


def _worktree_archive_sources(
    lane: dict[str, Any],
) -> tuple[dict[str, Path], dict[str, str]]:
    """Inventory every removable regular file, including ignored files."""

    worktree = Path(lane["worktree_path"])
    tracked, gitlinks = _tracked_inventory(worktree)
    owned = (lane.get("git") or {}).get("harness_owned_paths")
    if not isinstance(owned, list):
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED, "lane lacks its harness-owned path inventory"
        )
    exact_owned: set[str] = set()
    for value in owned:
        if value == ".agent-workspace/**":
            continue
        relative = _safe_relative_path(
            value,
            code=RETIRE_ARCHIVE_FAILED,
            context="harness-owned archive path",
        )
        exact_owned.add(relative.as_posix())

    sources: dict[str, Path] = {}
    worktree_files: dict[str, str] = {}

    # A populated Gitlink is another repository, not disposable untracked
    # content in this worktree.  Never descend into, archive, or delete it.
    # Fail before any retirement copy/removal so the operator can deinitialize
    # it explicitly and retry without damage.
    for gitlink in sorted(gitlinks):
        relative = _safe_relative_path(
            gitlink,
            code=RETIRE_ARCHIVE_FAILED,
            context="Gitlink path",
        )
        path = _path_beneath(
            worktree,
            relative,
            code=RETIRE_ARCHIVE_FAILED,
            context="Gitlink path",
        )
        if not os.path.lexists(path):
            continue
        try:
            info = path.lstat()
        except OSError as exc:
            raise LaunchError(
                RETIRE_ARCHIVE_FAILED, f"cannot inspect Gitlink {gitlink}: {exc}"
            ) from exc
        if not stat.S_ISDIR(info.st_mode):
            raise LaunchError(
                RETIRE_ARCHIVE_FAILED,
                f"Gitlink path is not a real directory: {gitlink}",
            )
        try:
            populated = next(path.iterdir(), None) is not None
        except OSError as exc:
            raise LaunchError(
                RETIRE_ARCHIVE_FAILED, f"cannot inspect Gitlink {gitlink}: {exc}"
            ) from exc
        if populated:
            raise LaunchError(
                RETIRE_ARCHIVE_FAILED,
                "initialized Gitlink prevents safe retirement; deinitialize it "
                f"and retry: {gitlink}",
            )

    def walk_error(exc: OSError) -> None:
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED, f"cannot inventory worktree: {exc}"
        ) from exc

    for current, directories, filenames in os.walk(
        worktree, followlinks=False, onerror=walk_error
    ):
        current_path = Path(current)
        if current_path == worktree and ".git" in directories:
            directories.remove(".git")
        for name in list(directories):
            child = current_path / name
            relative = child.relative_to(worktree).as_posix()
            if relative in gitlinks:
                directories.remove(name)
                continue
            try:
                info = child.lstat()
            except OSError as exc:
                raise LaunchError(
                    RETIRE_ARCHIVE_FAILED,
                    f"cannot inspect worktree directory {relative}: {exc}",
                ) from exc
            if stat.S_ISDIR(info.st_mode):
                continue
            directories.remove(name)
            if relative not in tracked:
                raise LaunchError(
                    RETIRE_ARCHIVE_FAILED,
                    f"untracked or ignored worktree directory is a symlink or special file: {relative}",
                )
        for name in filenames:
            source = current_path / name
            relative = source.relative_to(worktree).as_posix()
            if relative == ".git":
                continue
            _require_regular_file(
                source,
                code=RETIRE_ARCHIVE_FAILED,
                context=f"removable worktree file {relative}",
            )
            safe_source = _safe_relative_path(
                relative,
                code=RETIRE_ARCHIVE_FAILED,
                context="removable worktree path",
            )
            if relative.startswith(".agent-workspace/") or relative == "RESULT.json":
                archive_name = relative
            elif relative in exact_owned:
                archive_name = f"harness-owned/{relative}"
            elif relative in tracked:
                archive_name = f"worktree-tracked/{relative}"
            else:
                archive_name = f"worktree-untracked/{relative}"
            safe_archive = _safe_relative_path(
                archive_name,
                code=RETIRE_ARCHIVE_FAILED,
                context="worktree archive path",
            ).as_posix()
            _path_beneath(
                worktree,
                safe_source,
                code=RETIRE_ARCHIVE_FAILED,
                context="removable worktree path",
            )
            if safe_archive in sources:
                raise LaunchError(
                    RETIRE_ARCHIVE_FAILED,
                    f"duplicate worktree archive destination: {safe_archive}",
                )
            sources[safe_archive] = source
            worktree_files[relative] = safe_archive
    return sources, worktree_files


def _archive_tree_names(target: Path) -> set[str]:
    """Return every regular archive file and reject non-regular entries."""

    names: set[str] = set()
    def walk_error(exc: OSError) -> None:
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED, f"cannot inventory retirement archive: {exc}"
        ) from exc

    for current, directories, filenames in os.walk(
        target, followlinks=False, onerror=walk_error
    ):
        current_path = Path(current)
        for name in directories:
            child = current_path / name
            try:
                info = child.lstat()
            except OSError as exc:
                raise LaunchError(
                    RETIRE_ARCHIVE_FAILED,
                    f"cannot inspect retirement archive directory: {child}: {exc}",
                ) from exc
            if not stat.S_ISDIR(info.st_mode):
                raise LaunchError(
                    RETIRE_ARCHIVE_FAILED,
                    f"retirement archive contains a symlink or special directory: {child}",
                )
        for name in filenames:
            archived = current_path / name
            _require_regular_file(
                archived,
                code=RETIRE_ARCHIVE_FAILED,
                context="retirement archive evidence",
            )
            relative = archived.relative_to(target).as_posix()
            safe = _safe_relative_path(
                relative,
                code=RETIRE_ARCHIVE_FAILED,
                context="retirement archive inventory path",
            )
            names.add(safe.as_posix())
    return names


def _read_semantic_archive_record(
    target: Path,
    name: str,
    schema: str,
    *,
    require_content_hash: bool = True,
) -> dict[str, Any]:
    path = target.joinpath(*PurePosixPath(name).parts)
    try:
        record = read_record(path, schema)
    except (OSError, ValueError) as exc:
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED, f"invalid archived {name}: {exc}"
        ) from exc
    if require_content_hash and record.get("content_hash") != content_hash(record):
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED, f"archived {name} content hash mismatch"
        )
    return record


def _validate_archived_authoritative_records(
    target: Path,
    lane: dict[str, Any],
    review: dict[str, Any],
    acceptance: dict[str, Any],
    manifest: dict[str, Any],
) -> None:
    archived_result = _read_semantic_archive_record(
        target, "RESULT.json", RESULT_SCHEMA
    )
    archived_task = _read_semantic_archive_record(
        target,
        ".agent-workspace/task-card.json",
        TASK_CARD_SCHEMA,
        require_content_hash=False,
    )
    archived_invocation = _read_semantic_archive_record(
        target, ".agent-workspace/invocation.json", INVOCATION_SCHEMA
    )
    archived_review = _read_semantic_archive_record(
        target, "COMPLETION_REVIEW.json", COMPLETION_REVIEW_SCHEMA
    )
    archived_acceptance = _read_semantic_archive_record(
        target, "ORCHESTRATOR_ACCEPTANCE.json", ACCEPTANCE_SCHEMA
    )

    if archived_review != review or archived_acceptance != acceptance:
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED,
            "archived review/acceptance differs from the authoritative pair",
        )
    folder = target.parent
    for name in ("COMPLETION_REVIEW.json", "ORCHESTRATOR_ACCEPTANCE.json"):
        current_path = folder / name
        _require_regular_file(
            current_path,
            code=RETIRE_ARCHIVE_FAILED,
            context=f"authoritative {name}",
        )
        if _sha256_file(target / name) != _sha256_file(current_path):
            raise LaunchError(
                RETIRE_ARCHIVE_FAILED,
                f"archived {name} bytes differ from the authoritative record",
            )
    if not validate_lane_acceptance_chain(
        archived_review, archived_acceptance, lane
    ):
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED,
            "archived review/acceptance no longer binds to the accepted lane",
        )
    if (
        archived_result.get("lane_id") != lane.get("lane_id")
        or archived_result.get("run_id") != lane.get("run_id")
        or archived_result.get("content_hash") != review.get("result_hash")
        or archived_result.get("outcome") not in {"PASS", "FAIL", "BLOCKED"}
        or not isinstance(archived_result.get("summary"), str)
        or not archived_result["summary"].strip()
        or not isinstance(archived_result.get("evidence"), list)
        or not isinstance(archived_result.get("completed_at"), str)
        or not archived_result["completed_at"].strip()
    ):
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED,
            "archived RESULT.json does not match the accepted result chain",
        )
    task_hash = content_hash(archived_task)
    task_id = str(
        archived_task.get("card_id") or archived_task.get("id") or task_hash
    )
    if (
        task_hash != review.get("task_card_hash")
        or task_hash != lane.get("task_card_hash")
        or task_id != review.get("task_card_id")
        or not isinstance(archived_task.get("task"), str)
        or not archived_task["task"].strip()
    ):
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED,
            "archived task card does not match the accepted task identity",
        )
    if (
        archived_invocation.get("lane_id") != lane.get("lane_id")
        or archived_invocation.get("run_id") != lane.get("run_id")
        or archived_invocation.get("provider") != lane.get("provider")
        or archived_invocation.get("git") != lane.get("git")
        or archived_invocation.get("content_hash") != lane.get("invocation_hash")
    ):
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED,
            "archived invocation does not match the accepted lane identity",
        )
    expected_manifest = {
        "acceptance_hash": acceptance.get("content_hash"),
        "review_hash": review.get("content_hash"),
        "result_hash": review.get("result_hash"),
        "task_card_hash": review.get("task_card_hash"),
        "invocation_hash": review.get("invocation_hash"),
        "commit": review.get("commit"),
    }
    if any(manifest.get(key) != value for key, value in expected_manifest.items()):
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED,
            "retirement manifest hashes do not match the authoritative chain",
        )


def _read_retirement_archive(
    target: Path,
    lane: dict[str, Any],
    review: dict[str, Any],
    acceptance: dict[str, Any],
) -> dict[str, Any]:
    try:
        target_info = target.lstat()
    except OSError as exc:
        raise LaunchError(RETIRE_ARCHIVE_FAILED, str(exc)) from exc
    if not stat.S_ISDIR(target_info.st_mode):
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED,
            f"retirement archive is not a real directory: {target}",
        )
    manifest_path = target / "RETIREMENT_ARCHIVE.json"
    _require_regular_file(
        manifest_path,
        code=RETIRE_ARCHIVE_FAILED,
        context="retirement archive manifest",
    )
    try:
        manifest = read_record(manifest_path, RETIREMENT_ARCHIVE_SCHEMA)
    except (OSError, ValueError) as exc:
        raise LaunchError(RETIRE_ARCHIVE_FAILED, str(exc)) from exc
    if (
        manifest.get("content_hash") != content_hash(manifest)
        or manifest.get("lane_id") != lane["lane_id"]
        or manifest.get("run_id") != lane["run_id"]
        or manifest.get("acceptance_hash") != acceptance.get("content_hash")
        or manifest.get("invocation_hash") != lane.get("invocation_hash")
        or manifest.get("commit") != review.get("commit")
    ):
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED,
            "existing retirement archive does not match this accepted lane",
        )
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED, "retirement archive file inventory is invalid"
        )
    required = {
        ".agent-workspace/task-card.json",
        ".agent-workspace/invocation.json",
        ".agent-workspace/controller.status.json",
        ".agent-workspace/controller.events.jsonl",
        ".agent-workspace/provider-transcript.jsonl",
        ".agent-workspace/provider-stderr.txt",
        ".agent-workspace/controller.attempts.jsonl",
        "RESULT.json",
        "COMPLETION_REVIEW.json",
        "ORCHESTRATOR_ACCEPTANCE.json",
        "lane.json",
    }
    if not required.issubset(files):
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED,
            "retirement archive is missing required evidence inventory entries",
        )
    for name, metadata in files.items():
        relative = _safe_relative_path(
            name,
            code=RETIRE_ARCHIVE_FAILED,
            context="retirement archive inventory path",
        )
        if relative.as_posix() == "RETIREMENT_ARCHIVE.json":
            raise LaunchError(
                RETIRE_ARCHIVE_FAILED,
                "retirement archive manifest cannot inventory itself",
            )
        archived = _path_beneath(
            target,
            relative,
            code=RETIRE_ARCHIVE_FAILED,
            context="retirement archive inventory path",
        )
        info = _require_regular_file(
            archived,
            code=RETIRE_ARCHIVE_FAILED,
            context="retirement archive evidence",
        )
        if (
            not isinstance(metadata, dict)
            or metadata.get("sha256") != _sha256_file(archived)
            or metadata.get("size") != info.st_size
        ):
            raise LaunchError(
                RETIRE_ARCHIVE_FAILED,
                f"retirement archive file failed hash validation: {name}",
            )
    actual_names = _archive_tree_names(target)
    expected_names = set(files) | {"RETIREMENT_ARCHIVE.json"}
    if actual_names != expected_names:
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED,
            "retirement archive contents do not exactly match its file inventory",
        )
    worktree_files = manifest.get("worktree_files")
    if (
        manifest.get("complete_worktree_inventory") is not True
        or not isinstance(worktree_files, dict)
        or not worktree_files
    ):
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED,
            "retirement archive lacks its complete worktree inventory",
        )
    for original, archived_name in worktree_files.items():
        _safe_relative_path(
            original,
            code=RETIRE_ARCHIVE_FAILED,
            context="retirement worktree inventory path",
        )
        safe_archived = _safe_relative_path(
            archived_name,
            code=RETIRE_ARCHIVE_FAILED,
            context="retirement worktree archive path",
        ).as_posix()
        if safe_archived not in files:
            raise LaunchError(
                RETIRE_ARCHIVE_FAILED,
                f"worktree inventory references missing archive file: {safe_archived}",
            )
    _validate_archived_authoritative_records(
        target, lane, review, acceptance, manifest
    )
    return manifest


def _archive_retirement_evidence(
    rt: Path,
    epoch_id: str,
    lane: dict[str, Any],
    review: dict[str, Any],
    acceptance: dict[str, Any],
    git_proof: dict[str, Any],
    process_proof: dict[str, Any],
) -> Path:
    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    target = folder / "retirement-archive"
    if os.path.lexists(target):
        _read_retirement_archive(target, lane, review, acceptance)
        return target

    worktree = Path(lane["worktree_path"])
    sources, worktree_files = _worktree_archive_sources(lane)
    _tracked, gitlinks = _tracked_inventory(worktree)
    sources.update({
        "RESULT.json": worktree / "RESULT.json",
        "COMPLETION_REVIEW.json": folder / "COMPLETION_REVIEW.json",
        "ORCHESTRATOR_ACCEPTANCE.json": folder / "ORCHESTRATOR_ACCEPTANCE.json",
    })
    required_workspace = {
        ".agent-workspace/task-card.json",
        ".agent-workspace/invocation.json",
        ".agent-workspace/controller.status.json",
        ".agent-workspace/controller.events.jsonl",
        ".agent-workspace/provider-transcript.jsonl",
        ".agent-workspace/provider-stderr.txt",
        ".agent-workspace/controller.attempts.jsonl",
    }
    missing = sorted(required_workspace.difference(sources))
    missing.extend(
        name
        for name in (
            "RESULT.json",
            "COMPLETION_REVIEW.json",
            "ORCHESTRATOR_ACCEPTANCE.json",
        )
        if name not in sources
        or not sources[name].exists()
    )
    if missing:
        raise LaunchError(
            RETIRE_ARCHIVE_FAILED,
            "required retirement evidence is missing: " + ", ".join(missing),
        )
    temp = folder / f".retirement-archive.{new_id()}.tmp"
    try:
        temp.mkdir(parents=False, exist_ok=False)
        files: dict[str, dict[str, Any]] = {}
        for name, source in sources.items():
            relative = _safe_relative_path(
                name,
                code=RETIRE_ARCHIVE_FAILED,
                context="retirement archive path",
            )
            destination = _path_beneath(
                temp,
                relative,
                code=RETIRE_ARCHIVE_FAILED,
                context="retirement archive path",
            )
            _copy_regular_file(source, destination, context=f"retirement evidence {name}")
            files[name] = {
                "sha256": _sha256_file(destination),
                "size": destination.stat().st_size,
            }
        atomic_write_json(temp / "lane.json", lane)
        files["lane.json"] = {
            "sha256": _sha256_file(temp / "lane.json"),
            "size": (temp / "lane.json").stat().st_size,
        }
        manifest = {
            "schema": RETIREMENT_ARCHIVE_SCHEMA,
            "lane_id": lane["lane_id"],
            "run_id": lane["run_id"],
            "acceptance_hash": acceptance["content_hash"],
            "review_hash": review["content_hash"],
            "result_hash": review["result_hash"],
            "task_card_hash": review["task_card_hash"],
            "invocation_hash": review["invocation_hash"],
            "commit": review["commit"],
            "git_proof": git_proof,
            "process_proof": process_proof,
            "discarded_harness_paths": list(
                (lane.get("git") or {}).get("harness_owned_paths") or []
            ),
            "result_evidence": read_json(sources["RESULT.json"]).get("evidence", []),
            "complete_worktree_inventory": True,
            "gitlinks": sorted(gitlinks),
            "worktree_files": worktree_files,
            "files": files,
            "archived_at": iso_utc(),
        }
        manifest["content_hash"] = content_hash(manifest)
        atomic_write_json(temp / "RETIREMENT_ARCHIVE.json", manifest)
        _read_retirement_archive(temp, lane, review, acceptance)
        os.replace(temp, target)
    except LaunchError:
        raise
    except Exception as exc:
        raise LaunchError(RETIRE_ARCHIVE_FAILED, str(exc)) from exc
    finally:
        if temp.exists():
            shutil.rmtree(temp, ignore_errors=True)
    return target


def _cleanup_inventory(lane: dict[str, Any]) -> tuple[Path, list[PurePosixPath]]:
    worktree = Path(lane["worktree_path"])
    owned = (lane.get("git") or {}).get("harness_owned_paths")
    if not isinstance(owned, list) or ".agent-workspace/**" not in owned or "RESULT.json" not in owned:
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "harness-owned cleanup inventory is missing or unsafe",
        )
    exact: list[PurePosixPath] = []
    for value in owned:
        if value == ".agent-workspace/**":
            continue
        relative = _safe_relative_path(
            value,
            code=RETIRE_WORKTREE_REMOVE_FAILED,
            context="harness-owned cleanup path",
        )
        _path_beneath(
            worktree,
            relative,
            code=RETIRE_WORKTREE_REMOVE_FAILED,
            context="harness-owned cleanup path",
        )
        exact.append(relative)
    return worktree, exact


def _prove_cleanup_paths_untracked(
    worktree: Path, exact: list[PurePosixPath]
) -> None:
    completed = subprocess.run(
        ["git", "-C", str(worktree), "ls-files", "--cached", "-z"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            completed.stderr.strip() or "cannot inspect tracked cleanup paths",
        )
    exact_names = {item.as_posix() for item in exact}
    selected = sorted(
        path
        for path in completed.stdout.split("\0")
        if path
        and (
            path == ".agent-workspace"
            or path.startswith(".agent-workspace/")
            or path in exact_names
        )
    )
    if selected:
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "refusing to delete tracked harness-owned paths: "
            + ", ".join(selected[:10]),
        )


def _remove_archived_harness_artifacts(
    lane: dict[str, Any], manifest: dict[str, Any]
) -> None:
    worktree, _ = _cleanup_inventory(lane)
    worktree_files = manifest.get("worktree_files")
    if not isinstance(worktree_files, dict):
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "archive lacks the removable worktree inventory",
        )
    try:
        current_sources, current_mapping = _worktree_archive_sources(lane)
    except LaunchError as exc:
        raise LaunchError(RETIRE_WORKTREE_REMOVE_FAILED, str(exc)) from exc
    if current_mapping != worktree_files:
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "worktree files changed after retirement evidence was archived",
        )
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED, "archive file inventory is invalid"
        )
    for original, archived_name in current_mapping.items():
        source = current_sources[str(archived_name)]
        metadata = files.get(archived_name)
        if (
            not isinstance(metadata, dict)
            or metadata.get("sha256") != _sha256_file(source)
            or metadata.get("size") != source.stat().st_size
        ):
            raise LaunchError(
                RETIRE_WORKTREE_REMOVE_FAILED,
                f"worktree file changed after archival: {original}",
            )
    workspace = worktree / ".agent-workspace"
    if os.path.lexists(workspace):
        try:
            workspace_info = workspace.lstat()
        except OSError as exc:
            raise LaunchError(RETIRE_WORKTREE_REMOVE_FAILED, str(exc)) from exc
        if not stat.S_ISDIR(workspace_info.st_mode):
            raise LaunchError(
                RETIRE_WORKTREE_REMOVE_FAILED,
                "refusing to remove a symlink or special .agent-workspace",
            )
    candidates: list[Path] = []
    for original in current_mapping:
        relative = _safe_relative_path(
            original,
            code=RETIRE_WORKTREE_REMOVE_FAILED,
            context="archived worktree cleanup path",
        )
        candidate = worktree.joinpath(*relative.parts)
        if not os.path.lexists(candidate):
            continue
        try:
            info = candidate.lstat()
        except OSError as exc:
            raise LaunchError(RETIRE_WORKTREE_REMOVE_FAILED, str(exc)) from exc
        if not stat.S_ISREG(info.st_mode):
            raise LaunchError(
                RETIRE_WORKTREE_REMOVE_FAILED,
                f"refusing to remove a symlink or special harness-owned path: {relative}",
            )
        candidates.append(candidate)

    # This check is deliberately immediately before the first unlink/rmtree.
    # A clean tracked file would otherwise only be noticed after its content
    # had already been deleted by the cleanup below.
    _prove_cleanup_paths_untracked(
        worktree,
        [
            _safe_relative_path(
                item,
                code=RETIRE_WORKTREE_REMOVE_FAILED,
                context="archived worktree cleanup path",
            )
            for item in current_mapping
        ],
    )

    for candidate in candidates:
        candidate.unlink()
        parent = candidate.parent
        while parent != worktree:
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent
    if os.path.lexists(workspace):
        for current, directories, _files in os.walk(
            workspace, topdown=False, followlinks=False
        ):
            for name in directories:
                (Path(current) / name).rmdir()
        workspace.rmdir()


def _worktree_registered(root_workspace: Path, worktree: Path) -> bool:
    completed = subprocess.run(
        ["git", "-C", str(root_workspace), "worktree", "list", "--porcelain"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            completed.stderr.strip() or "cannot inspect Git worktree registry",
        )
    expected = os.path.normcase(str(worktree.resolve()))
    return any(
        line.startswith("worktree ")
        and os.path.normcase(str(Path(line[9:]).resolve())) == expected
        for line in completed.stdout.splitlines()
    )


def _prove_retained_branch_tip(
    root_workspace: Path, lane: dict[str, Any], accepted_commit: str
) -> None:
    """Prove the retained branch still names the accepted commit exactly."""

    branch = (lane.get("git") or {}).get("branch")
    if not isinstance(branch, str) or not branch:
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "lane has no authoritative retained branch",
        )
    completed = subprocess.run(
        [
            "git",
            "-C",
            str(root_workspace),
            "show-ref",
            "--verify",
            "--hash",
            f"refs/heads/{branch}",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    actual = completed.stdout.strip()
    if completed.returncode != 0 or actual != accepted_commit:
        detail = completed.stderr.strip() or actual or "missing"
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "retained lane branch changed from the accepted commit: "
            f"expected {accepted_commit}, got {detail}",
        )


def _quarantine_path(lane: dict[str, Any]) -> Path:
    worktree = Path(lane["worktree_path"])
    token = hashlib.sha256(
        f"{lane.get('lane_id')}\0{lane.get('run_id')}".encode("utf-8")
    ).hexdigest()[:16]
    return worktree.parent / f".{worktree.name}.retirement-{token}"


def _capture_linked_gitfile(worktree: Path, lane: dict[str, Any]) -> dict[str, Any]:
    gitfile = worktree / ".git"
    info = _require_regular_file(
        gitfile,
        code=RETIRE_WORKTREE_REMOVE_FAILED,
        context="linked-worktree .git file",
    )
    try:
        value = gitfile.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise LaunchError(RETIRE_WORKTREE_REMOVE_FAILED, str(exc)) from exc
    if not value.startswith("gitdir: "):
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED, "linked-worktree .git file is invalid"
        )
    admin = Path(value[8:])
    if not admin.is_absolute():
        admin = worktree / admin
    admin = admin.resolve()
    common = Path(str((lane.get("git") or {}).get("common_dir") or "")).resolve()
    try:
        admin.relative_to(common / "worktrees")
    except ValueError as exc:
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "linked-worktree administration path is outside Git common state",
        ) from exc
    return {
        "dev": info.st_dev,
        "ino": info.st_ino,
        "sha256": _sha256_file(gitfile),
        "admin_path": str(admin),
    }


def _gitlinks_from_manifest(manifest: dict[str, Any]) -> set[str]:
    values = manifest.get("gitlinks", [])
    if not isinstance(values, list):
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED, "retirement Gitlink inventory is invalid"
        )
    result: set[str] = set()
    for value in values:
        result.add(
            _safe_relative_path(
                value,
                code=RETIRE_WORKTREE_REMOVE_FAILED,
                context="retirement Gitlink path",
            ).as_posix()
        )
    return result


def _filesystem_worktree_files(root: Path, gitlinks: set[str]) -> dict[str, Path]:
    """Inventory regular files without following Gitlinks or symlinks."""

    files: dict[str, Path] = {}

    def walk_error(exc: OSError) -> None:
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED, f"cannot inventory quarantined worktree: {exc}"
        ) from exc

    for current, directories, filenames in os.walk(
        root, followlinks=False, onerror=walk_error
    ):
        current_path = Path(current)
        for name in list(directories):
            child = current_path / name
            relative = child.relative_to(root).as_posix()
            try:
                info = child.lstat()
            except OSError as exc:
                raise LaunchError(RETIRE_WORKTREE_REMOVE_FAILED, str(exc)) from exc
            if not stat.S_ISDIR(info.st_mode):
                raise LaunchError(
                    RETIRE_WORKTREE_REMOVE_FAILED,
                    f"quarantined worktree contains a symlink or special directory: {relative}",
                )
            if relative in gitlinks:
                directories.remove(name)
                try:
                    populated = next(child.iterdir(), None) is not None
                except OSError as exc:
                    raise LaunchError(RETIRE_WORKTREE_REMOVE_FAILED, str(exc)) from exc
                if populated:
                    raise LaunchError(
                        RETIRE_WORKTREE_REMOVE_FAILED,
                        "initialized Gitlink prevents safe retirement: " + relative,
                    )
        for name in filenames:
            path = current_path / name
            relative = path.relative_to(root).as_posix()
            if relative == ".git":
                continue
            _require_regular_file(
                path,
                code=RETIRE_WORKTREE_REMOVE_FAILED,
                context=f"quarantined worktree file {relative}",
            )
            files[relative] = path
    return files


def _validate_quarantine_inventory(
    root: Path,
    manifest: dict[str, Any],
    *,
    allow_missing: bool,
) -> dict[str, Path]:
    mapping = manifest.get("worktree_files")
    archive_files = manifest.get("files")
    if not isinstance(mapping, dict) or not isinstance(archive_files, dict):
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED, "archive worktree inventory is invalid"
        )
    current = _filesystem_worktree_files(root, _gitlinks_from_manifest(manifest))
    expected = set(mapping)
    if set(current) - expected or (not allow_missing and set(current) != expected):
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "quarantined worktree contents changed after archival",
        )
    for original, path in current.items():
        archived_name = mapping.get(original)
        metadata = archive_files.get(archived_name)
        if (
            not isinstance(archived_name, str)
            or not isinstance(metadata, dict)
            or metadata.get("sha256") != _sha256_file(path)
            or metadata.get("size") != path.stat().st_size
        ):
            raise LaunchError(
                RETIRE_WORKTREE_REMOVE_FAILED,
                f"quarantined worktree file changed after archival: {original}",
            )
    return current


def _plan_worktree_quarantine(
    root_workspace: Path,
    lane: dict[str, Any],
    manifest: dict[str, Any],
    accepted_commit: str,
) -> dict[str, Any]:
    worktree = Path(lane["worktree_path"])
    quarantine = _quarantine_path(lane)
    if os.path.lexists(quarantine):
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            f"retirement quarantine path already exists: {quarantine}",
        )
    source_root = Path(str((lane.get("git") or {}).get("source_root") or ""))
    if source_root.resolve() != root_workspace.resolve():
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "lane source root does not match configured root workspace",
        )
    try:
        final_git = validate_merge_ready_git(lane)
    except GitStateError as exc:
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            f"final worktree identity check failed: {exc}",
        ) from exc
    if (
        final_git.get("branch") != (lane.get("git") or {}).get("branch")
        or final_git.get("commit") != accepted_commit
        or final_git.get("common_dir")
        != str(Path(str((lane.get("git") or {}).get("common_dir"))).resolve())
    ):
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "worktree branch, commit, or common directory changed before quarantine",
        )
    _validate_quarantine_inventory(worktree, manifest, allow_missing=False)
    _prove_retained_branch_tip(root_workspace, lane, accepted_commit)
    return {
        "path": str(quarantine),
        "gitfile": _capture_linked_gitfile(worktree, lane),
    }


def _validate_quarantine_gitfile(
    quarantine: Path, identity: Any, *, allow_missing: bool
) -> Path | None:
    if not isinstance(identity, dict):
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "retirement quarantine lacks its .git file identity",
        )
    gitfile = quarantine / ".git"
    if not os.path.lexists(gitfile):
        if allow_missing:
            return None
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED, "quarantined .git file is missing"
        )
    info = _require_regular_file(
        gitfile,
        code=RETIRE_WORKTREE_REMOVE_FAILED,
        context="quarantined .git file",
    )
    if (
        (info.st_dev, info.st_ino) != (identity.get("dev"), identity.get("ino"))
        or _sha256_file(gitfile) != identity.get("sha256")
    ):
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "quarantined .git file identity changed",
        )
    return gitfile


def _remove_quarantine_directories(root: Path) -> None:
    directories: list[Path] = []
    for current, children, _files in os.walk(root, topdown=False, followlinks=False):
        current_path = Path(current)
        for name in children:
            child = current_path / name
            info = child.lstat()
            if not stat.S_ISDIR(info.st_mode):
                raise LaunchError(
                    RETIRE_WORKTREE_REMOVE_FAILED,
                    f"refusing to remove special quarantined path: {child}",
                )
            directories.append(child)
    for directory in sorted(directories, key=lambda item: len(item.parts), reverse=True):
        try:
            directory.rmdir()
        except OSError as exc:
            raise LaunchError(
                RETIRE_WORKTREE_REMOVE_FAILED,
                f"late content remains in retirement quarantine: {directory}: {exc}",
            ) from exc
    try:
        root.rmdir()
    except OSError as exc:
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            f"late content remains in retirement quarantine: {root}: {exc}",
        ) from exc


def _remove_exact_worktree(
    root_workspace: Path,
    lane: dict[str, Any],
    manifest: dict[str, Any],
    accepted_commit: str,
) -> None:
    """Quarantine, verify, and remove one worktree without recursive deletion."""

    worktree = Path(lane["worktree_path"])
    retirement = lane.get("retirement") or {}
    quarantine = Path(str(retirement.get("quarantine_path") or ""))
    if quarantine != _quarantine_path(lane):
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "recorded retirement quarantine path is not authoritative",
        )
    worktree_present = os.path.lexists(worktree)
    quarantine_present = os.path.lexists(quarantine)
    if worktree_present and quarantine_present:
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "both public worktree and retirement quarantine exist",
        )
    if worktree_present:
        # Re-prove immediately before the atomic move.  The planning proof can
        # be stale if retirement was interrupted before this call.
        planned = _plan_worktree_quarantine(
            root_workspace, lane, manifest, accepted_commit
        )
        if planned.get("gitfile") != retirement.get("gitfile"):
            raise LaunchError(
                RETIRE_WORKTREE_REMOVE_FAILED,
                "linked-worktree identity changed after quarantine planning",
            )
        before = worktree.lstat()
        try:
            os.rename(worktree, quarantine)
        except OSError as exc:
            raise LaunchError(RETIRE_WORKTREE_REMOVE_FAILED, str(exc)) from exc
        after = quarantine.lstat()
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            raise LaunchError(
                RETIRE_WORKTREE_REMOVE_FAILED,
                "retirement quarantine did not preserve directory identity",
            )
    elif not quarantine_present:
        if not _worktree_registered(root_workspace, worktree):
            _prove_retained_branch_tip(root_workspace, lane, accepted_commit)
            return
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "lane worktree and its retirement quarantine are both missing",
        )

    removed_any = False
    try:
        allow_missing = retirement.get("phase") == "worktree_removal_started"
        current = _validate_quarantine_inventory(
            quarantine, manifest, allow_missing=allow_missing
        )
        gitfile = _validate_quarantine_gitfile(
            quarantine, retirement.get("gitfile"), allow_missing=allow_missing
        )
        _prove_retained_branch_tip(root_workspace, lane, accepted_commit)

        # The public path is absent after rename, so pruning unregisters only
        # this now-missing worktree.  No recursive Git removal is used.
        _prune_worktrees(root_workspace)
        if _worktree_registered(root_workspace, worktree):
            raise LaunchError(
                RETIRE_WORKTREE_REMOVE_FAILED,
                "Git retained the quarantined worktree registration",
            )
        _prove_retained_branch_tip(root_workspace, lane, accepted_commit)

        # Re-inventory after pruning and before the first unlink.  A late file
        # is never selected for deletion and prevents directory removal.
        current = _validate_quarantine_inventory(
            quarantine, manifest, allow_missing=allow_missing
        )
        for relative in sorted(current):
            path = current[relative]
            _validate_quarantine_inventory(
                quarantine, manifest, allow_missing=True
            )
            try:
                path.unlink()
            except OSError as exc:
                raise LaunchError(RETIRE_WORKTREE_REMOVE_FAILED, str(exc)) from exc
            removed_any = True
        if gitfile is not None:
            _validate_quarantine_gitfile(
                quarantine, retirement.get("gitfile"), allow_missing=False
            )
            gitfile.unlink()
            removed_any = True
        _remove_quarantine_directories(quarantine)
    except Exception:
        # Before registry pruning/file removal the exact directory can be put
        # back at its public path.  Once removal began, preserve the quarantine
        # and every late/changed file for a truthful retry.
        if (
            not removed_any
            and os.path.lexists(quarantine)
            and not os.path.lexists(worktree)
            and _worktree_registered(root_workspace, worktree)
        ):
            try:
                os.rename(quarantine, worktree)
            except OSError:
                pass
        raise
    if os.path.lexists(worktree) or os.path.lexists(quarantine):
        raise LaunchError(
            RETIRE_WORKTREE_REMOVE_FAILED,
            "retirement quarantine was not removed completely",
        )
    _prove_retained_branch_tip(root_workspace, lane, accepted_commit)


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
        acceptance_path = Path(acceptance_ref)
        acceptance = _validate_acceptance_ref(acceptance_path)
        lane_id = str(acceptance["lane_id"])
        epoch_id, lane = find_active_lane(rt, lane_id)
        review, acceptance = _validate_retirement_chain(
            rt, epoch_id, lane, acceptance_path, acceptance
        )
        phase = ((lane.get("retirement") or {}).get("phase"))
        archive_path = lane_record_dir(rt, epoch_id, lane_id) / "retirement-archive"
        archived_manifest: dict[str, Any] | None = None
        if phase in {
            "archive_complete",
            "quarantine_planned",
            "worktree_removal_started",
            "worktree_removed",
            "finalized",
        }:
            archived_manifest = _read_retirement_archive(
                archive_path, lane, review, acceptance
            )
        if phase in {
            "quarantine_planned",
            "worktree_removal_started",
            "worktree_removed",
            "finalized",
        }:
            process_proof = archived_manifest.get("process_proof") if archived_manifest else None
            gone = bool(
                isinstance(process_proof, dict)
                and process_proof.get("controller_gone") is True
                and process_proof.get("provider_boundary_gone") is True
                and process_proof.get("provider_gone") is True
                and process_proof.get("cleanup_proven") is True
            )
            status = None
        else:
            status = _read_controller_status(lane)
            gone, status, process_proof = _prove_lane_processes_gone(lane, status)
        if not gone:
            return {
                "ok": False,
                "code": RETIRE_CLEANUP_UNPROVEN,
                "summary": "lane cleanup is not proven; force-stop the lane first",
                "evidence_paths": [str(Path(lane["worktree_path"]) / ".agent-workspace" / "controller.status.json")],
                "next_action": "run `lane force-stop --lane-id <id>` then treat as done",
            }

        if phase not in {"worktree_removed", "finalized"}:
            validation = lane.get("result_validation") or {}
            worktree = Path(lane["worktree_path"])
            quarantine = _quarantine_path(lane)
            if worktree.exists() and phase not in {
                "quarantine_planned",
                "worktree_removal_started",
            }:
                try:
                    git_proof = validate_merge_ready_git(lane)
                except GitStateError as exc:
                    raise LaunchError(RETIRE_GIT_INVALID, str(exc)) from exc
                if (
                    git_proof.get("branch") != validation.get("branch")
                    or git_proof.get("commit") != validation.get("commit")
                    or review.get("commit") != git_proof.get("commit")
                ):
                    raise LaunchError(
                        RETIRE_GIT_INVALID,
                        "accepted commit is not the lane's current clean branch tip",
                    )
                archive_path = _archive_retirement_evidence(
                    rt,
                    epoch_id,
                    lane,
                    review,
                    acceptance,
                    git_proof,
                    process_proof,
                )
                archived_manifest = _read_retirement_archive(
                    archive_path, lane, review, acceptance
                )
                if phase not in {"archive_complete", "worktree_removal_started"}:
                    lane = update_lane(
                        rt,
                        epoch_id,
                        lane_id,
                        lambda current: {
                            **current,
                            "retirement": {
                                "phase": "archive_complete",
                                "archive_path": str(archive_path),
                                "acceptance_hash": acceptance["content_hash"],
                                "commit": review["commit"],
                                "updated_at": iso_utc(),
                            },
                        },
                    )
            elif not archive_path.is_dir():
                raise LaunchError(
                    RETIRE_ARCHIVE_FAILED,
                    "lane worktree moved before retirement evidence was archived",
                )

            if archived_manifest is None:
                archived_manifest = _read_retirement_archive(
                    archive_path, lane, review, acceptance
                )
            phase = str((lane.get("retirement") or {}).get("phase") or phase or "")
            if phase not in {"quarantine_planned", "worktree_removal_started"}:
                plan = _plan_worktree_quarantine(
                    config.root_workspace,
                    lane,
                    archived_manifest,
                    str(review["commit"]),
                )
                lane = update_lane(
                    rt,
                    epoch_id,
                    lane_id,
                    lambda current, value=plan: {
                        **current,
                        "retirement": {
                            **(current.get("retirement") or {}),
                            "phase": "quarantine_planned",
                            "quarantine_path": value["path"],
                            "gitfile": value["gitfile"],
                            "updated_at": iso_utc(),
                        },
                    },
                )
            if (lane.get("retirement") or {}).get("phase") != "worktree_removal_started":
                lane = update_lane(
                    rt,
                    epoch_id,
                    lane_id,
                    lambda current: {
                        **current,
                        "retirement": {
                            **(current.get("retirement") or {}),
                            "phase": "worktree_removal_started",
                            "updated_at": iso_utc(),
                        },
                    },
                )
            if worktree.exists() or os.path.lexists(quarantine):
                _remove_exact_worktree(
                    config.root_workspace,
                    lane,
                    archived_manifest,
                    str(review["commit"]),
                )
            elif _worktree_registered(
                config.root_workspace, worktree
            ):
                raise LaunchError(
                    RETIRE_WORKTREE_REMOVE_FAILED,
                    "lane worktree disappeared but remains registered",
                )
            _prove_retained_branch_tip(
                config.root_workspace, lane, str(review["commit"])
            )
            lane = update_lane(
                rt,
                epoch_id,
                lane_id,
                lambda current: {
                    **current,
                    "retirement": {
                        **(current.get("retirement") or {}),
                        "phase": "worktree_removed",
                        "updated_at": iso_utc(),
                    },
                },
            )
        elif archive_path != Path(str((lane.get("retirement") or {}).get("archive_path") or "")):
            raise LaunchError(
                RETIRE_ARCHIVE_FAILED, "recorded retirement archive path is not authoritative"
            )

        # A retry after successful worktree deletion must re-prove the retained
        # branch before releasing leases or removing the active-lane entry.
        _prove_retained_branch_tip(
            config.root_workspace, lane, str(review["commit"])
        )

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
        update_lane(
            rt,
            epoch_id,
            lane_id,
            lambda current: {
                **current,
                "lifecycle": "retired",
                "process": {},
                "retirement": {
                    **(current.get("retirement") or {}),
                    "phase": "finalized",
                    "updated_at": iso_utc(),
                },
            },
        )
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
        "evidence_paths": [str(Path(acceptance_ref)), str(archive_path)],
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
