"""The one public launcher: ``python -m orchestrator_harness.operator_launch``.

Every public command returns the small structured result ``{ ok, code,
summary, evidence_paths, next_action }``.  Success prints a short human
status line on stdout and exits 0; failure prints one stable failure code and
a message on stderr and exits non-zero.  ``--json`` emits the machine-readable
result object instead.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import iso_utc
from .processes import targeted_process_query

from . import bootstrap, launch, resume, review, scan_watch, setup, shutdown
from .config import find_harness_root, load_config
from .lanes import find_active_lane
from .manager_queue import (
    MANAGER_ACK_ALREADY_ACKNOWLEDGED,
    MANAGER_ACK_EVENT_NOT_FOUND,
    MANAGER_ACK_NOT_ROOT_EVENT,
    MANAGER_CLOSE_ALREADY_CLOSED,
    MANAGER_CLOSE_INVALID_OUTCOME,
    MANAGER_CLOSE_NOT_ACKNOWLEDGED,
    ManagerQueueError,
    acknowledge_event,
    append_assignment,
    close_event,
    read_manager_queue,
)
from .records import read_record

SEND_LANE_NOT_FOUND = "SEND_LANE_NOT_FOUND"
SEND_LANE_NOT_MANAGED = "SEND_LANE_NOT_MANAGED"
SEND_LANE_NOT_RUNNING = "SEND_LANE_NOT_RUNNING"
SEND_LANE_WRITE_FAILED = "SEND_LANE_WRITE_FAILED"


def _emit(result: dict[str, Any], *, as_json: bool) -> int:
    """Print one structured result and return the process exit code."""
    if as_json:
        sys.stdout.write(json.dumps(result, sort_keys=True, indent=2) + "\n")
    elif result.get("ok"):
        sys.stdout.write(str(result.get("summary", "")).strip() + "\n")
    else:
        sys.stderr.write(f"{result.get('code', 'FAILED')}: {result.get('summary', '')}\n")
    return 0 if result.get("ok") else 1


def _manager_acknowledge(event_id: str) -> dict[str, Any]:
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": MANAGER_ACK_EVENT_NOT_FOUND,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "fix the configuration and re-run setup",
        }
    rt = config.runtime_root
    try:
        queue = read_manager_queue(rt)
        event = next(
            (item for item in queue.get("events", []) if item.get("event_id") == event_id),
            None,
        )
        if event is None:
            raise ManagerQueueError(
                MANAGER_ACK_EVENT_NOT_FOUND, f"event not found: {event_id}"
            )
        if not event.get("lane_id") or not event.get("run_id"):
            raise ManagerQueueError(
                MANAGER_ACK_NOT_ROOT_EVENT,
                f"event {event_id} is not a valid ROOT event",
            )
        acknowledge_event(rt, event_id)
    except ManagerQueueError as exc:
        return {
            "ok": False,
            "code": exc.code,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "handle the event, then `manager close`",
        }
    except Exception as exc:
        return {
            "ok": False,
            "code": MANAGER_ACK_EVENT_NOT_FOUND,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "resolve the error and retry",
        }
    return {
        "ok": True,
        "code": "MANAGER_ACK_OK",
        "summary": f"event {event_id} acknowledged",
        "evidence_paths": [],
        "next_action": "handle the event, then `manager close`",
    }


def _manager_close(
    event_id: str, outcome: str, summary: str | None
) -> dict[str, Any]:
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": MANAGER_CLOSE_NOT_ACKNOWLEDGED,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "fix the configuration and re-run setup",
        }
    rt = config.runtime_root
    try:
        queue = read_manager_queue(rt)
        event = next(
            (item for item in queue.get("events", []) if item.get("event_id") == event_id),
            None,
        )
        if event is None:
            raise ManagerQueueError(
                MANAGER_CLOSE_NOT_ACKNOWLEDGED, f"event not found: {event_id}"
            )
        if not event.get("lane_id") or not event.get("run_id"):
            raise ManagerQueueError(
                MANAGER_CLOSE_NOT_ACKNOWLEDGED,
                f"event {event_id} is not a valid ROOT event",
            )
        close_event(rt, event_id, outcome, summary=summary)
    except ManagerQueueError as exc:
        return {
            "ok": False,
            "code": exc.code,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "closing the event does not clear the underlying condition",
        }
    except Exception as exc:
        return {
            "ok": False,
            "code": MANAGER_CLOSE_NOT_ACKNOWLEDGED,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "resolve the error and retry",
        }
    return {
        "ok": True,
        "code": "MANAGER_CLOSE_OK",
        "summary": f"event {event_id} closed as {outcome}",
        "evidence_paths": [],
        "next_action": "none",
    }


def _send_lane_notification(lane_id: str, prompt: str) -> dict[str, Any]:
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": SEND_LANE_NOT_FOUND,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "fix the configuration and re-run setup",
        }
    rt = config.runtime_root
    try:
        _epoch_id, lane = find_active_lane(rt, lane_id)
    except Exception as exc:
        return {
            "ok": False,
            "code": SEND_LANE_NOT_FOUND,
            "summary": f"lane not found: {lane_id}",
            "evidence_paths": [],
            "next_action": "check the lane id",
        }
    if not lane.get("incoming_queue_path"):
        return {
            "ok": False,
            "code": SEND_LANE_NOT_MANAGED,
            "summary": f"lane {lane_id} is not a managed lane",
            "evidence_paths": [],
            "next_action": "use a managed lane for ROOT-to-worker assignments",
        }
    from . import processes

    process = lane.get("process") or {}
    if lane.get("lifecycle") != "running" or not processes.identity_matches(
        process.get("pid"), process.get("creation_time")
    ):
        return {
            "ok": False,
            "code": SEND_LANE_NOT_RUNNING,
            "summary": f"lane {lane_id} is not running",
            "evidence_paths": [],
            "next_action": "launch the lane before messaging it",
        }
    try:
        assignment = append_assignment(rt, lane, prompt)
    except ManagerQueueError as exc:
        return {
            "ok": False,
            "code": SEND_LANE_WRITE_FAILED,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "resolve the error and retry",
        }
    return {
        "ok": True,
        "code": "SEND_LANE_OK",
        "summary": f"assignment {assignment['event_id']} appended to lane {lane_id}",
        "evidence_paths": [str(lane["incoming_queue_path"])],
        "next_action": "the worker acts via its lane-assignment skill",
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="operator_launch",
        description="The harness v2 public launcher.",
    )
    parser.add_argument("--json", action="store_true", help="emit the machine-readable result object")
    subparsers = parser.add_subparsers(dest="command", required=True)

    harness = subparsers.add_parser("harness", help="runtime lifecycle commands")
    harness_sub = harness.add_subparsers(dest="harness_command", required=True)
    setup_parser = harness_sub.add_parser("setup", help="one-time idempotent integration")
    setup_parser.add_argument("--overwrite", action="store_true")
    harness_sub.add_parser("shutdown", help="end the whole runtime")

    lane = subparsers.add_parser("lane", help="lane lifecycle commands")
    lane_sub = lane.add_subparsers(dest="lane_command", required=True)
    bootstrap_parser = lane_sub.add_parser("bootstrap", help="prepare one lane")
    bootstrap_parser.add_argument("--lane-id", required=True)
    bootstrap_parser.add_argument("--provider", required=True)
    bootstrap_parser.add_argument("--model", required=True)
    bootstrap_parser.add_argument("--exclusive-resource", action="append", default=[])
    bootstrap_parser.add_argument("--task-card", required=True)
    launch_parser = lane_sub.add_parser("launch", help="start one prepared lane")
    launch_parser.add_argument("--lane-id", required=True)
    review_parser = lane_sub.add_parser("completion-review", help="record ROOT's review and acceptance")
    review_selector = review_parser.add_mutually_exclusive_group(required=True)
    review_selector.add_argument("--event-id")
    review_selector.add_argument("--lane-id")
    review_parser.add_argument("--review-outcome", required=True, choices=["PASS", "FAIL", "BLOCKED"])
    review_parser.add_argument("--approval", required=True, choices=["ACCEPTED", "REJECTED"])
    review_parser.add_argument("--review-summary", required=True)
    review_parser.add_argument("--evidence", action="append", default=[])
    review_parser.add_argument("--force-accept", action="store_true")
    review_parser.add_argument("--force-reason")
    force_stop_parser = lane_sub.add_parser("force-stop", help="hard-stop one stuck lane")
    force_stop_parser.add_argument("--lane-id", required=True)
    retire_parser = lane_sub.add_parser("retire", help="gracefully retire one accepted lane")
    retire_parser.add_argument("--acceptance-ref", required=True)

    resume_parser = subparsers.add_parser("resume-lane", help="re-run a stopped, unaccepted lane")
    resume_parser.add_argument("--lane-id", required=True)
    resume_parser.add_argument("--resume-task-card", required=True)
    resume_parser.add_argument("--rationale")

    manager = subparsers.add_parser("manager", help="manager-queue commands (managed)")
    manager_sub = manager.add_subparsers(dest="manager_command", required=True)
    ack_parser = manager_sub.add_parser("acknowledge", help="move one event PENDING -> ACKNOWLEDGED")
    ack_parser.add_argument("--event-id", required=True)
    close_parser = manager_sub.add_parser("close", help="close an acknowledged event")
    close_parser.add_argument("--event-id", required=True)
    close_parser.add_argument("--outcome", required=True, choices=["COMPLETE", "BLOCKED"])
    close_parser.add_argument("--summary")

    send_parser = subparsers.add_parser("send-lane-notification", help="append one assignment to a running managed lane")
    send_parser.add_argument("--lane-id", required=True)
    send_parser.add_argument("--prompt", required=True)

    scan_parser = subparsers.add_parser("scan", help="read-only lane-status snapshot")
    scan_parser.add_argument("--no-write", action="store_true")
    watch_parser = subparsers.add_parser("watch", help="block until an actionable condition exists")
    watch_parser.add_argument("--until-actionable", action="store_true")
    watch_parser.add_argument("--timeout")
    watch_parser.add_argument("--until-event")

    health = subparsers.add_parser("health", help="health commands")
    health_sub = health.add_subparsers(dest="health_command", required=True)
    health_sub.add_parser("reconcile", help="rebuild active-lanes and re-derive status")

    return parser


def _dispatch(args: argparse.Namespace) -> dict[str, Any]:
    command = args.command
    if command == "harness":
        if args.harness_command == "setup":
            return setup.run_setup(overwrite=args.overwrite)
        if args.harness_command == "shutdown":
            return shutdown.run_shutdown()
        raise ValueError(f"unknown harness command: {args.harness_command}")
    if command == "lane":
        if args.lane_command == "bootstrap":
            return bootstrap.run_bootstrap(
                lane_id=args.lane_id,
                provider=args.provider,
                model=args.model,
                exclusive_resources=list(args.exclusive_resource),
                task_card_path=args.task_card,
            )
        if args.lane_command == "launch":
            return launch.run_launch(args.lane_id)
        if args.lane_command == "completion-review":
            return review.run_completion_review(
                event_id=args.event_id,
                lane_id=args.lane_id,
                review_outcome=args.review_outcome,
                approval=args.approval,
                review_summary=args.review_summary,
                evidence=list(args.evidence),
                force_accept=args.force_accept,
                force_reason=args.force_reason,
            )
        if args.lane_command == "force-stop":
            return launch.run_force_stop(args.lane_id)
        if args.lane_command == "retire":
            return launch.run_retire(args.acceptance_ref)
        raise ValueError(f"unknown lane command: {args.lane_command}")
    if command == "resume-lane":
        return resume.run_resume(
            lane_id=args.lane_id,
            resume_task_card=args.resume_task_card,
            rationale=args.rationale,
        )
    if command == "manager":
        if args.manager_command == "acknowledge":
            return _manager_acknowledge(args.event_id)
        if args.manager_command == "close":
            return _manager_close(args.event_id, args.outcome, args.summary)
        raise ValueError(f"unknown manager command: {args.manager_command}")
    if command == "send-lane-notification":
        return _send_lane_notification(args.lane_id, args.prompt)
    if command == "scan":
        return scan_watch.run_scan()
    if command == "watch":
        return scan_watch.run_watch(timeout=args.timeout, until_event=args.until_event)
    if command == "health":
        if args.health_command == "reconcile":
            return scan_watch.run_health_reconcile()
        raise ValueError(f"unknown health command: {args.health_command}")
    raise ValueError(f"unknown command: {command}")


def _launch_cli_main(argv: Sequence[str]) -> int:
    """Legacy detached-launch CLI: ``--receipt``-driven operator launch."""
    args = _parser().parse_args(argv)
    command = list(args.argv)
    if command[:1] == ["--"]:
        command = command[1:]
    try:
        result = launch_process(
            receipt=args.receipt,
            label=args.label,
            role=args.role,
            cwd=args.cwd,
            argv=command,
            expected_state_path=args.expected_state_path,
        )
    except Exception as exc:
        print(json.dumps({"launched": False, "error": str(exc)}), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if "--receipt" in argv:
        return _launch_cli_main(argv)
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        result = _dispatch(args)
    except Exception as exc:
        result = {
            "ok": False,
            "code": "LAUNCHER_FAILED",
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "resolve the error and retry",
        }
    return _emit(result, as_json=args.json)


_RECEIPT_SCHEMA = "orchestrator-operator-launch/v1"
_DETACHED_RECORDS_LOCK = threading.Lock()
_DETACHED_RECORDS: dict[tuple[int, str], dict[str, Any]] = {}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _validate_path(value: str, *, directory: bool) -> Path:
    path = Path(value).resolve(strict=False)
    if directory:
        if not path.is_dir():
            raise ValueError("cwd must name an existing directory")
    elif path.exists():
        raise ValueError("receipt already exists")
    if not path.parent.is_dir():
        raise ValueError("receipt parent must exist")
    return path


def _reserve_receipt(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise ValueError("receipt already exists") from exc
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump({"schema": _RECEIPT_SCHEMA, "status": "reserving"}, handle)
        handle.flush()
        os.fsync(handle.fileno())


def _finalize(path: Path, receipt: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(receipt, sort_keys=True, indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink(missing_ok=True)


def _creation_identity(
    pid: int, *, attempts: int = 12, delay_seconds: float = 0.05
) -> str | None:
    for _ in range(attempts):
        query = targeted_process_query(pid)
        item = query.process if query.complete else None
        if item is not None and item.created_utc is not None:
            return iso_utc(item.created_utc)
        time.sleep(delay_seconds)
    return None


def detached_owner_snapshot() -> list[dict[str, Any]]:
    """Return the observability ledger of live detached processes.

    This is not a reaper ledger: Windows ownership is a native detached
    process whose handle is closed after publication, and POSIX ownership is
    handed to the system reaper by the double-fork boundary.  The ledger only
    reports which exact PID/creation identities are still observable, so tests
    and operators can confirm natural completion without any Python owner.
    """

    with _DETACHED_RECORDS_LOCK:
        records = tuple(_DETACHED_RECORDS.items())
    live: list[dict[str, Any]] = []
    stale: list[tuple[int, str]] = []
    for key, record in records:
        query = targeted_process_query(record["pid"])
        observed = query.process
        if observed is None:
            stale.append(key)
            continue
        if (
            observed.created_utc is None
            or iso_utc(observed.created_utc) != record["created_utc"]
        ):
            stale.append(key)
            continue
        live.append(dict(record, state="live"))
    if stale:
        with _DETACHED_RECORDS_LOCK:
            for key in stale:
                _DETACHED_RECORDS.pop(key, None)
    return live


def _cleanup_exact_posix(pid: int, created_utc: str) -> tuple[bool, str | None]:
    """Clean up one exact POSIX identity during a pre-publication failure."""

    try:
        query = targeted_process_query(pid)
        if query.process is None:
            return True, None
        if (
            query.process.created_utc is None
            or iso_utc(query.process.created_utc) != created_utc
        ):
            return False, "POSIX process identity was reused"
        os.kill(pid, 15)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            current = targeted_process_query(pid)
            if current.process is None:
                return True, None
            if (
                current.process.created_utc is None
                or iso_utc(current.process.created_utc) != created_utc
            ):
                return False, "POSIX process identity changed during cleanup"
            time.sleep(0.05)
        return False, "POSIX process remained live after exact cleanup"
    except Exception as exc:
        return False, str(exc)


def _spawn_posix_detached(
    argv: Sequence[str], cwd: Path, environment: dict[str, str]
) -> int:
    """Spawn through a native double-fork handoff.

    The short intermediary is synchronously reaped so it cannot become a
    zombie.  The controller grandchild is adopted by the system reaper after
    the route entry exits, which is the lifetime needed by the public API.
    The close-on-exec pipe gives the caller a deterministic exec-failure
    signal without retaining a Python child handle.
    """

    read_fd, write_fd = os.pipe()
    os.set_inheritable(write_fd, False)
    try:
        intermediary = os.fork()
    except BaseException:
        os.close(read_fd)
        os.close(write_fd)
        raise
    if intermediary == 0:
        try:
            os.close(read_fd)
            os.setsid()
            child = os.fork()
            if child == 0:
                try:
                    os.chdir(cwd)
                    os.execvpe(argv[0], list(argv), environment)
                except BaseException as exc:

                    try:
                        os.write(
                            write_fd,
                            f"ERROR {type(exc).__name__}: {exc}".encode(
                                "utf-8", "replace"
                            ),
                        )
                    except OSError:
                        pass
                    os._exit(127)
            os.write(write_fd, f"{child}\n".encode("ascii"))
        except BaseException as exc:
            try:
                os.write(
                    write_fd,
                    f"ERROR {type(exc).__name__}: {exc}".encode("utf-8", "replace"),
                )
            except OSError:
                pass
        finally:
            os.close(write_fd)
            os._exit(0)
    os.close(write_fd)
    try:
        _, status = os.waitpid(intermediary, 0)
        payload = bytearray()
        while True:
            chunk = os.read(read_fd, 4096)
            if not chunk:
                break
            payload.extend(chunk)
        if not os.waitstatus_to_exitcode(status) == 0:
            raise RuntimeError("POSIX detached intermediary failed")
        text = bytes(payload).decode("utf-8", "replace")
        if "ERROR " in text:
            raise RuntimeError(text.strip())
        first = text.splitlines()[0] if text.splitlines() else ""
        if not first.isdigit() or int(first) <= 0:
            raise RuntimeError("POSIX detached handoff did not publish a child PID")
        return int(first)
    finally:
        os.close(read_fd)


def _create_windows_native(
    argv: Sequence[str],
    cwd: Path,
    environment: dict[str, str] | None,
) -> tuple[object, object, int, str | None, int]:
    """Create a suspended, detached Windows process without constructing Popen."""

    import _winapi  # type: ignore[import-not-found]

    flags = (
        0x00000004  # CREATE_SUSPENDED; exposed inconsistently by Python's _winapi
        | _winapi.DETACHED_PROCESS
        | _winapi.CREATE_NEW_PROCESS_GROUP
        | _winapi.CREATE_NO_WINDOW
        | _winapi.CREATE_BREAKAWAY_FROM_JOB
        | 0x00000400  # CREATE_UNICODE_ENVIRONMENT
    )
    startup = subprocess.STARTUPINFO()
    process_handle = None
    thread_handle = None
    pid: int | None = None
    created_utc: str | None = None
    command_line = subprocess.list2cmdline(list(argv))

    def remember_attempt(exc: OSError) -> None:
        setattr(exc, "_operator_launch_creationflags", flags)
        setattr(
            exc,
            "_operator_launch_ownership_strategy",
            (
                "windows-native-detached-inherited-job-no-wait"
                if (flags & _winapi.CREATE_BREAKAWAY_FROM_JOB) == 0
                else "windows-native-detached-no-wait"
            ),
        )

    try:
        process_handle, thread_handle, pid, _thread_id = _winapi.CreateProcess(
            None,
            command_line,
            None,
            None,
            False,
            flags,
            environment,
            str(cwd),
            startup,
        )
    except OSError as exc:
        remember_attempt(exc)
        if getattr(exc, "winerror", None) != 5:
            raise
        flags &= ~_winapi.CREATE_BREAKAWAY_FROM_JOB
        try:
            process_handle, thread_handle, pid, _thread_id = (
                _winapi.CreateProcess(
                    None,
                    command_line,
                    None,
                    None,
                    False,
                    flags,
                    environment,
                    str(cwd),
                    startup,
                )
            )
        except OSError as retry_exc:
            remember_attempt(retry_exc)
            raise
    created_utc = _creation_identity(pid)
    assert pid is not None
    assert process_handle is not None and thread_handle is not None
    return process_handle, thread_handle, pid, created_utc, flags


def _terminate_windows_exact(
    process_handle: object, pid: int
) -> tuple[bool, str | None]:
    import _winapi  # type: ignore[import-not-found]

    try:
        _winapi.TerminateProcess(process_handle, 1)
        result = _winapi.WaitForSingleObject(process_handle, 5000)
        if result != _winapi.WAIT_OBJECT_0:
            return False, "Windows exact process handle did not signal"
        query = targeted_process_query(pid)
        return (
            query.process is None,
            None if query.process is None else "Windows process remained observable",
        )
    except Exception as exc:
        return False, str(exc)


def _resume_windows_thread(thread_handle: object) -> None:
    """Resume one exact native thread handle."""

    import ctypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.ResumeThread.argtypes = [ctypes.c_void_p]
    kernel32.ResumeThread.restype = ctypes.c_uint32
    result = kernel32.ResumeThread(ctypes.c_void_p(int(thread_handle)))
    if result == 0xFFFFFFFF:
        error = ctypes.get_last_error()

        raise OSError(error, "ResumeThread failed")


def launch_process(
    *,
    receipt: str | Path,
    label: str,
    role: str,
    cwd: str | Path,
    argv: Sequence[str],
    expected_state_path: str | Path | None = None,
    environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Launch one detached manager-owned process and atomically prove its identity.

    The Windows path is native and publishes a suspended process only after
    its exact creation identity is recorded.  The POSIX path performs a
    double-fork handoff and synchronously reaps only the short intermediary;
    the long-lived controller is naturally reaped by the system owner.
    """
    if (
        not isinstance(label, str)
        or not label.strip()
        or not isinstance(role, str)
        or not role.strip()
    ):
        raise ValueError("label and role are required")
    if not argv or any(not isinstance(item, str) or not item for item in argv):
        raise ValueError("argv must contain nonempty strings")
    receipt_path = _validate_path(str(receipt), directory=False)
    cwd_path = _validate_path(str(cwd), directory=True)
    expected = (
        Path(expected_state_path).resolve(strict=False) if expected_state_path else None
    )
    _reserve_receipt(receipt_path)
    launched_at = _utc_now()
    if environment is not None and any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in environment.items()
    ):
        raise ValueError("environment keys and values must be strings")
    inherited_environment = dict(os.environ if environment is None else environment)
    flags = 0
    platform = "windows" if os.name == "nt" else "posix"
    process_handle: object | None = None
    thread_handle: object | None = None
    pid: int | None = None
    created_utc: str | None = None
    ownership_strategy = (
        "windows-native-detached-no-wait"
        if os.name == "nt"
        else "posix-double-fork-system-reaped"
    )
    try:
        if os.name == "nt":
            import _winapi  # type: ignore[import-not-found]

            process_handle, thread_handle, pid, created_utc, flags = (
                _create_windows_native(argv, cwd_path, inherited_environment)
            )
            if (flags & _winapi.CREATE_BREAKAWAY_FROM_JOB) == 0:
                ownership_strategy = "windows-native-detached-inherited-job-no-wait"
        else:
            pid = _spawn_posix_detached(argv, cwd_path, inherited_environment)
            created_utc = _creation_identity(pid)
        if pid is None or created_utc is None:
            raise RuntimeError("child identity could not be proved live")
        result = {
            "schema": _RECEIPT_SCHEMA,
            "status": "launched",
            "label": label,
            "role": role,
            "argv": list(argv),
            "cwd": str(cwd_path),
            "pid": pid,
            "created_utc": created_utc,
            "launched_utc": iso_utc(launched_at),
            "platform": platform,
            "creationflags": flags,
            "ownership_strategy": ownership_strategy,
            "expected_state_path": str(expected) if expected else None,
        }
        result["pid"] = pid
        _finalize(receipt_path, result)
        with _DETACHED_RECORDS_LOCK:
            _DETACHED_RECORDS[(pid, created_utc)] = {
                "pid": pid,
                "created_utc": created_utc,
                "label": label,
                "role": role,
                "ownership_strategy": ownership_strategy,
            }
        if os.name == "nt":
            if process_handle is None or thread_handle is None:
                raise RuntimeError("Windows native launch handles are unavailable")
            import _winapi  # type: ignore[import-not-found]

            _resume_windows_thread(thread_handle)
            _winapi.CloseHandle(thread_handle)
            _winapi.CloseHandle(process_handle)
            thread_handle = None
            process_handle = None
        return result
    except Exception as exc:
        if os.name == "nt":
            flags = getattr(exc, "_operator_launch_creationflags", flags)
            ownership_strategy = getattr(
                exc, "_operator_launch_ownership_strategy", ownership_strategy
            )
        cleanup_confirmed: bool | None = None
        cleanup_error: str | None = None
        if os.name == "nt" and process_handle is not None and pid is not None:
            cleanup_confirmed, cleanup_error = _terminate_windows_exact(
                process_handle, pid
            )
        elif pid is not None and created_utc is not None:
            cleanup_confirmed, cleanup_error = _cleanup_exact_posix(pid, created_utc)
        if os.name == "nt":
            import _winapi  # type: ignore[import-not-found]

            if thread_handle is not None:
                _winapi.CloseHandle(thread_handle)
            if process_handle is not None:
                _winapi.CloseHandle(process_handle)
        if pid is not None and created_utc is not None:
            with _DETACHED_RECORDS_LOCK:
                _DETACHED_RECORDS.pop((pid, created_utc), None)
        failure = {
            "schema": _RECEIPT_SCHEMA,
            "status": "failed",
            "label": label,
            "role": role,
            "argv": list(argv),
            "cwd": str(cwd_path),
            "launched_utc": iso_utc(launched_at),
            "platform": platform,
            "creationflags": flags,
            "error": str(exc),
            "ownership_strategy": ownership_strategy,
            "child_pid": pid,
            "cleanup_confirmed": cleanup_confirmed,
            "cleanup_error": cleanup_error,
        }
        _finalize(receipt_path, failure)
        if cleanup_confirmed is False:
            raise RuntimeError(
                f"{exc}; exact child cleanup could not be confirmed: {cleanup_error or 'child remained live'}"
            ) from exc
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Launch one detached manager-owned process"
    )
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--role", required=True)
    parser.add_argument("--cwd", required=True)
    parser.add_argument("--expected-state-path")
    parser.add_argument("argv", nargs=argparse.REMAINDER)
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
