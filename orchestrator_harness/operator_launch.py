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
import sys
from typing import Any

from . import bootstrap, launch, resume, review, scan_watch, setup, shutdown
from .config import find_harness_root, load_config, load_resource_manifest
from .lanes import LaneError, find_active_lane
from .leases import (
    FORCE_RELEASE_CONFIG_INVALID,
    FORCE_RELEASE_DELETE_FAILED,
    FORCE_RELEASE_HOLDER_LIVE,
    FORCE_RELEASE_HOLDER_UNPROVEN,
    FORCE_RELEASE_LEASE_INVALID,
    FORCE_RELEASE_LEASE_MISSING,
    FORCE_RELEASE_OK,
    FORCE_RELEASE_UNDECLARED,
    LeaseError,
    force_release_lease,
    lease_path,
    read_lease,
)
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


def _lease_force_release(resource_id: str) -> dict[str, Any]:
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
        manifest = load_resource_manifest(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": FORCE_RELEASE_CONFIG_INVALID,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "fix the configuration and re-run setup",
        }
    if not manifest.is_declared(resource_id):
        return {
            "ok": False,
            "code": FORCE_RELEASE_UNDECLARED,
            "summary": f"resource not declared: {resource_id}",
            "evidence_paths": [],
            "next_action": "declare the resource in resource-manifest.json",
        }
    rt = config.runtime_root
    lane: dict[str, Any] | None = None
    lease = read_lease(rt, resource_id)
    if lease is not None:
        lane_id = lease.get("lane_id")
        if isinstance(lane_id, str) and lane_id:
            try:
                _epoch_id, lane = find_active_lane(rt, lane_id)
            except LaneError:
                lane = None
    try:
        force_release_lease(rt, resource_id, lane=lane)
    except LeaseError as exc:
        return {
            "ok": False,
            "code": exc.code,
            "summary": str(exc),
            "evidence_paths": [str(lease_path(rt, resource_id))],
            "next_action": {
                FORCE_RELEASE_LEASE_MISSING: "the resource has no lease; nothing to release",
                FORCE_RELEASE_LEASE_INVALID: "resolve the invalid lease record and retry",
                FORCE_RELEASE_HOLDER_LIVE: "stop the exact holder process before force-releasing",
                FORCE_RELEASE_HOLDER_UNPROVEN: "prove the holder dead or retire/abandon the lane before retrying",
                FORCE_RELEASE_DELETE_FAILED: "resolve the error and retry",
            }.get(exc.code, "resolve the error and retry"),
        }
    return {
        "ok": True,
        "code": FORCE_RELEASE_OK,
        "summary": f"lease force-released: {resource_id}",
        "evidence_paths": [str(lease_path(rt, resource_id))],
        "next_action": "none",
    }


def _nonblank_summary(value: str) -> str:
    summary = value.strip()
    if not summary:
        raise argparse.ArgumentTypeError("summary must be nonblank")
    return summary


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
    close_parser.add_argument("--summary", required=True, type=_nonblank_summary)

    lease = subparsers.add_parser("lease", help="exclusive-resource lease commands")
    lease_sub = lease.add_subparsers(dest="lease_command", required=True)
    force_release_parser = lease_sub.add_parser(
        "force-release", help="force-release one orphaned exclusive-resource lease"
    )
    force_release_parser.add_argument("--resource-id", required=True)

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
    health_sub.add_parser(
        "monitor-recover", help="recover the persistent monitor (managed, runtime OPEN)"
    )

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
    if command == "lease":
        if args.lease_command == "force-release":
            return _lease_force_release(args.resource_id)
        raise ValueError(f"unknown lease command: {args.lease_command}")
    if command == "send-lane-notification":
        return _send_lane_notification(args.lane_id, args.prompt)
    if command == "scan":
        return scan_watch.run_scan()
    if command == "watch":
        return scan_watch.run_watch(timeout=args.timeout, until_event=args.until_event)
    if command == "health":
        if args.health_command == "reconcile":
            return scan_watch.run_health_reconcile()
        if args.health_command == "monitor-recover":
            return setup.run_monitor_recover()
        raise ValueError(f"unknown health command: {args.health_command}")
    raise ValueError(f"unknown command: {command}")


def main(argv: list[str] | None = None) -> int:
    """Run the sole v2 public launcher route."""
    if argv is None:
        argv = sys.argv[1:]
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


if __name__ == "__main__":
    raise SystemExit(main())
