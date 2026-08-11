from __future__ import annotations

"""Native diagnostic and lifecycle command surface.

The CLI owns no managed watcher, heartbeat, pending-notification selector, or
acknowledgement policy.  Actionability and acknowledgement belong to the S3
manager queue; these commands only render bounded diagnostic observations.
"""

import argparse
import json
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from .codex_adapter import (
    CodexAdapterError,
    check_codex_adapter,
    install_codex_adapter,
    run_codex_hook,
    synthetic_wake_self_test,
    uninstall_codex_adapter,
    upgrade_codex_adapter,
)
from .config import ConfigError, HarnessConfig, load_config
from .discovery import discover_suite
from .events import diff_conditions
from .handoff_preflight import exit_code as handoff_preflight_exit_code
from .handoff_preflight import preflight_handoff
from .lane_lifecycle import allocate_immutable_source_view, retire_terminal_lane
from .models import parse_utc, utc_now
from .processes import process_snapshot
from .reconcile import reconcile
from .stable_io import PathSafetyError, SafeOutput, canonical_json
from .watcher_integration import merge_watcher_conditions


EXIT_OK = 0
EXIT_ERROR = 1
EXIT_TIMEOUT = 3


def _print_json(value: Any, *, stream: Any = sys.stdout) -> None:
    stream.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
    stream.flush()


def _print_events(events: list[dict[str, Any]], *, stream: Any = sys.stdout) -> None:
    for event in events:
        stream.write(canonical_json(event) + "\n")
    stream.flush()


def _store(config: HarnessConfig) -> SafeOutput:
    store = SafeOutput(
        harness_root=config.harness_root,
        output_root=config.output_dir,
        forbidden_roots=config.forbidden_output_roots,
        allowed_output_roots=(config.suite_root / "runtime", config.suite_root / "multi-agent-logs"),
    )
    store.prepare()
    return store


def observe(
    config: HarnessConfig,
    *,
    process_provider: Callable[[], Any] = process_snapshot,
    clock: Callable[[], datetime] = utc_now,
) -> tuple[dict[str, Any], datetime]:
    now = clock()
    return reconcile(discover_suite(config), process_provider(), config, now=now), now


def scan_command(
    config: HarnessConfig,
    *,
    process_provider: Callable[[], Any] = process_snapshot,
    clock: Callable[[], datetime] = utc_now,
    stream: Any = sys.stdout,
) -> int:
    snapshot, _ = observe(config, process_provider=process_provider, clock=clock)
    _print_json(snapshot, stream=stream)
    return EXIT_OK


def _diagnostic_conditions(snapshot: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return merge_watcher_conditions(dict(snapshot))


def _is_actionable(event: Mapping[str, Any]) -> bool:
    data = event.get("data")
    if isinstance(data, Mapping) and data.get("manager_actionable") is True:
        return True
    return event.get("type") in {
        "MANAGER_SIGNAL", "RESOURCE_CONFLICT", "RESOURCE_AMBIGUOUS",
        "REQUEST_EXPIRING", "REQUEST_STALE", "RELAY_READY", "LANE_STAGE_REPEAT",
    }


def watch_once(
    config: HarnessConfig,
    *,
    no_write: bool,
    process_provider: Callable[[], Any] = process_snapshot,
    clock: Callable[[], datetime] = utc_now,
    stream: Any = sys.stdout,
    store_factory: Callable[[HarnessConfig], SafeOutput] = _store,
) -> tuple[int, list[dict[str, Any]]]:
    store = None if no_write else store_factory(config)
    prior = store.load_cursor() if store else None
    previous = prior.get("conditions") if isinstance(prior, Mapping) else None
    snapshot, observed = observe(config, process_provider=process_provider, clock=clock)
    conditions = _diagnostic_conditions(snapshot)
    events = diff_conditions(previous, conditions, observed_at=observed)
    if store:
        store.commit(snapshot=snapshot, events=events, conditions=conditions)
    _print_events(events, stream=stream)
    return EXIT_OK, events


def watch_until_event(
    config: HarnessConfig,
    *,
    no_write: bool,
    timeout_seconds: float | None,
    process_provider: Callable[[], Any] = process_snapshot,
    clock: Callable[[], datetime] = utc_now,
    sleeper: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    stream: Any = sys.stdout,
    store_factory: Callable[[HarnessConfig], SafeOutput] = _store,
) -> int:
    store = None if no_write else store_factory(config)
    prior = store.load_cursor() if store else None
    previous = prior.get("conditions") if isinstance(prior, Mapping) else None
    timeout = config.watch_timeout_seconds if timeout_seconds is None else timeout_seconds
    deadline = monotonic() + timeout
    while True:
        snapshot, observed = observe(config, process_provider=process_provider, clock=clock)
        conditions = _diagnostic_conditions(snapshot)
        events = diff_conditions(previous, conditions, observed_at=observed)
        if events:
            if store:
                store.commit(snapshot=snapshot, events=events, conditions=conditions)
            _print_events(events, stream=stream)
            return EXIT_OK
        if monotonic() >= deadline:
            _print_events([{
                "event_id": "WATCH_TIMEOUT", "identity": "watch:timeout", "type": "WATCH_TIMEOUT",
                "severity": "info", "observed_utc": observed.isoformat(),
                "data": {"timeout_seconds": timeout},
            }], stream=stream)
            return EXIT_TIMEOUT
        sleeper(min(config.poll_interval_seconds, max(0.0, deadline - monotonic())))


def watch_until_actionable(
    config: HarnessConfig,
    *,
    timeout_seconds: float | None,
    process_provider: Callable[[], Any] = process_snapshot,
    clock: Callable[[], datetime] = utc_now,
    sleeper: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    stream: Any = sys.stdout,
    store_factory: Callable[[HarnessConfig], SafeOutput] = _store,
) -> int:
    """Block on diagnostic facts without creating a competing pending queue."""

    store = store_factory(config)
    prior = store.load_cursor()
    previous = prior.get("conditions") if isinstance(prior, Mapping) else None
    timeout = config.watch_timeout_seconds if timeout_seconds is None else timeout_seconds
    deadline = monotonic() + timeout
    while True:
        snapshot, observed = observe(config, process_provider=process_provider, clock=clock)
        conditions = _diagnostic_conditions(snapshot)
        events = diff_conditions(previous, conditions, observed_at=observed)
        actionable = [event for event in events if _is_actionable(event)]
        if actionable:
            store.commit(snapshot=snapshot, events=events, conditions=conditions)
            _print_events(actionable, stream=stream)
            return EXIT_OK
        store.commit(snapshot=snapshot, events=events, conditions=conditions)
        previous = conditions
        if monotonic() >= deadline:
            _print_events([{
                "event_id": "WATCH_TIMEOUT", "identity": "watch:timeout", "type": "WATCH_TIMEOUT",
                "severity": "info", "observed_utc": observed.isoformat(),
                "data": {"timeout_seconds": timeout},
            }], stream=stream)
            return EXIT_TIMEOUT
        sleeper(min(config.poll_interval_seconds, max(0.0, deadline - monotonic())))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m orchestrator_harness",
        description="Durable diagnostic observation and safe lane lifecycle boundaries",
    )
    parser.add_argument("--config", default=str(Path(__file__).resolve().parent / "config.example.json"))
    subparsers = parser.add_subparsers(dest="command", required=True)
    scan = subparsers.add_parser("scan", help="print one reconciled snapshot")
    scan.add_argument("--no-write", action="store_true")
    watch = subparsers.add_parser("watch", help="run a bounded diagnostic watch")
    mode = watch.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--until-event", action="store_true")
    mode.add_argument("--until-actionable", action="store_true")
    watch.add_argument("--timeout", type=float, default=None)
    watch.add_argument("--no-write", action="store_true")
    preflight = subparsers.add_parser("handoff-preflight")
    for name in ("task-card", "invocation", "result", "dependency-map", "worktree", "evidence-root"):
        preflight.add_argument(f"--{name}", required=True, type=Path)
    preflight.add_argument("--required-evidence", action="append", default=[], type=Path)
    adapter = subparsers.add_parser("adapter", help="install or inspect a project-local host adapter")
    adapter_modes = adapter.add_subparsers(dest="adapter_action", required=True)
    for action in ("install", "check", "upgrade", "uninstall", "self-test"):
        command = adapter_modes.add_parser(action)
        command.add_argument("--host", default="codex")
        command.add_argument("--project-root", required=True, type=Path)
        if action == "self-test":
            command.add_argument("--queue-root", type=Path)
    hook = adapter_modes.add_parser("hook")
    hook.add_argument("--host", default="codex")
    hook.add_argument("--project-root", required=True, type=Path)
    hook.add_argument("--boundary", choices=("post_tool_use", "stop"), required=True)

    def add_view_commands(parent: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
        view = parent.add_parser("view", help="allocate an exact retained-ref immutable source view")
        modes = view.add_subparsers(dest="view_action", required=True)
        allocate = modes.add_parser("allocate")
        allocate.add_argument("--source-root", required=True, type=Path)
        allocate.add_argument("--revision", required=True)
        allocate.add_argument("--retained-ref", required=True)
        allocate.add_argument("--view-root", required=True, type=Path)
        allocate.add_argument("--result-root", required=True, type=Path)
        allocate.add_argument("--cache-root", required=True, type=Path)
        allocate.add_argument("--view-id", default="immutable-view")
    add_view_commands(subparsers)
    source = subparsers.add_parser("source")
    modes = source.add_subparsers(dest="source_action", required=True)
    allocate = modes.add_parser("allocate")
    allocate.add_argument("--source-root", required=True, type=Path)
    allocate.add_argument("--revision", required=True)
    allocate.add_argument("--retained-ref", required=True)
    allocate.add_argument("--view-root", required=True, type=Path)
    allocate.add_argument("--result-root", required=True, type=Path)
    allocate.add_argument("--cache-root", required=True, type=Path)
    allocate.add_argument("--view-id", default="immutable-view")

    lane = subparsers.add_parser("lane")
    modes = lane.add_subparsers(dest="lane_action", required=True)
    retire = modes.add_parser("retire")
    retire.add_argument("--lane-root", required=True, type=Path)
    retire.add_argument("--archive-root", required=True, type=Path)
    retire.add_argument("--lane-id", required=True)
    for name in ("task-ref", "result-ref", "findings-ref", "acceptance-ref", "transcript-ref", "dependency-ref"):
        retire.add_argument(f"--{name}", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "adapter":
            if args.host != "codex":
                raise CodexAdapterError("only the implemented codex host supports this command")
            if args.adapter_action == "install":
                _print_json(install_codex_adapter(args.project_root))
            elif args.adapter_action == "check":
                _print_json(check_codex_adapter(args.project_root))
            elif args.adapter_action == "upgrade":
                _print_json(upgrade_codex_adapter(args.project_root))
            elif args.adapter_action == "uninstall":
                _print_json(uninstall_codex_adapter(args.project_root))
            elif args.adapter_action == "self-test":
                _print_json(synthetic_wake_self_test(args.project_root, queue_root=args.queue_root))
            else:
                _print_json(run_codex_hook(args.boundary, project_root=args.project_root))
            return EXIT_OK
        if args.command in {"view", "source"}:
            result = allocate_immutable_source_view(
                args.source_root, revision=args.revision, retained_ref=args.retained_ref,
                view_root=args.view_root, result_root=args.result_root, cache_root=args.cache_root,
                view_id=args.view_id,
            )
            _print_json(result.as_record())
            return EXIT_OK
        if args.command == "lane":
            result = retire_terminal_lane(
                args.lane_root, args.archive_root, lane_id=args.lane_id,
                task_ref=args.task_ref, result_ref=args.result_ref, findings_ref=args.findings_ref,
                acceptance_ref=args.acceptance_ref, transcript_ref=args.transcript_ref,
                dependency_ref=args.dependency_ref,
            )
            _print_json(result.as_record())
            return EXIT_OK if result.outcome.startswith("CLOSED") else EXIT_ERROR
        if args.command == "handoff-preflight":
            result = preflight_handoff(
                task_card_path=args.task_card, invocation_path=args.invocation,
                result_path=args.result, dependency_map_path=args.dependency_map,
                worktree=args.worktree, evidence_root=args.evidence_root,
                required_evidence=args.required_evidence,
            )
            _print_json(result)
            return handoff_preflight_exit_code(result)
        config = load_config(args.config)
        if args.command == "scan":
            return scan_command(config)
        if args.once:
            return watch_once(config, no_write=args.no_write)[0]
        if args.until_actionable:
            if args.no_write:
                raise ValueError("--until-actionable requires its diagnostic cursor")
            return watch_until_actionable(config, timeout_seconds=args.timeout)
        return watch_until_event(config, no_write=args.no_write, timeout_seconds=args.timeout)
    except (ConfigError, PathSafetyError, OSError, ValueError) as exc:
        sys.stderr.write(f"orchestrator_harness: {exc}\n")
        return EXIT_ERROR
