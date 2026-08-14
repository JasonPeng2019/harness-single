from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from harness_common.process_identity import exact_process_identity

from . import settings
from .attention import (
    AttentionValidationError,
    append_producer_record,
    make_source_record,
)
from .config import load_config
from .evaluator import TerraHighEvaluator
from .logging import log
from .poller import initialize_service_cursor, poll


def _identity(pid: int) -> dict[str, object] | None:
    return exact_process_identity(pid)


def _same(record: object) -> bool:
    if (
        not isinstance(record, dict)
        or not isinstance(record.get("pid"), int)
        or not isinstance(record.get("created_utc"), str)
    ):
        return False
    actual = _identity(record["pid"])
    return actual is not None and actual == {
        "pid": record["pid"],
        "created_utc": record["created_utc"],
    }


def _read(path: Path) -> dict[str, object] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _atomic(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=".service-", suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(value, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(name, path)


def _sleep_until(seconds: int, service: Path, owner: dict[str, object]) -> str:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        state = _read(service) or {}
        if state.get("stop_requested"):
            return "stop-requested"
        if not _same(owner):
            return "owner-identity-lost"
        time.sleep(min(1.0, max(0, deadline - time.monotonic())))
    return "poll"


def _terminal(
    service: Path,
    runtime: Path,
    state: dict[str, object],
    reason: str,
    startup_token: str,
    watcher: dict[str, object] | None,
) -> bool:
    current = _read(service)
    if not isinstance(current, dict) or current.get("startup_token") != startup_token:
        return False
    if watcher is not None and current.get("watcher") != watcher:
        return False
    current["exit_reason"] = reason
    current["exited_utc"] = datetime.now(timezone.utc).isoformat()
    _atomic(service, current)
    log(runtime, "watcher", "SERVICE_STOPPED", {"reason": reason})
    return True


def _terminate_reap(child: Any) -> None:
    if child.poll() is not None:
        return
    child.terminate()
    try:
        child.wait(timeout=5)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=5)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m harness_watcher_implementation")
    parser.add_argument("--config")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ("poll", "status", "stop"):
        sub.add_parser(name)
    serve = sub.add_parser("serve")
    serve.add_argument("--startup-token", required=True)
    record = sub.add_parser("record-attention")
    record.add_argument("--role", required=True, choices=("orchestrator", "subagent"))
    record.add_argument("--source-id", required=True)
    record.add_argument("--epoch-id", required=True)
    record.add_argument("--event-id", required=True)
    record.add_argument("--kind", required=True)
    record.add_argument("--source-timestamp-utc")
    metadata_input = record.add_mutually_exclusive_group()
    metadata_input.add_argument("--metadata")
    metadata_input.add_argument("--metadata-file")
    start = sub.add_parser("start")
    start.add_argument("--owner-pid", type=int)
    args = parser.parse_args(argv)
    if args.cmd == "record-attention" and not settings.harness_watcher_active:
        print(json.dumps({"disabled": True}))
        return 0
    cfg = load_config(args.config)
    runtime = cfg.runtime_root
    service = runtime / "watcher" / "service.json"
    if args.cmd == "record-attention":
        if not cfg.attention_logging_enabled:
            print(json.dumps({"disabled": True}))
            return 0
        if (args.role, args.source_id) not in cfg.attention_producers:
            print(json.dumps({"error": "producer identity is not allowlisted"}))
            return 2
        try:
            metadata_text = (
                Path(args.metadata_file).read_text(encoding="utf-8")
                if args.metadata_file
                else (args.metadata if args.metadata is not None else "{}")
            )
            metadata = json.loads(metadata_text)
            if not isinstance(metadata, dict):
                raise AttentionValidationError("metadata must be an object")
            record_value = make_source_record(
                recorder=args.source_id,
                epoch_id=args.epoch_id,
                event_id=args.event_id,
                kind=args.kind,
                source_timestamp_utc=args.source_timestamp_utc,
                metadata=metadata,
            )
            path = append_producer_record(
                runtime,
                role=args.role,
                source_id=args.source_id,
                record=record_value,
                lock_timeout_seconds=cfg.attention_lock_timeout_seconds,
            )
        except (
            OSError,
            json.JSONDecodeError,
            AttentionValidationError,
            ValueError,
            TimeoutError,
        ) as exc:
            print(json.dumps({"error": str(exc)}))
            return 2
        print(json.dumps({"record_id": record_value["record_id"], "path": str(path)}))
        return 0
    if not settings.harness_watcher_active:
        print(json.dumps({"active": False, "result": "disabled-no-op"}))
        return 0
    if args.cmd == "poll":
        print(
            json.dumps(
                {
                    "alert": poll(
                        cfg,
                        TerraHighEvaluator(cfg.evaluator_command)
                        if cfg.evaluator_enabled
                        else None,
                    )["alert"]
                },
                default=str,
            )
        )
        return 0
    if args.cmd == "status":
        state = _read(service)
        running = bool(
            state
            and _same(state.get("watcher"))
            and _same(state.get("owner"))
            and not state.get("stop_requested")
            and not state.get("exit_reason")
        )
        print(json.dumps({"running": running, "state": state}))
        return 0
    if args.cmd == "stop":
        state = _read(service)
        if not state or not _same(state.get("watcher")):
            print(json.dumps({"stop_requested": False, "reason": "no-live-service"}))
            return 0
        state["stop_requested"] = True
        state["stop_requested_utc"] = datetime.now(timezone.utc).isoformat()
        _atomic(service, state)
        log(runtime, "watcher", "STOP_REQUESTED", {})
        print(json.dumps({"stop_requested": True}))
        return 0
    if args.cmd == "start":
        state = _read(service)
        if state and _same(state.get("watcher")) and not state.get("stop_requested"):
            print(json.dumps({"running": True, "result": "already-started"}))
            return 0
        service.parent.mkdir(parents=True, exist_ok=True)
        claim = service.with_suffix(".claim")
        try:
            fd = os.open(claim, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            print(json.dumps({"running": False, "result": "startup-in-progress"}))
            return 1
        child: Any = None
        try:
            with os.fdopen(fd, "w") as handle:
                handle.write(str(os.getpid()))
            owner = _identity(
                args.owner_pid if args.owner_pid is not None else os.getppid()
            )
            if owner is None:
                raise RuntimeError("cannot establish owner process identity")
            startup_token = str(uuid.uuid4())
            _atomic(
                service,
                {
                    "schema": "harness-watcher-service/v1",
                    "watcher": None,
                    "owner": owner,
                    "startup_token": startup_token,
                    "startup_status": "STARTING",
                    "started_utc": datetime.now(timezone.utc).isoformat(),
                    "stop_requested": False,
                    "exit_reason": None,
                    "evaluator_enabled": cfg.evaluator_enabled,
                },
            )
            command = (
                [sys.executable, "-m", "harness_watcher_implementation"]
                + (["--config", args.config] if args.config else [])
                + ["serve", "--startup-token", startup_token]
            )
            flags = 0
            if os.name == "nt":
                flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) | getattr(
                    subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0
                )
            child = subprocess.Popen(
                command,
                cwd=str(cfg.repository_root),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=(os.name != "nt"),
                creationflags=flags,
            )
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                published = _read(service)
                observed_watcher = (
                    published.get("watcher") if isinstance(published, dict) else None
                )
                if (
                    isinstance(published, dict)
                    and published.get("startup_token") == startup_token
                    and published.get("startup_status") == "READY"
                    and isinstance(observed_watcher, dict)
                    and isinstance(observed_watcher.get("pid"), int)
                    and _same(observed_watcher)
                ):
                    print(
                        json.dumps(
                            {
                                "started": True,
                                "pid": observed_watcher["pid"],
                                "owner": owner,
                            }
                        )
                    )
                    return 0
                if child.poll() is not None:
                    break
                time.sleep(0.05)
            raise RuntimeError("watcher did not publish its exact startup identity")
        except Exception as exc:
            log(
                runtime,
                "watcher",
                "SERVICE_ERROR",
                {"phase": "start", "error": str(exc)[:500]},
            )
            if child is not None:
                _terminate_reap(child)
            raise
        finally:
            claim.unlink(missing_ok=True)
    if args.cmd == "serve":
        state: dict[str, object] | None = None
        watcher: dict[str, object] | None = None
        reason = "service-error"
        try:
            for _ in range(100):
                state = _read(service)
                if state and state.get("startup_token") == args.startup_token:
                    break
                time.sleep(0.05)
            if not isinstance(state, dict):
                raise RuntimeError("startup state was not published")
            if state.get("startup_token") != args.startup_token:
                raise RuntimeError(
                    "startup token does not match published service state"
                )
            owner = state.get("owner")
            if not isinstance(owner, dict) or not _same(owner):
                raise RuntimeError("startup owner identity is not live")
            watcher_identity = _identity(os.getpid())
            if watcher_identity is None:
                raise RuntimeError("cannot establish watcher process identity")
            watcher = watcher_identity
            state["watcher"] = watcher
            state["startup_status"] = "READY"
            _atomic(service, state)
            initialize_service_cursor(cfg)
            log(
                runtime,
                "watcher",
                "SERVICE_STARTED",
                {"pid": os.getpid(), "evaluator_enabled": cfg.evaluator_enabled},
            )
            while True:
                if not _same(owner):
                    reason = "owner-identity-lost"
                    break
                poll(
                    cfg,
                    TerraHighEvaluator(cfg.evaluator_command)
                    if cfg.evaluator_enabled
                    else None,
                )
                reason = _sleep_until(cfg.poll_interval_seconds, service, owner)
                if reason != "poll":
                    break
            return 0
        except Exception as exc:
            reason = "service-error"
            log(runtime, "watcher", "SERVICE_ERROR", {"error": str(exc)[:500]})
            return 1
        finally:
            if state is not None:
                _terminal(service, runtime, state, reason, args.startup_token, watcher)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
