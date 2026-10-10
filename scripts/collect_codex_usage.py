#!/usr/bin/env python3
"""Capture one Codex CLI process or account for a ROOT plus native worker run.

``run`` launches an initial ROOT/raw Codex process. ``resume`` continues the
same native session in a new evidence segment after a sealed planned pause.
The native harness continues to own all worker launches. ``snapshot`` and
``gate`` only read evidence. ``finalize`` makes a write-once task archive.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from prepare_root_launch import validate_prepared_root

from token_ledger import (
    RUN_SCHEMA,
    TASK_SCHEMA,
    LedgerError,
    build_snapshot,
    extract_usage,
    finalize,
    load_task,
    read_object,
    utc_now,
    valid_id,
)


def write_once(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def write_stdout_safe(line: str) -> None:
    """Keep the raw UTF-8 archive intact when Windows stdout uses cp1252."""
    encoding = sys.stdout.encoding or "utf-8"
    try:
        line.encode(encoding)
    except UnicodeEncodeError:
        try:
            line = json.dumps(json.loads(line), ensure_ascii=True) + "\n"
        except json.JSONDecodeError:
            line = line.encode(encoding, errors="backslashreplace").decode(encoding)
    sys.stdout.write(line)
    sys.stdout.flush()


def inhibit_idle_sleep() -> bool:
    """Keep an active Windows run awake without changing the power plan."""
    if os.name != "nt":
        return False
    # ES_CONTINUOUS | ES_SYSTEM_REQUIRED; the request belongs to this thread
    # and is released when the run ends or the process exits.
    if not ctypes.windll.kernel32.SetThreadExecutionState(0x80000001):
        raise OSError("could not inhibit Windows idle sleep for this run")
    return True


def release_idle_sleep() -> None:
    if os.name == "nt":
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)


def stop_process_tree(process: subprocess.Popen[str]) -> None:
    """Stop the exact launched Codex process and its descendants."""
    if process.poll() is not None:
        return
    if os.name == "nt":
        result = subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            capture_output=True, text=True, timeout=30, check=False,
        )
        if result.returncode and process.poll() is None:
            process.kill()
    else:
        os.killpg(process.pid, signal.SIGKILL)


def shutdown_harness_runtime(expected_runtime: Path, harness_dir: Path | None = None) -> str | None:
    """Ask the native harness to stop any detached lanes after a watchdog fire."""
    harness_dir = harness_dir or Path(__file__).resolve().parent.parent
    config_path = harness_dir / "harness-config.json"
    if not config_path.is_file():
        return f"harness configuration not found: {harness_dir}"
    try:
        configured = Path(json.loads(config_path.read_text(encoding="utf-8"))["root_workspace"]).resolve()
    except (OSError, KeyError, ValueError, TypeError) as exc:
        return f"could not validate harness configuration: {exc}"
    if configured / ".harness-runtime" != expected_runtime.resolve():
        return "harness configuration no longer selects this run's ROOT workspace"
    try:
        result = subprocess.run(
            [sys.executable, "-m", "orchestrator_harness.operator_launch",
             "--json", "harness", "shutdown"],
            cwd=harness_dir, capture_output=True, text=True,
            timeout=90, check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return str(exc)
    if result.returncode:
        return (result.stderr or result.stdout).strip() or "harness shutdown failed"
    return None


def active_harness_lanes(runtime: Path) -> bool:
    """Keep the safety timer alive while native harness worker lanes run."""
    for lane_path in runtime.glob("epochs/*/lanes/*/lane.json"):
        try:
            lane = json.loads(lane_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return True  # Unreadable live state is not proof that workers stopped.
        if lane.get("lifecycle") == "running":
            return True
    return False


def quiescent_open_runtime(runtime: Path) -> bool:
    """Allow a sealed ROOT turn to continue a review without losing its lane."""
    for lane_path in runtime.glob("epochs/*/lanes/*/lane.json"):
        try:
            lane = read_object(lane_path)
            lifecycle = lane.get("lifecycle")
            if lifecycle == "retired":
                continue
            if lifecycle != "review_pending":
                return False
            status = read_object(Path(lane["controller_status_path"]))
            if (status.get("run_id") != lane.get("run_id")
                    or status.get("recorded_status") != "review_pending"
                    or status.get("controller_state") != "exited"
                    or status.get("cleanup_proven") is not True
                    or status.get("provider_state", {}).get("state") != "exited"):
                return False
        except (OSError, ValueError, KeyError, TypeError, LedgerError):
            return False
    return True


def prepare_resume(args: argparse.Namespace) -> tuple[Path, int, str, float]:
    """Validate a sealed segment and return the next segment's native session."""
    task_path = Path(args.resume_task_file).resolve()
    task = load_task(task_path)
    if task.get("interruption_policy") != "planned_resume":
        raise LedgerError("this task was not declared as a planned interrupted run")
    run_dir = Path(task["root_run_dir"])
    original = read_object(run_dir / "launch.json")
    if (original.get("run_id") != task["run_id"] or original.get("arm") != task["arm"]
            or original.get("task") != task["task"]
            or original.get("budget_tokens") != task["budget_tokens"]
            or original.get("harness_runtime") != task.get("harness_runtime")):
        raise LedgerError("resume task and original launch differ")
    if task["arm"] == "harness":
        runtime = Path(task["harness_runtime"])
        if Path(original.get("workspace", "")).resolve() != runtime.parent:
            raise LedgerError("ROOT workspace differs from the selected harness runtime")
        state = read_object(runtime / "RUNTIME_STATE.json")
        if state.get("state") == "CLOSED":
            if active_harness_lanes(runtime):
                raise LedgerError("close the native harness runtime and retire lanes before ROOT resume")
        elif (state.get("state") != "OPEN"
              or not getattr(args, "continue_open_runtime", False)
              or not quiescent_open_runtime(runtime)):
            raise LedgerError("close the native harness runtime and retire lanes before ROOT resume, or explicitly continue a quiescent open review")
    snapshot = build_snapshot(task_path)
    if snapshot["errors"] or snapshot["in_flight_invocations"]:
        raise LedgerError("prior task evidence is not sealed and countable: " + "; ".join(snapshot["errors"]))
    segment_root = run_dir / "segments"
    children = sorted(item for item in segment_root.iterdir() if item.is_dir()) if segment_root.is_dir() else []
    for number, child in enumerate(children, 2):
        if child.name != f"{number:04d}":
            raise LedgerError(f"nonconsecutive ROOT segment directory: {child}")
    prior_dirs = [run_dir, *children]
    consumed = 0.0
    for directory in prior_dirs:
        launch = read_object(directory / "launch.json")
        final = read_object(directory / "manifest.json")
        if launch.get("run_id") != task["run_id"] or final.get("run_id") != task["run_id"]:
            raise LedgerError("ROOT segment belongs to another task run")
        elapsed = final.get("elapsed_seconds")
        if isinstance(elapsed, bool) or not isinstance(elapsed, (int, float)) or elapsed < 0:
            raise LedgerError(f"ROOT segment has no valid active elapsed time: {directory}")
        if final.get("watchdog_expired"):
            raise LedgerError("the active-time watchdog expired; this task run cannot be resumed")
        consumed += elapsed
    first_limit = original.get("watchdog_hours")
    if isinstance(first_limit, bool) or not isinstance(first_limit, (int, float)) or first_limit <= 0:
        raise LedgerError("original active-time watchdog is missing")
    remaining = first_limit - consumed / 3600
    if remaining <= 0:
        raise LedgerError("the task's active-time allowance is exhausted")
    prior = prior_dirs[-1]
    usage = extract_usage(prior / "codex-events.jsonl")
    if usage["status"] != "ok" or not usage.get("thread_id"):
        raise LedgerError("prior ROOT segment lacks countable usage or a native session ID")
    for field in ("model", "reasoning_effort", "service_tier", "sandbox", "workspace"):
        if not isinstance(original.get(field), str) or not original[field]:
            raise LedgerError(f"original ROOT {field} is missing")
    args.run_id = task["run_id"]
    args.task = task["task"]
    args.arm = task["arm"]
    args.cwd = original["workspace"]
    args.model = original["model"]
    args.reasoning_effort = original["reasoning_effort"]
    args.service_tier = original["service_tier"]
    args.sandbox = original["sandbox"]
    args.budget_tokens = task["budget_tokens"]
    args.harness_runtime = task.get("harness_runtime")
    args.harness_dir = original.get("harness_dir")
    args.trust_project_hooks = bool(original.get("trust_project_hooks"))
    args.watchdog_hours = remaining
    args.results_dir = str(run_dir.parent)
    return run_dir / "segments" / f"{len(prior_dirs) + 1:04d}", len(prior_dirs) + 1, usage["thread_id"], first_limit


def run_codex(args: argparse.Namespace) -> int:
    resuming = bool(getattr(args, "resume_task_file", None))
    if resuming:
        segment_dir, segment_number, session_id, total_limit = prepare_resume(args)
    prompt = Path(args.prompt_file).resolve()
    workspace = Path(args.cwd).resolve()
    if not prompt.is_file():
        raise LedgerError(f"prompt file does not exist: {prompt}")
    if not workspace.is_dir():
        raise LedgerError(f"workspace does not exist: {workspace}")
    if not args.task.strip():
        raise LedgerError("task must be non-empty")
    if args.budget_tokens is not None and args.budget_tokens <= 0:
        raise LedgerError("budget-tokens must be positive")
    if args.watchdog_hours is None or not math.isfinite(args.watchdog_hours) or args.watchdog_hours <= 0:
        raise LedgerError("watchdog-hours must be positive")
    if args.arm == "harness" and not args.harness_runtime:
        raise LedgerError("harness runs require --harness-runtime")
    if args.arm == "harness" and not getattr(args, "trust_project_hooks", False):
        raise LedgerError("harness runs require --trust-project-hooks")
    if args.arm == "harness" and Path(args.harness_runtime).resolve() != workspace / ".harness-runtime":
        raise LedgerError("harness runtime must belong to the selected ROOT workspace")
    if args.arm == "raw" and args.harness_runtime:
        raise LedgerError("raw runs must not name a harness runtime")
    harness_dir = (Path(args.harness_dir).resolve() if getattr(args, "harness_dir", None)
                   else Path(__file__).resolve().parent.parent)
    if args.arm == "harness":
        config_path = harness_dir / "harness-config.json"
        if not config_path.is_file():
            raise LedgerError(f"harness configuration not found: {config_path}")
        try:
            configured = Path(json.loads(config_path.read_text(encoding="utf-8"))["root_workspace"]).resolve()
        except (OSError, KeyError, ValueError, TypeError) as exc:
            raise LedgerError(f"invalid harness configuration: {exc}") from exc
        if configured != workspace:
            raise LedgerError("harness configuration does not select this ROOT workspace")

    root_prelaunch = None
    if args.arm == "harness":
        try:
            root_prelaunch = validate_prepared_root(harness_dir, workspace, args.run_id, resuming=resuming)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise LedgerError(str(exc)) from exc

    task_run_dir = Path(args.results_dir).resolve() / args.run_id
    run_dir = segment_dir if resuming else task_run_dir
    run_dir.mkdir(parents=True, exist_ok=False)
    events_path = run_dir / "codex-events.jsonl"
    stderr_path = run_dir / "codex-stderr.txt"
    last_message_path = run_dir / "last-message.txt"
    started = utc_now()
    hook_trust_args = (
        ["--dangerously-bypass-hook-trust"]
        if getattr(args, "trust_project_hooks", False)
        else []
    )
    if args.arm == "harness":
        hook_trust_args += [
            "-c", "features.hooks=true",
            "-c", f'projects.{json.dumps(str(workspace))}.trust_level="trusted"',
        ]
    if resuming:
        command = [
            "codex", "exec", "resume", *hook_trust_args, "--json",
            "-c", f"sandbox_mode={json.dumps(args.sandbox)}",
            "-c", 'approval_policy="never"',
            "--skip-git-repo-check", "-m", args.model,
            "-c", f"model_reasoning_effort={json.dumps(args.reasoning_effort)}",
            "-c", f"service_tier={json.dumps(args.service_tier)}",
            "--output-last-message", str(last_message_path), session_id, "-",
        ]
    else:
        command = [
            "codex", "exec", *hook_trust_args, "--json", "--sandbox", args.sandbox,
            "-c", 'approval_policy="never"',
            "--skip-git-repo-check", "-m", args.model,
            "-c", f"model_reasoning_effort={json.dumps(args.reasoning_effort)}",
            "-c", f"service_tier={json.dumps(args.service_tier)}",
            "--cd", str(workspace),
            "--output-last-message", str(last_message_path),
            "-",
        ]
    codex_executable = shutil.which("codex.cmd" if os.name == "nt" else "codex")
    if codex_executable is None:
        raise OSError("Codex CLI executable not found on PATH")
    launch = {
        "schema": RUN_SCHEMA,
        "run_id": args.run_id,
        "task": args.task,
        "arm": args.arm,
        "role": "root" if args.arm == "harness" else "raw",
        "model": args.model,
        "reasoning_effort": args.reasoning_effort,
        "service_tier": args.service_tier,
        "sandbox": args.sandbox,
        "workspace": str(workspace),
        "prompt_file": str(prompt),
        "started_at_utc": started,
        "command": command,
        "resolved_executable": codex_executable,
        "watchdog_hours": args.watchdog_hours,
        "budget_tokens": args.budget_tokens,
        "harness_runtime": str(Path(args.harness_runtime).resolve()) if args.harness_runtime else None,
        "harness_dir": str(harness_dir) if args.arm == "harness" and getattr(args, "harness_dir", None) else None,
        "trust_project_hooks": bool(getattr(args, "trust_project_hooks", False)),
    }
    if root_prelaunch is not None:
        launch["root_prelaunch"] = root_prelaunch
    if resuming:
        launch["segment_number"] = segment_number
        launch["resume_session_id"] = session_id
        launch["total_active_watchdog_hours"] = total_limit
    write_once(run_dir / "launch.json", launch)
    task_path = Path(args.resume_task_file).resolve() if resuming else run_dir / "task.json"
    if not resuming:
        task = {
            "schema": TASK_SCHEMA,
            "run_id": args.run_id,
            "task": args.task,
            "arm": args.arm,
            "budget_tokens": args.budget_tokens,
            "root_run_dir": str(run_dir),
        }
        if getattr(args, "interrupted_run", False):
            task["interruption_policy"] = "planned_resume"
        if args.harness_runtime:
            task["harness_runtime"] = str(Path(args.harness_runtime).resolve())
        write_once(task_path, task)
    environment = os.environ.copy()
    # ROOT invokes the host-side harness package outside its writable workspace.
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    # An uncapped run must not inherit an earlier diagnostic gate by accident.
    environment.pop("SWE_TOKEN_TASK_FILE", None)
    environment.pop("SWE_TOKEN_COLLECTOR", None)
    if args.arm == "harness" and args.budget_tokens is not None:
        environment["SWE_TOKEN_TASK_FILE"] = str(task_path)
        environment["SWE_TOKEN_COLLECTOR"] = str(Path(__file__).resolve())

    start_clock = time.monotonic()
    exit_code: int | None = None
    launch_error: str | None = None
    process: subprocess.Popen[str] | None = None
    watchdog_expired = threading.Event()
    watchdog_errors: list[str] = []
    timer: threading.Timer | None = None
    sleep_inhibited = False
    try:
        if getattr(args, "inhibit_sleep", False):
            sleep_inhibited = inhibit_idle_sleep()
        with prompt.open("rb") as prompt_stream, \
             events_path.open("x", encoding="utf-8", newline="") as events_stream, \
             stderr_path.open("x", encoding="utf-8", newline="") as stderr_stream:
            process = subprocess.Popen(
                [codex_executable, *command[1:]],
                cwd=workspace,
                env=environment,
                stdin=prompt_stream,
                stdout=subprocess.PIPE,
                stderr=stderr_stream,
                text=True,
                encoding="utf-8",
                errors="replace",
                start_new_session=os.name != "nt",
            )
            def expire() -> None:
                assert process is not None
                root_live = process.poll() is None
                workers_live = args.arm == "harness" and active_harness_lanes(Path(args.harness_runtime))
                if not root_live and not workers_live:
                    return
                watchdog_expired.set()
                if root_live:
                    try:
                        stop_process_tree(process)
                    except (OSError, subprocess.TimeoutExpired) as exc:
                        watchdog_errors.append(f"Codex stop: {exc}")
                if args.arm == "harness":
                    failure = shutdown_harness_runtime(Path(args.harness_runtime), harness_dir)
                    if failure:
                        watchdog_errors.append(f"harness shutdown: {failure}")

            timer = threading.Timer(args.watchdog_hours * 3600, expire)
            timer.daemon = True
            timer.start()
            assert process.stdout is not None
            for line in process.stdout:
                events_stream.write(line)
                events_stream.flush()
                write_stdout_safe(line)
            exit_code = process.wait()
            if args.arm == "harness":
                while not watchdog_expired.is_set() and active_harness_lanes(Path(args.harness_runtime)):
                    time.sleep(2)
    except KeyboardInterrupt:
        if process is not None and process.poll() is None:
            stop_process_tree(process)
            try:
                exit_code = process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                exit_code = process.wait()
        launch_error = "interrupted by operator"
    except (OSError, ValueError) as exc:
        launch_error = str(exc)
    finally:
        if timer is not None:
            timer.cancel()
            if watchdog_expired.is_set():
                timer.join(timeout=120)
        if sleep_inhibited:
            release_idle_sleep()
        final = {
            **launch,
            "ended_at_utc": utc_now(),
            "elapsed_seconds": round(time.monotonic() - start_clock, 3),
            "exit_code": exit_code,
            "launch_error": launch_error,
            "watchdog_expired": watchdog_expired.is_set(),
            "watchdog_stop_errors": watchdog_errors,
        }
        write_once(run_dir / "manifest.json", final)
    print(f"artifacts: {run_dir}", file=sys.stderr)
    if launch_error:
        print(f"error: {launch_error}", file=sys.stderr)
        return 130 if launch_error == "interrupted by operator" else 2
    if watchdog_expired.is_set():
        print("error: run watchdog expired", file=sys.stderr)
        return 124
    return exit_code if exit_code is not None else 2


def show_snapshot(args: argparse.Namespace) -> int:
    report = build_snapshot(Path(args.task_file).resolve())
    print(json.dumps(report, indent=2, sort_keys=True))
    return 2 if report["errors"] else 0


def gate(args: argparse.Namespace) -> int:
    report = build_snapshot(Path(args.task_file).resolve())
    summary = {
        key: report[key]
        for key in (
            "run_id", "budget_tokens", "known_total_tokens", "remaining_tokens",
            "aggregate_components", "overshoot_tokens", "in_flight_invocations",
            "unknown_usage_invocations", "token_total_is_exact",
            "errors", "gate_allows_new_work",
        )
    }
    print(json.dumps(summary, sort_keys=True))
    if report["errors"]:
        return 2
    return 0 if report["gate_allows_new_work"] else 3


def archive(args: argparse.Namespace) -> int:
    summary = finalize(Path(args.task_file).resolve(), Path(args.output).resolve())
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def summarize(args: argparse.Namespace) -> int:
    path = Path(args.events).resolve()
    result = extract_usage(path, completed=not args.in_flight)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "ok" else 2


def self_test(_: argparse.Namespace) -> int:
    tests = Path(__file__).parent / "tests"
    return subprocess.call(
        [sys.executable, "-m", "unittest", "discover", "-s", str(tests), "-v"],
        cwd=Path(__file__).parent.parent,
    )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="capture one ROOT or raw Codex process")
    run.add_argument("--run-id", required=True, type=lambda value: valid_id(value, "run-id"))
    run.add_argument("--task", required=True)
    run.add_argument("--arm", required=True, choices=("harness", "raw"))
    run.add_argument("--cwd", required=True)
    run.add_argument("--prompt-file", required=True)
    run.add_argument("--results-dir", default="results")
    run.add_argument("--model", required=True)
    run.add_argument("--reasoning-effort", required=True)
    run.add_argument("--service-tier", required=True)
    run.add_argument("--sandbox", default="workspace-write", choices=("read-only", "workspace-write", "danger-full-access"))
    run.add_argument("--budget-tokens", type=int,
                     help="optional diagnostic launch cap; omit for uncapped reportable runs")
    run.add_argument("--interrupted-run", action="store_true",
                     help="declare planned pause/resume segments; label this run non-standard")
    run.add_argument("--harness-runtime", help="active ROOT workspace's .harness-runtime")
    run.add_argument("--harness-dir", help="isolated harness installation selecting this ROOT workspace")
    run.add_argument("--trust-project-hooks", action="store_true",
                     help="run the prepared project hooks without an interactive trust prompt; required for harness runs")
    run.add_argument("--watchdog-hours", type=float, required=True,
                     help="hard wall-clock limit; stops Codex and shuts down harness lanes")
    run.set_defaults(handler=run_codex, inhibit_sleep=True)

    resume = commands.add_parser("resume", help="continue a sealed task run in the same Codex session")
    resume.add_argument("--task-file", dest="resume_task_file", required=True)
    resume.add_argument("--prompt-file", required=True)
    resume.add_argument("--continue-open-runtime", action="store_true",
                        help="continue a sealed ROOT turn with only cleanup-proven review-pending lanes")
    resume.set_defaults(handler=run_codex, inhibit_sleep=True)

    for name, handler, help_text in (
        ("snapshot", show_snapshot, "read the current aggregate and invocation evidence"),
        ("gate", gate, "allow another launch only while known usage is below B"),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--task-file", required=True)
        command.set_defaults(handler=handler)

    finish = commands.add_parser("finalize", help="write one immutable task evidence archive")
    finish.add_argument("--task-file", required=True)
    finish.add_argument("--output", required=True)
    finish.set_defaults(handler=archive)

    single = commands.add_parser("summarize", help="inspect one Codex JSONL transcript")
    single.add_argument("--events", required=True)
    single.add_argument("--in-flight", action="store_true")
    single.set_defaults(handler=summarize)

    check = commands.add_parser("self-test", help="run offline ledger tests only")
    check.set_defaults(handler=self_test)
    return root


def main() -> int:
    try:
        args = parser().parse_args()
        return args.handler(args)
    except (LedgerError, FileExistsError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
