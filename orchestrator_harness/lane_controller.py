"""Small, externally invoked controller for one persistent Codex lane turn.

This module deliberately does not schedule lanes or inspect the firmware server.  It validates a
manager-written invocation, launches one child, and leaves factual process/output records for the
read-only harness to observe.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import ProcessInfo, iso_utc
from .processes import process_snapshot


class InvocationError(ValueError):
    pass


def _utc() -> str:
    return iso_utc(datetime.now(timezone.utc)) or ""


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    data = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    try:
        with temporary.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _append_event(path: Path, value: dict[str, Any]) -> None:
    data = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "ab", closefd=True) as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _safe_path(value: object, *, root: Path, name: str, must_exist: bool = False) -> Path:
    if not isinstance(value, str) or not value:
        raise InvocationError(f"{name} must be a non-empty path string")
    candidate = Path(value).expanduser().resolve(strict=False)
    if not _inside(candidate, root):
        raise InvocationError(f"{name} escapes its allowed root")
    if must_exist and not candidate.is_file():
        raise InvocationError(f"{name} is not an existing regular file")
    return candidate


def _string(raw: dict[str, Any], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise InvocationError(f"{key} must be a non-empty string")
    return value.strip()


def _string_list(value: object, name: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise InvocationError(f"{name} must be a list of non-empty strings")
    return list(value)


@dataclass(frozen=True)
class Invocation:
    action: str
    run_root: Path
    workspace: Path
    prompt_path: Path
    prompt_sha256: str
    policy_path: Path
    policy_sha256: str
    label: str
    doer: str
    task: str
    phase: str
    lane_id: str
    leases: list[str]
    board_tokens: list[str]
    mcp_servers: list[str]
    server_snapshot: dict[str, Any]
    model: str
    reasoning_effort: str
    service_tier: str
    codex_command: list[str]
    config_overrides: list[str]
    requested_thread_id: str | None
    status_path: Path
    jsonl_path: Path
    stderr_path: Path
    last_message_path: Path
    event_log: Path


def load_invocation(path: Path) -> Invocation:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise InvocationError(f"cannot read invocation: {exc}") from exc
    if not isinstance(raw, dict):
        raise InvocationError("invocation root must be an object")
    action = _string(raw, "action").lower()
    if action not in {"start", "resume"}:
        raise InvocationError("action must be start or resume")
    run_root_value = raw.get("run_root")
    if not isinstance(run_root_value, str) or not run_root_value:
        raise InvocationError("run_root must be a path")
    run_root = Path(run_root_value).resolve(strict=True)
    workspace = (run_root / ".agent-workspace").resolve(strict=False)
    if workspace.parent != run_root:
        raise InvocationError("invalid run workspace")
    workspace.mkdir(exist_ok=True)
    prompt_path = _safe_path(raw.get("prompt_path"), root=run_root, name="prompt_path", must_exist=True)
    outputs = raw.get("output_paths")
    if not isinstance(outputs, dict):
        raise InvocationError("output_paths must be an object")
    status_path = _safe_path(outputs.get("status"), root=workspace, name="status")
    jsonl_path = _safe_path(outputs.get("jsonl"), root=workspace, name="jsonl")
    stderr_path = _safe_path(outputs.get("stderr"), root=workspace, name="stderr")
    last_message_path = _safe_path(outputs.get("last_message"), root=workspace, name="last_message")
    label = _string(raw, "label")
    expected = {
        "status": f"{label}_controller.status.json",
        "jsonl": f"{label}_codex.jsonl",
    }
    if status_path.name != expected["status"] or jsonl_path.name != expected["jsonl"]:
        raise InvocationError("controller status/JSONL names must match the lane label")
    server_snapshot = raw.get("server_snapshot")
    if not isinstance(server_snapshot, dict):
        raise InvocationError("server_snapshot must be an object")
    model_settings = raw.get("model_settings")
    if not isinstance(model_settings, dict):
        raise InvocationError("model_settings must be an object")
    command = raw.get("codex_command", ["codex"])
    command = _string_list(command, "codex_command")
    overrides = _string_list(raw.get("config_overrides", []), "config_overrides")
    requested_thread = raw.get("resume_thread_id")
    if requested_thread is not None and (not isinstance(requested_thread, str) or not requested_thread.strip()):
        raise InvocationError("resume_thread_id must be a non-empty string when supplied")
    suite_root = Path(__file__).resolve().parent.parent
    policy_path = suite_root / ".agent-workspace" / "AUTONOMOUS_EXECUTION_POLICY.md"
    sidecar_path = suite_root / ".agent-workspace" / "AUTONOMOUS_EXECUTION_POLICY.sha256"
    policy_sha256 = _string(raw, "policy_sha256").lower()
    prompt_sha256 = _string(raw, "prompt_sha256").lower()
    if any(len(value) != 64 or any(char not in "0123456789abcdef" for char in value) for value in (policy_sha256, prompt_sha256)):
        raise InvocationError("policy_sha256 and prompt_sha256 must be SHA-256 hex digests")
    try:
        policy_bytes = policy_path.read_bytes()
        sidecar = sidecar_path.read_text(encoding="utf-8").split()[0].lower()
        prompt_bytes = prompt_path.read_bytes()
    except (OSError, IndexError) as exc:
        raise InvocationError(f"cannot verify policy-bound prompt: {exc}") from exc
    if hashlib.sha256(policy_bytes).hexdigest() != policy_sha256 or sidecar != policy_sha256:
        raise InvocationError("canonical policy file or sidecar does not match policy_sha256")
    if hashlib.sha256(prompt_bytes).hexdigest() != prompt_sha256:
        raise InvocationError("prompt bytes do not match prompt_sha256")
    try:
        prompt_text = prompt_bytes.decode("utf-8-sig")
        policy_text = policy_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise InvocationError("policy-bound prompt must be UTF-8") from exc
    markers = (
        "## AUTHORITATIVE ZERO-OPERATOR OVERRIDE",
        f"Policy SHA-256: `{policy_sha256}`",
        "## END AUTHORITATIVE ZERO-OPERATOR OVERRIDE",
        "## FINAL PRECEDENCE REMINDER",
        f"Policy `{policy_sha256}` and the latest signed run amendment control.",
    )
    if any(marker not in prompt_text for marker in markers) or policy_text not in prompt_text:
        raise InvocationError("prompt is not the required policy-bound prompt composition")
    log_root = suite_root / "multi-agent-logs" / "orchestrator-harness"
    event_log = _safe_path(raw.get("lane_event_log"), root=log_root, name="lane_event_log")
    if event_log.name != "LANE_EVENTS.jsonl":
        raise InvocationError("lane_event_log must be named LANE_EVENTS.jsonl")
    event_log.parent.mkdir(parents=True, exist_ok=True)
    return Invocation(
        action, run_root, workspace, prompt_path, prompt_sha256, policy_path, policy_sha256, label, _string(raw, "doer"), _string(raw, "task"),
        _string(raw, "phase"), _string(raw, "declared_lane_id"), _string_list(raw.get("leases", []), "leases"),
        _string_list(raw.get("board_tokens", []), "board_tokens"), _string_list(raw.get("mcp_servers", []), "mcp_servers"), server_snapshot, _string(model_settings, "model"), _string(model_settings, "reasoning_effort"),
        _string(model_settings, "service_tier"), command, overrides, requested_thread.strip() if requested_thread else None,
        status_path, jsonl_path, stderr_path, last_message_path, event_log,
    )


def _identity(pid: int, *, parent: int | None = None, timeout: float = 5.0) -> ProcessInfo:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snapshot = process_snapshot()
        item = snapshot.by_pid.get(pid) if snapshot.complete else None
        if item is not None and item.created_utc is not None and (parent is None or item.ppid == parent):
            return item
        time.sleep(0.05)
    raise RuntimeError(f"cannot establish exact process identity for PID {pid}")


def _thread_id(line: bytes) -> str | None:
    try:
        value = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict) or value.get("type") != "thread.started":
        return None
    for key in ("thread_id", "threadId"):
        item = value.get(key)
        if isinstance(item, str) and item:
            return item
    return None


def _read_prior_thread(invocation: Invocation) -> str | None:
    if not invocation.status_path.is_file():
        return None
    try:
        value = json.loads(invocation.status_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    thread = value.get("thread_id") if isinstance(value, dict) else None
    return thread if isinstance(thread, str) and thread else None


def run(invocation: Invocation) -> int:
    prompt = invocation.prompt_path.read_bytes()
    if not prompt:
        raise InvocationError("policy-bound prompt is empty")
    prior_thread = _read_prior_thread(invocation)
    if invocation.action == "resume":
        thread = invocation.requested_thread_id or prior_thread
        if not thread or (prior_thread and invocation.requested_thread_id and prior_thread != invocation.requested_thread_id):
            raise InvocationError("resume requires the persisted lane thread ID")
    else:
        thread = None
    controller = _identity(os.getpid())
    argv = [*invocation.codex_command, "exec"]
    if invocation.action == "resume":
        argv.extend(["resume", thread])  # type: ignore[arg-type]
    argv.extend([
        "--dangerously-bypass-approvals-and-sandbox", "--ignore-user-config", "--skip-git-repo-check",
        "-c", 'approval_policy="never"', "-c", 'approvals_reviewer="user"', "-m", invocation.model,
        "-c", f'model_reasoning_effort="{invocation.reasoning_effort}"', "-c", f'service_tier="{invocation.service_tier}"',
    ])
    for override in invocation.config_overrides:
        argv.extend(["-c", override])
    argv.extend(["--json", "--output-last-message", str(invocation.last_message_path)])
    if invocation.action == "start":
        argv.extend(["--cd", str(invocation.run_root)])
    argv.append("-")
    state: dict[str, Any] = {
        "schema": "orchestrator-lane-controller/v1", "state": "LAUNCH_FAILED", "started_utc": _utc(),
        "controller_pid": controller.pid, "controller_started_utc": iso_utc(controller.created_utc),
        "controller_created_utc": iso_utc(controller.created_utc), "codex_pid": None, "codex_started_utc": None,
        "doer": invocation.doer, "task": invocation.task, "phase": invocation.phase,
        "declared_lane_id": invocation.lane_id, "thread_id": thread, "leases": invocation.leases,
        "board_tokens": invocation.board_tokens, "mcp_servers": invocation.mcp_servers,
        "server_snapshot": invocation.server_snapshot, "jsonl_path": str(invocation.jsonl_path),
        "stderr_path": str(invocation.stderr_path), "last_message_path": str(invocation.last_message_path),
        "prompt_path": str(invocation.prompt_path), "prompt_sha256": invocation.prompt_sha256,
        "policy_path": str(invocation.policy_path), "policy_sha256": invocation.policy_sha256,
        "launcher_settings": {"model": invocation.model, "model_reasoning_effort": invocation.reasoning_effort,
            "service_tier": invocation.service_tier, "sandbox": "danger-full-access", "approval_policy": "never",
            "approvals_reviewer": "user", "jsonl": True, "ephemeral": False, "action": invocation.action, "argv": argv[:-1]},
    }
    lock = threading.Lock()
    process: subprocess.Popen[bytes] | None = None
    try:
        with invocation.jsonl_path.open("wb") as jsonl, invocation.stderr_path.open("wb") as stderr:
            process = subprocess.Popen(argv, cwd=invocation.run_root, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            child = _identity(process.pid, parent=controller.pid)
            state.update({"state": "RUNNING_CODEX", "codex_pid": child.pid, "codex_started_utc": iso_utc(child.created_utc), "codex_created_utc": iso_utc(child.created_utc)})
            _atomic_json(invocation.status_path, state)
            _append_event(invocation.event_log, {"utc": _utc(), "event": "CODEX_STARTED", "label": invocation.label, "declared_lane_id": invocation.lane_id, "controller_pid": controller.pid, "codex_pid": child.pid})
            assert process.stdin is not None and process.stdout is not None and process.stderr is not None
            process.stdin.write(prompt); process.stdin.close()
            def drain(source: Any, destination: Any, parse: bool) -> None:
                nonlocal state
                for line in iter(source.readline, b""):
                    destination.write(line); destination.flush(); os.fsync(destination.fileno())
                    if parse:
                        found = _thread_id(line)
                        if found:
                            with lock:
                                state["thread_id"] = found
                                _atomic_json(invocation.status_path, state)
            out_thread = threading.Thread(target=drain, args=(process.stdout, jsonl, True), daemon=True)
            err_thread = threading.Thread(target=drain, args=(process.stderr, stderr, False), daemon=True)
            out_thread.start(); err_thread.start()
            exit_code = process.wait(); out_thread.join(); err_thread.join()
            process.stdout.close(); process.stderr.close()
        if invocation.action == "start" and not state.get("thread_id"):
            state.update({"state": "LAUNCH_FAILED", "exit_code": exit_code, "ended_utc": _utc(), "error": "Codex exited without thread.started/thread_id; inspect stderr_path"})
            _atomic_json(invocation.status_path, state)
            _append_event(invocation.event_log, {"utc": _utc(), "event": "LAUNCH_FAILED", "label": invocation.label, "declared_lane_id": invocation.lane_id, "exit_code": exit_code})
            return 1
        state.update({"state": "CODEX_EXITED", "exit_code": exit_code, "ended_utc": _utc()})
        _atomic_json(invocation.status_path, state)
        _append_event(invocation.event_log, {"utc": _utc(), "event": "CODEX_EXITED", "label": invocation.label, "declared_lane_id": invocation.lane_id, "exit_code": exit_code, "thread_id": state.get("thread_id")})
        return exit_code
    except KeyboardInterrupt:
        if process is not None and process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        state.update({"state": "CONTROLLER_INTERRUPTED", "ended_utc": _utc()})
        _atomic_json(invocation.status_path, state)
        _append_event(invocation.event_log, {"utc": _utc(), "event": "CONTROLLER_INTERRUPTED", "label": invocation.label, "declared_lane_id": invocation.lane_id})
        return 130
    except Exception as exc:
        if process is not None and process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        state.update({"state": "CONTROLLER_FAILED" if state.get("codex_pid") else "LAUNCH_FAILED", "ended_utc": _utc(), "error": str(exc)})
        _atomic_json(invocation.status_path, state)
        _append_event(invocation.event_log, {"utc": _utc(), "event": state["state"], "label": invocation.label, "declared_lane_id": invocation.lane_id, "error": str(exc)})
        return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Launch one observable Codex lane turn")
    parser.add_argument("invocation", type=Path)
    args = parser.parse_args(argv)
    try:
        return run(load_invocation(args.invocation))
    except InvocationError as exc:
        print(f"lane-controller invocation error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
