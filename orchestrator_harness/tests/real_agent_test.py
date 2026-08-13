"""One focused real-agent journey with a Windows-native public controller.

The synthetic Git repository, invocation, controller state, lifecycle and
result stay on Windows.  Ubuntu WSL prepares an isolated provider side and
the lane controller launches only ``wsl.exe`` as its provider child.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from orchestrator_harness.cli import watch_once
from orchestrator_harness.config import load_config
from orchestrator_harness.lane_controller import load_invocation
from orchestrator_harness.lane_lifecycle import lifecycle_registry_path
from orchestrator_harness.models import iso_utc
from orchestrator_harness.processes import process_snapshot, targeted_process_query
from orchestrator_harness.public_launch import launch_lane_controller
from orchestrator_harness.tests.wsl_identity import validate_cross_os_identity_relation


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SUPPORT = Path(__file__).resolve().parent / "support"
DEFAULT_DISTRO = "Ubuntu"
DEFAULT_CODEX_ROOT = "/opt/orchestrator-harness-codex"
LANE_ID = "real-agent"
WORKER_ID = "real-agent-001"
if str(SUPPORT) not in sys.path:
    sys.path.insert(0, str(SUPPORT))
from wsl_real_agent_driver import validate_prepared_state  # type: ignore[import-not-found]


def wsl_path(_distro: str, path: Path) -> str:
    """Translate one Windows path for WSL's explicit host bind source."""

    resolved = path.resolve()
    drive = resolved.drive
    if len(drive) != 2 or drive[1] != ":":
        raise RuntimeError(f"real-agent test requires a drive-letter path: {resolved}")
    return f"/mnt/{drive[0].lower()}/{resolved.as_posix()[2:].lstrip('/')}"


def _json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _run(cwd: Path, *argv: str) -> str:
    result = subprocess.run(
        list(argv), cwd=cwd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, encoding="utf-8", errors="replace",
        check=False, timeout=30, shell=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Git command failed: {argv[0]} {argv[1:]}")
    return result.stdout.strip()


def _read_object(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _wait_json(path: Path, predicate: Any, *, timeout: float, process: subprocess.Popen[str] | None = None) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = _read_object(path)
        if value is not None and predicate(value):
            return value
        if process is not None and process.poll() is not None and not path.exists():
            raise RuntimeError(f"prepared Linux side exited before publishing {path.name}")
        time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for {path}")


def _make_repository(root: Path) -> tuple[Path, str, str]:
    repo = root / "synthetic-repository"
    repo.mkdir()
    _run(repo, "git", "init", "-b", LANE_ID)
    _run(repo, "git", "config", "user.email", "real-agent@example.invalid")
    _run(repo, "git", "config", "user.name", "Synthetic Real Agent")
    (repo / ".gitignore").write_text(".agent-workspace/\n", encoding="utf-8")
    (repo / "task.txt").write_text(
        "Create marker.txt, commit it, and publish the exact coding result.\n",
        encoding="utf-8",
    )
    _run(repo, "git", "add", ".gitignore", "task.txt")
    _run(repo, "git", "commit", "-m", "Initialize synthetic real-agent repository")
    common_dir = _run(repo, "git", "rev-parse", "--git-common-dir")
    return repo, _run(repo, "git", "rev-parse", "HEAD"), str((repo / common_dir).resolve())


def _make_invocation(
    repo: Path, base_commit: str, common_dir: str, runtime: Path, prompt: Path,
    provider_command: list[str],
) -> Path:
    workspace = repo / ".agent-workspace"
    workspace.mkdir(exist_ok=True)
    status = workspace / "real_agent_controller.status.json"
    invocation = {
        "schema": "orchestrator-coding-invocation/v1",
        "action": "start",
        "runtime_root": str(runtime),
        "resource_lock_root": str(runtime / "coding-resource-locks"),
        "run_root": str(repo),
        "repository": {
            "common_dir": common_dir,
            "worktree_root": str(repo),
            "branch": LANE_ID,
            "base_commit": base_commit,
        },
        "prompt_path": str(prompt),
        "prompt_sha256": hashlib.sha256(prompt.read_bytes()).hexdigest(),
        "output_paths": {
            "status": str(status),
            "jsonl": str(workspace / "real_agent_codex.jsonl"),
            "stderr": str(workspace / "real_agent_codex.stderr.log"),
            "last_message": str(workspace / "real_agent_last_message.txt"),
        },
        "event_log_path": str(runtime / "LANE_EVENTS.jsonl"),
        "lane_id": LANE_ID,
        "worker_invocation_id": WORKER_ID,
        "task": "Complete the public real-agent release route in the synthetic repository",
        "phase": "public-route",
        "exclusive_resources": [],
        "codex": {
            "command": provider_command,
            "model": "gpt-5.6-terra",
            "reasoning_effort": "high",
            "service_tier": "priority",
            "sandbox": "danger-full-access",
            "approval_policy": "never",
            "config_overrides": ["mcp_servers={}"],
        },
    }
    path = workspace / "invocation.json"
    _json(path, invocation)
    return path


def _prompt(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """Work only in the synthetic repository at the current working directory.
Do not inspect the parent, host authentication, source checkout, hardware, MCP,
USB, or any external service except the already configured provider session.

Create marker.txt with exactly one line: public route passed. Commit it with
message Complete public route task. Then write .agent-workspace/RESULT.json
with exactly this coding result shape and the exact branch/tip after your
commit:
{
  "schema": "orchestrator-lane-result/v1",
  "lane_id": "real-agent",
  "worker_invocation_id": "real-agent-001",
  "branch": "real-agent",
  "commit": "<exact git rev-parse HEAD>",
  "outcome": "PASS",
  "summary": "public route task passed",
  "checks": [{"name": "synthetic public route", "outcome": "PASS", "summary": "marker committed"}]
}
Leave the repository clean and reply PUBLIC_ROUTE_COMPLETE.
""",
        encoding="utf-8",
    )


def _event_types(event_log: Path) -> list[str]:
    types: list[str] = []
    try:
        for line in event_log.read_text(encoding="utf-8").splitlines():
            value = json.loads(line)
            if isinstance(value, dict) and isinstance(value.get("type"), str):
                types.append(value["type"])
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        pass
    return types


def _redacted_controller_evidence(status: dict[str, Any], result: dict[str, Any], lifecycle: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": status,
        "result": result,
        "lifecycle": lifecycle,
        "resource_claims": status.get("held_resource_claims", []),
        "cleanup": status.get("cleanup"),
        "process_boundary": status.get("process_boundary"),
        "credentials_in_evidence": False,
        "transcript_persisted": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Windows public route with one isolated Ubuntu provider")
    parser.add_argument("--distro", default=os.environ.get("ORCH_HARNESS_WSL_DISTRO", DEFAULT_DISTRO))
    parser.add_argument("--codex-root", default=os.environ.get("ORCH_HARNESS_WSL_CODEX_ROOT", DEFAULT_CODEX_ROOT))
    parser.add_argument("--model", default="gpt-5.6-terra")
    args = parser.parse_args()
    if os.name != "nt":
        raise RuntimeError("real-agent public route requires the Windows controller host")
    if args.distro != "Ubuntu":
        raise RuntimeError("repair-003 real-agent oracle is pinned to Ubuntu")
    if args.codex_root != DEFAULT_CODEX_ROOT:
        raise RuntimeError("real-agent oracle requires the isolated pinned Codex root")
    auth = Path.home() / ".codex" / "auth.json"
    if not auth.is_file():
        raise RuntimeError("Codex authentication file is unavailable")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    evidence = (REPOSITORY_ROOT / ".real-agent" / stamp).resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    run_id = uuid.uuid4().hex
    nonce = uuid.uuid4().hex + uuid.uuid4().hex
    temp_name = tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-")
    temp_root = Path(temp_name).resolve()
    prep_process: subprocess.Popen[str] | None = None
    result: dict[str, Any] | None = None
    failure: BaseException | None = None
    try:
        repo, base_commit, common_dir = _make_repository(temp_root)
        runtime = temp_root / "runtime"
        runtime.mkdir()
        state_path = temp_root / "prepared-state.json"
        release_signal = temp_root / "release-signal.json"
        bridge_evidence = temp_root / "bridge-evidence.json"
        claim_path = temp_root / "prepared-claim.json"
        prompt_path = repo / ".agent-workspace" / "real_agent_prompt.md"
        _prompt(prompt_path)
        driver = wsl_path(args.distro, SUPPORT / "wsl_real_agent_driver.py")
        guarded = wsl_path(args.distro, SUPPORT / "wsl_guarded_entry.py")
        provider = wsl_path(args.distro, SUPPORT / "wsl_codex_provider.py")
        cgroup_launcher = wsl_path(args.distro, SUPPORT / "cgroup_exec.py")
        prep_command = [
            "wsl.exe", "-d", args.distro, "-u", "root", "--", "python3", guarded,
            "--run-id", run_id, "--driver", driver, "--",
            "--mode", "prepare", "--nonce", nonce, "--invocation-id", WORKER_ID,
            "--state", wsl_path(args.distro, state_path),
            "--release-signal", wsl_path(args.distro, release_signal),
            "--evidence", wsl_path(args.distro, evidence),
            "--auth-json", wsl_path(args.distro, auth),
            "--workspace", wsl_path(args.distro, repo),
            "--codex-root", args.codex_root,
            "--cgroup-launcher", cgroup_launcher,
            "--provider-entry", provider,
            "--wait-seconds", "900",
        ]
        prep_process = subprocess.Popen(
            prep_command, cwd=str(REPOSITORY_ROOT), stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=False,
        )
        prepared = _wait_json(state_path, lambda value: value.get("status") == "READY", timeout=90, process=prep_process)
        validate_prepared_state(prepared, nonce=nonce, invocation_id=WORKER_ID)
        if prepared.get("credentials_in_state") is not False:
            raise RuntimeError("prepared state claims to contain credentials")
        provider_command = [
            "wsl.exe", "-d", args.distro, "-u", "root", "--", "python3", provider,
            "--state", wsl_path(args.distro, state_path), "--nonce", nonce,
            "--invocation-id", WORKER_ID, "--evidence", wsl_path(args.distro, bridge_evidence),
            "--claim", wsl_path(args.distro, claim_path),
        ]
        invocation = _make_invocation(repo, base_commit, common_dir, runtime, prompt_path, provider_command)
        # Deterministic preflight: the controller must accept the invocation before it is launched.
        load_invocation(invocation)
        receipt_path = repo / ".agent-workspace" / "real_agent_operator.receipt.json"
        status_path = repo / ".agent-workspace" / "real_agent_controller.status.json"
        receipt = launch_lane_controller(
            invocation, receipt=receipt_path, cwd=REPOSITORY_ROOT,
            label="real-agent-coding-controller", role="coding-lane-controller",
            expected_state_path=status_path,
        )
        controller_pid = receipt.get("pid")
        controller_created = receipt.get("created_utc")
        if not isinstance(controller_pid, int) or not isinstance(controller_created, str):
            raise RuntimeError("Windows controller receipt lacks exact identity")
        provider_observed = False
        provider_identity: dict[str, Any] | None = None
        event_types: list[str] = []
        watcher_failures: list[str] = []
        config_path = temp_root / "watcher.json"
        watcher_root = temp_root / "runtime" / "watcher-state"
        _json(config_path, {
            "suite_root": str(temp_root), "run_globs": ["synthetic-repository"],
            "workspace_relpath": ".agent-workspace", "output_dir": str(watcher_root),
            "poll_interval_seconds": 0.1, "watch_timeout_seconds": 900,
            "request_warning_seconds": 900, "request_critical_seconds": 899,
            "process_start_tolerance_seconds": 2, "stable_read_delay_seconds": 0.02,
        })
        config = load_config(config_path)
        deadline = time.monotonic() + 900
        status: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            candidate = _read_object(status_path)
            if candidate is not None:
                status = candidate
                provider_pid = candidate.get("provider_pid")
                provider_created = candidate.get("provider_created_utc")
                if isinstance(provider_pid, int) and isinstance(provider_created, str) and not provider_observed:
                    query = targeted_process_query(provider_pid, expected_parent_pid=controller_pid)
                    if query.process is not None and query.process.created_utc is not None:
                        observed_created = iso_utc(query.process.created_utc)
                        if observed_created != provider_created:
                            raise RuntimeError("Windows wsl.exe provider creation identity changed")
                        command_line = query.process.command_line.lower()
                        if "wsl.exe" not in command_line and "wslhost" not in command_line:
                            raise RuntimeError("provider identity is not the controller-owned wsl.exe child")
                        provider_identity = {
                            "platform": "windows", "pid": provider_pid,
                            "created_utc": provider_created, "nonce": nonce,
                            "invocation_id": WORKER_ID, "parent_pid": controller_pid,
                        }
                        provider_observed = True
            try:
                _, observed_events = watch_once(config, no_write=False)
                event_types.extend(item.get("type") for item in observed_events if isinstance(item, dict) and isinstance(item.get("type"), str))
            except Exception as watcher_error:
                # A watcher observation is diagnostic; native controller state
                # and lifecycle evidence remain decisive for this route.
                watcher_failures.append(f"{type(watcher_error).__name__}: {watcher_error}")
            controller_query = targeted_process_query(controller_pid)
            terminal = (
                status is not None
                and status.get("state") in {"CODEX_EXITED", "PROVIDER_EXITED", "CONTROLLER_FAILED", "LAUNCH_FAILED"}
                and status.get("ended_utc") is not None
            )
            if terminal and controller_query.process is None:
                break
            time.sleep(0.1)
        else:
            raise TimeoutError("Windows public controller did not reach terminal state")
        if status is None:
            raise RuntimeError("controller status was never published")
        if not provider_observed or provider_identity is None:
            raise RuntimeError("exact Windows wsl.exe provider identity was not observed")
        if status.get("state") not in {"CODEX_EXITED", "PROVIDER_EXITED"} or status.get("exit_code") != 0:
            raise RuntimeError(f"public controller did not exit successfully: {status.get('state')}")
        if status.get("result_valid") is not True or not isinstance(status.get("result_validation"), dict) or status["result_validation"].get("state") != "VALID":
            raise RuntimeError("controller did not validate a distinct successful result")
        if status.get("held_resource_claims") != [] or not all(status.get(key) is True for key in ("helpers_complete", "direct_child_reaped", "resource_claim_release_safe")):
            raise RuntimeError("native lifecycle/claim cleanup evidence is incomplete")
        boundary = status.get("process_boundary")
        if not isinstance(boundary, dict) or boundary.get("complete") is not True or boundary.get("live_members"):
            raise RuntimeError("native controller process boundary is incomplete")
        result_path = repo / ".agent-workspace" / "RESULT.json"
        result_value = _read_object(result_path)
        if result_value is None:
            raise RuntimeError("controller result is missing")
        actual_branch = _run(repo, "git", "branch", "--show-current")
        actual_head = _run(repo, "git", "rev-parse", "HEAD")
        dirty = _run(repo, "git", "status", "--porcelain=v1", "--untracked-files=all")
        validation = status["result_validation"]
        if result_value.get("lane_id") != LANE_ID or result_value.get("worker_invocation_id") != WORKER_ID or result_value.get("branch") != actual_branch or result_value.get("commit") != actual_head or validation.get("commit") != actual_head or actual_branch != LANE_ID or dirty:
            raise RuntimeError("result identity is not exact and distinct from stale state")
        lifecycle_path = lifecycle_registry_path(repo, LANE_ID, WORKER_ID)
        lifecycle = _read_object(lifecycle_path)
        if lifecycle is None or lifecycle.get("lifecycle", {}).get("complete") is not True or lifecycle.get("lifecycle", {}).get("helpers_complete") is not True:
            raise RuntimeError("native lifecycle registry is incomplete")
        bridge = _wait_json(bridge_evidence, lambda value: value.get("status") == "PASS", timeout=30)
        prepared_cleaned = _wait_json(state_path, lambda value: value.get("status") == "CLEANED", timeout=60, process=prep_process)
        if prepared_cleaned.get("cleanup_complete") is not True or prepared_cleaned.get("credentials_in_state") is not False:
            raise RuntimeError("prepared Linux cleanup was not complete")
        linux_bridge = bridge.get("linux_bridge")
        if not isinstance(linux_bridge, dict):
            raise RuntimeError("Linux bridge identity evidence is missing")
        validate_cross_os_identity_relation(
            provider_identity, linux_bridge, nonce=nonce, invocation_id=WORKER_ID
        )
        if bridge.get("sandbox", {}).get("mnt_c_exposed") is not False or bridge.get("sandbox", {}).get("usb_exposed") is not False or bridge.get("cleanup_complete") is not True:
            raise RuntimeError("Linux sandbox/cleanup evidence is incomplete")
        required_events = {"CONTROLLER_ACTIVE", "CONTROLLER_EXITED", "RESOURCE_RELEASE_POSSIBLE"}
        if not required_events.issubset(set(event_types)):
            raise RuntimeError(
                f"native watcher events are incomplete: {sorted(set(event_types))}"
                + (f"; watcher failures: {watcher_failures}" if watcher_failures else "")
            )
        result = {
            "status": "PASS", "completed_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "route": "public_launch -> operator_launch -> lane_controller -> wsl.exe provider bridge",
            "distro": args.distro, "codex_root": args.codex_root,
            "controller_identity": {"platform": "windows", "pid": controller_pid, "created_utc": controller_created},
            "provider_identity": provider_identity, "linux_bridge_identity": linux_bridge,
            "cross_os_relation": "nonce-and-invocation-bound-independent-identities",
            "controller_evidence": _redacted_controller_evidence(status, result_value, lifecycle),
            "bridge_evidence": bridge, "prepared_cleanup": prepared_cleaned,
            "event_types": sorted(set(event_types)), "credentials_persisted": False,
            "transcript_persisted": False,
        }
    except BaseException as exc:
        failure = exc
    finally:
        if prep_process is not None:
            if prep_process.poll() is None:
                # Release is also safe on a failed public route: it cannot
                # leave the Linux preparation waiter behind.
                release_signal.write_text("release\n", encoding="utf-8")
            try:
                prep_process.wait(timeout=90)
            except subprocess.TimeoutExpired:
                prep_process.kill()
                prep_process.wait(timeout=10)
        if failure is not None:
            _json(evidence / "REAL_AGENT_TEST_RESULT.json", {
                "status": "FAIL", "completed_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "failure_type": type(failure).__name__, "credentials_persisted": False,
                "transcript_persisted": False,
            })
        elif result is not None:
            _json(evidence / "REAL_AGENT_TEST_RESULT.json", result)
        if failure is not None:
            # Retain the exact controller-side objects so a failed live
            # route stays inspectable instead of being silently destroyed.
            failure_evidence = evidence / "controller-failure-evidence"
            failure_evidence.mkdir(parents=True, exist_ok=True)
            for name in (
                "prepared-state.json", "prepared-claim.json", "release-signal.json",
                "bridge-evidence.json", "real_agent_operator.receipt.json",
            ):
                source = temp_root / name
                if source.is_file():
                    shutil.copy2(source, failure_evidence / name)
            for relative in (
                "synthetic-repository/.agent-workspace/real_agent_controller.status.json",
                "synthetic-repository/.agent-workspace/real_agent_operator.receipt.json",
                "synthetic-repository/.agent-workspace/invocation.json",
                "synthetic-repository/.agent-workspace/RESULT.json",
                "synthetic-repository/.agent-workspace/real_agent_codex.stderr.log",
                "synthetic-repository/.agent-workspace/real_agent_codex.jsonl",
            ):
                source = temp_root / relative
                if source.is_file():
                    shutil.copy2(source, failure_evidence / source.name)
            try:
                secrets = json.loads(auth.read_text(encoding="utf-8")).get("tokens", {})
                secret_values = [
                    value for value in secrets.values()
                    if isinstance(value, str) and value
                ]
            except Exception:
                secret_values = []
            for file in failure_evidence.rglob("*"):
                if not file.is_file():
                    continue
                raw = file.read_bytes()
                changed = raw
                for secret in secret_values:
                    changed = changed.replace(secret.encode(), b"<REDACTED>")
                if changed != raw:
                    file.write_bytes(changed)
        # No runtime or authentication material is retained in the source
        # checkout; only the explicitly redacted evidence directory remains.
        shutil.rmtree(temp_root, ignore_errors=True)
    if failure is not None:
        raise failure
    assert result is not None
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
