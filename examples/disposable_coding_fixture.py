"""Disposable end-to-end fixture for the ordinary coding workflow.

Run this file directly from the portable repository root. It uses only temporary
directories, local Python processes, and Git. No Codex service or firmware records
are required.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

FIXTURE_RESOURCE = "service:fixture-database"
HARNESS_ROOT = Path(__file__).resolve().parent.parent


class FixtureError(RuntimeError):
    pass


def _run(
    argv: Sequence[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    expected: tuple[int, ...] = (0,),
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        list(argv),
        cwd=cwd,
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        shell=False,
    )
    if completed.returncode not in expected:
        raise FixtureError(
            f"command failed ({completed.returncode}): {list(argv)!r}\n"
            f"stdout: {completed.stdout[-2000:]}\nstderr: {completed.stderr[-2000:]}"
        )
    return completed


def _git(cwd: Path, *args: str) -> str:
    return _run(("git", *args), cwd=cwd).stdout.strip()


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _result(
    lane_id: str,
    worker_invocation_id: str,
    branch: str,
    commit: str,
    summary: str,
) -> dict[str, object]:
    return {
        "schema": "orchestrator-lane-result/v1",
        "lane_id": lane_id,
        "worker_invocation_id": worker_invocation_id,
        "branch": branch,
        "commit": commit,
        "outcome": "PASS",
        "summary": summary,
        "checks": [
            {
                "name": "fixture Python tests",
                "command": "python -m unittest -v",
                "outcome": "PASS",
            }
        ],
    }


def _fake_worker(argv: Sequence[str]) -> int:
    if len(argv) < 3:
        return 2
    lane, delay_text = argv[1:3]
    delay = float(delay_text)
    _ = sys.stdin.buffer.read()
    print(
        json.dumps({"type": "thread.started", "thread_id": f"fixture-{lane}"}),
        flush=True,
    )
    workspace = Path.cwd() / ".agent-workspace"
    workspace.mkdir(exist_ok=True)
    (workspace / "PARALLEL_CHECKPOINT.md").write_text(
        f"# {lane} checkpoint\n\nFake worker started.\n", encoding="utf-8"
    )
    time.sleep(delay)
    if lane in {"alpha", "beta"}:
        value = 1 if lane == "alpha" else 2
        (Path.cwd() / f"feature_{lane}.py").write_text(
            f"def value() -> int:\n    return {value}\n", encoding="utf-8"
        )
        _git(Path.cwd(), "add", f"feature_{lane}.py")
        _git(Path.cwd(), "commit", "-m", f"Add {lane} feature")
    elif lane == "merge":
        _run((sys.executable, "-m", "unittest", "-v"), cwd=Path.cwd())
    else:
        return 2
    if lane != "beta":
        branch = _git(Path.cwd(), "branch", "--show-current")
        commit = _git(Path.cwd(), "rev-parse", "HEAD")
        _write_json(
            workspace / "RESULT.json",
            _result(lane, f"{lane}-001", branch, commit, f"{lane} fixture work passed"),
        )
    last_message_index = next(
        (index for index, value in enumerate(argv) if value == "--output-last-message"),
        None,
    )
    if last_message_index is not None and last_message_index + 1 < len(argv):
        Path(argv[last_message_index + 1]).write_text(
            f"fixture worker {lane} complete\n", encoding="utf-8"
        )
    return 0


def _invocation(
    *,
    lane: str,
    worktree: Path,
    common_dir: Path,
    base_commit: str,
    runtime: Path,
    delay: float,
    merge_inputs: list[str] | None = None,
) -> Path:
    workspace = worktree / ".agent-workspace"
    workspace.mkdir(exist_ok=True)
    prompt = workspace / "worker-prompt.md"
    prompt.write_text(
        f"Complete the disposable {lane} coding lane.\n", encoding="utf-8"
    )
    branch = _git(worktree, "branch", "--show-current")
    invocation = {
        "schema": "orchestrator-coding-invocation/v1",
        "action": "start",
        "runtime_root": str(runtime),
        "resource_lock_root": str(runtime / "coding-resource-locks"),
        "run_root": str(worktree),
        "repository": {
            "common_dir": str(common_dir),
            "worktree_root": str(worktree),
            "branch": branch,
            "base_commit": base_commit,
            "merge_inputs": merge_inputs or [],
        },
        "prompt_path": str(prompt),
        "prompt_sha256": hashlib.sha256(prompt.read_bytes()).hexdigest(),
        "output_paths": {
            "status": str(workspace / "fixture_controller.status.json"),
            "jsonl": str(workspace / "fixture_codex.jsonl"),
            "stderr": str(workspace / "fixture_codex.stderr.log"),
            "last_message": str(workspace / "fixture_last_message.txt"),
        },
        "event_log_path": str(runtime / "LANE_EVENTS.jsonl"),
        "lane_id": lane,
        "worker_invocation_id": f"{lane}-001",
        "task": f"Disposable {lane} coding work",
        "phase": "merge" if lane == "merge" else "implementation",
        "exclusive_resources": [] if lane == "merge" else [FIXTURE_RESOURCE],
        "codex": {
            "command": [
                sys.executable,
                str(Path(__file__).resolve()),
                "_fake_worker",
                lane,
                str(delay),
            ],
            "model": "fixture-model",
            "reasoning_effort": "low",
            "service_tier": "fixture",
            "sandbox": "workspace-write",
            "approval_policy": "never",
            "config_overrides": [],
        },
    }
    path = workspace / "invocation.json"
    _write_json(path, invocation)
    return path


def _controller_env() -> dict[str, str]:
    env = dict(os.environ)
    prior = env.get("PYTHONPATH")
    env["PYTHONPATH"] = str(HARNESS_ROOT) + (os.pathsep + prior if prior else "")
    return env


def _start_controller(invocation: Path) -> subprocess.Popen[str]:
    return subprocess.Popen(
        (sys.executable, "-m", "orchestrator_harness.lane_controller", str(invocation)),
        cwd=HARNESS_ROOT,
        env=_controller_env(),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        shell=False,
    )


def _wait_for_status(
    path: Path, predicate: Any, *, timeout: float = 10.0
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            time.sleep(0.05)
            continue
        if isinstance(value, dict) and predicate(value):
            return value
        time.sleep(0.05)
    raise FixtureError(f"timed out waiting for status condition in {path}")


def _finish_controller(
    process: subprocess.Popen[str], *, expected: tuple[int, ...]
) -> None:
    try:
        stdout, stderr = process.communicate(timeout=20)
    except subprocess.TimeoutExpired as exc:
        process.terminate()
        process.wait(timeout=5)
        raise FixtureError(f"controller PID {process.pid} did not exit") from exc
    if process.returncode not in expected:
        raise FixtureError(
            f"controller PID {process.pid} exited {process.returncode}\n"
            f"stdout: {stdout[-2000:]}\nstderr: {stderr[-2000:]}"
        )


def _harness(
    config: Path, *args: str, expected: tuple[int, ...] = (0,)
) -> subprocess.CompletedProcess[str]:
    return _run(
        (sys.executable, "-m", "orchestrator_harness", "--config", str(config), *args),
        cwd=HARNESS_ROOT,
        env=_controller_env(),
        expected=expected,
    )


def run_fixture(root: Path) -> dict[str, object]:
    root = root.resolve()
    project = root / "project"
    worktrees = root / "worktrees"
    runtime = root / "runtime"
    project.mkdir(parents=True)
    worktrees.mkdir()
    runtime.mkdir()
    _git(project, "init", "-b", "main")
    _git(project, "config", "user.email", "fixture@example.invalid")
    _git(project, "config", "user.name", "Disposable Fixture")
    (project / ".gitignore").write_text(
        ".agent-workspace/\n__pycache__/\n*.py[cod]\n", encoding="utf-8"
    )
    (project / "test_features.py").write_text(
        "import unittest\n\n"
        "from feature_alpha import value as alpha\n"
        "from feature_beta import value as beta\n\n"
        "class FeatureTests(unittest.TestCase):\n"
        "    def test_combined_value(self) -> None:\n"
        "        self.assertEqual(alpha() + beta(), 3)\n\n"
        "if __name__ == '__main__':\n"
        "    unittest.main()\n",
        encoding="utf-8",
    )
    _git(project, "add", ".gitignore", "test_features.py")
    _git(project, "commit", "-m", "Initial fixture project")
    base_commit = _git(project, "rev-parse", "HEAD")
    common_dir = (project / ".git").resolve()
    alpha = worktrees / "alpha"
    beta = worktrees / "beta"
    merge = worktrees / "merge"
    _git(project, "worktree", "add", "-b", "lane/alpha", str(alpha), base_commit)
    _git(project, "worktree", "add", "-b", "lane/beta", str(beta), base_commit)

    config = root / "harness.json"
    _write_json(
        config,
        {
            "suite_root": str(root),
            "run_globs": ["worktrees/*"],
            "workspace_relpath": ".agent-workspace",
            "output_dir": str(runtime / "manager-epoch"),
            "poll_interval_seconds": 0.05,
            "watch_timeout_seconds": 5,
            "manager_review_interval_seconds": 300,
            "lane_no_progress_seconds": 600,
            "manager_heartbeat_timeout_seconds": 420,
            "request_warning_seconds": 120,
            "request_critical_seconds": 30,
            "process_start_tolerance_seconds": 2,
            "attention_logging_enabled": False,
            "attention_epoch_id": "disposable-coding-fixture",
        },
    )
    _harness(config, "watch", "--once")

    alpha_invocation = _invocation(
        lane="alpha",
        worktree=alpha,
        common_dir=common_dir,
        base_commit=base_commit,
        runtime=runtime,
        delay=5.0,
    )
    beta_invocation = _invocation(
        lane="beta",
        worktree=beta,
        common_dir=common_dir,
        base_commit=base_commit,
        runtime=runtime,
        delay=0.0,
    )
    _write_json(
        beta / ".agent-workspace" / "RESULT.json",
        _result(
            "beta", "stale-beta-000", "lane/beta", base_commit, "stale fixture result"
        ),
    )

    alpha_process = _start_controller(alpha_invocation)
    alpha_status = alpha / ".agent-workspace" / "fixture_controller.status.json"
    _wait_for_status(
        alpha_status, lambda value: bool(value.get("held_resource_claims"))
    )
    beta_process = _start_controller(beta_invocation)
    beta_status = beta / ".agent-workspace" / "fixture_controller.status.json"
    _wait_for_status(
        beta_status, lambda value: value.get("state") == "WAITING_RESOURCE"
    )
    _finish_controller(alpha_process, expected=(0,))
    _finish_controller(beta_process, expected=(1,))
    stale_status = json.loads(beta_status.read_text(encoding="utf-8"))
    if stale_status.get("result_validation", {}).get("state") != "INVALID":
        raise FixtureError("stale result was not rejected")

    beta_commit = _git(beta, "rev-parse", "HEAD")
    _write_json(
        beta / ".agent-workspace" / "RESULT.json",
        _result(
            "beta", "beta-001", "lane/beta", beta_commit, "beta fixture work passed"
        ),
    )
    scan = json.loads(_harness(config, "scan", "--no-write").stdout)
    lanes = scan.get("lanes", [])
    coding_lanes = [lane for lane in lanes if lane.get("lane_id") in {"alpha", "beta"}]
    if len(coding_lanes) != 2:
        raise FixtureError(
            "scan did not preserve two independent coding lane identities"
        )
    if any(
        lane.get("mcp_servers") or lane.get("board_tokens") for lane in coding_lanes
    ):
        raise FixtureError(
            "ordinary coding fixture unexpectedly required firmware records"
        )

    delivered = _harness(config, "watch", "--until-actionable", "--timeout", "5")
    event = json.loads(delivered.stdout.splitlines()[-1])
    event_id = event.get("event_id")
    if not isinstance(event_id, str) or event_id == "WATCH_TIMEOUT":
        raise FixtureError("native wait did not return a durable actionable event")
    acknowledged = json.loads(_harness(config, "ack", "--event-id", event_id).stdout)
    if (
        acknowledged.get("acknowledged") is not True
        or acknowledged.get("event_id") != event_id
    ):
        raise FixtureError("exact event acknowledgement failed")

    _git(project, "worktree", "add", "-b", "integration/merge", str(merge), base_commit)
    _git(merge, "merge", "--no-edit", "lane/alpha")
    _git(merge, "merge", "--no-edit", "lane/beta")
    merge_invocation = _invocation(
        lane="merge",
        worktree=merge,
        common_dir=common_dir,
        base_commit=base_commit,
        runtime=runtime,
        delay=0.0,
        merge_inputs=["lane/alpha", "lane/beta"],
    )
    merge_process = _start_controller(merge_invocation)
    _finish_controller(merge_process, expected=(0,))
    tests = _run((sys.executable, "-m", "unittest", "-v"), cwd=merge)
    remaining_claims = list((runtime / "coding-resource-locks").glob("*.json"))
    if remaining_claims:
        raise FixtureError(f"resource claims were not cleaned up: {remaining_claims}")
    events_path = runtime / "LANE_EVENTS.jsonl"
    event_count = len(events_path.read_text(encoding="utf-8").splitlines())
    return {
        "schema": "orchestrator-disposable-coding-fixture/v1",
        "base_commit": base_commit,
        "branches": ["lane/alpha", "lane/beta", "integration/merge"],
        "coding_lane_count": 3,
        "contention_observed": True,
        "stale_result_rejected": True,
        "valid_results": ["alpha", "beta", "merge"],
        "acknowledged_event_id": event_id,
        "lane_event_count": event_count,
        "python_tests": "PASS" if tests.returncode == 0 else "FAIL",
        "resource_claims_remaining": 0,
        "firmware_records": 0,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if args[:1] == ["_fake_worker"]:
        return _fake_worker(args)
    keep_root: Path | None = None
    if args:
        if len(args) != 2 or args[0] != "--keep":
            print(
                "usage: disposable_coding_fixture.py [--keep DIRECTORY]",
                file=sys.stderr,
            )
            return 2
        keep_root = Path(args[1]).resolve()
        if keep_root.exists():
            print("--keep DIRECTORY must not already exist", file=sys.stderr)
            return 2
        keep_root.mkdir(parents=True)
    try:
        if keep_root is not None:
            result = run_fixture(keep_root)
            result["cleanup"] = "retained by --keep"
        else:
            with tempfile.TemporaryDirectory(
                prefix="orchestrator-coding-fixture-"
            ) as temporary:
                temporary_path = Path(temporary)
                result = run_fixture(temporary_path)
            result["cleanup"] = "temporary repository removed"
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (FixtureError, OSError, ValueError, json.JSONDecodeError) as exc:
        if keep_root is not None:
            print(f"fixture failed; retained at {keep_root}: {exc}", file=sys.stderr)
        else:
            print(f"fixture failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
