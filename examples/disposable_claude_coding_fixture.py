"""Disposable end-to-end fixture for one real Claude Code lane on an Ollama backend.

This fixture is the claude-code counterpart of ``disposable_coding_fixture.py``
but intentionally simpler: one Git worktree, one lane, the real ``claude`` CLI
as the provider binary.  It exercises the canonical
``orchestrator-worker-invocation/v1`` route for ``provider.id="claude-code"``.

The fixture never sets ANTHROPIC_* process variables itself.  The ONLY
mechanism that redirects the provider child to the local Ollama
Anthropic-compatible endpoint is the invocation's ``provider.config_overrides``
channel: the ``model_provider="ollama"`` alias is translated by
``claude_config_override_env`` into ``ANTHROPIC_BASE_URL`` /
``ANTHROPIC_AUTH_TOKEN`` / ``ANTHROPIC_API_KEY`` child-environment entries that
the lane controller merges after any child-environment isolation.  If that
merge were broken, the claude child would attempt api.anthropic.com and fail
authentication.

Run this file directly from the portable repository root:
    python examples/disposable_claude_coding_fixture.py [--keep DIRECTORY]

The fixture passes when the real claude subprocess launches (PROVIDER_STARTED),
exits cleanly with a real session id, the provider terminal outcome is
COMPLETED, and the worktree contains the committed HELLO.txt artifact.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

# Direct execution puts ``examples/`` first on sys.path. Keep the documented
# root-level command equivalent to importing this fixture from its package.
HARNESS_ROOT = Path(__file__).resolve().parent.parent
if str(HARNESS_ROOT) not in sys.path:
    sys.path.insert(0, str(HARNESS_ROOT))

from orchestrator_harness.models import iso_utc
from orchestrator_harness.processes import (
    WINDOWS_CREATE_NO_WINDOW,
    targeted_process_query,
)
from orchestrator_harness.prompt_bundle import prompt_bundle_record_from_paths
from orchestrator_harness.public_launch import launch_lane_controller
from orchestrator_harness.workspace_overlay import ingest_super_cache, prepare_worktree

# The real claude lane makes a real Ollama round-trip; bound it generously but
# keep the fixture finite.
FIXTURE_CONTROLLER_COMPLETION_SECONDS = 240

# A real model served by the local Ollama (the endpoint the config_overrides
# channel redirects to).  Must be passed through provider.model so the claude
# CLI sends it as ``--model``.
CLAUDE_MODEL = "deepseek-v4-flash:0731-cloud"

# The config-overrides alias that expands (in claude_config_override_env)
# to ANTHROPIC_BASE_URL=http://localhost:11434, ANTHROPIC_AUTH_TOKEN=ollama,
# ANTHROPIC_API_KEY="".  This is the ONLY redirect the fixture relies on.
CLAUDE_OLLAMA_OVERRIDE = 'model_provider="ollama"'


class FixtureError(RuntimeError):
    pass


def _run(
    argv: Sequence[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    expected: tuple[int, ...] = (0,),
    timeout: float = 60,
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
        timeout=timeout,
        shell=False,
        creationflags=WINDOWS_CREATE_NO_WINDOW if os.name == "nt" else 0,
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


def _prepare_overlay_receipt(worktree: Path, runtime: Path) -> Path:
    source = runtime / "fixture-overlay-source"
    harness = runtime / "fixture-overlay-harness"
    source_file = source / "fixture-overlay" / "receipt-proof.txt"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_text("nonempty disposable Claude overlay\n", encoding="utf-8")
    harness.mkdir(parents=True, exist_ok=True)
    ingest_super_cache(source_folder=source, harness_worktree=harness)
    receipt = worktree / ".agent-workspace" / "fixture-overlay.receipt.json"
    prepare_worktree(
        super_cache=harness / "super-cache",
        target_worktree=worktree,
        role="subagent",
        receipt_path=receipt,
    )
    return receipt


def _claude_command() -> list[str]:
    """Resolve the real Claude Code CLI exactly as a caller would on PATH."""
    claude = shutil.which("claude")
    if not claude:
        raise FixtureError("no claude CLI on PATH; install Claude Code")
    return [claude]


def _invocation(
    *,
    lane: str,
    worktree: Path,
    common_dir: Path,
    base_commit: str,
    runtime: Path,
    prompt_text: str,
    command: Sequence[str],
) -> Path:
    workspace = worktree / ".agent-workspace"
    workspace.mkdir(exist_ok=True)
    prompt = workspace / "worker-prompt.md"
    prompt.write_text(prompt_text, encoding="utf-8")
    branch = _git(worktree, "branch", "--show-current")
    worker_id = f"{lane}-001"
    overlay_receipt = _prepare_overlay_receipt(worktree, runtime)
    card = {
        "schema": "orchestrator-task-card/v1",
        "card_id": f"card-{lane}",
        "lane_id": lane,
        "stage_cohort_id": "cohort-claude-fixture",
        "worker_invocation_id": worker_id,
        "objective": "Create HELLO.txt and commit it",
        "revision": "r1",
    }
    workflow_id = "disposable-claude-fixture"
    profile_id = f"profile-{lane}"
    tools = ["Read", "Bash", "Write", "Edit", "Glob", "Grep"]
    # Canonical lanes isolate the provider child environment by default.  The
    # provider_needs grants keep Claude's config/home resolvable under the
    # caller's control (set CLAUDE_CONFIG_DIR when launching the fixture); the
    # config_overrides channel below still supplies the ANTHROPIC_* redirect.
    profile = {
        "schema": "orchestrator-runtime-profile/v1",
        "id": profile_id,
        "role": "implementer",
        "provider": "claude-code",
        "model": CLAUDE_MODEL,
        "tools": tools,
        "capabilities": ["repo"],
        "resources": [],
        "provider_needs": ["CLAUDE_CONFIG_DIR", "HOME"],
    }
    bundle = prompt_bundle_record_from_paths(
        workflow_id=workflow_id,
        task_card_id=card["card_id"],
        profile_id=profile_id,
        paths=(("instructions", prompt),),
        run_root=worktree,
    )
    invocation = {
        "schema": "orchestrator-worker-invocation/v1",
        "action": "start",
        "run_root": str(worktree),
        "runtime_root": str(runtime),
        "lane_id": lane,
        "worker_invocation_id": worker_id,
        "overlay_receipt": str(overlay_receipt),
        "cohort_id": "cohort-claude-fixture",
        "workflow": {"id": workflow_id, "version": "1"},
        "task_card": {
            "id": card["card_id"],
            "revision": card["revision"],
            "sha256": hashlib.sha256(
                json.dumps(card, sort_keys=True, separators=(",", ":")).encode(
                    "utf-8"
                )
            ).hexdigest(),
        },
        "role": "implementer",
        "provider": {
            "id": "claude-code",
            "model": CLAUDE_MODEL,
            "command": list(command),
            "allowed_tools": tools,
            # The child-env channel: the alias expands to ANTHROPIC_BASE_URL
            # / ANTHROPIC_AUTH_TOKEN / ANTHROPIC_API_KEY and is merged into the
            # provider child environment AFTER isolation.  No service_tier or
            # approval_policy may appear here (both raise for claude-code).
            "config_overrides": [CLAUDE_OLLAMA_OVERRIDE],
        },
        "profile": profile,
        "prompt_bundle": bundle,
        "output_paths": {
            "status": str(workspace / "worker_controller.status.json"),
            "jsonl": str(workspace / "worker_claude.jsonl"),
            "stderr": str(workspace / "worker_claude.stderr.log"),
            "last_message": str(workspace / "worker_last_message.txt"),
        },
        "event_log_path": str(runtime / "LANE_EVENTS.jsonl"),
        "resources": [],
        "repository": {
            "common_dir": str(common_dir),
            "worktree_root": str(worktree),
            "branch": branch,
            "base_commit": base_commit,
        },
    }
    path = workspace / "invocation.json"
    _write_json(path, invocation)
    return path


def _start_controller(invocation: Path) -> dict[str, Any]:
    """Use the same operator-launch -> lane-controller path as a manager."""
    raw = json.loads(invocation.read_text(encoding="utf-8"))
    status_path = Path(raw["output_paths"]["status"])
    receipt_path = invocation.parent / "fixture_operator_launch.json"
    return launch_lane_controller(
        invocation,
        receipt=receipt_path,
        cwd=HARNESS_ROOT,
        expected_state_path=status_path,
    )


def _wait_for_status(
    path: Path,
    predicate: Any,
    *,
    timeout: float = FIXTURE_CONTROLLER_COMPLETION_SECONDS,
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


def _finish_controller(receipt: Mapping[str, Any]) -> dict[str, Any]:
    """Wait for the canonical provider lane to reach its terminal state.

    A canonical invocation uses the provider-neutral ``PROVIDER_EXITED``
    terminal success state.
    """
    pid = receipt.get("pid")
    created_utc = receipt.get("created_utc")
    if not isinstance(pid, int) or not isinstance(created_utc, str):
        raise FixtureError(
            f"operator launch receipt has no exact process identity: {receipt}"
        )
    terminal_states = {
        "PROVIDER_EXITED",
        "CONTROLLER_FAILED",
        "LAUNCH_FAILED",
        "PROVIDER_HANDOFF",
        "PROVIDER_OPERATION_UNSUPPORTED",
        "COORDINATION_FAILED",
    }
    deadline = time.monotonic() + FIXTURE_CONTROLLER_COMPLETION_SECONDS
    while time.monotonic() < deadline:
        query = targeted_process_query(pid)
        if query.complete and not query.errors:
            process = query.process
            if process is None or iso_utc(process.created_utc) != created_utc:
                status_path = Path(str(receipt["expected_state_path"]))
                return _wait_for_status(
                    status_path,
                    lambda value: value.get("state") in terminal_states,
                )
        time.sleep(0.05)
    raise FixtureError(
        f"controller PID {pid} with creation identity {created_utc} did not exit"
    )


def _lane_events(runtime: Path) -> list[dict[str, Any]]:
    path = runtime / "LANE_EVENTS.jsonl"
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


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
    (project / "README.md").write_text("# Fixture project\n", encoding="utf-8")
    # Keep the controller workspace out of the model's commit scope.
    (project / ".gitignore").write_text(".agent-workspace/\n", encoding="utf-8")
    _git(project, "add", "README.md", ".gitignore")
    _git(project, "commit", "-m", "Initial fixture project")
    base_commit = _git(project, "rev-parse", "HEAD")
    common_dir = (project / ".git").resolve()
    lane = worktrees / "lane-claude"
    _git(project, "worktree", "add", "-b", "lane/claude-hello", str(lane), base_commit)

    prompt_text = (
        "This is a disposable fixture task. Do exactly this:\n"
        "\n"
        "1. Create a file named HELLO.txt in the current directory whose\n"
        "   content is exactly this single line:\n"
        "\n"
        "   hello from claude\n"
        "\n"
        "2. Stage and commit it:\n"
        "\n"
        "   git add HELLO.txt\n"
        "   git commit -m \"add HELLO.txt\"\n"
        "\n"
        "Then reply with one short line containing the commit hash. Do not\n"
        "modify anything else and do not ask questions.\n"
    )
    invocation = _invocation(
        lane="claude-hello",
        worktree=lane,
        common_dir=common_dir,
        base_commit=base_commit,
        runtime=runtime,
        prompt_text=prompt_text,
        command=_claude_command(),
    )

    receipt = _start_controller(invocation)
    status = _finish_controller(receipt)

    if status.get("state") != "PROVIDER_EXITED":
        raise FixtureError(
            f"controller did not reach PROVIDER_EXITED; state={status.get('state')!r} "
            f"error={status.get('error')!r}"
        )
    if status.get("exit_code") != 0:
        raise FixtureError(
            f"provider exit code {status.get('exit_code')!r} is not 0; "
            f"stderr_path={status.get('stderr_path')!r}"
        )
    outcome = status.get("provider_terminal_outcome")
    if outcome != "COMPLETED":
        raise FixtureError(
            f"provider terminal outcome {outcome!r} is not COMPLETED"
        )
    session_id = status.get("provider_session_id")
    if not isinstance(session_id, str) or not session_id.strip():
        raise FixtureError(
            f"provider_session_id was not recorded: {session_id!r}"
        )

    events = _lane_events(runtime)
    # Lane event rows carry the event name under ``event`` (see lane_controller
    # ``_event``), not ``type``; the claude stream-json lines use ``type``.
    event_types = [row.get("event") for row in events]
    if "PROVIDER_STARTED" not in event_types or "PROVIDER_EXITED" not in event_types:
        raise FixtureError(
            f"lane event log is missing PROVIDER_STARTED/PROVIDER_EXITED: {event_types}"
        )
    started = next(row for row in events if row.get("event") == "PROVIDER_STARTED")
    exited = next(row for row in events if row.get("event") == "PROVIDER_EXITED")
    if not isinstance(started.get("provider_pid"), int):
        raise FixtureError(f"PROVIDER_STARTED has no provider_pid: {started}")
    if exited.get("provider_terminal_outcome") != "COMPLETED":
        raise FixtureError(f"PROVIDER_EXITED outcome wrong: {exited}")
    if exited.get("session_id") != session_id:
        raise FixtureError(
            "PROVIDER_EXITED session_id does not match the recorded provider_session_id"
        )

    # The real artifact: the model must have created and committed HELLO.txt.
    # Models write the fixed content with or without a trailing newline
    # (echo-style writes append one), so compare the stripped content.
    hello = lane / "HELLO.txt"
    if not hello.is_file():
        raise FixtureError("claude lane did not create HELLO.txt")
    hello_text = hello.read_text(encoding="utf-8")
    if hello_text.strip() != "hello from claude":
        raise FixtureError(
            f"HELLO.txt content is not the fixed fixture content: {hello_text!r}"
        )
    head = _git(lane, "rev-parse", "HEAD")
    if head == base_commit:
        raise FixtureError("claude lane made no commit")
    commit_count = int(_git(project, "rev-list", "--count", f"{base_commit}..{head}"))
    if commit_count < 1:
        raise FixtureError("HELLO.txt was not committed by the claude lane")

    return {
        "schema": "orchestrator-disposable-claude-fixture/v1",
        "provider_id": "claude-code",
        "model": CLAUDE_MODEL,
        "config_overrides": [CLAUDE_OLLAMA_OVERRIDE],
        "terminal_state": status.get("state"),
        "provider_terminal_outcome": outcome,
        "provider_exit_code": status.get("exit_code"),
        "provider_session_id": session_id,
        "provider_pid": started.get("provider_pid"),
        "base_commit": base_commit,
        "head_commit": head,
        "commit_count": commit_count,
        "hello_content": hello_text,
        "lane_event_count": len(events),
        "lane_event_types": event_types,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    keep_root: Path | None = None
    if args:
        if len(args) != 2 or args[0] != "--keep":
            print(
                "usage: disposable_claude_coding_fixture.py [--keep DIRECTORY]",
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
                prefix="orchestrator-claude-fixture-"
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
