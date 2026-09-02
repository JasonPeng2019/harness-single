"""Direct v2 launcher binding for Codex."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PROVIDER_ID = "codex"
ADAPTER_VERSION = "codex-v1"


def build_argv(
    *,
    model: str,
    worktree: str,
    prompt_path: str,
    session_id: str | None = None,
    resume: bool = False,
) -> list[str]:
    """Build the provider-owned, stdin-prompted Codex launch vector."""
    del prompt_path
    argv = ["codex", "exec"]
    if resume:
        if not session_id:
            raise ValueError("Codex resume requires a session ID")
        argv.extend(["resume", session_id])
    argv.extend(
        [
            "--dangerously-bypass-approvals-and-sandbox",
            "--skip-git-repo-check",
            "-c",
            'approval_policy="never"',
            "-m",
            model,
            "-c",
            'model_reasoning_effort="medium"',
            "-c",
            'service_tier="priority"',
            "--dangerously-bypass-hook-trust",
            "-c",
            "features.hooks=true",
            "-c",
            f'projects.{json.dumps(worktree)}.trust_level="trusted"',
            "--json",
            "--output-last-message",
            str(Path(worktree) / ".agent-workspace" / "last-message.txt"),
        ]
    )
    if not resume:
        argv.extend(["--cd", worktree])
    argv.append("-")
    return argv


def parse_line(line: str) -> dict[str, Any] | None:
    """Parse one Codex JSON transcript line into controller facts."""
    try:
        value = json.loads(line)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    raw_type = value.get("type")
    if raw_type not in {"thread.started", "turn.completed", "turn.failed", "turn.cancelled"}:
        return None
    session_id = value.get("thread_id") or value.get("threadId")
    parsed: dict[str, Any] = {}
    if isinstance(session_id, str) and session_id:
        parsed["session_id"] = session_id
    if raw_type != "thread.started":
        parsed["message"] = raw_type
    return parsed
