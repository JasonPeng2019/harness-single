"""Direct v2 launcher binding for Codex."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PROVIDER_ID = "codex"
ADAPTER_VERSION = "codex-v2"

_LAUNCH_CONFIG_KEYS = frozenset({"reasoning_effort", "service_tier"})


def validate_launch_config(*, model: str, launch_config: dict[str, Any]) -> dict[str, str]:
    """Validate every Codex model preference before a provider can start."""
    if not isinstance(model, str) or not model.strip():
        raise ValueError("Codex model must be configured")
    if not isinstance(launch_config, dict):
        raise ValueError("Codex launch_config must be an object")
    missing = sorted(_LAUNCH_CONFIG_KEYS - set(launch_config))
    if missing:
        raise ValueError(f"Codex launch_config is missing: {', '.join(missing)}")
    unsupported = sorted(set(launch_config) - _LAUNCH_CONFIG_KEYS)
    if unsupported:
        raise ValueError(f"Codex launch_config has unsupported options: {', '.join(unsupported)}")
    normalized: dict[str, str] = {}
    for key in sorted(_LAUNCH_CONFIG_KEYS):
        value = launch_config[key]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Codex launch_config {key} must be a non-empty string")
        normalized[key] = value.strip()
    return normalized


def build_argv(
    *,
    model: str,
    launch_config: dict[str, Any],
    worktree: str,
    prompt_path: str,
    session_id: str | None = None,
    resume: bool = False,
) -> list[str]:
    """Build the provider-owned, stdin-prompted Codex launch vector."""
    del prompt_path
    configured = validate_launch_config(model=model, launch_config=launch_config)
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
            f"model_reasoning_effort={json.dumps(configured['reasoning_effort'])}",
            "-c",
            f"service_tier={json.dumps(configured['service_tier'])}",
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
    if raw_type in {"turn.failed", "turn.cancelled"}:
        parsed["non_retryable_failure"] = True
    return parsed
