"""Direct v2 launcher binding for Claude Code."""

from __future__ import annotations

import json
from typing import Any

PROVIDER_ID = "claude-code"
ADAPTER_VERSION = "claude-code-v2"

_LAUNCH_CONFIG_KEYS = frozenset({"effort"})


def validate_launch_config(*, model: str, launch_config: dict[str, Any]) -> dict[str, str]:
    """Validate every Claude model preference before a provider can start."""
    if not isinstance(model, str) or not model.strip():
        raise ValueError("Claude Code model must be configured")
    if not isinstance(launch_config, dict):
        raise ValueError("Claude Code launch_config must be an object")
    missing = sorted(_LAUNCH_CONFIG_KEYS - set(launch_config))
    if missing:
        raise ValueError(f"Claude Code launch_config is missing: {', '.join(missing)}")
    unsupported = sorted(set(launch_config) - _LAUNCH_CONFIG_KEYS)
    if unsupported:
        raise ValueError(
            f"Claude Code launch_config has unsupported options: {', '.join(unsupported)}"
        )
    effort = launch_config["effort"]
    if not isinstance(effort, str) or not effort.strip():
        raise ValueError("Claude Code launch_config effort must be a non-empty string")
    return {"effort": effort.strip()}


def build_argv(
    *,
    model: str,
    launch_config: dict[str, Any],
    worktree: str,
    prompt_path: str,
    session_id: str | None = None,
    resume: bool = False,
) -> list[str]:
    """Build the provider-owned, stdin-prompted Claude Code launch vector."""
    del worktree, prompt_path
    configured = validate_launch_config(model=model, launch_config=launch_config)
    argv = ["claude", "--print", "--output-format", "stream-json", "--verbose"]
    if resume:
        if not session_id:
            raise ValueError("Claude Code resume requires a session ID")
        argv.extend(["--resume", session_id])
    argv.extend(["--model", model, "--effort", configured["effort"]])
    argv.extend(["--permission-mode", "bypassPermissions"])
    return argv


def parse_line(line: str) -> dict[str, Any] | None:
    """Parse one Claude Code JSON transcript line into controller facts."""
    try:
        value = json.loads(line)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    raw_type = value.get("type")
    session_id = value.get("session_id") or value.get("sessionId")
    parsed: dict[str, Any] = {}
    if isinstance(session_id, str) and session_id:
        parsed["session_id"] = session_id
    if raw_type == "system" and value.get("subtype") == "init":
        return parsed
    if raw_type == "system" and value.get("subtype") == "permission_denied":
        parsed["message"] = value.get("message") or "permission_denied"
        parsed["non_retryable_failure"] = True
        return parsed
    if raw_type != "result":
        return None
    denials = value.get("permission_denials")
    subtype = value.get("subtype")
    if (
        value.get("is_error") is True
        or subtype in {"error", "error_during_execution"}
        or (isinstance(denials, list) and bool(denials))
    ):
        parsed["message"] = value.get("result") or subtype or "error"
        parsed["non_retryable_failure"] = True
        return parsed
    parsed["message"] = "result"
    return parsed
