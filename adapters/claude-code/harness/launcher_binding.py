"""Direct v2 launcher binding for Claude Code."""

from __future__ import annotations

import json
from typing import Any

PROVIDER_ID = "claude-code"
ADAPTER_VERSION = "claude-code-v1"


def build_argv(
    *,
    model: str,
    worktree: str,
    prompt_path: str,
    session_id: str | None = None,
    resume: bool = False,
) -> list[str]:
    """Build the provider-owned, stdin-prompted Claude Code launch vector."""
    del worktree, prompt_path
    argv = ["claude", "--print", "--output-format", "stream-json", "--verbose"]
    if resume:
        if not session_id:
            raise ValueError("Claude Code resume requires a session ID")
        argv.extend(["--resume", session_id])
    if model:
        argv.extend(["--model", model])
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
        return parsed
    parsed["message"] = "result"
    return parsed
