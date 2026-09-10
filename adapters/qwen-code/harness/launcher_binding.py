"""Direct v2 launcher binding for Qwen Code."""

from __future__ import annotations

import json
import shutil
from typing import Any

PROVIDER_ID = "qwen-code"
ADAPTER_VERSION = "qwen-code-v1"


def build_argv(
    *,
    model: str,
    worktree: str,
    prompt_path: str,
    session_id: str | None = None,
    resume: bool = False,
) -> list[str]:
    """Build the provider-owned, stdin-prompted Qwen Code launch vector."""
    del worktree, prompt_path
    executable = shutil.which("qwen") or "qwen"
    argv = [executable, "--approval-mode=yolo", "--model", model, "--output-format", "stream-json"]
    if resume:
        if not session_id:
            raise ValueError("Qwen Code resume requires a session ID")
        argv.extend(["--resume", session_id])
    return argv


def parse_line(line: str) -> dict[str, Any] | None:
    """Parse one Qwen Code JSON transcript line into controller facts."""
    try:
        value = json.loads(line)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    raw_type = value.get("type")
    subtype = value.get("subtype")
    session_id = value.get("session_id") or value.get("sessionId")
    if raw_type not in {"system", "result", "interrupt"} and subtype != "interrupt":
        return None
    parsed: dict[str, Any] = {}
    if isinstance(session_id, str) and session_id:
        parsed["session_id"] = session_id
    if raw_type == "system" and subtype == "init":
        return parsed
    if raw_type == "interrupt" or subtype == "interrupt":
        parsed["message"] = value.get("message") or "interrupt"
        parsed["non_retryable_failure"] = True
        return parsed
    if raw_type == "result":
        parsed["message"] = value.get("result") or subtype or "result"
        if value.get("is_error") is True or subtype in {"error", "error_during_execution"}:
            parsed["non_retryable_failure"] = True
        return parsed
    return None
