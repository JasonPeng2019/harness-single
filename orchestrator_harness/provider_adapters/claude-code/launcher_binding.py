"""Registered launcher binding for the Claude Code provider.

The controller loads this module from
``orchestrator_harness/provider_adapters/<provider_id>/launcher_binding.py``
and checks that ``PROVIDER_ID`` matches before use.  ``build_argv`` assembles
the headless launch vector and ``parse_line`` reads one transcript line into
controller facts; both delegate to the current runtime adapter so the shipped
flags and parsing stay conformant with ``orchestrator_harness.provider``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from orchestrator_harness.provider import (
    ClaudeCodeProviderAdapter,
    ProviderLaunchSpec,
)

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
    """Assemble the Claude Code headless launch argument vector.

    The prompt is transported on stdin; ``prompt_path`` is accepted for
    contract symmetry with the controller.
    """
    adapter = ClaudeCodeProviderAdapter()
    spec = ProviderLaunchSpec(
        action="resume" if resume else "start",
        command=("claude",),
        model=model,
        reasoning_effort="medium",
        service_tier="priority",
        session_id=session_id,
        run_root=Path(worktree),
        last_message_path=Path(worktree) / ".agent-workspace" / "last-message.txt",
        approval_policy="never",
    )
    return adapter.build_argv(spec)


def parse_line(line: str) -> dict[str, Any] | None:
    """Read one Claude Code transcript line into controller facts."""
    event = ClaudeCodeProviderAdapter().parse_transcript_line(line.encode("utf-8"))
    if event is None:
        return None
    parsed: dict[str, Any] = {}
    if event.session_id:
        parsed["session_id"] = event.session_id
    if event.kind in {"COMPLETED", "FAILED", "CANCELLED"}:
        parsed["message"] = event.detail or event.raw_type or event.kind
    return parsed
