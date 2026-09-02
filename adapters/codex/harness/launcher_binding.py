"""Registered launcher binding for the Codex provider.

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

from orchestrator_harness.provider import CodexProviderAdapter, ProviderLaunchSpec

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
    """Assemble the Codex headless launch argument vector.

    The prompt is transported on stdin, so the vector ends with ``-``;
    ``prompt_path`` is accepted for contract symmetry with the controller.
    """
    adapter = CodexProviderAdapter()
    spec = ProviderLaunchSpec(
        action="resume" if resume else "start",
        command=("codex",),
        model=model,
        reasoning_effort="medium",
        service_tier="priority",
        session_id=session_id,
        run_root=Path(worktree),
        last_message_path=Path(worktree) / ".agent-workspace" / "last-message.txt",
        trusted_project_root=Path(worktree),
        approval_policy="never",
    )
    return adapter.build_argv(spec)


def parse_line(line: str) -> dict[str, Any] | None:
    """Read one Codex transcript line into controller facts."""
    event = CodexProviderAdapter().parse_transcript_line(line.encode("utf-8"))
    if event is None:
        return None
    parsed: dict[str, Any] = {}
    if event.session_id:
        parsed["session_id"] = event.session_id
    if event.kind in {"COMPLETED", "FAILED", "CANCELLED"}:
        parsed["message"] = event.detail or event.raw_type or event.kind
    return parsed
