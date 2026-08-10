"""Bounded provider adapters for child launch and transcript/session facts."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol


class ProviderAdapterError(ValueError):
    """Raised when a provider-specific launch or record is invalid."""


@dataclass(frozen=True)
class ProviderLaunchSpec:
    action: str
    command: tuple[str, ...]
    model: str
    reasoning_effort: str
    service_tier: str
    session_id: str | None
    run_root: Path
    last_message_path: Path
    config_overrides: tuple[str, ...] = ()
    sandbox: str = "workspace-write"
    approval_policy: str = "never"
    worker_invocation_id: str | None = None
    permission_mode: str | None = None
    allowed_tools: tuple[str, ...] = ()
    disallowed_tools: tuple[str, ...] = ()
    mcp_config: str | Mapping[str, Any] | list[Any] | None = None
    provider_options: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProviderEvent:
    """Provider-neutral facts extracted from one provider transcript line."""

    kind: str
    session_id: str | None = None
    outcome: str | None = None
    raw_type: str | None = None
    detail: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.kind in {"COMPLETED", "FAILED", "CANCELLED"}


class ProviderAdapter(Protocol):
    provider_id: str

    def build_argv(self, spec: ProviderLaunchSpec) -> list[str]: ...

    def encode_prompt(self, prompt: bytes) -> bytes: ...

    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None: ...

    def terminal_outcome(self, event: ProviderEvent | None, exit_code: int) -> str: ...


def _json_line(line: bytes) -> Mapping[str, Any] | None:
    try:
        value = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, Mapping) else None


def _nonempty(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


class CodexProviderAdapter:
    """Own the current Codex command and transcript shapes."""

    provider_id = "codex"

    def build_argv(self, spec: ProviderLaunchSpec) -> list[str]:
        if spec.action not in {"start", "resume"}:
            raise ProviderAdapterError("Codex action must be start or resume")
        argv = [*spec.command, "exec"]
        if spec.action == "resume":
            if not spec.session_id:
                raise ProviderAdapterError("Codex resume requires a session ID")
            argv.extend(["resume", spec.session_id])
        argv.append("--dangerously-bypass-approvals-and-sandbox")
        argv.extend(
            [
                "--ignore-user-config",
                "--skip-git-repo-check",
                "-c",
                f'approval_policy="{spec.approval_policy}"',
                "-m",
                spec.model,
                "-c",
                f'model_reasoning_effort="{spec.reasoning_effort}"',
                "-c",
                f'service_tier="{spec.service_tier}"',
            ]
        )
        if spec.worker_invocation_id is None:
            argv.extend(["-c", 'approvals_reviewer="user"'])
        for override in spec.config_overrides:
            argv.extend(["-c", override])
        argv.extend(["--json", "--output-last-message", str(spec.last_message_path)])
        if spec.action == "start":
            argv.extend(["--cd", str(spec.run_root)])
        argv.append("-")
        return argv

    def encode_prompt(self, prompt: bytes) -> bytes:
        if not isinstance(prompt, bytes) or not prompt:
            raise ProviderAdapterError("Codex prompt must be non-empty bytes")
        return prompt

    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None:
        value = _json_line(line)
        if value is None:
            return None
        raw_type = _nonempty(value.get("type"))
        if raw_type != "thread.started" and raw_type not in {
            "turn.completed",
            "turn.failed",
            "turn.cancelled",
        }:
            return None
        session_id = _nonempty(value.get("thread_id")) or _nonempty(value.get("threadId"))
        if raw_type == "thread.started":
            return ProviderEvent("STARTED", session_id=session_id, raw_type=raw_type)
        kind = {
            "turn.completed": "COMPLETED",
            "turn.failed": "FAILED",
            "turn.cancelled": "CANCELLED",
        }[raw_type]
        return ProviderEvent(kind, session_id=session_id, outcome=kind, raw_type=raw_type)

    def terminal_outcome(self, event: ProviderEvent | None, exit_code: int) -> str:
        if event is not None and event.kind in {"FAILED", "CANCELLED"}:
            return event.kind
        return "COMPLETED" if exit_code == 0 else "FAILED"


class ClaudeCodeProviderAdapter:
    """Own Claude Code print/stream-JSON flags and record shapes."""

    provider_id = "claude-code"

    def build_argv(self, spec: ProviderLaunchSpec) -> list[str]:
        if spec.action not in {"start", "resume"}:
            raise ProviderAdapterError("Claude Code action must be start or resume")
        argv = [*spec.command, "--print", "--output-format", "stream-json"]
        if spec.action == "resume":
            if not spec.session_id:
                raise ProviderAdapterError("Claude Code resume requires a session ID")
            argv.extend(["--resume", spec.session_id])
        if spec.model:
            argv.extend(["--model", spec.model])
        if spec.permission_mode:
            argv.extend(["--permission-mode", spec.permission_mode])
        for tool in spec.allowed_tools:
            argv.extend(["--allowedTools", tool])
        for tool in spec.disallowed_tools:
            argv.extend(["--disallowedTools", tool])
        if spec.mcp_config:
            mcp_value = (
                spec.mcp_config
                if isinstance(spec.mcp_config, str)
                else json.dumps(spec.mcp_config, sort_keys=True, separators=(",", ":"))
            )
            argv.extend(["--mcp-config", mcp_value])
        return argv

    def encode_prompt(self, prompt: bytes) -> bytes:
        if not isinstance(prompt, bytes) or not prompt:
            raise ProviderAdapterError("Claude Code prompt must be non-empty bytes")
        return prompt

    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None:
        value = _json_line(line)
        if value is None:
            return None
        raw_type = _nonempty(value.get("type"))
        session_id = _nonempty(value.get("session_id")) or _nonempty(value.get("sessionId"))
        if raw_type == "system" and value.get("subtype") == "init":
            return ProviderEvent("STARTED", session_id=session_id, raw_type=raw_type)
        if raw_type != "result":
            return None
        subtype = _nonempty(value.get("subtype"))
        is_error = value.get("is_error") is True or subtype in {
            "error",
            "error_during_execution",
        }
        if is_error:
            return ProviderEvent(
                "FAILED",
                session_id=session_id,
                outcome="FAILED",
                raw_type=raw_type,
                detail=_nonempty(value.get("result")) or subtype,
            )
        return ProviderEvent("COMPLETED", session_id=session_id, outcome="COMPLETED", raw_type=raw_type)

    def terminal_outcome(self, event: ProviderEvent | None, exit_code: int) -> str:
        if event is not None and event.kind in {"FAILED", "CANCELLED"}:
            return event.kind
        return "COMPLETED" if exit_code == 0 else "FAILED"


_ADAPTERS: dict[str, ProviderAdapter] = {
    "codex": CodexProviderAdapter(),
    "claude-code": ClaudeCodeProviderAdapter(),
}


def provider_adapter(provider_id: str) -> ProviderAdapter:
    try:
        return _ADAPTERS[provider_id]
    except KeyError as exc:
        raise ProviderAdapterError(f"unsupported provider: {provider_id}") from exc


CodexAdapter = CodexProviderAdapter
ClaudeCodeAdapter = ClaudeCodeProviderAdapter
get_provider_adapter = provider_adapter


__all__ = [
    "ClaudeCodeProviderAdapter",
    "ClaudeCodeAdapter",
    "CodexProviderAdapter",
    "CodexAdapter",
    "ProviderAdapter",
    "ProviderAdapterError",
    "ProviderEvent",
    "ProviderLaunchSpec",
    "provider_adapter",
    "get_provider_adapter",
]
