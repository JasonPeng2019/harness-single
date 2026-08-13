"""Bounded provider adapters for child launch and transcript/session facts.

The versioned adapter contract (``orchestrator-provider-adapter/v1``) keeps
provider semantics inside each adapter: command construction, prompt
transport, result/session parsing, permission mapping, and redacted
provenance.  The generic core only selects a registered adapter and consumes
the declared lifecycle contract.  Codex and Claude Code are maintained
built-ins; a separately registered external CLI adapter becomes selectable
without edits to generic dispatch, workflow, task, event, supervisor, or
cleanup code.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, Protocol

from .stable_io import canonical_json


PROVIDER_ADAPTER_SCHEMA = "orchestrator-provider-adapter/v1"
PROVIDER_HANDOFF_SCHEMA = "orchestrator-provider-handoff/v1"

# The exact content-free, non-preemptive wake text required by REQ-O38.
# It never contains notification payload and never stops, redirects, or
# preempts the active directive.
NOTIFICATION_WAKE_TEXT = (
    "A new assignment requiring response is queued. Do not stop or change your current work. "
    "Do not inspect or act on it now. Complete your current assigned directive. "
    "Only when you are fully ready for a next assignment, inspect the durable notification queue."
)
WAKE_TEXT = NOTIFICATION_WAKE_TEXT

# The eight declared capability dimensions of the versioned contract.
PROVIDER_OPERATION_NAMES = (
    "launch",
    "prompt",
    "event_result",
    "session",
    "resume",
    "permission",
    "configuration",
    "notification",
)

# The closed provider-neutral terminal-outcome vocabulary (REL.R1-001).
# The generic controller may publish PROVIDER_EXITED or return success only
# for one of these adapter-produced outcomes; any other string is a
# controller failure even when the child exited 0 and the result is
# shape-valid.
PROVIDER_TERMINAL_OUTCOMES = frozenset({"COMPLETED", "FAILED", "CANCELLED"})

# Deterministic redaction markers for command provenance.  Provenance never
# carries credentials; any token that looks like a credential is replaced.
_REDACTION_MARKERS = (
    "API_KEY",
    "ACCESS_KEY",
    "PRIVATE_KEY",
    "PASSWORD",
    "SECRET",
    "TOKEN",
    "CREDENTIAL",
)


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
        return self.kind in PROVIDER_TERMINAL_OUTCOMES


class ProviderAdapter(Protocol):
    provider_id: str

    def build_argv(self, spec: ProviderLaunchSpec) -> list[str]: ...

    def encode_prompt(self, prompt: bytes) -> bytes: ...

    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None: ...

    def terminal_outcome(self, event: ProviderEvent | None, exit_code: int) -> str: ...

    def last_message_path(self, run_root: Path, last_message_path: Path) -> Path | None:
        """Optional adapter-owned launch identity extension.

        The default returns ``None`` so the generic launch record stays
        provider-neutral.  Codex overrides this to own its
        ``.agent-workspace/last_message`` spelling without a generic branch.
        """
        return None

    def notification_wake_text(self) -> str | None:
        """The exact content-free wake text, or ``None`` for SAFE_BOUNDARY_ONLY.

        This is part of the executable notification contract: an adapter that
        declares ``notification=True`` must return a non-empty exact wake text;
        an adapter without immediate wake declares ``notification=False`` and
        resolves ``SAFE_BOUNDARY_ONLY`` without this method being called.
        """
        return None

    def redact_argv(self, argv: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        """Return the complete redacted command provenance owned by this adapter.

        The generic core never guesses provider credential spellings: the
        selected adapter replaces every credential-bearing token and value
        while preserving the complete argv shape.  The one returned value is
        used identically for ``ProviderEvidence.command_provenance`` and
        ``launcher_settings.argv`` so the two durable surfaces cannot drift.
        """
        ...

    def deliver_notification(
        self,
        coordinator: Any,
        notice: Any,
        *,
        boundary: str,
    ) -> Any:
        """Deliver the exact content-free wake at a supported safe boundary.

        ``coordinator`` is the per-binding ``DeliveryCoordinator`` and
        ``notice`` its outstanding ``DeliveryNotice``.  Returns the transport
        receipt, or ``None`` when immediate delivery is unavailable
        (SAFE_BOUNDARY_ONLY).  An adapter that declares ``notification=True``
        must override this with one real safe-boundary delivery binding.
        """
        return None


class BaseProviderAdapter:
    """Small default surface so foreign adapters can stay minimal."""

    provider_id: str

    def last_message_path(self, run_root: Path, last_message_path: Path) -> Path | None:
        return None

    def notification_wake_text(self) -> str | None:
        return None

    def redact_argv(self, argv: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        # Convenience default: the deterministic generic marker redaction.
        # This inherited default is NOT sufficient for registration: the
        # selected adapter must explicitly implement redact_argv (a
        # generic-safe adapter may explicitly delegate to redact_command) so
        # provider-specific credential spellings stay adapter-owned and the
        # generic core never guesses flag names.
        return redact_command(argv)

    def deliver_notification(
        self,
        coordinator: Any,
        notice: Any,
        *,
        boundary: str,
    ) -> Any:
        # Default: no immediate safe-boundary delivery (SAFE_BOUNDARY_ONLY).
        del coordinator, notice, boundary
        return None


@dataclass(frozen=True)
class ProviderCapabilities:
    """The truthful declared capability set of one registered adapter."""

    launch: bool
    prompt: bool
    event_result: bool
    session: bool
    resume: bool
    permission: bool
    configuration: bool
    notification: bool

    def __post_init__(self) -> None:
        for name in PROVIDER_OPERATION_NAMES:
            if not isinstance(getattr(self, name), bool):
                raise ProviderAdapterError(f"provider capability {name} must be boolean")

    def supports(self, operation: str) -> bool:
        if operation not in PROVIDER_OPERATION_NAMES:
            raise ProviderAdapterError(f"unknown provider operation: {operation}")
        return bool(getattr(self, operation))

    def as_record(self) -> dict[str, bool]:
        return {name: bool(getattr(self, name)) for name in PROVIDER_OPERATION_NAMES}

    @classmethod
    def codex(cls) -> "ProviderCapabilities":
        return cls(
            launch=True,
            prompt=True,
            event_result=True,
            session=True,
            resume=True,
            permission=True,
            configuration=True,
            notification=True,
        )

    @classmethod
    def claude_code(cls) -> "ProviderCapabilities":
        return cls(
            launch=True,
            prompt=True,
            event_result=True,
            session=True,
            resume=True,
            permission=True,
            configuration=True,
            notification=False,
        )


@dataclass(frozen=True)
class ProviderOperationResult:
    """An actionable classified result for a provider operation.

    Unsupported and unregistered results carry the explicit required external
    actor and action; supported results leave both empty.
    """

    schema: str = PROVIDER_ADAPTER_SCHEMA
    provider_id: str = ""
    operation: str = ""
    supported: bool = False
    reason: str = ""
    actionable: str = ""
    required_actor: str = ""
    required_action: str = ""

    def as_record(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "provider_id": self.provider_id,
            "operation": self.operation,
            "supported": self.supported,
            "reason": self.reason,
            "actionable": self.actionable,
            "required_actor": self.required_actor,
            "required_action": self.required_action,
        }


@dataclass(frozen=True)
class ProviderAdapterRegistration:
    """Identity/version/capability facts bound to one registered adapter."""

    schema: str = PROVIDER_ADAPTER_SCHEMA
    provider_id: str = ""
    version: str = ""
    adapter_class: str = ""
    capabilities: ProviderCapabilities = field(default_factory=ProviderCapabilities.codex)

    def as_record(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "provider_id": self.provider_id,
            "version": self.version,
            "adapter_class": self.adapter_class,
            "capabilities": self.capabilities.as_record(),
        }


@dataclass(frozen=True)
class ProviderEvidence:
    """REQ-O36 evidence: identity, capabilities, digest, and redacted provenance."""

    schema: str = PROVIDER_ADAPTER_SCHEMA
    provider_id: str = ""
    adapter_version: str = ""
    capabilities: Mapping[str, bool] = field(default_factory=dict)
    configuration_digest: str = ""
    attempt_identity: str = ""
    session_id: str | None = None
    command_provenance: tuple[str, ...] = ()

    def as_record(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "provider_id": self.provider_id,
            "adapter_version": self.adapter_version,
            "capabilities": dict(self.capabilities),
            "configuration_digest": self.configuration_digest,
            "attempt_identity": self.attempt_identity,
            "session_id": self.session_id,
            "command_provenance": list(self.command_provenance),
        }


@dataclass(frozen=True)
class ProviderHandoff:
    """A declared same-role structured handoff with no fabricated continuity."""

    schema: str = PROVIDER_HANDOFF_SCHEMA
    role: str = ""
    logical_task_id: str = ""
    worker_invocation_id: str = ""
    provider_id: str = ""
    adapter_version: str = ""
    prior_session_id: str | None = None
    requested_session_id: str | None = None
    reason: str = ""
    continuity: str = "DECLARED_SAME_ROLE_HANDOFF"
    fabricated_continuity: bool = False
    preserved_logical_task_state: bool = True

    def as_record(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "role": self.role,
            "logical_task_id": self.logical_task_id,
            "worker_invocation_id": self.worker_invocation_id,
            "provider_id": self.provider_id,
            "adapter_version": self.adapter_version,
            "prior_session_id": self.prior_session_id,
            "requested_session_id": self.requested_session_id,
            "reason": self.reason,
            "continuity": self.continuity,
            "fabricated_continuity": self.fabricated_continuity,
            "preserved_logical_task_state": self.preserved_logical_task_state,
        }


@dataclass(frozen=True)
class ProviderResumeDecision:
    """RESUME when the adapter and identity support it, else a declared handoff."""

    mode: str
    reason: str
    handoff: ProviderHandoff | None = None

    def as_record(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "reason": self.reason,
            "handoff": self.handoff.as_record() if self.handoff is not None else None,
        }


def _json_line(line: bytes) -> Mapping[str, Any] | None:
    try:
        value = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, Mapping) else None


def _nonempty(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


class CodexProviderAdapter(BaseProviderAdapter):
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

    def notification_wake_text(self) -> str | None:
        return NOTIFICATION_WAKE_TEXT

    def redact_argv(self, argv: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        # Codex command provenance uses the deterministic generic markers; the
        # adapter owns the operation so foreign spellings stay adapter-bound.
        return redact_command(argv)

    def deliver_notification(
        self,
        coordinator: Any,
        notice: Any,
        *,
        boundary: str,
    ) -> Any:
        # The one real safe-boundary delivery binding: the per-binding
        # DeliveryCoordinator delivers the outstanding notice at the supported
        # boundary and returns transport evidence only (never an
        # acknowledgement).
        return coordinator.deliver_at_boundary(notice, boundary=boundary)

    def last_message_path(self, run_root: Path, last_message_path: Path) -> Path | None:
        workspace = run_root.expanduser().resolve(strict=False) / ".agent-workspace"
        effective = last_message_path if last_message_path.is_absolute() else workspace / last_message_path
        return effective.expanduser().resolve(strict=False)


class ClaudeCodeProviderAdapter(BaseProviderAdapter):
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

    def notification_wake_text(self) -> str | None:
        # Claude Code has no immediate in-turn wake boundary; drain is
        # SAFE_BOUNDARY_ONLY and is never fabricated as an immediate wake.
        return None

    def redact_argv(self, argv: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        # Claude Code command provenance uses the deterministic generic
        # markers; the adapter owns the operation so foreign spellings stay
        # adapter-bound.
        return redact_command(argv)


_ADAPTERS: dict[str, ProviderAdapter] = {
    "codex": CodexProviderAdapter(),
    "claude-code": ClaudeCodeProviderAdapter(),
}

_REGISTRY: dict[str, ProviderAdapterRegistration] = {
    "codex": ProviderAdapterRegistration(
        provider_id="codex",
        version="codex-v1",
        adapter_class="CodexProviderAdapter",
        capabilities=ProviderCapabilities.codex(),
    ),
    "claude-code": ProviderAdapterRegistration(
        provider_id="claude-code",
        version="claude-code-v1",
        adapter_class="ClaudeCodeProviderAdapter",
        capabilities=ProviderCapabilities.claude_code(),
    ),
}

_BUILTIN_PROVIDER_IDS = frozenset(_REGISTRY)


def _validate_adapter_contract(provider_id: str, adapter: object) -> None:
    if not isinstance(provider_id, str) or not provider_id.strip():
        raise ProviderAdapterError("provider_id must be a non-empty string")
    for method in (
        "build_argv",
        "encode_prompt",
        "parse_transcript_line",
        "terminal_outcome",
        "redact_argv",
    ):
        if not callable(getattr(adapter, method, None)):
            raise ProviderAdapterError(
                f"provider adapter {provider_id!r} does not implement {method}"
            )
    # PA-ROOT-COMPLETION-016: the selected adapter must explicitly own its
    # redacted command provenance.  The inherited BaseProviderAdapter generic
    # marker fallback cannot satisfy that contract because the generic core
    # never guesses foreign credential spellings; a subclass that does not
    # override redact_argv is rejected at registration, before selection or
    # evidence construction.
    redact = getattr(adapter, "redact_argv", None)
    if getattr(redact, "__func__", None) is BaseProviderAdapter.redact_argv:
        raise ProviderAdapterError(
            f"provider adapter {provider_id!r} must explicitly implement redact_argv; "
            "the inherited generic fallback cannot satisfy the adapter-owned "
            "provenance contract"
        )
    declared = getattr(adapter, "provider_id", None)
    if declared != provider_id:
        raise ProviderAdapterError(
            f"provider adapter {provider_id!r} declares provider_id {declared!r}"
        )


def register_provider_adapter(
    provider_id: str,
    adapter: ProviderAdapter,
    *,
    version: str,
    capabilities: ProviderCapabilities,
    replace: bool = False,
) -> ProviderAdapterRegistration:
    """Register one external CLI adapter without edits to generic core code.

    The adapter becomes selectable through :func:`provider_adapter` and is
    admitted by invocation/profile validation.  Built-in providers cannot be
    replaced unless ``replace=True`` is explicitly requested.
    """
    provider_id = provider_id.strip()
    if not version or not version.strip():
        raise ProviderAdapterError("adapter version must be a non-empty string")
    if not isinstance(capabilities, ProviderCapabilities):
        raise ProviderAdapterError("adapter capabilities must be a ProviderCapabilities record")
    _validate_adapter_contract(provider_id, adapter)
    if capabilities.notification:
        wake = getattr(adapter, "notification_wake_text", None)
        if not callable(wake):
            raise ProviderAdapterError(
                f"provider adapter {provider_id!r} declares notification but has no notification_wake_text"
            )
        try:
            wake_text = wake()
        except Exception as exc:
            raise ProviderAdapterError(
                f"provider adapter {provider_id!r} notification_wake_text failed: {exc}"
            ) from exc
        if wake_text != NOTIFICATION_WAKE_TEXT:
            # REQ-O38: the one exact content-free, non-preemptive wake sentence.
            # A merely non-empty, whitespace-varied, payload-bearing, or
            # preemptive value is rejected at registration.
            raise ProviderAdapterError(
                f"provider adapter {provider_id!r} declares notification without the exact "
                "content-free wake text"
            )
        # PA-R1-002: notification=true is executable only when the adapter
        # supplies one real safe-boundary delivery binding consumed by the
        # production notification lifecycle.  The base default (returns None)
        # is not a delivery; such an adapter must declare notification=False.
        deliver = getattr(adapter, "deliver_notification", None)
        if not callable(deliver):
            raise ProviderAdapterError(
                f"provider adapter {provider_id!r} declares notification but has no "
                "deliver_notification safe-boundary binding"
            )
        if getattr(deliver, "__func__", None) is BaseProviderAdapter.deliver_notification:
            raise ProviderAdapterError(
                f"provider adapter {provider_id!r} declares notification without a real "
                "safe-boundary delivery binding; declare notification=False instead"
            )
    if provider_id in _BUILTIN_PROVIDER_IDS and not replace:
        raise ProviderAdapterError(
            f"provider {provider_id!r} is a maintained built-in; pass replace=True to override"
        )
    registration = ProviderAdapterRegistration(
        provider_id=provider_id,
        version=version.strip(),
        adapter_class=type(adapter).__name__,
        capabilities=capabilities,
    )
    _ADAPTERS[provider_id] = adapter
    _REGISTRY[provider_id] = registration
    return registration


def unregister_provider_adapter(provider_id: str) -> bool:
    """Remove a foreign registration (test isolation); built-ins are retained."""
    provider_id = provider_id.strip()
    if provider_id in _BUILTIN_PROVIDER_IDS:
        raise ProviderAdapterError(f"built-in provider {provider_id!r} cannot be unregistered")
    removed = _REGISTRY.pop(provider_id, None)
    _ADAPTERS.pop(provider_id, None)
    return removed is not None


def provider_registry() -> Mapping[str, ProviderAdapterRegistration]:
    """Immutable snapshot of the current registered adapter set."""
    return MappingProxyType(dict(_REGISTRY))


def provider_adapter(provider_id: str) -> ProviderAdapter:
    try:
        return _ADAPTERS[provider_id]
    except KeyError as exc:
        registered = ", ".join(sorted(_REGISTRY))
        raise ProviderAdapterError(
            f"unsupported provider: {provider_id}; registered providers: {registered}"
        ) from exc


def classify_operation(provider_id: str, operation: str) -> ProviderOperationResult:
    """Return an actionable classified result for one provider operation."""
    if operation not in PROVIDER_OPERATION_NAMES:
        raise ProviderAdapterError(f"unknown provider operation: {operation}")
    registration = _REGISTRY.get(provider_id)
    if registration is None:
        return ProviderOperationResult(
            provider_id=provider_id,
            operation=operation,
            supported=False,
            reason="provider is not registered",
            actionable=f"register provider {provider_id!r} before selecting it",
            required_actor="ROOT-IM",
            required_action=f"register provider {provider_id!r} before selecting it",
        )
    supported = registration.capabilities.supports(operation)
    if supported:
        return ProviderOperationResult(
            provider_id=provider_id,
            operation=operation,
            supported=True,
            reason="declared capability",
            actionable="operation is supported by the selected adapter",
        )
    return ProviderOperationResult(
        provider_id=provider_id,
        operation=operation,
        supported=False,
        reason=f"adapter {registration.version!r} does not declare {operation}",
        actionable=(
            f"select a provider that declares {operation}, or extend the adapter "
            f"registration with an honest capability declaration"
        ),
        required_actor="ROOT-IM",
        required_action=(
            f"select a provider that declares {operation}, or extend the adapter "
            f"registration with an honest capability declaration"
        ),
    )


def unsupported_operation_result(
    provider_id: str, operation: str, reason: str
) -> ProviderOperationResult:
    """Build an explicit classified result for an unsupported operation."""
    if operation not in PROVIDER_OPERATION_NAMES:
        raise ProviderAdapterError(f"unknown provider operation: {operation}")
    return ProviderOperationResult(
        provider_id=provider_id,
        operation=operation,
        supported=False,
        reason=reason,
        actionable=(
            f"operation {operation!r} is not available for provider {provider_id!r}; "
            f"use a declared capability or a different registered provider"
        ),
        required_actor="ROOT-IM",
        required_action=(
            f"operation {operation!r} is not available for provider {provider_id!r}; "
            f"use a declared capability or a different registered provider"
        ),
    )


def provider_config_digest(spec: ProviderLaunchSpec) -> str:
    """Deterministic digest of the selected effective configuration."""
    record = {
        "action": spec.action,
        "command": list(spec.command),
        "model": spec.model,
        "reasoning_effort": spec.reasoning_effort,
        "service_tier": spec.service_tier,
        "session_id": spec.session_id,
        "run_root": str(spec.run_root),
        "last_message_path": str(spec.last_message_path),
        "config_overrides": list(spec.config_overrides),
        "sandbox": spec.sandbox,
        "approval_policy": spec.approval_policy,
        "worker_invocation_id": spec.worker_invocation_id,
        "permission_mode": spec.permission_mode,
        "allowed_tools": list(spec.allowed_tools),
        "disallowed_tools": list(spec.disallowed_tools),
        "mcp_config": spec.mcp_config,
        "provider_options": dict(spec.provider_options),
    }
    return hashlib.sha256(canonical_json(record).encode("utf-8")).hexdigest()


_CREDENTIAL_FLAG_MARKERS = (
    "--api-key",
    "--apikey",
    "--access-key",
    "--accesskey",
    "--private-key",
    "--password",
    "--secret",
    "--token",
    "--credential",
)


def redact_command(argv: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    """Redact credential-shaped tokens and their values from command provenance."""
    redacted: list[str] = []
    redact_next = False
    for token in argv:
        upper = token.upper()
        if redact_next:
            redacted.append("<redacted>")
            redact_next = False
            continue
        lowered = token.lower()
        if any(lowered.startswith(marker) for marker in _CREDENTIAL_FLAG_MARKERS):
            redacted.append("<redacted>")
            if lowered in {
                "--api-key", "--apikey", "--access-key", "--accesskey",
                "--private-key", "--password", "--secret", "--token", "--credential",
            }:
                # Exact flag form: the following value is also redacted.
                redact_next = True
            continue
        if any(marker in upper for marker in _REDACTION_MARKERS):
            redacted.append("<redacted>")
            continue
        redacted.append(token)
    if redact_next:
        redacted.append("<redacted>")
    return tuple(redacted)


def build_provider_evidence(
    provider_id: str,
    *,
    spec: ProviderLaunchSpec,
    argv: list[str] | tuple[str, ...],
    worker_invocation_id: str,
    session_id: str | None = None,
) -> ProviderEvidence:
    """Bind adapter identity/version, capabilities, digest, and redacted provenance."""
    registration = _REGISTRY.get(provider_id)
    if registration is None:
        raise ProviderAdapterError(f"provider {provider_id!r} is not registered")
    adapter = _ADAPTERS.get(provider_id)
    if adapter is None:
        raise ProviderAdapterError(f"provider {provider_id!r} has no adapter instance")
    # PA-R1-001: the selected adapter owns complete redacted command
    # provenance.  The generic core validates the closed shape (string vector
    # with the complete argv length) but never guesses provider credential
    # spellings.
    redacted = adapter.redact_argv(argv)
    if not isinstance(redacted, tuple) or not all(isinstance(item, str) for item in redacted):
        raise ProviderAdapterError(
            f"provider adapter {provider_id!r} redact_argv must return a tuple of strings"
        )
    if len(redacted) != len(argv):
        raise ProviderAdapterError(
            f"provider adapter {provider_id!r} redact_argv must preserve the complete argv shape"
        )
    return ProviderEvidence(
        provider_id=provider_id,
        adapter_version=registration.version,
        capabilities=registration.capabilities.as_record(),
        configuration_digest=provider_config_digest(spec),
        attempt_identity=worker_invocation_id,
        session_id=session_id,
        command_provenance=redacted,
    )


def structured_handoff(
    *,
    role: str,
    logical_task_id: str,
    worker_invocation_id: str,
    provider_id: str,
    prior_session_id: str | None,
    requested_session_id: str | None,
    reason: str,
) -> ProviderHandoff:
    """Build a declared same-role handoff that never fabricates continuity."""
    registration = _REGISTRY.get(provider_id)
    version = registration.version if registration is not None else "unregistered"
    return ProviderHandoff(
        role=role,
        logical_task_id=logical_task_id,
        worker_invocation_id=worker_invocation_id,
        provider_id=provider_id,
        adapter_version=version,
        prior_session_id=prior_session_id,
        requested_session_id=requested_session_id,
        reason=reason,
    )


NOTIFICATION_MODE_WAKE = "WAKE"
NOTIFICATION_MODE_SAFE_BOUNDARY_ONLY = "SAFE_BOUNDARY_ONLY"


def notification_mode(provider_id: str) -> dict[str, Any]:
    """Executable notification operation for one registered adapter.

    Returns a typed classified record.  ``WAKE`` carries the exact
    content-free, non-preemptive wake text owned by the adapter;
    ``SAFE_BOUNDARY_ONLY`` declares honestly that immediate wake is
    unavailable and only safe-boundary drain exists.  No payload is ever
    included in either mode.  ``WAKE`` is admissible only for adapters whose
    registration supplied one real ``deliver_notification`` safe-boundary
    delivery binding (enforced at registration); classification alone is
    never delivery.
    """
    registration = _REGISTRY.get(provider_id)
    if registration is None:
        return {
            "schema": PROVIDER_ADAPTER_SCHEMA,
            "provider_id": provider_id,
            "mode": NOTIFICATION_MODE_SAFE_BOUNDARY_ONLY,
            "wake_text": None,
            "reason": "provider is not registered",
            "actionable": f"register provider {provider_id!r} before selecting it",
        }
    if not registration.capabilities.notification:
        return {
            "schema": PROVIDER_ADAPTER_SCHEMA,
            "provider_id": provider_id,
            "mode": NOTIFICATION_MODE_SAFE_BOUNDARY_ONLY,
            "wake_text": None,
            "reason": "notification is not a declared capability of this adapter",
            "actionable": "drain only at an existing safe boundary",
        }
    adapter = _ADAPTERS.get(provider_id)
    wake_text = adapter.notification_wake_text() if adapter is not None else None
    if wake_text != NOTIFICATION_WAKE_TEXT:
        # REQ-O38: recheck byte equality at every selection.  A live adapter
        # that returns anything other than the exact wake text (including
        # after post-registration mutation) fails closed to SAFE_BOUNDARY_ONLY
        # with no wake text.
        return {
            "schema": PROVIDER_ADAPTER_SCHEMA,
            "provider_id": provider_id,
            "mode": NOTIFICATION_MODE_SAFE_BOUNDARY_ONLY,
            "wake_text": None,
            "reason": "adapter wake text is not the exact content-free wake",
            "actionable": "return the exact NOTIFICATION_WAKE_TEXT or use SAFE_BOUNDARY_ONLY",
        }
    return {
        "schema": PROVIDER_ADAPTER_SCHEMA,
        "provider_id": provider_id,
        "mode": NOTIFICATION_MODE_WAKE,
        "wake_text": wake_text,
        "reason": "declared notification capability with exact content-free wake",
        "actionable": "emit the exact wake text at a supported safe boundary",
    }


def decide_resume_or_handoff(
    provider: "ProviderAdapter | str",
    *,
    role: str,
    logical_task_id: str,
    worker_invocation_id: str,
    requested_session_id: str | None = None,
    persisted_session_id: str | None = None,
    identity_mismatch: bool = False,
) -> ProviderResumeDecision:
    """REQ-O35: resume when supported and identity matches, else declared handoff.

    ``provider`` may be a registered adapter instance or its ``provider_id``;
    the string form keeps real controller admission free of adapter
    instantiation before deep identity checks.
    """
    provider_id = provider if isinstance(provider, str) else provider.provider_id
    registration = _REGISTRY.get(provider_id)
    if registration is None:
        return ProviderResumeDecision(
            "HANDOFF",
            "adapter is not registered",
            structured_handoff(
                role=role,
                logical_task_id=logical_task_id,
                worker_invocation_id=worker_invocation_id,
                provider_id=provider_id,
                prior_session_id=persisted_session_id,
                requested_session_id=requested_session_id,
                reason="adapter is not registered",
            ),
        )
    if not registration.capabilities.resume:
        return ProviderResumeDecision(
            "HANDOFF",
            "resume is not a declared capability of this adapter",
            structured_handoff(
                role=role,
                logical_task_id=logical_task_id,
                worker_invocation_id=worker_invocation_id,
                provider_id=provider_id,
                prior_session_id=persisted_session_id,
                requested_session_id=requested_session_id,
                reason="resume is not a declared capability of this adapter",
            ),
        )
    if identity_mismatch:
        return ProviderResumeDecision(
            "HANDOFF",
            "resume identity mismatch",
            structured_handoff(
                role=role,
                logical_task_id=logical_task_id,
                worker_invocation_id=worker_invocation_id,
                provider_id=provider_id,
                prior_session_id=persisted_session_id,
                requested_session_id=requested_session_id,
                reason="resume identity mismatch",
            ),
        )
    if (
        requested_session_id is not None
        and persisted_session_id is not None
        and requested_session_id != persisted_session_id
    ):
        return ProviderResumeDecision(
            "HANDOFF",
            "requested session does not match the persisted session",
            structured_handoff(
                role=role,
                logical_task_id=logical_task_id,
                worker_invocation_id=worker_invocation_id,
                provider_id=provider_id,
                prior_session_id=persisted_session_id,
                requested_session_id=requested_session_id,
                reason="requested session does not match the persisted session",
            ),
        )
    return ProviderResumeDecision("RESUME", "adapter supports resume and identity matches")


CodexAdapter = CodexProviderAdapter
ClaudeCodeAdapter = ClaudeCodeProviderAdapter
get_provider_adapter = provider_adapter


__all__ = [
    "BaseProviderAdapter",
    "ClaudeCodeAdapter",
    "ClaudeCodeProviderAdapter",
    "CodexAdapter",
    "CodexProviderAdapter",
    "PROVIDER_ADAPTER_SCHEMA",
    "PROVIDER_HANDOFF_SCHEMA",
    "PROVIDER_OPERATION_NAMES",
    "ProviderAdapter",
    "ProviderAdapterError",
    "ProviderAdapterRegistration",
    "ProviderCapabilities",
    "ProviderEvent",
    "ProviderEvidence",
    "ProviderHandoff",
    "ProviderLaunchSpec",
    "ProviderOperationResult",
    "ProviderResumeDecision",
    "build_provider_evidence",
    "classify_operation",
    "decide_resume_or_handoff",
    "NOTIFICATION_MODE_SAFE_BOUNDARY_ONLY",
    "NOTIFICATION_MODE_WAKE",
    "NOTIFICATION_WAKE_TEXT",
    "get_provider_adapter",
    "provider_adapter",
    "provider_config_digest",
    "provider_registry",
    "notification_mode",
    "redact_command",
    "WAKE_TEXT",
    "register_provider_adapter",
    "structured_handoff",
    "unregister_provider_adapter",
    "unsupported_operation_result",
]
