"""Durable observation and lane-control primitives for parallel coding work."""

from .models import ProcessInfo, ProcessSnapshot
from .invocation import CANONICAL_INVOCATION_SCHEMA, CanonicalInvocation
from .prompt_bundle import PromptBundle, PromptComponent
from .provider import ClaudeCodeProviderAdapter, CodexProviderAdapter
from .profile import RuntimeProfile
from .resume import ResumeAdmission
from .host_adapters import (
    AdapterCapabilities,
    DeliveryCoordinator,
    DeliveryNotice,
    DeliveryReceipt,
    FutureHostFixture,
    HostAdapter,
    HostProfile,
    ManagerEventAck,
)
from .codex_adapter import CodexAdapter, SyntheticCodexTransport
from .lane_lifecycle import ImmutableSourceView, RetirementResult

__all__ = [
    "CANONICAL_INVOCATION_SCHEMA",
    "CanonicalInvocation",
    "ClaudeCodeProviderAdapter",
    "CodexProviderAdapter",
    "ProcessInfo",
    "ProcessSnapshot",
    "PromptBundle",
    "PromptComponent",
    "ResumeAdmission",
    "RuntimeProfile",
    "AdapterCapabilities",
    "CodexAdapter",
    "DeliveryCoordinator",
    "DeliveryNotice",
    "DeliveryReceipt",
    "FutureHostFixture",
    "HostAdapter",
    "HostProfile",
    "ImmutableSourceView",
    "ManagerEventAck",
    "RetirementResult",
    "SyntheticCodexTransport",
]
__version__ = "0.1.0"
