"""Caller-declared firmware campaign pack and operation declarations.

The pack owns only declarative campaign data: a caller-declared capability
name, action declarations, canonical resource identities, and a nonempty
public policy binding.  It never allocates a resource, creates a transport,
launches a process, or dispatches an action.  The harness provides no default
pack, fixture, action, manifest, policy, path, hash, server revision,
provider, or machine value.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .capability_broker import (
    CapabilityAdapterUnavailable,
    CapabilityApproval,
    CapabilityRequest,
)

CAMPAIGN_PACK_SCHEMA = "firmware-capability-campaign-pack/v1"


@dataclass(frozen=True)
class FirmwareAction:
    """One caller-declared firmware action mapping."""

    mcp_tool: str
    method_version: int
    maximum_duration_seconds: int
    required_arguments: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.mcp_tool, str) or not self.mcp_tool:
            raise ValueError("firmware action MCP tool must be a non-empty string")
        if isinstance(self.method_version, bool) or not isinstance(self.method_version, int) or self.method_version <= 0:
            raise ValueError("firmware action method version must be a positive integer")
        if isinstance(self.maximum_duration_seconds, bool) or not isinstance(self.maximum_duration_seconds, int) or self.maximum_duration_seconds <= 0:
            raise ValueError("firmware action maximum duration must be a positive integer")
        if not isinstance(self.required_arguments, tuple) or not self.required_arguments or any(not isinstance(item, str) or not item for item in self.required_arguments):
            raise ValueError("firmware action required arguments must be a non-empty tuple of non-empty names")
        if len(set(self.required_arguments)) != len(self.required_arguments):
            raise ValueError("firmware action required arguments must be unique")


@dataclass(frozen=True)
class FirmwareOperation:
    """One resolved firmware operation bound to a caller-declared pack."""

    capability: str
    action: str
    canonical_resource: str
    resource_identity: dict[str, Any]
    mcp_tool: str
    method_version: int
    maximum_duration_seconds: int
    arguments: dict[str, Any]
    policy: dict[str, Any]


class FirmwareCampaignPack:
    """Caller-declared campaign data; authoritative only when resolved."""

    def __init__(
        self,
        *,
        capability: str,
        actions: Mapping[str, FirmwareAction],
        resources: Mapping[str, Mapping[str, Any]],
        policy: Mapping[str, Any],
    ) -> None:
        if not isinstance(capability, str) or not capability:
            raise ValueError("campaign capability must be a non-empty string")
        if not isinstance(actions, Mapping) or not actions or any(not isinstance(name, str) or not name or not isinstance(action, FirmwareAction) for name, action in actions.items()):
            raise ValueError("campaign actions must be a non-empty mapping of action names to FirmwareAction declarations")
        if len(set(actions)) != len(actions):
            raise ValueError("campaign action names must be unique")
        if not isinstance(resources, Mapping) or not resources or any(not isinstance(name, str) or not name or not isinstance(identity, Mapping) or not identity for name, identity in resources.items()):
            raise ValueError("campaign resources must be a non-empty mapping of canonical resource names to non-empty identities")
        if len(set(resources)) != len(resources):
            raise ValueError("campaign resource names must be unique")
        if not isinstance(policy, Mapping) or not policy:
            raise ValueError("campaign policy binding must be a non-empty object")
        self.capability = capability
        self.actions = dict(actions)
        self.resources = {name: dict(identity) for name, identity in resources.items()}
        self.policy = dict(policy)

    def resolve(self, request: CapabilityRequest) -> FirmwareOperation:
        """Resolve one operation from the declared pack and a closed request.

        The requested capability and action must be declared, the request must
        name exactly one declared canonical resource, and the request
        arguments must be exactly the action's declared argument keys.  Caller
        arguments pass through unchanged.  A lane ID is never treated as a
        board/resource ID.
        """
        if request.capability != self.capability:
            raise CapabilityAdapterUnavailable("firmware campaign capability is not declared")
        action = self.actions.get(request.action)
        if action is None:
            raise CapabilityAdapterUnavailable("firmware campaign action is not declared")
        if len(request.resources) != 1:
            raise CapabilityAdapterUnavailable("firmware campaign requires exactly one canonical resource")
        canonical_resource = request.resources[0]
        resource_identity = self.resources.get(canonical_resource)
        if resource_identity is None:
            raise CapabilityAdapterUnavailable("firmware campaign resource is not declared")
        if set(request.arguments) != set(action.required_arguments):
            raise CapabilityAdapterUnavailable("firmware campaign arguments are not the declared closed keys")
        return FirmwareOperation(
            capability=self.capability,
            action=request.action,
            canonical_resource=canonical_resource,
            resource_identity=dict(resource_identity),
            mcp_tool=action.mcp_tool,
            method_version=action.method_version,
            maximum_duration_seconds=action.maximum_duration_seconds,
            arguments=dict(request.arguments),
            policy=dict(self.policy),
        )

    def supports(self, request: CapabilityRequest) -> bool:
        try:
            self.resolve(request)
            return True
        except CapabilityAdapterUnavailable:
            return False

    def approval_policy(self, request: CapabilityRequest, operation: FirmwareOperation, approval: CapabilityApproval) -> bool:
        """Bind the approval policy to the declared pack facts.

        The binding covers the declared policy, capability, action, MCP tool,
        method version, maximum duration, canonical resource, and resource
        identity.  A caller may deliberately include a revision/hash inside
        its declared policy; the harness never generates one.
        """
        expected = {
            "policy": self.policy,
            "capability": self.capability,
            "action": operation.action,
            "mcp_tool": operation.mcp_tool,
            "method_version": operation.method_version,
            "maximum_duration_seconds": operation.maximum_duration_seconds,
            "canonical_resource": operation.canonical_resource,
            "resource_identity": operation.resource_identity,
        }
        if dict(approval.policy) != expected:
            return False
        if request.capability != self.capability or request.resources != (operation.canonical_resource,):
            return False
        return True

    def effective_duration(self, request: CapabilityRequest, operation: FirmwareOperation, *, now_monotonic: float) -> float:
        return now_monotonic + operation.maximum_duration_seconds

    def as_record(self) -> dict[str, Any]:
        return {
            "schema": CAMPAIGN_PACK_SCHEMA,
            "capability": self.capability,
            "actions": {
                name: {
                    "mcp_tool": action.mcp_tool,
                    "method_version": action.method_version,
                    "maximum_duration_seconds": action.maximum_duration_seconds,
                    "required_arguments": list(action.required_arguments),
                }
                for name, action in self.actions.items()
            },
            "resources": {name: dict(identity) for name, identity in self.resources.items()},
            "policy": dict(self.policy),
        }


__all__ = [
    "CAMPAIGN_PACK_SCHEMA",
    "FirmwareAction",
    "FirmwareCampaignPack",
    "FirmwareOperation",
]
