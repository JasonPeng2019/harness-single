"""Inert firmware campaign data for the capability adapter.

This module validates fixed campaign intent and policy references only.  It
does not allocate a resource, create a transport, launch a process, or
dispatch an action.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from orchestrator_harness.capability_broker import CapabilityAdapterUnavailable, CapabilityRequest


CAMPAIGN_PACK_SCHEMA = "firmware-capability-campaign-pack/v1"
CAMPAIGN_CAPABILITY = "firmware-acceptance"
FIXTURE_ALIASES = ("STM-A", "STM-B", "NRF-A", "NRF-B")


@dataclass(frozen=True)
class FirmwareOperation:
    """Hardware-specific mapping kept out of the generic broker."""

    capability: str
    action: str
    fixture_alias: str
    method: str
    method_version: int
    maximum_duration_seconds: int
    arguments: dict[str, Any]
    policy_reference: dict[str, str]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"campaign source is not an object: {path.name}")
    return value


class FirmwareCampaignPack:
    """Fixed, inert fixture/action/policy data for later authorized use."""

    _ACTION_METHODS = {
        "observe": ("get_board_info", ("board_id",)),
        "read_state": ("get_state", ("board_id",)),
        "reset": ("reset_and_run", ("board_id",)),
        "flash_application": ("flash_application", ("board_id", "artifact")),
        "read_serial": ("read_serial", ("board_id", "expected_text", "read_seconds", "baudrate", "port", "reset_on_open", "on_exit")),
        "write_serial": ("write_serial", ("board_id", "text", "baudrate", "port", "append_newline", "timeout_seconds", "on_exit")),
        "serial_exchange": ("serial_exchange", ("board_id", "steps", "read_seconds", "baudrate", "port", "ready_text", "ready_seconds", "ready_probe_text", "ready_probe_line_ending", "ready_probe_delay_seconds", "clear_input")),
        "disconnect": ("disconnect", ("board_id",)),
    }

    def __init__(self, *, manifest_path: Path | None = None, policy_path: Path | None = None, templates_path: Path | None = None) -> None:
        self.manifest_path = (manifest_path or Path(__file__).with_name("ACCEPTANCE_MANIFEST.json")).resolve()
        self.policy_path = (policy_path or Path(__file__).with_name("MCP_METHOD_POLICY.json")).resolve()
        self.templates_path = (templates_path or Path(__file__).with_name("LANE_TEMPLATES.json")).resolve()
        self.manifest = _load(self.manifest_path)
        self.policy = _load(self.policy_path)
        self.templates = _load(self.templates_path)
        fixtures = self.manifest.get("fixtures")
        if not isinstance(fixtures, dict) or set(fixtures) != set(FIXTURE_ALIASES):
            raise ValueError("campaign pack must retain exactly the four accepted fixture aliases")
        methods = self.policy.get("methods")
        if not isinstance(methods, dict):
            raise ValueError("campaign method policy is malformed")
        lanes = self.templates.get("lanes")
        if not isinstance(lanes, list) or {item.get("lane_id") for item in lanes if isinstance(item, dict)} != set(FIXTURE_ALIASES):
            raise ValueError("campaign lane aliases are not the fixed four")
        self.fixture_aliases = FIXTURE_ALIASES
        self.method_policy_reference = {"path": self.policy_path.name, "sha256": _sha256(self.policy_path)}
        self.manifest_reference = {"path": self.manifest_path.name, "sha256": _sha256(self.manifest_path)}
        self.authorized_maxima = {
            action: int(methods[method]["maximum_duration_seconds"])
            for action, (method, _) in self._ACTION_METHODS.items()
            if isinstance(methods.get(method), dict) and isinstance(methods[method].get("maximum_duration_seconds"), int)
        }
        self.hil_intent = {
            "mode": "inert",
            "physical_execution": False,
            "requires_external_authority": True,
            "fixture_count": len(FIXTURE_ALIASES),
        }
        self.scenarios = {
            "observation": ("observe", "read_state"),
            "application": ("flash_application", "reset"),
            "serial": ("read_serial", "write_serial", "serial_exchange"),
            "return": ("disconnect",),
        }

    def as_record(self) -> dict[str, Any]:
        return {
            "schema": CAMPAIGN_PACK_SCHEMA,
            "capability": CAMPAIGN_CAPABILITY,
            "fixture_aliases": list(self.fixture_aliases),
            "scenarios": {key: list(value) for key, value in self.scenarios.items()},
            "authorized_maxima": dict(self.authorized_maxima),
            "method_policy_reference": dict(self.method_policy_reference),
            "manifest_reference": dict(self.manifest_reference),
            "hil_intent": dict(self.hil_intent),
        }

    def supports(self, *, capability: str, action: str, fixture_alias: str) -> bool:
        return capability == CAMPAIGN_CAPABILITY and fixture_alias in self.fixture_aliases and action in self._ACTION_METHODS and action in self.authorized_maxima

    def resolve(self, request: CapabilityRequest) -> FirmwareOperation:
        if not self.supports(capability=request.capability, action=request.action, fixture_alias=request.lane_id):
            raise CapabilityAdapterUnavailable("firmware campaign action is not available for this fixture")
        method, required = self._ACTION_METHODS[request.action]
        source = dict(request.arguments)
        if request.action in {"observe", "read_state", "reset", "disconnect"}:
            if source and set(source) != {"fixture"}:
                raise CapabilityAdapterUnavailable("campaign action arguments are not closed")
            if source.get("fixture", request.lane_id) != request.lane_id:
                raise CapabilityAdapterUnavailable("campaign fixture argument is not bound to the lane")
            arguments = {"board_id": request.lane_id}
        else:
            fixture = source.pop("fixture", request.lane_id)
            if fixture != request.lane_id:
                raise CapabilityAdapterUnavailable("campaign fixture argument is not bound to the lane")
            if set(source) != set(required) - {"board_id"} or any(key not in source for key in required if key != "board_id"):
                raise CapabilityAdapterUnavailable("campaign action arguments are not the accepted closed shape")
            arguments = {"board_id": request.lane_id, **source}
        policy = self.policy.get("methods", {}).get(method)
        if not isinstance(policy, dict) or policy.get("version") != 1:
            raise CapabilityAdapterUnavailable("campaign method policy revision is unavailable")
        return FirmwareOperation(
            capability=request.capability,
            action=request.action,
            fixture_alias=request.lane_id,
            method=method,
            method_version=1,
            maximum_duration_seconds=self.authorized_maxima[request.action],
            arguments=arguments,
            policy_reference=dict(self.method_policy_reference),
        )

    def approval_policy(self, request: CapabilityRequest, operation: FirmwareOperation, policy: Mapping[str, Any]) -> bool:
        expected = {
            "policy_reference": self.method_policy_reference,
            "method_version": operation.method_version,
            "maximum_duration_seconds": operation.maximum_duration_seconds,
            "action": operation.action,
        }
        return dict(policy) == expected and request.capability == CAMPAIGN_CAPABILITY and request.lane_id == operation.fixture_alias


DEFAULT_CAMPAIGN_PACK = FirmwareCampaignPack()


__all__ = [
    "CAMPAIGN_CAPABILITY",
    "CAMPAIGN_PACK_SCHEMA",
    "DEFAULT_CAMPAIGN_PACK",
    "FIXTURE_ALIASES",
    "FirmwareCampaignPack",
    "FirmwareOperation",
]
