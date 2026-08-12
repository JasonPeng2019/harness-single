"""Inert, controller-pinned firmware campaign data.

The pack owns fixture identity, fixed action selection, and the existing
method-policy predicates.  It never allocates a resource, creates a transport,
launches a process, or dispatches an action.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from orchestrator_harness.capability_broker import CapabilityAdapterUnavailable, CapabilityApproval, CapabilityRequest


CAMPAIGN_PACK_SCHEMA = "firmware-capability-campaign-pack/v1"
CAMPAIGN_CAPABILITY = "firmware-acceptance"
FIXTURE_ALIASES = ("STM-A", "STM-B", "NRF-A", "NRF-B")
_MANIFEST_SHA256 = "ffde1fd5aec3f515f1f5c0ec1b7f657b196fed7c008a4650fc6c2d0bc3a81800"
_POLICY_SHA256 = "35bc68b6ec79a8e9680c42dd32c32be228ee956ee7b53d68908b8dec13d19b78"
_TEMPLATES_SHA256 = "af7cebbc5e3910c32f0ef183fcad401234ea8cd6afe466fdb6990e75016d83f4"


@dataclass(frozen=True)
class FirmwareOperation:
    """One controller-authorized firmware action mapping."""

    capability: str
    action: str
    fixture_alias: str
    canonical_resource: str
    fixture_identity: dict[str, str]
    method: str
    method_version: int
    maximum_duration_seconds: int
    arguments: dict[str, Any]
    policy_reference: dict[str, str]
    manifest_reference: dict[str, str]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"campaign source is not an object: {path.name}")
    return value


class FirmwareCampaignPack:
    """Fixed campaign data that becomes authoritative only when controller-bound."""

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

    def __init__(
        self,
        *,
        manifest_path: Path | None = None,
        policy_path: Path | None = None,
        templates_path: Path | None = None,
    ) -> None:
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
        self.templates_reference = {"path": self.templates_path.name, "sha256": _sha256(self.templates_path)}
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
        self._controller_authority: dict[str, Any] | None = None

    @property
    def controller_bound(self) -> bool:
        return self._controller_authority is not None

    def with_controller_authority(self, authority: Mapping[str, Any]) -> "FirmwareCampaignPack":
        """Return a pack bound to the already-validated controller inputs."""

        expected_keys = {"manifest", "policy", "templates", "manifest_path", "policy_path", "templates_path"}
        if set(authority) != expected_keys:
            raise CapabilityAdapterUnavailable("controller campaign authority is not closed")
        manifest_path = Path(authority["manifest_path"]).resolve()
        policy_path = Path(authority["policy_path"]).resolve()
        templates_path = Path(authority["templates_path"]).resolve()
        if (manifest_path != self.manifest_path or policy_path != self.policy_path or templates_path != self.templates_path):
            raise CapabilityAdapterUnavailable("campaign source paths are not the controller-pinned sources")
        if (_sha256(manifest_path) != _MANIFEST_SHA256 or _sha256(policy_path) != _POLICY_SHA256 or _sha256(templates_path) != _TEMPLATES_SHA256):
            raise CapabilityAdapterUnavailable("campaign source identity is not the pinned immutable source")
        if authority["manifest"] != self.manifest or authority["policy"] != self.policy or authority["templates"] != self.templates:
            raise CapabilityAdapterUnavailable("controller campaign inputs differ from the pinned source")
        bound = FirmwareCampaignPack(
            manifest_path=self.manifest_path,
            policy_path=self.policy_path,
            templates_path=self.templates_path,
        )
        bound._controller_authority = {
            "manifest": json.loads(json.dumps(authority["manifest"], sort_keys=True)),
            "policy": json.loads(json.dumps(authority["policy"], sort_keys=True)),
            "templates": json.loads(json.dumps(authority["templates"], sort_keys=True)),
            "manifest_path": manifest_path,
            "policy_path": policy_path,
            "templates_path": templates_path,
        }
        return bound

    def as_record(self) -> dict[str, Any]:
        return {
            "schema": CAMPAIGN_PACK_SCHEMA,
            "capability": CAMPAIGN_CAPABILITY,
            "fixture_aliases": list(self.fixture_aliases),
            "scenarios": {key: list(value) for key, value in self.scenarios.items()},
            "authorized_maxima": dict(self.authorized_maxima),
            "method_policy_reference": dict(self.method_policy_reference),
            "manifest_reference": dict(self.manifest_reference),
            "templates_reference": dict(self.templates_reference),
            "controller_bound": self.controller_bound,
            "hil_intent": dict(self.hil_intent),
        }

    @staticmethod
    def canonical_resource(fixture_alias: str) -> str:
        return fixture_alias

    def supports(self, *, capability: str, action: str, fixture_alias: str) -> bool:
        return capability == CAMPAIGN_CAPABILITY and fixture_alias in self.fixture_aliases and action in self._ACTION_METHODS and action in self.authorized_maxima

    def resolve(self, request: CapabilityRequest) -> FirmwareOperation:
        if not self.controller_bound:
            raise CapabilityAdapterUnavailable("controller campaign authority is unavailable")
        if not self.supports(capability=request.capability, action=request.action, fixture_alias=request.lane_id):
            raise CapabilityAdapterUnavailable("firmware campaign action is not available for this fixture")
        canonical_resource = self.canonical_resource(request.lane_id)
        if request.resources != (canonical_resource,):
            raise CapabilityAdapterUnavailable("firmware fixture resource is not the one canonical lane resource")
        fixtures = self._controller_authority["manifest"]["fixtures"]
        fixture_identity = fixtures.get(request.lane_id)
        if not isinstance(fixture_identity, dict) or set(fixture_identity) != {"probe_uid", "target", "profile", "serial_route"}:
            raise CapabilityAdapterUnavailable("firmware fixture identity is not closed")
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
        policy = self._controller_authority["policy"].get("methods", {}).get(method)
        if not isinstance(policy, dict) or policy.get("version") != 1:
            raise CapabilityAdapterUnavailable("campaign method policy revision is unavailable")
        operation = FirmwareOperation(
            capability=request.capability,
            action=request.action,
            fixture_alias=request.lane_id,
            canonical_resource=canonical_resource,
            fixture_identity=json.loads(json.dumps(fixture_identity, sort_keys=True)),
            method=method,
            method_version=1,
            maximum_duration_seconds=self.authorized_maxima[request.action],
            arguments=arguments,
            policy_reference=dict(self.method_policy_reference),
            manifest_reference=dict(self.manifest_reference),
        )
        return operation

    def validate_operation(self, request: CapabilityRequest, operation: FirmwareOperation, *, now_monotonic: float) -> dict[str, Any]:
        """Reuse the established method-policy predicates for generic dispatch."""

        from .kit import _USER_ISSUED_SCOPE, canonical_sha256, evaluate_call

        rule = self._controller_authority["policy"]["methods"].get(operation.method)
        if not isinstance(rule, dict):
            raise CapabilityAdapterUnavailable("campaign method is default-denied")
        null_effect = {"schema": "firmware-call-effect/v1", "effect_action_class": None, "target_operation_manifest": None, "electronic_admission": None, "limits": None}
        call = {
            "call_id": request.request_id,
            "attempt_id": request.request_id,
            "lane_id": request.lane_id,
            "board": request.lane_id,
            "probe_uid": operation.fixture_identity["probe_uid"],
            "target": operation.fixture_identity["target"],
            "profile": operation.fixture_identity["profile"],
            "method": operation.method,
            "method_version": operation.method_version,
            "arguments": dict(operation.arguments),
            "proposal_sha256": "0" * 64,
            "decision_sha256": "0" * 64,
            "authorization_sha256": "0" * 64,
            "deadline_monotonic": request.expires_monotonic,
            "plan": {"max_operation_duration_seconds": operation.maximum_duration_seconds},
            "permission": {"granted": True},
            "delegated_user_scope_sha256": canonical_sha256(_USER_ISSUED_SCOPE),
            "action_class": rule["action_class"],
            "scope_effect": null_effect,
        }
        try:
            return evaluate_call(call, now_monotonic=now_monotonic, policy_path=self.policy_path)
        except Exception as exc:
            raise CapabilityAdapterUnavailable("firmware method policy rejected the operation") from exc

    def approval_policy(self, request: CapabilityRequest, operation: FirmwareOperation, approval: CapabilityApproval) -> bool:
        if not self.controller_bound:
            return False
        policy = approval.policy
        expected = {
            "policy_reference": self.method_policy_reference,
            "manifest_reference": self.manifest_reference,
            "method_version": operation.method_version,
            "maximum_duration_seconds": operation.maximum_duration_seconds,
            "action": operation.action,
            "canonical_resource": operation.canonical_resource,
            "fixture_identity": operation.fixture_identity,
        }
        if dict(policy) != expected or request.capability != CAMPAIGN_CAPABILITY or request.lane_id != operation.fixture_alias or request.resources != (operation.canonical_resource,):
            return False
        try:
            self.validate_operation(request, operation, now_monotonic=approval.issued_monotonic)
        except CapabilityAdapterUnavailable:
            return False
        return True

    def effective_duration(self, request: CapabilityRequest, operation: FirmwareOperation, *, now_monotonic: float) -> float:
        self.validate_operation(request, operation, now_monotonic=now_monotonic)
        return now_monotonic + operation.maximum_duration_seconds


DEFAULT_CAMPAIGN_PACK = FirmwareCampaignPack()


__all__ = [
    "CAMPAIGN_CAPABILITY",
    "CAMPAIGN_PACK_SCHEMA",
    "DEFAULT_CAMPAIGN_PACK",
    "FIXTURE_ALIASES",
    "FirmwareCampaignPack",
    "FirmwareOperation",
]
