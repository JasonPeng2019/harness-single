"""Small, default-deny capability request-to-cleanup broker.

The broker owns only the invariants shared by capability operations.  An
adapter owns observation, dispatch, raw-result interpretation, and cleanup.
The broker never receives or serializes an adapter endpoint.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Mapping, Protocol, Sequence, cast

from harness_common.process_identity import exact_process_identity

from .models import iso_utc
from .mutation import MutationConflict, MutationUnsupported, capture_target, replace as mutation_replace
from .processes import process_snapshot
from .resource_locks import BOUNDARY_ARMED_STATE, ResourceClaims


REQUEST_SCHEMA = "orchestrator-capability-request/v1"
SNAPSHOT_SCHEMA = "orchestrator-capability-snapshot/v1"
APPROVAL_SCHEMA = "orchestrator-capability-approval/v1"
PERMIT_SCHEMA = "orchestrator-capability-permit/v1"
RESULT_SCHEMA = "orchestrator-capability-result/v1"
CLEANUP_SCHEMA = "orchestrator-capability-cleanup/v1"
STATE_SCHEMA = "orchestrator-capability-state/v1"

_IDENTITY_KEYS = {"pid", "created_utc", "creation_identity"}
_ADAPTER_IDENTITY_KEYS = {"adapter_id", "adapter_version"}
_AUTHORITY_KEY_PARTS = {
    "endpoint",
    "mcp",
    "server",
    "connection",
    "transport",
    "session",
    "handle",
    "config",
    "token",
    "apikey",
    "authorization",
    "bearer",
    "credential",
    "secret",
    "password",
    "privatekey",
    "stdio",
    "provider",
    "command",
    "physical",
    "socket",
    "pipe",
    "channel",
}
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class CapabilityError(ValueError):
    """Base error for malformed capability boundary facts."""


class CapabilityDenied(CapabilityError):
    """A typed, non-authorizing denial that can be published as evidence."""

    def __init__(self, reason_code: str, message: str, *, stage: str, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.stage = stage
        self.details = _public_json(dict(details or {}), label="denial details")

    def to_record(self) -> dict[str, Any]:
        return {
            "reason_code": self.reason_code,
            "stage": self.stage,
            "message": str(self),
            "details": dict(self.details),
        }


class CapabilityAdapterError(CapabilityError):
    """Adapter evidence was unavailable or could not be interpreted."""


class CapabilityAdapterUnavailable(CapabilityAdapterError):
    """The adapter cannot observe or perform the requested operation."""


def _public_json(value: Any, *, label: str) -> Any:
    """Copy JSON values while rejecting private capability material."""

    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CapabilityError(f"{label} contains a non-finite number")
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise CapabilityError(f"{label} has a non-string key")
            normalized_key = re.sub(r"[^a-z0-9]", "", key.casefold())
            if any(part in normalized_key for part in _AUTHORITY_KEY_PARTS):
                raise CapabilityError(f"{label} contains private capability material")
            result[key] = _public_json(item, label=label)
        return {key: result[key] for key in sorted(result)}
    if isinstance(value, (list, tuple)):
        return [_public_json(item, label=label) for item in value]
    raise CapabilityError(f"{label} is not canonical JSON")


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw(item) for item in value]
    return value


def canonical_json_bytes(value: Any) -> bytes:
    """Return the one compact, deterministic representation used by this seam."""

    try:
        return json.dumps(
            _public_json(value, label="canonical value"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise CapabilityError("value is not canonical JSON") from exc


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise CapabilityDenied("INVALID_IDENTITY", f"{label} is not a bounded identifier", stage="request")
    return value


def _finite_time(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise CapabilityDenied("INVALID_EXPIRY", f"{label} is not finite", stage="request")
    return float(value)


def _process_identity(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _IDENTITY_KEYS:
        raise CapabilityDenied("INVALID_IDENTITY", f"{label} is not a closed process identity", stage="request")
    pid = value.get("pid")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        raise CapabilityDenied("INVALID_IDENTITY", f"{label} has an invalid PID", stage="request")
    if not all(isinstance(value.get(key), str) and bool(value[key]) for key in ("created_utc", "creation_identity")):
        raise CapabilityDenied("INVALID_IDENTITY", f"{label} has an incomplete creation identity", stage="request")
    return {"pid": pid, "created_utc": value["created_utc"], "creation_identity": value["creation_identity"]}


def _adapter_identity(value: Any, label: str = "adapter identity") -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != _ADAPTER_IDENTITY_KEYS:
        raise CapabilityError(f"{label} is not a closed adapter identity")
    result = {key: value[key] for key in _ADAPTER_IDENTITY_KEYS}
    if not all(isinstance(item, str) and item for item in result.values()):
        raise CapabilityError(f"{label} is incomplete")
    return {"adapter_id": result["adapter_id"], "adapter_version": result["adapter_version"]}


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise CapabilityError(f"{label} is not an object")
    copied = _public_json(value, label=label)
    if not isinstance(copied, dict):
        raise CapabilityError(f"{label} is not an object")
    return copied


@dataclass(frozen=True)
class CapabilityRequest:
    """Closed, canonical operation request with no endpoint-bearing fields."""

    request_id: str
    lane_id: str
    controller_identity: dict[str, Any]
    capability: str
    action: str
    arguments: dict[str, Any]
    resources: tuple[str, ...]
    expires_monotonic: float
    route: str = "capability"

    def __post_init__(self) -> None:
        object.__setattr__(self, "controller_identity", _freeze(self.controller_identity))
        object.__setattr__(self, "arguments", _freeze(self.arguments))

    @classmethod
    def from_record(cls, value: Any, *, now_monotonic: float) -> "CapabilityRequest":
        if not isinstance(value, Mapping) or set(value) != {
            "schema", "request_id", "lane_id", "controller_identity", "capability", "action",
            "arguments", "resources", "expires_monotonic", "route",
        }:
            raise CapabilityDenied("REQUEST_NOT_CLOSED", "request schema is not closed", stage="request")
        if value.get("schema") != REQUEST_SCHEMA or value.get("route") != "capability":
            raise CapabilityDenied("MIXED_ROUTE", "record is not a capability-route request", stage="request")
        request_id = _identifier(value.get("request_id"), "request_id")
        lane_id = _identifier(value.get("lane_id"), "lane_id")
        capability = _identifier(value.get("capability"), "capability")
        action = _identifier(value.get("action"), "action")
        identity = _process_identity(value.get("controller_identity"), "controller_identity")
        arguments = _mapping(value.get("arguments"), "arguments")
        raw_resources = value.get("resources")
        if not isinstance(raw_resources, list) or not raw_resources:
            raise CapabilityDenied("INVALID_RESOURCES", "resources must be a non-empty list", stage="request")
        resources: list[str] = []
        for item in raw_resources:
            resource = _identifier(item, "resource")
            if resource in resources:
                raise CapabilityDenied("DUPLICATE_RESOURCE", "duplicate resource is not canonical", stage="request")
            resources.append(resource)
        expiry = _finite_time(value.get("expires_monotonic"), "request expiry")
        if expiry <= now_monotonic:
            raise CapabilityDenied("EXPIRED_REQUEST", "request is already expired", stage="request")
        return cls(
            request_id=request_id,
            lane_id=lane_id,
            controller_identity=identity,
            capability=capability,
            action=action,
            arguments=arguments,
            resources=tuple(sorted(resources)),
            expires_monotonic=expiry,
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "schema": REQUEST_SCHEMA,
            "request_id": self.request_id,
            "lane_id": self.lane_id,
            "controller_identity": _thaw(self.controller_identity),
            "capability": self.capability,
            "action": self.action,
            "arguments": _thaw(self.arguments),
            "resources": list(self.resources),
            "expires_monotonic": self.expires_monotonic,
            "route": self.route,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.to_record())


@dataclass(frozen=True)
class CapabilitySnapshot:
    """Verified current observation supplied by an adapter without a handle."""

    request_id: str
    lane_id: str
    controller_identity: dict[str, Any]
    capability: str
    action: str
    resources: tuple[str, ...]
    snapshot_id: str
    identity: dict[str, Any]
    resource_identities: dict[str, Any]
    capabilities: dict[str, tuple[str, ...]]
    adapter_identity: dict[str, str]
    observed_monotonic: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "controller_identity", _freeze(self.controller_identity))
        object.__setattr__(self, "identity", _freeze(self.identity))
        object.__setattr__(self, "resource_identities", _freeze(self.resource_identities))
        object.__setattr__(self, "capabilities", _freeze(self.capabilities))
        object.__setattr__(self, "adapter_identity", _freeze(self.adapter_identity))

    @classmethod
    def from_record(cls, value: Any) -> "CapabilitySnapshot":
        if not isinstance(value, Mapping) or set(value) != {
            "schema", "request_id", "lane_id", "controller_identity", "capability", "action",
            "resources", "snapshot_id", "identity", "resource_identities", "capabilities",
            "adapter_identity", "observed_monotonic",
        }:
            raise CapabilityError("snapshot is not closed")
        if value.get("schema") != SNAPSHOT_SCHEMA:
            raise CapabilityError("snapshot schema is invalid")
        request_id = _identifier(value.get("request_id"), "snapshot request_id")
        lane_id = _identifier(value.get("lane_id"), "snapshot lane_id")
        capability = _identifier(value.get("capability"), "snapshot capability")
        action = _identifier(value.get("action"), "snapshot action")
        snapshot_id = _identifier(value.get("snapshot_id"), "snapshot_id")
        identity = _mapping(value.get("identity"), "snapshot identity")
        if not identity:
            raise CapabilityError("snapshot identity is empty")
        controller_identity = _process_identity(value.get("controller_identity"), "snapshot controller_identity")
        raw_resources = value.get("resources")
        if not isinstance(raw_resources, list) or not raw_resources or any(not isinstance(item, str) or not item for item in raw_resources):
            raise CapabilityError("snapshot resources are invalid")
        resources = tuple(sorted(raw_resources))
        if len(resources) != len(set(resources)):
            raise CapabilityError("snapshot resources are duplicated")
        resource_identities = _mapping(value.get("resource_identities"), "resource identities")
        if set(resource_identities) != set(resources):
            raise CapabilityError("snapshot resource identities are not exact")
        capabilities_raw = value.get("capabilities")
        if not isinstance(capabilities_raw, Mapping) or not capabilities_raw:
            raise CapabilityError("snapshot capabilities are unavailable")
        capabilities: dict[str, tuple[str, ...]] = {}
        for cap, actions in capabilities_raw.items():
            if not isinstance(cap, str) or not cap or not isinstance(actions, list) or not actions:
                raise CapabilityError("snapshot capability advertisement is malformed")
            if any(not isinstance(item, str) or not item or not _IDENTIFIER.fullmatch(item) for item in actions):
                raise CapabilityError("snapshot capability actions are malformed")
            normalized = tuple(sorted(actions))
            if len(normalized) != len(set(normalized)):
                raise CapabilityError("snapshot capability actions are malformed or duplicated")
            if not _IDENTIFIER.fullmatch(cap):
                raise CapabilityError("snapshot capability name is malformed")
            capabilities[cap] = normalized
        adapter_identity = _adapter_identity(value.get("adapter_identity"))
        observed = value.get("observed_monotonic")
        if isinstance(observed, bool) or not isinstance(observed, (int, float)) or not math.isfinite(float(observed)):
            raise CapabilityError("snapshot observation time is invalid")
        return cls(
            request_id=request_id,
            lane_id=lane_id,
            controller_identity=controller_identity,
            capability=capability,
            action=action,
            resources=resources,
            snapshot_id=snapshot_id,
            identity=identity,
            resource_identities=resource_identities,
            capabilities=capabilities,
            adapter_identity=adapter_identity,
            observed_monotonic=float(observed),
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "schema": SNAPSHOT_SCHEMA,
            "request_id": self.request_id,
            "lane_id": self.lane_id,
            "controller_identity": _thaw(self.controller_identity),
            "capability": self.capability,
            "action": self.action,
            "resources": list(self.resources),
            "snapshot_id": self.snapshot_id,
            "identity": _thaw(self.identity),
            "resource_identities": _thaw(self.resource_identities),
            "capabilities": _thaw(self.capabilities),
            "adapter_identity": _thaw(self.adapter_identity),
            "observed_monotonic": self.observed_monotonic,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.to_record())


@dataclass(frozen=True)
class CapabilityApproval:
    """Signed approval bound to one request and one observed snapshot."""

    approval_id: str
    request_id: str
    request_sha256: str
    lane_id: str
    controller_identity: dict[str, Any]
    capability: str
    action: str
    arguments: dict[str, Any]
    resources: tuple[str, ...]
    snapshot_id: str
    snapshot_sha256: str
    policy: dict[str, Any]
    issued_monotonic: float
    expires_monotonic: float
    decision: str
    public_key: str
    signature: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "controller_identity", _freeze(self.controller_identity))
        object.__setattr__(self, "arguments", _freeze(self.arguments))
        object.__setattr__(self, "resources", tuple(self.resources))
        object.__setattr__(self, "policy", _freeze(self.policy))

    @classmethod
    def from_record(cls, value: Any) -> "CapabilityApproval":
        expected = {
            "schema", "approval_id", "request_id", "request_sha256", "lane_id", "controller_identity",
            "capability", "action", "arguments", "resources", "snapshot_id", "snapshot_sha256", "policy",
            "issued_monotonic", "expires_monotonic", "decision", "public_key", "signature",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise CapabilityDenied("APPROVAL_NOT_CLOSED", "approval schema is not closed", stage="approval")
        if value.get("schema") != APPROVAL_SCHEMA:
            raise CapabilityDenied("APPROVAL_SCHEMA", "approval schema is invalid", stage="approval")
        approval_id = _identifier(value.get("approval_id"), "approval_id")
        request_id = _identifier(value.get("request_id"), "approval request_id")
        lane_id = _identifier(value.get("lane_id"), "approval lane_id")
        for key in ("request_sha256", "snapshot_sha256"):
            if not isinstance(value.get(key), str) or not re.fullmatch(r"[0-9a-f]{64}", value[key]):
                raise CapabilityDenied("APPROVAL_DIGEST", f"{key} is not a SHA-256 digest", stage="approval")
        controller_identity = _process_identity(value.get("controller_identity"), "approval controller_identity")
        capability = _identifier(value.get("capability"), "approval capability")
        action = _identifier(value.get("action"), "approval action")
        arguments = _mapping(value.get("arguments"), "approval arguments")
        raw_resources = value.get("resources")
        if not isinstance(raw_resources, list) or not raw_resources or any(not isinstance(item, str) or not item for item in raw_resources):
            raise CapabilityDenied("APPROVAL_RESOURCES", "approval resources are invalid", stage="approval")
        resources = tuple(sorted(raw_resources))
        if len(resources) != len(set(resources)):
            raise CapabilityDenied("APPROVAL_RESOURCES", "approval resources are duplicated", stage="approval")
        snapshot_id = _identifier(value.get("snapshot_id"), "approval snapshot_id")
        policy = _mapping(value.get("policy"), "approval policy")
        if not policy:
            raise CapabilityDenied("APPROVAL_POLICY", "approval policy binding is empty", stage="approval")
        issued = _finite_time(value.get("issued_monotonic"), "approval issue time")
        expires = _finite_time(value.get("expires_monotonic"), "approval expiry")
        if issued > expires:
            raise CapabilityDenied("APPROVAL_WINDOW", "approval time window is inverted", stage="approval")
        if value.get("decision") != "approve":
            raise CapabilityDenied("APPROVAL_DENIED", "approval decision is not approve", stage="approval")
        public_key = value.get("public_key")
        signature = value.get("signature")
        if not isinstance(public_key, str) or not public_key or not isinstance(signature, str) or not signature:
            raise CapabilityDenied("APPROVAL_SIGNATURE", "approval signature material is incomplete", stage="approval")
        return cls(
            approval_id=approval_id,
            request_id=request_id,
            request_sha256=value["request_sha256"],
            lane_id=lane_id,
            controller_identity=controller_identity,
            capability=capability,
            action=action,
            arguments=arguments,
            resources=resources,
            snapshot_id=snapshot_id,
            snapshot_sha256=value["snapshot_sha256"],
            policy=policy,
            issued_monotonic=issued,
            expires_monotonic=expires,
            decision="approve",
            public_key=public_key,
            signature=signature,
        )

    def to_record(self, *, include_signature: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema": APPROVAL_SCHEMA,
            "approval_id": self.approval_id,
            "request_id": self.request_id,
            "request_sha256": self.request_sha256,
            "lane_id": self.lane_id,
            "controller_identity": _thaw(self.controller_identity),
            "capability": self.capability,
            "action": self.action,
            "arguments": _thaw(self.arguments),
            "resources": list(self.resources),
            "snapshot_id": self.snapshot_id,
            "snapshot_sha256": self.snapshot_sha256,
            "policy": _thaw(self.policy),
            "issued_monotonic": self.issued_monotonic,
            "expires_monotonic": self.expires_monotonic,
            "decision": self.decision,
            "public_key": self.public_key,
        }
        if include_signature:
            result["signature"] = self.signature
        return result

    @property
    def signed_payload(self) -> bytes:
        return canonical_json_bytes(self.to_record(include_signature=False))

    @property
    def sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.to_record())).hexdigest()


@dataclass(frozen=True)
class CapabilityPermit:
    """Immutable in-memory handoff from the controller to an adapter."""

    request: CapabilityRequest
    snapshot: CapabilitySnapshot
    approval: CapabilityApproval
    claims: tuple[dict[str, Any], ...]
    owner_identity: dict[str, Any]
    adapter_identity: dict[str, str]
    expires_monotonic: float
    permit_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "claims", tuple(_freeze(item) for item in self.claims))
        object.__setattr__(self, "owner_identity", _freeze(self.owner_identity))
        object.__setattr__(self, "adapter_identity", _freeze(self.adapter_identity))

    def to_record(self) -> dict[str, Any]:
        return _thaw({
            "schema": PERMIT_SCHEMA,
            "request": self.request.to_record(),
            "snapshot": self.snapshot.to_record(),
            "approval": self.approval.to_record(),
            "claims": list(self.claims),
            "owner_identity": self.owner_identity,
            "adapter_identity": self.adapter_identity,
            "expires_monotonic": self.expires_monotonic,
            "permit_sha256": self.permit_sha256,
        })


@dataclass(frozen=True)
class AdapterResult:
    """Adapter output reduced to a generic raw/interpreted result pair."""

    succeeded: bool
    raw_result: Any
    interpreted_result: dict[str, Any]
    adapter_identity: dict[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "raw_result", _freeze(self.raw_result))
        object.__setattr__(self, "interpreted_result", _freeze(self.interpreted_result))
        object.__setattr__(self, "adapter_identity", _freeze(self.adapter_identity))
        if not isinstance(self.succeeded, bool):
            raise CapabilityAdapterError("adapter result success flag is invalid")
        _public_json(self.raw_result, label="adapter raw result")
        _public_json(self.interpreted_result, label="adapter interpreted result")
        _adapter_identity(self.adapter_identity)

    @property
    def raw_result_sha256(self) -> str:
        return canonical_sha256(self.raw_result)


@dataclass(frozen=True)
class CleanupEvidence:
    """Adapter proof consumed by the broker before release."""

    proved: bool
    boundary: dict[str, Any]
    identities: tuple[dict[str, Any], ...]
    details: dict[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "boundary", _freeze(self.boundary))
        object.__setattr__(self, "identities", tuple(_freeze(item) for item in self.identities))
        object.__setattr__(self, "details", _freeze(self.details))
        if not isinstance(self.proved, bool):
            raise CapabilityAdapterError("cleanup proof flag is invalid")
        _public_json(self.boundary, label="cleanup boundary")
        _public_json(list(self.identities), label="cleanup identities")
        _public_json(self.details, label="cleanup details")

    def to_record(self) -> dict[str, Any]:
        return _thaw({
            "schema": CLEANUP_SCHEMA,
            "proved": self.proved,
            "boundary": self.boundary,
            "identities": list(self.identities),
            "details": self.details,
        })


@dataclass(frozen=True)
class CapabilityResult:
    """Structured terminal or denial fact returned by :class:`CapabilityBroker`."""

    record: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "record", _freeze(self.record))

    @property
    def outcome(self) -> str:
        return str(self.record.get("outcome", "UNKNOWN"))

    @property
    def state(self) -> str:
        return str(self.record.get("state", "UNKNOWN"))

    @property
    def request_id(self) -> str | None:
        value = self.record.get("request_id")
        return value if isinstance(value, str) else None

    def to_record(self) -> dict[str, Any]:
        return _thaw(self.record)


class CapabilityAdapter(Protocol):
    """Small adapter seam; private endpoint state stays inside the adapter."""

    @property
    def adapter_identity(self) -> Mapping[str, str]: ...

    def supports(self, request: CapabilityRequest) -> bool: ...

    def observe(self, request: CapabilityRequest) -> CapabilitySnapshot: ...

    def dispatch(self, permit: CapabilityPermit) -> AdapterResult: ...

    def effective_expiry(
        self,
        request: CapabilityRequest,
        snapshot: CapabilitySnapshot,
        approval: CapabilityApproval,
        now_monotonic: float,
    ) -> float: ...

    def cleanup(
        self,
        permit: CapabilityPermit,
        dispatch_result: AdapterResult | None,
        failure: Mapping[str, Any] | None,
    ) -> CleanupEvidence: ...


ApprovalVerifier = Callable[[bytes, str, str], bool]
PolicyVerifier = Callable[[CapabilityRequest, CapabilitySnapshot, CapabilityApproval], bool]
IdentityProvider = Callable[[], Mapping[str, Any] | None]
ClaimsFactory = Callable[[str, str], Any]


def current_process_identity() -> dict[str, Any] | None:
    """Build the generic controller identity from the accepted OS primitives."""

    pid = os.getpid()
    exact = exact_process_identity(pid)
    process = process_snapshot().by_pid.get(pid)
    if exact is None or process is None or process.created_utc is None:
        return None
    return {
        "pid": pid,
        "created_utc": iso_utc(process.created_utc),
        "creation_identity": exact.get("created_utc"),
    }


class FakeCapabilityAdapter:
    """Deterministic disposable adapter used by contract tests and local smoke."""

    def __init__(
        self,
        capabilities: Mapping[str, Sequence[str]],
        *,
        snapshot_identity: Mapping[str, Any] | None = None,
        adapter_identity: Mapping[str, str] | None = None,
        result_factory: Callable[[CapabilityPermit], AdapterResult] | None = None,
        fail_dispatch: bool = False,
        cleanup_proved: bool = True,
    ) -> None:
        self._capabilities = {str(key): tuple(sorted(str(item) for item in value)) for key, value in capabilities.items()}
        self._snapshot_identity = dict(snapshot_identity or {"revision": "fake-snapshot-1"})
        self._adapter_identity = _adapter_identity(adapter_identity or {"adapter_id": "fake", "adapter_version": "v1"})
        self._result_factory = result_factory
        self.fail_dispatch = fail_dispatch
        self.cleanup_proved = cleanup_proved
        self.support_calls = 0
        self.observe_calls = 0
        self.dispatch_calls = 0
        self.cleanup_calls = 0
        self.permits: list[dict[str, Any]] = []

    @property
    def adapter_identity(self) -> Mapping[str, str]:
        return dict(self._adapter_identity)

    def supports(self, request: CapabilityRequest) -> bool:
        self.support_calls += 1
        return request.action in self._capabilities.get(request.capability, ())

    def observe(self, request: CapabilityRequest) -> CapabilitySnapshot:
        self.observe_calls += 1
        return CapabilitySnapshot(
            request_id=request.request_id,
            lane_id=request.lane_id,
            controller_identity=dict(request.controller_identity),
            capability=request.capability,
            action=request.action,
            resources=request.resources,
            # The fake represents one stable observed target.  The call count
            # remains visible separately, while an approval can bind the same
            # snapshot across the preflight and broker observation.
            snapshot_id="snapshot-1",
            identity=dict(self._snapshot_identity),
            resource_identities={resource: {"revision": "fake-resource-1"} for resource in request.resources},
            capabilities={key: tuple(value) for key, value in self._capabilities.items()},
            adapter_identity=dict(self._adapter_identity),
            observed_monotonic=0.0,
        )

    def dispatch(self, permit: CapabilityPermit) -> AdapterResult:
        self.dispatch_calls += 1
        self.permits.append(permit.to_record())
        if self.fail_dispatch:
            raise CapabilityAdapterError("fake dispatch failed")
        if self._result_factory is not None:
            result = self._result_factory(permit)
        else:
            result = AdapterResult(
                succeeded=True,
                raw_result={"status": "completed", "request_id": permit.request.request_id},
                interpreted_result={"status": "PASS"},
                adapter_identity=dict(self._adapter_identity),
            )
        return result

    def cleanup(
        self,
        permit: CapabilityPermit,
        dispatch_result: AdapterResult | None,
        failure: Mapping[str, Any] | None,
    ) -> CleanupEvidence:
        self.cleanup_calls += 1
        return CleanupEvidence(
            proved=self.cleanup_proved,
            boundary={"complete": self.cleanup_proved, "live_identities": [] if self.cleanup_proved else [{"unknown": True}]},
            identities=(),
            details={"fake": True, "dispatch_failed": failure is not None},
        )


class CapabilityBroker:
    """One closed request-to-cleanup state machine for generic capabilities."""

    def __init__(
        self,
        adapter: CapabilityAdapter,
        *,
        approval_verifier: ApprovalVerifier | Any | None,
        policy_verifier: PolicyVerifier | None,
        identity_provider: IdentityProvider | None = None,
        claims_factory: ClaimsFactory | None = None,
        claims_root: Path | None = None,
        state_root: Path | None = None,
        clock: Callable[[], float] = time.monotonic,
        wait_excess_seconds: float = 30.0,
        evidence_sink: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.adapter = adapter
        self.approval_verifier = approval_verifier
        self.policy_verifier = policy_verifier
        self.identity_provider = identity_provider or current_process_identity
        self.clock = clock
        self.wait_excess_seconds = wait_excess_seconds
        self.evidence_sink = evidence_sink
        self._claims_factory = claims_factory
        self.claims_root = claims_root.resolve() if claims_root is not None else None
        if state_root is not None and Path(state_root).is_symlink():
            raise ValueError("capability state root cannot be a symlink")
        self.state_root = state_root.resolve() if state_root is not None else None
        if self.state_root is not None:
            if self.state_root.is_symlink():
                raise ValueError("capability state root cannot be a symlink")
            self.state_root.mkdir(parents=True, exist_ok=True)
            self._state_requests_root = self.state_root / "requests"
            self._state_approvals_root = self.state_root / "approvals"
            for root in (self._state_requests_root, self._state_approvals_root):
                if root.is_symlink():
                    raise ValueError("capability state directory cannot be a symlink")
                root.mkdir(parents=True, exist_ok=True)
        else:
            self._state_requests_root = None
            self._state_approvals_root = None
        self._active: dict[str, str] = {}
        self._terminal: dict[str, CapabilityResult] = {}
        self._used_approvals: set[str] = set()
        self._state_lock = threading.Lock()

        try:
            self._adapter_identity = _adapter_identity(adapter.adapter_identity)
        except (AttributeError, CapabilityError) as exc:
            raise ValueError("capability adapter identity is unavailable") from exc

    @staticmethod
    def _state_filename(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest() + ".json"

    def _state_path(self, kind: str, value: str) -> Path | None:
        root = self._state_requests_root if kind == "request" else self._state_approvals_root
        return root / self._state_filename(value) if root is not None else None

    def _read_state(self, path: Path) -> dict[str, Any] | None:
        if path.is_symlink() or not path.is_file():
            if not path.exists() and not path.is_symlink():
                return None
            raise CapabilityError("durable capability state path is not a regular file")
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CapabilityError("durable capability state is unreadable") from exc
        if not isinstance(value, dict) or value.get("schema") != STATE_SCHEMA:
            raise CapabilityError("durable capability state schema is invalid")
        copied = _public_json(value, label="durable capability state")
        if not isinstance(copied, dict):
            raise CapabilityError("durable capability state is not an object")
        return copied

    def _write_state(self, path: Path, value: Mapping[str, Any], *, expected: Any | None = None) -> None:
        raw = canonical_json_bytes(value)
        target = expected if expected is not None else capture_target(path.parent, path.name)
        mutation_replace(path.parent, path.name, raw, expected=target)

    def _supplied_approval_sha(self, value: CapabilityApproval | Mapping[str, Any]) -> tuple[str | None, str | None]:
        try:
            record = value.to_record() if isinstance(value, CapabilityApproval) else value
            if not isinstance(record, Mapping):
                return None, None
            approval_id = record.get("approval_id")
            approval_sha = canonical_sha256(record)
            return approval_id if isinstance(approval_id, str) else None, approval_sha
        except (CapabilityError, TypeError, ValueError):
            return None, None

    def _load_durable_retry(
        self,
        request: CapabilityRequest,
        approval_value: CapabilityApproval | Mapping[str, Any],
    ) -> CapabilityResult | None:
        path = self._state_path("request", request.request_id)
        if path is None:
            return None
        state = self._read_state(path)
        if state is None:
            return None
        if state.get("request_sha256") != request.sha256:
            raise CapabilityDenied(
                "REPLAY_MISMATCH",
                "request identity was reused with changed semantics",
                stage="request",
            )
        approval_id, approval_sha = self._supplied_approval_sha(approval_value)
        if approval_id != state.get("approval_id") or approval_sha != state.get("approval_sha256"):
            raise CapabilityDenied(
                "APPROVAL_REPLAY",
                "request retry does not carry the exact consumed approval",
                stage="approval",
            )
        stage = state.get("stage")
        if stage in {"TERMINAL", "TERMINAL_PENDING_RELEASE"}:
            terminal = state.get("terminal_result")
            if not isinstance(terminal, Mapping):
                raise CapabilityDenied("STATE_INVALID", "durable terminal result is missing", stage="state")
            result = CapabilityResult(dict(terminal))
            with self._state_lock:
                self._terminal[request.request_id] = result
            self._publish(result.to_record())
            return result
        raise CapabilityDenied(
            "REQUEST_INDETERMINATE",
            "durable request state records an admitted operation without a terminal fact",
            stage="state",
            details={"state": stage},
        )

    def _reserve_permit_state(
        self,
        request: CapabilityRequest,
        snapshot: CapabilitySnapshot,
        approval: CapabilityApproval,
        permit: CapabilityPermit,
        wait_findings: Sequence[Mapping[str, Any]],
    ) -> tuple[bool, str | None]:
        request_path = self._state_path("request", request.request_id)
        approval_path = self._state_path("approval", approval.approval_id)
        if request_path is None or approval_path is None:
            return True, None
        approval_record = {
            "schema": STATE_SCHEMA,
            "stage": "APPROVAL_CONSUMED",
            "request_id": request.request_id,
            "request_sha256": request.sha256,
            "approval_id": approval.approval_id,
            "approval_sha256": approval.sha256,
            "permit_sha256": permit.permit_sha256,
        }
        try:
            self._write_state(approval_path, approval_record)
        except MutationConflict:
            return False, "approval was already durably consumed"
        except (MutationUnsupported, OSError, CapabilityError) as exc:
            return False, f"approval state could not be durably written: {type(exc).__name__}"
        state = {
            "schema": STATE_SCHEMA,
            "stage": "PERMIT_COMMITTED",
            "request_id": request.request_id,
            "request_sha256": request.sha256,
            "approval_id": approval.approval_id,
            "approval_sha256": approval.sha256,
            "snapshot_sha256": snapshot.sha256,
            "permit_sha256": permit.permit_sha256,
            "request": request.to_record(),
            "snapshot": snapshot.to_record(),
            "approval": approval.to_record(),
            "permit": permit.to_record(),
            "wait_findings": list(wait_findings),
            "terminal_result": None,
            "updated_monotonic": self.clock(),
        }
        try:
            self._write_state(request_path, state)
        except MutationConflict:
            return False, "request identity was already durably admitted"
        except (MutationUnsupported, OSError, CapabilityError) as exc:
            return False, f"request state could not be durably written: {type(exc).__name__}"
        return True, None

    def _persist_terminal_state(
        self,
        request: CapabilityRequest,
        approval: CapabilityApproval,
        permit: CapabilityPermit,
        record: Mapping[str, Any],
        *,
        stage: str,
    ) -> tuple[bool, str | None]:
        path = self._state_path("request", request.request_id)
        if path is None:
            return True, None
        try:
            current = self._read_state(path)
            if current is None:
                return False, "durable request state disappeared before terminal publication"
            updated = dict(current)
            updated["stage"] = stage
            updated["terminal_result"] = dict(record)
            updated["updated_monotonic"] = self.clock()
            self._write_state(path, updated, expected=capture_target(path.parent, path.name))
            return True, None
        except (MutationConflict, MutationUnsupported, OSError, CapabilityError) as exc:
            return False, f"terminal state could not be durably written: {type(exc).__name__}"

    def _effective_expiry(
        self,
        request: CapabilityRequest,
        snapshot: CapabilitySnapshot,
        approval: CapabilityApproval,
    ) -> float:
        candidate = min(request.expires_monotonic, approval.expires_monotonic)
        authority = getattr(self.adapter, "effective_expiry", None)
        if callable(authority):
            try:
                candidate = min(candidate, float(authority(request, snapshot, approval, self.clock())))
            except Exception as exc:
                raise CapabilityDenied(
                    "APPROVAL_POLICY_UNAVAILABLE",
                    "capability duration policy could not be evaluated",
                    stage="approval",
                    details={"error_type": type(exc).__name__},
                ) from exc
        if not math.isfinite(candidate):
            raise CapabilityDenied("INVALID_EXPIRY", "effective capability expiry is not finite", stage="approval")
        return candidate

    def _arm_claims(self, claims: Any) -> list[str]:
        arm = getattr(claims, "arm_boundary", None)
        if not callable(arm):
            raise CapabilityDenied(
                "CLAIM_ARM_UNAVAILABLE",
                "resource claims do not expose the accepted arm_boundary primitive",
                stage="claim",
            )
        failures = arm()
        if not isinstance(failures, (list, tuple)) or any(not isinstance(item, str) for item in failures):
            raise CapabilityDenied("CLAIM_ARM_INVALID", "resource claim arming evidence is malformed", stage="claim")
        return list(failures)

    @staticmethod
    def _cleanup_is_valid(cleanup: CleanupEvidence) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        if cleanup.proved is not True:
            reasons.append("adapter did not prove cleanup")
        boundary = cleanup.boundary if isinstance(cleanup.boundary, Mapping) else {}
        if boundary.get("complete") is not True:
            reasons.append("owned cleanup boundary is incomplete")
        if cleanup.identities:
            reasons.append("cleanup retains live or unresolved identities")
        for key in ("errors", "boundary_errors"):
            value = boundary.get(key)
            if value not in (None, [], (), ""):
                reasons.append(f"cleanup boundary reports {key}")
        for key in ("live_members", "live_identities", "members"):
            value = boundary.get(key)
            if value not in (None, [], (), ""):
                reasons.append(f"cleanup boundary reports {key}")
        details = cleanup.details if isinstance(cleanup.details, Mapping) else {}
        stack: list[tuple[str, Any]] = [(str(key).casefold(), value) for key, value in details.items()]
        while stack:
            key, value = stack.pop()
            normalized = re.sub(r"[^a-z0-9]", "", key)
            if normalized not in {"dispatchfailed", "launchstarted"} and any(token in normalized for token in ("error", "failure", "unresolved")) and value not in (None, False, "", [], ()):
                reasons.append(f"cleanup details report {key}")
            if normalized in {"closed", "helpersstopped", "helperthreadsstopped", "stderrlogcomplete"} and value is not True:
                reasons.append(f"cleanup details do not prove {key}")
            if normalized == "stopped" and value is not True:
                reasons.append(f"cleanup details do not prove {key}")
            if isinstance(value, Mapping):
                stack.extend((str(child).casefold(), item) for child, item in value.items())
            elif isinstance(value, (list, tuple)):
                stack.extend((key, item) for item in value)
        return not reasons, sorted(set(reasons))

    def execute(self, request_value: CapabilityRequest | Mapping[str, Any], approval_value: CapabilityApproval | Mapping[str, Any]) -> CapabilityResult:
        """Run one request or return an exact prior terminal result on retry."""

        request_hint = request_value.get("request_id") if isinstance(request_value, Mapping) else getattr(request_value, "request_id", None)
        try:
            request = CapabilityRequest.from_record(
                request_value.to_record() if isinstance(request_value, CapabilityRequest) else request_value,
                now_monotonic=self.clock(),
            )
        except CapabilityDenied as denial:
            return self._denial(request_hint if isinstance(request_hint, str) else None, denial)
        except (CapabilityError, TypeError, ValueError) as exc:
            return self._denial(request_hint if isinstance(request_hint, str) else None, CapabilityDenied("REQUEST_INVALID", str(exc), stage="request"))

        try:
            durable = self._load_durable_retry(request, approval_value)
        except CapabilityDenied as denial:
            return self._denial(request.request_id, denial)
        except (CapabilityError, OSError, TypeError, ValueError) as exc:
            return self._denial(
                request.request_id,
                CapabilityDenied("STATE_INVALID", "durable capability state could not be read", stage="state", details={"error_type": type(exc).__name__}),
            )
        if durable is not None:
            return durable

        with self._state_lock:
            prior = self._terminal.get(request.request_id)
            if prior is not None:
                if prior.record.get("request_sha256") == request.sha256:
                    return prior
                return self._denial(request.request_id, CapabilityDenied("REPLAY_MISMATCH", "request identity was reused with changed semantics", stage="request"))
            if request.request_id in self._active:
                return self._denial(request.request_id, CapabilityDenied("REQUEST_ACTIVE", "request is already active", stage="request"))
            self._active[request.request_id] = request.sha256
        try:
            return self._execute_new(request, approval_value)
        finally:
            with self._state_lock:
                self._active.pop(request.request_id, None)

    run = execute

    def execute_or_raise(self, request_value: CapabilityRequest | Mapping[str, Any], approval_value: CapabilityApproval | Mapping[str, Any]) -> CapabilityResult:
        result = self.execute(request_value, approval_value)
        if result.outcome == "DENIED":
            denial = result.record.get("denial")
            if isinstance(denial, Mapping):
                raise CapabilityDenied(
                    str(denial.get("reason_code", "DENIED")),
                    str(denial.get("message", "capability request denied")),
                    stage=str(denial.get("stage", "unknown")),
                    details=cast(Mapping[str, Any], denial.get("details", {})),
                )
            raise CapabilityDenied("DENIED", "capability request denied", stage="unknown")
        return result

    def _execute_new(self, request: CapabilityRequest, approval_value: CapabilityApproval | Mapping[str, Any]) -> CapabilityResult:
        claims: Any | None = None
        arming_attempted = False
        wait_findings: list[dict[str, Any]] = []
        try:
            self._verify_owner(request)
            try:
                supported = self.adapter.supports(request)
            except Exception as exc:
                raise CapabilityDenied("CAPABILITY_UNAVAILABLE", "adapter capability check failed", stage="request", details={"error_type": type(exc).__name__}) from exc
            if supported is not True:
                raise CapabilityDenied("CAPABILITY_UNAVAILABLE", "requested capability/action is unavailable", stage="request")
            try:
                snapshot = self.adapter.observe(request)
                snapshot = CapabilitySnapshot.from_record(snapshot.to_record() if isinstance(snapshot, CapabilitySnapshot) else snapshot)
            except CapabilityAdapterUnavailable as exc:
                raise CapabilityDenied(
                    "SNAPSHOT_UNAVAILABLE",
                    "adapter snapshot is unavailable",
                    stage="snapshot",
                    details={"error_type": type(exc).__name__},
                ) from exc
            except Exception as exc:
                raise CapabilityDenied("SNAPSHOT_INVALID", "adapter snapshot is unavailable or invalid", stage="snapshot", details={"error_type": type(exc).__name__}) from exc
            self._verify_snapshot(request, snapshot)
            approval = self._verify_approval(request, snapshot, approval_value)
            effective_expiry = self._effective_expiry(request, snapshot, approval)
            if self.clock() >= effective_expiry:
                raise CapabilityDenied("APPROVAL_EXPIRED", "authorized capability duration is already exhausted", stage="approval")
        except CapabilityDenied as denial:
            return self._denial(request.request_id, denial)
        except Exception as exc:
            return self._denial(
                request.request_id,
                CapabilityDenied("ADMISSION_FAILED", "capability admission failed closed", stage="approval", details={"error_type": type(exc).__name__}),
            )

        try:
            claims = self._new_claims(request)

            def on_wait(finding: Mapping[str, Any]) -> None:
                fact = {key: finding.get(key) for key in ("resource", "state", "reason", "wait_seconds", "actionable") if key in finding}
                wait_findings.append(_public_json(fact, label="resource finding"))
                state = finding.get("state")
                if state in {"CONTENDED"}:
                    return
                if state == "PROVEN_STALE":
                    return
                raise CapabilityDenied("RESOURCE_UNCERTAIN", "resource ownership is malformed, stale-uncertain, or unavailable", stage="claim", details={"finding": fact})

            claims.acquire_all(list(request.resources), on_wait=on_wait)
            claim_records = self._exact_claims(claims, request)
            self._verify_owner(request)
            if self.clock() >= min(request.expires_monotonic, approval.expires_monotonic):
                raise CapabilityDenied("APPROVAL_EXPIRED", "approval expired before claim arming", stage="claim")
            self._verify_approval(request, snapshot, approval)
            effective_expiry = self._effective_expiry(request, snapshot, approval)
            if self.clock() >= effective_expiry:
                raise CapabilityDenied("APPROVAL_EXPIRED", "authorized capability duration expired before claim arming", stage="claim")
            self._exact_claims(claims, request)
            arming_attempted = True
            arm_failures = self._arm_claims(claims)
            if arm_failures:
                raise CapabilityDenied(
                    "CLAIM_ARM_FAILED",
                    "not every exact native resource claim could be durably armed",
                    stage="claim",
                    details={"arming_failures": arm_failures},
                )
            claim_records = self._exact_claims(claims, request, require_armed=True)
        except CapabilityDenied as denial:
            if claims is not None and arming_attempted:
                retained = self._retain_claims(
                    claims,
                    CleanupEvidence(
                        proved=False,
                        boundary={"complete": False, "errors": ["claim arming did not complete"]},
                        identities=(),
                        details={"arming_failure": denial.reason_code},
                    ),
                )
                denial.details["claims_released"] = False
                denial.details["retention_failures"] = retained
            else:
                release = self._release_claims(claims)
                denial.details["claims_released"] = release[0]
                if not release[0]:
                    denial.details["release_failures"] = release[1]
                    denial.details["retention_failures"] = self._retain_claims(
                        claims,
                        CleanupEvidence(
                            proved=False,
                            boundary={"complete": False, "errors": ["claim release was not proved"]},
                            identities=(),
                            details={"release_failures": release[1]},
                        ),
                    )
            return self._denial(request.request_id, denial)
        except Exception as exc:
            if claims is not None and arming_attempted:
                retained = self._retain_claims(
                    claims,
                    CleanupEvidence(
                        proved=False,
                        boundary={"complete": False, "errors": ["claim arming raised"]},
                        identities=(),
                        details={"arming_error": type(exc).__name__},
                    ),
                )
                denial = CapabilityDenied("CLAIM_ARM_FAILED", "resource claim arming failed closed", stage="claim", details={"error_type": type(exc).__name__, "claims_released": False, "retention_failures": retained})
            else:
                release = self._release_claims(claims)
                retention_failures: list[str] = []
                if not release[0]:
                    retention_failures = self._retain_claims(
                        claims,
                        CleanupEvidence(
                            proved=False,
                            boundary={"complete": False, "errors": ["claim release was not proved"]},
                            identities=(),
                            details={"release_failures": release[1]},
                        ),
                    )
                denial = CapabilityDenied(
                    "CLAIM_FAILED",
                    "resource claim was not safely acquired",
                    stage="claim",
                    details={
                        "error_type": type(exc).__name__,
                        "claims_released": release[0],
                        "release_failures": release[1],
                        "retention_failures": retention_failures,
                    },
                )
            return self._denial(request.request_id, denial)

        permit_record = {
            "schema": PERMIT_SCHEMA,
            "request": request.to_record(),
            "snapshot": snapshot.to_record(),
            "approval": approval.to_record(),
            "claims": claim_records,
            "owner_identity": dict(request.controller_identity),
            "adapter_identity": dict(self._adapter_identity),
            "expires_monotonic": effective_expiry,
        }
        permit_sha = canonical_sha256(permit_record)
        permit = CapabilityPermit(
            request=request,
            snapshot=snapshot,
            approval=approval,
            claims=tuple(claim_records),
            owner_identity=dict(request.controller_identity),
            adapter_identity=dict(self._adapter_identity),
            expires_monotonic=effective_expiry,
            permit_sha256=permit_sha,
        )
        state_ok, state_error = self._reserve_permit_state(request, snapshot, approval, permit, wait_findings)
        if not state_ok:
            retained = self._retain_claims(
                claims,
                CleanupEvidence(
                    proved=False,
                    boundary={"complete": False, "errors": ["durable at-most-once state was not committed"]},
                    identities=(),
                    details={"state_error": state_error or "unknown"},
                ),
            )
            reason_code = "APPROVAL_REPLAY" if state_error and "approval" in state_error else "STATE_PERSIST_FAILED"
            denial = CapabilityDenied(reason_code, state_error or "durable request state was not committed", stage="state", details={"claims_released": False, "retention_failures": retained})
            return self._denial(request.request_id, denial)
        with self._state_lock:
            approval_replayed = approval.approval_id in self._used_approvals
            if not approval_replayed:
                self._used_approvals.add(approval.approval_id)
        if approval_replayed:
            retained = self._retain_claims(claims, CleanupEvidence(proved=False, boundary={"complete": False, "errors": ["approval replay"]}, identities=(), details={"approval_replay": True}))
            denial = CapabilityDenied("APPROVAL_REPLAY", "approval was already consumed", stage="dispatch", details={"claims_released": False, "retention_failures": retained})
            return self._denial(request.request_id, denial)

        dispatch_result: AdapterResult | None = None
        dispatch_failure: dict[str, Any] | None = None
        dispatch_count = 0
        try:
            if self.clock() >= permit.expires_monotonic:
                raise CapabilityAdapterError("permit expired immediately before dispatch")
            self._verify_owner(request)
            self._exact_claims(claims, request, require_armed=True)
            dispatch_count = 1
            raw_dispatch = self.adapter.dispatch(permit)
            if not isinstance(raw_dispatch, AdapterResult):
                raise CapabilityAdapterError("adapter returned an invalid result type")
            if raw_dispatch.adapter_identity != self._adapter_identity:
                raise CapabilityAdapterError("adapter result identity changed")
            dispatch_result = raw_dispatch
        except Exception as exc:
            dispatch_failure = {"error_type": type(exc).__name__, "reason_code": "DISPATCH_FAILED"}

        cleanup = self._cleanup_adapter(permit, dispatch_result, dispatch_failure)
        if dispatch_result is None:
            raw_result = {"status": "FAIL", "reason_code": (dispatch_failure or {}).get("reason_code", "DISPATCH_FAILED")}
            interpreted = {"status": "FAIL"}
            dispatch_succeeded = False
        else:
            raw_result = dispatch_result.raw_result
            interpreted = dispatch_result.interpreted_result
            dispatch_succeeded = dispatch_result.succeeded
        raw_result_sha = canonical_sha256(raw_result)
        cleanup_valid, cleanup_reasons = self._cleanup_is_valid(cleanup)

        def make_record(
            *,
            outcome: str,
            terminal_reason: str,
            claims_released: bool,
            release_failures: Sequence[str],
            retention_failures: Sequence[str],
        ) -> dict[str, Any]:
            record: dict[str, Any] = {
                "schema": RESULT_SCHEMA,
                "request_id": request.request_id,
                "request_sha256": request.sha256,
                "state": "TERMINAL",
                "outcome": outcome,
                "terminal_reason": terminal_reason,
                "lane_id": request.lane_id,
                "capability": request.capability,
                "action": request.action,
                "arguments": dict(request.arguments),
                "resources": list(request.resources),
                "controller_identity": dict(request.controller_identity),
                "snapshot_id": snapshot.snapshot_id,
                "snapshot_sha256": snapshot.sha256,
                "approval_id": approval.approval_id,
                "approval_sha256": approval.sha256,
                "permit_sha256": permit.permit_sha256,
                "adapter_identity": dict(self._adapter_identity),
                "raw_result": raw_result,
                "raw_result_sha256": raw_result_sha,
                "interpreted_result": interpreted,
                "cleanup": cleanup.to_record(),
                "cleanup_validation": {"valid": cleanup_valid, "reasons": list(cleanup_reasons)},
                "claims_released": claims_released,
                "release_failures": list(release_failures),
                "retention_failures": list(retention_failures),
                "dispatch_count": dispatch_count,
                "wait_findings": wait_findings,
            }
            if dispatch_failure is not None:
                record["dispatch_failure"] = dispatch_failure
            return record

        if not cleanup_valid:
            retention_failures = self._retain_claims(claims, cleanup)
            record = make_record(
                outcome="UNCERTAIN",
                terminal_reason="cleanup_or_release_unproved",
                claims_released=False,
                release_failures=(),
                retention_failures=retention_failures,
            )
            state_ok, state_error = self._persist_terminal_state(request, approval, permit, record, stage="TERMINAL")
            if not state_ok:
                record["durable_state_error"] = state_error
            result = CapabilityResult(record)
        else:
            pending = make_record(
                outcome="UNCERTAIN",
                terminal_reason="release_pending",
                claims_released=False,
                release_failures=(),
                retention_failures=(),
            )
            pending_ok, pending_error = self._persist_terminal_state(request, approval, permit, pending, stage="TERMINAL_PENDING_RELEASE")
            if not pending_ok:
                retention_failures = self._retain_claims(claims, cleanup)
                record = make_record(
                    outcome="UNCERTAIN",
                    terminal_reason="terminal_state_persist_failed",
                    claims_released=False,
                    release_failures=(),
                    retention_failures=retention_failures,
                )
                record["durable_state_error"] = pending_error
                result = CapabilityResult(record)
            else:
                claims_released, release_failures = self._release_claims(claims)
                retention_failures: list[str] = []
                if not claims_released:
                    retention_failures = self._retain_claims(claims, cleanup)
                if not claims_released:
                    outcome, terminal_reason = "UNCERTAIN", "cleanup_or_release_unproved"
                elif not dispatch_succeeded:
                    outcome, terminal_reason = "FAIL", "adapter_result_failed"
                else:
                    outcome, terminal_reason = "PASS", "completed"
                record = make_record(
                    outcome=outcome,
                    terminal_reason=terminal_reason,
                    claims_released=claims_released,
                    release_failures=release_failures,
                    retention_failures=retention_failures,
                )
                final_ok, final_error = self._persist_terminal_state(request, approval, permit, record, stage="TERMINAL")
                if not final_ok:
                    record["outcome"] = "UNCERTAIN"
                    record["terminal_reason"] = "terminal_state_persist_failed_after_release"
                    record["durable_state_error"] = final_error
                result = CapabilityResult(record)
        with self._state_lock:
            self._terminal[request.request_id] = result
        self._publish(result.to_record())
        return result

    def _verify_owner(self, request: CapabilityRequest) -> None:
        if self.identity_provider is None:
            raise CapabilityDenied("IDENTITY_UNAVAILABLE", "current controller identity is unavailable", stage="snapshot")
        try:
            current = self.identity_provider()
        except Exception as exc:
            raise CapabilityDenied("IDENTITY_UNAVAILABLE", "current controller identity could not be observed", stage="snapshot", details={"error_type": type(exc).__name__}) from exc
        if current is None:
            raise CapabilityDenied("IDENTITY_UNAVAILABLE", "current controller identity is unavailable", stage="snapshot")
        try:
            normalized = _process_identity(current, "current controller identity")
        except CapabilityDenied:
            raise
        if normalized != request.controller_identity:
            raise CapabilityDenied("IDENTITY_MISMATCH", "request controller identity is not the current owner", stage="snapshot")

    def _verify_snapshot(self, request: CapabilityRequest, snapshot: CapabilitySnapshot) -> None:
        if (
            snapshot.request_id != request.request_id
            or snapshot.lane_id != request.lane_id
            or snapshot.controller_identity != request.controller_identity
            or snapshot.capability != request.capability
            or snapshot.action != request.action
            or snapshot.resources != request.resources
            or snapshot.adapter_identity != self._adapter_identity
            or request.action not in snapshot.capabilities.get(request.capability, ())
        ):
            raise CapabilityDenied("SNAPSHOT_MISMATCH", "current capability snapshot contradicts the request", stage="snapshot")

    def _verify_approval(self, request: CapabilityRequest, snapshot: CapabilitySnapshot, value: CapabilityApproval | Mapping[str, Any]) -> CapabilityApproval:
        approval = CapabilityApproval.from_record(value.to_record() if isinstance(value, CapabilityApproval) else value)
        if (
            approval.request_id != request.request_id
            or approval.request_sha256 != request.sha256
            or approval.lane_id != request.lane_id
            or approval.controller_identity != request.controller_identity
            or approval.capability != request.capability
            or approval.action != request.action
            or approval.arguments != request.arguments
            or approval.resources != request.resources
            or approval.snapshot_id != snapshot.snapshot_id
            or approval.snapshot_sha256 != snapshot.sha256
            or approval.expires_monotonic > request.expires_monotonic
        ):
            raise CapabilityDenied("APPROVAL_MISMATCH", "approval is not bound to the exact request and snapshot", stage="approval")
        now = self.clock()
        if now >= approval.expires_monotonic or approval.issued_monotonic > now:
            raise CapabilityDenied("APPROVAL_EXPIRED", "approval is outside its monotonic validity window", stage="approval")
        if self.approval_verifier is None:
            raise CapabilityDenied("APPROVAL_VERIFIER_UNAVAILABLE", "approval trust boundary is unavailable", stage="approval")
        try:
            verify = self.approval_verifier.verify if hasattr(self.approval_verifier, "verify") else self.approval_verifier
            if verify(approval.signed_payload, approval.signature, approval.public_key) is not True:
                raise CapabilityDenied("APPROVAL_SIGNATURE_INVALID", "approval signature is not valid", stage="approval")
        except CapabilityDenied:
            raise
        except Exception as exc:
            raise CapabilityDenied("APPROVAL_SIGNATURE_INVALID", "approval signature verification failed", stage="approval", details={"error_type": type(exc).__name__}) from exc
        if self.policy_verifier is None:
            raise CapabilityDenied("APPROVAL_POLICY_UNAVAILABLE", "approval policy trust boundary is unavailable", stage="approval")
        try:
            if self.policy_verifier(request, snapshot, approval) is not True:
                raise CapabilityDenied("APPROVAL_POLICY_DENIED", "approval policy did not authorize the exact request", stage="approval")
        except CapabilityDenied:
            raise
        except Exception as exc:
            raise CapabilityDenied("APPROVAL_POLICY_UNAVAILABLE", "approval policy could not be verified", stage="approval", details={"error_type": type(exc).__name__}) from exc
        return approval

    def _new_claims(self, request: CapabilityRequest) -> Any:
        if self._claims_factory is not None:
            return self._claims_factory(request.lane_id, request.request_id)
        if self.claims_root is None:
            raise CapabilityDenied("CLAIM_OWNER_UNAVAILABLE", "no resource-claim factory was supplied", stage="claim")
        process = process_snapshot().by_pid.get(os.getpid())
        if process is None:
            raise CapabilityDenied("CLAIM_OWNER_UNAVAILABLE", "current controller process is not observable", stage="claim")
        return ResourceClaims(
            self.claims_root,
            request.lane_id,
            request.request_id,
            process,
            wait_excess_seconds=self.wait_excess_seconds,
            identity_provider=lambda pid: exact_process_identity(pid),
        )

    def _exact_claims(self, claims: Any, request: CapabilityRequest, *, require_armed: bool = False) -> list[dict[str, Any]]:
        held_value = getattr(claims, "held", None)
        held = held_value() if callable(held_value) else held_value
        if not isinstance(held, (list, tuple)):
            raise CapabilityDenied("CLAIM_EVIDENCE_INVALID", "resource claims did not publish held ownership", stage="claim")
        records = [dict(item) for item in held if isinstance(item, Mapping)]
        if len(records) != len(request.resources) or tuple(sorted(str(item.get("resource")) for item in records)) != request.resources:
            raise CapabilityDenied("CLAIM_EVIDENCE_INVALID", "held resource set is not exact", stage="claim")
        for record in records:
            if record.get("owner") != request.controller_identity:
                raise CapabilityDenied("CLAIM_OWNER_MISMATCH", "resource claim belongs to another controller identity", stage="claim")
            if not isinstance(record.get("path"), str) or not record["path"]:
                raise CapabilityDenied("CLAIM_EVIDENCE_INVALID", "resource claim path is unavailable", stage="claim")
            if require_armed and record.get("boundary_state") != BOUNDARY_ARMED_STATE:
                raise CapabilityDenied("CLAIM_NOT_ARMED", "resource claim was not durably armed before dispatch", stage="claim")
        return sorted(records, key=lambda item: str(item["resource"]))

    def _cleanup_adapter(self, permit: CapabilityPermit, dispatch_result: AdapterResult | None, failure: Mapping[str, Any] | None) -> CleanupEvidence:
        try:
            cleanup = self.adapter.cleanup(permit, dispatch_result, failure)
            if not isinstance(cleanup, CleanupEvidence):
                raise CapabilityAdapterError("adapter returned an invalid cleanup proof")
            return cleanup
        except Exception as exc:
            return CleanupEvidence(
                proved=False,
                boundary={"complete": False, "errors": ["adapter cleanup raised"]},
                identities=(),
                details={"error_type": type(exc).__name__},
            )

    def _retain_claims(self, claims: Any, cleanup: CleanupEvidence) -> list[str]:
        retain = getattr(claims, "retain_boundary", None)
        if not callable(retain):
            return ["retain_boundary_unavailable"]
        try:
            # CleanupEvidence intentionally freezes nested facts.  Native
            # ResourceClaims persists ordinary JSON bytes, so thaw the
            # evidence at this one persistence seam rather than leaking
            # MappingProxyType/tuple implementation objects into claims.
            return list(retain(boundary=_thaw(cleanup.boundary), identities=_thaw(list(cleanup.identities))))
        except Exception as exc:
            return [f"retain_boundary_failed:{type(exc).__name__}"]

    def _release_claims(self, claims: Any | None) -> tuple[bool, list[str]]:
        if claims is None:
            return True, []
        try:
            failures = list(claims.release_all())
            held_value = getattr(claims, "held", ())
            held = held_value() if callable(held_value) else held_value
        except Exception as exc:
            return False, [f"release_failed:{type(exc).__name__}"]
        if failures or held:
            return False, failures or ["claims_remain_held"]
        return True, []

    def _denial(self, request_id: str | None, denial: CapabilityDenied) -> CapabilityResult:
        record: dict[str, Any] = {
            "schema": RESULT_SCHEMA,
            "state": "DENIED",
            "outcome": "DENIED",
            "request_id": request_id,
            "dispatch_count": 0,
            "claims_released": True,
            "denial": denial.to_record(),
        }
        result = CapabilityResult(record)
        self._publish(record)
        return result

    def _publish(self, record: dict[str, Any]) -> None:
        if self.evidence_sink is not None:
            try:
                self.evidence_sink(json.loads(json.dumps(_thaw(record), sort_keys=True, separators=(",", ":"), allow_nan=False)))
            except Exception:
                # Evidence publication is support-only; it cannot authorize,
                # release, or turn a product result into success.
                return


__all__ = [
    "APPROVAL_SCHEMA",
    "AdapterResult",
    "CapabilityAdapter",
    "CapabilityAdapterError",
    "CapabilityAdapterUnavailable",
    "CapabilityApproval",
    "CapabilityBroker",
    "CapabilityDenied",
    "CapabilityError",
    "CapabilityPermit",
    "CapabilityRequest",
    "CapabilityResult",
    "CapabilitySnapshot",
    "CLEANUP_SCHEMA",
    "CleanupEvidence",
    "FakeCapabilityAdapter",
    "PERMIT_SCHEMA",
    "REQUEST_SCHEMA",
    "RESULT_SCHEMA",
    "SNAPSHOT_SCHEMA",
    "STATE_SCHEMA",
    "canonical_json_bytes",
    "canonical_sha256",
    "current_process_identity",
]
