"""Firmware-specific capability adapter.

The adapter owns campaign method mapping, controller-local transport state,
child identity, raw response interpretation, and exact cleanup.  Its public
results contain only generic capability facts.
"""
from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Callable, Mapping

from orchestrator_harness.capability_broker import (
    AdapterResult,
    CapabilityAdapterError,
    CapabilityAdapterUnavailable,
    CapabilityPermit,
    CapabilityRequest,
    CapabilitySnapshot,
    CleanupEvidence,
)
from orchestrator_harness.process_supervisor import ProcessSupervisor

from .campaign_pack import DEFAULT_CAMPAIGN_PACK, FirmwareCampaignPack, FirmwareOperation


Launcher = Callable[[Mapping[str, Any]], Any]
ChildIdentityProvider = Callable[[int], Mapping[str, Any] | None]
ConfigProvider = Callable[[CapabilityRequest, FirmwareOperation], Mapping[str, Any]]
SnapshotProvider = Callable[[CapabilityRequest], CapabilitySnapshot | Mapping[str, Any]]
TransportFactory = Callable[[Any, Callable[[], float], str, Mapping[str, Any]], Any]
SupervisorFactory = Callable[[Any, Mapping[str, Any], str], ProcessSupervisor]


@dataclass
class _ActiveOperation:
    process: Any
    identity: Mapping[str, Any] | None
    transport: Any | None
    operation: FirmwareOperation


class FirmwareHardwareAdapter:
    """Controller-owned adapter for the inert campaign's authorized mappings."""

    ADAPTER_IDENTITY = {"adapter_id": "firmware-hardware", "adapter_version": "v1"}

    def __init__(
        self,
        *,
        campaign_pack: FirmwareCampaignPack = DEFAULT_CAMPAIGN_PACK,
        snapshot_provider: SnapshotProvider | None = None,
        launcher: Launcher | None = None,
        identity_provider: ChildIdentityProvider | None = None,
        config_provider: ConfigProvider | None = None,
        transport_factory: TransportFactory | None = None,
        supervisor_factory: SupervisorFactory | None = None,
        graceful_timeout_seconds: float = 5.0,
        force_timeout_seconds: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
        io_timeout: float = 5.0,
    ) -> None:
        self.campaign_pack = campaign_pack
        self.snapshot_provider = snapshot_provider
        self.launcher = launcher
        self.identity_provider = identity_provider
        self.config_provider = config_provider
        self.transport_factory = transport_factory
        self.supervisor_factory = supervisor_factory
        self.graceful_timeout_seconds = graceful_timeout_seconds
        self.force_timeout_seconds = force_timeout_seconds
        self.clock = clock
        self.io_timeout = io_timeout
        self._active: dict[str, _ActiveOperation] = {}
        self._cleanup_results: dict[str, CleanupEvidence] = {}

    @property
    def adapter_identity(self) -> Mapping[str, str]:
        return dict(self.ADAPTER_IDENTITY)

    def supports(self, request: CapabilityRequest) -> bool:
        try:
            self.campaign_pack.resolve(request)
            return True
        except CapabilityAdapterUnavailable:
            return False

    def observe(self, request: CapabilityRequest) -> CapabilitySnapshot:
        if self.snapshot_provider is None:
            raise CapabilityAdapterUnavailable("current target snapshot provider is unavailable")
        value = self.snapshot_provider(request)
        snapshot = CapabilitySnapshot.from_record(value.to_record() if isinstance(value, CapabilitySnapshot) else value)
        if snapshot.adapter_identity != dict(self.ADAPTER_IDENTITY):
            raise CapabilityAdapterError("snapshot belongs to another adapter")
        return snapshot

    def verify_approval(self, request: CapabilityRequest, snapshot: CapabilitySnapshot, approval: Any) -> bool:
        try:
            operation = self.campaign_pack.resolve(request)
        except CapabilityAdapterUnavailable:
            return False
        policy = getattr(approval, "policy", None)
        return isinstance(policy, Mapping) and self.campaign_pack.approval_policy(request, operation, policy)

    def dispatch(self, permit: CapabilityPermit) -> AdapterResult:
        operation = self.campaign_pack.resolve(permit.request)
        if self.launcher is None or self.identity_provider is None or self.config_provider is None or self.transport_factory is None:
            raise CapabilityAdapterUnavailable("controller-owned adapter dependencies are unavailable")
        config = self.config_provider(permit.request, operation)
        if not isinstance(config, Mapping):
            raise CapabilityAdapterError("adapter configuration is not an object")
        process = self.launcher(config)
        active = _ActiveOperation(process=process, identity=None, transport=None, operation=operation)
        self._active[permit.request.request_id] = active
        try:
            pid = getattr(process, "pid", None)
            if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
                raise CapabilityAdapterError("adapter child identity has no valid PID")
            identity = self.identity_provider(pid)
            if not isinstance(identity, Mapping) or identity.get("pid") != pid or not isinstance(identity.get("created_utc"), str) or not identity["created_utc"]:
                raise CapabilityAdapterError("adapter child creation identity is unavailable")
            active.identity = dict(identity)
            remaining = lambda: permit.expires_monotonic - self.clock()
            transport = self.transport_factory(process, remaining, permit.request.request_id, config)
            active.transport = transport
            transport.send(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {"capabilities": {}, "client": {"version": "1"}},
                },
                "initialize",
            )
            transport.receive(1)
            transport.send({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}, "initialized")
            transport.send(
                {"jsonrpc": "2.0", "id": 2, "method": operation.method, "params": {"arguments": dict(operation.arguments)}},
                "dispatch",
            )
            raw = transport.receive(2)
            if not isinstance(raw, Mapping) or not isinstance(raw.get("result"), Mapping):
                raise CapabilityAdapterError("adapter response is not a structured result")
            result_payload = dict(raw["result"])
            failed = result_payload.get("isError") is True or "error" in raw
            return AdapterResult(
                succeeded=not failed,
                raw_result={"status": "FAIL" if failed else "completed", "payload": result_payload},
                interpreted_result={"status": "FAIL" if failed else "PASS"},
                adapter_identity=dict(self.ADAPTER_IDENTITY),
            )
        except Exception:
            # Keep the active operation private so cleanup can still account
            # for a child that was created before dispatch failed.
            raise

    def cleanup(
        self,
        permit: CapabilityPermit,
        dispatch_result: AdapterResult | None,
        failure: Mapping[str, Any] | None,
    ) -> CleanupEvidence:
        request_id = permit.request.request_id
        prior = self._cleanup_results.get(request_id)
        if prior is not None:
            return prior
        active = self._active.pop(request_id, None)
        if active is None:
            result = CleanupEvidence(
                proved=True,
                boundary={"complete": True, "members": []},
                identities=(),
                details={"launch_started": False, "dispatch_failed": failure is not None},
            )
            self._cleanup_results[request_id] = result
            return result

        # Prove the exact child first.  Closing the channel and joining its
        # helpers follows the same ordering as the retained controller path.
        cleanup_result = None
        cleanup_error: str | None = None
        if active.identity is None:
            cleanup_error = "child identity was unavailable"
        else:
            try:
                supervisor = self.supervisor_factory(active.process, active.identity, request_id) if self.supervisor_factory is not None else ProcessSupervisor(
                    active.process,
                    active.identity,
                    graceful_timeout_seconds=self.graceful_timeout_seconds,
                    force_timeout_seconds=self.force_timeout_seconds,
                )
                cleanup_result = supervisor.cleanup()
            except Exception as exc:
                cleanup_error = f"{type(exc).__name__}"

        transport_ok = True
        transport_record: dict[str, Any] = {"closed": False, "helpers_stopped": False}
        if active.transport is not None:
            try:
                closed = active.transport.close_and_join()
                if isinstance(closed, tuple) and len(closed) == 2:
                    transport_ok = bool(closed[0])
                    transport_record["closed"] = transport_ok
                    transport_record["helpers_stopped"] = transport_ok
                    if isinstance(closed[1], Mapping):
                        transport_record["details"] = {key: value for key, value in closed[1].items() if key not in {"endpoint", "transport", "handle"}}
                else:
                    transport_ok = False
            except Exception as exc:
                transport_ok = False
                transport_record["error_type"] = type(exc).__name__

        if cleanup_error is not None or cleanup_result is None:
            result = CleanupEvidence(
                proved=False,
                boundary={"complete": False, "errors": ["exact child cleanup was unavailable"]},
                identities=() if active.identity is None else (dict(active.identity),),
                details={"error_type": cleanup_error or "missing cleanup result", "channel_cleanup": transport_record},
            )
            self._cleanup_results[request_id] = result
            return result
        try:
            cleanup_record = cleanup_result.to_record()
            proved = bool(cleanup_result.proved_reap and transport_ok)
            boundary = {
                "complete": bool(cleanup_result.boundary_complete),
                "members": [],
                "errors": list(cleanup_result.boundary_errors),
                "cleanup": cleanup_result.boundary_cleanup,
            }
            result = CleanupEvidence(
                proved=proved,
                boundary=boundary,
                identities=() if proved else (dict(active.identity),),
                details={"process": cleanup_record, "channel_cleanup": transport_record},
            )
        except Exception as exc:
            result = CleanupEvidence(
                proved=False,
                boundary={"complete": False, "errors": ["exact child cleanup was unavailable"]},
                identities=(dict(active.identity),),
                details={"error_type": type(exc).__name__, "channel_cleanup": transport_record},
            )
        self._cleanup_results[request_id] = result
        return result


HardwareCapabilityAdapter = FirmwareHardwareAdapter


__all__ = ["FirmwareHardwareAdapter", "HardwareCapabilityAdapter"]
