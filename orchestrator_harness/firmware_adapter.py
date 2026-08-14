"""Broker-compatible firmware hardware adapter with injected seams.

The adapter owns controller-private campaign mapping, process boundaries,
transport state, raw response interpretation, and exact cleanup.  The generic
broker receives only public capability facts.  Server, provider, command, and
endpoint details stay private inside the caller-supplied seams.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from .capability_broker import (
    AdapterResult,
    CapabilityAdapterError,
    CapabilityAdapterUnavailable,
    CapabilityPermit,
    CapabilityRequest,
    CapabilitySnapshot,
    CleanupEvidence,
)
from .firmware_campaign import FirmwareCampaignPack, FirmwareOperation
from .models import ProcessInfo, iso_utc
from .process_supervisor import ProcessBoundary, ProcessSupervisor
from .processes import process_snapshot


Launcher = Callable[[Mapping[str, Any]], Any]
ChildIdentityProvider = Callable[[int], Mapping[str, Any] | None]
ConfigProvider = Callable[[CapabilityRequest, FirmwareOperation], Mapping[str, Any]]
SnapshotProvider = Callable[[CapabilityRequest], CapabilitySnapshot | Mapping[str, Any]]
TransportFactory = Callable[[Any, Callable[[], float], str, Mapping[str, Any]], Any]
SupervisorFactory = Callable[..., ProcessSupervisor]
BoundaryFactory = Callable[[], ProcessBoundary]
OperationKey = tuple[str, str]


@dataclass
class _ActiveOperation:
    process: Any | None
    identity: Mapping[str, Any] | None
    supervisor_identity: ProcessInfo | Mapping[str, Any] | None
    transport: Any | None
    boundary: ProcessBoundary | Any
    operation: FirmwareOperation
    launch_attempted: bool = False


class FirmwareHardwareAdapter:
    """Caller-declared firmware adapter for the generic capability broker."""

    ADAPTER_IDENTITY = {"adapter_id": "firmware-hardware", "adapter_version": "v1"}

    def __init__(
        self,
        *,
        campaign_pack: FirmwareCampaignPack,
        snapshot_provider: SnapshotProvider,
        launcher: Launcher,
        identity_provider: ChildIdentityProvider,
        config_provider: ConfigProvider,
        transport_factory: TransportFactory,
        mcp_protocol_version: str = "2024-11-05",
        supervisor_factory: SupervisorFactory | None = None,
        boundary_factory: BoundaryFactory = ProcessBoundary.prepare,
        graceful_timeout_seconds: float = 5.0,
        force_timeout_seconds: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not isinstance(campaign_pack, FirmwareCampaignPack):
            raise TypeError("campaign_pack must be a FirmwareCampaignPack")
        if not isinstance(mcp_protocol_version, str) or not mcp_protocol_version:
            raise ValueError("MCP protocol version must be a non-empty string")
        self.campaign_pack = campaign_pack
        self.snapshot_provider = snapshot_provider
        self.launcher = launcher
        self.identity_provider = identity_provider
        self.config_provider = config_provider
        self.transport_factory = transport_factory
        self.mcp_protocol_version = mcp_protocol_version
        self.supervisor_factory = supervisor_factory
        self.boundary_factory = boundary_factory
        self.graceful_timeout_seconds = graceful_timeout_seconds
        self.force_timeout_seconds = force_timeout_seconds
        self.clock = clock
        self._active: dict[OperationKey, _ActiveOperation] = {}
        self._cleanup_results: dict[OperationKey, CleanupEvidence] = {}
        self._retained_boundaries: dict[OperationKey, Any] = {}

    @property
    def adapter_identity(self) -> Mapping[str, str]:
        return dict(self.ADAPTER_IDENTITY)

    @staticmethod
    def _operation_key(permit: CapabilityPermit) -> OperationKey:
        return (permit.request.request_id, permit.permit_sha256)

    def _operation(self, request: CapabilityRequest) -> FirmwareOperation:
        return self.campaign_pack.resolve(request)

    def supports(self, request: CapabilityRequest) -> bool:
        return self.campaign_pack.supports(request)

    def observe(self, request: CapabilityRequest) -> CapabilitySnapshot:
        operation = self._operation(request)
        value = self.snapshot_provider(request)
        snapshot = CapabilitySnapshot.from_record(value.to_record() if isinstance(value, CapabilitySnapshot) else value)
        if snapshot.adapter_identity != dict(self.ADAPTER_IDENTITY):
            raise CapabilityAdapterError("snapshot belongs to another adapter")
        if (
            snapshot.resources != (operation.canonical_resource,)
            or snapshot.resource_identities.get(operation.canonical_resource) != operation.resource_identity
            or snapshot.identity.get("canonical_resource") != operation.canonical_resource
            or snapshot.identity.get("resource_identity") != operation.resource_identity
        ):
            raise CapabilityAdapterError("snapshot does not bind the exact firmware resource")
        return snapshot

    def verify_approval(self, request: CapabilityRequest, snapshot: CapabilitySnapshot, approval: Any) -> bool:
        try:
            operation = self._operation(request)
            return hasattr(approval, "policy") and self.campaign_pack.approval_policy(request, operation, approval)
        except Exception:
            return False

    def effective_expiry(
        self,
        request: CapabilityRequest,
        snapshot: CapabilitySnapshot,
        approval: Any,
        now_monotonic: float,
    ) -> float:
        operation = self._operation(request)
        if not hasattr(approval, "issued_monotonic"):
            raise CapabilityAdapterUnavailable("approval duration identity is unavailable")
        return min(
            request.expires_monotonic,
            float(approval.expires_monotonic),
            self.campaign_pack.effective_duration(request, operation, now_monotonic=now_monotonic),
        )

    @staticmethod
    def _child_identity(pid: int, exact: Mapping[str, Any], process: ProcessInfo | None) -> dict[str, Any]:
        exact_created = exact.get("created_utc")
        if not isinstance(exact_created, str) or not exact_created:
            raise CapabilityAdapterError("adapter child creation identity is unavailable")
        created = iso_utc(process.created_utc) if process is not None else exact_created
        if not isinstance(created, str) or not created:
            raise CapabilityAdapterError("adapter child process timestamp is unavailable")
        return {"pid": pid, "created_utc": created, "creation_identity": exact_created}

    def dispatch(self, permit: CapabilityPermit) -> AdapterResult:
        operation = self._operation(permit.request)
        if self.clock() >= permit.expires_monotonic:
            raise CapabilityAdapterError("adapter permit expired before launch")
        boundary = self.boundary_factory()
        key = self._operation_key(permit)
        active = _ActiveOperation(process=None, identity=None, supervisor_identity=None, transport=None, boundary=boundary, operation=operation)
        self._active[key] = active
        try:
            config = self.config_provider(permit.request, operation)
            if not isinstance(config, Mapping):
                raise CapabilityAdapterError("adapter configuration is not an object")
            launch_config = dict(config)
            launch_config["popen_kwargs"] = dict(boundary.popen_kwargs)
            active.launch_attempted = True
            process = self.launcher(launch_config)
            active.process = process
            pid = getattr(process, "pid", None)
            if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
                raise CapabilityAdapterError("adapter child identity has no valid PID")
            exact = self.identity_provider(pid)
            if not isinstance(exact, Mapping) or exact.get("pid") != pid:
                raise CapabilityAdapterError("adapter child creation identity is unavailable")
            process_info = process_snapshot().by_pid.get(pid)
            active.identity = self._child_identity(pid, exact, process_info)
            active.supervisor_identity = process_info or active.identity
            boundary.attach(process, process_info or active.identity)
            remaining = lambda: permit.expires_monotonic - self.clock()
            authority_check = lambda: self.clock() < permit.expires_monotonic
            transport_config = dict(launch_config)
            transport_config["_authority_check"] = authority_check
            active.transport = self.transport_factory(process, remaining, permit.request.request_id, transport_config)

            def send_checked(value: dict[str, Any], label: str) -> None:
                if not authority_check():
                    raise CapabilityAdapterError("adapter permit expired before transport enqueue")
                active.transport.send(value, label)

            send_checked(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": self.mcp_protocol_version,
                        "capabilities": {},
                        "clientInfo": {"name": "orchestrator-harness", "version": "0.1.0"},
                    },
                },
                "initialize",
            )
            active.transport.receive(1)
            send_checked({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}, "initialized")
            send_checked(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {"name": operation.mcp_tool, "arguments": dict(operation.arguments)},
                },
                "dispatch",
            )
            raw = active.transport.receive(2)
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
            # Keep the active operation private so cleanup can account for a
            # child that was created before a later boundary/transport failure.
            raise

    @staticmethod
    def _public_child_cleanup(result: Any) -> dict[str, Any]:
        record = result.to_record() if hasattr(result, "to_record") else {}
        allowed = {
            "status", "stages", "terminate_attempted", "kill_attempted", "final_reap",
            "cleanup_confirmed", "identity_verified", "identity_uncertain", "exit_code",
            "reaped_after", "errors", "owned_boundary_empty", "boundary_complete",
            "boundary_cleanup", "boundary_errors",
        }
        return {key: value for key, value in record.items() if key in allowed}

    @staticmethod
    def _public_io_cleanup(value: Any) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            return {"closed": False, "helpers_stopped": False, "errors": ["invalid transport cleanup"]}
        allowed = {
            "closed", "helpers_stopped", "stderr_sha256", "stderr_log_complete",
            "stderr_log_sha256", "stderr_log_partial_sha256", "stderr_eof",
            "stderr_forced_close", "helper_threads", "stream_close_errors", "error_type",
        }
        return {key: value[key] for key in value if key in allowed}

    @staticmethod
    def _public_boundary_cleanup(value: Any) -> dict[str, Any]:
        """Reduce boundary implementation records to stable public facts.

        ProcessBoundary records also contain OS-specific group/session and
        kernel-handle identity fields.  Those are adapter authority, not
        generic cleanup evidence, and must not cross the public seam.
        """

        if not isinstance(value, Mapping):
            return {"complete": False, "members": [], "live_members": [], "errors": ["invalid boundary cleanup"]}
        allowed = {"kind", "complete", "inventory_source", "cleanup", "errors", "members", "live_members"}
        result = {key: value[key] for key in value if key in allowed}
        result.setdefault("complete", False)
        live_members = result.get("live_members", [])
        # ProcessBoundary also reports historical observations in `members`.
        # The public release fact is the current owned set, so do not let a
        # reaped historical member contradict complete-empty cleanup.
        result["live_members"] = live_members
        result["members"] = live_members
        return result

    def cleanup(
        self,
        permit: CapabilityPermit,
        dispatch_result: AdapterResult | None,
        failure: Mapping[str, Any] | None,
    ) -> CleanupEvidence:
        key = self._operation_key(permit)
        prior = self._cleanup_results.get(key)
        if prior is not None:
            return prior
        active = self._active.pop(key, None)
        if active is None:
            result = CleanupEvidence(
                proved=True,
                boundary={"complete": True, "members": [], "live_members": []},
                identities=(),
                details={"launch_started": False, "dispatch_failed": failure is not None},
            )
            self._cleanup_results[key] = result
            return result

        cleanup_result: Any | None = None
        cleanup_error: str | None = None
        try:
            if active.identity is None:
                if active.launch_attempted:
                    cleanup_error = "child identity was unavailable"
            elif self.supervisor_factory is not None:
                try:
                    supervisor = self.supervisor_factory(active.process, active.identity, permit.request.request_id, active.boundary)
                except TypeError:
                    supervisor = self.supervisor_factory(active.process, active.identity, permit.request.request_id)
                cleanup_result = supervisor.cleanup()
            else:
                supervisor = ProcessSupervisor(
                    active.process,
                    active.supervisor_identity or active.identity,
                    graceful_timeout_seconds=self.graceful_timeout_seconds,
                    force_timeout_seconds=self.force_timeout_seconds,
                    boundary=active.boundary,
                )
                cleanup_result = supervisor.cleanup()
        except Exception as exc:
            cleanup_error = type(exc).__name__

        boundary_record: dict[str, Any] = {"complete": False, "members": [], "live_members": [], "errors": ["boundary evidence unavailable"]}
        try:
            inventory = active.boundary.inventory()
            boundary_record = self._public_boundary_cleanup(active.boundary.to_record(inventory))
        except Exception as exc:
            boundary_record = {"complete": False, "members": [], "live_members": [], "errors": [f"boundary inventory failed: {type(exc).__name__}"]}

        io_record = {"closed": True, "helpers_stopped": True}
        if active.transport is not None:
            try:
                closed = active.transport.close_and_join()
                if isinstance(closed, tuple) and len(closed) == 2:
                    io_record = self._public_io_cleanup(closed[1])
                    io_record["closed"] = bool(closed[0])
                    io_record["helpers_stopped"] = bool(closed[0])
                else:
                    io_record = {"closed": False, "helpers_stopped": False, "errors": ["invalid transport cleanup"]}
            except Exception as exc:
                io_record = {"closed": False, "helpers_stopped": False, "error_type": type(exc).__name__}

        child_record = self._public_child_cleanup(cleanup_result) if cleanup_result is not None else {}
        child_ok = (
            cleanup_error is None
            and (
                (not active.launch_attempted and active.process is None)
                or (cleanup_result is not None and bool(getattr(cleanup_result, "proved_reap", False)))
            )
        )
        boundary_ok = boundary_record.get("complete") is True and not boundary_record.get("live_members") and not boundary_record.get("errors")
        io_ok = io_record.get("closed") is True and io_record.get("helpers_stopped") is True and not io_record.get("error_type") and not io_record.get("errors")
        proved = child_ok and boundary_ok and io_ok
        identities: tuple[dict[str, Any], ...] = () if proved else (() if active.identity is None else (dict(active.identity),))
        details: dict[str, Any] = {
            "launch_started": active.launch_attempted,
            "child_cleanup": child_record,
            "owned_boundary": boundary_record,
            "io_cleanup": io_record,
        }
        if cleanup_error is not None:
            details["cleanup_error"] = cleanup_error
        result = CleanupEvidence(proved=proved, boundary=boundary_record, identities=identities, details=details)
        if proved:
            try:
                active.boundary.close()
            except Exception:
                pass
        else:
            self._retained_boundaries[key] = active.boundary
        self._cleanup_results[key] = result
        return result


HardwareCapabilityAdapter = FirmwareHardwareAdapter


__all__ = ["FirmwareHardwareAdapter", "HardwareCapabilityAdapter"]
