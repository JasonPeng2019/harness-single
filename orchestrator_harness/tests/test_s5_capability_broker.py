from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Callable, Mapping

from orchestrator_harness.capability_broker import (
    APPROVAL_SCHEMA,
    AdapterResult,
    CapabilityAdapterError,
    CapabilityAdapterUnavailable,
    CapabilityApproval,
    CapabilityBroker,
    CapabilityDenied,
    CapabilityError,
    CapabilityPermit,
    CapabilityRequest,
    CapabilitySnapshot,
    CleanupEvidence,
    FakeCapabilityAdapter,
    canonical_json_bytes,
    current_process_identity,
)
from orchestrator_harness.firmware_adapter import FirmwareHardwareAdapter
from orchestrator_harness.firmware_campaign import FirmwareAction, FirmwareCampaignPack
from orchestrator_harness.process_supervisor import CleanupResult, ProcessBoundary, ProcessBoundaryUnsupported, ProcessSupervisor
from orchestrator_harness.processes import process_snapshot
from orchestrator_harness.resource_locks import BOUNDARY_ARMED_STATE, ResourceClaims, claim_filename


OWNER = {"pid": 321, "created_utc": "2026-01-01T00:00:00Z", "creation_identity": "fake:321"}


class _Verifier:
    def verify(self, payload: bytes, signature: str, public_key: str) -> bool:
        return bool(payload and signature and public_key)


class _Claims:
    def __init__(self, root: Path, events: list[str], *, owner: dict[str, Any] = OWNER, arm_failures: list[str] | None = None, release_failures: list[str] | None = None, retention_failures: list[str] | None = None) -> None:
        self.root = root
        self.events = events
        self.owner = dict(owner)
        self._held: dict[str, dict[str, Any]] = {}
        self.retained = False
        self.arm_failures = list(arm_failures or [])
        self.release_failures = list(release_failures or [])
        self.retention_failures = list(retention_failures or [])

    @property
    def held(self) -> list[dict[str, Any]]:
        return [dict(self._held[key]) for key in sorted(self._held)]

    def acquire_all(self, resources: list[str], *, on_wait) -> None:
        self.events.append("claim")
        for resource in sorted(resources):
            self._held[resource] = {
                "schema": "fake-resource-claim/v1",
                "resource": resource,
                "path": str(self.root / f"{resource}.claim"),
                "owner": dict(self.owner),
                "boundary_state": "NOT_ARMED",
                "boundary_may_exist": False,
            }

    def arm_boundary(self) -> list[str]:
        self.events.append("arm")
        if self.arm_failures:
            return list(self.arm_failures)
        for record in self._held.values():
            record["boundary_state"] = BOUNDARY_ARMED_STATE
            record["boundary_may_exist"] = True
        return []

    def retain_boundary(self, *, boundary, identities) -> list[str]:
        self.events.append("retain")
        self.retained = True
        for record in self._held.values():
            record["boundary_state"] = "RETAINED"
            record["boundary_may_exist"] = False
        return list(self.retention_failures)

    def release_all(self) -> list[str]:
        self.events.append("release")
        if self.release_failures:
            return list(self.release_failures)
        self._held.clear()
        return []


class _OrderedAdapter(FakeCapabilityAdapter):
    def __init__(self, events: list[str], **kwargs: Any) -> None:
        super().__init__({"synthetic": ("read",)}, **kwargs)
        self.events = events

    def cleanup(self, permit, dispatch_result, failure):
        self.events.append("cleanup")
        return super().cleanup(permit, dispatch_result, failure)


class _SharedClaims:
    def __init__(self) -> None:
        self.condition = threading.Condition()
        self.owners: dict[str, object] = {}


class _ContendedClaims:
    def __init__(self, shared: _SharedClaims, root: Path, owner: dict[str, Any] = OWNER) -> None:
        self.shared = shared
        self.root = root
        self.owner = dict(owner)
        self.identity = object()
        self._held: dict[str, dict[str, Any]] = {}

    @property
    def held(self) -> list[dict[str, Any]]:
        return [dict(self._held[key]) for key in sorted(self._held)]

    def acquire_all(self, resources: list[str], *, on_wait) -> None:
        for resource in sorted(resources):
            while True:
                with self.shared.condition:
                    if resource not in self.shared.owners:
                        self.shared.owners[resource] = self.identity
                        self._held[resource] = {
                            "schema": "fake-resource-claim/v1",
                            "resource": resource,
                            "path": str(self.root / f"{resource}.claim"),
                            "owner": dict(self.owner),
                            "boundary_state": "NOT_ARMED",
                            "boundary_may_exist": False,
                        }
                        break
                on_wait({"resource": resource, "state": "CONTENDED", "reason": "shared fake owner", "wait_seconds": 0.001, "actionable": False})
                time.sleep(0.001)

    def arm_boundary(self) -> list[str]:
        for record in self._held.values():
            record["boundary_state"] = BOUNDARY_ARMED_STATE
            record["boundary_may_exist"] = True
        return []

    def retain_boundary(self, *, boundary, identities) -> list[str]:
        return []

    def release_all(self) -> list[str]:
        with self.shared.condition:
            for resource in list(self._held):
                if self.shared.owners.get(resource) is self.identity:
                    del self.shared.owners[resource]
            self._held.clear()
            self.shared.condition.notify_all()
        return []


class _GateAdapter(FakeCapabilityAdapter):
    def __init__(self, first_request_id: str) -> None:
        super().__init__({"synthetic": ("read",)})
        self.first_request_id = first_request_id
        self.first_dispatch = threading.Event()
        self.allow_first = threading.Event()

    def dispatch(self, permit):
        if permit.request.request_id == self.first_request_id:
            self.first_dispatch.set()
            self.allow_first.wait(2.0)
        return super().dispatch(permit)


class _Transport:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []
        self.closed = False

    def send(self, value, label) -> None:
        self.messages.append(dict(value))

    def receive(self, request_id: int) -> dict[str, Any]:
        if request_id == 1:
            return {"result": {"status": "ready"}}
        return {"result": {"status": "completed"}}

    def close_and_join(self):
        self.closed = True
        return True, {"helpers_stopped": True}


class _Supervisor:
    def cleanup(self):
        return CleanupResult(
            pid=777,
            expected_created_utc="fake-child",
            status="REAPED",
            final_reap=True,
            cleanup_confirmed=True,
            identity_verified=True,
        )


class _Process:
    pid = 777


class _Boundary:
    """Disposable boundary double with the public ProcessBoundary seam."""

    def __init__(self, *, complete: bool = True, live_members: list[dict[str, Any]] | None = None, errors: list[str] | None = None) -> None:
        self.popen_kwargs: dict[str, Any] = {}
        self.complete = complete
        self.live_members = list(live_members or [])
        self.errors = list(errors or [])
        self.attached = False
        self.closed = False

    def attach(self, process, identity) -> None:
        self.attached = True

    def inventory(self):
        return object()

    def to_record(self, inventory) -> dict[str, Any]:
        return {
            "kind": "fake-boundary",
            "complete": self.complete,
            "inventory_source": "fake",
            "cleanup": "fake",
            "errors": list(self.errors),
            "members": list(self.live_members),
            "live_members": list(self.live_members),
        }

    def close(self) -> None:
        self.closed = True


class _MutableClock:
    def __init__(self, value: float = 10.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class _ExpiryTransport(_Transport):
    def __init__(self, clock: _MutableClock, *, expire_on: str | None = None) -> None:
        super().__init__()
        self.clock = clock
        self.expire_on = expire_on
        self.authority_check: Callable[[], bool] | None = None

    def send(self, value, label) -> None:
        if label == self.expire_on:
            self.clock.value = 100.0
        if self.authority_check is not None and not self.authority_check():
            raise CapabilityAdapterError("fake transport authority expired")
        super().send(value, label)


def _process_identity(pid: int) -> dict[str, Any] | None:
    """The OS-level primitive returns None once the exact child is absent."""
    from harness_common.process_identity import exact_process_identity
    if pid not in process_snapshot().by_pid:
        return None
    return exact_process_identity(pid)


def _campaign_pack() -> FirmwareCampaignPack:
    return FirmwareCampaignPack(
        capability="firmware-acceptance",
        actions={
            "observe": FirmwareAction(
                mcp_tool="get_board_info",
                method_version=1,
                maximum_duration_seconds=40,
                required_arguments=("board_id",),
            ),
            "flash_application": FirmwareAction(
                mcp_tool="flash_application",
                method_version=1,
                maximum_duration_seconds=120,
                required_arguments=("board_id", "artifact"),
            ),
        },
        resources={
            "board:stm-a": {"probe_uid": "probe-a", "target": "stm32l476rg", "profile": "stm32"},
            "board:nrf-a": {"probe_uid": "probe-b", "target": "nrf52840", "profile": "nrf"},
        },
        policy={"policy": "caller-declared-policy-v1", "revision": "caller-supplied-revision"},
    )


def _firmware_snapshot(request: CapabilityRequest, pack: FirmwareCampaignPack) -> CapabilitySnapshot:
    resource = request.resources[0]
    identity = pack.resources[resource]
    return CapabilitySnapshot(
        request_id=request.request_id,
        lane_id=request.lane_id,
        controller_identity=dict(request.controller_identity),
        capability=request.capability,
        action=request.action,
        resources=request.resources,
        snapshot_id="hardware-snapshot-1",
        identity={"target_revision": "fake-target-1", "canonical_resource": resource, "resource_identity": identity},
        resource_identities={resource: identity},
        capabilities={pack.capability: tuple(pack.actions)},
        adapter_identity={"adapter_id": "firmware-hardware", "adapter_version": "v1"},
        observed_monotonic=10.0,
    )


def _firmware_adapter(root: Path, clock: Callable[[], float] | None = None, *, transport=None, boundary_factory=None, pack=None) -> tuple[FirmwareHardwareAdapter, FirmwareCampaignPack, Any]:
    selected_pack = pack or _campaign_pack()
    selected_transport = transport or _Transport()

    def make_transport(process, remaining, request_id, config):
        if hasattr(selected_transport, "authority_check"):
            selected_transport.authority_check = config.get("_authority_check")
        return selected_transport

    adapter = FirmwareHardwareAdapter(
        campaign_pack=selected_pack,
        snapshot_provider=lambda request: _firmware_snapshot(request, selected_pack),
        launcher=lambda config: _Process(),
        identity_provider=lambda pid: {"pid": pid, "created_utc": "fake-child", "creation_identity": "fake-exact"},
        config_provider=lambda request, operation: {"private": "controller-config"},
        transport_factory=make_transport,
        supervisor_factory=lambda process, identity, request_id, boundary: _Supervisor(),
        boundary_factory=boundary_factory or (lambda: _Boundary()),
        clock=clock or (lambda: 10.0),
    )
    return adapter, selected_pack, selected_transport


def _firmware_request_and_approval(adapter: FirmwareHardwareAdapter, pack: FirmwareCampaignPack, *, request_id: str = "hardware-request", resource: str = "board:stm-a", expiry: float = 100.0, action: str = "observe", arguments: dict[str, Any] | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    request = _request(
        request_id=request_id,
        lane_id="lane-firmware",
        capability=pack.capability,
        action=action,
        arguments=arguments if arguments is not None else {"board_id": "stm-a"},
        expiry=expiry,
        resources=[resource],
    )
    parsed = CapabilityRequest.from_record(request, now_monotonic=0.0)
    snapshot = adapter.observe(parsed)
    approval = _approval(parsed, snapshot, approval_id=f"{request_id}-approval")
    operation = pack.resolve(parsed)
    approval["policy"] = {
        "policy": pack.policy,
        "capability": pack.capability,
        "action": action,
        "mcp_tool": operation.mcp_tool,
        "method_version": operation.method_version,
        "maximum_duration_seconds": operation.maximum_duration_seconds,
        "canonical_resource": resource,
        "resource_identity": pack.resources[resource],
    }
    return request, approval


def _request(*, request_id: str = "request-1", arguments: dict[str, Any] | None = None, expiry: float = 100.0, resources: list[str] | None = None, lane_id: str = "lane-a", capability: str = "synthetic", action: str = "read") -> dict[str, Any]:
    return {
        "schema": "orchestrator-capability-request/v1",
        "request_id": request_id,
        "lane_id": lane_id,
        "controller_identity": dict(OWNER),
        "capability": capability,
        "action": action,
        "arguments": dict(arguments or {"value": "safe"}),
        "resources": list(resources or ["resource-a"]),
        "expires_monotonic": expiry,
        "route": "capability",
    }


def _approval(request: CapabilityRequest, snapshot, *, approval_id: str = "approval-1", expires: float = 90.0, snapshot_sha256: str | None = None) -> dict[str, Any]:
    value = {
        "schema": APPROVAL_SCHEMA,
        "approval_id": approval_id,
        "request_id": request.request_id,
        "request_sha256": request.sha256,
        "lane_id": request.lane_id,
        "controller_identity": dict(request.controller_identity),
        "capability": request.capability,
        "action": request.action,
        "arguments": dict(request.arguments),
        "resources": list(request.resources),
        "snapshot_id": snapshot.snapshot_id,
        "snapshot_sha256": snapshot_sha256 or snapshot.sha256,
        "policy": {"policy": "synthetic-v1"},
        "issued_monotonic": 1.0,
        "expires_monotonic": expires,
        "decision": "approve",
        "public_key": "fake-public-key",
        "signature": "fake-signature",
    }
    return value


def _approval_for_native_claim(request: CapabilityRequest, adapter: FakeCapabilityAdapter) -> dict[str, Any]:
    return _approval(request, adapter.observe(request), approval_id="native-approval")


def _broker(adapter, claims, *, now: float = 10.0, events: list[str] | None = None, state_root: Path | None = None) -> CapabilityBroker:
    return CapabilityBroker(
        adapter,
        approval_verifier=_Verifier(),
        policy_verifier=lambda request, snapshot, approval: True,
        identity_provider=lambda: dict(OWNER),
        claims_factory=lambda lane, request_id: claims,
        state_root=state_root,
        clock=lambda: now,
        evidence_sink=(lambda record: events.append(f"published:{record['state']}") if events is not None else None),
    )


class S5CapabilityBrokerTests(unittest.TestCase):
    def _approval_for(self, request_value: dict[str, Any], adapter: FakeCapabilityAdapter) -> dict[str, Any]:
        request = CapabilityRequest.from_record(request_value, now_monotonic=0.0)
        snapshot = adapter.observe(request)
        return _approval(request, snapshot)

    def test_S5_F1_generic_success_binds_exact_facts_and_releases_after_cleanup(self) -> None:
        with TemporaryDirectory() as temporary:
            events: list[str] = []
            adapter = _OrderedAdapter(events)
            claims = _Claims(Path(temporary), events)
            request = _request()
            result = _broker(adapter, claims, events=events).execute(request, self._approval_for(request, adapter))

            self.assertEqual("PASS", result.outcome)
            self.assertEqual("TERMINAL", result.state)
            self.assertEqual(1, adapter.dispatch_calls)
            self.assertEqual(1, adapter.cleanup_calls)
            self.assertTrue(result.record["claims_released"])
            self.assertEqual(["claim", "arm", "cleanup"], events[:3])
            self.assertEqual("release", events[3])
            self.assertIn("published:TERMINAL", events)
            permit = adapter.permits[0]
            self.assertNotIn("endpoint", str(permit).lower())
            self.assertEqual(result.record["permit_sha256"], permit["permit_sha256"])
            self.assertEqual(result.record["raw_result_sha256"], hashlib.sha256(b'{"request_id":"request-1","status":"completed"}').hexdigest())


    def test_S5_F1_changed_expired_and_unbound_approval_never_dispatches(self) -> None:
        for name in ("changed", "expired", "unbound"):
            with self.subTest(name=name), TemporaryDirectory() as temporary:
                adapter = FakeCapabilityAdapter({"synthetic": ("read",)})
                claims = _Claims(Path(temporary), [])
                request = _request()
                approval = self._approval_for(request, adapter)
                if name == "changed":
                    supplied = _request(arguments={"value": "changed"})
                else:
                    supplied = request
                if name == "expired":
                    result = CapabilityBroker(
                        adapter,
                        approval_verifier=_Verifier(),
                        policy_verifier=lambda *_: True,
                        identity_provider=lambda: dict(OWNER),
                        claims_factory=lambda *_: claims,
                        clock=lambda: 95.0,
                    ).execute(supplied, approval)
                else:
                    if name == "unbound":
                        approval["snapshot_sha256"] = "0" * 64
                    result = _broker(adapter, claims).execute(supplied, approval)
                self.assertEqual("DENIED", result.outcome)
                self.assertEqual(0, adapter.dispatch_calls)
                self.assertEqual(0, len(adapter.permits))
                self.assertFalse(claims.held)


    def test_S5_F2_closed_request_rejects_endpoint_material_before_observation(self) -> None:
        adapter = FakeCapabilityAdapter({"synthetic": ("read",)})
        value = _request()
        value["arguments"] = {"endpoint": "controller-private"}
        result = _broker(adapter, _Claims(Path("."), [])).execute(value, {})
        self.assertEqual("DENIED", result.outcome)
        self.assertEqual(0, adapter.support_calls)
        self.assertEqual(0, adapter.observe_calls)
        self.assertNotIn("endpoint", str(result.to_record()).lower())


    def test_S5_F5_dispatch_failure_is_terminal_failure_and_cleanup_precedes_release(self) -> None:
        with TemporaryDirectory() as temporary:
            events: list[str] = []
            adapter = _OrderedAdapter(events, fail_dispatch=True)
            claims = _Claims(Path(temporary), events)
            request = _request()
            result = _broker(adapter, claims).execute(request, self._approval_for(request, adapter))
            self.assertEqual("FAIL", result.outcome)
            self.assertEqual(1, result.record["dispatch_count"])
            self.assertTrue(result.record["claims_released"])
            self.assertEqual(["claim", "arm", "cleanup"], events[:3])
            self.assertEqual("release", events[3])


    def test_S5_F5_unproved_cleanup_retains_exact_claim_and_is_uncertain(self) -> None:
        with TemporaryDirectory() as temporary:
            adapter = FakeCapabilityAdapter({"synthetic": ("read",)}, cleanup_proved=False)
            claims = _Claims(Path(temporary), [])
            request = _request()
            result = _broker(adapter, claims).execute(request, self._approval_for(request, adapter))
            self.assertEqual("UNCERTAIN", result.outcome)
            self.assertFalse(result.record["claims_released"])
            self.assertTrue(claims.retained)
            self.assertTrue(claims.held)


    def test_S5_F3_same_resource_serializes_and_disjoint_resource_progresses(self) -> None:
        with TemporaryDirectory() as temporary:
            shared = _SharedClaims()
            adapter = _GateAdapter("request-1")

            def make_broker() -> CapabilityBroker:
                return CapabilityBroker(
                    adapter,
                    approval_verifier=_Verifier(),
                    policy_verifier=lambda *_: True,
                    identity_provider=lambda: dict(OWNER),
                    claims_factory=lambda lane, request_id: _ContendedClaims(shared, Path(temporary)),
                    clock=lambda: 10.0,
                )

            first = _request(request_id="request-1", resources=["resource-a"])
            second_same = _request(request_id="request-2", resources=["resource-a"], lane_id="lane-b")
            first_result: dict[str, Any] = {}
            second_result: dict[str, Any] = {}
            first_broker = make_broker()
            second_broker = make_broker()
            first_approval = self._approval_for(first, adapter)
            second_approval = self._approval_for(second_same, adapter)
            thread_one = threading.Thread(target=lambda: first_result.setdefault("value", first_broker.execute(first, first_approval)), daemon=True)
            thread_one.start()
            self.assertTrue(adapter.first_dispatch.wait(1.0))
            thread_two = threading.Thread(target=lambda: second_result.setdefault("value", second_broker.execute(second_same, second_approval)), daemon=True)
            thread_two.start()
            time.sleep(0.05)
            self.assertNotIn("value", second_result)
            adapter.allow_first.set()
            thread_one.join(1.0)
            thread_two.join(1.0)
            self.assertEqual("PASS", first_result["value"].outcome)
            self.assertEqual("PASS", second_result["value"].outcome)

            shared = _SharedClaims()
            adapter = _GateAdapter("request-3")
            first = _request(request_id="request-3", resources=["resource-a"])
            disjoint = _request(request_id="request-4", resources=["resource-b"], lane_id="lane-b")
            first_result.clear()
            second_result.clear()

            def make_broker_for(adapter_value: FakeCapabilityAdapter) -> CapabilityBroker:
                return CapabilityBroker(
                    adapter_value,
                    approval_verifier=_Verifier(),
                    policy_verifier=lambda *_: True,
                    identity_provider=lambda: dict(OWNER),
                    claims_factory=lambda lane, request_id: _ContendedClaims(shared, Path(temporary)),
                    clock=lambda: 10.0,
                )

            first_broker = make_broker_for(adapter)
            second_broker = make_broker_for(adapter)
            thread_one = threading.Thread(target=lambda: first_result.setdefault("value", first_broker.execute(first, self._approval_for(first, adapter))), daemon=True)
            thread_one.start()
            self.assertTrue(adapter.first_dispatch.wait(1.0))
            thread_two = threading.Thread(target=lambda: second_result.setdefault("value", second_broker.execute(disjoint, self._approval_for(disjoint, adapter))), daemon=True)
            thread_two.start()
            thread_two.join(1.0)
            self.assertEqual("PASS", second_result["value"].outcome)
            self.assertNotIn("value", first_result)
            adapter.allow_first.set()
            thread_one.join(1.0)
            self.assertEqual("PASS", first_result["value"].outcome)


    def test_S5_F4_uncertain_resource_evidence_denies_without_dispatch(self) -> None:
        class UncertainClaims:
            held: list[dict[str, Any]] = []

            def acquire_all(self, resources, *, on_wait) -> None:
                on_wait({"resource": resources[0], "state": "INVENTORY_UNKNOWN", "reason": "fake incomplete inventory"})

            def release_all(self) -> list[str]:
                return []

        adapter = FakeCapabilityAdapter({"synthetic": ("read",)})
        request = _request()
        result = _broker(adapter, UncertainClaims()).execute(request, self._approval_for(request, adapter))
        self.assertEqual("DENIED", result.outcome)
        self.assertEqual("RESOURCE_UNCERTAIN", result.record["denial"]["reason_code"])
        self.assertEqual(0, adapter.dispatch_calls)


    def test_S5_F4_broker_consumes_native_resource_claim_owner_and_releases_it(self) -> None:
        owner = current_process_identity()
        self.assertIsNotNone(owner)
        assert owner is not None
        adapter = FakeCapabilityAdapter({"synthetic": ("read",)})
        request = _request()
        request["controller_identity"] = dict(owner)
        parsed = CapabilityRequest.from_record(request, now_monotonic=0.0)
        approval = _approval_for_native_claim(parsed, adapter)
        with TemporaryDirectory() as temporary:
            result = CapabilityBroker(
                adapter,
                approval_verifier=_Verifier(),
                policy_verifier=lambda *_: True,
                identity_provider=current_process_identity,
                claims_root=Path(temporary) / "claims",
                clock=lambda: 10.0,
            ).execute(request, approval)
        self.assertEqual("PASS", result.outcome)
        self.assertEqual(1, adapter.dispatch_calls)
        self.assertTrue(result.record["claims_released"])


    def test_S5_F6_legacy_and_coding_records_are_not_mixed_into_generic_route(self) -> None:
        adapter = FakeCapabilityAdapter({"synthetic": ("read",)})
        claims = _Claims(Path("."), [])
        records = [
            {"action": "start", "run_root": "legacy", "prompt_path": "prompt"},
            {"schema": "orchestrator-coding-invocation/v1", "action": "start", "lane_id": "coding-a"},
            {**_request(), "route": "coding"},
        ]
        for record in records:
            result = _broker(adapter, claims).execute(record, {})
            self.assertEqual("DENIED", result.outcome)
        self.assertEqual(0, adapter.support_calls)
        self.assertEqual(0, adapter.observe_calls)


    def test_S5_F8_exact_retry_reuses_terminal_result_and_changed_replay_denies(self) -> None:
        with TemporaryDirectory() as temporary:
            adapter = FakeCapabilityAdapter({"synthetic": ("read",)})
            claims = _Claims(Path(temporary), [])
            broker = _broker(adapter, claims)
            request = _request()
            approval = self._approval_for(request, adapter)
            first = broker.execute(request, approval)
            second = broker.execute(copy.deepcopy(request), copy.deepcopy(approval))
            changed = dict(request)
            changed["arguments"] = {"value": "new-semantics"}
            third = broker.execute(changed, approval)
            self.assertEqual(first.to_record(), second.to_record())
            self.assertEqual(1, adapter.dispatch_calls)
            self.assertEqual("DENIED", third.outcome)
            self.assertEqual("REPLAY_MISMATCH", third.record["denial"]["reason_code"])


    def test_S5_R1_001_native_claim_is_armed_before_dispatch_and_retained_after_uncertainty(self) -> None:
        class ArmObservingAdapter(FakeCapabilityAdapter):
            def __init__(self, claim_root: Path) -> None:
                super().__init__({"synthetic": ("read",)}, cleanup_proved=False)
                self.claim_root = claim_root
                self.observed_state: str | None = None

            def dispatch(self, permit):
                claim = json.loads((self.claim_root / claim_filename("resource-a")).read_text(encoding="utf-8"))
                self.observed_state = claim.get("boundary_state")
                return super().dispatch(permit)

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            owner = current_process_identity()
            self.assertIsNotNone(owner)
            request = _request()
            request["controller_identity"] = dict(owner or {})
            parsed = CapabilityRequest.from_record(request, now_monotonic=0.0)
            adapter = ArmObservingAdapter(root / "claims")
            approval = _approval(parsed, adapter.observe(parsed))
            result = CapabilityBroker(
                adapter,
                approval_verifier=_Verifier(),
                policy_verifier=lambda *_: True,
                identity_provider=current_process_identity,
                claims_root=root / "claims",
                clock=lambda: 10.0,
            ).execute(request, approval)
            self.assertEqual("UNCERTAIN", result.outcome, result.to_record())
            self.assertEqual(BOUNDARY_ARMED_STATE, adapter.observed_state)
            retained = json.loads((root / "claims" / claim_filename("resource-a")).read_text(encoding="utf-8"))
            self.assertEqual("RETAINED", retained["boundary_state"])
            self.assertIsInstance(retained.get("retained_boundary"), dict)
            self.assertFalse(result.record["claims_released"])


    def test_S5_R1_001_arm_failure_denies_before_any_adapter_launch(self) -> None:
        with TemporaryDirectory() as temporary:
            events: list[str] = []
            adapter = _OrderedAdapter(events)
            claims = _Claims(Path(temporary), events, arm_failures=["resource-a"])
            request = _request()
            result = _broker(adapter, claims).execute(request, self._approval_for(request, adapter))
            self.assertEqual("DENIED", result.outcome)
            self.assertEqual("CLAIM_ARM_FAILED", result.record["denial"]["reason_code"])
            self.assertEqual(0, adapter.dispatch_calls)
            self.assertEqual(["claim", "arm", "retain"], events)
            self.assertTrue(claims.held)


    def test_S5_R1_003_default_boundary_tracks_descendant_and_proves_empty_cleanup(self) -> None:
        base_python = getattr(sys, "_base_executable", sys.executable)
        script = "import subprocess,sys,time; subprocess.Popen([getattr(sys,'_base_executable',sys.executable),'-c','import time;time.sleep(30)']); time.sleep(5)"
        process = None
        boundary = None
        try:
            try:
                boundary = ProcessBoundary.prepare()
            except ProcessBoundaryUnsupported as exc:
                self.skipTest(f"native process boundary unavailable: {exc}")
            process = subprocess.Popen([base_python, "-c", script], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **boundary.popen_kwargs)
            identity = None
            for _ in range(30):
                identity = process_snapshot().by_pid.get(process.pid)
                if identity is not None:
                    break
                time.sleep(0.02)
            if identity is None:
                self.skipTest("native process identity was not observable")
            boundary.attach(process, identity)
            time.sleep(0.1)
            observed = boundary.inventory()
            self.assertTrue(observed.complete)
            self.assertTrue(observed.processes)
            cleanup = ProcessSupervisor(
                process,
                identity,
                graceful_timeout_seconds=0.2,
                force_timeout_seconds=0.5,
                boundary=boundary,
            ).cleanup()
            self.assertTrue(cleanup.proved_reap, cleanup.to_record())
            final = boundary.inventory()
            self.assertTrue(final.complete)
            self.assertFalse(final.processes)
        finally:
            if process is not None:
                try:
                    if process.poll() is None:
                        process.kill()
                    process.wait(timeout=1)
                except (OSError, subprocess.TimeoutExpired):
                    pass
            if boundary is not None:
                boundary.close()


    def test_S5_R1_003_boundary_contradiction_retains_claim_even_when_adapter_says_proved(self) -> None:
        with TemporaryDirectory() as temporary:
            events: list[str] = []

            class BoundaryContradictionAdapter(FakeCapabilityAdapter):
                def cleanup(self, permit, dispatch_result, failure):
                    return CleanupEvidence(
                        proved=True,
                        boundary={"complete": True, "members": [{"pid": 7}], "live_members": [{"pid": 7}]},
                        identities=(),
                        details={"launch_started": True},
                    )

            adapter = BoundaryContradictionAdapter({"synthetic": ("read",)})
            claims = _Claims(Path(temporary), events)
            request = _request()
            result = _broker(adapter, claims).execute(request, self._approval_for(request, adapter))
            self.assertEqual("UNCERTAIN", result.outcome)
            self.assertFalse(result.record["claims_released"])
            self.assertIn("cleanup boundary reports live_members", result.record["cleanup_validation"]["reasons"])
            self.assertNotIn("release", events)


    def test_S5_R1_004_fresh_broker_reuses_durable_terminal_and_rejects_replays(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_root = root / "state"
            adapter = FakeCapabilityAdapter({"synthetic": ("read",)})
            request = _request(request_id="durable-request")
            approval = self._approval_for(request, adapter)
            first_claims = _Claims(root / "claims-1", [])
            first = _broker(adapter, first_claims, state_root=state_root).execute(request, approval)
            self.assertEqual("PASS", first.outcome)
            second_claims = _Claims(root / "claims-2", [])
            second = _broker(adapter, second_claims, state_root=state_root).execute(copy.deepcopy(request), copy.deepcopy(approval))
            self.assertEqual(first.to_record(), second.to_record())
            self.assertEqual(1, adapter.dispatch_calls)
            self.assertFalse(second_claims.held)

            changed_request = copy.deepcopy(request)
            changed_request["arguments"] = {"value": "changed"}
            changed = _broker(adapter, _Claims(root / "claims-3", []), state_root=state_root).execute(changed_request, approval)
            self.assertEqual("DENIED", changed.outcome)
            self.assertEqual("REPLAY_MISMATCH", changed.record["denial"]["reason_code"])
            changed_approval = copy.deepcopy(approval)
            changed_approval["signature"] = "different-signature"
            approval_replay = _broker(adapter, _Claims(root / "claims-4", []), state_root=state_root).execute(request, changed_approval)
            self.assertEqual("DENIED", approval_replay.outcome)
            self.assertEqual("APPROVAL_REPLAY", approval_replay.record["denial"]["reason_code"])
            self.assertEqual(1, adapter.dispatch_calls)


    def test_S5_R1_005_cleanup_contradictions_and_retention_failures_never_release(self) -> None:
        variants = (
            {"complete": False, "members": [], "live_members": []},
            {"complete": True, "members": [], "live_members": []},
            {"complete": True, "members": [], "live_members": [], "errors": ["boundary"]},
            {"complete": True, "members": [{"pid": 9}], "live_members": []},
        )
        for index, boundary in enumerate(variants):
            with self.subTest(index=index), TemporaryDirectory() as temporary:
                class VariantAdapter(FakeCapabilityAdapter):
                    def cleanup(self, permit, dispatch_result, failure):
                        details = {"launch_started": True}
                        if index == 1:
                            details["io_cleanup"] = {"closed": False, "helpers_stopped": False, "errors": ["transport helper"]}
                        identities = ({"pid": 999, "created_utc": "fake", "creation_identity": "fake"},) if index == 2 else ()
                        return CleanupEvidence(proved=True, boundary=boundary, identities=identities, details=details)

                events: list[str] = []
                adapter = VariantAdapter({"synthetic": ("read",)})
                claims = _Claims(Path(temporary), events, retention_failures=["retention-proof-failed"])
                request = _request(request_id=f"cleanup-{index}")
                result = _broker(adapter, claims).execute(request, self._approval_for(request, adapter))
                self.assertEqual("UNCERTAIN", result.outcome)
                self.assertFalse(result.record["claims_released"])
                self.assertEqual(("retention-proof-failed",), result.record["retention_failures"])
                self.assertNotIn("release", events)


    def test_S5_R1_006_recursive_public_authority_aliases_are_rejected(self) -> None:
        aliases = (
            "endpoint", "mcp_server", "server", "connection", "server_config", "connection_token",
            "transport", "session", "handle", "credential", "api_key", "API-Key", "ApiKey", "apikey",
            "authorization", "Authorization", "bearer", "Bearer", "private_key", "private-key", "PrivateKey",
        )
        for alias in aliases:
            with self.subTest(alias=alias), TemporaryDirectory() as temporary:
                adapter = FakeCapabilityAdapter({"synthetic": ("read",)})
                request = _request(arguments={alias: {"nested": "private"}})
                result = _broker(adapter, _Claims(Path(temporary), [])).execute(request, {})
                self.assertEqual("DENIED", result.outcome)
                self.assertEqual(0, adapter.support_calls)
                self.assertNotIn(alias, json.dumps(result.to_record(), sort_keys=True).lower())
                with self.assertRaises(CapabilityError):
                    canonical_json_bytes({"outer": [{alias: "private"}]})

        class AliasSnapshotAdapter(FakeCapabilityAdapter):
            def __init__(self, alias: str) -> None:
                super().__init__({"synthetic": ("read",)})
                self.alias = alias

            def observe(self, request):
                base = super().observe(request)
                return CapabilitySnapshot(
                    request_id=base.request_id,
                    lane_id=base.lane_id,
                    controller_identity=dict(base.controller_identity),
                    capability=base.capability,
                    action=base.action,
                    resources=base.resources,
                    snapshot_id=base.snapshot_id,
                    identity={"nested": [{self.alias: "private"}]},
                    resource_identities=dict(base.resource_identities),
                    capabilities=dict(base.capabilities),
                    adapter_identity=dict(base.adapter_identity),
                    observed_monotonic=base.observed_monotonic,
                )

        for alias in aliases:
            with self.subTest(snapshot_fact=alias), TemporaryDirectory() as temporary:
                adapter = AliasSnapshotAdapter(alias)
                result = _broker(adapter, _Claims(Path(temporary), [])).execute(_request(), {})
                self.assertEqual("DENIED", result.outcome)
                self.assertEqual("SNAPSHOT_INVALID", result.record["denial"]["reason_code"])

            with self.subTest(public_fact=alias):
                with self.assertRaises(CapabilityError):
                    AdapterResult(True, {"nested": [{alias: "private"}]}, {"status": "PASS"}, {"adapter_id": "fake", "adapter_version": "v1"})
                with self.assertRaises(CapabilityError):
                    AdapterResult(True, {"status": "completed"}, {"nested": {alias: "private"}}, {"adapter_id": "fake", "adapter_version": "v1"})
                with self.assertRaises(CapabilityError):
                    CleanupEvidence(True, {"complete": True, "members": [], "live_members": []}, (), {"nested": {alias: "private"}})
                with self.assertRaises(CapabilityError):
                    CapabilityDenied("TEST", "private fact", stage="test", details={"nested": {alias: "private"}})

        parsed = CapabilityRequest.from_record(_request(), now_monotonic=0.0)
        snapshot = FakeCapabilityAdapter({"synthetic": ("read",)}).observe(parsed)
        for alias in aliases:
            with self.subTest(approval_fact=alias):
                approval = _approval(parsed, snapshot)
                approval["arguments"] = {alias: [{"nested": "private"}]}
                with self.assertRaises(CapabilityError):
                    CapabilityApproval.from_record(approval)

        public_approval = CapabilityApproval.from_record(_approval(parsed, snapshot))
        self.assertTrue(_Verifier().verify(public_approval.signed_payload, public_approval.signature, public_approval.public_key))
        self.assertEqual(public_approval.to_record(), json.loads(public_approval.signed_payload.decode("utf-8")) | {"signature": public_approval.signature})
        self.assertEqual(
            canonical_json_bytes({"keyboard": "ordinary", "monkey": "ordinary", "ordinary_key": "ordinary", "public_key": "typed-public"}),
            canonical_json_bytes({"public_key": "typed-public", "ordinary_key": "ordinary", "monkey": "ordinary", "keyboard": "ordinary"}),
        )


    def test_S5_R1_009_ordinary_preclaim_failures_are_typed_published_denials(self) -> None:
        cases: list[tuple[str, Any, Any]] = []

        class SupportsRaises(FakeCapabilityAdapter):
            def supports(self, request):
                raise RuntimeError("supports exploded")

        class ObserveRaises(FakeCapabilityAdapter):
            def observe(self, request):
                raise RuntimeError("observe exploded")

        cases.append(("supports", SupportsRaises({"synthetic": ("read",)}), lambda request, snapshot, approval: True))
        cases.append(("observe", ObserveRaises({"synthetic": ("read",)}), lambda request, snapshot, approval: True))
        for name, adapter, policy in cases:
            with self.subTest(name=name), TemporaryDirectory() as temporary:
                published: list[dict[str, Any]] = []
                result = CapabilityBroker(
                    adapter,
                    approval_verifier=_Verifier(),
                    policy_verifier=policy,
                    identity_provider=lambda: dict(OWNER),
                    claims_factory=lambda *_: _Claims(Path(temporary), []),
                    clock=lambda: 10.0,
                    evidence_sink=published.append,
                ).execute(_request(), {})
                self.assertEqual("DENIED", result.outcome)
                self.assertEqual(0, result.record["dispatch_count"])
                self.assertTrue(published)

        adapter = FakeCapabilityAdapter({"synthetic": ("read",)})
        request = _request()
        approval = self._approval_for(request, adapter)
        published: list[dict[str, Any]] = []
        result = CapabilityBroker(
            adapter,
            approval_verifier=lambda *_: (_ for _ in ()).throw(RuntimeError("signature exploded")),
            policy_verifier=lambda *_: True,
            identity_provider=lambda: dict(OWNER),
            claims_factory=lambda *_: _Claims(Path("."), []),
            clock=lambda: 10.0,
            evidence_sink=published.append,
        ).execute(request, approval)
        self.assertEqual("DENIED", result.outcome)
        self.assertEqual("APPROVAL_SIGNATURE_INVALID", result.record["denial"]["reason_code"])
        self.assertEqual(0, result.record["dispatch_count"])
        self.assertTrue(published)

        policy_adapter = FakeCapabilityAdapter({"synthetic": ("read",)})
        policy_request = _request(request_id="policy-callback")
        policy_approval = self._approval_for(policy_request, policy_adapter)
        policy_result = CapabilityBroker(
            policy_adapter,
            approval_verifier=_Verifier(),
            policy_verifier=lambda *_: (_ for _ in ()).throw(RuntimeError("policy exploded")),
            identity_provider=lambda: dict(OWNER),
            claims_factory=lambda *_: _Claims(Path("."), []),
            clock=lambda: 10.0,
        ).execute(policy_request, policy_approval)
        self.assertEqual("DENIED", policy_result.outcome)
        self.assertEqual("APPROVAL_POLICY_UNAVAILABLE", policy_result.record["denial"]["reason_code"])
        self.assertEqual(0, policy_result.record["dispatch_count"])

        malformed = self._approval_for(_request(request_id="approval-parse"), adapter)
        malformed["policy"] = ["not-an-object"]
        parse_result = _broker(adapter, _Claims(Path("."), [])).execute(_request(request_id="approval-parse"), malformed)
        self.assertEqual("DENIED", parse_result.outcome)
        self.assertEqual("ADMISSION_FAILED", parse_result.record["denial"]["reason_code"])
        self.assertEqual(0, parse_result.record["dispatch_count"])


    def test_S5_R1_010_malicious_adapter_cannot_mutate_nested_permit_or_hash(self) -> None:
        class MutatingAdapter(FakeCapabilityAdapter):
            def __init__(self):
                super().__init__({"synthetic": ("read",)})
                self.mutation_blocked = False
                self.original_hash = ""

            def dispatch(self, permit):
                self.original_hash = permit.permit_sha256
                try:
                    permit.request.arguments["value"] = "mutated"
                except TypeError:
                    self.mutation_blocked = True
                copied = permit.to_record()
                copied["request"]["arguments"]["value"] = "mutated-copy"
                self.mutation_blocked = self.mutation_blocked and permit.request.arguments["value"] == "safe"
                return super().dispatch(permit)

        adapter = MutatingAdapter()
        request = _request()
        result = _broker(adapter, _Claims(Path("."), [])).execute(request, self._approval_for(request, adapter))
        self.assertEqual("PASS", result.outcome)
        self.assertTrue(adapter.mutation_blocked)
        self.assertEqual(adapter.original_hash, adapter.permits[0]["permit_sha256"])


if __name__ == "__main__":
    unittest.main()

    def test_S5_R1_003_default_adapter_boundary_releases_after_disposable_process_cleanup(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            pack = _campaign_pack()
            try:
                adapter = FirmwareHardwareAdapter(
                    campaign_pack=pack,
                    snapshot_provider=lambda request: _firmware_snapshot(request, pack),
                    launcher=lambda config: subprocess.Popen(
                        [sys.executable, "-c", "import time; time.sleep(30)"],
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        **config["popen_kwargs"],
                    ),
                    identity_provider=_process_identity,
                    config_provider=lambda request, operation: {},
                    transport_factory=lambda process, remaining, request_id, config: _Transport(),
                    clock=lambda: 10.0,
                )
            except ProcessBoundaryUnsupported as exc:
                self.skipTest(f"native process boundary unavailable: {exc}")
            request, approval = _firmware_request_and_approval(adapter, pack, request_id="default-adapter-boundary")
            claims = _Claims(root, [])
            result = CapabilityBroker(
                adapter,
                approval_verifier=_Verifier(),
                policy_verifier=adapter.verify_approval,
                identity_provider=lambda: dict(OWNER),
                claims_factory=lambda *_: claims,
                clock=lambda: 10.0,
            ).execute(request, approval)
            self.assertEqual("PASS", result.outcome, result.to_record())
            self.assertTrue(result.record["claims_released"])
            self.assertNotIn("session_id", json.dumps(result.to_record(), sort_keys=True))

    def test_S5_R1_004_adapter_cleanup_cache_is_keyed_by_request_and_permit_digest(self) -> None:
        with TemporaryDirectory() as temporary:
            adapter, pack, _ = _firmware_adapter(Path(temporary))
            request_value, approval_value = _firmware_request_and_approval(adapter, pack, request_id="cache-request")
            request = CapabilityRequest.from_record(request_value, now_monotonic=0.0)
            snapshot = adapter.observe(request)
            approval = CapabilityApproval.from_record(approval_value)
            permit_a = CapabilityPermit(request, snapshot, approval, (), dict(OWNER), dict(adapter.adapter_identity), 40.0, "a" * 64)
            permit_b = CapabilityPermit(request, snapshot, approval, (), dict(OWNER), dict(adapter.adapter_identity), 40.0, "b" * 64)
            first = adapter.cleanup(permit_a, None, {"reason_code": "failed"})
            second = adapter.cleanup(permit_b, None, {"reason_code": "failed"})
            self.assertIsNot(first, second)
            self.assertEqual(2, len(adapter._cleanup_results))

    def test_S5_R1_008_expiry_is_checked_before_enqueue_and_at_writer_effect(self) -> None:
        for mode in ("before-enqueue", "dispatch"):
            with self.subTest(mode=mode), TemporaryDirectory() as temporary:
                clock = _MutableClock()
                transport = _ExpiryTransport(clock, expire_on="dispatch" if mode == "dispatch" else None)

                def boundary_factory():
                    if mode == "before-enqueue":
                        clock.value = 100.0
                    return _Boundary()

                adapter, pack, _ = _firmware_adapter(Path(temporary), clock, transport=transport, boundary_factory=boundary_factory)
                request, approval = _firmware_request_and_approval(adapter, pack, request_id=f"expiry-{mode}")
                claims = _Claims(Path(temporary), [])
                result = CapabilityBroker(
                    adapter,
                    approval_verifier=_Verifier(),
                    policy_verifier=adapter.verify_approval,
                    identity_provider=lambda: dict(OWNER),
                    claims_factory=lambda *_: claims,
                    clock=clock,
                ).execute(request, approval)
                self.assertEqual("FAIL", result.outcome, result.to_record())
                self.assertFalse(any(message.get("params", {}).get("name") == "get_board_info" for message in transport.messages))
                self.assertTrue(result.record["claims_released"])

    def test_S5_R2_001_caller_declared_pack_maps_actions_and_keeps_lane_and_resource_independent(self) -> None:
        pack = _campaign_pack()
        record = pack.as_record()
        self.assertEqual("firmware-acceptance", record["capability"])
        self.assertEqual({"observe", "flash_application"}, set(record["actions"]))
        self.assertEqual("caller-supplied-revision", record["policy"]["revision"])

        for lane_id in ("lane-a", "lane-b"):
            request = CapabilityRequest.from_record(
                _request(request_id=f"pack-{lane_id}", lane_id=lane_id, capability="firmware-acceptance", action="observe", arguments={"board_id": "stm-a"}, resources=["board:stm-a"]),
                now_monotonic=0.0,
            )
            operation = pack.resolve(request)
            self.assertEqual("board:stm-a", operation.canonical_resource)
            self.assertEqual({"board_id": "stm-a"}, operation.arguments)
            self.assertEqual("get_board_info", operation.mcp_tool)
            self.assertEqual(1, operation.method_version)
            self.assertEqual(40, operation.maximum_duration_seconds)
            self.assertEqual(pack.resources["board:stm-a"], operation.resource_identity)

        extra = CapabilityRequest.from_record(
            _request(request_id="pack-extra", lane_id="lane-a", capability="firmware-acceptance", action="observe", arguments={"board_id": "stm-a", "extra": True}, resources=["board:stm-a"]),
            now_monotonic=0.0,
        )
        with self.assertRaises(CapabilityAdapterUnavailable):
            pack.resolve(extra)
        missing = CapabilityRequest.from_record(
            _request(request_id="pack-missing", lane_id="lane-a", capability="firmware-acceptance", action="flash_application", arguments={"board_id": "stm-a"}, resources=["board:stm-a"]),
            now_monotonic=0.0,
        )
        with self.assertRaises(CapabilityAdapterUnavailable):
            pack.resolve(missing)

        for resources in (["board:stm-a", "board:nrf-a"], ["undeclared"]):
            request = CapabilityRequest.from_record(
                _request(request_id="pack-resource", lane_id="lane-a", capability="firmware-acceptance", action="observe", arguments={"board_id": "stm-a"}, resources=resources),
                now_monotonic=0.0,
            )
            with self.assertRaises(CapabilityAdapterUnavailable):
                pack.resolve(request)

    def test_S5_R2_002_exact_mcp_traffic_policy_duration_binding_and_private_config_stays_private(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            pack = _campaign_pack()
            transport = _Transport()
            adapter = FirmwareHardwareAdapter(
                campaign_pack=pack,
                snapshot_provider=lambda request: _firmware_snapshot(request, pack),
                launcher=lambda config: _Process(),
                identity_provider=lambda pid: {"pid": pid, "created_utc": "fake-child", "creation_identity": "fake-exact"},
                config_provider=lambda request, operation: {"private": "controller-config", "endpoint": "server-private"},
                transport_factory=lambda process, remaining, request_id, config: transport,
                supervisor_factory=lambda process, identity, request_id, boundary: _Supervisor(),
                boundary_factory=lambda: _Boundary(),
                mcp_protocol_version="2025-03-26",
                clock=lambda: 10.0,
            )
            request, approval = _firmware_request_and_approval(adapter, pack, request_id="mcp-traffic")
            claims = _Claims(root, [])
            result = CapabilityBroker(
                adapter,
                approval_verifier=_Verifier(),
                policy_verifier=adapter.verify_approval,
                identity_provider=lambda: dict(OWNER),
                claims_factory=lambda *_: claims,
                clock=lambda: 10.0,
            ).execute(request, approval)
            self.assertEqual("PASS", result.outcome, result.to_record())
            self.assertEqual(
                [
                    {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "orchestrator-harness", "version": "0.1.0"}}},
                    {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
                    {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "get_board_info", "arguments": {"board_id": "stm-a"}}},
                ],
                transport.messages,
            )
            self.assertTrue(transport.closed)
            self.assertNotIn("get_board_info", str(result.to_record()))
            self.assertNotIn("private", str(result.to_record()))
            self.assertNotIn("endpoint", str(result.to_record()))

            parsed = CapabilityRequest.from_record(request, now_monotonic=0.0)
            snapshot = adapter.observe(parsed)
            approval_object = CapabilityApproval.from_record(approval)
            self.assertTrue(adapter.verify_approval(parsed, snapshot, approval_object))
            self.assertEqual(50.0, adapter.effective_expiry(parsed, snapshot, approval_object, 10.0))
            operation = pack.resolve(parsed)
            self.assertEqual(40, operation.maximum_duration_seconds)
            self.assertEqual("caller-supplied-revision", operation.policy["revision"])

    def test_S5_R2_003_supervised_cleanup_releases_claim_only_after_boundary_and_transport_closed(self) -> None:
        for boundary_complete, transport_closed, expected in (
            (True, True, "PASS"),
            (True, False, "UNCERTAIN"),
            (False, True, "UNCERTAIN"),
        ):
            with self.subTest(boundary_complete=boundary_complete, transport_closed=transport_closed), TemporaryDirectory() as temporary:
                root = Path(temporary)
                pack = _campaign_pack()
                events: list[str] = []
                transport = _Transport()
                if not transport_closed:
                    transport.close_and_join = lambda: (False, {"helpers_stopped": False, "errors": ["transport helper"]})
                adapter = FirmwareHardwareAdapter(
                    campaign_pack=pack,
                    snapshot_provider=lambda request: _firmware_snapshot(request, pack),
                    launcher=lambda config: _Process(),
                    identity_provider=lambda pid: {"pid": pid, "created_utc": "fake-child", "creation_identity": "fake-exact"},
                    config_provider=lambda request, operation: {},
                    transport_factory=lambda process, remaining, request_id, config: transport,
                    supervisor_factory=lambda process, identity, request_id, boundary: _Supervisor(),
                    boundary_factory=lambda: _Boundary(complete=boundary_complete),
                    clock=lambda: 10.0,
                )
                request, approval = _firmware_request_and_approval(adapter, pack, request_id=f"cleanup-{boundary_complete}-{transport_closed}")
                claims = _Claims(root, events)
                result = CapabilityBroker(
                    adapter,
                    approval_verifier=_Verifier(),
                    policy_verifier=adapter.verify_approval,
                    identity_provider=lambda: dict(OWNER),
                    claims_factory=lambda *_: claims,
                    clock=lambda: 10.0,
                ).execute(request, approval)
                self.assertEqual(expected, result.outcome, result.to_record())
                self.assertEqual(expected == "PASS", result.record["claims_released"])
                if expected == "UNCERTAIN":
                    self.assertTrue(claims.held)
                    self.assertNotIn("release", events)


if __name__ == "__main__":
    unittest.main()
