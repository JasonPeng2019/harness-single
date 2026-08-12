from __future__ import annotations

import copy
import hashlib
import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from firmware_acceptance.campaign_pack import CAMPAIGN_CAPABILITY, DEFAULT_CAMPAIGN_PACK
from orchestrator_harness.capability_broker import (
    APPROVAL_SCHEMA,
    CapabilityApproval,
    CapabilityBroker,
    CapabilityRequest,
    CapabilitySnapshot,
    FakeCapabilityAdapter,
    current_process_identity,
)
from orchestrator_harness.process_supervisor import CleanupResult
from firmware_acceptance.capability_adapter import FirmwareHardwareAdapter
from firmware_acceptance.controller import FirmwareAcceptanceController
from firmware_acceptance.kit import AcceptanceBroker


OWNER = {"pid": 321, "created_utc": "2026-01-01T00:00:00Z", "creation_identity": "fake:321"}


class _Verifier:
    def verify(self, payload: bytes, signature: str, public_key: str) -> bool:
        return bool(payload and signature and public_key)


class _Claims:
    def __init__(self, root: Path, events: list[str], *, owner: dict[str, Any] = OWNER) -> None:
        self.root = root
        self.events = events
        self.owner = dict(owner)
        self._held: dict[str, dict[str, Any]] = {}
        self.retained = False

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
            }

    def retain_boundary(self, *, boundary, identities) -> list[str]:
        self.events.append("retain")
        self.retained = True
        return []

    def release_all(self) -> list[str]:
        self.events.append("release")
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
                        }
                        break
                on_wait({"resource": resource, "state": "CONTENDED", "reason": "shared fake owner", "wait_seconds": 0.001, "actionable": False})
                time.sleep(0.001)

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


def _broker(adapter, claims, *, now: float = 10.0, events: list[str] | None = None) -> CapabilityBroker:
    return CapabilityBroker(
        adapter,
        approval_verifier=_Verifier(),
        policy_verifier=lambda request, snapshot, approval: True,
        identity_provider=lambda: dict(OWNER),
        claims_factory=lambda lane, request_id: claims,
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
            self.assertEqual(["claim", "cleanup", "release"], events[:3])
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
            self.assertEqual(["claim", "cleanup", "release"], events[:3])

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

    def test_S5_F7_hardware_adapter_keeps_mapping_transport_and_child_cleanup_private(self) -> None:
        transport = _Transport()

        def snapshot_provider(request: CapabilityRequest) -> CapabilitySnapshot:
            return CapabilitySnapshot(
                request_id=request.request_id,
                lane_id=request.lane_id,
                controller_identity=dict(request.controller_identity),
                capability=request.capability,
                action=request.action,
                resources=request.resources,
                snapshot_id="hardware-snapshot-1",
                identity={"target_revision": "fake-target-1"},
                resource_identities={resource: {"revision": "fake-resource-1"} for resource in request.resources},
                capabilities={CAMPAIGN_CAPABILITY: ("observe",)},
                adapter_identity={"adapter_id": "firmware-hardware", "adapter_version": "v1"},
                observed_monotonic=10.0,
            )

        adapter = FirmwareHardwareAdapter(
            snapshot_provider=snapshot_provider,
            launcher=lambda config: _Process(),
            identity_provider=lambda pid: {"pid": pid, "created_utc": "fake-child"},
            config_provider=lambda request, operation: {"private": "controller-config"},
            transport_factory=lambda process, remaining, request_id, config: transport,
            supervisor_factory=lambda process, identity, request_id: _Supervisor(),
        )
        request = _request(
            request_id="hardware-request",
            lane_id="STM-A",
            capability=CAMPAIGN_CAPABILITY,
            action="observe",
            arguments={"fixture": "STM-A"},
        )
        parsed = CapabilityRequest.from_record(request, now_monotonic=0.0)
        snapshot = adapter.observe(parsed)
        approval = _approval(parsed, snapshot)
        approval["policy"] = {
            "policy_reference": DEFAULT_CAMPAIGN_PACK.method_policy_reference,
            "method_version": 1,
            "maximum_duration_seconds": DEFAULT_CAMPAIGN_PACK.authorized_maxima["observe"],
            "action": "observe",
        }
        claims = _Claims(Path("."), [])
        result = CapabilityBroker(
            adapter,
            approval_verifier=_Verifier(),
            policy_verifier=adapter.verify_approval,
            identity_provider=lambda: dict(OWNER),
            claims_factory=lambda *_: claims,
            clock=lambda: 10.0,
        ).execute(request, approval)
        self.assertEqual("PASS", result.outcome, result.to_record())
        self.assertTrue(transport.closed)
        self.assertEqual("get_board_info", transport.messages[2]["method"])
        self.assertNotIn("get_board_info", str(result.to_record()))
        self.assertNotIn("private", str(result.to_record()))

    def test_S5_F6_legacy_controller_exposes_generic_route_without_changing_old_entrypoints(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            acceptance = AcceptanceBroker(
                root / "broker",
                Path("firmware_acceptance/seed"),
                Path("firmware_acceptance/MCP_METHOD_POLICY.json"),
                Path("firmware_acceptance/LANE_TEMPLATES.json"),
            )
            claims = _Claims(root, [])
            controller = FirmwareAcceptanceController(
                acceptance,
                clock=lambda: 10.0,
                claims_factory=lambda lane, request_id: claims,
            )
            adapter = FakeCapabilityAdapter({"synthetic": ("read",)})
            request = _request()
            result = controller.execute_capability_request(
                request,
                self._approval_for(request, adapter),
                _Verifier(),
                adapter=adapter,
                policy_verifier=lambda *_: True,
                identity_provider=lambda: dict(OWNER),
            )
            self.assertEqual("PASS", result["outcome"])
            self.assertEqual(1, adapter.dispatch_calls)

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

    def test_S5_F6_campaign_pack_is_inert_and_retains_four_fixture_aliases(self) -> None:
        record = DEFAULT_CAMPAIGN_PACK.as_record()
        self.assertEqual(CAMPAIGN_CAPABILITY, record["capability"])
        self.assertEqual(["STM-A", "STM-B", "NRF-A", "NRF-B"], record["fixture_aliases"])
        self.assertFalse(record["hil_intent"]["physical_execution"])
        self.assertTrue(DEFAULT_CAMPAIGN_PACK.supports(capability=CAMPAIGN_CAPABILITY, action="observe", fixture_alias="STM-A"))
        self.assertFalse(DEFAULT_CAMPAIGN_PACK.supports(capability=CAMPAIGN_CAPABILITY, action="unknown", fixture_alias="STM-A"))


if __name__ == "__main__":
    unittest.main()
