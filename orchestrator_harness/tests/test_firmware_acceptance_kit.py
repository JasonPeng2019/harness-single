from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from firmware_acceptance.kit import AcceptanceBroker, AdmissionError, canonical_bound_operation, evaluate_call, validate_campaign_contract, validate_seed_manifest, worker_environment


class FirmwareAcceptanceKitTests(unittest.TestCase):
    def test_seed_is_exact_and_hash_bound(self) -> None:
        validate_seed_manifest(Path("firmware_acceptance/seed"))

    def test_controller_admission_is_bounded_and_fail_closed(self) -> None:
        call = {"call_id": "c1", "lane_id": "P3.STM", "board": "STM-A", "probe_uid": "uid", "target": "STM32L476RG", "profile": "stm", "method": "reset_and_halt", "arguments": {"board_id": "STM-A"}, "proposal_sha256": "a", "decision_sha256": "b", "authorization_sha256": "c", "deadline_monotonic": 100.0, "plan": {"max_operation_duration_seconds": 30}, "permission": {"granted": True}}
        self.assertEqual("ALLOW", evaluate_call(call, now_monotonic=1.0)["policy"])
        call["arguments"] = {"operation": "mass_erase"}
        with self.assertRaises(AdmissionError):
            evaluate_call(call, now_monotonic=1.0)

    def test_read_memory_policy_matches_pinned_signature_and_bounds(self) -> None:
        call = {"call_id": "m1", "lane_id": "P3.STM", "board": "STM-A", "probe_uid": "uid", "target": "STM32L476RG", "profile": "stm", "method": "read_memory_address", "arguments": {"board_id": "STM-A", "address": "0x20000000", "width": 32, "length": 4}, "proposal_sha256": "a", "decision_sha256": "b", "authorization_sha256": "c", "deadline_monotonic": 100.0, "plan": {"max_operation_duration_seconds": 30}, "permission": {"granted": True}}
        self.assertEqual("ALLOW", evaluate_call(call, now_monotonic=1.0)["policy"])
        call["arguments"] = {"board_id": "STM-A", "address": 0, "size": 4}
        with self.assertRaises(AdmissionError):
            evaluate_call(call, now_monotonic=1.0)
        call["arguments"] = {"board_id": "STM-A", "address": 0, "width": 64, "length": 4}
        with self.assertRaises(AdmissionError):
            evaluate_call(call, now_monotonic=1.0)

    def test_worker_environment_has_no_mcp_capability(self) -> None:
        env = worker_environment()
        self.assertEqual("", env["MCP_ENDPOINT"])
        self.assertEqual("", env["MCP_COMMAND"])

    def test_broker_materializes_seed_and_immutable_ordered_chain(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            broker = AcceptanceBroker(root / "broker", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
            target = root / "broker" / "targets" / "target"
            broker.materialize_seed(target)
            self.assertEqual(40, len(broker.validate_target(target)))
            config = broker.controller_config("STM-A", {})
            self.assertEqual("", config["worker_environment"]["MCP_ENDPOINT"])
            bound = {"server_commit":"f003f84a7df51cd8595a3203c62e225b21da2a22","method":"reset_and_halt","method_version":1,"arguments":{"board_id":"STM-A"},"policy_sha256":"p","schema_sha256":"s","plan_sha256":"pl","permission_sha256":"pe","authorization_sha256":"a","claim_sha256":"c","call_id":"call-1","attempt_id":"attempt-0001","lane_id":"STM-A","board":"STM-A","probe_uid":"uid","target":"STM32L476RG","profile":"stm","route":"rediscover","governing_hashes":{"goal":"g"},"c1_reference":{"path":"c1","sha256":"h"},"deadline_monotonic":100,"expires_monotonic":99,"seed_identity":{"manifest":"x"},"target_identity":{"commit":"y"},"raw_result_sha256":"r","cleanup_owner":"C3-HARNESS"}
            common = {"attempt_id": "attempt-0001", "lane_id": "STM-A", "board": "STM-A", "probe_uid": "uid", "target": "STM32L476RG", "profile": "stm", "route": "rediscover", "governing_hashes": {"goal": "g"}, "c1_reference": {"path": "c1", "sha256": "h"}, "identity": {"controller": "pid:1"}, "bound_operation": bound, "bound_operation_sha256": __import__("firmware_acceptance.kit", fromlist=["canonical_sha256"]).canonical_sha256(canonical_bound_operation(bound))}
            path, digest = broker.record("proposal", "call-1", common)
            broker.record("policy-evaluation", "call-1", common, (str(path), digest))
            with self.assertRaises(AdmissionError):
                broker.record("dispatch", "call-2", common)
            with self.assertRaises(AdmissionError):
                broker.record("dispatch", "call-1", common, (str(path), digest))

    def test_campaign_contract_is_closed_and_seed_rewrite_fails(self) -> None:
        validate_campaign_contract(Path("firmware_acceptance/seed"))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            broker = AcceptanceBroker(root / "broker", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
            target = root / "broker" / "targets" / "target"
            broker.materialize_seed(target)
            (target / "TARGET_CHARTER.md").chmod(0o600)
            (target / "TARGET_CHARTER.md").write_text("tampered", encoding="utf-8")
            with self.assertRaises(AdmissionError):
                broker.validate_target(target)

    def test_broker_rejects_ambient_capabilities_and_policy_is_data_driven(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            broker = AcceptanceBroker(root / "broker", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
            with self.assertRaises(AdmissionError):
                broker.controller_config("NRF-A", {"MCP_ENDPOINT": "forbidden"})
