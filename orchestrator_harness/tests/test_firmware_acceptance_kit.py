from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from firmware_acceptance.kit import AcceptanceBroker, AdmissionError, evaluate_call, validate_seed_manifest, worker_environment


class FirmwareAcceptanceKitTests(unittest.TestCase):
    def test_seed_is_exact_and_hash_bound(self) -> None:
        validate_seed_manifest(Path("firmware_acceptance/seed"))

    def test_controller_admission_is_bounded_and_fail_closed(self) -> None:
        call = {"call_id": "c1", "lane_id": "P3.STM", "board": "STM-A", "probe_uid": "uid", "target": "STM32L476RG", "profile": "stm", "method": "reset_and_halt", "arguments": {"board_id": "STM-A"}, "proposal_sha256": "a", "decision_sha256": "b", "authorization_sha256": "c", "deadline_monotonic": 100.0, "plan": {"max_operation_duration_seconds": 30}, "permission": {"granted": True}}
        self.assertEqual("ALLOW", evaluate_call(call, now_monotonic=1.0)["policy"])
        call["arguments"] = {"operation": "mass_erase"}
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
            common = {"attempt_id": "attempt-0001", "lane_id": "STM-A", "board": "STM-A", "probe_uid": "uid", "target": "STM32L476RG", "profile": "stm", "route": None, "governing_hashes": {"goal": "g"}, "c1_reference": {"path": "c1", "sha256": "h"}, "identity": {"controller": "pid:1"}}
            path, digest = broker.record("proposal", "call-1", common)
            broker.record("policy-evaluation", "call-1", common, (str(path), digest))
            with self.assertRaises(AdmissionError):
                broker.record("dispatch", "call-2", common)
            with self.assertRaises(AdmissionError):
                broker.record("dispatch", "call-1", common, (str(path), digest))

    def test_broker_rejects_ambient_capabilities_and_policy_is_data_driven(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            broker = AcceptanceBroker(root / "broker", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
            with self.assertRaises(AdmissionError):
                broker.controller_config("NRF-A", {"MCP_ENDPOINT": "forbidden"})
