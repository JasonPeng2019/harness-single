from __future__ import annotations

from pathlib import Path
import unittest

from firmware_acceptance.kit import AdmissionError, evaluate_call, validate_seed_manifest, worker_environment


class FirmwareAcceptanceKitTests(unittest.TestCase):
    def test_seed_is_exact_and_hash_bound(self) -> None:
        validate_seed_manifest(Path("firmware_acceptance/seed"))

    def test_controller_admission_is_bounded_and_fail_closed(self) -> None:
        call = {"call_id": "c1", "lane_id": "P3.STM", "board": "STM-A", "probe_uid": "uid", "target": "STM32L476RG", "profile": "stm", "method": "reset/v1", "arguments": {}, "proposal_sha256": "a", "decision_sha256": "b", "authorization_sha256": "c", "deadline_monotonic": 100.0, "plan": {"max_operation_duration_seconds": 30}, "permission": {"granted": True}}
        self.assertEqual("ALLOW", evaluate_call(call, now_monotonic=1.0)["policy"])
        call["arguments"] = {"operation": "mass_erase"}
        with self.assertRaises(AdmissionError):
            evaluate_call(call, now_monotonic=1.0)

    def test_worker_environment_has_no_mcp_capability(self) -> None:
        env = worker_environment()
        self.assertEqual("", env["MCP_ENDPOINT"])
        self.assertEqual("", env["MCP_COMMAND"])
