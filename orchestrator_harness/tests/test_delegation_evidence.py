"""The run gate requires real, accepted worker roles in sequence."""

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[2] / "tools" / "verify_delegation.py"
spec = importlib.util.spec_from_file_location("verify_delegation", SCRIPT)
assert spec and spec.loader
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


class DelegationEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "monitor").mkdir()

    def record(self, lane: str, event: str, *, detail: str = "") -> None:
        row = {
            "event": "worker.lifecycle", "lane_id": lane,
            "run_id": f"run-{lane}", "worker_event": event, "detail": detail,
        }
        with (self.root / "monitor" / "MONITOR_IMPORTANT.log").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(row) + "\n")

    def completed(self, lane: str, *, approval: str = "ACCEPTED") -> None:
        for event in ("provider_started", "result_valid"):
            self.record(lane, event)
        self.record(lane, "acceptance_copied", detail=approval)

    def test_distinct_planner_implementer_validator_pass(self) -> None:
        for lane in ("planner-01", "implementer-01", "validator-01"):
            self.completed(lane)
        self.assertEqual(
            {"planner": "planner-01", "implementer": "implementer-01", "validator": "validator-01"},
            verifier.verify(self.root),
        )

    def test_validator_only_cannot_pass(self) -> None:
        self.completed("validator-01")
        with self.assertRaisesRegex(ValueError, "planner"):
            verifier.verify(self.root)

    def test_rejected_planner_cannot_pass(self) -> None:
        self.completed("planner-01", approval="REJECTED")
        self.completed("implementer-01")
        self.completed("validator-01")
        with self.assertRaisesRegex(ValueError, "planner"):
            verifier.verify(self.root)

    def test_implementer_must_start_after_planner_acceptance(self) -> None:
        self.record("planner-01", "provider_started")
        self.completed("implementer-01")
        self.record("planner-01", "result_valid")
        self.record("planner-01", "acceptance_copied", detail="ACCEPTED")
        self.completed("validator-01")
        with self.assertRaisesRegex(ValueError, "implementer"):
            verifier.verify(self.root)


if __name__ == "__main__":
    unittest.main()
