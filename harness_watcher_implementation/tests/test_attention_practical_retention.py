from __future__ import annotations
import json, subprocess, sys, tempfile, unittest
from pathlib import Path

from orchestrator_harness.host_adapters import DELIVERY_NOTICE_SCHEMA


class RetentionTests(unittest.TestCase):
    def test_retained_wake_and_quiet_evidence(self):
        root = Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as temp:
            evidence = Path(temp) / "evidence"
            result = subprocess.run(
                [
                    sys.executable,
                    "harness_watcher_implementation/tests/run_attention_practical.py",
                    "--evidence-dir",
                    str(evidence),
                ],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(0, result.returncode, result.stderr)
            wake = json.loads((evidence / "wake-result.json").read_text())
            quiet = json.loads((evidence / "quiet-result.json").read_text())
            # Wake half: one sparse notice, one DELIVERED boundary receipt,
            # and queue work left pending until explicit acknowledgement.
            self.assertEqual("orchestrator-practical-wake/v1", wake["schema"])
            notice = wake["notice"]
            self.assertEqual(DELIVERY_NOTICE_SCHEMA, notice["schema"])
            for forbidden in ("event_id", "event_ids", "data", "payload"):
                self.assertNotIn(forbidden, notice)
            self.assertEqual(1, notice["pending_count"])
            receipt = wake["receipt"]
            self.assertEqual("DELIVERED", receipt["outcome"])
            self.assertEqual("post_tool_use", receipt["boundary"])
            self.assertEqual(notice["notice_id"], receipt["notice_id"])
            self.assertEqual(
                notice["observed_queue_revision"], receipt["observed_queue_revision"]
            )
            self.assertEqual(["wake"], wake["pending_event_ids"])
            self.assertEqual(1, wake["pending_after_delivery"])
            self.assertFalse(wake["acknowledged_by_delivery"])
            self.assertEqual(
                ["PostToolUse"], [call["method"] for call in wake["transport_calls"]]
            )
            self.assertNotIn("event_id", wake["transport_calls"][0]["notice"])
            self.assertEqual([], wake["delivery_journal"][0]["event_ids"])
            # Quiet half: a fresh empty queue/coordinator emits no notice and
            # makes no transport call; it is not a drained wake queue.
            self.assertEqual("orchestrator-practical-quiet/v1", quiet["schema"])
            self.assertIsNone(quiet["notice"])
            self.assertIsNone(quiet["receipt"])
            self.assertEqual(0, quiet["pending_count"])
            self.assertEqual([], quiet["transport_calls"])
            self.assertIn("attention practical host-only check: PASS", result.stdout)


if __name__ == "__main__":
    unittest.main()
