from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from orchestrator_harness.tests.v2_acceptance.contract import assert_valid_result, atomic_json, content_hash


class CheckU4Tests(unittest.TestCase):
    """CHECK-U4 — REQ-013/014 strict record and atomic-write oracle."""

    def test_result_rejects_stale_identity_bad_enum_and_bad_hash(self) -> None:
        result = {"schema": "result/v1", "lane_id": "lane", "run_id": "run", "outcome": "PASS", "summary": "done", "evidence": [], "completed_at": "2026-01-01T00:00:00Z"}
        result["content_hash"] = content_hash(result)
        assert_valid_result(result, "lane", "run")
        for field, value in (("run_id", "old-run"), ("outcome", "SUCCESS"), ("content_hash", "0" * 64)):
            mutated = dict(result)
            mutated[field] = value
            with self.subTest(field=field), self.assertRaises(AssertionError):
                assert_valid_result(mutated, "lane", "run")

    def test_rejected_replacement_preserves_complete_prior_record_and_leaves_no_temp(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "RUNTIME_STATE.json"
            prior = {"schema": "runtime-state/v1", "state": "OPEN"}
            atomic_json(path, prior)
            self.assertEqual(prior, json.loads(path.read_text(encoding="utf-8")))
            malformed = path.with_name(f".{path.name}.acceptance-tmp")
            malformed.write_text("{", encoding="utf-8")
            with self.assertRaises(json.JSONDecodeError):
                json.loads(malformed.read_text(encoding="utf-8"))
            self.assertEqual(prior, json.loads(path.read_text(encoding="utf-8")))
            malformed.unlink()
            self.assertFalse(malformed.exists())

    def test_public_launcher_exposes_the_complete_normative_v2_command_vocabulary(self) -> None:
        root = Path(__file__).resolve().parents[3]
        completed = subprocess.run(
            [sys.executable, "-m", "orchestrator_harness.operator_launch", "--help"],
            cwd=root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        help_text = completed.stdout
        required = (
            "harness setup",
            "harness shutdown",
            "lane bootstrap",
            "lane launch",
            "lane completion-review",
            "resume-lane",
            "lane force-stop",
            "lane retire",
            "manager acknowledge",
            "manager close",
            "send-lane-notification",
            "scan",
            "watch",
            "health reconcile",
        )
        missing = [token for token in required if token not in help_text]
        self.assertEqual([], missing, f"normative v2 public commands missing: {missing}")
