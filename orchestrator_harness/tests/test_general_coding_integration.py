from __future__ import annotations

import json
import os
import stat
import shutil
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from typing import Any

from examples.disposable_coding_fixture import run_fixture


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AssertionError(f"expected JSON object in {path}")
    return value


def _remove_readonly(func: Any, path: str, _: Any) -> None:
    os.chmod(path, stat.S_IWRITE)
    func(path)


class GeneralCodingIntegrationTests(unittest.TestCase):
    def test_disposable_coding_workflow_isolated_and_merge_ready(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="orchestrator-general-coding-"))
        try:
            result = run_fixture(root)

            alpha = root / "worktrees" / "alpha"
            beta = root / "worktrees" / "beta"
            merge = root / "worktrees" / "merge"
            statuses = {
                "alpha": _read_json(
                    alpha / ".agent-workspace" / "fixture_controller.status.json"
                ),
                "beta": _read_json(
                    beta / ".agent-workspace" / "fixture_controller.status.json"
                ),
                "merge": _read_json(
                    merge / ".agent-workspace" / "fixture_controller.status.json"
                ),
            }

            self.assertEqual("orchestrator-disposable-coding-fixture/v1", result["schema"])
            self.assertEqual(3, result["coding_lane_count"])
            self.assertEqual(0, result["firmware_records"])
            self.assertTrue(result["contention_observed"])
            self.assertTrue(result["stale_result_rejected"])
            self.assertEqual(["alpha", "beta", "merge"], result["valid_results"])
            self.assertEqual("PASS", result["python_tests"])
            self.assertEqual(0, result["resource_claims_remaining"])

            self.assertEqual({"alpha", "beta"}, {alpha.name, beta.name})
            self.assertTrue((alpha / "feature_alpha.py").is_file())
            self.assertTrue((beta / "feature_beta.py").is_file())
            self.assertTrue((merge / "feature_alpha.py").is_file())
            self.assertTrue((merge / "feature_beta.py").is_file())

            for lane in ("alpha", "beta"):
                status = statuses[lane]
                self.assertEqual("orchestrator-lane-controller/v1", status["schema"])
                self.assertEqual(lane, status["declared_lane_id"])
                self.assertEqual([], status["mcp_servers"])
                self.assertEqual([], status["board_tokens"])
                self.assertEqual([], status["held_resource_claims"])
                self.assertTrue((Path(status["jsonl_path"])).is_file())
                self.assertTrue((Path(status["stderr_path"])).is_file())

            self.assertEqual("VALID", statuses["alpha"]["result_validation"]["state"])
            self.assertEqual("INVALID", statuses["beta"]["result_validation"]["state"])
            self.assertEqual(
                "CODING_RESULT_INVALID", statuses["beta"]["result_validation"]["code"]
            )
            self.assertEqual("VALID", statuses["merge"]["result_validation"]["state"])

            alpha_ended = datetime.fromisoformat(statuses["alpha"]["ended_utc"])
            beta_started = datetime.fromisoformat(statuses["beta"]["codex_started_utc"])
            self.assertLessEqual(alpha_ended, beta_started)

            for lane, workspace in (("alpha", alpha), ("beta", beta), ("merge", merge)):
                result_record = _read_json(workspace / ".agent-workspace" / "RESULT.json")
                self.assertEqual(lane, result_record["lane_id"])
                self.assertEqual("PASS", result_record["outcome"])
                self.assertEqual(
                    statuses[lane]["branch"], result_record["branch"]
                )
                self.assertEqual(40, len(result_record["commit"]))
                if lane != "beta":
                    self.assertEqual(
                        statuses[lane]["result_validation"]["commit"],
                        result_record["commit"],
                    )

            event_id = result["acknowledged_event_id"]
            self.assertIsInstance(event_id, str)
            self.assertEqual(0, result["s3_queue_pending_after_ack"])
            queue_root = Path(result["s3_queue_root"])
            self.assertTrue((queue_root / "QUEUE.jsonl").is_file())
            self.assertTrue((queue_root / "STATE.json").is_file())
            self.assertEqual([], list((root / "runtime" / "coding-resource-locks").glob("*.json")))
        finally:
            shutil.rmtree(root, onerror=_remove_readonly)
        self.assertFalse(root.exists())


if __name__ == "__main__":
    unittest.main()
