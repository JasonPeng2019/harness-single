from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from orchestrator_harness.lane_status_export import export_lane_status


class LaneStatusExportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.workspace = self.root / "fresh-experiments" / "run" / "worktrees" / "lane" / ".agent-workspace"
        self.workspace.mkdir(parents=True)
        self.invocation = self.workspace / "invocation.json"
        self.invocation.write_text(
            json.dumps(
                {
                    "schema": "orchestrator-coding-invocation/v1",
                    "lane_id": "LANE-SPEC-01",
                    "worker_invocation_id": "a21-spec-v14-001",
                    "run_root": str(self.workspace.parent),
                }
            ),
            encoding="utf-8",
        )
        (self.workspace / "controller.status.json").write_text(
            json.dumps({"state": "launch_failed", "error": "provider did not start"}),
            encoding="utf-8",
        )
        (self.workspace / "RESULT.json").write_text(
            json.dumps({"lane_id": "wrong-lane", "verdict": "INCOMPLETE"}),
            encoding="utf-8",
        )
        (self.workspace / "last-message.txt").write_text(
            "Worker stopped before writing RESULT.json.", encoding="utf-8"
        )
        (self.workspace / "codex.stderr.log").write_text(
            ("leading noise\n" * 2000) + "terminal provider error\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_exports_status_and_result_only_outside_lane_workspace(self) -> None:
        destination = self.root / "runtime" / "a21-status.json"

        exported = export_lane_status(self.invocation, destination)

        self.assertEqual("orchestrator-lane-status-export/v1", exported["schema"])
        self.assertEqual("LANE-SPEC-01", exported["lane_id"])
        self.assertEqual("launch_failed", exported["controller_status"]["state"])
        self.assertEqual("wrong-lane", exported["worker_result"]["lane_id"])
        self.assertEqual(
            "Worker stopped before writing RESULT.json.",
            exported["worker_last_message"],
        )
        self.assertIn("terminal provider error", exported["worker_stderr_tail"])
        self.assertLessEqual(len(exported["worker_stderr_tail"].encode("utf-8")), 8192)
        self.assertEqual(exported, json.loads(destination.read_text(encoding="utf-8")))
        self.assertFalse(destination.is_relative_to(self.workspace))

    def test_rejects_an_export_destination_inside_lane_workspace(self) -> None:
        with self.assertRaisesRegex(ValueError, "outside the lane workspace"):
            export_lane_status(self.invocation, self.workspace / "status-export.json")


if __name__ == "__main__":
    unittest.main()
