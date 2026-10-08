"""A durable review pair can finish a failed manager-close retry."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import review
from orchestrator_harness.config import HarnessConfig
from orchestrator_harness.core import content_hash
from orchestrator_harness.epochs import lane_record_dir
from orchestrator_harness.records import atomic_write_json


class ReviewReplayTests(unittest.TestCase):
    def test_matching_pair_retries_manager_close_without_rewriting(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workspace = root / "workspace"
            workspace.mkdir()
            config = HarnessConfig(root, workspace, "enabled")
            rt = config.runtime_root
            lane = {"lane_id": "lane-1", "run_id": "run-1", "lifecycle": "accepted"}
            folder = lane_record_dir(rt, "epoch-1", "lane-1")
            review_record = {
                "schema": "completion-review/v1", "lane_id": "lane-1", "run_id": "run-1",
                "review_outcome": "PASS", "review_summary": "verified", "evidence": [],
                "task_card_id": "card-1", "task_card_hash": "card-hash",
                "result_id": "run-1", "result_hash": "result-hash",
                "commit": "commit-1", "reviewed_at": "2026-01-01T00:00:00Z",
            }
            review_record["content_hash"] = content_hash(review_record)
            acceptance = {
                "schema": "orchestrator-acceptance/v1", "lane_id": "lane-1", "run_id": "run-1",
                "approval": "ACCEPTED", "accepted_by": "ROOT",
                "review_ref": review_record["content_hash"],
                "task_card_id": "card-1", "task_card_hash": "card-hash",
                "result_id": "run-1", "result_hash": "result-hash",
                "commit": "commit-1", "decided_at": "2026-01-01T00:00:00Z",
            }
            acceptance["content_hash"] = content_hash(acceptance)
            atomic_write_json(folder / "COMPLETION_REVIEW.json", review_record)
            atomic_write_json(folder / "ORCHESTRATOR_ACCEPTANCE.json", acceptance)
            event = {"event_id": "event-1", "state": "ACKNOWLEDGED"}
            with (
                patch.object(review, "find_harness_root", return_value=root),
                patch.object(review, "load_config", return_value=config),
                patch.object(review, "_resolve_lane_managed", return_value=("epoch-1", lane, event)),
                patch.object(review, "_write_pair") as write_pair,
                patch.object(review, "close_event") as close_event,
            ):
                result = review.run_completion_review(
                    event_id="event-1", lane_id=None, review_outcome="PASS",
                    approval="ACCEPTED", review_summary="verified", evidence=[],
                    force_accept=False, force_reason=None,
                )
            self.assertTrue(result["ok"], result)
            write_pair.assert_not_called()
            close_event.assert_called_once()
            self.assertEqual(review_record["content_hash"],
                             review._replay_pair(
                                 folder, lane, review_outcome="PASS",
                                 review_summary="verified", evidence=[], approval="ACCEPTED",
                                 force_accept_reason=None,
                             )[0]["content_hash"])


if __name__ == "__main__":
    unittest.main()
