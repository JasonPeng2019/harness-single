"""A durable review pair can finish a failed manager-close retry."""

from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from unittest.mock import patch

from orchestrator_harness import review
from orchestrator_harness.config import HarnessConfig
from orchestrator_harness.core import content_hash
from orchestrator_harness.epochs import lane_record_dir
from orchestrator_harness.records import atomic_write_json


class ReviewReplayTests(unittest.TestCase):
    def test_orphan_acceptance_rebuilds_only_matching_review(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            worktree = root / "worktree"
            worktree.mkdir()
            lane = {
                "lane_id": "lane-1", "run_id": "run-1",
                "worktree_path": str(worktree),
            }
            folder = lane_record_dir(root, "epoch-1", "lane-1")
            review_path = folder / "COMPLETION_REVIEW.json"
            acceptance_path = folder / "ORCHESTRATOR_ACCEPTANCE.json"

            def publish(summary: str) -> tuple[dict, dict]:
                return review._write_pair(
                    root, "epoch-1", lane, review_outcome="PASS",
                    review_summary=summary, evidence=[], approval="ACCEPTED",
                    force_accept_reason=None,
                )

            with (
                patch.object(review, "_read_task_card", return_value={"card_id": "card-1"}),
                patch.object(review, "_read_result", return_value={"outcome": "PASS"}),
                patch.object(review, "_worktree_commit", return_value="commit-1"),
            ):
                publish("verified")
                original_review = review_path.read_bytes()
                original_acceptance = acceptance_path.read_bytes()
                review_path.unlink()
                with self.assertRaises(review.ReviewError):
                    publish("different decision")
                self.assertFalse(review_path.exists())
                self.assertEqual(original_acceptance, acceptance_path.read_bytes())
                publish("verified")
                self.assertEqual(original_review, review_path.read_bytes())
                self.assertEqual(original_acceptance, acceptance_path.read_bytes())

    def test_concurrent_decisions_cannot_overwrite_first_pair(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            worktree = root / "worktree"
            worktree.mkdir()
            lane = {
                "lane_id": "lane-1", "run_id": "run-1",
                "worktree_path": str(worktree),
            }
            ready = Barrier(2)

            def read_result(*_args: object) -> dict:
                ready.wait(5)
                return {"outcome": "PASS"}

            def publish(summary: str) -> str:
                try:
                    review._write_pair(
                        root, "epoch-1", lane, review_outcome="PASS",
                        review_summary=summary, evidence=[], approval="ACCEPTED",
                        force_accept_reason=None,
                    )
                    return "written"
                except review.ReviewError as exc:
                    return exc.code

            with (
                patch.object(review, "_read_task_card", return_value={"card_id": "card-1"}),
                patch.object(review, "_read_result", side_effect=read_result),
                patch.object(review, "_worktree_commit", return_value="commit-1"),
                ThreadPoolExecutor(max_workers=2) as pool,
            ):
                first = pool.submit(publish, "first")
                second = pool.submit(publish, "second")
                outcomes = {first.result(), second.result()}
            self.assertEqual({"written", review.COMPLETION_REVIEW_OUTPUT_CONFLICT}, outcomes)
            folder = lane_record_dir(root, "epoch-1", "lane-1")
            saved_review = review.read_json(folder / "COMPLETION_REVIEW.json")
            saved_acceptance = review.read_json(folder / "ORCHESTRATOR_ACCEPTANCE.json")
            self.assertIn(saved_review["review_summary"], {"first", "second"})
            self.assertEqual(saved_review["content_hash"], saved_acceptance["review_ref"])

    def test_plain_retry_rejects_changed_result_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workspace = root / "workspace"
            workspace.mkdir()
            config = HarnessConfig(root, workspace, "enabled")
            worktree = root / "worktree"
            worktree.mkdir()
            lane = {
                "lane_id": "lane-1", "run_id": "run-1", "lifecycle": "review_pending",
                "worktree_path": str(worktree),
            }
            with (
                patch.object(review, "_read_task_card", return_value={"card_id": "card-1"}),
                patch.object(review, "_worktree_commit", return_value="commit-1"),
                patch.object(review, "_read_result", return_value={"outcome": "PASS"}),
            ):
                review._write_pair(
                    config.runtime_root, "epoch-1", lane, review_outcome="PASS",
                    review_summary="verified", evidence=[], approval="ACCEPTED",
                    force_accept_reason=None,
                )
            with (
                patch.object(review, "find_harness_root", return_value=root),
                patch.object(review, "load_config", return_value=config),
                patch.object(review, "find_active_lane", return_value=("epoch-1", lane)),
                patch.object(review, "_read_task_card", return_value={"card_id": "card-1"}),
                patch.object(review, "_worktree_commit", return_value="commit-1"),
                patch.object(review, "_read_result", return_value={"outcome": "FAIL"}),
            ):
                result = review.run_completion_review(
                    event_id=None, lane_id="lane-1", review_outcome="PASS",
                    approval="ACCEPTED", review_summary="verified", evidence=[],
                    force_accept=False, force_reason=None,
                )
            self.assertFalse(result["ok"])
            self.assertEqual(review.COMPLETION_REVIEW_OUTPUT_CONFLICT, result["code"])

    def test_interrupted_publication_finishes_matching_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            worktree = root / "worktree"
            worktree.mkdir()
            lane = {
                "lane_id": "lane-1", "run_id": "run-1",
                "worktree_path": str(worktree),
            }
            folder = lane_record_dir(root, "epoch-1", "lane-1")
            review_path = folder / "COMPLETION_REVIEW.json"
            acceptance_path = folder / "ORCHESTRATOR_ACCEPTANCE.json"
            actual_write = atomic_write_json
            fail_once = True

            def interrupted_write(path: Path, record: dict) -> None:
                nonlocal fail_once
                if path == acceptance_path and fail_once:
                    fail_once = False
                    raise OSError("interrupted acceptance publication")
                actual_write(path, record)

            def publish(summary: str = "verified") -> tuple[dict, dict]:
                return review._write_pair(
                    root, "epoch-1", lane, review_outcome="PASS",
                    review_summary=summary, evidence=[], approval="ACCEPTED",
                    force_accept_reason=None,
                )

            with (
                patch.object(review, "_read_task_card", return_value={"card_id": "card-1"}),
                patch.object(review, "_read_result", return_value={"outcome": "PASS"}),
                patch.object(review, "_worktree_commit", return_value="commit-1"),
                patch.object(review, "atomic_write_json", side_effect=interrupted_write),
            ):
                with self.assertRaises(OSError):
                    publish()
                self.assertTrue(review_path.is_file())
                self.assertFalse(acceptance_path.exists())
                original_review = review_path.read_bytes()
                recovered_review, recovered_acceptance = publish()
                self.assertEqual(original_review, review_path.read_bytes())
                self.assertEqual(recovered_review["content_hash"], recovered_acceptance["review_ref"])
                with self.assertRaises(review.ReviewError):
                    publish("different decision")
                self.assertEqual(original_review, review_path.read_bytes())

    def test_complete_event_without_review_pair_does_not_republish(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workspace = root / "workspace"
            workspace.mkdir()
            config = HarnessConfig(root, workspace, "enabled")
            lane = {"lane_id": "lane-1", "run_id": "run-1", "lifecycle": "review_pending"}
            event = {"event_id": "event-1", "state": "COMPLETE"}
            with (
                patch.object(review, "find_harness_root", return_value=root),
                patch.object(review, "load_config", return_value=config),
                patch.object(review, "_resolve_lane_managed", return_value=("epoch-1", lane, event)),
                patch.object(review, "_write_pair") as write_pair,
            ):
                result = review.run_completion_review(
                    event_id="event-1", lane_id=None, review_outcome="PASS",
                    approval="ACCEPTED", review_summary="verified", evidence=[],
                    force_accept=False, force_reason=None,
                )
            self.assertFalse(result["ok"])
            self.assertEqual(review.COMPLETION_REVIEW_OUTPUT_CONFLICT, result["code"])
            write_pair.assert_not_called()

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
