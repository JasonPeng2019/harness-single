"""Durable review replay, publication recovery and concurrent decisions."""
from __future__ import annotations

import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch

from orchestrator_harness import review
from orchestrator_harness.epochs import lane_record_dir
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.tests.result_fixture import LinkedResultFixture


class ReviewReplayTests(LinkedResultFixture, unittest.TestCase):
    def publish_pair(self, summary="verified"):
        return review._write_pair(self.rt, self.epoch, self.lane, review_outcome="PASS",
                                  review_summary=summary, evidence=[], approval="ACCEPTED",
                                  force_accept_reason=None)

    def command(self, *, event=None):
        patches = [patch.object(review, "find_harness_root", return_value=self.base),
                   patch.object(review, "load_config", return_value=self.config)]
        if event is None:
            patches.append(patch.object(review, "find_active_lane", return_value=(self.epoch, self.lane)))
        else:
            patches.append(patch.object(review, "_resolve_lane_managed", return_value=(self.epoch, self.lane, event)))
        from contextlib import ExitStack
        with ExitStack() as stack:
            for context in patches:
                stack.enter_context(context)
            return review.run_completion_review(event_id=event["event_id"] if event else None,
                                                lane_id=None if event else self.lane["lane_id"],
                                                review_outcome="PASS", approval="ACCEPTED",
                                                review_summary="verified", evidence=[],
                                                force_accept=False, force_reason=None)

    def test_orphan_acceptance_rebuilds_only_matching_review(self):
        folder = lane_record_dir(self.rt, self.epoch, self.lane["lane_id"])
        review_path, acceptance_path = folder / "COMPLETION_REVIEW.json", folder / "ORCHESTRATOR_ACCEPTANCE.json"
        self.publish_pair()
        original_review, original_acceptance = review_path.read_bytes(), acceptance_path.read_bytes()
        review_path.unlink()
        with self.assertRaises(review.ReviewError):
            self.publish_pair("different decision")
        self.assertFalse(review_path.exists())
        self.assertEqual(original_acceptance, acceptance_path.read_bytes())
        self.publish_pair()
        self.assertEqual(original_review, review_path.read_bytes())
        self.assertEqual(original_acceptance, acceptance_path.read_bytes())

    def test_concurrent_decisions_cannot_overwrite_first_pair(self):
        ready = Barrier(2)
        def publish(summary):
            # Synchronize before entering the production serialization lock.
            ready.wait(5)
            try:
                self.publish_pair(summary)
                return "written"
            except review.ReviewError as exc:
                return exc.code
        with ThreadPoolExecutor(max_workers=2) as pool:
            first, second = pool.submit(publish, "first"), pool.submit(publish, "second")
            outcomes = {first.result(), second.result()}
        self.assertEqual({"written", review.COMPLETION_REVIEW_OUTPUT_CONFLICT}, outcomes)
        folder = lane_record_dir(self.rt, self.epoch, self.lane["lane_id"])
        saved_review = review.read_json(folder / "COMPLETION_REVIEW.json")
        saved_acceptance = review.read_json(folder / "ORCHESTRATOR_ACCEPTANCE.json")
        self.assertIn(saved_review["review_summary"], {"first", "second"})
        self.assertEqual(saved_review["content_hash"], saved_acceptance["review_ref"])

    def test_plain_retry_rejects_changed_result_source(self):
        self.publish_pair()
        self.publish(outcome="FAIL")
        result = self.command()
        self.assertFalse(result["ok"], result)
        self.assertEqual(review.COMPLETION_REVIEW_OUTPUT_CONFLICT, result["code"])

    def test_interrupted_publication_finishes_matching_acceptance(self):
        folder = lane_record_dir(self.rt, self.epoch, self.lane["lane_id"])
        review_path, acceptance_path = folder / "COMPLETION_REVIEW.json", folder / "ORCHESTRATOR_ACCEPTANCE.json"
        fail_once = True
        def interrupted_write(path, record):
            nonlocal fail_once
            if path == acceptance_path and fail_once:
                fail_once = False
                raise OSError("interrupted acceptance publication")
            atomic_write_json(path, record)
        with patch.object(review, "atomic_write_json", side_effect=interrupted_write):
            with self.assertRaises(OSError):
                self.publish_pair()
            self.assertTrue(review_path.is_file())
            self.assertFalse(acceptance_path.exists())
            original_review = review_path.read_bytes()
            recovered_review, recovered_acceptance = self.publish_pair()
            self.assertEqual(original_review, review_path.read_bytes())
            self.assertEqual(recovered_review["content_hash"], recovered_acceptance["review_ref"])
            with self.assertRaises(review.ReviewError):
                self.publish_pair("different decision")
            self.assertEqual(original_review, review_path.read_bytes())

    def test_complete_event_without_review_pair_does_not_republish(self):
        with patch.object(review, "_write_pair") as write_pair:
            result = self.command(event={"event_id": "event-1", "state": "COMPLETE"})
        self.assertFalse(result["ok"], result)
        self.assertEqual(review.COMPLETION_REVIEW_OUTPUT_CONFLICT, result["code"])
        write_pair.assert_not_called()

    def test_matching_pair_retries_manager_close_without_rewriting(self):
        review_record, acceptance = self.publish_pair()
        folder = lane_record_dir(self.rt, self.epoch, self.lane["lane_id"])
        before = [(path, path.read_bytes()) for path in (folder / "COMPLETION_REVIEW.json", folder / "ORCHESTRATOR_ACCEPTANCE.json")]
        with patch.object(review, "_write_pair") as write_pair, patch.object(review, "close_event") as close_event:
            result = self.command(event={"event_id": "event-1", "state": "ACKNOWLEDGED"})
        self.assertTrue(result["ok"], result)
        write_pair.assert_not_called()
        close_event.assert_called_once()
        for path, value in before:
            self.assertEqual(value, path.read_bytes())
        replayed = review._replay_pair(folder, self.lane, review_outcome="PASS", review_summary="verified",
                                       evidence=[], approval="ACCEPTED", force_accept_reason=None)
        self.assertEqual((review_record, acceptance), replayed)


if __name__ == "__main__":
    unittest.main()
