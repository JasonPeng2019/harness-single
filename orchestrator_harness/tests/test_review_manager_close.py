from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from orchestrator_harness import review


class ReviewManagerCloseTests(unittest.TestCase):
    def test_managed_review_supplies_nonblank_close_summary(self) -> None:
        runtime = Path("runtime")
        lane = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "lifecycle": "review_pending",
        }
        event = {"event_id": "event-1"}
        with (
            mock.patch.object(review, "find_harness_root", return_value=Path("harness")),
            mock.patch.object(
                review,
                "load_config",
                return_value=SimpleNamespace(runtime_root=runtime),
            ),
            mock.patch.object(
                review,
                "_resolve_lane_managed",
                return_value=("epoch-1", lane, event),
            ),
            mock.patch.object(review, "_write_pair", return_value=({}, {})),
            mock.patch.object(review, "close_event") as close_event,
        ):
            result = review.run_completion_review(
                event_id="event-1",
                lane_id=None,
                review_outcome="PASS",
                approval="ACCEPTED",
                review_summary="accepted by ROOT",
                evidence=[],
                force_accept=False,
                force_reason=None,
            )

        self.assertTrue(result["ok"])
        close_event.assert_called_once()
        args, kwargs = close_event.call_args
        self.assertEqual((runtime, "event-1", "COMPLETE"), args)
        summary = kwargs.get("summary", "")
        self.assertTrue(summary.strip())
        self.assertIn("PASS", summary)
        self.assertIn("ACCEPTED", summary)


if __name__ == "__main__":
    unittest.main()
