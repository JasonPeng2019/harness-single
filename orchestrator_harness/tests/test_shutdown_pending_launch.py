from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import shutdown


class ShutdownPendingLaunchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.status = self.root / "controller.status.json"
        self.lane = {
            "lane_id": "pending", "run_id": "fresh-resume",
            "lifecycle": "running", "launch_pending": True, "process": {},
            "controller_status_path": str(self.status),
        }

    def clean(self, **changes) -> bool:
        with patch.object(shutdown.processes, "identity_matches", return_value=False):
            return shutdown._lane_clean(self.root, {**self.lane, **changes})

    def test_resumed_but_never_launched_lane_can_close(self) -> None:
        self.assertTrue(self.clean())

    def test_initial_prepared_lane_can_close(self) -> None:
        self.assertTrue(self.clean(lifecycle="prepared", launch_pending=False))

    def test_missing_launch_attestation_remains_unproven(self) -> None:
        for pending in (False, None, "true"):
            with self.subTest(pending=pending):
                self.assertFalse(self.clean(launch_pending=pending))

    def test_recorded_or_malformed_process_remains_unproven(self) -> None:
        for process in ({"pid": 42}, {"pid": None}, {"pid": "42"}, None):
            with self.subTest(process=process):
                self.assertFalse(self.clean(process=process))

    def test_other_lifecycle_remains_unproven(self) -> None:
        for lifecycle in ("review_pending", "blocked", "retired", "unknown"):
            with self.subTest(lifecycle=lifecycle):
                self.assertFalse(self.clean(lifecycle=lifecycle))

    def test_existing_invalid_status_remains_unproven(self) -> None:
        self.status.write_text("not-json", encoding="utf-8")
        self.assertFalse(self.clean())

    def test_status_directory_is_not_absence(self) -> None:
        self.status.mkdir()
        self.assertFalse(self.clean())

    def test_observed_live_controller_prevents_closure(self) -> None:
        with patch.object(shutdown.processes, "identity_matches", return_value=True):
            self.assertFalse(shutdown._lane_clean(self.root, self.lane))


if __name__ == "__main__":
    unittest.main()
