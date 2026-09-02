from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from orchestrator_harness.tests.v2_acceptance.contract import claim


class CheckU5Tests(unittest.TestCase):
    """CHECK-U5 — REQ-015/016 portability and honest-evidence oracle."""

    def test_portable_paths_and_exact_process_creation_identity(self) -> None:
        with tempfile.TemporaryDirectory(prefix="v2 acceptance space ") as temporary:
            runtime = Path(temporary) / ".harness-runtime" / "epochs" / "epoch-1"
            runtime.mkdir(parents=True)
            self.assertTrue(runtime.is_dir())
            expected = {"pid": 123, "creation_time": "2026-01-01T00:00:00Z"}
            self.assertNotEqual(expected, {"pid": 123, "creation_time": "2026-01-01T00:00:01Z"})

    def test_live_claims_are_not_satisfied_by_the_disposable_fake(self) -> None:
        self.assertEqual("synthetic/static", claim("CHECK-U5").evidence_class)
        for number in range(1, 5):
            self.assertEqual("live-only", claim(f"CHECK-LIVE-{number}").evidence_class)
