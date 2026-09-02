from __future__ import annotations

import unittest

from orchestrator_harness.tests.v2_acceptance.contract import CLAIMS, covered_requirements


class ClaimMapTests(unittest.TestCase):
    """CHECK-ASSET-DISCOVERY: the explicit mapping is itself executable."""

    def test_every_check_has_independent_scenario_trigger_oracle_and_cleanup(self) -> None:
        names = {item.name for item in CLAIMS}
        self.assertEqual({f"CHECK-U{number}" for number in range(1, 6)} | {f"CHECK-LIVE-{number}" for number in range(1, 5)}, names)
        for item in CLAIMS:
            with self.subTest(item.name):
                self.assertTrue(item.requirements)
                self.assertTrue(item.scenario and item.trigger and item.expected)
                self.assertTrue(item.oracle and item.cleanup)
                self.assertIn(item.evidence_class, {"synthetic/static", "synthetic", "live-only"})

    def test_static_assets_cover_every_non_live_requirement_and_live_is_reserved(self) -> None:
        self.assertTrue({f"REQ-{number:03d}" for number in range(1, 17)} <= covered_requirements())
        live = [item for item in CLAIMS if item.evidence_class == "live-only"]
        self.assertEqual(["CHECK-LIVE-1", "CHECK-LIVE-2", "CHECK-LIVE-3", "CHECK-LIVE-4"], [item.name for item in live])
        self.assertEqual({"REQ-017"}, covered_requirements(live))
