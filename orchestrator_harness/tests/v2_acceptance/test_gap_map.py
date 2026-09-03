from __future__ import annotations

import unittest

from orchestrator_harness.tests.v2_acceptance.gap_map import GAP_ORACLES


class GapMapTests(unittest.TestCase):
    """CHECK-ASSET-DISCOVERY: no audited [!] row can silently lose an oracle."""

    def test_exactly_the_82_audited_gaps_have_strong_observable_oracles(self) -> None:
        ids = [item.gap_id for item in GAP_ORACLES]
        self.assertEqual(82, len(ids))
        self.assertEqual(82, len(set(ids)))
        for item in GAP_ORACLES:
            with self.subTest(item.gap_id):
                self.assertTrue(item.check.startswith("CHECK-"))
                self.assertTrue(item.test.startswith("test_"))
                self.assertTrue(item.scenario and item.trigger and item.expected)
                self.assertTrue(item.cleanup and item.invariant)

    def test_authoritative_interpretations_are_owned_by_their_scenarios(self) -> None:
        by_id = {item.gap_id: item for item in GAP_ORACLES}
        self.assertIn("BOUND-009/010", by_id["G12a"].expected)
        self.assertEqual("CHECK-U4", by_id["I13"].check)
        self.assertEqual("CHECK-U4", by_id["I14"].check)
