from __future__ import annotations

import unittest

from orchestrator_harness.tests.v2_acceptance.audited_gap_inventory import (
    AUDITED_GAP_IDS,
    AUTHORITY,
)
from orchestrator_harness.tests.v2_acceptance.gap_map import GAP_ORACLES


class GapMapTests(unittest.TestCase):
    """CHECK-ASSET-DISCOVERY: no audited [!] row can silently lose an oracle."""

    def test_every_authoritative_snapshot_gap_has_one_strong_observable_oracle(self) -> None:
        oracle_ids = [item.gap_id for item in GAP_ORACLES]
        self.assertEqual(82, len(AUDITED_GAP_IDS))
        self.assertEqual(82, len(set(AUDITED_GAP_IDS)))
        self.assertEqual(set(AUDITED_GAP_IDS), set(oracle_ids))
        self.assertEqual(len(AUDITED_GAP_IDS), len(oracle_ids))
        self.assertIn("MASTER-SPEC-IMPLEMENTATION-CHECKLIST.md", AUTHORITY.source)
        self.assertEqual(64, len(AUTHORITY.sha256))
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
