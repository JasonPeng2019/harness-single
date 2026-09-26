from __future__ import annotations

import unittest
from pathlib import Path

from orchestrator_harness.tests.v2_acceptance.reference_runtime import ReferenceRuntime


class CheckU1Tests(unittest.TestCase):
    """CHECK-U1 — REQ-001/004/005/006 synthetic lifecycle oracle."""

    def test_epoch_has_one_identity_and_lane_ids_never_reuse(self) -> None:
        runtime = ReferenceRuntime()
        runtime.bootstrap("layout", "run-1")
        with self.assertRaisesRegex(ValueError, "BOOTSTRAP_LANE_ID_IN_USE"):
            runtime.bootstrap("layout", "run-2")
        self.assertEqual("epoch-1", runtime.epoch_id)
        self.assertEqual("prepared", runtime.lanes["layout"]["lifecycle"])

    def test_shipped_source_has_the_normative_v2_setup_inputs_and_adapter_roots(self) -> None:
        """This is a source-layout acceptance check, not a candidate API import."""
        root = Path(__file__).resolve().parents[3]
        required = (
            "examples/harness-config.example.json",
            "examples/resource-manifest.example.json",
            "adapters",
            "super-cache",
            "orchestrator_harness/provider_adapters",
        )
        missing = [relative for relative in required if not (root / relative).exists()]
        self.assertEqual([], missing, f"normative v2 shipped layout is missing: {missing}")

    def test_setup_configuration_is_an_epoch_boundary_not_a_per_lane_override(self) -> None:
        managed = ReferenceRuntime(mode="managed", epoch_id="epoch-managed")
        plain = ReferenceRuntime(mode="plain", epoch_id="epoch-plain")
        self.assertNotEqual(managed.epoch_id, plain.epoch_id)
        self.assertNotEqual(managed.mode, plain.mode)
        self.assertEqual([], managed.events)
        self.assertEqual([], plain.events)
