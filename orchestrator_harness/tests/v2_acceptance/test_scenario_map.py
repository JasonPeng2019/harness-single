from __future__ import annotations

import unittest

from orchestrator_harness.tests.v2_acceptance.scenario_map import SCENARIO_FAMILIES, requirement_to_tests


class ScenarioMapTests(unittest.TestCase):
    def test_every_addendum_requirement_has_an_executable_family_and_named_test(self) -> None:
        mapped = requirement_to_tests()
        self.assertEqual({f"REQ-{number:03d}" for number in range(1, 18)}, set(mapped))
        self.assertEqual(len(SCENARIO_FAMILIES), len({family.name for family in SCENARIO_FAMILIES}))
        for requirement, tests in mapped.items():
            with self.subTest(requirement=requirement):
                self.assertTrue(tests)
                self.assertTrue(all(test.startswith("test_") for test in tests))
