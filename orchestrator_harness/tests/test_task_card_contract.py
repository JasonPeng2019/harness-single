"""Contract tests for required project task-card outcome fields."""
from __future__ import annotations

import copy
import json
import re
import tempfile
import unittest
from pathlib import Path

from orchestrator_harness import bootstrap, resume, review


VALID_CARD = {
    "schema": "project-task-card/v1",
    "task": "Implement the task-card contract",
    "acceptance_criteria": [
        "Invalid cards fail before lane work begins",
        "The worker prompt states the completion conditions",
    ],
    "deliverables": [
        "Shared task-card validation",
        "Focused regression tests",
    ],
    "reason_for_acceptance_and_deliverables": (
        "The criteria define success and the deliverables make it reviewable."
    ),
}


class TaskCardContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.worktree = Path(self.temporary.name) / "worktree"
        self.card_path = self.worktree / ".agent-workspace" / "task-card.json"
        self.card_path.parent.mkdir(parents=True)

    def _write_card(self, card: dict[str, object]) -> None:
        self.card_path.write_text(json.dumps(card), encoding="utf-8")

    def _readers(self):
        return (
            (
                "bootstrap",
                lambda: bootstrap._read_task_card(self.card_path),
                bootstrap.BootstrapError,
            ),
            ("resume", lambda: resume._read_task_card(self.card_path), ValueError),
            (
                "review",
                lambda: review._read_task_card(self.worktree),
                review.ReviewError,
            ),
        )

    def test_all_readers_accept_the_complete_contract(self) -> None:
        self._write_card(VALID_CARD)

        for reader_name, read, _error_type in self._readers():
            with self.subTest(reader=reader_name):
                self.assertEqual(VALID_CARD, read())

    def test_all_readers_reject_missing_required_outcome_fields(self) -> None:
        for field in (
            "acceptance_criteria",
            "deliverables",
            "reason_for_acceptance_and_deliverables",
        ):
            card = copy.deepcopy(VALID_CARD)
            del card[field]
            self._write_card(card)
            for reader_name, read, error_type in self._readers():
                with self.subTest(field=field, reader=reader_name):
                    with self.assertRaisesRegex(error_type, re.escape(field)):
                        read()

    def test_all_readers_reject_malformed_required_outcome_fields(self) -> None:
        invalid_values = {
            "acceptance_criteria": ([], [""], ["valid", 7], "not a list"),
            "deliverables": ([], ["  "], [None], "not a list"),
            "reason_for_acceptance_and_deliverables": ("", "  ", [], None),
        }
        for field, values in invalid_values.items():
            for value in values:
                card = copy.deepcopy(VALID_CARD)
                card[field] = value
                self._write_card(card)
                for reader_name, read, error_type in self._readers():
                    with self.subTest(field=field, value=value, reader=reader_name):
                        with self.assertRaisesRegex(error_type, re.escape(field)):
                            read()

    def test_worker_prompt_renders_all_outcome_fields(self) -> None:
        path = bootstrap._write_worker_prompt(
            self.worktree,
            VALID_CARD,
            managed=False,
        )

        self.assertEqual(
            """Implement the task-card contract

## Acceptance criteria
- Invalid cards fail before lane work begins
- The worker prompt states the completion conditions

## Deliverables
- Shared task-card validation
- Focused regression tests

## Reason for acceptance and deliverables
The criteria define success and the deliverables make it reviewable.
""",
            path.read_text(encoding="utf-8"),
        )


if __name__ == "__main__":
    unittest.main()
