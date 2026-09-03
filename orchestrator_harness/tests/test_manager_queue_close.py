from __future__ import annotations

import copy
import unittest
from pathlib import Path
from unittest import mock

from orchestrator_harness import manager_queue


class ManagerQueueCloseTests(unittest.TestCase):
    def _record(self) -> dict[str, object]:
        return {
            "events": [
                {
                    "event_id": "event-1",
                    "state": "ACKNOWLEDGED",
                    "history": [{"state": "ACKNOWLEDGED", "at": "earlier"}],
                }
            ]
        }

    def test_blank_summary_rejects_before_queue_access_or_mutation(self) -> None:
        runtime = Path("runtime")
        for summary in (None, "", "   "):
            record = self._record()
            original = copy.deepcopy(record)
            with (
                mock.patch.object(
                    manager_queue, "read_manager_queue", return_value=record
                ) as read_queue,
                mock.patch.object(
                    manager_queue, "_write_manager_queue"
                ) as write_queue,
            ):
                with self.subTest(summary=summary):
                    with self.assertRaises(
                        manager_queue.ManagerQueueError
                    ) as raised:
                        manager_queue.close_event(
                            runtime, "event-1", "COMPLETE", summary=summary
                        )
                self.assertEqual(
                    manager_queue.MANAGER_CLOSE_SUMMARY_REQUIRED,
                    raised.exception.code,
                )
                read_queue.assert_not_called()
                write_queue.assert_not_called()
            self.assertEqual(original, record)

    def test_terminal_summary_is_normalized_and_persisted(self) -> None:
        runtime = Path("runtime")
        for outcome in ("COMPLETE", "BLOCKED"):
            record = self._record()
            with (
                mock.patch.object(
                    manager_queue, "read_manager_queue", return_value=record
                ),
                mock.patch.object(
                    manager_queue, "_write_manager_queue"
                ) as write_queue,
            ):
                event = manager_queue.close_event(
                    runtime,
                    "event-1",
                    outcome,
                    summary="  operator decision  ",
                )
            self.assertEqual(outcome, event["state"])
            self.assertEqual("operator decision", event["summary"])
            self.assertEqual(outcome, event["history"][-1]["state"])
            self.assertEqual(
                "operator decision", event["history"][-1]["summary"]
            )
            write_queue.assert_called_once_with(runtime, record)


if __name__ == "__main__":
    unittest.main()
