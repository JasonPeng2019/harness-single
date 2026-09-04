from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from orchestrator_harness import manager_queue
from orchestrator_harness.epochs import (
    CURRENT_EPOCH_SCHEMA,
    MANAGER_QUEUE_SCHEMA,
    current_epoch_path,
    manager_queue_path,
)
from orchestrator_harness.records import RecordLock, atomic_write_json


class ManagerQueueAtomicityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.runtime = Path(self.temporary.name)
        atomic_write_json(
            current_epoch_path(self.runtime),
            {
                "schema": CURRENT_EPOCH_SCHEMA,
                "epoch_id": "epoch-1",
                "queue_id": "queue-1",
            },
        )
        atomic_write_json(
            manager_queue_path(self.runtime),
            {
                "schema": MANAGER_QUEUE_SCHEMA,
                "epoch_id": "epoch-1",
                "queue_id": "queue-1",
                "events": [
                    {
                        "event_id": "event-1",
                        "state": "ACKNOWLEDGED",
                        "summary": "pending review",
                        "history": [
                            {"state": "ACKNOWLEDGED", "at": "earlier"}
                        ],
                        "delivery_history": [],
                    }
                ],
            },
        )

    def test_queue_read_header_check_and_replace_share_one_lock(self) -> None:
        held = False
        queue_path = manager_queue_path(self.runtime).absolute()
        real_read_record = manager_queue.read_record
        real_read_epoch = manager_queue.read_current_epoch
        real_atomic_write = manager_queue.atomic_write_json

        class TrackingLock:
            def __init__(self, path: Path) -> None:
                self._real = RecordLock(path)

            def __enter__(self) -> "TrackingLock":
                nonlocal held
                self._real.__enter__()
                held = True
                return self

            def __exit__(self, *args: object) -> None:
                nonlocal held
                held = False
                self._real.__exit__(*args)

        def guarded_read(path: Path, schema: str) -> dict[str, object]:
            if Path(path).absolute() == queue_path:
                self.assertTrue(held, "queue read must occur under queue lock")
            return real_read_record(path, schema)

        def guarded_epoch(rt: Path) -> dict[str, object] | None:
            self.assertTrue(held, "queue header validation must occur under queue lock")
            return real_read_epoch(rt)

        def guarded_write(path: Path, value: object) -> None:
            if Path(path).absolute() == queue_path:
                self.assertTrue(held, "queue replacement must occur under queue lock")
            real_atomic_write(path, value)

        with (
            mock.patch.object(manager_queue, "RecordLock", TrackingLock),
            mock.patch.object(manager_queue, "read_record", side_effect=guarded_read),
            mock.patch.object(
                manager_queue, "read_current_epoch", side_effect=guarded_epoch
            ),
            mock.patch.object(
                manager_queue, "atomic_write_json", side_effect=guarded_write
            ),
        ):
            manager_queue.append_delivery_history(self.runtime, "event-1")
            manager_queue.close_event(
                self.runtime,
                "event-1",
                "COMPLETE",
                summary="review accepted",
            )

        record = manager_queue.read_manager_queue(self.runtime)
        event = record["events"][0]
        self.assertEqual("COMPLETE", event["state"])
        self.assertEqual(1, len(event["delivery_history"]))
        self.assertFalse(held)


if __name__ == "__main__":
    unittest.main()
