from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import shutdown
from orchestrator_harness.config import HarnessConfig


class ShutdownQueueGuardTests(unittest.TestCase):
    def test_open_managed_runtime_refuses_pending_or_acknowledged_queue_work(self) -> None:
        for state in ("PENDING", "ACKNOWLEDGED"):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                workspace = root / "workspace"
                workspace.mkdir()
                config = HarnessConfig(root, workspace, "enabled")
                with (
                    patch.object(shutdown, "find_harness_root", return_value=root),
                    patch.object(shutdown, "load_config", return_value=config),
                    patch.object(
                        shutdown,
                        "read_runtime_state",
                        return_value={"schema": "runtime-state/v1", "state": "OPEN"},
                    ),
                    patch.object(
                        shutdown,
                        "read_current_epoch",
                        return_value={"epoch_id": "epoch-1", "queue_id": "queue-1"},
                    ),
                    patch.object(
                        shutdown,
                        "read_manager_queue",
                        return_value={
                            "schema": "manager-queue/v1",
                            "events": [{"event_id": "event-1", "state": state}],
                        },
                    ),
                    patch.object(shutdown, "atomic_write_json") as write_state,
                ):
                    result = shutdown.run_shutdown()

                self.assertFalse(result["ok"])
                self.assertEqual(
                    shutdown.SHUTDOWN_UNRESOLVED_MANAGER_OBLIGATIONS,
                    result["code"],
                )
                self.assertIn("event-1", result["summary"])
                write_state.assert_not_called()


if __name__ == "__main__":
    unittest.main()
