from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import shutdown, setup
from orchestrator_harness.config import HarnessConfig, ResourceManifest
from orchestrator_harness.epochs import open_epoch
from orchestrator_harness.manager_queue import acknowledge_event, promote_event


class ShutdownQueueGuardTests(unittest.TestCase):
    def test_open_managed_runtime_refuses_pending_or_acknowledged_queue_work(self) -> None:
        for state in ("PENDING", "ACKNOWLEDGED"):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                workspace = root / "workspace"
                workspace.mkdir()
                config = HarnessConfig(root, workspace, "enabled")
                setup.set_runtime_state(config.runtime_root, "OPEN")
                open_epoch(config.runtime_root, config, ResourceManifest())
                event = promote_event(config.runtime_root, event_type="status", lane_id="lane",
                                      run_id="run", summary="ROOT decision remains open")
                if state == "ACKNOWLEDGED":
                    acknowledge_event(config.runtime_root, event["event_id"])
                with (
                    patch.object(shutdown, "find_harness_root", return_value=root),
                    patch.object(shutdown, "load_config", return_value=config),
                    patch.object(shutdown, "atomic_write_json") as write_state,
                ):
                    result = shutdown.run_shutdown()

                self.assertFalse(result["ok"])
                self.assertEqual(
                    shutdown.SHUTDOWN_UNRESOLVED_MANAGER_OBLIGATIONS,
                    result["code"],
                )
                self.assertIn(event["event_id"], result["summary"])
                write_state.assert_not_called()


if __name__ == "__main__":
    unittest.main()
