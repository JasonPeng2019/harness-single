from __future__ import annotations
# pyright: reportImplicitRelativeImport=false

import hashlib
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

from orchestrator_harness.discovery import discover_suite
from orchestrator_harness.processes import process_snapshot
from orchestrator_harness.reconcile import reconcile
from orchestrator_harness.tests.support import (
    SuiteFixture,
    hash_file,
    launch_synthetic_controller,
    write_json,
)


class RealProcessIntegrationTests(unittest.TestCase):
    fixture: SuiteFixture  # pyright: ignore[reportUninitializedInstanceVariable]

    def setUp(self) -> None:
        self.fixture = SuiteFixture.create()

    def tearDown(self) -> None:
        self.fixture.close()

    def _wait_for(self, path: Path, process, timeout: float = 15) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if path.exists():
                return
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                self.fail(
                    f"controller exited early: {process.returncode}\n{stdout}\n{stderr}"
                )
            time.sleep(0.05)
        self.fail(f"timed out waiting for {path}")

    def test_real_process_request_relay_checkpoint_exit(self) -> None:
        workspace = self.fixture.workspace("T00_real")
        controller = launch_synthetic_controller(workspace)
        request_path = workspace / "permission-requests" / "synthetic-request.json"
        status_path = workspace / "synthetic_controller.status.json"
        try:
            self._wait_for(request_path, controller)
            before = {path: hash_file(path) for path in (request_path, status_path)}
            observed = reconcile(
                discover_suite(self.fixture.config),
                process_snapshot(),
                self.fixture.config,
                now=datetime.now(timezone.utc),
            )
            lane = observed["lanes"][0]
            request = observed["requests"][0]
            self.assertIn(lane["operational_state"], {"RUNNING_CODEX", "WAITING_RELAY"})
            self.assertEqual("RELAY_READY", request["operational_state"])
            self.assertEqual(before, {path: hash_file(path) for path in before})

            relay_path = (
                workspace / "permission-requests" / "synthetic-request.relay.json"
            )
            request_hash = hashlib.sha256(request_path.read_bytes()).hexdigest()
            write_json(
                relay_path,
                {
                    "decision": "approved",
                    "request_sha256": request_hash,
                    "run_id": "synthetic-run",
                    "session_id": "synthetic-session",
                },
            )
            controller.wait(timeout=20)
            controller.communicate(timeout=1)
            self.assertEqual(0, controller.returncode)
            self.assertTrue((workspace / "PARALLEL_CHECKPOINT.md").exists())

            final = reconcile(
                discover_suite(self.fixture.config),
                process_snapshot(),
                self.fixture.config,
                now=datetime.now(timezone.utc),
            )
            self.assertEqual("CHECKPOINTED", final["lanes"][0]["operational_state"])
            self.assertEqual(
                "RELAYED_INACTIVE", final["requests"][0]["operational_state"]
            )
        finally:
            if controller.poll() is None:
                controller.terminate()
                controller.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
