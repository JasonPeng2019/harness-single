from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
import warnings
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from pathlib import Path

from orchestrator_harness.operator_launch import detached_owner_snapshot, launch_process
from orchestrator_harness.processes import process_snapshot
from orchestrator_harness.models import iso_utc


class OperatorLaunchTests(unittest.TestCase):
    def _wait_for_exact(self, pid: int, created: str | None = None) -> bool:
        for _ in range(20):
            item = process_snapshot().by_pid.get(pid)
            if item is not None and item.created_utc is not None:
                return True
            time.sleep(.05)
        return False

    def _wait_for_no_detached_owners(self) -> None:
        for _ in range(100):
            if not detached_owner_snapshot():
                return
            time.sleep(.05)
        self.fail(f"detached reaper ownership did not drain: {detached_owner_snapshot()}")

    def test_invalid_and_existing_receipts_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            receipt = root / "receipt.json"
            with self.assertRaises(ValueError):
                launch_process(receipt=receipt, label="", role="test", cwd=root, argv=[sys.executable])
            receipt.write_text("{}", encoding="utf-8")
            with self.assertRaises(ValueError):
                launch_process(receipt=receipt, label="test", role="test", cwd=root, argv=[sys.executable, "-c", "pass"])

    def test_failed_identity_proof_reaps_exact_spawned_child(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            receipt = root / "failed.json"
            captured: dict[str, subprocess.Popen[str]] = {}
            real_popen = subprocess.Popen

            def capture(*args, **kwargs):
                child = real_popen(*args, **kwargs)
                captured["child"] = child
                return child

            with patch("orchestrator_harness.operator_launch.subprocess.Popen", side_effect=capture), patch(
                "orchestrator_harness.operator_launch._creation_identity", return_value=None
            ):
                with self.assertRaises(RuntimeError):
                    launch_process(
                        receipt=receipt, label="forced-failure", role="test", cwd=root,
                        argv=[sys.executable, "-c", "import time; time.sleep(30)"],
                    )
            child = captured["child"]
            self.assertIsNotNone(child.returncode)
            self.assertIsNone(process_snapshot().by_pid.get(child.pid))
            failure = json.loads(receipt.read_text(encoding="utf-8"))
            self.assertEqual("failed", failure["status"])
            self.assertTrue(failure["cleanup_confirmed"])

    def test_child_survives_launch_cli_and_receipt_has_identity(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            receipt = root / "receipt.json"
            command = [
                sys.executable, "-m", "orchestrator_harness.operator_launch",
                "--receipt", str(receipt), "--label", "smoke", "--role", "test",
                "--cwd", str(root), "--", sys.executable, "-c", "import time; time.sleep(30)",
            ]
            completed = subprocess.run(command, cwd=str(Path(__file__).resolve().parents[2]), text=True, capture_output=True, check=False)
            self.assertEqual(0, completed.returncode, completed.stderr)
            data = json.loads(receipt.read_text(encoding="utf-8"))
            self.assertEqual("launched", data["status"])
            self.assertTrue(self._wait_for_exact(data["pid"], data["created_utc"]))
            try:
                exact = process_snapshot().by_pid.get(data["pid"])
                self.assertIsNotNone(exact)
                self.assertEqual(data["created_utc"], iso_utc(exact.created_utc))
                if os.name == "nt":
                    subprocess.run(["taskkill", "/PID", str(data["pid"]), "/T", "/F"], check=False, capture_output=True)
                else:
                    os.kill(data["pid"], 15)
            finally:
                for _ in range(20):
                    if process_snapshot().by_pid.get(data["pid"]) is None:
                        break
                    time.sleep(.05)
                self.assertIsNone(process_snapshot().by_pid.get(data["pid"]))

    def test_supported_reaper_waits_for_natural_exit_without_resource_warning(self) -> None:
        with tempfile.TemporaryDirectory() as raw, warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", ResourceWarning)
            root = Path(raw)
            result = launch_process(
                receipt=root / "natural.json",
                label="natural",
                role="test",
                cwd=root,
                argv=[sys.executable, "-c", "import time; time.sleep(2)"],
            )
            self.assertTrue(detached_owner_snapshot())
            self._wait_for_no_detached_owners()
            self.assertEqual(0, len([item for item in caught if item.category is ResourceWarning]))
            self.assertIsNone(process_snapshot().by_pid.get(result["pid"]))

    def test_concurrent_detached_reapers_are_bounded_and_observable(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)

            def launch(index: int) -> dict[str, object]:
                return launch_process(
                    receipt=root / f"concurrent-{index}.json",
                    label=f"concurrent-{index}",
                    role="test",
                    cwd=root,
                    argv=[sys.executable, "-c", "import time; time.sleep(2)"],
                )

            with ThreadPoolExecutor(max_workers=4) as executor:
                results = list(executor.map(launch, range(4)))
            self.assertEqual(4, len({(item["pid"], item["created_utc"]) for item in results}))
            self.assertLessEqual(len(detached_owner_snapshot()), 4)
            self._wait_for_no_detached_owners()


if __name__ == "__main__":
    unittest.main()
