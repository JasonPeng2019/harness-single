from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
import unittest
import warnings
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from examples.disposable_coding_fixture import _invocation
from orchestrator_harness.models import iso_utc
from orchestrator_harness.operator_launch import detached_owner_snapshot, launch_process
from orchestrator_harness.processes import process_snapshot


class OperatorLaunchTests(unittest.TestCase):
    def _wait_for_exact(self, pid: int, created: str | None = None):
        for _ in range(20):
            item = process_snapshot().by_pid.get(pid)
            if (
                item is not None
                and item.created_utc is not None
                and (created is None or iso_utc(item.created_utc) == created)
            ):
                return item
            time.sleep(0.05)
        return None

    def _wait_for_no_detached_owners(self) -> None:
        for _ in range(100):
            if not detached_owner_snapshot():
                return
            time.sleep(0.05)
        self.fail(
            f"detached reaper ownership did not drain: {detached_owner_snapshot()}"
        )

    def test_invalid_and_existing_receipts_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            receipt = root / "receipt.json"
            with self.assertRaises(ValueError):
                launch_process(
                    receipt=receipt,
                    label="",
                    role="test",
                    cwd=root,
                    argv=[sys.executable],
                )
            receipt.write_text("{}", encoding="utf-8")
            with self.assertRaises(ValueError):
                launch_process(
                    receipt=receipt,
                    label="test",
                    role="test",
                    cwd=root,
                    argv=[sys.executable, "-c", "pass"],
                )

    def test_failed_identity_proof_reaps_exact_spawned_child(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            receipt = root / "failed.json"
            with (
                patch(
                    "orchestrator_harness.operator_launch._creation_identity",
                    return_value=None,
                ),
                self.assertRaises(RuntimeError),
            ):
                launch_process(
                    receipt=receipt,
                    label="forced-failure",
                    role="test",
                    cwd=root,
                    argv=[sys.executable, "-c", "import time; time.sleep(30)"],
                )
            failure = json.loads(receipt.read_text(encoding="utf-8"))
            self.assertEqual("failed", failure["status"])
            self.assertTrue(failure["cleanup_confirmed"])
            self.assertIsNone(process_snapshot().by_pid.get(failure["child_pid"]))

    def test_child_survives_launch_cli_and_receipt_has_identity(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            receipt = root / "receipt.json"
            command = [
                sys.executable,
                "-W",
                "error::ResourceWarning",
                "-m",
                "orchestrator_harness.operator_launch",
                "--receipt",
                str(receipt),
                "--label",
                "smoke",
                "--role",
                "test",
                "--cwd",
                str(root),
                "--",
                sys.executable,
                "-c",
                "import time; time.sleep(2)",
            ]
            completed = subprocess.run(
                command,
                cwd=str(Path(__file__).resolve().parents[2]),
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(0, completed.returncode, completed.stderr)
            data = json.loads(receipt.read_text(encoding="utf-8"))
            self.assertEqual("launched", data["status"])
            exact = self._wait_for_exact(data["pid"], data["created_utc"])
            self.assertIsNotNone(exact)
            assert exact is not None
            self.assertEqual(data["created_utc"], iso_utc(exact.created_utc))
            for _ in range(40):
                if process_snapshot().by_pid.get(data["pid"]) is None:
                    break
                time.sleep(0.05)
            self.assertIsNone(process_snapshot().by_pid.get(data["pid"]))

    def test_native_ownership_waits_for_natural_exit_without_resource_warning(
        self,
    ) -> None:
        with (
            tempfile.TemporaryDirectory() as raw,
            warnings.catch_warnings(record=True) as caught,
        ):
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
            for _ in range(40):
                if not detached_owner_snapshot():
                    break
                time.sleep(0.05)
            self.assertFalse(detached_owner_snapshot())
            self.assertEqual(
                0, len([item for item in caught if item.category is ResourceWarning])
            )
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
            self.assertEqual(
                4, len({(item["pid"], item["created_utc"]) for item in results})
            )
            self.assertLessEqual(len(detached_owner_snapshot()), 4)
            self._wait_for_no_detached_owners()

    def test_public_route_entry_returns_without_waiting_and_naturally_reaps(
        self,
    ) -> None:
        with (
            tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as raw,
            warnings.catch_warnings(record=True) as caught,
        ):
            warnings.simplefilter("error", ResourceWarning)
            root = Path(raw)
            repo = root / "repo"
            repo.mkdir()
            for command in (
                ("git", "init", "-b", "route"),
                ("git", "config", "user.email", "route@example.invalid"),
                ("git", "config", "user.name", "Route Test"),
            ):
                subprocess.run(command, cwd=repo, check=True, capture_output=True)
            (repo / "task.txt").write_text("route\n", encoding="utf-8")
            subprocess.run(
                ("git", "add", "task.txt"), cwd=repo, check=True, capture_output=True
            )
            subprocess.run(
                ("git", "commit", "-m", "route base"),
                cwd=repo,
                check=True,
                capture_output=True,
            )
            runtime = root / "runtime"
            runtime.mkdir()
            base = subprocess.run(
                ("git", "rev-parse", "HEAD"),
                cwd=repo,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            invocation = _invocation(
                lane="beta-success",
                worktree=repo,
                common_dir=repo / ".git",
                base_commit=base,
                runtime=runtime,
                delay=0.2,
            )
            receipt = repo / ".agent-workspace" / "route-entry.receipt.json"
            status = repo / ".agent-workspace" / "fixture_controller.status.json"
            code = (
                "from orchestrator_harness.public_launch import launch_lane_controller; "
                f"launch_lane_controller({str(invocation)!r}, receipt={str(receipt)!r}, cwd={str(Path(__file__).resolve().parents[2])!r}, expected_state_path={str(status)!r})"
            )
            completed = subprocess.run(
                [sys.executable, "-W", "error::ResourceWarning", "-c", code],
                cwd=Path(__file__).resolve().parents[2],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, completed.returncode, completed.stderr)
            value: dict[str, object] = {}
            receipt_value: dict[str, object] = {}
            deadline = time.monotonic() + 60.0
            while time.monotonic() < deadline:
                value = (
                    json.loads(status.read_text(encoding="utf-8"))
                    if status.exists()
                    else {}
                )
                receipt_value = (
                    json.loads(receipt.read_text(encoding="utf-8"))
                    if receipt.exists()
                    else {}
                )
                terminal = (
                    value.get("state")
                    in {
                        "CODEX_EXITED",
                        "PROVIDER_EXITED",
                        "CONTROLLER_FAILED",
                        "LAUNCH_FAILED",
                    }
                    and value.get("ended_utc") is not None
                )
                if (
                    terminal
                    and process_snapshot().by_pid.get(receipt_value.get("pid")) is None
                ):
                    break
                time.sleep(0.1)
            self.assertEqual("CODEX_EXITED", value.get("state"), value)
            self.assertEqual(receipt_value.get("pid"), value.get("controller_pid"))
            self.assertEqual(
                receipt_value.get("created_utc"), value.get("controller_created_utc")
            )
            self.assertIsNone(process_snapshot().by_pid.get(receipt_value.get("pid")))
            time.sleep(0.5)
            self.assertEqual(0, len(caught))


if __name__ == "__main__":
    unittest.main()
