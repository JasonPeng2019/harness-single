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
from pathlib import Path
from unittest.mock import patch

from examples.disposable_coding_fixture import _invocation
from orchestrator_harness import operator_launch
from orchestrator_harness.models import iso_utc
from orchestrator_harness.operator_launch import detached_owner_snapshot, launch_process
from orchestrator_harness.processes import process_snapshot


class OperatorLaunchTests(unittest.TestCase):
    def _windows_api(self):
        if os.name != "nt":
            self.skipTest("Windows native launch is only available on Windows")
        import _winapi  # type: ignore[import-not-found]

        return _winapi

    @staticmethod
    def _native_error(winerror: int) -> OSError:
        error = OSError("native CreateProcess failure")
        error.winerror = winerror
        return error

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

    def test_access_denied_retries_without_breakaway_and_publishes_before_resume(
        self,
    ) -> None:
        winapi = self._windows_api()
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            receipt = root / "retry.json"
            argv = ("worker.exe", "argument with spaces")
            calls: list[tuple[object, ...]] = []
            events: list[str] = []
            closed: list[object] = []
            process_handle = object()
            thread_handle = object()
            pid = 424242

            def create_process(*args: object) -> tuple[object, object, int, int]:
                calls.append(args)
                events.append(f"create-{len(calls)}")
                if len(calls) == 1:
                    raise self._native_error(5)
                return process_handle, thread_handle, pid, 777

            def creation_identity(_pid: int) -> str:
                events.append("identity")
                return "2026-08-20T22:36:00.000000Z"

            observed_at_resume: dict[str, object] = {}

            def resume(_thread: object) -> None:
                events.append("resume")
                if receipt.exists():
                    observed_at_resume.update(
                        json.loads(receipt.read_text(encoding="utf-8"))
                    )

            def close(handle: object) -> None:
                events.append("close")
                closed.append(handle)

            with (
                patch.object(winapi, "CreateProcess", side_effect=create_process),
                patch.object(winapi, "CloseHandle", side_effect=close),
                patch.object(
                    operator_launch, "_creation_identity", side_effect=creation_identity
                ),
                patch.object(
                    operator_launch, "_resume_windows_thread", side_effect=resume
                ),
                patch.dict(operator_launch._DETACHED_RECORDS, {}, clear=True),
            ):
                result = launch_process(
                    receipt=receipt,
                    label="retry",
                    role="test",
                    cwd=root,
                    argv=argv,
                )

            self.assertEqual(2, len(calls))
            first_flags = calls[0][5]
            second_flags = calls[1][5]
            assert isinstance(first_flags, int)
            assert isinstance(second_flags, int)
            self.assertEqual(
                second_flags,
                first_flags & ~winapi.CREATE_BREAKAWAY_FROM_JOB,
            )
            self.assertTrue(second_flags & 0x00000004)
            self.assertIsNone(calls[0][0])
            self.assertEqual(subprocess.list2cmdline(list(argv)), calls[0][1])
            self.assertEqual(
                ["create-1", "create-2", "identity", "resume", "close", "close"],
                events,
            )
            self.assertEqual([thread_handle, process_handle], closed)
            receipt_value = json.loads(receipt.read_text(encoding="utf-8"))
            self.assertEqual("launched", receipt_value["status"])
            self.assertEqual(second_flags, result["creationflags"])
            self.assertEqual(second_flags, receipt_value["creationflags"])
            self.assertEqual(
                "windows-native-detached-inherited-job-no-wait",
                receipt_value["ownership_strategy"],
            )
            self.assertEqual(receipt_value, observed_at_resume)

    def test_non_access_denied_native_error_attempts_once_and_writes_failed_receipt(
        self,
    ) -> None:
        winapi = self._windows_api()
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            receipt = root / "failed-native.json"
            calls: list[tuple[object, ...]] = []
            closed: list[object] = []

            def create_process(*args: object) -> object:
                calls.append(args)
                raise self._native_error(123)

            with (
                patch.object(winapi, "CreateProcess", side_effect=create_process),
                patch.object(winapi, "CloseHandle", side_effect=closed.append),
                patch.object(
                    operator_launch,
                    "_creation_identity",
                    side_effect=AssertionError("identity must not run"),
                ),
                patch.dict(operator_launch._DETACHED_RECORDS, {}, clear=True),
                self.assertRaises(OSError),
            ):
                launch_process(
                    receipt=receipt,
                    label="failed-native",
                    role="test",
                    cwd=root,
                    argv=("worker.exe",),
                )

            self.assertEqual(1, len(calls))
            self.assertEqual([], closed)
            failure = json.loads(receipt.read_text(encoding="utf-8"))
            self.assertEqual("failed", failure["status"])
            self.assertIsNone(failure["child_pid"])
            self.assertIsNone(failure["cleanup_confirmed"])
            self.assertEqual(
                "windows-native-detached-no-wait",
                failure["ownership_strategy"],
            )

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
