from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from harness_common import process_identity as common_identity
from orchestrator_harness import controller, monitor, processes
from orchestrator_harness.core import content_hash
from orchestrator_harness.models import ProcessInfo, ProcessQuery, ProcessSnapshot
from orchestrator_harness.records import atomic_write_json, read_record


TEST_TEMP_ROOT = Path(__file__).resolve().parents[2] / ".agent-workspace" / "test-temp"


class ProcessIdentityCorrectionTests(unittest.TestCase):
    def test_windows_macos_and_linux_identity_dispatches_are_exact(self) -> None:
        cases = (
            ("nt", "win32", "windows-filetime:101"),
            ("posix", "darwin", "darwin-start-time:10:20"),
            ("posix", "linux", "linux-start-ticks:303"),
        )
        for os_name, platform, creation in cases:
            with self.subTest(platform=platform):
                if platform == "win32":
                    backend = patch.object(
                        common_identity,
                        "_windows_process_identity",
                        return_value={"pid": 41, "created_utc": creation},
                    )
                elif platform == "darwin":
                    backend = patch.object(
                        common_identity,
                        "darwin_process_info",
                        return_value={"pid": 41, "creation_identity": creation},
                    )
                else:
                    backend = patch.object(
                        common_identity,
                        "_linux_process_identity",
                        return_value={"pid": 41, "created_utc": creation},
                    )
                with (
                    patch.object(common_identity.os, "name", os_name),
                    patch.object(common_identity.sys, "platform", platform),
                    backend,
                ):
                    self.assertEqual(
                        {"pid": 41, "created_utc": creation},
                        common_identity.exact_process_identity(41),
                    )

    def test_unavailable_identity_is_not_live_and_recycled_pid_does_not_match(self) -> None:
        backends = (
            ("nt", "win32", "_windows_process_identity"),
            ("posix", "darwin", "darwin_process_info"),
            ("posix", "linux", "_linux_process_identity"),
        )
        for os_name, platform, backend_name in backends:
            with self.subTest(platform=platform), patch.object(
                common_identity, backend_name, return_value=None
            ), patch.object(common_identity.os, "name", os_name), patch.object(
                common_identity.sys, "platform", platform
            ):
                self.assertIsNone(common_identity.exact_process_identity(41))
        with (
            patch.object(processes, "process_alive", return_value=True),
            patch.object(
                processes,
                "process_identity",
                return_value={"pid": 41, "creation_time": "new-incarnation"},
            ),
        ):
            self.assertFalse(processes.identity_matches(41, "old-incarnation"))
            self.assertFalse(processes.terminate_process(41, None))

    def test_macos_targeted_query_does_not_fall_back_to_proc(self) -> None:
        process = ProcessInfo(
            pid=41,
            ppid=7,
            name="provider",
            command_line="provider",
            created_utc=datetime.now(timezone.utc),
        )
        with (
            patch.object(processes.os, "name", "posix"),
            patch.object(processes.sys, "platform", "darwin"),
            patch.object(processes, "_darwin_process_query", return_value=ProcessQuery(True, process)),
            patch.object(processes, "_linux_process_query", side_effect=AssertionError("macOS used /proc")),
        ):
            query = processes.targeted_process_query(41)
        self.assertTrue(query.complete)
        self.assertEqual(41, query.process.pid if query.process else None)


class ProviderBoundaryCorrectionTests(unittest.TestCase):
    @staticmethod
    def _provider_code() -> str:
        return (
            "import os,pathlib,subprocess,sys,time\n"
            "helper=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'])\n"
            "marker=pathlib.Path(sys.argv[1])\n"
            "pending=marker.with_name(marker.name+'.tmp')\n"
            "with pending.open('w',encoding='ascii',newline='') as handle:\n"
            " handle.write(str(helper.pid)); handle.flush(); os.fsync(handle.fileno())\n"
            "os.replace(pending,marker)\n"
            "time.sleep(30)"
        )

    @staticmethod
    def _read_complete_pid_marker(marker: Path, timeout_seconds: float = 5.0) -> int:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            try:
                content = marker.read_text(encoding="ascii")
            except (FileNotFoundError, OSError):
                content = ""
            content = content.strip()
            if content and content.isascii() and content.isdecimal():
                return int(content)
            time.sleep(0.05)
        raise AssertionError(f"PID marker was not published with complete content: {marker}")

    def test_windows_preassigned_job_close_kills_suspended_provider_before_resume(self) -> None:
        if os.name != "nt":
            self.skipTest("requires the Windows Job Object containment path")
        TEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as raw:
            marker = Path(raw) / "helper.pid"
            provider = None
            boundary = None
            try:
                stdin_path = Path(raw) / "stdin.txt"
                stdout_path = Path(raw) / "stdout.txt"
                stderr_path = Path(raw) / "stderr.txt"
                stdin_path.write_text("", encoding="utf-8")
                with (
                    stdin_path.open("r", encoding="utf-8") as stdin_handle,
                    stdout_path.open("w", encoding="utf-8") as stdout_handle,
                    stderr_path.open("w", encoding="utf-8") as stderr_handle,
                ):
                    provider = processes.spawn_provider(
                        [sys.executable, "-c", self._provider_code(), str(marker)],
                        cwd=raw,
                        stdin=stdin_handle,
                        stdout=stdout_handle,
                        stderr=stderr_handle,
                    )
                job_handle = provider.take_job_handle()
                self.assertIsNotNone(job_handle)
                boundary = processes.ProcessBoundary.for_process(
                    provider.pid,
                    windows_job_handle=job_handle,
                )
                self.assertIsNotNone(boundary.root_creation_time)
                self.assertFalse(marker.exists(), "provider ran before its Job boundary was closed")
                record = boundary.record()
                self.assertEqual(provider.pid, record["root"]["pid"])
                boundary._close_job()
                provider.wait(timeout=5)
                self.assertFalse(marker.exists(), "provider resumed after preassigned Job close")
            finally:
                if boundary is not None:
                    boundary._close_job()
                if provider is not None:
                    if provider.poll() is None:
                        provider.kill()
                        provider.wait(timeout=5)
                    provider.close()

    def test_windows_job_boundary_cleans_provider_descendant(self) -> None:
        if os.name != "nt":
            self.skipTest("requires the Windows Job Object containment path")
        TEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as raw:
            marker = Path(raw) / "helper.pid"
            code = self._provider_code()
            provider = None
            boundary = None
            try:
                stdin_path = Path(raw) / "stdin.txt"
                stdout_path = Path(raw) / "stdout.txt"
                stderr_path = Path(raw) / "stderr.txt"
                stdin_path.write_text("", encoding="utf-8")
                with (
                    stdin_path.open("r", encoding="utf-8") as stdin_handle,
                    stdout_path.open("w", encoding="utf-8") as stdout_handle,
                    stderr_path.open("w", encoding="utf-8") as stderr_handle,
                ):
                    provider = processes.spawn_provider(
                        [sys.executable, "-c", code, str(marker)],
                        cwd=raw,
                        stdin=stdin_handle,
                        stdout=stdout_handle,
                        stderr=stderr_handle,
                    )
                self.assertFalse(marker.exists(), "provider ran before containment")
                boundary = processes.ProcessBoundary.for_process(
                    provider.pid,
                    windows_job_handle=provider.take_job_handle(),
                )
                getattr(provider, "resume")()
                helper_pid = self._read_complete_pid_marker(marker)
                self.assertTrue(processes.process_alive(helper_pid))
                self.assertTrue(boundary.observe())
                self.assertTrue(boundary.cleanup(force=True, timeout_seconds=5))
                provider.wait(timeout=5)
                self.assertFalse(processes.process_alive(helper_pid))
            finally:
                if boundary is not None:
                    boundary._close_job()
                if provider is not None and provider.poll() is None:
                    provider.kill()
                    provider.wait(timeout=5)
                if provider is not None:
                    provider.close()

    def test_windows_job_handle_close_kills_members_and_serialized_boundary_recovers(self) -> None:
        if os.name != "nt":
            self.skipTest("requires the Windows Job Object containment path")
        TEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as raw:
            marker = Path(raw) / "helper.pid"
            code = self._provider_code()
            provider = None
            boundary = None
            try:
                stdin_path = Path(raw) / "stdin.txt"
                stdout_path = Path(raw) / "stdout.txt"
                stderr_path = Path(raw) / "stderr.txt"
                stdin_path.write_text("", encoding="utf-8")
                with (
                    stdin_path.open("r", encoding="utf-8") as stdin_handle,
                    stdout_path.open("w", encoding="utf-8") as stdout_handle,
                    stderr_path.open("w", encoding="utf-8") as stderr_handle,
                ):
                    provider = processes.spawn_provider(
                        [sys.executable, "-c", code, str(marker)],
                        cwd=raw,
                        stdin=stdin_handle,
                        stdout=stdout_handle,
                        stderr=stderr_handle,
                    )
                self.assertFalse(marker.exists(), "provider ran before containment")
                boundary = processes.ProcessBoundary.for_process(
                    provider.pid,
                    windows_job_handle=provider.take_job_handle(),
                )
                getattr(provider, "resume")()
                helper_pid = self._read_complete_pid_marker(marker)
                self.assertTrue(boundary.observe())
                record = boundary.record()
                self.assertIn(
                    helper_pid,
                    [item["pid"] for item in record["processes"]],
                )

                # Closing the native Job handle models an abrupt controller
                # exit.  Kill-on-close must terminate the provider and helper.
                boundary._close_job()
                provider.wait(timeout=5)
                self.assertFalse(processes.process_alive(helper_pid))
                with patch.object(
                    processes,
                    "process_snapshot",
                    return_value=ProcessSnapshot(True, ()),
                ):
                    self.assertTrue(processes.process_boundary_is_gone(record))
            finally:
                if boundary is not None:
                    boundary._close_job()
                if provider is not None and provider.poll() is None:
                    provider.kill()
                    provider.wait(timeout=5)
                if provider is not None:
                    provider.close()

    def test_removed_boundary_inventory_symbols_have_no_tracked_references(self) -> None:
        repository = Path(__file__).resolve().parents[2]
        for symbol in ("process_group_" + "inventory", "ProcessBoundary" + "Inventory"):
            with self.subTest(symbol=symbol):
                result = subprocess.run(
                    ["git", "-C", str(repository), "grep", "-n", "-e", symbol, "--"],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                )
                self.assertEqual(1, result.returncode, result.stdout)

    def test_cleanup_status_transition_retains_boundary_evidence(self) -> None:
        TEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as raw:
            lane = {
                "lane_id": "lane-1",
                "run_id": "run-1",
                "controller_status_path": str(Path(raw) / "controller.status.json"),
            }
            boundary = {
                "kind": "posix-process-group",
                "root": {"pid": 41, "creation_time": "start-1"},
                "processes": [{"pid": 41, "creation_time": "start-1"}],
            }
            controller._write_status(lane, {"process_boundary": boundary})
            controller._write_status(lane, {"cleanup_proven": True})
            status = read_record(Path(lane["controller_status_path"]), controller.CONTROLLER_STATUS_SCHEMA)
        self.assertTrue(status["cleanup_proven"])
        self.assertEqual(boundary, status["process_boundary"])


class AcceptanceLinkCorrectionTests(unittest.TestCase):
    def _pair(self) -> tuple[dict[str, object], dict[str, object]]:
        review: dict[str, object] = {
            "schema": "completion-review/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "review_outcome": "PASS",
            "review_summary": "good",
            "evidence": [],
            "task_card_id": "card-1",
            "task_card_hash": "card-hash",
            "result_id": "result-1",
            "result_hash": "result-hash",
            "invocation_hash": "invocation-hash",
            "commit": "commit-1",
            "reviewed_at": "2026-09-02T00:00:00Z",
        }
        review["content_hash"] = content_hash(review)
        acceptance: dict[str, object] = {
            "schema": "orchestrator-acceptance/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "approval": "ACCEPTED",
            "accepted_by": "ROOT",
            "review_ref": review["content_hash"],
            "task_card_id": "card-1",
            "task_card_hash": "card-hash",
            "result_id": "result-1",
            "result_hash": "result-hash",
            "invocation_hash": "invocation-hash",
            "commit": "commit-1",
            "decided_at": "2026-09-02T00:00:00Z",
        }
        acceptance["content_hash"] = content_hash(acceptance)
        return review, acceptance

    def test_monitor_requires_the_real_review_hash_link(self) -> None:
        TEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as raw:
            root = Path(raw) / "runtime" / "epochs" / "epoch-1" / "lanes" / "lane-1"
            root.mkdir(parents=True)
            review, acceptance = self._pair()
            acceptance["review_ref"] = "wrong-review-hash"
            acceptance["content_hash"] = content_hash(acceptance)
            atomic_write_json(root / "COMPLETION_REVIEW.json", review)
            atomic_write_json(root / "ORCHESTRATOR_ACCEPTANCE.json", acceptance)
            self.assertIsNone(monitor._read_acceptance_chain(Path(raw) / "runtime", "epoch-1", "lane-1"))

            review, acceptance = self._pair()
            atomic_write_json(root / "COMPLETION_REVIEW.json", review)
            atomic_write_json(root / "ORCHESTRATOR_ACCEPTANCE.json", acceptance)
            self.assertEqual(
                acceptance,
                monitor._read_acceptance_chain(Path(raw) / "runtime", "epoch-1", "lane-1"),
            )

    def test_monitor_rejects_mismatched_task_result_or_commit_identity(self) -> None:
        TEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        for field in ("task_card_id", "task_card_hash", "result_id", "result_hash", "commit"):
            with self.subTest(field=field), tempfile.TemporaryDirectory(dir=TEST_TEMP_ROOT) as raw:
                root = Path(raw) / "runtime" / "epochs" / "epoch-1" / "lanes" / "lane-1"
                root.mkdir(parents=True)
                review, acceptance = self._pair()
                acceptance[field] = "different"
                acceptance["content_hash"] = content_hash(acceptance)
                atomic_write_json(root / "COMPLETION_REVIEW.json", review)
                atomic_write_json(root / "ORCHESTRATOR_ACCEPTANCE.json", acceptance)
                self.assertIsNone(
                    monitor._read_acceptance_chain(Path(raw) / "runtime", "epoch-1", "lane-1")
                )


if __name__ == "__main__":
    unittest.main()
