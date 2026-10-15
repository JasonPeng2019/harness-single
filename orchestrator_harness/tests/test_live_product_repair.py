from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from orchestrator_harness import controller, git_source, lanes, launch, mutation, processes, records, review, setup
from orchestrator_harness.core import content_hash, iso_utc
from orchestrator_harness.epochs import lane_record_dir
from orchestrator_harness.tests.support import prepare_lane_fixture, seed_provider_history


def _windows_error(code: int) -> OSError:
    error = OSError(f"synthetic Windows error {code}")
    error.winerror = code
    return error


class WindowsReplacementRetryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    @unittest.skipUnless(os.name == "nt", "native Windows record replacement retry")
    def test_record_replacement_retries_access_and_sharing_errors(self) -> None:
        target = self.root / "record.json"
        target.write_bytes(b"old\n")
        real_replace = records.os.replace
        calls = 0

        def replace(source: object, destination: object) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise _windows_error(5)
            if calls == 2:
                raise _windows_error(32)
            real_replace(source, destination)

        with (
            patch.object(records.os, "name", "nt"),
            patch.object(records.os, "replace", side_effect=replace),
            patch.object(records.time, "sleep") as sleep,
        ):
            records.atomic_write_bytes(target, b"new\n")

        self.assertEqual(b"new\n", target.read_bytes())
        self.assertEqual(3, calls)
        self.assertEqual(2, sleep.call_count)
        self.assertEqual([], list(self.root.glob(".record.json.*.tmp")))

    @unittest.skipUnless(os.name == "nt", "native Windows record replacement retry")
    def test_record_permanent_replacement_error_is_immediate_and_preserves_old(self) -> None:
        target = self.root / "record.json"
        target.write_bytes(b"old\n")
        with (
            patch.object(records.os, "name", "nt"),
            patch.object(records.os, "replace", side_effect=_windows_error(5)) as replace,
            patch.object(records.time, "sleep") as sleep,
            self.assertRaises(OSError),
        ):
            records.atomic_write_bytes(target, b"new\n")

        self.assertEqual(records._WINDOWS_REPLACE_MAX_ATTEMPTS, replace.call_count)
        self.assertEqual(records._WINDOWS_REPLACE_MAX_ATTEMPTS - 1, sleep.call_count)
        self.assertEqual(b"old\n", target.read_bytes())
        self.assertEqual([], list(self.root.glob(".record.json.*.tmp")))

    @unittest.skipUnless(os.name == "nt", "native Windows record replacement retry")
    def test_record_nontransient_replacement_error_is_not_retried(self) -> None:
        target = self.root / "record.json"
        target.write_bytes(b"old\n")
        with (
            patch.object(records.os, "name", "nt"),
            patch.object(records.os, "replace", side_effect=_windows_error(123)) as replace,
            patch.object(records.time, "sleep") as sleep,
            self.assertRaises(OSError),
        ):
            records.atomic_write_bytes(target, b"new\n")

        self.assertEqual(1, replace.call_count)
        sleep.assert_not_called()
        self.assertEqual(b"old\n", target.read_bytes())
        self.assertEqual([], list(self.root.glob(".record.json.*.tmp")))

    @unittest.skipUnless(os.name == "nt", "native Windows cache rename retry")
    def test_super_cache_directory_replacement_retries_transient_errors(self) -> None:
        destination = self.root / "super-cache"
        destination.mkdir()
        (destination / "old.txt").write_text("old\n", encoding="utf-8")
        staging = self.root / ".super-cache.staging"
        staging.mkdir()
        (staging / "new.txt").write_text("new\n", encoding="utf-8")
        real_replace = mutation._NtSetInformationFile
        calls = 0

        def replace(*args: object) -> int:
            nonlocal calls
            calls += 1
            if calls == 1:
                return 0xc0000043 - 2**32
            return real_replace(*args)

        with (
            patch.object(mutation, "_NtSetInformationFile", side_effect=replace),
            patch.object(records.time, "sleep") as sleep,
        ):
            setup._replace_tree(staging, destination)

        self.assertEqual("new\n", (destination / "new.txt").read_text(encoding="utf-8"))
        self.assertFalse((destination / "old.txt").exists())
        self.assertFalse(staging.exists())
        self.assertEqual(3, calls)
        self.assertEqual(1, sleep.call_count)

    @unittest.skipUnless(os.name == "nt", "native Windows cache rename retry")
    def test_super_cache_staging_destination_retry_preserves_atomic_swap(self) -> None:
        destination = self.root / "super-cache"
        destination.mkdir()
        (destination / "old.txt").write_text("old\n", encoding="utf-8")
        staging = self.root / ".super-cache.staging"
        staging.mkdir()
        (staging / "new.txt").write_text("new\n", encoding="utf-8")
        real_replace = mutation._NtSetInformationFile
        calls = 0

        def replace(*args: object) -> int:
            nonlocal calls
            calls += 1
            if calls == 2:
                return 0xc0000022 - 2**32
            return real_replace(*args)

        with (
            patch.object(mutation, "_NtSetInformationFile", side_effect=replace),
            patch.object(records.time, "sleep") as sleep,
        ):
            setup._replace_tree(staging, destination)

        self.assertEqual("new\n", (destination / "new.txt").read_text(encoding="utf-8"))
        self.assertFalse((destination / "old.txt").exists())
        self.assertEqual(3, calls)
        self.assertEqual(1, sleep.call_count)

    @unittest.skipUnless(os.name == "nt", "native Windows cache rename retry")
    def test_super_cache_permanent_replacement_error_surfaces(self) -> None:
        destination = self.root / "super-cache"
        destination.mkdir()
        staging = self.root / ".super-cache.staging"
        staging.mkdir()
        with (
            patch.object(mutation, "_NtSetInformationFile", return_value=0xc0000022 - 2**32) as replace,
            patch.object(records.time, "sleep") as sleep,
            self.assertRaises(OSError),
        ):
            setup._replace_tree(staging, destination)

        self.assertEqual(records._WINDOWS_REPLACE_MAX_ATTEMPTS, replace.call_count)
        self.assertEqual(records._WINDOWS_REPLACE_MAX_ATTEMPTS - 1, sleep.call_count)
        self.assertTrue(destination.is_dir())
        self.assertTrue(staging.is_dir())


class CleanupLifecycleRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.runtime = self.root / "runtime"
        self.worktree = self.root / "worktree"
        workspace = self.worktree / ".agent-workspace"
        workspace.mkdir(parents=True)
        self.lane = {
            "schema": "lane/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "lifecycle": "prepared",
            "process": {},
            "worktree_path": str(self.worktree),
            "controller_status_path": str(workspace / "controller.status.json"),
            "controller_events_path": str(workspace / "controller.events.jsonl"),
        }
        self.invocation = {
            "schema": "controller-invocation/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "provider": {
                "id": "codex",
                "model": "configured-model",
                "launch_config": {"reasoning_effort": "high", "service_tier": "priority"},
            },
        }
        binding = (
            "PROVIDER_ID = 'codex'\n"
            "def build_argv(**kwargs): return ['codex']\n"
            "def parse_line(line): return None\n"
        )
        binding_path = (
            self.root
            / "orchestrator_harness"
            / "provider_adapters"
            / "codex"
            / "launcher_binding.py"
        )
        binding_path.parent.mkdir(parents=True)
        binding_path.write_text(binding, encoding="utf-8")

    def _controller(self) -> subprocess.Popen[bytes]:
        """A real controller stand-in that runs until the test releases its handle."""
        child = subprocess.Popen([sys.executable, "-B", "-c", "import sys; sys.stdin.buffer.read(1)"],
                                 stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL,
                                 creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)

        def stop() -> None:
            # Exact cleanup through the owned handle only.
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=10)
            child.stdin.close()

        self.addCleanup(stop)
        return child

    @staticmethod
    def _release(child: subprocess.Popen[bytes]) -> None:
        child.stdin.write(b"x")
        child.stdin.close()

    def _exited_identity(self) -> dict[str, object]:
        child = self._controller()
        identity = processes.process_identity(child.pid)
        self.assertIsNotNone(identity)
        self._release(child)
        child.wait(timeout=10)
        self.assertTrue(processes.identity_exit_proven(identity["pid"], identity["creation_time"]))
        return identity

    def test_provider_start_failure_clears_exact_controller_identity_after_handle_exit(
        self,
    ) -> None:
        lane = prepare_lane_fixture(
            self.runtime, "epoch-1", {**self.lane, "provider": self.invocation["provider"]}
        )
        seed_provider_history(self.runtime, lane)
        lane = lanes.write_lane(self.runtime, "epoch-1", "lane-1", lane)
        spawned: list[subprocess.Popen[bytes]] = []
        updates: list[dict[str, object]] = []
        real_update = launch.update_lane

        def spawn(_argv: object, **_options: object) -> subprocess.Popen[bytes]:
            spawned.append(self._controller())
            return spawned[-1]

        def update(
            rt: Path,
            epoch: str,
            lane_id: str,
            mutate: object,
        ) -> dict[str, object]:
            value = real_update(rt, epoch, lane_id, mutate)
            updates.append(value)
            if len(updates) == 2:
                # Registered: the controller cannot start its provider, publishes
                # its terminal status and event, then exits on its own.
                controller._write_status(value, {
                    "controller_identity": value["process"],
                    "controller_state": "exited",
                    "provider_state": {"state": "not_started"},
                    "cleanup_proven": True,
                    "recorded_status": "provider_start_failed",
                    "provider_start_error": "provider executable is missing",
                    "process_boundary": None,
                    "cleanup_error": None,
                })
                records.append_jsonl(
                    Path(value["controller_events_path"]),
                    {"ts": iso_utc(), "run_id": value["run_id"], "event_type": "provider_start_failed",
                     "detail": "provider executable is missing"},
                    header={"schema": controller.CONTROLLER_EVENTS_SCHEMA},
                )
                self._release(spawned[0])
            return value

        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(
                launch,
                "load_config",
                return_value=SimpleNamespace(runtime_root=self.runtime),
            ),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", lane)),
            patch.object(
                launch,
                "_validate_provider_launch_config",
                return_value=self.invocation["provider"]["launch_config"],
            ),
            patch.object(launch.processes, "spawn_detached", side_effect=spawn),
            patch.object(launch, "update_lane", side_effect=update),
        ):
            result = launch.run_launch("lane-1")

        self.assertFalse(result["ok"], result)
        self.assertEqual(launch.LAUNCH_PROVIDER_START_FAILED, result["code"])
        self.assertEqual(1, len(spawned))
        identity = updates[1]["process"]
        self.assertEqual(spawned[0].pid, identity["pid"])
        self.assertTrue(processes.identity_exit_proven(identity["pid"], identity["creation_time"]))
        self.assertEqual({}, updates[-1]["process"])
        self.assertEqual({}, lanes.read_lane(self.runtime, "epoch-1", "lane-1")["process"])

    def test_retire_waits_for_controller_handle_after_exited_status(self) -> None:
        lane = prepare_lane_fixture(self.runtime, "epoch-1", {
            **self.lane, "lifecycle": "review_pending", "provider": self.invocation["provider"],
        })
        result = {
            "schema": "result/v1", "lane_id": "lane-1", "run_id": "run-1",
            "worker_invocation_id": "run-1", "task_card_hash": lane["task_card_hash"],
            "branch": lane["git_source"]["branch_ref"].removeprefix("refs/heads/"),
            "commit": git_source.current_tip(lane), "outcome": "PASS", "summary": "completed",
            "checks": [{"name": "fixture", "command": ["python", "-m", "unittest"], "outcome": "PASS"}],
            "evidence": [{"stdout": "native diagnostics", "exit_code": 0}], "completed_at": iso_utc(),
        }
        result["content_hash"] = content_hash(result)
        records.atomic_write_json(Path(lane["result_path"]), result)
        review._write_pair(
            self.runtime, "epoch-1", lane, review_outcome="PASS", review_summary="reviewed",
            evidence=[], approval="ACCEPTED", force_accept_reason=None,
        )
        acceptance_path = lane_record_dir(self.runtime, "epoch-1", "lane-1") / "ORCHESTRATOR_ACCEPTANCE.json"
        provider = self._exited_identity()
        handle = self._controller()
        identity = processes.process_identity(handle.pid)
        self.assertIsNotNone(identity)
        lane = lanes.write_lane(self.runtime, "epoch-1", "lane-1", {
            **lane, "lifecycle": "accepted", "process": identity,
        })
        # The controller has published exited/cleanup-proven status, but its
        # process handle has not exited yet.
        controller._write_status(lane, {
            "controller_identity": identity,
            "controller_state": "exited",
            "provider_state": {"state": "exited", **provider, "exit_code": 0},
            "process_boundary": {"root": provider, "processes": [provider]},
            "cleanup_proven": True,
            "recorded_status": "review_pending",
        })
        real_wait = launch._wait_for_controller_exit
        waited: list[bool] = []

        def wait_for_exit(current: dict[str, object], timeout_seconds: float) -> bool:
            self.assertTrue(processes.identity_matches(identity["pid"], identity["creation_time"]))
            self._release(handle)
            handle.wait(timeout=10)
            waited.append(real_wait(current, timeout_seconds))
            return waited[-1]

        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(
                launch,
                "load_config",
                return_value=SimpleNamespace(
                    runtime_root=self.runtime, root_workspace=self.root
                ),
            ),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", lane)),
            patch.object(launch, "_wait_for_controller_exit", side_effect=wait_for_exit),
            # Provider boundary proof is covered elsewhere; the controller handle is the subject.
            patch.object(launch.processes, "process_boundary_is_gone", return_value=True),
            patch.object(launch, "release_leases"),
            patch.object(launch, "read_active_lanes", return_value=[]),
            patch.object(launch, "write_active_lanes"),
            patch.object(launch, "_prune_worktrees"),
            patch.object(launch, "_maybe_close_epoch"),
        ):
            outcome = launch.run_retire(str(acceptance_path))

        self.assertTrue(outcome["ok"], outcome)
        self.assertEqual("RETIRE_OK", outcome["code"])
        self.assertEqual([True], waited)
        self.assertEqual("retired", lanes.read_lane(self.runtime, "epoch-1", "lane-1")["lifecycle"])

if __name__ == "__main__":
    unittest.main()
