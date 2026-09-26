from __future__ import annotations

from contextlib import ExitStack
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from orchestrator_harness import launch, records, setup
from orchestrator_harness.core import content_hash


def _windows_error(code: int) -> OSError:
    error = OSError(f"synthetic Windows error {code}")
    error.winerror = code
    return error


class WindowsReplacementRetryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

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

    def test_super_cache_directory_replacement_retries_transient_errors(self) -> None:
        destination = self.root / "super-cache"
        destination.mkdir()
        (destination / "old.txt").write_text("old\n", encoding="utf-8")
        staging = self.root / ".super-cache.staging"
        staging.mkdir()
        (staging / "new.txt").write_text("new\n", encoding="utf-8")
        real_replace = setup.os.replace
        calls = 0

        def replace(source: object, target: object) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise _windows_error(32)
            real_replace(source, target)

        with (
            patch.object(setup.os, "name", "nt"),
            patch.object(setup.os, "replace", side_effect=replace),
            patch.object(records.time, "sleep") as sleep,
        ):
            setup._replace_tree(staging, destination)

        self.assertEqual("new\n", (destination / "new.txt").read_text(encoding="utf-8"))
        self.assertFalse((destination / "old.txt").exists())
        self.assertFalse(staging.exists())
        self.assertEqual(3, calls)
        self.assertEqual(1, sleep.call_count)

    def test_super_cache_staging_destination_retry_preserves_atomic_swap(self) -> None:
        destination = self.root / "super-cache"
        destination.mkdir()
        (destination / "old.txt").write_text("old\n", encoding="utf-8")
        staging = self.root / ".super-cache.staging"
        staging.mkdir()
        (staging / "new.txt").write_text("new\n", encoding="utf-8")
        real_replace = setup.os.replace
        calls = 0

        def replace(source: object, target: object) -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise _windows_error(5)
            real_replace(source, target)

        with (
            patch.object(setup.os, "name", "nt"),
            patch.object(setup.os, "replace", side_effect=replace),
            patch.object(records.time, "sleep") as sleep,
        ):
            setup._replace_tree(staging, destination)

        self.assertEqual("new\n", (destination / "new.txt").read_text(encoding="utf-8"))
        self.assertFalse((destination / "old.txt").exists())
        self.assertEqual(3, calls)
        self.assertEqual(1, sleep.call_count)

    def test_super_cache_permanent_replacement_error_surfaces(self) -> None:
        destination = self.root / "super-cache"
        destination.mkdir()
        staging = self.root / ".super-cache.staging"
        staging.mkdir()
        with (
            patch.object(setup.os, "name", "nt"),
            patch.object(setup.os, "replace", side_effect=_windows_error(5)) as replace,
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
            "git": {"branch": "lane/lane-1", "bootstrap_tip": "commit-1"},
        }
        self.invocation = {
            "schema": "controller-invocation/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "provider": {"id": "codex"},
            "git": dict(self.lane["git"]),
        }
        self.invocation["content_hash"] = content_hash(self.invocation)
        self.lane["provider"] = self.invocation["provider"]
        self.lane["invocation_hash"] = self.invocation["content_hash"]
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

    def test_provider_start_failure_clears_exact_controller_identity_after_handle_exit(
        self,
    ) -> None:
        Path(self.lane["controller_events_path"]).write_text(
            '{"event_type":"provider_start_failed"}\n', encoding="utf-8"
        )
        status = {
            "schema": "controller-status/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "controller_state": "exited",
            "provider_state": {"state": "not_started"},
            "cleanup_proven": True,
            "recorded_status": "provider_start_failed",
        }
        child = MagicMock(pid=41)
        child.poll.return_value = 0
        current = dict(self.lane)
        updates: list[dict[str, object]] = []

        def update(
            _rt: object,
            _epoch: object,
            _lane_id: object,
            mutate: object,
        ) -> dict[str, object]:
            value = mutate(dict(current))
            updates.append(value)
            current.update(value)
            return dict(current)

        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(
                launch,
                "load_config",
                return_value=SimpleNamespace(runtime_root=self.runtime),
            ),
            patch.object(launch, "read_runtime_state", return_value={"state": "OPEN"}),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", self.lane)),
            patch.object(launch, "read_record", return_value=self.invocation),
            patch.object(launch.processes, "spawn_detached", return_value=child),
            patch.object(
                launch.processes,
                "process_identity",
                return_value={"pid": 41, "creation_time": "created-1"},
            ),
            patch.object(launch, "update_lane", side_effect=update),
            patch.object(launch, "_read_controller_status", return_value=status),
        ):
            result = launch.run_launch("lane-1")

        self.assertFalse(result["ok"])
        self.assertEqual(launch.LAUNCH_PROVIDER_START_FAILED, result["code"])
        self.assertEqual(
            {"pid": 41, "creation_time": "created-1"}, updates[0]["process"]
        )
        self.assertEqual({}, updates[-1]["process"])

    def test_retire_waits_for_controller_handle_after_exited_status(self) -> None:
        lane = {
            **self.lane,
            "lifecycle": "accepted",
            "process": {"pid": 41, "creation_time": "created-1"},
            "result_validation": {
                "run_id": "run-1",
                "result_hash": "result-hash-1",
                "branch": "lane/lane-1",
                "commit": "commit-1",
                "clean": True,
            },
        }
        status = {
            "schema": "controller-status/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "controller_state": "exited",
            "provider_state": {
                "state": "exited",
                "pid": 42,
                "creation_time": "provider-created",
            },
            "process_boundary": {"root": {"pid": 42, "creation_time": "provider-created"}},
            "cleanup_proven": True,
        }
        acceptance = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "approval": "ACCEPTED",
            "content_hash": "acceptance-hash-1",
        }
        review = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "result_hash": "result-hash-1",
            "commit": "commit-1",
        }
        current = dict(lane)

        def update(
            _rt: object,
            _epoch: object,
            _lane_id: object,
            mutate: object,
        ) -> dict[str, object]:
            value = mutate(dict(current))
            current.update(value)
            return dict(current)

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(launch, "find_harness_root", return_value=self.root)
            )
            stack.enter_context(
                patch.object(
                launch,
                "load_config",
                return_value=SimpleNamespace(
                    runtime_root=self.runtime, root_workspace=self.root
                ),
                )
            )
            stack.enter_context(
                patch.object(launch, "_validate_acceptance_ref", return_value=acceptance)
            )
            stack.enter_context(
                patch.object(
                    launch, "find_active_lane", return_value=("epoch-1", lane)
                )
            )
            stack.enter_context(patch.object(
                launch,
                "_validate_retirement_chain",
                return_value=(review, acceptance),
            ))
            stack.enter_context(
                patch.object(launch, "_read_controller_status", return_value=status)
            )
            stack.enter_context(patch.object(
                launch.processes,
                "identity_matches",
                side_effect=[True, False, False],
            ))
            stack.enter_context(
                patch.object(launch.processes, "process_alive", return_value=False)
            )
            stack.enter_context(
                patch.object(
                    launch.processes, "process_boundary_is_gone", return_value=True
                )
            )
            stack.enter_context(patch.object(
                launch,
                "validate_merge_ready_git",
                return_value={
                    "branch": "lane/lane-1",
                    "commit": "commit-1",
                    "clean": True,
                },
            ))
            stack.enter_context(patch.object(
                launch,
                "_archive_retirement_evidence",
                return_value=self.runtime / "archive",
            ))
            stack.enter_context(patch.object(
                launch,
                "_read_retirement_archive",
                return_value={"files": {}, "worktree_files": {}},
            ))
            stack.enter_context(patch.object(
                launch,
                "_plan_worktree_quarantine",
                return_value={
                    "path": str(launch._quarantine_path(lane)),
                    "gitfile": {"fixture": True},
                },
            ))
            for context in (
                patch.object(launch, "_remove_exact_worktree"),
                patch.object(launch, "_prove_retained_branch_tip"),
                patch.object(launch, "release_leases"),
                patch.object(launch, "update_lane", side_effect=update),
                patch.object(launch, "read_active_lanes", return_value=[]),
                patch.object(launch, "write_active_lanes"),
                patch.object(launch, "_prune_worktrees"),
                patch.object(launch, "_maybe_close_epoch"),
            ):
                stack.enter_context(context)
            result = launch.run_retire("acceptance.json")

        self.assertTrue(result["ok"], result)
        self.assertEqual("RETIRE_OK", result["code"])


if __name__ == "__main__":
    unittest.main()
