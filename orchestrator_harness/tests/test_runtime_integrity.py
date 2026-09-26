from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from orchestrator_harness import controller, launch, monitor, resume, review
from orchestrator_harness.core import content_hash
from orchestrator_harness.records import atomic_write_json


class GitResultBindingTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="harness-git-binding-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "source"
        self.worktree = self.root / "lane"
        self.source.mkdir()
        self._git(self.source, "init", "--initial-branch=main")
        self._git(self.source, "config", "user.name", "Harness Test")
        self._git(self.source, "config", "user.email", "harness@example.invalid")
        (self.source / "tracked.txt").write_text("base\n", encoding="utf-8")
        self._git(self.source, "add", "tracked.txt")
        self._git(self.source, "commit", "-m", "base")
        self.base = self._git(self.source, "rev-parse", "HEAD")
        self._git(
            self.source,
            "worktree",
            "add",
            "-b",
            "lane/test",
            str(self.worktree),
            self.base,
        )
        common = Path(self._git(self.worktree, "rev-parse", "--git-common-dir"))
        if not common.is_absolute():
            common = self.worktree / common
        workspace = self.worktree / ".agent-workspace"
        workspace.mkdir()
        self.lane = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "worktree_path": str(self.worktree),
            "result_path": str(self.worktree / "RESULT.json"),
            "git": {
                "source_root": str(self.source.resolve()),
                "common_dir": str(common.resolve()),
                "branch": "lane/test",
                "base_commit": self.base,
                "origin_tip": self.base,
                "bootstrap_tip": self.base,
                "harness_owned_paths": [".agent-workspace/**", "RESULT.json"],
            },
        }
        task = {
            "schema": "project-task-card/v1",
            "card_id": "card-1",
            "task": "test",
            "branch": "lane/test",
            "base_commit": self.base,
        }
        atomic_write_json(workspace / "task-card.json", task)
        self.lane["task_card_hash"] = content_hash(task)
        self.lane["provider"] = {"id": "codex", "model": "test-model"}
        self.invocation = {
            "schema": "controller-invocation/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "provider": self.lane["provider"],
            "exclusive_resources": ["test-resource"],
            "git": self.lane["git"],
            "launcher": {"entry": "test", "argv": ["test"]},
            "cwd": str(self.worktree),
            "paths": {"worktree": str(self.worktree)},
        }
        self.invocation["content_hash"] = content_hash(self.invocation)
        atomic_write_json(workspace / "invocation.json", self.invocation)
        self.lane["invocation_hash"] = self.invocation["content_hash"]
        self.result = {
            "schema": "result/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "outcome": "PASS",
            "summary": "complete",
            "evidence": [],
            "completed_at": "2026-09-26T00:00:00Z",
        }
        self.result["content_hash"] = content_hash(self.result)
        atomic_write_json(self.worktree / "RESULT.json", self.result)

    @staticmethod
    def _git(root: Path, *args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(root), *args],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        ).stdout.strip()

    def test_result_binds_to_current_clean_branch_tip(self) -> None:
        state, record = controller._validate_result(self.lane)
        self.assertEqual("valid", state)
        assert record is not None
        self.assertEqual("lane/test", record["_validated_git"]["branch"])
        self.assertEqual(self.base, record["_validated_git"]["commit"])

    def test_wrong_branch_dirty_and_unexpected_untracked_fail_closed(self) -> None:
        wrong = {**self.lane, "git": {**self.lane["git"], "branch": "lane/wrong"}}
        self.assertEqual("invalid", controller._validate_result(wrong)[0])

        (self.worktree / "tracked.txt").write_text("dirty\n", encoding="utf-8")
        self.assertEqual("invalid", controller._validate_result(self.lane)[0])
        self._git(self.worktree, "restore", "tracked.txt")

        (self.worktree / "unexpected.txt").write_text("untracked\n", encoding="utf-8")
        self.assertEqual("invalid", controller._validate_result(self.lane)[0])

    def test_unmerged_and_git_command_failure_fail_closed(self) -> None:
        with patch.object(
            review.subprocess,
            "run",
            return_value=subprocess.CompletedProcess([], 128, "", "git failed"),
        ):
            with self.assertRaises(review.GitStateError):
                review.validate_merge_ready_git(self.lane)

        with patch.object(review, "_git") as git:
            git.side_effect = [
                subprocess.CompletedProcess([], 0, "lane/test\n", ""),
                subprocess.CompletedProcess([], 0, self.base + "\n", ""),
                subprocess.CompletedProcess([], 0, str(Path(self.lane["git"]["common_dir"])) + "\n", ""),
                subprocess.CompletedProcess([], 0, "", ""),
                subprocess.CompletedProcess([], 0, "100644 x 1\tconflict\0", ""),
            ]
            with self.assertRaisesRegex(review.GitStateError, "unmerged"):
                review.validate_merge_ready_git(self.lane)

    def test_review_revalidates_exact_result_commit(self) -> None:
        state, record = controller._validate_result(self.lane)
        self.assertEqual("valid", state)
        assert record is not None
        git_state = record["_validated_git"]
        self.lane["result_validation"] = {
            "run_id": "run-1",
            "result_hash": content_hash(self.result),
            "invocation_hash": self.lane["invocation_hash"],
            "branch": git_state["branch"],
            "commit": git_state["commit"],
            "clean": True,
            "validated_at": "2026-09-26T00:00:01Z",
        }
        runtime = self.root / "runtime"
        review_record, acceptance = review._write_pair(
            runtime,
            "epoch-1",
            self.lane,
            review_outcome="PASS",
            review_summary="reviewed",
            evidence=[],
            approval="ACCEPTED",
            force_accept_reason=None,
        )
        self.assertEqual(self.base, review_record["commit"])
        self.assertTrue(
            review.validate_acceptance_chain(
                review_record, acceptance, lane_id="lane-1", run_id="run-1"
            )
        )

        self.lane["result_validation"] = {
            **self.lane["result_validation"],
            "commit": "f" * 40,
        }
        with self.assertRaises(review.ReviewError):
            review._write_pair(
                self.root / "other-runtime",
                "epoch-1",
                self.lane,
                review_outcome="PASS",
                review_summary="reviewed",
                evidence=[],
                approval="ACCEPTED",
                force_accept_reason=None,
            )

    def test_retirement_cleanup_refuses_every_tracked_selected_path_before_deletion(
        self,
    ) -> None:
        provider_file = self.worktree / ".provider" / "payload.txt"
        provider_file.parent.mkdir()
        provider_file.write_text("provider payload\n", encoding="utf-8")
        tracked_workspace_file = (
            self.worktree / ".agent-workspace" / "tracked-user-content.txt"
        )
        tracked_workspace_file.write_text("must survive\n", encoding="utf-8")
        self.lane["git"]["harness_owned_paths"].append(
            ".provider/payload.txt"
        )
        self._git(
            self.worktree,
            "add",
            "RESULT.json",
            ".agent-workspace/tracked-user-content.txt",
            ".provider/payload.txt",
        )
        self._git(self.worktree, "commit", "-m", "tracked collision")

        with self.assertRaisesRegex(
            launch.LaunchError, "refusing to delete tracked harness-owned paths"
        ):
            sources, mapping = launch._worktree_archive_sources(self.lane)
            manifest = {
                "worktree_files": mapping,
                "files": {
                    name: {
                        "sha256": launch._sha256_file(path),
                        "size": path.stat().st_size,
                    }
                    for name, path in sources.items()
                },
            }
            launch._remove_archived_harness_artifacts(self.lane, manifest)

        self.assertTrue((self.worktree / "RESULT.json").is_file())
        self.assertTrue(tracked_workspace_file.is_file())
        self.assertTrue(provider_file.is_file())

    def test_final_identity_check_rejects_concurrent_clean_commit(self) -> None:
        (self.source / "alternate.txt").write_text("alternate\n", encoding="utf-8")
        self._git(self.source, "add", "alternate.txt")
        self._git(self.source, "commit", "-m", "alternate")
        alternate = self._git(self.source, "rev-parse", "HEAD")
        sources, mapping = launch._worktree_archive_sources(self.lane)
        manifest = {
            "worktree_files": mapping,
            "gitlinks": [],
            "files": {
                name: {"sha256": launch._sha256_file(path), "size": path.stat().st_size}
                for name, path in sources.items()
            },
        }
        plan = launch._plan_worktree_quarantine(
            self.source, self.lane, manifest, self.base
        )
        self.lane["retirement"] = {
            "phase": "worktree_removal_started",
            "quarantine_path": plan["path"],
            "gitfile": plan["gitfile"],
        }
        real_rename = launch.os.rename

        def move_branch_then_rename(source, target):
            self._git(
                self.source,
                "update-ref",
                "refs/heads/lane/test",
                alternate,
            )
            return real_rename(source, target)

        with (
            patch.object(launch.os, "rename", side_effect=move_branch_then_rename),
            self.assertRaisesRegex(launch.LaunchError, "retained lane branch changed"),
        ):
            launch._remove_exact_worktree(self.source, self.lane, manifest, self.base)
        self.assertTrue(self.worktree.is_dir())

    def test_recomputed_invocation_mutation_is_rejected_before_any_spawn_or_review(
        self,
    ) -> None:
        mutated = {
            **self.invocation,
            "exclusive_resources": ["forged-resource"],
            "cwd": str(self.root / "other-cwd"),
        }
        mutated["content_hash"] = content_hash(mutated)
        invocation_path = self.worktree / ".agent-workspace" / "invocation.json"
        atomic_write_json(invocation_path, mutated)
        lane = {**self.lane, "lifecycle": "prepared"}

        with self.assertRaisesRegex(
            review.GitStateError, "authoritative lane invocation hash"
        ):
            review.validate_invocation_binding(lane, mutated)

        config = SimpleNamespace(runtime_root=self.root / "runtime", root_workspace=self.source)
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(launch, "load_config", return_value=config),
            patch.object(launch, "read_runtime_state", return_value={"state": "OPEN"}),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", lane)),
            patch.object(launch.processes, "spawn_detached") as spawn,
        ):
            launched = launch.run_launch("lane-1")
        self.assertEqual(launch.LAUNCH_INVOCATION_INVALID, launched["code"])
        spawn.assert_not_called()

        with (
            patch.object(controller, "find_harness_root", return_value=self.root),
            patch.object(controller, "load_config", return_value=config),
            patch.object(controller, "find_active_lane", return_value=("epoch-1", lane)),
            patch.object(controller, "_load_binding") as load_binding,
            self.assertRaisesRegex(
                controller.ControllerError, "authoritative lane invocation hash"
            ),
        ):
            controller.run_controller("lane-1")
        load_binding.assert_not_called()

        lane["result_validation"] = {
            "run_id": "run-1",
            "result_hash": self.result["content_hash"],
            "invocation_hash": self.lane["invocation_hash"],
            "branch": "lane/test",
            "commit": self.base,
            "clean": True,
        }
        with self.assertRaisesRegex(review.ReviewError, "authoritative lane invocation hash"):
            review._write_pair(
                self.root / "review-runtime",
                "epoch-1",
                lane,
                review_outcome="PASS",
                review_summary="reviewed",
                evidence=[],
                approval="ACCEPTED",
                force_accept_reason=None,
            )

    def test_branch_move_at_worktree_remove_boundary_fails_before_finalization(
        self,
    ) -> None:
        (self.source / "alternate.txt").write_text("alternate\n", encoding="utf-8")
        self._git(self.source, "add", "alternate.txt")
        self._git(self.source, "commit", "-m", "alternate")
        alternate = self._git(self.source, "rev-parse", "HEAD")
        sources, mapping = launch._worktree_archive_sources(self.lane)
        manifest = {
            "worktree_files": mapping,
            "files": {
                name: {
                    "sha256": launch._sha256_file(path),
                    "size": path.stat().st_size,
                }
                for name, path in sources.items()
            },
        }
        archive_evidence = self.root / "retirement-evidence.json"
        archive_evidence.write_text("preserve\n", encoding="utf-8")
        manifest["gitlinks"] = []
        plan = launch._plan_worktree_quarantine(
            self.source, self.lane, manifest, self.base
        )
        self.lane["retirement"] = {
            "phase": "worktree_removal_started",
            "quarantine_path": plan["path"],
            "gitfile": plan["gitfile"],
        }
        real_prune = launch._prune_worktrees

        def move_branch_after_prune(root):
            real_prune(root)
            self._git(
                self.source,
                "update-ref",
                "refs/heads/lane/test",
                alternate,
            )

        with (
            patch.object(launch, "_prune_worktrees", side_effect=move_branch_after_prune),
            self.assertRaisesRegex(launch.LaunchError, "retained lane branch changed"),
        ):
            launch._remove_exact_worktree(
                self.source, self.lane, manifest, self.base
            )
        self.assertFalse(self.worktree.exists())
        self.assertEqual(alternate, self._git(self.source, "rev-parse", "lane/test"))
        self.assertTrue(archive_evidence.is_file())

    def _assert_late_quarantine_file_survives(self, relative: str) -> None:
        sources, mapping = launch._worktree_archive_sources(self.lane)
        manifest = {
            "worktree_files": mapping,
            "gitlinks": [],
            "files": {
                name: {"sha256": launch._sha256_file(path), "size": path.stat().st_size}
                for name, path in sources.items()
            },
        }
        plan = launch._plan_worktree_quarantine(
            self.source, self.lane, manifest, self.base
        )
        self.lane["retirement"] = {
            "phase": "worktree_removal_started",
            "quarantine_path": plan["path"],
            "gitfile": plan["gitfile"],
        }
        real_validate = launch._validate_quarantine_inventory
        calls = 0

        def inject_late(root, value, *, allow_missing):
            nonlocal calls
            calls += 1
            if calls == 2:
                late = root.joinpath(*Path(relative).parts)
                late.parent.mkdir(parents=True, exist_ok=True)
                late.write_text("late writer\n", encoding="utf-8")
            return real_validate(root, value, allow_missing=allow_missing)

        with (
            patch.object(
                launch, "_validate_quarantine_inventory", side_effect=inject_late
            ),
            self.assertRaisesRegex(
                launch.LaunchError, "contents changed after archival"
            ),
        ):
            launch._remove_exact_worktree(self.source, self.lane, manifest, self.base)
        self.assertEqual(
            "late writer\n",
            (self.worktree / relative).read_text(encoding="utf-8"),
        )

    def test_late_ignored_root_file_survives_quarantine_failure(self) -> None:
        self._assert_late_quarantine_file_survives("late-root.tmp")

    def test_late_workspace_file_survives_quarantine_failure(self) -> None:
        self._assert_late_quarantine_file_survives(
            ".agent-workspace/late-workspace.tmp"
        )

    def test_legacy_launch_and_controller_reject_before_spawn_with_fresh_lane_action(
        self,
    ) -> None:
        legacy = {
            **self.lane,
            "lifecycle": "prepared",
            "git": {
                **self.lane["git"],
                "current_tip": self.lane["git"]["bootstrap_tip"],
            },
        }
        legacy["git"].pop("bootstrap_tip")
        legacy.pop("invocation_hash")
        config = SimpleNamespace(runtime_root=self.root / "runtime", root_workspace=self.source)
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(launch, "load_config", return_value=config),
            patch.object(launch, "read_runtime_state", return_value={"state": "OPEN"}),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", legacy)),
            patch.object(launch.processes, "spawn_detached") as spawn,
        ):
            launched = launch.run_launch("lane-1")
        self.assertEqual(launch.LAUNCH_INVOCATION_INVALID, launched["code"])
        self.assertEqual(
            "bootstrap a fresh lane in a fresh epoch", launched["next_action"]
        )
        self.assertIn(review.FRESH_LANE_INTEGRITY_DIAGNOSTIC, launched["summary"])
        spawn.assert_not_called()

        with (
            patch.object(controller, "find_harness_root", return_value=self.root),
            patch.object(controller, "load_config", return_value=config),
            patch.object(controller, "find_active_lane", return_value=("epoch-1", legacy)),
            patch.object(controller, "_load_binding") as load_binding,
            self.assertRaisesRegex(
                controller.ControllerError, "pre-integrity runtime contract"
            ),
        ):
            controller.run_controller("lane-1")
        load_binding.assert_not_called()


class RetirementArchiveSafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="harness-retire-archive-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.runtime = self.root / "runtime"
        self.worktree = self.root / "worktree"
        self.workspace = self.worktree / ".agent-workspace"
        self.workspace.mkdir(parents=True)
        subprocess.run(
            ["git", "init", "--initial-branch=main"],
            cwd=self.worktree,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Harness Test"],
            cwd=self.worktree,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.email", "harness@example.invalid"],
            cwd=self.worktree,
            check=True,
        )
        (self.worktree / "tracked.txt").write_text("base\n", encoding="utf-8")
        subprocess.run(["git", "add", "tracked.txt"], cwd=self.worktree, check=True)
        subprocess.run(
            ["git", "commit", "-m", "base"],
            cwd=self.worktree,
            check=True,
            capture_output=True,
        )
        self.commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=self.worktree,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.folder = (
            self.runtime / "epochs" / "epoch-1" / "lanes" / "lane-1"
        )
        self.folder.mkdir(parents=True)
        workspace_files = {
            "controller.status.json": "{}\n",
            "controller.events.jsonl": "{}\n",
            "provider-transcript.jsonl": "{}\n",
            "provider-stderr.txt": "stderr\n",
            "controller.attempts.jsonl": "{}\n",
            "nested/provider-evidence.json": '{"proof": true}\n',
        }
        for relative, contents in workspace_files.items():
            path = self.workspace / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(contents, encoding="utf-8")
        self.git_identity = {
            "source_root": str(self.worktree.resolve()),
            "common_dir": str((self.worktree / ".git").resolve()),
            "branch": "main",
            "base_commit": self.commit,
            "origin_tip": self.commit,
            "bootstrap_tip": self.commit,
            "harness_owned_paths": [".agent-workspace/**", "RESULT.json"],
        }
        self.task = {
            "schema": "project-task-card/v1",
            "card_id": "card-1",
            "task": "archive safely",
            "branch": "main",
            "base_commit": self.commit,
        }
        atomic_write_json(self.workspace / "task-card.json", self.task)
        self.invocation = {
            "schema": "controller-invocation/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "provider": {"id": "codex", "model": "test-model"},
            "git": self.git_identity,
        }
        self.invocation["content_hash"] = content_hash(self.invocation)
        atomic_write_json(self.workspace / "invocation.json", self.invocation)
        self.result = {
            "schema": "result/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "outcome": "PASS",
            "summary": "archived",
            "evidence": [],
            "completed_at": "2026-09-26T00:00:00Z",
        }
        self.result["content_hash"] = content_hash(self.result)
        atomic_write_json(self.worktree / "RESULT.json", self.result)
        self.review = {
            "schema": "completion-review/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "review_outcome": "PASS",
            "task_card_id": "card-1",
            "task_card_hash": content_hash(self.task),
            "result_id": "run-1",
            "result_hash": self.result["content_hash"],
            "invocation_hash": self.invocation["content_hash"],
            "commit": self.commit,
        }
        self.review["content_hash"] = content_hash(self.review)
        self.acceptance = {
            "schema": "orchestrator-acceptance/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "approval": "ACCEPTED",
            "accepted_by": "ROOT",
            "review_ref": self.review["content_hash"],
            "task_card_id": "card-1",
            "task_card_hash": content_hash(self.task),
            "result_id": "run-1",
            "result_hash": self.result["content_hash"],
            "invocation_hash": self.invocation["content_hash"],
            "commit": self.commit,
        }
        self.acceptance["content_hash"] = content_hash(self.acceptance)
        atomic_write_json(self.folder / "COMPLETION_REVIEW.json", self.review)
        atomic_write_json(
            self.folder / "ORCHESTRATOR_ACCEPTANCE.json", self.acceptance
        )
        self.lane = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "worktree_path": str(self.worktree),
            "provider": {"id": "codex", "model": "test-model"},
            "git": self.git_identity,
            "invocation_hash": self.invocation["content_hash"],
            "task_card_hash": content_hash(self.task),
            "result_validation": {
                "run_id": "run-1",
                "result_hash": self.result["content_hash"],
                "invocation_hash": self.invocation["content_hash"],
                "branch": "main",
                "commit": self.commit,
                "clean": True,
            },
            "acceptance_advancement": self.acceptance,
        }

    def _archive(self) -> Path:
        return launch._archive_retirement_evidence(
            self.runtime,
            "epoch-1",
            self.lane,
            self.review,
            self.acceptance,
            {"commit": self.commit, "clean": True},
            {"controller_gone": True, "cleanup_proven": True},
        )

    def test_archive_recursively_preserves_and_hashes_all_workspace_files(self) -> None:
        archive = self._archive()
        manifest = json.loads(
            (archive / "RETIREMENT_ARCHIVE.json").read_text(encoding="utf-8")
        )
        name = ".agent-workspace/nested/provider-evidence.json"
        archived = archive / name
        self.assertTrue(archived.is_file())
        self.assertEqual(
            launch._sha256_file(archived), manifest["files"][name]["sha256"]
        )
        self.assertEqual(archived.stat().st_size, manifest["files"][name]["size"])
        self.assertEqual(
            archive,
            self._archive(),
            "a validated existing archive must be reusable on retry",
        )

    def test_archive_rejects_symlinks_and_special_files(self) -> None:
        outside = self.root / "outside.txt"
        outside.write_text("outside\n", encoding="utf-8")
        link = self.workspace / "nested" / "escape"
        link.symlink_to(outside)
        with self.assertRaisesRegex(launch.LaunchError, "not a regular file"):
            self._archive()
        link.unlink()

        fifo = self.workspace / "nested" / "evidence.fifo"
        os.mkfifo(fifo)
        with self.assertRaisesRegex(launch.LaunchError, "not a regular file"):
            self._archive()

    def test_existing_archive_rejects_unsafe_manifest_paths(self) -> None:
        archive = self._archive()
        manifest_path = archive / "RETIREMENT_ARCHIVE.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["files"]["../escape"] = {
            "sha256": "0" * 64,
            "size": 0,
        }
        manifest["content_hash"] = content_hash(manifest)
        atomic_write_json(manifest_path, manifest)

        with self.assertRaisesRegex(launch.LaunchError, "unsafe.*inventory path"):
            launch._read_retirement_archive(
                archive, self.lane, self.review, self.acceptance
            )

    def test_existing_archive_rejects_uninventoried_files(self) -> None:
        archive = self._archive()
        (archive / "unlisted.txt").write_text("not inventoried\n", encoding="utf-8")

        with self.assertRaisesRegex(
            launch.LaunchError, "do not exactly match.*inventory"
        ):
            launch._read_retirement_archive(
                archive, self.lane, self.review, self.acceptance
            )

    def test_ignored_file_is_archived_and_bound_before_cleanup(self) -> None:
        (self.worktree / ".git" / "info" / "exclude").write_text(
            "ignored-evidence.log\n", encoding="utf-8"
        )
        ignored = self.worktree / "ignored-evidence.log"
        ignored.write_text("ignored but not disposable\n", encoding="utf-8")

        archive = self._archive()
        manifest = json.loads(
            (archive / "RETIREMENT_ARCHIVE.json").read_text(encoding="utf-8")
        )
        archived_name = manifest["worktree_files"]["ignored-evidence.log"]
        self.assertEqual(
            "worktree-untracked/ignored-evidence.log", archived_name
        )
        archived = archive / archived_name
        self.assertEqual(ignored.read_bytes(), archived.read_bytes())
        self.assertEqual(
            launch._sha256_file(archived),
            manifest["files"][archived_name]["sha256"],
        )

    def test_initialized_gitlink_fails_closed_without_archiving_or_deleting_contents(
        self,
    ) -> None:
        submodule_source = self.root / "submodule-source"
        submodule_source.mkdir()
        subprocess.run(
            ["git", "init", "--initial-branch=main"],
            cwd=submodule_source,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Harness Test"],
            cwd=submodule_source,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.email", "harness@example.invalid"],
            cwd=submodule_source,
            check=True,
        )
        payload = submodule_source / "payload.txt"
        payload.write_text("submodule evidence\n", encoding="utf-8")
        subprocess.run(["git", "add", "payload.txt"], cwd=submodule_source, check=True)
        subprocess.run(
            ["git", "commit", "-m", "submodule"],
            cwd=submodule_source,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "git",
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                str(submodule_source),
                "deps/submodule",
            ],
            cwd=self.worktree,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "add", ".gitmodules", "deps/submodule"],
            cwd=self.worktree,
            check=True,
        )
        subprocess.run(
            ["git", "commit", "-m", "add submodule"],
            cwd=self.worktree,
            check=True,
            capture_output=True,
        )
        initialized_payload = self.worktree / "deps" / "submodule" / "payload.txt"

        with self.assertRaisesRegex(
            launch.LaunchError, "initialized Gitlink prevents safe retirement"
        ):
            launch._worktree_archive_sources(self.lane)

        self.assertEqual("submodule evidence\n", initialized_payload.read_text(encoding="utf-8"))
        self.assertTrue((self.workspace / "invocation.json").is_file())
        self.assertFalse((self.folder / "retirement-archive").exists())

    def test_archive_rejects_semantically_forged_result_and_invocation(self) -> None:
        archive = self._archive()
        manifest_path = archive / "RETIREMENT_ARCHIVE.json"

        forged_result = dict(self.result)
        forged_result["outcome"] = "FAIL"
        forged_result["content_hash"] = content_hash(forged_result)
        atomic_write_json(archive / "RESULT.json", forged_result)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["files"]["RESULT.json"] = {
            "sha256": launch._sha256_file(archive / "RESULT.json"),
            "size": (archive / "RESULT.json").stat().st_size,
        }
        manifest["content_hash"] = content_hash(manifest)
        atomic_write_json(manifest_path, manifest)
        with self.assertRaisesRegex(launch.LaunchError, "accepted result chain"):
            launch._read_retirement_archive(
                archive, self.lane, self.review, self.acceptance
            )

        shutil.rmtree(archive)
        archive = self._archive()
        manifest_path = archive / "RETIREMENT_ARCHIVE.json"
        forged_invocation = dict(self.invocation)
        forged_invocation["provider"] = {"id": "other", "model": "test-model"}
        forged_invocation["content_hash"] = content_hash(forged_invocation)
        invocation_path = archive / ".agent-workspace" / "invocation.json"
        atomic_write_json(invocation_path, forged_invocation)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        key = ".agent-workspace/invocation.json"
        manifest["files"][key] = {
            "sha256": launch._sha256_file(invocation_path),
            "size": invocation_path.stat().st_size,
        }
        manifest["content_hash"] = content_hash(manifest)
        atomic_write_json(manifest_path, manifest)
        with self.assertRaisesRegex(launch.LaunchError, "invocation.*identity"):
            launch._read_retirement_archive(
                archive, self.lane, self.review, self.acceptance
            )

    def test_lane_chain_rejects_forged_task_result_and_force_acceptance(self) -> None:
        for field, forged_value in (
            ("task_card_hash", "f" * 64),
            ("result_id", "another-run"),
        ):
            forged_review = {**self.review, field: forged_value}
            forged_review["content_hash"] = content_hash(forged_review)
            forged_acceptance = {
                **self.acceptance,
                field: forged_value,
                "review_ref": forged_review["content_hash"],
            }
            forged_acceptance["content_hash"] = content_hash(forged_acceptance)
            with self.subTest(field=field):
                self.assertFalse(
                    review.validate_lane_acceptance_chain(
                        forged_review, forged_acceptance, self.lane
                    )
                )

        failed_review = {**self.review, "review_outcome": "FAIL"}
        failed_review["content_hash"] = content_hash(failed_review)
        forced_without_reason = {
            **self.acceptance,
            "review_ref": failed_review["content_hash"],
        }
        forced_without_reason["content_hash"] = content_hash(forced_without_reason)
        self.assertFalse(
            review.validate_lane_acceptance_chain(
                failed_review, forced_without_reason, self.lane
            )
        )

    def test_resume_only_blocks_on_the_canonical_accepted_chain(self) -> None:
        self.assertTrue(
            resume._has_valid_acceptance_chain(
                self.runtime, "epoch-1", self.lane
            )
        )
        malformed = dict(self.acceptance)
        malformed["result_id"] = "forged-run"
        malformed["content_hash"] = content_hash(malformed)
        atomic_write_json(
            self.folder / "ORCHESTRATOR_ACCEPTANCE.json", malformed
        )
        self.assertFalse(
            resume._has_valid_acceptance_chain(
                self.runtime, "epoch-1", self.lane
            ),
            "a malformed pair must not block same-session resume",
        )

    def test_legacy_current_tip_contract_fails_with_stable_fresh_lane_diagnostic(
        self,
    ) -> None:
        legacy = {
            **self.lane,
            "git": {
                **self.git_identity,
                "current_tip": self.git_identity["bootstrap_tip"],
            },
        }
        legacy["git"].pop("bootstrap_tip")
        with self.assertRaisesRegex(
            review.GitStateError,
            "pre-integrity runtime contract; bootstrap a fresh lane in a fresh epoch",
        ):
            review.validate_merge_ready_git(legacy)

    def test_monitor_preserves_legacy_review_evidence_and_emits_migration_diagnostic(
        self,
    ) -> None:
        legacy = {
            **self.lane,
            "git": {
                **self.git_identity,
                "current_tip": self.git_identity["bootstrap_tip"],
            },
        }
        legacy["git"].pop("bootstrap_tip")
        legacy.pop("invocation_hash")
        review_path = self.folder / "COMPLETION_REVIEW.json"
        acceptance_path = self.folder / "ORCHESTRATOR_ACCEPTANCE.json"
        review_before = review_path.read_bytes()
        acceptance_before = acceptance_path.read_bytes()

        with (
            patch.object(monitor, "read_current_epoch", return_value={"epoch_id": "epoch-1"}),
            patch.object(
                monitor,
                "read_epoch_state",
                return_value={"lifecycle": "active", "lane_mode": "plain"},
            ),
            patch.object(
                monitor,
                "reconcile_active_lanes",
                return_value=[{"lane_id": "lane-1"}],
            ),
            patch.object(monitor, "_retained_lanes", return_value=[legacy]),
            patch.object(monitor, "_discover_orphaned_leases", return_value=[]),
            patch.object(monitor, "read_lane", return_value=legacy),
            patch.object(monitor, "read_controller_status", return_value=None),
            patch.object(monitor, "derive_lane_status", return_value=None),
        ):
            count, diagnostics = monitor._monitor_pass(self.runtime, "config")

        self.assertEqual(1, count)
        self.assertTrue(
            any(
                item.get("kind") == "review_recovery"
                and review.FRESH_LANE_INTEGRITY_DIAGNOSTIC in item.get("error", "")
                for item in diagnostics
            ),
            diagnostics,
        )
        self.assertEqual(review_before, review_path.read_bytes())
        self.assertEqual(acceptance_before, acceptance_path.read_bytes())


class RetirementFailureOrderingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix="harness-retire-order-"))
        self.addCleanup(shutil.rmtree, self.root, True)
        self.runtime = self.root / "runtime"
        (self.root / "worktree").mkdir()
        self.acceptance_path = self.root / "acceptance.json"
        self.acceptance = {
            "schema": "orchestrator-acceptance/v1",
            "lane_id": "lane-1",
            "run_id": "run-1",
            "approval": "ACCEPTED",
        }
        self.acceptance["content_hash"] = content_hash(self.acceptance)
        atomic_write_json(self.acceptance_path, self.acceptance)
        self.review = {"commit": "a" * 40, "content_hash": "review"}
        self.lane = {
            "lane_id": "lane-1",
            "run_id": "run-1",
            "lifecycle": "accepted",
            "worktree_path": str(self.root / "worktree"),
            "controller_status_path": str(self.root / "status.json"),
            "result_validation": {
                "run_id": "run-1",
                "result_hash": "result",
                "branch": "lane/test",
                "commit": "a" * 40,
                "clean": True,
            },
        }

    def _run(self, *, archive_error: bool = False, remove_error: bool = False):
        events: list[str] = []

        def update(_rt, _epoch, _lane_id, mutate):
            events.append("update")
            self.lane = mutate(dict(self.lane))
            return dict(self.lane)

        def archive(*_args, **_kwargs):
            events.append("archive")
            if archive_error:
                raise launch.LaunchError(launch.RETIRE_ARCHIVE_FAILED, "archive failed")
            path = self.root / "archive"
            path.mkdir(exist_ok=True)
            return path

        def remove(*_args, **_kwargs):
            events.append("remove")
            if remove_error:
                raise launch.LaunchError(
                    launch.RETIRE_WORKTREE_REMOVE_FAILED, "remove failed"
                )

        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(
                launch,
                "load_config",
                return_value=SimpleNamespace(
                    runtime_root=self.runtime, root_workspace=self.root
                ),
            ),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", self.lane)),
            patch.object(
                launch,
                "_validate_retirement_chain",
                return_value=(self.review, self.acceptance),
            ),
            patch.object(
                launch,
                "_prove_lane_processes_gone",
                return_value=(True, {}, {"controller_gone": True}),
            ),
            patch.object(
                launch,
                "validate_merge_ready_git",
                return_value={
                    "branch": "lane/test",
                    "commit": "a" * 40,
                    "clean": True,
                },
            ),
            patch.object(launch, "_archive_retirement_evidence", side_effect=archive),
            patch.object(
                launch,
                "_read_retirement_archive",
                return_value={"files": {}, "worktree_files": {}},
            ),
            patch.object(
                launch,
                "_plan_worktree_quarantine",
                return_value={
                    "path": str(launch._quarantine_path(self.lane)),
                    "gitfile": {"fixture": True},
                },
            ),
            patch.object(launch, "_remove_exact_worktree", side_effect=remove),
            patch.object(launch, "_prove_retained_branch_tip"),
            patch.object(launch, "update_lane", side_effect=update),
            patch.object(launch, "release_leases", side_effect=lambda *_: events.append("release")),
            patch.object(launch, "read_active_lanes", return_value=[]),
            patch.object(launch, "write_active_lanes", side_effect=lambda *_: events.append("active")),
            patch.object(launch, "_prune_worktrees"),
            patch.object(launch, "_maybe_close_epoch"),
        ):
            result = launch.run_retire(str(self.acceptance_path))
        return result, events

    def test_forged_acceptance_hash_is_rejected(self) -> None:
        forged = dict(self.acceptance)
        forged["approval"] = "REJECTED"
        atomic_write_json(self.acceptance_path, forged)
        with self.assertRaises(launch.LaunchError):
            launch._validate_acceptance_ref(self.acceptance_path)

    def test_acceptance_for_prior_run_is_rejected(self) -> None:
        review_record = {
            "schema": "completion-review/v1",
            "lane_id": "lane-1",
            "run_id": "run-prior",
            "task_card_id": "card-1",
            "task_card_hash": "task-hash",
            "result_id": "result-1",
            "result_hash": "result-hash",
            "invocation_hash": "invocation-hash",
            "commit": "a" * 40,
            "review_outcome": "PASS",
        }
        review_record["content_hash"] = content_hash(review_record)
        acceptance = {
            "schema": "orchestrator-acceptance/v1",
            "lane_id": "lane-1",
            "run_id": "run-prior",
            "task_card_id": "card-1",
            "task_card_hash": "task-hash",
            "result_id": "result-1",
            "result_hash": "result-hash",
            "invocation_hash": "invocation-hash",
            "commit": "a" * 40,
            "approval": "ACCEPTED",
            "accepted_by": "test-reviewer",
            "review_ref": review_record["content_hash"],
        }
        acceptance["content_hash"] = content_hash(acceptance)

        self.assertTrue(
            review.validate_acceptance_chain(
                review_record,
                acceptance,
                lane_id="lane-1",
                run_id="run-prior",
            )
        )
        self.assertFalse(
            review.validate_acceptance_chain(
                review_record,
                acceptance,
                lane_id="lane-1",
                run_id="run-current",
            )
        )

    def test_archive_failure_prevents_remove_release_and_retirement(self) -> None:
        result, events = self._run(archive_error=True)
        self.assertEqual(launch.RETIRE_ARCHIVE_FAILED, result["code"])
        self.assertEqual(["archive"], events)

    def test_remove_failure_occurs_after_archive_and_before_release(self) -> None:
        result, events = self._run(remove_error=True)
        self.assertEqual(launch.RETIRE_WORKTREE_REMOVE_FAILED, result["code"])
        self.assertEqual("archive", events[0])
        self.assertIn("remove", events)
        self.assertNotIn("release", events)
        self.assertNotIn("active", events)

    def test_retry_with_absent_worktree_rejects_moved_retained_branch(self) -> None:
        shutil.rmtree(self.root / "worktree")
        archive_path = (
            self.runtime
            / "epochs"
            / "epoch-1"
            / "lanes"
            / "lane-1"
            / "retirement-archive"
        )
        archive_path.mkdir(parents=True)
        self.lane["retirement"] = {
            "phase": "worktree_removal_started",
            "archive_path": str(archive_path),
        }
        manifest = {
            "process_proof": {
                "controller_gone": True,
                "provider_boundary_gone": True,
                "provider_gone": True,
                "cleanup_proven": True,
            }
        }
        config = SimpleNamespace(runtime_root=self.runtime, root_workspace=self.root)
        with (
            patch.object(launch, "find_harness_root", return_value=self.root),
            patch.object(launch, "load_config", return_value=config),
            patch.object(launch, "find_active_lane", return_value=("epoch-1", self.lane)),
            patch.object(
                launch,
                "_validate_retirement_chain",
                return_value=(self.review, self.acceptance),
            ),
            patch.object(launch, "_read_retirement_archive", return_value=manifest),
            patch.object(launch, "_worktree_registered", return_value=False),
            patch.object(
                launch,
                "_prove_retained_branch_tip",
                side_effect=launch.LaunchError(
                    launch.RETIRE_WORKTREE_REMOVE_FAILED,
                    "retained lane branch changed from the accepted commit",
                ),
            ),
            patch.object(launch, "release_leases") as release,
        ):
            result = launch.run_retire(str(self.acceptance_path))
        self.assertEqual(launch.RETIRE_WORKTREE_REMOVE_FAILED, result["code"])
        self.assertIn("retained lane branch changed", result["summary"])
        release.assert_not_called()


if __name__ == "__main__":
    unittest.main()
