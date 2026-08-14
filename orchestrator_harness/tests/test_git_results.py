from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness.discovery import discover_run
from orchestrator_harness.git_safety import (
    GitDeclaration,
    GitSafetyError,
    _same_path,
    active_declaration_conflicts,
    declaration_from_invocation,
    inspect_repository,
    invalid_result_evidence,
    validate_coding_result,
)
from orchestrator_harness.models import ProcessInfo, ProcessSnapshot, iso_utc
from orchestrator_harness.tests.support import (
    NOW,
    SuiteFixture,
    TemporaryGitRepository,
    write_json,
)


class GitResultTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.repository = TemporaryGitRepository.create(self.root / "run")
        self.declaration = GitDeclaration(
            self.repository.common_dir,
            self.repository.root.resolve(),
            self.repository.branch,
            self.repository.head,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def result(self, **changes: object) -> dict[str, object]:
        return {
            "schema": "orchestrator-lane-result/v1",
            "lane_id": "coding:one",
            "worker_invocation_id": "worker-1",
            "branch": self.repository.branch,
            "commit": self.repository.head,
            "outcome": "PASS",
            "summary": "finished",
            "checks": [
                {"name": "unit", "command": "never execute this", "outcome": "PASS"}
            ],
            **changes,
        }

    def test_ordinary_clone_and_linked_worktree_identity(self) -> None:
        clone = self.root / "clone"
        self.repository.git("clone", str(self.repository.root), str(clone))
        cloned = TemporaryGitRepository(clone, self.repository.branch)
        linked = self.repository.linked_worktree(self.root / "linked", "coding-linked")
        for repo in (cloned, linked):
            identity = inspect_repository(
                GitDeclaration(repo.common_dir, repo.root, repo.branch, repo.head)
            )
            self.assertEqual(repo.root.resolve(), identity.worktree_root)
            self.assertEqual(repo.branch, identity.branch)
            self.assertEqual(repo.head, identity.base_commit)

    def test_repository_declaration_rejects_non_git_detached_wrong_branch_and_missing_base(
        self,
    ) -> None:
        raw = {"repository": self.repository.declaration()}
        declaration_from_invocation(raw, self.repository.root)
        non_git = self.root / "not-git"
        non_git.mkdir()
        bad = {
            "repository": {
                **self.repository.declaration(),
                "worktree_root": str(non_git),
            }
        }
        with self.assertRaisesRegex(GitSafetyError, "must equal run_root"):
            declaration_from_invocation(bad, self.repository.root)
        non_git_declaration = GitDeclaration(
            self.declaration.common_dir,
            non_git,
            self.declaration.branch,
            self.declaration.base_commit,
        )
        with self.assertRaises(GitSafetyError):
            inspect_repository(non_git_declaration)
        wrong = GitDeclaration(
            self.declaration.common_dir,
            self.declaration.worktree_root,
            "other",
            self.declaration.base_commit,
        )
        with self.assertRaisesRegex(GitSafetyError, "does not match"):
            inspect_repository(wrong)
        missing = GitDeclaration(
            self.declaration.common_dir,
            self.declaration.worktree_root,
            self.declaration.branch,
            "f" * 40,
        )
        with self.assertRaises(GitSafetyError):
            inspect_repository(missing)
        self.repository.git("checkout", "--detach")
        with self.assertRaisesRegex(GitSafetyError, "attached branch"):
            inspect_repository(self.declaration)

    def test_windows_path_comparison_normalizes_case(self) -> None:
        with (
            patch("orchestrator_harness.git_safety.os.name", "nt"),
            patch(
                "orchestrator_harness.git_safety.os.path.normcase",
                side_effect=str.lower,
            ),
        ):
            self.assertTrue(_same_path(self.root / "Run", self.root / "run"))

    def test_active_conflicts_accept_arbitrary_status_name_and_ignore_exited(
        self,
    ) -> None:
        workspace = self.repository.root / ".agent-workspace"
        workspace.mkdir()
        status = workspace / "permitted-any-name.json"
        started = NOW
        write_json(
            status,
            {
                "schema": "orchestrator-lane-controller/v1",
                "invocation_schema": "orchestrator-coding-invocation/v1",
                "state": "RUNNING_CODEX",
                "controller_pid": 11,
                "codex_pid": 12,
                "controller_started_utc": iso_utc(started),
                "codex_started_utc": iso_utc(started),
                "repository": self.repository.declaration(),
            },
        )
        snapshot = ProcessSnapshot(
            True,
            (
                ProcessInfo(11, 1, "python", "controller", started),
                ProcessInfo(12, 11, "codex", "codex", started),
            ),
        )
        conflicts = active_declaration_conflicts(
            self.declaration,
            current_status_path=self.root / "none.json",
            snapshot=snapshot,
        )
        self.assertEqual([f"worktree+branch already ACTIVE in {status}"], conflicts)
        status_value = json.loads(status.read_text(encoding="utf-8"))
        status_value["state"] = "EXITED"
        write_json(status, status_value)
        self.assertEqual(
            [],
            active_declaration_conflicts(
                self.declaration,
                current_status_path=self.root / "none.json",
                snapshot=snapshot,
            ),
        )

    def test_result_matrix_valid_identity_dirty_and_shape_checks(self) -> None:
        self.assertEqual(
            self.repository.head,
            validate_coding_result(
                self.result(),
                lane_id="coding:one",
                worker_invocation_id="worker-1",
                declaration=self.declaration,
            ).head_commit,
        )
        for changes, message in (
            ({"lane_id": "wrong"}, "lane_id"),
            ({"worker_invocation_id": "wrong"}, "worker_invocation_id"),
            ({"branch": "wrong"}, "branch"),
            ({"commit": "0" * 40}, "branch tip"),
            ({"outcome": "MAYBE"}, "outcome"),
            ({"summary": ""}, "summary"),
            ({"checks": [{"name": "x", "outcome": "MAYBE"}]}, "outcome"),
        ):
            with (
                self.subTest(changes=changes),
                self.assertRaisesRegex(GitSafetyError, message),
            ):
                validate_coding_result(
                    self.result(**changes),
                    lane_id="coding:one",
                    worker_invocation_id="worker-1",
                    declaration=self.declaration,
                )
        (self.repository.root / "tracked.txt").write_text("dirty\n", encoding="utf-8")
        with self.assertRaisesRegex(GitSafetyError, "clean"):
            validate_coding_result(
                self.result(),
                lane_id="coding:one",
                worker_invocation_id="worker-1",
                declaration=self.declaration,
            )
        self.repository.git("checkout", "--", "tracked.txt")
        stale = self.repository.head
        (self.repository.root / ".gitignore").write_text(
            ".agent-workspace/\n", encoding="utf-8"
        )
        self.repository.git("add", ".gitignore")
        self.repository.git("commit", "-m", "ignore runtime")
        with self.assertRaisesRegex(GitSafetyError, "branch tip"):
            validate_coding_result(
                self.result(commit=stale),
                lane_id="coding:one",
                worker_invocation_id="worker-1",
                declaration=GitDeclaration(
                    self.repository.common_dir,
                    self.repository.root,
                    self.repository.branch,
                    self.repository.head,
                ),
            )
        (self.repository.root / ".agent-workspace").mkdir()
        (self.repository.root / ".agent-workspace" / "runtime").write_text(
            "ok", encoding="utf-8"
        )
        self.assertEqual(
            self.repository.head,
            validate_coding_result(
                self.result(commit=self.repository.head),
                lane_id="coding:one",
                worker_invocation_id="worker-1",
                declaration=GitDeclaration(
                    self.repository.common_dir,
                    self.repository.root,
                    self.repository.branch,
                    self.repository.head,
                ),
            ).head_commit,
        )

    def test_invalid_evidence_is_bounded(self) -> None:
        evidence = invalid_result_evidence(
            self.root / "RESULT.json", "x" * 900, sha256="hash"
        )
        self.assertEqual("CODING_RESULT_INVALID", evidence["code"])
        self.assertEqual(500, len(str(evidence["detail"])))

    def test_git_environment_is_sanitized(self) -> None:
        with patch.dict(
            os.environ,
            {"GIT_DIR": str(self.root / "poisoned"), "GIT_WORK_TREE": str(self.root)},
            clear=False,
        ):
            self.assertEqual(
                self.repository.branch, inspect_repository(self.declaration).branch
            )


class DiscoveryCodingResultTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = SuiteFixture.create()
        self.run_root = self.fixture.workspace().parent
        self.repository = TemporaryGitRepository.create(self.run_root)
        (self.run_root / ".gitignore").write_text(
            ".agent-workspace/\n", encoding="utf-8"
        )
        self.repository.git("add", ".gitignore")
        self.repository.git("commit", "-m", "ignore runtime")
        self.workspace = self.run_root / ".agent-workspace"
        self.workspace.mkdir(exist_ok=True)
        self.status = self.workspace / "coding-status.json"

    def tearDown(self) -> None:
        self.fixture.close()

    def status_value(
        self, *, lane: str = "coding:one", worker: str = "worker-1"
    ) -> dict[str, object]:
        return {
            "schema": "orchestrator-lane-controller/v1",
            "invocation_schema": "orchestrator-coding-invocation/v1",
            "state": "CODEX_EXITED",
            "controller_pid": 1,
            "codex_pid": 2,
            "declared_lane_id": lane,
            "worker_invocation_id": worker,
            "repository": self.repository.declaration(),
        }

    def result(self, **changes: object) -> dict[str, object]:
        return {
            "schema": "orchestrator-lane-result/v1",
            "lane_id": "coding:one",
            "worker_invocation_id": "worker-1",
            "branch": self.repository.branch,
            "commit": self.repository.head,
            "outcome": "PASS",
            "summary": "ok",
            "checks": [],
            **changes,
        }

    def test_discovery_exact_binding_invalid_evidence_and_corrected_clearing(
        self,
    ) -> None:
        write_json(self.status, self.status_value())
        result_path = self.workspace / "RESULT.json"
        write_json(result_path, self.result(worker_invocation_id="other"))
        first = discover_run(self.run_root, self.workspace, self.fixture.config)
        self.assertIsNone(first.result)
        self.assertEqual(
            "CODING_RESULT_INVALID",
            first.invalid_result["code"] if first.invalid_result else None,
        )
        self.assertEqual(self.status, first.invalid_result_status_path)
        write_json(result_path, self.result())
        corrected = discover_run(self.run_root, self.workspace, self.fixture.config)
        self.assertIsNotNone(corrected.result)
        self.assertIsNone(corrected.invalid_result)

    def test_firmware_result_is_rejected_by_coding_lane_but_firmware_is_unaffected(
        self,
    ) -> None:
        write_json(self.status, self.status_value())
        write_json(self.workspace / "RESULT.json", {"status": "PASS"})
        coding = discover_run(self.run_root, self.workspace, self.fixture.config)
        self.assertIsNone(coding.result)
        self.assertEqual("CODING_RESULT_INVALID", coding.errors[0].code)
        firmware_workspace = self.fixture.workspace("firmware")
        write_json(
            firmware_workspace / "legacy.json",
            {"state": "exited", "controller_pid": 1, "codex_pid": 2},
        )
        write_json(firmware_workspace / "RESULT.json", {"status": "PASS"})
        firmware = discover_run(
            firmware_workspace.parent, firmware_workspace, self.fixture.config
        )
        self.assertIsNotNone(firmware.result)


if __name__ == "__main__":
    unittest.main()
