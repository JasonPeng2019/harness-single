"""Real linked Git fixtures for effective grants and trusted source reads."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from orchestrator_harness import git_source as source
from orchestrator_harness.bootstrap import BootstrapError, _install_codex_worker_isolation


class GitSourceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.root, self.worker = self.base / "root", self.base / "worker"
        self.root.mkdir()
        self.git(self.root, "init", "-q")
        self.git(self.root, "-c", "user.name=Fixture", "-c", "user.email=fixture@local",
                 "commit", "--allow-empty", "-qm", "base")
        self.git(self.root, "worktree", "add", "-qb", "lane/worker/work", str(self.worker))
        self.binding = source.capture(self.root, self.worker, branch="lane/worker/work")
        self.lane = {"worktree_path": str(self.worker), "git_source": self.binding}

    def git(self, root, *arguments):
        return subprocess.check_output(["git", "-C", str(root), *arguments], text=True).strip()

    def test_real_commit_advances_assigned_branch_and_keeps_origin(self):
        base = self.binding["base_commit"]
        (self.worker / "change.txt").write_text("commit\n")
        self.git(self.worker, "add", "change.txt")
        self.git(self.worker, "-c", "user.name=Fixture", "-c", "user.email=fixture@local", "commit", "-qm", "change")
        tip = source.current_tip(self.lane)
        self.assertNotEqual(base, tip)
        self.assertEqual(base, self.binding["base_commit"])
        self.assertEqual(self.git(self.worker, "rev-parse", "HEAD"), tip)
        self.assertEqual(base, self.git(self.root, "rev-parse", "HEAD"))

    def test_every_private_redirect_change_is_refused(self):
        private = Path(self.binding["private_dir"])
        paths = [self.worker / ".git"] + [private / name for name in ("HEAD", "commondir", "gitdir")]
        for path in paths:
            with self.subTest(path=path.name):
                original = path.read_bytes()
                original_mode = path.stat().st_mode
                try:
                    # Git marks the Windows pointer read-only. This is an
                    # owned fixture mutation to exercise ROOT's independent
                    # identity check, not a production permission change.
                    path.chmod(original_mode | stat.S_IWUSR)
                    # Windows also hides .git: CREATE_ALWAYS can reject a
                    # hidden existing file, so mutate the held existing file.
                    with path.open("r+b") as stream:
                        stream.write(original + b"\n")
                        stream.truncate()
                    with self.assertRaisesRegex(ValueError, "pointer changed|redirect changed"):
                        source.current_tip(self.lane)
                finally:
                    with path.open("r+b") as stream:
                        stream.write(original)
                        stream.truncate()
                    path.chmod(original_mode)

    def test_missing_redirect_and_replaced_private_directory_are_refused(self):
        private = Path(self.binding["private_dir"])
        head = private / "HEAD"
        original = head.read_bytes()
        head.unlink()
        with self.assertRaises(OSError):
            source.current_tip(self.lane)
        head.write_bytes(original)
        saved = private.with_name(private.name + "-saved")
        private.rename(saved)
        shutil.copytree(saved, private)
        with self.assertRaisesRegex(ValueError, "directory changed"):
            source.current_tip(self.lane)

    def test_unrelated_branch_tip_is_refused_without_base_fallback(self):
        tree = self.git(self.root, "rev-parse", "HEAD^{tree}")
        orphan = subprocess.check_output(
            ["git", "-C", str(self.root), "-c", "user.name=Fixture", "-c", "user.email=fixture@local",
             "commit-tree", tree, "-m", "unrelated"], text=True).strip()
        self.git(self.root, "update-ref", self.binding["branch_ref"], orphan)
        with self.assertRaisesRegex(ValueError, "protected origin"):
            source.current_tip(self.lane)

    def test_root_config_change_and_missing_authority_are_refused(self):
        with self.assertRaisesRegex(ValueError, "missing or corrupt"):
            source.current_tip({"worktree_path": str(self.worker)})
        self.git(self.root, "config", "fixture.changed", "true")
        with self.assertRaisesRegex(ValueError, "configuration changed"):
            source.current_tip(self.lane)

    def test_object_store_alternates_cannot_redirect_operator_reads(self):
        alternates = Path(self.binding["common_dir"]) / "objects/info/alternates"
        alternates.write_text(str(self.base / "foreign-objects") + "\n")
        with self.assertRaisesRegex(ValueError, "object store or alternates changed"):
            source.current_tip(self.lane)

    def test_protected_invocation_retains_git_origin_across_fresh_run(self):
        from orchestrator_harness.bootstrap import _write_invocation, _write_worker_prompt
        from orchestrator_harness.lane_authority import read_invocation, stage_authority
        task = {"schema": "project-task-card/v1", "task": "Test source ownership",
                "acceptance_criteria": ["Git identity remains assigned"], "deliverables": ["Commit"],
                "reason_for_acceptance_and_deliverables": "Validate exact source", "base_commit": "HEAD"}
        provider = {"id": "claude-code", "model": "fixture", "launch_config": {"effort": "high"}}
        lane = {"lane_id": "worker", "run_id": "first-run", "worktree_path": str(self.worker),
                "branch": "lane/worker/work", "provider": provider, "exclusive_resources": []}
        runtime = self.root / ".harness-runtime"
        def prepare(record):
            _write_worker_prompt(self.worker, task, managed=False)
            _write_invocation(self.worker, lane_id=record["lane_id"], run_id=record["run_id"],
                              provider_id=provider["id"], model=provider["model"],
                              launch_config=provider["launch_config"], exclusive_resources=[])
            return stage_authority(runtime, "epoch", record, task)
        first = prepare(lane)
        self.assertEqual(first["git_source"]["content_hash"], read_invocation(first)["git_source_hash"])
        second = prepare({**first, "run_id": "second-run"})
        self.assertEqual(first["git_source"], second["git_source"])
        self.assertEqual("second-run", read_invocation(second)["run_id"])
        private_head = Path(second["git_source"]["private_dir"]) / "HEAD"
        private_head.write_bytes(private_head.read_bytes() + b"\n")
        with self.assertRaisesRegex(ValueError, "redirect changed"):
            read_invocation(second)

    def test_inherited_git_redirects_and_worker_config_are_not_used(self):
        private = Path(self.binding["private_dir"])
        (private / "config.worktree").write_text("[core]\n\tfsmonitor = missing-malicious-helper\n")
        with patch.dict(os.environ, {
            "GIT_DIR": str(self.base / "foreign"), "GIT_COMMON_DIR": str(self.base / "foreign"),
            "GIT_INDEX_FILE": str(self.base / "foreign-index"), "GIT_WORK_TREE": str(self.base / "foreign"),
            "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "core.fsmonitor",
            "GIT_CONFIG_VALUE_0": "missing-injected-helper", "GIT_CONFIG_PARAMETERS": "invalid injected config",
        }):
            self.assertEqual(self.binding["base_commit"], source.current_tip(self.lane))
            self.assertEqual(b"", source.git(self.binding, "status", "--porcelain", "--untracked-files=no"))

    def test_shared_parent_and_flat_branch_permissions_are_refused(self):
        peer = self.base / "peer"
        self.git(self.root, "worktree", "add", "-qb", "lane/worker/peer", str(peer))
        with self.assertRaisesRegex(BootstrapError, "another branch"):
            _install_codex_worker_isolation(self.worker, self.base / "harness", self.root)
        self.assertFalse((self.worker / ".codex/config.toml").exists())
        with self.assertRaisesRegex(ValueError, "dedicated parent"):
            source.require_exclusive_namespace(self.root, "refs/heads/flat")

    def test_packed_peer_ref_is_also_protected(self):
        self.git(self.root, "branch", "lane/worker/packed-peer")
        self.git(self.root, "pack-refs", "--all")
        with self.assertRaisesRegex(ValueError, "another branch"):
            source.require_exclusive_namespace(self.root, self.binding["branch_ref"])

    def test_new_peer_ref_in_reserved_namespace_invalidates_launch_source(self):
        lane = {**self.lane, "git_write_namespace": "refs/heads/lane/worker"}
        self.assertEqual(self.binding["base_commit"], source.current_tip(lane))
        self.git(self.root, "branch", "lane/worker/new-peer")
        with self.assertRaisesRegex(ValueError, "another branch"):
            source.current_tip(lane)

    def test_retired_namespace_reservation_survives_ref_deletion(self):
        runtime = self.root / ".harness-runtime"
        record = runtime / "epochs/prior/lanes/old/lane.json"
        record.parent.mkdir(parents=True)
        record.write_text(json.dumps({"schema": "lane/v1", "lane_id": "old", "run_id": "old-run",
            "lifecycle": "retired", "git_source": self.binding, "git_write_namespace": "refs/heads/lane/worker"}))
        self.git(self.root, "update-ref", "-d", self.binding["branch_ref"])
        for branch in ("lane/worker/new", "lane/worker/nested/new"):
            with self.subTest(branch=branch):
                with self.assertRaisesRegex(ValueError, "reserved by a retained lane"):
                    source.require_exclusive_namespace(self.root, "refs/heads/" + branch, rt=runtime)
        self.assertEqual("refs/heads/lane/fresh", source.require_exclusive_namespace(
            self.root, "refs/heads/lane/fresh/work", rt=runtime))

    @unittest.skipUnless(os.name == "nt", "Windows metadata path casing contract")
    def test_equivalent_windows_metadata_casing_cannot_evade_retained_reservation(self):
        runtime = self.root / ".harness-runtime"
        record = runtime / "epochs/prior/lanes/old/lane.json"
        record.parent.mkdir(parents=True)
        binding = dict(self.binding, common_dir=self.binding["common_dir"].swapcase())
        self.assertNotEqual(self.binding["common_dir"], binding["common_dir"])
        binding["content_hash"] = source.content_hash(binding)
        record.write_text(json.dumps({"schema": "lane/v1", "lane_id": "old", "run_id": "old-run",
            "lifecycle": "retired", "git_source": binding, "git_write_namespace": "refs/heads/lane/worker"}))
        self.git(self.root, "update-ref", "-d", self.binding["branch_ref"])
        with self.assertRaisesRegex(ValueError, "reserved by a retained lane"):
            source.require_exclusive_namespace(self.root, "refs/heads/lane/worker/new", rt=runtime)

    def test_explicit_yolo_profile_does_not_grant_shared_git_parent(self):
        self.git(self.root, "branch", "lane/worker/peer")
        _install_codex_worker_isolation(self.worker, self.base / "harness", self.root, enforce_git_isolation=False)
        import tomllib
        profile = tomllib.loads((self.worker / ".codex/config.toml").read_text())
        writes = [key for key, value in profile["permissions"]["worker-isolated"]["filesystem"].items()
                  if value == "write"]
        self.assertEqual([], writes)


if __name__ == "__main__":
    unittest.main()
