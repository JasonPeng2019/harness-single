"""Regression checks for run-local harness/ROOT permission boundaries."""
from pathlib import Path
import os
import subprocess
import tempfile
import tomllib
import unittest

from orchestrator_harness.bootstrap import BootstrapError, _install_codex_worker_isolation


class WorkerIsolationBoundaryTests(unittest.TestCase):
    def test_manager_ancestor_protects_hooks_without_denying_product_or_git_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = Path(directory) / "manager"
            root = manager / "development/product"
            harness = manager / "harness"
            lane = root / ".harness-runtime/worktrees/epoch/worker"
            root.mkdir(parents=True)
            harness.mkdir()
            protected = []
            for leaf in (".codex", ".claude", ".qwen", ".agent-workspace"):
                target = manager / leaf
                target.mkdir()
                sentinel = target / "sentinel.txt"
                sentinel.write_text("protected ROOT\n", encoding="utf-8")
                protected.append(sentinel)
            def git(tree, *args):
                return subprocess.check_output(["git", "-C", str(tree), *args], text=True).strip()
            git(root, "init", "-q")
            git(root, "-c", "user.name=Fixture", "-c", "user.email=fixture@local",
                "commit", "--allow-empty", "-qm", "base")
            git(root, "worktree", "add", "-qb", "lane/worker/work", str(lane))
            _install_codex_worker_isolation(lane, harness, root, manager_workspace=manager)
            filesystem = tomllib.loads((lane / ".codex/config.toml").read_text())["permissions"]["worker-isolated"]["filesystem"]
            for sentinel in protected:
                self.assertEqual("deny", filesystem[str(sentinel.parent)])
                self.assertEqual("protected ROOT\n", sentinel.read_text(encoding="utf-8"))
            for name, mode in filesystem.items():
                if not name.startswith(":") and mode == "deny":
                    self.assertNotIn(Path(name), root.parents)
                    self.assertNotIn(Path(name), lane.parents)
            self.assertNotIn(str(manager), filesystem)
            (lane / "change.txt").write_text("worker change\n", encoding="utf-8")
            git(lane, "add", "change.txt")
            git(lane, "-c", "user.name=Fixture", "-c", "user.email=fixture@local", "commit", "-qm", "worker")
            self.assertEqual("worker", git(lane, "show", "--format=%s", "--no-patch"))

    def test_redirected_manager_is_refused_before_worker_config_write(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            real = base / "real-manager"
            real.mkdir()
            sentinel = real / "sentinel.txt"
            sentinel.write_text("unchanged\n", encoding="utf-8")
            manager = base / "redirected-manager"
            if os.name == "nt":
                subprocess.run(["cmd", "/c", "mklink", "/J", str(manager), str(real)],
                               check=True, capture_output=True)
            else:
                manager.symlink_to(real, target_is_directory=True)
            try:
                root = base / "product"
                root.mkdir()
                lane = root / ".harness-runtime/worktrees/epoch/worker"
                with self.assertRaisesRegex(BootstrapError, "invalid manager authority"):
                    _install_codex_worker_isolation(lane, base / "harness", root, manager_workspace=manager)
                self.assertFalse((lane / ".codex/config.toml").exists())
                self.assertEqual("unchanged\n", sentinel.read_text(encoding="utf-8"))
            finally:
                if os.name == "nt": manager.rmdir()
                else: manager.unlink()

    def test_manager_provider_directory_containing_product_is_a_named_conflict(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = Path(directory) / "manager"
            root = manager / ".codex/product"
            root.mkdir(parents=True)
            lane = root / ".harness-runtime/worktrees/epoch/worker"
            with self.assertRaisesRegex(BootstrapError, "manager ROOT destination contains"):
                _install_codex_worker_isolation(lane, manager / "harness", root, manager_workspace=manager)
            self.assertFalse((lane / ".codex/config.toml").exists())

    def test_linked_worker_can_commit_without_root_index_or_config_grants(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root = base / "run" / "workspace"
            harness = base / "run" / "harness"
            lane = root / ".harness-runtime" / "worktrees" / "epoch" / "worker"
            root.mkdir(parents=True)
            harness.mkdir()
            def git(path, *args):
                return subprocess.check_output(["git", "-C", str(path), *args], text=True).strip()
            git(root, "init", "-q")
            git(root, "-c", "user.name=Codex", "-c", "user.email=codex@local",
                "commit", "--allow-empty", "-qm", "fixture")
            git(root, "worktree", "add", "-qb", "lane/worker/work", str(lane))
            other = base / "other-worker"
            git(root, "worktree", "add", "-qb", "lane/other/work", str(other))
            root_branch = git(root, "symbolic-ref", "HEAD")
            other_git = Path(git(other, "rev-parse", "--absolute-git-dir"))
            protected = [root / ".git" / name for name in (
                "index", "config", "hooks", root_branch, "logs/" + root_branch)]
            protected += [other_git / "index", root / ".git/refs/heads/lane/other/work"]
            before = {path: path.read_bytes() for path in protected if path.is_file()}
            _install_codex_worker_isolation(lane, harness, root)
            config = tomllib.loads((lane / ".codex/config.toml").read_text())
            filesystem = config["permissions"]["worker-isolated"]["filesystem"]
            common = Path(git(root, "rev-parse", "--path-format=absolute", "--git-common-dir")).resolve()
            lane_git = Path(git(lane, "rev-parse", "--absolute-git-dir")).resolve()
            writes = {Path(name) for name, mode in filesystem.items() if mode == "write"}
            self.assertEqual({lane_git, common / "objects", common / "refs/heads/lane/worker",
                              common / "logs/refs/heads/lane/worker"}, writes)
            for path in protected + [other_git / "index.lock"]:
                self.assertFalse(any(path == grant or grant in path.parents for grant in writes), path)
            # Directory grants intentionally include private redirects; ROOT
            # detects changes through its protected source assignment instead
            # of asserting an ineffective read-only leaf policy.
            self.assertIn(lane_git, writes)
            self.assertNotIn(common, writes)
            self.assertNotIn(str(lane / ".git"), filesystem)
            self.assertEqual("deny", filesystem[str(harness.resolve())])
            (lane / "change.txt").write_text("worker commit\n", encoding="utf-8")
            git(lane, "add", "change.txt")
            git(lane, "-c", "user.name=Codex", "-c", "user.email=codex@local",
                "commit", "-qm", "worker commit")
            self.assertEqual("worker commit", git(lane, "show", "--format=%s", "--no-patch"))
            self.assertEqual(before, {path: path.read_bytes() for path in before})

    def test_foreign_linked_worktree_and_ordinary_repository_are_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            roots = [base / "root", base / "foreign"]
            for root in roots:
                root.mkdir()
                subprocess.run(["git", "-C", str(root), "init", "-q"], check=True)
                subprocess.run(["git", "-C", str(root), "-c", "user.name=Fixture",
                    "-c", "user.email=fixture@local", "commit", "--allow-empty", "-qm", "base"], check=True)
            lane = base / "lane"
            subprocess.run(["git", "-C", str(roots[1]), "worktree", "add", "-qb", "lane/foreign", str(lane)], check=True)
            for candidate in (lane, roots[1]):
                with self.assertRaisesRegex(BootstrapError, "linked worktree"):
                    _install_codex_worker_isolation(candidate, base / "harness", roots[0])
                self.assertFalse((candidate / ".codex/config.toml").exists())

    def test_run_parent_never_denies_its_root_or_lane(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory) / "batch" / "r01"
            root = run / "workspace"
            harness = run / "harness"
            results = run / "results"
            lane = root / ".harness-runtime" / "worktrees" / "epoch" / "worker"
            for path in (harness, results, lane):
                path.mkdir(parents=True, exist_ok=True)
            _install_codex_worker_isolation(lane, harness, root)
            config = tomllib.loads((lane / ".codex/config.toml").read_text())
            filesystem = config["permissions"]["worker-isolated"]["filesystem"]
            self.assertEqual("deny", filesystem[str(harness.resolve())])
            self.assertEqual("deny", filesystem[str(results.resolve())])
            self.assertEqual("write", filesystem[":workspace_roots"]["."])
            for name, mode in filesystem.items():
                if name.startswith(":") or mode != "deny":
                    continue
                denied = Path(name)
                self.assertNotEqual(root.resolve(), denied)
                self.assertNotIn(denied, root.resolve().parents)
                self.assertNotIn(denied, lane.resolve().parents)

    def test_separate_harness_remains_denied(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root = base / "runs" / "workspace"
            harness = base / "installation" / "harness"
            lane = root / ".harness-runtime" / "worktrees" / "epoch" / "worker"
            harness.mkdir(parents=True)
            lane.mkdir(parents=True)
            _install_codex_worker_isolation(lane, harness, root)
            config = tomllib.loads((lane / ".codex/config.toml").read_text())
            filesystem = config["permissions"]["worker-isolated"]["filesystem"]
            self.assertEqual("deny", filesystem[str(harness.resolve())])


if __name__ == "__main__":
    unittest.main()
