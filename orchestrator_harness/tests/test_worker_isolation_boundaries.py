"""Regression checks for run-local harness/ROOT permission boundaries."""
from pathlib import Path
import subprocess
import tempfile
import tomllib
import unittest

from orchestrator_harness.bootstrap import _install_codex_worker_isolation


class WorkerIsolationBoundaryTests(unittest.TestCase):
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
            git(root, "worktree", "add", "-qb", "lane/worker", str(lane))
            _install_codex_worker_isolation(lane, harness, root)
            config = tomllib.loads((lane / ".codex/config.toml").read_text())
            filesystem = config["permissions"]["worker-isolated"]["filesystem"]
            common = Path(git(root, "rev-parse", "--path-format=absolute", "--git-common-dir")).resolve()
            lane_git = Path(git(lane, "rev-parse", "--absolute-git-dir")).resolve()
            writes = {Path(name) for name, mode in filesystem.items() if mode == "write"}
            self.assertEqual({lane_git, common / "objects", common / "refs/heads",
                              common / "logs/refs/heads"}, writes)
            self.assertNotIn(common, writes)
            self.assertNotIn(str(lane / ".git"), filesystem)
            self.assertEqual("deny", filesystem[str(harness.resolve())])

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
