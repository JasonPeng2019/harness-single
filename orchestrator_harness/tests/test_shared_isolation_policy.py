"""Regression checks for cross-profile native read restrictions and Git layout."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import tomllib
import unittest
from unittest.mock import patch

from orchestrator_harness import bootstrap
from orchestrator_harness import isolation_policy as policy


class SharedPolicyTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="shared-policy-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.product = self.base / "development/product"
        self.product.mkdir(parents=True)
        self.manager = self.base / "manager"
        self.manager.mkdir()
        self.engine = self.base / "engine"
        self.engine.mkdir()
        self.home = self.base / "home"
        self.home.mkdir()
        self.codex_home = self.home / "codex-home"
        self.codex_home.mkdir()

    def denials(self):
        return policy.shared_read_denials(self.engine, self.product, self.manager,
                                         home=self.home, codex_home=self.codex_home)

    def git(self, *args, cwd=None):
        result = subprocess.run(["git", *map(str, args)], cwd=cwd,
                                capture_output=True, text=True, timeout=30,
                                env=dict(os.environ, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull))
        self.assertEqual(0, result.returncode, result.stderr)
        return result.stdout.strip()

    def linked_worker(self, common):
        common.parent.mkdir(parents=True, exist_ok=True)
        self.git("init", "--separate-git-dir", common, self.product)
        self.git("-C", self.product, "-c", "user.name=Private fixture", "-c", "user.email=fixture@example.invalid",
                 "commit", "--allow-empty", "-m", "private policy fixture")
        worker = self.product / ".harness-runtime/worktrees/epoch/lane"
        self.git("-C", self.product, "worktree", "add", "-b", "lane/shared-policy/work", worker)
        return worker

    def install(self, worker):
        with patch.object(Path, "home", return_value=self.home), patch.dict(os.environ, {"CODEX_HOME": str(self.codex_home)}):
            bootstrap._install_codex_worker_isolation(worker, self.engine, self.product,
                                                      manager_workspace=self.manager)

    def test_shared_policy_denies_operator_and_existing_credential_locations(self):
        denied = self.denials()
        expected = {self.codex_home, self.engine, self.manager / ".git", self.product / ".secrets",
                    self.product / ".harness-runtime/operator-only",
                    *(self.manager / leaf for leaf in (".codex", ".claude", ".qwen", ".agent-workspace")),
                    *(self.home / leaf for leaf in (".codex", ".ssh", ".aws", ".azure", ".config/gcloud",
                      ".docker", ".kube", ".npmrc", ".netrc", ".git-credentials", ".claude", ".qwen"))}
        self.assertTrue(expected.issubset(denied))

    def test_run_local_harness_ancestor_is_not_denied(self):
        denied = policy.shared_read_denials(self.base, self.product, self.manager,
                                           home=self.home, codex_home=self.codex_home)
        self.assertNotIn(self.base, denied)
        self.assertFalse(any(path == self.product or path in self.product.parents for path in denied))

    def test_colocated_manager_does_not_deny_product_git(self):
        denied = policy.shared_read_denials(self.engine, self.product, self.product,
                                           home=self.home, codex_home=self.codex_home)
        self.assertNotIn(self.product / ".git", denied)

    def test_public_copy_and_worker_source_remain_readable_but_sibling_is_denied(self):
        sibling = self.product.parent / "private-evidence"
        sibling.mkdir()
        denied = self.denials()
        self.assertIn(sibling, denied)
        for required in (self.product / "orchestrator_harness", self.product / ".harness-git/common.git",
                         self.product / ".harness-runtime/worktrees/epoch/lane",
                         self.product / ".harness-runtime/public-check-snapshots/snapshot-test"):
            self.assertFalse(any(path == required or path in required.parents for path in denied))

    def test_worker_installs_exact_shared_denials_and_narrow_git_writes(self):
        common = self.product / ".harness-git/common.git"
        worker = self.linked_worker(common)
        self.install(worker)
        filesystem = tomllib.loads((worker / ".codex/config.toml").read_text())["permissions"]["worker-isolated"]["filesystem"]
        self.assertEqual({str(path) for path in self.denials()},
                         {name for name, mode in filesystem.items() if mode == "deny"})
        writes = {Path(name) for name, mode in filesystem.items() if mode == "write"}
        self.assertIn(common / "objects", writes)
        self.assertIn(common / "refs/heads/lane/shared-policy", writes)
        self.assertNotIn(common, writes)
        self.assertNotIn(common / "refs/heads", writes)

    def test_sibling_git_store_is_refused_before_worker_config_publication(self):
        common = self.product.parent / "repositories/common.git"
        worker = self.linked_worker(common)
        with self.assertRaisesRegex(bootstrap.BootstrapError, "Git metadata.*inside denied path.*place the common Git directory"):
            self.install(worker)
        self.assertFalse((worker / ".codex/config.toml").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
