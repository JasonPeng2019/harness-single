"""Real Git regressions for scoped linked worktree materialization."""
from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import bootstrap


class CheckoutPathsGitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="checkout-scope-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        self.target = self.root / "worker"
        self.repo.mkdir()
        self.git(self.repo, "init", "-q", "-b", "main")
        self.git(self.repo, "config", "core.autocrlf", "false")
        self.git(self.repo, "config", "core.hooksPath", str(self.root / "no-hooks"))
        self.files = {
            "src/main.py": "selected source\n",
            "docs/has spaces/Δelta.md": "selected unicode path\n",
            "AGENTS.md": "top level instructions\n",
            "unrelated/large.log": "excluded contents\n",
        }
        for relative, contents in self.files.items():
            path = self.repo / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(contents, encoding="utf-8")
        self.git(self.repo, "add", ".")
        self.git(self.repo, "-c", "user.name=Scope Test", "-c", "user.email=scope@example.invalid",
                 "commit", "-qm", "fixture baseline")
        self.base = self.git(self.repo, "rev-parse", "HEAD").stdout.strip()

    @staticmethod
    def git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", "-C", str(repo), "-c", "user.name=Scope Test",
             "-c", "user.email=scope@example.invalid", *args],
            capture_output=True, text=True, encoding="utf-8", check=True,
        )

    def test_scoped_materialization_never_checks_out_excluded_contents(self) -> None:
        original_run = bootstrap._run_git
        empty_before_sparse = False

        def observe(root: Path, *args: str):
            nonlocal empty_before_sparse
            result = original_run(root, *args)
            if args[:2] == ("worktree", "add") and result.returncode == 0:
                self.assertEqual([".git"], sorted(item.name for item in self.target.iterdir()))
                empty_before_sparse = True
            return result

        with patch.object(bootstrap, "_run_git", side_effect=observe):
            bootstrap._git_worktree_add(
                self.repo, "lane/scoped", self.target, self.base,
                checkout_paths=("src", "docs/has spaces/Δelta"),
            )
        self.assertTrue(empty_before_sparse)
        self.assertEqual(self.files["src/main.py"], (self.target / "src/main.py").read_text())
        self.assertEqual(self.files["docs/has spaces/Δelta.md"],
                         (self.target / "docs/has spaces/Δelta.md").read_text())
        self.assertEqual(self.files["AGENTS.md"], (self.target / "AGENTS.md").read_text())
        self.assertFalse((self.target / "unrelated").exists())
        self.assertEqual(self.base, self.git(self.target, "rev-parse", "HEAD").stdout.strip())
        self.assertEqual("lane/scoped", self.git(self.target, "branch", "--show-current").stdout.strip())
        self.assertEqual("true", self.git(self.target, "config", "--worktree", "--get",
                                           "core.sparseCheckout").stdout.strip())
        self.assertEqual("true", self.git(self.target, "config", "--worktree", "--get",
                                           "core.sparseCheckoutCone").stdout.strip())
        self.assertEqual("", self.git(self.repo, "status", "--porcelain").stdout)
        self.assertEqual("", self.git(self.target, "status", "--porcelain").stdout)
        (self.target / "src/main.py").write_text("edited in sparse worktree\n", encoding="utf-8")
        self.git(self.target, "add", "src/main.py")
        self.git(self.target, "commit", "-qm", "sparse worker change")
        self.assertEqual("excluded contents\n",
                         self.git(self.target, "show", f"{self.base}:unrelated/large.log").stdout)
        self.assertEqual("", self.git(self.repo, "status", "--porcelain").stdout)

    def test_missing_directory_refuses_before_branch_or_worktree_creation(self) -> None:
        with self.assertRaisesRegex(bootstrap.BootstrapError, "absent from the base commit"):
            bootstrap._git_worktree_add(
                self.repo, "lane/missing", self.target, self.base,
                checkout_paths=("missing",),
            )
        self.assertFalse(self.target.exists())
        ref = subprocess.run(
            ["git", "-C", str(self.repo), "show-ref", "--verify", "--quiet",
             "refs/heads/lane/missing"], capture_output=True, check=False,
        )
        self.assertEqual(1, ref.returncode)

    def test_sparse_failure_rolls_back_only_attempt_created_branch_and_worktree(self) -> None:
        original_run = bootstrap._run_git

        def inject(root: Path, *args: str):
            if args[:2] == ("sparse-checkout", "set"):
                return subprocess.CompletedProcess(args, 1, "", "injected sparse failure")
            return original_run(root, *args)

        with patch.object(bootstrap, "_run_git", side_effect=inject):
            with self.assertRaisesRegex(bootstrap.BootstrapError, "injected sparse failure"):
                bootstrap._git_worktree_add(
                    self.repo, "lane/fails", self.target, self.base,
                    checkout_paths=("src",),
                )
        self.assertFalse(self.target.exists())
        self.assertEqual("", self.git(self.repo, "status", "--porcelain").stdout)
        self.assertNotIn("refs/heads/lane/fails", self.git(self.repo, "show-ref").stdout)
        self.assertEqual(self.base, self.git(self.repo, "rev-parse", "main").stdout.strip())

    def test_omitted_scope_keeps_full_checkout(self) -> None:
        bootstrap._git_worktree_add(self.repo, "lane/full", self.target, self.base)
        self.assertEqual(self.files["unrelated/large.log"],
                         (self.target / "unrelated/large.log").read_text())
        self.assertEqual("", self.git(self.repo, "status", "--porcelain").stdout)


if __name__ == "__main__":
    unittest.main()
