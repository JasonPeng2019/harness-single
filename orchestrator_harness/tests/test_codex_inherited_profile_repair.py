"""Rebind committed native Codex profiles without importing prior Git grants."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import tomllib
import unittest
from unittest.mock import patch

from orchestrator_harness import bootstrap, isolation_policy


class CodexInheritedProfileRepairTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="codex-inherited-profile-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.product = self.base / "project"
        self.harness = self.base / "harness"
        self.home = self.base / "home"
        for directory in (self.product, self.harness, self.home):
            directory.mkdir()
        self.git(self.product, "init", "-q")
        self.git(self.product, "config", "core.autocrlf", "false")
        self.git(
            self.product,
            "-c",
            "user.name=Profile fixture",
            "-c",
            "user.email=profile@example.invalid",
            "commit",
            "--allow-empty",
            "-qm",
            "base",
        )
        self.parent = self.product / ".harness-runtime/worktrees/epoch/parent"
        self.child = self.product / ".harness-runtime/worktrees/epoch/child"
        self.git(
            self.product, "worktree", "add", "-qb", "lane/parent/work", str(self.parent)
        )
        self.user_prefix = (
            '# User-owned preferences\nmodel = "example"\n[features]\nhooks = true\n'
        )
        config = self.parent / ".codex/config.toml"
        config.parent.mkdir()
        config.write_text(self.user_prefix, encoding="utf-8")

    def git(self, cwd, *arguments):
        result = subprocess.run(
            ["git", "-C", str(cwd), *arguments],
            capture_output=True,
            text=True,
            timeout=30,
            env=dict(os.environ, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull),
        )
        self.assertEqual(0, result.returncode, result.stderr)
        return result.stdout.strip()

    def install(self, worktree, *, enforce_git_isolation=True):
        with (
            patch.object(Path, "home", return_value=self.home),
            patch.dict(os.environ, {"CODEX_HOME": str(self.home / "codex-home")}),
        ):
            bootstrap._install_codex_worker_isolation(
                worktree,
                self.harness,
                self.product,
                enforce_git_isolation=enforce_git_isolation,
            )

    def committed_child(self):
        self.install(self.parent)
        self.git(self.parent, "add", ".codex/config.toml")
        self.git(
            self.parent,
            "-c",
            "user.name=Profile fixture",
            "-c",
            "user.email=profile@example.invalid",
            "commit",
            "-qm",
            "generated parent profile",
        )
        parent_tip = self.git(self.parent, "rev-parse", "HEAD")
        self.git(
            self.product,
            "worktree",
            "add",
            "-qb",
            "lane/child/work",
            str(self.child),
            parent_tip,
        )
        return self.child / ".codex/config.toml"

    def profile(self, config):
        return tomllib.loads(config.read_text(encoding="utf-8"))["permissions"][
            "worker-isolated"
        ]

    def test_committed_child_replaces_parent_grants_and_preserves_user_prefix(self):
        config = self.committed_child()
        inherited = config.read_text(encoding="utf-8")
        self.install(self.child)
        rendered = config.read_text(encoding="utf-8")
        filesystem = self.profile(config)["filesystem"]
        common = Path(
            self.git(
                self.product, "rev-parse", "--path-format=absolute", "--git-common-dir"
            )
        )
        parent_git = Path(self.git(self.parent, "rev-parse", "--absolute-git-dir"))
        child_git = Path(self.git(self.child, "rev-parse", "--absolute-git-dir"))
        expected_writes = {
            child_git,
            common / "objects",
            common / "refs/heads/lane/child",
            common / "logs/refs/heads/lane/child",
        }
        self.assertEqual(
            expected_writes, {Path(k) for k, v in filesystem.items() if v == "write"}
        )
        for stale in (
            parent_git,
            common / "refs/heads/lane/parent",
            common / "logs/refs/heads/lane/parent",
        ):
            self.assertNotIn(str(stale), filesystem)
        self.assertNotIn(str(common), filesystem)
        self.assertNotIn(str(common / "refs/heads"), filesystem)
        marker = "# Harness-owned worker isolation; regenerated for every lane.\n"
        self.assertEqual(inherited.split(marker)[0], rendered.split(marker)[0])
        self.assertIn(self.user_prefix, rendered)
        with (
            patch.object(Path, "home", return_value=self.home),
            patch.dict(os.environ, {"CODEX_HOME": str(self.home / "codex-home")}),
        ):
            denials = isolation_policy.shared_read_denials(
                self.harness, self.product, self.product
            )
        self.assertEqual(
            {str(p) for p in denials}, {k for k, v in filesystem.items() if v == "deny"}
        )

    def test_repeat_install_is_byte_idempotent(self):
        self.install(self.parent)
        config = self.parent / ".codex/config.toml"
        before = config.read_bytes()
        self.install(self.parent)
        self.assertEqual(before, config.read_bytes())

    def test_explicit_yolo_rebinds_inheritance_without_granting_git_writes(self):
        config = self.committed_child()
        self.install(self.child, enforce_git_isolation=False)
        self.assertFalse(
            any(
                value == "write"
                for value in self.profile(config)["filesystem"].values()
            )
        )
        self.assertIn(self.user_prefix, config.read_text(encoding="utf-8"))
        before = config.read_bytes()
        self.install(self.child, enforce_git_isolation=False)
        self.assertEqual(before, config.read_bytes())

    def test_custom_and_malformed_owned_suffixes_refuse_without_mutation(self):
        self.install(self.parent)
        config = self.parent / ".codex/config.toml"
        generated = config.read_text(encoding="utf-8")
        marker = "# Harness-owned worker isolation; regenerated for every lane.\n"
        variants = [
            generated + "[user_settings]\nkeep = true\n",
            generated + "[permissions.worker-isolated.network]\nenabled = true\n",
            generated.replace('extends = ":workspace"', 'extends = ":read-only"'),
            generated.replace(
                'default_permissions = "worker-isolated"',
                'default_permissions = "custom"',
            ),
            generated.replace('"." = "write"', '"." = "read"'),
            generated.replace(' = "deny"', ' = "read"', 1),
            'default_permissions = "worker-isolated"\n'
            + marker
            + 'permissions = "custom"\n',
            'default_permissions = "worker-isolated"\n'
            + marker
            + '[permissions]\nworker-isolated = "custom"\n',
            'default_permissions = "worker-isolated"\n'
            + marker
            + '[permissions.worker-isolated]\nfilesystem = "custom"\n',
            generated.replace(marker, marker + marker),
        ]
        for index, text in enumerate(variants):
            with self.subTest(variant=index):
                config.write_text(text, encoding="utf-8")
                before = config.read_bytes()
                with self.assertRaises(bootstrap.BootstrapError) as raised:
                    self.install(self.parent)
                self.assertEqual(
                    bootstrap.BOOTSTRAP_CACHE_COLLISION, raised.exception.code
                )
                self.assertEqual(before, config.read_bytes())

    def test_unowned_permission_configuration_refuses_without_mutation(self):
        config = self.parent / ".codex/config.toml"
        for text in (
            'default_permissions = "worker-isolated"\n',
            'sandbox_mode = "read-only"\n',
            '[permissions.worker-isolated]\nextends = ":workspace"\n',
        ):
            with self.subTest(text=text):
                config.write_text(text, encoding="utf-8")
                before = config.read_bytes()
                with self.assertRaises(bootstrap.BootstrapError):
                    self.install(self.parent)
                self.assertEqual(before, config.read_bytes())


if __name__ == "__main__":
    unittest.main(verbosity=2)
