"""Focused tests for v2 setup and profile-aware lane materialization."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import tomllib
import unittest
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from orchestrator_harness import bootstrap, setup


class CodexWorkerIsolationLayoutTests(unittest.TestCase):
    def test_isolation_follows_selected_workspace_without_host_names(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            host = Path(raw)
            workspace = host / "workspaces" / "task"
            sibling = host / "workspaces" / "another-task"
            unrelated = host / "unrelated-project"
            for path in (workspace, sibling, unrelated):
                path.mkdir(parents=True)
            worktree = host / "lanes" / "worker"
            harness = host / "harness-install"
            bootstrap._install_codex_worker_isolation(worktree, harness, workspace)
            config = tomllib.loads((worktree / ".codex" / "config.toml").read_text())
            filesystem = config["permissions"]["worker-isolated"]["filesystem"]
            self.assertEqual("deny", filesystem[str(sibling.resolve())])
            self.assertEqual("deny", filesystem[str(harness.resolve())])
            self.assertEqual("deny", filesystem[str(workspace / ".secrets")])
            self.assertNotIn(str(host.resolve()), filesystem)
            self.assertNotIn(str(unrelated.resolve()), filesystem)
            for leaf in (".codex", ".claude", ".qwen", ".agent-workspace"):
                self.assertEqual("deny", filesystem[str(workspace / leaf)])


class GitWorktreeFailureCleanupTests(unittest.TestCase):
    def _commands(self, run, root: Path):
        commands = []
        for entry in run.call_args_list:
            argv = entry.args[0]
            self.assertEqual(["git", "--no-replace-objects", "--no-lazy-fetch", "-C", str(root),
                              "-c", f"core.hooksPath={os.devnull}", "-c", "core.fsmonitor=false"], argv[:9])
            commands.append([argv[0], *argv[3:5], *argv[9:]])
        return commands

    @staticmethod
    def _completed(returncode: int, *, stderr: str = "") -> MagicMock:
        return MagicMock(returncode=returncode, stderr=stderr, stdout="")

    def test_failed_add_removes_branch_created_by_the_attempt(self) -> None:
        root = Path("root-workspace")
        target = Path("runtime/worktrees/epoch/lane")
        branch = "lane/failed-add"
        completed = [
            self._completed(1),
            self._completed(128, stderr="checkout failed"),
            self._completed(128, stderr="not a registered worktree"),
            self._completed(0),
            self._completed(0),
            self._completed(1),
        ]
        with patch.object(bootstrap.subprocess, "run", side_effect=completed) as run:
            with self.assertRaisesRegex(bootstrap.BootstrapError, "checkout failed"):
                bootstrap._git_worktree_add(root, branch, target, "base")

        commands = self._commands(run, root)
        branch_ref = f"refs/heads/{branch}"
        self.assertEqual(
            ["git", "-C", str(root), "show-ref", "--verify", "--quiet", branch_ref],
            commands[0],
        )
        self.assertEqual(
            [
                "git",
                "-C",
                str(root),
                "worktree",
                "add",
                "-b",
                branch,
                str(target),
                "base",
            ],
            commands[1],
        )
        self.assertIn(
            ["git", "-C", str(root), "branch", "-D", "--", branch], commands
        )
        self.assertEqual(
            ["git", "-C", str(root), "show-ref", "--verify", "--quiet", branch_ref],
            commands[-1],
        )

    def test_failed_add_preserves_preexisting_branch(self) -> None:
        root = Path("root-workspace")
        target = Path("runtime/worktrees/epoch/lane")
        branch = "lane/existing"
        completed = [
            self._completed(0),
            self._completed(128, stderr="branch already exists"),
            self._completed(128, stderr="not a registered worktree"),
            self._completed(0),
        ]
        with patch.object(bootstrap.subprocess, "run", side_effect=completed) as run:
            with self.assertRaisesRegex(bootstrap.BootstrapError, "branch already exists"):
                bootstrap._git_worktree_add(root, branch, target, "base")

        commands = self._commands(run, root)
        self.assertNotIn(
            ["git", "-C", str(root), "branch", "-D", "--", branch], commands
        )


class MaterializationFixture:
    def __init__(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.harness = self.root / "harness"
        self.root_workspace = self.root / "root-workspace"
        self.harness.mkdir()
        self.root_workspace.mkdir()
        self.monitor_children = []
        self.bootstrap_runtime = False
        self._write_json(
            self.harness / "harness-config.json",
            {
                "root_workspace": str(self.root_workspace),
                "managed_coordination": "enabled",
            },
        )
        self._write_json(
            self.harness / "resource-manifest.json",
            {"schema": "resource-manifest/v1", "resources": []},
        )
        agent_workspace = self.harness / "super-cache" / "workspace" / ".agent-workspace"
        for name, contents in {
            "README.md": "base workspace\n",
            "hook-dispatch.py": "# hook dispatch\n",
            "lane-queue.py": "# lane queue\n",
            "manager-notify.py": "# manager notify\n",
            "result-stop-check.py": "# result stop check\n",
        }.items():
            self._write_text(agent_workspace / name, contents)
        self._write_text(
            self.harness / "super-cache" / "custom" / "README.md",
            "custom marker\n",
        )
        self._write_provider("codex", ".codex", "codex-root\n", "codex-worker\n")
        self._write_provider(
            "disposable-custom", ".custom", "custom-root\n", "custom-worker\n"
        )

    def close(self) -> None:
        if self.bootstrap_runtime:
            from orchestrator_harness import shutdown
            runtime = self.root_workspace / ".harness-runtime"
            if setup.read_runtime_state(runtime) is not None:
                with patch.object(shutdown, "find_harness_root", return_value=self.harness):
                    result = shutdown.run_shutdown()
                if not result["ok"]:
                    raise AssertionError(f"native fixture shutdown failed; fixture preserved: {result}")
                shutdown.prove_runtime_closed(runtime, self.root_workspace, self.harness)
        self.stop_owned_monitors()
        self.temporary.cleanup()

    def stop_owned_monitors(self) -> None:
        """Quiesce original native writers while retaining the fixture bytes."""
        for child, birth in self.monitor_children:
            if child.poll() is None:
                rt = self.root_workspace / ".harness-runtime"
                from orchestrator_harness.records import RecordLock, atomic_write_json, runtime_admission_lock
                try:
                    with runtime_admission_lock(rt), RecordLock(setup.monitor_record_path(rt)):
                        record = setup.read_monitor_record(rt)
                        if (birth is not None and record is not None
                                and (record.get("pid"), record.get("creation_time")) ==
                                (birth["pid"], birth["creation_time"])):
                            record["stop_requested"] = True
                            atomic_write_json(setup.monitor_record_path(rt), record)
                    child.wait(timeout=3)
                except (OSError, ValueError, subprocess.TimeoutExpired):
                    if not setup.processes.reap_created_process(child):
                        raise AssertionError("exact owned monitor child cleanup is unproven; fixture preserved")
            if child.poll() is None:
                raise AssertionError("owned monitor child remains live; fixture preserved")

    @contextmanager
    def native_setup(self):
        """Observe actual native spawns; never synthesize monitor ownership."""
        real_spawn = setup.processes.spawn_detached
        def spawn(*args, **kwargs):
            child = real_spawn(*args, **kwargs)
            self.monitor_children.append((child, setup.processes.process_identity(child.pid)))
            return child
        with (patch.object(setup, "find_harness_root", return_value=self.harness),
              patch.dict(os.environ, {"PYTHONPATH": str(Path(__file__).resolve().parents[2])}),
              patch.object(setup.processes, "spawn_detached", side_effect=spawn) as observed):
            yield observed

    def _write_text(self, path: Path, contents: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")

    def _write_json(self, path: Path, value: object) -> None:
        self._write_text(path, json.dumps(value, indent=2) + "\n")

    def _write_provider(
        self,
        provider_id: str,
        dotdir: str,
        root_contents: str,
        worker_contents: str,
    ) -> None:
        adapter = self.harness / "adapters" / provider_id
        binding = (
            f"PROVIDER_ID = {provider_id!r}\n"
            "ADAPTER_VERSION = 'test-v1'\n"
            "def validate_launch_config(*, model, launch_config): return dict(launch_config)\n"
            "def build_argv(**kwargs): return [PROVIDER_ID]\n"
            "def parse_line(line): return None\n"
        )
        self._write_text(adapter / "harness" / "launcher_binding.py", binding)
        self._write_text(
            self.harness
            / "orchestrator_harness"
            / "provider_adapters"
            / provider_id
            / "launcher_binding.py",
            binding,
        )
        self._write_text(adapter / "root" / dotdir / "root.txt", root_contents)
        self._write_text(
            adapter / "root" / dotdir / "hooks" / "post-tool-use.py",
            "# root hook fixture\n",
        )
        self._write_json(
            adapter / "root" / dotdir / "orchestrator-harness-binding.json",
            {
                "schema": "harness-hook-binding/v1",
                "role": "root",
                "provider_id": provider_id,
                "harness_root": None,
                "runtime_root": None,
            },
        )
        self._write_text(
            adapter / "super-cache" / dotdir / "worker.txt", worker_contents
        )
        self._write_json(
            adapter / "super-cache" / dotdir / "orchestrator-harness-binding.json",
            {
                "schema": "harness-hook-binding/v1",
                "role": "worker",
                "provider_id": provider_id,
            },
        )
        self._write_text(
            adapter / "super-cache" / dotdir / "skills" / "lane-assignment" / "SKILL.md",
            ".agent-workspace/lane-queue.py\n",
        )
        self._write_text(
            adapter / "super-cache" / dotdir / "skills" / "manager-notify" / "SKILL.md",
            ".agent-workspace/manager-notify.py\n",
        )

    def _write_provider_to(self, harness: Path, provider_id: str, dotdir: str) -> None:
        """Install a disposable provider fixture next to a copied shipped tree."""
        adapter = harness / "adapters" / provider_id
        binding = (
            f"PROVIDER_ID = {provider_id!r}\n"
            "ADAPTER_VERSION = 'test-v1'\n"
            "def validate_launch_config(*, model, launch_config): return dict(launch_config)\n"
            "def build_argv(**kwargs): return [PROVIDER_ID]\n"
            "def parse_line(line): return None\n"
        )
        self._write_text(adapter / "harness" / "launcher_binding.py", binding)
        self._write_text(adapter / "root" / dotdir / "root.txt", "fixture\n")
        self._write_text(
            adapter / "root" / dotdir / "hooks" / "post-tool-use.py",
            "# root hook fixture\n",
        )
        self._write_json(
            adapter / "root" / dotdir / "orchestrator-harness-binding.json",
            {
                "schema": "harness-hook-binding/v1",
                "role": "root",
                "provider_id": provider_id,
                "harness_root": None,
                "runtime_root": None,
            },
        )
        self._write_text(
            adapter / "super-cache" / dotdir / "worker.txt", "fixture\n"
        )
        self._write_json(
            adapter / "super-cache" / dotdir / "orchestrator-harness-binding.json",
            {"schema": "harness-hook-binding/v1", "role": "worker", "provider_id": provider_id},
        )
        for name, helper in (("lane-assignment", "lane-queue.py"), ("manager-notify", "manager-notify.py")):
            self._write_text(
                adapter / "super-cache" / dotdir / "skills" / name / "SKILL.md",
                f".agent-workspace/{helper}\n",
            )
        self._write_text(
            harness
            / "orchestrator_harness"
            / "provider_adapters"
            / provider_id
            / "launcher_binding.py",
            binding,
        )

    def active_cache(self) -> Path:
        destination = self.root_workspace / ".harness-runtime" / "super-cache"
        for source, relative in setup._plan_active_cache(self.harness):
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        return destination

    def git(self, *args: str) -> str:
        result = bootstrap._run_git(self.root_workspace, *args)
        if result.returncode:
            raise AssertionError(f"owned fixture Git failed: {args}: {result.stderr}")
        return result.stdout.strip()

    def initialize_git(self) -> str:
        if not (self.root_workspace / ".git").exists():
            self._write_text(self.root_workspace / ".gitignore",
                             ".harness-runtime/\n.agent-workspace/\n.codex/\n.custom/\n.claude/\n.qwen/\nRESULT.json\n")
            self._write_text(self.root_workspace / "application.txt", "fixture product\n")
            self.git("init", "-q")
            self.git("add", ".gitignore", "application.txt")
            self.git("-c", "user.name=Fixture", "-c", "user.email=fixture@local", "commit", "-qm", "fixture base")
        return self.git("rev-parse", "HEAD")

    def prepare_runtime(self, *, managed: bool, manager: Path | None = None) -> str:
        """Actual setup/READY and Git; no epoch, ownership or source mocks."""
        self._write_json(self.harness / "harness-config.json", {
            "root_workspace": str(self.root_workspace),
            "managed_coordination": "enabled" if managed else "disabled",
            **({"manager_workspace": str(manager)} if manager is not None else {}),
        })
        base = self.initialize_git()
        self.bootstrap_runtime = True
        with self.native_setup():
            result = setup.run_setup()
        if not result["ok"]:
            raise AssertionError(f"native fixture setup failed: {result}")
        from orchestrator_harness.config import compute_config_identity, load_config, load_resource_manifest
        config = load_config(self.harness)
        identity = compute_config_identity(config, load_resource_manifest(self.harness))
        setup.require_monitor_ready(config.runtime_root, identity)
        return base

    def source_snapshot(self) -> dict[str, bytes]:
        return {
            str(path.relative_to(self.harness)): path.read_bytes()
            for path in self.harness.rglob("*")
            if path.is_file()
        }

    def add_shared_root_configs(self) -> None:
        self._write_text(
            self.harness / "adapters" / "codex" / "root" / ".codex" / "config.toml",
            '[features]\nhooks = true\n',
        )
        self._write_json(
            self.harness / "adapters" / "codex" / "root" / ".codex" / "hooks.json",
            {
                "hooks": {
                    "PostToolUse": [
                        {
                            "matcher": ".*",
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": "python .codex/hooks/orchestrator_harness_post_tool_use.py",
                                }
                            ],
                        }
                    ],
                    "Stop": [
                        {
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": "python .codex/hooks/orchestrator_harness_stop.py",
                                }
                            ]
                        }
                    ],
                }
            },
        )
        self._write_provider(
            "claude-code", ".claude", "claude-root\n", "claude-worker\n"
        )
        self._write_json(
            self.harness
            / "adapters"
            / "claude-code"
            / "root"
            / ".claude"
            / "settings.json",
            {
                "permissions": {"defaultMode": "bypassPermissions"},
                "hooks": {
                    "PostToolUse": [
                        {
                            "matcher": ".*",
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": "python .claude/hooks/orchestrator_harness_post_tool_use.py",
                                }
                            ],
                        }
                    ],
                    "Stop": [
                        {
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": "python .claude/hooks/orchestrator_harness_stop.py",
                                }
                            ]
                        }
                    ],
                },
            },
        )


class V2MaterializationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = MaterializationFixture()
        self.addCleanup(self.fixture.close)

    def test_active_cache_preserves_valid_and_rejects_invalid(self) -> None:
        destination = self.fixture.active_cache()
        before = {
            str(path.relative_to(destination)): path.read_bytes()
            for path in destination.rglob("*")
            if path.is_file()
        }
        with patch.object(setup, "_replace_tree") as replace_tree:
            setup._install_active_cache(
                self.fixture.harness,
                self.fixture.root_workspace / ".harness-runtime",
                plan=setup._plan_active_cache(self.fixture.harness),
                overwrite=False,
            )
        replace_tree.assert_not_called()
        self.assertEqual(
            before,
            {
                str(path.relative_to(destination)): path.read_bytes()
                for path in destination.rglob("*")
                if path.is_file()
            },
        )

        corrupt = next(path for path in destination.rglob("*") if path.is_file())
        corrupt.write_text("corrupt\n", encoding="utf-8")
        with self.assertRaises(setup.SetupError) as raised:
            setup._install_active_cache(
                self.fixture.harness,
                self.fixture.root_workspace / ".harness-runtime",
                plan=setup._plan_active_cache(self.fixture.harness),
                overwrite=False,
            )
        self.assertEqual(setup.SETUP_CACHE_INVALID, raised.exception.code)
        self.assertEqual("corrupt\n", corrupt.read_text(encoding="utf-8"))

    def test_root_provider_configs_merge_without_replacing_existing_setup(self) -> None:
        self.fixture.add_shared_root_configs()
        codex_config = self.fixture.root_workspace / ".codex" / "config.toml"
        codex_hooks = self.fixture.root_workspace / ".codex" / "hooks.json"
        claude_settings = self.fixture.root_workspace / ".claude" / "settings.json"
        self.fixture._write_text(
            codex_config,
            '# existing setup\nmodel = "custom"\n\n[features]\nhooks = true\n',
        )
        self.fixture._write_json(
            codex_hooks,
            {
                "description": "existing Codex setup",
                "hooks": {
                    "SessionStart": [{"hooks": [{"type": "command", "command": "session"}]}],
                    "Stop": [{"hooks": [{"type": "command", "command": "verify"}]}],
                },
            },
        )
        self.fixture._write_json(
            claude_settings,
            {
                "$schema": "existing-schema",
                "permissions": {"defaultMode": "ask"},
                "custom": {"keep": True},
                "hooks": {
                    "PreToolUse": [{"hooks": [{"type": "command", "command": "guard"}]}],
                    "Stop": [{"hooks": [{"type": "command", "command": "verify"}]}],
                },
            },
        )
        config_before = codex_config.read_bytes()
        plan = setup._plan_root_payloads(self.fixture.harness)

        setup._preflight_root_payloads(
            plan, self.fixture.root_workspace, overwrite=False
        )
        _installed, overwritten, merged = setup._install_root_payloads(
            self.fixture.harness,
            self.fixture.root_workspace,
            plan=plan,
            overwrite=False,
        )

        self.assertEqual(config_before, codex_config.read_bytes())
        self.assertEqual([], overwritten)
        self.assertEqual({codex_hooks, claude_settings}, set(merged))
        codex = json.loads(codex_hooks.read_text(encoding="utf-8"))
        self.assertEqual("existing Codex setup", codex["description"])
        self.assertIn("SessionStart", codex["hooks"])
        self.assertEqual("verify", codex["hooks"]["Stop"][0]["hooks"][0]["command"])
        self.assertEqual(2, len(codex["hooks"]["Stop"]))
        self.assertIn("PostToolUse", codex["hooks"])
        claude = json.loads(claude_settings.read_text(encoding="utf-8"))
        self.assertEqual({"defaultMode": "ask"}, claude["permissions"])
        self.assertEqual({"keep": True}, claude["custom"])
        self.assertIn("PreToolUse", claude["hooks"])
        self.assertEqual(2, len(claude["hooks"]["Stop"]))
        self.assertIn("PostToolUse", claude["hooks"])

        first_bytes = {path: path.read_bytes() for path in (codex_hooks, claude_settings)}
        setup._preflight_root_payloads(
            plan, self.fixture.root_workspace, overwrite=False
        )
        _installed, overwritten, merged = setup._install_root_payloads(
            self.fixture.harness,
            self.fixture.root_workspace,
            plan=plan,
            overwrite=False,
        )
        self.assertEqual([], overwritten)
        self.assertEqual([], merged)
        self.assertEqual(
            first_bytes,
            {path: path.read_bytes() for path in (codex_hooks, claude_settings)},
        )

    def test_existing_codex_config_must_enable_hooks_before_any_write(self) -> None:
        self.fixture.add_shared_root_configs()
        codex_config = self.fixture.root_workspace / ".codex" / "config.toml"
        self.fixture._write_text(
            codex_config, '[features]\nhooks = false\ncustom = true\n'
        )
        plan = setup._plan_root_payloads(self.fixture.harness)

        with self.assertRaises(setup.SetupError) as raised:
            setup._preflight_root_payloads(
                plan, self.fixture.root_workspace, overwrite=True
            )

        self.assertEqual(setup.SETUP_ADAPTER_COLLISION, raised.exception.code)
        self.assertIn("must enable [features] hooks = true", str(raised.exception))
        self.assertFalse((self.fixture.root_workspace / ".custom").exists())
        self.assertEqual(
            '[features]\nhooks = false\ncustom = true\n',
            codex_config.read_text(encoding="utf-8"),
        )

    def test_changed_harness_owned_hook_entry_is_a_collision(self) -> None:
        self.fixture.add_shared_root_configs()
        codex_hooks = self.fixture.root_workspace / ".codex" / "hooks.json"
        self.fixture._write_json(
            codex_hooks,
            {
                "hooks": {
                    "PreToolUse": [
                        {
                            "matcher": ".*",
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": "python .codex/hooks/orchestrator_harness_post_tool_use.py",
                                    "timeout": 1,
                                }
                            ],
                        }
                    ]
                }
            },
        )
        before = codex_hooks.read_bytes()
        plan = setup._plan_root_payloads(self.fixture.harness)

        with self.assertRaises(setup.SetupError) as raised:
            setup._preflight_root_payloads(
                plan, self.fixture.root_workspace, overwrite=True
            )

        self.assertEqual(setup.SETUP_ADAPTER_COLLISION, raised.exception.code)
        self.assertIn("harness-owned", str(raised.exception))
        self.assertEqual(before, codex_hooks.read_bytes())

    def test_overlay_collision_is_rejected_before_partial_write(self) -> None:
        source = self.fixture.root / "overlay-source"
        destination = self.fixture.root / "overlay-destination"
        self.fixture._write_text(source / "first.txt", "first\n")
        self.fixture._write_text(source / "second.txt", "second\n")
        self.fixture._write_text(destination / "second.txt", "original\n")

        with self.assertRaises(bootstrap.BootstrapError) as raised:
            bootstrap._copy_overlay(source, destination)
        self.assertEqual(bootstrap.BOOTSTRAP_CACHE_COLLISION, raised.exception.code)
        self.assertFalse((destination / "first.txt").exists())
        self.assertEqual(
            "original\n", (destination / "second.txt").read_text(encoding="utf-8")
        )

        first = self.fixture.root / "payload-a.txt"
        second = self.fixture.root / "payload-b.txt"
        first.write_text("a\n", encoding="utf-8")
        second.write_text("b\n", encoding="utf-8")
        with self.assertRaises(setup.SetupError) as duplicate:
            setup._preflight_root_payloads(
                [(first, Path("same.txt")), (second, Path("same.txt"))],
                self.fixture.root_workspace,
                overwrite=False,
            )
        self.assertEqual(setup.SETUP_ADAPTER_COLLISION, duplicate.exception.code)
        self.assertFalse((self.fixture.root_workspace / "same.txt").exists())

    def test_overlay_reuses_identical_existing_file(self) -> None:
        source = self.fixture.root / "identical-overlay-source"
        destination = self.fixture.root / "identical-overlay-destination"
        self.fixture._write_text(source / "existing.txt", "same\n")
        self.fixture._write_text(source / "new.txt", "new\n")
        self.fixture._write_text(destination / "existing.txt", "same\n")
        before = (destination / "existing.txt").stat().st_mtime_ns

        bootstrap._copy_overlay(source, destination)

        self.assertEqual(
            "same\n", (destination / "existing.txt").read_text(encoding="utf-8")
        )
        self.assertEqual(before, (destination / "existing.txt").stat().st_mtime_ns)
        self.assertEqual("new\n", (destination / "new.txt").read_text(encoding="utf-8"))

    def test_overwrite_replaces_only_planned_root_payloads(self) -> None:
        planned = (
            self.fixture.harness
            / "adapters"
            / "codex"
            / "root"
            / ".codex"
            / "root.txt"
        )
        planned_target = self.fixture.root_workspace / ".codex" / "root.txt"
        unrelated = self.fixture.root_workspace / "keep.txt"
        planned_target.parent.mkdir(parents=True, exist_ok=True)
        planned_target.write_text("old\n", encoding="utf-8")
        unrelated.write_text("preserve\n", encoding="utf-8")
        plan = setup._plan_root_payloads(self.fixture.harness)
        setup._preflight_root_payloads(plan, self.fixture.root_workspace, overwrite=True)
        _, overwritten, merged = setup._install_root_payloads(
            self.fixture.harness,
            self.fixture.root_workspace,
            plan=plan,
            overwrite=True,
        )
        self.assertEqual(
            planned.read_text(encoding="utf-8"),
            planned_target.read_text(encoding="utf-8"),
        )
        self.assertEqual("preserve\n", unrelated.read_text(encoding="utf-8"))
        self.assertEqual([planned_target], overwritten)
        self.assertEqual([], merged)

    def test_setup_root_collision_has_no_partial_runtime_write(self) -> None:
        collision = self.fixture.root_workspace / ".codex" / "root.txt"
        collision.parent.mkdir(parents=True, exist_ok=True)
        collision.write_text("existing\n", encoding="utf-8")
        with patch.object(
            setup, "find_harness_root", return_value=self.fixture.harness
        ):
            result = setup.run_setup()
        self.assertFalse(result["ok"])
        self.assertEqual(setup.SETUP_ADAPTER_COLLISION, result["code"])
        self.assertFalse(
            (self.fixture.root_workspace / ".harness-runtime").exists()
        )
        self.assertEqual("existing\n", collision.read_text(encoding="utf-8"))

    def test_composed_worker_destination_collision_precedes_all_setup_writes(self) -> None:
        self.fixture._write_text(
            self.fixture.harness
            / "adapters"
            / "codex"
            / "super-cache"
            / ".agent-workspace"
            / "lane-queue.py",
            "# competing provider owner\n",
        )
        with (
            patch.object(setup, "find_harness_root", return_value=self.fixture.harness),
            patch.object(setup, "_start_monitor") as start_monitor,
        ):
            result = setup.run_setup()
        self.assertFalse(result["ok"])
        self.assertEqual(setup.SETUP_ADAPTER_COLLISION, result["code"])
        self.assertFalse((self.fixture.root_workspace / ".harness-runtime").exists())
        self.assertFalse((self.fixture.root_workspace / ".codex").exists())
        start_monitor.assert_not_called()

    def test_unknown_or_duplicate_worker_hook_command_precedes_setup_writes(self) -> None:
        payload = self.fixture.harness / "adapters" / "codex" / "super-cache" / ".codex"
        self.fixture._write_text(payload / "hooks" / "known.py", "# known\n")
        for commands in (
            ["python .codex/hooks/missing.py"],
            ["python .codex/hooks/known.py", "python .codex/hooks/known.py"],
            ["python .codex/hooks/known.py extra"],
            ["python .codex/hooks/../known.py"],
        ):
            with self.subTest(commands=commands):
                self.fixture._write_json(
                    payload / "hooks.json",
                    {"hooks": {"Stop": [{"hooks": [{"command": command} for command in commands]}]}},
                )
                with (
                    patch.object(setup, "find_harness_root", return_value=self.fixture.harness),
                    patch.object(setup, "_start_monitor") as start_monitor,
                ):
                    result = setup.run_setup()
                self.assertFalse(result["ok"])
                self.assertEqual(setup.SETUP_ADAPTER_COLLISION, result["code"])
                self.assertFalse((self.fixture.root_workspace / ".harness-runtime").exists())
                self.assertFalse((self.fixture.root_workspace / ".codex").exists())
                start_monitor.assert_not_called()

    def test_equivalent_worker_hook_script_spellings_are_rejected_before_setup_writes(self) -> None:
        for variant in (r"python .codex\hooks\known.py", "python .CODEX/Hooks/KNOWN.py"):
            with self.subTest(variant=variant):
                fixture = MaterializationFixture()
                self.addCleanup(fixture.close)
                payload = fixture.harness / "adapters" / "codex" / "super-cache" / ".codex"
                fixture._write_text(payload / "hooks" / "known.py", "# known\n")
                fixture._write_json(
                    payload / "hooks.json",
                    {"hooks": {"Stop": [{"hooks": [
                        {"command": "python .codex/hooks/known.py"},
                        {"command": variant},
                    ]}]}},
                )
                with (
                    patch.object(setup, "find_harness_root", return_value=fixture.harness),
                    patch.object(setup, "_start_monitor") as start_monitor,
                ):
                    result = setup.run_setup()
                self.assertFalse(result["ok"], result)
                self.assertEqual(setup.SETUP_ADAPTER_COLLISION, result["code"])
                self.assertFalse((fixture.root_workspace / ".harness-runtime").exists())
                self.assertFalse((fixture.root_workspace / ".codex").exists())
                start_monitor.assert_not_called()

    def test_distinct_owned_worker_hook_commands_remain_valid(self) -> None:
        payload = self.fixture.harness / "adapters" / "codex" / "super-cache" / ".codex"
        for name in ("first.py", "second.py"):
            self.fixture._write_text(payload / "hooks" / name, "# known\n")
        self.fixture._write_json(
            payload / "hooks.json",
            {"hooks": {"Stop": [{"hooks": [
                {"command": "python .codex/hooks/first.py"},
                {"command": r"python .CODEX\HOOKS\second.py"},
            ]}]}},
        )
        plan = setup._plan_active_cache(self.fixture.harness)
        self.assertIn(
            Path("composed-payloads/codex/.codex/hooks/first.py"),
            [relative for _, relative in plan],
        )
        self.assertIn(
            Path("composed-payloads/codex/.codex/hooks/second.py"),
            [relative for _, relative in plan],
        )

    def test_setup_installs_shared_and_provider_files_in_one_worker_tree(self) -> None:
        with self.fixture.native_setup() as spawn_detached:
            result = setup.run_setup()
        self.assertTrue(result["ok"], result)
        composed = (
            self.fixture.root_workspace
            / ".harness-runtime"
            / "super-cache"
            / "composed-payloads"
        )
        for provider_id, dotdir in (("codex", ".codex"), ("disposable-custom", ".custom")):
            payload = composed / provider_id
            self.assertEqual(
                (self.fixture.harness / "super-cache" / "workspace" / ".agent-workspace" / "lane-queue.py").read_bytes(),
                (payload / ".agent-workspace" / "lane-queue.py").read_bytes(),
            )
            self.assertEqual(
                (self.fixture.harness / "adapters" / provider_id / "super-cache" / dotdir / "worker.txt").read_bytes(),
                (payload / dotdir / "worker.txt").read_bytes(),
            )
            self.assertFalse((payload / "AGENTS.md").exists())
            self.assertTrue((payload / dotdir / "skills" / "lane-assignment" / "SKILL.md").is_file())

    def test_setup_materialization_keeps_shipped_sources_immutable(self) -> None:
        before = self.fixture.source_snapshot()
        with self.fixture.native_setup() as spawn_detached:
            result = setup.run_setup()
        self.assertTrue(result["ok"], result)
        self.assertEqual(before, self.fixture.source_snapshot())
        self.assertTrue(
            (self.fixture.root_workspace / ".codex" / "root.txt").is_file()
        )
        for provider_id, dotdir in (
            ("codex", ".codex"),
            ("disposable-custom", ".custom"),
        ):
            binding = json.loads(
                (
                    self.fixture.root_workspace
                    / dotdir
                    / "orchestrator-harness-binding.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(str(self.fixture.harness.resolve()), binding["harness_root"])
            self.assertEqual(
                str((self.fixture.root_workspace / ".harness-runtime").resolve()),
                binding["runtime_root"],
            )
            self.assertEqual(provider_id, binding["provider_id"])
        self.assertTrue(
            (
                self.fixture.root_workspace
                / ".harness-runtime"
                / "super-cache"
                / "custom"
                / "README.md"
            ).is_file()
        )
        spawn_detached.assert_called_once()


    def _snapshot(self, root: Path) -> dict[str, bytes]:
        return {
            str(path.relative_to(root)): path.read_bytes()
            for path in root.rglob("*")
            if path.is_file()
        }

    def test_setup_second_unchanged_run_is_idempotent(self) -> None:
        with self.fixture.native_setup() as spawn_detached:
            first = setup.run_setup()
            runtime = self.fixture.root_workspace / ".harness-runtime"
            root_after_first = self._snapshot(self.fixture.root_workspace)
            cache_after_first = self._snapshot(runtime / "super-cache")
            manifest_after_first = (
                runtime / "resources" / "RESOURCE_MANIFEST.json"
            ).read_bytes()
            monitor_after_first = (runtime / "monitor" / "MONITOR.json").read_bytes()
            second = setup.run_setup()

        self.assertTrue(first["ok"], first)
        self.assertEqual("SETUP_OK", first["code"])
        self.assertTrue(second["ok"], second)
        self.assertEqual(setup.SETUP_MONITOR_ALREADY_RUNNING, second["code"])
        self.assertNotIn("overwritten_paths", second)
        spawn_detached.assert_called_once()

        self.assertEqual(
            root_after_first, self._snapshot(self.fixture.root_workspace)
        )
        self.assertEqual(cache_after_first, self._snapshot(runtime / "super-cache"))
        self.assertEqual(
            manifest_after_first,
            (runtime / "resources" / "RESOURCE_MANIFEST.json").read_bytes(),
        )
        self.assertEqual(
            monitor_after_first, (runtime / "monitor" / "MONITOR.json").read_bytes()
        )
        for provider_id, dotdir in (
            ("codex", ".codex"),
            ("disposable-custom", ".custom"),
        ):
            binding = json.loads(
                (
                    self.fixture.root_workspace
                    / dotdir
                    / "orchestrator-harness-binding.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(
                str(self.fixture.harness.resolve()), binding["harness_root"]
            )
            self.assertEqual(str(runtime.resolve()), binding["runtime_root"])
            self.assertEqual(provider_id, binding["provider_id"])

    def test_setup_manifest_change_ignores_persistent_lease_lock_file(self) -> None:
        with self.fixture.native_setup() as spawn_detached:
            first = setup.run_setup()
            runtime = self.fixture.root_workspace / ".harness-runtime"
            lease_dir = runtime / "resources" / "leases"
            from orchestrator_harness.records import RecordLock
            from orchestrator_harness import shutdown
            with RecordLock(lease_dir / ".leases.lock"):
                pass
            with patch.object(shutdown, "find_harness_root", return_value=self.fixture.harness):
                closed = shutdown.run_shutdown()
            self.assertTrue(closed["ok"], closed)
            self.fixture._write_json(
                self.fixture.harness / "resource-manifest.json",
                {
                    "schema": "resource-manifest/v1",
                    "resources": [{"id": "ollama-provider-home", "exclusive": True}],
                },
            )
            second = setup.run_setup(overwrite=True)

        self.assertTrue(first["ok"], first)
        self.assertTrue(second["ok"], second)
        self.assertEqual("SETUP_OK", second["code"])
        self.assertEqual(2, spawn_detached.call_count)
        self.assertEqual(
            [{"exclusive": True, "id": "ollama-provider-home"}],
            json.loads(
                (runtime / "resources" / "RESOURCE_MANIFEST.json").read_text(
                    encoding="utf-8"
                )
            )["resources"],
        )

    def test_setup_second_run_rejects_byte_changed_installed_payload(self) -> None:
        with self.fixture.native_setup() as spawn_detached:
            first = setup.run_setup()
        self.assertTrue(first["ok"], first)
        changed = self.fixture.root_workspace / ".codex" / "root.txt"
        changed.write_text("tampered\n", encoding="utf-8")
        with patch.object(
            setup, "find_harness_root", return_value=self.fixture.harness
        ):
            second = setup.run_setup()
        self.assertFalse(second["ok"])
        self.assertEqual(setup.SETUP_ADAPTER_COLLISION, second["code"])
        self.assertEqual("tampered\n", changed.read_text(encoding="utf-8"))

    def test_setup_second_run_unreadable_installed_payload_returns_collision(
        self,
    ) -> None:
        from orchestrator_harness import mutation
        original_read = mutation._read_file_observation
        target = self.fixture.root_workspace / ".codex" / "root.txt"

        def read_guard(root, relative, **kwargs):
            if Path(root) / relative == target.absolute():
                raise PermissionError("simulated unreadable installed payload")
            return original_read(root, relative, **kwargs)

        with self.fixture.native_setup() as spawn_detached:
            first = setup.run_setup()
        self.assertTrue(first["ok"], first)
        before = target.read_bytes()
        with (
            patch.object(
                setup, "find_harness_root", return_value=self.fixture.harness
            ),
            patch.object(mutation, "_read_file_observation", side_effect=read_guard),
        ):
            second = setup.run_setup()
        self.assertFalse(second["ok"])
        self.assertEqual(setup.SETUP_ADAPTER_COLLISION, second["code"])
        self.assertEqual(before, target.read_bytes())
        self.assertNotIn("overwritten_paths", second)
        spawn_detached.assert_called_once()

    def test_setup_second_run_rejects_binding_materialized_elsewhere(self) -> None:
        with self.fixture.native_setup() as spawn_detached:
            first = setup.run_setup()
        self.assertTrue(first["ok"], first)
        binding_path = (
            self.fixture.root_workspace
            / ".codex"
            / "orchestrator-harness-binding.json"
        )
        foreign = json.loads(binding_path.read_text(encoding="utf-8"))
        foreign["harness_root"] = str(self.fixture.root / "elsewhere")
        self.fixture._write_json(binding_path, foreign)
        with patch.object(
            setup, "find_harness_root", return_value=self.fixture.harness
        ):
            second = setup.run_setup()
        self.assertFalse(second["ok"])
        self.assertEqual(setup.SETUP_ADAPTER_COLLISION, second["code"])
        self.assertEqual(
            foreign, json.loads(binding_path.read_text(encoding="utf-8"))
        )
    def test_setup_rejects_hooked_provider_without_binding_before_writes(self) -> None:
        (
            self.fixture.harness
            / "adapters"
            / "codex"
            / "root"
            / ".codex"
            / "orchestrator-harness-binding.json"
        ).unlink()
        with patch.object(setup, "find_harness_root", return_value=self.fixture.harness):
            result = setup.run_setup()
        self.assertFalse(result["ok"])
        self.assertEqual(setup.SETUP_CONFIG_INVALID, result["code"])
        self.assertIn("installed codex hook has no harness binding", result["summary"])
        self.assertFalse((self.fixture.root_workspace / ".harness-runtime").exists())
        self.assertFalse((self.fixture.root_workspace / ".codex").exists())

    def test_setup_rejects_invalid_root_binding_before_writes(self) -> None:
        binding_path = (
            self.fixture.harness
            / "adapters"
            / "codex"
            / "root"
            / ".codex"
            / "orchestrator-harness-binding.json"
        )
        record = json.loads(binding_path.read_text(encoding="utf-8"))
        record["provider_id"] = "wrong-provider"
        self.fixture._write_json(binding_path, record)
        with patch.object(setup, "find_harness_root", return_value=self.fixture.harness):
            result = setup.run_setup()
        self.assertFalse(result["ok"])
        self.assertEqual(setup.SETUP_CONFIG_INVALID, result["code"])
        self.assertIn("invalid provider_id", result["summary"])
        self.assertFalse((self.fixture.root_workspace / ".harness-runtime").exists())
        self.assertFalse((self.fixture.root_workspace / ".codex").exists())

    def test_shipped_and_disposable_custom_provider_bindings_validate(self) -> None:
        repository = Path(__file__).resolve().parents[2]
        harness = self.fixture.root / "binding-harness"
        shutil.copytree(
            repository / "adapters" / "codex", harness / "adapters" / "codex"
        )
        shutil.copytree(
            repository / "orchestrator_harness" / "provider_adapters" / "codex",
            harness / "orchestrator_harness" / "provider_adapters" / "codex",
        )
        self.fixture._write_provider_to(harness, "disposable-custom", ".custom")
        checked = setup._check_launcher_bindings(harness)
        self.assertEqual(
            {"codex", "disposable-custom"},
            {path.parent.name for path in checked},
        )

        catalog_binding = (
            harness / "adapters" / "disposable-custom" / "harness" / "launcher_binding.py"
        )
        catalog_binding.write_text(
            catalog_binding.read_text(encoding="utf-8") + "# drift\n", encoding="utf-8"
        )
        with self.assertRaises(setup.SetupError) as raised:
            setup._check_launcher_bindings(harness)
        self.assertEqual(setup.SETUP_CONFIG_INVALID, raised.exception.code)

    def test_managed_bootstrap_copies_only_selected_provider(self) -> None:
        result, worktree = self._bootstrap("managed-lane", managed=True, provider="codex")
        self.assertTrue(result["ok"], result)
        self.assertEqual(
            "codex-worker\n",
            (worktree / ".codex" / "worker.txt").read_text(encoding="utf-8"),
        )
        self.assertFalse((worktree / ".custom" / "worker.txt").exists())
        self.assertTrue((worktree / ".agent-workspace" / "QUEUE.json").is_file())
        self.assertTrue(
            (worktree / ".agent-workspace" / "manager-notifications").is_dir()
        )
        self.assertEqual(
            "# lane queue\n",
            (worktree / ".agent-workspace" / "lane-queue.py").read_text(encoding="utf-8"),
        )
        receipt = json.loads(
            (worktree / ".agent-workspace" / "overlay-receipt.json").read_text(encoding="utf-8")
        )
        self.assertEqual("composed-payloads/codex", receipt["provider_payload"])
        codex_config = tomllib.loads(
            (worktree / ".codex" / "config.toml").read_text(encoding="utf-8")
        )
        self.assertEqual("worker-isolated", codex_config["default_permissions"])
        profile = codex_config["permissions"]["worker-isolated"]
        self.assertEqual(":workspace", profile["extends"])
        self.assertEqual("write", profile["filesystem"][":workspace_roots"]["."])
        self.assertEqual(
            "deny",
            profile["filesystem"][str((Path.home() / ".codex").resolve())],
        )

    def test_missing_or_changed_installed_composition_blocks_managed_bootstrap(self) -> None:
        self.fixture.prepare_runtime(managed=True)
        for failure in ("missing", "changed"):
            with self.subTest(failure=failure):
                installed = (
                    self.fixture.root_workspace
                    / ".harness-runtime"
                    / "super-cache"
                    / "composed-payloads"
                    / "codex"
                    / ".agent-workspace"
                    / "lane-queue.py"
                )
                original = installed.read_bytes()
                if failure == "missing":
                    installed.unlink()
                else:
                    installed.write_text("# tampered\n", encoding="utf-8")
                result, worktree = self._bootstrap(
                    f"composition-{failure}", managed=True, provider="codex", prime_cache=False
                )
                self.assertFalse(result["ok"])
                self.assertIn(result["code"], (bootstrap.BOOTSTRAP_CACHE_MISSING, bootstrap.BOOTSTRAP_CACHE_COLLISION))
                self.assertFalse(worktree.exists())
                installed.write_bytes(original)

    def test_managed_bootstrap_passes_split_manager_protection_destinations(self) -> None:
        self._assert_split_manager_protection(managed=True)

    def test_plain_bootstrap_passes_split_manager_protection_destinations(self) -> None:
        self._assert_split_manager_protection(managed=False)

    def _assert_split_manager_protection(self, *, managed: bool) -> None:
        manager = self.fixture.root
        result, worktree = self._bootstrap("split-manager", managed=managed, provider="codex", manager=manager)
        self.assertTrue(result["ok"], result)
        filesystem = tomllib.loads((worktree / ".codex/config.toml").read_text())["permissions"]["worker-isolated"]["filesystem"]
        for leaf in (".codex", ".claude", ".qwen", ".agent-workspace"):
            self.assertEqual("deny", filesystem[str(manager / leaf)])
        self.assertNotIn(str(manager), filesystem)
        self.assertEqual("write", filesystem[":workspace_roots"]["."])

    def test_managed_codex_payload_preserves_valid_existing_config(self) -> None:
        runtime = self.fixture.root_workspace / ".harness-runtime"
        payload_config = (
            self.fixture.harness
            / "adapters"
            / "codex"
            / "super-cache"
            / ".codex"
            / "config.toml"
        )
        self.fixture._write_text(
            payload_config,
            '[features]\nhooks = true\nmodel = "managed-default"\n',
        )
        worktree = runtime / "worktrees" / "epoch-1" / "codex-config"
        existing_config = worktree / ".codex" / "config.toml"
        original = b'# product-owned config\n[features]\nhooks = true\n'
        existing_config.parent.mkdir(parents=True, exist_ok=True)
        existing_config.write_bytes(original)
        payload_hooks = payload_config.with_name("hooks.json")
        shipped_hook = {
            "hooks": {
                "Stop": [
                    {
                        "hooks": [
                            {
                                "type": "command",
                                "command": "python .codex/hooks/orchestrator_harness_stop.py",
                            }
                        ]
                    }
                ]
            }
        }
        self.fixture._write_json(payload_hooks, shipped_hook)
        self.fixture._write_text(
            payload_config.parent / "hooks" / "orchestrator_harness_stop.py",
            "# worker stop hook\n",
        )
        self.fixture.active_cache()
        existing_hooks = worktree / ".codex" / "hooks.json"
        combined_hooks = {
            "description": "product hooks",
            "hooks": {
                "SessionStart": [{"hooks": [{"type": "command", "command": "start"}]}],
                **shipped_hook["hooks"],
            },
        }
        self.fixture._write_json(existing_hooks, combined_hooks)
        original_hooks = existing_hooks.read_bytes()
        (worktree / ".agent-workspace").mkdir(parents=True)

        bootstrap._install_managed_material(
            self.fixture.harness,
            runtime,
            worktree,
            provider_id="codex",
            lane_id="codex-config",
            run_id="run-1",
        )

        self.assertEqual(original, existing_config.read_bytes())
        self.assertEqual(original_hooks, existing_hooks.read_bytes())
        self.assertEqual(
            "codex-worker\n",
            (worktree / ".codex" / "worker.txt").read_text(encoding="utf-8"),
        )
        self.assertTrue(
            (worktree / ".agent-workspace" / "harness-hook-binding.json").is_file()
        )

    def test_managed_payload_replaces_only_exact_root_owned_file(self) -> None:
        relative = Path(".codex") / "hooks" / "shared.py"
        root_payload = self.fixture.harness / "adapters" / "codex" / "root" / relative
        worker_payload = (
            self.fixture.harness / "adapters" / "codex" / "super-cache" / relative
        )
        self.fixture._write_text(root_payload, "# root hook\n")
        self.fixture._write_text(worker_payload, "# worker hook\n")
        runtime = self.fixture.root_workspace / ".harness-runtime"
        self.fixture.active_cache()

        worktree = runtime / "worktrees" / "epoch-1" / "owned-replacement"
        self.fixture._write_text(worktree / relative, "# root hook\n")
        (worktree / ".agent-workspace").mkdir(parents=True)
        bootstrap._install_managed_material(
            self.fixture.harness,
            runtime,
            worktree,
            provider_id="codex",
            lane_id="owned-replacement",
            run_id="run-1",
        )
        self.assertEqual(
            "# worker hook\n", (worktree / relative).read_text(encoding="utf-8")
        )

        changed = runtime / "worktrees" / "epoch-1" / "changed-replacement"
        self.fixture._write_text(changed / relative, "# user change\n")
        (changed / ".agent-workspace").mkdir(parents=True)
        with self.assertRaises(bootstrap.BootstrapError) as raised:
            bootstrap._install_managed_material(
                self.fixture.harness,
                runtime,
                changed,
                provider_id="codex",
                lane_id="changed-replacement",
                run_id="run-2",
            )
        self.assertEqual(bootstrap.BOOTSTRAP_CACHE_COLLISION, raised.exception.code)
        self.assertEqual(
            "# user change\n", (changed / relative).read_text(encoding="utf-8")
        )

    def test_bootstrap_overlay_failure_rolls_back_attempt_worktree(self) -> None:
        self.fixture.initialize_git()
        collision = self.fixture.root_workspace / ".agent-workspace/README.md"
        self.fixture._write_text(collision, "tracked product file\n")
        self.fixture.git("add", "-f", ".agent-workspace/README.md")
        self.fixture.git("-c", "user.name=Fixture", "-c", "user.email=fixture@local",
                         "commit", "-qm", "tracked overlay collision")
        base = self.fixture.prepare_runtime(managed=True)
        lane_id = "overlay-failure"
        branch = f"lane/{lane_id}"
        task_card = self.fixture.root / f"{lane_id}.json"
        self.fixture._write_json(task_card, {
            "schema": "project-task-card/v1", "task": "prove failed materialization rollback",
            "acceptance_criteria": ["The failed worktree is rolled back"],
            "deliverables": ["A rollback disposition"],
            "reason_for_acceptance_and_deliverables": "These prove rollback.",
            "branch": branch, "base_commit": base,
        })
        with (
            patch("orchestrator_harness.config.find_harness_root", return_value=self.fixture.harness),
            patch.object(bootstrap, "_git_worktree_add", wraps=bootstrap._git_worktree_add) as git_add,
            patch.object(bootstrap, "_rollback_bootstrap_worktree",
                         wraps=bootstrap._rollback_bootstrap_worktree) as rollback,
        ):
            result = bootstrap.run_bootstrap(
                lane_id=lane_id, provider="codex", model="test-model",
                launch_config={"reasoning_effort": "high", "service_tier": "priority"},
                exclusive_resources=[], task_card_path=str(task_card),
            )
        self.assertFalse(result["ok"], result)
        self.assertEqual(bootstrap.BOOTSTRAP_CACHE_COLLISION, result["code"], result)
        worktree = Path(result["attempt_effects"]["worktree_path"])
        self.assertTrue(worktree.is_relative_to(self.fixture.root_workspace))
        git_add.assert_called_once_with(self.fixture.root_workspace, branch, worktree, base)
        rollback.assert_called_once_with(self.fixture.root_workspace, branch, worktree)
        self.assertTrue(result["attempt_effects"]["rollback_proven"], result)
        self.assertFalse(worktree.exists())
        self.assertEqual(base, self.fixture.git("rev-parse", "HEAD"))
        self.assertEqual("tracked product file\n", collision.read_text(encoding="utf-8"))
        self.assertEqual(1, bootstrap._run_git(self.fixture.root_workspace,
                                             "show-ref", "--verify", "--quiet", "refs/heads/" + branch).returncode)

    def test_bootstrap_rejects_missing_provider_preferences_before_mutation(self) -> None:
        self.fixture._write_json(
            self.fixture.harness / "harness-config.json",
            {
                "root_workspace": str(self.fixture.root_workspace),
                "managed_coordination": "enabled",
            },
        )
        task_card = self.fixture.root / "invalid-launch-task.json"
        self.fixture._write_json(
            task_card,
            {"schema": "project-task-card/v1", "task": "must not create a worktree"},
        )
        with (
            patch(
                "orchestrator_harness.config.find_harness_root",
                return_value=self.fixture.harness,
            ),
            patch.object(bootstrap, "open_epoch") as open_epoch,
            patch.object(bootstrap, "_git_worktree_add") as git_add,
            patch.object(
                bootstrap,
                "_validate_provider_launch_config",
                side_effect=bootstrap.BootstrapError(
                    bootstrap.BOOTSTRAP_REQUEST_INVALID,
                    "codex launch configuration requires reasoning_effort",
                ),
            ),
        ):
            result = bootstrap.run_bootstrap(
                lane_id="missing-preferences",
                provider="codex",
                model="configured-model",
                launch_config={},
                exclusive_resources=[],
                task_card_path=str(task_card),
            )
        self.assertFalse(result["ok"])
        self.assertEqual(bootstrap.BOOTSTRAP_REQUEST_INVALID, result["code"])
        self.assertIn("reasoning_effort", result["summary"])
        open_epoch.assert_not_called()
        git_add.assert_not_called()

    def test_plain_bootstrap_excludes_managed_helpers_and_provider_payload(self) -> None:
        result, worktree = self._bootstrap(
            "plain-lane", managed=False, provider="codex"
        )
        self.assertTrue(result["ok"], result)
        agent_workspace = worktree / ".agent-workspace"
        self.assertFalse((agent_workspace / "lane-queue.py").exists())
        self.assertFalse((agent_workspace / "manager-notify.py").exists())
        self.assertTrue((agent_workspace / "result-stop-check.py").is_file())
        config = tomllib.loads(
            (worktree / ".codex" / "config.toml").read_text(encoding="utf-8")
        )
        self.assertEqual("worker-isolated", config["default_permissions"])
        self.assertFalse((worktree / ".custom").exists())

    def _commit_product(self, files: dict[str, str]) -> None:
        for name, text in files.items():
            self.fixture._write_text(self.fixture.root_workspace / name, text)
        self.fixture.git("init", "-q")
        self.fixture.git("add", *files)
        self.fixture.git("-c", "user.name=Fixture", "-c", "user.email=fixture@local", "commit", "-qm", "fixture base")

    def test_generated_lane_material_is_ignored_without_hiding_project_source(self) -> None:
        from orchestrator_harness.records import read_record
        # The product ignores neither the runtime nor .codex, and ships .codex/dev source.
        self._commit_product({".gitignore": "*.log\n", ".codex/dev/shipped.json": "{}\n"})
        result, worktree = self._bootstrap("ignored-lane", managed=True, provider="codex")
        self.assertTrue(result["ok"], result)
        product_status = self.fixture.git("status", "--porcelain=v1", "--untracked-files=all")
        self.assertNotIn(".harness-runtime", product_status)
        lane = read_record(Path(result["evidence_paths"][1]), "lane/v1")
        def status() -> set[str]:
            return set(bootstrap._run_git(worktree, "status", "--porcelain=v1", "--untracked-files=all").stdout.splitlines())
        self.assertEqual(set(), status())
        def entries(path: Path) -> list[str]:
            return [line for line in path.read_text(encoding="utf-8").splitlines() if not line.startswith("#")]
        generated = {"/" + name.partition("/")[2] for name in lane["source_exclusions"] if name.startswith(".codex/")}
        self.assertIn("/config.toml", generated)
        self.assertIn("/.gitignore", generated)
        self.assertEqual(sorted(generated | {"/.gitignore"}), entries(worktree / ".codex/.gitignore"))
        self.assertEqual(["*"], entries(worktree / ".agent-workspace/.gitignore"))
        (worktree / ".codex/dev/new.json").write_text("{}\n", encoding="utf-8")
        (worktree / ".codex/dev/shipped.json").write_text("{\"changed\": true}\n", encoding="utf-8")
        self.assertEqual({"?? .codex/dev/new.json", " M .codex/dev/shipped.json"}, status())

    def test_product_owned_generated_directory_gitignore_is_preserved(self) -> None:
        self._commit_product({".gitignore": ".harness-runtime/\n", ".codex/.gitignore": "local/\n"})
        from orchestrator_harness.records import read_record
        result, worktree = self._bootstrap("owned-ignore-lane", managed=True, provider="codex")
        self.assertTrue(result["ok"], result)
        self.assertEqual("", bootstrap._run_git(worktree, "status", "--porcelain=v1", "--", ".codex/.gitignore").stdout)
        self.assertEqual("local/\n", (worktree / ".codex/.gitignore").read_text(encoding="utf-8").replace("\r\n", "\n"))
        # Generated files stay visible here; RESULT validation refuses committing them.
        status = bootstrap._run_git(worktree, "status", "--porcelain=v1", "--untracked-files=all").stdout
        self.assertIn("?? .codex/config.toml", status.splitlines())
        lane = read_record(Path(result["evidence_paths"][1]), "lane/v1")
        self.assertIn(".codex/config.toml", lane["source_exclusions"])
        self.assertNotIn(".codex/.gitignore", lane["source_exclusions"])

    def test_managed_bootstrap_preserves_validated_task_card_copy(self) -> None:
        self._assert_bootstrap_preserves_validated_task_card_copy(managed=True)

    def test_plain_bootstrap_preserves_validated_task_card_copy(self) -> None:
        self._assert_bootstrap_preserves_validated_task_card_copy(managed=False)

    def _assert_bootstrap_preserves_validated_task_card_copy(
        self, *, managed: bool
    ) -> None:
        profile = "managed" if managed else "plain"
        lane_id = f"{profile}-task-card-lane"
        result, worktree = self._bootstrap(
            lane_id, managed=managed, provider="codex"
        )
        self.assertTrue(result["ok"], result)
        copied_path = worktree / ".agent-workspace" / "task-card.json"
        self.assertTrue(copied_path.is_file(), copied_path)
        source_path = self.fixture.root / f"{lane_id}.json"
        self.assertEqual(
            json.loads(source_path.read_text(encoding="utf-8")),
            json.loads(copied_path.read_text(encoding="utf-8")),
        )

    def _bootstrap(
        self, lane_id: str, *, managed: bool, provider: str, prime_cache: bool = True,
        manager: Path | None = None,
    ) -> tuple[dict[str, object], Path]:
        from orchestrator_harness import processes
        from orchestrator_harness.records import read_record
        runtime = self.fixture.root_workspace / ".harness-runtime"
        if prime_cache:
            base = self.fixture.prepare_runtime(managed=managed, manager=manager)
        else:
            self.assertEqual("OPEN", setup.read_runtime_state(runtime)["state"])
            base = self.fixture.git("rev-parse", "HEAD")
        task_card = self.fixture.root / f"{lane_id}.json"
        self.fixture._write_json(task_card, {
            "schema": "project-task-card/v1", "task": f"materialize {lane_id}",
            "acceptance_criteria": [f"The {lane_id} lane is materialized"],
            "deliverables": ["A prepared worker worktree"],
            "reason_for_acceptance_and_deliverables": "These prove materialization.",
            "branch": f"lane/{lane_id}", "base_commit": base,
        })
        with (
            patch("orchestrator_harness.config.find_harness_root", return_value=self.fixture.harness),
            patch.object(bootstrap, "_git_worktree_add", wraps=bootstrap._git_worktree_add) as git_add,
            patch.object(processes, "spawn_provider", side_effect=AssertionError("bootstrap started a provider")) as provider_spawn,
            patch.object(processes, "spawn_detached", side_effect=AssertionError("bootstrap started a detached process")) as detached,
        ):
            result = bootstrap.run_bootstrap(
                lane_id=lane_id, provider=provider, model="test-model",
                launch_config=({"reasoning_effort": "high", "service_tier": "priority"}
                               if provider == "codex" else {"effort": "high"}
                               if provider == "claude-code" else {}),
                exclusive_resources=[], task_card_path=str(task_card),
            )
        provider_spawn.assert_not_called()
        detached.assert_not_called()
        if result["ok"]:
            git_add.assert_called_once()
            worktree = Path(result["evidence_paths"][0])
            self.assertTrue((worktree / ".git").is_file())
            lane = read_record(Path(result["evidence_paths"][1]), "lane/v1")
            self.assertEqual(base, lane["git_source"]["base_commit"])
            self.assertEqual(self.fixture.root_workspace / ".git", Path(lane["git_source"]["common_dir"]))
        else:
            if prime_cache:
                self.fail(f"actual prepared bootstrap failed: {result}")
            git_add.assert_not_called()
            self.assertFalse(list((runtime / "worktrees").glob(f"*/{lane_id}")))
            worktree = runtime / "worktrees/not-admitted" / lane_id
        return result, worktree
if __name__ == "__main__":
    unittest.main()
