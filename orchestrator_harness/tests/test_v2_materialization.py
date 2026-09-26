"""Focused tests for v2 setup and profile-aware lane materialization."""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from orchestrator_harness import bootstrap, lanes, setup


class MaterializationFixture:
    def __init__(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.harness = self.root / "harness"
        self.root_workspace = self.root / "root-workspace"
        self.harness.mkdir()
        self.root_workspace.mkdir()
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
        self.temporary.cleanup()

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

    def _write_provider_to(self, harness: Path, provider_id: str, dotdir: str) -> None:
        """Install a disposable provider fixture next to a copied shipped tree."""
        adapter = harness / "adapters" / provider_id
        binding = (
            f"PROVIDER_ID = {provider_id!r}\n"
            "ADAPTER_VERSION = 'test-v1'\n"
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

    def test_setup_materialization_keeps_shipped_sources_immutable(self) -> None:
        before = self.fixture.source_snapshot()
        child = MagicMock(pid=1701)
        with (
            patch.object(
                setup, "find_harness_root", return_value=self.fixture.harness
            ),
            patch.object(
                setup.processes,
                "python_argv",
                return_value=["python", "-m", "monitor"],
            ),
            patch.object(
                setup.processes, "spawn_detached", return_value=child
            ) as spawn_detached,
            patch.object(
                setup.processes,
                "process_identity",
                return_value={"pid": 1701, "creation_time": "test-creation"},
            ),
        ):
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
        child = MagicMock(pid=1701)
        with (
            patch.object(
                setup, "find_harness_root", return_value=self.fixture.harness
            ),
            patch.object(
                setup.processes,
                "python_argv",
                return_value=["python", "-m", "monitor"],
            ),
            patch.object(
                setup.processes, "spawn_detached", return_value=child
            ) as spawn_detached,
            patch.object(
                setup.processes,
                "process_identity",
                return_value={"pid": 1701, "creation_time": "test-creation"},
            ),
            patch.object(
                setup.processes, "identity_matches", return_value=True
            ),
        ):
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

    def test_setup_second_run_rejects_byte_changed_installed_payload(self) -> None:
        child = MagicMock(pid=1701)
        with (
            patch.object(
                setup, "find_harness_root", return_value=self.fixture.harness
            ),
            patch.object(
                setup.processes,
                "python_argv",
                return_value=["python", "-m", "monitor"],
            ),
            patch.object(
                setup.processes, "spawn_detached", return_value=child
            ),
            patch.object(
                setup.processes,
                "process_identity",
                return_value={"pid": 1701, "creation_time": "test-creation"},
            ),
        ):
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
        child = MagicMock(pid=1701)
        original_read_bytes = Path.read_bytes
        target = self.fixture.root_workspace / ".codex" / "root.txt"

        def read_bytes_guard(path: Path) -> bytes:
            if str(path) == str(target):
                raise PermissionError("simulated unreadable installed payload")
            return original_read_bytes(path)

        with (
            patch.object(
                setup, "find_harness_root", return_value=self.fixture.harness
            ),
            patch.object(
                setup.processes,
                "python_argv",
                return_value=["python", "-m", "monitor"],
            ),
            patch.object(
                setup.processes, "spawn_detached", return_value=child
            ) as spawn_detached,
            patch.object(
                setup.processes,
                "process_identity",
                return_value={"pid": 1701, "creation_time": "test-creation"},
            ),
        ):
            first = setup.run_setup()
        self.assertTrue(first["ok"], first)
        before = target.read_bytes()
        with (
            patch.object(
                setup, "find_harness_root", return_value=self.fixture.harness
            ),
            patch.object(Path, "read_bytes", read_bytes_guard),
        ):
            second = setup.run_setup()
        self.assertFalse(second["ok"])
        self.assertEqual(setup.SETUP_ADAPTER_COLLISION, second["code"])
        self.assertEqual(before, target.read_bytes())
        self.assertNotIn("overwritten_paths", second)
        spawn_detached.assert_called_once()

    def test_setup_second_run_rejects_binding_materialized_elsewhere(self) -> None:
        child = MagicMock(pid=1701)
        with (
            patch.object(
                setup, "find_harness_root", return_value=self.fixture.harness
            ),
            patch.object(
                setup.processes,
                "python_argv",
                return_value=["python", "-m", "monitor"],
            ),
            patch.object(
                setup.processes, "spawn_detached", return_value=child
            ),
            patch.object(
                setup.processes,
                "process_identity",
                return_value={"pid": 1701, "creation_time": "test-creation"},
            ),
        ):
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

    def test_plain_bootstrap_excludes_managed_helpers_and_provider_payload(self) -> None:
        result, worktree = self._bootstrap(
            "plain-lane", managed=False, provider="codex"
        )
        self.assertTrue(result["ok"], result)
        agent_workspace = worktree / ".agent-workspace"
        self.assertFalse((agent_workspace / "lane-queue.py").exists())
        self.assertFalse((agent_workspace / "manager-notify.py").exists())
        self.assertTrue((agent_workspace / "result-stop-check.py").is_file())
        self.assertFalse((worktree / ".codex").exists())
        self.assertFalse((worktree / ".custom").exists())

    def test_managed_bootstrap_preserves_validated_task_card_copy(self) -> None:
        self._assert_bootstrap_preserves_validated_task_card_copy(managed=True)

    def test_plain_bootstrap_preserves_validated_task_card_copy(self) -> None:
        self._assert_bootstrap_preserves_validated_task_card_copy(managed=False)

    def test_bootstrap_rejects_missing_provider_payload_before_worktree_creation(
        self,
    ) -> None:
        self.fixture._write_json(
            self.fixture.harness / "harness-config.json",
            {
                "root_workspace": str(self.fixture.root_workspace),
                "managed_coordination": "enabled",
            },
        )
        self.fixture.active_cache()
        task_card = self.fixture.root / "missing-provider.json"
        self.fixture._write_json(
            task_card,
            {
                "schema": "project-task-card/v1",
                "task": "must fail before creating a worktree",
                "branch": "lane/missing-provider",
                "base_commit": "HEAD",
            },
        )
        identity = {
            "source_root": str(self.fixture.root_workspace),
            "common_dir": str(self.fixture.root_workspace / ".git"),
            "branch": "lane/missing-provider",
            "base_commit": "a" * 40,
            "origin_tip": "a" * 40,
            "bootstrap_tip": "a" * 40,
        }
        with (
            patch(
                "orchestrator_harness.config.find_harness_root",
                return_value=self.fixture.harness,
            ),
            patch.object(bootstrap, "_resolve_git_identity", return_value=identity),
            patch.object(bootstrap, "open_epoch") as open_epoch,
            patch.object(bootstrap, "_git_worktree_add") as git_add,
        ):
            result = bootstrap.run_bootstrap(
                lane_id="missing-provider",
                provider="does-not-exist",
                model="test-model",
                exclusive_resources=[],
                task_card_path=str(task_card),
            )
        self.assertFalse(result["ok"], result)
        self.assertEqual(bootstrap.BOOTSTRAP_ADAPTER_MISSING, result["code"])
        open_epoch.assert_not_called()
        git_add.assert_not_called()

    def test_nonzero_worktree_add_preserves_external_winner_for_inspection(
        self,
    ) -> None:
        repository = self.fixture.root_workspace
        subprocess.run(
            ["git", "init", "--initial-branch=main"],
            cwd=repository,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Harness Test"],
            cwd=repository,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.email", "harness@example.invalid"],
            cwd=repository,
            check=True,
        )
        (repository / "base.txt").write_text("base\n", encoding="utf-8")
        subprocess.run(["git", "add", "base.txt"], cwd=repository, check=True)
        subprocess.run(
            ["git", "commit", "-m", "base"],
            cwd=repository,
            check=True,
            capture_output=True,
        )
        self.fixture.active_cache()
        task_card = self.fixture.root / "partial-add.json"
        branch = "lane/partial-add"
        self.fixture._write_json(
            task_card,
            {
                "schema": "project-task-card/v1",
                "task": "simulate a nonzero worktree-add after partial creation",
                "branch": branch,
                "base_commit": "HEAD",
            },
        )
        target = (
            repository
            / ".harness-runtime"
            / "worktrees"
            / "epoch-1"
            / "partial-add"
        )
        target.parent.mkdir(parents=True, exist_ok=True)

        def partially_add(
            root: Path, created_branch: str, path: Path, base_commit: str
        ) -> None:
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(root),
                    "worktree",
                    "add",
                    "-b",
                    created_branch,
                    str(path),
                    base_commit,
                ],
                check=True,
                capture_output=True,
            )
            raise bootstrap.BootstrapError(
                bootstrap.BOOTSTRAP_CLEANUP_FAILED,
                bootstrap.AMBIGUOUS_ADD_DIAGNOSTIC,
            )

        with (
            patch(
                "orchestrator_harness.config.find_harness_root",
                return_value=self.fixture.harness,
            ),
            patch.object(bootstrap, "open_epoch", return_value={"epoch_id": "epoch-1"}),
            patch.object(bootstrap, "read_active_lanes", return_value=[]),
            patch.object(bootstrap, "_git_worktree_add", side_effect=partially_add),
        ):
            result = bootstrap.run_bootstrap(
                lane_id="partial-add",
                provider="codex",
                model="test-model",
                exclusive_resources=[],
                task_card_path=str(task_card),
            )

        self.assertFalse(result["ok"], result)
        self.assertEqual(bootstrap.BOOTSTRAP_CLEANUP_FAILED, result["code"])
        self.assertTrue(target.exists())
        branch_check = subprocess.run(
            ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
            cwd=repository,
        )
        self.assertEqual(0, branch_check.returncode)
        worktrees = subprocess.run(
            ["git", "worktree", "list", "--porcelain"],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        self.assertIn(str(target), worktrees)

    def test_failed_add_with_no_artifacts_has_idempotent_exact_rollback(self) -> None:
        repository = self.fixture.root_workspace
        subprocess.run(
            ["git", "init", "--initial-branch=main"],
            cwd=repository,
            check=True,
            capture_output=True,
        )
        (repository / "base.txt").write_text("base\n", encoding="utf-8")
        subprocess.run(["git", "add", "base.txt"], cwd=repository, check=True)
        subprocess.run(
            [
                "git",
                "-c",
                "user.name=Harness Test",
                "-c",
                "user.email=harness@example.invalid",
                "commit",
                "-m",
                "base",
            ],
            cwd=repository,
            check=True,
            capture_output=True,
        )
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.fixture.active_cache()
        task_card = self.fixture.root / "never-created.json"
        branch = "lane/never-created"
        self.fixture._write_json(
            task_card,
            {
                "schema": "project-task-card/v1",
                "task": "fail without creating Git artifacts",
                "branch": branch,
                "base_commit": commit,
            },
        )
        target = (
            repository
            / ".harness-runtime"
            / "worktrees"
            / "epoch-1"
            / "never-created"
        )
        with (
            patch(
                "orchestrator_harness.config.find_harness_root",
                return_value=self.fixture.harness,
            ),
            patch.object(bootstrap, "open_epoch", return_value={"epoch_id": "epoch-1"}),
            patch.object(bootstrap, "read_active_lanes", return_value=[]),
            patch.object(
                bootstrap,
                "_git_worktree_add",
                side_effect=bootstrap.BootstrapError(
                    bootstrap.BOOTSTRAP_REQUEST_INVALID,
                    "synthetic failure before Git created anything",
                ),
            ),
        ):
            result = bootstrap.run_bootstrap(
                lane_id="never-created",
                provider="codex",
                model="test-model",
                exclusive_resources=[],
                task_card_path=str(task_card),
            )
        self.assertFalse(result["ok"], result)
        self.assertEqual(bootstrap.BOOTSTRAP_REQUEST_INVALID, result["code"])
        self.assertFalse(target.exists())
        self.assertEqual(
            1,
            subprocess.run(
                ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
                cwd=repository,
            ).returncode,
        )

    def test_preexisting_exact_worktree_path_is_preserved_before_add_or_rollback(
        self,
    ) -> None:
        self.fixture.active_cache()
        task_card = self.fixture.root / "preexisting-path.json"
        self.fixture._write_json(
            task_card,
            {
                "schema": "project-task-card/v1",
                "task": "preserve a pre-existing exact target",
                "branch": "lane/preexisting-path",
                "base_commit": "HEAD",
            },
        )
        target = (
            self.fixture.root_workspace
            / ".harness-runtime"
            / "worktrees"
            / "epoch-1"
            / "preexisting-path"
        )
        target.mkdir(parents=True)
        sentinel = target / "sentinel.txt"
        sentinel.write_text("must survive\n", encoding="utf-8")
        identity = {
            "source_root": str(self.fixture.root_workspace.resolve()),
            "common_dir": str((self.fixture.root_workspace / ".git").resolve()),
            "branch": "lane/preexisting-path",
            "base_commit": "a" * 40,
            "origin_tip": "a" * 40,
            "bootstrap_tip": "a" * 40,
        }
        with (
            patch(
                "orchestrator_harness.config.find_harness_root",
                return_value=self.fixture.harness,
            ),
            patch.object(bootstrap, "_resolve_git_identity", return_value=identity),
            patch.object(bootstrap, "open_epoch", return_value={"epoch_id": "epoch-1"}),
            patch.object(bootstrap, "read_active_lanes", return_value=[]),
            patch.object(bootstrap, "_git_worktree_add") as git_add,
            patch.object(bootstrap, "_rollback_created_worktree") as rollback,
        ):
            result = bootstrap.run_bootstrap(
                lane_id="preexisting-path",
                provider="codex",
                model="test-model",
                exclusive_resources=[],
                task_card_path=str(task_card),
            )
        self.assertEqual(bootstrap.BOOTSTRAP_WORKTREE_EXISTS, result["code"])
        git_add.assert_not_called()
        rollback.assert_not_called()
        self.assertEqual("must survive\n", sentinel.read_text(encoding="utf-8"))

    def test_external_path_race_after_preflight_is_preserved_fail_closed(self) -> None:
        repository = self.fixture.root_workspace
        subprocess.run(
            ["git", "init", "--initial-branch=main"],
            cwd=repository,
            check=True,
            capture_output=True,
        )
        (repository / "base.txt").write_text("base\n", encoding="utf-8")
        subprocess.run(["git", "add", "base.txt"], cwd=repository, check=True)
        subprocess.run(
            [
                "git",
                "-c",
                "user.name=Harness Test",
                "-c",
                "user.email=harness@example.invalid",
                "commit",
                "-m",
                "base",
            ],
            cwd=repository,
            check=True,
            capture_output=True,
        )
        self.fixture.active_cache()
        task_card = self.fixture.root / "external-race.json"
        self.fixture._write_json(
            task_card,
            {
                "schema": "project-task-card/v1",
                "task": "preserve an externally raced target",
                "branch": "lane/external-race",
                "base_commit": "HEAD",
            },
        )
        target = (
            repository
            / ".harness-runtime"
            / "worktrees"
            / "epoch-1"
            / "external-race"
        )
        sentinel = target / "external-sentinel.txt"

        def external_race(_root, _branch, path, _base_commit):
            path.mkdir(parents=True)
            sentinel.write_text("external owner\n", encoding="utf-8")
            raise bootstrap.BootstrapError(
                bootstrap.BOOTSTRAP_CLEANUP_FAILED,
                bootstrap.AMBIGUOUS_ADD_DIAGNOSTIC,
            )

        with (
            patch(
                "orchestrator_harness.config.find_harness_root",
                return_value=self.fixture.harness,
            ),
            patch.object(bootstrap, "open_epoch", return_value={"epoch_id": "epoch-1"}),
            patch.object(bootstrap, "read_active_lanes", return_value=[]),
            patch.object(bootstrap, "_git_worktree_add", side_effect=external_race),
        ):
            result = bootstrap.run_bootstrap(
                lane_id="external-race",
                provider="codex",
                model="test-model",
                exclusive_resources=[],
                task_card_path=str(task_card),
            )
        self.assertEqual(bootstrap.BOOTSTRAP_CLEANUP_FAILED, result["code"])
        self.assertIn("ownership is ambiguous", result["summary"])
        self.assertEqual("external owner\n", sentinel.read_text(encoding="utf-8"))
        self.assertEqual(
            1,
            subprocess.run(
                [
                    "git",
                    "show-ref",
                    "--verify",
                    "--quiet",
                    "refs/heads/lane/external-race",
                ],
                cwd=repository,
            ).returncode,
        )

    def test_failed_active_index_publication_leaves_no_operational_phantom(self) -> None:
        repository = self.fixture.root_workspace
        subprocess.run(
            ["git", "init", "--initial-branch=main"],
            cwd=repository,
            check=True,
            capture_output=True,
        )
        subprocess.run(["git", "config", "user.name", "Harness Test"], cwd=repository, check=True)
        subprocess.run(["git", "config", "user.email", "harness@example.invalid"], cwd=repository, check=True)
        (repository / "base.txt").write_text("base\n", encoding="utf-8")
        subprocess.run(["git", "add", "base.txt"], cwd=repository, check=True)
        subprocess.run(["git", "commit", "-m", "base"], cwd=repository, check=True, capture_output=True)
        self.fixture.active_cache()
        task_card = self.fixture.root / "publication-failure.json"
        self.fixture._write_json(
            task_card,
            {
                "schema": "project-task-card/v1",
                "task": "publication must be transactional",
                "branch": "lane/publication-failure",
                "base_commit": "HEAD",
            },
        )
        runtime = repository / ".harness-runtime"
        self.fixture._write_json(
            runtime / "CURRENT_EPOCH.json",
            {"schema": "current-epoch/v1", "epoch_id": "epoch-1", "queue_id": "q"},
        )
        real_write = bootstrap.write_active_lanes
        writes = 0

        def fail_first_write(rt, epoch_id, entries):
            nonlocal writes
            writes += 1
            if writes == 1:
                raise OSError("injected active-index publication failure")
            return real_write(rt, epoch_id, entries)

        with (
            patch("orchestrator_harness.config.find_harness_root", return_value=self.fixture.harness),
            patch.object(bootstrap, "open_epoch", return_value={"epoch_id": "epoch-1"}),
            patch.object(bootstrap, "write_active_lanes", side_effect=fail_first_write),
        ):
            result = bootstrap.run_bootstrap(
                lane_id="publication-failure",
                provider="codex",
                model="test-model",
                exclusive_resources=[],
                task_card_path=str(task_card),
            )
        self.assertFalse(result["ok"], result)
        lane_path = runtime / "epochs" / "epoch-1" / "lanes" / "publication-failure" / "lane.json"
        self.assertFalse(lane_path.exists())
        self.assertFalse((runtime / "worktrees" / "epoch-1" / "publication-failure").exists())
        with self.assertRaises(lanes.LaneError):
            lanes.find_active_lane(runtime, "publication-failure")

    def test_bootstrap_lock_prevents_loser_from_deleting_winner(self) -> None:
        repository = self.fixture.root_workspace
        subprocess.run(
            ["git", "init", "--initial-branch=main"],
            cwd=repository,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Harness Test"],
            cwd=repository,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.email", "harness@example.invalid"],
            cwd=repository,
            check=True,
        )
        (repository / "base.txt").write_text("base\n", encoding="utf-8")
        subprocess.run(["git", "add", "base.txt"], cwd=repository, check=True)
        subprocess.run(
            ["git", "commit", "-m", "base"],
            cwd=repository,
            check=True,
            capture_output=True,
        )
        self.fixture.active_cache()
        task_card = self.fixture.root / "contended.json"
        self.fixture._write_json(
            task_card,
            {
                "schema": "project-task-card/v1",
                "task": "only one contender may publish",
                "branch": "lane/contended",
                "base_commit": "HEAD",
            },
        )
        target = (
            repository
            / ".harness-runtime"
            / "worktrees"
            / "epoch-1"
            / "contended"
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        add_created = threading.Event()
        release_add = threading.Event()
        second_resolve = threading.Event()
        resolve_count = 0
        count_lock = threading.Lock()
        real_resolve = bootstrap._resolve_git_identity

        def observed_resolve(*args, **kwargs):
            nonlocal resolve_count
            with count_lock:
                resolve_count += 1
                if resolve_count == 2:
                    second_resolve.set()
            return real_resolve(*args, **kwargs)

        def held_add(
            root: Path, branch: str, path: Path, base_commit: str
        ) -> None:
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(root),
                    "worktree",
                    "add",
                    "-b",
                    branch,
                    str(path),
                    base_commit,
                ],
                check=True,
                capture_output=True,
            )
            add_created.set()
            self.assertTrue(release_add.wait(5))

        results: list[dict[str, object]] = []

        def contender() -> None:
            results.append(
                bootstrap.run_bootstrap(
                    lane_id="contended",
                    provider="codex",
                    model="test-model",
                    exclusive_resources=[],
                    task_card_path=str(task_card),
                )
            )

        with (
            patch(
                "orchestrator_harness.config.find_harness_root",
                return_value=self.fixture.harness,
            ),
            patch.object(bootstrap, "open_epoch", return_value={"epoch_id": "epoch-1"}),
            patch.object(bootstrap, "read_active_lanes", return_value=[]),
            patch.object(bootstrap, "write_active_lanes"),
            patch.object(bootstrap, "_resolve_git_identity", side_effect=observed_resolve),
            patch.object(bootstrap, "_git_worktree_add", side_effect=held_add),
        ):
            first = threading.Thread(target=contender)
            second = threading.Thread(target=contender)
            first.start()
            self.assertTrue(add_created.wait(5))
            second.start()
            self.assertFalse(
                second_resolve.wait(0.2),
                "the second contender reached Git preflight while the first held the lock",
            )
            release_add.set()
            first.join(10)
            second.join(10)

        self.assertEqual(2, len(results))
        self.assertEqual(1, sum(bool(item["ok"]) for item in results))
        self.assertTrue(target.is_dir(), "the losing contender deleted the winner")
        self.assertEqual(2, resolve_count)

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
        self, lane_id: str, *, managed: bool, provider: str
    ) -> tuple[dict[str, object], Path]:
        self.fixture._write_json(
            self.fixture.harness / "harness-config.json",
            {
                "root_workspace": str(self.fixture.root_workspace),
                "managed_coordination": "enabled" if managed else "disabled",
            },
        )
        runtime = self.fixture.root_workspace / ".harness-runtime"
        self.fixture.active_cache()
        task_card = self.fixture.root / f"{lane_id}.json"
        self.fixture._write_json(
            task_card,
            {
                "schema": "project-task-card/v1",
                "task": f"materialize {lane_id}",
                "branch": f"lane/{lane_id}",
                "base_commit": "test-base",
            },
        )
        epoch = {"epoch_id": "epoch-1"}
        worktree = runtime / "worktrees" / "epoch-1" / lane_id

        def fake_git(root: Path, branch: str, target: Path, base_commit: str) -> None:
            target.mkdir(parents=True, exist_ok=True)

        git_identity = {
            "source_root": str(self.fixture.root_workspace.resolve()),
            "common_dir": str((self.fixture.root_workspace / ".git").resolve()),
            "branch": f"lane/{lane_id}",
            "base_commit": "a" * 40,
            "origin_tip": "a" * 40,
            "bootstrap_tip": "a" * 40,
        }

        with (
            patch(
                "orchestrator_harness.config.find_harness_root",
                return_value=self.fixture.harness,
            ),
            patch.object(bootstrap, "open_epoch", return_value=epoch),
            patch.object(bootstrap, "read_active_lanes", return_value=[]),
            patch.object(
                bootstrap, "_resolve_git_identity", return_value=git_identity
            ),
            patch.object(
                bootstrap, "_git_worktree_add", side_effect=fake_git
            ) as git_add,
            patch.object(
                bootstrap,
                "_capture_created_worktree_identity",
                return_value={"fixture": "owned"},
            ),
            patch.object(bootstrap, "_verify_created_worktree"),
            patch.object(bootstrap.subprocess, "run") as provider_cli,
        ):
            result = bootstrap.run_bootstrap(
                lane_id=lane_id,
                provider=provider,
                model="test-model",
                exclusive_resources=[],
                task_card_path=str(task_card),
            )
        git_add.assert_called_once()
        provider_cli.assert_not_called()
        return result, worktree
if __name__ == "__main__":
    unittest.main()
