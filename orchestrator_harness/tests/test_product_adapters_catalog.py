"""Focused product tests for the shipped super-cache and provider adapter catalog.

Statically and importably proves the complete tree: the provider-neutral
super-cache helpers, the three provider catalogs, the exact 8+2 skill
inventory and content, the registered launcher bindings (provider IDs and
symbols), helper compilation, and the absence of empty placeholders.
"""

from __future__ import annotations

import importlib.util
import json
import py_compile
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

PROVIDERS = {
    "codex": ".codex",
    "claude-code": ".claude",
    "qwen-code": ".qwen",
}

ROOT_SKILLS = [
    "manager-notification-watch",
    "acknowledge-manager-notification",
    "close-manager-notification",
    "review-lane-completion",
    "send-lane-notification",
    "resume-lane",
    "force-stop-lane",
    "harness-shutdown",
]

WORKER_SKILLS = ["manager-notify", "lane-assignment"]

HELPERS = [
    "result-stop-check.py",
    "lane-queue.py",
    "manager-notify.py",
    "hook-dispatch.py",
]

PLACEHOLDER_MARKERS = ("TODO", "TBD", "FIXME", "lorem", "placeholder", "XXX")


class SuperCacheWorkspaceTests(unittest.TestCase):
    def test_workspace_skeleton_and_helpers(self) -> None:
        agent_workspace = (
            REPOSITORY_ROOT / "super-cache" / "workspace" / ".agent-workspace"
        )
        self.assertTrue(agent_workspace.is_dir())
        for helper in HELPERS:
            path = agent_workspace / helper
            self.assertTrue(path.is_file(), f"missing helper: {path}")
            self.assertGreater(path.stat().st_size, 0, f"empty helper: {path}")

    def test_custom_marker_only(self) -> None:
        custom = REPOSITORY_ROOT / "super-cache" / "custom"
        self.assertTrue(custom.is_dir())
        names = {item.name for item in custom.iterdir()}
        self.assertTrue(names, "custom/ must contain a README or keep marker")
        self.assertLessEqual(names, {"README.md", ".keep"})


class AdapterCatalogLayoutTests(unittest.TestCase):
    def test_catalog_guide_and_shipped_machinery(self) -> None:
        self.assertTrue((REPOSITORY_ROOT / "adapters" / "README.md").is_file())
        self.assertTrue((REPOSITORY_ROOT / "adapters" / "shipped-machinery").is_dir())

    def test_each_provider_tree(self) -> None:
        for provider_id, dotdir in PROVIDERS.items():
            adapter = REPOSITORY_ROOT / "adapters" / provider_id
            for required in ("README.md", "root", "super-cache", "harness", "shipped-machinery"):
                self.assertTrue(
                    (adapter / required).exists(), f"{provider_id}: missing {required}"
                )
            self.assertTrue((adapter / "harness" / "launcher_binding.py").is_file())
            self.assertTrue((adapter / "root" / dotdir).is_dir())
            self.assertTrue((adapter / "super-cache" / dotdir).is_dir())


class RootPayloadTests(unittest.TestCase):
    def test_exact_eight_root_skills(self) -> None:
        for provider_id, dotdir in PROVIDERS.items():
            skills = REPOSITORY_ROOT / "adapters" / provider_id / "root" / dotdir / "skills"
            self.assertTrue(skills.is_dir(), f"{provider_id}: skills dir missing")
            names = sorted(item.name for item in skills.iterdir() if item.is_dir())
            self.assertEqual(names, sorted(ROOT_SKILLS), provider_id)
            for name in ROOT_SKILLS:
                skill_md = skills / name / "SKILL.md"
                self.assertTrue(skill_md.is_file(), f"{provider_id}: missing {name}/SKILL.md")
                self.assertGreater(
                    skill_md.stat().st_size, 200, f"{provider_id}: {name} SKILL.md too small"
                )

    def test_root_payload_has_config_binding_hooks(self) -> None:
        for provider_id, dotdir in PROVIDERS.items():
            root = REPOSITORY_ROOT / "adapters" / provider_id / "root" / dotdir
            configs = [root / "config.toml", root / "settings.json"]
            self.assertTrue(any(path.is_file() for path in configs), provider_id)
            self.assertTrue(
                (root / "orchestrator-harness-binding.json").is_file(), provider_id
            )
            self.assertTrue((root / "hooks" / "post-tool-use.py").is_file(), provider_id)
            declarations = [root / "hooks.json", root / "settings.json"]
            self.assertTrue(any(path.is_file() for path in declarations), provider_id)

    def test_root_skills_wrap_public_commands(self) -> None:
        expectations = {
            "manager-notification-watch": "watch --until-actionable",
            "acknowledge-manager-notification": "manager acknowledge --event-id",
            "close-manager-notification": "manager close --event-id",
            "review-lane-completion": "lane completion-review",
            "send-lane-notification": "send-lane-notification --lane-id",
            "resume-lane": "resume-lane --lane-id",
            "force-stop-lane": "lane force-stop --lane-id",
            "harness-shutdown": "harness shutdown",
        }
        for provider_id, dotdir in PROVIDERS.items():
            skills = REPOSITORY_ROOT / "adapters" / provider_id / "root" / dotdir / "skills"
            for name, command in expectations.items():
                text = (skills / name / "SKILL.md").read_text(encoding="utf-8")
                self.assertIn(command, text, f"{provider_id}: {name}")

    def test_root_binding_record_valid(self) -> None:
        for provider_id, dotdir in PROVIDERS.items():
            path = (
                REPOSITORY_ROOT
                / "adapters"
                / provider_id
                / "root"
                / dotdir
                / "orchestrator-harness-binding.json"
            )
            record = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(record["schema"], "harness-hook-binding/v1", provider_id)
            self.assertEqual(record["role"], "root", provider_id)
            self.assertEqual(record["provider_id"], provider_id)


class WorkerPayloadTests(unittest.TestCase):
    def test_exact_two_worker_skills(self) -> None:
        for provider_id, dotdir in PROVIDERS.items():
            skills = (
                REPOSITORY_ROOT / "adapters" / provider_id / "super-cache" / dotdir / "skills"
            )
            self.assertTrue(skills.is_dir(), f"{provider_id}: worker skills dir missing")
            names = sorted(item.name for item in skills.iterdir() if item.is_dir())
            self.assertEqual(names, sorted(WORKER_SKILLS), provider_id)
            for name in WORKER_SKILLS:
                skill_md = skills / name / "SKILL.md"
                self.assertTrue(skill_md.is_file(), f"{provider_id}: missing {name}/SKILL.md")
                self.assertGreater(
                    skill_md.stat().st_size, 200, f"{provider_id}: {name} SKILL.md too small"
                )

    def test_worker_skills_use_only_shipped_helpers(self) -> None:
        allowed = {
            ".agent-workspace/manager-notify.py",
            ".agent-workspace/lane-queue.py",
        }
        for provider_id, dotdir in PROVIDERS.items():
            skills = (
                REPOSITORY_ROOT / "adapters" / provider_id / "super-cache" / dotdir / "skills"
            )
            for name in WORKER_SKILLS:
                text = (skills / name / "SKILL.md").read_text(encoding="utf-8")
                refs = set(re.findall(r"\.agent-workspace/[A-Za-z0-9_-]+\.py", text))
                self.assertLessEqual(refs, allowed, f"{provider_id}: {name}")
                self.assertIn("hand-edit", text, f"{provider_id}: {name} forbids edits")
                self.assertNotIn("operator_launch", text, f"{provider_id}: {name}")

    def test_worker_payload_has_config_binding_hooks(self) -> None:
        for provider_id, dotdir in PROVIDERS.items():
            payload = REPOSITORY_ROOT / "adapters" / provider_id / "super-cache" / dotdir
            configs = [payload / "config.toml", payload / "settings.json"]
            self.assertTrue(any(path.is_file() for path in configs), provider_id)
            self.assertTrue(
                (payload / "orchestrator-harness-binding.json").is_file(), provider_id
            )
            self.assertTrue((payload / "hooks" / "post-tool-use.py").is_file(), provider_id)

    def test_worker_binding_record_valid(self) -> None:
        for provider_id, dotdir in PROVIDERS.items():
            path = (
                REPOSITORY_ROOT
                / "adapters"
                / provider_id
                / "super-cache"
                / dotdir
                / "orchestrator-harness-binding.json"
            )
            record = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(record["schema"], "harness-hook-binding/v1", provider_id)
            self.assertEqual(record["role"], "worker", provider_id)
            self.assertEqual(record["provider_id"], provider_id)
            self.assertIn("inbox_path", record, provider_id)
            self.assertIn("outbox_dir", record, provider_id)
            self.assertIn("result_stop_check", record, provider_id)


class SkillContentTests(unittest.TestCase):
    def test_no_placeholder_markers(self) -> None:
        roots = [
            REPOSITORY_ROOT / "super-cache",
            REPOSITORY_ROOT / "adapters",
            REPOSITORY_ROOT / "orchestrator_harness" / "provider_adapters",
        ]
        suffixes = (".md", ".py", ".json", ".toml")
        for root in roots:
            for path in root.rglob("*"):
                if not path.is_file() or path.suffix not in suffixes:
                    continue
                text = path.read_text(encoding="utf-8")
                for marker in PLACEHOLDER_MARKERS:
                    self.assertNotIn(marker, text, f"{path} contains {marker!r}")


class RegisteredBindingTests(unittest.TestCase):
    def _load_registered(self, provider_id: str) -> object:
        path = (
            REPOSITORY_ROOT
            / "orchestrator_harness"
            / "provider_adapters"
            / provider_id
            / "launcher_binding.py"
        )
        self.assertTrue(path.is_file(), f"missing registered binding: {path}")
        spec = importlib.util.spec_from_file_location(f"_binding_{provider_id}", path)
        self.assertIsNotNone(spec, provider_id)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_registered_bindings_symbols_and_identity(self) -> None:
        for provider_id in PROVIDERS:
            module = self._load_registered(provider_id)
            self.assertEqual(module.PROVIDER_ID, provider_id)
            self.assertIsInstance(module.ADAPTER_VERSION, str)
            self.assertTrue(module.ADAPTER_VERSION)
            self.assertTrue(callable(module.build_argv))
            self.assertTrue(callable(module.parse_line))

    def test_catalog_binding_matches_registered(self) -> None:
        for provider_id in PROVIDERS:
            catalog = (
                REPOSITORY_ROOT / "adapters" / provider_id / "harness" / "launcher_binding.py"
            )
            registered = (
                REPOSITORY_ROOT
                / "orchestrator_harness"
                / "provider_adapters"
                / provider_id
                / "launcher_binding.py"
            )
            self.assertEqual(catalog.read_bytes(), registered.read_bytes(), provider_id)

    def test_build_argv_shapes(self) -> None:
        codex = self._load_registered("codex")
        argv = codex.build_argv(
            model="gpt-5.4",
            worktree="C:/wt",
            prompt_path="C:/wt/.agent-workspace/worker-prompt.md",
        )
        self.assertEqual(argv[0], "codex")
        self.assertIn("exec", argv)
        self.assertIn("--dangerously-bypass-approvals-and-sandbox", argv)
        resume_argv = codex.build_argv(
            model="gpt-5.4",
            worktree="C:/wt",
            prompt_path="C:/wt/.agent-workspace/worker-prompt.md",
            session_id="s1",
            resume=True,
        )
        self.assertIn("resume", resume_argv)
        self.assertIn("s1", resume_argv)

        claude = self._load_registered("claude-code")
        argv = claude.build_argv(
            model="deepseek-v4-flash:0731-cloud",
            worktree="C:/wt",
            prompt_path="C:/wt/.agent-workspace/worker-prompt.md",
        )
        self.assertEqual(argv[0], "claude")
        self.assertIn("--print", argv)
        self.assertIn("--output-format", argv)
        self.assertIn("stream-json", argv)
        self.assertIn("--verbose", argv)

        qwen = self._load_registered("qwen-code")
        with mock.patch.object(qwen.shutil, "which", return_value=None):
            argv = qwen.build_argv(
                model="qwen3-coder",
                worktree="C:/wt",
                prompt_path="C:/wt/.agent-workspace/worker-prompt.md",
            )
        self.assertEqual(argv[0], "qwen")
        self.assertIn("--approval-mode=yolo", argv)
        self.assertIn("--output-format", argv)
        self.assertIn("stream-json", argv)

    def test_qwen_binding_uses_portable_executable_discovery(self) -> None:
        qwen = self._load_registered("qwen-code")
        kwargs = {
            "model": "qwen3-coder",
            "worktree": "C:/wt",
            "prompt_path": "C:/wt/.agent-workspace/worker-prompt.md",
        }
        resolved = "C:/portable/qwen-code/bin/qwen.cmd"
        with mock.patch.object(qwen.shutil, "which", return_value=resolved) as discover:
            argv = qwen.build_argv(**kwargs)
        discover.assert_called_once_with("qwen")
        self.assertEqual(argv[0], resolved)
        self.assertIn("--approval-mode=yolo", argv)
        self.assertIn("--output-format", argv)
        self.assertIn("stream-json", argv)

        with mock.patch.object(qwen.shutil, "which", return_value=None) as discover:
            argv = qwen.build_argv(**kwargs)
        discover.assert_called_once_with("qwen")
        self.assertEqual(argv[0], "qwen")
        self.assertIn("--approval-mode=yolo", argv)

    def test_parse_line_facts(self) -> None:
        codex = self._load_registered("codex")
        self.assertEqual(
            codex.parse_line('{"type": "thread.started", "thread_id": "t1"}'),
            {"session_id": "t1"},
        )
        self.assertIsNone(codex.parse_line("not json"))

        claude = self._load_registered("claude-code")
        self.assertEqual(
            claude.parse_line('{"type": "system", "subtype": "init", "session_id": "s1"}'),
            {"session_id": "s1"},
        )

        qwen = self._load_registered("qwen-code")
        parsed = qwen.parse_line('{"type": "result", "subtype": "success", "session_id": "s2"}')
        self.assertEqual(parsed.get("session_id"), "s2")
        self.assertIn("message", parsed)


class HelperCompilationTests(unittest.TestCase):
    def test_helpers_compile(self) -> None:
        agent_workspace = (
            REPOSITORY_ROOT / "super-cache" / "workspace" / ".agent-workspace"
        )
        with tempfile.TemporaryDirectory() as tmp:
            for helper in HELPERS:
                py_compile.compile(
                    str(agent_workspace / helper),
                    cfile=str(Path(tmp) / (helper + "c")),
                    doraise=True,
                )

    def test_hooks_compile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            for path in (REPOSITORY_ROOT / "adapters").rglob("hooks/*.py"):
                py_compile.compile(
                    str(path), cfile=str(Path(tmp) / (path.name + "c")), doraise=True
                )


if __name__ == "__main__":
    unittest.main()
