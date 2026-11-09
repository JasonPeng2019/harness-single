from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from orchestrator_harness import setup

ROOT = Path(__file__).resolve().parents[2]
CLAUDE = ROOT / "adapters" / "claude-code"
HOOKS = {
    "PostToolUse": "orchestrator_harness_post_tool_use.py",
    "Stop": "orchestrator_harness_stop.py",
}


class ClaudeHookProjectRootTests(unittest.TestCase):
    def test_rooted_commands_validate_against_captured_worker_catalog(self) -> None:
        payload = CLAUDE / "super-cache"
        files = {
            Path(".claude/settings.json"): payload / ".claude/settings.json",
            **{
                Path(".claude/hooks") / name: payload / ".claude/hooks" / name
                for name in HOOKS.values()
            },
        }
        setup._validate_worker_commands("claude-code", ".claude", files)
        settings = json.loads(
            (payload / ".claude/settings.json").read_text(encoding="utf-8")
        )
        for event, groups in settings["hooks"].items():
            command = groups[0]["hooks"][0]["command"]
            self.assertEqual(
                f'python "${{CLAUDE_PROJECT_DIR}}/.claude/hooks/{HOOKS[event]}"',
                command,
            )

    def test_declared_commands_open_both_payloads_from_nested_unicode_cwd(self) -> None:
        for payload_name in ("root", "super-cache"):
            with (
                self.subTest(payload=payload_name),
                tempfile.TemporaryDirectory(
                    prefix="claude-root-", ignore_cleanup_errors=True
                ) as raw,
            ):
                project = Path(raw) / "project space \u03a9"
                nested = project / "nested path" / "deeper"
                nested.mkdir(parents=True)
                payload = CLAUDE / payload_name
                settings = json.loads(
                    (payload / ".claude/settings.json").read_text(encoding="utf-8")
                )
                for hook_name in HOOKS.values():
                    destination = project / ".claude/hooks" / hook_name
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(payload / ".claude/hooks" / hook_name, destination)
                if payload_name == "super-cache":
                    shutil.copytree(
                        ROOT / "super-cache" / "workspace" / ".agent-workspace",
                        project / ".agent-workspace",
                    )
                else:
                    (
                        project / ".claude" / "orchestrator-harness-binding.json"
                    ).write_text("{}", encoding="utf-8")
                # The wrapper emits a JSON decision even when a deliberately minimal
                # fixture has no live binding; stdout proves Python opened the target.
                env = dict(os.environ, CLAUDE_PROJECT_DIR=str(project))
                git = shutil.which("git")
                candidates = []
                if git:
                    git_root = Path(git).resolve().parents[1]
                    candidates = [
                        git_root / "bin" / "bash.exe",
                        git_root / "usr" / "bin" / "bash.exe",
                    ]
                bash = next((path for path in candidates if path.is_file()), None)
                if bash is None:
                    self.skipTest(
                        "Git Bash is required to execute Claude's POSIX hook command"
                    )
                for event, groups in settings["hooks"].items():
                    command = groups[0]["hooks"][0]["command"]
                    run = subprocess.run(
                        [str(bash), "-c", command],
                        cwd=nested,
                        env=env,
                        input="{}\n",
                        text=True,
                        capture_output=True,
                        timeout=15,
                    )
                    self.assertEqual(0, run.returncode, run.stderr)
                    self.assertIsInstance(json.loads(run.stdout), dict)
                    self.assertNotIn("can't open file", run.stderr)

    def test_altered_rooted_command_is_rejected_before_catalog_acceptance(self) -> None:
        payload = CLAUDE / "super-cache"
        settings_path = payload / ".claude/settings.json"
        settings = json.loads(settings_path.read_text(encoding="utf-8"))
        settings["hooks"]["Stop"][0]["hooks"][0]["command"] = (
            'python "${CLAUDE_PROJECT_DIR}/.claude/hooks/other.py"'
        )
        with tempfile.TemporaryDirectory() as raw:
            temp = Path(raw)
            config = temp / ".claude/settings.json"
            config.parent.mkdir()
            config.write_text(json.dumps(settings), encoding="utf-8")
            files = {
                Path(".claude/settings.json"): config,
                **{
                    Path(".claude/hooks") / name: payload / ".claude/hooks" / name
                    for name in HOOKS.values()
                },
            }
            with self.assertRaises(setup.SetupError):
                setup._validate_worker_commands("claude-code", ".claude", files)


if __name__ == "__main__":
    unittest.main()
