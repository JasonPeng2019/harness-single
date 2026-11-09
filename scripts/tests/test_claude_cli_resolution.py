"""Windows Claude CLI discovery supports native and npm shim installs."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import collect_codex_usage as collector


class ClaudeCliResolutionTests(unittest.TestCase):
    def test_windows_prefers_native_claude_executable(self):
        with (
            patch.object(collector.os, "name", "nt"),
            patch.object(
                collector.shutil,
                "which",
                side_effect=lambda name: {
                    "claude": "native-claude.exe",
                    "claude.cmd": "npm-claude.cmd",
                }.get(name),
            ) as which,
        ):
            self.assertEqual(
                collector.resolve_provider_executable("claude-code"),
                "native-claude.exe",
            )
        which.assert_called_once_with("claude")

    def test_windows_falls_back_to_npm_cmd_shim(self):
        with (
            patch.object(collector.os, "name", "nt"),
            patch.object(
                collector.shutil,
                "which",
                side_effect=lambda name: (
                    "npm-claude.cmd" if name == "claude.cmd" else None
                ),
            ) as which,
        ):
            self.assertEqual(
                collector.resolve_provider_executable("claude-code"),
                "npm-claude.cmd",
            )
        self.assertEqual(
            [call.args[0] for call in which.call_args_list], ["claude", "claude.cmd"]
        )

    def test_missing_claude_cli_refuses_to_launch(self):
        with (
            patch.object(collector.os, "name", "nt"),
            patch.object(collector.shutil, "which", return_value=None),
            self.assertRaisesRegex(
                OSError, "claude-code CLI executable not found on PATH"
            ),
        ):
            collector.resolve_provider_executable("claude-code")

    def test_windows_codex_selection_is_unchanged(self):
        with (
            patch.object(collector.os, "name", "nt"),
            patch.object(collector.shutil, "which", return_value="codex.cmd") as which,
        ):
            self.assertEqual(
                collector.resolve_provider_executable("codex"),
                "codex.cmd",
            )
        which.assert_called_once_with("codex.cmd")


if __name__ == "__main__":
    unittest.main()
