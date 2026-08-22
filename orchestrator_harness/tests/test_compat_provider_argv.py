"""Focused regression test for Claude Code print/stream-json argv.

The real Claude CLI hard-rejects
``--print --output-format stream-json`` without ``--verbose``
(``Error: When using --print, --output-format=stream-json requires --verbose``,
exit code 1), so every claude-code lane died before its first turn.  This test
pins the fixed argv shape so the flag cannot regress.
"""

from __future__ import annotations

import unittest
from pathlib import Path
from typing import Any, cast

from orchestrator_harness.provider import ClaudeCodeProviderAdapter, ProviderLaunchSpec


def _spec(**overrides: object) -> ProviderLaunchSpec:
    values: dict[str, object] = dict(
        action="start",
        command=("claude",),
        model="deepseek-v4-flash:0731-cloud",
        reasoning_effort="medium",
        service_tier="standard",
        session_id=None,
        run_root=Path("C:/run"),
        last_message_path=Path("C:/run/.agent-workspace/last"),
        permission_mode="bypassPermissions",
    )
    values.update(overrides)
    return ProviderLaunchSpec(**cast(dict[str, Any], values))


class ClaudeCodeArgvCompatTests(unittest.TestCase):
    def test_claude_start_argv_has_verbose(self) -> None:
        adapter = ClaudeCodeProviderAdapter()
        argv = adapter.build_argv(_spec(action="start"))
        self.assertIn("--verbose", argv)
        # --verbose must sit immediately after stream-json: the flag only
        # applies in print/stream-json mode.
        self.assertEqual(
            "--verbose", argv[argv.index("--output-format") + 2]
        )
        self.assertEqual(
            [
                "claude",
                "--print",
                "--output-format",
                "stream-json",
                "--verbose",
                "--model",
                "deepseek-v4-flash:0731-cloud",
                "--permission-mode",
                "bypassPermissions",
            ],
            argv,
        )

    def test_claude_resume_argv_keeps_resume_and_verbose(self) -> None:
        adapter = ClaudeCodeProviderAdapter()
        argv = adapter.build_argv(
            _spec(action="resume", session_id="session-abc123")
        )
        self.assertIn("--resume", argv)
        self.assertEqual("session-abc123", argv[argv.index("--resume") + 1])
        self.assertIn("--verbose", argv)
        self.assertEqual(
            "--verbose", argv[argv.index("--output-format") + 2]
        )
        self.assertLess(
            argv.index("--verbose"), argv.index("--resume"), "stream-json flags come first"
        )


if __name__ == "__main__":
    unittest.main()
