"""Focused regression tests for Claude permission defaults and denials.

The two related contracts are:
1. ``ClaudeCodeProviderAdapter.build_argv`` only emitted ``--permission-mode``
   when the caller set ``spec.permission_mode``, so every documented-shape
   lane silently ran under the CLI's interactive default and blocked every
   tool while still exiting 0 (a no-op lane).
2. ``parse_transcript_line`` ignored ``system/subtype=permission_denied``
   lines while the final ``result`` line reported ``is_error:false`` /
   ``subtype:success`` with EXIT 0 â€” so a blocked turn was published as a
   false COMPLETED.

This test pins the default-bypass argv and the fail-closed classification.
"""

from __future__ import annotations

import unittest
from pathlib import Path
from typing import Any, cast

from orchestrator_harness.provider import (
    ClaudeCodeProviderAdapter,
    ProviderEvent,
    ProviderLaunchSpec,
)


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
    )
    values.update(overrides)
    return ProviderLaunchSpec(**cast(dict[str, Any], values))


class ClaudeCodePermissionDenialTests(unittest.TestCase):
    def test_default_spec_emits_bypass_permission_mode(self) -> None:
        adapter = ClaudeCodeProviderAdapter()
        argv = adapter.build_argv(_spec())
        self.assertIn("--permission-mode", argv)
        self.assertEqual(
            "bypassPermissions",
            argv[argv.index("--permission-mode") + 1],
        )

    def test_explicit_permission_mode_wins(self) -> None:
        adapter = ClaudeCodeProviderAdapter()
        argv = adapter.build_argv(_spec(permission_mode="default"))
        self.assertEqual("default", argv[argv.index("--permission-mode") + 1])

    def test_permission_denied_transcript_never_classifies_completed(self) -> None:
        # The real CLI shape: a mid-stream permission_denied system line, then
        # a terminal result line that still says is_error:false/success but
        # carries its own permission_denials list.
        adapter = ClaudeCodeProviderAdapter()
        transcript = [
            b'{"type":"system","subtype":"init","session_id":"session-1"}',
            b'{"type":"system","subtype":"permission_denied","tool_name":"Write",'
            b'"message":"Claude requested permissions to write, but you haven\'t '
            b'granted it yet.","session_id":"session-1"}',
            b'{"type":"result","subtype":"success","is_error":false,'
            b'"session_id":"session-1",'
            b'"permission_denials":[{"tool_name":"Write"}]}',
        ]
        terminal = self._drain(adapter, transcript)
        self.assertIsNotNone(terminal)
        assert terminal is not None
        self.assertNotEqual("COMPLETED", terminal.kind)
        self.assertEqual("FAILED", terminal.kind)
        self.assertEqual("FAILED", adapter.terminal_outcome(terminal, 0))

    def test_result_permission_denials_alone_classifies_failed(self) -> None:
        # Even without the mid-stream system line, the terminal result line
        # carries its own permission_denials list: the LAST terminal event must
        # not be a false COMPLETED.
        adapter = ClaudeCodeProviderAdapter()
        transcript = [
            b'{"type":"system","subtype":"init","session_id":"session-1"}',
            b'{"type":"result","subtype":"success","is_error":false,'
            b'"session_id":"session-1",'
            b'"permission_denials":[{"tool_name":"Write","tool_use_id":"call-1"}]}',
        ]
        terminal = self._drain(adapter, transcript)
        self.assertIsNotNone(terminal)
        assert terminal is not None
        self.assertEqual("FAILED", terminal.kind)
        self.assertEqual("FAILED", adapter.terminal_outcome(terminal, 0))

    def test_clean_success_transcript_still_completed(self) -> None:
        adapter = ClaudeCodeProviderAdapter()
        transcript = [
            b'{"type":"system","subtype":"init","session_id":"session-1"}',
            b'{"type":"result","subtype":"success","is_error":false,'
            b'"session_id":"session-1"}',
        ]
        terminal = self._drain(adapter, transcript)
        self.assertIsNotNone(terminal)
        assert terminal is not None
        self.assertEqual("COMPLETED", terminal.kind)
        self.assertEqual("COMPLETED", adapter.terminal_outcome(terminal, 0))

    @staticmethod
    def _drain(
        adapter: ClaudeCodeProviderAdapter, transcript: list[bytes]
    ) -> ProviderEvent | None:
        terminal: ProviderEvent | None = None
        for line in transcript:
            event = adapter.parse_transcript_line(line)
            if event is not None and event.is_terminal:
                terminal = event
        return terminal


if __name__ == "__main__":
    unittest.main()
