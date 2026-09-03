"""Deliberately strict product oracles for known admitted baseline gaps.

These tests are expected to be red at 7d74fb6.  Their assertions are not marked
as expected failures: after DEL-001 they must turn green without editing this
asset.  A red result is a product finding, while the map/fixture tests establish
that the asset mechanics themselves are sound.
"""

from __future__ import annotations

import unittest
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path

from orchestrator_harness import operator_launch


class BaselineProductOracleTests(unittest.TestCase):
    """Product-facing baseline-red CHECK-U1/U3/U4 contract oracles."""

    def test_bound_011_exposes_only_public_orphan_lease_force_release_route(self) -> None:
        parser = operator_launch._build_parser()
        try:
            with redirect_stderr(StringIO()):
                parsed = parser.parse_args(["lease", "force-release", "--resource-id", "resource-1"])
        except SystemExit as exc:
            self.fail(f"BOUND-011 public route is absent (parser exited {exc.code})")
        self.assertEqual("force-release", parsed.lease_command)
        self.assertEqual("resource-1", parsed.resource_id)

    def test_bound_012_requires_nonempty_close_summary_for_every_outcome(self) -> None:
        parser = operator_launch._build_parser()
        for outcome in ("COMPLETE", "BLOCKED"):
            for summary in (None, "", "   "):
                command = ["manager", "close", "--event-id", "event-1", "--outcome", outcome]
                if summary is not None:
                    command.extend(["--summary", summary])
                with self.subTest(outcome=outcome, summary=summary):
                    with redirect_stderr(StringIO()), self.assertRaises(SystemExit):
                        parser.parse_args(command)
            accepted = parser.parse_args(
                ["manager", "close", "--event-id", "event-1", "--outcome", outcome, "--summary", "operator decision"]
            )
            self.assertEqual("operator decision", accepted.summary)

    def test_runtime_and_lane_worktrees_are_explicitly_git_ignored(self) -> None:
        root = Path(__file__).resolve().parents[3]
        ignored = (root / ".gitignore").read_text(encoding="utf-8")
        self.assertIn(".harness-runtime/", ignored)
        self.assertIn(".agent-workspace/", ignored)
