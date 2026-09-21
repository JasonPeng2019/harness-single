from __future__ import annotations

import contextlib
import io
import unittest
from unittest import mock

from orchestrator_harness import operator_launch


class OperatorLaunchV2Tests(unittest.TestCase):
    def test_parser_exposes_only_v2_route(self) -> None:
        parser = operator_launch._build_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args(["--receipt", "detached.json"])
        parsed = parser.parse_args(["harness", "shutdown"])
        self.assertEqual("harness", parsed.command)
        self.assertEqual("shutdown", parsed.harness_command)

    def test_receipt_dispatch_is_not_reachable(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            with self.assertRaises(SystemExit) as raised:
                operator_launch.main(["--receipt", "detached.json"])
        self.assertEqual(2, raised.exception.code)
        self.assertIn("invalid choice", output.getvalue())

    def test_force_release_is_only_under_top_level_lease(self) -> None:
        parser = operator_launch._build_parser()
        parsed = parser.parse_args(
            ["lease", "force-release", "--resource-id", "resource-1"]
        )
        self.assertEqual("lease", parsed.command)
        self.assertEqual("force-release", parsed.lease_command)
        self.assertEqual("resource-1", parsed.resource_id)
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(
                    ["lane", "force-release", "--resource-id", "resource-1"]
                )

    def test_manager_close_requires_and_normalizes_summary(self) -> None:
        parser = operator_launch._build_parser()
        for outcome in ("COMPLETE", "BLOCKED"):
            for summary in (None, "", "   "):
                argv = [
                    "manager", "close", "--event-id", "event-1",
                    "--outcome", outcome,
                ]
                if summary is not None:
                    argv.extend(["--summary", summary])
                with self.subTest(outcome=outcome, summary=summary):
                    with contextlib.redirect_stderr(io.StringIO()):
                        with self.assertRaises(SystemExit):
                            parser.parse_args(argv)
            parsed = parser.parse_args(
                [
                    "manager", "close", "--event-id", "event-1",
                    "--outcome", outcome, "--summary",
                    "  operator decision  ",
                ]
            )
            self.assertEqual("operator decision", parsed.summary)

    def test_bootstrap_collects_explicit_provider_options(self) -> None:
        parser = operator_launch._build_parser()
        parsed = parser.parse_args(
            [
                "lane", "bootstrap", "--lane-id", "lane-1",
                "--provider", "codex", "--model", "configured-model",
                "--provider-option", "reasoning_effort=xhigh",
                "--provider-option", "service_tier=flex",
                "--task-card", "task.json",
            ]
        )
        self.assertEqual(
            {"reasoning_effort": "xhigh", "service_tier": "flex"},
            operator_launch._provider_options(parsed.provider_option),
        )
        with self.assertRaisesRegex(ValueError, "duplicate provider option"):
            operator_launch._provider_options([("effort", "high"), ("effort", "low")])

    def test_v2_commands_dispatch_through_native_modules(self) -> None:
        with mock.patch.object(
            operator_launch.setup,
            "run_setup",
            return_value={"ok": True, "summary": "setup"},
        ) as run_setup:
            self.assertEqual(0, operator_launch.main(["harness", "setup"]))
        run_setup.assert_called_once_with(overwrite=False)


if __name__ == "__main__":
    unittest.main()
