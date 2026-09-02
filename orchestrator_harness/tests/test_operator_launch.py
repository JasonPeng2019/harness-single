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
