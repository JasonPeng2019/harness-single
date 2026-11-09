"""CLI contract tests for explicit profile preparation."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import windows_worker_preparation

SCRIPT = Path(__file__).resolve().parents[1] / "prepare_windows_workers.py"
spec = importlib.util.spec_from_file_location("prepare_windows_workers", SCRIPT)
if spec is None or spec.loader is None:
    raise RuntimeError("worker preparation CLI could not be loaded")
prepare_cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare_cli)


class PrepareWindowsWorkersCliTests(unittest.TestCase):
    def test_fresh_source_entrypoint_imports_the_requested_harness_without_pythonpath(
        self,
    ) -> None:
        harness = Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            environment = {
                key: value for key, value in os.environ.items() if key != "PYTHONPATH"
            }
            completed = subprocess.run(
                [
                    sys.executable,
                    "-S",
                    str(SCRIPT),
                    "--harness-dir",
                    str(harness),
                    "--profile",
                    str(root / "missing" / ".codex" / "config.toml"),
                    "--worktree",
                    str(root / "missing"),
                    "--permission-mode",
                    "worker-isolated",
                    "--local-appdata",
                    str(root / "cache"),
                    "--temp-dir",
                    str(root / "temp"),
                    "--evidence-dir",
                    str(harness / "runtime" / "missing"),
                ],
                cwd=root,
                check=False,
                env=environment,
                capture_output=True,
                text=True,
                timeout=30,
            )
        self.assertEqual(2, completed.returncode)
        self.assertIn("WINDOWS_WORKER_PREPARATION_FAILED", completed.stderr)
        self.assertNotIn("ModuleNotFoundError", completed.stderr)

    def test_cli_requires_explicit_profiles_mode_cache_temps_and_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            profile = root / "worktree" / ".codex" / "config.toml"
            cache = root / "cache"
            first_temp = root / "one-temp"
            second_temp = root / "two-temp"
            evidence = root / "evidence"
            argv = [
                "prepare_windows_workers.py",
                "--harness-dir",
                str(root),
                "--profile",
                str(profile),
                "--worktree",
                str(profile.parent.parent),
                "--profile",
                str(root / "peer" / ".codex" / "config.toml"),
                "--worktree",
                str(root / "peer"),
                "--permission-mode",
                "worker-isolated",
                "--local-appdata",
                str(cache),
                "--temp-dir",
                str(first_temp),
                "--temp-dir",
                str(second_temp),
                "--evidence-dir",
                str(evidence),
            ]
            called = {}

            def prepare(*args, **kwargs):
                called["args"] = args
                called["kwargs"] = kwargs
                return [
                    {
                        "profile_path": str(profile),
                        "profile_sha256": "a" * 64,
                        "permission_mode": "worker-isolated",
                    }
                ]

            stdout = io.StringIO()
            with (
                patch.object(sys, "argv", argv),
                patch.object(
                    windows_worker_preparation, "prepare_profiles", side_effect=prepare
                ),
                contextlib.redirect_stdout(stdout),
            ):
                result = prepare_cli.main()
            self.assertEqual(0, result)
            targets = called["args"][1]
            self.assertEqual(
                [profile, root / "peer" / ".codex" / "config.toml"],
                [item.config_path for item in targets],
            )
            self.assertEqual(
                [first_temp, second_temp], [item.temporary for item in targets]
            )
            self.assertEqual(cache, called["kwargs"]["local_appdata"])
            self.assertIn('"status": "PREPARED"', stdout.getvalue())

    def test_cli_returns_error_without_replacing_preparation_contract(self) -> None:
        with (
            patch.object(sys, "argv", ["prepare_windows_workers.py", "--help"]),
            self.assertRaises(SystemExit) as raised,
        ):
            prepare_cli.main()
        self.assertEqual(0, raised.exception.code)


if __name__ == "__main__":
    unittest.main(verbosity=2)
