from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


class CandidateSafeguardTests(unittest.TestCase):
    def test_launcher_is_portable_exact_root_bound_and_selector_owned(self) -> None:
        script = (
            REPOSITORY_ROOT / "tools" / "Invoke-CandidateSafeguard.ps1"
        ).read_text(encoding="utf-8")
        self.assertNotIn("C:/Users/", script)
        self.assertIn("RepositoryRoot", script)
        self.assertIn("ExpectedTip", script)
        self.assertIn("orchestrator_harness.release_checks", script)
        self.assertIn("--root", script)
        self.assertIn("--expected-tip", script)
        self.assertIn("firmware/v2-candidate", script)
        self.assertIn("refusing dirty repository root", script)
        self.assertIn("4699d27bd5bf7c0b41bbed9ddb6b0b7d019e215f", script)
        self.assertIn("baseline changed during safeguard", script)

    def test_launcher_rejects_this_non_candidate_lane_without_running_checks(
        self,
    ) -> None:
        script = REPOSITORY_ROOT / "tools" / "Invoke-CandidateSafeguard.ps1"
        completed = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(script),
            ],
            cwd=REPOSITORY_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(0, completed.returncode)
        self.assertIn("unexpected branch", completed.stderr)

    def test_launcher_rejects_different_same_suffix_root(self) -> None:
        script = REPOSITORY_ROOT / "tools" / "Invoke-CandidateSafeguard.ps1"
        with tempfile.TemporaryDirectory() as temporary:
            same_suffix_root = (
                Path(temporary)
                / "other-clone"
                / "plans"
                / "general-coding-harness"
                / "runtime"
                / "firmware-v2"
                / "worktrees"
                / "candidate-root"
            )
            copied_script = same_suffix_root / "tools" / script.name
            copied_script.parent.mkdir(parents=True)
            shutil.copy2(script, copied_script)
            completed = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(copied_script),
                ],
                text=True,
                capture_output=True,
                check=False,
            )
        self.assertNotEqual(0, completed.returncode)
        self.assertIn("not a git repository", completed.stderr.lower())

    def test_legacy_example_remains_schema_less_and_dual_path_recipe_names_native_wait(
        self,
    ) -> None:
        legacy = json.loads(
            (
                REPOSITORY_ROOT / "examples" / "legacy-firmware.invocation.example.json"
            ).read_text(encoding="utf-8")
        )
        self.assertNotIn("schema", legacy)
        self.assertIn("policy_sha256", legacy)
        self.assertNotIn("repository", legacy)
        recipe = (
            REPOSITORY_ROOT / "examples" / "dual-path-manager.example.md"
        ).read_text(encoding="utf-8")
        self.assertIn("watch --until-actionable", recipe)
        self.assertIn("top-level-event-id", recipe)


if __name__ == "__main__":
    unittest.main()
