from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from orchestrator_harness import release_checks
from orchestrator_harness.tests.support import TemporaryGitRepository

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_WINDOWLESS_CREATION_FLAGS = 0x08000000 if os.name == "nt" else 0


class CandidateSafeguardTests(unittest.TestCase):
    def test_launcher_is_portable_exact_root_bound_and_selector_owned(self) -> None:
        script = (
            REPOSITORY_ROOT / "tools" / "Invoke-CandidateSafeguard.ps1"
        ).read_text(encoding="utf-8")
        core = (REPOSITORY_ROOT / "tools" / "CandidateSafeguard.Core.psm1").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("C:/Users/", script)
        self.assertIn("RepositoryRoot", script)
        self.assertIn("ExpectedTip", script)
        self.assertIn("orchestrator_harness.release_checks", script)
        self.assertIn("--root", script)
        self.assertIn("--expected-tip", script)
        self.assertIn("ExpectedBranch", script)
        self.assertIn("Mandatory", script)
        self.assertNotIn("firmware/v2-candidate", script)
        self.assertIn("refusing dirty repository root", script)
        self.assertIn("4699d27bd5bf7c0b41bbed9ddb6b0b7d019e215f", script)
        self.assertIn("baseline changed during safeguard", core)

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
        self.assertIn("ExpectedBranch", completed.stderr)

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
            shutil.copy2(
                REPOSITORY_ROOT / "tools" / "CandidateSafeguard.Core.psm1",
                same_suffix_root / "tools" / "CandidateSafeguard.Core.psm1",
            )
            completed = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(copied_script),
                    "-ExpectedBranch",
                    "firmware/v2-candidate",
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


class CandidateSafeguardCheckpointCoreTests(unittest.TestCase):
    def _repository_fixture(
        self,
    ) -> tuple[
        tempfile.TemporaryDirectory[str], Path, Path, Path, TemporaryGitRepository
    ]:
        temporary = tempfile.TemporaryDirectory(prefix="orchestrator-csg-checkpoint-")
        root = Path(temporary.name) / "candidate"
        repository = TemporaryGitRepository.create(
            root, branch="firmware/v2-candidate"
        )
        baseline = root / ".codex" / "dev" / "basedpyright-baseline.json"
        baseline.parent.mkdir(parents=True)
        baseline.write_text("{}\n", encoding="utf-8")
        pyright_config = root / "pyrightconfig.json"
        pyright_config.write_text(
            '{"baselineFile": ".codex/dev/basedpyright-baseline.json"}\n',
            encoding="utf-8",
        )
        repository.git("add", ".")
        repository.git("commit", "-m", "candidate core fixture")
        return temporary, root, baseline, pyright_config, repository

    def _selection(
        self,
        repository: TemporaryGitRepository,
        root: Path,
        selected: list[dict[str, object]],
        preserved: list[dict[str, object]],
    ) -> dict[str, object]:
        return {
            "schema": "orchestrator-check-selection/v1",
            "source": {
                "source_root": str(root.resolve()),
                "git_common_dir": str(repository.common_dir),
                "branch": "firmware/v2-candidate",
                "tip": repository.head,
            },
            "selected": selected,
            "preserved_credit_records": preserved,
        }

    def test_core_records_fail_and_continues_later_independent_units(self) -> None:
        temporary, root, baseline, pyright_config, repository = (
            self._repository_fixture()
        )
        try:
            failing = root / "failing.ps1"
            passing = root / "passing.ps1"
            failing.write_text("exit 3\n", encoding="utf-8")
            passing.write_text(
                f"Set-Content -LiteralPath '{Path(temporary.name) / 'second-ran.txt'}' -Value 'second'\n",
                encoding="utf-8",
            )
            repository.git("add", ".")
            repository.git("commit", "-m", "continuation fixture")
            module = REPOSITORY_ROOT / "tools" / "CandidateSafeguard.Core.psm1"
            driver = Path(temporary.name) / "invoke-core.ps1"
            driver.write_text(
                f"""Import-Module -Force '{module}'
$checks = @(
    [pscustomobject]@{{ stable_id = 'TEST.FAILING'; command = @('powershell', '-NoProfile', '-File', 'failing.ps1') }},
    [pscustomobject]@{{ stable_id = 'TEST.PASSING'; command = @('powershell', '-NoProfile', '-File', 'passing.ps1') }}
)
$summary = Invoke-ReleaseChecks -Checks $checks -RepositoryRoot '{root}' -ExpectedHead '{repository.head}' `
    -ExpectedBranch 'firmware/v2-candidate' -ExpectedCommonDirectory '{repository.common_dir}' `
    -Baseline '{baseline}' -PyrightConfig '{pyright_config}' `
    -BaselineHash '{hashlib.sha256(baseline.read_bytes()).hexdigest().upper()}' -ConfigHash '{hashlib.sha256(pyright_config.read_bytes()).hexdigest().upper()}'
$summary | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath '{Path(temporary.name) / 'summary.json'}' -Encoding UTF8
if ($summary.incomplete) {{
    exit 17
}}
exit 0
""",
                encoding="utf-8",
            )
            completed = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(driver),
                ],
                cwd=root,
                capture_output=True,
                text=True,
                check=False,
                creationflags=_WINDOWLESS_CREATION_FLAGS,
            )
            self.assertEqual(
                17, completed.returncode, completed.stdout + completed.stderr
            )
            # The ordinary failure was recorded, the later independent unit
            # still executed, and the incomplete pool stayed nonzero.
            self.assertTrue((Path(temporary.name) / "second-ran.txt").is_file())
            summary = json.loads(
                (Path(temporary.name) / "summary.json").read_text(encoding="utf-8-sig")
            )
            self.assertEqual(2, summary["total"])
            self.assertEqual(1, summary["failed"])
            self.assertEqual(1, summary["passed"])
            self.assertTrue(summary["incomplete"])
        finally:
            temporary.cleanup()

    def test_core_identity_uncertainty_never_passes_and_persists_checkpoint(
        self,
    ) -> None:
        temporary, root, baseline, pyright_config, repository = (
            self._repository_fixture()
        )
        try:
            first = root / "first.ps1"
            second = root / "second.ps1"
            first.write_text(
                "Set-Content -LiteralPath (Join-Path $PSScriptRoot 'first-ran.txt') -Value 'first'\n",
                encoding="utf-8",
            )
            second.write_text(
                "Set-Content -LiteralPath (Join-Path $PSScriptRoot 'second-ran.txt') -Value 'second'\n",
                encoding="utf-8",
            )
            repository.git("add", ".")
            repository.git("commit", "-m", "identity uncertainty fixture")
            selection = self._selection(
                repository,
                root,
                [
                    {
                        "stable_id": "TEST.FIRST",
                        "tier": "affected",
                        "command": ["powershell", "-NoProfile", "-File", "first.ps1"],
                        "output_contract": "orchestrator-check-credit/v1",
                        "dependency_fingerprint": "1" * 64,
                    },
                    {
                        "stable_id": "TEST.SECOND",
                        "tier": "affected",
                        "command": ["powershell", "-NoProfile", "-File", "second.ps1"],
                        "output_contract": "orchestrator-check-credit/v1",
                        "dependency_fingerprint": "2" * 64,
                    },
                ],
                [],
            )
            selection_path = Path(temporary.name) / "selection.json"
            selection_path.write_text(
                json.dumps(selection), encoding="utf-8"
            )
            checkpoint_path = Path(temporary.name) / "checkpoint.json"
            results_path = Path(temporary.name) / "results.json"
            module = REPOSITORY_ROOT / "tools" / "CandidateSafeguard.Core.psm1"
            driver = Path(temporary.name) / "invoke-core.ps1"
            driver.write_text(
                f"""$env:PYTHONPATH = '{REPOSITORY_ROOT}'
Import-Module -Force '{module}'
$checks = @(
    [pscustomobject]@{{ stable_id = 'TEST.FIRST'; command = @('powershell', '-NoProfile', '-File', 'first.ps1') }},
    [pscustomobject]@{{ stable_id = 'TEST.SECOND'; command = @('powershell', '-NoProfile', '-File', 'second.ps1') }}
)
$selection = Get-Content -LiteralPath '{selection_path}' -Raw | ConvertFrom-Json
$summary = Invoke-ReleaseChecks -Checks $checks -RepositoryRoot '{root}' -ExpectedHead '{repository.head}' `
    -ExpectedBranch 'firmware/v2-candidate' -ExpectedCommonDirectory '{repository.common_dir}' `
    -Baseline '{baseline}' -PyrightConfig '{pyright_config}' `
    -BaselineHash '{hashlib.sha256(baseline.read_bytes()).hexdigest().upper()}' -ConfigHash '{hashlib.sha256(pyright_config.read_bytes()).hexdigest().upper()}' `
    -Selection $selection -CheckpointPath '{checkpoint_path}' -ResultsPath '{results_path}'
$summary | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath '{Path(temporary.name) / 'summary.json'}' -Encoding UTF8
if ($summary.incomplete) {{
    exit 17
}}
exit 0
""",
                encoding="utf-8",
            )
            completed = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(driver),
                ],
                cwd=root,
                capture_output=True,
                text=True,
                check=False,
                creationflags=_WINDOWLESS_CREATION_FLAGS,
            )
            self.assertEqual(
                17, completed.returncode, completed.stdout + completed.stderr
            )
            self.assertTrue((root / "first-ran.txt").is_file())
            self.assertFalse((root / "second-ran.txt").exists())
            summary = json.loads(
                (Path(temporary.name) / "summary.json").read_text(encoding="utf-8-sig")
            )
            self.assertEqual(1, summary["unresolved"])
            self.assertEqual(1, summary["skipped"])
            self.assertEqual("TEST.FIRST", summary["first_unresolved_unit"])
            checkpoint = json.loads(
                checkpoint_path.read_text(encoding="utf-8-sig")
            )
            self.assertEqual("orchestrator-checkpoint/v1", checkpoint["schema"])
            self.assertEqual([], checkpoint["credits"])
            by_id = {item["stable_id"]: item for item in checkpoint["dispositions"]}
            self.assertEqual("UNRESOLVED", by_id["TEST.FIRST"]["status"])
            self.assertIn("identity", by_id["TEST.FIRST"]["reason"].lower())
            self.assertEqual("SKIP", by_id["TEST.SECOND"]["status"])
            self.assertIn("holds", by_id["TEST.SECOND"]["reason"].lower())
            self.assertEqual("TEST.FIRST", checkpoint["first_unresolved_unit"])
        finally:
            temporary.cleanup()

    def test_core_persists_preserved_pass_credit_with_new_pass(self) -> None:
        temporary, root, baseline, pyright_config, repository = (
            self._repository_fixture()
        )
        try:
            probe = root / "probe.py"
            probe.write_text(
                "from __future__ import annotations\n"
                "import sys\n"
                "EXPECTED = 'probe.py'\n"
                "assert len(sys.argv) == 1 and sys.argv[0] == EXPECTED, sys.argv\n"
                "print('PROBE-OK')\n",
                encoding="utf-8",
            )
            repository.git("add", ".")
            repository.git("commit", "-m", "preserved credit fixture")
            source = {
                "source_root": str(root.resolve()),
                "git_common_dir": str(repository.common_dir),
                "branch": "firmware/v2-candidate",
                "tip": repository.head,
            }
            credit_a = {
                "schema": "orchestrator-check-credit/v1",
                "stable_id": "TEST.PRESERVED",
                "status": "PASS",
                "outcome": "PASS",
                "tier": "affected",
                "command": ["python", "-c", "pass"],
                "output_contract": "orchestrator-check-credit/v1",
                "dependency_fingerprint": "a" * 64,
                "source": source,
                "runner": release_checks.runner_coordinate(),
                "observed_utc": "2026-08-15T00:00:00Z",
            }
            selection = self._selection(
                repository,
                root,
                [
                    {
                        "stable_id": "TEST.NEW",
                        "tier": "affected",
                        "command": ["python", "probe.py"],
                        "output_contract": "orchestrator-check-credit/v1",
                        "dependency_fingerprint": "b" * 64,
                    },
                ],
                [credit_a],
            )
            selection_path = Path(temporary.name) / "selection.json"
            selection_path.write_text(
                json.dumps(selection), encoding="utf-8"
            )
            checkpoint_path = Path(temporary.name) / "checkpoint.json"
            checkpoint_path.write_text(
                json.dumps({"credits": [credit_a], "dispositions": []}),
                encoding="utf-8",
            )
            results_path = Path(temporary.name) / "results.json"
            module = REPOSITORY_ROOT / "tools" / "CandidateSafeguard.Core.psm1"
            driver = Path(temporary.name) / "invoke-core.ps1"
            driver.write_text(
                f"""$env:PYTHONPATH = '{REPOSITORY_ROOT}'
Import-Module -Force '{module}'
$checks = @(
    [pscustomobject]@{{ stable_id = 'TEST.NEW'; command = @('python', 'probe.py') }}
)
$selection = Get-Content -LiteralPath '{selection_path}' -Raw | ConvertFrom-Json
$summary = Invoke-ReleaseChecks -Checks $checks -RepositoryRoot '{root}' -ExpectedHead '{repository.head}' `
    -ExpectedBranch 'firmware/v2-candidate' -ExpectedCommonDirectory '{repository.common_dir}' `
    -Baseline '{baseline}' -PyrightConfig '{pyright_config}' `
    -BaselineHash '{hashlib.sha256(baseline.read_bytes()).hexdigest().upper()}' -ConfigHash '{hashlib.sha256(pyright_config.read_bytes()).hexdigest().upper()}' `
    -Selection $selection -CheckpointPath '{checkpoint_path}' -ResultsPath '{results_path}'
$summary | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath '{Path(temporary.name) / 'summary.json'}' -Encoding UTF8
if ($summary.incomplete) {{
    exit 17
}}
exit 0
""",
                encoding="utf-8",
            )
            completed = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(driver),
                ],
                cwd=root,
                capture_output=True,
                text=True,
                check=False,
                creationflags=_WINDOWLESS_CREATION_FLAGS,
            )
            self.assertEqual(
                0, completed.returncode, completed.stdout + completed.stderr
            )
            self.assertIn("PROBE-OK", completed.stdout)
            checkpoint = json.loads(
                checkpoint_path.read_text(encoding="utf-8-sig")
            )
            self.assertEqual("orchestrator-checkpoint/v1", checkpoint["schema"])
            credits_by_id = {
                item["stable_id"]: item for item in checkpoint["credits"]
            }
            self.assertIn("TEST.PRESERVED", credits_by_id)
            self.assertEqual("PASS", credits_by_id["TEST.PRESERVED"]["status"])
            self.assertIn("TEST.NEW", credits_by_id)
            self.assertEqual("PASS", credits_by_id["TEST.NEW"]["status"])
            self.assertEqual(
                release_checks.runner_coordinate(),
                credits_by_id["TEST.NEW"]["runner"],
            )
            self.assertEqual([], checkpoint["dispositions"])
            self.assertIsNone(checkpoint["first_unresolved_unit"])
        finally:
            temporary.cleanup()

    def test_core_persists_checkpoint_after_each_unit_before_later_unit_completes(
        self,
    ) -> None:
        temporary, root, baseline, pyright_config, repository = (
            self._repository_fixture()
        )
        try:
            first = root / "first.ps1"
            slow = root / "slow.ps1"
            first.write_text("Write-Output 'FIRST-DIAG'\n", encoding="utf-8")
            slow.write_text(
                f"Set-Content -LiteralPath '{Path(temporary.name) / 'second-started.txt'}' -Value 'started'\n"
                "Start-Sleep -Seconds 6\n",
                encoding="utf-8",
            )
            repository.git("add", ".")
            repository.git("commit", "-m", "per-unit persistence fixture")
            selection = self._selection(
                repository,
                root,
                [
                    {
                        "stable_id": "TEST.FIRST",
                        "tier": "affected",
                        "command": ["powershell", "-NoProfile", "-File", "first.ps1"],
                        "output_contract": "orchestrator-check-credit/v1",
                        "dependency_fingerprint": "1" * 64,
                    },
                    {
                        "stable_id": "TEST.SLOW",
                        "tier": "affected",
                        "command": ["powershell", "-NoProfile", "-File", "slow.ps1"],
                        "output_contract": "orchestrator-check-credit/v1",
                        "dependency_fingerprint": "2" * 64,
                    },
                ],
                [],
            )
            selection_path = Path(temporary.name) / "selection.json"
            selection_path.write_text(json.dumps(selection), encoding="utf-8")
            # The checkpoint path does not exist yet: this is the cold input
            # case, and the run must create it after the first disposition.
            checkpoint_path = Path(temporary.name) / "checkpoint.json"
            results_path = Path(temporary.name) / "results.json"
            module = REPOSITORY_ROOT / "tools" / "CandidateSafeguard.Core.psm1"
            driver = Path(temporary.name) / "invoke-core.ps1"
            driver.write_text(
                f"""$env:PYTHONPATH = '{REPOSITORY_ROOT}'
Import-Module -Force '{module}'
$checks = @(
    [pscustomobject]@{{ stable_id = 'TEST.FIRST'; command = @('powershell', '-NoProfile', '-File', 'first.ps1') }},
    [pscustomobject]@{{ stable_id = 'TEST.SLOW'; command = @('powershell', '-NoProfile', '-File', 'slow.ps1') }}
)
$selection = Get-Content -LiteralPath '{selection_path}' -Raw | ConvertFrom-Json
$summary = Invoke-ReleaseChecks -Checks $checks -RepositoryRoot '{root}' -ExpectedHead '{repository.head}' `
    -ExpectedBranch 'firmware/v2-candidate' -ExpectedCommonDirectory '{repository.common_dir}' `
    -Baseline '{baseline}' -PyrightConfig '{pyright_config}' `
    -BaselineHash '{hashlib.sha256(baseline.read_bytes()).hexdigest().upper()}' -ConfigHash '{hashlib.sha256(pyright_config.read_bytes()).hexdigest().upper()}' `
    -Selection $selection -CheckpointPath '{checkpoint_path}' -ResultsPath '{results_path}'
$summary | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath '{Path(temporary.name) / 'summary.json'}' -Encoding UTF8
if ($summary.incomplete) {{
    exit 17
}}
exit 0
""",
                encoding="utf-8",
            )
            process = subprocess.Popen(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(driver),
                ],
                cwd=root,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                creationflags=_WINDOWLESS_CREATION_FLAGS,
            )
            try:
                started = Path(temporary.name) / "second-started.txt"
                deadline = time.monotonic() + 20
                while not started.is_file() and time.monotonic() < deadline:
                    if process.poll() is not None:
                        break
                    time.sleep(0.1)
                # The later unit is in flight; the earlier unit's truthful
                # PASS disposition must already be persisted, and the cold
                # checkpoint path must now exist.
                self.assertTrue(started.is_file())
                self.assertTrue(checkpoint_path.is_file())
                mid = json.loads(checkpoint_path.read_text(encoding="utf-8-sig"))
                self.assertEqual("orchestrator-checkpoint/v1", mid["schema"])
                mid_ids = {item["stable_id"] for item in mid["credits"]}
                self.assertIn("TEST.FIRST", mid_ids)
                self.assertNotIn("TEST.SLOW", mid_ids)
            finally:
                stdout, stderr = process.communicate(timeout=40)
            self.assertEqual(0, process.returncode, stdout + stderr)
            self.assertIn("FIRST-DIAG", stdout)
            final = json.loads(checkpoint_path.read_text(encoding="utf-8-sig"))
            final_ids = {item["stable_id"] for item in final["credits"]}
            self.assertIn("TEST.FIRST", final_ids)
            self.assertIn("TEST.SLOW", final_ids)
            self.assertEqual([], final["dispositions"])
        finally:
            temporary.cleanup()

    def test_core_throw_during_later_unit_retains_earlier_checkpoint(self) -> None:
        temporary, root, baseline, pyright_config, repository = (
            self._repository_fixture()
        )
        try:
            first = root / "first.ps1"
            first.write_text(
                f"Set-Content -LiteralPath '{Path(temporary.name) / 'first-ran.txt'}' -Value 'first'\n",
                encoding="utf-8",
            )
            repository.git("add", ".")
            repository.git("commit", "-m", "throw persistence fixture")
            selection = self._selection(
                repository,
                root,
                [
                    {
                        "stable_id": "TEST.FIRST",
                        "tier": "affected",
                        "command": ["powershell", "-NoProfile", "-File", "first.ps1"],
                        "output_contract": "orchestrator-check-credit/v1",
                        "dependency_fingerprint": "1" * 64,
                    },
                    {
                        "stable_id": "TEST.UNSUPPORTED",
                        "tier": "affected",
                        "command": ["cmd", "/c", "exit 0"],
                        "output_contract": "orchestrator-check-credit/v1",
                        "dependency_fingerprint": "2" * 64,
                    },
                ],
                [],
            )
            selection_path = Path(temporary.name) / "selection.json"
            selection_path.write_text(json.dumps(selection), encoding="utf-8")
            checkpoint_path = Path(temporary.name) / "checkpoint.json"
            results_path = Path(temporary.name) / "results.json"
            module = REPOSITORY_ROOT / "tools" / "CandidateSafeguard.Core.psm1"
            driver = Path(temporary.name) / "invoke-core.ps1"
            driver.write_text(
                f"""$env:PYTHONPATH = '{REPOSITORY_ROOT}'
Import-Module -Force '{module}'
$checks = @(
    [pscustomobject]@{{ stable_id = 'TEST.FIRST'; command = @('powershell', '-NoProfile', '-File', 'first.ps1') }},
    [pscustomobject]@{{ stable_id = 'TEST.UNSUPPORTED'; command = @('cmd', '/c', 'exit 0') }}
)
$selection = Get-Content -LiteralPath '{selection_path}' -Raw | ConvertFrom-Json
Invoke-ReleaseChecks -Checks $checks -RepositoryRoot '{root}' -ExpectedHead '{repository.head}' `
    -ExpectedBranch 'firmware/v2-candidate' -ExpectedCommonDirectory '{repository.common_dir}' `
    -Baseline '{baseline}' -PyrightConfig '{pyright_config}' `
    -BaselineHash '{hashlib.sha256(baseline.read_bytes()).hexdigest().upper()}' -ConfigHash '{hashlib.sha256(pyright_config.read_bytes()).hexdigest().upper()}' `
    -Selection $selection -CheckpointPath '{checkpoint_path}' -ResultsPath '{results_path}'
exit 0
""",
                encoding="utf-8",
            )
            completed = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(driver),
                ],
                cwd=root,
                capture_output=True,
                text=True,
                check=False,
                creationflags=_WINDOWLESS_CREATION_FLAGS,
            )
            # The unsupported-runner throw interrupted the pool during the
            # later unit, but the earlier unit's PASS was already persisted.
            self.assertNotEqual(0, completed.returncode)
            self.assertTrue((Path(temporary.name) / "first-ran.txt").is_file())
            self.assertTrue(checkpoint_path.is_file())
            checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8-sig"))
            self.assertEqual("orchestrator-checkpoint/v1", checkpoint["schema"])
            credit_ids = {item["stable_id"] for item in checkpoint["credits"]}
            self.assertIn("TEST.FIRST", credit_ids)
            self.assertNotIn("TEST.UNSUPPORTED", credit_ids)
            self.assertEqual([], checkpoint["dispositions"])
        finally:
            temporary.cleanup()

    def test_core_failure_plus_identity_uncertainty_records_first_unresolved_and_drops_preserved(
        self,
    ) -> None:
        temporary, root, baseline, pyright_config, repository = (
            self._repository_fixture()
        )
        try:
            first_pass = root / "first-pass.ps1"
            fail_dirty = root / "fail-dirty.ps1"
            second = root / "second.ps1"
            first_pass.write_text(
                f"Set-Content -LiteralPath '{Path(temporary.name) / 'first-pass-ran.txt'}' -Value 'first-pass'\n",
                encoding="utf-8",
            )
            fail_dirty.write_text(
                "Set-Content -LiteralPath (Join-Path $PSScriptRoot 'dirty.txt') -Value 'dirty'\n"
                "exit 3\n",
                encoding="utf-8",
            )
            second.write_text(
                f"Set-Content -LiteralPath '{Path(temporary.name) / 'second-ran.txt'}' -Value 'second'\n",
                encoding="utf-8",
            )
            repository.git("add", ".")
            repository.git("commit", "-m", "failure identity uncertainty fixture")
            source = {
                "source_root": str(root.resolve()),
                "git_common_dir": str(repository.common_dir),
                "branch": "firmware/v2-candidate",
                "tip": repository.head,
            }
            credit_a = {
                "schema": "orchestrator-check-credit/v1",
                "stable_id": "TEST.PRESERVED",
                "status": "PASS",
                "outcome": "PASS",
                "tier": "affected",
                "command": ["python", "-c", "pass"],
                "output_contract": "orchestrator-check-credit/v1",
                "dependency_fingerprint": "a" * 64,
                "source": source,
                "runner": release_checks.runner_coordinate(),
                "observed_utc": "2026-08-15T00:00:00Z",
            }
            selection = self._selection(
                repository,
                root,
                [
                    {
                        "stable_id": "TEST.PASSED",
                        "tier": "affected",
                        "command": ["powershell", "-NoProfile", "-File", "first-pass.ps1"],
                        "output_contract": "orchestrator-check-credit/v1",
                        "dependency_fingerprint": "0" * 64,
                    },
                    {
                        "stable_id": "TEST.FAILING",
                        "tier": "affected",
                        "command": ["powershell", "-NoProfile", "-File", "fail-dirty.ps1"],
                        "output_contract": "orchestrator-check-credit/v1",
                        "dependency_fingerprint": "1" * 64,
                    },
                    {
                        "stable_id": "TEST.SECOND",
                        "tier": "affected",
                        "command": ["powershell", "-NoProfile", "-File", "second.ps1"],
                        "output_contract": "orchestrator-check-credit/v1",
                        "dependency_fingerprint": "2" * 64,
                    },
                ],
                [credit_a],
            )
            selection_path = Path(temporary.name) / "selection.json"
            selection_path.write_text(json.dumps(selection), encoding="utf-8")
            checkpoint_path = Path(temporary.name) / "checkpoint.json"
            checkpoint_path.write_text(
                json.dumps({"credits": [credit_a], "dispositions": []}),
                encoding="utf-8",
            )
            results_path = Path(temporary.name) / "results.json"
            module = REPOSITORY_ROOT / "tools" / "CandidateSafeguard.Core.psm1"
            driver = Path(temporary.name) / "invoke-core.ps1"
            driver.write_text(
                f"""$env:PYTHONPATH = '{REPOSITORY_ROOT}'
Import-Module -Force '{module}'
$checks = @(
    [pscustomobject]@{{ stable_id = 'TEST.PASSED'; command = @('powershell', '-NoProfile', '-File', 'first-pass.ps1') }},
    [pscustomobject]@{{ stable_id = 'TEST.FAILING'; command = @('powershell', '-NoProfile', '-File', 'fail-dirty.ps1') }},
    [pscustomobject]@{{ stable_id = 'TEST.SECOND'; command = @('powershell', '-NoProfile', '-File', 'second.ps1') }}
)
$selection = Get-Content -LiteralPath '{selection_path}' -Raw | ConvertFrom-Json
$summary = Invoke-ReleaseChecks -Checks $checks -RepositoryRoot '{root}' -ExpectedHead '{repository.head}' `
    -ExpectedBranch 'firmware/v2-candidate' -ExpectedCommonDirectory '{repository.common_dir}' `
    -Baseline '{baseline}' -PyrightConfig '{pyright_config}' `
    -BaselineHash '{hashlib.sha256(baseline.read_bytes()).hexdigest().upper()}' -ConfigHash '{hashlib.sha256(pyright_config.read_bytes()).hexdigest().upper()}' `
    -Selection $selection -CheckpointPath '{checkpoint_path}' -ResultsPath '{results_path}'
$summary | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath '{Path(temporary.name) / 'summary.json'}' -Encoding UTF8
if ($summary.incomplete) {{
    exit 17
}}
exit 0
""",
                encoding="utf-8",
            )
            completed = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(driver),
                ],
                cwd=root,
                capture_output=True,
                text=True,
                check=False,
                creationflags=_WINDOWLESS_CREATION_FLAGS,
            )
            self.assertEqual(
                17, completed.returncode, completed.stdout + completed.stderr
            )
            self.assertFalse((Path(temporary.name) / "second-ran.txt").exists())
            summary = json.loads(
                (Path(temporary.name) / "summary.json").read_text(encoding="utf-8-sig")
            )
            self.assertTrue(
                (Path(temporary.name) / "first-pass-ran.txt").is_file()
            )
            self.assertEqual(3, summary["total"])
            self.assertEqual(0, summary["passed"])
            self.assertEqual(2, summary["unresolved"])
            self.assertEqual(1, summary["skipped"])
            self.assertEqual("TEST.PASSED", summary["first_unresolved_unit"])
            checkpoint = json.loads(
                checkpoint_path.read_text(encoding="utf-8-sig")
            )
            self.assertEqual("orchestrator-checkpoint/v1", checkpoint["schema"])
            # Identity uncertainty after the ordinary failure must never
            # preserve an earlier current-run PASS as trusted: the first
            # identity-uncertain checkpoint has already converted it to
            # UNRESOLVED with a reason and set the earliest unresolved unit.
            self.assertEqual([], checkpoint["credits"])
            by_id = {item["stable_id"]: item for item in checkpoint["dispositions"]}
            self.assertEqual("UNRESOLVED", by_id["TEST.PASSED"]["status"])
            self.assertIn("identity", by_id["TEST.PASSED"]["reason"].lower())
            self.assertIn(
                "invalidates this run's PASS", by_id["TEST.PASSED"]["reason"]
            )
            self.assertEqual("UNRESOLVED", by_id["TEST.FAILING"]["status"])
            self.assertIn(
                "ordinary nonzero exit code 3", by_id["TEST.FAILING"]["reason"]
            )
            self.assertIn("identity", by_id["TEST.FAILING"]["reason"].lower())
            self.assertEqual("SKIP", by_id["TEST.SECOND"]["status"])
            self.assertEqual("TEST.PASSED", checkpoint["first_unresolved_unit"])
        finally:
            temporary.cleanup()


if __name__ == "__main__":
    unittest.main()
