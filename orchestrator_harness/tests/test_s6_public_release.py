from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from examples.disposable_coding_fixture import run_fixture
from orchestrator_harness import release_checks
from orchestrator_harness.lane_lifecycle import lifecycle_registry_path
from orchestrator_harness.models import iso_utc
from orchestrator_harness.processes import process_snapshot
from orchestrator_harness.release_assets import read_package_asset, release_manifest
from orchestrator_harness.tests.support import TemporaryGitRepository

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


class S6SelectorTests(unittest.TestCase):
    def _custom_specs(self) -> tuple[release_checks.CheckSpec, ...]:
        return (
            release_checks.CheckSpec(
                "TEST.DECISIVE-SHORT",
                "short decisive",
                "affected",
                ("python", "-c", "pass"),
                ("domain-a",),
                ("tracked.txt",),
                estimated_duration_seconds=1.0,
                decisive=True,
            ),
            release_checks.CheckSpec(
                "TEST.DECISIVE-LONG",
                "long decisive",
                "affected",
                ("python", "-c", "pass"),
                ("domain-b",),
                ("other.txt",),
                estimated_duration_seconds=5.0,
                decisive=True,
            ),
            release_checks.CheckSpec(
                "TEST.NONDECISIVE",
                "nondecisive",
                "affected",
                ("python", "-c", "pass"),
                ("domain-c",),
                ("third.txt",),
                estimated_duration_seconds=0.1,
            ),
        )

    def _repository(
        self,
    ) -> tuple[tempfile.TemporaryDirectory[str], TemporaryGitRepository]:
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        repository = TemporaryGitRepository.create(root, branch="test")
        for name in ("other.txt", "third.txt"):
            (root / name).write_text(f"{name}\n", encoding="utf-8")
        repository.git("add", "other.txt", "third.txt")
        repository.git("commit", "-m", "declared dependencies")
        return temporary, repository

    def test_registry_has_one_stable_owner_and_tier_does_not_leak_real_agent_into_fast(
        self,
    ) -> None:
        registry = release_checks.registry()
        self.assertEqual(len({spec.stable_id for spec in registry}), len(registry))
        fast = release_checks.select_checks("fast", REPOSITORY_ROOT)
        self.assertTrue(fast.selected)
        fast_ids = {item.spec.stable_id for item in fast.selected}
        self.assertNotIn("S6.AFFECTED.REAL-AGENT", fast_ids)
        self.assertNotIn(release_checks.RELEASE_AGGREGATE_ID, fast_ids)
        full = release_checks.select_checks("full", REPOSITORY_ROOT)
        full_ids = {item.spec.stable_id for item in full.selected}
        self.assertIn("S6.AFFECTED.REAL-AGENT", full_ids)
        self.assertIn(release_checks.RELEASE_AGGREGATE_ID, full_ids)
        release = release_checks.select_checks("release", REPOSITORY_ROOT)
        self.assertIn(
            release_checks.RELEASE_AGGREGATE_ID,
            {item.spec.stable_id for item in release.selected},
        )

    def test_dependency_mutation_invalidates_only_consumers_and_preserves_unrelated_credit(
        self,
    ) -> None:
        temporary, repository = self._repository()
        try:
            specs = self._custom_specs()
            with patch.object(release_checks, "CHECK_REGISTRY", specs):
                credits = [
                    release_checks.credit_record(spec, repository.root)
                    for spec in specs
                ]
                (repository.root / "tracked.txt").write_text(
                    "changed\n", encoding="utf-8"
                )
                decision = release_checks.select_checks(
                    "affected",
                    repository.root,
                    credits=credits,
                    changed_paths=["tracked.txt"],
                )
            self.assertEqual(("TEST.DECISIVE-SHORT",), decision.selected_ids)
            self.assertIn("TEST.DECISIVE-SHORT", decision.invalidated_credit_ids)
            self.assertIn("TEST.DECISIVE-LONG", decision.preserved_credit_ids)
            self.assertIn("TEST.NONDECISIVE", decision.preserved_credit_ids)
        finally:
            temporary.cleanup()

    def test_shortest_decisive_invalidated_checks_are_first(self) -> None:
        temporary, repository = self._repository()
        try:
            specs = self._custom_specs()
            with patch.object(release_checks, "CHECK_REGISTRY", specs):
                decision = release_checks.select_checks(
                    "affected",
                    repository.root,
                    changed_paths=["tracked.txt", "other.txt", "third.txt"],
                )
            self.assertEqual(
                ("TEST.DECISIVE-SHORT", "TEST.DECISIVE-LONG", "TEST.NONDECISIVE"),
                decision.selected_ids,
            )
        finally:
            temporary.cleanup()

    def test_wrong_root_branch_tip_and_malformed_or_unknown_credit_fail_closed(
        self,
    ) -> None:
        temporary, repository = self._repository()
        other_temporary, other_repository = self._repository()
        try:
            spec = self._custom_specs()[0]
            with patch.object(release_checks, "CHECK_REGISTRY", (spec,)):
                credit = release_checks.credit_record(spec, repository.root)
                with self.assertRaisesRegex(
                    release_checks.SelectionError, "branch mismatch"
                ):
                    release_checks.select_checks(
                        "affected", repository.root, expected_branch="wrong"
                    )
                with self.assertRaisesRegex(
                    release_checks.SelectionError, "tip mismatch"
                ):
                    release_checks.select_checks(
                        "affected", repository.root, expected_tip="0" * 40
                    )
                mixed = release_checks.select_checks(
                    "affected",
                    other_repository.root,
                    credits=[credit],
                    changed_paths=["tracked.txt"],
                )
                self.assertEqual((spec.stable_id,), mixed.selected_ids)
                self.assertIn(spec.stable_id, mixed.invalidated_credit_ids)
                dishonest = release_checks.select_checks(
                    "affected",
                    repository.root,
                    credits=[
                        credit,
                        {"stable_id": "UNKNOWN"},
                        {"stable_id": spec.stable_id},
                    ],
                    changed_paths=["tracked.txt"],
                )
                self.assertIn("UNKNOWN", dishonest.rejected_credit_ids)
                self.assertIn(spec.stable_id, dishonest.rejected_credit_ids)
                self.assertEqual((spec.stable_id,), dishonest.selected_ids)
        finally:
            temporary.cleanup()
            other_temporary.cleanup()


class S6PublicJourneyTests(unittest.TestCase):
    def test_disposable_fake_agent_uses_public_operator_controller_path_and_cleans_exact_runs(
        self,
    ) -> None:
        root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-public-"))
        try:
            result = run_fixture(root)
            self.assertEqual("PASS", result["python_tests"])
            self.assertEqual(0, result["resource_claims_remaining"])
            event_lines = (
                (root / "runtime" / "LANE_EVENTS.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            )
            self.assertTrue(
                any(
                    json.loads(line)["event"] == "CODEX_STARTED" for line in event_lines
                )
            )
            self.assertTrue(
                any(json.loads(line)["event"] == "CODEX_EXITED" for line in event_lines)
            )
            for lane in ("alpha", "beta", "merge"):
                workspace = root / "worktrees" / lane / ".agent-workspace"
                receipt = json.loads(
                    (workspace / "fixture_operator_launch.json").read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual(
                    ["-m", "orchestrator_harness.lane_controller"], receipt["argv"][1:3]
                )
                status = json.loads(
                    (workspace / "fixture_controller.status.json").read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual("CODEX_EXITED", status["state"])
                self.assertEqual(lane, status["declared_lane_id"])
                self.assertEqual([], status["held_resource_claims"])
                registry_path = lifecycle_registry_path(
                    root / "worktrees" / lane, lane, f"{lane}-001"
                )
                registry = json.loads(registry_path.read_text(encoding="utf-8"))
                self.assertTrue(registry["lifecycle"]["complete"])
                self.assertTrue(registry["lifecycle"]["helpers_complete"])
                process = process_snapshot().by_pid.get(receipt["pid"])
                self.assertTrue(
                    process is None
                    or iso_utc(process.created_utc) != receipt["created_utc"]
                )
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_source_does_not_add_a_test_only_controller_launcher(self) -> None:
        source = (
            REPOSITORY_ROOT / "examples" / "disposable_coding_fixture.py"
        ).read_text(encoding="utf-8")
        start = source.index("def _start_controller")
        finish = source.index("def _wait_for_status", start)
        section = source[start:finish]
        self.assertIn("launch_lane_controller", section)
        self.assertNotIn("subprocess.Popen", section)


class S6SafeguardTests(unittest.TestCase):
    def test_safeguard_is_portable_exact_root_bound_and_selector_owned(self) -> None:
        script = (
            REPOSITORY_ROOT / "tools" / "Invoke-CandidateSafeguard.ps1"
        ).read_text(encoding="utf-8")
        self.assertNotIn("C:/Users/", script)
        self.assertIn("RepositoryRoot", script)
        self.assertIn("ExpectedTip", script)
        self.assertIn("orchestrator_harness.release_checks", script)
        self.assertIn("--root", script)
        self.assertIn("--expected-tip", script)
        self.assertIn("S6.RELEASE.ACCUMULATED-SAFEGUARD", script)
        self.assertNotIn("@{ Name = 'ruff'", script)

    def test_non_candidate_branch_is_rejected_before_selector_or_checks(self) -> None:
        script = REPOSITORY_ROOT / "tools" / "Invoke-CandidateSafeguard.ps1"
        completed = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(script),
                "-RepositoryRoot",
                str(REPOSITORY_ROOT),
            ],
            cwd=REPOSITORY_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(0, completed.returncode)
        self.assertIn("unexpected branch", completed.stderr)


class S6DocumentationTests(unittest.TestCase):
    def test_current_examples_and_docs_name_the_real_public_apis(self) -> None:
        launch_doc = (
            REPOSITORY_ROOT / "examples" / "public-coding-launch.example.md"
        ).read_text(encoding="utf-8")
        selection = json.loads(
            (REPOSITORY_ROOT / "examples" / "release-selection.example.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual("orchestrator-check-selection-request/v1", selection["schema"])
        self.assertIn("operator_launch", launch_doc)
        self.assertIn("lane_controller", launch_doc)
        self.assertIn("watch --until-actionable", launch_doc)
        self.assertIn("top-level `event_id`", launch_doc)
        for document in (
            REPOSITORY_ROOT / "README.md",
            REPOSITORY_ROOT / "QUICK_START.md",
            REPOSITORY_ROOT / "orchestrator_harness" / "README.md",
        ):
            text = document.read_text(encoding="utf-8")
            self.assertIn("release_checks", text)
            self.assertIn("exact source", text)
            self.assertNotIn("C:/Users/", text)


class S6PackageTests(unittest.TestCase):
    def test_package_manifest_and_metadata_declare_shipped_release_surface(
        self,
    ) -> None:
        manifest = release_manifest()
        self.assertEqual(
            "orchestrator_harness.release_checks", manifest["registry_module"]
        )
        self.assertIn("orchestrator_harness.public_launch", manifest["public_modules"])
        pyproject = (
            REPOSITORY_ROOT / "orchestrator_harness" / "pyproject.toml"
        ).read_text(encoding="utf-8")
        self.assertIn("assets/release/*.json", pyproject)
        self.assertIn("release_evidence_templates", pyproject)
        for relative_path in (
            manifest["examples"] + manifest["release_evidence_templates"]
        ):
            self.assertTrue((REPOSITORY_ROOT / relative_path).is_file(), relative_path)
        for relative_path in manifest["package_assets"]:
            self.assertTrue(read_package_asset(relative_path), relative_path)
        self.assertTrue(
            (REPOSITORY_ROOT / "orchestrator_harness" / "release_checks.py").is_file()
        )
        self.assertTrue(
            (REPOSITORY_ROOT / "orchestrator_harness" / "public_launch.py").is_file()
        )


if __name__ == "__main__":
    unittest.main()
