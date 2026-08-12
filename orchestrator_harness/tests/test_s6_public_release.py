from __future__ import annotations

import json
import hashlib
import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from examples.disposable_coding_fixture import run_fixture
from orchestrator_harness import release_checks
from orchestrator_harness.lane_lifecycle import lifecycle_registry_path
from orchestrator_harness.models import iso_utc
from orchestrator_harness.processes import process_snapshot
from orchestrator_harness.release_assets import (
    manifest_asset_paths,
    read_package_asset,
    release_manifest,
)
from orchestrator_harness.tests.support import TemporaryGitRepository
from orchestrator_harness.tests.wsl_identity import (
    provider_identity_matches,
    validate_codex_identity,
)

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

    def test_credit_survives_same_root_descendant_and_rejects_divergence(self) -> None:
        temporary, repository = self._repository()
        try:
            spec = self._custom_specs()[0]
            with patch.object(release_checks, "CHECK_REGISTRY", (spec,)):
                origin = release_checks.credit_record(spec, repository.root)
                (repository.root / "unrelated.txt").write_text("descendant-1\n", encoding="utf-8")
                repository.git("add", "unrelated.txt")
                repository.git("commit", "-m", "unrelated descendant one")
                (repository.root / "unrelated-two.txt").write_text("descendant-2\n", encoding="utf-8")
                repository.git("add", "unrelated-two.txt")
                repository.git("commit", "-m", "unrelated descendant two")
                preserved = release_checks.select_checks(
                    "affected",
                    repository.root,
                    credits=[origin],
                    changed_paths=["unrelated.txt", "unrelated-two.txt"],
                )
                self.assertEqual((), preserved.selected_ids)
                self.assertEqual((spec.stable_id,), preserved.preserved_credit_ids)

                (repository.root / "tracked.txt").write_text("changed dependency\n", encoding="utf-8")
                repository.git("add", "tracked.txt")
                repository.git("commit", "-m", "dependency mutation")
                changed = release_checks.select_checks(
                    "affected",
                    repository.root,
                    credits=[origin],
                    changed_paths=["tracked.txt"],
                )
                self.assertEqual((spec.stable_id,), changed.selected_ids)
                self.assertIn(spec.stable_id, changed.invalidated_credit_ids)

                repository.git("checkout", "--orphan", "divergent")
                repository.git("rm", "-rf", ".")
                (repository.root / "tracked.txt").write_text("divergent dependency\n", encoding="utf-8")
                (repository.root / "divergent.txt").write_text("divergent\n", encoding="utf-8")
                repository.git("add", "divergent.txt")
                repository.git("commit", "-m", "divergent identity")
                repository.git("branch", "-D", "test")
                repository.git("branch", "-m", "test")
                divergent = release_checks.select_checks(
                    "affected", repository.root, credits=[origin], changed_paths=None
                )
                self.assertEqual((spec.stable_id,), divergent.selected_ids)
                self.assertIn(spec.stable_id, divergent.invalidated_credit_ids)
        finally:
            temporary.cleanup()

    def test_release_scope_mutations_invalidate_the_broad_component_credit(self) -> None:
        broad_specs = tuple(
            spec
            for spec in release_checks.registry()
            if spec.stable_id.startswith("S6.RELEASE.")
            and spec.stable_id != release_checks.RELEASE_AGGREGATE_ID
        )
        self.assertEqual(8, len(broad_specs))
        for spec in broad_specs:
            with self.subTest(stable_id=spec.stable_id):
                scoped = release_checks.resolve_input_scope(spec, REPOSITORY_ROOT)
                candidates = tuple(
                    path for path in scoped if path not in spec.dependency_paths
                )
                self.assertTrue(candidates, spec.stable_id)
                relative = candidates[0]
                path = REPOSITORY_ROOT / Path(relative)
                original = path.read_bytes()
                try:
                    with patch.object(release_checks, "CHECK_REGISTRY", (spec,)):
                        credit = release_checks.credit_record(spec, REPOSITORY_ROOT)
                        path.write_bytes(original + b"\n# audited release-scope mutation\n")
                        decision = release_checks.select_checks(
                            "release",
                            REPOSITORY_ROOT,
                            credits=[credit],
                            changed_paths=[relative],
                        )
                    self.assertIn(spec.stable_id, decision.selected_ids)
                    self.assertIn(spec.stable_id, decision.invalidated_credit_ids)
                finally:
                    path.write_bytes(original)

    def test_scope_validation_rejects_escape_and_missing_required_inputs(self) -> None:
        with self.assertRaises(release_checks.SelectionError):
            release_checks.InputScope("file", "../outside.py")
        spec = release_checks.CheckSpec(
            "TEST.REQUIRED-SCOPE",
            "required scope",
            "release",
            ("python", "-c", "pass"),
            (),
            (),
            input_scopes=(release_checks.InputScope("file", "required.py"),),
        )
        with self.assertRaisesRegex(release_checks.SelectionError, "required input scope"):
            release_checks.resolve_input_scope(spec, REPOSITORY_ROOT)

    def test_real_route_manifest_mutations_invalidate_public_consumers(self) -> None:
        specs = tuple(
            spec
            for spec in release_checks.registry()
            if spec.stable_id in {
                "S6.AFFECTED.PUBLIC-E2E",
                "S6.AFFECTED.REAL-AGENT",
            }
        )
        self.assertEqual(2, len(specs))
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-manifest-") as raw:
            source = Path(raw) / "repo"
            shutil.copytree(
                REPOSITORY_ROOT,
                source,
                ignore=shutil.ignore_patterns(
                    ".git", ".agent-workspace", "__pycache__", "*.pyc", ".ruff_cache"
                ),
            )
            repository = TemporaryGitRepository.create(source, branch="test")
            repository.git("add", "-A")
            repository.git("commit", "-m", "copy public route")
            credits = [release_checks.credit_record(spec, source) for spec in specs]
            unrelated = release_checks.select_checks(
                "affected",
                source,
                credits=credits,
                changed_paths=["unrelated-release-note.txt"],
            )
            self.assertEqual(
                {
                    spec.stable_id
                    for spec in release_checks.registry()
                    if spec.tier == "fast"
                },
                set(unrelated.selected_ids),
            )
            self.assertEqual(
                tuple(sorted(spec.stable_id for spec in specs)),
                unrelated.preserved_credit_ids,
            )
            for relative in release_checks.PUBLIC_ROUTE_DEPENDENCIES:
                path = source / relative
                original = path.read_text(encoding="utf-8")
                path.write_text(original + "\n# audited mutation\n", encoding="utf-8")
                decision = release_checks.select_checks(
                    "affected",
                    source,
                    credits=credits,
                    changed_paths=[relative],
                )
                self.assertIn("S6.AFFECTED.PUBLIC-E2E", decision.selected_ids, relative)
                self.assertIn("S6.AFFECTED.REAL-AGENT", decision.selected_ids, relative)
                path.write_text(original, encoding="utf-8")

    def test_release_components_are_stable_and_nonrecursive(self) -> None:
        decision = release_checks.select_checks(
            "release", REPOSITORY_ROOT, exclude_ids=(release_checks.RELEASE_AGGREGATE_ID,)
        )
        selected = [item.spec.stable_id for item in decision.selected]
        expected = {
            spec.stable_id
            for spec in release_checks.registry()
            if spec.stable_id != release_checks.RELEASE_AGGREGATE_ID
        }
        expected.update({
            "S6.RELEASE.RUFF",
            "S6.RELEASE.FORMAT",
            "S6.RELEASE.BASEDPYRIGHT",
            "S6.RELEASE.COMPILE",
            "S6.RELEASE.ORCHESTRATOR-UNIT",
            "S6.RELEASE.WATCHER-UNIT",
            "S6.RELEASE.ATTENTION",
            "S6.RELEASE.SYNTHETIC-CLEANUP",
            "S6.AFFECTED.REAL-AGENT",
        })
        self.assertEqual(expected, set(selected))
        self.assertEqual(len(selected), len(set(selected)))
        self.assertNotIn(release_checks.RELEASE_AGGREGATE_ID, selected)


class S6LocalIsolationTests(unittest.TestCase):
    def test_provider_wrapper_preserves_exec_action_and_adapter_flags(self) -> None:
        support = REPOSITORY_ROOT / "orchestrator_harness" / "tests" / "support"
        path = support / "wsl_codex_provider.py"
        fake_driver = types.ModuleType("wsl_real_agent_driver")
        fake_driver.bwrap_base = lambda *args: ["bwrap", "--die-with-parent"]
        spec = importlib.util.spec_from_file_location("s6_wsl_codex_provider", path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"wsl_real_agent_driver": fake_driver}):
            spec.loader.exec_module(module)
        argv = module._provider_argv(
            [
                "exec",
                "--ignore-user-config",
                "--skip-git-repo-check",
                "--json",
                "--cd",
                "/host/workspace",
                "--output-last-message",
                "/host/message.txt",
            ],
            ["bwrap", "--die-with-parent"],
        )
        self.assertEqual(
            [
                "bwrap",
                "--die-with-parent",
                "/opt/codex/bin/codex",
                "exec",
                "--ignore-user-config",
                "--skip-git-repo-check",
                "--json",
                "--cd",
                "/workspace",
                "--output-last-message",
                "/workspace/.agent-workspace/real_agent_last_message.txt",
            ],
            argv,
        )
        with self.assertRaisesRegex(RuntimeError, "exec action"):
            module._provider_argv(["--json"], ["bwrap"])

    def test_real_agent_uses_native_public_route_and_native_evidence(self) -> None:
        driver = (
            REPOSITORY_ROOT / "orchestrator_harness" / "tests" / "support" / "wsl_real_agent_driver.py"
        ).read_text(encoding="utf-8")
        route = (
            REPOSITORY_ROOT / "orchestrator_harness" / "tests" / "support" / "wsl_public_route_entry.py"
        ).read_text(encoding="utf-8")
        mutation = (REPOSITORY_ROOT / "orchestrator_harness" / "mutation.py").read_text(encoding="utf-8")
        provider = (
            REPOSITORY_ROOT / "orchestrator_harness" / "tests" / "support" / "wsl_codex_provider.py"
        ).read_text(encoding="utf-8")
        self.assertIn("wsl_public_route_entry.py", driver)
        self.assertIn("operator-receipt.json", driver)
        self.assertIn("controller-status.json", driver)
        self.assertIn("controller-result.json", driver)
        self.assertIn("lifecycle-registry.json", driver)
        self.assertIn("CONTROLLER_ACTIVE", driver)
        self.assertIn("RESOURCE_RELEASE_POSSIBLE", driver)
        self.assertNotIn("subprocess.Popen(", driver)
        self.assertNotIn("synthetic_controller.status", driver)
        self.assertNotIn("/opt/codex/bin/codex", driver)
        self.assertLess(driver.index("chown_tree(synthetic_run)"), driver.index("isolation = preflight("))
        self.assertGreaterEqual(driver.count("chown_tree(synthetic_run)"), 2)
        self.assertIn("provider_identity_matches", driver)
        self.assertIn("discover_codex", driver)
        self.assertIn("validate_codex_identity", driver)
        self.assertIn("provider_observed_created_utc", driver)
        self.assertIn("codex_ancestry", driver)
        self.assertIn("launch_lane_controller", route)
        self.assertIn("from orchestrator_harness.public_launch", route)
        self.assertIn("_drop_to_workspace_owner", route)
        self.assertNotIn("ORCH_HARNESS_POSIX_MUTATION_ROOTS", driver + route + mutation)
        self.assertNotIn("safe.directory", driver + route)
        self.assertIn("os.execvp", provider)
        self.assertIn("/opt/codex/bin/codex", provider)
        self.assertNotIn("subprocess.Popen(", provider)
        self.assertEqual(
            tuple(release_checks.PUBLIC_ROUTE_DEPENDENCIES),
            tuple(
                release_checks.get_check("S6.AFFECTED.PUBLIC-E2E").dependency_paths[: len(release_checks.PUBLIC_ROUTE_DEPENDENCIES)]
            ),
        )
        real_dependencies = set(
            release_checks.get_check("S6.AFFECTED.REAL-AGENT").dependency_paths
        )
        self.assertTrue(set(release_checks.REAL_AGENT_ROUTE_DEPENDENCIES).issubset(real_dependencies))

    def test_real_agent_identity_helpers_reject_mixed_or_unpinned_processes(self) -> None:
        self.assertTrue(provider_identity_matches(11, "created", 11, "created"))
        self.assertFalse(provider_identity_matches(11, "created", 12, "created"))
        self.assertFalse(provider_identity_matches(11, "created", 11, "reused"))
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-codex-identity-") as raw:
            root = Path(raw)
            pinned = root / "codex"
            other = root / "other-codex"
            pinned.write_bytes(b"pinned")
            other.write_bytes(b"different")
            chain = [
                {"pid": 11, "ppid": 22},
                {"pid": 22, "ppid": 33},
                {"pid": 33, "ppid": 0},
            ]
            validate_codex_identity(
                11, pinned, chain, provider_pid=33, observed_executable=pinned
            )
            with self.assertRaisesRegex(RuntimeError, "pinned"):
                validate_codex_identity(
                    11, pinned, chain, provider_pid=33, observed_executable=other
                )
            with self.assertRaisesRegex(RuntimeError, "parent"):
                validate_codex_identity(
                    11,
                    pinned,
                    [{"pid": 11, "ppid": 99}, {"pid": 33, "ppid": 0}],
                    provider_pid=33,
                    observed_executable=pinned,
                )

    def test_wsl_installer_is_byte_exact_repeatable_and_pinned(self) -> None:
        installer = (
            REPOSITORY_ROOT / "orchestrator_harness" / "install_wsl_codex.ps1"
        ).read_text(encoding="utf-8")
        self.assertIn("ProcessStartInfo", installer)
        self.assertIn("UTF8.GetBytes", installer)
        self.assertIn("replace \"`r`n?\", \"`n\"", installer)
        self.assertIn("requested %s", installer.lower())
        self.assertIn("already installed", installer.lower())
        self.assertNotIn("$script | wsl.exe", installer)


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
            successful_runs = (
                ("alpha", "fixture_operator_launch.json", "fixture_controller.status.json", "alpha-001"),
                ("beta-success", "fixture_operator_launch-success.json", "fixture_controller-success.status.json", "beta-success-001"),
                ("merge", "fixture_operator_launch.json", "fixture_controller.status.json", "merge-001"),
            )
            for lane, receipt_name, status_name, worker_id in successful_runs:
                workspace = root / "worktrees" / lane / ".agent-workspace"
                receipt = json.loads(
                    (workspace / receipt_name).read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual(
                    ["-m", "orchestrator_harness.lane_controller"], receipt["argv"][1:3]
                )
                status = json.loads(
                    (workspace / status_name).read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual("CODEX_EXITED", status["state"])
                self.assertEqual(0, status["exit_code"])
                self.assertTrue(status["result_valid"])
                self.assertEqual("VALID", status["result_validation"]["state"])
                self.assertEqual(lane, status["declared_lane_id"])
                self.assertEqual([], status["held_resource_claims"])
                self.assertTrue(status["helpers_complete"])
                self.assertTrue(status["direct_child_reaped"])
                self.assertTrue(status["resource_claim_release_safe"])
                self.assertTrue(status["process_boundary"]["complete"])
                self.assertEqual([], status["process_boundary"]["live_members"])
                branch = subprocess.run(
                    ["git", "-C", str(root / "worktrees" / lane), "branch", "--show-current"],
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout.strip()
                head = subprocess.run(
                    ["git", "-C", str(root / "worktrees" / lane), "rev-parse", "HEAD"],
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout.strip()
                result_value = json.loads((workspace / "RESULT.json").read_text(encoding="utf-8"))
                self.assertEqual(lane, result_value["lane_id"])
                self.assertEqual(worker_id, result_value["worker_invocation_id"])
                self.assertEqual(branch, result_value["branch"])
                self.assertEqual(head, result_value["commit"])
                self.assertEqual(head, status["result_validation"]["commit"])
                registry_path = lifecycle_registry_path(
                    root / "worktrees" / lane, lane, worker_id
                )
                registry = json.loads(registry_path.read_text(encoding="utf-8"))
                self.assertTrue(registry["lifecycle"]["complete"])
                self.assertTrue(registry["lifecycle"]["helpers_complete"])
                process = process_snapshot().by_pid.get(receipt["pid"])
                self.assertTrue(
                    process is None
                    or iso_utc(process.created_utc) != receipt["created_utc"]
                )
            stale_workspace = root / "worktrees" / "beta" / ".agent-workspace"
            stale_status = json.loads(
                (stale_workspace / "fixture_controller.status.json").read_text(encoding="utf-8")
            )
            self.assertEqual("CODEX_EXITED", stale_status["state"])
            self.assertFalse(stale_status["result_valid"])
            self.assertEqual("INVALID", stale_status["result_validation"]["state"])
            self.assertEqual(0, stale_status["exit_code"])
            stale_receipt = json.loads(
                (stale_workspace / "fixture_operator_launch.json").read_text(encoding="utf-8")
            )
            stale_process = process_snapshot().by_pid.get(stale_receipt["pid"])
            self.assertTrue(
                stale_process is None
                or iso_utc(stale_process.created_utc) != stale_receipt["created_utc"]
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
        core = (
            REPOSITORY_ROOT / "tools" / "CandidateSafeguard.Core.psm1"
        ).read_text(encoding="utf-8")
        self.assertNotIn("C:/Users/", script)
        self.assertIn("RepositoryRoot", script)
        self.assertIn("ExpectedTip", script)
        self.assertIn("orchestrator_harness.release_checks", script)
        self.assertIn("--root", script)
        self.assertIn("--expected-tip", script)
        self.assertIn("S6.RELEASE.ACCUMULATED-SAFEGUARD", script)
        self.assertNotIn("@{ Name = 'ruff'", script)
        self.assertIn("CreditFile", script)
        self.assertIn("CandidateSafeguard.Core.psm1", script)
        self.assertIn("Invoke-ReleaseChecks", script)
        self.assertNotIn("Assert-FinalIdentity", script)
        self.assertNotIn("SelectionFile", script)
        self.assertNotIn("PythonExecutable", script)
        self.assertIn("--credit-file", script)
        self.assertIn("--git-common-dir", script)
        self.assertIn("--untracked-files=all", core)

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

    def test_safeguard_rejects_injection_and_uses_native_credit_selection(self) -> None:
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-selector-forward-") as raw:
            root = Path(raw) / "candidate"
            shutil.copytree(
                REPOSITORY_ROOT,
                root,
                ignore=shutil.ignore_patterns(
                    ".git", ".agent-workspace", "__pycache__", "*.pyc", ".ruff_cache"
                ),
            )
            subprocess.run(["git", "-C", str(root), "init", "-b", "firmware/v2-candidate"], check=True, capture_output=True)
            subprocess.run(["git", "-C", str(root), "config", "user.email", "selector@example.invalid"], check=True)
            subprocess.run(["git", "-C", str(root), "config", "user.name", "Selector Test"], check=True)
            baseline = root / ".codex" / "dev" / "basedpyright-baseline.json"
            baseline.parent.mkdir(parents=True)
            baseline.write_text("{}\n", encoding="utf-8")
            (root / "pyrightconfig.json").write_text(
                '{"baselineFile": ".codex/dev/basedpyright-baseline.json"}\n',
                encoding="utf-8",
            )
            subprocess.run(["git", "-C", str(root), "add", "."], check=True)
            subprocess.run(["git", "-C", str(root), "commit", "-m", "selector candidate"], check=True, capture_output=True)
            credit_file = Path(raw) / "credits.json"
            credit_file.write_text('{"credits": []}\n', encoding="utf-8")
            script = REPOSITORY_ROOT / "tools" / "Invoke-CandidateSafeguard.ps1"
            completed = subprocess.run(
                [
                    "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script),
                    "-RepositoryRoot", str(root), "-ExpectedBranch", "firmware/v2-candidate",
                    "-CreditFile", str(credit_file),
                ],
                cwd=REPOSITORY_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
            self.assertIn("READY S6.RELEASE.", completed.stdout)
            self.assertNotIn("TEST.SAFEGUARD", completed.stdout)

            injected = subprocess.run(
                [
                    "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script),
                    "-RepositoryRoot", str(root), "-ExpectedBranch", "firmware/v2-candidate",
                    "-SelectionFile", str(Path(raw) / "selection.json"),
                ],
                cwd=REPOSITORY_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(0, injected.returncode)
            self.assertIn("parameter", (injected.stdout + injected.stderr).lower())

    def test_safeguard_stops_before_second_component_after_identity_failure(self) -> None:
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-safeguard-core-") as raw:
            root = Path(raw) / "candidate"
            repository = TemporaryGitRepository.create(root, branch="firmware/v2-candidate")
            baseline = root / ".codex" / "dev" / "basedpyright-baseline.json"
            baseline.parent.mkdir(parents=True)
            baseline.write_text("{}\n", encoding="utf-8")
            pyright_config = root / "pyrightconfig.json"
            pyright_config.write_text(
                '{"baselineFile": ".codex/dev/basedpyright-baseline.json"}\n',
                encoding="utf-8",
            )
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
            repository.git("commit", "-m", "safeguard core fixture")
            baseline_hash = hashlib.sha256(baseline.read_bytes()).hexdigest()
            config_hash = hashlib.sha256(pyright_config.read_bytes()).hexdigest()
            module = REPOSITORY_ROOT / "tools" / "CandidateSafeguard.Core.psm1"
            driver = Path(raw) / "invoke-core.ps1"
            driver.write_text(
                f"""Import-Module -Force '{module}'
$checks = @(
    [pscustomobject]@{{ stable_id = 'TEST.FIRST'; command = @('powershell', '-NoProfile', '-File', 'first.ps1') }},
    [pscustomobject]@{{ stable_id = 'TEST.SECOND'; command = @('powershell', '-NoProfile', '-File', 'second.ps1') }}
)
try {{
    Invoke-ReleaseChecks -Checks $checks -RepositoryRoot '{root}' -ExpectedHead '{repository.head}' `
        -ExpectedBranch 'firmware/v2-candidate' -ExpectedCommonDirectory '{repository.common_dir}' `
        -Baseline '{baseline}' -PyrightConfig '{pyright_config}' `
        -BaselineHash '{baseline_hash}' -ConfigHash '{config_hash}'
    exit 0
}} catch {{
    Write-Error $_
    exit 17
}}
""",
                encoding="utf-8",
            )
            completed = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(driver)],
                cwd=root,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(17, completed.returncode, completed.stdout + completed.stderr)
            self.assertIn("dirty", (completed.stdout + completed.stderr).lower())
            self.assertTrue((root / "first-ran.txt").is_file())
            self.assertFalse((root / "second-ran.txt").exists())


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
        canonical_paths = manifest_asset_paths()
        self.assertEqual(
            tuple(manifest["examples"] + manifest["release_evidence_templates"]),
            canonical_paths,
        )
        for relative_path in canonical_paths:
            self.assertTrue(read_package_asset(relative_path), relative_path)
        for checkout_path, package_path in manifest["checkout_copies"].items():
            checkout_bytes = (REPOSITORY_ROOT / checkout_path).read_bytes()
            package_bytes = read_package_asset(package_path).encode("utf-8")
            self.assertEqual(
                checkout_bytes.replace(b"\r\n", b"\n"),
                package_bytes.replace(b"\r\n", b"\n"),
                checkout_path,
            )
        self.assertTrue(
            (REPOSITORY_ROOT / "orchestrator_harness" / "release_checks.py").is_file()
        )
        self.assertTrue(
            (REPOSITORY_ROOT / "orchestrator_harness" / "public_launch.py").is_file()
        )

    def test_out_of_tree_wheel_resolves_one_canonical_asset_tree_and_leaves_source_clean(
        self,
    ) -> None:
        before = subprocess.run(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            cwd=REPOSITORY_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-wheel-") as raw:
            temporary = Path(raw)
            source = temporary / "source"
            wheel_dir = temporary / "wheel"
            probe_dir = temporary / "probe"
            ignored = shutil.ignore_patterns(
                ".git",
                ".agent-workspace",
                ".ruff_cache",
                "__pycache__",
                "*.pyc",
                "*.egg-info",
                "build",
                "dist",
            )
            shutil.copytree(REPOSITORY_ROOT, source, ignore=ignored)
            wheel_dir.mkdir()
            probe_dir.mkdir()
            built = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "wheel",
                    "--no-deps",
                    "--no-build-isolation",
                    "--wheel-dir",
                    str(wheel_dir),
                    str(source / "orchestrator_harness"),
                ],
                cwd=source,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, built.returncode, built.stdout + built.stderr)
            wheels = list(wheel_dir.glob("*.whl"))
            self.assertEqual(1, len(wheels))
            wheel = wheels[0]
            with zipfile.ZipFile(wheel) as archive:
                names = set(archive.namelist())
            self.assertTrue(
                any(name.endswith("orchestrator_harness/assets/release/manifest.json") for name in names)
            )
            self.assertTrue(any(".data/data/examples/" in name for name in names))
            self.assertTrue(any(".data/data/release_evidence_templates/" in name for name in names))
            probe = (
                "import sys\n"
                "sys.path.insert(0, sys.argv[1])\n"
                "from orchestrator_harness.release_assets import manifest_asset_paths, read_package_asset\n"
                "paths = manifest_asset_paths()\n"
                "assert paths and all(read_package_asset(path) for path in paths)\n"
            )
            environment = dict(os.environ)
            environment.pop("PYTHONPATH", None)
            checked = subprocess.run(
                [sys.executable, "-I", "-c", probe, str(wheel)],
                cwd=probe_dir,
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, checked.returncode, checked.stdout + checked.stderr)
        after = subprocess.run(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            cwd=REPOSITORY_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
