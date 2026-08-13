from __future__ import annotations

import ctypes
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
import zipfile
from ctypes import wintypes
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from examples.disposable_coding_fixture import run_fixture
from orchestrator_harness import release_checks
from orchestrator_harness.lane_lifecycle import lifecycle_registry_path
from orchestrator_harness.models import ProcessInfo, ProcessQuery, iso_utc
from orchestrator_harness.processes import process_snapshot
from orchestrator_harness.release_assets import (
    manifest_asset_paths,
    read_package_asset,
    release_manifest,
)
from orchestrator_harness.tests import real_agent_test as real_agent_test_module
from orchestrator_harness.tests.real_agent_test import (
    DEFAULT_EVIDENCE_DIRECTORY,
    _capture_directory_identity,
    _close_evidence_root_handle,
    _finalize_attempt_evidence,
    _open_evidence_root_handle,
    _provider_identity_from_query,
    _release_preparation_process,
    _remove_disposable_temp_root,
    _resolve_evidence_root,
    _revalidate_evidence_root_binding,
    _safe_failure_record,
    _safe_success_record,
    _terminal_finalize,
    _validate_controller_receipt_identity,
)
from orchestrator_harness.tests.support import TemporaryGitRepository
from orchestrator_harness.tests.wsl_identity import (
    provider_identity_matches,
    validate_codex_identity,
    validate_cross_os_identity_relation,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_SUPPORT_ROOT = Path(__file__).resolve().parent / "support"
_WINDOWLESS_CREATION_FLAGS = 0x08000000 if os.name == "nt" else 0
if str(_SUPPORT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SUPPORT_ROOT))
_support_spec = importlib.util.spec_from_file_location(
    "s6_wsl_real_agent_driver", _SUPPORT_ROOT / "wsl_real_agent_driver.py"
)
assert _support_spec is not None and _support_spec.loader is not None
_support_module = importlib.util.module_from_spec(_support_spec)
_support_spec.loader.exec_module(_support_module)
PREPARED_STATE_SCHEMA = _support_module.PREPARED_STATE_SCHEMA
claim_prepared_state = _support_module.claim_prepared_state
validate_prepared_state = _support_module.validate_prepared_state


class _FakePrepProcess:
    """Duck-typed exact Popen handle for terminal-seam fault injection."""

    def __init__(
        self,
        *,
        running: bool = True,
        wait_fault: BaseException | None = None,
        kill_fault: BaseException | None = None,
        post_kill_wait_fault: BaseException | None = None,
    ) -> None:
        self._running = running
        self._wait_fault = wait_fault
        self._kill_fault = kill_fault
        self._post_kill_wait_fault = post_kill_wait_fault
        self.kill_calls = 0
        self.wait_calls = 0

    def poll(self) -> int | None:
        return None if self._running else 0

    def kill(self) -> None:
        self.kill_calls += 1
        if self._kill_fault is not None:
            raise self._kill_fault

    def wait(self, timeout: float | None = None) -> int:
        self.wait_calls += 1
        if timeout == 90:
            if self._wait_fault is not None:
                raise self._wait_fault
            if self._running:
                raise subprocess.TimeoutExpired("wsl.exe", timeout)
            return 0
        if timeout == 10:
            if self._post_kill_wait_fault is not None:
                raise self._post_kill_wait_fault
            return 0
        return 0


class _FakeReleaseSignal:
    """Duck-typed release signal whose write can be fault-injected."""

    def __init__(self, fault: BaseException | None = None) -> None:
        self._fault = fault
        self.writes = 0

    def write_text(self, text: str, encoding: str = "utf-8") -> None:
        self.writes += 1
        if self._fault is not None:
            raise self._fault


def _make_directory_link(link: Path, target: Path) -> None:
    """Create a directory symlink or a Windows junction at link -> target."""

    if os.name == "nt":
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(target)],
            capture_output=True, text=True, shell=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"junction creation failed: {result.stderr}")
    else:
        os.symlink(target, link, target_is_directory=True)


def _is_directory_link(path: Path) -> bool:
    if path.is_symlink():
        return True
    return os.name == "nt" and os.path.isjunction(path)


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
        self.assertEqual(
            ("python", "-m", "orchestrator_harness.tests.real_agent_test"),
            release_checks.get_check("S6.AFFECTED.REAL-AGENT").command,
        )
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

    def test_selector_and_aggregate_credit_cover_their_actual_inputs(self) -> None:
        cases = {
            "S6.FAST.SELECTOR": (
                ".gitignore",
                "tools/Invoke-CandidateSafeguard.ps1",
                "tools/CandidateSafeguard.Core.psm1",
            ),
            release_checks.RELEASE_AGGREGATE_ID: (".gitignore",),
        }
        for stable_id, relatives in cases.items():
            spec = release_checks.get_check(stable_id)
            for relative in relatives:
                with self.subTest(stable_id=stable_id, relative=relative):
                    path = REPOSITORY_ROOT / relative
                    original = path.read_bytes()
                    credit = release_checks.credit_record(spec, REPOSITORY_ROOT)
                    original_fingerprint = release_checks.dependency_fingerprint(spec, REPOSITORY_ROOT)
                    try:
                        path.write_bytes(original + b"\n# audited actual-input mutation\n")
                        decision = release_checks.select_checks(
                            "release" if spec.tier == "release" else "affected",
                            REPOSITORY_ROOT, credits=[credit], changed_paths=[relative],
                        )
                        self.assertIn(stable_id, decision.selected_ids)
                        self.assertIn(stable_id, decision.invalidated_credit_ids)
                        self.assertNotEqual(
                            original_fingerprint,
                            release_checks.dependency_fingerprint(spec, REPOSITORY_ROOT),
                        )
                    finally:
                        path.write_bytes(original)
                    self.assertEqual(
                        original_fingerprint,
                        release_checks.dependency_fingerprint(spec, REPOSITORY_ROOT),
                    )
                    preserved = release_checks.select_checks(
                        "release" if spec.tier == "release" else "affected",
                        REPOSITORY_ROOT, credits=[credit], changed_paths=[".gitattributes"],
                    )
                    self.assertIn(stable_id, preserved.preserved_credit_ids)
                    self.assertNotIn(stable_id, preserved.invalidated_credit_ids)

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

    def test_package_scope_consumes_manifest_assets_and_checkout_copies(self) -> None:
        spec = release_checks.get_check("S6.FAST.PACKAGE")
        credit = release_checks.credit_record(spec, REPOSITORY_ROOT)
        manifest = release_manifest()
        scoped = set(release_checks.resolve_input_scope(spec, REPOSITORY_ROOT))
        excluded = set(spec.dependency_paths)
        # The manifest names canonical assets relative to the
        # orchestrator_harness package root; the tracked paths are the same
        # files under the package tree.  Checkout copies are tracked at their
        # repository-relative copies.
        canonical = tuple(
            f"orchestrator_harness/{relative}"
            for relative in manifest["examples"] + manifest["release_evidence_templates"]
            if f"orchestrator_harness/{relative}" in scoped
            and f"orchestrator_harness/{relative}" not in excluded
        )
        checkout = tuple(
            relative for relative in manifest["checkout_copies"]
            if relative in scoped and relative not in excluded
        )
        self.assertTrue(canonical)
        self.assertTrue(checkout)
        for relative in canonical + checkout:
            with self.subTest(relative=relative):
                decision = release_checks.select_checks(
                    "affected", REPOSITORY_ROOT, credits=[credit], changed_paths=[relative],
                )
                self.assertIn(spec.stable_id, decision.invalidated_credit_ids, relative)
                self.assertNotIn(spec.stable_id, decision.preserved_credit_ids, relative)
        preserved = release_checks.select_checks(
            "affected", REPOSITORY_ROOT, credits=[credit], changed_paths=[".gitattributes"],
        )
        self.assertIn(spec.stable_id, preserved.preserved_credit_ids)
        self.assertNotIn(spec.stable_id, preserved.invalidated_credit_ids)

    def test_unit_scope_consumes_non_python_docs_config_and_fixtures(self) -> None:
        spec = release_checks.get_check("S6.RELEASE.ORCHESTRATOR-UNIT")
        credit = release_checks.credit_record(spec, REPOSITORY_ROOT)
        scoped = set(release_checks.resolve_input_scope(spec, REPOSITORY_ROOT))
        excluded = set(spec.dependency_paths)
        candidates = tuple(
            relative for relative in scoped
            if relative not in excluded and not relative.endswith(".py")
        )
        self.assertTrue(candidates)
        for relative in candidates[:3]:
            with self.subTest(relative=relative):
                decision = release_checks.select_checks(
                    "affected", REPOSITORY_ROOT, credits=[credit], changed_paths=[relative],
                )
                self.assertIn(spec.stable_id, decision.invalidated_credit_ids, relative)
                self.assertNotIn(spec.stable_id, decision.preserved_credit_ids, relative)
        preserved = release_checks.select_checks(
            "affected", REPOSITORY_ROOT, credits=[credit], changed_paths=[".gitattributes"],
        )
        self.assertIn(spec.stable_id, preserved.preserved_credit_ids)
        self.assertNotIn(spec.stable_id, preserved.invalidated_credit_ids)

    def test_safeguard_rejects_foreign_selection_command_from_candidate_registry(self) -> None:
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-foreign-selection-") as raw:
            raw_path = Path(raw)
            root = raw_path / "candidate"
            shutil.copytree(
                REPOSITORY_ROOT, root,
                ignore=shutil.ignore_patterns(
                    ".git", ".agent-workspace", "__pycache__", "*.pyc", ".ruff_cache"
                ),
            )
            subprocess.run(["git", "-C", str(root), "init", "-b", "firmware/v2-candidate"], check=True, capture_output=True)
            subprocess.run(["git", "-C", str(root), "config", "user.email", "foreign@example.invalid"], check=True, capture_output=True)
            subprocess.run(["git", "-C", str(root), "config", "user.name", "Foreign Selection"], check=True, capture_output=True)
            subprocess.run(["git", "-C", str(root), "add", "."], check=True, capture_output=True)
            subprocess.run(["git", "-C", str(root), "commit", "-m", "candidate"], check=True, capture_output=True)
            head = subprocess.run(
                ["git", "-C", str(root), "rev-parse", "HEAD"], check=True,
                capture_output=True, text=True,
            ).stdout.strip()
            environment = dict(os.environ)
            environment["PYTHONPATH"] = str(root) + (
                os.pathsep + environment["PYTHONPATH"] if environment.get("PYTHONPATH") else ""
            )
            real = subprocess.run(
                [
                    sys.executable, "-m", "orchestrator_harness.release_checks", "select",
                    "--intent", "release", "--root", str(root),
                    "--expected-branch", "firmware/v2-candidate", "--expected-tip", head,
                    "--exclude-id", "S6.RELEASE.ACCUMULATED-SAFEGUARD",
                ],
                cwd=root, env=environment, capture_output=True, text=True, check=False,
            )
            self.assertEqual(0, real.returncode, real.stdout + real.stderr)
            selection = json.loads(real.stdout)
            tampered = False
            for item in selection["selected"]:
                if item["stable_id"].startswith("S6.RELEASE.RUFF"):
                    item["command"] = ["python", "-m", "ruff", "check", "--foreign", "."]
                    tampered = True
                    break
            self.assertTrue(tampered)
            crafted = raw_path / "crafted-selection.json"
            crafted.write_text(json.dumps(selection, separators=(",", ":")), encoding="utf-8")
            shim_dir = raw_path / "shim"
            shim_dir.mkdir()
            shim = shim_dir / "python.cmd"
            shim.write_text(
                "@echo off\r\n"
                'if "%1"=="-c" (\r\n'
                f'  "{sys.executable}" %*\r\n'
                "  exit /b %errorlevel%\r\n"
                ")\r\n"
                'if "%3"=="registry" (\r\n'
                f'  "{sys.executable}" -m orchestrator_harness.release_checks registry\r\n'
                "  exit /b %errorlevel%\r\n"
                ")\r\n"
                f'type "{crafted}"\r\n'
                "exit /b 0\r\n",
                encoding="utf-8",
            )
            credit_file = raw_path / "credits.json"
            credit_file.write_text('{"credits": []}\n', encoding="utf-8")
            shim_environment = dict(os.environ)
            shim_environment["PATH"] = str(shim_dir) + os.pathsep + shim_environment.get("PATH", "")
            completed = subprocess.run(
                [
                    "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                    str(root / "tools" / "Invoke-CandidateSafeguard.ps1"),
                    "-RepositoryRoot", str(root), "-ExpectedBranch", "firmware/v2-candidate",
                    "-ExpectedTip", head, "-CreditFile", str(credit_file),
                ],
                cwd=REPOSITORY_ROOT, env=shim_environment,
                capture_output=True, text=True, check=False,
                creationflags=_WINDOWLESS_CREATION_FLAGS,
            )
            self.assertNotEqual(0, completed.returncode)
            self.assertIn("does not match the candidate registry", completed.stderr)


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

    def test_real_agent_provider_query_and_receipt_identity_fail_closed(self) -> None:
        created = datetime(2026, 8, 13, 1, 0, tzinfo=timezone.utc)
        created_text = iso_utc(created)
        assert created_text is not None
        receipt = {"pid": 101, "created_utc": created_text}
        status = {"controller_pid": 101, "controller_created_utc": created_text}
        self.assertEqual((101, created_text), _validate_controller_receipt_identity(receipt, status))
        for changed in (
            {**status, "controller_pid": 102},
            {**status, "controller_created_utc": "2026-08-13T01:00:01Z"},
        ):
            with self.subTest(changed=changed), self.assertRaises(RuntimeError):
                _validate_controller_receipt_identity(receipt, changed)

        process = ProcessInfo(
            pid=202, ppid=101, name="wsl.exe",
            command_line="wsl.exe -d Ubuntu -- provider", created_utc=created,
        )
        identity = _provider_identity_from_query(
            ProcessQuery(True, process), provider_pid=202,
            provider_created=created_text, controller_pid=101,
            nonce="nonce", invocation_id="invocation",
        )
        self.assertEqual(101, identity["parent_pid"])
        invalid_queries = (
            ProcessQuery(False, process),
            ProcessQuery(True, process, ("parent mismatch",)),
            ProcessQuery(True, None),
            ProcessQuery(True, ProcessInfo(
                pid=202, ppid=999, name="wsl.exe",
                command_line="wsl.exe -d Ubuntu -- provider", created_utc=created,
            )),
        )
        for query in invalid_queries:
            with self.subTest(query=query), self.assertRaises(RuntimeError):
                _provider_identity_from_query(
                    query, provider_pid=202, provider_created=created_text,
                    controller_pid=101, nonce="nonce", invocation_id="invocation",
                )

    def test_real_agent_failure_retains_only_safe_out_of_source_summary(self) -> None:
        sentinel = "PROVIDER_TRANSCRIPT_SENTINEL"
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-safe-evidence-") as raw:
            root = Path(raw)
            source = root / "source"
            source.mkdir()
            outside = _resolve_evidence_root(root / "evidence", repository_root=source)
            self.assertFalse(outside.is_relative_to(source))
            with patch(
                "orchestrator_harness.tests.real_agent_test.tempfile.gettempdir",
                return_value=str(root / "detected-temp"),
            ):
                detected = _resolve_evidence_root(None, repository_root=source)
            self.assertEqual((root / "detected-temp" / DEFAULT_EVIDENCE_DIRECTORY).resolve(), detected)
            with self.assertRaisesRegex(RuntimeError, "outside the source"):
                _resolve_evidence_root(source / "runtime", repository_root=source)

            temp_root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-"))
            try:
                workspace = temp_root / "synthetic-repository" / ".agent-workspace"
                workspace.mkdir(parents=True)
                for name in (
                    "real_agent_codex.jsonl", "real_agent_codex.stderr.log",
                    "real_agent_last_message.txt",
                ):
                    (workspace / name).write_text(sentinel, encoding="utf-8")
                (workspace / "real_agent_controller.status.json").write_text(json.dumps({
                    "state": "CONTROLLER_FAILED", "exit_code": 1, "task": sentinel,
                    "launcher_settings": {"argv": [sentinel]}, "held_resource_claims": [],
                }), encoding="utf-8")
                (temp_root / "bridge-evidence.json").write_text(
                    json.dumps({"status": "FAIL", "provider_output": sentinel}), encoding="utf-8",
                )
                prepared_evidence = temp_root / "prepared-evidence"
                prepared_evidence.mkdir()
                (prepared_evidence / "proxy-audit.jsonl").write_text(
                    sentinel, encoding="utf-8"
                )
                (prepared_evidence / "prepared-state.json").write_text(
                    sentinel, encoding="utf-8"
                )
                root_binding = _open_evidence_root_handle(outside)
                attempt_name = "forced-failure"
                try:
                    with self.assertRaisesRegex(RuntimeError, "PROVIDER_TRANSCRIPT_SENTINEL"):
                        _terminal_finalize(
                            root_binding=root_binding, attempt_name=attempt_name,
                            temp_root=temp_root, failure=RuntimeError(sentinel),
                            result=None, prep_process=None, release_signal=None,
                        )
                finally:
                    _close_evidence_root_handle(root_binding)
                self.assertFalse(temp_root.exists())
                published = outside / attempt_name
                self.assertTrue(published.is_dir())
                self.assertEqual([attempt_name], [p.name for p in outside.iterdir()])
                retained = tuple(
                    path.relative_to(published).as_posix()
                    for path in published.rglob("*") if path.is_file()
                )
                self.assertEqual(("REAL_AGENT_TEST_RESULT.json",), retained)
                text = (published / retained[0]).read_text(encoding="utf-8")
                self.assertNotIn(sentinel, text)
                self.assertNotIn("real_agent_codex.jsonl", text)
                self.assertNotIn("real_agent_codex.stderr.log", text)
                result = json.loads(text)
                self.assertFalse(result["transcript_persisted"])
                self.assertEqual(1, result["retained_file_count"])
                self.assertTrue(result["host_temp_cleanup_complete"])
                self.assertTrue(result["source_artifact_facts"]["provider_event_stream"]["present"])
                self.assertNotIn(
                    ".real-agent/", (REPOSITORY_ROOT / ".gitignore").read_text(encoding="utf-8"),
                )
            finally:
                if temp_root.exists():
                    shutil.rmtree(temp_root, ignore_errors=True)

    def test_real_agent_success_finalizer_retains_only_allowlisted_record(self) -> None:
        sentinel = "SUCCESS_PROVIDER_TRANSCRIPT_SENTINEL"
        created = datetime(2026, 8, 13, 1, 0, tzinfo=timezone.utc)
        created_text = iso_utc(created)
        assert created_text is not None
        commit = "0" * 40
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-success-evidence-") as raw:
            root = Path(raw)
            temp_root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-"))
            try:
                workspace = temp_root / "synthetic-repository" / ".agent-workspace"
                workspace.mkdir(parents=True)
                for name in (
                    "real_agent_codex.jsonl",
                    "real_agent_codex.stderr.log",
                    "real_agent_last_message.txt",
                    "real_agent_prompt.md",
                ):
                    (workspace / name).write_text(sentinel, encoding="utf-8")
                receipt_path = workspace / "real_agent_operator.receipt.json"
                status_path = workspace / "real_agent_controller.status.json"
                result_path = workspace / "RESULT.json"
                prompt_path = workspace / "real_agent_prompt.md"
                lifecycle_path = workspace / "lifecycle.json"
                receipt_path.write_text(
                    json.dumps({"pid": 101, "created_utc": created_text}), encoding="utf-8",
                )
                status_path.write_text(json.dumps({
                    "controller_pid": 101, "controller_created_utc": created_text,
                    "state": "CODEX_EXITED", "exit_code": 0, "result_valid": True,
                    "result_validation": {"state": "VALID", "commit": commit},
                    "helpers_complete": True, "direct_child_reaped": True,
                    "resource_claim_release_safe": True, "held_resource_claims": [],
                    "process_boundary": {"complete": True, "live_members": []},
                    "task": sentinel, "launcher_settings": {"argv": [sentinel]},
                }), encoding="utf-8")
                poisoned_result = {
                    "schema": "orchestrator-lane-result/v1",
                    "lane_id": "real-agent",
                    "worker_invocation_id": "real-agent-001",
                    "branch": "real-agent",
                    "commit": commit,
                    "outcome": "PASS",
                    "summary": f"public route task passed {sentinel}",
                    "checks": [{
                        "name": "synthetic public route",
                        "outcome": "PASS",
                        "summary": f"marker committed {sentinel}",
                        "command": f"provider check command {sentinel}",
                    }],
                }
                result_path.write_text(json.dumps(poisoned_result), encoding="utf-8")
                lifecycle_path.write_text(json.dumps({
                    "lifecycle": {"complete": True, "helpers_complete": True},
                    "record": sentinel,
                }), encoding="utf-8")
                bridge_evidence = temp_root / "bridge-evidence.json"
                state_path = temp_root / "prepared-state.json"
                claim_path = temp_root / "prepared-claim.json"
                bridge_evidence.write_text(json.dumps({
                    "status": "PASS", "cleanup_complete": True,
                    "sandbox": {"mnt_c_exposed": False, "usb_exposed": False},
                    "provider_output": sentinel,
                }), encoding="utf-8")
                state_path.write_text(json.dumps({
                    "status": "CLEANED", "cleanup_complete": True,
                    "credentials_in_state": False, "evidence": sentinel,
                }), encoding="utf-8")
                claim_path.write_text(json.dumps({"claim": sentinel}), encoding="utf-8")
                prepared_evidence = temp_root / "prepared-evidence"
                prepared_evidence.mkdir()
                (prepared_evidence / "proxy-audit.jsonl").write_text(sentinel, encoding="utf-8")
                (prepared_evidence / "prepared-state.json").write_text(sentinel, encoding="utf-8")
                evidence_root = root / "evidence"
                evidence_root.mkdir()
                root_binding = _open_evidence_root_handle(evidence_root)
                attempt_name = "success-attempt"
                try:
                    expected_result_sha = hashlib.sha256(result_path.read_bytes()).hexdigest()
                    record = _safe_success_record(
                        completed_utc="2026-08-13T01:00:05Z",
                        route="public_launch -> operator_launch -> lane_controller -> wsl.exe provider bridge",
                        distro="Ubuntu", codex_root="/opt/orchestrator-harness-codex",
                        nonce="nonce", invocation_id="real-agent-001",
                        controller_pid=101, controller_created=created_text,
                        provider_identity={
                            "platform": "windows", "pid": 202, "created_utc": created_text,
                            "nonce": "nonce", "invocation_id": "real-agent-001",
                            "parent_pid": 101,
                        },
                        linux_bridge_identity={
                            "platform": "linux", "pid": 303, "created_utc": created_text,
                            "nonce": "nonce", "invocation_id": "real-agent-001",
                        },
                        status=json.loads(status_path.read_text(encoding="utf-8")),
                        result_value=poisoned_result,
                        lifecycle=json.loads(lifecycle_path.read_text(encoding="utf-8")),
                        bridge=json.loads(bridge_evidence.read_text(encoding="utf-8")),
                        prepared=json.loads(state_path.read_text(encoding="utf-8")),
                        event_types=[
                            "CONTROLLER_ACTIVE", "CONTROLLER_EXITED", "RESOURCE_RELEASE_POSSIBLE",
                        ],
                        workspace=workspace,
                        receipt_path=receipt_path, result_path=result_path,
                        lifecycle_path=lifecycle_path, bridge_evidence=bridge_evidence,
                        state_path=state_path, claim_path=claim_path, prompt_path=prompt_path,
                    )
                    published = _terminal_finalize(
                        root_binding=root_binding, attempt_name=attempt_name,
                        temp_root=temp_root, failure=None, result=record,
                        prep_process=None, release_signal=None,
                    )
                finally:
                    _close_evidence_root_handle(root_binding)
                self.assertEqual(evidence_root / attempt_name, published)
                self.assertFalse(temp_root.exists())
                self.assertEqual([attempt_name], [p.name for p in evidence_root.iterdir()])
                retained = tuple(
                    path.relative_to(published).as_posix()
                    for path in published.rglob("*") if path.is_file()
                )
                self.assertEqual(("REAL_AGENT_TEST_RESULT.json",), retained)
                text = (published / retained[0]).read_text(encoding="utf-8")
                self.assertNotIn(sentinel, text)
                self.assertNotIn("real_agent_codex.jsonl", text)
                self.assertNotIn("real_agent_codex.stderr.log", text)
                self.assertNotIn("real_agent_last_message.txt", text)
                self.assertNotIn("real_agent_prompt.md", text)
                self.assertNotIn("bridge-evidence.json", text)
                self.assertNotIn("prepared-state.json", text)
                result = json.loads(text)
                self.assertEqual("orchestrator-real-agent-evidence/v1", result["schema"])
                self.assertEqual("PASS", result["status"])
                self.assertEqual(1, result["retained_file_count"])
                self.assertFalse(result["credentials_persisted"])
                self.assertFalse(result["transcript_persisted"])
                self.assertTrue(result["host_temp_cleanup_complete"])
                self.assertEqual("real-agent", result["result_identity"]["lane_id"])
                self.assertEqual(commit, result["result_identity"]["commit"])
                self.assertEqual("PASS", result["result_identity"]["outcome"])
                self.assertEqual("VALID", result["controller_summary"]["result_validation_state"])
                self.assertFalse(result["bridge_summary"]["mnt_c_exposed"])
                self.assertTrue(result["prepared_summary"]["cleanup_complete"])
                self.assertEqual(
                    expected_result_sha,
                    result["source_artifact_facts"]["controller_result"]["sha256"],
                )
                self.assertNotIn("controller_evidence", result)
                self.assertNotIn("bridge_evidence", result)
                self.assertNotIn("prepared_cleanup", result)
                self.assertNotIn("checks", result)
                self.assertNotIn("summary", result)
            finally:
                if temp_root.exists():
                    shutil.rmtree(temp_root, ignore_errors=True)

    def test_real_agent_terminal_routes_use_one_shared_finalizer(self) -> None:
        source = (
            REPOSITORY_ROOT / "orchestrator_harness" / "tests" / "real_agent_test.py"
        ).read_text(encoding="utf-8")
        self.assertIn("def _finalize_attempt_evidence(", source)
        self.assertIn("def _terminal_finalize(", source)
        self.assertIn("def _release_preparation_process(", source)
        self.assertIn("def _open_evidence_root_handle(", source)
        self.assertIn("def _close_evidence_root_handle(", source)
        self.assertIn("def _rename_attempt_relative(", source)
        self.assertIn("_PUBLICATION_INTERPOSITION_HOOK", source)
        self.assertIn("_PRE_FINAL_STAGING_VALIDATION_HOOK", source)
        self.assertNotIn("def _redacted_controller_evidence(", source)
        self.assertNotIn("def _write_safe_failure_result(", source)
        self.assertNotIn('"controller_evidence":', source)
        self.assertNotIn('"bridge_evidence": bridge', source)
        self.assertNotIn('"prepared_cleanup":', source)
        self.assertNotIn("def _remove_attempt_entry(", source)
        self.assertNotIn("def _revalidate_attempt_identity(", source)
        self.assertNotIn("evidence.rglob(", source)
        self.assertNotIn("evidence.iterdir(", source)
        self.assertNotIn("evidence.resolve(", source)
        self.assertNotIn("_json(evidence /", source)
        self.assertNotIn("os.replace(evidence", source)
        self.assertNotIn("shutil.rmtree(evidence", source)
        main_tail = source.split("def main()", 1)[1]
        finally_block = main_tail.split("finally:", 1)[1].split("assert result", 1)[0]
        self.assertEqual(1, finally_block.count("_terminal_finalize("))
        self.assertEqual(1, finally_block.count("REAL_AGENT_EVIDENCE="))
        self.assertNotIn("release_signal.write_text(", finally_block)
        self.assertNotIn("prep_process.wait(", finally_block)
        self.assertNotIn("prep_process.kill(", finally_block)
        self.assertEqual(1, source.count("release_signal.write_text("))
        self.assertGreaterEqual(source.count("prep_process.wait("), 2)
        self.assertGreaterEqual(source.count("prep_process.kill("), 2)
        seam = source.split("def _terminal_finalize(", 1)[1].split("\ndef ", 1)[0]
        self.assertEqual(2, seam.count("_finalize_attempt_evidence("))
        self.assertIn("_close_evidence_root_handle(", seam)

    def test_real_agent_cleanup_faults_funnel_through_terminal_seam(self) -> None:
        sentinel = "CLEANUP_FAULT_TRANSCRIPT_SENTINEL"
        scenarios = (
            (
                "release_write",
                _FakeReleaseSignal(OSError("release write fault")),
                _FakePrepProcess(running=True),
            ),
            (
                "non_timeout_wait",
                None,
                _FakePrepProcess(running=True, wait_fault=OSError("initial wait fault")),
            ),
            (
                "timeout_kill",
                None,
                _FakePrepProcess(running=True, kill_fault=OSError("timeout kill fault")),
            ),
            (
                "post_kill_wait",
                None,
                _FakePrepProcess(
                    running=True, post_kill_wait_fault=OSError("post-kill wait fault"),
                ),
            ),
        )
        for name, release_signal, prep_process in scenarios:
            with self.subTest(operation=name):
                with tempfile.TemporaryDirectory(prefix="orchestrator-s6-cleanup-fault-") as raw:
                    root = Path(raw)
                    temp_root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-"))
                    try:
                        workspace = temp_root / "synthetic-repository" / ".agent-workspace"
                        workspace.mkdir(parents=True)
                        (workspace / "real_agent_codex.jsonl").write_text(
                            sentinel, encoding="utf-8",
                        )
                        (workspace / "real_agent_controller.status.json").write_text(
                            json.dumps({
                                "state": "CONTROLLER_FAILED", "exit_code": 1,
                                "task": sentinel, "held_resource_claims": [],
                            }),
                            encoding="utf-8",
                        )
                        evidence_root = root / "evidence"
                        evidence_root.mkdir()
                        root_binding = _open_evidence_root_handle(evidence_root)
                        attempt_name = name
                        success_record = {
                            "schema": "orchestrator-real-agent-evidence/v1",
                            "status": "PASS",
                            "completed_utc": "2026-08-13T06:30:00Z",
                            "retained_file_count": 1,
                            "credentials_persisted": False,
                            "transcript_persisted": False,
                        }
                        try:
                            with self.assertRaises(OSError):
                                _terminal_finalize(
                                    root_binding=root_binding, attempt_name=attempt_name,
                                    temp_root=temp_root, failure=None, result=success_record,
                                    prep_process=prep_process, release_signal=release_signal,
                                )
                        finally:
                            _close_evidence_root_handle(root_binding)
                        self.assertFalse(temp_root.exists())
                        published = evidence_root / attempt_name
                        retained = tuple(
                            path.relative_to(published).as_posix()
                            for path in published.rglob("*") if path.is_file()
                        )
                        self.assertEqual(("REAL_AGENT_TEST_RESULT.json",), retained)
                        text = (published / retained[0]).read_text(encoding="utf-8")
                        self.assertNotIn(sentinel, text)
                        record = json.loads(text)
                        self.assertEqual("FAIL", record["status"])
                        self.assertEqual("OSError", record["preparation_cleanup_failure_type"])
                        self.assertTrue(record["host_temp_cleanup_complete"])
                        self.assertEqual(1, record["retained_file_count"])
                    finally:
                        if temp_root.exists():
                            shutil.rmtree(temp_root, ignore_errors=True)

    def test_real_agent_cleanup_fault_on_failure_route_preserves_original_failure(self) -> None:
        sentinel = "ORIGINAL_FAILURE_TRANSCRIPT_SENTINEL"
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-cleanup-fault-") as raw:
            root = Path(raw)
            temp_root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-"))
            try:
                workspace = temp_root / "synthetic-repository" / ".agent-workspace"
                workspace.mkdir(parents=True)
                (workspace / "real_agent_codex.jsonl").write_text(
                    sentinel, encoding="utf-8",
                )
                evidence_root = root / "evidence"
                evidence_root.mkdir()
                root_binding = _open_evidence_root_handle(evidence_root)
                attempt_name = "failure-route"
                try:
                    with self.assertRaisesRegex(RuntimeError, "original product failure"):
                        _terminal_finalize(
                            root_binding=root_binding, attempt_name=attempt_name,
                            temp_root=temp_root, failure=RuntimeError("original product failure"),
                            result=None,
                            prep_process=_FakePrepProcess(
                                running=True, kill_fault=OSError("kill fault"),
                            ),
                            release_signal=None,
                        )
                finally:
                    _close_evidence_root_handle(root_binding)
                self.assertFalse(temp_root.exists())
                published = evidence_root / attempt_name
                retained = tuple(
                    path.relative_to(published).as_posix()
                    for path in published.rglob("*") if path.is_file()
                )
                self.assertEqual(("REAL_AGENT_TEST_RESULT.json",), retained)
                record = json.loads((published / retained[0]).read_text(encoding="utf-8"))
                self.assertEqual("FAIL", record["status"])
                self.assertEqual("RuntimeError", record["failure_type"])
                self.assertEqual("OSError", record["preparation_cleanup_failure_type"])
                self.assertTrue(record["host_temp_cleanup_complete"])
            finally:
                if temp_root.exists():
                    shutil.rmtree(temp_root, ignore_errors=True)

    def test_real_agent_attempt_identity_capture_and_revalidation(self) -> None:
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-identity-") as raw:
            root = Path(raw)
            first = root / "first"
            first.mkdir()
            binding = _open_evidence_root_handle(first)
            try:
                self.assertEqual(str(first), binding["path"])
                self.assertTrue(binding["volume_serial"])
                self.assertTrue(binding["file_index"])
                _revalidate_evidence_root_binding(binding)
                captured = _capture_directory_identity(first)
                self.assertEqual(binding["volume_serial"], captured["volume_serial"])
                self.assertEqual(binding["file_index"], captured["file_index"])
                second = root / "second"
                second.mkdir()
                other = _open_evidence_root_handle(second)
                try:
                    self.assertNotEqual(
                        (binding["volume_serial"], binding["file_index"]),
                        (other["volume_serial"], other["file_index"]),
                    )
                finally:
                    _close_evidence_root_handle(other)
                with self.assertRaises(PermissionError):
                    os.rename(first, root / "first-moved")
                with self.assertRaises(PermissionError):
                    os.rmdir(first)
                with self.assertRaises(PermissionError):
                    os.replace(first, root / "first-replaced")
            finally:
                _close_evidence_root_handle(binding)
            with self.assertRaisesRegex(RuntimeError, "already closed"):
                _revalidate_evidence_root_binding(binding)
            os.rename(first, root / "first-moved")
            first.mkdir()
            replacement = _open_evidence_root_handle(first)
            try:
                self.assertNotEqual(
                    (binding["volume_serial"], binding["file_index"]),
                    (replacement["volume_serial"], replacement["file_index"]),
                )
            finally:
                _close_evidence_root_handle(replacement)

    def test_real_agent_attempt_root_replacement_fails_closed(self) -> None:
        sentinel = "OUTSIDE_SENTINEL_MARKER"
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-replacement-") as raw:
            root = Path(raw)
            temp_root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-"))
            try:
                evidence_root = root / "evidence"
                evidence_root.mkdir()
                outside = root / "outside-target"
                outside.mkdir()
                (outside / "sentinel.txt").write_text(sentinel, encoding="utf-8")
                root_binding = _open_evidence_root_handle(evidence_root)
                attempt_name = "attempt"
                observed: list[str] = []

                def interpose(binding, attempt_name, staging):
                    bound = Path(binding["path"])
                    try:
                        os.rename(bound, Path(str(bound) + "-moved"))
                    except PermissionError:
                        observed.append("rename-denied")
                    try:
                        os.rmdir(bound)
                    except PermissionError:
                        observed.append("rmdir-denied")
                    try:
                        _make_directory_link(bound, outside)
                    except (OSError, RuntimeError):
                        observed.append("link-refused")

                record = {
                    "schema": "orchestrator-real-agent-evidence/v1",
                    "status": "PASS",
                    "retained_file_count": 1,
                }
                try:
                    with patch.object(
                        real_agent_test_module, "_PUBLICATION_INTERPOSITION_HOOK", interpose,
                    ):
                        published = _terminal_finalize(
                            root_binding=root_binding, attempt_name=attempt_name,
                            temp_root=temp_root, failure=None, result=record,
                            prep_process=None, release_signal=None,
                        )
                finally:
                    _close_evidence_root_handle(root_binding)
                self.assertEqual(
                    ["rename-denied", "rmdir-denied", "link-refused"], observed,
                )
                self.assertEqual(sentinel, (outside / "sentinel.txt").read_text(encoding="utf-8"))
                self.assertFalse((outside / "REAL_AGENT_TEST_RESULT.json").exists())
                self.assertEqual(["sentinel.txt"], [p.name for p in outside.iterdir()])
                self.assertEqual(evidence_root / attempt_name, published)
                retained = tuple(
                    path.relative_to(published).as_posix()
                    for path in published.rglob("*") if path.is_file()
                )
                self.assertEqual(("REAL_AGENT_TEST_RESULT.json",), retained)
            finally:
                if temp_root.exists():
                    shutil.rmtree(temp_root, ignore_errors=True)

    def test_real_agent_final_check_use_interval_refuses_staging_substitution(self) -> None:
        sentinel = "FINAL_CHECK_USE_SENTINEL"
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-final-check-") as raw:
            root = Path(raw)
            evidence_root = root / "evidence"
            evidence_root.mkdir()
            outside = root / "outside-target"
            outside.mkdir()
            (outside / "sentinel.txt").write_text(sentinel, encoding="utf-8")
            temp_root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-"))
            try:
                root_binding = _open_evidence_root_handle(evidence_root)
                attempt_name = "attempt"
                observed: list[str] = []

                def interpose(binding, attempt_name, staging):
                    staging_path = Path(staging["path"])
                    moved = Path(str(staging_path) + "-moved")
                    try:
                        os.rename(staging_path, moved)
                        observed.append("rename-allowed")
                    except PermissionError:
                        observed.append("rename-refused")
                    try:
                        _make_directory_link(staging_path, outside)
                        observed.append("junction-allowed")
                    except (OSError, RuntimeError):
                        observed.append("junction-refused")
                    try:
                        os.rmdir(staging_path)
                        observed.append("rmdir-allowed")
                    except PermissionError:
                        observed.append("rmdir-refused")

                record = {
                    "schema": "orchestrator-real-agent-evidence/v1",
                    "status": "PASS",
                    "retained_file_count": 1,
                }
                try:
                    with patch.object(
                        real_agent_test_module, "_PUBLICATION_INTERPOSITION_HOOK", interpose,
                    ):
                        published = _terminal_finalize(
                            root_binding=root_binding, attempt_name=attempt_name,
                            temp_root=temp_root, failure=None, result=record,
                            prep_process=None, release_signal=None,
                        )
                finally:
                    _close_evidence_root_handle(root_binding)
                self.assertEqual(
                    ["rename-refused", "junction-refused", "rmdir-refused"], observed,
                )
                self.assertEqual(evidence_root / attempt_name, published)
                retained = tuple(
                    path.relative_to(published).as_posix()
                    for path in published.rglob("*") if path.is_file()
                )
                self.assertEqual(("REAL_AGENT_TEST_RESULT.json",), retained)
                text = (published / retained[0]).read_text(encoding="utf-8")
                self.assertNotIn(sentinel, text)
                self.assertEqual(sentinel, (outside / "sentinel.txt").read_text(encoding="utf-8"))
                self.assertEqual(["sentinel.txt"], [p.name for p in outside.iterdir()])
            finally:
                if temp_root.exists():
                    shutil.rmtree(temp_root, ignore_errors=True)

    def test_real_agent_no_retained_tree_recursive_cleanup(self) -> None:
        source = (
            REPOSITORY_ROOT / "orchestrator_harness" / "tests" / "real_agent_test.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("def _remove_attempt_entry(", source)
        self.assertNotIn("evidence.rglob(", source)
        self.assertNotIn("evidence.iterdir(", source)
        self.assertNotIn("_json(evidence /", source)
        self.assertNotIn("shutil.rmtree(evidence", source)
        self.assertNotIn("os.replace(evidence", source)
        self.assertNotIn("def _remove_private_staging_root(", source)
        self.assertNotIn("staging.resolve(", source)
        self.assertNotIn("shutil.rmtree(staging", source)
        self.assertNotIn("os.walk(staging", source)
        self.assertNotIn("_capture_directory_identity(staging", source)
        self.assertNotIn("_verify_regular_record_file(", source)
        self.assertIn('evidence_dir = temp_root / "prepared-evidence"', source)
        self.assertIn("def _build_staging_evidence(", source)
        self.assertIn("def _dispose_staging_evidence(", source)
        self.assertIn("def _rename_attempt_relative(", source)
        self.assertIn("_FILE_RENAME_INFORMATION.FileName.offset", source)
        self.assertIn("def _enumerate_directory_handle(", source)
        self.assertIn("_CLEANUP_INTERPOSITION_HOOK", source)
        self.assertIn("_PRE_FINAL_STAGING_VALIDATION_HOOK", source)
        self.assertIn("NtSetInformationFile", source)

    def test_real_agent_destination_collision_fails_closed(self) -> None:
        sentinel = "DEST_COLLISION_SENTINEL"
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-collision-") as raw:
            root = Path(raw)
            temp_root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-"))
            try:
                evidence_root = root / "evidence"
                evidence_root.mkdir()
                root_binding = _open_evidence_root_handle(evidence_root)
                attempt_name = "attempt"
                precreated: Path | None = None

                def interpose(binding, attempt_name, staging):
                    nonlocal precreated
                    precreated = Path(binding["path"]) / attempt_name
                    precreated.mkdir()
                    (precreated / "marker.txt").write_text(sentinel, encoding="utf-8")

                record = {
                    "schema": "orchestrator-real-agent-evidence/v1",
                    "status": "PASS",
                    "retained_file_count": 1,
                }
                try:
                    with patch.object(
                        real_agent_test_module, "_PUBLICATION_INTERPOSITION_HOOK", interpose,
                    ):
                        with self.assertRaisesRegex(RuntimeError, "already exists"):
                            _terminal_finalize(
                                root_binding=root_binding, attempt_name=attempt_name,
                                temp_root=temp_root, failure=None, result=record,
                                prep_process=None, release_signal=None,
                            )
                finally:
                    _close_evidence_root_handle(root_binding)
                self.assertIsNotNone(precreated)
                self.assertEqual(
                    sentinel, (precreated / "marker.txt").read_text(encoding="utf-8")
                )
                self.assertFalse((precreated / "REAL_AGENT_TEST_RESULT.json").exists())
                self.assertEqual(["marker.txt"], [p.name for p in precreated.iterdir()])
            finally:
                if temp_root.exists():
                    shutil.rmtree(temp_root, ignore_errors=True)

    def test_real_agent_destination_reparse_collision_fails_closed(self) -> None:
        sentinel = "DEST_REPARSE_SENTINEL"
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-collision-") as raw:
            root = Path(raw)
            temp_root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-"))
            try:
                evidence_root = root / "evidence"
                evidence_root.mkdir()
                outside = root / "outside-target"
                outside.mkdir()
                (outside / "sentinel.txt").write_text(sentinel, encoding="utf-8")
                root_binding = _open_evidence_root_handle(evidence_root)
                attempt_name = "attempt"
                destination = evidence_root / attempt_name

                def interpose(binding, attempt_name, staging):
                    _make_directory_link(Path(binding["path"]) / attempt_name, outside)

                record = {
                    "schema": "orchestrator-real-agent-evidence/v1",
                    "status": "PASS",
                    "retained_file_count": 1,
                }
                try:
                    with patch.object(
                        real_agent_test_module, "_PUBLICATION_INTERPOSITION_HOOK", interpose,
                    ):
                        with self.assertRaisesRegex(RuntimeError, "already exists"):
                            _terminal_finalize(
                                root_binding=root_binding, attempt_name=attempt_name,
                                temp_root=temp_root, failure=None, result=record,
                                prep_process=None, release_signal=None,
                            )
                finally:
                    _close_evidence_root_handle(root_binding)
                self.assertTrue(_is_directory_link(destination))
                self.assertEqual(sentinel, (outside / "sentinel.txt").read_text(encoding="utf-8"))
                self.assertFalse((outside / "REAL_AGENT_TEST_RESULT.json").exists())
                self.assertEqual(["sentinel.txt"], [p.name for p in outside.iterdir()])
            finally:
                if _is_directory_link(evidence_root / attempt_name):
                    (evidence_root / attempt_name).unlink()
                if temp_root.exists():
                    shutil.rmtree(temp_root, ignore_errors=True)

    def test_real_agent_staging_record_substitution_is_detected_and_fails_closed(self) -> None:
        sentinel = "STAGING_RECORD_SUBSTITUTION_SENTINEL"
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-staging-record-") as raw:
            root = Path(raw)
            evidence_root = root / "evidence"
            evidence_root.mkdir()
            outside = root / "outside-target"
            outside.mkdir()
            (outside / "sentinel.txt").write_text(sentinel, encoding="utf-8")
            temp_root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-"))
            staging_path: Path | None = None
            try:
                root_binding = _open_evidence_root_handle(evidence_root)
                attempt_name = "attempt"

                def interpose(staging):
                    nonlocal staging_path
                    staging_path = Path(staging["path"])
                    record_path = staging_path / "REAL_AGENT_TEST_RESULT.json"
                    record_path.unlink()
                    _make_directory_link(record_path, outside)

                record = {
                    "schema": "orchestrator-real-agent-evidence/v1",
                    "status": "PASS",
                    "retained_file_count": 1,
                }
                try:
                    with patch.object(
                        real_agent_test_module, "_PRE_FINAL_STAGING_VALIDATION_HOOK", interpose,
                    ):
                        with self.assertRaisesRegex(RuntimeError, "cleanup failed closed"):
                            _terminal_finalize(
                                root_binding=root_binding, attempt_name=attempt_name,
                                temp_root=temp_root, failure=None, result=record,
                                prep_process=None, release_signal=None,
                            )
                finally:
                    _close_evidence_root_handle(root_binding)
                self.assertIsNotNone(staging_path)
                self.assertTrue(staging_path.exists())
                self.assertTrue(
                    _is_directory_link(staging_path / "REAL_AGENT_TEST_RESULT.json")
                )
                self.assertEqual(sentinel, (outside / "sentinel.txt").read_text(encoding="utf-8"))
                self.assertFalse((outside / "REAL_AGENT_TEST_RESULT.json").exists())
                self.assertEqual(["sentinel.txt"], [p.name for p in outside.iterdir()])
                self.assertFalse((evidence_root / attempt_name).exists())
                self.assertFalse(
                    (evidence_root / attempt_name / "REAL_AGENT_TEST_RESULT.json").exists()
                )
            finally:
                if staging_path is not None and staging_path.exists():
                    record_link = staging_path / "REAL_AGENT_TEST_RESULT.json"
                    if _is_directory_link(record_link):
                        record_link.unlink()
                    shutil.rmtree(staging_path, ignore_errors=True)
                if temp_root.exists():
                    shutil.rmtree(temp_root, ignore_errors=True)

    def test_real_agent_publication_event_order_verifies_then_hooks_then_renames(self) -> None:
        module = real_agent_test_module
        record = {
            "schema": "orchestrator-real-agent-evidence/v1",
            "status": "PASS",
            "retained_file_count": 1,
        }
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-event-order-") as raw:
            root = Path(raw)
            evidence_root = root / "evidence"
            evidence_root.mkdir()
            temp_root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-"))
            order: list[str] = []
            try:
                root_binding = _open_evidence_root_handle(evidence_root)
                real_verify = module._verify_staging_via_handles
                real_rename = module._rename_attempt_relative

                def tracked_verify(binding):
                    order.append("final-verify")
                    real_verify(binding)

                def tracked_rename(source_handle, root_handle, attempt_name):
                    order.append("native-rename")
                    real_rename(source_handle, root_handle, attempt_name)

                def pre_final_hook(staging):
                    order.append("pre-final-hook")

                def publication_hook(binding, attempt_name, staging):
                    order.append("publication-hook")

                try:
                    with patch.object(module, "_verify_staging_via_handles", tracked_verify), \
                         patch.object(module, "_rename_attempt_relative", tracked_rename), \
                         patch.object(module, "_PRE_FINAL_STAGING_VALIDATION_HOOK", pre_final_hook), \
                         patch.object(module, "_PUBLICATION_INTERPOSITION_HOOK", publication_hook):
                        published = _terminal_finalize(
                            root_binding=root_binding, attempt_name="attempt",
                            temp_root=temp_root, failure=None, result=record,
                            prep_process=None, release_signal=None,
                        )
                finally:
                    _close_evidence_root_handle(root_binding)
                self.assertEqual(
                    ["pre-final-hook", "final-verify", "publication-hook", "native-rename"],
                    order,
                )
                self.assertEqual(evidence_root / "attempt", published)
                retained = tuple(
                    path.relative_to(published).as_posix()
                    for path in published.rglob("*") if path.is_file()
                )
                self.assertEqual(("REAL_AGENT_TEST_RESULT.json",), retained)
            finally:
                if temp_root.exists():
                    shutil.rmtree(temp_root, ignore_errors=True)

    def test_real_agent_file_rename_information_layout_derives_native_offset(self) -> None:
        module = real_agent_test_module
        self.assertEqual(
            module._FILE_RENAME_INFO_HEADER_SIZE,
            module._FILE_RENAME_INFORMATION.FileName.offset,
        )
        self.assertLess(
            module._FILE_RENAME_INFO_HEADER_SIZE,
            ctypes.sizeof(module._FILE_RENAME_INFORMATION),
        )

        class _RenameInfo32(ctypes.Structure):
            _fields_ = [
                ("ReplaceIfExists", wintypes.BOOL),
                ("RootDirectory", ctypes.c_uint32),
                ("FileNameLength", wintypes.DWORD),
                ("FileName", wintypes.WCHAR * 1),
            ]

        # Fixed-width c_uint64 keeps the synthetic 64-bit pointer member 8
        # bytes on any host; c_void_p would collapse to 4 bytes on 32-bit.
        class _RenameInfo64(ctypes.Structure):
            _fields_ = [
                ("ReplaceIfExists", wintypes.BOOL),
                ("RootDirectory", ctypes.c_uint64),
                ("FileNameLength", wintypes.DWORD),
                ("FileName", wintypes.WCHAR * 1),
            ]

        self.assertEqual(12, _RenameInfo32.FileName.offset)
        self.assertEqual(20, _RenameInfo64.FileName.offset)
        pointer_width = ctypes.sizeof(ctypes.c_void_p)
        self.assertIn(pointer_width, (4, 8))
        expected = 12 if pointer_width == 4 else 20
        self.assertEqual(expected, module._FILE_RENAME_INFO_HEADER_SIZE)
        self.assertEqual(module._FILE_RENAME_INFORMATION.FileName.offset, expected)

    def test_real_agent_long_attempt_name_reaches_native_publication(self) -> None:
        module = real_agent_test_module
        long_name = "a" * 240
        huge_name = "b" * 300
        self.assertEqual(long_name, module._validate_attempt_name(long_name))
        self.assertEqual(huge_name, module._validate_attempt_name(huge_name))
        record = {
            "schema": "orchestrator-real-agent-evidence/v1",
            "status": "PASS",
            "retained_file_count": 1,
        }
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-long-name-") as raw:
            root = Path(raw)
            evidence_root = root / "evidence"
            evidence_root.mkdir()
            temp_root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-"))
            try:
                root_binding = _open_evidence_root_handle(evidence_root)
                try:
                    published = _terminal_finalize(
                        root_binding=root_binding, attempt_name=long_name,
                        temp_root=temp_root, failure=None, result=record,
                        prep_process=None, release_signal=None,
                    )
                finally:
                    _close_evidence_root_handle(root_binding)
                self.assertEqual(evidence_root / long_name, published)
                self.assertTrue(published.is_dir())
                retained = tuple(
                    path.relative_to(published).as_posix()
                    for path in published.rglob("*") if path.is_file()
                )
                self.assertEqual(("REAL_AGENT_TEST_RESULT.json",), retained)

                root_binding = _open_evidence_root_handle(evidence_root)
                try:
                    with self.assertRaisesRegex(RuntimeError, "publication failed closed"):
                        _terminal_finalize(
                            root_binding=root_binding, attempt_name=huge_name,
                            temp_root=temp_root, failure=None, result=record,
                            prep_process=None, release_signal=None,
                        )
                finally:
                    _close_evidence_root_handle(root_binding)
                self.assertFalse((evidence_root / huge_name).exists())
            finally:
                if temp_root.exists():
                    shutil.rmtree(temp_root, ignore_errors=True)

    def test_real_agent_cleanup_refuses_staging_substitution_and_disposes_exact(self) -> None:
        sentinel = "CLEANUP_SWAP_SENTINEL"
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-cleanup-swap-") as raw:
            root = Path(raw)
            evidence_root = root / "evidence"
            evidence_root.mkdir()
            outside = root / "outside-target"
            outside.mkdir()
            (outside / "sentinel.txt").write_text(sentinel, encoding="utf-8")
            temp_root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-"))
            cleanup_refusals: list[str] = []
            staging_path: Path | None = None
            try:
                root_binding = _open_evidence_root_handle(evidence_root)
                attempt_name = "attempt"

                def publication_interpose(binding, attempt_name, staging):
                    destination = Path(binding["path"]) / attempt_name
                    destination.mkdir()
                    (destination / "marker.txt").write_text(sentinel, encoding="utf-8")

                def cleanup_interpose(staging):
                    nonlocal staging_path
                    staging_path = Path(staging["path"])
                    moved = Path(str(staging_path) + "-moved")
                    try:
                        os.rename(staging_path, moved)
                        cleanup_refusals.append("rename-allowed")
                    except PermissionError:
                        cleanup_refusals.append("rename-refused")
                    try:
                        _make_directory_link(staging_path, outside)
                        cleanup_refusals.append("junction-allowed")
                    except (OSError, RuntimeError):
                        cleanup_refusals.append("junction-refused")

                record = {
                    "schema": "orchestrator-real-agent-evidence/v1",
                    "status": "PASS",
                    "retained_file_count": 1,
                }
                try:
                    with patch.object(
                        real_agent_test_module, "_PUBLICATION_INTERPOSITION_HOOK", publication_interpose,
                    ), patch.object(
                        real_agent_test_module, "_CLEANUP_INTERPOSITION_HOOK", cleanup_interpose,
                    ):
                        with self.assertRaisesRegex(RuntimeError, "already exists"):
                            _terminal_finalize(
                                root_binding=root_binding, attempt_name=attempt_name,
                                temp_root=temp_root, failure=None, result=record,
                                prep_process=None, release_signal=None,
                            )
                finally:
                    _close_evidence_root_handle(root_binding)
                self.assertEqual(["rename-refused", "junction-refused"], cleanup_refusals)
                self.assertIsNotNone(staging_path)
                self.assertFalse(staging_path.exists())
                self.assertEqual(sentinel, (outside / "sentinel.txt").read_text(encoding="utf-8"))
                self.assertEqual(["sentinel.txt"], [p.name for p in outside.iterdir()])
                self.assertEqual(
                    sentinel,
                    (evidence_root / attempt_name / "marker.txt").read_text(encoding="utf-8"),
                )
                self.assertFalse(
                    (evidence_root / attempt_name / "REAL_AGENT_TEST_RESULT.json").exists()
                )
            finally:
                if temp_root.exists():
                    shutil.rmtree(temp_root, ignore_errors=True)

    def test_real_agent_cleanup_leaves_untouched_when_stage_gains_entries(self) -> None:
        sentinel = "CLEANUP_ENTRY_SENTINEL"
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-cleanup-entry-") as raw:
            root = Path(raw)
            evidence_root = root / "evidence"
            evidence_root.mkdir()
            outside = root / "outside-target"
            outside.mkdir()
            (outside / "sentinel.txt").write_text(sentinel, encoding="utf-8")
            temp_root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-"))
            staging_path: Path | None = None
            try:
                root_binding = _open_evidence_root_handle(evidence_root)
                attempt_name = "attempt"

                def publication_interpose(binding, attempt_name, staging):
                    destination = Path(binding["path"]) / attempt_name
                    destination.mkdir()
                    (destination / "marker.txt").write_text(sentinel, encoding="utf-8")

                def cleanup_interpose(staging):
                    nonlocal staging_path
                    staging_path = Path(staging["path"])
                    (staging_path / "attacker.txt").write_text(sentinel, encoding="utf-8")

                record = {
                    "schema": "orchestrator-real-agent-evidence/v1",
                    "status": "PASS",
                    "retained_file_count": 1,
                }
                try:
                    with patch.object(
                        real_agent_test_module, "_PUBLICATION_INTERPOSITION_HOOK", publication_interpose,
                    ), patch.object(
                        real_agent_test_module, "_CLEANUP_INTERPOSITION_HOOK", cleanup_interpose,
                    ):
                        with self.assertRaisesRegex(RuntimeError, "cleanup failed closed"):
                            _terminal_finalize(
                                root_binding=root_binding, attempt_name=attempt_name,
                                temp_root=temp_root, failure=None, result=record,
                                prep_process=None, release_signal=None,
                            )
                finally:
                    _close_evidence_root_handle(root_binding)
                self.assertIsNotNone(staging_path)
                self.assertTrue(staging_path.exists())
                self.assertEqual(
                    {"REAL_AGENT_TEST_RESULT.json", "attacker.txt"},
                    {p.name for p in staging_path.iterdir()},
                )
                self.assertEqual(
                    sentinel, (staging_path / "attacker.txt").read_text(encoding="utf-8")
                )
                self.assertEqual(sentinel, (outside / "sentinel.txt").read_text(encoding="utf-8"))
                self.assertEqual(["sentinel.txt"], [p.name for p in outside.iterdir()])
                self.assertEqual(
                    sentinel,
                    (evidence_root / attempt_name / "marker.txt").read_text(encoding="utf-8"),
                )
            finally:
                if staging_path is not None and staging_path.exists():
                    shutil.rmtree(staging_path, ignore_errors=True)
                if temp_root.exists():
                    shutil.rmtree(temp_root, ignore_errors=True)

    def test_real_agent_cleanup_hook_fault_closes_handle_and_leaves_stage_exact(self) -> None:
        sentinel = "CLEANUP_HOOK_FAULT_SENTINEL"
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-cleanup-hook-fault-") as raw:
            root = Path(raw)
            evidence_root = root / "evidence"
            evidence_root.mkdir()
            outside = root / "outside-target"
            outside.mkdir()
            (outside / "sentinel.txt").write_text(sentinel, encoding="utf-8")
            temp_root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-"))
            staging_path: Path | None = None
            captured: list[dict[str, object]] = []
            try:
                root_binding = _open_evidence_root_handle(evidence_root)
                attempt_name = "attempt"

                def publication_interpose(binding, attempt_name, staging):
                    destination = Path(binding["path"]) / attempt_name
                    destination.mkdir()
                    (destination / "marker.txt").write_text(sentinel, encoding="utf-8")

                def cleanup_interpose(staging):
                    nonlocal staging_path
                    staging_path = Path(staging["path"])
                    captured.append(staging)
                    raise RuntimeError("simulated cleanup interposition fault")

                record = {
                    "schema": "orchestrator-real-agent-evidence/v1",
                    "status": "PASS",
                    "retained_file_count": 1,
                }
                try:
                    with patch.object(
                        real_agent_test_module, "_PUBLICATION_INTERPOSITION_HOOK", publication_interpose,
                    ), patch.object(
                        real_agent_test_module, "_CLEANUP_INTERPOSITION_HOOK", cleanup_interpose,
                    ):
                        with self.assertRaisesRegex(RuntimeError, "cleanup failed closed"):
                            _terminal_finalize(
                                root_binding=root_binding, attempt_name=attempt_name,
                                temp_root=temp_root, failure=None, result=record,
                                prep_process=None, release_signal=None,
                            )
                finally:
                    _close_evidence_root_handle(root_binding)
                self.assertEqual(1, len(captured))
                self.assertIsNone(captured[0]["handle"])
                self.assertIsNotNone(staging_path)
                self.assertTrue(staging_path.exists())
                self.assertEqual(
                    ["REAL_AGENT_TEST_RESULT.json"],
                    [p.name for p in staging_path.iterdir()],
                )
                retained_text = (
                    staging_path / "REAL_AGENT_TEST_RESULT.json"
                ).read_text(encoding="utf-8")
                self.assertNotIn(sentinel, retained_text)
                self.assertEqual(
                    "orchestrator-real-agent-evidence/v1",
                    json.loads(retained_text)["schema"],
                )
                self.assertEqual(sentinel, (outside / "sentinel.txt").read_text(encoding="utf-8"))
                self.assertEqual(["sentinel.txt"], [p.name for p in outside.iterdir()])
                self.assertEqual(
                    sentinel,
                    (evidence_root / attempt_name / "marker.txt").read_text(encoding="utf-8"),
                )
                self.assertFalse(
                    (evidence_root / attempt_name / "REAL_AGENT_TEST_RESULT.json").exists()
                )
            finally:
                if staging_path is not None and staging_path.exists():
                    shutil.rmtree(staging_path, ignore_errors=True)
                if temp_root.exists():
                    shutil.rmtree(temp_root, ignore_errors=True)

    def test_real_agent_handles_close_on_every_terminal_route(self) -> None:
        module = real_agent_test_module
        record = {
            "schema": "orchestrator-real-agent-evidence/v1",
            "status": "PASS",
            "retained_file_count": 1,
        }
        for scenario in ("success", "failure"):
            with self.subTest(route=scenario):
                opened: list[int] = []
                closed: list[int] = []
                real_open_dir = module._open_directory_handle
                real_open_file = module._open_regular_file_handle
                real_close = module._close_handle

                def track_open_dir(path, *, access, share):
                    handle = real_open_dir(path, access=access, share=share)
                    opened.append(handle)
                    return handle

                def track_open_file(path, *, access, share):
                    handle = real_open_file(path, access=access, share=share)
                    opened.append(handle)
                    return handle

                def track_close(handle):
                    if handle is not None and handle != module._INVALID_HANDLE_VALUE:
                        closed.append(handle)
                    return real_close(handle)

                with tempfile.TemporaryDirectory(prefix="orchestrator-s6-closure-") as raw:
                    root = Path(raw)
                    evidence_root = root / "evidence"
                    evidence_root.mkdir()
                    temp_root = Path(tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-"))
                    try:
                        with patch.object(module, "_open_directory_handle", side_effect=track_open_dir), \
                             patch.object(module, "_open_regular_file_handle", side_effect=track_open_file), \
                             patch.object(module, "_close_handle", side_effect=track_close):
                            root_binding = _open_evidence_root_handle(evidence_root)
                            try:
                                if scenario == "success":
                                    published = _terminal_finalize(
                                        root_binding=root_binding, attempt_name="attempt",
                                        temp_root=temp_root, failure=None, result=record,
                                        prep_process=None, release_signal=None,
                                    )
                                    self.assertTrue(
                                        (published / "REAL_AGENT_TEST_RESULT.json").is_file()
                                    )
                                else:
                                    with self.assertRaisesRegex(RuntimeError, "original product failure"):
                                        _terminal_finalize(
                                            root_binding=root_binding, attempt_name="attempt",
                                            temp_root=temp_root, failure=RuntimeError("original product failure"),
                                            result=None, prep_process=None, release_signal=None,
                                        )
                            finally:
                                _close_evidence_root_handle(root_binding)
                    finally:
                        if temp_root.exists():
                            shutil.rmtree(temp_root, ignore_errors=True)
                self.assertTrue(opened)
                self.assertEqual(sorted(opened), sorted(closed))

    def test_real_agent_host_temp_cleanup_is_exact_and_complete(self) -> None:
        disposable = Path(
            tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-cleanup-")
        )
        read_only = disposable / "nested" / "provider-output.jsonl"
        try:
            (disposable / "nested").mkdir()
            read_only.write_text(
                "disposable\n", encoding="utf-8"
            )
            os.chmod(read_only, 0o444)
            remove_tree = shutil.rmtree
            attempts = 0

            def transient_lock(path, **options):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise PermissionError("simulated transient Windows lock")
                remove_tree(path, **options)

            with patch(
                "orchestrator_harness.tests.real_agent_test.shutil.rmtree",
                side_effect=transient_lock,
            ):
                _remove_disposable_temp_root(disposable)
            self.assertFalse(disposable.exists())
            self.assertEqual(2, attempts)
        finally:
            if disposable.exists():
                if read_only.exists():
                    os.chmod(read_only, 0o666)
                shutil.rmtree(disposable)
        with tempfile.TemporaryDirectory(
            prefix="orchestrator-s6-unrelated-"
        ) as unrelated, self.assertRaisesRegex(
            RuntimeError, "unexpected real-agent"
        ):
            _remove_disposable_temp_root(Path(unrelated))
        legacy = Path(
            tempfile.mkdtemp(prefix="orchestrator-s6-real-agent-legacy-cleanup-")
        )
        legacy_file = legacy / "read-only.jsonl"
        try:
            legacy_file.write_text("disposable\n", encoding="utf-8")
            os.chmod(legacy_file, 0o444)
            with patch(
                "orchestrator_harness.tests.real_agent_test.sys.version_info",
                (3, 11),
            ):
                _remove_disposable_temp_root(legacy)
            self.assertFalse(legacy.exists())
        finally:
            if legacy.exists():
                if legacy_file.exists():
                    os.chmod(legacy_file, 0o666)
                shutil.rmtree(legacy)

    def test_real_agent_uses_native_public_route_and_native_evidence(self) -> None:
        host_driver = (
            REPOSITORY_ROOT / "orchestrator_harness" / "tests" / "real_agent_test.py"
        ).read_text(encoding="utf-8")
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
        self.assertIn("public_launch -> operator_launch -> lane_controller", host_driver)
        self.assertIn("launch_lane_controller", host_driver)
        self.assertIn("wsl.exe", host_driver)
        self.assertIn("prepared-state", driver)
        self.assertIn("PREPARED_STATE_SCHEMA", driver)
        self.assertIn("claim_prepared_state", provider)
        self.assertNotIn("wsl_public_route_entry.py", driver + host_driver)
        self.assertNotIn("synthetic_controller.status", driver + host_driver)
        self.assertNotIn("/opt/codex/bin/codex", driver)
        self.assertLess(driver.index("chown_tree(workspace)"), driver.index("isolation = preflight("))
        self.assertIn("validate_cross_os_identity_relation", host_driver)
        self.assertIn("discover_codex", driver)
        self.assertIn("validate_prepared_state", driver)
        self.assertNotIn("launch_lane_controller", route)
        self.assertIn("Windows-native", route)
        self.assertNotIn("ORCH_HARNESS_POSIX_MUTATION_ROOTS", driver + route + mutation)
        self.assertNotIn("safe.directory", driver + route)
        self.assertIn("os.execv", provider)
        self.assertIn("/opt/codex/bin/codex", provider)
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
        validate_cross_os_identity_relation(
            {"platform": "windows", "pid": 17, "created_utc": "host-created", "nonce": "n", "invocation_id": "i"},
            {"platform": "linux", "pid": 17, "created_utc": "linux-created", "nonce": "n", "invocation_id": "i"},
            nonce="n", invocation_id="i",
        )
        with self.assertRaisesRegex(RuntimeError, "nonce"):
            validate_cross_os_identity_relation(
                {"platform": "windows", "pid": 17, "created_utc": "host-created", "nonce": "n", "invocation_id": "i"},
                {"platform": "linux", "pid": 18, "created_utc": "linux-created", "nonce": "different", "invocation_id": "i"},
                nonce="n", invocation_id="i",
            )

    def test_prepared_state_is_one_use_and_rejects_negative_identity(self) -> None:
        state = {
            "schema": PREPARED_STATE_SCHEMA, "status": "READY", "consumed": False,
            "run_id": "a" * 32, "nonce": "b" * 64, "invocation_id": "invocation",
            "cgroup": "/sys/fs/cgroup/orchestrator-harness-aaaaaaaa",
            "network_namespace": "oh-aaaaaaaa", "proxy_url": "http://10.0.0.1:1",
            "workspace": "/mnt/c/synthetic", "codex_home": "/tmp/codex-home",
            "release": "/opt/codex", "pinned_codex": "/opt/codex/bin/codex",
            "bwrap": "/opt/codex/codex-resources/bwrap",
            "cgroup_launcher": "/mnt/c/cgroup_exec.py", "release_signal": "/mnt/c/release",
        }
        self.assertEqual(state, validate_prepared_state(state, nonce="b" * 64, invocation_id="invocation"))
        with self.assertRaisesRegex(RuntimeError, "nonce"):
            validate_prepared_state(state, nonce="c" * 64, invocation_id="invocation")
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-claim-") as raw:
            claim = Path(raw) / "claim.json"
            first = claim_prepared_state(claim, nonce="b" * 64, invocation_id="invocation")
            self.assertEqual("orchestrator-wsl-prepared-claim/v1", first["schema"])
            with self.assertRaisesRegex(RuntimeError, "more than once"):
                claim_prepared_state(claim, nonce="b" * 64, invocation_id="invocation")

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

    def test_prepared_protocol_state_machine_rejects_foreign_or_reused_state(self) -> None:
        state = {
            "schema": PREPARED_STATE_SCHEMA, "status": "READY", "consumed": False,
            "run_id": "a" * 32, "nonce": "b" * 64, "invocation_id": "invocation",
            "cgroup": "/sys/fs/cgroup/orchestrator-harness-aaaaaaaa",
            "network_namespace": "oh-aaaaaaaa", "proxy_url": "http://10.0.0.1:1",
            "workspace": "/mnt/c/synthetic", "codex_home": "/tmp/codex-home",
            "release": "/opt/codex", "pinned_codex": "/opt/codex/bin/codex",
            "bwrap": "/opt/codex/codex-resources/bwrap",
            "cgroup_launcher": "/mnt/c/cgroup_exec.py", "release_signal": "/mnt/c/release",
        }
        with self.assertRaisesRegex(RuntimeError, "invocation"):
            validate_prepared_state(state, nonce="b" * 64, invocation_id="foreign")
        with self.assertRaisesRegex(RuntimeError, "consumed"):
            validate_prepared_state(dict(state, consumed=True), nonce="b" * 64, invocation_id="invocation")
        with self.assertRaisesRegex(RuntimeError, "not READY"):
            validate_prepared_state(dict(state, status="FAILED"), nonce="b" * 64, invocation_id="invocation")
        with self.assertRaisesRegex(RuntimeError, "namespace"):
            validate_prepared_state(dict(state, network_namespace="oh-zzzzzzzz"), nonce="b" * 64, invocation_id="invocation")
        with self.assertRaisesRegex(RuntimeError, "namespace"):
            validate_prepared_state(dict(state, run_id="z" * 32), nonce="b" * 64, invocation_id="invocation")

    def test_cross_os_identity_relation_never_asserts_pid_equality(self) -> None:
        host = {"platform": "windows", "pid": 17, "created_utc": "host-created", "nonce": "n", "invocation_id": "i"}
        linux = {"platform": "linux", "pid": 17, "created_utc": "linux-created", "nonce": "n", "invocation_id": "i"}
        # Equal PIDs across the two domains are deliberately not a relation:
        # the nonce and invocation are the only binding, so equality neither
        # validates nor invalidates the pairing.
        validate_cross_os_identity_relation(host, linux, nonce="n", invocation_id="i")
        validate_cross_os_identity_relation(dict(host, pid=18), linux, nonce="n", invocation_id="i")
        with self.assertRaisesRegex(RuntimeError, "nonce"):
            validate_cross_os_identity_relation(host, dict(linux, nonce="foreign"), nonce="n", invocation_id="i")
        with self.assertRaisesRegex(RuntimeError, "invocation"):
            validate_cross_os_identity_relation(host, dict(linux, invocation_id="foreign"), nonce="n", invocation_id="i")
        with self.assertRaisesRegex(RuntimeError, "platform"):
            validate_cross_os_identity_relation(dict(host, platform="linux"), linux, nonce="n", invocation_id="i")
        with self.assertRaisesRegex(RuntimeError, "host PID"):
            validate_cross_os_identity_relation(dict(host, pid=0), linux, nonce="n", invocation_id="i")
        with self.assertRaisesRegex(RuntimeError, "creation identity"):
            validate_cross_os_identity_relation(dict(host, created_utc=""), linux, nonce="n", invocation_id="i")
        with self.assertRaisesRegex(RuntimeError, "nonce and invocation"):
            validate_cross_os_identity_relation(host, linux, nonce="", invocation_id="i")

    def test_bwrap_manifest_binds_only_declared_roots_and_drops_capabilities(self) -> None:
        argv = _support_module.bwrap_base(
            Path("/pinned/bwrap"), Path("/pinned/release"), Path("/synthetic/workspace"),
            Path("/synthetic/home"), "http://10.0.0.1:1234",
        )
        rendered = "\0".join(argv).replace("\\", "/")
        self.assertNotIn("/mnt/c", rendered)
        self.assertNotIn("/dev/bus/usb", rendered)
        self.assertNotIn("BYO-Firmware-MCP", rendered)
        self.assertNotIn("/root/.codex", rendered)
        self.assertIn("--cap-drop\0ALL", rendered)
        self.assertNotIn("--cap-add", rendered)
        self.assertIn("--ro-bind\0/usr\0/usr", rendered)
        self.assertNotIn("--bind\0/usr", rendered)
        writable_binds = rendered.count("--bind\0")
        self.assertEqual(2, writable_binds, rendered)
        self.assertIn("--bind\0/synthetic/workspace\0/workspace", rendered)
        self.assertIn("--bind\0/synthetic/home\0/home/agent/.codex", rendered)

    def test_retired_linux_controller_route_fails_closed_when_executed(self) -> None:
        route = (
            REPOSITORY_ROOT / "orchestrator_harness" / "tests" / "support" / "wsl_public_route_entry.py"
        )
        completed = subprocess.run(
            [sys.executable, str(route)], cwd=REPOSITORY_ROOT,
            capture_output=True, text=True, check=False,
        )
        self.assertNotEqual(0, completed.returncode)
        self.assertIn("Windows-native", completed.stderr)

    def test_bridge_evidence_linux_identity_carries_nonce_for_cross_os_relation(self) -> None:
        provider = (
            REPOSITORY_ROOT / "orchestrator_harness" / "tests" / "support" / "wsl_codex_provider.py"
        ).read_text(encoding="utf-8")
        # The provider's linux_bridge identity must carry the one-use nonce and
        # invocation so the Windows host can bind the two halves without ever
        # asserting PID equality across the two identity domains.
        self.assertIn('"nonce": args.nonce, "invocation_id": args.invocation_id', provider)
        bridge_evidence = {
            "status": "PASS",
            "nonce": "n" * 64,
            "invocation_id": "invocation",
            "linux_bridge": {
                "platform": "linux",
                "pid": 999,
                "created_utc": "linux-created",
                "nonce": "n" * 64,
                "invocation_id": "invocation",
            },
        }
        host_identity = {
            "platform": "windows",
            "pid": 1234,
            "created_utc": "host-created",
            "nonce": "n" * 64,
            "invocation_id": "invocation",
            "parent_pid": 12,
        }
        validate_cross_os_identity_relation(
            host_identity,
            bridge_evidence["linux_bridge"],
            nonce="n" * 64,
            invocation_id="invocation",
        )
        with self.assertRaisesRegex(RuntimeError, "nonce"):
            validate_cross_os_identity_relation(
                host_identity,
                dict(bridge_evidence["linux_bridge"], nonce="m" * 64),
                nonce="n" * 64,
                invocation_id="invocation",
            )

    def test_provider_bridge_argv_is_the_only_wsl_route_element(self) -> None:
        host = (REPOSITORY_ROOT / "orchestrator_harness" / "tests" / "real_agent_test.py").read_text(encoding="utf-8")
        controller = (REPOSITORY_ROOT / "orchestrator_harness" / "public_launch.py").read_text(encoding="utf-8")
        operator = (REPOSITORY_ROOT / "orchestrator_harness" / "operator_launch.py").read_text(encoding="utf-8")
        # The provider shim argv is the only WSL element on the route: the
        # controller command stays a native Python lane-controller invocation.
        self.assertIn('"wsl.exe"', host)
        self.assertIn("provider_command = [", host)
        self.assertIn('"-m",\n            "orchestrator_harness.lane_controller"', controller)
        self.assertNotIn("wsl.exe", controller)
        self.assertNotIn("wsl.exe", operator)


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
                self.assertEqual(receipt["pid"], status["controller_pid"])
                self.assertEqual(
                    receipt["created_utc"], status["controller_created_utc"]
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
            creationflags=_WINDOWLESS_CREATION_FLAGS,
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
            subprocess.run(["git", "-C", str(root), "add", "."], check=True)
            subprocess.run(["git", "-C", str(root), "commit", "-m", "selector candidate"], check=True, capture_output=True)
            credit_file = Path(raw) / "credits.json"
            credit_file.write_text('{"credits": []}\n', encoding="utf-8")
            candidate_script = root / "tools" / "Invoke-CandidateSafeguard.ps1"
            self.assertTrue(candidate_script.is_file())
            # The safeguard must be executed from the candidate copy itself;
            # running it from a foreign cwd is fine, a foreign script is not.
            completed = subprocess.run(
                [
                    "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(candidate_script),
                    "-RepositoryRoot", str(root), "-ExpectedBranch", "firmware/v2-candidate",
                    "-CreditFile", str(credit_file),
                ],
                cwd=REPOSITORY_ROOT,
                capture_output=True,
                text=True,
                check=False,
                creationflags=_WINDOWLESS_CREATION_FLAGS,
            )
            self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
            self.assertIn("READY S6.RELEASE.", completed.stdout)
            self.assertNotIn("TEST.SAFEGUARD", completed.stdout)

            foreign_script = subprocess.run(
                [
                    "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                    str(REPOSITORY_ROOT / "tools" / "Invoke-CandidateSafeguard.ps1"),
                    "-RepositoryRoot", str(root), "-ExpectedBranch", "firmware/v2-candidate",
                ],
                cwd=REPOSITORY_ROOT,
                capture_output=True,
                text=True,
                check=False,
                creationflags=_WINDOWLESS_CREATION_FLAGS,
            )
            self.assertNotEqual(0, foreign_script.returncode)
            self.assertIn("candidate RepositoryRoot tools directory", foreign_script.stderr)

            injected = subprocess.run(
                [
                    "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(candidate_script),
                    "-RepositoryRoot", str(root), "-ExpectedBranch", "firmware/v2-candidate",
                    "-SelectionFile", str(Path(raw) / "selection.json"),
                ],
                cwd=REPOSITORY_ROOT,
                capture_output=True,
                text=True,
                check=False,
                creationflags=_WINDOWLESS_CREATION_FLAGS,
            )
            self.assertNotEqual(0, injected.returncode)
            self.assertIn("parameter", (injected.stdout + injected.stderr).lower())

    def test_safeguard_reaches_ready_from_candidate_owned_prerequisites(self) -> None:
        with tempfile.TemporaryDirectory(prefix="orchestrator-s6-candidate-owned-") as raw:
            root = Path(raw) / "candidate"
            shutil.copytree(
                REPOSITORY_ROOT,
                root,
                ignore=shutil.ignore_patterns(
                    ".git", ".agent-workspace", "__pycache__", "*.pyc", ".ruff_cache"
                ),
            )
            baseline = root / ".codex" / "dev" / "basedpyright-baseline.json"
            pyright_config = root / "pyrightconfig.json"
            self.assertTrue(baseline.is_file())
            self.assertTrue(pyright_config.is_file())
            subprocess.run(
                ["git", "-C", str(root), "init", "-b", "firmware/v2-candidate"],
                check=True, capture_output=True,
            )
            subprocess.run(
                ["git", "-C", str(root), "config", "user.email", "candidate@example.invalid"],
                check=True, capture_output=True,
            )
            subprocess.run(
                ["git", "-C", str(root), "config", "user.name", "Candidate Test"],
                check=True, capture_output=True,
            )
            subprocess.run(["git", "-C", str(root), "add", "."], check=True, capture_output=True)
            subprocess.run(
                ["git", "-C", str(root), "commit", "-m", "candidate tree"],
                check=True, capture_output=True,
            )
            head = subprocess.run(
                ["git", "-C", str(root), "rev-parse", "HEAD"], check=True,
                capture_output=True, text=True,
            ).stdout.strip()
            completed = subprocess.run(
                [
                    "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                    str(root / "tools" / "Invoke-CandidateSafeguard.ps1"),
                    "-RepositoryRoot", str(root), "-ExpectedBranch", "firmware/v2-candidate",
                    "-ExpectedTip", head,
                ],
                cwd=REPOSITORY_ROOT,
                capture_output=True,
                text=True,
                check=False,
                creationflags=_WINDOWLESS_CREATION_FLAGS,
            )
            self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
            self.assertIn("READY S6.RELEASE.", completed.stdout)
            self.assertNotIn("TEST.SAFEGUARD", completed.stdout)

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
                creationflags=_WINDOWLESS_CREATION_FLAGS,
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
