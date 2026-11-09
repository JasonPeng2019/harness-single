"""Exercise explicit ROOT modes with owned fixtures, never Codex or sandbox setup."""
from __future__ import annotations

from contextlib import ExitStack
import os
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from scripts.tests import test_root_prelaunch as fixtures

preparation = fixtures.preparation
collector = fixtures.collector


class _RootModeCase(TestCase):
    def setUp(self):
        self.fixture = fixtures.RootPrelaunchTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def prepare_full_access(self):
        f = self.fixture
        return preparation.prepare(f.harness, f.workspace, "fixture", sandbox="danger-full-access")


class RootSandboxModeTests(_RootModeCase):
    """Real receipt/mode code with deterministic installed owner/service fixtures."""
    def setUp(self):
        super().setUp()
        f = self.fixture
        f.write(f.workspace / ".codex/config.toml", {})
        (f.workspace / ".codex/config.toml").write_text('[features]\nhooks = true\n', encoding="utf-8")
        stack = ExitStack()
        self.addCleanup(stack.close)
        session = {"run_id": "fixture", "provider_id": "codex", "session_id": "owned-root",
                   "native_session_id": None, "config_identity": "controlled-fixture"}
        stack.enter_context(patch.object(preparation, "check_installed", return_value={"fixture": "hooks"}))
        stack.enter_context(patch.object(preparation, "_monitor_owner", return_value={"startup_id": "owned-monitor"}))
        stack.enter_context(patch.object(preparation, "publish_root_session", return_value=session))
        stack.enter_context(patch.object(preparation, "read_root_session", return_value=session))
        stack.enter_context(patch.object(preparation.subprocess, "run", return_value=
            fixtures.subprocess.CompletedProcess([], 0, '{"ok":true}', '')))
        from orchestrator_harness import public_checks
        stack.enter_context(patch.object(public_checks, "start_service", return_value=None))

    def test_explicit_full_access_prepares_and_revalidates_without_sandbox_health(self):
        f = self.fixture
        with patch.object(preparation, "require_ready", side_effect=AssertionError("unused sandbox gate")):
            first = self.prepare_full_access()
            again = self.prepare_full_access()
            validated = preparation.validate_prepared_root(
                f.harness, f.workspace, "fixture", sandbox="danger-full-access")
            resumed = preparation.validate_prepared_root(
                f.harness, f.workspace, "fixture", sandbox="danger-full-access", resuming=True)
        self.assertEqual(first, again)
        self.assertEqual(first, validated)
        self.assertEqual(first, resumed)
        receipt = preparation.read(f.workspace / preparation.RECEIPT)
        self.assertEqual("danger-full-access", receipt["root_sandbox"])
        self.assertNotIn("windows_sandbox_health", receipt)
        self.assertFalse((f.harness / "local-config/windows-sandbox-health.json").exists())
        self.assertEqual("danger-full-access", preparation.read(
            f.workspace / preparation.ATTEMPT)["root_sandbox"])

    def test_default_and_read_only_require_health_before_setup(self):
        f = self.fixture
        for mode in (None, "workspace-write", "read-only"):
            with self.subTest(mode=mode), patch.object(
                preparation, "require_ready", side_effect=ValueError("WINDOWS_SANDBOX_NOT_READY")
            ) as health, patch.object(preparation.subprocess, "run") as process:
                with self.assertRaisesRegex(ValueError, "WINDOWS_SANDBOX_NOT_READY"):
                    preparation.prepare(f.harness, f.workspace, "fixture", sandbox=mode)
                health.assert_called_once_with(f.harness, workspace=f.workspace)
                process.assert_not_called()
        self.assertFalse((f.workspace / ".harness-runtime").exists())
        self.assertFalse((f.workspace / preparation.ATTEMPT).exists())

    def test_changed_mode_refuses_fresh_resume_and_reprepare_before_output(self):
        f = self.fixture
        self.prepare_full_access()
        original = (f.workspace / preparation.RECEIPT).read_bytes()
        for resuming in (False, True):
            with self.subTest(resuming=resuming), self.assertRaisesRegex(ValueError, "ROOT sandbox differs"):
                preparation.validate_prepared_root(f.harness, f.workspace, "fixture", resuming=resuming)
        with self.assertRaisesRegex(ValueError, "ROOT sandbox differs"):
            preparation.prepare(f.harness, f.workspace, "fixture")
        self.assertEqual(original, (f.workspace / preparation.RECEIPT).read_bytes())
        args = f.collector_args()
        with patch.object(fixtures.processes, "spawn_provider") as spawn:
            with self.assertRaisesRegex(collector.LedgerError, "ROOT sandbox differs"):
                collector.run_codex(args)
        spawn.assert_not_called()
        self.assertFalse((f.workspace / "results").exists())

    def test_retained_failure_latch_still_blocks_full_access(self):
        f = self.fixture
        self.prepare_full_access()
        block = f.workspace / ".harness-runtime/operator-only/SANDBOX_LAUNCH_BLOCKED.json"
        f.write(block, {"fixture": "retained failure"})
        with patch.object(preparation, "require_ready", side_effect=AssertionError("unused sandbox gate")):
            for resuming in (False, True):
                with self.subTest(resuming=resuming), self.assertRaisesRegex(ValueError, "already recorded"):
                    preparation.validate_prepared_root(f.harness, f.workspace, "fixture",
                        resuming=resuming, sandbox="danger-full-access")
            with self.assertRaisesRegex(ValueError, "already recorded"):
                self.prepare_full_access()

    def test_retained_failure_latch_blocks_other_root_providers_without_readiness(self):
        f = self.fixture
        f.write(f.workspace / ".harness-runtime/operator-only/SANDBOX_LAUNCH_BLOCKED.json",
                {"fixture": "retained failure"})
        with patch.object(preparation, "require_ready", side_effect=AssertionError("unused sandbox gate")):
            for provider in ("claude-code", "qwen-code"):
                with self.subTest(provider=provider), self.assertRaisesRegex(ValueError, "already recorded"):
                    preparation.prepare(f.harness, f.workspace, "fixture", provider_id=provider)

    def test_legacy_v2_receipt_keeps_default_health_gate_and_cannot_authorize_full_access(self):
        f = self.fixture
        preparation.prepare(f.harness, f.workspace, "fixture")
        path = f.workspace / preparation.RECEIPT
        receipt = preparation.read(path)
        del receipt["root_sandbox"]
        f.write(path, receipt)
        before = path.read_bytes()
        with patch.object(preparation, "require_ready", return_value=None) as health:
            result = preparation.validate_prepared_root(f.harness, f.workspace, "fixture")
        health.assert_called_once_with(f.harness, workspace=f.workspace)
        self.assertEqual("workspace-write", result["root_sandbox"])
        with self.assertRaisesRegex(ValueError, "ROOT sandbox differs"):
            preparation.validate_prepared_root(f.harness, f.workspace, "fixture", sandbox="danger-full-access")
        self.assertEqual(before, path.read_bytes())

    def test_explicit_read_only_is_bound_separately_from_default_workspace_mode(self):
        f = self.fixture
        prepared = preparation.prepare(f.harness, f.workspace, "fixture", sandbox="read-only")
        self.assertEqual("read-only", prepared["root_sandbox"])
        self.assertEqual(prepared, preparation.validate_prepared_root(
            f.harness, f.workspace, "fixture", sandbox="read-only"))
        with self.assertRaisesRegex(ValueError, "ROOT sandbox differs"):
            preparation.validate_prepared_root(f.harness, f.workspace, "fixture")

    def test_invalid_or_non_codex_mode_refuses_without_setup(self):
        f = self.fixture
        with patch.object(preparation.subprocess, "run") as process:
            for provider, mode in (("codex", "auto"), ("claude-code", "danger-full-access"),
                                   ("qwen-code", "workspace-write")):
                with self.subTest(provider=provider), self.assertRaises(ValueError):
                    preparation.prepare(f.harness, f.workspace, "fixture", provider_id=provider, sandbox=mode)
            process.assert_not_called()
        self.assertFalse((f.workspace / ".harness-runtime").exists())


class RootSandboxNativeJourneyTests(_RootModeCase):
    """One real setup/monitor/collector/cleanup journey; no actual provider or sandbox."""
    def test_native_full_access_journey_with_empty_codex_home(self):
        from orchestrator_harness.windows_sandbox_preflight import require_ready
        f = self.fixture
        empty_home = f.workspace.parent / "empty-codex-home"
        empty_home.mkdir()
        with patch.dict(os.environ, {"CODEX_HOME": str(empty_home)}), patch.object(
            preparation, "require_ready", require_ready
        ):
            if os.name == "nt":
                with self.assertRaisesRegex(ValueError, "WINDOWS_SANDBOX_NOT_READY"):
                    require_ready(f.harness, workspace=f.workspace)
            prepared = self.prepare_full_access()
            self.assertEqual(prepared, preparation.validate_prepared_root(
                f.harness, f.workspace, "fixture", sandbox="danger-full-access"))
            args = f.collector_args()
            args.sandbox = "danger-full-access"
            code, manifest, _environment, _lines = f.fixture_collector(args, ["owned-thread"])
            self.assertEqual(0, code, manifest)
            launch = preparation.read(Path(args.results_dir) / "fixture/launch.json")
            self.assertEqual("danger-full-access", launch["sandbox"])
            self.assertEqual("danger-full-access", launch["root_prelaunch"]["root_sandbox"])
            self.assertEqual(prepared["root_session_id"], launch["root_prelaunch"]["root_session_id"])
            args.resume_task_file = str(f.workspace / "results/fixture/task.json")
            segment = f.workspace / "results/fixture/segments/0002"
            with patch.object(collector, "prepare_resume", return_value=(segment, 2, "owned-thread", 1)):
                code, manifest, environment, _lines = f.fixture_collector(args, ["owned-thread"])
            self.assertEqual(0, code, manifest)
            resumed = preparation.read(segment / "launch.json")
            self.assertEqual("danger-full-access", resumed["sandbox"])
            self.assertEqual("danger-full-access", resumed["root_prelaunch"]["root_sandbox"])
            self.assertEqual(prepared["root_session_id"], environment["HARNESS_ROOT_SESSION_ID"])
        self.assertFalse((empty_home / ".sandbox/setup_marker.json").exists())
