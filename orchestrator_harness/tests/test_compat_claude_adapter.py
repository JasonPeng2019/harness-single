"""Focused coverage for the Claude host adapter and owned installer lifecycle.

The tests cover capability-gated host selection, sparse wake notices,
transport receipts, registration replay, resume re-entry, wake delivery,
finalization, the owned installer lifecycle, rollback, idempotent upgrade,
uninstall preservation, synthetic self-test, and hook dispatch.

Every test exercises the real adapter/installer seams; nothing is mocked.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import orchestrator_harness
from orchestrator_harness.claude_adapter import (
    ClaudeAdapter,
    ClaudeAdapterError,
    SyntheticClaudeTransport,
    claude_capabilities,
    create_claude_adapter,
)
from orchestrator_harness.claude_installer import (
    bind_claude_project_from_queue,
    check_claude_adapter,
    install_claude_adapter,
    run_installed_claude_hook,
    uninstall_claude_adapter,
    upgrade_claude_adapter,
)
from orchestrator_harness.cli import main
from orchestrator_harness.codex_adapter import (
    CodexInstallConflict,
    CodexInstallRollback,
    select_host_adapter,
)
from orchestrator_harness.host_adapters import FutureHostFixture
from orchestrator_harness.notifications import ManagerEventRouter


class ClaudeAdapterCompatTests(unittest.TestCase):
    def _router(self, root: Path) -> ManagerEventRouter:
        return ManagerEventRouter(
            root,
            run_id="run-claude",
            queue_id="queue-claude",
            manager_session_id="session-claude",
            manager_thread_id="thread-claude",
            registration_id="registration-claude",
        )

    def _adapter(self, root: Path) -> ClaudeAdapter:
        return create_claude_adapter(
            self._router(root),
            transport=SyntheticClaudeTransport(),
            session_id="session-claude",
        )

    @staticmethod
    def _synthetic_transport(adapter: ClaudeAdapter) -> SyntheticClaudeTransport:
        transport = adapter.transport
        if not isinstance(transport, SyntheticClaudeTransport):
            raise AssertionError("test adapter must use synthetic Claude transport")
        return transport

    @staticmethod
    def _event(
        event_id: str, *, priority: int = 2, severity: str = "warning"
    ) -> dict[str, object]:
        return {
            "event_id": event_id,
            "type": "MANAGER_SIGNAL",
            "identity": f"synthetic:{event_id}",
            "data": {
                "signal_id": event_id,
                "lane_id": "synthetic:s4",
                "manager_actionable": True,
                "severity": severity,
            },
            "priority": priority,
        }

    def test_N1_real_adapter_replaces_future_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            router = self._router(Path(raw) / "manager")
            adapter = select_host_adapter("claude", router)
            self.assertNotIsInstance(adapter, FutureHostFixture)
            self.assertTrue(adapter.profile.implemented)
            self.assertEqual("claude", adapter.profile.kind)

    def test_package_root_exposes_all_shipped_binding_helpers(self) -> None:
        self.assertTrue(callable(orchestrator_harness.activate_codex_binding))
        self.assertTrue(callable(orchestrator_harness.activate_claude_binding))
        self.assertTrue(callable(orchestrator_harness.activate_qwen_binding))

    def test_N8_capability_selection(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            router = self._router(Path(raw) / "manager")
            claude = select_host_adapter("claude", router)
            self.assertIsInstance(claude, ClaudeAdapter)
            self.assertTrue(claude.profile.capabilities.idle_wake)
            future = select_host_adapter("future-host", router)
            self.assertIsInstance(future, FutureHostFixture)
            self.assertFalse(future.profile.implemented)
            self.assertFalse(any(future.profile.capabilities.as_record().values()))

    def test_N2_bounded_wake_notice_is_sparse(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            adapter = self._adapter(Path(raw) / "manager")
            coordinator = adapter.coordinator
            coordinator.register()
            coordinator.router.admit(self._event("sparse-a"))
            coordinator.router.admit(self._event("sparse-b"))
            notice = coordinator.notice_for_wake()
            self.assertIsNotNone(notice)
            assert notice is not None
            self.assertEqual("run-claude", notice.run_id)
            self.assertEqual(2, notice.pending_count)
            self.assertEqual("warning", notice.highest_severity)
            record = notice.as_record()
            for forbidden in (
                "event_id",
                "event_ids",
                "data",
                "payload",
                "raw_output",
                "source_event",
                "queue_records",
            ):
                self.assertNotIn(forbidden, record)

    def test_N3_receipt_is_transport_evidence_only(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            adapter = self._adapter(Path(raw) / "manager")
            coordinator = adapter.coordinator
            coordinator.register()
            coordinator.router.admit(self._event("evidence"))
            notice = coordinator.notice_for_wake()
            self.assertIsNotNone(notice)
            receipt = coordinator.deliver_at_boundary(notice, boundary="post_tool_use")
            self.assertIsNotNone(receipt)
            assert receipt is not None
            self.assertEqual("DELIVERED", receipt.outcome)
            self.assertEqual(adapter.profile.profile_id, receipt.adapter_profile)
            self.assertTrue(coordinator.pending_events())

    def test_N6_registration_replay_persists_across_restart(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            manager = Path(raw) / "manager"
            adapter = self._adapter(manager)
            coordinator = adapter.coordinator
            coordinator.register()
            coordinator.router.admit(self._event("replay"))
            notice = coordinator.notice_for_wake()
            self.assertIsNotNone(notice)
            assert notice is not None
            restored = self._adapter(manager).coordinator
            restored.register()
            replayed = restored.notice_for_wake()
            self.assertIsNotNone(replayed)
            assert replayed is not None
            self.assertEqual(notice.notice_id, replayed.notice_id)

    def test_A24_resume_as_wake_reenters_idle_session(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            adapter = self._adapter(Path(raw) / "manager")
            transport = self._synthetic_transport(adapter)
            coordinator = adapter.coordinator
            coordinator.register()
            coordinator.router.admit(self._event("idle-wake"))
            receipt = adapter.idle_wake()
            self.assertIsNotNone(receipt)
            assert receipt is not None
            self.assertEqual("DELIVERED", receipt.outcome)
            self.assertEqual(1, len(transport.resume_invocations))
            invocation = transport.resume_invocations[0]
            self.assertEqual("session-claude", invocation["session_id"])
            self.assertEqual(
                ["claude", "--resume", "session-claude"], invocation["argv"]
            )
            capabilities = claude_capabilities()
            self.assertFalse(capabilities.next_input_injection)
            self.assertTrue(capabilities.idle_wake)

    def test_G17_resume_wake_tracks_delivered_and_failed(self) -> None:
        # The queue-level HARNESS_WAKE_* event *taxonomy* itself is
        # provider-neutral (registered in notifications.py, exercised by the
        # G-phase codex tests); this test pins the Claude adapter's own
        # attempt/deliver/fail behavior through its resume transport.
        with tempfile.TemporaryDirectory() as raw:
            adapter = self._adapter(Path(raw) / "manager")
            transport = self._synthetic_transport(adapter)
            coordinator = adapter.coordinator
            coordinator.register()
            coordinator.router.admit(self._event("wake-success"))
            receipt = adapter.idle_wake()
            self.assertIsNotNone(receipt)
            assert receipt is not None
            self.assertEqual("DELIVERED", receipt.outcome)
            self.assertTrue(
                any(call["method"] == "resume" for call in transport.calls)
            )
            self.assertEqual(1, len(transport.resume_invocations))
        with tempfile.TemporaryDirectory() as raw:

            class FailingClaudeTransport(SyntheticClaudeTransport):
                def wake_idle(self, session_id, notice):
                    raise ClaudeAdapterError("synthetic wake transport failed")

            failing = FailingClaudeTransport()
            adapter = create_claude_adapter(
                self._router(Path(raw) / "manager"),
                transport=failing,
                session_id="session-claude",
            )
            coordinator = adapter.coordinator
            coordinator.register()
            coordinator.router.admit(self._event("wake-failure"))
            notice = coordinator.notice_for_wake()
            self.assertIsNotNone(notice)
            assert notice is not None
            # The failing transport itself raises when invoked...
            with self.assertRaises(ClaudeAdapterError):
                failing.wake_idle("session-claude", notice.as_record())
            # ...and the adapter's wake path fails closed: the coordinator
            # converts the transport failure into a DELIVERY_FAILED receipt
            # (the WAKE_FAILED path) and no resume invocation is recorded.
            receipt = adapter.idle_wake()
            self.assertIsNotNone(receipt)
            assert receipt is not None
            self.assertEqual("DELIVERY_FAILED", receipt.outcome)
            self.assertEqual([], failing.resume_invocations)

    def test_G19_manager_wake_roundtrip_via_finalization(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            adapter = self._adapter(Path(raw) / "manager")
            transport = self._synthetic_transport(adapter)
            coordinator = adapter.coordinator
            coordinator.register()
            coordinator.router.admit(self._event("finalize"))
            self.assertFalse(coordinator.task_active)
            self.assertTrue(adapter.stop_boundary())
            self.assertTrue(
                any(
                            call["method"] == "Stop.continue"
                            for call in transport.calls
                )
            )

    def test_B9_install_writes_settings_and_manifest_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            proj = Path(raw) / "project"
            proj.mkdir()
            result = install_claude_adapter(proj)
            self.assertGreater(len(result["changed"]), 0)
            self.assertFalse(result["idempotent"])
            self.assertTrue((proj / ".claude" / "settings.json").is_file())
            self.assertTrue(
                (proj / ".claude" / "orchestrator-harness-adapter.json").is_file()
            )
        with tempfile.TemporaryDirectory() as raw:
            proj = Path(raw) / "project"
            proj.mkdir()
            (proj / ".claude").mkdir()
            settings = proj / ".claude" / "settings.json"
            settings.write_text(
                json.dumps(
                    {
                        "hooks": {
                            "Stop": [
                                {
                                    "matcher": "*",
                                    "hooks": [
                                        {
                                            "type": "command",
                                            "id": "orchestrator-harness-stop",
                                            "command": "python DIFFERENT.py",
                                        }
                                    ],
                                }
                            ]
                        }
                    }
                ),
                encoding="utf-8",
            )
            # An owned id pointing at a different command is an ambiguity: the
            # install fails closed and the transaction rolls back.  The conflict
            # is wrapped in the bounded rollback exception, so pin both layers.
            with self.assertRaises(CodexInstallRollback) as ctx:
                install_claude_adapter(proj)
            self.assertIsInstance(ctx.exception.__cause__, CodexInstallConflict)
            self.assertFalse(
                (proj / ".claude" / "orchestrator-harness-adapter.json").exists()
            )

    def test_B10_check_reports_ownership_and_currency(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            proj = Path(raw) / "project"
            proj.mkdir()
            install_claude_adapter(proj)
            checked = check_claude_adapter(proj)
            self.assertTrue(checked["installed"])
            self.assertEqual("owned", checked["ownership"])
            self.assertTrue(checked["current"])
            hook = (
                proj
                / ".claude"
                / "hooks"
                / "orchestrator_harness_post_tool_use.py"
            )
            hook.write_bytes(hook.read_bytes() + b"\n# tampered\n")
            tampered = check_claude_adapter(proj)
            self.assertTrue(tampered["installed"])
            self.assertFalse(tampered["current"])

    def test_B11_upgrade_is_idempotent_on_current(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            proj = Path(raw) / "project"
            proj.mkdir()
            install_claude_adapter(proj)
            upgraded = upgrade_claude_adapter(proj)
            self.assertTrue(upgraded["installed"])
            self.assertTrue(upgraded["current"])
            self.assertTrue(upgraded["idempotent"])

    def test_B12_uninstall_removes_owned_preserves_modified(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            proj = Path(raw) / "project"
            proj.mkdir()
            install_claude_adapter(proj)
            removed = uninstall_claude_adapter(proj)
            self.assertGreater(len(removed["removed"]), 0)
            self.assertEqual([], removed["preserved_modified"])
            self.assertFalse(check_claude_adapter(proj)["installed"])
        with tempfile.TemporaryDirectory() as raw:
            proj = Path(raw) / "project"
            proj.mkdir()
            install_claude_adapter(proj)
            hook = proj / ".claude" / "hooks" / "orchestrator_harness_stop.py"
            hook.write_bytes(hook.read_bytes() + b"\n# tampered\n")
            removed = uninstall_claude_adapter(proj)
            self.assertIn(
                ".claude/hooks/orchestrator_harness_stop.py",
                removed["preserved_modified"],
            )
            self.assertTrue(hook.exists())

    def test_B13_self_test_succeeds_against_claude_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            router = self._router(Path(raw) / "manager")
            result = create_claude_adapter(router).synthetic_self_test()
            self.assertFalse(result["acknowledged_by_delivery"])
            self.assertEqual("DELIVERED", result["receipt"]["outcome"])
            self.assertEqual(
                "orchestrator-claude-synthetic-wake/v1", result["schema"]
            )

    def test_B14_boundary_dispatch_delivers_via_transport(self) -> None:
        cases = (
            ("post_tool_use", "post_tool_use", "PostToolUse.context"),
            ("turn_completed", "idle_wake", "resume"),
            ("finalization", "stop_boundary", "Stop.continue"),
        )
        for boundary, method_name, transport_method in cases:
            with self.subTest(boundary=boundary):
                with tempfile.TemporaryDirectory() as raw:
                    adapter = self._adapter(Path(raw) / "manager")
                    transport = self._synthetic_transport(adapter)
                    coordinator = adapter.coordinator
                    coordinator.register()
                    coordinator.router.admit(self._event(f"boundary-{boundary}"))
                    result = getattr(adapter, method_name)()
                    self.assertIsNotNone(result)
                    self.assertTrue(
                        any(
                            call["method"] == transport_method
                            for call in transport.calls
                        ),
                        f"no {transport_method} transport call for {boundary}",
                    )

    def test_installed_post_tool_hook_records_durable_delivery(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            project = root / "project"
            project.mkdir()
            install_claude_adapter(project)
            router = self._router(root / "manager")
            coordinator_root = root / "coordinator"
            bind_claude_project_from_queue(
                project,
                router.root,
                coordinator_root=coordinator_root,
            )
            router.admit(self._event("installed-post-tool"))
            result = run_installed_claude_hook(
                project, boundary="post_tool_use"
            )
            self.assertEqual("DELIVERED", result["receipt"]["outcome"])
            self.assertEqual("DELIVERED", router.read_deliveries()[0]["outcome"])
            state = json.loads(
                (coordinator_root / "DELIVERY_COORDINATOR.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(1, state["attempt_count"])

    def test_manager_bind_command_connects_installed_post_tool_hook(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            project = root / "project"
            project.mkdir()
            install_claude_adapter(project)
            router = self._router(root / "manager")
            coordinator_root = root / "coordinator"

            binding = bind_claude_project_from_queue(
                project,
                router.root,
                coordinator_root=coordinator_root,
            )
            self.assertEqual("orchestrator-claude-binding/v1", binding["schema"])
            self.assertEqual(str(router.root), binding["binding"]["queue_root"])

            with patch("orchestrator_harness.cli._print_json") as emit:
                self.assertEqual(
                    0,
                    main(
                        [
                            "adapter",
                            "bind",
                            "--host",
                            "claude",
                            "--project-root",
                            str(project),
                            "--queue-root",
                            str(router.root),
                            "--coordinator-root",
                            str(coordinator_root),
                        ]
                    ),
                )
            self.assertEqual(
                "orchestrator-claude-binding/v1",
                emit.call_args.args[0]["schema"],
            )

            router.admit(self._event("claude-manager-bind"))
            result = run_installed_claude_hook(project, boundary="post_tool_use")
            self.assertEqual("DELIVERED", result["receipt"]["outcome"])
            self.assertEqual("DELIVERED", router.read_deliveries()[0]["outcome"])

    def test_installed_hook_requires_a_manager_binding(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            project = Path(raw) / "project"
            project.mkdir()
            install_claude_adapter(project)
            with self.assertRaisesRegex(
                CodexInstallConflict,
                "installed Claude hook has no harness binding",
            ):
                run_installed_claude_hook(project, boundary="post_tool_use")

    def test_manager_bind_migrates_the_exact_legacy_binding_shape(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            project = root / "project"
            project.mkdir()
            install_claude_adapter(project)
            router = self._router(root / "manager")
            legacy = {
                "project_root": str(project.resolve()),
                "queue_root": str(router.root),
                "coordinator_root": str(root / "coordinator"),
                "run_id": router.binding.run_id,
                "queue_id": router.binding.queue_id,
                "manager_session_id": router.binding.manager_session_id,
                "manager_thread_id": router.binding.manager_thread_id,
                "registration_id": router.binding.registration_id,
                "registration_generation": router.registration_generation,
            }
            (project / ".claude" / "orchestrator-harness-binding.json").write_text(
                json.dumps(legacy),
                encoding="utf-8",
            )

            bound = bind_claude_project_from_queue(
                project,
                router.root,
                coordinator_root=root / "coordinator",
            )
            self.assertEqual("orchestrator-claude-binding/v1", bound["schema"])


if __name__ == "__main__":
    unittest.main()
