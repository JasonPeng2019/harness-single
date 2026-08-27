"""Focused deterministic coverage for the Qwen host and project surface."""

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

from orchestrator_harness.cli import build_parser
from orchestrator_harness.codex_adapter import select_host_adapter
from orchestrator_harness.host_adapters import FutureHostFixture
from orchestrator_harness.notifications import ManagerEventRouter
from orchestrator_harness.qwen_adapter import (
    QwenAdapter,
    SyntheticQwenTransport,
    create_qwen_adapter,
    qwen_capabilities,
)
from orchestrator_harness.qwen_installer import (
    check_qwen_adapter,
    install_qwen_adapter,
    run_installed_qwen_hook,
    uninstall_qwen_adapter,
)


class QwenAdapterCompatTests(unittest.TestCase):
    def _router(self, root: Path) -> ManagerEventRouter:
        return ManagerEventRouter(
            root,
            run_id="run-qwen",
            queue_id="queue-qwen",
            manager_session_id="session-qwen",
            manager_thread_id="thread-qwen",
            registration_id="registration-qwen",
        )

    def _adapter(self, root: Path) -> QwenAdapter:
        return create_qwen_adapter(
            self._router(root),
            transport=SyntheticQwenTransport(),
            session_id="session-qwen",
        )

    @staticmethod
    def _event(event_id: str) -> dict[str, object]:
        return {
            "event_id": event_id,
            "type": "MANAGER_SIGNAL",
            "identity": f"synthetic:{event_id}",
            "data": {
                "signal_id": event_id,
                "lane_id": "synthetic:qwen",
                "manager_actionable": True,
                "severity": "warning",
            },
        }

    @staticmethod
    def _transport(adapter: QwenAdapter) -> SyntheticQwenTransport:
        transport = adapter.transport
        if not isinstance(transport, SyntheticQwenTransport):
            raise AssertionError("test adapter must use a synthetic Qwen transport")
        return transport

    def test_registration_selects_native_qwen_host(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            router = self._router(Path(raw) / "manager")
            adapter = select_host_adapter("qwen", router)
            self.assertIsInstance(adapter, QwenAdapter)
            self.assertNotIsInstance(adapter, FutureHostFixture)
            self.assertEqual("qwen", adapter.profile.kind)
            self.assertTrue(adapter.profile.implemented)
            self.assertFalse(adapter.profile.capabilities.next_input_injection)

    def test_sparse_delivery_has_receipt_but_no_ack(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            adapter = self._adapter(Path(raw) / "manager")
            coordinator = adapter.coordinator
            coordinator.register()
            coordinator.router.admit(self._event("sparse"))
            notice = coordinator.notice_for_wake()
            self.assertIsNotNone(notice)
            assert notice is not None
            record = notice.as_record()
            for forbidden in ("event_id", "event_ids", "data", "payload", "queue_records"):
                self.assertNotIn(forbidden, record)
            receipt = coordinator.deliver_at_boundary(notice, boundary="post_tool_use")
            self.assertIsNotNone(receipt)
            assert receipt is not None
            self.assertEqual("DELIVERED", receipt.outcome)
            self.assertTrue(coordinator.pending_events())
            self.assertFalse(any(call.get("acknowledged") for call in self._transport(adapter).calls))

    def test_idle_resume_and_stop_use_synthetic_transport(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            adapter = self._adapter(Path(raw) / "manager")
            coordinator = adapter.coordinator
            transport = self._transport(adapter)
            coordinator.register()
            coordinator.router.admit(self._event("idle"))
            receipt = adapter.idle_wake()
            self.assertIsNotNone(receipt)
            self.assertEqual(["qwen", "exec", "--resume", "session-qwen"], transport.resume_invocations[0]["argv"])
            with tempfile.TemporaryDirectory() as other:
                final_adapter = self._adapter(Path(other) / "manager")
                final_adapter.coordinator.register()
                final_adapter.coordinator.router.admit(self._event("stop"))
                self.assertTrue(final_adapter.stop_boundary())

    def test_owned_install_check_uninstall_preserves_project_settings(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            project = Path(raw) / "project"
            project.mkdir()
            settings = project / ".qwen" / "settings.json"
            settings.parent.mkdir()
            original = {
                "theme": "user-choice",
                "hooks": {"SessionStart": [{"matcher": "*", "hooks": []}]},
            }
            settings.write_text(json.dumps(original), encoding="utf-8")
            installed = install_qwen_adapter(project)
            self.assertTrue(installed["current"])
            self.assertEqual("owned", check_qwen_adapter(project)["ownership"])
            merged = json.loads(settings.read_text(encoding="utf-8"))
            self.assertEqual("user-choice", merged["theme"])
            self.assertIn("SessionStart", merged["hooks"])
            self.assertEqual("idle_prompt", merged["hooks"]["Notification"][0]["matcher"])
            self.assertEqual("*", merged["hooks"]["PostToolUse"][0]["matcher"])
            self.assertNotIn("matcher", merged["hooks"]["Stop"][0])
            notification_hook = merged["hooks"]["Notification"][0]["hooks"][0]
            self.assertEqual("command", notification_hook["type"])
            self.assertEqual("orchestrator-harness-notification", notification_hook["name"])
            hook = project / ".qwen" / "hooks" / "orchestrator_harness_stop.py"
            hook.write_bytes(hook.read_bytes() + b"\n# user edit\n")
            self.assertFalse(check_qwen_adapter(project)["current"])
            removed = uninstall_qwen_adapter(project)
            self.assertIn(".qwen/hooks/orchestrator_harness_stop.py", removed["preserved_modified"])
            self.assertTrue(hook.is_file())
            self.assertFalse((project / ".qwen" / "orchestrator-harness-adapter.json").exists())
            remaining = json.loads(settings.read_text(encoding="utf-8"))
            self.assertEqual("user-choice", remaining["theme"])
            self.assertIn("SessionStart", remaining["hooks"])

    def test_fixture_and_example_are_provider_free_and_truthful(self) -> None:
        root = Path(__file__).resolve().parents[2]
        fixture_path = root / "examples" / "disposable_qwen_coding_fixture.py"
        self.assertTrue(fixture_path.is_file())
        spec = importlib.util.spec_from_file_location("disposable_qwen_fixture", fixture_path)
        self.assertIsNotNone(spec)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as raw:
            result = module.run_fixture(Path(raw))
        self.assertEqual("qwen-code", result["provider_id"])
        self.assertFalse(result["live_provider_session"])
        self.assertFalse(result["live_hook_claim"])
        example = json.loads(
            (root / "examples" / "coding.qwen.invocation.example.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual("qwen-code", example["provider"]["id"])
        self.assertFalse(example["provider"]["notification"])

    def test_self_test_proves_sparse_no_ack(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            result = self._adapter(Path(raw) / "manager").synthetic_self_test()
        self.assertEqual("orchestrator-qwen-synthetic-wake/v1", result["schema"])
        self.assertEqual("DELIVERED", result["receipt"]["outcome"])
        self.assertFalse(result["acknowledged_by_delivery"])
        self.assertGreaterEqual(result["pending_after_delivery"], 1)

    def test_capabilities_are_explicit(self) -> None:
        self.assertTrue(qwen_capabilities().active_turn_notice)
        self.assertTrue(qwen_capabilities().idle_wake)
        self.assertTrue(qwen_capabilities().finalization_gate)
        self.assertFalse(qwen_capabilities().next_input_injection)

    def test_cli_exposes_qwen_project_hook_boundaries(self) -> None:
        args = build_parser().parse_args(
            [
                "adapter",
                "hook",
                "--host",
                "qwen",
                "--project-root",
                "C:/project",
                "--boundary",
                "notification",
            ]
        )
        self.assertEqual("qwen", args.host)
        self.assertEqual("notification", args.boundary)

    def test_installed_post_tool_hook_records_durable_delivery(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            project = root / "project"
            project.mkdir()
            install_qwen_adapter(project)
            router = self._router(root / "manager")
            coordinator_root = root / "coordinator"
            coordinator = create_qwen_adapter(
                router,
                state_root=coordinator_root,
                registration_generation=router.registration_generation,
            ).coordinator
            coordinator.restore()
            binding = router.binding
            (project / ".qwen" / "orchestrator-harness-binding.json").write_text(
                json.dumps(
                    {
                        "project_root": str(project.resolve()),
                        "queue_root": str(router.root),
                        "coordinator_root": str(coordinator_root),
                        "run_id": binding.run_id,
                        "queue_id": binding.queue_id,
                        "manager_session_id": binding.manager_session_id,
                        "manager_thread_id": binding.manager_thread_id,
                        "registration_id": binding.registration_id,
                        "registration_generation": router.registration_generation,
                    }
                ),
                encoding="utf-8",
            )
            router.admit(self._event("installed-post-tool"))
            result = run_installed_qwen_hook(project, boundary="post_tool_use")
            self.assertEqual("DELIVERED", result["receipt"]["outcome"])
            self.assertEqual("DELIVERED", router.read_deliveries()[0]["outcome"])
            state = json.loads(
                (coordinator_root / "DELIVERY_COORDINATOR.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(1, state["attempt_count"])


if __name__ == "__main__":
    unittest.main()
