"""Focused deterministic checks for the native Qwen Code runner path."""

from __future__ import annotations

import unittest
from pathlib import Path

from orchestrator_harness.invocation import (
    CANONICAL_INVOCATION_SCHEMA,
    CanonicalInvocation,
    InvocationValidationError,
    _provider,
)
from orchestrator_harness.provider import (
    ProviderAdapterError,
    ProviderLaunchSpec,
    QwenCodeProviderAdapter,
    provider_adapter,
    provider_default_command,
    provider_registry,
)


def _spec(**overrides: object) -> ProviderLaunchSpec:
    values: dict[str, object] = {
        "action": "start",
        "command": ("qwen", "exec"),
        "model": "qwen-model",
        "reasoning_effort": "medium",
        "service_tier": "priority",
        "session_id": None,
        "run_root": Path("C:/run"),
        "last_message_path": Path("C:/run/.agent-workspace/last-message"),
    }
    values.update(overrides)
    return ProviderLaunchSpec(**values)  # type: ignore[arg-type]


class QwenProviderTests(unittest.TestCase):
    def test_native_registry_and_canonical_runner_path(self) -> None:
        registry = provider_registry()
        self.assertIn("qwen-code", registry)
        self.assertEqual("QwenCodeProviderAdapter", registry["qwen-code"].adapter_class)
        self.assertIsInstance(provider_adapter("qwen-code"), QwenCodeProviderAdapter)
        self.assertEqual(("qwen", "exec"), provider_default_command("qwen-code"))
        self.assertFalse(registry["qwen-code"].capabilities.notification)

        invocation = CanonicalInvocation(
            schema=CANONICAL_INVOCATION_SCHEMA,
            action="start",
            run_root=Path("C:/run"),
            runtime_root=Path("C:/runtime"),
            lane_id="lane-qwen",
            worker_invocation_id="worker-qwen",
            cohort_id="cohort-qwen",
            workflow_id="workflow",
            workflow_version="v1",
            task_card_id="task",
            task_card_revision="1",
            task_card_sha256="0" * 64,
            role="worker",
            provider_id="qwen-code",
            provider_model="qwen-model",
            provider_options={},
            profile={},
            prompt_bundle={},
            output_paths={"last_message": Path("C:/run/.agent-workspace/last-message")},
            event_log_path=Path("C:/runtime/events.jsonl"),
            resources=(),
            repository=None,
            requested_session_id=None,
            label="qwen",
            task="task",
            phase="implementation",
        )
        self.assertEqual(["qwen", "exec"], invocation.provider_launch_record()["command"])

    def test_start_and_resume_stream_json_argv(self) -> None:
        adapter = QwenCodeProviderAdapter()
        self.assertEqual(
            [
                "qwen",
                "exec",
                "--approval-mode=yolo",
                "--model",
                "qwen-model",
                "--output-format",
                "stream-json",
            ],
            adapter.build_argv(_spec()),
        )
        self.assertEqual(
            [
                "qwen",
                "exec",
                "--approval-mode=yolo",
                "--model",
                "qwen-model",
                "--output-format",
                "stream-json",
                "--resume",
                "session-1",
            ],
            adapter.build_argv(_spec(action="resume", session_id="session-1")),
        )
        with self.assertRaisesRegex(ProviderAdapterError, "resume requires"):
            adapter.build_argv(_spec(action="resume"))

    def test_unsupported_invocation_fields_reject_loudly(self) -> None:
        unsupported = {
            "reasoning_effort": "high",
            "service_tier": "none",
            "permission_mode": "default",
            "allowed_tools": ["Read"],
            "disallowed_tools": ["Bash"],
            "mcp_config": "mcp.json",
            "config_overrides": ["model_provider=ollama"],
            "sandbox": "danger-full-access",
            "approval_policy": "on-request",
        }
        for name, value in unsupported.items():
            with self.subTest(name=name):
                with self.assertRaisesRegex(InvocationValidationError, name):
                    _provider({"id": "qwen-code", "model": "qwen-model", name: value})
        with self.assertRaisesRegex(InvocationValidationError, "notification"):
            _provider(
                {
                    "id": "qwen-code",
                    "model": "qwen-model",
                    "notification": True,
                }
            )

    def test_interrupt_and_exit_130_are_cancelled(self) -> None:
        adapter = QwenCodeProviderAdapter()
        started = adapter.parse_transcript_line(
            b'{"type":"system","subtype":"init","session_id":"session-1"}'
        )
        self.assertIsNotNone(started)
        self.assertEqual("STARTED", started.kind)  # type: ignore[union-attr]
        interrupt = adapter.parse_transcript_line(
            b'{"type":"interrupt","session_id":"session-1"}'
        )
        self.assertIsNotNone(interrupt)
        self.assertEqual("CANCELLED", interrupt.kind)  # type: ignore[union-attr]
        subtype_interrupt = adapter.parse_transcript_line(
            b'{"type":"result","subtype":"interrupt","session_id":"session-1"}'
        )
        self.assertIsNotNone(subtype_interrupt)
        self.assertEqual(
            "CANCELLED", subtype_interrupt.kind  # type: ignore[union-attr]
        )
        self.assertEqual("CANCELLED", adapter.terminal_outcome(None, 130))
        self.assertEqual("FAILED", adapter.terminal_outcome(None, 1))
        self.assertEqual(
            "COMPLETED",
            adapter.parse_transcript_line(
                b'{"type":"result","subtype":"success","session_id":"session-1"}'
            ).kind,  # type: ignore[union-attr]
        )
        self.assertEqual(
            "FAILED",
            adapter.parse_transcript_line(
                b'{"type":"result","subtype":"error","session_id":"session-1"}'
            ).kind,  # type: ignore[union-attr]
        )


if __name__ == "__main__":
    unittest.main()
