"""Focused deterministic checks for the versioned provider-adapter contract.

REQ-O31..REQ-O36: a small versioned registry admits a separately registered
external CLI adapter without generic-core edits; Codex and Claude Code
built-ins preserve supported behavior; capabilities and unsupported
operations are truthful and actionable; provider semantics (command
construction, prompt transport, result/session parsing, permission mapping,
redacted provenance) live in the selected adapter; resume-or-handoff never
fabricates continuity; evidence binds identity/version/capabilities/digest/
session/redacted provenance.  Only deterministic fake CLI fixtures are used.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from orchestrator_harness.invocation import (
    CANONICAL_INVOCATION_SCHEMA,
    InvocationValidationError,
    parse_canonical_invocation,
)
from orchestrator_harness.profile import ProfileError, RuntimeProfile
from orchestrator_harness.provider import (
    NOTIFICATION_WAKE_TEXT,
    PROVIDER_ADAPTER_SCHEMA,
    PROVIDER_HANDOFF_SCHEMA,
    BaseProviderAdapter,
    ClaudeCodeProviderAdapter,
    CodexProviderAdapter,
    ProviderAdapterError,
    ProviderCapabilities,
    ProviderEvent,
    ProviderLaunchSpec,
    build_provider_evidence,
    classify_operation,
    decide_resume_or_handoff,
    provider_adapter,
    provider_config_digest,
    provider_registry,
    redact_command,
    register_provider_adapter,
    structured_handoff,
    unregister_provider_adapter,
)


class FakeCliProviderAdapter(BaseProviderAdapter):
    """Deterministic third-party fake CLI adapter used only by these tests."""

    provider_id = "fake-cli"

    def build_argv(self, spec: ProviderLaunchSpec) -> list[str]:
        if spec.action not in {"start", "resume"}:
            raise ProviderAdapterError("fake-cli action must be start or resume")
        argv = [*spec.command, "--run"]
        if spec.action == "resume":
            if not spec.session_id:
                raise ProviderAdapterError("fake-cli resume requires a session ID")
            argv.extend(["--resume", spec.session_id])
        argv.extend(["--model", spec.model])
        if spec.permission_mode:
            argv.extend(["--permission-mode", spec.permission_mode])
        return argv

    def encode_prompt(self, prompt: bytes) -> bytes:
        if not isinstance(prompt, bytes) or not prompt:
            raise ProviderAdapterError("fake-cli prompt must be non-empty bytes")
        return prompt

    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None:
        text = line.decode("utf-8", errors="replace").strip()
        if text.startswith("session:"):
            return ProviderEvent(
                "STARTED", session_id=text.split(":", 1)[1], raw_type="session"
            )
        if text == "done":
            return ProviderEvent("COMPLETED", outcome="COMPLETED", raw_type="done")
        if text == "error":
            return ProviderEvent("FAILED", outcome="FAILED", raw_type="error")
        return None

    def terminal_outcome(self, event: ProviderEvent | None, exit_code: int) -> str:
        if event is not None and event.kind in {"FAILED", "CANCELLED"}:
            return event.kind
        return "COMPLETED" if exit_code == 0 else "FAILED"

    def redact_argv(self, argv: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        # PA-ROOT-COMPLETION-016: the fake CLI states its redaction choice
        # explicitly; it is generic-safe and delegates to redact_command.
        return redact_command(argv)


class NoRedactAdapter:
    """A contract-shaped adapter that does not own redacted provenance."""

    provider_id = "no-redact"

    def build_argv(self, spec: ProviderLaunchSpec) -> list[str]:
        return [*spec.command]

    def encode_prompt(self, prompt: bytes) -> bytes:
        return prompt

    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None:
        return None

    def terminal_outcome(self, event: ProviderEvent | None, exit_code: int) -> str:
        return "COMPLETED" if exit_code == 0 else "FAILED"


class BaseOnlyNotificationAdapter(BaseProviderAdapter):
    """Declares notification but keeps the base (non-delivery) seam."""

    provider_id = "base-only-notification"

    def build_argv(self, spec: ProviderLaunchSpec) -> list[str]:
        return [*spec.command]

    def encode_prompt(self, prompt: bytes) -> bytes:
        return prompt

    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None:
        return None

    def terminal_outcome(self, event: ProviderEvent | None, exit_code: int) -> str:
        return "COMPLETED" if exit_code == 0 else "FAILED"

    def redact_argv(self, argv: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        # PA-ROOT-COMPLETION-016: explicit redaction choice (generic-safe).
        return redact_command(argv)

    def notification_wake_text(self) -> str | None:
        return NOTIFICATION_WAKE_TEXT


class InheritedBaseRedactAdapter(BaseProviderAdapter):
    """A contract-shaped adapter that inherits the base generic redaction.

    It implements every required method except ``redact_argv``; the inherited
    ``BaseProviderAdapter`` generic marker fallback must not satisfy the
    adapter-owned provenance contract (PA-ROOT-COMPLETION-016).
    """

    provider_id = "inherited-base-redact"

    def build_argv(self, spec: ProviderLaunchSpec) -> list[str]:
        return [*spec.command]

    def encode_prompt(self, prompt: bytes) -> bytes:
        return prompt

    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None:
        return None

    def terminal_outcome(self, event: ProviderEvent | None, exit_code: int) -> str:
        return "COMPLETED" if exit_code == 0 else "FAILED"


def _spec(**overrides: object) -> ProviderLaunchSpec:
    values: dict[str, object] = {
        "action": "start",
        "command": ("fake-cli",),
        "model": "fake-model",
        "reasoning_effort": "medium",
        "service_tier": "priority",
        "session_id": None,
        "run_root": Path("C:/run"),
        "last_message_path": Path("C:/run/.agent-workspace/last"),
    }
    values.update(overrides)
    return ProviderLaunchSpec(**values)  # type: ignore[arg-type]


class ProviderAdapterRegistryTests(unittest.TestCase):
    def tearDown(self) -> None:
        unregister_provider_adapter("fake-cli")

    def test_builtin_registry_is_versioned_and_truthful(self) -> None:
        registry = provider_registry()
        self.assertEqual({"claude-code", "codex"}, set(registry))
        for provider_id in ("codex", "claude-code"):
            registration = registry[provider_id]
            self.assertEqual(PROVIDER_ADAPTER_SCHEMA, registration.schema)
            self.assertTrue(registration.version)
            self.assertTrue(registration.adapter_class)
            self.assertEqual(
                set(registration.capabilities.as_record()),
                {
                    "launch",
                    "prompt",
                    "event_result",
                    "session",
                    "resume",
                    "permission",
                    "configuration",
                    "notification",
                },
            )
        self.assertTrue(registry["codex"].capabilities.notification)
        self.assertFalse(registry["claude-code"].capabilities.notification)
        self.assertTrue(registry["codex"].capabilities.resume)
        self.assertTrue(registry["claude-code"].capabilities.resume)

    def test_builtin_adapters_preserve_supported_behavior(self) -> None:
        codex = provider_adapter("codex")
        self.assertIsInstance(codex, CodexProviderAdapter)
        argv = codex.build_argv(_spec(action="resume", session_id="session-1"))
        self.assertIn("resume", argv)
        self.assertIn("session-1", argv)
        self.assertEqual(
            "COMPLETED",
            codex.parse_transcript_line(
                b'{"type":"turn.completed","thread_id":"session-1"}'
            ).kind,  # type: ignore[union-attr]
        )
        self.assertEqual("FAILED", codex.terminal_outcome(None, 1))

        claude = provider_adapter("claude-code")
        self.assertIsInstance(claude, ClaudeCodeProviderAdapter)
        argv = claude.build_argv(
            _spec(action="resume", session_id="session-1", permission_mode="default")
        )
        self.assertIn("--resume", argv)
        self.assertIn("--permission-mode", argv)
        self.assertEqual(
            "STARTED",
            claude.parse_transcript_line(
                b'{"type":"system","subtype":"init","session_id":"session-1"}'
            ).kind,  # type: ignore[union-attr]
        )

    def test_codex_keeps_user_configuration_for_a_prepared_overlay(self) -> None:
        codex = CodexProviderAdapter()
        project_root = Path("/prepared/subagent-worktree")
        argv = codex.build_argv(
            _spec(trusted_project_root=project_root)
        )

        self.assertNotIn("--ignore-user-config", argv)
        self.assertIn("--dangerously-bypass-hook-trust", argv)
        self.assertIn(
            f'projects.{json.dumps(str(project_root))}.trust_level="trusted"', argv
        )

    def test_foreign_adapter_registers_and_is_selectable_without_core_edits(
        self,
    ) -> None:
        registration = register_provider_adapter(
            "fake-cli",
            FakeCliProviderAdapter(),
            version="fake-cli-v1",
            capabilities=ProviderCapabilities(
                launch=True,
                prompt=True,
                event_result=True,
                session=True,
                resume=True,
                permission=True,
                configuration=True,
                notification=False,
            ),
        )
        self.assertEqual("fake-cli", registration.provider_id)
        self.assertEqual("fake-cli-v1", registration.version)
        self.assertIn("fake-cli", provider_registry())
        adapter = provider_adapter("fake-cli")
        self.assertIsInstance(adapter, FakeCliProviderAdapter)
        argv = adapter.build_argv(_spec())
        self.assertEqual(["fake-cli", "--run", "--model", "fake-model"], argv)

    def test_foreign_adapter_is_admitted_by_invocation_and_profile(self) -> None:
        register_provider_adapter(
            "fake-cli",
            FakeCliProviderAdapter(),
            version="fake-cli-v1",
            capabilities=ProviderCapabilities(
                launch=True,
                prompt=True,
                event_result=True,
                session=True,
                resume=True,
                permission=True,
                configuration=True,
                notification=False,
            ),
        )
        profile = RuntimeProfile(
            profile_id="profile-fake",
            role="implementer",
            provider="fake-cli",
            model="fake-model",
            tools=("Read",),
            capabilities=("repo",),
            resources=("resource-1",),
        )
        self.assertEqual("fake-cli", profile.provider)
        invocation = parse_canonical_invocation(
            {
                "schema": CANONICAL_INVOCATION_SCHEMA,
                "action": "start",
                "run_root": "C:/run",
                "runtime_root": "C:/runtime",
                "lane_id": "lane-1",
                "worker_invocation_id": "worker-1",
                "cohort_id": "cohort-1",
                "workflow": {"id": "wf-1", "version": "1"},
                "task_card": {
                    "id": "card-1",
                    "revision": "1",
                    "sha256": "a" * 64,
                },
                "role": "implementer",
                "provider": {"id": "fake-cli", "model": "fake-model"},
                "profile": profile.to_record(),
                "prompt_bundle": {
                    "schema": "orchestrator-prompt-bundle/v1",
                    "version": 1,
                    "workflow_id": "wf-1",
                    "task_card_id": "card-1",
                    "profile_id": "profile-fake",
                    "components": [],
                    "final_sha256": "b" * 64,
                    "final_size": 0,
                    "bundle_sha256": "c" * 64,
                },
                "output_paths": {
                    "status": "C:/run/.agent-workspace/status.json",
                    "jsonl": "C:/run/.agent-workspace/output.jsonl",
                    "stderr": "C:/run/.agent-workspace/output.stderr.log",
                    "last_message": "C:/run/.agent-workspace/last-message.txt",
                },
                "event_log_path": "C:/runtime/events/events.jsonl",
                "resources": ["resource-1"],
            }
        )
        self.assertEqual("fake-cli", invocation.provider_id)

    def test_unregistered_provider_is_rejected_by_invocation_and_profile(self) -> None:
        with self.assertRaises(InvocationValidationError):
            parse_canonical_invocation(
                {
                    "schema": CANONICAL_INVOCATION_SCHEMA,
                    "action": "start",
                    "run_root": "C:/run",
                    "runtime_root": "C:/runtime",
                    "lane_id": "lane-1",
                    "worker_invocation_id": "worker-1",
                    "cohort_id": "cohort-1",
                    "workflow": {"id": "wf-1", "version": "1"},
                    "task_card": {
                        "id": "card-1",
                        "revision": "1",
                        "sha256": "a" * 64,
                    },
                    "role": "implementer",
                    "provider": {"id": "not-registered", "model": "m"},
                    "profile": {
                        "schema": "orchestrator-runtime-profile/v1",
                        "id": "p",
                        "role": "implementer",
                        "provider": "not-registered",
                        "model": "m",
                        "tools": [],
                        "capabilities": [],
                        "resources": [],
                    },
                    "prompt_bundle": {
                        "schema": "orchestrator-prompt-bundle/v1",
                        "version": 1,
                        "workflow_id": "wf-1",
                        "task_card_id": "card-1",
                        "profile_id": "p",
                        "components": [],
                        "final_sha256": "b" * 64,
                        "final_size": 0,
                        "bundle_sha256": "c" * 64,
                    },
                    "output_paths": {
                        "status": "C:/run/status.json",
                        "jsonl": "C:/run/output.jsonl",
                        "stderr": "C:/run/output.stderr.log",
                        "last_message": "C:/run/last-message.txt",
                    },
                    "event_log_path": "C:/runtime/events/events.jsonl",
                    "resources": [],
                }
            )
        with self.assertRaises(ProfileError):
            RuntimeProfile(
                profile_id="p",
                role="implementer",
                provider="not-registered",
                model="m",
                tools=(),
                capabilities=(),
                resources=(),
            )
        with self.assertRaises(ProviderAdapterError):
            provider_adapter("not-registered")

    def test_unsupported_operation_returns_actionable_classified_result(self) -> None:
        result = classify_operation("claude-code", "notification")
        self.assertFalse(result.supported)
        self.assertEqual("notification", result.operation)
        self.assertTrue(result.actionable)
        self.assertEqual(PROVIDER_ADAPTER_SCHEMA, result.schema)
        supported = classify_operation("codex", "resume")
        self.assertTrue(supported.supported)
        with self.assertRaises(ProviderAdapterError):
            classify_operation("codex", "not-an-operation")

    def test_provenance_redaction_is_deterministic(self) -> None:
        argv = [
            "fake-cli",
            "--api-key",
            "sk-live-secret-123",
            "--token",
            "abc",
            "--model",
            "fake-model",
        ]
        redacted = redact_command(argv)
        self.assertEqual(
            (
                "fake-cli",
                "<redacted>",
                "<redacted>",
                "<redacted>",
                "<redacted>",
                "--model",
                "fake-model",
            ),
            redacted,
        )
        self.assertNotIn("sk-live-secret-123", redacted)
        self.assertNotIn("abc", redacted)

    def test_evidence_binds_identity_version_capabilities_digest_and_provenance(
        self,
    ) -> None:
        register_provider_adapter(
            "fake-cli",
            FakeCliProviderAdapter(),
            version="fake-cli-v1",
            capabilities=ProviderCapabilities(
                launch=True,
                prompt=True,
                event_result=True,
                session=True,
                resume=True,
                permission=True,
                configuration=True,
                notification=False,
            ),
        )
        spec = _spec()
        argv = FakeCliProviderAdapter().build_argv(spec)
        evidence = build_provider_evidence(
            "fake-cli",
            spec=spec,
            argv=argv,
            worker_invocation_id="worker-1",
            session_id="session-1",
        )
        record = evidence.as_record()
        self.assertEqual("fake-cli", record["provider_id"])
        self.assertEqual("fake-cli-v1", record["adapter_version"])
        self.assertEqual(provider_config_digest(spec), record["configuration_digest"])
        self.assertEqual("worker-1", record["attempt_identity"])
        self.assertEqual("session-1", record["session_id"])
        self.assertEqual(
            ["fake-cli", "--run", "--model", "fake-model"], record["command_provenance"]
        )
        self.assertFalse(record["capabilities"]["notification"])

    def test_resume_or_handoff_matrix_never_fabricates_continuity(self) -> None:
        adapter = FakeCliProviderAdapter()
        # Unsupported resume capability -> declared handoff.
        register_provider_adapter(
            "fake-cli",
            adapter,
            version="fake-cli-v1",
            capabilities=ProviderCapabilities(
                launch=True,
                prompt=True,
                event_result=True,
                session=True,
                resume=False,
                permission=True,
                configuration=True,
                notification=False,
            ),
        )
        decision = decide_resume_or_handoff(
            adapter,
            role="implementer",
            logical_task_id="task-1",
            worker_invocation_id="worker-1",
            requested_session_id="session-2",
            persisted_session_id="session-1",
        )
        self.assertEqual("HANDOFF", decision.mode)
        self.assertIsNotNone(decision.handoff)
        handoff = decision.handoff.as_record()  # type: ignore[union-attr]
        self.assertEqual(PROVIDER_HANDOFF_SCHEMA, handoff["schema"])
        self.assertEqual("DECLARED_SAME_ROLE_HANDOFF", handoff["continuity"])
        self.assertFalse(handoff["fabricated_continuity"])
        self.assertTrue(handoff["preserved_logical_task_state"])
        self.assertEqual("session-1", handoff["prior_session_id"])
        self.assertEqual("session-2", handoff["requested_session_id"])

        # Identity mismatch -> declared handoff.
        register_provider_adapter(
            "fake-cli",
            adapter,
            version="fake-cli-v1",
            capabilities=ProviderCapabilities(
                launch=True,
                prompt=True,
                event_result=True,
                session=True,
                resume=True,
                permission=True,
                configuration=True,
                notification=False,
            ),
        )
        mismatch = decide_resume_or_handoff(
            adapter,
            role="implementer",
            logical_task_id="task-1",
            worker_invocation_id="worker-1",
            requested_session_id="session-2",
            persisted_session_id="session-1",
            identity_mismatch=True,
        )
        self.assertEqual("HANDOFF", mismatch.mode)
        self.assertIn("mismatch", mismatch.reason)

        session_mismatch = decide_resume_or_handoff(
            adapter,
            role="implementer",
            logical_task_id="task-1",
            worker_invocation_id="worker-1",
            requested_session_id="session-2",
            persisted_session_id="session-1",
        )
        self.assertEqual("HANDOFF", session_mismatch.mode)
        self.assertIn("does not match", session_mismatch.reason)

        # Matching identity -> RESUME.
        resume = decide_resume_or_handoff(
            adapter,
            role="implementer",
            logical_task_id="task-1",
            worker_invocation_id="worker-1",
            requested_session_id="session-1",
            persisted_session_id="session-1",
        )
        self.assertEqual("RESUME", resume.mode)
        self.assertIsNone(resume.handoff)

    def test_structured_handoff_is_explicit_and_same_role(self) -> None:
        handoff = structured_handoff(
            role="implementer",
            logical_task_id="task-1",
            worker_invocation_id="worker-1",
            provider_id="fake-cli",
            prior_session_id="session-1",
            requested_session_id=None,
            reason="resume unsupported by adapter",
        )
        record = handoff.as_record()
        self.assertEqual("implementer", record["role"])
        self.assertEqual("task-1", record["logical_task_id"])
        self.assertEqual("worker-1", record["worker_invocation_id"])
        self.assertEqual("fake-cli", record["provider_id"])
        self.assertFalse(record["fabricated_continuity"])

    def test_builtin_cannot_be_unregistered_and_foreign_can(self) -> None:
        with self.assertRaises(ProviderAdapterError):
            unregister_provider_adapter("codex")
        register_provider_adapter(
            "fake-cli",
            FakeCliProviderAdapter(),
            version="fake-cli-v1",
            capabilities=ProviderCapabilities(
                launch=True,
                prompt=True,
                event_result=True,
                session=True,
                resume=True,
                permission=True,
                configuration=True,
                notification=False,
            ),
        )
        self.assertTrue(unregister_provider_adapter("fake-cli"))
        self.assertNotIn("fake-cli", provider_registry())
        with self.assertRaises(ProviderAdapterError):
            provider_adapter("fake-cli")

    def test_last_message_path_is_owned_by_the_codex_adapter(self) -> None:
        codex = provider_adapter("codex")
        effective = codex.last_message_path(Path("C:/run"), Path("last-message.txt"))
        self.assertEqual(Path("C:/run/.agent-workspace/last-message.txt"), effective)
        claude = provider_adapter("claude-code")
        self.assertIsNone(
            claude.last_message_path(Path("C:/run"), Path("last-message.txt"))
        )

    def test_redact_argv_is_a_required_adapter_contract_method(self) -> None:
        # PA-R1-001: complete redacted command provenance is an explicit
        # required selected-adapter responsibility; an adapter without it is
        # rejected at registration.
        with self.assertRaises(ProviderAdapterError):
            register_provider_adapter(
                "no-redact",
                NoRedactAdapter(),
                version="no-redact-v1",
                capabilities=ProviderCapabilities(
                    launch=True,
                    prompt=True,
                    event_result=True,
                    session=True,
                    resume=True,
                    permission=True,
                    configuration=True,
                    notification=False,
                ),
            )

    def test_inherited_base_redact_argv_is_rejected_before_selection(self) -> None:
        # PA-ROOT-COMPLETION-016: a BaseProviderAdapter subclass without an
        # explicit redact_argv override cannot satisfy the adapter-owned
        # provenance contract.  Registration is rejected before selection or
        # evidence construction; the provider never appears in the registry.
        with self.assertRaises(ProviderAdapterError) as ctx:
            register_provider_adapter(
                "inherited-base-redact",
                InheritedBaseRedactAdapter(),
                version="inherited-base-redact-v1",
                capabilities=ProviderCapabilities(
                    launch=True,
                    prompt=True,
                    event_result=True,
                    session=True,
                    resume=True,
                    permission=True,
                    configuration=True,
                    notification=False,
                ),
            )
        self.assertIn("redact_argv", str(ctx.exception))
        self.assertNotIn("inherited-base-redact", provider_registry())
        with self.assertRaises(ProviderAdapterError):
            provider_adapter("inherited-base-redact")
        with self.assertRaises(ProviderAdapterError):
            build_provider_evidence(
                "inherited-base-redact",
                spec=_spec(),
                argv=["inherited-base-redact"],
                worker_invocation_id="worker-1",
            )

    def test_notification_true_requires_a_real_delivery_seam(self) -> None:
        # PA-R1-002: notification=true is admissible only when the adapter
        # supplies one real safe-boundary delivery binding; the base default
        # (returns None) is not a delivery and must declare notification=False.
        with self.assertRaises(ProviderAdapterError):
            register_provider_adapter(
                "base-only-notification",
                BaseOnlyNotificationAdapter(),
                version="base-only-notification-v1",
                capabilities=ProviderCapabilities(
                    launch=True,
                    prompt=True,
                    event_result=True,
                    session=True,
                    resume=True,
                    permission=True,
                    configuration=True,
                    notification=True,
                ),
            )


if __name__ == "__main__":
    unittest.main()
