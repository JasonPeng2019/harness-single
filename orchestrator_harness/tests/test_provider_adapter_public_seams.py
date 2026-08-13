"""Public-seam tests for the production provider-adapter lifecycle.

REQ-O31..REQ-O38 through the real controller boundary, real resume
admission, the ManagerEventRouter/DeliveryCoordinator queue authority, and
the packaged installed Codex hook routes.  Only deterministic fake CLI and
hook fixtures are used; no real provider, credentials, network, hardware,
MCP, USB, or WSL real-agent is ever launched.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import orchestrator_harness.lane_controller as controller
from orchestrator_harness.codex_adapter import (
    activate_codex_binding,
    create_codex_adapter,
    install_codex_adapter,
)
from orchestrator_harness.host_adapters import (
    NOTIFICATION_EXTERNALLY_BLOCKED,
    NOTIFICATION_OPEN,
    NOTIFICATION_STOP_EXTERNAL_DECLARED,
    NOTIFICATION_STOP_EXTERNAL_RESPONSE_REQUIRED,
    NOTIFICATION_STOP_OPEN_ITEMS_REMAIN,
    NOTIFICATION_STOP_QUEUE_EMPTY,
    DeliveryCoordinator,
    HostAdapterError,
)
from orchestrator_harness.invocation import CANONICAL_INVOCATION_SCHEMA
from orchestrator_harness.notifications import ManagerEventRouter
from orchestrator_harness.profile import RuntimeProfile
from orchestrator_harness.provider import (
    NOTIFICATION_MODE_SAFE_BOUNDARY_ONLY,
    NOTIFICATION_MODE_WAKE,
    NOTIFICATION_WAKE_TEXT,
    BaseProviderAdapter,
    ClaudeCodeProviderAdapter,
    CodexProviderAdapter,
    ProviderAdapterError,
    ProviderCapabilities,
    ProviderEvent,
    ProviderLaunchSpec,
    notification_mode,
    provider_adapter,
    redact_command,
    register_provider_adapter,
    unregister_provider_adapter,
)
from orchestrator_harness.prompt_bundle import prompt_bundle_record_from_paths
from orchestrator_harness.task import TASK_CARD_SCHEMA, TASK_RESULT_SCHEMA, record_sha256


FAKE_CLI = r'''
import json, os, sys
from pathlib import Path
marker = Path(sys.argv[1])
marker.write_text(marker.read_text(encoding="utf-8") + "launch\n" if marker.exists() else "launch\n", encoding="utf-8")
sys.stdin.read()
print(json.dumps({"type": "thread.started", "thread_id": "fake-cli-session"}), flush=True)
print(json.dumps({"type": "turn.completed"}), flush=True)
'''


FAKE_CLI_ARGV = r'''
import json, sys
from pathlib import Path
Path(sys.argv[1]).write_text(json.dumps(sys.argv), encoding="utf-8")
sys.stdin.read()
print(json.dumps({"type": "thread.started", "thread_id": "fake-cli-session"}), flush=True)
print(json.dumps({"type": "turn.completed"}), flush=True)
'''


BOOTSTRAP_SCRIPT = r'''
"""PA-R1-003 documented same-process bootstrap shape.

Registers one minimal external adapter in this interpreter before canonical
invocation parsing, then calls the native controller.  A fresh interpreter
contains only the built-ins, so registration must happen in the same process
that parses and launches the invocation.
"""
import sys

import orchestrator_harness.lane_controller as controller
from orchestrator_harness.provider import (
    BaseProviderAdapter,
    ProviderCapabilities,
    ProviderEvent,
    ProviderLaunchSpec,
    register_provider_adapter,
)


class MyCliProviderAdapter(BaseProviderAdapter):
    provider_id = "my-cli"

    def build_argv(self, spec: ProviderLaunchSpec) -> list[str]:
        return [*spec.command, "--run", "--model", spec.model]

    def encode_prompt(self, prompt: bytes) -> bytes:
        return prompt

    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None:
        text = line.decode("utf-8", errors="replace").strip()
        if "thread.started" in text:
            return ProviderEvent("STARTED", session_id="my-cli-session", raw_type="thread.started")
        if "turn.completed" in text:
            return ProviderEvent("COMPLETED", outcome="COMPLETED", raw_type="turn.completed")
        return None

    def terminal_outcome(self, event: ProviderEvent | None, exit_code: int) -> str:
        if event is not None and event.kind in {"FAILED", "CANCELLED"}:
            return event.kind
        return "COMPLETED" if exit_code == 0 else "FAILED"

    def redact_argv(self, argv: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        # my-cli owns its credential spelling: --auth values never persist.
        redacted: list[str] = []
        redact_next = False
        for token in argv:
            if redact_next:
                redacted.append("<redacted>")
                redact_next = False
                continue
            if token == "--auth":
                redacted.append("<redacted>")
                redact_next = True
                continue
            redacted.append(token)
        if redact_next:
            redacted.append("<redacted>")
        return tuple(redacted)


register_provider_adapter(
    "my-cli",
    MyCliProviderAdapter(),
    version="my-cli-v1",
    capabilities=ProviderCapabilities(
        launch=True, prompt=True, event_result=True, session=True,
        resume=True, permission=True, configuration=True, notification=False,
    ),
)
sys.exit(controller.main([sys.argv[1]]))
'''


class FakeCliProviderAdapter(BaseProviderAdapter):
    """Deterministic external fake CLI adapter used only by these tests."""

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
        return argv

    def encode_prompt(self, prompt: bytes) -> bytes:
        return prompt

    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None:
        text = line.decode("utf-8", errors="replace").strip()
        if "thread.started" in text:
            return ProviderEvent("STARTED", session_id="fake-cli-session", raw_type="thread.started")
        if "turn.completed" in text:
            return ProviderEvent("COMPLETED", outcome="COMPLETED", raw_type="turn.completed")
        return None

    def terminal_outcome(self, event: ProviderEvent | None, exit_code: int) -> str:
        if event is not None and event.kind in {"FAILED", "CANCELLED"}:
            return event.kind
        return "COMPLETED" if exit_code == 0 else "FAILED"

    def notification_wake_text(self) -> str | None:
        # The fake declares notification=True by default, so it must own the
        # exact content-free wake text as an executable contract.
        return NOTIFICATION_WAKE_TEXT

    def redact_argv(self, argv: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        # PA-R1-001: the fake CLI owns its credential spelling.  It starts
        # from the deterministic generic markers and adds its own --auth
        # spelling that the generic list does not know; the complete argv
        # shape is preserved.
        redacted: list[str] = []
        redact_next = False
        for token in redact_command(argv):
            if redact_next:
                redacted.append("<redacted>")
                redact_next = False
                continue
            if token == "--auth":
                redacted.append("<redacted>")
                redact_next = True
                continue
            redacted.append(token)
        if redact_next:
            redacted.append("<redacted>")
        return tuple(redacted)

    def deliver_notification(
        self,
        coordinator: object,
        notice: object,
        *,
        boundary: str,
    ) -> object:
        # PA-R1-002: the one real safe-boundary delivery binding.  The
        # per-binding DeliveryCoordinator delivers the outstanding notice and
        # returns transport evidence only (never an acknowledgement).
        return coordinator.deliver_at_boundary(notice, boundary=boundary)  # type: ignore[attr-defined]


class SideEffectingFakeAdapter(FakeCliProviderAdapter):
    """Records every adapter method call to prove zero construction."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def build_argv(self, spec: ProviderLaunchSpec) -> list[str]:
        self.calls.append("build_argv")
        return super().build_argv(spec)

    def encode_prompt(self, prompt: bytes) -> bytes:
        self.calls.append("encode_prompt")
        return super().encode_prompt(prompt)

    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None:
        self.calls.append("parse_transcript_line")
        return super().parse_transcript_line(line)

    def terminal_outcome(self, event: ProviderEvent | None, exit_code: int) -> str:
        self.calls.append("terminal_outcome")
        return super().terminal_outcome(event, exit_code)


class ForeignWakeAdapter(BaseProviderAdapter):
    """Foreign adapter with a truthful immediate-wake declaration."""

    provider_id = "foreign-wake"

    def build_argv(self, spec: ProviderLaunchSpec) -> list[str]:
        return [*spec.command, "--wake"]

    def encode_prompt(self, prompt: bytes) -> bytes:
        return prompt

    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None:
        text = line.decode("utf-8", errors="replace").strip()
        if "thread.started" in text:
            return ProviderEvent("STARTED", session_id="fake-cli-session", raw_type="thread.started")
        if "turn.completed" in text:
            return ProviderEvent("COMPLETED", outcome="COMPLETED", raw_type="turn.completed")
        return None

    def terminal_outcome(self, event: ProviderEvent | None, exit_code: int) -> str:
        return "COMPLETED" if exit_code == 0 else "FAILED"

    def redact_argv(self, argv: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        # PA-ROOT-COMPLETION-016: explicit redaction choice (generic-safe).
        return redact_command(argv)

    def notification_wake_text(self) -> str | None:
        return NOTIFICATION_WAKE_TEXT

    def deliver_notification(
        self,
        coordinator: object,
        notice: object,
        *,
        boundary: str,
    ) -> object:
        # PA-R1-002: the foreign adapter owns one real safe-boundary delivery
        # binding through the per-binding DeliveryCoordinator.
        return coordinator.deliver_at_boundary(notice, boundary=boundary)  # type: ignore[attr-defined]


class ForeignSafeBoundaryAdapter(BaseProviderAdapter):
    """Foreign adapter that truthfully declares no immediate wake."""

    provider_id = "foreign-safe"

    def build_argv(self, spec: ProviderLaunchSpec) -> list[str]:
        return [*spec.command, "--safe"]

    def encode_prompt(self, prompt: bytes) -> bytes:
        return prompt

    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None:
        text = line.decode("utf-8", errors="replace").strip()
        if "thread.started" in text:
            return ProviderEvent("STARTED", session_id="fake-cli-session", raw_type="thread.started")
        if "turn.completed" in text:
            return ProviderEvent("COMPLETED", outcome="COMPLETED", raw_type="turn.completed")
        return None

    def terminal_outcome(self, event: ProviderEvent | None, exit_code: int) -> str:
        return "COMPLETED" if exit_code == 0 else "FAILED"

    def redact_argv(self, argv: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        # PA-ROOT-COMPLETION-016: explicit redaction choice (generic-safe).
        return redact_command(argv)


class ForeignInvalidWakeAdapter(BaseProviderAdapter):
    """Foreign adapter declaring notification without an executable wake."""

    provider_id = "foreign-invalid"

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


class ForeignWrongWakeAdapter(BaseProviderAdapter):
    """Foreign adapter declaring notification with a wrong non-empty wake."""

    provider_id = "foreign-wrong-wake"

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
        # Payload-bearing and preemptive: must never register as WAKE.
        return "Inspect queued payload now."


class ForeignMutatedWakeAdapter(ForeignWakeAdapter):
    """Foreign adapter whose wake return is mutated after registration."""

    provider_id = "foreign-mutated"


class MyCliBuildAdapter(BaseProviderAdapter):
    """Test-process stand-in so the bootstrap canonical record can be built.

    The fresh subprocess registers its own ``my-cli`` adapter (the documented
    bootstrap shape); this class only satisfies the in-process
    ``RuntimeProfile`` validation while the invocation record is assembled.
    """

    provider_id = "my-cli"

    def build_argv(self, spec: ProviderLaunchSpec) -> list[str]:
        return [*spec.command, "--run", "--model", spec.model]

    def encode_prompt(self, prompt: bytes) -> bytes:
        return prompt

    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None:
        return None

    def terminal_outcome(self, event: ProviderEvent | None, exit_code: int) -> str:
        return "COMPLETED" if exit_code == 0 else "FAILED"

    def redact_argv(self, argv: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        # PA-ROOT-COMPLETION-016: explicit redaction choice (generic-safe).
        return redact_command(argv)


class ForeignUnknownTerminalAdapter(BaseProviderAdapter):
    """Foreign adapter whose terminal outcome is outside the closed vocabulary.

    REL.R1-001: an external adapter may return a string outside the closed
    provider-neutral COMPLETED/FAILED/CANCELLED vocabulary; the generic
    controller must fail closed instead of publishing PROVIDER_EXITED or
    returning success.
    """

    provider_id = "foreign-unknown"

    def build_argv(self, spec: ProviderLaunchSpec) -> list[str]:
        return [*spec.command, "--unknown"]

    def encode_prompt(self, prompt: bytes) -> bytes:
        return prompt

    def parse_transcript_line(self, line: bytes) -> ProviderEvent | None:
        text = line.decode("utf-8", errors="replace").strip()
        if "thread.started" in text:
            return ProviderEvent("STARTED", session_id="fake-cli-session", raw_type="thread.started")
        if "turn.completed" in text:
            return ProviderEvent("COMPLETED", outcome="COMPLETED", raw_type="turn.completed")
        return None

    def terminal_outcome(self, event: ProviderEvent | None, exit_code: int) -> str:
        return "UNKNOWN"

    def redact_argv(self, argv: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        # PA-ROOT-COMPLETION-016: explicit redaction choice (generic-safe).
        return redact_command(argv)


def _capabilities(**overrides: bool) -> ProviderCapabilities:
    values: dict[str, bool] = {
        "launch": True,
        "prompt": True,
        "event_result": True,
        "session": True,
        "resume": True,
        "permission": True,
        "configuration": True,
        "notification": True,
    }
    values.update(overrides)
    return ProviderCapabilities(**values)


class ProviderAdapterPublicSeamTests(unittest.TestCase):
    """Controller boundary, resume admission, and evidence seams."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.run = self.root / "run"
        self.workspace = self.run / ".agent-workspace"
        self.workspace.mkdir(parents=True)
        self.runtime = self.root / "runtime"
        self.runtime.mkdir()
        self.fake = self.root / "fake_cli.py"
        self.fake.write_text(FAKE_CLI, encoding="utf-8")
        self.marker = self.root / "launches.txt"
        register_provider_adapter(
            "fake-cli",
            FakeCliProviderAdapter(),
            version="fake-cli-1.0",
            capabilities=_capabilities(),
        )

    def tearDown(self) -> None:
        unregister_provider_adapter("fake-cli")
        self.temporary.cleanup()

    def _canonical(self, *, provider_id: str = "fake-cli", notification: bool | None = None) -> dict[str, object]:
        prompt_a = self.run / "prompt-a.md"
        prompt_b = self.run / "prompt-b.md"
        prompt_a.write_bytes(b"workflow\n")
        prompt_b.write_bytes(b"task\n")
        card = {
            "schema": TASK_CARD_SCHEMA,
            "card_id": "card-1",
            "lane_id": "lane-1",
            "stage_cohort_id": "cohort-1",
            "worker_invocation_id": "worker-1",
            "objective": "Do the bounded task",
            "revision": "r1",
        }
        profile = RuntimeProfile(
            "profile-1", "implementer", provider_id, "fake-model", ("Read",), ("repo",), ("resource-1",),
            ("FAKE_CLI_TEST",), ("WORKFLOW_TEST_FLAG",),
        )
        bundle = prompt_bundle_record_from_paths(
            workflow_id="workflow-1",
            task_card_id="card-1",
            profile_id="profile-1",
            paths=(("instructions", prompt_a), ("task", prompt_b)),
            run_root=self.run,
        )
        provider: dict[str, object] = {
            "id": provider_id,
            "model": "fake-model",
            "command": [sys.executable, str(self.fake), str(self.marker)],
        }
        if notification is not None:
            provider["notification"] = notification
        return {
            "schema": CANONICAL_INVOCATION_SCHEMA,
            "action": "start",
            "run_root": str(self.run),
            "runtime_root": str(self.runtime),
            "lane_id": "lane-1",
            "worker_invocation_id": "worker-1",
            "cohort_id": "cohort-1",
            "workflow": {"id": "workflow-1", "version": "1"},
            "task_card": {"id": "card-1", "revision": "r1", "sha256": record_sha256(card)},
            "role": "implementer",
            "provider": provider,
            "profile": profile.to_record(),
            "prompt_bundle": bundle,
            "output_paths": {
                "status": str(self.workspace / "worker_controller.status.json"),
                "jsonl": str(self.workspace / "worker_provider.jsonl"),
                "stderr": str(self.workspace / "worker.stderr.log"),
                "last_message": str(self.workspace / "worker.last-message"),
            },
            "event_log_path": str(self.runtime / "events.jsonl"),
            "resources": ["resource-1"],
        }

    def _write(self, path: Path, value: dict[str, object]) -> None:
        path.write_text(json.dumps(value), encoding="utf-8")

    def _status(self) -> dict[str, object]:
        return json.loads((self.workspace / "worker_controller.status.json").read_text(encoding="utf-8"))

    def test_controller_selects_external_fake_adapter_without_core_edits(self) -> None:
        path = self.workspace / "start.invocation.json"
        self._write(path, self._canonical())
        self.assertEqual(0, controller.main([str(path)]))
        status = self._status()
        self.assertEqual("fake-cli", status["provider_id"])
        self.assertEqual("fake-cli-session", status["provider_session_id"])
        self.assertEqual("launch\n", self.marker.read_text(encoding="utf-8"))
        evidence = status["provider_evidence"]
        self.assertIsNotNone(evidence)
        self.assertEqual("fake-cli", evidence["provider_id"])
        self.assertEqual("fake-cli-1.0", evidence["adapter_version"])
        self.assertTrue(evidence["capabilities"]["launch"])
        self.assertTrue(evidence["configuration_digest"])
        self.assertEqual("worker-1", evidence["attempt_identity"])
        self.assertEqual("fake-cli-session", evidence["session_id"])
        self.assertIn("--run", evidence["command_provenance"])
        self.assertNotIn("AUTHORITATIVE", " ".join(evidence["command_provenance"]))

    def test_launch_false_adapter_never_launches_and_returns_classified_result(self) -> None:
        unregister_provider_adapter("fake-cli")
        register_provider_adapter(
            "fake-cli",
            FakeCliProviderAdapter(),
            version="fake-cli-no-launch",
            capabilities=_capabilities(launch=False),
        )
        path = self.workspace / "start.invocation.json"
        self._write(path, self._canonical())
        self.assertEqual(1, controller.main([str(path)]))
        self.assertFalse(self.marker.exists())
        status = self._status()
        self.assertEqual("PROVIDER_OPERATION_UNSUPPORTED", status["state"])
        results = status["provider_operation_results"]
        launch_result = next(item for item in results if item["operation"] == "launch")
        self.assertFalse(launch_result["supported"])
        self.assertEqual("fake-cli", launch_result["provider_id"])
        self.assertTrue(launch_result["actionable"])
        self.assertIn("launch", launch_result["reason"])

    def test_unsupported_notification_operation_returns_actionable_classified_result(self) -> None:
        unregister_provider_adapter("fake-cli")
        register_provider_adapter(
            "fake-cli",
            FakeCliProviderAdapter(),
            version="fake-cli-no-notification",
            capabilities=_capabilities(notification=False),
        )
        path = self.workspace / "start.invocation.json"
        self._write(path, self._canonical(notification=True))
        self.assertEqual(1, controller.main([str(path)]))
        self.assertFalse(self.marker.exists())
        status = self._status()
        self.assertEqual("PROVIDER_OPERATION_UNSUPPORTED", status["state"])
        results = status["provider_operation_results"]
        notification_result = next(item for item in results if item["operation"] == "notification")
        self.assertFalse(notification_result["supported"])
        self.assertEqual("fake-cli", notification_result["provider_id"])
        self.assertIn("notification", notification_result["reason"])
        self.assertIn("actionable", notification_result)

    def test_supported_resume_proceeds_through_real_admission(self) -> None:
        start = self.workspace / "start.invocation.json"
        self._write(start, self._canonical())
        self.assertEqual(0, controller.main([str(start)]))
        resume = self._canonical()
        resume["action"] = "resume"
        resume["resume"] = {"session_id": "fake-cli-session"}
        resume_path = self.workspace / "resume.invocation.json"
        self._write(resume_path, resume)
        self.assertEqual(0, controller.main([str(resume_path)]))
        status = self._status()
        self.assertTrue(status["resume_admission"]["admitted"])
        self.assertEqual("fake-cli-session", status["provider_session_id"])

    def test_identity_mismatched_resume_emits_structured_handoff_without_fabricated_continuity(self) -> None:
        start = self.workspace / "start.invocation.json"
        self._write(start, self._canonical())
        self.assertEqual(0, controller.main([str(start)]))
        wrong = self._canonical()
        wrong["action"] = "resume"
        wrong["worker_invocation_id"] = "worker-2"
        wrong["resume"] = {"session_id": "fake-cli-session"}
        wrong_path = self.workspace / "wrong.invocation.json"
        self._write(wrong_path, wrong)
        self.assertEqual(1, controller.main([str(wrong_path)]))
        status = self._status()
        self.assertEqual("PROVIDER_HANDOFF", status["state"])
        handoff = status["provider_handoff"]
        self.assertIsNotNone(handoff)
        self.assertFalse(handoff["fabricated_continuity"])
        self.assertEqual("implementer", handoff["role"])
        self.assertEqual("card-1", handoff["logical_task_id"])
        self.assertEqual("fake-cli", handoff["provider_id"])
        self.assertEqual("fake-cli-session", handoff["prior_session_id"])
        self.assertIn("worker_invocation_id", handoff["reason"])
        self.assertIsNone(status["provider_evidence"])

    def test_unsupported_launch_permission_configuration_do_zero_adapter_work(self) -> None:
        # PA-ROOT-COMPLETION-006/011: capability enforcement precedes any
        # adapter construction or provider work, and every unsupported result
        # carries explicit required_actor/required_action through status.
        unregister_provider_adapter("fake-cli")
        side_effecting = SideEffectingFakeAdapter()
        register_provider_adapter(
            "fake-cli",
            side_effecting,
            version="fake-cli-side-effecting",
            capabilities=_capabilities(launch=False, permission=False, configuration=False),
        )
        path = self.workspace / "start.invocation.json"
        self._write(path, self._canonical())
        self.assertEqual(1, controller.main([str(path)]))
        self.assertFalse(self.marker.exists())
        self.assertEqual([], side_effecting.calls)
        status = self._status()
        self.assertEqual("PROVIDER_OPERATION_UNSUPPORTED", status["state"])
        results = status["provider_operation_results"]
        for operation in ("launch", "permission", "configuration"):
            result = next(item for item in results if item["operation"] == operation)
            self.assertFalse(result["supported"])
            self.assertEqual("fake-cli", result["provider_id"])
            self.assertEqual("ROOT-IM", result["required_actor"])
            self.assertTrue(result["required_action"])
            self.assertIn(operation, result["reason"])
        self.assertIsNone(status["provider_evidence"])
        self.assertEqual([], status["launcher_settings"]["argv"])

    def test_exact_resume_false_emits_handoff_with_zero_adapter_construction(self) -> None:
        # PA-ROOT-COMPLETION-007: an exact persisted resume with an adapter
        # that does not declare resume emits the same-role structured handoff
        # with fabricated_continuity=false and zero adapter construction.
        unregister_provider_adapter("fake-cli")
        side_effecting = SideEffectingFakeAdapter()
        register_provider_adapter(
            "fake-cli",
            side_effecting,
            version="fake-cli-no-resume",
            capabilities=_capabilities(resume=False),
        )
        start = self.workspace / "start.invocation.json"
        self._write(start, self._canonical())
        self.assertEqual(0, controller.main([str(start)]))
        self.assertIn("build_argv", side_effecting.calls)
        side_effecting.calls.clear()
        resume = self._canonical()
        resume["action"] = "resume"
        resume["resume"] = {"session_id": "fake-cli-session"}
        resume_path = self.workspace / "resume.invocation.json"
        self._write(resume_path, resume)
        self.assertEqual(1, controller.main([str(resume_path)]))
        self.assertEqual([], side_effecting.calls)
        status = self._status()
        self.assertEqual("PROVIDER_HANDOFF", status["state"])
        handoff = status["provider_handoff"]
        self.assertIsNotNone(handoff)
        self.assertFalse(handoff["fabricated_continuity"])
        self.assertEqual("implementer", handoff["role"])
        self.assertEqual("card-1", handoff["logical_task_id"])
        self.assertEqual("fake-cli", handoff["provider_id"])
        self.assertEqual("fake-cli-session", handoff["prior_session_id"])
        self.assertEqual("fake-cli-session", handoff["requested_session_id"])
        self.assertIn("resume is not a declared capability", handoff["reason"])
        self.assertIsNone(status["provider_evidence"])
        decision = status["provider_resume_decision"]
        self.assertEqual("HANDOFF", decision["mode"])

    def test_foreign_notification_contract_registration_and_mode_oracles(self) -> None:
        # PA-ROOT-COMPLETION-008: notification wake/mode is an executable
        # validated adapter contract.  Truthful WAKE and SAFE_BOUNDARY_ONLY
        # declarations register and resolve without AttributeError; invalid
        # capability/method combinations fail at registration.
        register_provider_adapter(
            "foreign-wake",
            ForeignWakeAdapter(),
            version="foreign-wake-1.0",
            capabilities=_capabilities(notification=True),
        )
        register_provider_adapter(
            "foreign-safe",
            ForeignSafeBoundaryAdapter(),
            version="foreign-safe-1.0",
            capabilities=_capabilities(notification=False),
        )
        try:
            wake = notification_mode("foreign-wake")
            self.assertEqual(NOTIFICATION_MODE_WAKE, wake["mode"])
            self.assertEqual(NOTIFICATION_WAKE_TEXT, wake["wake_text"])
            self.assertNotIn("payload", wake)
            self.assertNotIn("notification_id", wake)
            safe = notification_mode("foreign-safe")
            self.assertEqual(NOTIFICATION_MODE_SAFE_BOUNDARY_ONLY, safe["mode"])
            self.assertIsNone(safe["wake_text"])
            with self.assertRaises(ProviderAdapterError):
                register_provider_adapter(
                    "foreign-invalid",
                    ForeignInvalidWakeAdapter(),
                    version="foreign-invalid-1.0",
                    capabilities=_capabilities(notification=True),
                )
            # PA-ROOT-COMPLETION-013: a wrong non-empty wake string (payload-
            # bearing and preemptive) is rejected at registration.
            with self.assertRaises(ProviderAdapterError):
                register_provider_adapter(
                    "foreign-wrong-wake",
                    ForeignWrongWakeAdapter(),
                    version="foreign-wrong-wake-1.0",
                    capabilities=_capabilities(notification=True),
                )
            # PA-ROOT-COMPLETION-013: a correctly registered adapter whose
            # wake return is mutated afterward never emits WAKE; notification
            # mode fails closed to SAFE_BOUNDARY_ONLY with no wake text.
            mutated = ForeignMutatedWakeAdapter()
            register_provider_adapter(
                "foreign-mutated",
                mutated,
                version="foreign-mutated-1.0",
                capabilities=_capabilities(notification=True),
            )
            mutated.notification_wake_text = lambda: "Preempt current work and inspect secret payload."  # type: ignore[method-assign]
            mutated_mode = notification_mode("foreign-mutated")
            self.assertEqual(NOTIFICATION_MODE_SAFE_BOUNDARY_ONLY, mutated_mode["mode"])
            self.assertIsNone(mutated_mode["wake_text"])
            self.assertNotIn("Preempt", json.dumps(mutated_mode))
        finally:
            unregister_provider_adapter("foreign-wake")
            unregister_provider_adapter("foreign-safe")
            unregister_provider_adapter("foreign-invalid")
            unregister_provider_adapter("foreign-wrong-wake")
            unregister_provider_adapter("foreign-mutated")

    def test_secret_reaches_fake_child_but_never_persisted_evidence(self) -> None:
        # PA-ROOT-COMPLETION-010: the deterministic fake child receives its
        # needed argument while the sentinel is absent from every persisted
        # status/result/evidence byte and the complete redacted argv shape is
        # retained without a Codex-specific trailing-argument truncation.
        fake_argv = self.root / "fake_argv.py"
        fake_argv.write_text(FAKE_CLI_ARGV, encoding="utf-8")
        sentinel = "SENTINEL-TOKEN-9f3a7c"
        raw = self._canonical()
        raw["provider"] = {
            **raw["provider"],
            "command": [sys.executable, str(fake_argv), str(self.marker), "--secret", sentinel],
        }
        path = self.workspace / "start.invocation.json"
        self._write(path, raw)
        self.assertEqual(0, controller.main([str(path)]))
        child_argv = json.loads(self.marker.read_text(encoding="utf-8"))
        self.assertIn(sentinel, child_argv)
        status = self._status()
        status_bytes = json.dumps(status).encode("utf-8")
        self.assertNotIn(sentinel.encode("utf-8"), status_bytes)
        evidence = status["provider_evidence"]
        self.assertNotIn(sentinel.encode("utf-8"), json.dumps(evidence).encode("utf-8"))
        provenance = evidence["command_provenance"]
        # Full redacted argv shape is retained: python + script + marker +
        # --secret + <redacted> + --run + --model + fake-model.  No
        # Codex-specific trailing-argument truncation is applied.
        self.assertEqual(8, len(provenance))
        self.assertEqual("<redacted>", provenance[3])
        self.assertEqual("<redacted>", provenance[4])
        self.assertEqual("--run", provenance[5])
        self.assertEqual("--model", provenance[6])
        self.assertEqual("fake-model", provenance[7])
        settings = status["launcher_settings"]
        self.assertNotIn(sentinel.encode("utf-8"), json.dumps(settings).encode("utf-8"))
        self.assertEqual(provenance, settings["argv"])
        self.assertTrue(settings["configuration_digest"])
        self.assertNotIn("config_overrides", settings)

    def test_terminal_failure_retains_complete_redacted_provider_evidence(self) -> None:
        # PA-ROOT-COMPLETION-012-ORACLE: a deterministic terminal-failure
        # controller run retains adapter identity/version, complete
        # capabilities, configuration digest, attempt identity, actual session
        # identity, and redacted full command provenance in status/result
        # evidence.  The fake child receives a synthetic credential-shaped
        # sentinel and emits an arbitrary terminal payload; both are absent
        # from every persisted status/result/provider-evidence byte.
        sentinel = "SENTINEL-TOKEN-7f2d9c"
        payload = "ARBITRARY-PAYLOAD-4b8e1a"
        fake_fail = self.root / "fake_fail.py"
        fake_fail.write_text(
            "import json, sys\n"
            "from pathlib import Path\n"
            "Path(sys.argv[1]).write_text(json.dumps({'argv': sys.argv, 'payload': %r}), encoding='utf-8')\n"
            "sys.stdin.read()\n"
            "print(json.dumps({'type': 'thread.started', 'thread_id': 'fake-cli-session'}), flush=True)\n"
            "print(json.dumps({'type': 'turn.failed', 'outcome': 'FAILED', 'payload': %r}), flush=True)\n"
            "sys.exit(1)\n" % (payload, payload),
            encoding="utf-8",
        )
        raw = self._canonical()
        raw["provider"] = {
            **raw["provider"],
            "command": [sys.executable, str(fake_fail), str(self.marker), "--secret", sentinel],
        }
        path = self.workspace / "start.invocation.json"
        self._write(path, raw)
        self.assertEqual(1, controller.main([str(path)]))
        child_record = json.loads(self.marker.read_text(encoding="utf-8"))
        self.assertIn(sentinel, child_record["argv"])
        self.assertEqual(payload, child_record["payload"])
        status = self._status()
        self.assertEqual("PROVIDER_EXITED", status["state"])
        self.assertEqual("FAILED", status["provider_terminal_outcome"])
        status_bytes = json.dumps(status).encode("utf-8")
        self.assertNotIn(sentinel.encode("utf-8"), status_bytes)
        self.assertNotIn(payload.encode("utf-8"), status_bytes)
        evidence = status["provider_evidence"]
        self.assertIsNotNone(evidence)
        evidence_bytes = json.dumps(evidence).encode("utf-8")
        self.assertNotIn(sentinel.encode("utf-8"), evidence_bytes)
        self.assertNotIn(payload.encode("utf-8"), evidence_bytes)
        self.assertEqual("fake-cli", evidence["provider_id"])
        self.assertEqual("fake-cli-1.0", evidence["adapter_version"])
        capabilities = evidence["capabilities"]
        for name in ("launch", "prompt", "event_result", "session", "resume", "permission", "configuration", "notification"):
            self.assertTrue(capabilities[name])
        self.assertTrue(evidence["configuration_digest"])
        self.assertEqual("worker-1", evidence["attempt_identity"])
        self.assertEqual("fake-cli-session", evidence["session_id"])
        provenance = evidence["command_provenance"]
        # Complete redacted argv shape: python + script + marker + --secret +
        # <redacted> + --run + --model + fake-model (no trailing truncation).
        self.assertEqual(8, len(provenance))
        self.assertEqual("<redacted>", provenance[3])
        self.assertEqual("<redacted>", provenance[4])
        self.assertIn("--run", provenance)
        self.assertIn("--model", provenance)
        self.assertEqual("fake-model", provenance[7])
        settings = status["launcher_settings"]
        self.assertEqual(evidence["configuration_digest"], settings["configuration_digest"])
        self.assertEqual(provenance, settings["argv"])
        self.assertNotIn("config_overrides", settings)
        # The arbitrary payload was exercised through the real transcript
        # drain, while the sentinel never reaches the transcript.
        transcript = (self.workspace / "worker_provider.jsonl").read_text(encoding="utf-8")
        self.assertIn(payload, transcript)
        self.assertNotIn(sentinel, transcript)

    def test_unknown_terminal_outcome_fails_closed_with_controller_failure(self) -> None:
        # REL.R1-001: the selected adapter's provider-neutral terminal outcome
        # must be in the closed COMPLETED/FAILED/CANCELLED vocabulary before
        # the generic controller may publish PROVIDER_EXITED or return
        # success.  An external adapter returning UNKNOWN after a child exit
        # 0 and a shape-valid RESULT.json yields truthful CONTROLLER_FAILED
        # evidence and controller exit 1.
        register_provider_adapter(
            "foreign-unknown",
            ForeignUnknownTerminalAdapter(),
            version="foreign-unknown-1.0",
            capabilities=_capabilities(notification=False),
        )
        try:
            raw = self._canonical(provider_id="foreign-unknown")
            card = {
                "schema": TASK_CARD_SCHEMA,
                "card_id": "card-1",
                "lane_id": "lane-1",
                "stage_cohort_id": "cohort-1",
                "worker_invocation_id": "worker-1",
                "objective": "Do the bounded task",
                "revision": "r1",
            }
            bundle = raw["prompt_bundle"]
            result = {
                "schema": TASK_RESULT_SCHEMA,
                "card_id": card["card_id"],
                "lane_id": card["lane_id"],
                "worker_invocation_id": card["worker_invocation_id"],
                "cohort_id": card["stage_cohort_id"],
                "revision": card["revision"],
                "task_card_sha256": record_sha256(card),
                "branch": "synthetic",
                "commit": "a" * 40,
                "outcome": "PASS",
                "summary": "synthetic provider completed",
                "checks": [{"name": "fake-provider", "outcome": "PASS"}],
                "prompt_bundle_sha256": bundle["bundle_sha256"],
                "prompt_content_sha256": bundle["final_sha256"],
            }
            # The child writes the shape-valid RESULT.json during its run so
            # the canonical start pre-launch fixed-artifact check passes and
            # the controller validates it after the exit-0 child.
            result_file = self.root / "result.json"
            result_file.write_text(json.dumps(result), encoding="utf-8")
            fake_unknown = self.root / "fake_unknown.py"
            fake_unknown.write_text(
                "import json, sys\n"
                "from pathlib import Path\n"
                "marker = Path(sys.argv[1])\n"
                "target = Path(sys.argv[2])\n"
                "source = Path(sys.argv[3])\n"
                "marker.write_text('launch\\n', encoding='utf-8')\n"
                "target.write_text(source.read_text(encoding='utf-8'), encoding='utf-8')\n"
                "sys.stdin.read()\n"
                "print(json.dumps({'type': 'thread.started', 'thread_id': 'fake-cli-session'}), flush=True)\n"
                "print(json.dumps({'type': 'turn.completed'}), flush=True)\n",
                encoding="utf-8",
            )
            raw["provider"] = {
                **raw["provider"],
                "command": [
                    sys.executable,
                    str(fake_unknown),
                    str(self.marker),
                    str(self.workspace / "RESULT.json"),
                    str(result_file),
                ],
            }
            path = self.workspace / "start.invocation.json"
            self._write(path, raw)
            self.assertEqual(1, controller.main([str(path)]))
            self.assertEqual("launch\n", self.marker.read_text(encoding="utf-8"))
            status = self._status()
            self.assertEqual("CONTROLLER_FAILED", status["state"])
            self.assertEqual("UNKNOWN", status["provider_terminal_outcome"])
            self.assertEqual(0, status["exit_code"])
            self.assertEqual("SHAPE_VALID", status["result_validation"]["state"])
            self.assertTrue(status["result_valid"])
            self.assertIn("closed provider-neutral vocabulary", status["error"])
            invalid = status["terminal_outcome_invalid"]
            self.assertEqual("UNKNOWN", invalid["returned"])
            self.assertEqual(["CANCELLED", "COMPLETED", "FAILED"], invalid["allowed"])
            evidence = status["provider_evidence"]
            self.assertIsNotNone(evidence)
            self.assertEqual("foreign-unknown", evidence["provider_id"])
            self.assertEqual("foreign-unknown-1.0", evidence["adapter_version"])
            self.assertEqual("fake-cli-session", evidence["session_id"])
        finally:
            unregister_provider_adapter("foreign-unknown")

    def test_foreign_auth_secret_reaches_child_but_never_persists(self) -> None:
        # PA-R1-001: the selected adapter owns complete redacted argv
        # provenance.  A foreign --auth synthetic credential reaches the fake
        # child command but is absent from every persisted status/provider-
        # evidence byte while the complete adapter-redacted argv shape remains.
        fake_argv = self.root / "fake_argv.py"
        fake_argv.write_text(FAKE_CLI_ARGV, encoding="utf-8")
        sentinel = "ROOT-SYNTHETIC-AUTH-71d3"
        raw = self._canonical()
        raw["provider"] = {
            **raw["provider"],
            "command": [sys.executable, str(fake_argv), str(self.marker), "--auth", sentinel],
        }
        path = self.workspace / "start.invocation.json"
        self._write(path, raw)
        self.assertEqual(0, controller.main([str(path)]))
        child_argv = json.loads(self.marker.read_text(encoding="utf-8"))
        self.assertIn(sentinel, child_argv)
        status = self._status()
        status_bytes = json.dumps(status).encode("utf-8")
        self.assertNotIn(sentinel.encode("utf-8"), status_bytes)
        evidence = status["provider_evidence"]
        self.assertNotIn(sentinel.encode("utf-8"), json.dumps(evidence).encode("utf-8"))
        provenance = evidence["command_provenance"]
        # Complete redacted argv shape: python + script + marker + --auth +
        # <redacted> + --run + --model + fake-model.
        self.assertEqual(8, len(provenance))
        self.assertEqual("<redacted>", provenance[3])
        self.assertEqual("<redacted>", provenance[4])
        self.assertEqual("--run", provenance[5])
        self.assertEqual("--model", provenance[6])
        self.assertEqual("fake-model", provenance[7])
        settings = status["launcher_settings"]
        self.assertNotIn(sentinel.encode("utf-8"), json.dumps(settings).encode("utf-8"))
        self.assertEqual(provenance, settings["argv"])
        # The generic marker list does not know --auth: adapter ownership is
        # what keeps the credential out of durable evidence.
        self.assertIn("--auth", redact_command(["--auth", sentinel]))
        self.assertIn(sentinel, redact_command(["--auth", sentinel]))

    def test_builtin_redact_argv_preserves_byte_compatible_output(self) -> None:
        # PA-R1-001: Codex and Claude Code preserve their existing
        # command/provenance results under the required adapter-owned
        # operation.
        codex_sample = ["codex", "exec", "--api-key", "sk-live", "--token", "abc", "--model", "m"]
        self.assertEqual(
            redact_command(codex_sample),
            CodexProviderAdapter().redact_argv(codex_sample),
        )
        claude_sample = ["claude", "--print", "--secret", "s3cr3t", "--model", "m"]
        self.assertEqual(
            redact_command(claude_sample),
            ClaudeCodeProviderAdapter().redact_argv(claude_sample),
        )

    def test_foreign_notification_true_reaches_production_safe_boundary_seam(self) -> None:
        # PA-R1-002: a foreign notification=true adapter selected through a
        # real canonical controller invocation reaches the production
        # safe-boundary delivery seam and emits only the exact content-free
        # wake; its queue/ack/Stop lifecycle uses the existing authorities.
        register_provider_adapter(
            "foreign-wake",
            ForeignWakeAdapter(),
            version="foreign-wake-1.0",
            capabilities=_capabilities(notification=True),
        )
        try:
            path = self.workspace / "start.invocation.json"
            self._write(path, self._canonical(provider_id="foreign-wake", notification=True))
            self.assertEqual(0, controller.main([str(path)]))
            status = self._status()
            self.assertEqual("foreign-wake", status["provider_id"])
            results = status["provider_operation_results"]
            notification_result = next(
                item for item in results if item["operation"] == "notification"
            )
            self.assertTrue(notification_result["supported"])
            # The production safe-boundary seam: the selected adapter delivers
            # through the per-binding DeliveryCoordinator.
            router = ManagerEventRouter(
                self.root / "manager",
                run_id="run-foreign",
                queue_id="queue-foreign",
                manager_session_id="session-foreign",
                manager_thread_id="thread-foreign",
                registration_id="registration-foreign",
            )
            coordinator = create_codex_adapter(router).coordinator
            coordinator.register()
            coordinator.admit_notification(notification_id="notice-1")
            notice = coordinator.notice_for_wake()
            self.assertIsNotNone(notice)
            adapter = provider_adapter("foreign-wake")
            receipt = adapter.deliver_notification(coordinator, notice, boundary="post_tool_use")
            self.assertIsNotNone(receipt)
            self.assertEqual("DELIVERED", receipt.outcome)
            mode = notification_mode("foreign-wake")
            self.assertEqual(NOTIFICATION_MODE_WAKE, mode["mode"])
            self.assertEqual(NOTIFICATION_WAKE_TEXT, mode["wake_text"])
            self.assertNotIn("notice-1", json.dumps(mode))
            self.assertNotIn("payload", json.dumps(mode))
            # Transport delivery alone never acknowledges queue work.
            self.assertEqual(1, len(router.pending_events()))
            self.assertEqual(1, len(coordinator.notification_items()))
            # Atomic addressed removal through the existing authority.
            ack = coordinator.acknowledge_event("notice-1")
            self.assertEqual("notice-1", ack.event_id)
            self.assertEqual(0, len(router.pending_events()))
            self.assertEqual([], coordinator.notification_items())
            # Stop matrix through the existing authorities.
            coordinator.admit_notification(notification_id="notice-2")
            open_decision = coordinator.notification_stop_request()
            self.assertFalse(open_decision.permitted)
            self.assertEqual(NOTIFICATION_STOP_OPEN_ITEMS_REMAIN, open_decision.reason)
            coordinator.acknowledge_event("notice-2")
            empty_decision = coordinator.notification_stop_request()
            self.assertTrue(empty_decision.permitted)
            self.assertEqual(NOTIFICATION_STOP_QUEUE_EMPTY, empty_decision.reason)
            coordinator.admit_notification(
                notification_id="notice-3",
                externally_blocked=True,
                required_actor="ROOT-IM",
                required_action="approve the external dependency",
            )
            blocked_decision = coordinator.notification_stop_request()
            self.assertFalse(blocked_decision.permitted)
            self.assertEqual(NOTIFICATION_STOP_EXTERNAL_RESPONSE_REQUIRED, blocked_decision.reason)
            coordinator.record_notification_final_response(
                [
                    {
                        "notification_id": "notice-3",
                        "required_actor": "ROOT-IM",
                        "required_action": "approve the external dependency",
                    }
                ]
            )
            declared_decision = coordinator.notification_stop_request()
            self.assertTrue(declared_decision.permitted)
            self.assertEqual(NOTIFICATION_STOP_EXTERNAL_DECLARED, declared_decision.reason)
            self.assertEqual(1, len(coordinator.notification_items()))
        finally:
            unregister_provider_adapter("foreign-wake")

    def test_notification_false_performs_no_immediate_wake(self) -> None:
        # PA-R1-002: notification=false and unavailable delivery remain
        # SAFE_BOUNDARY_ONLY and perform no immediate wake.
        register_provider_adapter(
            "foreign-safe",
            ForeignSafeBoundaryAdapter(),
            version="foreign-safe-1.0",
            capabilities=_capabilities(notification=False),
        )
        try:
            path = self.workspace / "start.invocation.json"
            self._write(path, self._canonical(provider_id="foreign-safe"))
            self.assertEqual(0, controller.main([str(path)]))
            status = self._status()
            self.assertEqual("foreign-safe", status["provider_id"])
            mode = notification_mode("foreign-safe")
            self.assertEqual(NOTIFICATION_MODE_SAFE_BOUNDARY_ONLY, mode["mode"])
            self.assertIsNone(mode["wake_text"])
            # The default seam performs no immediate delivery and no wake.
            router = ManagerEventRouter(
                self.root / "manager-safe",
                run_id="run-safe",
                queue_id="queue-safe",
                manager_session_id="session-safe",
                manager_thread_id="thread-safe",
                registration_id="registration-safe",
            )
            coordinator = create_codex_adapter(router).coordinator
            coordinator.register()
            coordinator.admit_notification(notification_id="notice-1")
            notice = coordinator.notice_for_wake()
            self.assertIsNotNone(notice)
            adapter = provider_adapter("foreign-safe")
            receipt = adapter.deliver_notification(coordinator, notice, boundary="post_tool_use")
            self.assertIsNone(receipt)
            self.assertEqual(1, len(router.pending_events()))
        finally:
            unregister_provider_adapter("foreign-safe")

    def test_same_process_bootstrap_registers_before_controller_parsing(self) -> None:
        # PA-R1-003: a fresh subprocess follows the documented same-process
        # registration/bootstrap shape, registers the foreign adapter before
        # parsing, selects it through the native controller path, and finishes
        # with identity-bound adapter-owned redacted evidence.  No generic-core
        # edit or plugin discovery is involved.
        # The test process registers my-cli only to build the canonical record
        # (RuntimeProfile validates against the in-process registry); the
        # fresh subprocess starts with built-ins only and registers my-cli
        # itself before controller parsing.
        register_provider_adapter(
            "my-cli",
            MyCliBuildAdapter(),
            version="my-cli-test-build",
            capabilities=_capabilities(notification=False),
        )
        try:
            bootstrap = self.root / "bootstrap_my_cli.py"
            bootstrap.write_text(BOOTSTRAP_SCRIPT, encoding="utf-8")
            fake_argv = self.root / "fake_argv.py"
            fake_argv.write_text(FAKE_CLI_ARGV, encoding="utf-8")
            sentinel = "BOOTSTRAP-AUTH-5c1e"
            raw = self._canonical(provider_id="my-cli")
            raw["provider"] = {
                **raw["provider"],
                "command": [sys.executable, str(fake_argv), str(self.marker), "--auth", sentinel],
            }
            path = self.workspace / "start.invocation.json"
            self._write(path, raw)
            env = os.environ.copy()
            repo_root = str(Path(__file__).resolve().parents[2])
            env["PYTHONPATH"] = repo_root + os.pathsep + env.get("PYTHONPATH", "")
            completed = subprocess.run(
                [sys.executable, str(bootstrap), str(path)],
                cwd=self.root,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, completed.returncode, completed.stderr)
            status = self._status()
            self.assertEqual("my-cli", status["provider_id"])
            evidence = status["provider_evidence"]
            self.assertIsNotNone(evidence)
            self.assertEqual("my-cli", evidence["provider_id"])
            self.assertEqual("my-cli-v1", evidence["adapter_version"])
            self.assertEqual("worker-1", evidence["attempt_identity"])
            self.assertNotIn(sentinel, json.dumps(status))
            provenance = evidence["command_provenance"]
            # Complete adapter-redacted argv shape: python + script + marker +
            # <redacted> (--auth) + <redacted> (value) + --run + --model +
            # fake-model.
            self.assertEqual(8, len(provenance))
            self.assertEqual("<redacted>", provenance[3])
            self.assertEqual("<redacted>", provenance[4])
            self.assertEqual("--run", provenance[5])
            self.assertEqual("--model", provenance[6])
            self.assertEqual("fake-model", provenance[7])
            self.assertEqual(provenance, status["launcher_settings"]["argv"])
            child_argv = json.loads(self.marker.read_text(encoding="utf-8"))
            self.assertIn(sentinel, child_argv)
        finally:
            unregister_provider_adapter("my-cli")

    def test_notification_flag_is_rejected_when_not_boolean(self) -> None:
        raw = self._canonical()
        raw["provider"] = {**raw["provider"], "notification": "yes"}  # type: ignore[arg-type]
        path = self.workspace / "bad.invocation.json"
        self._write(path, raw)
        with self.assertRaises(controller.InvocationError):
            controller.load_invocation(path)


class ManagerQueuePublicSeamTests(unittest.TestCase):
    """ManagerEventRouter authority and DeliveryCoordinator facade seams."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _router(self, root: Path, *, session: str = "session-seam") -> ManagerEventRouter:
        return ManagerEventRouter(
            root,
            run_id="run-seam",
            queue_id="queue-seam",
            manager_session_id=session,
            manager_thread_id="thread-seam",
            registration_id="registration-seam",
        )

    def _coordinator(self, router: ManagerEventRouter) -> DeliveryCoordinator:
        adapter = create_codex_adapter(router)
        adapter.coordinator.register()
        return adapter.coordinator

    def test_typed_transition_admission_and_exact_wake(self) -> None:
        router = self._router(self.root / "manager")
        coordinator = self._coordinator(router)
        admitted = coordinator.admit_notification(notification_id="notice-1")
        self.assertIsNotNone(admitted)
        self.assertEqual(1, len(router.pending_events()))
        items = coordinator.notification_items()
        self.assertEqual(1, len(items))
        self.assertEqual("notice-1", items[0]["notification_id"])
        self.assertEqual(NOTIFICATION_OPEN, items[0]["state"])
        mode = notification_mode("codex")
        self.assertEqual(NOTIFICATION_MODE_WAKE, mode["mode"])
        self.assertEqual(NOTIFICATION_WAKE_TEXT, mode["wake_text"])
        self.assertNotIn("payload", mode)
        self.assertNotIn("notification_id", mode)
        self.assertEqual(NOTIFICATION_MODE_SAFE_BOUNDARY_ONLY, notification_mode("claude-code")["mode"])

    def test_transport_receipt_alone_never_acknowledges_and_ack_removes_atomically(self) -> None:
        router = self._router(self.root / "manager")
        coordinator = self._coordinator(router)
        coordinator.admit_notification(notification_id="notice-1")
        notice = coordinator.notice_for_wake()
        self.assertIsNotNone(notice)
        receipt = coordinator.deliver_at_boundary(notice, boundary="post_tool_use")
        self.assertIsNotNone(receipt)
        self.assertEqual("DELIVERED", receipt.outcome)
        self.assertEqual(1, len(router.pending_events()))
        self.assertEqual(1, len(coordinator.notification_items()))
        ack = coordinator.acknowledge_event("notice-1")
        self.assertEqual("notice-1", ack.event_id)
        self.assertEqual(0, len(router.pending_events()))
        self.assertEqual([], coordinator.notification_items())
        self.assertNotIn("CLOSED", json.dumps(coordinator.load_state()))

    def test_restart_replay_preserves_active_items(self) -> None:
        manager = self.root / "manager"
        router = self._router(manager)
        coordinator = self._coordinator(router)
        coordinator.admit_notification(notification_id="notice-1")
        restored_router = self._router(manager)
        restored = create_codex_adapter(restored_router).coordinator
        restored.register()
        items = restored.notification_items()
        self.assertEqual(1, len(items))
        self.assertEqual("notice-1", items[0]["notification_id"])
        self.assertEqual(NOTIFICATION_OPEN, items[0]["state"])

    def test_externally_blocked_items_remain_active_and_worker_ack_is_rejected(self) -> None:
        router = self._router(self.root / "manager")
        coordinator = self._coordinator(router)
        coordinator.admit_notification(
            notification_id="notice-1",
            externally_blocked=True,
            required_actor="ROOT-IM",
            required_action="approve the external dependency",
        )
        items = coordinator.notification_items()
        self.assertEqual(1, len(items))
        self.assertEqual(NOTIFICATION_EXTERNALLY_BLOCKED, items[0]["state"])
        self.assertEqual("ROOT-IM", items[0]["required_actor"])
        self.assertEqual("approve the external dependency", items[0]["required_action"])
        with self.assertRaises(HostAdapterError):
            coordinator.acknowledge_event("notice-1")
        self.assertEqual(1, len(router.pending_events()))


class InstalledHookPublicSeamTests(unittest.TestCase):
    """Packaged installed Codex hook routes: wake and stop matrix."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.project = self.root / "project"
        self.project.mkdir()
        (self.project / ".codex").mkdir()
        self.install = install_codex_adapter(self.project)
        self.router = ManagerEventRouter(
            self.root / "manager",
            run_id="run-hook",
            queue_id="queue-hook",
            manager_session_id="session-hook",
            manager_thread_id="thread-hook",
            registration_id="registration-hook",
        )
        activate_codex_binding(self.project, self.router)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _run_hook(self, name: str, payload: dict[str, object] | None = None) -> dict[str, object]:
        hook = self.project / ".codex" / "hooks" / name
        env = os.environ.copy()
        repo_root = str(Path(__file__).resolve().parents[2])
        env["PYTHONPATH"] = repo_root + os.pathsep + env.get("PYTHONPATH", "")
        completed = subprocess.run(
            [sys.executable, str(hook)],
            cwd=self.project,
            input=(json.dumps(payload) if payload is not None else ""),
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        return json.loads(completed.stdout)

    def test_installed_post_tool_hook_emits_exact_content_free_wake(self) -> None:
        coordinator = create_codex_adapter(
            self.router, state_root=self.root / "manager" / "codex-coordinator"
        ).coordinator
        coordinator.register()
        coordinator.admit_notification(notification_id="notice-1")
        evidence = self._run_hook("orchestrator_harness_post_tool_use.py", {"provider_payload": "ignored"})
        self.assertEqual("orchestrator-codex-installed-hook/v1", evidence["schema"])
        wake = evidence["wake"]
        self.assertEqual(NOTIFICATION_MODE_WAKE, wake["mode"])
        self.assertEqual(NOTIFICATION_WAKE_TEXT, wake["wake_text"])
        self.assertTrue(wake["content_free"])
        self.assertFalse(wake["preemptive"])
        self.assertNotIn("notice-1", json.dumps(wake))
        self.assertNotIn("ignored", json.dumps(wake))
        self.assertFalse(evidence["acknowledged_by_hook"])
        self.assertEqual(1, evidence["pending_count"])

    def test_installed_stop_hook_open_rejects_without_payload_disclosure(self) -> None:
        coordinator = create_codex_adapter(
            self.router, state_root=self.root / "manager" / "codex-coordinator"
        ).coordinator
        coordinator.register()
        coordinator.admit_notification(notification_id="notice-1")
        evidence = self._run_hook("orchestrator_harness_stop.py", {"secret_payload": "do-not-leak"})
        decision = evidence["stop_decision"]
        self.assertFalse(decision["permitted"])
        self.assertEqual(NOTIFICATION_STOP_OPEN_ITEMS_REMAIN, decision["reason"])
        self.assertEqual(1, decision["open_count"])
        self.assertNotIn("notice-1", json.dumps(decision))
        self.assertNotIn("do-not-leak", json.dumps(decision))
        self.assertTrue(evidence["continuation_requested"])
        self.assertIs(True, evidence["continuation_result"])
        self.assertEqual([{"method": "Stop.continue", "continue": True}], evidence["transport_calls"])
        self.assertEqual(1, evidence["pending_count"])

    def test_installed_stop_hook_empty_queue_permits(self) -> None:
        evidence = self._run_hook("orchestrator_harness_stop.py")
        decision = evidence["stop_decision"]
        self.assertTrue(decision["permitted"])
        self.assertEqual(NOTIFICATION_STOP_QUEUE_EMPTY, decision["reason"])
        self.assertFalse(evidence["continuation_requested"])
        self.assertIsNone(evidence["continuation_result"])
        self.assertEqual([], evidence["transport_calls"])
        self.assertEqual(0, evidence["pending_count"])

    def test_installed_stop_hook_externally_blocked_requires_exact_final_response(self) -> None:
        coordinator = create_codex_adapter(
            self.router, state_root=self.root / "manager" / "codex-coordinator"
        ).coordinator
        coordinator.register()
        coordinator.admit_notification(
            notification_id="notice-1",
            externally_blocked=True,
            required_actor="ROOT-IM",
            required_action="approve the external dependency",
        )
        rejected = self._run_hook("orchestrator_harness_stop.py")
        decision = rejected["stop_decision"]
        self.assertFalse(decision["permitted"])
        self.assertEqual(NOTIFICATION_STOP_EXTERNAL_RESPONSE_REQUIRED, decision["reason"])
        self.assertEqual(1, decision["externally_blocked_count"])
        self.assertTrue(rejected["continuation_requested"])
        self.assertIs(True, rejected["continuation_result"])
        self.assertEqual([{"method": "Stop.continue", "continue": True}], rejected["transport_calls"])
        partial = self._run_hook(
            "orchestrator_harness_stop.py",
            {"orchestrator_final_response": [{"notification_id": "notice-1", "required_actor": "WRONG", "required_action": "approve the external dependency"}]},
        )
        self.assertFalse(partial["stop_decision"]["permitted"])
        self.assertTrue(partial["continuation_requested"])
        self.assertIs(True, partial["continuation_result"])
        self.assertEqual([{"method": "Stop.continue", "continue": True}], partial["transport_calls"])
        exact = self._run_hook(
            "orchestrator_harness_stop.py",
            {"orchestrator_final_response": [{"notification_id": "notice-1", "required_actor": "ROOT-IM", "required_action": "approve the external dependency"}]},
        )
        decision = exact["stop_decision"]
        self.assertTrue(decision["permitted"])
        self.assertEqual(NOTIFICATION_STOP_EXTERNAL_DECLARED, decision["reason"])
        self.assertEqual(1, decision["externally_blocked_count"])
        self.assertEqual("notice-1", decision["declarations"][0]["notification_id"])
        self.assertFalse(exact["continuation_requested"])
        self.assertIsNone(exact["continuation_result"])
        self.assertEqual([], exact["transport_calls"])
        self.assertEqual(1, exact["pending_count"])
        items = coordinator.notification_items()
        self.assertEqual(1, len(items))
        self.assertEqual(NOTIFICATION_EXTERNALLY_BLOCKED, items[0]["state"])


if __name__ == "__main__":
    unittest.main()
