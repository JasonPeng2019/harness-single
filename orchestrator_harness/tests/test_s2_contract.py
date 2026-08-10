from __future__ import annotations

import json
import hashlib
import sys
import tempfile
import unittest
from pathlib import Path

from orchestrator_harness.invocation import (
    CANONICAL_INVOCATION_SCHEMA,
    InvocationValidationError,
    adapt_coding_v1,
    adapt_legacy_firmware,
    parse_canonical_invocation,
)
from orchestrator_harness.lane_controller import load_invocation
import orchestrator_harness.lane_controller as controller
from orchestrator_harness.discovery import discover_run
from orchestrator_harness.profile import RuntimeProfile, build_child_environment
from orchestrator_harness.prompt_bundle import (
    bundle_from_record,
    compose_prompt_bundle,
    prompt_bundle_record_from_paths,
)
from orchestrator_harness.provider import ClaudeCodeProviderAdapter, ProviderLaunchSpec
from orchestrator_harness.resume import make_resume_admission
from orchestrator_harness.task import (
    COMPLETION_REVIEW_SCHEMA,
    ORCHESTRATOR_ACCEPTANCE_SCHEMA,
    TASK_CARD_SCHEMA,
    TASK_RESULT_SCHEMA,
    advance_task,
    record_sha256,
    read_task_advancement,
    TaskValidationError,
    validate_completion_review,
    validate_orchestrator_acceptance,
    validate_task_card,
    validate_task_result,
)
from orchestrator_harness.tests.support import SuiteFixture


class S2ContractTests(unittest.TestCase):
    def _canonical(self, root: Path) -> tuple[dict[str, object], object]:
        run = root / "run"
        workspace = run / ".agent-workspace"
        workspace.mkdir(parents=True)
        runtime = root / "runtime"
        runtime.mkdir()
        prompt_a = run / "prompt-a.md"
        prompt_b = run / "prompt-b.md"
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
            "profile-1", "implementer", "claude-code", "claude-test", ("Read",), ("repo",), ("resource-1",),
            ("CLAUDE_CODE_TEST",), ("WORKFLOW_TEST_FLAG",),
        )
        bundle = prompt_bundle_record_from_paths(
            workflow_id="workflow-1",
            task_card_id="card-1",
            profile_id="profile-1",
            paths=(("instructions", prompt_a), ("task", prompt_b)),
            run_root=run,
        )
        value: dict[str, object] = {
            "schema": CANONICAL_INVOCATION_SCHEMA,
            "action": "start",
            "run_root": str(run),
            "runtime_root": str(runtime),
            "lane_id": "lane-1",
            "worker_invocation_id": "worker-1",
            "cohort_id": "cohort-1",
            "workflow": {"id": "workflow-1", "version": "1"},
            "task_card": {"id": "card-1", "revision": "r1", "sha256": record_sha256(card)},
            "role": "implementer",
            "provider": {
                "id": "claude-code",
                "model": "claude-test",
                "command": ["synthetic-claude"],
                "permission_mode": "default",
                "allowed_tools": ["Read"],
            },
            "profile": profile.to_record(),
            "prompt_bundle": bundle,
            "output_paths": {
                "status": str(workspace / "worker_controller.status.json"),
                "jsonl": str(workspace / "worker_provider.jsonl"),
                "stderr": str(workspace / "worker.stderr.log"),
                "last_message": str(workspace / "worker.last-message"),
            },
            "event_log_path": str(runtime / "events.jsonl"),
            "resources": ["resource-1"],
        }
        return value, card

    def test_canonical_prompt_resume_and_task_advancement_are_content_bound(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw, card_raw = self._canonical(root)
            canonical = parse_canonical_invocation(raw)
            invocation_path = root / "run" / ".agent-workspace" / "canonical.invocation.json"
            invocation_path.write_text(json.dumps(raw), encoding="utf-8")
            loaded = load_invocation(invocation_path)
            self.assertEqual(CANONICAL_INVOCATION_SCHEMA, loaded.invocation_schema)
            self.assertEqual("claude-code", loaded.provider_id)
            self.assertEqual(b"workflow\ntask\n", loaded.prompt_bytes)
            bundle = bundle_from_record(raw["prompt_bundle"], run_root=root / "run")  # type: ignore[arg-type]
            self.assertEqual(b"workflow\ntask\n", bundle.final_bytes)
            self.assertEqual(canonical.prompt_bundle_sha256, bundle.bundle_sha256)

            mixed = dict(raw)
            mixed["prompt_path"] = str(root / "run" / "prompt-a.md")
            with self.assertRaises(InvocationValidationError):
                parse_canonical_invocation(mixed)
            tampered = dict(raw["prompt_bundle"])  # type: ignore[arg-type]
            tampered["components"] = list(tampered["components"])
            tampered["components"][0] = dict(tampered["components"][0])
            tampered["components"][0]["sha256"] = "0" * 64
            with self.assertRaises(ValueError):
                bundle_from_record(tampered, run_root=root / "run")

            identity = canonical.identity(session_id="session-1", starting_commit="a" * 40)
            identity["live_identity"] = {"provider_id": "claude-code", "session_id": "session-1"}
            persisted = dict(identity)
            live = {
                "live_identity": {"provider_id": "claude-code", "session_id": "session-1"},
                "repository": identity["repository"],
            }
            self.assertTrue(make_resume_admission(identity, persisted, live).admitted)
            wrong = dict(identity)
            wrong["task_card_id"] = "other-card"
            self.assertFalse(make_resume_admission(wrong, persisted, live).admitted)
            terminal = dict(persisted)
            terminal["terminal_acceptance_state"] = "ACCEPTED"
            self.assertFalse(make_resume_admission(identity, terminal, live).admitted)

            card = validate_task_card(card_raw)  # type: ignore[arg-type]
            result_raw = {
                "schema": TASK_RESULT_SCHEMA,
                "card_id": card.card_id,
                "lane_id": card.lane_id,
                "worker_invocation_id": card.worker_invocation_id,
                "cohort_id": card.cohort_id,
                "revision": card.revision,
                "task_card_sha256": card.content_sha256,
                "branch": "lane-1",
                "commit": "a" * 40,
                "outcome": "PASS",
                "summary": "done",
                "checks": [{"name": "fake", "outcome": "PASS"}],
            }
            result = validate_task_result(result_raw, card=card)
            pending = advance_task(card, result)
            self.assertEqual("ACCEPTANCE_PENDING", pending.state)
            review_raw = {
                "schema": COMPLETION_REVIEW_SCHEMA,
                "card_id": card.card_id,
                "lane_id": card.lane_id,
                "worker_invocation_id": card.worker_invocation_id,
                "cohort_id": card.cohort_id,
                "revision": card.revision,
                "result_sha256": result.content_sha256,
                "owner": card.completion_review_owner,
                "verdict": "PASS",
                "evidence": ["test://s2"],
            }
            review = validate_completion_review(review_raw, card=card, result=result)
            acceptance_raw = {
                "schema": ORCHESTRATOR_ACCEPTANCE_SCHEMA,
                "card_id": card.card_id,
                "lane_id": card.lane_id,
                "worker_invocation_id": card.worker_invocation_id,
                "cohort_id": card.cohort_id,
                "revision": card.revision,
                "card_sha256": card.content_sha256,
                "result_sha256": result.content_sha256,
                "completion_review_sha256": record_sha256(review_raw),
                "accepted_commit": result.commit,
                "accepted_by": "ROOT-IM",
                "verdict": "ACCEPTED",
            }
            acceptance = validate_orchestrator_acceptance(
                acceptance_raw,
                card=card,
                result=result,
                review=review,
                review_record=review_raw,
            )
            advanced = advance_task(card, result, review=review, acceptance=acceptance)
            self.assertEqual("ACCEPTED", advanced.state)
            self.assertTrue(advanced.terminal)

    def test_fixed_task_advancement_chain_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            card_raw = {
                "schema": TASK_CARD_SCHEMA,
                "card_id": "card-chain",
                "lane_id": "lane-chain",
                "stage_cohort_id": "cohort-chain",
                "worker_invocation_id": "worker-chain",
                "objective": "chain",
                "revision": "r1",
            }
            card = validate_task_card(card_raw)
            result_raw = {
                "schema": TASK_RESULT_SCHEMA,
                "card_id": card.card_id,
                "lane_id": card.lane_id,
                "worker_invocation_id": card.worker_invocation_id,
                "cohort_id": card.cohort_id,
                "revision": card.revision,
                "task_card_sha256": card.content_sha256,
                "branch": "lane-chain",
                "commit": "b" * 40,
                "outcome": "PASS",
                "summary": "chain",
                "checks": [{"name": "chain", "outcome": "PASS"}],
            }
            result_bytes = (json.dumps(result_raw, indent=2, sort_keys=True) + "\n").encode("utf-8")
            result = validate_task_result(result_raw, card=card, raw_bytes=result_bytes)
            pending = read_task_advancement(workspace, card=card, result=result)
            self.assertEqual("PENDING", pending.state)

            review = {
                "schema": COMPLETION_REVIEW_SCHEMA,
                "card_id": card.card_id,
                "lane_id": card.lane_id,
                "worker_invocation_id": card.worker_invocation_id,
                "cohort_id": card.cohort_id,
                "revision": card.revision,
                "result_sha256": result.content_sha256,
                "owner": card.completion_review_owner,
                "verdict": "PASS",
                "evidence": ["test://chain"],
            }
            review_bytes = (json.dumps(review, indent=2, sort_keys=True) + "\n").encode("utf-8")
            review_sha = hashlib.sha256(review_bytes).hexdigest()
            acceptance = {
                "schema": ORCHESTRATOR_ACCEPTANCE_SCHEMA,
                "card_id": card.card_id,
                "lane_id": card.lane_id,
                "worker_invocation_id": card.worker_invocation_id,
                "cohort_id": card.cohort_id,
                "revision": card.revision,
                "card_sha256": card.content_sha256,
                "result_sha256": result.content_sha256,
                "completion_review_sha256": review_sha,
                "accepted_commit": result.commit,
                "accepted_by": "ROOT-IM",
                "verdict": "ACCEPTED",
            }
            (workspace / "COMPLETION_REVIEW.json").write_bytes(review_bytes)
            self.assertRaises(TaskValidationError, read_task_advancement, workspace, card=card, result=result)
            (workspace / "ORCHESTRATOR_ACCEPTANCE.json").write_text("not-json", encoding="utf-8")
            with self.assertRaises(TaskValidationError):
                read_task_advancement(workspace, card=card, result=result)

            for field, bad_value in (
                ("card_sha256", "c" * 64),
                ("result_sha256", "d" * 64),
                ("completion_review_sha256", "e" * 64),
                ("accepted_commit", "f" * 40),
            ):
                case = workspace / field
                case.mkdir()
                (case / "COMPLETION_REVIEW.json").write_bytes(review_bytes)
                invalid_acceptance = dict(acceptance)
                invalid_acceptance[field] = bad_value
                (case / "ORCHESTRATOR_ACCEPTANCE.json").write_text(
                    json.dumps(invalid_acceptance, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                with self.assertRaises(TaskValidationError):
                    read_task_advancement(case, card=card, result=result)

    def test_claude_adapter_and_profile_use_only_synthetic_provider_facts(self) -> None:
        adapter = ClaudeCodeProviderAdapter()
        spec = ProviderLaunchSpec(
            action="resume",
            command=("synthetic-claude",),
            model="claude-test",
            reasoning_effort="medium",
            service_tier="priority",
            session_id="session-1",
            run_root=Path("C:/run"),
            last_message_path=Path("C:/run/.agent-workspace/last"),
            permission_mode="default",
            allowed_tools=("Read",),
            disallowed_tools=("Bash",),
        )
        argv = adapter.build_argv(spec)
        self.assertEqual(
            [
                "synthetic-claude",
                "--print",
                "--output-format",
                "stream-json",
                "--resume",
                "session-1",
                "--model",
                "claude-test",
                "--permission-mode",
                "default",
                "--allowedTools",
                "Read",
                "--disallowedTools",
                "Bash",
            ],
            argv,
        )
        self.assertEqual("session-1", adapter.parse_transcript_line(b'{"type":"system","subtype":"init","session_id":"session-1"}').session_id)  # type: ignore[union-attr]
        self.assertEqual("COMPLETED", adapter.parse_transcript_line(b'{"type":"result","subtype":"success","session_id":"session-1"}').kind)  # type: ignore[union-attr]
        self.assertEqual("FAILED", adapter.parse_transcript_line(b'{"type":"result","subtype":"error_during_execution","session_id":"session-1"}').kind)  # type: ignore[union-attr]

        profile = RuntimeProfile(
            "profile", "worker", "claude-code", "claude-test", (), (), (),
            ("CLAUDE_CODE_TEST",), ("WORKFLOW_TEST_FLAG",),
        )
        environment, cleared = build_child_environment(
            profile,
            {
                "PATH": "path",
                "CLAUDE_CODE_TEST": "provider",
                "WORKFLOW_TEST_FLAG": "workflow",
                "MCP_ENDPOINT": "ambient",
                "ANTHROPIC_API_KEY": "credential",
                "GITHUB_TOKEN": "credential",
                "PYOCD_TARGET": "hardware",
            },
        )
        self.assertEqual({"PATH": "path", "CLAUDE_CODE_TEST": "provider", "WORKFLOW_TEST_FLAG": "workflow"}, environment)
        self.assertEqual(["ANTHROPIC_API_KEY", "GITHUB_TOKEN", "MCP_ENDPOINT", "PYOCD_TARGET"], cleared)

    def test_legacy_adapters_are_separate_and_reject_ambiguous_aliases(self) -> None:
        coding = {
            "schema": "orchestrator-coding-invocation/v1",
            "action": "start",
            "run_root": "C:/run",
            "runtime_root": "C:/runtime",
            "prompt_path": "C:/run/prompt.md",
            "prompt_sha256": "a" * 64,
            "worker_invocation_id": "worker-1",
            "lane_id": "lane-1",
            "task": "coding-task",
            "phase": "implementation",
            "event_log_path": "C:/runtime/events.jsonl",
            "output_paths": {
                "status": "C:/run/.agent-workspace/status.json",
                "jsonl": "C:/run/.agent-workspace/provider.jsonl",
                "stderr": "C:/run/.agent-workspace/stderr.log",
                "last_message": "C:/run/.agent-workspace/last-message",
            },
            "codex": {
                "model": "codex-test",
                "reasoning_effort": "medium",
                "service_tier": "priority",
                "sandbox": "workspace-write",
                "approval_policy": "never",
            },
        }
        self.assertEqual("codex", adapt_coding_v1(coding).provider_id)
        ambiguous = dict(coding)
        ambiguous["codex_settings"] = dict(coding["codex"])  # type: ignore[index]
        with self.assertRaises(InvocationValidationError):
            adapt_coding_v1(ambiguous)

        firmware = {
            "action": "start",
            "run_root": "C:/run",
            "prompt_path": "C:/run/prompt.md",
            "prompt_sha256": "b" * 64,
            "declared_lane_id": "legacy-lane",
            "label": "legacy-worker",
            "doer": "firmware-worker",
            "task": "legacy-task",
            "phase": "implementation",
            "lane_event_log": "C:/run/.agent-workspace/events.jsonl",
            "output_paths": coding["output_paths"],
            "model_settings": {
                "model": "codex-test",
                "reasoning_effort": "medium",
                "service_tier": "priority",
            },
        }
        self.assertEqual("legacy-firmware", adapt_legacy_firmware(firmware).legacy_route)
        mixed = dict(firmware)
        mixed["worker_invocation_id"] = "not-legacy"
        with self.assertRaises(InvocationValidationError):
            adapt_legacy_firmware(mixed)

    def test_coding_v1_closed_contract_and_alias_matrix(self) -> None:
        coding = {
            "schema": "orchestrator-coding-invocation/v1",
            "action": "start",
            "run_root": "C:/run",
            "runtime_root": "C:/runtime",
            "prompt_path": "C:/run/prompt.md",
            "prompt_sha256": "a" * 64,
            "worker_invocation_id": "worker-1",
            "lane_id": "lane-1",
            "task": "coding-task",
            "phase": "implementation",
            "event_log_path": "C:/runtime/events.jsonl",
            "output_paths": {
                "status": "C:/run/.agent-workspace/status.json",
                "jsonl": "C:/run/.agent-workspace/provider.jsonl",
                "stderr": "C:/run/.agent-workspace/stderr.log",
                "last_message": "C:/run/.agent-workspace/last-message",
            },
            "codex": {
                "model": "codex-test",
                "reasoning_effort": "medium",
                "service_tier": "priority",
                "sandbox": "workspace-write",
                "approval_policy": "never",
            },
        }
        aliases = (
            ("codex", "codex_settings"),
            ("codex", "model_settings"),
            ("event_log_path", "event_log"),
            ("event_log_path", "lane_event_log"),
            ("lane_id", "declared_lane_id"),
            ("resources", "exclusive_resources"),
            ("repository", "git"),
            ("resume_identity", "resume"),
        )
        for original, alias in aliases:
            variant = dict(coding)
            if original in {"codex", "event_log_path"}:
                variant[alias] = variant.pop(original)
            elif original == "lane_id":
                variant[alias] = variant.pop(original)
            elif original == "resources":
                variant[alias] = []
            elif original == "repository":
                variant[alias] = {"worktree_root": "C:/run"}
            else:
                variant[alias] = {"thread_id": "thread-1"}
            adapted = adapt_coding_v1(variant)
            self.assertEqual("codex", adapted.provider_id)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for field in ("provider", "profile", "prompt_bundle", "workflow", "task_card", "cohort_id", "unknown_field"):
                invocation = root / f"{field}.json"
                invalid = dict(coding)
                invalid[field] = {}
                invocation.write_text(json.dumps(invalid), encoding="utf-8")
                with self.assertRaises(controller.InvocationError):
                    controller.load_invocation(invocation)
                self.assertFalse((root / ".agent-workspace").exists())

        ambiguous = dict(coding)
        ambiguous["codex_settings"] = dict(coding["codex"])
        with self.assertRaises(InvocationValidationError):
            adapt_coding_v1(ambiguous)

    def test_controller_fake_claude_start_resume_failure_and_wrong_session(self) -> None:
        fake_source = """
import json, sys
failure = '--failure' in sys.argv
sys.stdin.read()
print(json.dumps({'type': 'system', 'subtype': 'init', 'session_id': 'session-1'}), flush=True)
print(json.dumps({'type': 'result', 'subtype': 'error_during_execution' if failure else 'success', 'session_id': 'session-1'}), flush=True)
"""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake = root / "fake_claude.py"
            fake.write_text(fake_source, encoding="utf-8")
            raw, card = self._canonical(root)
            raw["provider"] = {
                **raw["provider"],  # type: ignore[arg-type]
                "command": [sys.executable, str(fake)],
            }
            bundle = raw["prompt_bundle"]  # type: ignore[assignment]
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
            invocation_path = root / "run" / ".agent-workspace" / "start.invocation.json"
            invocation_path.write_text(json.dumps(raw), encoding="utf-8")
            (root / "run" / ".agent-workspace" / "RESULT.json").write_text(json.dumps(result), encoding="utf-8")
            self.assertEqual(0, controller.main([str(invocation_path)]))
            status_path = root / "run" / ".agent-workspace" / "worker_controller.status.json"
            status = json.loads(status_path.read_text(encoding="utf-8"))
            self.assertEqual("PROVIDER_EXITED", status["state"])
            self.assertEqual("claude-code", status["provider_id"])
            self.assertEqual("session-1", status["provider_session_id"])
            self.assertEqual("SHAPE_VALID", status["result_validation"]["state"])
            self.assertEqual("PENDING", status["terminal_acceptance_state"])
            self.assertTrue((root / "run" / ".agent-workspace" / "worker_provider.jsonl").is_file())

            resume = dict(raw)
            resume["action"] = "resume"
            resume["resume"] = {"session_id": "session-1"}
            resume_path = root / "run" / ".agent-workspace" / "resume.invocation.json"
            resume_path.write_text(json.dumps(resume), encoding="utf-8")
            self.assertEqual(0, controller.main([str(resume_path)]))
            resumed = json.loads(status_path.read_text(encoding="utf-8"))
            self.assertTrue(resumed["resume_admission"]["admitted"])

            wrong = dict(resume)
            wrong["resume"] = {"session_id": "wrong-session"}
            wrong_path = root / "run" / ".agent-workspace" / "wrong.invocation.json"
            wrong_path.write_text(json.dumps(wrong), encoding="utf-8")
            self.assertEqual(2, controller.main([str(wrong_path)]))

            failure_root = root / "failure"
            failure_raw, _ = self._canonical(failure_root)
            failure_raw["provider"] = {
                **failure_raw["provider"],  # type: ignore[arg-type]
                "command": [sys.executable, str(fake), "--failure"],
            }
            failure_path = failure_root / "run" / ".agent-workspace" / "failure.invocation.json"
            failure_path.write_text(json.dumps(failure_raw), encoding="utf-8")
            self.assertEqual(1, controller.main([str(failure_path)]))
            failure_status = json.loads(
                (failure_root / "run" / ".agent-workspace" / "worker_controller.status.json").read_text(encoding="utf-8")
            )
            self.assertEqual("FAILED", failure_status["provider_terminal_outcome"])

    def test_controller_acceptance_chain_blocks_resume_before_fake_provider(self) -> None:
        fake_source = """
import json, sys
from pathlib import Path
marker = Path(sys.argv[1])
marker.write_text(marker.read_text(encoding='utf-8') + 'launch\\n' if marker.exists() else 'launch\\n', encoding='utf-8')
sys.stdin.read()
print(json.dumps({'type': 'system', 'subtype': 'init', 'session_id': 'session-accepted'}), flush=True)
print(json.dumps({'type': 'result', 'subtype': 'success', 'session_id': 'session-accepted'}), flush=True)
"""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            marker = root / "provider-launches.txt"
            fake = root / "fake_claude.py"
            fake.write_text(fake_source, encoding="utf-8")
            raw, card = self._canonical(root)
            raw["provider"] = {
                **raw["provider"],  # type: ignore[arg-type]
                "command": [sys.executable, str(fake), str(marker)],
            }
            bundle = raw["prompt_bundle"]  # type: ignore[assignment]
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
            invocation_path = root / "run" / ".agent-workspace" / "start.invocation.json"
            invocation_path.write_text(json.dumps(raw), encoding="utf-8")
            (root / "run" / ".agent-workspace" / "RESULT.json").write_text(json.dumps(result), encoding="utf-8")
            self.assertEqual(0, controller.main([str(invocation_path)]))
            workspace = root / "run" / ".agent-workspace"
            self.assertEqual("PENDING", json.loads((workspace / "worker_controller.status.json").read_text())["terminal_acceptance_state"])
            fixture = SuiteFixture.create()
            try:
                pending = discover_run(root / "run", workspace, fixture.config)
                self.assertEqual("PENDING", pending.result_acceptance_state)
            finally:
                fixture.close()

            result_bytes = (workspace / "RESULT.json").read_bytes()
            result_sha = hashlib.sha256(result_bytes).hexdigest()
            review = {
                "schema": COMPLETION_REVIEW_SCHEMA,
                "card_id": card["card_id"],
                "lane_id": card["lane_id"],
                "worker_invocation_id": card["worker_invocation_id"],
                "cohort_id": card["stage_cohort_id"],
                "revision": card["revision"],
                "result_sha256": result_sha,
                "owner": "ROOT-IM",
                "verdict": "PASS",
                "evidence": ["test://s2-controller"],
            }
            review_bytes = (json.dumps(review, indent=2, sort_keys=True) + "\n").encode("utf-8")
            review_sha = hashlib.sha256(review_bytes).hexdigest()
            acceptance = {
                "schema": ORCHESTRATOR_ACCEPTANCE_SCHEMA,
                "card_id": card["card_id"],
                "lane_id": card["lane_id"],
                "worker_invocation_id": card["worker_invocation_id"],
                "cohort_id": card["stage_cohort_id"],
                "revision": card["revision"],
                "card_sha256": record_sha256(card),
                "result_sha256": result_sha,
                "completion_review_sha256": review_sha,
                "accepted_commit": "a" * 40,
                "accepted_by": "ROOT-IM",
                "verdict": "ACCEPTED",
            }
            (workspace / "COMPLETION_REVIEW.json").write_bytes(review_bytes)
            (workspace / "ORCHESTRATOR_ACCEPTANCE.json").write_text(
                json.dumps(acceptance, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            fixture = SuiteFixture.create()
            try:
                accepted = discover_run(root / "run", workspace, fixture.config)
                self.assertEqual("ACCEPTED", accepted.result_acceptance_state)
            finally:
                fixture.close()

            resume = dict(raw)
            resume["action"] = "resume"
            resume["resume"] = {"session_id": "session-accepted"}
            resume_path = workspace / "resume-accepted.invocation.json"
            resume_path.write_text(json.dumps(resume), encoding="utf-8")
            self.assertEqual(1, marker.read_text(encoding="utf-8").count("launch"))
            self.assertEqual(2, controller.main([str(resume_path)]))
            self.assertEqual(1, marker.read_text(encoding="utf-8").count("launch"))
            status = json.loads((workspace / "worker_controller.status.json").read_text(encoding="utf-8"))
            self.assertEqual("ACCEPTED", status["terminal_acceptance_state"])
            self.assertEqual("ACCEPTED", status["resume_identity"]["terminal_acceptance_state"])
            self.assertEqual(
                acceptance["accepted_commit"],
                status["resume_identity"]["acceptance_identity"]["accepted_commit"],
            )

    def test_controller_result_gate_rejects_wrong_task_card_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw, card = self._canonical(root)
            invocation_path = root / "run" / ".agent-workspace" / "gate.invocation.json"
            invocation_path.write_text(json.dumps(raw), encoding="utf-8")
            invocation = controller.load_invocation(invocation_path)
            bundle = raw["prompt_bundle"]  # type: ignore[assignment]
            result = {
                "schema": TASK_RESULT_SCHEMA,
                "card_id": card["card_id"],
                "lane_id": card["lane_id"],
                "worker_invocation_id": card["worker_invocation_id"],
                "cohort_id": card["stage_cohort_id"],
                "revision": card["revision"],
                "task_card_sha256": "0" * 64,
                "branch": "synthetic",
                "commit": "a" * 40,
                "outcome": "PASS",
                "summary": "wrong card digest",
                "checks": [],
                "prompt_bundle_sha256": bundle["bundle_sha256"],
                "prompt_content_sha256": bundle["final_sha256"],
            }
            (root / "run" / ".agent-workspace" / "RESULT.json").write_text(json.dumps(result), encoding="utf-8")
            evidence, valid, task_result = controller._canonical_result_validation(invocation)
            self.assertFalse(valid)
            self.assertIsNone(task_result)
            self.assertIn("task card content identity", evidence["detail"])


if __name__ == "__main__":
    unittest.main()
