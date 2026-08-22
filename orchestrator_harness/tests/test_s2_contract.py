from __future__ import annotations

import json
import hashlib
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

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
from orchestrator_harness.provider import (
    ClaudeCodeProviderAdapter,
    ProviderEvent,
    ProviderLaunchSpec,
)
from orchestrator_harness.resume import (
    RESUME_AMENDMENT_REVIEW_SCHEMA,
    make_resume_admission,
    validate_resume_amendment_review,
)
from orchestrator_harness.task import (
    COMPLETION_REVIEW_FILENAME,
    COMPLETION_REVIEW_SCHEMA,
    CompletionReview,
    ORCHESTRATOR_ACCEPTANCE_FILENAME,
    ORCHESTRATOR_ACCEPTANCE_SCHEMA,
    OrchestratorAcceptance,
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
from orchestrator_harness.workspace_overlay import (
    SUPER_CACHE_NAME,
    ingest_super_cache,
    prepare_worktree,
)


class S2ContractTests(unittest.TestCase):
    @staticmethod
    def _prepare_canonical_overlay(root: Path, run: Path) -> Path:
        workspace = run / ".agent-workspace"
        overlay_source = root / "overlay-source"
        overlay_source.mkdir()
        overlay_harness = root / "overlay-harness"
        overlay_harness.mkdir()
        ingest_super_cache(
            source_folder=overlay_source, harness_worktree=overlay_harness
        )
        overlay_receipt = workspace / "overlay-receipt.json"
        prepare_worktree(
            super_cache=overlay_harness / SUPER_CACHE_NAME,
            target_worktree=run,
            role="subagent",
            receipt_path=overlay_receipt,
        )
        return overlay_receipt

    def _canonical(
        self, root: Path, *, with_overlay: bool = False
    ) -> tuple[dict[str, object], object]:
        run = root / "run"
        workspace = run / ".agent-workspace"
        workspace.mkdir(parents=True)
        runtime = root / "runtime"
        runtime.mkdir()
        overlay_receipt: Path | None = None
        if with_overlay:
            overlay_receipt = self._prepare_canonical_overlay(root, run)
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
            "profile-1",
            "implementer",
            "claude-code",
            "claude-test",
            ("Read",),
            ("repo",),
            ("resource-1",),
            ("CLAUDE_CODE_TEST",),
            ("WORKFLOW_TEST_FLAG",),
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
            "task_card": {
                "id": "card-1",
                "revision": "r1",
                "sha256": record_sha256(card),
            },
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
        if overlay_receipt is not None:
            value["overlay_receipt"] = str(overlay_receipt)
        return value, card

    @staticmethod
    def _write_canonical_json(path: Path, value: dict[str, object]) -> str:
        data = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        path.write_bytes(data)
        return hashlib.sha256(data).hexdigest()

    def _start_fake_canonical(
        self, root: Path
    ) -> tuple[dict[str, object], dict[str, object], Path, Path, Path]:
        fake_source = """
import json, sys
from pathlib import Path
marker = Path(sys.argv[1])
marker.write_text(marker.read_text(encoding='utf-8') + 'launch\\n' if marker.exists() else 'launch\\n', encoding='utf-8')
sys.stdin.read()
print(json.dumps({'type': 'system', 'subtype': 'init', 'session_id': 'session-1'}), flush=True)
print(json.dumps({'type': 'result', 'subtype': 'success', 'session_id': 'session-1'}), flush=True)
"""
        fake = root / "fake_claude.py"
        marker = root / "launches.txt"
        fake.write_text(fake_source, encoding="utf-8")
        raw, card = self._canonical(root, with_overlay=True)
        raw["provider"] = {
            **raw["provider"],  # type: ignore[arg-type]
            "command": [sys.executable, str(fake), str(marker)],
        }
        start_path = root / "start.invocation.json"
        start_path.write_text(json.dumps(raw), encoding="utf-8")
        self.assertEqual(0, controller.main([str(start_path)]))
        workspace = root / "run" / ".agent-workspace"
        status_path = workspace / "worker_controller.status.json"
        event_path = root / "runtime" / "events.jsonl"
        return raw, card, workspace, status_path, event_path

    def _amendment_review(
        self,
        root: Path,
        raw: dict[str, object],
        *,
        persisted: dict[str, object],
        old_card: Path,
        new_card: Path,
        old_prompt: Path,
        new_prompt: Path,
        disposition: str = "NO_CONTINUATION_REQUIRED",
        classification: str = "HARMLESS",
    ) -> dict[str, object]:
        def pair(old_path: Path, new_path: Path) -> dict[str, object]:
            return {
                "old_path": str(old_path),
                "old_sha256": hashlib.sha256(old_path.read_bytes()).hexdigest(),
                "new_path": str(new_path),
                "new_sha256": hashlib.sha256(new_path.read_bytes()).hexdigest(),
                "diff": {
                    "command": f"git diff --no-index -- {old_path} {new_path}",
                    "working_directory": str(root),
                    "exit_code": 1,
                    "stdout_encoding": "utf-8",
                    "stdout_sha256": "a" * 64,
                    "stdout_bytes": 1,
                    "stderr_sha256": "b" * 64,
                    "stderr_bytes": 0,
                },
            }

        return {
            "schema": RESUME_AMENDMENT_REVIEW_SCHEMA,
            "run_id": "synthetic-run",
            "stage_id": "S2",
            "lane_id": raw["lane_id"],
            "recorded_utc": "2026-01-01T00:00:00Z",
            "recorded_by": "ROOT-IM",
            "disposition": disposition,
            "job_state": "UNACCEPTED",
            "same_job_identity": {
                "card_id": raw["task_card"]["id"],  # type: ignore[index]
                "stage_cohort_id": raw["cohort_id"],
                "worker_invocation_id": raw["worker_invocation_id"],
                "lane_id": raw["lane_id"],
                "task_kind": "focused_implementation",
                "provider_session_id": "session-1",
                "repository_common_dir": "synthetic-common-dir",
                "worktree_root": str(root),
                "branch": "synthetic-branch",
                "original_base_commit": "0" * 40,
                "continuation_start_commit": "0" * 40,
                "exclusive_resources": list(raw["resources"]),
            },
            "reviewed_pairs": {
                "task_card": pair(old_card, new_card),
                "prompt": pair(old_prompt, new_prompt),
            },
            "semantic_impact": {
                "classification": classification,
                "rationale": "ROOT reviewed this exact pair for the focused controller test.",
                "affected_scope": ["canonical resume identity"],
                "preserved_credit": ["unaffected prior checks"],
            },
            "route": {
                "kind": "EXACT_REVIEWED_PAIR",
                "resume_same_worker": True,
                "resume_same_provider_session": True,
                "reopen_accepted_tasks": False,
                "rerun_only_affected_checks": True,
                "require_fresh_affected_review": False,
            },
        }

    def test_canonical_prompt_resume_and_task_advancement_are_content_bound(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw, card_raw = self._canonical(root)
            canonical = parse_canonical_invocation(raw)
            invocation_path = (
                root / "run" / ".agent-workspace" / "canonical.invocation.json"
            )
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

            identity = canonical.identity(
                session_id="session-1", starting_commit="a" * 40
            )
            identity["live_identity"] = {
                "provider_id": "claude-code",
                "session_id": "session-1",
            }
            persisted = dict(identity)
            live = {
                "live_identity": {
                    "provider_id": "claude-code",
                    "session_id": "session-1",
                },
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

    def test_canonical_resume_admits_exact_reviewed_card_and_prompt_amendments(
        self,
    ) -> None:
        for amendment_kind in ("card", "prompt"):
            with (
                self.subTest(amendment_kind=amendment_kind),
                tempfile.TemporaryDirectory() as temporary,
            ):
                root = Path(temporary)
                raw, card, workspace, status_path, _ = self._start_fake_canonical(root)
                persisted = json.loads(status_path.read_text(encoding="utf-8"))
                persisted_identity = persisted["resume_identity"]

                old_card = workspace / f"{amendment_kind}-card-old.json"
                new_card = workspace / f"{amendment_kind}-card-new.json"
                self._write_canonical_json(old_card, card)
                changed_card = dict(card)
                changed_card["owner"] = "ROOT-IM"
                new_card_hash = self._write_canonical_json(new_card, changed_card)
                old_prompt = workspace / f"{amendment_kind}-prompt-old.md"
                new_prompt = workspace / f"{amendment_kind}-prompt-new.md"
                old_prompt.write_bytes(b"workflow\ntask\n")
                if amendment_kind == "card":
                    new_prompt.write_bytes(old_prompt.read_bytes())
                    requested_card_hash = new_card_hash
                    requested_bundle = raw["prompt_bundle"]
                else:
                    new_prompt.write_bytes(b"workflow\ntask amended\n")
                    components = raw["prompt_bundle"]["components"]  # type: ignore[index]
                    prompt_a = Path(components[0]["path"])
                    prompt_b = Path(components[1]["path"])
                    prompt_b.write_bytes(b"task amended\n")
                    requested_bundle = prompt_bundle_record_from_paths(
                        workflow_id="workflow-1",
                        task_card_id="card-1",
                        profile_id="profile-1",
                        paths=(("instructions", prompt_a), ("task", prompt_b)),
                        run_root=root / "run",
                    )
                    requested_card_hash = raw["task_card"]["sha256"]
                    requested_card_hash = str(requested_card_hash)
                    requested_card_hash = requested_card_hash.lower()
                resume = json.loads(json.dumps(raw))
                resume["action"] = "resume"
                resume["resume"] = {"session_id": "session-1"}
                resume["task_card"] = {
                    **resume["task_card"],
                    "sha256": requested_card_hash
                    if amendment_kind == "prompt"
                    else new_card_hash,
                }
                resume["prompt_bundle"] = requested_bundle
                review = self._amendment_review(
                    root,
                    resume,
                    persisted=persisted_identity,
                    old_card=old_card,
                    new_card=new_card if amendment_kind == "card" else old_card,
                    old_prompt=old_prompt,
                    new_prompt=new_prompt,
                )
                review_path = workspace / "RESUME_ADMISSION.json"
                review_path.write_text(json.dumps(review, indent=2), encoding="utf-8")
                resume_path = root / f"resume-{amendment_kind}.invocation.json"
                resume_path.write_text(json.dumps(resume), encoding="utf-8")

                self.assertEqual(0, controller.main([str(resume_path)]))
                self.assertEqual(
                    2,
                    (root / "launches.txt").read_text(encoding="utf-8").count("launch"),
                )
                admitted = json.loads(status_path.read_text(encoding="utf-8"))[
                    "resume_admission"
                ]
                self.assertTrue(admitted["admitted"])
                self.assertEqual(
                    "NO_CONTINUATION_REQUIRED",
                    admitted["amendment_review"]["disposition"],
                )
                self.assertEqual(
                    resume["task_card"]["sha256"],
                    json.loads(status_path.read_text(encoding="utf-8"))[
                        "resume_identity"
                    ]["task_card_sha256"],
                )

    def test_canonical_resume_amendment_rejects_unreviewed_material_wrong_pair_and_accepted(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw, card, workspace, status_path, event_path = self._start_fake_canonical(
                root
            )
            persisted = json.loads(status_path.read_text(encoding="utf-8"))
            persisted_identity = persisted["resume_identity"]
            old_card = workspace / "card-old.json"
            new_card = workspace / "card-new.json"
            wrong_card = workspace / "card-wrong.json"
            self._write_canonical_json(old_card, card)
            changed_card = dict(card)
            changed_card["owner"] = "ROOT-IM"
            new_hash = self._write_canonical_json(new_card, changed_card)
            self._write_canonical_json(wrong_card, {**changed_card, "owner": "wrong"})
            old_prompt = workspace / "prompt-old.md"
            new_prompt = workspace / "prompt-new.md"
            old_prompt.write_bytes(b"workflow\ntask\n")
            new_prompt.write_bytes(old_prompt.read_bytes())
            resume = json.loads(json.dumps(raw))
            resume["action"] = "resume"
            resume["resume"] = {"session_id": "session-1"}
            resume["task_card"] = {**resume["task_card"], "sha256": new_hash}
            resume_path = root / "resume-amendment-control.json"
            resume_path.write_text(json.dumps(resume), encoding="utf-8")
            status_before = status_path.read_bytes()
            event_before = event_path.read_bytes()

            with patch.object(
                controller,
                "provider_adapter",
                side_effect=AssertionError("provider must not be called"),
            ):
                with self.assertRaisesRegex(
                    controller.InvocationError, "resume amendment"
                ):
                    controller.run(controller.load_invocation(resume_path))
            self.assertEqual(status_before, status_path.read_bytes())
            self.assertEqual(event_before, event_path.read_bytes())

            material = self._amendment_review(
                root,
                resume,
                persisted=persisted_identity,
                old_card=old_card,
                new_card=new_card,
                old_prompt=old_prompt,
                new_prompt=new_prompt,
                disposition="CONTINUATION_REQUIRED",
                classification="MATERIAL",
            )
            (workspace / "RESUME_ADMISSION.json").write_text(
                json.dumps(material), encoding="utf-8"
            )
            with patch.object(
                controller,
                "provider_adapter",
                side_effect=AssertionError("provider must not be called"),
            ):
                with self.assertRaisesRegex(
                    controller.InvocationError, "scoped continuation"
                ):
                    controller.run(controller.load_invocation(resume_path))
            self.assertEqual(status_before, status_path.read_bytes())
            self.assertEqual(event_before, event_path.read_bytes())

            wrong_pair = self._amendment_review(
                root,
                resume,
                persisted=persisted_identity,
                old_card=old_card,
                new_card=wrong_card,
                old_prompt=old_prompt,
                new_prompt=new_prompt,
            )
            (workspace / "RESUME_ADMISSION.json").write_text(
                json.dumps(wrong_pair), encoding="utf-8"
            )
            with patch.object(
                controller,
                "provider_adapter",
                side_effect=AssertionError("provider must not be called"),
            ):
                with self.assertRaisesRegex(
                    controller.InvocationError, "reviewed task-card new identity"
                ):
                    controller.run(controller.load_invocation(resume_path))
            self.assertEqual(status_before, status_path.read_bytes())
            self.assertEqual(event_before, event_path.read_bytes())

            accepted = dict(persisted)
            accepted["terminal_acceptance_state"] = "ACCEPTED"
            accepted["resume_identity"] = {
                **persisted_identity,
                "terminal_acceptance_state": "ACCEPTED",
            }
            status_path.write_text(json.dumps(accepted), encoding="utf-8")
            accepted_before = status_path.read_bytes()
            harmless = self._amendment_review(
                root,
                resume,
                persisted=persisted_identity,
                old_card=old_card,
                new_card=new_card,
                old_prompt=old_prompt,
                new_prompt=new_prompt,
            )
            (workspace / "RESUME_ADMISSION.json").write_text(
                json.dumps(harmless), encoding="utf-8"
            )
            with patch.object(
                controller,
                "provider_adapter",
                side_effect=AssertionError("provider must not be called"),
            ):
                with self.assertRaisesRegex(
                    controller.InvocationError, "accepted task"
                ):
                    controller.run(controller.load_invocation(resume_path))
            self.assertEqual(accepted_before, status_path.read_bytes())

    def test_canonical_resume_admits_valid_large_root_review_without_local_cap(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw, card, workspace, status_path, _ = self._start_fake_canonical(root)
            persisted = json.loads(status_path.read_text(encoding="utf-8"))
            persisted_identity = persisted["resume_identity"]
            old_card = workspace / "large-card-old.json"
            new_card = workspace / "large-card-new.json"
            self._write_canonical_json(old_card, card)
            changed_card = dict(card)
            changed_card["owner"] = "ROOT-IM"
            new_card_hash = self._write_canonical_json(new_card, changed_card)
            old_prompt = workspace / "large-prompt-old.md"
            new_prompt = workspace / "large-prompt-new.md"
            old_prompt.write_bytes(b"workflow\ntask\n")
            new_prompt.write_bytes(old_prompt.read_bytes())
            resume = json.loads(json.dumps(raw))
            resume["action"] = "resume"
            resume["resume"] = {"session_id": "session-1"}
            resume["task_card"] = {**resume["task_card"], "sha256": new_card_hash}
            review = self._amendment_review(
                root,
                resume,
                persisted=persisted_identity,
                old_card=old_card,
                new_card=new_card,
                old_prompt=old_prompt,
                new_prompt=new_prompt,
            )
            review["semantic_impact"]["rationale"] = "x" * (2 * 1024 * 1024)  # type: ignore[index]
            review_path = workspace / "RESUME_ADMISSION.json"
            review_path.write_text(json.dumps(review), encoding="utf-8")
            self.assertGreater(review_path.stat().st_size, 2 * 1024 * 1024)
            resume_path = root / "resume-large-review.invocation.json"
            resume_path.write_text(json.dumps(resume), encoding="utf-8")

            self.assertEqual(0, controller.main([str(resume_path)]))
            self.assertEqual(
                2, (root / "launches.txt").read_text(encoding="utf-8").count("launch")
            )

    def test_review_pair_paths_require_absolute_identity_before_hashing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw, card, workspace, status_path, _ = self._start_fake_canonical(root)
            persisted = json.loads(status_path.read_text(encoding="utf-8"))
            persisted_identity = persisted["resume_identity"]
            old_card = workspace / "path-card-old.json"
            new_card = workspace / "path-card-new.json"
            self._write_canonical_json(old_card, card)
            changed_card = dict(card)
            changed_card["owner"] = "ROOT-IM"
            new_card_hash = self._write_canonical_json(new_card, changed_card)
            old_prompt = workspace / "path-prompt-old.md"
            new_prompt = workspace / "path-prompt-new.md"
            old_prompt.write_bytes(b"workflow\ntask\n")
            new_prompt.write_bytes(old_prompt.read_bytes())
            resume = json.loads(json.dumps(raw))
            resume["action"] = "resume"
            resume["resume"] = {"session_id": "session-1"}
            resume["task_card"] = {**resume["task_card"], "sha256": new_card_hash}
            review = self._amendment_review(
                root,
                resume,
                persisted=persisted_identity,
                old_card=old_card,
                new_card=new_card,
                old_prompt=old_prompt,
                new_prompt=new_prompt,
            )
            for name, pair in review["reviewed_pairs"].items():  # type: ignore[union-attr]
                pair["old_path"] = f"relative-{name}-old"
                pair["new_path"] = f"relative-{name}-new"
                pair["diff"]["command"] = (
                    f"git diff --no-index -- {pair['old_path']} {pair['new_path']}"
                )
            requested = dict(persisted_identity)
            with self.assertRaisesRegex(ValueError, "absolute"):
                validate_resume_amendment_review(
                    review,
                    requested,
                    persisted_identity,
                    expected_job_identity=review["same_job_identity"],
                )

    def test_canonical_output_reservations_reject_before_workspace_mutation(
        self,
    ) -> None:
        reserved_names = (
            "RESULT.json",
            COMPLETION_REVIEW_FILENAME,
            ORCHESTRATOR_ACCEPTANCE_FILENAME,
        )
        output_names = ("status", "jsonl", "stderr", "last_message")
        pairs = (
            ("status", "jsonl"),
            ("status", "stderr"),
            ("status", "last_message"),
            ("jsonl", "stderr"),
            ("jsonl", "last_message"),
            ("stderr", "last_message"),
        )

        def assert_rejected(raw: dict[str, object], root: Path, label: str) -> None:
            workspace = root / "run" / ".agent-workspace"
            event_parent = root / "runtime" / "event-parent"
            raw["event_log_path"] = str(event_parent / "events.jsonl")
            invocation_path = root / f"{label}.invocation.json"
            invocation_path.write_text(json.dumps(raw), encoding="utf-8")
            self.assertTrue(workspace.is_dir())
            workspace.rmdir()
            with self.assertRaises(controller.InvocationError):
                load_invocation(invocation_path)
            self.assertFalse(workspace.exists())
            self.assertFalse(event_parent.exists())

        for reserved_name in reserved_names:
            for output_name in output_names:
                with self.subTest(reserved_name=reserved_name, output_name=output_name):
                    with tempfile.TemporaryDirectory() as temporary:
                        root = Path(temporary)
                        raw, _ = self._canonical(root)
                        output_paths = dict(raw["output_paths"])  # type: ignore[arg-type]
                        output_paths[output_name] = str(
                            root / "run" / ".agent-workspace" / reserved_name
                        )
                        raw["output_paths"] = output_paths
                        assert_rejected(raw, root, f"reserved-{output_name}")

        for first, second in pairs:
            with self.subTest(first=first, second=second):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    raw, _ = self._canonical(root)
                    output_paths = dict(raw["output_paths"])  # type: ignore[arg-type]
                    output_paths[second] = output_paths[first]
                    raw["output_paths"] = output_paths
                    assert_rejected(raw, root, f"duplicate-{first}-{second}")

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
            result_bytes = (
                json.dumps(result_raw, indent=2, sort_keys=True) + "\n"
            ).encode("utf-8")
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
            review_bytes = (json.dumps(review, indent=2, sort_keys=True) + "\n").encode(
                "utf-8"
            )
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
            self.assertRaises(
                TaskValidationError,
                read_task_advancement,
                workspace,
                card=card,
                result=result,
            )
            (workspace / "ORCHESTRATOR_ACCEPTANCE.json").write_text(
                "not-json", encoding="utf-8"
            )
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
        self.assertEqual(
            "session-1",
            cast(
                ProviderEvent,
                adapter.parse_transcript_line(
                    b'{"type":"system","subtype":"init","session_id":"session-1"}'
                ),
            ).session_id,
        )  # type: ignore[union-attr]
        self.assertEqual(
            "COMPLETED",
            cast(
                ProviderEvent,
                adapter.parse_transcript_line(
                    b'{"type":"result","subtype":"success","session_id":"session-1"}'
                ),
            ).kind,
        )  # type: ignore[union-attr]
        self.assertEqual(
            "FAILED",
            cast(
                ProviderEvent,
                adapter.parse_transcript_line(
                    b'{"type":"result","subtype":"error_during_execution","session_id":"session-1"}'
                ),
            ).kind,
        )  # type: ignore[union-attr]

        profile = RuntimeProfile(
            "profile",
            "worker",
            "claude-code",
            "claude-test",
            (),
            (),
            (),
            ("CLAUDE_CODE_TEST",),
            ("WORKFLOW_TEST_FLAG",),
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
        self.assertEqual(
            {
                "PATH": "path",
                "CLAUDE_CODE_TEST": "provider",
                "WORKFLOW_TEST_FLAG": "workflow",
            },
            environment,
        )
        self.assertEqual(
            ["ANTHROPIC_API_KEY", "GITHUB_TOKEN", "MCP_ENDPOINT", "PYOCD_TARGET"],
            cleared,
        )

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
        self.assertEqual(
            "legacy-firmware", adapt_legacy_firmware(firmware).legacy_route
        )
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
            for field in (
                "provider",
                "profile",
                "prompt_bundle",
                "workflow",
                "task_card",
                "cohort_id",
                "unknown_field",
            ):
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

    def test_controller_fake_claude_start_resume_failure_and_wrong_session(
        self,
    ) -> None:
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
            raw, card = self._canonical(root, with_overlay=True)
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
            invocation_path = (
                root / "run" / ".agent-workspace" / "start.invocation.json"
            )
            invocation_path.write_text(json.dumps(raw), encoding="utf-8")
            self.assertEqual(0, controller.main([str(invocation_path)]))
            (root / "run" / ".agent-workspace" / "RESULT.json").write_text(
                json.dumps(result), encoding="utf-8"
            )
            status_path = (
                root / "run" / ".agent-workspace" / "worker_controller.status.json"
            )
            status = json.loads(status_path.read_text(encoding="utf-8"))
            self.assertEqual("PROVIDER_EXITED", status["state"])
            self.assertEqual("claude-code", status["provider_id"])
            self.assertEqual("session-1", status["provider_session_id"])
            self.assertEqual("MISSING", status["result_validation"]["state"])
            self.assertEqual("PENDING", status["terminal_acceptance_state"])
            self.assertTrue(
                (root / "run" / ".agent-workspace" / "worker_provider.jsonl").is_file()
            )

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
            # REQ-O35: identity-mismatched resume emits a declared same-role
            # structured handoff with fabricated_continuity=false instead of
            # silently rejecting and losing the logical task.
            self.assertEqual(1, controller.main([str(wrong_path)]))
            wrong_status = json.loads(status_path.read_text(encoding="utf-8"))
            self.assertEqual("PROVIDER_HANDOFF", wrong_status["state"])
            handoff = wrong_status["provider_handoff"]
            self.assertIsNotNone(handoff)
            self.assertFalse(handoff["fabricated_continuity"])
            self.assertEqual("session-1", handoff["prior_session_id"])
            self.assertEqual("wrong-session", handoff["requested_session_id"])
            self.assertIn("session", handoff["reason"])

            failure_root = root / "failure"
            failure_raw, _ = self._canonical(failure_root, with_overlay=True)
            failure_raw["provider"] = {
                **failure_raw["provider"],  # type: ignore[arg-type]
                "command": [sys.executable, str(fake), "--failure"],
            }
            failure_path = (
                failure_root / "run" / ".agent-workspace" / "failure.invocation.json"
            )
            failure_path.write_text(json.dumps(failure_raw), encoding="utf-8")
            self.assertEqual(1, controller.main([str(failure_path)]))
            failure_status = json.loads(
                (
                    failure_root
                    / "run"
                    / ".agent-workspace"
                    / "worker_controller.status.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual("FAILED", failure_status["provider_terminal_outcome"])

    def test_controller_acceptance_chain_blocks_start_and_resume_before_fake_provider(
        self,
    ) -> None:
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
            raw, card = self._canonical(root, with_overlay=True)
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
            invocation_path = (
                root / "run" / ".agent-workspace" / "start.invocation.json"
            )
            invocation_path.write_text(json.dumps(raw), encoding="utf-8")
            self.assertEqual(0, controller.main([str(invocation_path)]))
            workspace = root / "run" / ".agent-workspace"
            (workspace / "RESULT.json").write_text(json.dumps(result), encoding="utf-8")
            self.assertEqual(
                "PENDING",
                json.loads((workspace / "worker_controller.status.json").read_text())[
                    "terminal_acceptance_state"
                ],
            )
            pending_status_bytes = (
                workspace / "worker_controller.status.json"
            ).read_bytes()
            pending_start = workspace / "start-pending.invocation.json"
            pending_start.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(
                controller.InvocationError, "action:start.*resume"
            ):
                controller.run(controller.load_invocation(pending_start))
            self.assertEqual(1, marker.read_text(encoding="utf-8").count("launch"))
            self.assertEqual(
                pending_status_bytes,
                (workspace / "worker_controller.status.json").read_bytes(),
            )

            different_card = dict(raw)
            different_card["task_card"] = {
                **cast(dict[str, Any], different_card["task_card"]),
                "revision": "different",
            }  # type: ignore[arg-type]
            different_path = workspace / "start-different-card.invocation.json"
            different_path.write_text(json.dumps(different_card), encoding="utf-8")
            with self.assertRaisesRegex(
                controller.InvocationError, "identity does not match"
            ):
                controller.run(controller.load_invocation(different_path))
            self.assertEqual(1, marker.read_text(encoding="utf-8").count("launch"))
            self.assertEqual(
                pending_status_bytes,
                (workspace / "worker_controller.status.json").read_bytes(),
            )

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
            review_bytes = (json.dumps(review, indent=2, sort_keys=True) + "\n").encode(
                "utf-8"
            )
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

            start = dict(raw)
            start_path = workspace / "start-accepted.invocation.json"
            start_path.write_text(json.dumps(start), encoding="utf-8")
            self.assertEqual(1, marker.read_text(encoding="utf-8").count("launch"))
            self.assertEqual(2, controller.main([str(start_path)]))
            self.assertEqual(1, marker.read_text(encoding="utf-8").count("launch"))
            status = json.loads(
                (workspace / "worker_controller.status.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual("ACCEPTED", status["terminal_acceptance_state"])
            self.assertEqual("ACCEPTED", status["task_advancement_state"])
            self.assertEqual(
                "ACCEPTED", status["resume_identity"]["terminal_acceptance_state"]
            )
            self.assertEqual(
                "ACCEPTED", status["resume_identity"]["task_advancement_state"]
            )
            self.assertEqual(
                acceptance["accepted_commit"],
                status["resume_identity"]["acceptance_identity"]["accepted_commit"],
            )

            resume = dict(raw)
            resume["action"] = "resume"
            resume["resume"] = {"session_id": "session-accepted"}
            resume_path = workspace / "resume-accepted.invocation.json"
            resume_path.write_text(json.dumps(resume), encoding="utf-8")
            self.assertEqual(1, marker.read_text(encoding="utf-8").count("launch"))
            self.assertEqual(2, controller.main([str(resume_path)]))
            self.assertEqual(1, marker.read_text(encoding="utf-8").count("launch"))
            status = json.loads(
                (workspace / "worker_controller.status.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual("ACCEPTED", status["terminal_acceptance_state"])
            self.assertEqual(
                "ACCEPTED", status["resume_identity"]["terminal_acceptance_state"]
            )
            self.assertEqual("ACCEPTED", status["task_advancement_state"])
            self.assertEqual(
                "ACCEPTED", status["resume_identity"]["task_advancement_state"]
            )
            self.assertEqual(
                acceptance["accepted_commit"],
                status["resume_identity"]["acceptance_identity"]["accepted_commit"],
            )

            event_log = root / "runtime" / "events.jsonl"
            event_bytes = event_log.read_bytes()
            status_path = workspace / "worker_controller.status.json"
            status_path.unlink()
            missing_status_path = workspace / "start-missing-status.invocation.json"
            missing_status_path.write_text(json.dumps(start), encoding="utf-8")
            self.assertEqual(2, controller.main([str(missing_status_path)]))
            self.assertEqual(1, marker.read_text(encoding="utf-8").count("launch"))
            self.assertFalse(status_path.exists())
            self.assertEqual(event_bytes, event_log.read_bytes())

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
            (root / "run" / ".agent-workspace" / "RESULT.json").write_text(
                json.dumps(result), encoding="utf-8"
            )
            evidence, valid, task_result = controller._canonical_result_validation(
                invocation
            )
            self.assertFalse(valid)
            self.assertIsNone(task_result)
            self.assertIn("task card content identity", evidence["detail"])

    def test_canonical_no_status_fixed_artifacts_are_occupied(self) -> None:
        fake_source = """
import json, sys
from pathlib import Path
marker = Path(sys.argv[1])
marker.write_text(marker.read_text(encoding='utf-8') + 'launch\\n' if marker.exists() else 'launch\\n', encoding='utf-8')
sys.stdin.read()
print(json.dumps({'type': 'system', 'subtype': 'init', 'session_id': 'fresh-session'}), flush=True)
print(json.dumps({'type': 'result', 'subtype': 'success', 'session_id': 'fresh-session'}), flush=True)
"""
        cases = ("result-only", "partial", "malformed", "different", "fresh")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                fake = root / "fake_claude.py"
                fake.write_text(fake_source, encoding="utf-8")
                marker = root / "launches.txt"
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
                    "summary": "synthetic result",
                    "checks": [{"name": "fake", "outcome": "PASS"}],
                    "prompt_bundle_sha256": bundle["bundle_sha256"],
                    "prompt_content_sha256": bundle["final_sha256"],
                }
                workspace = root / "run" / ".agent-workspace"
                if case == "malformed":
                    (workspace / "RESULT.json").write_text("not-json", encoding="utf-8")
                elif case != "fresh":
                    if case == "different":
                        result["card_id"] = "different-card"
                    (workspace / "RESULT.json").write_text(
                        json.dumps(result), encoding="utf-8"
                    )
                    if case == "partial":
                        (workspace / COMPLETION_REVIEW_FILENAME).write_text(
                            "not-json", encoding="utf-8"
                        )
                if case == "fresh":
                    workspace.rmdir()
                    raw["overlay_receipt"] = str(
                        self._prepare_canonical_overlay(root, root / "run")
                    )
                event_parent = root / "runtime" / "missing-events"
                raw["event_log_path"] = str(event_parent / "events.jsonl")
                invocation_path = root / f"{case}.invocation.json"
                invocation_path.write_text(json.dumps(raw), encoding="utf-8")
                status_path = workspace / "worker_controller.status.json"
                if case == "fresh":
                    self.assertEqual(0, controller.main([str(invocation_path)]))
                    self.assertEqual(
                        1, marker.read_text(encoding="utf-8").count("launch")
                    )
                    self.assertTrue(status_path.is_file())
                    self.assertTrue((event_parent / "events.jsonl").is_file())
                else:
                    self.assertEqual(2, controller.main([str(invocation_path)]))
                    self.assertFalse(marker.exists())
                    self.assertFalse(status_path.exists())
                    self.assertFalse(event_parent.exists())

    def test_canonical_provider_launch_identity_is_immutable_before_adapter(
        self,
    ) -> None:
        fake_source = """
import json, sys
from pathlib import Path
marker = Path(sys.argv[1])
marker.write_text(marker.read_text(encoding='utf-8') + 'launch\\n' if marker.exists() else 'launch\\n', encoding='utf-8')
sys.stdin.read()
print(json.dumps({'type': 'system', 'subtype': 'init', 'session_id': 'provider-session'}), flush=True)
print(json.dumps({'type': 'result', 'subtype': 'success', 'session_id': 'provider-session'}), flush=True)
"""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake = root / "fake_claude.py"
            fake.write_text(fake_source, encoding="utf-8")
            marker = root / "launches.txt"
            raw, _ = self._canonical(root, with_overlay=True)
            raw["provider"] = {
                **raw["provider"],  # type: ignore[arg-type]
                "command": [sys.executable, str(fake), str(marker)],
            }
            start_path = root / "start.invocation.json"
            start_path.write_text(json.dumps(raw), encoding="utf-8")
            self.assertEqual(0, controller.main([str(start_path)]))
            workspace = root / "run" / ".agent-workspace"
            status_path = workspace / "worker_controller.status.json"
            status_before = status_path.read_bytes()
            persisted_identity = json.loads(status_before)["resume_identity"]
            self.assertEqual(64, len(persisted_identity["provider_launch_sha256"]))
            self.assertNotIn("provider_options", persisted_identity)
            self.assertNotIn("provider_launch_record", persisted_identity)

            changes = {
                "command": [sys.executable, str(fake), str(marker), "changed"],
                "permission_mode": "acceptEdits",
                "allowed_tools": ["Read", "Write"],
                "mcp_config": {"mcpServers": {"synthetic": {"command": ["synthetic"]}}},
                "config_overrides": ["changed=true"],
                "reasoning_effort": "high",
                "service_tier": "standard",
                "sandbox": "read-only",
                "approval_policy": "on-request",
            }
            for field, changed_value in changes.items():
                with self.subTest(field=field):
                    resume = json.loads(json.dumps(raw))
                    resume["action"] = "resume"
                    resume["resume"] = {"session_id": "provider-session"}
                    resume["provider"][field] = changed_value
                    resume_path = root / f"resume-{field}.invocation.json"
                    resume_path.write_text(json.dumps(resume), encoding="utf-8")
                    with patch.object(
                        controller,
                        "provider_adapter",
                        side_effect=AssertionError("adapter must not be called"),
                    ):
                        with self.assertRaises(controller.InvocationError):
                            controller.run(controller.load_invocation(resume_path))
                    self.assertEqual(
                        1, marker.read_text(encoding="utf-8").count("launch")
                    )
                    self.assertEqual(status_before, status_path.read_bytes())

            unchanged = json.loads(json.dumps(raw))
            unchanged["action"] = "resume"
            unchanged["resume"] = {"session_id": "provider-session"}
            unchanged_path = root / "resume-unchanged.invocation.json"
            unchanged_path.write_text(json.dumps(unchanged), encoding="utf-8")
            self.assertEqual(0, controller.main([str(unchanged_path)]))
            self.assertEqual(2, marker.read_text(encoding="utf-8").count("launch"))

    def test_canonical_codex_last_message_path_change_rejects_before_adapter(
        self,
    ) -> None:
        fake_source = """
import json, sys
from pathlib import Path
marker = Path(sys.argv[1])
marker.write_text(marker.read_text(encoding='utf-8') + 'launch\\n' if marker.exists() else 'launch\\n', encoding='utf-8')
sys.stdin.buffer.read()
print(json.dumps({'type': 'thread.started', 'thread_id': 'provider-session'}), flush=True)
print(json.dumps({'type': 'turn.completed', 'thread_id': 'provider-session'}), flush=True)
"""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake = root / "fake_codex.py"
            fake.write_text(fake_source, encoding="utf-8")
            marker = root / "launches.txt"
            raw, _ = self._canonical(root, with_overlay=True)
            raw["provider"] = {
                **raw["provider"],  # type: ignore[arg-type]
                "id": "codex",
                "command": [sys.executable, str(fake), str(marker)],
            }
            raw["profile"] = {
                **raw["profile"],  # type: ignore[arg-type]
                "provider": "codex",
            }
            start_path = root / "start.invocation.json"
            start_path.write_text(json.dumps(raw), encoding="utf-8")
            self.assertEqual(0, controller.main([str(start_path)]))

            workspace = root / "run" / ".agent-workspace"
            status_path = workspace / "worker_controller.status.json"
            event_path = root / "runtime" / "events.jsonl"
            resource_root = root / "runtime" / "canonical-resource-locks"
            status_before = status_path.read_bytes()
            event_before = event_path.read_bytes()
            resources_before = sorted(
                str(path.relative_to(resource_root))
                for path in resource_root.rglob("*")
            )
            self.assertEqual(1, marker.read_text(encoding="utf-8").count("launch"))

            resume = json.loads(json.dumps(raw))
            resume["action"] = "resume"
            resume["resume"] = {"session_id": "provider-session"}
            resume["output_paths"]["last_message"] = str(
                workspace / "worker.last-message.changed"
            )
            resume_path = root / "changed-last-message.invocation.json"
            resume_path.write_text(json.dumps(resume), encoding="utf-8")
            with (
                patch.object(controller, "provider_adapter") as provider_factory,
                patch.object(
                    controller, "ResourceClaims", wraps=controller.ResourceClaims
                ) as resource_factory,
            ):
                provider_factory.return_value.build_argv.side_effect = AssertionError(
                    "adapter build_argv must not be called"
                )
                with self.assertRaisesRegex(
                    controller.InvocationError, "prior task identity"
                ):
                    controller.run(controller.load_invocation(resume_path))
                provider_factory.assert_not_called()
                provider_factory.return_value.build_argv.assert_not_called()
                resource_factory.assert_not_called()

            self.assertEqual(status_before, status_path.read_bytes())
            self.assertEqual(event_before, event_path.read_bytes())
            self.assertEqual(
                resources_before,
                sorted(
                    str(path.relative_to(resource_root))
                    for path in resource_root.rglob("*")
                ),
            )
            self.assertEqual(1, marker.read_text(encoding="utf-8").count("launch"))

    def test_canonical_codex_equivalent_last_message_spellings_share_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw, _ = self._canonical(root)
            raw["provider"] = {
                **raw["provider"],  # type: ignore[arg-type]
                "id": "codex",
            }
            raw["profile"] = {
                **raw["profile"],  # type: ignore[arg-type]
                "provider": "codex",
            }
            equivalent = json.loads(json.dumps(raw))
            workspace = root / "run" / ".agent-workspace"
            equivalent["output_paths"]["last_message"] = str(
                workspace / "." / "nested" / ".." / "worker.last-message"
            )
            first_path = root / "first.invocation.json"
            second_path = root / "equivalent.invocation.json"
            first_path.write_text(json.dumps(raw), encoding="utf-8")
            second_path.write_text(json.dumps(equivalent), encoding="utf-8")
            first = load_invocation(first_path).canonical
            second = load_invocation(second_path).canonical
            assert first is not None
            assert second is not None
            self.assertEqual(
                first.provider_launch_sha256, second.provider_launch_sha256
            )

    def test_canonical_unconsumed_last_message_path_does_not_change_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw, _ = self._canonical(root)
            changed = json.loads(json.dumps(raw))
            workspace = root / "run" / ".agent-workspace"
            changed["output_paths"]["last_message"] = str(
                workspace / "worker.last-message.changed"
            )
            first = parse_canonical_invocation(raw)
            second = parse_canonical_invocation(changed)
            self.assertEqual("claude-code", first.provider_id)
            self.assertEqual(
                first.provider_launch_sha256, second.provider_launch_sha256
            )
            self.assertNotIn("last_message_path", first.provider_launch_record())

    def test_canonical_workspace_symlink_is_rejected_before_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw, _ = self._canonical(root)
            workspace = root / "run" / ".agent-workspace"
            workspace.rmdir()
            external = root / "external-workspace"
            external.mkdir()
            try:
                os.symlink(str(external), str(workspace), target_is_directory=True)
            except (OSError, NotImplementedError) as exc:
                if os.name != "nt":
                    self.skipTest(f"directory symlink is unavailable: {exc}")
                junction = subprocess.run(
                    [
                        "cmd.exe",
                        "/d",
                        "/c",
                        "mklink",
                        "/J",
                        str(workspace),
                        str(external),
                    ],
                    capture_output=True,
                    text=True,
                )
                if junction.returncode != 0:
                    self.skipTest(
                        f"directory symlink/junction is unavailable: {junction.stderr.strip()}"
                    )
            event_parent = root / "runtime" / "external-events"
            raw["event_log_path"] = str(event_parent / "events.jsonl")
            invocation_path = root / "symlink.invocation.json"
            invocation_path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(controller.InvocationError, "direct child"):
                controller.load_invocation(invocation_path)
            self.assertEqual([], list(external.iterdir()))
            self.assertFalse(event_parent.exists())

    def test_canonical_rejected_preflight_does_not_create_event_parent(self) -> None:
        fake_source = """
import json, sys
from pathlib import Path
marker = Path(sys.argv[1])
marker.write_text(marker.read_text(encoding='utf-8') + 'launch\\n' if marker.exists() else 'launch\\n', encoding='utf-8')
sys.stdin.read()
print(json.dumps({'type': 'system', 'subtype': 'init', 'session_id': 'mutation-session'}), flush=True)
print(json.dumps({'type': 'result', 'subtype': 'success', 'session_id': 'mutation-session'}), flush=True)
"""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake = root / "fake_claude.py"
            fake.write_text(fake_source, encoding="utf-8")
            marker = root / "launches.txt"
            raw, card = self._canonical(root, with_overlay=True)
            raw["provider"] = {
                **raw["provider"],  # type: ignore[arg-type]
                "command": [sys.executable, str(fake), str(marker)],
            }
            start_path = root / "start.invocation.json"
            start_path.write_text(json.dumps(raw), encoding="utf-8")
            self.assertEqual(0, controller.main([str(start_path)]))
            workspace = root / "run" / ".agent-workspace"
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
                "summary": "synthetic result",
                "checks": [{"name": "fake", "outcome": "PASS"}],
                "prompt_bundle_sha256": bundle["bundle_sha256"],
                "prompt_content_sha256": bundle["final_sha256"],
            }
            result_bytes = json.dumps(result).encode("utf-8")
            (workspace / "RESULT.json").write_bytes(result_bytes)
            status_path = workspace / "worker_controller.status.json"

            def candidate(name: str, *, action: str = "start") -> Path:
                value = json.loads(json.dumps(raw))
                value["action"] = action
                if action == "resume":
                    value["resume"] = {"session_id": "mutation-session"}
                value["event_log_path"] = str(root / "runtime" / name / "events.jsonl")
                path = root / f"{name}.invocation.json"
                path.write_text(json.dumps(value), encoding="utf-8")
                return path

            pending_status = status_path.read_bytes()
            self.assertEqual(2, controller.main([str(candidate("pending-start"))]))
            self.assertFalse((root / "runtime" / "pending-start").exists())
            self.assertEqual(pending_status, status_path.read_bytes())

            different = json.loads(json.dumps(raw))
            different["task_card"]["revision"] = "different"
            different["event_log_path"] = str(
                root / "runtime" / "different-task" / "events.jsonl"
            )
            different_path = root / "different-task.invocation.json"
            different_path.write_text(json.dumps(different), encoding="utf-8")
            self.assertEqual(2, controller.main([str(different_path)]))
            self.assertFalse((root / "runtime" / "different-task").exists())

            (workspace / COMPLETION_REVIEW_FILENAME).write_text(
                "not-json", encoding="utf-8"
            )
            self.assertEqual(2, controller.main([str(candidate("malformed-chain"))]))
            self.assertFalse((root / "runtime" / "malformed-chain").exists())
            (workspace / COMPLETION_REVIEW_FILENAME).unlink()

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
                "evidence": ["test://mutation"],
            }
            review_bytes = (json.dumps(review, indent=2, sort_keys=True) + "\n").encode(
                "utf-8"
            )
            acceptance = {
                "schema": ORCHESTRATOR_ACCEPTANCE_SCHEMA,
                "card_id": card["card_id"],
                "lane_id": card["lane_id"],
                "worker_invocation_id": card["worker_invocation_id"],
                "cohort_id": card["stage_cohort_id"],
                "revision": card["revision"],
                "card_sha256": record_sha256(card),
                "result_sha256": result_sha,
                "completion_review_sha256": hashlib.sha256(review_bytes).hexdigest(),
                "accepted_commit": "a" * 40,
                "accepted_by": "ROOT-IM",
                "verdict": "ACCEPTED",
            }
            (workspace / COMPLETION_REVIEW_FILENAME).write_bytes(review_bytes)
            (workspace / ORCHESTRATOR_ACCEPTANCE_FILENAME).write_text(
                json.dumps(acceptance, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            self.assertEqual(2, controller.main([str(candidate("accepted-start"))]))
            self.assertFalse((root / "runtime" / "accepted-start").exists())
            self.assertEqual(
                2, controller.main([str(candidate("accepted-resume", action="resume"))])
            )
            self.assertFalse((root / "runtime" / "accepted-resume").exists())

            fresh_root = root / "fresh"
            fresh_raw, _ = self._canonical(fresh_root, with_overlay=True)
            fresh_marker = root / "fresh-launches.txt"
            fresh_raw["provider"] = {
                **fresh_raw["provider"],  # type: ignore[arg-type]
                "command": [sys.executable, str(fake), str(fresh_marker)],
            }
            fresh_event_parent = fresh_root / "runtime" / "fresh-events"
            fresh_raw["event_log_path"] = str(fresh_event_parent / "events.jsonl")
            fresh_path = root / "fresh.invocation.json"
            fresh_path.write_text(json.dumps(fresh_raw), encoding="utf-8")
            self.assertEqual(0, controller.main([str(fresh_path)]))
            self.assertTrue((fresh_event_parent / "events.jsonl").is_file())

    def test_file_backed_declared_content_hashes_preserve_raw_chain_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw, card_raw = self._canonical(root)
            card = validate_task_card(card_raw)  # type: ignore[arg-type]
            bundle = raw["prompt_bundle"]  # type: ignore[assignment]
            result = {
                "schema": TASK_RESULT_SCHEMA,
                "card_id": card.card_id,
                "lane_id": card.lane_id,
                "worker_invocation_id": card.worker_invocation_id,
                "cohort_id": card.cohort_id,
                "revision": card.revision,
                "task_card_sha256": card.content_sha256,
                "branch": "synthetic",
                "commit": "a" * 40,
                "outcome": "PASS",
                "summary": "declared result",
                "checks": [{"name": "digest", "outcome": "PASS"}],
                "prompt_bundle_sha256": bundle["bundle_sha256"],
                "prompt_content_sha256": bundle["final_sha256"],
            }
            result["content_sha256"] = record_sha256(result)
            result_bytes = (json.dumps(result, indent=2, sort_keys=True) + "\n").encode(
                "utf-8"
            )
            result_record = validate_task_result(
                result, card=card, raw_bytes=result_bytes
            )
            self.assertEqual(
                hashlib.sha256(result_bytes).hexdigest(), result_record.content_sha256
            )

            review = {
                "schema": COMPLETION_REVIEW_SCHEMA,
                "card_id": card.card_id,
                "lane_id": card.lane_id,
                "worker_invocation_id": card.worker_invocation_id,
                "cohort_id": card.cohort_id,
                "revision": card.revision,
                "result_sha256": result_record.content_sha256,
                "owner": card.completion_review_owner,
                "verdict": "PASS",
                "evidence": ["test://declared"],
            }
            review["content_sha256"] = record_sha256(review)
            review_bytes = (json.dumps(review, indent=2, sort_keys=True) + "\n").encode(
                "utf-8"
            )
            review_record = validate_completion_review(
                review, card=card, result=result_record, raw_bytes=review_bytes
            )
            self.assertEqual(
                hashlib.sha256(review_bytes).hexdigest(), review_record.content_sha256
            )

            acceptance = {
                "schema": ORCHESTRATOR_ACCEPTANCE_SCHEMA,
                "card_id": card.card_id,
                "lane_id": card.lane_id,
                "worker_invocation_id": card.worker_invocation_id,
                "cohort_id": card.cohort_id,
                "revision": card.revision,
                "card_sha256": card.content_sha256,
                "result_sha256": result_record.content_sha256,
                "completion_review_sha256": review_record.content_sha256,
                "accepted_commit": result_record.commit,
                "accepted_by": "ROOT-IM",
                "verdict": "ACCEPTED",
            }
            acceptance["content_sha256"] = record_sha256(acceptance)
            acceptance_bytes = (
                json.dumps(acceptance, indent=2, sort_keys=True) + "\n"
            ).encode("utf-8")
            acceptance_record = validate_orchestrator_acceptance(
                acceptance,
                card=card,
                result=result_record,
                review=review_record,
                raw_bytes=acceptance_bytes,
            )
            self.assertEqual(
                hashlib.sha256(acceptance_bytes).hexdigest(),
                acceptance_record.content_sha256,
            )

            workspace = root / "chain-workspace"
            workspace.mkdir()
            (workspace / "RESULT.json").write_bytes(result_bytes)
            (workspace / COMPLETION_REVIEW_FILENAME).write_bytes(review_bytes)
            (workspace / ORCHESTRATOR_ACCEPTANCE_FILENAME).write_bytes(acceptance_bytes)
            advancement = read_task_advancement(
                workspace, card=card, result=result_record
            )
            self.assertEqual("ACCEPTED", advancement.state)
            self.assertEqual(
                hashlib.sha256(review_bytes).hexdigest(),
                cast(CompletionReview, advancement.review).content_sha256,
            )  # type: ignore[union-attr]
            self.assertEqual(
                hashlib.sha256(acceptance_bytes).hexdigest(),
                cast(OrchestratorAcceptance, advancement.acceptance).content_sha256,
            )  # type: ignore[union-attr]

            for original, validator, kwargs in (
                (result, validate_task_result, {"card": card}),
                (
                    review,
                    validate_completion_review,
                    {"card": card, "result": result_record},
                ),
                (
                    acceptance,
                    validate_orchestrator_acceptance,
                    {"card": card, "result": result_record, "review": review_record},
                ),
            ):
                with self.subTest(record=original["schema"]):
                    wrong_declaration = dict(original)
                    wrong_declaration["content_sha256"] = "0" * 64
                    wrong_bytes = (
                        json.dumps(wrong_declaration, indent=2, sort_keys=True) + "\n"
                    ).encode("utf-8")
                    with self.assertRaises(TaskValidationError):
                        validator(wrong_declaration, raw_bytes=wrong_bytes, **kwargs)
                    tampered = dict(original)
                    if original["schema"] == TASK_RESULT_SCHEMA:
                        tampered["summary"] = "tampered"
                    elif original["schema"] == COMPLETION_REVIEW_SCHEMA:
                        tampered["evidence"] = ["tampered"]
                    else:
                        tampered["accepted_by"] = "tampered"
                    tampered_bytes = (
                        json.dumps(tampered, indent=2, sort_keys=True) + "\n"
                    ).encode("utf-8")
                    with self.assertRaises(TaskValidationError):
                        validator(tampered, raw_bytes=tampered_bytes, **kwargs)


if __name__ == "__main__":
    unittest.main()
