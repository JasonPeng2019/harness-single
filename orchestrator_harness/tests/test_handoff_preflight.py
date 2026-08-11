# pyright: reportUninitializedInstanceVariable=false
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from collections.abc import Mapping
from pathlib import Path
from typing import cast, final
from unittest.mock import patch

from orchestrator_harness import cli
from orchestrator_harness.handoff_preflight import (
    EXIT_INCOMPLETE,
    EXIT_REPORT_ONLY_ERROR,
    INCOMPLETE,
    PASS,
    REPORT_ONLY_ERROR,
    preflight_handoff,
)
from orchestrator_harness.tests.support import TemporaryGitRepository, write_json


def _failed_predicates(result: Mapping[str, object]) -> set[str]:
    failures = result.get("failed_predicates")
    if not isinstance(failures, list):
        raise TypeError("preflight failed_predicates must be a list")
    predicates: set[str] = set()
    for item in cast(list[object], failures):
        if not isinstance(item, Mapping):
            raise TypeError("preflight failure must be an object")
        predicate = cast(Mapping[str, object], item).get("predicate")
        if not isinstance(predicate, str):
            raise TypeError("preflight failure predicate must be a string")
        predicates.add(predicate)
    return predicates


@final
class HandoffPreflightTests(unittest.TestCase):
    def setUp(self) -> None:  # pyright: ignore[reportImplicitOverride]
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.worktree = self.root / "worktree"
        self.repository = TemporaryGitRepository.create(self.worktree)
        _ = (self.worktree / ".gitignore").write_text(
            ".agent-workspace/\n", encoding="utf-8"
        )
        _ = self.repository.git("add", ".gitignore")
        _ = self.repository.git("commit", "-m", "ignore evidence")
        self.starting_commit = self.repository.head
        self.workspace = self.worktree / ".agent-workspace"
        _ = self.workspace.mkdir()
        self.task_card_path = self.root / "TASK_CARD.json"
        self.invocation_path = self.workspace / "INVOCATION.json"
        self.result_path = self.workspace / "RESULT.json"
        self.dependency_map_path = self.workspace / "DEPENDENCY_MAP.json"
        self.prompt_path = self.workspace / "PROMPT.md"
        _ = self.prompt_path.write_text(
            "Complete the bounded task.\n", encoding="utf-8"
        )
        self.output_paths = {
            "status": str(self.workspace / "status.json"),
            "jsonl": str(self.workspace / "worker.jsonl"),
            "stderr": str(self.workspace / "worker.stderr.log"),
            "last_message": str(self.workspace / "worker.last-message.txt"),
        }
        for output_path in self.output_paths.values():
            _ = Path(output_path).write_text("completed\n", encoding="utf-8")
        self.required_evidence = self.workspace / "proof.json"
        _ = self.required_evidence.write_text("{}\n", encoding="utf-8")
        self._write_handoff()

    def tearDown(self) -> None:  # pyright: ignore[reportImplicitOverride]
        self.temporary.cleanup()

    def _write_handoff(self) -> None:
        task_card = {
            "schema": "orchestrator-task-card/v1",
            "card_id": "card-1",
            "lane_id": "S3.P",
            "worker_invocation_id": "worker-1",
            "starting_state": {
                "branch": self.repository.branch,
                "starting_commit": self.starting_commit,
            },
        }
        write_json(self.task_card_path, task_card)
        invocation: dict[str, object] = {
            "schema": "orchestrator-coding-invocation/v1",
            "action": "start",
            "run_root": str(self.worktree),
            "runtime_root": str(self.root),
            "event_log_path": str(self.root / "events.jsonl"),
            "worker_invocation_id": "worker-1",
            "lane_id": "S3.P",
            "task": "bounded task",
            "phase": "implementation",
            "prompt_path": str(self.prompt_path),
            "prompt_sha256": hashlib.sha256(self.prompt_path.read_bytes()).hexdigest(),
            "output_paths": self.output_paths,
            "resources": [],
            "repository": {
                **self.repository.declaration(),
                "base_commit": self.starting_commit,
            },
            "codex": {
                "model": "configured-at-runtime",
                "reasoning_effort": "configured-at-runtime",
                "service_tier": "configured-at-runtime",
                "command": ["provider"],
                "config_overrides": [],
                "sandbox": "workspace-write",
                "approval_policy": "never",
            },
        }
        write_json(self.invocation_path, invocation)
        result: dict[str, object] = {
            "schema": "orchestrator-lane-result/v1",
            "lane_id": "S3.P",
            "worker_invocation_id": "worker-1",
            "branch": self.repository.branch,
            "commit": self.repository.head,
            "outcome": "PASS",
            "summary": "completed",
            "checks": [],
        }
        write_json(self.result_path, result)
        dependency_map: dict[str, object] = {
            "schema": "orchestrator-dependency-map/v1",
            "task": {
                "card_id": "card-1",
                "card_path": str(self.task_card_path),
                "card_sha256": hashlib.sha256(
                    self.task_card_path.read_bytes()
                ).hexdigest(),
                "lane_id": "S3.P",
                "worker_invocation_id": "worker-1",
            },
            "starting_point": {"commit": self.starting_commit},
            "final_tip": {
                "branch": self.repository.branch,
                "commit": self.repository.head,
            },
        }
        write_json(self.dependency_map_path, dependency_map)

    def preflight(self, *required: Path) -> dict[str, object]:
        return preflight_handoff(
            task_card_path=self.task_card_path,
            invocation_path=self.invocation_path,
            result_path=self.result_path,
            dependency_map_path=self.dependency_map_path,
            worktree=self.worktree,
            evidence_root=self.workspace,
            required_evidence=required or (Path("proof.json"),),
        )

    def test_passes_a_clean_identity_bound_handoff(self) -> None:
        result = self.preflight()
        self.assertEqual(PASS, result["disposition"])
        self.assertEqual([], result["failed_predicates"])

    def test_malformed_or_identity_mismatched_result_is_report_only(self) -> None:
        _ = self.result_path.write_text("{", encoding="utf-8")
        malformed = self.preflight()
        self.assertEqual(REPORT_ONLY_ERROR, malformed["disposition"])
        self.assertIn("JSON_OBJECT", _failed_predicates(malformed))
        self._write_handoff()
        value = cast(
            dict[str, object],
            json.loads(self.result_path.read_text(encoding="utf-8")),
        )
        value["worker_invocation_id"] = "wrong-worker"
        write_json(self.result_path, value)
        mismatch = self.preflight()
        self.assertEqual(REPORT_ONLY_ERROR, mismatch["disposition"])
        self.assertIn("HANDOFF_IDENTITY", _failed_predicates(mismatch))

    def test_dirty_worktree_or_missing_required_evidence_is_incomplete(self) -> None:
        _ = (self.worktree / "tracked.txt").write_text("dirty\n", encoding="utf-8")
        dirty = self.preflight()
        self.assertEqual(INCOMPLETE, dirty["disposition"])
        self.assertIn("RESULT_ENVELOPE", _failed_predicates(dirty))
        _ = self.repository.git("checkout", "--", "tracked.txt")
        missing = self.preflight(Path("missing-proof.json"))
        self.assertEqual(INCOMPLETE, missing["disposition"])
        self.assertIn("REQUIRED_EVIDENCE_EXISTS", _failed_predicates(missing))

    def test_committed_whitespace_error_is_incomplete(self) -> None:
        _ = (self.worktree / "tracked.txt").write_text(
            "bad trailing space \n", encoding="utf-8"
        )
        _ = self.repository.git("add", "tracked.txt")
        _ = self.repository.git("commit", "-m", "introduce whitespace error")
        self._write_handoff()
        result = self.preflight()
        self.assertEqual(INCOMPLETE, result["disposition"])
        self.assertIn("COMMITTED_DIFF_WHITESPACE", _failed_predicates(result))

    def test_cli_does_not_require_harness_config_and_preserves_disposition_exit(
        self,
    ) -> None:
        with patch("orchestrator_harness.cli._print_json") as emit:
            code = cli.main(
                [
                    "--config",
                    str(self.root / "does-not-exist.json"),
                    "handoff-preflight",
                    "--task-card",
                    str(self.task_card_path),
                    "--invocation",
                    str(self.invocation_path),
                    "--result",
                    str(self.result_path),
                    "--dependency-map",
                    str(self.dependency_map_path),
                    "--worktree",
                    str(self.worktree),
                    "--evidence-root",
                    str(self.workspace),
                    "--required-evidence",
                    "proof.json",
                ]
            )
        self.assertEqual(0, code)
        self.assertEqual(PASS, emit.call_args.args[0]["disposition"])

        self.required_evidence.unlink()
        with patch("orchestrator_harness.cli._print_json"):
            incomplete_code = cli.main(
                [
                    "handoff-preflight",
                    "--task-card",
                    str(self.task_card_path),
                    "--invocation",
                    str(self.invocation_path),
                    "--result",
                    str(self.result_path),
                    "--dependency-map",
                    str(self.dependency_map_path),
                    "--worktree",
                    str(self.worktree),
                    "--evidence-root",
                    str(self.workspace),
                    "--required-evidence",
                    "proof.json",
                ]
            )
        self.assertEqual(EXIT_INCOMPLETE, incomplete_code)

        _ = self.required_evidence.write_text("{}\n", encoding="utf-8")
        _ = self.result_path.write_text("{", encoding="utf-8")
        with patch("orchestrator_harness.cli._print_json"):
            report_code = cli.main(
                [
                    "handoff-preflight",
                    "--task-card",
                    str(self.task_card_path),
                    "--invocation",
                    str(self.invocation_path),
                    "--result",
                    str(self.result_path),
                    "--dependency-map",
                    str(self.dependency_map_path),
                    "--worktree",
                    str(self.worktree),
                    "--evidence-root",
                    str(self.workspace),
                    "--required-evidence",
                    "proof.json",
                ]
            )
        self.assertEqual(EXIT_REPORT_ONLY_ERROR, report_code)


if __name__ == "__main__":
    _ = unittest.main()
