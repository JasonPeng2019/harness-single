"""Resume must preserve the lane owner it checked before mutation."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from orchestrator_harness import resume


class ResumeIdentityGuardTests(unittest.TestCase):
    def test_missing_controller_status_path_fails_closed(self) -> None:
        lane = {"lane_id": "lane-1", "run_id": "run-1", "process": {}}
        with self.assertRaisesRegex(ValueError, "controller status path is missing"):
            resume._live_controller(lane)

    def test_newer_run_is_not_replaced_or_cleaned(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            worktree = root / "worktree"
            worktree.mkdir()
            agent = worktree / ".agent-workspace"
            agent.mkdir()
            (agent / "controller.attempts.jsonl").write_text(
                '{"run_id":"run-old","attempt":1,"provider_started":true}\n', encoding="utf-8"
            )
            lane = {
                "lane_id": "lane-1", "run_id": "run-old",
                "worktree_path": str(worktree), "lifecycle": "review_pending",
                "session": {"session_id": "session-1"},
                "provider": {"id": "codex"}, "process": {},
                "controller_status_path": str(worktree / "status.json"),
            }

            def mutate_newer(_rt: Path, _epoch: str, _lane: str, mutate: object) -> dict:
                return mutate({**lane, "run_id": "run-new"})

            with (
                patch.object(resume, "find_harness_root", return_value=root),
                patch.object(resume, "load_config", return_value=SimpleNamespace(runtime_root=root)),
                patch.object(resume, "find_active_lane", return_value=("epoch-1", lane)),
                patch.object(resume, "_has_valid_acceptance_chain", return_value=False),
                patch.object(resume, "_read_task_card", return_value={"card_id": "card-1"}),
                patch.object(resume, "_live_controller", return_value=False),
                patch.object(resume, "update_lane", side_effect=mutate_newer),
                patch.object(resume, "_clear_prior_run") as clear,
            ):
                result = resume.run_resume(lane_id="lane-1", resume_task_card="unused.json")
            self.assertFalse(result["ok"])
            self.assertEqual(resume.RESUME_LANE_WRITE_FAILED, result["code"])
            clear.assert_not_called()


if __name__ == "__main__":
    unittest.main()
