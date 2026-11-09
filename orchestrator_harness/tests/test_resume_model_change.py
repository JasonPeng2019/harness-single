"""Explicit resume profile changes create protected fresh-session boundaries."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from orchestrator_harness import operator_launch, resume
from orchestrator_harness.bootstrap import BootstrapError, normalize_fresh_launch_config
from orchestrator_harness.config import HarnessConfig
from orchestrator_harness.epochs import lane_record_dir
from orchestrator_harness.lanes import read_lane, write_lane
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness import retry_policy, setup
from orchestrator_harness.tests.result_fixture import LinkedResultFixture


class ResumeModelChangeTests(unittest.TestCase):
    def test_cli_accepts_model_and_repeated_complete_profile_options(self):
        args = operator_launch._build_parser().parse_args(
            [
                "resume-lane",
                "--lane-id",
                "lane-1",
                "--resume-task-card",
                "card.json",
                "--model",
                "new-model",
                "--provider-option",
                "reasoning_effort=xhigh",
                "--provider-option",
                "service_tier=priority",
                "--rationale",
                "continue retained work",
            ]
        )
        self.assertEqual("new-model", args.model)
        self.assertEqual(
            {"reasoning_effort": "xhigh", "service_tier": "priority"},
            operator_launch._provider_options(args.provider_option),
        )

    def test_incomplete_profile_refuses_before_any_mutation(self):
        lane = {
            "lane_id": "lane-1",
            "run_id": "old-run",
            "lifecycle": "review_pending",
            "worktree_path": "unused",
            "provider": {"id": "codex"},
            "process": {},
        }
        for model, options in ((None, {"reasoning_effort": "high"}),):
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                with (
                    patch.object(resume, "find_harness_root", return_value=root),
                    patch.object(
                        resume,
                        "load_config",
                        return_value=SimpleNamespace(runtime_root=root),
                    ),
                    patch.object(
                        resume, "find_active_lane", return_value=("epoch", lane)
                    ),
                    patch.object(resume, "update_lane") as mutation,
                    patch.object(
                        resume, "_validate_provider_launch_config"
                    ) as validate,
                ):
                    result = resume.run_resume(
                        lane_id="lane-1",
                        resume_task_card="card.json",
                        model=model,
                        launch_config=options,
                    )
            self.assertFalse(result["ok"])
            self.assertEqual(resume.PROFILE_CHANGE_INVALID, result["code"])
            mutation.assert_not_called()
            validate.assert_not_called()

    def test_model_only_uses_adapter_catalog_defaults_and_required_options(self):
        harness_root = Path(__file__).resolve().parents[2]
        self.assertEqual({}, normalize_fresh_launch_config("qwen-code", {}))
        from orchestrator_harness.bootstrap import _validate_provider_launch_config

        self.assertEqual(
            {},
            _validate_provider_launch_config(
                harness_root,
                provider_id="qwen-code",
                model="qwen-test-model",
                launch_config={},
            ),
        )
        with self.assertRaisesRegex(BootstrapError, "missing"):
            _validate_provider_launch_config(
                harness_root,
                provider_id="codex",
                model="codex-test-model",
                launch_config={},
            )

    def test_fresh_windows_codex_default_is_worker_workspace(self):
        import os

        config = normalize_fresh_launch_config(
            "codex", {"reasoning_effort": "high", "service_tier": "default"}
        )
        if os.name == "nt":
            self.assertEqual("worker-workspace", config["permission_mode"])
        else:
            self.assertNotIn("permission_mode", config)

    def test_invalid_complete_profile_refuses_before_lane_or_worktree_mutation(self):
        lane = {
            "lane_id": "lane-1",
            "run_id": "old-run",
            "lifecycle": "review_pending",
            "worktree_path": "unused",
            "provider": {"id": "codex"},
            "process": {},
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (
                patch.object(resume, "find_harness_root", return_value=root),
                patch.object(
                    resume,
                    "load_config",
                    return_value=SimpleNamespace(runtime_root=root),
                ),
                patch.object(resume, "find_active_lane", return_value=("epoch", lane)),
                patch.object(
                    resume,
                    "_validate_provider_launch_config",
                    side_effect=ValueError("missing service_tier"),
                ),
                patch.object(resume, "update_lane") as mutation,
            ):
                result = resume.run_resume(
                    lane_id="lane-1",
                    resume_task_card="card.json",
                    model="new-model",
                    launch_config={"reasoning_effort": "high"},
                )
        self.assertFalse(result["ok"])
        self.assertEqual(resume.PROFILE_CHANGE_INVALID, result["code"])
        self.assertIn("missing service_tier", result["summary"])
        mutation.assert_not_called()

    def test_codex_permission_migration_regenerates_only_for_worker_profiles(self):
        config = SimpleNamespace(
            root_workspace=Path("product"), manager_workspace=Path("manager")
        )
        old = {
            "id": "codex",
            "model": "old",
            "launch_config": {"permission_mode": "worker-isolated"},
        }
        for new_mode, should_regenerate in (
            ("worker-workspace", True),
            ("worker-isolated", False),
            ("yolo", False),
        ):
            new = {
                "id": "codex",
                "model": "new",
                "launch_config": {"permission_mode": new_mode},
            }
            with patch(
                "orchestrator_harness.bootstrap.regenerate_codex_worker_profile",
                return_value=Path("worker/.codex/config.toml"),
            ) as regenerate:
                result = resume._regenerate_codex_profile_if_needed(
                    Path("worker"), Path("harness"), config, old, new
                )
            self.assertEqual(should_regenerate, regenerate.called)
            if should_regenerate:
                regenerate.assert_called_once_with(
                    Path("worker"),
                    Path("harness"),
                    Path("product"),
                    permission_mode="worker-workspace",
                    manager_workspace=Path("manager"),
                )
                self.assertEqual(Path("worker/.codex/config.toml"), result)
            else:
                self.assertIsNone(result)

        yolo = {
            "id": "codex",
            "model": "old",
            "launch_config": {"permission_mode": "yolo"},
        }
        workspace = {
            "id": "codex",
            "model": "new",
            "launch_config": {"permission_mode": "worker-workspace"},
        }
        with patch(
            "orchestrator_harness.bootstrap.regenerate_codex_worker_profile"
        ) as regenerate:
            resume._regenerate_codex_profile_if_needed(
                Path("worker"), Path("harness"), config, yolo, workspace
            )
        regenerate.assert_called_once()

    def test_historical_omitted_codex_mode_matches_explicit_strict_mode(self):
        omitted = {
            "id": "codex",
            "model": "same",
            "launch_config": {"reasoning_effort": "high"},
        }
        explicit = {
            "id": "codex",
            "model": "same",
            "launch_config": {
                "reasoning_effort": "high",
                "permission_mode": "worker-isolated",
            },
        }
        self.assertEqual(
            resume._effective_profile(omitted), resume._effective_profile(explicit)
        )

    def test_windows_resume_keeps_saved_permission_mode_when_override_omits_it(self):
        for saved, expected in (
            ({"reasoning_effort": "high"}, "worker-isolated"),
            ({"permission_mode": "worker-workspace"}, "worker-workspace"),
            ({"permission_mode": "yolo"}, "yolo"),
        ):
            with self.subTest(saved=saved):
                configured = resume._resume_launch_config(
                    "codex",
                    saved,
                    {"reasoning_effort": "xhigh", "service_tier": "priority"},
                )
                self.assertEqual(expected, configured["permission_mode"])
                self.assertEqual("xhigh", configured["reasoning_effort"])
                self.assertEqual("priority", configured["service_tier"])
                self.assertNotIn("launcher", configured)

        explicit = resume._resume_launch_config(
            "codex",
            {"permission_mode": "yolo"},
            {
                "reasoning_effort": "xhigh",
                "service_tier": "priority",
                "permission_mode": "worker-workspace",
            },
        )
        self.assertEqual("worker-workspace", explicit["permission_mode"])
        self.assertEqual(
            {"reasoning_effort", "service_tier", "permission_mode"}, set(explicit)
        )

    def test_archive_keeps_exact_prior_identity_and_dirty_product_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            worktree = root / "w"
            agent = worktree / ".agent-workspace"
            agent.mkdir(parents=True)
            records = root / "epochs" / "epoch-1" / "lanes" / "lane-1"
            records.mkdir(parents=True)
            invocation = agent / "invocation.json"
            invocation.write_bytes(b'{"protected":"original invocation bytes"}\n')
            dirty = worktree / "unfinished-product.txt"
            dirty.write_bytes(b"uncommitted product work\x00must survive\n")
            lane = {
                "lane_id": "lane-1",
                "run_id": "old-run",
                "worktree_path": str(worktree),
                "invocation_path": str(invocation),
                "provider": {
                    "id": "codex",
                    "model": "old",
                    "launch_config": {"service_tier": "default"},
                },
                "session": {"session_id": "old-session"},
                "lifecycle": "result_invalid",
            }
            atomic_write_json(records / "lane.json", lane)

            class Output:
                def read_bytes(self, path: Path) -> bytes:
                    return Path(path).read_bytes()

            history = resume._archive_profile_transition(
                root,
                "epoch-1",
                lane,
                new_run_id="new-run",
                new_provider={
                    "id": "codex",
                    "model": "new",
                    "launch_config": {"service_tier": "priority"},
                },
                rationale="continue the dirty product work with an explicitly selected model",
                output=Output(),
            )
            self.assertEqual(
                dirty.read_bytes(), b"uncommitted product work\x00must survive\n"
            )
            self.assertEqual(
                invocation.read_bytes(), (history / "invocation.json").read_bytes()
            )
            transition = json.loads(
                (history / "transition.json").read_text(encoding="utf-8")
            )
            self.assertEqual("old-session", transition["previous_session_id"])
            self.assertEqual("old", transition["old_provider"]["model"])
            self.assertEqual("new", transition["new_provider"]["model"])
            self.assertEqual("new-run", transition["to_run_id"])
            self.assertIn("explicitly selected model", transition["rationale"])


class ResumeModelChangeIntegrationTests(LinkedResultFixture, unittest.TestCase):
    def resume_with_replacement(
        self,
        managed: bool,
        *,
        interrupt_once: bool = False,
        interrupt_before_mark: bool = False,
    ):
        self.config = HarnessConfig(
            self.base, self.root, "enabled" if managed else "disabled"
        )
        setup.set_runtime_state(self.rt, "OPEN")
        retry_policy.initialize_history(self.rt, self.lane)
        token = retry_policy.reserve_invocation(self.rt, self.lane)
        retry_policy.record_outcome(
            self.rt,
            self.lane,
            token,
            provider_started=True,
            cleanup_proven=True,
            session_id="old-provider-session",
        )
        self.lane["session"] = {"session_id": "old-provider-session"}
        write_lane(self.rt, self.epoch, "worker", self.lane)
        (self.worker / "unfinished-product-work.txt").write_bytes(
            b"dirty product work survives\n"
        )
        old_invocation = Path(self.lane["invocation_path"]).read_bytes()
        card_path = self.base / "replacement-card.json"
        card = {**self.task, "task": "Continue the retained assignment"}
        atomic_write_json(card_path, card)
        with (
            patch.object(
                resume,
                "find_harness_root",
                return_value=Path(__file__).resolve().parents[2],
            ),
            patch.object(resume, "load_config", return_value=self.config),
            patch.object(resume, "_live_controller", return_value=False),
            patch("orchestrator_harness.public_checks.install_route") as install_route,
        ):
            if interrupt_once:
                with patch.object(
                    resume,
                    "_write_worker_prompt",
                    side_effect=OSError("interrupted profile publication"),
                ):
                    interrupted = resume.run_resume(
                        lane_id="worker",
                        resume_task_card=str(card_path),
                        rationale="continue dirty product work with the explicitly selected replacement model",
                        model="replacement-model",
                        launch_config={"effort": "high"},
                    )
                self.assertFalse(interrupted["ok"])
                self.assertEqual(
                    "resuming", read_lane(self.rt, self.epoch, "worker")["lifecycle"]
                )
            if interrupt_before_mark:
                update = resume.update_lane
                calls = 0

                def fail_before_first_mutation(*args, **kwargs):
                    nonlocal calls
                    calls += 1
                    if calls == 1:
                        raise OSError("interrupted after protected profile intent")
                    return update(*args, **kwargs)

                with patch.object(
                    resume, "update_lane", side_effect=fail_before_first_mutation
                ):
                    interrupted = resume.run_resume(
                        lane_id="worker",
                        resume_task_card=str(card_path),
                        rationale="continue dirty product work with the explicitly selected replacement model",
                        model="replacement-model",
                        launch_config={"effort": "high"},
                    )
                self.assertFalse(interrupted["ok"])
                self.assertEqual(
                    "review_pending",
                    read_lane(self.rt, self.epoch, "worker")["lifecycle"],
                )
            result = resume.run_resume(
                lane_id="worker",
                resume_task_card=str(card_path),
                rationale="continue dirty product work with the explicitly selected replacement model",
                model="replacement-model",
                launch_config={"effort": "high"},
            )
        self.assertTrue(result["ok"], result)
        current = read_lane(self.rt, self.epoch, "worker")
        self.assertNotEqual("first-run", current["run_id"])
        self.assertEqual("replacement-model", current["provider"]["model"])
        self.assertEqual({}, current["session"])
        self.assertTrue(current["launch_pending"])
        self.assertEqual(
            old_invocation,
            (
                lane_record_dir(self.rt, self.epoch, "worker")
                / "resume-history"
                / "first-run"
                / "invocation.json"
            ).read_bytes(),
        )
        self.assertEqual(
            b"dirty product work survives\n",
            (self.worker / "unfinished-product-work.txt").read_bytes(),
        )
        self.assertEqual(
            1, retry_policy.completed_provider_invocations(current, self.rt)
        )
        transition = json.loads(
            (
                lane_record_dir(self.rt, self.epoch, "worker")
                / "resume-history"
                / "first-run"
                / "transition.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual("old-provider-session", transition["previous_session_id"])
        self.assertEqual(current["run_id"], transition["to_run_id"])
        self.assertEqual(managed, install_route.called)
        if managed:
            inbox = json.loads(
                (self.worker / ".agent-workspace" / "QUEUE.json").read_text(
                    encoding="utf-8"
                )
            )
            binding = json.loads(
                (
                    self.worker / ".agent-workspace" / "harness-hook-binding.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(current["run_id"], inbox["run_id"])
            self.assertEqual(current["run_id"], binding["run_id"])

    def test_plain_explicit_change_preserves_work_and_starts_without_saved_session(
        self,
    ):
        self.resume_with_replacement(managed=False)

    def test_managed_explicit_change_refreshes_inbox_and_binding(self):
        self.resume_with_replacement(managed=True)

    def test_interrupted_profile_publication_recovers_from_immutable_archive(self):
        self.resume_with_replacement(managed=False, interrupt_once=True)

    def test_intent_before_lane_mutation_reuses_archived_run_identity(self):
        self.resume_with_replacement(managed=False, interrupt_before_mark=True)


if __name__ == "__main__":
    unittest.main()
