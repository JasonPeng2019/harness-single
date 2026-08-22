from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from orchestrator_harness.codex_adapter import check_codex_adapter
from orchestrator_harness.lane_bootstrap import bootstrap_coding_lane, cleanup_coding_lane
from orchestrator_harness.lane_controller import load_invocation
from orchestrator_harness.tests.support import TemporaryGitRepository
from orchestrator_harness.workspace_overlay import verify_overlay_receipt


class CodingLaneBootstrapTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source_root = self.root / "source"
        self.source = TemporaryGitRepository.create(self.source_root, branch="main")
        self.experiment_root = self.root / "fresh-experiments" / "active-run"
        self.runtime_root = self.root / "runtime" / "bootstrap-01"
        self.super_cache = self.root / "super-cache"
        (self.super_cache / ".codex").mkdir(parents=True)
        (self.super_cache / ".codex" / "config.toml").write_text(
            "[features]\nhooks = true\n", encoding="utf-8"
        )
        (self.super_cache / ".codex" / "hooks.json").write_text(
            json.dumps(
                {
                    "hooks": {
                        "SessionStart": [{"hooks": [{"type": "command", "command": "session"}]}],
                        "PreToolUse": [{"hooks": [{"type": "command", "command": "pre"}]}],
                        "Stop": [{"hooks": [{"type": "command", "command": "stop"}]}],
                    }
                }
            ),
            encoding="utf-8",
        )
        self.mapping_path = self.root / "mapping.json"
        self.mapping_path.write_text(
            json.dumps(
                {
                    "schema": "firmware-role-agent-mapping/v1",
                    "roles": {
                        "ROLE_EXECUTOR": {
                            "provider": "codex",
                            "model": "gpt-5.6-luna",
                            "reasoning_effort": "xhigh",
                            "service_tier": "default",
                            "config_overrides": [
                                "model_auto_compact_token_limit=150000"
                            ],
                        },
                    },
                    "lane_slots": {"LANE-EXEC-01": {"role": "ROLE_EXECUTOR"}},
                }
            ),
            encoding="utf-8",
        )
        self.manifest_path = self.root / "dispatch" / "bootstrap.json"
        self.manifest_path.parent.mkdir(parents=True)
        self.manifest_path.write_text(
            json.dumps(
                {
                    "schema": "orchestrator-coding-lane-bootstrap/v1",
                    "experiment_root": str(self.experiment_root),
                    "source_repository_root": str(self.source_root),
                    "base_commit": self.source.head,
                    "branch": "lane/a21-source-v13",
                    "mapping": str(self.mapping_path),
                    "workflow_role": "ROLE_EXECUTOR",
                    "lane_id": "LANE-EXEC-01",
                    "worktree_name": "a21-source-lane-02",
                    "runtime_root": str(self.runtime_root),
                    "worker_invocation_id": "a21-source-v13-001",
                    "task": "Prepare the A21 target-local responder seam.",
                    "phase": "MI-NORMAL-A21-SPEC",
                    "prompt": "Perform only the declared A21 source/build unit.\n",
                    "task_card_name": "STEP-012-source-v13.md",
                    "task_card_markdown": "# STEP-012 source/build\n",
                    "resource_manifest_name": "STEP-012-source-v13.resources.json",
                    "resource_manifest": {
                        "schema": "firmware-campaign-resource-claims/v1",
                        "board_tokens": [],
                    },
                    "overlay_cache": str(self.super_cache),
                    "launch_options": {
                        "command": ["codex"],
                        "sandbox": "danger-full-access",
                        "approval_policy": "never",
                    },
                    "exclusive_resources": [],
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_bootstrap_creates_a_verified_controller_lane_without_root_files(self) -> None:
        result = bootstrap_coding_lane(self.manifest_path)

        worktree = self.experiment_root / "worktrees" / "a21-source-lane-02"
        invocation_path = worktree / ".agent-workspace" / "invocation.json"
        receipt_path = worktree / ".agent-workspace" / "overlay-receipt.json"
        result_template_path = worktree / ".agent-workspace" / "RESULT_TEMPLATE.json"
        worker_prompt_path = worktree / ".agent-workspace" / "worker-prompt.md"
        self.assertEqual(str(worktree.resolve()), result["worktree_root"])
        self.assertTrue((worktree / ".git").exists())
        self.assertEqual(
            "[features]\nhooks = true\n",
            (worktree / ".codex" / "config.toml").read_text(encoding="utf-8"),
        )
        self.assertTrue(
            (self.experiment_root / "dispatch" / "STEP-012-source-v13.md").is_file()
        )
        self.assertTrue(invocation_path.is_file())
        self.assertIn(
            "Create the terminal JSON only at `.agent-workspace/RESULT.json`.",
            worker_prompt_path.read_text(encoding="utf-8"),
        )
        self.assertIn(
            "Terminal outcomes: `PASS`, `FAIL`, or `BLOCKED`.",
            worker_prompt_path.read_text(encoding="utf-8"),
        )
        self.assertIn(
            "Check outcomes: `PASS`, `FAIL`, `SKIP`, or `NOT_RUN`.",
            worker_prompt_path.read_text(encoding="utf-8"),
        )
        self.assertEqual(
            {
                "schema": "orchestrator-lane-result/v1",
                "lane_id": "LANE-EXEC-01",
                "worker_invocation_id": "a21-source-v13-001",
                "branch": "lane/a21-source-v13",
                "commit": self.source.head,
                "outcome": "BLOCKED",
                "summary": "Replace this template with the truthful terminal lane result.",
                "checks": [
                    {
                        "name": "replace with a completed verification",
                        "command": "replace with the exact command or observation",
                        "outcome": "NOT_RUN",
                        "summary": "replace with the truthful result summary",
                    }
                ],
            },
            json.loads(result_template_path.read_text(encoding="utf-8")),
        )
        status = subprocess.run(
            [
                "git",
                "-C",
                str(worktree),
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
                "--",
                ".",
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertEqual("", status.stdout)

        invocation = load_invocation(invocation_path)
        self.assertEqual("LANE-EXEC-01", invocation.lane_id)
        self.assertEqual(worktree.resolve(), invocation.run_root)
        self.assertEqual("lane/a21-source-v13", invocation.repository.branch)
        self.assertTrue(
            verify_overlay_receipt(
                receipt_path=receipt_path,
                expected_target_worktree_id=worktree,
                role="subagent",
            )["verified"]
        )

    def test_bootstrap_rejects_an_existing_lane_target(self) -> None:
        target = self.experiment_root / "worktrees" / "a21-source-lane-02"
        target.mkdir(parents=True)

        with self.assertRaisesRegex(ValueError, "must not already exist"):
            bootstrap_coding_lane(self.manifest_path)

    def test_bootstrap_installs_and_binds_declared_codex_event_delivery(self) -> None:
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        queue_root = self.runtime_root / "manager-event-queue"
        manifest["event_delivery"] = {
            "schema": "orchestrator-codex-event-delivery/v1",
            "queue_root": str(queue_root),
            "run_id": "campaign-epoch-01",
            "queue_id": "campaign-queue-01",
            "manager_session_id": "manager-session-01",
            "manager_thread_id": "manager-thread-01",
            "registration_id": "manager-registration-01",
            "manager_invocation_id": "manager-invocation-01",
            "registration_generation": 1,
        }
        self.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = bootstrap_coding_lane(self.manifest_path)

        worktree = Path(result["worktree_root"])
        self.assertTrue(check_codex_adapter(worktree)["current"])
        binding = json.loads(
            (worktree / ".codex" / "orchestrator-harness-binding.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(str(queue_root.resolve()), binding["queue_root"])
        self.assertEqual("campaign-epoch-01", binding["run_id"])
        self.assertEqual("manager-registration-01", binding["registration_id"])
        self.assertEqual("campaign-epoch-01", result["event_delivery"]["run_id"])
        self.assertEqual(str(queue_root.resolve()), result["event_delivery"]["queue_root"])
        hooks = json.loads((worktree / ".codex" / "hooks.json").read_text(encoding="utf-8"))
        self.assertIn("SessionStart", hooks["hooks"])
        self.assertIn("PreToolUse", hooks["hooks"])
        self.assertIn(
            {
                "matcher": ".*",
                "hooks": [
                    {
                        "command": "python .codex/hooks/orchestrator_harness_post_tool_use.py",
                        "type": "command",
                    }
                ],
            },
            hooks["hooks"]["PostToolUse"],
        )
        self.assertIn(
            {
                "hooks": [
                    {
                        "command": "python .codex/hooks/orchestrator_harness_stop.py",
                        "type": "command",
                    }
                ],
            },
            hooks["hooks"]["Stop"],
        )

    def test_executor_bootstrap_rejects_a_missing_super_cache_before_worktree_creation(self) -> None:
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        manifest.pop("overlay_cache")
        self.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "ROLE_EXECUTOR requires overlay_cache"):
            bootstrap_coding_lane(self.manifest_path)

        self.assertFalse(
            (self.experiment_root / "worktrees" / "a21-source-lane-02").exists()
        )

    def test_executor_bootstrap_rejects_a_hook_disabled_super_cache_before_worktree_creation(self) -> None:
        (self.super_cache / ".codex" / "config.toml").write_text(
            "[features]\nhooks = false\n", encoding="utf-8"
        )

        with self.assertRaisesRegex(ValueError, "super-cache must enable native Codex hooks"):
            bootstrap_coding_lane(self.manifest_path)

        self.assertFalse(
            (self.experiment_root / "worktrees" / "a21-source-lane-02").exists()
        )

    def test_executor_bootstrap_rejects_a_super_cache_missing_a_native_hook_before_worktree_creation(
        self,
    ) -> None:
        hooks_path = self.super_cache / ".codex" / "hooks.json"
        hooks = json.loads(hooks_path.read_text(encoding="utf-8"))
        hooks["hooks"].pop("PreToolUse")
        hooks_path.write_text(json.dumps(hooks), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "native PreToolUse hook"):
            bootstrap_coding_lane(self.manifest_path)

        self.assertFalse(
            (self.experiment_root / "worktrees" / "a21-source-lane-02").exists()
        )

    def test_bootstrap_rejects_malformed_event_delivery_before_worktree_creation(self) -> None:
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        manifest["event_delivery"] = []
        self.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "event_delivery must be an object"):
            bootstrap_coding_lane(self.manifest_path)

        self.assertFalse(
            (self.experiment_root / "worktrees" / "a21-source-lane-02").exists()
        )

    def test_cleanup_removes_only_a_clean_bootstrapped_worktree(self) -> None:
        result = bootstrap_coding_lane(self.manifest_path)
        worktree = Path(result["worktree_root"])

        cleanup = cleanup_coding_lane(self.manifest_path)

        self.assertEqual("removed", cleanup["status"])
        self.assertEqual(str(worktree), cleanup["worktree_root"])
        self.assertFalse(worktree.exists())

    def test_bootstrap_snapshots_an_explicit_untracked_source_tree(self) -> None:
        allowed_root = self.root / "declared-candidate"
        candidate = allowed_root / "Firmware" / "app"
        (candidate / "src").mkdir(parents=True)
        (candidate / "build").mkdir()
        (candidate / "CMakeLists.txt").write_text("project(relay)\n", encoding="utf-8")
        (candidate / "src" / "main.c").write_text("int main(void) {}\n", encoding="utf-8")
        (candidate / "build" / "stale.elf").write_text("stale\n", encoding="utf-8")
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        mapping = json.loads(self.mapping_path.read_text(encoding="utf-8"))
        mapping["roles"]["ROLE_CHECKER"] = mapping["roles"]["ROLE_EXECUTOR"]
        self.mapping_path.write_text(json.dumps(mapping), encoding="utf-8")
        manifest["workflow_role"] = "ROLE_CHECKER"
        manifest["lane_id"] = "LANE-CHECK-01"
        manifest["branch"] = "lane/a25-readiness-v01"
        manifest["phase"] = "MI-NORMAL-A25-READINESS"
        manifest["source_snapshot"] = {
            "source_root": str(candidate),
            "allowed_root": str(allowed_root),
            "exclude_paths": ["build"],
        }
        self.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = bootstrap_coding_lane(self.manifest_path)

        snapshot = (
            self.experiment_root
            / "worktrees"
            / "a21-source-lane-02"
            / ".agent-workspace"
            / "source-snapshot"
        )
        self.assertEqual("source-snapshot/v1", result["source_snapshot"]["schema"])
        self.assertEqual(2, result["source_snapshot"]["file_count"])
        self.assertTrue((snapshot / "CMakeLists.txt").is_file())
        self.assertEqual("int main(void) {}\n", (snapshot / "src" / "main.c").read_text(encoding="utf-8"))
        self.assertFalse((snapshot / "build").exists())
        self.assertTrue((candidate / "build" / "stale.elf").is_file())
        status = subprocess.run(
            ["git", "-C", str(snapshot.parents[1]), "status", "--porcelain=v1"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertEqual("", status.stdout)

    def test_bootstrap_rejects_a_snapshot_outside_board_free_readiness(self) -> None:
        candidate = self.source_root / "Firmware" / "app"
        candidate.mkdir(parents=True)
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        manifest["source_snapshot"] = {"source_root": str(candidate)}
        self.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        with self.assertRaisesRegex(
            ValueError, "limited to ROLE_CHECKER readiness lanes"
        ):
            bootstrap_coding_lane(self.manifest_path)

        self.assertFalse(
            (self.experiment_root / "worktrees" / "a21-source-lane-02").exists()
        )

    def test_bootstrap_resolves_a_non_executor_role_from_the_canonical_mapping(self) -> None:
        mapping_path = self.root / "mapping.json"
        mapping_path.write_text(
            json.dumps(
                {
                    "schema": "firmware-role-agent-mapping/v1",
                    "roles": {
                        "ROLE_SPEC_AUTHOR": {
                            "provider": "codex",
                            "model": "gpt-5.6-terra",
                            "reasoning_effort": "xhigh",
                            "service_tier": "default",
                            "config_overrides": [
                                "model_auto_compact_token_limit=225000"
                            ],
                        },
                    },
                    "lane_slots": {},
                }
            ),
            encoding="utf-8",
        )
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        manifest["mapping"] = str(mapping_path)
        manifest["workflow_role"] = "ROLE_SPEC_AUTHOR"
        manifest["lane_id"] = "LANE-SPEC-01"
        manifest["worktree_name"] = "a21-spec-lane-01"
        manifest["branch"] = "lane/a21-spec-v13"
        manifest["worker_invocation_id"] = "a21-spec-v13-001"
        self.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = bootstrap_coding_lane(self.manifest_path)

        invocation = load_invocation(Path(result["invocation"]))
        self.assertEqual("ROLE_SPEC_AUTHOR", result["workflow_role"])
        self.assertEqual("gpt-5.6-terra", invocation.model)
        self.assertEqual("xhigh", invocation.reasoning_effort)


if __name__ == "__main__":
    unittest.main()
