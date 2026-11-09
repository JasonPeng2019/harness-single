from __future__ import annotations

from copy import deepcopy
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from orchestrator_harness import bootstrap, setup
from orchestrator_harness.tests.test_v2_materialization import MaterializationFixture

ROOT = Path(__file__).resolve().parents[2]
CLAUDE = ROOT / "adapters" / "claude-code"
LEGACY = {
    "PostToolUse": "python .claude/hooks/orchestrator_harness_post_tool_use.py",
    "Stop": "python .claude/hooks/orchestrator_harness_stop.py",
}


def _copy_claude_payload(harness: Path) -> None:
    for part in ("root", "super-cache"):
        shutil.copytree(
            CLAUDE / part,
            harness / "adapters" / "claude-code" / part,
            dirs_exist_ok=True,
        )
    shutil.copytree(
        ROOT / "super-cache" / "workspace" / ".agent-workspace",
        harness / "super-cache" / "workspace" / ".agent-workspace",
        dirs_exist_ok=True,
    )


class ClaudeBootstrapHookMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.source = CLAUDE / "super-cache" / ".claude" / "settings.json"
        self.shipped = json.loads(self.source.read_text(encoding="utf-8"))

    def _legacy_config(self) -> dict:
        value = deepcopy(self.shipped)
        value["env"] = {"KEEP_USER_VALUE": "same"}
        for event, command in LEGACY.items():
            value["hooks"][event][0]["hooks"][0]["command"] = command
        return value

    def test_root_setup_migrates_exact_legacy_and_deduplicates_without_touching_users(
        self,
    ) -> None:
        target = self.base / ".claude" / "settings.json"
        target.parent.mkdir()
        legacy = self._legacy_config()
        # An old-plus-new registration is a recognized exact migration cohort.
        for event in LEGACY:
            legacy["hooks"][event].append(deepcopy(self.shipped["hooks"][event][0]))
        target.write_text(json.dumps(legacy), encoding="utf-8")
        merged = setup._merge_root_hook_config(self.source, target)
        self.assertEqual(legacy["env"], merged["env"])
        for event, groups in merged["hooks"].items():
            if event in LEGACY:
                self.assertEqual([self.shipped["hooks"][event][0]], groups)
        self.assertEqual(merged, setup._merge_root_hook_config(self.source, target))
        self.assertEqual(json.dumps(legacy), target.read_text(encoding="utf-8"))

    def test_root_setup_refuses_altered_owned_group_without_mutating_target(
        self,
    ) -> None:
        target = self.base / ".claude" / "settings.json"
        target.parent.mkdir()
        legacy = self._legacy_config()
        legacy["hooks"]["Stop"][0]["matcher"] = "altered"
        target.write_text(json.dumps(legacy), encoding="utf-8")
        before = target.read_bytes()
        with self.assertRaises(setup.SetupError):
            setup._merge_root_hook_config(self.source, target)
        self.assertEqual(before, target.read_bytes())

    def test_current_settings_remain_byte_idempotent(self) -> None:
        target = self.base / ".claude" / "settings.json"
        target.parent.mkdir()
        current = deepcopy(self.shipped)
        current["env"] = {"KEEP_USER_VALUE": "same"}
        before = json.dumps(current, ensure_ascii=False).encode("utf-8")
        target.write_bytes(before)
        merged = setup._merge_root_hook_config(self.source, target)
        self.assertEqual(current, merged)
        self.assertEqual(before, target.read_bytes())

    def test_qwen_current_settings_with_shared_hook_basenames_are_preserved(
        self,
    ) -> None:
        source = ROOT / "adapters" / "qwen-code" / "root" / ".qwen" / "settings.json"
        target = self.base / ".qwen" / "settings.json"
        target.parent.mkdir()
        current = json.loads(source.read_text(encoding="utf-8"))
        current["user_value"] = {"keep": True}
        before = json.dumps(current, ensure_ascii=False).encode("utf-8")
        target.write_bytes(before)

        merged = setup._merge_root_hook_config(source, target)

        self.assertEqual(current, merged)
        self.assertEqual(before, target.read_bytes())

    def test_worker_bootstrap_migrates_exact_legacy_and_preserves_current_binding(
        self,
    ) -> None:
        for cohort in ("legacy-only", "legacy-plus-current"):
            with self.subTest(cohort=cohort):
                fixture = MaterializationFixture()
                try:
                    _copy_claude_payload(fixture.harness)
                    fixture._write_provider_to(
                        fixture.harness, "claude-code", ".claude"
                    )
                    fixture.active_cache()
                    fixture.initialize_git()
                    runtime = fixture.root_workspace / ".harness-runtime"
                    lane_id = f"claude-{cohort}"
                    worktree = runtime / "worktrees" / "epoch-1" / lane_id
                    worktree.parent.mkdir(parents=True)
                    fixture.git(
                        "worktree",
                        "add",
                        "-b",
                        f"lane/{lane_id}",
                        str(worktree),
                        "HEAD",
                    )
                    settings = worktree / ".claude" / "settings.json"
                    settings.parent.mkdir(parents=True)
                    inherited = self._legacy_config()
                    if cohort == "legacy-plus-current":
                        for event in LEGACY:
                            inherited["hooks"][event].append(
                                deepcopy(self.shipped["hooks"][event][0])
                            )
                    settings.write_text(json.dumps(inherited), encoding="utf-8")
                    (worktree / ".agent-workspace").mkdir(parents=True)
                    bootstrap._write_worker_binding(worktree, runtime, lane_id, "run-1")
                    bootstrap._install_managed_material(
                        fixture.harness,
                        runtime,
                        worktree,
                        provider_id="claude-code",
                        lane_id=lane_id,
                        run_id="run-1",
                    )
                    actual = json.loads(settings.read_text(encoding="utf-8"))
                    self.assertEqual(inherited["env"], actual["env"])
                    self.assertEqual(self.shipped["hooks"], actual["hooks"])
                    binding = json.loads(
                        (
                            worktree / ".agent-workspace" / "harness-hook-binding.json"
                        ).read_text(encoding="utf-8")
                    )
                    self.assertEqual("worker", binding["role"])
                    self.assertEqual(lane_id, binding["lane_id"])
                    self.assertEqual("run-1", binding["run_id"])
                finally:
                    fixture.close()

    def test_worker_bootstrap_refuses_missing_or_altered_groups_unchanged(self) -> None:
        for kind in ("missing", "altered"):
            with self.subTest(kind=kind):
                fixture = MaterializationFixture()
                try:
                    _copy_claude_payload(fixture.harness)
                    fixture._write_provider_to(
                        fixture.harness, "claude-code", ".claude"
                    )
                    fixture.active_cache()
                    runtime = fixture.root_workspace / ".harness-runtime"
                    worktree = runtime / "worktrees" / "epoch-1" / f"claude-{kind}"
                    settings = worktree / ".claude" / "settings.json"
                    settings.parent.mkdir(parents=True)
                    config = self._legacy_config()
                    if kind == "missing":
                        config["hooks"].pop("Stop")
                    else:
                        config["hooks"]["Stop"][0]["matcher"] = "altered"
                    before = json.dumps(config).encode()
                    settings.write_bytes(before)
                    (worktree / ".agent-workspace").mkdir(parents=True)
                    with self.assertRaises(bootstrap.BootstrapError):
                        bootstrap._install_managed_material(
                            fixture.harness,
                            runtime,
                            worktree,
                            provider_id="claude-code",
                            lane_id=f"claude-{kind}",
                            run_id="run-1",
                        )
                    self.assertEqual(before, settings.read_bytes())
                finally:
                    fixture.close()


if __name__ == "__main__":
    unittest.main()
