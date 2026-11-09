"""Private Windows Codex temp inheritance and native failed-start cancellation."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from orchestrator_harness import lane_cleanup, launch, processes
from orchestrator_harness.config import HarnessConfig, ResourceManifest
from orchestrator_harness.epochs import open_epoch, write_active_lanes
from orchestrator_harness.lanes import read_lane, write_lane
from orchestrator_harness.tests.support import (
    prepare_lane_fixture,
    seed_provider_history,
)


class WindowsWorkerTempTests(unittest.TestCase):
    def test_native_child_and_its_tool_inherit_private_temp_and_resume_retains_files(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            worktree = root / "worktree space Unicode-α"
            (worktree / ".agent-workspace").mkdir(parents=True)
            runtime = root / "runtime"
            lane = {
                "lane_id": "lane",
                "run_id": "initial-run",
                "worktree_path": str(worktree),
                "controller_status_path": str(
                    worktree / ".agent-workspace/controller.status.json"
                ),
            }
            invocation = {"provider": {"id": "codex", "launch_config": {}}}
            before = dict(os.environ)
            with patch.object(launch.sys, "platform", "win32"):
                options = launch._controller_spawn_options(root, lane, invocation)
            expected = worktree / ".agent-workspace/tmp/initial-run"
            self.assertTrue(expected.is_dir())
            self.assertEqual(before, dict(os.environ))
            self.assertEqual(str(expected), options["env"]["TEMP"])
            self.assertEqual(str(expected), options["env"]["TMP"])
            self.assertEqual(
                {k: v for k, v in before.items() if k.upper() not in {"TEMP", "TMP"}},
                {
                    k: v
                    for k, v in options["env"].items()
                    if k.upper() not in {"TEMP", "TMP"}
                },
            )
            tool_code = (
                "import json,tempfile; from pathlib import Path; "
                "p=Path(tempfile.gettempdir())/'tool.tmp'; "
                "p.write_text('tool'); print(json.dumps(str(p)))"
            )
            output = root / "observed.json"
            code = (
                "import json,subprocess,sys,tempfile; from pathlib import Path; "
                "controller=Path(tempfile.gettempdir())/'controller.tmp'; "
                "controller.write_text('controller'); "
                "tool=subprocess.run([sys.executable,'-B','-c',sys.argv[2]],"
                "capture_output=True,text=True,timeout=10,check=True); "
                "Path(sys.argv[1]).write_text(json.dumps({'controller':str(controller),"
                "'tool':json.loads(tool.stdout)}))"
            )
            child = launch._spawn_controller_logged(
                runtime,
                lane,
                [sys.executable, "-B", "-c", code, str(output), tool_code],
                options,
            )
            try:
                self.assertEqual(0, child.wait(timeout=20))
            finally:
                self.assertTrue(processes.reap_created_process(child))
            observed = json.loads(output.read_text())
            for role in ("controller", "tool"):
                path = Path(observed[role])
                self.assertEqual(expected.resolve(), path.parent.resolve())
                self.assertEqual(role, path.read_text())
            with patch.object(launch.sys, "platform", "win32"):
                resumed = launch._controller_spawn_options(
                    root, {**lane, "run_id": "resumed-run"}, invocation
                )
            self.assertNotEqual(options["env"]["TEMP"], resumed["env"]["TEMP"])
            self.assertTrue(Path(resumed["env"]["TEMP"]).is_dir())
            self.assertTrue(Path(observed["tool"]).is_file())
            self.assertEqual(before, dict(os.environ))

    def test_unrelated_modes_inherit_existing_environment_without_temp_directories(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw).resolve()
            lane = {"worktree_path": str(root / "worktree"), "run_id": "run"}
            for platform, provider, config in (
                ("win32", "codex", {"permission_mode": "yolo"}),
                ("win32", "claude-code", {}),
                ("win32", "qwen-code", {}),
                ("linux", "codex", {}),
                ("darwin", "codex", {}),
            ):
                with (
                    self.subTest(platform=platform, provider=provider),
                    patch.object(launch.sys, "platform", platform),
                ):
                    self.assertEqual(
                        {"cwd": str(root)},
                        launch._controller_spawn_options(
                            root,
                            lane,
                            {"provider": {"id": provider, "launch_config": config}},
                        ),
                    )
            self.assertFalse((root / "worktree").exists())


class PrivateTempFailedStartTests(unittest.TestCase):
    def test_temp_creation_failure_consumes_launch_and_preserves_cancellation_proof(
        self,
    ) -> None:
        for pending_resume in (False, True):
            with (
                self.subTest(pending_resume=pending_resume),
                tempfile.TemporaryDirectory() as raw,
            ):
                root = Path(raw).resolve()
                harness = root / "harness"
                workspace = root / "project"
                workspace.mkdir()
                binding = (
                    harness
                    / "orchestrator_harness/provider_adapters/codex/launcher_binding.py"
                )
                binding.parent.mkdir(parents=True)
                binding.write_text("# owned fixture binding\n", encoding="utf-8")
                config = HarnessConfig(harness, workspace, "enabled")
                runtime = config.runtime_root
                epoch = open_epoch(runtime, config, ResourceManifest())["epoch_id"]
                worktree = workspace / "worker"
                launch_config = {
                    "reasoning_effort": "medium",
                    "service_tier": "priority",
                    "permission_mode": "worker-isolated",
                }
                lane = prepare_lane_fixture(
                    runtime,
                    epoch,
                    {
                        "lane_id": "lane",
                        "run_id": "resumed-run" if pending_resume else "initial-run",
                        "lifecycle": "running" if pending_resume else "prepared",
                        "launch_pending": pending_resume,
                        "process": {},
                        "worktree_path": str(worktree),
                        "provider": {
                            "id": "codex",
                            "model": "fixture",
                            "launch_config": launch_config,
                        },
                        "controller_status_path": str(
                            worktree / ".agent-workspace/controller.status.json"
                        ),
                        "controller_events_path": str(
                            worktree / ".agent-workspace/controller.events.jsonl"
                        ),
                    },
                )
                seed_provider_history(runtime, lane)
                write_lane(runtime, epoch, "lane", lane)
                write_active_lanes(runtime, epoch, [{"lane_id": "lane"}])
                # A genuine filesystem collision before spawn, not a fabricated
                # controller error: mkdir cannot descend through this file.
                (worktree / ".agent-workspace/tmp").write_text(
                    "retained collision", encoding="utf-8"
                )
                with (
                    patch.object(launch, "find_harness_root", return_value=harness),
                    patch.object(launch, "load_config", return_value=config),
                    patch.object(
                        launch,
                        "_validate_provider_launch_config",
                        return_value=launch_config,
                    ),
                    patch.object(launch.sys, "platform", "win32"),
                    patch.object(launch, "_spawn_controller_logged") as spawn,
                ):
                    result = launch.run_launch("lane")
                self.assertFalse(result["ok"], result)
                self.assertEqual(launch.LAUNCH_CONTROLLER_START_FAILED, result["code"])
                spawn.assert_not_called()
                failed = read_lane(runtime, epoch, "lane")
                self.assertEqual(lane["run_id"], failed["run_id"])
                self.assertEqual("running", failed["lifecycle"])
                self.assertIs(False, failed["launch_pending"])
                self.assertEqual({}, failed["process"])
                self.assertEqual(
                    {
                        "run_id": lane["run_id"],
                        "controller_not_created": True,
                        "controller_reaped": False,
                    },
                    failed["launch_failure"],
                )
                self.assertTrue(
                    lane_cleanup.controller_not_created(
                        failed, lane_cleanup.observe_controller_status(failed)
                    )
                )
                self.assertTrue(lane_cleanup.cleanup_is_proven(failed))
                with (
                    patch.object(launch, "find_harness_root", return_value=harness),
                    patch.object(launch, "load_config", return_value=config),
                    patch.object(launch, "_prune_worktrees"),
                    patch.object(processes, "terminate_process") as terminate,
                ):
                    cancelled = launch.run_force_stop("lane")
                self.assertTrue(cancelled["ok"], cancelled)
                terminate.assert_not_called()
                retired = read_lane(runtime, epoch, "lane")
                self.assertEqual("retired", retired["lifecycle"])
                self.assertEqual(lane["run_id"], retired["run_id"])
                self.assertTrue(lane_cleanup.cleanup_is_proven(retired))


if __name__ == "__main__":
    unittest.main()
