"""Exercise provider-aware reports from an installed sibling-package layout."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


HARNESS = Path(__file__).resolve().parents[2]


class InstalledUsageEntrypointTests(unittest.TestCase):
    def test_report_routes_discover_sibling_harness_for_deferred_worker_imports(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            install = base / "installed-harness"
            install.mkdir()
            shutil.copytree(HARNESS / "scripts", install / "scripts",
                            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
            shutil.copytree(HARNESS / "orchestrator_harness", install / "orchestrator_harness",
                            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "tests"))
            shutil.copytree(HARNESS / "harness_common", install / "harness_common",
                            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))

            root = base / "task-run"
            root.mkdir()
            runtime = base / "harness-runtime"
            launch = {
                "schema": "codex-token-run/v1", "run_id": "installed-test", "task": "fixture",
                "arm": "harness", "harness_runtime": str(runtime), "provider_id": "codex",
                "model": "fixture-model", "reasoning_effort": "high", "service_tier": "priority",
                "started_at_utc": "2026-11-09T12:00:00+00:00",
                "command": ["codex", "exec", "-m", "fixture-model", "-c",
                            'model_reasoning_effort="high"', "-c", 'service_tier="priority"'],
                "budget_tokens": 1000, "root_boundary_required": True,
            }
            (root / "launch.json").write_text(json.dumps(launch), encoding="utf-8")
            identity = {"pid": 2147483000, "creation_time": "windows-filetime:1"}
            final = {
                **launch, "ended_at_utc": "2026-11-09T12:02:00+00:00", "exit_code": 0,
                "elapsed_seconds": 120, "cleanup_proven": True,
                "process_boundary": {"kind": "windows-job", "root": identity, "processes": [identity]},
            }
            (root / "manifest.json").write_text(json.dumps(final), encoding="utf-8")
            (root / "codex-events.jsonl").write_text(
                json.dumps({"type": "thread.started", "thread_id": "root-session"}) + "\n" +
                json.dumps({"type": "turn.completed", "usage": {"input_tokens": 5,
                    "cached_input_tokens": 0, "output_tokens": 2}}) + "\n", encoding="utf-8")
            task = {"schema": "codex-token-task/v1", "run_id": "installed-test",
                    "task": "fixture", "arm": "harness", "root_run_dir": str(root),
                    "harness_runtime": str(runtime), "epoch_id": "epoch-1", "budget_tokens": 1000}
            task_file = root / "task.json"
            task_file.write_text(json.dumps(task), encoding="utf-8")

            # Include a real provider-start record so report construction traverses worker discovery.
            worktree = runtime / "worktrees" / "epoch-1" / "worker-a"
            workspace = worktree / ".agent-workspace"
            workspace.mkdir(parents=True)
            provider = {"id": "codex", "model": "worker-model", "launch_config": {
                "reasoning_effort": "high", "service_tier": "priority"}}
            (workspace / "invocation.json").write_text(json.dumps({
                "schema": "controller-invocation/v1", "lane_id": "worker-a", "run_id": "worker-run",
                "provider": provider, "paths": {},
            }), encoding="utf-8")
            (workspace / "controller.events.jsonl").write_text(json.dumps({
                "event_type": "provider_started", "run_id": "worker-run",
                "ts": "2026-11-09T12:01:00+00:00",
            }) + "\n", encoding="utf-8")
            (workspace / "controller.attempts.jsonl").write_text(json.dumps({"schema": "controller-attempts/v1"}) + "\n" + json.dumps({
                "attempt": 1, "run_id": "worker-run", "lane_id": "worker-a",
                "argv": ["codex", "exec", "-m", "worker-model", "-c", 'model_reasoning_effort="high"',
                         "-c", 'service_tier="priority"'], "provider": provider,
                "at": "2026-11-09T12:01:30+00:00", "exit_code": 0,
                "transcript_path": str(workspace / "provider-transcript.jsonl"),
            }) + "\n", encoding="utf-8")
            (workspace / "provider-transcript.jsonl").write_text(
                json.dumps({"type": "thread.started", "thread_id": "worker-session"}) + "\n" +
                json.dumps({"type": "turn.completed", "usage": {"input_tokens": 3,
                    "cached_input_tokens": 0, "output_tokens": 1}}) + "\n", encoding="utf-8")

            environment = os.environ.copy()
            environment.pop("PYTHONPATH", None)
            script = install / "scripts" / "collect_usage.py"

            def invoke(*arguments: str) -> subprocess.CompletedProcess[str]:
                return subprocess.run([sys.executable, "-B", str(script), *arguments],
                                      cwd=base, env=environment, text=True, capture_output=True, check=False)

            snapshot = invoke("snapshot", "--task-file", str(task_file))
            self.assertNotIn("ModuleNotFoundError", snapshot.stderr, snapshot.stderr)
            self.assertIn(snapshot.returncode, (0, 2), snapshot.stderr)
            report = json.loads(snapshot.stdout)
            worker_rows = [row for row in report["rows"] if row["role"] == "worker"]
            self.assertEqual(1, len(worker_rows), report)
            self.assertEqual("worker-a", worker_rows[0]["lane_id"])

            gate = invoke("gate", "--task-file", str(task_file))
            self.assertNotIn("ModuleNotFoundError", gate.stderr, gate.stderr)
            self.assertIn(gate.returncode, (0, 2), gate.stderr)
            self.assertEqual(11, json.loads(gate.stdout)["known_total_tokens"])

            archive = base / "archive"
            finalized = invoke("finalize", "--task-file", str(task_file), "--output", str(archive))
            self.assertNotIn("ModuleNotFoundError", finalized.stderr, finalized.stderr)
            self.assertIn(finalized.returncode, (0, 2), finalized.stderr)
            if finalized.returncode == 0:
                self.assertTrue((archive / "usage.json").is_file())


if __name__ == "__main__":
    unittest.main()
