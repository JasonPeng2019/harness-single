"""Exercise collector ownership with harmless real processes, not an AI run."""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import collect_codex_usage as collector
from orchestrator_harness import processes


class CollectorProcessBoundaryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.prompt = self.root / "prompt.txt"
        self.prompt.write_text("owned fixture prompt", encoding="utf-8")

    def args(self, run_id):
        return argparse.Namespace(resume_task_file=None, prompt_file=str(self.prompt), cwd=str(self.root),
            task="owned boundary regression", arm="raw", run_id=run_id, results_dir=str(self.root / "results"),
            harness_runtime=None, harness_dir=None, model="fixture", reasoning_effort="fixture",
            service_tier="fixture", sandbox="read-only", budget_tokens=None, watchdog_hours=2 / 3600,
            trust_project_hooks=False, inhibit_sleep=False, interrupted_run=False)

    def launch(self, script, run_id):
        entry = self.root / (run_id + ".py")
        entry.write_text(script, encoding="utf-8")
        spawn = processes.spawn_provider
        which = collector.shutil.which
        cleanup = processes.ProcessBoundary.cleanup
        cleanup_observations = []
        def observed_cleanup(boundary, **kwargs):
            before = {"job_pids": sorted(boundary._job_process_ids() or []), "remaining": boundary._remaining(),
                      "record": boundary.record()}
            result = cleanup(boundary, **kwargs)
            after = {"record": boundary.record(), "result": result, "errors": list(boundary.errors),
                     "exit_proofs": [{**identity, "gone": processes.identity_exit_proven(**identity)}
                                     for identity in boundary.record()["processes"]]}
            cleanup_observations.append({"before": before, "after": after})
            return result
        commands = []
        def fixture_spawn(argv, **kwargs):
            commands.append(argv)
            kwargs["env"] = {**kwargs["env"], "MERGE_COLLECTOR_FIXTURE": "caf\u00e9"}
            return spawn([sys.executable, "-B", str(entry)], **kwargs)
        def fixture_which(command, *args, **kwargs):
            return sys.executable if command in ("codex", "codex.cmd") else which(command, *args, **kwargs)
        with patch.object(collector.shutil, "which", side_effect=fixture_which), \
             patch.object(collector.processes, "spawn_provider", side_effect=fixture_spawn), \
             patch.object(processes.ProcessBoundary, "cleanup", new=observed_cleanup), \
             contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result = collector.run_codex(self.args(run_id))
        self.assertEqual(1, len(commands), "collector must never relaunch ROOT")
        manifest = json.loads((self.root / "results" / run_id / "manifest.json").read_text(encoding="utf-8"))
        evidence_root = os.environ.get("MERGE_VALIDATION_RECORD")
        if evidence_root:
            from uuid import uuid4
            Path(evidence_root).with_name(f"collector-{run_id}-{uuid4().hex}.json").write_text(
                json.dumps({"manifest": manifest, "cleanup_observations": cleanup_observations}, indent=2), encoding="utf-8")
        return result, manifest

    def test_native_normal_exit_seals_exact_boundary_and_explicit_environment(self):
        result, manifest = self.launch(
            'import json, os\nprint(json.dumps({"environment": os.environ["MERGE_COLLECTOR_FIXTURE"]}), flush=True)\n', "normal")
        self.assertEqual(0, result, manifest)
        self.assertTrue(manifest["cleanup_proven"])
        root = manifest["process_boundary"]["root"]
        self.assertTrue(processes.identity_exit_proven(root["pid"], root["creation_time"]))
        events = (self.root / "results/normal/codex-events.jsonl").read_text(encoding="utf-8")
        self.assertEqual("caf\u00e9", json.loads(events)["environment"])

    def test_native_root_exit_with_descendant_holding_stdout_is_cleaned(self):
        sentinel = subprocess.Popen([sys.executable, "-B", "-c", "import time; time.sleep(30)"],
                                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        sentinel_identity = processes.process_identity(sentinel.pid)
        try:
            product = Path(__file__).resolve().parents[2]
            child_record = self.root / "descendant.json"
            script = (
                f"import sys; sys.path.insert(0, {str(product)!r})\n"
                "import subprocess, json\nfrom pathlib import Path\n"
                "from orchestrator_harness import processes\n"
                "child = subprocess.Popen([sys.executable, '-B', '-c', 'import time; time.sleep(30)'])\n"
                f"Path({str(child_record)!r}).write_text(json.dumps(processes.process_identity(child.pid)), encoding='utf-8')\n"
                "print(json.dumps({'root_exited': True}), flush=True)\n"
            )
            result, manifest = self.launch(script, "orphan")
            self.assertEqual(124, result)
            self.assertTrue(manifest["watchdog_expired"])
            self.assertTrue(manifest["cleanup_proven"], manifest)
            descendant = json.loads(child_record.read_text(encoding="utf-8"))
            self.assertIn(descendant, manifest["process_boundary"]["processes"])
            self.assertTrue(processes.identity_exit_proven(descendant["pid"], descendant["creation_time"]))
            self.assertIsNone(sentinel.poll(), "unrelated sentinel must remain alive")
            self.assertIsNotNone(sentinel_identity)
            self.assertTrue(processes.identity_matches(sentinel.pid, sentinel_identity["creation_time"]))
        finally:
            sentinel.terminate()  # Held Popen handle, never a numeric/name kill.
            sentinel.wait(timeout=10)
            if sentinel_identity is not None:
                self.assertTrue(processes.identity_exit_proven(sentinel.pid, sentinel_identity["creation_time"]))

    def test_unknown_boundary_cleanup_is_an_error(self):
        class UnknownBoundary:
            def cleanup(self, **kwargs):
                return False
        with self.assertRaisesRegex(OSError, "cleanup is unproven"):
            collector.stop_process_tree(UnknownBoundary())

    def test_transient_unknown_member_waits_for_actual_exact_exit(self):
        boundary = processes.ProcessBoundary(41, "windows-filetime:134361777213898726")
        with patch.object(boundary, "_remaining", side_effect=[None, []]) as observation:
            self.assertTrue(boundary._wait_exact_members(0.3))
        self.assertEqual(2, observation.call_count)
        with patch.object(boundary, "_remaining", return_value=None):
            self.assertFalse(boundary._wait_exact_members(0.1))


if __name__ == "__main__":
    unittest.main()
