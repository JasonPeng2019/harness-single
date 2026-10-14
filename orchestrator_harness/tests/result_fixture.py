"""Real disposable linked-source and protected RESULT fixture for product tests."""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from orchestrator_harness import controller, git_source, monitor, review
from orchestrator_harness.bootstrap import _write_invocation, _write_worker_binding, _write_worker_prompt
from orchestrator_harness.config import HarnessConfig, ResourceManifest
from orchestrator_harness.core import content_hash, iso_utc
from orchestrator_harness.epochs import lane_record_dir, open_epoch
from orchestrator_harness.lane_authority import stage_authority
from orchestrator_harness.lanes import write_lane
from orchestrator_harness.records import atomic_write_json


class LinkedResultFixture:
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="result-source-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.root, self.worker = self.base / "root", self.base / "worker"
        self.root.mkdir()
        self.git(self.root, "init", "-q")
        self.git(self.root, "config", "core.autocrlf", "false")
        (self.root / ".gitignore").write_text(".agent-workspace/\nRESULT.json\n.harness-runtime/\n", encoding="utf-8")
        (self.root / "source.txt").write_bytes(b"original source\n")
        self.git(self.root, "add", ".gitignore", "source.txt")
        self.commit(self.root, "base")
        self.git(self.root, "worktree", "add", "-qb", "lane/worker/work", str(self.worker))
        self.config = HarnessConfig(self.base, self.root, "disabled")
        self.rt = self.config.runtime_root
        self.epoch = open_epoch(self.rt, self.config, ResourceManifest())["epoch_id"]
        self.task = {"schema": "project-task-card/v1", "task": "Complete a linked source assignment",
                     "acceptance_criteria": ["Exact committed source is reviewed"], "deliverables": ["Source"],
                     "reason_for_acceptance_and_deliverables": "Exercise RESULT and independent review", "base_commit": "HEAD"}
        self.provider = {"id": "claude-code", "model": "fixture", "launch_config": {"effort": "high"}}
        self.lane = self.prepare({"lane_id": "worker", "run_id": "first-run", "lifecycle": "review_pending",
                                  "worktree_path": str(self.worker), "result_path": str(self.worker / "RESULT.json"),
                                  "branch": "lane/worker/work", "provider": self.provider, "exclusive_resources": [],
                                  "source_exclusions": []})
        script = Path(__file__).resolve().parents[2] / "super-cache/workspace/.agent-workspace/result-stop-check.py"
        shutil.copyfile(script, self.worker / ".agent-workspace/result-stop-check.py")
        self.publish()

    def git(self, root, *arguments, input_bytes=None):
        result = subprocess.run(["git", "-C", str(root), *arguments], input=input_bytes,
                                capture_output=True, env=git_source.environment(), timeout=20)
        self.assertEqual(0, result.returncode, result.stderr.decode("utf-8", "replace"))
        return result.stdout.decode("utf-8").strip()

    def commit(self, root, message):
        return self.git(root, "-c", "user.name=Fixture", "-c", "user.email=fixture@local", "commit", "-qm", message)

    def prepare(self, record):
        _write_worker_prompt(self.worker, self.task, managed=False)
        _write_invocation(self.worker, lane_id=record["lane_id"], run_id=record["run_id"],
                          provider_id=self.provider["id"], model=self.provider["model"],
                          launch_config=self.provider["launch_config"], exclusive_resources=[])
        _write_worker_binding(self.worker, self.rt, record["lane_id"], record["run_id"])
        return write_lane(self.rt, self.epoch, record["lane_id"], stage_authority(self.rt, self.epoch, record, self.task))

    def record(self, **changes):
        result = {"schema": "result/v1", "lane_id": self.lane["lane_id"], "run_id": self.lane["run_id"],
                  "worker_invocation_id": self.lane["run_id"], "task_card_hash": self.lane["task_card_hash"],
                  "branch": "lane/worker/work", "commit": git_source.current_tip(self.lane), "outcome": "PASS",
                  "summary": "Source and checks complete", "checks": [{"name": "fixture", "command": ["python", "-m", "unittest"], "outcome": "PASS"}],
                  "evidence": [{"stdout": "native diagnostics", "exit_code": 0}], "completed_at": iso_utc()}
        result.update(changes)
        result["content_hash"] = content_hash(result)
        return result

    def publish(self, **changes):
        result = self.record(**changes)
        atomic_write_json(self.worker / "RESULT.json", result)
        return result

    def worker_check(self):
        result = subprocess.run([sys.executable, "-I", "-B", str(self.worker / ".agent-workspace/result-stop-check.py"),
                                 "--json"], capture_output=True, text=True, timeout=30)
        self.assertIn(result.returncode, (0, 1), result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(result.returncode == 0, report["valid"], report)
        return report

    def assert_consumers(self, valid):
        state, record = controller._validate_result(self.lane)
        self.assertEqual("valid" if valid else "invalid", state)
        self.assertEqual(valid, record is not None)
        self.assertEqual(valid, monitor._valid_current_result(self.lane))
        self.assertEqual(valid, self.worker_check()["valid"])

    def pair(self, *, approval="ACCEPTED", outcome="PASS"):
        return review._write_pair(self.rt, self.epoch, self.lane, review_outcome=outcome,
                                  review_summary="independent review", evidence=["fixture inspection"],
                                  approval=approval, force_accept_reason=None)

    def replay(self, *, approval="ACCEPTED", outcome="PASS"):
        return review._replay_pair(lane_record_dir(self.rt, self.epoch, "worker"), self.lane,
                                  review_outcome=outcome, review_summary="independent review",
                                  evidence=["fixture inspection"], approval=approval, force_accept_reason=None)
