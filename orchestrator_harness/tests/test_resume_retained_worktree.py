"""Retained resume uses protected product authority, never operational file copies.

Native finite Python children prove cleanup here; these tests do not claim model
provider execution. All repositories/runtimes belong to shallow OS temp fixtures.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from orchestrator_harness import processes, resume, retry_policy, setup
from orchestrator_harness.config import ResourceManifest
from orchestrator_harness.epochs import close_epoch, lane_record_dir, open_epoch, read_active_lanes, read_current_epoch
from orchestrator_harness.lane_authority import read_invocation
from orchestrator_harness.lane_cleanup import cleanup_is_proven
from orchestrator_harness.lanes import read_lane, reconcile_active_lanes, write_lane
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.tests.result_fixture import LinkedResultFixture


class RetainedResumeTests(LinkedResultFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        atomic_write_json(self.base / "harness-config.json", {
            "root_workspace": str(self.root), "managed_coordination": "disabled",
        })
        atomic_write_json(self.base / "resource-manifest.json", {
            "schema": "resource-manifest/v1", "resources": [],
        })
        setup.set_runtime_state(self.rt, "OPEN")
        self.finish_native_fixture(self.lane)
        self.lane["lifecycle"] = "retired"
        write_lane(self.rt, self.epoch, "worker", self.lane)
        close_epoch(self.rt, self.epoch)
        self.card_path = self.base / "resume-card.json"
        atomic_write_json(self.card_path, {**self.task, "task": "Correct the retained work", "branch": "lane/worker/work"})

    def finish_native_fixture(self, lane):
        """Exercise real process containment/history, without a model provider."""
        if not lane.get("provider_history_path"):
            retry_policy.initialize_history(self.rt, lane)
        token = retry_policy.reserve_invocation(self.rt, lane)
        with open(os.devnull, "rb") as stdin, open(os.devnull, "wb") as out, open(os.devnull, "wb") as err:
            child = processes.spawn_provider([sys.executable, "-B", "-c", "pass"],
                                            cwd=self.worker, stdin=stdin, stdout=out, stderr=err)
            boundary = None
            try:
                job = child.take_job_handle() if os.name == "nt" else None
                boundary = processes.ProcessBoundary.for_process(child.pid, windows_job_handle=job)
                self.assertTrue(boundary.observe())
                record = boundary.record()
                retry_policy.record_started(self.rt, lane, token, process_boundary=record)
                if os.name == "nt":
                    child.resume()
                self.assertEqual(0, child.wait(timeout=10))
            finally:
                if boundary is not None:
                    self.assertTrue(boundary.cleanup(force=True, timeout_seconds=10))
                child.wait(timeout=10)
                if os.name == "nt":
                    child.close()
        retry_policy.record_outcome(self.rt, lane, token, provider_started=True,
                                   cleanup_proven=True, session_id="finite-fixture-session")
        lane["session"] = {"session_id": "finite-fixture-session"}
        lane["process"] = record["root"]
        atomic_write_json(Path(lane["controller_status_path"]), {
            "schema": "controller-status/v1", "lane_id": lane["lane_id"], "run_id": lane["run_id"],
            "controller_state": "exited", "controller_identity": record["root"],
            "provider_state": {**record["root"], "state": "exited"},
            "process_boundary": record, "cleanup_proven": True,
        })
        self.assertTrue(cleanup_is_proven(lane))

    def command(self):
        env = dict(os.environ)
        env["PYTHONPATH"] = str(Path(__file__).resolve().parents[2])
        return subprocess.run([sys.executable, "-B", "-m", "orchestrator_harness.operator_launch", "--json",
                               "resume-lane", "--lane-id", "worker", "--resume-task-card", str(self.card_path)],
                              cwd=self.base, env=env, capture_output=True, text=True, timeout=30)

    def run_resume(self):
        completed = self.command()
        result = json.loads(completed.stdout)
        self.assertEqual(0 if result["ok"] else 1, completed.returncode, completed.stderr)
        return result

    def current(self):
        epoch = read_current_epoch(self.rt)["epoch_id"]
        return epoch, read_lane(self.rt, epoch, "worker")

    def snapshot(self):
        # Lock files are bookkeeping, not source/ownership records.
        return {str(p.relative_to(self.base)): p.read_bytes() for p in self.base.rglob("*")
                if p.is_file() and not p.name.endswith(".lock")}

    def assert_refused(self, contains="", code=resume.RESUME_LANE_WRITE_FAILED):
        before = self.snapshot()
        result = self.run_resume()
        self.assertFalse(result["ok"], result)
        self.assertEqual(code, result["code"], result)
        self.assertIn(contains, result["summary"])
        self.assertEqual(before, self.snapshot())

    def test_public_resume_preserves_dirty_worktree_session_history_and_closed_records(self):
        (self.worker / "source.txt").write_bytes(b"unfinished user changes\n")
        (self.worker / "untracked.txt").write_bytes(b"retained work\n")
        self.git(self.worker, "add", "source.txt")
        history = Path(self.lane["provider_history_path"]).read_bytes()
        old_folder = lane_record_dir(self.rt, self.epoch, "worker")
        old_records = {str(p.relative_to(old_folder)): p.read_bytes() for p in old_folder.rglob("*")
                       if p.is_file() and not p.name.endswith(".lock")}
        inode = self.worker.stat().st_ino
        self.assertTrue(self.run_resume()["ok"])
        epoch, fresh = self.current()
        self.assertNotEqual(self.epoch, epoch)
        self.assertNotEqual(self.lane["run_id"], fresh["run_id"])
        for field in ("worktree_path", "branch", "git_source", "session", "provider", "provider_history_path"):
            self.assertEqual(self.lane[field], fresh[field], field)
        self.assertNotEqual(self.lane["task_card_hash"], fresh["task_card_hash"])
        self.assertNotEqual(self.lane["control_dir"], fresh["control_dir"])
        self.assertEqual(inode, self.worker.stat().st_ino)
        self.assertEqual(b"unfinished user changes\n", (self.worker / "source.txt").read_bytes())
        self.assertEqual(b"retained work\n", (self.worker / "untracked.txt").read_bytes())
        self.assertEqual(history, Path(fresh["provider_history_path"]).read_bytes())
        for name, value in old_records.items():
            self.assertEqual(value, (old_folder / name).read_bytes())
        self.assertEqual(fresh["run_id"], read_active_lanes(self.rt, epoch)[0]["run_id"])
        self.assertTrue(fresh["launch_pending"])
        self.assertTrue(cleanup_is_proven(fresh))
        self.assertEqual(fresh["run_id"], read_invocation(fresh)["run_id"])
        self.assertEqual(1, retry_policy.completed_provider_invocations(fresh, self.rt))

    def test_reuses_compatible_epoch_and_preserves_its_inventory(self):
        state = open_epoch(self.rt, self.config, ResourceManifest())
        write_lane(self.rt, state["epoch_id"], "peer", {**self.lane, "lane_id": "peer", "run_id": "peer-run", "lifecycle": "blocked"})
        reconcile_active_lanes(self.rt, state["epoch_id"])
        self.assertTrue(self.run_resume()["ok"])
        self.assertEqual(state["epoch_id"], self.current()[0])
        self.assertEqual({"peer", "worker"}, {e["lane_id"] for e in read_active_lanes(self.rt, state["epoch_id"])})

    def test_same_worktree_retry_spends_second_invocation_and_third_is_refused(self):
        self.assertTrue(self.run_resume()["ok"])
        epoch, fresh = self.current()
        self.finish_native_fixture(fresh)
        fresh["lifecycle"] = "review_pending"
        write_lane(self.rt, epoch, "worker", fresh)
        self.assertEqual(2, retry_policy.completed_provider_invocations(fresh, self.rt))
        self.assert_refused(code=retry_policy.WORKER_RETRY_LIMIT)
        with self.assertRaisesRegex(ValueError, "exhausted"):
            retry_policy.reserve_invocation(self.rt, {**fresh, "run_id": "third-run"})

    def test_accepted_closed_lane_never_resumes(self):
        self.pair()
        self.assert_refused(code=resume.ALREADY_ACCEPTED)

    def test_orphan_or_invalid_acceptance_is_preserved_and_refused(self):
        self.pair()
        (lane_record_dir(self.rt, self.epoch, "worker") / "COMPLETION_REVIEW.json").unlink()
        self.assert_refused(contains="incomplete")

    def test_ambiguous_retained_identity_is_refused(self):
        state = open_epoch(self.rt, self.config, ResourceManifest())
        write_lane(self.rt, state["epoch_id"], "worker", {**self.lane})
        close_epoch(self.rt, state["epoch_id"])
        self.assert_refused(contains="2 matching epoch records")

    def test_changed_source_branch_is_refused(self):
        self.git(self.worker, "checkout", "-qb", "another-branch")
        self.assert_refused(contains="redirect changed")

    def test_card_cannot_retarget_branch(self):
        atomic_write_json(self.card_path, {**self.task, "branch": "another-branch"})
        self.assert_refused(contains="cannot change")

    def test_closed_runtime_requires_setup(self):
        setup.set_runtime_state(self.rt, "CLOSED")
        self.assert_refused(contains="not OPEN")

    def test_configuration_mismatch_is_refused(self):
        path = self.rt / "epochs" / self.epoch / "epoch-state.json"
        state = json.loads(path.read_bytes())
        atomic_write_json(path, {**state, "config_identity": "changed"})
        self.assert_refused(contains="configuration is incompatible")

    def test_unknown_cleanup_is_refused(self):
        path = Path(self.lane["controller_status_path"])
        status = json.loads(path.read_bytes())
        atomic_write_json(path, {**status, "cleanup_proven": False})
        self.assert_refused(contains="cleanup is live or unproven")

    def test_missing_history_does_not_reset_budget(self):
        Path(self.lane["provider_history_path"]).unlink()
        self.assert_refused(code=retry_policy.WORKER_RETRY_LIMIT)

    def test_corrupt_current_inventory_is_not_replaced(self):
        state = open_epoch(self.rt, self.config, ResourceManifest())
        (self.rt / "epochs" / state["epoch_id"] / "active-lanes.json").write_bytes(b"damaged")
        self.assert_refused()

    def test_active_corrupt_lane_does_not_fall_back_to_closed_owner(self):
        state = open_epoch(self.rt, self.config, ResourceManifest())
        folder = lane_record_dir(self.rt, state["epoch_id"], "worker")
        folder.mkdir(parents=True)
        (folder / "lane.json").write_bytes(b"damaged")
        self.assert_refused()


if __name__ == "__main__":
    unittest.main()
