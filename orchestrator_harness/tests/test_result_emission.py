"""Executable contract for frozen-compatible result publication (no provider required)."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


HELPER = Path(__file__).resolve().parents[1] / "result_emit.py"


class ResultPublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.repo = self.root / "repo with spaces"
        self.repo.mkdir()
        self.git("init", "-b", "test-lane")
        self.git("config", "user.name", "Result Test")
        self.git("config", "user.email", "test@example.invalid")
        (self.repo / ".gitignore").write_text(".agent-workspace/\n")
        self.git("add", ".gitignore")
        self.git("commit", "-m", "fixture")
        self.head = self.git("rev-parse", "HEAD")
        self.workspace = self.repo / ".agent-workspace"
        self.workspace.mkdir()
        self.result = self.workspace / "RESULT.json"
        self.context = self.root / "invocation.json"
        self.identity = {
            "schema": "orchestrator-coding-invocation/v1",
            "run_root": str(self.repo), "lane_id": "test-lane",
            "worker_invocation_id": "test-worker",
            "repository": {"worktree_root": str(self.repo),
                           "common_dir": str(self.repo / ".git"),
                           "branch": "test-lane", "base_commit": self.head},
        }
        self.context.write_text(json.dumps(self.identity))
        self.facts = self.workspace / "facts.json"
        self.payload = {"outcome": "FAIL", "summary": "A real check failed.",
                        "checks": [{"name": "example", "outcome": "FAIL"}]}
        self.facts.write_text(json.dumps(self.payload), encoding="utf-8-sig")

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.repo), *args],
                                       stderr=subprocess.STDOUT, text=True).strip()

    def run_helper(self, *extra):
        return subprocess.run([sys.executable, str(HELPER), "--context", str(self.context),
                               "--facts", str(self.facts), *extra], cwd=self.root,
                              capture_output=True, text=True)

    def test_derives_identity_and_preserves_failed_and_blocked_outcomes(self):
        for outcome in ("FAIL", "BLOCKED"):
            self.payload["outcome"] = outcome
            self.facts.write_text(json.dumps(self.payload))
            run = self.run_helper()
            self.assertEqual(0, run.returncode, run.stderr)
            result = json.loads(self.result.read_text())
            self.assertEqual(outcome, result["outcome"])
            self.assertEqual(self.payload["checks"], result["checks"])
            self.assertEqual(self.head, result["commit"])
            self.assertEqual("test-worker", result["worker_invocation_id"])
            self.assertEqual("VALID", json.loads(run.stdout)["state"])

    def test_bad_check_outcome_cannot_overwrite_existing_result(self):
        self.result.write_text("retained previous evidence")
        self.payload["checks"][0]["outcome"] = "BLOCKED"
        self.facts.write_text(json.dumps(self.payload))
        run = self.run_helper()
        self.assertNotEqual(0, run.returncode)
        self.assertIn("PASS, FAIL, SKIP, or NOT_RUN", run.stderr)
        self.assertEqual("retained previous evidence", self.result.read_text())

    def test_worker_cannot_supply_identity_or_silently_drop_extra_fields(self):
        for key in ("worker_invocation_id", "findings"):
            self.facts.write_text(json.dumps({**self.payload, key: "wrong"}))
            run = self.run_helper()
            self.assertNotEqual(0, run.returncode)
            self.assertIn(key, run.stderr)
            self.assertFalse(self.result.exists())

    def test_dirty_worktree_refused_without_committing_or_cleaning(self):
        path = self.repo / "uncommitted.txt"
        path.write_text("preserve me")
        run = self.run_helper()
        self.assertNotEqual(0, run.returncode)
        self.assertIn("clean project worktree", run.stderr)
        self.assertEqual("preserve me", path.read_text())
        self.assertFalse(self.result.exists())

    def test_preflight_does_not_publish(self):
        run = self.run_helper("--check-only")
        self.assertEqual(0, run.returncode, run.stderr)
        self.assertFalse(self.result.exists())

    def test_wrong_branch_context_refused(self):
        self.identity["repository"]["branch"] = "another-lane"
        self.context.write_text(json.dumps(self.identity))
        run = self.run_helper()
        self.assertNotEqual(0, run.returncode)
        self.assertIn("branch", run.stderr)
        self.assertFalse(self.result.exists())

    def test_uses_current_tip_after_authorized_commit(self):
        self.git("commit", "--allow-empty", "-m", "authorized work")
        run = self.run_helper()
        self.assertEqual(0, run.returncode, run.stderr)
        self.assertEqual(self.git("rev-parse", "HEAD"), json.loads(self.result.read_text())["commit"])

    def test_external_context_must_match_frozen_receipt(self):
        receipt = self.root / "receipt.json"
        receipt.write_text(json.dumps({"schema": "orchestrator-operator-launch/v1",
                                       "status": "launched", "label": "test-lane",
                                       "cwd": str(self.repo)}))
        self.identity.update(schema="orchestrator-lane-result-context/v1", receipt_path=str(receipt))
        self.context.write_text(json.dumps(self.identity))
        run = self.run_helper()
        self.assertEqual(0, run.returncode, run.stderr)
        receipt.write_text(json.dumps({"schema": "orchestrator-operator-launch/v1",
                                       "status": "launched", "label": "another-lane",
                                       "cwd": str(self.repo)}))
        prior = self.result.read_bytes()
        run = self.run_helper()
        self.assertNotEqual(0, run.returncode)
        self.assertIn("receipt", run.stderr)
        self.assertEqual(prior, self.result.read_bytes())


if __name__ == "__main__":
    unittest.main()
