"""Shared RESULT checks against real linked Git and a fresh staged worker."""
from __future__ import annotations

import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import unittest
from unittest.mock import patch
import zlib

from orchestrator_harness import controller, git_source, launch, monitor, resume, review
from orchestrator_harness.core import content_hash
from orchestrator_harness.epochs import lane_record_dir
from orchestrator_harness.lane_authority import read_invocation
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.result_validation import read_result, validate_record
from orchestrator_harness.tests.result_fixture import LinkedResultFixture


class ResultValidationTests(LinkedResultFixture, unittest.TestCase):
    def test_valid_result_is_accepted_by_root_monitor_and_fresh_staged_worker(self):
        self.assert_consumers(True)
        self.assertEqual(self.lane["result_assignment_hash"], read_invocation(self.lane)["result_assignment_hash"])
        template = json.loads((self.worker / ".agent-workspace/result-template.json").read_text())
        self.assertEqual(self.lane["task_card_hash"], template["task_card_hash"])
        self.assertEqual(self.lane["run_id"], template["worker_invocation_id"])

    def test_embedded_worker_checker_uses_staged_modules_without_replacing_operator_imports(self):
        from orchestrator_harness import result_validation as operator_validation
        path = self.worker / ".agent-workspace/result-stop-check.py"
        spec = importlib.util.spec_from_file_location("fixture_worker_checker", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        valid, reason, result = module.check_result(path.parent)
        self.assertTrue(valid, reason)
        self.assertEqual(self.lane["run_id"], result["run_id"])
        self.assertIs(operator_validation, sys.modules["orchestrator_harness.result_validation"])
        self.assertFalse(any(name.startswith("worker_result_validation_") for name in sys.modules))

    def test_full_contract_rejects_wrong_identity_revision_and_diagnostics(self):
        changes = [dict(schema="other/v1"), dict(lane_id="other"), dict(run_id="older"),
                   dict(worker_invocation_id="older"), dict(task_card_hash="wrong"), dict(branch="main"),
                   dict(commit=self.lane["git_source"]["base_commit"][:12]), dict(summary=" "),
                   dict(completed_at="2026-10-11T00:00:00"), dict(checks={}), dict(evidence=[""]),
                   dict(checks=[{"name": "test", "command": [], "outcome": "PASS"}]),
                   dict(checks=[{"name": "test", "command": "test", "outcome": "FAIL"}]),
                   dict(checks=[{"name": "test", "command": "test", "outcome": "SKIPPED"}])]
        for change in changes:
            with self.subTest(change=change):
                self.publish(**change)
                self.assert_consumers(False)
        self.publish()
        raw = json.loads((self.worker / "RESULT.json").read_text())
        raw["summary"] = "changed without updating hash"
        atomic_write_json(self.worker / "RESULT.json", raw)
        self.assert_consumers(False)

    def test_resume_rebinds_assignment_and_old_result_cannot_be_consumed(self):
        prior = copy.deepcopy(self.lane)
        self.lane = self.prepare({**self.lane, "run_id": "second-run"})
        self.assertNotEqual(prior["result_assignment_hash"], self.lane["result_assignment_hash"])
        self.assertEqual(prior["git_source"], self.lane["git_source"])
        self.assert_consumers(False)
        self.publish()
        self.assert_consumers(True)

    def test_dirty_tracked_index_and_untracked_source_are_rejected(self):
        (self.worker / "source.txt").write_bytes(b"uncommitted\n")
        self.assert_consumers(False)
        self.git(self.worker, "add", "source.txt")
        self.assert_consumers(False)
        self.git(self.worker, "reset", "--hard", "-q", "HEAD")
        (self.worker / "new-source.py").write_text("untracked\n")
        self.assert_consumers(False)

    def test_git_index_flags_cannot_hide_actual_changed_bytes(self):
        for flag in ("assume-unchanged", "skip-worktree"):
            with self.subTest(flag=flag):
                self.git(self.worker, "update-index", "--" + flag, "source.txt")
                (self.worker / "source.txt").write_bytes(b"hidden change\n")
                self.assertEqual("", self.git(self.worker, "diff", "--name-only"))
                self.assert_consumers(False)
                (self.worker / "source.txt").write_bytes(b"original source\n")
                self.git(self.worker, "update-index", "--no-" + flag, "source.txt")

    def test_owned_bookkeeping_is_excluded_without_excluding_project_files(self):
        (self.worker / ".agent-workspace/local-diagnostic.txt").write_text("diagnostic\n")
        self.assert_consumers(True)
        (self.worker / "project-diagnostic.txt").write_text("project change\n")
        self.assert_consumers(False)

    def test_dirty_failure_and_blocked_diagnostics_remain_valid(self):
        (self.worker / "source.txt").write_bytes(b"incomplete\n")
        for outcome in ("FAIL", "BLOCKED"):
            with self.subTest(outcome=outcome):
                self.publish(outcome=outcome, checks=[{"name": "test", "command": "test", "outcome": outcome}])
                self.assert_consumers(True)
                with self.assertRaises(ValueError):
                    read_result(self.lane, clean=True)
        self.pair(approval="REJECTED", outcome="BLOCKED")
        self.assertTrue(monitor._review_pair_is_valid(self.rt, self.epoch, self.lane))
        with self.assertRaises(review.ReviewError):
            self.pair(approval="ACCEPTED", outcome="BLOCKED")

    def test_valid_chain_replays_and_authorizes_retirement(self):
        pair = self.pair()
        self.assertEqual(pair, self.replay())
        self.assertTrue(monitor._review_pair_is_valid(self.rt, self.epoch, self.lane))
        launch._validate_current_acceptance(self.rt, self.epoch, self.lane, pair[1])
        self.assertTrue(resume._has_valid_acceptance_chain(self.rt, self.epoch, self.lane))

    def test_rejected_decision_does_not_become_already_accepted_on_resume(self):
        self.publish(outcome="FAIL")
        pair = self.pair(approval="REJECTED", outcome="FAIL")
        self.assertTrue(review.validate_acceptance_chain(*pair, lane_id=self.lane["lane_id"], run_id=self.lane["run_id"]))
        self.assertFalse(resume._has_valid_acceptance_chain(self.rt, self.epoch, self.lane))

    def test_resume_preserves_invalid_current_decision_instead_of_claiming_accepted(self):
        self.pair()
        folder = lane_record_dir(self.rt, self.epoch, self.lane["lane_id"])
        acceptance_path = folder / "ORCHESTRATOR_ACCEPTANCE.json"
        acceptance_path.write_text("{damaged", encoding="utf-8")
        before = acceptance_path.read_bytes()
        with patch.object(resume, "find_harness_root", return_value=self.base), \
             patch.object(resume, "load_config", return_value=self.config), \
             patch.object(resume, "find_active_lane", return_value=(self.epoch, self.lane)), \
             patch.object(resume, "_clear_prior_run") as clear:
            result = resume.run_resume(lane_id=self.lane["lane_id"], resume_task_card=str(self.lane["task_card_path"]))
        self.assertEqual(resume.RESUME_LANE_WRITE_FAILED, result["code"], result)
        self.assertIn("reconciliation", result["summary"])
        clear.assert_not_called()
        self.assertEqual(before, acceptance_path.read_bytes())

    def test_resume_checks_current_accepted_source_using_retirement_validation(self):
        self.pair()
        (self.worker / "source.txt").write_bytes(b"changed accepted source\n")
        with self.assertRaisesRegex(ValueError, "accepted source is no longer valid"):
            resume._has_valid_acceptance_chain(self.rt, self.epoch, self.lane)

    def test_incomplete_or_foreign_decision_is_reconciliation_not_absence(self):
        pair = self.pair()
        folder = lane_record_dir(self.rt, self.epoch, self.lane["lane_id"])
        acceptance_path = folder / "ORCHESTRATOR_ACCEPTANCE.json"
        acceptance_path.unlink()
        with self.assertRaisesRegex(ValueError, "incomplete"):
            resume._has_valid_acceptance_chain(self.rt, self.epoch, self.lane)
        atomic_write_json(acceptance_path, pair[1])
        wrong = dict(pair[1], lane_id="foreign")
        wrong["content_hash"] = content_hash(wrong)
        atomic_write_json(acceptance_path, wrong)
        with self.assertRaisesRegex(ValueError, "invalid or stale"):
            resume._has_valid_acceptance_chain(self.rt, self.epoch, self.lane)

    def test_dirty_source_invalidates_replay_and_retirement_without_rewriting_pair(self):
        pair = self.pair()
        folder = lane_record_dir(self.rt, self.epoch, "worker")
        before = {path.name: path.read_bytes() for path in (folder / "COMPLETION_REVIEW.json", folder / "ORCHESTRATOR_ACCEPTANCE.json")}
        (self.worker / "source.txt").write_bytes(b"later dirty source\n")
        with self.assertRaises(review.ReviewError):
            self.replay()
        with self.assertRaises(launch.LaunchError):
            launch._validate_current_acceptance(self.rt, self.epoch, self.lane, pair[1])
        self.assertFalse(monitor._review_pair_is_valid(self.rt, self.epoch, self.lane))
        for name, value in before.items():
            self.assertEqual(value, (folder / name).read_bytes())

    def test_changed_result_or_tip_cannot_reuse_an_existing_decision(self):
        pair = self.pair()
        self.publish(summary="different completed diagnostics")
        with self.assertRaises(review.ReviewError):
            self.replay()
        with self.assertRaises(launch.LaunchError):
            launch._validate_current_acceptance(self.rt, self.epoch, self.lane, pair[1])
        (self.worker / "source.txt").write_bytes(b"later committed source\n")
        self.git(self.worker, "add", "source.txt")
        self.commit(self.worker, "later")
        self.assert_consumers(False)
        self.publish()
        self.assert_consumers(True)
        with self.assertRaises(review.ReviewError):
            self.replay()

    def test_worker_mirror_rewrite_cannot_change_root_authority(self):
        mirror_path = self.worker / ".agent-workspace/result-assignment.json"
        mirror = json.loads(mirror_path.read_text())
        mirror["task_card_hash"] = "worker-selected-task"
        mirror["content_hash"] = content_hash(mirror)
        atomic_write_json(mirror_path, mirror)
        self.publish(task_card_hash=mirror["task_card_hash"])
        self.assertEqual("invalid", controller._validate_result(self.lane)[0])
        self.assertFalse(monitor._valid_current_result(self.lane))
        with self.assertRaises(review.ReviewError):
            self.pair()

    def test_source_changed_while_waiting_for_review_lock_is_not_published(self):
        actual_lock = review.RecordLock
        class SourceChangesAtLock:
            def __init__(inner, path):
                inner.lock = actual_lock(path)
            def __enter__(inner):
                inner.lock.__enter__()
                (self.worker / "source.txt").write_bytes(b"changed before publication\n")
                return inner
            def __exit__(inner, *arguments):
                return inner.lock.__exit__(*arguments)
        with patch.object(review, "RecordLock", SourceChangesAtLock):
            with self.assertRaises(review.ReviewError):
                self.pair()
        folder = lane_record_dir(self.rt, self.epoch, "worker")
        self.assertFalse((folder / "COMPLETION_REVIEW.json").exists())
        self.assertFalse((folder / "ORCHESTRATOR_ACCEPTANCE.json").exists())

    def test_malformed_or_oversized_result_is_a_diagnostic_not_an_exception(self):
        for raw in (b"{broken", b"[]", b" " * 4_000_001):
            with self.subTest(size=len(raw)):
                (self.worker / "RESULT.json").write_bytes(raw)
                self.assert_consumers(False)

    def test_corrupt_shared_blob_is_rejected_even_when_working_bytes_are_unchanged(self):
        oid = self.git(self.worker, "rev-parse", "HEAD:source.txt")
        path = Path(self.lane["git_source"]["common_dir"]) / "objects" / oid[:2] / oid[2:]
        original = path.read_bytes()
        mode = path.stat().st_mode
        try:
            path.chmod(mode | 0o200)
            path.write_bytes(zlib.compress(b"blob 15\0altered source\n"))
            self.assertEqual(b"original source\n", (self.worker / "source.txt").read_bytes())
            self.assert_consumers(False)
        finally:
            path.write_bytes(original)
            path.chmod(mode)

    def test_corrupt_tree_and_commit_objects_cannot_supply_source_evidence(self):
        for kind, revision in (("tree", "HEAD^{tree}"), ("commit", "HEAD")):
            with self.subTest(kind=kind):
                oid = self.git(self.worker, "rev-parse", revision)
                path = Path(self.lane["git_source"]["common_dir"]) / "objects" / oid[:2] / oid[2:]
                original, mode = path.read_bytes(), path.stat().st_mode
                unpacked = zlib.decompress(original)
                try:
                    path.chmod(mode | 0o200)
                    changed = unpacked[:-1] + bytes([unpacked[-1] ^ 1])
                    path.write_bytes(zlib.compress(changed))
                    self.assert_consumers(False)
                finally:
                    path.write_bytes(original)
                    path.chmod(mode)

    def test_linked_index_cannot_redirect_to_a_foreign_file(self):
        index = Path(self.lane["git_source"]["private_dir"]) / "index"
        foreign = self.base / "foreign-index"
        shutil.copyfile(index, foreign)
        before = foreign.read_bytes()
        index.unlink()
        # A hardlink needs no elevated symlink privilege on Windows. The
        # no-follow index reader must refuse this foreign alias as well.
        os.link(foreign, index)
        self.assert_consumers(False)
        self.assertEqual(before, foreign.read_bytes())

    def test_root_attributes_change_invalidates_pinned_source(self):
        path = Path(self.lane["git_source"]["common_dir"]) / "info/attributes"
        path.write_text("* -text\n", encoding="utf-8")
        self.assert_consumers(False)

    def test_crlf_normalization_uses_committed_attributes(self):
        (self.worker / ".gitattributes").write_bytes(b"source.txt text eol=crlf\n")
        self.git(self.worker, "add", ".gitattributes")
        self.commit(self.worker, "attributes")
        (self.worker / "source.txt").write_bytes(b"original source\r\n")
        self.publish()
        self.assert_consumers(True)
        (self.worker / ".gitattributes").write_bytes(b"source.txt -text\n")
        self.assert_consumers(False)

    def test_external_filter_is_never_executed_even_when_named_set(self):
        marker = self.base / "filter-ran"
        command = f'"{sys.executable}" -c "from pathlib import Path; Path({str(marker)!r}).write_text(\'ran\')"'
        # Configure before capture: ROOT config is immutable after assignment.
        self.git(self.root, "config", "filter.set.clean", command)
        self.git(self.root, "config", "filter.set.required", "true")
        self.lane["git_source"] = git_source.capture(self.root, self.worker, branch="lane/worker/work")
        (self.worker / ".gitattributes").write_bytes(b"source.txt filter=set text\n")
        self.git(self.worker, "-c", "filter.set.clean=", "-c", "filter.set.required=false", "add", ".gitattributes")
        self.commit(self.worker, "filter attributes")
        result = self.record()
        (self.worker / "source.txt").write_bytes(b"original source\r\n")
        validate_record(result, self.lane)
        self.assertFalse(marker.exists(), "validation executed a project-defined external filter")

    def test_filter_name_with_equals_cannot_escape_command_disabling(self):
        marker, helper = self.base / "filter-ran", self.base / "clean.py"
        helper.write_text("import sys\nfrom pathlib import Path\nPath(" + repr(str(marker)) + ").write_text('ran')\nsys.stdout.buffer.write(b'original source\\n')\n")
        self.git(self.root, "config", "filter.a=b.clean", f'"{sys.executable}" "{helper}"')
        self.git(self.root, "config", "filter.a=b.required", "true")
        (self.worker / ".gitattributes").write_text("source.txt filter=a=b text\n")
        self.git(self.worker, "add", ".gitattributes")
        self.commit(self.worker, "filter attributes")
        self.lane["git_source"] = git_source.capture(self.root, self.worker, branch="lane/worker/work")
        # Ordinary Git setup may run the configured filter. Start observation
        # after setup, isolating execution attributable to the validator.
        if marker.exists():
            marker.unlink()
        result = self.record()
        (self.worker / "source.txt").write_bytes(b"original source\r\n")
        validate_record(result, self.lane)
        self.assertFalse(marker.exists(), "validation executed an external filter with '=' in its name")

    def test_sha256_commit_and_blob_format_is_supported(self):
        root, worker = self.base / "sha256-root", self.base / "sha256-worker"
        root.mkdir()
        self.git(root, "init", "--object-format=sha256", "-q")
        (root / "source.txt").write_bytes(b"sha256 source\n")
        self.git(root, "add", "source.txt")
        self.commit(root, "sha256 base")
        self.git(root, "worktree", "add", "-qb", "lane/hash/work", str(worker))
        source = git_source.capture(root, worker, branch="lane/hash/work")
        lane = {**self.lane, "worktree_path": str(worker), "git_source": source,
                "worktree_identity": [worker.stat().st_dev, worker.stat().st_ino]}
        result = self.record(branch="lane/hash/work", commit=source["base_commit"])
        self.assertEqual(64, len(result["commit"]))
        validate_record(result, lane)


if __name__ == "__main__":
    unittest.main()
