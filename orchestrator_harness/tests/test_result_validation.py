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

from orchestrator_harness import bootstrap, controller, git_source, launch, monitor, resume, review, retry_policy, setup
from orchestrator_harness.core import content_hash
from orchestrator_harness.epochs import lane_record_dir
from orchestrator_harness.lane_authority import read_invocation, stage_authority
from orchestrator_harness.lanes import read_lane, write_lane
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.result_validation import read_result, validate_record
from orchestrator_harness.tests.result_fixture import LinkedResultFixture


class ResultValidationTests(LinkedResultFixture, unittest.TestCase):
    def _stage_recaptured_filter_source(self, run_id):
        source = git_source.capture(self.root, self.worker, branch="lane/worker/work")
        bootstrap._write_worker_prompt(self.worker, self.task, managed=False)
        bootstrap._write_invocation(
            self.worker, lane_id="worker", run_id=run_id,
            provider_id=self.provider["id"], model=self.provider["model"],
            launch_config=self.provider["launch_config"], exclusive_resources=[],
        )
        bootstrap._write_worker_binding(self.worker, self.rt, "worker", run_id)
        self.lane = stage_authority(
            self.rt, self.epoch,
            {**self.lane, "run_id": run_id, "git_source": source},
            self.task,
        )

    def test_sparse_pass_uses_protected_scope_and_rejects_tampered_materialization(self):
        selected = "src café"
        selected_file = self.worker / selected / "chosen file.py"
        excluded_file = self.worker / "scope_excluded" / "hidden file.py"
        selected_file.parent.mkdir(parents=True)
        excluded_file.parent.mkdir(parents=True)
        selected_file.write_bytes("print('selected ✓')\n".encode("utf-8"))
        excluded_file.write_bytes(b"must remain omitted\n")
        self.git(self.worker, "add", "src café", "scope_excluded")
        self.commit(self.worker, "add sparse fixture sources")
        initial_base = self.lane["git_source"]["base_commit"]

        self.git(self.worker, "sparse-checkout", "init", "--cone", "--no-sparse-index")
        self.git(self.worker, "sparse-checkout", "set", selected)
        self.assertTrue(selected_file.is_file())
        self.assertFalse(excluded_file.exists())

        source = git_source.capture(self.root, self.worker, branch="lane/worker/work")
        source["base_commit"] = initial_base
        source["content_hash"] = content_hash(source)
        scope = [selected]
        run_id = "sparse-pass"
        bootstrap._write_invocation(
            self.worker, lane_id="worker", run_id=run_id,
            provider_id=self.provider["id"], model=self.provider["model"],
            launch_config=self.provider["launch_config"], exclusive_resources=[],
            checkout_paths=scope,
        )
        record = {**self.lane, "run_id": run_id, "git_source": source, "checkout_paths": scope}
        self.lane = write_lane(self.rt, self.epoch, "worker",
                               stage_authority(self.rt, self.epoch, record, self.task))
        bootstrap._write_worker_binding(self.worker, self.rt, "worker", run_id)

        # Ordinary retry and an explicit model transition both carry the same
        # protected sparse scope into their new invocation and assignment.
        retry_policy.initialize_history(self.rt, self.lane)
        reservation = retry_policy.reserve_invocation(self.rt, self.lane)
        retry_policy.record_started(self.rt, self.lane, reservation)
        retry_policy.record_outcome(
            self.rt, self.lane, reservation, provider_started=True,
            cleanup_proven=True, session_id="fixture-saved-session",
        )
        setup.set_runtime_state(self.rt, "OPEN")
        self.lane["session"] = {"session_id": "fixture-saved-session"}
        write_lane(self.rt, self.epoch, "worker", self.lane)
        with patch.object(resume, "find_harness_root", return_value=self.base), \
             patch.object(resume, "load_config", return_value=self.config), \
             patch.object(resume, "find_active_lane", return_value=(self.epoch, self.lane)), \
             patch.object(resume, "_live_controller", return_value=False):
            resumed = resume.run_resume(
                lane_id="worker", resume_task_card=str(self.lane["task_card_path"])
            )
        self.assertTrue(resumed["ok"], resumed)
        self.lane = read_lane(self.rt, self.epoch, "worker")
        bootstrap._write_worker_binding(self.worker, self.rt, "worker", self.lane["run_id"])
        self.assertEqual(scope, self.lane["checkout_paths"])
        self.assertEqual(scope, read_invocation(self.lane)["checkout_paths"])

        with patch.object(resume, "find_harness_root", return_value=self.base), \
             patch.object(resume, "load_config", return_value=self.config), \
             patch.object(resume, "find_active_lane", return_value=(self.epoch, self.lane)), \
             patch.object(resume, "_live_controller", return_value=False), \
             patch.object(resume, "_validate_provider_launch_config", side_effect=lambda *args, **kwargs: kwargs["launch_config"]):
            changed = resume.run_resume(
                lane_id="worker", resume_task_card=str(self.lane["task_card_path"]),
                model="fixture-next", launch_config={"effort": "high"},
                rationale="verify sparse scope survives explicit model change",
            )
        self.assertTrue(changed["ok"], changed)
        self.lane = read_lane(self.rt, self.epoch, "worker")
        bootstrap._write_worker_binding(self.worker, self.rt, "worker", self.lane["run_id"])
        self.assertEqual(scope, self.lane["checkout_paths"])
        self.assertEqual(scope, read_invocation(self.lane)["checkout_paths"])
        self.assertEqual(scope, json.loads((self.worker / ".agent-workspace/result-assignment.json").read_text())["checkout_paths"])

        self.publish()
        stop_report = self.worker_check()
        self.assertTrue(stop_report["valid"], stop_report)
        status, result = controller._validate_result(self.lane)
        self.assertEqual("valid", status)
        self.assertIsNotNone(result)
        self.assertEqual(scope, read_invocation(self.lane)["checkout_paths"])
        acceptance_pair = self.pair()
        self.assertTrue(monitor._review_pair_is_valid(self.rt, self.epoch, self.lane))
        launch._validate_current_acceptance(self.rt, self.epoch, self.lane, acceptance_pair[1])
        self.assertTrue(resume._has_valid_acceptance_chain(self.rt, self.epoch, self.lane))

        def root_accepts(expected):
            status, result = controller._validate_result(self.lane)
            self.assertEqual("valid" if expected else "invalid", status)
            self.assertEqual(expected, result is not None)

        self.lane["checkout_paths"] = ["scope_excluded"]
        root_accepts(False)
        self.lane["checkout_paths"] = scope
        root_accepts(True)

        # Clearing the protected omission bit cannot make a missing excluded
        # file part of the accepted materialization.
        self.git(self.worker, "update-index", "--no-skip-worktree", "scope_excluded/hidden file.py")
        root_accepts(False)
        self.git(self.worker, "update-index", "--skip-worktree", "scope_excluded/hidden file.py")
        root_accepts(True)

        # Skip-worktree is not an authorization to ignore a file that exists.
        # Identical materialization remains clean; changed bytes and a linked
        # substitution are rejected even while the omission bit remains set.
        excluded_file.parent.mkdir(parents=True)
        excluded_file.write_bytes(b"must remain omitted\n")
        root_accepts(True)
        excluded_file.write_bytes(b"changed behind skip-worktree\n")
        root_accepts(False)
        excluded_file.write_bytes(b"must remain omitted\n")
        root_accepts(True)
        excluded_file.unlink()
        os.link(selected_file, excluded_file)
        root_accepts(False)
        excluded_file.unlink()
        excluded_file.write_bytes(b"must remain omitted\n")
        root_accepts(True)

        hidden_oid = self.git(self.worker, "rev-parse", "HEAD:scope_excluded/hidden file.py")
        hidden_object = Path(self.lane["git_source"]["common_dir"]) / "objects" / hidden_oid[:2] / hidden_oid[2:]
        original_object, object_mode = hidden_object.read_bytes(), hidden_object.stat().st_mode
        try:
            hidden_object.chmod(object_mode | 0o200)
            hidden_object.write_bytes(zlib.compress(b"blob 19\ncorrupt hidden bytes\n"))
            root_accepts(False)
        finally:
            hidden_object.write_bytes(original_object)
            hidden_object.chmod(object_mode)

        self.git(self.worker, "update-index", "--skip-worktree", "src café/chosen file.py")
        root_accepts(False)
        self.git(self.worker, "update-index", "--no-skip-worktree", "src café/chosen file.py")
        selected_file.unlink()
        root_accepts(False)
        with self.assertRaises(launch.LaunchError):
            launch._validate_current_acceptance(self.rt, self.epoch, self.lane, acceptance_pair[1])
        with self.assertRaisesRegex(ValueError, "accepted source is no longer valid"):
            resume._has_valid_acceptance_chain(self.rt, self.epoch, self.lane)

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

    def test_committed_generated_lane_file_is_rejected_until_restored(self):
        # A product-owned .codex/.gitignore leaves generated files visible to
        # 'git add -A'; the shared contract still refuses to accept them.
        self.lane = self.prepare({**self.lane, "run_id": "second-run", "source_exclusions": [".codex/config.toml"]})
        generated = self.worker / ".codex/config.toml"
        generated.parent.mkdir()
        generated.write_text("generated\n")
        self.publish()
        self.assert_consumers(True)
        (self.worker / "source.txt").write_text("worker change\n")
        self.git(self.worker, "add", "-A")
        self.commit(self.worker, "ordinary worker habit")
        self.publish()
        self.assert_consumers(False)
        self.assertIn(".codex/config.toml", self.worker_check()["reason"])
        base = self.lane["git_source"]["base_commit"]
        self.git(self.worker, "reset", "-q", base, "--", ".codex/config.toml")
        self.commit(self.worker, "restore generated file")
        self.publish()
        # The tip is clean, but lane history would still carry the file.
        self.assert_consumers(False)
        self.assertIn("in its history", self.worker_check()["reason"])
        self.git(self.worker, "reset", "-q", "--soft", base)
        self.commit(self.worker, "rebuilt without generated files")
        self.publish()
        self.assert_consumers(True)
        self.assertTrue(generated.is_file())

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

    @unittest.skipUnless(os.name == "nt", "only Windows volume serial width differs between Python versions")
    def test_staged_checker_accepts_identities_recorded_by_another_windows_python(self):
        # A worker may run its Stop check under PATH Python: >=3.12 reports the
        # 64-bit volume serial where the operator's earlier Python reported its low 32 bits.
        def other_python(identity):
            device, file_id = identity
            return [device & 0xFFFFFFFF if device > 0xFFFFFFFF else device | (0x5A5A << 32), file_id]

        mirror_path = self.worker / ".agent-workspace/result-assignment.json"
        original = json.loads(mirror_path.read_text())
        for changed_file_id in (False, True):
            mirror = copy.deepcopy(original)
            source = mirror["git_source"]
            for name in ("root_identity", "worktree_identity", "common_identity", "objects_identity", "private_identity"):
                source[name] = other_python(source[name])
            source["content_hash"] = content_hash(source)
            mirror["worktree_identity"] = other_python(mirror["worktree_identity"])
            mirror["worktree_identity"][1] += changed_file_id
            mirror["content_hash"] = content_hash(mirror)
            atomic_write_json(mirror_path, mirror)
            with self.subTest(changed_file_id=changed_file_id):
                report = self.worker_check()
                self.assertEqual(not changed_file_id, report["valid"], report)
                if changed_file_id:
                    self.assertEqual("read root identity changed after preparation", report["reason"])

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

    def test_windows_autocrlf_checkout_of_committed_lf_text_is_clean(self):
        # Git for Windows enables core.autocrlf in system config, which operator
        # Git ignores: an ordinary worker commit stores LF while CRLF stays on disk.
        (self.worker / "source.txt").write_bytes(b"original source\r\n")
        self.assert_consumers(True)
        (self.worker / "source.txt").write_bytes(b"changed source\r\n")
        self.assert_consumers(False)
        (self.worker / ".gitattributes").write_bytes(b"source.txt -text\n")
        self.git(self.worker, "add", ".gitattributes")
        self.commit(self.worker, "literal attributes")
        (self.worker / "source.txt").write_bytes(b"original source\r\n")
        self.publish()
        self.assert_consumers(False)

    def test_external_filter_is_never_executed_even_when_named_set(self):
        marker = self.base / "filter-ran"
        command = f'"{sys.executable}" -c "from pathlib import Path; Path({str(marker)!r}).write_text(\'ran\')"'
        # Configure before capture: ROOT config is immutable after assignment.
        self.git(self.root, "config", "filter.set.clean", command)
        self.git(self.root, "config", "filter.set.required", "true")
        (self.worker / ".gitattributes").write_bytes(b"source.txt filter=set text\n")
        self.git(self.worker, "-c", "filter.set.clean=", "-c", "filter.set.required=false", "add", ".gitattributes")
        self.commit(self.worker, "filter attributes")
        # The test deliberately changes ROOT config and committed attributes;
        # stage a fresh protected assignment for that exact source state.
        self._stage_recaptured_filter_source("filter-validation-run")
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
        # The test deliberately changes ROOT config and committed attributes;
        # stage a fresh protected assignment for that exact source state.
        self._stage_recaptured_filter_source("filter-equals-validation-run")
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
        run_id = "sha256-format-run"
        # Stage a real protected assignment for this SHA256 worktree so clean
        # RESULT validation exercises source validation instead of stale fixture authority.
        bootstrap._write_worker_prompt(worker, self.task, managed=False)
        bootstrap._write_invocation(
            worker, lane_id="worker", run_id=run_id,
            provider_id=self.provider["id"], model=self.provider["model"],
            launch_config=self.provider["launch_config"], exclusive_resources=[],
        )
        bootstrap._write_worker_binding(worker, self.rt, "worker", run_id)
        lane = stage_authority(
            self.rt, self.epoch,
            {**self.lane, "run_id": run_id, "worktree_path": str(worker),
             "result_path": str(worker / "RESULT.json"), "branch": "lane/hash/work",
             "git_source": source,
             "worktree_identity": [worker.stat().st_dev, worker.stat().st_ino]},
            self.task,
        )
        result = self.record(branch="lane/hash/work", commit=source["base_commit"],
                             run_id=run_id, worker_invocation_id=run_id)
        self.assertEqual(64, len(result["commit"]))
        self.assertEqual(source["content_hash"], read_invocation(lane)["git_source_hash"])
        validate_record(result, lane)


if __name__ == "__main__":
    unittest.main()
