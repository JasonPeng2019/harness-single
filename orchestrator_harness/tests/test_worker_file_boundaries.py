"""Native file boundaries preserve identity, bytes and portable Git names."""
from __future__ import annotations

import hashlib
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from orchestrator_harness import epochs, lanes, manager_queue, monitor, mutation, portable_paths, public_checks
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.stable_io import PreparedOutputTransaction


class WorkerFileBoundaryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def junction(self, link, destination):
        if os.name == "nt":
            subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(destination)],
                           check=True, capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
        else:
            link.symlink_to(destination, target_is_directory=True)

    @unittest.skipUnless(os.name == "nt", "native Windows append capability")
    def test_append_verifies_bytes_through_its_held_writer_handle(self):
        path = self.root / "events.jsonl"
        first = mutation.append_bytes(self.root, path.name, b'{"first":true}\n')
        second = mutation.append_bytes(self.root, path.name, b'{"second":true}\n')
        self.assertEqual(b'{"first":true}\n', first.content)
        self.assertEqual(b'{"first":true}\n{"second":true}\n', second.content)
        self.assertEqual(path.read_bytes(), second.content)
        self.assertEqual(hashlib.sha256(second.content).hexdigest(), second.content_sha256)

    @unittest.skipUnless(os.name == "nt", "native Windows append capability")
    def test_append_refuses_a_hardlink_before_modifying_protected_bytes(self):
        protected = self.root / "protected.jsonl"
        protected.write_bytes(b"protected original\n")
        linked = self.root / "events.jsonl"
        os.link(protected, linked)
        with self.assertRaisesRegex(mutation.MutationConflict, "hard links"):
            mutation.append_bytes(self.root, linked.name, b"forbidden append\n")
        self.assertEqual(b"protected original\n", protected.read_bytes())
        self.assertEqual(protected.read_bytes(), linked.read_bytes())

    def test_components_reject_windows_aliases_devices_and_escapes(self):
        for name in ("", ".", "..", "lane.", "lane ", "NUL", "aux.txt", "COM1.log", "LPT9",
                     "COM\u00b9.txt", "bad:name", "bad\x00name", "bad\x1fname", "x/y", "x\\y", "a?b", 'a"b'):
            with self.subTest(name=repr(name)), self.assertRaises(ValueError):
                portable_paths.component(name)
        for name in ("worker-1", ".agent-workspace", "caf\u00e9", "space inside"):
            self.assertEqual(name, portable_paths.component(name))

    def test_epoch_lane_and_mutation_paths_reject_aliases_before_effects(self):
        for name in ("..", "NUL", "lane.", "lane:stream"):
            with self.subTest(name=name):
                with self.assertRaises(epochs.EpochError):
                    epochs.lane_record_dir(self.root, "epoch", name)
                with self.assertRaises(epochs.EpochError):
                    epochs.epoch_dir(self.root, name)
                with self.assertRaises(mutation.MutationConflict):
                    mutation.safe_relative_path(name)
        self.assertEqual([], list(self.root.iterdir()))

    def test_anchored_read_enforces_size_on_opened_file(self):
        source = self.root / "request.json"
        source.write_bytes(b"{}")
        with patch.object(mutation, "_after_target_proof", side_effect=lambda: source.write_bytes(b"x" * 65)):
            with self.assertRaisesRegex(ValueError, "64-byte"):
                mutation.read_bytes(self.root, source.name, max_bytes=64)

    def test_anchored_read_refuses_replacement_after_file_proof(self):
        source = self.root / "request.json"
        source.write_bytes(b"original")
        replacement = self.root / "replacement"
        replacement.write_bytes(b"replacement")
        with patch.object(mutation, "_after_target_proof", side_effect=lambda: os.replace(replacement, source)):
            with self.assertRaisesRegex(mutation.MutationConflict, "changed before opening"):
                mutation.read_bytes(self.root, source.name, max_bytes=64)

    def test_anchored_read_refuses_linked_parent(self):
        outside = self.root / "protected"
        outside.mkdir()
        (outside / "request.json").write_bytes(b"protected original")
        worker = self.root / "worker"
        worker.mkdir()
        self.junction(worker / "requests", outside)
        with self.assertRaises(OSError):
            mutation.read_bytes(worker, "requests/request.json", max_bytes=64)
        self.assertEqual(b"protected original", (outside / "request.json").read_bytes())

    def test_root_incarnation_is_checked_for_reads_and_publication(self):
        worker = self.root / "worker"
        worker.mkdir()
        output = PreparedOutputTransaction(worker, allowed_roots=(self.root,))
        output.prepare()
        original = output.root_identity
        worker.rename(self.root / "retained-worker")
        worker.mkdir()
        (worker / "request.json").write_bytes(b"new owner")
        with self.assertRaisesRegex(mutation.MutationConflict, "root identity"):
            mutation.read_bytes(worker, "request.json", expected_root_identity=original)
        with self.assertRaises(OSError):
            output.publish_bytes(output.admit("response.json"), b"old owner's response")
        self.assertFalse((worker / "response.json").exists())

    @unittest.skipUnless(os.name == "nt", "existing mutation backend requires native Windows containment")
    def test_atomic_publication_refuses_hardlinked_destination(self):
        protected = self.root / "authority.json"
        protected.write_bytes(b"operator authority")
        worker = self.root / "worker"
        worker.mkdir()
        os.link(protected, worker / "response.json")
        with self.assertRaisesRegex(mutation.MutationConflict, "hard links"):
            mutation.publish_bytes(worker, "response.json", b"worker feedback")
        self.assertEqual(b"operator authority", protected.read_bytes())

    def git_tree(self, entries):
        repo = self.root / "repo"
        repo.mkdir(exist_ok=True)
        subprocess.run(["git", "init", "-q", str(repo)], check=True, capture_output=True)
        def git(*args, data=None):
            return subprocess.check_output(["git", "-C", str(repo), *args], input=data).decode().strip()
        blob = git("hash-object", "-w", "--stdin", data=b"exact committed bytes\n")
        tree = {}
        for entry in entries:
            current = tree
            parts = entry.split("/")
            for part in parts[:-1]:
                current = current.setdefault(part, {})
            current[parts[-1]] = blob
        def materialize(node):
            lines = []
            for name, child in node.items():
                kind = "tree" if isinstance(child, dict) else "blob"
                digest = materialize(child) if isinstance(child, dict) else child
                lines.append(f"{'040000' if kind == 'tree' else '100644'} {kind} {digest}\t{name}\n")
            return git("mktree", data="".join(lines).encode())
        return repo, materialize(tree)

    def test_native_git_alias_trees_are_rejected_before_snapshot_writes(self):
        cases = (("source.txt", "source.txt."), ("Source/a", "source/b"),
                 ("source", "SOURCE/child"), ("src/NUL.txt",), ("src/bad name ",))
        for index, entries in enumerate(cases):
            with self.subTest(entries=entries):
                repo, revision = self.git_tree(entries)
                target = self.root / f"snapshot-{index}"
                target.mkdir()
                inputs = sorted({name.split('/')[0] for name in entries})
                with self.assertRaises(ValueError):
                    public_checks.snapshot(repo, revision, target, {"input_paths": inputs, "optional_input_paths": []})
                self.assertEqual([], list(target.iterdir()))

    def test_snapshot_does_not_inherit_or_overwrite_existing_files(self):
        repo, revision = self.git_tree(("source.txt",))
        target = self.root / "snapshot"
        target.mkdir()
        (target / "unexpected").write_bytes(b"retain existing bytes")
        with self.assertRaisesRegex(ValueError, "empty owned"):
            public_checks.snapshot(repo, revision, target, {"input_paths": ["source.txt"], "optional_input_paths": []})
        self.assertEqual(b"retain existing bytes", (target / "unexpected").read_bytes())
        self.assertFalse((target / "source.txt").exists())

    def notice_fixture(self):
        rt = self.root / "runtime"
        atomic_write_json(rt / "CURRENT_EPOCH.json", {"schema": epochs.CURRENT_EPOCH_SCHEMA, "epoch_id": "epoch", "queue_id": "queue"})
        atomic_write_json(rt / "manager/QUEUE.json", {"schema": epochs.MANAGER_QUEUE_SCHEMA, "epoch_id": "epoch", "queue_id": "queue", "events": []})
        worker = rt / "worktrees/epoch/worker"
        (worker / ".agent-workspace/manager-notifications").mkdir(parents=True)
        lane = lanes.write_lane(rt, "epoch", "worker", {"run_id": "current", "worktree_path": str(worker), "lifecycle": "running", "process": {}})
        notice = {"schema": "manager-notice/v1", "lane_id": "worker", "run_id": "current",
                  "notice_id": "notice", "signal_id": "signal", "summary": "ROOT decision needed",
                  "severity": "blocking", "event_class": "WORKER_ESCALATION", "created_at": "2026-10-11T00:00:00Z"}
        path = worker / ".agent-workspace/manager-notifications/notice.json"
        return rt, worker, lane, notice, path

    def inbox_fixture(self):
        from orchestrator_harness import processes
        rt, worker, lane, _notice, _path = self.notice_fixture()
        path = worker / ".agent-workspace/QUEUE.json"
        atomic_write_json(rt / "RUNTIME_STATE.json", {"schema": "runtime-state/v1", "state": "OPEN"})
        metadata = worker.stat()
        lane = lanes.write_lane(rt, "epoch", "worker", {**lane,
            "incoming_queue_path": str(path), "worktree_identity": [metadata.st_dev, metadata.st_ino],
            "process": processes.process_identity(os.getpid())})
        atomic_write_json(path, {"schema": manager_queue.LANE_INBOX_SCHEMA, "lane_id": "worker",
                                "run_id": "current", "assignments": []})
        return rt, worker, lane, path

    def test_root_assignment_preserves_existing_worker_acknowledgement(self):
        rt, worker, lane, path = self.inbox_fixture()
        first = manager_queue.append_assignment(rt, lane, "first")
        helper = Path(__file__).resolve().parents[2] / "super-cache/workspace/.agent-workspace/lane-queue.py"
        spec = importlib.util.spec_from_file_location("inbox_worker_fixture", helper)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        atomic_write_json(worker / ".agent-workspace/harness-hook-binding.json", {
            "schema": module.BINDING_SCHEMA, "lane_id": "worker", "run_id": "current", "inbox_path": str(path)})
        module._run(worker / ".agent-workspace", "acknowledge", first["event_id"], None)
        second = manager_queue.append_assignment(rt, lane, "second")
        inbox = manager_queue.read_lane_inbox(worker)
        self.assertEqual([first["event_id"], second["event_id"]], [item["event_id"] for item in inbox["assignments"]])
        self.assertEqual(["ACKNOWLEDGED", "PENDING"], [item["state"] for item in inbox["assignments"]])

    def test_root_assignment_rejects_stale_lane_or_shutting_down_runtime(self):
        rt, worker, lane, path = self.inbox_fixture()
        original = path.read_bytes()
        lanes.write_lane(rt, "epoch", "worker", {**lane, "run_id": "new-run"})
        with self.assertRaises(manager_queue.ManagerQueueError):
            manager_queue.append_assignment(rt, lane, "stale")
        self.assertEqual(original, path.read_bytes())
        lanes.write_lane(rt, "epoch", "worker", lane)
        atomic_write_json(rt / "RUNTIME_STATE.json", {"schema": "runtime-state/v1", "state": "SHUTTING_DOWN"})
        with self.assertRaises(manager_queue.ManagerQueueError):
            manager_queue.append_assignment(rt, lane, "late")
        self.assertEqual(original, path.read_bytes())

    def test_root_assignment_rejects_foreign_and_malformed_inbox_without_replacing_it(self):
        rt, worker, lane, path = self.inbox_fixture()
        initial = manager_queue.read_lane_inbox(worker)
        for invalid in (["not-an-object"], {**initial, "lane_id": "other"},
                        {**initial, "run_id": "prior"}, {**initial, "assignments": [None]}):
            with self.subTest(invalid=invalid):
                atomic_write_json(path, invalid)
                original = path.read_bytes()
                with self.assertRaises(manager_queue.ManagerQueueError):
                    manager_queue.append_assignment(rt, lane, "refused")
                self.assertEqual(original, path.read_bytes())

    def test_root_inbox_writer_cannot_follow_a_worker_agent_workspace_junction(self):
        rt, worker, lane, path = self.inbox_fixture()
        agent = worker / ".agent-workspace"
        path.unlink()
        (agent / "manager-notifications").rmdir()
        agent.rmdir()
        protected = self.root / "protected-inbox"
        protected.mkdir()
        marker = protected / "QUEUE.json"
        marker.write_bytes(b"protected original")
        self.junction(agent, protected)
        with self.assertRaises(manager_queue.ManagerQueueError):
            manager_queue.append_assignment(rt, lane, "refused")
        with self.assertRaises(OSError):
            manager_queue.write_lane_inbox(worker, {})
        self.assertEqual(["QUEUE.json"], sorted(item.name for item in protected.iterdir()))
        self.assertEqual(b"protected original", marker.read_bytes())

    def test_root_assignment_cannot_overwrite_an_inbox_replaced_after_read(self):
        rt, worker, lane, path = self.inbox_fixture()
        original_atomic = PreparedOutputTransaction.atomic_json
        replacement = manager_queue.read_lane_inbox(worker)
        replacement["replacement_marker"] = "retain this worker publication"

        def replace_before_publication(output, target, value, **kwargs):
            atomic_write_json(path, replacement)
            return original_atomic(output, target, value, **kwargs)

        with patch.object(PreparedOutputTransaction, "atomic_json", replace_before_publication):
            with self.assertRaises(manager_queue.ManagerQueueError):
                manager_queue.append_assignment(rt, lane, "refused")
        self.assertEqual(replacement, manager_queue.read_lane_inbox(worker))

    def test_root_assignment_rechecks_current_run_after_read(self):
        rt, worker, lane, path = self.inbox_fixture()
        original = path.read_bytes()
        read = manager_queue.read_lane_inbox

        def change_run(worktree):
            inbox = read(worktree)
            lanes.write_lane(rt, "epoch", "worker", {**lane, "run_id": "new-run"})
            return inbox

        with patch.object(manager_queue, "read_lane_inbox", side_effect=change_run):
            with self.assertRaises(manager_queue.ManagerQueueError):
                manager_queue.append_assignment(rt, lane, "stale")
        self.assertEqual(original, path.read_bytes())
        self.assertEqual("new-run", lanes.read_lane(rt, "epoch", "worker")["run_id"])

    @unittest.skipUnless(os.name == "nt", "native Windows anchored lock release")
    def test_failed_kernel_lock_closes_its_anchored_file_and_releases_local_lock(self):
        from orchestrator_harness.records import RecordLock
        lock = RecordLock(self.root / "record.json", root=self.root)
        with patch("msvcrt.locking", side_effect=OSError("controlled kernel failure")):
            with self.assertRaises(OSError):
                lock.__enter__()
        self.assertIsNone(lock._anchor)
        self.assertIsNone(lock._local)
        lock.lock_path.unlink()  # A leaked no-delete native handle would refuse this.
        with RecordLock(self.root / "record.json", root=self.root):
            pass

    @unittest.skipUnless(os.name == "nt", "native Windows kernel unlock cleanup")
    def test_failed_kernel_unlock_closes_the_file_and_releases_both_lock_modes(self):
        import msvcrt
        from orchestrator_harness.records import RecordLock
        native_locking = msvcrt.locking

        def fail_unlock(descriptor, mode, count):
            if mode == msvcrt.LK_UNLCK:
                raise OSError("controlled unlock failure")
            return native_locking(descriptor, mode, count)

        for boundary in (None, self.root):
            with self.subTest(anchored=boundary is not None):
                lock = RecordLock(self.root / "unlock.json", root=boundary)
                with patch("msvcrt.locking", side_effect=fail_unlock):
                    with self.assertRaises(OSError):
                        with lock:
                            pass
                self.assertIsNone(lock._anchor)
                self.assertIsNone(lock._local)
                lock.lock_path.unlink()
                with RecordLock(self.root / "unlock.json", root=boundary):
                    pass

    def test_stale_and_malformed_notices_are_retained_without_promotion(self):
        rt, worker, lane, notice, path = self.notice_fixture()
        for bad in ({**notice, "run_id": "old"}, {**notice, "lane_id": "other"}, {**notice, "schema": "unknown"}, {}):
            atomic_write_json(path, bad)
            before = path.read_bytes()
            diagnostics = monitor._consume_outbox(rt, "epoch", lane)
            self.assertTrue(diagnostics)
            self.assertEqual(before, path.read_bytes())
            self.assertEqual([], manager_queue.read_manager_queue(rt)["events"])

    def test_processed_junction_cannot_overwrite_operator_state(self):
        rt, worker, lane, notice, path = self.notice_fixture()
        protected = rt / "operator-only"
        protected.mkdir()
        (protected / path.name).write_bytes(b"retain operator authority")
        self.junction(worker / ".agent-workspace/processed-notifications", protected)
        atomic_write_json(path, notice)
        self.assertTrue(monitor._consume_outbox(rt, "epoch", lane))
        self.assertTrue(path.exists())
        self.assertEqual(b"retain operator authority", (protected / path.name).read_bytes())
        self.assertEqual([], manager_queue.read_manager_queue(rt)["events"])

    def test_every_resume_metadata_writer_refuses_linked_agent_workspace(self):
        from orchestrator_harness import bootstrap, resume
        worker = self.root / "worker"
        worker.mkdir()
        protected = self.root / "protected"
        protected.mkdir()
        sentinels = ("worker-prompt.md", "result-template.json", "invocation.json",
                     "harness-hook-binding.json", "QUEUE.json", "overlay-receipt.json")
        for name in sentinels:
            (protected / name).write_bytes(b"operator authority")
        self.junction(worker / ".agent-workspace", protected)
        task = {"task": "Fixture task", "acceptance_criteria": ["Preserve boundaries"],
                "deliverables": ["Verified output"], "reason_for_acceptance_and_deliverables": "Fixture check"}
        operations = (
            lambda: bootstrap._write_worker_prompt(worker, task, managed=False),
            lambda: bootstrap._write_result_template(worker, "worker", "run"),
            lambda: bootstrap._write_invocation(worker, lane_id="worker", run_id="run", provider_id="qwen-code",
                                                model="fixture", launch_config={}, exclusive_resources=[]),
            lambda: bootstrap._write_worker_binding(worker, self.root / "runtime", "worker", "run"),
            lambda: resume._reset_worker_inbox(worker, "worker", "run"),
            lambda: resume._rewrite_overlay_receipt(worker, {"lane_id": "worker"}, "run", managed=False),
        )
        for operation in operations:
            with self.subTest(operation=operation), self.assertRaises(OSError):
                operation()
        for name in sentinels:
            self.assertEqual(b"operator authority", (protected / name).read_bytes())

    def test_notice_producer_rejects_invalid_or_oversized_messages_before_publication(self):
        source = Path(__file__).resolve().parents[2] / "super-cache/workspace/.agent-workspace/manager-notify.py"
        spec = importlib.util.spec_from_file_location("notice_producer_boundary_fixture", source)
        producer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(producer)
        agent = self.root / "worker/.agent-workspace"
        atomic_write_json(agent / "harness-hook-binding.json", {
            "schema": producer.BINDING_SCHEMA, "lane_id": "worker", "run_id": "run"})
        for runtime_writer in (producer._runtime_atomic_write_json, None):
            with patch.object(producer, "_runtime_atomic_write_json", runtime_writer):
                for fields in ({"severity": "blocking", "summary": "x" * 65536},
                               {"severity": "unknown", "summary": "need a decision"},
                               {"severity": "blocking", "summary": " "}):
                    with self.subTest(fields=str(fields)[:80], standalone=runtime_writer is None), self.assertRaises(ValueError):
                        producer.write_notice(agent, **fields)
                self.assertFalse((agent / "manager-notifications").exists())

    @unittest.skipUnless(os.name == "nt", "existing mutation backend requires native Windows containment")
    def test_notice_bytes_are_retained_and_retries_cannot_change_identity(self):
        rt, worker, lane, notice, path = self.notice_fixture()
        atomic_write_json(path, notice)
        original = path.read_bytes()
        with patch.object(PreparedOutputTransaction, "write_bytes", side_effect=OSError("archive temporarily unavailable")):
            self.assertTrue(monitor._consume_outbox(rt, "epoch", lane))
        digest = hashlib.sha256(original).hexdigest()
        retained = epochs.lane_record_dir(rt, "epoch", "worker") / "notice-archive/current" / f"{digest}.json"
        self.assertEqual(original, retained.read_bytes())
        atomic_write_json(path, {**notice, "summary": "mutated same signal"})
        self.assertTrue(monitor._consume_outbox(rt, "epoch", lane))
        self.assertTrue(path.exists())
        self.assertEqual(1, len(manager_queue.read_manager_queue(rt)["events"]))
        path.write_bytes(original)
        self.assertEqual([], monitor._consume_outbox(rt, "epoch", lane))
        self.assertFalse(path.exists())
        self.assertEqual(original, (worker / ".agent-workspace/processed-notifications" / path.name).read_bytes())


if __name__ == "__main__":
    unittest.main()
