"""Public test feedback must preserve revision, lane and sandbox boundaries."""
import importlib.util
from concurrent.futures import Future
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from orchestrator_harness import public_checks as service
from orchestrator_harness.records import atomic_write_json

SOURCE = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("worker_public_check_client", SOURCE / "tools/worker_public_check.py")
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)


class PublicCheckTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        base = Path(self.temporary.name)
        self.harness, self.root = base / "harness", base / "workspace"
        self.harness.mkdir()
        self.root.mkdir()
        self.lane_id, self.run_id = "worker", "1" * 32
        self.lane = self.root / ".harness-runtime/worktrees/epoch" / self.lane_id
        for name in service.TASK_INPUTS:
            target = self.root / name
            if name in {"src", "test_artifacts"}:
                target.mkdir()
            else:
                target.write_text("public fixture\n")
        (self.root / "src/Makefile").write_text("all:\n\ttrue\n")
        (self.root / "src/harness.c").write_text("fixed harness\n")
        (self.root / "src/zstd_decompress.c").write_text("committed code\n")
        (self.root / "test_artifacts/visible.zst").write_bytes(b"visible")
        (self.root / ".gitignore").write_text(".harness-runtime/\n.agent-workspace/\n.codex/\n")
        self.git(self.root, "init", "-q")
        self.git(self.root, "config", "user.name", "Codex")
        self.git(self.root, "config", "user.email", "codex@local")
        self.git(self.root, "add", ".")
        self.git(self.root, "commit", "-qm", "public fixture")
        self.git(self.root, "worktree", "add", "-qb", "lane/worker", str(self.lane))
        self.agent = self.lane / ".agent-workspace"
        self.agent.mkdir()
        self.route = {"schema": "worker-public-check-route/v1", "lane_id": self.lane_id,
                      "run_id": self.run_id, "worktree": str(self.lane)}
        atomic_write_json(self.agent / "public-check-route.json", self.route)
        atomic_write_json(self.agent / "harness-hook-binding.json", {
            "schema": "harness-hook-binding/v1", "lane_id": self.lane_id, "run_id": self.run_id,
        })
        atomic_write_json(self.root / ".harness-runtime/RUNTIME_STATE.json", {"state": "OPEN"})
        self.native_path = self.root / ".harness-runtime/epochs/epoch/lanes/worker/lane.json"
        atomic_write_json(self.native_path, {"run_id": self.run_id, "worktree_path": str(self.lane), "lifecycle": "running"})
        self.revision = self.git(self.lane, "rev-parse", "HEAD")
        self.config = {"root": str(self.root), "harness": str(self.harness),
                       "fixed_inputs": service._file_digests(self.root, self.revision),
                       "policy": {"timeout_seconds": 10, "max_parallel_checks": 1}}
        service.home(self.root).mkdir(parents=True)

    def git(self, tree, *args):
        return subprocess.check_output(["git", "-C", str(tree), *args], text=True).strip()

    def submit(self, check="public", command=None):
        value = client.request(self.agent, check, command or [])
        return Path(value["request_path"])

    def runner(self, selected, command, **kwargs):
        self.assertEqual("committed code\n", (selected / "src/zstd_decompress.c").read_text())
        self.assertFalse((selected / ".codex").exists())
        self.assertFalse((selected / ".agent-workspace").exists())
        return {"exit_code": 1, "stdout": "PASS visible\nFAIL edge\n", "stderr": "compiler warning\n", "cleanup_proven": True}

    def serve(self, path, runner=None):
        return service.serve_request(self.config, self.route, path, threading.Event(), runner or self.runner)

    def mutate_request(self, path, **fields):
        value = client.read(path)
        value.update(fields)
        atomic_write_json(path, value)

    def test_enabled_route_installs_client_and_worker_feedback_guidance(self):
        from orchestrator_harness import bootstrap
        public_policy = {"schema": "public-check-policy/v1", "task": "zstd-decoder",
                         "timeout_seconds": 10, "max_parallel_checks": 1}
        atomic_write_json(self.harness / service.POLICY, public_policy)
        helper = self.harness / "tools/worker_public_check.py"
        helper.parent.mkdir(parents=True)
        shutil.copyfile(SOURCE / "tools/worker_public_check.py", helper)
        with patch.object(service, "validate_service", return_value={"status": "READY"}):
            service.install_route(self.harness, self.root, self.lane, self.lane_id, self.run_id)
        self.assertEqual(helper.read_bytes(), (self.agent / "public-check.py").read_bytes())
        self.assertEqual(self.route, service.read(service.home(self.root) / "routes" / f"{self.run_id}.json"))
        bootstrap._write_worker_prompt(self.lane, {
            "task": "Implement and test", "acceptance_criteria": ["Build and focused tests pass"],
            "deliverables": ["Committed tested code"],
            "reason_for_acceptance_and_deliverables": "Verify useful progress",
        }, managed=True)
        prompt = (self.agent / "worker-prompt.md").read_text(encoding="utf-8")
        self.assertIn("public-check.py request --check build", prompt)
        self.assertIn("public-check.py request --check public", prompt)
        self.assertIn("Exit 75 means pending", prompt)
        self.assertIn("Do not submit duplicates, finalize RESULT", prompt)

    def test_policy_accepts_windows_utf8_bom(self):
        path = self.harness / service.POLICY
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"schema": "public-check-policy/v1", "task": "zstd-decoder",
                                   "timeout_seconds": 10, "max_parallel_checks": 1}), encoding="utf-8-sig")
        self.assertEqual(10, service.policy(self.harness)["timeout_seconds"])

    def test_failed_checks_return_full_feedback_without_root_or_manager_queue(self):
        path = self.submit()
        result = self.serve(path)
        feedback, code = client.wait(self.agent, path.parent.name, 0)
        self.assertEqual(1, code)
        self.assertEqual(self.revision, feedback["revision"])
        self.assertEqual("compiler warning\n", feedback["stderr"])
        self.assertEqual("PASS visible\nFAIL edge\n", feedback["stdout"])
        self.assertEqual(result, feedback)
        self.assertFalse((self.root / ".harness-runtime/manager/QUEUE.json").exists())
        self.assertFalse((self.agent / "manager-notifications").exists())

    def test_uncommitted_changes_are_not_checked_in_place_of_requested_revision(self):
        path = self.submit()
        (self.lane / "src/zstd_decompress.c").write_text("later uncommitted code\n")
        self.assertEqual("FAIL", self.serve(path)["status"])
        self.assertEqual("later uncommitted code\n", (self.lane / "src/zstd_decompress.c").read_text())

    def test_client_requires_committed_task_changes(self):
        (self.lane / "src/zstd_decompress.c").write_text("dirty\n")
        with self.assertRaisesRegex(ValueError, "commit task code"):
            self.submit()

    def test_changed_tip_requires_a_new_request(self):
        path = self.submit()
        self.git(self.lane, "commit", "--allow-empty", "-qm", "new revision")
        execute = Mock()
        result = self.serve(path, execute)
        self.assertIn("no longer the lane tip", result["error"])
        execute.assert_not_called()

    def test_cross_lane_request_is_rejected(self):
        path = self.submit()
        self.mutate_request(path, lane_id="other-worker")
        execute = Mock()
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            self.serve(path, execute)
        execute.assert_not_called()
        self.assertFalse((path.parent / "result.json").exists())

    def resume_route(self):
        self.run_id = "2" * 32
        self.route = {**self.route, "run_id": self.run_id}
        atomic_write_json(self.agent / "public-check-route.json", self.route)
        atomic_write_json(self.agent / "harness-hook-binding.json", {
            "schema": "harness-hook-binding/v1", "lane_id": self.lane_id, "run_id": self.run_id,
        })
        atomic_write_json(self.native_path, {
            "run_id": self.run_id, "worktree_path": str(self.lane), "lifecycle": "running",
        })

    def test_stale_route_cannot_overwrite_resumed_feedback(self):
        old_route = dict(self.route)
        self.resume_route()
        path = self.submit()
        feedback = self.serve(path)
        execute = Mock()
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            service.serve_request(self.config, old_route, path, threading.Event(), execute)
        execute.assert_not_called()
        self.assertEqual(feedback, client.wait(self.agent, path.parent.name, 0)[0])
        self.assertFalse((service.home(self.root) / "jobs" / old_route["run_id"] / path.parent.name).exists())

    def test_service_dispatches_each_request_only_to_its_registered_run(self):
        old_route, old_path = dict(self.route), self.submit()
        self.resume_route()
        new_path = self.submit()
        base = service.home(self.root)
        for route in (old_route, self.route):
            atomic_write_json(base / "routes" / f'{route["run_id"]}.json', route)
        self.config["policy"]["max_parallel_checks"] = 2
        atomic_write_json(base / "config.json", self.config)
        dispatched = []

        def submit_inline(function, config, route, path, cancel):
            dispatched.append((route["run_id"], path))
            future = Future()
            future.set_result(function(config, route, path, cancel, self.runner))
            return future

        def request_stop(_delay):
            record = service.read(base / "service.json")
            atomic_write_json(base / "service.json", {**record, "stop_requested": True})

        with (patch.object(service.processes, "process_identity", return_value={"creation_time": "fixture"}),
              patch.object(service, "ThreadPoolExecutor") as pool,
              patch.object(service.time, "sleep", side_effect=request_stop)):
            pool.return_value.__enter__.return_value.submit.side_effect = submit_inline
            service.run_service(self.root)
        self.assertCountEqual([(old_route["run_id"], old_path), (self.run_id, new_path)], dispatched)
        self.assertEqual(self.run_id, client.wait(self.agent, new_path.parent.name, 0)[0]["run_id"])
        self.assertTrue(service.read(base / "service.json")["cleanup_proven"])

    def test_closed_resume_requires_stopped_service_with_cleanup_proof(self):
        selected_policy = {"schema": "public-check-policy/v1", "task": "zstd-decoder",
                           "timeout_seconds": 10, "max_parallel_checks": 1}
        atomic_write_json(self.harness / service.POLICY, selected_policy)
        atomic_write_json(service.home(self.root) / "config.json", {"policy": selected_policy})
        record = {"schema": "worker-public-check-service/v1", "root": str(self.root),
                  "harness": str(self.harness), "pid": 123, "creation_time": "fixture",
                  "status": "STOPPED", "cleanup_proven": True}
        atomic_write_json(service.home(self.root) / "service.json", record)
        with (patch.object(service.processes, "identity_matches", return_value=False),
              patch.object(service.processes, "process_alive", return_value=False)):
            self.assertEqual("STOPPED", service.validate_service(self.root, self.harness, runtime_closed=True)["status"])
            for changes, error in (({"status": "READY"}, "not healthy"),
                                   ({"cleanup_proven": False}, "cleanup is not proven")):
                atomic_write_json(service.home(self.root) / "service.json", {**record, **changes})
                with self.assertRaisesRegex(ValueError, error):
                    service.validate_service(self.root, self.harness, runtime_closed=True)

    def test_closed_resume_rejects_still_live_service_identity(self):
        record = {"schema": "worker-public-check-service/v1", "root": str(self.root),
                  "harness": str(self.harness), "pid": 123, "creation_time": "fixture",
                  "status": "STOPPED", "cleanup_proven": True}
        atomic_write_json(service.home(self.root) / "service.json", record)
        with patch.object(service.processes, "identity_matches", return_value=True):
            with self.assertRaisesRegex(ValueError, "cleanup is not proven"):
                service.validate_service(self.root, self.harness, runtime_closed=True)

    def test_resumed_invocation_invalidates_old_registration(self):
        path = self.submit()
        atomic_write_json(self.native_path, {"run_id": "2" * 32, "worktree_path": str(self.lane), "lifecycle": "running"})
        execute = Mock()
        self.assertIn("stale or foreign", self.serve(path, execute)["error"])
        execute.assert_not_called()

    def test_fixed_public_inputs_cannot_be_replaced(self):
        (self.lane / "src/Makefile").write_text("tampered\n")
        self.git(self.lane, "add", "src/Makefile")
        self.git(self.lane, "commit", "-qm", "tamper")
        path = self.submit()
        execute = Mock()
        self.assertIn("Makefile changed", self.serve(path, execute)["error"])
        execute.assert_not_called()

    def test_duplicate_delivery_reuses_authoritative_result(self):
        path = self.submit()
        execute = Mock(side_effect=self.runner)
        first, second = self.serve(path, execute), self.serve(path, execute)
        self.assertEqual(first, second)
        execute.assert_called_once()

    def test_mutated_completed_request_is_not_replayed(self):
        path = self.submit()
        self.serve(path)
        self.mutate_request(path, check="build")
        with self.assertRaisesRegex(ValueError, "mutated"):
            self.serve(path)

    def test_worker_does_not_consume_mismatched_feedback(self):
        path = self.submit()
        result = self.serve(path)
        result["revision"] = "0" * 40
        atomic_write_json(path.parent / "result.json", result)
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            client.wait(self.agent, path.parent.name, 0)

    def test_pending_is_bounded_and_keeps_same_request(self):
        path = self.submit()
        result, code = client.wait(self.agent, path.parent.name, 0)
        self.assertEqual(client.PENDING, code)
        self.assertEqual(path.parent.name, result["request_id"])

    def test_build_check_cannot_inject_docker_options(self):
        path = self.submit("build")
        self.mutate_request(path, command=["--mount", "type=bind,source=C:/,target=/host"])
        execute = Mock()
        self.assertIn("invalid public check operation", self.serve(path, execute)["error"])
        execute.assert_not_called()

    def test_closed_runtime_rejects_checks(self):
        path = self.submit()
        atomic_write_json(self.root / ".harness-runtime/RUNTIME_STATE.json", {"state": "CLOSED"})
        execute = Mock()
        self.assertIn("no longer OPEN", self.serve(path, execute)["error"])
        execute.assert_not_called()

    def test_archive_rejects_committed_symlinks(self):
        blob = subprocess.check_output(["git", "-C", str(self.lane), "hash-object", "-w", "--stdin"], input=b"../../private")
        self.git(self.lane, "update-index", "--add", "--cacheinfo", "120000", blob.decode().strip(), "scratch/link")
        self.git(self.lane, "commit", "-qm", "link fixture")
        revision = self.git(self.lane, "rev-parse", "HEAD")
        with tempfile.TemporaryDirectory() as output:
            with self.assertRaisesRegex(ValueError, "links and special files"):
                service.snapshot(self.lane, revision, Path(output))

    def test_no_policy_preserves_other_task_bootstrap(self):
        service.install_route(self.harness, self.root, self.lane, self.lane_id, self.run_id)
        self.assertFalse((self.agent / "public-check.py").exists())


if __name__ == "__main__":
    unittest.main()
