"""Public test feedback must preserve revision, lane and sandbox boundaries."""
import importlib.util
import hashlib
from concurrent.futures import Future, ThreadPoolExecutor
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from orchestrator_harness import public_checks as service
from orchestrator_harness.records import atomic_write_json

SOURCE = Path(__file__).resolve().parents[2]
ADAPTER_SOURCE = '''import subprocess
def preflight():
    return {"backend": "temporary-project-checks"}
def run_public_command(snapshot, command, *, timeout, cancel):
    if cancel.is_set():
        return {"exit_code": 125, "stdout": "", "stderr": "canceled", "cleanup_proven": True}
    checked = subprocess.run(command, cwd=snapshot, capture_output=True, text=True, timeout=timeout)
    return {"exit_code": checked.returncode, "stdout": checked.stdout,
            "stderr": checked.stderr, "cleanup_proven": True}
'''
DATACLASS_ADAPTER_SOURCE = '''from __future__ import annotations
from dataclasses import dataclass
@dataclass
class Status:
    backend: str = "dataclass-checks"
def preflight():
    return {"backend": Status().backend}
def run_public_command(snapshot, command, *, timeout, cancel):
    return {"exit_code": 0, "stdout": "", "stderr": "", "cleanup_proven": True}
'''
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
        for name in ("src", "tests", "config"):
            (self.root / name).mkdir()
        (self.root / "config/build.settings").write_text("fixed build settings\n")
        (self.root / "src/application.py").write_text("def square(value):\n    return value * value\n")
        (self.root / "tests/test_application.py").write_text(
            "import unittest\nfrom src.application import square\n"
            "class ApplicationTests(unittest.TestCase):\n"
            "    def test_square(self):\n        self.assertEqual(9, square(3))\n")
        adapter = self.harness / "project_checks.py"
        adapter.write_text(ADAPTER_SOURCE)
        self.policy = {"schema": "public-check-policy/v2",
                       "adapter": {"path": str(adapter), "sha256": hashlib.sha256(adapter.read_bytes()).hexdigest()},
                       "input_paths": ["src", "tests", "config/build.settings"],
                       "optional_input_paths": ["scratch"], "fixed_paths": ["config/build.settings"],
                       "commands": {"build": [sys.executable, "-m", "compileall", "-q", "src"],
                                    "public": [sys.executable, "-m", "unittest", "discover", "-s", "tests"]},
                       "allow_focused": True, "timeout_seconds": 10, "max_parallel_checks": 1}
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
                      "run_id": self.run_id, "worktree": str(self.lane),
                      "input_paths": self.policy["input_paths"] + self.policy["optional_input_paths"],
                      "allow_focused": True}
        atomic_write_json(self.agent / "public-check-route.json", self.route)
        atomic_write_json(self.agent / "harness-hook-binding.json", {
            "schema": "harness-hook-binding/v1", "lane_id": self.lane_id, "run_id": self.run_id,
        })
        atomic_write_json(self.root / ".harness-runtime/RUNTIME_STATE.json", {"state": "OPEN"})
        self.native_path = self.root / ".harness-runtime/epochs/epoch/lanes/worker/lane.json"
        atomic_write_json(self.native_path, {"run_id": self.run_id, "worktree_path": str(self.lane), "lifecycle": "running"})
        self.revision = self.git(self.lane, "rev-parse", "HEAD")
        self.config = {"root": str(self.root), "harness": str(self.harness),
                       "fixed_inputs": service._file_digests(self.root, self.revision, self.policy["fixed_paths"]),
                       "policy": self.policy}
        service.home(self.root).mkdir(parents=True)

    def git(self, tree, *args):
        return subprocess.check_output(["git", "-C", str(tree), *args], text=True).strip()

    def submit(self, check="public", command=None):
        value = client.request(self.agent, check, command or [])
        return Path(value["request_path"])

    def runner(self, selected, command, **kwargs):
        self.assertIn("return value * value", (selected / "src/application.py").read_text())
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
        public_policy = self.policy
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
        path.write_text(json.dumps(self.policy), encoding="utf-8-sig")
        self.assertEqual(10, service.policy(self.harness)["timeout_seconds"])

    def _configure_adapter(self, source):
        path = Path(self.policy["adapter"]["path"])
        path.write_text(source)
        self.policy["adapter"]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        return path

    def test_adapter_supports_dataclasses_with_postponed_annotations(self):
        self._configure_adapter(DATACLASS_ADAPTER_SOURCE)
        module = service.bridge(self.policy, self.root)
        self.assertEqual({"backend": "dataclass-checks"}, module.preflight())
        self.assertIs(module, sys.modules[module.Status.__module__])

    def test_concurrent_adapter_loads_share_one_registered_module(self):
        self._configure_adapter(DATACLASS_ADAPTER_SOURCE)
        with ThreadPoolExecutor(max_workers=4) as pool:
            modules = list(pool.map(lambda _: service.bridge(self.policy, self.root), range(8)))
        self.assertTrue(all(module is modules[0] for module in modules))
        self.assertEqual("dataclass-checks", modules[0].Status().backend)

    def test_identical_adapters_at_different_paths_have_distinct_modules(self):
        first = self._configure_adapter(DATACLASS_ADAPTER_SOURCE)
        second = self.harness / "other_checks.py"
        shutil.copyfile(first, second)
        other = {**self.policy, "adapter": {**self.policy["adapter"], "path": str(second)}}
        one, two = service.bridge(self.policy, self.root), service.bridge(other, self.root)
        self.assertIsNot(one, two)
        self.assertNotEqual(one.Status.__module__, two.Status.__module__)
        self.assertEqual(one.preflight(), two.preflight())

    def test_failed_adapter_import_removes_partial_module_registration(self):
        path = self._configure_adapter(DATACLASS_ADAPTER_SOURCE + '\nraise RuntimeError("import failed")\n')
        with self.assertRaisesRegex(RuntimeError, "import failed"):
            service.bridge(self.policy, self.root)
        partial = [module for module in list(sys.modules.values())
                   if getattr(module, "__file__", None) == str(path)]
        self.assertEqual([], partial)

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
        (self.lane / "src/application.py").write_text("later uncommitted code\n")
        self.assertEqual("FAIL", self.serve(path)["status"])
        self.assertEqual("later uncommitted code\n", (self.lane / "src/application.py").read_text())

    def test_client_requires_committed_task_changes(self):
        (self.lane / "src/application.py").write_text("dirty\n")
        with self.assertRaisesRegex(ValueError, "commit the configured project inputs"):
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
        selected_policy = self.policy
        atomic_write_json(self.harness / service.POLICY, selected_policy)
        atomic_write_json(service.home(self.root) / "config.json", {"policy": selected_policy})
        record = {"schema": "worker-public-check-service/v1", "root": str(self.root),
                  "harness": str(self.harness), "pid": 123, "creation_time": "fixture",
                  "status": "STOPPED", "cleanup_proven": True,
                  "policy_sha256": hashlib.sha256(json.dumps(self.policy, sort_keys=True).encode()).hexdigest()}
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
        (self.lane / "config/build.settings").write_text("tampered\n")
        self.git(self.lane, "add", "config/build.settings")
        self.git(self.lane, "commit", "-qm", "tamper")
        path = self.submit()
        execute = Mock()
        self.assertIn("fixed public check inputs changed", self.serve(path, execute)["error"])
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
                service.snapshot(self.lane, revision, Path(output), self.policy)

    def test_real_project_build_and_test_use_operator_adapter_and_return_feedback(self):
        for check in ("build", "public"):
            path = self.submit(check)
            result = service.serve_request(self.config, self.route, path, threading.Event())
            self.assertEqual("PASS", result["status"], result)
            self.assertEqual(0, client.wait(self.agent, path.parent.name, 0)[1])
            self.assertEqual(self.policy["commands"][check], result["command"])
        (self.lane / "src/application.py").write_text("def square(value):\n    return value\n")
        self.git(self.lane, "add", "src/application.py")
        self.git(self.lane, "commit", "-qm", "introduce a test failure")
        path = self.submit()
        result = service.serve_request(self.config, self.route, path, threading.Event())
        self.assertEqual("FAIL", result["status"])
        self.assertIn("AssertionError", result["stderr"])
        self.assertEqual(1, client.wait(self.agent, path.parent.name, 0)[1])

    def test_adapter_change_is_rejected_before_execution(self):
        path = self.submit()
        Path(self.policy["adapter"]["path"]).write_text("raise RuntimeError('changed')\n")
        execute = Mock()
        self.assertIn("adapter changed", self.serve(path, execute)["error"])
        execute.assert_not_called()

    def test_adapter_cannot_overwrite_request_identity(self):
        path = self.submit()
        checked = {**self.runner(self.root, [], timeout=10), "run_id": "foreign", "revision": "0" * 40}
        result = self.serve(path, Mock(return_value=checked))
        self.assertEqual(self.run_id, result["run_id"])
        self.assertEqual(self.revision, result["revision"])
        self.assertEqual("foreign", result["backend"]["run_id"])
        self.assertEqual(1, client.wait(self.agent, path.parent.name, 0)[1])

    def test_invalid_adapter_feedback_and_exceptions_preserve_failure_and_cleanup(self):
        for execute in (Mock(return_value={"exit_code": 0}), Mock(side_effect=RuntimeError("backend failed"))):
            with self.subTest(execute=execute):
                path = self.submit()
                result = self.serve(path, execute)
                self.assertEqual("ERROR", result["status"])
                self.assertEqual(2, client.wait(self.agent, path.parent.name, 0)[1])
                self.assertFalse(result["cleanup_proven"])

    def test_zero_exit_without_cleanup_proof_is_not_accepted(self):
        path = self.submit()
        checked = {"stdout": "finished", "stderr": "", "exit_code": 0, "cleanup_proven": False}
        result = self.serve(path, Mock(return_value=checked))
        self.assertEqual("ERROR", result["status"])
        self.assertEqual(2, client.wait(self.agent, path.parent.name, 0)[1])
        self.assertEqual(0, result["backend_exit_code"])

    def test_native_crash_codes_preserve_feedback_and_proven_cleanup(self):
        for code in (-9, 3221225477):
            with self.subTest(code=code):
                path = self.submit()
                checked = {"exit_code": code, "stdout": "partial output", "stderr": "native crash",
                           "cleanup_proven": True}
                result = self.serve(path, Mock(return_value=checked))
                self.assertEqual("FAIL", result["status"])
                self.assertEqual(code, result["exit_code"])
                self.assertEqual("partial output", result["stdout"])
                self.assertEqual("native crash", result["stderr"])
                self.assertTrue(result["cleanup_proven"])
                self.assertEqual(2, client.wait(self.agent, path.parent.name, 0)[1])

    def test_completed_exit_75_is_failure_without_pending_cli_status(self):
        path = self.submit()
        checked = {"exit_code": 75, "stdout": "partial output", "stderr": "temporary failure",
                   "cleanup_proven": True}
        result = self.serve(path, Mock(return_value=checked))
        feedback, code = client.wait(self.agent, path.parent.name, 0)
        self.assertEqual("FAIL", result["status"])
        self.assertEqual(75, result["exit_code"])
        self.assertEqual("partial output", result["stdout"])
        self.assertEqual("temporary failure", result["stderr"])
        self.assertTrue(result["cleanup_proven"])
        self.assertEqual(result, feedback)
        self.assertEqual(2, code)

    def test_adapter_must_be_outside_worker_editable_workspace(self):
        adapter = self.lane / "adapter.py"
        adapter.write_text(ADAPTER_SOURCE)
        selected = {**self.policy, "adapter": {"path": str(adapter),
                    "sha256": hashlib.sha256(adapter.read_bytes()).hexdigest()}}
        with self.assertRaisesRegex(ValueError, "outside.*ROOT"):
            service.bridge(selected, self.root)

    def test_focused_command_is_disabled_unless_operator_allows_it(self):
        path = self.submit("focused", [sys.executable, "-c", "print('focused')"])
        self.config["policy"]["allow_focused"] = False
        execute = Mock()
        self.assertIn("invalid public check operation", self.serve(path, execute)["error"])
        execute.assert_not_called()

    def test_policy_rejects_unsafe_inputs_and_protected_metadata(self):
        for name in ("../private", ".git", "src/.codex/auth.json", "/absolute", "src/*", "src/../private"):
            with self.subTest(name=name):
                selected = {**self.policy, "input_paths": [name]}
                atomic_write_json(self.harness / service.POLICY, selected)
                with self.assertRaisesRegex(ValueError, "unsafe input_paths"):
                    service.policy(self.harness)

    def test_nested_snapshot_contains_only_configured_committed_inputs(self):
        (self.lane / "private.txt").write_text("do not stage\n")
        with tempfile.TemporaryDirectory() as output:
            selected = Path(output)
            service.snapshot(self.lane, self.revision, selected, self.policy)
            self.assertEqual("fixed build settings\n", (selected / "config/build.settings").read_text())
            self.assertFalse((selected / "private.txt").exists())
            self.assertFalse((selected / "scratch").exists())

    def test_no_policy_preserves_other_task_bootstrap(self):
        service.install_route(self.harness, self.root, self.lane, self.lane_id, self.run_id)
        self.assertFalse((self.agent / "public-check.py").exists())


if __name__ == "__main__":
    unittest.main()
