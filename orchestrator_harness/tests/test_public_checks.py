"""Public test feedback must preserve revision, lane and sandbox boundaries."""
import importlib.util
import hashlib
from concurrent.futures import Future, ThreadPoolExecutor
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

from orchestrator_harness import public_checks as service
from orchestrator_harness.records import atomic_write_json

SOURCE = Path(__file__).resolve().parents[2]
ADAPTER_SOURCE = '''import subprocess
import time
from orchestrator_harness import processes
def preflight():
    return {"backend": "temporary-project-checks"}
def run_public_command(snapshot, command, *, timeout, cancel):
    if cancel.is_set():
        return {"exit_code": 125, "stdout": "", "stderr": "canceled", "cleanup_proven": True}
    child = subprocess.Popen(command, cwd=snapshot, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True)
    identity = processes.process_identity(child.pid)
    deadline = time.monotonic() + timeout
    canceled = timed_out = False
    try:
        while True:
            try:
                stdout, stderr = child.communicate(timeout=0.1)
                break
            except subprocess.TimeoutExpired:
                canceled, timed_out = cancel.is_set(), time.monotonic() >= deadline
                if canceled or timed_out:
                    # These finite fixture commands have no descendants. The
                    # original Popen handle owns this exact child.
                    child.terminate()
                    stdout, stderr = child.communicate(timeout=5)
                    break
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
    clean = bool(identity and processes.identity_exit_proven(child.pid, identity["creation_time"]))
    return {"exit_code": 125 if canceled else 124 if timed_out else child.returncode,
            "stdout": stdout, "stderr": stderr, "cleanup_proven": clean,
            "process": identity, "canceled": canceled, "timed_out": timed_out}
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
                      "worktree_identity": [self.lane.stat().st_dev, self.lane.stat().st_ino],
                      "input_paths": self.policy["input_paths"] + self.policy["optional_input_paths"],
                      "allow_focused": True}
        atomic_write_json(self.agent / "public-check-route.json", self.route)
        atomic_write_json(self.agent / "harness-hook-binding.json", {
            "schema": "harness-hook-binding/v1", "lane_id": self.lane_id, "run_id": self.run_id,
        })
        atomic_write_json(self.root / ".harness-runtime/RUNTIME_STATE.json", {"state": "OPEN"})
        self.native_path = self.root / ".harness-runtime/epochs/epoch/lanes/worker/lane.json"
        from orchestrator_harness.git_source import capture
        self.git_source = capture(self.root, self.lane)
        atomic_write_json(self.native_path, {"run_id": self.run_id, "worktree_path": str(self.lane),
                                          "git_source": self.git_source, "lifecycle": "running"})
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

    def test_snapshot_ignores_export_ignore_and_export_subst_attributes(self):
        fixed = self.lane / "config/build.settings"
        fixed.write_bytes(b"$Format:%H$\n")
        self.git(self.lane, "add", "config/build.settings")
        self.git(self.lane, "commit", "-qm", "literal fixed input")
        baseline = self.git(self.lane, "rev-parse", "HEAD")
        expected = service._file_digests(self.lane, baseline, self.policy["fixed_paths"])
        for attribute in ("export-ignore", "export-subst"):
            with self.subTest(attribute=attribute):
                (self.lane / ".gitattributes").write_text(f"config/build.settings {attribute}\n")
                self.git(self.lane, "add", ".gitattributes")
                self.git(self.lane, "commit", "-qm", attribute)
                revision = self.git(self.lane, "rev-parse", "HEAD")
                self.assertEqual(expected, service._file_digests(self.lane, revision, self.policy["fixed_paths"]))
                with tempfile.TemporaryDirectory() as output:
                    service.snapshot(self.lane, revision, Path(output), self.policy)
                    self.assertEqual(b"$Format:%H$\n", (Path(output) / "config/build.settings").read_bytes())

    def test_snapshot_rejects_reserved_metadata_nested_in_selected_directory(self):
        nested = self.lane / "src/.CODEX/auth.json"
        nested.parent.mkdir()
        nested.write_text("private metadata")
        self.git(self.lane, "add", "--force", "src/.CODEX/auth.json")
        self.git(self.lane, "commit", "-qm", "nested metadata")
        revision = self.git(self.lane, "rev-parse", "HEAD")
        with tempfile.TemporaryDirectory() as output:
            with self.assertRaisesRegex(ValueError, "unsafe public input"):
                service.snapshot(self.lane, revision, Path(output), self.policy)

    def test_wait_rejects_completed_feedback_from_previous_run(self):
        path = self.submit()
        self.serve(path)
        self.resume_route()
        with self.assertRaisesRegex(ValueError, "stale"):
            client.wait(self.agent, path.parent.name, 0)

    def test_wait_revalidates_route_after_resume_during_poll(self):
        path = self.submit()
        with patch.object(client.time, "sleep", side_effect=lambda _: self.resume_route()):
            with self.assertRaisesRegex(ValueError, "stale"):
                client.wait(self.agent, path.parent.name, 1)

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
            "run_id": self.run_id, "worktree_path": str(self.lane),
            "git_source": self.git_source, "lifecycle": "running",
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

    def test_inflight_old_run_cannot_overwrite_reused_request_after_resume(self):
        path = self.submit()
        old_run = self.run_id
        new_feedback = {"run_id": "2" * 32, "stdout": "new invocation feedback"}
        def complete_after_resume(*args, **kwargs):
            self.resume_route()
            self.mutate_request(path, run_id=self.run_id)
            atomic_write_json(path.parent / "result.json", new_feedback)
            return self.runner(*args, **kwargs)
        result = self.serve(path, complete_after_resume)
        self.assertEqual(old_run, result["run_id"])
        self.assertEqual(new_feedback, client.read(path.parent / "result.json"))
        self.assertEqual(result, service.read(service.home(self.root) / "jobs" / old_run / path.parent.name / "result.json"))

    def test_missing_expected_service_cannot_prove_shutdown(self):
        atomic_write_json(self.harness / service.POLICY, self.policy)
        atomic_write_json(service.home(self.root) / "config.json", self.config)
        with self.assertRaisesRegex(ValueError, "missing"):
            service.stop_service(self.root, self.harness)

    def test_malformed_service_identity_refused_before_process_queries(self):
        record = {"schema": "worker-public-check-service/v1", "root": str(self.root),
                  "harness": str(self.harness), "status": "STOPPED", "cleanup_proven": True}
        with patch.object(service.processes, "identity_matches") as query:
            for identity in ({}, {"pid": True, "creation_time": "fixture"},
                             {"pid": -1, "creation_time": "fixture"},
                             {"pid": 123, "creation_time": ""}):
                atomic_write_json(service.home(self.root) / "service.json", {**record, **identity})
                with self.assertRaisesRegex(ValueError, "identity"):
                    service.validate_service(self.root, self.harness, runtime_closed=True)
            query.assert_not_called()

    def test_service_environment_does_not_forward_credentials(self):
        environment = {"PATH": "fixture-tools", "SystemRoot": "fixture-os", "TEMP": str(self.harness),
                       "OPENAI_API_KEY": "secret", "CUSTOM_PASSWORD": "secret", "GITHUB_TOKEN": "secret",
                       "HTTP_PROXY": "http://user:secret@proxy", "UNRELATED_VARIABLE": "secret"}
        with patch.dict(service.os.environ, environment, clear=True):
            selected = service._service_environment(self.harness)
        self.assertEqual("fixture-tools", selected["PATH"])
        normalized = {key.upper(): value for key, value in selected.items()}
        self.assertEqual("fixture-os", normalized["SYSTEMROOT"])
        self.assertFalse({key.upper() for key in environment} - {"PATH", "SYSTEMROOT", "TEMP"} & set(normalized))

    def test_real_service_feedback_and_exact_shutdown_without_credentials(self):
        import os
        selected = self.policy
        adapter = Path(selected["adapter"]["path"])
        adapter.write_text(ADAPTER_SOURCE.replace(
            'return {"backend": "temporary-project-checks"}',
            'return {"backend": "temporary-project-checks", "credential_forwarded": "OPENAI_API_KEY" in __import__("os").environ}'))
        selected["adapter"]["sha256"] = hashlib.sha256(adapter.read_bytes()).hexdigest()
        atomic_write_json(self.harness / service.POLICY, selected)
        atomic_write_json(service.home(self.root) / "routes" / f"{self.run_id}.json", self.route)
        original_environment = service._service_environment
        def environment(harness):
            value = original_environment(harness)
            value["PYTHONPATH"] = str(SOURCE)
            return value
        children = []
        native_popen = service.subprocess.Popen
        def capture_child(*args, **kwargs):
            child = native_popen(*args, **kwargs)
            children.append(child)
            return child
        with (patch.dict(os.environ, {"OPENAI_API_KEY": "owned-fixture-secret"}),
              patch.object(service, "_service_environment", side_effect=environment),
              patch.object(service.subprocess, "Popen", side_effect=capture_child)):
            started = service.start_service(self.harness, self.root)
        self.addCleanup(service.stop_service, self.root, self.harness)
        self.assertEqual("READY", started["status"])
        config = service.read(service.home(self.root) / "config.json")
        self.assertFalse(config["backend"]["credential_forwarded"])
        path = self.submit()
        feedback, code = client.wait(self.agent, path.parent.name, 15)
        self.assertEqual(0, code, feedback)
        self.assertEqual(self.run_id, feedback["run_id"])
        service.stop_service(self.root, self.harness)
        self.assertEqual(0, children[0].wait(timeout=5))
        record = service.read(service.home(self.root) / "service.json")
        self.assertEqual("STOPPED", record["status"])
        self.assertTrue(record["cleanup_proven"])
        self.assertFalse(service.processes.identity_matches(record["pid"], record["creation_time"]))

    def test_idle_service_survives_long_native_admission_transaction(self):
        from orchestrator_harness.records import runtime_admission_lock
        atomic_write_json(self.harness / service.POLICY, self.policy)
        environment = service._service_environment(self.harness)
        environment["PYTHONPATH"] = str(SOURCE)
        with patch.object(service, "_service_environment", return_value=environment):
            # Bootstrap legitimately holds admission longer than Windows'
            # ten-second LK_LOCK contention window. Idle observation must not
            # require that lock or kill the already READY service.
            with runtime_admission_lock(self.root / ".harness-runtime"):
                started = service.start_service(self.harness, self.root)
                self.addCleanup(service.stop_service, self.root, self.harness)
                time.sleep(12 if os.name == "nt" else 0.5)
                try:
                    self.assertEqual(started, service.validate_service(self.root, self.harness))
                except Exception:
                    print((service.home(self.root) / "service.stderr.log").read_text(), flush=True)
                    raise

    def test_service_dispatches_each_request_only_to_its_registered_run(self):
        old_route, old_path = dict(self.route), self.submit()
        self.resume_route()
        new_path = self.submit()
        base = service.home(self.root)
        for route in (old_route, self.route):
            atomic_write_json(base / "routes" / f'{route["run_id"]}.json', route)
        self.config["policy"]["max_parallel_checks"] = 2
        self.config["startup_id"] = "3" * 32
        atomic_write_json(base / "config.json", self.config)
        dispatched = []

        def submit_inline(function, config, route, path, cancel, **kwargs):
            dispatched.append((route["run_id"], path))
            future = Future()
            future.set_result(function(config, route, path, cancel, self.runner, **kwargs))
            return future

        def request_stop(_delay):
            record = service.read(base / "service.json")
            atomic_write_json(base / "service.json", {**record, "stop_requested": True})

        with (patch.object(service.processes, "process_identity", return_value=service.processes.process_identity(os.getpid())),
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
        atomic_write_json(service.home(self.root) / "config.json", {"policy": selected_policy, "startup_id": "3" * 32})
        record = {"schema": "worker-public-check-service/v1", "root": str(self.root),
                  "harness": str(self.harness), "pid": 123, "creation_time": "windows-filetime:134361000000000000",
                  "status": "STOPPED", "cleanup_proven": True,
                  "startup_id": "3" * 32,
                  "policy_sha256": hashlib.sha256(json.dumps(self.policy, sort_keys=True).encode()).hexdigest()}
        atomic_write_json(service.home(self.root) / "service.json", record)
        with patch.object(service.processes, "identity_exit_proven", return_value=True):
            self.assertEqual("STOPPED", service.validate_service(self.root, self.harness, runtime_closed=True)["status"])
            for changes, error in (({"status": "READY"}, "not healthy"),
                                   ({"cleanup_proven": False}, "cleanup is not proven")):
                atomic_write_json(service.home(self.root) / "service.json", {**record, **changes})
                with self.assertRaisesRegex(ValueError, error):
                    service.validate_service(self.root, self.harness, runtime_closed=True)

    def bind_service_owner(self):
        identity = service.processes.process_identity(os.getpid())
        self.config["startup_id"] = "3" * 32
        base = service.home(self.root)
        atomic_write_json(base / "config.json", self.config)
        record = {"schema": "worker-public-check-service/v1", **identity,
                  "root": str(self.root), "harness": str(self.harness),
                  "startup_id": self.config["startup_id"], "status": "READY", "stop_requested": False,
                  "policy_sha256": hashlib.sha256(json.dumps(self.policy, sort_keys=True).encode()).hexdigest()}
        atomic_write_json(base / "service.json", record)
        return identity["pid"], identity["creation_time"], self.config["startup_id"]

    def test_replaced_service_cannot_publish_worker_feedback(self):
        owner = self.bind_service_owner()
        path = self.submit()
        def replace_owner(*args, **kwargs):
            base = service.home(self.root)
            record = service.read(base / "service.json")
            atomic_write_json(base / "service.json", {**record, "startup_id": "4" * 32})
            return self.runner(*args, **kwargs)
        result = service.serve_request(self.config, self.route, path, threading.Event(), replace_owner,
                                       service_owner=owner)
        self.assertEqual("FAIL", result["status"])
        self.assertFalse((path.parent / "result.json").exists())
        self.assertEqual(result, service.read(service.home(self.root) / "jobs" / self.run_id /
                                             path.parent.name / "result.json"))

    def test_delayed_preflight_cannot_overwrite_new_startup_config(self):
        self.config["startup_id"] = "3" * 32
        base = service.home(self.root)
        atomic_write_json(base / "config.json", self.config)
        replacement = {**self.config, "startup_id": "4" * 32}
        backend = Mock()
        def preflight():
            atomic_write_json(base / "config.json", replacement)
            return {"backend": "old preflight"}
        backend.preflight.side_effect = preflight
        with patch.object(service, "bridge", return_value=backend):
            with self.assertRaisesRegex(ValueError, "configuration changed during startup"):
                service.run_service(self.root)
        self.assertEqual(replacement, service.read(base / "config.json"))
        self.assertFalse((base / "service.json").exists())

    def test_old_service_cannot_publish_stopped_over_new_owner(self):
        self.config["startup_id"] = "3" * 32
        base = service.home(self.root)
        atomic_write_json(base / "config.json", self.config)
        def replace(_delay):
            record = service.read(base / "service.json")
            atomic_write_json(base / "service.json", {**record, "startup_id": "4" * 32})
        with patch.object(service.time, "sleep", side_effect=replace):
            with self.assertRaisesRegex(ValueError, "ownership changed"):
                service.run_service(self.root)
        record = service.read(base / "service.json")
        self.assertEqual("4" * 32, record["startup_id"])
        self.assertEqual("READY", record["status"])

    def test_preflight_json_values_survive_persisted_owner_validation(self):
        self.config["startup_id"] = "3" * 32
        base = service.home(self.root)
        atomic_write_json(base / "config.json", self.config)
        backend = Mock()
        backend.preflight.return_value = {"devices": ("first", "second")}
        def stop(_delay):
            record = service.read(base / "service.json")
            atomic_write_json(base / "service.json", {**record, "stop_requested": True})
        with (patch.object(service, "bridge", return_value=backend),
              patch.object(service.time, "sleep", side_effect=stop)):
            service.run_service(self.root)
        self.assertEqual(["first", "second"], service.read(base / "config.json")["backend"]["devices"])
        self.assertEqual("STOPPED", service.read(base / "service.json")["status"])

    def test_closed_resume_rejects_still_live_service_identity(self):
        record = {"schema": "worker-public-check-service/v1", "root": str(self.root),
                  "harness": str(self.harness), **service.processes.process_identity(os.getpid()),
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

    def test_changed_worker_git_redirect_cannot_authorize_public_adapter(self):
        path = self.submit()
        head = Path(self.git_source["private_dir"]) / "HEAD"
        head.write_bytes(head.read_bytes() + b"\n")
        execute = Mock()
        result = self.serve(path, execute)
        self.assertEqual("ERROR", result["status"])
        self.assertIn("redirect changed", result["error"])
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

    def test_request_growth_after_path_observation_cannot_evade_size_limit(self):
        from orchestrator_harness import mutation
        path = self.submit()
        execute = Mock()
        with patch.object(mutation, "_after_target_proof", side_effect=lambda: path.write_bytes(b"x" * 65537)):
            with self.assertRaisesRegex(ValueError, "65536-byte"):
                self.serve(path, execute)
        execute.assert_not_called()
        self.assertFalse((path.parent / "result.json").exists())

    def test_client_refuses_oversized_requests_before_publication(self):
        with self.assertRaisesRegex(ValueError, "65536-byte"):
            client.request(self.agent, "focused", ["x" * 65536])
        self.assertFalse((self.agent / "public-checks").exists())

    def test_replacement_worktree_cannot_adopt_a_prepared_route(self):
        path = self.submit()
        self.lane.rename(self.lane.with_name("retained-worker"))
        self.lane.mkdir()
        path.parent.mkdir(parents=True)
        path.write_bytes(b"{}")
        execute = Mock()
        with self.assertRaisesRegex(OSError, "root identity"):
            self.serve(path, execute)
        execute.assert_not_called()
        self.assertFalse((path.parent / "result.json").exists())

    @unittest.skipUnless(os.name == "nt", "native Windows junction publication boundary")
    def test_request_parent_swapped_to_junction_during_check_cannot_publish_outside_lane(self):
        path = self.submit()
        protected = self.harness / "protected-feedback"
        protected.mkdir()
        original = b"operator-owned feedback must survive"
        (protected / "result.json").write_bytes(original)

        def swap_after_execution(*args, **kwargs):
            checked = self.runner(*args, **kwargs)
            path.parent.rename(path.parent.with_name("retained-request"))
            subprocess.run(["cmd", "/c", "mklink", "/J", str(path.parent), str(protected)],
                           check=True, capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
            return checked

        result = self.serve(path, swap_after_execution)
        self.assertEqual(original, (protected / "result.json").read_bytes())
        retained = service.home(self.root) / "jobs" / self.run_id / path.parent.name / "result.json"
        self.assertEqual(result, service.read(retained))

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
        self.mutate_request(path, command=["--mount", f"type=bind,source={self.root.anchor},target={self.root / 'host'}"])
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
        absolute = self.root.as_posix().removeprefix(self.root.drive)
        for name in ("../private", ".git", "src/.codex/auth.json", absolute, "src/*", "src/../private"):
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
