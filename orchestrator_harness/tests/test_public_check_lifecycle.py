"""Staged client feedback through native bootstrap, saved-session resume and shutdown.

The copied, trusted provider fixture emits native session events; no model service
is invoked. Commands run real finite Python children in committed input snapshots.
Set SERVICE_EVIDENCE_DIR to retain command receipts and native runtime records.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

from orchestrator_harness import processes, setup, shutdown
from orchestrator_harness.lanes import read_lane
from orchestrator_harness.epochs import read_current_epoch
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.tests.test_public_checks import ADAPTER_SOURCE

SOURCE = Path(__file__).resolve().parents[2]


class StagedPublicCheckLifecycleTests(unittest.TestCase):
    def test_enabled_feedback_resume_and_active_cancellation(self):
        self.exercise(enabled=True)

    def test_disabled_service_preserves_ordinary_native_coding(self):
        self.exercise(enabled=False)

    def exercise(self, *, enabled):
        temporary = tempfile.TemporaryDirectory(prefix="svc-")
        base = Path(temporary.name)
        harness, workspace = base / "h", base / "p"
        harness.mkdir()
        workspace.mkdir()
        evidence = {"enabled": enabled, "fixture": str(base), "commands": [], "feedback": [],
                    "source_root": str(SOURCE), "source_commit": subprocess.check_output(
                        ["git", "-C", str(SOURCE), "rev-parse", "HEAD"], text=True).strip()}
        rt = workspace / ".harness-runtime"
        env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        for key in ("HARNESS_ROOT_SESSION_ID", "HARNESS_SESSION_ID", "HARNESS_BINDING_ID",
                    "CODEX_THREAD_ID", "CLAUDE_SESSION_ID", "QWEN_SESSION_ID"):
            env.pop(key, None)
        monitor = None
        service_started = False

        def run(argv, cwd, *, code=0, timeout=45):
            started = time.monotonic()
            try:
                completed = subprocess.run(argv, cwd=cwd, env=env, capture_output=True,
                                           text=True, timeout=timeout)
            except subprocess.TimeoutExpired as exc:
                evidence["commands"].append({"argv": list(map(str, argv)), "cwd": str(cwd),
                    "timeout_seconds": timeout, "elapsed_seconds": round(time.monotonic() - started, 3),
                    "timed_out": True, "stdout": (exc.stdout or b"").decode("utf-8", "replace"),
                    "stderr": (exc.stderr or b"").decode("utf-8", "replace")})
                raise
            evidence["commands"].append({"argv": list(map(str, argv)), "cwd": str(cwd),
                "timeout_seconds": timeout, "exit_code": completed.returncode,
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "stdout": completed.stdout, "stderr": completed.stderr})
            self.assertIn(completed.returncode, code if isinstance(code, tuple) else (code,),
                          completed.stdout + completed.stderr)
            return completed.stdout

        def public(*args, timeout=45):
            return json.loads(run([sys.executable, "-B", "-m",
                "orchestrator_harness.operator_launch", "--json", *args], harness, timeout=timeout))

        def git(tree, *args):
            return run(["git", "-C", str(tree), *args], harness).strip()

        def wait_for(predicate, description):
            deadline = time.monotonic() + 35
            while time.monotonic() < deadline:
                value = predicate()
                if value:
                    return value
                time.sleep(0.1)
            self.fail(description)

        def close_events():
            # Discover obligations only through the native blocking interface.
            while True:
                observed = json.loads(run([sys.executable, "-B", "-m", "orchestrator_harness.operator_launch",
                    "--json", "watch", "--until-actionable", "--timeout", "5s"], harness, code=(0, 1)))
                if observed["code"] == "WATCH_TIMEOUT":
                    return
                self.assertEqual("WATCH_EVENT", observed["code"], observed)
                public("manager", "acknowledge", "--event-id", observed["event_id"])
                public("manager", "close", "--event-id", observed["event_id"], "--outcome", "BLOCKED",
                       "--summary", "trusted fixture deliberately emits no valid worker result")

        try:
            for name in ("adapters", "super-cache", "orchestrator_harness", "harness_common", "tools", "scripts"):
                shutil.copytree(SOURCE / name, harness / name, ignore=shutil.ignore_patterns("__pycache__"))
            binding = harness / "orchestrator_harness/provider_adapters/codex/launcher_binding.py"
            with binding.open("a", encoding="utf-8") as stream:
                stream.write('\ndef build_argv(**kwargs):\n    import sys\n'
                    '    return [sys.executable, "-B", str(Path(__file__).resolve().parents[3] / "fixture-provider.py"), kwargs.get("session_id") or "fixture-session"]\n')
            shutil.copyfile(binding, harness / "adapters/codex/harness/launcher_binding.py")
            (harness / "fixture-provider.py").write_text(
                'import json, sys\nfrom pathlib import Path\n'
                'Path("RESULT.json").write_text("invalid result")\n'
                'print(json.dumps({"type": "thread.started", "thread_id": sys.argv[1]}), flush=True)\n'
                'print(json.dumps({"type": "turn.completed", "usage": {"input_tokens": 1, "output_tokens": 1}}), flush=True)\n', encoding="utf-8")
            atomic_write_json(harness / "harness-config.json", {"root_workspace": str(workspace), "managed_coordination": "enabled"})
            atomic_write_json(harness / "resource-manifest.json", {"schema": "resource-manifest/v1", "resources": []})
            (workspace / ".gitignore").write_text(".harness-runtime/\n.agent-workspace/\n.codex/\nRESULT.json\n", encoding="utf-8")
            (workspace / "value.txt").write_text("initial\n", encoding="utf-8")
            (workspace / "check.py").write_text(
                'from pathlib import Path\nimport sys, time\n'
                'value = Path("value.txt").read_text().strip()\n'
                'print("source=" + value, flush=True)\nprint("diagnostic=" + value, file=sys.stderr, flush=True)\n'
                'if "--wait" in sys.argv:\n    Path("active.txt").write_text("started")\n    time.sleep(30)\n'
                'sys.exit(1 if value.endswith("fail") else 0)\n', encoding="utf-8")
            git(workspace, "init", "-q")
            git(workspace, "config", "user.name", "Harness Fixture")
            git(workspace, "config", "user.email", "fixture@example.invalid")
            git(workspace, "config", "core.longpaths", "true")
            git(workspace, "add", ".")
            git(workspace, "commit", "-qm", "initial source")
            if enabled:
                adapter = harness / "fixture-adapter.py"
                adapter.write_text(ADAPTER_SOURCE, encoding="utf-8")
                atomic_write_json(harness / "local-config/public-check-policy.json", {
                    "schema": "public-check-policy/v2", "adapter": {"path": str(adapter),
                        "sha256": hashlib.sha256(adapter.read_bytes()).hexdigest()},
                    "input_paths": ["value.txt", "check.py"], "optional_input_paths": [], "fixed_paths": [],
                    "commands": {key: [sys.executable, "-B", "check.py"] for key in ("build", "public")},
                    "allow_focused": True, "timeout_seconds": 45, "max_parallel_checks": 1})
            card = harness / "task.json"
            atomic_write_json(card, {"schema": "project-task-card/v1", "task": "Check committed fixture source",
                "acceptance_criteria": ["Staged feedback follows native invocation"],
                "deliverables": ["Feedback and exact cleanup"],
                "reason_for_acceptance_and_deliverables": "Prove optional service integration"})
            public("harness", "setup", timeout=180)
            monitor = setup.read_monitor_record(rt)
            prepared = json.loads(run([sys.executable, "-B", "-c",
                'import json; from pathlib import Path; from orchestrator_harness.config import load_config; '
                'from orchestrator_harness.root_session import publish_root_session; '
                'print(json.dumps(publish_root_session(load_config(Path.cwd()), run_id="service-fixture", provider_id="codex")))'], harness))
            env.update(HARNESS_ROOT_SESSION_ID=prepared["session_id"], HARNESS_BINDING_ID=prepared["provider_id"])
            if enabled:
                run([sys.executable, "-B", "-c", 'from pathlib import Path; from orchestrator_harness.config import load_config; '
                    'from orchestrator_harness.public_checks import start_service; h=Path.cwd(); start_service(h, load_config(h).root_workspace)'], harness)
                service_started = True
            public("lane", "bootstrap", "--lane-id", "worker", "--provider", "codex", "--model", "fixture-model",
                "--provider-option", "reasoning_effort=high", "--provider-option", "service_tier=priority",
                "--provider-option", "permission_mode=yolo", "--task-card", str(card), timeout=240)
            public("scan", "--no-write")
            epoch = read_current_epoch(rt)["epoch_id"]
            lane = read_lane(rt, epoch, "worker")
            worktree = Path(lane["worktree_path"])
            agent = worktree / ".agent-workspace"
            old_run = lane["run_id"]
            evidence["old_run"] = old_run

            def feedback(label, exit_code):
                (worktree / "value.txt").write_text(label + "\n", encoding="utf-8")
                git(worktree, "add", "value.txt")
                git(worktree, "commit", "-qm", label)
                revision = git(worktree, "rev-parse", "HEAD")
                request = json.loads(run([sys.executable, "-B", str(agent / "public-check.py"), "request", "--check", "public"], worktree))
                self.assertEqual(revision, request["revision"])
                value = json.loads(run([sys.executable, "-B", str(agent / "public-check.py"), "wait",
                    "--request-id", request["request_id"], "--timeout", "30"], worktree, code=exit_code))
                self.assertEqual(revision, value["revision"])
                self.assertEqual(lane["run_id"], value["run_id"])
                self.assertEqual("source=" + label + "\n", value["stdout"])
                self.assertEqual("diagnostic=" + label + "\n", value["stderr"])
                self.assertEqual(exit_code, value["exit_code"])
                self.assertTrue(value["cleanup_proven"])
                evidence["feedback"].append(value)
                return agent / "public-checks" / request["request_id"] / "result.json"

            if enabled:
                old_results = [feedback("before-pass", 0), feedback("before-fail", 1)]
                old_bytes = {path: path.read_bytes() for path in old_results}
            else:
                self.assertFalse((agent / "public-check.py").exists())
                (worktree / "value.txt").write_text("ordinary coding\n", encoding="utf-8")
                git(worktree, "add", "value.txt")
                git(worktree, "commit", "-qm", "ordinary coding remains usable")
            public("lane", "launch", "--lane-id", "worker")

            def exited_lane():
                current = read_lane(rt, epoch, "worker")
                from orchestrator_harness.lane_cleanup import observe_controller_status
                status = observe_controller_status(current).status or {}
                if (status.get("cleanup_proven") is True and status.get("controller_state") == "exited"
                        and processes.identity_exit_proven(current["process"]["pid"], current["process"]["creation_time"])):
                    return current
                return None

            lane = wait_for(exited_lane, "fixture provider/controller cleanup unproven")
            self.assertEqual("fixture-session", lane["session"]["session_id"])
            public("resume-lane", "--lane-id", "worker", "--resume-task-card", str(card))
            lane = read_lane(rt, epoch, "worker")
            self.assertNotEqual(old_run, lane["run_id"])
            self.assertEqual("fixture-session", lane["session"]["session_id"])
            evidence["resumed_lane"] = lane
            close_events()
            if enabled:
                feedback("after-pass", 0)
                feedback("after-fail", 1)
                for path, raw in old_bytes.items():
                    self.assertEqual(raw, path.read_bytes())
                    self.assertEqual(old_run, json.loads(raw)["run_id"])
                stale = json.loads(run([sys.executable, "-B", str(agent / "public-check.py"), "wait",
                    "--request-id", old_results[0].parent.name, "--timeout", "0"], worktree, code=2) or "{}")
                self.assertEqual({}, stale)
                request = json.loads(run([sys.executable, "-B", str(agent / "public-check.py"), "request", "--check", "focused",
                    "--", sys.executable, "-B", "check.py", "--wait"], worktree))
                wait_for(lambda: list((rt / "operator-only/public-check-service").glob("snapshot-*/active.txt")),
                         "finite cancellation command never became active")
                evidence["active_request"] = request
            public("harness", "shutdown")
            self.assertEqual("CLOSED", setup.read_runtime_state(rt)["state"])
            self.assertTrue(processes.identity_exit_proven(monitor["pid"], monitor["creation_time"]))
            if enabled:
                from orchestrator_harness.public_checks import home, validate_service
                stopped = validate_service(workspace, harness, runtime_closed=True)
                value = json.loads((home(workspace) / "jobs" / lane["run_id"] / request["request_id"] / "result.json").read_text())
                self.assertTrue(value["backend"]["canceled"], value)
                self.assertEqual(125, value["exit_code"])
                self.assertTrue(value["cleanup_proven"])
                identity = value["backend"]["process"]
                self.assertTrue(processes.identity_exit_proven(identity["pid"], identity["creation_time"]))
                evidence.update(cancellation=value, stopped_service=stopped)
            evidence["outcome"] = "PASS"
        except BaseException as exc:
            evidence.update(outcome="FAIL", error=repr(exc))
            raise
        finally:
            cleanup_errors = []
            if service_started:
                try:
                    from orchestrator_harness.public_checks import stop_service
                    stop_service(workspace, harness)
                except Exception as exc:
                    cleanup_errors.append(repr(exc))
            if monitor:
                try:
                    shutdown._stop_monitor(rt)
                except Exception as exc:
                    cleanup_errors.append(repr(exc))
            evidence["cleanup_errors"] = cleanup_errors
            if monitor:
                evidence["monitor_exit_proven"] = processes.identity_exit_proven(monitor["pid"], monitor["creation_time"])
            evidence["runtime_state"] = setup.read_runtime_state(rt)
            destination = os.environ.get("SERVICE_EVIDENCE_DIR")
            if destination:
                target = Path(destination) / ("enabled" if enabled else "disabled")
                target.mkdir(parents=True, exist_ok=True)
                if rt.exists():
                    shutil.copytree(rt, target / "native-runtime", ignore=shutil.ignore_patterns("worktrees"))
                if (harness / "local-config").exists():
                    shutil.copytree(harness / "local-config", target / "local-config")
                atomic_write_json(target / "evidence.json", evidence)
            temporary.cleanup()
            self.assertEqual([], cleanup_errors, "owned fixture cleanup unproven")


if __name__ == "__main__":
    unittest.main()
