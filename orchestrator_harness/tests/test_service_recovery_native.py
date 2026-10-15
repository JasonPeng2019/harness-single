"""Finite installed-layout service recovery; no model/provider invocation.

Retain every native command and fixture on failure. This is service integration
evidence, not provider/sandbox proof. Production launcher bindings stay intact.
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

from orchestrator_harness import processes
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.tests.test_public_checks import ADAPTER_SOURCE

SOURCE = Path(__file__).resolve().parents[2]


class InstalledServiceRecoveryTests(unittest.TestCase):
    def test_public_failure_recovery_and_same_run_reopen(self):
        base = Path(tempfile.mkdtemp(prefix="sr-"))
        harness, workspace = base / "h", base / "p"
        harness.mkdir()
        workspace.mkdir()
        rt = workspace / ".harness-runtime"
        service_home = rt / "operator-only/public-check-service"
        evidence = {"fixture": str(base), "commands": [], "owners": [], "feedback": [],
                    "provider_invocations": 0, "production_bindings_unchanged": True}
        evidence["source_sha256"] = {name: hashlib.sha256((SOURCE / name).read_bytes()).hexdigest()
            for name in ("orchestrator_harness/public_checks.py", "orchestrator_harness/records.py",
                         "orchestrator_harness/setup.py", "orchestrator_harness/shutdown.py",
                         "scripts/prepare_root_launch.py")}
        environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONPATH": str(harness)}
        for key in ("HARNESS_ROOT_SESSION_ID", "HARNESS_SESSION_ID", "HARNESS_BINDING_ID",
                    "CODEX_THREAD_ID", "CLAUDE_SESSION_ID", "QWEN_SESSION_ID"):
            environment.pop(key, None)
        git_config = base / "gitconfig"
        git_config.write_text("")
        environment.update(GIT_CONFIG_GLOBAL=str(git_config), GIT_CONFIG_NOSYSTEM="1")

        def read(path):
            return json.loads(path.read_text(encoding="utf-8-sig"))

        def run(argv, *, cwd=harness, code=0, timeout=240):
            start = time.monotonic()
            entry = {"argv": list(map(str, argv)), "cwd": str(cwd), "timeout_seconds": timeout}
            evidence["commands"].append(entry)
            try:
                completed = subprocess.run(argv, cwd=cwd, env=environment, capture_output=True,
                                           text=True, encoding="utf-8", timeout=timeout)
                entry.update(exit_code=completed.returncode, stdout=completed.stdout, stderr=completed.stderr)
            except subprocess.TimeoutExpired as exc:
                entry.update(timed_out=True, stdout=str(exc.stdout), stderr=str(exc.stderr))
                raise
            finally:
                entry["elapsed_seconds"] = round(time.monotonic() - start, 3)
                atomic_write_json(base / "evidence.json", evidence)
            self.assertEqual(code, completed.returncode, entry)
            return completed.stdout

        def public(*args, code=0):
            return json.loads(run([sys.executable, "-B", "-m", "orchestrator_harness.operator_launch",
                                   "--json", *args], code=code))

        def prepare(*, code=0):
            return run([sys.executable, "-B", str(harness / "scripts/prepare_root_launch.py"),
                        "--harness-dir", str(harness), "--workspace", str(workspace),
                        "--run-id", "service-recovery", "--provider", "qwen-code"], code=code)

        def git(tree, *args):
            return run(["git", "-C", str(tree), *args]).strip()

        def owners():
            # Use the actual public native monitor record location.
            from orchestrator_harness.setup import read_monitor_record
            monitor = read_monitor_record(rt)
            service = read(service_home / "service.json")
            entry = {"monitor": monitor, "service": service}
            evidence["owners"].append(entry)
            return entry

        def closed():
            captured = owners()
            result = public("harness", "shutdown")
            self.assertTrue(result["ok"], result)
            self.assertEqual("CLOSED", read(rt / "RUNTIME_STATE.json")["state"])
            for kind, identity in captured.items():
                if identity and identity.get("pid"):
                    self.assertTrue(processes.identity_exit_proven(identity["pid"], identity["creation_time"]), kind)
            captured["exact_exits_proven"] = True

        def bootstrap(lane_id):
            result = public("lane", "bootstrap", "--lane-id", lane_id, "--provider", "qwen-code",
                            "--model", "deterministic-no-launch", "--task-card", str(base / "task.json"))
            self.assertTrue(result["ok"], result)
            epoch = read(rt / "CURRENT_EPOCH.json")["epoch_id"]
            lane = read(rt / "epochs" / epoch / "lanes" / lane_id / "lane.json")
            return lane, Path(lane["worktree_path"])

        def feedback(lane, worktree, label, expected):
            (worktree / "value.txt").write_text(label + "\n")
            git(worktree, "add", "value.txt")
            git(worktree, "commit", "-qm", label)
            revision = git(worktree, "rev-parse", "HEAD")
            helper = worktree / ".agent-workspace/public-check.py"
            request = json.loads(run([sys.executable, "-B", str(helper), "request", "--check", "public"], cwd=worktree))
            result = json.loads(run([sys.executable, "-B", str(helper), "wait", "--request-id",
                                     request["request_id"], "--timeout", "180"], cwd=worktree, code=expected))
            self.assertEqual(revision, result["revision"])
            self.assertEqual(lane["run_id"], result["run_id"])
            self.assertEqual("source=" + label + "\n", result["stdout"])
            self.assertEqual("diagnostic=" + label + "\n", result["stderr"])
            self.assertTrue(result["cleanup_proven"])
            child = result["backend"]["process"]
            self.assertTrue(processes.identity_exit_proven(child["pid"], child["creation_time"]))
            evidence["feedback"].append(result)
            return service_home / "jobs" / lane["run_id"] / request["request_id"] / "result.json"

        try:
            # Independently install the documented full portable source layout.
            evidence["installation"] = "full portable source layout"
            for name in ("orchestrator_harness", "harness_common", "adapters", "super-cache", "scripts", "tools", "harness_watcher_implementation", "docs", "examples"):
                shutil.copytree(SOURCE / name, harness / name, ignore=shutil.ignore_patterns("__pycache__"))
            for name in ("README.md", "QUICK_RULES.md", "QUICK_START.md", ".gitignore"):
                shutil.copyfile(SOURCE / name, harness / name)
            bindings = {str(p.relative_to(harness)): hashlib.sha256(p.read_bytes()).hexdigest()
                        for p in harness.glob("orchestrator_harness/provider_adapters/*/launcher_binding.py")}
            atomic_write_json(harness / "harness-config.json", {"root_workspace": str(workspace), "managed_coordination": "enabled"})
            atomic_write_json(harness / "resource-manifest.json", {"schema": "resource-manifest/v1", "resources": []})
            atomic_write_json(base / "task.json", {"schema": "project-task-card/v1", "task": "Finite staged service feedback",
                "acceptance_criteria": ["Feedback matches committed inputs"], "deliverables": ["PASS/FAIL and cleanup"],
                "reason_for_acceptance_and_deliverables": "Exercise native optional service lifecycle"})
            (workspace / ".gitignore").write_text(".harness-runtime/\n.agent-workspace/\n.codex/\n.claude/\n.qwen/\nRESULT.json\n")
            (workspace / "value.txt").write_text("initial\n")
            (workspace / "check.py").write_text(
                'from pathlib import Path\nimport sys,time\nvalue=Path("value.txt").read_text().strip()\n'
                'print("source="+value,flush=True)\nprint("diagnostic="+value,file=sys.stderr,flush=True)\n'
                'if "--wait" in sys.argv:\n Path("active.txt").write_text("active"); time.sleep(120)\n'
                'sys.exit(1 if value.endswith("fail") else 0)\n')
            git(workspace, "init", "-q")
            git(workspace, "config", "user.name", "Fixture")
            git(workspace, "config", "user.email", "fixture@example.invalid")
            git(workspace, "config", "core.longpaths", "true")
            git(workspace, "add", ".")
            git(workspace, "commit", "-qm", "committed fixture inputs")
            adapter = harness / "fixture-adapter.py"
            adapter.write_text(ADAPTER_SOURCE.replace('def preflight():',
                'def preflight():\n    from pathlib import Path\n'
                '    if Path(__file__).with_suffix(".fail").exists():\n'
                '        raise RuntimeError("deliberate preflight configuration failure")'))
            adapter.with_suffix(".fail").write_text("operator fixture misconfiguration")
            atomic_write_json(harness / "local-config/public-check-policy.json", {
                "schema": "public-check-policy/v2", "adapter": {"path": str(adapter),
                    "sha256": hashlib.sha256(adapter.read_bytes()).hexdigest()},
                "input_paths": ["value.txt", "check.py"], "optional_input_paths": [], "fixed_paths": [],
                "commands": {key: [sys.executable, "-B", "check.py"] for key in ("build", "public")},
                "allow_focused": True, "timeout_seconds": 180, "max_parallel_checks": 1})
            prepare(code=2)
            failed = read(service_home / "service.json")
            self.assertEqual("STOPPED", failed["status"])
            self.assertIn("deliberate preflight", failed["error"])
            self.assertFalse((workspace / ".agent-workspace/ROOT_PRELAUNCH_SETUP.json").exists())
            evidence["failed_startup"] = failed
            closed()
            adapter.with_suffix(".fail").unlink()
            prepare()
            first_receipt = read(workspace / ".agent-workspace/ROOT_PRELAUNCH_SETUP.json")
            first = read(service_home / "service.json")
            lane, worktree = bootstrap("before")
            old_result = feedback(lane, worktree, "before-pass", 0)
            old_bytes = old_result.read_bytes()
            public("lane", "force-stop", "--lane-id", "before")
            closed()
            public("harness", "setup")
            self.assertEqual("STOPPED", read(service_home / "service.json")["status"])
            prepare()
            second_receipt = read(workspace / ".agent-workspace/ROOT_PRELAUNCH_SETUP.json")
            second = read(service_home / "service.json")
            self.assertNotEqual(first["startup_id"], second["startup_id"])
            self.assertEqual(first_receipt["root_session_id"], second_receipt["root_session_id"])
            self.assertEqual(first_receipt["configuration_sha256"], second_receipt["configuration_sha256"])
            self.assertEqual(first_receipt["hook_sha256"], second_receipt["hook_sha256"])
            self.assertNotEqual(first_receipt["monitor"]["startup_id"], second_receipt["monitor"]["startup_id"])
            lane, worktree = bootstrap("after")
            feedback(lane, worktree, "after-pass", 0)
            feedback(lane, worktree, "after-fail", 1)
            self.assertEqual(old_bytes, old_result.read_bytes())
            # An ordinary malformed/stale route must not take down the service.
            atomic_write_json(service_home / "routes/bad.json", {"schema": "worker-public-check-route/v1"})
            helper = worktree / ".agent-workspace/public-check.py"
            request = json.loads(run([sys.executable, "-B", str(helper), "request", "--check", "focused", "--",
                                      sys.executable, "-B", "check.py", "--wait"], cwd=worktree))
            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                if list(service_home.glob("snapshot-*/active.txt")):
                    break
                time.sleep(0.2)
            else:
                self.fail("owned cancellation fixture did not become active within measured startup allowance")
            public("lane", "force-stop", "--lane-id", "after")
            closed()
            canceled = read(service_home / "jobs" / lane["run_id"] / request["request_id"] / "result.json")
            self.assertTrue(canceled["cleanup_proven"], canceled)
            self.assertTrue(canceled["backend"]["canceled"], canceled)
            child = canceled["backend"]["process"]
            self.assertTrue(processes.identity_exit_proven(child["pid"], child["creation_time"]))
            self.assertTrue((service_home / "errors.jsonl").is_file())
            evidence.update(canceled=canceled, first_receipt=first_receipt, second_receipt=second_receipt)
            self.assertEqual(bindings, {name: hashlib.sha256((harness / name).read_bytes()).hexdigest() for name in bindings})
            evidence["outcome"] = "PASS"
        except BaseException as exc:
            evidence.update(outcome="FAIL", error=repr(exc))
            raise
        finally:
            if (rt / "RUNTIME_STATE.json").exists() and read(rt / "RUNTIME_STATE.json").get("state") != "CLOSED":
                try:
                    evidence["final_cleanup"] = public("harness", "shutdown")
                except Exception as exc:
                    evidence["cleanup_error"] = repr(exc)
            atomic_write_json(base / "evidence.json", evidence)
            print("SERVICE_RECOVERY_EVIDENCE=" + str(base / "evidence.json"), flush=True)


if __name__ == "__main__":
    unittest.main()
