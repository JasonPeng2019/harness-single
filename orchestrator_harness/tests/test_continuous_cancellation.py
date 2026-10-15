"""Public cancellation of owned output-producing processes in a private copy."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

from orchestrator_harness import lane_cleanup, processes, setup
from orchestrator_harness.config import load_config
from orchestrator_harness.epochs import read_current_epoch
from orchestrator_harness.lanes import read_lane
from orchestrator_harness.leases import lease_path
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.root_session import publish_root_session


PROVIDER = r'''
import json, os, subprocess, sys, time
from pathlib import Path
from orchestrator_harness import processes
from orchestrator_harness.records import atomic_write_json

if len(sys.argv) > 1 and sys.argv[1] == "child":
    end = time.monotonic() + 40
    while time.monotonic() < end:
        print(json.dumps({"type": "fixture.output", "source": "child", "text": "x" * 256}), flush=True)
        time.sleep(.02)
else:
    child = subprocess.Popen([sys.executable, "-B", __file__, "child"])
    atomic_write_json(Path(".agent-workspace/fixture-processes.json"), {
        "parent": processes.process_identity(os.getpid()),
        "child": processes.process_identity(child.pid),
    })
    print(json.dumps({"type": "thread.started", "thread_id": "fixture-session"}), flush=True)
    for _ in range(200):
        print(json.dumps({"type": "fixture.output", "source": "parent", "text": "x" * 256}), flush=True)
    end = time.monotonic() + 40
    while time.monotonic() < end:
        print(json.dumps({"type": "fixture.output", "source": "parent", "text": "x" * 256}), flush=True)
        time.sleep(.02)
    child.wait(timeout=5)
'''


class PublicContinuousCancellationTests(unittest.TestCase):
    def test_force_stop_reuses_resource_then_shutdown_stops_busy_parent_and_child(self):
        source = Path(__file__).resolve().parents[2]
        root = Path(tempfile.mkdtemp(prefix="c4-")).resolve()
        harness, workspace = root / "h", root / "p"
        harness.mkdir()
        workspace.mkdir()
        commands, observations, lanes = [], [], []
        sentinel = sentinel_identity = monitor = None
        success = False
        rt = workspace / ".harness-runtime"
        env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}

        def command(argv, *, cwd=harness, timeout=45):
            started = time.monotonic()
            try:
                completed = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, text=True, timeout=timeout)
            except subprocess.TimeoutExpired as error:
                commands.append({"argv": argv, "cwd": str(cwd), "timeout_seconds": timeout,
                                 "error": str(error), "seconds": round(time.monotonic() - started, 3)})
                raise
            commands.append({"argv": argv, "cwd": str(cwd), "returncode": completed.returncode,
                             "seconds": round(time.monotonic() - started, 3),
                             "stdout": completed.stdout, "stderr": completed.stderr})
            print("CLEANUP_NATIVE_COMMAND " + json.dumps(commands[-1]), flush=True)
            return completed

        def public(*args, expected=None):
            completed = command([sys.executable, "-B", "-m", "orchestrator_harness.operator_launch", "--json", *args],
                                timeout=180 if args[:2] in (("harness", "setup"), ("lane", "bootstrap")) else 45)
            result = json.loads(completed.stdout)
            if expected is None:
                self.assertEqual(0, completed.returncode, result)
            elif isinstance(expected, tuple):
                self.assertIn(result["code"], expected, result)
            else:
                self.assertEqual(expected, result["code"], result)
            return result

        def bootstrap(lane_id):
            public("lane", "bootstrap", "--lane-id", lane_id, "--provider", "codex", "--model", "fixture-model",
                   "--provider-option", "reasoning_effort=high", "--provider-option", "service_tier=priority",
                   "--provider-option", "permission_mode=yolo", "--exclusive-resource", "shared", "--task-card", str(card))
            epoch = read_current_epoch(rt)["epoch_id"]
            lanes.append((epoch, lane_id))
            return read_lane(rt, epoch, lane_id)

        def start(lane):
            public("scan", "--no-write")
            public("lane", "launch", "--lane-id", lane["lane_id"])
            marker = Path(lane["worktree_path"]) / ".agent-workspace/fixture-processes.json"
            deadline = time.monotonic() + 15
            while not marker.is_file() and time.monotonic() < deadline:
                time.sleep(.05)
            owned = json.loads(marker.read_text(encoding="utf-8"))
            current = read_lane(rt, lanes[-1][0], lane["lane_id"])
            owned["controller"] = current["process"]
            self.assertTrue(lease_path(rt, "shared").is_file())
            for identity in owned.values():
                self.assertIsNotNone(identity)
                self.assertTrue(processes.identity_matches(**identity), identity)
            observations.append({"lane_id": lane["lane_id"], "owned": owned})
            return owned

        def prove_exits(owned):
            for identity in owned.values():
                self.assertTrue(processes.wait_for_exit(**identity, timeout_seconds=10), identity)

        def drain_delivered():
            # Request discovery uses only the native blocking wait.
            for _ in range(10):
                result = public("watch", "--until-actionable", "--timeout", "1s", expected=("WATCH_EVENT", "WATCH_TIMEOUT"))
                if result["code"] == "WATCH_TIMEOUT":
                    return
                public("manager", "acknowledge", "--event-id", result["event_id"])
                public("manager", "close", "--event-id", result["event_id"], "--outcome", "BLOCKED",
                       "--summary", "fixture cancellation is intentional")
            self.fail("unexpected fixture manager obligations")

        try:
            for name in ("adapters", "super-cache", "orchestrator_harness", "harness_common", "tools", "scripts"):
                shutil.copytree(source / name, harness / name, ignore=shutil.ignore_patterns("__pycache__"))
            binding = harness / "orchestrator_harness/provider_adapters/codex/launcher_binding.py"
            with binding.open("a", encoding="utf-8") as stream:
                stream.write('\ndef build_argv(**kwargs):\n    import sys\n'
                             '    return [sys.executable, "-B", str(Path(__file__).resolve().parents[3] / "fixture-provider.py")]\n'
                             '_fixture_parse_line = parse_line\n'
                             'def parse_line(line):\n    import time\n    time.sleep(.01)\n    return _fixture_parse_line(line)\n')
            shutil.copyfile(binding, harness / "adapters/codex/harness/launcher_binding.py")
            (harness / "fixture-provider.py").write_text(PROVIDER, encoding="utf-8")
            atomic_write_json(harness / "harness-config.json", {"root_workspace": str(workspace), "managed_coordination": "enabled"})
            atomic_write_json(harness / "resource-manifest.json", {"schema": "resource-manifest/v1", "resources": [{"id": "shared", "exclusive": True}]})
            for argv in (["git", "init", str(workspace)], ["git", "-C", str(workspace), "config", "core.longpaths", "true"]):
                self.assertEqual(0, command(argv).returncode)
            (workspace / "example.txt").write_text("fixture\n", encoding="utf-8")
            self.assertEqual(0, command(["git", "-C", str(workspace), "add", "example.txt"]).returncode)
            self.assertEqual(0, command(["git", "-C", str(workspace), "-c", "user.name=Harness Test", "-c", "user.email=harness@example.invalid", "commit", "-m", "fixture"]).returncode)
            card = harness / "task.json"
            atomic_write_json(card, {"schema": "project-task-card/v1", "task": "Owned continuous cancellation fixture",
                                    "acceptance_criteria": ["Owned exits and resource reuse"], "deliverables": ["Native cleanup evidence"],
                                    "reason_for_acceptance_and_deliverables": "Prove native cancellation without model sessions"})
            public("harness", "setup")
            monitor = setup.read_monitor_record(rt)
            session = publish_root_session(load_config(harness), run_id="continuous-fixture", provider_id="codex")
            env.update(HARNESS_ROOT_SESSION_ID=session["session_id"], HARNESS_BINDING_ID=session["provider_id"])
            sentinel = processes.spawn_detached([sys.executable, "-B", "-c", "import time; time.sleep(600)"], cwd=root)
            sentinel_identity = processes.process_identity(sentinel.pid)
            self.assertIsNotNone(sentinel_identity)
            first = bootstrap("busy-first")
            first_owned = start(first)
            public("lane", "force-stop", "--lane-id", "busy-first")
            prove_exits(first_owned)
            retired = read_lane(rt, lanes[0][0], "busy-first")
            self.assertTrue(lane_cleanup.cleanup_is_proven(retired))
            cleanup = json.loads(Path(retired["forced_cleanup_ref"]["path"]).read_text(encoding="utf-8"))
            self.assertEqual("forced_cancellation", cleanup["kind"])
            self.assertFalse(lease_path(rt, "shared").exists())
            self.assertTrue(processes.identity_matches(**sentinel_identity))

            # A fresh lane actually acquires and uses the released resource.
            second = bootstrap("busy-reuse")
            second_owned = start(second)
            drain_delivered()
            public("harness", "shutdown")
            prove_exits(second_owned)
            self.assertEqual("CLOSED", setup.read_runtime_state(rt)["state"])
            self.assertTrue(lane_cleanup.cleanup_is_proven(read_lane(rt, lanes[-1][0], "busy-reuse")))
            events = Path(second["controller_events_path"]).read_text(encoding="utf-8")
            self.assertIn("runtime shutting down; terminating provider boundary", events)
            self.assertTrue(processes.identity_exit_proven(monitor["pid"], monitor["creation_time"]))
            self.assertFalse(lease_path(rt, "shared").exists())
            self.assertTrue(processes.identity_matches(**sentinel_identity))
            observations.append({"sentinel_survived": sentinel_identity, "resource_reused": True, "runtime_state": "CLOSED"})
            public("harness", "shutdown")  # Repeated public closure revalidates proof.
            success = True
        finally:
            # Failure evidence and original identities remain until native cleanup
            # proves the owned processes gone. Never remove an unproven runtime.
            clean = True
            for epoch, lane_id in lanes:
                current = read_lane(rt, epoch, lane_id)
                if current.get("lifecycle") != "retired":
                    try:
                        public("lane", "force-stop", "--lane-id", lane_id)
                    except Exception as error:
                        clean = False
                        observations.append({"cleanup_error": str(error), "lane_id": lane_id})
                clean = lane_cleanup.cleanup_is_proven(read_lane(rt, epoch, lane_id)) and clean
            if monitor is not None and not processes.identity_exit_proven(monitor["pid"], monitor["creation_time"]):
                try:
                    public("harness", "shutdown")
                except Exception as error:
                    clean = False
                    observations.append({"shutdown_error": str(error)})
            if sentinel is not None:
                clean = (processes.terminate_process(**sentinel_identity, force=True) if sentinel_identity is not None
                         else processes.reap_created_process(sentinel)) and clean
                sentinel.wait(timeout=10)
                clean = (sentinel.poll() is not None) and clean
                observations.append({"sentinel_cleanup": sentinel_identity, "returncode": sentinel.returncode,
                                     "exit_proven": sentinel_identity is not None and processes.identity_exit_proven(**sentinel_identity)})
            for observation in observations:
                if "owned" in observation:
                    observation["exits"] = [{**identity, "exit_proven": processes.identity_exit_proven(**identity)}
                                             for identity in observation["owned"].values()]
                    clean = all(item["exit_proven"] for item in observation["exits"]) and clean
            record = {"success": success, "cleanup_proven": clean, "fixture_root": str(root),
                      "commands": commands, "observations": observations}
            destination = os.environ.get("CLEANUP_TEST_EVIDENCE")
            if destination:
                target = Path(destination).resolve()
                target.mkdir(parents=True, exist_ok=True)
                atomic_write_json(target / (root.name + ".json"), record)
                if rt.is_dir():
                    shutil.copytree(rt, target / root.name, ignore=shutil.ignore_patterns("worktrees"))
                # Current worktree records/transcripts are runtime evidence too.
                for epoch, lane_id in lanes:
                    lane = read_lane(rt, epoch, lane_id)
                    shutil.copytree(Path(lane["controller_status_path"]).parent, target / root.name / "workers" / lane_id)
            print("CLEANUP_NATIVE_RESULT " + json.dumps(record), flush=True)
            # Retain the fixed copy and native history for inspection. Git can
            # create read-only fixture files; cleanup proof needs no permission
            # changes or recursive removal of their evidence.
            self.assertTrue(clean, "original fixture cleanup is unproven; retain " + str(root))


if __name__ == "__main__":
    unittest.main()
