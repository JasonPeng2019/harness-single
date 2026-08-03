from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


@unittest.skipUnless(
    os.environ.get("RUN_REAL_MANAGER_NOTIFICATION") == "1",
    "set RUN_REAL_MANAGER_NOTIFICATION=1 to run the external Codex deployment test",
)
class RealManagerNotificationDeploymentTest(unittest.TestCase):
    def test_external_doer_signal_response_and_continuation(self) -> None:
        repo = Path(__file__).resolve().parents[2]
        model = os.environ.get("REAL_MANAGER_NOTIFICATION_MODEL", "gpt-5.6-luna")
        reasoning = os.environ.get("REAL_MANAGER_NOTIFICATION_REASONING", "medium")
        evidence_path = os.environ.get("REAL_MANAGER_NOTIFICATION_EVIDENCE")
        with tempfile.TemporaryDirectory(prefix="manager-notification-real-") as raw:
            root = Path(raw)
            suite = root / "suite"
            run = suite / "runs" / "REAL_DOER"
            workspace = run / ".agent-workspace"
            shutil.copytree(repo / "orchestrator_harness", root / "orchestrator_harness")
            harness = root / "orchestrator_harness"
            output = harness / "watcher-state"
            workspace.mkdir(parents=True)
            (workspace / "manager-signals").mkdir()
            config = harness / "config.json"
            config.write_text(
                json.dumps(
                    {
                        "suite_root": str(suite),
                        "run_globs": ["runs/*"],
                        "workspace_relpath": ".agent-workspace",
                        "output_dir": str(output),
                        "poll_interval_seconds": 0.1,
                        "watch_timeout_seconds": 60,
                        "stable_read_delay_seconds": 0,
                    }
                ),
                encoding="utf-8",
            )
            doer = root / "doer.py"
            doer.write_text(
                """import json, time
from pathlib import Path
workspace = Path(__file__).parent / 'suite/runs/REAL_DOER/.agent-workspace'
signal = workspace / 'manager-signals/real-doer.json'
signal.write_text(json.dumps({'schema':'manager-signal/v1','signal_id':'real-doer-1','kind':'HELP','created_utc':'2026-07-30T12:00:00Z','lane_id':'REAL_DOER:Codex:REAL','task':'REAL','phase':'await-response','summary':'real doer requests manager response','evidence_paths':['not-opened.txt']}) + '\\n')
response = Path(__file__).parent / 'manager-response.json'
deadline = time.time() + 60
while time.time() < deadline and not response.exists():
    time.sleep(0.05)
if not response.exists():
    raise SystemExit(4)
payload = json.loads(response.read_text())
if payload.get('instruction') != 'continue' or not isinstance(payload.get('event_id'), str) or len(payload['event_id']) != 64:
    raise SystemExit(5)
(workspace / 'continued.json').write_text(json.dumps({'continued':True, 'event_id':payload['event_id']}) + '\\n')
""",
                encoding="utf-8",
            )
            child_env = os.environ.copy()
            child_env["PYTHONPATH"] = str(root)
            watcher = None
            agent = None
            try:
                watcher = subprocess.Popen(
                    [sys.executable, "-m", "orchestrator_harness", "--config", str(config), "watch", "--until-actionable", "--timeout", "60"],
                    cwd=root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=child_env,
                )
                agent = subprocess.Popen(
                    [
                        "codex",
                        "--dangerously-bypass-approvals-and-sandbox",
                        "-m",
                        model,
                        "-c",
                        f'model_reasoning_effort="{reasoning}"',
                        "-c",
                        'service_tier="priority"',
                        "-c",
                        'approval_policy="never"',
                        "-c",
                        "mcp_servers={}",
                        "exec",
                        "--ephemeral",
                        "--ignore-user-config",
                        "--skip-git-repo-check",
                        "--json",
                        "Run exactly: python doer.py",
                    ],
                    cwd=root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=child_env,
                )
                watcher_output, watcher_error = watcher.communicate(timeout=75)
                if watcher.returncode != 0:
                    agent_snapshot = ""
                    if agent.poll() is not None:
                        agent_snapshot = (agent.stderr.read() if agent.stderr else "")[-4000:]
                    self.fail(f"watcher exited {watcher.returncode}: {watcher_error}; agent={agent.poll()} stderr={agent_snapshot}")
                notification = json.loads(watcher_output.strip().splitlines()[-1])
                self.assertEqual("MANAGER_SIGNAL", notification["type"])
                event_id = notification["event_id"]

                redelivery = subprocess.run(
                    [sys.executable, "-m", "orchestrator_harness", "--config", str(config), "watch", "--until-actionable", "--timeout", "2"],
                    cwd=root, capture_output=True, text=True, timeout=10, env=child_env,
                )
                self.assertEqual(event_id, json.loads(redelivery.stdout.strip().splitlines()[-1])["event_id"])
                wrong = subprocess.run(
                    [sys.executable, "-m", "orchestrator_harness", "--config", str(config), "ack", "--event-id", "WRONG"],
                    cwd=root, capture_output=True, text=True, timeout=10, env=child_env,
                )
                self.assertNotEqual(0, wrong.returncode)
                self.assertEqual(event_id, json.loads(subprocess.run(
                    [sys.executable, "-m", "orchestrator_harness", "--config", str(config), "watch", "--until-actionable", "--timeout", "2"],
                    cwd=root, capture_output=True, text=True, timeout=10, env=child_env,
                ).stdout.strip().splitlines()[-1])["event_id"])

                (root / "manager-response.json").write_text(
                    json.dumps({"event_id": event_id, "instruction": "continue"}),
                    encoding="utf-8",
                )
                acknowledged = subprocess.run(
                    [sys.executable, "-m", "orchestrator_harness", "--config", str(config), "ack", "--event-id", event_id],
                    cwd=root, capture_output=True, text=True, timeout=10, env=child_env,
                )
                self.assertEqual(0, acknowledged.returncode, acknowledged.stderr)
                agent_output, agent_error = agent.communicate(timeout=30)
                self.assertEqual(0, agent.returncode, agent_error + agent_output[-2000:])
                continued = json.loads((workspace / "continued.json").read_text(encoding="utf-8"))
                self.assertEqual({"continued": True, "event_id": event_id}, continued)
                agent_events = [
                    json.loads(line)
                    for line in agent_output.splitlines()
                    if line.strip().startswith("{")
                ]
                thread_ids = [
                    item["thread_id"]
                    for item in agent_events
                    if item.get("type") == "thread.started"
                ]
                self.assertEqual(1, len(thread_ids), agent_output[-2000:])
                run_files = {path.relative_to(run).as_posix() for path in run.rglob("*") if path.is_file()}
                self.assertEqual({".agent-workspace/manager-signals/real-doer.json", ".agent-workspace/continued.json"}, run_files)
                notification_state = json.loads(
                    (output / "pending-notification.json").read_text(encoding="utf-8")
                )
                self.assertIsNone(notification_state["pending"])
                self.assertIn(event_id, notification_state["acknowledged_event_ids"])
                if evidence_path:
                    Path(evidence_path).write_text(
                        json.dumps(
                            {
                                "status": "PASS",
                                "model": model,
                                "reasoning": reasoning,
                                "service_tier": "priority",
                                "thread_id": thread_ids[0],
                                "event_id": event_id,
                                "same_doer_continued": continued,
                                "run_files": sorted(run_files),
                                "pending_after_ack": notification_state["pending"],
                                "acknowledged": event_id
                                in notification_state["acknowledged_event_ids"],
                            },
                            indent=2,
                            sort_keys=True,
                        )
                        + "\n",
                        encoding="utf-8",
                    )
            finally:
                for process in (agent, watcher):
                    if process is not None and process.poll() is None:
                        if os.name == "nt":
                            subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], capture_output=True)
                        else:
                            process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()


if __name__ == "__main__":
    unittest.main()
