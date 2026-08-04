from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

import orchestrator_harness.lane_controller as controller
from orchestrator_harness.tests.support import TemporaryGitRepository


FAKE_CODEX = r'''
import json, os, sys
argv = sys.argv[1:]
capture = os.environ.get("CODING_CONTROLLER_CAPTURE")
if capture:
    open(capture, "w", encoding="utf-8").write(json.dumps(argv))
sys.stdin.read()
if os.environ.get("CODING_CONTROLLER_NO_THREAD") != "1":
    print(json.dumps({"type": "thread.started", "thread_id": os.environ.get("CODING_CONTROLLER_THREAD", "coding-thread")}), flush=True)
print(json.dumps({"type": "turn.completed"}), flush=True)
raise SystemExit(int(os.environ.get("CODING_CONTROLLER_EXIT", "0")))
'''


class CodingLaneControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.run_root = self.root / "run"
        self.repository = TemporaryGitRepository.create(self.run_root)
        self.workspace = self.run_root / ".agent-workspace"
        self.workspace.mkdir(parents=True)
        self.runtime_root = self.root / "runtime"
        self.runtime_root.mkdir()
        self.prompt = self.run_root / "prompt.md"
        self.prompt.write_text("Implement the focused change.\n", encoding="utf-8")
        self.fake = self.root / "fake_codex.py"
        self.fake.write_text(FAKE_CODEX, encoding="utf-8")
        self.capture = self.root / "argv.json"

    def tearDown(self) -> None:
        for key in ("CODING_CONTROLLER_CAPTURE", "CODING_CONTROLLER_NO_THREAD", "CODING_CONTROLLER_THREAD", "CODING_CONTROLLER_EXIT"):
            os.environ.pop(key, None)
        self.temporary.cleanup()

    def invocation(self, *, action: str = "start", worker_id: str = "worker-1") -> tuple[Path, dict[str, object]]:
        outputs = {
            "status": str(self.workspace / "controller.status.json"),
            "jsonl": str(self.workspace / "codex.jsonl"),
            "stderr": str(self.workspace / "codex.stderr.log"),
            "last_message": str(self.workspace / "last-message.txt"),
        }
        value: dict[str, object] = {
            "schema": controller.CODING_INVOCATION_SCHEMA,
            "action": action,
            "run_root": str(self.run_root),
            "runtime_root": str(self.runtime_root),
            "event_log_path": str(self.runtime_root / "events" / "controller.jsonl"),
            "worker_invocation_id": worker_id,
            "lane_id": "coding:worker-1",
            "task": "focused test",
            "phase": "implementation",
            "prompt_path": str(self.prompt),
            "prompt_sha256": hashlib.sha256(self.prompt.read_bytes()).hexdigest(),
            "output_paths": outputs,
            "resources": ["workspace"],
            "repository": self.repository.declaration(),
            "codex": {
                "model": "gpt-5.6-codex",
                "reasoning_effort": "high",
                "service_tier": "priority",
                "command": [sys.executable, str(self.fake)],
                "config_overrides": ["feature_flag=true"],
                "sandbox": "workspace-write",
                "approval_policy": "never",
            },
        }
        path = self.workspace / f"{action}.invocation.json"
        path.write_text(json.dumps(value), encoding="utf-8")
        return path, value

    def _write(self, path: Path, value: dict[str, object]) -> None:
        path.write_text(json.dumps(value), encoding="utf-8")

    def test_minimal_coding_invocation_has_no_firmware_contract(self) -> None:
        path, _ = self.invocation()
        parsed = controller.load_invocation(path)
        self.assertEqual(controller.CODING_INVOCATION_SCHEMA, parsed.invocation_schema)
        self.assertEqual("worker-1", parsed.worker_invocation_id)
        self.assertEqual([], parsed.leases)
        self.assertEqual([], parsed.board_tokens)
        self.assertEqual([], parsed.mcp_servers)
        self.assertEqual({}, parsed.server_snapshot)
        self.assertIsNone(parsed.policy_path)
        self.assertIsNone(parsed.policy_sha256)

    def test_unknown_schema_and_prompt_integrity_or_confinement_are_rejected(self) -> None:
        path, raw = self.invocation()
        raw["schema"] = "unknown/v1"
        self._write(path, raw)
        with self.assertRaisesRegex(controller.InvocationError, "unsupported invocation schema"):
            controller.load_invocation(path)
        path, raw = self.invocation()
        raw["output_paths"] = {**raw["output_paths"], "jsonl": str(self.root / "escape.jsonl")}  # type: ignore[arg-type]
        self._write(path, raw)
        with self.assertRaisesRegex(controller.InvocationError, "escapes its allowed root"):
            controller.load_invocation(path)
        path, raw = self.invocation()
        raw["event_log_path"] = str(self.root / "outside-events.jsonl")
        self._write(path, raw)
        with self.assertRaisesRegex(controller.InvocationError, "escapes its allowed root"):
            controller.load_invocation(path)
        path, _ = self.invocation()
        self.prompt.write_text("mutated after invocation", encoding="utf-8")
        with self.assertRaisesRegex(controller.InvocationError, "prompt bytes do not match"):
            controller.load_invocation(path)

    def test_coding_rejects_firmware_only_fields(self) -> None:
        path, raw = self.invocation()
        raw["policy_sha256"] = "0" * 64
        self._write(path, raw)
        with self.assertRaisesRegex(controller.InvocationError, "reserved for the other route"):
            controller.load_invocation(path)

    def test_start_records_identity_events_and_configured_codex_argv(self) -> None:
        path, _ = self.invocation()
        os.environ["CODING_CONTROLLER_CAPTURE"] = str(self.capture)
        self.assertEqual(0, controller.main([str(path)]))
        status = json.loads((self.workspace / "controller.status.json").read_text(encoding="utf-8"))
        self.assertEqual("orchestrator-lane-controller/v1", status["schema"])
        self.assertEqual("CODEX_EXITED", status["state"])
        self.assertEqual("worker-1", status["worker_invocation_id"])
        self.assertEqual(controller.CODING_INVOCATION_SCHEMA, status["invocation_schema"])
        self.assertEqual("coding-thread", status["thread_id"])
        argv = json.loads(self.capture.read_text(encoding="utf-8"))
        self.assertIn("--dangerously-bypass-approvals-and-sandbox", argv)
        self.assertNotIn("--sandbox", argv)
        self.assertIn('approval_policy="never"', argv); self.assertIn("gpt-5.6-codex", argv)
        self.assertIn('model_reasoning_effort="high"', argv); self.assertIn('service_tier="priority"', argv)
        self.assertIn("feature_flag=true", argv)
        events = [json.loads(line) for line in (self.runtime_root / "events" / "controller.jsonl").read_text(encoding="utf-8").splitlines()]
        self.assertEqual(["CODEX_STARTED", "CODEX_EXITED"], [event["event"] for event in events])
        self.assertTrue(all(event["worker_invocation_id"] == "worker-1" for event in events))

    def test_start_without_thread_is_failure_and_resume_identity_mismatches_are_rejected(self) -> None:
        path, raw = self.invocation()
        os.environ["CODING_CONTROLLER_NO_THREAD"] = "1"
        self.assertEqual(1, controller.main([str(path)]))
        status = json.loads((self.workspace / "controller.status.json").read_text(encoding="utf-8"))
        self.assertEqual("LAUNCH_FAILED", status["state"])
        os.environ.pop("CODING_CONTROLLER_NO_THREAD")
        path, raw = self.invocation(action="resume")
        raw["resume_identity"] = {"worker_invocation_id": "another-worker", "thread_id": "coding-thread"}
        self._write(path, raw)
        with self.assertRaisesRegex(controller.InvocationError, "resume identity worker_invocation_id mismatch"):
            controller.load_invocation(path)
        path, _ = self.invocation(action="resume", worker_id="worker-2")
        self.assertEqual(2, controller.main([str(path)]))

    def test_resume_rejects_child_thread_mismatch(self) -> None:
        start, _ = self.invocation()
        self.assertEqual(0, controller.main([str(start)]))
        resume, raw = self.invocation(action="resume")
        raw["resume_thread_id"] = "coding-thread"
        self._write(resume, raw)
        os.environ["CODING_CONTROLLER_THREAD"] = "wrong-thread"
        self.assertEqual(1, controller.main([str(resume)]))
        status = json.loads((self.workspace / "controller.status.json").read_text(encoding="utf-8"))
        self.assertEqual("CONTROLLER_FAILED", status["state"])
        self.assertIn("does not match", status["error"])

    def test_resume_rejects_a_branch_switch_after_start(self) -> None:
        start, _ = self.invocation()
        self.assertEqual(0, controller.main([str(start)]))
        self.repository.git("checkout", "-b", "switched")
        resume, _ = self.invocation(action="resume")
        self.assertEqual(2, controller.main([str(resume)]))


if __name__ == "__main__":
    unittest.main()
