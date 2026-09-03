"""RED specification for the worker hook dispatcher (CARD-PRODUCT-CORRECTION-008).

The dispatcher is implemented at super-cache/workspace/.agent-workspace/hook-dispatch.py
and exposes dispatch(agent_workspace: Path, boundary: str) -> dict. This module is the
test-first RED slice: the unittest run must fail until hook-dispatch.py exists.
"""

import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

WORKTREE_ROOT = Path(__file__).resolve().parents[2]
HOOK_DISPATCH_PATH = (
    WORKTREE_ROOT / "super-cache" / "workspace" / ".agent-workspace" / "hook-dispatch.py"
)
RESULT_STOP_CHECK_PATH = (
    WORKTREE_ROOT / "super-cache" / "workspace" / ".agent-workspace" / "result-stop-check.py"
)

BINDING_SCHEMA = "harness-hook-binding/v1"
INBOX_SCHEMA = "lane-inbox/v1"
RESULT_SCHEMA = "result/v1"


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _content_hash(payload):
    body = {key: value for key, value in payload.items() if key != "content_hash"}
    compact = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(compact.encode("utf-8")).hexdigest()


def _make_assignment(event_id, state, signal_id=None):
    return {
        "event_id": event_id,
        "signal_id": signal_id if signal_id is not None else "signal-" + event_id,
        "state": state,
    }


def _make_inbox(assignments):
    return {
        "schema": INBOX_SCHEMA,
        "lane_id": "lane-1",
        "run_id": "run-1",
        "assignments": assignments,
    }


def _make_binding(manager_queue_path, inbox_path, result_path, role="worker"):
    return {
        "schema": BINDING_SCHEMA,
        "role": role,
        "lane_id": "lane-1",
        "run_id": "run-1",
        "manager_queue_path": str(manager_queue_path),
        "inbox_path": str(inbox_path),
        "result_path": str(result_path),
        "result_stop_check": str(RESULT_STOP_CHECK_PATH),
    }


def _make_result(outcome, summary="slice completed", evidence=None,
                 completed_at="2026-09-02T00:00:00Z"):
    evidence = evidence if evidence is not None else [{"check": "example", "status": "PASS"}]
    payload = {
        "schema": RESULT_SCHEMA,
        "lane_id": "lane-1",
        "run_id": "run-1",
        "outcome": outcome,
        "summary": summary,
        "evidence": evidence,
        "completed_at": completed_at,
    }
    payload["content_hash"] = _content_hash(payload)
    return payload


class HookDispatchTestCase(unittest.TestCase):
    """Hermetic agent workspace plus separate fake root queue and worker inbox."""

    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("hook_dispatch", HOOK_DISPATCH_PATH)
        if spec is None or spec.loader is None:
            raise AssertionError(
                "RED: hook-dispatch.py not found at %s" % HOOK_DISPATCH_PATH
            )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        cls.dispatch_func = staticmethod(module.dispatch)

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.agent_workspace = Path(self._tmpdir.name)
        self.root_queue_path = self.agent_workspace / "root-queue" / "QUEUE.json"
        self.worker_inbox_path = self.agent_workspace / "worker-inbox" / "QUEUE.json"
        self.result_path = self.agent_workspace / "RESULT.json"
        self.binding_path = self.agent_workspace / "harness-hook-binding.json"

    def tearDown(self):
        self._tmpdir.cleanup()

    def write_binding(self, **overrides):
        binding = _make_binding(
            manager_queue_path=self.root_queue_path,
            inbox_path=self.worker_inbox_path,
            result_path=self.result_path,
        )
        binding.update(overrides)
        _write_json(self.binding_path, binding)
        return binding

    def write_inbox(self, assignments, path=None):
        target = path if path is not None else self.worker_inbox_path
        _write_json(target, _make_inbox(assignments))

    def write_root_queue(self, assignments):
        _write_json(self.root_queue_path, _make_inbox(assignments))

    def write_result(self, outcome="PASS", **overrides):
        payload = _make_result(outcome)
        payload.update(overrides)
        if "content_hash" not in overrides:
            payload["content_hash"] = _content_hash(payload)
        _write_json(self.result_path, payload)
        return payload

    def call_dispatch(self, boundary):
        return self.dispatch_func(self.agent_workspace, boundary)

    def test_post_tool_use_notice_lists_pending_event_ids(self):
        self.write_binding()
        self.write_inbox(
            [
                _make_assignment("evt-pending-1", "PENDING"),
                _make_assignment("evt-pending-2", "PENDING"),
                _make_assignment("evt-complete-1", "COMPLETE"),
            ]
        )
        self.write_root_queue([])
        result = self.call_dispatch("post-tool-use")
        self.assertEqual(result["decision"], "NOTICE")
        self.assertIn("evt-pending-1", result["notice"])
        self.assertIn("evt-pending-2", result["notice"])

    def test_post_tool_use_allows_terminal_inbox_despite_pending_root_queue(self):
        self.write_binding()
        self.write_inbox(
            [
                _make_assignment("evt-complete-1", "COMPLETE"),
                _make_assignment("evt-blocked-1", "BLOCKED"),
            ]
        )
        self.write_root_queue([_make_assignment("evt-root-pending-1", "PENDING")])
        result = self.call_dispatch("post-tool-use")
        self.assertEqual(result["decision"], "ALLOW")

    def test_stop_rejects_pending_work(self):
        self.write_binding()
        self.write_inbox([_make_assignment("evt-pending-1", "PENDING")])
        result = self.call_dispatch("stop")
        self.assertEqual(result["decision"], "REJECT")

    def test_stop_rejects_acknowledged_work(self):
        self.write_binding()
        self.write_inbox([_make_assignment("evt-ack-1", "ACKNOWLEDGED")])
        result = self.call_dispatch("stop")
        self.assertEqual(result["decision"], "REJECT")

    def test_stop_rejects_missing_result_after_terminal_inbox(self):
        self.write_binding()
        self.write_inbox([_make_assignment("evt-complete-1", "COMPLETE")])
        result = self.call_dispatch("stop")
        self.assertEqual(result["decision"], "REJECT")

    def test_stop_rejects_invalid_result_schema(self):
        self.write_binding()
        self.write_inbox([_make_assignment("evt-complete-1", "COMPLETE")])
        self.write_result("PASS", schema="result/other")
        result = self.call_dispatch("stop")
        self.assertEqual(result["decision"], "REJECT")

    def test_stop_rejects_result_with_bad_content_hash(self):
        self.write_binding()
        self.write_inbox([_make_assignment("evt-complete-1", "COMPLETE")])
        self.write_result("PASS", content_hash="0" * 64)
        result = self.call_dispatch("stop")
        self.assertEqual(result["decision"], "REJECT")

    def test_stop_rejects_result_with_empty_summary(self):
        self.write_binding()
        self.write_inbox([_make_assignment("evt-complete-1", "COMPLETE")])
        self.write_result("PASS", summary="")
        result = self.call_dispatch("stop")
        self.assertEqual(result["decision"], "REJECT")

    def test_stop_rejects_result_with_invalid_outcome(self):
        self.write_binding()
        self.write_inbox([_make_assignment("evt-complete-1", "COMPLETE")])
        self.write_result(outcome="UNKNOWN")
        result = self.call_dispatch("stop")
        self.assertEqual(result["decision"], "REJECT")

    def test_stop_allows_valid_pass_result(self):
        self.write_binding()
        self.write_inbox([_make_assignment("evt-complete-1", "COMPLETE")])
        self.write_result("PASS")
        result = self.call_dispatch("stop")
        self.assertEqual(result["decision"], "ALLOW")

    def test_stop_allows_valid_fail_result(self):
        self.write_binding()
        self.write_inbox([_make_assignment("evt-complete-1", "COMPLETE")])
        self.write_result("FAIL")
        result = self.call_dispatch("stop")
        self.assertEqual(result["decision"], "ALLOW")

    def test_stop_allows_valid_blocked_result(self):
        self.write_binding()
        self.write_inbox([_make_assignment("evt-complete-1", "COMPLETE")])
        self.write_result("BLOCKED")
        result = self.call_dispatch("stop")
        self.assertEqual(result["decision"], "ALLOW")

    def test_missing_binding_rejects(self):
        result = self.call_dispatch("stop")
        self.assertEqual(result["decision"], "REJECT")

    def test_invalid_binding_rejects(self):
        _write_json(self.binding_path, {"schema": BINDING_SCHEMA})
        result = self.call_dispatch("stop")
        self.assertEqual(result["decision"], "REJECT")

    def test_non_worker_role_rejects(self):
        self.write_binding(role="root")
        self.write_inbox([_make_assignment("evt-complete-1", "COMPLETE")])
        result = self.call_dispatch("stop")
        self.assertEqual(result["decision"], "REJECT")


if __name__ == "__main__":
    unittest.main()
