from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import root_hook_dispatch as hooks
from orchestrator_harness.config import HarnessConfig


class RootHookDispatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        self.config = HarnessConfig(self.root, self.workspace, "enabled")
        self.open_state = {"schema": "runtime-state/v1", "state": "OPEN"}

    def tearDown(self) -> None:
        self.temp.cleanup()

    def call(
        self,
        boundary: str,
        *,
        recovery: dict[str, object] | None = None,
        marker: dict[str, object] | None = None,
        queue: dict[str, object] | None = None,
        runtime: dict[str, object] | None = None,
    ) -> dict[str, object]:
        recovery = recovery or {"ok": True, "code": "MONITOR_HEALTHY"}
        marker = marker if marker is not None else {"epoch_id": "epoch-1"}
        queue = queue or {"events": []}
        runtime = runtime if runtime is not None else self.open_state
        with (
            patch.object(hooks, "load_config", return_value=self.config),
            patch.object(hooks, "read_runtime_state", return_value=runtime),
            patch.object(hooks, "read_current_epoch", return_value=marker),
            patch.object(hooks, "read_manager_queue", return_value=queue),
            patch.object(hooks, "run_monitor_recover", return_value=recovery) as recover,
        ):
            result = hooks.dispatch(self.root, boundary, "codex")
        if boundary == "post-tool-use" and runtime.get("state") == "OPEN":
            recover.assert_called_once_with(self.root.resolve())
        return result

    def test_post_tool_use_notice_is_content_free(self) -> None:
        queue = {
            "events": [
                {"event_id": "secret-id", "type": "LANE_RESULT_INVALID", "state": "PENDING", "summary": "secret summary"},
                {"event_id": "done", "type": "LANE_STATUS_CHANGED", "state": "COMPLETE"},
            ]
        }
        result = self.call("post-tool-use", queue=queue)
        self.assertEqual(result["decision"], "NOTICE")
        notice = result["notice"]
        self.assertEqual(notice["unresolved_count"], 1)
        self.assertEqual(notice["event_classes"], ["LANE_RESULT_INVALID"])
        self.assertEqual(notice["provider_id"], "codex")
        self.assertNotIn("secret-id", str(notice))
        self.assertNotIn("secret summary", str(notice))

    def test_stop_rejects_pending_or_acknowledged_obligations(self) -> None:
        for state in ("PENDING", "ACKNOWLEDGED"):
            with self.subTest(state=state):
                result = self.call(
                    "stop",
                    queue={"events": [{"type": "LANE_STATUS_CHANGED", "state": state}]},
                )
                self.assertEqual(result["decision"], "REJECT")
                self.assertTrue(result["reason"])

    def test_terminal_queue_allows_both_boundaries(self) -> None:
        queue = {"events": [{"type": "LANE_STATUS_CHANGED", "state": "COMPLETE"}]}
        self.assertEqual(self.call("post-tool-use", queue=queue), {"decision": "ALLOW"})
        self.assertEqual(self.call("stop", queue=queue), {"decision": "ALLOW"})

    def test_monitor_recovery_failure_is_a_notice(self) -> None:
        result = self.call(
            "post-tool-use",
            recovery={"ok": False, "code": "MONITOR_CLEANUP_UNPROVEN", "next_action": "verify exact process"},
        )
        self.assertEqual(result["decision"], "NOTICE")
        self.assertEqual(result["notice"]["monitor_code"], "MONITOR_CLEANUP_UNPROVEN")
        self.assertIn("verify exact process", result["notice"]["message"])

    def test_queue_corruption_fails_closed(self) -> None:
        self.assertEqual(self.call("post-tool-use", queue={"events": "bad"})["decision"], "NOTICE")
        self.assertEqual(self.call("stop", queue={"events": "bad"})["decision"], "REJECT")

    def test_no_epoch_has_no_queue_obligation(self) -> None:
        with (
            patch.object(hooks, "load_config", return_value=self.config),
            patch.object(hooks, "read_runtime_state", return_value=self.open_state),
            patch.object(hooks, "read_current_epoch", return_value=None),
            patch.object(hooks, "current_epoch_path", return_value=self.root / "missing"),
            patch.object(hooks, "read_manager_queue") as queue,
            patch.object(hooks, "run_monitor_recover") as recover,
        ):
            self.assertEqual(hooks.dispatch(self.root, "stop", "codex"), {"decision": "ALLOW"})
        queue.assert_not_called()
        recover.assert_not_called()

    def test_closed_or_plain_runtime_allows_without_recovery(self) -> None:
        closed = {"schema": "runtime-state/v1", "state": "CLOSED"}
        with (
            patch.object(hooks, "load_config", return_value=self.config),
            patch.object(hooks, "read_runtime_state", return_value=closed),
            patch.object(hooks, "run_monitor_recover") as recover,
        ):
            self.assertEqual(hooks.dispatch(self.root, "post-tool-use", "codex"), {"decision": "ALLOW"})
        recover.assert_not_called()
        plain = HarnessConfig(self.root, self.workspace, "disabled")
        with (
            patch.object(hooks, "load_config", return_value=plain),
            patch.object(hooks, "run_monitor_recover") as recover,
        ):
            self.assertEqual(hooks.dispatch(self.root, "post-tool-use", "codex"), {"decision": "ALLOW"})
        recover.assert_not_called()


if __name__ == "__main__":
    unittest.main()
