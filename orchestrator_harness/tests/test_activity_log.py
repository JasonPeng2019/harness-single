from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness import controller, monitor
from orchestrator_harness.activity_log import (
    append_activity,
    append_detail,
    detail_log_path,
    important_log_path,
    monitor_log_path,
)


class ActivityLogTests(unittest.TestCase):
    def test_append_and_monitor_pass_are_line_oriented(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            runtime = Path(folder) / ".harness-runtime"
            append_activity(runtime, "manual", value=1)
            with (
                patch.object(monitor, "_monitor_pass", return_value=(2, [])),
                patch.object(monitor, "_heartbeat"),
            ):
                monitor.run_monitor_once(runtime, "config-id")

            records = [
                json.loads(line)
                for line in monitor_log_path(runtime).read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(
                ["manual", "pass.started", "pass.completed"],
                [record["event"] for record in records],
            )
            self.assertTrue(all(record["schema"] == "harness-activity/v1" for record in records))

            detailed = [
                json.loads(line)
                for line in detail_log_path(runtime).read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(
                ["manual", "pass.started", "pass.completed"],
                [record["event"] for record in detailed],
            )
            self.assertFalse(important_log_path(runtime).exists())

    def test_detail_retains_prompt_and_redacts_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            runtime = Path(folder) / ".harness-runtime"
            append_detail(
                runtime,
                "prompt.root_to_worker",
                component="prompt",
                prompt="Implement the complete decoder. bearer abc.def.ghi",
                api_key="sk-example0123456789",
                database_uri="mongodb+srv://name:password@example.test/db",
            )

            record = json.loads(
                detail_log_path(runtime).read_text(encoding="utf-8").strip()
            )
            self.assertIn("Implement the complete decoder", record["prompt"])
            self.assertNotIn("abc.def.ghi", record["prompt"])
            self.assertEqual("[REDACTED]", record["api_key"])
            self.assertNotIn("password", record["database_uri"])
            important = json.loads(
                important_log_path(runtime).read_text(encoding="utf-8").strip()
            )
            self.assertEqual("prompt.root_to_worker", important["event"])
            self.assertIn("Implement the complete decoder", important["prompt"])
            self.assertEqual("[REDACTED]", important["api_key"])

    def test_provider_native_event_is_mirrored_with_full_payload(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            runtime = Path(folder) / ".harness-runtime"
            controller._append_provider_event(
                runtime,
                {"lane_id": "lane-1", "run_id": "run-1"},
                attempt_number=2,
                raw_line='{"type":"item.completed","item":{"text":"request review"}}\n',
                parsed={"message": "request review"},
            )

            record = json.loads(
                detail_log_path(runtime).read_text(encoding="utf-8").strip()
            )
            self.assertEqual("worker.native_event", record["event"])
            self.assertEqual("item.completed", record["native_event"]["type"])
            self.assertEqual("request review", record["native_event"]["item"]["text"])


if __name__ == "__main__":
    unittest.main()
