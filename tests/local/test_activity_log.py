from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from memory_harness.activity_log import (
    configure_activity_log,
    write_activity,
    write_detail,
)


class MemoryActivityLogTests(unittest.TestCase):
    def test_operator_recall_is_kept_out_of_root_view(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            configure_activity_log(root / "MONITOR.log")
            with patch.dict("os.environ", {"MEMORY_HARNESS_ACTOR": "operator"}):
                write_activity("memory.atlas.recall.completed", hits=1)
                write_detail("memory.atlas.recall.vector_results", results=[{"text": "operator preflight"}])
            self.assertFalse((root / "MONITOR_IMPORTANT.log").exists())
            rows = [
                json.loads(line)
                for line in (root / "MONITOR_OPERATOR_MEMORY.log").read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(2, len(rows))
            self.assertTrue(all(row["actor"] == "operator" for row in rows))

    def test_high_level_and_expanded_memory_payloads_use_separate_logs(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            monitor = root / "MONITOR.log"
            detail = root / "MONITOR_DETAIL.log"
            important = root / "MONITOR_IMPORTANT.log"
            configure_activity_log(monitor, detail_path=detail)

            with patch.dict("os.environ", {"MEMORY_HARNESS_ACTOR": "root"}):
                write_activity("memory.recall.completed", hits=1)
                write_detail(
                    "memory.recall.results",
                    query="decoder edge cases",
                    results=[{"text": "Preserve the exact recalled procedure text."}],
                    access_token="do-not-log-this",
                )

            monitor_records = [
                json.loads(line) for line in monitor.read_text(encoding="utf-8").splitlines()
            ]
            detail_records = [
                json.loads(line) for line in detail.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(["memory.recall.completed"], [row["event"] for row in monitor_records])
            self.assertEqual(
                ["memory.recall.completed", "memory.recall.results"],
                [row["event"] for row in detail_records],
            )
            self.assertEqual(
                "Preserve the exact recalled procedure text.",
                detail_records[1]["results"][0]["text"],
            )
            self.assertEqual("[REDACTED]", detail_records[1]["access_token"])
            self.assertTrue(all(row["actor"] == "root" for row in detail_records))
            important_records = [
                json.loads(line) for line in important.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(
                ["memory.recall.completed", "memory.recall.results"],
                [row["event"] for row in important_records],
            )
            self.assertEqual("[REDACTED]", important_records[1]["access_token"])


if __name__ == "__main__":
    unittest.main()
