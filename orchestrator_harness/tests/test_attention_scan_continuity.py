from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from orchestrator_harness.stable_io import PathSafetyError, SafeOutput


class AttentionScanContinuityTests(unittest.TestCase):
    def _store(self, root: Path) -> SafeOutput:
        store = SafeOutput(harness_root=root, output_root=root / "output", forbidden_roots=(), attention_logging_enabled=True, attention_epoch_id="epoch")
        store.prepare()
        return store

    @staticmethod
    def _commit(store: SafeOutput, observed: str) -> None:
        store.commit(snapshot={"observed_utc": observed}, events=[], conditions={})

    @staticmethod
    def _scans(root: Path) -> list[dict[str, object]]:
        path=root / "output" / "attention-events.jsonl"
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if json.loads(line).get("kind") == "HARNESS_SCAN_COMMITTED"]

    def test_reconstructed_store_preserves_identity_and_uses_durable_coverage_end(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); identity={"pid":os.getpid(),"created_utc":"test-creation-a"}
            with patch("orchestrator_harness.stable_io.exact_process_identity", return_value=identity):
                first=self._store(root); self._commit(first,"2026-01-01T00:00:00Z")
                second=self._store(root); self._commit(second,"2026-01-01T00:00:01Z")
            scans=self._scans(root)
            self.assertEqual(2,len(scans))
            self.assertEqual(scans[0]["harness_created_utc"],scans[1]["harness_created_utc"])
            self.assertEqual(scans[0]["coverage_end_utc"],scans[1]["coverage_start_utc"])

    def test_reused_pid_identity_refuses_to_extend_prior_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            with patch("orchestrator_harness.stable_io.exact_process_identity", return_value={"pid":os.getpid(),"created_utc":"test-creation-a"}):
                self._commit(self._store(root),"2026-01-01T00:00:00Z")
            with patch("orchestrator_harness.stable_io.exact_process_identity", return_value={"pid":os.getpid(),"created_utc":"test-creation-b"}):
                with self.assertRaisesRegex(PathSafetyError,"process identity changed"):
                    self._commit(self._store(root),"2026-01-01T00:00:01Z")
            self.assertEqual(1,len(self._scans(root)))

    def test_unknown_creation_identity_cannot_emit_authoritative_scan(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            with patch("orchestrator_harness.stable_io.exact_process_identity", return_value=None):
                with self.assertRaisesRegex(PathSafetyError,"exact harness process identity"):
                    self._commit(self._store(root),"2026-01-01T00:00:00Z")
            self.assertFalse((root / "output" / "attention-events.jsonl").exists())

    def test_repeated_stage_with_new_timestamp_is_distinct_not_a_record_id_collision(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); store=self._store(root)
            first=datetime.fromisoformat("2026-01-01T00:00:00+00:00")
            second=datetime.fromisoformat("2026-01-01T00:00:01+00:00")
            store.append_attention(kind="HARNESS_EVENT_DEFERRED",event_id="event",timestamp=first)
            store.append_attention(kind="HARNESS_EVENT_DEFERRED",event_id="event",timestamp=second)
            rows=[json.loads(line) for line in (root/"output"/"attention-events.jsonl").read_text().splitlines()]
            self.assertEqual(2,len(rows));self.assertNotEqual(rows[0]["record_id"],rows[1]["record_id"])


if __name__ == "__main__":
    unittest.main()
