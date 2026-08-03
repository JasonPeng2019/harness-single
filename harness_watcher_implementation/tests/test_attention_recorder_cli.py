from __future__ import annotations

import json
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from harness_watcher_implementation import settings
from harness_watcher_implementation.__main__ import main
from harness_watcher_implementation.attention import producer_path
from harness_watcher_implementation.config import WatcherConfig


class AttentionRecorderCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.original_active = settings.harness_watcher_active
        settings.harness_watcher_active = True

    def tearDown(self) -> None:
        settings.harness_watcher_active = self.original_active
        self.temp.cleanup()

    def _config(self, runtime: str = "runtime") -> WatcherConfig:
        return WatcherConfig(
            self.root, self.root / runtime, (), attention_logging_enabled=True,
            attention_producers=(("subagent", "lane"),),
        )

    def _args(self, *metadata: str) -> list[str]:
        return [
            "record-attention", "--role", "subagent", "--source-id", "lane",
            "--epoch-id", "epoch", "--event-id", "event", "--kind",
            "AGENT_SIGNAL_CREATED", *metadata,
        ]

    def test_metadata_file_writes_the_same_record_bytes_as_inline_metadata(self) -> None:
        metadata = '{"agent_blocked":true,"lane_id":"lane"}'
        metadata_file = self.root / "metadata.json"
        metadata_file.write_text(metadata, encoding="utf-8")
        fixed_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
        with patch("harness_watcher_implementation.__main__.load_config", return_value=self._config("inline")), patch("harness_watcher_implementation.attention.uuid.uuid4", return_value=fixed_id), patch("harness_watcher_implementation.attention.utc_now", return_value="2026-08-01T00:00:00+00:00"):
            self.assertEqual(0, main(self._args("--metadata", metadata)))
        with patch("harness_watcher_implementation.__main__.load_config", return_value=self._config("file")), patch("harness_watcher_implementation.attention.uuid.uuid4", return_value=fixed_id), patch("harness_watcher_implementation.attention.utc_now", return_value="2026-08-01T00:00:00+00:00"):
            self.assertEqual(0, main(self._args("--metadata-file", str(metadata_file))))
        inline = producer_path(self.root / "inline", role="subagent", source_id="lane").read_bytes()
        from_file = producer_path(self.root / "file", role="subagent", source_id="lane").read_bytes()
        self.assertEqual(inline, from_file)

    def test_publication_cli_preserves_captured_source_timestamp(self) -> None:
        config = self._config()
        captured = "2026-08-02T12:34:56+00:00"
        args = [
            "record-attention", "--role", "subagent", "--source-id", "lane",
            "--epoch-id", "epoch", "--event-id", "event", "--kind", "AGENT_SIGNAL_PUBLISHED",
            "--source-timestamp-utc", captured,
            "--metadata", '{"lane_id":"lane","agent_blocked":true,"signal_id":"event"}',
        ]
        with patch("harness_watcher_implementation.__main__.load_config", return_value=config):
            self.assertEqual(0, main(args))
        record = json.loads(producer_path(config.runtime_root, role="subagent", source_id="lane").read_text(encoding="utf-8"))
        self.assertEqual(captured, record["source_timestamp_utc"])

    def test_metadata_file_rejects_missing_malformed_and_non_object_without_writing(self) -> None:
        missing = self.root / "missing.json"
        malformed = self.root / "malformed.json"; malformed.write_text("{", encoding="utf-8")
        non_object = self.root / "array.json"; non_object.write_text("[]", encoding="utf-8")
        config = self._config()
        for path in (missing, malformed, non_object):
            with self.subTest(path=path), patch("harness_watcher_implementation.__main__.load_config", return_value=config):
                self.assertEqual(2, main(self._args("--metadata-file", str(path))))
        self.assertFalse((config.runtime_root / "attention-producers").exists())

    def test_metadata_file_supports_fresh_lane_endpoint_and_invocation_marker(self) -> None:
        config = WatcherConfig(
            self.root, self.root / "readiness", (), attention_logging_enabled=True,
            attention_producers=(("subagent", "lane"), ("orchestrator", "root")),
        )
        lane = self.root / "lane.json"
        lane.write_text('{"lane_id":"20260801-attention-r7:Atlas:A22","agent_blocked":true}', encoding="utf-8")
        invocation = self.root / "invocation.json"
        invocation.write_text(json.dumps({
            "manager_session_id": "root-session",
            "manager_invocation_id": "fresh-invocation-r7",
            "pending_work_snapshot": {"complete": False, "events": [], "selected_event_id": None, "selection_reason": "UNKNOWN"},
        }), encoding="utf-8")
        with patch("harness_watcher_implementation.__main__.load_config", return_value=config):
            self.assertEqual(0, main(self._args("--metadata-file", str(lane))))
            self.assertEqual(0, main([
                "record-attention", "--role", "orchestrator", "--source-id", "root",
                "--epoch-id", "20260801-attention-r7", "--event-id", "root-r7",
                "--kind", "MANAGER_INVOCATION_STARTED", "--metadata-file", str(invocation),
            ]))
        record = json.loads(producer_path(config.runtime_root, role="orchestrator", source_id="root").read_text(encoding="utf-8"))
        self.assertEqual("fresh-invocation-r7", record["manager_invocation_id"])

    def test_metadata_inputs_are_mutually_exclusive(self) -> None:
        metadata_file = self.root / "metadata.json"; metadata_file.write_text("{}", encoding="utf-8")
        with self.assertRaises(SystemExit) as raised:
            main(self._args("--metadata", "{}", "--metadata-file", str(metadata_file)))
        self.assertEqual(2, raised.exception.code)

    def test_orchestrator_can_record_complete_formal_baseline_but_subagent_cannot(self) -> None:
        config=WatcherConfig(self.root,self.root/"baseline",(),attention_logging_enabled=True,attention_producers=(("orchestrator","root"),("subagent","lane")))
        metadata=self.root/"baseline.json"; metadata.write_text(json.dumps({"pending_work_snapshot":{"complete":True,"events":[],"selected_event_id":None,"selection_reason":"FORMAL_REVIEW_BASELINE"}}),encoding="utf-8")
        with patch("harness_watcher_implementation.__main__.load_config",return_value=config):
            self.assertEqual(0,main(["record-attention","--role","orchestrator","--source-id","root","--epoch-id","epoch","--event-id","baseline","--kind","FORMAL_REVIEW_BASELINE_ADVANCED","--metadata-file",str(metadata)]))
            self.assertEqual(2,main(["record-attention","--role","subagent","--source-id","lane","--epoch-id","epoch","--event-id","baseline","--kind","FORMAL_REVIEW_BASELINE_ADVANCED","--metadata-file",str(metadata)]))

    def test_disabled_metadata_file_is_a_no_op_before_reading_it(self) -> None:
        settings.harness_watcher_active = False
        with patch("harness_watcher_implementation.__main__.load_config") as load:
            self.assertEqual(0, main(self._args("--metadata-file", str(self.root / "missing.json"))))
        load.assert_not_called()


if __name__ == "__main__":
    unittest.main()
