from __future__ import annotations
import json, os, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
from harness_watcher_implementation.attention import (
    append_producer_record,
    canonicalize,
    ingest_attention,
    make_source_record,
    producer_path,
    source_digest,
)
from harness_watcher_implementation.config import WatcherConfig


class AttentionIngestionTests(unittest.TestCase):
    def config(self, root):
        return WatcherConfig(
            root, root, (), 1, 10, 1024, (), (), True, (("subagent", "lane"),), 1.0
        )

    def test_partial_burst_replay_and_malformed_isolation(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = self.config(root)
            first = make_source_record(
                recorder="lane",
                epoch_id="epoch",
                event_id="event",
                kind="AGENT_SIGNAL_CREATED",
                metadata={"lane_id": "lane", "agent_blocked": True},
            )
            path = append_producer_record(
                root, role="subagent", source_id="lane", record=first
            )
            path.write_bytes(
                path.read_bytes() + b"{not-json}\n" + b'{"schema":"manager-attention'
            )
            result = ingest_attention(cfg)
            self.assertEqual(result["records"], 1)
            self.assertEqual(result["errors"], 1)
            cursor = json.loads(
                (root / "watcher" / "attention-cursor.json").read_text()
            )
            before = cursor["sources"][str(path)]["offset"]
            self.assertEqual(before, path.stat().st_size)
            self.assertGreater(
                cursor["sources"][str(path)]["partial_bytes"], 0
            )  # incomplete tail is durable
            with path.open("ab") as h:
                h.write(b'-timeline/v1"}\n')
            again = ingest_attention(cfg)
            self.assertEqual(again["records"], 0)
            lines = (
                (root / "watcher" / "attention-timeline.jsonl").read_text().splitlines()
            )
            self.assertEqual(len(lines), 1)
            self.assertTrue((root / "watcher" / "attention-report.json").exists())

    def test_disabled_creates_nothing(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = WatcherConfig(root, root, (), 1, 10, 1024, (), (), False, (), 1.0)
            self.assertTrue(ingest_attention(cfg)["disabled"])
            self.assertFalse((root / "watcher").exists())

    def test_historical_impossible_signal_is_durable_event_level_insufficient(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = self.config(root)
            invalid = make_source_record(
                recorder="lane",
                epoch_id="epoch",
                event_id="atlas",
                kind="AGENT_SIGNAL_CREATED",
                metadata={"lane_id": "lane", "agent_blocked": True},
            )
            invalid["response_deadline_utc"] = "2000-01-01T00:00:00+00:00"
            path = producer_path(root, "subagent", "lane")
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(invalid) + "\n", encoding="utf-8")
            result = ingest_attention(cfg)
            self.assertEqual(0, result["records"])
            self.assertEqual(1, result["errors"])
            self.assertEqual(
                "INSUFFICIENT_EVIDENCE", result["findings"][0]["classification"]
            )
            self.assertEqual(
                ("epoch", "atlas"),
                (result["findings"][0]["epoch_id"], result["findings"][0]["event_id"]),
            )
            self.assertTrue(
                any(
                    "response_deadline_utc precedes" in value
                    for value in result["findings"][0]["contradictory_evidence"]
                )
            )

    def test_corrupt_canonical_timeline_signal_remains_event_level_insufficient(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = self.config(root)
            source = make_source_record(
                recorder="lane",
                epoch_id="epoch",
                event_id="atlas-timeline",
                kind="AGENT_SIGNAL_CREATED",
                metadata={"lane_id": "lane", "agent_blocked": True},
            )
            canonical = canonicalize(
                source,
                observed_timestamp_utc="2026-01-01T00:00:00+00:00",
                source_path="historical",
                source_role="subagent",
                source_id="lane",
                source_generation="0",
                byte_start=0,
                byte_end=1,
            )
            canonical["response_deadline_utc"] = "2000-01-01T00:00:00+00:00"
            source_form = {
                k: v
                for k, v in canonical.items()
                if k
                not in {
                    "observed_timestamp_utc",
                    "source_path",
                    "source_role",
                    "source_id",
                    "source_generation",
                    "byte_start",
                    "byte_end",
                    "source_record_sha256",
                }
            }
            canonical["source_record_sha256"] = source_digest(source_form)
            timeline = root / "watcher" / "attention-timeline.jsonl"
            timeline.parent.mkdir(parents=True)
            timeline.write_text(json.dumps(canonical) + "\n", encoding="utf-8")
            result = ingest_attention(cfg)
            self.assertEqual(
                "INSUFFICIENT_EVIDENCE", result["findings"][0]["classification"]
            )
            self.assertEqual(
                ("epoch", "atlas-timeline"),
                (result["findings"][0]["epoch_id"], result["findings"][0]["event_id"]),
            )
            self.assertTrue(
                any(
                    "response_deadline_utc precedes" in value
                    for value in result["findings"][0]["contradictory_evidence"]
                )
            )

    def test_configured_epoch_excludes_historical_timeline_events_from_report(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = WatcherConfig(
                root,
                root,
                (),
                1,
                10,
                1024,
                (),
                (),
                True,
                (("subagent", "lane"),),
                1.0,
                None,
                "current",
            )
            old = make_source_record(
                recorder="lane",
                epoch_id="old",
                event_id="old-event",
                kind="AGENT_SIGNAL_CREATED",
                metadata={
                    "lane_id": "lane",
                    "agent_blocked": False,
                    "non_blocking": True,
                },
            )
            current = make_source_record(
                recorder="lane",
                epoch_id="current",
                event_id="current-event",
                kind="AGENT_SIGNAL_CREATED",
                metadata={
                    "lane_id": "lane",
                    "agent_blocked": False,
                    "non_blocking": True,
                },
            )
            append_producer_record(root, role="subagent", source_id="lane", record=old)
            append_producer_record(
                root, role="subagent", source_id="lane", record=current
            )
            result = ingest_attention(cfg)
            self.assertEqual(
                ["current-event"], [item["event_id"] for item in result["findings"]]
            )

    def test_restarts_rebuild_dedup_after_timeline_fsync_before_cursor_commit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = self.config(root)
            record = make_source_record(
                recorder="lane",
                epoch_id="epoch",
                event_id="event",
                kind="AGENT_SIGNAL_CREATED",
                metadata={"lane_id": "lane", "agent_blocked": True},
            )
            append_producer_record(
                root, role="subagent", source_id="lane", record=record
            )
            import harness_watcher_implementation.attention as attention

            original = attention._atomic_json

            def crash_before_cursor(path, value):
                if path.name == "attention-cursor.json":
                    raise RuntimeError("simulated crash")
                return original(path, value)

            with patch.object(
                attention, "_atomic_json", side_effect=crash_before_cursor
            ):
                with self.assertRaisesRegex(RuntimeError, "simulated crash"):
                    ingest_attention(cfg)
            first = json.loads(
                (root / "watcher" / "attention-timeline.jsonl")
                .read_text()
                .splitlines()[0]
            )
            recovered = ingest_attention(cfg)
            lines = [
                json.loads(line)
                for line in (root / "watcher" / "attention-timeline.jsonl")
                .read_text()
                .splitlines()
            ]
            self.assertEqual(0, recovered["records"])
            self.assertEqual(1, len(lines))
            self.assertEqual(
                first["observed_timestamp_utc"], lines[0]["observed_timestamp_utc"]
            )

    def test_rotation_discards_old_partial_carry(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = self.config(root)
            path = producer_path(root, "subagent", "lane")
            path.parent.mkdir(parents=True)
            path.write_bytes(b'{"schema":"manager-attention' + b"x" * 10000)
            self.assertEqual(0, ingest_attention(cfg)["records"])
            record = make_source_record(
                recorder="lane",
                epoch_id="epoch",
                event_id="new",
                kind="AGENT_SIGNAL_CREATED",
                metadata={"lane_id": "lane", "agent_blocked": True},
            )
            replacement = path.with_name("replacement.jsonl")
            replacement.write_text(json.dumps(record) + "\n", encoding="utf-8")
            os.replace(replacement, path)
            result = ingest_attention(cfg)
            self.assertEqual(1, result["records"])
            self.assertEqual(0, result["errors"])
            saved = json.loads((root / "watcher" / "attention-cursor.json").read_text())
            self.assertEqual(0, saved["sources"][str(path)]["partial_bytes"])

    def test_same_record_id_with_different_source_bytes_is_a_durable_conflict(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = WatcherConfig(
                root,
                root,
                (),
                1,
                10,
                1024,
                (),
                (),
                True,
                (("subagent", "one"), ("subagent", "two")),
                1.0,
            )
            first = make_source_record(
                recorder="one",
                epoch_id="epoch",
                event_id="event",
                kind="AGENT_SIGNAL_CREATED",
                metadata={"lane_id": "one", "agent_blocked": True},
            )
            second = {**first, "recorder": "two", "lane_id": "two"}
            append_producer_record(root, role="subagent", source_id="one", record=first)
            append_producer_record(
                root, role="subagent", source_id="two", record=second
            )
            result = ingest_attention(cfg)
            self.assertEqual(1, result["records"])
            self.assertEqual(1, result["errors"])
            self.assertEqual(
                1,
                len(
                    (root / "watcher" / "attention-timeline.jsonl")
                    .read_text()
                    .splitlines()
                ),
            )

    def test_torn_timeline_neighbor_and_lost_cursor_preserve_valid_dedup(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = self.config(root)
            record = make_source_record(
                recorder="lane",
                epoch_id="epoch",
                event_id="event",
                kind="AGENT_SIGNAL_CREATED",
                metadata={"lane_id": "lane", "agent_blocked": True},
            )
            append_producer_record(
                root, role="subagent", source_id="lane", record=record
            )
            self.assertEqual(1, ingest_attention(cfg)["records"])
            timeline = root / "watcher" / "attention-timeline.jsonl"
            first = json.loads(timeline.read_text().splitlines()[0])
            with timeline.open("ab") as handle:
                handle.write(b'{"torn":')
            (root / "watcher" / "attention-cursor.json").unlink()
            recovered = ingest_attention(cfg)
            valid = [
                json.loads(line)
                for line in timeline.read_text().splitlines()
                if line.startswith("{") and '"record_id"' in line
            ]
            self.assertEqual(0, recovered["records"])
            self.assertEqual(1, len(valid))
            self.assertEqual(
                first["observed_timestamp_utc"], valid[0]["observed_timestamp_utc"]
            )
            self.assertEqual(1, len(recovered["report"]["observation_errors"]))
            self.assertEqual(0, ingest_attention(cfg)["records"])

    def test_observation_errors_survive_restart_and_fail_affected_event_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = self.config(root)
            path = producer_path(root, "subagent", "lane")
            path.parent.mkdir(parents=True)
            path.write_text(
                json.dumps(
                    {"epoch_id": "epoch", "event_id": "event", "kind": "NOT_A_KIND"}
                )
                + "\n",
                encoding="utf-8",
            )
            record = make_source_record(
                recorder="lane",
                epoch_id="epoch",
                event_id="event",
                kind="AGENT_SIGNAL_CREATED",
                metadata={
                    "lane_id": "lane",
                    "agent_blocked": False,
                    "non_blocking": True,
                },
            )
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record) + "\n")
            first = ingest_attention(cfg)
            self.assertEqual(
                "INSUFFICIENT_EVIDENCE", first["findings"][0]["classification"]
            )
            restarted = ingest_attention(cfg)
            self.assertEqual(
                "INSUFFICIENT_EVIDENCE", restarted["findings"][0]["classification"]
            )
            self.assertEqual(1, len(restarted["report"]["observation_errors"]))

    def test_exact_event_observation_error_remains_after_source_generation_changes(
        self,
    ):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = self.config(root)
            path = producer_path(root, "subagent", "lane")
            path.parent.mkdir(parents=True)
            path.write_text(
                json.dumps(
                    {"epoch_id": "epoch", "event_id": "event", "kind": "NOT_A_KIND"}
                )
                + "\n",
                encoding="utf-8",
            )
            self.assertEqual(1, ingest_attention(cfg)["errors"])
            record = make_source_record(
                recorder="lane",
                epoch_id="epoch",
                event_id="event",
                kind="AGENT_SIGNAL_CREATED",
                metadata={
                    "lane_id": "lane",
                    "agent_blocked": False,
                    "non_blocking": True,
                },
            )
            replacement = path.with_name("next-generation.jsonl")
            replacement.write_text(json.dumps(record) + "\n", encoding="utf-8")
            os.replace(replacement, path)
            result = ingest_attention(cfg)
            self.assertEqual(
                "INSUFFICIENT_EVIDENCE", result["findings"][0]["classification"]
            )


if __name__ == "__main__":
    unittest.main()
