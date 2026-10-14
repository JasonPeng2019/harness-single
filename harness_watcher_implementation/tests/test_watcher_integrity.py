from __future__ import annotations

import base64
import io
import json
import os
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from harness_common.process_identity import exact_process_identity
from harness_watcher_implementation import __main__ as cli
from harness_watcher_implementation import attention, poller
from harness_watcher_implementation.config import ObservedSource, WatcherConfig, load_config
from harness_watcher_implementation.io import atomic_json
from harness_watcher_implementation.logging import log
from harness_watcher_implementation.state import _save as save_state


class WatcherIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source.jsonl"
        self.runtime = self.root / "runtime"
        self.cfg = WatcherConfig(self.root, self.runtime, (self.source,),
                                 observed_sources=(ObservedSource(self.source, "harness", "native"),))
        self.source.write_bytes(b'{"event":"first"}\n')

    def routed(self):
        path = self.runtime / "harness" / "events.jsonl"
        return [json.loads(line) for line in path.read_bytes().splitlines()] if path.exists() else []

    def test_producer_path_derivation_does_not_create_output(self):
        root = self.root / "not-created"
        path = attention.producer_path(root, "subagent", "lane")
        self.assertTrue(path.is_relative_to(root))
        self.assertFalse(root.exists())

    def test_configuration_requires_explicit_epoch_and_observations(self):
        with self.assertRaisesRegex(ValueError, "explicit configuration"):
            load_config()
        product = Path(__file__).resolve().parents[2]
        config = self.root / "watcher.json"
        valid = {"runtime_root": str(self.runtime.relative_to(product)),
                 "observed_sources": [{"path": str(self.source), "role": "harness", "source_id": "native"}]}
        config.write_text(json.dumps(valid), encoding="utf-8")
        selected = load_config(config)
        self.assertEqual(self.runtime, selected.runtime_root)
        self.assertFalse(self.runtime.exists())
        for change in ({"runtime_root": "harness_watcher"}, {"runtime_root": "runtime"},
                       {"runtime_root": str(self.root.relative_to(product))},
                       {"observed_sources": [{"path": str(self.runtime / "input.jsonl"), "role": "harness", "source_id": "native"}]},
                       {"poll_interval_seconds": True}, {"attention_lock_timeout_seconds": float("nan")}):
            with self.subTest(change=change):
                config.write_text(json.dumps({**valid, **change}), encoding="utf-8")
                with self.assertRaises((ValueError, OSError)): load_config(config)
                self.assertFalse(self.runtime.exists())

    def test_missing_status_is_read_only(self):
        self.assertIsNone(cli._read(self.runtime / "watcher" / "service.json"))
        self.assertFalse(self.runtime.exists())

    def test_service_baseline_keeps_an_unfinished_record_until_completion(self):
        self.source.write_bytes(b'{"old":true}\n{"new":')
        self.assertTrue(poller.initialize_service_cursor(self.cfg))
        with self.source.open("ab") as handle: handle.write(b'true}\n')
        poller.poll(self.cfg)
        rows = self.routed()
        self.assertEqual(1, len(rows))
        self.assertEqual([{"new": True}], rows[0]["data"]["records"])

    def test_crlf_records_preserve_their_physical_byte_ranges(self):
        first = b'{"one":true}\r\n'
        second = b'{"two":true}\r\n'
        self.source.write_bytes(first + second)
        poller.poll(self.cfg)
        rows = self.routed()
        self.assertEqual([(0, len(first)), (len(first), len(first + second))],
                         [(row["data"]["byte_start"], row["data"]["byte_end"]) for row in rows])
        self.assertEqual([(0, first[:-1]), (len(first), second[:-1])],
                         list(attention._line_spans(first + second)))

    def test_partial_utf8_record_survives_two_polls_and_restart(self):
        raw = (json.dumps({"event": "snow", "text": "雪"}, ensure_ascii=False) + "\n").encode()
        split = raw.index("雪".encode()) + 1
        self.source.write_bytes(raw[:split])
        poller.poll(self.cfg)
        cursor = self.runtime / "watcher" / "cursor.json"
        self.assertEqual(0, json.loads(cursor.read_bytes())["sources"][str(self.source)]["offset"])
        self.assertEqual([], self.routed())
        with self.source.open("ab") as handle:
            handle.write(raw[split:])
        poller.poll(self.cfg)
        poller.poll(self.cfg)
        self.assertEqual(1, len(self.routed()))
        self.assertEqual("雪", self.routed()[0]["data"]["records"][0]["text"])
        self.assertEqual(len(raw), json.loads(cursor.read_bytes())["sources"][str(self.source)]["offset"])

    def test_growth_during_read_is_left_for_next_sample(self):
        first = self.source.read_bytes()
        extra = b'{"event":"second"}\n'
        real_open = Path.open
        source = self.source

        class GrowingReader:
            def __init__(self, handle): self.handle = handle
            def __enter__(self): return self
            def __exit__(self, *_): self.handle.close()
            def fileno(self): return self.handle.fileno()
            def seek(self, offset): return self.handle.seek(offset)
            def read(self, size):
                with open(source, "ab") as writer: writer.write(extra)
                return self.handle.read(size)

        def open_source(path, *args, **kwargs):
            handle = real_open(path, *args, **kwargs)
            return GrowingReader(handle) if path == source and args == ("rb",) else handle

        with patch.object(Path, "open", open_source):
            item = poller._read(self.source, 65536, {})
        self.assertEqual(first, base64.b64decode(item["raw_b64"]))
        self.assertEqual(len(first), item["next_offset"])
        follow = poller._read(self.source, 65536, {"file_id": item["file_id"], "offset": item["next_offset"]})
        self.assertEqual(extra, base64.b64decode(follow["raw_b64"]))

    def test_output_fsync_then_cursor_failure_replays_without_duplicate(self):
        with patch.object(poller, "_save_cursor", side_effect=OSError("interrupted cursor publication")):
            with self.assertRaisesRegex(OSError, "interrupted"):
                poller.poll(self.cfg)
        first = self.routed()
        self.assertEqual(1, len(first))
        poller.poll(self.cfg)
        self.assertEqual(first, self.routed())

    def test_concurrent_polls_route_one_event(self):
        errors = []
        barrier = threading.Barrier(2)
        def run():
            try:
                barrier.wait(timeout=5)
                poller.poll(self.cfg)
            except BaseException as exc: errors.append(exc)
        threads = [threading.Thread(target=run) for _ in range(2)]
        for thread in threads: thread.start()
        for thread in threads: thread.join(timeout=10)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual([], errors)
        self.assertEqual(1, len(self.routed()))

    def test_corrupt_cursor_is_preserved_and_does_not_reset_history(self):
        path = self.runtime / "watcher" / "cursor.json"
        path.parent.mkdir(parents=True)
        raw = b'{"schema":"harness-watcher-cursor/v2","sources":'
        path.write_bytes(raw)
        with self.assertRaises(ValueError): poller.poll(self.cfg)
        self.assertEqual(raw, path.read_bytes())
        self.assertEqual([], self.routed())

    def test_unreadable_source_preserves_committed_cursor(self):
        poller.poll(self.cfg)
        path = self.runtime / "watcher" / "cursor.json"
        prior = json.loads(path.read_bytes())["sources"][str(self.source)]
        with patch.object(poller, "_read", return_value={"path": str(self.source), "changed": False, "error": "access denied"}):
            poller.poll(self.cfg)
        self.assertEqual(prior, json.loads(path.read_bytes())["sources"][str(self.source)])

    def test_live_stopping_service_is_preserved_without_spawning(self):
        owner = exact_process_identity(os.getpid())
        self.assertIsNotNone(owner)
        service = self.runtime / "watcher" / "service.json"
        state = {"startup_token": "live", "watcher": owner, "owner": owner, "stop_requested": True}
        cli._atomic(service, state)
        raw = service.read_bytes()
        with patch.object(cli, "load_config", return_value=self.cfg), patch.object(cli.subprocess, "Popen") as spawn, patch("sys.stdout", io.StringIO()) as stdout:
            self.assertEqual(0, cli.main(["start", "--owner-pid", str(os.getpid())]))
        spawn.assert_not_called()
        self.assertEqual("stopping", json.loads(stdout.getvalue())["result"])
        self.assertEqual(raw, service.read_bytes())

    def test_unknown_service_is_preserved_without_spawning(self):
        service = self.runtime / "watcher" / "service.json"
        cli._atomic(service, {"startup_token": "unknown", "watcher": {"pid": 123, "created_utc": "windows-filetime:1"}})
        raw = service.read_bytes()
        with patch.object(cli, "load_config", return_value=self.cfg), patch.object(cli, "_same", return_value=False), patch.object(cli, "_gone", return_value=False), patch.object(cli.subprocess, "Popen") as spawn:
            with self.assertRaisesRegex(RuntimeError, "unproven"): cli.main(["start"])
        spawn.assert_not_called()
        self.assertEqual(raw, service.read_bytes())

    @unittest.skipUnless(os.name == "nt", "native Windows junction boundary")
    def test_all_watcher_output_families_refuse_junction_escape(self):
        operations = {
            "log": lambda root: log(root, "watcher", "PROBE", {}),
            "cursor": lambda root: poller._save_cursor(root, {"schema": "harness-watcher-cursor/v2", "sources": {}}),
            "alerts": lambda root: save_state(root, {"schema": "harness-watcher-alerts/v1", "alerts": []}),
            "service": lambda root: cli._atomic(root / "watcher" / "service.json", {"startup_token": "probe"}),
            "attention": lambda root: attention._atomic_json(root / "watcher" / "attention-report.json", {"events": []}),
        }
        for name, operation in operations.items():
            with self.subTest(name=name):
                root = self.root / name
                root.mkdir()
                observed = self.root / (name + "-observed")
                observed.mkdir()
                marker = observed / "marker"
                marker.write_bytes(b"unchanged")
                result = subprocess.run(["cmd", "/c", "mklink", "/J", str(root / "watcher"), str(observed)], capture_output=True, text=True)
                self.assertEqual(0, result.returncode, result.stderr)
                with self.assertRaises((OSError, ValueError)):
                    operation(root)
                self.assertEqual(["marker"], sorted(path.name for path in observed.iterdir()))
                self.assertEqual(b"unchanged", marker.read_bytes())


if __name__ == "__main__":
    unittest.main()
