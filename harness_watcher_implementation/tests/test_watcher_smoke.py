"""Focused contract tests for the optional Harness Watcher.

Some tests deliberately express acceptance requirements which the implementation
does not yet meet.  They are kept as ordinary tests so a repair cannot silently
turn a known defect into an expected failure.
"""

from __future__ import annotations

import io
import importlib
import json
import subprocess
import sys
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from harness_watcher_implementation import settings
from harness_watcher_implementation.config import (
    ObservedSource,
    WatcherConfig,
    load_config,
)
from harness_watcher_implementation.attention import (
    AttentionValidationError,
    make_source_record,
)
from harness_watcher_implementation.evaluator import (
    FakeEvaluator,
    OWNERSHIP_PHASE_RULES,
    STALE_STATUS_QUARANTINE_RULE,
    CommandEvaluator,
    validate_verdict,
)
from harness_watcher_implementation.poller import initialize_service_cursor, poll
from harness_watcher_implementation.state import (
    acknowledge,
    save_alert,
    transition,
    unresolved_alerts,
)
from orchestrator_harness.events import conditions_from_snapshot
from orchestrator_harness.watcher_integration import merge_watcher_conditions
from harness_watcher_implementation import __main__ as watcher_cli


def defect_verdict(path: str = "observed.jsonl") -> dict[str, object]:
    return {
        "defect": True,
        "kind": "loop",
        "severity": "error",
        "summary": "lane repeats a completed stage",
        "implicated": ["atlas"],
        "evidence": [{"path": path, "sha256": "a" * 64, "offset": 0}],
    }


class HarnessWatcherSmokeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.log_file = self.root / "observed.jsonl"
        self.log_file.write_text('{"event":"progress"}\n', encoding="utf-8")
        self.config = WatcherConfig(
            repository_root=self.root,
            runtime_root=self.root / "harness_watcher",
            observed_log_roots=(self.log_file,),
            poll_interval_seconds=300,
            no_progress_seconds=900,
            max_tail_bytes=1024,
            evaluator_enabled=True,
        )
        self.original_active = settings.harness_watcher_active

    def tearDown(self) -> None:
        settings.harness_watcher_active = self.original_active
        self.temp.cleanup()

    def test_diagnostic_only_start_serve_never_runs_evaluator(self) -> None:
        root = Path(__file__).resolve().parents[2]
        token = "diagnostic-owner-path-test"
        runtime = root / "multi-agent-logs" / token
        source = self.root / "changed.jsonl"
        sentinel = self.root / "sentinel"
        source.write_text('{"changed":true}\n')
        config = root / "multi-agent-logs" / (token + ".json")
        try:
            config.parent.mkdir(parents=True, exist_ok=True)
            config.write_text(
                json.dumps(
                    {
                        "runtime_root": str(runtime.relative_to(root)),
                        "observed_sources": [
                            {
                                "path": str(source),
                                "role": "orchestrator",
                                "source_id": "root",
                            }
                        ],
                        "evaluator_enabled": False,
                        "poll_interval_seconds": 1,
                        "evaluator_command": [
                            sys.executable,
                            "-c",
                            f"open(r'{sentinel}','w').write('called')",
                        ],
                    }
                )
            )
            started = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "harness_watcher_implementation",
                    "--config",
                    str(config),
                    "start",
                    "--owner-pid",
                    str(os.getpid()),
                ],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(0, started.returncode, started.stderr)
            service = runtime / "watcher" / "service.json"
            state = json.loads(service.read_text())
            pid = state["watcher"]["pid"]
            self.assertFalse(state["evaluator_enabled"])
            import time

            time.sleep(0.2)
            self.assertFalse(sentinel.exists())
            self.assertTrue((runtime / "watcher" / "cursor.json").exists())
            self.assertIn(
                "EVALUATOR_SKIPPED", (runtime / "watcher" / "events.jsonl").read_text()
            )
            stopped = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "harness_watcher_implementation",
                    "--config",
                    str(config),
                    "stop",
                ],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(0, stopped.returncode)
            for _ in range(30):
                status = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "harness_watcher_implementation",
                        "--config",
                        str(config),
                        "status",
                    ],
                    cwd=root,
                    capture_output=True,
                    text=True,
                )
                if not json.loads(status.stdout)["running"]:
                    break
                time.sleep(0.1)
            self.assertFalse(json.loads(status.stdout)["running"])
            self.assertNotIn(
                pid,
                [
                    p.pid
                    for p in __import__(
                        "orchestrator_harness.processes", fromlist=["process_snapshot"]
                    )
                    .process_snapshot()
                    .processes
                ],
            )
        finally:
            import shutil

            shutil.rmtree(runtime, ignore_errors=True)
            config.unlink(missing_ok=True)

    def test_poll_mid_tail_partial_oversized_line_routes_following_valid_line(
        self,
    ) -> None:
        oversized = json.dumps({"payload": "x" * 5000}) + "\n"
        valid = '{"ok":true}\n'
        self.log_file.write_text(oversized + valid, encoding="utf-8")
        cfg = WatcherConfig(
            self.root,
            self.root / "runtime",
            (self.log_file,),
            300,
            900,
            len(valid) + 100,
            evaluator_enabled=False,
        )
        poll(cfg, None)
        self.assertEqual(
            self.log_file.stat().st_size,
            json.loads((cfg.runtime_root / "watcher" / "cursor.json").read_text())[
                "sources"
            ][str(self.log_file)]["offset"],
        )
        watcher = (cfg.runtime_root / "watcher" / "events.jsonl").read_text()
        self.assertIn("malformed json record", watcher)
        self.assertTrue(
            any(
                "PROGRESS_INGESTED" in path.read_text()
                for path in (cfg.runtime_root / "subagents").rglob("events.jsonl")
            )
        )

    def test_route_oversized_raw_byte_range_and_following_record(self) -> None:
        import hashlib
        from harness_watcher_implementation.poller import _route

        huge = {"payload": "x" * 70000}
        raw = "1\n" + json.dumps(huge) + "\n" + json.dumps({"ok": True}) + "\n"
        item = {"changed": True, "text": raw, "path": str(self.log_file), "offset": 17}
        cfg = WatcherConfig(
            self.root,
            self.root / "runtime",
            (),
            300,
            900,
            200000,
            evaluator_enabled=False,
        )
        _route(cfg, item)
        entries = [
            json.loads(line)
            for line in (cfg.runtime_root / "watcher" / "events.jsonl")
            .read_text()
            .splitlines()
        ]
        rejected = next(
            x["data"]
            for x in entries
            if x["event_type"] == "ROUTE_REJECTED"
            and x["data"].get("reason") == "record exceeds route limit"
        )
        huge_raw = (json.dumps(huge) + "\n").encode()
        self.assertEqual(19, rejected["byte_start"])
        self.assertEqual(19 + len(huge_raw), rejected["byte_end"])
        self.assertEqual(len(huge_raw), rejected["encoded_size"])
        self.assertEqual(hashlib.sha256(huge_raw).hexdigest(), rejected["sha256"])
        self.assertEqual("raw_line_utf8", rejected["digest_scope"])
        self.assertTrue(
            any(
                "PROGRESS_INGESTED" in path.read_text()
                for path in (cfg.runtime_root / "subagents").rglob("events.jsonl")
            )
        )

    def test_reserved_provenance_rejected_by_append_and_canonicalize(self) -> None:
        from harness_watcher_implementation.attention import (
            append_producer_record,
            canonicalize,
        )

        base = make_source_record(
            recorder="lane",
            epoch_id="e",
            event_id="x",
            kind="AGENT_SIGNAL_CREATED",
            metadata={"lane_id": "l", "agent_blocked": True},
        )
        for key in (
            "observed_timestamp_utc",
            "source_path",
            "source_role",
            "source_id",
            "source_generation",
            "byte_start",
            "byte_end",
            "source_record_sha256",
        ):
            forged = {**base, key: "bad"}
            with self.subTest(key=key), self.assertRaises(AttentionValidationError):
                append_producer_record(
                    self.root, role="subagent", source_id="lane", record=forged
                )
            with self.subTest(key=key), self.assertRaises(AttentionValidationError):
                canonicalize(
                    forged,
                    observed_timestamp_utc="2026-01-01T00:00:00+00:00",
                    source_path="p",
                    source_role="subagent",
                    source_id="lane",
                    source_generation="g",
                    byte_start=0,
                    byte_end=1,
                )

    def test_route_rejects_scalar_and_oversized_without_stopping_polling(self) -> None:
        self.log_file.write_text(
            "1\n" + json.dumps({"payload": "x" * 70000}) + "\n", encoding="utf-8"
        )
        cfg = WatcherConfig(
            self.root,
            self.root / "runtime",
            (self.log_file,),
            300,
            900,
            200000,
            evaluator_enabled=False,
        )
        first = poll(cfg, None)
        self.assertTrue(first["skipped"])
        events = (cfg.runtime_root / "watcher" / "events.jsonl").read_text(
            encoding="utf-8"
        )
        self.assertIn("json record is not an object", events)
        self.assertIn("record exceeds route limit", events)
        self.assertIn("encoded_size", events)
        self.assertIn("sha256", events)
        with self.log_file.open("a", encoding="utf-8") as handle:
            handle.write('{"ok":true}\n')
        self.assertTrue(poll(cfg, None)["skipped"])

    def test_reserved_provenance_metadata_is_rejected(self) -> None:
        for key in (
            "observed_timestamp_utc",
            "source_path",
            "source_role",
            "source_id",
            "source_generation",
            "byte_start",
            "byte_end",
            "source_record_sha256",
        ):
            with self.subTest(key=key), self.assertRaises(AttentionValidationError):
                make_source_record(
                    recorder="lane",
                    epoch_id="e",
                    event_id="x",
                    kind="AGENT_SIGNAL_CREATED",
                    metadata={"lane_id": "l", "agent_blocked": True, key: "bad"},
                )

    def test_diagnostic_only_poll_reads_and_never_invokes_evaluator(self) -> None:
        sentinel = self.root / "evaluator-sentinel"

        class SentinelEvaluator:
            def evaluate(self, packet):
                sentinel.write_text("called", encoding="utf-8")
                raise AssertionError("evaluator must not run")

        self.config = WatcherConfig(
            self.root,
            self.root / "harness_watcher",
            (self.log_file,),
            300,
            900,
            1024,
            evaluator_enabled=False,
        )
        result = poll(self.config, SentinelEvaluator())
        self.assertTrue(result["skipped"])
        self.assertEqual("diagnostic-only", result["mode"])
        self.assertIsNone(result["alert"])
        self.assertFalse(sentinel.exists())
        self.assertTrue((self.config.runtime_root / "watcher" / "cursor.json").exists())
        events = (self.config.runtime_root / "watcher" / "events.jsonl").read_text(
            encoding="utf-8"
        )
        self.assertIn('"event_type":"EVALUATOR_SKIPPED"', events)
        self.assertIn('"evaluator_enabled":false', events)

    def test_evaluator_enabled_requires_explicit_command_and_identity(
        self,
    ) -> None:
        config = self.root / "watcher.json"
        config.write_text(json.dumps({"runtime_root": "runtime"}), encoding="utf-8")
        self.assertFalse(load_config(config).evaluator_enabled)
        config.write_text(
            json.dumps({"runtime_root": "runtime", "evaluator_enabled": False}),
            encoding="utf-8",
        )
        self.assertFalse(load_config(config).evaluator_enabled)
        config.write_text(
            json.dumps({"runtime_root": "runtime", "evaluator_enabled": True}),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "evaluator_command is required"):
            load_config(config)
        config.write_text(
            json.dumps(
                {
                    "runtime_root": "runtime",
                    "evaluator_enabled": True,
                    "evaluator_command": ["configured-provider", "--configured"],
                    "evaluator_identity": {
                        "provider": "configured-provider",
                        "model": "configured-model",
                        "effort": "configured-effort",
                    },
                }
            ),
            encoding="utf-8",
        )
        loaded = load_config(config)
        self.assertTrue(loaded.evaluator_enabled)
        self.assertEqual(
            {
                "provider": "configured-provider",
                "model": "configured-model",
                "effort": "configured-effort",
            },
            dict(loaded.evaluator_identity),
        )
        config.write_text(
            json.dumps({"runtime_root": "runtime", "evaluator_enabled": "true"}),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "evaluator_enabled must be boolean"):
            load_config(config)

    def test_enabled_healthy_poll_is_passive_and_logs_only_watcher_activity(
        self,
    ) -> None:
        result = poll(self.config, FakeEvaluator())
        self.assertIsNone(result["alert"])
        self.assertTrue(
            (self.config.runtime_root / "watcher" / "events.jsonl").is_file()
        )
        self.assertFalse((self.config.runtime_root / "orchestrator").exists())
        self.assertFalse((self.config.runtime_root / "harness").exists())

    def test_rejected_evaluator_verdict_is_durable_and_next_poll_continues(
        self,
    ) -> None:
        class InvalidThenHealthyEvaluator:
            def __init__(self) -> None:
                self.calls = 0

            def evaluate(self, packet):
                self.calls += 1
                if self.calls == 1:
                    raise ValueError("defect requires supported kind and evidence")
                return FakeEvaluator().evaluate(packet)

        evaluator = InvalidThenHealthyEvaluator()
        first = poll(self.config, evaluator)
        self.assertEqual(
            "defect requires supported kind and evidence", first["evaluation_error"]
        )
        events = (self.config.runtime_root / "watcher" / "events.jsonl").read_text(
            encoding="utf-8"
        )
        self.assertIn('"event_type":"EVALUATOR_REJECTED"', events)
        with self.log_file.open("a", encoding="utf-8") as handle:
            handle.write('{"event":"next-progress"}\n')
        second = poll(self.config, evaluator)
        self.assertIn("verdict", second)
        self.assertEqual(2, evaluator.calls)

    def test_historical_ordinary_event_remains_visible_to_evaluator(self) -> None:
        class CapturingEvaluator:
            def __init__(self) -> None:
                self.packet = None

            def evaluate(self, packet):
                self.packet = packet
                return FakeEvaluator().evaluate(packet)

        historical = {
            "type": "MANAGER_SIGNAL",
            "event_id": "sig-20260731-long-canary-atlas-a22-s1-prepare-b14-20260731T140114Z",
            "data": {"lane_id": "20260731-s1-clean:Atlas:A22"},
        }
        self.log_file.write_text(json.dumps(historical) + "\n", encoding="utf-8")
        evaluator = CapturingEvaluator()
        poll(self.config, evaluator)
        self.assertIn(
            "sig-20260731-long-canary", evaluator.packet["observations"][0]["text"]
        )

    def test_harness_attention_append_is_ingested_without_evaluator_work(self) -> None:
        class CountingEvaluator:
            def __init__(self) -> None:
                self.calls = 0

            def evaluate(self, packet):
                self.calls += 1
                return FakeEvaluator().evaluate(packet)

        attention_path = self.root / "attention-events.jsonl"
        record = make_source_record(
            recorder="orchestrator_harness",
            epoch_id="epoch",
            event_id="signal-1",
            kind="HARNESS_SIGNAL_OBSERVED",
            metadata={"response_deadline_utc": "2026-08-01T00:02:00+00:00"},
        )
        attention_path.write_text(json.dumps(record) + "\n", encoding="utf-8")
        self.config = WatcherConfig(
            self.root,
            self.root / "harness_watcher",
            (attention_path,),
            300,
            900,
            1024,
            (),
            (ObservedSource(attention_path, "harness", "orchestrator_harness"),),
            True,
        )
        evaluator = CountingEvaluator()
        result = poll(self.config, evaluator)
        self.assertTrue(result["skipped"])
        self.assertEqual(0, evaluator.calls)
        self.assertTrue(
            (
                self.config.runtime_root / "watcher" / "attention-timeline.jsonl"
            ).is_file()
        )

    def test_default_off_does_not_create_attention_outputs(self) -> None:
        self.config = WatcherConfig(
            self.root,
            self.root / "harness_watcher",
            (self.log_file,),
            300,
            900,
            1024,
            evaluator_enabled=False,
        )
        result = poll(self.config, FakeEvaluator())
        self.assertFalse(result["packet"]["attention_findings"])
        self.assertFalse(
            (self.config.runtime_root / "watcher" / "attention-timeline.jsonl").exists()
        )

    def test_primary_identity_terminal_is_logged_once_without_evaluator_work(
        self,
    ) -> None:
        self.log_file.write_text("", encoding="utf-8")
        identity = self.root / "primary.json"
        identity.write_text(
            json.dumps(
                {
                    "owner": {},
                    "managed_owner": {},
                    "managed_watcher": {},
                    "exit_reason": "watcher-exited",
                }
            ),
            encoding="utf-8",
        )
        self.config = WatcherConfig(
            self.root,
            self.root / "harness_watcher",
            (self.log_file,),
            300,
            900,
            1024,
            (),
            (),
            False,
            (),
            5.0,
            identity,
        )

        class CountingEvaluator:
            def __init__(self):
                self.calls = 0

            def evaluate(self, packet):
                self.calls += 1
                return FakeEvaluator().evaluate(packet)

        evaluator = CountingEvaluator()
        self.assertEqual(
            "primary-terminal",
            poll(self.config, evaluator)["primary_harness_loss"]["reason"],
        )
        self.assertTrue(poll(self.config, evaluator)["skipped"])
        self.assertEqual(0, evaluator.calls)
        events = (self.config.runtime_root / "watcher" / "events.jsonl").read_text(
            encoding="utf-8"
        )
        self.assertEqual(1, events.count("PRIMARY_HARNESS_LOST"))

    def test_primary_identity_healthy_mismatch_and_incomplete_are_distinct(
        self,
    ) -> None:
        from datetime import datetime, timezone
        from orchestrator_harness.models import ProcessInfo, ProcessSnapshot
        import harness_watcher_implementation.poller as poller

        identity = self.root / "primary.json"
        stamp = "2026-01-01T00:00:00+00:00"
        identity.write_text(
            json.dumps(
                {
                    key: {"pid": pid, "created_utc": stamp}
                    for key, pid in (
                        ("owner", 1),
                        ("managed_owner", 2),
                        ("managed_watcher", 3),
                    )
                }
            ),
            encoding="utf-8",
        )
        cfg = WatcherConfig(
            self.root,
            self.root / "runtime",
            (),
            300,
            900,
            1024,
            (),
            (),
            False,
            (),
            5.0,
            identity,
        )
        processes = tuple(
            ProcessInfo(pid, 0, "", "", datetime(2026, 1, 1, tzinfo=timezone.utc))
            for pid in (1, 2, 3)
        )
        with patch(
            "orchestrator_harness.processes.process_snapshot",
            return_value=ProcessSnapshot(True, processes),
        ):
            self.assertIsNone(poller._primary_loss(cfg, {})[0])
        with patch(
            "orchestrator_harness.processes.process_snapshot",
            return_value=ProcessSnapshot(True, processes[:2]),
        ):
            self.assertEqual(
                "identity-mismatch", poller._primary_loss(cfg, {})[0]["reason"]
            )
        with patch(
            "orchestrator_harness.processes.process_snapshot",
            return_value=ProcessSnapshot(False, ()),
        ):
            self.assertEqual(
                "process-coverage-unknown", poller._primary_loss(cfg, {})[0]["reason"]
            )

    def test_serve_preflight_failure_logs_diagnostic_before_terminal_state(
        self,
    ) -> None:
        runner = importlib.import_module("harness_watcher_implementation.__main__")
        with (
            patch.object(runner, "load_config", return_value=self.config),
            patch.object(runner.time, "sleep"),
        ):
            self.assertEqual(
                1, runner.main(["serve", "--startup-token", "missing-state"])
            )
        events = (self.config.runtime_root / "watcher" / "events.jsonl").read_text(
            encoding="utf-8"
        )
        self.assertIn('"event_type":"SERVICE_ERROR"', events)
        self.assertIn("startup state was not published", events)

    def test_serve_survives_rejected_evaluator_and_completes_later_poll(self) -> None:
        runner = importlib.import_module("harness_watcher_implementation.__main__")
        service = self.config.runtime_root / "watcher" / "service.json"
        owner = {"pid": 11, "created_utc": "owner"}
        runner._atomic(
            service,
            {
                "startup_token": "token",
                "owner": owner,
                "watcher": None,
                "stop_requested": False,
                "exit_reason": None,
            },
        )

        class InvalidThenHealthy:
            def __init__(self, command) -> None:
                self.calls = 0

            def evaluate(self, packet):
                self.calls += 1
                if self.calls == 1:
                    raise ValueError("defect requires supported kind and evidence")
                return FakeEvaluator().evaluate(packet)

        evaluator = InvalidThenHealthy(())
        sleeps = 0

        def sleep_until(*_args):
            nonlocal sleeps
            sleeps += 1
            events = (self.config.runtime_root / "watcher" / "events.jsonl").read_text(
                encoding="utf-8"
            )
            if sleeps == 1:
                self.assertIn('"event_type":"EVALUATOR_REJECTED"', events)
                self.assertNotIn('"event_type":"EVALUATOR_OUTCOME"', events)
                self.assertNotIn('"event_type":"SERVICE_ERROR"', events)
                self.assertIsNone(
                    json.loads(service.read_text(encoding="utf-8"))["exit_reason"]
                )
                with self.log_file.open("a", encoding="utf-8") as handle:
                    handle.write('{"event":"next-progress"}\n')
                return "poll"
            return "stop-requested"

        with (
            patch.object(runner, "load_config", return_value=self.config),
            patch.object(runner, "_same", return_value=True),
            patch.object(
                runner, "_identity", return_value={"pid": 12, "created_utc": "watcher"}
            ),
            patch.object(runner, "initialize_service_cursor"),
            patch.object(runner, "CommandEvaluator", return_value=evaluator),
            patch.object(runner, "_sleep_until", side_effect=sleep_until),
        ):
            self.assertEqual(0, runner.main(["serve", "--startup-token", "token"]))
        events = (self.config.runtime_root / "watcher" / "events.jsonl").read_text(
            encoding="utf-8"
        )
        self.assertEqual(1, events.count('"event_type":"EVALUATOR_REJECTED"'))
        self.assertEqual(1, events.count('"event_type":"EVALUATOR_OUTCOME"'))
        self.assertNotIn('"event_type":"SERVICE_ERROR"', events)
        self.assertEqual(2, evaluator.calls)

    def test_stale_serve_terminal_cannot_overwrite_newer_startup_state(self) -> None:
        runner = importlib.import_module("harness_watcher_implementation.__main__")
        service = self.config.runtime_root / "watcher" / "service.json"
        newer = {
            "startup_token": "new",
            "watcher": {"pid": 2, "created_utc": "new"},
            "exit_reason": None,
        }
        runner._atomic(service, newer)
        self.assertFalse(
            runner._terminal(
                service,
                self.config.runtime_root,
                {"startup_token": "old"},
                "service-error",
                "old",
                None,
            )
        )
        self.assertEqual(newer, json.loads(service.read_text(encoding="utf-8")))

    def test_mismatched_watcher_terminal_cannot_overwrite_retry_state(self) -> None:
        runner = importlib.import_module("harness_watcher_implementation.__main__")
        service = self.config.runtime_root / "watcher" / "service.json"
        current = {
            "startup_token": "same",
            "watcher": {"pid": 2, "created_utc": "new"},
            "exit_reason": None,
        }
        runner._atomic(service, current)
        old_watcher = {"pid": 1, "created_utc": "old"}
        self.assertFalse(
            runner._terminal(
                service,
                self.config.runtime_root,
                {"startup_token": "same"},
                "service-error",
                "same",
                old_watcher,
            )
        )
        self.assertEqual(current, json.loads(service.read_text(encoding="utf-8")))

    def test_start_failure_reaps_only_its_spawned_child(self) -> None:
        runner = importlib.import_module("harness_watcher_implementation.__main__")

        class Child:
            def __init__(self) -> None:
                self.terminated = 0
                self.killed = 0
                self.waits = 0

            def poll(self):
                return None

            def terminate(self):
                self.terminated += 1

            def kill(self):
                self.killed += 1

            def wait(self, *, timeout):
                self.waits += 1
                return 0

        child = Child()
        runner._terminate_reap(child)
        self.assertEqual((1, 0, 1), (child.terminated, child.killed, child.waits))

    def test_windows_start_launch_flags_keep_no_window_without_breakaway(
        self,
    ) -> None:
        runner = importlib.import_module("harness_watcher_implementation.__main__")
        no_window = 0x08000000
        breakaway = 0x01000000
        captured: dict[str, int] = {}
        owner = {"pid": 4242, "created_utc": "owner-utc"}
        watcher = {"pid": 4243, "created_utc": "watcher-utc"}
        service = self.config.runtime_root / "watcher" / "service.json"
        read_calls = 0

        class Child:
            def poll(self):
                return None

        def fake_popen(*args, **kwargs):
            captured["creationflags"] = kwargs.get("creationflags", 0)
            return Child()

        def fake_identity(pid):
            return {owner["pid"]: owner, watcher["pid"]: watcher}.get(pid)

        def fake_read(path):
            nonlocal read_calls
            read_calls += 1
            if read_calls == 1:
                return None
            state = json.loads(path.read_text(encoding="utf-8"))
            state["startup_status"] = "READY"
            state["watcher"] = watcher
            return state

        with (
            patch.object(runner.os, "name", "nt"),
            patch.object(
                runner.subprocess, "CREATE_NO_WINDOW", no_window, create=True
            ),
            patch.object(
                runner.subprocess, "CREATE_BREAKAWAY_FROM_JOB", breakaway, create=True
            ),
            patch.object(runner, "load_config", return_value=self.config),
            patch.object(runner, "_identity", side_effect=fake_identity),
            patch.object(runner, "_read", side_effect=fake_read),
            patch.object(runner.time, "sleep"),
            patch.object(runner.subprocess, "Popen", side_effect=fake_popen),
        ):
            result = runner.main(["start", "--owner-pid", str(owner["pid"])])
        self.assertEqual(0, result)
        flags = captured["creationflags"]
        self.assertEqual(no_window, flags & no_window)
        self.assertEqual(0, flags & breakaway)

    def test_new_service_baseline_skips_preexisting_defect_shaped_bytes(self) -> None:
        class CountingEvaluator:
            def __init__(self):
                self.packets = []

            def evaluate(self, packet):
                self.packets.append(packet)
                return FakeEvaluator().evaluate(packet)

        self.log_file.write_text(
            json.dumps({"type": "STALE_STATUS", "identity": "lane:historical:process"})
            + "\n",
            encoding="utf-8",
        )
        evaluator = CountingEvaluator()

        self.assertTrue(initialize_service_cursor(self.config))
        result = poll(self.config, evaluator)

        self.assertTrue(result["skipped"])
        self.assertIsNone(result["alert"])
        self.assertEqual([], evaluator.packets)

    def test_service_baseline_evaluates_and_alerts_for_post_baseline_append(
        self,
    ) -> None:
        class DefectEvaluator:
            def __init__(self):
                self.packets = []

            def evaluate(self, packet):
                self.packets.append(packet)
                observed = packet["observations"][0]
                return validate_verdict(
                    {
                        "defect": True,
                        "kind": "loop",
                        "severity": "error",
                        "summary": "new loop",
                        "implicated": ["atlas"],
                        "evidence": [
                            {
                                "path": observed["path"],
                                "sha256": observed["sha256"],
                                "offset": observed["offset"],
                            }
                        ],
                    },
                    packet=packet,
                )

        evaluator = DefectEvaluator()

        initialize_service_cursor(self.config)
        with self.log_file.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {"type": "LANE_PROGRESS", "identity": "lane:current:process"}
                )
                + "\n"
            )
        result = poll(self.config, evaluator)

        self.assertEqual(1, len(evaluator.packets))
        self.assertIsNotNone(result["alert"])

    def test_service_baseline_does_not_advance_an_existing_cursor_or_skip_unread_bytes(
        self,
    ) -> None:
        cursor_path = self.config.runtime_root / "watcher" / "cursor.json"
        cursor_path.parent.mkdir(parents=True)
        stat = self.log_file.stat()
        cursor_path.write_text(
            json.dumps(
                {
                    "schema": "harness-watcher-cursor/v2",
                    "sources": {
                        str(self.log_file): {
                            "file_id": [stat.st_dev, stat.st_ino],
                            "offset": 0,
                            "generation": None,
                            "context": {},
                            "last_changed_epoch": 0,
                            "last_changed_utc": None,
                            "no_progress_evaluated_generation": None,
                        }
                    },
                }
            ),
            encoding="utf-8",
        )

        class CapturingEvaluator:
            def __init__(self):
                self.packets = []

            def evaluate(self, packet):
                self.packets.append(packet)
                return FakeEvaluator().evaluate(packet)

        evaluator = CapturingEvaluator()

        self.assertFalse(initialize_service_cursor(self.config))
        cursor = json.loads(cursor_path.read_text(encoding="utf-8"))
        self.assertEqual(0, cursor["sources"][str(self.log_file)]["offset"])
        poll(self.config, evaluator)
        self.assertEqual(1, len(evaluator.packets))
        self.assertIn("progress", evaluator.packets[0]["observations"][0]["text"])

    def test_source_created_after_service_baseline_is_evaluated_normally(self) -> None:
        late = self.root / "late.jsonl"
        config = WatcherConfig(
            self.root,
            self.config.runtime_root,
            (late,),
            max_tail_bytes=1024,
            evaluator_enabled=True,
        )

        class CapturingEvaluator:
            def __init__(self):
                self.packets = []

            def evaluate(self, packet):
                self.packets.append(packet)
                return FakeEvaluator().evaluate(packet)

        evaluator = CapturingEvaluator()

        initialize_service_cursor(config)
        late.write_text('{"event":"current"}\n', encoding="utf-8")
        poll(config, evaluator)

        self.assertEqual(1, len(evaluator.packets))
        self.assertIn("current", evaluator.packets[0]["observations"][0]["text"])

    def test_prebaseline_bytes_never_become_no_progress_context(self) -> None:
        class CountingEvaluator:
            def __init__(self):
                self.packets = []

            def evaluate(self, packet):
                self.packets.append(packet)
                return FakeEvaluator().evaluate(packet)

        evaluator = CountingEvaluator()

        initialize_service_cursor(self.config)
        cursor_path = self.config.runtime_root / "watcher" / "cursor.json"
        cursor = json.loads(cursor_path.read_text(encoding="utf-8"))
        cursor["sources"][str(self.log_file)]["last_changed_epoch"] = 0
        cursor_path.write_text(json.dumps(cursor), encoding="utf-8")
        result = poll(self.config, evaluator)

        self.assertTrue(result["skipped"])
        self.assertEqual([], evaluator.packets)
        saved = json.loads(cursor_path.read_text(encoding="utf-8"))["sources"][
            str(self.log_file)
        ]
        self.assertNotIn("text", saved["context"])

    def test_direct_poll_still_reads_preexisting_bytes_for_diagnostics(self) -> None:
        class CapturingEvaluator:
            def __init__(self):
                self.packets = []

            def evaluate(self, packet):
                self.packets.append(packet)
                return FakeEvaluator().evaluate(packet)

        evaluator = CapturingEvaluator()

        poll(self.config, evaluator)

        self.assertEqual(1, len(evaluator.packets))
        self.assertIn("progress", evaluator.packets[0]["observations"][0]["text"])

    def test_disabled_cli_poll_is_a_noop(self) -> None:
        config_path = self.root / "config.json"
        config_path.write_text(
            json.dumps({"runtime_root": "harness_watcher"}), encoding="utf-8"
        )
        settings.harness_watcher_active = False
        output = io.StringIO()
        with patch("sys.stdout", output):
            self.assertEqual(
                0,
                __import__(
                    "harness_watcher_implementation.__main__", fromlist=["main"]
                ).main(["--config", str(config_path), "poll"]),
            )
        self.assertIn("disabled-no-op", output.getvalue())
        self.assertFalse((self.root / "harness_watcher").exists())

    def test_disabled_public_poller_api_is_also_a_noop(self) -> None:
        """Feature-off must cover integrations, not merely one CLI entry point."""
        settings.harness_watcher_active = False
        result = poll(self.config, FakeEvaluator())
        self.assertIsNone(result["alert"])
        self.assertFalse(
            self.config.runtime_root.exists(),
            "disabled poll must not create watcher logs/state",
        )

    def test_strict_verdict_rejects_evidence_not_present_in_review_packet(self) -> None:
        """A model must not manufacture an alert by citing an unobserved path/hash."""
        packet = {
            "observations": [
                {
                    "path": str(self.log_file),
                    "sha256": "b" * 64,
                    "offset": 0,
                    "text": "progress",
                }
            ]
        }
        with self.assertRaisesRegex(ValueError, "evidence"):
            validate_verdict(defect_verdict("invented.jsonl"), packet=packet)

    def test_legacy_default_source_uses_current_multi_agent_logs_owner(self) -> None:
        cfg = load_config()
        self.assertEqual(
            (
                Path.cwd()
                / "multi-agent-logs"
                / "orchestrator-harness"
                / "current"
                / "events.jsonl"
            ).resolve(),
            cfg.observed_log_roots[0],
        )

    def test_default_config_has_no_evaluator_launch_preferences(self) -> None:
        cfg = load_config()
        self.assertFalse(cfg.evaluator_enabled)
        self.assertEqual((), cfg.evaluator_command)
        self.assertEqual((), cfg.evaluator_identity)

    def test_example_config_does_not_choose_an_evaluator(self) -> None:
        cfg = load_config("harness_watcher_implementation/config.example.json")
        self.assertFalse(cfg.evaluator_enabled)
        self.assertEqual((), cfg.evaluator_command)
        self.assertEqual((), cfg.evaluator_identity)
        self.assertTrue(
            {"orchestrator", "harness", "subagent"}.issubset(
                {item.role for item in cfg.observed_sources}
            ),
            "production example must declare manager, harness, and lane sources so a real epoch can populate all four trees",
        )

    def test_alert_is_deduplicated_then_resolved_in_strict_order(self) -> None:
        verdict = validate_verdict(defect_verdict(str(self.log_file)))
        first = save_alert(self.config.runtime_root, verdict, {"packet_id": "packet-1"})
        second = save_alert(
            self.config.runtime_root, verdict, {"packet_id": "packet-2"}
        )
        self.assertEqual(first["alert_id"], second["alert_id"])
        self.assertTrue(acknowledge(self.config.runtime_root, first["event_id"]))
        for state in (
            "STOP_ASSIGNING",
            "CHECKPOINT_REQUESTED",
            "PAUSED",
            "REPAIRED",
            "RESUMED",
            "RESOLVED",
        ):
            transition(self.config.runtime_root, first["alert_id"], state)
        self.assertEqual([], unresolved_alerts(self.config.runtime_root))

    def test_recovery_rejects_out_of_order_transition(self) -> None:
        alert = save_alert(
            self.config.runtime_root,
            validate_verdict(defect_verdict(str(self.log_file))),
            {"packet_id": "packet"},
        )
        with self.assertRaisesRegex(ValueError, "out of order"):
            transition(self.config.runtime_root, alert["alert_id"], "PAUSED")

    def test_enabled_watcher_alert_never_enters_native_conditions(self) -> None:
        snapshot = {
            "lanes": [],
            "requests": [],
            "process_snapshot_complete": True,
        }
        watcher_alert = {
            "alert_id": "hwa-1",
            "event_id": "hwa-event-1",
            "severity": "error",
            "summary": "meaningful loop",
            "implicated": ["atlas"],
            "evidence": [],
        }
        with (
            patch(
                "harness_watcher_implementation.settings.harness_watcher_active", True
            ),
            patch(
                "harness_watcher_implementation.state.unresolved_alerts",
                return_value=[watcher_alert],
            ),
            patch("harness_watcher_implementation.logging.log"),
        ):
            conditions = merge_watcher_conditions(snapshot)
        self.assertEqual(conditions_from_snapshot(snapshot), conditions)
        self.assertEqual({}, conditions)
        self.assertNotIn(
            "HARNESS_WATCHER_ALERT",
            {condition["type"] for condition in conditions.values()},
        )

    def test_disabled_integration_returns_exact_original_conditions(self) -> None:
        snapshot = {"lanes": [], "requests": []}
        settings.harness_watcher_active = False
        self.assertEqual(
            conditions_from_snapshot(snapshot), merge_watcher_conditions(snapshot)
        )

    def test_watcher_cli_exposes_no_ack_command_or_acknowledgement_api(self) -> None:
        """The diagnostic CLI owns no manager queue or acknowledgement policy."""
        self.assertFalse(hasattr(watcher_cli, "ack_command"))
        self.assertFalse(hasattr(watcher_cli, "acknowledge_watcher_event"))

    def test_poll_routes_new_manager_harness_and_lane_records_to_four_separate_trees(
        self,
    ) -> None:
        manager = self.root / "manager-events.jsonl"
        harness = self.root / "harness-events.jsonl"
        lane = self.root / "lane-progress.jsonl"
        manager.write_text('{"event":"manager-heartbeat"}\n', encoding="utf-8")
        harness.write_text('{"event":"harness-observation"}\n', encoding="utf-8")
        lane.write_text('{"doer":"Atlas/one","stage":"build"}\n', encoding="utf-8")
        cfg = WatcherConfig(
            self.root,
            self.config.runtime_root,
            (manager, harness, lane),
            max_tail_bytes=1024,
        )
        poll(cfg, FakeEvaluator())
        self.assertTrue((cfg.runtime_root / "orchestrator" / "events.jsonl").is_file())
        self.assertTrue((cfg.runtime_root / "harness" / "events.jsonl").is_file())
        self.assertTrue((cfg.runtime_root / "watcher" / "events.jsonl").is_file())
        self.assertTrue(
            (cfg.runtime_root / "subagents" / "Atlas_one" / "events.jsonl").is_file()
        )

    def test_cursor_preserves_last_change_context_and_does_not_represent_unchanged_bytes_as_new_work(
        self,
    ) -> None:
        class CountingEvaluator:
            def __init__(self):
                self.packets = []

            def evaluate(self, packet):
                self.packets.append(packet)
                return FakeEvaluator().evaluate(packet)

        evaluator = CountingEvaluator()
        poll(self.config, evaluator)
        poll(self.config, evaluator)
        cursor = json.loads(
            (self.config.runtime_root / "watcher" / "cursor.json").read_text(
                encoding="utf-8"
            )
        )
        source = cursor["sources"][str(self.log_file)]
        self.assertIn("last_changed_utc", source)
        self.assertIn(
            "elapsed_since_change_seconds",
            evaluator.packets[-1],
            "Terra packet needs explicit no-progress age",
        )
        self.assertEqual(
            1,
            len(evaluator.packets),
            "unchanged bytes must not be represented as new evaluator work",
        )
        cursor["sources"][str(self.log_file)]["last_changed_epoch"] = 0
        (self.config.runtime_root / "watcher" / "cursor.json").write_text(
            json.dumps(cursor), encoding="utf-8"
        )
        poll(self.config, evaluator)
        poll(self.config, evaluator)
        self.assertEqual(
            2,
            len(evaluator.packets),
            "threshold must evaluate one no-progress candidate exactly once",
        )
        self.assertTrue(
            evaluator.packets[-1]["observations"][0]["no_progress_candidate"]
        )

    def test_mirrored_watcher_alert_advances_raw_cursor_without_evaluator_feedback(
        self,
    ) -> None:
        class CountingEvaluator:
            def __init__(self):
                self.packets = []

            def evaluate(self, packet):
                self.packets.append(packet)
                return FakeEvaluator().evaluate(packet)

        derived = {
            "type": "HARNESS_WATCHER_ALERT",
            "identity": "harness-watcher:hwa-1",
            "summary": "old watcher alert",
        }
        self.log_file.write_text(json.dumps(derived) + "\n", encoding="utf-8")
        evaluator = CountingEvaluator()
        result = poll(self.config, evaluator)
        self.assertTrue(result["skipped"])
        self.assertEqual([], evaluator.packets)
        self.assertFalse(
            (self.config.runtime_root / "watcher" / "alerts.json").exists()
        )
        cursor = json.loads(
            (self.config.runtime_root / "watcher" / "cursor.json").read_text(
                encoding="utf-8"
            )
        )["sources"][str(self.log_file)]
        self.assertEqual(self.log_file.stat().st_size, cursor["offset"])
        self.assertIn("HARNESS_WATCHER_ALERT", cursor["context"]["text"])

    def test_mixed_delta_hides_watcher_alert_but_evaluates_genuine_record(self) -> None:
        class CapturingEvaluator:
            def __init__(self):
                self.packets = []

            def evaluate(self, packet):
                self.packets.append(packet)
                return FakeEvaluator().evaluate(packet)

        derived = {"type": "HARNESS_WATCHER_ALERT", "identity": "harness-watcher:hwa-2"}
        genuine = {"type": "LANE_PROGRESS", "identity": "lane:atlas", "stage": "build"}
        self.log_file.write_text(
            json.dumps(derived) + "\n" + json.dumps(genuine) + "\n", encoding="utf-8"
        )
        evaluator = CapturingEvaluator()
        poll(self.config, evaluator)
        self.assertEqual(1, len(evaluator.packets))
        text = evaluator.packets[0]["observations"][0]["text"]
        self.assertNotIn("HARNESS_WATCHER_ALERT", text)
        self.assertIn("LANE_PROGRESS", text)
        cursor = json.loads(
            (self.config.runtime_root / "watcher" / "cursor.json").read_text(
                encoding="utf-8"
            )
        )["sources"][str(self.log_file)]
        self.assertEqual(self.log_file.stat().st_size, cursor["offset"])
        self.assertIn("HARNESS_WATCHER_ALERT", cursor["context"]["text"])

    def test_derived_only_retained_context_never_becomes_no_progress_evaluator_work(
        self,
    ) -> None:
        class CountingEvaluator:
            def __init__(self):
                self.packets = []

            def evaluate(self, packet):
                self.packets.append(packet)
                return FakeEvaluator().evaluate(packet)

        self.log_file.write_text(
            json.dumps(
                {"type": "HARNESS_WATCHER_ALERT", "identity": "harness-watcher:hwa-3"}
            )
            + "\n",
            encoding="utf-8",
        )
        evaluator = CountingEvaluator()
        poll(self.config, evaluator)
        cursor_path = self.config.runtime_root / "watcher" / "cursor.json"
        cursor = json.loads(cursor_path.read_text(encoding="utf-8"))
        cursor["sources"][str(self.log_file)]["last_changed_epoch"] = 0
        cursor_path.write_text(json.dumps(cursor), encoding="utf-8")
        result = poll(self.config, evaluator)
        self.assertTrue(result["skipped"])
        self.assertEqual([], evaluator.packets)

    def test_same_delta_stale_then_exact_recovery_is_hidden_from_evaluator(
        self,
    ) -> None:
        class CapturingEvaluator:
            def __init__(self):
                self.packets = []

            def evaluate(self, packet):
                self.packets.append(packet)
                return FakeEvaluator().evaluate(packet)

        stale = {"type": "STALE_STATUS", "identity": "lane:atlas:process"}
        recovered = {"type": "CONTROLLER_ACTIVE", "identity": "lane:atlas:process"}
        self.log_file.write_text(
            json.dumps(stale) + "\n" + json.dumps(recovered) + "\n", encoding="utf-8"
        )
        evaluator = CapturingEvaluator()
        poll(self.config, evaluator)
        self.assertEqual([], evaluator.packets)
        cursor = json.loads(
            (self.config.runtime_root / "watcher" / "cursor.json").read_text(
                encoding="utf-8"
            )
        )["sources"][str(self.log_file)]
        self.assertIn("STALE_STATUS", cursor["context"]["text"])

    def test_stale_in_one_poll_then_exact_exit_in_next_poll_never_calls_evaluator(
        self,
    ) -> None:
        class CapturingEvaluator:
            def __init__(self):
                self.packets = []

            def evaluate(self, packet):
                self.packets.append(packet)
                return FakeEvaluator().evaluate(packet)

        self.log_file.write_text(
            json.dumps({"type": "STALE_STATUS", "identity": "lane:atlas:process"})
            + "\n",
            encoding="utf-8",
        )
        evaluator = CapturingEvaluator()
        with patch("harness_watcher_implementation.poller.time.time", return_value=100):
            self.assertTrue(poll(self.config, evaluator)["skipped"])
        with self.log_file.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {"type": "CONTROLLER_EXITED", "identity": "lane:atlas:process"}
                )
                + "\n"
            )
        with patch("harness_watcher_implementation.poller.time.time", return_value=200):
            self.assertTrue(poll(self.config, evaluator)["skipped"])
        self.assertEqual([], evaluator.packets)

    def test_sustained_stale_reloads_cursor_then_releases_once_with_valid_evidence(
        self,
    ) -> None:
        class DefectEvaluator:
            def __init__(self):
                self.packets = []

            def evaluate(self, packet):
                self.packets.append(packet)
                observed = packet["observations"][-1]
                return validate_verdict(
                    {
                        "defect": True,
                        "kind": "manager_failure",
                        "severity": "error",
                        "summary": "sustained stale",
                        "implicated": ["atlas"],
                        "evidence": [
                            {
                                "path": observed["path"],
                                "sha256": observed["sha256"],
                                "offset": observed["offset"],
                            }
                        ],
                    },
                    packet=packet,
                )

        self.log_file.write_text(
            json.dumps({"type": "STALE_STATUS", "identity": "lane:atlas:process"})
            + "\n",
            encoding="utf-8",
        )
        evaluator = DefectEvaluator()
        with patch("harness_watcher_implementation.poller.time.time", return_value=100):
            self.assertTrue(poll(self.config, evaluator)["skipped"])
        cursor = json.loads(
            (self.config.runtime_root / "watcher" / "cursor.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertIn("stale_quarantine", cursor)
        with patch("harness_watcher_implementation.poller.time.time", return_value=401):
            result = poll(self.config, evaluator)
        self.assertIsNotNone(result["alert"])
        self.assertEqual(1, len(evaluator.packets))
        self.assertIn("STALE_STATUS", evaluator.packets[0]["observations"][-1]["text"])
        with patch("harness_watcher_implementation.poller.time.time", return_value=800):
            self.assertTrue(poll(self.config, evaluator)["skipped"])
        self.assertEqual(1, len(evaluator.packets))

    def test_unrelated_identity_or_clear_does_not_suppress_stale_release(self) -> None:
        class CapturingEvaluator:
            def __init__(self):
                self.packets = []

            def evaluate(self, packet):
                self.packets.append(packet)
                return FakeEvaluator().evaluate(packet)

        stale = {"type": "STALE_STATUS", "identity": "lane:atlas:process"}
        unrelated = {"type": "CONTROLLER_EXITED", "identity": "lane:boreal:process"}
        wrong_clear = {
            "type": "CONDITION_CLEARED",
            "identity": "lane:atlas:process",
            "data": {"cleared_type": "CHECKPOINT_UPDATED"},
        }
        self.log_file.write_text(json.dumps(stale) + "\n", encoding="utf-8")
        evaluator = CapturingEvaluator()
        with patch("harness_watcher_implementation.poller.time.time", return_value=100):
            poll(self.config, evaluator)
        with self.log_file.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(unrelated) + "\n" + json.dumps(wrong_clear) + "\n")
        with patch("harness_watcher_implementation.poller.time.time", return_value=401):
            poll(self.config, evaluator)
        self.assertEqual(1, len(evaluator.packets))
        self.assertIn("STALE_STATUS", evaluator.packets[0]["observations"][-1]["text"])

    def test_legacy_v2_cursor_without_quarantine_state_remains_readable(self) -> None:
        cursor_path = self.config.runtime_root / "watcher" / "cursor.json"
        cursor_path.parent.mkdir(parents=True)
        stat = self.log_file.stat()
        cursor_path.write_text(
            json.dumps(
                {
                    "schema": "harness-watcher-cursor/v2",
                    "sources": {
                        str(self.log_file): {
                            "file_id": [stat.st_dev, stat.st_ino],
                            "offset": 0,
                            "generation": None,
                            "context": {},
                            "last_changed_epoch": 0,
                            "last_changed_utc": None,
                            "no_progress_evaluated_generation": None,
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        evaluator = FakeEvaluator()
        result = poll(self.config, evaluator)
        self.assertIn("verdict", result)
        self.assertIn(
            "stale_quarantine", json.loads(cursor_path.read_text(encoding="utf-8"))
        )

    def test_non_stale_condition_clear_does_not_hide_stale_status_after_quarantine(
        self,
    ) -> None:
        class CapturingEvaluator:
            def __init__(self):
                self.packets = []

            def evaluate(self, packet):
                self.packets.append(packet)
                return FakeEvaluator().evaluate(packet)

        stale = {"type": "STALE_STATUS", "identity": "lane:atlas:process"}
        unrelated_clear = {
            "type": "CONDITION_CLEARED",
            "identity": "lane:atlas:process",
            "data": {"cleared_type": "CHECKPOINT_UPDATED"},
        }
        self.log_file.write_text(
            json.dumps(stale) + "\n" + json.dumps(unrelated_clear) + "\n",
            encoding="utf-8",
        )
        evaluator = CapturingEvaluator()
        with patch("harness_watcher_implementation.poller.time.time", return_value=100):
            poll(self.config, evaluator)
        with patch("harness_watcher_implementation.poller.time.time", return_value=401):
            poll(self.config, evaluator)
        self.assertIn("STALE_STATUS", evaluator.packets[-1]["observations"][-1]["text"])

    def test_configured_evaluator_prompt_and_review_packet_bind_the_ownership_phase_rules(
        self,
    ) -> None:
        class CapturingEvaluator:
            def __init__(self):
                self.packet = None

            def evaluate(self, packet):
                self.packet = packet
                return FakeEvaluator().evaluate(packet)

        capture = CapturingEvaluator()
        poll(self.config, capture)
        self.assertEqual(
            [dict(rule) for rule in OWNERSHIP_PHASE_RULES],
            capture.packet["constraints"]["ownership_phase_rules"],
        )
        self.assertEqual(
            dict(STALE_STATUS_QUARANTINE_RULE),
            capture.packet["constraints"]["stale_status_quarantine_rule"],
        )
        completed = subprocess.CompletedProcess(
            ("configured-evaluator",),
            0,
            json.dumps(
                {
                    "defect": False,
                    "kind": None,
                    "severity": "warning",
                    "summary": "healthy",
                    "implicated": [],
                    "evidence": [],
                }
            ),
            "",
        )
        with patch(
            "harness_watcher_implementation.evaluator.subprocess.run",
            return_value=completed,
        ) as run:
            CommandEvaluator(("configured-evaluator",)).evaluate(capture.packet)
        prompt = json.loads(run.call_args.kwargs["input"])
        self.assertEqual(
            capture.packet["constraints"]["ownership_phase_rules"],
            prompt["packet"]["constraints"]["ownership_phase_rules"],
        )
        for rule in OWNERSHIP_PHASE_RULES:
            self.assertIn(rule["rule"], prompt["instructions"])
        self.assertIn(STALE_STATUS_QUARANTINE_RULE["rule"], prompt["instructions"])

    def test_recorded_pre_request_ambiguity_and_request_awaiting_loss_remain_distinct_for_evaluator(
        self,
    ) -> None:
        class CapturingEvaluator:
            def __init__(self):
                self.packet = None

            def evaluate(self, packet):
                self.packet = packet
                return FakeEvaluator().evaluate(packet)

        pre_request = {
            "lane_id": "clean-d:Atlas:A22",
            "phase": "setup_preparation",
            "request_identity": None,
            "declared_mcp_lifetime": None,
        }
        awaiting_loss = {
            "lane_id": "clean-d:Atlas:A24",
            "phase": "awaiting_permission_relay",
            "request_identity": None,
            "active_lifetime_identity": None,
        }
        self.log_file.write_text(
            json.dumps(pre_request) + "\n" + json.dumps(awaiting_loss) + "\n",
            encoding="utf-8",
        )
        capture = CapturingEvaluator()
        poll(self.config, capture)
        constraints = {
            rule["case"]: rule
            for rule in capture.packet["constraints"]["ownership_phase_rules"]
        }
        self.assertEqual(
            "not_alertable",
            constraints["ordinary_setup_or_preparation_missing_request_identity"][
                "disposition"
            ],
        )
        self.assertEqual(
            "not_alertable",
            constraints["controller_pre_start_missing_declared_mcp_lifetime"][
                "disposition"
            ],
        )
        self.assertEqual(
            "alertable_only_with_explicit_evidence",
            constraints["ownership_failure"]["disposition"],
        )
        observed = capture.packet["observations"][0]["text"]
        self.assertIn("setup_preparation", observed)
        self.assertIn("awaiting_permission_relay", observed)
        completed = subprocess.CompletedProcess(
            ("configured-evaluator",),
            0,
            json.dumps(
                {
                    "defect": False,
                    "kind": None,
                    "severity": "warning",
                    "summary": "healthy",
                    "implicated": [],
                    "evidence": [],
                }
            ),
            "",
        )
        with patch(
            "harness_watcher_implementation.evaluator.subprocess.run",
            return_value=completed,
        ) as run:
            CommandEvaluator(("configured-evaluator",)).evaluate(capture.packet)
        prompt = json.loads(run.call_args.kwargs["input"])
        self.assertIn("setup_preparation", prompt["packet"]["observations"][0]["text"])
        self.assertIn(
            "awaiting_permission_relay", prompt["packet"]["observations"][0]["text"]
        )
        self.assertIn(
            "Absence of request identity during ordinary setup or preparation is not a defect.",
            prompt["instructions"],
        )
        self.assertIn(
            "request-awaiting identity loss remain alertable.", prompt["instructions"]
        )

    def test_status_identity_rejects_a_reused_or_wrong_creation_time(self) -> None:
        from harness_watcher_implementation.__main__ import _identity, _same

        live = _identity(os.getpid())
        self.assertTrue(_same(live))
        stale = {**live, "created_utc": str(live["created_utc"]) + "-different"}
        self.assertFalse(_same(stale))
