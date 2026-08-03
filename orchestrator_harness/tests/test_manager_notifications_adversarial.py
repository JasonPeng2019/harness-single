from __future__ import annotations

import io
import json
import unittest

from orchestrator_harness.cli import EXIT_TIMEOUT, observe, watch_until_actionable
from orchestrator_harness.events import conditions_from_snapshot
from orchestrator_harness.tests.support import NOW, SuiteFixture, write_json


class ManagerNotificationAdversarialTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = SuiteFixture.create()

    def tearDown(self) -> None:
        self.fixture.close()

    def _write_signal(self, signal_id: str, **updates: object) -> None:
        value: dict[str, object] = {
            "schema": "manager-signal/v1",
            "signal_id": signal_id,
            "kind": "HELP",
            "created_utc": "2026-07-30T12:00:00Z",
            "lane_id": "A00_test:Atlas:A00",
            "summary": "manager input needed",
            "evidence_paths": ["missing/secret.txt"],
        }
        value.update(updates)
        write_json(self.fixture.workspace() / "manager-signals" / f"{signal_id}.json", value)

    def _timeout_wait(self) -> tuple[int, str]:
        ticks = iter([0.0, 1.0])
        output = io.StringIO()
        code = watch_until_actionable(
            self.fixture.config,
            timeout_seconds=0.5,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
            sleeper=lambda _: None,
            monotonic=lambda: next(ticks, 1.0),
            stream=output,
        )
        return code, output.getvalue()

    def test_malformed_signal_is_recorded_but_does_not_wake_without_active_state(self) -> None:
        path = self.fixture.workspace() / "manager-signals" / "bad.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{not-json", encoding="utf-8")

        code, output = self._timeout_wait()

        self.assertEqual(EXIT_TIMEOUT, code)
        self.assertEqual("WATCH_TIMEOUT", json.loads(output)["type"])
        snapshot, _ = observe(
            self.fixture.config,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
        )
        self.assertEqual("MANAGER_SIGNAL_READ_ERROR", snapshot["observation_errors"][0]["code"])

    def test_signal_evidence_is_never_followed_and_source_is_not_modified(self) -> None:
        self._write_signal("immutable")
        signal_path = self.fixture.workspace() / "manager-signals" / "immutable.json"
        before = signal_path.read_bytes()
        missing_evidence = self.fixture.suite_root / "runs" / "outside" / "missing" / "secret.txt"

        snapshot, _ = observe(
            self.fixture.config,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
        )

        self.assertEqual(before, signal_path.read_bytes())
        self.assertFalse(missing_evidence.exists())
        self.assertEqual(["missing/secret.txt"], snapshot["manager_signals"][0]["evidence_paths"])
        self.assertFalse((self.fixture.suite_root / ".agent-workspace" / "pending-notification.json").exists())

    def test_identical_duplicate_signal_files_are_one_condition(self) -> None:
        self._write_signal("same")
        duplicate = self.fixture.workspace() / "manager-signals" / "same-copy.json"
        duplicate.write_bytes((self.fixture.workspace() / "manager-signals" / "same.json").read_bytes())

        snapshot, _ = observe(
            self.fixture.config,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
        )

        self.assertEqual(1, len(snapshot["manager_signals"]))
        self.assertNotIn("MANAGER_SIGNAL_READ_ERROR", {item["code"] for item in snapshot["observation_errors"]})

    def test_hidden_atomic_signal_file_is_not_a_second_native_event(self) -> None:
        self._write_signal("atomic")
        signals = self.fixture.workspace() / "manager-signals"
        (signals / ".sig-atomic.tmp.json").write_bytes((signals / "atomic.json").read_bytes())

        first, _ = observe(
            self.fixture.config,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
        )
        second, _ = observe(
            self.fixture.config,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
        )
        first_events = conditions_from_snapshot(first)
        second_events = conditions_from_snapshot(second)

        self.assertEqual(["atomic"], [item["signal_id"] for item in first["manager_signals"]])
        self.assertEqual(["manager-signal:atomic"], list(first_events))
        self.assertEqual(first_events["manager-signal:atomic"]["event_id"], second_events["manager-signal:atomic"]["event_id"])
        self.assertNotIn("MANAGER_SIGNAL_READ_ERROR", {item["code"] for item in first["observation_errors"]})


if __name__ == "__main__":
    unittest.main()
