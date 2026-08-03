from __future__ import annotations
# pyright: reportImplicitRelativeImport=false

import io
import json
import unittest
from datetime import timedelta

from orchestrator_harness.cli import EXIT_TIMEOUT, watch_once, watch_until_event
from orchestrator_harness.events import conditions_from_snapshot, diff_conditions
from orchestrator_harness.tests.support import NOW, SuiteFixture


class EventAndCliTests(unittest.TestCase):
    fixture: SuiteFixture  # pyright: ignore[reportUninitializedInstanceVariable]

    def setUp(self) -> None:
        self.fixture = SuiteFixture.create()
        self.fixture.status()
        self.fixture.jsonl()

    def tearDown(self) -> None:
        self.fixture.close()

    def test_stable_event_ids_and_no_unchanged_events(self) -> None:
        snapshot = {
            "process_snapshot_complete": True,
            "process_provider": "fake",
            "process_errors": [],
            "lanes": [
                {
                    "lane_id": "run:Atlas:A00",
                    "operational_state": "RUNNING_CODEX",
                    "run_root": "run",
                    "doer": "Atlas",
                    "task": "A00",
                    "phase": "x",
                    "reason": "live",
                    "controller_pid": 1,
                    "codex_pid": 2,
                }
            ],
            "requests": [],
            "resource_conflicts": [],
            "observation_errors": [],
        }
        current = conditions_from_snapshot(snapshot)
        first = diff_conditions(None, current, observed_at=NOW)
        second = diff_conditions(
            current, current, observed_at=NOW + timedelta(seconds=1)
        )
        self.assertEqual(1, len(first))
        self.assertEqual([], second)
        self.assertEqual(64, len(first[0]["event_id"]))

    def test_condition_clear_event(self) -> None:
        previous = {
            "x": {
                "event_id": "old",
                "identity": "x",
                "type": "RESOURCE_CONFLICT",
            }
        }
        events = diff_conditions(previous, {}, observed_at=NOW)
        self.assertEqual("CONDITION_CLEARED", events[0]["type"])
        self.assertEqual("old", events[0]["data"]["cleared_event_id"])

    def test_exit_checkpoint_and_release_are_independent_conditions(self) -> None:
        snapshot = {
            "process_snapshot_complete": True,
            "process_provider": "fake",
            "process_errors": [],
            "lanes": [
                {
                    "lane_id": "run:Atlas:A00",
                    "operational_state": "CHECKPOINTED",
                    "process_state": "EXITED",
                    "process_reason": "clean exit",
                    "run_root": "run",
                    "doer": "Atlas",
                    "task": "A00",
                    "controller_pid": 1,
                    "codex_pid": 2,
                    "checkpoint_path": "checkpoint.md",
                    "checkpoint_sha256": "abc",
                    "resource_release_possible": True,
                    "resources": [],
                }
            ],
            "requests": [],
            "helpers": [],
            "resource_conflicts": [],
            "observation_errors": [],
        }
        types = {item["type"] for item in conditions_from_snapshot(snapshot).values()}
        self.assertEqual(
            {
                "CONTROLLER_EXITED",
                "CHECKPOINT_UPDATED",
                "RESOURCE_RELEASE_POSSIBLE",
            },
            types,
        )

    def test_helper_lifecycle_condition_is_independent(self) -> None:
        base = {
            "process_snapshot_complete": True,
            "process_provider": "fake",
            "process_errors": [],
            "lanes": [],
            "requests": [],
            "resource_conflicts": [],
            "observation_errors": [],
        }
        running = {
            **base,
            "helpers": [
                {
                    "path": "helper_process.json",
                    "sha256": "one",
                    "operational_state": "HELPER_RUNNING",
                    "processes": [{"pid": 10, "state": "live"}],
                }
            ],
        }
        exited = {
            **base,
            "helpers": [
                {
                    "path": "helper_process.json",
                    "sha256": "one",
                    "operational_state": "HELPER_EXITED",
                    "processes": [{"pid": 10, "state": "absent"}],
                }
            ],
        }
        first = conditions_from_snapshot(running)
        second = conditions_from_snapshot(exited)
        self.assertEqual("HELPER_ACTIVE", next(iter(first.values()))["type"])
        events = diff_conditions(first, second, observed_at=NOW)
        self.assertTrue(any(item["type"] == "HELPER_EXITED" for item in events))

    def test_provider_wait_does_not_replace_exit_or_checkpoint(self) -> None:
        snapshot = {
            "process_snapshot_complete": True,
            "process_provider": "fake",
            "process_errors": [],
            "lanes": [
                {
                    "lane_id": "run:Atlas:A00",
                    "operational_state": "CHECKPOINTED",
                    "process_state": "EXITED",
                    "process_reason": "clean exit",
                    "provider_wait": True,
                    "phase": "backend unavailable",
                    "checkpoint_path": "checkpoint.md",
                    "checkpoint_sha256": "abc",
                    "resource_release_possible": True,
                    "resources": [],
                }
            ],
            "requests": [],
            "helpers": [],
            "resource_conflicts": [],
            "observation_errors": [],
        }
        types = {item["type"] for item in conditions_from_snapshot(snapshot).values()}
        self.assertTrue(
            {
                "CONTROLLER_EXITED",
                "CHECKPOINT_UPDATED",
                "PROVIDER_WAIT",
                "RESOURCE_RELEASE_POSSIBLE",
            }.issubset(types)
        )

    def test_mcp_lifecycle_events_are_explicit(self) -> None:
        base = {
            "process_snapshot_complete": True,
            "process_provider": "fake",
            "process_errors": [],
            "lanes": [],
            "requests": [],
            "helpers": [],
            "resource_conflicts": [],
            "observation_errors": [],
        }
        active = {
            **base,
            "mcps": [
                {
                    "path": "request.json#mcp-lifetime:run-1",
                    "run_id": "run-1",
                    "operational_state": "MCP_RUNNING",
                    "processes": [{"pid": 4, "state": "live"}],
                }
            ],
        }
        exited = {
            **base,
            "mcps": [
                {
                    "path": "request.json#mcp-lifetime:run-1",
                    "run_id": "run-1",
                    "operational_state": "MCP_EXITED",
                    "processes": [{"pid": 4, "state": "absent"}],
                }
            ],
        }
        first = conditions_from_snapshot(active)
        second = conditions_from_snapshot(exited)
        self.assertEqual("MCP_ACTIVE", next(iter(first.values()))["type"])
        events = diff_conditions(first, second, observed_at=NOW)
        self.assertTrue(any(item["type"] == "MCP_EXITED" for item in events))

    def test_watch_once_persists_then_deduplicates(self) -> None:
        output1 = io.StringIO()
        code, first = watch_once(
            self.fixture.config,
            no_write=False,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
            stream=output1,
        )
        self.assertEqual(0, code)
        self.assertTrue(first)
        output2 = io.StringIO()
        _, second = watch_once(
            self.fixture.config,
            no_write=False,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
            stream=output2,
        )
        self.assertEqual([], second)
        self.assertEqual("", output2.getvalue())

    def test_no_write_mode_creates_no_state(self) -> None:
        watch_once(
            self.fixture.config,
            no_write=True,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
            stream=io.StringIO(),
        )
        self.assertFalse(self.fixture.config.output_dir.exists())

    def test_watch_until_event_timeout(self) -> None:
        # Establish the initial cursor.
        watch_once(
            self.fixture.config,
            no_write=False,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
            stream=io.StringIO(),
        )
        values = iter([0.0, 0.0, 1.0, 1.0])

        def monotonic():
            return next(values, 1.0)

        output = io.StringIO()
        code = watch_until_event(
            self.fixture.config,
            no_write=False,
            timeout_seconds=0.5,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
            sleeper=lambda _: None,
            monotonic=monotonic,
            stream=output,
        )
        self.assertEqual(EXIT_TIMEOUT, code)
        event = json.loads(output.getvalue())
        self.assertEqual("WATCH_TIMEOUT", event["type"])

    def test_watch_until_event_returns_on_state_change(self) -> None:
        watch_once(
            self.fixture.config,
            no_write=False,
            process_provider=self.fixture.process_snapshot,
            clock=lambda: NOW,
            stream=io.StringIO(),
        )
        snapshots = [
            self.fixture.process_snapshot(),
            self.fixture.process_snapshot(missing_codex=True),
        ]

        def provider():
            return (
                snapshots.pop(0)
                if snapshots
                else self.fixture.process_snapshot(missing_codex=True)
            )

        ticks = iter([0.0, 0.0, 0.1, 0.1])
        output = io.StringIO()
        code = watch_until_event(
            self.fixture.config,
            no_write=False,
            timeout_seconds=1,
            process_provider=provider,
            clock=lambda: NOW,
            sleeper=lambda _: None,
            monotonic=lambda: next(ticks, 0.2),
            stream=output,
        )
        self.assertEqual(0, code)
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertTrue(any(item["type"] == "STALE_STATUS" for item in events))


if __name__ == "__main__":
    unittest.main()
