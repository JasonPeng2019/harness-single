from __future__ import annotations

import unittest
from datetime import datetime, timezone
from typing import Any

from orchestrator_harness.cli import _append_event_attention


class _Store:
    def __init__(self, attention_epoch_id: str = "epoch") -> None:
        self.records: list[dict[str, Any]] = []
        self.attention_epoch_id = attention_epoch_id

    def append_attention(self, **record: Any) -> None:
        self.records.append(record)


class ManagerSignalAttentionCorrelationTests(unittest.TestCase):
    def test_real_manager_signal_shape_uses_signal_id_for_every_harness_stage(self) -> None:
        event = {
            "event_id": "5c9015ee-ordinary-harness-id",
            "type": "MANAGER_SIGNAL",
            "data": {
                "signal_id": "atlas-help-001",
                "deadline_utc": "2026-08-01T00:02:00+00:00",
                "delivery_deadline_utc": "2026-08-01T00:01:00+00:00",
                "lane_id": "epoch:Atlas:A22",
            },
        }
        store = _Store()
        for kind in (
            "HARNESS_SIGNAL_OBSERVED", "HARNESS_EVENT_ACTIONABLE",
            "HARNESS_EVENT_DEFERRED", "HARNESS_EVENT_INELIGIBLE", "HARNESS_EVENT_PENDING",
            "HARNESS_ACK_ATTEMPTED", "HARNESS_ACK_SUCCEEDED",
        ):
            _append_event_attention(store, kind=kind, event=event, timestamp=datetime.now(timezone.utc))
        self.assertEqual(["atlas-help-001"] * 7, [record["event_id"] for record in store.records])
        for record in store.records:
            metadata = record["metadata"]
            self.assertEqual("5c9015ee-ordinary-harness-id", metadata["harness_event_id"])
            self.assertEqual("2026-08-01T00:02:00+00:00", metadata["response_deadline_utc"])
            self.assertEqual("2026-08-01T00:01:00+00:00", metadata["delivery_deadline_utc"])

    def test_current_non_signal_keeps_harness_event_id(self) -> None:
        store = _Store()
        _append_event_attention(store, kind="HARNESS_SIGNAL_OBSERVED", event={"event_id": "ordinary", "type": "CHECKPOINT_UPDATED", "data": {"lane_id": "epoch:Atlas:A22"}})
        self.assertEqual("ordinary", store.records[0]["event_id"])
        self.assertIsNone(store.records[0]["metadata"])

    def test_historical_observation_remains_ordinary_and_is_not_restamped(self) -> None:
        store = _Store("20260801-attention-r6")
        _append_event_attention(
            store,
            kind="HARNESS_SIGNAL_OBSERVED",
            event={"event_id": "r5-historical-manager-event", "type": "MANAGER_SIGNAL", "data": {"signal_id": "sig-20260731-long-canary-atlas-a22-s1-prepare-b14-20260731T140114Z", "lane_id": "20260731-s1-clean:Atlas:A22"}},
        )
        _append_event_attention(
            store,
            kind="HARNESS_SIGNAL_OBSERVED",
            event={"event_id": "r5-historical-non-signal", "type": "MCP_EXITED", "data": {"declared_lane_id": "20260731-s1-clean:Atlas:A22"}},
        )
        _append_event_attention(
            store,
            kind="HARNESS_SIGNAL_OBSERVED",
            event={"event_id": "current-signal", "type": "MANAGER_SIGNAL", "data": {"signal_id": "current-signal", "lane_id": "20260801-attention-r6:Atlas:A22"}},
        )
        self.assertEqual(["current-signal"], [record["event_id"] for record in store.records])

    def test_explicit_epoch_is_authoritative_over_lane_fallback(self) -> None:
        store = _Store("20260801-attention-r6")
        cases = (
            ("explicit-current-stale-lane", {"attention_epoch_id": "20260801-attention-r6", "lane_id": "20260731-old:Atlas:A22"}),
            ("explicit-old-current-lane", {"attention_epoch_id": "20260731-old", "lane_id": "20260801-attention-r6:Atlas:A22"}),
            ("unbound", {}),
        )
        for event_id, data in cases:
            _append_event_attention(store, kind="HARNESS_SIGNAL_OBSERVED", event={"event_id": event_id, "type": "MANAGER_SIGNAL", "data": {"signal_id": event_id, **data}})
        self.assertEqual(["explicit-current-stale-lane"], [record["event_id"] for record in store.records])

    def test_current_signal_stages_are_all_retained(self) -> None:
        store = _Store("20260801-attention-r6")
        event = {"event_id": "harness-id", "type": "MANAGER_SIGNAL", "data": {"signal_id": "current", "attention_epoch_id": "20260801-attention-r6"}}
        for kind in ("HARNESS_SIGNAL_OBSERVED", "HARNESS_EVENT_ACTIONABLE", "HARNESS_EVENT_DEFERRED", "HARNESS_EVENT_INELIGIBLE", "HARNESS_EVENT_PENDING", "HARNESS_ACK_ATTEMPTED", "HARNESS_ACK_SUCCEEDED"):
            _append_event_attention(store, kind=kind, event=event)
        self.assertEqual(["current"] * 7, [record["event_id"] for record in store.records])


if __name__ == "__main__":
    unittest.main()

