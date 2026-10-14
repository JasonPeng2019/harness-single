from __future__ import annotations
import tempfile, unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from harness_watcher_implementation.attention import (
    AttentionConflictError,
    AttentionValidationError,
    PENDING_SNAPSHOT_KINDS,
    _paired_manager_intervals,
    _wake_evidence,
    analyze_event,
    append_producer_record,
    canonicalize,
    make_source_record,
    source_digest,
    validate_record,
)


def stamp(seconds=0):
    return (
        datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=seconds)
    ).isoformat()


def rec_for(event_id, kind, t, **metadata):
    if kind in PENDING_SNAPSHOT_KINDS and "pending_work_snapshot" not in metadata:
        metadata["pending_work_snapshot"] = {
            "complete": False,
            "events": [],
            "selected_event_id": None,
            "selection_reason": "UNKNOWN",
        }
    role = (
        "subagent"
        if kind.startswith("AGENT_") or kind == "WATCHER_NOTIFICATION_SENT"
        else "orchestrator"
        if kind.startswith("MANAGER_")
        else "harness"
    )
    source = make_source_record(
        recorder="test",
        epoch_id="epoch",
        event_id=event_id,
        kind=kind,
        source_timestamp_utc=stamp(t),
        metadata=metadata,
    )
    return canonicalize(
        source,
        observed_timestamp_utc=stamp(t),
        source_path="source",
        source_role=role,
        source_id="test" if role in {"subagent", "orchestrator"} else "harness",
        source_generation="g",
        byte_start=0,
        byte_end=1,
    )


def rec(kind, t, **metadata):
    return rec_for("event", kind, t, **metadata)


def published(signal, t, **metadata):
    fields = {
        "lane_id": signal["lane_id"],
        "agent_blocked": signal["agent_blocked"],
        "signal_id": signal["event_id"],
    }
    for field in ("delivery_deadline_utc", "response_deadline_utc"):
        if field in signal:
            fields[field] = signal[field]
    fields.update(metadata)
    return rec("AGENT_SIGNAL_PUBLISHED", t, **fields)


class AttentionContractTests(unittest.TestCase):
    def test_pending_work_snapshot_boundaries_validate_shape_and_limits(self):
        snapshot = {
            "complete": True,
            "events": [
                {
                    "event_id": "event",
                    "type": "HELP",
                    "priority": 2,
                    "age_seconds": 1.5,
                    "agent_blocked": True,
                    "response_deadline_utc": stamp(9),
                }
            ],
            "selected_event_id": "event",
            "selection_reason": "SELECT_ACTIONABLE",
        }
        for kind, metadata in (
            (
                "MANAGER_INVOCATION_STARTED",
                {
                    "manager_session_id": "s",
                    "manager_invocation_id": "i",
                    "pending_work_snapshot": snapshot,
                },
            ),
            (
                "MANAGER_INVOCATION_FINISHED",
                {
                    "manager_session_id": "s",
                    "manager_invocation_id": "i",
                    "pending_work_snapshot": snapshot,
                },
            ),
            ("HARNESS_EVENT_PENDING", {"pending_work_snapshot": snapshot}),
            ("FORMAL_REVIEW_BASELINE_ADVANCED", {"pending_work_snapshot": snapshot}),
        ):
            self.assertEqual(
                kind,
                make_source_record(
                    recorder="test",
                    epoch_id="epoch",
                    event_id="event",
                    kind=kind,
                    metadata=metadata,
                )["kind"],
            )
        with self.assertRaises(AttentionValidationError):
            make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id="event",
                kind="MANAGER_INVOCATION_STARTED",
                metadata={"manager_session_id": "s", "manager_invocation_id": "i"},
            )
        bad = {**snapshot, "events": snapshot["events"] * 129}
        with self.assertRaises(AttentionValidationError):
            make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id="event",
                kind="HARNESS_EVENT_PENDING",
                metadata={"pending_work_snapshot": bad},
            )
        transcript = {
            **snapshot,
            "events": [{**snapshot["events"][0], "transcript": "forbidden"}],
        }
        with self.assertRaises(AttentionValidationError):
            make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id="event",
                kind="HARNESS_EVENT_PENDING",
                metadata={"pending_work_snapshot": transcript},
            )
        unknown = {
            "complete": False,
            "events": [],
            "selected_event_id": None,
            "selection_reason": "UNKNOWN",
        }
        self.assertEqual(
            "HARNESS_EVENT_PENDING",
            make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id="event",
                kind="HARNESS_EVENT_PENDING",
                metadata={"pending_work_snapshot": unknown},
            )["kind"],
        )

    def test_production_wake_record_contract_and_provenance(self):
        wake_id = "b3bbd28a-6cdd-4dd5-9d49-bd6d0c14fc4e"
        common = {
            "wake_id": wake_id,
            "wake_component": "orchestrator_harness.watch_until_actionable",
            "wake_transport": "blocking_harness_wait_stdout",
            "manager_session_id": "s",
            "manager_invocation_id": "i",
        }
        attempted = make_source_record(
            recorder="harness",
            epoch_id="epoch",
            event_id="event",
            kind="MANAGER_WAKE_ATTEMPTED",
            metadata=common,
        )
        delivered = make_source_record(
            recorder="harness",
            epoch_id="epoch",
            event_id="event",
            kind="MANAGER_WAKE_DELIVERED",
            metadata={**common, "delivery_succeeded": True},
        )
        failed = make_source_record(
            recorder="harness",
            epoch_id="epoch",
            event_id="event",
            kind="MANAGER_WAKE_FAILED",
            metadata={
                **common,
                "delivery_succeeded": False,
                "failure_kind": "BrokenPipeError",
            },
        )
        received = make_source_record(
            recorder="manager",
            epoch_id="epoch",
            event_id="event",
            kind="MANAGER_WAKE_RECEIVED",
            metadata={k: v for k, v in common.items() if k != "wake_component"},
        )
        for source, role, source_id in (
            (attempted, "harness", "harness"),
            (delivered, "harness", "harness"),
            (failed, "harness", "harness"),
            (received, "orchestrator", "manager"),
        ):
            self.assertEqual(
                source["kind"],
                canonicalize(
                    source,
                    observed_timestamp_utc=stamp(),
                    source_path="source",
                    source_role=role,
                    source_id=source_id,
                    source_generation="g",
                    byte_start=0,
                    byte_end=1,
                )["kind"],
            )
        for kind, metadata in (
            ("MANAGER_WAKE_ATTEMPTED", {**common, "wake_id": "not-a-uuid"}),
            ("MANAGER_WAKE_ATTEMPTED", {**common, "delivery_succeeded": False}),
            ("MANAGER_WAKE_ATTEMPTED", {**common, "failure_kind": "BrokenPipeError"}),
            ("MANAGER_WAKE_DELIVERED", common),
            (
                "MANAGER_WAKE_DELIVERED",
                {
                    **common,
                    "delivery_succeeded": True,
                    "failure_kind": "BrokenPipeError",
                },
            ),
            ("MANAGER_WAKE_FAILED", {**common, "delivery_succeeded": False}),
            (
                "MANAGER_WAKE_FAILED",
                {
                    **common,
                    "delivery_succeeded": True,
                    "failure_kind": "BrokenPipeError",
                },
            ),
        ):
            with self.assertRaises(AttentionValidationError):
                make_source_record(
                    recorder="test",
                    epoch_id="epoch",
                    event_id="event",
                    kind=kind,
                    metadata=metadata,
                )
        with self.assertRaisesRegex(AttentionValidationError, "harness provenance"):
            canonicalize(
                attempted,
                observed_timestamp_utc=stamp(),
                source_path="source",
                source_role="orchestrator",
                source_id="harness",
                source_generation="g",
                byte_start=0,
                byte_end=1,
            )
        with self.assertRaisesRegex(
            AttentionValidationError, "orchestrator provenance"
        ):
            canonicalize(
                received,
                observed_timestamp_utc=stamp(),
                source_path="source",
                source_role="harness",
                source_id="harness",
                source_generation="g",
                byte_start=0,
                byte_end=1,
            )
        finished = make_source_record(
            recorder="manager",
            epoch_id="epoch",
            event_id="event",
            kind="MANAGER_WAIT_FINISHED",
            metadata={
                "manager_session_id": "s",
                "manager_invocation_id": "i",
                "activity_id": "wait",
                "wake_id": wake_id,
                "wake_transport": "blocking_harness_wait_stdout",
            },
        )
        self.assertEqual(
            "MANAGER_WAIT_FINISHED",
            canonicalize(
                finished,
                observed_timestamp_utc=stamp(),
                source_path="source",
                source_role="orchestrator",
                source_id="manager",
                source_generation="g",
                byte_start=0,
                byte_end=1,
            )["kind"],
        )
        with self.assertRaises(AttentionValidationError):
            make_source_record(
                recorder="manager",
                epoch_id="epoch",
                event_id="event",
                kind="MANAGER_WAIT_FINISHED",
                metadata={
                    "manager_session_id": "s",
                    "manager_invocation_id": "i",
                    "activity_id": "wait",
                    "wake_id": wake_id,
                },
            )
        with self.assertRaises(AttentionValidationError):
            make_source_record(
                recorder="manager",
                epoch_id="epoch",
                event_id="event",
                kind="MANAGER_WAIT_FINISHED",
                metadata={
                    "manager_session_id": "s",
                    "manager_invocation_id": "i",
                    "activity_id": "wait",
                    "wake_transport": "blocking_harness_wait_stdout",
                },
            )
        with self.assertRaises(AttentionValidationError):
            make_source_record(
                recorder="manager",
                epoch_id="epoch",
                event_id="event",
                kind="MANAGER_WAIT_STARTED",
                metadata={
                    "manager_session_id": "s",
                    "manager_invocation_id": "i",
                    "activity_id": "wait",
                    "wake_id": "not-a-uuid",
                },
            )
        with self.assertRaisesRegex(
            AttentionValidationError, "orchestrator provenance"
        ):
            canonicalize(
                finished,
                observed_timestamp_utc=stamp(),
                source_path="source",
                source_role="harness",
                source_id="harness",
                source_generation="g",
                byte_start=0,
                byte_end=1,
            )

    def test_production_wake_evidence_matrix(self):
        wake = "b3bbd28a-6cdd-4dd5-9d49-bd6d0c14fc4e"

        def item(kind, t, **metadata):
            role = (
                "harness"
                if kind
                in {
                    "HARNESS_SIGNAL_OBSERVED",
                    "MANAGER_WAKE_ATTEMPTED",
                    "MANAGER_WAKE_DELIVERED",
                    "MANAGER_WAKE_FAILED",
                }
                else "orchestrator"
                if kind.startswith("MANAGER_")
                else "subagent"
            )
            source = make_source_record(
                recorder="h"
                if role == "harness"
                else "root"
                if role == "orchestrator"
                else "lane",
                epoch_id="epoch",
                event_id="event",
                kind=kind,
                source_timestamp_utc=stamp(t),
                metadata=metadata,
            )
            return canonicalize(
                source,
                observed_timestamp_utc=stamp(100 - t),
                source_path="source",
                source_role=role,
                source_id="h"
                if role == "harness"
                else "root"
                if role == "orchestrator"
                else "lane",
                source_generation="g",
                byte_start=0,
                byte_end=1,
            )

        common = {
            "wake_id": wake,
            "wake_component": "orchestrator_harness.watch_until_actionable",
            "wake_transport": "blocking_harness_wait_stdout",
            "manager_session_id": "s",
            "manager_invocation_id": "i",
        }
        complete = [
            item("AGENT_SIGNAL_CREATED", 0, lane_id="l", agent_blocked=True),
            item("HARNESS_SIGNAL_OBSERVED", 1),
            item("MANAGER_WAKE_ATTEMPTED", 2, **common),
            item("MANAGER_WAKE_DELIVERED", 3, **common, delivery_succeeded=True),
            item(
                "MANAGER_WAKE_RECEIVED",
                4,
                **{k: v for k, v in common.items() if k != "wake_component"},
            ),
            item(
                "MANAGER_WAIT_FINISHED",
                4,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="w",
                wake_id=wake,
                wake_transport="blocking_harness_wait_stdout",
            ),
            item(
                "MANAGER_EVENT_CLAIMED",
                5,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="READING_EVENT",
            ),
        ]
        evidence = _wake_evidence(complete, event_id="event")
        self.assertEqual("COMPLETE", evidence["status"])
        self.assertEqual(1.0, evidence["attempted_to_delivered_seconds"])
        self.assertEqual(1.0, evidence["delivered_to_received_seconds"])
        self.assertEqual(1.0, evidence["received_to_claim_seconds"])
        for index in range(len(complete)):
            expected = "NOT_APPLICABLE" if index == 2 else "INSUFFICIENT_EVIDENCE"
            self.assertEqual(
                expected,
                _wake_evidence(
                    complete[:index] + complete[index + 1 :], event_id="event"
                )["status"],
            )
        duplicate = complete + [
            item("MANAGER_WAKE_DELIVERED", 3, **common, delivery_succeeded=True)
        ]
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            _wake_evidence(duplicate, event_id="event")["status"],
        )
        reused = complete + [item("MANAGER_WAKE_ATTEMPTED", 2, **common)]
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE", _wake_evidence(reused, event_id="event")["status"]
        )
        reordered = list(complete)
        reordered[3] = item(
            "MANAGER_WAKE_DELIVERED", 1, **common, delivery_succeeded=True
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            _wake_evidence(reordered, event_id="event")["status"],
        )
        action = complete + [
            item(
                "MANAGER_DECISION_RECORDED",
                3.5,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="READING_EVENT",
            )
        ]
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE", _wake_evidence(action, event_id="event")["status"]
        )
        failed = [
            item("AGENT_SIGNAL_CREATED", 0, lane_id="l", agent_blocked=True),
            item("HARNESS_SIGNAL_OBSERVED", 1),
            item("MANAGER_WAKE_ATTEMPTED", 2, **common),
            item(
                "MANAGER_WAKE_FAILED",
                3,
                **common,
                delivery_succeeded=False,
                failure_kind="BrokenPipeError",
            ),
        ]
        self.assertEqual("FAILED", _wake_evidence(failed, event_id="event")["status"])
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            _wake_evidence(failed[1:], event_id="event")["status"],
        )
        failed_reordered = list(failed)
        failed_reordered[1] = item("HARNESS_SIGNAL_OBSERVED", 3)
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            _wake_evidence(failed_reordered, event_id="event")["status"],
        )
        retry = failed + [
            {**record, "wake_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479"}
            for record in complete[2:]
        ]
        self.assertEqual("COMPLETE", _wake_evidence(retry, event_id="event")["status"])

    def test_causal_metrics_use_source_time_and_reject_negative_source_order(self):
        signal = rec("AGENT_SIGNAL_CREATED", 0, lane_id="l", agent_blocked=True)
        publication = published(signal, 1)
        observed = rec("HARNESS_SIGNAL_OBSERVED", 2)
        signal["observed_timestamp_utc"] = stamp(9)
        publication["observed_timestamp_utc"] = stamp(8)
        observed["observed_timestamp_utc"] = stamp(1)
        report = analyze_event(
            [signal, publication, observed], epoch_id="epoch", event_id="event"
        )
        self.assertEqual(1.0, report["metrics"]["signal_to_observation_seconds"])
        self.assertEqual(1.0, report["metrics"]["creation_to_publication_seconds"])
        self.assertFalse(
            any(
                "negative causal metric" in value
                for value in report["contradictory_evidence"]
            )
        )
        inverted = rec("HARNESS_SIGNAL_OBSERVED", -1)
        report = analyze_event(
            [signal, publication, inverted], epoch_id="epoch", event_id="event"
        )
        self.assertEqual("INSUFFICIENT_EVIDENCE", report["classification"])

    def test_explicit_deferral_metric_requires_pre_pending_endpoint(self):
        pending = rec("HARNESS_EVENT_PENDING", 10, response_deadline_utc=stamp(15))
        deferred = rec("HARNESS_EVENT_DEFERRED", 4)
        report = analyze_event([deferred, pending], epoch_id="epoch", event_id="event")
        self.assertEqual(6.0, report["metrics"]["explicit_deferral_seconds"])

        # A post-pending deferral is a later event, not a negative causal duration.
        post_pending = rec("HARNESS_EVENT_DEFERRED", 11)
        report = analyze_event(
            [pending, post_pending], epoch_id="epoch", event_id="event"
        )
        self.assertIsNone(report["metrics"]["explicit_deferral_seconds"])
        self.assertFalse(
            any(
                "negative causal metric: explicit_deferral_seconds" in value
                for value in report["contradictory_evidence"]
            )
        )

        # The narrow metric repair must not suppress genuinely reversed causal stages.
        reversed_signal = rec(
            "AGENT_SIGNAL_CREATED", 12, lane_id="l", agent_blocked=True
        )
        reversed_publication = published(reversed_signal, 12)
        reversed_observation = rec("HARNESS_SIGNAL_OBSERVED", 11)
        report = analyze_event(
            [
                deferred,
                pending,
                reversed_signal,
                reversed_publication,
                reversed_observation,
            ],
            epoch_id="epoch",
            event_id="event",
        )
        self.assertEqual("INSUFFICIENT_EVIDENCE", report["classification"])
        self.assertIsNone(report["metrics"]["publication_to_observation_seconds"])

    def test_final_publication_is_required_for_blocked_harness_attribution(self):
        signal = rec(
            "AGENT_SIGNAL_CREATED",
            0,
            lane_id="l",
            agent_blocked=True,
            delivery_deadline_utc=stamp(5),
            response_deadline_utc=stamp(12),
        )
        observed = rec("HARNESS_SIGNAL_OBSERVED", 7)
        actionable = rec("HARNESS_EVENT_ACTIONABLE", 7)

        timely = published(signal, 1)
        report = analyze_event(
            [signal, timely, observed, actionable], epoch_id="epoch", event_id="event"
        )
        self.assertEqual("HARNESS_DELIVERY_DELAY", report["classification"])
        self.assertEqual(6.0, report["metrics"]["publication_to_observation_seconds"])
        self.assertEqual(1.0, report["metrics"]["creation_to_publication_seconds"])

        missing = analyze_event(
            [signal, observed, actionable], epoch_id="epoch", event_id="event"
        )
        self.assertEqual("INSUFFICIENT_EVIDENCE", missing["classification"])
        self.assertIn(
            "unique AGENT_SIGNAL_PUBLISHED record", missing["missing_evidence"]
        )

        delayed = published(signal, 6)
        report = analyze_event(
            [signal, delayed, observed, actionable], epoch_id="epoch", event_id="event"
        )
        self.assertEqual("INSUFFICIENT_EVIDENCE", report["classification"])
        self.assertIn(
            "publication follows delivery deadline", report["contradictory_evidence"]
        )

        reversed_publication = published(signal, -1)
        report = analyze_event(
            [signal, reversed_publication, observed, actionable],
            epoch_id="epoch",
            event_id="event",
        )
        self.assertEqual("INSUFFICIENT_EVIDENCE", report["classification"])
        self.assertIn(
            "publication precedes signal creation", report["contradictory_evidence"]
        )

        with self.assertRaisesRegex(AttentionValidationError, "signal_id"):
            make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id="event",
                kind="AGENT_SIGNAL_PUBLISHED",
                metadata={"lane_id": "l", "agent_blocked": True},
            )

    def test_source_rejects_observed_timestamp(self):
        value = rec("HARNESS_EVENT_PENDING", 0, response_deadline_utc=stamp(5))
        value["observed_timestamp_utc"] = stamp(0)
        # Observed timestamp is only accepted when canonical provenance is complete.
        with self.assertRaises(AttentionValidationError):
            validate_record(value)

    def test_signal_id_requires_exact_attention_event_correlation(self):
        exact = make_source_record(
            recorder="test",
            epoch_id="epoch",
            event_id="signal-1",
            kind="AGENT_SIGNAL_CREATED",
            metadata={"lane_id": "l", "agent_blocked": True, "signal_id": "signal-1"},
        )
        self.assertEqual("signal-1", exact["event_id"])
        with self.assertRaisesRegex(
            AttentionValidationError, "signal_id must exactly match event_id"
        ):
            make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id="generated-id",
                kind="AGENT_WAIT_STARTED",
                metadata={
                    "lane_id": "l",
                    "agent_blocked": True,
                    "signal_id": "signal-1",
                },
            )

    def test_signal_deadline_must_not_precede_signal_creation(self):
        # R10 Atlas shape: the declared response deadline was already past at source creation.
        with self.assertRaisesRegex(
            AttentionValidationError, "response_deadline_utc precedes"
        ):
            make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id="atlas",
                kind="AGENT_SIGNAL_CREATED",
                source_timestamp_utc=stamp(10),
                metadata={
                    "lane_id": "l",
                    "agent_blocked": True,
                    "response_deadline_utc": stamp(9),
                },
            )
        # Equality is on time, not impossible.
        self.assertEqual(
            "AGENT_SIGNAL_CREATED",
            make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id="equal",
                kind="AGENT_SIGNAL_CREATED",
                source_timestamp_utc=stamp(10),
                metadata={
                    "lane_id": "l",
                    "agent_blocked": True,
                    "delivery_deadline_utc": stamp(10),
                },
            )["kind"],
        )
        canonical = rec(
            "AGENT_SIGNAL_CREATED",
            0,
            lane_id="l",
            agent_blocked=True,
            response_deadline_utc=stamp(1),
        )
        canonical["response_deadline_utc"] = stamp(-1)
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
        report = analyze_event([canonical], epoch_id="epoch", event_id="event")
        self.assertEqual("INSUFFICIENT_EVIDENCE", report["classification"])
        self.assertTrue(
            any(
                "response_deadline_utc precedes" in value
                for value in report["contradictory_evidence"]
            )
        )

    def test_timely_blocking_response_has_no_blocking_impact(self):
        signal = rec(
            "AGENT_SIGNAL_CREATED",
            0,
            lane_id="l",
            agent_blocked=True,
            response_deadline_utc=stamp(5),
        )
        notification = rec(
            "WATCHER_NOTIFICATION_SENT",
            1,
            wake_transport="collaboration.send_message",
            delivery_succeeded=True,
        )
        invocation = rec(
            "MANAGER_INVOCATION_STARTED",
            1,
            manager_session_id="s",
            manager_invocation_id="i",
        )
        claim = rec(
            "MANAGER_EVENT_CLAIMED",
            2,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        )
        response = rec(
            "MANAGER_RESPONSE_PUBLISHED",
            4,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        )
        receipt = rec("AGENT_RESPONSE_RECEIVED", 8, lane_id="l")
        resumed = rec("AGENT_WORK_RESUMED", 9, lane_id="l")
        self.assertEqual(
            "NO_BLOCKING_IMPACT",
            analyze_event(
                [signal, notification, invocation, claim, response, receipt, resumed],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(
                [signal, claim, response], epoch_id="epoch", event_id="event"
            )["classification"],
        )
        late_notification = rec(
            "WATCHER_NOTIFICATION_SENT",
            6,
            wake_transport="collaboration.send_message",
            delivery_succeeded=True,
        )
        publication = published(signal, 1)
        self.assertEqual(
            "HARNESS_DELIVERY_DELAY",
            analyze_event(
                [signal, publication, late_notification, claim, response],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )
        late_claim = rec(
            "MANAGER_EVENT_CLAIMED",
            7,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        )
        tool_start = rec(
            "MANAGER_TOOL_STARTED",
            1,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="tool",
            manager_state="RUNNING_TOOL",
        )
        tool_end = rec(
            "MANAGER_TOOL_FINISHED",
            8,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="tool",
            manager_state="RUNNING_TOOL",
        )
        self.assertEqual(
            "BUSY_MANAGER_DELAY",
            analyze_event(
                [signal, notification, invocation, late_claim, tool_start, tool_end],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )
        response_before_claim = rec(
            "MANAGER_RESPONSE_PUBLISHED",
            1,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(
                [signal, notification, invocation, claim, response_before_claim],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(
                [signal, notification, claim, response],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )
        early_delivery = rec(
            "AGENT_SIGNAL_CREATED",
            0,
            lane_id="l",
            agent_blocked=True,
            delivery_deadline_utc=stamp(1),
            response_deadline_utc=stamp(5),
        )
        early_publication = published(early_delivery, 1)
        late_actionable = rec("HARNESS_EVENT_ACTIONABLE", 2)
        self.assertEqual(
            "HARNESS_DELIVERY_DELAY",
            analyze_event(
                [
                    early_delivery,
                    early_publication,
                    notification,
                    invocation,
                    claim,
                    response,
                    late_actionable,
                ],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )

    def test_idle_busy_and_silence_are_distinct(self):
        pending = rec("HARNESS_EVENT_PENDING", 0, response_deadline_utc=stamp(5))
        wait_start = rec(
            "MANAGER_WAIT_STARTED",
            0,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="a",
            manager_state="WAITING_ON_TOOL",
        )
        wait_end = rec(
            "MANAGER_WAIT_FINISHED",
            7,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="a",
            manager_state="WAITING_ON_TOOL",
        )
        blocked = rec("AGENT_SIGNAL_CREATED", 0, lane_id="l", agent_blocked=True)
        self.assertEqual(
            analyze_event(
                [blocked, pending, wait_start, wait_end],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
            "INSUFFICIENT_EVIDENCE",
        )
        tool_start = rec(
            "MANAGER_TOOL_STARTED",
            0,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="a",
            manager_state="RUNNING_TOOL",
        )
        tool_end = rec(
            "MANAGER_TOOL_FINISHED",
            7,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="a",
            manager_state="RUNNING_TOOL",
        )
        self.assertEqual(
            analyze_event(
                [pending, tool_start, tool_end], epoch_id="epoch", event_id="event"
            )["classification"],
            "INSUFFICIENT_EVIDENCE",
        )
        notified = rec(
            "WATCHER_NOTIFICATION_SENT",
            1,
            wake_transport="collaboration.send_message",
            delivery_succeeded=True,
        )
        self.assertEqual(
            analyze_event(
                [pending, notified, tool_start, tool_end],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
            "BUSY_MANAGER_DELAY",
        )
        self.assertEqual(
            analyze_event([pending], epoch_id="epoch", event_id="event")[
                "classification"
            ],
            "INSUFFICIENT_EVIDENCE",
        )

    def test_wait_wake_claim_order_accepts_only_sealed_ten_second_bound(self):
        signal = rec("AGENT_SIGNAL_CREATED", 0, lane_id="l", agent_blocked=True)
        pending = rec("HARNESS_EVENT_PENDING", 0, response_deadline_utc=stamp(5))
        start = rec(
            "MANAGER_WAIT_STARTED",
            0,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="wait",
            manager_state="WAITING_ON_TOOL",
        )
        finish = rec(
            "MANAGER_WAIT_FINISHED",
            6,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="wait",
            manager_state="WAITING_ON_TOOL",
            wake_transport="collaboration.send_message",
        )
        claim = rec(
            "MANAGER_EVENT_CLAIMED",
            7,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        )
        notified = rec(
            "WATCHER_NOTIFICATION_SENT",
            1,
            wake_transport="collaboration.send_message",
            delivery_succeeded=True,
        )
        self.assertEqual(
            "IDLE_OR_ABSENT_MANAGER_DELAY",
            analyze_event(
                [signal, notified, pending, start, finish, claim],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )
        late_claim = rec(
            "MANAGER_EVENT_CLAIMED",
            17,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(
                [signal, notified, pending, start, finish, late_claim],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )

    def test_delivery_ack_and_nonblocking(self):
        signal = rec(
            "AGENT_SIGNAL_CREATED",
            0,
            lane_id="l",
            agent_blocked=True,
            delivery_deadline_utc=stamp(4),
        )
        publication = published(signal, 1)
        late = rec("HARNESS_EVENT_ACTIONABLE", 5)
        self.assertEqual(
            analyze_event(
                [signal, publication, late], epoch_id="epoch", event_id="event"
            )["classification"],
            "HARNESS_DELIVERY_DELAY",
        )
        response = rec(
            "MANAGER_RESPONSE_PUBLISHED",
            0,
            manager_session_id="s",
            manager_invocation_id="i",
            ack_deadline_utc=stamp(2),
            response_deadline_utc=stamp(1),
            manager_state="READING_EVENT",
        )
        ack = rec("HARNESS_ACK_SUCCEEDED", 3)
        self.assertEqual(
            analyze_event([response, ack], epoch_id="epoch", event_id="event")[
                "classification"
            ],
            "ACKNOWLEDGEMENT_ONLY_DELAY",
        )
        self.assertEqual(
            analyze_event(
                [
                    rec(
                        "AGENT_SIGNAL_CREATED",
                        0,
                        lane_id="l",
                        agent_blocked=False,
                        non_blocking=True,
                    )
                ],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
            "NO_BLOCKING_IMPACT",
        )

    def test_delivery_deadline_precedes_response_attribution(self):
        signal = rec(
            "AGENT_SIGNAL_CREATED",
            0,
            lane_id="l",
            agent_blocked=True,
            response_deadline_utc=stamp(5),
            delivery_deadline_utc=stamp(2),
        )
        observed = rec("HARNESS_SIGNAL_OBSERVED", 1)
        notified = rec(
            "WATCHER_NOTIFICATION_SENT",
            1,
            wake_transport="collaboration.send_message",
            delivery_succeeded=True,
        )
        pending = rec("HARNESS_EVENT_PENDING", 2, response_deadline_utc=stamp(5))
        late_delivery = rec("HARNESS_EVENT_ACTIONABLE", 7)
        claim = rec(
            "MANAGER_EVENT_CLAIMED",
            8,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        )
        busy_start = rec(
            "MANAGER_TOOL_STARTED",
            0,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="tool",
            manager_state="RUNNING_TOOL",
        )
        busy_end = rec(
            "MANAGER_TOOL_FINISHED",
            9,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="tool",
            manager_state="RUNNING_TOOL",
        )
        report = analyze_event(
            [
                signal,
                observed,
                notified,
                pending,
                late_delivery,
                claim,
                busy_start,
                busy_end,
            ],
            epoch_id="epoch",
            event_id="event",
        )
        self.assertEqual("BUSY_MANAGER_DELAY", report["classification"])
        self.assertEqual(5.0, report["metrics"]["deadline_lateness_seconds"])
        # Only the actual overrun must be explained.  An on-time observation may
        # precede the manager work without making a fully covered late window invalid.
        deadline_busy_start = rec(
            "MANAGER_TOOL_STARTED",
            2,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="deadline-tool",
            manager_state="RUNNING_TOOL",
        )
        deadline_busy_end = rec(
            "MANAGER_TOOL_FINISHED",
            8,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="deadline-tool",
            manager_state="RUNNING_TOOL",
        )
        report = analyze_event(
            [signal, observed, late_delivery, deadline_busy_start, deadline_busy_end],
            epoch_id="epoch",
            event_id="event",
        )
        self.assertEqual("BUSY_MANAGER_DELAY", report["classification"])
        prior_claim = rec_for(
            "prior",
            "MANAGER_EVENT_CLAIMED",
            2,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="HANDLING_OTHER_EVENT",
            related_event_id="prior",
        )
        prior_response = rec_for(
            "prior",
            "MANAGER_RESPONSE_PUBLISHED",
            8,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="HANDLING_OTHER_EVENT",
            related_event_id="prior",
        )
        report = analyze_event(
            [signal, observed, late_delivery, prior_claim, prior_response],
            epoch_id="epoch",
            event_id="event",
        )
        self.assertEqual("BUSY_MANAGER_DELAY", report["classification"])
        wait_start = rec(
            "MANAGER_WAIT_STARTED",
            0,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="wait",
            manager_state="WAITING_ON_TOOL",
        )
        wait_end = rec(
            "MANAGER_WAIT_FINISHED",
            7,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="wait",
            manager_state="WAITING_ON_TOOL",
            wake_transport="blocking_harness_wait_stdout",
            wake_id="b3bbd28a-6cdd-4dd5-9d49-bd6d0c14fc4e",
        )
        publication = published(signal, 1)
        report = analyze_event(
            [
                signal,
                publication,
                observed,
                notified,
                pending,
                late_delivery,
                claim,
                wait_start,
                wait_end,
            ],
            epoch_id="epoch",
            event_id="event",
        )
        self.assertEqual("HARNESS_DELIVERY_DELAY", report["classification"])
        self.assertEqual(5.0, report["metrics"]["deadline_lateness_seconds"])
        partial_wait_start = rec(
            "MANAGER_WAIT_STARTED",
            3,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="partial-wait",
            manager_state="WAITING_ON_TOOL",
        )
        partial_wait_end = rec(
            "MANAGER_WAIT_FINISHED",
            8,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="partial-wait",
            manager_state="WAITING_ON_TOOL",
            wake_transport="blocking_harness_wait_stdout",
            wake_id="f47ac10b-58cc-4372-a567-0e02b2c3d479",
        )
        report = analyze_event(
            [signal, observed, late_delivery, partial_wait_start, partial_wait_end],
            epoch_id="epoch",
            event_id="event",
        )
        self.assertEqual("INSUFFICIENT_EVIDENCE", report["classification"])
        late_observation = rec("HARNESS_SIGNAL_OBSERVED", 3)
        report = analyze_event(
            [
                signal,
                publication,
                late_observation,
                late_delivery,
                busy_start,
                busy_end,
            ],
            epoch_id="epoch",
            event_id="event",
        )
        self.assertEqual("HARNESS_DELIVERY_DELAY", report["classification"])
        incomplete_busy = rec(
            "MANAGER_TOOL_STARTED",
            1,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="incomplete",
            manager_state="RUNNING_TOOL",
        )
        report = analyze_event(
            [signal, observed, late_delivery, incomplete_busy],
            epoch_id="epoch",
            event_id="event",
        )
        self.assertEqual("INSUFFICIENT_EVIDENCE", report["classification"])
        completed_before_late_window_start = rec(
            "MANAGER_TOOL_STARTED",
            0,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="old-tool",
            manager_state="RUNNING_TOOL",
        )
        completed_before_late_window_end = rec(
            "MANAGER_TOOL_FINISHED",
            1,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="old-tool",
            manager_state="RUNNING_TOOL",
        )
        report = analyze_event(
            [
                signal,
                publication,
                observed,
                late_delivery,
                completed_before_late_window_start,
                completed_before_late_window_end,
            ],
            epoch_id="epoch",
            event_id="event",
        )
        self.assertEqual("HARNESS_DELIVERY_DELAY", report["classification"])

    def test_late_actionability_rejects_unclosed_or_overlapping_manager_work(self):
        signal = rec(
            "AGENT_SIGNAL_CREATED",
            0,
            lane_id="l",
            agent_blocked=True,
            delivery_deadline_utc=stamp(2),
        )
        observed = rec("HARNESS_SIGNAL_OBSERVED", 1)
        actionable = rec("HARNESS_EVENT_ACTIONABLE", 7)
        claim = rec_for(
            "other",
            "MANAGER_EVENT_CLAIMED",
            2,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="HANDLING_OTHER_EVENT",
            related_event_id="other",
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(
                [signal, observed, actionable, claim],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )
        review = rec_for(
            "other",
            "MANAGER_REVIEW_STARTED",
            2,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="REASONING",
            formal_review_due_utc=stamp(2),
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(
                [signal, observed, actionable, review],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )
        first_claim = rec_for(
            "first",
            "MANAGER_EVENT_CLAIMED",
            1,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="HANDLING_OTHER_EVENT",
            related_event_id="first",
        )
        first_response = rec_for(
            "first",
            "MANAGER_RESPONSE_PUBLISHED",
            6,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="HANDLING_OTHER_EVENT",
            related_event_id="first",
        )
        second_claim = rec_for(
            "second",
            "MANAGER_EVENT_CLAIMED",
            2,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="HANDLING_OTHER_EVENT",
            related_event_id="second",
        )
        second_response = rec_for(
            "second",
            "MANAGER_RESPONSE_PUBLISHED",
            7,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="HANDLING_OTHER_EVENT",
            related_event_id="second",
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(
                [
                    signal,
                    observed,
                    actionable,
                    first_claim,
                    first_response,
                    second_claim,
                    second_response,
                ],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )

    def test_response_availability_and_delivery_metrics_are_truthful(self):
        signal = rec(
            "AGENT_SIGNAL_CREATED",
            0,
            lane_id="l",
            agent_blocked=True,
            response_deadline_utc=stamp(5),
        )
        publication = published(signal, 1)
        late_notification = rec(
            "WATCHER_NOTIFICATION_SENT",
            7,
            wake_transport="collaboration.send_message",
            delivery_succeeded=True,
        )
        report = analyze_event(
            [signal, publication, late_notification], epoch_id="epoch", event_id="event"
        )
        self.assertEqual("HARNESS_DELIVERY_DELAY", report["classification"])
        self.assertEqual(2.0, report["metrics"]["deadline_lateness_seconds"])
        signal = rec(
            "AGENT_SIGNAL_CREATED",
            0,
            lane_id="l",
            agent_blocked=True,
            delivery_deadline_utc=stamp(4),
        )
        publication = published(signal, 1)
        actionable = rec("HARNESS_EVENT_ACTIONABLE", 7)
        report = analyze_event(
            [signal, publication, actionable], epoch_id="epoch", event_id="event"
        )
        self.assertEqual("HARNESS_DELIVERY_DELAY", report["classification"])
        self.assertEqual(3.0, report["metrics"]["deadline_lateness_seconds"])

    def test_on_time_claim_and_incomplete_manager_interval_do_not_fabricate_delay(self):
        signal = rec(
            "AGENT_SIGNAL_CREATED",
            0,
            lane_id="l",
            agent_blocked=True,
            response_deadline_utc=stamp(5),
        )
        pending = rec("HARNESS_EVENT_PENDING", 1, response_deadline_utc=stamp(5))
        notified = rec(
            "WATCHER_NOTIFICATION_SENT",
            1,
            wake_transport="collaboration.send_message",
            delivery_succeeded=True,
        )
        on_time = rec(
            "MANAGER_EVENT_CLAIMED",
            4,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        )
        self.assertNotIn(
            analyze_event(
                [signal, pending, notified, on_time], epoch_id="epoch", event_id="event"
            )["classification"],
            {
                "BUSY_MANAGER_DELAY",
                "IDLE_OR_ABSENT_MANAGER_DELAY",
                "HARNESS_DELIVERY_DELAY",
            },
        )
        late = rec(
            "MANAGER_EVENT_CLAIMED",
            7,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        )
        start = rec(
            "MANAGER_TOOL_STARTED",
            1,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="tool",
            manager_state="RUNNING_TOOL",
        )
        report = analyze_event(
            [signal, pending, notified, late, start], epoch_id="epoch", event_id="event"
        )
        self.assertEqual("INSUFFICIENT_EVIDENCE", report["classification"])
        self.assertIsNone(report["metrics"]["deadline_lateness_seconds"])

    def test_observation_is_not_availability_and_notification_provenance_is_enforced(
        self,
    ):
        signal = rec(
            "AGENT_SIGNAL_CREATED",
            0,
            lane_id="l",
            agent_blocked=True,
            response_deadline_utc=stamp(5),
        )
        observed = rec("HARNESS_SIGNAL_OBSERVED", 1)
        pending = rec("HARNESS_EVENT_PENDING", 1, response_deadline_utc=stamp(5))
        late = rec(
            "MANAGER_EVENT_CLAIMED",
            7,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        )
        tool_start = rec(
            "MANAGER_TOOL_STARTED",
            1,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="tool",
            manager_state="RUNNING_TOOL",
        )
        tool_end = rec(
            "MANAGER_TOOL_FINISHED",
            8,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="tool",
            manager_state="RUNNING_TOOL",
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(
                [signal, observed, pending, late, tool_start, tool_end],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )
        with self.assertRaisesRegex(
            AttentionValidationError, "successful collaboration.send_message"
        ):
            make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id="event",
                kind="WATCHER_NOTIFICATION_SENT",
                source_timestamp_utc=stamp(1),
                metadata={
                    "wake_transport": "collaboration.send_message",
                    "delivery_succeeded": False,
                },
            )
        sent = make_source_record(
            recorder="test",
            epoch_id="epoch",
            event_id="event",
            kind="WATCHER_NOTIFICATION_SENT",
            source_timestamp_utc=stamp(1),
            metadata={
                "wake_transport": "collaboration.send_message",
                "delivery_succeeded": True,
            },
        )
        with self.assertRaisesRegex(AttentionValidationError, "subagent provenance"):
            canonicalize(
                sent,
                observed_timestamp_utc=stamp(1),
                source_path="source",
                source_role="harness",
                source_id="harness",
                source_generation="g",
                byte_start=0,
                byte_end=1,
            )
        receipt = make_source_record(
            recorder="test",
            epoch_id="epoch",
            event_id="event",
            kind="MANAGER_WAIT_FINISHED",
            source_timestamp_utc=stamp(2),
            metadata={
                "manager_session_id": "s",
                "manager_invocation_id": "i",
                "activity_id": "wait",
                "wake_transport": "collaboration.send_message",
            },
        )
        with self.assertRaisesRegex(
            AttentionValidationError, "wake receipt requires orchestrator provenance"
        ):
            canonicalize(
                receipt,
                observed_timestamp_utc=stamp(2),
                source_path="source",
                source_role="harness",
                source_id="harness",
                source_generation="g",
                byte_start=0,
                byte_end=1,
            )

    def test_wait_finished_wake_receipt_corrobates_successful_notification(self):
        signal = rec(
            "AGENT_SIGNAL_CREATED",
            0,
            lane_id="l",
            agent_blocked=True,
            response_deadline_utc=stamp(5),
        )
        notified = rec(
            "WATCHER_NOTIFICATION_SENT",
            1,
            wake_transport="collaboration.send_message",
            delivery_succeeded=True,
        )
        pending = rec("HARNESS_EVENT_PENDING", 1, response_deadline_utc=stamp(5))
        started = rec(
            "MANAGER_WAIT_STARTED",
            0,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="wait",
            manager_state="WAITING_ON_TOOL",
        )
        finished = rec(
            "MANAGER_WAIT_FINISHED",
            6,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="wait",
            wake_transport="collaboration.send_message",
        )
        claim = rec(
            "MANAGER_EVENT_CLAIMED",
            7,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        )
        report = analyze_event(
            [signal, notified, pending, started, finished, claim],
            epoch_id="epoch",
            event_id="event",
        )
        self.assertEqual("IDLE_OR_ABSENT_MANAGER_DELAY", report["classification"])
        self.assertEqual(2.0, report["metrics"]["deadline_lateness_seconds"])

    def test_canonical_and_collision_safe_append(self):
        source = make_source_record(
            recorder="test",
            epoch_id="epoch",
            event_id="event",
            kind="AGENT_SIGNAL_CREATED",
            source_timestamp_utc=stamp(0),
            metadata={"lane_id": "l", "agent_blocked": True},
        )
        can = canonicalize(
            source,
            observed_timestamp_utc=stamp(),
            source_path="x",
            source_role="subagent",
            source_id="test",
            source_generation="g",
            byte_start=0,
            byte_end=1,
        )
        self.assertEqual(can["source_record_sha256"], source_digest(source))
        with tempfile.TemporaryDirectory() as root:
            path = append_producer_record(
                Path(root), role="subagent", source_id="test", record=source
            )
            append_producer_record(
                Path(root), role="subagent", source_id="test", record=source
            )
            self.assertEqual(len(path.read_text().splitlines()), 1)
            altered = dict(source)
            altered["event_id"] = "other"
            with self.assertRaises(AttentionConflictError):
                append_producer_record(
                    Path(root), role="subagent", source_id="test", record=altered
                )

    def test_exact_post_observation_non_live_evidence_fails_closed_without_blame(self):
        signal = rec(
            "AGENT_SIGNAL_CREATED",
            1,
            lane_id="l",
            agent_blocked=True,
            delivery_deadline_utc=stamp(5),
        )
        publication = published(signal, 2)
        observed = rec("HARNESS_SIGNAL_OBSERVED", 3, harness_event_id="harness-event")
        scan = rec(
            "HARNESS_SCAN_COMMITTED",
            6,
            coverage_start_utc=stamp(0),
            coverage_end_utc=stamp(6),
            scan_sequence=1,
            previous_committed_scan_record_id=None,
            harness_pid=1,
            harness_created_utc="x",
            output_root_identity="root",
            cursor_complete=True,
            source_complete=True,
        )
        trusted = {
            "source": {
                "generation": "g",
                "offset": 1,
                "partial_bytes": 0,
                "error": False,
            }
        }
        ineligible = rec(
            "HARNESS_EVENT_INELIGIBLE",
            4,
            harness_event_id="harness-event",
            ineligibility_reason="LANE_NOT_LIVE",
        )
        for reason in ("ALREADY_ANSWERED", "INVALID_LANE_ID", "LANE_NOT_LIVE"):
            candidate = rec(
                "HARNESS_EVENT_INELIGIBLE",
                4,
                harness_event_id="harness-event",
                ineligibility_reason=reason,
            )
            report = analyze_event(
                [signal, publication, observed, candidate, scan],
                epoch_id="epoch",
                event_id="event",
                trusted_coverage=trusted,
            )
            self.assertEqual("INSUFFICIENT_EVIDENCE", report["classification"])
            self.assertIn(
                "manager signal was explicitly ineligible: " + reason,
                report["missing_evidence"],
            )
        for bad in (
            rec(
                "HARNESS_EVENT_INELIGIBLE",
                4,
                harness_event_id="wrong",
                ineligibility_reason="LANE_NOT_LIVE",
            ),
            rec(
                "HARNESS_EVENT_INELIGIBLE",
                2,
                harness_event_id="harness-event",
                ineligibility_reason="LANE_NOT_LIVE",
            ),
            rec(
                "HARNESS_EVENT_INELIGIBLE",
                4,
                harness_event_id="harness-event",
                ineligibility_reason="UNSUPPORTED",
            ),
        ):
            self.assertEqual(
                "HARNESS_DELIVERY_DELAY",
                analyze_event(
                    [signal, publication, observed, bad, scan],
                    epoch_id="epoch",
                    event_id="event",
                    trusted_coverage=trusted,
                )["classification"],
            )
        duplicate = rec(
            "HARNESS_EVENT_INELIGIBLE",
            4,
            harness_event_id="harness-event",
            ineligibility_reason="LANE_NOT_LIVE",
        )
        self.assertEqual(
            "HARNESS_DELIVERY_DELAY",
            analyze_event(
                [signal, publication, observed, ineligible, duplicate, scan],
                epoch_id="epoch",
                event_id="event",
                trusted_coverage=trusted,
            )["classification"],
        )
        actionable = rec(
            "HARNESS_EVENT_ACTIONABLE", 7, harness_event_id="harness-event"
        )
        self.assertEqual(
            "HARNESS_DELIVERY_DELAY",
            analyze_event(
                [signal, publication, observed, ineligible, actionable],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )

    def test_missing_delivery_requires_complete_scan_coverage(self):
        signal = rec(
            "AGENT_SIGNAL_CREATED",
            1,
            lane_id="l",
            agent_blocked=True,
            delivery_deadline_utc=stamp(5),
        )
        publication = published(signal, 2)
        self.assertEqual(
            analyze_event([signal], epoch_id="epoch", event_id="event")[
                "classification"
            ],
            "INSUFFICIENT_EVIDENCE",
        )
        scan = rec(
            "HARNESS_SCAN_COMMITTED",
            6,
            coverage_start_utc=stamp(0),
            coverage_end_utc=stamp(6),
            scan_sequence=1,
            previous_committed_scan_record_id=None,
            harness_pid=1,
            harness_created_utc="x",
            output_root_identity="root",
            cursor_complete=True,
            source_complete=True,
        )
        self.assertEqual(
            analyze_event([signal, scan], epoch_id="epoch", event_id="event")[
                "classification"
            ],
            "INSUFFICIENT_EVIDENCE",
        )
        trusted = {
            "source": {
                "generation": "g",
                "offset": 1,
                "partial_bytes": 0,
                "error": False,
            }
        }
        self.assertEqual(
            analyze_event(
                [signal, publication, scan],
                epoch_id="epoch",
                event_id="event",
                trusted_coverage=trusted,
            )["classification"],
            "HARNESS_DELIVERY_DELAY",
        )
        trusted["source"]["partial_bytes"] = 1
        self.assertEqual(
            analyze_event(
                [signal, scan],
                epoch_id="epoch",
                event_id="event",
                trusted_coverage=trusted,
            )["classification"],
            "INSUFFICIENT_EVIDENCE",
        )

    def test_absence_requires_authority_and_busy_other_event_is_epoch_wide(self):
        pending = rec("HARNESS_EVENT_PENDING", 0, response_deadline_utc=stamp(5))
        weak = make_source_record(
            recorder="test",
            epoch_id="epoch",
            event_id="event",
            kind="MANAGER_ABSENCE_STARTED",
            source_timestamp_utc=stamp(0),
            metadata={
                "activity_id": "a",
                "supervisor_id": "s",
                "session_lock_id": "l",
                "registry_generation": "g",
                "registered_invocation_ids": ["i"],
                "process_snapshot_hash": "h",
                "registered_manager_identities": "pid:1",
                "process_snapshot_complete": True,
            },
        )
        # no matching boundary: it cannot prove absence.
        self.assertEqual(
            analyze_event([pending, weak], epoch_id="epoch", event_id="event")[
                "classification"
            ],
            "INSUFFICIENT_EVIDENCE",
        )
        other_start = canonicalize(
            make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id="other",
                kind="MANAGER_TOOL_STARTED",
                source_timestamp_utc=stamp(0),
                metadata={
                    "manager_session_id": "s",
                    "manager_invocation_id": "i",
                    "activity_id": "tool",
                    "manager_state": "RUNNING_TOOL",
                },
            ),
            observed_timestamp_utc=stamp(0),
            source_path="s",
            source_role="harness",
            source_id="h",
            source_generation="g",
            byte_start=0,
            byte_end=1,
        )
        other_end = canonicalize(
            make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id="other",
                kind="MANAGER_TOOL_FINISHED",
                source_timestamp_utc=stamp(6),
                metadata={
                    "manager_session_id": "s",
                    "manager_invocation_id": "i",
                    "activity_id": "tool",
                    "manager_state": "RUNNING_TOOL",
                },
            ),
            observed_timestamp_utc=stamp(6),
            source_path="s",
            source_role="harness",
            source_id="h",
            source_generation="g",
            byte_start=1,
            byte_end=2,
        )
        notified = rec(
            "WATCHER_NOTIFICATION_SENT",
            1,
            wake_transport="collaboration.send_message",
            delivery_succeeded=True,
        )
        self.assertEqual(
            analyze_event(
                [pending, notified, other_start, other_end],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
            "BUSY_MANAGER_DELAY",
        )

    def test_canonical_provenance_and_activity_id_controls(self):
        source = make_source_record(
            recorder="test",
            epoch_id="epoch",
            event_id="event",
            kind="AGENT_SIGNAL_CREATED",
            source_timestamp_utc=stamp(0),
            metadata={"lane_id": "l", "agent_blocked": True},
        )
        with self.assertRaises(AttentionValidationError):
            canonicalize(
                source,
                observed_timestamp_utc=stamp(),
                source_path="x",
                source_role="bad",
                source_id="test",
                source_generation="g",
                byte_start=0,
                byte_end=1,
            )
        pending = rec("HARNESS_EVENT_PENDING", 0, response_deadline_utc=stamp(5))
        start = rec(
            "MANAGER_WAIT_STARTED",
            0,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="a",
        )
        end = rec(
            "MANAGER_WAIT_FINISHED",
            6,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="b",
        )
        self.assertEqual(
            analyze_event([pending, start, end], epoch_id="epoch", event_id="event")[
                "classification"
            ],
            "INSUFFICIENT_EVIDENCE",
        )

    def test_overlapping_manager_activity_refuses_idle_cause(self):
        signal = rec("AGENT_SIGNAL_CREATED", 0, lane_id="l", agent_blocked=True)
        pending = rec("HARNESS_EVENT_PENDING", 0, response_deadline_utc=stamp(5))
        wait = rec(
            "MANAGER_WAIT_STARTED",
            0,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="wait",
            manager_state="WAITING_ON_TOOL",
        )
        tool = rec(
            "MANAGER_TOOL_STARTED",
            1,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="tool",
            manager_state="RUNNING_TOOL",
        )
        finish = rec(
            "MANAGER_WAIT_FINISHED",
            6,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="wait",
            manager_state="WAITING_ON_TOOL",
            wake_transport="collaboration.send_message",
        )
        self.assertEqual(
            analyze_event(
                [signal, pending, wait, tool, finish],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
            "INSUFFICIENT_EVIDENCE",
        )

    def test_scan_event_id_is_independent_and_gap_refuses_delivery(self):
        signal = rec(
            "AGENT_SIGNAL_CREATED",
            1,
            lane_id="l",
            agent_blocked=True,
            delivery_deadline_utc=stamp(8),
        )
        common = {
            "scan_sequence": 1,
            "previous_committed_scan_record_id": None,
            "harness_pid": 1,
            "harness_created_utc": "x",
            "output_root_identity": "root",
            "cursor_complete": True,
            "source_complete": True,
        }
        scan = canonicalize(
            make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id="scan-1",
                kind="HARNESS_SCAN_COMMITTED",
                source_timestamp_utc=stamp(4),
                metadata={
                    **common,
                    "coverage_start_utc": stamp(0),
                    "coverage_end_utc": stamp(4),
                },
            ),
            observed_timestamp_utc=stamp(4),
            source_path="s",
            source_role="harness",
            source_id="h",
            source_generation="g",
            byte_start=0,
            byte_end=1,
        )
        scan2 = canonicalize(
            make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id="scan-2",
                kind="HARNESS_SCAN_COMMITTED",
                source_timestamp_utc=stamp(9),
                metadata={
                    **common,
                    "scan_sequence": 2,
                    "previous_committed_scan_record_id": scan["record_id"],
                    "coverage_start_utc": stamp(6),
                    "coverage_end_utc": stamp(9),
                },
            ),
            observed_timestamp_utc=stamp(9),
            source_path="s",
            source_role="harness",
            source_id="h",
            source_generation="g",
            byte_start=1,
            byte_end=2,
        )
        self.assertEqual(
            analyze_event([signal, scan, scan2], epoch_id="epoch", event_id="event")[
                "classification"
            ],
            "INSUFFICIENT_EVIDENCE",
        )

    def test_formal_baseline_and_utc_only(self):
        with self.assertRaises(AttentionValidationError):
            make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id="event",
                kind="HARNESS_EVENT_PENDING",
                source_timestamp_utc="2026-01-01T00:00:00+01:00",
            )
        review = rec(
            "MANAGER_RESPONSE_PUBLISHED",
            0,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
            response_deadline_utc=stamp(1),
            formal_review_due_utc=stamp(2),
        )
        review_start = rec(
            "MANAGER_REVIEW_STARTED",
            0,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
            formal_review_due_utc=stamp(2),
        )
        decision = rec(
            "MANAGER_DECISION_RECORDED",
            1,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="REASONING",
        )
        baseline = rec(
            "FORMAL_REVIEW_BASELINE_ADVANCED", 3, formal_review_due_utc=stamp(2)
        )
        self.assertEqual(
            analyze_event(
                [review_start, decision, review, baseline],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
            "ACKNOWLEDGEMENT_ONLY_DELAY",
        )
        self.assertEqual(
            analyze_event([review, baseline], epoch_id="epoch", event_id="event")[
                "classification"
            ],
            "INSUFFICIENT_EVIDENCE",
        )

    def test_causal_interval_chain_and_formal_review_deadlines(self):
        def item(event, kind, t, **metadata):
            if (
                kind in PENDING_SNAPSHOT_KINDS
                and "pending_work_snapshot" not in metadata
            ):
                metadata["pending_work_snapshot"] = {
                    "complete": False,
                    "events": [],
                    "selected_event_id": None,
                    "selection_reason": "UNKNOWN",
                }
            role = (
                "subagent"
                if kind == "WATCHER_NOTIFICATION_SENT"
                else "orchestrator"
                if kind.startswith("MANAGER_")
                else "harness"
            )
            source = make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id=event,
                kind=kind,
                source_timestamp_utc=stamp(t),
                metadata=metadata,
            )
            return canonicalize(
                source,
                observed_timestamp_utc=stamp(t),
                source_path="source",
                source_role=role,
                source_id="test" if role != "harness" else "harness",
                source_generation="g",
                byte_start=0,
                byte_end=1,
            )

        # Notification -> wait -> two exact other-event handling intervals -> late claim.
        common = [
            item(
                "event",
                "AGENT_SIGNAL_CREATED",
                0,
                lane_id="l",
                agent_blocked=True,
                response_deadline_utc=stamp(4),
            ),
            item(
                "event",
                "WATCHER_NOTIFICATION_SENT",
                1,
                wake_transport="collaboration.send_message",
                delivery_succeeded=True,
            ),
            item(
                "event",
                "MANAGER_WAIT_STARTED",
                1,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="wait",
                manager_state="WAITING_ON_TOOL",
            ),
            item(
                "event",
                "MANAGER_WAIT_FINISHED",
                3,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="wait",
                manager_state="WAITING_ON_TOOL",
            ),
            item(
                "other-1",
                "MANAGER_EVENT_CLAIMED",
                3,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="HANDLING_OTHER_EVENT",
                related_event_id="other-1",
            ),
            item(
                "other-1",
                "MANAGER_RESPONSE_PUBLISHED",
                5,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="HANDLING_OTHER_EVENT",
                related_event_id="other-1",
            ),
            item(
                "other-2",
                "MANAGER_EVENT_CLAIMED",
                5,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="HANDLING_OTHER_EVENT",
                related_event_id="other-2",
            ),
            item(
                "other-2",
                "MANAGER_RESPONSE_PUBLISHED",
                8,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="HANDLING_OTHER_EVENT",
                related_event_id="other-2",
            ),
            item(
                "event",
                "MANAGER_EVENT_CLAIMED",
                8,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="READING_EVENT",
            ),
        ]
        self.assertEqual(
            "BUSY_MANAGER_DELAY",
            analyze_event(common, epoch_id="epoch", event_id="event")["classification"],
        )
        gap = [r for r in common if not (r["event_id"] == "other-2")]
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(gap, epoch_id="epoch", event_id="event")["classification"],
        )
        idle = [
            item(
                "event",
                "AGENT_SIGNAL_CREATED",
                0,
                lane_id="l",
                agent_blocked=True,
                response_deadline_utc=stamp(2),
            ),
            item(
                "event",
                "WATCHER_NOTIFICATION_SENT",
                0,
                wake_transport="collaboration.send_message",
                delivery_succeeded=True,
            ),
            item(
                "event",
                "MANAGER_WAIT_STARTED",
                0,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="w",
                manager_state="WAITING_ON_TOOL",
            ),
            item(
                "event",
                "MANAGER_WAIT_FINISHED",
                4,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="w",
                manager_state="WAITING_ON_TOOL",
            ),
            item(
                "event",
                "MANAGER_EVENT_CLAIMED",
                4,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="READING_EVENT",
            ),
        ]
        self.assertEqual(
            "IDLE_OR_ABSENT_MANAGER_DELAY",
            analyze_event(idle, epoch_id="epoch", event_id="event")["classification"],
        )
        # A review due while another exact event is being handled is busy, not an acknowledgement delay.
        formal = [
            item(
                "other",
                "MANAGER_EVENT_CLAIMED",
                1,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="HANDLING_OTHER_EVENT",
                related_event_id="other",
            ),
            item(
                "other",
                "MANAGER_RESPONSE_PUBLISHED",
                5,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="HANDLING_OTHER_EVENT",
                related_event_id="other",
            ),
            item(
                "review",
                "MANAGER_REVIEW_STARTED",
                5,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="READING_EVENT",
                formal_review_due_utc=stamp(1),
            ),
        ]
        self.assertEqual(
            "BUSY_MANAGER_DELAY",
            analyze_event(formal, epoch_id="epoch", event_id="review")[
                "classification"
            ],
        )
        formal_idle = [
            item(
                "review",
                "MANAGER_WAIT_STARTED",
                1,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="w",
                manager_state="WAITING_ON_TOOL",
            ),
            item(
                "review",
                "MANAGER_WAIT_FINISHED",
                5,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="w",
                manager_state="WAITING_ON_TOOL",
            ),
            item(
                "review",
                "MANAGER_REVIEW_STARTED",
                5,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="READING_EVENT",
                formal_review_due_utc=stamp(1),
            ),
        ]
        self.assertEqual(
            "IDLE_OR_ABSENT_MANAGER_DELAY",
            analyze_event(formal_idle, epoch_id="epoch", event_id="review")[
                "classification"
            ],
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event([formal[0], formal[-1]], epoch_id="epoch", event_id="review")[
                "classification"
            ],
        )

    def test_late_endpoint_invocation_and_terminal_precedence(self):
        def item(event, kind, t, **metadata):
            if (
                kind in PENDING_SNAPSHOT_KINDS
                and "pending_work_snapshot" not in metadata
            ):
                metadata["pending_work_snapshot"] = {
                    "complete": False,
                    "events": [],
                    "selected_event_id": None,
                    "selection_reason": "UNKNOWN",
                }
            role = (
                "subagent"
                if kind == "WATCHER_NOTIFICATION_SENT"
                else "orchestrator"
                if kind.startswith("MANAGER_")
                else "harness"
            )
            source = make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id=event,
                kind=kind,
                source_timestamp_utc=stamp(t),
                metadata=metadata,
            )
            return canonicalize(
                source,
                observed_timestamp_utc=stamp(t),
                source_path="source",
                source_role=role,
                source_id="test" if role != "harness" else "harness",
                source_generation="g",
                byte_start=0,
                byte_end=1,
            )

        base = [
            item(
                "event",
                "AGENT_SIGNAL_CREATED",
                0,
                lane_id="l",
                agent_blocked=True,
                response_deadline_utc=stamp(2),
            ),
            item(
                "event",
                "WATCHER_NOTIFICATION_SENT",
                0,
                wake_transport="collaboration.send_message",
                delivery_succeeded=True,
            ),
            item(
                "other",
                "MANAGER_TOOL_STARTED",
                0,
                manager_session_id="s",
                manager_invocation_id="A",
                activity_id="a",
                manager_state="RUNNING_TOOL",
            ),
            item(
                "other",
                "MANAGER_TOOL_FINISHED",
                4,
                manager_session_id="s",
                manager_invocation_id="A",
                activity_id="a",
                manager_state="RUNNING_TOOL",
            ),
            item(
                "event",
                "MANAGER_EVENT_CLAIMED",
                4,
                manager_session_id="s",
                manager_invocation_id="B",
                manager_state="READING_EVENT",
            ),
        ]
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(base, epoch_id="epoch", event_id="event")["classification"],
        )
        formal = [
            item(
                "other",
                "MANAGER_TOOL_STARTED",
                0,
                manager_session_id="s",
                manager_invocation_id="A",
                activity_id="a",
                manager_state="RUNNING_TOOL",
            ),
            item(
                "other",
                "MANAGER_TOOL_FINISHED",
                4,
                manager_session_id="s",
                manager_invocation_id="A",
                activity_id="a",
                manager_state="RUNNING_TOOL",
            ),
            item(
                "review",
                "MANAGER_REVIEW_STARTED",
                4,
                manager_session_id="s",
                manager_invocation_id="B",
                manager_state="READING_EVENT",
                formal_review_due_utc=stamp(0),
            ),
        ]
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(formal, epoch_id="epoch", event_id="review")[
                "classification"
            ],
        )
        ontime = item(
            "review",
            "MANAGER_REVIEW_STARTED",
            1,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
            formal_review_due_utc=stamp(1),
        )
        self.assertEqual(
            "NO_BLOCKING_IMPACT",
            analyze_event([ontime], epoch_id="epoch", event_id="review")[
                "classification"
            ],
        )
        preferred = [
            item(
                "event",
                "AGENT_SIGNAL_CREATED",
                0,
                lane_id="l",
                agent_blocked=True,
                response_deadline_utc=stamp(2),
            ),
            item(
                "event",
                "WATCHER_NOTIFICATION_SENT",
                0,
                wake_transport="collaboration.send_message",
                delivery_succeeded=True,
            ),
            item(
                "other",
                "MANAGER_EVENT_CLAIMED",
                0,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="HANDLING_OTHER_EVENT",
                related_event_id="other",
            ),
            item(
                "other",
                "MANAGER_RESPONSE_PUBLISHED",
                4,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="HANDLING_OTHER_EVENT",
                related_event_id="other",
            ),
            item(
                "other",
                "MANAGER_CHECKPOINT_COMPLETED",
                5,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="CHECKPOINTING",
                terminal_for_activity=True,
            ),
            item(
                "event",
                "MANAGER_EVENT_CLAIMED",
                4,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="READING_EVENT",
            ),
        ]
        self.assertEqual(
            "BUSY_MANAGER_DELAY",
            analyze_event(preferred, epoch_id="epoch", event_id="event")[
                "classification"
            ],
        )

    def test_explicit_continuity_links_bridge_real_recorder_gaps_only(self):
        def item(event, kind, t, **metadata):
            if (
                kind in PENDING_SNAPSHOT_KINDS
                and "pending_work_snapshot" not in metadata
            ):
                metadata["pending_work_snapshot"] = {
                    "complete": False,
                    "events": [],
                    "selected_event_id": None,
                    "selection_reason": "UNKNOWN",
                }
            role = (
                "subagent"
                if kind == "WATCHER_NOTIFICATION_SENT"
                else "orchestrator"
                if kind.startswith("MANAGER_")
                else "harness"
            )
            source = make_source_record(
                recorder="test",
                epoch_id="epoch",
                event_id=event,
                kind=kind,
                source_timestamp_utc=stamp(t),
                metadata=metadata,
            )
            return canonicalize(
                source,
                observed_timestamp_utc=stamp(t),
                source_path="source",
                source_role=role,
                source_id="test" if role != "harness" else "harness",
                source_generation="g",
                byte_start=0,
                byte_end=1,
            )

        signal = item(
            "event",
            "AGENT_SIGNAL_CREATED",
            0,
            lane_id="l",
            agent_blocked=True,
            response_deadline_utc=stamp(2),
        )
        notice = item(
            "event",
            "WATCHER_NOTIFICATION_SENT",
            0,
            wake_transport="collaboration.send_message",
            delivery_succeeded=True,
        )
        wait_start = item(
            "event",
            "MANAGER_WAIT_STARTED",
            0,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="wait",
            manager_state="WAITING_ON_TOOL",
        )
        wait_end = item(
            "event",
            "MANAGER_WAIT_FINISHED",
            3,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="wait",
            manager_state="WAITING_ON_TOOL",
        )
        tool_start = item(
            "other",
            "MANAGER_TOOL_STARTED",
            5,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="tool",
            manager_state="RUNNING_TOOL",
            continuous_from_record_id=wait_end["record_id"],
        )
        tool_end = item(
            "other",
            "MANAGER_TOOL_FINISHED",
            6,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="tool",
            manager_state="RUNNING_TOOL",
        )
        claim = item(
            "event",
            "MANAGER_EVENT_CLAIMED",
            8,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
            continuous_from_record_id=tool_end["record_id"],
        )
        self.assertEqual(
            "BUSY_MANAGER_DELAY",
            analyze_event(
                [signal, notice, wait_start, wait_end, tool_start, tool_end, claim],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )
        no_link = item(
            "event",
            "MANAGER_EVENT_CLAIMED",
            8,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(
                [signal, notice, wait_start, wait_end, tool_start, tool_end, no_link],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )
        wrong = item(
            "event",
            "MANAGER_EVENT_CLAIMED",
            8,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
            continuous_from_record_id=notice["record_id"],
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(
                [signal, notice, wait_start, wait_end, wrong],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )
        foreign_end = item(
            "other",
            "MANAGER_TOOL_FINISHED",
            6,
            manager_session_id="s",
            manager_invocation_id="foreign",
            activity_id="foreign",
            manager_state="RUNNING_TOOL",
        )
        cross = item(
            "event",
            "MANAGER_EVENT_CLAIMED",
            8,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
            continuous_from_record_id=foreign_end["record_id"],
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(
                [signal, notice, wait_start, wait_end, foreign_end, cross],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )
        nonterminal = item(
            "other",
            "MANAGER_EVENT_CLAIMED",
            6,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        )
        bad_terminal = item(
            "event",
            "MANAGER_EVENT_CLAIMED",
            8,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
            continuous_from_record_id=nonterminal["record_id"],
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(
                [signal, notice, wait_start, wait_end, nonterminal, bad_terminal],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )
        fork_a = item(
            "event",
            "MANAGER_EVENT_CLAIMED",
            8,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
            continuous_from_record_id=wait_end["record_id"],
        )
        fork_b = item(
            "other",
            "MANAGER_TOOL_STARTED",
            7,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="fork",
            manager_state="RUNNING_TOOL",
            continuous_from_record_id=wait_end["record_id"],
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(
                [signal, notice, wait_start, wait_end, fork_a, fork_b],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )
        self_response = item(
            "other",
            "MANAGER_RESPONSE_PUBLISHED",
            6,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        )
        equal_time = item(
            "event",
            "MANAGER_EVENT_CLAIMED",
            6,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
            continuous_from_record_id=self_response["record_id"],
        )
        self.assertFalse(
            any(
                interval["start_record_id"] == self_response["record_id"]
                and interval["end_record_id"] == equal_time["record_id"]
                for interval in _paired_manager_intervals([self_response, equal_time])
            )
        )
        future_end = item(
            "other",
            "MANAGER_RESPONSE_PUBLISHED",
            9,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        )
        forward = item(
            "event",
            "MANAGER_EVENT_CLAIMED",
            8,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
            continuous_from_record_id=future_end["record_id"],
        )
        self.assertEqual(
            "INSUFFICIENT_EVIDENCE",
            analyze_event(
                [signal, notice, wait_start, wait_end, future_end, forward],
                epoch_id="epoch",
                event_id="event",
            )["classification"],
        )


if __name__ == "__main__":
    unittest.main()
