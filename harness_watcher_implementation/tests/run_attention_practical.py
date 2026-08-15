"""Synthetic, host-only practical attention diagnosis and wake smoke."""

import json
import os
import subprocess
import sys
import tempfile
import threading
import argparse
import shutil
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from datetime import datetime, timedelta, timezone
from harness_watcher_implementation.attention import (
    AttentionValidationError,
    analyze_event,
    canonicalize,
    make_source_record,
)


def ts(seconds):
    return (
        datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=seconds)
    ).isoformat()


def record(kind, second, event="e", **meta):
    if (
        kind
        in {
            "HARNESS_EVENT_PENDING",
            "FORMAL_REVIEW_BASELINE_ADVANCED",
            "MANAGER_INVOCATION_STARTED",
            "MANAGER_INVOCATION_FINISHED",
        }
        and "pending_work_snapshot" not in meta
    ):
        meta["pending_work_snapshot"] = {
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
        recorder="practical",
        epoch_id="practical",
        event_id=event,
        kind=kind,
        source_timestamp_utc=ts(second),
        metadata=meta,
    )
    return canonicalize(
        source,
        observed_timestamp_utc=ts(second),
        source_path="synthetic",
        source_role=role,
        source_id="practical" if role in {"subagent", "orchestrator"} else "synthetic",
        source_generation="0",
        byte_start=second,
        byte_end=second + 1,
    )


def check(expected, records):
    actual = analyze_event(records, epoch_id="practical", event_id="e")[
        "classification"
    ]
    if actual != expected:
        raise AssertionError(f"expected {expected}, got {actual}")


def published(signal, second):
    meta = {
        "lane_id": signal["lane_id"],
        "agent_blocked": signal["agent_blocked"],
        "signal_id": signal["event_id"],
    }
    for field in ("delivery_deadline_utc", "response_deadline_utc"):
        if field in signal:
            meta[field] = signal[field]
    return record("AGENT_SIGNAL_PUBLISHED", second, **meta)


def production_wake_smoke(evidence_dir=None):
    """Host-only exercise of the current public host-delivery seams.

    The wake half admits one actionable event into a fresh ManagerEventRouter
    queue, produces one sparse DeliveryNotice (no event ID or payload), obtains
    one DELIVERED boundary receipt from the synthetic Codex adapter, and proves
    delivery does not acknowledge the pending event.  The quiet half uses a
    second fresh empty queue/coordinator and proves no notice and no transport
    call.  This replaces the retired diagnostic-watch manager-flag route.
    """
    from orchestrator_harness.codex_adapter import (
        CodexAdapter,
        SyntheticCodexTransport,
    )
    from orchestrator_harness.host_adapters import (
        DELIVERY_NOTICE_SCHEMA,
        DeliveryCoordinator,
        FutureHostFixture,
    )
    from orchestrator_harness.notifications import ManagerEventRouter

    if evidence_dir is not None:
        destination = Path(evidence_dir)
        if destination.exists() and any(destination.iterdir()):
            raise AssertionError("evidence directory must be fresh and empty")
        destination.mkdir(parents=True, exist_ok=True)

    def build(manager_root):
        router = ManagerEventRouter(
            manager_root,
            run_id="practical-run",
            queue_id="practical-queue",
            manager_session_id="practical-session",
            manager_thread_id="practical-thread",
            registration_id="practical-registration",
            manager_invocation_id="practical-invocation",
        )
        transport = SyntheticCodexTransport()
        coordinator = DeliveryCoordinator(
            router=router, adapter=FutureHostFixture("codex-bootstrap")
        )
        CodexAdapter(transport, coordinator)
        coordinator.register()
        return router, coordinator, transport

    with tempfile.TemporaryDirectory(prefix="attention-wake-") as wake_name:
        router, coordinator, transport = build(Path(wake_name))
        event_id = "wake"
        admitted = router.admit(
            {
                "event_id": event_id,
                "type": "MANAGER_SIGNAL",
                "identity": "synthetic:practical:wake",
                "data": {
                    "signal_id": event_id,
                    "lane_id": "practical:lane",
                    "manager_actionable": True,
                    "severity": "warning",
                },
            },
            priority=2,
            binding=coordinator.binding,
        )
        assert admitted is not None and admitted["event_id"] == event_id, admitted
        notice = coordinator.notice_for_wake()
        assert notice is not None, "wake queue produced no delivery notice"
        notice_record = notice.as_record()
        assert notice_record["schema"] == DELIVERY_NOTICE_SCHEMA, notice_record
        for forbidden in ("event_id", "event_ids", "data", "payload"):
            assert forbidden not in notice_record, (
                f"delivery notice contains {forbidden}"
            )
        assert notice.pending_count == 1, notice_record
        receipt = coordinator.deliver_at_boundary(notice, boundary="post_tool_use")
        assert receipt is not None and receipt.outcome == "DELIVERED", receipt
        assert receipt.boundary == "post_tool_use", receipt.as_record()
        assert receipt.notice_id == notice.notice_id, receipt.as_record()
        pending_after = router.pending_events()
        assert [item["event_id"] for item in pending_after] == [event_id], (
            "delivery acknowledged queue work"
        )
        assert len(transport.calls) == 1, transport.calls
        call = transport.calls[0]
        assert call["method"] == "PostToolUse", call
        assert call["notice"]["schema"] == DELIVERY_NOTICE_SCHEMA, call
        assert "event_id" not in call["notice"], call
        deliveries = router.read_deliveries()
        assert len(deliveries) == 1 and deliveries[0]["event_ids"] == [], deliveries
        wake_evidence = {
            "schema": "orchestrator-practical-wake/v1",
            "notice": notice_record,
            "receipt": receipt.as_record(),
            "pending_after_delivery": len(pending_after),
            "pending_event_ids": [item["event_id"] for item in pending_after],
            "acknowledged_by_delivery": False,
            "transport_calls": list(transport.calls),
            "delivery_journal": deliveries,
        }

    with tempfile.TemporaryDirectory(prefix="attention-quiet-") as quiet_name:
        quiet_router, quiet_coordinator, quiet_transport = build(Path(quiet_name))
        quiet_notice = quiet_coordinator.notice_for_wake()
        assert quiet_notice is None, "empty queue produced a delivery notice"
        quiet_receipt = quiet_coordinator.deliver_at_boundary(
            quiet_notice, boundary="post_tool_use"
        )
        assert quiet_receipt is None, "empty queue produced a delivery receipt"
        assert quiet_transport.calls == [], quiet_transport.calls
        assert quiet_router.pending_events() == [], quiet_router.pending_events()
        quiet_evidence = {
            "schema": "orchestrator-practical-quiet/v1",
            "notice": None,
            "receipt": None,
            "pending_count": 0,
            "transport_calls": list(quiet_transport.calls),
        }

    if evidence_dir is not None:
        (destination / "wake-result.json").write_text(
            json.dumps(wake_evidence, indent=2), encoding="utf-8"
        )
        (destination / "quiet-result.json").write_text(
            json.dumps(quiet_evidence, indent=2), encoding="utf-8"
        )
    return wake_evidence, quiet_evidence


def main(evidence_dir=None):
    try:
        make_source_record(
            recorder="practical",
            epoch_id="practical",
            event_id="impossible",
            kind="AGENT_SIGNAL_CREATED",
            source_timestamp_utc=ts(5),
            metadata={
                "lane_id": "lane",
                "agent_blocked": True,
                "response_deadline_utc": ts(4),
            },
        )
    except AttentionValidationError:
        pass
    else:
        raise AssertionError("impossible signal deadline accepted")
    signal = record(
        "AGENT_SIGNAL_CREATED",
        0,
        lane_id="lane",
        agent_blocked=True,
        delivery_deadline_utc=ts(2),
    )
    delivery = analyze_event(
        [signal, published(signal, 1), record("HARNESS_EVENT_ACTIONABLE", 3)],
        epoch_id="practical",
        event_id="e",
    )
    if (
        delivery["classification"] != "HARNESS_DELIVERY_DELAY"
        or delivery["metrics"]["deadline_lateness_seconds"] != 1.0
    ):
        raise AssertionError("delivery lateness")
    composite = [
        record(
            "AGENT_SIGNAL_CREATED",
            0,
            lane_id="lane",
            agent_blocked=True,
            response_deadline_utc=ts(2),
            delivery_deadline_utc=ts(1),
        ),
        record("HARNESS_SIGNAL_OBSERVED", 0),
        record(
            "WATCHER_NOTIFICATION_SENT",
            1,
            wake_transport="collaboration.send_message",
            delivery_succeeded=True,
        ),
        record("HARNESS_EVENT_ACTIONABLE", 4),
        record("HARNESS_EVENT_PENDING", 1, response_deadline_utc=ts(2)),
        record(
            "MANAGER_EVENT_CLAIMED",
            5,
            manager_session_id="s",
            manager_invocation_id="i",
            manager_state="READING_EVENT",
        ),
        record(
            "MANAGER_TOOL_STARTED",
            0,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="tool",
            manager_state="RUNNING_TOOL",
        ),
        record(
            "MANAGER_TOOL_FINISHED",
            6,
            manager_session_id="s",
            manager_invocation_id="i",
            activity_id="tool",
            manager_state="RUNNING_TOOL",
        ),
    ]
    composite_report = analyze_event(composite, epoch_id="practical", event_id="e")
    if (
        composite_report["classification"] != "BUSY_MANAGER_DELAY"
        or composite_report["metrics"]["deadline_lateness_seconds"] != 3.0
    ):
        raise AssertionError(
            "on-time observation with complete busy coverage of the late window"
        )
    check(
        "INSUFFICIENT_EVIDENCE",
        [
            record(
                "AGENT_SIGNAL_CREATED",
                0,
                lane_id="lane",
                agent_blocked=True,
                delivery_deadline_utc=ts(2),
            ),
            record("HARNESS_SIGNAL_OBSERVED", 1),
            record(
                "MANAGER_WAIT_STARTED",
                3,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="partial-native-wait",
                manager_state="WAITING_ON_TOOL",
            ),
            record("HARNESS_EVENT_ACTIONABLE", 7),
            record(
                "MANAGER_WAIT_FINISHED",
                8,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="partial-native-wait",
                manager_state="WAITING_ON_TOOL",
                wake_transport="blocking_harness_wait_stdout",
                wake_id="f47ac10b-58cc-4372-a567-0e02b2c3d479",
            ),
        ],
    )
    pending = record("HARNESS_EVENT_PENDING", 0, response_deadline_utc=ts(2))
    check(
        "IDLE_OR_ABSENT_MANAGER_DELAY",
        [
            record("AGENT_SIGNAL_CREATED", 0, lane_id="lane", agent_blocked=True),
            record(
                "WATCHER_NOTIFICATION_SENT",
                1,
                wake_transport="collaboration.send_message",
                delivery_succeeded=True,
            ),
            pending,
            record(
                "MANAGER_WAIT_STARTED",
                0,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="wait",
                manager_state="WAITING_ON_TOOL",
            ),
            record(
                "MANAGER_WAIT_FINISHED",
                3,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="wait",
                manager_state="WAITING_ON_TOOL",
                wake_transport="collaboration.send_message",
            ),
        ],
    )
    check(
        "BUSY_MANAGER_DELAY",
        [
            pending,
            record(
                "WATCHER_NOTIFICATION_SENT",
                1,
                wake_transport="collaboration.send_message",
                delivery_succeeded=True,
            ),
            record(
                "MANAGER_TOOL_STARTED",
                0,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="tool",
                manager_state="RUNNING_TOOL",
            ),
            record(
                "MANAGER_TOOL_FINISHED",
                3,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="tool",
                manager_state="RUNNING_TOOL",
            ),
        ],
    )
    # A fully covered chain: wait, then two successive explicit tool intervals,
    # then the late exact claim.  No timing tolerance fills any gap.
    check(
        "BUSY_MANAGER_DELAY",
        [
            record(
                "AGENT_SIGNAL_CREATED",
                0,
                lane_id="lane",
                agent_blocked=True,
                response_deadline_utc=ts(3),
            ),
            record(
                "WATCHER_NOTIFICATION_SENT",
                1,
                wake_transport="collaboration.send_message",
                delivery_succeeded=True,
            ),
            record(
                "MANAGER_WAIT_STARTED",
                1,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="wait",
                manager_state="WAITING_ON_TOOL",
            ),
            record(
                "MANAGER_WAIT_FINISHED",
                2,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="wait",
                manager_state="WAITING_ON_TOOL",
            ),
            record(
                "MANAGER_TOOL_STARTED",
                2,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="tool-1",
                manager_state="RUNNING_TOOL",
            ),
            record(
                "MANAGER_TOOL_FINISHED",
                4,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="tool-1",
                manager_state="RUNNING_TOOL",
            ),
            record(
                "MANAGER_TOOL_STARTED",
                4,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="tool-2",
                manager_state="RUNNING_TOOL",
            ),
            record(
                "MANAGER_TOOL_FINISHED",
                6,
                manager_session_id="s",
                manager_invocation_id="i",
                activity_id="tool-2",
                manager_state="RUNNING_TOOL",
            ),
            record(
                "MANAGER_EVENT_CLAIMED",
                6,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="READING_EVENT",
            ),
        ],
    )
    # Separate CLI invocations have real timestamp gaps; the recorded terminal
    # ID explicitly links the next activity rather than relying on a tolerance.
    signal = record(
        "AGENT_SIGNAL_CREATED",
        0,
        lane_id="lane",
        agent_blocked=True,
        response_deadline_utc=ts(2),
    )
    notice = record(
        "WATCHER_NOTIFICATION_SENT",
        0,
        wake_transport="collaboration.send_message",
        delivery_succeeded=True,
    )
    wait_start = record(
        "MANAGER_WAIT_STARTED",
        0,
        manager_session_id="s",
        manager_invocation_id="i",
        activity_id="wait-link",
        manager_state="WAITING_ON_TOOL",
    )
    wait_end = record(
        "MANAGER_WAIT_FINISHED",
        2,
        manager_session_id="s",
        manager_invocation_id="i",
        activity_id="wait-link",
        manager_state="WAITING_ON_TOOL",
    )
    tool_start = record(
        "MANAGER_TOOL_STARTED",
        4,
        manager_session_id="s",
        manager_invocation_id="i",
        activity_id="tool-link",
        manager_state="RUNNING_TOOL",
        continuous_from_record_id=wait_end["record_id"],
    )
    tool_end = record(
        "MANAGER_TOOL_FINISHED",
        5,
        manager_session_id="s",
        manager_invocation_id="i",
        activity_id="tool-link",
        manager_state="RUNNING_TOOL",
    )
    linked_claim = record(
        "MANAGER_EVENT_CLAIMED",
        7,
        manager_session_id="s",
        manager_invocation_id="i",
        manager_state="READING_EVENT",
        continuous_from_record_id=tool_end["record_id"],
    )
    check(
        "BUSY_MANAGER_DELAY",
        [signal, notice, wait_start, wait_end, tool_start, tool_end, linked_claim],
    )
    check(
        "ACKNOWLEDGEMENT_ONLY_DELAY",
        [
            record(
                "MANAGER_RESPONSE_PUBLISHED",
                0,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="READING_EVENT",
                response_deadline_utc=ts(1),
                ack_deadline_utc=ts(2),
            ),
            record("HARNESS_ACK_SUCCEEDED", 3),
        ],
    )
    # Timely watcher notification, exact manager claim, and exact response are
    # healthy even if the agent has not yet received or resumed the work.
    check(
        "NO_BLOCKING_IMPACT",
        [
            record(
                "AGENT_SIGNAL_CREATED",
                0,
                lane_id="lane",
                agent_blocked=True,
                response_deadline_utc=ts(4),
            ),
            record(
                "WATCHER_NOTIFICATION_SENT",
                1,
                wake_transport="collaboration.send_message",
                delivery_succeeded=True,
            ),
            record(
                "MANAGER_INVOCATION_STARTED",
                1,
                manager_session_id="s",
                manager_invocation_id="i",
            ),
            record(
                "MANAGER_EVENT_CLAIMED",
                2,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="READING_EVENT",
            ),
            record(
                "MANAGER_RESPONSE_PUBLISHED",
                3,
                manager_session_id="s",
                manager_invocation_id="i",
                manager_state="READING_EVENT",
            ),
            record("AGENT_RESPONSE_RECEIVED", 7, lane_id="lane"),
            record("AGENT_WORK_RESUMED", 8, lane_id="lane"),
        ],
    )
    check(
        "NO_BLOCKING_IMPACT",
        [
            record(
                "AGENT_SIGNAL_CREATED",
                0,
                lane_id="lane",
                agent_blocked=False,
                non_blocking=True,
            )
        ],
    )
    check("INSUFFICIENT_EVIDENCE", [pending])
    # The disabled producer command exits before config/path loading: gate-matrix no-op.
    from harness_watcher_implementation.__main__ import main as cli_main
    from harness_watcher_implementation import settings

    previous = settings.harness_watcher_active
    settings.harness_watcher_active = False
    try:
        if (
            cli_main(
                [
                    "record-attention",
                    "--role",
                    "subagent",
                    "--source-id",
                    "lane",
                    "--epoch-id",
                    "e",
                    "--event-id",
                    "x",
                    "--kind",
                    "AGENT_SIGNAL_CREATED",
                ]
            )
            != 0
        ):
            raise AssertionError("disabled CLI gate")
    finally:
        settings.harness_watcher_active = previous
    production_wake_smoke(evidence_dir)
    print(
        "attention practical host-only check: PASS (R10 deadline-origin + healthy blocking path, R9 continuity precedence, CLI disabled gate)"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-dir")
    args = parser.parse_args()
    main(args.evidence_dir)
