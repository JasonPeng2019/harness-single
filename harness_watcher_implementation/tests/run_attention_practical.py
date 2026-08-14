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
    if evidence_dir is not None:
        destination = Path(evidence_dir)
        if destination.exists() and any(destination.iterdir()):
            raise AssertionError("evidence directory must be fresh and empty")
        destination.mkdir(parents=True, exist_ok=True)
    from orchestrator_harness.tests.support import SuiteFixture, write_json
    from orchestrator_harness.config import load_config as load_harness_config
    from harness_watcher_implementation.config import load_config as load_watcher_config
    from harness_watcher_implementation.attention import ingest_attention, producer_path

    root = Path(__file__).resolve().parents[2]
    fixture = SuiteFixture.create()
    quiet_fixture = None
    processes = []
    timers = []
    try:
        fixture.suite_root.joinpath("multi-agent-logs").mkdir(
            parents=True, exist_ok=True
        )
        (root / "multi-agent-logs").mkdir(parents=True, exist_ok=True)
        with (
            tempfile.TemporaryDirectory(
                prefix="attention-harness-", dir=fixture.suite_root / "multi-agent-logs"
            ) as harness_output_name,
            tempfile.TemporaryDirectory(
                prefix="attention-runtime-", dir=root / "multi-agent-logs"
            ) as runtime_name,
        ):
            harness_output = Path(harness_output_name).resolve()
            runtime_root = Path(runtime_name).resolve()
            epoch = "A00_test"
            session = "practical-session"
            invocation = "practical-invocation"
            event_id = "wake"
            lane_id = "A00_test:Atlas:A00"
            activity_id = "blocking-wait"

            # Configure the real harness and watcher before producing any attention record.
            raw = json.loads(fixture.config_path.read_text(encoding="utf-8"))
            raw.update(
                {
                    "attention_logging_enabled": True,
                    "attention_epoch_id": epoch,
                    "output_dir": str(harness_output),
                }
            )
            write_json(fixture.config_path, raw)
            fixture.config = load_harness_config(
                fixture.config_path, harness_root=fixture.harness_root
            )
            fixture.workspace()
            from orchestrator_harness.models import iso_utc
            from orchestrator_harness.processes import process_snapshot

            self_process = process_snapshot().by_pid.get(os.getpid())
            assert self_process is not None and self_process.created_utc is not None, (
                "host process identity unavailable"
            )
            write_json(
                fixture.workspace() / "helper_process.json",
                {
                    "pid": os.getpid(),
                    "started_utc": iso_utc(self_process.created_utc),
                    "declared_lane_id": lane_id,
                },
            )
            watcher_config = runtime_root / "watcher.json"
            write_json(
                watcher_config,
                {
                    "runtime_root": str(runtime_root),
                    "attention_logging_enabled": True,
                    "attention_epoch_id": epoch,
                    "attention_producers": [
                        {"role": "subagent", "source_id": "lane"},
                        {"role": "orchestrator", "source_id": "root"},
                    ],
                    "observed_sources": [
                        {
                            "path": str(harness_output / "attention-events.jsonl"),
                            "role": "harness",
                            "source_id": "harness",
                        }
                    ],
                },
            )
            watcher = load_watcher_config(watcher_config)

            def record_attention(role, source_id, kind, metadata):
                result = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "harness_watcher_implementation",
                        "--config",
                        str(watcher_config),
                        "record-attention",
                        "--role",
                        role,
                        "--source-id",
                        source_id,
                        "--epoch-id",
                        epoch,
                        "--event-id",
                        event_id,
                        "--kind",
                        kind,
                        "--metadata",
                        json.dumps(metadata),
                    ],
                    cwd=root,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if result.returncode != 0:
                    raise AssertionError(
                        f"{kind} recorder failed: rc={result.returncode}, stdout={result.stdout!r}, stderr={result.stderr!r}"
                    )
                return json.loads(result.stdout)

            record_attention(
                "subagent",
                "lane",
                "AGENT_SIGNAL_CREATED",
                {"lane_id": lane_id, "agent_blocked": True},
            )
            record_attention(
                "orchestrator",
                "root",
                "MANAGER_WAIT_STARTED",
                {
                    "manager_session_id": session,
                    "manager_invocation_id": invocation,
                    "activity_id": activity_id,
                    "manager_state": "WAITING_ON_TOOL",
                },
            )

            signal = fixture.workspace() / "manager-signals" / "wake.json"
            published = threading.Event()

            def publish_signal():
                write_json(
                    signal,
                    {
                        "schema": "manager-signal/v1",
                        "signal_id": event_id,
                        "kind": "HELP",
                        "created_utc": "2026-07-30T12:00:00Z",
                        "lane_id": lane_id,
                        "task": "A00",
                        "phase": "synthetic",
                        "summary": "wake",
                        "evidence_paths": [],
                        "attention_epoch_id": epoch,
                    },
                )
                record_attention(
                    "subagent",
                    "lane",
                    "AGENT_SIGNAL_PUBLISHED",
                    {"lane_id": lane_id, "agent_blocked": True, "signal_id": event_id},
                )
                published.set()

            command = [
                sys.executable,
                "-m",
                "orchestrator_harness",
                "--config",
                str(fixture.config_path),
                "watch",
                "--until-actionable",
                "--timeout",
                "2",
                "--manager-session-id",
                session,
                "--manager-invocation-id",
                invocation,
            ]
            process = subprocess.Popen(
                command,
                cwd=root,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            processes.append(process)
            timer = threading.Timer(0.2, publish_signal)
            timers.append(timer)
            timer.start()
            try:
                stdout, stderr = process.communicate(timeout=10)
            except subprocess.TimeoutExpired as exc:
                process.kill()
                stdout, stderr = process.communicate(timeout=5)
                raise AssertionError(
                    f"blocking wait did not return: stdout={stdout!r}, stderr={stderr!r}"
                ) from exc
            assert process.returncode == 0, (
                f"blocking wait failed: rc={process.returncode}, stdout={stdout!r}, stderr={stderr!r}"
            )
            assert published.is_set(), (
                "synthetic manager signal was not published while the wait ran"
            )
            event = json.loads(stdout)
            assert (
                event["data"]["signal_id"] == event_id
                and event["wake_id"]
                and event["wake_transport"] == "blocking_harness_wait_stdout"
            ), event
            wake_id = event["wake_id"]

            # These are deliberately separate real recorder invocations, in manager order.
            record_attention(
                "orchestrator",
                "root",
                "MANAGER_WAKE_RECEIVED",
                {
                    "wake_id": wake_id,
                    "wake_transport": event["wake_transport"],
                    "manager_session_id": session,
                    "manager_invocation_id": invocation,
                },
            )
            record_attention(
                "orchestrator",
                "root",
                "MANAGER_WAIT_FINISHED",
                {
                    "wake_id": wake_id,
                    "wake_transport": event["wake_transport"],
                    "manager_session_id": session,
                    "manager_invocation_id": invocation,
                    "activity_id": activity_id,
                    "manager_state": "WAITING_ON_TOOL",
                },
            )
            record_attention(
                "orchestrator",
                "root",
                "MANAGER_EVENT_CLAIMED",
                {
                    "manager_session_id": session,
                    "manager_invocation_id": invocation,
                    "manager_state": "READING_EVENT",
                },
            )

            attention_path = harness_output / "attention-events.jsonl"
            assert attention_path.exists(), {
                "harness_output": str(harness_output),
                "observed_sources": [
                    str(source.path) for source in watcher.observed_sources
                ],
            }
            report = ingest_attention(watcher)
            finding = next(
                item for item in report["findings"] if item["event_id"] == event_id
            )
            evidence = finding["wake_evidence"]
            assert (
                evidence["status"] == "COMPLETE" and evidence["wake_id"] == wake_id
            ), {
                "finding": finding,
                "report": report,
                "harness": attention_path.read_text(encoding="utf-8"),
            }
            for metric in (
                "attempted_to_delivered_seconds",
                "delivered_to_received_seconds",
                "received_to_claim_seconds",
            ):
                assert (
                    isinstance(evidence[metric], (int, float)) and evidence[metric] >= 0
                ), evidence
            timeline = json.loads(
                "["
                + ",".join(
                    line
                    for line in (runtime_root / "watcher" / "attention-timeline.jsonl")
                    .read_text(encoding="utf-8")
                    .splitlines()
                )
                + "]"
            )
            wake_kinds = [
                item["kind"] for item in timeline if item.get("event_id") == event_id
            ]
            assert (
                "AGENT_SIGNAL_CREATED" in wake_kinds
                and "AGENT_SIGNAL_PUBLISHED" in wake_kinds
                and "HARNESS_SIGNAL_OBSERVED" in wake_kinds
                and "MANAGER_WAKE_ATTEMPTED" in wake_kinds
                and "MANAGER_WAKE_DELIVERED" in wake_kinds
            ), wake_kinds
            manager_records = [
                item
                for item in timeline
                if item.get("event_id") == event_id
                and item.get("source_role") == "orchestrator"
            ]
            assert [item["kind"] for item in manager_records] == [
                "MANAGER_WAIT_STARTED",
                "MANAGER_WAKE_RECEIVED",
                "MANAGER_WAIT_FINISHED",
                "MANAGER_EVENT_CLAIMED",
            ], manager_records

            # A fresh epoch/output must time out cleanly and never create a production wake stage.
            quiet_fixture = SuiteFixture.create()
            quiet_fixture.suite_root.joinpath("multi-agent-logs").mkdir(
                parents=True, exist_ok=True
            )
            with (
                tempfile.TemporaryDirectory(
                    prefix="attention-quiet-harness-",
                    dir=quiet_fixture.suite_root / "multi-agent-logs",
                ) as quiet_output_name,
                tempfile.TemporaryDirectory(
                    prefix="attention-quiet-runtime-", dir=root / "multi-agent-logs"
                ) as quiet_runtime_name,
            ):
                quiet_output = Path(quiet_output_name).resolve()
                quiet_runtime = Path(quiet_runtime_name).resolve()
                quiet_epoch = "A00_practical_quiet"
                quiet_raw = json.loads(
                    quiet_fixture.config_path.read_text(encoding="utf-8")
                )
                quiet_raw.update(
                    {"attention_epoch_id": quiet_epoch, "output_dir": str(quiet_output)}
                )
                write_json(quiet_fixture.config_path, quiet_raw)
                quiet_config_path = quiet_fixture.config_path
                quiet_watcher_config = quiet_runtime / "watcher.json"
                write_json(
                    quiet_watcher_config,
                    {
                        "runtime_root": str(quiet_runtime),
                        "attention_logging_enabled": True,
                        "attention_epoch_id": quiet_epoch,
                        "attention_producers": [
                            {"role": "subagent", "source_id": "lane"},
                            {"role": "orchestrator", "source_id": "root"},
                        ],
                        "observed_sources": [
                            {
                                "path": str(quiet_output / "attention-events.jsonl"),
                                "role": "harness",
                                "source_id": "harness",
                            }
                        ],
                    },
                )
                quiet_watcher = load_watcher_config(quiet_watcher_config)
                quiet_command = [
                    sys.executable,
                    "-m",
                    "orchestrator_harness",
                    "--config",
                    str(quiet_config_path),
                    "watch",
                    "--until-actionable",
                    "--timeout",
                    "0.3",
                    "--manager-session-id",
                    session,
                    "--manager-invocation-id",
                    invocation,
                ]
                quiet_result = subprocess.run(
                    quiet_command, cwd=root, capture_output=True, text=True, timeout=10
                )
                assert quiet_result.returncode == 3, (
                    f"quiet wait expected rc=3, got {quiet_result.returncode}: {quiet_result.stderr}"
                )
                quiet_event = json.loads(quiet_result.stdout)
                assert "wake_id" not in quiet_event
                quiet_report = ingest_attention(quiet_watcher)
                quiet_event = json.loads(quiet_result.stdout)
                assert (
                    quiet_event["event_id"] == "WATCH_TIMEOUT"
                    and "wake_id" not in quiet_event
                ), quiet_event
                quiet_report = ingest_attention(quiet_watcher)
                quiet_files = [
                    quiet_output / "attention-events.jsonl",
                    producer_path(quiet_runtime, "subagent", "lane"),
                    producer_path(quiet_runtime, "orchestrator", "root"),
                    quiet_runtime / "watcher" / "attention-timeline.jsonl",
                ]
                quiet_records = []
                for path in quiet_files:
                    if path.exists():
                        quiet_records.extend(
                            json.loads(line)
                            for line in path.read_text(encoding="utf-8").splitlines()
                            if line.strip()
                        )
                assert not any(
                    str(item.get("kind", "")).startswith("MANAGER_WAKE_")
                    for item in quiet_records
                ), quiet_records
                assert not any(
                    str(item.get("kind", "")).startswith("MANAGER_WAKE_")
                    for item in quiet_report.get("findings", [])
                ), quiet_report
                if evidence_dir is not None:
                    shutil.copytree(harness_output, destination / "wake-harness")
                    shutil.copytree(runtime_root, destination / "wake-watcher")
                    shutil.copytree(quiet_output, destination / "quiet-harness")
                    shutil.copytree(quiet_runtime, destination / "quiet-watcher")
                    matching = sorted(
                        (
                            item
                            for item in timeline
                            if item.get("event_id") == event_id
                            and item["kind"]
                            in {
                                "AGENT_SIGNAL_CREATED",
                                "AGENT_SIGNAL_PUBLISHED",
                                "HARNESS_SIGNAL_OBSERVED",
                                "MANAGER_WAKE_ATTEMPTED",
                                "MANAGER_WAKE_DELIVERED",
                                "MANAGER_WAKE_RECEIVED",
                                "MANAGER_EVENT_CLAIMED",
                            }
                        ),
                        key=lambda item: item["source_timestamp_utc"],
                    )
                    (destination / "wake-result.json").write_text(
                        json.dumps(
                            {
                                "event": event,
                                "wake_id": wake_id,
                                "wake_evidence": evidence,
                                "records": matching,
                            },
                            indent=2,
                        ),
                        encoding="utf-8",
                    )
                    (destination / "quiet-result.json").write_text(
                        json.dumps(
                            {
                                "timeout_event": quiet_event,
                                "wake_records": [
                                    item
                                    for item in quiet_records
                                    if str(item.get("kind", "")).startswith(
                                        "MANAGER_WAKE_"
                                    )
                                ],
                            },
                            indent=2,
                        ),
                        encoding="utf-8",
                    )
    finally:
        for timer in timers:
            timer.cancel()
        for timer in timers:
            timer.join(timeout=2)
        for process in processes:
            if process.poll() is None:
                process.kill()
            if process.poll() is None:
                process.wait(timeout=5)
        if quiet_fixture is not None:
            quiet_fixture.close()
        fixture.close()


def main(evidence_dir=None):
    from orchestrator_harness.attention_sprint import (
        event_selection_snapshot,
        formal_baseline_snapshot,
        invocation_snapshot,
        validate_sprint_boundary,
        validate_sprint_finalize,
    )

    boundary = validate_sprint_boundary(
        epoch_id="practical-boundary",
        heartbeat_timeout_seconds=11,
        formal_review_interval_seconds=5,
        bounded_lifetime_seconds=10,
    )
    if "kind" in boundary:
        raise AssertionError("boundary helper emitted synthetic activity")
    event = {
        "event_id": "gate",
        "type": "HELP",
        "priority": 1,
        "age_seconds": 0.0,
        "agent_blocked": True,
    }
    inventory = invocation_snapshot([event])
    baseline = formal_baseline_snapshot([event])
    selection = event_selection_snapshot([event], event_id="gate")
    finalize = [
        {
            "epoch_id": "practical-boundary",
            "event_id": "start",
            "kind": "MANAGER_INVOCATION_STARTED",
            "pending_work_snapshot": inventory,
        },
        {
            "epoch_id": "practical-boundary",
            "event_id": "baseline",
            "kind": "FORMAL_REVIEW_BASELINE_ADVANCED",
            "source_role": "orchestrator",
            "pending_work_snapshot": baseline,
        },
        {
            "epoch_id": "practical-boundary",
            "event_id": "gate",
            "kind": "MANAGER_EVENT_CLAIMED",
            "pending_work_snapshot": selection,
        },
        {
            "epoch_id": "practical-boundary",
            "event_id": "gate",
            "kind": "AGENT_SIGNAL_CREATED",
            "agent_blocked": True,
        },
        {
            "epoch_id": "practical-boundary",
            "event_id": "finish",
            "kind": "MANAGER_INVOCATION_FINISHED",
            "pending_work_snapshot": inventory,
        },
    ]
    validate_sprint_finalize(
        [
            *finalize,
            {
                "epoch_id": "practical-boundary",
                "event_id": "gate",
                "kind": "AGENT_RESPONSE_RECEIVED",
            },
            {
                "epoch_id": "practical-boundary",
                "event_id": "gate",
                "kind": "AGENT_WORK_RESUMED",
            },
        ],
        epoch_id="practical-boundary",
    )
    validate_sprint_finalize(
        [
            *finalize,
            {
                "epoch_id": "practical-boundary",
                "event_id": "gate",
                "kind": "AGENT_GATE_EXPIRED",
                "terminal_gate_expired": True,
            },
        ],
        epoch_id="practical-boundary",
    )
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
