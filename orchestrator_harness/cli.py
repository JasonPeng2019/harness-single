from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from .active_management import (
    acknowledge_management_event,
    transition_active_management,
)
from .codex_adapter import (
    CodexAdapterError,
    check_codex_adapter,
    install_codex_adapter,
    run_codex_hook,
    synthetic_wake_self_test,
    uninstall_codex_adapter,
    upgrade_codex_adapter,
)
from .config import ConfigError, HarnessConfig, load_config
from .discovery import discover_suite
from .events import diff_conditions
from .handoff_preflight import exit_code as handoff_preflight_exit_code
from .handoff_preflight import preflight_handoff
from .models import ProcessInfo, ProcessSnapshot, parse_utc, utc_now
from .notifications import (
    _manager_signal_ineligibility_reason,
    _priority,
    admit_deferred_handoffs,
    coalesce_mutable_handoffs,
    preempt_pending_with_higher_priority,
    select_actionable_with_deferred,
)
from .processes import process_snapshot
from .reconcile import reconcile
from .stable_io import PathSafetyError, SafeOutput, canonical_json
from .watcher_integration import acknowledge_watcher_event, merge_watcher_conditions
from .lane_lifecycle import (
    allocate_immutable_source_view,
    retire_terminal_lane,
)

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_TIMEOUT = 3


def _unknown_pending_snapshot() -> dict[str, Any]:
    return {"complete": False, "events": [], "selected_event_id": None, "selection_reason": "UNKNOWN"}


def _pending_work_snapshot(*, selected: dict[str, Any] | None, deferred: list[dict[str, Any]], snapshot: dict[str, Any], observed: datetime, selection_reason: str) -> dict[str, Any]:
    """Render bounded pending metadata, refusing to fill unknown event facts."""
    candidates=[]; seen=set()
    for item in [selected, *(entry.get("event") for entry in deferred if isinstance(entry, dict))]:
        if not isinstance(item,dict) or not isinstance(item.get("event_id"),str) or item["event_id"] in seen: continue
        seen.add(item["event_id"]); candidates.append(item)
    if len(candidates)>128: return _unknown_pending_snapshot()
    rendered=[]
    for event in candidates:
        data=event.get("data") if isinstance(event.get("data"),dict) else {}
        priority=_priority(event,snapshot,observed)
        since=parse_utc(data.get("created_utc") or data.get("observed_utc") or event.get("observed_utc"))
        if not isinstance(event.get("type"),str) or priority is None or since is None or since>observed or not isinstance(data.get("agent_blocked"),bool): return _unknown_pending_snapshot()
        row={"event_id":event["event_id"],"type":event["type"],"priority":priority,"age_seconds":(observed-since).total_seconds(),"agent_blocked":data["agent_blocked"]}
        response_by = data.get("delivery_deadline_utc") or data.get("response_deadline_utc")
        if response_by is not None:
            if parse_utc(response_by) is None: return _unknown_pending_snapshot()
            row["response_deadline_utc"] = response_by
        lease_deadline = data.get("lease_deadline_utc")
        if lease_deadline is not None:
            if parse_utc(lease_deadline) is None: return _unknown_pending_snapshot()
            row["lease_deadline_utc"] = lease_deadline
        rendered.append(row)
    return {"complete": True,"events":rendered,"selected_event_id":selected.get("event_id") if selected else None,"selection_reason":selection_reason}


def _attention_identity(event: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    """Use a manager signal's durable ID while preserving harness provenance."""
    harness_event_id = event.get("event_id")
    if not isinstance(harness_event_id, str):
        raise ValueError("attention event requires a harness event ID")
    data = event.get("data")
    if event.get("type") != "MANAGER_SIGNAL" or not isinstance(data, Mapping):
        return harness_event_id, {}
    signal_id = data.get("signal_id")
    if not isinstance(signal_id, str) or not signal_id:
        return harness_event_id, {}
    metadata: dict[str, Any] = {"harness_event_id": harness_event_id}
    deadline = data.get("deadline_utc")
    if isinstance(deadline, str) and deadline:
        metadata["response_deadline_utc"] = deadline
    delivery_deadline = data.get("delivery_deadline_utc")
    if isinstance(delivery_deadline, str) and delivery_deadline:
        metadata["delivery_deadline_utc"] = delivery_deadline
    return signal_id, metadata


def _is_current_attention_event(store: Any, event: Mapping[str, Any]) -> bool:
    """Admit only harness observations explicitly bound to this attention epoch.

    Reconciliation intentionally sees every retained run.  Attention evidence is
    narrower: a historical condition must not acquire a new causal epoch merely
    because a later harness scan rediscovers it.
    """
    epoch = getattr(store, "attention_epoch_id", None)
    if not isinstance(epoch, str) or not epoch:
        return True
    data = event.get("data")
    if not isinstance(data, Mapping):
        return False
    declared_epoch = data.get("attention_epoch_id")
    # An explicit epoch is authoritative.  In particular, an old explicit
    # marker cannot be accidentally rescued by a reused/current lane name.
    if declared_epoch is not None:
        return declared_epoch == epoch
    lane_id = data.get("lane_id") or data.get("declared_lane_id")
    return isinstance(lane_id, str) and lane_id.startswith(f"{epoch}:")


def _append_event_attention(
    store: Any, *, kind: str, event: Mapping[str, Any], timestamp: datetime | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> None:
    """Append one harness stage with the correlation identity required by its event."""
    if not _is_current_attention_event(store, event):
        return
    event_id, identity_metadata = _attention_identity(event)
    merged = dict(identity_metadata)
    merged.update(dict(metadata or {}))
    store.append_attention(kind=kind, event_id=event_id, metadata=merged or None, timestamp=timestamp)


def observe(
    config: HarnessConfig,
    *,
    process_provider: Callable[[], ProcessSnapshot] = process_snapshot,
    clock: Callable[[], datetime] = utc_now,
) -> tuple[dict[str, Any], datetime]:
    runs = discover_suite(config)
    processes = process_provider()
    now = clock()
    return reconcile(runs, processes, config, now=now), now


def _reconcile_observation(
    config: HarnessConfig, *, processes: ProcessSnapshot, observed_at: datetime
) -> dict[str, Any]:
    return reconcile(discover_suite(config), processes, config, now=observed_at)


def _print_json(value: Any, *, stream: Any = sys.stdout) -> None:
    stream.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
    stream.flush()


def _print_events(events: list[dict[str, Any]], *, stream: Any = sys.stdout) -> None:
    for event in events:
        stream.write(canonical_json(event) + "\n")
    stream.flush()


def _store(config: HarnessConfig) -> SafeOutput:
    store = SafeOutput(
        harness_root=config.harness_root,
        output_root=config.output_dir,
        forbidden_roots=config.forbidden_output_roots,
        allowed_output_roots=(
            config.suite_root / "runtime",
            config.suite_root / "multi-agent-logs",
        ),
        attention_logging_enabled=config.attention_logging_enabled,
        attention_epoch_id=config.attention_epoch_id,
    )
    store.prepare()
    return store


def scan_command(
    config: HarnessConfig,
    *,
    process_provider: Callable[[], ProcessSnapshot] = process_snapshot,
    clock: Callable[[], datetime] = utc_now,
    stream: Any = sys.stdout,
) -> int:
    snapshot, _ = observe(config, process_provider=process_provider, clock=clock)
    _print_json(snapshot, stream=stream)
    return EXIT_OK


def watch_once(
    config: HarnessConfig,
    *,
    no_write: bool,
    process_provider: Callable[[], ProcessSnapshot] = process_snapshot,
    clock: Callable[[], datetime] = utc_now,
    stream: Any = sys.stdout,
    store_factory: Callable[[HarnessConfig], SafeOutput] = _store,
) -> tuple[int, list[dict[str, Any]]]:
    store = None if no_write else store_factory(config)
    prior_cursor = store.load_cursor() if store else None
    previous = prior_cursor.get("conditions") if prior_cursor else None
    snapshot, observed = observe(config, process_provider=process_provider, clock=clock)
    conditions = merge_watcher_conditions(snapshot)
    events = diff_conditions(previous, conditions, observed_at=observed)
    if store:
        store.commit(snapshot=snapshot, events=events, conditions=conditions)
        for event in events:
            if isinstance(event.get("event_id"), str):
                _append_event_attention(store, kind="HARNESS_SIGNAL_OBSERVED", event=event, metadata={"condition_type": event.get("type")})
    _print_events(events, stream=stream)
    return EXIT_OK, events


def watch_until_event(
    config: HarnessConfig,
    *,
    no_write: bool,
    timeout_seconds: float | None,
    process_provider: Callable[[], ProcessSnapshot] = process_snapshot,
    clock: Callable[[], datetime] = utc_now,
    sleeper: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    stream: Any = sys.stdout,
    store_factory: Callable[[HarnessConfig], SafeOutput] = _store,
) -> int:
    store = None if no_write else store_factory(config)
    prior_cursor = store.load_cursor() if store else None
    previous = prior_cursor.get("conditions") if prior_cursor else None
    timeout = (
        config.watch_timeout_seconds if timeout_seconds is None else timeout_seconds
    )
    deadline = monotonic() + timeout
    while True:
        snapshot, observed = observe(
            config, process_provider=process_provider, clock=clock
        )
        conditions = merge_watcher_conditions(snapshot)
        events = diff_conditions(previous, conditions, observed_at=observed)
        if events:
            if store:
                store.commit(snapshot=snapshot, events=events, conditions=conditions)
            _print_events(events, stream=stream)
            return EXIT_OK
        if monotonic() >= deadline:
            _print_events(
                [
                    {
                        "event_id": "WATCH_TIMEOUT",
                        "identity": "watch:timeout",
                        "type": "WATCH_TIMEOUT",
                        "severity": "info",
                        "observed_utc": observed.isoformat(),
                        "data": {"timeout_seconds": timeout},
                    }
                ],
                stream=stream,
            )
            return EXIT_TIMEOUT
        sleeper(min(config.poll_interval_seconds, max(0.0, deadline - monotonic())))


def _deliver_blocking_wake(
    store: SafeOutput, *, event: Mapping[str, Any], stream: Any, timestamp: datetime,
    manager_session_id: str, manager_invocation_id: str,
) -> None:
    """Log one stdout delivery attempt and return its correlation fields to the manager."""
    wake_id = str(uuid.uuid4())
    metadata = {
        "wake_id": wake_id,
        "wake_component": "orchestrator_harness.watch_until_actionable",
        "wake_transport": "blocking_harness_wait_stdout",
        "manager_session_id": manager_session_id,
        "manager_invocation_id": manager_invocation_id,
    }
    _append_event_attention(store, kind="MANAGER_WAKE_ATTEMPTED", event=event, timestamp=timestamp, metadata=metadata)
    delivered_event = dict(event)
    delivered_event.update({key: metadata[key] for key in ("wake_id", "wake_component", "wake_transport")})
    try:
        stream.write(canonical_json(delivered_event) + "\n")
        stream.flush()
    except Exception as exc:
        try:
            _append_event_attention(store, kind="MANAGER_WAKE_FAILED", event=event, timestamp=timestamp, metadata={**metadata, "delivery_succeeded": False, "failure_kind": type(exc).__name__})
        except Exception:
            pass
        raise
    _append_event_attention(store, kind="MANAGER_WAKE_DELIVERED", event=event, timestamp=timestamp, metadata={**metadata, "delivery_succeeded": True})


def _managed_consumer_runtime(store: SafeOutput, observed: datetime, process_provider: Callable[[], ProcessSnapshot] = process_snapshot, *, required: bool = False) -> bool:
    runtime = store.load_managed_runtime()
    if runtime is None:
        if required: raise ValueError("managed watcher runtime disappeared")
        return False
    processes = process_provider()
    if not processes.complete or runtime.get("stop_requested") or runtime.get("exit_reason") is not None:
        raise ValueError("managed watcher runtime is not live")
    for identity in ("watcher", "owner"):
        if store._process_identity_status(runtime, identity, processes) != "live":
            raise ValueError("managed watcher runtime identity is not live")
    expiry=parse_utc(runtime.get("lease_expires_utc"))
    if expiry is None or observed >= expiry: raise ValueError("managed watcher runtime lease expired")
    return True


def _consume_managed_notification(store: SafeOutput, *, process_provider: Callable[[], ProcessSnapshot], poll_interval_seconds: float, timeout: float, clock: Callable[[], datetime], sleeper: Callable[[float], None], monotonic: Callable[[], float], stream: Any, manager_session_id: str | None, manager_invocation_id: str | None, attention_enabled: bool) -> int:
    deadline=monotonic()+timeout
    while True:
        observed=clock(); _managed_consumer_runtime(store,observed,process_provider,required=True)
        pending=store.load_notification_state().get("pending")
        if isinstance(pending,dict) and isinstance(pending.get("event_id"),str):
            _managed_consumer_runtime(store, observed, process_provider, required=True)
            if attention_enabled:
                assert manager_session_id is not None and manager_invocation_id is not None
                _deliver_blocking_wake(store,event=pending,stream=stream,timestamp=observed,manager_session_id=manager_session_id,manager_invocation_id=manager_invocation_id)
            else: _print_events([pending],stream=stream)
            return EXIT_OK
        if monotonic() >= deadline:
            _print_events([{"event_id":"WATCH_TIMEOUT","identity":"watch:timeout","type":"WATCH_TIMEOUT","severity":"info","observed_utc":observed.isoformat(),"data":{"timeout_seconds":timeout}}],stream=stream)
            return EXIT_TIMEOUT
        sleeper(min(poll_interval_seconds,max(0.0,deadline-monotonic())))


def watch_until_actionable(
    config: HarnessConfig,
    *,
    timeout_seconds: float | None,
    process_provider: Callable[[], ProcessSnapshot] = process_snapshot,
    clock: Callable[[], datetime] = utc_now,
    sleeper: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    stream: Any = sys.stdout,
    store_factory: Callable[[HarnessConfig], SafeOutput] = _store,
    manager_session_id: str | None = None,
    manager_invocation_id: str | None = None,
) -> int:
    """Consume a managed owner's durable notification or run standalone scan-and-select."""
    if config.attention_logging_enabled and (not manager_session_id or not manager_invocation_id):
        raise ValueError("attention-enabled --until-actionable requires manager session and invocation IDs")
    store = store_factory(config)
    timeout = config.watch_timeout_seconds if timeout_seconds is None else timeout_seconds
    managed_runtime = store.load_managed_runtime()
    if managed_runtime is not None:
        _managed_consumer_runtime(store, clock(), process_provider, required=True)
        return _consume_managed_notification(store, process_provider=process_provider, poll_interval_seconds=config.poll_interval_seconds, timeout=timeout, clock=clock, sleeper=sleeper, monotonic=monotonic, stream=stream, manager_session_id=manager_session_id, manager_invocation_id=manager_invocation_id, attention_enabled=config.attention_logging_enabled)
    notification_state = store.load_notification_state()
    pending = notification_state.get("pending")
    if isinstance(pending, dict) and isinstance(pending.get("event_id"), str):
        if config.attention_logging_enabled:
            assert manager_session_id is not None and manager_invocation_id is not None
            _deliver_blocking_wake(store, event=pending, stream=stream, timestamp=clock(), manager_session_id=manager_session_id, manager_invocation_id=manager_invocation_id)
        else:
            _print_events([pending], stream=stream)
        return EXIT_OK

    prior_cursor = store.load_cursor()
    previous = prior_cursor.get("conditions") if prior_cursor else None
    timeout = config.watch_timeout_seconds if timeout_seconds is None else timeout_seconds
    deadline = monotonic() + timeout
    while True:
        snapshot, observed = observe(config, process_provider=process_provider, clock=clock)
        conditions = merge_watcher_conditions(snapshot)
        had_prior_conditions = previous is not None
        events = diff_conditions(previous, conditions, observed_at=observed)
        store.commit(snapshot=snapshot, events=events, conditions=conditions)
        for event in events:
            if not isinstance(event.get("event_id"), str):
                continue
            _append_event_attention(store, kind="HARNESS_SIGNAL_OBSERVED", event=event, timestamp=observed, metadata={"condition_type": event.get("type")})
            if event.get("type") == "MANAGER_SIGNAL" and isinstance(event.get("data"), Mapping):
                reason = _manager_signal_ineligibility_reason(dict(event["data"]), snapshot, observed)
                if reason is not None:
                    _append_event_attention(
                        store, kind="HARNESS_EVENT_INELIGIBLE", event=event,
                        timestamp=observed, metadata={"ineligibility_reason": reason},
                    )
        acknowledged = {
            item for item in notification_state.get("acknowledged_event_ids", []) if isinstance(item, str)
        }
        newly = {event["event_id"] for event in events} if had_prior_conditions else set()
        deferred = admit_deferred_handoffs(
            conditions, snapshot, observed_at=observed,
            acknowledged_event_ids=acknowledged, newly_observed_event_ids=newly,
            existing=notification_state.get("deferred", []),
        )
        selected, deferred = select_actionable_with_deferred(
            conditions, snapshot, observed_at=observed,
            acknowledged_event_ids=acknowledged, newly_observed_event_ids=newly,
            deferred=deferred,
        )
        if selected is not None:
            store.save_notification_state(
                pending=selected,
                acknowledged_event_ids=list(acknowledged),
                deferred=deferred,
            )
            _append_event_attention(store, kind="HARNESS_EVENT_ACTIONABLE", event=selected, timestamp=observed)
            _append_event_attention(store, kind="HARNESS_EVENT_PENDING", event=selected, metadata={"pending_work_snapshot":_pending_work_snapshot(selected=selected,deferred=deferred,snapshot=snapshot,observed=observed,selection_reason="SELECT_ACTIONABLE")}, timestamp=observed)
            if config.attention_logging_enabled:
                assert manager_session_id is not None and manager_invocation_id is not None
                _deliver_blocking_wake(store, event=selected, stream=stream, timestamp=observed, manager_session_id=manager_session_id, manager_invocation_id=manager_invocation_id)
            else:
                _print_events([selected], stream=stream)
            return EXIT_OK
        store.save_notification_state(
            pending=None, acknowledged_event_ids=list(acknowledged), deferred=deferred,
        )
        previous = conditions
        if monotonic() >= deadline:
            _print_events(
                [{
                    "event_id": "WATCH_TIMEOUT",
                    "identity": "watch:timeout",
                    "type": "WATCH_TIMEOUT",
                    "severity": "info",
                    "observed_utc": observed.isoformat(),
                    "data": {"timeout_seconds": timeout},
                }],
                stream=stream,
            )
            return EXIT_TIMEOUT
        sleeper(min(config.poll_interval_seconds, max(0.0, deadline - monotonic())))


def _managed_processes(
    processes: ProcessSnapshot,
    *,
    watcher: ProcessInfo | None,
    owner: ProcessInfo | None,
) -> tuple[ProcessInfo, ProcessInfo]:
    current = watcher or processes.by_pid.get(os.getpid())
    launching = owner or processes.by_pid.get(os.getppid())
    if current is None:
        raise ValueError("managed watcher process is absent from the process inventory")
    if launching is None:
        raise ValueError("managed watcher owner is absent from the process inventory")
    return current, launching


def _managed_exit(
    store: SafeOutput,
    *,
    watcher: ProcessInfo,
    reason: str,
    observed_at: datetime,
) -> int:
    store.record_managed_watcher_exit(
        watcher=watcher, reason=reason, exited_at=observed_at
    )
    return EXIT_OK


def watch_managed(
    config: HarnessConfig,
    *,
    process_provider: Callable[[], ProcessSnapshot] = process_snapshot,
    clock: Callable[[], datetime] = utc_now,
    sleeper: Callable[[float], None] = time.sleep,
    stream: Any = sys.stdout,
    store_factory: Callable[[HarnessConfig], SafeOutput] = _store,
    watcher: ProcessInfo | None = None,
    owner: ProcessInfo | None = None,
) -> int:
    """Run one foreground, acknowledgement-rearmed managed watcher."""
    store = store_factory(config)
    initial_processes = process_provider()
    watcher_process, owner_process = _managed_processes(
        initial_processes, watcher=watcher, owner=owner
    )
    store.claim_managed_watcher(
        watcher=watcher_process,
        owner=owner_process,
        processes=initial_processes,
        heartbeat_timeout_seconds=config.manager_heartbeat_timeout_seconds,
        started_at=clock(),
    )
    prior_cursor = store.load_cursor()
    previous = prior_cursor.get("conditions") if prior_cursor else None
    emitted_pending_ids: set[str] = set()

    while True:
        runs = discover_suite(config)
        processes = process_provider()
        observed = clock()
        ownership = store.managed_watcher_ownership_status(
            watcher=watcher_process,
            owner=owner_process,
            processes=processes,
        )
        if ownership in {"owner-absent", "owner-reused"}:
            return _managed_exit(
                store,
                watcher=watcher_process,
                reason="owner-identity-lost",
                observed_at=observed,
            )
        if ownership in {
            "runtime-missing",
            "watcher-mismatch",
            "owner-mismatch",
            "watcher-absent",
            "watcher-reused",
        }:
            return _managed_exit(
                store,
                watcher=watcher_process,
                reason="watcher-ownership-lost",
                observed_at=observed,
            )

        runtime = store.load_managed_runtime()
        assert runtime is not None
        if runtime.get("stop_requested", False):
            return _managed_exit(
                store,
                watcher=watcher_process,
                reason="stop-requested",
                observed_at=observed,
            )
        lease_expiry = parse_utc(runtime.get("lease_expires_utc"))
        if lease_expiry is not None and observed >= lease_expiry:
            return _managed_exit(
                store,
                watcher=watcher_process,
                reason="manager-heartbeat-expired",
                observed_at=observed,
            )

        snapshot = reconcile(runs, processes, config, now=observed)
        base_conditions = merge_watcher_conditions(snapshot)
        history = store.load_active_management_history()
        history, management_conditions = transition_active_management(
            snapshot, history, config, observed_at=observed
        )
        conditions = {
            **base_conditions,
            **{item["identity"]: item for item in management_conditions},
        }
        had_prior_conditions = previous is not None
        events = diff_conditions(previous, conditions, observed_at=observed)
        store.commit(snapshot=snapshot, events=events, conditions=conditions)
        for event in events:
            if isinstance(event.get("event_id"), str):
                _append_event_attention(store, kind="HARNESS_SIGNAL_OBSERVED", event=event, metadata={"condition_type": event.get("type")}, timestamp=observed)
        store.save_active_management_history(history)
        previous = conditions

        notification_state = store.load_notification_state()
        acknowledged = {
            item for item in notification_state.get("acknowledged_event_ids", [])
            if isinstance(item, str)
        }
        newly = {event["event_id"] for event in events} if had_prior_conditions else set()
        deferred = admit_deferred_handoffs(
            conditions, snapshot, observed_at=observed,
            acknowledged_event_ids=acknowledged, newly_observed_event_ids=newly,
            existing=notification_state.get("deferred", []),
        )
        pending, deferred = coalesce_mutable_handoffs(
            notification_state.get("pending"), deferred, observed_at=observed,
        )
        pending, deferred, preempted = preempt_pending_with_higher_priority(
            pending, deferred, snapshot, observed_at=observed,
        )
        # Admission/coalescing and any strict-priority correction commit together.
        store.save_notification_state(
            pending=pending,
            acknowledged_event_ids=list(acknowledged), deferred=deferred,
        )
        for item in deferred:
            event = item.get("event") if isinstance(item, dict) else None
            if isinstance(event, dict) and isinstance(event.get("event_id"), str):
                _append_event_attention(store, kind="HARNESS_EVENT_DEFERRED", event=event, metadata={"related_event_id": pending.get("event_id") if isinstance(pending, dict) else None}, timestamp=observed)
        if preempted and isinstance(pending, dict):
            _append_event_attention(store, kind="HARNESS_EVENT_ACTIONABLE", event=pending, timestamp=observed)
            _append_event_attention(store, kind="HARNESS_EVENT_PENDING", event=pending, metadata={"pending_work_snapshot":_pending_work_snapshot(selected=pending,deferred=deferred,snapshot=snapshot,observed=observed,selection_reason="PREEMPT_PENDING")}, timestamp=observed)
        notification_state = store.load_notification_state()
        pending = notification_state.get("pending")
        if isinstance(pending, dict) and isinstance(pending.get("event_id"), str):
            event_id = pending["event_id"]
            if event_id not in emitted_pending_ids:
                _print_events([pending], stream=stream)
                emitted_pending_ids.add(event_id)
            sleeper(config.poll_interval_seconds)
            continue

        selected, deferred = select_actionable_with_deferred(
            conditions, snapshot, observed_at=observed,
            acknowledged_event_ids=acknowledged, newly_observed_event_ids=newly,
            deferred=deferred,
        )
        if selected is not None:
            store.save_notification_state(
                pending=selected,
                acknowledged_event_ids=list(acknowledged),
                deferred=deferred,
            )
            _append_event_attention(store, kind="HARNESS_EVENT_ACTIONABLE", event=selected, timestamp=observed)
            _append_event_attention(store, kind="HARNESS_EVENT_PENDING", event=selected, metadata={"pending_work_snapshot":_pending_work_snapshot(selected=selected,deferred=deferred,snapshot=snapshot,observed=observed,selection_reason="SELECT_ACTIONABLE")}, timestamp=observed)
            _print_events([selected], stream=stream)
            emitted_pending_ids.add(selected["event_id"])
        sleeper(config.poll_interval_seconds)


def ack_command(
    config: HarnessConfig,
    *,
    event_id: str,
    clock: Callable[[], datetime] = utc_now,
    stream: Any = sys.stdout,
    store_factory: Callable[[HarnessConfig], SafeOutput] = _store,
) -> int:
    store = store_factory(config)
    notification_state = store.load_notification_state()
    pending = notification_state.get("pending")
    is_exact_management_ack = (
        isinstance(pending, dict)
        and pending.get("event_id") == event_id
        and pending.get("type")
        in {"MANAGER_REVIEW_DUE", "LANE_NO_PROGRESS", "LANE_STAGE_REPEAT"}
    )
    try:
        from harness_watcher_implementation import settings as watcher_settings
    except ImportError:
        watcher_settings = None
    acknowledged_at = clock()
    emit_attention = getattr(store, "append_attention", None)
    attention_event: Mapping[str, Any] = pending if isinstance(pending, Mapping) else {"event_id": event_id}
    if callable(emit_attention): _append_event_attention(store, kind="HARNESS_ACK_ATTEMPTED", event=attention_event, timestamp=acknowledged_at)
    if not store.acknowledge_notification(event_id, heartbeat_at=acknowledged_at):
        raise ValueError("event ID does not match a pending or acknowledged notification")
    if callable(emit_attention): _append_event_attention(store, kind="HARNESS_ACK_SUCCEEDED", event=attention_event, timestamp=acknowledged_at)
    watcher_acknowledged = acknowledge_watcher_event(event_id) if watcher_settings is not None and watcher_settings.harness_watcher_active else False
    if is_exact_management_ack and isinstance(pending, dict):
        history = acknowledge_management_event(
            store.load_active_management_history(),
            pending,
            acknowledged_at=acknowledged_at,
        )
        store.save_active_management_history(history)
        if callable(emit_attention): emit_attention(kind="FORMAL_REVIEW_BASELINE_ADVANCED", event_id=event_id, metadata={"pending_work_snapshot":_unknown_pending_snapshot()}, timestamp=acknowledged_at)
    result = {"event_id": event_id, "acknowledged": True}
    if watcher_settings is not None and watcher_settings.harness_watcher_active:
        result["watcher_received"] = watcher_acknowledged
    _print_json(result, stream=stream)
    return EXIT_OK


def heartbeat_command(
    config: HarnessConfig,
    *,
    clock: Callable[[], datetime] = utc_now,
    stream: Any = sys.stdout,
    store_factory: Callable[[HarnessConfig], SafeOutput] = _store,
) -> int:
    store = store_factory(config)
    if not store.renew_manager_heartbeat(renewed_at=clock()):
        raise ValueError("no non-expired managed watcher heartbeat can be renewed")
    _print_json({"heartbeat_renewed": True}, stream=stream)
    return EXIT_OK


def stop_command(
    config: HarnessConfig,
    *,
    clock: Callable[[], datetime] = utc_now,
    stream: Any = sys.stdout,
    store_factory: Callable[[HarnessConfig], SafeOutput] = _store,
) -> int:
    store = store_factory(config)
    if not store.request_managed_watcher_stop(requested_at=clock()):
        raise ValueError("no managed watcher runtime is present")
    _print_json({"stop_requested": True}, stream=stream)
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m orchestrator_harness",
        description="Durable event observer for parallel coding lanes",
    )
    parser.add_argument(
        "--config",
        default=str(Path(__file__).resolve().parent / "config.example.json"),
        help="JSON configuration path",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    scan = subparsers.add_parser("scan", help="print one reconciled snapshot")
    scan.add_argument(
        "--no-write",
        action="store_true",
        help="accepted for symmetry; scan never writes",
    )
    watch = subparsers.add_parser(
        "watch", help="run a bounded diagnostic event watch"
    )
    mode = watch.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="perform one event diff")
    mode.add_argument(
        "--until-event", action="store_true", help="wait until a material event"
    )
    mode.add_argument(
        "--until-actionable", action="store_true", help="wait until manager review is needed"
    )
    watch.add_argument("--timeout", type=float, default=None)
    watch.add_argument("--no-write", action="store_true")
    watch.add_argument("--manager-session-id")
    watch.add_argument("--manager-invocation-id")
    ack = subparsers.add_parser("ack", help="acknowledge one pending manager notification")
    ack.add_argument("--event-id", required=True)
    preflight = subparsers.add_parser(
        "handoff-preflight",
        help="read-only structural admission check for one completed coding handoff",
    )
    preflight.add_argument("--task-card", required=True, type=Path)
    preflight.add_argument("--invocation", required=True, type=Path)
    preflight.add_argument("--result", required=True, type=Path)
    preflight.add_argument("--dependency-map", required=True, type=Path)
    preflight.add_argument("--worktree", required=True, type=Path)
    preflight.add_argument("--evidence-root", required=True, type=Path)
    preflight.add_argument(
        "--required-evidence",
        action="append",
        default=[],
        type=Path,
        help="required file below evidence-root; may be supplied more than once",
    )
    adapter = subparsers.add_parser(
        "adapter", help="install or inspect a project-local host adapter"
    )
    adapter_modes = adapter.add_subparsers(dest="adapter_action", required=True)
    for action in ("install", "check", "upgrade", "uninstall", "self-test"):
        command = adapter_modes.add_parser(action)
        command.add_argument("--host", default="codex")
        command.add_argument("--project-root", required=True, type=Path)
        if action == "self-test":
            command.add_argument("--queue-root", type=Path)
    hook = adapter_modes.add_parser("hook")
    hook.add_argument("--host", default="codex")
    hook.add_argument("--boundary", choices=("post_tool_use", "stop"), required=True)

    def add_view_commands(parent: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
        view = parent.add_parser("view", help="allocate an exact-revision immutable source view")
        view_modes = view.add_subparsers(dest="view_action", required=True)
        allocate = view_modes.add_parser("allocate")
        allocate.add_argument("--source-root", required=True, type=Path)
        allocate.add_argument("--revision", required=True)
        allocate.add_argument("--view-root", required=True, type=Path)
        allocate.add_argument("--result-root", required=True, type=Path)
        allocate.add_argument("--cache-root", required=True, type=Path)
        allocate.add_argument("--view-id", default="immutable-view")

    add_view_commands(subparsers)
    source = subparsers.add_parser("source", help="source-view lifecycle aliases")
    source_modes = source.add_subparsers(dest="source_action", required=True)
    source_allocate = source_modes.add_parser("allocate")
    source_allocate.add_argument("--source-root", required=True, type=Path)
    source_allocate.add_argument("--revision", required=True)
    source_allocate.add_argument("--view-root", required=True, type=Path)
    source_allocate.add_argument("--result-root", required=True, type=Path)
    source_allocate.add_argument("--cache-root", required=True, type=Path)
    source_allocate.add_argument("--view-id", default="immutable-view")

    lane = subparsers.add_parser("lane", help="terminal lane lifecycle operations")
    lane_modes = lane.add_subparsers(dest="lane_action", required=True)
    retire = lane_modes.add_parser("retire")
    retire.add_argument("--lane-root", required=True, type=Path)
    retire.add_argument("--archive-root", required=True, type=Path)
    retire.add_argument("--lane-id", required=True)
    retire.add_argument("--retained-revision", required=True)
    retire.add_argument("--task-ref", type=Path)
    retire.add_argument("--result-ref", type=Path)
    retire.add_argument("--findings-ref", type=Path)
    retire.add_argument("--acceptance-ref", type=Path)
    retire.add_argument("--transcript-ref", type=Path)
    retire.add_argument("--dependency-ref", type=Path)
    retire.add_argument("--unmerged-work-proved", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "adapter":
            if args.host != "codex":
                raise CodexAdapterError("only the implemented codex host supports installation")
            if args.adapter_action == "install":
                _print_json(install_codex_adapter(args.project_root))
                return EXIT_OK
            if args.adapter_action == "check":
                _print_json(check_codex_adapter(args.project_root))
                return EXIT_OK
            if args.adapter_action == "upgrade":
                _print_json(upgrade_codex_adapter(args.project_root))
                return EXIT_OK
            if args.adapter_action == "uninstall":
                _print_json(uninstall_codex_adapter(args.project_root))
                return EXIT_OK
            if args.adapter_action == "self-test":
                _print_json(synthetic_wake_self_test(args.project_root, queue_root=args.queue_root))
                return EXIT_OK
            _print_json(run_codex_hook(args.boundary))
            return EXIT_OK
        if args.command in {"view", "source"}:
            action = args.view_action if args.command == "view" else args.source_action
            if action != "allocate":
                raise ValueError("source lifecycle requires allocate")
            result = allocate_immutable_source_view(
                args.source_root,
                revision=args.revision,
                view_root=args.view_root,
                result_root=args.result_root,
                cache_root=args.cache_root,
                view_id=args.view_id,
            )
            _print_json(result.as_record())
            return EXIT_OK
        if args.command == "lane":
            if args.lane_action != "retire":
                raise ValueError("lane lifecycle requires retire")
            result = retire_terminal_lane(
                args.lane_root,
                args.archive_root,
                lane_id=args.lane_id,
                retained_revision=args.retained_revision,
                task_ref=args.task_ref,
                result_ref=args.result_ref,
                findings_ref=args.findings_ref,
                acceptance_ref=args.acceptance_ref,
                transcript_ref=args.transcript_ref,
                dependency_ref=args.dependency_ref,
                process_snapshot=process_snapshot(),
                live_processes=(),
                unmerged_work_proof=args.unmerged_work_proved,
            )
            _print_json(result.as_record())
            return EXIT_OK if result.outcome == "CLOSED" else EXIT_ERROR
        if args.command == "handoff-preflight":
            result = preflight_handoff(
                task_card_path=args.task_card,
                invocation_path=args.invocation,
                result_path=args.result,
                dependency_map_path=args.dependency_map,
                worktree=args.worktree,
                evidence_root=args.evidence_root,
                required_evidence=args.required_evidence,
            )
            _print_json(result)
            return handoff_preflight_exit_code(result)
        config = load_config(args.config)
        if args.command == "scan":
            return scan_command(config)
        if args.command == "ack":
            return ack_command(config, event_id=args.event_id)
        if not (args.once or args.until_event or args.until_actionable):
            raise ValueError("watch requires one mode: --once, --until-event, or --until-actionable")
        if args.once:
            return watch_once(config, no_write=args.no_write)[0]
        if args.until_actionable:
            if args.no_write:
                raise ValueError("--until-actionable requires writable harness output")
            return watch_until_actionable(config, timeout_seconds=args.timeout, manager_session_id=args.manager_session_id, manager_invocation_id=args.manager_invocation_id)
        return watch_until_event(
            config,
            no_write=args.no_write,
            timeout_seconds=args.timeout,
        )
    except (ConfigError, PathSafetyError, OSError, ValueError) as exc:
        sys.stderr.write(f"orchestrator_harness: {exc}\n")
        return EXIT_ERROR

