"""Independent, candidate-observable Addendum 3 acceptance oracles.

The functions consume records a candidate exposes (queue snapshots, audit JSONL,
hook notices, attempt ledgers, or observed process state).  They intentionally do
not import candidate internals or reproduce an implementation algorithm.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def _events(items: Sequence[Mapping[str, Any]], kind: str) -> list[Mapping[str, Any]]:
    return [item for item in items if item.get("kind") == kind]


def _one(items: Sequence[Mapping[str, Any]], kind: str) -> Mapping[str, Any]:
    matches = _events(items, kind)
    if len(matches) != 1:
        raise AssertionError(f"expected one {kind!r} observation, found {len(matches)}")
    return matches[0]


def assert_outbox_retry(observations: Sequence[Mapping[str, Any]]) -> None:
    """REQ-001: failed promotion retains one source, then admits one event."""
    failed = _one(observations, "promotion-failed")
    retained = _one(observations, "outbox-retained")
    promoted = _one(observations, "queue-event-admitted")
    archived = _one(observations, "outbox-archived")
    if not failed.get("diagnostic") or retained.get("notice_id") != failed.get("notice_id"):
        raise AssertionError("failed promotion must retain and diagnose its exact notice")
    if promoted.get("notice_id") != failed.get("notice_id") or archived.get("notice_id") != failed.get("notice_id"):
        raise AssertionError("retry/archive must correlate with the retained notice")
    if promoted.get("event_count") != 1 or archived.get("before_admission"):
        raise AssertionError("one durable admission must precede archive")


def assert_queue_watch(observations: Sequence[Mapping[str, Any]]) -> None:
    """REQ-002: managed queue-only wake is deduplicated and non-mutating."""
    wake = _one(observations, "watch-wake")
    if wake.get("profile") != "managed" or wake.get("source") != "manager-event":
        raise AssertionError("managed queue event must identify its wake source")
    if not wake.get("event_id") or wake.get("event_state_before") != wake.get("event_state_after"):
        raise AssertionError("watch must report top-level identity without advancing event state")
    if _events(observations, "duplicate-wake"):
        raise AssertionError("the same unresolved event woke a bound ROOT session twice")
    plain = _one(observations, "plain-watch")
    if plain.get("queue_read") or plain.get("queue_created"):
        raise AssertionError("plain watch must remain queue-free")


def assert_review_recovery(observations: Sequence[Mapping[str, Any]]) -> None:
    """REQ-003: broken pairs recover pending review without inferred acceptance."""
    recovery = _one(observations, "review-recovery")
    if recovery.get("accepted") or recovery.get("pair_preserved"):
        raise AssertionError("incomplete review/acceptance pair cannot imply acceptance")
    if recovery.get("replacement_open_reviews") != 1 or recovery.get("lifecycle") != "review_pending":
        raise AssertionError("valid result needs exactly one restored open review")


def assert_hook_delivery_and_notice(observations: Sequence[Mapping[str, Any]]) -> None:
    """REQ-004/005: hook delivery is receipt-only and notice is structured/content-free."""
    receipt = _one(observations, "hook-receipt")
    notice = _one(observations, "root-notice")
    if receipt.get("state_before") != "PENDING" or receipt.get("state_after") != "PENDING":
        raise AssertionError("DELIVERED receipt must not acknowledge or close the event")
    required = {"pending_count", "highest_class", "highest_severity", "binding_id", "queue_id", "timestamp"}
    if required - set(notice) or notice.get("payload_included"):
        raise AssertionError("ROOT notice must be structured and content-free")


def assert_heartbeat_and_recovery_authority(observations: Sequence[Mapping[str, Any]]) -> None:
    """REQ-006/007: heartbeat diagnoses failures and hooks only direct ROOT."""
    heartbeat = _one(observations, "heartbeat")
    unhealthy = _one(observations, "monitor-unhealthy-notice")
    if heartbeat.get("watched_lane_count") is None or not heartbeat.get("timestamp"):
        raise AssertionError("heartbeat must atomically expose a count, including zero")
    if not heartbeat.get("diagnostics_preserved"):
        raise AssertionError("inspection/promotion failures cannot disappear behind healthy evidence")
    if unhealthy.get("replacement_performed") or unhealthy.get("recovery_action") != "health monitor-recover":
        raise AssertionError("hook detects/directs; ROOT alone requests replacement")
    if not unhealthy.get("plain_refusal") or not unhealthy.get("deliberate_stop_preserved"):
        raise AssertionError("plain refusal and deliberate stop must remain visible")


def assert_bounded_result_correction(
    attempts: Sequence[Mapping[str, Any]], *, shipped_adapters: Sequence[str]
) -> None:
    """REQ-008/009: five native continuations, sixth escalation, no fallback."""
    if len(attempts) != 6:
        raise AssertionError("scenario must preserve all six invalid-result exits")
    for index, attempt in enumerate(attempts, start=1):
        if not attempt.get("cleanup_proven") or not attempt.get("lease_held"):
            raise AssertionError(f"attempt {index} lacks cleanup proof or held lease")
        if index <= 5:
            if attempt.get("terminal") or attempt.get("resume") is not True or not attempt.get("session_id"):
                raise AssertionError("first five invalid results require same-session native continuation")
            if attempt.get("provider") not in shipped_adapters or attempt.get("fallback_provider"):
                raise AssertionError("continuation cannot switch provider/model")
            argv = attempt.get("adapter_argv")
            if not isinstance(argv, Sequence) or isinstance(argv, (str, bytes)) or attempt["session_id"] not in argv:
                raise AssertionError("candidate must retain the saved session in adapter-built native argv")
            required_prompt = {"lane_id", "run_id", "summary", "outcome", "evidence", "completed_at", "content_hash"}
            if required_prompt - set(attempt.get("correction_fields", ())):
                raise AssertionError("correction prompt omits required result/v1 fields")
        elif attempt.get("terminal") != "provider_exited_no_result" or attempt.get("resume"):
            raise AssertionError("sixth invalid exit must escalate exactly once without a sixth prompt")


def assert_orphan_discovery_and_audit(observations: Sequence[Mapping[str, Any]]) -> None:
    """REQ-010/011: global per-lease discovery plus two-phase force-release audit."""
    discovery = _one(observations, "orphan-discovery")
    audit = _events(observations, "release-audit")
    if discovery.get("scan_count") != 1 or not discovery.get("resource_ids"):
        raise AssertionError("one monitor pass must scan lease files once and name each resource")
    if discovery.get("live_holder_reported") or discovery.get("deduplicated_per_lease") is not True:
        raise AssertionError("exact live holder is excluded; distinct leases deduplicate independently")
    if len(audit) < 2 or audit[0].get("state") != "STARTED" or not audit[0].get("locked"):
        raise AssertionError("force release requires locked STARTED audit before deletion")
    terminal = audit[-1]
    if terminal.get("state") not in {"SUCCEEDED", "FAILED"} or not terminal.get("prior_holder_readable"):
        raise AssertionError("terminal audit must truthfully retain prior-holder evidence")
    if terminal.get("state") == "SUCCEEDED" and not terminal.get("absence_readback"):
        raise AssertionError("successful release needs absence readback")
