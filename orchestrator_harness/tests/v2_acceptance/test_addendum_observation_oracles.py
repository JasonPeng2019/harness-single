from __future__ import annotations

import unittest

from orchestrator_harness.tests.v2_acceptance import candidate_observations as oracle


class AddendumObservationOracleTests(unittest.TestCase):
    """Failure-injection examples for candidate-facing, implementation-independent oracles."""

    def test_outbox_retry_rejects_archive_before_admission(self) -> None:
        good = [
            {"kind": "promotion-failed", "notice_id": "n", "diagnostic": "queue unavailable"},
            {"kind": "outbox-retained", "notice_id": "n"},
            {"kind": "queue-event-admitted", "notice_id": "n", "event_count": 1},
            {"kind": "outbox-archived", "notice_id": "n", "before_admission": False},
        ]
        oracle.assert_outbox_retry(good)
        bad = [*good[:-1], {"kind": "outbox-archived", "notice_id": "n", "before_admission": True}]
        with self.assertRaises(AssertionError):
            oracle.assert_outbox_retry(bad)

    def test_watch_and_review_reject_state_mutation_or_inferred_acceptance(self) -> None:
        watch = [
            {"kind": "watch-wake", "profile": "managed", "source": "manager-event", "event_id": "e", "event_state_before": "PENDING", "event_state_after": "PENDING"},
            {"kind": "plain-watch", "queue_read": False, "queue_created": False},
        ]
        oracle.assert_queue_watch(watch)
        with self.assertRaises(AssertionError):
            oracle.assert_queue_watch([{**watch[0], "event_state_after": "ACKNOWLEDGED"}, watch[1]])
        review = [{"kind": "review-recovery", "accepted": False, "pair_preserved": False, "replacement_open_reviews": 1, "lifecycle": "review_pending"}]
        oracle.assert_review_recovery(review)
        with self.assertRaises(AssertionError):
            oracle.assert_review_recovery([{**review[0], "replacement_open_reviews": 2}])

    def test_hook_heartbeat_and_recovery_authority_reject_mutants(self) -> None:
        hook = [
            {"kind": "hook-receipt", "state_before": "PENDING", "state_after": "PENDING"},
            {"kind": "root-notice", "pending_count": 2, "highest_class": "manager", "highest_severity": "blocking", "binding_id": "b", "queue_id": "q", "timestamp": "t", "payload_included": False},
        ]
        oracle.assert_hook_delivery_and_notice(hook)
        with self.assertRaises(AssertionError):
            oracle.assert_hook_delivery_and_notice([{**hook[0], "state_after": "ACKNOWLEDGED"}, hook[1]])
        health = [
            {"kind": "heartbeat", "watched_lane_count": 0, "timestamp": "t", "diagnostics_preserved": True},
            {"kind": "monitor-unhealthy-notice", "replacement_performed": False, "recovery_action": "health monitor-recover", "plain_refusal": True, "deliberate_stop_preserved": True},
        ]
        oracle.assert_heartbeat_and_recovery_authority(health)
        with self.assertRaises(AssertionError):
            oracle.assert_heartbeat_and_recovery_authority([health[0], {**health[1], "replacement_performed": True}])

    def test_correction_and_lease_audit_reject_limit_and_live_holder_mutants(self) -> None:
        attempts = [
            {"provider": "codex", "session_id": "s", "adapter_argv": ["native-provider", "native-resume", "s"], "resume": True, "cleanup_proven": True, "lease_held": True, "fallback_provider": None, "correction_fields": ["lane_id", "run_id", "summary", "outcome", "evidence", "completed_at", "content_hash"]}
            for _ in range(5)
        ] + [{"provider": "codex", "resume": False, "cleanup_proven": True, "lease_held": True, "terminal": "provider_exited_no_result"}]
        for provider in ("codex", "claude-code", "qwen-code"):
            with self.subTest(provider=provider):
                provider_attempts = [{**attempt, "provider": provider} for attempt in attempts]
                oracle.assert_bounded_result_correction(provider_attempts, shipped_adapters=("codex", "claude-code", "qwen-code"))
        with self.assertRaises(AssertionError):
            oracle.assert_bounded_result_correction([*attempts[:5], {**attempts[-1], "resume": True}], shipped_adapters=("codex",))
        audit = [
            {"kind": "orphan-discovery", "scan_count": 1, "resource_ids": ["a", "b"], "live_holder_reported": False, "deduplicated_per_lease": True},
            {"kind": "release-audit", "state": "STARTED", "locked": True},
            {"kind": "release-audit", "state": "SUCCEEDED", "prior_holder_readable": True, "absence_readback": True},
        ]
        oracle.assert_orphan_discovery_and_audit(audit)
        with self.assertRaises(AssertionError):
            oracle.assert_orphan_discovery_and_audit([{**audit[0], "live_holder_reported": True}, *audit[1:]])
