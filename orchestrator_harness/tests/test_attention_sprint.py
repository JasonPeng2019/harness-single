from __future__ import annotations

import unittest

from orchestrator_harness.attention_sprint import AttentionSprintError, event_selection_snapshot, formal_baseline_snapshot, invocation_snapshot, validate_sprint_boundary, validate_sprint_finalize


EVENT={"event_id":"gate","type":"HELP","priority":1,"age_seconds":2.0,"agent_blocked":True}


class AttentionSprintTests(unittest.TestCase):
    def test_boundary_rejects_heartbeat_expiry_risk_without_emitting_activity(self) -> None:
        with self.assertRaisesRegex(AttentionSprintError,"bounded sprint lifetime"):
            validate_sprint_boundary(epoch_id="r12",heartbeat_timeout_seconds=10,formal_review_interval_seconds=5,bounded_lifetime_seconds=10)
        boundary=validate_sprint_boundary(epoch_id="r12",heartbeat_timeout_seconds=11,formal_review_interval_seconds=5,bounded_lifetime_seconds=10)
        self.assertEqual({"epoch_id","heartbeat_timeout_seconds","formal_review_interval_seconds","bounded_lifetime_seconds"},set(boundary))
        self.assertNotIn("kind",boundary)

    def test_complete_boundaries_and_terminal_gate_contract(self) -> None:
        epoch="r12"; inventory=invocation_snapshot([EVENT]); baseline=formal_baseline_snapshot([EVENT]); selection=event_selection_snapshot([EVENT],event_id="gate")
        common=[
            {"epoch_id":epoch,"event_id":"start","kind":"MANAGER_INVOCATION_STARTED","pending_work_snapshot":inventory},
            {"epoch_id":epoch,"event_id":"baseline","kind":"FORMAL_REVIEW_BASELINE_ADVANCED","source_role":"orchestrator","pending_work_snapshot":baseline},
            {"epoch_id":epoch,"event_id":"gate","kind":"MANAGER_EVENT_CLAIMED","pending_work_snapshot":selection},
            {"epoch_id":epoch,"event_id":"gate","kind":"AGENT_SIGNAL_CREATED","agent_blocked":True},
            {"epoch_id":epoch,"event_id":"finish","kind":"MANAGER_INVOCATION_FINISHED","pending_work_snapshot":inventory},
        ]
        validate_sprint_finalize([*common,{"epoch_id":epoch,"event_id":"gate","kind":"AGENT_RESPONSE_RECEIVED"},{"epoch_id":epoch,"event_id":"gate","kind":"AGENT_WORK_RESUMED"}],epoch_id=epoch)
        validate_sprint_finalize([*common,{"epoch_id":epoch,"event_id":"gate","kind":"AGENT_GATE_EXPIRED","terminal_gate_expired":True}],epoch_id=epoch)
        with self.assertRaisesRegex(AttentionSprintError,"blocking gate"):
            validate_sprint_finalize(common,epoch_id=epoch)
        incomplete={"complete":False,"events":[],"selected_event_id":None,"selection_reason":"UNKNOWN"}
        with self.assertRaisesRegex(AttentionSprintError,"activation formal-review baseline"):
            validate_sprint_finalize([{**row,"pending_work_snapshot":incomplete} if row["kind"] == "FORMAL_REVIEW_BASELINE_ADVANCED" else row for row in common],epoch_id=epoch)
        duplicate=[*common,{"epoch_id":epoch,"event_id":"gate","kind":"MANAGER_EVENT_CLAIMED","pending_work_snapshot":inventory},{"epoch_id":epoch,"event_id":"gate","kind":"AGENT_GATE_EXPIRED","terminal_gate_expired":True}]
        with self.assertRaisesRegex(AttentionSprintError,"claim requires"):
            validate_sprint_finalize(duplicate,epoch_id=epoch)
        review=[*common,{"epoch_id":epoch,"event_id":"review","kind":"MANAGER_REVIEW_STARTED"},{"epoch_id":epoch,"event_id":"gate","kind":"AGENT_GATE_EXPIRED","terminal_gate_expired":True}]
        with self.assertRaisesRegex(AttentionSprintError,"formal review lacks"):
            validate_sprint_finalize(review,epoch_id=epoch)
        validate_sprint_finalize([*review[:-1],{"epoch_id":epoch,"event_id":"review-baseline","kind":"FORMAL_REVIEW_BASELINE_ADVANCED","source_role":"orchestrator","pending_work_snapshot":baseline},review[-1]],epoch_id=epoch)


if __name__ == "__main__":
    unittest.main()
