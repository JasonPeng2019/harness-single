from __future__ import annotations

import unittest

from orchestrator_harness.cli import observe
from orchestrator_harness.events import conditions_from_snapshot, diff_conditions
from orchestrator_harness.models import ProcessSnapshot, iso_utc
from orchestrator_harness.notifications import select_actionable
from orchestrator_harness.tests.support import (
    NOW,
    SuiteFixture,
    write_json,
)

CODING_SCHEMA = "orchestrator-coding-invocation/v1"


def coding_lane(
    *,
    lane_id: str = "coding:one",
    state: str = "EXITED",
    declared_state: str = "codex_exited",
    **changes: object,
) -> dict[str, object]:
    return {
        "lane_id": lane_id,
        "process_state": state,
        "operational_state": state,
        "declared_state": declared_state,
        "invocation_schema": CODING_SCHEMA,
        "worker_invocation_id": "worker-1",
        "repository": {
            "common_dir": "C:/repo/.git",
            "worktree_root": "C:/repo",
            "branch": "coding",
        },
        **changes,
    }


def snapshot(*lanes: dict[str, object], **changes: object) -> dict[str, object]:
    return {
        "process_snapshot_complete": True,
        "lanes": list(lanes),
        "requests": [],
        "helpers": [],
        "mcps": [],
        "manager_signals": [],
        "resource_conflicts": [],
        "coding_conflicts": [],
        "observation_errors": [],
        **changes,
    }


class CodingCoordinationEventTests(unittest.TestCase):
    def test_coding_snapshot_fields_and_repeated_observation_are_stable(self) -> None:
        fixture = SuiteFixture.create()
        self.addCleanup(fixture.close)
        status = fixture.status(state="exited", declared_lane_id="coding:one")
        value = {
            "state": "coordination_failed",
            "controller_pid": 101,
            "codex_pid": 102,
            "controller_started_utc": iso_utc(NOW),
            "codex_started_utc": iso_utc(NOW),
            "declared_lane_id": "coding:one",
            "invocation_schema": CODING_SCHEMA,
            "worker_invocation_id": "worker-1",
            "repository": {
                "common_dir": "C:/repo/.git",
                "worktree_root": "C:/repo",
                "branch": "coding",
            },
            "result_validation": {"code": "CODING_RESULT_INVALID"},
            "result_valid": False,
            "coordination_failure": {"error": "malformed resource claim"},
            "waiting_resource_claim": {
                "resource": "db",
                "wait_seconds": 1,
                "actionable": False,
            },
            "resource_claim_findings": [{"resource": "db", "code": "MALFORMED_CLAIM"}],
        }
        write_json(status, value)
        absent = ProcessSnapshot(True, (), (), "fake")
        first, _ = observe(
            fixture.config, process_provider=lambda: absent, clock=lambda: NOW
        )
        second, _ = observe(
            fixture.config, process_provider=lambda: absent, clock=lambda: NOW
        )
        lane = first["lanes"][0]
        self.assertEqual(CODING_SCHEMA, lane["invocation_schema"])
        self.assertEqual("worker-1", lane["worker_invocation_id"])
        self.assertFalse(lane["result_valid"])
        self.assertEqual(
            {"error": "malformed resource claim"}, lane["coordination_failure"]
        )
        self.assertEqual(first, second)
        self.assertEqual(
            [],
            diff_conditions(
                conditions_from_snapshot(first),
                conditions_from_snapshot(second),
                observed_at=NOW,
            ),
        )

    def test_duplicate_branch_and_worktree_conditions_are_stable_and_priority_two(
        self,
    ) -> None:
        conflicts = [
            {
                "type": "DUPLICATE_CODING_WORKTREE",
                "identity": "c:/repo",
                "lanes": ["coding:a", "coding:b"],
            },
            {
                "type": "DUPLICATE_CODING_BRANCH",
                "identity": "c:/repo/.git::coding",
                "lanes": ["coding:a", "coding:b"],
            },
        ]
        current = snapshot(
            coding_lane(lane_id="coding:a"),
            coding_lane(lane_id="coding:b"),
            coding_conflicts=conflicts,
        )
        conditions = conditions_from_snapshot(current)
        events = diff_conditions(None, conditions, observed_at=NOW)
        self.assertEqual(events, diff_conditions(None, conditions, observed_at=NOW))
        selected = select_actionable(
            conditions, current, observed_at=NOW, acknowledged_event_ids=set()
        )
        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual("DUPLICATE_CODING_BRANCH", selected["type"])
        self.assertEqual(2, selected["admitted_priority"])

    def test_invalid_result_clears_when_corrected(self) -> None:
        invalid = snapshot(
            coding_lane(
                invalid_result={
                    "code": "CODING_RESULT_INVALID",
                    "detail": "worker mismatch",
                }
            )
        )
        corrected = snapshot(
            coding_lane(result_valid=True, result_validation={"commit": "a" * 40})
        )
        invalid_conditions = conditions_from_snapshot(invalid)
        invalid_event = invalid_conditions["lane:coding:one:invalid-result"]
        selected = select_actionable(
            invalid_conditions, invalid, observed_at=NOW, acknowledged_event_ids=set()
        )
        self.assertEqual(
            "CODING_RESULT_INVALID", selected["type"] if selected else None
        )
        corrected_conditions = conditions_from_snapshot(corrected)
        self.assertNotIn("lane:coding:one:invalid-result", corrected_conditions)
        self.assertEqual(
            "CONDITION_CLEARED",
            next(
                item
                for item in diff_conditions(
                    invalid_conditions, corrected_conditions, observed_at=NOW
                )
                if item["identity"] == invalid_event["identity"]
            )["type"],
        )

    def test_short_wait_is_passive_but_excess_wait_is_actionable(self) -> None:
        short = snapshot(
            coding_lane(
                state="WAITING_RESOURCE",
                waiting_resource_claim={
                    "resource": "db",
                    "wait_seconds": 1,
                    "remaining_seconds": 59,
                    "actionable": False,
                },
            )
        )
        excessive = snapshot(
            coding_lane(
                state="WAITING_RESOURCE",
                waiting_resource_claim={
                    "resource": "db",
                    "wait_seconds": 60,
                    "remaining_seconds": 0,
                    "actionable": True,
                },
            )
        )
        short_condition = conditions_from_snapshot(short)[
            "lane:coding:one:resource-wait"
        ]
        excessive_condition = conditions_from_snapshot(excessive)[
            "lane:coding:one:resource-wait"
        ]
        self.assertEqual(
            short_condition["event_id"],
            conditions_from_snapshot(short)[short_condition["identity"]]["event_id"],
        )
        self.assertIsNone(
            select_actionable(
                {short_condition["identity"]: short_condition},
                short,
                observed_at=NOW,
                acknowledged_event_ids=set(),
            )
        )
        selected = select_actionable(
            {excessive_condition["identity"]: excessive_condition},
            excessive,
            observed_at=NOW,
            acknowledged_event_ids=set(),
        )
        self.assertEqual("RESOURCE_WAIT", selected["type"] if selected else None)

    def test_malformed_stale_and_unknown_claims_are_durable_priority_two_events(
        self,
    ) -> None:
        lane = coding_lane(
            resource_claim_findings=[
                {"resource": "malformed", "code": "MALFORMED_CLAIM"},
                {"resource": "stale", "code": "STALE_CLAIM"},
                {"resource": "unknown", "code": "UNKNOWN_CLAIM"},
            ]
        )
        current = snapshot(lane)
        conditions = conditions_from_snapshot(current)
        claims = [
            condition
            for identity, condition in conditions.items()
            if ":resource-claim:" in identity
        ]
        self.assertEqual(3, len(claims))
        for condition in claims:
            with self.subTest(resource=condition["data"]["resource"]):
                selected = select_actionable(
                    {condition["identity"]: condition},
                    current,
                    observed_at=NOW,
                    acknowledged_event_ids=set(),
                )
                self.assertEqual(
                    "RESOURCE_CLAIM_STALE", selected["type"] if selected else None
                )
                self.assertEqual(2, selected["admitted_priority"] if selected else None)

    def test_post_exit_coordination_failure_is_durable_and_requires_exact_ack(
        self,
    ) -> None:
        current = snapshot(
            coding_lane(
                declared_state="coordination_failed",
                coordination_failure={"error": "claim release failed"},
            )
        )
        conditions = conditions_from_snapshot(current)
        failure = conditions["lane:coding:one:coordination-failure"]
        selected = select_actionable(
            conditions, current, observed_at=NOW, acknowledged_event_ids=set()
        )
        self.assertEqual("COORDINATION_FAILED", selected["type"] if selected else None)
        self.assertEqual(2, selected["admitted_priority"] if selected else None)
        self.assertEqual(
            failure["event_id"],
            conditions_from_snapshot(current)[failure["identity"]]["event_id"],
        )
        self.assertIsNotNone(
            select_actionable(
                conditions, current, observed_at=NOW, acknowledged_event_ids={"wrong"}
            )
        )
        self.assertIsNone(
            select_actionable(
                {failure["identity"]: failure},
                current,
                observed_at=NOW,
                acknowledged_event_ids={failure["event_id"]},
            )
        )

    def test_application_failure_is_not_a_harness_coordination_failure(self) -> None:
        application = snapshot(
            coding_lane(declared_state="controller_failed", coordination_failure=None)
        )
        harness = snapshot(
            coding_lane(
                declared_state="coordination_failed",
                coordination_failure={"error": "claim malformed"},
            )
        )
        application_conditions = conditions_from_snapshot(application)
        harness_conditions = conditions_from_snapshot(harness)
        self.assertNotIn("lane:coding:one:coordination-failure", application_conditions)
        self.assertIn("lane:coding:one:coordination-failure", harness_conditions)
        selected = select_actionable(
            harness_conditions,
            harness,
            observed_at=NOW,
            acknowledged_event_ids=set(),
        )
        assert selected is not None
        self.assertEqual("COORDINATION_FAILED", selected["type"])

if __name__ == "__main__":
    unittest.main()
