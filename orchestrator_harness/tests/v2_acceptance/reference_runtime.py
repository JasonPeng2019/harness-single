"""Tiny contract model used only to make synthetic oracle scenarios executable.

It is intentionally not an adapter around the candidate product.  It provides no
provider, hook, or real-process simulation, and therefore cannot produce live proof.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .contract import content_hash


@dataclass
class ReferenceRuntime:
    mode: str = "managed"
    epoch_id: str = "epoch-1"
    queue_id: str = "queue-1"
    lanes: dict[str, dict[str, Any]] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    leases: dict[str, dict[str, str]] = field(default_factory=dict)

    def bootstrap(self, lane_id: str, run_id: str, resources: tuple[str, ...] = ()) -> None:
        if lane_id in self.lanes:
            raise ValueError("BOOTSTRAP_LANE_ID_IN_USE")
        self.lanes[lane_id] = {"run_id": run_id, "lifecycle": "prepared", "resources": resources, "last": None}

    def launch(self, lane_id: str, holder: str) -> None:
        lane = self.lanes[lane_id]
        wanted = lane["resources"]
        if any(resource in self.leases for resource in wanted):
            raise ValueError("LAUNCH_LEASE_BUSY")
        for resource in wanted:
            self.leases[resource] = {"lane_id": lane_id, "run_id": lane["run_id"], "holder": holder}
        lane["lifecycle"] = "running"

    def terminal(self, lane_id: str, status: str) -> None:
        lane = self.lanes[lane_id]
        if status not in {"review_pending", "result_invalid"}:
            raise ValueError("invalid controller status")
        lane["lifecycle"] = status
        if self.mode == "managed" and lane["last"] != status:
            event_type = "COMPLETION_REVIEW_REQUIRED" if status == "review_pending" else "LANE_RESULT_INVALID"
            self.events.append({"event_id": f"event-{len(self.events) + 1}", "type": event_type, "lane_id": lane_id, "run_id": lane["run_id"], "state": "PENDING", "history": ["PENDING"]})
            lane["last"] = status

    def acknowledge(self, event_id: str) -> None:
        event = next(event for event in self.events if event["event_id"] == event_id)
        if event["state"] != "PENDING":
            raise ValueError("MANAGER_ACK_ALREADY_ACKNOWLEDGED")
        event["state"] = "ACKNOWLEDGED"
        event["history"].append("ACKNOWLEDGED")

    def cleanup(self, lane_id: str, holder: str) -> None:
        for resource, lease in tuple(self.leases.items()):
            if lease["lane_id"] == lane_id:
                if lease["holder"] != holder:
                    raise ValueError("cleanup identity does not own lease")
                del self.leases[resource]

    def resume(self, lane_id: str, new_run_id: str) -> None:
        lane = self.lanes[lane_id]
        if lane["lifecycle"] == "accepted":
            raise ValueError("ALREADY_ACCEPTED")
        if lane["lifecycle"] == "running":
            raise ValueError("LANE_RUNNING")
        lane.update(run_id=new_run_id, lifecycle="running", last=None)

    def resume_ordered(self, lane_id: str, new_run_id: str) -> list[str]:
        """Reference the BOUND-009 observable ordering, not candidate internals."""
        lane = self.lanes[lane_id]
        if lane["lifecycle"] in {"accepted", "running"}:
            raise ValueError("resume is limited to stopped, unaccepted lanes")
        trace = ["resuming-persisted", "task-rationale-instructions-replaced", "obsolete-current-run-artifacts-cleared", "fresh-invocation-validated"]
        lane.update(run_id=new_run_id, lifecycle="running", last=None)
        trace.append("running-persisted")
        return trace

    def review_pair(self, lane_id: str, outcome: str, approval: str, force_reason: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
        lane = self.lanes[lane_id]
        if approval == "ACCEPTED" and outcome != "PASS" and not force_reason:
            raise ValueError("COMPLETION_REVIEW_FORCE_REASON_INVALID")
        review = {"schema": "completion-review/v1", "lane_id": lane_id, "run_id": lane["run_id"], "review_outcome": outcome, "task_card_hash": "task", "result_hash": "result", "commit": "commit"}
        acceptance = {"schema": "orchestrator-acceptance/v1", "lane_id": lane_id, "run_id": lane["run_id"], "approval": approval, "task_card_hash": "task", "result_hash": "result", "commit": "commit", "review_ref": content_hash(review)}
        if force_reason:
            acceptance["force_accept_reason"] = force_reason
        if approval == "ACCEPTED":
            lane["lifecycle"] = "accepted"
        return review, acceptance
