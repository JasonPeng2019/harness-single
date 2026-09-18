from __future__ import annotations

"""Producer-local regressions for recorded-boundary exact ownership.

Proof634 showed that ProcessBoundary.observe selected by numeric PID alone and
adopted a reused non-root PID plus its unrelated descendants into _owned.
These tests use the real ProcessBoundary methods with fake snapshot, identity,
and termination functions only; no real process is observed or terminated.
"""

import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from orchestrator_harness import processes as p
from orchestrator_harness.models import ProcessInfo, ProcessSnapshot

CREATED = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)


def recorded_boundary() -> dict[str, object]:
    """The proof634 recorded boundary: root 11001 and child 22002, both old."""
    return {
        "kind": "windows-job",
        "root": {"pid": 11001, "creation_time": "old-root"},
        "process_group_id": None,
        "session_id": None,
        "processes": [
            {"pid": 11001, "creation_time": "old-root"},
            {"pid": 22002, "creation_time": "old-child"},
        ],
    }


def snapshot(*items: ProcessInfo) -> ProcessSnapshot:
    return ProcessSnapshot(True, tuple(items), (), "fake")


def fake_identity(identities: dict[int, str]):
    def identity(pid: int) -> dict[str, object] | None:
        creation = identities.get(pid)
        if creation is None:
            return None
        return {"pid": pid, "creation_time": creation}

    return identity


class RecordedBoundaryIdentityTests(unittest.TestCase):
    def test_reused_child_pid_is_not_adopted_and_never_terminated(self) -> None:
        record = recorded_boundary()
        processes = snapshot(
            ProcessInfo(22002, 90000, "unrelated.exe", "unrelated user process", CREATED),
            ProcessInfo(33003, 22002, "unrelated-child.exe", "unrelated descendant", CREATED),
        )
        identities = {22002: "new-unrelated", 33003: "new-descendant"}
        terminate = MagicMock(return_value=True)
        with (
            patch.object(p, "process_identity", side_effect=fake_identity(identities)),
            patch.object(p, "process_alive", side_effect=lambda pid: pid in identities),
            patch.object(p, "process_snapshot", return_value=processes),
            patch.object(p, "terminate_process", terminate),
        ):
            boundary = p.ProcessBoundary.from_record(record, snapshot_provider=lambda: processes)
            self.assertTrue(boundary.observe())
            self.assertNotIn((22002, "new-unrelated"), boundary._owned)
            self.assertNotIn((33003, "new-descendant"), boundary._owned)
            self.assertEqual(
                {(11001, "old-root"), (22002, "old-child")}, set(boundary._owned)
            )
            self.assertEqual([], boundary._remaining())
            self.assertTrue(p.process_boundary_is_gone(record))
            self.assertTrue(boundary.cleanup())
            self.assertTrue(
                p.cleanup_recorded_process_boundary(record, force=False, timeout_seconds=1.0)
            )
        unrelated_terminations = [
            call
            for call in terminate.call_args_list
            if call.args[0] == 33003
            or call.args[1] in ("new-unrelated", "new-descendant")
        ]
        self.assertEqual([], unrelated_terminations)

    def test_exact_live_owned_child_still_anchors_its_legitimate_descendant(self) -> None:
        record = recorded_boundary()
        processes = snapshot(
            ProcessInfo(22002, 90000, "child.exe", "owned child", CREATED),
            ProcessInfo(33003, 22002, "descendant.exe", "legitimate descendant", CREATED),
        )
        identities = {22002: "old-child", 33003: "new-descendant"}
        with (
            patch.object(p, "process_identity", side_effect=fake_identity(identities)),
            patch.object(p, "process_alive", side_effect=lambda pid: pid in identities),
            patch.object(p, "process_snapshot", return_value=processes),
        ):
            boundary = p.ProcessBoundary.from_record(record, snapshot_provider=lambda: processes)
            self.assertTrue(boundary.observe())
            self.assertIn((33003, "new-descendant"), boundary._owned)
            remaining = boundary._remaining()
            self.assertIsNotNone(remaining)
            self.assertEqual(
                {(22002, "old-child"), (33003, "new-descendant")}, set(remaining)
            )
            self.assertFalse(p.process_boundary_is_gone(record))

    def test_unknown_live_identity_fails_closed_without_termination(self) -> None:
        record = recorded_boundary()
        processes = snapshot(
            ProcessInfo(11001, 1, "root.exe", "root", CREATED),
            ProcessInfo(22002, 11001, "child.exe", "child", CREATED),
        )
        terminate = MagicMock(return_value=True)
        with (
            patch.object(p, "process_identity", side_effect=fake_identity({11001: "old-root"})),
            patch.object(p, "process_alive", side_effect=lambda pid: pid in (11001, 22002)),
            patch.object(p, "process_snapshot", return_value=processes),
            patch.object(p, "terminate_process", terminate),
        ):
            boundary = p.ProcessBoundary.from_record(record, snapshot_provider=lambda: processes)
            self.assertFalse(boundary.observe())
            self.assertIsNone(boundary._remaining())
            self.assertFalse(p.process_boundary_is_gone(record))
            self.assertFalse(boundary.cleanup())
            self.assertFalse(
                p.cleanup_recorded_process_boundary(record, force=False, timeout_seconds=1.0)
            )
        self.assertEqual([], terminate.call_args_list)

    def test_reused_root_pid_fails_closed_and_is_never_adopted(self) -> None:
        record = recorded_boundary()
        processes = snapshot(
            ProcessInfo(11001, 1, "unrelated.exe", "unrelated root user", CREATED),
        )
        identities = {11001: "new-root-user"}
        terminate = MagicMock(return_value=True)
        with (
            patch.object(p, "process_identity", side_effect=fake_identity(identities)),
            patch.object(p, "process_alive", side_effect=lambda pid: pid == 11001),
            patch.object(p, "process_snapshot", return_value=processes),
            patch.object(p, "terminate_process", terminate),
        ):
            boundary = p.ProcessBoundary.from_record(record, snapshot_provider=lambda: processes)
            self.assertFalse(boundary.observe())
            self.assertNotIn((11001, "new-root-user"), boundary._owned)
            self.assertEqual(
                {(11001, "old-root"), (22002, "old-child")}, set(boundary._owned)
            )
            self.assertFalse(p.process_boundary_is_gone(record))
            self.assertFalse(boundary.cleanup())
            self.assertFalse(
                p.cleanup_recorded_process_boundary(record, force=False, timeout_seconds=1.0)
            )
        self.assertEqual([], terminate.call_args_list)

    def test_identity_drift_between_admission_and_traversal_fails_closed(self) -> None:
        record = recorded_boundary()
        processes = snapshot(
            ProcessInfo(22002, 90000, "child.exe", "owned child", CREATED),
            ProcessInfo(33003, 22002, "descendant.exe", "descendant", CREATED),
        )
        probe_counts: dict[int, int] = {}
        reads: list[int] = []

        def identity(pid: int) -> dict[str, object] | None:
            probe_counts[pid] = probe_counts.get(pid, 0) + 1
            reads.append(pid)
            if pid == 22002:
                # Sequential reads: old-child at admission, new-unrelated on any
                # later probe (the child exits and its PID is reused mid-observe).
                creation = "old-child" if probe_counts[pid] == 1 else "new-unrelated"
                return {"pid": pid, "creation_time": creation}
            if pid == 33003:
                return {"pid": pid, "creation_time": "new-descendant"}
            return None

        terminate = MagicMock(return_value=True)
        with (
            patch.object(p, "process_identity", side_effect=identity),
            patch.object(p, "process_alive", side_effect=lambda pid: pid in (22002, 33003)),
            patch.object(p, "process_snapshot", return_value=processes),
            patch.object(p, "terminate_process", terminate),
        ):
            boundary = p.ProcessBoundary.from_record(record, snapshot_provider=lambda: processes)
            self.assertFalse(boundary.observe())
            self.assertIn((22002, "old-child"), boundary._owned)
            self.assertNotIn((22002, "new-unrelated"), boundary._owned)
            self.assertNotIn((33003, "new-descendant"), boundary._owned)
            self.assertEqual(
                {(11001, "old-root"), (22002, "old-child")}, set(boundary._owned)
            )
            self.assertTrue(p.process_boundary_is_gone(record))
            # The same instance keeps the drift error and fails closed on a
            # later cleanup attempt; a fresh module-level boundary still
            # completes using only the recorded identities.
            self.assertFalse(boundary.cleanup())
            self.assertTrue(
                p.cleanup_recorded_process_boundary(record, force=False, timeout_seconds=1.0)
            )
        # Admission probe, descendant capture probe, then parent re-verification.
        self.assertEqual([22002, 33003, 22002], reads[:3])
        unrelated_terminations = [
            call
            for call in terminate.call_args_list
            if call.args[0] == 33003
            or call.args[1] in ("new-unrelated", "new-descendant")
        ]
        self.assertEqual([], unrelated_terminations)


if __name__ == "__main__":
    unittest.main()
