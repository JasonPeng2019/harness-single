from __future__ import annotations

"""Producer-local regressions for recorded-boundary exact ownership.

Proof634 showed that ProcessBoundary.observe selected by numeric PID alone and
adopted a reused non-root PID plus its unrelated descendants into _owned.
These tests use the real ProcessBoundary methods with fake snapshot, identity,
and termination functions only; no real process is observed or terminated.
"""

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from orchestrator_harness import processes as p
from orchestrator_harness.models import ProcessInfo, ProcessSnapshot, iso_utc

CREATED = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)
# Snapshot facts in the motivating regressions capture the old child
# incarnation at 04:00; live probes report the reused incarnation at 04:01.
SNAPSHOT_CHILD_CREATED = datetime(2026, 9, 18, 4, 0, tzinfo=timezone.utc)
REUSED_CHILD_CREATED = datetime(2026, 9, 18, 4, 1, tzinfo=timezone.utc)
# Native identity fixtures mirror the repository's real snapshot
# construction: Linux /proc start_ticks from boot/clock ticks, Windows CIM
# FILETIME truncation to whole microseconds (ROOT-681 readback), and Darwin
# libproc start_tvsec/start_tvusec.
LINUX_BOOT = datetime(2026, 9, 18, 0, 0, tzinfo=timezone.utc)
LINUX_CLOCK_TICKS = 100
LINUX_START_TICKS = 360_000
LINUX_CHILD_CREATED = LINUX_BOOT + timedelta(
    seconds=LINUX_START_TICKS / LINUX_CLOCK_TICKS
)
WINDOWS_FILETIME_EPOCH = datetime(1601, 1, 1, tzinfo=timezone.utc)
WINDOWS_FILETIME = 134341817652159491
WINDOWS_NATIVE_CREATED = WINDOWS_FILETIME_EPOCH + timedelta(
    microseconds=WINDOWS_FILETIME // 10
)
DARWIN_SECONDS = 1789708165
DARWIN_MICROSECONDS = 215949
DARWIN_NATIVE_CREATED = datetime.fromtimestamp(
    DARWIN_SECONDS + DARWIN_MICROSECONDS / 1_000_000, tz=timezone.utc
)


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
        identities = {22002: "old-child", 33003: iso_utc(CREATED)}
        with (
            patch.object(p, "process_identity", side_effect=fake_identity(identities)),
            patch.object(p, "process_alive", side_effect=lambda pid: pid in identities),
            patch.object(p, "process_snapshot", return_value=processes),
        ):
            boundary = p.ProcessBoundary.from_record(record, snapshot_provider=lambda: processes)
            self.assertTrue(boundary.observe())
            self.assertIn((33003, iso_utc(CREATED)), boundary._owned)
            remaining = boundary._remaining()
            self.assertIsNotNone(remaining)
            self.assertEqual(
                {(22002, "old-child"), (33003, iso_utc(CREATED))}, set(remaining)
            )
            self.assertFalse(p.process_boundary_is_gone(record))

    def test_snapshot_child_reused_beneath_exact_surviving_parent_fails_closed(
        self,
    ) -> None:
        # ROOT-664: the recorded parent 22002 survives exact, but the
        # descendant PID 33003 was reused after the snapshot captured its stale
        # ppid edge. The live 33003 is a different incarnation and must not be
        # adopted nor terminated.
        record = recorded_boundary()
        processes = snapshot(
            ProcessInfo(22002, 90000, "child.exe", "owned child", SNAPSHOT_CHILD_CREATED),
            ProcessInfo(
                33003,
                22002,
                "unrelated-child.exe",
                "unrelated descendant",
                SNAPSHOT_CHILD_CREATED,
            ),
        )
        identities = {
            22002: "old-child",
            33003: iso_utc(REUSED_CHILD_CREATED),
        }
        terminate = MagicMock(return_value=True)
        with (
            patch.object(p, "process_identity", side_effect=fake_identity(identities)),
            patch.object(p, "process_alive", side_effect=lambda pid: pid in identities),
            patch.object(p, "process_snapshot", return_value=processes),
            patch.object(p, "terminate_process", terminate),
        ):
            boundary = p.ProcessBoundary.from_record(record, snapshot_provider=lambda: processes)
            self.assertFalse(boundary.observe())
            self.assertNotIn((33003, iso_utc(REUSED_CHILD_CREATED)), boundary._owned)
            self.assertIn((22002, "old-child"), boundary._owned)
            self.assertEqual(
                {(11001, "old-root"), (22002, "old-child")}, set(boundary._owned)
            )
            self.assertEqual([(22002, "old-child")], boundary._remaining())
            self.assertFalse(p.process_boundary_is_gone(record))
            self.assertFalse(boundary.cleanup())
            self.assertFalse(
                p.cleanup_recorded_process_boundary(record, force=False, timeout_seconds=1.0)
            )
        reused_terminations = [
            call
            for call in terminate.call_args_list
            if call.args[0] == 33003
            or call.args[1] == iso_utc(REUSED_CHILD_CREATED)
        ]
        self.assertEqual([], reused_terminations)

    def test_stale_group_snapshot_reused_child_fails_closed_and_never_terminated(
        self,
    ) -> None:
        # ROOT-664 group/session variant: the recorded group containment fact
        # comes from the snapshot, but PID 22002 was reused after that snapshot.
        # The stale group affiliation must not lend ownership to the new
        # incarnation; no unrelated termination may be authorized.
        record = recorded_boundary()
        record["process_group_id"] = 9000
        processes = snapshot(
            ProcessInfo(
                11001,
                90000,
                "root.exe",
                "root",
                SNAPSHOT_CHILD_CREATED,
                process_group_id=9000,
            ),
            ProcessInfo(
                22002,
                11001,
                "child.exe",
                "owned child",
                SNAPSHOT_CHILD_CREATED,
                process_group_id=9000,
            ),
        )
        identities = {
            11001: "old-root",
            22002: iso_utc(REUSED_CHILD_CREATED),
        }
        terminate = MagicMock(return_value=True)
        with (
            patch.object(p, "process_identity", side_effect=fake_identity(identities)),
            patch.object(p, "process_alive", side_effect=lambda pid: pid in identities),
            patch.object(p, "process_snapshot", return_value=processes),
            patch.object(p, "terminate_process", terminate),
        ):
            boundary = p.ProcessBoundary.from_record(record, snapshot_provider=lambda: processes)
            self.assertFalse(boundary.observe())
            self.assertNotIn((22002, iso_utc(REUSED_CHILD_CREATED)), boundary._owned)
            self.assertEqual(
                {(11001, "old-root"), (22002, "old-child")}, set(boundary._owned)
            )
            self.assertFalse(p.process_boundary_is_gone(record))
            self.assertFalse(boundary.cleanup())
            self.assertFalse(
                p.cleanup_recorded_process_boundary(record, force=False, timeout_seconds=1.0)
            )
        reused_terminations = [
            call
            for call in terminate.call_args_list
            if call.args[0] == 22002
            and call.args[1] == iso_utc(REUSED_CHILD_CREATED)
        ]
        self.assertEqual([], reused_terminations)

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

    def test_linux_native_start_ticks_binds_unchanged_new_descendant(self) -> None:
        # Finding S1: _identity_time must convert linux-start-ticks through
        # the same boot/clock-ticks primitives the /proc snapshot uses, so an
        # unchanged never-recorded descendant remains legitimate and can be
        # adopted through the stale snapshot edge.
        record = recorded_boundary()
        token = f"linux-start-ticks:{LINUX_START_TICKS}"
        processes = snapshot(
            ProcessInfo(22002, 90000, "child.exe", "owned child", LINUX_CHILD_CREATED),
            ProcessInfo(
                33003,
                22002,
                "descendant.exe",
                "legitimate descendant",
                LINUX_CHILD_CREATED,
            ),
        )
        identities = {22002: "old-child", 33003: token}
        terminate = MagicMock(return_value=True)
        with (
            patch.object(p, "process_identity", side_effect=fake_identity(identities)),
            patch.object(p, "process_alive", side_effect=lambda pid: pid in identities),
            patch.object(p, "process_snapshot", return_value=processes),
            patch.object(p, "_linux_boot_time", return_value=LINUX_BOOT),
            patch.object(p, "_linux_clock_ticks", return_value=LINUX_CLOCK_TICKS),
            patch.object(p, "terminate_process", terminate),
        ):
            self.assertEqual(p._identity_time(token), LINUX_CHILD_CREATED)
            boundary = p.ProcessBoundary.from_record(record, snapshot_provider=lambda: processes)
            self.assertTrue(boundary.observe())
            self.assertIn((33003, token), boundary._owned)
            remaining = boundary._remaining()
            self.assertIsNotNone(remaining)
            self.assertEqual({(22002, "old-child"), (33003, token)}, set(remaining))
            self.assertFalse(p.process_boundary_is_gone(record))
            # Adoption itself never terminates anything.
            self.assertEqual([], terminate.call_args_list)

    def test_linux_native_reused_start_ticks_rejected_and_never_terminated(self) -> None:
        # A reused Linux incarnation (different start_ticks) discovered
        # through the stale snapshot edge must fail closed and never be
        # adopted or terminated.
        record = recorded_boundary()
        reused_token = f"linux-start-ticks:{LINUX_START_TICKS + 1}"
        processes = snapshot(
            ProcessInfo(22002, 90000, "child.exe", "owned child", LINUX_CHILD_CREATED),
            ProcessInfo(
                33003,
                22002,
                "unrelated-child.exe",
                "later incarnation",
                LINUX_CHILD_CREATED,
            ),
        )
        identities = {22002: "old-child", 33003: reused_token}
        terminate = MagicMock(return_value=True)
        with (
            patch.object(p, "process_identity", side_effect=fake_identity(identities)),
            patch.object(p, "process_alive", side_effect=lambda pid: pid in identities),
            patch.object(p, "process_snapshot", return_value=processes),
            patch.object(p, "_linux_boot_time", return_value=LINUX_BOOT),
            patch.object(p, "_linux_clock_ticks", return_value=LINUX_CLOCK_TICKS),
            patch.object(p, "terminate_process", terminate),
        ):
            boundary = p.ProcessBoundary.from_record(record, snapshot_provider=lambda: processes)
            self.assertFalse(boundary.observe())
            self.assertNotIn((33003, reused_token), boundary._owned)
            self.assertEqual(
                {(11001, "old-root"), (22002, "old-child")}, set(boundary._owned)
            )
            self.assertEqual([(22002, "old-child")], boundary._remaining())
            self.assertFalse(p.process_boundary_is_gone(record))
            self.assertFalse(boundary.cleanup())
            self.assertFalse(
                p.cleanup_recorded_process_boundary(record, force=False, timeout_seconds=1.0)
            )
        reused_terminations = [
            call
            for call in terminate.call_args_list
            if call.args[0] == 33003 or call.args[1] == reused_token
        ]
        self.assertEqual([], reused_terminations)

    def test_windows_native_filetime_conversion_matches_snapshot_precision(self) -> None:
        # Finding S2: the live FILETIME must floor to whole microseconds like
        # the CIM snapshot's .NET 'o' string truncation. Float division would
        # round ROOT-681's 134341817652159491 (and remainder >= 5
        # neighbours) upward to ...215950, breaking the exact equality.
        self.assertEqual(
            p._identity_time(f"windows-filetime:{WINDOWS_FILETIME}"),
            WINDOWS_NATIVE_CREATED,
        )
        for nearby in (
            WINDOWS_FILETIME - 1,
            WINDOWS_FILETIME,
            WINDOWS_FILETIME + 4,
            WINDOWS_FILETIME + 8,
        ):
            self.assertEqual(
                p._identity_time(f"windows-filetime:{nearby}"),
                WINDOWS_NATIVE_CREATED,
                f"FILETIME {nearby} must floor to {WINDOWS_NATIVE_CREATED}",
            )
        later = WINDOWS_FILETIME + 10
        self.assertNotEqual(
            p._identity_time(f"windows-filetime:{later}"), WINDOWS_NATIVE_CREATED
        )
        # Malformed and unprovable values fail closed (no safe absence).
        self.assertIsNone(p._identity_time("windows-filetime:not-a-number"))
        self.assertIsNone(p._identity_time("windows-filetime:-1"))
        self.assertIsNone(p._identity_time("windows-filetime:18446744073709551615"))

    def test_windows_native_filetime_binds_unchanged_new_descendant(self) -> None:
        record = recorded_boundary()
        token = f"windows-filetime:{WINDOWS_FILETIME}"
        processes = snapshot(
            ProcessInfo(22002, 90000, "child.exe", "owned child", WINDOWS_NATIVE_CREATED),
            ProcessInfo(
                33003,
                22002,
                "descendant.exe",
                "legitimate descendant",
                WINDOWS_NATIVE_CREATED,
            ),
        )
        identities = {22002: "old-child", 33003: token}
        terminate = MagicMock(return_value=True)
        with (
            patch.object(p, "process_identity", side_effect=fake_identity(identities)),
            patch.object(p, "process_alive", side_effect=lambda pid: pid in identities),
            patch.object(p, "process_snapshot", return_value=processes),
            patch.object(p, "terminate_process", terminate),
        ):
            boundary = p.ProcessBoundary.from_record(record, snapshot_provider=lambda: processes)
            self.assertTrue(boundary.observe())
            self.assertIn((33003, token), boundary._owned)
            remaining = boundary._remaining()
            self.assertIsNotNone(remaining)
            self.assertEqual({(22002, "old-child"), (33003, token)}, set(remaining))
            self.assertFalse(p.process_boundary_is_gone(record))
            self.assertEqual([], terminate.call_args_list)

    def test_windows_native_filetime_reused_incarnation_rejected_and_never_terminated(
        self,
    ) -> None:
        record = recorded_boundary()
        later_token = f"windows-filetime:{WINDOWS_FILETIME + 10}"
        processes = snapshot(
            ProcessInfo(22002, 90000, "child.exe", "owned child", WINDOWS_NATIVE_CREATED),
            ProcessInfo(
                33003,
                22002,
                "unrelated-child.exe",
                "later incarnation",
                WINDOWS_NATIVE_CREATED,
            ),
        )
        identities = {22002: "old-child", 33003: later_token}
        terminate = MagicMock(return_value=True)
        with (
            patch.object(p, "process_identity", side_effect=fake_identity(identities)),
            patch.object(p, "process_alive", side_effect=lambda pid: pid in identities),
            patch.object(p, "process_snapshot", return_value=processes),
            patch.object(p, "terminate_process", terminate),
        ):
            boundary = p.ProcessBoundary.from_record(record, snapshot_provider=lambda: processes)
            self.assertFalse(boundary.observe())
            self.assertNotIn((33003, later_token), boundary._owned)
            self.assertEqual(
                {(11001, "old-root"), (22002, "old-child")}, set(boundary._owned)
            )
            self.assertEqual([(22002, "old-child")], boundary._remaining())
            self.assertFalse(boundary.cleanup())
            self.assertFalse(
                p.cleanup_recorded_process_boundary(record, force=False, timeout_seconds=1.0)
            )
        reused_terminations = [
            call
            for call in terminate.call_args_list
            if call.args[0] == 33003 or call.args[1] == later_token
        ]
        self.assertEqual([], reused_terminations)

    def test_darwin_native_start_time_binds_unchanged_new_descendant(self) -> None:
        # The Darwin conversion must keep matching its native snapshot math
        # (libproc start_tvsec + start_tvusec).
        record = recorded_boundary()
        token = f"darwin-start-time:{DARWIN_SECONDS}:{DARWIN_MICROSECONDS}"
        processes = snapshot(
            ProcessInfo(22002, 90000, "child.exe", "owned child", DARWIN_NATIVE_CREATED),
            ProcessInfo(
                33003,
                22002,
                "descendant.exe",
                "legitimate descendant",
                DARWIN_NATIVE_CREATED,
            ),
        )
        identities = {22002: "old-child", 33003: token}
        terminate = MagicMock(return_value=True)
        with (
            patch.object(p, "process_identity", side_effect=fake_identity(identities)),
            patch.object(p, "process_alive", side_effect=lambda pid: pid in identities),
            patch.object(p, "process_snapshot", return_value=processes),
            patch.object(p, "terminate_process", terminate),
        ):
            self.assertEqual(p._identity_time(token), DARWIN_NATIVE_CREATED)
            boundary = p.ProcessBoundary.from_record(record, snapshot_provider=lambda: processes)
            self.assertTrue(boundary.observe())
            self.assertIn((33003, token), boundary._owned)
            remaining = boundary._remaining()
            self.assertIsNotNone(remaining)
            self.assertEqual({(22002, "old-child"), (33003, token)}, set(remaining))
            self.assertFalse(p.process_boundary_is_gone(record))
            self.assertEqual([], terminate.call_args_list)
        self.assertIsNone(p._identity_time("darwin-start-time:abc:def"))
        self.assertIsNone(p._identity_time("darwin-start-time:5:1000000"))


if __name__ == "__main__":
    unittest.main()
