"""Recorded Windows Job absence must distinguish exact PID reuse from survival."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from orchestrator_harness import processes as p
from orchestrator_harness.models import ProcessQuery, ProcessSnapshot
from orchestrator_harness.tests.test_recorded_boundary_identity import (
    CREATED,
    fake_identity,
    recorded_boundary,
    snapshot,
)
from orchestrator_harness.models import ProcessInfo


class RecordedJobAbsenceTests(unittest.TestCase):
    def setUp(self):
        # Fixture PIDs must be interpreted by the simulated OS throughout.
        native = patch.object(p, "_windows_identity_exit_proven", return_value=None)
        native.start()
        self.addCleanup(native.stop)
        # The fake OS reports complete absence only for fixture PIDs known dead.
        query = patch.object(p, "targeted_process_query", side_effect=lambda pid: ProcessQuery(not p.process_alive(pid), None))
        query.start()
        self.addCleanup(query.stop)

    def check_absence(self, identities, live, observations, expected):
        with (
            patch.object(p.os, "name", "nt"),
            patch.object(p, "process_identity", side_effect=fake_identity(identities)),
            patch.object(p, "process_alive", side_effect=lambda pid: pid in live),
            patch.object(p, "process_snapshot", return_value=observations),
            patch.object(p, "terminate_process") as terminate,
        ):
            self.assertIs(p.process_boundary_is_gone(recorded_boundary()), expected)
            terminate.assert_not_called()

    def test_reused_root_is_exact_absence_without_adopting_new_descendants(self):
        self.check_absence(
            {11001: "2026-07-30T12:01:00Z", 33003: "2026-07-30T12:03:00Z"},
            {11001, 33003},
            snapshot(
                ProcessInfo(11001, 1, "chrome.exe", "unrelated", CREATED),
                ProcessInfo(33003, 11001, "chrome.exe", "unrelated child", CREATED),
            ),
            True,
        )

    def test_all_exact_members_gone_does_not_require_unrelated_global_inventory(self):
        self.check_absence(
            {}, set(), ProcessSnapshot(False, (), ("unrelated denied",), "fake"), True
        )

    def test_exact_live_child_remains_unconfirmed_after_root_reuse(self):
        self.check_absence(
            {11001: "2026-07-30T12:01:00Z", 22002: "2026-07-30T12:00:00Z"},
            {11001, 22002},
            snapshot(ProcessInfo(11001, 1, "chrome.exe", "unrelated", CREATED)),
            False,
        )

    def test_denied_live_child_identity_remains_unconfirmed_after_root_reuse(self):
        self.check_absence(
            {11001: "2026-07-30T12:01:00Z"},
            {11001, 22002},
            snapshot(ProcessInfo(11001, 1, "chrome.exe", "unrelated", CREATED)),
            False,
        )

    def test_denied_live_root_identity_remains_unconfirmed(self):
        self.check_absence({}, {11001}, snapshot(), False)

    def test_malformed_record_is_not_absence_even_when_no_pid_is_live(self):
        record = recorded_boundary()
        record["processes"].append({"pid": 33003})
        with (
            patch.object(p.os, "name", "nt"),
            patch.object(p, "process_alive", return_value=False),
            patch.object(p, "process_snapshot", return_value=snapshot()),
        ):
            self.assertFalse(p.process_boundary_is_gone(record))

    def test_posix_record_still_requires_complete_group_inventory(self):
        record = recorded_boundary()
        record["kind"] = "posix-process-group"
        record["process_group_id"] = 11001
        with (
            patch.object(p.os, "name", "posix"),
            patch.object(p, "process_alive", return_value=False),
            patch.object(
                p,
                "process_snapshot",
                return_value=ProcessSnapshot(False, (), ("denied",), "fake"),
            ),
        ):
            self.assertFalse(p.process_boundary_is_gone(record))


if __name__ == "__main__":
    unittest.main()
