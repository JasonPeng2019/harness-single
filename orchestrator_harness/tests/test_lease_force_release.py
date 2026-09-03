from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from orchestrator_harness import leases
from orchestrator_harness.records import atomic_write_json


class ForceReleaseLeaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.runtime = Path(self.temporary.name)

    def _write(
        self, resource_id: str = "resource-1", *, record_resource_id: str | None = None
    ) -> Path:
        path = leases.lease_path(self.runtime, resource_id)
        atomic_write_json(
            path,
            {
                "schema": leases.LEASE_SCHEMA,
                "resource_id": record_resource_id or resource_id,
                "lane_id": "lane-1",
                "run_id": "run-1",
                "pid": 41,
                "creation_time": "created-1",
                "acquired_at": "2026-09-03T00:00:00Z",
            },
        )
        return path

    def test_exact_live_holder_refuses_even_when_lane_is_retired(self) -> None:
        path = self._write()
        lane = {"lane_id": "lane-1", "run_id": "run-1", "lifecycle": "retired"}
        with mock.patch.object(leases, "identity_matches", return_value=True):
            with self.assertRaises(leases.LeaseError) as raised:
                leases.force_release_lease(self.runtime, "resource-1", lane=lane)
        self.assertEqual(leases.FORCE_RELEASE_HOLDER_LIVE, raised.exception.code)
        self.assertTrue(path.is_file())

    def test_absent_holder_releases_and_reads_back_absence(self) -> None:
        path = self._write()
        with (
            mock.patch.object(leases, "identity_matches", return_value=False),
            mock.patch.object(leases, "process_alive", return_value=False),
        ):
            leases.force_release_lease(self.runtime, "resource-1", lane=None)
        self.assertFalse(path.exists())

    def test_recycled_pid_releases_and_reads_back_absence(self) -> None:
        path = self._write()
        with (
            mock.patch.object(leases, "identity_matches", return_value=False),
            mock.patch.object(leases, "process_alive", return_value=True),
            mock.patch.object(
                leases,
                "process_identity",
                return_value={"pid": 41, "creation_time": "created-2"},
            ),
        ):
            leases.force_release_lease(self.runtime, "resource-1", lane=None)
        self.assertFalse(path.exists())

    def test_unobservable_current_same_run_refuses(self) -> None:
        path = self._write()
        lane = {"lane_id": "lane-1", "run_id": "run-1", "lifecycle": "running"}
        with (
            mock.patch.object(leases, "identity_matches", return_value=False),
            mock.patch.object(leases, "process_alive", return_value=True),
            mock.patch.object(leases, "process_identity", return_value=None),
        ):
            with self.assertRaises(leases.LeaseError) as raised:
                leases.force_release_lease(self.runtime, "resource-1", lane=lane)
        self.assertEqual(
            leases.FORCE_RELEASE_HOLDER_UNPROVEN, raised.exception.code
        )
        self.assertTrue(path.is_file())

    def test_retired_abandoned_or_superseded_run_releases(self) -> None:
        lane_cases = (
            {"lane_id": "lane-1", "run_id": "run-1", "lifecycle": "retired"},
            {"lane_id": "lane-1", "run_id": "run-1", "lifecycle": "abandoned"},
            {"lane_id": "lane-1", "run_id": "run-2", "lifecycle": "running"},
        )
        for index, lane in enumerate(lane_cases):
            resource_id = f"resource-{index}"
            path = self._write(resource_id)
            with (
                mock.patch.object(leases, "identity_matches", return_value=False),
                mock.patch.object(leases, "process_alive", return_value=True),
                mock.patch.object(leases, "process_identity", return_value=None),
            ):
                leases.force_release_lease(self.runtime, resource_id, lane=lane)
            self.assertFalse(path.exists())

    def test_missing_and_mismatched_record_have_stable_codes(self) -> None:
        with self.assertRaises(leases.LeaseError) as missing:
            leases.force_release_lease(self.runtime, "resource-1", lane=None)
        self.assertEqual(
            leases.FORCE_RELEASE_LEASE_MISSING, missing.exception.code
        )
        path = self._write("resource-1", record_resource_id="other-resource")
        with self.assertRaises(leases.LeaseError) as invalid:
            leases.force_release_lease(self.runtime, "resource-1", lane=None)
        self.assertEqual(
            leases.FORCE_RELEASE_LEASE_INVALID, invalid.exception.code
        )
        self.assertTrue(path.is_file())


if __name__ == "__main__":
    unittest.main()
