from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from orchestrator_harness import leases, processes, retry_policy
from orchestrator_harness.core import content_hash
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
                "creation_time": "windows-filetime:134361777213898726",
                "acquired_at": "2026-09-03T00:00:00Z",
            },
        )
        directory = self.runtime / "epochs/epoch-1/lanes/lane-1/runs/run-1"
        worktree = self.runtime / "worktrees/lane-1"
        worktree.mkdir(parents=True, exist_ok=True)
        status = directory / "controller.status.json"
        self.lane = {"lane_id": "lane-1", "run_id": "run-1", "lifecycle": "running",
                     "process": {"pid": 41, "creation_time": "windows-filetime:134361777213898726"},
                     "controller_status_path": str(status), "worktree_path": str(worktree),
                     "provider": {"id": "qwen-code", "model": "fixture", "launch_config": {}}}
        retry_policy.initialize_history(self.runtime, self.lane)
        atomic_write_json(status, {"schema": "controller-status/v1", "lane_id": "lane-1", "run_id": "run-1",
            "controller_identity": self.lane["process"], "controller_state": "exited", "cleanup_proven": True,
            "provider_state": {"state": "not_started"}})
        invocation = {"schema": "controller-invocation/v1", "lane_id": "lane-1", "run_id": "run-1",
                      "cwd": str(worktree), "provider": self.lane["provider"],
                      "exclusive_resources": [resource_id], "paths": {"controller_status": str(status)}}
        invocation["content_hash"] = content_hash(invocation)
        atomic_write_json(directory / "invocation.json", invocation)
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
            mock.patch.object(leases, "identity_exit_proven", return_value=True),
            mock.patch.object(processes, "identity_exit_proven", return_value=True),
        ):
            leases.force_release_lease(self.runtime, "resource-1", lane=None)
        self.assertFalse(path.exists())

    def test_recycled_pid_releases_and_reads_back_absence(self) -> None:
        path = self._write()
        with (
            mock.patch.object(leases, "identity_matches", return_value=False),
            mock.patch.object(leases, "identity_exit_proven", return_value=True),
            mock.patch.object(processes, "identity_exit_proven", return_value=True),
        ):
            leases.force_release_lease(self.runtime, "resource-1", lane=None)
        self.assertFalse(path.exists())

    def test_unobservable_current_same_run_refuses(self) -> None:
        path = self._write()
        lane = {"lane_id": "lane-1", "run_id": "run-1", "lifecycle": "running"}
        with (
            mock.patch.object(leases, "identity_matches", return_value=False),
            mock.patch.object(leases, "identity_exit_proven", return_value=False),
        ):
            with self.assertRaises(leases.LeaseError) as raised:
                leases.force_release_lease(self.runtime, "resource-1", lane=lane)
        self.assertEqual(
            leases.FORCE_RELEASE_HOLDER_UNPROVEN, raised.exception.code
        )
        self.assertTrue(path.is_file())

    def test_retired_abandoned_or_superseded_run_cannot_release_unknown_holder(self) -> None:
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
                mock.patch.object(leases, "identity_exit_proven", return_value=False),
            ):
                with self.assertRaises(leases.LeaseError) as raised:
                    leases.force_release_lease(self.runtime, resource_id, lane=lane)
            self.assertEqual(leases.FORCE_RELEASE_HOLDER_UNPROVEN, raised.exception.code)
            self.assertTrue(path.exists())

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

    def test_lane_is_resolved_after_locked_lease_reread(self) -> None:
        path = self._write()
        held = False
        real_lock = leases._leases_lock

        class TrackingLock:
            def __enter__(self) -> "TrackingLock":
                nonlocal held
                self._lock = real_lock(self.runtime)
                self._lock.__enter__()
                held = True
                return self

            def __exit__(self, *args: object) -> None:
                nonlocal held
                held = False
                self._lock.__exit__(*args)

            def __init__(self, runtime: Path) -> None:
                self.runtime = runtime

        def resolve(lane_id: str) -> dict[str, object]:
            self.assertTrue(held, "lane proof must be resolved under the leases lock")
            self.assertEqual("lane-1", lane_id)
            return self.lane

        with (
            mock.patch.object(leases, "_leases_lock", TrackingLock),
            mock.patch.object(leases, "identity_matches", return_value=False),
            mock.patch.object(leases, "identity_exit_proven", return_value=True),
            mock.patch.object(processes, "identity_exit_proven", return_value=True),
        ):
            leases.force_release_lease(
                self.runtime, "resource-1", lane_resolver=resolve
            )

        self.assertFalse(path.exists())
        self.assertFalse(held)

    def test_superseded_owner_requires_old_provider_boundary_proof(self):
        path = self._write()
        status = Path(self.lane["controller_status_path"])
        atomic_write_json(status, {"schema": "controller-status/v1", "lane_id": "lane-1", "run_id": "run-1",
            "controller_identity": self.lane["process"], "controller_state": "exited", "cleanup_proven": True,
            "provider_state": {"state": "exited"}, "process_boundary": {"fixture": "old-provider"}})
        for gone in (False, True):
            with self.subTest(boundary_gone=gone), mock.patch.object(leases, "identity_matches", return_value=False), \
                 mock.patch.object(leases, "identity_exit_proven", return_value=True), \
                 mock.patch.object(processes, "identity_exit_proven", return_value=True), \
                 mock.patch.object(processes, "process_boundary_is_gone", return_value=gone) as proof:
                if gone:
                    leases.force_release_lease(self.runtime, "resource-1", lane={"lane_id": "lane-1", "run_id": "run-2"})
                else:
                    with self.assertRaises(leases.LeaseError):
                        leases.force_release_lease(self.runtime, "resource-1", lane={"lane_id": "lane-1", "run_id": "run-2"})
                    self.assertTrue(path.exists())
                proof.assert_called_with({"fixture": "old-provider"})
        self.assertFalse(path.exists())

    def test_missing_retained_authority_never_uses_newer_lane_cleanup(self):
        path = self._write()
        Path(self.lane["controller_status_path"]).unlink()
        with mock.patch.object(leases, "identity_matches", return_value=False), \
             mock.patch.object(leases, "identity_exit_proven", return_value=True):
            with self.assertRaises(leases.LeaseError) as raised:
                leases.force_release_lease(self.runtime, "resource-1", lane={"lane_id": "lane-1", "run_id": "run-2"})
        self.assertEqual(leases.FORCE_RELEASE_HOLDER_UNPROVEN, raised.exception.code)
        self.assertTrue(path.exists())

    def test_non_file_lease_blocks_acquisition_without_replacement(self):
        path = leases.lease_path(self.runtime, "resource-1")
        path.mkdir(parents=True)
        with self.assertRaises(leases.LeaseError):
            leases.acquire_leases(self.runtime, ["resource-1"], lane_id="lane-1", run_id="run-1",
                                  pid=41, creation_time="windows-filetime:134361777213898726")
        self.assertTrue(path.is_dir())

    def test_corrupt_inventory_blocks_release_before_any_owned_deletion(self):
        path = self._write()
        malformed = leases.lease_path(self.runtime, "broken")
        malformed.write_text("{broken", encoding="utf-8")
        with self.assertRaises(leases.LeaseError):
            leases.release_leases(self.runtime, "lane-1", "run-1")
        self.assertTrue(path.exists())
        with self.assertRaises(leases.LeaseError):
            leases.read_lease(self.runtime, "broken")


if __name__ == "__main__":
    unittest.main()
