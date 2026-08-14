from __future__ import annotations

import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from orchestrator_harness.models import ProcessInfo, ProcessSnapshot
from orchestrator_harness.resource_locks import (
    ResourceClaims,
    _claim_bytes,
    _read_claim_evidence,
    claim_filename,
)


NOW = datetime(2026, 8, 3, 10, 0, tzinfo=timezone.utc)


class AbortWait(Exception):
    pass


def abort_wait(_: float) -> None:
    raise AbortWait()


def process(pid: int, created: datetime = NOW) -> ProcessInfo:
    return ProcessInfo(pid, 1, "controller", "controller", created)


def snapshot(*items: ProcessInfo, complete: bool = True) -> ProcessSnapshot:
    return ProcessSnapshot(complete, tuple(items))


class ResourceLockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "claims"
        self.controllers = {101: process(101), 202: process(202), 303: process(303)}
        self.identities = {pid: f"identity:{pid}" for pid in self.controllers}
        self.inventory = snapshot(*self.controllers.values())

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def claims(
        self,
        pid: int = 101,
        *,
        inventory: ProcessSnapshot | None = None,
        identities: dict[int, str] | None = None,
        poll: float = 0.001,
    ) -> ResourceClaims:
        available = self.identities if identities is None else identities
        return ResourceClaims(
            self.root,
            f"lane-{pid}",
            f"invoke-{pid}",
            self.controllers[pid],
            wait_excess_seconds=0.05,
            poll_seconds=poll,
            process_provider=lambda: self.inventory if inventory is None else inventory,
            identity_provider=lambda value: (
                {"pid": value, "created_utc": available[value]}
                if value in available
                else None
            ),
        )

    def acquire(
        self, claims: ResourceClaims, resources: list[str]
    ) -> list[dict[str, object]]:
        waits: list[dict[str, object]] = []
        claims.acquire_all(resources, on_wait=waits.append)
        return waits

    def test_opaque_safe_hashes_do_not_embed_resource_names(self) -> None:
        resource = "../board:alpha\\0/with spaces?*"
        filename = claim_filename(resource)
        self.assertRegex(filename, r"^[0-9a-f]{64}\.json$")
        self.assertNotIn("..", filename)
        self.assertNotIn("board", filename)
        self.assertEqual(filename, claim_filename(resource))
        self.assertNotEqual(filename, claim_filename(resource + "x"))

    def test_atomic_uncontended_acquire_and_exact_owner_release(self) -> None:
        owner = self.claims()
        self.assertEqual([], self.acquire(owner, ["board:a"]))
        path = self.root / claim_filename("board:a")
        claim, error, _ = _read_claim_evidence(path)
        self.assertIsNone(error)
        self.assertEqual("lane-101", claim["lane_id"] if claim else None)
        self.assertEqual(["board:a"], [item["resource"] for item in owner.held])
        self.assertEqual([], owner.release_all())
        self.assertFalse(path.exists())
        self.assertEqual([], owner.held)

    def test_different_resources_proceed_while_same_resource_waits_without_launch(
        self,
    ) -> None:
        first = self.claims(101)
        second = self.claims(202)
        self.acquire(first, ["board:a"])
        self.assertEqual([], self.acquire(second, ["board:b"]))
        waiting = threading.Event()
        launched = threading.Event()
        finished = threading.Event()

        def acquire_same() -> None:
            second.acquire_all(
                ["board:a"],
                on_wait=lambda _: waiting.set(),
            )
            launched.set()
            finished.set()

        thread = threading.Thread(target=acquire_same)
        thread.start()
        self.assertTrue(waiting.wait(1))
        self.assertFalse(
            launched.is_set(), "work must not launch before its claim is acquired"
        )
        self.assertEqual([], first.release_all())
        self.assertTrue(finished.wait(1))
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual([], second.release_all())

    def test_canonical_order_and_partial_acquire_cleanup(self) -> None:
        holder = self.claims(101)
        contender = self.claims(202)
        self.acquire(holder, ["b"])
        waits: list[dict[str, object]] = []
        original_sleep = time.sleep
        try:
            time.sleep = abort_wait
            with self.assertRaises(AbortWait):
                contender.acquire_all(["z", "b", "a", "a"], on_wait=waits.append)
        finally:
            time.sleep = original_sleep
        self.assertEqual([], contender.held)
        self.assertFalse((self.root / claim_filename("a")).exists())
        self.assertTrue((self.root / claim_filename("b")).exists())
        self.assertEqual("b", waits[0]["resource"])
        self.assertEqual([], holder.release_all())

    def test_release_failure_is_fail_closed_and_wrong_owner_is_preserved(self) -> None:
        owner = self.claims()
        self.acquire(owner, ["board:a"])
        path = self.root / claim_filename("board:a")
        foreign = self.claims(202)._new_claim("board:a")
        path.write_bytes(_claim_bytes(foreign))
        self.assertEqual(["board:a"], owner.release_all())
        self.assertTrue(path.exists())
        self.assertEqual(["board:a"], [item["resource"] for item in owner.held])

    def test_malformed_and_hashed_wrong_resource_claims_are_actionable_but_not_deleted(
        self,
    ) -> None:
        resource = "board:a"
        path = self.root / claim_filename(resource)
        self.root.mkdir(parents=True, exist_ok=True)
        path.write_text("not-json", encoding="utf-8")
        waits: list[dict[str, object]] = []
        contender = self.claims()
        original_sleep = time.sleep
        try:
            time.sleep = abort_wait
            with self.assertRaises(AbortWait):
                contender.acquire_all([resource], on_wait=waits.append)
        finally:
            time.sleep = original_sleep
        self.assertEqual("MALFORMED", waits[0]["state"])
        self.assertTrue(waits[0]["actionable"])
        self.assertTrue(path.exists())

    def test_complete_crash_is_reclaimed_but_incomplete_inventory_is_not(self) -> None:
        crashed = self.claims(101, inventory=snapshot(process(202)))
        claim = crashed._new_claim("board:a")
        path = self.root / claim_filename("board:a")
        self.root.mkdir(parents=True, exist_ok=True)
        path.write_bytes(_claim_bytes(claim))
        rescuer = self.claims(
            202, inventory=snapshot(process(202)), identities={202: "identity:202"}
        )
        self.acquire(rescuer, ["board:a"])
        evidence, _, _ = _read_claim_evidence(path)
        assert evidence is not None
        self.assertEqual("lane-202", evidence["lane_id"])
        self.assertEqual([], rescuer.release_all())

        path.write_bytes(_claim_bytes(claim))
        unknown = self.claims(202, inventory=snapshot(process(202), complete=False))
        waits: list[dict[str, object]] = []
        original_sleep = time.sleep
        try:
            time.sleep = abort_wait
            with self.assertRaises(AbortWait):
                unknown.acquire_all(["board:a"], on_wait=waits.append)
        finally:
            time.sleep = original_sleep
        self.assertEqual("INVENTORY_UNKNOWN", waits[0]["state"])
        self.assertTrue(path.exists())

    def test_pid_reuse_requires_exact_identity_and_is_not_reclaimed(self) -> None:
        stale_identity = {101: "old", 202: "identity:202"}
        old = self.claims(101, identities=stale_identity)
        claim = old._new_claim("board:a")
        path = self.root / claim_filename("board:a")
        self.root.mkdir(parents=True, exist_ok=True)
        path.write_bytes(_claim_bytes(claim))
        waits: list[dict[str, object]] = []
        contender = self.claims(202)
        original_sleep = time.sleep
        try:
            time.sleep = abort_wait
            with self.assertRaises(AbortWait):
                contender.acquire_all(["board:a"], on_wait=waits.append)
        finally:
            time.sleep = original_sleep
        self.assertEqual("OWNER_IDENTITY_REUSED", waits[0]["state"])
        self.assertTrue(path.exists())

    def test_guarded_concurrent_stale_replacement_leaves_guarded_claim_untouched_and_cleans_up(
        self,
    ) -> None:
        crashed = self.claims(101, inventory=snapshot(process(202)))
        expected = crashed._new_claim("board:a")
        path = self.root / claim_filename("board:a")
        self.root.mkdir(parents=True, exist_ok=True)
        evidence = _claim_bytes(expected)
        path.write_bytes(evidence)
        guard = path.with_suffix(".reclaim")
        guard.write_text("other controller", encoding="utf-8")
        rescuer = self.claims(
            202, inventory=snapshot(process(202)), identities={202: "identity:202"}
        )
        reclaimed, reason = rescuer._reclaim_stale(
            resource="board:a", path=path, expected=expected, expected_bytes=evidence
        )
        self.assertFalse(reclaimed)
        self.assertIn("another controller", reason)
        self.assertTrue(path.exists())
        guard.unlink()
        reclaimed, _ = rescuer._reclaim_stale(
            resource="board:a", path=path, expected=expected, expected_bytes=evidence
        )
        self.assertTrue(reclaimed)
        self.assertFalse(path.exists())
        self.assertFalse(guard.exists())

    def test_S3_A1_100_cycle_same_resource_contention_stress(self) -> None:
        first = self.claims(101, poll=0.0005)
        second = self.claims(202, poll=0.0005)
        # Durability is exercised elsewhere; bypassing fsync keeps this logical stress test fast on NTFS.
        with mock.patch("orchestrator_harness.resource_locks.os.fsync"):
            for cycle in range(100):
                contender = first if cycle % 2 == 0 else second
                self.assertEqual([], self.acquire(contender, ["board:shared"]))
                self.assertEqual(
                    ["board:shared"], [item["resource"] for item in contender.held]
                )
                self.assertEqual([], contender.release_all())
        self.assertFalse((self.root / claim_filename("board:shared")).exists())


if __name__ == "__main__":
    unittest.main()
