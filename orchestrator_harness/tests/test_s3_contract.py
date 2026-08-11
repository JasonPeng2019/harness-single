from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from orchestrator_harness.models import ProcessInfo
from orchestrator_harness.notifications import (
    CURRENT_EVENT_DISPOSITIONS,
    EVENT_DISPOSITION_OBSERVED,
    ManagerBindingError,
    ManagerEventRouter,
    ManagerRecordError,
)
from orchestrator_harness.process_supervisor import ProcessSupervisor
from orchestrator_harness.stable_io import (
    PathSafetyError,
    PreparedOutputTransaction,
)


class S3ContractTests(unittest.TestCase):
    def router(self, root: Path, *, session: str = "manager-session") -> ManagerEventRouter:
        return ManagerEventRouter(
            root,
            run_id="run-s3",
            queue_id="queue-s3",
            manager_session_id=session,
            manager_thread_id="manager-thread",
            registration_id="registration-s3",
        )

    @staticmethod
    def event(kind: str, event_id: str, identity: str, **data: object) -> dict[str, object]:
        return {
            "event_id": event_id,
            "identity": identity,
            "type": kind,
            "data": data,
        }

    def test_manager_only_queue_and_binding_isolation(self) -> None:
        """S3-MANAGER-QUEUE-001: only mapped manager facts wake one binding."""
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "manager"
            router = self.router(root)
            admitted = router.admit(
                self.event(
                    "MANAGER_SIGNAL", "signal-1", "manager-signal:signal-1",
                    signal_id="signal-1", lane_id="run-s3:lane-a", summary="review needed",
                    path="/immutable/signal-1.json", sha256="a" * 64,
                    manager_actionable=True,
                )
            )
            self.assertIsNotNone(admitted)
            router.admit(self.event("RAW_OUTPUT", "raw-1", "lane:raw", output="secret transcript"))
            router.admit(self.event("CONTROLLER_ACTIVE", "healthy-1", "lane:healthy", state="RUNNING_CODEX"))
            with self.assertRaises(ManagerBindingError):
                router.admit(self.event("MANAGER_SIGNAL", "wrong-1", "manager-signal:wrong", lane_id="other"), binding={
                    "run_id": "other-run", "queue_id": "queue-s3", "manager_session_id": "manager-session",
                    "manager_thread_id": "manager-thread", "registration_id": "registration-s3",
                })

            uncertain = router.admit(self.event("FUTURE_SOURCE", "unknown-1", "future:1", lane_id="run-s3:lane-a"))
            self.assertEqual("OBSERVATION_UNCERTAIN", uncertain["event_type"] if uncertain else None)
            queue = [json.loads(line) for line in (root / "QUEUE.jsonl").read_text(encoding="utf-8").splitlines()]
            event_types = [record["event_type"] for record in queue if record["record_kind"] == "EVENT"]
            self.assertEqual(["MANAGER_SIGNAL", "OBSERVATION_UNCERTAIN"], event_types)
            queue_text = (root / "QUEUE.jsonl").read_text(encoding="utf-8")
            self.assertNotIn("secret transcript", queue_text)
            self.assertEqual(router.wake_revision, json.loads((root / "WAKE.json").read_text(encoding="utf-8"))["wake_revision"])
            self.assertEqual(2, len(router.pending_events()))
            self.assertRaises(ManagerBindingError, router.admit, self.event("MANAGER_SIGNAL", "bad", "bad"), binding={
                "run_id": "wrong", "queue_id": "queue-s3", "manager_session_id": "manager-session",
                "manager_thread_id": "manager-thread", "registration_id": "registration-s3",
            })

    def test_queue_recovery_rebuilds_after_state_publication_failure(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "manager"
            router = self.router(root)
            original = router._transaction.atomic_json
            failed = False

            def fail_state(path: Path, value: object) -> None:
                nonlocal failed
                if path == router.state_path and not failed and isinstance(value, dict) and value.get("events"):
                    failed = True
                    raise OSError("synthetic crash after queue admission")
                original(path, value)

            with mock.patch.object(router._transaction, "atomic_json", side_effect=fail_state):
                with self.assertRaises(OSError):
                    router.admit(self.event("RESOURCE_CONFLICT", "event-1", "resource:one", resource="one"))
            self.assertEqual(1, len((root / "QUEUE.jsonl").read_text(encoding="utf-8").splitlines()))
            recovered = self.router(root)
            self.assertEqual(["event-1"], [item["event_id"] for item in recovered.replay()])
            self.assertGreaterEqual(recovered.wake_revision, 1)
            self.assertTrue(recovered.acknowledge("event-1"))
            self.assertEqual([], recovered.pending_events())

    def test_repair_recordability_rejects_oversized_whitelisted_list_before_append(self) -> None:
        """S3-REPAIR-QUEUE-RECORDABILITY: list members cannot poison the journal."""
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "manager"
            router = self.router(root)
            state_before = (root / "STATE.json").read_bytes()
            wake_before = (root / "WAKE.json").read_bytes()
            oversized = "x" * 20_000_000
            with self.assertRaises(ManagerRecordError):
                router.admit(self.event(
                    "RESOURCE_CONFLICT",
                    "oversized-list",
                    "resource:recordability",
                    resource="recordability",
                    resources=[oversized],
                ))
            self.assertEqual(b"", (root / "QUEUE.jsonl").read_bytes())
            self.assertEqual(state_before, (root / "STATE.json").read_bytes())
            self.assertEqual(wake_before, (root / "WAKE.json").read_bytes())
            aggregate = ["x" * 4096 for _ in range(5000)]
            with self.assertRaises(ManagerRecordError):
                router.admit(self.event(
                    "RESOURCE_CONFLICT",
                    "aggregate-list",
                    "resource:recordability",
                    resource="recordability",
                    resources=aggregate,
                ))
            self.assertEqual(b"", (root / "QUEUE.jsonl").read_bytes())
            self.assertEqual(state_before, (root / "STATE.json").read_bytes())
            self.assertEqual(wake_before, (root / "WAKE.json").read_bytes())

    def test_repair_supersession_requires_existing_pending_same_condition(self) -> None:
        """S3-REPAIR-EXACT-SUPERSESSION: invalid references do not mutate the journal."""
        with tempfile.TemporaryDirectory() as raw:
            router = self.router(Path(raw) / "manager")
            first = router.admit(self.event(
                "RESOURCE_CONFLICT", "condition-a", "resource:a", resource="a"
            ))
            second = router.admit(self.event(
                "RESOURCE_CONFLICT", "condition-b", "resource:b", resource="b"
            ))
            self.assertIsNotNone(first)
            self.assertIsNotNone(second)
            before = (router.queue_path).read_bytes()
            with self.assertRaises(ManagerRecordError):
                router.admit(self.event(
                    "CONDITION_CLEARED",
                    "clear-a",
                    "resource:a",
                    cleared_event_id="condition-b",
                ))
            self.assertEqual(before, router.queue_path.read_bytes())
            with self.assertRaises(ManagerRecordError):
                router.supersede("unknown-event")

            router.admit(self.event(
                "CONDITION_CLEARED",
                "clear-a",
                "resource:a",
                cleared_event_id="condition-a",
                cleared_type="RESOURCE_CONFLICT",
            ))
            self.assertEqual(["condition-b"], [item["event_id"] for item in router.pending_events()])
            router.acknowledge("condition-b")
            after_acknowledged = router.queue_path.read_bytes()
            with self.assertRaises(ManagerRecordError):
                router.supersede("condition-b", identity="resource:b")
            self.assertEqual(after_acknowledged, router.queue_path.read_bytes())
            before_already_superseded = router.queue_path.read_bytes()
            with self.assertRaises(ManagerRecordError):
                router.admit(self.event(
                    "CONDITION_CLEARED",
                    "clear-a-replay",
                    "resource:a",
                    cleared_event_id="condition-a",
                    cleared_type="RESOURCE_CONFLICT",
                ))
            self.assertEqual(before_already_superseded, router.queue_path.read_bytes())

            router.admit(self.event("RESOURCE_CONFLICT", "condition-c", "resource:c", resource="c"))
            self.assertTrue(
                router.supersede(
                    "condition-c",
                    identity="resource:c",
                    event_type="RESOURCE_CONFLICT",
                    source_type="RESOURCE_CONFLICT",
                )
            )
            self.assertNotIn("condition-c", {item["event_id"] for item in router.pending_events()})

    def test_repair_restart_republishes_later_lost_wake(self) -> None:
        """S3-REPAIR-LATER-WAKE: a nonzero old edge does not hide a new obligation."""
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "manager"
            router = self.router(root)
            router.admit(self.event("RESOURCE_CONFLICT", "event-a", "resource:a", resource="a"))
            self.assertEqual(1, router.wake_revision)
            with mock.patch.object(router, "_publish_wake", side_effect=OSError("lost revision 2")):
                with self.assertRaises(OSError):
                    router.admit(self.event("RESOURCE_CONFLICT", "event-b", "resource:b", resource="b"))
            self.assertEqual(1, router.wake_revision)
            recovered = self.router(root)
            self.assertGreater(recovered.wake_revision, 1)
            self.assertIn("event-b", {item["event_id"] for item in recovered.pending_events()})

    def test_wiring_is_exhaustive_and_timers_are_manager_events(self) -> None:
        self.assertIn("MANAGER_REVIEW_DUE", CURRENT_EVENT_DISPOSITIONS)
        self.assertIn("LANE_NO_PROGRESS", CURRENT_EVENT_DISPOSITIONS)
        self.assertIn("LANE_STAGE_REPEAT", CURRENT_EVENT_DISPOSITIONS)
        self.assertNotEqual(EVENT_DISPOSITION_OBSERVED, CURRENT_EVENT_DISPOSITIONS["MANAGER_REVIEW_DUE"])
        for raw_type in ("RAW_OUTPUT", "WORKER_OUTPUT", "LOG", "DIAGNOSTIC", "PROVIDER_TELEMETRY", "HEARTBEAT"):
            self.assertEqual(EVENT_DISPOSITION_OBSERVED, CURRENT_EVENT_DISPOSITIONS[raw_type])

    def test_wake_coalesces_delivery_without_acknowledging_events(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            router = self.router(Path(raw) / "manager")
            first = router.admit(self.event("CHECKPOINT_UPDATED", "checkpoint-1", "lane:a:checkpoint", lane_id="a", checkpoint_path="a", checkpoint_sha256="a" * 64))
            second = router.admit(self.event("CHECKPOINT_UPDATED", "checkpoint-2", "lane:a:checkpoint", lane_id="a", checkpoint_path="a", checkpoint_sha256="b" * 64))
            assert first is not None and second is not None
            pending = router.pending_events()
            self.assertEqual(["checkpoint-2"], [item["event_id"] for item in pending])
            receipt = router.record_delivery(delivery_id="delivery-1", wake_revision=router.wake_revision, event_ids=["checkpoint-2"])
            self.assertEqual(["checkpoint-2"], receipt["event_ids"])
            self.assertEqual(["checkpoint-2"], [item["event_id"] for item in router.pending_events()])
            queue = [json.loads(line) for line in (Path(raw) / "manager" / "QUEUE.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertTrue(any(item["record_kind"] == "SUPERSESSION" for item in queue))

    def test_shared_append_lock_keeps_two_processes_intact(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "shared.jsonl"
            script = (
                "from pathlib import Path; import sys; "
                "from orchestrator_harness.stable_io import append_jsonl_record; "
                "p=Path(sys.argv[1]); n=sys.argv[2]; "
                "[append_jsonl_record(p, {'writer': n, 'index': i}) for i in range(75)]"
            )
            env = dict(os.environ)
            existing_pythonpath = env.get("PYTHONPATH")
            env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(Path.cwd()), existing_pythonpath]))
            processes = [
                subprocess.Popen([sys.executable, "-c", script, str(path), str(index)], env=env)
                for index in range(2)
            ]
            self.assertEqual([0, 0], [process.wait(timeout=30) for process in processes])
            records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(150, len(records))
            self.assertEqual(150, len({(item["writer"], item["index"]) for item in records}))

    def test_prepared_output_rejects_path_attacks_and_root_swap(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            root = base / "prepared"
            outside = base / "outside"
            outside.mkdir()
            transaction = PreparedOutputTransaction(root, allowed_roots=(base,), forbidden_roots=(outside,))
            transaction.prepare()
            transaction.atomic_json(transaction.child("safe.json"), {"ok": True})
            escaped = root / "escape.json"
            if hasattr(os, "symlink"):
                try:
                    os.symlink(outside, escaped, target_is_directory=True)
                except OSError:
                    pass
                else:
                    with self.assertRaises(PathSafetyError):
                        transaction.atomic_json(escaped / "written.json", {"bad": True})
                    self.assertFalse((outside / "written.json").exists())
            with self.assertRaises(PathSafetyError):
                transaction.child("stream:name")
            moved = base / "prepared-old"
            root.rename(moved)
            root.mkdir()
            with self.assertRaises(PathSafetyError):
                transaction.atomic_json(root / "swapped.json", {"bad": True})
            self.assertFalse((outside / "swapped.json").exists())

    def test_supervisor_proves_force_reap_before_release(self) -> None:
        class HungChild:
            pid = 404

            def __init__(self) -> None:
                self.terminated = False
                self.killed = False

            def poll(self) -> None:
                return None

            def terminate(self) -> None:
                self.terminated = True

            def kill(self) -> None:
                self.killed = True

            def wait(self, timeout: float | None = None) -> int:
                if self.killed:
                    return 137
                raise subprocess.TimeoutExpired("child", timeout or 0)

        child = HungChild()
        identity = ProcessInfo(404, os.getpid(), "child", "child", datetime.now(timezone.utc))
        result = ProcessSupervisor(child, identity, graceful_timeout_seconds=0, observer=None).cleanup()
        self.assertTrue(child.terminated)
        self.assertTrue(child.killed)
        self.assertTrue(result.proved_reap)
        self.assertEqual("force_stop", result.reaped_after)


if __name__ == "__main__":
    unittest.main()
