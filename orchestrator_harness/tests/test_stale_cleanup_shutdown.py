"""Public shutdown leaves stale cleanup bookkeeping only through exact native proof.

Finite native Python processes stand in for the controller and provider; the
provider and its helper run in a real provider Job/process group with protected
invocation history. No model provider runs. Fixtures belong to shallow OS temp
directories. Set STALE_CLEANUP_EVIDENCE to retain public command transcripts.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from orchestrator_harness import lane_cleanup, processes, retry_policy, setup, shutdown
from orchestrator_harness.epochs import lane_record_dir, read_current_epoch
from orchestrator_harness.lanes import read_lane, reconcile_active_lanes, write_lane
from orchestrator_harness.records import atomic_write_json
from orchestrator_harness.tests.result_fixture import LinkedResultFixture

# The provider leaves one helper in its boundary, then exits.
PROVIDER = ("import subprocess, sys\n"
            "helper = subprocess.Popen([sys.executable, '-B', '-c', 'import time; time.sleep(120)'])\n"
            "open(sys.argv[1], 'w').write(str(helper.pid))\n")
WAITER = "import sys; sys.stdin.buffer.read(1)"
SESSION = "saved-native-session"


def release(child: subprocess.Popen) -> None:
    if child.poll() is None:
        child.communicate(b"x", timeout=10)


class StaleCleanupShutdownTests(LinkedResultFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        atomic_write_json(self.base / "harness-config.json", {
            "root_workspace": str(self.root), "managed_coordination": "disabled",
        })
        atomic_write_json(self.base / "resource-manifest.json", {
            "schema": "resource-manifest/v1", "resources": [],
        })
        setup.set_runtime_state(self.rt, "OPEN")
        reconcile_active_lanes(self.rt, self.epoch)
        self.card_path = self.base / "resume-card.json"
        atomic_write_json(self.card_path, {**self.task, "task": "Retry with the saved session",
                                           "branch": "lane/worker/work"})
        self.commands: list[dict] = []
        self.addCleanup(self.retain_evidence)
        # An unrelated live process must survive every cleanup route.
        self.sentinel = self.waiter()
        self.sentinel_identity = processes.process_identity(self.sentinel.pid)
        self.assertIsNotNone(self.sentinel_identity)

    def waiter(self) -> subprocess.Popen:
        child = subprocess.Popen([sys.executable, "-B", "-c", WAITER], stdin=subprocess.PIPE,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(release, child)
        return child

    def retain_evidence(self):
        folder = os.environ.get("STALE_CLEANUP_EVIDENCE")
        if folder:
            target = Path(folder).absolute()
            target.mkdir(parents=True, exist_ok=True)
            atomic_write_json(target / (self.id().rsplit(".", 1)[-1] + ".json"),
                              {"fixture": str(self.base), "commands": self.commands})

    def public(self, *args: str) -> dict:
        env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
               "PYTHONDONTWRITEBYTECODE": "1"}
        completed = subprocess.run([sys.executable, "-B", "-m", "orchestrator_harness.operator_launch",
                                    "--json", *args], cwd=self.base, env=env, capture_output=True,
                                   text=True, timeout=180)
        self.commands.append({"argv": list(args), "exit_code": completed.returncode,
                              "stdout": completed.stdout, "stderr": completed.stderr,
                              "runtime_state": (setup.read_runtime_state(self.rt) or {}).get("state")})
        result = json.loads(completed.stdout)
        self.assertEqual(0 if result["ok"] else 1, completed.returncode, completed.stderr)
        return result

    def native_run(self, epoch: str, lane: dict, *, proven: bool = True, root: dict | None = None):
        """Exact finite owners; an unproven run keeps its helper live in the boundary.

        ``root`` replaces the recorded provider root identity with an exited
        incarnation whose numeric PID the OS has since given to another process.
        """
        if not lane.get("provider_history_path"):
            retry_policy.initialize_history(self.rt, lane)
        controller = self.waiter()
        controller_identity = processes.process_identity(controller.pid)
        self.assertIsNotNone(controller_identity)
        marker = self.base / f"helper-{lane['run_id']}.pid"
        token = retry_policy.reserve_invocation(self.rt, lane)
        with open(os.devnull, "rb") as stdin, open(os.devnull, "wb") as out, open(os.devnull, "wb") as err:
            child = processes.spawn_provider([sys.executable, "-B", "-c", PROVIDER, str(marker)],
                                            cwd=self.worker, stdin=stdin, stdout=out, stderr=err)
        try:
            job = child.take_job_handle() if os.name == "nt" else None
            boundary = processes.ProcessBoundary.for_process(child.pid, windows_job_handle=job)
            self.addCleanup(boundary.cleanup, force=True, timeout_seconds=10)
            self.assertTrue(boundary.observe())
            retry_policy.record_started(self.rt, lane, token, process_boundary=self.rooted(boundary.record(), root))
            if os.name == "nt":
                child.resume()
            self.assertEqual(0, child.wait(timeout=30))
        finally:
            if os.name == "nt":
                child.close()
        helper = int(marker.read_text(encoding="utf-8"))
        self.assertTrue(boundary.observe())
        record = boundary.record()
        self.assertIn(helper, [member["pid"] for member in record["processes"]])
        if proven:
            self.assertTrue(boundary.cleanup(force=True, timeout_seconds=10))
        record = self.rooted(record, root)
        # The unproven branch is the controller's terminal cleanup-unproven record.
        retry_policy.record_outcome(self.rt, lane, token, provider_started=True,
                                    cleanup_proven=proven, session_id=SESSION)
        status = {"schema": "controller-status/v1", "lane_id": lane["lane_id"], "run_id": lane["run_id"],
                  "controller_state": "exited" if proven else "running",
                  "controller_identity": controller_identity,
                  "provider_state": {"state": "exited", "exit_code": 0, **record["root"]},
                  "process_boundary": record, "cleanup_proven": proven}
        if not proven:
            status["cleanup_error"] = "provider/helper process boundary remains unknown or live"
        atomic_write_json(Path(lane["controller_status_path"]), status)
        lane.update(process=controller_identity, session={"session_id": SESSION}, launch_pending=False,
                    lifecycle="review_pending" if proven else "running")
        write_lane(self.rt, epoch, lane["lane_id"], lane)
        release(controller)
        self.assertTrue(processes.identity_exit_proven(controller_identity["pid"], controller_identity["creation_time"]))
        return lane, boundary, record

    @staticmethod
    def rooted(record: dict, root: dict | None) -> dict:
        return record if root is None else {**record, "root": root, "processes": [root, *record["processes"]]}

    def reused_root_run(self) -> dict:
        """Workflow-1 r01: the exited Job root's PID belongs to an unrelated live process."""
        ticks = int(self.sentinel_identity["creation_time"].split(":", 1)[1])
        root = {"pid": self.sentinel.pid, "creation_time": f"windows-filetime:{ticks - 36_000_000_000}"}
        lane, _, record = self.native_run(self.epoch, self.lane, root=root)
        strict = processes.ProcessBoundary.from_record(record)
        # The r01 harness proved the serialized Job only through this global observation.
        self.assertFalse(strict.observe())
        self.assertIn(f"root PID {self.sentinel.pid} creation identity was reused", strict.errors)
        for member in record["processes"]:
            self.assertTrue(processes.identity_exit_proven(member["pid"], member["creation_time"]))
        self.assertTrue(lane_cleanup.cleanup_is_proven(lane))
        return lane

    @unittest.skipUnless(os.name == "nt", "the serialized provider Job record is Windows-specific")
    def test_reused_provider_root_pid_does_not_trap_shutdown(self):
        self.reused_root_run()
        closed = self.public("harness", "shutdown")
        self.assertTrue(closed["ok"], closed)
        self.assert_closed()
        self.assert_sentinel_survived()

    @unittest.skipUnless(os.name == "nt", "the serialized provider Job record is Windows-specific")
    def test_force_stop_proves_reused_provider_root_pid_exited(self):
        lane = self.reused_root_run()
        stopped = self.public("lane", "force-stop", "--lane-id", "worker")
        self.assertTrue(stopped["ok"], stopped)
        self.assert_sentinel_survived()
        retired = read_lane(self.rt, self.epoch, "worker")
        receipt = json.loads(Path(retired["forced_cleanup_ref"]["path"]).read_bytes())
        self.assertEqual("controller_created", receipt["observations"]["kind"])
        self.assertEqual(lane["process"], receipt["controller"])
        self.assertEqual(self.sentinel.pid, receipt["provider"]["pid"])
        closed = self.public("harness", "shutdown")
        self.assertTrue(closed["ok"], closed)
        self.assert_closed()
        self.assert_sentinel_survived()

    def assert_closed(self):
        self.assertEqual("CLOSED", setup.read_runtime_state(self.rt)["state"])
        shutdown.prove_runtime_closed(self.rt, self.root, self.base)
        again = self.public("harness", "shutdown")
        self.assertTrue(again["ok"], again)
        self.assertEqual("runtime is already CLOSED", again["summary"])

    def assert_sentinel_survived(self):
        self.assertTrue(processes.identity_matches(self.sentinel.pid, self.sentinel_identity["creation_time"]))

    def test_stale_negative_status_refuses_then_force_stop_reaches_closed(self):
        lane, boundary, record = self.native_run(self.epoch, self.lane, proven=False)
        live = self.public("harness", "shutdown")
        self.assertEqual(shutdown.SHUTDOWN_LANE_CLEANUP_UNPROVEN, live["code"], live)
        self.assertEqual("SHUTTING_DOWN", setup.read_runtime_state(self.rt)["state"])
        self.assertFalse(lane_cleanup.cleanup_is_proven(lane))
        # Closing the provider Job stands in for the exited controller's kill-on-close.
        self.assertTrue(boundary.cleanup(force=True, timeout_seconds=10))
        for member in record["processes"]:
            self.assertTrue(processes.identity_exit_proven(member["pid"], member["creation_time"]))
        folder = lane_record_dir(self.rt, self.epoch, "worker")
        before = {path: path.read_bytes() for path in folder.rglob("*.json")}
        for _ in range(2):
            stale = self.public("harness", "shutdown")
            self.assertEqual(shutdown.SHUTDOWN_LANE_CLEANUP_UNPROVEN, stale["code"], stale)
            self.assertEqual("lane worker cleanup is unproven; force-stop the lane", stale["summary"])
            self.assertIn("lane force-stop", stale["next_action"])
            self.assertEqual("SHUTTING_DOWN", setup.read_runtime_state(self.rt)["state"])
        self.assertEqual(before, {path: path.read_bytes() for path in folder.rglob("*.json")})
        stopped = self.public("lane", "force-stop", "--lane-id", "worker")
        self.assertTrue(stopped["ok"], stopped)
        retired = read_lane(self.rt, self.epoch, "worker")
        self.assertEqual("retired", retired["lifecycle"])
        receipt = json.loads(Path(retired["forced_cleanup_ref"]["path"]).read_bytes())
        self.assertEqual("controller_created", receipt["observations"]["kind"])
        self.assertEqual(lane["process"], receipt["controller"])
        self.assertEqual(record, receipt["boundary"])
        closed = self.public("harness", "shutdown")
        self.assertTrue(closed["ok"], closed)
        self.assert_closed()
        self.assertTrue(lane_cleanup.cleanup_is_proven(retired))
        self.assert_sentinel_survived()

    def force_stop_close_and_retry(self, *, pending: bool):
        """Force-stop, close, then spend the saved-session retry in a later epoch."""
        self.native_run(self.epoch, self.lane)
        if pending:
            self.assertTrue(self.public("resume-lane", "--lane-id", "worker",
                                        "--resume-task-card", str(self.card_path))["ok"])
        stopped = self.public("lane", "force-stop", "--lane-id", "worker")
        self.assertTrue(stopped["ok"], stopped)
        old = read_lane(self.rt, self.epoch, "worker")
        receipt_path = Path(old["forced_cleanup_ref"]["path"])
        receipt = receipt_path.read_bytes()
        self.assertEqual("never_launched" if pending else "controller_created",
                         json.loads(receipt)["observations"]["kind"])
        closed = self.public("harness", "shutdown")
        self.assertTrue(closed["ok"], closed)
        # Native setup reopens only after this same CLOSED proof.
        shutdown.prove_runtime_closed(self.rt, self.root, self.base)
        setup.set_runtime_state(self.rt, "OPEN")
        self.assertTrue(self.public("resume-lane", "--lane-id", "worker",
                                    "--resume-task-card", str(self.card_path))["ok"])
        epoch = read_current_epoch(self.rt)["epoch_id"]
        self.assertNotEqual(self.epoch, epoch)
        fresh, _, _ = self.native_run(epoch, read_lane(self.rt, epoch, "worker"))
        self.assertEqual(2, retry_policy.completed_provider_invocations(fresh, self.rt))
        result = self.public("harness", "shutdown")
        self.assertTrue(result["ok"], result)
        self.assert_closed()
        self.assertEqual(receipt, receipt_path.read_bytes())
        self.assertTrue(lane_cleanup.cleanup_is_proven(read_lane(self.rt, self.epoch, "worker")))
        self.assert_sentinel_survived()
        return old, Path(fresh["provider_history_path"])

    def test_pending_force_stop_receipt_survives_later_saved_session_retry(self):
        self.force_stop_close_and_retry(pending=True)

    def test_forced_receipt_survives_retry_but_not_rewritten_or_replaced_history(self):
        old, journal = self.force_stop_close_and_retry(pending=False)
        original = journal.read_bytes()

        def in_place(data: bytes) -> None:  # Keeps the native file identity.
            with journal.open("r+b") as stream:
                stream.write(data)
                stream.truncate()

        rows = original.decode("utf-8").splitlines(keepends=True)
        stamp = json.loads(rows[1])["recorded_at"]
        in_place(original.replace(stamp.encode(), b"2000-01-01T00:00:00Z", 1))
        self.assertFalse(lane_cleanup.cleanup_is_proven(old))
        refused = self.public("harness", "shutdown")
        self.assertEqual(shutdown.SHUTDOWN_LANE_CLEANUP_UNPROVEN, refused["code"], refused)
        self.assertEqual("retained lane cleanup is unproven", refused["summary"])
        in_place(original)
        self.assertTrue(self.public("harness", "shutdown")["ok"])
        # The reader deduplicates a repeated fact; the receipt still refuses it.
        forced = [row for row in rows[1:] if json.loads(row)["run_id"] == old["run_id"]]
        in_place(original + forced[-1].encode("utf-8"))
        self.assertEqual(2, retry_policy.completed_provider_invocations(old, self.rt))
        self.assertFalse(lane_cleanup.cleanup_is_proven(old))
        in_place(original)
        controller = old["process"]
        with patch.object(processes, "identity_exit_proven",
                          side_effect=lambda pid, creation: pid != controller["pid"]):
            self.assertFalse(lane_cleanup.cleanup_is_proven(old))
        self.assertTrue(lane_cleanup.cleanup_is_proven(old))
        journal.unlink()
        journal.write_bytes(original)
        self.assertFalse(lane_cleanup.cleanup_is_proven(old))
        refused = self.public("harness", "shutdown")
        self.assertEqual(shutdown.SHUTDOWN_LANE_CLEANUP_UNPROVEN, refused["code"], refused)
        self.assert_sentinel_survived()


if __name__ == "__main__":
    unittest.main()
