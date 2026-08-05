"""Adversarial, host-only tests for C3 identity watcher containment."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from firmware_acceptance.c3_watcher_oracle import classify


class C3WatcherOracleTests(unittest.TestCase):
    def _put(self, root: Path, name: str, value: object) -> Path:
        path = root / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def _evidence(self, root: Path, *, request_id: str = "a1",
                  reason: str = "controller did not publish an authentic initial status",
                  command: str | None = None) -> tuple[Path, Path, Path]:
        invocation = root / "a1.invocation.json"
        invocation.write_text("{}", encoding="utf-8")
        digest = hashlib.sha256(invocation.read_bytes()).hexdigest()
        status = self._put(root, "status.json", {"schema":"orchestrator-lane-controller/v1", "state":"WAITING_RESOURCE", "controller_pid":4242, "controller_created_utc":"created"})
        status_digest = hashlib.sha256(status.read_bytes()).hexdigest()
        controller_command = command or f'python -m orchestrator_harness.lane_controller "{invocation}"'
        identity = {"pid":4242, "created_utc":"created"}
        snapshot = {"pid":4242, "ppid":1, "name":"python", "command_line":controller_command, "created_utc":"created"}
        relationship = self._put(root, "relationship.json", {"schema":"firmware-c3-controller-relationship/v1", "assignment_id":"a1", "shape":"same-process", "launcher_identity":identity, "controller_identity":identity, "controller_status_identity":identity, "launcher_snapshot":snapshot, "controller_snapshot":snapshot, "invocation":{"path":str(invocation.resolve()), "sha256":digest}, "initial_status":{"path":str(status.resolve()), "sha256":status_digest, "state":"WAITING_RESOURCE", "controller_identity":identity}})
        response = self._put(root, "response.json", {"schema":"firmware-c3-harness-response/v1", "request_id":request_id, "outcome":"REJECTED", "reason":reason})
        return response, status, relationship

    def test_s26_a1_watcher_aborts_only_for_bound_raw_or_recovery_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for reason in ("controller did not publish an authentic initial status", "pre-registration cleanup requires recovery: controller did not publish an authentic initial status"):
                with self.subTest(reason=reason):
                    response, status, relationship = self._evidence(root, reason=reason)
                    result = classify(response, status, relationship)
                    self.assertEqual("ABORT_REQUIRED", result["classification"])
                    self.assertEqual({"schema", "classification", "invariant", "response", "status", "relationship", "evidence"}, set(result))

    def test_s26_a1_watcher_contains_mutable_stale_unrelated_and_self_correlated_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cases = {
                "unrelated-reason": {"reason":"some other rejection"},
                "stale-status": {"status":{"controller_created_utc":"reused"}},
                "mutable-status": {"mutate_status":True},
                "unrelated-request": {"request_id":"other-assignment"},
                "command-token-collision": {"command":"python -m orchestrator_harness.lane_controller_extra a1.invocation.json.bak"},
            }
            for name, case in cases.items():
                with self.subTest(name=name):
                    response, status, relationship = self._evidence(root, **{key:value for key, value in case.items() if key in {"request_id", "reason", "command"}})
                    if "status" in case:
                        status.write_text(json.dumps({"schema":"orchestrator-lane-controller/v1", "state":"WAITING_RESOURCE", "controller_pid":4242, **case["status"]}), encoding="utf-8")
                    if case.get("mutate_status"):
                        status.write_text(json.dumps({"schema":"orchestrator-lane-controller/v1", "state":"RUNNING_CODEX", "controller_pid":4242, "controller_created_utc":"created"}), encoding="utf-8")
                    self.assertEqual("EXPECTED_CONTAINMENT", classify(response, status, relationship)["classification"])
