"""Adversarial, host-only tests for C3 immutable-observation containment."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from firmware_acceptance.c3_watcher_oracle import classify


class C3WatcherOracleTests(unittest.TestCase):
    def _put(self, root: Path, name: str, value: object) -> Path:
        path = root / name; path.write_text(json.dumps(value), encoding="utf-8"); return path

    def _evidence(self, root: Path, *, response_id: str = "request-1", request_assignment: str = "assignment-1", observation_assignment: str = "assignment-1", outcome: str = "REJECTED", reason: str = "controller did not publish an authentic initial status", recovery_assignment: str | None = None) -> tuple[Path, Path, Path, Path]:
        invocation = root / "assignment-1.invocation.json"; invocation.write_text("{}", encoding="utf-8")
        current = self._put(root, "current-status.json", {"schema":"orchestrator-lane-controller/v1", "state":"WAITING_RESOURCE", "controller_pid":4242, "controller_created_utc":"2026-08-05T00:00:00Z"})
        initial = {"schema":"orchestrator-lane-controller/v1", "state":"WAITING_RESOURCE", "controller_pid":4242, "controller_created_utc":"2026-08-05T00:00:00Z"}
        initial_bytes = json.dumps(initial).encode(); initial_hash = hashlib.sha256(initial_bytes).hexdigest()
        identity = {"pid":4242,"created_utc":"windows-filetime:133000000000000000"}
        snapshot = {"pid":4242,"ppid":1,"name":"python","command_line":"lossy posix command line is irrelevant for same-process","created_utc":"2026-08-05T00:00:00Z"}
        observation = self._put(root, "observation.json", {"schema":"firmware-c3-controller-observation/v1", "assignment_id":observation_assignment, "shape":"same-process", "launcher_identity":identity, "launcher_observed_identity":identity, "controller_identity":identity, "controller_status_identity":{"pid":4242,"created_utc":"2026-08-05T00:00:00Z"}, "launcher_snapshot":snapshot, "controller_snapshot":snapshot, "invocation":{"path":str(invocation.resolve()),"sha256":hashlib.sha256(invocation.read_bytes()).hexdigest()}, "initial_status":{"sha256":initial_hash,"value":initial}, "status_source":{"path":str((root / "initial-status.json").resolve()),"sha256":initial_hash}})
        request = self._put(root, "request.json", {"schema":"firmware-c3-harness-request/v1","request_id":"request-1","attempt_id":"attempt","c1_reference":{},"delegated_reference":{},"orchestrator_identity":{},"topology_key_release":{},"kind":"assignment","issued_utc":"2026-08-05T00:00:00Z","issued_monotonic":0,"expires_monotonic":1,"payload":{"assignment_id":request_assignment},"public_key":"key","signature":"sig"})
        response: dict[str, object] = {"schema":"firmware-c3-harness-response/v1","request_id":response_id,"outcome":outcome,"reason":reason}
        if outcome == "RECOVERY_REQUIRED":
            assignment = recovery_assignment if recovery_assignment is not None else request_assignment
            cleanup = {"schema":"firmware-c3-prestart-rejection/v1","assignment_id":assignment,"reason":"x","launcher_identity":None,"controller_identity":None,"handles_closed":True,"launcher_reaped":False,"controller_reaped":True,"process_reaped":False,"worktree_removed":False,"branch_removed":False,"channels_removed":False,"outcome":"RECOVERY_REQUIRED"}
            response["recovery"] = {"schema":"firmware-c3-recovery-required/v1","assignment_id":assignment,"launcher_pid":4242,"launcher_identity":None,"observed_launcher_identity":None,"controller_identity":None,"worktree":"w","branch":"b","channels":[],"cleanup":cleanup,"path":"p","sha256":"0" * 64}
        return request, self._put(root, "response.json", response), observation, current

    def test_s26_a1_oracle_aborts_for_correlated_raw_and_recovery_rejections_after_status_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cases = (("REJECTED", "controller did not publish an authentic initial status"), ("REJECTED", "pre-registration cleanup requires recovery: controller did not publish an authentic initial status"), ("RECOVERY_REQUIRED", "pre-registration cleanup requires recovery: controller did not publish an authentic initial status"))
            for outcome, reason in cases:
                with self.subTest(outcome=outcome):
                    request, response, observation, current = self._evidence(root, outcome=outcome, reason=reason)
                    current.write_text(json.dumps({"schema":"orchestrator-lane-controller/v1","state":"CODEX_EXITED","controller_pid":7,"controller_created_utc":"reused"}), encoding="utf-8")
                    result = classify(request, response, observation, current)
                    self.assertEqual("ABORT_REQUIRED", result["classification"])
                    self.assertEqual({"schema","classification","invariant","request","response","observation","status","evidence"}, set(result))

    def test_s26_a1_oracle_contains_unrelated_or_mutated_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cases = {
                "response-request-mismatch": {"response_id":"other-request"},
                "request-observation-mismatch": {"request_assignment":"other-assignment"},
                "recovery-assignment-mismatch": {"outcome":"RECOVERY_REQUIRED", "reason":"pre-registration cleanup requires recovery: controller did not publish an authentic initial status", "recovery_assignment":"other-assignment"},
                "unrelated-reason": {"reason":"other rejection"},
            }
            for name, kwargs in cases.items():
                with self.subTest(name=name):
                    request, response, observation, current = self._evidence(root, **kwargs)
                    self.assertEqual("EXPECTED_CONTAINMENT", classify(request, response, observation, current)["classification"])
            request, response, observation, current = self._evidence(root)
            invocation = root / "assignment-1.invocation.json"; invocation.write_text("changed", encoding="utf-8")
            self.assertEqual("EXPECTED_CONTAINMENT", classify(request, response, observation, current)["classification"])
            request, response, observation, current = self._evidence(root)
            value = json.loads(observation.read_text()); value["initial_status"]["value"]["controller_pid"] = True; observation.write_text(json.dumps(value), encoding="utf-8")
            self.assertEqual("EXPECTED_CONTAINMENT", classify(request, response, observation, current)["classification"])
