"""Closed, host-only oracle regressions for the C3 initial-status invariant."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from firmware_acceptance.c3_watcher_oracle import classify


class C3WatcherOracleTests(unittest.TestCase):
    PID = 4242
    CREATED = "2026-08-05T00:00:00Z"

    def _put(self, root: Path, name: str, value: object) -> Path:
        path = root / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def _response(self) -> dict[str, object]:
        return {"schema":"firmware-c3-harness-response/v1", "request_id":"r1", "outcome":"REJECTED", "reason":"controller did not publish an authentic initial status"}

    def _status(self) -> dict[str, object]:
        return {"schema":"orchestrator-lane-controller/v1", "controller_pid":self.PID, "controller_created_utc":self.CREATED, "state":"WAITING_RESOURCE", "held_resource_claims":[]}

    def test_s25_a1_oracle_closed_authentic_contradiction_and_hash_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); response = self._put(root, "response.json", self._response()); status = self._put(root, "status.json", self._status())
            result = classify(response, status, expected_pid=self.PID, expected_created_utc=self.CREATED)
            self.assertEqual("ABORT_REQUIRED", result["classification"])
            self.assertEqual({"schema","classification","invariant","expected_controller_identity","response","status","evidence"}, set(result))
            self.assertEqual({"path":str(response.resolve()),"sha256":hashlib.sha256(response.read_bytes()).hexdigest()}, result["response"])
            self.assertTrue(result["evidence"]["candidate_rejected_unauthentic_status"])
            self.assertTrue(result["evidence"]["status_authentic_for_expected_live_controller"])
            self._put(root, "running.json", {**self._status(), "state":"RUNNING_CODEX"})
            self.assertEqual("ABORT_REQUIRED", classify(response, root / "running.json", expected_pid=self.PID, expected_created_utc=self.CREATED)["classification"])

    def test_s25_a1_oracle_contains_malformed_wrong_identity_and_noncontradictions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); response = self._put(root, "response.json", self._response()); status = self._put(root, "status.json", self._status())
            cases = [
                ("malformed-response", "{", self._status()),
                ("scalar-response", [], self._status()),
                ("candidate-accepted", {**self._response(), "outcome":"ACCEPTED"}, self._status()),
                ("wrong-pid", self._response(), {**self._status(), "controller_pid":1}),
                ("wrong-created", self._response(), {**self._status(), "controller_created_utc":"other"}),
                ("unsupported-state", self._response(), {**self._status(), "state":"CODEX_EXITED"}),
                ("unauthentic-rejected", {**self._response(), "reason":"other rejection"}, self._status()),
            ]
            for name, response_value, status_value in cases:
                with self.subTest(name=name):
                    if isinstance(response_value, str): (root / "response.json").write_text(response_value, encoding="utf-8")
                    else: response = self._put(root, "response.json", response_value)
                    status = self._put(root, "status.json", status_value)
                    result = classify(response, status, expected_pid=self.PID, expected_created_utc=self.CREATED)
                    self.assertEqual("EXPECTED_CONTAINMENT", result["classification"])
                    self.assertFalse(result["evidence"]["candidate_rejected_unauthentic_status"] and result["evidence"]["status_authentic_for_expected_live_controller"])

