"""Host-only regression coverage for the candidate C3 control plane.

The suite deliberately fakes only process/controller boundaries.  Its Git targets,
seed trees, request artifacts, and C3 state are real temporary filesystem objects.
"""
from __future__ import annotations

import hashlib
import json
import tempfile
import time
import unittest
from concurrent.futures import Future
from pathlib import Path
from unittest.mock import Mock, patch

import firmware_acceptance.c3_harness as c3
from firmware_acceptance.kit import AcceptanceBroker, AdmissionError, worker_environment


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class _Process:
    pid = 4242

    def __init__(self, code: int | None = None) -> None:
        self.code = code
        self.waited = False

    def poll(self) -> int | None: return self.code
    def wait(self, timeout: float | None = None) -> int: self.waited = True; return self.code or 0
    def terminate(self) -> None: self.code = -15
    def kill(self) -> None: self.code = -9


class C3HarnessTests(unittest.TestCase):
    def _bare(self, root: Path) -> c3.C3Harness:
        harness = object.__new__(c3.C3Harness)
        harness.root = root
        harness.seed = Path("firmware_acceptance/seed").resolve()
        harness.policy = Path("firmware_acceptance/MCP_METHOD_POLICY.json").resolve()
        harness.templates = Path("firmware_acceptance/LANE_TEMPLATES.json").resolve()
        harness.seed_identity = c3._seed_snapshot(harness.seed)
        harness.broker = AcceptanceBroker(root, harness.seed, harness.policy, harness.templates, Path("firmware_acceptance/ACCEPTANCE_MANIFEST.json"))
        harness.verifier = Mock(); harness.controllers = {}; harness.workers = {}; harness.assignments = {}
        harness.limitation_completed = {}; harness.operations = {}; harness.operation_pending = {}
        harness.executor = Mock(); harness.shutdown = False; harness.admission_closed = False
        harness.topology = {"attempt_id":"attempt-1", "release":{"path":"release","sha256":"r"}, "identity_binding":{"path":"identity","sha256":"i"}}
        harness.request_root = root / "manager-signals" / "c3-requests"; harness.response_root = root / "manager-signals" / "c3-responses"
        harness.admission_root = root / "manager-signals" / "c3-admissions"; harness.state_root = root / "c3-harness"
        for path in (harness.request_root, harness.response_root, harness.admission_root, harness.state_root): path.mkdir(parents=True, exist_ok=True)
        harness.status_path = harness.state_root / "STATUS.jsonl"; harness.registry_path = harness.state_root / "REGISTRY.jsonl"
        return harness

    def test_c3_cp_01_02_startup_request_binding_seed_and_replay_barrier(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root)
            self.assertEqual(5, len(harness.seed_identity))
            request = {"schema":"firmware-c3-harness-request/v1", "request_id":"r1", "attempt_id":"attempt-1", "c1_reference":{"path":"c1","sha256":"c"}, "delegated_reference":{"path":"delegated","sha256":"d"}, "orchestrator_identity":harness.topology["identity_binding"], "topology_key_release":harness.topology["release"], "kind":"materialize", "issued_utc":"2026-08-04T00:00:00+00:00", "issued_monotonic":time.monotonic()-1, "expires_monotonic":time.monotonic()+60, "payload":{"target_id":"target"}, "public_key":"key", "signature":"sig"}
            path = harness.request_root / "r1.json"; path.write_text(json.dumps(request), encoding="utf-8")
            with patch.object(c3, "_ref", side_effect=lambda value, label, expected=None: value), patch.object(harness.verifier, "verify", return_value=True):
                self.assertEqual("r1", harness._load(path)["request_id"])
            request["kind"] = "wrong"; path.write_text(json.dumps(request), encoding="utf-8")
            with patch.object(harness.verifier, "verify", return_value=False):
                self.assertEqual("REJECTED", harness.handle(path)["outcome"])
            claim = harness.admission_root / "r1.json"; claim.write_text(json.dumps({"request_sha256":"different"}), encoding="utf-8")
            with patch.object(harness, "_load", return_value={**request, "request_id":"r1"}):
                self.assertEqual("REJECTED", harness.handle(path)["outcome"])

    def test_c3_cp_04_disposable_target_assignment_and_fail_closed_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root)
            target = root / "targets" / "target"; harness.broker.materialize_seed(target)
            original = {name: _digest(target / name) for name in harness.seed_identity}
            payload = {"assignment_id":"a1", "role":"F.C3.A1", "sprint":"S23", "task":"host test", "prompt":"do bounded work", "target_id":"target", "declared_resources":[]}
            with patch.object(c3.subprocess, "Popen", return_value=_Process()), patch.object(c3, "exact_process_identity", return_value={"pid":4242,"creation_identity":"created"}):
                launched = harness._assignment(payload)
            worktree = Path(harness.assignments["a1"]["worktree"])
            self.assertTrue((worktree / ".git").is_file())
            self.assertEqual(original, {name: _digest(worktree / name) for name in original})
            self.assertEqual("gpt-5.6-terra", harness.assignments["a1"]["invocation"]["codex"]["model"])
            self.assertEqual("priority", harness.assignments["a1"]["invocation"]["codex"]["service_tier"])
            self.assertIn("finding_gate", harness.assignments["a1"]["invocation"])
            self.assertEqual("LAUNCHED", launched["state"])
            harness.workers.clear(); harness.assignments["a1"]["completion"] = {"outcome":"FAIL"}
            with self.assertRaises(AdmissionError): harness._accept_assignment({"assignment_id":"a1", "target_id":"target"})

    def test_c3_cp_05_sessions_overlap_and_p1_channel_is_token_bound(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root)
            first, second = Mock(), Mock(); harness.controllers = {"s1":first, "s2":second}
            harness.executor.submit.side_effect = [Future(), Future()]
            for sid, controller in (("s1", first), ("s2", second)):
                result = harness._session({"session_id":sid,"proposal_path":"p","decision_path":"d","authorization_path":"a"}, "execute")
                self.assertEqual("PENDING", result["state"])
            with self.assertRaises(AdmissionError): harness._session({"session_id":"s1","proposal_path":"p","decision_path":"d","authorization_path":"a"}, "execute")
            pending = next((root / "sessions" / "s1" / "operations").glob("*.PENDING.json"))
            with self.assertRaises(AdmissionError): harness._recover_or_fail_closed()
            terminal = pending.with_name(pending.name.removesuffix(".PENDING.json") + ".TERMINAL.json")
            terminal.write_text(json.dumps({"pending":{"path":str(pending),"sha256":_digest(pending)}}), encoding="utf-8")
            harness._recover_or_fail_closed()
            inbox, responses = root / "inbox", root / "responses"; inbox.mkdir(); responses.mkdir()
            harness.workers = {"p1":_Process()}; harness.assignments = {"p1":{"invocation":{"lane_id":"F.C3.P1"},"inbox":inbox,"responses":responses,"token_hash":hashlib.sha256(b"token").hexdigest()}}
            (inbox / "bad.json").write_text(json.dumps({"endpoint":"forbidden"}), encoding="utf-8"); harness.service_worker_channels()
            self.assertEqual("REJECTED", json.loads((responses / "bad.json").read_text())["outcome"])

    def test_c3_cp_06_limitation_shapes_roles_and_broker_gate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            harness = self._bare(Path(temporary))
            diagnostic = {"mode":"diagnostic","limitation_id":"L1","attempt_id":"attempt-1","lane_id":"STM-A","session_id":"s1","raw_result":{"path":"x","sha256":"x"},"source_path":"README.md"}
            shaped = harness._limitation_metadata(diagnostic, "F.C3.A1")
            self.assertEqual("F.C3.A1", shaped["worker_role"])
            with self.assertRaises(AdmissionError): harness._limitation_metadata({**diagnostic,"endpoint":"bad"}, "F.C3.A1")
            self.assertEqual("", worker_environment()["MCP_ENDPOINT"]); self.assertEqual("", worker_environment()["MCP_COMMAND"])
            harness.limitation_completed = {"L1":{"diagnostic":{},"substitute":{}}}; harness.broker.record_server_limitation = Mock(return_value={"physical_certification":{"status":"NOT_CERTIFIED"}})
            decision = harness._server_limitation({"decision_path":"decision.json","limitation_id":"L1"})
            self.assertEqual("NOT_CERTIFIED", decision["physical_certification"]["status"])

    def test_c3_cp_07_shutdown_blocks_then_reaps_and_refuses_unreconciled_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); harness.workers = {"a1":_Process(None)}
            self.assertEqual("BLOCKED", harness._shutdown({})["state"]); self.assertTrue(harness.admission_closed)
            harness.workers = {}; controller = Mock(); controller.abort_session.return_value = {"exact_reaped":True,"claim_released":True}; harness.controllers = {"s1":controller}
            result = harness._shutdown({})
            self.assertEqual("SHUTDOWN", result["state"]); self.assertTrue(Path(result["shutdown_path"]).is_file()); harness.executor.shutdown.assert_called_once()
            harness.registry_path.write_text(json.dumps({"schema":"firmware-c3-worker-lifecycle/v1","state":"STARTED","assignment_id":"lost"}) + "\n", encoding="utf-8")
            with self.assertRaises(AdmissionError): harness._recover_or_fail_closed()
