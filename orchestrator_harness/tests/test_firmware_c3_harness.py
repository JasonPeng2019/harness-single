"""Host-only regressions for the candidate-owned C3 control-plane facade."""
from __future__ import annotations

import hashlib
import json
import stat
import subprocess
import tempfile
import time
import unittest
from concurrent.futures import Future
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import firmware_acceptance.c3_harness as c3
from firmware_acceptance.kit import AcceptanceBroker, AdmissionError, worker_environment
from orchestrator_harness.models import ProcessInfo, ProcessSnapshot, iso_utc


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class _Process:
    pid = 4242

    def __init__(self, code: int | None = None) -> None: self.code = code; self.terminated = False; self.waited = False
    def poll(self) -> int | None: return self.code
    def wait(self, timeout: float | None = None) -> int: self.waited = True; return self.code or 0
    def terminate(self) -> None: self.terminated = True; self.code = -15
    def kill(self) -> None: self.code = -9


class C3HarnessTests(unittest.TestCase):
    def _bare(self, root: Path) -> c3.C3Harness:
        harness = object.__new__(c3.C3Harness)
        harness.root = root; harness.seed = Path("firmware_acceptance/seed").resolve()
        harness.candidate_root = Path(__file__).resolve().parents[2]
        harness.policy = Path("firmware_acceptance/MCP_METHOD_POLICY.json").resolve(); harness.templates = Path("firmware_acceptance/LANE_TEMPLATES.json").resolve()
        harness.seed_identity = c3._seed_snapshot(harness.seed)
        harness.broker = AcceptanceBroker(root, harness.seed, harness.policy, harness.templates, Path("firmware_acceptance/ACCEPTANCE_MANIFEST.json"))
        harness.verifier = Mock(); harness.controllers = {}; harness.workers = {}; harness.assignments = {}; harness.limitation_completed = {}
        harness.operations = {}; harness.operation_pending = {}; harness.executor = Mock(); harness.shutdown = False; harness.admission_closed = False
        harness.topology = {"attempt_id":"attempt-1", "release":{"path":"release","sha256":"r"}, "identity_binding":{"path":"identity","sha256":"i"}}
        harness.request_root = root / "manager-signals" / "c3-requests"; harness.response_root = root / "manager-signals" / "c3-responses"; harness.admission_root = root / "manager-signals" / "c3-admissions"; harness.state_root = root / "c3-harness"
        for path in (harness.request_root, harness.response_root, harness.admission_root, harness.state_root): path.mkdir(parents=True, exist_ok=True)
        harness.status_path = harness.state_root / "STATUS.jsonl"; harness.registry_path = harness.state_root / "REGISTRY.jsonl"
        return harness

    def test_c3_cp_01_02_startup_request_binding_seed_and_replay_barrier(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root)
            expected_names = {"TARGET_SEED_MANIFEST.json", "TARGET_CHARTER.md", "PINNED_INPUTS.json", "TEST_CONTRACT.json", "EVIDENCE_SCHEMA.json"}
            self.assertEqual(expected_names, set(harness.seed_identity)); self.assertTrue(all(isinstance(value, str) and len(value) == 64 for value in harness.seed_identity.values()))
            with patch.object(c3, "exact_process_identity", return_value={"pid":1,"creation_identity":"created"}):
                harness.write_readiness()
            readiness = json.loads((harness.state_root / "C3_HARNESS_READY.json").read_text(encoding="utf-8"))
            self.assertEqual(c3._seed_digest(harness.seed_identity), readiness["bindings"]["seed"])
            request = {"schema":"firmware-c3-harness-request/v1", "request_id":"r1", "attempt_id":"attempt-1", "c1_reference":{"path":"c1","sha256":"c"}, "delegated_reference":{"path":"delegated","sha256":"d"}, "orchestrator_identity":harness.topology["identity_binding"], "topology_key_release":harness.topology["release"], "kind":"materialize", "issued_utc":"2026-08-04T00:00:00+00:00", "issued_monotonic":time.monotonic()-1, "expires_monotonic":time.monotonic()+60, "payload":{"target_id":"target"}, "public_key":"key", "signature":"sig"}
            path = harness.request_root / "r1.json"; path.write_text(json.dumps(request), encoding="utf-8")
            with patch.object(c3, "_ref", side_effect=lambda value, label, expected=None: value), patch.object(harness.verifier, "verify", return_value=True): self.assertEqual("r1", harness._load(path)["request_id"])
            request["payload"] = []; path.write_text(json.dumps(request), encoding="utf-8")
            self.assertEqual("REJECTED", harness.handle(path)["outcome"])
            request["payload"] = {"target_id":"target"}; path.write_text(json.dumps(request), encoding="utf-8")
            side_effect = Mock(return_value={"target":"target"})
            with patch.object(harness, "_load", return_value=request), patch.object(harness, "_dispatch", side_effect=side_effect):
                self.assertEqual("ACCEPTED", harness.handle(path)["outcome"]); self.assertEqual("ACCEPTED", harness.handle(path)["outcome"])
            self.assertEqual(1, side_effect.call_count)
            request["signature"] = "tampered"; path.write_text(json.dumps(request), encoding="utf-8")
            with patch.object(harness.verifier, "verify", return_value=False): self.assertEqual("REJECTED", harness.handle(path)["outcome"])

    def test_c3_cp_04_disposable_target_assignment_and_fail_closed_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); target = root / "targets" / "target"; harness.broker.materialize_seed(target)
            payload = {"assignment_id":"a1", "role":"F.C3.A1", "sprint":"S23", "task":"host test", "prompt":"do bounded work", "target_id":"target", "declared_resources":[]}; process = _Process(); launches: list[tuple[list[str], Path]] = []
            created = datetime(2026, 8, 4, tzinfo=timezone.utc); snapshot = ProcessSnapshot(True, (ProcessInfo(process.pid, 1, "python", "lane controller", created),), (), "synthetic")
            def launch(command: list[str], *, cwd: Path, **_: object) -> _Process:
                invocation = json.loads(Path(command[-1]).read_text(encoding="utf-8")); status = Path(invocation["output_paths"]["status"])
                status.write_text(json.dumps({"schema":"orchestrator-lane-controller/v1","controller_pid":process.pid,"controller_created_utc":iso_utc(created)}), encoding="utf-8")
                launches.append((command, cwd)); return process
            try:
                identity = {"pid":4242,"creation_identity":"created"}
                observations = Mock(side_effect=[identity, identity])
                controller_subprocess = SimpleNamespace(Popen=launch, run=subprocess.run, TimeoutExpired=subprocess.TimeoutExpired, SubprocessError=subprocess.SubprocessError)
                with patch.object(c3, "subprocess", controller_subprocess), patch.object(c3, "process_snapshot", return_value=snapshot), patch.object(c3, "exact_process_identity", observations): launched = harness._assignment(payload)
                record = harness.assignments["a1"]; worktree = Path(record["worktree"])
                self.assertTrue((worktree / ".git").is_file()); self.assertEqual("LAUNCHED", launched["state"])
                self.assertEqual(harness.candidate_root, launches[0][1]); self.assertEqual(str(worktree), record["invocation"]["run_root"])
                self.assertEqual(2, observations.call_count)
                self.assertEqual({"pid":process.pid,"created_utc":iso_utc(created)}, record["status_identity"])
                self.assertEqual(harness.seed_identity, {name:_digest(worktree / name) for name in harness.seed_identity})
                self.assertTrue(all(not ((worktree / name).stat().st_mode & stat.S_IWRITE) for name in harness.seed_identity))
                changed = worktree / "TARGET_CHARTER.md"; changed.chmod(stat.S_IWRITE | stat.S_IREAD)
                with self.assertRaises(AdmissionError): c3._protected_seed_snapshot(worktree, harness.seed_identity)
                changed.chmod(stat.S_IREAD); c3._protected_seed_snapshot(worktree, harness.seed_identity)
                status_path = Path(record["invocation"]["output_paths"]["status"]); result_path = worktree / ".agent-workspace" / "RESULT.json"; tip = subprocess.run(["git","rev-parse","HEAD"], cwd=worktree, capture_output=True, text=True, check=True).stdout.strip()
                result_path.write_text(json.dumps({"schema":"orchestrator-lane-result/v1","lane_id":"F.C3.A1","worker_invocation_id":"a1","branch":record["branch"],"commit":tip,"outcome":"PASS","summary":"candidate passed","checks":[]}), encoding="utf-8")
                status_path.write_text(json.dumps({"state":"CODEX_EXITED","exit_code":0,"controller_pid":process.pid,"controller_created_utc":"2026-08-04T00:00:01Z","held_resource_claims":[],"result_valid":True,"result_validation":{"findings":{}},"schema":"orchestrator-lane-controller/v1"}), encoding="utf-8")
                process.code = 0; harness.reap_workers()
                self.assertEqual("FAIL", harness.assignments["a1"]["completion"]["outcome"])
                with self.assertRaises(AdmissionError): harness._accept_assignment({"assignment_id":"a1","target_id":"target"})
                rejected = _Process(); rejected_snapshot = ProcessSnapshot(True, (ProcessInfo(rejected.pid, 1, "python", "lane controller", created),), (), "synthetic")
                rejected_subprocess = SimpleNamespace(Popen=Mock(return_value=rejected), run=subprocess.run, TimeoutExpired=subprocess.TimeoutExpired, SubprocessError=subprocess.SubprocessError)
                with patch.object(c3, "subprocess", rejected_subprocess), patch.object(c3, "process_snapshot", return_value=rejected_snapshot), patch.object(c3, "exact_process_identity", side_effect=[{"pid":4242,"creation_identity":"before"}, {"pid":4242,"creation_identity":"after"}]):
                    with self.assertRaises(AdmissionError): harness._assignment({**payload,"assignment_id":"a2"})
                self.assertTrue(rejected.terminated); self.assertTrue(rejected.waited)
            finally:
                for assignment_id in ("a1", "a2"):
                    worktree = root / "assignment-worktrees" / assignment_id
                    if worktree.exists(): subprocess.run(["git","worktree","remove","--force",str(worktree)], cwd=target, check=True, capture_output=True, text=True)

    def test_c3_cp_05_sessions_overlap_and_p1_channel_is_token_bound(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); first, second = Future(), Future(); harness.controllers = {"s1":Mock(), "s2":Mock()}; harness.executor.submit.side_effect = [first, second]
            for sid in ("s1","s2"): self.assertEqual("PENDING", harness._session({"session_id":sid,"proposal_path":"p","decision_path":"d","authorization_path":"a"}, "execute")["state"])
            with self.assertRaises(AdmissionError): harness._session({"session_id":"s1","proposal_path":"p","decision_path":"d","authorization_path":"a"}, "execute")
            first.set_result({"call":"one"}); second.set_result({"call":"two"}); harness.reap_operations()
            for sid in ("s1","s2"):
                terminal = next((root / "sessions" / sid / "operations").glob("*.TERMINAL.json")); value = json.loads(terminal.read_text(encoding="utf-8")); self.assertEqual(_digest(Path(value["pending"]["path"])), value["pending"]["sha256"])
            harness._recover_or_fail_closed()
            pending = root / "sessions" / "lost" / "operations" / "x.PENDING.json"; pending.parent.mkdir(parents=True); pending.write_text("{}", encoding="utf-8")
            with self.assertRaises(AdmissionError): harness._recover_or_fail_closed()
            inbox, responses = root / "inbox", root / "responses"; inbox.mkdir(); responses.mkdir(); controller = Mock(); harness.controllers = {"s1":controller}; harness.workers = {"p1":_Process()}; harness.assignments = {"p1":{"invocation":{"lane_id":"F.C3.P1"},"inbox":inbox,"responses":responses,"token_hash":_digest_token("token")}}
            (inbox / "bad.json").write_text(json.dumps({"endpoint":"forbidden"}), encoding="utf-8")
            def publish(path: Path, call: dict[str, object]) -> dict[str, object]: path.parent.mkdir(parents=True, exist_ok=True); path.write_text(json.dumps(call), encoding="utf-8"); return {"accepted":True}
            controller.session_publish_proposal.side_effect = publish
            good = {"schema":"firmware-c3-worker-request/v1","request_id":"good","assignment_id":"p1","token":"token","kind":"session-proposal","session_id":"s1","sequence":0,"predecessor_state":"READY","current_state":"READY","next_state":"OP_PLAN_DISCLOSED","call":{}}
            (inbox / "good.json").write_text(json.dumps(good), encoding="utf-8"); harness.service_worker_channels()
            self.assertEqual("REJECTED", json.loads((responses / "bad.json").read_text())["outcome"]); self.assertEqual("ACCEPTED", json.loads((responses / "good.json").read_text())["outcome"])

    def test_c3_cp_06_limitation_shapes_roles_and_broker_gate(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root)
            diagnostic = {"mode":"diagnostic","limitation_id":"L1","attempt_id":"attempt-1","lane_id":"STM-A","session_id":"s1","raw_result":{"path":"x","sha256":"x"},"source_path":"README.md"}
            self.assertEqual("F.C3.A1", harness._limitation_metadata(diagnostic, "F.C3.A1")["worker_role"])
            with self.assertRaises(AdmissionError): harness._limitation_metadata({**diagnostic,"endpoint":"bad"}, "F.C3.A1")
            self.assertEqual("", worker_environment()["MCP_ENDPOINT"]); self.assertEqual("", worker_environment()["MCP_COMMAND"])
            invocation = root / "assignments" / "a1.invocation.json"; invocation.parent.mkdir(); invocation.write_text("{}", encoding="utf-8")
            preparation = {"path":"prep","sha256":"credit"}; status, result = {"path":"status","sha256":"s"}, {"path":"result","sha256":"r"}
            harness.assignments = {"a1":{"preparation":preparation,"limitation":{"limitation_id":"L1","mode":"diagnostic"},"completion":{"outcome":"PASS","status":status,"result":result}}}; harness.limitation_adapter = Mock(); harness.limitation_adapter.complete.return_value = {"attribution_evidence":{}}
            with self.assertRaises(AdmissionError): harness._server_limitation({"decision_path":"decision","limitation_id":"L1"})
            completed = harness._limitation_complete({"assignment_id":"a1"}); self.assertEqual("diagnostic", completed["mode"])
            harness.limitation_adapter.complete.assert_called_once_with(preparation, {"path":str(invocation),"sha256":_digest(invocation)}, status, result)
            with self.assertRaises(AdmissionError): harness._limitation_complete({"assignment_id":"a1"})
            with self.assertRaises(AdmissionError): harness._limitation_complete({"assignment_id":"a1","status":status})
            harness.limitation_completed["L1"]["substitute"] = {}; harness.broker.record_server_limitation = Mock(return_value={"physical_certification":{"status":"NOT_CERTIFIED"}})
            self.assertEqual("NOT_CERTIFIED", harness._server_limitation({"decision_path":"decision","limitation_id":"L1"})["physical_certification"]["status"])

    def test_c3_cp_07_shutdown_blocks_then_reaps_and_refuses_unreconciled_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); harness.workers = {"a1":_Process(None)}
            self.assertEqual("BLOCKED", harness._shutdown({})["state"]); self.assertTrue(harness.admission_closed)
            harness.workers = {}; controller = Mock(); controller.abort_session.return_value = {"exact_reaped":True,"claim_released":True}; harness.controllers = {"s1":controller}
            result = harness._shutdown({}); self.assertEqual("SHUTDOWN", result["state"]); self.assertTrue(Path(result["shutdown_path"]).is_file()); harness.executor.shutdown.assert_called_once(); controller.abort_session.assert_called_once_with("signed harness shutdown")
            harness.registry_path.write_text(json.dumps({"schema":"firmware-c3-worker-lifecycle/v1","state":"STARTED","assignment_id":"lost"}) + "\n", encoding="utf-8")
            with self.assertRaises(AdmissionError): harness._recover_or_fail_closed()


def _digest_token(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()
