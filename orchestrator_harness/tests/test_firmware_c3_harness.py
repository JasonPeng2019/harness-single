"""Host-only regressions for the candidate-owned C3 control-plane facade."""
from __future__ import annotations

import hashlib
import json
import stat
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from concurrent.futures import Future
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import firmware_acceptance.c3_harness as c3
import firmware_acceptance.c3_process_identity as c3_process
from firmware_acceptance.kit import AcceptanceBroker, AdmissionError, worker_environment
from orchestrator_harness.models import ProcessInfo, ProcessSnapshot, iso_utc


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class _Process:
    pid = 4242

    def __init__(self, code: int | None = None) -> None: self.code = code; self.terminated = False; self.waited = False
    def poll(self) -> int | None: return self.code
    def wait(self, timeout: float | None = None) -> int:
        self.waited = True
        if self.code is None: raise subprocess.TimeoutExpired(["synthetic"], timeout)
        return self.code
    def terminate(self) -> None: self.terminated = True; self.code = -15
    def kill(self) -> None: self.code = -9


class C3HarnessTests(unittest.TestCase):
    def _bare(self, root: Path) -> c3.C3Harness:
        harness = object.__new__(c3.C3Harness)
        harness.root = root; harness.seed = Path("firmware_acceptance/seed").resolve()
        harness.candidate_root = Path(__file__).resolve().parents[2]
        harness.policy = Path("firmware_acceptance/MCP_METHOD_POLICY.json").resolve(); harness.templates = Path("firmware_acceptance/LANE_TEMPLATES.json").resolve()
        harness.manifest = Path("firmware_acceptance/ACCEPTANCE_MANIFEST.json").resolve(); harness.c1 = None; harness.delegated = None
        harness.seed_identity = c3._seed_snapshot(harness.seed)
        harness.broker = AcceptanceBroker(root, harness.seed, harness.policy, harness.templates, Path("firmware_acceptance/ACCEPTANCE_MANIFEST.json"))
        harness.verifier = Mock(); harness.controllers = {}; harness.session_lanes = {}; harness.workers = {}; harness.assignments = {}; harness.limitation_completed = {}
        harness.operations = {}; harness.operation_pending = {}; harness.executor = Mock(); harness.shutdown = False; harness.admission_closed = False; harness.recovery = None; harness.recovery_closed = False
        harness.topology = {"attempt_id":"attempt-1", "public_key":"key", "release":{"path":"release","sha256":"r"}, "identity_binding":{"path":"identity","sha256":"i"}}
        harness.request_root = root / "manager-signals" / "c3-requests"; harness.response_root = root / "manager-signals" / "c3-responses"; harness.admission_root = root / "manager-signals" / "c3-admissions"; harness.state_root = root / "c3-harness"
        for path in (harness.request_root, harness.response_root, harness.admission_root, harness.state_root): path.mkdir(parents=True, exist_ok=True)
        harness.status_path = harness.state_root / "STATUS.jsonl"; harness.registry_path = harness.state_root / "REGISTRY.jsonl"
        return harness

    def test_c3_cp_01_02_startup_request_binding_seed_and_replay_barrier(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root)
            expected_names = {"TARGET_SEED_MANIFEST.json", "TARGET_CHARTER.md", "PINNED_INPUTS.json", "TEST_CONTRACT.json", "EVIDENCE_SCHEMA.json"}
            self.assertEqual(expected_names, set(harness.seed_identity)); self.assertTrue(all(isinstance(value, str) and len(value) == 64 for value in harness.seed_identity.values()))
            with patch.object(c3, "exact_process_identity", return_value={"pid":1,"created_utc":"created"}):
                harness.write_readiness()
            readiness = json.loads((harness.state_root / "C3_HARNESS_READY.json").read_text(encoding="utf-8"))
            self.assertEqual(c3._seed_digest(harness.seed_identity), readiness["bindings"]["seed"])
            request = {"schema":"firmware-c3-harness-request/v1", "request_id":"r1", "attempt_id":"attempt-1", "c1_reference":{"path":"c1","sha256":"c"}, "delegated_reference":{"path":"delegated","sha256":"d"}, "orchestrator_identity":harness.topology["identity_binding"], "topology_key_release":harness.topology["release"], "kind":"materialize", "issued_utc":"2026-08-04T00:00:00+00:00", "issued_monotonic":time.monotonic()-1, "expires_monotonic":time.monotonic()+60, "payload":{"target_id":"target"}, "public_key":"key", "signature":"sig"}
            path = harness.request_root / "r1.json"; path.write_text(json.dumps(request), encoding="utf-8")
            with patch.object(c3, "_ref", side_effect=lambda value, label, expected=None: value), patch.object(harness.verifier, "verify", return_value=True): self.assertEqual("r1", harness._load(path)["request_id"])
            request["payload"] = []; path.write_text(json.dumps(request), encoding="utf-8")
            with patch.object(c3, "_ref", side_effect=lambda value, label, expected=None: value), patch.object(harness.verifier, "verify", return_value=True): self.assertEqual("REJECTED", harness.handle(path)["outcome"])
            self.assertFalse((harness.admission_root / "r1.json").exists())
            request["payload"] = {"target_id":"target"}; path.write_text(json.dumps(request), encoding="utf-8")
            stale_dispatch = Mock(return_value={"target":"target"})
            with patch.object(harness, "_load", return_value=request), patch.object(harness, "_dispatch", side_effect=stale_dispatch): self.assertEqual("REJECTED", harness.handle(path)["outcome"])
            self.assertEqual(0, stale_dispatch.call_count)
            accepted = {**request,"request_id":"r2"}; accepted_path = harness.request_root / "r2.json"; accepted_path.write_text(json.dumps(accepted), encoding="utf-8")
            accepted_dispatch = Mock(return_value={"target":"target"})
            with patch.object(harness, "_load", return_value=accepted), patch.object(harness, "_dispatch", side_effect=accepted_dispatch):
                self.assertEqual("ACCEPTED", harness.handle(accepted_path)["outcome"]); self.assertEqual("ACCEPTED", harness.handle(accepted_path)["outcome"])
            self.assertEqual(1, accepted_dispatch.call_count)
            tampered = {**request,"request_id":"r3","signature":"tampered"}; tampered_path = harness.request_root / "r3.json"; tampered_path.write_text(json.dumps(tampered), encoding="utf-8")
            with patch.object(c3, "_ref", side_effect=lambda value, label, expected=None: value), patch.object(harness.verifier, "verify", return_value=False): self.assertEqual("REJECTED", harness.handle(tampered_path)["outcome"])

    def test_c3_startup_distinguishes_operative_and_source_policy_bindings(self) -> None:
        policy_body = {"schema": "firmware-mcp-method-policy/v1", "methods": ["observe"]}

        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)

            def build(case: str) -> tuple[dict[str, object], Path, Path, Path]:
                case_root = base / case; candidate = case_root / "candidate"; source_dir = candidate / "firmware_acceptance"
                source_dir.mkdir(parents=True); root = case_root / "runtime"; root.mkdir(); topology = case_root / "topology"; topology.mkdir()
                operative = case_root / "c1-local-policy.json"; operative.write_text(json.dumps(policy_body, indent=2), encoding="utf-8")
                source = source_dir / "MCP_METHOD_POLICY.json"; source.write_text(json.dumps(policy_body, separators=(",", ":"), sort_keys=True), encoding="utf-8")
                templates, manifest, delegated = case_root / "templates.json", case_root / "manifest.json", case_root / "delegated.json"
                for path in (templates, manifest, delegated): path.write_text("{}", encoding="utf-8")
                if case == "substitution":
                    source_binding = {"path": str(source_dir / "substitute.json"), "sha256": _digest(source)}
                elif case == "hash-drift":
                    source_binding = {"path": str(source), "sha256": "0" * 64}
                else:
                    source_binding = {"path": str(source), "sha256": _digest(source)}
                if case == "malformed": source.write_text("{", encoding="utf-8"); source_binding["sha256"] = _digest(source)
                if case == "semantic-mismatch":
                    source.write_text(json.dumps({"schema": "firmware-mcp-method-policy/v1", "methods": ["operate"]}), encoding="utf-8"); source_binding["sha256"] = _digest(source)
                if case == "linked":
                    linked_dir = case_root / "linked-policy-source"; linked_dir.mkdir(); linked_source = linked_dir / source.name
                    linked_source.write_bytes(source.read_bytes()); source.unlink(); source_dir.rmdir()
                    created = subprocess.run(["cmd", "/c", "mklink", "/J", str(source_dir), str(linked_dir)], capture_output=True, text=True)
                    self.assertEqual(0, created.returncode, created.stderr)
                    source = source_dir / source.name; source_binding = {"path": str(source), "sha256": _digest(linked_source)}
                seed = Path("firmware_acceptance/seed").resolve()
                c1_body = {"schema":"firmware-v2-c1-lock/v1", "candidate":{"path":str(candidate),"branch":"test-branch","commit":"test-commit","clean":True}, "authorization":{"path":str(delegated),"sha256":_digest(delegated)}, "server":{"commit":"f003f84a7df51cd8595a3203c62e225b21da2a22","immutable_fixture":True}, "candidate_acceptance_inputs":{"acceptance_manifest":{"path":str(manifest),"sha256":_digest(manifest)},"lane_templates":{"path":str(templates),"sha256":_digest(templates)},"mcp_method_policy":{"path":str(operative),"sha256":_digest(operative)},"mcp_method_policy_source":source_binding}, "target_seed":{"manifest":{"path":str(seed / "TARGET_SEED_MANIFEST.json"),"sha256":_digest(seed / "TARGET_SEED_MANIFEST.json")}}}
                c1 = case_root / "C1.json"; c1.write_text(json.dumps(c1_body), encoding="utf-8")
                return c1_body, root, topology, c1

            def git_identity(command: list[str], **_: object) -> SimpleNamespace:
                if command[1:] == ["rev-parse", "HEAD"]: return SimpleNamespace(stdout="test-commit\n")
                if command[1:] == ["branch", "--show-current"]: return SimpleNamespace(stdout="test-branch\n")
                if command[1:] == ["status", "--porcelain", "--untracked-files=all"]: return SimpleNamespace(stdout="")
                raise AssertionError(command)

            def construct(case: str) -> tuple[c3.C3Harness, dict[str, object], Path]:
                c1_body, root, topology, c1 = build(case); candidate = Path(c1_body["candidate"]["path"])
                with patch.object(c3, "__file__", str(candidate / "firmware_acceptance" / "c3_harness.py")), patch.object(c3.subprocess, "run", side_effect=git_identity), patch.object(c3, "load_root_topology", return_value={"attempt_id":"attempt"}), patch.object(c3, "validate_manifest", return_value={}), patch.object(c3, "validate_delegated_authorization", return_value={}), patch.object(c3, "AcceptanceBroker"), patch.object(c3, "LimitationEvidenceAdapter"):
                    harness = c3.C3Harness(root, Path("firmware_acceptance/seed").resolve(), Path(c1_body["candidate_acceptance_inputs"]["mcp_method_policy"]["path"]), Path(c1_body["candidate_acceptance_inputs"]["lane_templates"]["path"]), topology, c1={"path":str(c1),"sha256":_digest(c1)}, delegated=c1_body["authorization"], manifest=Path(c1_body["candidate_acceptance_inputs"]["acceptance_manifest"]["path"]))
                return harness, c1_body, root

            harness, valid, _ = construct("valid")
            try:
                operative = valid["candidate_acceptance_inputs"]["mcp_method_policy"]
                source = valid["candidate_acceptance_inputs"]["mcp_method_policy_source"]
                self.assertNotEqual(operative["path"], source["path"]); self.assertNotEqual(operative["sha256"], source["sha256"])
                self.assertEqual(operative["path"], str(harness.policy)); self.assertEqual(operative["sha256"], _digest(harness.policy))
                self.assertEqual(source["path"], str(harness.candidate_root / "firmware_acceptance" / "MCP_METHOD_POLICY.json")); self.assertEqual(source["sha256"], _digest(Path(source["path"])))
            finally:
                harness.executor.shutdown(wait=True)
            for case in ("substitution", "hash-drift", "linked", "malformed", "semantic-mismatch"):
                with self.assertRaises(AdmissionError): construct(case)
                self.assertFalse((base / case / "runtime" / "c3-harness" / "C3_HARNESS_READY.json").exists())

    def test_c3_cp_04_disposable_target_assignment_and_fail_closed_acceptance(self) -> None:
        self.assertEqual({"F.C3.A1":"priority", "F.C3.C1":"priority", "F.C3.P1":"priority", "F.C3.R1":"priority"}, {role:spec[2] for role, spec in c3._ROLES.items()})
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); target = root / "target"; harness.broker = AcceptanceBroker(root, harness.seed, harness.policy, harness.templates, harness.manifest, target_root=target); harness.broker.materialize_seed(target)
            payload = {"assignment_id":"a1", "role":"F.C3.A1", "sprint":"S23", "task":"host test", "prompt":"do bounded work", "target_id":"target", "declared_resources":[]}; process = _Process(); launches: list[tuple[list[str], Path]] = []
            created = datetime(2026, 8, 4, tzinfo=timezone.utc); snapshot = ProcessSnapshot(True, (ProcessInfo(process.pid, 1, "python", "lane controller", created),), (), "synthetic")
            def launch(command: list[str], *, cwd: Path, **_: object) -> _Process:
                invocation = json.loads(Path(command[-1]).read_text(encoding="utf-8")); status = Path(invocation["output_paths"]["status"])
                def publish_delayed(_: float) -> None:
                    status.write_text(json.dumps({"schema":"orchestrator-lane-controller/v1","state":"WAITING_RESOURCE","controller_pid":process.pid,"controller_created_utc":iso_utc(created)}), encoding="utf-8")
                delayed_sleeps.append(publish_delayed)
                launches.append((command, cwd)); return process
            try:
                identity = {"pid":4242,"created_utc":"created"}
                observations = Mock(side_effect=[identity] * 7)
                delayed_sleeps: list[object] = []
                controller_subprocess = SimpleNamespace(Popen=launch, run=subprocess.run, TimeoutExpired=subprocess.TimeoutExpired, SubprocessError=subprocess.SubprocessError)
                def sleep(_: float) -> None:
                    delayed_sleeps.pop(0)(_)  # authentic publication happens only after the first poll.
                with patch.object(c3, "subprocess", controller_subprocess), patch.object(c3, "process_snapshot", return_value=snapshot), patch.object(c3, "exact_process_identity", observations), patch.object(c3.time, "sleep", side_effect=sleep): launched = harness._assignment(payload)
                record = harness.assignments["a1"]; worktree = Path(record["worktree"])
                self.assertTrue((worktree / ".git").is_file()); self.assertEqual("LAUNCHED", launched["state"])
                self.assertEqual(harness.candidate_root, launches[0][1]); self.assertEqual(str(worktree), record["invocation"]["run_root"])
                self.assertEqual(7, observations.call_count)
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
                with patch.object(c3, "subprocess", rejected_subprocess), patch.object(c3, "process_snapshot", return_value=rejected_snapshot), patch.object(c3, "exact_process_identity", side_effect=[{"pid":4242,"created_utc":"before"}, {"pid":4242,"created_utc":"after"}, {"pid":4242,"created_utc":"after"}, {"pid":4242,"created_utc":"after"}]):
                    with self.assertRaises(AdmissionError): harness._assignment({**payload,"assignment_id":"a2"})
                self.assertFalse(rejected.terminated); self.assertTrue(rejected.waited); self.assertTrue(harness.admission_closed)
                recovery = json.loads((harness.state_root / "RECOVERY_REQUIRED.json").read_text()); self.assertEqual("RECOVERY_REQUIRED", recovery["cleanup"]["outcome"]); self.assertEqual(4242, recovery["launcher_pid"]); self.assertEqual({"pid":4242,"created_utc":"before"}, recovery["launcher_identity"]); self.assertEqual({"pid":4242,"created_utc":"after"}, recovery["observed_launcher_identity"]); self.assertIsNone(recovery["controller_identity"])
            finally:
                for assignment_id in ("a1", "a2"):
                    worktree = root / "assignment-worktrees" / assignment_id
                    if worktree.exists(): subprocess.run(["git","worktree","remove","--force",str(worktree)], cwd=target, check=True, capture_output=True, text=True)

    def test_c3_cp_05_sessions_overlap_and_p1_channel_is_token_bound(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); first, second = Future(), Future(); controllers = {"s1":Mock(), "s2":Mock(), "bad":Mock()}; harness.controllers = controllers; harness.session_lanes = {"s1":"STM-A", "s2":"NRF-A", "bad":"BAD-A"}
            def rejected(*args: object) -> None:
                self.assertEqual((Path("bad-p"), Path("bad-d"), Path("bad-a"), harness.verifier), args); self.assertFalse((root / "hil" / "BAD-A" / "sessions" / "bad" / "operations").exists()); self.assertEqual(0, harness.executor.submit.call_count); raise AdmissionError("authorization rejected")
            controllers["bad"].session_derive_authorization.side_effect = rejected
            with self.assertRaises(AdmissionError): harness._session({"session_id":"bad","proposal_path":"bad-p","decision_path":"bad-d","authorization_path":"bad-a"}, "execute")
            self.assertNotIn("bad", harness.operations); self.assertNotIn("bad", harness.operation_pending); harness.executor.submit.assert_not_called()
            futures = [first, second]
            def derived(sid: str, *args: object) -> None:
                operation_root = root / "hil" / harness.session_lanes[sid] / "sessions" / sid / "operations"
                self.assertEqual((Path("p"), Path("d"), Path("a"), harness.verifier), args); self.assertFalse(operation_root.exists()); self.assertNotIn(sid, harness.operations); self.assertNotIn(sid, harness.operation_pending); self.assertEqual(("s1", "s2").index(sid), harness.executor.submit.call_count)
            for sid in ("s1", "s2"): controllers[sid].session_derive_authorization.side_effect = lambda *args, sid=sid: derived(sid, *args)
            def submit(*args: object) -> Future[object]:
                sid = ("s1", "s2")[harness.executor.submit.call_count - 1]; operation_root = root / "hil" / harness.session_lanes[sid] / "sessions" / sid / "operations"
                controllers[sid].session_derive_authorization.assert_called_once_with(Path("p"), Path("d"), Path("a"), harness.verifier); self.assertEqual(1, len(list(operation_root.glob("*.PENDING.json")))); return futures.pop(0)
            harness.executor.submit.side_effect = submit
            outer_session, request_session = str(uuid.uuid4()), str(uuid.uuid4()); request = root / "closed-session.json"; request.write_text(json.dumps({"session_id":request_session,"lane_id":"STM-A"}), encoding="utf-8")
            mismatch_controller = c3.FirmwareAcceptanceController(harness.broker, topology=harness.topology)
            with patch.object(mismatch_controller, "_load_external", return_value={"session_id":request_session}), patch.object(mismatch_controller, "create_session_request") as create_request, patch.object(c3, "FirmwareAcceptanceController", return_value=mismatch_controller), self.assertRaises(AdmissionError): harness._open({"session_id":outer_session,"request_path":str(request)})
            create_request.assert_not_called(); self.assertNotIn(outer_session, harness.controllers); self.assertFalse(harness.registry_path.exists())
            for sid in ("s1","s2"): self.assertEqual("PENDING", harness._session({"session_id":sid,"proposal_path":"p","decision_path":"d","authorization_path":"a"}, "execute")["state"])
            with self.assertRaises(AdmissionError): harness._session({"session_id":"s1","proposal_path":"p","decision_path":"d","authorization_path":"a"}, "execute")
            first.set_result({"call":"one"}); second.set_result({"call":"two"}); harness.reap_operations()
            for sid in ("s1","s2"):
                terminal = next((root / "hil" / harness.session_lanes[sid] / "sessions" / sid / "operations").glob("*.TERMINAL.json")); value = json.loads(terminal.read_text(encoding="utf-8")); self.assertEqual(_digest(Path(value["pending"]["path"])), value["pending"]["sha256"])
            harness._recover_or_fail_closed()
            pending = root / "hil" / "STM-A" / "sessions" / "lost" / "operations" / "x.PENDING.json"; pending.parent.mkdir(parents=True); pending.write_text("{}", encoding="utf-8")
            with self.assertRaises(AdmissionError): harness._recover_or_fail_closed()
            controller = Mock(); controller.open_session.return_value = {"session_id":request_session}
            with patch.object(c3, "FirmwareAcceptanceController", return_value=controller): self.assertEqual({"session_id":request_session}, harness._open({"session_id":request_session,"request_path":str(request)}))
            controller.open_session.assert_called_once_with(request, expected_session_id=request_session); self.assertIs(harness.controllers[request_session], controller)
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
            root = Path(temporary); harness = self._bare(root); harness.workers = {"a1":_Process(None)}; harness.session_lanes = {"s1":"STM-A"}
            self.assertEqual("BLOCKED", harness._shutdown({})["state"]); self.assertTrue(harness.admission_closed)
            harness.workers = {}; controller = Mock(); controller.abort_session.return_value = {"exact_reaped":True,"claim_released":True}; harness.controllers = {"s1":controller}
            result = harness._shutdown({}); self.assertEqual("SHUTDOWN", result["state"]); self.assertTrue(Path(result["shutdown_path"]).is_file()); harness.executor.shutdown.assert_called_once(); controller.abort_session.assert_called_once_with("signed harness shutdown")
            harness.registry_path.write_text(json.dumps({"schema":"firmware-c3-worker-lifecycle/v1","state":"STARTED","assignment_id":"lost"}) + "\n", encoding="utf-8")
            with self.assertRaises(AdmissionError): harness._recover_or_fail_closed()

    def test_s25_a1_singular_target_preserves_generic_broker_default(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); generic = AcceptanceBroker(root / "generic", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
            generic_target = root / "generic" / "targets" / "ordinary"; generic.materialize_seed(generic_target)
            self.assertEqual(40, len(generic.validate_target(generic_target)))
            harness = self._bare(root / "c3"); target = harness.root / "target"
            harness.broker = AcceptanceBroker(harness.root, harness.seed, harness.policy, harness.templates, harness.manifest, target_root=target)
            self.assertEqual(str(target), harness._dispatch("materialize", {"target_id":"target"})["target"])
            with self.assertRaises(AdmissionError): harness._dispatch("materialize", {"target_id":"other"})
            self.assertFalse((harness.root / "targets").exists())

    def test_s25_a1_recovery_record_is_closed_and_restart_remains_shutdown_only(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); aid = "a1"; identity = {"pid":4242,"created_utc":"old"}; observed = {"pid":4242,"created_utc":"new"}
            controller = {"pid":4343,"created_utc":"controller"}; cleanup = {"schema":"firmware-c3-prestart-rejection/v1","assignment_id":aid,"reason":"identity changed","launcher_identity":identity,"controller_identity":controller,"handles_closed":True,"launcher_reaped":False,"controller_reaped":False,"process_reaped":False,"worktree_removed":False,"branch_removed":False,"channels_removed":False,"outcome":"RECOVERY_REQUIRED"}
            recovery = {"schema":"firmware-c3-recovery-required/v1","assignment_id":aid,"launcher_pid":4242,"launcher_identity":identity,"observed_launcher_identity":observed,"controller_identity":controller,"worktree":str(root / "assignment-worktrees" / aid),"branch":"c3/target/a1","channels":[str(root / "worker-channel" / aid),str(root / "worker-channel-responses" / aid)],"cleanup":cleanup}
            self.assertEqual(aid, harness._validate_recovery_record(recovery))
            for label, mutate in {
                "extra": lambda r: r.__setitem__("extra", True), "bad-launcher-pid": lambda r: r.__setitem__("launcher_pid", 0),
                "wrong-observed-pid": lambda r: r.__setitem__("observed_launcher_identity", {"pid":1,"created_utc":"new"}), "bad-branch": lambda r: r.__setitem__("branch", "other"),
                "bad-controller": lambda r: r.__setitem__("controller_identity", {"pid":True,"created_utc":"bad"}), "bad-cleanup": lambda r: r["cleanup"].__setitem__("worktree_removed", True),
            }.items():
                with self.subTest(label=label):
                    value = json.loads(json.dumps(recovery)); mutate(value)
                    with self.assertRaises(AdmissionError): harness._validate_recovery_record(value)
            recovery_path = harness.state_root / "RECOVERY_REQUIRED.json"; recovery_path.write_text(json.dumps(recovery), encoding="utf-8")
            cleanup_path = root / "assignments" / "a1.PRESTART_RECOVERY_RECONCILED.json"; cleanup_path.parent.mkdir(); reconciled_cleanup = {**cleanup,"outcome":"REAPED","launcher_reaped":True,"controller_reaped":True,"process_reaped":True,"worktree_removed":True,"branch_removed":True,"channels_removed":True}
            cleanup_path.write_text(json.dumps(reconciled_cleanup), encoding="utf-8")
            reconciled = {"schema":"firmware-c3-recovery-reconciled/v1","recovery":{"path":str(recovery_path),"sha256":_digest(recovery_path)},"cleanup":{"path":str(cleanup_path),"sha256":_digest(cleanup_path)}}
            (harness.state_root / "RECOVERY_RECONCILED.json").write_text(json.dumps(reconciled), encoding="utf-8")
            harness._recover_or_fail_closed(); self.assertTrue(harness.admission_closed); self.assertTrue(harness.recovery_closed)
            blocked = harness.request_root / "blocked.json"; blocked.write_text("{}", encoding="utf-8")
            with patch.object(harness, "_load", return_value={"request_id":"blocked","kind":"materialize","payload":{"target_id":"target"}}), patch.object(harness, "_dispatch") as dispatch:
                self.assertEqual("REJECTED", harness.handle(blocked)["outcome"])
            dispatch.assert_not_called()

    def test_s25_a1_recovery_required_response_and_reap_reconciliation_are_exact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); process = _Process(); process.code = None
            identity, observed, controller = {"pid":4242,"created_utc":"old"}, {"pid":4242,"created_utc":"new"}, None
            channels = (root / "worker-channel" / "a1", root / "worker-channel-responses" / "a1")
            target = root / "target"; harness.broker = AcceptanceBroker(root, harness.seed, harness.policy, harness.templates, harness.manifest, target_root=target); harness.broker.materialize_seed(target)
            worktree, branch = root / "assignment-worktrees" / "a1", "c3/target/a1"
            subprocess.run(["git","worktree","add","-b",branch,str(worktree),"HEAD"],cwd=target,check=True,capture_output=True,text=True)
            for channel in channels: channel.mkdir(parents=True)
            recovery_cleanup = {"schema":"firmware-c3-prestart-rejection/v1","assignment_id":"a1","reason":"identity changed","launcher_identity":identity,"controller_identity":controller,"handles_closed":True,"launcher_reaped":False,"controller_reaped":True,"process_reaped":False,"worktree_removed":False,"branch_removed":False,"channels_removed":False,"outcome":"RECOVERY_REQUIRED"}
            c3._write_new(root / "assignments" / "a1.PRESTART_REJECTED.json", recovery_cleanup)
            first_path = harness.request_root / "first.json"; first_path.write_text("{}", encoding="utf-8"); first = {"request_id":"first","kind":"assignment","payload":{}}
            def discover(_: str, __: dict[str, object]) -> dict[str, object]:
                harness._require_recovery("a1",process,identity,controller,worktree,branch,channels,recovery_cleanup)
                raise AssertionError("recovery must raise")
            with patch.object(c3, "exact_process_identity", return_value=observed), patch.object(harness,"_load",return_value=first), patch.object(harness,"_dispatch",side_effect=discover):
                response = harness.handle(first_path)
            self.assertEqual("RECOVERY_REQUIRED",response["outcome"]); self.assertTrue(harness.admission_closed); self.assertIs(harness.recovery["process"], process)
            recovery_ref = response["recovery"]; recovery_path = Path(recovery_ref["path"]); self.assertEqual({"path":str(recovery_path),"sha256":_digest(recovery_path)}, {"path":recovery_ref["path"],"sha256":recovery_ref["sha256"]})
            statuses = [json.loads(line) for line in harness.status_path.read_text().splitlines()]; self.assertEqual("RECOVERY_REQUIRED",statuses[-1]["state"])
            second_path = harness.request_root / "second.json"; second_path.write_text("{}",encoding="utf-8"); second = {"request_id":"second","kind":"materialize","payload":{"target_id":"target"}}
            with patch.object(harness,"_load",return_value=second), patch.object(harness,"_dispatch") as dispatch:
                rejected = harness.handle(second_path)
            self.assertEqual("REJECTED",rejected["outcome"]); self.assertIn("admission is closed",rejected["reason"]); dispatch.assert_not_called()
            process.code = 0
            harness.reap_recovery(); cleanup_path = root / "assignments" / "a1.PRESTART_RECOVERY_RECONCILED.json"; cleanup = json.loads(cleanup_path.read_text(encoding="utf-8"))
            self.assertEqual({"schema","assignment_id","reason","launcher_identity","controller_identity","handles_closed","launcher_reaped","controller_reaped","process_reaped","worktree_removed","branch_removed","channels_removed","outcome"},set(cleanup)); self.assertEqual("REAPED",cleanup["outcome"]); self.assertEqual(identity, cleanup["launcher_identity"]); self.assertIsNone(cleanup["controller_identity"]); self.assertTrue(all(cleanup[key] for key in ("handles_closed","launcher_reaped","controller_reaped","process_reaped","worktree_removed","branch_removed","channels_removed")))
            self.assertFalse(worktree.exists()); self.assertFalse(any(channel.exists() for channel in channels)); self.assertNotEqual(0,subprocess.run(["git","show-ref","--verify","--quiet","refs/heads/" + branch],cwd=target).returncode)
            reconciled = json.loads((harness.state_root / "RECOVERY_RECONCILED.json").read_text(encoding="utf-8")); self.assertEqual({"path":str(cleanup_path),"sha256":_digest(cleanup_path)},reconciled["cleanup"]); self.assertIsNone(harness.recovery)

    def test_s25_a1_recovery_restart_rejects_closed_evidence_mutation_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); aid = "a1"; identity = {"pid":4242,"created_utc":"old"}; observed = {"pid":4242,"created_utc":"new"}
            controller = {"pid":4343,"created_utc":"controller"}; cleanup = {"schema":"firmware-c3-prestart-rejection/v1","assignment_id":aid,"reason":"identity changed","launcher_identity":identity,"controller_identity":controller,"handles_closed":True,"launcher_reaped":False,"controller_reaped":False,"process_reaped":False,"worktree_removed":False,"branch_removed":False,"channels_removed":False,"outcome":"RECOVERY_REQUIRED"}
            recovery = {"schema":"firmware-c3-recovery-required/v1","assignment_id":aid,"launcher_pid":4242,"launcher_identity":identity,"observed_launcher_identity":observed,"controller_identity":controller,"worktree":str(root / "assignment-worktrees" / aid),"branch":"c3/target/a1","channels":[str(root / "worker-channel" / aid),str(root / "worker-channel-responses" / aid)],"cleanup":cleanup}
            recovery_path, cleanup_path, reconciled_path = harness.state_root / "RECOVERY_REQUIRED.json", root / "assignments" / "a1.PRESTART_RECOVERY_RECONCILED.json", harness.state_root / "RECOVERY_RECONCILED.json"
            cleanup_path.parent.mkdir(); recovery_path.write_text(json.dumps(recovery), encoding="utf-8")
            reaped = {**cleanup,"outcome":"REAPED","launcher_reaped":True,"controller_reaped":True,"process_reaped":True,"worktree_removed":True,"branch_removed":True,"channels_removed":True}; cleanup_path.write_text(json.dumps(reaped), encoding="utf-8")
            reconciled = {"schema":"firmware-c3-recovery-reconciled/v1","recovery":{"path":str(recovery_path),"sha256":_digest(recovery_path)},"cleanup":{"path":str(cleanup_path),"sha256":_digest(cleanup_path)}}; reconciled_path.write_text(json.dumps(reconciled), encoding="utf-8")
            mutations = {
                "absent-reconciled": lambda: reconciled_path.unlink(), "bad-reconciled-schema": lambda: reconciled_path.write_text(json.dumps({**reconciled,"schema":"bad"}), encoding="utf-8"),
                "bad-recovery-digest": lambda: reconciled_path.write_text(json.dumps({**reconciled,"recovery":{**reconciled["recovery"],"sha256":"0" * 64}}), encoding="utf-8"),
                "bad-cleanup-path": lambda: reconciled_path.write_text(json.dumps({**reconciled,"cleanup":{**reconciled["cleanup"],"path":str(root / "other")}}), encoding="utf-8"),
                "bad-cleanup-digest": lambda: reconciled_path.write_text(json.dumps({**reconciled,"cleanup":{**reconciled["cleanup"],"sha256":"0" * 64}}), encoding="utf-8"),
                "non-reaped": lambda: cleanup_path.write_text(json.dumps(cleanup), encoding="utf-8"),
                "wrong-assignment": lambda: recovery_path.write_text(json.dumps({**recovery,"assignment_id":"a2"}), encoding="utf-8"),
                "wrong-launcher-identity": lambda: recovery_path.write_text(json.dumps({**recovery,"launcher_identity":{"pid":1,"created_utc":"wrong"}}), encoding="utf-8"),
                "extra-key": lambda: recovery_path.write_text(json.dumps({**recovery,"extra":True}), encoding="utf-8"),
            }
            for name, mutate in mutations.items():
                with self.subTest(name=name):
                    recovery_path.write_text(json.dumps(recovery), encoding="utf-8"); cleanup_path.write_text(json.dumps(reaped), encoding="utf-8"); reconciled_path.write_text(json.dumps(reconciled), encoding="utf-8"); mutate()
                    with self.assertRaises(AdmissionError): harness._recover_or_fail_closed()

    def test_s25_a1_session_roots_are_single_hil_lane_and_generic_defaults_remain(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); sid, lane = "session-1", "STM-A"; request = root / "request.json"; request.write_text(json.dumps({"session_id":sid,"lane_id":lane}), encoding="utf-8")
            controller = Mock(); controller.open_session.return_value = {"session_id":sid}
            with patch.object(c3, "FirmwareAcceptanceController", return_value=controller) as constructed:
                harness._open({"session_id":sid,"request_path":str(request)})
            kwargs = constructed.call_args.kwargs; session_root = kwargs["session_root_for"]({"session_id":sid,"lane_id":lane}); lane_root = kwargs["lane_root_for"](lane)
            self.assertEqual(root / "hil" / lane / "sessions" / sid, session_root); self.assertEqual(root / "hil" / lane, lane_root)
            self.assertNotIn(root / "sessions", (session_root, lane_root)); self.assertEqual(lane, harness.session_lanes[sid])
            broker = AcceptanceBroker(root / "generic", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
            self.assertEqual(str(root / "generic" / "lanes" / lane / ".firm"), broker.controller_config(lane, {})["roots"]["firm"])

    def test_s25_a1_initial_status_rejects_malformed_wrong_identity_exit_and_timeout(self) -> None:
        """Real assignment admission never registers STARTED without one exact live status."""
        cases: dict[str, object] = {"malformed":"{", "scalar":1, "list":[], "null":None,
                                    "missing-schema":{"state":"WAITING_RESOURCE","controller_pid":4242,"controller_created_utc":"2026-08-04T00:00:00Z"},
                                    "wrong-schema":{"schema":"other","state":"WAITING_RESOURCE","controller_pid":4242,"controller_created_utc":"2026-08-04T00:00:00Z"},
                                    "wrong-pid":{"schema":"orchestrator-lane-controller/v1","state":"WAITING_RESOURCE","controller_pid":1,"controller_created_utc":"2026-08-04T00:00:00Z"},
                                    "wrong-created":{"schema":"orchestrator-lane-controller/v1","state":"WAITING_RESOURCE","controller_pid":4242,"controller_created_utc":"other"},
                                    "wrong-state":{"schema":"orchestrator-lane-controller/v1","state":"CODEX_EXITED","controller_pid":4242,"controller_created_utc":"2026-08-04T00:00:00Z"},
                                    "timeout":None, "quick-exit":None}
        for name, value in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary); harness = self._bare(root); target = root / "target"; harness.broker = AcceptanceBroker(root, harness.seed, harness.policy, harness.templates, harness.manifest, target_root=target); harness.broker.materialize_seed(target)
                process = _Process(9 if name == "quick-exit" else None); created = datetime(2026, 8, 4, tzinfo=timezone.utc); snapshot = ProcessSnapshot(True, (ProcessInfo(4242, 1, "python", "lane", created),), (), "synthetic")
                def launch(command: list[str], **_: object) -> _Process:
                    if command[:3] != [sys.executable, "-m", "orchestrator_harness.lane_controller"]: return real_popen(command, **_)
                    status = Path(json.loads(Path(command[-1]).read_text(encoding="utf-8"))["output_paths"]["status"])
                    if name != "timeout" and name != "quick-exit": status.write_text(value if isinstance(value, str) else json.dumps(value), encoding="utf-8")
                    return process
                identity = {"pid":4242,"created_utc":"exact"}
                def exact(_: int) -> dict[str, object] | None: return identity if process.poll() is None else None
                payload = {"assignment_id":"a-" + name,"role":"F.C3.A1","sprint":"S25","task":"status","prompt":"x","target_id":"target","declared_resources":[]}
                real_popen = subprocess.Popen
                # The synthetic clock consumes one attempted read, then expires; sleep is a no-op.
                with patch.object(c3.subprocess, "Popen", side_effect=launch), patch.object(c3, "process_snapshot", return_value=snapshot), patch.object(c3, "exact_process_identity", side_effect=exact), patch.object(c3.time, "monotonic", side_effect=[0.0, 0.0, 91.0]), patch.object(c3.time, "sleep"):
                    with self.assertRaises(AdmissionError): harness._assignment(payload)
                self.assertNotIn(payload["assignment_id"], harness.workers); self.assertFalse(harness.registry_path.exists())
                cleanup = json.loads((root / "assignments" / (payload["assignment_id"] + ".PRESTART_REJECTED.json")).read_text(encoding="utf-8"))
                self.assertEqual({"schema","assignment_id","reason","launcher_identity","controller_identity","handles_closed","launcher_reaped","controller_reaped","process_reaped","worktree_removed","branch_removed","channels_removed","outcome"}, set(cleanup)); self.assertEqual("REAPED", cleanup["outcome"])
                self.assertEqual(None if name == "quick-exit" else identity, cleanup["launcher_identity"]); self.assertIsNone(cleanup["controller_identity"])
                self.assertTrue(all(cleanup[key] for key in ("handles_closed","launcher_reaped","controller_reaped","process_reaped","worktree_removed","branch_removed","channels_removed")))
                self.assertFalse((root / "assignment-worktrees" / payload["assignment_id"]).exists()); self.assertFalse((root / "worker-channel" / payload["assignment_id"]).exists()); self.assertFalse((root / "worker-channel-responses" / payload["assignment_id"]).exists())
                self.assertNotEqual(0, subprocess.run(["git","show-ref","--verify","--quiet","refs/heads/c3/target/" + payload["assignment_id"]], cwd=target).returncode); self.assertTrue(process.terminated or name == "quick-exit")
                if name == "quick-exit": self.assertFalse(process.terminated); self.assertEqual(9, process.code)

    def test_s27_a1_inauthentic_direct_controller_observation_retains_exact_identity_for_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); target = root / "target"
            harness.broker = AcceptanceBroker(root, harness.seed, harness.policy, harness.templates, harness.manifest, target_root=target); harness.broker.materialize_seed(target)
            process = _Process(); launcher = {"pid":4242,"created_utc":"launcher"}; controller_identity = {"pid":4343,"created_utc":"controller"}
            launcher_created, controller_created = datetime(2026, 8, 4, tzinfo=timezone.utc), datetime(2026, 8, 4, 0, 0, 1, tzinfo=timezone.utc)
            snapshot = ProcessSnapshot(True, (ProcessInfo(4242, 1, "python", "launcher", launcher_created), ProcessInfo(4343, 4242, "python", "controller", controller_created)), (), "synthetic")
            real_popen = subprocess.Popen
            def launch(command: object, *args: object, **kwargs: object) -> _Process | subprocess.Popen[bytes]:
                if not (isinstance(command, list) and command[:3] == [sys.executable, "-m", "orchestrator_harness.lane_controller"]):
                    return real_popen(command, *args, **kwargs)
                status = Path(json.loads(Path(command[-1]).read_text(encoding="utf-8"))["output_paths"]["status"])
                status.write_text(json.dumps({"schema":"orchestrator-lane-controller/v1","state":"CODEX_EXITED","controller_pid":4343,"controller_created_utc":iso_utc(controller_created)}), encoding="utf-8")
                return process
            def exact(pid: int) -> dict[str, object] | None:
                if process.poll() is not None: return None
                return launcher if pid == 4242 else controller_identity if pid == 4343 else None
            payload = {"assignment_id":"a1","role":"F.C3.A1","sprint":"S27","task":"x","prompt":"x","target_id":"target","declared_resources":[]}
            with patch.object(c3.subprocess, "Popen", side_effect=launch), patch.object(c3, "process_snapshot", return_value=snapshot), patch.object(c3, "exact_process_identity", side_effect=exact):
                with self.assertRaises(AdmissionError): harness._assignment(payload)
            cleanup = json.loads((root / "assignments" / "a1.PRESTART_REJECTED.json").read_text(encoding="utf-8"))
            observation = json.loads((root / "assignments" / "a1.CONTROLLER_OBSERVATION.json").read_text(encoding="utf-8"))
            self.assertEqual(controller_identity, observation["controller_identity"])
            self.assertEqual(controller_identity, cleanup["controller_identity"])
            self.assertEqual("REAPED", cleanup["outcome"])

    def test_s25_a1_prestart_branch_only_and_setup_launch_failures_cleanup_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); target = root / "target"; harness.broker = AcceptanceBroker(root, harness.seed, harness.policy, harness.templates, harness.manifest, target_root=target); harness.broker.materialize_seed(target)
            for name, failure in {"branch-only": "worktree", "setup": "seed", "handle": "open", "popen": "popen"}.items():
                with self.subTest(name=name):
                    aid, branch, worktree = "a-" + name, "c3/target/a-" + name, root / "assignment-worktrees" / ("a-" + name)
                    channels = (root / "worker-channel" / aid, root / "worker-channel-responses" / aid)
                    payload = {"assignment_id":aid,"role":"F.C3.A1","sprint":"S25","task":"x","prompt":"x","target_id":"target","declared_resources":[]}; real_run = subprocess.run
                    def run(command: list[str], **kwargs: object) -> object:
                        if failure == "worktree" and command[:3] == ["git","worktree","add"]:
                            real_run(["git","branch",branch,command[-1]], cwd=target, check=True, capture_output=True, text=True)
                            return SimpleNamespace(returncode=1, stdout="", stderr="synthetic branch-only failure")
                        return real_run(command, **kwargs)
                    protected = AdmissionError("synthetic setup failure") if failure == "seed" else c3._protected_seed_snapshot
                    original_open = Path.open
                    captured_stdout: list[object] = []
                    def open_fault(path: Path, mode: str = "r", *args: object, **kwargs: object) -> object:
                        if failure == "open" and path.name == aid + ".controller.stderr.log" and mode == "xb": raise OSError("synthetic second-handle failure")
                        handle = original_open(path, mode, *args, **kwargs)
                        if failure == "open" and path.name == aid + ".controller.stdout.log" and mode == "xb": captured_stdout.append(handle)
                        return handle
                    real_popen = subprocess.Popen
                    def controller_popen(command: list[str], *args: object, **kwargs: object) -> object:
                        if command[:3] != [sys.executable, "-m", "orchestrator_harness.lane_controller"]: return real_popen(command, *args, **kwargs)
                        if failure == "popen": raise OSError("synthetic Popen failure")
                        raise AssertionError("controller launch must not run for " + failure)
                    with patch.object(c3.subprocess, "run", side_effect=run), patch.object(c3, "_protected_seed_snapshot", side_effect=protected), patch.object(Path, "open", new=open_fault), patch.object(c3.subprocess, "Popen", side_effect=controller_popen):
                        with self.assertRaises(AdmissionError): harness._assignment(payload)
                    evidence = root / "assignments" / (aid + ".PRESTART_REJECTED.json")
                    cleanup = json.loads(evidence.read_text(encoding="utf-8")); self.assertEqual("REAPED", cleanup["outcome"])
                    self.assertEqual({"schema","assignment_id","reason","launcher_identity","controller_identity","handles_closed","launcher_reaped","controller_reaped","process_reaped","worktree_removed","branch_removed","channels_removed","outcome"},set(cleanup))
                    self.assertIsNone(cleanup["launcher_identity"]); self.assertIsNone(cleanup["controller_identity"])
                    self.assertTrue(all(cleanup[key] for key in ("handles_closed","launcher_reaped","controller_reaped","process_reaped","worktree_removed","branch_removed","channels_removed")))
                    self.assertFalse(worktree.exists()); self.assertFalse(any(path.exists() for path in channels)); self.assertNotIn(aid, harness.workers)
                    self.assertNotEqual(0, real_run(["git","show-ref","--verify","--quiet","refs/heads/" + branch], cwd=target).returncode); self.assertFalse(harness.registry_path.exists())
                    invocation = root / "assignments" / (aid + ".invocation.json")
                    if failure in {"open","popen"}: self.assertTrue(invocation.is_file()); self.assertEqual(64,len(_digest(invocation)))
                    if failure == "open": self.assertEqual(1,len(captured_stdout)); self.assertTrue(bool(getattr(captured_stdout[0], "closed", False)))

    def test_s25_a1_started_publication_ambiguity_gets_bound_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); aid = "a1"; target = root / "target"; harness.broker = AcceptanceBroker(root, harness.seed, harness.policy, harness.templates, harness.manifest, target_root=target); harness.broker.materialize_seed(target)
            process = _Process(); identity = {"pid":4242,"created_utc":"exact"}; created = datetime(2026,8,4,tzinfo=timezone.utc); snapshot = ProcessSnapshot(True,(ProcessInfo(4242,1,"python","lane",created),),(),"synthetic")
            def launch(command: list[str], **_: object) -> _Process:
                if command[:3] != [sys.executable, "-m", "orchestrator_harness.lane_controller"]: return real_popen(command, **_)
                status = Path(json.loads(Path(command[-1]).read_text(encoding="utf-8"))["output_paths"]["status"]); status.write_text(json.dumps({"schema":"orchestrator-lane-controller/v1","state":"RUNNING_CODEX","controller_pid":4242,"controller_created_utc":iso_utc(created)}), encoding="utf-8"); return process
            def exact(_: int) -> dict[str, object] | None: return identity if process.poll() is None else None
            original_append, persisted = c3._atomic_append, {"started":False}
            def ambiguous(path: Path, value: dict[str, object]) -> None:
                original_append(path, value)
                if value.get("state") == "STARTED" and not persisted["started"]: persisted["started"] = True; raise OSError("fsync acknowledgement ambiguous")
            payload = {"assignment_id":aid,"role":"F.C3.A1","sprint":"S25","task":"x","prompt":"x","target_id":"target","declared_resources":[]}
            real_popen = subprocess.Popen
            with patch.object(c3.subprocess,"Popen",side_effect=launch), patch.object(c3,"process_snapshot",return_value=snapshot), patch.object(c3,"exact_process_identity",side_effect=exact), patch.object(c3,"_atomic_append",side_effect=ambiguous):
                with self.assertRaises(AdmissionError): harness._assignment(payload)
            cleanup_path = root / "assignments" / (aid + ".PRESTART_REJECTED.json"); cleanup = json.loads(cleanup_path.read_text(encoding="utf-8")); self.assertEqual("REAPED",cleanup["outcome"])
            lines = [json.loads(line) for line in harness.registry_path.read_text().splitlines()]; self.assertEqual(["STARTED","TERMINAL"],[line["state"] for line in lines]); self.assertEqual({"path":str(cleanup_path),"sha256":_digest(cleanup_path)},lines[-1]["prestart_cleanup"])
            harness._recover_or_fail_closed()

    def test_s25_a1_reconciled_restart_emits_closed_status_and_allows_signed_shutdown_only(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); aid = "a1"; old, new = {"pid":4242,"created_utc":"old"}, {"pid":4242,"created_utc":"new"}
            controller = {"pid":4343,"created_utc":"controller"}; required = {"schema":"firmware-c3-prestart-rejection/v1","assignment_id":aid,"reason":"identity unresolved","launcher_identity":old,"controller_identity":controller,"handles_closed":True,"launcher_reaped":False,"controller_reaped":False,"process_reaped":False,"worktree_removed":False,"branch_removed":False,"channels_removed":False,"outcome":"RECOVERY_REQUIRED"}; recovery = {"schema":"firmware-c3-recovery-required/v1","assignment_id":aid,"launcher_pid":4242,"launcher_identity":old,"observed_launcher_identity":new,"controller_identity":controller,"worktree":str(root / "assignment-worktrees" / aid),"branch":"c3/target/a1","channels":[str(root / "worker-channel" / aid),str(root / "worker-channel-responses" / aid)],"cleanup":required}
            recovery_path = harness.state_root / "RECOVERY_REQUIRED.json"; recovery_path.write_text(json.dumps(recovery), encoding="utf-8"); cleanup_path = root / "assignments" / "a1.PRESTART_RECOVERY_RECONCILED.json"; cleanup_path.parent.mkdir(); reaped = {**required,"launcher_reaped":True,"controller_reaped":True,"process_reaped":True,"worktree_removed":True,"branch_removed":True,"channels_removed":True,"outcome":"REAPED"}; cleanup_path.write_text(json.dumps(reaped), encoding="utf-8")
            (harness.state_root / "RECOVERY_RECONCILED.json").write_text(json.dumps({"schema":"firmware-c3-recovery-reconciled/v1","recovery":{"path":str(recovery_path),"sha256":_digest(recovery_path)},"cleanup":{"path":str(cleanup_path),"sha256":_digest(cleanup_path)}}), encoding="utf-8")
            harness._recover_or_fail_closed(); self.assertTrue(harness.admission_closed); self.assertFalse((harness.state_root / "C3_HARNESS_READY.json").exists())
            c1, delegated = root / "c1.json", root / "delegated.json"; c1.write_text("{}",encoding="utf-8"); delegated.write_text("{}",encoding="utf-8")
            observed_payloads: list[bytes] = []
            class RecordingVerifier:
                def verify(self, payload: bytes, signature: str, key: str) -> bool:
                    observed_payloads.append(payload)
                    return signature == "signed" and key == "key" and bool(payload)
            harness.verifier = RecordingVerifier()
            def request(request_id: str, kind: str, payload: dict[str, object]) -> dict[str, object]:
                value = {"schema":"firmware-c3-harness-request/v1","request_id":request_id,"attempt_id":"attempt-1","c1_reference":{"path":str(c1),"sha256":_digest(c1)},"delegated_reference":{"path":str(delegated),"sha256":_digest(delegated),},"orchestrator_identity":harness.topology["identity_binding"],"topology_key_release":harness.topology["release"],"kind":kind,"issued_utc":"2026-08-05T00:00:00+00:00","issued_monotonic":0.0,"expires_monotonic":100.0,"payload":payload,"public_key":"key","signature":"signed"}
                (harness.request_root / (request_id + ".json")).write_text(json.dumps(value),encoding="utf-8")
                return value
            materialize_request = request("a-nonshutdown","materialize",{"target_id":"target"}); shutdown_request = request("b-shutdown","shutdown",{})
            harness.last_heartbeat = 0.0
            sleeps = {"count":0, "limit":3}
            def bounded_sleep(_: float) -> None:
                sleeps["count"] += 1
                if sleeps["count"] > sleeps["limit"]: raise AssertionError("shutdown did not terminate bounded C3 service loop")
            with patch.object(c3,"C3Harness",return_value=harness), patch.object(c3.time,"monotonic",return_value=31.0), patch.object(c3.time,"sleep",side_effect=bounded_sleep):
                self.assertEqual(0,c3.main(["--root",str(root),"--seed",str(harness.seed),"--policy",str(harness.policy),"--templates",str(harness.templates),"--topology-root",str(root),"--c1-path",str(c1),"--c1-sha256",_digest(c1),"--delegated-path",str(delegated),"--delegated-sha256",_digest(delegated),"--manifest",str(harness.manifest),"serve","--poll-seconds","0"]))
            self.assertLessEqual(sleeps["count"],sleeps["limit"]); self.assertEqual([c3.canonical_decision_payload(materialize_request),c3.canonical_decision_payload(shutdown_request)],observed_payloads)
            statuses = [json.loads(line)["state"] for line in harness.status_path.read_text().splitlines()]; self.assertGreaterEqual(statuses.count("RECOVERY_RECONCILED_CLOSED"),2); self.assertIn("SHUTDOWN",statuses); self.assertNotIn("READY",statuses)
            rejected = json.loads((harness.response_root / "a-nonshutdown.json").read_text()); self.assertEqual("REJECTED",rejected["outcome"]); self.assertIn("admission is closed",rejected["reason"]); self.assertFalse((root / "target").exists())
            self.assertEqual("ACCEPTED",json.loads((harness.response_root / "b-shutdown.json").read_text())["outcome"])


    def test_s26_a1_observation_brackets_opaque_identity_and_accepts_same_or_direct_shape(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); invocation = root / "a1.invocation.json"; invocation.write_text("{}", encoding="utf-8")
            created = datetime(2026, 8, 5, tzinfo=timezone.utc); stamp = iso_utc(created)
            launcher = ProcessInfo(100, 1, "python", "lossy posix command", created); controller = ProcessInfo(200, 100, "python", "windows argv", created)
            status = {"schema":"orchestrator-lane-controller/v1","state":"WAITING_RESOURCE","controller_pid":100,"controller_created_utc":stamp}; opaque = {"pid":100,"created_utc":"windows-filetime:133000000000000000"}
            with patch.object(c3, "exact_process_identity", side_effect=[opaque, opaque]):
                same = c3._controller_observation(ProcessSnapshot(True, (launcher,), (), "synthetic"), opaque, 100, status, json.dumps(status).encode(), root / "status.json", invocation, opaque)
            self.assertIsNotNone(same); self.assertTrue(c3._observation_authenticates(same)); self.assertEqual("same-process", same["shape"])
            direct_status = {**status,"controller_pid":200}; direct = {"pid":200,"created_utc":"windows-filetime:133000000000000001"}; launcher_identity = {"pid":100,"created_utc":"windows-filetime:133000000000000000"}
            with patch.object(c3, "exact_process_identity", side_effect=[direct, launcher_identity]), patch.object(c3, "lane_controller_command_matches", return_value=True) as matcher:
                observation = c3._controller_observation(ProcessSnapshot(True, (launcher, controller), (), "synthetic"), launcher_identity, 100, direct_status, json.dumps(direct_status).encode(), root / "status.json", invocation, direct)
                self.assertTrue(c3._observation_authenticates(observation)); matcher.assert_called_once()
            with patch.object(c3, "exact_process_identity", return_value={"pid":100,"created_utc":"windows-filetime:new"}):
                self.assertIsNone(c3._controller_observation(ProcessSnapshot(True, (launcher,), (), "synthetic"), opaque, 100, status, b"{}", root / "status.json", invocation, opaque))

    def test_s26_a1_windows_redirector_requires_exact_python_module_argv(self) -> None:
        class Function:
            def __init__(self, result: object) -> None: self.result = result
            def __call__(self, *_: object) -> object: return self.result
        def matches(argv: list[str]) -> bool:
            class Count:
                def __init__(self) -> None: self.value = len(argv)
            shell = SimpleNamespace(CommandLineToArgvW=Function(argv)); kernel = SimpleNamespace(LocalFree=Function(None))
            fake = SimpleNamespace(c_int=Count, POINTER=lambda _: object, c_wchar_p=str, c_void_p=object, byref=lambda value: value, cast=lambda value, _: value, WinDLL=lambda name, **_: shell if name == "shell32" else kernel)
            with patch.object(c3_process.os, "name", "nt"), patch.dict(sys.modules, {"ctypes":fake}):
                return c3_process.lane_controller_command_matches("direct-venv-redirector", "opaque", "C:/a path/a1.json")
        self.assertTrue(matches(["python.exe","-m","orchestrator_harness.lane_controller","C:/a path/a1.json"]))
        for argv in (["worker.exe","-m","orchestrator_harness.lane_controller","C:/a path/a1.json"], ["python.exe","-m","orchestrator_harness.lane_controller_extra","C:/a path/a1.json"], ["python.exe","-m","orchestrator_harness.lane_controller","C:/a path/a1.json.bak"], ["python.exe","orchestrator_harness.lane_controller","-m","C:/a path/a1.json"], ["python.exe","-m","orchestrator_harness.lane_controller","C:/a path/a1.json","extra"]):
            with self.subTest(argv=argv): self.assertFalse(matches(argv))

    def test_s26_a1_observation_rejects_incomplete_non_direct_and_invalid_initial_projection(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); invocation = root / "a1.invocation.json"; invocation.write_text("{}", encoding="utf-8")
            created = datetime(2026, 8, 5, tzinfo=timezone.utc); stamp = iso_utc(created); identity = {"pid":100,"created_utc":"windows-filetime:1"}; launcher = ProcessInfo(100, 1, "python", "x", created); child = ProcessInfo(200, 101, "python", "x", created)
            status = {"schema":"orchestrator-lane-controller/v1","state":"WAITING_RESOURCE","controller_pid":100,"controller_created_utc":stamp}
            with patch.object(c3, "exact_process_identity", return_value=identity):
                self.assertIsNone(c3._controller_observation(ProcessSnapshot(False, (launcher,), (), "synthetic"), identity, 100, status, b"{}", root / "status", invocation, identity))
                self.assertIsNone(c3._controller_observation(ProcessSnapshot(True, (launcher, child), (), "synthetic"), identity, 100, {**status,"controller_pid":200}, b"{}", root / "status", invocation, {"pid":200,"created_utc":"windows-filetime:2"}))
            for key, value in (("controller_pid", True), ("controller_pid", 0), ("controller_created_utc", ""), ("state", "CODEX_EXITED"), ("schema", "other")):
                with self.subTest(key=key, value=value), patch.object(c3, "exact_process_identity", side_effect=[identity, identity]):
                    candidate = c3._controller_observation(ProcessSnapshot(True, (launcher,), (), "synthetic"), identity, 100, {**status,key:value}, b"status", root / "status", invocation, identity)
                    self.assertTrue(candidate is None or not c3._observation_authenticates(candidate))

    def test_s26_a1_recovery_validates_before_persisting_and_allows_null_or_changed_launcher_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); process = _Process(); channels = (root / "worker-channel" / "a1", root / "worker-channel-responses" / "a1"); worktree = root / "assignment-worktrees" / "a1"
            cleanup = {"schema":"firmware-c3-prestart-rejection/v1","assignment_id":"a1","reason":"rejected","launcher_identity":None,"controller_identity":None,"handles_closed":True,"launcher_reaped":False,"controller_reaped":True,"process_reaped":False,"worktree_removed":False,"branch_removed":False,"channels_removed":False,"outcome":"RECOVERY_REQUIRED"}
            with patch.object(c3, "exact_process_identity", return_value=None), self.assertRaises(c3.RecoveryRequired): harness._require_recovery("a1", process, None, None, worktree, "c3/target/a1", channels, cleanup)
            record = json.loads((harness.state_root / "RECOVERY_REQUIRED.json").read_text()); self.assertEqual(process.pid, record["launcher_pid"])
            (harness.state_root / "RECOVERY_REQUIRED.json").unlink(); harness.recovery = None
            bad = {**cleanup,"launcher_identity":{"pid":999,"created_utc":"bad"}}
            with patch.object(c3, "exact_process_identity", return_value="not-an-identity"), self.assertRaises(AdmissionError): harness._require_recovery("a1", process, bad["launcher_identity"], None, worktree, "c3/target/a1", channels, bad)
            self.assertFalse((harness.state_root / "RECOVERY_REQUIRED.json").exists())

    def test_s26_a1_rejection_keeps_worktree_and_channels_when_launcher_exits_but_controller_survives(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); target = root / "target"; target.mkdir()
            worktree = root / "assignment-worktrees" / "a1"; worktree.mkdir(parents=True)
            channels = (root / "worker-channel" / "a1", root / "worker-channel-responses" / "a1")
            for channel in channels: channel.mkdir(parents=True)
            launcher, controller = {"pid":4242,"created_utc":"launcher"}, {"pid":4343,"created_utc":"controller"}
            process = _Process(0)
            with patch.object(c3, "exact_process_identity", side_effect=lambda pid: controller if pid == 4343 else None), patch.object(c3.subprocess, "run") as git:
                cleanup = harness._reject_unregistered_assignment("a1", process, launcher, controller, worktree, "c3/target/a1", target, channels, (), "controller survived launcher exit")
            self.assertEqual("RECOVERY_REQUIRED", cleanup["outcome"])
            self.assertFalse(cleanup["process_reaped"]); self.assertFalse(cleanup["controller_reaped"])
            self.assertTrue(worktree.exists()); self.assertTrue(all(channel.exists() for channel in channels)); git.assert_not_called()

    def test_s26_a1_terminal_popen_without_captured_identity_reaps_and_completion_keeps_legacy_exit_code(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); target = root / "target"; target.mkdir(); process = _Process(7)
            channels = (root / "worker-channel" / "a1", root / "worker-channel-responses" / "a1")
            with patch.object(c3.subprocess, "run", return_value=SimpleNamespace(returncode=1)):
                cleanup = harness._reject_unregistered_assignment("a1", process, None, None, root / "missing-worktree", "c3/target/a1", target, channels, (), "terminal handle")
            self.assertTrue(cleanup["launcher_reaped"])
            harness.workers = {"a1":process}; harness.assignments = {"a1":{"identity":None,"controller_identity":None,"worktree":root,"seed":{},"invocation":{"output_paths":{}},"status_identity":{}}}
            with patch.object(c3, "_protected_seed_snapshot", side_effect=AdmissionError("synthetic")):
                harness.reap_workers()
            completion = json.loads((root / "assignments" / "a1.WORKER_COMPLETION.json").read_text())
            self.assertEqual(7, completion["exit_code"]); self.assertEqual(7, completion["launcher_exit_code"])
            terminal = json.loads(harness.registry_path.read_text().splitlines()[-1]); self.assertEqual(7, terminal["exit_code"]); self.assertEqual(7, terminal["launcher_exit_code"])

    def test_s27_a1_recovery_retries_stay_artifact_free_until_terminal_reconciliation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); harness = self._bare(root); process = _Process(0)
            launcher, controller_identity = {"pid":4242,"created_utc":"windows-filetime:1"}, {"pid":4343,"created_utc":"controller"}
            recovery_path = harness.state_root / "RECOVERY_REQUIRED.json"; recovery_path.write_text(json.dumps({"synthetic":True}), encoding="utf-8")
            harness.recovery = {"assignment_id":"a1","process":process,"launcher_identity":launcher,"controller_identity":controller_identity,"worktree":str(root / "missing-worktree"),"branch":"c3/target/a1","channels":[str(root / "missing-inbox"),str(root / "missing-responses")],"path":str(recovery_path),"sha256":_digest(recovery_path)}
            controller_live = True
            def exact(pid: int) -> dict[str, object] | None:
                if pid == launcher["pid"]: return launcher
                return controller_identity if pid == controller_identity["pid"] and controller_live else None
            with patch.object(c3, "exact_process_identity", side_effect=exact), patch.object(c3.subprocess, "run", return_value=SimpleNamespace(returncode=1)):
                harness.reap_recovery(); harness.reap_recovery()
                cleanup_path = root / "assignments" / "a1.PRESTART_RECOVERY_RECONCILED.json"
                reconciled_path = harness.state_root / "RECOVERY_RECONCILED.json"
                self.assertFalse(cleanup_path.exists()); self.assertFalse(reconciled_path.exists())
                controller_live = False
                harness.reap_recovery()
                cleanup_bytes, reconciled_bytes = cleanup_path.read_bytes(), reconciled_path.read_bytes()
                self.assertEqual("REAPED", json.loads(cleanup_bytes)["outcome"])
                self.assertIsNone(harness.recovery); self.assertTrue(process.waited)
                harness.reap_recovery()
            self.assertEqual(cleanup_bytes, cleanup_path.read_bytes())
            self.assertEqual(reconciled_bytes, reconciled_path.read_bytes())


def _digest_token(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()
