from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from firmware_acceptance.controller import FirmwareAcceptanceController, _StdioTransport, _process_identity
from firmware_acceptance.kit import AcceptanceBroker, AdmissionError, SignatureVerifier, canonical_decision_payload


class _Input(io.BytesIO):
    def close(self) -> None: pass


class _Process:
    def __init__(self) -> None:
        self.pid, self.stdin, self.stderr = 4242, _Input(), io.BytesIO()
        self.stdout = io.BytesIO(b'{"jsonrpc":"2.0","id":1,"result":{}}\n{"jsonrpc":"2.0","id":2,"result":{"ok":true}}\n')
        self.exit: int | None = None
        self.terminate_exits, self.wait_timeouts = True, 0
        self.terminated, self.killed = False, False
    def poll(self) -> int | None: return self.exit
    def terminate(self) -> None: self.terminated = True; self.exit = 0 if self.terminate_exits else None
    def kill(self) -> None: self.killed = True; self.exit = -9
    def wait(self, timeout: float | None = None) -> int:
        if self.wait_timeouts: self.wait_timeouts -= 1; raise subprocess.TimeoutExpired("fake", timeout)
        self.exit = 0 if self.exit is None else self.exit; return self.exit


class _Claims:
    def __init__(self, root: Path) -> None: self.path, self.live = root / "claim.json", False
    def acquire_all(self, resources: list[str], *, on_wait: object) -> None: self.path.write_text('{"claim":"live"}', encoding="utf-8"); self.live = True
    @property
    def held(self) -> list[dict[str, object]]: return [{"resource": "STM-A", "path": str(self.path), "owner": {"pid": 1}}] if self.live else []
    def release_all(self) -> list[str]: self.live = False; return []


class _Verifier(SignatureVerifier):
    def verify(self, payload: bytes, signature: str, public_key: str) -> bool:
        return signature == "signed" and public_key == "public" and payload == self.expected
    expected = b""


class FirmwareAcceptanceControllerTests(unittest.TestCase):
    def _controller(self, root: Path, launches: list[_Process], claims: list[_Claims]) -> FirmwareAcceptanceController:
        broker = AcceptanceBroker(root / "broker", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
        def launch(_: dict[str, object]) -> _Process: process = _Process(); launches.append(process); return process
        def factory(_: str, __: str) -> _Claims: item = _Claims(root); claims.append(item); return item
        return FirmwareAcceptanceController(broker, launcher=launch, clock=lambda: 1.0, identity_provider=lambda pid: None if launches and launches[-1].exit is not None else {"pid":pid,"created_utc":"synthetic"}, claims_factory=factory)

    @staticmethod
    def _call(root: Path) -> dict[str, object]:
        def ref(name: str) -> dict[str, str]:
            path = root / (name + ".json"); path.write_text(name, encoding="utf-8")
            return {"path":str(path), "sha256":hashlib.sha256(path.read_bytes()).hexdigest()}
        policy = Path("firmware_acceptance/MCP_METHOD_POLICY.json").resolve()
        plan, permission = ref("plan"), ref("permission")
        plan["max_operation_duration_seconds"] = 30  # type: ignore[index]
        permission["granted"] = True  # type: ignore[index]
        governing = {name: ref(name) for name in ("goal", "generalization_spec", "implementation_roadmap", "execution_plan", "execution_readiness")}
        return {"call_id":"controller-1","attempt_id":"attempt-1","lane_id":"STM-A","board":"STM-A","resource":"STM-A","probe_uid":"uid","target":"STM32L476RG","profile":"stm","route":None,"method":"reset_and_halt","method_version":1,"arguments":{"board_id":"STM-A"},"deadline_monotonic":100.0,"plan":plan,"permission":permission,"c1_reference":ref("c1"),"delegated_reference":ref("delegated"),"board_identity":ref("board"),"mcp_schema":ref("schema"),"policy":{"path":str(policy),"sha256":hashlib.sha256(policy.read_bytes()).hexdigest()},"server_revision":"f003f84a7df51cd8595a3203c62e225b21da2a22","seed_identity":ref("seed"),"target_identity":ref("target"),"topology_key_release":ref("release"),"governing_documents":governing}

    def _flow(self, root: Path, controller: FirmwareAcceptanceController, verifier: _Verifier) -> tuple[Path, Path, Path]:
        proposal_path = root / "broker" / "proposal.json"; proposal = controller.publish_proposal(proposal_path, {"call":self._call(root)})
        self.assertTrue(controller._live_claims.held)  # O signs only after exact claim is live.
        self.assertFalse({"proposal_sha256","decision_sha256","authorization_sha256"} & set(proposal["call"]))
        decision_path = root / "broker" / "decision.json"
        decision = {"schema":"firmware-o-decision/v3","proposal_path":str(proposal_path.resolve()),"proposal_sha256":proposal["raw_sha256"],"call":proposal["call"],"claim":proposal["claim"],"decision":"approve","rationale":"reviewed","issued_utc":"2026-01-01T00:00:00Z","issued_monotonic":1.0,"expires_monotonic":99.0,"topology_key_release":proposal["call"]["topology_key_release"],"orchestrator_identity":{"path":"identity","sha256":"identity"},"public_key":"public","signature":"signed"}
        verifier.expected = canonical_decision_payload(decision); decision_path.write_text(json.dumps(decision, sort_keys=True, separators=(",",":")), encoding="utf-8")
        authorization_path = root / "broker" / "authorization.json"; controller.derive_authorization(proposal_path, decision_path, authorization_path, verifier)
        return proposal_path, decision_path, authorization_path

    def test_begin_sign_derive_authorize_and_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
            paths = self._flow(root, controller, verifier)
            result = controller.execute_artifacts(*paths, verifier)
            self.assertEqual("PASS", result["outcome"]); self.assertFalse(claims[0].live); self.assertEqual(1, len(launches))
            self.assertIsNone(controller._live_claims)
            messages = [json.loads(line) for line in launches[0].stdin.getvalue().splitlines()]
            self.assertEqual(["initialize", "notifications/initialized", "tools/call"], [item["method"] for item in messages[:3]])
            self.assertEqual([1, None, 2], [item.get("id") for item in messages[:3]])

    def test_single_lifecycle_retains_the_publishing_controller_until_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
            request = root / "request.json"; request.write_text(json.dumps({"call": self._call(root)}), encoding="utf-8")
            proposal, decision, authorization, result = (root / "broker" / name for name in ("proposal.json", "decision.json", "authorization.json", "result.json"))
            def observe(path: Path) -> None:
                published = json.loads(proposal.read_text())
                self.assertIsNotNone(controller._live_claims)
                signed = {"schema":"firmware-o-decision/v3","proposal_path":str(proposal.resolve()),"proposal_sha256":hashlib.sha256(proposal.read_bytes()).hexdigest(),"call":published["call"],"claim":published["claim"],"decision":"approve","rationale":"reviewed","issued_utc":"2026-01-01T00:00:00Z","issued_monotonic":1.0,"expires_monotonic":99.0,"topology_key_release":published["call"]["topology_key_release"],"orchestrator_identity":{"path":"identity","sha256":"identity"},"public_key":"public","signature":"signed"}
                verifier.expected = canonical_decision_payload(signed); path.write_text(json.dumps(signed, sort_keys=True, separators=(",", ":")), encoding="utf-8")
            outcome = controller.run_lifecycle(request, proposal, decision, authorization, verifier, wait_for_decision=observe, result_path=result)
            self.assertEqual("PASS", outcome["outcome"]); self.assertTrue(result.is_file()); self.assertFalse(claims[0].live)

    def test_path_hash_owner_claim_and_replay_mismatch_deny_before_launch(self) -> None:
        for mutation in ("path", "hash", "owner", "claim", "replay", "deny"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
                proposal, decision, authorization = self._flow(root, controller, verifier)
                value = json.loads(decision.read_text())
                if mutation == "path": value["proposal_path"] = "other"
                elif mutation == "hash": value["proposal_sha256"] = "bad"
                elif mutation == "claim": value["claim"]["resource"] = "other"
                elif mutation == "replay":
                    value = json.loads(authorization.read_text()); value["one_shot_id"] = "other"; authorization.write_text(json.dumps(value), encoding="utf-8")
                elif mutation == "deny": value["decision"] = "deny"
                elif mutation == "owner": controller._live_claim["owner"] = {"pid":2}
                if mutation not in ("replay", "owner"): decision.write_text(json.dumps(value, sort_keys=True, separators=(",",":")), encoding="utf-8")
                with self.assertRaises(AdmissionError): controller.execute_artifacts(proposal, decision, authorization, verifier)
                self.assertEqual([], launches); self.assertFalse(claims[0].live)

    def test_record_config_result_and_admit_errors_release_retained_claim(self) -> None:
        for failure in ("record", "config", "result", "admit"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary:
                root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
                paths = self._flow(root, controller, verifier)
                original_record, original_config, original_admit = controller.broker.record, controller.broker.controller_config, controller.broker.admit
                if failure == "record": controller.broker.record = lambda *args, **kwargs: (_ for _ in ()).throw(AdmissionError("record"))  # type: ignore[method-assign]
                elif failure == "config": controller.broker.controller_config = lambda *args, **kwargs: (_ for _ in ()).throw(AdmissionError("config"))  # type: ignore[method-assign]
                elif failure == "result":
                    controller.broker.record = lambda stage, *args, **kwargs: (_ for _ in ()).throw(AdmissionError("result")) if stage == "result" else original_record(stage, *args, **kwargs)  # type: ignore[method-assign]
                else: controller.broker.admit = lambda *args, **kwargs: (_ for _ in ()).throw(AdmissionError("admit"))  # type: ignore[method-assign]
                with self.assertRaises(AdmissionError): controller.execute_artifacts(*paths, verifier)
                self.assertFalse(claims[0].live); self.assertIsNone(controller._live_claims)

    def test_transport_rejects_malformed_wrong_id_error_and_eof_and_joins_helpers(self) -> None:
        for frame in (b"not-json\n", b'{"jsonrpc":"2.0","id":9,"result":{}}\n', b'{"jsonrpc":"2.0","id":1,"error":{"code":1}}\n', b""):
            with self.subTest(frame=frame):
                process = _Process(); process.stdout = io.BytesIO(frame)
                transport = _StdioTransport(process, lambda: 1.0, 0.2)
                with self.assertRaises(AdmissionError): transport.receive(1)
                stopped, cleanup = transport.close_and_join()
                self.assertTrue(stopped); self.assertTrue(all(item["stopped"] for item in cleanup["helper_threads"]))

    def test_transport_drains_pipe_sized_stderr_without_blocking_and_serializes_writes(self) -> None:
        process = _Process(); process.stdout = io.BytesIO(b'{"jsonrpc":"2.0","id":1,"result":{}}\n')
        process.stderr = io.BytesIO(b"x" * (128 * 1024))
        transport = _StdioTransport(process, lambda: 1.0, 0.2)
        transport.send({"jsonrpc":"2.0", "id":1, "method":"one", "params":{}}, "one")
        self.assertEqual(1, transport.receive(1)["id"])
        time.sleep(0.03)
        stopped, cleanup = transport.close_and_join()
        self.assertTrue(stopped); self.assertEqual(64 * 1024, len(transport.stderr_bytes))

    def test_failed_transport_commits_raw_failure_and_cleanup_before_claim_release(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
            paths = self._flow(root, controller, verifier)
            bad = _Process(); bad.stdout = io.BytesIO(b"not-json\n"); launches.append(bad)
            controller.launcher = lambda _: bad
            with self.assertRaises(AdmissionError) as caught: controller.execute_artifacts(*paths, verifier)
            self.assertEqual("MCP response or exact cleanup failed", str(caught.exception))
            raw = json.loads((controller.broker.root / "calls" / "controller-1" / "06-raw-result.json").read_text())
            cleanup = json.loads((controller.broker.root / "calls" / "controller-1" / "07-returning-state-cleanup.json").read_text())
            self.assertEqual("FAIL", raw["outcome"]); self.assertEqual("failure", cleanup["classification"])
            self.assertTrue(cleanup["exact_reaped"]); self.assertFalse(claims[0].live); self.assertIsNone(controller._live_claims)

    def test_terminate_timeout_escalates_to_kill_and_reaps_captured_handle(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
            paths = self._flow(root, controller, verifier)
            process = _Process(); process.terminate_exits = False; process.wait_timeouts = 1; launches.append(process)
            controller.launcher = lambda _: process
            result = controller.execute_artifacts(*paths, verifier)
            cleanup = json.loads((controller.broker.root / "calls" / "controller-1" / "07-returning-state-cleanup.json").read_text())
            self.assertEqual("PASS", result["outcome"]); self.assertTrue(process.terminated); self.assertTrue(process.killed)
            self.assertEqual(["terminate", "kill"], cleanup["attempts"]); self.assertTrue(cleanup["exact_reaped"])

    def test_same_identity_after_wait_is_cleanup_ambiguity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
            paths = self._flow(root, controller, verifier)
            process = _Process(); launches.append(process); controller.launcher = lambda _: process
            controller.identity_provider = lambda pid: {"pid": pid, "created_utc": "unchanged"}
            with self.assertRaises(AdmissionError): controller.execute_artifacts(*paths, verifier)
            cleanup = json.loads((controller.broker.root / "calls" / "controller-1" / "07-returning-state-cleanup.json").read_text())
            self.assertIn("cleanup_error", cleanup); self.assertFalse(cleanup["exact_reaped"]); self.assertFalse(claims[0].live)

    def test_launcher_failure_writes_failure_cleanup_and_releases_claim(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
            paths = self._flow(root, controller, verifier)
            controller.launcher = lambda _: (_ for _ in ()).throw(OSError("launch"))
            with self.assertRaises(AdmissionError): controller.execute_artifacts(*paths, verifier)
            self.assertTrue((controller.broker.root / "calls" / "controller-1" / "07-returning-state-cleanup.json").is_file())
            self.assertFalse(claims[0].live); self.assertIsNone(controller._live_claims)

    def test_default_identity_provider_proves_tiny_subprocess_reaped(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
            paths = self._flow(root, controller, verifier)
            fixture = "import sys,json\nfor line in sys.stdin:\n v=json.loads(line)\n if 'id' in v: print(json.dumps({'jsonrpc':'2.0','id':v['id'],'result':{}}),flush=True)\n"
            controller.identity_provider = _process_identity
            controller.launcher = lambda _: subprocess.Popen([sys.executable, "-c", fixture], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            result = controller.execute_artifacts(*paths, verifier)
            cleanup = json.loads((controller.broker.root / "calls" / "controller-1" / "07-returning-state-cleanup.json").read_text())
            self.assertEqual("PASS", result["outcome"]); self.assertTrue(cleanup["initial_process_identity"])
            self.assertIsNone(cleanup["post_reap_identity"]); self.assertTrue(cleanup["exact_reaped"])
