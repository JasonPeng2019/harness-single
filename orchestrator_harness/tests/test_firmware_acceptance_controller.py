from __future__ import annotations

import io
import json
import tempfile
import hashlib
import subprocess
import sys
import unittest
from pathlib import Path

from firmware_acceptance.controller import FirmwareAcceptanceController
from firmware_acceptance.kit import AcceptanceBroker, AdmissionError, SignatureVerifier


class _Verifier(SignatureVerifier):
    def __init__(self, payload: bytes | None = None) -> None: self.payload = payload
    def verify(self, payload: bytes, signature: str, public_key: str) -> bool:
        return signature == "signed" and public_key == "public" and (self.payload is None or payload == self.payload)


class _Input(io.BytesIO):
    def close(self) -> None: pass


class _Process:
    def __init__(self, replies: list[dict[str, object]]) -> None:
        self.pid, self.stdin, self.stderr = 4242, _Input(), io.BytesIO()
        self.stdout = io.BytesIO(b"".join((json.dumps(reply) + "\n").encode() for reply in replies))
        self.exit: int | None = None
    def poll(self) -> int | None: return self.exit
    def terminate(self) -> None: self.exit = 0
    def wait(self, timeout: float | None = None) -> int: self.exit = 0; return 0


class _Claims:
    def __init__(self, root: Path) -> None:
        self.path = root / "claim.json"; self.live = False
    def acquire_all(self, resources: list[str], *, on_wait: object) -> None:
        self.path.write_text('{"claim":"live"}', encoding="utf-8"); self.live = True
    @property
    def held(self) -> list[dict[str, object]]:
        return [{"path": str(self.path), "owner": {"pid": 1}}] if self.live else []
    def release_all(self) -> list[str]: self.live = False; return []


class FirmwareAcceptanceControllerTests(unittest.TestCase):
    def _controller(self, root: Path, replies: list[dict[str, object]], launches: list[_Process], configs: list[dict[str, object]] | None = None) -> FirmwareAcceptanceController:
        broker = AcceptanceBroker(root / "broker", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
        def launch(config: dict[str, object]) -> _Process:
            if configs is not None: configs.append(config)
            process = _Process(replies); launches.append(process); return process
        return FirmwareAcceptanceController(broker, launcher=launch, clock=lambda: 1.0, identity_provider=lambda pid: {"pid": pid, "created_utc": "synthetic"}, claims_factory=lambda _lane, _call: _Claims(root))

    @staticmethod
    def _request() -> dict[str, object]:
        call = {"call_id":"controller-1","lane_id":"STM-A","board":"STM-A","probe_uid":"uid","target":"STM32L476RG","profile":"stm","method":"reset_and_halt","arguments":{"board_id":"STM-A"},"proposal_sha256":"p","decision_sha256":"d","authorization_sha256":"a","deadline_monotonic":100.0,"plan":{"max_operation_duration_seconds":30},"permission":{"granted":True}}
        return {"call":call,"lane_id":"STM-A","claim":"lane:STM-A"}

    def test_fake_stdio_success_is_controller_owned(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            launches: list[_Process] = []
            configs: list[dict[str, object]] = []
            controller = self._controller(Path(temporary), [{"jsonrpc":"2.0","id":1,"result":{}},{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"ok"}]}}], launches, configs)
            proposal = controller.create_proposal(self._request())
            result = controller.execute(proposal, {"proposal_sha256":proposal["sha256"],"signature":"signed","public_key":"public"}, {"proposal_sha256":proposal["sha256"],"expires_monotonic":99,"delegated_authority":"grant"}, _Verifier(proposal["sha256"].encode()))
            self.assertEqual("PASS", result["outcome"])
            self.assertEqual("", result["worker_environment"]["MCP_ENDPOINT"])
            self.assertTrue(launches[0].poll() is not None)
            self.assertIn(b'"method":"tools/call"', launches[0].stdin.getvalue())
            self.assertIn(b'"method":"notifications/initialized"', launches[0].stdin.getvalue())
            self.assertTrue({"BYO_MCP_ARTIFACT_ROOT", "PYOCD_PROBE_UID", "PYOCD_TARGET", "PYTHONPYCACHEPREFIX"} <= set(configs[0]["environment"]))

    def test_no_launch_before_signature_admission(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            launches: list[_Process] = []
            controller = self._controller(Path(temporary), [], launches); proposal = controller.create_proposal(self._request())
            with self.assertRaises(AdmissionError):
                controller.execute(proposal, {"proposal_sha256":proposal["sha256"],"signature":"bad","public_key":"public"}, {"proposal_sha256":proposal["sha256"],"expires_monotonic":99,"delegated_authority":"grant"}, _Verifier())
            self.assertEqual([], launches)

    def test_durable_proposal_mutation_rejects_before_launch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches = Path(temporary), []
            controller = self._controller(root, [], launches)
            proposal_path = root / "broker" / "proposal.json"
            proposal = controller.publish_proposal(proposal_path, self._request())
            mutated = json.loads(proposal_path.read_text(encoding="utf-8")); mutated["request"]["call"]["method"] = "reset_and_run"
            proposal_path.chmod(0o600); proposal_path.write_text(json.dumps(mutated), encoding="utf-8")
            decision_path, auth_path = root / "broker" / "decision.json", root / "broker" / "authorization.json"
            decision_path.write_text(json.dumps({"proposal_sha256": proposal["sha256"], "signature": "signed", "public_key": "public"}), encoding="utf-8")
            auth_path.write_text(json.dumps({"proposal_sha256": proposal["sha256"], "expires_monotonic": 99, "delegated_authority": "grant"}), encoding="utf-8")
            with self.assertRaises(AdmissionError): controller.execute_artifacts(proposal_path, decision_path, auth_path, _Verifier())
            self.assertEqual([], launches)

    def test_direct_capability_is_denied(self) -> None:
        request = self._request(); request["call"]["endpoint"] = "stdio://bypass"  # type: ignore[index]
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(AdmissionError): self._controller(Path(temporary), [], []).create_proposal(request)

    def test_failure_reaps_exact_child(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            launches: list[_Process] = []
            controller = self._controller(Path(temporary), [{"jsonrpc":"2.0","id":1,"result":{}}, {"jsonrpc":"2.0","id":2,"error":{}}], launches); proposal = controller.create_proposal(self._request())
            with self.assertRaises(AdmissionError):
                controller.execute(proposal, {"proposal_sha256":proposal["sha256"],"signature":"signed","public_key":"public"}, {"proposal_sha256":proposal["sha256"],"expires_monotonic":99,"delegated_authority":"grant"}, _Verifier())
            self.assertEqual(0, launches[0].poll())

    def test_silent_stdout_is_bounded_and_claim_is_released(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            launches: list[_Process] = []
            controller = self._controller(Path(temporary), [], launches); controller.io_timeout = 0.01
            proposal = controller.create_proposal(self._request())
            with self.assertRaises(AdmissionError):
                controller.execute(proposal, {"proposal_sha256":proposal["sha256"],"signature":"signed","public_key":"public"}, {"proposal_sha256":proposal["sha256"],"expires_monotonic":99,"delegated_authority":"grant"}, _Verifier())

    def test_real_subprocess_transport_sends_initialize_notification_and_call(self) -> None:
        program = """import sys,json
for line in sys.stdin:
 x=json.loads(line)
 if x.get('id') == 1: print(json.dumps({'jsonrpc':'2.0','id':1,'result':{}}),flush=True)
 if x.get('id') == 2: print(json.dumps({'jsonrpc':'2.0','id':2,'result':{'ok':True}}),flush=True)
"""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); broker = AcceptanceBroker(root / "broker", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
            controller = FirmwareAcceptanceController(broker, launcher=lambda _: subprocess.Popen([sys.executable, "-u", "-c", program], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE), clock=lambda: 1.0, identity_provider=lambda pid: {"pid":pid,"created_utc":"synthetic"}, claims_factory=lambda _l, _c: _Claims(root))
            proposal = controller.create_proposal(self._request())
            result = controller.execute(proposal, {"proposal_sha256":proposal["sha256"],"signature":"signed","public_key":"public"}, {"proposal_sha256":proposal["sha256"],"expires_monotonic":99,"delegated_authority":"grant"}, _Verifier())
            self.assertEqual("PASS", result["outcome"])
