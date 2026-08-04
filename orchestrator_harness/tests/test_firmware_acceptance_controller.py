from __future__ import annotations

import base64
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from firmware_acceptance.controller import Ed25519Verifier, FirmwareAcceptanceController
from firmware_acceptance.kit import AcceptanceBroker, AdmissionError, SignatureVerifier, canonical_decision_payload, canonical_sha256


class _Input(io.BytesIO):
    def close(self) -> None: pass


class _Process:
    def __init__(self, replies: list[dict[str, object]], stderr: bytes = b"") -> None:
        self.pid, self.stdin, self.stderr = 4242, _Input(), io.BytesIO(stderr)
        self.stdout = io.BytesIO(b"".join((json.dumps(reply) + "\n").encode() for reply in replies)); self.exit: int | None = None
    def poll(self) -> int | None: return self.exit
    def terminate(self) -> None: self.exit = 0
    def wait(self, timeout: float | None = None) -> int: self.exit = 0; return 0


class _Claims:
    def __init__(self, root: Path) -> None: self.path, self.live = root / "claim.json", False
    def acquire_all(self, resources: list[str], *, on_wait: object) -> None: self.path.write_text('{"claim":"live"}', encoding="utf-8"); self.live = True
    @property
    def held(self) -> list[dict[str, object]]: return [{"resource": "STM-A", "path": str(self.path), "owner": {"pid": 1}}] if self.live else []
    def release_all(self) -> list[str]: self.live = False; return []


class _ExactVerifier(SignatureVerifier):
    def __init__(self, expected: bytes) -> None: self.expected = expected
    def verify(self, payload: bytes, signature: str, public_key: str) -> bool: return payload == self.expected and signature == "signed" and public_key == "public"


class FirmwareAcceptanceControllerTests(unittest.TestCase):
    def _controller(self, root: Path, replies: list[dict[str, object]], launches: list[_Process], claims: list[_Claims]) -> FirmwareAcceptanceController:
        broker = AcceptanceBroker(root / "broker", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
        def launch(_: dict[str, object]) -> _Process:
            process = _Process(replies); launches.append(process); return process
        def factory(_: str, __: str) -> _Claims:
            item = _Claims(root); claims.append(item); return item
        return FirmwareAcceptanceController(broker, launcher=launch, clock=lambda: 1.0, identity_provider=lambda pid: {"pid": pid, "created_utc": "synthetic"}, claims_factory=factory)

    @staticmethod
    def _call(root: Path) -> dict[str, object]:
        ref = lambda name: {"path": str(root / (name + ".json")), "sha256": name * 8}
        return {"call_id":"controller-1","attempt_id":"attempt-1","lane_id":"STM-A","board":"STM-A","probe_uid":"uid","target":"STM32L476RG","profile":"stm","route":None,"method":"reset_and_halt","method_version":1,"arguments":{"board_id":"STM-A"},"proposal_sha256":"p","decision_sha256":"d","authorization_sha256":"a","deadline_monotonic":100.0,"plan":{"path":"plan","sha256":"planhash","max_operation_duration_seconds":30},"permission":{"path":"permission","sha256":"permissionhash","granted":True},"c1_reference":ref("c1"),"delegated_reference":ref("delegated"),"topology":ref("topology"),"claim":{"path":str(root / "claim.json"),"sha256":hashlib.sha256(b'{"claim":"live"}').hexdigest()},"governing_hashes":{"goal":"g","plan":"p","readiness":"r","policy":"m","topology":"t"},"seed_identity":ref("seed"),"target_identity":ref("target")}

    def _artifacts(self, root: Path, controller: FirmwareAcceptanceController) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
        proposal = controller.create_proposal({"call": self._call(root)})
        call = proposal["call"]
        decision: dict[str, object] = {"schema":"firmware-o-decision/v2","proposal_path":"proposal","proposal_sha256":proposal["sha256"],"call":call,"decision":"approve","issued_monotonic":1.0,"expires_monotonic":99.0,"topology":call["topology"],"public_key":"public","signature":"signed"}
        authorization = {"schema":"firmware-derived-authorization/v2","proposal_path":"proposal","proposal_sha256":proposal["sha256"],"decision_path":"decision","decision_sha256":canonical_sha256(decision),"call":call,"expires_monotonic":99.0,"one_shot_id":"controller-1","revoked":False}
        return proposal, decision, authorization

    def test_exact_canonical_decision_payload_is_used_before_launch_and_admission(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims = Path(temporary), [], []
            controller = self._controller(root, [{"jsonrpc":"2.0","id":1,"result":{}},{"jsonrpc":"2.0","id":2,"result":{"ok":True}}], launches, claims)
            proposal, decision, authorization = self._artifacts(root, controller)
            result = controller.execute(proposal, decision, authorization, _ExactVerifier(canonical_decision_payload(decision)))
            self.assertEqual("PASS", result["outcome"]); self.assertTrue(claims[0].live is False); self.assertEqual(0, launches[0].poll())

    def test_real_ed25519_verifier_rejects_payload_mutation(self) -> None:
        key = Ed25519PrivateKey.generate(); public = base64.b64encode(key.public_key().public_bytes_raw()).decode()
        decision = {"schema":"x","signature":""}; decision["signature"] = base64.b64encode(key.sign(canonical_decision_payload(decision))).decode()
        self.assertTrue(Ed25519Verifier().verify(canonical_decision_payload(decision), decision["signature"], public))
        self.assertFalse(Ed25519Verifier().verify(b"different", decision["signature"], public))

    def test_deny_replay_revocation_and_claim_mismatch_do_not_launch(self) -> None:
        for mutation in ("deny", "replay", "revoked", "claim"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                root, launches, claims = Path(temporary), [], []
                controller = self._controller(root, [], launches, claims); proposal, decision, authorization = self._artifacts(root, controller)
                if mutation == "deny": decision["decision"] = "deny"
                if mutation == "replay": authorization["one_shot_id"] = "other"
                if mutation == "revoked": authorization["revoked"] = True
                if mutation == "claim": proposal["call"]["claim"]["sha256"] = "wrong"  # type: ignore[index]
                with self.assertRaises(AdmissionError): controller.execute(proposal, decision, authorization, _ExactVerifier(canonical_decision_payload(decision)))
                self.assertEqual([], launches)

    def test_silent_stdout_reaps_and_releases_claim(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims = Path(temporary), [], []
            controller = self._controller(root, [], launches, claims); controller.io_timeout = .01
            proposal, decision, authorization = self._artifacts(root, controller)
            with self.assertRaises(AdmissionError): controller.execute(proposal, decision, authorization, _ExactVerifier(canonical_decision_payload(decision)))
            self.assertEqual(0, launches[0].poll()); self.assertFalse(claims[0].live)
