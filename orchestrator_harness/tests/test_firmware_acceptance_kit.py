from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from firmware_acceptance.kit import AcceptanceBroker, AdmissionError, SignatureVerifier, canonical_bound_operation, canonical_sha256, evaluate_call, raw_result_sha256, validate_campaign_contract, validate_seed_manifest, worker_environment


class FirmwareAcceptanceKitTests(unittest.TestCase):
    class _Verifier(SignatureVerifier):
        def verify(self, payload: bytes, signature: str, public_key: str) -> bool:
            return signature == "sig" and public_key == "key" and bool(payload)

    def _complete_chain(self, broker: AcceptanceBroker, raw_payload: object, *, bound_digest: str | None = None) -> list[tuple[Path, str]]:
        digest = "PENDING" if bound_digest is None else bound_digest
        bound = {"resource":"STM-A","server_commit":"f003f84a7df51cd8595a3203c62e225b21da2a22","method":"reset_and_halt","method_version":1,"arguments":{"board_id":"STM-A"},"policy_sha256":"p","schema_sha256":"s","plan_sha256":"pl","permission_sha256":"pe","authorization_sha256":"a","claim_sha256":"c","call_id":"call-raw","attempt_id":"attempt-raw","lane_id":"STM-A","board":"STM-A","probe_uid":"uid","target":"STM32L476RG","profile":"stm","route":"rediscover","governing_hashes":{"goal":"g"},"c1_reference":{"path":"c1","sha256":"h"},"deadline_monotonic":100,"expires_monotonic":99,"seed_identity":{"manifest":"x"},"target_identity":{"commit":"y"},"raw_result_sha256":digest,"cleanup_owner":"C3-HARNESS"}
        bound |= {"max_operation_duration_seconds":30,"permission_granted":True,"authorization_path":"authorization","claim":{"resource":"STM-A","path":"claim","sha256":"c","owner":{"pid":1,"created_utc":"2026-01-01T00:00:00Z","creation_identity":"test"}},"controller_owner":{"pid":1,"created_utc":"2026-01-01T00:00:00Z","creation_identity":"test"},"governing_documents":{"goal":{"path":"goal","sha256":"g"}},"delegated_reference":{"path":"delegated","sha256":"d"},"board_identity":{"path":"board","sha256":"b"},"mcp_schema":{"path":"schema","sha256":"s"},"policy":{"path":"policy","sha256":"p"},"plan":{"path":"plan","sha256":"pl"},"permission":{"path":"permission","sha256":"pe"},"seed_identity":{"path":"seed","sha256":"x"},"target_identity":{"path":"target","sha256":"y"},"topology_key_release":{"path":"release","sha256":"r"}}
        bound["cleanup_owner"] = {"pid":1,"created_utc":"2026-01-01T00:00:00Z","creation_identity":"test"}
        common = {"attempt_id":"attempt-raw","lane_id":"STM-A","board":"STM-A","probe_uid":"uid","target":"STM32L476RG","profile":"stm","route":"rediscover","governing_hashes":{"goal":"g"},"c1_reference":{"path":"c1","sha256":"h"},"identity":{"controller":"pid:1"},"bound_operation":bound,"bound_operation_sha256":canonical_sha256(canonical_bound_operation(bound))}
        stages: list[tuple[Path, str]] = []
        for stage in ("proposal", "policy-evaluation", "signed-decision", "authorization", "dispatch-admission", "dispatch", "raw-result", "returning-state-cleanup", "result"):
            extra = {"schema":"firmware-o-decision/v3","proposal_path":"proposal","proposal_sha256":"proposal","call":{},"claim":{},"decision":"approve","rationale":"reviewed","issued_utc":"2026-01-01T00:00:00Z","issued_monotonic":1,"expires_monotonic":99,"topology_key_release":{},"orchestrator_identity":{},"signature":"sig","public_key":"key"} if stage == "signed-decision" else {}
            extra |= {"expires_monotonic":99} if stage == "authorization" else {}
            extra |= {"deadline_monotonic":100} if stage == "dispatch-admission" else {}
            extra |= {"raw_result":raw_payload,"outcome":"PASS"} if stage == "raw-result" else {}
            extra |= {"exact_reaped":True} if stage == "returning-state-cleanup" else {}
            stages.append(broker.record(stage, "call-raw", {**common, **extra}, (str(stages[-1][0]), stages[-1][1]) if stages else None))
            if stage == "raw-result":
                recorded = __import__("json").loads(stages[-1][0].read_text(encoding="utf-8"))
                common = {**common, "bound_operation": recorded["bound_operation"], "bound_operation_sha256": recorded["bound_operation_sha256"]}
        return stages

    def test_seed_is_exact_and_hash_bound(self) -> None:
        validate_seed_manifest(Path("firmware_acceptance/seed"))

    def test_controller_admission_is_bounded_and_fail_closed(self) -> None:
        call = {"call_id": "c1", "lane_id": "P3.STM", "board": "STM-A", "probe_uid": "uid", "target": "STM32L476RG", "profile": "stm", "method": "reset_and_halt", "arguments": {"board_id": "STM-A"}, "proposal_sha256": "a", "decision_sha256": "b", "authorization_sha256": "c", "deadline_monotonic": 100.0, "plan": {"max_operation_duration_seconds": 30}, "permission": {"granted": True}}
        self.assertEqual("ALLOW", evaluate_call(call, now_monotonic=1.0)["policy"])
        call["arguments"] = {"operation": "mass_erase"}
        with self.assertRaises(AdmissionError):
            evaluate_call(call, now_monotonic=1.0)

    def test_read_memory_policy_matches_pinned_signature_and_bounds(self) -> None:
        call = {"call_id": "m1", "lane_id": "P3.STM", "board": "STM-A", "probe_uid": "uid", "target": "STM32L476RG", "profile": "stm", "method": "read_memory_address", "arguments": {"board_id": "STM-A", "address": "0x20000000", "width": 32, "length": 4}, "proposal_sha256": "a", "decision_sha256": "b", "authorization_sha256": "c", "deadline_monotonic": 100.0, "plan": {"max_operation_duration_seconds": 30}, "permission": {"granted": True}}
        self.assertEqual("ALLOW", evaluate_call(call, now_monotonic=1.0)["policy"])
        call["arguments"] = {"board_id": "STM-A", "address": 0, "size": 4}
        with self.assertRaises(AdmissionError):
            evaluate_call(call, now_monotonic=1.0)
        call["arguments"] = {"board_id": "STM-A", "address": 0, "width": 64, "length": 4}
        with self.assertRaises(AdmissionError):
            evaluate_call(call, now_monotonic=1.0)

    def test_worker_environment_has_no_mcp_capability(self) -> None:
        env = worker_environment()
        self.assertEqual("", env["MCP_ENDPOINT"])
        self.assertEqual("", env["MCP_COMMAND"])

    def test_broker_materializes_seed_and_immutable_ordered_chain(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            broker = AcceptanceBroker(root / "broker", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
            target = root / "broker" / "targets" / "target"
            broker.materialize_seed(target)
            self.assertEqual(40, len(broker.validate_target(target)))
            config = broker.controller_config("STM-A", {})
            self.assertEqual("", config["worker_environment"]["MCP_ENDPOINT"])
            bound = {"resource":"STM-A","server_commit":"f003f84a7df51cd8595a3203c62e225b21da2a22","method":"reset_and_halt","method_version":1,"arguments":{"board_id":"STM-A"},"policy_sha256":"p","schema_sha256":"s","plan_sha256":"pl","permission_sha256":"pe","authorization_sha256":"a","claim_sha256":"c","call_id":"call-1","attempt_id":"attempt-0001","lane_id":"STM-A","board":"STM-A","probe_uid":"uid","target":"STM32L476RG","profile":"stm","route":"rediscover","governing_hashes":{"goal":"g"},"c1_reference":{"path":"c1","sha256":"h"},"deadline_monotonic":100,"expires_monotonic":99,"seed_identity":{"manifest":"x"},"target_identity":{"commit":"y"},"raw_result_sha256":"PENDING","cleanup_owner":"C3-HARNESS"}
            bound |= {"max_operation_duration_seconds":30,"permission_granted":True,"authorization_path":"authorization","claim":{"resource":"STM-A","path":"claim","sha256":"c","owner":{"pid":1,"created_utc":"2026-01-01T00:00:00Z","creation_identity":"test"}},"controller_owner":{"pid":1,"created_utc":"2026-01-01T00:00:00Z","creation_identity":"test"},"governing_documents":{"goal":{"path":"goal","sha256":"g"}},"delegated_reference":{"path":"delegated","sha256":"d"},"board_identity":{"path":"board","sha256":"b"},"mcp_schema":{"path":"schema","sha256":"s"},"policy":{"path":"policy","sha256":"p"},"plan":{"path":"plan","sha256":"pl"},"permission":{"path":"permission","sha256":"pe"},"seed_identity":{"path":"seed","sha256":"x"},"target_identity":{"path":"target","sha256":"y"},"topology_key_release":{"path":"release","sha256":"r"},"cleanup_owner":{"pid":1,"created_utc":"2026-01-01T00:00:00Z","creation_identity":"test"}}
            common = {"attempt_id": "attempt-0001", "lane_id": "STM-A", "board": "STM-A", "probe_uid": "uid", "target": "STM32L476RG", "profile": "stm", "route": "rediscover", "governing_hashes": {"goal": "g"}, "c1_reference": {"path": "c1", "sha256": "h"}, "identity": {"controller": "pid:1"}, "bound_operation": bound, "bound_operation_sha256": __import__("firmware_acceptance.kit", fromlist=["canonical_sha256"]).canonical_sha256(canonical_bound_operation(bound))}
            path, digest = broker.record("proposal", "call-1", common)
            broker.record("policy-evaluation", "call-1", common, (str(path), digest))
            with self.assertRaises(AdmissionError):
                broker.record("dispatch", "call-2", common)
            with self.assertRaises(AdmissionError):
                broker.record("dispatch", "call-1", common, (str(path), digest))

    def test_admission_accepts_hash_bound_retained_raw_result(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            broker = AcceptanceBroker(Path(temporary) / "broker", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
            stages = self._complete_chain(broker, {"mcp": {"result": "ok"}})
            self.assertEqual("PASS", broker.admit("call-raw", stages, 50, self._Verifier()))

    def test_admission_rejects_raw_result_digest_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            broker = AcceptanceBroker(Path(temporary) / "broker", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
            with self.assertRaises(AdmissionError):
                self._complete_chain(broker, {"mcp": {"result": "substituted"}}, bound_digest=raw_result_sha256({"mcp": {"result": "authorized"}}))

    def test_campaign_contract_is_closed_and_seed_rewrite_fails(self) -> None:
        validate_campaign_contract(Path("firmware_acceptance/seed"))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            broker = AcceptanceBroker(root / "broker", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
            target = root / "broker" / "targets" / "target"
            broker.materialize_seed(target)
            (target / "TARGET_CHARTER.md").chmod(0o600)
            (target / "TARGET_CHARTER.md").write_text("tampered", encoding="utf-8")
            with self.assertRaises(AdmissionError):
                broker.validate_target(target)

    def test_broker_rejects_ambient_capabilities_and_policy_is_data_driven(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            broker = AcceptanceBroker(root / "broker", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
            with self.assertRaises(AdmissionError):
                broker.controller_config("NRF-A", {"MCP_ENDPOINT": "forbidden"})
