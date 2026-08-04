from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import firmware_acceptance.kit as kit
from firmware_acceptance.kit import AcceptanceBroker, AdmissionError, SignatureVerifier, _USER_ISSUED_SCOPE, canonical_bound_operation, canonical_sha256, evaluate_call, raw_result_sha256, validate_campaign_contract, validate_delegated_authorization, validate_seed_manifest, worker_environment
from orchestrator_harness.tests.support import TemporaryGitRepository


class FirmwareAcceptanceKitTests(unittest.TestCase):
    class _Verifier(SignatureVerifier):
        def verify(self, payload: bytes, signature: str, public_key: str) -> bool:
            return signature == "sig" and public_key == "key" and bool(payload)

    @staticmethod
    def _scope(action_class: str) -> dict[str, object]:
        return {"delegated_user_scope_sha256": canonical_sha256(_USER_ISSUED_SCOPE), "action_class": action_class, "scope_effect": {"schema":"firmware-call-effect/v1","effect_action_class":None,"target_operation_manifest":None,"electronic_admission":None,"limits":None}}

    def _complete_chain(self, broker: AcceptanceBroker, raw_payload: object, *, bound_digest: str | None = None, raw_outcome: str = "PASS") -> list[tuple[Path, str]]:
        digest = "PENDING" if bound_digest is None else bound_digest
        bound = {"resource":"STM-A","server_commit":"f003f84a7df51cd8595a3203c62e225b21da2a22","method":"reset_and_halt","method_version":1,"arguments":{"board_id":"STM-A"},"policy_sha256":"p","schema_sha256":"s","plan_sha256":"pl","permission_sha256":"pe","authorization_sha256":"a","claim_sha256":"c","call_id":"call-raw","attempt_id":"attempt-raw","lane_id":"STM-A","board":"STM-A","probe_uid":"uid","target":"STM32L476RG","profile":"stm","route":"rediscover","governing_hashes":{"goal":"g"},"c1_reference":{"path":"c1","sha256":"h"},"deadline_monotonic":100,"expires_monotonic":99,"seed_identity":{"manifest":"x"},"target_identity":{"commit":"y"},"raw_result_sha256":digest,"cleanup_owner":"C3-HARNESS"}
        bound |= {"max_operation_duration_seconds":30,"permission_granted":True,"authorization_path":"authorization","claim":{"resource":"STM-A","path":"claim","sha256":"c","owner":{"pid":1,"created_utc":"2026-01-01T00:00:00Z","creation_identity":"test"}},"controller_owner":{"pid":1,"created_utc":"2026-01-01T00:00:00Z","creation_identity":"test"},"governing_documents":{"goal":{"path":"goal","sha256":"g"}},"delegated_reference":{"path":"delegated","sha256":"d"},"board_identity":{"path":"board","sha256":"b"},"mcp_schema":{"path":"schema","sha256":"s"},"policy":{"path":"policy","sha256":"p"},"plan":{"path":"plan","sha256":"pl"},"permission":{"path":"permission","sha256":"pe"},"seed_identity":{"path":"seed","sha256":"x"},"target_identity":{"path":"target","sha256":"y"},"topology_key_release":{"path":"release","sha256":"r"}}
        bound["cleanup_owner"] = {"pid":1,"created_utc":"2026-01-01T00:00:00Z","creation_identity":"test"}
        bound |= self._scope("reset")
        common = {"attempt_id":"attempt-raw","lane_id":"STM-A","board":"STM-A","probe_uid":"uid","target":"STM32L476RG","profile":"stm","route":"rediscover","governing_hashes":{"goal":"g"},"c1_reference":{"path":"c1","sha256":"h"},"identity":{"controller":"pid:1"},"bound_operation":bound,"bound_operation_sha256":canonical_sha256(canonical_bound_operation(bound))}
        stages: list[tuple[Path, str]] = []
        for stage in ("proposal", "policy-evaluation", "signed-decision", "authorization", "dispatch-admission", "dispatch", "raw-result", "returning-state-cleanup", "result"):
            extra = {"schema":"firmware-o-decision/v3","proposal_path":"proposal","proposal_sha256":"proposal","call":{},"claim":{},"decision":"approve","rationale":"reviewed","issued_utc":"2026-01-01T00:00:00Z","issued_monotonic":1,"expires_monotonic":99,"topology_key_release":{},"orchestrator_identity":{},"signature":"sig","public_key":"key"} if stage == "signed-decision" else {}
            extra |= {"expires_monotonic":99} if stage == "authorization" else {}
            extra |= {"deadline_monotonic":100} if stage == "dispatch-admission" else {}
            extra |= {"raw_result":raw_payload,"outcome":raw_outcome} if stage == "raw-result" else {}
            extra |= {"exact_reaped":True} if stage == "returning-state-cleanup" else {}
            stages.append(broker.record(stage, "call-raw", {**common, **extra}, (str(stages[-1][0]), stages[-1][1]) if stages else None))
            if stage == "raw-result":
                recorded = __import__("json").loads(stages[-1][0].read_text(encoding="utf-8"))
                common = {**common, "bound_operation": recorded["bound_operation"], "bound_operation_sha256": recorded["bound_operation_sha256"]}
        return stages

    def test_seed_is_exact_and_hash_bound(self) -> None:
        validate_seed_manifest(Path("firmware_acceptance/seed"))

    def test_pinned_server_rejects_regular_and_linked_dotenv(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); subprocess.run(["git", "init", "-q"], cwd=root, check=True); (root / "pin").write_text("pin")
            subprocess.run(["git", "add", "pin"], cwd=root, check=True); subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@invalid", "commit", "-qm", "pin"], cwd=root, check=True)
            commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True).stdout.strip()
            with patch.object(kit, "_PINNED_SERVER_ROOT", root), patch.object(kit, "_PINNED_SERVER_COMMIT", commit):
                (root / ".env").write_text("forbidden")
                with self.assertRaises(AdmissionError): kit.validate_pinned_server()
                (root / ".env").unlink()
                try: (root / ".env").symlink_to(root / "missing.env")
                except OSError: return
                with self.assertRaises(AdmissionError): kit.validate_pinned_server()

    def test_controller_admission_is_bounded_and_fail_closed(self) -> None:
        call = {"call_id": "c1", "lane_id": "P3.STM", "board": "STM-A", "probe_uid": "uid", "target": "STM32L476RG", "profile": "stm", "method": "reset_and_run", "method_version": 1, "arguments": {"board_id": "STM-A"}, "proposal_sha256": "a", "decision_sha256": "b", "authorization_sha256": "c", "deadline_monotonic": 100.0, "plan": {"max_operation_duration_seconds": 30}, "permission": {"granted": True}, **self._scope("reset")}
        self.assertEqual("ALLOW", evaluate_call(call, now_monotonic=1.0)["policy"])
        call["arguments"] = {"operation": "mass_erase"}
        with self.assertRaises(AdmissionError):
            evaluate_call(call, now_monotonic=1.0)
        call["arguments"] = {"board_id": "STM-A"}; call["method_version"] = 2
        with self.assertRaises(AdmissionError):
            evaluate_call(call, now_monotonic=1.0)

    def test_read_memory_policy_matches_pinned_signature_and_bounds(self) -> None:
        call = {"call_id": "m1", "lane_id": "P3.STM", "board": "STM-A", "probe_uid": "uid", "target": "STM32L476RG", "profile": "stm", "method": "read_memory_symbol", "method_version": 1, "arguments": {"board_id": "STM-A", "symbol": "state", "width": 32, "elf_artifact": None}, "proposal_sha256": "a", "decision_sha256": "b", "authorization_sha256": "c", "deadline_monotonic": 100.0, "plan": {"max_operation_duration_seconds": 30}, "permission": {"granted": True}, **self._scope("memory_register_read")}
        self.assertEqual("ALLOW", evaluate_call(call, now_monotonic=1.0)["policy"])
        call["arguments"] = {"board_id": "STM-A", "symbol": "", "width": 32, "elf_artifact": None}
        with self.assertRaises(AdmissionError):
            evaluate_call(call, now_monotonic=1.0)
        call["arguments"] = {"board_id": "STM-A", "symbol": "state", "width": 64, "elf_artifact": None}
        with self.assertRaises(AdmissionError):
            evaluate_call(call, now_monotonic=1.0)

    def test_write_serial_uses_locked_utf8_byte_limit(self) -> None:
        call = {"call_id":"serial","lane_id":"STM-A","board":"STM-A","probe_uid":"uid","target":"STM32L476RG","profile":"stm","method":"write_serial","method_version":1,"arguments":{"board_id":"STM-A","text":"x" * 256,"baudrate":None,"port":None,"append_newline":True,"timeout_seconds":1,"on_exit":None},"proposal_sha256":"a","decision_sha256":"b","authorization_sha256":"c","deadline_monotonic":100.0,"plan":{"max_operation_duration_seconds":30},"permission":{"granted":True},**self._scope("uart_session_io")}
        self.assertEqual("ALLOW", evaluate_call(call, now_monotonic=1.0)["policy"])
        for text in ("x" * 257, "é" * 129):
            call["arguments"]["text"] = text
            with self.subTest(chars=len(text)), self.assertRaises(AdmissionError): evaluate_call(call, now_monotonic=1.0)

    def _uart_call(self, method: str, arguments: dict[str, object], *, plan: bool = False) -> dict[str, object]:
        call = {"call_id":"uart","lane_id":"STM-A","board":"STM-A","probe_uid":"uid","target":"STM32L476RG","profile":"stm","method":method + ("-plan" if plan else ""),"method_version":1,"proposal_sha256":"a","decision_sha256":"b","authorization_sha256":"c","deadline_monotonic":100.0,"plan":{"max_operation_duration_seconds":30},"permission":{"granted":True},**self._scope("uart_session_io")}
        if not plan:
            return {**call, "arguments":{"board_id":"server-route", **arguments}}
        return {**call, "arguments":{"board_id":"server-route","hypothesis":"h","strategy":"s","hypothesis_made":True,"strategy_evaluated":True,"expected_fail_return":"fail","expected_success_return":"ok","max_calls":1,"max_calls_buffer":1,"action_parameters":arguments}}

    @staticmethod
    def _uart_arguments(method: str) -> dict[str, object]:
        if method == "read_serial":
            return {"expected_text":None,"read_seconds":1,"baudrate":None,"port":None,"reset_on_open":False,"on_exit":None}
        if method == "write_serial":
            return {"text":"x","baudrate":None,"port":None,"append_newline":False,"timeout_seconds":1,"on_exit":None}
        return {"steps":[{"text":"x","expected_text":"ok","line_ending":"none"}],"read_seconds":1,"baudrate":None,"port":None,"ready_text":None,"ready_seconds":0,"ready_probe_text":None,"ready_probe_line_ending":"none","ready_probe_delay_seconds":0,"clear_input":False}

    def _assert_uart_admission(self, method: str, arguments: dict[str, object], *, plan: bool, allowed: bool) -> None:
        call = self._uart_call(method, arguments, plan=plan)
        if allowed:
            self.assertEqual("ALLOW", evaluate_call(call, now_monotonic=1)["policy"])
        else:
            with self.assertRaises(AdmissionError):
                evaluate_call(call, now_monotonic=1)

    def test_uart_timing_read_and_write_values_are_closed_direct_and_plan(self) -> None:
        positive_invalid = (True, 0, -1, 31, float("nan"), float("inf"), 10 ** 1000)
        for method, field in (("read_serial", "read_seconds"), ("write_serial", "timeout_seconds"), ("serial_exchange", "read_seconds")):
            for plan in (False, True):
                for value in (1, 30, 0.5, 30.0):
                    arguments = self._uart_arguments(method); arguments[field] = value
                    with self.subTest(method=method, plan=plan, field=field, value=repr(value)):
                        self._assert_uart_admission(method, arguments, plan=plan, allowed=True)
                arguments = self._uart_arguments(method); arguments |= {field:1, "baudrate":115200, "port":"COM1"}
                with self.subTest(method=method, plan=plan, baudrate="positive", port="nonempty"):
                    self._assert_uart_admission(method, arguments, plan=plan, allowed=True)
                for value in positive_invalid:
                    arguments = self._uart_arguments(method); arguments[field] = value
                    with self.subTest(method=method, plan=plan, field=field, value=repr(value)):
                        self._assert_uart_admission(method, arguments, plan=plan, allowed=False)
        for method, field, bad_values in (("read_serial", "expected_text", ("", 1)), ("read_serial", "baudrate", (False, 0, -1, 1.0)), ("read_serial", "port", ("", " ", 1)), ("read_serial", "reset_on_open", (True, 0)), ("read_serial", "on_exit", ("close", False)), ("write_serial", "baudrate", (False, 0, -1, 1.0)), ("write_serial", "port", ("", " ", 1)), ("write_serial", "append_newline", (1, None)), ("write_serial", "on_exit", ("close", False)), ("serial_exchange", "baudrate", (False, 0, -1, 1.0)), ("serial_exchange", "port", ("", " ", 1))):
            for plan in (False, True):
                for value in bad_values:
                    arguments = self._uart_arguments(method); arguments[field] = value
                    with self.subTest(method=method, plan=plan, field=field, value=repr(value)):
                        self._assert_uart_admission(method, arguments, plan=plan, allowed=False)

    def test_uart_write_transmitted_utf8_limits_apply_direct_and_plan(self) -> None:
        cases = (("x" * 256, False, True), ("x" * 255, True, True), ("x" * 256, True, True), ("é" * 128, False, True), ("é" * 128, True, True), ("", False, False), ("é" * 129, False, False))
        for plan in (False, True):
            for text, append_newline, allowed in cases:
                arguments = self._uart_arguments("write_serial"); arguments |= {"text":text,"append_newline":append_newline}
                with self.subTest(plan=plan, chars=len(text), append_newline=append_newline):
                    self._assert_uart_admission("write_serial", arguments, plan=plan, allowed=allowed)

    def test_uart_exchange_surface_and_null_plan_disclosure_are_closed(self) -> None:
        null = {key:None for key in ("board_id","hypothesis","strategy","hypothesis_made","strategy_evaluated","expected_fail_return","expected_success_return","max_calls","max_calls_buffer","action_parameters","user_permission")}
        for method in ("read_serial", "write_serial", "serial_exchange"):
            self.assertEqual("ALLOW", evaluate_call({**self._uart_call(method, self._uart_arguments(method), plan=True), "arguments":null}, now_monotonic=1)["policy"])
        invalid_nonnegative = (True, -1, 31, float("nan"), float("inf"), 10 ** 1000)
        for plan in (False, True):
            for field, values in (("steps", ([], "row")), ("clear_input", (1, None)), ("ready_seconds", (True, 0, -1, 31, float("nan"), float("inf"), 10 ** 1000)), ("ready_probe_delay_seconds", invalid_nonnegative), ("ready_probe_line_ending", (None, "bad"))):
                for value in values:
                    arguments = self._uart_arguments("serial_exchange"); arguments["ready_text"] = "ready"; arguments[field] = value
                    with self.subTest(plan=plan, field=field, value=repr(value)):
                        self._assert_uart_admission("serial_exchange", arguments, plan=plan, allowed=False)
            for row in ({}, {"text":"","expected_text":"ok","line_ending":"none"}, {"text":"x","expected_text":"","line_ending":"none"}, {"text":1,"expected_text":"ok","line_ending":"none"}, {"text":"x","expected_text":1,"line_ending":"none"}, {"text":"x","expected_text":"ok","line_ending":None}, {"text":"x","expected_text":"ok","line_ending":"bad"}, {"text":"x","expected_text":"ok","line_ending":"none","extra":None}, {"text":"x" * 256,"expected_text":"ok","line_ending":"lf"}):
                arguments = self._uart_arguments("serial_exchange"); arguments["steps"] = [row]
                with self.subTest(plan=plan, row=row):
                    self._assert_uart_admission("serial_exchange", arguments, plan=plan, allowed=False)
            for mutate in (lambda a: a.update({"ready_text":None,"ready_seconds":1}), lambda a: a.update({"ready_text":1}), lambda a: a.update({"ready_text":None,"ready_probe_text":"?"}), lambda a: a.update({"ready_text":"ready","ready_seconds":0}), lambda a: a.update({"ready_text":"ready","ready_seconds":1,"ready_probe_text":None,"ready_probe_delay_seconds":1}), lambda a: a.update({"ready_text":"ready","ready_seconds":1,"ready_probe_text":"","ready_probe_line_ending":"none"}), lambda a: a.update({"ready_text":"ready","ready_seconds":1,"ready_probe_text":1}), lambda a: a.update({"ready_text":"ready","ready_seconds":1,"ready_probe_text":"?","ready_probe_delay_seconds":2})):
                arguments = self._uart_arguments("serial_exchange"); mutate(arguments)
                with self.subTest(plan=plan, arguments=arguments):
                    self._assert_uart_admission("serial_exchange", arguments, plan=plan, allowed=False)
            for arguments in ({**self._uart_arguments("serial_exchange"), "steps":[{"text":"x" * 254,"expected_text":"ok","line_ending":"crlf"}]}, {**self._uart_arguments("serial_exchange"), "steps":[{"text":"é" * 127,"expected_text":"ok","line_ending":"crlf"}]}, {**self._uart_arguments("serial_exchange"), "ready_text":"ready","ready_seconds":0.5,"ready_probe_text":"?","ready_probe_line_ending":"lf","ready_probe_delay_seconds":0.25}, {**self._uart_arguments("serial_exchange"), "ready_text":"ready","ready_seconds":1,"ready_probe_text":"","ready_probe_line_ending":"cr","ready_probe_delay_seconds":0}):
                with self.subTest(plan=plan, valid=arguments):
                    self._assert_uart_admission("serial_exchange", arguments, plan=plan, allowed=True)

    def test_scope_effect_is_closed_and_independently_limited(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            def ref(name: str) -> dict[str, str]:
                path = root / name; path.write_text(name, encoding="utf-8"); return {"path":str(path),"sha256":__import__("hashlib").sha256(path.read_bytes()).hexdigest()}
            call = {"call_id":"effect","lane_id":"STM-A","board":"STM-A","probe_uid":"uid","target":"STM32L476RG","profile":"stm","method":"reset_and_run","method_version":1,"arguments":{"board_id":"STM-A"},"proposal_sha256":"a","decision_sha256":"b","authorization_sha256":"c","deadline_monotonic":100.0,"plan":{"max_operation_duration_seconds":30},"permission":{"granted":True},**self._scope("reset")}
            effect = call["scope_effect"] = {"schema":"firmware-call-effect/v1","effect_action_class":"ble_gatt_test","target_operation_manifest":ref("manifest"),"electronic_admission":ref("admission"),"limits":{"max_tx_power_dbm":0}}
            self.assertEqual("ALLOW", evaluate_call(call, now_monotonic=1)["policy"])
            effect["limits"] = {"max_tx_power_dbm": 1}
            with self.assertRaises(AdmissionError): evaluate_call(call, now_monotonic=1)
            effect.update({"effect_action_class":"lora_ping_pong_test","limits":{"bandwidth_hz":125000,"center_frequency_hz":915000000,"coding_rate_denominator":5,"max_campaign_minutes":30,"max_payload_bytes":64,"max_tx_airtime_ms_per_60s":6000,"max_tx_power_dbm":10,"spreading_factor_min":7,"spreading_factor_max":10}})
            for minimum, maximum, allowed in ((7, 10, True), (8, 10, True), (6, 10, False), (7, 11, False), (10, 9, False)):
                effect["limits"] |= {"spreading_factor_min":minimum,"spreading_factor_max":maximum}
                with self.subTest(spreading_factor_min=minimum, spreading_factor_max=maximum):
                    if allowed:
                        self.assertEqual("ALLOW", evaluate_call(call, now_monotonic=1)["policy"])
                    else:
                        with self.assertRaises(AdmissionError): evaluate_call(call, now_monotonic=1)
            call["scope_effect"] = {"schema":"firmware-call-effect/v1","effect_action_class":None,"target_operation_manifest":None,"electronic_admission":None,"limits":{},"extra":True}
            with self.assertRaises(AdmissionError): evaluate_call(call, now_monotonic=1)

    def test_policy_rejects_unknown_state_and_unsafe_flash_arguments(self) -> None:
        call = {"call_id":"flash","lane_id":"STM-A","board":"STM-A","probe_uid":"uid","target":"STM32L476RG","profile":"stm","method":"flash_application","method_version":1,"arguments":{"board_id":"STM-A","artifact":"bootloader.hex"},"proposal_sha256":"a","decision_sha256":"b","authorization_sha256":"c","deadline_monotonic":200.0,"plan":{"max_operation_duration_seconds":120},"permission":{"granted":True},**self._scope("application_flash")}
        with self.assertRaises(AdmissionError): evaluate_call(call, now_monotonic=1)
        call["method"] = "not-in-inventory"
        with self.assertRaises(AdmissionError): evaluate_call(call, now_monotonic=1)
        with tempfile.TemporaryDirectory() as temporary:
            policy = __import__("json").loads(Path("firmware_acceptance/MCP_METHOD_POLICY.json").read_text(encoding="utf-8")); policy["methods"]["reset_and_run"]["allowed_from"] = ["INVENTED"]
            path = Path(temporary) / "policy.json"; path.write_text(__import__("json").dumps(policy), encoding="utf-8")
            call["method"] = "reset_and_run"; call["arguments"] = {"board_id":"STM-A"}; call["plan"] = {"max_operation_duration_seconds":30}; call["action_class"] = "reset"
            with self.assertRaises(AdmissionError): evaluate_call(call, now_monotonic=1, policy_path=path)

    def test_delegated_artifact_is_exact_and_verbatim(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); policy = Path("firmware_acceptance/MCP_METHOD_POLICY.json").resolve()
            def ref(name: str) -> dict[str, str]:
                path = root / name; path.write_text(name, encoding="utf-8"); return {"path":str(path),"sha256":__import__("hashlib").sha256(path.read_bytes()).hexdigest()}
            governing = {name: ref(name) for name in ("goal","generalization_spec","implementation_roadmap","execution_plan","execution_readiness")}
            fixtures = {"STM-A":{"probe_uid":"066FFF514988525067233337","target":"STM32L476RG","profile":"stm-a-l476"},"STM-B":{"probe_uid":"0668FF514988525067213913","target":"STM32L476RG","profile":"stm-b-l476"},"NRF-A":{"probe_uid":"683710208","target":"nRF52840","profile":"nrf-a-52840"},"NRF-B":{"probe_uid":"683854191","target":"nRF52840","profile":"nrf-b-52840"}}
            value = {"schema_version":"delegated-hardware-authorization-v1","issuance_source":"goal.md Section 11 USER_HARDWARE_AUTHORIZATION_V1","canonical_user_scope_sha256":canonical_sha256(_USER_ISSUED_SCOPE),"user_issued_scope":_USER_ISSUED_SCOPE,"derived_bindings":{"c1_lock_id":"C1","operative_goal_sha256":"goal","stable_fixtures":fixtures,"destructive_exclusions":_USER_ISSUED_SCOPE["prohibited_action_classes"],"rf_limits":{"ble":_USER_ISSUED_SCOPE["limits"]["ble"],"lora":_USER_ISSUED_SCOPE["limits"]["lora"]},"mcp_server_pin":ref("pin"),"mcp_method_policy":{"path":str(policy),"sha256":__import__("hashlib").sha256(policy.read_bytes()).hexdigest()},"governing_documents":governing}}
            artifact = root / "delegated.json"; artifact.write_text(__import__("json").dumps(value), encoding="utf-8"); reference = {"path":str(artifact),"sha256":__import__("hashlib").sha256(artifact.read_bytes()).hexdigest()}
            self.assertEqual(value, validate_delegated_authorization(reference, policy_path=policy, manifest=AcceptanceBroker(root / "broker", Path("firmware_acceptance/seed"), policy, Path("firmware_acceptance/LANE_TEMPLATES.json")).manifest))
            for mutation in (lambda: value.__setitem__("extra", True), lambda: value.__setitem__("canonical_user_scope_sha256", "0" * 64), lambda: value["derived_bindings"].pop("rf_limits")):
                mutated = __import__("json").loads(__import__("json").dumps(value)); mutation(); artifact.write_text(__import__("json").dumps(value), encoding="utf-8"); reference["sha256"] = __import__("hashlib").sha256(artifact.read_bytes()).hexdigest()
                with self.assertRaises(AdmissionError): validate_delegated_authorization(reference, policy_path=policy, manifest=AcceptanceBroker(root / "broker" / str(len(str(value))), Path("firmware_acceptance/seed"), policy, Path("firmware_acceptance/LANE_TEMPLATES.json")).manifest)
                value = mutated

    def _limitation_evidence(self, root: Path, *, limitation_id: str = "L-1") -> tuple[AcceptanceBroker, Path, dict[str, object]]:
        """Construct the real, bounded create-once evidence graph; no workflow is simulated."""
        broker = AcceptanceBroker(root / "broker", Path("firmware_acceptance/seed"), Path("firmware_acceptance/MCP_METHOD_POLICY.json"), Path("firmware_acceptance/LANE_TEMPLATES.json"))
        def put(path: Path, value: object) -> dict[str, str]:
            path.parent.mkdir(parents=True, exist_ok=True); path.write_text(json.dumps(value), encoding="utf-8")
            return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        raw = {"result": {"server": "pinned failure"}}
        chain = self._complete_chain(broker, raw, raw_outcome="FAIL")
        chain_refs = [{"path": str(path), "sha256": digest, "stage": json.loads(path.read_text(encoding="utf-8"))["stage"]} for path, digest in chain[:7]]
        call = json.loads(chain[-1][0].read_text(encoding="utf-8")); bound = call["bound_operation"]
        attempt, lane, session, call_id = "attempt-raw", "STM-A", "session-1", "call-raw"
        c3 = root / "broker" / "hil" / lane / "server-limitations" / attempt / limitation_id / "c3-harness"
        repo = TemporaryGitRepository.create(root / "candidate")
        (repo.root / ".gitignore").write_text(".agent-workspace/\n", encoding="utf-8"); repo.git("add", ".gitignore"); repo.git("commit", "-m", "ignore workspace")
        workspace = repo.root / ".agent-workspace"; workspace.mkdir()
        prompt = workspace / "prompt.md"; prompt.write_text("bounded diagnostic", encoding="utf-8")
        status_path, result_path = workspace / "controller.status.json", workspace / "RESULT.json"
        worker = "limitation-worker"; invocation_path = workspace / "candidate.invocation.json"
        invocation = {"schema":"orchestrator-coding-invocation/v1", "action":"start", "runtime_root":str(root / "runtime"), "resource_lock_root":str(root / "runtime" / "locks"), "run_root":str(repo.root), "repository":repo.declaration(), "prompt_path":str(prompt), "prompt_sha256":hashlib.sha256(prompt.read_bytes()).hexdigest(), "output_paths":{"status":str(status_path),"jsonl":str(workspace / "worker.jsonl"),"stderr":str(workspace / "worker.log"),"last_message":str(workspace / "worker.last")}, "event_log_path":str(root / "runtime" / "LANE_EVENTS.jsonl"), "lane_id":lane, "worker_invocation_id":worker, "task":"causal diagnostic", "phase":"test", "exclusive_resources":[], "codex":{"command":["codex"],"model":"gpt-5.6-terra","reasoning_effort":"medium","service_tier":"priority","sandbox":"danger-full-access","approval_policy":"never","config_overrides":[]}}
        (root / "runtime").mkdir(); invocation_ref = put(invocation_path, invocation)
        result = {"schema":"orchestrator-lane-result/v1","lane_id":lane,"worker_invocation_id":worker,"branch":repo.branch,"commit":repo.head,"outcome":"PASS","summary":"bounded evidence only","checks":[{"name":"firmware-limitation-credit","command":"pending","outcome":"PASS"}]}
        result_ref = put(result_path, result)
        status = {"schema":"orchestrator-lane-controller/v1","invocation_schema":"orchestrator-coding-invocation/v1","state":"CODEX_EXITED","exit_code":0,"declared_lane_id":lane,"worker_invocation_id":worker,"held_resource_claims":[],"result_valid":True,"result_validation":{"state":"VALID","path":str(result_path),"sha256":result_ref["sha256"],"commit":repo.head},"controller_pid":1,"controller_created_utc":"2026-01-01T00:00:00Z","repository":repo.declaration(),"worktree_root":str(repo.root)}
        status_ref = put(status_path, status)
        result["checks"][0]["command"] = "pending"  # assignment hash is filled below, then all dependent evidence is rewritten.
        diagnostic_assignment = {"schema":"firmware-pinned-component-diagnostic-assignment/v1","attempt_id":attempt,"lane_id":lane,"session_id":session,"call_id":call_id,"server_commit":kit._PINNED_SERVER_COMMIT,"source_path":"README.md","source_sha256":hashlib.sha256(subprocess.run(["git","show",kit._PINNED_SERVER_COMMIT + ":README.md"],cwd=kit._PINNED_SERVER_ROOT,capture_output=True,check=True).stdout).hexdigest(),"input_signature":canonical_sha256(bound),"failure_signature":raw_result_sha256(raw),"predicate":"PINNED_COMPONENT_REPRODUCTION","worker_role":"F.C3.A1","worker_invocation_id":worker,"expected_status_path":str(status_path),"expected_result_path":str(result_path)}
        diagnostic_assignment_ref = put(c3 / "DIAGNOSTIC_ASSIGNMENT.json", diagnostic_assignment)
        result["checks"][0]["command"] = diagnostic_assignment_ref["sha256"]; result_ref = put(result_path, result); status["result_validation"]["sha256"] = result_ref["sha256"]; status_ref = put(status_path, status)
        diagnostic = {"schema":"firmware-pinned-component-diagnostic/v1","attempt_id":attempt,"lane_id":lane,"session_id":session,"call_id":call_id,"controller_owner":{"pid":1,"creation_identity":"2026-01-01T00:00:00Z"},"server_commit":kit._PINNED_SERVER_COMMIT,"source_path":diagnostic_assignment["source_path"],"source_sha256":diagnostic_assignment["source_sha256"],"input_signature":diagnostic_assignment["input_signature"],"failure_signature":diagnostic_assignment["failure_signature"],"predicate":"PINNED_COMPONENT_REPRODUCTION","target_independent":True,"locked_environment":{"server_commit":kit._PINNED_SERVER_COMMIT,"policy_sha256":bound["policy_sha256"],"schema_sha256":bound["schema_sha256"]},"assignment_path":diagnostic_assignment_ref["path"],"assignment_sha256":diagnostic_assignment_ref["sha256"],"candidate_invocation":invocation_ref,"controller_status":status_ref,"worker_result":result_ref,"worker_invocation_id":worker,"outcome":"PASS"}
        diagnostic_ref = put(c3 / "DIAGNOSTIC.json", diagnostic)
        # The substitute is a separate C3 completion; never rewrite the diagnostic's credited result.
        repo = TemporaryGitRepository.create(root / "substitute-candidate")
        (repo.root / ".gitignore").write_text(".agent-workspace/\n", encoding="utf-8"); repo.git("add", ".gitignore"); repo.git("commit", "-m", "ignore workspace")
        workspace = repo.root / ".agent-workspace"; workspace.mkdir()
        prompt = workspace / "prompt.md"; prompt.write_text("bounded substitute", encoding="utf-8")
        status_path, result_path = workspace / "controller.status.json", workspace / "RESULT.json"
        invocation["run_root"] = str(repo.root); invocation["repository"] = repo.declaration(); invocation["prompt_path"] = str(prompt); invocation["prompt_sha256"] = hashlib.sha256(prompt.read_bytes()).hexdigest(); invocation["output_paths"] = {"status":str(status_path),"jsonl":str(workspace / "worker.jsonl"),"stderr":str(workspace / "worker.log"),"last_message":str(workspace / "worker.last")}
        invocation_ref = put(workspace / "candidate.invocation.json", invocation)
        assignment = {"schema":"firmware-limitation-substitute-assignment/v1","limitation_id":limitation_id,"attempt_id":attempt,"lane_id":lane,"session_id":session,"kind":"PARTIAL_MCP","stable_id":"SUB-A21","assignment_id":"assign-1","worker_role":"F.C3.P1","worker_invocation_id":worker,"expected_status_path":str(status_path),"expected_result_path":str(result_path)}
        assignment_ref = put(c3 / "ASSIGNMENT.json", assignment)
        # A distinct substitute assignment gets its own exact credit token and the same real C3 completion.
        result = {**result,"branch":repo.branch,"commit":repo.head,"checks":[{"name":"firmware-limitation-credit","command":assignment_ref["sha256"],"outcome":"PASS"}]}; result_ref = put(result_path, result)
        status["repository"] = repo.declaration(); status["worktree_root"] = str(repo.root); status["result_validation"] = {"state":"VALID","path":str(result_path),"sha256":result_ref["sha256"],"commit":repo.head}; status_ref = put(status_path, status)
        launch = {"schema":"firmware-limitation-c3-launch/v1","attempt_id":attempt,"lane_id":lane,"session_id":session,"limitation_id":limitation_id,"assignment_path":assignment_ref["path"],"assignment_sha256":assignment_ref["sha256"],"candidate_invocation":invocation_ref,"controller_identity":{"pid":1,"creation_identity":"2026-01-01T00:00:00Z"},"controller_status":status_ref,"worker_result":result_ref,"worker_invocation_id":worker}; launch_ref = put(c3 / "LAUNCH.json", launch)
        execution = {"schema":"firmware-limitation-substitute-execution/v1","attempt_id":attempt,"lane_id":lane,"session_id":session,"limitation_id":limitation_id,"assignment_path":assignment_ref["path"],"assignment_sha256":assignment_ref["sha256"],"launch":launch_ref,"candidate_invocation":invocation_ref,"controller_identity":launch["controller_identity"],"controller_status":status_ref,"worker_result":result_ref,"worker_invocation_id":worker,"execution_id":"execution-1","outcome":"PASS"}; execution_ref = put(c3 / "EXECUTION.json", execution)
        worker_result = {"schema":"firmware-limitation-c3-worker-result/v1","attempt_id":attempt,"lane_id":lane,"session_id":session,"limitation_id":limitation_id,"assignment_path":assignment_ref["path"],"assignment_sha256":assignment_ref["sha256"],"launch":launch_ref,"execution":execution_ref,"candidate_invocation":invocation_ref,"controller_identity":launch["controller_identity"],"controller_status":status_ref,"candidate_result":result_ref,"worker_invocation_id":worker,"outcome":"PASS"}; worker_ref = put(c3 / "WORKER_RESULT.json", worker_result)
        substitute_result = {"schema":"firmware-limitation-substitute-result/v1","limitation_id":limitation_id,"attempt_id":attempt,"lane_id":lane,"session_id":session,"kind":"PARTIAL_MCP","stable_id":"SUB-A21","assignment_id":"assign-1","result_id":"result-1","assignment_path":assignment_ref["path"],"assignment_sha256":assignment_ref["sha256"],"candidate_invocation":invocation_ref,"controller_identity":launch["controller_identity"],"controller_status":status_ref,"candidate_result":result_ref,"execution":execution_ref,"worker_result":worker_ref,"outcome":"PASS"}; substitute_ref = put(c3 / "RESULT.json", substitute_result)
        attribution = {"schema":"firmware-pinned-server-attribution/v1","attempt_id":attempt,"lane_id":lane,"session_id":session,"call_id":call_id,"raw_result":{"path":chain_refs[-1]["path"],"sha256":chain_refs[-1]["sha256"]},"source_path":diagnostic_assignment["source_path"],"source_sha256":diagnostic_assignment["source_sha256"],"input_signature":diagnostic_assignment["input_signature"],"failure_signature":diagnostic_assignment["failure_signature"],"predicate":"PINNED_COMPONENT_REPRODUCTION","diagnostic_assignment":diagnostic_assignment_ref,"diagnostic_execution":diagnostic_ref}; attribution_ref = put(root / "attribution.json", attribution)
        terminal_ref = put(root / "terminal.json", {"schema":"firmware-session-terminal/v1","session_id":session,"session_open_path":"open","session_open_sha256":"open","terminal_state":"ABORTED","reason":"raw failure","natural_eof":True,"exact_reaped":True,"transport_cleanup":True,"helpers_stopped":True,"claim_released":True})
        process_ref = put(root / "process.json", {"session_id":session,"exact_reaped":True,"helpers_stopped":True,"claim_released":True})
        protected_ref = put(root / "protected.json", {"schema":"c1-protected-test-ids/v1","c1_reference":call["c1_reference"],"protected_ids":["A21"]})
        alternatives = [{"kind":"PARTIAL_MCP","available":True,"reason":"safe bounded substitute","evidence":put(root / "partial.json", {"safe":True})},{"kind":"PINNED_COMPONENT_INTEGRATION","available":False,"reason":"not available","evidence":put(root / "integration.json", {"safe":False})},{"kind":"CANDIDATE_BOUNDARY_UNIT","available":False,"reason":"weaker","evidence":put(root / "unit.json", {"safe":False})}]
        decision = {"schema":"firmware-server-limitation-decision/v1","limitation_id":limitation_id,"attempt_id":attempt,"lane_id":lane,"session_id":session,"original_test":"PHYSICAL-A21","classification":"AUTHORIZED_SERVER_LIMITATION","call_chain":chain_refs,"session_terminal":terminal_ref,"process_evidence":process_ref,"pinned_source":{"commit":kit._PINNED_SERVER_COMMIT,"immutable":True},"attribution":"PINNED_SERVER_SOURCE","attribution_evidence":attribution_ref,"alternatives":alternatives,"substitute":{"kind":"PARTIAL_MCP","stable_id":"SUB-A21","assignment":assignment_ref,"result":substitute_ref},"physical_certification":{"status":"NOT_CERTIFIED"},"o_decision":{"public_key":"key","protected_suite":protected_ref},"created_utc":"2026-01-01T00:00:00Z","signature":"sig"}
        path = root / "decision.json"; put(path, decision)
        return broker, path, decision

    def test_server_limitation_create_once_has_causal_and_c3_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            broker, path, decision = self._limitation_evidence(Path(temporary))
            result = broker.record_server_limitation(path, "L-1", self._Verifier())
            self.assertEqual("AUTHORIZED_SERVER_LIMITATION", result["classification"]); self.assertEqual({"status":"NOT_CERTIFIED"}, result["physical_certification"])
            with self.assertRaises(AdmissionError): broker.record_server_limitation(path, "L-1", self._Verifier())
            with self.assertRaises(AdmissionError): broker.record_server_limitation(path, "L-2", self._Verifier())

    def test_server_limitation_rejects_closed_evidence_mutations(self) -> None:
        cases = {
            "self-attribution": lambda d: d.__setitem__("attribution", "SELF_DECLARED"),
            "unsafe-order": lambda d: d.__setitem__("alternatives", list(reversed(d["alternatives"]))),
            "protected-original": lambda d: d.__setitem__("original_test", "A21"),
            "wrong-attempt": lambda d: d.__setitem__("attempt_id", "later-attempt"),
            "wrong-cleanup": lambda d: d.__setitem__("process_evidence", {"path":"missing","sha256":"missing"}),
            "wrong-signature": lambda d: d.__setitem__("signature", "forged"),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                broker, path, decision = self._limitation_evidence(Path(temporary)); mutate(decision)
                path.write_text(json.dumps(decision), encoding="utf-8")
                with self.assertRaises(AdmissionError): broker.record_server_limitation(path, "L-1", self._Verifier())

    def test_plan_action_admits_exact_null_and_populated_shapes_only(self) -> None:
        base = {"call_id":"plan","lane_id":"STM-A","board":"STM-A","probe_uid":"uid","target":"STM32L476RG","profile":"stm","method":"flash_application-plan","method_version":1,"proposal_sha256":"a","decision_sha256":"b","authorization_sha256":"c","deadline_monotonic":100.0,"plan":{"max_operation_duration_seconds":30},"permission":{"granted":True},**self._scope("application_flash")}
        null = {key: None for key in ("board_id","hypothesis","strategy","hypothesis_made","strategy_evaluated","expected_fail_return","expected_success_return","max_calls","max_calls_buffer","action_parameters","user_permission")}
        self.assertEqual("ALLOW", evaluate_call({**base,"arguments":null}, now_monotonic=1)["policy"])
        populated = {"board_id":"server-route","hypothesis":"h","strategy":"s","hypothesis_made":True,"strategy_evaluated":True,"expected_fail_return":"fail","expected_success_return":"ok","max_calls":1,"max_calls_buffer":1,"action_parameters":{"artifact":"app.hex"}}
        self.assertEqual("ALLOW", evaluate_call({**base,"arguments":populated}, now_monotonic=1)["policy"])
        for bad in ({**null,"extra":None},{key:value for key,value in null.items() if key != "user_permission"},{**populated,"user_permission":True},{**populated,"max_calls":True}):
            with self.subTest(arguments=bad), self.assertRaises(AdmissionError): evaluate_call({**base,"arguments":bad}, now_monotonic=1)

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
            bound |= self._scope("reset")
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
