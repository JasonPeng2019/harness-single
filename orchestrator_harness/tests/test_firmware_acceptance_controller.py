from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import firmware_acceptance.controller as controller_module
from firmware_acceptance.controller import FirmwareAcceptanceController, _StdioTransport, _process_identity
from firmware_acceptance.kit import AcceptanceBroker, AdmissionError, SignatureVerifier, _USER_ISSUED_SCOPE, canonical_decision_payload, canonical_sha256


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
    def held(self) -> list[dict[str, object]]: return [{"resource": "STM-A", "path": str(self.path), "owner": {"pid": 1, "created_utc": "2026-01-01T00:00:00Z", "creation_identity": "test-process-1"}}] if self.live else []
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
        pin = ref("pin")
        delegated_value = {"schema_version":"delegated-hardware-authorization-v1","issuance_source":"goal.md Section 11 USER_HARDWARE_AUTHORIZATION_V1","canonical_user_scope_sha256":canonical_sha256(_USER_ISSUED_SCOPE),"user_issued_scope":_USER_ISSUED_SCOPE,"derived_bindings":{"c1_lock_id":"C1","operative_goal_sha256":"goal","stable_fixtures":{"STM-A":{"probe_uid":"066FFF514988525067233337","target":"STM32L476RG","profile":"stm-a-l476"},"STM-B":{"probe_uid":"0668FF514988525067213913","target":"STM32L476RG","profile":"stm-b-l476"},"NRF-A":{"probe_uid":"683710208","target":"nRF52840","profile":"nrf-a-52840"},"NRF-B":{"probe_uid":"683854191","target":"nRF52840","profile":"nrf-b-52840"}},"destructive_exclusions":_USER_ISSUED_SCOPE["prohibited_action_classes"],"rf_limits":{"ble":_USER_ISSUED_SCOPE["limits"]["ble"],"lora":_USER_ISSUED_SCOPE["limits"]["lora"]},"mcp_server_pin":pin,"mcp_method_policy":{"path":str(policy),"sha256":hashlib.sha256(policy.read_bytes()).hexdigest()},"governing_documents":governing}}
        delegated_path = root / "delegated.json"; delegated_path.write_text(json.dumps(delegated_value), encoding="utf-8")
        delegated = {"path":str(delegated_path),"sha256":hashlib.sha256(delegated_path.read_bytes()).hexdigest()}
        normal_effect = {"schema":"firmware-call-effect/v1","effect_action_class":None,"target_operation_manifest":None,"electronic_admission":None,"limits":None}
        return {"call_id":"controller-1","attempt_id":"attempt-1","lane_id":"STM-A","board":"STM-A","resource":"STM-A","probe_uid":"066FFF514988525067233337","target":"STM32L476RG","profile":"stm-a-l476","route":None,"method":"reset_and_run","method_version":1,"arguments":{"board_id":"STM-A"},"deadline_monotonic":100.0,"plan":plan,"permission":permission,"c1_reference":ref("c1"),"delegated_reference":delegated,"board_identity":ref("board"),"mcp_schema":ref("schema"),"policy":{"path":str(policy),"sha256":hashlib.sha256(policy.read_bytes()).hexdigest()},"server_revision":"f003f84a7df51cd8595a3203c62e225b21da2a22","seed_identity":ref("seed"),"target_identity":ref("target"),"topology_key_release":ref("release"),"governing_documents":governing,"delegated_user_scope_sha256":canonical_sha256(_USER_ISSUED_SCOPE),"action_class":"reset","scope_effect":normal_effect}

    def _flow(self, root: Path, controller: FirmwareAcceptanceController, verifier: _Verifier) -> tuple[Path, Path, Path]:
        proposal_path = root / "broker" / "proposal.json"; proposal = controller.publish_proposal(proposal_path, {"call":self._call(root)})
        self.assertTrue(controller._live_claims.held)  # O signs only after exact claim is live.
        self.assertFalse({"proposal_sha256","decision_sha256","authorization_sha256"} & set(proposal["call"]))
        decision_path = root / "broker" / "decision.json"
        decision = {"schema":"firmware-o-decision/v3","proposal_path":str(proposal_path.resolve()),"proposal_sha256":proposal["raw_sha256"],"call":proposal["call"],"claim":proposal["claim"],"decision":"approve","rationale":"reviewed","issued_utc":"2026-01-01T00:00:00Z","issued_monotonic":1.0,"expires_monotonic":99.0,"topology_key_release":proposal["call"]["topology_key_release"],"orchestrator_identity":{"path":"identity","sha256":"identity"},"public_key":"public","signature":"signed"}
        verifier.expected = canonical_decision_payload(decision); decision_path.write_text(json.dumps(decision, sort_keys=True, separators=(",",":")), encoding="utf-8")
        authorization_path = root / "broker" / "authorization.json"; controller.derive_authorization(proposal_path, decision_path, authorization_path, verifier)
        return proposal_path, decision_path, authorization_path

    def _session_request(self, root: Path) -> dict[str, object]:
        call = self._call(root)
        return {key: call[key] for key in ("attempt_id","lane_id","board","resource","probe_uid","target","profile","route","c1_reference","delegated_reference","board_identity","mcp_schema","policy","server_revision","seed_identity","target_identity","topology_key_release","governing_documents")} | {"session_id":"123e4567-e89b-12d3-a456-426614174000","deadline_monotonic":100.0,"initial_state":"BOOTSTRAPPED"}

    def _session_artifacts(self, root: Path, controller: FirmwareAcceptanceController, verifier: _Verifier, request: dict[str, object]) -> tuple[Path, Path, Path]:
        sequence = request["sequence_number"]
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence <= 0:
            raise AssertionError("session test fixture requires a positive integer sequence number")
        proposal_path = root / "broker" / "sessions" / "123e4567-e89b-12d3-a456-426614174000" / f"proposal-{sequence}.json"
        proposal = controller.session_publish_proposal(proposal_path, request)
        decision_path = proposal_path.with_name(f"decision-{sequence}.json")
        decision = {"schema":"firmware-o-decision/v3","proposal_path":str(proposal_path.resolve()),"proposal_sha256":proposal["raw_sha256"],"call":proposal["call"],"claim":proposal["claim"],"decision":"approve","rationale":"session","issued_utc":"2026-01-01T00:00:00Z","issued_monotonic":1.0,"expires_monotonic":99.0,"topology_key_release":proposal["call"]["topology_key_release"],"orchestrator_identity":{"path":"identity","sha256":"identity"},"public_key":"public","signature":"signed"}
        verifier.expected = canonical_decision_payload(decision); decision_path.write_text(json.dumps(decision, sort_keys=True, separators=(",",":")), encoding="utf-8")
        authorization_path = proposal_path.with_name(f"authorization-{sequence}.json")
        authorization = {"schema":"firmware-derived-authorization/v2","proposal_path":str(proposal_path.resolve()),"proposal_sha256":proposal["raw_sha256"],"decision_path":str(decision_path.resolve()),"decision_sha256":hashlib.sha256(decision_path.read_bytes()).hexdigest(),"launch_intent":proposal["call"]["topology_key_release"],"orchestrator_identity":decision["orchestrator_identity"],"topology_key_release":decision["topology_key_release"],"c1_reference":proposal["call"]["c1_reference"],"delegated_reference":proposal["call"]["delegated_reference"],"call":proposal["call"],"claim":proposal["claim"],"expires_monotonic":99.0,"one_shot_id":proposal["call"]["call_id"],"revoked":False}
        authorization_path.write_text(json.dumps(authorization, sort_keys=True, separators=(",",":")), encoding="utf-8")
        return proposal_path, decision_path, authorization_path

    @staticmethod
    def _rpc_result(identifier: int, payload: dict[str, object]) -> bytes:
        return json.dumps({"jsonrpc":"2.0", "id":identifier, "result":{"content":[{"type":"text", "text":json.dumps(payload, sort_keys=True, separators=(",",":"))}]}}).encode("utf-8") + b"\n"

    def _open_routed_session(self, root: Path, controller: FirmwareAcceptanceController, launches: list[_Process], payloads: list[dict[str, object]]) -> dict[str, object]:
        request_path = root / "session-request.json"
        request_path.write_text(json.dumps(self._session_request(root)), encoding="utf-8")
        bootstrap = (
            b'{"jsonrpc":"2.0","id":1,"result":{}}\n'
            b'{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"Server Run\\n- run_id: run-1\\n- started_at: 2026-01-01T00:00:00Z"}]}}\n'
        )
        process = _Process()
        process.stdout = io.BytesIO(bootstrap + b"".join(self._rpc_result(index, value) for index, value in enumerate(payloads, start=3)))
        controller.launcher = lambda _: (launches.append(process) or process)
        return controller.open_session(request_path)

    def _session_call(self, root: Path, *, call_id: str, method: str, arguments: dict[str, object], action_class: str = "connect_setup") -> dict[str, object]:
        call = self._call(root)
        call.update({"call_id":call_id, "method":method, "method_version":1, "arguments":arguments, "action_class":action_class})
        return call

    def _execute_session_call(self, root: Path, controller: FirmwareAcceptanceController, verifier: _Verifier, opened: dict[str, object], call: dict[str, object], next_state: str) -> dict[str, object]:
        session = controller._session
        assert session is not None
        artifacts = self._session_artifacts(root, controller, verifier, {"session_id":opened["session_id"], "sequence_number":session["sequence"] + 1, "prior_result":session["prior_result"], "current_state":session["state"], "next_state":next_state, "call":call})
        return controller.session_execute_artifacts(*artifacts, verifier)

    @staticmethod
    def _setup_plan_arguments(board_id: str) -> dict[str, object]:
        return {"board_id":board_id, "hypothesis":"probe is attached", "strategy":"connect normally", "hypothesis_made":True, "strategy_evaluated":True, "expected_fail_return":"setup error", "expected_success_return":"setup complete", "max_calls":1, "max_calls_buffer":1, "action_parameters":{"mode":"normal", "connection_id":"wired", "display_name":"STM-A", "mcu_part_number":"STM32L476RG", "requires_uart":True, "serial_baudrate":115200, "serial_id":"COM1", "datasheet_path":"locked.pdf"}}

    def test_s2_a1_dynamic_route_plan_validation_ready_uart_and_terminal_contract(self) -> None:
        """One retained child may advance only along its server-returned route."""
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
            board_id = "server-stm-a"
            null_plan = {"board_id":None,"hypothesis":None,"strategy":None,"hypothesis_made":None,"strategy_evaluated":None,"expected_fail_return":None,"expected_success_return":None,"max_calls":None,"max_calls_buffer":None,"action_parameters":None,"user_permission":None}
            setup_args = self._setup_plan_arguments(board_id)
            action_args = {"board_id":board_id, **setup_args["action_parameters"]}
            route = {"display_name":"STM-A","route":"setup","board_id":board_id,"load_call":{"tool":"load_setup_tool","arguments":{"board_id":board_id,"tool_name":"board_setup-plan"}},"plan_initialization_call":{"tool":"board_setup-plan","arguments":null_plan}}
            validate_route = {"display_name":"STM-A","route":"validate","board_id":board_id,"load_call":{"tool":"load_setup_tool","arguments":{"board_id":board_id,"tool_name":"board_validate"}},"next_call":{"tool":"board_validate","arguments":{"board_id":board_id,"probe_id":"probe-1"}}}
            payloads = [
                {"status":"setup_names_required", "connection_assignments":{"STM-A":"wired"}},
                {"status":"setup_routes_ready", "routes":[route]},
                {"status":"setup_tool_loaded", "board_id":board_id, "tool_name":"board_setup-plan"},
                {"status":"plan_disclosed"},
                {"status":"plan_accepted", "plan_id":"plan-1", "underlying_action":"board_setup", "preferred_call":{"tool_name":"board_setup", "arguments":action_args}},
                {"status":"setup_completed"},
                {"status":"setup_routes_ready", "routes":[validate_route]},
                {"status":"setup_tool_loaded", "board_id":board_id, "tool_name":"board_validate"},
                {"status":"validation_passed"},
                {"status":"setup_ready", "configuration_ready":True, "live_session_ready":True, "ready_for_code":True, "ready_for_uart_work":True},
                {"status":"disconnected"},
            ]
            opened = self._open_routed_session(root, controller, launches, payloads)
            calls = (
                ("overview-null", "setup_overview", {"board_names":None,"connection_assignments":None}, "ROUTED", "probe_discovery_read"),
                ("overview-route", "setup_overview", {"board_names":["STM-A"],"connection_assignments":{"STM-A":"wired"}}, "ROUTED", "probe_discovery_read"),
                ("load-plan", "load_setup_tool", route["load_call"]["arguments"], "SETUP_LOADED", "connect_setup"),
                ("plan-null", "board_setup-plan", null_plan, "SETUP_PLAN_DISCLOSED", "connect_setup"),
                ("plan-filled", "board_setup-plan", setup_args, "SETUP_ACTION_READY", "connect_setup"),
                ("setup", "board_setup", action_args, "ROUTED", "connect_setup"),
                ("overview-validate", "setup_overview", {"board_names":["STM-A"],"connection_assignments":{"STM-A":"wired"}}, "ROUTED", "probe_discovery_read"),
                ("load-validate", "load_setup_tool", validate_route["load_call"]["arguments"], "VALIDATION_LOADED", "connect_setup"),
                ("validate", "board_validate", validate_route["next_call"]["arguments"], "READY", "connect_setup"),
                ("ready", "get_setup_status", {"board_id":board_id}, "READY", "connect_setup"),
                ("disconnect", "disconnect", {"board_id":board_id}, "RETURNED", "connect_setup"),
            )
            final: dict[str, object] | None = None
            for call_id, method, arguments, next_state, action_class in calls:
                final = self._execute_session_call(root, controller, verifier, opened, self._session_call(root, call_id=call_id, method=method, arguments=arguments, action_class=action_class), next_state)
            self.assertEqual("RETURNED", final["resulting_state"] if final else None)
            self.assertEqual(1, len(launches))
            terminal = controller.abort_session("test terminal")
            self.assertEqual("firmware-session-terminal/v1", terminal["schema"])
            self.assertEqual("ABORTED", terminal["terminal_state"])
            self.assertTrue(terminal["exact_reaped"] and terminal["helpers_stopped"] and terminal["claim_released"])

    def test_s2_a1_continuation_responses_are_closed_and_fresh_per_call(self) -> None:
        choice = {"status":"setup_needs_user_input", "continuation_id":"choice-1", "choices":[{"choice_id":"one"}, {"choice_id":"two"}], "accepted_response":{"tool":"continue_setup", "response":{"choice_id":"one"}}}
        continuation = FirmwareAcceptanceController._continuation_from_payload({"method":"board_setup", "arguments":{"board_id":"server-stm-a"}}, choice)
        self.assertEqual({"choice_id":"one"}, continuation and {"choice_id":"one"})
        self.assertTrue(FirmwareAcceptanceController._validate_continuation_response(continuation or {}, {"choice_id":"two"}))
        self.assertFalse(FirmwareAcceptanceController._validate_continuation_response(continuation or {}, {"choice_id":"other"}))
        self.assertFalse(FirmwareAcceptanceController._validate_continuation_response(continuation or {}, {"choice_id":"one", "extra":True}))
        with self.assertRaises(AdmissionError):
            FirmwareAcceptanceController._continuation_from_payload({"method":"board_setup", "arguments":{"board_id":"server-stm-a"}}, {**choice, "choices":[{"choice_id":"one"}, {"choice_id":3}]})
        target = {"pyocd_target":"stm32l476rg", "evidence":"datasheet", "reasoning_summary":"locked"}
        pack = {"pack_id":"STM32L4", "version":"1", "filename":"x.pack", "url":"https://official.invalid/x.pack", "source_path":"pack", "official_sha256":None, "evidence":"official", "reasoning_summary":"locked"}
        for response in (target, {**target, "debug_protocol":"swd", "debug_connect_mode":"normal", "debug_clock_hz":1000000}, pack, {**pack, "debug_protocol":"swd", "debug_connect_mode":"normal", "debug_clock_hz":1000000}):
            with self.subTest(response=response):
                payload = {"status":"setup_research_required", "continuation_id":"research-1", "accepted_response":{"tool":"continue_setup", "response":response}}
                research = FirmwareAcceptanceController._continuation_from_payload({"method":"board_fix_setup", "arguments":{"board_id":"server-stm-a"}}, payload)
                self.assertTrue(FirmwareAcceptanceController._validate_continuation_response(research or {}, response))
                self.assertFalse(FirmwareAcceptanceController._validate_continuation_response(research or {}, {**response, "unexpected":True}))
        with self.assertRaises(AdmissionError):
            FirmwareAcceptanceController._continuation_from_payload({"method":"board_setup", "arguments":{"board_id":"server-stm-a"}}, {"status":"setup_research_required", "continuation_id":"research-1", "accepted_response":{"tool":"continue_setup", "response":{**target, "official_sha256":None}}})

    def test_s2_a1_pre_result_route_and_continuation_rejections_leave_no_successor(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
            opened = self._open_routed_session(root, controller, launches, [{"status":"setup_routes_ready", "routes":[]}])
            bad = self._session_call(root, call_id="bad-route", method="setup_overview", arguments={"board_names":["STM-A"], "connection_assignments":None}, action_class="probe_discovery_read")
            artifacts = self._session_artifacts(root, controller, verifier, {"session_id":opened["session_id"], "sequence_number":1, "prior_result":None, "current_state":"BOOTSTRAPPED", "next_state":"ROUTED", "call":bad})
            with self.assertRaises(AdmissionError): controller.session_execute_artifacts(*artifacts, verifier)
            result_root = controller.broker.root / "sessions" / str(opened["session_id"]) / "results"
            self.assertFalse(result_root.exists())
            self.assertTrue(controller._session and controller._session["terminal"])

    def test_retained_session_bootstraps_once_executes_and_aborts_terminally(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
            request_path = root / "session-request.json"; request_path.write_text(json.dumps(self._session_request(root)), encoding="utf-8")
            # initialize, empty handshake, then the one permitted route call.
            process = _Process(); process.stdout = io.BytesIO(b'{"jsonrpc":"2.0","id":1,"result":{}}\n{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"Server Run\\n- run_id: run-1\\n- started_at: 2026-01-01T00:00:00Z"}]}}\n{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"{\\"status\\":\\"setup_names_required\\"}"}]}}\n')
            controller.launcher = lambda _: (launches.append(process) or process)
            opened = controller.open_session(request_path)
            self.assertEqual("BOOTSTRAPPED", opened["state"]); self.assertEqual(1, len(claims)); self.assertTrue(claims[0].live)
            call = self._call(root); call.update({"call_id":"route-1","method":"setup_overview","method_version":1,"arguments":{"board_names":None,"connection_assignments":None},"action_class":"probe_discovery_read"})
            paths = self._session_artifacts(root, controller, verifier, {"session_id":opened["session_id"],"sequence_number":1,"prior_result":None,"current_state":"BOOTSTRAPPED","next_state":"ROUTED","call":call})
            result = controller.session_execute_artifacts(*paths, verifier)
            self.assertEqual("ROUTED", result["resulting_state"]); self.assertEqual(1, len(launches))
            with self.assertRaises(AdmissionError): controller.session_publish_proposal(root / "broker" / "bad.json", {"session_id":opened["session_id"],"sequence_number":3,"prior_result":None,"current_state":"ROUTED","next_state":"ROUTED","call":call})
            controller._session["state"] = "OP_ACTION_READY"; controller._session["active_plan"] = {"method":"write_serial","parameters":{"text":"expected","baudrate":None,"port":None,"append_newline":True,"timeout_seconds":1,"on_exit":None}}
            action = self._call(root); action.update({"call_id":"action-2","method":"write_serial","method_version":1,"arguments":{"board_id":"STM-A","text":"altered","baudrate":None,"port":None,"append_newline":True,"timeout_seconds":1,"on_exit":None},"action_class":"uart_session_io"})
            with self.assertRaises(AdmissionError): controller.session_publish_proposal(root / "broker" / "paired-action.json", {"session_id":opened["session_id"],"sequence_number":2,"prior_result":{"path":result["path"],"sha256":result["raw_sha256"]},"current_state":"OP_ACTION_READY","next_state":"READY","call":action})
            aborted = controller.abort_session("synthetic")
            self.assertEqual("ABORTED", aborted["terminal_state"]); self.assertTrue(aborted["exact_reaped"]); self.assertFalse(claims[0].live)
            with self.assertRaises(AdmissionError): controller.session_publish_proposal(root / "broker" / "after.json", {"session_id":opened["session_id"],"sequence_number":2,"prior_result":result["path"],"current_state":"ROUTED","next_state":"ROUTED","call":call})

    def test_retained_session_signed_close_requires_returned_disconnect(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
            request_path = root / "session-request.json"; request_path.write_text(json.dumps(self._session_request(root)), encoding="utf-8")
            route = {"display_name":"STM-A","route":"validate","board_id":"server-stm-a","load_call":{"tool":"load_setup_tool","arguments":{"board_id":"server-stm-a","tool_name":"board_validate"}},"next_call":{"tool":"board_validate","arguments":{"board_id":"server-stm-a","probe_id":"probe-1"}}}
            process = _Process(); process.wait_timeouts = 0; process.stdout = io.BytesIO(b'{"jsonrpc":"2.0","id":1,"result":{}}\n{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"Server Run\\n- run_id: run-1\\n- started_at: 2026-01-01T00:00:00Z"}]}}\n' + self._rpc_result(3, {"status":"setup_routes_ready","routes":[route]}) + self._rpc_result(4, {"status":"disconnected"}))
            controller.launcher = lambda _: (launches.append(process) or process); opened = controller.open_session(request_path)
            overview = self._session_call(root, call_id="route-1", method="setup_overview", arguments={"board_names":["STM-A"],"connection_assignments":None}, action_class="probe_discovery_read")
            self._execute_session_call(root, controller, verifier, opened, overview, "ROUTED")
            # READY is the prerequisite established by the separately tested setup/validation state graph.
            controller._session["state"] = "READY"
            call = self._session_call(root, call_id="disconnect-1", method="disconnect", arguments={"board_id":"server-stm-a"})
            proposal, decision, authorization = self._session_artifacts(root, controller, verifier, {"session_id":opened["session_id"],"sequence_number":2,"prior_result":controller._session["prior_result"],"current_state":"READY","next_state":"RETURNED","call":call})
            result = controller.session_execute_artifacts(proposal, decision, authorization, verifier)
            close_path = root / "close.json"; close = {"schema":"firmware-session-close-decision/v1","session_id":opened["session_id"],"session_open_path":opened["path"],"session_open_sha256":opened["raw_sha256"],"final_result":{"path":result["path"],"sha256":result["raw_sha256"]},"final_state":"RETURNED","rationale":"return complete","issued_monotonic":1.0,"expected_returning_state":"disconnected","public_key":"public","signature":"signed"}
            verifier.expected = canonical_decision_payload(close); close_path.write_text(json.dumps(close, sort_keys=True, separators=(",",":")), encoding="utf-8")
            closed = controller.close_session(close_path, verifier)
            self.assertEqual("CLOSED", closed["terminal_state"]); self.assertTrue(closed["claim_released"]); self.assertTrue(closed["natural_eof"]); self.assertEqual(1, len(launches))

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
            self.assertNotIn("scope_effect", messages[2]["params"]["arguments"])
            cleanup = json.loads((controller.broker.root / "calls" / "controller-1" / "07-returning-state-cleanup.json").read_text())
            self.assertTrue(cleanup["stderr_log_complete"]); self.assertEqual(cleanup["stderr_sha256"], cleanup["stderr_log_sha256"])
            self.assertTrue(launches[0].stdout.closed); self.assertTrue(launches[0].stderr.closed)
            self.assertNotIn("stream_close_errors", cleanup)

    def test_stderr_persistence_failure_is_incomplete_and_fails_closed(self) -> None:
        class FailingSink:
            def write(self, _: bytes) -> int: raise OSError("synthetic write failure")
            def flush(self) -> None: raise OSError("synthetic flush failure")
            def close(self) -> None: pass
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims); paths = self._flow(root, controller, verifier)
            original = controller_module._StdioTransport
            def transport(process: _Process, *args: object) -> _StdioTransport:
                value = original(process, *args); self.assertIsNotNone(value.stderr_log); self.assertFalse(value.stderr_log.closed); value.stderr_log.close(); value.stderr_log = FailingSink(); process.stderr = io.BytesIO(b"stderr"); value._stderr(); return value
            with patch.object(controller_module, "_StdioTransport", transport), self.assertRaises(AdmissionError): controller.execute_artifacts(*paths, verifier)
            cleanup = json.loads((controller.broker.root / "calls" / "controller-1" / "07-returning-state-cleanup.json").read_text())
            self.assertFalse(cleanup["stderr_log_complete"]); self.assertIn("stderr_log_error", cleanup); self.assertFalse(claims[0].live)

    def test_retained_authorization_raw_rewrite_rejects_before_launch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
            paths = self._flow(root, controller, verifier)
            paths[2].write_text(paths[2].read_text(encoding="utf-8") + "\n", encoding="utf-8")
            with self.assertRaises(AdmissionError): controller.execute_artifacts(*paths, verifier)
            self.assertEqual([], launches); self.assertFalse(claims[0].live)

    def test_lane_template_binding_mutation_matrix_rejects_before_claim(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims = Path(temporary), [], []
            controller = self._controller(root, launches, claims)
            for key, value in (("lane_id", "NRF-A"), ("board", "NRF-A"), ("resource", "NRF-A"), ("probe_uid", "683710208"), ("target", "nRF52840"), ("profile", "nrf-a-52840"), ("route", "wrong")):
                call = self._call(root); call[key] = value
                with self.subTest(key=key), self.assertRaises(AdmissionError): controller.publish_proposal(root / "broker" / (key + ".json"), {"call": call})
            self.assertEqual([], claims); self.assertEqual([], launches)

    def test_existing_result_and_incomplete_authority_reject_before_claim(self) -> None:
        for mutation in ("result", "duration", "permission", "plan-hash"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
                if mutation == "result":
                    request, result = root / "request.json", root / "broker" / "result.json"
                    request.write_text(json.dumps({"call": self._call(root)}), encoding="utf-8"); result.parent.mkdir(parents=True, exist_ok=True); result.write_text("reserved", encoding="utf-8")
                    with self.assertRaises(AdmissionError): controller.run_lifecycle(request, root / "broker" / "proposal.json", root / "broker" / "decision.json", root / "broker" / "authorization.json", verifier, result_path=result)
                else:
                    call = self._call(root)
                    if mutation == "duration": call["plan"].pop("max_operation_duration_seconds")  # type: ignore[index]
                    elif mutation == "permission": call["permission"].pop("granted")  # type: ignore[index]
                    else: call["plan"]["sha256"] = "drift"  # type: ignore[index]
                    with self.assertRaises(AdmissionError): controller.publish_proposal(root / "broker" / "proposal.json", {"call": call})
                self.assertEqual([], claims); self.assertEqual([], launches)

    def test_unretained_execution_and_retained_authority_drift_reject_before_launch(self) -> None:
        for mutation in ("direct", "claim", "template", "manifest", "attempt"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
                proposal, decision, authorization = self._flow(root, controller, verifier)
                if mutation == "direct":
                    with self.assertRaises(AdmissionError): controller.execute(json.loads(proposal.read_text()), json.loads(decision.read_text()), json.loads(authorization.read_text()), verifier, proposal, hashlib.sha256(proposal.read_bytes()).hexdigest(), decision, hashlib.sha256(decision.read_bytes()).hexdigest(), root / "broker" / "other.json", "other")
                    controller._release_live_claim()
                else:
                    if mutation == "claim": claims[0].live = False
                    elif mutation == "template": controller.broker.templates["lanes"][0]["probe_uid"] = "drift"
                    elif mutation == "manifest": controller.broker.manifest["fixtures"]["STM-A"]["probe_uid"] = "drift"
                    else: controller.topology = {"attempt_id": "wrong"}
                    with self.assertRaises(AdmissionError): controller.execute_artifacts(proposal, decision, authorization, verifier)
                self.assertEqual([], launches); self.assertFalse(claims[0].live)

    def test_authorization_launch_or_identity_mutation_rejects_before_launch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, launches, claims, verifier = Path(temporary), [], [], _Verifier(); controller = self._controller(root, launches, claims)
            paths = self._flow(root, controller, verifier)
            artifact = json.loads(paths[2].read_text(encoding="utf-8")); artifact["launch_intent"] = {"path":"mutated","sha256":"mutated"}
            paths[2].write_text(json.dumps(artifact), encoding="utf-8")
            with self.assertRaises(AdmissionError): controller.execute_artifacts(*paths, verifier)
            self.assertEqual([], launches); self.assertFalse(claims[0].live)

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

    def test_transport_post_reap_stderr_drain_reaches_eof_before_join_budget(self) -> None:
        class CoordinatedStderr:
            def __init__(self) -> None:
                self.initial_read, self.release_suffix = threading.Event(), threading.Event()
                self.chunks = [b"initial-", b"suffix"]
                self.closed = False
            def read(self, _: int) -> bytes:
                if self.chunks:
                    chunk = self.chunks.pop(0)
                    if chunk == b"initial-":
                        self.initial_read.set()
                        if not self.release_suffix.wait(1.0): raise AssertionError("suffix was not released")
                    return chunk
                return b""
            def close(self) -> None: self.closed = True
        with tempfile.TemporaryDirectory() as temporary:
            process = _Process(); process.stderr = CoordinatedStderr()
            stderr_path = Path(temporary) / "stderr.log"
            transport = _StdioTransport(process, lambda: 1.0, 0.5, stderr_path)
            self.assertTrue(process.stderr.initial_read.wait(1.0))
            process.stderr.release_suffix.set()
            stopped, cleanup = transport.close_and_join()
            expected = b"initial-suffix"
            self.assertTrue(stopped); self.assertEqual(expected, stderr_path.read_bytes())
            self.assertTrue(cleanup["stderr_eof"]); self.assertFalse(cleanup["stderr_forced_close"])
            self.assertTrue(cleanup["stderr_log_complete"])
            self.assertEqual(hashlib.sha256(expected).hexdigest(), cleanup["stderr_sha256"])
            self.assertEqual(cleanup["stderr_sha256"], cleanup["stderr_log_sha256"])
            self.assertTrue(all(item["stopped"] for item in cleanup["helper_threads"]))
            self.assertTrue(process.stderr.closed); self.assertNotIn("stream_close_errors", cleanup)

    def test_transport_forced_close_of_blocked_stderr_is_incomplete_with_partial_digest(self) -> None:
        class BlockedStderr:
            def __init__(self) -> None: self.reading, self.closed = threading.Event(), threading.Event()
            def read(self, _: int) -> bytes:
                self.reading.set()
                if not self.closed.wait(1.0): raise AssertionError("blocked stream was not closed")
                return b""
            def close(self) -> None: self.closed.set()
        with tempfile.TemporaryDirectory() as temporary:
            process = _Process(); process.stderr = BlockedStderr()
            stderr_path = Path(temporary) / "stderr.log"
            transport = _StdioTransport(process, lambda: 1.0, 0.2, stderr_path)
            self.assertTrue(process.stderr.reading.wait(1.0))
            stopped, cleanup = transport.close_and_join()
            self.assertTrue(stopped); self.assertTrue(process.stderr.closed.is_set())
            self.assertTrue(cleanup["stderr_eof"]); self.assertTrue(cleanup["stderr_forced_close"])
            self.assertFalse(cleanup["stderr_log_complete"]); self.assertIn("stderr_log_partial_sha256", cleanup)
            self.assertIn("cleanup_error", cleanup)
            self.assertTrue(all(item["stopped"] for item in cleanup["helper_threads"]))

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
