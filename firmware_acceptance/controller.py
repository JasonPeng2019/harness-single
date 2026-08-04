"""Bounded C3-HARNESS MCP stdio owner; transport is injectable for host-only tests."""

from __future__ import annotations

import hashlib
import json
import os
import argparse
import base64
import subprocess
import threading
import time
import queue
import datetime as _datetime
import math
from pathlib import Path
from typing import Any, Callable, Protocol

from orchestrator_harness.models import ProcessInfo
from orchestrator_harness.processes import process_snapshot
from orchestrator_harness.resource_locks import ResourceClaims, ResourceLockError
from harness_common.process_identity import exact_process_identity
from .kit import AcceptanceBroker, AdmissionError, SignatureVerifier, _safe_child, _write_new, canonical_decision_payload, canonical_sha256, raw_result_sha256, reject_linked_path


class StdioProcess(Protocol):
    pid: int
    stdin: Any
    stdout: Any
    stderr: Any
    def poll(self) -> int | None: ...
    def terminate(self) -> None: ...
    def kill(self) -> None: ...
    def wait(self, timeout: float | None = None) -> int: ...


Launcher = Callable[[dict[str, Any]], StdioProcess]
_FORBIDDEN_REQUEST_KEYS = {"endpoint", "stdio", "mcp_command", "environment", "credential", "process", "handle"}
_CALL_KEYS = {"call_id", "attempt_id", "lane_id", "board", "resource", "probe_uid", "target", "profile", "route", "method", "method_version", "arguments", "deadline_monotonic", "plan", "permission", "c1_reference", "delegated_reference", "board_identity", "mcp_schema", "policy", "server_revision", "seed_identity", "target_identity", "topology_key_release", "governing_documents"}
_REFERENCE_KEYS = {"path", "sha256"}
_GOVERNING_KEYS = {"goal", "generalization_spec", "implementation_roadmap", "execution_plan", "execution_readiness"}
_DECISION_KEYS = {"schema", "proposal_path", "proposal_sha256", "call", "claim", "decision", "rationale", "issued_utc", "issued_monotonic", "expires_monotonic", "topology_key_release", "orchestrator_identity", "public_key", "signature"}
_AUTH_KEYS = {"schema", "proposal_path", "proposal_sha256", "decision_path", "decision_sha256", "launch_intent", "orchestrator_identity", "topology_key_release", "c1_reference", "delegated_reference", "call", "claim", "expires_monotonic", "one_shot_id", "revoked"}


class Ed25519Verifier(SignatureVerifier):
    """Production verifier; its key is read only from ROOT's launch evidence."""
    def verify(self, payload: bytes, signature: str, public_key: str) -> bool:
        try:
            from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
            Ed25519PublicKey.from_public_bytes(base64.b64decode(public_key, validate=True)).verify(base64.b64decode(signature, validate=True), payload)
            return True
        except Exception:
            return False


def _reference(value: Any, label: str) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != _REFERENCE_KEYS or not all(isinstance(value[key], str) and value[key] for key in _REFERENCE_KEYS):
        raise AdmissionError(label + " must be an exact path/hash binding")
    return value


def _closed_call(call: Any) -> dict[str, Any]:
    if not isinstance(call, dict) or set(call) != _CALL_KEYS or _FORBIDDEN_REQUEST_KEYS & set(call):
        raise AdmissionError("call authority is not closed")
    for key in ("call_id", "attempt_id", "lane_id", "board", "probe_uid", "target", "profile", "method", "resource"):
        if not isinstance(call[key], str) or not call[key]: raise AdmissionError("call identity is incomplete")
    if call["route"] is not None and not isinstance(call["route"], str): raise AdmissionError("route must be canonical null or an exact route")
    if call["resource"] != call["board"]: raise AdmissionError("declared resource must equal exact board resource")
    if not isinstance(call["method_version"], int) or isinstance(call["method_version"], bool) or not isinstance(call["arguments"], dict): raise AdmissionError("call method binding is invalid")
    if not isinstance(call["deadline_monotonic"], (int, float)) or isinstance(call["deadline_monotonic"], bool) or not math.isfinite(call["deadline_monotonic"]): raise AdmissionError("call deadline is invalid")
    for key in ("c1_reference", "delegated_reference", "board_identity", "mcp_schema", "policy", "seed_identity", "target_identity", "topology_key_release"):
        _reference(call[key], key)
    if not isinstance(call["governing_documents"], dict) or set(call["governing_documents"]) != _GOVERNING_KEYS: raise AdmissionError("governing documents are incomplete")
    for key in _GOVERNING_KEYS: _reference(call["governing_documents"][key], "governing " + key)
    if call["server_revision"] != "f003f84a7df51cd8595a3203c62e225b21da2a22": raise AdmissionError("server revision is not pinned")
    if not isinstance(call["plan"], dict) or set(call["plan"]) != {"path", "sha256", "max_operation_duration_seconds"} or not isinstance(call["plan"]["max_operation_duration_seconds"], int) or call["plan"]["max_operation_duration_seconds"] <= 0: raise AdmissionError("plan authority is invalid")
    if not isinstance(call["permission"], dict) or set(call["permission"]) != {"path", "sha256", "granted"} or call["permission"]["granted"] is not True: raise AdmissionError("permission authority is invalid")
    return json.loads(json.dumps(call, sort_keys=True))


def _verify_raw_reference(reference: dict[str, str], label: str) -> None:
    path = Path(reference["path"])
    reject_linked_path(path)
    if path.is_symlink() or not path.is_file(): raise AdmissionError(label + " is unsafe or absent")
    try: actual = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc: raise AdmissionError(label + " is unreadable") from exc
    if actual != reference["sha256"]: raise AdmissionError(label + " raw hash drifted")


def _verify_live_call_inputs(call: dict[str, Any], broker: "AcceptanceBroker") -> dict[str, str]:
    for key in ("c1_reference", "delegated_reference", "board_identity", "mcp_schema", "policy", "seed_identity", "target_identity", "topology_key_release"):
        _verify_raw_reference(call[key], key)
    for key, value in call["governing_documents"].items(): _verify_raw_reference(value, "governing " + key)
    if Path(call["policy"]["path"]).resolve() != broker.policy_path or call["policy"]["sha256"] != hashlib.sha256(broker.policy_path.read_bytes()).hexdigest():
        raise AdmissionError("live broker policy is not the bound policy")
    _verify_raw_reference(call["plan"], "plan"); _verify_raw_reference(call["permission"], "permission")
    return {key: value["sha256"] for key, value in call["governing_documents"].items()}


def _launch(config: dict[str, Any]) -> StdioProcess:
    return subprocess.Popen(  # type: ignore[return-value]
        config["mcp_command"], cwd=config["working_directory"], env=config["environment"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )


def _process_identity(pid: int) -> dict[str, Any] | None:
    """The OS-level primitive returns None once the exact child is absent."""
    if pid not in process_snapshot().by_pid:
        return None
    return exact_process_identity(pid)


class _StdioTransport:
    """The controller's only stdio owner: one writer and permanent stdout/stderr drains."""
    def __init__(self, process: StdioProcess, remaining: Callable[[], float], io_cap: float) -> None:
        self.process, self.remaining, self.io_cap = process, remaining, io_cap
        self.writes: queue.Queue[tuple[bytes, threading.Event, list[BaseException]]] = queue.Queue()
        self.responses: queue.Queue[dict[str, Any] | BaseException | None] = queue.Queue()
        self.stop = threading.Event(); self.accepting = threading.Event(); self.accepting.set()
        self.stderr_bytes = bytearray(); self._stderr_lock = threading.Lock()
        self.threads = [threading.Thread(target=self._write, name="firmware-mcp-writer"), threading.Thread(target=self._stdout, name="firmware-mcp-stdout"), threading.Thread(target=self._stderr, name="firmware-mcp-stderr")]
        for thread in self.threads: thread.start()

    def _wait(self, event: threading.Event, label: str) -> None:
        while not event.is_set():
            remaining = self._budget()
            if remaining <= 0: raise AdmissionError("MCP " + label + " timed out")
            event.wait(min(remaining, 0.02))

    def _budget(self) -> float:
        return min(max(0.0, self.remaining()), self.io_cap)

    def send(self, value: dict[str, Any], label: str) -> None:
        if not self.accepting.is_set() or self.stop.is_set() or self.process.stdin is None: raise AdmissionError("MCP stdin is unavailable")
        done, errors = threading.Event(), []
        self.writes.put((json.dumps(value, separators=(",", ":")).encode() + b"\n", done, errors))
        self._wait(done, label)
        if errors: raise AdmissionError("MCP " + label + " failed") from errors[0]

    def receive(self, request_id: int) -> dict[str, Any]:
        while True:
            remaining = self._budget()
            if remaining <= 0: raise AdmissionError("MCP read timed out")
            try: value = self.responses.get(timeout=min(remaining, 0.02))
            except queue.Empty: continue
            if value is None: raise AdmissionError("MCP stdout reached EOF")
            if isinstance(value, BaseException): raise AdmissionError("MCP stdio framing is invalid") from value
            if value.get("jsonrpc") != "2.0" or value.get("id") != request_id or "error" in value or "result" not in value:
                raise AdmissionError("MCP response is invalid")
            return value

    def cancel(self, request_id: int, reason: str) -> bool:
        if not self.stop.is_set() and self.process.stdin is not None:
            try:
                self.send({"jsonrpc":"2.0", "method":"notifications/cancelled", "params":{"requestId":request_id, "reason":reason}}, "cancellation")
                return True
            except AdmissionError: return False
        return False

    def use_budget(self, remaining: Callable[[], float]) -> None:
        """Switch only teardown to its reserved authorization authority."""
        self.remaining = remaining

    def close_and_join(self) -> tuple[bool, dict[str, Any]]:
        self.accepting.clear(); self.stop.set(); outcome: dict[str, Any] = {"helper_threads": []}
        for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
            try:
                if stream is not None: stream.close()
            except BaseException as exc: outcome.setdefault("stream_close_errors", []).append(type(exc).__name__)
        for thread in self.threads:
            thread.join(max(0.0, self._budget()))
            outcome["helper_threads"].append({"name": thread.name, "stopped": not thread.is_alive()})
        outcome["stderr_sha256"] = hashlib.sha256(bytes(self.stderr_bytes)).hexdigest()
        return all(not thread.is_alive() for thread in self.threads), outcome

    def _write(self) -> None:
        while not self.stop.is_set():
            try: data, done, errors = self.writes.get(timeout=0.02)
            except queue.Empty: continue
            try:
                if self.process.stdin is None: raise OSError("stdin unavailable")
                self.process.stdin.write(data); self.process.stdin.flush()
            except BaseException as exc: errors.append(exc)
            finally: done.set()

    def _stdout(self) -> None:
        try:
            if self.process.stdout is None: self.responses.put(AdmissionError("stdout unavailable")); return
            while not self.stop.is_set():
                line = self.process.stdout.readline()
                if not line: self.responses.put(None); return
                if isinstance(line, str): line = line.encode()
                value = json.loads(line.decode("utf-8"))
                if not isinstance(value, dict): raise ValueError("non-object JSON-RPC frame")
                self.responses.put(value)
        except BaseException as exc: self.responses.put(exc)

    def _stderr(self) -> None:
        try:
            if self.process.stderr is None: return
            while not self.stop.is_set():
                chunk = self.process.stderr.read(4096)
                if not chunk: return
                if isinstance(chunk, str): chunk = chunk.encode("utf-8", "replace")
                with self._stderr_lock: self.stderr_bytes.extend(chunk[: max(0, 65536 - len(self.stderr_bytes))])
        except BaseException: return


def _scrubbed_environment(config: dict[str, Any]) -> dict[str, str]:
    """Keep runtime discovery, discard ambient operation/import/route authority."""
    blocked = ("MCP_", "PYOCD_", "PROBE_", "TARGET_", "SERIAL_", "FIRMWARE_", "BYO_", "OPENOCD_", "JLINK_", "UV_PROJECT_")
    keep = {"PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP", "HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA"}
    env = {k: v for k, v in os.environ.items() if k.upper() in keep and not k.upper().startswith(blocked)}
    env.update({"PYTHONNOUSERSITE": "1", "PYTHONSAFEPATH": "1"})
    for key, value in config["environment"].items():
        if key in {"BYO_MCP_ARTIFACT_ROOT", "PYOCD_PROBE_UID", "PYOCD_TARGET", "PYTHONPYCACHEPREFIX"}:
            env[key] = value
    return env


class FirmwareAcceptanceController:
    """Two-phase controller that alone owns the MCP stdio process and physical capability."""

    def __init__(self, broker: AcceptanceBroker, *, launcher: Launcher | None = None, clock: Callable[[], float] = time.monotonic, identity_provider: Callable[[int], dict[str, Any] | None] = _process_identity, claims_factory: Callable[[str, str], Any] | None = None, io_timeout: float = 5.0, topology: dict[str, Any] | None = None) -> None:
        self.broker, self.launcher, self.clock, self.identity_provider = broker, launcher or _launch, clock, identity_provider
        self.claims_factory, self.io_timeout = claims_factory, io_timeout
        self._live_claims: Any | None = None
        self._live_claim: dict[str, Any] | None = None
        self._proposal_binding: tuple[Path, str] | None = None
        self._authorization_binding: tuple[Path, str] | None = None
        self.topology = topology

    def run_lifecycle(self, request_path: Path, proposal_path: Path, decision_path: Path,
                      authorization_path: Path, verifier: SignatureVerifier, *,
                      wait_for_decision: Callable[[Path], None] | None = None, result_path: Path | None = None) -> dict[str, Any]:
        """One retained-controller lifecycle.  The waiter only observes the O-owned decision."""
        try:
            safe_result: Path | None = None
            if result_path is not None:
                reject_linked_path(result_path)
                safe_result = result_path.resolve()
                if self.broker.root not in safe_result.parents: raise AdmissionError("result path escapes controller root")
            reject_linked_path(request_path)
            request = self._load_external(request_path, {"call"})
            self.publish_proposal(proposal_path, request)
            (wait_for_decision or self._wait_for_decision)(decision_path)
            self.derive_authorization(proposal_path, decision_path, authorization_path, verifier)
            result = self.execute_artifacts(proposal_path, decision_path, authorization_path, verifier)
            if safe_result is not None:
                _write_new(safe_result, result)
            return result
        except BaseException:
            self._release_live_claim()
            raise

    def _wait_for_decision(self, path: Path) -> None:
        """Bounded read-only observation of the O-owned, predeclared path."""
        if self._live_claim is None: raise AdmissionError("no retained claim while awaiting O")
        limit = self._load_artifact(self._proposal_binding[0], {"schema", "call", "claim"})["call"]["deadline_monotonic"]
        while self.clock() < limit:
            # resolve is deliberately repeated: substitution is not tolerated between observations.
            reject_linked_path(path)
            if path.exists():
                resolved = path.resolve()
                if resolved.is_symlink() or not resolved.is_file() or self.broker.root not in resolved.parents:
                    raise AdmissionError("O decision path is unsafe")
                return
            time.sleep(min(0.05, max(0.0, limit - self.clock())))
        raise AdmissionError("timed out waiting for O decision")

    def create_proposal(self, request: dict[str, Any]) -> dict[str, Any]:
        if set(request) != {"call"}: raise AdmissionError("proposal must contain exactly one closed call authority")
        call = _closed_call(request["call"])
        _verify_live_call_inputs(call, self.broker)
        return {"schema": "firmware-controller-proposal/v2", "call": call}

    def publish_proposal(self, path: Path, request: dict[str, Any]) -> dict[str, Any]:
        """Acquire the board before create-once publication; O receives live claim evidence."""
        if self._live_claims is not None: raise AdmissionError("controller already owns a live proposal claim")
        proposal = self.create_proposal(request)
        reject_linked_path(path)
        resolved = path.resolve()
        if self.broker.root not in resolved.parents or resolved.is_symlink():
            raise AdmissionError("proposal path escapes controller root")
        call = proposal["call"]
        claims = self._claims(call["lane_id"], call["call_id"])
        try:
            claims.acquire_all([call["resource"]], on_wait=lambda finding: (_ for _ in ()).throw(AdmissionError("resource claim unavailable: " + str(finding.get("state")))))
            held = claims.held
            if len(held) != 1 or held[0].get("resource") != call["board"] or not isinstance(held[0].get("path"), str) or held[0].get("owner") is None:
                raise AdmissionError("resource claim evidence is unavailable")
            claim_original = Path(str(held[0]["path"])); reject_linked_path(claim_original); claim_path = claim_original.resolve()
            claim = {"resource": call["board"], "path": str(claim_path), "sha256": hashlib.sha256(claim_path.read_bytes()).hexdigest(), "owner": held[0]["owner"]}
            proposal = {**proposal, "claim": claim}
            raw_sha = _write_new(resolved, proposal)
        except BaseException:
            if claims.release_all(): raise AdmissionError("resource claim release failed")
            raise
        self._live_claims, self._live_claim, self._proposal_binding = claims, claim, (resolved, raw_sha)
        return {**proposal, "path": str(resolved), "raw_sha256": raw_sha}

    def derive_authorization(self, proposal_path: Path, decision_path: Path, authorization_path: Path, verifier: SignatureVerifier) -> dict[str, Any]:
        """Candidate-only post-approval authorization; no future digest is supplied by a caller."""
        try:
            proposal, proposal_sha = self._read_bound(proposal_path, {"schema", "call", "claim"})
            self._require_live_claim(proposal_path, proposal_sha, proposal)
            decision, decision_sha = self._read_bound(decision_path, _DECISION_KEYS)
            self._validate_decision(proposal_path, proposal_sha, proposal, decision, verifier)
            if self.clock() >= decision["expires_monotonic"]: raise AdmissionError("approval expired before authorization")
            authorization = {"schema":"firmware-derived-authorization/v2", "proposal_path":str(proposal_path.resolve()), "proposal_sha256":proposal_sha, "decision_path":str(decision_path.resolve()), "decision_sha256":decision_sha, "launch_intent":self.topology["launch"] if self.topology else proposal["call"]["topology_key_release"], "orchestrator_identity":decision["orchestrator_identity"], "topology_key_release":decision["topology_key_release"], "c1_reference":proposal["call"]["c1_reference"], "delegated_reference":proposal["call"]["delegated_reference"], "call":proposal["call"], "claim":proposal["claim"], "expires_monotonic":decision["expires_monotonic"], "one_shot_id":proposal["call"]["call_id"], "revoked":False}
            reject_linked_path(authorization_path)
            resolved = authorization_path.resolve()
            if self.broker.root not in resolved.parents or resolved.is_symlink(): raise AdmissionError("authorization path escapes controller root")
            raw_sha = _write_new(resolved, authorization)
            self._authorization_binding = (resolved, raw_sha)
            return {**authorization, "path":str(resolved), "raw_sha256":raw_sha}
        except BaseException:
            self._release_live_claim(); raise

    def execute_artifacts(self, proposal_path: Path, decision_path: Path, authorization_path: Path, verifier: SignatureVerifier) -> dict[str, Any]:
        try:
            proposal, proposal_sha = self._read_bound(proposal_path, {"schema", "call", "claim"})
            self._require_live_claim(proposal_path, proposal_sha, proposal)
            decision, decision_sha = self._read_bound(decision_path, _DECISION_KEYS)
            authorization, authorization_sha = self._read_bound(authorization_path, _AUTH_KEYS)
            if self._authorization_binding != (authorization_path.resolve(), authorization_sha):
                raise AdmissionError("retained authorization artifact drifted or was substituted")
            return self.execute(proposal, decision, authorization, verifier, proposal_path.resolve(), proposal_sha, decision_path.resolve(), decision_sha, authorization_path.resolve(), authorization_sha)
        except BaseException:
            self._release_live_claim(); raise

    def execute(self, proposal: dict[str, Any], decision: dict[str, Any], authorization: dict[str, Any], verifier: SignatureVerifier, proposal_path: Path | None = None, proposal_sha: str | None = None, decision_path: Path | None = None, decision_sha: str | None = None, authorization_path: Path | None = None, authorization_sha: str | None = None) -> dict[str, Any]:
        if proposal_path is None or proposal_sha is None or decision_path is None or decision_sha is None or authorization_path is None or authorization_sha is None:
            raise AdmissionError("execution requires this controller's live artifact bindings")
        self._require_live_claim(proposal_path, proposal_sha, proposal)
        call = _closed_call(proposal.get("call"))
        self._validate_decision(proposal_path, proposal_sha, proposal, decision, verifier)
        expected_launch = self.topology["launch"] if self.topology is not None else call["topology_key_release"]
        expected_identity = self.topology["identity_binding"] if self.topology is not None else decision["orchestrator_identity"]
        if set(authorization) != _AUTH_KEYS or authorization["schema"] != "firmware-derived-authorization/v2" or authorization["proposal_path"] != str(proposal_path) or authorization["proposal_sha256"] != proposal_sha or authorization["decision_path"] != str(decision_path) or authorization["decision_sha256"] != decision_sha or authorization["launch_intent"] != expected_launch or authorization["orchestrator_identity"] != expected_identity or authorization["call"] != call or authorization["claim"] != proposal["claim"] or authorization["c1_reference"] != call["c1_reference"] or authorization["delegated_reference"] != call["delegated_reference"] or authorization["topology_key_release"] != decision["topology_key_release"] or not isinstance(authorization["expires_monotonic"], (int, float)) or authorization["expires_monotonic"] != decision["expires_monotonic"] or self.clock() >= authorization["expires_monotonic"] or authorization["revoked"] is not False or not isinstance(authorization["one_shot_id"], str) or authorization["one_shot_id"] != call["call_id"]:
            raise AdmissionError("delegated authorization is absent, stale, or mismatched")
        governing = _verify_live_call_inputs(call, self.broker)
        duration = call["plan"]["max_operation_duration_seconds"]
        if min(call["deadline_monotonic"], decision["expires_monotonic"], authorization["expires_monotonic"]) - self.clock() < duration + 60:
            raise AdmissionError("authorization cannot cover operation plus cleanup margin")
        policy = self._policy_admission({**call, "proposal_sha256":proposal_sha, "decision_sha256":decision_sha, "authorization_sha256":authorization_sha})
        call_id = call["call_id"]
        intent = {"proposal_sha256": proposal_sha, "decision_sha256": decision_sha, "authorization_sha256": authorization_sha, "policy_sha256": policy["evaluation_sha256"], "call": call}
        intent_sha = canonical_sha256(intent)
        claims, claim_evidence = self._live_claims, self._live_claim
        assert claims is not None and claim_evidence is not None
        bound = self._intent_bound(call, intent_sha, authorization, authorization_path, authorization_sha, claim_evidence)
        common = {"attempt_id": bound["attempt_id"], "lane_id": bound["lane_id"], "board": bound["board"], "probe_uid": bound["probe_uid"], "target": bound["target"], "profile": bound["profile"], "route": bound["route"], "governing_hashes": bound["governing_hashes"], "c1_reference": bound["c1_reference"], "identity": {"controller": claim_evidence["owner"]}, "bound_operation": bound, "bound_operation_sha256": canonical_sha256(bound)}
        evidence: list[tuple[Path, str]] = []
        try:
            for stage, value in (("proposal", proposal), ("policy-evaluation", policy), ("signed-decision", decision), ("authorization", authorization)):
                evidence.append(self.broker.record(stage, call_id, {**common, **value}, (str(evidence[-1][0]), evidence[-1][1]) if evidence else None))
            config = self.broker.controller_config(call["lane_id"], {})
            config = {**config, "environment": _scrubbed_environment(config)}
            dispatch_start = self.clock()
            operation_deadline = min(dispatch_start + duration, call["deadline_monotonic"], authorization["expires_monotonic"])
            evidence.append(self.broker.record("dispatch-admission", call_id, {**common, "deadline_monotonic": call["deadline_monotonic"], "dispatch_start_monotonic": dispatch_start, "operation_deadline_monotonic": operation_deadline, "authorization_path": str(authorization_path), "authorization_sha256": authorization_sha, "governing_hashes": governing, "policy_evaluation": policy, "policy_evaluation_sha256": policy["evaluation_sha256"], "intent_sha256": intent_sha}, (str(evidence[-1][0]), evidence[-1][1])))
        except BaseException:
            self._release_live_claim()
            raise
        process: StdioProcess | None = None
        transport: _StdioTransport | None = None
        raw: dict[str, Any] | None = None
        failure: BaseException | None = None
        cleanup: dict[str, Any] = {"pid": None, "exact_reaped": False, "classification": "error", "attempts": []}
        cleanup_deadline = min(call["deadline_monotonic"], authorization["expires_monotonic"])
        def operation_remaining() -> float: return operation_deadline - self.clock()
        def cleanup_remaining() -> float: return cleanup_deadline - self.clock()
        try:
            if operation_remaining() <= 0: raise AdmissionError("MCP authority expired before launch")
            process = self.launcher(config)
            cleanup["pid"] = process.pid
            initial_identity = self.identity_provider(process.pid)
            if not isinstance(initial_identity, dict) or initial_identity.get("pid") != process.pid or not isinstance(initial_identity.get("created_utc"), str) or not initial_identity["created_utc"]:
                raise AdmissionError("MCP process creation identity is unavailable")
            cleanup["initial_process_identity"] = initial_identity
            evidence.append(self.broker.record("dispatch", call_id, {**common, "process_identity": initial_identity, "claim": claim_evidence}, (str(evidence[-1][0]), evidence[-1][1])))
            transport = _StdioTransport(process, operation_remaining, self.io_timeout)
            transport.send({"jsonrpc":"2.0", "id":1, "method":"initialize", "params":{"protocolVersion":"2024-11-05", "capabilities":{}, "clientInfo":{"name":"firmware-acceptance", "version":"1"}}}, "initialize")
            transport.receive(1)
            transport.send({"jsonrpc":"2.0", "method":"notifications/initialized", "params":{}}, "initialized notification")
            transport.send({"jsonrpc":"2.0", "id":2, "method":"tools/call", "params":{"name":call["method"], "arguments":call["arguments"]}}, "tools/call")
            raw = transport.receive(2)
            if not isinstance(raw.get("result"), dict): raise AdmissionError("MCP tool result is not an object")
            if raw["result"].get("isError") is True: raise AdmissionError("MCP tool returned isError")
            if operation_remaining() <= 0: raise AdmissionError("MCP operation deadline expired")
            cleanup["classification"] = "success"
            evidence.append(self.broker.record("raw-result", call_id, {**common, "raw_result": raw, "outcome": "PASS"}, (str(evidence[-1][0]), evidence[-1][1])))
            raw_record = json.loads(evidence[-1][0].read_text(encoding="utf-8"))
            common = {**common, "bound_operation": raw_record["bound_operation"], "bound_operation_sha256": raw_record["bound_operation_sha256"]}
        except BaseException as exc:
            failure = exc
            cleanup["classification"] = "INDETERMINATE_TIMEOUT" if operation_remaining() <= 0 else "failure"
            cleanup["error"] = f"{type(exc).__name__}: {exc}"
            if not evidence or evidence[-1][0].name != "05-dispatch.json":
                evidence.append(self.broker.record("dispatch", call_id, {**common, "launch_failure": cleanup["error"], "claim": claim_evidence}, (str(evidence[-1][0]), evidence[-1][1]) if evidence else None))
            # Failure evidence occupies the same immutable raw-result slot, so cleanup can
            # remain durably chained even though no successful tool result exists.
            raw = {"transport_failure": {"classification": cleanup["classification"], "error": cleanup["error"]}}
            evidence.append(self.broker.record("raw-result", call_id, {**common, "raw_result": raw, "outcome": "FAIL"}, (str(evidence[-1][0]), evidence[-1][1])))
            raw_record = json.loads(evidence[-1][0].read_text(encoding="utf-8"))
            common = {**common, "bound_operation": raw_record["bound_operation"], "bound_operation_sha256": raw_record["bound_operation_sha256"]}
        finally:
            if process is not None:
                try:
                    if transport is not None:
                        transport.use_budget(cleanup_remaining)
                        cleanup["cancellation_sent"] = transport.cancel(2, cleanup["classification"]) if failure is not None else False
                        transport.accepting.clear()
                    observed = self.identity_provider(process.pid)
                    cleanup["pre_termination_identity"] = observed
                    if process.poll() is None and observed != cleanup.get("initial_process_identity"):
                        raise AdmissionError("MCP PID creation identity changed during cleanup")
                    if process.poll() is None:
                        cleanup["attempts"].append("terminate")
                        process.terminate()
                    try:
                        cleanup["captured_handle_exit_code"] = process.wait(timeout=max(0.0, min(cleanup_remaining(), self.io_timeout)))
                    except subprocess.TimeoutExpired:
                        cleanup["attempts"].append("kill")
                        process.kill()
                        cleanup["captured_handle_exit_code"] = process.wait(timeout=max(0.0, min(cleanup_remaining(), self.io_timeout)))
                    cleanup["post_reap_identity"] = self.identity_provider(process.pid)
                    if cleanup["post_reap_identity"] == cleanup.get("initial_process_identity"):
                        raise AdmissionError("MCP exact process incarnation remains live after reap")
                    cleanup["exact_reaped"] = True
                except Exception as exc:
                    cleanup["cleanup_error"] = f"{type(exc).__name__}: {exc}"
                if transport is not None:
                    stopped, transport_cleanup = transport.close_and_join()
                    cleanup.update(transport_cleanup)
                    cleanup["helpers_stopped"] = stopped
                    if not stopped: cleanup["cleanup_error"] = "MCP helper thread did not stop"
            prior = evidence[-1] if evidence else None
            try:
                evidence.append(self.broker.record("returning-state-cleanup", call_id, {**common, **cleanup}, (str(prior[0]), prior[1]) if prior else None))
            except BaseException:
                self._release_live_claim()
                raise
        if failure is not None or raw is None or not cleanup["exact_reaped"] or not cleanup.get("helpers_stopped", process is None) or "cleanup_error" in cleanup:
            try: self._release_live_claim()
            finally: raise AdmissionError("MCP response or exact cleanup failed") from failure
        actual = {"raw_result_sha256": raw_result_sha256(raw), "cleanup": cleanup}
        try:
            evidence.append(self.broker.record("result", call_id, {**common, **actual}, (str(evidence[-1][0]), evidence[-1][1])))
            outcome = self.broker.admit(call_id, evidence, self.clock(), verifier)
        finally:
            self._release_live_claim()
        return {"outcome": outcome, "raw_result": raw, "intent_sha256": intent_sha, "actual_sha256": canonical_sha256(actual), "evidence": [(str(path), digest) for path, digest in evidence], "worker_environment": config["worker_environment"]}

    def _policy_admission(self, call: dict[str, Any]) -> dict[str, Any]:
        from .kit import evaluate_call
        return evaluate_call(call, now_monotonic=self.clock(), policy_path=self.broker.policy_path)

    def _claims(self, lane: str, call_id: str) -> Any:
        if self.claims_factory is not None: return self.claims_factory(lane, call_id)
        item = process_snapshot().by_pid.get(os.getpid())
        if item is None: raise AdmissionError("controller process identity unavailable for claims")
        return ResourceClaims(self.broker.root / "claims", lane, call_id, item)

    def _intent_bound(self, call: dict[str, Any], intent_sha: str, authorization: dict[str, Any], authorization_path: Path, authorization_sha: str, claim: dict[str, Any]) -> dict[str, Any]:
        policy_sha = hashlib.sha256(self.broker.policy_path.read_bytes()).hexdigest()
        return {"server_commit":call["server_revision"],"method":call["method"],"method_version":call["method_version"],"arguments":call["arguments"],"policy_sha256":policy_sha,"schema_sha256":call["mcp_schema"]["sha256"],"resource":call["resource"],"plan_sha256":call["plan"]["sha256"],"permission_sha256":call["permission"]["sha256"],"authorization_sha256":authorization_sha,"authorization_path":str(authorization_path),"claim_sha256":claim["sha256"],"claim":claim,"controller_owner":claim["owner"],"call_id":call["call_id"],"attempt_id":call["attempt_id"],"lane_id":call["lane_id"],"board":call["board"],"probe_uid":call["probe_uid"],"target":call["target"],"profile":call["profile"],"route":call["route"],"governing_hashes":{key: value["sha256"] for key, value in call["governing_documents"].items()},"governing_documents":call["governing_documents"],"c1_reference":call["c1_reference"],"delegated_reference":call["delegated_reference"],"board_identity":call["board_identity"],"mcp_schema":call["mcp_schema"],"policy":call["policy"],"plan":{"path":call["plan"]["path"],"sha256":call["plan"]["sha256"]},"permission":{"path":call["permission"]["path"],"sha256":call["permission"]["sha256"]},"deadline_monotonic":call["deadline_monotonic"],"expires_monotonic":authorization["expires_monotonic"],"seed_identity":call["seed_identity"],"target_identity":call["target_identity"],"topology_key_release":call["topology_key_release"],"raw_result_sha256":"PENDING","cleanup_owner":claim["owner"]}

    def _read_bound(self, path: Path, keys: set[str]) -> tuple[dict[str, Any], str]:
        value = self._load_artifact(path, keys)
        return value, hashlib.sha256(path.resolve().read_bytes()).hexdigest()

    def _require_live_claim(self, proposal_path: Path, proposal_sha: str, proposal: dict[str, Any]) -> None:
        if self._live_claims is None or self._live_claim is None or self._proposal_binding != (proposal_path.resolve(), proposal_sha):
            raise AdmissionError("controller was restarted or has no retained proposal claim")
        held = self._live_claims.held
        if len(held) != 1 or proposal.get("claim") != self._live_claim or held[0].get("resource") != self._live_claim["resource"] or held[0].get("owner") != self._live_claim["owner"] or Path(str(held[0].get("path"))).resolve() != Path(self._live_claim["path"]).resolve() or hashlib.sha256(Path(self._live_claim["path"]).read_bytes()).hexdigest() != self._live_claim["sha256"]:
            raise AdmissionError("retained exact resource claim drifted or was substituted")

    def _validate_decision(self, proposal_path: Path, proposal_sha: str, proposal: dict[str, Any], decision: dict[str, Any], verifier: SignatureVerifier) -> None:
        if set(decision) != _DECISION_KEYS or decision["schema"] != "firmware-o-decision/v3" or decision["proposal_path"] != str(proposal_path.resolve()) or decision["proposal_sha256"] != proposal_sha or decision["call"] != proposal["call"] or decision["claim"] != proposal["claim"] or decision["decision"] != "approve" or not isinstance(decision["rationale"], str) or not decision["rationale"] or not isinstance(decision["issued_monotonic"], (int, float)) or not isinstance(decision["expires_monotonic"], (int, float)) or not math.isfinite(decision["issued_monotonic"]) or not math.isfinite(decision["expires_monotonic"]) or decision["issued_monotonic"] > decision["expires_monotonic"] or _reference(decision["topology_key_release"], "decision release") != proposal["call"]["topology_key_release"] or not isinstance(decision["orchestrator_identity"], dict):
            raise AdmissionError("decision does not bind the exact proposal")
        issued = _utc(decision["issued_utc"], "decision issue")
        if not verifier.verify(canonical_decision_payload(decision), decision["signature"], decision["public_key"]): raise AdmissionError("proposal decision signature is invalid")
        if self.topology is not None:
            if decision["public_key"] != self.topology["public_key"] or decision["orchestrator_identity"] != self.topology["identity_binding"] or decision["topology_key_release"] != self.topology["release"] or issued < self.topology["released_utc"]:
                raise AdmissionError("decision predates key release or has an unbound key")

    def _release_live_claim(self) -> None:
        claims, self._live_claims = self._live_claims, None
        self._live_claim, self._proposal_binding, self._authorization_binding = None, None, None
        if claims is not None and claims.release_all(): raise AdmissionError("resource claim release failed")

    def _load_artifact(self, path: Path, keys: set[str]) -> dict[str, Any]:
        reject_linked_path(path)
        resolved = path.resolve()
        if self.broker.root not in resolved.parents or resolved.is_symlink() or not resolved.is_file():
            raise AdmissionError("authorization artifact path is unsafe")
        try: value = json.loads(resolved.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc: raise AdmissionError("authorization artifact is unreadable") from exc
        if not isinstance(value, dict) or set(value) != keys: raise AdmissionError("authorization artifact is not closed")
        return value

    def _load_external(self, path: Path, keys: set[str]) -> dict[str, Any]:
        """Read-only request input; unlike artifacts it need not live beneath broker evidence."""
        reject_linked_path(path)
        resolved = path.resolve()
        if resolved.is_symlink() or not resolved.is_file():
            raise AdmissionError("request path is unsafe")
        try: value = json.loads(resolved.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc: raise AdmissionError("request is unreadable") from exc
        if not isinstance(value, dict) or set(value) != keys: raise AdmissionError("request is not closed")
        return value



def _raw_topology_record(root: Path, name: str) -> tuple[Path, dict[str, Any], str]:
    """Topology is ROOT-owned raw evidence, never canonicalized or repaired by C3."""
    reject_linked_path(root)
    root = root.resolve()
    path = root / name
    reject_linked_path(path)
    if root.is_symlink() or path.parent != root or path.is_symlink() or not path.is_file():
        raise AdmissionError("ROOT topology record is unsafe")
    raw = path.read_bytes()
    try: value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc: raise AdmissionError("ROOT topology record is malformed") from exc
    if not isinstance(value, dict): raise AdmissionError("ROOT topology record is not an object")
    return path.resolve(), value, hashlib.sha256(raw).hexdigest()


def _utc(value: Any, label: str) -> _datetime.datetime:
    if not isinstance(value, str): raise AdmissionError(label + " must be UTC text")
    try: parsed = _datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc: raise AdmissionError(label + " is malformed") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != _datetime.timedelta(0): raise AdmissionError(label + " is not UTC")
    return parsed


def load_root_topology(root: Path) -> dict[str, Any]:
    """Validate the launch -> identity -> key-release chain using raw-file hashes only."""
    intent_path, intent, intent_sha = _raw_topology_record(root, "ORCHESTRATOR_LAUNCH_INTENT.json")
    identity_path, identity, identity_sha = _raw_topology_record(root, "ORCHESTRATOR_IDENTITY.json")
    release_path, release, release_sha = _raw_topology_record(root, "ORCHESTRATOR_KEY_RELEASE.json")
    required_intent = {"schema", "attempt_id", "nonce", "public_key", "model", "reasoning_effort", "service_tier", "issued_utc"}
    required_identity = required_intent | {"intent_path", "intent_sha256", "pid", "created_utc", "issued_utc", "thread_id", "acknowledgement"}
    required_release = {"schema", "identity_path", "identity_sha256", "acknowledgement", "released_utc"}
    if set(intent) != required_intent or set(identity) != required_identity or set(release) != required_release:
        raise AdmissionError("ROOT topology fields are incomplete")
    if intent["schema"] != "firmware-orchestrator-launch-intent/v1" or identity["schema"] != "firmware-orchestrator-identity/v1" or release["schema"] != "firmware-orchestrator-key-release/v1":
        raise AdmissionError("ROOT topology schemas are not exact")
    if identity["intent_path"] != str(intent_path) or identity["intent_sha256"] != intent_sha or release["identity_path"] != str(identity_path) or release["identity_sha256"] != identity_sha:
        raise AdmissionError("ROOT topology raw-file binding drifted")
    for key in ("attempt_id", "nonce", "public_key", "model", "reasoning_effort", "service_tier"):
        if identity[key] != intent[key]: raise AdmissionError("ROOT identity substituted launch binding")
    if any(not isinstance(intent[key], str) or not intent[key] for key in ("attempt_id", "nonce", "public_key")) or intent["model"] != "gpt-5.6-sol" or intent["reasoning_effort"] != "high" or intent["service_tier"] != "priority":
        raise AdmissionError("ROOT launch model assignment is invalid")
    if release["acknowledgement"] != identity["acknowledgement"] or not isinstance(identity["pid"], int) or identity["pid"] <= 0 or any(not isinstance(identity[key], str) or not identity[key] for key in ("thread_id", "acknowledgement")):
        raise AdmissionError("ROOT identity acknowledgement is invalid")
    intent_utc, created_utc, identity_utc, release_utc = _utc(intent["issued_utc"], "intent issue"), _utc(identity["created_utc"], "identity creation"), _utc(identity["issued_utc"], "identity issue"), _utc(release["released_utc"], "key release")
    if created_utc < intent_utc or identity_utc < created_utc or release_utc < identity_utc:
        raise AdmissionError("ROOT topology timing is invalid")
    # monotonic decisions used by the existing controller are separately checked against this
    # release marker; the UTC chain remains immutable evidence for external audit.
    return {"public_key": intent["public_key"], "attempt_id": intent["attempt_id"], "identity": identity,
            "launch": {"path": str(intent_path), "sha256": intent_sha},
            "identity_binding": {"path": str(identity_path), "sha256": identity_sha},
            "release": {"path": str(release_path), "sha256": release_sha}, "released_utc": release_utc}


def main(argv: list[str] | None = None) -> int:
    """Single-controller production entry point; it has no endpoint or launch seam."""
    parser = argparse.ArgumentParser(prog="firmware-acceptance-controller")
    parser.add_argument("--request", required=True)
    parser.add_argument("--proposal", required=True)
    parser.add_argument("--decision", required=True)
    parser.add_argument("--authorization", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--seed", required=True)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--templates", required=True)
    parser.add_argument("--topology-root", required=True)
    args = parser.parse_args(argv)
    topology = load_root_topology(Path(args.topology_root))
    verifier = Ed25519Verifier()
    broker = AcceptanceBroker(Path(args.root), Path(args.seed), Path(args.policy), Path(args.templates))
    controller = FirmwareAcceptanceController(broker, topology=topology)
    result = controller.run_lifecycle(Path(args.request), Path(args.proposal), Path(args.decision), Path(args.authorization), verifier, result_path=Path(args.result))
    print(json.dumps({"outcome": result["outcome"], "evidence": result["evidence"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
