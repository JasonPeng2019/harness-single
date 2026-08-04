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
from pathlib import Path
from typing import Any, Callable, Protocol

from orchestrator_harness.models import ProcessInfo
from orchestrator_harness.processes import process_snapshot
from orchestrator_harness.resource_locks import ResourceClaims, ResourceLockError
from .kit import AcceptanceBroker, AdmissionError, SignatureVerifier, _safe_child, _write_new, canonical_decision_payload, canonical_sha256, raw_result_sha256


class StdioProcess(Protocol):
    pid: int
    stdin: Any
    stdout: Any
    stderr: Any
    def poll(self) -> int | None: ...
    def terminate(self) -> None: ...
    def wait(self, timeout: float | None = None) -> int: ...


Launcher = Callable[[dict[str, Any]], StdioProcess]
_FORBIDDEN_REQUEST_KEYS = {"endpoint", "stdio", "mcp_command", "environment", "credential", "process", "handle"}
_CALL_KEYS = {"call_id", "attempt_id", "lane_id", "board", "probe_uid", "target", "profile", "route", "method", "method_version", "arguments", "proposal_sha256", "decision_sha256", "authorization_sha256", "deadline_monotonic", "plan", "permission", "c1_reference", "delegated_reference", "topology", "claim", "governing_hashes", "seed_identity", "target_identity"}
_REFERENCE_KEYS = {"path", "sha256"}


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
    for key in ("call_id", "attempt_id", "lane_id", "board", "probe_uid", "target", "profile", "method", "proposal_sha256", "decision_sha256", "authorization_sha256"):
        if not isinstance(call[key], str) or not call[key]: raise AdmissionError("call identity is incomplete")
    if call["route"] is not None and not isinstance(call["route"], str): raise AdmissionError("route must be canonical null or an exact route")
    if not isinstance(call["method_version"], int) or isinstance(call["method_version"], bool) or not isinstance(call["arguments"], dict): raise AdmissionError("call method binding is invalid")
    if not isinstance(call["deadline_monotonic"], (int, float)) or isinstance(call["deadline_monotonic"], bool): raise AdmissionError("call deadline is invalid")
    for key in ("c1_reference", "delegated_reference", "topology", "claim", "seed_identity", "target_identity"):
        _reference(call[key], key)
    if not isinstance(call["governing_hashes"], dict) or set(call["governing_hashes"]) != {"goal", "plan", "readiness", "policy", "topology"} or not all(isinstance(value, str) and value for value in call["governing_hashes"].values()): raise AdmissionError("governing hashes are incomplete")
    if not isinstance(call["plan"], dict) or set(call["plan"]) != {"path", "sha256", "max_operation_duration_seconds"} or not isinstance(call["plan"]["max_operation_duration_seconds"], int) or call["plan"]["max_operation_duration_seconds"] <= 0: raise AdmissionError("plan authority is invalid")
    if not isinstance(call["permission"], dict) or set(call["permission"]) != {"path", "sha256", "granted"} or call["permission"]["granted"] is not True: raise AdmissionError("permission authority is invalid")
    return json.loads(json.dumps(call, sort_keys=True))


def _launch(config: dict[str, Any]) -> StdioProcess:
    return subprocess.Popen(  # type: ignore[return-value]
        config["mcp_command"], cwd=config["working_directory"], env=config["environment"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )


def _process_identity(pid: int) -> dict[str, Any]:
    item = process_snapshot().by_pid.get(pid)
    if item is None or item.created_utc is None:
        raise AdmissionError("MCP process creation identity is unavailable")
    return {"pid": item.pid, "created_utc": item.created_utc.isoformat()}


def _bounded(call: Callable[[], Any], seconds: float, label: str) -> Any:
    """Windows-safe bounded pipe operation; the blocked reader is daemonized."""
    result: list[tuple[bool, Any]] = []
    done = threading.Event()
    def run() -> None:
        try: result.append((True, call()))
        except BaseException as exc: result.append((False, exc))
        finally: done.set()
    threading.Thread(target=run, daemon=True, name="firmware-mcp-" + label).start()
    if not done.wait(seconds): raise AdmissionError("MCP " + label + " timed out")
    ok, value = result[0]
    if not ok: raise AdmissionError("MCP " + label + " failed") from value
    return value


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

    def __init__(self, broker: AcceptanceBroker, *, launcher: Launcher | None = None, clock: Callable[[], float] = time.monotonic, identity_provider: Callable[[int], dict[str, Any]] = _process_identity, claims_factory: Callable[[str, str], Any] | None = None, io_timeout: float = 5.0) -> None:
        self.broker, self.launcher, self.clock, self.identity_provider = broker, launcher or _launch, clock, identity_provider
        self.claims_factory, self.io_timeout = claims_factory, io_timeout

    def create_proposal(self, request: dict[str, Any]) -> dict[str, Any]:
        if set(request) != {"call"}: raise AdmissionError("proposal must contain exactly one closed call authority")
        call = _closed_call(request["call"])
        proposal = {"schema": "firmware-controller-proposal/v2", "call": call}
        proposal["sha256"] = canonical_sha256(proposal)
        return proposal

    def publish_proposal(self, path: Path, request: dict[str, Any]) -> dict[str, Any]:
        """Create-once canonical proposal for O to sign; only this controller writes it."""
        proposal = self.create_proposal(request)
        resolved = path.resolve()
        if self.broker.root not in resolved.parents or resolved.is_symlink():
            raise AdmissionError("proposal path escapes controller root")
        _write_new(resolved, proposal)
        return {**proposal, "path": str(resolved)}

    def execute_artifacts(self, proposal_path: Path, decision_path: Path, authorization_path: Path, verifier: SignatureVerifier) -> dict[str, Any]:
        proposal = self._load_artifact(proposal_path, {"schema", "call", "sha256"})
        if proposal.get("sha256") != canonical_sha256({"schema": proposal.get("schema"), "call": proposal.get("call")}):
            raise AdmissionError("proposal bytes were mutated or substituted")
        decision = self._load_artifact(decision_path, {"schema", "proposal_path", "proposal_sha256", "call", "decision", "issued_monotonic", "expires_monotonic", "topology", "public_key", "signature"})
        authorization = self._load_artifact(authorization_path, {"schema", "proposal_path", "proposal_sha256", "decision_path", "decision_sha256", "call", "expires_monotonic", "one_shot_id", "revoked"})
        return self.execute(proposal, decision, authorization, verifier)

    def execute(self, proposal: dict[str, Any], decision: dict[str, Any], authorization: dict[str, Any], verifier: SignatureVerifier) -> dict[str, Any]:
        call = _closed_call(proposal.get("call"))
        if set(decision) != {"schema", "proposal_path", "proposal_sha256", "call", "decision", "issued_monotonic", "expires_monotonic", "topology", "public_key", "signature"} or decision["schema"] != "firmware-o-decision/v2" or decision["proposal_sha256"] != proposal.get("sha256") or decision["call"] != call or decision["decision"] != "approve" or not isinstance(decision["issued_monotonic"], (int, float)) or not isinstance(decision["expires_monotonic"], (int, float)) or decision["issued_monotonic"] > decision["expires_monotonic"] or _reference(decision["topology"], "decision topology") != call["topology"]:
            raise AdmissionError("decision does not bind the exact proposal")
        if not verifier.verify(canonical_decision_payload(decision), decision["signature"], decision["public_key"]):
            raise AdmissionError("proposal decision signature is invalid")
        if set(authorization) != {"schema", "proposal_path", "proposal_sha256", "decision_path", "decision_sha256", "call", "expires_monotonic", "one_shot_id", "revoked"} or authorization["schema"] != "firmware-derived-authorization/v2" or authorization["proposal_sha256"] != proposal["sha256"] or authorization["decision_sha256"] != canonical_sha256(decision) or authorization["call"] != call or not isinstance(authorization["expires_monotonic"], (int, float)) or authorization["expires_monotonic"] != decision["expires_monotonic"] or self.clock() >= authorization["expires_monotonic"] or authorization["revoked"] is not False or not isinstance(authorization["one_shot_id"], str) or authorization["one_shot_id"] != call["call_id"]:
            raise AdmissionError("delegated authorization is absent, stale, or mismatched")
        policy = self._policy_admission(call)
        call_id = call["call_id"]
        intent = {"proposal_sha256": proposal["sha256"], "decision_sha256": canonical_sha256(decision), "authorization_sha256": canonical_sha256(authorization), "policy_sha256": policy["evaluation_sha256"], "call": call}
        intent_sha = canonical_sha256(intent)
        claims = self._claims(call["lane_id"], call_id)
        try:
            claims.acquire_all([call["board"]], on_wait=lambda finding: (_ for _ in ()).throw(AdmissionError("resource claim unavailable: " + str(finding.get("state")))))
        except ResourceLockError as exc:
            raise AdmissionError("resource claim acquisition failed") from exc
        held = claims.held
        if len(held) != 1 or not isinstance(held[0].get("path"), str) or held[0].get("resource") != call["board"] or held[0].get("owner") is None:
            _ = claims.release_all()
            raise AdmissionError("resource claim evidence is unavailable")
        claim_path = Path(str(held[0]["path"]))
        claim_evidence = {"resource": held[0]["resource"], "path": str(claim_path), "sha256": hashlib.sha256(claim_path.read_bytes()).hexdigest(), "owner": held[0].get("owner")}
        if claim_evidence["path"] != call["claim"]["path"] or claim_evidence["sha256"] != call["claim"]["sha256"]:
            _ = claims.release_all()
            raise AdmissionError("acquired resource claim differs from proposal")
        bound = self._intent_bound(call, intent_sha, authorization, claim_evidence["sha256"])
        common = {"attempt_id": bound["attempt_id"], "lane_id": bound["lane_id"], "board": bound["board"], "probe_uid": bound["probe_uid"], "target": bound["target"], "profile": bound["profile"], "route": bound["route"], "governing_hashes": bound["governing_hashes"], "c1_reference": bound["c1_reference"], "identity": {"controller": "C3-HARNESS"}, "bound_operation": bound, "bound_operation_sha256": canonical_sha256(bound)}
        evidence: list[tuple[Path, str]] = []
        try:
            for stage, value in (("proposal", proposal), ("policy-evaluation", policy), ("signed-decision", decision), ("authorization", authorization), ("dispatch-admission", {"deadline_monotonic": call["deadline_monotonic"], "intent_sha256": intent_sha})):
                evidence.append(self.broker.record(stage, call_id, {**common, **value}, (str(evidence[-1][0]), evidence[-1][1]) if evidence else None))
            config = self.broker.controller_config(call["lane_id"], {})
            config = {**config, "environment": _scrubbed_environment(config)}
        except BaseException:
            if claims.release_all(): raise AdmissionError("resource claim release failed")
            raise
        process: StdioProcess | None = None
        raw: dict[str, Any] | None = None
        cleanup: dict[str, Any] = {"pid": None, "exact_reaped": False}
        try:
            process = self.launcher(config)
            cleanup["process_identity"] = self.identity_provider(process.pid)
            evidence.append(self.broker.record("dispatch", call_id, {**common, "process_identity": cleanup["process_identity"], "claim": claim_evidence}, (str(evidence[-1][0]), evidence[-1][1])))
            stderr = self._drain_stderr(process)
            self._send(process, 1, "initialize", {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "firmware-acceptance", "version": "1"}})
            self._receive(process, 1)
            self._notify(process, "notifications/initialized", {})
            self._send(process, 2, "tools/call", {"name": call["method"], "arguments": call["arguments"]})
            raw = self._receive(process, 2)
            evidence.append(self.broker.record("raw-result", call_id, {**common, "raw_result": raw, "outcome": "PASS"}, (str(evidence[-1][0]), evidence[-1][1])))
            raw_record = json.loads(evidence[-1][0].read_text(encoding="utf-8"))
            common = {**common, "bound_operation": raw_record["bound_operation"], "bound_operation_sha256": raw_record["bound_operation_sha256"]}
        finally:
            if process is not None:
                try:
                    if process.stdin is not None:
                        process.stdin.close()
                    if cleanup.get("process_identity") != self.identity_provider(process.pid):
                        raise AdmissionError("MCP PID creation identity changed during cleanup")
                    if process.poll() is None:
                        process.terminate()
                    _bounded(lambda: process.wait(timeout=self.io_timeout), self.io_timeout, "wait")
                    cleanup["exact_reaped"] = process.poll() is not None
                    cleanup["stderr_sha256"] = hashlib.sha256(stderr()).hexdigest() if 'stderr' in locals() else hashlib.sha256(b"").hexdigest()
                    for stream in (process.stdout, process.stderr):
                        if stream is not None: stream.close()
                except Exception as exc:
                    cleanup["error"] = type(exc).__name__
            prior = evidence[-1] if evidence else None
            try:
                evidence.append(self.broker.record("returning-state-cleanup", call_id, {**common, **cleanup}, (str(prior[0]), prior[1]) if prior else None))
            except BaseException:
                if claims.release_all(): raise AdmissionError("resource claim release failed")
                raise
        if raw is None or not cleanup["exact_reaped"]:
            if claims.release_all(): raise AdmissionError("resource claim release failed")
            raise AdmissionError("MCP response or exact cleanup is ambiguous")
        actual = {"raw_result_sha256": raw_result_sha256(raw), "cleanup": cleanup}
        evidence.append(self.broker.record("result", call_id, {**common, **actual}, (str(evidence[-1][0]), evidence[-1][1])))
        try:
            outcome = self.broker.admit(call_id, evidence, self.clock(), verifier)
        finally:
            if claims.release_all(): raise AdmissionError("resource claim release failed")
        return {"outcome": outcome, "raw_result": raw, "intent_sha256": intent_sha, "actual_sha256": canonical_sha256(actual), "evidence": [(str(path), digest) for path, digest in evidence], "worker_environment": config["worker_environment"]}

    def _policy_admission(self, call: dict[str, Any]) -> dict[str, Any]:
        from .kit import evaluate_call
        return evaluate_call(call, now_monotonic=self.clock())

    def _claims(self, lane: str, call_id: str) -> Any:
        if self.claims_factory is not None: return self.claims_factory(lane, call_id)
        item = process_snapshot().by_pid.get(os.getpid())
        if item is None: raise AdmissionError("controller process identity unavailable for claims")
        return ResourceClaims(self.broker.root / "claims", lane, call_id, item)

    def _intent_bound(self, call: dict[str, Any], intent_sha: str, authorization: dict[str, Any], claim_sha: str) -> dict[str, Any]:
        policy_sha = hashlib.sha256(self.broker.policy_path.read_bytes()).hexdigest()
        return {"server_commit":"f003f84a7df51cd8595a3203c62e225b21da2a22","method":call["method"],"method_version":call["method_version"],"arguments":call["arguments"],"policy_sha256":policy_sha,"schema_sha256":intent_sha,"plan_sha256":canonical_sha256(call["plan"]),"permission_sha256":canonical_sha256(call["permission"]),"authorization_sha256":canonical_sha256(authorization),"claim_sha256":claim_sha,"call_id":call["call_id"],"attempt_id":call["attempt_id"],"lane_id":call["lane_id"],"board":call["board"],"probe_uid":call["probe_uid"],"target":call["target"],"profile":call["profile"],"route":call["route"],"governing_hashes":call["governing_hashes"],"c1_reference":call["c1_reference"],"deadline_monotonic":call["deadline_monotonic"],"expires_monotonic":authorization["expires_monotonic"],"seed_identity":call["seed_identity"],"target_identity":call["target_identity"],"raw_result_sha256":"PENDING","cleanup_owner":"C3-HARNESS"}

    def _load_artifact(self, path: Path, keys: set[str]) -> dict[str, Any]:
        resolved = path.resolve()
        if self.broker.root not in resolved.parents or resolved.is_symlink() or not resolved.is_file():
            raise AdmissionError("authorization artifact path is unsafe")
        try: value = json.loads(resolved.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc: raise AdmissionError("authorization artifact is unreadable") from exc
        if not isinstance(value, dict) or set(value) != keys: raise AdmissionError("authorization artifact is not closed")
        return value

    def _send(self, process: StdioProcess, request_id: int, method: str, params: dict[str, Any]) -> None:
        if process.stdin is None:
            raise AdmissionError("MCP stdin is unavailable")
        data = (json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}, separators=(",", ":")) + "\n").encode()
        _bounded(lambda: process.stdin.write(data), self.io_timeout, "write")
        _bounded(process.stdin.flush, self.io_timeout, "flush")

    def _notify(self, process: StdioProcess, method: str, params: dict[str, Any]) -> None:
        if process.stdin is None: raise AdmissionError("MCP stdin is unavailable")
        data = (json.dumps({"jsonrpc": "2.0", "method": method, "params": params}, separators=(",", ":")) + "\n").encode()
        _bounded(lambda: process.stdin.write(data), self.io_timeout, "notification write")
        _bounded(process.stdin.flush, self.io_timeout, "notification flush")

    def _receive(self, process: StdioProcess, request_id: int) -> dict[str, Any]:
        if process.stdout is None:
            raise AdmissionError("MCP stdout is unavailable")
        line = _bounded(process.stdout.readline, self.io_timeout, "read")
        try:
            value = json.loads(line.decode("utf-8"))
        except (AttributeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AdmissionError("MCP stdio framing is invalid") from exc
        if not isinstance(value, dict) or value.get("jsonrpc") != "2.0" or value.get("id") != request_id or "error" in value:
            raise AdmissionError("MCP response is invalid")
        return value

    def _drain_stderr(self, process: StdioProcess) -> Callable[[], bytes]:
        captured = bytearray(); lock = threading.Lock(); cap = 64 * 1024
        if process.stderr is None: return lambda: b""
        def drain() -> None:
            while True:
                try: chunk = process.stderr.read(4096)
                except Exception: return
                if not chunk: return
                if isinstance(chunk, str): chunk = chunk.encode("utf-8", "replace")
                with lock:
                    if len(captured) < cap: captured.extend(chunk[:cap - len(captured)])
        threading.Thread(target=drain, daemon=True, name="firmware-mcp-stderr").start()
        return lambda: bytes(captured)


def main(argv: list[str] | None = None) -> int:
    """Dedicated structured-artifact entry point; production requires an injected verifier."""
    parser = argparse.ArgumentParser(prog="firmware-acceptance-controller")
    parser.add_argument("--proposal", required=True)
    parser.add_argument("--decision", required=True)
    parser.add_argument("--authorization", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--seed", required=True)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--templates", required=True)
    parser.add_argument("--topology-root", required=True)
    args = parser.parse_args(argv)
    topology = Path(args.topology_root).resolve()
    intent = json.loads((topology / "ORCHESTRATOR_LAUNCH_INTENT.json").read_text(encoding="utf-8"))
    identity = json.loads((topology / "ORCHESTRATOR_IDENTITY.json").read_text(encoding="utf-8"))
    release = json.loads((topology / "ORCHESTRATOR_KEY_RELEASE.json").read_text(encoding="utf-8"))
    if not all(isinstance(item, dict) for item in (intent, identity, release)) or identity.get("intent_sha256") != canonical_sha256(intent) or release.get("identity_sha256") != canonical_sha256(identity) or identity.get("public_key") != intent.get("public_key"):
        raise SystemExit("ROOT topology launch identity is incomplete or substituted")
    try:
        decision = json.loads(Path(args.decision).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit("O decision is unreadable") from exc
    if decision.get("public_key") != intent.get("public_key"):
        raise SystemExit("O decision key is not bound to ROOT launch intent")
    verifier = Ed25519Verifier()
    broker = AcceptanceBroker(Path(args.root), Path(args.seed), Path(args.policy), Path(args.templates))
    result = FirmwareAcceptanceController(broker).execute_artifacts(Path(args.proposal), Path(args.decision), Path(args.authorization), verifier)
    print(json.dumps({"outcome": result["outcome"], "evidence": result["evidence"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
