"""Bounded C3-HARNESS MCP stdio owner; transport is injectable for host-only tests."""

from __future__ import annotations

import hashlib
import json
import subprocess
import time
from pathlib import Path
from typing import Any, Callable, Protocol

from orchestrator_harness.processes import process_snapshot
from .kit import AcceptanceBroker, AdmissionError, SignatureVerifier, _safe_child, _write_new, canonical_sha256, raw_result_sha256


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


class FirmwareAcceptanceController:
    """Two-phase controller that alone owns the MCP stdio process and physical capability."""

    def __init__(self, broker: AcceptanceBroker, *, launcher: Launcher | None = None, clock: Callable[[], float] = time.monotonic, identity_provider: Callable[[int], dict[str, Any]] = _process_identity) -> None:
        self.broker, self.launcher, self.clock, self.identity_provider = broker, launcher or _launch, clock, identity_provider

    def create_proposal(self, request: dict[str, Any]) -> dict[str, Any]:
        if set(request) != {"call", "lane_id", "claim"} or not isinstance(request["call"], dict):
            raise AdmissionError("proposal must contain only call, lane_id, and claim")
        if request["lane_id"] != request["call"].get("lane_id") or not isinstance(request["claim"], str) or not request["claim"]:
            raise AdmissionError("proposal lane or claim mismatch")
        if _FORBIDDEN_REQUEST_KEYS & set(request["call"]):
            raise AdmissionError("workers cannot supply an MCP endpoint, command, environment, or handle")
        proposal = {"schema": "firmware-controller-proposal/v1", "request": json.loads(json.dumps(request, sort_keys=True))}
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
        proposal = self._load_artifact(proposal_path, {"schema", "request", "sha256"})
        if proposal.get("sha256") != canonical_sha256({"schema": proposal.get("schema"), "request": proposal.get("request")}):
            raise AdmissionError("proposal bytes were mutated or substituted")
        decision = self._load_artifact(decision_path, {"proposal_sha256", "signature", "public_key"})
        authorization = self._load_artifact(authorization_path, {"proposal_sha256", "expires_monotonic", "delegated_authority"})
        return self.execute(proposal, decision, authorization, verifier)

    def execute(self, proposal: dict[str, Any], decision: dict[str, Any], authorization: dict[str, Any], verifier: SignatureVerifier) -> dict[str, Any]:
        if set(decision) != {"proposal_sha256", "signature", "public_key"} or decision["proposal_sha256"] != proposal.get("sha256"):
            raise AdmissionError("decision does not bind the exact proposal")
        if not verifier.verify(str(proposal["sha256"]).encode(), decision["signature"], decision["public_key"]):
            raise AdmissionError("proposal decision signature is invalid")
        call = proposal["request"]["call"]
        if set(authorization) != {"proposal_sha256", "expires_monotonic", "delegated_authority"} or authorization["proposal_sha256"] != proposal["sha256"] or not isinstance(authorization["expires_monotonic"], (int, float)) or self.clock() >= authorization["expires_monotonic"] or not authorization["delegated_authority"]:
            raise AdmissionError("delegated authorization is absent, stale, or mismatched")
        policy = self._policy_admission(call)
        call_id = call["call_id"]
        intent = {"proposal_sha256": proposal["sha256"], "decision_sha256": canonical_sha256(decision), "authorization_sha256": canonical_sha256(authorization), "policy_sha256": policy["evaluation_sha256"], "call": call}
        intent_sha = canonical_sha256(intent)
        bound = self._intent_bound(call, intent_sha, authorization)
        common = {"attempt_id": bound["attempt_id"], "lane_id": bound["lane_id"], "board": bound["board"], "probe_uid": bound["probe_uid"], "target": bound["target"], "profile": bound["profile"], "route": bound["route"], "governing_hashes": bound["governing_hashes"], "c1_reference": bound["c1_reference"], "identity": {"controller": "C3-HARNESS"}, "bound_operation": bound, "bound_operation_sha256": canonical_sha256(bound)}
        evidence: list[tuple[Path, str]] = []
        for stage, value in (("proposal", proposal), ("policy-evaluation", policy), ("signed-decision", decision), ("authorization", authorization), ("dispatch-admission", {"deadline_monotonic": call["deadline_monotonic"], "intent_sha256": intent_sha})):
            evidence.append(self.broker.record(stage, call_id, {**common, **value}, (str(evidence[-1][0]), evidence[-1][1]) if evidence else None))
        config = self.broker.controller_config(call["lane_id"], {})
        process: StdioProcess | None = None
        raw: dict[str, Any] | None = None
        cleanup: dict[str, Any] = {"pid": None, "exact_reaped": False}
        try:
            process = self.launcher(config)
            cleanup["process_identity"] = self.identity_provider(process.pid)
            evidence.append(self.broker.record("dispatch", call_id, {**common, "process_identity": cleanup["process_identity"], "claim": proposal["request"]["claim"]}, (str(evidence[-1][0]), evidence[-1][1])))
            self._send(process, 1, "initialize", {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "firmware-acceptance", "version": "1"}})
            self._receive(process, 1)
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
                    if process.poll() is None:
                        process.terminate()
                    process.wait(timeout=5)
                    cleanup["exact_reaped"] = process.poll() is not None
                except Exception as exc:
                    cleanup["error"] = type(exc).__name__
            prior = evidence[-1] if evidence else None
            evidence.append(self.broker.record("returning-state-cleanup", call_id, {**common, **cleanup}, (str(prior[0]), prior[1]) if prior else None))
        if raw is None or not cleanup["exact_reaped"]:
            raise AdmissionError("MCP response or exact cleanup is ambiguous")
        actual = {"raw_result_sha256": raw_result_sha256(raw), "cleanup": cleanup}
        evidence.append(self.broker.record("result", call_id, {**common, **actual}, (str(evidence[-1][0]), evidence[-1][1])))
        outcome = self.broker.admit(call_id, evidence, self.clock(), verifier)
        return {"outcome": outcome, "raw_result": raw, "intent_sha256": intent_sha, "actual_sha256": canonical_sha256(actual), "evidence": [(str(path), digest) for path, digest in evidence], "worker_environment": config["worker_environment"]}

    def _policy_admission(self, call: dict[str, Any]) -> dict[str, Any]:
        from .kit import evaluate_call
        return evaluate_call(call, now_monotonic=self.clock())

    def _record(self, call_id: str, stage: str, value: dict[str, Any], previous: tuple[str, str] | None) -> tuple[str, str]:
        path = _safe_child(self.broker.root, "controller-calls", call_id, f"{len(stage):02d}-{stage}.json")
        if previous is not None:
            value = {**value, "previous_path": previous[0], "previous_sha256": previous[1]}
        return str(path), _write_new(path, {"schema": "firmware-controller-evidence/v1", "stage": stage, **value})

    def _intent_bound(self, call: dict[str, Any], intent_sha: str, authorization: dict[str, Any]) -> dict[str, Any]:
        policy_sha = hashlib.sha256(self.broker.policy_path.read_bytes()).hexdigest()
        return {"server_commit":"f003f84a7df51cd8595a3203c62e225b21da2a22","method":call["method"],"method_version":1,"arguments":call["arguments"],"policy_sha256":policy_sha,"schema_sha256":intent_sha,"plan_sha256":canonical_sha256(call["plan"]),"permission_sha256":canonical_sha256(call["permission"]),"authorization_sha256":canonical_sha256(authorization),"claim_sha256":canonical_sha256({"claim": authorization["delegated_authority"]}),"call_id":call["call_id"],"attempt_id":call.get("attempt_id", call["call_id"]),"lane_id":call["lane_id"],"board":call["board"],"probe_uid":call["probe_uid"],"target":call["target"],"profile":call["profile"],"route":call.get("route", "rediscover"),"governing_hashes":call.get("governing_hashes", {"controller": "local"}),"c1_reference":call.get("c1_reference", {"path": "C1", "sha256": "local"}),"deadline_monotonic":call["deadline_monotonic"],"expires_monotonic":authorization["expires_monotonic"],"seed_identity":call.get("seed_identity", {"manifest": "local"}),"target_identity":call.get("target_identity", {"commit": "local"}),"raw_result_sha256":"PENDING","cleanup_owner":"C3-HARNESS"}

    def _load_artifact(self, path: Path, keys: set[str]) -> dict[str, Any]:
        resolved = path.resolve()
        if self.broker.root not in resolved.parents or resolved.is_symlink() or not resolved.is_file():
            raise AdmissionError("authorization artifact path is unsafe")
        try: value = json.loads(resolved.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc: raise AdmissionError("authorization artifact is unreadable") from exc
        if not isinstance(value, dict) or set(value) != keys: raise AdmissionError("authorization artifact is not closed")
        return value

    @staticmethod
    def _send(process: StdioProcess, request_id: int, method: str, params: dict[str, Any]) -> None:
        if process.stdin is None:
            raise AdmissionError("MCP stdin is unavailable")
        process.stdin.write((json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}, separators=(",", ":")) + "\n").encode())
        process.stdin.flush()

    @staticmethod
    def _receive(process: StdioProcess, request_id: int) -> dict[str, Any]:
        if process.stdout is None:
            raise AdmissionError("MCP stdout is unavailable")
        line = process.stdout.readline()
        try:
            value = json.loads(line.decode("utf-8"))
        except (AttributeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AdmissionError("MCP stdio framing is invalid") from exc
        if not isinstance(value, dict) or value.get("jsonrpc") != "2.0" or value.get("id") != request_id or "error" in value:
            raise AdmissionError("MCP response is invalid")
        return value
