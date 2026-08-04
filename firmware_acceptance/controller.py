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
        proposal = {"schema": "firmware-controller-proposal/v1", "request": request}
        proposal["sha256"] = canonical_sha256(proposal)
        return proposal

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
        evidence: list[tuple[str, str]] = []
        for stage, value in (("proposal", proposal), ("policy-evaluation", policy), ("signed-decision", decision), ("authorization", authorization), ("dispatch-admission", {"intent_sha256": intent_sha})):
            evidence.append(self._record(call_id, stage, value, evidence[-1] if evidence else None))
        config = self.broker.controller_config(call["lane_id"], {})
        process: StdioProcess | None = None
        raw: dict[str, Any] | None = None
        cleanup: dict[str, Any] = {"pid": None, "exact_reaped": False}
        try:
            process = self.launcher(config)
            cleanup["process_identity"] = self.identity_provider(process.pid)
            evidence.append(self._record(call_id, "dispatch", {"intent_sha256": intent_sha, "process_identity": cleanup["process_identity"], "claim": proposal["request"]["claim"]}, evidence[-1]))
            self._send(process, 1, "initialize", {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "firmware-acceptance", "version": "1"}})
            self._receive(process, 1)
            self._send(process, 2, "tools/call", {"name": call["method"], "arguments": call["arguments"]})
            raw = self._receive(process, 2)
            evidence.append(self._record(call_id, "raw-result", {"intent_sha256": intent_sha, "raw_result": raw, "raw_result_sha256": raw_result_sha256(raw)}, evidence[-1]))
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
            evidence.append(self._record(call_id, "returning-state-cleanup", cleanup, prior))
        if raw is None or not cleanup["exact_reaped"]:
            raise AdmissionError("MCP response or exact cleanup is ambiguous")
        actual = {"intent_sha256": intent_sha, "raw_result_sha256": raw_result_sha256(raw), "cleanup": cleanup}
        evidence.append(self._record(call_id, "result", actual, evidence[-1]))
        return {"outcome": "PASS", "raw_result": raw, "intent_sha256": intent_sha, "actual_sha256": canonical_sha256(actual), "evidence": evidence, "worker_environment": config["worker_environment"]}

    def _policy_admission(self, call: dict[str, Any]) -> dict[str, Any]:
        from .kit import evaluate_call
        return evaluate_call(call, now_monotonic=self.clock())

    def _record(self, call_id: str, stage: str, value: dict[str, Any], previous: tuple[str, str] | None) -> tuple[str, str]:
        path = _safe_child(self.broker.root, "controller-calls", call_id, f"{len(stage):02d}-{stage}.json")
        if previous is not None:
            value = {**value, "previous_path": previous[0], "previous_sha256": previous[1]}
        return str(path), _write_new(path, {"schema": "firmware-controller-evidence/v1", "stage": stage, **value})

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
