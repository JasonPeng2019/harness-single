"""Create the bounded C3 evidence consumed by the server-limitation broker.

This module is deliberately only an evidence adapter.  It neither launches a
worker nor has any MCP, hardware, or server-revision capability.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

from .kit import (
    AcceptanceBroker,
    AdmissionError,
    _C3_WORKER_ROLES,
    _PINNED_SERVER_COMMIT,
    _PINNED_SERVER_ROOT,
    _safe_child,
    _write_new,
    canonical_sha256,
    raw_result_sha256,
    reject_linked_path,
)


_DIAGNOSTIC_KEYS = {"mode", "limitation_id", "attempt_id", "lane_id", "session_id", "raw_result", "source_path", "worker_role"}
_SUBSTITUTE_KEYS = {"mode", "limitation_id", "attempt_id", "lane_id", "session_id", "kind", "stable_id", "worker_role"}
_SUBSTITUTE_KINDS = {"PARTIAL_MCP", "PINNED_COMPONENT_INTEGRATION", "CANDIDATE_BOUNDARY_UNIT"}


class LimitationEvidenceAdapter:
    """Create-once evidence producer for one already-dispatched C3 worker."""

    def __init__(self, broker: AcceptanceBroker) -> None:
        if not isinstance(broker, AcceptanceBroker):
            raise AdmissionError("limitation adapter requires an AcceptanceBroker")
        self.broker = broker

    @staticmethod
    def _ref(path: Path, digest: str) -> dict[str, str]:
        return {"path": str(path.resolve()), "sha256": digest}

    @staticmethod
    def _identity(value: Any, label: str) -> str:
        if not isinstance(value, str) or not value or any(char in value for char in "/\\") or value in {".", ".."}:
            raise AdmissionError(label + " is invalid")
        return value

    def _root(self, value: dict[str, Any]) -> Path:
        return _safe_child(self.broker.root, "hil", value["lane_id"], "server-limitations", value["attempt_id"], value["limitation_id"], "c3-harness")

    def _diagnostic_root(self, path: Path, assignment: dict[str, Any]) -> Path:
        """Validate the diagnostic subtree without inventing a schema field."""
        if path.name != "DIAGNOSTIC_ASSIGNMENT.json" or path.parent.name != "c3-harness":
            raise AdmissionError("diagnostic preparation is outside its exact attempt subtree")
        limitation_id = self._identity(path.parent.parent.name, "diagnostic limitation")
        expected = _safe_child(
            self.broker.root,
            "hil",
            assignment["lane_id"],
            "server-limitations",
            assignment["attempt_id"],
            limitation_id,
            "c3-harness",
        )
        if path.parent != expected:
            raise AdmissionError("diagnostic preparation is outside its exact attempt subtree")
        return expected

    def _confined_path(self, value: Any, label: str, *, exists: bool) -> Path:
        if not isinstance(value, (str, Path)):
            raise AdmissionError(label + " path is invalid")
        path = Path(value)
        reject_linked_path(path)
        resolved = path.resolve()
        if self.broker.root.resolve() not in resolved.parents or (exists and (path.is_symlink() or not path.is_file())):
            raise AdmissionError(label + " escapes the broker root or is unsafe")
        return resolved

    def _reference(self, value: Any, label: str) -> dict[str, str]:
        self.broker._verify_limitation_reference(value, label)
        path = self._confined_path(value["path"], label, exists=True)
        return {"path": str(path), "sha256": value["sha256"]}

    def prepare(self, metadata: dict[str, Any], worker_invocation_id: str, expected_status_path: str | Path, expected_result_path: str | Path) -> dict[str, str]:
        """Write the sole pre-run assignment and return its required credit token."""
        if not isinstance(metadata, dict) or metadata.get("mode") not in {"diagnostic", "substitute"}:
            raise AdmissionError("limitation metadata mode is invalid")
        keys = _DIAGNOSTIC_KEYS if metadata["mode"] == "diagnostic" else _SUBSTITUTE_KEYS
        if set(metadata) != keys:
            raise AdmissionError("limitation metadata is not closed")
        value = dict(metadata)
        for key in ("limitation_id", "attempt_id", "lane_id", "session_id"):
            value[key] = self._identity(value[key], key)
        worker_id = self._identity(worker_invocation_id, "worker invocation")
        if value["worker_role"] not in _C3_WORKER_ROLES:
            raise AdmissionError("limitation worker role is invalid")
        status = self._confined_path(expected_status_path, "expected status", exists=False)
        result = self._confined_path(expected_result_path, "expected result", exists=False)
        root = self._root(value)
        if value["mode"] == "diagnostic":
            raw_ref = self._reference(value["raw_result"], "retained raw failure")
            raw_path = Path(raw_ref["path"])
            if raw_path.parent.parent != _safe_child(self.broker.root, "calls") or raw_path.name != "06-raw-result.json":
                raise AdmissionError("retained raw failure is not the broker raw-result artifact")
            try:
                raw = json.loads(raw_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise AdmissionError("retained raw failure is unreadable") from exc
            payload = raw.get("raw_result") if isinstance(raw, dict) else None
            bound = raw.get("bound_operation") if isinstance(raw, dict) else None
            if not isinstance(raw, dict) or raw.get("schema") != "firmware-call-evidence/v1" or raw.get("stage") != "raw-result" or raw.get("outcome") != "FAIL" or not isinstance(payload, dict) or "transport_failure" in payload or not isinstance(payload.get("result"), dict) or not isinstance(bound, dict) or raw.get("attempt_id") != value["attempt_id"] or raw.get("lane_id") != value["lane_id"] or not isinstance(raw.get("call_id"), str) or not raw["call_id"]:
                raise AdmissionError("retained raw failure is not a dispatched pinned-server FAIL")
            source_path = value["source_path"]
            if not isinstance(source_path, str) or not source_path or Path(source_path).is_absolute() or ".." in Path(source_path).parts:
                raise AdmissionError("pinned source path is unsafe")
            shown = subprocess.run(["git", "show", _PINNED_SERVER_COMMIT + ":" + source_path], cwd=_PINNED_SERVER_ROOT, capture_output=True)
            if shown.returncode:
                raise AdmissionError("pinned source bytes are unavailable")
            for key in ("policy_sha256", "schema_sha256"):
                if not isinstance(bound.get(key), str) or not bound[key]:
                    raise AdmissionError("retained raw failure lacks locked environment")
            assignment = {
                "schema": "firmware-pinned-component-diagnostic-assignment/v1", "attempt_id": value["attempt_id"], "lane_id": value["lane_id"], "session_id": value["session_id"], "call_id": raw["call_id"], "server_commit": _PINNED_SERVER_COMMIT,
                "source_path": source_path, "source_sha256": hashlib.sha256(shown.stdout).hexdigest(), "input_signature": canonical_sha256(bound), "failure_signature": raw_result_sha256(payload), "predicate": "PINNED_COMPONENT_REPRODUCTION", "worker_role": value["worker_role"], "worker_invocation_id": worker_id, "expected_status_path": str(status), "expected_result_path": str(result),
            }
            path = _safe_child(root, "DIAGNOSTIC_ASSIGNMENT.json")
        else:
            if value["kind"] not in _SUBSTITUTE_KINDS or not isinstance(value["stable_id"], str) or not value["stable_id"]:
                raise AdmissionError("limitation substitute identity is invalid")
            assignment = {
                "schema": "firmware-limitation-substitute-assignment/v1", "limitation_id": value["limitation_id"], "attempt_id": value["attempt_id"], "lane_id": value["lane_id"], "session_id": value["session_id"], "kind": value["kind"], "stable_id": value["stable_id"], "assignment_id": worker_id, "worker_role": value["worker_role"], "worker_invocation_id": worker_id, "expected_status_path": str(status), "expected_result_path": str(result),
            }
            path = _safe_child(root, "ASSIGNMENT.json")
        return self._ref(path, _write_new(path, assignment))

    def complete(self, preparation_ref: dict[str, str], candidate_invocation_ref: dict[str, str], controller_status_ref: dict[str, str], candidate_result_ref: dict[str, str]) -> dict[str, Any]:
        """Validate the actual terminal worker result, then create its evidence graph."""
        preparation = self._reference(preparation_ref, "limitation preparation")
        path = Path(preparation["path"])
        try:
            assignment = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AdmissionError("limitation preparation is unreadable") from exc
        if not isinstance(assignment, dict):
            raise AdmissionError("limitation preparation is malformed")
        diagnostic = assignment.get("schema") == "firmware-pinned-component-diagnostic-assignment/v1"
        assignment_keys = (
            {"schema", "attempt_id", "lane_id", "session_id", "call_id", "server_commit", "source_path", "source_sha256", "input_signature", "failure_signature", "predicate", "worker_role", "worker_invocation_id", "expected_status_path", "expected_result_path"}
            if diagnostic
            else {"schema", "limitation_id", "attempt_id", "lane_id", "session_id", "kind", "stable_id", "assignment_id", "worker_role", "worker_invocation_id", "expected_status_path", "expected_result_path"}
        )
        expected_schema = "firmware-pinned-component-diagnostic-assignment/v1" if diagnostic else "firmware-limitation-substitute-assignment/v1"
        if set(assignment) != assignment_keys or assignment.get("schema") != expected_schema:
            raise AdmissionError("limitation preparation is not a closed assignment")
        if diagnostic:
            root = self._diagnostic_root(path, assignment)
        else:
            root = self._root(assignment)
            if path != root / "ASSIGNMENT.json":
                raise AdmissionError("limitation preparation is outside its exact attempt subtree")
        worker_role, worker_id = assignment.get("worker_role"), assignment.get("worker_invocation_id")
        if worker_role not in _C3_WORKER_ROLES or not isinstance(worker_id, str) or not worker_id:
            raise AdmissionError("limitation preparation worker identity is invalid")
        invocation = self._reference(candidate_invocation_ref, "C3 candidate invocation")
        status = self._reference(controller_status_ref, "C3 controller status")
        result = self._reference(candidate_result_ref, "C3 worker result")
        controller_identity = self.broker._verify_c3_completion(invocation, status, result, worker_role, worker_id, assignment["expected_status_path"], assignment["expected_result_path"], preparation["sha256"])
        if diagnostic:
            # The assignment binds the retained call identity; the immutable broker
            # path makes its raw artifact the only permissible attribution input.
            call_path = _safe_child(self.broker.root, "calls", assignment["call_id"], "06-raw-result.json")
            self._confined_path(call_path, "retained raw failure", exists=True)
            raw_digest = hashlib.sha256(call_path.read_bytes()).hexdigest()
            raw_ref = self._ref(call_path, raw_digest)
            try:
                raw = json.loads(call_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise AdmissionError("retained raw failure is unreadable") from exc
            payload = raw.get("raw_result") if isinstance(raw, dict) else None
            bound = raw.get("bound_operation") if isinstance(raw, dict) else None
            if not isinstance(raw, dict) or raw.get("schema") != "firmware-call-evidence/v1" or raw.get("stage") != "raw-result" or raw.get("outcome") != "FAIL" or raw.get("call_id") != assignment["call_id"] or raw.get("attempt_id") != assignment["attempt_id"] or raw.get("lane_id") != assignment["lane_id"] or not isinstance(payload, dict) or "transport_failure" in payload or not isinstance(payload.get("result"), dict) or not isinstance(bound, dict) or bound.get("server_commit") != _PINNED_SERVER_COMMIT or canonical_sha256(bound) != assignment["input_signature"] or raw_result_sha256(payload) != assignment["failure_signature"]:
                raise AdmissionError("retained raw failure drifted after preparation")
            diagnostic_value = {
                "schema": "firmware-pinned-component-diagnostic/v1", "attempt_id": assignment["attempt_id"], "lane_id": assignment["lane_id"], "session_id": assignment["session_id"], "call_id": assignment["call_id"], "controller_owner": controller_identity, "server_commit": _PINNED_SERVER_COMMIT, "source_path": assignment["source_path"], "source_sha256": assignment["source_sha256"], "input_signature": assignment["input_signature"], "failure_signature": assignment["failure_signature"], "predicate": assignment["predicate"], "target_independent": True,
                "locked_environment": {"server_commit": _PINNED_SERVER_COMMIT, "policy_sha256": raw["bound_operation"]["policy_sha256"], "schema_sha256": raw["bound_operation"]["schema_sha256"]}, "assignment_path": preparation["path"], "assignment_sha256": preparation["sha256"], "candidate_invocation": invocation, "controller_status": status, "worker_result": result, "worker_invocation_id": worker_id, "outcome": "PASS",
            }
            diagnostic_ref = self._ref(_safe_child(root, "DIAGNOSTIC.json"), _write_new(_safe_child(root, "DIAGNOSTIC.json"), diagnostic_value))
            attribution = {"schema": "firmware-pinned-server-attribution/v1", "attempt_id": assignment["attempt_id"], "lane_id": assignment["lane_id"], "session_id": assignment["session_id"], "call_id": assignment["call_id"], "raw_result": raw_ref, "source_path": assignment["source_path"], "source_sha256": assignment["source_sha256"], "input_signature": assignment["input_signature"], "failure_signature": assignment["failure_signature"], "predicate": assignment["predicate"], "diagnostic_assignment": preparation, "diagnostic_execution": diagnostic_ref}
            attribution_ref = self._ref(_safe_child(root, "ATTRIBUTION.json"), _write_new(_safe_child(root, "ATTRIBUTION.json"), attribution))
            return {"attribution_evidence": attribution_ref}
        launch = {"schema": "firmware-limitation-c3-launch/v1", "attempt_id": assignment["attempt_id"], "lane_id": assignment["lane_id"], "session_id": assignment["session_id"], "limitation_id": assignment["limitation_id"], "assignment_path": preparation["path"], "assignment_sha256": preparation["sha256"], "candidate_invocation": invocation, "controller_identity": controller_identity, "controller_status": status, "worker_result": result, "worker_invocation_id": worker_id}
        launch_ref = self._ref(_safe_child(root, "LAUNCH.json"), _write_new(_safe_child(root, "LAUNCH.json"), launch))
        execution = {"schema": "firmware-limitation-substitute-execution/v1", "attempt_id": assignment["attempt_id"], "lane_id": assignment["lane_id"], "session_id": assignment["session_id"], "limitation_id": assignment["limitation_id"], "assignment_path": preparation["path"], "assignment_sha256": preparation["sha256"], "launch": launch_ref, "candidate_invocation": invocation, "controller_identity": controller_identity, "controller_status": status, "worker_result": result, "worker_invocation_id": worker_id, "execution_id": worker_id + "-execution", "outcome": "PASS"}
        execution_ref = self._ref(_safe_child(root, "EXECUTION.json"), _write_new(_safe_child(root, "EXECUTION.json"), execution))
        worker = {"schema": "firmware-limitation-c3-worker-result/v1", "attempt_id": assignment["attempt_id"], "lane_id": assignment["lane_id"], "session_id": assignment["session_id"], "limitation_id": assignment["limitation_id"], "assignment_path": preparation["path"], "assignment_sha256": preparation["sha256"], "launch": launch_ref, "execution": execution_ref, "candidate_invocation": invocation, "controller_identity": controller_identity, "controller_status": status, "candidate_result": result, "worker_invocation_id": worker_id, "outcome": "PASS"}
        worker_ref = self._ref(_safe_child(root, "WORKER_RESULT.json"), _write_new(_safe_child(root, "WORKER_RESULT.json"), worker))
        outcome = {"schema": "firmware-limitation-substitute-result/v1", "limitation_id": assignment["limitation_id"], "attempt_id": assignment["attempt_id"], "lane_id": assignment["lane_id"], "session_id": assignment["session_id"], "kind": assignment["kind"], "stable_id": assignment["stable_id"], "assignment_id": assignment["assignment_id"], "result_id": worker_id + "-result", "assignment_path": preparation["path"], "assignment_sha256": preparation["sha256"], "candidate_invocation": invocation, "controller_identity": controller_identity, "controller_status": status, "candidate_result": result, "execution": execution_ref, "worker_result": worker_ref, "outcome": "PASS"}
        outcome_ref = self._ref(_safe_child(root, "RESULT.json"), _write_new(_safe_child(root, "RESULT.json"), outcome))
        return {"assignment": preparation, "result": outcome_ref}
