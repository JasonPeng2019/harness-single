"""Candidate-owned, long-lived C3 control-plane boundary.

This module deliberately contains no hardware transport or scheduling policy.  It
only admits closed signed records and owns the small amount of process/session
lifecycle state needed to use the existing broker, retained controller, and coding
lane controller safely.
"""
from __future__ import annotations

import argparse
import datetime as _datetime
import hashlib
import json
import math
import os
import secrets
import shutil
import stat
import subprocess
import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any

from harness_common.process_identity import exact_process_identity
from .controller import Ed25519Verifier, FirmwareAcceptanceController, load_root_topology
from .c3_limitation import LimitationEvidenceAdapter
from .kit import (AcceptanceBroker, AdmissionError, _safe_child, _write_new,
                  canonical_decision_payload, reject_linked_path, validate_seed_manifest,
                  validate_delegated_authorization, validate_manifest)
from orchestrator_harness.lane_controller import load_invocation
from orchestrator_harness.models import iso_utc
from orchestrator_harness.processes import process_snapshot

_REQUEST_KEYS = {"schema", "request_id", "attempt_id", "c1_reference", "delegated_reference", "orchestrator_identity", "topology_key_release", "kind", "issued_utc", "issued_monotonic", "expires_monotonic", "payload", "public_key", "signature"}
_ROLES = {
    "F.C3.A1": ("gpt-5.6-terra", "medium", "priority", "test_writer"),
    "F.C3.C1": ("gpt-5.6-terra", "medium", "priority", None),
    "F.C3.P1": ("gpt-5.6-luna", "high", "priority", "test_executor"),
    "F.C3.R1": ("gpt-5.6-terra", "medium", "priority", "reviewer"),
}
_SAFE_ID = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")
_INITIAL_CONTROLLER_STATES = {"WAITING_RESOURCE", "RUNNING_CODEX"}
_INITIAL_STATUS_TIMEOUT_SECONDS = 90.0
_LANE_CONTROLLER_MODULE = "orchestrator_harness.lane_controller"


class RecoveryRequired(AdmissionError):
    pass


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _id(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or any(c not in _SAFE_ID for c in value):
        raise AdmissionError(label + " is not a safe candidate identifier")
    return value


def _atomic_append(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
        stream.flush(); os.fsync(stream.fileno())


def _ref(value: Any, label: str, expected: dict[str, str] | None = None) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != {"path", "sha256"} or not all(isinstance(value.get(k), str) and value[k] for k in ("path", "sha256")):
        raise AdmissionError(label + " must be an exact path/hash reference")
    path = Path(value["path"]); reject_linked_path(path)
    if path.is_symlink() or not path.is_file() or _sha(path) != value["sha256"]:
        raise AdmissionError(label + " drifted or is unsafe")
    result = {"path": str(path.resolve()), "sha256": value["sha256"]}
    if expected is not None and result != expected:
        raise AdmissionError(label + " differs from startup binding")
    return result


def _seed_snapshot(seed: Path) -> dict[str, str]:
    reject_linked_path(seed)
    if seed.is_symlink() or not seed.is_dir(): raise AdmissionError("seed input is not a safe directory")
    validate_seed_manifest(seed)
    names = ("TARGET_SEED_MANIFEST.json", "TARGET_CHARTER.md", "PINNED_INPUTS.json", "TEST_CONTRACT.json", "EVIDENCE_SCHEMA.json")
    if {p.name for p in seed.iterdir() if p.is_file()} != set(names): raise AdmissionError("seed does not contain exactly five files")
    result = {}
    for name in names:
        path = seed / name
        if path.is_symlink(): raise AdmissionError("seed file is linked")
        result[name] = _sha(path)
    return result


def _protected_seed_snapshot(root: Path, expected: dict[str, str]) -> None:
    """Validate only protected seed files in a target/worktree, never its full tree."""
    reject_linked_path(root)
    if root.is_symlink() or not root.is_dir(): raise AdmissionError("target worktree is unsafe")
    for name, identity in expected.items():
        path = root / name
        if path.is_symlink() or not path.is_file() or _sha(path) != identity or path.stat().st_mode & stat.S_IWRITE:
            raise AdmissionError("protected seed file drifted")


def _seed_digest(snapshot: dict[str, str]) -> str:
    return hashlib.sha256(json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _snapshot_process(value: Any) -> dict[str, Any] | None:
    """Serialize only the closed process facts needed to authenticate a launch."""
    created = iso_utc(getattr(value, "created_utc", None))
    if (not isinstance(getattr(value, "pid", None), int) or value.pid <= 0
            or not isinstance(getattr(value, "ppid", None), int) or value.ppid < 0
            or not isinstance(getattr(value, "name", None), str) or not value.name
            or not isinstance(getattr(value, "command_line", None), str) or not value.command_line
            or created is None):
        return None
    return {"pid":value.pid,"ppid":value.ppid,"name":value.name,
            "command_line":value.command_line,"created_utc":created}


def _controller_command_matches(command_line: str, invocation_path: Path) -> bool:
    """Require the module launch and this exact invocation path, not merely Python."""
    normalized = command_line.casefold()
    invocation = str(invocation_path.resolve()).casefold()
    return ("-m" in normalized and _LANE_CONTROLLER_MODULE.casefold() in normalized
            and invocation in normalized)


def _initial_controller_relationship(snapshot: Any, launcher_identity: dict[str, Any],
                                     launcher_pid: int, status: dict[str, Any],
                                     invocation_path: Path) -> dict[str, Any] | None:
    """Authenticate one controller from one complete, live process snapshot."""
    if not getattr(snapshot, "complete", False): return None
    launcher = _snapshot_process(snapshot.by_pid.get(launcher_pid))
    controller_pid, created = status.get("controller_pid"), status.get("controller_created_utc")
    if launcher is None or not isinstance(controller_pid, int) or controller_pid <= 0 or not isinstance(created, str) or not created:
        return None
    controller = _snapshot_process(snapshot.by_pid.get(controller_pid))
    if controller is None or controller["created_utc"] != created or not _controller_command_matches(controller["command_line"], invocation_path):
        return None
    controller_identity = exact_process_identity(controller_pid)
    if controller_identity is None:
        return None
    if controller_pid == launcher_pid:
        shape = "same-process"
    elif controller["ppid"] == launcher_pid:
        shape = "direct-venv-redirector"
    else:
        return None
    if exact_process_identity(launcher_pid) != launcher_identity:
        return None
    return {"schema":"firmware-c3-controller-relationship/v1","shape":shape,
            "launcher_identity":launcher_identity,"controller_identity":controller_identity,
            "controller_status_identity":{"pid":controller_pid,"created_utc":created},
            "launcher_snapshot":launcher,"controller_snapshot":controller,
            "invocation":{"path":str(invocation_path.resolve()),"sha256":_sha(invocation_path)},
            "initial_status":{"state":status["state"]}}


class C3Harness:
    def __init__(self, root: Path, seed: Path, policy: Path, templates: Path, topology_root: Path,
                 *, c1: dict[str, str] | None = None, delegated: dict[str, str] | None = None,
                 manifest: Path | None = None) -> None:
        reject_linked_path(root); reject_linked_path(topology_root)
        self.root = root.resolve(); self.seed = seed.resolve(); self.policy = policy.resolve(); self.templates = templates.resolve()
        self.seed_identity = _seed_snapshot(self.seed)
        for source, label in ((self.policy, "policy"), (self.templates, "templates")):
            reject_linked_path(source)
            if source.is_symlink() or not source.is_file(): raise AdmissionError(label + " input is unsafe or absent")
        if manifest is not None:
            reject_linked_path(manifest)
            if manifest.is_symlink() or not manifest.is_file(): raise AdmissionError("manifest input is unsafe or absent")
        if c1 is None or delegated is None or manifest is None: raise AdmissionError("C3 startup requires exact C1, delegated, and manifest inputs")
        c1 = _ref(c1, "startup C1"); delegated = _ref(delegated, "startup delegated authorization")
        self.topology = load_root_topology(topology_root)
        self.c1 = c1; self.delegated = delegated; self.manifest = manifest.resolve() if manifest else None
        locked_manifest = validate_manifest(self.manifest)
        validate_delegated_authorization(self.delegated, policy_path=self.policy, manifest=locked_manifest)
        try: c1_body = json.loads(Path(self.c1["path"]).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc: raise AdmissionError("C1 lock is unreadable") from exc
        required = {"schema","candidate","authorization","server","candidate_acceptance_inputs","target_seed"}
        if not isinstance(c1_body, dict) or not required <= set(c1_body) or c1_body.get("schema") != "firmware-v2-c1-lock/v1": raise AdmissionError("C1 lock is not the closed C1 shape")
        candidate, auth, server, inputs, target_seed = (c1_body[k] for k in ("candidate","authorization","server","candidate_acceptance_inputs","target_seed"))
        if not isinstance(candidate,dict) or not isinstance(candidate.get("commit"),str) or not candidate.get("clean") or not isinstance(auth,dict) or auth.get("path") != self.delegated["path"] or auth.get("sha256") != self.delegated["sha256"] or not isinstance(server,dict) or server.get("commit") != "f003f84a7df51cd8595a3203c62e225b21da2a22" or not server.get("immutable_fixture") or not isinstance(inputs,dict) or not isinstance(target_seed,dict): raise AdmissionError("C1 startup bindings are incomplete")
        candidate_root = Path(candidate.get("path", "")).resolve()
        module_root = Path(__file__).resolve().parent.parent
        try:
            head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=module_root, capture_output=True, text=True, check=True).stdout.strip()
            branch = subprocess.run(["git", "branch", "--show-current"], cwd=module_root, capture_output=True, text=True, check=True).stdout.strip()
            dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=all"], cwd=module_root, capture_output=True, text=True, check=True).stdout
        except subprocess.SubprocessError as exc: raise AdmissionError("cannot prove candidate checkout identity") from exc
        if candidate_root != module_root or candidate.get("branch") != branch or not branch or candidate.get("commit") != head or dirty:
            raise AdmissionError("C1 candidate does not identify this clean checkout")
        self.candidate_root = candidate_root
        for key, path in (("acceptance_manifest",self.manifest),("lane_templates",self.templates),("mcp_method_policy",self.policy)):
            if not isinstance(inputs.get(key),dict) or inputs[key].get("path") != str(path) or inputs[key].get("sha256") != _sha(path): raise AdmissionError("C1 candidate input binding drifted")
        source_policy = candidate_root / "firmware_acceptance" / "MCP_METHOD_POLICY.json"
        source_binding = inputs.get("mcp_method_policy_source")
        reject_linked_path(source_policy)
        if (not isinstance(source_binding, dict) or source_binding.get("path") != str(source_policy)
                or source_policy.is_symlink() or not source_policy.is_file()
                or source_binding.get("sha256") != _sha(source_policy)):
            raise AdmissionError("C1 candidate source policy binding drifted")
        try:
            operative_policy = json.loads(self.policy.read_text(encoding="utf-8"))
            candidate_source_policy = json.loads(source_policy.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AdmissionError("C1 policy input is unreadable") from exc
        if operative_policy != candidate_source_policy:
            raise AdmissionError("C1 operative and candidate source policies differ")
        if not isinstance(target_seed.get("manifest"),dict) or target_seed["manifest"].get("path") != str(self.seed / "TARGET_SEED_MANIFEST.json") or target_seed["manifest"].get("sha256") != self.seed_identity["TARGET_SEED_MANIFEST.json"]: raise AdmissionError("C1 seed binding drifted")
        self.broker = AcceptanceBroker(self.root, self.seed, self.policy, self.templates, self.manifest, target_root=_safe_child(self.root, "target"))
        self.limitation_adapter = LimitationEvidenceAdapter(self.broker)
        self.verifier = Ed25519Verifier(); self.controllers: dict[str, FirmwareAcceptanceController] = {}; self.session_lanes: dict[str, str] = {}
        self.workers: dict[str, subprocess.Popen[Any]] = {}; self.assignments: dict[str, dict[str, Any]] = {}
        self.limitation_completed: dict[str, dict[str, dict[str, Any]]] = {}
        self.operations: dict[str, Future[Any]] = {}; self.operation_pending: dict[str, Path] = {}; self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="c3-lane")
        self.shutdown = False; self.admission_closed = False; self.last_heartbeat = 0.0
        self.recovery: dict[str, Any] | None = None
        self.recovery_closed = False
        self.request_root = _safe_child(self.root, "manager-signals", "c3-requests")
        self.response_root = _safe_child(self.root, "manager-signals", "c3-responses")
        self.admission_root = _safe_child(self.root, "manager-signals", "c3-admissions")
        self.state_root = _safe_child(self.root, "c3-harness")
        for item in (self.request_root, self.response_root, self.admission_root, self.state_root): item.mkdir(parents=True, exist_ok=True)
        self.status_path = _safe_child(self.state_root, "STATUS.jsonl")
        self.registry_path = _safe_child(self.state_root, "REGISTRY.jsonl")
        self._recover_or_fail_closed()

    @property
    def active_worker(self) -> bool:
        return bool(self.workers)

    def _recover_or_fail_closed(self) -> None:
        # A process restart never adopts an incompletely recorded controller/worker.
        recovery = _safe_child(self.state_root, "RECOVERY_REQUIRED.json")
        reconciled = _safe_child(self.state_root, "RECOVERY_RECONCILED.json")
        if recovery.is_file():
            if not reconciled.is_file(): raise AdmissionError("prior C3 process requires exact controller recovery")
            try:
                reject_linked_path(recovery); reject_linked_path(reconciled)
                if recovery.is_symlink() or reconciled.is_symlink(): raise AdmissionError("prior C3 recovery evidence is linked")
                recovery_value = json.loads(recovery.read_text(encoding="utf-8")); value = json.loads(reconciled.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise AdmissionError("prior C3 recovery reconciliation is unreadable") from exc
            aid = self._validate_recovery_record(recovery_value)
            cleanup_path = _safe_child(self.root,"assignments",aid + ".PRESTART_RECOVERY_RECONCILED.json")
            expected = {"schema":"firmware-c3-recovery-reconciled/v1","recovery":{"path":str(recovery),"sha256":_sha(recovery)}}
            if not isinstance(value,dict) or set(value) != {"schema","recovery","cleanup"} or value.get("schema") != expected["schema"] or value.get("recovery") != expected["recovery"]:
                raise AdmissionError("prior C3 recovery reconciliation is malformed or mismatched")
            cleanup_ref = value["cleanup"]
            if not isinstance(cleanup_ref,dict) or set(cleanup_ref) != {"path","sha256"} or cleanup_ref.get("path") != str(cleanup_path) or not isinstance(cleanup_ref.get("sha256"),str) or len(cleanup_ref["sha256"]) != 64: raise AdmissionError("prior C3 recovery cleanup reference is invalid")
            reject_linked_path(cleanup_path)
            if cleanup_path.is_symlink() or not cleanup_path.is_file() or _sha(cleanup_path) != cleanup_ref["sha256"]: raise AdmissionError("prior C3 recovery cleanup drifted")
            try: cleanup = json.loads(cleanup_path.read_text(encoding="utf-8"))
            except (OSError,json.JSONDecodeError) as exc: raise AdmissionError("prior C3 recovery cleanup is unreadable") from exc
            keys = {"schema","assignment_id","reason","launcher_identity","controller_identity","handles_closed","launcher_reaped","controller_reaped","process_reaped","worktree_removed","branch_removed","channels_removed","outcome"}
            if not isinstance(cleanup,dict) or set(cleanup) != keys or cleanup.get("schema") != "firmware-c3-prestart-rejection/v1" or cleanup.get("assignment_id") != aid or cleanup.get("outcome") != "REAPED" or not all(cleanup.get(key) is True for key in ("handles_closed","launcher_reaped","controller_reaped","process_reaped","worktree_removed","branch_removed","channels_removed")):
                raise AdmissionError("prior C3 recovery cleanup is incomplete")
            self.admission_closed = True; self.recovery_closed = True
        if self.registry_path.exists():
            lines = self.registry_path.read_text(encoding="utf-8").splitlines()
            live = [json.loads(line) for line in lines if line.strip()]
            starts = {x.get("assignment_id") for x in live if x.get("schema") == "firmware-c3-worker-lifecycle/v1" and x.get("state") == "STARTED"}
            terminals = {x.get("assignment_id") for x in live if x.get("schema") == "firmware-c3-worker-lifecycle/v1" and x.get("state") == "TERMINAL"}
            sessions = {x.get("session_id") for x in live if x.get("schema") == "firmware-c3-session-lifecycle/v1" and x.get("state") == "OPEN"}
            closed_sessions = {x.get("session_id") for x in live if x.get("schema") == "firmware-c3-session-lifecycle/v1" and x.get("state") == "TERMINAL"}
            if starts - terminals or sessions - closed_sessions:
                raise AdmissionError("prior C3 process has incomplete worker lifecycle; refusing adoption")
        for pending in _safe_child(self.root,"hil").rglob("*.PENDING.json"):
            terminal = pending.with_name(pending.name.removesuffix(".PENDING.json") + ".TERMINAL.json")
            if not terminal.is_file(): raise AdmissionError("prior C3 process has incomplete session operation; refusing adoption")
            value = json.loads(terminal.read_text(encoding="utf-8"))
            if value.get("pending") != {"path":str(pending),"sha256":_sha(pending)}: raise AdmissionError("session operation terminal does not bind pending evidence")

    def _validate_recovery_record(self, value: Any) -> str:
        keys = {"schema","assignment_id","pid","controller_identity","observed_identity","observed_controller_identity","worktree","branch","channels","cleanup"}
        if not isinstance(value,dict) or set(value) != keys or value.get("schema") != "firmware-c3-recovery-required/v1": raise AdmissionError("recovery record is not closed")
        aid = _id(value.get("assignment_id"), "recovery assignment")
        pid = value.get("pid")
        if isinstance(pid,bool) or not isinstance(pid,int) or pid <= 0: raise AdmissionError("recovery PID is invalid")
        for key in ("controller_identity","observed_identity"):
            identity = value[key]
            if identity is not None and (not isinstance(identity,dict) or set(identity) != {"pid","created_utc"} or identity.get("pid") != pid or not isinstance(identity.get("created_utc"),str) or not identity["created_utc"]): raise AdmissionError("recovery process identity is invalid")
        expected_worktree = _safe_child(self.root,"assignment-worktrees",aid); expected_channels = [_safe_child(self.root,"worker-channel",aid),_safe_child(self.root,"worker-channel-responses",aid)]
        if value.get("worktree") != str(expected_worktree) or value.get("branch") != "c3/target/" + aid or value.get("channels") != [str(path) for path in expected_channels]: raise AdmissionError("recovery ownership paths are invalid")
        cleanup = value["cleanup"]; cleanup_keys = {"schema","assignment_id","reason","launcher_identity","controller_identity","handles_closed","launcher_reaped","controller_reaped","process_reaped","worktree_removed","branch_removed","channels_removed","outcome"}
        if (not isinstance(cleanup,dict) or set(cleanup) != cleanup_keys or cleanup.get("schema") != "firmware-c3-prestart-rejection/v1" or cleanup.get("assignment_id") != aid or cleanup.get("launcher_identity") != value["controller_identity"] or cleanup.get("controller_identity") != value["observed_controller_identity"] or cleanup.get("outcome") != "RECOVERY_REQUIRED" or not isinstance(cleanup.get("reason"),str) or not cleanup["reason"] or any(type(cleanup.get(key)) is not bool for key in ("handles_closed","launcher_reaped","controller_reaped","process_reaped","worktree_removed","branch_removed","channels_removed")) or any(cleanup[key] is not False for key in ("process_reaped","worktree_removed","branch_removed","channels_removed"))): raise AdmissionError("recovery cleanup is invalid")
        return aid

    def _status(self, state: str, **extra: Any) -> None:
        _atomic_append(self.status_path, {"schema": "firmware-c3-harness-status/v1", "state": state, "pid": os.getpid(), "monotonic": time.monotonic(), **extra})

    def write_readiness(self) -> None:
        """Publish the one immutable service identity only after all validation."""
        ready = _safe_child(self.state_root, "C3_HARNESS_READY.json")
        bindings = {"seed": _seed_digest(self.seed_identity), "policy": _sha(self.policy), "templates": _sha(self.templates)}
        if self.manifest is not None: bindings["manifest"] = _sha(self.manifest)
        if self.c1 is not None: bindings["c1"] = self.c1["sha256"]
        if self.delegated is not None: bindings["delegated"] = self.delegated["sha256"]
        identity = exact_process_identity(os.getpid())
        if identity is None: raise AdmissionError("cannot prove C3 service process creation identity")
        _write_new(ready, {"schema":"firmware-c3-harness-readiness/v1", "attempt_id":self.topology["attempt_id"], "service_identity":identity, "created_utc":_datetime.datetime.now(_datetime.timezone.utc).isoformat(), "topology_release":self.topology["release"], "bindings":bindings})

    def _record(self, request_id: str, value: dict[str, Any]) -> dict[str, Any]:
        path = _safe_child(self.response_root, request_id + ".json")
        answer = {"schema": "firmware-c3-harness-response/v1", "request_id": request_id, **value}
        try: _write_new(path, answer)
        except (FileExistsError, AdmissionError):
            if not path.is_file(): raise
            preserved = json.loads(path.read_text(encoding="utf-8")); return {**preserved, "path": str(path), "raw_sha256": _sha(path)}
        return {**answer, "path": str(path), "raw_sha256": _sha(path)}

    def _load(self, path: Path) -> dict[str, Any]:
        reject_linked_path(path); path = path.resolve()
        if path.parent != self.request_root or path.suffix != ".json" or path.is_symlink() or not path.is_file(): raise AdmissionError("request is not a confined regular artifact")
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict) or set(value) != _REQUEST_KEYS or value.get("schema") != "firmware-c3-harness-request/v1": raise AdmissionError("request is not closed")
        request_id = _id(value.get("request_id"), "request identity")
        if path.name != request_id + ".json": raise AdmissionError("request filename differs from identity")
        if value.get("attempt_id") != self.topology["attempt_id"] or value.get("topology_key_release") != self.topology["release"] or value.get("orchestrator_identity") != self.topology["identity_binding"]: raise AdmissionError("request topology binding differs")
        if self.c1 is not None: _ref(value.get("c1_reference"), "C1 reference", self.c1)
        else: _ref(value.get("c1_reference"), "C1 reference")
        if self.delegated is not None: _ref(value.get("delegated_reference"), "delegated reference", self.delegated)
        else: _ref(value.get("delegated_reference"), "delegated reference")
        if value.get("public_key") != self.topology["public_key"] or not isinstance(value.get("kind"), str) or not isinstance(value.get("payload"), dict): raise AdmissionError("request signer or body is invalid")
        try: issued = _datetime.datetime.fromisoformat(str(value["issued_utc"]).replace("Z", "+00:00"))
        except ValueError as exc: raise AdmissionError("request UTC is invalid") from exc
        if issued.tzinfo is None: raise AdmissionError("request UTC lacks timezone")
        released = self.topology.get("released_utc")
        if released is not None and issued < released: raise AdmissionError("request predates topology release")
        start, end = value.get("issued_monotonic"), value.get("expires_monotonic")
        now = time.monotonic()
        if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) for x in (start, end)) or end <= start or not start <= now < end: raise AdmissionError("request monotonic window is invalid")
        if not self.verifier.verify(canonical_decision_payload(value), value["signature"], value["public_key"]): raise AdmissionError("request signature is invalid")
        return value

    def handle(self, path: Path) -> dict[str, Any]:
        request_id = path.stem
        try:
            request = self._load(path); request_id = request["request_id"]
            response = _safe_child(self.response_root, request_id + ".json")
            if response.is_file():
                preserved = json.loads(response.read_text(encoding="utf-8"))
                return {**preserved, "path": str(response), "raw_sha256": _sha(response)}
            claim = _safe_child(self.admission_root, request_id + ".json")
            # The exclusive immutable claim precedes every dispatch side effect.
            raw_sha = _sha(path)
            try: _write_new(claim, {"schema":"firmware-c3-request-admission/v1", "request_id":request_id, "request_sha256":raw_sha, "state":"CLAIMED"})
            except AdmissionError:
                if not claim.is_file(): raise
                prior = json.loads(claim.read_text(encoding="utf-8"))
                response = _safe_child(self.response_root, request_id + ".json")
                if prior.get("request_sha256") == raw_sha and response.is_file(): return {**json.loads(response.read_text(encoding="utf-8")),"path":str(response),"raw_sha256":_sha(response)}
                raise AdmissionError("request replay or request-id collision")
            if self.admission_closed and request["kind"] != "shutdown": raise AdmissionError("harness admission is closed")
            result = self._dispatch(request["kind"], request["payload"])
            self._status("HEARTBEAT", request_id=request_id, outcome="ACCEPTED")
            return self._record(request_id, {"outcome":"ACCEPTED", "kind":request["kind"], "result":result})
        except RecoveryRequired as exc:
            self._status("RECOVERY_REQUIRED", request_id=request_id, outcome="RECOVERY_REQUIRED")
            recovery = {key:value for key,value in (self.recovery or {}).items() if key != "process"}
            return self._record(request_id, {"outcome":"RECOVERY_REQUIRED", "reason":str(exc), "recovery":recovery})
        except (AdmissionError, OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
            self._status("HEARTBEAT", request_id=request_id, outcome="REJECTED")
            return self._record(request_id, {"outcome":"REJECTED", "reason":str(exc)})

    def _dispatch(self, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        if kind == "materialize":
            if set(payload) != {"target_id"}: raise AdmissionError("materialize payload is closed")
            target_id = _id(payload["target_id"], "target")
            if target_id != "target": raise AdmissionError("C3 supports exactly the singular target id")
            target = _safe_child(self.root, target_id); self.broker.materialize_seed(target)
            return {"target":str(target), "accepted_commit":self.broker.validate_target(target)}
        if kind == "assignment": return self._assignment(payload)
        if kind == "assignment-accept": return self._accept_assignment(payload)
        if kind == "session-open": return self._open(payload)
        if kind in {"session-proposal", "session-execute", "session-close", "session-abort"}: return self._session(payload, kind.removeprefix("session-"))
        if kind == "limitation-complete": return self._limitation_complete(payload)
        if kind == "server-limitation": return self._server_limitation(payload)
        if kind == "shutdown": return self._shutdown(payload)
        raise AdmissionError("unknown request kind")

    def _reject_unregistered_assignment(self, aid: str, proc: subprocess.Popen[Any] | None, identity: dict[str, Any] | None, controller_identity: dict[str, Any] | None, worktree: Path, branch: str, target: Path, channels: tuple[Path, ...], handles: tuple[Any | None, ...], reason: str) -> dict[str, Any]:
        """Close unpublished ownership transactionally; never signal an identity-unknown PID."""
        cleanup: dict[str, Any] = {"schema":"firmware-c3-prestart-rejection/v1", "assignment_id":aid, "reason":reason, "launcher_identity":identity, "controller_identity":controller_identity, "handles_closed":True, "launcher_reaped":proc is None, "controller_reaped":controller_identity is None, "process_reaped":proc is None and controller_identity is None, "worktree_removed":False, "branch_removed":False, "channels_removed":False}
        for handle in handles:
            if handle is not None:
                try: handle.close()
                except OSError: cleanup["handles_closed"] = False
        if proc is not None:
            exited = proc.poll()
            current = exact_process_identity(proc.pid) if exited is None else None
            if exited is None and identity is not None and current == identity:
                proc.terminate()
                try: proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    if exact_process_identity(proc.pid) == identity:
                        proc.kill(); proc.wait(timeout=5)
            elif exited is None:
                cleanup["process_identity_unresolved"] = True
            try: proc.wait(timeout=0)
            except subprocess.TimeoutExpired: pass
            cleanup["launcher_reaped"] = proc.poll() is not None and identity is not None and exact_process_identity(proc.pid) != identity
        if controller_identity is not None:
            cleanup["controller_reaped"] = exact_process_identity(controller_identity["pid"]) != controller_identity
        cleanup["process_reaped"] = cleanup["launcher_reaped"] and cleanup["controller_reaped"]
        if cleanup["process_reaped"] and cleanup["handles_closed"]:
            removed = subprocess.run(["git", "worktree", "remove", "--force", str(worktree)], cwd=target, capture_output=True, text=True) if worktree.exists() else None
            cleanup["worktree_removed"] = not worktree.exists() or (removed is not None and removed.returncode == 0 and not worktree.exists())
            exists = subprocess.run(["git", "show-ref", "--verify", "--quiet", "refs/heads/" + branch], cwd=target, capture_output=True)
            deleted = subprocess.run(["git", "branch", "-D", branch], cwd=target, capture_output=True, text=True) if exists.returncode == 0 else None
            cleanup["branch_removed"] = exists.returncode != 0 or (deleted is not None and deleted.returncode == 0)
            if cleanup["worktree_removed"] and cleanup["branch_removed"]:
                try:
                    for channel in channels:
                        if channel.exists(): shutil.rmtree(channel)
                    cleanup["channels_removed"] = all(not channel.exists() for channel in channels)
                except OSError: pass
        cleanup["outcome"] = "REAPED" if all(cleanup[key] for key in ("handles_closed", "launcher_reaped", "controller_reaped", "process_reaped", "worktree_removed", "branch_removed", "channels_removed")) else "RECOVERY_REQUIRED"
        evidence = _safe_child(self.root,"assignments",aid + ".PRESTART_REJECTED.json")
        if evidence.exists(): evidence = _safe_child(self.root,"assignments",aid + ".PRESTART_RECOVERY_RECONCILED.json")
        _write_new(evidence, cleanup)
        return cleanup

    def reap_recovery(self) -> None:
        if self.recovery is None or self.recovery["process"].poll() is None: return
        cleanup = self._reject_unregistered_assignment(self.recovery["assignment_id"], self.recovery["process"], self.recovery["controller_identity"], self.recovery.get("observed_controller_identity"), Path(self.recovery["worktree"]), self.recovery["branch"], _safe_child(self.root,"target"), tuple(Path(item) for item in self.recovery["channels"]), (), "exact child absence observed during recovery")
        if cleanup["outcome"] != "REAPED": return
        cleanup_path = _safe_child(self.root,"assignments",self.recovery["assignment_id"] + ".PRESTART_RECOVERY_RECONCILED.json")
        _write_new(_safe_child(self.state_root,"RECOVERY_RECONCILED.json"), {"schema":"firmware-c3-recovery-reconciled/v1","recovery":{"path":self.recovery["path"],"sha256":self.recovery["sha256"]},"cleanup":{"path":str(cleanup_path),"sha256":_sha(cleanup_path)}})
        self.recovery = None

    def _require_recovery(self, aid: str, proc: subprocess.Popen[Any], identity: dict[str, Any] | None, controller_identity: dict[str, Any] | None, worktree: Path, branch: str, channels: tuple[Path, ...], cleanup: dict[str, Any]) -> None:
        """Retain the unproven live child for exact external recovery; never downgrade it."""
        self.admission_closed = True
        record = {"schema":"firmware-c3-recovery-required/v1","assignment_id":aid,"pid":proc.pid,"controller_identity":identity,"observed_identity":exact_process_identity(proc.pid),"observed_controller_identity":controller_identity,"worktree":str(worktree),"branch":branch,"channels":[str(item) for item in channels],"cleanup":cleanup}
        path = _safe_child(self.state_root, "RECOVERY_REQUIRED.json")
        _write_new(path, record)
        self.recovery = {**record,"path":str(path),"sha256":_sha(path),"process":proc}
        raise RecoveryRequired("pre-registration controller identity is unresolved; recovery is required")

    def _assignment(self, p: dict[str, Any]) -> dict[str, Any]:
        allowed = {"assignment_id", "role", "sprint", "task", "prompt", "target_id", "declared_resources", "limitation"}
        if not set(p) <= allowed or not {"assignment_id","role","sprint","task","prompt","target_id","declared_resources"} <= set(p): raise AdmissionError("assignment payload is closed")
        aid, role, target_id = _id(p["assignment_id"], "assignment"), p.get("role"), _id(p["target_id"], "target")
        if role not in _ROLES or not all(isinstance(p[k], str) and p[k] for k in ("sprint","task","prompt")) or not isinstance(p["declared_resources"], list) or any(_id(x,"resource") != x for x in p["declared_resources"]) or len(set(p["declared_resources"])) != len(p["declared_resources"]): raise AdmissionError("assignment fields are invalid")
        limitation = self._limitation_metadata(p.get("limitation"), role) if "limitation" in p else None
        if self.active_worker: raise AdmissionError("exactly one target worker may be active")
        if target_id != "target": raise AdmissionError("C3 supports exactly the singular target id")
        target = _safe_child(self.root, target_id); base = self.broker.validate_target(target)
        worktree = _safe_child(self.root, "assignment-worktrees", aid); branch = "c3/" + target_id + "/" + aid
        if worktree.exists(): raise AdmissionError("candidate assignment worktree already exists")
        branch_before = subprocess.run(["git", "show-ref", "--verify", "--quiet", "refs/heads/" + branch], cwd=target, capture_output=True).returncode == 0
        if branch_before: raise AdmissionError("candidate assignment branch already exists")
        created = subprocess.run(["git", "worktree", "add", "-b", branch, str(worktree), base], cwd=target, capture_output=True, text=True)
        inbox, response_root = _safe_child(self.root, "worker-channel", aid), _safe_child(self.root, "worker-channel-responses", aid)
        if created.returncode:
            branch_after = subprocess.run(["git", "show-ref", "--verify", "--quiet", "refs/heads/" + branch], cwd=target, capture_output=True).returncode == 0
            if worktree.exists() or branch_after:
                self._reject_unregistered_assignment(aid, None, None, None, worktree, branch, target, (inbox, response_root), (), "candidate worktree creation failed")
            raise AdmissionError("candidate worktree creation failed")
        proc: subprocess.Popen[Any] | None = None; identity: dict[str, Any] | None = None
        controller_identity: dict[str, Any] | None = None; relationship: dict[str, Any] | None = None
        out_handle = err_handle = None; started = False; started_publication = False
        try:
            for name in self.seed_identity: (worktree / name).chmod(stat.S_IREAD)
            _protected_seed_snapshot(worktree, self.seed_identity)
            model, effort, tier, finding = _ROLES[role]; workspace = worktree / ".agent-workspace"; workspace.mkdir(exist_ok=True)
            inbox.mkdir(parents=True, exist_ok=False); response_root.mkdir(parents=True, exist_ok=False)
            token = secrets.token_urlsafe(32); prompt = workspace / "C3_PROMPT.md"
            outputs = {name:str(workspace / (aid + suffix)) for name, suffix in {"status":".status.json","jsonl":".jsonl","stderr":".stderr.log","last_message":".last-message.txt"}.items()}
            runtime = _safe_child(self.root,"runtime"); locks = _safe_child(runtime,"coding-resource-locks"); events = _safe_child(runtime,"events"); runtime.mkdir(parents=True,exist_ok=True); locks.mkdir(parents=True,exist_ok=True); events.mkdir(parents=True,exist_ok=True)
            common = subprocess.run(["git","rev-parse","--git-common-dir"],cwd=worktree,capture_output=True,text=True,check=True).stdout.strip(); common_dir = (worktree / common).resolve() if not Path(common).is_absolute() else Path(common).resolve()
            invocation: dict[str, Any] = {"schema":"orchestrator-coding-invocation/v1", "action":"start", "runtime_root":str(runtime), "resource_lock_root":str(locks), "run_root":str(worktree), "repository":{"common_dir":str(common_dir),"worktree_root":str(worktree),"branch":branch,"base_commit":base,"merge_inputs":[]}, "prompt_path":str(prompt),"prompt_sha256":"PENDING","output_paths":outputs,"event_log_path":str(events / "LANE_EVENTS.jsonl"),"lane_id":role,"worker_invocation_id":aid,"task":p["task"],"phase":"c3","exclusive_resources":p["declared_resources"],"codex":{"command":["codex"],"model":model,"reasoning_effort":effort,"service_tier":tier,"sandbox":"danger-full-access","approval_policy":"never","config_overrides":[]},"child_environment_isolation":True}
            if finding: invocation["finding_gate"] = {"role":finding,"path":str(workspace / "FINDINGS.json")}
            invocation_path = _safe_child(self.root, "assignments", aid + ".invocation.json"); preparation = self.limitation_adapter.prepare(limitation, aid, outputs["status"], str(worktree / ".agent-workspace" / "RESULT.json")) if limitation is not None else None
            credit = "" if preparation is None else "\nFinal RESULT.json checks must contain {name: firmware-limitation-credit, command: " + preparation["sha256"] + ", outcome: PASS}.\n"
            prompt.write_text("C3-HARNESS assignment token: " + token + "\nOnly create closed session-proposal requests in fixed inbox " + str(inbox) + "; responses appear only in " + str(response_root) + "." + credit + "\n" + p["prompt"], encoding="utf-8")
            invocation["prompt_sha256"] = _sha(prompt); _write_new(invocation_path, invocation)
            out_handle = (workspace / (aid + ".controller.stdout.log")).open("xb")
            err_handle = (workspace / (aid + ".controller.stderr.log")).open("xb")
            proc = subprocess.Popen([sys.executable, "-m", "orchestrator_harness.lane_controller", str(invocation_path)], cwd=self.candidate_root, stdout=out_handle, stderr=err_handle); identity = exact_process_identity(proc.pid)
            snapshot = process_snapshot(); observed = snapshot.by_pid.get(proc.pid); after_identity = exact_process_identity(proc.pid)
            if identity is None or after_identity != identity or proc.poll() is not None or not snapshot.complete or _snapshot_process(observed) is None: raise AdmissionError("cannot prove controller launcher OS identity")
            status_identity: dict[str, Any] | None = None; status_path = Path(outputs["status"]); launch_status = None; deadline = time.monotonic() + _INITIAL_STATUS_TIMEOUT_SECONDS
            while time.monotonic() < deadline:
                try: value = json.loads(status_path.read_text(encoding="utf-8"))
                except (OSError, UnicodeError, json.JSONDecodeError): value = None
                if (isinstance(value, dict) and value.get("schema") == "orchestrator-lane-controller/v1"
                        and value.get("state") in _INITIAL_CONTROLLER_STATES and proc.poll() is None):
                    candidate_pid = value.get("controller_pid")
                    if isinstance(candidate_pid, int) and candidate_pid > 0:
                        candidate_identity = exact_process_identity(candidate_pid)
                        if candidate_identity is not None:
                            controller_identity = candidate_identity
                    candidate = _initial_controller_relationship(process_snapshot(), identity, proc.pid, value, invocation_path)
                    if candidate is not None:
                        relationship = candidate; controller_identity = candidate["controller_identity"]
                        status_identity = candidate["controller_status_identity"]; launch_status = value; break
                if proc.poll() is not None: break
                time.sleep(.05)
            if launch_status is None: raise AdmissionError("controller did not publish an authentic initial status")
            if proc.poll() is not None or exact_process_identity(proc.pid) != identity or controller_identity is None or exact_process_identity(controller_identity["pid"]) != controller_identity: raise AdmissionError("controller did not remain authentically live for STARTED registration")
            relationship_path = _safe_child(self.root,"assignments",aid + ".CONTROLLER_RELATIONSHIP.json")
            relationship["assignment_id"] = aid; relationship["initial_status"] = {"path":str(status_path),"sha256":_sha(status_path),"state":launch_status["state"],"controller_identity":status_identity}
            _write_new(relationship_path, relationship)
            out_handle.close(); err_handle.close(); out_handle = err_handle = None
            record = {"target_id":target_id,"worktree":worktree,"branch":branch,"base":base,"invocation":invocation,"inbox":inbox,"responses":response_root,"token_hash":hashlib.sha256(token.encode()).hexdigest(),"seed":self.seed_identity,"limitation":limitation,"preparation":preparation,"identity":identity,"controller_identity":controller_identity,"status_identity":status_identity,"relationship":{"path":str(relationship_path),"sha256":_sha(relationship_path)}}
            started_publication = True
            _atomic_append(self.registry_path, {"schema":"firmware-c3-worker-lifecycle/v1","state":"STARTED","assignment_id":aid,"launcher_identity":identity,"controller_identity":controller_identity,"controller_status_identity":status_identity,"controller_relationship":record["relationship"],"invocation":{"path":str(invocation_path),"sha256":_sha(invocation_path)},"worktree":str(worktree),"branch":branch,"worker_channel":str(inbox),"response_root":str(response_root),"token_sha256":record["token_hash"],"limitation":p.get("limitation")})
            self.workers[aid] = proc; self.assignments[aid] = record; started = True
        except Exception as exc:
            cleanup = self._reject_unregistered_assignment(aid, proc, identity, controller_identity, worktree, branch, target, (inbox, response_root), (out_handle, err_handle), str(exc))
            if cleanup["outcome"] != "REAPED" and proc is not None:
                self._require_recovery(aid, proc, identity, controller_identity, worktree, branch, (inbox, response_root), cleanup)
            if started_publication and cleanup["outcome"] == "REAPED":
                cleanup_path = _safe_child(self.root,"assignments",aid + ".PRESTART_REJECTED.json")
                _atomic_append(self.registry_path, {"schema":"firmware-c3-worker-lifecycle/v1","state":"TERMINAL","assignment_id":aid,"launcher_identity":identity,"controller_identity":controller_identity,"rejected":True,"reaped":True,"prestart_cleanup":{"path":str(cleanup_path),"sha256":_sha(cleanup_path)}})
            raise AdmissionError(str(exc) if cleanup["outcome"] == "REAPED" else "pre-registration cleanup requires recovery: " + str(exc)) from exc
        if proc is None or not started: raise AdmissionError("controller launch failed")
        answer = {"assignment_id":aid,"state":"LAUNCHED","invocation":{"path":str(invocation_path),"sha256":_sha(invocation_path)},"worker_channel":{"path":str(inbox)}}
        if preparation is not None: answer["limitation_preparation"] = preparation
        return answer

    def _limitation_metadata(self, value: Any, worker_role: str) -> dict[str, Any]:
        if not isinstance(value, dict) or value.get("mode") not in {"diagnostic", "substitute"}: raise AdmissionError("limitation metadata mode is invalid")
        diagnostic = {"mode","limitation_id","attempt_id","lane_id","session_id","raw_result","source_path"}
        substitute = {"mode","limitation_id","attempt_id","lane_id","session_id","kind","stable_id"}
        if set(value) != (diagnostic if value["mode"] == "diagnostic" else substitute): raise AdmissionError("limitation metadata is not closed")
        if value.get("attempt_id") != self.topology["attempt_id"]: raise AdmissionError("limitation attempt differs")
        for key in ("limitation_id","lane_id","session_id"):_id(value.get(key), "limitation " + key)
        lanes = self.broker.templates.get("lanes") if isinstance(self.broker.templates,dict) else None
        if not isinstance(lanes,list) or sum(isinstance(row,dict) and row.get("lane_id") == value["lane_id"] for row in lanes) != 1: raise AdmissionError("limitation physical lane is invalid")
        result = {**value,"worker_role":worker_role}
        if value["mode"] == "substitute" and value.get("kind") not in {"PARTIAL_MCP","PINNED_COMPONENT_INTEGRATION","CANDIDATE_BOUNDARY_UNIT"}: raise AdmissionError("limitation substitute kind is unsupported")
        return result

    def _limitation_complete(self, payload: dict[str, Any]) -> dict[str, Any]:
        if set(payload) != {"assignment_id"}: raise AdmissionError("limitation completion payload is closed")
        aid = _id(payload["assignment_id"], "assignment")
        record = self.assignments.get(aid)
        if record is None or record.get("preparation") is None or aid in self.workers or record.get("completion",{}).get("outcome") != "PASS": raise AdmissionError("limitation worker has not completed exact PASS")
        metadata, preparation = record["limitation"], record["preparation"]
        lid, mode = metadata["limitation_id"], metadata["mode"]
        if mode in self.limitation_completed.get(lid, {}): raise AdmissionError("limitation completion already exists")
        invocation = _safe_child(self.root,"assignments",aid + ".invocation.json")
        completed = self.limitation_adapter.complete(preparation,{"path":str(invocation),"sha256":_sha(invocation)},record["completion"]["status"],record["completion"]["result"])
        self.limitation_completed.setdefault(lid,{})[mode] = completed
        _atomic_append(self.registry_path,{"schema":"firmware-c3-limitation-completion/v1","assignment_id":aid,"limitation_id":lid,"mode":mode,"preparation":preparation,"completion":completed})
        return {"limitation_id":lid,"mode":mode,"preparation":preparation,"completion":completed}

    def _server_limitation(self, payload: dict[str, Any]) -> dict[str, Any]:
        if set(payload) != {"decision_path","limitation_id"}: raise AdmissionError("limitation payload is closed")
        lid = _id(payload["limitation_id"], "limitation")
        completed = self.limitation_completed.get(lid,{})
        if set(completed) != {"diagnostic","substitute"}: raise AdmissionError("server limitation lacks exact diagnostic and substitute completion")
        return self.broker.record_server_limitation(Path(payload["decision_path"]), lid, self.verifier)

    def _accept_assignment(self, p: dict[str, Any]) -> dict[str, Any]:
        if set(p) != {"assignment_id", "target_id"}: raise AdmissionError("assignment acceptance is closed")
        aid, target_id = _id(p["assignment_id"], "assignment"), _id(p["target_id"], "target")
        if target_id != "target": raise AdmissionError("C3 supports exactly the singular target id")
        if aid in self.workers: raise AdmissionError("assignment is still active")
        record = self.assignments.get(aid)
        if record is None or record.get("target_id") != target_id or record.get("completion",{}).get("outcome") != "PASS": raise AdmissionError("assignment has no exact PASS completion")
        invocation_path = _safe_child(self.root, "assignments", aid + ".invocation.json")
        if not invocation_path.is_file(): raise AdmissionError("assignment invocation is absent")
        invocation = json.loads(invocation_path.read_text(encoding="utf-8")); worktree = Path(invocation["run_root"]); target = _safe_child(self.root,target_id)
        _protected_seed_snapshot(worktree, record["seed"])
        status = Path(invocation["output_paths"]["status"])
        result = worktree / ".agent-workspace" / "RESULT.json"
        if not status.is_file() or not result.is_file(): raise AdmissionError("assignment has no complete controller result")
        state = json.loads(status.read_text(encoding="utf-8"))
        result_value = json.loads(result.read_text(encoding="utf-8"))
        if not isinstance(state,dict) or not isinstance(result_value,dict) or state.get("schema") != "orchestrator-lane-controller/v1" or result_value.get("schema") != "orchestrator-lane-result/v1" or state.get("state") != "CODEX_EXITED" or state.get("exit_code") != 0 or state.get("result_valid") is not True or result_value.get("outcome") != "PASS": raise AdmissionError("assignment completion is not valid PASS")
        current = subprocess.run(["git","rev-parse","HEAD"],cwd=worktree,capture_output=True,text=True,check=True).stdout.strip()
        ff = subprocess.run(["git","merge-base","--is-ancestor",subprocess.run(["git","rev-parse","HEAD"],cwd=target,capture_output=True,text=True,check=True).stdout.strip(),current],cwd=target)
        if ff.returncode: raise AdmissionError("assignment result is not a target fast-forward")
        moved = subprocess.run(["git","merge","--ff-only",current],cwd=target,capture_output=True,text=True)
        if moved.returncode: raise AdmissionError("target accepted branch could not fast-forward")
        return {"assignment_id":aid,"accepted_commit":current}

    def _open(self, p: dict[str, Any]) -> dict[str, Any]:
        if set(p) != {"session_id","request_path"}: raise AdmissionError("session-open payload is closed")
        sid = _id(p["session_id"], "session")
        if sid in self.controllers: raise AdmissionError("session already exists")
        request = Path(p["request_path"]); reject_linked_path(request)
        if self.root not in request.resolve().parents or not request.is_file(): raise AdmissionError("session request escapes attempt")
        controller = FirmwareAcceptanceController(self.broker, topology=self.topology, session_root_for=lambda value: _safe_child(self.root, "hil", _id(value["lane_id"], "lane"), "sessions", _id(value["session_id"], "session")), lane_root_for=lambda lane: _safe_child(self.root, "hil", _id(lane, "lane")))
        result = controller.open_session(request, expected_session_id=sid)
        lane_id = _id(json.loads(request.read_text(encoding="utf-8"))["lane_id"], "lane")
        self.controllers[sid] = controller; self.session_lanes[sid] = lane_id
        _atomic_append(self.registry_path,{"schema":"firmware-c3-session-lifecycle/v1","state":"OPEN","session_id":sid}); return result

    def _session(self, p: dict[str, Any], action: str) -> dict[str, Any]:
        sid = p.get("session_id"); controller = self.controllers.get(sid) if isinstance(sid,str) else None
        if controller is None: raise AdmissionError("unknown retained session")
        if action == "proposal" and set(p) == {"session_id","proposal_path","request"}: return controller.session_publish_proposal(Path(p["proposal_path"]),p["request"])
        if action == "execute" and set(p) == {"session_id","proposal_path","decision_path","authorization_path"}:
            if sid in self.operations: raise AdmissionError("same-lane session operation is active")
            pending = _safe_child(self.root,"hil",self.session_lanes[sid],"sessions",sid,"operations",hashlib.sha256(json.dumps(p,sort_keys=True).encode()).hexdigest() + ".PENDING.json")
            _write_new(pending,{"schema":"firmware-c3-session-operation/v1","state":"PENDING","session_id":sid,"payload":p})
            self.operations[sid] = self.executor.submit(controller.session_execute_artifacts,Path(p["proposal_path"]),Path(p["decision_path"]),Path(p["authorization_path"]),self.verifier)
            self.operation_pending[sid] = pending
            return {"state":"PENDING","path":str(pending),"raw_sha256":_sha(pending)}
        if action in {"close","abort"} and sid in self.operations: raise AdmissionError("session operation is active")
        if action == "close" and set(p) == {"session_id","decision_path"}: result = controller.close_session(Path(p["decision_path"]),self.verifier)
        elif action == "abort" and set(p) == {"session_id","reason"} and isinstance(p["reason"],str) and p["reason"]: result = controller.abort_session(p["reason"])
        else: raise AdmissionError("session operation payload is closed")
        if not result.get("exact_reaped") or not result.get("claim_released"): raise AdmissionError("session terminal cleanup is incomplete")
        del self.controllers[sid]; del self.session_lanes[sid]; _atomic_append(self.registry_path,{"schema":"firmware-c3-session-lifecycle/v1","state":"TERMINAL","session_id":sid,"terminal":result}); return result

    def reap_workers(self) -> None:
        for aid, process in list(self.workers.items()):
            exit_code = process.poll()
            if exit_code is None: continue
            process.wait(); del self.workers[aid]; record = self.assignments[aid]; inv = record["invocation"]
            completion: dict[str, Any] = {"schema":"firmware-c3-worker-completion/v1","assignment_id":aid,"launcher_exit_code":exit_code,"launcher_identity":record["identity"],"controller_identity":record["controller_identity"],"outcome":"FAIL"}
            try:
                _protected_seed_snapshot(record["worktree"], record["seed"])
                status_path, result_path = Path(inv["output_paths"]["status"]), record["worktree"] / ".agent-workspace" / "RESULT.json"
                status, result = json.loads(status_path.read_text(encoding="utf-8")), json.loads(result_path.read_text(encoding="utf-8"))
                loaded = load_invocation(_safe_child(self.root,"assignments",aid + ".invocation.json"))
                valid = isinstance(status,dict) and isinstance(result,dict) and status.get("schema") == "orchestrator-lane-controller/v1" and result.get("schema") == "orchestrator-lane-result/v1" and status.get("state") == "CODEX_EXITED" and exit_code == 0 and status.get("exit_code") == 0 and status.get("controller_pid") == record["status_identity"]["pid"] and status.get("controller_created_utc") == record["status_identity"]["created_utc"] and status.get("held_resource_claims") == [] and status.get("result_valid") is True and result.get("outcome") == "PASS" and result.get("lane_id") == inv["lane_id"] and result.get("worker_invocation_id") == aid and result.get("branch") == record["branch"] and loaded.repository is not None and loaded.repository.branch == record["branch"]
                if inv.get("finding_gate") is not None:
                    valid = valid and isinstance(status.get("result_validation"),dict) and status["result_validation"].get("findings") is not None
                tip = subprocess.run(["git","rev-parse","HEAD"],cwd=record["worktree"],capture_output=True,text=True,check=True).stdout.strip()
                valid = valid and result.get("commit") == tip
                if not valid: raise AdmissionError("controller completion identity is not exact")
                completion.update({"outcome":"PASS","current_tip":tip,"status":{"path":str(status_path),"sha256":_sha(status_path)},"result":{"path":str(result_path),"sha256":_sha(result_path)}})
            except (OSError, ValueError, subprocess.SubprocessError, AdmissionError) as exc: completion["reason"] = str(exc)
            completion_path = _safe_child(self.root,"assignments",aid + ".WORKER_COMPLETION.json"); _write_new(completion_path,completion)
            record["completion"] = {"path":str(completion_path),"sha256":_sha(completion_path),"outcome":completion["outcome"]}
            _atomic_append(self.registry_path,{"schema":"firmware-c3-worker-lifecycle/v1","state":"TERMINAL","assignment_id":aid,"launcher_identity":record["identity"],"controller_identity":record["controller_identity"],"launcher_exit_code":exit_code,"reaped":True,"completion":record["completion"]})

    def reap_operations(self) -> None:
        for sid, future in list(self.operations.items()):
            if not future.done(): continue
            try: value = future.result(); outcome, error = "PASS", None
            except Exception as exc: value, outcome, error = None, "FAIL", str(exc)
            pending = self.operation_pending[sid]
            terminal = pending.with_name(pending.name.removesuffix(".PENDING.json") + ".TERMINAL.json")
            _write_new(terminal,{"schema":"firmware-c3-session-operation/v1","state":"TERMINAL","session_id":sid,"pending":{"path":str(pending),"sha256":_sha(pending)},"outcome":outcome,"result":value,"error":error})
            del self.operations[sid]; del self.operation_pending[sid]

    def service_worker_channels(self) -> None:
        """The only worker-facing ingress: one active P1 inbox, closed and token-bound."""
        for aid, record in list(self.assignments.items()):
            if record["invocation"]["lane_id"] != "F.C3.P1" or aid not in self.workers: continue
            for path in sorted(record["inbox"].glob("*.json")):
                response = _safe_child(record["responses"], path.name)
                if response.exists(): continue
                try:
                    raw = json.loads(path.read_text(encoding="utf-8")); required = {"schema","request_id","assignment_id","token","kind","session_id","sequence","predecessor_state","current_state","next_state","call"}
                    if not isinstance(raw,dict) or set(raw) != required or raw.get("schema") != "firmware-c3-worker-request/v1" or raw.get("kind") != "session-proposal" or raw.get("assignment_id") != aid or path.name != raw.get("request_id","") + ".json" or not isinstance(raw.get("token"),str) or not secrets.compare_digest(hashlib.sha256(raw["token"].encode()).hexdigest(),record["token_hash"]): raise AdmissionError("worker request is not closed or token-bound")
                    sid = _id(raw.get("session_id"),"session")
                    if sid not in self.controllers or not isinstance(raw.get("sequence"),int) or raw["sequence"] < 0 or not all(isinstance(raw.get(k),str) and raw[k] for k in ("predecessor_state","current_state","next_state")) or not isinstance(raw.get("call"),dict): raise AdmissionError("worker request state is invalid")
                    proposal = _safe_child(self.root,"hil",self.session_lanes[sid],"sessions",sid,"worker-proposals",raw["request_id"] + ".json")
                    value = self.controllers[sid].session_publish_proposal(proposal,raw["call"])
                    _write_new(response,{"schema":"firmware-c3-worker-response/v1","request_id":raw["request_id"],"outcome":"ACCEPTED","proposal":{"path":str(proposal),"sha256":_sha(proposal)},"result":value})
                except (AdmissionError,OSError,ValueError,json.JSONDecodeError) as exc:
                    if not response.exists(): _write_new(response,{"schema":"firmware-c3-worker-response/v1","request_id":path.stem,"outcome":"REJECTED","reason":str(exc)})

    def _shutdown(self, p: dict[str, Any]) -> dict[str, Any]:
        if p: raise AdmissionError("shutdown payload must be empty")
        self.admission_closed = True
        if self.recovery is not None:
            return {"state":"BLOCKED","reason":"identity-unresolved controller requires exact recovery","recovery":{key:value for key,value in self.recovery.items() if key != "process"}}
        self.reap_workers()
        self.reap_operations()
        if self.workers or self.operations: return {"state":"BLOCKED","reason":"target worker or session operation is active"}
        self.admission_closed = True; terminals=[]
        for sid, controller in list(self.controllers.items()):
            result = controller.abort_session("signed harness shutdown")
            if not result.get("exact_reaped") or not result.get("claim_released"): raise AdmissionError("shutdown session cleanup is incomplete")
            terminals.append({"session_id":sid,"terminal":result}); del self.controllers[sid]; del self.session_lanes[sid]
        evidence = _safe_child(self.state_root,"C3_HARNESS_SHUTDOWN.json")
        self.executor.shutdown(wait=True, cancel_futures=False)
        _write_new(evidence,{"schema":"firmware-c3-harness-shutdown/v1","pid":os.getpid(),"sessions":terminals,"workers_reaped":True,"admission_closed":True})
        self.shutdown = True; self._status("SHUTDOWN", shutdown_path=str(evidence)); return {"state":"SHUTDOWN","sessions":terminals,"shutdown_path":str(evidence)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="firmware-acceptance-c3-harness")
    for name in ("root","seed","policy","templates","topology-root"): parser.add_argument("--" + name,type=Path,required=True)
    parser.add_argument("--c1-path",type=Path,required=True); parser.add_argument("--c1-sha256",required=True)
    parser.add_argument("--delegated-path",type=Path,required=True); parser.add_argument("--delegated-sha256",required=True)
    parser.add_argument("--manifest",type=Path,required=True)
    sub = parser.add_subparsers(dest="command",required=True); serve=sub.add_parser("serve"); serve.add_argument("--poll-seconds",type=float,default=.25)
    args = parser.parse_args(argv)
    harness = C3Harness(args.root,args.seed,args.policy,args.templates,args.topology_root,c1={"path":str(args.c1_path.resolve()),"sha256":args.c1_sha256},delegated={"path":str(args.delegated_path.resolve()),"sha256":args.delegated_sha256},manifest=args.manifest)
    if harness.recovery_closed:
        harness._status("RECOVERY_RECONCILED_CLOSED")
    else:
        harness.write_readiness(); harness._status("READY")
    while not harness.shutdown:
        harness.reap_recovery(); harness.reap_workers(); harness.reap_operations(); harness.service_worker_channels()
        if time.monotonic() - harness.last_heartbeat >= 30:
            harness._status("RECOVERY_RECONCILED_CLOSED" if harness.recovery_closed else "HEARTBEAT", idle=True); harness.last_heartbeat = time.monotonic()
        for request in sorted(harness.request_root.glob("*.json")):
            if not (_safe_child(harness.response_root,request.name)).exists(): print(json.dumps(harness.handle(request),sort_keys=True),flush=True)
        time.sleep(args.poll_seconds)
    return 0


if __name__ == "__main__": raise SystemExit(main())
