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
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from harness_common.process_identity import exact_process_identity
from .controller import Ed25519Verifier, FirmwareAcceptanceController, load_root_topology
from .kit import (AcceptanceBroker, AdmissionError, _safe_child, _write_new,
                  canonical_decision_payload, reject_linked_path, validate_seed_manifest,
                  worker_environment)

_REQUEST_KEYS = {"schema", "request_id", "attempt_id", "c1_reference", "delegated_reference", "orchestrator_identity", "topology_key_release", "kind", "issued_utc", "issued_monotonic", "expires_monotonic", "payload", "public_key", "signature"}
_ROLES = {
    "F.C3.A1": ("gpt-5.6-terra", "medium", "priority", "test_writer"),
    "F.C3.C1": ("gpt-5.6-terra", "medium", "default", None),
    "F.C3.P1": ("gpt-5.6-luna", "high", "default", "test_executor"),
    "F.C3.R1": ("gpt-5.6-terra", "medium", "priority", "reviewer"),
}
_SAFE_ID = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")


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


class C3Harness:
    def __init__(self, root: Path, seed: Path, policy: Path, templates: Path, topology_root: Path,
                 *, c1: dict[str, str] | None = None, delegated: dict[str, str] | None = None,
                 manifest: Path | None = None) -> None:
        reject_linked_path(root); reject_linked_path(topology_root)
        self.root = root.resolve(); self.seed = seed.resolve(); self.policy = policy.resolve(); self.templates = templates.resolve()
        for source, label in ((self.seed, "seed"), (self.policy, "policy"), (self.templates, "templates")):
            reject_linked_path(source)
            if source.is_symlink() or not source.is_file(): raise AdmissionError(label + " input is unsafe or absent")
        if manifest is not None:
            reject_linked_path(manifest)
            if manifest.is_symlink() or not manifest.is_file(): raise AdmissionError("manifest input is unsafe or absent")
        if c1 is not None: _ref(c1, "startup C1")
        if delegated is not None: _ref(delegated, "startup delegated authorization")
        self.topology = load_root_topology(topology_root)
        self.c1 = c1; self.delegated = delegated; self.manifest = manifest.resolve() if manifest else None
        if self.c1 is not None and self.delegated is not None and self.manifest is not None:
            # C1 remains the authority artifact, but every startup input must be
            # visibly pinned in its raw immutable body before this service makes
            # any attempt-local directory or readiness record.
            c1_body = Path(self.c1["path"]).read_text(encoding="utf-8")
            required_hashes = (_sha(self.seed), _sha(self.policy), _sha(self.templates),
                               _sha(self.manifest), self.delegated["sha256"],
                               self.topology["release"]["sha256"])
            if any(value not in c1_body for value in required_hashes) or self.topology["attempt_id"] not in c1_body:
                raise AdmissionError("C1 does not bind the exact startup inputs")
        self.broker = AcceptanceBroker(self.root, self.seed, self.policy, self.templates, self.manifest)
        self.verifier = Ed25519Verifier(); self.controllers: dict[str, FirmwareAcceptanceController] = {}
        self.workers: dict[str, subprocess.Popen[Any]] = {}; self.shutdown = False; self.admission_closed = False
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
        if self.registry_path.exists():
            lines = self.registry_path.read_text(encoding="utf-8").splitlines()
            live = [json.loads(line) for line in lines if line.strip()]
            starts = {x.get("assignment_id") for x in live if x.get("schema") == "firmware-c3-worker-lifecycle/v1" and x.get("state") == "STARTED"}
            terminals = {x.get("assignment_id") for x in live if x.get("schema") == "firmware-c3-worker-lifecycle/v1" and x.get("state") == "TERMINAL"}
            sessions = {x.get("session_id") for x in live if x.get("schema") == "firmware-c3-session-lifecycle/v1" and x.get("state") == "OPEN"}
            closed_sessions = {x.get("session_id") for x in live if x.get("schema") == "firmware-c3-session-lifecycle/v1" and x.get("state") == "TERMINAL"}
            if starts - terminals or sessions - closed_sessions:
                raise AdmissionError("prior C3 process has incomplete worker lifecycle; refusing adoption")

    def _status(self, state: str, **extra: Any) -> None:
        _atomic_append(self.status_path, {"schema": "firmware-c3-harness-status/v1", "state": state, "pid": os.getpid(), "monotonic": time.monotonic(), **extra})

    def write_readiness(self) -> None:
        """Publish the one immutable service identity only after all validation."""
        ready = _safe_child(self.state_root, "C3_HARNESS_READY.json")
        bindings = {"seed": _sha(self.seed), "policy": _sha(self.policy), "templates": _sha(self.templates)}
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
        except FileExistsError:
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
        if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) for x in (start, end)) or end <= start or time.monotonic() >= end: raise AdmissionError("request monotonic window is invalid")
        if not self.verifier.verify(canonical_decision_payload(value), value["signature"], value["public_key"]): raise AdmissionError("request signature is invalid")
        return value

    def handle(self, path: Path) -> dict[str, Any]:
        request_id = path.stem
        try:
            request = self._load(path); request_id = request["request_id"]
            claim = _safe_child(self.admission_root, request_id + ".json")
            # The exclusive immutable claim precedes every dispatch side effect.
            _write_new(claim, {"schema":"firmware-c3-request-admission/v1", "request_id":request_id, "request_sha256":_sha(path), "state":"CLAIMED"})
            if self.admission_closed and request["kind"] != "shutdown": raise AdmissionError("harness admission is closed")
            result = self._dispatch(request["kind"], request["payload"])
            self._status("HEARTBEAT", request_id=request_id, outcome="ACCEPTED")
            return self._record(request_id, {"outcome":"ACCEPTED", "kind":request["kind"], "result":result})
        except (AdmissionError, OSError, ValueError, json.JSONDecodeError) as exc:
            self._status("HEARTBEAT", request_id=request_id, outcome="REJECTED")
            return self._record(request_id, {"outcome":"REJECTED", "reason":str(exc)})

    def _dispatch(self, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        if kind == "materialize":
            if set(payload) != {"target_id"}: raise AdmissionError("materialize payload is closed")
            target = _safe_child(self.root, "targets", _id(payload["target_id"], "target")); self.broker.materialize_seed(target)
            return {"target":str(target), "accepted_commit":self.broker.validate_target(target)}
        if kind == "assignment": return self._assignment(payload)
        if kind == "assignment-accept": return self._accept_assignment(payload)
        if kind == "session-open": return self._open(payload)
        if kind in {"session-proposal", "session-execute", "session-close", "session-abort"}: return self._session(payload, kind.removeprefix("session-"))
        if kind == "server-limitation":
            if set(payload) != {"decision_path", "limitation_id"}: raise AdmissionError("limitation payload is closed")
            return self.broker.record_server_limitation(Path(payload["decision_path"]), _id(payload["limitation_id"], "limitation"), self.verifier)
        if kind == "shutdown": return self._shutdown(payload)
        raise AdmissionError("unknown request kind")

    def _assignment(self, p: dict[str, Any]) -> dict[str, Any]:
        allowed = {"assignment_id", "role", "sprint", "task", "prompt", "target_id", "declared_resources", "limitation"}
        if not set(p) <= allowed or not {"assignment_id","role","sprint","task","prompt","target_id","declared_resources"} <= set(p): raise AdmissionError("assignment payload is closed")
        aid, role, target_id = _id(p["assignment_id"], "assignment"), p.get("role"), _id(p["target_id"], "target")
        if role not in _ROLES or not all(isinstance(p[k], str) and p[k] for k in ("task","prompt")) or not isinstance(p["declared_resources"], list) or any(not isinstance(x, str) for x in p["declared_resources"]): raise AdmissionError("assignment fields are invalid")
        if "limitation" in p and (not isinstance(p["limitation"], dict) or set(p["limitation"]) != {"limitation_id", "stable_id"} or not all(isinstance(p["limitation"].get(k), str) and p["limitation"][k] for k in ("limitation_id", "stable_id"))): raise AdmissionError("assignment limitation metadata is not closed")
        if self.active_worker: raise AdmissionError("exactly one target worker may be active")
        target = _safe_child(self.root, "targets", target_id); base = self.broker.validate_target(target)
        worktree = _safe_child(self.root, "assignment-worktrees", aid); branch = "c3/" + target_id + "/" + aid
        if worktree.exists(): raise AdmissionError("candidate assignment worktree already exists")
        created = subprocess.run(["git", "worktree", "add", "-b", branch, str(worktree), base], cwd=target, capture_output=True, text=True)
        if created.returncode: raise AdmissionError("candidate worktree creation failed")
        validate_seed_manifest(worktree)  # seed content remains immutable in the assigned copy
        model, effort, tier, finding = _ROLES[role]; workspace = worktree / ".agent-workspace"; workspace.mkdir(exist_ok=True)
        inbox = _safe_child(self.root, "worker-channel", aid); inbox.mkdir(parents=True, exist_ok=False)
        token = hashlib.sha256((aid + str(time.monotonic())).encode()).hexdigest()
        prompt = workspace / "C3_PROMPT.md"; prompt.write_text("C3-HARNESS assignment token: " + token + "\nOnly write closed session-proposal records beneath " + str(inbox) + ".\n\n" + p["prompt"], encoding="utf-8")
        outputs = {name:str(workspace / (aid + suffix)) for name, suffix in {"status":".status.json","jsonl":".jsonl","stderr":".stderr.log","last_message":".last-message.txt"}.items()}
        invocation: dict[str, Any] = {"schema":"orchestrator-coding-invocation/v1", "action":"start", "runtime_root":str(_safe_child(self.root,"runtime")), "run_root":str(worktree), "repository":{"common_dir":str((worktree / ".git").resolve()),"worktree_root":str(worktree),"branch":branch,"base_commit":base,"merge_inputs":[]}, "prompt_path":str(prompt),"prompt_sha256":_sha(prompt),"output_paths":outputs,"event_log_path":str(_safe_child(self.root,"events","LANE_EVENTS.jsonl")),"lane_id":role,"worker_invocation_id":aid,"task":p["task"],"phase":"c3","exclusive_resources":[],"codex":{"command":["codex"],"model":model,"reasoning_effort":effort,"service_tier":tier,"sandbox":"danger-full-access","approval_policy":"never","config_overrides":[]},"child_environment_isolation":True,"worker_environment":worker_environment()}
        if finding: invocation["finding_gate"] = {"role":finding,"path":str(workspace / "FINDINGS.json")}
        invocation_path = _safe_child(self.root, "assignments", aid + ".invocation.json"); _write_new(invocation_path, invocation)
        proc = subprocess.Popen([sys.executable, "-m", "orchestrator_harness.lane_controller", str(invocation_path)], cwd=self.root, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.workers[aid] = proc
        _atomic_append(self.registry_path, {"schema":"firmware-c3-worker-lifecycle/v1","state":"STARTED","assignment_id":aid,"pid":proc.pid,"invocation":{"path":str(invocation_path),"sha256":_sha(invocation_path)},"worktree":str(worktree),"branch":branch,"worker_channel":str(inbox)})
        return {"assignment_id":aid,"state":"LAUNCHED","invocation":{"path":str(invocation_path),"sha256":_sha(invocation_path)},"worker_channel":{"path":str(inbox)}}

    def _accept_assignment(self, p: dict[str, Any]) -> dict[str, Any]:
        if set(p) != {"assignment_id", "target_id"}: raise AdmissionError("assignment acceptance is closed")
        aid, target_id = _id(p["assignment_id"], "assignment"), _id(p["target_id"], "target")
        if aid in self.workers: raise AdmissionError("assignment is still active")
        invocation_path = _safe_child(self.root, "assignments", aid + ".invocation.json")
        if not invocation_path.is_file(): raise AdmissionError("assignment invocation is absent")
        invocation = json.loads(invocation_path.read_text(encoding="utf-8")); worktree = Path(invocation["run_root"]); target = _safe_child(self.root,"targets",target_id)
        validate_seed_manifest(worktree); status = Path(invocation["output_paths"]["status"])
        result = worktree / ".agent-workspace" / "RESULT.json"
        if not status.is_file() or not result.is_file(): raise AdmissionError("assignment has no complete controller result")
        state = json.loads(status.read_text(encoding="utf-8"))
        if state.get("state") != "CODEX_EXITED" or state.get("exit_code") != 0 or state.get("result_valid") is not True: raise AdmissionError("assignment completion is not valid")
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
        controller = FirmwareAcceptanceController(self.broker, topology=self.topology); result = controller.open_session(request); self.controllers[sid] = controller
        _atomic_append(self.registry_path,{"schema":"firmware-c3-session-lifecycle/v1","state":"OPEN","session_id":sid}); return result

    def _session(self, p: dict[str, Any], action: str) -> dict[str, Any]:
        sid = p.get("session_id"); controller = self.controllers.get(sid) if isinstance(sid,str) else None
        if controller is None: raise AdmissionError("unknown retained session")
        if action == "proposal" and set(p) == {"session_id","proposal_path","request"}: return controller.session_publish_proposal(Path(p["proposal_path"]),p["request"])
        if action == "execute" and set(p) == {"session_id","proposal_path","decision_path","authorization_path"}: return controller.session_execute_artifacts(Path(p["proposal_path"]),Path(p["decision_path"]),Path(p["authorization_path"]),self.verifier)
        if action == "close" and set(p) == {"session_id","decision_path"}: result = controller.close_session(Path(p["decision_path"]),self.verifier)
        elif action == "abort" and set(p) == {"session_id","reason"} and isinstance(p["reason"],str) and p["reason"]: result = controller.abort_session(p["reason"])
        else: raise AdmissionError("session operation payload is closed")
        if not result.get("exact_reaped") or not result.get("claim_released"): raise AdmissionError("session terminal cleanup is incomplete")
        del self.controllers[sid]; _atomic_append(self.registry_path,{"schema":"firmware-c3-session-lifecycle/v1","state":"TERMINAL","session_id":sid,"terminal":result}); return result

    def reap_workers(self) -> None:
        for aid, process in list(self.workers.items()):
            exit_code = process.poll()
            if exit_code is None: continue
            process.wait(); del self.workers[aid]
            _atomic_append(self.registry_path,{"schema":"firmware-c3-worker-lifecycle/v1","state":"TERMINAL","assignment_id":aid,"pid":process.pid,"exit_code":exit_code,"reaped":True})

    def _shutdown(self, p: dict[str, Any]) -> dict[str, Any]:
        if p: raise AdmissionError("shutdown payload must be empty")
        self.reap_workers()
        if self.workers: return {"state":"BLOCKED","reason":"target worker is active"}
        self.admission_closed = True; terminals=[]
        for sid, controller in list(self.controllers.items()):
            result = controller.abort_session("signed harness shutdown")
            if not result.get("exact_reaped") or not result.get("claim_released"): raise AdmissionError("shutdown session cleanup is incomplete")
            terminals.append({"session_id":sid,"terminal":result}); del self.controllers[sid]
        evidence = _safe_child(self.state_root,"C3_HARNESS_SHUTDOWN.json")
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
    harness.write_readiness(); harness._status("READY")
    while not harness.shutdown:
        harness.reap_workers()
        for request in sorted(harness.request_root.glob("*.json")):
            if not (_safe_child(harness.response_root,request.name)).exists(): print(json.dumps(harness.handle(request),sort_keys=True),flush=True)
        time.sleep(args.poll_seconds)
    return 0


if __name__ == "__main__": raise SystemExit(main())
