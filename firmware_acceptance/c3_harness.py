"""Small, long-lived C3-HARNESS facade.

It deliberately accepts only signed, attempt-local JSON records.  It is not a
scheduler: each accepted request selects one of the already implemented broker,
controller, or coding-lane operations and leaves immutable evidence behind.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import datetime as _datetime
import os
import time
from pathlib import Path
from typing import Any

from .controller import Ed25519Verifier, FirmwareAcceptanceController, load_root_topology
from .kit import AcceptanceBroker, AdmissionError, _safe_child, _write_new, canonical_decision_payload, reject_linked_path


_REQUEST_KEYS = {"schema", "request_id", "attempt_id", "c1_reference", "delegated_reference", "orchestrator_identity", "topology_key_release", "kind", "issued_utc", "issued_monotonic", "expires_monotonic", "payload", "public_key", "signature"}
_ROLES = {
    "F.C3.A1": ("gpt-5.6-terra", "medium", "priority", "test_writer"),
    "F.C3.C1": ("gpt-5.6-terra", "medium", "default", None),
    "F.C3.P1": ("gpt-5.6-luna", "high", "default", "test_executor"),
    "F.C3.R1": ("gpt-5.6-terra", "medium", "priority", "reviewer"),
}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class C3Harness:
    def __init__(self, root: Path, seed: Path, policy: Path, templates: Path, topology_root: Path) -> None:
        reject_linked_path(root); reject_linked_path(topology_root)
        self.root = root.resolve(); self.topology = load_root_topology(topology_root)
        self.broker = AcceptanceBroker(self.root, seed, policy, templates)
        self.verifier = Ed25519Verifier()
        self.controllers: dict[str, FirmwareAcceptanceController] = {}
        self.active_worker = False
        self.shutdown = False
        self.request_root = _safe_child(self.root, "manager-signals", "c3-requests")
        self.response_root = _safe_child(self.root, "manager-signals", "c3-responses")
        self.admission_root = _safe_child(self.root, "manager-signals", "c3-admissions")
        self.request_root.mkdir(parents=True, exist_ok=True); self.response_root.mkdir(parents=True, exist_ok=True)
        self.admission_root.mkdir(parents=True, exist_ok=True)

    def _record(self, request_id: str, value: dict[str, Any]) -> dict[str, Any]:
        path = _safe_child(self.response_root, request_id + ".json")
        value = {"schema": "firmware-c3-harness-response/v1", "request_id": request_id, **value}
        _write_new(path, value)
        return {**value, "path": str(path), "raw_sha256": _sha(path)}

    def _load(self, path: Path) -> dict[str, Any]:
        reject_linked_path(path)
        path = path.resolve()
        if path.parent != self.request_root or path.suffix != ".json" or path.is_symlink() or not path.is_file():
            raise AdmissionError("request is not an attempt-local regular request artifact")
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict) or set(value) != _REQUEST_KEYS or value["schema"] != "firmware-c3-harness-request/v1":
            raise AdmissionError("request is not closed")
        if not isinstance(value["request_id"], str) or not value["request_id"] or path.name != value["request_id"] + ".json":
            raise AdmissionError("request identity is invalid")
        if value["attempt_id"] != self.topology["attempt_id"] or value["topology_key_release"] != self.topology["release"] or value["orchestrator_identity"] != self.topology["identity_binding"]:
            raise AdmissionError("request attempt or ROOT release binding differs")
        if value["public_key"] != self.topology["public_key"] or not isinstance(value["kind"], str) or not isinstance(value["payload"], dict):
            raise AdmissionError("request signer or body is invalid")
        if not isinstance(value["issued_utc"], str): raise AdmissionError("request issue time is invalid")
        try: issued = _datetime.datetime.fromisoformat(value["issued_utc"].replace("Z", "+00:00"))
        except ValueError as exc: raise AdmissionError("request issue time is invalid") from exc
        released = self.topology.get("released_utc")
        if released is not None and issued < released: raise AdmissionError("request predates key release")
        if not all(isinstance(value[k], (int, float)) and not isinstance(value[k], bool) for k in ("issued_monotonic", "expires_monotonic")) or value["expires_monotonic"] <= value["issued_monotonic"] or time.monotonic() >= value["expires_monotonic"]:
            raise AdmissionError("request is expired")
        if not self.verifier.verify(canonical_decision_payload(value), value["signature"], value["public_key"]):
            raise AdmissionError("request signature is invalid")
        return value

    def handle(self, path: Path) -> dict[str, Any]:
        request_id = path.stem
        try:
            request = self._load(path); request_id = request["request_id"]
            admission = _safe_child(self.admission_root, request_id + ".json")
            # This is the replay guard, intentionally before every side effect.
            _write_new(admission, {"schema":"firmware-c3-request-admission/v1","request_id":request_id,"request_path":str(path.resolve()),"request_sha256":_sha(path),"state":"ADMITTED"})
            if self.shutdown and request["kind"] != "shutdown": raise AdmissionError("harness is shut down")
            result = self._dispatch(request["kind"], request["payload"])
            return self._record(request_id, {"outcome": "ACCEPTED", "kind": request["kind"], "result": result})
        except (AdmissionError, OSError, ValueError, json.JSONDecodeError) as exc:
            # Deliberately ordinary orchestration evidence, never a watcher abort.
            response = _safe_child(self.response_root, request_id + ".json")
            if response.exists(): return {"schema":"firmware-c3-harness-response/v1","request_id":request_id,"outcome":"REJECTED","reason":str(exc),"replay":True,"path":str(response)}
            return self._record(request_id, {"outcome": "REJECTED", "reason": str(exc)})

    def _dispatch(self, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        if kind == "materialize":
            if set(payload) != {"target_id"} or not isinstance(payload["target_id"], str): raise AdmissionError("materialize payload is closed")
            target = _safe_child(self.root, "targets", payload["target_id"])
            self.broker.materialize_seed(target); return {"target": str(target), "commit": self.broker.validate_target(target)}
        if kind == "assignment": return self._assignment(payload)
        if kind == "session-open": return self._open(payload)
        if kind == "session-proposal": return self._session(payload, "proposal")
        if kind == "session-execute": return self._session(payload, "execute")
        if kind == "session-close": return self._session(payload, "close")
        if kind == "session-abort": return self._session(payload, "abort")
        if kind == "server-limitation":
            if set(payload) != {"decision_path", "limitation_id"}: raise AdmissionError("limitation payload is closed")
            return self.broker.record_server_limitation(Path(payload["decision_path"]), payload["limitation_id"], self.verifier)
        if kind == "shutdown": return self._shutdown(payload)
        raise AdmissionError("unknown request kind")

    def _assignment(self, p: dict[str, Any]) -> dict[str, Any]:
        if set(p) != {"assignment_id", "role", "target_id", "task", "prompt"} or p["role"] not in _ROLES or not all(isinstance(p[k], str) and p[k] for k in p): raise AdmissionError("assignment payload is closed")
        if self.active_worker: raise AdmissionError("exactly one target worker may be active")
        target = _safe_child(self.root, "targets", p["target_id"]); head = self.broker.validate_target(target)
        model, effort, tier, finding = _ROLES[p["role"]]; ws = target / ".agent-workspace"; ws.mkdir(exist_ok=True)
        _safe_child(self.root, "runtime").mkdir(parents=True, exist_ok=True)
        _safe_child(self.root, "events").mkdir(parents=True, exist_ok=True)
        prompt = ws / (p["assignment_id"] + ".prompt.md")
        prompt.write_text(p["prompt"], encoding="utf-8")
        invocation = {"schema":"orchestrator-coding-invocation/v1","action":"start","runtime_root":str(_safe_child(self.root,"runtime")),"run_root":str(target),"repository":{"common_dir":str((target / ".git").resolve()),"worktree_root":str(target),"branch":"master","base_commit":head,"merge_inputs":[]},"prompt_path":str(prompt),"prompt_sha256":_sha(prompt),"output_paths":{"status":str(ws/(p["assignment_id"]+".status.json")),"jsonl":str(ws/(p["assignment_id"]+".jsonl")),"stderr":str(ws/(p["assignment_id"]+".stderr.log")),"last_message":str(ws/(p["assignment_id"]+".last-message.txt"))},"event_log_path":str(_safe_child(self.root,"events","LANE_EVENTS.jsonl")),"lane_id":p["role"],"worker_invocation_id":p["assignment_id"],"task":p["task"],"phase":"c3","exclusive_resources":[],"codex":{"command":["codex"],"model":model,"reasoning_effort":effort,"service_tier":tier,"sandbox":"danger-full-access","approval_policy":"never","config_overrides":[]},"child_environment_isolation":True}
        if finding: invocation["finding_gate"] = {"role": finding, "path": str(ws / "FINDINGS.json")}
        ip = _safe_child(self.root, "assignments", p["assignment_id"] + ".invocation.json"); _write_new(ip, invocation)
        from orchestrator_harness.lane_controller import load_invocation, run
        self.active_worker = True
        try: exit_code = run(load_invocation(ip))
        finally: self.active_worker = False
        status = Path(invocation["output_paths"]["status"])
        return {"assignment_id":p["assignment_id"],"exit_code":exit_code,"invocation":{"path":str(ip),"sha256":_sha(ip)},"status":{"path":str(status),"sha256":_sha(status) if status.is_file() else None}}

    def _open(self, p: dict[str, Any]) -> dict[str, Any]:
        if set(p) != {"session_id", "request_path"} or not all(isinstance(v, str) and v for v in p.values()): raise AdmissionError("session-open payload is closed")
        if p["session_id"] in self.controllers: raise AdmissionError("session already exists")
        request = Path(p["request_path"]).resolve()
        if self.root not in request.parents: raise AdmissionError("session request escapes the attempt")
        c = FirmwareAcceptanceController(self.broker, topology=self.topology); result = c.open_session(request); self.controllers[p["session_id"]] = c; return result

    def _session(self, p: dict[str, Any], action: str) -> dict[str, Any]:
        sid = p.get("session_id"); c = self.controllers.get(sid) if isinstance(sid, str) else None
        if c is None: raise AdmissionError("unknown retained session")
        if action == "proposal" and set(p) == {"session_id","proposal_path","request"}: return c.session_publish_proposal(Path(p["proposal_path"]), p["request"])
        if action == "execute" and set(p) == {"session_id","proposal_path","decision_path","authorization_path"}: return c.session_execute_artifacts(Path(p["proposal_path"]),Path(p["decision_path"]),Path(p["authorization_path"]),self.verifier)
        if action == "close" and set(p) == {"session_id","decision_path"}:
            result=c.close_session(Path(p["decision_path"]),self.verifier)
            if not result.get("exact_reaped") or not result.get("claim_released"): raise AdmissionError("session close lacks terminal cleanup")
            del self.controllers[sid]; return result
        if action == "abort" and set(p) == {"session_id","reason"} and isinstance(p["reason"],str) and p["reason"]:
            result=c.abort_session(p["reason"])
            if not result.get("exact_reaped") or not result.get("claim_released"): raise AdmissionError("session abort lacks terminal cleanup")
            del self.controllers[sid]; return result
        raise AdmissionError("session operation payload is closed")

    def _shutdown(self, p: dict[str, Any]) -> dict[str, Any]:
        if p: raise AdmissionError("shutdown payload must be empty")
        terminals=[]
        if self.active_worker: return {"state":"BLOCKED","reason":"target worker is active"}
        for sid, controller in list(self.controllers.items()):
            terminal=controller.abort_session("signed harness shutdown")
            if not terminal.get("exact_reaped") or not terminal.get("claim_released"): raise AdmissionError("shutdown session cleanup is incomplete")
            terminals.append({"session_id":sid,"terminal":terminal}); del self.controllers[sid]
        self.shutdown=True
        return {"state":"SHUTDOWN","sessions":terminals,"active_worker":self.active_worker}


def main(argv: list[str] | None = None) -> int:
    parser=argparse.ArgumentParser(prog="firmware-acceptance-c3-harness"); parser.add_argument("--root",type=Path,required=True); parser.add_argument("--seed",type=Path,required=True); parser.add_argument("--policy",type=Path,required=True); parser.add_argument("--templates",type=Path,required=True); parser.add_argument("--topology-root",type=Path,required=True); sub=parser.add_subparsers(dest="command",required=True); serve=sub.add_parser("serve"); serve.add_argument("--poll-seconds",type=float,default=.25)
    a=parser.parse_args(argv); h=C3Harness(a.root,a.seed,a.policy,a.templates,a.topology_root)
    while not h.shutdown:
        paths=sorted(h.request_root.glob("*.json")); pending=[x for x in paths if not (_safe_child(h.response_root,x.name)).exists()]
        if not pending: time.sleep(a.poll_seconds); continue
        for request in pending: print(json.dumps(h.handle(request),sort_keys=True),flush=True)
    return 0

if __name__ == "__main__": raise SystemExit(main())
