"""Read-only deterministic oracle for immutable C3 controller observations."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from firmware_acceptance.c3_process_identity import lane_controller_command_matches

_LIVE_STATES = {"WAITING_RESOURCE", "RUNNING_CODEX"}
_RAW = "controller did not publish an authentic initial status"
_REQUEST_KEYS = {"schema","request_id","attempt_id","c1_reference","delegated_reference","orchestrator_identity","topology_key_release","kind","issued_utc","issued_monotonic","expires_monotonic","payload","public_key","signature"}
_OBS_KEYS = {"schema","assignment_id","shape","launcher_identity","launcher_observed_identity","controller_identity","controller_status_identity","launcher_snapshot","controller_snapshot","invocation","initial_status","status_source"}

def _sha(path: Path) -> str: return hashlib.sha256(path.read_bytes()).hexdigest()

def _load(path: Path) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    try: digest = _sha(path)
    except OSError: digest = None
    ref = {"path":str(path.resolve()),"sha256":digest}
    try: value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError): return None, ref
    return value if isinstance(value,dict) else None, ref

def _identity(value: Any) -> bool:
    return isinstance(value,dict) and set(value) == {"pid","created_utc"} and not isinstance(value.get("pid"),bool) and isinstance(value.get("pid"),int) and value["pid"] > 0 and isinstance(value.get("created_utc"),str) and bool(value["created_utc"])

def _snapshot(value: Any) -> bool:
    return isinstance(value,dict) and set(value) == {"pid","ppid","name","command_line","created_utc"} and not isinstance(value.get("pid"),bool) and isinstance(value.get("pid"),int) and value["pid"] > 0 and not isinstance(value.get("ppid"),bool) and isinstance(value.get("ppid"),int) and value["ppid"] >= 0 and all(isinstance(value.get(k),str) and value[k] for k in ("name","command_line","created_utc"))

def _recovery_valid(value: Any) -> bool:
    keys = {"schema","assignment_id","launcher_pid","launcher_identity","observed_launcher_identity","controller_identity","worktree","branch","channels","cleanup","path","sha256"}
    if not isinstance(value,dict) or set(value) != keys or value.get("schema") != "firmware-c3-recovery-required/v1" or not isinstance(value.get("assignment_id"),str) or not value["assignment_id"] or isinstance(value.get("launcher_pid"),bool) or not isinstance(value.get("launcher_pid"),int) or value["launcher_pid"] <= 0 or not isinstance(value.get("path"),str) or not isinstance(value.get("sha256"),str) or len(value["sha256"]) != 64: return False
    pid = value["launcher_pid"]
    if any(item is not None and (not _identity(item) or item["pid"] != pid) for item in (value.get("launcher_identity"),value.get("observed_launcher_identity"))): return False
    controller = value.get("controller_identity")
    cleanup = value.get("cleanup")
    cleanup_keys = {"schema","assignment_id","reason","launcher_identity","controller_identity","handles_closed","launcher_reaped","controller_reaped","process_reaped","worktree_removed","branch_removed","channels_removed","outcome"}
    return (controller is None or _identity(controller)) and isinstance(cleanup,dict) and set(cleanup) == cleanup_keys and cleanup.get("schema") == "firmware-c3-prestart-rejection/v1" and cleanup.get("assignment_id") == value["assignment_id"] and cleanup.get("launcher_identity") == value.get("launcher_identity") and cleanup.get("controller_identity") == controller and cleanup.get("outcome") == "RECOVERY_REQUIRED"

def _observation_authentic(value: dict[str, Any] | None) -> bool:
    if not isinstance(value,dict) or set(value) != _OBS_KEYS or value.get("schema") != "firmware-c3-controller-observation/v1" or value.get("shape") not in {"same-process","direct-venv-redirector"}: return False
    launcher, observed, controller, status_identity = (value.get(k) for k in ("launcher_identity","launcher_observed_identity","controller_identity","controller_status_identity"))
    ls, cs, initial, invocation, source = (value.get(k) for k in ("launcher_snapshot","controller_snapshot","initial_status","invocation","status_source"))
    if not (_identity(launcher) and observed == launcher and _identity(controller) and _identity(status_identity) and _snapshot(ls) and _snapshot(cs) and isinstance(value.get("assignment_id"),str) and value["assignment_id"]): return False
    if ls["pid"] != launcher["pid"] or cs["pid"] != controller["pid"] or cs["pid"] != status_identity["pid"] or cs["created_utc"] != status_identity["created_utc"]: return False
    if value["shape"] == "same-process":
        if ls["pid"] != cs["pid"] or launcher != controller: return False
    elif cs["ppid"] != ls["pid"]: return False
    if not (isinstance(invocation,dict) and set(invocation)=={"path","sha256"} and isinstance(invocation.get("path"),str) and isinstance(invocation.get("sha256"),str) and len(invocation["sha256"]) == 64): return False
    try:
        if _sha(Path(invocation["path"])) != invocation["sha256"]: return False
    except OSError: return False
    if not lane_controller_command_matches(value["shape"], cs["command_line"], invocation["path"]): return False
    if not (isinstance(initial,dict) and set(initial)=={"sha256","value"} and isinstance(initial.get("sha256"),str) and len(initial["sha256"]) == 64 and isinstance(initial.get("value"),dict)): return False
    status = initial["value"]
    return (isinstance(source,dict) and set(source)=={"path","sha256"} and source.get("sha256") == initial["sha256"] and isinstance(source.get("path"),str)
            and set(status) == {"schema","state","controller_pid","controller_created_utc"} and status.get("schema") == "orchestrator-lane-controller/v1" and status.get("state") in _LIVE_STATES and not isinstance(status.get("controller_pid"),bool) and isinstance(status.get("controller_pid"),int) and status["controller_pid"] > 0 and isinstance(status.get("controller_created_utc"),str) and bool(status["controller_created_utc"]) and status.get("controller_pid") == status_identity["pid"] and status.get("controller_created_utc") == status_identity["created_utc"])

def _rejected(response: dict[str, Any] | None) -> bool:
    if not isinstance(response,dict) or not isinstance(response.get("request_id"),str) or not response["request_id"]: return False
    prefix = "pre-registration cleanup requires recovery: " + _RAW
    if set(response) == {"schema","request_id","outcome","reason"} and response.get("schema") == "firmware-c3-harness-response/v1" and response.get("outcome") == "REJECTED": return response.get("reason") in {_RAW,prefix}
    recovery = response.get("recovery")
    recovery_keys = {"schema","assignment_id","launcher_pid","launcher_identity","observed_launcher_identity","controller_identity","worktree","branch","channels","cleanup","path","sha256"}
    return (set(response) == {"schema","request_id","outcome","reason","recovery"} and response.get("schema") == "firmware-c3-harness-response/v1" and response.get("outcome") == "RECOVERY_REQUIRED" and response.get("reason") == prefix and isinstance(recovery,dict) and set(recovery) == recovery_keys and _recovery_valid(recovery))

def classify(request_path: Path, response_path: Path, observation_path: Path, status_path: Path) -> dict[str, Any]:
    request, request_ref = _load(request_path); response, response_ref = _load(response_path)
    observation, observation_ref = _load(observation_path); _, status_ref = _load(status_path)
    correlated = (isinstance(request,dict) and set(request) == _REQUEST_KEYS and request.get("schema") == "firmware-c3-harness-request/v1" and request.get("kind") == "assignment" and isinstance(request.get("request_id"),str) and isinstance(request.get("payload"),dict) and isinstance(observation,dict) and request["request_id"] == (response or {}).get("request_id") and request["payload"].get("assignment_id") == observation.get("assignment_id"))
    authentic = _observation_authentic(observation)
    recovery_bound = not isinstance(response,dict) or response.get("outcome") != "RECOVERY_REQUIRED"
    if isinstance(response,dict) and response.get("outcome") == "RECOVERY_REQUIRED": recovery_bound = isinstance(request,dict) and isinstance(response.get("recovery"),dict) and response["recovery"].get("assignment_id") == request.get("payload",{}).get("assignment_id")
    return {"schema":"firmware-c3-watcher-oracle/v1","classification":"ABORT_REQUIRED" if correlated and recovery_bound and authentic and _rejected(response) else "EXPECTED_CONTAINMENT","invariant":"C3_AUTHENTIC_INITIAL_CONTROLLER_STATUS","request":request_ref,"response":response_ref,"observation":observation_ref,"status":status_ref,"evidence":{"request_response_observation_correlated":correlated,"recovery_assignment_bound":recovery_bound,"observation_authenticates_initial_controller":authentic,"candidate_rejected_authentic_status":_rejected(response)}}

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="firmware-c3-watcher-oracle")
    parser.add_argument("--request",type=Path,required=True); parser.add_argument("--response",type=Path,required=True); parser.add_argument("--observation",type=Path,required=True); parser.add_argument("--status",type=Path,required=True)
    args = parser.parse_args(argv); print(json.dumps(classify(args.request,args.response,args.observation,args.status),sort_keys=True)); return 0

if __name__ == "__main__": raise SystemExit(main())
