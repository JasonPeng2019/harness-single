"""Read-only deterministic oracle for C3 controller-identity admission evidence."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


_LIVE_STATES = {"WAITING_RESOURCE", "RUNNING_CODEX"}
_RAW_REJECTION = "controller did not publish an authentic initial status"
_REJECTION_KEYS = {"schema", "request_id", "outcome", "reason"}
_RELATION_KEYS = {"schema","assignment_id","shape","launcher_identity","controller_identity","controller_status_identity","launcher_snapshot","controller_snapshot","invocation","initial_status"}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load(path: Path) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    try: digest: str | None = _sha(path)
    except OSError: digest = None
    ref = {"path": str(path.resolve()), "sha256": digest}
    try: value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError): return None, ref
    return value if isinstance(value, dict) else None, ref


def _identity(value: Any) -> bool:
    return isinstance(value, dict) and set(value) == {"pid","created_utc"} and isinstance(value.get("pid"), int) and value["pid"] > 0 and isinstance(value.get("created_utc"), str) and bool(value["created_utc"])


def _snapshot(value: Any) -> bool:
    return (isinstance(value, dict) and set(value) == {"pid","ppid","name","command_line","created_utc"}
            and isinstance(value.get("pid"), int) and value["pid"] > 0 and isinstance(value.get("ppid"), int) and value["ppid"] >= 0
            and all(isinstance(value.get(key), str) and value[key] for key in ("name","command_line","created_utc")))


def _ref(value: Any, actual: dict[str, Any]) -> bool:
    return isinstance(value, dict) and set(value) == {"path","sha256"} and value == actual and isinstance(value["sha256"], str) and len(value["sha256"]) == 64


def _relationship_authentic(value: dict[str, Any] | None, relationship_ref: dict[str, Any], status_ref: dict[str, Any]) -> bool:
    if not isinstance(value, dict) or set(value) != _RELATION_KEYS or value.get("schema") != "firmware-c3-controller-relationship/v1" or value.get("shape") not in {"same-process","direct-venv-redirector"}:
        return False
    launcher, controller = value.get("launcher_identity"), value.get("controller_identity")
    status_identity = value.get("controller_status_identity")
    launcher_snapshot, controller_snapshot = value.get("launcher_snapshot"), value.get("controller_snapshot")
    initial = value.get("initial_status")
    if not (_identity(launcher) and _identity(controller) and _identity(status_identity) and _snapshot(launcher_snapshot) and _snapshot(controller_snapshot) and isinstance(value.get("assignment_id"), str) and value["assignment_id"]): return False
    if (launcher_snapshot["pid"] != launcher["pid"] or controller_snapshot["pid"] != controller["pid"]
            or controller_snapshot["pid"] != status_identity["pid"] or controller_snapshot["created_utc"] != status_identity["created_utc"]): return False
    if value["shape"] == "same-process":
        if launcher_snapshot["pid"] != controller_snapshot["pid"] or launcher != controller: return False
    elif controller_snapshot["ppid"] != launcher_snapshot["pid"]:
        return False
    invocation = value.get("invocation")
    if not (isinstance(invocation, dict) and set(invocation) == {"path","sha256"} and isinstance(invocation.get("path"), str) and isinstance(invocation.get("sha256"), str) and len(invocation["sha256"]) == 64): return False
    try:
        invocation_path = Path(invocation["path"])
        if _sha(invocation_path) != invocation["sha256"]: return False
    except OSError:
        return False
    command = controller_snapshot["command_line"].casefold()
    if "-m" not in command or "orchestrator_harness.lane_controller" not in command or str(invocation_path.resolve()).casefold() not in command:
        return False
    return (isinstance(initial, dict) and set(initial) == {"path","sha256","state","controller_identity"}
            and _ref({"path":initial.get("path"),"sha256":initial.get("sha256")}, status_ref)
            and initial.get("state") in _LIVE_STATES and initial.get("controller_identity") == status_identity)


def classify(response_path: Path, status_path: Path, relationship_path: Path) -> dict[str, Any]:
    """Classify only a hash-bound rejected authentic controller relationship."""
    response, response_ref = _load(response_path); status, status_ref = _load(status_path)
    relationship, relationship_ref = _load(relationship_path)
    rejected = (isinstance(response, dict) and set(response) == _REJECTION_KEYS and response.get("schema") == "firmware-c3-harness-response/v1"
                and isinstance(response.get("request_id"), str) and bool(response["request_id"]) and response.get("outcome") == "REJECTED"
                and response.get("reason") in {_RAW_REJECTION, "pre-registration cleanup requires recovery: " + _RAW_REJECTION})
    authentic = (_relationship_authentic(relationship, relationship_ref, status_ref) and isinstance(status, dict)
                 and status.get("schema") == "orchestrator-lane-controller/v1" and status.get("state") in _LIVE_STATES
                 and status.get("controller_pid") == relationship["controller_status_identity"]["pid"]
                 and status.get("controller_created_utc") == relationship["controller_status_identity"]["created_utc"])
    return {"schema":"firmware-c3-watcher-oracle/v1","classification":"ABORT_REQUIRED" if rejected and authentic else "EXPECTED_CONTAINMENT","invariant":"C3_AUTHENTIC_INITIAL_CONTROLLER_STATUS","response":response_ref,"status":status_ref,"relationship":relationship_ref,"evidence":{"candidate_rejected_authentic_status":rejected,"relationship_authenticates_live_controller":authentic}}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="firmware-c3-watcher-oracle")
    parser.add_argument("--response", type=Path, required=True); parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--relationship", type=Path, required=True)
    args = parser.parse_args(argv)
    print(json.dumps(classify(args.response, args.status, args.relationship), sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
