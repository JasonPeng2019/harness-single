"""Read-only deterministic oracle for the C3 initial-controller-status invariant."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


_LIVE_STATES = {"WAITING_RESOURCE", "RUNNING_CODEX"}
_REJECTION_KEYS = {"schema", "request_id", "outcome", "reason"}
_STATUS_REQUIRED_KEYS = {"schema", "state", "controller_pid", "controller_created_utc"}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load(path: Path) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    try:
        digest: str | None = _sha(path)
    except OSError:
        digest = None
    ref = {"path": str(path.resolve()), "sha256": digest}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, ref
    return value if isinstance(value, dict) else None, ref


def classify(response_path: Path, status_path: Path, *, expected_pid: int, expected_created_utc: str) -> dict[str, Any]:
    """Classify only the proven contradiction; malformed or unrelated data contains normally."""
    response, response_ref = _load(response_path)
    status, status_ref = _load(status_path)
    rejected_unauthentic = (isinstance(response, dict) and set(response) == _REJECTION_KEYS
                            and response.get("schema") == "firmware-c3-harness-response/v1"
                            and isinstance(response.get("request_id"), str) and response["request_id"]
                            and response.get("outcome") == "REJECTED"
                            and response.get("reason") == "controller did not publish an authentic initial status")
    authentic = (isinstance(status, dict) and _STATUS_REQUIRED_KEYS <= set(status)
                 and status.get("schema") == "orchestrator-lane-controller/v1"
                 and status.get("controller_pid") == expected_pid
                 and status.get("controller_created_utc") == expected_created_utc
                 and status.get("state") in _LIVE_STATES)
    contradiction = rejected_unauthentic and authentic
    return {
        "schema": "firmware-c3-watcher-oracle/v1",
        "classification": "ABORT_REQUIRED" if contradiction else "EXPECTED_CONTAINMENT",
        "invariant": "C3_AUTHENTIC_INITIAL_CONTROLLER_STATUS",
        "expected_controller_identity": {"pid": expected_pid, "created_utc": expected_created_utc},
        "response": response_ref,
        "status": status_ref,
        "evidence": {"candidate_rejected_unauthentic_status": rejected_unauthentic, "status_authentic_for_expected_live_controller": authentic},
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="firmware-c3-watcher-oracle")
    parser.add_argument("--response", type=Path, required=True)
    parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--expected-pid", type=int, required=True)
    parser.add_argument("--expected-created-utc", required=True)
    args = parser.parse_args(argv)
    if args.expected_pid <= 0 or not args.expected_created_utc:
        parser.error("expected controller identity is invalid")
    print(json.dumps(classify(args.response, args.status, expected_pid=args.expected_pid, expected_created_utc=args.expected_created_utc), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
