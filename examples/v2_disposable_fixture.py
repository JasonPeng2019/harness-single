"""Run disposable, synthetic rehearsals of the v2 acceptance assets.

Neither mode is a substitute for ``examples/v2_live_matrix.py``.  They use no
provider credential, installed agent CLI, network service, or live target.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from orchestrator_harness.tests.v2_acceptance.contract import (  # noqa: E402
    assert_review_pair,
    assert_valid_result,
    atomic_json,
    content_hash,
)
from orchestrator_harness.tests.v2_acceptance.reference_runtime import ReferenceRuntime  # noqa: E402


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _disposable_root(prefix: str) -> Path:
    """Create a root below this checkout, never in the system temporary area."""
    workspace = ROOT / ".agent-workspace"
    workspace.mkdir(exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=prefix, dir=workspace))


def _validate_keep_root(root: Path) -> Path:
    root = root.resolve()
    try:
        root.relative_to(ROOT)
    except ValueError as exc:
        raise ValueError("--keep must be inside this workspace") from exc
    if root.exists():
        raise ValueError("--keep must name a path that does not already exist")
    root.parent.mkdir(parents=True, exist_ok=True)
    root.mkdir()
    return root


def run_fake_only(root: Path) -> dict[str, object]:
    """Exercise fake lifecycle evidence and prove its exact cleanup boundary."""
    runtime_root = root / "project with spaces" / ".harness-runtime"
    records = runtime_root / "epochs" / "epoch-1" / "lanes" / "fixture"
    records.mkdir(parents=True)
    state_path = runtime_root / "RUNTIME_STATE.json"
    atomic_json(state_path, {"schema": "runtime-state/v1", "state": "OPEN"})

    fake = ReferenceRuntime()
    fake.bootstrap("fixture", "run-1", ("fixture-resource",))
    fake.launch("fixture", "pid:101@created:fixture")
    fake.terminal("fixture", "review_pending")
    event = fake.events[0]
    fake.acknowledge(event["event_id"])
    result: dict[str, object] = {
        "schema": "result/v1",
        "lane_id": "fixture",
        "run_id": "run-1",
        "outcome": "PASS",
        "summary": "fake-only acceptance rehearsal",
        "evidence": ["evidence/fake.txt"],
        "completed_at": "2026-01-01T00:00:00Z",
    }
    result["content_hash"] = content_hash(result)
    assert_valid_result(result, "fixture", "run-1")
    review, acceptance = fake.review_pair("fixture", "PASS", "ACCEPTED")
    assert_review_pair(review, acceptance)
    fake.cleanup("fixture", "pid:101@created:fixture")
    if fake.leases:
        raise RuntimeError("fake lease cleanup was incomplete")
    return {
        "schema": "v2-disposable-fixture/v1",
        "evidence_class": "synthetic",
        "checks": ["CHECK-U1", "CHECK-U2", "CHECK-U3", "CHECK-U4", "CHECK-U5"],
        "live_claims": "RESERVED_FOR_M09",
        "manager_event_state": event["state"],
        "resource_claims_remaining": len(fake.leases),
        "runtime_root": str(runtime_root),
    }


_LOCAL_FAKE_CHILD = r"""
import json
import os
import sys
from datetime import datetime, timezone

token = sys.argv[1]
created_at = datetime.now(timezone.utc).isoformat()
print(json.dumps({"event": "observer-ready", "pid": os.getpid(), "created_at": created_at, "identity_token": token}), flush=True)
action = json.loads(sys.stdin.readline())
print(json.dumps({"event": "action-complete", "action": action["name"], "unit": action["unit"], "identity_token": token}), flush=True)
print("local-fake-child:" + action["name"], file=sys.stderr, flush=True)
"""


def _run_local_fake_child(root: Path, action: str, unit: str) -> dict[str, Any]:
    """Run one local Python child and retain its exact observed identity."""
    identity_token = uuid4().hex
    command = [sys.executable, "-c", _LOCAL_FAKE_CHILD, identity_token]
    started_at = _timestamp()
    process = subprocess.Popen(
        command,
        cwd=root,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdin and process.stdout and process.stderr
    ready_line = process.stdout.readline()
    observer_ready_at = _timestamp()
    ready = json.loads(ready_line)
    if ready["event"] != "observer-ready" or ready["pid"] != process.pid:
        raise RuntimeError("local fake observer did not report its spawned process")
    if ready["identity_token"] != identity_token:
        raise RuntimeError("local fake observer identity token mismatch")
    action_dispatched_at = _timestamp()
    process.stdin.write(json.dumps({"name": action, "unit": unit}) + "\n")
    process.stdin.flush()
    process.stdin.close()
    stdout_tail = process.stdout.read()
    stderr = process.stderr.read()
    exit_code = process.wait()
    ended_at = _timestamp()
    action_record = json.loads(stdout_tail)
    if action_record["identity_token"] != identity_token or exit_code != 0:
        raise RuntimeError("local fake child failed its correlated action")
    return {
        "command": command,
        "stdout": [ready, action_record],
        "stderr": stderr,
        "exit_code": exit_code,
        "started_at": started_at,
        "observer_ready_at": observer_ready_at,
        "action_dispatched_at": action_dispatched_at,
        "ended_at": ended_at,
        "observer_ready_before_action": observer_ready_at <= action_dispatched_at,
        "process_identity": {
            "pid": process.pid,
            "creation_time_utc": ready["created_at"],
            "identity_token": identity_token,
        },
        "exact_identity_closed": process.poll() == 0,
    }


def run_readiness_only(root: Path, input_arguments: list[str]) -> dict[str, object]:
    """Rehearse the M08 control boundary with only local disposable fakes."""
    lane_id = "readiness-fixture"
    run_id = "readiness-run-1"
    worker_invocation_id = "readiness-worker-1"
    runtime_root = root / "runtime" / "epochs" / run_id
    checkpoint_path = runtime_root / "checkpoint.json"
    fake_target_root = root / "fake-target"
    runtime_root.mkdir(parents=True)
    fake_target_root.mkdir(parents=True)
    units = ["unit-1", "unit-2", "unit-3"]
    checkpoint = {
        "schema": "readiness-checkpoint/v1",
        "lane_id": lane_id,
        "run_id": run_id,
        "worker_invocation_id": worker_invocation_id,
        "completed_units": [units[0]],
        "pending_units": units[1:],
    }
    atomic_json(checkpoint_path, checkpoint)
    checkpoint_read = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    resume_unit = checkpoint_read["pending_units"][0]
    if checkpoint_read != checkpoint or resume_unit != "unit-2":
        raise RuntimeError("atomic checkpoint did not resume from the earliest pending unit")

    binding = {
        "kind": "local-fake-target/v1",
        "target_root": str(fake_target_root),
        "authorization": {
            "principal": "readiness-fixture",
            "scopes": ["fake:execute"],
            "credential": None,
            "retained": True,
        },
    }
    aborted = _run_local_fake_child(root, "abort", resume_unit)
    recovered = _run_local_fake_child(root, "recover", resume_unit)
    if not aborted["observer_ready_before_action"] or not recovered["observer_ready_before_action"]:
        raise RuntimeError("observer was not ready before the local child action")

    process_correlation = {
        "lane_id": lane_id,
        "run_id": run_id,
        "worker_invocation_id": worker_invocation_id,
        "abort_process_identity": aborted["process_identity"],
        "recovery_process_identity": recovered["process_identity"],
    }
    resource_closed = False
    cleanup_passes = 0
    for _ in range(2):
        if fake_target_root.exists():
            shutil.rmtree(fake_target_root)
        resource_closed = not fake_target_root.exists()
        cleanup_passes += 1
    if not resource_closed or not aborted["exact_identity_closed"] or not recovered["exact_identity_closed"]:
        raise RuntimeError("local fake terminal closure was not verified")
    return {
        "schema": "v2-disposable-readiness/v1",
        "evidence_class": "synthetic/readiness-only",
        "live_claims": "RESERVED_FOR_M09",
        "no_real_side_effect": {
            "status": "CONFIRMED_LOCAL_FAKE_ONLY",
            "used_installed_target_or_provider_cli": False,
            "used_credentials": False,
            "used_network_service": False,
            "used_scarce_namespace": False,
            "used_m09_execution": False,
        },
        "input_arguments": input_arguments,
        "identity_correlation": process_correlation,
        "checkpoint": {
            "path": str(checkpoint_path),
            "atomic_write": True,
            "read_back_matches": checkpoint_read == checkpoint,
            "resume_from_earliest_pending_unit": resume_unit,
        },
        "fake_target_binding": binding,
        "child_actions": {"abort": aborted, "recovery": recovered},
        "observer_ready_before_child_action": True,
        "abort": {
            "requested_process_identity": aborted["process_identity"],
            "observed_process_identity": aborted["process_identity"],
            "exact_identity_match": True,
            "cooperative_exit": aborted["exit_code"] == 0,
        },
        "recovery_retry": {
            "attempt": 2,
            "resumed_unit": resume_unit,
            "exit_code": recovered["exit_code"],
        },
        "terminal_closure": {
            "abort_process_closed": aborted["exact_identity_closed"],
            "recovery_process_closed": recovered["exact_identity_closed"],
            "fake_resource_closed": resource_closed,
        },
        "idempotent_cleanup": {"passes": cleanup_passes, "safe": True},
        "runtime_root": str(runtime_root),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--fake-only", action="store_true", help="run the existing fake-only rehearsal")
    mode.add_argument("--readiness-only", action="store_true", help="run only local readiness fakes; never M09")
    parser.add_argument("--keep", type=Path, help="retain the disposable workspace-local root for diagnosis")
    args = parser.parse_args(argv)
    try:
        root = _validate_keep_root(args.keep) if args.keep else _disposable_root("harness-v2-")
    except ValueError as exc:
        parser.error(str(exc))
    try:
        result = run_readiness_only(root, sys.argv[1:] if argv is None else argv) if args.readiness_only else run_fake_only(root)
        if args.keep:
            result["cleanup"] = {"disposable_root": "retained by --keep", "root_exists": root.is_dir()}
        else:
            shutil.rmtree(root)
            result["cleanup"] = {"disposable_root_removed": not root.exists(), "artifacts_proven_gone": not root.exists()}
        print(json.dumps(result, sort_keys=True))
        return 0
    except BaseException as exc:
        print(f"disposable fixture failed; retained at {root}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
