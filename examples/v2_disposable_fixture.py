"""Run a disposable fake-only rehearsal of the v2 acceptance assets.

This is intentionally not a substitute for ``examples/v2_live_matrix.py``.  It
uses no provider credential, no installed agent CLI, and reports no live claim.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from pathlib import Path


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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fake-only", action="store_true", help="required acknowledgement that no real CLI may run")
    parser.add_argument("--keep", type=Path, help="retain a failed/successful disposable root for diagnosis")
    args = parser.parse_args(argv)
    if not args.fake_only:
        parser.error("--fake-only is required; live evidence belongs exclusively to M09")
    if args.keep:
        root = args.keep.resolve()
        if root.exists():
            parser.error("--keep must name a path that does not already exist")
        root.mkdir(parents=True, exist_ok=False)
    else:
        root = Path(tempfile.mkdtemp(prefix="harness-v2-fake-"))
    try:
        result = run_fake_only(root)
        if args.keep:
            result["cleanup"] = "retained by --keep"
        else:
            shutil.rmtree(root)
            result["cleanup"] = "removed disposable root after fake-only rehearsal"
        print(json.dumps(result, sort_keys=True))
        return 0
    except BaseException as exc:
        print(f"fake-only fixture failed; retained at {root}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
