"""M09-only native CLI matrix runner; planning/fakes never satisfy a live claim.

By default this prints the reserved matrix.  ``--execute`` requires an explicit
M09 authorization environment value and a JSON manifest whose commands invoke the
real target CLI and whose cleanup command proves the disposable runtime is retired.
No credential is read, copied, or accepted as an argument by this script.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


CHECKS = {
    "CHECK-LIVE-1": "Stop hooks reject unresolved/missing/invalid work and allow valid terminals.",
    "CHECK-LIVE-2": "ROOT and worker queues remain role-isolated.",
    "CHECK-LIVE-3": "Liveness restarts dead/hung monitor, never deliberate stop.",
    "CHECK-LIVE-4": "A serial exclusive lease is released only after cleanup proof and then reused.",
}
AUTHORIZATION_ENV = "HARNESS_V2_M09_AUTHORIZED"


def _load_manifest(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema") != "harness-v2-live-matrix/v1":
        raise ValueError("manifest schema must be harness-v2-live-matrix/v1")
    attempts = value.get("attempts")
    if not isinstance(attempts, list) or len(attempts) != 4:
        raise ValueError("manifest must declare exactly four live attempts")
    names = {attempt.get("name") for attempt in attempts if isinstance(attempt, dict)}
    if names != set(CHECKS):
        raise ValueError("manifest attempts must be exactly CHECK-LIVE-1 through CHECK-LIVE-4")
    for attempt in attempts:
        if not isinstance(attempt, dict):
            raise ValueError("attempt must be an object")
        for key in ("target", "command", "cleanup_command", "evidence_paths"):
            if key not in attempt:
                raise ValueError(f"{attempt.get('name')} lacks {key}")
        if not isinstance(attempt["command"], list) or not all(isinstance(x, str) and x for x in attempt["command"]):
            raise ValueError(f"{attempt['name']} command must be a nonempty argv list")
        if not isinstance(attempt["cleanup_command"], list) or not all(isinstance(x, str) and x for x in attempt["cleanup_command"]):
            raise ValueError(f"{attempt['name']} cleanup_command must be a nonempty argv list")
    return value


def _reserved() -> dict[str, object]:
    return {"schema": "harness-v2-live-matrix-result/v1", "outcome": "RESERVED_FOR_M09", "authorization": "not requested", "checks": [{"name": name, "scenario": scenario, "outcome": "NOT_RUN_LIVE"} for name, scenario in CHECKS.items()]}


def execute(manifest: dict[str, Any]) -> dict[str, object]:
    if os.environ.get(AUTHORIZATION_ENV) != "M09":
        raise PermissionError(f"--execute requires {AUTHORIZATION_ENV}=M09 from the authorized M09 executor")
    results: list[dict[str, object]] = []
    for attempt in manifest["attempts"]:
        command = subprocess.run(attempt["command"], text=True, capture_output=True, check=False)
        cleanup = subprocess.run(attempt["cleanup_command"], text=True, capture_output=True, check=False)
        evidence = [Path(item) for item in attempt["evidence_paths"] if isinstance(item, str)]
        outcome = "PASS" if command.returncode == 0 and cleanup.returncode == 0 and evidence and all(item.exists() for item in evidence) else "FAIL"
        results.append({"name": attempt["name"], "target": attempt["target"], "outcome": outcome, "command_returncode": command.returncode, "cleanup_returncode": cleanup.returncode, "evidence_paths": [str(item) for item in evidence], "cleanup_required": True})
    return {"schema": "harness-v2-live-matrix-result/v1", "outcome": "PASS" if all(item["outcome"] == "PASS" for item in results) else "FAIL", "authorization": "M09", "checks": results}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--result", type=Path, help="optional external result path; never write into source")
    args = parser.parse_args(argv)
    try:
        if not args.execute:
            result = _reserved()
        elif args.manifest is None:
            parser.error("--execute requires a M09 manifest")
        else:
            result = execute(_load_manifest(args.manifest.resolve()))
        encoded = json.dumps(result, sort_keys=True)
        if args.result:
            args.result.parent.mkdir(parents=True, exist_ok=True)
            args.result.write_text(encoded + "\n", encoding="utf-8")
        print(encoded)
        return 0 if result["outcome"] in {"RESERVED_FOR_M09", "PASS"} else 1
    except (OSError, ValueError, PermissionError, json.JSONDecodeError) as exc:
        print(f"live matrix did not run: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
