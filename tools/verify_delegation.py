"""Fail a run that lacks accepted, distinct native planning/coding/validation lanes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


ROLE_PREFIXES = ("planner-", "implementer-", "validator-")
REQUIRED_EVENTS = ("provider_started", "result_valid", "acceptance_copied")


def verify(runtime_root: Path) -> dict[str, str]:
    path = runtime_root / "monitor" / "MONITOR_IMPORTANT.log"
    if not path.is_file():
        raise ValueError(f"important monitor log missing: {path}")
    attempts: dict[tuple[str, str], dict[str, int]] = {}
    with path.open(encoding="utf-8") as stream:
        for position, line in enumerate(stream):
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"malformed monitor record at line {position + 1}") from exc
            if not isinstance(row, dict) or row.get("event") != "worker.lifecycle":
                continue
            lane_id, run_id, event = row.get("lane_id"), row.get("run_id"), row.get("worker_event")
            if not all(isinstance(value, str) and value for value in (lane_id, run_id, event)):
                continue
            if event == "acceptance_copied" and row.get("detail") != "ACCEPTED":
                continue
            if event in REQUIRED_EVENTS:
                attempts.setdefault((lane_id, run_id), {}).setdefault(event, position)

    chosen: dict[str, str] = {}
    previous_acceptance = -1
    for prefix in ROLE_PREFIXES:
        candidates = []
        for (lane_id, run_id), events in attempts.items():
            if not lane_id.startswith(prefix) or not set(REQUIRED_EVENTS).issubset(events):
                continue
            start, result, accepted = (events[name] for name in REQUIRED_EVENTS)
            if previous_acceptance < start < result < accepted and lane_id not in chosen.values():
                candidates.append((accepted, lane_id, run_id))
        if not candidates:
            role = prefix.removesuffix("-")
            raise ValueError(f"missing accepted native {role} lane after previous role")
        previous_acceptance, lane_id, _ = min(candidates)
        chosen[prefix.removesuffix("-")] = lane_id
    return chosen


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-root", type=Path, required=True)
    args = parser.parse_args()
    try:
        roles = verify(args.runtime_root)
    except (OSError, ValueError) as exc:
        print(f"DELEGATION_INCOMPLETE: {exc}")
        return 1
    print("DELEGATION_VERIFIED: " + json.dumps(roles, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
