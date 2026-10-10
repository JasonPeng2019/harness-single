"""Harness combined provider budget: initial assignment plus one retry."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

MAX_LANE_PROVIDER_INVOCATIONS = 2
WORKER_RETRY_LIMIT = "WORKER_RETRY_LIMIT"


def completed_provider_invocations(lane: dict[str, Any]) -> int:
    """Count actual launches across automatic corrections and fresh run IDs.

    The append-only controller ledger survives resume-lane. Header rows and
    proven pre-provider failures do not spend the budget; duplicate records
    for the same launch are counted once. Unreadable history cannot reset it.
    """
    path = Path(lane.get("attempts_path") or Path(lane["worktree_path"]) / ".agent-workspace/controller.attempts.jsonl")
    if not path.exists():
        if (lane.get("session") or {}).get("session_id"):
            raise ValueError("saved worker session has no invocation history; use a fresh lane/session")
        return 0
    launches: set[tuple[str, int]] = set()
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            raise ValueError("worker invocation history contains an invalid record")
        if row.get("provider_started") is not True:
            continue
        run_id, attempt = row.get("run_id"), row.get("attempt")
        if not isinstance(run_id, str) or not run_id or type(attempt) is not int or attempt < 1:
            raise ValueError("worker invocation history has an ambiguous launch identity")
        launches.add((run_id, attempt))
    if not launches and (lane.get("session") or {}).get("session_id"):
        raise ValueError("saved worker session has no proven invocation history; use a fresh lane/session")
    return len(launches)
