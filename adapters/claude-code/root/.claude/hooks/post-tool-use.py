#!/usr/bin/env python3
"""Provider PostToolUse hook: liveness and managed delivery receipts.

Runs as the provider's PostToolUse hook.  It appends one liveness receipt
per tool use to ``<workspace>/.agent-workspace/<role>-hook-liveness.jsonl``.
When the ``HARNESS_EVENT_ID`` environment variable is set and the hook runs
in the ROOT role, it also appends a DELIVERED receipt to the manager queue
for that event (the only manager-queue write a ROOT hook may perform).  The
hook never edits the queue otherwise and stays portable on Windows and
Linux.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BINDING_SCHEMA = "harness-hook-binding/v1"


def _iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"record is not a JSON object: {path}")
    return value


def _binding(hook_dir: Path) -> dict[str, Any]:
    path = hook_dir.parent / "orchestrator-harness-binding.json"
    if not path.is_file():
        return {}
    record = _read_json(path)
    return record if record.get("schema") == BINDING_SCHEMA else {}


def _append_line(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")


def _tool_name() -> str:
    return os.environ.get("HARNESS_TOOL_NAME") or os.environ.get("TOOL_NAME") or "unknown"


def main() -> int:
    hook_dir = Path(__file__).resolve().parent
    binding = _binding(hook_dir)
    role = binding.get("role") or "root"
    workspace = hook_dir.parents[1]
    agent_workspace = workspace / ".agent-workspace"
    _append_line(
        agent_workspace / f"{role}-hook-liveness.jsonl",
        {
            "schema": "hook-liveness/v1",
            "role": role,
            "provider_id": binding.get("provider_id"),
            "tool": _tool_name(),
            "at": _iso_utc(),
        },
    )
    event_id = os.environ.get("HARNESS_EVENT_ID")
    if event_id and role == "root":
        receipt = {"outcome": "DELIVERED", "event_id": event_id, "at": _iso_utc()}
        try:
            from orchestrator_harness.config import find_harness_root, load_config
            from orchestrator_harness.manager_queue import append_delivery_history

            harness_root = find_harness_root()
            rt = load_config(harness_root).runtime_root
            append_delivery_history(rt, event_id)
        except Exception as exc:  # pragma: no cover - runtime may be absent
            receipt = {
                "outcome": "DELIVERY_FAILED",
                "event_id": event_id,
                "error": str(exc),
                "at": _iso_utc(),
            }
        _append_line(agent_workspace / "root-hook-delivery.jsonl", receipt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
