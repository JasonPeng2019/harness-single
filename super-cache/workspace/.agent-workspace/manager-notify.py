#!/usr/bin/env python3
"""Provider-neutral worker escalation helper.

Writes one durable notice into the worker outbox
(``.agent-workspace/manager-notifications/``).  The monitor consumes outbox
files and promotes them into the manager queue; the worker never writes the
manager queue directly.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BINDING_SCHEMA = "harness-hook-binding/v1"
NOTICE_SCHEMA = "manager-notice/v1"
SEVERITIES = frozenset({"blocking", "info"})

try:
    from orchestrator_harness.core import iso_utc as _runtime_iso_utc
    from orchestrator_harness.records import (
        atomic_write_json as _runtime_atomic_write_json,
    )
except Exception:  # pragma: no cover - standalone fallback
    _runtime_iso_utc = None
    _runtime_atomic_write_json = None


def iso_utc() -> str:
    if _runtime_iso_utc is not None:
        return _runtime_iso_utc()
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"record is not a JSON object: {path}")
    return value


def _atomic_write_json(path: Path, value: Any) -> None:
    if _runtime_atomic_write_json is not None:
        _runtime_atomic_write_json(path, value)
        return
    target = Path(path).absolute()
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent)
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temp_path, target)
    except BaseException:
        try:
            temp_path.unlink()
        except OSError:
            pass
        raise


def _binding(agent_workspace: Path) -> dict[str, Any]:
    path = agent_workspace / "harness-hook-binding.json"
    if not path.is_file():
        raise ValueError(f"worker binding missing: {path}")
    record = _read_json(path)
    if record.get("schema") != BINDING_SCHEMA:
        raise ValueError(f"binding schema mismatch at {path}")
    return record


def _outbox_dir(agent_workspace: Path) -> Path:
    binding = _binding(agent_workspace)
    raw = binding.get("outbox_dir")
    return Path(raw) if raw else agent_workspace / "manager-notifications"


def write_notice(
    agent_workspace: Path,
    *,
    severity: str,
    summary: str,
    detail: str | None = None,
) -> dict[str, Any]:
    binding = _binding(agent_workspace)
    notice: dict[str, Any] = {
        "schema": NOTICE_SCHEMA,
        "notice_id": uuid.uuid4().hex,
        "lane_id": binding.get("lane_id"),
        "run_id": binding.get("run_id"),
        "severity": severity,
        "summary": summary,
        "created_at": iso_utc(),
    }
    if detail:
        notice["detail"] = detail
    outbox = _outbox_dir(agent_workspace)
    outbox.mkdir(parents=True, exist_ok=True)
    path = outbox / f"notice-{notice['notice_id']}.json"
    _atomic_write_json(path, notice)
    return notice


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="manager-notify",
        description="Raise a worker escalation notice for ROOT.",
    )
    parser.add_argument(
        "--agent-workspace",
        default=None,
        help="path to the worktree .agent-workspace (default: this script's directory)",
    )
    parser.add_argument(
        "--severity",
        choices=sorted(SEVERITIES),
        default="blocking",
        help="notice severity (default: blocking)",
    )
    parser.add_argument(
        "--summary", required=True, help="the decision or action ROOT needs"
    )
    parser.add_argument("--detail", default=None, help="relevant local evidence")
    args = parser.parse_args(argv)
    agent_workspace = (
        Path(args.agent_workspace).resolve()
        if args.agent_workspace
        else Path(__file__).resolve().parent
    )
    try:
        notice = write_notice(
            agent_workspace,
            severity=args.severity,
            summary=args.summary,
            detail=args.detail,
        )
    except (OSError, ValueError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(notice, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
