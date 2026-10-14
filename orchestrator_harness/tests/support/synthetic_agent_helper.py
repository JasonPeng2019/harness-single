from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("workspace")
    parser.add_argument("--identity-file", required=False)
    parser.add_argument("--wait-seconds", type=float, default=90)
    args = parser.parse_args()
    workspace = Path(args.workspace).resolve()
    request_dir = workspace / "permission-requests"
    request_dir.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    request_path = request_dir / "real-agent-synthetic-request.json"
    relay_path = request_dir / "real-agent-synthetic-request.relay.json"
    producer_pid = os.getpid()
    producer_started = started
    if args.identity_file:
        identity = json.loads(Path(args.identity_file).read_text(encoding="utf-8"))
        producer_pid = int(identity["pid"])
        producer_started = str(identity["started_utc"])
    request = {
        "schema": "orchestrator-real-agent-request/v1",
        "request_id": "real-agent-synthetic-request",
        "created_utc": started,
        "run": {"session_id": "real-agent-synthetic-session"},
        "live_lifetime": {
            "run_id": "real-agent-synthetic-run",
            "process": {
                "pid": producer_pid,
                "started_utc": producer_started,
            },
        },
        "roots": {
            "project": str(workspace.parent),
            "state": str(workspace),
        },
        "relay_path": str(relay_path),
        "zero_hardware": True,
        "zero_action_before_relay": True,
    }
    atomic_json(request_path, request)
    request_hash = hashlib.sha256(request_path.read_bytes()).hexdigest()
    Path(str(request_path) + ".sha256").write_text(
        f"{request_hash}  {request_path.name}\n", encoding="ascii"
    )

    deadline = time.monotonic() + args.wait_seconds
    while time.monotonic() < deadline:
        if relay_path.exists():
            try:
                relay = json.loads(relay_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                time.sleep(0.05)
                continue
            if (
                relay.get("request_sha256") == request_hash
                and relay.get("run_id") == "real-agent-synthetic-run"
                and relay.get("session_id") == "real-agent-synthetic-session"
            ):
                checkpoint = workspace / "PARALLEL_CHECKPOINT.md"
                checkpoint.write_text(
                    "# Real-agent synthetic checkpoint\n\n"
                    "The exact synthetic relay was observed. No external provider "
                    "service or physical action was used.\n",
                    encoding="utf-8",
                )
                atomic_json(
                    workspace / "synthetic-helper-result.json",
                    {
                        "status": "PASS",
                        "request_sha256": request_hash,
                        "relay_path": str(relay_path),
                        "inner_process_token": f"synthetic-{os.getpid()}",
                        "completed_utc": datetime.now(timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z"),
                    },
                )
                return 0
        time.sleep(0.05)
    return 4


if __name__ == "__main__":
    raise SystemExit(main())
