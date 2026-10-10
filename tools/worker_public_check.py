#!/usr/bin/env python3
"""Lane-local client for the operator's restricted public build/test service."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

REQUEST_SCHEMA = "worker-public-check/v1"
RESULT_SCHEMA = "worker-public-check-result/v1"
PENDING = 75


def read(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return value


def atomic(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".public-check-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True, indent=2)
            stream.write("\n")
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def request(agent: Path, check: str, command: list[str]) -> dict:
    route = read(agent / "public-check-route.json")
    binding = read(agent / "harness-hook-binding.json")
    if route.get("schema") != "worker-public-check-route/v1":
        raise ValueError("public check route is not installed")
    for field in ("lane_id", "run_id"):
        if binding.get(field) != route.get(field):
            raise ValueError("public check route is stale; ROOT must refresh the lane")
    revision = subprocess.check_output(
        ["git", "-C", str(agent.parent), "rev-parse", "HEAD"], text=True,
    ).strip()
    dirty = subprocess.check_output(
        ["git", "-C", str(agent.parent), "status", "--porcelain", "--",
         "src", "test_artifacts", "rfc8878.txt", "test.sh", "timer.sh", "scratch"],
        text=True,
    )
    if dirty.strip():
        raise ValueError("commit task code and scratch files before requesting a check")
    identifier = uuid.uuid4().hex
    value = {
        "schema": REQUEST_SCHEMA, "request_id": identifier,
        "lane_id": route["lane_id"], "run_id": route["run_id"],
        "revision": revision, "check": check, "command": command,
    }
    path = agent / "public-checks" / identifier / "request.json"
    atomic(path, value)
    return {"request_id": identifier, "request_path": str(path), "revision": revision}


def wait(agent: Path, identifier: str, timeout: float) -> tuple[dict, int]:
    if not re.fullmatch(r"[a-f0-9]{32}", identifier):
        raise ValueError("invalid public check request ID")
    directory = agent / "public-checks" / identifier
    original = read(directory / "request.json")
    digest = hashlib.sha256((directory / "request.json").read_bytes()).hexdigest()
    deadline = time.monotonic() + min(max(timeout, 0), 30)
    while True:
        if (directory / "result.json").is_file():
            result = read(directory / "result.json")
            if result.get("schema") != RESULT_SCHEMA or result.get("request_sha256") != digest:
                raise ValueError("uncorrelated public check result")
            for field in ("request_id", "lane_id", "run_id", "revision"):
                if result.get(field) != original.get(field):
                    raise ValueError("public check result identity mismatch")
            code = result.get("exit_code")
            return result, code if isinstance(code, int) and 0 <= code <= 255 else 2
        if time.monotonic() >= deadline:
            return {"request_id": identifier, "status": "PENDING",
                    "next_action": "wait again for this same request; remain in this worker session"}, PENDING
        time.sleep(0.25)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agent-workspace", type=Path, default=Path(__file__).resolve().parent)
    commands = parser.add_subparsers(dest="action", required=True)
    submit = commands.add_parser("request")
    submit.add_argument("--check", choices=("build", "public", "focused"), default="public")
    submit.add_argument("command", nargs=argparse.REMAINDER)
    poll = commands.add_parser("wait")
    poll.add_argument("--request-id", required=True)
    poll.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    try:
        agent = args.agent_workspace.resolve(strict=True)
        if args.action == "request":
            command = args.command[1:] if args.command[:1] == ["--"] else args.command
            if (args.check == "focused") != bool(command):
                raise ValueError("focused checks require a container command; build/public checks use fixed commands")
            result, code = request(agent, args.check, command), 0
        else:
            result, code = wait(agent, args.request_id, args.timeout)
        print(json.dumps(result, indent=2))
        return code
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"public check error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
