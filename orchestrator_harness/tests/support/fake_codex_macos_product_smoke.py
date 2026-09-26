#!/usr/bin/env python3
"""Controlled Codex stand-in for the hermetic macOS product demonstration.

This fixture implements only the shipped Codex adapter's process contract. It
never contacts a provider and every artifact it writes is explicitly labeled
synthetic.
"""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def _content_hash(record: dict[str, Any]) -> str:
    payload = {key: value for key, value in record.items() if key != "content_hash"}
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


def main() -> int:
    worktree = Path.cwd()
    prompt = sys.stdin.read()
    invocation = json.loads(
        (worktree / ".agent-workspace" / "invocation.json").read_text(
            encoding="utf-8"
        )
    )
    evidence_path = (
        worktree / ".agent-workspace" / "synthetic-provider-evidence.json"
    )
    evidence = {
        "schema": "synthetic-provider-evidence/v1",
        "synthetic": True,
        "live_provider_proof": False,
        "label": "SYNTHETIC FAKE CODEX MACOS PRODUCT DEMONSTRATION ONLY",
        "prompt_received": bool(prompt.strip()),
        "argv": sys.argv[1:],
    }
    evidence_path.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    result = {
        "schema": "result/v1",
        "lane_id": invocation["lane_id"],
        "run_id": invocation["run_id"],
        "outcome": "PASS",
        "summary": "Synthetic fake-provider result for the hermetic macOS product demonstration.",
        "evidence": [
            {
                "path": str(evidence_path),
                "synthetic": True,
                "live_provider_proof": False,
            }
        ],
        "completed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    result["content_hash"] = _content_hash(result)
    (worktree / "RESULT.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {"type": "thread.started", "thread_id": "synthetic-smoke-session"}
        ),
        flush=True,
    )
    print(json.dumps({"type": "turn.completed"}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
