#!/usr/bin/env python3
"""Qwen Code ROOT PostToolUse hook bootstrap."""
from __future__ import annotations
import json
import sys
from pathlib import Path
PROVIDER_ID = "qwen-code"
BOUNDARY = "post-tool-use"

def _failure(reason: str) -> dict[str, object]:
    return {"continue": False, "stopReason": reason}

def main() -> int:
    sys.stdin.read()
    binding_path = Path(__file__).resolve().parent.parent / "orchestrator-harness-binding.json"
    try:
        binding = json.loads(binding_path.read_text(encoding="utf-8-sig"))
        harness_root = binding.get("harness_root") if isinstance(binding, dict) else None
        if not isinstance(harness_root, str) or not harness_root or not Path(harness_root).is_absolute():
            raise ValueError(f"installed {PROVIDER_ID} hook has no bound harness_root")
        sys.path.insert(0, harness_root)
        from orchestrator_harness.root_hook_wrapper import run
        output = run(PROVIDER_ID, BOUNDARY, binding_path)
    except Exception as exc:
        output = _failure(str(exc))
    print(json.dumps(output, sort_keys=True))
    return 0

if __name__ == "__main__":
    sys.exit(main())
