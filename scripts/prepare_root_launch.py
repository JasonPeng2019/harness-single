"""Initialize and bind the run's native harness before Codex ROOT starts."""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from orchestrator_harness.windows_sandbox_preflight import require_ready

SCHEMA = "root-prelaunch-setup/v1"
RECEIPT = ".agent-workspace/ROOT_PRELAUNCH_SETUP.json"
HOOK_FILES = (
    ".codex/hooks.json",
    ".codex/orchestrator-harness-binding.json",
    ".codex/hooks/orchestrator_harness_post_tool_use.py",
    ".codex/hooks/orchestrator_harness_stop.py",
)


def read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return value


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check_installed(harness: Path, workspace: Path, *, allow_closed: bool = False) -> dict[str, str]:
    config = read(harness / "harness-config.json")
    if (Path(config.get("root_workspace", "")).resolve() != workspace
            or config.get("managed_coordination") != "enabled"):
        raise ValueError("harness configuration does not select this managed ROOT workspace")
    runtime = workspace / ".harness-runtime"
    allowed = {"OPEN", "CLOSED"} if allow_closed else {"OPEN"}
    if read(runtime / "RUNTIME_STATE.json").get("state") not in allowed:
        raise ValueError("ROOT's prepared harness runtime is not OPEN")
    binding = read(workspace / ".codex/orchestrator-harness-binding.json")
    for field, expected in {"schema": "harness-hook-binding/v1", "role": "root", "provider_id": "codex"}.items():
        if binding.get(field) != expected:
            raise ValueError(f"ROOT hook binding has the wrong {field}")
    for field, expected in (("harness_root", harness), ("runtime_root", runtime)):
        if Path(binding.get(field, "")).resolve() != expected:
            raise ValueError(f"ROOT hook binding has the wrong {field}")
    hooks = read(workspace / ".codex/hooks.json").get("hooks", {})
    for event, filename in (("PostToolUse", "orchestrator_harness_post_tool_use.py"), ("Stop", "orchestrator_harness_stop.py")):
        commands = [hook.get("command", "") for group in hooks.get(event, []) for hook in group.get("hooks", [])]
        if not any(filename in command for command in commands):
            raise ValueError(f"ROOT's native {event} hook is missing")
    return {relative: digest(workspace / relative) for relative in HOOK_FILES}


def validate_prepared_root(harness: Path, workspace: Path, run_id: str, *, resuming: bool = False) -> dict[str, Any]:
    harness, workspace = harness.resolve(), workspace.resolve()
    receipt_path = workspace / RECEIPT
    if not receipt_path.is_file():
        raise ValueError("ROOT setup must finish before CLI launch; pre-launch setup receipt is missing")
    receipt = read(receipt_path)
    expected = {"schema": SCHEMA, "run_id": run_id, "harness_dir": str(harness), "workspace": str(workspace)}
    if any(receipt.get(key) != value for key, value in expected.items()):
        raise ValueError("pre-launch setup receipt belongs to a different ROOT run")
    if receipt.get("status") != "PASS":
        raise ValueError("pre-launch harness setup did not pass")
    if receipt.get("hook_sha256") != check_installed(harness, workspace, allow_closed=resuming):
        raise ValueError("ROOT hook files changed after pre-launch setup")
    if receipt.get("harness_config_sha256") != digest(harness / "harness-config.json"):
        raise ValueError("harness configuration changed after pre-launch setup")
    sandbox_health = require_ready(harness, workspace=workspace)
    if receipt.get("windows_sandbox_health") != sandbox_health:
        raise ValueError("Windows sandbox health changed after pre-launch setup")
    sys.path.insert(0, str(harness))
    from orchestrator_harness.public_checks import policy, validate_service
    if policy(harness) is not None:
        closed = resuming and read(workspace / ".harness-runtime/RUNTIME_STATE.json").get("state") == "CLOSED"
        service = validate_service(workspace, harness, runtime_closed=closed)
        expected_service = receipt.get("public_check_service")
        if closed and isinstance(expected_service, dict):
            expected_service = {**expected_service, "status": "STOPPED"}
        if expected_service != service:
            raise ValueError("public check service changed after pre-launch setup")
    return {"receipt_path": str(receipt_path), "receipt_sha256": digest(receipt_path), "prepared_at_utc": receipt["prepared_at_utc"]}


def prepare(harness: Path, workspace: Path, run_id: str) -> dict[str, Any]:
    harness, workspace = harness.resolve(), workspace.resolve()
    sandbox_health = require_ready(harness, workspace=workspace)
    runtime = workspace / ".harness-runtime"
    if runtime.exists():
        # Only this exact run's successful preparation may be reused. Never
        # adopt an existing benchmark runtime or repeat setup in a live run.
        return validate_prepared_root(harness, workspace, run_id)
    config = read(harness / "harness-config.json")
    if Path(config.get("root_workspace", "")).resolve() != workspace:
        raise ValueError("harness configuration does not select this ROOT workspace")
    command = [sys.executable, "-B", "-m", "orchestrator_harness.operator_launch", "--json", "harness", "setup"]
    completed = subprocess.run(command, cwd=harness, capture_output=True, text=True, encoding="utf-8", timeout=120)
    try:
        result = json.loads(completed.stdout)
    except ValueError as exc:
        raise ValueError("native harness setup did not return a structured result") from exc
    if completed.returncode or not isinstance(result, dict) or result.get("ok") is not True:
        raise ValueError(f"native harness setup failed: {result}")
    hashes = check_installed(harness, workspace)
    sys.path.insert(0, str(harness))
    from orchestrator_harness.public_checks import start_service
    service = start_service(harness, workspace)
    receipt = {
        "schema": SCHEMA, "status": "PASS", "run_id": run_id,
        "harness_dir": str(harness), "workspace": str(workspace),
        "prepared_at_utc": datetime.now(timezone.utc).isoformat(),
        "setup_command": command, "setup_result": result,
        "hook_sha256": hashes,
        "harness_config_sha256": digest(harness / "harness-config.json"),
    }
    if sandbox_health is not None:
        receipt["windows_sandbox_health"] = sandbox_health
    if service is not None:
        receipt["public_check_service"] = service
    receipt_path = workspace / RECEIPT
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    with receipt_path.open("x", encoding="utf-8") as stream:
        json.dump(receipt, stream, indent=2)
        stream.write("\n")
    return validate_prepared_root(harness, workspace, run_id)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness-dir", required=True, type=Path)
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(prepare(args.harness_dir, args.workspace, args.run_id), indent=2))
        return 0
    except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as exc:
        print(f"ROOT pre-launch setup failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
