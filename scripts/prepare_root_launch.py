"""Explicitly prepare provider ROOT hooks and the product runtime before launch."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from orchestrator_harness.windows_sandbox_preflight import require_ready
from orchestrator_harness.config import compute_config_identity, load_config, load_resource_manifest, path_identity
from orchestrator_harness.core import canonical_json, new_id
from orchestrator_harness.mutation import (
    _DirectoryAnchor, _publish, TargetState, capture_target, ensure_directory_path, read_bytes, replace,
)
from orchestrator_harness.records import RecordLock, runtime_admission_lock
from orchestrator_harness.root_hook_wrapper import _read_binding
from orchestrator_harness.root_session import publish_root_session, read_root_session
from orchestrator_harness.setup import read_monitor_record, read_runtime_state, require_monitor_ready

SCHEMA = "root-prelaunch-setup/v2"
LEGACY_SCHEMA = "root-prelaunch-setup/v1"
RECEIPT = ".agent-workspace/ROOT_PRELAUNCH_SETUP.json"
PROVIDERS = {"codex": ".codex", "claude-code": ".claude", "qwen-code": ".qwen"}
MONITOR_FIELDS = ("startup_id", "pid", "creation_time", "config_identity", "ready_at")
HOOK_FILES = (
    ".codex/hooks.json", ".codex/orchestrator-harness-binding.json",
    ".codex/hooks/orchestrator_harness_post_tool_use.py",
    ".codex/hooks/orchestrator_harness_stop.py",
)
HARNESS_INPUTS = (
    "harness-config.json", "resource-manifest.json",
    "orchestrator_harness/root_hook_dispatch.py", "orchestrator_harness/root_hook_wrapper.py",
    "orchestrator_harness/root_session.py", "orchestrator_harness/setup.py",
    "scripts/collect_codex_usage.py", "scripts/prepare_root_launch.py",
    "local-config/public-check-policy.json",
)


def _bytes(path: Path) -> bytes:
    target = path.absolute()
    return read_bytes(target.parent, target.name)


def read(path: Path) -> dict[str, Any]:
    value = json.loads(_bytes(path).decode("utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return value


def digest(path: Path) -> str:
    return hashlib.sha256(_bytes(path)).hexdigest()


def _configured(harness: Path, workspace: Path, provider_id: str):
    if provider_id not in PROVIDERS:
        raise ValueError("ROOT preparation requires a registered provider")
    config = load_config(harness.absolute())
    if config.profile != "managed" or path_identity(config.manager_workspace) != path_identity(workspace.absolute()):
        raise ValueError("harness configuration does not select this managed ROOT workspace")
    return config


def _absolute(path: Path) -> Path:
    """Normalize relative spellings without following a redirected ancestor."""
    return Path(os.path.abspath(path))


def hook_files(provider_id: str) -> tuple[str, ...]:
    dotdir = PROVIDERS[provider_id]
    files = (f"{dotdir}/{'hooks.json' if provider_id == 'codex' else 'settings.json'}",
             f"{dotdir}/orchestrator-harness-binding.json",
             f"{dotdir}/hooks/orchestrator_harness_post_tool_use.py",
             f"{dotdir}/hooks/orchestrator_harness_stop.py")
    return files + ((f"{dotdir}/hooks/orchestrator_harness_notification.py",) if provider_id == "qwen-code" else ())


def configuration_hashes(harness: Path, workspace: Path, *, provider_id: str = "codex") -> dict[str, str | None]:
    inputs = {relative: harness / relative for relative in HARNESS_INPUTS}
    binding = f"orchestrator_harness/provider_adapters/{provider_id}/launcher_binding.py"
    inputs[binding] = harness / binding
    if provider_id == "codex":
        inputs["ROOT/.codex/config.toml"] = workspace / ".codex/config.toml"
    # Capture absence too; an unsafe/redirected existing input never becomes None.
    hashes = {}
    for name, path in inputs.items():
        try:
            state = capture_target(path.absolute().parent, path.name)
        except FileNotFoundError:
            state = TargetState.absent()
        if not state.present and name != "local-config/public-check-policy.json":
            raise ValueError(f"required ROOT configuration input is missing: {path}")
        if state.present and state.kind != "file":
            raise ValueError(f"configuration input is not a plain file: {path}")
        hashes[name] = state.content_sha256 if state.present else None
    return hashes


def workspace_identity(workspace: Path) -> dict[str, Any]:
    workspace = workspace.absolute()
    with _DirectoryAnchor(workspace, workspace) as anchor:
        anchor.assert_stable()
        device, inode = anchor.root_identity
        git = capture_target(workspace, ".git")
        git_identity: Any = None
        if git.present:
            if git.kind == "file":
                git_identity = {"pointer_sha256": git.content_sha256}
            elif git.kind == "directory":
                with _DirectoryAnchor(workspace, workspace / ".git") as git_anchor:
                    git_anchor.assert_stable()
                    git_identity = {"device": git_anchor.parent_identity[0], "inode": git_anchor.parent_identity[1]}
            else:
                raise ValueError("ROOT Git binding is not a plain file or directory")
        anchor.assert_stable()
        return {"device": device, "inode": inode, "git": git_identity}


def _monitor_owner(config, *, allow_closed: bool = False) -> dict[str, Any]:
    with runtime_admission_lock(config.runtime_root):
        state = read_runtime_state(config.runtime_root)
        if state and state.get("state") == "OPEN":
            identity = compute_config_identity(config, load_resource_manifest(config.harness_root))
            monitor = require_monitor_ready(config.runtime_root, identity)
        elif state and state.get("state") == "CLOSED" and allow_closed:
            from orchestrator_harness.shutdown import prove_runtime_closed
            try:
                prove_runtime_closed(config.runtime_root, config.root_workspace, config.harness_root)
            except (RuntimeError, OSError, ValueError) as exc:
                raise ValueError(f"ROOT resume cannot prove CLOSED cleanup: {exc}") from exc
            monitor = read_monitor_record(config.runtime_root)
            if not monitor or monitor.get("initialized") is not True:
                raise ValueError("ROOT resume has no previously initialized monitor owner")
        else:
            raise ValueError("ROOT's prepared harness runtime is not OPEN")
        return {field: monitor.get(field) for field in MONITOR_FIELDS}


def check_installed(harness: Path, workspace: Path, *, allow_closed: bool = False,
                    provider_id: str = "codex") -> dict[str, str]:
    config = _configured(harness, workspace, provider_id)
    _monitor_owner(config, allow_closed=allow_closed)
    files = hook_files(provider_id)
    _read_binding(workspace / files[1], provider_id)
    hooks = read(workspace / files[0]).get("hooks", {})
    dotdir = PROVIDERS[provider_id]
    events = [("PostToolUse", "orchestrator_harness_post_tool_use.py"),
              ("Stop", "orchestrator_harness_stop.py")]
    if provider_id == "qwen-code":
        events.append(("Notification", "orchestrator_harness_notification.py"))
    for event, filename in events:
        expected = {"hooks": [{"type": "command", "command": f"python {dotdir}/hooks/{filename}"}]}
        if event == "PostToolUse":
            expected["matcher"] = ".*"
        groups = hooks.get(event, []) if isinstance(hooks, dict) else []
        if not isinstance(groups, list) or expected not in groups:
            raise ValueError(f"ROOT's native {event} hook is missing")
    if provider_id == "codex":
        codex_config = tomllib.loads(_bytes(workspace / ".codex/config.toml").decode("utf-8-sig"))
        if codex_config.get("features", {}).get("hooks") is not True:
            raise ValueError("ROOT native hooks must be explicitly enabled in .codex/config.toml")
    return {relative: digest(workspace / relative) for relative in files}


def _validated_receipt(harness: Path, workspace: Path, run_id: str, *,
                       resuming: bool, provider_id: str, compare_monitor: bool = True):
    config = _configured(harness, workspace, provider_id)
    receipt_path = workspace / RECEIPT
    try:
        receipt_state = capture_target(workspace, RECEIPT)
    except FileNotFoundError as exc:
        raise ValueError("ROOT setup must finish before CLI launch; pre-launch setup receipt is missing") from exc
    if not receipt_state.present:
        raise ValueError("ROOT setup must finish before CLI launch; pre-launch setup receipt is missing")
    if receipt_state.kind != "file":
        raise ValueError("pre-launch setup receipt is not a plain file")
    receipt = json.loads(receipt_state.content.decode("utf-8-sig"))
    if not isinstance(receipt, dict):
        raise ValueError("pre-launch setup receipt is not a JSON object")
    if receipt.get("schema") == LEGACY_SCHEMA:
        raise ValueError("legacy v1 pre-launch receipt requires explicit ROOT preparation")
    expected = {"schema": SCHEMA, "run_id": run_id, "harness_dir": str(harness),
                "workspace": str(workspace), "provider_id": provider_id,
                "config_identity": compute_config_identity(config, load_resource_manifest(harness)),
                "manager_workspace": path_identity(config.manager_workspace),
                "root_workspace": path_identity(config.root_workspace), "runtime_root": path_identity(config.runtime_root)}
    if any(receipt.get(key) != value for key, value in expected.items()):
        raise ValueError("pre-launch setup receipt belongs to a different ROOT run or configuration")
    if receipt.get("status") != "PASS":
        raise ValueError("pre-launch harness setup did not pass")
    if receipt.get("workspace_identity") != workspace_identity(workspace):
        raise ValueError("ROOT workspace identity changed after pre-launch setup")
    if receipt.get("product_workspace_identity") != workspace_identity(config.root_workspace):
        raise ValueError("product workspace identity changed after pre-launch setup")
    if receipt.get("hook_sha256") != check_installed(harness, workspace, allow_closed=resuming, provider_id=provider_id):
        raise ValueError("ROOT hook files changed after pre-launch setup")
    if receipt.get("harness_config_sha256") != digest(harness / "harness-config.json"):
        raise ValueError("harness configuration changed after pre-launch setup")
    health = require_ready(harness, workspace=config.root_workspace) if provider_id == "codex" else None
    if receipt.get("windows_sandbox_health") != health:
        raise ValueError("Windows sandbox health changed after pre-launch setup")
    from orchestrator_harness.public_checks import policy, validate_service
    configured_policy = policy(harness)
    if (configured_policy is not None) != (receipt.get("public_check_service") is not None):
        raise ValueError("public check service changed after pre-launch setup (enabled/disabled policy)")
    if configured_policy is not None:
        closed = resuming and read_runtime_state(config.runtime_root).get("state") == "CLOSED"
        service = validate_service(config.root_workspace, harness, runtime_closed=closed)
        expected_service = receipt.get("public_check_service")
        if closed and isinstance(expected_service, dict):
            expected_service = {**expected_service, "status": "STOPPED"}
        if expected_service != service:
            raise ValueError("public check service changed after pre-launch setup")
    if receipt.get("configuration_sha256") != configuration_hashes(harness, workspace, provider_id=provider_id):
        raise ValueError("ROOT or provider configuration changed after pre-launch setup")
    session = read_root_session(config)
    if (session["run_id"] != run_id or session["provider_id"] != provider_id
            or session["session_id"] != receipt.get("root_session_id")):
        raise ValueError("pre-launch ROOT session differs from the prepared binding")
    owner = _monitor_owner(config, allow_closed=resuming)
    if compare_monitor and receipt.get("monitor") != owner:
        raise ValueError("prepared monitor owner changed; explicitly prepare ROOT again")
    if capture_target(workspace, RECEIPT) != receipt_state:
        raise ValueError("pre-launch setup receipt changed during validation")
    return receipt, session, owner, receipt_state


def validate_prepared_root(harness: Path, workspace: Path, run_id: str, *,
                           resuming: bool = False, provider_id: str = "codex") -> dict[str, Any]:
    harness, workspace = _absolute(harness), _absolute(workspace)
    config = _configured(harness, workspace, provider_id)
    with runtime_admission_lock(config.runtime_root):
        receipt, session, _owner, receipt_state = _validated_receipt(harness, workspace, run_id,
                                                   resuming=resuming, provider_id=provider_id)
        receipt_path = workspace / RECEIPT
        return {"receipt_path": str(receipt_path), "receipt_sha256": receipt_state.content_sha256,
                "prepared_at_utc": receipt["prepared_at_utc"], "provider_id": provider_id,
                "root_session_id": session["session_id"], "native_session_id": session["native_session_id"],
                "config_identity": session["config_identity"], "manager_workspace": str(config.manager_workspace),
                "root_workspace": str(config.root_workspace), "runtime_root": str(config.runtime_root)}


def prepare(harness: Path, workspace: Path, run_id: str, *, provider_id: str = "codex") -> dict[str, Any]:
    harness, workspace = _absolute(harness), _absolute(workspace)
    config = _configured(harness, workspace, provider_id)
    original_identity = compute_config_identity(config, load_resource_manifest(harness))
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("ROOT preparation requires a nonempty run ID")
    health = require_ready(harness, workspace=config.root_workspace) if provider_id == "codex" else None
    receipt_path = workspace / RECEIPT
    prior = capture_target(workspace, RECEIPT) if receipt_path.parent.is_dir() else None
    previous = None
    if prior is not None and prior.present:
        previous = json.loads(prior.content.decode("utf-8-sig"))
        if not isinstance(previous, dict) or any(previous.get(key) != value for key, value in
                {"run_id": run_id, "harness_dir": str(harness), "workspace": str(workspace)}.items()):
            raise ValueError("pre-launch setup receipt belongs to a different ROOT run")
        if previous.get("schema") == SCHEMA:
            with runtime_admission_lock(config.runtime_root):
                _receipt, _session, owner, _state = _validated_receipt(harness, workspace, run_id,
                                            resuming=False, provider_id=provider_id, compare_monitor=False)
            if previous.get("monitor") == owner:
                return validate_prepared_root(harness, workspace, run_id, provider_id=provider_id)
        elif previous.get("schema") != LEGACY_SCHEMA:
            raise ValueError("unknown pre-launch setup receipt; preserve it before preparation")
    elif config.runtime_root.exists():
        # K04 will admit matching interrupted birth intents; unrelated runtime
        # directories are never adopted merely because their configuration fits.
        return validate_prepared_root(harness, workspace, run_id, provider_id=provider_id)

    command = [sys.executable, "-B", "-m", "orchestrator_harness.operator_launch", "--json", "harness", "setup"]
    if previous is None:
        completed = subprocess.run(command, cwd=harness, capture_output=True, text=True, encoding="utf-8", timeout=120)
        try:
            result = json.loads(completed.stdout)
        except ValueError as exc:
            raise ValueError("native harness setup did not return a structured result") from exc
        if completed.returncode or not isinstance(result, dict) or result.get("ok") is not True:
            raise ValueError(f"native harness setup failed: {result}")
    else:
        result = {"ok": True, "code": "PREPARATION_REVALIDATED", "summary": "explicit preparation of current installed owners"}
    hashes = check_installed(harness, workspace, provider_id=provider_id)
    from orchestrator_harness.public_checks import start_service
    service = start_service(harness, config.root_workspace)
    with runtime_admission_lock(config.runtime_root):
        current = _configured(harness, workspace, provider_id)
        identity = compute_config_identity(current, load_resource_manifest(harness))
        if identity != original_identity:
            raise ValueError("configuration changed during ROOT preparation")
        if hashes != check_installed(harness, workspace, provider_id=provider_id):
            raise ValueError("ROOT hook files changed during preparation")
        current_health = require_ready(harness, workspace=current.root_workspace) if provider_id == "codex" else None
        if current_health != health:
            raise ValueError("Windows sandbox health changed during ROOT preparation")
        from orchestrator_harness.public_checks import policy, validate_service
        if (policy(harness) is not None) != (service is not None):
            raise ValueError("public check service policy changed during ROOT preparation")
        if service is not None and validate_service(current.root_workspace, harness) != service:
            raise ValueError("public check service ownership changed during ROOT preparation")
        owner = _monitor_owner(current)
        inputs = configuration_hashes(harness, workspace, provider_id=provider_id)
        session = publish_root_session(current, run_id=run_id, provider_id=provider_id)
        receipt = {"schema": SCHEMA, "status": "PASS", "run_id": run_id,
                   "harness_dir": str(harness), "workspace": str(workspace), "provider_id": provider_id,
                   "prepared_at_utc": datetime.now(timezone.utc).isoformat(),
                   "setup_command": command, "setup_result": result, "hook_sha256": hashes,
                   "harness_config_sha256": digest(harness / "harness-config.json"),
                   "configuration_sha256": inputs,
                   "workspace_identity": workspace_identity(workspace),
                   "product_workspace_identity": workspace_identity(current.root_workspace),
                   "manager_workspace": path_identity(current.manager_workspace),
                   "root_workspace": path_identity(current.root_workspace), "runtime_root": path_identity(current.runtime_root),
                   "config_identity": identity, "root_session_id": session["session_id"], "monitor": owner}
        if health is not None:
            receipt["windows_sandbox_health"] = health
        if service is not None:
            receipt["public_check_service"] = service
        ensure_directory_path(receipt_path.parent)
        with RecordLock(receipt_path):
            expected = prior if prior is not None else TargetState.absent()
            if previous is not None:
                history = receipt_path.parent / "legacy-preparation"
                ensure_directory_path(history)
                archive = history / (new_id() + ".json")
                absent = capture_target(history, archive.name)
                if absent.present:
                    raise ValueError("preparation history destination already exists")
                replace(history, archive.name, prior.content, expected=absent)
                if _bytes(archive) != prior.content:
                    raise ValueError("preparation history preservation failed")
                receipt["prior_receipt"] = {"path": str(archive), "sha256": hashlib.sha256(prior.content).hexdigest()}
            _publish(workspace, RECEIPT, canonical_json(receipt) + b"\n", expected=expected,
                     expected_root_identity=(receipt["workspace_identity"]["device"], receipt["workspace_identity"]["inode"]))
    return validate_prepared_root(harness, workspace, run_id, provider_id=provider_id)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harness-dir", required=True, type=Path)
    parser.add_argument("--workspace", required=True, type=Path, help="configured manager ROOT workspace")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--provider", choices=tuple(PROVIDERS), default="codex")
    args = parser.parse_args()
    try:
        print(json.dumps(prepare(args.harness_dir, args.workspace, args.run_id, provider_id=args.provider), indent=2))
        return 0
    except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as exc:
        print(f"ROOT pre-launch setup failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
