"""Prepare one coding lane before its controller is launched.

The bootstrap process is a runner-owned preparation boundary: it creates the
declared linked worktree and all lane inputs beneath that worktree.  It never
launches a provider or performs campaign work; callers still launch the
resulting invocation through ``operator_launch`` and ``lane_controller``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any, Mapping, Sequence

from .codex_adapter import activate_codex_binding, install_codex_adapter
from .notifications import ManagerEventRouter
from .workspace_overlay import prepare_worktree


BOOTSTRAP_SCHEMA = "orchestrator-coding-lane-bootstrap/v1"
SOURCE_SNAPSHOT_SCHEMA = "source-snapshot/v1"
CODEX_EVENT_DELIVERY_SCHEMA = "orchestrator-codex-event-delivery/v1"

_RESULT_LOCATION_RULE = """## Canonical terminal result location

Create the terminal JSON only at `.agent-workspace/RESULT.json`.
Start from `.agent-workspace/RESULT_TEMPLATE.json`; never write a bare
`RESULT.json` at the worktree or experiment root.

Terminal outcomes: `PASS`, `FAIL`, or `BLOCKED`.
Check outcomes: `PASS`, `FAIL`, `SKIP`, or `NOT_RUN`.
Describe incomplete or indeterminate evidence in a summary; use terminal
`BLOCKED` and a check `SKIP` or `NOT_RUN` when the required proof is unavailable.
"""
_LANE_ID = re.compile(r"LANE-[A-Z][A-Z0-9-]*-\d{2}$")


def _string(value: Mapping[str, Any], field: str) -> str:
    item = value.get(field)
    if not isinstance(item, str) or not item.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return item.strip()


def _path(value: Mapping[str, Any], field: str) -> Path:
    return Path(_string(value, field)).expanduser().resolve(strict=False)


def _single_name(value: Mapping[str, Any], field: str) -> str:
    name = _string(value, field)
    if Path(name).name != name or name in {".", ".."}:
        raise ValueError(f"{field} must be one path segment")
    return name


def _write_new(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(content)


def _write_new_json(path: Path, value: Mapping[str, Any]) -> None:
    _write_new(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def _run_git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), *args],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        shell=False,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ValueError(f"git {' '.join(args)} failed: {detail}")
    return completed.stdout.strip()


def _exclude_lane_runtime_files(worktree: Path) -> None:
    """Keep runner-owned lane control files out of product-worktree status."""

    exclude = Path(_run_git(worktree, "rev-parse", "--git-path", "info/exclude"))
    if not exclude.is_absolute():
        exclude = worktree / exclude
    exclude.parent.mkdir(parents=True, exist_ok=True)
    entries = (
        "# orchestrator harness lane runtime\n",
        "/.agent-runtime/\n",
        "/.agent-workspace/\n",
        "/.agent/\n",
        "/.claude/\n",
        "/.codex/\n",
    )
    with exclude.open("a", encoding="utf-8", newline="\n") as handle:
        handle.writelines(entries)


def _inside(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _snapshot_excludes(value: Mapping[str, Any]) -> tuple[Path, ...]:
    entries = value.get("exclude_paths", [])
    if not isinstance(entries, list) or not all(
        isinstance(item, str) and item.strip() for item in entries
    ):
        raise ValueError("source_snapshot.exclude_paths must be a string list")
    excludes: list[Path] = []
    for item in entries:
        relative = Path(item)
        if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
            raise ValueError("source_snapshot.exclude_paths must be relative descendant paths")
        excludes.append(relative)
    return tuple(excludes)


def _is_excluded(relative: Path, excludes: tuple[Path, ...]) -> bool:
    return any(relative == excluded or excluded in relative.parents for excluded in excludes)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _source_snapshot_request(
    manifest: Mapping[str, Any],
    source_repository_root: Path,
    workflow_role: str,
    phase: str,
    exclusive_resources: Sequence[str],
    resource_manifest: Mapping[str, Any],
) -> tuple[Path, tuple[Path, ...], Path] | None:
    value = manifest.get("source_snapshot")
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError("source_snapshot must be an object")
    if workflow_role != "ROLE_CHECKER" or not phase.endswith("-READINESS"):
        raise ValueError("source_snapshot is limited to ROLE_CHECKER readiness lanes")
    if exclusive_resources or resource_manifest.get("board_tokens") != []:
        raise ValueError("source_snapshot requires a board-free resource manifest")
    source = _path(value, "source_root")
    allowed_root = (
        _path(value, "allowed_root")
        if "allowed_root" in value
        else source_repository_root
    )
    if not source.is_dir():
        raise ValueError("source_snapshot.source_root must name an existing directory")
    if not allowed_root.is_dir():
        raise ValueError("source_snapshot.allowed_root must name an existing directory")
    if not _inside(source, allowed_root):
        raise ValueError("source_snapshot.source_root must be below source_snapshot.allowed_root")
    excludes = _snapshot_excludes(value)
    for item in source.rglob("*"):
        relative = item.relative_to(source)
        if _is_excluded(relative, excludes):
            continue
        if item.is_symlink():
            raise ValueError("source_snapshot.source_root must not contain symlinks")
        if not item.is_dir() and not item.is_file():
            raise ValueError("source_snapshot.source_root contains an unsupported entry")
    return source, excludes, allowed_root


def _has_native_command_hook(value: Any) -> bool:
    """Return whether one cached Codex hook entry runs a native command."""

    if not isinstance(value, Mapping):
        return False
    hooks = value.get("hooks")
    if not isinstance(hooks, list):
        return False
    return any(
        isinstance(hook, Mapping)
        and hook.get("type") == "command"
        and isinstance(hook.get("command"), str)
        and hook["command"].strip()
        for hook in hooks
    )


def _executor_overlay_cache(
    manifest: Mapping[str, Any], workflow_role: str
) -> Path | None:
    """Require a hook-enabled super-cache for executor lanes before mutation."""

    cache_value = manifest.get("overlay_cache")
    if cache_value is None:
        if workflow_role == "ROLE_EXECUTOR":
            raise ValueError("ROLE_EXECUTOR requires overlay_cache")
        return None
    if not isinstance(cache_value, str) or not cache_value.strip():
        raise ValueError("overlay_cache must be an omitted or non-empty path string")
    overlay_cache = Path(cache_value).expanduser().resolve(strict=False)
    if workflow_role != "ROLE_EXECUTOR":
        return overlay_cache
    if not overlay_cache.is_dir():
        raise ValueError("ROLE_EXECUTOR overlay_cache must name an existing directory")

    config_path = overlay_cache / ".codex" / "config.toml"
    if not config_path.is_file():
        raise ValueError("ROLE_EXECUTOR super-cache must include .codex/config.toml")
    try:
        config = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ValueError("ROLE_EXECUTOR super-cache has an invalid .codex/config.toml") from exc
    features = config.get("features")
    if not isinstance(features, Mapping) or features.get("hooks") is not True:
        raise ValueError("super-cache must enable native Codex hooks")

    hooks_path = overlay_cache / ".codex" / "hooks.json"
    if not hooks_path.is_file():
        raise ValueError("ROLE_EXECUTOR super-cache must include .codex/hooks.json")
    try:
        hook_document = json.loads(hooks_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("ROLE_EXECUTOR super-cache has an invalid .codex/hooks.json") from exc
    hooks = hook_document.get("hooks") if isinstance(hook_document, Mapping) else None
    if not isinstance(hooks, Mapping):
        raise ValueError("ROLE_EXECUTOR super-cache hooks.json must contain hooks")
    for event_name in ("SessionStart", "PreToolUse", "Stop"):
        entries = hooks.get(event_name)
        if not isinstance(entries, list) or not any(
            _has_native_command_hook(entry) for entry in entries
        ):
            raise ValueError(
                f"ROLE_EXECUTOR super-cache must provide a native {event_name} hook"
            )
    return overlay_cache


def _event_delivery_request(
    manifest: Mapping[str, Any], runtime_root: Path
) -> dict[str, Any] | None:
    """Validate one exact manager binding before creating the lane worktree."""

    value = manifest.get("event_delivery")
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError("event_delivery must be an object")
    allowed = {
        "schema",
        "queue_root",
        "run_id",
        "queue_id",
        "manager_session_id",
        "manager_thread_id",
        "registration_id",
        "manager_invocation_id",
        "registration_generation",
    }
    extras = sorted(set(value) - allowed)
    if extras:
        raise ValueError(f"event_delivery has unsupported fields: {', '.join(extras)}")
    if _string(value, "schema") != CODEX_EVENT_DELIVERY_SCHEMA:
        raise ValueError(
            f"event_delivery.schema must be {CODEX_EVENT_DELIVERY_SCHEMA}"
        )
    queue_root = _path(value, "queue_root")
    if not _inside(queue_root, runtime_root):
        raise ValueError("event_delivery.queue_root must be below runtime_root")
    registration_generation = value.get("registration_generation")
    if (
        not isinstance(registration_generation, int)
        or isinstance(registration_generation, bool)
        or registration_generation < 0
    ):
        raise ValueError(
            "event_delivery.registration_generation must be a non-negative integer"
        )
    return {
        "queue_root": queue_root,
        "run_id": _string(value, "run_id"),
        "queue_id": _string(value, "queue_id"),
        "manager_session_id": _string(value, "manager_session_id"),
        "manager_thread_id": _string(value, "manager_thread_id"),
        "registration_id": _string(value, "registration_id"),
        "manager_invocation_id": _string(value, "manager_invocation_id"),
        "registration_generation": registration_generation,
    }


def _activate_event_delivery(
    delivery: Mapping[str, Any] | None,
    *,
    worktree: Path,
    runtime_root: Path,
    lane_id: str,
) -> dict[str, Any] | None:
    """Install and bind Codex hooks only in the runner-created lane worktree."""

    if delivery is None:
        return None
    queue_root = delivery["queue_root"]
    if not isinstance(queue_root, Path):
        raise ValueError("event_delivery queue root is invalid")
    coordinator_root = runtime_root / "codex-coordinators" / lane_id
    coordinator_root.parent.mkdir(parents=True, exist_ok=True)
    install_codex_adapter(worktree)
    router = ManagerEventRouter(
        queue_root,
        run_id=str(delivery["run_id"]),
        queue_id=str(delivery["queue_id"]),
        manager_session_id=str(delivery["manager_session_id"]),
        manager_thread_id=str(delivery["manager_thread_id"]),
        registration_id=str(delivery["registration_id"]),
        manager_invocation_id=str(delivery["manager_invocation_id"]),
        registration_generation=int(delivery["registration_generation"]),
    )
    activation = activate_codex_binding(
        worktree, router, coordinator_root=coordinator_root
    )
    binding = activation["binding"]
    return {
        "schema": CODEX_EVENT_DELIVERY_SCHEMA,
        "queue_root": str(queue_root),
        "coordinator_root": str(coordinator_root),
        "run_id": binding["run_id"],
        "queue_id": binding["queue_id"],
        "manager_session_id": binding["manager_session_id"],
        "manager_thread_id": binding["manager_thread_id"],
        "registration_id": binding["registration_id"],
        "manager_invocation_id": binding["manager_invocation_id"],
        "registration_generation": binding["registration_generation"],
    }


def _snapshot_source(
    source: Path, excludes: tuple[Path, ...], allowed_root: Path, workspace: Path
) -> dict[str, Any]:
    """Copy one prevalidated untracked candidate into the ignored lane workspace."""

    destination = workspace / "source-snapshot"
    destination.mkdir()

    entries: list[dict[str, Any]] = []
    for item in sorted(source.rglob("*"), key=lambda path: path.as_posix()):
        relative = item.relative_to(source)
        if _is_excluded(relative, excludes):
            continue
        target = destination / relative
        if item.is_dir():
            target.mkdir()
            continue
        shutil.copy2(item, target)
        entries.append(
            {
                "path": relative.as_posix(),
                "bytes": item.stat().st_size,
                "sha256": _file_sha256(target),
            }
        )

    digest = hashlib.sha256()
    for entry in entries:
        digest.update(
            f"{entry['path']}\0{entry['bytes']}\0{entry['sha256']}\n".encode("utf-8")
        )
    snapshot = {
        "schema": SOURCE_SNAPSHOT_SCHEMA,
        "source_root": str(source),
        "allowed_root": str(allowed_root),
        "snapshot_root": str(destination),
        "manifest": str(workspace / "source-snapshot.manifest.json"),
        "exclude_paths": [path.as_posix() for path in excludes],
        "file_count": len(entries),
        "bytes": sum(entry["bytes"] for entry in entries),
        "sha256": digest.hexdigest(),
        "files": entries,
    }
    _write_new_json(workspace / "source-snapshot.manifest.json", snapshot)
    return {key: value for key, value in snapshot.items() if key != "files"}


def _launch_options(value: Mapping[str, Any]) -> dict[str, Any]:
    options = value.get("launch_options")
    if not isinstance(options, Mapping):
        raise ValueError("launch_options must be an object")
    command = options.get("command")
    if (
        not isinstance(command, list)
        or not command
        or not all(isinstance(item, str) and item for item in command)
    ):
        raise ValueError("codex.command must be a non-empty string list")
    return {
        "command": list(command),
        "sandbox": _string(options, "sandbox"),
        "approval_policy": _string(options, "approval_policy"),
    }


def _mapped_codex_selection(
    manifest: Mapping[str, Any], workflow_role: str, lane_id: str
) -> dict[str, Any]:
    mapping_path = _path(manifest, "mapping")
    try:
        mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read canonical role mapping: {exc}") from exc
    if (
        not isinstance(mapping, dict)
        or mapping.get("schema") != "firmware-role-agent-mapping/v1"
        or not isinstance(mapping.get("roles"), dict)
    ):
        raise ValueError("canonical role mapping is invalid")
    selection = mapping["roles"].get(workflow_role)
    if not isinstance(selection, dict):
        raise ValueError(f"canonical role mapping has no {workflow_role} selection")
    lane_slots = mapping.get("lane_slots", {})
    if not isinstance(lane_slots, dict):
        raise ValueError("canonical role mapping lane_slots must be an object")
    lane = lane_slots.get(lane_id)
    if workflow_role == "ROLE_EXECUTOR":
        if not isinstance(lane, dict) or lane.get("role") != workflow_role:
            raise ValueError("executor lane is not bound to ROLE_EXECUTOR in the canonical mapping")
        selection = lane.get("selection", selection)
    elif lane is not None:
        raise ValueError("only ROLE_EXECUTOR may use a declared LANE-EXEC slot")
    if not isinstance(selection, dict) or selection.get("provider") != "codex":
        raise ValueError("mapped workflow role must select the Codex provider")
    overrides = selection.get("config_overrides", [])
    if not isinstance(overrides, list) or not all(
        isinstance(item, str) and item for item in overrides
    ):
        raise ValueError("mapped config_overrides must be a string list")
    return {
        "model": _string(selection, "model"),
        "reasoning_effort": _string(selection, "reasoning_effort"),
        "service_tier": _string(selection, "service_tier"),
        "config_overrides": list(overrides),
    }


def _load_manifest(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read bootstrap manifest: {exc}") from exc
    if not isinstance(value, dict) or value.get("schema") != BOOTSTRAP_SCHEMA:
        raise ValueError(f"manifest schema must be {BOOTSTRAP_SCHEMA}")
    return value


def bootstrap_coding_lane(manifest_path: str | Path) -> dict[str, Any]:
    """Create one prepared linked worktree and controller-valid invocation."""

    manifest = _load_manifest(Path(manifest_path))
    experiment_root = _path(manifest, "experiment_root")
    source_root = _path(manifest, "source_repository_root")
    runtime_root = _path(manifest, "runtime_root")
    workflow_role = _string(manifest, "workflow_role")
    lane_id = _string(manifest, "lane_id")
    if not _LANE_ID.fullmatch(lane_id):
        raise ValueError("lane_id must match LANE-<ROLE>-NN")
    if workflow_role == "ROLE_EXECUTOR" and not re.fullmatch(r"LANE-EXEC-\d{2}", lane_id):
        raise ValueError("ROLE_EXECUTOR lane_id must match LANE-EXEC-NN")
    if workflow_role != "ROLE_EXECUTOR" and re.fullmatch(r"LANE-EXEC-\d{2}", lane_id):
        raise ValueError("only ROLE_EXECUTOR may use a LANE-EXEC slot")
    worktree_name = _single_name(manifest, "worktree_name")
    branch = _string(manifest, "branch")
    phase = _string(manifest, "phase")
    base_commit = _string(manifest, "base_commit").lower()
    if not re.fullmatch(r"[0-9a-f]{40}", base_commit):
        raise ValueError("base_commit must be a full lowercase Git SHA-1")
    if not source_root.is_dir():
        raise ValueError("source_repository_root must name an existing directory")
    worktrees_root = experiment_root / "worktrees"
    worktree = (worktrees_root / worktree_name).resolve(strict=False)
    if not _inside(worktree, worktrees_root.resolve(strict=False)):
        raise ValueError("worktree must be below experiment_root/worktrees")
    if worktree.exists():
        raise ValueError("declared worktree must not already exist")
    _run_git(source_root, "rev-parse", "--verify", f"{base_commit}^{{commit}}")
    _run_git(source_root, "check-ref-format", "--branch", branch)

    task_card_name = _single_name(manifest, "task_card_name")
    task_card_markdown = _string(manifest, "task_card_markdown") + "\n"
    resource_manifest_name = _single_name(manifest, "resource_manifest_name")
    resource_manifest = manifest.get("resource_manifest")
    if not isinstance(resource_manifest, Mapping):
        raise ValueError("resource_manifest must be an object")
    prompt = _string(manifest, "prompt") + "\n"
    selection = _mapped_codex_selection(manifest, workflow_role, lane_id)
    launch_options = _launch_options(manifest)
    resources = manifest.get("exclusive_resources", [])
    if not isinstance(resources, list) or not all(
        isinstance(item, str) and item for item in resources
    ):
        raise ValueError("exclusive_resources must be a string list")
    overlay_cache = _executor_overlay_cache(manifest, workflow_role)
    event_delivery = _event_delivery_request(manifest, runtime_root)
    source_snapshot_request = _source_snapshot_request(
        manifest,
        source_root,
        workflow_role,
        phase,
        resources,
        resource_manifest,
    )

    runtime_root.mkdir(parents=True, exist_ok=True)
    worktrees_root.mkdir(parents=True, exist_ok=True)
    dispatch_root = experiment_root / "dispatch"
    _write_new(dispatch_root / task_card_name, task_card_markdown)
    _write_new_json(dispatch_root / resource_manifest_name, resource_manifest)

    _run_git(source_root, "worktree", "add", "-b", branch, str(worktree), base_commit)
    workspace = worktree / ".agent-workspace"
    workspace.mkdir()
    if overlay_cache is None:
        overlay_cache = runtime_root / "empty-overlay-cache"
        overlay_cache.mkdir(exist_ok=True)
    receipt_path = workspace / "overlay-receipt.json"
    prepare_worktree(
        super_cache=overlay_cache,
        target_worktree=worktree,
        role="subagent",
        receipt_path=receipt_path,
    )
    _exclude_lane_runtime_files(worktree)
    activated_event_delivery = _activate_event_delivery(
        event_delivery,
        worktree=worktree,
        runtime_root=runtime_root,
        lane_id=lane_id,
    )
    source_snapshot = None
    if source_snapshot_request is not None:
        snapshot_source_root, snapshot_excludes, snapshot_allowed_root = source_snapshot_request
        source_snapshot = _snapshot_source(
            snapshot_source_root, snapshot_excludes, snapshot_allowed_root, workspace
        )

    prompt_path = workspace / "worker-prompt.md"
    snapshot_rule = ""
    if source_snapshot is not None:
        snapshot_rule = (
            "\n\n## Frozen source snapshot\n\n"
            "Use only `.agent-workspace/source-snapshot` as the source candidate. "
            "Its complete content manifest is `.agent-workspace/source-snapshot.manifest.json`; "
            f"the frozen content SHA-256 is `{source_snapshot['sha256']}`.\n"
        )
    _write_new(
        prompt_path, f"{prompt.rstrip()}{snapshot_rule}\n\n{_RESULT_LOCATION_RULE}"
    )
    common_dir = _run_git(worktree, "rev-parse", "--git-common-dir")
    common_path = Path(common_dir)
    if not common_path.is_absolute():
        common_path = worktree / common_path
    invocation_path = workspace / "invocation.json"
    worker_invocation_id = _string(manifest, "worker_invocation_id")
    current_commit = _run_git(worktree, "rev-parse", "HEAD")
    _write_new_json(
        workspace / "RESULT_TEMPLATE.json",
        {
            "schema": "orchestrator-lane-result/v1",
            "lane_id": lane_id,
            "worker_invocation_id": worker_invocation_id,
            "branch": branch,
            "commit": current_commit,
            "outcome": "BLOCKED",
            "summary": "Replace this template with the truthful terminal lane result.",
            "checks": [
                {
                    "name": "replace with a completed verification",
                    "command": "replace with the exact command or observation",
                    "outcome": "NOT_RUN",
                    "summary": "replace with the truthful result summary",
                }
            ],
        },
    )
    invocation = {
        "schema": "orchestrator-coding-invocation/v1",
        "action": "start",
        "runtime_root": str(runtime_root),
        "resource_lock_root": str(runtime_root / "coding-resource-locks"),
        "run_root": str(worktree),
        "repository": {
            "common_dir": str(common_path.resolve()),
            "worktree_root": str(worktree),
            "branch": branch,
            "base_commit": base_commit,
            "merge_inputs": [],
        },
        "prompt_path": str(prompt_path),
        "prompt_sha256": hashlib.sha256(prompt_path.read_bytes()).hexdigest(),
        "output_paths": {
            "status": str(workspace / "controller.status.json"),
            "jsonl": str(workspace / "codex.jsonl"),
            "stderr": str(workspace / "codex.stderr.log"),
            "last_message": str(workspace / "last-message.txt"),
        },
        "event_log_path": str(runtime_root / "LANE_EVENTS.jsonl"),
        "lane_id": lane_id,
        "worker_invocation_id": worker_invocation_id,
        "task": _string(manifest, "task"),
        "phase": phase,
        "exclusive_resources": list(resources),
        "overlay_receipt": str(receipt_path),
        "codex": {**selection, **launch_options},
    }
    _write_new_json(invocation_path, invocation)
    return {
        "schema": BOOTSTRAP_SCHEMA,
        "status": "prepared",
        "lane_id": lane_id,
        "workflow_role": workflow_role,
        "worktree_root": str(worktree),
        "task_card": str(dispatch_root / task_card_name),
        "resource_manifest": str(dispatch_root / resource_manifest_name),
        "overlay_receipt": str(receipt_path),
        "invocation": str(invocation_path),
        **({"source_snapshot": source_snapshot} if source_snapshot is not None else {}),
        **({"event_delivery": activated_event_delivery} if activated_event_delivery is not None else {}),
    }


def cleanup_coding_lane(manifest_path: str | Path) -> dict[str, Any]:
    """Remove one clean, runner-created worktree named by its bootstrap manifest."""

    manifest = _load_manifest(Path(manifest_path))
    source_root = _path(manifest, "source_repository_root")
    experiment_root = _path(manifest, "experiment_root")
    worktrees_root = experiment_root / "worktrees"
    worktree_name = _single_name(manifest, "worktree_name")
    worktree = (worktrees_root / worktree_name).resolve(strict=False)
    if not _inside(worktree, worktrees_root.resolve(strict=False)):
        raise ValueError("worktree must be below experiment_root/worktrees")
    if not source_root.is_dir() or not worktree.is_dir():
        raise ValueError("cleanup requires an existing source repository and worktree")
    status = _run_git(
        worktree, "status", "--porcelain=v1", "--untracked-files=all", "--", "."
    )
    if status:
        raise ValueError("cleanup refuses a worktree with tracked or untracked changes")
    _run_git(source_root, "worktree", "remove", str(worktree))
    if worktree.exists():
        raise ValueError("git worktree remove did not remove the declared worktree")
    return {
        "schema": BOOTSTRAP_SCHEMA,
        "status": "removed",
        "lane_id": _string(manifest, "lane_id"),
        "worktree_root": str(worktree),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare one firmware-v2 coding lane before controller launch"
    )
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--cleanup", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = cleanup_coding_lane(args.manifest) if args.cleanup else bootstrap_coding_lane(args.manifest)
        _write_new_json(args.result, result)
    except (OSError, shutil.Error, ValueError) as exc:
        error = {"schema": BOOTSTRAP_SCHEMA, "status": "failed", "error": str(exc)}
        try:
            _write_new_json(args.result, error)
        except (OSError, ValueError):
            pass
        print(json.dumps(error, sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
