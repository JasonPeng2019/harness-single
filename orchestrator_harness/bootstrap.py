"""``lane bootstrap``: prepare one lane (a short program, not an agent).

Creates the worktree + ``.agent-workspace``, applies the cache overlay, writes
the worker prompt/result template and the controller invocation.  Opens a new
epoch if none is active.  Does not start a provider or take a lease.

Managed bootstrap copies the active ``workspace/`` base and only the selected
provider payload, then generates the lane-specific queue/result/invocation/
records.  Plain bootstrap gets no queue helpers, hook payload, or worker
skills.  Source trees stay unchanged.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any

from .config import load_config, load_resource_manifest
from .core import content_hash, iso_utc, new_id, read_json, require_schema
from .epochs import (
    lane_record_dir,
    open_epoch,
    read_active_lanes,
    write_active_lanes,
)
from .lanes import LANE_SCHEMA, write_lane
from .records import atomic_write_json
from .task_cards import validate_task_card

INVOCATION_SCHEMA = "controller-invocation/v1"
OVERLAY_RECEIPT_SCHEMA = "overlay-receipt/v1"
LANE_INBOX_SCHEMA = "lane-inbox/v1"
HOOK_BINDING_SCHEMA = "harness-hook-binding/v1"

BOOTSTRAP_REQUEST_INVALID = "BOOTSTRAP_REQUEST_INVALID"
BOOTSTRAP_LANE_ID_IN_USE = "BOOTSTRAP_LANE_ID_IN_USE"
BOOTSTRAP_WORKTREE_EXISTS = "BOOTSTRAP_WORKTREE_EXISTS"
BOOTSTRAP_CACHE_COLLISION = "BOOTSTRAP_CACHE_COLLISION"
BOOTSTRAP_ADAPTER_MISSING = "BOOTSTRAP_ADAPTER_MISSING"
BOOTSTRAP_CACHE_MISSING = "BOOTSTRAP_CACHE_MISSING"
BOOTSTRAP_RESOURCE_UNDECLARED = "BOOTSTRAP_RESOURCE_UNDECLARED"

# Managed-only helpers carried by the workspace base; plain bootstrap omits them.
PLAIN_EXCLUDED_HELPERS = (
    ".agent-workspace/lane-queue.py",
    ".agent-workspace/manager-notify.py",
)


class BootstrapError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _read_task_card(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise BootstrapError(BOOTSTRAP_REQUEST_INVALID, f"task card missing: {path}")
    try:
        record = read_json(path)
        validate_task_card(record, path)
    except (OSError, ValueError) as exc:
        raise BootstrapError(BOOTSTRAP_REQUEST_INVALID, str(exc)) from exc
    return record


def _run_git(
    root_workspace: Path, *args: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(root_workspace), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def _rollback_bootstrap_worktree(
    root_workspace: Path, branch: str, worktree_path: Path
) -> list[str]:
    """Roll back one exact bootstrap-owned worktree and branch.

    Callers may use this only after proving the target path and branch did not
    exist before their attempt.  Cleanup remains Git-owned; this helper never
    recursively deletes a directory.
    """

    notes: list[str] = []
    removed = _run_git(
        root_workspace, "worktree", "remove", "--force", "--", str(worktree_path)
    )

    branch_ref = f"refs/heads/{branch}"
    branch_after = _run_git(
        root_workspace, "show-ref", "--verify", "--quiet", branch_ref
    )
    if branch_after.returncode == 0:
        deleted = _run_git(root_workspace, "branch", "-D", "--", branch)
        if deleted.returncode != 0:
            notes.append(
                f"attempt-created branch cleanup failed: {deleted.stderr.strip()}"
            )
        branch_remaining = _run_git(
            root_workspace, "show-ref", "--verify", "--quiet", branch_ref
        )
        if branch_remaining.returncode == 0:
            notes.append(f"attempt-created branch remains: {branch_ref}")
        elif branch_remaining.returncode != 1:
            notes.append(
                "could not verify attempt-created branch cleanup: "
                f"{branch_remaining.stderr.strip()}"
            )
    elif branch_after.returncode != 1:
        notes.append(
            "could not inspect attempt-created branch after failure: "
            f"{branch_after.stderr.strip()}"
        )

    if worktree_path.exists():
        detail = removed.stderr.strip()
        suffix = f": {detail}" if detail else ""
        notes.append(f"partial worktree path remains: {worktree_path}{suffix}")
    return notes


def _git_worktree_add(
    root_workspace: Path,
    branch: str,
    worktree_path: Path,
    base_commit: str,
) -> None:
    if worktree_path.exists():
        raise BootstrapError(BOOTSTRAP_WORKTREE_EXISTS, f"worktree exists: {worktree_path}")

    branch_ref = f"refs/heads/{branch}"
    branch_before = _run_git(
        root_workspace, "show-ref", "--verify", "--quiet", branch_ref
    )
    if branch_before.returncode not in (0, 1):
        raise BootstrapError(
            BOOTSTRAP_REQUEST_INVALID,
            f"git could not inspect target branch {branch!r}: {branch_before.stderr.strip()}",
        )
    branch_existed = branch_before.returncode == 0

    completed = _run_git(
        root_workspace,
        "worktree",
        "add",
        "-b",
        branch,
        str(worktree_path),
        base_commit,
    )
    if completed.returncode != 0:
        # ``git worktree add -b`` creates the branch before checkout.  A checkout
        # failure (for example a Windows long-path refusal) can therefore leave a
        # branch that makes every retry fail.  Roll back only identities proven to
        # have been absent before this exact attempt.
        rollback_notes = (
            _rollback_bootstrap_worktree(root_workspace, branch, worktree_path)
            if not branch_existed
            else []
        )
        rollback_suffix = (
            f"; rollback incomplete: {'; '.join(rollback_notes)}"
            if rollback_notes
            else ""
        )
        raise BootstrapError(
            BOOTSTRAP_REQUEST_INVALID,
            f"git worktree add failed: {completed.stderr.strip()}{rollback_suffix}",
        )


def _overlay_plan(
    source: Path,
    destination: Path,
    *,
    exclude: tuple[str, ...] = (),
    replace_if_matches: dict[str, Path] | None = None,
) -> list[tuple[Path, Path]]:
    """Plan one overlay tree and reject collisions before any write.

    ``exclude`` holds relative POSIX paths (e.g. ``.agent-workspace/lane-queue.py``)
    that are skipped; plain bootstrap uses it to omit the managed queue helpers.
    """
    if not source.is_dir():
        return []
    excluded = {Path(relative).as_posix() for relative in exclude}
    replaceable = replace_if_matches or {}
    planned: list[tuple[Path, Path]] = []
    for item in source.rglob("*"):
        if not item.is_file():
            continue
        relative = item.relative_to(source)
        if relative.as_posix() in excluded:
            continue
        target = destination / relative
        if target.exists():
            if target.is_file() and target.read_bytes() == item.read_bytes():
                continue
            expected = replaceable.get(relative.as_posix())
            if (
                expected is not None
                and expected.is_file()
                and target.is_file()
                and target.read_bytes() == expected.read_bytes()
            ):
                planned.append((item, target))
                continue
            raise BootstrapError(BOOTSTRAP_CACHE_COLLISION, f"collision at {target}")
        planned.append((item, target))
    return planned


def _copy_overlay(
    source: Path,
    destination: Path,
    *,
    exclude: tuple[str, ...] = (),
    replace_if_matches: dict[str, Path] | None = None,
) -> None:
    """Copy one overlay tree after preflighting all destination paths."""
    for item, target in _overlay_plan(
        source,
        destination,
        exclude=exclude,
        replace_if_matches=replace_if_matches,
    ):
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(item, target)


def _write_worker_prompt(
    worktree: Path,
    task_card: dict[str, Any],
    *,
    managed: bool,
    rationale: str | None = None,
) -> Path:
    acceptance_criteria = "\n".join(
        f"- {item.strip()}" for item in task_card["acceptance_criteria"]
    )
    deliverables = "\n".join(
        f"- {item.strip()}" for item in task_card["deliverables"]
    )
    lines = [
        str(task_card["task"]).strip(),
        f"## Acceptance criteria\n{acceptance_criteria}",
        f"## Deliverables\n{deliverables}",
        "## Reason for acceptance and deliverables\n"
        + str(task_card["reason_for_acceptance_and_deliverables"]).strip(),
    ]
    if rationale and rationale.strip():
        lines.append(f"\n## Resume rationale\n{rationale.strip()}")
    if managed:
        lines.append(
            "\n## Escalation (managed coordination)\n"
            "If this work needs a ROOT decision, authority, missing input, or help, run:\n"
            "  python .agent-workspace/manager-notify.py --severity blocking "
            "--summary \"<decision/action needed>\"\n"
            "Include the decision/action ROOT needs plus relevant local evidence.  Do not "
            "rely on a final chat answer as a notification.  After a blocking escalation, "
            "stop at a safe boundary rather than inventing the missing decision."
        )
    path = worktree / ".agent-workspace" / "worker-prompt.md"
    path.write_text("\n\n".join(lines) + "\n", encoding="utf-8")
    return path


def _write_result_template(worktree: Path, lane_id: str, run_id: str) -> Path:
    path = worktree / ".agent-workspace" / "result-template.json"
    template = {
        "schema": "result/v1",
        "lane_id": lane_id,
        "run_id": run_id,
        "outcome": "PASS",
        "summary": "",
        "evidence": [],
        "content_hash": "",
        "completed_at": "",
    }
    atomic_write_json(path, template)
    return path


def _write_invocation(
    worktree: Path,
    *,
    lane_id: str,
    run_id: str,
    provider_id: str,
    model: str,
    launch_config: dict[str, str],
    exclusive_resources: list[str],
) -> dict[str, Any]:
    agent_workspace = worktree / ".agent-workspace"
    invocation = {
        "schema": INVOCATION_SCHEMA,
        "lane_id": lane_id,
        "run_id": run_id,
        "provider": {
            "id": provider_id,
            "model": model,
            "launch_config": dict(launch_config),
        },
        "exclusive_resources": exclusive_resources,
        "launcher": {
            "entry": "python -m orchestrator_harness.controller",
            "argv": ["python", "-m", "orchestrator_harness.controller", lane_id],
        },
        "env": {},
        "cwd": str(worktree),
        "paths": {
            "worktree": str(worktree),
            "result": str(worktree / "RESULT.json"),
            "controller_status": str(agent_workspace / "controller.status.json"),
            "controller_events": str(agent_workspace / "controller.events.jsonl"),
            "transcript": str(agent_workspace / "provider-transcript.jsonl"),
            "stderr": str(agent_workspace / "provider-stderr.txt"),
            "attempts": str(agent_workspace / "controller.attempts.jsonl"),
            "last_message": str(agent_workspace / "last-message.txt"),
            "prompt": str(agent_workspace / "worker-prompt.md"),
        },
        "created_at": iso_utc(),
    }
    invocation["content_hash"] = content_hash(invocation)
    atomic_write_json(agent_workspace / "invocation.json", invocation)
    return invocation


def _write_worker_binding(worktree: Path, rt: Path, lane_id: str, run_id: str) -> Path:
    agent_workspace = worktree / ".agent-workspace"
    binding = {
        "schema": HOOK_BINDING_SCHEMA,
        "role": "worker",
        "lane_id": lane_id,
        "run_id": run_id,
        "manager_queue_path": str(rt / "manager" / "QUEUE.json"),
        "inbox_path": str(agent_workspace / "QUEUE.json"),
        "outbox_dir": str(agent_workspace / "manager-notifications"),
        "result_path": str(worktree / "RESULT.json"),
        "result_stop_check": str(agent_workspace / "result-stop-check.py"),
    }
    atomic_write_json(agent_workspace / "harness-hook-binding.json", binding)
    return agent_workspace / "harness-hook-binding.json"


def _install_managed_material(
    harness_root: Path,
    rt: Path,
    worktree: Path,
    *,
    provider_id: str,
    lane_id: str,
    run_id: str,
) -> None:
    """Install the managed worker payload: provider payload + inbox/outbox."""
    agent_workspace = worktree / ".agent-workspace"
    payload = rt / "super-cache" / "adapter-payloads" / provider_id
    if not payload.is_dir():
        raise BootstrapError(
            BOOTSTRAP_ADAPTER_MISSING,
            f"adapter payload missing for provider {provider_id}: {payload}",
        )
    payload_exclusions: list[str] = []
    if provider_id == "codex":
        config_relative = Path(".codex") / "config.toml"
        payload_config = payload / config_relative
        worktree_config = worktree / config_relative
        if payload_config.is_file() and worktree_config.exists():
            # Setup's public contract preserves a product-owned Codex config
            # byte-for-byte once it has explicitly enabled hooks.  Materialized
            # worktrees must honor the same contract instead of treating the
            # valid tracked file as an overlay collision.
            from .setup import SetupError, _validate_existing_codex_config

            try:
                _validate_existing_codex_config(worktree_config)
            except SetupError as exc:
                raise BootstrapError(BOOTSTRAP_CACHE_COLLISION, str(exc)) from exc
            payload_exclusions.append(config_relative.as_posix())

    shared_hook_relative = {
        "codex": Path(".codex") / "hooks.json",
        "claude-code": Path(".claude") / "settings.json",
    }.get(provider_id)
    if shared_hook_relative is not None:
        payload_hooks = payload / shared_hook_relative
        worktree_hooks = worktree / shared_hook_relative
        if payload_hooks.is_file() and worktree_hooks.exists():
            from .setup import SetupError, _merge_root_hook_config

            try:
                merged = _merge_root_hook_config(payload_hooks, worktree_hooks)
                existing = read_json(worktree_hooks)
            except (OSError, ValueError, SetupError) as exc:
                raise BootstrapError(BOOTSTRAP_CACHE_COLLISION, str(exc)) from exc
            if merged != existing:
                raise BootstrapError(
                    BOOTSTRAP_CACHE_COLLISION,
                    f"existing worker hook configuration lacks the shipped harness hooks: "
                    f"{worktree_hooks}",
                )
            payload_exclusions.append(shared_hook_relative.as_posix())
    root_payload = harness_root / "adapters" / provider_id / "root"
    replace_if_matches = {
        item.relative_to(root_payload).as_posix(): item
        for item in root_payload.rglob("*")
        if item.is_file() and "__pycache__" not in item.parts
    }
    _copy_overlay(
        payload,
        worktree,
        exclude=tuple(payload_exclusions),
        replace_if_matches=replace_if_matches,
    )
    inbox = {
        "schema": LANE_INBOX_SCHEMA,
        "lane_id": lane_id,
        "run_id": run_id,
        "assignments": [],
    }
    atomic_write_json(agent_workspace / "QUEUE.json", inbox)
    (agent_workspace / "manager-notifications").mkdir(parents=True, exist_ok=True)
    (agent_workspace / "processed-notifications").mkdir(parents=True, exist_ok=True)
    _write_worker_binding(worktree, rt, lane_id, run_id)


def _validate_provider_launch_config(
    harness_root: Path,
    *,
    provider_id: str,
    model: str,
    launch_config: dict[str, Any],
) -> dict[str, str]:
    """Resolve and validate provider preferences before any lane mutation."""
    from .setup import _load_binding

    binding_path = (
        harness_root
        / "orchestrator_harness"
        / "provider_adapters"
        / provider_id
        / "launcher_binding.py"
    )
    if not binding_path.is_file():
        raise BootstrapError(
            BOOTSTRAP_ADAPTER_MISSING,
            f"launcher binding missing for provider {provider_id}: {binding_path}",
        )
    try:
        binding = _load_binding(binding_path)
    except Exception as exc:
        raise BootstrapError(
            BOOTSTRAP_ADAPTER_MISSING,
            f"launcher binding cannot be loaded for provider {provider_id}: {exc}",
        ) from exc
    if getattr(binding, "PROVIDER_ID", None) != provider_id:
        raise BootstrapError(
            BOOTSTRAP_ADAPTER_MISSING,
            f"launcher binding identity does not match provider {provider_id}",
        )
    validate = getattr(binding, "validate_launch_config", None)
    if not callable(validate):
        raise BootstrapError(
            BOOTSTRAP_ADAPTER_MISSING,
            f"launcher binding lacks validate_launch_config for provider {provider_id}",
        )
    try:
        configured = validate(model=model, launch_config=launch_config)
    except (TypeError, ValueError) as exc:
        raise BootstrapError(BOOTSTRAP_REQUEST_INVALID, str(exc)) from exc
    if not isinstance(configured, dict) or not all(
        isinstance(key, str)
        and key
        and isinstance(value, str)
        and value
        for key, value in configured.items()
    ):
        raise BootstrapError(
            BOOTSTRAP_REQUEST_INVALID,
            f"launcher binding returned invalid launch configuration for {provider_id}",
        )
    return dict(configured)


def run_bootstrap(
    *,
    lane_id: str,
    provider: str,
    model: str,
    launch_config: dict[str, Any],
    exclusive_resources: list[str],
    task_card_path: str,
) -> dict[str, Any]:
    """Execute ``lane bootstrap`` and return the structured result."""
    try:
        from .config import find_harness_root

        harness_root = find_harness_root()
        config = load_config(harness_root)
        manifest = load_resource_manifest(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": BOOTSTRAP_REQUEST_INVALID,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "fix harness-config.json and resource-manifest.json, then re-run setup",
        }

    if not lane_id or not isinstance(lane_id, str) or "/" in lane_id or "\\" in lane_id:
        return {
            "ok": False,
            "code": BOOTSTRAP_REQUEST_INVALID,
            "summary": f"invalid lane id: {lane_id!r}",
            "evidence_paths": [],
            "next_action": "choose a simple lane id without path separators",
        }
    if not provider or not model:
        return {
            "ok": False,
            "code": BOOTSTRAP_REQUEST_INVALID,
            "summary": "provider and model are required",
            "evidence_paths": [],
            "next_action": "pass --provider and --model",
        }
    try:
        configured_launch = _validate_provider_launch_config(
            harness_root,
            provider_id=provider,
            model=model,
            launch_config=launch_config,
        )
    except BootstrapError as exc:
        return {
            "ok": False,
            "code": exc.code,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "configure every provider launch preference and re-run bootstrap",
        }
    for resource_id in exclusive_resources:
        if not manifest.is_declared(resource_id):
            return {
                "ok": False,
                "code": BOOTSTRAP_RESOURCE_UNDECLARED,
                "summary": f"resource not declared in the manifest: {resource_id}",
                "evidence_paths": [],
                "next_action": "fix the manifest (requires shutdown) or drop the resource",
            }

    rt = config.runtime_root
    worktree_path: Path | None = None
    branch: str | None = None
    worktree_created = False
    lane_record_written = False

    def rollback_failure_summary(summary: str) -> str:
        if not worktree_created:
            return summary
        if lane_record_written:
            return f"{summary}; rollback withheld because a lane record was published"
        if worktree_path is None or branch is None:
            return f"{summary}; rollback incomplete: attempt identity is unavailable"
        try:
            notes = _rollback_bootstrap_worktree(
                config.root_workspace, branch, worktree_path
            )
        except Exception as rollback_exc:
            return f"{summary}; rollback incomplete: {rollback_exc}"
        if notes:
            return f"{summary}; rollback incomplete: {'; '.join(notes)}"
        return f"{summary}; attempt worktree rolled back"

    try:
        task_card = _read_task_card(Path(task_card_path))
        state = open_epoch(rt, config, manifest)
        epoch_id = state["epoch_id"]
        for entry in read_active_lanes(rt, epoch_id):
            if entry.get("lane_id") == lane_id:
                raise BootstrapError(
                    BOOTSTRAP_LANE_ID_IN_USE,
                    f"lane id already used this epoch: {lane_id}",
                )
        run_id = new_id()
        worktree_path = rt / "worktrees" / epoch_id / lane_id
        branch = str(task_card.get("branch") or f"lane/{lane_id}")
        base_commit = str(task_card.get("base_commit") or "HEAD")
        _git_worktree_add(config.root_workspace, branch, worktree_path, base_commit)
        worktree_created = True
        agent_workspace = worktree_path / ".agent-workspace"
        agent_workspace.mkdir(parents=True, exist_ok=True)

        managed = config.profile == "managed"
        base_overlay = rt / "super-cache" / "workspace"
        if not base_overlay.is_dir():
            raise BootstrapError(
                BOOTSTRAP_CACHE_MISSING,
                f"active workspace base missing: {base_overlay} (run harness setup first)",
            )
        if managed:
            _copy_overlay(base_overlay, worktree_path)
            _install_managed_material(
                harness_root,
                rt,
                worktree_path,
                provider_id=provider,
                lane_id=lane_id,
                run_id=run_id,
            )
        else:
            _copy_overlay(
                base_overlay,
                worktree_path,
                exclude=PLAIN_EXCLUDED_HELPERS,
            )
        receipt = {
            "schema": OVERLAY_RECEIPT_SCHEMA,
            "lane_id": lane_id,
            "run_id": run_id,
            "profile": "managed" if managed else "plain",
            "base_cache_ref": "super-cache/workspace",
            "applied_at": iso_utc(),
        }
        if managed:
            receipt["provider_payload"] = f"adapter-payloads/{provider}"
        atomic_write_json(agent_workspace / "overlay-receipt.json", receipt)
        atomic_write_json(agent_workspace / "task-card.json", task_card)

        _write_worker_prompt(worktree_path, task_card, managed=managed)
        _write_result_template(worktree_path, lane_id, run_id)
        _write_invocation(
            worktree_path,
            lane_id=lane_id,
            run_id=run_id,
            provider_id=provider,
            model=model,
            launch_config=configured_launch,
            exclusive_resources=list(exclusive_resources),
        )

        lane = {
            "schema": LANE_SCHEMA,
            "lane_id": lane_id,
            "run_id": run_id,
            "worktree_path": str(worktree_path),
            "result_path": str(worktree_path / "RESULT.json"),
            "controller_status_path": str(agent_workspace / "controller.status.json"),
            "controller_events_path": str(agent_workspace / "controller.events.jsonl"),
            "transcript_path": str(agent_workspace / "provider-transcript.jsonl"),
            "stderr_path": str(agent_workspace / "provider-stderr.txt"),
            "attempts_path": str(agent_workspace / "controller.attempts.jsonl"),
            "last_message_path": str(agent_workspace / "last-message.txt"),
            "provider": {
                "id": provider,
                "model": model,
                "launch_config": configured_launch,
            },
            "session": {},
            "process": {},
            "lifecycle": "prepared",
            "acceptance_advancement": None,
            "last_reported_actionable_status": None,
        }
        if managed:
            lane["incoming_queue_path"] = str(agent_workspace / "QUEUE.json")
            lane["incoming_queue_command"] = str(agent_workspace / "lane-queue.py")
        write_lane(rt, epoch_id, lane_id, lane)
        lane_record_written = True

        entries = read_active_lanes(rt, epoch_id)
        entries.append(
            {
                "lane_id": lane_id,
                "lane_record_path": str(lane_record_dir(rt, epoch_id, lane_id) / "lane.json"),
                "run_id": run_id,
            }
        )
        write_active_lanes(rt, epoch_id, entries)
    except BootstrapError as exc:
        return {
            "ok": False,
            "code": exc.code,
            "summary": rollback_failure_summary(str(exc)),
            "evidence_paths": [],
            "next_action": "resolve the named target and re-run bootstrap",
        }
    except Exception as exc:
        return {
            "ok": False,
            "code": BOOTSTRAP_REQUEST_INVALID,
            "summary": rollback_failure_summary(str(exc)),
            "evidence_paths": [],
            "next_action": "resolve the error and re-run bootstrap",
        }

    return {
        "ok": True,
        "code": "BOOTSTRAP_OK",
        "summary": "status: prepared",
        "evidence_paths": [
            str(rt / "worktrees" / epoch_id / lane_id),
            str(lane_record_dir(rt, epoch_id, lane_id) / "lane.json"),
        ],
        "next_action": "run `lane launch --lane-id <id>`",
    }
