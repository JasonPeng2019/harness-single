"""``lane bootstrap``: prepare one lane (a short program, not an agent).

Creates the worktree + ``.agent-workspace``, applies the cache overlay, writes
the worker prompt/result template and the controller invocation.  Opens a new
epoch if none is active.  Does not start a provider or take a lease.

Managed bootstrap validates and copies the one installed composed payload for
the selected provider, then generates the lane-specific queue/result/invocation
records. Plain bootstrap uses the active ``workspace/`` base without managed
helpers or provider material. Source trees stay unchanged.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Sequence

from .activity_log import append_current_detail
from .config import load_config, load_resource_manifest
from .core import content_hash, iso_utc, new_id, read_json, require_schema
from .epochs import (
    lane_record_dir,
    open_epoch,
    read_active_lanes,
    write_active_lanes,
)
from .lanes import LANE_SCHEMA, write_lane
from . import memory_handoff
from .records import atomic_write_bytes, atomic_write_json

TASK_CARD_SCHEMA = "project-task-card/v1"
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
BOOTSTRAP_PLAN_PENDING = "BOOTSTRAP_PLAN_PENDING"
BOOTSTRAP_ALLOWANCE_EXPIRED = "BOOTSTRAP_ALLOWANCE_EXPIRED"

# Managed-only helpers carried by the workspace base; plain bootstrap omits them.
PLAIN_EXCLUDED_HELPERS = (
    ".agent-workspace/lane-queue.py",
    ".agent-workspace/manager-notify.py",
)


class BootstrapError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        attempt_effects: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.attempt_effects = attempt_effects


def _read_task_card(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise BootstrapError(BOOTSTRAP_REQUEST_INVALID, f"task card missing: {path}")
    try:
        record = read_json(path)
        require_schema(record, TASK_CARD_SCHEMA, path)
    except (OSError, ValueError) as exc:
        raise BootstrapError(BOOTSTRAP_REQUEST_INVALID, str(exc)) from exc
    task = record.get("task")
    if not isinstance(task, str) or not task.strip():
        raise BootstrapError(BOOTSTRAP_REQUEST_INVALID, "task card has no task text")
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
        # Record the exact proof of this failed boundary for the caller: the
        # attempt path never existed at entry, so a surviving path is this
        # attempt's exact identity, and the rollback helper's own
        # re-verification (an empty note list) is the only proof that an
        # attempt-created branch was removed.  A pre-existing branch is never
        # claimed by this attempt.
        surviving_path = worktree_path.exists()
        branch_unproven = (not branch_existed) and bool(rollback_notes)
        rollback_proven = not surviving_path and not branch_unproven
        boundary_effects: dict[str, Any] = {
            "worktree_created": False,
            "worktree_add_attempted": True,
            "lane_record_written": False,
            "worktree_path": str(worktree_path),
            "rollback_proven": rollback_proven,
        }
        if not rollback_proven:
            notes = list(rollback_notes)
            if surviving_path and not any(
                "partial worktree path remains" in note for note in notes
            ):
                notes.append(f"partial worktree path remains: {worktree_path}")
            boundary_effects["rollback_notes"] = notes
        raise BootstrapError(
            BOOTSTRAP_REQUEST_INVALID,
            f"git worktree add failed: {completed.stderr.strip()}{rollback_suffix}",
            attempt_effects=boundary_effects,
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




def _render_memory_context(worktree: Path) -> list[str]:
    """Render the finalized optional-memory context for the worker prompt.

    Only a finalized envelope bound to an exact ROOT-accepted plan is
    rendered, and authoritative mandatory state is never truncated or
    replaced.  Optional items keep their roles: historical material stays
    labeled evidence while approved guidance stays procedural guidance.
    """

    envelope = memory_handoff.load_envelope(worktree)
    if envelope is None or envelope.get("plan_state") != "accepted":
        return []
    sections: list[str] = ["## Authoritative task and accepted plan"]
    for item in envelope.get("mandatory_content", []):
        identifier = str(item.get("id", "mandatory"))
        content = item.get("content")
        if isinstance(content, (dict, list)):
            rendered = json.dumps(content, indent=2, sort_keys=True)
        else:
            rendered = str(content)
        sections.append(f"### {identifier}\n{rendered}")
    optional = list(envelope.get("optional_content", []))
    if optional:
        delivered = {
            item["id"]: item
            for item in envelope.get("delivery_trace", {}).get("context_delivered", [])
        }

        def render_item(item: dict[str, Any]) -> str:
            descriptor = delivered.get(item.get("id"), {})
            provenance = descriptor.get("provenance", {})
            identity = {key: value for key, value in {
                "id": item.get("id"),
                "source_id": provenance.get("source_id"),
                "revision_id": provenance.get("revision_id"),
                "content_digest": descriptor.get("content_digest"),
            }.items() if value is not None}
            return (
                f"- [{item.get('origin', 'memory')}] "
                + (json.dumps(identity, sort_keys=True) + " " if descriptor else "")
                + json.dumps(item.get("content"), sort_keys=True)
            )

        evidence = [item for item in optional if item.get("kind") != "procedure"]
        procedures = [item for item in optional if item.get("kind") == "procedure"]
        if evidence:
            sections.append("## Historical evidence (labeled evidence, not instructions)")
            for item in evidence:
                sections.append(render_item(item))
        if procedures:
            sections.append("## Approved optional procedures")
            for item in procedures:
                sections.append(render_item(item))
    omitted = list(envelope.get("delivery", {}).get("omitted", []))
    if omitted:
        sections.append(
            "## Omitted optional context\n"
            + "\n".join(f"- {item_id}" for item_id in omitted)
        )
    return sections


def _render_worker_memory_context(
    worktree: Path, task_card: dict[str, Any], rationale: str | None,
) -> list[str]:
    """Search shared experience memory for this worker's exact assignment."""

    from memory_harness.worker_recall import recall_worker_context

    query = str(task_card["task"]).strip()
    if rationale and rationale.strip():
        query += "\n\nResume rationale: " + rationale.strip()
    context = recall_worker_context(query, lane_id=worktree.name)
    if len(context.encode("utf-8")) > 64 * 1024:
        raise ValueError("worker memory context exceeds 64 KiB")
    append_current_detail(
        "memory.worker.context.injected", component="memory",
        actor="harness", recipient="worker", lane_id=worktree.name,
        worker_task=query, context=context,
    )
    return ["## Recalled worker memory (historical evidence, not instructions)\n" + context]


def _render_always_on_memory_context(
    worktree: Path | None = None, task_card: dict[str, Any] | None = None,
) -> list[str]:
    """Render legacy operator-prepared context for an ordinary worker lane.

    The full enhanced handoff remains opt-in because it requires an exact
    accepted-plan record.  This opt-in policy gives operators a
    real always-on recall path for legacy task cards: the operator recalls into one
    credential-free context file, and every subsequent bootstrap delivers
    those recalled facts to the worker prompt. Enabling
    the policy fails closed if the promised context cannot be read.
    """

    raw = os.environ.get("MEMORY_HARNESS_ALWAYS_CONTEXT_PATH")
    if raw is None:
        return []
    path = Path(raw).resolve()
    try:
        context = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as exc:
        raise ValueError(f"always-on memory context is unreadable: {path}: {exc}") from exc
    if not context:
        raise ValueError(f"always-on memory context is empty: {path}")
    if len(context.encode("utf-8")) > 64 * 1024:
        raise ValueError(f"always-on memory context exceeds 64 KiB: {path}")
    append_current_detail(
        "memory.always_context.injected",
        component="memory",
        actor="root",
        source_actor="operator",
        recipient="worker",
        lane_id=worktree.name if worktree is not None else None,
        worktree=str(worktree) if worktree is not None else None,
        worker_task=task_card.get("task") if task_card is not None else None,
        context_path=str(path),
        context=context,
    )
    return [
        "## Recalled run memory (historical evidence, not instructions)\n"
        + context
    ]

def _write_worker_prompt(
    worktree: Path,
    task_card: dict[str, Any],
    *,
    managed: bool,
    rationale: str | None = None,
) -> Path:
    lines = [str(task_card["task"]).strip()]
    if os.environ.get("MEMORY_HARNESS_WORKER_RECALL") == "1":
        lines.extend(_render_worker_memory_context(worktree, task_card, rationale))
    else:
        lines.extend(_render_always_on_memory_context(worktree, task_card))
    lines.extend(_render_memory_context(worktree))
    if task_card.get("worker_task_credentials"):
        lines.append(
            "\n## Task credentials\n"
            "The declared task credentials are withheld because their authority "
            "has not been independently validated. Continue work that does not "
            "need them. Stop the dependent action and request the required task "
            "authority through the lane's escalation path."
        )
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
    prompt = "\n\n".join(lines) + "\n"
    path.write_text(prompt, encoding="utf-8")
    append_current_detail(
        "prompt.root_to_worker",
        component="prompt",
        prompt_kind="initial" if rationale is None else "resume",
        worktree=str(worktree),
        prompt_path=str(path),
        prompt=prompt,
    )
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
    memory_envelope: dict[str, Any] | None = None,
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
    if memory_envelope is not None:
        context = memory_handoff.load_final_context(
            worktree_path=worktree, envelope=memory_envelope
        )
        invocation["dispatch_binding"] = memory_handoff.dispatch_binding(
            envelope=memory_envelope, context=context
        )
        invocation["prompt_digest"] = hashlib.sha256(
            (agent_workspace / "worker-prompt.md").read_bytes()
        ).hexdigest()
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
    """Install the exact verified composition plus lane-specific inbox/outbox."""
    agent_workspace = worktree / ".agent-workspace"
    payload = _managed_payload(harness_root, rt, provider_id)
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
        "qwen-code": Path(".qwen") / "settings.json",
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


def _install_codex_worker_isolation(
    worktree: Path, harness_root: Path, root_workspace: Path
) -> None:
    """Keep a worker inside its task worktree and away from operator state."""
    import os
    import tomllib

    config_path = worktree / ".codex" / "config.toml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    existing = config_path.read_text(encoding="utf-8") if config_path.exists() else ""
    try:
        parsed = tomllib.loads(existing)
    except tomllib.TOMLDecodeError as exc:
        raise BootstrapError(
            BOOTSTRAP_CACHE_COLLISION, f"invalid Codex worker config: {exc}"
        ) from exc
    if (
        "sandbox_mode" in parsed
        or "default_permissions" in parsed
        or "worker-isolated" in parsed.get("permissions", {})
    ):
        raise BootstrapError(
            BOOTSTRAP_CACHE_COLLISION,
            "Codex worker config conflicts with the required isolation profile",
        )

    root_workspace = root_workspace.resolve()
    workspaces_root = root_workspace.parent
    codex_home = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")).resolve()
    denied = {
        codex_home,
        (Path.home() / ".codex").resolve(),
        harness_root.resolve(),
        root_workspace / ".harness-runtime" / "operator-only",
        root_workspace / ".secrets",
    }
    if workspaces_root.is_dir():
        denied.update(
            sibling.resolve()
            for sibling in workspaces_root.iterdir()
            if sibling.is_dir() and sibling.resolve() != root_workspace
        )

    lines = [
        "# Harness-owned worker read isolation; regenerated for every lane.",
        "[permissions.worker-isolated]",
        'extends = ":workspace"',
        "[permissions.worker-isolated.filesystem]",
    ]
    lines.extend(
        f'{json.dumps(str(path))} = "deny"'
        for path in sorted(denied, key=lambda value: str(value).casefold())
    )
    lines.extend(
        [
            '[permissions.worker-isolated.filesystem.":workspace_roots"]',
            '"." = "write"',
            "",
        ]
    )
    rendered = (
        'default_permissions = "worker-isolated"\n'
        + existing.rstrip()
        + "\n"
        + "\n".join(lines)
    )
    atomic_write_bytes(config_path, rendered.encode("utf-8"))


def _managed_payload(harness_root: Path, rt: Path, provider_id: str) -> Path:
    """Fail closed when the installed tree differs from setup's exact plan."""
    from .setup import (
        COMPOSED_PAYLOADS,
        SETUP_CACHE_INVALID,
        SetupError,
        _require_plain_workspace_boundary,
        _validated_worker_payload,
    )

    payload = rt / "super-cache" / COMPOSED_PAYLOADS / provider_id
    try:
        _require_plain_workspace_boundary(rt, code=SETUP_CACHE_INVALID)
        if not payload.is_dir():
            raise BootstrapError(BOOTSTRAP_CACHE_MISSING, f"installed worker composition missing: {payload}")
        return _validated_worker_payload(harness_root, rt, provider_id)
    except (SetupError, OSError) as exc:
        raise BootstrapError(
            BOOTSTRAP_CACHE_COLLISION,
            str(exc),
        ) from exc


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
    allowance_seconds: float | None = None,
    search_stores: Sequence[Any] = (),
) -> dict[str, Any]:
    """Execute ``lane bootstrap`` and return the structured result.

    ``allowance_seconds`` is the caller's remaining share of one enclosing
    absolute deadline captured when the call enters, before any potentially
    blocking configuration or provider validation, so a slow preflight cannot
    renew the cutoff.  A spent allowance refuses before any worktree, epoch, or
    lane record exists, and the same absolute instant is re-checked
    immediately before each effect of this call, so a call that started inside
    the allowance and then ran long can never create a lane the caller has
    already stopped waiting for.  ``None`` keeps the ordinary caller's
    unbounded behaviour.
    """
    # Capture the caller's one absolute cutoff at function entry, before any
    # potentially blocking preflight; a slow config/provider validation must
    # not renew the allowance and start a lane the caller has stopped waiting
    # for.
    allowance_deadline: float | None = None
    if allowance_seconds is not None:
        allowance_deadline = time.monotonic() + float(allowance_seconds)
        if allowance_seconds <= 0:
            return {
                "ok": False,
                "code": BOOTSTRAP_ALLOWANCE_EXPIRED,
                "summary": (
                    "the enclosing allowance is spent; no lane was created and "
                    "nothing may be queued after it"
                ),
                "evidence_paths": [],
                "attempt_effects": {
                    "worktree_created": False,
                    "lane_record_written": False,
                    "rollback_proven": True,
                },
                "next_action": (
                    "reconcile the exact child of the enclosing attempt or "
                    "prepare again inside the remaining decision time"
                ),
            }

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
    if config.profile == "managed":
        try:
            _managed_payload(harness_root, config.runtime_root, provider)
        except BootstrapError as exc:
            return {
                "ok": False,
                "code": exc.code,
                "summary": str(exc),
                "evidence_paths": [],
                "next_action": "run harness setup and verify the installed worker composition",
            }
    else:
        from .setup import SetupError, _validated_cache_for_dispatch

        try:
            _validated_cache_for_dispatch(harness_root, config.runtime_root)
        except (SetupError, OSError) as exc:
            return {
                "ok": False,
                "code": BOOTSTRAP_CACHE_COLLISION,
                "summary": str(exc),
                "evidence_paths": [],
                "next_action": "run harness setup and verify the installed cache",
            }
    def allowance_expired() -> bool:
        """Report one absolute expiry without touching any effectful phase."""

        return allowance_deadline is not None and time.monotonic() >= allowance_deadline

    def refuse_expired_allowance(phase: str) -> "BootstrapError":
        return BootstrapError(
            BOOTSTRAP_ALLOWANCE_EXPIRED,
            f"the enclosing allowance expired before the {phase}; nothing was "
            "created after the caller stopped waiting",
        )

    rt = config.runtime_root
    worktree_path: Path | None = None
    branch: str | None = None
    epoch_id: str | None = None
    worktree_created = False
    lane_record_written = False
    memory: memory_handoff.LaneMemory | None = None

    def close_failed_attempt(summary: str) -> tuple[str, dict[str, Any]]:
        """Roll back attempt-created identity and report exact ownership.

        The returned effects tell a caller whether this attempt can still own
        a worktree or lane record, so an unproven rollback stays an unresolved
        child instead of a terminal proven-no-child refusal.
        """

        effects: dict[str, Any] = {
            "worktree_created": worktree_created,
            "lane_record_written": lane_record_written,
        }
        if worktree_path is not None:
            effects["worktree_path"] = str(worktree_path)
        if lane_record_written and epoch_id is not None:
            effects["lane_record_path"] = str(
                lane_record_dir(rt, epoch_id, lane_id) / "lane.json"
            )
        if not worktree_created:
            effects["rollback_proven"] = True
            return summary, effects
        if lane_record_written:
            effects["rollback_proven"] = False
            return (
                f"{summary}; rollback withheld because a lane record was published",
                effects,
            )
        if worktree_path is None or branch is None:
            effects["rollback_proven"] = False
            return (
                f"{summary}; rollback incomplete: attempt identity is unavailable",
                effects,
            )
        try:
            notes = _rollback_bootstrap_worktree(
                config.root_workspace, branch, worktree_path
            )
        except Exception as rollback_exc:
            effects["rollback_proven"] = False
            effects["rollback_notes"] = [str(rollback_exc)]
            return f"{summary}; rollback incomplete: {rollback_exc}", effects
        if notes:
            effects["rollback_proven"] = False
            effects["rollback_notes"] = list(notes)
            return f"{summary}; rollback incomplete: {'; '.join(notes)}", effects
        effects["rollback_proven"] = True
        return f"{summary}; attempt worktree rolled back", effects

    try:
        task_card = _read_task_card(Path(task_card_path))
        memory_handoff.validate_task_card(task_card)
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
        if allowance_expired():
            # The call entered inside the allowance and ran long; its first
            # effectful phase may not start now that the caller has stopped
            # waiting, so no late lane appears.
            raise refuse_expired_allowance("lane worktree was created")
        _git_worktree_add(config.root_workspace, branch, worktree_path, base_commit)
        worktree_created = True
        agent_workspace = worktree_path / ".agent-workspace"
        agent_workspace.mkdir(parents=True, exist_ok=True)

        managed = config.profile == "managed"
        if managed:
            _install_managed_material(
                harness_root,
                rt,
                worktree_path,
                provider_id=provider,
                lane_id=lane_id,
                run_id=run_id,
            )
        else:
            base_overlay = rt / "super-cache" / "workspace"
            if not base_overlay.is_dir():
                raise BootstrapError(
                    BOOTSTRAP_CACHE_MISSING,
                    f"active workspace base missing: {base_overlay} (run harness setup first)",
                )
            _copy_overlay(
                base_overlay,
                worktree_path,
                exclude=PLAIN_EXCLUDED_HELPERS,
            )
        if provider == "codex":
            _install_codex_worker_isolation(
                worktree_path, harness_root, config.root_workspace
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
            receipt["provider_payload"] = f"composed-payloads/{provider}"
        atomic_write_json(agent_workspace / "overlay-receipt.json", receipt)
        atomic_write_json(agent_workspace / "task-card.json", task_card)
        memory = memory_handoff.prepare_lane_memory(
            task_card=task_card,
            lane_id=lane_id,
            run_id=run_id,
            worktree_path=worktree_path,
            base_commit=base_commit,
            search_stores=search_stores,
        )
        if memory.envelope is not None:
            network = memory_handoff.captured_network_resolution(
                worktree_path=worktree_path, envelope=memory.envelope,
                task_card=task_card,
            )
            if network["effective_mode"] != "normal":
                from .provider_network_payload import install_soft_controls

                install_soft_controls(provider, worktree_path)

        def publish_lane(lane: dict[str, Any]) -> None:
            """Persist one prepared-or-pending lane record and declare it active."""
            nonlocal lane_record_written
            if allowance_expired():
                # The durable lane record is the ownership claim; it is the
                # last effect that may not appear after the caller's deadline.
                raise refuse_expired_allowance("lane record was published")
            write_lane(rt, epoch_id, lane_id, lane)
            lane_record_written = True
            entries = read_active_lanes(rt, epoch_id)
            entries.append(
                {
                    "lane_id": lane_id,
                    "lane_record_path": str(
                        lane_record_dir(rt, epoch_id, lane_id) / "lane.json"
                    ),
                    "run_id": run_id,
                }
            )
            write_active_lanes(rt, epoch_id, entries)

        def base_lane_record() -> dict[str, Any]:
            """Build the lane record shared by prepared and pending-plan lanes."""
            record: dict[str, Any] = {
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
            declared_environment = task_card.get("worker_environment")
            if isinstance(declared_environment, str) and declared_environment:
                record["worker_environment"] = declared_environment
            if managed:
                record["incoming_queue_path"] = str(agent_workspace / "QUEUE.json")
                record["incoming_queue_command"] = str(agent_workspace / "lane-queue.py")
            return record

        # An enabled enhanced handoff that is not an exact ROOT-accepted plan
        # has no execution authority at all, so this lane is materialized as a
        # pending-plan lane: it keeps its durable preparation disposition but
        # never receives a worker prompt, result template, or controller
        # invocation, and it cannot be launched until ROOT decides the plan.
        if memory.pending_plan:
            publish_lane(
                {
                    **base_lane_record(),
                    "memory_plan_state": memory.state,
                    "dispatchable": False,
                    "memory_pending_reason": memory_handoff.plan_state_summary(
                        str(memory.state)
                    ),
                }
            )
            return {
                "ok": False,
                "code": BOOTSTRAP_PLAN_PENDING,
                "summary": (
                    f"lane {lane_id} has no ROOT-accepted execution plan: "
                    + memory_handoff.plan_state_summary(str(memory.state))
                ),
                "evidence_paths": [
                    str(lane_record_dir(rt, epoch_id, lane_id) / "lane.json"),
                    str(memory_handoff.memory_paths(worktree_path)[0]),
                ],
                "next_action": (
                    "ROOT must review and accept the exact current plan, then "
                    "prepare the lane again; no worker was created or launched"
                ),
            }

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
            memory_envelope=memory.envelope,
        )

        lane = base_lane_record()
        if memory.state is not None:
            lane["memory_plan_state"] = memory.state
            lane["dispatchable"] = memory.dispatchable
        publish_lane(lane)
    except BootstrapError as exc:
        summary, effects = close_failed_attempt(str(exc))
        boundary_effects = getattr(exc, "attempt_effects", None)
        if isinstance(boundary_effects, dict):
            # The failed boundary recorded the exact proof of its own attempt; a
            # surviving attempt-created path or branch keeps its exact ownership
            # visible instead of a proven-no-child refusal.
            effects.update(boundary_effects)
        return {
            "ok": False,
            "code": exc.code,
            "summary": summary,
            "evidence_paths": [],
            "attempt_effects": effects,
            "next_action": "resolve the named target and re-run bootstrap",
        }
    except Exception as exc:
        summary, effects = close_failed_attempt(str(exc))
        return {
            "ok": False,
            "code": BOOTSTRAP_REQUEST_INVALID,
            "summary": summary,
            "evidence_paths": [],
            "attempt_effects": effects,
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
