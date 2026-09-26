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

import hashlib
import json
import os
import shutil
import stat
import subprocess
import time
from pathlib import Path
from typing import Any, Sequence

from .config import load_config, load_resource_manifest
from .core import content_hash, iso_utc, new_id, read_json
from .epochs import (
    lane_record_dir,
    open_epoch,
    read_active_lanes,
    write_active_lanes,
)
from .lanes import LANE_SCHEMA, update_lane, write_lane
from . import memory_handoff
from .records import RecordLock, atomic_write_json
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
BOOTSTRAP_PLAN_PENDING = "BOOTSTRAP_PLAN_PENDING"
BOOTSTRAP_ALLOWANCE_EXPIRED = "BOOTSTRAP_ALLOWANCE_EXPIRED"
BOOTSTRAP_CLEANUP_FAILED = "BOOTSTRAP_CLEANUP_FAILED"

AMBIGUOUS_ADD_DIAGNOSTIC = (
    "git worktree add did not report success; preserving every newly observed "
    "branch, path, and registration because ownership is ambiguous; inspect the "
    "exact bootstrap target before retrying"
)

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
        memory_handoff.validate_task_card(record)
    except (OSError, ValueError) as exc:
        raise BootstrapError(BOOTSTRAP_REQUEST_INVALID, str(exc)) from exc
    return record


def _git_worktree_add(
    root_workspace: Path,
    branch: str,
    worktree_path: Path,
    base_commit: str,
) -> None:
    if os.path.lexists(worktree_path):
        raise BootstrapError(BOOTSTRAP_WORKTREE_EXISTS, f"worktree exists: {worktree_path}")
    completed = subprocess.run(
        ["git", "-C", str(root_workspace), "worktree", "add", "-b", branch, str(worktree_path), base_commit],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        raise BootstrapError(
            BOOTSTRAP_CLEANUP_FAILED,
            f"{AMBIGUOUS_ADD_DIAGNOSTIC}: {completed.stderr.strip()}",
        )


def _capture_created_worktree_identity(
    worktree_path: Path, git_identity: dict[str, Any]
) -> dict[str, Any]:
    """Capture the unique linked-worktree administration inode after success."""

    admin_text = _git(
        worktree_path, "rev-parse", "--path-format=absolute", "--git-dir"
    ).stdout.strip()
    admin = Path(admin_text).resolve()
    common = Path(str(git_identity["common_dir"])).resolve()
    try:
        admin.relative_to(common / "worktrees")
    except ValueError as exc:
        raise BootstrapError(
            BOOTSTRAP_CLEANUP_FAILED,
            "created worktree administration directory is outside Git common state",
        ) from exc
    try:
        admin_info = admin.lstat()
        gitfile = worktree_path / ".git"
        gitfile_info = gitfile.lstat()
    except OSError as exc:
        raise BootstrapError(
            BOOTSTRAP_CLEANUP_FAILED,
            f"cannot capture created worktree administration identity: {exc}",
        ) from exc
    if not stat.S_ISDIR(admin_info.st_mode) or not stat.S_ISREG(gitfile_info.st_mode):
        raise BootstrapError(
            BOOTSTRAP_CLEANUP_FAILED,
            "created worktree administration identity is not a real directory/file",
        )
    return {
        "admin_path": str(admin),
        "admin_dev": admin_info.st_dev,
        "admin_ino": admin_info.st_ino,
        "gitfile_dev": gitfile_info.st_dev,
        "gitfile_ino": gitfile_info.st_ino,
    }


def _git(
    root_workspace: Path,
    *args: str,
    allowed_returncodes: frozenset[int] = frozenset({0}),
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        ["git", "-C", str(root_workspace), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode not in allowed_returncodes:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise BootstrapError(
            BOOTSTRAP_REQUEST_INVALID,
            f"git {' '.join(args)} failed: {detail or f'exit {completed.returncode}'}",
        )
    return completed


def _resolve_git_identity(
    root_workspace: Path,
    branch: str,
    base_commit: str,
) -> dict[str, Any]:
    """Resolve and validate the immutable Git identity before creating a lane.

    The worktree command receives only the resolved full commit, never the
    operator-provided revision string.  The new branch must not already exist;
    that makes rollback ownership unambiguous if a later bootstrap stage fails.
    """

    branch = branch.strip()
    base_commit = base_commit.strip()
    if not branch or not base_commit:
        raise BootstrapError(
            BOOTSTRAP_REQUEST_INVALID, "task card branch and base_commit must be nonempty"
        )
    _git(root_workspace, "check-ref-format", "--branch", branch)
    existing = _git(
        root_workspace,
        "show-ref",
        "--verify",
        "--quiet",
        f"refs/heads/{branch}",
        allowed_returncodes=frozenset({0, 1}),
    )
    if existing.returncode == 0:
        raise BootstrapError(
            BOOTSTRAP_REQUEST_INVALID, f"lane branch already exists: {branch}"
        )
    resolved = _git(
        root_workspace,
        "rev-parse",
        "--verify",
        "--end-of-options",
        f"{base_commit}^{{commit}}",
    ).stdout.strip()
    if len(resolved) != 40:
        raise BootstrapError(
            BOOTSTRAP_REQUEST_INVALID,
            f"base_commit did not resolve to a full commit: {base_commit}",
        )
    source_root_text = _git(root_workspace, "rev-parse", "--show-toplevel").stdout.strip()
    source_root = Path(source_root_text).resolve()
    if source_root != root_workspace.resolve():
        raise BootstrapError(
            BOOTSTRAP_REQUEST_INVALID,
            f"root_workspace must be the Git top level: {source_root}",
        )
    common_text = _git(root_workspace, "rev-parse", "--git-common-dir").stdout.strip()
    common_dir = Path(common_text)
    if not common_dir.is_absolute():
        common_dir = root_workspace / common_dir
    return {
        "source_root": str(source_root),
        "common_dir": str(common_dir.resolve()),
        "branch": branch,
        "base_commit": resolved,
        "origin_tip": resolved,
        "bootstrap_tip": resolved,
    }


def _verify_created_worktree(worktree_path: Path, git_identity: dict[str, Any]) -> None:
    branch = _git(worktree_path, "symbolic-ref", "--quiet", "--short", "HEAD").stdout.strip()
    head = _git(worktree_path, "rev-parse", "--verify", "HEAD^{commit}").stdout.strip()
    common_text = _git(worktree_path, "rev-parse", "--git-common-dir").stdout.strip()
    common_dir = Path(common_text)
    if not common_dir.is_absolute():
        common_dir = worktree_path / common_dir
    if branch != git_identity["branch"]:
        raise BootstrapError(
            BOOTSTRAP_REQUEST_INVALID,
            f"created worktree branch mismatch: expected {git_identity['branch']}, got {branch}",
        )
    if head != git_identity["bootstrap_tip"]:
        raise BootstrapError(
            BOOTSTRAP_REQUEST_INVALID,
            f"created worktree tip mismatch: expected {git_identity['bootstrap_tip']}, got {head}",
        )
    if common_dir.resolve() != Path(git_identity["common_dir"]).resolve():
        raise BootstrapError(
            BOOTSTRAP_REQUEST_INVALID, "created worktree Git common directory mismatch"
        )


def _rollback_created_worktree(
    root_workspace: Path,
    worktree_path: Path,
    branch: str,
    expected_commit: str,
    ownership: dict[str, Any],
) -> None:
    """Remove an exact successfully-added worktree whose admin inode matches."""

    admin = Path(str(ownership.get("admin_path") or ""))
    gitfile = worktree_path / ".git"
    try:
        admin_info = admin.lstat()
        gitfile_info = gitfile.lstat()
    except OSError as exc:
        raise BootstrapError(
            BOOTSTRAP_CLEANUP_FAILED,
            f"cannot re-prove created worktree ownership; preserving it: {exc}",
        ) from exc
    if (
        not stat.S_ISDIR(admin_info.st_mode)
        or (admin_info.st_dev, admin_info.st_ino)
        != (ownership.get("admin_dev"), ownership.get("admin_ino"))
        or not stat.S_ISREG(gitfile_info.st_mode)
        or (gitfile_info.st_dev, gitfile_info.st_ino)
        != (ownership.get("gitfile_dev"), ownership.get("gitfile_ino"))
    ):
        raise BootstrapError(
            BOOTSTRAP_CLEANUP_FAILED,
            "created worktree administration identity changed; preserving it",
        )

    listed = subprocess.run(
        ["git", "-C", str(root_workspace), "worktree", "list", "--porcelain"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if listed.returncode != 0:
        raise BootstrapError(
            BOOTSTRAP_CLEANUP_FAILED,
            listed.stderr.strip() or "git worktree list failed",
        )
    expected_path = os.path.normcase(str(worktree_path.resolve()))
    registered = any(
        line.startswith("worktree ")
        and os.path.normcase(str(Path(line[9:]).resolve())) == expected_path
        for line in listed.stdout.splitlines()
    )

    branch_status = subprocess.run(
        [
            "git",
            "-C",
            str(root_workspace),
            "show-ref",
            "--verify",
            "--quiet",
            f"refs/heads/{branch}",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if branch_status.returncode not in {0, 1}:
        raise BootstrapError(
            BOOTSTRAP_CLEANUP_FAILED,
            branch_status.stderr.strip() or "git branch inspection failed",
        )
    branch_exists = branch_status.returncode == 0
    if branch_exists:
        resolved_branch = subprocess.run(
            [
                "git",
                "-C",
                str(root_workspace),
                "rev-parse",
                "--verify",
                f"refs/heads/{branch}^{{commit}}",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if (
            resolved_branch.returncode != 0
            or resolved_branch.stdout.strip() != expected_commit
        ):
            actual = resolved_branch.stdout.strip() or "unresolved"
            raise BootstrapError(
                BOOTSTRAP_CLEANUP_FAILED,
                f"refusing to remove branch {branch}: expected {expected_commit}, "
                f"got {actual}",
            )

    if not registered:
        raise BootstrapError(
            BOOTSTRAP_CLEANUP_FAILED,
            "created worktree registration disappeared; preserving all artifacts",
        )
    completed = subprocess.run(
        ["git", "-C", str(root_workspace), "worktree", "remove", "--force", str(worktree_path)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        raise BootstrapError(
            BOOTSTRAP_CLEANUP_FAILED,
            completed.stderr.strip() or "git worktree remove failed",
        )
    if branch_exists:
        deleted = subprocess.run(
            ["git", "-C", str(root_workspace), "branch", "-D", branch],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if deleted.returncode != 0:
            raise BootstrapError(
                BOOTSTRAP_CLEANUP_FAILED,
                deleted.stderr.strip() or "git branch delete failed",
            )


def _rollback_lane_publication(
    rt: Path,
    epoch_id: str,
    lane_id: str,
    run_id: str,
    lane_record_hashes: set[str],
) -> None:
    """Remove only this exact failed publication, never a replacement lane."""

    expected_path = lane_record_dir(rt, epoch_id, lane_id) / "lane.json"
    entries = read_active_lanes(rt, epoch_id)
    matches = [entry for entry in entries if entry.get("lane_id") == lane_id]
    for entry in matches:
        if (
            entry.get("run_id") != run_id
            or Path(str(entry.get("lane_record_path") or "")).resolve()
            != expected_path.resolve()
        ):
            raise BootstrapError(
                BOOTSTRAP_CLEANUP_FAILED,
                "active lane publication changed ownership; preserving lane record",
            )
    if matches:
        write_active_lanes(
            rt,
            epoch_id,
            [entry for entry in entries if entry.get("lane_id") != lane_id],
        )
    with RecordLock(expected_path):
        if not expected_path.is_file():
            return
        try:
            current = read_json(expected_path)
        except (OSError, ValueError) as exc:
            raise BootstrapError(
                BOOTSTRAP_CLEANUP_FAILED,
                f"cannot validate failed lane publication: {exc}",
            ) from exc
        if (
            current.get("lane_id") != lane_id
            or current.get("run_id") != run_id
            or content_hash(current) not in lane_record_hashes
        ):
            raise BootstrapError(
                BOOTSTRAP_CLEANUP_FAILED,
                "lane record changed ownership; preserving it",
            )
        expected_path.unlink()


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
    """Render only finalized, identity-bound optional memory context."""
    envelope = memory_handoff.load_envelope(worktree)
    if envelope is None or envelope.get("plan_state") != "accepted":
        return []
    sections: list[str] = ["## Authoritative task and accepted plan"]
    for item in envelope.get("mandatory_content", []):
        identifier = str(item.get("id", "mandatory"))
        content = item.get("content")
        rendered = (
            json.dumps(content, indent=2, sort_keys=True)
            if isinstance(content, (dict, list))
            else str(content)
        )
        sections.append(f"### {identifier}\n{rendered}")
    optional = list(envelope.get("optional_content", []))
    if optional:
        delivered = {
            item["id"]: item
            for item in envelope.get("delivery_trace", {}).get(
                "context_delivered", []
            )
        }

        def render_item(item: dict[str, Any]) -> str:
            descriptor = delivered.get(item.get("id"), {})
            provenance = descriptor.get("provenance", {})
            identity = {
                key: value
                for key, value in {
                    "id": item.get("id"),
                    "source_id": provenance.get("source_id"),
                    "revision_id": provenance.get("revision_id"),
                    "content_digest": descriptor.get("content_digest"),
                }.items()
                if value is not None
            }
            return (
                f"- [{item.get('origin', 'memory')}] "
                + (json.dumps(identity, sort_keys=True) + " " if descriptor else "")
                + json.dumps(item.get("content"), sort_keys=True)
            )

        evidence = [item for item in optional if item.get("kind") != "procedure"]
        procedures = [item for item in optional if item.get("kind") == "procedure"]
        if evidence:
            sections.append(
                "## Historical evidence (labeled evidence, not instructions)"
            )
            sections.extend(render_item(item) for item in evidence)
        if procedures:
            sections.append("## Approved optional procedures")
            sections.extend(render_item(item) for item in procedures)
    omitted = list(envelope.get("delivery", {}).get("omitted", []))
    if omitted:
        sections.append(
            "## Omitted optional context\n"
            + "\n".join(f"- {item_id}" for item_id in omitted)
        )
    return sections


def _write_worker_prompt(
    worktree: Path,
    task_card: dict[str, Any],
    *,
    managed: bool,
    rationale: str | None = None,
    memory_source: Path | None = None,
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
    lines.extend(_render_memory_context(memory_source or worktree))
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
    git_identity: dict[str, Any] | None = None,
    memory_envelope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    agent_workspace = worktree / ".agent-workspace"
    if git_identity is None:
        # Resume rewrites the invocation in place.  Preserve the original
        # authoritative bootstrap identity rather than re-resolving it from a
        # mutable worktree.
        prior_path = agent_workspace / "invocation.json"
        try:
            prior = read_json(prior_path)
            candidate = prior.get("git")
        except (OSError, ValueError):
            candidate = None
        if not isinstance(candidate, dict):
            raise BootstrapError(
                BOOTSTRAP_REQUEST_INVALID,
                "cannot rewrite invocation without its authoritative Git identity",
            )
        git_identity = candidate
    invocation = _build_invocation(
        worktree,
        lane_id=lane_id,
        run_id=run_id,
        provider_id=provider_id,
        model=model,
        launch_config=launch_config,
        exclusive_resources=exclusive_resources,
        git_identity=git_identity,
        memory_envelope=memory_envelope,
    )
    atomic_write_json(agent_workspace / "invocation.json", invocation)
    return invocation


def _build_invocation(
    worktree: Path,
    *,
    lane_id: str,
    run_id: str,
    provider_id: str,
    model: str,
    launch_config: dict[str, str],
    exclusive_resources: list[str],
    git_identity: dict[str, Any],
    memory_envelope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a hashed invocation without publishing it.

    Resume uses this to durably stage the next invocation outside the lane
    worktree before replacing any current-run evidence.
    """

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
        "git": dict(git_identity),
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
    payload = _managed_payload(harness_root, rt, provider_id)
    payload_exclusions: list[str] = []
    if provider_id == "codex":
        config_relative = Path(".codex") / "config.toml"
        payload_config = payload / config_relative
        worktree_config = worktree / config_relative
        if payload_config.is_file() and worktree_config.exists():
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
                    "existing worker hook configuration lacks the shipped harness hooks: "
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


def _managed_payload(harness_root: Path, rt: Path, provider_id: str) -> Path:
    """Return only setup's byte-exact installed worker composition."""
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
            raise BootstrapError(
                BOOTSTRAP_CACHE_MISSING,
                f"installed worker composition missing: {payload}",
            )
        return _validated_worker_payload(harness_root, rt, provider_id)
    except (SetupError, OSError) as exc:
        raise BootstrapError(BOOTSTRAP_CACHE_COLLISION, str(exc)) from exc


def _validate_provider_launch_config(
    harness_root: Path,
    *,
    provider_id: str,
    model: str,
    launch_config: dict[str, Any],
) -> dict[str, str]:
    """Resolve provider-owned launch preferences before mutating lane state."""
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
    """Execute ``lane bootstrap`` with one optional enclosing time allowance."""
    allowance_deadline: float | None = None
    if allowance_seconds is not None:
        allowance_deadline = time.monotonic() + float(allowance_seconds)
        if allowance_seconds <= 0:
            return {
                "ok": False,
                "code": BOOTSTRAP_ALLOWANCE_EXPIRED,
                "summary": "the enclosing allowance is spent; no lane was created",
                "evidence_paths": [],
                "attempt_effects": {
                    "worktree_created": False,
                    "lane_record_written": False,
                    "rollback_proven": True,
                },
                "next_action": "prepare again inside the remaining decision time",
            }

    def allowance_expired() -> bool:
        return allowance_deadline is not None and time.monotonic() >= allowance_deadline
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
    worktree_add_succeeded = False
    worktree_ownership: dict[str, Any] | None = None
    worktree_path: Path | None = None
    created_branch: str | None = None
    published_epoch_id: str | None = None
    published_run_id: str | None = None
    lane_record_hashes: set[str] = set()
    active_index_published = False
    pending_memory_state: str | None = None
    bootstrap_lock = RecordLock(rt / ".bootstrap.lock")
    lock_held = False
    try:
        bootstrap_lock.__enter__()
        lock_held = True
        task_card = _read_task_card(Path(task_card_path))
        branch = str(task_card.get("branch") or f"lane/{lane_id}")
        base_commit = str(task_card.get("base_commit") or "HEAD")
        git_identity = _resolve_git_identity(config.root_workspace, branch, base_commit)

        # Validate every material source before creating a Git worktree.  In
        # particular, an unknown/missing provider must not leave a branch or
        # worktree behind.
        managed = config.profile == "managed"
        base_overlay = rt / "super-cache" / "workspace"
        if managed:
            provider_payload = _managed_payload(harness_root, rt, provider)
        else:
            from .setup import SetupError, _validated_cache_for_dispatch

            try:
                _validated_cache_for_dispatch(harness_root, rt)
            except (SetupError, OSError) as exc:
                raise BootstrapError(BOOTSTRAP_CACHE_COLLISION, str(exc)) from exc
            if not base_overlay.is_dir():
                raise BootstrapError(
                    BOOTSTRAP_CACHE_MISSING,
                    f"active workspace base missing: {base_overlay} (run harness setup first)",
                )
            provider_payload = rt / "super-cache" / "composed-payloads" / provider

        if allowance_expired():
            raise BootstrapError(
                BOOTSTRAP_ALLOWANCE_EXPIRED,
                "the enclosing allowance expired before opening an epoch",
            )
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
        # This exact lexists check belongs inside the bootstrap lock and must
        # precede any ownership/rollback marker.  In particular, a broken
        # symlink or an externally-created sentinel is still a pre-existing
        # path and can never be attributed to this bootstrap attempt.
        if os.path.lexists(worktree_path):
            raise BootstrapError(
                BOOTSTRAP_WORKTREE_EXISTS, f"worktree exists: {worktree_path}"
            )
        created_branch = branch
        # Branch and path were both absent at preflight.  Rollback ownership is
        # established only after `git worktree add` reports success and the
        # linked-worktree administrative identity is captured below.
        if allowance_expired():
            raise BootstrapError(
                BOOTSTRAP_ALLOWANCE_EXPIRED,
                "the enclosing allowance expired before creating the lane worktree",
            )
        _git_worktree_add(
            config.root_workspace,
            branch,
            worktree_path,
            str(git_identity["base_commit"]),
        )
        worktree_add_succeeded = True
        worktree_ownership = _capture_created_worktree_identity(
            worktree_path, git_identity
        )
        _verify_created_worktree(worktree_path, git_identity)
        agent_workspace = worktree_path / ".agent-workspace"
        agent_workspace.mkdir(parents=True, exist_ok=True)

        harness_owned_paths = {".agent-workspace/**", "RESULT.json"}
        if managed:
            harness_owned_paths.update(
                item.relative_to(provider_payload).as_posix()
                for item in provider_payload.rglob("*")
                if item.is_file()
            )
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
        git_identity["harness_owned_paths"] = sorted(harness_owned_paths)
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
            base_commit=str(git_identity["base_commit"]),
            search_stores=search_stores,
        )
        if memory.envelope is not None:
            network = memory_handoff.captured_network_resolution(
                worktree_path=worktree_path,
                envelope=memory.envelope,
                task_card=task_card,
            )
            if network["effective_mode"] != "normal":
                from .provider_network_payload import install_soft_controls

                install_soft_controls(provider, worktree_path)

        invocation: dict[str, Any] | None = None
        if not memory.pending_plan:
            _write_worker_prompt(worktree_path, task_card, managed=managed)
            _write_result_template(worktree_path, lane_id, run_id)
            invocation = _write_invocation(
                worktree_path,
                lane_id=lane_id,
                run_id=run_id,
                provider_id=provider,
                model=model,
                launch_config=configured_launch,
                exclusive_resources=list(exclusive_resources),
                git_identity=git_identity,
                memory_envelope=memory.envelope,
            )
        else:
            pending_memory_state = str(memory.state)

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
            "git": git_identity,
            "invocation_hash": (
                invocation["content_hash"] if invocation is not None else None
            ),
            "task_card_hash": content_hash(task_card),
            "result_validation": None,
            "session": {},
            "process": {},
            "lifecycle": "prepared",
            "acceptance_advancement": None,
            "last_reported_actionable_status": None,
            "publication_state": "staged",
        }
        declared_environment = task_card.get("worker_environment")
        if isinstance(declared_environment, str) and declared_environment:
            lane["worker_environment"] = declared_environment
        if memory.state is not None:
            lane["memory_plan_state"] = memory.state
            lane["dispatchable"] = memory.dispatchable
        if memory.pending_plan:
            lane["memory_pending_reason"] = memory_handoff.plan_state_summary(
                str(memory.state)
            )
        if managed:
            lane["incoming_queue_path"] = str(agent_workspace / "QUEUE.json")
            lane["incoming_queue_command"] = str(agent_workspace / "lane-queue.py")
        if allowance_expired():
            raise BootstrapError(
                BOOTSTRAP_ALLOWANCE_EXPIRED,
                "the enclosing allowance expired before publishing the lane record",
            )
        written_lane = write_lane(rt, epoch_id, lane_id, lane)
        published_epoch_id = epoch_id
        published_run_id = run_id
        lane_record_hashes.add(content_hash(written_lane))

        entries = read_active_lanes(rt, epoch_id)
        entries.append(
            {
                "lane_id": lane_id,
                "lane_record_path": str(lane_record_dir(rt, epoch_id, lane_id) / "lane.json"),
                "run_id": run_id,
            }
        )
        write_active_lanes(rt, epoch_id, entries)
        active_index_published = True
        published_lane = update_lane(
            rt,
            epoch_id,
            lane_id,
            lambda current: {**current, "publication_state": "published"},
        )
        lane_record_hashes.add(content_hash(published_lane))
    except BootstrapError as exc:
        cleanup_errors: list[str] = []
        publication_cleanup_safe = True
        if active_index_published:
            cleanup_errors.append(
                "active lane index was already published; preserving the exact lane and worktree for inspection"
            )
            publication_cleanup_safe = False
        elif (
            published_epoch_id is not None
            and published_run_id is not None
            and lane_record_hashes
        ):
            try:
                _rollback_lane_publication(
                    rt,
                    published_epoch_id,
                    lane_id,
                    published_run_id,
                    lane_record_hashes,
                )
            except BootstrapError as cleanup_exc:
                cleanup_errors.append(str(cleanup_exc))
                publication_cleanup_safe = False
        if (
            publication_cleanup_safe
            and worktree_add_succeeded
            and worktree_ownership is not None
            and worktree_path is not None
            and created_branch is not None
        ):
            try:
                _rollback_created_worktree(
                    config.root_workspace,
                    worktree_path,
                    created_branch,
                    str(git_identity["base_commit"]),
                    worktree_ownership,
                )
            except BootstrapError as cleanup_exc:
                cleanup_errors.append(str(cleanup_exc))
        if cleanup_errors:
            exc = BootstrapError(
                BOOTSTRAP_CLEANUP_FAILED,
                f"{exc}; bootstrap rollback also failed: {'; '.join(cleanup_errors)}",
            )
        return {
            "ok": False,
            "code": exc.code,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "resolve the named target and re-run bootstrap",
        }
    except Exception as exc:
        cleanup_errors = []
        publication_cleanup_safe = True
        if active_index_published:
            cleanup_errors.append(
                "active lane index was already published; preserving the exact lane and worktree for inspection"
            )
            publication_cleanup_safe = False
        elif (
            published_epoch_id is not None
            and published_run_id is not None
            and lane_record_hashes
        ):
            try:
                _rollback_lane_publication(
                    rt,
                    published_epoch_id,
                    lane_id,
                    published_run_id,
                    lane_record_hashes,
                )
            except BootstrapError as cleanup_exc:
                cleanup_errors.append(str(cleanup_exc))
                publication_cleanup_safe = False
        if (
            publication_cleanup_safe
            and
            worktree_add_succeeded
            and worktree_ownership is not None
            and worktree_path is not None
            and created_branch is not None
        ):
            try:
                _rollback_created_worktree(
                    config.root_workspace,
                    worktree_path,
                    created_branch,
                    str(git_identity["base_commit"]),
                    worktree_ownership,
                )
            except BootstrapError as cleanup_exc:
                cleanup_errors.append(str(cleanup_exc))
        if cleanup_errors:
            return {
                "ok": False,
                "code": BOOTSTRAP_CLEANUP_FAILED,
                "summary": f"{exc}; bootstrap rollback also failed: {'; '.join(cleanup_errors)}",
                "evidence_paths": [str(worktree_path)] if worktree_path else [],
                "next_action": "inspect and remove only the reported exact bootstrap artifacts",
            }
        return {
            "ok": False,
            "code": BOOTSTRAP_REQUEST_INVALID,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "resolve the error and re-run bootstrap",
        }
    finally:
        if lock_held:
            bootstrap_lock.__exit__(None, None, None)

    if pending_memory_state is not None:
        return {
            "ok": False,
            "code": BOOTSTRAP_PLAN_PENDING,
            "summary": (
                f"lane {lane_id} has no ROOT-accepted execution plan: "
                + memory_handoff.plan_state_summary(pending_memory_state)
            ),
            "evidence_paths": [
                str(lane_record_dir(rt, epoch_id, lane_id) / "lane.json"),
                str(memory_handoff.memory_paths(worktree_path)[0]),
            ],
            "next_action": (
                "ROOT must accept the exact plan before preparing a worker; "
                "no worker was created or launched"
            ),
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
