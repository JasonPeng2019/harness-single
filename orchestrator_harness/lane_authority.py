"""Operator-owned invocation inputs and controller evidence outside the worker."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .core import content_hash
from .epochs import lane_record_dir
from .records import atomic_write_bytes, atomic_write_json, read_record
from .mutation import read_bytes as anchored_read
from .portable_paths import component as portable_component

INVOCATION_SCHEMA = "controller-invocation/v1"
CONTROL_FILES = {
    "controller_status_path": "controller.status.json",
    "controller_events_path": "controller.events.jsonl",
    "transcript_path": "provider-transcript.jsonl",
    "stderr_path": "provider-stderr.txt",
    "attempts_path": "controller.attempts.jsonl",
    "last_message_path": "last-message.txt",
    "prompt_path": "worker-prompt.md",
    "task_card_path": "task-card.json",
    "invocation_path": "invocation.json",
}


def stage_authority(rt: Path, epoch_id: str, lane: dict[str, Any], task_card: dict[str, Any]) -> dict[str, Any]:
    """Pin validated operator inputs before publishing a new native run owner."""
    worktree = Path(lane["worktree_path"])
    portable_component(lane["run_id"])
    metadata = worktree.lstat()
    identity = (metadata.st_dev, metadata.st_ino)
    if "worktree_identity" in lane and tuple(lane["worktree_identity"]) != identity:
        raise ValueError("assigned worktree identity changed before staging authority")
    source = worktree / ".agent-workspace" / "invocation.json"
    invocation = json.loads(anchored_read(worktree, source.relative_to(worktree), expected_root_identity=identity))
    if not isinstance(invocation, dict) or invocation.get("schema") != INVOCATION_SCHEMA:
        raise ValueError("generated invocation schema is invalid")
    if (invocation.get("content_hash") != content_hash(invocation)
            or invocation.get("lane_id") != lane["lane_id"] or invocation.get("run_id") != lane["run_id"]
            or invocation.get("provider") != lane["provider"]
            or invocation.get("exclusive_resources") != lane["exclusive_resources"]):
        raise ValueError("generated invocation differs from the operator-owned assignment")
    directory = lane_record_dir(rt, epoch_id, lane["lane_id"]) / "runs" / lane["run_id"]
    if directory.exists():
        raise ValueError("native invocation inputs already exist; preserve them for reconciliation")
    directory.mkdir(parents=True)
    result = dict(lane)
    from .git_source import capture, current_tip
    if lane.get("git_source") is not None:
        current_tip(lane)
    else:
        result["git_source"] = capture(rt.parent, worktree, branch=lane.get("branch"))
    result["worktree_identity"] = list(identity)
    result["control_dir"] = str(directory)
    result.update({key: str(directory / filename) for key, filename in CONTROL_FILES.items()})
    prompt = anchored_read(worktree, ".agent-workspace/worker-prompt.md", expected_root_identity=identity)
    atomic_write_bytes(Path(result["prompt_path"]), prompt)
    atomic_write_json(Path(result["task_card_path"]), task_card)
    result["prompt_sha256"] = hashlib.sha256(prompt).hexdigest()
    result["task_card_hash"] = content_hash(task_card)
    from .result_validation import stage_worker_support
    result["result_assignment_hash"] = stage_worker_support(worktree, result)["content_hash"]
    if lane["provider"]["id"] == "codex":
        profile = anchored_read(worktree, ".codex/config.toml", expected_root_identity=identity)
        result["isolation_config_path"] = str(directory / "codex-config.toml")
        result["isolation_config_sha256"] = hashlib.sha256(profile).hexdigest()
        atomic_write_bytes(Path(result["isolation_config_path"]), profile)
    paths = invocation["paths"]
    invocation["git_source_hash"] = result["git_source"]["content_hash"]
    invocation["result_assignment_hash"] = result["result_assignment_hash"]
    for name in ("controller_status", "controller_events", "transcript", "stderr", "attempts", "last_message", "prompt"):
        paths[name] = result[name + "_path"]
    invocation["content_hash"] = content_hash(invocation)
    atomic_write_json(Path(result["invocation_path"]), invocation)
    result["invocation_hash"] = invocation["content_hash"]
    return result


def read_invocation(lane: dict[str, Any]) -> dict[str, Any]:
    from .git_source import current_tip
    current_tip(lane)
    metadata = Path(lane["worktree_path"]).lstat()
    identity = tuple(lane["worktree_identity"])
    if identity != (metadata.st_dev, metadata.st_ino):
        raise ValueError("assigned worktree identity changed after preparation")
    path = Path(lane["invocation_path"])
    if path.resolve().is_relative_to(Path(lane["worktree_path"]).resolve()):
        raise ValueError("controller invocation authority cannot be inside the worker worktree")
    invocation = read_record(path, INVOCATION_SCHEMA)
    from .result_validation import assignment
    if (invocation.get("lane_id") != lane["lane_id"] or invocation.get("run_id") != lane["run_id"]
            or invocation.get("provider") != lane["provider"]
            or invocation.get("exclusive_resources") != lane["exclusive_resources"]
            or invocation.get("content_hash") != lane["invocation_hash"]
            or invocation.get("content_hash") != content_hash(invocation)
            or invocation.get("git_source_hash") != lane["git_source"]["content_hash"]
            or invocation.get("result_assignment_hash") != lane["result_assignment_hash"]
            or assignment(lane)["content_hash"] != lane["result_assignment_hash"]
            or hashlib.sha256(Path(lane["prompt_path"]).read_bytes()).hexdigest() != lane["prompt_sha256"]
            or content_hash(read_record(Path(lane["task_card_path"]), "project-task-card/v1")) != lane["task_card_hash"]):
        raise ValueError("protected invocation, provider, resources, prompt or task identity changed")
    if lane["provider"]["id"] == "codex":
        expected = lane["isolation_config_sha256"]
        generated = anchored_read(Path(lane["worktree_path"]), ".codex/config.toml", expected_root_identity=identity)
        if (hashlib.sha256(Path(lane["isolation_config_path"]).read_bytes()).hexdigest() != expected
                or hashlib.sha256(generated).hexdigest() != expected):
            raise ValueError("Codex worker isolation configuration changed after preparation")
    return invocation


def retained_cleanup_owner(rt: Path, lease: dict[str, Any]) -> dict[str, Any]:
    """Recover one old run's protected cleanup inputs without adopting worker copies.

    A superseding lane is not cleanup evidence. Missing/ambiguous retained
    authority requires reconciliation; it cannot authorize resource reuse.
    Callers hold runtime admission and the leases lock.
    """
    lane_id, run_id = lease["lane_id"], lease["run_id"]
    portable_component(lane_id)
    portable_component(run_id)
    epochs = rt / "epochs"
    found = []
    for epoch in sorted(epochs.iterdir()):
        portable_component(epoch.name)
        if not epoch.is_dir() or epoch.resolve() != epoch.absolute():
            raise ValueError("retained epoch inventory is redirected or invalid")
        directory = epoch / "lanes" / lane_id / "runs" / run_id
        try:
            directory.lstat()
        except FileNotFoundError:
            continue
        raw = anchored_read(rt, (directory / "invocation.json").relative_to(rt))
        invocation = json.loads(raw)
        if (not isinstance(invocation, dict) or invocation.get("schema") != INVOCATION_SCHEMA
                or invocation.get("lane_id") != lane_id or invocation.get("run_id") != run_id
                or invocation.get("content_hash") != content_hash(invocation)
                or not isinstance(invocation.get("paths"), dict)
                or not isinstance(invocation.get("exclusive_resources"), list)
                or invocation.get("paths", {}).get("controller_status") != str(directory / CONTROL_FILES["controller_status_path"])
                or lease["resource_id"] not in invocation.get("exclusive_resources", [])):
            raise ValueError("retained invocation does not match resource ownership")
        owner = {"lane_id": lane_id, "run_id": run_id, "lifecycle": "running",
                 "launch_pending": False, "process": {key: lease[key] for key in ("pid", "creation_time")},
                 "worktree_path": invocation["cwd"], "provider": invocation["provider"],
                 "controller_status_path": str(directory / CONTROL_FILES["controller_status_path"])}
        from .retry_policy import history_path
        from .lane_cleanup import observe_controller_status
        owner["provider_history_path"] = str(history_path(rt, owner))
        observation = observe_controller_status(owner)
        identity = (observation.status or {}).get("controller_identity")
        if observation.kind != "valid" or not isinstance(identity, dict) or any(
                identity.get(key) != lease[key] for key in ("pid", "creation_time")):
            raise ValueError("retained controller identity does not match the lease holder")
        found.append(owner)
    if len(found) != 1:
        raise ValueError("exactly one protected prior invocation is required for orphan cleanup")
    return found[0]
