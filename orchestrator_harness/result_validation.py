"""One RESULT contract for ROOT and the staged worker Stop checker.

ROOT supplies protected native assignment fields. A worker uses its local mirror
for Stop diagnostics; that mirror can never authorize ROOT review or retirement.
"""
from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
import re
import stat
from typing import Any

from .core import content_hash
from . import git_source
from .mutation import read_bytes as anchored_read
from .portable_paths import repository_parts

RESULT_SCHEMA = "result/v1"
ASSIGNMENT_SCHEMA = "worker-result-assignment/v1"
RESULT_OUTCOMES = frozenset({"PASS", "FAIL", "BLOCKED"})
MAX_RESULT_BYTES = 4_000_000
VALIDATION_MODULES = ("__init__", "core", "portable_paths", "mutation", "git_source", "result_validation")


def assignment(lane: dict[str, Any]) -> dict[str, Any]:
    fields = ("lane_id", "run_id", "worktree_path", "worktree_identity", "git_source",
              "git_write_namespace", "task_card_hash", "source_exclusions")
    record = {key: lane[key] for key in fields if key in lane}
    record["schema"] = ASSIGNMENT_SCHEMA
    record["content_hash"] = content_hash(record)
    return record


def _excluded(path: str, lane: dict[str, Any]) -> bool:
    if path == "RESULT.json" or path == ".agent-workspace" or path.startswith(".agent-workspace/"):
        return True
    return path in lane.get("source_exclusions", [])


def _index_entries(source: dict[str, Any]) -> dict[str, tuple[str, str]]:
    private = Path(source["private_dir"])
    before = anchored_read(private, "index", expected_root_identity=tuple(source["private_identity"]))
    result = {}
    for entry in git_source.git(source, "ls-files", "--stage", "-z").split(b"\0"):
        if not entry:
            continue
        header, path = entry.split(b"\t", 1)
        mode, oid, stage = header.decode("ascii").split()
        relative = path.decode("utf-8")
        repository_parts(relative)
        if stage != "0" or relative in result:
            raise ValueError("assigned worktree index contains an unresolved or duplicate entry")
        result[relative] = (mode, oid)
    if anchored_read(private, "index", expected_root_identity=tuple(source["private_identity"])) != before:
        raise ValueError("assigned index changed during observation")
    return result


def _canonical_working_digest(source: dict[str, Any], tip: str, path: str, value: bytes, *,
                              normalize_crlf: bool = False) -> str:
    # Read attributes from the verified commit, never a dirty .gitattributes.
    attributes = git_source.git(source, "-c", "core.attributesFile=" + os.devnull,
                                "check-attr", "--source=" + tip, "-z", "filter", "--", path).split(b"\0")
    driver = attributes[2].decode("utf-8") if len(attributes) >= 3 else "unspecified"
    options = ["-c", "core.attributesFile=" + os.devnull]
    # Reserved output tokens can also be literal driver names. Disable the
    # selected name even for 'set'/'unset'/'unspecified', avoiding that ambiguity.
    overrides = {f"filter.{driver}.clean": "", f"filter.{driver}.process": "",
                 f"filter.{driver}.required": "false"}
    if normalize_crlf:
        # Operator Git ignores system config, but Git for Windows enables
        # core.autocrlf there, so an ordinary worker commit stores LF text
        # while CRLF stays on disk. Apply only that text CRLF->LF input rule;
        # committed attributes (-text, eol) still decide as for the worker.
        overrides["core.autocrlf"] = "true"
    digest = git_source.git(source, "--attr-source=" + tip, *options,
                            "hash-object", "--stdin", "--path=" + path, input_bytes=value,
                            config_overrides=overrides).decode().strip()
    return digest


def require_clean(lane: dict[str, Any], tip: str) -> None:
    source, worktree = lane["git_source"], Path(lane["worktree_path"])
    tree = git_source.tree_entries(source, tip)
    # A product-owned ignore file can leave generated lane files visible to
    # 'git add -A'; never accept a branch whose history commits harness material
    # (generated config holds machine paths and would reach integration).
    base_commit = source["base_commit"]
    base = git_source.tree_entries(source, base_commit)
    changed = [name for name in sorted(set(tree) | set(base)) if _excluded(name, lane) and tree.get(name) != base.get(name)]
    generated = ["RESULT.json", ".agent-workspace", *lane.get("source_exclusions", [])]
    if changed or git_source.git(source, "--literal-pathspecs", "rev-list", "--full-history",
                                 base_commit + ".." + tip, "--", *generated).strip():
        restore = f"'git reset -q -- {' '.join(changed)}', " if changed else ""
        raise ValueError(f"reported branch commits generated lane files ({', '.join(changed) or 'in its history'}); "
                         f"rebuild it with 'git reset --soft {base_commit}', {restore}then commit again "
                         "(working files stay)")
    index = _index_entries(source)
    expected = {name: entry for name, entry in tree.items() if not _excluded(name, lane)}
    actual = {name: entry for name, entry in index.items() if not _excluded(name, lane)}
    if actual != expected:
        raise ValueError("assigned worktree index differs from the reported commit")
    git_source.verify_objects(source, "blob", [oid for mode, oid in expected.values() if mode != "160000"])
    for name, (mode, oid) in expected.items():
        if mode == "160000":
            raise ValueError("source cleanliness for a Git submodule requires explicit validation; it is not proven by the parent index")
        # Read actual bytes regardless of assume-unchanged/skip-worktree flags.
        # Real links remain unproven; never follow one outside the assignment.
        value = anchored_read(worktree, name, expected_root_identity=tuple(lane["worktree_identity"]))
        digest = git_source.object_digest("blob", value, len(oid))
        if digest != oid and mode in {"100644", "100755"}:
            digest = _canonical_working_digest(source, tip, name, value)
        if digest != oid and mode in {"100644", "100755"} and b"\r\n" in value:
            digest = _canonical_working_digest(source, tip, name, value, normalize_crlf=True)
        if digest != oid:
            raise ValueError(f"working source differs from committed bytes (or needs an external filter validator): {name}")
        if mode == "100755" and os.name != "nt" and not (worktree / name).stat().st_mode & stat.S_IXUSR:
            raise ValueError(f"working source executable mode differs: {name}")
    for raw in git_source.git(source, "ls-files", "--others", "--exclude-standard", "-z").split(b"\0"):
        if raw and not _excluded(raw.decode("utf-8"), lane):
            raise ValueError(f"untracked project source remains: {raw.decode('utf-8')}")
    if _index_entries(source) != index or git_source.current_tip(lane) != tip:
        raise ValueError("assigned source changed during result validation")


def validate_record(record: dict[str, Any], lane: dict[str, Any], *, clean: bool | None = None) -> dict[str, Any]:
    if not isinstance(record, dict) or record.get("schema") != RESULT_SCHEMA:
        raise ValueError("RESULT schema must be result/v1")
    for field in ("lane_id", "run_id"):
        if not lane.get(field) or record.get(field) != lane[field]:
            raise ValueError(f"RESULT {field} differs from its current assignment")
    if record.get("worker_invocation_id") != lane["run_id"]:
        raise ValueError("RESULT worker_invocation_id differs from the native invocation")
    if not lane.get("task_card_hash") or record.get("task_card_hash") != lane["task_card_hash"]:
        raise ValueError("RESULT task card differs from its protected assignment")
    if record.get("outcome") not in RESULT_OUTCOMES:
        raise ValueError("RESULT outcome is invalid")
    if not isinstance(record.get("summary"), str) or not record["summary"].strip():
        raise ValueError("RESULT summary is empty")
    if not isinstance(record.get("evidence"), list) or any(
            not ((isinstance(item, str) and item.strip()) or (isinstance(item, dict) and item))
            for item in record["evidence"]):
        raise ValueError("RESULT evidence must contain nonempty strings or structured objects")
    json.dumps(record, allow_nan=False)
    completed = record.get("completed_at")
    if not isinstance(completed, str) or not completed.strip():
        raise ValueError("RESULT completed_at is empty")
    try:
        if datetime.fromisoformat(completed.replace("Z", "+00:00")).tzinfo is None:
            raise ValueError("timestamp has no timezone")
    except ValueError as exc:
        raise ValueError("RESULT completed_at must be an ISO timestamp with timezone") from exc
    checks = record.get("checks")
    if not isinstance(checks, list):
        raise ValueError("RESULT checks must be a list")
    for check in checks:
        if not isinstance(check, dict) or not isinstance(check.get("name"), str) or not check["name"].strip():
            raise ValueError("RESULT check name is invalid")
        command = check.get("command")
        if not ((isinstance(command, str) and command.strip()) or
                (isinstance(command, list) and command and all(isinstance(arg, str) and arg for arg in command))):
            raise ValueError("RESULT check command must be text or a nonempty argv list")
        if check.get("outcome") not in {*RESULT_OUTCOMES, "SKIPPED"}:
            raise ValueError("RESULT check outcome is invalid")
        if record["outcome"] == "PASS" and check["outcome"] in {"FAIL", "BLOCKED"}:
            raise ValueError("a PASS RESULT cannot report failed or blocked checks")
        if check["outcome"] == "SKIPPED" and (not isinstance(check.get("reason"), str) or not check["reason"].strip()):
            raise ValueError("a skipped check requires its reason")
    if record.get("content_hash") != content_hash(record):
        raise ValueError("RESULT content hash differs from its bytes")
    tip = git_source.current_tip(lane)
    if record.get("branch") != lane["git_source"]["branch_ref"].removeprefix("refs/heads/"):
        raise ValueError("RESULT branch differs from its assignment")
    if not isinstance(record.get("commit"), str) or not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", record["commit"]) or record["commit"] != tip:
        raise ValueError("RESULT commit is not the exact current assigned tip")
    needs_clean = record["outcome"] == "PASS" if clean is None else clean
    if needs_clean:
        require_clean(lane, tip)
    return record


def read_result(lane: dict[str, Any], *, clean: bool | None = None) -> dict[str, Any]:
    worktree = Path(lane["worktree_path"])
    if Path(lane.get("result_path", worktree / "RESULT.json")).absolute() != worktree / "RESULT.json":
        raise ValueError("RESULT path is outside its assigned location")
    raw = anchored_read(worktree, "RESULT.json", max_bytes=MAX_RESULT_BYTES,
                        expected_root_identity=tuple(lane["worktree_identity"]))
    return validate_record(json.loads(raw), lane, clean=clean)


def stage_worker_support(worktree: Path, lane: dict[str, Any]) -> dict[str, Any]:
    from .stable_io import PreparedOutputTransaction
    output = PreparedOutputTransaction(worktree, allowed_roots=(worktree.parent,))
    output.prepare()
    if output.root_identity != tuple(lane["worktree_identity"]):
        raise ValueError("worker validation destination changed")
    directory = worktree / ".agent-workspace/validation-runtime/orchestrator_harness"
    for module in VALIDATION_MODULES:
        path = directory / (module + ".py")
        output.ensure_parent(path)
        output.publish_bytes(output.admit(path), (Path(__file__).parent / (module + ".py")).read_bytes())
    mirror = assignment(lane)
    path = worktree / ".agent-workspace/result-assignment.json"
    output.ensure_parent(path)
    output.publish_json(output.admit(path), mirror)
    template = {"schema": RESULT_SCHEMA, "lane_id": lane["lane_id"], "run_id": lane["run_id"],
                "worker_invocation_id": lane["run_id"], "task_card_hash": lane["task_card_hash"],
                "branch": lane["git_source"]["branch_ref"].removeprefix("refs/heads/"),
                "commit": "<full-current-assigned-branch-tip>", "outcome": "PASS", "summary": "",
                "checks": [], "evidence": [], "completed_at": "", "content_hash": ""}
    output.publish_json(output.admit(worktree / ".agent-workspace/result-template.json"), template)
    return mirror
