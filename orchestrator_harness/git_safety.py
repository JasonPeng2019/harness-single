"""Shell-free Git identity and coding-result checks for one declared worktree."""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .models import parse_utc
from .processes import WINDOWS_CREATE_NO_WINDOW, ProcessSnapshot, process_snapshot


class GitSafetyError(ValueError):
    pass


@dataclass(frozen=True)
class GitDeclaration:
    common_dir: Path
    worktree_root: Path
    branch: str
    base_commit: str


@dataclass(frozen=True)
class GitIdentity:
    common_dir: Path
    worktree_root: Path
    branch: str
    base_commit: str
    head_commit: str


_HEX_COMMIT = re.compile(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}")
_RESULT_OUTCOMES = {"PASS", "FAIL", "BLOCKED"}
_CHECK_OUTCOMES = {"PASS", "FAIL", "SKIP", "NOT_RUN"}
_MAX_STATUS_BYTES = 256 * 1024
_MAX_WORKTREES = 256
_MAX_JSON_CANDIDATES_PER_WORKTREE = 256
_MAX_FINDINGS_BYTES = 256 * 1024
_GIT_INSPECTION_ENV_KEYS = (
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "WINDIR",
    "COMSPEC",
    "SYSTEMDRIVE",
    "TEMP",
    "TMP",
    "TMPDIR",
)


def _git_inspection_env() -> dict[str, str]:
    env = {
        key: os.environ[key]
        for key in _GIT_INSPECTION_ENV_KEYS
        if key in os.environ
    }
    env.update({"GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0", "LC_ALL": "C"})
    return env


def validate_findings(path: Path, *, lane_id: str, worker_invocation_id: str, role: str, commit: str, outcome: str) -> dict[str, Any]:
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > _MAX_FINDINGS_BYTES:
            raise GitSafetyError("finding gate path is unsafe or oversized")
        raw = json.loads(path.read_bytes().decode("utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GitSafetyError(f"finding gate cannot read findings: {exc}") from exc
    required = {"schema", "lane_id", "worker_invocation_id", "role", "commit", "findings"}
    if not isinstance(raw, dict) or set(raw) != required or raw.get("schema") != "orchestrator-review-findings/v1":
        raise GitSafetyError("finding gate has invalid closed schema")
    if (raw.get("lane_id"), raw.get("worker_invocation_id"), raw.get("role"), raw.get("commit")) != (lane_id, worker_invocation_id, role, commit):
        raise GitSafetyError("finding gate identity mismatch")
    findings = raw.get("findings")
    if not isinstance(findings, list) or len(findings) > 32 or (outcome == "PASS" and findings) or (outcome == "FAIL" and not findings):
        raise GitSafetyError("finding gate outcome/count mismatch")
    ids: set[str] = set()
    required_finding = {"id", "category", "affected_ids", "evidence", "observed", "expected", "reproduction", "impact", "no_fix_consequence", "smallest_fix", "complexity", "regression_risk", "verification_cost", "alternatives", "cost_benefit", "problem_outweighs_fix_risk"}
    for finding in findings:
        if not isinstance(finding, dict) or set(finding) != required_finding or finding.get("category") not in {"CODEBASE_BREAKING", "FUNCTIONALITY_BREAKING", "WORTH_FIXING"} or finding.get("problem_outweighs_fix_risk") is not True:
            raise GitSafetyError("finding gate finding is malformed or inadmissible")
        identifier = finding.get("id")
        if not isinstance(identifier, str) or not identifier or identifier in ids:
            raise GitSafetyError("finding gate IDs must be unique")
        ids.add(identifier)
        if any(not isinstance(finding.get(key), str) or not finding[key].strip() or len(finding[key]) > 4000 for key in required_finding - {"id", "category", "affected_ids", "evidence", "problem_outweighs_fix_risk"}):
            raise GitSafetyError("finding gate tradeoff evidence is incomplete")
        if not isinstance(finding.get("affected_ids"), list) or not finding["affected_ids"] or len(finding["affected_ids"]) > 64 or any(not isinstance(item, str) or not item or len(item) > 200 for item in finding["affected_ids"]):
            raise GitSafetyError("finding gate affected IDs are invalid")
        if not isinstance(finding.get("evidence"), list) or not finding["evidence"] or len(finding["evidence"]) > 32 or any(not isinstance(item, str) or not item or len(item) > 1000 for item in finding["evidence"]):
            raise GitSafetyError("finding gate evidence references are invalid")
    return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "count": len(findings)}


def validate_finding_triage(value: Mapping[str, Any], *, findings_path: Path, findings_sha256: str, finding_ids: set[str]) -> None:
    required = {"schema", "owner", "findings_path", "findings_sha256", "decisions"}
    if set(value) != required or value.get("schema") != "orchestrator-review-triage/v1" or value.get("owner") not in {"ROOT-IM", "F.C3.O"}:
        raise GitSafetyError("triage has invalid closed schema or owner")
    if findings_path.is_symlink() or not findings_path.is_file() or findings_path.stat().st_size > _MAX_FINDINGS_BYTES:
        raise GitSafetyError("triage findings path is unsafe or oversized")
    actual_hash = hashlib.sha256(findings_path.read_bytes()).hexdigest()
    if value.get("findings_path") != str(findings_path) or value.get("findings_sha256") != actual_hash or findings_sha256 != actual_hash:
        raise GitSafetyError("triage findings reference mismatch")
    decisions = value.get("decisions")
    if not isinstance(decisions, list) or len(decisions) != len(finding_ids):
        raise GitSafetyError("triage must decide every submitted finding exactly once")
    seen: set[str] = set()
    for item in decisions:
        if not isinstance(item, dict) or item.get("id") not in finding_ids or item.get("id") in seen or item.get("decision") not in {"ACCEPT", "REJECT"} or not isinstance(item.get("rationale"), str) or not item["rationale"].strip() or len(item["rationale"]) > 4000:
            raise GitSafetyError("triage decision is invalid")
        seen.add(item["id"])
        if item["decision"] == "ACCEPT" and (set(item) != {"id", "decision", "rationale", "conclusion", "smallest_fix"} or item.get("conclusion") != "PROBLEM_OUTWEIGHS_FIX_RISK" or not isinstance(item.get("smallest_fix"), str) or not item["smallest_fix"].strip() or len(item["smallest_fix"]) > 4000):
            raise GitSafetyError("accepted triage requires conclusion and smallest fix")
        if item["decision"] == "REJECT" and (set(item) != {"id", "decision", "rationale", "reason"} or item.get("reason") not in {"unsupported", "not_reproducible", "out_of_scope", "net_negative_complexity"}):
            raise GitSafetyError("rejected triage requires a bounded reason")


def _normalized_path(path: Path) -> str:
    resolved = str(path.resolve(strict=False))
    return os.path.normcase(resolved) if os.name == "nt" else resolved


def _same_path(left: Path, right: Path) -> bool:
    return _normalized_path(left) == _normalized_path(right)


def _declared_path(value: object, name: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise GitSafetyError(f"repository.{name} must be a non-empty path string")
    try:
        path = Path(value).expanduser().resolve(strict=True)
    except OSError as exc:
        raise GitSafetyError(f"repository.{name} cannot be resolved: {exc}") from exc
    if not path.is_dir():
        raise GitSafetyError(f"repository.{name} must be an existing directory")
    return path


def declaration_from_invocation(raw: Mapping[str, Any], run_root: Path) -> GitDeclaration:
    value = raw.get("repository", raw.get("git"))
    if not isinstance(value, Mapping):
        raise GitSafetyError("repository must be an object")
    common_dir = _declared_path(value.get("common_dir"), "common_dir")
    worktree_root = _declared_path(value.get("worktree_root"), "worktree_root")
    if not _same_path(worktree_root, run_root):
        raise GitSafetyError("repository.worktree_root must equal run_root")
    branch = value.get("branch")
    if not isinstance(branch, str) or not branch.strip() or branch != branch.strip():
        raise GitSafetyError("repository.branch must be a non-empty trimmed string")
    base_commit = value.get("base_commit")
    if not isinstance(base_commit, str) or _HEX_COMMIT.fullmatch(base_commit) is None:
        raise GitSafetyError("repository.base_commit must be a full hexadecimal commit ID")
    return GitDeclaration(common_dir, worktree_root, branch, base_commit.lower())


def declaration_from_status(raw: Mapping[str, Any], run_root: Path) -> GitDeclaration:
    nested = raw.get("repository")
    value: Mapping[str, Any]
    if isinstance(nested, Mapping):
        value = nested
    else:
        value = {
            "common_dir": raw.get("repository_common_dir"),
            "worktree_root": raw.get("worktree_root"),
            "branch": raw.get("branch"),
            "base_commit": raw.get("base_commit"),
        }
    return declaration_from_invocation({"repository": value}, run_root)


def _git(cwd: Path, *args: str, allow_failure: bool = False) -> str | None:
    env = _git_inspection_env()
    try:
        completed = subprocess.run(
            ["git", "-C", str(cwd), *args],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=10,
            env=env,
            shell=False,
            creationflags=WINDOWS_CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise GitSafetyError(f"Git inspection failed: {exc}") from exc
    if completed.returncode != 0:
        if allow_failure:
            return None
        detail = completed.stderr.decode("utf-8", errors="replace").strip()[:300]
        raise GitSafetyError(f"Git inspection failed ({completed.returncode}): {detail}")
    try:
        return completed.stdout.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise GitSafetyError("Git inspection returned non-UTF-8 output") from exc


def inspect_repository(declaration: GitDeclaration) -> GitIdentity:
    top_text = _git(declaration.worktree_root, "rev-parse", "--show-toplevel")
    common_text = _git(declaration.worktree_root, "rev-parse", "--git-common-dir")
    if top_text is None or common_text is None:
        raise GitSafetyError("Git did not report repository identity")
    actual_worktree = Path(top_text).resolve(strict=False)
    common_candidate = Path(common_text)
    if not common_candidate.is_absolute():
        common_candidate = declaration.worktree_root / common_candidate
    actual_common = common_candidate.resolve(strict=False)
    if not _same_path(actual_worktree, declaration.worktree_root):
        raise GitSafetyError("declared worktree_root is not the actual Git worktree root")
    if not _same_path(actual_common, declaration.common_dir):
        raise GitSafetyError("declared common_dir is not the actual Git common directory")
    branch = _git(declaration.worktree_root, "symbolic-ref", "--quiet", "--short", "HEAD", allow_failure=True)
    if branch is None:
        raise GitSafetyError("coding worktree must have an attached branch")
    if branch != declaration.branch:
        raise GitSafetyError(f"coding worktree branch {branch!r} does not match declared branch {declaration.branch!r}")
    base = _git(declaration.worktree_root, "rev-parse", "--verify", f"{declaration.base_commit}^{{commit}}")
    head = _git(declaration.worktree_root, "rev-parse", "--verify", "HEAD^{commit}")
    if base is None or head is None or _HEX_COMMIT.fullmatch(base) is None or _HEX_COMMIT.fullmatch(head) is None:
        raise GitSafetyError("Git returned an invalid commit identity")
    return GitIdentity(actual_common, actual_worktree, branch, base.lower(), head.lower())


def repository_status(identity: GitIdentity, *, starting_commit: str | None = None) -> dict[str, str]:
    return {
        "common_dir": str(identity.common_dir),
        "worktree_root": str(identity.worktree_root),
        "branch": identity.branch,
        "base_commit": identity.base_commit,
        "starting_commit": starting_commit or identity.head_commit,
    }


def _worktree_roots(declaration: GitDeclaration) -> list[Path]:
    output = _git(declaration.worktree_root, "worktree", "list", "--porcelain", "-z") or ""
    roots: list[Path] = []
    for token in output.split("\0"):
        if token.startswith("worktree "):
            roots.append(Path(token[len("worktree ") :]).resolve(strict=False))
            if len(roots) > _MAX_WORKTREES:
                raise GitSafetyError("too many registered Git worktrees to inspect safely")
    return roots


def _creation_matches(value: object, actual: object) -> bool:
    expected = parse_utc(value)
    created = getattr(actual, "created_utc", None)
    return expected is not None and created is not None and abs((created - expected).total_seconds()) <= 2.0


def _trustworthy_live_status(raw: Mapping[str, Any], snapshot: ProcessSnapshot) -> bool:
    if (
        raw.get("schema") != "orchestrator-lane-controller/v1"
        or raw.get("invocation_schema") not in {
            "orchestrator-coding-invocation/v1",
            "orchestrator-worker-invocation/v1",
        }
        or str(raw.get("state", "")).upper() not in {"RUNNING_CODEX", "RUNNING_PROVIDER"}
        or not snapshot.complete
    ):
        return False
    controller_pid = raw.get("controller_pid")
    provider_pid = raw.get("provider_pid") or raw.get("codex_pid")
    if not isinstance(controller_pid, int) or isinstance(controller_pid, bool):
        return False
    if not isinstance(provider_pid, int) or isinstance(provider_pid, bool):
        return False
    controller = snapshot.by_pid.get(controller_pid)
    provider = snapshot.by_pid.get(provider_pid)
    return bool(
        controller is not None
        and provider is not None
        and provider.ppid == controller.pid
        and _creation_matches(raw.get("controller_created_utc", raw.get("controller_started_utc")), controller)
        and _creation_matches(
            raw.get("provider_created_utc")
            or raw.get("codex_created_utc")
            or raw.get("provider_started_utc")
            or raw.get("codex_started_utc"),
            provider,
        )
    )


def active_declaration_conflicts(
    declaration: GitDeclaration,
    *,
    current_status_path: Path,
    snapshot: ProcessSnapshot | None = None,
) -> list[str]:
    processes = snapshot or process_snapshot()
    conflicts: list[str] = []
    current = _normalized_path(current_status_path)
    for root in _worktree_roots(declaration):
        workspace = root / ".agent-workspace"
        if not workspace.is_dir():
            continue
        paths = sorted(workspace.glob("*.json"))
        if len(paths) > _MAX_JSON_CANDIDATES_PER_WORKTREE:
            raise GitSafetyError(f"too many JSON candidates under {workspace}")
        for path in paths:
            if _normalized_path(path) == current or path.is_symlink() or not path.is_file():
                continue
            try:
                data = path.read_bytes()
                if len(data) > _MAX_STATUS_BYTES:
                    continue
                raw = json.loads(data.decode("utf-8-sig"))
                if not isinstance(raw, Mapping) or not _trustworthy_live_status(raw, processes):
                    continue
                other = declaration_from_status(raw, root)
                actual = inspect_repository(other)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, GitSafetyError):
                continue
            duplicate = []
            if _same_path(actual.worktree_root, declaration.worktree_root):
                duplicate.append("worktree")
            if _same_path(actual.common_dir, declaration.common_dir) and actual.branch == declaration.branch:
                duplicate.append("branch")
            if duplicate:
                conflicts.append(f"{'+'.join(duplicate)} already ACTIVE in {path}")
    return sorted(conflicts)


def _bounded_text(value: object, name: str, limit: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise GitSafetyError(f"{name} must be a non-empty string of at most {limit} characters")
    return value


def validate_coding_result(
    value: Mapping[str, Any],
    *,
    lane_id: str,
    worker_invocation_id: str,
    declaration: GitDeclaration,
) -> GitIdentity:
    allowed_result_keys = {
        "schema",
        "lane_id",
        "worker_invocation_id",
        "branch",
        "commit",
        "outcome",
        "summary",
        "checks",
    }
    if any(key not in allowed_result_keys for key in value):
        raise GitSafetyError("coding result has an invalid root shape")
    if value.get("schema") != "orchestrator-lane-result/v1":
        raise GitSafetyError("coding result schema must be orchestrator-lane-result/v1")
    if value.get("lane_id") != lane_id:
        raise GitSafetyError("coding result lane_id does not match the current lane")
    if value.get("worker_invocation_id") != worker_invocation_id:
        raise GitSafetyError("coding result worker_invocation_id does not match the current invocation")
    if value.get("branch") != declaration.branch:
        raise GitSafetyError("coding result branch does not match the declared branch")
    commit = value.get("commit")
    if not isinstance(commit, str) or _HEX_COMMIT.fullmatch(commit) is None:
        raise GitSafetyError("coding result commit must be a full hexadecimal commit ID")
    outcome = value.get("outcome")
    if outcome not in _RESULT_OUTCOMES:
        raise GitSafetyError("coding result outcome must be PASS, FAIL, or BLOCKED")
    _bounded_text(value.get("summary"), "coding result summary", 4000)
    checks = value.get("checks")
    if not isinstance(checks, list) or len(checks) > 64:
        raise GitSafetyError("coding result checks must be a list with at most 64 entries")
    allowed = {"name", "command", "outcome", "status", "summary"}
    for index, check in enumerate(checks):
        if not isinstance(check, Mapping) or any(key not in allowed for key in check):
            raise GitSafetyError(f"coding result check {index} has an invalid shape")
        if "name" not in check and "command" not in check:
            raise GitSafetyError(f"coding result check {index} requires name or command")
        if "name" in check:
            _bounded_text(check["name"], f"coding result check {index} name", 200)
        if "command" in check:
            _bounded_text(check["command"], f"coding result check {index} command", 1000)
        check_outcome = check.get("outcome", check.get("status"))
        if check_outcome not in _CHECK_OUTCOMES:
            raise GitSafetyError(f"coding result check {index} outcome must be PASS, FAIL, SKIP, or NOT_RUN")
        if "outcome" in check and "status" in check and check["outcome"] != check["status"]:
            raise GitSafetyError(f"coding result check {index} has conflicting outcome and status")
        if "summary" in check:
            _bounded_text(check["summary"], f"coding result check {index} summary", 2000)
    identity = inspect_repository(declaration)
    if commit.lower() != identity.head_commit:
        raise GitSafetyError("coding result commit does not equal the current branch tip")
    dirty = _git(
        declaration.worktree_root,
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
        "--",
        ".",
    )
    if dirty:
        raise GitSafetyError("coding result requires a clean project worktree (ignored runtime state is excluded)")
    return identity


def invalid_result_evidence(path: Path, detail: str, *, sha256: str | None = None) -> dict[str, Any]:
    return {
        "state": "INVALID",
        "code": "CODING_RESULT_INVALID",
        "path": str(path),
        "sha256": sha256,
        "detail": detail[:500],
    }


def validate_task_result_repository(
    value: Mapping[str, Any],
    *,
    card: Any,
    declaration: GitDeclaration,
    raw_bytes: bytes | None = None,
) -> Any:
    """Apply the existing branch/tip/cleanliness gate to a canonical result."""

    from .task import validate_task_result

    result = validate_task_result(value, card=card, raw_bytes=raw_bytes)
    identity = inspect_repository(declaration)
    if result.branch != identity.branch:
        raise GitSafetyError("task result branch does not match the declared branch")
    if result.commit != identity.head_commit:
        raise GitSafetyError("task result commit does not equal the current branch tip")
    dirty = _git(
        declaration.worktree_root,
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
        "--",
        ".",
    )
    if dirty:
        raise GitSafetyError("task result requires a clean project worktree (ignored runtime state is excluded)")
    return result


# S4 public lifecycle spellings live in one focused module but are re-exported
# here because Git identity is the ownership boundary callers already use.
from .lane_lifecycle import (  # noqa: E402
    allocate_immutable_source_view,
    allocate_immutable_view,
    allocate_source_view,
    retire_lane,
    retire_terminal_lane,
)
