"""Read-only structural admission checks for one completed coding handoff."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

from .git_safety import (
    GitDeclaration,
    GitIdentity,
    GitSafetyError,
    declaration_from_invocation,
    inspect_repository,
    validate_coding_result,
)
from .resume import validate_resume_amendment_review

PREFLIGHT_SCHEMA = "orchestrator-handoff-preflight-result/v1"
PASS = "PASS"
REPORT_ONLY_ERROR = "REPORT_ONLY_ERROR"
INCOMPLETE = "INCOMPLETE"
EXIT_PASS = 0
EXIT_REPORT_ONLY_ERROR = 3
EXIT_INCOMPLETE = 4

_EXPECTED_SCHEMAS = {
    "task_card": "orchestrator-task-card/v1",
    "invocation": "orchestrator-coding-invocation/v1",
    "result": "orchestrator-lane-result/v1",
    "dependency_map": "orchestrator-dependency-map/v1",
}
_GIT_ENV_KEYS = (
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


def _failure(
    predicate: str, disposition: str, artifact: str, detail: str
) -> dict[str, str]:
    return {
        "predicate": predicate,
        "disposition": disposition,
        "artifact": artifact,
        "detail": detail,
    }


def _same_path(left: Path, right: Path) -> bool:
    left_text = str(left.resolve(strict=False))
    right_text = str(right.resolve(strict=False))
    if os.name == "nt":
        return os.path.normcase(left_text) == os.path.normcase(right_text)
    return left_text == right_text


def _inside(path: Path, root: Path) -> bool:
    try:
        _ = path.relative_to(root)
        return True
    except ValueError:
        return False


def _json_object(path: Path, *, max_bytes: int | None = 2 * 1024 * 1024) -> tuple[dict[str, object], str]:
    if path.is_symlink() or not path.is_file():
        raise ValueError("path is not a regular non-symlink file")
    data = path.read_bytes()
    if max_bytes is not None and len(data) > max_bytes:
        raise ValueError("artifact exceeds 2 MiB")
    decoded = cast(object, json.loads(data.decode("utf-8")))
    if not isinstance(decoded, dict):
        raise TypeError("JSON root is not an object")
    value = cast(dict[object, object], decoded)
    if any(not isinstance(key, str) for key in value):
        raise TypeError("JSON object keys must be strings")
    return cast(dict[str, object], value), hashlib.sha256(data).hexdigest()


def _mapping(value: object) -> Mapping[str, object] | None:
    return cast(Mapping[str, object], value) if isinstance(value, Mapping) else None


def _text(value: object) -> str | None:
    return (
        value if isinstance(value, str) and value and value.strip() == value else None
    )


def _git(worktree: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    environment = {key: os.environ[key] for key in _GIT_ENV_KEYS if key in os.environ}
    environment.update(
        {"GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0", "LC_ALL": "C"}
    )
    return subprocess.run(
        ["git", "-C", str(worktree), *args],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        timeout=15,
        env=environment,
        shell=False,
    )


def _dependency_endpoint(
    dependency_map: Mapping[str, object],
) -> tuple[str | None, str | None]:
    final_tip = _mapping(dependency_map.get("final_tip"))
    if final_tip is not None:
        return _text(final_tip.get("branch")), _text(final_tip.get("commit"))
    commit = _mapping(dependency_map.get("commit"))
    return (
        (_text(commit.get("branch")), _text(commit.get("sha")))
        if commit is not None
        else (None, None)
    )


def _starting_commit(
    task_card: Mapping[str, object],
    dependency_map: Mapping[str, object],
    declaration: GitDeclaration | None,
) -> str | None:
    starting_state = _mapping(task_card.get("starting_state"))
    starting_point = _mapping(dependency_map.get("starting_point"))
    return (
        (
            _text(starting_state.get("starting_commit"))
            if starting_state is not None
            else None
        )
        or (_text(starting_point.get("commit")) if starting_point is not None else None)
        or (declaration.base_commit if declaration is not None else None)
    )


def _check_invocation_files(
    invocation: Mapping[str, object],
    worktree: Path,
    failures: list[dict[str, str]],
) -> None:
    if not isinstance(invocation.get("run_root"), str) or not _same_path(
        Path(cast(str, invocation.get("run_root"))), worktree
    ):
        failures.append(
            _failure(
                "RUN_ROOT_IDENTITY",
                REPORT_ONLY_ERROR,
                "invocation",
                "run_root must identify the supplied worktree",
            )
        )
    workspace = (worktree / ".agent-workspace").resolve(strict=False)
    outputs = _mapping(invocation.get("output_paths"))
    for name in ("status", "jsonl", "stderr", "last_message"):
        value = _text(outputs.get(name)) if outputs is not None else None
        if value is None:
            failures.append(
                _failure(
                    "DECLARED_OUTPUT_PATH",
                    REPORT_ONLY_ERROR,
                    "invocation",
                    f"output_paths.{name} is missing",
                )
            )
            continue
        path = Path(value).expanduser().resolve(strict=False)
        if not _inside(path, workspace):
            failures.append(
                _failure(
                    "DECLARED_OUTPUT_CONFINEMENT",
                    REPORT_ONLY_ERROR,
                    "invocation",
                    f"output_paths.{name} escapes the evidence workspace",
                )
            )
        elif path.is_symlink() or not path.is_file():
            failures.append(
                _failure(
                    "DECLARED_OUTPUT_EXISTS",
                    INCOMPLETE,
                    "invocation",
                    f"output_paths.{name} is not an existing regular file",
                )
            )
    prompt_value = _text(invocation.get("prompt_path"))
    prompt_hash = _text(invocation.get("prompt_sha256"))
    if prompt_value is None or prompt_hash is None:
        failures.append(
            _failure(
                "PROMPT_IDENTITY",
                REPORT_ONLY_ERROR,
                "invocation",
                "prompt path/hash is missing",
            )
        )
        return
    prompt = Path(prompt_value).expanduser().resolve(strict=False)
    if prompt.is_symlink() or not prompt.is_file():
        failures.append(
            _failure(
                "PROMPT_EXISTS",
                INCOMPLETE,
                "invocation",
                "prompt is not an existing regular file",
            )
        )
    elif hashlib.sha256(prompt.read_bytes()).hexdigest() != prompt_hash.lower():
        failures.append(
            _failure(
                "PROMPT_CONTENT_IDENTITY",
                REPORT_ONLY_ERROR,
                "invocation",
                "prompt_sha256 does not match prompt bytes",
            )
        )


def _check_amendment_identity(
    *,
    invocation: Mapping[str, object],
    task_artifact_sha256: str | None,
    dependencies: Mapping[str, object],
    failures: list[dict[str, str]],
) -> None:
    """Bind a candidate-only amendment record without admitting semantics."""
    amendment = _mapping(dependencies.get("amendment"))
    if amendment is None:
        return
    review_path_value = _text(amendment.get("review_path"))
    review_hash = _text(amendment.get("review_sha256"))
    requested = _mapping(amendment.get("requested_identity"))
    persisted = _mapping(amendment.get("persisted_identity"))
    job_identity = _mapping(amendment.get("job_identity"))
    if (
        review_path_value is None
        or review_hash is None
        or requested is None
        or persisted is None
        or job_identity is None
    ):
        failures.append(
            _failure(
                "RESUME_AMENDMENT_IDENTITY",
                REPORT_ONLY_ERROR,
                "dependency_map",
                "amendment review path/hash and identity records are required",
            )
        )
        return
    review_path = Path(review_path_value).expanduser().resolve(strict=False)
    try:
        review, actual_hash = _json_object(review_path, max_bytes=None)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        failures.append(
            _failure("RESUME_AMENDMENT_IDENTITY", REPORT_ONLY_ERROR, "amendment", str(exc))
        )
        return
    if actual_hash != review_hash.lower():
        failures.append(
            _failure(
                "RESUME_AMENDMENT_IDENTITY",
                REPORT_ONLY_ERROR,
                "amendment",
                "review file hash does not match dependency map",
            )
        )
    pairs = _mapping(review.get("reviewed_pairs"))
    card_pair = _mapping(pairs.get("task_card")) if pairs is not None else None
    prompt_pair = _mapping(pairs.get("prompt")) if pairs is not None else None
    prompt_hash = _text(invocation.get("prompt_sha256"))
    if prompt_hash is None:
        bundle = _mapping(invocation.get("prompt_bundle"))
        prompt_hash = _text(bundle.get("final_sha256")) if bundle is not None else None
    if (
        task_artifact_sha256 is None
        or card_pair is None
        or _text(card_pair.get("new_sha256")) != task_artifact_sha256.lower()
        or prompt_pair is None
        or prompt_hash is None
        or _text(prompt_pair.get("new_sha256")) != prompt_hash.lower()
    ):
        failures.append(
            _failure(
                "RESUME_AMENDMENT_IDENTITY",
                REPORT_ONLY_ERROR,
                "amendment",
                "reviewed new raw identity does not match supplied candidate artifacts",
            )
        )
    declared_disposition = _text(amendment.get("disposition"))
    if declared_disposition is not None and declared_disposition != _text(review.get("disposition")):
        failures.append(
            _failure(
                "RESUME_AMENDMENT_IDENTITY",
                REPORT_ONLY_ERROR,
                "amendment",
                "dependency-map disposition does not match review",
            )
        )
    try:
        _ = validate_resume_amendment_review(
            review,
            requested,
            persisted,
            expected_job_identity=job_identity,
        )
    except (OSError, TypeError, ValueError) as exc:
        failures.append(
            _failure("RESUME_AMENDMENT_IDENTITY", REPORT_ONLY_ERROR, "amendment", str(exc))
        )


def _load_artifacts(
    paths: Mapping[str, Path], failures: list[dict[str, str]]
) -> tuple[dict[str, dict[str, object]], dict[str, dict[str, str]]]:
    loaded: dict[str, dict[str, object]] = {}
    evidence: dict[str, dict[str, str]] = {}
    for name, supplied in paths.items():
        path = supplied.expanduser().resolve(strict=False)
        try:
            value, digest = _json_object(path)
        except (
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
        ) as exc:
            failures.append(_failure("JSON_OBJECT", REPORT_ONLY_ERROR, name, str(exc)))
            continue
        loaded[name] = value
        evidence[name] = {"path": str(path), "sha256": digest}
        if value.get("schema") != _EXPECTED_SCHEMAS[name]:
            failures.append(
                _failure(
                    "DECLARED_SCHEMA",
                    REPORT_ONLY_ERROR,
                    name,
                    f"schema must be {_EXPECTED_SCHEMAS[name]}",
                )
            )
    return loaded, evidence


def _check_required_evidence(
    root: Path,
    paths: Sequence[Path],
    failures: list[dict[str, str]],
) -> list[dict[str, str]]:
    admitted: list[dict[str, str]] = []
    for supplied in paths:
        candidate = supplied if supplied.is_absolute() else root / supplied
        resolved = candidate.expanduser().resolve(strict=False)
        if not _inside(resolved, root):
            failures.append(
                _failure(
                    "REQUIRED_EVIDENCE_CONFINEMENT",
                    INCOMPLETE,
                    "required_evidence",
                    f"{supplied} escapes the admitted evidence root",
                )
            )
        elif candidate.is_symlink() or not resolved.is_file():
            failures.append(
                _failure(
                    "REQUIRED_EVIDENCE_EXISTS",
                    INCOMPLETE,
                    "required_evidence",
                    f"{supplied} is not an existing regular file",
                )
            )
        else:
            admitted.append(
                {
                    "path": str(resolved),
                    "sha256": hashlib.sha256(resolved.read_bytes()).hexdigest(),
                }
            )
    return admitted


def preflight_handoff(
    *,
    task_card_path: Path,
    invocation_path: Path,
    result_path: Path,
    dependency_map_path: Path,
    worktree: Path,
    evidence_root: Path,
    required_evidence: Sequence[Path] = (),
) -> dict[str, object]:
    """Return a machine disposition without changing source or runtime state."""
    failures: list[dict[str, str]] = []
    worktree = worktree.expanduser().resolve(strict=False)
    evidence_root = evidence_root.expanduser().resolve(strict=False)
    if not worktree.is_dir():
        failures.append(
            _failure("WORKTREE_EXISTS", INCOMPLETE, "worktree", "directory is missing")
        )
    if not evidence_root.is_dir():
        failures.append(
            _failure(
                "EVIDENCE_ROOT_EXISTS",
                INCOMPLETE,
                "evidence_root",
                "directory is missing",
            )
        )
    loaded, artifacts = _load_artifacts(
        {
            "task_card": task_card_path,
            "invocation": invocation_path,
            "result": result_path,
            "dependency_map": dependency_map_path,
        },
        failures,
    )
    task = loaded.get("task_card", {})
    invocation = loaded.get("invocation", {})
    result = loaded.get("result", {})
    dependencies = loaded.get("dependency_map", {})
    if invocation and worktree.is_dir():
        _check_invocation_files(invocation, worktree, failures)
    if invocation and task and dependencies:
        _check_amendment_identity(
            invocation=invocation,
            task_artifact_sha256=artifacts.get("task_card", {}).get("sha256"),
            dependencies=dependencies,
            failures=failures,
        )

    declaration: GitDeclaration | None = None
    identity: GitIdentity | None = None
    if invocation and worktree.is_dir():
        try:
            declaration = declaration_from_invocation(invocation, worktree)
        except GitSafetyError as exc:
            failures.append(
                _failure(
                    "REPOSITORY_DECLARATION", REPORT_ONLY_ERROR, "invocation", str(exc)
                )
            )
        if declaration is not None:
            try:
                identity = inspect_repository(declaration)
            except GitSafetyError as exc:
                failures.append(
                    _failure("WORKTREE_IDENTITY", INCOMPLETE, "worktree", str(exc))
                )

    dependency_task = _mapping(dependencies.get("task"))
    identity_sets = {
        "lane_id": (
            _text(task.get("lane_id")),
            _text(invocation.get("lane_id")),
            _text(result.get("lane_id")),
            _text(dependency_task.get("lane_id")) if dependency_task else None,
        ),
        "worker_invocation_id": (
            _text(task.get("worker_invocation_id")),
            _text(invocation.get("worker_invocation_id")),
            _text(result.get("worker_invocation_id")),
            _text(dependency_task.get("worker_invocation_id"))
            if dependency_task
            else None,
        ),
        "card_id": (
            _text(task.get("card_id")),
            _text(dependency_task.get("card_id")) if dependency_task else None,
        ),
    }
    for name, values in identity_sets.items():
        if any(value is None for value in values) or len(set(values)) != 1:
            failures.append(
                _failure(
                    "HANDOFF_IDENTITY",
                    REPORT_ONLY_ERROR,
                    "handoff",
                    f"{name} is missing or differs across artifacts",
                )
            )

    if dependency_task is None:
        failures.append(
            _failure(
                "DEPENDENCY_TASK_REFERENCE",
                REPORT_ONLY_ERROR,
                "dependency_map",
                "task reference is missing",
            )
        )
    else:
        task_path = _text(dependency_task.get("card_path"))
        task_hash = _text(dependency_task.get("card_sha256"))
        if (
            task_path is None
            or not _same_path(Path(task_path), task_card_path)
            or task_hash is None
            or task_hash.lower() != artifacts.get("task_card", {}).get("sha256")
        ):
            failures.append(
                _failure(
                    "TASK_CARD_CONTENT_IDENTITY",
                    REPORT_ONLY_ERROR,
                    "dependency_map",
                    "task card path/hash does not match the supplied card",
                )
            )

    dependency_branch, dependency_commit = _dependency_endpoint(dependencies)
    declared_branch = declaration.branch if declaration is not None else None
    starting_state = _mapping(task.get("starting_state"))
    task_start_branch = (
        _text(starting_state.get("branch")) if starting_state is not None else None
    )
    if (
        task_start_branch is not None
        and declared_branch is not None
        and task_start_branch != declared_branch
    ):
        failures.append(
            _failure(
                "START_BRANCH_IDENTITY",
                REPORT_ONLY_ERROR,
                "task_card",
                "starting_state.branch does not match the invocation branch",
            )
        )
    final_branches = (declared_branch, _text(result.get("branch")), dependency_branch)
    final_commits = tuple(
        value.lower() if value is not None else None
        for value in (_text(result.get("commit")), dependency_commit)
    )
    for predicate, values in (
        ("FINAL_BRANCH_IDENTITY", final_branches),
        ("FINAL_COMMIT_IDENTITY", final_commits),
    ):
        if any(value is None for value in values) or len(set(values)) != 1:
            failures.append(
                _failure(
                    predicate,
                    REPORT_ONLY_ERROR,
                    "handoff",
                    "final identities are missing or differ",
                )
            )

    lane_id = _text(invocation.get("lane_id"))
    worker_id = _text(invocation.get("worker_invocation_id"))
    if result and declaration is not None and identity is not None:
        try:
            _ = validate_coding_result(
                result,
                lane_id=lane_id or "",
                worker_invocation_id=worker_id or "",
                declaration=declaration,
            )
        except GitSafetyError as exc:
            disposition = (
                INCOMPLETE if "requires a clean" in str(exc) else REPORT_ONLY_ERROR
            )
            failures.append(
                _failure("RESULT_ENVELOPE", disposition, "result", str(exc))
            )

    starting_commit = _starting_commit(task, dependencies, declaration)
    if identity is not None and starting_commit is not None:
        try:
            status = _git(
                worktree,
                "status",
                "--porcelain=v1",
                "-z",
                "--untracked-files=all",
                "--",
                ".",
            )
            ancestor = _git(
                worktree,
                "merge-base",
                "--is-ancestor",
                starting_commit,
                identity.head_commit,
            )
            diff = _git(
                worktree,
                "diff",
                "--check",
                starting_commit,
                identity.head_commit,
                "--",
                ".",
            )
            if status.returncode != 0 or status.stdout:
                failures.append(
                    _failure(
                        "WORKTREE_CLEAN",
                        INCOMPLETE,
                        "worktree",
                        "project worktree is dirty or cannot be inspected",
                    )
                )
            if ancestor.returncode != 0:
                failures.append(
                    _failure(
                        "STARTING_COMMIT_ANCESTRY",
                        INCOMPLETE,
                        "worktree",
                        "starting commit is not an ancestor of HEAD",
                    )
                )
            if diff.returncode != 0:
                detail = (diff.stderr or diff.stdout).decode("utf-8", errors="replace")[
                    :500
                ]
                failures.append(
                    _failure(
                        "COMMITTED_DIFF_WHITESPACE",
                        INCOMPLETE,
                        "worktree",
                        detail.strip(),
                    )
                )
        except (OSError, subprocess.TimeoutExpired) as exc:
            failures.append(
                _failure("COMMITTED_DIFF_INSPECTION", INCOMPLETE, "worktree", str(exc))
            )
    elif identity is not None:
        failures.append(
            _failure(
                "STARTING_COMMIT_DECLARED",
                REPORT_ONLY_ERROR,
                "handoff",
                "no starting commit is declared",
            )
        )

    admitted = (
        _check_required_evidence(evidence_root, required_evidence, failures)
        if evidence_root.is_dir()
        else []
    )
    disposition = (
        INCOMPLETE
        if any(item["disposition"] == INCOMPLETE for item in failures)
        else REPORT_ONLY_ERROR
        if failures
        else PASS
    )
    return {
        "schema": PREFLIGHT_SCHEMA,
        "disposition": disposition,
        "failed_predicates": failures,
        "artifacts": artifacts,
        "required_evidence": admitted,
        "identity": {
            "lane_id": lane_id,
            "worker_invocation_id": worker_id,
            "branch": identity.branch if identity is not None else declared_branch,
            "starting_commit": starting_commit,
            "commit": identity.head_commit
            if identity is not None
            else _text(result.get("commit")),
        },
    }


def exit_code(result: Mapping[str, object]) -> int:
    if result.get("disposition") == PASS:
        return EXIT_PASS
    if result.get("disposition") == REPORT_ONLY_ERROR:
        return EXIT_REPORT_ONLY_ERROR
    return EXIT_INCOMPLETE
