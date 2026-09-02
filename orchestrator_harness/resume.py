"""``resume-lane``: re-run a stopped, unaccepted lane in its same worktree and
provider session with a fresh ``run_id``.

Resume is a short program, not an agent.  It re-does work: it clears the prior
run's obsolete current state, writes a fresh invocation for the new run, and
leaves the lane ready for ``lane launch``.  It never reconstructs a session,
PID, worktree, or amendment/hash record.
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from .bootstrap import (
    _write_invocation,
    _write_result_template,
    _write_worker_binding,
    _write_worker_prompt,
)
from .config import find_harness_root, load_config
from .core import iso_utc, new_id, read_json, require_schema
from .epochs import lane_record_dir
from .lanes import find_active_lane, update_lane
from .records import atomic_write_json, remove_record

TASK_CARD_SCHEMA = "project-task-card/v1"
INVOCATION_SCHEMA = "controller-invocation/v1"
OVERLAY_RECEIPT_SCHEMA = "overlay-receipt/v1"
LANE_INBOX_SCHEMA = "lane-inbox/v1"
COMPLETION_REVIEW_SCHEMA = "completion-review/v1"
ACCEPTANCE_SCHEMA = "orchestrator-acceptance/v1"

ALREADY_ACCEPTED = "ALREADY_ACCEPTED"
LANE_RUNNING = "LANE_RUNNING"
RESUME_WORKTREE_MISSING = "RESUME_WORKTREE_MISSING"
NO_SAVED_SESSION_ID = "NO_SAVED_SESSION_ID"
INVALID_RESUME_TASK_CARD = "INVALID_RESUME_TASK_CARD"
RESUME_LANE_WRITE_FAILED = "RESUME_LANE_WRITE_FAILED"

_RESUMABLE_LIFECYCLES = frozenset(
    {"review_pending", "result_invalid", "blocked", "abandoned"}
)


def _read_task_card(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"resume task card missing: {path}")
    record = read_json(path)
    require_schema(record, TASK_CARD_SCHEMA, path)
    task = record.get("task")
    if not isinstance(task, str) or not task.strip():
        raise ValueError("resume task card has no task text")
    return record


def _has_valid_acceptance_chain(
    rt: Path, epoch_id: str, lane: dict[str, Any]
) -> bool:
    """Return whether a complete, linked ACCEPTED chain exists for this run."""
    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    review_path = folder / "COMPLETION_REVIEW.json"
    acceptance_path = folder / "ORCHESTRATOR_ACCEPTANCE.json"
    if not review_path.is_file() or not acceptance_path.is_file():
        return False
    try:
        review = read_json(review_path)
        acceptance = read_json(acceptance_path)
        require_schema(review, COMPLETION_REVIEW_SCHEMA, review_path)
        require_schema(acceptance, ACCEPTANCE_SCHEMA, acceptance_path)
    except (OSError, ValueError):
        return False
    if review.get("run_id") != lane.get("run_id"):
        return False
    if acceptance.get("run_id") != lane.get("run_id"):
        return False
    if acceptance.get("review_ref") != review.get("content_hash"):
        return False
    return acceptance.get("approval") == "ACCEPTED"


def _clear_prior_run(rt: Path, epoch_id: str, lane: dict[str, Any]) -> None:
    """Remove the prior run's obsolete current state (best-effort, honest)."""
    worktree = Path(lane["worktree_path"])
    remove_record(worktree / "RESULT.json")
    folder = lane_record_dir(rt, epoch_id, lane["lane_id"])
    remove_record(folder / "COMPLETION_REVIEW.json")
    remove_record(folder / "ORCHESTRATOR_ACCEPTANCE.json")
    remove_record(worktree / ".agent-workspace" / "controller.status.json")


def _reset_worker_inbox(worktree: Path, lane_id: str, run_id: str) -> None:
    """Reset the managed worker inbox to a valid empty queue for the new run."""
    inbox = {
        "schema": LANE_INBOX_SCHEMA,
        "lane_id": lane_id,
        "run_id": run_id,
        "assignments": [],
    }
    atomic_write_json(worktree / ".agent-workspace" / "QUEUE.json", inbox)


def _rewrite_overlay_receipt(
    worktree: Path, lane: dict[str, Any], run_id: str, *, managed: bool
) -> None:
    receipt = {
        "schema": OVERLAY_RECEIPT_SCHEMA,
        "lane_id": lane["lane_id"],
        "run_id": run_id,
        "profile": "managed" if managed else "plain",
        "base_cache_ref": "super-cache/workspace",
        "applied_at": iso_utc(),
    }
    if managed:
        receipt["provider_payload"] = f"adapter-payloads/{lane['provider']['id']}"
    atomic_write_json(worktree / ".agent-workspace" / "overlay-receipt.json", receipt)


def run_resume(
    *,
    lane_id: str,
    resume_task_card: str,
    rationale: str | None = None,
) -> dict[str, Any]:
    """Execute ``resume-lane`` and return the structured result."""
    try:
        harness_root = find_harness_root()
        config = load_config(harness_root)
    except Exception as exc:
        return {
            "ok": False,
            "code": RESUME_LANE_WRITE_FAILED,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "fix the configuration and re-run setup",
        }
    rt = config.runtime_root
    try:
        epoch_id, lane = find_active_lane(rt, lane_id)
    except Exception as exc:
        return {
            "ok": False,
            "code": RESUME_LANE_WRITE_FAILED,
            "summary": f"lane not found: {lane_id}",
            "evidence_paths": [],
            "next_action": "check the lane id or bootstrap a fresh lane",
        }

    try:
        if _has_valid_acceptance_chain(rt, epoch_id, lane):
            return {
                "ok": False,
                "code": ALREADY_ACCEPTED,
                "summary": f"lane {lane_id} already has a valid ACCEPTED chain; it is not re-resumed",
                "evidence_paths": [
                    str(lane_record_dir(rt, epoch_id, lane_id) / "ORCHESTRATOR_ACCEPTANCE.json")
                ],
                "next_action": "retire the lane with `lane retire --acceptance-ref <file>`",
            }
        lifecycle = lane.get("lifecycle")
        if lifecycle == "accepted":
            return {
                "ok": False,
                "code": ALREADY_ACCEPTED,
                "summary": f"lane {lane_id} is accepted; it is not re-resumed",
                "evidence_paths": [],
                "next_action": "retire the lane with `lane retire --acceptance-ref <file>`",
            }
        if lifecycle == "retired":
            return {
                "ok": False,
                "code": RESUME_LANE_WRITE_FAILED,
                "summary": f"lane {lane_id} is retired; resume is not a cleanup tool",
                "evidence_paths": [],
                "next_action": "bootstrap a fresh lane",
            }
        process = lane.get("process") or {}
        from . import processes

        if lifecycle == "running" and processes.identity_matches(
            process.get("pid"), process.get("creation_time")
        ):
            return {
                "ok": False,
                "code": LANE_RUNNING,
                "summary": f"lane {lane_id} is still active; it is not resumed",
                "evidence_paths": [],
                "next_action": "wait for the lane to stop, or force-stop it first",
            }
        if lifecycle not in _RESUMABLE_LIFECYCLES and lifecycle != "running":
            return {
                "ok": False,
                "code": RESUME_LANE_WRITE_FAILED,
                "summary": f"lane {lane_id} is not resumable (lifecycle={lifecycle})",
                "evidence_paths": [],
                "next_action": "bootstrap a fresh lane",
            }

        worktree = Path(lane["worktree_path"])
        if not worktree.is_dir():
            return {
                "ok": False,
                "code": RESUME_WORKTREE_MISSING,
                "summary": f"lane worktree is missing: {worktree}",
                "evidence_paths": [],
                "next_action": "bootstrap a fresh lane (resume cannot rebuild a worktree)",
            }
        session = lane.get("session") or {}
        session_id = session.get("session_id")
        if not isinstance(session_id, str) or not session_id:
            return {
                "ok": False,
                "code": NO_SAVED_SESSION_ID,
                "summary": f"lane {lane_id} has no saved provider session to resume",
                "evidence_paths": [],
                "next_action": "bootstrap a fresh lane (resume requires a native session)",
            }

        try:
            task_card = _read_task_card(Path(resume_task_card))
        except (OSError, ValueError) as exc:
            return {
                "ok": False,
                "code": INVALID_RESUME_TASK_CARD,
                "summary": str(exc),
                "evidence_paths": [],
                "next_action": "supply a valid project-task-card/v1 resume card",
            }

        run_id = new_id()
        managed = config.profile == "managed"
        _clear_prior_run(rt, epoch_id, lane)
        if managed:
            _reset_worker_inbox(worktree, lane_id, run_id)
        _write_worker_prompt(worktree, task_card, managed=managed)
        _write_result_template(worktree, lane_id, run_id)
        invocation_path = worktree / ".agent-workspace" / "invocation.json"
        try:
            prior_invocation = read_json(invocation_path)
            require_schema(prior_invocation, INVOCATION_SCHEMA, invocation_path)
            exclusive_resources = [
                str(item) for item in prior_invocation.get("exclusive_resources", [])
            ]
        except (OSError, ValueError):
            exclusive_resources = []
        _write_invocation(
            worktree,
            lane_id=lane_id,
            run_id=run_id,
            provider_id=lane["provider"]["id"],
            model=lane["provider"]["model"],
            exclusive_resources=exclusive_resources,
        )
        _rewrite_overlay_receipt(worktree, lane, run_id, managed=managed)
        if managed:
            _write_worker_binding(worktree, rt, lane_id, run_id)
        # The resume task card is a per-lane input; keep the current copy.
        atomic_write_json(worktree / ".agent-workspace" / "task-card.json", task_card)

        update_lane(
            rt,
            epoch_id,
            lane_id,
            lambda current, value=run_id: {
                **current,
                "run_id": value,
                "lifecycle": "running",
                "process": {},
                "acceptance_advancement": None,
                "last_reported_actionable_status": None,
            },
        )
    except Exception as exc:
        return {
            "ok": False,
            "code": RESUME_LANE_WRITE_FAILED,
            "summary": str(exc),
            "evidence_paths": [],
            "next_action": "resolve the error and retry resume",
        }

    return {
        "ok": True,
        "code": "RESUME_OK",
        "summary": f"lane {lane_id} resumed under a fresh run_id; launch it to start the provider",
        "evidence_paths": [
            str(worktree / ".agent-workspace" / "invocation.json"),
            str(lane_record_dir(rt, epoch_id, lane_id) / "lane.json"),
        ],
        "next_action": "run `lane launch --lane-id <id>` to start the resumed run",
    }

RESUME_ADMISSION_SCHEMA = "orchestrator-resume-admission/v1"
RESUME_AMENDMENT_REVIEW_SCHEMA = "orchestrator-resume-amendment-review/v1"
_IDENTITY_FIELDS = (
    "cohort_id",
    "workflow_id",
    "workflow_version",
    "task_card_id",
    "task_card_revision",
    "task_card_sha256",
    "lane_id",
    "worker_invocation_id",
    "provider_id",
    "provider_launch_sha256",
    "session_id",
    "repository",
    "prompt_bundle_sha256",
    "prompt_content_sha256",
    "resources",
)
_TERMINAL_STATES = {"ACCEPTED", "ACCEPTED_TERMINAL", "TERMINAL_ACCEPTED"}


class ResumeAdmissionError(ValueError):
    """Raised when continuation identity cannot be admitted."""

    def __init__(
        self, message: str, admission: "ResumeAdmission | None" = None
    ) -> None:
        super().__init__(message)
        self.admission = admission


@dataclass(frozen=True)
class ResumeAmendmentCheck:
    """Validated facts from the one ROOT-owned amendment review record."""

    disposition: str
    allowed_mismatches: frozenset[str]
    record: Mapping[str, Any]


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _normalized_path(value: object) -> object:
    if not isinstance(value, str) or not value.strip():
        return value
    return os.path.normcase(os.path.abspath(value))


def _normalize(value: Any, *, key: str = "") -> Any:
    if key in {"common_dir", "worktree_root", "starting_commit"} and isinstance(
        value, str
    ):
        return _normalized_path(value) if key != "starting_commit" else value.lower()
    if isinstance(value, Mapping):
        return {str(k): _normalize(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    return value


def _mapping(value: Mapping[str, Any] | None) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _closed_mapping(
    value: object, *, required: frozenset[str], name: str
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    keys = {str(key) for key in value}
    if keys != required:
        missing = sorted(required - keys)
        extra = sorted(keys - required)
        detail = []
        if missing:
            detail.append("missing " + ", ".join(missing))
        if extra:
            detail.append("unknown " + ", ".join(extra))
        raise ValueError(f"{name} has an invalid closed shape ({'; '.join(detail)})")
    return {str(key): item for key, item in value.items()}


def _review_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")
    return value


def _review_digest(value: object, name: str) -> str:
    text = _review_text(value, name).lower()
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return text


def _review_file_path(value: object, name: str) -> Path:
    path = Path(_review_text(value, name))
    if not path.is_absolute():
        raise ValueError(f"{name} must be absolute")
    normalized = path.expanduser().resolve(strict=False)
    if path.is_symlink() or not normalized.is_file():
        raise ValueError(f"{name} is not an existing regular file")
    return normalized


def _review_path_digest(value: object, name: str) -> str:
    path = _review_file_path(value, name)
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise ValueError(f"cannot read {name}: {exc}") from exc


def _review_card_payload(path_value: object, name: str) -> Mapping[str, Any]:
    path = _review_file_path(path_value, name)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{name} is not a readable JSON task card: {exc}") from exc
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must contain a task-card object")
    return value


def _validate_review_diff_command(
    diff: Mapping[str, Any], *, old_path: object, new_path: object, name: str
) -> None:
    working_directory_text = _review_text(
        diff["working_directory"], f"{name}.working_directory"
    )
    working_directory = Path(working_directory_text)
    if not working_directory.is_absolute():
        raise ValueError(f"{name}.working_directory must be absolute")
    working_directory = working_directory.expanduser().resolve(strict=False)
    if not working_directory.is_dir():
        raise ValueError(f"{name}.working_directory is not an existing directory")
    command = _review_text(diff["command"], f"{name}.command")
    try:
        parts = shlex.split(command, posix=os.name != "nt")
    except ValueError as exc:
        raise ValueError(f"{name}.command is not parseable") from exc
    parts = [part.strip('"') for part in parts]
    if len(parts) != 6 or parts[:4] != ["git", "diff", "--no-index", "--"]:

        raise ValueError(f"{name}.command is not the closed no-index diff form")
    old_normalized = _review_file_path(old_path, f"{name}.old_path")
    new_normalized = _review_file_path(new_path, f"{name}.new_path")
    for argument, expected, label in (
        (parts[4], old_normalized, "old_path"),
        (parts[5], new_normalized, "new_path"),
    ):
        candidate = Path(argument)
        if not candidate.is_absolute():
            candidate = working_directory / candidate
        if candidate.expanduser().resolve(strict=False) != expected:
            raise ValueError(f"{name}.command {label} does not match the reviewed path")


def _review_normalize(field: str, value: object) -> object:
    if field in {
        "repository_common_dir",
        "worktree_root",
    } and isinstance(value, str):
        return _normalized_path(value)
    if field in {"original_base_commit", "continuation_start_commit"} and isinstance(
        value, str
    ):
        return value.lower()
    return value


def _review_job_identity(
    requested: Mapping[str, Any], persisted: Mapping[str, Any]
) -> dict[str, Any]:
    requested_repository = requested.get("repository")
    persisted_repository = persisted.get("repository")
    repository = (
        requested_repository if isinstance(requested_repository, Mapping) else {}
    )
    prior_repository = (
        persisted_repository if isinstance(persisted_repository, Mapping) else {}
    )
    expected: dict[str, Any] = {
        "card_id": requested.get("task_card_id"),
        "stage_cohort_id": requested.get("cohort_id"),
        "worker_invocation_id": requested.get("worker_invocation_id"),
        "lane_id": requested.get("lane_id"),
        "provider_session_id": requested.get("session_id"),
    }
    for field, value in (
        ("repository_common_dir", repository.get("common_dir")),
        ("worktree_root", repository.get("worktree_root")),
        ("branch", repository.get("branch")),
        ("original_base_commit", repository.get("base_commit")),
        ("continuation_start_commit", prior_repository.get("starting_commit")),
    ):
        if value is not None:
            expected[field] = value
    if "resources" in requested:
        expected["exclusive_resources"] = requested.get("resources")
    return expected


def validate_resume_amendment_review(
    review: Mapping[str, Any],
    requested: Mapping[str, Any],
    persisted: Mapping[str, Any],
    *,
    expected_job_identity: Mapping[str, Any] | None = None,
) -> ResumeAmendmentCheck:
    """Validate one strict ROOT review and return its narrowly allowed fields."""

    record = _closed_mapping(
        review,
        required=frozenset(
            {
                "schema",
                "run_id",
                "stage_id",
                "lane_id",
                "recorded_utc",
                "recorded_by",
                "disposition",
                "job_state",
                "same_job_identity",
                "reviewed_pairs",
                "semantic_impact",
                "route",
            }
        ),
        name="resume amendment review",
    )
    if record["schema"] != RESUME_AMENDMENT_REVIEW_SCHEMA:
        raise ValueError("unsupported resume amendment review schema")
    _review_text(record["run_id"], "review.run_id")
    _review_text(record["stage_id"], "review.stage_id")
    _review_text(record["lane_id"], "review.lane_id")
    _review_text(record["recorded_utc"], "review.recorded_utc")
    if record["recorded_by"] != "ROOT-IM":
        raise ValueError("resume amendment review must be recorded by ROOT-IM")
    disposition = record["disposition"]
    if disposition not in {"NO_CONTINUATION_REQUIRED", "CONTINUATION_REQUIRED"}:
        raise ValueError("resume amendment review disposition is invalid")
    if record["job_state"] != "UNACCEPTED":
        raise ValueError("resume amendment review is only valid for an unaccepted job")

    same_job = _closed_mapping(
        record["same_job_identity"],
        required=frozenset(
            {
                "card_id",
                "stage_cohort_id",
                "worker_invocation_id",
                "lane_id",
                "task_kind",
                "provider_session_id",
                "repository_common_dir",
                "worktree_root",
                "branch",
                "original_base_commit",
                "continuation_start_commit",
                "exclusive_resources",
            }
        ),
        name="review.same_job_identity",
    )
    for field, value in same_job.items():
        if field == "exclusive_resources":
            if (
                not isinstance(value, list)
                or any(not isinstance(item, str) or not item.strip() for item in value)
                or len(value) != len(set(value))
            ):
                raise ValueError(
                    "review.same_job_identity.exclusive_resources is invalid"
                )
        else:
            _review_text(value, f"review.same_job_identity.{field}")
    if record["lane_id"] != same_job["lane_id"]:
        raise ValueError(
            "resume amendment review lane does not match same-job identity"
        )
    expected_job = dict(
        expected_job_identity or _review_job_identity(requested, persisted)
    )
    for field, expected in expected_job.items():
        if expected is None:
            raise ValueError(
                f"resume identity cannot establish amendment field {field}"
            )
        if field not in same_job:
            raise ValueError(f"review.same_job_identity is missing {field}")
        if _review_normalize(field, same_job[field]) != _review_normalize(
            field, expected

        ):
            raise ValueError(f"resume amendment same-job identity mismatch: {field}")

    pairs = _closed_mapping(
        record["reviewed_pairs"],
        required=frozenset({"task_card", "prompt"}),
        name="review.reviewed_pairs",
    )
    pair_records: dict[str, dict[str, Any]] = {}
    pair_keys = frozenset({"old_path", "old_sha256", "new_path", "new_sha256", "diff"})
    diff_keys = frozenset(
        {
            "command",
            "working_directory",
            "exit_code",
            "stdout_encoding",
            "stdout_sha256",
            "stdout_bytes",
            "stderr_sha256",
            "stderr_bytes",
        }
    )
    for name in ("task_card", "prompt"):
        pair = _closed_mapping(
            pairs[name], required=pair_keys, name=f"reviewed_pairs.{name}"
        )
        _review_text(pair["old_path"], f"reviewed_pairs.{name}.old_path")
        _review_text(pair["new_path"], f"reviewed_pairs.{name}.new_path")
        old_digest = _review_digest(
            pair["old_sha256"], f"reviewed_pairs.{name}.old_sha256"
        )
        new_digest = _review_digest(
            pair["new_sha256"], f"reviewed_pairs.{name}.new_sha256"
        )
        if (
            _review_path_digest(pair["old_path"], f"reviewed_pairs.{name}.old_path")
            != old_digest
        ):
            raise ValueError(
                f"reviewed_pairs.{name} old raw identity does not match its path"
            )
        if (
            _review_path_digest(pair["new_path"], f"reviewed_pairs.{name}.new_path")
            != new_digest
        ):
            raise ValueError(
                f"reviewed_pairs.{name} new raw identity does not match its path"
            )
        diff = _closed_mapping(
            pair["diff"], required=diff_keys, name=f"reviewed_pairs.{name}.diff"
        )
        _validate_review_diff_command(
            diff,
            old_path=pair["old_path"],
            new_path=pair["new_path"],
            name=f"reviewed_pairs.{name}.diff",
        )
        if diff["exit_code"] != 1:
            raise ValueError(f"reviewed_pairs.{name}.diff.exit_code must be 1")
        if diff["stdout_encoding"] != "utf-8":
            raise ValueError(
                f"reviewed_pairs.{name}.diff.stdout_encoding must be utf-8"
            )
        _review_digest(
            diff["stdout_sha256"], f"reviewed_pairs.{name}.diff.stdout_sha256"
        )
        _review_digest(
            diff["stderr_sha256"], f"reviewed_pairs.{name}.diff.stderr_sha256"
        )
        for field in ("stdout_bytes", "stderr_bytes"):
            if (
                not isinstance(diff[field], int)
                or isinstance(diff[field], bool)
                or diff[field] < 0
            ):
                raise ValueError(f"reviewed_pairs.{name}.diff.{field} is invalid")
        pair_records[name] = pair

    if persisted.get("task_card_sha256") != pair_records["task_card"]["old_sha256"]:
        raise ValueError(
            "reviewed task-card old identity does not match persisted resume identity"
        )
    if requested.get("task_card_sha256") != pair_records["task_card"]["new_sha256"]:
        raise ValueError(
            "reviewed task-card new identity does not match requested resume identity"
        )
    if persisted.get("prompt_content_sha256") != pair_records["prompt"]["old_sha256"]:
        raise ValueError(
            "reviewed prompt old identity does not match persisted resume identity"
        )
    if requested.get("prompt_content_sha256") != pair_records["prompt"]["new_sha256"]:
        raise ValueError(
            "reviewed prompt new identity does not match requested resume identity"
        )

    old_card_payload = _review_card_payload(
        pair_records["task_card"]["old_path"], "reviewed_pairs.task_card.old_path"
    )
    new_card_payload = _review_card_payload(
        pair_records["task_card"]["new_path"], "reviewed_pairs.task_card.new_path"
    )
    card_identity_fields = (
        ("card_id", "task_card_id"),
        ("lane_id", "lane_id"),
        ("stage_cohort_id", "cohort_id"),
        ("worker_invocation_id", "worker_invocation_id"),
        ("revision", "task_card_revision"),
    )
    for card_field, identity_field in card_identity_fields:
        old_present = card_field in old_card_payload
        new_present = card_field in new_card_payload
        old_value = old_card_payload.get(card_field)
        new_value = new_card_payload.get(card_field)
        if old_present and new_present and old_value != new_value:
            if disposition == "NO_CONTINUATION_REQUIRED":
                raise ValueError(
                    f"reviewed task-card {card_field} changed across the pair"
                )
            if requested.get(identity_field) != new_value:
                raise ValueError(
                    f"reviewed task-card {card_field} does not match requested resume identity"
                )
            continue
        if old_present != new_present:
            if disposition == "NO_CONTINUATION_REQUIRED":
                raise ValueError(
                    f"reviewed task-card {card_field} changed across the pair"
                )
            if new_present and requested.get(identity_field) != new_value:
                raise ValueError(
                    f"reviewed task-card {card_field} does not match requested resume identity"
                )
            continue
        if old_present and requested.get(identity_field) != old_value:
            raise ValueError(
                f"reviewed task-card {card_field} does not match resume identity"
            )
    task_kind_values = [
        payload.get("task_kind")
        for payload in (old_card_payload, new_card_payload)
        if "task_kind" in payload
    ]
    if task_kind_values and (
        len(task_kind_values) != 2
        or task_kind_values[0] != task_kind_values[1]
        or same_job["task_kind"] != task_kind_values[0]
    ):
        raise ValueError(
            "reviewed task-card task_kind does not match the same-job review"
        )


    semantic = _closed_mapping(
        record["semantic_impact"],
        required=frozenset(
            {"classification", "rationale", "affected_scope", "preserved_credit"}
        ),
        name="review.semantic_impact",
    )
    classification = _review_text(
        semantic["classification"], "review.semantic_impact.classification"
    )
    _review_text(semantic["rationale"], "review.semantic_impact.rationale")
    for field in ("affected_scope", "preserved_credit"):
        values = semantic[field]
        if not isinstance(values, list) or any(
            not isinstance(item, str) or not item.strip() for item in values
        ):
            raise ValueError(f"review.semantic_impact.{field} is invalid")
    harmless_classes = {"HARMLESS", "NO_CONTINUATION_REQUIRED", "NO_MATERIAL_CHANGE"}
    material_classes = {"MATERIAL", "UNCERTAIN", "CONTINUATION_REQUIRED"}
    if (
        disposition == "NO_CONTINUATION_REQUIRED"
        and classification not in harmless_classes
    ):
        raise ValueError("NO_CONTINUATION_REQUIRED review is not classified harmless")
    if (
        disposition == "CONTINUATION_REQUIRED"
        and classification not in material_classes
    ):
        raise ValueError(
            "CONTINUATION_REQUIRED review is not classified material or uncertain"
        )

    route = _closed_mapping(
        record["route"],
        required=frozenset(
            {
                "kind",
                "resume_same_worker",
                "resume_same_provider_session",
                "reopen_accepted_tasks",
                "rerun_only_affected_checks",
                "require_fresh_affected_review",
            }
        ),
        name="review.route",
    )
    _review_text(route["kind"], "review.route.kind")
    for field in (
        "resume_same_worker",
        "resume_same_provider_session",
        "reopen_accepted_tasks",
        "rerun_only_affected_checks",
        "require_fresh_affected_review",
    ):
        if not isinstance(route[field], bool):
            raise ValueError(f"review.route.{field} must be boolean")
    if not route["resume_same_worker"] or not route["resume_same_provider_session"]:
        raise ValueError(
            "resume amendment review does not preserve worker/provider session"
        )
    if route["reopen_accepted_tasks"]:
        raise ValueError("resume amendment review cannot reopen an accepted task")

    allowed: set[str] = set()
    if disposition == "NO_CONTINUATION_REQUIRED":
        if (
            pair_records["task_card"]["old_sha256"]
            != pair_records["task_card"]["new_sha256"]
        ):
            allowed.add("task_card_sha256")
        if pair_records["prompt"]["old_sha256"] != pair_records["prompt"]["new_sha256"]:
            allowed.update({"prompt_bundle_sha256", "prompt_content_sha256"})
    return ResumeAmendmentCheck(
        disposition=disposition,
        allowed_mismatches=frozenset(allowed),
        record=record,
    )


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _freeze(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


@dataclass(frozen=True)
class ResumeAdmission:
    """An immutable record of every identity checked for a continuation."""

    admitted: bool
    reasons: tuple[str, ...]
    requested: Mapping[str, Any]
    persisted: Mapping[str, Any]
    live: Mapping[str, Any]
    decision_sha256: str
    schema: str = RESUME_ADMISSION_SCHEMA
    amendment_review: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.schema != RESUME_ADMISSION_SCHEMA:
            raise ResumeAdmissionError("unsupported resume admission schema")
        object.__setattr__(self, "reasons", tuple(self.reasons))
        for field_name in ("requested", "persisted", "live"):
            value = getattr(self, field_name)
            object.__setattr__(self, field_name, _freeze(_thaw(value)))
        if self.amendment_review is not None:
            object.__setattr__(
                self, "amendment_review", _freeze(_thaw(self.amendment_review))
            )
        if self.decision_sha256 != _digest(self._decision_payload()):
            raise ResumeAdmissionError(
                "resume admission decision hash is not content-bound"
            )

    def _decision_payload(self) -> dict[str, Any]:
        payload = {
            "schema": self.schema,
            "admitted": self.admitted,
            "reasons": list(self.reasons),
            "requested": _thaw(self.requested),
            "persisted": _thaw(self.persisted),
            "live": _thaw(self.live),
        }
        if self.amendment_review is not None:
            payload["amendment_review"] = _thaw(self.amendment_review)
        return payload

    def to_record(self) -> dict[str, Any]:
        return {**self._decision_payload(), "decision_sha256": self.decision_sha256}


def _compare_identity(
    requested: Mapping[str, Any],
    persisted: Mapping[str, Any],
    reasons: list[str],

    *,
    allowed_mismatches: frozenset[str] = frozenset(),
) -> None:
    for field in _IDENTITY_FIELDS:
        if field not in requested:
            reasons.append(f"requested identity missing {field}")
            continue
        if field not in persisted:
            reasons.append(f"persisted identity missing {field}")
            continue
        if (
            _normalize(requested[field], key=field)
            != _normalize(persisted[field], key=field)
            and field not in allowed_mismatches
        ):
            reasons.append(f"resume identity mismatch: {field}")


def make_resume_admission(
    requested: Mapping[str, Any],
    persisted: Mapping[str, Any],
    live: Mapping[str, Any],
    *,
    amendment_review: Mapping[str, Any] | None = None,
    expected_job_identity: Mapping[str, Any] | None = None,
) -> ResumeAdmission:
    """Compare requested, persisted, and live identities without mutation."""

    requested_value = _mapping(requested)
    persisted_value = _mapping(persisted)
    live_value = _mapping(live)
    reasons: list[str] = []
    allowed_mismatches = frozenset()
    amendment_record: Mapping[str, Any] | None = None
    if amendment_review is not None:
        try:
            amendment = validate_resume_amendment_review(
                amendment_review,
                requested_value,
                persisted_value,
                expected_job_identity=expected_job_identity,
            )
            amendment_record = amendment.record
            if amendment.disposition != "NO_CONTINUATION_REQUIRED":
                reasons.append(
                    "resume amendment requires the scoped continuation route"
                )
            else:
                allowed_mismatches = amendment.allowed_mismatches
        except (OSError, TypeError, ValueError) as exc:
            reasons.append(f"resume amendment review rejected: {exc}")
    if requested_value.get("schema") != persisted_value.get("schema"):
        reasons.append("resume invocation schema mismatch")
    _compare_identity(
        requested_value,
        persisted_value,
        reasons,
        allowed_mismatches=allowed_mismatches,
    )
    if amendment_review is None and any(
        requested_value.get(field) != persisted_value.get(field)
        for field in (
            "task_card_sha256",
            "prompt_bundle_sha256",
            "prompt_content_sha256",
        )
    ):
        reasons.append(
            "resume amendment review is required for changed task-card or prompt identity"
        )
    terminal_state = persisted_value.get("terminal_acceptance_state")
    if isinstance(terminal_state, str) and terminal_state.upper() in _TERMINAL_STATES:
        reasons.append("accepted terminal task cannot be resumed")
    if "live_identity" not in requested_value:
        reasons.append("requested identity missing live_identity")
    elif live_value.get("live_identity") != requested_value.get("live_identity"):
        reasons.append("live process/session identity mismatch")
    requested_repository = requested_value.get("repository")
    live_repository = live_value.get("repository")
    if requested_repository is not None and _normalize(
        requested_repository
    ) != _normalize(live_repository):
        reasons.append("live repository identity mismatch")
    admitted = not reasons
    payload = {
        "schema": RESUME_ADMISSION_SCHEMA,
        "admitted": admitted,
        "reasons": reasons,
        "requested": requested_value,
        "persisted": persisted_value,
        "live": live_value,
    }
    if amendment_record is not None:
        payload["amendment_review"] = amendment_record
    return ResumeAdmission(
        admitted=admitted,
        reasons=tuple(reasons),
        requested=_freeze(requested_value),
        persisted=_freeze(persisted_value),
        live=_freeze(live_value),
        decision_sha256=_digest(payload),
        amendment_review=amendment_record,
    )


def require_resume_admission(
    requested: Mapping[str, Any],
    persisted: Mapping[str, Any],
    live: Mapping[str, Any],
    *,
    amendment_review: Mapping[str, Any] | None = None,
    expected_job_identity: Mapping[str, Any] | None = None,
) -> ResumeAdmission:
    admission = make_resume_admission(
        requested,
        persisted,
        live,
        amendment_review=amendment_review,
        expected_job_identity=expected_job_identity,
    )
    if not admission.admitted:
        raise ResumeAdmissionError(
            "resume admission rejected: " + "; ".join(admission.reasons), admission
        )
    return admission


ResumeAdmissionDecision = ResumeAdmission
admit_resume = make_resume_admission


__all__ = [
    "RESUME_ADMISSION_SCHEMA",
    "RESUME_AMENDMENT_REVIEW_SCHEMA",
    "ResumeAdmission",
    "ResumeAdmissionDecision",
    "ResumeAdmissionError",
    "ResumeAmendmentCheck",
    "make_resume_admission",
    "admit_resume",
    "require_resume_admission",
    "validate_resume_amendment_review",
]
