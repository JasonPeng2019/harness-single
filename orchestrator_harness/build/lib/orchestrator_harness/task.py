"""Task-card, result, review, and semantic advancement records.

The harness validates shape and content binding.  It does not decide whether
the work is good: that judgment arrives as a separately owned acceptance
record whose references must match the exact card, result, revision, and
commit.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


TASK_CARD_SCHEMA = "orchestrator-task-card/v1"
TASK_RESULT_SCHEMA = "orchestrator-task-result/v1"
COMPLETION_REVIEW_SCHEMA = "orchestrator-completion-review/v1"
ORCHESTRATOR_ACCEPTANCE_SCHEMA = "orchestrator-acceptance/v1"
COMPLETION_REVIEW_FILENAME = "COMPLETION_REVIEW.json"
ORCHESTRATOR_ACCEPTANCE_FILENAME = "ORCHESTRATOR_ACCEPTANCE.json"
_HEX_COMMIT = re.compile(r"^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$")
_DIGEST = re.compile(r"^[0-9a-fA-F]{64}$")
_TASK_CARD_ALLOWED = {
    "schema",
    "card_id",
    "lane_id",
    "stage_cohort_id",
    "worker_invocation_id",
    "authored_by",
    "task_kind",
    "objective",
    "why_now",
    "working_scope",
    "starting_state",
    "context",
    "task_actions",
    "deliverables",
    "acceptance_criteria",
    "constraints",
    "verification",
    "entrypoint_budget",
    "failure_route",
    "completion_review_owner",
    "revision",
    "content_sha256",
    "owner",
}


class TaskValidationError(ValueError):
    """Raised when a task lifecycle record is not content-bound."""


def canonical_record(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def record_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_record(value).encode("utf-8")).hexdigest()


def _content_sha256(value: Mapping[str, Any], raw_bytes: bytes | None) -> str:
    if raw_bytes is not None:
        return hashlib.sha256(raw_bytes).hexdigest()
    if "content_sha256" in value:
        return record_sha256(_without_declared_content_hash(value))
    return record_sha256(value)


def _without_declared_content_hash(value: Mapping[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key != "content_sha256"}


def _validate_declared_content_hash(value: Mapping[str, Any], name: str) -> None:
    if "content_sha256" not in value:
        return
    declared = _digest(value.get("content_sha256"), f"{name}.content_sha256")
    canonical = record_sha256(_without_declared_content_hash(value))
    if declared != canonical:
        raise TaskValidationError(
            f"{name} content hash does not match canonical content"
        )


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TaskValidationError(f"{name} must be a non-empty string")
    return value.strip()


def _digest(value: object, name: str) -> str:
    result = _text(value, name).lower()
    if _DIGEST.fullmatch(result) is None:
        raise TaskValidationError(f"{name} must be a SHA-256 hex digest")
    return result


def _closed(
    value: object, required: set[str], optional: set[str], name: str
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TaskValidationError(f"{name} must be an object")
    keys = set(value)
    if not required.issubset(keys) or not keys.issubset(required | optional):
        raise TaskValidationError(f"{name} has an invalid closed shape")
    return value


def _strings(value: object, name: str) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise TaskValidationError(f"{name} must be a list of non-empty strings")
    return [item.strip() for item in value]


@dataclass(frozen=True)
class TaskCard:
    card_id: str
    lane_id: str
    worker_invocation_id: str
    cohort_id: str
    revision: str
    content_sha256: str
    completion_review_owner: str
    raw: Mapping[str, Any]

    @property
    def identity(self) -> dict[str, str]:
        return {
            "task_card_id": self.card_id,
            "lane_id": self.lane_id,
            "worker_invocation_id": self.worker_invocation_id,
            "cohort_id": self.cohort_id,
            "task_card_revision": self.revision,
            "task_card_sha256": self.content_sha256,
        }


def task_card_from_identity(
    *,
    card_id: object,
    lane_id: object,
    worker_invocation_id: object,
    cohort_id: object,
    revision: object,
    content_sha256: object,
    completion_review_owner: object = "ROOT-IM",
) -> TaskCard:
    """Build a card identity without hashing a synthetic replacement card.

    Controller and discovery status records carry the manager-declared card
    digest, not the full card bytes.  This adapter keeps that exact digest
    bound to result/review validation instead of manufacturing a different
    content hash from a partial local record.
    """

    card_id_text = _text(card_id, "task card.card_id")
    lane_id_text = _text(lane_id, "task card.lane_id")
    worker_text = _text(worker_invocation_id, "task card.worker_invocation_id")
    cohort_text = _text(cohort_id, "task card.stage_cohort_id")
    revision_text = _text(revision, "task card.revision")
    content_digest = _digest(content_sha256, "task card.content_sha256")
    owner_text = _text(completion_review_owner, "task card.completion_review_owner")
    return TaskCard(
        card_id_text,
        lane_id_text,
        worker_text,
        cohort_text,
        revision_text,
        content_digest,
        owner_text,
        {
            "schema": TASK_CARD_SCHEMA,
            "card_id": card_id_text,
            "lane_id": lane_id_text,
            "stage_cohort_id": cohort_text,
            "worker_invocation_id": worker_text,
            "revision": revision_text,
            "content_sha256": content_digest,
            "completion_review_owner": owner_text,
        },
    )


def validate_task_card(
    value: Mapping[str, Any], *, raw_bytes: bytes | None = None
) -> TaskCard:
    if not isinstance(value, Mapping):
        raise TaskValidationError("task card must be an object")
    if value.get("schema") != TASK_CARD_SCHEMA:
        raise TaskValidationError("task card schema must be orchestrator-task-card/v1")
    required = {
        "schema",
        "card_id",
        "lane_id",
        "stage_cohort_id",
        "worker_invocation_id",
        "objective",
    }
    _closed(value, required, _TASK_CARD_ALLOWED - required, "task card")
    card_id = _text(value.get("card_id"), "task card.card_id")
    lane_id = _text(value.get("lane_id"), "task card.lane_id")
    worker = _text(value.get("worker_invocation_id"), "task card.worker_invocation_id")
    cohort = _text(value.get("stage_cohort_id"), "task card.stage_cohort_id")
    _text(value.get("objective"), "task card.objective")
    revision_value = value.get("revision")
    if revision_value is None:
        starting = value.get("starting_state")
        revision_value = (
            starting.get("starting_commit") if isinstance(starting, Mapping) else None
        )
    revision = _text(revision_value or "1", "task card.revision")
    actual_content_sha256 = _content_sha256(value, raw_bytes)
    supplied = value.get("content_sha256")
    if (
        supplied is not None
        and _digest(supplied, "task card.content_sha256") != actual_content_sha256
    ):
        raise TaskValidationError("task card content hash does not match bytes")
    owner = value.get("completion_review_owner", value.get("owner", "ROOT-IM"))
    owner_text = _text(owner, "task card.completion_review_owner")
    return TaskCard(
        card_id,
        lane_id,
        worker,
        cohort,
        revision,
        actual_content_sha256,
        owner_text,
        dict(value),
    )


@dataclass(frozen=True)
class TaskResult:
    card: TaskCard
    branch: str
    commit: str
    outcome: str
    summary: str
    checks: tuple[Mapping[str, Any], ...]
    content_sha256: str
    acceptance_state: str

    @property
    def identity(self) -> dict[str, str]:
        return {
            **self.card.identity,
            "result_sha256": self.content_sha256,
            "commit": self.commit,
            "revision": self.card.revision,
        }


def validate_task_result(
    value: Mapping[str, Any],
    *,
    card: TaskCard,
    raw_bytes: bytes | None = None,
) -> TaskResult:
    required = {
        "schema",
        "card_id",
        "lane_id",
        "worker_invocation_id",
        "cohort_id",
        "revision",
        "task_card_sha256",
        "branch",
        "commit",
        "outcome",
        "summary",
        "checks",
    }
    optional = {
        "content_sha256",
        "prompt_bundle_sha256",
        "prompt_content_sha256",
        "acceptance_state",
    }
    _closed(value, required, optional, "task result")
    if value.get("schema") != TASK_RESULT_SCHEMA:
        raise TaskValidationError("task result schema is invalid")
    expected_identity = {
        "card_id": card.card_id,
        "lane_id": card.lane_id,
        "worker_invocation_id": card.worker_invocation_id,
        "cohort_id": card.cohort_id,
        "revision": card.revision,
    }
    if any(value.get(key) != expected for key, expected in expected_identity.items()):
        raise TaskValidationError("task result task/card identity does not match")
    if (
        _digest(value.get("task_card_sha256"), "task result.task_card_sha256")
        != card.content_sha256
    ):
        raise TaskValidationError(
            "task result task card content identity does not match"
        )
    branch = _text(value.get("branch"), "task result.branch")
    commit = _text(value.get("commit"), "task result.commit").lower()
    if _HEX_COMMIT.fullmatch(commit) is None:
        raise TaskValidationError(
            "task result.commit must be a full hexadecimal commit ID"
        )
    outcome = value.get("outcome")
    if outcome not in {"PASS", "FAIL", "BLOCKED"}:
        raise TaskValidationError("task result.outcome is invalid")
    summary = _text(value.get("summary"), "task result.summary")
    checks = value.get("checks")
    if (
        not isinstance(checks, list)
        or len(checks) > 64
        or any(not isinstance(item, Mapping) for item in checks)
    ):
        raise TaskValidationError("task result.checks is invalid")
    for check in checks:
        if set(check) - {"name", "command", "outcome", "status", "summary"}:
            raise TaskValidationError("task result check has an invalid shape")
        if "name" not in check and "command" not in check:
            raise TaskValidationError("task result check requires name or command")
        if check.get("outcome", check.get("status")) not in {
            "PASS",
            "FAIL",
            "SKIP",
            "NOT_RUN",
        }:
            raise TaskValidationError("task result check outcome is invalid")
    if "prompt_bundle_sha256" in value:
        _digest(value.get("prompt_bundle_sha256"), "task result.prompt_bundle_sha256")
    if "prompt_content_sha256" in value:
        _digest(value.get("prompt_content_sha256"), "task result.prompt_content_sha256")
    _validate_declared_content_hash(value, "task result")
    actual_hash = _content_sha256(value, raw_bytes)
    acceptance_state = value.get("acceptance_state", "PENDING")
    if acceptance_state not in {"PENDING", "ACCEPTED", "REJECTED"}:
        raise TaskValidationError("task result.acceptance_state is invalid")
    return TaskResult(
        card,
        branch,
        commit,
        str(outcome),
        summary,
        tuple(dict(item) for item in checks),
        actual_hash,
        str(acceptance_state),
    )


@dataclass(frozen=True)
class CompletionReview:
    card: TaskCard
    result_sha256: str
    owner: str
    verdict: str
    evidence: tuple[str, ...]
    content_sha256: str


def validate_completion_review(
    value: Mapping[str, Any],
    *,
    card: TaskCard,
    result: TaskResult,
    raw_bytes: bytes | None = None,
) -> CompletionReview:
    required = {
        "schema",
        "card_id",
        "lane_id",
        "worker_invocation_id",
        "cohort_id",
        "revision",
        "result_sha256",
        "owner",
        "verdict",
        "evidence",
    }
    _closed(value, required, {"content_sha256", "summary"}, "completion review")
    if value.get("schema") != COMPLETION_REVIEW_SCHEMA:
        raise TaskValidationError("completion review schema is invalid")
    for key, expected in {
        "card_id": card.card_id,
        "lane_id": card.lane_id,
        "worker_invocation_id": card.worker_invocation_id,
        "cohort_id": card.cohort_id,
        "revision": card.revision,
    }.items():
        if value.get(key) != expected:
            raise TaskValidationError(
                "completion review task/card identity does not match"
            )
    if (
        _digest(value.get("result_sha256"), "completion review.result_sha256")
        != result.content_sha256
    ):
        raise TaskValidationError("completion review result identity does not match")
    owner = _text(value.get("owner"), "completion review.owner")
    if owner != card.completion_review_owner:
        raise TaskValidationError("completion review owner does not own the task card")
    verdict = value.get("verdict")
    if verdict not in {"PASS", "FAIL", "BLOCKED"}:
        raise TaskValidationError("completion review.verdict is invalid")
    evidence = tuple(_strings(value.get("evidence"), "completion review.evidence"))
    _validate_declared_content_hash(value, "completion review")
    actual_hash = _content_sha256(value, raw_bytes)
    return CompletionReview(
        card, result.content_sha256, owner, str(verdict), evidence, actual_hash
    )


@dataclass(frozen=True)
class OrchestratorAcceptance:
    card: TaskCard
    result_sha256: str
    review_sha256: str
    accepted_commit: str
    accepted_by: str
    verdict: str
    content_sha256: str


def validate_orchestrator_acceptance(
    value: Mapping[str, Any],
    *,
    card: TaskCard,
    result: TaskResult,
    review: CompletionReview,
    review_record: Mapping[str, Any] | None = None,
    raw_bytes: bytes | None = None,
) -> OrchestratorAcceptance:
    required = {
        "schema",
        "card_id",
        "lane_id",
        "worker_invocation_id",
        "cohort_id",
        "revision",
        "card_sha256",
        "result_sha256",
        "completion_review_sha256",
        "accepted_commit",
        "accepted_by",
        "verdict",
    }
    _closed(value, required, {"content_sha256", "summary"}, "orchestrator acceptance")
    if value.get("schema") != ORCHESTRATOR_ACCEPTANCE_SCHEMA:
        raise TaskValidationError("orchestrator acceptance schema is invalid")
    for key, expected in {
        "card_id": card.card_id,
        "lane_id": card.lane_id,
        "worker_invocation_id": card.worker_invocation_id,
        "cohort_id": card.cohort_id,
        "revision": card.revision,
    }.items():
        if value.get(key) != expected:
            raise TaskValidationError("orchestrator acceptance identity does not match")
    if (
        _digest(value.get("card_sha256"), "orchestrator acceptance.card_sha256")
        != card.content_sha256
    ):
        raise TaskValidationError(
            "orchestrator acceptance card identity does not match"
        )
    if (
        _digest(value.get("result_sha256"), "orchestrator acceptance.result_sha256")
        != result.content_sha256
    ):
        raise TaskValidationError(
            "orchestrator acceptance result identity does not match"
        )
    expected_review_sha = review.content_sha256
    if (
        review_record is not None
        and _content_sha256(review_record, None) != expected_review_sha
    ):
        raise TaskValidationError(
            "orchestrator acceptance review record does not match review"
        )
    if (
        _digest(
            value.get("completion_review_sha256"),
            "orchestrator acceptance.completion_review_sha256",
        )
        != expected_review_sha
    ):
        raise TaskValidationError(
            "orchestrator acceptance review identity does not match"
        )
    accepted_commit = _text(
        value.get("accepted_commit"), "orchestrator acceptance.accepted_commit"
    ).lower()
    if accepted_commit != result.commit:
        raise TaskValidationError(
            "orchestrator acceptance commit does not match result"
        )
    accepted_by = _text(value.get("accepted_by"), "orchestrator acceptance.accepted_by")
    verdict = value.get("verdict")
    if verdict not in {"ACCEPTED", "REJECTED"}:
        raise TaskValidationError("orchestrator acceptance.verdict is invalid")
    _validate_declared_content_hash(value, "orchestrator acceptance")
    actual_hash = _content_sha256(value, raw_bytes)
    return OrchestratorAcceptance(
        card,
        result.content_sha256,
        expected_review_sha,
        accepted_commit,
        accepted_by,
        str(verdict),
        actual_hash,
    )


@dataclass(frozen=True)
class TaskAdvancement:
    state: str
    terminal: bool
    card_id: str
    revision: str
    result_sha256: str
    acceptance_sha256: str | None = None


@dataclass(frozen=True)
class TaskAdvancementEvidence:
    """The validated fixed-workspace advancement chain."""

    advancement: TaskAdvancement
    review: CompletionReview | None
    acceptance: OrchestratorAcceptance | None

    @property
    def state(self) -> str:
        return (
            "PENDING"
            if self.advancement.state == "ACCEPTANCE_PENDING"
            else self.advancement.state
        )

    @property
    def terminal(self) -> bool:
        return self.advancement.terminal

    @property
    def acceptance_identity(self) -> dict[str, str] | None:
        if self.acceptance is None:
            return None
        return {
            "schema": ORCHESTRATOR_ACCEPTANCE_SCHEMA,
            "card_id": self.acceptance.card.card_id,
            "lane_id": self.acceptance.card.lane_id,
            "worker_invocation_id": self.acceptance.card.worker_invocation_id,
            "cohort_id": self.acceptance.card.cohort_id,
            "revision": self.acceptance.card.revision,
            "card_sha256": self.acceptance.card.content_sha256,
            "result_sha256": self.acceptance.result_sha256,
            "completion_review_sha256": self.acceptance.review_sha256,
            "accepted_commit": self.acceptance.accepted_commit,
            "accepted_by": self.acceptance.accepted_by,
            "verdict": self.acceptance.verdict,
            "content_sha256": self.acceptance.content_sha256,
        }


def advance_task(
    card: TaskCard,
    result: TaskResult,
    *,
    review: CompletionReview | None = None,
    acceptance: OrchestratorAcceptance | None = None,
) -> TaskAdvancement:
    if result.card.identity != card.identity:
        raise TaskValidationError("result is not for the supplied task card")
    if review is None or acceptance is None:
        return TaskAdvancement(
            "ACCEPTANCE_PENDING",
            False,
            card.card_id,
            card.revision,
            result.content_sha256,
        )
    if (
        review.card.identity != card.identity
        or acceptance.card.identity != card.identity
    ):
        raise TaskValidationError("review/acceptance is not for the supplied task card")
    if review.result_sha256 != result.content_sha256:
        raise TaskValidationError("completion review result identity does not match")
    if acceptance.result_sha256 != result.content_sha256:
        raise TaskValidationError("acceptance result identity does not match")
    if acceptance.review_sha256 != review.content_sha256:
        raise TaskValidationError("acceptance review identity does not match")
    if acceptance.accepted_commit != result.commit:
        raise TaskValidationError("acceptance commit does not match result")
    accepted = acceptance.verdict == "ACCEPTED"
    return TaskAdvancement(
        "ACCEPTED" if accepted else "REJECTED",
        accepted,
        card.card_id,
        card.revision,
        result.content_sha256,
        acceptance.content_sha256,
    )


def _read_fixed_artifact(
    workspace: Path, filename: str
) -> tuple[Mapping[str, Any], bytes] | None:
    path = workspace / filename
    if path.is_symlink():
        raise TaskValidationError(
            f"task advancement artifact {filename} must be a regular file"
        )
    if not path.exists():
        return None
    if not path.is_file():
        raise TaskValidationError(
            f"task advancement artifact {filename} must be a regular file"
        )
    try:
        raw_bytes = path.read_bytes()
    except OSError as exc:
        raise TaskValidationError(
            f"cannot read task advancement artifact {filename}: {exc}"
        ) from exc
    if len(raw_bytes) > 1024 * 1024:
        raise TaskValidationError(
            f"task advancement artifact {filename} exceeds the 1 MiB limit"
        )
    try:
        value = json.loads(raw_bytes.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TaskValidationError(
            f"task advancement artifact {filename} is not valid UTF-8 JSON: {exc}"
        ) from exc
    if not isinstance(value, Mapping):
        raise TaskValidationError(
            f"task advancement artifact {filename} must contain an object"
        )
    return value, raw_bytes


def read_task_advancement(
    workspace: Path,
    *,
    card: TaskCard,
    result: TaskResult,
) -> TaskAdvancementEvidence:
    """Validate one manager-owned card -> result -> review -> acceptance chain.

    The two artifact names are deliberately fixed.  Both absent is the only
    PENDING case; a partial or malformed publication is an actionable error.
    """

    review_artifact = _read_fixed_artifact(workspace, COMPLETION_REVIEW_FILENAME)
    acceptance_artifact = _read_fixed_artifact(
        workspace, ORCHESTRATOR_ACCEPTANCE_FILENAME
    )
    if review_artifact is None and acceptance_artifact is None:
        advancement = advance_task(card, result)
        return TaskAdvancementEvidence(advancement, None, None)
    if review_artifact is None or acceptance_artifact is None:
        missing = (
            COMPLETION_REVIEW_FILENAME
            if review_artifact is None
            else ORCHESTRATOR_ACCEPTANCE_FILENAME
        )
        raise TaskValidationError(
            f"task advancement chain is incomplete: {missing} is missing; publish both fixed artifacts together"
        )
    review_value, review_bytes = review_artifact
    acceptance_value, acceptance_bytes = acceptance_artifact
    review = validate_completion_review(
        review_value,
        card=card,
        result=result,
        raw_bytes=review_bytes,
    )
    acceptance = validate_orchestrator_acceptance(
        acceptance_value,
        card=card,
        result=result,
        review=review,
        raw_bytes=acceptance_bytes,
    )
    advancement = advance_task(card, result, review=review, acceptance=acceptance)
    return TaskAdvancementEvidence(advancement, review, acceptance)


validate_task_record = validate_task_result
validate_completion_review_record = validate_completion_review
validate_acceptance_record = validate_orchestrator_acceptance
TaskRecord = TaskResult


__all__ = [
    "COMPLETION_REVIEW_SCHEMA",
    "COMPLETION_REVIEW_FILENAME",
    "ORCHESTRATOR_ACCEPTANCE_SCHEMA",
    "ORCHESTRATOR_ACCEPTANCE_FILENAME",
    "TASK_CARD_SCHEMA",
    "TASK_RESULT_SCHEMA",
    "CompletionReview",
    "OrchestratorAcceptance",
    "TaskAdvancement",
    "TaskAdvancementEvidence",
    "TaskCard",
    "TaskResult",
    "TaskRecord",
    "TaskValidationError",
    "advance_task",
    "canonical_record",
    "record_sha256",
    "read_task_advancement",
    "task_card_from_identity",
    "validate_completion_review",
    "validate_completion_review_record",
    "validate_acceptance_record",
    "validate_orchestrator_acceptance",
    "validate_task_card",
    "validate_task_result",
    "validate_task_record",
]
