"""Canonical record schemas, serialization, and integrity contracts."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence
from uuid import uuid4

from .privacy import PrivacyPolicy, guard_mandatory
from .config import NetworkResolution, NETWORK_MODES, NETWORK_CONTEXT_FIELDS

TASK_CARD_SCHEMA = "project-task-card/v1"
# The explicit worker-environment boundary a task card may declare.  A
# "scrubbed" card runs its controller and provider without the product
# control credentials; legacy and all-off cards declare nothing and keep the
# harness's inherited environment.
WORKER_ENVIRONMENT_MODES = frozenset({"scrubbed"})
MEMORY_HANDOFF_SCHEMA = "memory-handoff/v1"
PLAN_SCHEMA = "memory-plan/v1"
DECISION_SCHEMA = "memory-decision/v1"
ENVELOPE_SCHEMA = "memory-dispatch/v1"
FINAL_ENVELOPE_SCHEMA = "memory-final-dispatch/v1"
FINAL_CONTEXT_SECURITY = {
    "execution_role": "worker",
    "control_plane": "excluded",
    "memory_authority": "none",
    "may_approve": False,
    "may_publish": False,
    "may_execute_parent": False,
}
OPERATION_SCHEMA = "memory-operation/v1"
EFFECT_OPERATION_SCHEMA = "effect-operation/v1"
LOCAL_EFFECT_KINDS = (
    "review_receipt", "recent_evidence", "experience_ingestion", "generated_skill_creation",
)
EFFECT_OPERATION_STATUSES = frozenset({
    "waiting_source", "waiting_payload", "pending", "in_flight", "uncertain", "confirmed",
})
OUTCOME_SCHEMA = "memory-outcome/v1"
NATIVE_TERMINAL_EVIDENCE_SCHEMA = "native-terminal-evidence/v1"
REJECTED_NATIVE_ATTEMPT_SCHEMA = "rejected-native-attempt/v1"
REVIEW_RECEIPT_SCHEMA = "memory-review-receipt/v1"
REVIEWED_TRAJECTORY_SCHEMA = "reviewed-trajectory/v1"
EXPERIENCE_INGESTION_SCHEMA = "reviewed-experience-ingestion/v1"
CASE_RECEIPT_SCHEMA = "reviewed-case-receipt/v1"
GENERATED_SKILL_SCHEMA = "generated-skill-candidate/v1"
SKILL_APPROVAL_SCHEMA = "generated-skill-approval/v1"
PROCEDURE_REVISION_SCHEMA = "trusted-procedure-revision/v1"
PROCEDURE_APPROVAL_SCHEMA = "trusted-procedure-approval/v1"
PROCEDURE_REPRESENTATION_SCHEMA = "trusted-procedure-representation/v1"
PROCEDURE_COMPACT_REPRESENTATION_SCHEMA = "trusted-procedure-compact-representation/v1"
PROCEDURE_COMPACT_APPROVAL_SCHEMA = "trusted-procedure-compact-approval/v1"
FINAL_SOURCE_RECHECK_SCHEMA = "memory-final-source-recheck/v1"
PROCEDURE_DESIGNATION_SCHEMA = "trusted-procedure-designation/v1"
PROCEDURE_WITHDRAWAL_SCHEMA = "trusted-procedure-withdrawal/v1"
PROCEDURE_PUBLICATION_SCHEMA = "trusted-procedure-publication/v1"
PROCEDURE_REVOCATION_SCHEMA = "trusted-procedure-revocation/v1"
PROCEDURE_REMOTE_OPERATION_SCHEMA = "trusted-procedure-remote-operation/v1"
PROCEDURE_EXPOSURE_SCHEMA = "trusted-procedure-exposure/v1"
APC_REQUEST_SCHEMA = "apc-request/v1"
APC_RESULT_SCHEMA = "apc-result/v1"
NATIVE_USAGE_START_SCHEMA = "native-usage-start/v1"
NATIVE_USAGE_RECEIPT_SCHEMA = "native-usage-receipt/v1"
NATIVE_USAGE_CATEGORIES = frozenset({
    "outer_implementation", "product_test_root", "inner_candidate",
    "online_adaptation", "online_planning", "online_review", "online_execution",
    "maintenance", "embedding", "retrieval",
})

PLAN_STATES = frozenset({"candidate", "accepted", "proposed", "fresh"})
ROUTES = frozenset({"ordinary", "problem_focused", "deeper"})
OUTCOME_STATUSES = frozenset({"PASS", "FAIL", "BLOCKED", "UNKNOWN"})
OPERATION_STATUSES = frozenset({"pending", "ambiguous", "delivered", "failed_pre_spawn", "abandoned"})
REVIEW_STATES = frozenset({"reviewed", "accepted"})
REVIEWED_TRAJECTORY_STATUSES = frozenset({"reviewed_success", "reviewed_failure"})
GENERATED_SKILL_STATES = frozenset({"proposed"})
EXPERIENCE_INGESTION_STATUSES = frozenset({
    "pending",
    "uncertain",
    "confirmed",
    "blocked",
})
PROCEDURE_ORIGINS = frozenset({"generated", "curated", "builtin"})
PROCEDURE_PARTITION_SCOPES = frozenset({"private", "project", "shared"})
PROCEDURE_METRICS = frozenset({"cosine", "dot_product", "euclidean"})
PROCEDURE_OPERATION_STATUSES = frozenset({
    "intent",
    "ambiguous",
    "remote_committed",
    "acknowledged",
    "fenced",
    "withdrawn",
    "revoked",
    "blocked",
    "revocation_pending",
})


class ContractError(ValueError):
    """A product-owned record is malformed or violates an integrity contract."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_hex(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def content_hash(record: Mapping[str, Any]) -> str:
    payload = {key: value for key, value in record.items() if key != "content_hash"}
    return sha256_hex(payload)


def effect_operation_id(outcome_id: str, kind: str, scope_key: str = "outcome") -> str:
    """Stable slot identity shared by local and later source-specific effects."""
    for name, value in (("outcome_id", outcome_id), ("kind", kind), ("scope_key", scope_key)):
        _require_nonempty_str(value, name)
    return sha256_hex({"schema": EFFECT_OPERATION_SCHEMA, "outcome_id": outcome_id,
                       "kind": kind, "scope_key": scope_key})


def external_effect_operation_id(source_id: str, kind: str, scope_key: str) -> str:
    """One external mutation slot for an exact durable source and recipient scope."""
    for name, value in (("source_id", source_id), ("kind", kind), ("scope_key", scope_key)):
        _require_nonempty_str(value, name)
    return sha256_hex({"schema": EFFECT_OPERATION_SCHEMA, "external_source_id": source_id,
                       "kind": kind, "scope_key": scope_key})


def validate_record(record: Mapping[str, Any], schema: str) -> None:
    if not isinstance(record, Mapping):
        raise ContractError("record must be a JSON object")
    if record.get("schema") != schema:
        raise ContractError(f"record schema mismatch: expected {schema!r}, got {record.get('schema')!r}")
    if "content_hash" not in record:
        raise ContractError("record has no content hash")
    if record["content_hash"] != content_hash(record):
        raise ContractError("record content hash mismatch")


def _require_nonempty_str(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{field} must be a nonempty string")
    return value


def _require_canonical_identity(value: Any, field: str) -> str:
    identity = _require_nonempty_str(value, field)
    if identity != identity.strip() or any(ord(char) < 32 for char in identity):
        raise ContractError(f"{field.replace('_', ' ')} must be canonical")
    return identity


def _validate_final_mandatory(
    mandatory: list[dict[str, Any]], *, task: str, plan_content: Any,
    base_commit: str, route: str, checkpoint: str,
) -> None:
    expected = {
        "task": task,
        "accepted-plan": plan_content,
        "base": base_commit,
        "route": route,
        "checkpoint": checkpoint,
        "security": FINAL_CONTEXT_SECURITY,
    }
    by_id = {item["id"]: item for item in mandatory}
    if len(by_id) != len(mandatory):
        raise ContractError("finalized mandatory content has duplicate ids")
    for identifier, content in expected.items():
        item = by_id.get(identifier)
        if item is None or item.get("kind", identifier) != identifier or item.get("content") != content:
            raise ContractError(f"finalized mandatory {identifier} content is missing or inconsistent")


def _optional_descriptor(item: Mapping[str, Any]) -> dict[str, Any]:
    provenance_source = {
        key: value for key, value in item.items() if key not in {"id", "content"}
    }
    # The compact body is optional context, not provenance. Keep only its
    # exact identifiers and digests in the trace, including when omitted.
    for field, keys in (
        ("compact_representation", ("representation_id", "revision_id", "procedure_digest", "content_digest", "content_hash")),
        ("compact_approval", ("approval_id", "revision_id", "procedure_digest", "representation_id", "content_digest", "content_hash")),
    ):
        record = provenance_source.get(field)
        if isinstance(record, Mapping):
            provenance_source[field] = {key: record.get(key) for key in keys}
    provenance = json.loads(canonical_json(provenance_source))
    return {
        "id": _require_canonical_identity(item.get("id"), "optional item id"),
        "provenance": provenance,
        "provenance_digest": sha256_hex(provenance),
        "content_digest": sha256_hex(item.get("content")),
    }


def _packed_descriptor(item: Mapping[str, Any], selected: Mapping[str, Any]) -> dict[str, Any]:
    """Bind source observations in the trace without altering rendered content."""

    descriptor = _optional_descriptor(item)
    for field in ("final_recheck", "frozen_contract", "freshness", "source_id",
                  "source_owner_approval_digest", "source_owner_compact_approval_digest"):
        if field in selected["provenance"]:
            descriptor["provenance"].setdefault(field, selected["provenance"][field])
    descriptor["provenance_digest"] = sha256_hex(descriptor["provenance"])
    return descriptor


FINAL_RECHECK_STATUSES = frozenset({
    "eligible", "frozen", "stale", "revoked", "ineligible", "unavailable",
})


def make_final_source_recheck(
    *, item: Mapping[str, Any], status: str,
    observed_revision_id: str | None = None,
    observed_content_digest: str | None = None,
) -> dict[str, Any]:
    """Bind one finite source observation to the selected, original item."""

    record = {
        "schema": FINAL_SOURCE_RECHECK_SCHEMA,
        "item_id": _require_canonical_identity(item.get("id"), "recheck item id"),
        "source_id": _require_nonempty_str(item.get("source_id"), "recheck source id"),
        "revision_id": _require_nonempty_str(item.get("revision_id"), "recheck revision id"),
        "content_digest": sha256_hex(item.get("content")),
        "status": status,
        "observed_revision_id": observed_revision_id,
        "observed_content_digest": observed_content_digest,
    }
    record["content_hash"] = content_hash(record)
    validate_final_source_recheck(record, item=item)
    return record


def validate_final_source_recheck(
    record: Mapping[str, Any], *, item: Mapping[str, Any] | None = None,
) -> None:
    validate_record(record, FINAL_SOURCE_RECHECK_SCHEMA)
    if set(record) != {
        "schema", "item_id", "source_id", "revision_id", "content_digest",
        "status", "observed_revision_id", "observed_content_digest", "content_hash",
    }:
        raise ContractError("final source recheck has unexpected fields")
    for field in ("item_id", "source_id", "revision_id"):
        _require_nonempty_str(record.get(field), f"recheck {field}")
    for field in ("content_digest", "observed_content_digest"):
        value = record.get(field)
        if value is None and field == "observed_content_digest":
            continue
        if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            raise ContractError(f"final source recheck {field} is invalid")
    if record.get("status") not in FINAL_RECHECK_STATUSES:
        raise ContractError("final source recheck status is invalid")
    observed = record.get("observed_revision_id")
    if observed is not None:
        _require_nonempty_str(observed, "observed_revision_id")
    if record["status"] in {"eligible", "frozen"} and (
        observed != record["revision_id"]
        or record["observed_content_digest"] != record["content_digest"]
    ):
        raise ContractError("eligible final recheck does not match the selected source")
    if record["status"] == "stale" and (
        observed == record["revision_id"]
        and record["observed_content_digest"] == record["content_digest"]
    ):
        raise ContractError("stale final recheck reports unchanged source content")
    if record["status"] == "unavailable" and (
        observed is not None or record["observed_content_digest"] is not None
    ):
        raise ContractError("unavailable final recheck cannot claim an observation")
    if item is not None and any((
        record["item_id"] != item.get("id"),
        record["source_id"] != item.get("source_id"),
        record["revision_id"] != item.get("revision_id"),
        record["content_digest"] != sha256_hex(item.get("content")),
    )):
        raise ContractError("final source recheck does not bind the selected item")


def _validate_delivery_trace(
    trace: Any, optional: list[dict[str, Any]], omitted_ids: list[str],
) -> None:
    if not isinstance(trace, Mapping) or set(trace) != {
        "selected", "packed", "omitted", "context_delivered"
    }:
        raise ContractError("finalized delivery trace is incomplete")
    for key in ("selected", "packed", "omitted", "context_delivered"):
        if not isinstance(trace[key], list):
            raise ContractError(f"finalized delivery trace {key} must be a list")
    selected = trace["selected"]
    packed = trace["packed"]
    omitted = trace["omitted"]
    entries = [(key, item) for key in ("selected", "packed", "context_delivered", "omitted") for item in trace[key]]
    for key, item in entries:
        if not isinstance(item, Mapping):
            raise ContractError("finalized delivery trace entries must be objects")
        required = {"id", "provenance", "provenance_digest", "content_digest"}
        if set(item) != (required | ({"reason"} if key == "omitted" else set())):
            raise ContractError("finalized delivery trace entry has unexpected fields")
        provenance = item.get("provenance")
        if not isinstance(provenance, Mapping) or item.get("provenance_digest") != sha256_hex(provenance):
            raise ContractError("finalized delivery provenance digest mismatch")
        _require_canonical_identity(item.get("id"), "optional item id")
        digest = item.get("content_digest")
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ContractError("finalized delivery content digest is invalid")
    selected_by_id = {item["id"]: item for item in selected}
    if len(selected_by_id) != len(selected):
        raise ContractError("finalized delivery selected ids are duplicated")
    expected_packed = [
        _packed_descriptor(item, selected_by_id[item["id"]])
        for item in optional if item["id"] in selected_by_id
    ]
    if packed != expected_packed or trace["context_delivered"] != expected_packed:
        raise ContractError("finalized delivery packed/context-delivered trace mismatch")
    if [item.get("id") for item in omitted] != omitted_ids:
        raise ContractError("finalized delivery omitted ids mismatch")
    if set(selected_by_id) != {item["id"] for item in packed} | {item["id"] for item in omitted}:
        raise ContractError("finalized delivery selected partition mismatch")
    if len(packed) + len(omitted) != len(selected):
        raise ContractError("finalized delivery selected partition is duplicated")
    for item in packed:
        selected_item = selected_by_id[item["id"]]
        guidance = item["provenance"].get("delivery_representation") == "guidance"
        if ((not guidance and item["content_digest"] != selected_item["content_digest"])
                or (guidance and item["provenance"].get("full_content_digest")
                    != selected_item["content_digest"]) or any(
            key not in selected_item["provenance"] or selected_item["provenance"][key] != value
            for key, value in item["provenance"].items()
        )):
            raise ContractError("finalized delivery selected/packed provenance mismatch")
        recheck = item["provenance"].get("final_recheck")
        if guidance and recheck is None:
            raise ContractError("procedure guidance requires an exact source recheck")
        if recheck is not None:
            validate_final_source_recheck(recheck)
            if recheck["item_id"] != item["id"] or recheck["status"] not in {"eligible", "frozen"}:
                raise ContractError("finalized delivery packed source recheck is not eligible")
            if (recheck["source_id"] != item["provenance"].get("source_id")
                    or recheck["revision_id"] != item["provenance"].get("revision_id")):
                raise ContractError("finalized delivery recheck source identity mismatch")
            if (item["provenance"].get("kind") != "historical_evidence"
                    and item["provenance"].get("delivery_representation") not in {"compact", "guidance"}
                    and recheck["content_digest"] != item["content_digest"]):
                raise ContractError("finalized delivery content differs from rechecked source")
            if guidance and recheck["content_digest"] != selected_item["content_digest"]:
                raise ContractError("procedure guidance lacks rechecked full content binding")
            frozen = item["provenance"].get("frozen_contract")
            if (recheck["status"] == "frozen" or item["provenance"].get("freshness") == "frozen") and (
                recheck["status"] != "frozen"
                or item["provenance"].get("freshness") != "frozen"
                or frozen != {
                    "source_id": recheck["source_id"],
                    "revision_id": recheck["revision_id"],
                    "content_digest": recheck["content_digest"],
                }
            ):
                raise ContractError("finalized frozen source lacks its explicit contract")
        elif item["provenance"].get("freshness") == "frozen":
            raise ContractError("finalized frozen source lacks its final owner recheck")
    for item in omitted:
        if not isinstance(item.get("reason"), str) or not item["reason"].strip():
            raise ContractError("finalized delivery omission requires a reason")
        if selected_by_id[item["id"]] != {key: value for key, value in item.items() if key != "reason"}:
            raise ContractError("finalized delivery selected/omitted provenance mismatch")
        recheck = item["provenance"].get("final_recheck")
        if recheck is not None:
            validate_final_source_recheck(recheck)
            if recheck["item_id"] != item["id"]:
                raise ContractError("finalized delivery omitted recheck item mismatch")
            if (recheck["source_id"] != item["provenance"].get("source_id")
                    or recheck["revision_id"] != item["provenance"].get("revision_id")):
                raise ContractError("finalized delivery omitted recheck source mismatch")
            if recheck["status"] not in {"eligible", "frozen"} and item["reason"] != f"final recheck: {recheck['status']}":
                raise ContractError("finalized delivery omission contradicts source recheck")
        elif item["reason"].startswith("final recheck:"):
            raise ContractError("finalized delivery recheck omission lacks evidence")


def _validate_content(value: Any, field: str = "content") -> None:
    if not isinstance(value, (dict, list)):
        raise ContractError(f"{field} must be a JSON object or array")


def _normalize_json_object(value: Any, field: str) -> dict[str, Any]:
    """Return a nonempty JSON object with a stable, portable representation."""

    if not isinstance(value, Mapping) or not value:
        raise ContractError(f"{field} must be a nonempty JSON object")
    try:
        normalized = json.loads(canonical_json(value).decode("utf-8"))
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{field} must be JSON serializable") from exc
    if not isinstance(normalized, dict) or not normalized:
        raise ContractError(f"{field} must be a nonempty JSON object")
    return normalized


def make_task_card(
    *,
    task: str,
    base_commit: str,
    branch: str | None = None,
    memory_handoff: Mapping[str, Any] | None = None,
    worker_environment: str | None = None,
    acceptance_criteria: Sequence[str] | None = None,
    deliverables: Sequence[str] | None = None,
    reason_for_acceptance_and_deliverables: str | None = None,
) -> dict[str, Any]:
    task_text = _require_nonempty_str(task, "task")
    base = _require_nonempty_str(base_commit, "base_commit")
    criteria = list(acceptance_criteria or ("Complete the requested task",))
    outputs = list(deliverables or ("Working implementation and verification evidence",))
    if not criteria or any(not isinstance(item, str) or not item.strip() for item in criteria):
        raise ContractError("acceptance_criteria must contain nonempty strings")
    if not outputs or any(not isinstance(item, str) or not item.strip() for item in outputs):
        raise ContractError("deliverables must contain nonempty strings")
    reason = _require_nonempty_str(
        reason_for_acceptance_and_deliverables
        or "The criteria and deliverables define the observable completion boundary.",
        "reason_for_acceptance_and_deliverables",
    )
    record: dict[str, Any] = {
        "schema": TASK_CARD_SCHEMA,
        "task": task_text,
        "base_commit": base,
        "acceptance_criteria": criteria,
        "deliverables": outputs,
        "reason_for_acceptance_and_deliverables": reason,
    }
    if branch is not None:
        record["branch"] = _require_nonempty_str(branch, "branch")
    if worker_environment is not None:
        mode = _require_nonempty_str(worker_environment, "worker_environment")
        if mode not in WORKER_ENVIRONMENT_MODES:
            raise ContractError(
                f"unknown worker environment mode: {mode!r}"
            )
        record["worker_environment"] = mode
    if memory_handoff is not None:
        if not isinstance(memory_handoff, Mapping):
            raise ContractError("memory_handoff must be an object")
        record["memory_handoff"] = dict(memory_handoff)
    record["content_hash"] = content_hash(record)
    validate_task_card(record)
    return record


def validate_task_card(record: Mapping[str, Any]) -> None:
    validate_record(record, TASK_CARD_SCHEMA)
    _require_nonempty_str(record.get("task"), "task")
    _require_nonempty_str(record.get("base_commit"), "base_commit")
    for field in ("acceptance_criteria", "deliverables"):
        values = record.get(field)
        if (
            not isinstance(values, list)
            or not values
            or any(not isinstance(item, str) or not item.strip() for item in values)
        ):
            raise ContractError(f"{field} must contain nonempty strings")
    _require_nonempty_str(
        record.get("reason_for_acceptance_and_deliverables"),
        "reason_for_acceptance_and_deliverables",
    )
    if "worker_environment" in record:
        mode = _require_nonempty_str(record["worker_environment"], "worker_environment")
        if mode not in WORKER_ENVIRONMENT_MODES:
            raise ContractError(f"unknown worker environment mode: {mode!r}")
    if "memory_handoff" in record:
        validate_memory_handoff(record["memory_handoff"])


def make_memory_handoff(
    *,
    objective_id: str,
    route: str,
    plan: Mapping[str, Any] | None = None,
    plan_state: str | None = None,
    configuration: Mapping[str, Any] | None = None,
    checkpoint: str | None = None,
) -> dict[str, Any]:
    """Build one enhanced handoff that states its exact current-plan state.

    ``plan`` may be a validated plan record or ``None`` for a truly absent
    plan.  The record always carries the explicit current-plan state, so an
    absent plan is never confused with a candidate still in ROOT review or an
    exact ROOT-accepted execution plan. ``checkpoint`` is an optional
    pre-bootstrap ROOT/harness source; accepted enhanced finalization requires
    it, while staging and non-finalizing planning do not.
    """

    objective = _require_nonempty_str(objective_id, "objective_id")
    selected_route = _require_nonempty_str(route, "route")
    if selected_route not in ROUTES:
        raise ContractError(f"unknown route: {selected_route!r}")
    if plan is None:
        if plan_state is not None and plan_state != "absent":
            raise ContractError(
                "an absent plan cannot declare the current plan state "
                f"{plan_state!r}"
            )
        derived_state = "absent"
        bound_plan: dict[str, Any] | None = None
    else:
        validate_plan(
            plan,
            expected_objective_id=objective,
            expected_route=selected_route,
        )
        derived_state = current_plan_state_for(plan)
        if plan_state is not None and plan_state != derived_state:
            raise ContractError(
                "declared current plan state does not match the exact plan: "
                f"expected {derived_state!r}, got {plan_state!r}"
            )
        bound_plan = dict(plan)
    record: dict[str, Any] = {
        "schema": MEMORY_HANDOFF_SCHEMA,
        "objective_id": objective,
        "route": selected_route,
        "plan_state": derived_state,
        "plan": bound_plan,
        "configuration": dict(configuration or {}),
    }
    if checkpoint is not None:
        record["checkpoint"] = _require_canonical_identity(checkpoint, "handoff checkpoint")
    record["content_hash"] = content_hash(record)
    validate_memory_handoff(record)
    return record


def validate_memory_handoff(record: Mapping[str, Any]) -> None:
    validate_record(record, MEMORY_HANDOFF_SCHEMA)
    objective = _require_nonempty_str(record.get("objective_id"), "objective_id")
    route = _require_nonempty_str(record.get("route"), "route")
    if route not in ROUTES:
        raise ContractError(f"unknown route: {route!r}")
    plan_state = _require_nonempty_str(record.get("plan_state"), "plan_state")
    if plan_state not in HANDOFF_PLAN_STATES:
        raise ContractError(f"unknown handoff plan state: {plan_state!r}")
    plan = record.get("plan")
    if plan_state == "absent":
        if plan is not None:
            raise ContractError(
                "an absent plan handoff must not carry a nonempty plan reference"
            )
    else:
        if not isinstance(plan, Mapping):
            raise ContractError(
                "a handoff that declares a current plan requires its exact plan object"
            )
        validate_plan(plan, expected_objective_id=objective, expected_route=route)
        derived_state = current_plan_state_for(plan)
        if derived_state != plan_state:
            raise ContractError(
                "declared current plan state does not match the exact plan: "
                f"expected {derived_state!r}, got {plan_state!r}"
            )
    if not isinstance(record.get("configuration", {}), Mapping):
        raise ContractError("configuration must be an object")
    if "checkpoint" in record:
        _require_canonical_identity(record["checkpoint"], "handoff checkpoint")


def _require_bound_checkpoint(task_card: Mapping[str, Any], checkpoint: str) -> None:
    """Bind accepted enhanced execution to ROOT's pre-bootstrap handoff source."""

    handoff = task_card.get("memory_handoff")
    if handoff is None or handoff["plan_state"] != "execution_accepted":
        return
    source = handoff.get("checkpoint")
    _require_canonical_identity(source, "handoff checkpoint")
    if checkpoint != source:
        raise ContractError("finalized checkpoint does not match handoff checkpoint")


def handoff_plan(record: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return the exact bound plan of a validated handoff, or ``None`` absent."""

    validate_memory_handoff(record)
    plan = record.get("plan")
    return dict(plan) if isinstance(plan, Mapping) else None


def make_plan(
    *,
    plan_id: str,
    objective_id: str,
    route: str,
    state: str,
    content: Any,
    revision: int = 1,
    accepted_by: str | None = None,
    supersedes: str | None = None,
    source: Mapping[str, Any] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    plan = _require_nonempty_str(plan_id, "plan_id")
    objective = _require_nonempty_str(objective_id, "objective_id")
    selected_route = _require_nonempty_str(route, "route")
    plan_state = _require_nonempty_str(state, "state")
    if selected_route not in ROUTES:
        raise ContractError(f"unknown route: {selected_route!r}")
    if plan_state not in PLAN_STATES:
        raise ContractError(f"unknown plan state: {plan_state!r}")
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
        raise ContractError("revision must be a positive integer")
    _validate_content(content)
    if plan_state == "accepted":
        accepted_by = _require_nonempty_str(accepted_by, "accepted_by")
    elif accepted_by is not None:
        raise ContractError("accepted_by is only valid for an accepted plan")
    if supersedes is not None:
        supersedes = _require_nonempty_str(supersedes, "supersedes")
        if supersedes == plan:
            raise ContractError("a plan cannot supersede itself")
    if source is not None and not isinstance(source, Mapping):
        raise ContractError("source must be an object")
    record: dict[str, Any] = {
        "schema": PLAN_SCHEMA,
        "plan_id": plan,
        "objective_id": objective,
        "route": selected_route,
        "state": plan_state,
        "revision": revision,
        "content": content,
        "accepted_by": accepted_by,
        "supersedes": supersedes,
        **({"source": dict(source)} if source is not None else {}),
        "created_at": created_at or utc_now(),
    }
    record["content_hash"] = content_hash(record)
    validate_plan(record)
    return record


def validate_plan(
    record: Mapping[str, Any],
    *,
    expected_objective_id: str | None = None,
    expected_route: str | None = None,
    expected_state: str | None = None,
) -> None:
    plan_id = _require_nonempty_str(record.get("plan_id"), "plan_id")
    objective_id = _require_nonempty_str(record.get("objective_id"), "objective_id")
    route = _require_nonempty_str(record.get("route"), "route")
    state = _require_nonempty_str(record.get("state"), "state")
    validate_record(record, PLAN_SCHEMA)
    if route not in ROUTES:
        raise ContractError(f"unknown route: {route!r}")
    if state not in PLAN_STATES:
        raise ContractError(f"unknown plan state: {state!r}")
    revision = record.get("revision")
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
        raise ContractError("revision must be a positive integer")
    _validate_content(record.get("content"))
    if state == "accepted":
        if record.get("accepted_by") != "ROOT":
            raise ContractError("only ROOT may accept a plan")
    elif record.get("accepted_by") is not None:
        raise ContractError("accepted_by is only valid for an accepted plan")
    supersedes = record.get("supersedes")
    if supersedes is not None:
        if not isinstance(supersedes, str) or not supersedes:
            raise ContractError("supersedes must be a nonempty string")
        if supersedes == plan_id:
            raise ContractError("a plan cannot supersede itself")
    source = record.get("source")
    if source is not None and not isinstance(source, Mapping):
        raise ContractError("source must be an object")
    if expected_objective_id is not None and objective_id != expected_objective_id:
        raise ContractError(f"plan objective mismatch: expected {expected_objective_id!r}, got {objective_id!r}")
    if expected_route is not None and route != expected_route:
        raise ContractError(f"plan route mismatch: expected {expected_route!r}, got {route!r}")
    if expected_state is not None and state != expected_state:
        raise ContractError(f"plan state mismatch: expected {expected_state!r}, got {state!r}")


def revise_plan(
    old_plan: Mapping[str, Any],
    *,
    new_plan_id: str,
    new_content: Any,
    accepted_by: str = "ROOT",
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    validate_plan(old_plan)
    if accepted_by != "ROOT":
        raise ContractError("only ROOT may accept a revised plan")
    _validate_content(new_content)
    return make_plan(
        plan_id=new_plan_id,
        objective_id=old_plan["objective_id"],
        route=old_plan["route"],
        state="accepted",
        content=new_content,
        revision=int(old_plan["revision"]) + 1,
        accepted_by=accepted_by,
        supersedes=old_plan["plan_id"],
        source=source if source is not None else old_plan.get("source"),
    )


def validate_task_plan_binding(
    task_card: Mapping[str, Any], plan: Mapping[str, Any]
) -> None:
    """Require an enhanced task card to name the same exact current plan.

    A handoff that explicitly declares a truly absent plan carries no plan
    reference, so there is no binding to compare.
    """

    validate_task_card(task_card)
    validate_plan(plan)
    handoff = task_card.get("memory_handoff")
    if handoff is None:
        return
    bound_plan = handoff_plan(handoff)
    if bound_plan is None:
        return
    if bound_plan["content_hash"] != plan["content_hash"]:
        raise ContractError("task card plan does not match the supplied plan")


def make_decision(
    task_card: Mapping[str, Any],
    plan: Mapping[str, Any],
    *,
    strategy: str = "standard",
    configuration: Mapping[str, Any] | None = None,
    decision_id: str | None = None,
) -> dict[str, Any]:
    validate_task_plan_binding(task_card, plan)
    resolved_configuration = dict(configuration or {"strategy": strategy})
    if resolved_configuration.get("strategy") != strategy:
        raise ContractError("decision strategy does not match its configuration")
    configuration_digest = sha256_hex(resolved_configuration)
    identity = decision_id or sha256_hex(
        {
            "task_card_digest": task_card["content_hash"],
            "objective_id": plan["objective_id"],
            "route": plan["route"],
            "plan_id": plan["plan_id"],
            "strategy": strategy,
            "configuration_digest": configuration_digest,
        }
    )
    record: dict[str, Any] = {
        "schema": DECISION_SCHEMA,
        "decision_id": identity,
        "task_card_digest": task_card["content_hash"],
        "objective_id": plan["objective_id"],
        "route": plan["route"],
        "plan_id": plan["plan_id"],
        "plan_state": plan["state"],
        "plan_digest": plan["content_hash"],
        "strategy": strategy,
        "configuration": resolved_configuration,
        "configuration_digest": configuration_digest,
        "state": "prepared" if plan["state"] == "accepted" else plan["state"],
        "created_at": utc_now(),
    }
    record["content_hash"] = content_hash(record)
    validate_decision(record)
    return record


def logical_decision_identity(
    task_card: Mapping[str, Any], plan: Mapping[str, Any]
) -> dict[str, str]:
    """The exact mandatory state that owns one captured preparation decision."""

    validate_task_plan_binding(task_card, plan)
    return {
        "task_card_digest": task_card["content_hash"],
        "objective_id": plan["objective_id"],
        "route": plan["route"],
        "plan_id": plan["plan_id"],
        "plan_state": plan["state"],
        "plan_digest": plan["content_hash"],
    }


def validate_decision(record: Mapping[str, Any]) -> None:
    validate_record(record, DECISION_SCHEMA)
    for field in (
        "decision_id",
        "task_card_digest",
        "objective_id",
        "route",
        "plan_id",
        "plan_state",
        "plan_digest",
        "strategy",
        "configuration_digest",
        "state",
    ):
        _require_nonempty_str(record.get(field), field)
    if record["route"] not in ROUTES:
        raise ContractError(f"unknown route: {record['route']!r}")
    configuration = record.get("configuration")
    if not isinstance(configuration, Mapping):
        raise ContractError("configuration must be an object")
    if configuration.get("strategy") != record["strategy"]:
        raise ContractError("decision strategy does not match its configuration")
    if record["configuration_digest"] != sha256_hex(configuration):
        raise ContractError("decision configuration digest mismatch")


def _normalize_content_list(value: Iterable[Mapping[str, Any]] | None, field: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ContractError(f"{field} must be a list")
    result: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise ContractError(f"{field} entries must be objects")
        normalized = dict(item)
        if "id" not in normalized or not isinstance(normalized["id"], str) or not normalized["id"]:
            raise ContractError(f"{field} entries require a nonempty id")
        result.append(normalized)
    return result


def _digest_content_list(value: list[dict[str, Any]]) -> str:
    return sha256_hex(value)


def make_envelope(
    *,
    task_card: Mapping[str, Any],
    plan: Mapping[str, Any],
    decision_id: str,
    lane_id: str,
    run_id: str,
    worktree_path: str,
    base_commit: str,
    mandatory_content: list[Mapping[str, Any]] | None = None,
    optional_content: list[Mapping[str, Any]] | None = None,
    omitted_content: list[str] | None = None,
    strategy: str = "standard",
    configuration: Mapping[str, Any] | None = None,
    final_context: Mapping[str, Any] | None = None,
    created_at: str | None = None,
    privacy_policy: PrivacyPolicy | None = None,
) -> dict[str, Any]:
    validate_task_plan_binding(task_card, plan)
    validate_plan(plan, expected_state="accepted")
    validate_decision_id = _require_nonempty_str(decision_id, "decision_id")
    lane = _require_nonempty_str(lane_id, "lane_id")
    run = _require_nonempty_str(run_id, "run_id")
    worktree = _require_nonempty_str(str(worktree_path), "worktree_path")
    base = _require_nonempty_str(base_commit, "base_commit")
    resolved_configuration = dict(configuration or {"strategy": strategy})
    if resolved_configuration.get("strategy") != strategy:
        raise ContractError("envelope strategy does not match its configuration")
    if task_card["base_commit"] != base:
        raise ContractError(
            f"task card base mismatch: expected {task_card['base_commit']!r}, got {base!r}"
        )
    mandatory = _normalize_content_list(mandatory_content, "mandatory_content")
    optional = _normalize_content_list(optional_content, "optional_content")
    guard_mandatory({"task": task_card["task"], "plan": plan["content"],
                     "mandatory": mandatory, "optional": optional,
                     "configuration": resolved_configuration}, privacy_policy or PrivacyPolicy())
    omitted = omitted_content or []
    if not isinstance(omitted, list) or any(not isinstance(item, str) or not item for item in omitted):
        raise ContractError("omitted_content must be a list of nonempty strings")
    record: dict[str, Any] = {
        "schema": FINAL_ENVELOPE_SCHEMA if final_context is not None else ENVELOPE_SCHEMA,
        "lane_id": lane,
        "run_id": run,
        "decision_id": validate_decision_id,
        "strategy": strategy,
        "configuration": resolved_configuration,
        "configuration_digest": sha256_hex(resolved_configuration),
        "task_card_digest": task_card["content_hash"],
        "objective_id": plan["objective_id"],
        "route": plan["route"],
        "plan_id": plan["plan_id"],
        "plan_state": plan["state"],
        "plan_digest": plan["content_hash"],
        "base_commit": base,
        "worktree_path": worktree,
        "mandatory_content": mandatory,
        "optional_content": optional,
        "mandatory_digest": _digest_content_list(mandatory),
        "optional_digest": _digest_content_list(optional),
        "delivery": {
            "mandatory": [item["id"] for item in mandatory],
            "optional": [item["id"] for item in optional],
            "omitted": list(omitted),
        },
        "dispatch_state": "finalized",
        "created_at": created_at or utc_now(),
    }
    if final_context is not None:
        validate_finalized_context(final_context)
        guard_mandatory(final_context, privacy_policy or PrivacyPolicy())
        record["final_context"] = dict(final_context)
        for field in (
            "task", "plan_revision", "accepted_by", "checkpoint", "execution_role",
            "invocation_target", "recipient", "delivery_trace",
        ):
            record[field] = final_context[field]
        record["final_context_id"] = final_context["context_id"]
        record["final_context_integrity"] = final_context["integrity"]
    record["content_hash"] = content_hash(record)
    validate_envelope(
        record,
        task_card=task_card,
        plan=plan,
        lane_id=lane,
        run_id=run,
        base_commit=base,
        worktree_path=worktree,
    )
    return record


def validate_envelope(
    record: Mapping[str, Any],
    *,
    task_card: Mapping[str, Any],
    plan: Mapping[str, Any],
    lane_id: str,
    run_id: str,
    base_commit: str,
    worktree_path: str,
    decision_id: str | None = None,
    require_final_context: bool = False,
) -> None:
    schema = record.get("schema") if isinstance(record, Mapping) else None
    if schema not in (ENVELOPE_SCHEMA, FINAL_ENVELOPE_SCHEMA):
        raise ContractError("unknown envelope schema")
    validate_record(record, schema)
    if require_final_context and schema != FINAL_ENVELOPE_SCHEMA:
        raise ContractError("a domain-finalized envelope is required")
    validate_task_card(task_card)
    validate_task_plan_binding(task_card, plan)
    validate_plan(plan, expected_state="accepted")
    expected = {
        "lane_id": lane_id,
        "run_id": run_id,
        "task_card_digest": task_card["content_hash"],
        "objective_id": plan["objective_id"],
        "route": plan["route"],
        "plan_id": plan["plan_id"],
        "plan_state": plan["state"],
        "plan_digest": plan["content_hash"],
        "base_commit": base_commit,
        "worktree_path": str(worktree_path),
        "decision_id": decision_id
        or make_decision(
            task_card,
            plan,
            strategy=str(record.get("strategy")),
            configuration=record.get("configuration"),
        )["decision_id"],
    }
    for field, expected_value in expected.items():
        actual = record.get(field)
        if actual != expected_value:
            raise ContractError(f"envelope {field.replace('_', ' ')} mismatch: expected {expected_value!r}, got {actual!r}")
    strategy = _require_nonempty_str(record.get("strategy"), "strategy")
    configuration = record.get("configuration")
    if not isinstance(configuration, Mapping):
        raise ContractError("envelope configuration must be an object")
    if configuration.get("strategy") != strategy:
        raise ContractError("envelope strategy does not match its configuration")
    if record.get("configuration_digest") != sha256_hex(configuration):
        raise ContractError("envelope configuration digest mismatch")
    mandatory = _normalize_content_list(record.get("mandatory_content"), "mandatory_content")
    optional = _normalize_content_list(record.get("optional_content"), "optional_content")
    if record.get("mandatory_digest") != _digest_content_list(mandatory):
        raise ContractError("mandatory digest mismatch")
    if record.get("optional_digest") != _digest_content_list(optional):
        raise ContractError("optional digest mismatch")
    delivery = record.get("delivery")
    if not isinstance(delivery, Mapping):
        raise ContractError("delivery must be an object")
    expected_delivery = {
        "mandatory": [item["id"] for item in mandatory],
        "optional": [item["id"] for item in optional],
        "omitted": delivery.get("omitted", []),
    }
    if dict(delivery) != expected_delivery:
        raise ContractError("delivery trace does not match the rendered content")
    if record.get("dispatch_state") != "finalized":
        raise ContractError("envelope is not finalized")
    if schema == FINAL_ENVELOPE_SCHEMA:
        _require_bound_checkpoint(task_card, record.get("checkpoint"))
        bound_context = record.get("final_context")
        validate_finalized_context(bound_context)
        if bound_context["context_id"] != record.get("final_context_id") or bound_context["integrity"] != record.get("final_context_integrity"):
            raise ContractError("finalized envelope context identity mismatch")
        for field in ("checkpoint", "execution_role", "invocation_target", "recipient"):
            _require_canonical_identity(record.get(field), field)
        for field in ("final_context_id", "final_context_integrity"):
            _require_nonempty_str(record.get(field), field)
        if record.get("task") != task_card["task"]:
            raise ContractError("finalized envelope task mismatch")
        if record.get("plan_revision") != plan["revision"] or record.get("accepted_by") != plan["accepted_by"]:
            raise ContractError("finalized envelope accepted plan revision mismatch")
        if record["execution_role"] != FINAL_CONTEXT_SECURITY["execution_role"]:
            raise ContractError("finalized envelope execution role mismatch")
        _validate_final_mandatory(
            mandatory, task=task_card["task"], plan_content=plan["content"],
            base_commit=base_commit, route=plan["route"], checkpoint=record["checkpoint"],
        )
        _validate_delivery_trace(record.get("delivery_trace"), optional, delivery["omitted"])
        bound_fields = {
            "lane_id", "run_id", "decision_id", "task", "task_card_digest", "objective_id",
            "route", "plan_id", "plan_digest", "base_commit", "worktree_path", "strategy",
            "configuration", "configuration_digest", "mandatory_content", "optional_content",
            "mandatory_digest", "optional_digest", "checkpoint", "execution_role",
            "invocation_target", "recipient", "plan_revision", "accepted_by", "delivery_trace",
        }
        for field in bound_fields:
            if bound_context.get(field) != record.get(field):
                raise ContractError(f"finalized envelope {field.replace('_', ' ')} context binding mismatch")
        if bound_context.get("omitted") != delivery["omitted"]:
            raise ContractError("finalized envelope omitted context binding mismatch")
    elif any(field in record for field in ("final_context", "final_context_id", "final_context_integrity")):
        raise ContractError("generic envelope cannot carry a finalized-context binding")


def make_operation(
    *,
    kind: str,
    envelope: Mapping[str, Any],
    status: str = "pending",
    observed_invocation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    selected_kind = _require_nonempty_str(kind, "kind")
    validate_envelope_identity = _require_nonempty_str(envelope.get("content_hash"), "envelope digest")
    operation_id = sha256_hex({"kind": selected_kind, "envelope_digest": validate_envelope_identity})
    record: dict[str, Any] = {
        "schema": OPERATION_SCHEMA,
        "operation_id": operation_id,
        "kind": selected_kind,
        "decision_id": envelope["decision_id"],
        "envelope_digest": validate_envelope_identity,
        "run_id": envelope["run_id"],
        "status": status,
        "observed_invocation": dict(observed_invocation) if observed_invocation is not None else None,
        "created_at": utc_now(),
    }
    if envelope.get("schema") == FINAL_ENVELOPE_SCHEMA:
        record["envelope_record"] = dict(envelope)
    record["content_hash"] = content_hash(record)
    validate_operation(record)
    return record


def validate_operation(record: Mapping[str, Any]) -> None:
    validate_record(record, OPERATION_SCHEMA)
    for field in ("operation_id", "kind", "decision_id", "envelope_digest", "run_id", "status"):
        _require_nonempty_str(record.get(field), field)
    if record["status"] not in OPERATION_STATUSES:
        raise ContractError(f"unknown operation status: {record['status']!r}")
    if record["status"] in {"failed_pre_spawn", "abandoned"} and record["kind"] != "dispatch":
        raise ContractError("pre-spawn terminal status belongs only to dispatch")
    if record["status"] == "delivered" and record.get("observed_invocation") is None:
        raise ContractError("delivered operation requires an observed invocation")
    if record.get("observed_invocation") is not None and not isinstance(record["observed_invocation"], Mapping):
        raise ContractError("observed_invocation must be an object or null")
    if "envelope_record" in record and not isinstance(record["envelope_record"], Mapping):
        raise ContractError("operation envelope binding must be an object")


def make_outcome(
    *,
    decision_id: str,
    plan_id: str,
    plan_digest: str,
    status: str,
    evidence_digest: str,
    linked_run_id: str,
    task_card_digest: str,
    objective_id: str,
    observed_at: str | None = None,
) -> dict[str, Any]:
    decision = _require_nonempty_str(decision_id, "decision_id")
    plan = _require_nonempty_str(plan_id, "plan_id")
    digest = _require_nonempty_str(plan_digest, "plan_digest")
    outcome_status = _require_nonempty_str(status, "status")
    if outcome_status not in OUTCOME_STATUSES:
        raise ContractError(f"unknown outcome status: {outcome_status!r}")
    evidence = _require_nonempty_str(evidence_digest, "evidence_digest")
    run = _require_nonempty_str(linked_run_id, "linked_run_id")
    task_digest = _require_nonempty_str(task_card_digest, "task_card_digest")
    objective = _require_nonempty_str(objective_id, "objective_id")
    outcome_id = sha256_hex(
        {
            "decision_id": decision,
            "plan_id": plan,
            "plan_digest": digest,
            "status": outcome_status,
            "evidence_digest": evidence,
            "linked_run_id": run,
            "task_card_digest": task_digest,
            "objective_id": objective,
        }
    )
    record: dict[str, Any] = {
        "schema": OUTCOME_SCHEMA,
        "outcome_id": outcome_id,
        "decision_id": decision,
        "plan_id": plan,
        "plan_digest": digest,
        "status": outcome_status,
        "evidence_digest": evidence,
        "linked_run_id": run,
        "task_card_digest": task_digest,
        "objective_id": objective,
        "observed_at": observed_at or utc_now(),
    }
    record["content_hash"] = content_hash(record)
    validate_outcome(record)
    return record


def validate_outcome(record: Mapping[str, Any]) -> None:
    validate_record(record, OUTCOME_SCHEMA)
    for field in (
        "outcome_id", "decision_id", "task_card_digest", "objective_id",
        "plan_id", "plan_digest", "status", "evidence_digest", "linked_run_id",
    ):
        _require_nonempty_str(record.get(field), field)
    if record["status"] not in OUTCOME_STATUSES:
        raise ContractError(f"unknown outcome status: {record['status']!r}")


def validate_native_terminal_evidence(evidence: Mapping[str, Any]) -> None:
    """Validate lane 2's retained enhanced-parent bundle before local fixation.

    The store separately compares its dispatch against the durable observation.
    """
    validate_record(evidence, NATIVE_TERMINAL_EVIDENCE_SCHEMA)
    for field in ("epoch_id", "lane_id", "run_id", "objective_id", "decision_id"):
        _require_nonempty_str(evidence.get(field), field)
    card, plan, decision = (evidence.get(key) for key in
                            ("task_card", "accepted_plan", "decision"))
    if not all(isinstance(record, Mapping) for record in (card, plan, decision)):
        raise ContractError("terminal task, plan, or decision missing")
    validate_task_plan_binding(card, plan)
    validate_decision(decision)
    handoff = card.get("memory_handoff")
    if not isinstance(handoff, Mapping) or handoff.get("plan_state") != "execution_accepted":
        raise ContractError("terminal evidence is not an accepted enhanced parent")
    if handoff.get("plan") != plan or plan.get("state") != "accepted":
        raise ContractError("terminal evidence accepted plan mismatch")
    for field, expected in (
        ("decision_id", decision["decision_id"]),
        ("objective_id", plan["objective_id"]),
    ):
        if evidence[field] != expected:
            raise ContractError(f"terminal evidence {field} mismatch")
    for field, expected in (
        ("task_card_digest", card["content_hash"]), ("objective_id", plan["objective_id"]),
        ("route", plan["route"]), ("plan_id", plan["plan_id"]),
        ("plan_state", "accepted"), ("plan_digest", plan["content_hash"]),
        ("state", "prepared"),
    ):
        if decision.get(field) != expected:
            raise ContractError(f"terminal decision {field} mismatch")
    expected_decision = make_decision(card, plan, strategy=decision["strategy"],
                                      configuration=decision["configuration"])
    if decision["decision_id"] != expected_decision["decision_id"]:
        raise ContractError("terminal decision identity mismatch")
    configuration = evidence.get("configuration")
    if configuration != decision["configuration"] or evidence.get("configuration_digest") != decision["configuration_digest"]:
        raise ContractError("terminal configuration mismatch")
    dispatch = evidence.get("dispatch")
    if not isinstance(dispatch, Mapping):
        raise ContractError("terminal dispatch missing")
    envelope = dispatch.get("envelope")
    context = evidence.get("final_context")
    if not isinstance(envelope, Mapping):
        raise ContractError("terminal dispatch envelope missing")
    validate_envelope(
        envelope, task_card=card, plan=plan, lane_id=evidence["lane_id"],
        run_id=evidence["run_id"], base_commit=card["base_commit"],
        worktree_path=envelope.get("worktree_path"), decision_id=decision["decision_id"],
    )
    validate_finalized_context(context)
    if envelope.get("schema") == FINAL_ENVELOPE_SCHEMA and context != envelope["final_context"]:
        raise ContractError("terminal final context differs from dispatch envelope")
    for field in ("lane_id", "run_id", "decision_id", "task_card_digest", "objective_id",
                  "route", "plan_id", "plan_digest", "base_commit", "worktree_path",
                  "strategy", "configuration_digest"):
        if context.get(field) != envelope.get(field):
            raise ContractError(f"terminal final context {field} mismatch")
    if context.get("configuration") != configuration or envelope.get("configuration") != configuration:
        raise ContractError("terminal final context or configuration mismatch")
    operation = dispatch.get("operation")
    observed = dispatch.get("observed_invocation")
    if not isinstance(operation, Mapping) or not isinstance(observed, Mapping):
        raise ContractError("terminal native invocation missing")
    if set(operation) != {
        "operation_id", "decision_id", "envelope_digest", "run_id", "kind",
        "status", "observed_invocation", "created_at", "updated_at",
    }:
        raise ContractError("terminal native operation fields invalid")
    _require_nonempty_str(operation["created_at"], "native operation created_at")
    _require_nonempty_str(operation["updated_at"], "native operation updated_at")
    if dispatch.get("operation_digest") != sha256_hex(operation):
        raise ContractError("terminal dispatch operation digest mismatch")
    if dispatch.get("envelope_digest") != envelope["content_hash"]:
        raise ContractError("terminal dispatch envelope digest mismatch")
    for field, expected in (
        ("operation_id", sha256_hex({"kind": "dispatch", "envelope_digest": envelope["content_hash"]})),
        ("decision_id", decision["decision_id"]), ("envelope_digest", envelope["content_hash"]),
        ("run_id", evidence["run_id"]), ("kind", "dispatch"), ("status", "delivered"),
        ("observed_invocation", observed),
    ):
        if operation.get(field) != expected:
            raise ContractError(f"terminal dispatch {field} mismatch")
    pid, creation = observed.get("pid"), observed.get("creation_time")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid < 1 or not isinstance(creation, str) or not creation or observed.get("invocation_id") != f"controller:{pid}:{creation}":
        raise ContractError("terminal native invocation identity invalid")
    for field, expected in (
        ("task_card_digest", card["content_hash"]), ("decision_id", decision["decision_id"]),
        ("plan_id", plan["plan_id"]), ("plan_digest", plan["content_hash"]),
        ("envelope_digest", envelope["content_hash"]), ("lane_id", evidence["lane_id"]),
        ("run_id", evidence["run_id"]), ("base_commit", card["base_commit"]),
        ("route", plan["route"]), ("configuration_digest", decision["configuration_digest"]),
        ("context_id", context["context_id"]), ("context_digest", context["content_hash"]),
    ):
        if observed.get(field) != expected:
            raise ContractError(f"terminal native observation {field} mismatch")
    review, acceptance, result = (evidence.get(key) for key in ("review", "acceptance", "result"))
    validate_record(review, "completion-review/v1")
    validate_record(acceptance, "orchestrator-acceptance/v1")
    if acceptance.get("accepted_by") != "ROOT" or acceptance.get("approval") not in {"ACCEPTED", "REJECTED"} or acceptance.get("review_ref") != review["content_hash"]:
        raise ContractError("terminal ROOT acceptance or review link invalid")
    card_id = str(card.get("card_id") or card.get("id") or card["content_hash"])
    for field, expected in (
        ("lane_id", evidence["lane_id"]), ("run_id", evidence["run_id"]),
        ("task_card_id", card_id), ("task_card_hash", card["content_hash"]),
    ):
        if review.get(field) != expected or acceptance.get(field) != expected:
            raise ContractError(f"terminal review/acceptance {field} mismatch")
    for field in ("result_id", "result_hash", "commit"):
        if review.get(field) != acceptance.get(field):
            raise ContractError(f"terminal review/acceptance {field} mismatch")
    _require_nonempty_str(review.get("commit"), "review commit")
    _require_nonempty_str(review.get("reviewed_at"), "reviewed_at")
    if acceptance.get("decided_at") != review["reviewed_at"]:
        raise ContractError("terminal acceptance time differs from review")
    status = review.get("review_outcome")
    if status == "UNKNOWN":
        proof = evidence.get("terminal_proof")
        if result is not None or review.get("result_id") is not None or review.get("result_hash") is not None:
            raise ContractError("UNKNOWN cannot carry a result")
        if acceptance["approval"] != "ACCEPTED" or not isinstance(acceptance.get("force_accept_reason"), str) or not acceptance["force_accept_reason"].strip():
            raise ContractError("UNKNOWN requires exceptional ROOT acceptance")
        if not isinstance(proof, Mapping) or any((
            proof.get("schema") != "controller-status/v1",
            proof.get("lane_id") != evidence["lane_id"], proof.get("run_id") != evidence["run_id"],
            proof.get("controller_state") != "exited",
            not isinstance(proof.get("provider_state"), Mapping) or proof["provider_state"].get("state") != "exited",
            proof.get("result_state") not in {"absent", "invalid"},
            proof.get("recorded_status") != "provider_exited_no_result", proof.get("cleanup_proven") is not True,
        )):
            raise ContractError("UNKNOWN lacks terminal native no-result proof")
        digest = sha256_hex(proof)
        if review.get("terminal_proof_digest") != digest or acceptance.get("terminal_proof_digest") != digest:
            raise ContractError("UNKNOWN proof digest mismatch")
    else:
        if status not in {"PASS", "FAIL", "BLOCKED"}:
            raise ContractError("terminal review status invalid")
        validate_record(result, "result/v1")
        if result.get("lane_id") != evidence["lane_id"] or result.get("run_id") != evidence["run_id"] or result.get("outcome") not in {"PASS", "FAIL", "BLOCKED"}:
            raise ContractError("terminal result mismatch")
        if review.get("result_id") != evidence["run_id"] or review.get("result_hash") != result["content_hash"] or evidence.get("terminal_proof") is not None:
            raise ContractError("terminal result/review link mismatch")
        if "terminal_proof_digest" in review or "terminal_proof_digest" in acceptance:
            raise ContractError("ordinary result cannot carry UNKNOWN proof")
        if acceptance["approval"] == "ACCEPTED" and status != "PASS" and (not isinstance(acceptance.get("force_accept_reason"), str) or not acceptance["force_accept_reason"].strip()):
            raise ContractError("forced acceptance reason missing")


def make_rejected_native_attempt(evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Identify one exact ROOT-rejected native review without fixing quality."""
    validate_native_terminal_evidence(evidence)
    if evidence["acceptance"]["approval"] != "REJECTED":
        raise ContractError("rejected native attempt requires ROOT REJECTED")
    operation = evidence["dispatch"]["operation"]
    record = {
        "schema": REJECTED_NATIVE_ATTEMPT_SCHEMA,
        "rejected_attempt_id": sha256_hex({
            "decision_id": evidence["decision_id"],
            "operation_id": operation["operation_id"],
            "evidence_digest": evidence["content_hash"],
        }),
        "decision_id": evidence["decision_id"],
        "operation_id": operation["operation_id"],
        "run_id": evidence["run_id"],
        "result_digest": evidence["result"]["content_hash"],
        "review_digest": evidence["review"]["content_hash"],
        "acceptance_digest": evidence["acceptance"]["content_hash"],
        "evidence_digest": evidence["content_hash"],
        "terminal_evidence": dict(evidence),
    }
    record["content_hash"] = content_hash(record)
    return record


def normalize_experience_scope(scope: Mapping[str, Any]) -> dict[str, str]:
    """Validate the exact application/project/namespace/owner boundary."""

    if not isinstance(scope, Mapping):
        raise ContractError("experience scope must be an object")
    return {
        field: _require_nonempty_str(scope.get(field), f"scope.{field}")
        for field in ("application", "project", "namespace", "owner")
    }


def _normalize_string_refs(
    values: Iterable[str], field: str, *, sort_values: bool = True
) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise ContractError(f"{field} must be a list of strings")
    result: list[str] = []
    for value in values:
        normalized = _require_nonempty_str(value, field)
        if normalized in result:
            raise ContractError(f"{field} must not contain duplicate values")
        result.append(normalized)
    return sorted(result) if sort_values else result


def _validate_outcome_provenance(
    *,
    task_card: Mapping[str, Any],
    plan: Mapping[str, Any],
    decision: Mapping[str, Any],
    outcome: Mapping[str, Any],
) -> None:
    validate_task_plan_binding(task_card, plan)
    validate_decision(decision)
    validate_outcome(outcome)
    expected = {
        "decision_id": decision["decision_id"],
        "task_card_digest": task_card["content_hash"],
        "objective_id": plan["objective_id"],
        "plan_id": plan["plan_id"],
        "plan_digest": plan["content_hash"],
    }
    for field, value in expected.items():
        if outcome.get(field) != value:
            raise ContractError(f"outcome {field.replace('_', ' ')} does not match provenance")
    if decision["task_card_digest"] != task_card["content_hash"]:
        raise ContractError("decision task card digest does not match provenance")
    if decision["objective_id"] != plan["objective_id"]:
        raise ContractError("decision objective does not match provenance")
    if decision["plan_id"] != plan["plan_id"]:
        raise ContractError("decision plan does not match provenance")
    if decision["plan_digest"] != plan["content_hash"]:
        raise ContractError("decision plan digest does not match provenance")
    if plan.get("state") != "accepted":
        raise ContractError("reviewed trajectory requires an accepted plan")


def make_review_receipt(
    *,
    review_id: str,
    outcome: Mapping[str, Any],
    decision: Mapping[str, Any],
    task_card: Mapping[str, Any],
    plan: Mapping[str, Any],
    reviewed_by: str,
    evidence_refs: Iterable[str],
    state: str = "reviewed",
    protected_source_refs: Iterable[str] = (),
    raw_evidence: str,
    failed_hypotheses: Iterable[str] = (),
) -> dict[str, Any]:
    """Bind existing review evidence to one exact terminal product outcome.

    This record retains references to ROOT/harness review evidence.  It does
    not create a replacement review workflow or treat a worker narrative as
    authoritative evidence.
    """

    _validate_outcome_provenance(
        task_card=task_card, plan=plan, decision=decision, outcome=outcome
    )
    selected_review_id = _require_nonempty_str(review_id, "review_id")
    reviewer = _require_nonempty_str(reviewed_by, "reviewed_by")
    if reviewer != "ROOT":
        raise ContractError("only ROOT may review a reusable trajectory")
    if state not in REVIEW_STATES:
        raise ContractError(f"unknown review state: {state!r}")
    references = _normalize_string_refs(evidence_refs, "evidence_refs")
    if not references:
        raise ContractError("evidence_refs must not be empty")
    protected = _normalize_string_refs(
        protected_source_refs, "protected_source_refs"
    )
    evidence = _require_nonempty_str(raw_evidence, "raw_evidence")
    hypotheses = _normalize_string_refs(
        failed_hypotheses, "failed_hypotheses", sort_values=False
    )
    receipt_id = sha256_hex(
        {
            "review_id": selected_review_id,
            "outcome_id": outcome["outcome_id"],
            "decision_id": decision["decision_id"],
            "task_card_digest": task_card["content_hash"],
            "task_text": task_card["task"],
            "objective_id": plan["objective_id"],
            "run_id": outcome["linked_run_id"],
            "plan_id": plan["plan_id"],
            "plan_digest": plan["content_hash"],
            "route": plan["route"],
            "reviewed_by": reviewer,
            "state": state,
            "evidence_refs": references,
            "protected_source_refs": protected,
            "raw_evidence": evidence,
            "failed_hypotheses": hypotheses,
        }
    )
    record: dict[str, Any] = {
        "schema": REVIEW_RECEIPT_SCHEMA,
        "review_receipt_id": receipt_id,
        "review_id": selected_review_id,
        "outcome_id": outcome["outcome_id"],
        "decision_id": decision["decision_id"],
        "task_card_digest": task_card["content_hash"],
        "task_text": task_card["task"],
        "objective_id": plan["objective_id"],
        "run_id": outcome["linked_run_id"],
        "plan_id": plan["plan_id"],
        "plan_digest": plan["content_hash"],
        "route": plan["route"],
        "reviewed_by": reviewer,
        "state": state,
        "evidence_refs": references,
        "protected_source_refs": protected,
        "raw_evidence": evidence,
        "failed_hypotheses": hypotheses,
        "reviewed_at": outcome["observed_at"],
    }
    record["content_hash"] = content_hash(record)
    validate_review_receipt(record)
    return record


def validate_review_receipt(record: Mapping[str, Any]) -> None:
    validate_record(record, REVIEW_RECEIPT_SCHEMA)
    for field in (
        "review_receipt_id",
        "review_id",
        "outcome_id",
        "decision_id",
        "task_card_digest",
        "task_text",
        "objective_id",
        "run_id",
        "plan_id",
        "plan_digest",
        "route",
        "reviewed_by",
        "state",
        "raw_evidence",
        "reviewed_at",
    ):
        _require_nonempty_str(record.get(field), field)
    if record["state"] not in REVIEW_STATES:
        raise ContractError(f"unknown review state: {record['state']!r}")
    if record["reviewed_by"] != "ROOT":
        raise ContractError("only ROOT may review a reusable trajectory")
    if record["route"] not in ROUTES:
        raise ContractError(f"unknown route: {record['route']!r}")
    evidence_refs = record.get("evidence_refs")
    if not isinstance(evidence_refs, list) or not evidence_refs:
        raise ContractError("evidence_refs must be a nonempty list")
    normalized_evidence_refs = _normalize_string_refs(evidence_refs, "evidence_refs")
    if evidence_refs != normalized_evidence_refs:
        raise ContractError("evidence_refs must use canonical ordering")
    protected = record.get("protected_source_refs")
    if not isinstance(protected, list):
        raise ContractError("protected_source_refs must be a list")
    normalized_protected = _normalize_string_refs(protected, "protected_source_refs")
    if protected != normalized_protected:
        raise ContractError("protected_source_refs must use canonical ordering")
    failed_hypotheses = record.get("failed_hypotheses")
    if not isinstance(failed_hypotheses, list):
        raise ContractError("failed_hypotheses must be a list")
    normalized_hypotheses = _normalize_string_refs(
        failed_hypotheses, "failed_hypotheses", sort_values=False
    )
    if failed_hypotheses != normalized_hypotheses:
        raise ContractError("failed_hypotheses must preserve review ordering")
    expected_receipt_id = sha256_hex(
        {
            "review_id": record["review_id"],
            "outcome_id": record["outcome_id"],
            "decision_id": record["decision_id"],
            "task_card_digest": record["task_card_digest"],
            "task_text": record["task_text"],
            "objective_id": record["objective_id"],
            "run_id": record["run_id"],
            "plan_id": record["plan_id"],
            "plan_digest": record["plan_digest"],
            "route": record["route"],
            "reviewed_by": record["reviewed_by"],
            "state": record["state"],
            "evidence_refs": normalized_evidence_refs,
            "protected_source_refs": normalized_protected,
            "raw_evidence": record["raw_evidence"],
            "failed_hypotheses": normalized_hypotheses,
        }
    )
    if record["review_receipt_id"] != expected_receipt_id:
        raise ContractError("review receipt identity does not match its provenance")


def make_reviewed_trajectory(
    *,
    task_card: Mapping[str, Any],
    plan: Mapping[str, Any],
    decision: Mapping[str, Any],
    outcome: Mapping[str, Any],
    review_receipt: Mapping[str, Any],
    scope: Mapping[str, Any],
) -> dict[str, Any]:
    """Create one immutable reviewed trajectory from linked terminal evidence."""

    _validate_outcome_provenance(
        task_card=task_card, plan=plan, decision=decision, outcome=outcome
    )
    validate_review_receipt(review_receipt)
    for field, expected in {
        "outcome_id": outcome["outcome_id"],
        "decision_id": decision["decision_id"],
        "task_card_digest": task_card["content_hash"],
        "task_text": task_card["task"],
        "objective_id": plan["objective_id"],
        "run_id": outcome["linked_run_id"],
        "plan_id": plan["plan_id"],
        "plan_digest": plan["content_hash"],
        "route": plan["route"],
    }.items():
        if review_receipt.get(field) != expected:
            raise ContractError(
                f"review receipt {field.replace('_', ' ')} does not match terminal outcome"
            )
    if outcome["status"] == "PASS":
        trajectory_status = "reviewed_success"
    elif outcome["status"] in {"FAIL", "BLOCKED"}:
        trajectory_status = "reviewed_failure"
    else:
        raise ContractError("terminal UNKNOWN outcome cannot become reviewed experience")
    normalized_scope = normalize_experience_scope(scope)
    trajectory_id = sha256_hex(
        {
            "outcome_id": outcome["outcome_id"],
            "review_receipt_id": review_receipt["review_receipt_id"],
            "scope": normalized_scope,
        }
    )
    record: dict[str, Any] = {
        "schema": REVIEWED_TRAJECTORY_SCHEMA,
        "trajectory_id": trajectory_id,
        "outcome_id": outcome["outcome_id"],
        "task_card_digest": task_card["content_hash"],
        "task_text": review_receipt["task_text"],
        "objective_id": plan["objective_id"],
        "decision_id": decision["decision_id"],
        "run_id": outcome["linked_run_id"],
        "accepted_plan_id": plan["plan_id"],
        "accepted_plan_digest": plan["content_hash"],
        "route": review_receipt["route"],
        "scope": normalized_scope,
        "scope_digest": sha256_hex(normalized_scope),
        "status": trajectory_status,
        "review_receipt_id": review_receipt["review_receipt_id"],
        "review_id": review_receipt["review_id"],
        "review_receipt_digest": review_receipt["content_hash"],
        "review_state": review_receipt["state"],
        "reviewed_by": review_receipt["reviewed_by"],
        "reviewed_at": review_receipt["reviewed_at"],
        "evidence_refs": list(review_receipt["evidence_refs"]),
        "protected_source_refs": list(review_receipt["protected_source_refs"]),
        "failed_hypotheses": list(review_receipt["failed_hypotheses"]),
        "raw_evidence": review_receipt["raw_evidence"],
        "evidence_digest": outcome["evidence_digest"],
        "recorded_at": outcome["observed_at"],
    }
    record["content_hash"] = content_hash(record)
    validate_reviewed_trajectory(record)
    return record


def validate_reviewed_trajectory(record: Mapping[str, Any]) -> None:
    validate_record(record, REVIEWED_TRAJECTORY_SCHEMA)
    for field in (
        "trajectory_id",
        "outcome_id",
        "task_card_digest",
        "task_text",
        "objective_id",
        "decision_id",
        "run_id",
        "accepted_plan_id",
        "accepted_plan_digest",
        "route",
        "scope_digest",
        "status",
        "review_receipt_id",
        "review_id",
        "review_receipt_digest",
        "review_state",
        "reviewed_by",
        "reviewed_at",
        "raw_evidence",
        "evidence_digest",
        "recorded_at",
    ):
        _require_nonempty_str(record.get(field), field)
    if record["route"] not in ROUTES:
        raise ContractError(f"unknown route: {record['route']!r}")
    if record["status"] not in REVIEWED_TRAJECTORY_STATUSES:
        raise ContractError(f"unknown reviewed trajectory status: {record['status']!r}")
    if record["review_state"] not in REVIEW_STATES:
        raise ContractError(f"unknown review state: {record['review_state']!r}")
    scope = normalize_experience_scope(record.get("scope"))
    if record["scope_digest"] != sha256_hex(scope):
        raise ContractError("reviewed trajectory scope digest mismatch")
    expected_trajectory_id = sha256_hex(
        {
            "outcome_id": record["outcome_id"],
            "review_receipt_id": record["review_receipt_id"],
            "scope": scope,
        }
    )
    if record["trajectory_id"] != expected_trajectory_id:
        raise ContractError("reviewed trajectory identity does not match its provenance")
    for field in ("evidence_refs", "protected_source_refs", "failed_hypotheses"):
        values = record.get(field)
        if not isinstance(values, list):
            raise ContractError(f"{field} must be a list")
        _normalize_string_refs(values, field, sort_values=field != "failed_hypotheses")


def make_experience_ingestion(
    *,
    trajectory: Mapping[str, Any],
    destination: str,
    session_id: str,
    payload_digest: str,
) -> dict[str, Any]:
    """Create the durable intent for one optional EverOS representation write."""

    validate_reviewed_trajectory(trajectory)
    selected_destination = _require_nonempty_str(destination, "destination")
    selected_session = _require_nonempty_str(session_id, "session_id")
    selected_payload_digest = _require_nonempty_str(payload_digest, "payload_digest")
    ingestion_id = sha256_hex(
        {
            "trajectory_id": trajectory["trajectory_id"],
            "destination": selected_destination,
            "session_id": selected_session,
        }
    )
    record: dict[str, Any] = {
        "schema": EXPERIENCE_INGESTION_SCHEMA,
        "ingestion_id": ingestion_id,
        "trajectory_id": trajectory["trajectory_id"],
        "scope": dict(trajectory["scope"]),
        "scope_digest": trajectory["scope_digest"],
        "destination": selected_destination,
        "session_id": selected_session,
        "payload_digest": selected_payload_digest,
        "status": "pending",
        "case_ids": [],
        "error": None,
        "version": 0,
        "created_at": trajectory["recorded_at"],
    }
    record["content_hash"] = content_hash(record)
    validate_experience_ingestion(record)
    return record


def validate_experience_ingestion(record: Mapping[str, Any]) -> None:
    validate_record(record, EXPERIENCE_INGESTION_SCHEMA)
    for field in (
        "ingestion_id",
        "trajectory_id",
        "scope_digest",
        "destination",
        "session_id",
        "payload_digest",
        "status",
        "created_at",
    ):
        _require_nonempty_str(record.get(field), field)
    scope = normalize_experience_scope(record.get("scope"))
    if record["scope_digest"] != sha256_hex(scope):
        raise ContractError("experience ingestion scope digest mismatch")
    expected_ingestion_id = sha256_hex(
        {
            "trajectory_id": record["trajectory_id"],
            "destination": record["destination"],
            "session_id": record["session_id"],
        }
    )
    if record["ingestion_id"] != expected_ingestion_id:
        raise ContractError("experience ingestion identity does not match its receipt")
    if record["status"] not in EXPERIENCE_INGESTION_STATUSES:
        raise ContractError(f"unknown experience ingestion status: {record['status']!r}")
    case_ids = record.get("case_ids")
    if not isinstance(case_ids, list):
        raise ContractError("case_ids must be a list")
    _normalize_string_refs(case_ids, "case_ids")
    if record.get("error") is not None and not isinstance(record["error"], str):
        raise ContractError("experience ingestion error must be a string or null")
    version = record.get("version")
    if not isinstance(version, int) or isinstance(version, bool) or version < 0:
        raise ContractError("experience ingestion version must be a nonnegative integer")
    if record["status"] == "confirmed" and not case_ids:
        raise ContractError("confirmed experience ingestion requires case_ids")


def make_case_receipt(
    *,
    trajectory: Mapping[str, Any],
    ingestion: Mapping[str, Any],
    source_case: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind one exact returned EverOS case to its reviewed local receipt."""

    validate_reviewed_trajectory(trajectory)
    validate_experience_ingestion(ingestion)
    if ingestion["trajectory_id"] != trajectory["trajectory_id"]:
        raise ContractError("case receipt ingestion does not belong to trajectory")
    if ingestion["scope_digest"] != trajectory["scope_digest"]:
        raise ContractError("case receipt ingestion scope does not match trajectory")
    if not isinstance(source_case, Mapping):
        raise ContractError("source_case must be an object")
    case_id = _require_nonempty_str(source_case.get("id"), "source_case.id")
    case_receipt_id = sha256_hex(
        {
            "case_id": case_id,
            "trajectory_id": trajectory["trajectory_id"],
            "scope_digest": trajectory["scope_digest"],
        }
    )
    record: dict[str, Any] = {
        "schema": CASE_RECEIPT_SCHEMA,
        "case_receipt_id": case_receipt_id,
        "case_id": case_id,
        "trajectory_id": trajectory["trajectory_id"],
        "ingestion_id": ingestion["ingestion_id"],
        "scope": dict(trajectory["scope"]),
        "scope_digest": trajectory["scope_digest"],
        "review_receipt_id": trajectory["review_receipt_id"],
        "review_receipt_digest": trajectory["review_receipt_digest"],
        "source_case": dict(source_case),
        "source_case_digest": sha256_hex(source_case),
        "created_at": trajectory["recorded_at"],
    }
    record["content_hash"] = content_hash(record)
    validate_case_receipt(record)
    return record


def validate_case_receipt(record: Mapping[str, Any]) -> None:
    validate_record(record, CASE_RECEIPT_SCHEMA)
    for field in (
        "case_receipt_id",
        "case_id",
        "trajectory_id",
        "ingestion_id",
        "scope_digest",
        "review_receipt_id",
        "review_receipt_digest",
        "source_case_digest",
        "created_at",
    ):
        _require_nonempty_str(record.get(field), field)
    scope = normalize_experience_scope(record.get("scope"))
    if record["scope_digest"] != sha256_hex(scope):
        raise ContractError("case receipt scope digest mismatch")
    source_case = record.get("source_case")
    if not isinstance(source_case, Mapping):
        raise ContractError("source_case must be an object")
    if source_case.get("id") != record["case_id"]:
        raise ContractError("case receipt source case id mismatch")
    if record["source_case_digest"] != sha256_hex(source_case):
        raise ContractError("case receipt source case digest mismatch")
    expected_case_receipt_id = sha256_hex(
        {
            "case_id": record["case_id"],
            "trajectory_id": record["trajectory_id"],
            "scope_digest": record["scope_digest"],
        }
    )
    if record["case_receipt_id"] != expected_case_receipt_id:
        raise ContractError("case receipt identity does not match its provenance")


def make_generated_skill_candidate(
    *,
    scope: Mapping[str, Any],
    skill_id: str,
    content: str,
    source_cases: Iterable[Mapping[str, Any]],
    metadata: Mapping[str, Any] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Record generated EverOS guidance as non-authoritative evidence."""

    normalized_scope = normalize_experience_scope(scope)
    selected_skill_id = _require_nonempty_str(skill_id, "skill_id")
    selected_content = _require_nonempty_str(content, "content")
    seen_case_ids: set[str] = set()
    normalized_sources: list[dict[str, str]] = []
    for source in source_cases:
        if not isinstance(source, Mapping):
            raise ContractError("generated skill source_cases must contain objects")
        normalized = {
            field: _require_nonempty_str(source.get(field), f"source_case.{field}")
            for field in (
                "case_id",
                "case_receipt_id",
                "trajectory_id",
                "review_receipt_id",
                "review_receipt_digest",
            )
        }
        if normalized["case_id"] in seen_case_ids:
            raise ContractError("generated skill source case ids must resolve uniquely")
        seen_case_ids.add(normalized["case_id"])
        normalized_sources.append(normalized)
    if not normalized_sources:
        raise ContractError("generated skill requires at least one source case")
    normalized_sources.sort(key=lambda source: source["case_id"])
    content_digest = sha256_hex({"content": selected_content})
    candidate_id = sha256_hex(
        {
            "scope": normalized_scope,
            "skill_id": selected_skill_id,
            "content_digest": content_digest,
            "source_cases": normalized_sources,
            "state": "proposed",
        }
    )
    record: dict[str, Any] = {
        "schema": GENERATED_SKILL_SCHEMA,
        "candidate_id": candidate_id,
        "skill_id": selected_skill_id,
        "origin": "generated",
        "state": "proposed",
        "scope": normalized_scope,
        "scope_digest": sha256_hex(normalized_scope),
        "content": selected_content,
        "content_digest": content_digest,
        "source_cases": normalized_sources,
        "metadata": dict(metadata or {}),
        "created_at": created_at or utc_now(),
    }
    record["content_hash"] = content_hash(record)
    validate_generated_skill_candidate(record)
    return record


def validate_generated_skill_candidate(record: Mapping[str, Any]) -> None:
    validate_record(record, GENERATED_SKILL_SCHEMA)
    for field in (
        "candidate_id",
        "skill_id",
        "origin",
        "state",
        "scope_digest",
        "content",
        "content_digest",
        "created_at",
    ):
        _require_nonempty_str(record.get(field), field)
    if record["origin"] != "generated":
        raise ContractError("generated skill candidate origin must remain generated")
    if record["state"] not in GENERATED_SKILL_STATES:
        raise ContractError("generated skill candidate must remain proposed")
    scope = normalize_experience_scope(record.get("scope"))
    if record["scope_digest"] != sha256_hex(scope):
        raise ContractError("generated skill candidate scope digest mismatch")
    if record["content_digest"] != sha256_hex({"content": record["content"]}):
        raise ContractError("generated skill candidate content digest mismatch")
    source_cases = record.get("source_cases")
    if not isinstance(source_cases, list) or not source_cases:
        raise ContractError("generated skill candidate requires source cases")
    if not isinstance(record.get("metadata"), Mapping):
        raise ContractError("generated skill candidate metadata must be an object")
    seen_case_ids: set[str] = set()
    normalized_sources: list[dict[str, str]] = []
    for source in source_cases:
        if not isinstance(source, Mapping):
            raise ContractError("generated skill source_cases must contain objects")
        case_id = _require_nonempty_str(source.get("case_id"), "source_case.case_id")
        if case_id in seen_case_ids:
            raise ContractError("generated skill source case ids must resolve uniquely")
        seen_case_ids.add(case_id)
        normalized_sources.append(
            {
                "case_id": case_id,
                "case_receipt_id": _require_nonempty_str(
                    source.get("case_receipt_id"), "source_case.case_receipt_id"
                ),
                "trajectory_id": _require_nonempty_str(
                    source.get("trajectory_id"), "source_case.trajectory_id"
                ),
                "review_receipt_id": _require_nonempty_str(
                    source.get("review_receipt_id"), "source_case.review_receipt_id"
                ),
                "review_receipt_digest": _require_nonempty_str(
                    source.get("review_receipt_digest"),
                    "source_case.review_receipt_digest",
                ),
            }
        )
    canonical_sources = sorted(normalized_sources, key=lambda source: source["case_id"])
    if source_cases != canonical_sources:
        raise ContractError("generated skill source cases must use canonical ordering")
    expected_candidate_id = sha256_hex(
        {
            "scope": scope,
            "skill_id": record["skill_id"],
            "content_digest": record["content_digest"],
            "source_cases": canonical_sources,
            "state": record["state"],
        }
    )
    if record["candidate_id"] != expected_candidate_id:
        raise ContractError("generated skill candidate identity does not match its provenance")


def make_skill_approval(
    *,
    approval_id: str,
    candidate: Mapping[str, Any],
    issuer: str,
    recipients: Iterable[str],
    approved_at: str,
    authority_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind explicit trusted approval to one immutable generated candidate."""

    validate_generated_skill_candidate(candidate)
    selected_approval_id = _require_nonempty_str(approval_id, "approval_id")
    selected_issuer = _require_nonempty_str(issuer, "issuer")
    selected_recipients = _normalize_string_refs(recipients, "recipients")
    if not selected_recipients:
        raise ContractError("approval recipients must not be empty")
    selected_approved_at = _require_nonempty_str(approved_at, "approved_at")
    selected_authority_evidence = _normalize_json_object(
        authority_evidence, "authority_evidence"
    )
    record: dict[str, Any] = {
        "schema": SKILL_APPROVAL_SCHEMA,
        "approval_id": selected_approval_id,
        "candidate_id": candidate["candidate_id"],
        "skill_id": candidate["skill_id"],
        "origin": candidate["origin"],
        "scope": dict(candidate["scope"]),
        "scope_digest": candidate["scope_digest"],
        "content_digest": candidate["content_digest"],
        "issuer": selected_issuer,
        "recipients": selected_recipients,
        "source_cases": [dict(item) for item in candidate["source_cases"]],
        "approved_at": selected_approved_at,
        "authority_evidence": selected_authority_evidence,
    }
    record["content_hash"] = content_hash(record)
    validate_skill_approval(record)
    return record


def validate_skill_approval(record: Mapping[str, Any]) -> None:
    validate_record(record, SKILL_APPROVAL_SCHEMA)
    for field in (
        "approval_id",
        "candidate_id",
        "skill_id",
        "origin",
        "scope_digest",
        "content_digest",
        "issuer",
        "approved_at",
    ):
        _require_nonempty_str(record.get(field), field)
    if record["origin"] != "generated":
        raise ContractError("approval cannot rewrite generated origin")
    scope = normalize_experience_scope(record.get("scope"))
    if record["scope_digest"] != sha256_hex(scope):
        raise ContractError("approval scope digest mismatch")
    recipients = record.get("recipients")
    if not isinstance(recipients, list) or not recipients:
        raise ContractError("approval recipients must be a nonempty list")
    _normalize_string_refs(recipients, "recipients")
    authority_evidence = _normalize_json_object(
        record.get("authority_evidence"), "authority_evidence"
    )
    if record.get("authority_evidence") != authority_evidence:
        raise ContractError("authority_evidence must use canonical JSON values")
    source_cases = record.get("source_cases")
    if not isinstance(source_cases, list) or not source_cases:
        raise ContractError("approval source cases must be a nonempty list")
    seen_case_ids: set[str] = set()
    for source in source_cases:
        if not isinstance(source, Mapping):
            raise ContractError("approval source cases must contain objects")
        case_id = _require_nonempty_str(source.get("case_id"), "source_case.case_id")
        if case_id in seen_case_ids:
            raise ContractError("approval source case ids must resolve uniquely")
        seen_case_ids.add(case_id)
        for field in (
            "case_receipt_id",
            "trajectory_id",
            "review_receipt_id",
            "review_receipt_digest",
        ):
            _require_nonempty_str(source.get(field), f"source_case.{field}")


# Trusted procedures deliberately use records separate from generated-skill
# candidates.  A Step-02 approval proves the source candidate; it does not
# approve a later change to references, predicates, or representation policy.


def _normalize_json_value(value: Any, field: str) -> Any:
    try:
        return json.loads(canonical_json(value).decode("utf-8"))
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{field} must be JSON serializable") from exc


def _normalize_recipient_scopes(values: Iterable[Mapping[str, Any]]) -> list[dict[str, str]]:
    if isinstance(values, (str, bytes)):
        raise ContractError("procedure recipients must be a collection of scopes")
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for value in values:
        scope = normalize_experience_scope(value)
        key = canonical_json(scope).decode("utf-8")
        if key in seen:
            raise ContractError("procedure recipients must not contain duplicate scopes")
        seen.add(key)
        result.append(scope)
    if not result:
        raise ContractError("procedure recipients must not be empty")
    return sorted(result, key=canonical_json)


def normalize_procedure_partition(partition: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize a full authorization partition, not just a scope label.

    ``private`` has one exact owner recipient, ``project`` stays within one
    project, and ``shared`` can name explicit cross-project recipients while
    remaining inside one application/namespace.  This intentionally keeps the
    destination boundary distinct from the source procedure's origin scope.
    """

    if not isinstance(partition, Mapping):
        raise ContractError("procedure partition must be an object")
    scope = _require_nonempty_str(partition.get("scope"), "partition.scope")
    if scope not in PROCEDURE_PARTITION_SCOPES:
        raise ContractError(f"unknown procedure partition scope: {scope!r}")
    application = _require_nonempty_str(
        partition.get("application"), "partition.application"
    )
    project = _require_nonempty_str(partition.get("project"), "partition.project")
    namespace = _require_nonempty_str(
        partition.get("namespace"), "partition.namespace"
    )
    recipients = _normalize_recipient_scopes(partition.get("recipients", ()))
    for recipient in recipients:
        if recipient["application"] != application or recipient["namespace"] != namespace:
            raise ContractError("procedure partition recipients cross application or namespace")
    owner = partition.get("owner")
    if scope == "private":
        owner = _require_nonempty_str(owner, "partition.owner")
        if len(recipients) != 1:
            raise ContractError("private procedure partition requires exactly one recipient")
        recipient = recipients[0]
        if recipient["project"] != project or recipient["owner"] != owner:
            raise ContractError("private procedure partition recipient must match owner and project")
    else:
        if owner is not None:
            raise ContractError("only private procedure partitions may include an owner")
        if scope == "project" and any(
            recipient["project"] != project for recipient in recipients
        ):
            raise ContractError("project procedure partition recipients must stay in its project")
    normalized: dict[str, Any] = {
        "scope": scope,
        "application": application,
        "project": project,
        "namespace": namespace,
        "recipients": recipients,
    }
    if scope == "private":
        normalized["owner"] = owner
    return normalized


def procedure_partition_id(partition: Mapping[str, Any]) -> str:
    return sha256_hex(
        {
            "domain": "trusted-procedure-partition/v1",
            "partition": normalize_procedure_partition(partition),
        }
    )


def _normalize_reference_list(values: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(values, (str, bytes)):
        raise ContractError("procedure references must be a list")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, Mapping):
            raise ContractError("procedure references must contain objects")
        reference_id = _require_nonempty_str(value.get("id"), "procedure reference id")
        content = _require_nonempty_str(value.get("content"), "procedure reference content")
        if reference_id in seen:
            raise ContractError("procedure references must not contain duplicate ids")
        seen.add(reference_id)
        item = _normalize_json_value(value, "procedure reference")
        if not isinstance(item, dict):
            raise ContractError("procedure reference must remain an object")
        item["id"] = reference_id
        item["content"] = content
        normalized.append(item)
    return sorted(normalized, key=lambda item: str(item["id"]))


_PREDICATE_OPERATORS = frozenset({"equals", "in", "contains", "present", "absent"})
_PREDICATE_GROUPS = ("all", "any", "none")
_PROCEDURE_PREDICATE_KINDS = ("applicability", "conflicts", "capabilities", "routes")


def _normalize_predicate_condition(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ContractError(f"{field} entries must be objects")
    name = _require_nonempty_str(value.get("field"), f"{field}.field")
    operator = _require_nonempty_str(value.get("operator"), f"{field}.operator")
    if operator not in _PREDICATE_OPERATORS:
        raise ContractError(f"unsupported predicate operator: {operator!r}")
    if operator in {"present", "absent"}:
        if "value" in value and value["value"] is not None:
            raise ContractError(f"{operator} predicate must not carry a value")
        return {"field": name, "operator": operator}
    if "value" not in value:
        raise ContractError(f"{operator} predicate requires a value")
    expected = _normalize_json_value(value["value"], f"{field}.value")
    if operator == "in":
        if not isinstance(expected, list) or not expected:
            raise ContractError("in predicate value must be a nonempty list")
    return {"field": name, "operator": operator, "value": expected}


def _normalize_predicate_expression(value: Any, field: str) -> dict[str, list[dict[str, Any]]]:
    if value is None:
        raise ContractError(f"{field} must be an object; use an empty object for no constraint")
    if not isinstance(value, Mapping):
        raise ContractError(f"{field} must be an object")
    unexpected = set(value) - set(_PREDICATE_GROUPS)
    if unexpected:
        raise ContractError(f"{field} contains unknown predicate groups")
    normalized: dict[str, list[dict[str, Any]]] = {}
    for group in _PREDICATE_GROUPS:
        entries = value.get(group, [])
        if not isinstance(entries, list):
            raise ContractError(f"{field}.{group} must be a list")
        conditions = [
            _normalize_predicate_condition(item, f"{field}.{group}") for item in entries
        ]
        deduplicated = {canonical_json(item).decode("utf-8") for item in conditions}
        if len(deduplicated) != len(conditions):
            raise ContractError(f"{field}.{group} must not contain duplicate conditions")
        normalized[group] = sorted(conditions, key=canonical_json)
    return normalized


def normalize_procedure_predicates(value: Mapping[str, Any]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    if not isinstance(value, Mapping):
        raise ContractError("procedure predicates must be an object")
    if set(value) != set(_PROCEDURE_PREDICATE_KINDS):
        raise ContractError("procedure predicates must define applicability, conflicts, capabilities, and routes")
    return {
        kind: _normalize_predicate_expression(value[kind], f"predicates.{kind}")
        for kind in _PROCEDURE_PREDICATE_KINDS
    }


def _predicate_expression_is_empty(expression: Mapping[str, Any]) -> bool:
    return all(not expression.get(group) for group in _PREDICATE_GROUPS)


def _lookup_predicate_fact(facts: Mapping[str, Any], field: str) -> tuple[bool, Any]:
    if field in facts:
        return True, facts[field]
    current: Any = facts
    for part in field.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return False, None
        current = current[part]
    return True, current


def _evaluate_predicate_condition(condition: Mapping[str, Any], facts: Mapping[str, Any]) -> bool | None:
    present, actual = _lookup_predicate_fact(facts, str(condition["field"]))
    operator = condition["operator"]
    if operator == "present":
        return present
    if operator == "absent":
        return not present
    if not present:
        return None
    expected = condition["value"]
    if operator == "equals":
        return actual == expected
    if operator == "in":
        return actual in expected
    if operator == "contains":
        try:
            return expected in actual
        except TypeError:
            return False
    raise ContractError(f"unsupported predicate operator: {operator!r}")


def _evaluate_predicate_expression(
    expression: Mapping[str, Any], facts: Mapping[str, Any]
) -> bool | None:
    normalized = _normalize_predicate_expression(expression, "predicate expression")
    all_values = [_evaluate_predicate_condition(item, facts) for item in normalized["all"]]
    if any(value is False for value in all_values):
        return False
    if any(value is None for value in all_values):
        return None
    any_values = [_evaluate_predicate_condition(item, facts) for item in normalized["any"]]
    if any_values:
        if any(value is True for value in any_values):
            any_result: bool | None = True
        elif any(value is None for value in any_values):
            any_result = None
        else:
            any_result = False
        if any_result is not True:
            return any_result
    none_values = [_evaluate_predicate_condition(item, facts) for item in normalized["none"]]
    if any(value is True for value in none_values):
        return False
    if any(value is None for value in none_values):
        return None
    return True


def procedure_predicates_match(
    predicates: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    route: str,
) -> bool:
    """Evaluate structured procedure predicates with explicit fail-closed unknowns."""

    normalized = normalize_procedure_predicates(predicates)
    if not isinstance(facts, Mapping):
        raise ContractError("procedure facts must be an object")
    if route not in ROUTES:
        raise ContractError(f"unknown route: {route!r}")
    effective_facts = dict(facts)
    effective_facts["route"] = route
    for kind in ("applicability", "capabilities", "routes"):
        if _evaluate_predicate_expression(normalized[kind], effective_facts) is not True:
            return False
    conflicts = normalized["conflicts"]
    if not _predicate_expression_is_empty(conflicts):
        # A conflict with an unknown required fact cannot safely be ignored.
        if _evaluate_predicate_expression(conflicts, effective_facts) is not False:
            return False
    return True


def make_procedure_revision(
    *,
    logical_name: str,
    origin: str,
    origin_scope: Mapping[str, Any],
    body: str,
    references: Iterable[Mapping[str, Any]],
    predicates: Mapping[str, Any],
    source: Mapping[str, Any],
    metadata: Mapping[str, Any] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Create an immutable, origin-qualified behavior revision.

    Derived search representations deliberately do not participate in the
    behavior revision identity.  They are immutable sibling projections that
    can be rebuilt without rewriting approved procedure meaning.
    """

    name = _require_nonempty_str(logical_name, "logical_name")
    selected_origin = _require_nonempty_str(origin, "origin")
    if selected_origin not in PROCEDURE_ORIGINS:
        raise ContractError(f"unknown procedure origin: {selected_origin!r}")
    normalized_scope = normalize_experience_scope(origin_scope)
    selected_body = _require_nonempty_str(body, "procedure body")
    normalized_references = _normalize_reference_list(references)
    normalized_predicates = normalize_procedure_predicates(predicates)
    normalized_source = _normalize_json_object(source, "procedure source")
    if not isinstance(normalized_source.get("kind"), str) or not normalized_source["kind"].strip():
        raise ContractError("procedure source requires a nonempty kind")
    normalized_metadata = _normalize_json_value(metadata or {}, "procedure metadata")
    if not isinstance(normalized_metadata, dict):
        raise ContractError("procedure metadata must be an object")
    logical_id = sha256_hex(
        {
            "domain": "trusted-procedure-logical/v1",
            "logical_name": name,
            "origin": selected_origin,
            "origin_scope": normalized_scope,
        }
    )
    behavior = {
        "body": selected_body,
        "references": normalized_references,
        "predicates": normalized_predicates,
        "metadata": normalized_metadata,
    }
    behavior_digest = sha256_hex(behavior)
    revision_id = sha256_hex(
        {
            "domain": "trusted-procedure-revision/v1",
            "logical_id": logical_id,
            "behavior_digest": behavior_digest,
        }
    )
    record: dict[str, Any] = {
        "schema": PROCEDURE_REVISION_SCHEMA,
        "logical_id": logical_id,
        "logical_name": name,
        "origin": selected_origin,
        "origin_scope": normalized_scope,
        "origin_scope_digest": sha256_hex(normalized_scope),
        "revision_id": revision_id,
        "behavior": behavior,
        "behavior_digest": behavior_digest,
        "source": normalized_source,
        "source_digest": sha256_hex(normalized_source),
        "created_at": created_at or utc_now(),
    }
    record["content_hash"] = content_hash(record)
    validate_procedure_revision(record)
    return record


def validate_procedure_revision(record: Mapping[str, Any]) -> None:
    validate_record(record, PROCEDURE_REVISION_SCHEMA)
    for field in (
        "logical_id",
        "logical_name",
        "origin",
        "origin_scope_digest",
        "revision_id",
        "behavior_digest",
        "source_digest",
        "created_at",
    ):
        _require_nonempty_str(record.get(field), field)
    origin = record["origin"]
    if origin not in PROCEDURE_ORIGINS:
        raise ContractError(f"unknown procedure origin: {origin!r}")
    scope = normalize_experience_scope(record.get("origin_scope"))
    if record["origin_scope_digest"] != sha256_hex(scope):
        raise ContractError("procedure origin scope digest mismatch")
    expected_logical_id = sha256_hex(
        {
            "domain": "trusted-procedure-logical/v1",
            "logical_name": record["logical_name"],
            "origin": origin,
            "origin_scope": scope,
        }
    )
    if record["logical_id"] != expected_logical_id:
        raise ContractError("procedure logical identity does not match its origin")
    behavior = record.get("behavior")
    if not isinstance(behavior, Mapping):
        raise ContractError("procedure behavior must be an object")
    normalized_behavior = {
        "body": _require_nonempty_str(behavior.get("body"), "procedure body"),
        "references": _normalize_reference_list(behavior.get("references", ())),
        "predicates": normalize_procedure_predicates(behavior.get("predicates")),
        "metadata": _normalize_json_value(behavior.get("metadata"), "procedure metadata"),
    }
    if not isinstance(normalized_behavior["metadata"], dict):
        raise ContractError("procedure metadata must be an object")
    if dict(behavior) != normalized_behavior:
        raise ContractError("procedure behavior must use canonical representation")
    if record["behavior_digest"] != sha256_hex(normalized_behavior):
        raise ContractError("procedure behavior digest mismatch")
    expected_revision_id = sha256_hex(
        {
            "domain": "trusted-procedure-revision/v1",
            "logical_id": record["logical_id"],
            "behavior_digest": record["behavior_digest"],
        }
    )
    if record["revision_id"] != expected_revision_id:
        raise ContractError("procedure revision identity does not match its behavior")
    source = _normalize_json_object(record.get("source"), "procedure source")
    if not isinstance(source.get("kind"), str) or not source["kind"].strip():
        raise ContractError("procedure source requires a nonempty kind")
    if record["source"] != source:
        raise ContractError("procedure source must use canonical JSON values")
    if record["source_digest"] != sha256_hex(source):
        raise ContractError("procedure source digest mismatch")


def make_procedure_approval(
    *,
    approval_id: str,
    procedure: Mapping[str, Any],
    issuer: str,
    recipients: Iterable[Mapping[str, Any]],
    authority_evidence: Mapping[str, Any],
    approved_at: str | None = None,
) -> dict[str, Any]:
    """Bind trusted approval to one exact immutable procedure revision."""

    validate_procedure_revision(procedure)
    selected_approval_id = _require_nonempty_str(approval_id, "approval_id")
    selected_issuer = _require_nonempty_str(issuer, "issuer")
    normalized_recipients = _normalize_recipient_scopes(recipients)
    if any(
        recipient["application"] != procedure["origin_scope"]["application"]
        or recipient["namespace"] != procedure["origin_scope"]["namespace"]
        for recipient in normalized_recipients
    ):
        raise ContractError(
            "procedure approval recipients must remain in the origin application and namespace"
        )
    evidence = _normalize_json_object(authority_evidence, "authority_evidence")
    record: dict[str, Any] = {
        "schema": PROCEDURE_APPROVAL_SCHEMA,
        "approval_id": selected_approval_id,
        "logical_id": procedure["logical_id"],
        "revision_id": procedure["revision_id"],
        "procedure_digest": procedure["content_hash"],
        "behavior_digest": procedure["behavior_digest"],
        "source_digest": procedure["source_digest"],
        "origin": procedure["origin"],
        "origin_scope": dict(procedure["origin_scope"]),
        "origin_scope_digest": procedure["origin_scope_digest"],
        "issuer": selected_issuer,
        "recipients": normalized_recipients,
        "authority_evidence": evidence,
        "approved_at": approved_at or utc_now(),
    }
    record["content_hash"] = content_hash(record)
    validate_procedure_approval(record, procedure=procedure)
    return record


def validate_procedure_approval(
    record: Mapping[str, Any],
    *,
    procedure: Mapping[str, Any] | None = None,
) -> None:
    validate_record(record, PROCEDURE_APPROVAL_SCHEMA)
    for field in (
        "approval_id",
        "logical_id",
        "revision_id",
        "procedure_digest",
        "behavior_digest",
        "source_digest",
        "origin",
        "origin_scope_digest",
        "issuer",
        "approved_at",
    ):
        _require_nonempty_str(record.get(field), field)
    if record["origin"] not in PROCEDURE_ORIGINS:
        raise ContractError("procedure approval has an unknown origin")
    scope = normalize_experience_scope(record.get("origin_scope"))
    if record["origin_scope_digest"] != sha256_hex(scope):
        raise ContractError("procedure approval origin scope digest mismatch")
    recipients = _normalize_recipient_scopes(record.get("recipients", ()))
    if record.get("recipients") != recipients:
        raise ContractError("procedure approval recipients must use canonical ordering")
    if any(
        recipient["application"] != scope["application"]
        or recipient["namespace"] != scope["namespace"]
        for recipient in recipients
    ):
        raise ContractError(
            "procedure approval recipients must remain in the origin application and namespace"
        )
    evidence = _normalize_json_object(record.get("authority_evidence"), "authority_evidence")
    if record.get("authority_evidence") != evidence:
        raise ContractError("procedure approval authority evidence must use canonical JSON values")
    if procedure is not None:
        validate_procedure_revision(procedure)
        expected = {
            "logical_id": procedure["logical_id"],
            "revision_id": procedure["revision_id"],
            "procedure_digest": procedure["content_hash"],
            "behavior_digest": procedure["behavior_digest"],
            "source_digest": procedure["source_digest"],
            "origin": procedure["origin"],
            "origin_scope": procedure["origin_scope"],
            "origin_scope_digest": procedure["origin_scope_digest"],
        }
        for field, expected_value in expected.items():
            if record.get(field) != expected_value:
                raise ContractError(
                    f"procedure approval does not bind the exact procedure {field}"
                )


def make_procedure_compact_representation(
    *, procedure: Mapping[str, Any], content: Any,
) -> dict[str, Any]:
    """Create a distinct immutable compact body for one approved revision."""

    validate_procedure_revision(procedure)
    normalized = _normalize_json_value(content, "compact procedure content")
    if not isinstance(normalized, (str, dict, list)) or not normalized:
        raise ContractError("compact procedure content must be nonempty")
    digest = sha256_hex(normalized)
    record = {
        "schema": PROCEDURE_COMPACT_REPRESENTATION_SCHEMA,
        "logical_id": procedure["logical_id"],
        "revision_id": procedure["revision_id"],
        "procedure_digest": procedure["content_hash"],
        "content": normalized,
        "content_digest": digest,
        "representation_id": sha256_hex({
            "domain": PROCEDURE_COMPACT_REPRESENTATION_SCHEMA,
            "revision_id": procedure["revision_id"],
            "procedure_digest": procedure["content_hash"],
            "content_digest": digest,
        }),
    }
    record["content_hash"] = content_hash(record)
    validate_procedure_compact_representation(record, procedure=procedure)
    return record


def validate_procedure_compact_representation(
    record: Mapping[str, Any], *, procedure: Mapping[str, Any] | None = None,
) -> None:
    validate_record(record, PROCEDURE_COMPACT_REPRESENTATION_SCHEMA)
    if set(record) != {
        "schema", "logical_id", "revision_id", "procedure_digest", "content",
        "content_digest", "representation_id", "content_hash",
    }:
        raise ContractError("compact procedure representation has unexpected fields")
    for field in ("logical_id", "revision_id", "procedure_digest", "representation_id"):
        _require_nonempty_str(record.get(field), f"compact {field}")
    content = _normalize_json_value(record.get("content"), "compact procedure content")
    if not isinstance(content, (str, dict, list)) or not content:
        raise ContractError("compact procedure content must be nonempty")
    if record["content_digest"] != sha256_hex(content):
        raise ContractError("compact procedure content digest mismatch")
    if record["representation_id"] != sha256_hex({
        "domain": PROCEDURE_COMPACT_REPRESENTATION_SCHEMA,
        "revision_id": record["revision_id"],
        "procedure_digest": record["procedure_digest"],
        "content_digest": record["content_digest"],
    }):
        raise ContractError("compact procedure representation identity mismatch")
    if procedure is not None:
        validate_procedure_revision(procedure)
        for field, value in (
            ("logical_id", procedure["logical_id"]),
            ("revision_id", procedure["revision_id"]),
            ("procedure_digest", procedure["content_hash"]),
        ):
            if record[field] != value:
                raise ContractError(f"compact representation does not bind procedure {field}")


def make_procedure_compact_approval(
    *, procedure: Mapping[str, Any], full_approval: Mapping[str, Any],
    representation: Mapping[str, Any], approval_id: str, issuer: str,
    authority_evidence: Mapping[str, Any], approved_at: str | None = None,
) -> dict[str, Any]:
    """Approve exact compact bytes independently of the full-body approval."""

    validate_procedure_approval(full_approval, procedure=procedure)
    validate_procedure_compact_representation(representation, procedure=procedure)
    selected_id = _require_nonempty_str(approval_id, "compact approval_id")
    selected_issuer = _require_nonempty_str(issuer, "compact issuer")
    if selected_id == full_approval["approval_id"] or selected_issuer != full_approval["issuer"]:
        raise ContractError("compact procedure needs separate approval by the trusted issuer")
    record = {
        "schema": PROCEDURE_COMPACT_APPROVAL_SCHEMA,
        "approval_id": selected_id,
        "issuer": selected_issuer,
        "logical_id": procedure["logical_id"],
        "revision_id": procedure["revision_id"],
        "procedure_digest": procedure["content_hash"],
        "full_approval_id": full_approval["approval_id"],
        "full_approval_digest": full_approval["content_hash"],
        "representation_id": representation["representation_id"],
        "content_digest": representation["content_digest"],
        "authority_evidence": _normalize_json_object(authority_evidence, "compact authority_evidence"),
        "approved_at": approved_at or utc_now(),
    }
    record["content_hash"] = content_hash(record)
    validate_procedure_compact_approval(
        record, procedure=procedure, full_approval=full_approval,
        representation=representation,
    )
    return record


def validate_procedure_compact_approval(
    record: Mapping[str, Any], *, procedure: Mapping[str, Any] | None = None,
    full_approval: Mapping[str, Any] | None = None,
    representation: Mapping[str, Any] | None = None,
) -> None:
    validate_record(record, PROCEDURE_COMPACT_APPROVAL_SCHEMA)
    if set(record) != {
        "schema", "approval_id", "issuer", "logical_id", "revision_id",
        "procedure_digest", "full_approval_id", "full_approval_digest",
        "representation_id", "content_digest", "authority_evidence",
        "approved_at", "content_hash",
    }:
        raise ContractError("compact procedure approval has unexpected fields")
    for field in (
        "approval_id", "issuer", "logical_id", "revision_id", "procedure_digest",
        "full_approval_id", "full_approval_digest", "representation_id",
        "content_digest", "approved_at",
    ):
        _require_nonempty_str(record.get(field), f"compact {field}")
    _normalize_json_object(record.get("authority_evidence"), "compact authority_evidence")
    if record["approval_id"] == record["full_approval_id"]:
        raise ContractError("compact procedure approval must be separate")
    if procedure is not None:
        validate_procedure_revision(procedure)
        for field, value in (
            ("logical_id", procedure["logical_id"]),
            ("revision_id", procedure["revision_id"]),
            ("procedure_digest", procedure["content_hash"]),
        ):
            if record[field] != value:
                raise ContractError(f"compact approval does not bind procedure {field}")
    if full_approval is not None:
        validate_procedure_approval(full_approval, procedure=procedure)
        for field, value in (
            ("full_approval_id", full_approval["approval_id"]),
            ("full_approval_digest", full_approval["content_hash"]),
            ("issuer", full_approval["issuer"]),
        ):
            if record[field] != value:
                raise ContractError(f"compact approval does not bind full approval {field}")
    if representation is not None:
        validate_procedure_compact_representation(representation, procedure=procedure)
        for field, value in (
            ("logical_id", representation["logical_id"]),
            ("revision_id", representation["revision_id"]),
            ("procedure_digest", representation["procedure_digest"]),
            ("representation_id", representation["representation_id"]),
            ("content_digest", representation["content_digest"]),
        ):
            if record[field] != value:
                raise ContractError(f"compact approval does not bind representation {field}")


def partition_is_authorized(
    approval: Mapping[str, Any], partition: Mapping[str, Any]
) -> bool:
    """Return whether a partition narrows, rather than broadens, approval."""

    validate_procedure_approval(approval)
    normalized = normalize_procedure_partition(partition)
    approval_recipients = {
        canonical_json(item).decode("utf-8") for item in approval["recipients"]
    }
    return all(
        canonical_json(recipient).decode("utf-8") in approval_recipients
        for recipient in normalized["recipients"]
    )


def make_procedure_representation(
    *,
    procedure: Mapping[str, Any],
    model: str,
    dimensions: int,
    metric: str,
    sanitizer_version: str,
    search_text: str,
    vector: Iterable[float],
    created_at: str | None = None,
) -> dict[str, Any]:
    """Create one immutable, separately identified search projection."""

    validate_procedure_revision(procedure)
    selected_model = _require_nonempty_str(model, "representation model")
    if not isinstance(dimensions, int) or isinstance(dimensions, bool) or dimensions < 1:
        raise ContractError("representation dimensions must be a positive integer")
    selected_metric = _require_nonempty_str(metric, "representation metric")
    if selected_metric not in PROCEDURE_METRICS:
        raise ContractError(f"unknown representation metric: {selected_metric!r}")
    selected_sanitizer = _require_nonempty_str(
        sanitizer_version, "representation sanitizer_version"
    )
    selected_search_text = _require_nonempty_str(search_text, "representation search_text")
    if isinstance(vector, (str, bytes)):
        raise ContractError("representation vector must be a list of finite numbers")
    normalized_vector: list[float] = []
    for item in vector:
        if not isinstance(item, (int, float)) or isinstance(item, bool) or not math.isfinite(item):
            raise ContractError("representation vector must contain finite numbers")
        normalized_vector.append(float(item))
    if len(normalized_vector) != dimensions:
        raise ContractError("representation dimensions do not match vector length")
    payload = {
        "model": selected_model,
        "dimensions": dimensions,
        "metric": selected_metric,
        "sanitizer_version": selected_sanitizer,
        "search_text": selected_search_text,
        "vector": normalized_vector,
    }
    representation_id = sha256_hex(
        {
            "domain": "trusted-procedure-representation/v1",
            "revision_id": procedure["revision_id"],
            "payload": payload,
        }
    )
    record: dict[str, Any] = {
        "schema": PROCEDURE_REPRESENTATION_SCHEMA,
        "representation_id": representation_id,
        "logical_id": procedure["logical_id"],
        "revision_id": procedure["revision_id"],
        "procedure_digest": procedure["content_hash"],
        "behavior_digest": procedure["behavior_digest"],
        **payload,
        "payload_digest": sha256_hex(payload),
        "created_at": created_at or utc_now(),
    }
    record["content_hash"] = content_hash(record)
    validate_procedure_representation(record, procedure=procedure)
    return record


def validate_procedure_representation(
    record: Mapping[str, Any],
    *,
    procedure: Mapping[str, Any] | None = None,
) -> None:
    validate_record(record, PROCEDURE_REPRESENTATION_SCHEMA)
    for field in (
        "representation_id",
        "logical_id",
        "revision_id",
        "procedure_digest",
        "behavior_digest",
        "model",
        "metric",
        "sanitizer_version",
        "search_text",
        "payload_digest",
        "created_at",
    ):
        _require_nonempty_str(record.get(field), field)
    dimensions = record.get("dimensions")
    if not isinstance(dimensions, int) or isinstance(dimensions, bool) or dimensions < 1:
        raise ContractError("representation dimensions must be a positive integer")
    if record["metric"] not in PROCEDURE_METRICS:
        raise ContractError("representation metric is unsupported")
    vector = record.get("vector")
    if not isinstance(vector, list) or len(vector) != dimensions:
        raise ContractError("representation vector dimensions mismatch")
    normalized_vector: list[float] = []
    for item in vector:
        if not isinstance(item, (int, float)) or isinstance(item, bool) or not math.isfinite(item):
            raise ContractError("representation vector must contain finite numbers")
        normalized_vector.append(float(item))
    payload = {
        "model": record["model"],
        "dimensions": dimensions,
        "metric": record["metric"],
        "sanitizer_version": record["sanitizer_version"],
        "search_text": record["search_text"],
        "vector": normalized_vector,
    }
    if record["vector"] != normalized_vector:
        raise ContractError("representation vector must use canonical float values")
    if record["payload_digest"] != sha256_hex(payload):
        raise ContractError("representation payload digest mismatch")
    expected_representation_id = sha256_hex(
        {
            "domain": "trusted-procedure-representation/v1",
            "revision_id": record["revision_id"],
            "payload": payload,
        }
    )
    if record["representation_id"] != expected_representation_id:
        raise ContractError("representation identity does not match its payload")
    if procedure is not None:
        validate_procedure_revision(procedure)
        for field, expected_value in {
            "logical_id": procedure["logical_id"],
            "revision_id": procedure["revision_id"],
            "procedure_digest": procedure["content_hash"],
            "behavior_digest": procedure["behavior_digest"],
        }.items():
            if record.get(field) != expected_value:
                raise ContractError(f"representation does not bind exact procedure {field}")


def make_procedure_designation(
    *,
    procedure: Mapping[str, Any],
    approval: Mapping[str, Any],
    partition: Mapping[str, Any],
    generation: int,
    predecessor_generation: int | None,
    issuer: str,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Create one ordered explicit current-designation operation.

    This does not publish anything and does not claim that a remote current
    document has accepted the designation.  The generation is a causal fence,
    independent from the opaque content-derived revision identifier.
    """

    validate_procedure_revision(procedure)
    validate_procedure_approval(approval, procedure=procedure)
    normalized_partition = normalize_procedure_partition(partition)
    if not partition_is_authorized(approval, normalized_partition):
        raise ContractError("procedure designation partition broadens approval recipients")
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
        raise ContractError("procedure designation generation must be a positive integer")
    if generation == 1:
        if predecessor_generation is not None:
            raise ContractError("initial procedure designation must not name a predecessor")
    elif predecessor_generation != generation - 1:
        raise ContractError("procedure designation must name its immediate predecessor generation")
    selected_issuer = _require_nonempty_str(issuer, "designation issuer")
    partition_id = procedure_partition_id(normalized_partition)
    designation_id = sha256_hex(
        {
            "domain": "trusted-procedure-designation/v1",
            "logical_id": procedure["logical_id"],
            "revision_id": procedure["revision_id"],
            "approval_id": approval["approval_id"],
            "partition_id": partition_id,
            "generation": generation,
            "predecessor_generation": predecessor_generation,
            "issuer": selected_issuer,
        }
    )
    record: dict[str, Any] = {
        "schema": PROCEDURE_DESIGNATION_SCHEMA,
        "designation_id": designation_id,
        "logical_id": procedure["logical_id"],
        "revision_id": procedure["revision_id"],
        "procedure_digest": procedure["content_hash"],
        "approval_id": approval["approval_id"],
        "approval_digest": approval["content_hash"],
        "partition": normalized_partition,
        "partition_id": partition_id,
        "generation": generation,
        "predecessor_generation": predecessor_generation,
        "issuer": selected_issuer,
        "created_at": created_at or utc_now(),
    }
    record["content_hash"] = content_hash(record)
    validate_procedure_designation(record, procedure=procedure, approval=approval)
    return record


def validate_procedure_designation(
    record: Mapping[str, Any],
    *,
    procedure: Mapping[str, Any] | None = None,
    approval: Mapping[str, Any] | None = None,
) -> None:
    validate_record(record, PROCEDURE_DESIGNATION_SCHEMA)
    for field in (
        "designation_id",
        "logical_id",
        "revision_id",
        "procedure_digest",
        "approval_id",
        "approval_digest",
        "partition_id",
        "issuer",
        "created_at",
    ):
        _require_nonempty_str(record.get(field), field)
    partition = normalize_procedure_partition(record.get("partition"))
    if record["partition"] != partition:
        raise ContractError("procedure designation partition must use canonical values")
    if record["partition_id"] != procedure_partition_id(partition):
        raise ContractError("procedure designation partition identity mismatch")
    generation = record.get("generation")
    predecessor = record.get("predecessor_generation")
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
        raise ContractError("procedure designation generation must be a positive integer")
    if generation == 1:
        if predecessor is not None:
            raise ContractError("initial procedure designation must not name a predecessor")
    elif not isinstance(predecessor, int) or isinstance(predecessor, bool) or predecessor != generation - 1:
        raise ContractError("procedure designation must name its immediate predecessor generation")
    expected_designation_id = sha256_hex(
        {
            "domain": "trusted-procedure-designation/v1",
            "logical_id": record["logical_id"],
            "revision_id": record["revision_id"],
            "approval_id": record["approval_id"],
            "partition_id": record["partition_id"],
            "generation": generation,
            "predecessor_generation": predecessor,
            "issuer": record["issuer"],
        }
    )
    if record["designation_id"] != expected_designation_id:
        raise ContractError("procedure designation identity mismatch")
    if procedure is not None:
        validate_procedure_revision(procedure)
        for field, expected_value in {
            "logical_id": procedure["logical_id"],
            "revision_id": procedure["revision_id"],
            "procedure_digest": procedure["content_hash"],
        }.items():
            if record.get(field) != expected_value:
                raise ContractError(f"designation does not bind exact procedure {field}")
    if approval is not None:
        if procedure is not None:
            validate_procedure_approval(approval, procedure=procedure)
        else:
            validate_procedure_approval(approval)
        if record["approval_id"] != approval["approval_id"] or record["approval_digest"] != approval["content_hash"]:
            raise ContractError("designation does not bind exact approval")
        if not partition_is_authorized(approval, partition):
            raise ContractError("designation partition broadens approval recipients")


def make_procedure_withdrawal(
    *,
    predecessor_designation: Mapping[str, Any],
    issuer: str,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Create a narrow partition withdrawal with a newer lifecycle fence."""

    validate_procedure_designation(predecessor_designation)
    selected_issuer = _require_nonempty_str(issuer, "withdrawal issuer")
    generation = int(predecessor_designation["generation"]) + 1
    withdrawal_id = sha256_hex(
        {
            "domain": "trusted-procedure-withdrawal/v1",
            "logical_id": predecessor_designation["logical_id"],
            "partition_id": predecessor_designation["partition_id"],
            "predecessor_designation_id": predecessor_designation["designation_id"],
            "generation": generation,
            "issuer": selected_issuer,
        }
    )
    record: dict[str, Any] = {
        "schema": PROCEDURE_WITHDRAWAL_SCHEMA,
        "withdrawal_id": withdrawal_id,
        "logical_id": predecessor_designation["logical_id"],
        "revision_id": predecessor_designation["revision_id"],
        "partition": _normalize_json_value(
            predecessor_designation["partition"], "withdrawal partition"
        ),
        "partition_id": predecessor_designation["partition_id"],
        "predecessor_designation_id": predecessor_designation["designation_id"],
        "predecessor_generation": predecessor_designation["generation"],
        "generation": generation,
        "issuer": selected_issuer,
        "created_at": created_at or utc_now(),
    }
    record["content_hash"] = content_hash(record)
    validate_procedure_withdrawal(record)
    return record


def validate_procedure_withdrawal(record: Mapping[str, Any]) -> None:
    validate_record(record, PROCEDURE_WITHDRAWAL_SCHEMA)
    for field in (
        "withdrawal_id",
        "logical_id",
        "revision_id",
        "partition_id",
        "predecessor_designation_id",
        "issuer",
        "created_at",
    ):
        _require_nonempty_str(record.get(field), field)
    partition = normalize_procedure_partition(record.get("partition"))
    if record.get("partition") != partition or record["partition_id"] != procedure_partition_id(partition):
        raise ContractError("procedure withdrawal partition identity mismatch")
    predecessor = record.get("predecessor_generation")
    generation = record.get("generation")
    if not isinstance(predecessor, int) or isinstance(predecessor, bool) or predecessor < 1:
        raise ContractError("procedure withdrawal predecessor generation must be positive")
    if generation != predecessor + 1:
        raise ContractError("procedure withdrawal must use the next generation")
    expected_id = sha256_hex(
        {
            "domain": "trusted-procedure-withdrawal/v1",
            "logical_id": record["logical_id"],
            "partition_id": record["partition_id"],
            "predecessor_designation_id": record["predecessor_designation_id"],
            "generation": generation,
            "issuer": record["issuer"],
        }
    )
    if record["withdrawal_id"] != expected_id:
        raise ContractError("procedure withdrawal identity mismatch")


def _publication_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "logical_id": record["logical_id"],
        "revision_id": record["revision_id"],
        "partition": record["partition"],
        "partition_id": record["partition_id"],
        "procedure": record["procedure"],
        "approval": record["approval"],
        "representation": record["representation"],
        "designation": record["designation"],
    }


def make_procedure_publication(
    *,
    procedure: Mapping[str, Any],
    approval: Mapping[str, Any],
    representation: Mapping[str, Any],
    designation: Mapping[str, Any],
    created_at: str | None = None,
) -> dict[str, Any]:
    """Create one stable partition publication intent before a remote call."""

    validate_procedure_revision(procedure)
    validate_procedure_approval(approval, procedure=procedure)
    validate_procedure_representation(representation, procedure=procedure)
    validate_procedure_designation(
        designation, procedure=procedure, approval=approval
    )
    partition = normalize_procedure_partition(designation["partition"])
    publication_id = sha256_hex(
        {
            "domain": "trusted-procedure-publication/v1",
            "designation_id": designation["designation_id"],
            "representation_id": representation["representation_id"],
        }
    )
    record: dict[str, Any] = {
        "schema": PROCEDURE_PUBLICATION_SCHEMA,
        "publication_id": publication_id,
        "logical_id": procedure["logical_id"],
        "revision_id": procedure["revision_id"],
        "partition": partition,
        "partition_id": designation["partition_id"],
        "procedure": _normalize_json_value(procedure, "publication procedure"),
        "approval": _normalize_json_value(approval, "publication approval"),
        "representation": _normalize_json_value(
            representation, "publication representation"
        ),
        "designation": _normalize_json_value(designation, "publication designation"),
        "payload_digest": "",
        "status": "intent",
        "remote_receipt": None,
        "error": None,
        "version": 0,
        "created_at": created_at or utc_now(),
    }
    record["payload_digest"] = sha256_hex(_publication_payload(record))
    record["content_hash"] = content_hash(record)
    validate_procedure_publication(record)
    return record


def validate_procedure_publication(record: Mapping[str, Any]) -> None:
    validate_record(record, PROCEDURE_PUBLICATION_SCHEMA)
    for field in (
        "publication_id",
        "logical_id",
        "revision_id",
        "partition_id",
        "payload_digest",
        "status",
        "created_at",
    ):
        _require_nonempty_str(record.get(field), field)
    procedure = record.get("procedure")
    approval = record.get("approval")
    representation = record.get("representation")
    designation = record.get("designation")
    if not all(isinstance(value, Mapping) for value in (procedure, approval, representation, designation)):
        raise ContractError("procedure publication requires complete procedure evidence")
    validate_procedure_revision(procedure)
    validate_procedure_approval(approval, procedure=procedure)
    validate_procedure_representation(representation, procedure=procedure)
    validate_procedure_designation(designation, procedure=procedure, approval=approval)
    partition = normalize_procedure_partition(record.get("partition"))
    if record.get("partition") != partition or record["partition_id"] != procedure_partition_id(partition):
        raise ContractError("procedure publication partition identity mismatch")
    if partition != designation["partition"] or record["partition_id"] != designation["partition_id"]:
        raise ContractError("procedure publication partition does not match designation")
    expected_fields = {
        "logical_id": procedure["logical_id"],
        "revision_id": procedure["revision_id"],
    }
    for field, expected_value in expected_fields.items():
        if record.get(field) != expected_value:
            raise ContractError(f"procedure publication does not bind exact procedure {field}")
    expected_id = sha256_hex(
        {
            "domain": "trusted-procedure-publication/v1",
            "designation_id": designation["designation_id"],
            "representation_id": representation["representation_id"],
        }
    )
    if record["publication_id"] != expected_id:
        raise ContractError("procedure publication identity mismatch")
    if record["payload_digest"] != sha256_hex(_publication_payload(record)):
        raise ContractError("procedure publication payload digest mismatch")
    if record["status"] not in PROCEDURE_OPERATION_STATUSES:
        raise ContractError("unknown procedure publication status")
    if record.get("remote_receipt") is not None and not isinstance(record["remote_receipt"], Mapping):
        raise ContractError("procedure publication remote receipt must be an object or null")
    if record.get("error") is not None and not isinstance(record["error"], str):
        raise ContractError("procedure publication error must be a string or null")
    version = record.get("version")
    if not isinstance(version, int) or isinstance(version, bool) or version < 0:
        raise ContractError("procedure publication version must be nonnegative")


def revise_procedure_publication(
    record: Mapping[str, Any],
    *,
    status: str,
    remote_receipt: Mapping[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    validate_procedure_publication(record)
    if status not in PROCEDURE_OPERATION_STATUSES:
        raise ContractError("unknown procedure publication status")
    updated = _normalize_json_value(record, "procedure publication")
    assert isinstance(updated, dict)
    updated["status"] = status
    updated["remote_receipt"] = (
        _normalize_json_object(remote_receipt, "procedure remote receipt")
        if remote_receipt is not None
        else None
    )
    updated["error"] = error
    updated["version"] = int(record["version"]) + 1
    updated["content_hash"] = content_hash(updated)
    validate_procedure_publication(updated)
    return updated


def make_procedure_revocation(
    *,
    procedure: Mapping[str, Any],
    issuer: str,
    reason: str,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Create an immutable revision tombstone; it never chooses an older revision."""

    validate_procedure_revision(procedure)
    selected_issuer = _require_nonempty_str(issuer, "revocation issuer")
    selected_reason = _require_nonempty_str(reason, "revocation reason")
    revocation_id = sha256_hex(
        {
            "domain": "trusted-procedure-revocation/v1",
            "logical_id": procedure["logical_id"],
            "revision_id": procedure["revision_id"],
            "issuer": selected_issuer,
            "reason": selected_reason,
        }
    )
    record: dict[str, Any] = {
        "schema": PROCEDURE_REVOCATION_SCHEMA,
        "revocation_id": revocation_id,
        "logical_id": procedure["logical_id"],
        "revision_id": procedure["revision_id"],
        "procedure_digest": procedure["content_hash"],
        "issuer": selected_issuer,
        "reason": selected_reason,
        "created_at": created_at or utc_now(),
    }
    record["content_hash"] = content_hash(record)
    validate_procedure_revocation(record, procedure=procedure)
    return record


def validate_procedure_revocation(
    record: Mapping[str, Any],
    *,
    procedure: Mapping[str, Any] | None = None,
) -> None:
    validate_record(record, PROCEDURE_REVOCATION_SCHEMA)
    for field in (
        "revocation_id",
        "logical_id",
        "revision_id",
        "procedure_digest",
        "issuer",
        "reason",
        "created_at",
    ):
        _require_nonempty_str(record.get(field), field)
    expected_id = sha256_hex(
        {
            "domain": "trusted-procedure-revocation/v1",
            "logical_id": record["logical_id"],
            "revision_id": record["revision_id"],
            "issuer": record["issuer"],
            "reason": record["reason"],
        }
    )
    if record["revocation_id"] != expected_id:
        raise ContractError("procedure revocation identity mismatch")
    if procedure is not None:
        validate_procedure_revision(procedure)
        for field, expected_value in {
            "logical_id": procedure["logical_id"],
            "revision_id": procedure["revision_id"],
            "procedure_digest": procedure["content_hash"],
        }.items():
            if record.get(field) != expected_value:
                raise ContractError(f"procedure revocation does not bind exact procedure {field}")


def make_procedure_remote_operation(
    *,
    kind: str,
    logical_id: str,
    revision_id: str,
    payload_id: str,
    payload_digest: str,
    partition_id: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Create a stable outbox operation with intent/remote/ack phases."""

    selected_kind = _require_nonempty_str(kind, "procedure operation kind")
    selected_logical_id = _require_nonempty_str(logical_id, "logical_id")
    selected_revision_id = _require_nonempty_str(revision_id, "revision_id")
    selected_payload_id = _require_nonempty_str(payload_id, "payload_id")
    selected_payload_digest = _require_nonempty_str(payload_digest, "payload_digest")
    if partition_id is not None:
        partition_id = _require_nonempty_str(partition_id, "partition_id")
    operation_id = sha256_hex(
        {
            "domain": "trusted-procedure-remote-operation/v1",
            "kind": selected_kind,
            "logical_id": selected_logical_id,
            "revision_id": selected_revision_id,
            "payload_id": selected_payload_id,
            "payload_digest": selected_payload_digest,
            "partition_id": partition_id,
        }
    )
    record: dict[str, Any] = {
        "schema": PROCEDURE_REMOTE_OPERATION_SCHEMA,
        "operation_id": operation_id,
        "kind": selected_kind,
        "logical_id": selected_logical_id,
        "revision_id": selected_revision_id,
        "partition_id": partition_id,
        "payload_id": selected_payload_id,
        "payload_digest": selected_payload_digest,
        "status": "intent",
        "remote_receipt": None,
        "error": None,
        "version": 0,
        "created_at": created_at or utc_now(),
    }
    record["content_hash"] = content_hash(record)
    validate_procedure_remote_operation(record)
    return record


def validate_procedure_remote_operation(record: Mapping[str, Any]) -> None:
    validate_record(record, PROCEDURE_REMOTE_OPERATION_SCHEMA)
    for field in (
        "operation_id",
        "kind",
        "logical_id",
        "revision_id",
        "payload_id",
        "payload_digest",
        "status",
        "created_at",
    ):
        _require_nonempty_str(record.get(field), field)
    partition_id = record.get("partition_id")
    if partition_id is not None:
        _require_nonempty_str(partition_id, "partition_id")
    expected_id = sha256_hex(
        {
            "domain": "trusted-procedure-remote-operation/v1",
            "kind": record["kind"],
            "logical_id": record["logical_id"],
            "revision_id": record["revision_id"],
            "payload_id": record["payload_id"],
            "payload_digest": record["payload_digest"],
            "partition_id": partition_id,
        }
    )
    if record["operation_id"] != expected_id:
        raise ContractError("procedure remote operation identity mismatch")
    if record["status"] not in PROCEDURE_OPERATION_STATUSES:
        raise ContractError("unknown procedure remote operation status")
    receipt = record.get("remote_receipt")
    if receipt is not None and not isinstance(receipt, Mapping):
        raise ContractError("procedure remote operation receipt must be an object or null")
    if record.get("error") is not None and not isinstance(record["error"], str):
        raise ContractError("procedure remote operation error must be a string or null")
    version = record.get("version")
    if not isinstance(version, int) or isinstance(version, bool) or version < 0:
        raise ContractError("procedure remote operation version must be nonnegative")


def revise_procedure_remote_operation(
    record: Mapping[str, Any],
    *,
    status: str,
    remote_receipt: Mapping[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    validate_procedure_remote_operation(record)
    if status not in PROCEDURE_OPERATION_STATUSES:
        raise ContractError("unknown procedure remote operation status")
    updated = _normalize_json_value(record, "procedure remote operation")
    assert isinstance(updated, dict)
    updated["status"] = status
    updated["remote_receipt"] = (
        _normalize_json_object(remote_receipt, "procedure remote receipt")
        if remote_receipt is not None
        else None
    )
    updated["error"] = error
    updated["version"] = int(record["version"]) + 1
    updated["content_hash"] = content_hash(updated)
    validate_procedure_remote_operation(updated)
    return updated


def make_procedure_exposure(
    *,
    publication: Mapping[str, Any],
    recipient: Mapping[str, Any],
    delivery_id: str,
    delivered_at: str | None = None,
) -> dict[str, Any]:
    """Record known historical exposure; revocation cannot erase it."""

    validate_procedure_publication(publication)
    normalized_recipient = normalize_experience_scope(recipient)
    if canonical_json(normalized_recipient).decode("utf-8") not in {
        canonical_json(item).decode("utf-8") for item in publication["partition"]["recipients"]
    }:
        raise ContractError("procedure exposure recipient is outside publication partition")
    selected_delivery_id = _require_nonempty_str(delivery_id, "delivery_id")
    exposure_id = sha256_hex(
        {
            "domain": "trusted-procedure-exposure/v1",
            "publication_id": publication["publication_id"],
            "recipient": normalized_recipient,
            "delivery_id": selected_delivery_id,
        }
    )
    record: dict[str, Any] = {
        "schema": PROCEDURE_EXPOSURE_SCHEMA,
        "exposure_id": exposure_id,
        "publication_id": publication["publication_id"],
        "logical_id": publication["logical_id"],
        "revision_id": publication["revision_id"],
        "recipient": normalized_recipient,
        "delivery_id": selected_delivery_id,
        "delivered_at": delivered_at or utc_now(),
    }
    record["content_hash"] = content_hash(record)
    validate_procedure_exposure(record)
    return record


def validate_procedure_exposure(record: Mapping[str, Any]) -> None:
    validate_record(record, PROCEDURE_EXPOSURE_SCHEMA)
    for field in (
        "exposure_id",
        "publication_id",
        "logical_id",
        "revision_id",
        "delivery_id",
        "delivered_at",
    ):
        _require_nonempty_str(record.get(field), field)
    recipient = normalize_experience_scope(record.get("recipient"))
    if record["recipient"] != recipient:
        raise ContractError("procedure exposure recipient must use canonical scope")
    expected_id = sha256_hex(
        {
            "domain": "trusted-procedure-exposure/v1",
            "publication_id": record["publication_id"],
            "recipient": recipient,
            "delivery_id": record["delivery_id"],
        }
    )
    if record["exposure_id"] != expected_id:
        raise ContractError("procedure exposure identity mismatch")


# ---------------------------------------------------------------------------
# STEP-04 preparation, search, plan-disposition, child-operation, and
# finalized-context records.  These extend the accepted baseline compatibly.
# ---------------------------------------------------------------------------

PREPARATION_SCHEMA = "memory-preparation/v1"
CANDIDATE_SCHEMA = "memory-search-candidate/v1"
SEARCH_TRACE_SCHEMA = "memory-search-trace/v1"
PLAN_DISPOSITION_SCHEMA = "memory-plan-disposition/v1"
FINAL_CONTEXT_SCHEMA = "memory-final-context/v1"
APC_CHILD_OPERATION_SCHEMA = "memory-apc-child-operation/v1"

CURRENT_PLAN_STATES = frozenset(
    {"absent", "candidate_review", "execution_accepted", "inconsistent"}
)
# The enhanced handoff states only represent an exact current-plan fact; a
# reported inconsistency is a mandatory-state failure, not a handoff state.
HANDOFF_PLAN_STATES = frozenset(
    {"absent", "candidate_review", "execution_accepted"}
)
CANDIDATE_KINDS = frozenset({"historical_evidence", "procedure", "template"})
CANDIDATE_DISPOSITIONS = frozenset(
    {"eligible", "selected", "rejected", "invalid", "deduplicated", "unattempted"}
)
ATTEMPT_STATUSES = frozenset(
    {
        "disabled",
        "unavailable",
        "unattempted-by-budget",
        "attempted",
        "timed-out",
        "invalid",
        "completed",
    }
)
PLAN_BRANCHES = frozenset(
    {"preserved_accepted", "candidate_review", "direct_fill", "apc_proposal", "fresh"}
)
APC_CHILD_STATUSES = frozenset(
    {
        "intent_recorded",
        "launched",
        "ambiguous",
        # Ownership is still unresolved: the exact child may be live and its
        # cleanup has not been proven, so a retry must reconcile it first.
        "cleanup_pending",
        "reconciled",
        "failed",
        "cancelled",
        "refused",
    }
)
# Ownership is only unresolved while the exact child may still be live or its
# identity is unknown.  A terminal disposition is durable evidence, not an
# outstanding claim, so ordinary reuse work may continue from a fresh attempt.
APC_CHILD_UNRESOLVED_STATUSES = frozenset(
    {"intent_recorded", "launched", "ambiguous", "cleanup_pending"}
)
APC_CHILD_TERMINAL_STATUSES = frozenset({"reconciled", "failed", "cancelled", "refused"})
SEARCH_OUTCOMES = frozenset({"no_optional_memory", "optional_memory", "blocked"})


class PlanStateError(ContractError):
    """A nonempty current plan is inconsistent with the exact task state."""


def current_plan_state_for(record: Mapping[str, Any] | None) -> str:
    """Classify one exact plan reference into its explicit current-plan state.

    ``None`` is a truly absent plan.  Any nonempty reference must be an exact,
    readable plan record: a nonempty missing, unreadable, or stale reference is
    a mandatory-state inconsistency rather than an absent plan.
    """

    if record is None:
        return "absent"
    if not isinstance(record, Mapping):
        raise PlanStateError("nonempty current plan reference is not an object")
    try:
        validate_plan(record)
    except ContractError as exc:
        raise PlanStateError(f"nonempty current plan is unreadable: {exc}") from exc
    if record["state"] == "accepted":
        return "execution_accepted"
    return "candidate_review"


def classify_current_plan(
    record: Mapping[str, Any] | None,
    *,
    expected_objective_id: str,
    expected_route: str | None = None,
    expected_base_commit: str | None = None,
) -> str:
    """Return the explicit current-plan state for one exact objective."""

    state = current_plan_state_for(record)
    if state == "absent":
        return state
    if record["objective_id"] != expected_objective_id:
        raise PlanStateError(
            "current plan is bound to another objective: "
            f"expected {expected_objective_id!r}, got {record['objective_id']!r}"
        )
    if expected_route is not None and record["route"] != expected_route:
        raise PlanStateError(
            "current plan is bound to another route: "
            f"expected {expected_route!r}, got {record['route']!r}"
        )
    if expected_base_commit is not None:
        bound_base = record.get("base_commit")
        if bound_base is not None and bound_base != expected_base_commit:
            raise PlanStateError("current plan is bound to another repository baseline")
    return state


def network_resolution_record(resolution: NetworkResolution) -> dict[str, Any]:
    """JSON-safe public preparation form of a per-objective resolution."""
    return {
        "requested_mode": resolution.requested_mode,
        "effective_mode": resolution.effective_mode,
        "enforcement_sources": list(resolution.enforcement_sources),
        "disclosed_limits": list(resolution.disclosed_limits),
        "input_evidence": dict(resolution.input_evidence),
        "context": dict(resolution.context),
        "verification_result": resolution.verification_result,
    }


def preparation_network_resolution(preparation: Mapping[str, Any]) -> dict[str, Any]:
    """Read rich facts or project an old row without changing its stored bytes."""
    validate_preparation(preparation)
    if "network_resolution" in preparation:
        return dict(preparation["network_resolution"])
    mode = preparation["network_mode"]
    return {
        "requested_mode": mode,
        "effective_mode": mode,
        "enforcement_sources": ["legacy_captured_mode"],
        "disclosed_limits": ["legacy row has no captured network enforcement proof"],
        "input_evidence": {},
    }


def make_preparation(
    *,
    task_card: Mapping[str, Any],
    decision_id: str,
    objective_id: str,
    route: str,
    plan_id: str,
    plan_digest: str,
    plan_state: str,
    current_plan_state: str,
    strategy: str,
    requested_strategy: str,
    configuration: Mapping[str, Any],
    network_mode: str,
    network_resolution: Mapping[str, Any] | None = None,
    budget_source: str,
    remaining_seconds: float | None,
    execution_reserve_seconds: float,
    stage_allowance_seconds: float,
    deadline_monotonic: float | None = None,
    spent_seconds: float = 0.0,
    attempt: int = 1,
    preparation_id: str | None = None,
    supersedes: str | None = None,
    superseded_by: str | None = None,
    status: str = "prepared",
    route_correction: Mapping[str, Any] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    validate_task_card(task_card)
    if current_plan_state not in CURRENT_PLAN_STATES:
        raise ContractError(f"unknown current plan state: {current_plan_state!r}")
    if route not in ROUTES:
        raise ContractError(f"unknown route: {route!r}")
    for field, value in (
        ("decision_id", decision_id),
        ("objective_id", objective_id),
        ("plan_id", plan_id),
        ("plan_digest", plan_digest),
        ("plan_state", plan_state),
        ("strategy", strategy),
        ("network_mode", network_mode),
        ("budget_source", budget_source),
    ):
        _require_nonempty_str(value, field)
    resolved_configuration = _normalize_json_object(configuration, "configuration")
    if remaining_seconds is not None and not isinstance(remaining_seconds, (int, float)):
        raise ContractError("remaining_seconds must be a number or null")
    if execution_reserve_seconds < 0:
        raise ContractError("execution reserve must not be negative")
    if stage_allowance_seconds < 0:
        raise ContractError("stage allowance must not be negative")
    if isinstance(spent_seconds, bool) or not isinstance(spent_seconds, (int, float)):
        raise ContractError("spent_seconds must be a number")
    if spent_seconds < 0:
        raise ContractError("spent_seconds must not be negative")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise ContractError("attempt must be a positive integer")
    identity = preparation_id or sha256_hex(
        {
            "domain": "memory-preparation/v1",
            "task_card_digest": task_card["content_hash"],
            "decision_id": decision_id,
            "objective_id": objective_id,
            "route": route,
            "strategy": strategy,
            "configuration_digest": sha256_hex(resolved_configuration),
            "supersedes": supersedes,
            "attempt": attempt,
        }
    )
    record: dict[str, Any] = {
        "schema": PREPARATION_SCHEMA,
        "preparation_id": identity,
        "decision_id": decision_id,
        "task_card_digest": task_card["content_hash"],
        "objective_id": objective_id,
        "route": route,
        "plan_id": plan_id,
        "plan_digest": plan_digest,
        "plan_state": plan_state,
        "current_plan_state": current_plan_state,
        "requested_strategy": requested_strategy,
        "strategy": strategy,
        "configuration": resolved_configuration,
        "configuration_digest": sha256_hex(resolved_configuration),
        "network_mode": network_mode,
        "budget_source": budget_source,
        "remaining_seconds": remaining_seconds,
        "deadline_monotonic": deadline_monotonic,
        "execution_reserve_seconds": float(execution_reserve_seconds),
        "stage_allowance_seconds": float(stage_allowance_seconds),
        "spent_seconds": float(spent_seconds),
        "attempt": int(attempt),
        "supersedes": supersedes,
        "superseded_by": superseded_by,
        "route_correction": dict(route_correction) if route_correction else None,
        "status": status,
        "created_at": created_at or utc_now(),
    }
    if network_resolution is not None:
        record["network_resolution"] = _normalize_json_object(
            network_resolution, "network_resolution"
        )
    record["content_hash"] = content_hash(record)
    validate_preparation(record)
    return record


def validate_preparation(record: Mapping[str, Any]) -> None:
    validate_record(record, PREPARATION_SCHEMA)
    for field in (
        "preparation_id",
        "decision_id",
        "task_card_digest",
        "objective_id",
        "route",
        "plan_id",
        "plan_digest",
        "plan_state",
        "current_plan_state",
        "strategy",
        "configuration_digest",
        "network_mode",
        "budget_source",
        "status",
    ):
        _require_nonempty_str(record.get(field), field)
    if record["route"] not in ROUTES:
        raise ContractError(f"unknown route: {record['route']!r}")
    if record["current_plan_state"] not in CURRENT_PLAN_STATES:
        raise ContractError("unknown current plan state")
    configuration = record.get("configuration")
    if not isinstance(configuration, Mapping):
        raise ContractError("preparation configuration must be an object")
    if configuration.get("strategy") != record["strategy"]:
        raise ContractError("preparation strategy does not match its configuration")
    if record["configuration_digest"] != sha256_hex(configuration):
        raise ContractError("preparation configuration digest mismatch")
    if "network_resolution" in record:
        resolution = record["network_resolution"]
        if not isinstance(resolution, Mapping) or set(resolution) != {
            "requested_mode", "effective_mode", "enforcement_sources",
            "disclosed_limits", "input_evidence", "context", "verification_result",
        }:
            raise ContractError("network resolution fields are invalid")
        context = resolution["context"]
        expected_context = {key: record[key] for key in (
            "objective_id", "task_card_digest", "plan_id", "plan_digest",
            "decision_id", "route", "plan_state",
        )}
        if not isinstance(context, Mapping) or set(context) != NETWORK_CONTEXT_FIELDS or dict(context) != expected_context:
            raise ContractError("network resolution context conflicts with preparation")
        requested = resolution["requested_mode"]
        effective = resolution["effective_mode"]
        sources = resolution["enforcement_sources"]
        limits = resolution["disclosed_limits"]
        evidence = resolution["input_evidence"]
        verified = resolution["verification_result"]
        if (requested not in NETWORK_MODES or effective not in NETWORK_MODES
                or not isinstance(sources, list) or not all(isinstance(x, str) for x in sources)
                or not isinstance(limits, list) or not all(isinstance(x, str) for x in limits)
                or not isinstance(evidence, Mapping)):
            raise ContractError("network resolution is invalid")
        allowed_effective = {
            "normal": "normal", "soft_guardrail_network": "soft_guardrail_network",
            "restricted_local": "restricted_local", "atlas_memory_only": "soft_guardrail_network",
        }
        if effective != "atlas_memory_only" and effective != allowed_effective[requested]:
            raise ContractError("network resolution requested/effective modes conflict")
        if effective != "atlas_memory_only":
            expected_sources = (
                ["legacy_normal_compatibility"] if requested == "normal" else
                ["service_entry_policy_required"] if requested == "restricted_local" else
                ["requested_soft_guardrail_policy"]
            )
            if sources != expected_sources:
                raise ContractError("network resolution enforcement source is invalid")
            if effective == "soft_guardrail_network" and not any(
                "shell egress" in limit for limit in limits
            ):
                raise ContractError("soft network resolution omits shell-egress limit")
            if requested == "atlas_memory_only" and not any(
                "missing or unverified" in limit for limit in limits
            ):
                raise ContractError("downgraded network resolution omits proof limit")
        if effective == "atlas_memory_only":
            payload = evidence.get("launched_payload")
            block = evidence.get("unrelated_destination_block")
            if (requested != "atlas_memory_only" or not isinstance(payload, Mapping)
                    or not isinstance(block, Mapping) or payload.get("verified") is not True
                    or block.get("blocked") is not True or block.get("independent") is not True
                    or block.get("source_kind") != "independent_network_boundary"
                    or not isinstance(payload.get("evidence_id"), str)
                    or not isinstance(block.get("evidence_id"), str)
                    or not payload["evidence_id"] or not block["evidence_id"]
                    or verified is not True and not (
                        isinstance(verified, Mapping) and set(verified) == {
                            "context", "launched_payload", "unrelated_destination_block",
                        } and verified["context"] == context
                        and verified["launched_payload"] == payload
                        and verified["unrelated_destination_block"] == block
                    ) or sources != [
                        f"verified_launched_payload:{payload['evidence_id']}",
                        f"independent_egress_block:{block['evidence_id']}",
                    ]):
                raise ContractError("network resolution lacks captured verified proof")
        elif (verified is not None or any(source.startswith((
                "verified_launched_payload:", "independent_egress_block:"
        )) for source in sources)):
            raise ContractError("unverified network resolution claims verified enforcement")
        if record["network_mode"] != effective:
            raise ContractError("network resolution conflicts with captured effective mode")
    if record.get("supersedes") is not None:
        _require_nonempty_str(record["supersedes"], "supersedes")
        if record["supersedes"] == record["preparation_id"]:
            raise ContractError("a preparation cannot supersede itself")


def make_candidate(
    *,
    kind: str,
    logical_id: str,
    revision_id: str,
    origin: str,
    source_id: str,
    payload_digest: str,
    payload: Mapping[str, Any],
    scope: Mapping[str, Any] | None = None,
    provenance: Iterable[Mapping[str, Any]] = (),
    freshness: str = "live",
    representation: Mapping[str, Any] | None = None,
    score: float = 0.0,
    comparable: bool = True,
    disposition: str = "eligible",
    reasons: Iterable[str] = (),
    candidate_id: str | None = None,
) -> dict[str, Any]:
    selected_kind = _require_nonempty_str(kind, "kind")
    if selected_kind not in CANDIDATE_KINDS:
        raise ContractError(f"unknown candidate kind: {selected_kind!r}")
    if disposition not in CANDIDATE_DISPOSITIONS:
        raise ContractError(f"unknown candidate disposition: {disposition!r}")
    for field, value in (
        ("logical_id", logical_id),
        ("revision_id", revision_id),
        ("origin", origin),
        ("source_id", source_id),
        ("payload_digest", payload_digest),
    ):
        _require_nonempty_str(value, field)
    identity = candidate_id or sha256_hex(
        {
            "domain": "memory-search-candidate/v1",
            "kind": selected_kind,
            "logical_id": logical_id,
            "revision_id": revision_id,
        }
    )
    record: dict[str, Any] = {
        "schema": CANDIDATE_SCHEMA,
        "candidate_id": identity,
        "kind": selected_kind,
        "logical_id": logical_id,
        "revision_id": revision_id,
        "origin": origin,
        "source_id": source_id,
        "payload_digest": payload_digest,
        "payload": dict(payload),
        "scope": dict(scope) if scope else None,
        "provenance": [dict(item) for item in provenance],
        "freshness": freshness,
        "representation": dict(representation) if representation else None,
        "score": float(score),
        "comparable": bool(comparable),
        "disposition": disposition,
        "reasons": [str(item) for item in reasons],
    }
    record["content_hash"] = content_hash(record)
    validate_candidate(record)
    return record


def validate_candidate(record: Mapping[str, Any]) -> None:
    validate_record(record, CANDIDATE_SCHEMA)
    for field in (
        "candidate_id",
        "kind",
        "logical_id",
        "revision_id",
        "origin",
        "source_id",
        "payload_digest",
        "freshness",
    ):
        _require_nonempty_str(record.get(field), field)
    if record["kind"] not in CANDIDATE_KINDS:
        raise ContractError(f"unknown candidate kind: {record['kind']!r}")
    if record["disposition"] not in CANDIDATE_DISPOSITIONS:
        raise ContractError(f"unknown candidate disposition: {record['disposition']!r}")
    payload = record.get("payload")
    if not isinstance(payload, Mapping):
        raise ContractError("candidate payload must be an object")
    if record["payload_digest"] != sha256_hex(payload):
        raise ContractError("candidate payload digest mismatch")
    provenance = record.get("provenance")
    if not isinstance(provenance, list):
        raise ContractError("candidate provenance must be a list")


def make_search_trace(
    *,
    preparation_id: str,
    strategy: str,
    rounds: int,
    attempts: Iterable[Mapping[str, Any]],
    candidates: Iterable[Mapping[str, Any]],
    selected_ids: Iterable[str],
    delivered_ids: Iterable[str],
    outcome: str,
) -> dict[str, Any]:
    _require_nonempty_str(preparation_id, "preparation_id")
    _require_nonempty_str(strategy, "strategy")
    if outcome not in SEARCH_OUTCOMES:
        raise ContractError(f"unknown search outcome: {outcome!r}")
    normalized_attempts: list[dict[str, Any]] = []
    for attempt in attempts:
        if not isinstance(attempt, Mapping):
            raise ContractError("search attempts must be objects")
        entry = dict(attempt)
        for field in ("store_id", "kind", "status"):
            _require_nonempty_str(entry.get(field), field)
        if entry["status"] not in ATTEMPT_STATUSES:
            raise ContractError(f"unknown attempt status: {entry['status']!r}")
        if entry["kind"] not in CANDIDATE_KINDS:
            raise ContractError(f"unknown attempt kind: {entry['kind']!r}")
        normalized_attempts.append(entry)
    normalized_candidates = [dict(item) for item in candidates]
    for candidate in normalized_candidates:
        validate_candidate(candidate)
    record: dict[str, Any] = {
        "schema": SEARCH_TRACE_SCHEMA,
        "preparation_id": preparation_id,
        "strategy": strategy,
        "rounds": int(rounds),
        "attempts": normalized_attempts,
        "candidates": normalized_candidates,
        "selected": [str(item) for item in selected_ids],
        "delivered": [str(item) for item in delivered_ids],
        "outcome": outcome,
    }
    record["content_hash"] = content_hash(record)
    validate_search_trace(record)
    return record


def validate_search_trace(record: Mapping[str, Any]) -> None:
    validate_record(record, SEARCH_TRACE_SCHEMA)
    for field in ("preparation_id", "strategy", "outcome"):
        _require_nonempty_str(record.get(field), field)
    if record["outcome"] not in SEARCH_OUTCOMES:
        raise ContractError("unknown search outcome")
    if not isinstance(record.get("rounds"), int) or record["rounds"] < 0:
        raise ContractError("search rounds must be a non-negative integer")
    for field in ("attempts", "candidates", "selected", "delivered"):
        if not isinstance(record.get(field), list):
            raise ContractError(f"search trace {field} must be a list")


def make_plan_disposition(
    *,
    decision_id: str,
    objective_id: str,
    route: str,
    branch: str,
    reason: str,
    template: Mapping[str, Any] | None = None,
    proposal: Mapping[str, Any] | None = None,
    apc: Mapping[str, Any] | None = None,
    reuse_attempts: Iterable[Mapping[str, Any]] = (),
    fresh: Mapping[str, Any] | None = None,
    preserved_plan: Mapping[str, Any] | None = None,
    root_replan: Mapping[str, Any] | None = None,
    root_acceptance: Mapping[str, Any] | None = None,
    disposition_id: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    for field, value in (
        ("decision_id", decision_id),
        ("objective_id", objective_id),
        ("reason", reason),
    ):
        _require_nonempty_str(value, field)
    if route not in ROUTES:
        raise ContractError(f"unknown route: {route!r}")
    if branch not in PLAN_BRANCHES:
        raise ContractError(f"unknown plan branch: {branch!r}")
    if proposal is not None:
        validate_plan(proposal)
    if fresh is not None:
        validate_plan(fresh)
    if preserved_plan is not None:
        validate_plan(preserved_plan)
    if root_replan is not None and not isinstance(root_replan, Mapping):
        raise ContractError("root_replan must be an object")
    identity = disposition_id or sha256_hex(
        {"domain": "memory-plan-disposition/v1", "decision_id": decision_id, "branch": branch}
    )
    record: dict[str, Any] = {
        "schema": PLAN_DISPOSITION_SCHEMA,
        "disposition_id": identity,
        "decision_id": decision_id,
        "objective_id": objective_id,
        "route": route,
        "branch": branch,
        "reason": reason,
        "template": dict(template) if template else None,
        "proposal": dict(proposal) if proposal else None,
        "apc": dict(apc) if apc else None,
        "reuse_attempts": [dict(item) for item in reuse_attempts],
        "fresh": dict(fresh) if fresh else None,
        "preserved_plan": dict(preserved_plan) if preserved_plan else None,
        "root_replan": dict(root_replan) if root_replan else None,
        "root_acceptance": dict(root_acceptance) if root_acceptance else None,
        "created_at": created_at or utc_now(),
    }
    record["content_hash"] = content_hash(record)
    validate_plan_disposition(record)
    return record


def validate_plan_disposition(record: Mapping[str, Any]) -> None:
    validate_record(record, PLAN_DISPOSITION_SCHEMA)
    for field in ("disposition_id", "decision_id", "objective_id", "route", "branch", "reason"):
        _require_nonempty_str(record.get(field), field)
    if record["branch"] not in PLAN_BRANCHES:
        raise ContractError("unknown plan disposition branch")
    if record["route"] not in ROUTES:
        raise ContractError("unknown plan disposition route")
    if record["branch"] in {"direct_fill", "apc_proposal", "fresh"}:
        if not isinstance(record.get("proposal") if record["branch"] != "fresh" else record.get("fresh"), Mapping):
            raise ContractError("plan disposition branch requires an exact proposal")
    if record["branch"] == "direct_fill":
        template = record.get("template")
        if not isinstance(template, Mapping) or not template.get("template_id"):
            raise ContractError("direct fill disposition requires the selected template")
    if record["branch"] == "candidate_review":
        preserved = record.get("preserved_plan")
        if not isinstance(preserved, Mapping) or not preserved.get("plan_id"):
            raise ContractError(
                "a candidate review disposition requires the exact preserved plan"
            )
        if record.get("root_replan"):
            raise ContractError(
                "a candidate review disposition cannot claim a ROOT replan request"
            )
    if record.get("root_replan") is not None and not isinstance(
        record["root_replan"], Mapping
    ):
        raise ContractError("plan disposition root_replan must be an object")
    for field in ("reuse_attempts",):
        if not isinstance(record.get(field), list):
            raise ContractError(f"plan disposition {field} must be a list")


def make_finalized_context(
    *,
    lane_id: str,
    run_id: str,
    decision_id: str,
    task: str,
    task_card_digest: str,
    objective_id: str,
    route: str,
    plan_id: str,
    plan_revision: int,
    accepted_by: str,
    accepted_plan_content: Any,
    plan_digest: str,
    base_commit: str,
    worktree_path: str,
    checkpoint: str,
    strategy: str,
    configuration: Mapping[str, Any],
    execution_role: str,
    invocation_target: str,
    recipient: str,
    mandatory_content: list[Mapping[str, Any]],
    optional_content: list[Mapping[str, Any]],
    delivery_trace: Mapping[str, Any],
    role_separation: Mapping[str, Any],
    freshness: Mapping[str, Any],
    context_limit: int,
    context_id: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    mandatory = _normalize_content_list(mandatory_content, "mandatory_content")
    optional = _normalize_content_list(optional_content, "optional_content")
    for field, value in (
        ("lane_id", lane_id),
        ("run_id", run_id),
        ("decision_id", decision_id),
        ("task", task),
        ("task_card_digest", task_card_digest),
        ("objective_id", objective_id),
        ("plan_id", plan_id),
        ("plan_digest", plan_digest),
        ("base_commit", base_commit),
        ("worktree_path", worktree_path),
        ("strategy", strategy),
    ):
        _require_nonempty_str(value, field)
    for field, value in (
        ("checkpoint", checkpoint), ("execution_role", execution_role),
        ("invocation_target", invocation_target), ("recipient", recipient),
    ):
        _require_canonical_identity(value, field)
    if route not in ROUTES:
        raise ContractError(f"unknown route: {route!r}")
    if not isinstance(plan_revision, int) or isinstance(plan_revision, bool) or plan_revision < 1:
        raise ContractError("finalized plan revision must be a positive integer")
    if accepted_by != "ROOT":
        raise ContractError("finalized plan must be ROOT accepted")
    resolved_configuration = _normalize_json_object(configuration, "configuration")
    if not isinstance(context_limit, int) or isinstance(context_limit, bool) or context_limit < 1:
        raise ContractError("context_limit must be a positive integer")
    trace = dict(delivery_trace)
    record: dict[str, Any] = {
        "schema": FINAL_CONTEXT_SCHEMA,
        "lane_id": lane_id,
        "run_id": run_id,
        "decision_id": decision_id,
        "task": task,
        "task_card_digest": task_card_digest,
        "objective_id": objective_id,
        "route": route,
        "plan_id": plan_id,
        "plan_revision": plan_revision,
        "accepted_by": accepted_by,
        "accepted_plan_content": accepted_plan_content,
        "plan_digest": plan_digest,
        "base_commit": base_commit,
        "worktree_path": worktree_path,
        "checkpoint": checkpoint,
        "strategy": strategy,
        "configuration": resolved_configuration,
        "configuration_digest": sha256_hex(resolved_configuration),
        "execution_role": execution_role,
        "invocation_target": invocation_target,
        "recipient": recipient,
        "mandatory_content": mandatory,
        "mandatory_items": [item["id"] for item in mandatory],
        "mandatory_digest": sha256_hex(mandatory),
        "optional_content": optional,
        "optional_items": [item["id"] for item in optional],
        "optional_digest": sha256_hex(optional),
        "omitted": [item["id"] for item in trace["omitted"]],
        "delivery_trace": trace,
        "role_separation": dict(role_separation),
        "freshness": dict(freshness),
        "context_limit": context_limit,
        "state": "ready",
        "created_at": created_at or utc_now(),
    }
    record["integrity"] = _final_context_integrity(record)
    record["context_id"] = context_id or _final_context_id(record)
    record["content_hash"] = content_hash(record)
    validate_finalized_context(record)
    return record


def _final_context_integrity(record: Mapping[str, Any]) -> str:
    return sha256_hex({
        "domain": "memory-final-context-integrity/v2",
        "record": {key: value for key, value in record.items()
                   if key not in {"context_id", "integrity", "content_hash", "created_at"}},
    })


def _final_context_id(record: Mapping[str, Any]) -> str:
    return sha256_hex({
        "domain": "memory-final-context-identity/v2",
        "decision_id": record["decision_id"],
        "integrity": record["integrity"],
    })


def validate_finalized_context(record: Mapping[str, Any]) -> None:
    validate_record(record, FINAL_CONTEXT_SCHEMA)
    for field in (
        "context_id",
        "lane_id",
        "run_id",
        "decision_id",
        "task",
        "task_card_digest",
        "objective_id",
        "plan_id",
        "accepted_by",
        "plan_digest",
        "base_commit",
        "worktree_path",
        "checkpoint",
        "strategy",
        "execution_role",
        "invocation_target",
        "recipient",
        "configuration_digest",
        "mandatory_digest",
        "optional_digest",
        "integrity",
    ):
        _require_nonempty_str(record.get(field), field)
    for field in ("checkpoint", "execution_role", "invocation_target", "recipient"):
        _require_canonical_identity(record.get(field), field)
    if record.get("execution_role") != FINAL_CONTEXT_SECURITY["execution_role"]:
        raise ContractError("finalized context execution role mismatch")
    if record.get("accepted_by") != "ROOT" or not isinstance(record.get("plan_revision"), int) or isinstance(record["plan_revision"], bool) or record["plan_revision"] < 1:
        raise ContractError("finalized context accepted plan revision is invalid")
    if record["route"] not in ROUTES:
        raise ContractError("unknown finalized-context route")
    configuration = record.get("configuration")
    if not isinstance(configuration, Mapping):
        raise ContractError("finalized context configuration must be an object")
    if record["configuration_digest"] != sha256_hex(configuration):
        raise ContractError("finalized context configuration digest mismatch")
    if configuration.get("strategy") != record["strategy"]:
        raise ContractError("finalized context strategy mismatch")
    for field in ("mandatory_items", "optional_items", "omitted"):
        if not isinstance(record.get(field), list):
            raise ContractError(f"finalized context {field} must be a list")
    for field in ("role_separation", "freshness"):
        if not isinstance(record.get(field), Mapping):
            raise ContractError(f"finalized context {field} must be an object")
    if dict(record["role_separation"]) != FINAL_CONTEXT_SECURITY:
        raise ContractError("finalized context security boundary mismatch")
    if record.get("state") != "ready":
        raise ContractError("finalized context is not ready")
    if not isinstance(record.get("context_limit"), int) or isinstance(record["context_limit"], bool) or record["context_limit"] < 1:
        raise ContractError("finalized context limit must be a positive integer")
    _validate_content(record.get("accepted_plan_content"), "accepted_plan_content")
    mandatory = _normalize_content_list(record.get("mandatory_content"), "mandatory_content")
    optional = _normalize_content_list(record.get("optional_content"), "optional_content")
    _validate_final_mandatory(
        mandatory, task=record["task"], plan_content=record.get("accepted_plan_content"),
        base_commit=record["base_commit"], route=record["route"], checkpoint=record["checkpoint"],
    )
    if record["mandatory_items"] != [item["id"] for item in mandatory] or record["mandatory_digest"] != sha256_hex(mandatory):
        raise ContractError("finalized context mandatory content mismatch")
    if record["optional_items"] != [item["id"] for item in optional] or record["optional_digest"] != sha256_hex(optional):
        raise ContractError("finalized context optional content mismatch")
    _validate_delivery_trace(record.get("delivery_trace"), optional, record["omitted"])
    selected_by_id = {item["id"]: item for item in record["delivery_trace"]["selected"]}
    for item in optional:
        selected_provenance = selected_by_id[item["id"]]["provenance"]
        if item.get("kind") == "historical_evidence":
            if item.get("authority") != "historical_evidence_only" or not isinstance(item.get("content"), Mapping) or item["content"].get("procedural_authority") is not False or "evidence" not in item["content"]:
                raise ContractError("historical evidence must have no procedural authority")
            recheck = selected_provenance.get("final_recheck")
            if recheck is not None and recheck["content_digest"] != sha256_hex(item["content"]["evidence"]):
                raise ContractError("historical evidence differs from its rechecked source")
        if item.get("delivery_representation") == "compact":
            representation = item.get("compact_representation")
            approval = item.get("compact_approval")
            if not isinstance(representation, Mapping) or not isinstance(approval, Mapping):
                raise ContractError("compact procedure requires exact representation and approval")
            validate_procedure_compact_representation(representation)
            validate_procedure_compact_approval(approval, representation=representation)
            if item.get("kind") != "procedure" or item.get("content") != representation["content"] or item.get("revision_id") != representation["revision_id"]:
                raise ContractError("compact procedure delivery does not match approved representation")
            if item.get("full_content_digest") != selected_provenance.get("final_recheck", {}).get("content_digest"):
                raise ContractError("compact procedure lacks rechecked full content binding")
            if (item.get("full_procedure_digest") != representation["procedure_digest"]
                    or item.get("full_approval_digest") != approval["full_approval_digest"]):
                raise ContractError("compact procedure approval does not bind the full revision")
        if item.get("delivery_representation") == "guidance":
            content = item.get("content")
            recheck = selected_provenance.get("final_recheck")
            if (item.get("kind") != "procedure" or not isinstance(content, Mapping)
                    or set(content) != {"source_kind", "logical_id", "revision_id",
                                        "procedure_digest", "approval_digest", "recipient",
                                        "body", "references"}
                    or content.get("source_kind") not in {
                        "everos_generated_skill", "atlas_trusted_procedure"
                    }
                    or content.get("logical_id") != selected_provenance.get("logical_id")
                    or content.get("revision_id") != selected_provenance.get("revision_id")
                    or content.get("recipient") != selected_provenance.get("scope")
                    or not isinstance(content.get("body"), str) or not content["body"]
                    or not isinstance(content.get("references"), list)
                    or not isinstance(recheck, Mapping) or recheck.get("status") != "eligible"
                    or item.get("full_content_digest") != recheck.get("content_digest")):
                raise ContractError("procedure guidance does not bind its rechecked source")
            for field in ("procedure_digest", "approval_digest"):
                value = content.get(field)
                if (not isinstance(value, str) or len(value) != 64
                        or any(char not in "0123456789abcdef" for char in value)):
                    raise ContractError("procedure guidance digest is invalid")
    if record["integrity"] != _final_context_integrity(record):
        raise ContractError("finalized context integrity mismatch")
    if record["context_id"] != _final_context_id(record):
        raise ContractError("finalized context id mismatch")


def make_apc_child_operation(
    *,
    request: Mapping[str, Any],
    status: str,
    binding: Mapping[str, Any] | None = None,
    launch_intent: Mapping[str, Any] | None = None,
    observed_invocation: Mapping[str, Any] | None = None,
    result: Mapping[str, Any] | None = None,
    cleanup: Mapping[str, Any] | None = None,
    operation_id: str | None = None,
    attempt: int = 1,
    created_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(request, Mapping):
        raise ContractError("APC child operation requires its exact request")
    request_digest = _require_nonempty_str(request.get("content_hash"), "APC request digest")
    if status not in APC_CHILD_STATUSES:
        raise ContractError(f"unknown APC child status: {status!r}")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise ContractError("APC child attempt must be a positive integer")
    if status in {"launched", "reconciled"} and observed_invocation is None:
        raise ContractError("a launched APC child requires its observed invocation")
    identity = operation_id or sha256_hex(
        {
            "domain": "apc-child-operation/v1",
            "request_digest": request_digest,
            "attempt": attempt,
        }
    )
    record: dict[str, Any] = {
        "schema": APC_CHILD_OPERATION_SCHEMA,
        "child_operation_id": identity,
        "request_digest": request_digest,
        "parent_decision_id": request.get("parent_decision_id"),
        "parent_objective_id": request.get("parent_objective_id"),
        "template_id": request.get("template_id"),
        "template_version": request.get("template_version"),
        "binding": dict(binding) if binding else dict(request.get("binding") or {}),
        "status": status,
        "launch_intent": dict(launch_intent) if launch_intent else None,
        "observed_invocation": dict(observed_invocation) if observed_invocation else None,
        "result_digest": result.get("content_hash") if isinstance(result, Mapping) else None,
        "cleanup": dict(cleanup) if cleanup else None,
        "attempt": attempt,
        "created_at": created_at or utc_now(),
    }
    record["content_hash"] = content_hash(record)
    validate_apc_child_operation(record)
    return record


def validate_apc_child_operation(record: Mapping[str, Any]) -> None:
    validate_record(record, APC_CHILD_OPERATION_SCHEMA)
    for field in ("child_operation_id", "request_digest", "status"):
        _require_nonempty_str(record.get(field), field)
    if record["status"] not in APC_CHILD_STATUSES:
        raise ContractError("unknown APC child status")
    attempt = record.get("attempt", 1)
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise ContractError("APC child attempt must be a positive integer")
    if record["status"] in {"launched", "reconciled"} and record.get("observed_invocation") is None:
        raise ContractError("launched APC child operation requires an observed invocation")


def _native_usage_identity(
    *, source: str, invocation_id: str, objective_id: str | None,
    decision_id: str | None, maintenance_operation_id: str | None,
    stage: str, category: str, window_id: str, binding: Mapping[str, Any],
) -> dict[str, Any]:
    for name, value in (("source", source), ("invocation_id", invocation_id),
                        ("stage", stage), ("window_id", window_id)):
        _require_canonical_identity(value, name)
    if (objective_id is None) == (maintenance_operation_id is None):
        raise ContractError("usage requires exactly one objective or maintenance operation")
    for name, value in (("objective_id", objective_id), ("decision_id", decision_id),
                        ("maintenance_operation_id", maintenance_operation_id)):
        if value is not None:
            _require_canonical_identity(value, name)
    if decision_id is not None and objective_id is None:
        raise ContractError("usage decision requires an objective")
    if category not in NATIVE_USAGE_CATEGORIES:
        raise ContractError("unknown native usage category")
    if not isinstance(binding, Mapping) or set(binding) != {"requested", "resolved", "native"}:
        raise ContractError("usage binding needs requested, resolved, and native values")
    for name, value in binding.items():
        if value is not None:
            _require_nonempty_str(value, f"binding {name}")
    return dict(source=source, invocation_id=invocation_id, objective_id=objective_id,
                decision_id=decision_id, maintenance_operation_id=maintenance_operation_id,
                stage=stage, category=category, window_id=window_id, binding=dict(binding))


def make_native_usage_start(
    *, source: str, invocation_id: str, objective_id: str | None,
    decision_id: str | None, maintenance_operation_id: str | None,
    stage: str, category: str, window_id: str, binding: Mapping[str, Any],
    parent_invocation_id: str | None = None, parent_source: str | None = None,
    accepted_support: bool = False,
) -> dict[str, Any]:
    """Declare one actual started call. Binding values may be unknown (None)."""
    identity = _native_usage_identity(**dict(
        source=source, invocation_id=invocation_id, objective_id=objective_id,
        decision_id=decision_id, maintenance_operation_id=maintenance_operation_id,
        stage=stage, category=category, window_id=window_id, binding=binding))
    if parent_invocation_id is not None:
        _require_canonical_identity(parent_invocation_id, "parent_invocation_id")
        parent_source = parent_source or source
        _require_canonical_identity(parent_source, "parent_source")
        if (parent_source == source and parent_invocation_id == invocation_id) or category != "online_adaptation":
            raise ContractError("APC support needs a distinct online adaptation parent")
    elif parent_source is not None:
        raise ContractError("APC parent source requires a parent invocation")
    if not isinstance(accepted_support, bool) or (accepted_support and parent_invocation_id is None):
        raise ContractError("accepted support needs a parent invocation")
    record = {"schema": NATIVE_USAGE_START_SCHEMA, **identity,
              "parent_invocation_id": parent_invocation_id, "parent_source": parent_source,
              "accepted_support": accepted_support}
    record["content_hash"] = content_hash(record)
    return record


def validate_native_usage_start(record: Mapping[str, Any]) -> None:
    validate_record(record, NATIVE_USAGE_START_SCHEMA)
    expected = make_native_usage_start(**{key: record.get(key) for key in (
        "source", "invocation_id", "objective_id", "decision_id", "maintenance_operation_id",
        "stage", "category", "window_id", "binding", "parent_invocation_id",
        "parent_source", "accepted_support")})
    if dict(record) != expected:
        raise ContractError("native usage start has unexpected fields")


def make_native_usage_receipt(
    *, source: str, invocation_id: str, receipt_id: str,
    objective_id: str | None, decision_id: str | None,
    maintenance_operation_id: str | None, stage: str, category: str,
    window_id: str, binding: Mapping[str, Any], mode: str, complete: bool,
    measures: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Capture source-native measures; each value keeps its unit and total inclusion."""
    identity = _native_usage_identity(**dict(
        source=source, invocation_id=invocation_id, objective_id=objective_id,
        decision_id=decision_id, maintenance_operation_id=maintenance_operation_id,
        stage=stage, category=category, window_id=window_id, binding=binding))
    _require_canonical_identity(receipt_id, "receipt_id")
    if mode not in {"cumulative", "incremental"} or not isinstance(complete, bool):
        raise ContractError("usage receipt mode or completeness is invalid")
    if not isinstance(measures, Mapping):
        raise ContractError("usage measures must be a mapping")
    validated = {}
    for name, measure in measures.items():
        _require_canonical_identity(name, "measure name")
        if not isinstance(measure, Mapping) or set(measure) != {"value", "unit", "included_in_total"}:
            raise ContractError("usage measure needs value, unit, and included_in_total")
        value = measure["value"]
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value < 0):
            raise ContractError("usage measure value must be finite and nonnegative")
        _require_canonical_identity(measure["unit"], "measure unit")
        if measure["included_in_total"] is not None and not isinstance(measure["included_in_total"], bool):
            raise ContractError("usage total inclusion must be true, false, or unknown")
        validated[name] = dict(measure)
    record = {"schema": NATIVE_USAGE_RECEIPT_SCHEMA, **identity,
              "receipt_id": receipt_id, "mode": mode, "complete": complete,
              "measures": validated}
    record["content_hash"] = content_hash(record)
    return record


def validate_native_usage_receipt(record: Mapping[str, Any]) -> None:
    validate_record(record, NATIVE_USAGE_RECEIPT_SCHEMA)
    expected = make_native_usage_receipt(**{key: record.get(key) for key in (
        "source", "invocation_id", "receipt_id", "objective_id", "decision_id",
        "maintenance_operation_id", "stage", "category", "window_id", "binding",
        "mode", "complete", "measures")})
    if dict(record) != expected:
        raise ContractError("native usage receipt has unexpected fields")


def accept_plan(
    proposal: Mapping[str, Any],
    *,
    accepted_plan_id: str | None = None,
    accepted_content: Any | None = None,
    accepted_by: str = "ROOT",
) -> dict[str, Any]:
    """ROOT acceptance is a distinct, exact revision of a proposal."""

    validate_plan(proposal)
    if proposal["state"] == "accepted":
        if accepted_plan_id is not None and accepted_plan_id != proposal["plan_id"]:
            raise ContractError("an accepted plan cannot be re-accepted under a new identity")
        return dict(proposal)
    if accepted_by != "ROOT":
        raise ContractError("only ROOT may accept a plan")
    if accepted_content is None:
        content = proposal["content"]
        plan_id = proposal["plan_id"]
    else:
        content = accepted_content
        plan_id = accepted_plan_id
        if not plan_id:
            raise ContractError("a revised accepted plan requires its exact new identity")
        plan_id = _require_nonempty_str(plan_id, "accepted_plan_id")
        if plan_id == proposal["plan_id"]:
            raise ContractError("a revised plan must be a distinct exact revision")
    return make_plan(
        plan_id=plan_id,
        objective_id=proposal["objective_id"],
        route=proposal["route"],
        state="accepted",
        content=content,
        revision=int(proposal["revision"]) + (0 if accepted_content is None else 1),
        accepted_by="ROOT",
        supersedes=None if accepted_content is None else proposal["plan_id"],
        source=proposal.get("source"),
    )



__all__ = [
    "ContractError",
    "TASK_CARD_SCHEMA",
    "MEMORY_HANDOFF_SCHEMA",
    "PLAN_SCHEMA",
    "DECISION_SCHEMA",
    "ENVELOPE_SCHEMA",
    "FINAL_ENVELOPE_SCHEMA",
    "OPERATION_SCHEMA",
    "EFFECT_OPERATION_SCHEMA",
    "OUTCOME_SCHEMA",
    "NATIVE_TERMINAL_EVIDENCE_SCHEMA",
    "REJECTED_NATIVE_ATTEMPT_SCHEMA",
    "REVIEW_RECEIPT_SCHEMA",
    "REVIEWED_TRAJECTORY_SCHEMA",
    "EXPERIENCE_INGESTION_SCHEMA",
    "CASE_RECEIPT_SCHEMA",
    "GENERATED_SKILL_SCHEMA",
    "SKILL_APPROVAL_SCHEMA",
    "PROCEDURE_REVISION_SCHEMA",
    "PROCEDURE_APPROVAL_SCHEMA",
    "PROCEDURE_REPRESENTATION_SCHEMA",
    "PROCEDURE_DESIGNATION_SCHEMA",
    "PROCEDURE_WITHDRAWAL_SCHEMA",
    "PROCEDURE_PUBLICATION_SCHEMA",
    "PROCEDURE_REVOCATION_SCHEMA",
    "PROCEDURE_REMOTE_OPERATION_SCHEMA",
    "PROCEDURE_EXPOSURE_SCHEMA",
    "APC_REQUEST_SCHEMA",
    "APC_RESULT_SCHEMA",
    "PLAN_STATES",
    "ROUTES",
    "OUTCOME_STATUSES",
    "OPERATION_STATUSES",
    "LOCAL_EFFECT_KINDS",
    "EFFECT_OPERATION_STATUSES",
    "REVIEW_STATES",
    "REVIEWED_TRAJECTORY_STATUSES",
    "GENERATED_SKILL_STATES",
    "EXPERIENCE_INGESTION_STATUSES",
    "PROCEDURE_ORIGINS",
    "PROCEDURE_PARTITION_SCOPES",
    "PROCEDURE_METRICS",
    "PROCEDURE_OPERATION_STATUSES",
    "canonical_json",
    "sha256_hex",
    "content_hash",
    "effect_operation_id",
    "external_effect_operation_id",
    "validate_record",
    "make_task_card",
    "validate_task_card",
    "make_memory_handoff",
    "validate_memory_handoff",
    "make_plan",
    "validate_plan",
    "revise_plan",
    "validate_task_plan_binding",
    "make_decision",
    "validate_decision",
    "make_envelope",
    "validate_envelope",
    "make_operation",
    "validate_operation",
    "make_outcome",
    "validate_outcome",
    "validate_native_terminal_evidence",
    "make_rejected_native_attempt",
    "normalize_experience_scope",
    "make_review_receipt",
    "validate_review_receipt",
    "make_reviewed_trajectory",
    "validate_reviewed_trajectory",
    "make_experience_ingestion",
    "validate_experience_ingestion",
    "make_case_receipt",
    "validate_case_receipt",
    "make_generated_skill_candidate",
    "validate_generated_skill_candidate",
    "make_skill_approval",
    "validate_skill_approval",
    "normalize_procedure_partition",
    "procedure_partition_id",
    "normalize_procedure_predicates",
    "procedure_predicates_match",
    "make_procedure_revision",
    "validate_procedure_revision",
    "make_procedure_approval",
    "validate_procedure_approval",
    "make_procedure_compact_representation",
    "validate_procedure_compact_representation",
    "make_procedure_compact_approval",
    "validate_procedure_compact_approval",
    "partition_is_authorized",
    "make_procedure_representation",
    "validate_procedure_representation",
    "make_procedure_designation",
    "validate_procedure_designation",
    "make_procedure_withdrawal",
    "validate_procedure_withdrawal",
    "make_procedure_publication",
    "validate_procedure_publication",
    "revise_procedure_publication",
    "make_procedure_revocation",
    "validate_procedure_revocation",
    "make_procedure_remote_operation",
    "validate_procedure_remote_operation",
    "revise_procedure_remote_operation",
    "make_procedure_exposure",
    "validate_procedure_exposure",
    "PREPARATION_SCHEMA",
    "CANDIDATE_SCHEMA",
    "SEARCH_TRACE_SCHEMA",
    "PLAN_DISPOSITION_SCHEMA",
    "FINAL_CONTEXT_SCHEMA",
    "FINAL_CONTEXT_SECURITY",
    "APC_CHILD_OPERATION_SCHEMA",
    "WORKER_ENVIRONMENT_MODES",
    "CURRENT_PLAN_STATES",
    "HANDOFF_PLAN_STATES",
    "CANDIDATE_KINDS",
    "CANDIDATE_DISPOSITIONS",
    "ATTEMPT_STATUSES",
    "PLAN_BRANCHES",
    "APC_CHILD_STATUSES",
    "SEARCH_OUTCOMES",
    "PlanStateError",
    "classify_current_plan",
    "current_plan_state_for",
    "handoff_plan",
    "make_preparation",
    "validate_preparation",
    "make_candidate",
    "validate_candidate",
    "make_search_trace",
    "validate_search_trace",
    "make_plan_disposition",
    "validate_plan_disposition",
    "make_finalized_context",
    "validate_finalized_context",
    "make_final_source_recheck",
    "validate_final_source_recheck",
    "make_apc_child_operation",
    "validate_apc_child_operation",
    "NATIVE_USAGE_START_SCHEMA",
    "NATIVE_USAGE_RECEIPT_SCHEMA",
    "NATIVE_USAGE_CATEGORIES",
    "make_native_usage_start",
    "validate_native_usage_start",
    "make_native_usage_receipt",
    "validate_native_usage_receipt",
    "accept_plan",
]
