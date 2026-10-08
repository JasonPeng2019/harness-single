"""Thin APC request/result contracts with an explicit, non-inherited binding."""

from __future__ import annotations

from typing import Any, Mapping

from . import contracts

APC_REQUEST_SCHEMA = contracts.APC_REQUEST_SCHEMA
APC_RESULT_SCHEMA = contracts.APC_RESULT_SCHEMA
_FORBIDDEN_BINDING_KEYS = {"api_key", "token", "credential", "password", "secret"}


class APCError(ValueError):
    """An APC request or result violates its contract."""


class APCBindingError(APCError):
    """The required explicit adaptation binding is missing or inherited."""


def _validate_binding(binding: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(binding, Mapping):
        raise APCBindingError("apc_adaptation_binding must be an object")
    required = ("provider", "model", "cli", "effort", "source")
    missing = [key for key in required if key not in binding]
    if missing:
        raise APCBindingError(f"apc_adaptation_binding is missing: {', '.join(missing)}")
    invalid = [
        key for key in required
        if not isinstance(binding[key], str) or not binding[key].strip()
    ]
    if invalid:
        raise APCBindingError(
            f"apc_adaptation_binding fields must be nonempty strings: {', '.join(invalid)}"
        )
    if binding["source"] != "explicit":
        raise APCBindingError("apc_adaptation_binding must be explicit and must not be inherited")
    for key in binding:
        if str(key).lower() in _FORBIDDEN_BINDING_KEYS:
            raise APCBindingError(f"apc_adaptation_binding must not contain credentials: {key}")
    return {key: value for key, value in binding.items() if key not in _FORBIDDEN_BINDING_KEYS}


def make_apc_request(
    *,
    template: Mapping[str, Any],
    parent_decision_id: str,
    parent_objective_id: str,
    permitted_edits: list[str] | tuple[str, ...],
    binding: Mapping[str, Any],
    credentials: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a bounded drafting request; credentials are never serialized."""

    if not isinstance(template, Mapping):
        if hasattr(template, "to_record"):
            template = template.to_record()
        else:
            raise APCError("template must be an object")
    template = dict(template)
    template_id = template.get("template_id")
    if not isinstance(template_id, str) or not template_id:
        raise APCError("template_id must be a nonempty string")
    if not isinstance(parent_decision_id, str) or not parent_decision_id:
        raise APCError("parent_decision_id must be a nonempty string")
    if not isinstance(parent_objective_id, str) or not parent_objective_id:
        raise APCError("parent_objective_id must be a nonempty string")
    template_version = template.get("version")
    if not isinstance(template_version, int) or isinstance(template_version, bool) or template_version < 1:
        raise APCError("template version must be a positive integer")
    allowed_edits = template.get("allowed_edits")
    if not isinstance(allowed_edits, list) or any(
        not isinstance(item, str) or not item for item in allowed_edits
    ):
        raise APCError("template allowed_edits must be a list of nonempty strings")
    if not isinstance(permitted_edits, (list, tuple)) or not permitted_edits:
        raise APCError("permitted_edits must be a nonempty list")
    if any(not isinstance(item, str) or not item for item in permitted_edits):
        raise APCError("permitted_edits must contain nonempty strings")
    normalized_edits = tuple(permitted_edits)
    if any(item not in allowed_edits for item in normalized_edits):
        raise APCError("permitted_edits exceed the selected template's allowed edits")
    safe_binding = _validate_binding(binding)
    record: dict[str, Any] = {
        "schema": APC_REQUEST_SCHEMA,
        "parent_decision_id": parent_decision_id,
        "parent_objective_id": parent_objective_id,
        "template_id": template_id,
        "template_version": template_version,
        "template": template,
        "template_digest": contracts.sha256_hex(template),
        "permitted_edits": list(normalized_edits),
        "binding": safe_binding,
        "result_contract": {
            "state": "proposed",
            "authority": "none",
            "may_approve": False,
            "may_publish": False,
            "may_execute_parent": False,
        },
    }
    record["content_hash"] = contracts.content_hash(record)
    validate_apc_request(record)
    return record


def validate_apc_request(record: Mapping[str, Any]) -> None:
    if not isinstance(record, Mapping):
        raise APCError("APC request must be an object")
    if record.get("schema") != APC_REQUEST_SCHEMA:
        raise APCError("APC request schema mismatch")
    for field in ("parent_decision_id", "parent_objective_id", "template_id"):
        if not isinstance(record.get(field), str) or not record[field]:
            raise APCError(f"APC request {field} must be a nonempty string")
    if not isinstance(record.get("permitted_edits"), list) or not record["permitted_edits"]:
        raise APCError("APC request permitted_edits must be a nonempty list")
    template = record.get("template")
    if not isinstance(template, Mapping):
        raise APCError("APC request template must be an object")
    if record.get("template_id") != template.get("template_id"):
        raise APCError("APC request template identity mismatch")
    if record.get("template_version") != template.get("version"):
        raise APCError("APC request template version mismatch")
    if record.get("template_digest") != contracts.sha256_hex(template):
        raise APCError("APC request template digest mismatch")
    allowed_edits = template.get("allowed_edits")
    if not isinstance(allowed_edits, list) or any(
        item not in allowed_edits for item in record["permitted_edits"]
    ):
        raise APCError("APC request permitted edits exceed the template")
    _validate_binding(record["binding"])
    if record.get("content_hash") != contracts.content_hash(record):
        raise APCError("APC request content hash mismatch")
    result_contract = record.get("result_contract")
    if not isinstance(result_contract, Mapping):
        raise APCError("APC request result_contract must be an object")
    if result_contract.get("state") != "proposed" or result_contract.get("authority") != "none":
        raise APCError("APC request must request a proposed draft with no authority")


def make_apc_result(
    request: Mapping[str, Any], proposed_plan: Mapping[str, Any]
) -> dict[str, Any]:
    validate_apc_request(request)
    contracts.validate_plan(proposed_plan, expected_state="proposed")
    record: dict[str, Any] = {
        "schema": APC_RESULT_SCHEMA,
        "parent_decision_id": request["parent_decision_id"],
        "parent_objective_id": request["parent_objective_id"],
        "template_id": request["template_id"],
        "template_version": request["template_version"],
        "template_digest": request["template_digest"],
        "permitted_edits": list(request["permitted_edits"]),
        "binding": dict(request["binding"]),
        "proposed_plan": dict(proposed_plan),
        "status": "proposed",
    }
    record["content_hash"] = contracts.content_hash(record)
    validate_apc_result(record, request)
    return record


def validate_apc_result(record: Mapping[str, Any], request: Mapping[str, Any]) -> None:
    if not isinstance(record, Mapping):
        raise APCError("APC result must be an object")
    if record.get("schema") != APC_RESULT_SCHEMA:
        raise APCError("APC result schema mismatch")
    if record.get("content_hash") != contracts.content_hash(record):
        raise APCError("APC result content hash mismatch")
    for field in (
        "parent_decision_id", "parent_objective_id", "template_id",
        "template_version", "template_digest",
    ):
        if record.get(field) != request.get(field):
            raise APCError(f"APC result {field} does not match its request")
    if tuple(record.get("permitted_edits", ())) != tuple(request.get("permitted_edits", ())):
        raise APCError("APC result permitted_edits do not match its request")
    if record.get("binding") != request.get("binding"):
        raise APCError("APC result binding does not match its request")
    proposed_plan = record.get("proposed_plan")
    if not isinstance(proposed_plan, Mapping):
        raise APCError("APC result proposed_plan must be an object")
    contracts.validate_plan(proposed_plan, expected_state="proposed")
    if proposed_plan["objective_id"] != request["parent_objective_id"]:
        raise APCError("APC result objective does not match its request")
    source = proposed_plan.get("source")
    if not isinstance(source, Mapping) or source.get("template_id") != request["template_id"]:
        raise APCError("APC result is not bound to the selected template")
    if source.get("apc_request") != request["content_hash"]:
        raise APCError("APC result is not bound to the exact request")
    if record.get("status") != "proposed":
        raise APCError("APC result must remain a proposed draft")


__all__ = [
    "APCError",
    "APCBindingError",
    "APC_REQUEST_SCHEMA",
    "APC_RESULT_SCHEMA",
    "make_apc_request",
    "validate_apc_request",
    "make_apc_result",
    "validate_apc_result",
]
