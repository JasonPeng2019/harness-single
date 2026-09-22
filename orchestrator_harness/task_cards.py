"""Shared validation for the public project task-card contract."""
from __future__ import annotations

from typing import Any

from .core import require_schema

TASK_CARD_SCHEMA = "project-task-card/v1"
TASK_CARD_LIST_FIELDS = ("acceptance_criteria", "deliverables")
TASK_CARD_REASON_FIELD = "reason_for_acceptance_and_deliverables"


def validate_task_card(record: dict[str, Any], path: Any) -> None:
    """Reject a task card that does not satisfy the complete v1 contract."""
    require_schema(record, TASK_CARD_SCHEMA, path)

    task = record.get("task")
    if not isinstance(task, str) or not task.strip():
        raise ValueError("task card has no task text")

    for field in TASK_CARD_LIST_FIELDS:
        value = record.get(field)
        if (
            not isinstance(value, list)
            or not value
            or any(not isinstance(item, str) or not item.strip() for item in value)
        ):
            raise ValueError(
                f"task card {field} must be a non-empty list of non-empty strings"
            )

    reason = record.get(TASK_CARD_REASON_FIELD)
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError(
            f"task card {TASK_CARD_REASON_FIELD} must be a non-empty string"
        )
