"""Shared binding, receipt, and provider-output support for ROOT hooks."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .core import iso_utc
from .config import load_config
from .manager_queue import append_delivery_history
from .root_hook_dispatch import dispatch, unresolved_event_ids

BINDING_SCHEMA = "harness-hook-binding/v1"
DECISIONS = frozenset({"ALLOW", "NOTICE", "REJECT"})


class RootHookBindingError(ValueError):
    """An installed ROOT hook is absent, unbound, or cross-bound."""


def _read_binding(path: Path, provider_id: str) -> dict[str, Any]:
    if not path.is_file():
        raise RootHookBindingError(
            f"installed {provider_id} hook has no harness binding"
        )
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RootHookBindingError(
            f"installed {provider_id} hook has invalid harness binding: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise RootHookBindingError(
            f"installed {provider_id} hook has invalid harness binding object"
        )
    expected = {
        "schema": BINDING_SCHEMA,
        "role": "root",
        "provider_id": provider_id,
    }
    for field, wanted in expected.items():
        if value.get(field) != wanted:
            raise RootHookBindingError(
                f"installed {provider_id} hook has invalid binding {field}"
            )
    for field in ("harness_root", "runtime_root"):
        raw = value.get(field)
        if not isinstance(raw, str) or not raw or not Path(raw).is_absolute():
            raise RootHookBindingError(
                f"installed {provider_id} hook has no bound {field}"
            )
    return value


def _workspace_path(binding_path: Path, relative: object, field: str) -> Path:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise RootHookBindingError(f"ROOT hook binding has invalid {field}")
    workspace = binding_path.resolve().parent.parent
    target = (workspace / relative).resolve(strict=False)
    try:
        target.relative_to(workspace)
    except ValueError as exc:
        raise RootHookBindingError(f"ROOT hook binding {field} escapes workspace") from exc
    return target


def _append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _notice_text(notice: object) -> str:
    if not isinstance(notice, dict):
        return "Harness ROOT hook reported an actionable notice."
    return "Harness ROOT notice: " + json.dumps(
        notice, sort_keys=True, separators=(",", ":")
    )


def translate(boundary: str, decision: dict[str, Any]) -> dict[str, Any]:
    kind = decision.get("decision")
    if kind == "ALLOW":
        return {}
    if boundary == "post-tool-use" and kind == "NOTICE":
        return {
            "hookSpecificOutput": {
                "hookEventName": "PostToolUse",
                "additionalContext": _notice_text(decision.get("notice")),
            }
        }
    reason = decision.get("reason") or "rejected by harness ROOT hook"
    if boundary == "stop":
        return {"decision": "block", "reason": str(reason)}
    return {"continue": False, "stopReason": str(reason)}


def run(provider_id: str, boundary: str, binding_path: Path) -> dict[str, Any]:
    """Run one ROOT hook and return its native JSON output object."""
    try:
        binding = _read_binding(binding_path, provider_id)
        liveness = _workspace_path(
            binding_path, binding.get("liveness_path"), "liveness_path"
        )
        _append_jsonl(
            liveness,
            {
                "schema": "hook-liveness/v1",
                "role": "root",
                "provider_id": provider_id,
                "boundary": boundary,
                "at": iso_utc(),
            },
        )
        decision = dispatch(Path(binding["harness_root"]), boundary, provider_id)
        if not isinstance(decision, dict) or decision.get("decision") not in DECISIONS:
            decision = {
                "decision": "REJECT",
                "reason": "ROOT hook dispatcher returned an invalid decision",
            }
        # Plain mode must remain queue-free, including when a provider leaves
        # an old event-id environment variable behind.
        try:
            managed = load_config(Path(binding["harness_root"])).profile == "managed"
        except Exception:
            managed = False
        event_id = os.environ.get("HARNESS_EVENT_ID") if managed else None
        event_ids = (
            [event_id]
            if event_id
            else (unresolved_event_ids(Path(binding["runtime_root"])) if managed else [])
        )
        if boundary == "post-tool-use" and event_ids:
            receipt_path = _workspace_path(
                binding_path,
                binding.get("delivery_receipt_path"),
                "delivery_receipt_path",
            )
            for selected_event_id in event_ids:
                receipt: dict[str, Any] = {
                    "outcome": "DELIVERED",
                    "source": "hook",
                    "event_id": selected_event_id,
                    "provider_id": provider_id,
                    "at": iso_utc(),
                }
                try:
                    # Keep the two-argument call compatible with the shipped
                    # queue API; its default source is the ROOT hook.
                    append_delivery_history(
                        Path(binding["runtime_root"]), selected_event_id
                    )
                except Exception as exc:
                    receipt.update({"outcome": "DELIVERY_FAILED", "error": str(exc)})
                    if decision.get("decision") == "ALLOW":
                        decision = {
                            "decision": "NOTICE",
                            "notice": {
                                "provider_id": provider_id,
                                "binding_id": provider_id,
                                "unresolved_count": len(event_ids),
                                "event_classes": [],
                                "highest_class": None,
                                "highest_severity": "error",
                                "at": iso_utc(),
                                "message": f"manager delivery receipt failed: {exc}",
                            },
                        }
                _append_jsonl(receipt_path, receipt)
    except Exception as exc:
        decision = {"decision": "REJECT", "reason": str(exc)}
    return translate(boundary, decision)
