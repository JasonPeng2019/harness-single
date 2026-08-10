"""One immutable, fail-closed continuation admission decision."""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping


RESUME_ADMISSION_SCHEMA = "orchestrator-resume-admission/v1"
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
    "session_id",
    "repository",
    "prompt_bundle_sha256",
    "prompt_content_sha256",
)
_TERMINAL_STATES = {"ACCEPTED", "ACCEPTED_TERMINAL", "TERMINAL_ACCEPTED"}


class ResumeAdmissionError(ValueError):
    """Raised when continuation identity cannot be admitted."""

    def __init__(self, message: str, admission: "ResumeAdmission | None" = None) -> None:
        super().__init__(message)
        self.admission = admission


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _normalized_path(value: object) -> object:
    if not isinstance(value, str) or not value.strip():
        return value
    return os.path.normcase(os.path.abspath(value))


def _normalize(value: Any, *, key: str = "") -> Any:
    if key in {"common_dir", "worktree_root", "starting_commit"} and isinstance(value, str):
        return _normalized_path(value) if key != "starting_commit" else value.lower()
    if isinstance(value, Mapping):
        return {str(k): _normalize(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    return value


def _mapping(value: Mapping[str, Any] | None) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
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

    def __post_init__(self) -> None:
        if self.schema != RESUME_ADMISSION_SCHEMA:
            raise ResumeAdmissionError("unsupported resume admission schema")
        object.__setattr__(self, "reasons", tuple(self.reasons))
        for field_name in ("requested", "persisted", "live"):
            value = getattr(self, field_name)
            object.__setattr__(self, field_name, _freeze(_thaw(value)))
        if self.decision_sha256 != _digest(self._decision_payload()):
            raise ResumeAdmissionError("resume admission decision hash is not content-bound")

    def _decision_payload(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "admitted": self.admitted,
            "reasons": list(self.reasons),
            "requested": _thaw(self.requested),
            "persisted": _thaw(self.persisted),
            "live": _thaw(self.live),
        }

    def to_record(self) -> dict[str, Any]:
        return {**self._decision_payload(), "decision_sha256": self.decision_sha256}


def _compare_identity(
    requested: Mapping[str, Any], persisted: Mapping[str, Any], reasons: list[str]
) -> None:
    for field in _IDENTITY_FIELDS:
        if field not in requested:
            reasons.append(f"requested identity missing {field}")
            continue
        if field not in persisted:
            reasons.append(f"persisted identity missing {field}")
            continue
        if _normalize(requested[field], key=field) != _normalize(persisted[field], key=field):
            reasons.append(f"resume identity mismatch: {field}")


def make_resume_admission(
    requested: Mapping[str, Any],
    persisted: Mapping[str, Any],
    live: Mapping[str, Any],
) -> ResumeAdmission:
    """Compare requested, persisted, and live identities without mutation."""

    requested_value = _mapping(requested)
    persisted_value = _mapping(persisted)
    live_value = _mapping(live)
    reasons: list[str] = []
    if requested_value.get("schema") != persisted_value.get("schema"):
        reasons.append("resume invocation schema mismatch")
    _compare_identity(requested_value, persisted_value, reasons)
    terminal_state = persisted_value.get("terminal_acceptance_state")
    if isinstance(terminal_state, str) and terminal_state.upper() in _TERMINAL_STATES:
        reasons.append("accepted terminal task cannot be resumed")
    if "live_identity" not in requested_value:
        reasons.append("requested identity missing live_identity")
    elif live_value.get("live_identity") != requested_value.get("live_identity"):
        reasons.append("live process/session identity mismatch")
    requested_repository = requested_value.get("repository")
    live_repository = live_value.get("repository")
    if requested_repository is not None and _normalize(requested_repository) != _normalize(live_repository):
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
    return ResumeAdmission(
        admitted=admitted,
        reasons=tuple(reasons),
        requested=_freeze(requested_value),
        persisted=_freeze(persisted_value),
        live=_freeze(live_value),
        decision_sha256=_digest(payload),
    )


def require_resume_admission(
    requested: Mapping[str, Any],
    persisted: Mapping[str, Any],
    live: Mapping[str, Any],
) -> ResumeAdmission:
    admission = make_resume_admission(requested, persisted, live)
    if not admission.admitted:
        raise ResumeAdmissionError("resume admission rejected: " + "; ".join(admission.reasons), admission)
    return admission


ResumeAdmissionDecision = ResumeAdmission
admit_resume = make_resume_admission


__all__ = [
    "RESUME_ADMISSION_SCHEMA",
    "ResumeAdmission",
    "ResumeAdmissionDecision",
    "ResumeAdmissionError",
    "make_resume_admission",
    "admit_resume",
    "require_resume_admission",
]
