"""Fixed-strategy configuration and the deferred learned-selector boundary."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, Callable, Mapping

STANDARD = "standard"
PROBLEM_FOCUSED = "problem_focused"
DEEPER = "deeper"
FIXED_STRATEGIES = frozenset({STANDARD, PROBLEM_FOCUSED, DEEPER})
NETWORK_MODES = frozenset({
    "normal", "soft_guardrail_network", "atlas_memory_only", "restricted_local",
})


@dataclass(frozen=True)
class NetworkResolution:
    """Per-objective claim, attribution, and capture-time verification result.

    This is separate from MemoryConfig: network facts never enter the fixed
    strategy configuration or its decision digest. A proof reference identifies
    evidence held by the launch/egress owner. Its booleans are untrusted hints;
    only a constructor-bound verifier can attest that the actual launch and
    independent egress measurement match.
    """

    requested_mode: str
    effective_mode: str
    enforcement_sources: tuple[str, ...]
    disclosed_limits: tuple[str, ...]
    input_evidence: dict[str, Any]
    context: dict[str, str]
    verification_result: bool | dict[str, Any] | None


NETWORK_CONTEXT_FIELDS = frozenset({
    "objective_id", "task_card_digest", "plan_id", "plan_digest",
    "decision_id", "route", "plan_state",
})


class NetworkResolver:
    """Resolve network claims with a verifier fixed by the trusted composer.

    The verifier receives normalized nonsecret references and exact context.
    It returns literal True, or a record containing that context and both
    matching proof facts, only after checking the installed/launched payload
    and an independent unrelated-destination block measurement. Exceptions,
    absent proof, and mismatches fail closed. A same-user setting is never
    independent egress proof.
    """

    def __init__(self, verifier: Callable[[dict[str, Any], dict[str, str]], Any] | None = None):
        self._verifier = verifier

    def resolve(
        self, requested_mode: str = "normal", *, evidence: Mapping[str, Any] | None = None,
        context: Mapping[str, str] | None = None,
    ) -> NetworkResolution:
        """Resolve one requested profile from opaque proof references.

        ``evidence`` accepts ``launched_payload`` with ``verified`` and
        ``evidence_id``, and ``unrelated_destination_block`` with ``blocked``,
        ``independent``, ``source_kind``, and ``evidence_id``. Positive facts
        need a nonempty opaque ID. The caller supplies nonsecret IDs.
        """
        return _resolve_network_mode(requested_mode, evidence, context, self._verifier)


def resolve_network_mode(
    requested_mode: str = "normal", *, evidence: Mapping[str, Any] | None = None,
) -> NetworkResolution:
    """Legacy untrusted entry point; per-call evidence cannot authorize Atlas-only."""
    return NetworkResolver().resolve(requested_mode, evidence=evidence)


def _resolve_network_mode(
    requested_mode: str, evidence: Mapping[str, Any] | None,
    context: Mapping[str, str] | None,
    verifier: Callable[[dict[str, Any], dict[str, str]], Any] | None,
) -> NetworkResolution:
    if not isinstance(requested_mode, str) or requested_mode not in NETWORK_MODES:
        raise ValueError("unsupported network mode")
    if context is None:
        normalized_context: dict[str, str] = {}
    elif not isinstance(context, Mapping) or set(context) != NETWORK_CONTEXT_FIELDS or any(
        not isinstance(value, str) or not value for value in context.values()
    ):
        raise ValueError("network context is incomplete")
    else:
        normalized_context = dict(context)
    if evidence is None:
        evidence = {}
    if not isinstance(evidence, Mapping) or set(evidence) - {
        "launched_payload", "unrelated_destination_block",
    }:
        raise ValueError("network evidence has unsupported fields")
    normalized: dict[str, Any] = {}
    fields_by_kind = {
        "launched_payload": {"verified"},
        "unrelated_destination_block": {"blocked", "independent", "source_kind"},
    }
    for kind, facts in evidence.items():
        if not isinstance(facts, Mapping) or set(facts) != fields_by_kind[kind] | {"evidence_id"}:
            raise ValueError(f"{kind} evidence fields are incomplete")
        boolean_fields = fields_by_kind[kind] - {"source_kind"}
        if any(not isinstance(facts[name], bool) for name in boolean_fields):
            raise ValueError(f"{kind} evidence facts must be boolean")
        if kind == "unrelated_destination_block" and facts["source_kind"] not in (
            "independent_network_boundary", "same_user_process",
        ):
            raise ValueError("unrelated_destination_block source_kind is invalid")
        evidence_id = facts["evidence_id"]
        if not isinstance(evidence_id, str) or not evidence_id.strip():
            raise ValueError(f"{kind} evidence_id must be a nonempty string")
        normalized[kind] = dict(facts)

    if requested_mode == "restricted_local":
        return NetworkResolution(
            requested_mode, requested_mode, ("service_entry_policy_required",),
            ("optional Atlas task-path work requires service-entry blocking; "
             "authorized safety administration may remain pending",), normalized,
            normalized_context, None,
        )
    if requested_mode == "normal":
        return NetworkResolution(
            requested_mode, requested_mode, ("legacy_normal_compatibility",),
            ("no network restriction is claimed",), normalized, normalized_context, None,
        )
    sources = ["requested_soft_guardrail_policy"]
    limits = [
        "native general web/fetch/search suppression requires lane-2 launched-payload application",
        "shell egress remains possible; no hardened isolation is claimed",
    ]
    if requested_mode == "soft_guardrail_network":
        return NetworkResolution(requested_mode, requested_mode, tuple(sources),
                                 tuple(limits), normalized, normalized_context, None)
    payload = normalized.get("launched_payload")
    block = normalized.get("unrelated_destination_block")
    payload_ok = payload is not None and payload["verified"]
    block_ok = (block is not None and block["blocked"] and block["independent"]
                and block["source_kind"] == "independent_network_boundary")
    if not payload_ok:
        limits.append("launched_payload verification is missing")
    if not block_ok:
        limits.append("independent unrelated_destination_block proof is missing")
    verified: bool | dict[str, Any] | None = None
    if payload_ok and block_ok and normalized_context and verifier is not None:
        try:
            result = verifier(dict(normalized), dict(normalized_context))
            if result is True:
                verified = True
            elif isinstance(result, Mapping) and set(result) == {
                "context", "launched_payload", "unrelated_destination_block",
            } and result["context"] == normalized_context and (
                result["launched_payload"] == payload
                and result["unrelated_destination_block"] == block
            ):
                verified = {
                    "context": dict(normalized_context),
                    "launched_payload": dict(payload),
                    "unrelated_destination_block": dict(block),
                }
        except Exception:
            pass  # An unavailable verifier cannot grant a stronger claim.
    if verified is None:
        limits.append("trusted launched-payload and independent egress proof is missing or unverified")
        return NetworkResolution(requested_mode, "soft_guardrail_network",
                                 tuple(sources), tuple(limits), normalized,
                                 normalized_context, None)
    return NetworkResolution(
        requested_mode, requested_mode,
        (f"verified_launched_payload:{payload['evidence_id']}",
         f"independent_egress_block:{block['evidence_id']}"),
        ("capture-time verifier confirmed launched payload and independent measured egress; "
         "a new launch requires a new check",), normalized, normalized_context, verified,
    )

LEARNED_REQUEST_KEYS = (
    "learned_mode",
    "learned_selection",
    "policy_load",
    "policy_update",
    "training",
    "train",
    "learner",
)


class DeferredCapabilityError(RuntimeError):
    """A future capability was explicitly requested but is not implemented."""


@dataclass(frozen=True)
class MemoryConfig:
    strategy: str = STANDARD
    requested_strategy: str = STANDARD
    reason: str = "fixed strategy"
    experience_read: bool = True
    experience_write: bool = True
    generated_skill_creation: bool = True
    generated_skill_use: bool = True
    shared_publication: bool = True
    atlas_shared_retrieval: bool = True
    template_memory: bool = True
    apc: bool = True
    light_adaptation: bool = True
    deeper: bool = True

    @property
    def all_off(self) -> bool:
        return not any(
            (
                self.experience_read,
                self.experience_write,
                self.generated_skill_creation,
                self.generated_skill_use,
                self.shared_publication,
                self.atlas_shared_retrieval,
                self.template_memory,
                self.apc,
                self.light_adaptation,
                self.deeper,
            )
        )


def effect_submission_enabled(configuration: Mapping[str, Any] | MemoryConfig, kind: str) -> bool:
    """Apply the captured or current effective write gate to an effect kind."""
    resolved = configuration if isinstance(configuration, MemoryConfig) else resolve_config(configuration)
    if kind == "experience_ingestion":
        return resolved.experience_write
    if kind == "generated_skill_creation":
        return resolved.experience_write and resolved.generated_skill_creation
    if kind == "procedure_publication":
        return resolved.shared_publication
    return True


def _reject_learned_requests(raw: Mapping[str, Any]) -> None:
    requested_strategy = raw.get("strategy")
    if isinstance(requested_strategy, str) and requested_strategy.lower() in {
        "learned",
        "learned-selection",
        "learned_strategy",
        "learned_selector",
    }:
        raise DeferredCapabilityError(
            "deferred/not implemented: learned strategy selection is not implemented"
        )
    for key in LEARNED_REQUEST_KEYS:
        value = raw.get(key)
        if isinstance(value, Mapping):
            if any(bool(item) for item in value.values()):
                raise DeferredCapabilityError(
                    f"deferred/not implemented: learned request {key!r} is not implemented"
                )
        elif value:
            raise DeferredCapabilityError(
                f"deferred/not implemented: learned request {key!r} is not implemented"
            )
    if raw.get("policy") is not None and raw.get("strategy") is None:
        raise DeferredCapabilityError(
            "deferred/not implemented: learned policy loading is not implemented"
        )


def resolve_config(raw: Mapping[str, Any] | None = None) -> MemoryConfig:
    """Resolve one fixed recipe or the all-off baseline before preparation."""

    if raw is None:
        raw = {}
    if not isinstance(raw, Mapping):
        raise ValueError("configuration must be an object")
    _reject_learned_requests(raw)

    all_features = raw.get("all_features", True)
    if not isinstance(all_features, bool):
        raise ValueError("all_features must be boolean")
    if not all_features:
        return MemoryConfig(
            strategy=STANDARD,
            requested_strategy=str(raw.get("strategy", STANDARD)),
            reason="all enhancements off",
            experience_read=False,
            experience_write=False,
            generated_skill_creation=False,
            generated_skill_use=False,
            shared_publication=False,
            atlas_shared_retrieval=False,
            template_memory=False,
            apc=False,
            light_adaptation=False,
            deeper=False,
        )

    requested_strategy = raw.get("strategy", STANDARD)
    if not isinstance(requested_strategy, str):
        raise ValueError("strategy must be a string")
    normalized = requested_strategy.strip().lower().replace("-", "_")
    if normalized in FIXED_STRATEGIES:
        strategy = normalized
        reason = f"fixed strategy: {strategy}"
    else:
        strategy = STANDARD
        reason = f"fallback to standard: unsupported strategy {requested_strategy!r}"

    feature_values = {
        "experience_read": raw.get("experience_read", True),
        "experience_write": raw.get("experience_write", True),
        "generated_skill_creation": raw.get("generated_skill_creation", True),
        "generated_skill_use": raw.get("generated_skill_use", True),
        "shared_publication": raw.get("shared_publication", True),
        "atlas_shared_retrieval": raw.get("atlas_shared_retrieval", True),
        "template_memory": raw.get("template_memory", True),
        "apc": raw.get("apc", True),
        "light_adaptation": raw.get("light_adaptation", True),
        "deeper": raw.get("deeper", strategy == DEEPER),
    }
    for key, value in feature_values.items():
        if not isinstance(value, bool):
            raise ValueError(f"{key} must be boolean")

    feature_values["generated_skill_creation"] = bool(
        feature_values["generated_skill_creation"] and feature_values["experience_write"]
    )
    feature_values["apc"] = bool(feature_values["apc"] and feature_values["template_memory"])
    feature_values["light_adaptation"] = bool(
        feature_values["light_adaptation"] and feature_values["apc"]
    )
    if strategy == DEEPER and not feature_values["deeper"]:
        strategy = STANDARD
        reason = "fallback to standard: deeper strategy is disabled"

    return MemoryConfig(
        strategy=strategy,
        requested_strategy=requested_strategy,
        reason=reason,
        **feature_values,  # type: ignore[arg-type]
    )


def all_off() -> MemoryConfig:
    return resolve_config({"all_features": False})


def standard() -> MemoryConfig:
    return resolve_config({"strategy": STANDARD})


def problem_focused() -> MemoryConfig:
    return resolve_config({"strategy": PROBLEM_FOCUSED})


def deeper() -> MemoryConfig:
    return resolve_config({"strategy": DEEPER})


# ---------------------------------------------------------------------------
# STEP-04 central preparation limits and calibrated thresholds.
# Implementation Section 16 starting values live here and nowhere else.
# ---------------------------------------------------------------------------

REPRESENTATION_MODEL = "local-token-overlap/v1"
REPRESENTATION_DIMENSIONS = 512
REPRESENTATION_METRIC = "cosine"
REPRESENTATION_SANITIZER_VERSION = "v1"


class LimitsError(ValueError):
    """A resolved preparation limit is missing or invalid."""


@dataclass(frozen=True)
class PreparationLimits:
    """One resolved, centrally-owned set of preparation bounds."""

    default_deadline_seconds: float = 120.0
    execution_reserve_seconds: float = 30.0
    unknown_time_budget_seconds: float = 20.0
    minimum_optional_slice_seconds: float = 1.0
    standard_stage_seconds: float = 20.0
    problem_focused_stage_seconds: float = 40.0
    deeper_stage_seconds: float = 60.0
    deeper_rounds: int = 3
    store_seconds: float = 8.0
    apc_stage_seconds: float = 45.0
    candidate_capacity: Mapping[str, int] = field(
        default_factory=lambda: MappingProxyType(
            {"historical_evidence": 4, "procedure": 4, "template": 5}
        )
    )
    direct_fill_threshold: float = 0.6
    near_match_threshold: float = 0.25
    minimum_comparable_score: float = 0.2
    context_limit: int = 12000
    context_char_limit: int = 200000
    apc_output_char_limit: int = 40000
    representation_model: str = REPRESENTATION_MODEL
    representation_dimensions: int = REPRESENTATION_DIMENSIONS
    representation_metric: str = REPRESENTATION_METRIC
    representation_sanitizer_version: str = REPRESENTATION_SANITIZER_VERSION

    @property
    def representation_identity(self) -> dict[str, Any]:
        return {
            "model": self.representation_model,
            "dimensions": self.representation_dimensions,
            "metric": self.representation_metric,
            "sanitizer_version": self.representation_sanitizer_version,
        }

    def stage_seconds_for(self, strategy: str) -> float:
        if strategy == PROBLEM_FOCUSED:
            return self.problem_focused_stage_seconds
        if strategy == DEEPER:
            return self.deeper_stage_seconds
        return self.standard_stage_seconds

    def capacity_for(self, kind: str) -> int:
        capacity = self.candidate_capacity.get(kind)
        if not isinstance(capacity, int) or capacity < 0:
            raise LimitsError(f"no candidate capacity is configured for {kind!r}")
        return capacity

    def rounds_for(self, strategy: str) -> int:
        if strategy == DEEPER:
            return self.deeper_rounds
        return 1


_LIMIT_FIELDS = (
    "default_deadline_seconds",
    "execution_reserve_seconds",
    "unknown_time_budget_seconds",
    "minimum_optional_slice_seconds",
    "context_char_limit",
    "apc_output_char_limit",
    "standard_stage_seconds",
    "problem_focused_stage_seconds",
    "deeper_stage_seconds",
    "deeper_rounds",
    "store_seconds",
    "apc_stage_seconds",
    "direct_fill_threshold",
    "near_match_threshold",
    "minimum_comparable_score",
    "context_limit",
)


def resolve_limits(raw: Mapping[str, Any] | None = None) -> PreparationLimits:
    """Resolve one central limit set; malformed values fail closed."""

    if raw is None:
        return PreparationLimits()
    if not isinstance(raw, Mapping):
        raise LimitsError("preparation limits must be an object")
    values: dict[str, Any] = {}
    for name in _LIMIT_FIELDS:
        if name not in raw:
            continue
        candidate = raw[name]
        if isinstance(candidate, bool) or not isinstance(candidate, (int, float)):
            raise LimitsError(f"preparation limit {name} must be a number")
        if candidate < 0:
            raise LimitsError(f"preparation limit {name} must not be negative")
        values[name] = candidate
    if "stage_seconds" in raw:
        alias = raw["stage_seconds"]
        if isinstance(alias, bool) or not isinstance(alias, (int, float)) or alias < 0:
            raise LimitsError("preparation limit stage_seconds must be a non-negative number")
        values.setdefault("standard_stage_seconds", alias)
    if "deeper_rounds" in values and int(values["deeper_rounds"]) != values["deeper_rounds"]:
        raise LimitsError("deeper_rounds must be a whole number")
    capacity = raw.get("candidate_capacity")
    if capacity is not None:
        if not isinstance(capacity, Mapping):
            raise LimitsError("candidate_capacity must be an object")
        resolved: dict[str, int] = dict(PreparationLimits().candidate_capacity)
        for kind, value in capacity.items():
            if not isinstance(kind, str) or not kind:
                raise LimitsError("candidate capacity keys must be nonempty strings")
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise LimitsError(f"candidate capacity for {kind!r} must be a non-negative integer")
            resolved[kind] = value
        capacity_value: Mapping[str, int] = MappingProxyType(resolved)
    else:
        capacity_value = PreparationLimits().candidate_capacity
    if values.get("execution_reserve_seconds", 0.0) < 0:
        raise LimitsError("the execution reserve must not be negative")
    return PreparationLimits(
        candidate_capacity=capacity_value,
        **{name: (int(value) if name == "deeper_rounds" else value) for name, value in values.items()},
    )




__all__ = [
    "DeferredCapabilityError",
    "MemoryConfig",
    "FIXED_STRATEGIES",
    "STANDARD",
    "PROBLEM_FOCUSED",
    "DEEPER",
    "all_off",
    "standard",
    "problem_focused",
    "deeper",
    "resolve_config",
    "NETWORK_MODES",
    "NetworkResolution",
    "NetworkResolver",
    "resolve_network_mode",
    "LimitsError",
    "PreparationLimits",
    "REPRESENTATION_MODEL",
    "REPRESENTATION_DIMENSIONS",
    "REPRESENTATION_METRIC",
    "REPRESENTATION_SANITIZER_VERSION",
    "resolve_limits",]
