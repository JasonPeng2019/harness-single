"""A small versioned local plan-template registry for the Stage-A slice."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from . import config, contracts


class TemplateError(ValueError):
    """A template or its bindings are invalid."""


class MissingBindingsError(TemplateError):
    """A required task-specific binding is missing."""


@dataclass(frozen=True)
class Template:
    template_id: str
    version: int
    family: str
    fixed_steps: tuple[str, ...]
    required_fields: tuple[str, ...]
    allowed_edits: tuple[str, ...]
    verification_intent: str
    routes: tuple[str, ...] = ("ordinary",)
    keywords: tuple[str, ...] = ()
    representation: tuple[tuple[str, Any], ...] = ()

    @property
    def representation_declared(self) -> bool:
        """Only a template with a declared representation can be compared."""

        return bool(self.representation)

    def to_record(self) -> dict[str, Any]:
        return {
            "template_id": self.template_id,
            "version": self.version,
            "family": self.family,
            "fixed_steps": list(self.fixed_steps),
            "required_fields": list(self.required_fields),
            "allowed_edits": list(self.allowed_edits),
            "verification_intent": self.verification_intent,
            "routes": list(self.routes),
            "representation": [list(item) for item in self.representation],
        }


# The five seeded families pin the one canonical representation identity from
# central configuration; a template without a declared representation can never
# be compared with an objective representation.
_CANONICAL_REPRESENTATION: tuple[tuple[str, Any], ...] = (
    ("model", config.REPRESENTATION_MODEL),
    ("dimensions", config.REPRESENTATION_DIMENSIONS),
    ("metric", config.REPRESENTATION_METRIC),
    ("sanitizer_version", config.REPRESENTATION_SANITIZER_VERSION),
)


_REGRESSION_REPAIR = Template(
    template_id="ordinary-regression-repair/v1",
    version=1,
    family="Regression repair",
    fixed_steps=(
        "Establish the concrete failure and affected component.",
        "Run one discriminating check before editing.",
        "Make the bounded repair.",
        "Re-run the focused check and affected integration checks.",
    ),
    required_fields=("failure", "component"),
    allowed_edits=("bindings",),
    verification_intent="Focused failure check passes and affected regression suite is green.",
    keywords=("regression", "failure", "test", "repair"),
    representation=_CANONICAL_REPRESENTATION,
)

_PUBLIC_INTERFACE_CHANGE = Template(
    template_id="ordinary-public-interface-change/v1",
    version=1,
    family="Public interface change",
    fixed_steps=(
        "Establish the requested contract and enumerate consumers.",
        "Change the interface and implementation coherently.",
        "Preserve required compatibility or provide the approved migration.",
        "Verify consumers and compatibility checks.",
    ),
    required_fields=("contract", "consumers", "compatibility_policy"),
    allowed_edits=("bindings",),
    verification_intent="Contract tests and required consumer compatibility checks pass.",
    keywords=("interface", "api", "contract", "consumer"),
    representation=_CANONICAL_REPRESENTATION,
)

_DEPENDENCY_UPGRADE = Template(
    template_id="ordinary-dependency-upgrade/v1",
    version=1,
    family="Dependency upgrade",
    fixed_steps=(
        "Establish the dependency and exact target version.",
        "Update the declared dependency and lock surfaces.",
        "Repair concrete fallout only.",
        "Re-run dependency and affected tests.",
    ),
    required_fields=("dependency", "target_version"),
    allowed_edits=("bindings",),
    verification_intent="Dependency resolution and affected tests pass at the target version.",
    keywords=("dependency", "upgrade", "version"),
    representation=_CANONICAL_REPRESENTATION,
)

_SCHEMA_MIGRATION = Template(
    template_id="ordinary-schema-data-migration/v1",
    version=1,
    family="Schema/data migration",
    fixed_steps=(
        "Establish the current and required data states.",
        "Preserve compatibility and an explicit recovery path.",
        "Change related schema/data surfaces together.",
        "Verify before, after, and failure cases.",
    ),
    required_fields=("current_state", "required_state", "rollback_policy"),
    allowed_edits=("bindings",),
    verification_intent="Before/after migration checks and the declared failure path pass.",
    keywords=("schema", "data", "migration"),
    representation=_CANONICAL_REPRESENTATION,
)

_INTEGRATION_FAILURE = Template(
    template_id="ordinary-integration-failure/v1",
    version=1,
    family="Integration-failure investigation",
    fixed_steps=(
        "Reproduce and localize the boundary failure.",
        "Discriminate between competing hypotheses.",
        "Repair after localization.",
        "Re-run the integration boundary checks.",
    ),
    required_fields=("boundary", "reproduction"),
    allowed_edits=("bindings",),
    verification_intent="The localized integration boundary check passes.",
    keywords=("integration", "boundary", "external", "integration failure"),
    representation=_CANONICAL_REPRESENTATION,
)


def load_default_templates() -> tuple[Template, ...]:
    return (
        _REGRESSION_REPAIR,
        _PUBLIC_INTERFACE_CHANGE,
        _DEPENDENCY_UPGRADE,
        _SCHEMA_MIGRATION,
        _INTEGRATION_FAILURE,
    )


def select_template(objective_text: str, registry: Iterable[Template]) -> Template | None:
    text = objective_text.lower()
    best: tuple[int, Template] | None = None
    for template in registry:
        score = sum(1 for keyword in template.keywords if keyword in text)
        if score and (best is None or score > best[0]):
            best = (score, template)
    return best[1] if best is not None else None


def validate_bindings(template: Template, bindings: Mapping[str, Any]) -> None:
    if not isinstance(bindings, Mapping):
        raise TemplateError("bindings must be an object")
    missing = [field for field in template.required_fields if field not in bindings]
    if missing:
        raise MissingBindingsError(f"missing required template bindings: {', '.join(missing)}")
    for field in template.required_fields:
        if bindings[field] is None:
            raise MissingBindingsError(f"required template binding {field!r} is null")


def direct_fill(
    template: Template,
    bindings: Mapping[str, Any],
    *,
    objective_id: str,
    route: str,
) -> dict[str, Any]:
    """Fill only declared bindings while preserving the template structure."""

    validate_bindings(template, bindings)
    if route not in template.routes:
        raise TemplateError(f"template is not applicable to route {route!r}")
    content = {
        "fixed_steps": list(template.fixed_steps),
        "bindings": dict(bindings),
        "verification_intent": template.verification_intent,
    }
    return contracts.make_plan(
        plan_id=f"{template.template_id}:{objective_id}",
        objective_id=objective_id,
        route=route,
        state="proposed",
        content=content,
        source={
            "template_id": template.template_id,
            "template_version": template.version,
            "branch": "direct_fill",
        },
    )


def fresh_plan(*, objective_id: str, route: str, content: Any | None = None) -> dict[str, Any]:
    """Return a fresh-plan record with no reusable template provenance."""

    return contracts.make_plan(
        plan_id=f"fresh:{objective_id}",
        objective_id=objective_id,
        route=route,
        state="fresh",
        content=content if content is not None else {"steps": []},
    )


# --- STEP-04: comparable sanitized representations and ranked shortlists ----

def _tokens(value: str) -> tuple[str, ...]:
    import re

    return tuple(sorted(set(re.findall(r"[a-z0-9_]+", value.casefold()))))


def representation_identity(
    *,
    limits: Any | None = None,
) -> dict[str, Any]:
    """Return the one canonical representation identity used for comparison."""

    from .config import PreparationLimits

    resolved = limits if isinstance(limits, PreparationLimits) else PreparationLimits()
    return resolved.representation_identity


MAX_REPRESENTATION_TOKENS = 32


def bounded_token_projection(tokens: Any) -> list[str]:
    """Return the one bounded sanitized token projection both phases compare.

    The bounded store query carries at most this many canonical tokens in their
    canonical order, and the trusted reuse recomputation scores exactly that
    projection, so a long task can never be selected against one token set and
    then judged against another.
    """

    if not isinstance(tokens, (list, tuple)):
        return []
    return [
        token
        for token in list(tokens)[:MAX_REPRESENTATION_TOKENS]
        if isinstance(token, str) and token
    ]


def projected_objective(objective: Mapping[str, Any]) -> dict[str, Any]:
    """Return the canonical objective reduced to that one projection."""

    record = dict(objective)
    record["tokens"] = bounded_token_projection(objective.get("tokens", []))
    return record


def objective_representation(
    objective_text: str,
    *,
    route: str,
    limits: Any | None = None,
    failure_context: str | None = None,
) -> dict[str, Any]:
    """Build the canonical sanitized representation of one exact objective."""

    if not isinstance(objective_text, str) or not objective_text.strip():
        raise TemplateError("objective text must be a nonempty string")
    if route not in contracts.ROUTES:
        raise TemplateError(f"unknown route: {route!r}")
    if failure_context is not None and not isinstance(failure_context, str):
        raise TemplateError("failure context must be a string or null")
    text = objective_text.casefold()
    if failure_context:
        text = text + " " + failure_context.casefold()
    record = representation_identity(limits=limits)
    record["route"] = route
    record["tokens"] = list(_tokens(text))
    return record


def template_representation(template: Template, *, limits: Any | None = None) -> dict[str, Any]:
    """Return the declared representation, or an uncomparable placeholder.

    A template that declares no representation cannot be compared with any
    objective representation, so it is never eligible for reuse.
    """

    record: dict[str, Any] = dict(template.representation) if template.representation else {}
    record["route"] = template.routes[0] if template.routes else "ordinary"
    record["tokens"] = list(_tokens(" ".join(template.keywords)))
    record["declared"] = template.representation_declared
    return record


def representations_comparable(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    """Representations are comparable only when every identity field matches."""

    identity_keys = ("model", "dimensions", "metric", "sanitizer_version")
    if not isinstance(left, Mapping) or not isinstance(right, Mapping):
        return False
    return all(
        key in left and key in right and left[key] == right[key] for key in identity_keys
    )


def _token_overlap(left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
    left_tokens = {str(item) for item in left.get("tokens", [])}
    right_tokens = {str(item) for item in right.get("tokens", [])}
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / float(len(left_tokens))


def score_representations(left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
    """Deterministic containment score over sanitized token sets."""

    if not representations_comparable(left, right):
        raise TemplateError("representations are not comparable")
    return _token_overlap(left, right)


@dataclass(frozen=True)
class TemplateMatch:
    """One bounded shortlist entry; ineligible matches keep their reason."""

    template: Template
    score: float
    comparable: bool
    eligible: bool
    reason: str

    @property
    def direct_fill_eligible(self) -> bool:
        return self.eligible and self.score >= 0.0


_SPECIFICITY_RANK = {"private": 0, "project": 1, "shared": 2}


def rank_templates(
    objective: Mapping[str, Any],
    registry: Iterable[Template],
    *,
    limits: Any | None = None,
) -> list[TemplateMatch]:
    """Rank every template deterministically, keeping ineligible reasons."""

    from .config import PreparationLimits

    resolved = limits if isinstance(limits, PreparationLimits) else PreparationLimits()
    route = str(objective.get("route", "ordinary"))
    matches: list[TemplateMatch] = []
    for template in registry:
        candidate = template_representation(template, limits=resolved)
        comparable = template.representation_declared and representations_comparable(
            objective, candidate
        )
        # Raw lexical overlap keeps an ineligible match visible at its true
        # relevance position so it can never mask a lower eligible candidate;
        # it is a trace ordering only, never a comparable reuse score.
        raw = _token_overlap(objective, candidate)
        if not comparable:
            matches.append(
                TemplateMatch(template, raw, False, False, "incomparable representation")
            )
            continue
        if route not in template.routes:
            matches.append(
                TemplateMatch(
                    template, raw, True, False, f"template is not applicable to route {route!r}"
                )
            )
            continue
        score = score_representations(objective, candidate)
        if score < resolved.minimum_comparable_score:
            matches.append(
                TemplateMatch(template, score, True, False, "below the configured minimum score")
            )
            continue
        matches.append(TemplateMatch(template, score, True, True, "eligible"))
    # Relevance first; equal relevance keeps the caller's declared registry
    # order, which is the deterministic local tie policy.
    matches.sort(key=lambda item: (-item.score,))
    return matches


def eligible_matches(matches: Iterable[TemplateMatch]) -> list[TemplateMatch]:
    return [match for match in matches if match.eligible]


def direct_fill_band(
    matches: Iterable[TemplateMatch], *, limits: Any | None = None
) -> list[TemplateMatch]:
    from .config import PreparationLimits

    resolved = limits if isinstance(limits, PreparationLimits) else PreparationLimits()
    return [match for match in matches if match.eligible and match.score >= resolved.direct_fill_threshold]


def near_match_band(
    matches: Iterable[TemplateMatch], *, limits: Any | None = None
) -> list[TemplateMatch]:
    from .config import PreparationLimits

    resolved = limits if isinstance(limits, PreparationLimits) else PreparationLimits()
    return [
        match
        for match in matches
        if match.eligible and resolved.near_match_threshold <= match.score < resolved.direct_fill_threshold
    ]


_ALLOWED_BINDING_TYPES = (str, int, float, bool)


def validate_typed_bindings(template: Template, bindings: Mapping[str, Any]) -> None:
    """Direct fill may populate only declared, typed, trusted fields."""

    validate_bindings(template, bindings)
    unknown = [key for key in bindings if key not in template.required_fields]
    if unknown:
        raise TemplateError(
            "direct fill may not populate undeclared fields: " + ", ".join(sorted(unknown))
        )
    for field in template.required_fields:
        value = bindings[field]
        if isinstance(value, bool) or not isinstance(value, _ALLOWED_BINDING_TYPES):
            raise TemplateError(f"direct fill field {field!r} must be a typed scalar value")
        if isinstance(value, str) and not value.strip():
            raise TemplateError(f"direct fill field {field!r} must not be blank")


def direct_fill_typed(
    template: Template,
    bindings: Mapping[str, Any],
    *,
    objective_id: str,
    route: str,
) -> dict[str, Any]:
    """Fill declared typed fields while preserving fixed structure."""

    validate_typed_bindings(template, bindings)
    plan = direct_fill(template, bindings, objective_id=objective_id, route=route)
    content = plan["content"]
    content["fixed_steps"] = list(template.fixed_steps)
    content["verification_intent"] = template.verification_intent
    plan["content"] = content
    plan["content_hash"] = contracts.content_hash(plan)
    contracts.validate_plan(plan)
    return plan




__all__ = [
    "Template",
    "TemplateError",
    "MissingBindingsError",
    "load_default_templates",
    "select_template",
    "validate_bindings",
    "direct_fill",
    "fresh_plan",
    "TemplateMatch",
    "representation_identity",
    "MAX_REPRESENTATION_TOKENS",
    "bounded_token_projection",
    "projected_objective",
    "objective_representation",
    "template_representation",
    "representations_comparable",
    "score_representations",
    "rank_templates",
    "eligible_matches",
    "direct_fill_band",
    "near_match_band",
    "validate_typed_bindings",
    "direct_fill_typed",
]

