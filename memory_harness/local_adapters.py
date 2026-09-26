"""Local SearchStore adapters over the accepted evidence and template owners.

This module is the narrow STEP-04 integration seam that turns the already
accepted local reviewed-evidence service and the bundled template families into
real `search.SearchStore` inputs for `preparation.PreparationService`.  It adds
no configuration system and no second authority: the caller keeps passing its
own stores through the existing `prepare(stores=...)` seam, and this factory
only builds two local adapters from explicit inputs.

Historical evidence stays evidence.  Every evidence candidate is the accepted
service's sanitized projection of one durable ROOT-reviewed trajectory with its
exact reviewed status, review receipt, evidence references, and scope.  It is
delivered as the `historical_evidence` candidate kind only, so it never becomes
procedural guidance or an accepted plan.  Templates are adapted with their
declared representation identity, declared routes, configured scoring, and the
central preparation limits; an undeclared, incomparable, low-scoring, or
route-inapplicable template is never emitted.

Every store query consumes only the already sanitized objective payload it is
handed, reads the local service and the declared registry only, and makes no
external call.  The evidence store calls the accepted service on the thread the
accepted bounded search hands it, and the accepted durable store serves that
reviewed-evidence read on its own read-only connection, so the lookup and its
durable receipt validation work inside the bounded worker thread without
sharing the thread-affine store handle or weakening any check.
"""

from __future__ import annotations

import re
from typing import Any, Iterable, Mapping

from . import config, contracts, experience, search, templates


class LocalAdapterError(ValueError):
    """The local adapter factory received unusable explicit inputs."""


_IDENTITY_KEYS = ("model", "dimensions", "metric", "sanitizer_version")
_EVIDENCE_KIND = "historical_evidence"
_TEMPLATE_KIND = "template"
_EVIDENCE_STORE_ID = "local-reviewed-experience"
_TEMPLATE_STORE_ID = "local-template-registry"
_EVIDENCE_AUTHORITY = "reviewed_historical_evidence"


def make_local_search_stores(
    *,
    experience_service: Any,
    scope: experience.ExperienceScope,
    registry: Iterable[templates.Template] | None = None,
    limits: config.PreparationLimits | None = None,
) -> tuple[search.SearchStore, ...]:
    """Return the two smallest local stores for one explicit scope.

    The first store queries the accepted
    `ReviewedExperienceService.search_recent_evidence` with the explicit
    `ExperienceScope` and emits only sanitized, reviewed, exactly scoped
    historical evidence.  The second ranks the caller-declared registry with
    each template's declared representation and the central limits and emits
    only eligible matches.  Both values are ordinary `search.SearchStore`
    instances for the existing caller-supplied `prepare(stores=...)` API.
    """

    if not callable(getattr(experience_service, "search_recent_evidence", None)):
        raise LocalAdapterError(
            "the local evidence adapter needs a service exposing search_recent_evidence"
        )
    if not isinstance(scope, experience.ExperienceScope):
        raise LocalAdapterError(
            "the local evidence adapter needs one explicit ExperienceScope"
        )
    if limits is not None and not isinstance(limits, config.PreparationLimits):
        raise LocalAdapterError("preparation limits must be a PreparationLimits value")
    resolved_limits = limits or config.PreparationLimits()
    resolved_registry = (
        tuple(registry) if registry is not None else templates.load_default_templates()
    )
    scope_record = scope.to_record()
    return (
        search.SearchStore(
            store_id=_EVIDENCE_STORE_ID,
            kind=_EVIDENCE_KIND,
            query=lambda payload: _evidence_candidates(
                experience_service, scope, scope_record, payload, resolved_limits
            ),
            scope=scope_record,
            freshness="live",
            specificity="local",
        ),
        search.SearchStore(
            store_id=_TEMPLATE_STORE_ID,
            kind=_TEMPLATE_KIND,
            query=lambda payload: _template_candidates(
                resolved_registry, payload, resolved_limits
            ),
            scope=None,
            freshness="live",
            specificity="local",
        ),
    )


# --- shared payload helpers -----------------------------------------------

def _tokens(value: str) -> tuple[str, ...]:
    return tuple(sorted(set(re.findall(r"[a-z0-9_]+", value.casefold()))))


def _payload_tokens(payload: Any) -> list[str]:
    """Read the one bounded sanitized token projection out of a query."""

    if not isinstance(payload, Mapping):
        return []
    return templates.bounded_token_projection(payload.get("tokens", []))


def _objective_representation(payload: Any) -> dict[str, Any] | None:
    """Read the already sanitized objective representation of one query."""

    if not isinstance(payload, Mapping):
        return None
    representation = payload.get("representation")
    route = payload.get("route")
    if not isinstance(representation, Mapping) or route not in contracts.ROUTES:
        return None
    identity: dict[str, Any] = {}
    for key in _IDENTITY_KEYS:
        if key not in representation:
            return None
        identity[key] = representation[key]
    tokens = _payload_tokens(payload)
    if not tokens:
        return None
    return {**identity, "route": route, "tokens": tokens}


# --- reviewed historical evidence -----------------------------------------

def _evidence_candidates(
    service: Any,
    scope: experience.ExperienceScope,
    scope_record: Mapping[str, str],
    payload: Any,
    limits: config.PreparationLimits,
) -> list[dict[str, Any]]:
    """Query only the accepted reviewed-evidence service for this exact scope."""

    objective = _objective_representation(payload)
    if objective is None:
        return []
    projections = service.search_recent_evidence(scope, " ".join(objective["tokens"]))
    identity = templates.representation_identity(limits=limits)
    candidates: list[dict[str, Any]] = []
    for projection in projections or ():
        candidate = _evidence_candidate(
            projection,
            objective=objective,
            identity=identity,
            scope_record=scope_record,
        )
        if candidate is not None:
            candidates.append(candidate)
    return candidates


def _evidence_candidate(
    projection: Any,
    *,
    objective: Mapping[str, Any],
    identity: Mapping[str, Any],
    scope_record: Mapping[str, str],
) -> dict[str, Any] | None:
    """Convert one accepted projection into one evidence-only candidate.

    A projection that cannot be established as exactly reviewed, exactly
    scoped evidence with a locally comparable representation is omitted: this
    adapter never delivers historical evidence on partial trust, and it never
    promotes that evidence into guidance or a plan.
    """

    if not isinstance(projection, Mapping):
        return None
    if projection.get("kind") != _EVIDENCE_KIND:
        return None
    if projection.get("authority") != _EVIDENCE_AUTHORITY:
        return None
    if not experience.is_reviewed(projection):
        return None
    logical_id = projection.get("id")
    revision_id = projection.get("review_receipt_id")
    content = projection.get("content")
    projection_scope = projection.get("scope")
    if not isinstance(logical_id, str) or not logical_id.strip():
        return None
    if not isinstance(revision_id, str) or not revision_id.strip():
        return None
    if not isinstance(content, str) or not content.strip():
        return None
    if not isinstance(projection_scope, Mapping) or dict(projection_scope) != dict(scope_record):
        # A digest, title, or token match is never authority to cross scope.
        return None
    references = projection.get("evidence_refs")
    if not isinstance(references, (list, tuple)) or not references:
        return None
    evidence_refs = [item for item in references if isinstance(item, str) and item]
    if len(evidence_refs) != len(references):
        return None
    evidence_tokens = _tokens(content)
    if not evidence_tokens:
        # Without locally derived content tokens no comparable representation
        # can be established, so the evidence is not delivered at all.
        return None
    representation: dict[str, Any] = dict(identity)
    representation["tokens"] = list(evidence_tokens)
    representation["route"] = str(objective.get("route", "ordinary"))
    representation["declared"] = True
    if not templates.representations_comparable(objective, representation):
        return None
    body = {
        "kind": _EVIDENCE_KIND,
        "authority": _EVIDENCE_AUTHORITY,
        "status": str(projection.get("status")),
        "content": content,
        "evidence_refs": evidence_refs,
        "review_receipt_id": revision_id,
        "scope": dict(scope_record),
    }
    return {
        "kind": _EVIDENCE_KIND,
        "logical_id": logical_id,
        "revision_id": revision_id,
        "origin": _EVIDENCE_STORE_ID,
        "source_id": _EVIDENCE_STORE_ID,
        "payload": body,
        "payload_digest": contracts.sha256_hex(body),
        "scope": dict(scope_record),
        "representation": representation,
        "score": templates.score_representations(objective, representation),
        "freshness": "live",
    }


# --- declared local template registry -------------------------------------

def _template_candidates(
    registry: Iterable[templates.Template],
    payload: Any,
    limits: config.PreparationLimits,
) -> list[dict[str, Any]]:
    """Rank the declared registry and emit only eligible templates."""

    objective = _objective_representation(payload)
    if objective is None:
        return []
    candidates: list[dict[str, Any]] = []
    for match in templates.rank_templates(objective, registry, limits=limits):
        if not match.eligible:
            # Undeclared, incomparable, low-scoring, and route-inapplicable
            # templates are never eligible through this seam.
            continue
        template = match.template
        record = template.to_record()
        candidates.append(
            {
                "kind": _TEMPLATE_KIND,
                "logical_id": template.template_id,
                "revision_id": f"v{template.version}",
                "origin": _TEMPLATE_STORE_ID,
                "source_id": _TEMPLATE_STORE_ID,
                "payload": record,
                "payload_digest": contracts.sha256_hex(record),
                "scope": {},
                "routes": list(template.routes),
                "representation": templates.template_representation(
                    template, limits=limits
                ),
                "score": match.score,
                "freshness": "live",
            }
        )
    return candidates


__all__ = [
    "LocalAdapterError",
    "make_local_search_stores",
]

