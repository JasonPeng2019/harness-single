"""EverOS case SearchStore adapter over the accepted reviewed-experience service.

This module is the narrow STEP-04 integration seam that turns the already
accepted EverOS case receipts and ``ReviewedExperienceService`` durable-join
path into one real ``search.SearchStore`` input for
``preparation.PreparationService``, separate from the accepted local
recent/unrepresented evidence store.

A remote case is discovery, never authority.  The store consumes only the
already sanitized bounded-search query it is handed (the exact representation
identity, at most 32 sanitized tokens, and the route of this preparation), asks
``EverOSAdapter.search_case_candidates`` for this exact scope's case
candidates through the accepted public search surface, and then requires the
accepted service to rejoin every hit to its durable confirmed ingestion, case
receipt, reviewed trajectory, and review receipt in the same scope before a
sanitized historical-evidence candidate is derived.  A foreign, malformed,
unconfirmed, altered, unreviewed, or missing receipt omits that one hit and can
never discard an unrelated eligible candidate, and no remote case ever becomes
procedural guidance or a plan.

The store declares ``requires_network=True`` because the case query performs a
real shared/remote task-path call, so the existing preparation gate suppresses
the actual call when ``experience_read`` is disabled or the network mode is
``restricted_local`` while every eligible local store keeps its chance.

The same module carries the narrower EverOS generated-skill procedure
store: one sanitized scoped skill query on the accepted adapter, an exact
match of the durable Step-02 candidate (stable skill id, sanitized content,
exact source case ids, scope, and its sanitized stable metadata), and the
accepted local generated procedure's exact trusted current-candidate path
for the intended recipient.  The remote hit stays discovery provenance
only: it never supplies authority or a ranking score, and it only adds
provenance to the one existing procedure candidate.  The store declares
``requires_network=True`` and ``source_kind="everos_generated_skill"`` so
the accepted gate suppresses the actual call when generated-skill use is
off or the network mode is ``restricted_local``.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any, Mapping

from . import (
    config,
    contracts,
    experience,
    local_procedure_adapters,
    privacy,
    search,
    templates,
)

_IDENTITY_KEYS = ("model", "dimensions", "metric", "sanitizer_version")
_EVIDENCE_KIND = "historical_evidence"
_EVIDENCE_AUTHORITY = "reviewed_historical_evidence"
_EVEROS_STORE_ID = "everos-reviewed-cases"
_EVEROS_SKILL_STORE_ID = "everos-generated-skills"
_PROCEDURE_KIND = "procedure"
_GENERATED_SKILL_SOURCE = "generated_skill"
_EVEROS_GENERATED_SKILL_SOURCE_KIND = "everos_generated_skill"
_SKILL_METADATA_KEYS = ("name", "description", "confidence", "maturity_score")
_MAX_TOKENS = 32


class EverOSAdapterError(ValueError):
    """The EverOS case-store factory received unusable explicit inputs."""


def make_everos_case_search_store(
    *,
    experience_service: Any,
    adapter: Any,
    scope: experience.ExperienceScope,
    limits: config.PreparationLimits | None = None,
    top_k: int = 100,
) -> search.SearchStore:
    """Return the one smallest EverOS case store for one exact scope.

    ``experience_service`` is the accepted ``ReviewedExperienceService`` whose
    ``confirmed_case_evidence`` rejoins one remote case to its durable
    confirmed ingestion, case receipt, reviewed trajectory, and review receipt;
    ``adapter`` is the accepted ``EverOSAdapter`` (or a deterministic double of
    its public scoped case-query surface) already bound to the same scope;
    ``scope`` is that exact four-part scope; ``limits`` is the central
    preparation limit set whose representation identity this store declares;
    and ``top_k`` bounds the one keyword query.
    """

    if not callable(getattr(experience_service, "confirmed_case_evidence", None)):
        raise EverOSAdapterError(
            "the EverOS case store needs a service exposing confirmed_case_evidence"
        )
    if not callable(getattr(adapter, "search_case_candidates", None)):
        raise EverOSAdapterError(
            "the EverOS case store needs an adapter exposing the scoped case query"
        )
    if not callable(getattr(adapter, "validate_case_scope", None)):
        raise EverOSAdapterError(
            "the EverOS case store needs an adapter exposing its case scope check"
        )
    if not isinstance(scope, experience.ExperienceScope):
        raise EverOSAdapterError(
            "the EverOS case store needs one explicit ExperienceScope"
        )
    if getattr(adapter, "scope", None) != scope:
        raise EverOSAdapterError(
            "the EverOS adapter is bound to a different experience scope"
        )
    if limits is not None and not isinstance(limits, config.PreparationLimits):
        raise EverOSAdapterError("preparation limits must be a PreparationLimits value")
    if isinstance(top_k, bool) or not isinstance(top_k, int) or top_k <= 0:
        raise EverOSAdapterError("the EverOS case query bound must be a positive integer")
    resolved_limits = limits or config.PreparationLimits()
    scope_record = scope.to_record()
    return search.SearchStore(
        store_id=_EVEROS_STORE_ID,
        kind=_EVIDENCE_KIND,
        query=lambda payload: _case_evidence_candidates(
            experience_service,
            adapter,
            scope,
            scope_record,
            resolved_limits,
            int(top_k),
            payload,
        ),
        scope=scope_record,
        freshness="live",
        specificity="project",
        requires_network=True,
    )


# --- the bounded query -----------------------------------------------------

def _tokens(value: str) -> tuple[str, ...]:
    return tuple(sorted(set(re.findall(r"[a-z0-9_]+", value.casefold()))))


def _objective(payload: Any) -> dict[str, Any] | None:
    """Read only the already sanitized bounded-search query of one attempt."""

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
    raw_tokens = payload.get("tokens")
    if not isinstance(raw_tokens, (list, tuple)):
        return None
    tokens = [
        token
        for token in list(raw_tokens)[:_MAX_TOKENS]
        if isinstance(token, str) and token.strip()
    ]
    if not tokens:
        return None
    return {**identity, "route": route, "tokens": tokens}


def _await(coroutine: Any) -> Any:
    """Run the accepted public async search from the bounded worker thread."""

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coroutine)
    raise EverOSAdapterError(
        "the EverOS case query cannot run inside a running event loop"
    )


def _case_evidence_candidates(
    service: Any,
    adapter: Any,
    scope: experience.ExperienceScope,
    scope_record: Mapping[str, str],
    limits: config.PreparationLimits,
    top_k: int,
    payload: Any,
) -> list[dict[str, Any]]:
    """Query this scope's case candidates and convert only exact durable joins."""

    objective = _objective(payload)
    if objective is None:
        return []
    central = templates.representation_identity(limits=limits)
    if any(objective.get(key) != central.get(key) for key in _IDENTITY_KEYS):
        # The store's central identity and the bounded query disagree: no exact
        # comparison can be established, so no remote call begins at all.
        return []
    adapter.assert_scope(scope_record)
    source_cases = _await(
        adapter.search_case_candidates(query=" ".join(objective["tokens"]), top_k=top_k)
    )
    candidates: list[dict[str, Any]] = []
    for source_case in _materialize(source_cases):
        candidate = _evidence_candidate(
            service,
            adapter,
            scope,
            scope_record,
            objective,
            central,
            source_case,
        )
        if candidate is not None:
            candidates.append(candidate)
    return candidates


def _materialize(value: Any) -> list[Any]:
    return [] if value is None else list(value)


def _evidence_candidate(
    service: Any,
    adapter: Any,
    scope: experience.ExperienceScope,
    scope_record: Mapping[str, str],
    objective: Mapping[str, Any],
    identity: Mapping[str, Any],
    source_case: Any,
) -> dict[str, Any] | None:
    """Convert one remote hit into one evidence-only candidate, or omit it.

    The adapter's exact scope check, the accepted service's durable rejoin, and
    the central comparable representation all have to hold for this exact hit:
    an unreviewed, unconfirmed, unresolved, altered, cross-scope, malformed, or
    stale remote case simply never becomes a candidate, and no such hit can
    suppress an unrelated one.
    """

    if not isinstance(source_case, Mapping):
        return None
    try:
        adapter.validate_case_scope(source_case)
    except Exception:
        return None
    case_id = source_case.get("id")
    if not isinstance(case_id, str) or not case_id.strip():
        return None
    try:
        projection = service.confirmed_case_evidence(scope, source_case)
    except Exception:
        return None
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
        return None
    references = projection.get("evidence_refs")
    if not isinstance(references, (list, tuple)) or not references:
        return None
    evidence_refs = [item for item in references if isinstance(item, str) and item]
    if len(evidence_refs) != len(references):
        return None
    evidence_tokens = _tokens(content)
    if not evidence_tokens:
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
        "case_id": case_id,
        "case_receipt_id": str(projection.get("case_receipt_id")),
    }
    return {
        "kind": _EVIDENCE_KIND,
        "logical_id": logical_id,
        "revision_id": revision_id,
        "origin": _EVEROS_STORE_ID,
        "source_id": _EVEROS_STORE_ID,
        "payload": body,
        "payload_digest": contracts.sha256_hex(body),
        "scope": dict(scope_record),
        "representation": representation,
        "score": templates.score_representations(objective, representation),
        "freshness": "live",
    }


# --- the EverOS generated-skill procedure store -----------------------------

def make_everos_generated_skill_search_store(
    *,
    experience_service: Any,
    procedure_service: Any,
    memory_store: Any,
    adapter: Any,
    scope: experience.ExperienceScope,
    receiver: Mapping[str, Any],
    facts: Mapping[str, Any],
    route: str,
    limits: config.PreparationLimits | None = None,
    top_k: int = 100,
) -> search.SearchStore:
    """Return the one smallest EverOS generated-skill procedure store.

    ``experience_service`` is the accepted ``ReviewedExperienceService`` whose
    ``privacy_policy`` is the exact sanitizer the accepted Step-02 skill path
    used for the durable candidate's content and metadata; ``procedure_service``
    is the accepted ``TrustedProcedureService`` whose trusted-issuer, recipient,
    predicate, designation, revocation, and representation policy stays
    authoritative; ``memory_store`` is the accepted durable store exposing
    ``read_generated_skill_candidates_for_scope``; ``adapter`` is the accepted
    ``EverOSAdapter`` (or a deterministic double of its public scoped
    skill-query surface) already bound to the same scope; ``scope`` is that
    exact skill scope; ``receiver`` is the intended recipient scope; ``facts``
    are the trusted predicate facts; ``route`` is the exact route whose
    predicates must hold; ``limits`` is the central preparation limit set whose
    representation identity this store declares; and ``top_k`` bounds the one
    keyword query.

    The store is discovery over already sanitized queries only: one remote hit
    becomes the existing local procedure candidate exclusively through its
    exact durable Step-02 candidate and the accepted local generated
    procedure's current trusted designation for this recipient.  The remote
    hit contributes discovery provenance, never authority and never a ranking
    score.
    """

    policy = getattr(experience_service, "privacy_policy", None)
    if not isinstance(policy, privacy.PrivacyPolicy):
        raise EverOSAdapterError(
            "the EverOS generated-skill store needs the accepted service's "
            "sanitizer policy"
        )
    if not callable(
        getattr(memory_store, "read_generated_skill_candidates_for_scope", None)
    ):
        raise EverOSAdapterError(
            "the EverOS generated-skill store needs a store exposing the durable "
            "Step-02 candidate lookup"
        )
    for method in ("search_skill_candidates", "validate_skill", "assert_scope"):
        if not callable(getattr(adapter, method, None)):
            raise EverOSAdapterError(
                f"the EverOS generated-skill store needs an adapter exposing {method}"
            )
    if not isinstance(scope, experience.ExperienceScope):
        raise EverOSAdapterError(
            "the EverOS generated-skill store needs one explicit ExperienceScope"
        )
    if getattr(adapter, "scope", None) != scope:
        raise EverOSAdapterError(
            "the EverOS adapter is bound to a different experience scope"
        )
    if limits is not None and not isinstance(limits, config.PreparationLimits):
        raise EverOSAdapterError("preparation limits must be a PreparationLimits value")
    if isinstance(top_k, bool) or not isinstance(top_k, int) or top_k <= 0:
        raise EverOSAdapterError(
            "the EverOS skill query bound must be a positive integer"
        )
    resolved_limits = limits or config.PreparationLimits()
    try:
        local_stores = local_procedure_adapters.make_local_procedure_search_stores(
            procedure_service=procedure_service,
            memory_store=memory_store,
            receiver=receiver,
            facts=facts,
            route=route,
            limits=resolved_limits,
        )
    except local_procedure_adapters.LocalProcedureAdapterError as exc:
        raise EverOSAdapterError(
            "the EverOS generated-skill store needs the accepted local generated "
            f"procedure path: {exc}"
        ) from exc
    generated_store = next(
        item
        for item in local_stores
        if item.store_id == local_procedure_adapters.GENERATED_STORE_ID
    )
    scope_record = scope.to_record()
    receiver_record = dict(generated_store.scope or {})
    return search.SearchStore(
        store_id=_EVEROS_SKILL_STORE_ID,
        kind=_PROCEDURE_KIND,
        query=lambda payload: _generated_skill_procedure_candidates(
            policy,
            generated_store,
            memory_store,
            adapter,
            scope_record,
            resolved_limits,
            int(top_k),
            payload,
        ),
        scope=receiver_record,
        freshness="live",
        specificity="project",
        requires_network=True,
        source_kind=_EVEROS_GENERATED_SKILL_SOURCE_KIND,
    )


def _generated_skill_procedure_candidates(
    policy: privacy.PrivacyPolicy,
    local_generated_store: search.SearchStore,
    memory_store: Any,
    adapter: Any,
    scope_record: Mapping[str, str],
    limits: config.PreparationLimits,
    top_k: int,
    payload: Any,
) -> list[dict[str, Any]]:
    """Query this scope's skill hits and convert only exact durable joins."""

    objective = _objective(payload)
    if objective is None:
        return []
    central = templates.representation_identity(limits=limits)
    if any(objective.get(key) != central.get(key) for key in _IDENTITY_KEYS):
        # The store's central identity and the bounded query disagree: no exact
        # comparison can be established, so no remote call begins at all.
        return []
    adapter.assert_scope(scope_record)
    source_skills = _await(
        adapter.search_skill_candidates(
            query=" ".join(objective["tokens"]), top_k=top_k
        )
    )
    hits = _materialize(source_skills)
    if not hits:
        return []
    # The eligible current trusted generated procedures of this exact recipient
    # come exclusively from the accepted local generated-procedure store, so
    # every issuer, recipient, predicate, revocation, designation, and
    # representation check stays the accepted one.
    eligible = list(local_generated_store.query(payload))
    if not eligible:
        return []
    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for source_skill in hits:
        for candidate in _joined_procedure_candidates(
            policy, memory_store, adapter, scope_record, eligible, source_skill
        ):
            key = (candidate["logical_id"], candidate["revision_id"])
            if key in seen:
                continue
            seen.add(key)
            candidates.append(candidate)
    return candidates


def _joined_procedure_candidates(
    policy: privacy.PrivacyPolicy,
    memory_store: Any,
    adapter: Any,
    scope_record: Mapping[str, str],
    eligible: list[Any],
    source_skill: Any,
) -> list[dict[str, Any]]:
    """Rejoin one raw remote hit to its eligible current trusted procedure.

    A foreign, malformed, changed, or uncurrent hit is omitted by itself and
    can never suppress an unrelated hit's candidate: the adapter's exact skill
    scope check, the durable Step-02 candidate match, and the joined local
    procedure's retained source all have to hold for this exact hit.
    """

    if not isinstance(source_skill, Mapping):
        return []
    try:
        adapter.validate_skill(source_skill)
    except Exception:
        return []
    durable = _matched_durable_candidate(
        policy, memory_store, scope_record, source_skill
    )
    if durable is None:
        return []
    source_cases = [dict(item) for item in durable["source_cases"]]
    joined: list[dict[str, Any]] = []
    for record in eligible:
        if not isinstance(record, Mapping):
            continue
        body = record.get("payload")
        if not isinstance(body, Mapping):
            continue
        source = body.get("source")
        if (
            not isinstance(source, Mapping)
            or source.get("kind") != _GENERATED_SKILL_SOURCE
        ):
            continue
        if source.get("candidate_id") != durable["candidate_id"]:
            continue
        if source.get("candidate_digest") != durable["content_hash"]:
            continue
        if source.get("source_cases") != source_cases:
            continue
        joined.append(_remote_sighting(record))
    return joined


def _matched_durable_candidate(
    policy: privacy.PrivacyPolicy,
    memory_store: Any,
    scope_record: Mapping[str, str],
    source_skill: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    """Match one sanitized remote hit against one intact durable candidate.

    Only the query-dependent EverOS ``score`` may differ between discovery and
    Step-02; every stable field has to match exactly -- the stable skill id, the
    sanitized content, the exact source case ids, the scope, and the
    Step-02-bound sanitized stable metadata.  Anything else is a changed,
    foreign, or uncurrent hit and is omitted.
    """

    skill_id = source_skill.get("id")
    content = source_skill.get("content")
    source_case_ids = source_skill.get("source_case_ids")
    if not isinstance(skill_id, str) or not skill_id.strip():
        return None
    if not isinstance(content, str) or not content.strip():
        return None
    if not isinstance(source_case_ids, (list, tuple)) or not source_case_ids:
        return None
    if any(
        not isinstance(case_id, str) or not case_id for case_id in source_case_ids
    ):
        return None
    if len(set(source_case_ids)) != len(source_case_ids):
        return None
    sanitized_content = privacy.sanitize_text(content, policy)
    metadata = privacy.sanitize_payload(
        {
            key: source_skill[key]
            for key in _SKILL_METADATA_KEYS
            if key in source_skill
        },
        policy,
    )
    if not isinstance(metadata, Mapping):
        return None
    expected_case_ids = sorted(source_case_ids)
    try:
        records = memory_store.read_generated_skill_candidates_for_scope(
            skill_id, scope_record
        )
    except Exception:
        # An unreadable durable lookup yields no match instead of a partial
        # one; the read itself is read-only and fail-closed.
        return None
    for record in records or ():
        if not isinstance(record, Mapping):
            continue
        if record.get("skill_id") != skill_id:
            continue
        if record.get("content") != sanitized_content:
            continue
        if dict(record.get("metadata") or {}) != dict(metadata):
            continue
        record_scope = record.get("scope")
        if not isinstance(record_scope, Mapping) or dict(record_scope) != dict(
            scope_record
        ):
            continue
        cases = record.get("source_cases")
        if not isinstance(cases, list) or len(cases) != len(expected_case_ids):
            continue
        if [item.get("case_id") for item in cases] != expected_case_ids:
            continue
        return record
    return None


def _remote_sighting(record: Mapping[str, Any]) -> dict[str, Any]:
    """Restate one accepted local procedure candidate as this store's sighting.

    The payload, digest, representation, score, and exact procedure identity
    remain the accepted local ones; only the discovery origin moves to this
    store, and the wider declared specificity keeps the accepted dedup policy
    preferring the narrower local sighting of the same revision.
    """

    sighting = dict(record)
    sighting["origin"] = _EVEROS_SKILL_STORE_ID
    sighting["source_id"] = _EVEROS_SKILL_STORE_ID
    sighting["specificity"] = "project"
    return sighting


__all__ = [
    "EverOSAdapterError",
    "make_everos_case_search_store",
    "make_everos_generated_skill_search_store",
]
