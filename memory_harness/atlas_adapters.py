"""Atlas SearchStore adapter over the accepted trusted-procedure service.

This module is the narrow STEP-04 integration seam that turns the already
accepted ``procedures.TrustedProcedureService.resolve_atlas``
discovery/exact-read/delivery path into one real ``search.SearchStore`` input
for ``preparation.PreparationService``.  It adds no second authority: the
accepted service keeps owning discovery limits, exact reads, recipient and
predicate revalidation, current-designation and revocation checks, exact
representation comparison, and the narrower-scope selection, and this adapter
only converts the deliveries that service already validated into
integrity-bound procedure candidates with their exact
logical/revision/publication identity, approval and current-designation
evidence, recipient scope, predicate result, live freshness, and provenance.

The store consumes only the already sanitized bounded-search query payload it
is handed: the exact representation identity and at most 32 sanitized tokens
of one search attempt.  It never re-sanitizes, widens, or rebuilds that query,
and it never treats a discovery score as relevance: candidates are scored on
the same comparable sanitized representation the bounded search itself
compares.  The store declares ``requires_network=True`` so the preparation
gate suppresses the actual task-path call when ``atlas_shared_retrieval`` is
disabled or the network mode is ``restricted_local``.
"""

from __future__ import annotations

import re
from typing import Any, Mapping

from . import atlas, config, contracts, search, templates


class AtlasAdapterError(ValueError):
    """The Atlas adapter factory received unusable explicit inputs."""


_ATLAS_STORE_ID = "atlas-shared-procedures"
_ATLAS_AUTHORITY = "atlas_trusted_procedure"
_PROCEDURE_KIND = "procedure"
_REMOTE_PARTITION_SCOPES = frozenset({"project", "shared"})


def make_atlas_search_store(
    *,
    procedure_service: Any,
    adapter: Any,
    receiver: Mapping[str, Any],
    facts: Mapping[str, Any],
    route: str,
    limits: config.PreparationLimits | None = None,
    network_resolution: config.NetworkResolution | Mapping[str, Any] | None = None,
    atlas_shared_retrieval_enabled: bool = True,
) -> search.SearchStore:
    """Return the one smallest Atlas procedure store for one exact receiver.

    ``procedure_service`` is the accepted ``TrustedProcedureService`` whose
    ``resolve_atlas`` revalidates every delivery; ``adapter`` is the accepted
    ``AtlasProcedureAdapter`` (or a deterministic double of its public
    discovery/exact-read/metric surface); ``receiver`` is the exact recipient
    scope; ``facts`` are the trusted predicate facts; ``route`` is the exact
    route whose predicates must hold; and ``limits`` is the central
    preparation limit set whose representation identity this store declares.
    """

    if not callable(getattr(procedure_service, "resolve_atlas", None)):
        raise AtlasAdapterError(
            "the Atlas adapter needs a service exposing resolve_atlas"
        )
    if not callable(getattr(adapter, "discover", None)) or not callable(
        getattr(adapter, "exact_read", None)
    ):
        raise AtlasAdapterError(
            "the Atlas adapter needs a discovery/exact-read Atlas procedure adapter"
        )
    metric = getattr(adapter, "metric", None)
    if not isinstance(metric, str) or not metric.strip():
        raise AtlasAdapterError("the Atlas adapter must declare its vector metric")
    if not isinstance(receiver, Mapping):
        raise AtlasAdapterError("the Atlas adapter needs one explicit receiver scope")
    try:
        receiver_record = contracts.normalize_experience_scope(receiver)
    except contracts.ContractError as exc:
        raise AtlasAdapterError("the Atlas receiver scope is not exact") from exc
    if not isinstance(facts, Mapping):
        raise AtlasAdapterError("trusted predicate facts must be an object")
    if route not in contracts.ROUTES:
        raise AtlasAdapterError(f"unknown route: {route!r}")
    if limits is not None and not isinstance(limits, config.PreparationLimits):
        raise AtlasAdapterError("preparation limits must be a PreparationLimits value")
    resolved_limits = limits or config.PreparationLimits()
    predicate_facts = dict(facts)
    return search.SearchStore(
        store_id=_ATLAS_STORE_ID,
        kind=_PROCEDURE_KIND,
        query=lambda payload: _atlas_candidates(
            procedure_service,
            adapter,
            receiver_record,
            predicate_facts,
            route,
            resolved_limits,
            payload,
            network_resolution=network_resolution,
            atlas_shared_retrieval_enabled=atlas_shared_retrieval_enabled,
        ),
        scope=receiver_record,
        freshness="live",
        specificity="shared",
        requires_network=True,
    )


# --- the bounded query -----------------------------------------------------

def _tokens(value: str) -> tuple[str, ...]:
    return tuple(sorted(set(re.findall(r"[a-z0-9_]+", value.casefold()))))


def _bounded_query(payload: Any, *, route: str) -> tuple[dict[str, Any], list[str]] | None:
    """Read only the already sanitized bounded-search query of one attempt.

    The payload is the exact one the accepted bounded search built and already
    sanitized: one representation identity, at most 32 tokens, and the route
    of this preparation.  Anything else -- a foreign route, a missing
    identity, or no tokens -- cannot establish the exact comparison this
    store needs, so the query is refused instead of widened.
    """

    if not isinstance(payload, Mapping):
        return None
    representation = payload.get("representation")
    if not isinstance(representation, Mapping):
        return None
    identity: dict[str, Any] = {}
    for key in atlas.ATLAS_QUERY_REPRESENTATION_IDENTITY_FIELDS:
        if key not in representation:
            return None
        identity[key] = representation[key]
    if payload.get("route") != route:
        return None
    raw_tokens = payload.get("tokens")
    if not isinstance(raw_tokens, (list, tuple)):
        return None
    tokens = [
        token
        for token in list(raw_tokens)[:32]
        if isinstance(token, str) and token.strip()
    ]
    if not tokens:
        return None
    return identity, tokens


def _atlas_candidates(
    service: Any,
    adapter: Any,
    receiver_record: Mapping[str, str],
    facts: Mapping[str, Any],
    route: str,
    limits: config.PreparationLimits,
    payload: Any,
    *,
    network_resolution: config.NetworkResolution | Mapping[str, Any] | None = None,
    atlas_shared_retrieval_enabled: bool = True,
) -> list[dict[str, Any]]:
    """Query the accepted service and convert only its validated deliveries."""

    if not atlas_shared_retrieval_enabled or not atlas.atlas_task_network_allowed(
        network_resolution
    ):
        return []

    query = _bounded_query(payload, route=route)
    if query is None:
        return []
    identity, tokens = query
    central = templates.representation_identity(limits=limits)
    if any(
        identity.get(key) != central.get(key)
        for key in atlas.ATLAS_QUERY_REPRESENTATION_IDENTITY_FIELDS
    ):
        # The store's central identity and the bounded query disagree: no
        # exact comparison can be established, so no discovery call begins.
        return []
    objective: dict[str, Any] = dict(central)
    objective["route"] = route
    objective["tokens"] = list(tokens)
    deliveries = service.resolve_atlas(
        " ".join(tokens),
        receiver=receiver_record,
        facts=facts,
        route=route,
        adapter=adapter,
        representation=identity,
        network_resolution=network_resolution,
        atlas_shared_retrieval_enabled=atlas_shared_retrieval_enabled,
    )
    candidates: list[dict[str, Any]] = []
    for delivery in deliveries or ():
        candidate = _procedure_candidate(
            delivery,
            objective=objective,
            identity=central,
            receiver=receiver_record,
            route=route,
        )
        if candidate is not None:
            candidates.append(candidate)
    return candidates


def _procedure_candidate(
    delivery: Any,
    *,
    objective: Mapping[str, Any],
    identity: Mapping[str, Any],
    receiver: Mapping[str, str],
    route: str,
) -> dict[str, Any] | None:
    """Convert one accepted delivery into one integrity-bound candidate.

    The accepted service already revalidated the exact live publication,
    recipient, predicates, current designation, revocation state, and
    representation identity; this conversion never widens that authority and
    omits any delivery that cannot be established exactly.
    """

    if not isinstance(delivery, Mapping):
        return None
    publication_id = delivery.get("publication_id")
    logical_id = delivery.get("logical_id")
    revision_id = delivery.get("revision_id")
    procedure = delivery.get("procedure")
    approval = delivery.get("approval")
    designation = delivery.get("designation")
    representation = delivery.get("representation")
    partition = delivery.get("partition")
    for value in (publication_id, logical_id, revision_id):
        if not isinstance(value, str) or not value.strip():
            return None
    for value in (procedure, approval, designation, representation, partition):
        if not isinstance(value, Mapping):
            return None
    if (
        procedure.get("logical_id") != logical_id
        or procedure.get("revision_id") != revision_id
    ):
        return None
    if approval.get("revision_id") != revision_id:
        return None
    if designation.get("revision_id") != revision_id:
        return None
    if any(
        representation.get(key) != identity.get(key)
        for key in atlas.ATLAS_QUERY_REPRESENTATION_IDENTITY_FIELDS
    ):
        # The delivered representation is not the exact one this store
        # compares; it never becomes guidance.
        return None
    partition_scope = partition.get("scope")
    if partition_scope not in _REMOTE_PARTITION_SCOPES:
        # Private (or unknown) partitions are never remote deliverables.
        return None
    recipients = partition.get("recipients")
    if not isinstance(recipients, (list, tuple)):
        return None
    receiver_key = contracts.canonical_json(receiver).decode("utf-8")
    recipient_keys = {
        contracts.canonical_json(item).decode("utf-8")
        for item in recipients
        if isinstance(item, Mapping)
    }
    if receiver_key not in recipient_keys:
        return None
    search_text = representation.get("search_text")
    if not isinstance(search_text, str) or not search_text.strip():
        return None

    candidate_representation: dict[str, Any] = dict(identity)
    candidate_representation["route"] = route
    candidate_representation["tokens"] = list(_tokens(search_text))
    candidate_representation["declared"] = True
    provenance = {
        "authority": _ATLAS_AUTHORITY,
        "store_id": _ATLAS_STORE_ID,
        "publication_id": publication_id,
        "logical_id": logical_id,
        "revision_id": revision_id,
        "partition_id": designation.get("partition_id"),
        "partition_scope": partition_scope,
        "designation_id": designation.get("designation_id"),
        "designation_generation": designation.get("generation"),
        "representation_id": representation.get("representation_id"),
        "receiver": dict(receiver),
    }
    body = {
        "kind": _PROCEDURE_KIND,
        "authority": _ATLAS_AUTHORITY,
        "publication_id": publication_id,
        "logical_id": logical_id,
        "revision_id": revision_id,
        "procedure": dict(procedure),
        "approval": dict(approval),
        "designation": dict(designation),
        "partition": dict(partition),
        "recipient": dict(receiver),
        # The accepted service proved all three facts for this exact delivery:
        # the approval is current and trusted, the publication is the exact
        # live current designation, and its predicates hold for these facts
        # and this route.
        "predicates_ok": True,
        "freshness": "live",
        "provenance": provenance,
    }
    return {
        "kind": _PROCEDURE_KIND,
        "logical_id": logical_id,
        "revision_id": revision_id,
        "origin": _ATLAS_STORE_ID,
        "source_id": _ATLAS_STORE_ID,
        "payload": body,
        "payload_digest": contracts.sha256_hex(body),
        "scope": dict(receiver),
        "representation": candidate_representation,
        "score": templates.score_representations(objective, candidate_representation),
        "specificity": partition_scope,
        "freshness": "live",
        "approval_status": "approved",
        "designation": "current",
        "predicates_ok": True,
    }


__all__ = [
    "AtlasAdapterError",
    "make_atlas_search_store",
]

