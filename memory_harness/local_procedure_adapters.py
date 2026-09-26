"""Local SearchStore adapters over the accepted trusted-procedure owner.

This module is the narrow STEP-04 integration seam that turns the already
accepted local trusted-procedure lifecycle into real `search.SearchStore`
inputs for `preparation.PreparationService`.  It adds no second authority: the
accepted `procedures.TrustedProcedureService` keeps owning trusted-issuer
policy, `contracts` keeps owning the procedure, approval, designation,
representation, revocation, and predicate contracts, and the durable
`store.MemoryStore` keeps owning the exact records.  This factory only converts
the durable local current designations that all three owners already validate
into integrity-bound local procedure candidates.

Two stores exist because the product feature contract gates their sources
separately:

- `local-curated-procedures` carries curated and builtin procedure guidance.  It
  performs no shared/remote call, so it stays eligible when Atlas shared
  retrieval and generated-skill use are off, and `restricted_local` never
  suppresses it.
- `local-generated-procedures` carries generated-origin guidance that retains
  its exact Step-02 candidate, skill approval, source case, and review-receipt
  provenance, plus its own explicit trusted procedure approval and current
  designation.  It is separately gated: while `generated_skill_use` is off the
  preparation gate makes no query at all.

Neither store treats local skill presence, a Step-02 generated-skill approval,
or a semantic hit as delivery approval.  Every delivery resolves one exact
current designation for the intended recipient, and a record that is stale,
withdrawn, revoked, altered, cross-scope, predicate-incompatible,
representationally incomparable, or missing/replacing its durable evidence is
omitted without suppressing any unrelated eligible procedure.

Every store query consumes only the already sanitized bounded-search payload it
is handed, reads the local durable store on the thread the accepted bounded
search hands it, and makes no external call.
"""

from __future__ import annotations

import re
from typing import Any, Iterable, Mapping

from . import config, contracts, search, templates

CURATED_STORE_ID = "local-curated-procedures"
GENERATED_STORE_ID = "local-generated-procedures"

_PROCEDURE_KIND = "procedure"
_LOCAL_AUTHORITY = "local_trusted_procedure"
_CURATED_ORIGINS = frozenset({"curated", "builtin"})
_IDENTITY_KEYS = ("model", "dimensions", "metric", "sanitizer_version")


class LocalProcedureAdapterError(ValueError):
    """The local procedure adapter factory received unusable explicit inputs."""


def make_local_procedure_search_stores(
    *,
    procedure_service: Any,
    memory_store: Any,
    receiver: Mapping[str, Any],
    facts: Mapping[str, Any],
    route: str,
    limits: config.PreparationLimits | None = None,
) -> tuple[search.SearchStore, search.SearchStore]:
    """Return the two smallest local procedure stores for one exact receiver.

    ``procedure_service`` is the accepted `TrustedProcedureService` whose
    trusted-issuer configuration stays authoritative; ``memory_store`` is the
    accepted durable store exposing the read-only
    `read_local_current_procedures` lookup; ``receiver`` is the exact
    recipient scope; ``facts`` are the trusted predicate facts; ``route`` is
    the exact route whose predicates must hold; and ``limits`` is the central
    preparation limit set whose representation identity both stores declare.
    """

    if not callable(getattr(memory_store, "read_local_current_procedures", None)):
        raise LocalProcedureAdapterError(
            "the local procedure adapter needs a store exposing "
            "read_local_current_procedures"
        )
    trusted_issuers = getattr(procedure_service, "trusted_issuers", None)
    if not isinstance(trusted_issuers, (frozenset, set, tuple, list)) or not trusted_issuers:
        raise LocalProcedureAdapterError(
            "the local procedure adapter needs the accepted service's trusted issuers"
        )
    issuer_set = frozenset(
        issuer for issuer in trusted_issuers if isinstance(issuer, str) and issuer
    )
    if not issuer_set:
        raise LocalProcedureAdapterError(
            "the accepted trusted-procedure service has no usable trusted issuer"
        )
    if not isinstance(receiver, Mapping):
        raise LocalProcedureAdapterError(
            "the local procedure adapter needs one explicit recipient scope"
        )
    try:
        receiver_record = contracts.normalize_experience_scope(receiver)
    except contracts.ContractError as exc:
        raise LocalProcedureAdapterError(
            "the local procedure recipient scope is not exact"
        ) from exc
    if not isinstance(facts, Mapping):
        raise LocalProcedureAdapterError("trusted predicate facts must be an object")
    if route not in contracts.ROUTES:
        raise LocalProcedureAdapterError(f"unknown route: {route!r}")
    if limits is not None and not isinstance(limits, config.PreparationLimits):
        raise LocalProcedureAdapterError(
            "preparation limits must be a PreparationLimits value"
        )
    resolved_limits = limits or config.PreparationLimits()
    predicate_facts = dict(facts)
    identity = templates.representation_identity(limits=resolved_limits)
    return (
        search.SearchStore(
            store_id=CURATED_STORE_ID,
            kind=_PROCEDURE_KIND,
            query=lambda payload: _local_candidates(
                memory_store,
                issuer_set,
                receiver_record,
                predicate_facts,
                route,
                identity,
                payload,
                store_id=CURATED_STORE_ID,
                origins=_CURATED_ORIGINS,
            ),
            scope=receiver_record,
            freshness="live",
            specificity="local",
            requires_network=False,
            source_kind="curated_local_procedure",
        ),
        search.SearchStore(
            store_id=GENERATED_STORE_ID,
            kind=_PROCEDURE_KIND,
            query=lambda payload: _local_candidates(
                memory_store,
                issuer_set,
                receiver_record,
                predicate_facts,
                route,
                identity,
                payload,
                store_id=GENERATED_STORE_ID,
                origins=frozenset({"generated"}),
            ),
            scope=receiver_record,
            freshness="live",
            specificity="local",
            requires_network=False,
            source_kind="generated_local_procedure",
        ),
    )


# --- the bounded query -----------------------------------------------------

def _tokens(value: str) -> tuple[str, ...]:
    return tuple(sorted(set(re.findall(r"[a-z0-9_]+", value.casefold()))))


def _objective(payload: Any) -> dict[str, Any] | None:
    """Read only the already sanitized bounded objective of one attempt."""

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
    tokens = templates.bounded_token_projection(payload.get("tokens", []))
    if not tokens:
        return None
    return {**identity, "route": route, "tokens": tokens}


def _local_candidates(
    memory_store: Any,
    trusted_issuers: frozenset[str],
    receiver: Mapping[str, str],
    facts: Mapping[str, Any],
    route: str,
    identity: Mapping[str, Any],
    payload: Any,
    *,
    store_id: str,
    origins: frozenset[str],
) -> list[dict[str, Any]]:
    """Resolve the eligible local procedures of one origin family."""

    objective = _objective(payload)
    if objective is None:
        return []
    if any(
        objective.get(key) != identity.get(key)
        for key in _IDENTITY_KEYS
    ):
        # The store's central identity and the bounded query disagree: no
        # exact comparison can be established, so no candidate is emitted.
        return []
    partitions = _recipient_partitions(receiver)
    if not partitions:
        return []
    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for partition in partitions:
        try:
            entries = memory_store.read_local_current_procedures(
                partition,
                receiver=receiver if partition["scope"] != "private" else None,
            )
        except Exception:
            # One unreadable partition is isolated: it can never suppress an
            # independently eligible procedure of another partition.
            continue
        for entry in entries or ():
            candidate = _local_candidate(
                entry,
                trusted_issuers=trusted_issuers,
                receiver=receiver,
                facts=facts,
                route=route,
                identity=identity,
                objective=objective,
                store_id=store_id,
                origins=origins,
            )
            if candidate is None:
                continue
            key = (candidate["logical_id"], candidate["revision_id"])
            if key in seen:
                continue
            seen.add(key)
            candidates.append(candidate)
    return candidates


def _recipient_partitions(receiver: Mapping[str, str]) -> tuple[dict[str, Any], ...]:
    """Return the exact candidate partitions that can name this recipient.

    A local delivery is authorized by the designation partition itself, so the
    lookup asks for the partitions a local designation can use to name this
    exact recipient scope: the private partition this recipient owns, and the
    project lookup whose receiver-scoped discovery also returns any durable
    project/shared partition that names this exact recipient -- including a
    partition that also names other recipients, whose stored identity differs
    from the constructed lookup key.  Both returned partitions are normalized
    by the contract, so an unknown or inconsistent shape fails closed here.
    """

    partitions: list[dict[str, Any]] = []
    candidates = (
        {
            "scope": "private",
            "application": receiver["application"],
            "project": receiver["project"],
            "namespace": receiver["namespace"],
            "owner": receiver["owner"],
            "recipients": [dict(receiver)],
        },
        {
            "scope": "project",
            "application": receiver["application"],
            "project": receiver["project"],
            "namespace": receiver["namespace"],
            "recipients": [dict(receiver)],
        },
    )
    for candidate in candidates:
        try:
            partitions.append(contracts.normalize_procedure_partition(candidate))
        except contracts.ContractError:
            continue
    return tuple(partitions)


def _local_candidate(
    entry: Any,
    *,
    trusted_issuers: frozenset[str],
    receiver: Mapping[str, str],
    facts: Mapping[str, Any],
    route: str,
    identity: Mapping[str, Any],
    objective: Mapping[str, Any],
    store_id: str,
    origins: frozenset[str],
) -> dict[str, Any] | None:
    """Convert one durable local entry into one integrity-bound candidate.

    Every check fails closed: a defective, stale, withdrawn, revoked, altered,
    cross-scope, predicate-incompatible, or incomparable local record is
    omitted instead of being delivered on partial trust.
    """

    if not isinstance(entry, Mapping):
        return None
    if entry.get("defects"):
        return None
    if entry.get("state") != "active":
        return None
    if entry.get("revoked") is True:
        return None
    procedure = entry.get("procedure")
    approval = entry.get("approval")
    designation = entry.get("designation")
    partition = entry.get("partition")
    for value in (procedure, approval, designation, partition):
        if not isinstance(value, Mapping):
            return None
    if procedure.get("origin") not in origins:
        return None
    if procedure.get("logical_id") != entry.get("logical_id"):
        return None
    if procedure.get("revision_id") != entry.get("revision_id"):
        return None
    try:
        contracts.validate_procedure_revision(procedure)
        contracts.validate_procedure_approval(approval, procedure=procedure)
        contracts.validate_procedure_designation(
            designation, procedure=procedure, approval=approval
        )
        normalized_partition = contracts.normalize_procedure_partition(partition)
    except contracts.ContractError:
        return None
    if designation.get("logical_id") != procedure["logical_id"]:
        return None
    if designation.get("revision_id") != procedure["revision_id"]:
        return None
    if designation.get("partition") != normalized_partition:
        return None
    if approval.get("issuer") not in trusted_issuers:
        # A raw durable approval is never delivery approval by itself.
        return None
    if designation.get("issuer") not in trusted_issuers:
        # The designation is the second authorization an exact current
        # delivery stands on: an issuer outside the accepted trusted set can
        # never make one durable record current for a recipient.
        return None
    if not contracts.partition_is_authorized(approval, normalized_partition):
        return None
    recipient_keys = {
        contracts.canonical_json(item).decode("utf-8")
        for item in normalized_partition["recipients"]
    }
    if contracts.canonical_json(dict(receiver)).decode("utf-8") not in recipient_keys:
        return None
    if not contracts.procedure_predicates_match(
        procedure["behavior"]["predicates"], facts, route=route
    ):
        return None
    representation = _comparable_representation(
        entry.get("representations"), identity=identity
    )
    if representation is None:
        return None
    candidate_representation = dict(representation)
    candidate_representation["route"] = route
    candidate_representation["declared"] = True
    if not templates.representations_comparable(objective, candidate_representation):
        return None
    score = templates.score_representations(objective, candidate_representation)
    source = procedure.get("source")
    provenance = {
        "authority": _LOCAL_AUTHORITY,
        "store_id": store_id,
        "logical_id": procedure["logical_id"],
        "revision_id": procedure["revision_id"],
        "partition_id": normalized_partition and contracts.procedure_partition_id(
            normalized_partition
        ),
        "partition_scope": normalized_partition["scope"],
        "designation_id": designation["designation_id"],
        "designation_generation": designation["generation"],
        "representation_id": representation.get("representation_id"),
        "receiver": dict(receiver),
        "origin": procedure["origin"],
    }
    body = {
        "kind": _PROCEDURE_KIND,
        "authority": _LOCAL_AUTHORITY,
        "logical_id": procedure["logical_id"],
        "revision_id": procedure["revision_id"],
        "procedure": dict(procedure),
        "approval": dict(approval),
        "designation": dict(designation),
        "partition": dict(normalized_partition),
        "recipient": dict(receiver),
        "source": dict(source) if isinstance(source, Mapping) else None,
        "source_cases": [dict(item) for item in entry.get("source_cases", ())],
        # Proven here for this exact delivery: the durable approval binds the
        # revision and is issued by a configured trusted issuer, the durable
        # designation is the exact current one, and its predicates hold for
        # these facts and this route.
        "predicates_ok": True,
        "freshness": "live",
        "provenance": provenance,
    }
    return {
        "kind": _PROCEDURE_KIND,
        "logical_id": procedure["logical_id"],
        "revision_id": procedure["revision_id"],
        "origin": store_id,
        "source_id": store_id,
        "payload": body,
        "payload_digest": contracts.sha256_hex(body),
        "scope": dict(receiver),
        "representation": candidate_representation,
        "score": score,
        "specificity": "local",
        "freshness": "live",
        "approval_status": "approved",
        "designation": "current",
        "predicates_ok": True,
    }


def _comparable_representation(
    representations: Any, *, identity: Mapping[str, Any]
) -> dict[str, Any] | None:
    """Return one declared projection with the store's exact identity."""

    if not isinstance(representations, Iterable):
        return None
    for representation in representations:
        if not isinstance(representation, Mapping):
            continue
        if any(representation.get(key) != identity.get(key) for key in _IDENTITY_KEYS):
            # A projection another representation identity recorded cannot be
            # compared with this objective and is never delivered.
            continue
        search_text = representation.get("search_text")
        if not isinstance(search_text, str) or not search_text.strip():
            continue
        tokens = _tokens(search_text)
        if not tokens:
            continue
        selected = {key: representation[key] for key in _IDENTITY_KEYS}
        selected["representation_id"] = representation.get("representation_id")
        selected["tokens"] = list(tokens)
        return selected
    return None


__all__ = [
    "CURATED_STORE_ID",
    "GENERATED_STORE_ID",
    "LocalProcedureAdapterError",
    "make_local_procedure_search_stores",
]

