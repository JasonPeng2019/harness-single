"""Bounded optional-memory search over the accepted store adapters.

One logical preparation owns one absolute deadline.  Every enabled store gets
a bounded, independent chance to start inside its own sub-budget; a slow,
lazily blocking, or malformed store is isolated and valid completed results
from another store are preserved.  The same deadline covers the complete
consumption of a store's result, normalization, trust gating, scoring, final
deduplication, ranking, and capacity selection, so late values never enter the
packet and a blocked phase returns control at its deadline.  Candidates are normalized and gated
*before* ranking, one logical revision is delivered once no matter how many
stores saw it, and one logical procedure delivers at most one revision: the
narrower authorized scope wins before the discovery score, with the revision
id as the deterministic tie.  Duplicate sightings of one revision merge
their provenance and any ineligible attribution on the narrower eligible
sighting without lending it a score, so no sighting can be boosted by
another.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Sequence

from . import contracts, templates
from .config import PreparationLimits
from .privacy import PrivacyPolicy, safe_query_payload


class SearchError(RuntimeError):
    """A bounded search invariant was violated."""


class UnknownKindError(SearchError):
    """A store declared a candidate kind the coordinator does not own."""


# The finite set of optional-store source markers the preparation gate
# understands.  Only the two local procedure sources change the accepted
# generic gate; every other value keeps the accepted behavior, and ``caller``
# is the default so existing caller-supplied stores are unaffected.
STORE_SOURCE_KINDS = frozenset(
    {
        "caller",
        "curated_local_procedure",
        "generated_local_procedure",
        "everos_generated_skill",
    }
)
_LOCAL_PROCEDURE_SOURCE_KINDS = frozenset(
    {"curated_local_procedure", "generated_local_procedure"}
)
# The one future shared/remote procedure source: an EverOS generated-skill
# store performs a real remote SearchStore call, so the marker is only honest
# together with ``requires_network=True``.
_REMOTE_PROCEDURE_SOURCE_KINDS = frozenset({"everos_generated_skill"})


@dataclass(frozen=True)
class SearchStore:
    """One bounded, enabled optional store adapter.

    ``requires_network`` is the smallest explicit source marker the
    preparation gate needs: it declares that the store's query performs a
    shared/remote task-path call (the Atlas procedure service), so disabling
    its shared-retrieval feature or selecting ``restricted_local`` must
    suppress the actual call before it begins instead of filtering its output
    afterwards.  Existing caller-supplied stores keep their exact behavior:
    the field defaults to a local store.

    ``source_kind`` is the one additive, finite source marker a purely local
    procedure store needs: the accepted generic ``procedure`` gate cannot tell
    a local curated/builtin store, a local generated-origin store, and a
    shared/remote store apart.  Its documented values are exactly ``caller``
    (the default: every existing caller keeps the accepted generic behavior),
    ``curated_local_procedure`` (a local curated/builtin store that performs no
    shared/remote call, so it stays eligible when shared retrieval and
    generated-skill use are off), and ``generated_local_procedure`` (a local
    generated-origin store that makes no query at all while
    ``generated_skill_use`` is off), and ``everos_generated_skill`` (the future
    EverOS generated-skill procedure store whose query performs a real remote
    call, admitted only while ``generated_skill_use`` is true outside
    ``restricted_local``).  A local procedure source never declares
    ``requires_network``: ``restricted_local`` suppresses only the remote call.
    ``everos_generated_skill`` is the opposite case: the remote call is
    intrinsic to the source, so the marker is rejected unless it also declares
    ``requires_network=True``.

    ``final_proof`` is an optional, separately invoked source-owner readback.
    It receives the selected store/source/logical/revision/content-digest/route
    identity and may return an exact ``frozen_contract`` and/or the distinct
    ``compact_representation`` and ``compact_approval`` records. It must read
    owner state rather than echo candidate fields. Preparation invokes it
    inside the same final cutoff as a live recheck.
    """

    store_id: str
    kind: str
    query: Callable[[Mapping[str, Any]], Iterable[Mapping[str, Any]]]
    scope: Mapping[str, Any] | None = None
    freshness: str = "live"
    specificity: str = "project"
    requires_network: bool = False
    source_kind: str = "caller"
    # A distinct readback from this selected source owner. The finalizer may
    # use it for an immutable frozen contract or a separately approved compact
    # procedure; candidate fields and store freshness never supply that proof.
    final_proof: Callable[[Mapping[str, Any]], Mapping[str, Any] | None] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.store_id, str) or not self.store_id.strip():
            raise SearchError("store_id must be a nonempty string")
        if self.kind not in contracts.CANDIDATE_KINDS:
            raise UnknownKindError(f"unknown candidate kind: {self.kind!r}")
        if not callable(self.query):
            raise SearchError("store query must be callable")
        if self.final_proof is not None and not callable(self.final_proof):
            raise SearchError("store final_proof must be callable")
        if not isinstance(self.requires_network, bool):
            raise SearchError("store requires_network must be boolean")
        if self.source_kind not in STORE_SOURCE_KINDS:
            raise SearchError(f"unknown store source_kind: {self.source_kind!r}")
        if self.source_kind in _LOCAL_PROCEDURE_SOURCE_KINDS and self.requires_network:
            raise SearchError(
                "a local procedure source never performs a shared/remote call"
            )
        if self.source_kind in _REMOTE_PROCEDURE_SOURCE_KINDS and not self.requires_network:
            raise SearchError(
                "an EverOS generated-skill source always performs a shared/remote "
                "call and never declares a local/non-network call"
            )


@dataclass(frozen=True)
class SearchResult:
    outcome: str
    candidates: list[dict[str, Any]]
    attempts: list[dict[str, Any]]
    rounds: int
    delivered: list[str]
    reason: str | None = None

    @property
    def delivered_candidates(self) -> list[dict[str, Any]]:
        return [item for item in self.candidates if item["disposition"] == "selected"]


_SPECIFICITY_ORDER = {"private": 0, "local": 1, "project": 2, "shared": 3}
_KIND_ORDER = {"procedure": 0, "historical_evidence": 1, "template": 2}


def _sighting_rank(sighting: Mapping[str, Any]) -> tuple[Any, ...]:
    """Order duplicate sightings of one revision deterministically.

    The narrower authorized scope wins before the discovery score, and the
    source identity (then origin) is the deterministic tie, so the same set
    of sightings always elects the same representative no matter in which
    store or order they were observed.
    """

    return (
        _SPECIFICITY_ORDER.get(str(sighting.get("specificity")), 9),
        -float(sighting.get("score", 0.0)),
        str(sighting.get("source_id", "")),
        str(sighting.get("origin", "")),
    )


class BoundedSearch:
    """Run one bounded, fair, gated, and deduplicated optional search."""

    def __init__(
        self,
        *,
        limits: PreparationLimits | None = None,
        privacy_policy: PrivacyPolicy | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.limits = limits or PreparationLimits()
        self.privacy_policy = privacy_policy or PrivacyPolicy()
        self.clock = clock or time.monotonic

    def run(
        self,
        *,
        objective: Mapping[str, Any],
        stores: Sequence[SearchStore],
        route: str,
        stage_seconds: float,
        rounds: int,
        policy_context: Mapping[str, str] | None = None,
    ) -> SearchResult:
        if not isinstance(objective, Mapping):
            raise SearchError("objective representation must be an object")
        if route not in contracts.ROUTES:
            raise SearchError(f"unknown route: {route!r}")
        if stage_seconds < 0:
            raise SearchError("stage allowance must not be negative")
        if policy_context is not None:
            if not isinstance(policy_context, Mapping) or set(policy_context) != {
                "schema", "preparation_id", "preparation_digest",
            }:
                raise SearchError("policy context must have exactly the three opaque fields")
            if (policy_context["schema"] != "memory-search-policy-context/v1"
                    or not isinstance(policy_context["preparation_id"], str)
                    or re.fullmatch(r"[0-9a-f]{64}", policy_context["preparation_id"]) is None
                    or not isinstance(policy_context["preparation_digest"], str)
                    or re.fullmatch(r"[0-9a-f]{64}", policy_context["preparation_digest"]) is None):
                raise SearchError("policy context identity or digest is invalid")
            policy_context = dict(policy_context)
        enabled_kinds = {store.kind for store in stores}
        capacity = {
            kind: self.limits.capacity_for(kind) for kind in enabled_kinds
        }
        used = {kind: 0 for kind in enabled_kinds}
        attempts: list[dict[str, Any]] = []
        collected: list[dict[str, Any]] = []
        completed_rounds = 0
        # One real monotonic deadline covers the whole stage: every store call,
        # the complete consumption of its result, normalization, trust gating,
        # scoring, deduplication, and ranking must finish inside it, or their
        # late values never enter this packet.  Each enabled store is scheduled
        # a bounded slice of the *remaining real* time, so an early store
        # returns its unused slice to the stores behind it and one slow store
        # can never remove another store's own chance to start.  The injected
        # logical clock is never consulted for phase allowance, so a frozen
        # logical clock cannot hand a phase a fresh slice of real time.
        stage_deadline = time.monotonic() + float(stage_seconds)
        for round_index in range(max(1, int(rounds))):
            if stage_deadline - time.monotonic() < self.limits.minimum_optional_slice_seconds:
                break
            completed_rounds += 1
            for store in stores:
                # One real absolute slice deadline per attempt, itself bounded
                # by the enclosing stage: the store call, the complete
                # consumption of its result, and the normalization, trust
                # gating, and scoring of every candidate share it.  The
                # injected logical clock may describe budget but never
                # replenishes real phase time.
                slice_deadline = min(
                    stage_deadline, time.monotonic() + self.limits.store_seconds
                )
                entry = self._attempt(
                    store,
                    objective=objective,
                    route=route,
                    policy_context=policy_context,
                    slice_deadline=slice_deadline,
                    attempt_limit=self._attempt_limit(capacity[store.kind]),
                )
                if entry["status"] == "completed":
                    used[store.kind] += entry.get("accepted", 0)
                if entry["status"] == "completed":
                    accepted = entry.pop("candidates")
                    for raw in accepted:
                        collected.append(raw)
                    entry["candidates"] = len(accepted)
                attempts.append(entry)
        # Deduplication, ranking, capacity selection, and result publication
        # belong to the same absolute stage bound: a blocking finalization
        # returns control at the deadline and publishes nothing late, and the
        # abandoned worker only ever writes to its own discarded copy.
        finalization_reason: str | None = None
        if not collected:
            finalized, delivered_ids = self._finalize(
                [],
                objective=objective,
                route=route,
                capacity=capacity,
                enabled_kinds=enabled_kinds,
            )
        else:
            remaining = stage_deadline - time.monotonic()
            if remaining < self.limits.minimum_optional_slice_seconds:
                finalized, delivered_ids = [], []
                finalization_reason = (
                    "the stage allowance expired before finalization; no candidates were published"
                )
            else:
                try:
                    published, bounded = self._call_bounded(
                        lambda: self._finalize(
                            collected,
                            objective=objective,
                            route=route,
                            capacity=capacity,
                            enabled_kinds=enabled_kinds,
                        ),
                        deadline=stage_deadline,
                        label="finalization",
                    )
                except Exception as exc:  # isolated finalization failure
                    published, bounded = None, "failed"
                    finalization_reason = f"finalization failed closed: {type(exc).__name__}"
                if bounded == "timed-out":
                    finalized, delivered_ids = [], []
                    finalization_reason = (
                        "the stage allowance expired during deduplication and ranking; "
                        "no candidates were published"
                    )
                elif published is None:
                    finalized, delivered_ids = [], []
                else:
                    finalized, delivered_ids = published
        outcome = (
            "optional_memory"
            if any(item["disposition"] == "selected" for item in finalized)
            else "no_optional_memory"
        )
        return SearchResult(
            outcome=outcome,
            candidates=finalized,
            attempts=attempts,
            rounds=completed_rounds,
            delivered=delivered_ids,
            reason=finalization_reason,
        )

    def _finalize(
        self,
        collected: list[dict[str, Any]],
        *,
        objective: Mapping[str, Any],
        route: str,
        capacity: Mapping[str, int],
        enabled_kinds: Any,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """Deduplicate, rank, select by capacity, and publish one packet.

        Every returned record is freshly built, so an abandoned worker that
        times out mid-finalization can never mutate what was published.
        """

        ranked = self._normalize_and_rank(collected, objective=objective, route=route)
        delivered: list[dict[str, Any]] = []
        delivered_ids: list[str] = []
        per_kind_count: dict[str, int] = {kind: 0 for kind in enabled_kinds}
        finalized: list[dict[str, Any]] = []
        for candidate in ranked:
            kind = candidate["kind"]
            if candidate["disposition"] != "eligible":
                finalized.append(candidate)
                continue
            if per_kind_count.get(kind, 0) >= capacity.get(kind, 0):
                candidate = self._dispositioned(
                    candidate, "rejected", "capacity for this candidate kind is exhausted"
                )
                finalized.append(candidate)
                continue
            per_kind_count[kind] = per_kind_count.get(kind, 0) + 1
            selected = self._dispositioned(candidate, "selected", "selected within its kind capacity")
            delivered.append(selected)
            delivered_ids.append(selected["candidate_id"])
            finalized.append(selected)
        for selected in delivered:
            for index, candidate in enumerate(finalized):
                if candidate["candidate_id"] == selected["candidate_id"]:
                    finalized[index] = self._dispositioned(
                        candidate, "selected", "selected within its kind capacity", delivered=True
                    )
        return finalized, delivered_ids

    # -- attempt scheduling -------------------------------------------------

    def _call_bounded(
        self, work: Callable[[], Any], *, deadline: float, label: str = "work"
    ) -> tuple[Any, str | None]:
        """Run one bounded unit of stage work on a daemon worker thread.

        ``deadline`` is one absolute real monotonic instant shared with the
        enclosing stage, so neither a store call, the complete consumption of
        its lazy result, its normalization, trust gating and scoring, nor final
        deduplication and ranking may hold the caller past it.  Work that is
        still running at the deadline and work that finishes after it are both
        reported as timed-out, so late values can never enter this packet;
        every abandoned worker is a daemon thread that cannot keep the process
        alive.
        """

        import threading

        box: dict[str, Any] = {}

        def target() -> None:
            try:
                box["value"] = work()
            except BaseException as exc:  # isolated failure
                box["error"] = exc
            finally:
                box["finished"] = time.monotonic()

        thread = threading.Thread(target=target, name=f"memory-store:{label}", daemon=True)
        thread.start()
        thread.join(max(0.0, float(deadline) - time.monotonic()))
        finished = box.get("finished")
        if thread.is_alive() or finished is None or float(finished) > float(deadline):
            return None, "timed-out"
        if "error" in box:
            raise box["error"]
        return box.get("value"), None

    @staticmethod
    def _materialize(
        query: Callable[[Mapping[str, Any]], Any], payload: Mapping[str, Any]
    ) -> list[Any]:
        """Call one store and consume its complete result inside the worker."""

        produced = query(payload)
        return [] if produced is None else list(produced)

    def _attempt(
        self,
        store: SearchStore,
        *,
        objective: Mapping[str, Any],
        route: str,
        policy_context: Mapping[str, str] | None,
        slice_deadline: float,
        attempt_limit: int,
    ) -> dict[str, Any]:
        entry: dict[str, Any] = {
            "store_id": store.store_id,
            "kind": store.kind,
            "status": "unattempted-by-budget",
            "candidates": 0,
            "accepted": 0,
            "rejected": [],
            "reason": None,
        }
        if slice_deadline - time.monotonic() < self.limits.minimum_optional_slice_seconds:
            entry["reason"] = "no remaining time inside the enclosing stage bound"
            return entry
        query_fields = {
            "representation": {key: objective[key] for key in ("model", "dimensions", "metric", "sanitizer_version")},
            "tokens": templates.bounded_token_projection(objective.get("tokens", [])),
            "route": route,
        }
        if policy_context is not None:
            query_fields["policy_context"] = dict(policy_context)
        query = safe_query_payload(query_fields, self.privacy_policy)
        entry["status"] = "attempted"
        started = self.clock()
        # The injected logical clock still owns logical budget semantics, but
        # every real phase allowance is derived from the one absolute slice
        # deadline, so a frozen logical clock cannot replenish real time.
        logical_budget = slice_deadline - time.monotonic()
        try:
            # The store call and the complete consumption of its result both
            # run inside the bounded worker, never on the caller thread, and
            # they may spend no more than the one real slice deadline they
            # share with the normalization phase behind them.
            produced, bounded = self._call_bounded(
                lambda: self._materialize(store.query, query),
                deadline=slice_deadline,
                label=store.store_id,
            )
        except Exception as exc:  # isolated store failure
            entry["status"] = "invalid" if isinstance(exc, (TypeError, ValueError)) else "unavailable"
            entry["reason"] = f"{type(exc).__name__}"
            entry["elapsed_seconds"] = self.clock() - started
            return entry
        elapsed = self.clock() - started
        entry["elapsed_seconds"] = elapsed
        if bounded == "timed-out" or elapsed > logical_budget:
            # Late work stays attributable but can never enter this packet.
            entry["status"] = "timed-out"
            entry["reason"] = "store exceeded its bounded sub-budget"
            return entry
        raw_items = produced or []
        remaining = slice_deadline - time.monotonic()
        if remaining < self.limits.minimum_optional_slice_seconds:
            return self._late_attempt(
                entry,
                [],
                started,
                reason=(
                    "no slice of the stage allowance remained to normalize the store "
                    "results; late values were dropped"
                ),
            )
        try:
            # Normalization, trust gating, and scoring of every candidate of
            # this store run on the bounded worker too, so a blocking
            # candidate can never hold the caller past the deadline.
            normalized, bounded = self._call_bounded(
                lambda: self._normalize_items(
                    store,
                    raw_items,
                    objective=objective,
                    route=route,
                    attempt_limit=attempt_limit,
                    started=started,
                    logical_budget=logical_budget,
                    slice_deadline=slice_deadline,
                ),
                deadline=slice_deadline,
                label=f"{store.store_id}:normalize",
            )
        except Exception as exc:  # isolated store failure
            entry["status"] = "invalid" if isinstance(exc, (TypeError, ValueError)) else "unavailable"
            entry["reason"] = f"{type(exc).__name__}"
            entry["elapsed_seconds"] = self.clock() - started
            return entry
        if bounded == "timed-out":
            return self._late_attempt(
                entry,
                [],
                started,
                reason=(
                    "normalization exceeded the assigned slice; late values were dropped"
                ),
            )
        accepted, rejected, late = normalized
        if late:
            return self._late_attempt(entry, rejected, started)
        entry["status"] = "completed"
        entry["accepted"] = len(accepted)
        entry["candidates"] = accepted
        entry["rejected"] = rejected
        return entry

    def _normalize_items(
        self,
        store: SearchStore,
        raw_items: Sequence[Any],
        *,
        objective: Mapping[str, Any],
        route: str,
        attempt_limit: int,
        started: float,
        logical_budget: float,
        slice_deadline: float,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool]:
        """Normalize, gate, and score one store's candidates inside its slice.

        This runs on the bounded worker thread.  The returned collections are
        freshly built locals, so an abandoned worker can never mutate the
        packet that was already published.  ``late`` reports that the work
        crossed the one real absolute slice deadline (or the injected logical
        budget), which makes the whole attempt late.
        """

        accepted: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []

        def expired() -> bool:
            return (
                self.clock() - started > logical_budget
                or time.monotonic() > slice_deadline
            )

        for raw in raw_items:
            if expired():
                return accepted, rejected, True
            try:
                record, rejection = self._normalize(store, raw, objective=objective, route=route)
            except Exception as exc:  # one malformed item never breaks the store
                record, rejection = None, f"{type(exc).__name__}: {exc}"
            if expired():
                # Trust gating or scoring ran past the assigned slice: this
                # value and everything behind it are late.
                return accepted, rejected, True
            if record is None:
                logical_id = raw.get("logical_id") if isinstance(raw, Mapping) else None
                rejected.append(
                    {
                        "logical_id": logical_id if isinstance(logical_id, str) else None,
                        "revision_id": (
                            raw.get("revision_id")
                            if isinstance(raw, Mapping) and isinstance(raw.get("revision_id"), str)
                            else None
                        ),
                        "reason": rejection or "candidate is not usable",
                    }
                )
                continue
            if record["disposition"] != "eligible":
                rejected.append(
                    {
                        "logical_id": record["logical_id"],
                        "revision_id": record["revision_id"],
                        "reason": "; ".join(record["reasons"]) or "candidate is not eligible",
                    }
                )
            accepted.append(record)
            if len(accepted) >= attempt_limit:
                break
        return accepted, rejected, False

    def _late_attempt(
        self,
        entry: dict[str, Any],
        rejected: list[dict[str, Any]],
        started: float,
        *,
        reason: str | None = None,
    ) -> dict[str, Any]:
        """Close one attempt whose work ran past its assigned slice.

        The late values stay attributable in the trace (status, reason, and the
        rejections recorded so far) but can never contribute to the packet.
        """
        entry["status"] = "timed-out"
        entry["reason"] = (
            reason or "normalization exceeded the assigned slice; late values were dropped"
        )
        entry["elapsed_seconds"] = self.clock() - started
        entry["candidates"] = 0
        entry["accepted"] = 0
        entry["rejected"] = rejected
        return entry

    @staticmethod
    def _attempt_limit(capacity: int) -> int:
        # A store may report more candidates than its delivery capacity so a
        # lower-ranked eligible candidate stays visible; the returned set is
        # still bounded, and only the selection stage applies capacity.
        return max(2, int(capacity) * 4 + 4)

    # -- normalization and gating ------------------------------------------

    def _normalize(
        self,
        store: SearchStore,
        raw: Any,
        *,
        objective: Mapping[str, Any],
        route: str,
    ) -> tuple[dict[str, Any] | None, str | None]:
        """Return a normalized candidate or an explicit rejection reason.

        Mandatory trust gates fail closed *before* ranking.  A well-formed but
        untrusted candidate keeps its exact identity and reason so it stays
        visible in the trace without ever being delivered.
        """

        if not isinstance(raw, Mapping):
            return None, "candidate is not an object"
        required = ("kind", "logical_id", "revision_id", "payload", "scope", "representation")
        missing = [key for key in required if key not in raw]
        if missing:
            return None, "candidate is missing required fields: " + ", ".join(missing)
        if raw["kind"] != store.kind:
            return None, "candidate kind does not match its store"
        logical_id = raw["logical_id"]
        revision_id = raw["revision_id"]
        if not isinstance(logical_id, str) or not logical_id:
            return None, "candidate logical_id must be a nonempty string"
        if not isinstance(revision_id, str) or not revision_id:
            return None, "candidate revision_id must be a nonempty string"
        try:
            payload = dict(raw["payload"])
        except (TypeError, ValueError):
            return None, "candidate payload is not an object"
        scope = raw["scope"]
        if not isinstance(scope, Mapping):
            return None, "candidate scope is not an object"
        representation = raw["representation"]
        if not isinstance(representation, Mapping):
            return None, "candidate representation is not an object"

        reasons: list[str] = []
        invalid = False
        if store.scope is not None and dict(scope) != dict(store.scope):
            reasons.append("candidate scope is outside the authorized recipient boundary")
        if raw.get("revoked") is True:
            reasons.append("candidate revision is revoked")
        if raw.get("withdrawn") is True:
            reasons.append("candidate revision is withdrawn")
        approval = raw.get("approval_status")
        designation = raw.get("designation")
        if store.kind == "procedure":
            if approval != "approved":
                reasons.append("procedure approval is not current and approved")
            if designation != "current":
                reasons.append("procedure designation is not current")
            if raw.get("predicates_ok") is not True:
                reasons.append("procedure predicates are not satisfied")
        else:
            if approval is not None and approval != "approved":
                reasons.append("candidate approval is not approved")
            if designation is not None and designation != "current":
                reasons.append("candidate designation is not current")
            if raw.get("predicates_ok") is False:
                reasons.append("candidate predicates are not satisfied")
        candidate_routes = raw.get("routes")
        if candidate_routes is not None:
            if not isinstance(candidate_routes, (list, tuple, set, frozenset, str)):
                return None, "candidate routes must be a list of routes"
            if route not in tuple(candidate_routes):
                reasons.append("candidate is not applicable to this route")
        payload_digest = contracts.sha256_hex(payload)
        declared_digest = raw.get("payload_digest")
        if declared_digest is not None and declared_digest != payload_digest:
            reasons.append("candidate payload digest does not match its payload")
        if store.kind == "procedure" and declared_digest is None:
            reasons.append("procedure candidate carries no integrity digest")
        if representation.get("declared") is False:
            comparable = False
            reasons.append("incomparable representation: candidate is not declared")
        else:
            from .templates import representations_comparable

            comparable = representations_comparable(objective, representation)
            if not comparable:
                reasons.append("incomparable representation")
        raw_score = raw.get("score", 0.0)
        score = (
            float(raw_score)
            if isinstance(raw_score, (int, float)) and not isinstance(raw_score, bool)
            else 0.0
        )
        if not comparable:
            score = 0.0
        freshness = str(raw.get("freshness", store.freshness))
        if freshness not in {"live", "frozen"}:
            return None, f"candidate declares an unknown freshness: {freshness!r}"
        disposition = "eligible" if not reasons else ("invalid" if invalid else "rejected")
        return (
            {
                "kind": store.kind,
                "logical_id": logical_id,
                "revision_id": revision_id,
                "origin": str(raw.get("origin", store.store_id)),
                "source_id": str(raw.get("source_id", store.store_id)),
                "payload": payload,
                "payload_digest": payload_digest,
                "scope": dict(scope),
                "freshness": freshness,
                "representation": dict(representation),
                "comparable": comparable,
                "score": score,
                "specificity": str(raw.get("specificity", store.specificity)),
                "provenance": [
                    {
                        "store_id": store.store_id,
                        "origin": str(raw.get("origin", store.store_id)),
                        "specificity": str(raw.get("specificity", store.specificity)),
                    }
                ],
                "disposition": disposition,
                "reasons": reasons,
            },
            None,
        )

    def _normalize_and_rank(
        self,
        collected: list[dict[str, Any]],
        *,
        objective: Mapping[str, Any],
        route: str,
    ) -> list[dict[str, Any]]:
        deduplicated: dict[tuple[str, str, str], dict[str, Any]] = {}
        order: list[tuple[str, str, str]] = []
        for candidate in collected:
            key = (candidate["kind"], candidate["logical_id"], candidate["revision_id"])
            prior = deduplicated.get(key)
            if prior is None:
                deduplicated[key] = candidate
                order.append(key)
                continue
            # One logical revision keeps exactly one representative sighting.
            # Among eligible sightings the narrower authorized scope wins
            # before the discovery score, with the sighting identity as the
            # deterministic tie, so no arrival order can change the outcome.
            # An ineligible duplicate contributes only provenance and
            # attributable reasons: it never lends its score, specificity,
            # source identity, or payload to the eligible representative, and
            # it can never suppress an independently eligible sighting.
            if prior["disposition"] != "eligible" and candidate["disposition"] == "eligible":
                representative, duplicate = candidate, prior
            elif prior["disposition"] == "eligible" and candidate["disposition"] != "eligible":
                representative, duplicate = prior, candidate
            elif prior["disposition"] == "eligible":
                representative, duplicate = (
                    (candidate, prior)
                    if _sighting_rank(candidate) < _sighting_rank(prior)
                    else (prior, candidate)
                )
            else:
                # Two ineligible sightings keep the first as representative.
                representative, duplicate = prior, candidate
            merged_provenance = list(representative["provenance"])
            for entry in duplicate["provenance"]:
                if entry not in merged_provenance:
                    merged_provenance.append(entry)
            merged_reasons = list(representative.get("reasons", []))
            for reason in duplicate.get("reasons", []):
                attributed = (
                    f"duplicate sighting: {reason}"
                    if representative["disposition"] == "eligible"
                    else reason
                )
                if attributed not in merged_reasons:
                    merged_reasons.append(attributed)
            merged_record = dict(representative)
            merged_record["provenance"] = merged_provenance
            merged_record["reasons"] = merged_reasons
            deduplicated[key] = merged_record
            prior = merged_record
        ranked = [deduplicated[key] for key in order]
        # One logical procedure delivers at most one revision.  Eligible
        # competitors of the same logical identity are compared by the default
        # scope specificity order (private, local, project, shared) *before*
        # the discovery score, exactly like the accepted
        # ``TrustedProcedureService.resolve_atlas`` policy, with the revision
        # id as the deterministic tie.  Duplicate sightings of one revision
        # already merged above, so every remaining competitor is a distinct
        # revision identity; a rejected or invalid revision never competes and
        # can never suppress an independently eligible one.
        procedure_winners: dict[str, tuple[tuple[Any, ...], str]] = {}
        for item in ranked:
            if item["kind"] != "procedure" or item["disposition"] != "eligible":
                continue
            key = (
                _SPECIFICITY_ORDER.get(str(item.get("specificity")), 9),
                -float(item.get("score", 0.0)),
                item["revision_id"],
            )
            prior = procedure_winners.get(item["logical_id"])
            if prior is None or key < prior[0]:
                procedure_winners[item["logical_id"]] = (key, item["revision_id"])
        for item in ranked:
            if item["kind"] != "procedure" or item["disposition"] != "eligible":
                continue
            winner = procedure_winners.get(item["logical_id"])
            if winner is None or item["revision_id"] == winner[1]:
                continue
            item["disposition"] = "rejected"
            selection_reason = (
                "competing revision of the same logical procedure: "
                f"revision {winner[1]!r} is the deterministic winner "
                "(narrower authorized scope before the discovery score, "
                "then the revision id)"
            )
            reasons = list(item.get("reasons", []))
            if selection_reason not in reasons:
                reasons.append(selection_reason)
            item["reasons"] = reasons
        ranked.sort(
            key=lambda item: (
                item["disposition"] != "eligible",
                -item["score"],
                _KIND_ORDER.get(item["kind"], 9),
                _SPECIFICITY_ORDER.get(item["specificity"], 9),
                item["logical_id"],
                item["revision_id"],
            )
        )
        records: list[dict[str, Any]] = []
        for item in ranked:
            records.append(
                contracts.make_candidate(
                    kind=item["kind"],
                    logical_id=item["logical_id"],
                    revision_id=item["revision_id"],
                    origin=item["origin"],
                    source_id=item["source_id"],
                    payload_digest=item["payload_digest"],
                    payload=item["payload"],
                    scope=item["scope"],
                    provenance=item["provenance"],
                    freshness=item["freshness"],
                    representation=item["representation"],
                    score=item["score"],
                    comparable=item["comparable"],
                    disposition=item["disposition"],
                    reasons=item["reasons"],
                )
            )
        return records

    @staticmethod
    def _dispositioned(
        candidate: Mapping[str, Any],
        disposition: str,
        reason: str,
        *,
        delivered: bool = False,
    ) -> dict[str, Any]:
        revised = dict(candidate)
        revised["disposition"] = disposition
        reasons = list(revised.get("reasons", []))
        if reason not in reasons:
            reasons.append(reason)
        revised["reasons"] = reasons
        revised["content_hash"] = contracts.content_hash(revised)
        return revised

