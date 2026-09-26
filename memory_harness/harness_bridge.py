"""Narrow adapter over the existing product harness for one APC child.

The product harness remains the only launcher, corrector, and cleanup owner.
This adapter wraps that lifecycle for a single bounded drafting child: it
persists launch intent before the call, records the actual observed invocation,
reconciles an ambiguous acknowledgement by exact identity before any retry, and
never falls back to a raw provider or a second planner.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from . import apc, contracts
from .config import PreparationLimits


class HarnessBridgeError(RuntimeError):
    """The bounded APC child could not be run safely."""


class ApcUnavailableError(HarnessBridgeError):
    """Light adaptation is unavailable for this exact run."""


class ApcChildUnavailableError(HarnessBridgeError):
    """The product harness proved no child was launched for this attempt.

    The refusal is terminal: the queue or launch failed before any child
    existed, so a later attempt is not blocked by a live owner and the
    recorded operation never claims an unresolved live child.
    """


class ApcChildAmbiguityError(HarnessBridgeError):
    """The child acknowledgement is ambiguous and must be reconciled exactly."""


class ApcChildTimeoutError(HarnessBridgeError):
    """The child exceeded the enclosing absolute deadline."""


class ApcChildRejectedError(HarnessBridgeError):
    """The child result violated its bounded contract."""


# The one enclosing absolute deadline is handed to a launcher only through
# this exact opt-in channel: a launcher that can bound its own native phases
# declares the attribute below on its callable, receives the effective cutoff
# under the key below, and must remove the key before it validates, hashes, or
# embeds the request, so the request record stays exact for every other
# consumer.  Every launcher that does not opt in keeps the exact request.
APC_ENCLOSING_DEADLINE_ATTRIBUTE = "apc_child_receives_enclosing_deadline"
APC_ENCLOSING_DEADLINE_KEY = "enclosing_deadline_monotonic"


@dataclass(frozen=True)
class ApcAttempt:
    """The recorded child operation and, when valid, its proposed artifact."""

    child_operation: dict[str, Any]
    proposal: dict[str, Any] | None
    reason: str

    @property
    def proposed(self) -> bool:
        return self.proposal is not None


def validate_adapted_proposal(
    template_record: Mapping[str, Any],
    proposal: Mapping[str, Any],
    *,
    permitted_edits: tuple[str, ...] | list[str] | tuple[Any, ...],
    limits: PreparationLimits | None = None,
    objective_id: str,
) -> None:
    """Reject structure, invariant, edit, or authority violations."""

    resolved = limits or PreparationLimits()
    try:
        contracts.validate_plan(proposal, expected_state="proposed")
    except contracts.ContractError as exc:
        raise ApcChildRejectedError(f"adapted proposal is not a proposed plan: {exc}") from exc
    if proposal["objective_id"] != objective_id:
        raise ApcChildRejectedError("adapted proposal objective does not match the parent")
    content = proposal["content"]
    if not isinstance(content, Mapping):
        raise ApcChildRejectedError("adapted proposal content must be an object")
    if list(content.get("fixed_steps", [])) != list(template_record.get("fixed_steps", [])):
        raise ApcChildRejectedError("adapted proposal changed the fixed structure")
    if content.get("verification_intent") != template_record.get("verification_intent"):
        raise ApcChildRejectedError("adapted proposal changed the verification intent")
    allowed = set(permitted_edits)
    for surface in content:
        if surface in {"fixed_steps", "verification_intent"}:
            continue
        if surface not in allowed:
            raise ApcChildRejectedError(f"adapted proposal edited an unauthorized surface: {surface}")
    bindings = content.get("bindings")
    if bindings is not None:
        if "bindings" not in allowed:
            raise ApcChildRejectedError("adapted proposal edited bindings it was not permitted to edit")
        if not isinstance(bindings, Mapping):
            raise ApcChildRejectedError("adapted proposal bindings must be an object")
        for key, value in bindings.items():
            if not isinstance(key, str) or not key:
                raise ApcChildRejectedError("adapted proposal binding keys must be nonempty strings")
            if isinstance(value, (dict, list, tuple)) or value is None:
                raise ApcChildRejectedError("adapted proposal bindings must be typed scalar values")
    size = len(contracts.canonical_json(proposal))
    if size > resolved.apc_output_char_limit:
        raise ApcChildRejectedError("adapted proposal exceeds the bounded output size")
    source = proposal.get("source")
    if not isinstance(source, Mapping) or source.get("template_id") != template_record.get("template_id"):
        raise ApcChildRejectedError("adapted proposal is not bound to the selected template revision")


def run_apc_child(
    *,
    request: Mapping[str, Any],
    template_record: Mapping[str, Any],
    launcher: Callable[[Mapping[str, Any]], Mapping[str, Any] | None],
    store: Any | None = None,
    limits: PreparationLimits | None = None,
    clock: Callable[[], float] | None = None,
    deadline: float | None = None,
    cleanup: Mapping[str, Any] | None = None,
) -> ApcAttempt:
    """Persist intent, launch once, and validate one bounded child attempt."""

    resolved = limits or PreparationLimits()
    now = clock or time.monotonic
    try:
        apc.validate_apc_request(request)
    except apc.APCError as exc:
        raise ApcUnavailableError(f"invalid APC request: {exc}") from exc

    attempt_number = 1
    if store is not None:
        prior = store.list_apc_child_operations_by_request(request["content_hash"])
        unresolved = [
            operation
            for operation in prior
            if operation["status"] in contracts.APC_CHILD_UNRESOLVED_STATUSES
        ]
        if unresolved:
            latest = unresolved[-1]
            raise ApcChildAmbiguityError(
                f"{latest['status']} APC child already exists; "
                "reconcile the exact child before any retry"
            )
        attempt_number = len(prior) + 1

    intent = contracts.make_apc_child_operation(
        request=request,
        status="intent_recorded",
        launch_intent={"binding": dict(request["binding"]), "permitted_edits": list(request["permitted_edits"])},
        attempt=attempt_number,
    )
    if store is not None:
        existing, created = store.create_apc_child_operation(intent)
        if not created:
            raise ApcChildAmbiguityError(
                f"{existing['status']} APC child already exists; "
                "reconcile the exact child before any retry"
            )
    if deadline is not None and now() > deadline:
        if store is not None:
            store.record_apc_child_operation(
                contracts.make_apc_child_operation(
                    request=request,
                    status="refused",
                    cleanup={"reason": "stage allowance expired before launch"},
                    attempt=attempt_number,
                )
            )
        raise ApcChildTimeoutError("the child stage allowance expired before launch")

    launch_request: Mapping[str, Any] = request
    if (
        deadline is not None
        and getattr(launcher, APC_ENCLOSING_DEADLINE_ATTRIBUTE, None) is True
    ):
        # The launcher opts into the one effective cutoff for this attempt;
        # it strips the key before any request validation or child-card use.
        launch_request = {
            **request,
            APC_ENCLOSING_DEADLINE_KEY: float(deadline),
        }
    try:
        observed = launcher(launch_request)
    except ApcChildUnavailableError as exc:
        # The product harness proved that no child was launched, so the
        # recorded attempt becomes terminal instead of an unresolved claim
        # that would block every later attempt.
        if store is not None:
            store.record_apc_child_operation(
                contracts.make_apc_child_operation(
                    request=request,
                    status="refused",
                    cleanup={
                        "state": "no child was launched; the product harness refused the attempt",
                        "reason": str(exc),
                    },
                    attempt=attempt_number,
                )
            )
        raise
    details = dict(observed) if isinstance(observed, Mapping) else {}
    if not isinstance(observed, Mapping) or not observed.get("invocation_id"):
        # The acknowledgement is ambiguous.  The exact identity the product
        # harness did record (the lane, its run, and the phase that stopped
        # answering) stays visible on the durable operation, so a retry must
        # reconcile that exact child instead of blindly relaunching it.
        launch_intent: dict[str, Any] = {
            "acknowledgement": str(
                details.get("acknowledgement")
                or ("lost" if observed is None else "unreadable")
            )
        }
        for key in ("phase", "lane_id", "run_id", "worktree_path"):
            value = details.get(key)
            if isinstance(value, str) and value:
                launch_intent[key] = value
        ambiguous = contracts.make_apc_child_operation(
            request=request,
            status="ambiguous",
            launch_intent=launch_intent,
            cleanup=(
                dict(details["cleanup"])
                if isinstance(details.get("cleanup"), Mapping)
                else None
            ),
            attempt=attempt_number,
        )
        if store is not None:
            store.record_apc_child_operation(ambiguous)
        raise ApcChildAmbiguityError(
            "child launch acknowledgement is ambiguous; "
            "reconcile the exact child before retrying"
        )

    result = observed.get("result")
    observed_cleanup = observed.get("cleanup")
    effective_cleanup = (
        dict(observed_cleanup)
        if isinstance(observed_cleanup, Mapping)
        else (dict(cleanup) if cleanup else None)
    )
    # Only an explicit proof retires the exact child.  Without it the child may
    # still be live, so a timeout or rejection must stay unresolved.
    cleanup_proven = bool(
        isinstance(effective_cleanup, Mapping)
        and effective_cleanup.get("cleanup_proven") is True
    )
    launched = contracts.make_apc_child_operation(
        request=request,
        status="launched",
        launch_intent={"binding": dict(request["binding"])},
        observed_invocation={
            key: observed[key] for key in observed if key not in ("result", "cleanup")
        },
        result=result if isinstance(result, Mapping) else None,
        cleanup=effective_cleanup,
        attempt=attempt_number,
    )
    if store is not None:
        store.record_apc_child_operation(launched)

    if deadline is not None and now() > deadline:
        # The child exceeded the one shared stage bound.  Only a proven
        # retirement makes the attempt terminal; otherwise the exact live
        # ownership stays unresolved and a retry must reconcile it first.
        if cleanup_proven:
            retired = contracts.make_apc_child_operation(
                request=request,
                status="failed",
                launch_intent={"binding": dict(request["binding"])},
                observed_invocation=launched["observed_invocation"],
                result=result if isinstance(result, Mapping) else None,
                cleanup=effective_cleanup,
                attempt=attempt_number,
            )
            if store is not None:
                store.record_apc_child_operation(retired)
            raise ApcChildTimeoutError(
                "the child exceeded the enclosing absolute deadline; its exact "
                "owned lane was retired by the product harness"
            )
        cleanup_pending = contracts.make_apc_child_operation(
            request=request,
            status="cleanup_pending",
            launch_intent={"binding": dict(request["binding"])},
            observed_invocation=launched["observed_invocation"],
            result=result if isinstance(result, Mapping) else None,
            cleanup=(
                effective_cleanup
                if isinstance(effective_cleanup, Mapping)
                else {
                    "state": "cleanup pending on the product harness",
                    "reason": "the child exceeded the enclosing absolute deadline after launch",
                }
            ),
            attempt=attempt_number,
        )
        if store is not None:
            store.record_apc_child_operation(cleanup_pending)
        raise ApcChildTimeoutError(
            "the child exceeded the enclosing absolute deadline; "
            "reconcile its exact owned child before any retry"
        )

    def _reject(reason: str) -> ApcChildRejectedError:
        failure = ApcChildRejectedError(reason)
        if store is not None:
            if effective_cleanup is not None and not cleanup_proven:
                # The rejected child may still be live; retain the exact
                # unresolved ownership instead of a terminal claim.
                store.record_apc_child_operation(
                    contracts.make_apc_child_operation(
                        request=request,
                        status="cleanup_pending",
                        observed_invocation=launched["observed_invocation"],
                        cleanup={**dict(effective_cleanup), "reason": reason},
                        attempt=attempt_number,
                    )
                )
            else:
                store.record_apc_child_operation(
                    contracts.make_apc_child_operation(
                        request=request,
                        status="failed",
                        observed_invocation=launched["observed_invocation"],
                        cleanup=(
                            dict(effective_cleanup)
                            if isinstance(effective_cleanup, Mapping)
                            else {
                                "state": "retired by the product harness",
                                "reason": reason,
                            }
                        ),
                        attempt=attempt_number,
                    )
                )
        return failure

    if not isinstance(result, Mapping):
        raise _reject("the child returned no bounded result")
    try:
        apc.validate_apc_result(result, request)
    except apc.APCError as exc:
        raise _reject(f"invalid child result: {exc}") from exc
    proposal = result.get("proposed_plan")
    if not isinstance(proposal, Mapping):
        raise _reject("child result has no proposed plan")
    try:
        validate_adapted_proposal(
            template_record,
            proposal,
            permitted_edits=request["permitted_edits"],
            limits=resolved,
            objective_id=request["parent_objective_id"],
        )
    except ApcChildRejectedError as exc:
        raise _reject(str(exc)) from exc
    if isinstance(effective_cleanup, Mapping) and not cleanup_proven:
        # BEHAVIOR-02 only shows ROOT a proposal once the exact child is proven
        # retired by the product harness.  Reported cleanup without that proof
        # stays unresolved, so the attempt falls back instead of reconciling.
        raise _reject(
            "the child's cleanup is not proven by the product harness; "
            "its exact ownership stays unresolved"
        )
    reconciled = contracts.make_apc_child_operation(
        request=request,
        status="reconciled",
        launch_intent={"binding": dict(request["binding"])},
        observed_invocation=launched["observed_invocation"],
        result=result,
        cleanup=(
            effective_cleanup
            if isinstance(effective_cleanup, Mapping)
            else {"state": "owned by the product harness"}
        ),
        attempt=attempt_number,
    )
    if store is not None:
        store.record_apc_child_operation(reconciled)
    return ApcAttempt(child_operation=reconciled, proposal=dict(proposal), reason="proposed")


def reconcile_apc_child(
    *,
    store: Any,
    request: Mapping[str, Any],
    observed_invocation: Mapping[str, Any],
    cleanup: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Reconcile one ambiguous child by exact harness identity."""

    if not isinstance(observed_invocation, Mapping) or not observed_invocation.get("invocation_id"):
        raise ApcChildRejectedError("reconciliation requires the exact observed invocation")
    attempt = request.get("attempt", 1)
    reconciled = contracts.make_apc_child_operation(
        request=request,
        status="reconciled",
        observed_invocation=observed_invocation,
        cleanup=dict(cleanup) if cleanup else None,
        attempt=attempt if isinstance(attempt, int) and not isinstance(attempt, bool) else 1,
    )
    existing = store.get_apc_child_operation(reconciled["child_operation_id"])
    if existing["status"] == "reconciled":
        return existing
    return store.record_apc_child_operation(reconciled)


__all__ = [
    "ApcAttempt",
    "ApcChildAmbiguityError",
    "ApcChildUnavailableError",
    "ApcChildRejectedError",
    "ApcChildTimeoutError",
    "ApcUnavailableError",
    "HarnessBridgeError",
    "reconcile_apc_child",
    "run_apc_child",
    "validate_adapted_proposal",
]

