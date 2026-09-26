"""Narrow optional-memory handoff seam for bootstrap, resume, and launch.

This module is the only harness integration point for the Stage-A memory
slice.  Legacy task cards without ``memory_handoff`` take the ordinary path.
"""

from __future__ import annotations

import json
import os
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .core import content_hash, read_json
from .records import atomic_write_json


class MemoryHandoffError(RuntimeError):
    """The optional memory handoff is invalid or cannot be materialized."""


class PendingPlanError(MemoryHandoffError):
    """The enhanced handoff has no ROOT-accepted plan, so nothing may dispatch.

    This is the fail-closed pending-plan condition: an enabled enhanced
    handoff that is explicitly ``absent``, still in ``candidate_review``, or
    missing its finalized dispatch envelope cannot execute.  It is a distinct
    condition from an invalid or copied envelope, and it is never resolved by
    silently falling back to the legacy path.
    """


def _memory_module(name: str):
    """Import one ``memory_harness`` module for the enhanced path only.

    The product package is optional for legacy and all-off cards, so it is
    imported lazily at the point of use instead of at module import.  A
    missing package is a precise handoff error rather than an attribute
    failure, and the import is immune to whichever module happened to be
    imported first.
    """

    import importlib

    try:
        return importlib.import_module(f"memory_harness.{name}")
    except Exception as exc:  # pragma: no cover - package unavailable
        raise MemoryHandoffError(
            f"the memory_harness package is not importable: {exc}"
        ) from exc


def handoff_from_task_card(task_card: Mapping[str, Any]) -> Mapping[str, Any] | None:
    value = task_card.get("memory_handoff")
    return value if isinstance(value, Mapping) else None


def memory_paths(worktree: str | Path) -> tuple[Path, Path]:
    root = Path(worktree) / ".agent-workspace"
    return root / "memory-state.sqlite3", root / "memory-dispatch.json"


def captured_network_resolution(
    *, worktree_path: str | Path, envelope: Mapping[str, Any],
    task_card: Mapping[str, Any],
) -> dict[str, Any]:
    """Read the per-objective profile that preparation actually captured.

    The task handoff names ROOT's requested profile; the durable preparation
    owns its effective resolution. Neither the task text nor a launch option
    can replace that captured record.
    """
    handoff = enabled_memory_handoff(task_card)
    if handoff is None or handoff["plan_state"] != "execution_accepted":
        raise MemoryHandoffError("network payload requires an accepted memory handoff")
    requested = handoff.get("configuration", {}).get("network_profile", "normal")
    if not isinstance(requested, str) or requested not in _memory_module("config").NETWORK_MODES:
        raise MemoryHandoffError("ROOT handoff has an invalid network_profile")
    store_path, _ = memory_paths(worktree_path)
    if not store_path.is_file():
        raise MemoryHandoffError("captured network preparation is missing")
    memory_store = _memory_module("store").MemoryStore(store_path)
    try:
        memory_store.initialize()
        rows = memory_store.list_captured_preparations(envelope["decision_id"])
    except Exception as exc:
        raise MemoryHandoffError(f"cannot read captured network preparation: {exc}") from exc
    finally:
        memory_store.close()
    first = [row for row in rows if row.get("attempt") == 1]
    if len(first) != 1 or not rows:
        raise MemoryHandoffError("captured first network preparation is missing or ambiguous")
    resolution = first[0].get("network_resolution")
    expected_context = {
        "objective_id": envelope["objective_id"],
        "task_card_digest": envelope["task_card_digest"],
        "plan_id": envelope["plan_id"],
        "plan_digest": envelope["plan_digest"],
        "decision_id": envelope["decision_id"],
        "route": envelope["route"],
        "plan_state": envelope["plan_state"],
    }
    if any(row.get(key) != envelope[key] for row in rows for key in expected_context):
        raise MemoryHandoffError("captured network preparation has another dispatch identity")
    # Older enhanced preparations had only the effective normal-mode column.
    # Their explicit read projection makes no native suppression claim.
    if resolution is None and requested == "normal" and all(
        row.get("network_resolution") is None and row.get("network_mode") == "normal"
        for row in rows
    ):
        return _memory_module("contracts").preparation_network_resolution(first[0])
    if (not isinstance(resolution, dict)
            or resolution.get("requested_mode") != requested
            or resolution.get("effective_mode") != first[0].get("network_mode")
            or resolution.get("context") != expected_context
            or any(row.get("network_resolution") != resolution for row in rows)):
        raise MemoryHandoffError("captured network resolution conflicts with the accepted dispatch")
    return dict(resolution)


def validate_task_card(task_card: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """Validate an optional handoff before any worktree mutation."""

    handoff = handoff_from_task_card(task_card)
    if handoff is None:
        return None
    try:
        contracts = _memory_module("contracts")

        contracts.validate_task_card(task_card)
        contracts.validate_memory_handoff(handoff)
    except MemoryHandoffError:
        raise
    except Exception as exc:
        raise MemoryHandoffError(f"invalid memory_handoff: {exc}") from exc
    return handoff


def handoff_plan_state(task_card: Mapping[str, Any]) -> str | None:
    """Return the validated explicit current-plan state of a task card.

    ``None`` means the card carries no enhanced handoff at all.  Every other
    value is one of ``absent``, ``candidate_review``, or
    ``execution_accepted`` and is always accompanied by a consistent plan
    reference (or its explicit absence).
    """

    handoff = validate_task_card(task_card)
    if handoff is None:
        return None
    return str(handoff["plan_state"])


def handoff_plan(task_card: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return the exact bound plan of an enhanced card, or ``None`` absent."""

    handoff = validate_task_card(task_card)
    if handoff is None:
        return None
    return _memory_module("contracts").handoff_plan(handoff)


def enabled_memory_handoff(task_card: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """Return the handoff only when its resolved configuration is enabled.

    Legacy cards and all-off enhanced cards return ``None`` so bootstrap,
    resume, and launch keep the inherited ordinary path without touching the
    optional store.  Malformed configurations fail closed here.
    """

    handoff = validate_task_card(task_card)
    if handoff is None:
        return None
    try:
        config = _memory_module("config")
        resolved = config.resolve_config(handoff.get("configuration"))
    except MemoryHandoffError:
        raise
    except Exception as exc:
        raise MemoryHandoffError(f"invalid memory configuration: {exc}") from exc
    if resolved.all_off:
        return None
    return handoff


def enabled_handoff_state(task_card: Mapping[str, Any]) -> str | None:
    """Return the enabled enhanced handoff's exact plan state, or ``None``.

    ``None`` means this card has no enhanced handoff, or its resolved
    configuration is all-off, so the inherited ordinary harness path applies
    and no optional memory state may be initialized for it.
    """

    handoff = enabled_memory_handoff(task_card)
    return None if handoff is None else str(handoff["plan_state"])


def lane_handoff_state(
    task_card: Mapping[str, Any] | None, lane: Mapping[str, Any] | None
) -> str | None:
    """Resolve one lane's governing enhanced-handoff state, or ``None``.

    The durable task card is the primary authority, and the lane record's
    recorded state is the second witness.  A lane without either is the
    inherited ordinary path.  When both exist they must agree: a stale or
    copied task card cannot quietly change what a lane was prepared to do.
    """

    card_state: str | None = None
    if task_card is not None:
        card_state = enabled_handoff_state(task_card)
    recorded: object = None
    if isinstance(lane, Mapping):
        recorded = lane.get("memory_plan_state")
    lane_state: str | None = None
    if isinstance(recorded, str) and recorded:
        lane_state = recorded
        try:
            states = _memory_module("contracts").HANDOFF_PLAN_STATES
        except MemoryHandoffError:
            raise
        if recorded not in states:
            raise MemoryHandoffError(
                f"lane records an unknown memory plan state: {recorded!r}"
            )
    if card_state is not None and lane_state is not None and card_state != lane_state:
        raise MemoryHandoffError(
            "the durable task card and the lane record disagree about the "
            f"current plan state: card={card_state!r}, lane={lane_state!r}"
        )
    return card_state if card_state is not None else lane_state


def require_accepted_handoff(task_card: Mapping[str, Any]) -> dict[str, Any]:
    """Return the exact bound plan of a dispatchable enhanced card.

    Only the explicit ``execution_accepted`` state may dispatch, and that
    state always carries its exact ROOT-accepted plan.  Every other state is
    the fail-closed pending-plan condition, and a missing or unreadable plan
    reference is an invalid handoff rather than an absent plan.
    """

    handoff = validate_task_card(task_card)
    if handoff is None:
        raise MemoryHandoffError("the task card has no enhanced memory handoff")
    state = str(handoff["plan_state"])
    if state != "execution_accepted":
        raise PendingPlanError(
            "the enhanced handoff is not a ROOT-accepted execution plan "
            f"(plan_state={state!r}); ROOT review must finish before any launch"
        )
    plan = _memory_module("contracts").handoff_plan(handoff)
    if plan is None:
        raise MemoryHandoffError(
            "an execution-accepted handoff must carry its exact accepted plan"
        )
    return plan


def _accepted_checkpoint(task_card: Mapping[str, Any]) -> str:
    """Read the canonical pre-bootstrap checkpoint from ROOT's bound handoff."""

    handoff = validate_task_card(task_card)
    if handoff is None or handoff["plan_state"] != "execution_accepted":
        raise MemoryHandoffError("the task card has no accepted checkpoint source")
    try:
        return _memory_module("contracts")._require_canonical_identity(
            handoff.get("checkpoint"), "handoff checkpoint"
        )
    except Exception as exc:
        raise MemoryHandoffError(
            "accepted memory finalization requires an authoritative canonical "
            "checkpoint in the bound task handoff"
        ) from exc


def plan_state_summary(plan_state: str) -> str:
    """Return the operator-facing meaning of one enhanced plan state."""

    if plan_state == "absent":
        return (
            "no current plan exists; bounded preparation recorded its fresh "
            "ROOT-planning disposition and nothing may execute yet"
        )
    if plan_state == "candidate_review":
        return (
            "a candidate plan is still in ROOT review; its exact review "
            "identity is retained and nothing may execute yet"
        )
    return f"the enhanced handoff state {plan_state!r} is not dispatchable"


def _write_envelope(worktree: str | Path, envelope: Mapping[str, Any]) -> Path:
    _, envelope_path = memory_paths(worktree)
    envelope_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(envelope_path, dict(envelope))
    return envelope_path


def _bind_final_context(
    envelope: Mapping[str, Any],
    context: Mapping[str, Any],
) -> dict[str, Any]:
    """Keep Lane 1's canonical envelope and its embedded context intact."""

    if envelope.get("schema") != _memory_module("contracts").FINAL_ENVELOPE_SCHEMA:
        raise MemoryHandoffError("the accepted plan has no domain-finalized envelope")
    if envelope.get("final_context") != context:
        raise MemoryHandoffError("dispatch envelope differs from the durable finalized context")
    return dict(envelope)


def _record_bound_final_context(
    worktree_path: str | Path,
    context: Mapping[str, Any],
    envelope: Mapping[str, Any],
) -> None:
    """Link the durable final context to the envelope actually handed off."""

    store = _memory_module("store")
    store_path, _ = memory_paths(worktree_path)
    memory_store = store.MemoryStore(store_path)
    memory_store.initialize()
    try:
        memory_store.record_final_context(
            context, envelope_digest=envelope["content_hash"]
        )
    finally:
        memory_store.close()


def _prepare_memory_outcome(
    *,
    task_card: Mapping[str, Any],
    handoff: Mapping[str, Any],
    resolved_config: Any,
    lane_id: str,
    run_id: str,
    worktree_path: str | Path,
    base_commit: str,
    finalize: bool,
    network_mode: str,
    search_stores: Sequence[Any] = (),
    required_sources: frozenset[tuple[str, str, str]] = frozenset(),
):
    """Run one bounded preparation for the exact handoff state.

    The explicit ``absent``, ``candidate_review``, and ``execution_accepted``
    states all run the same bounded decision: an absent plan starts fresh ROOT
    planning, a pending candidate keeps its exact review identity, and only an
    exact ROOT-accepted plan is finalized for dispatch.  The plan reference is
    never dereferenced when it is explicitly absent.
    """

    runtime = _memory_module("runtime")
    store = _memory_module("store")

    plan = handoff["plan"]
    checkpoint = _accepted_checkpoint(task_card) if finalize else None
    mandatory_content = [
        {"id": "task", "kind": "task", "content": task_card["task"]},
    ]
    if isinstance(plan, Mapping):
        mandatory_content.append(
            {
                "id": "accepted-plan",
                "kind": "accepted-plan",
                "content": plan["content"],
            }
        )
    if finalize:
        mandatory_content.extend([
            {"id": "base", "kind": "base", "content": base_commit},
            {"id": "route", "kind": "route", "content": handoff["route"]},
            {"id": "checkpoint", "kind": "checkpoint", "content": checkpoint},
            {"id": "security", "kind": "security", "content": _memory_module("contracts").FINAL_CONTEXT_SECURITY},
        ])
    store_path, _ = memory_paths(worktree_path)
    memory_store = store.MemoryStore(store_path)
    memory_store.initialize()
    try:
        memory_runtime = runtime.MemoryRuntime(memory_store, config=resolved_config)
        return memory_runtime.prepare_with_memory(
            task_card=task_card,
            plan=plan,
            objective_id=handoff["objective_id"],
            route=handoff.get("route", "ordinary"),
            request=handoff.get("configuration"),
            network_mode=network_mode,
            lane_id=lane_id,
            run_id=run_id,
            worktree_path=str(worktree_path),
            base_commit=base_commit,
            mandatory_content=mandatory_content,
            checkpoint=checkpoint,
            execution_role="worker" if finalize else None,
            invocation_target="orchestrator_harness.controller" if finalize else None,
            recipient=f"worker:{lane_id}" if finalize else None,
            finalize=finalize,
            stores=search_stores,
            required_sources=required_sources,
        )
    finally:
        memory_store.close()


@dataclass(frozen=True)
class LaneMemory:
    """One lane preparation's exact optional-memory result.

    ``state`` is ``None`` only for legacy cards and all-off enhanced cards,
    which keep the inherited ordinary harness path and never initialize the
    optional store.  Every other value is the enhanced handoff's explicit plan
    state, and a dispatchable lane always carries its exact finalized
    envelope.  ``pending_plan`` is the fail-closed condition bootstrap and
    resume must report instead of preparing an execution worker.
    """

    state: str | None
    envelope: dict[str, Any] | None
    outcome: Any

    @property
    def dispatchable(self) -> bool:
        return self.state == "execution_accepted" and self.envelope is not None

    @property
    def pending_plan(self) -> bool:
        return self.state is not None and not self.dispatchable


def prepare_lane_memory(
    *,
    task_card: Mapping[str, Any],
    lane_id: str,
    run_id: str,
    worktree_path: str | Path,
    base_commit: str,
    search_stores: Sequence[Any] = (),
    required_sources: frozenset[tuple[str, str, str]] = frozenset(),
) -> "LaneMemory":
    """Run one bounded lane preparation and report its exact disposition.

    The bounded STEP-04 preparation runs over the real task card, objective,
    plan, and dispatch identity.  Only an exact ROOT-accepted plan is
    finalized and written for dispatch: an explicitly absent plan starts fresh
    ROOT planning and a pending candidate keeps its exact review identity, so
    both still get one durable preparation decision, trace, and disposition
    while never becoming execution authority.  Legacy cards and all-off
    enhanced cards return ``state=None`` without touching the optional store.
    """

    handoff = enabled_memory_handoff(task_card)
    if handoff is None:
        _, envelope_path = memory_paths(worktree_path)
        envelope_path.unlink(missing_ok=True)
        return LaneMemory(state=None, envelope=None, outcome=None)
    try:
        config = _memory_module("config")

        resolved_config = config.resolve_config(handoff.get("configuration"))
        requested_network = handoff.get("configuration", {}).get("network_profile", "normal")
        if not isinstance(requested_network, str) or requested_network not in config.NETWORK_MODES:
            raise MemoryHandoffError("ROOT handoff has an invalid network_profile")
        # Only the explicit execution-accepted state finalizes a dispatchable
        # envelope.  An explicitly absent plan and a pending candidate still
        # run one bounded preparation (their fresh ROOT-planning or review
        # disposition is durable), but they never become execution authority.
        state = str(handoff["plan_state"])
        accepted = state == "execution_accepted"
        outcome = _prepare_memory_outcome(
            task_card=task_card,
            handoff=handoff,
            resolved_config=resolved_config,
            lane_id=lane_id,
            run_id=run_id,
            worktree_path=worktree_path,
            base_commit=base_commit,
            finalize=accepted,
            network_mode=requested_network,
            search_stores=search_stores,
            required_sources=required_sources,
        )
        if not accepted:
            _, envelope_path = memory_paths(worktree_path)
            envelope_path.unlink(missing_ok=True)
            return LaneMemory(state=state, envelope=None, outcome=outcome)
        if outcome is None or not isinstance(outcome.envelope, Mapping):
            raise MemoryHandoffError(
                "the accepted plan has no finalized dispatch envelope"
            )
        if not isinstance(outcome.context, Mapping):
            raise MemoryHandoffError(
                "the accepted plan has no durable finalized context"
            )
        envelope = _bind_final_context(
            outcome.envelope,
            outcome.context,
        )
        validate_envelope_for_launch(
            envelope=envelope,
            task_card=task_card,
            lane_id=lane_id,
            run_id=run_id,
            worktree_path=worktree_path,
            base_commit=base_commit,
        )
        validate_final_context_for_launch(
            context=outcome.context,
            envelope=envelope,
            task_card=task_card,
            lane_id=lane_id,
            run_id=run_id,
            worktree_path=worktree_path,
            base_commit=base_commit,
        )
        envelope_path = _write_envelope(worktree_path, envelope)
        try:
            _record_bound_final_context(worktree_path, outcome.context, envelope)
        except Exception:
            envelope_path.unlink(missing_ok=True)
            raise
        return LaneMemory(state=state, envelope=envelope, outcome=outcome)
    except MemoryHandoffError:
        raise
    except Exception as exc:
        raise MemoryHandoffError(f"cannot prepare memory handoff: {exc}") from exc


def prepare_bootstrap_envelope(
    *,
    task_card: Mapping[str, Any],
    lane_id: str,
    run_id: str,
    worktree_path: str | Path,
    base_commit: str,
) -> dict[str, Any] | None:
    """Prepare and write the finalized bootstrap envelope, or return ``None``.

    Only an exact ROOT-accepted plan produces a dispatchable envelope; the
    enhanced ``absent`` and ``candidate_review`` states keep their durable
    disposition and leave no envelope behind.
    """

    return prepare_lane_memory(
        task_card=task_card,
        lane_id=lane_id,
        run_id=run_id,
        worktree_path=worktree_path,
        base_commit=base_commit,
    ).envelope


def prepare_resume_envelope(
    *,
    task_card: Mapping[str, Any],
    lane_id: str,
    run_id: str,
    worktree_path: str | Path,
    base_commit: str,
) -> dict[str, Any] | None:
    """Prepare the resumed envelope with bootstrap-equivalent validation.

    A resume is the same logical decision as the original bootstrap, so it
    reuses the durable decision identity and its absolute deadline instead of
    replenishing the objective's budget.
    """

    validate_resume_handoff(
        task_card=task_card,
        lane_id=lane_id,
        worktree_path=worktree_path,
        base_commit=base_commit,
    )
    return prepare_bootstrap_envelope(
        task_card=task_card,
        lane_id=lane_id,
        run_id=run_id,
        worktree_path=worktree_path,
        base_commit=base_commit,
    )


def validate_resume_handoff(
    *,
    task_card: Mapping[str, Any],
    lane_id: str,
    worktree_path: str | Path,
    base_commit: str,
    prior_run_id: str | None = None,
) -> None:
    """Check the prior accepted artifact before resume can replace it.

    Resume is allowed to create a fresh run envelope only from the same exact
    accepted task and plan. Reading the prior durable context first exposes a
    stale or corrupted artifact instead of silently replacing it.
    """

    if enabled_handoff_state(task_card) != "execution_accepted":
        if memory_paths(worktree_path)[1].exists():
            raise MemoryHandoffError(
                "resume task card changed an accepted lane into a non-dispatchable handoff"
            )
        return
    envelope = load_envelope(worktree_path)
    if envelope is None:
        raise MemoryHandoffError("the accepted lane has no prior finalized handoff")
    validate_envelope_for_launch(
        envelope=envelope,
        task_card=task_card,
        lane_id=lane_id,
        run_id=prior_run_id or str(envelope.get("run_id") or ""),
        worktree_path=worktree_path,
        base_commit=base_commit,
    )
    context = load_final_context(worktree_path=worktree_path, envelope=envelope)
    validate_final_context_for_launch(
        context=context,
        envelope=envelope,
        task_card=task_card,
        lane_id=lane_id,
        run_id=prior_run_id or str(envelope.get("run_id") or ""),
        worktree_path=worktree_path,
        base_commit=base_commit,
    )


def finalize_envelope(
    *,
    task_card: Mapping[str, Any],
    plan: Mapping[str, Any],
    decision_id: str,
    lane_id: str,
    run_id: str,
    worktree_path: str | Path,
    base_commit: str,
    mandatory_content: list[Mapping[str, Any]] | None = None,
    optional_items: list[Mapping[str, Any]] | None = None,
    configuration: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Finalize one exact safe context and write it for this worktree.

    Returns the finalized envelope, or ``None`` when the resolved configuration
    is all-off so the inherited harness path applies unchanged.
    """

    from memory_harness import config as memory_config
    from memory_harness import context as memory_context

    resolved = memory_config.resolve_config(configuration)
    if resolved.all_off:
        _, envelope_path = memory_paths(worktree_path)
        envelope_path.unlink(missing_ok=True)
        return None
    limits = memory_config.resolve_limits(None)
    finalized = memory_context.finalize_context(
        task_card=task_card,
        plan=plan,
        decision_id=decision_id,
        lane_id=lane_id,
        run_id=run_id,
        worktree_path=str(worktree_path),
        base_commit=base_commit,
        strategy=resolved.strategy,
        configuration={**dict(configuration or {}), "strategy": resolved.strategy},
        mandatory_content=list(mandatory_content or []),
        optional_items=list(optional_items or []),
        limits=limits,
    )
    _write_envelope(worktree_path, finalized.envelope)
    return finalized.envelope


def validate_final_context_for_launch(
    *,
    context: Mapping[str, Any],
    envelope: Mapping[str, Any],
    task_card: Mapping[str, Any],
    lane_id: str,
    run_id: str,
    worktree_path: str | Path,
    base_commit: str,
) -> dict[str, Any]:
    """Validate a finalized context against the actual dispatch target."""

    plan = require_accepted_handoff(task_card)
    try:
        if context.get("observed_invocation") is not None:
            raise MemoryHandoffError("finalized context cannot claim an observed launch")
        from memory_harness import context as memory_context

        memory_context.validate_final_context(
            context,
            envelope=envelope,
            task_card=task_card,
            plan=plan,
            lane_id=lane_id,
            run_id=run_id,
            base_commit=base_commit,
            worktree_path=str(worktree_path),
            checkpoint=_accepted_checkpoint(task_card),
            execution_role="worker",
            invocation_target="orchestrator_harness.controller",
            recipient=f"worker:{lane_id}",
        )
    except PendingPlanError:
        raise
    except MemoryHandoffError:
        raise
    except Exception as exc:
        raise MemoryHandoffError(f"invalid finalized context: {exc}") from exc
    return dict(context)


def load_envelope(worktree_path: str | Path) -> dict[str, Any] | None:
    _, envelope_path = memory_paths(worktree_path)
    if not envelope_path.is_file():
        return None
    try:
        return read_json(envelope_path)
    except (OSError, ValueError) as exc:
        raise MemoryHandoffError(f"cannot read memory dispatch envelope: {exc}") from exc


def validate_envelope_for_launch(
    *,
    envelope: Mapping[str, Any],
    task_card: Mapping[str, Any],
    lane_id: str,
    run_id: str,
    worktree_path: str | Path,
    base_commit: str,
) -> dict[str, Any]:
    """Validate a durable dispatch envelope against the real launch target.

    A copied, stale, or otherwise mismatched envelope fails here: the exact
    task card, objective, route, accepted plan, lane, run, worktree, and base
    must all agree before any dispatch intent is recorded.
    """

    try:
        from memory_harness import contracts

        plan = require_accepted_handoff(task_card)
        checkpoint = _accepted_checkpoint(task_card)
        if envelope.get("observed_invocation") is not None:
            raise MemoryHandoffError("finalized envelope cannot claim an observed launch")
        contracts.validate_envelope(
            envelope,
            task_card=task_card,
            plan=plan,
            lane_id=lane_id,
            run_id=run_id,
            base_commit=base_commit,
            worktree_path=str(worktree_path),
            require_final_context=True,
        )
        if envelope.get("checkpoint") != checkpoint:
            raise MemoryHandoffError("dispatch envelope checkpoint changed from the bound task handoff")
        if envelope.get("invocation_target") != "orchestrator_harness.controller":
            raise MemoryHandoffError("dispatch envelope invocation target changed")
        if envelope.get("recipient") != f"worker:{lane_id}":
            raise MemoryHandoffError("dispatch envelope recipient does not match the lane")
    except PendingPlanError:
        raise
    except MemoryHandoffError:
        raise
    except Exception as exc:
        raise MemoryHandoffError(f"invalid memory dispatch envelope: {exc}") from exc
    return dict(envelope)


def load_final_context(
    *, worktree_path: str | Path, envelope: Mapping[str, Any]
) -> dict[str, Any]:
    """Return the durable finalized context for one exact dispatch envelope.

    The store is opened only for an enhanced lane that already produced a
    dispatchable envelope.  A missing or unreadable durable final context is a
    fail-closed invalid-launch condition rather than a reason to dispatch from
    the envelope alone.
    """

    decision_id = envelope.get("decision_id")
    if not isinstance(decision_id, str) or not decision_id:
        raise MemoryHandoffError("dispatch envelope has no decision identity")
    store_path, _ = memory_paths(worktree_path)
    if not Path(store_path).is_file():
        raise MemoryHandoffError(
            "the enhanced dispatch has no durable memory state to validate"
        )
    store = _memory_module("store")
    try:
        memory_store = store.MemoryStore(store_path)
        memory_store.initialize()
        try:
            context = memory_store.get_final_context_for_decision(decision_id)
        finally:
            memory_store.close()
    except Exception as exc:
        raise MemoryHandoffError(f"cannot read durable finalized context: {exc}") from exc
    if not isinstance(context, Mapping):
        raise MemoryHandoffError(
            "the enhanced dispatch has no durable finalized context for its decision"
        )
    return dict(context)


def required_resume_sources(context: Mapping[str, Any]) -> frozenset[tuple[str, str, str]]:
    """Keep only plan-required sources actually delivered by the prior handoff."""

    trace = context["delivery_trace"]
    delivered = {entry["id"] for entry in trace["context_delivered"]}
    required = set()
    for entry in trace["selected"]:
        if entry["id"] not in delivered or entry["provenance"].get("plan_affecting") is not True:
            continue
        provenance = entry["provenance"]
        identity = tuple(provenance.get(key) for key in (
            "source_id", "logical_id", "revision_id",
        ))
        if any(not isinstance(value, str) or not value for value in identity):
            raise MemoryHandoffError("prior plan-required source has no exact identity")
        required.add(identity)
    return frozenset(required)


def _open_runtime(worktree_path: str | Path):
    from memory_harness import runtime, store

    store_path, _ = memory_paths(worktree_path)
    memory_store = store.MemoryStore(store_path)
    memory_store.initialize()
    return memory_store, runtime.MemoryRuntime(memory_store)


_PROVIDER_CREDENTIAL_KEYS = {
    "codex": frozenset({"OPENAI_API_KEY"}),
    "claude-code": frozenset({"ANTHROPIC_API_KEY"}),
    "qwen-code": frozenset({"QWEN_API_KEY", "DASHSCOPE_API_KEY", "GEMINI_API_KEY"}),
}


def _task_credential_keys(task_card: Mapping[str, Any] | None) -> frozenset[str]:
    """Validate declared names; a name cannot prove a token's authority."""

    declared = (task_card or {}).get("worker_task_credentials", [])
    if not isinstance(declared, list) or any(not isinstance(key, str) for key in declared):
        raise MemoryHandoffError("worker_task_credentials must list environment names")
    if len(declared) != len(set(declared)):
        raise MemoryHandoffError("worker_task_credentials contains duplicate names")
    for key in declared:
        if (
            re.fullmatch(r"TASK_ONLY_[A-Z0-9_]+", key) is None
            or not _credential_key(key)
            or _control_credential_key(key)
        ):
            raise MemoryHandoffError("worker_task_credentials contains a non-task credential name")
    return frozenset(declared)


def worker_environment(
    task_card: Mapping[str, Any] | None = None,
    *,
    provider_id: str = "codex",
    worktree_path: str | Path | None = None,
) -> dict[str, str]:
    """Return a worker environment without product control credentials.

    A task card supplies credential names but cannot attest the authority of
    their values.  Withhold them until a separate validated channel exists;
    actions that require them must stop. Provider model-auth keys are separate
    from task authority.
    """

    from memory_harness import privacy

    inherited = privacy.worker_environment(os.environ)
    _task_credential_keys(task_card)
    allowed_provider = _PROVIDER_CREDENTIAL_KEYS.get(provider_id, frozenset())
    forbidden = {
        key: value for key, value in os.environ.items()
        if _control_credential_key(key) and isinstance(value, str) and value
    }
    environment = {
        key: value for key, value in inherited.items()
        if not _control_credential_key(key)
        and value not in forbidden.values()
        and (not _credential_key(key) or key in allowed_provider)
    }
    if worktree_path is not None:
        worktree = Path(worktree_path).resolve()
        provider_home = {
            "codex": ("CODEX_HOME", worktree / ".codex"),
            "claude-code": ("CLAUDE_CONFIG_DIR", worktree / ".claude"),
            "qwen-code": ("QWEN_HOME", worktree / ".qwen"),
        }.get(provider_id)
        if provider_home is not None:
            key, path = provider_home
            environment[key] = str(path)
    return environment


_CONTROL_AUTHORITY_WORDS = frozenset({
    "APPROVAL", "APPROVE", "PUBLICATION", "PUBLISH", "REVOCATION",
    "REVOKE", "POLICY", "CONTROL",
})
_CREDENTIAL_WORDS = frozenset({
    "TOKEN", "KEY", "SECRET", "PASSWORD", "CREDENTIAL", "CREDENTIALS", "AUTH", "ASKPASS",
})


def _credential_key(key: str) -> bool:
    return bool(set(re.split(r"[^A-Z0-9]+", key.upper())) & _CREDENTIAL_WORDS)


def _control_credential_key(key: str) -> bool:
    words = set(re.split(r"[^A-Z0-9]+", key.upper()))
    if not _credential_key(key):
        return False
    if words & _CONTROL_AUTHORITY_WORDS:
        return True
    return key.upper().startswith("MEMORY_HARNESS_") and not key.upper().startswith(
        "MEMORY_HARNESS_TASK_ONLY_"
    )


def redact_control_diagnostic(message: str) -> str:
    """Never echo a ROOT control credential through launch diagnostics."""

    values = sorted(
        {
            value for key, value in os.environ.items()
            if _control_credential_key(key) and isinstance(value, str) and value
        },
        key=len,
        reverse=True,
    )
    for value in values:
        message = message.replace(value, "[REDACTED]")
    return message


def validate_worker_material(
    *, worktree_path: str | Path, invocation: Mapping[str, Any],
    environment: Mapping[str, str], task_card: Mapping[str, Any] | None = None,
) -> None:
    """Check the actual restricted worker inputs before recording an intent.

    The prompt and provider tool configuration may not embed a product
    control credential from the ROOT environment.  Report only the artifact
    name: a refusal must not echo the credential into launch diagnostics.
    """

    worktree = Path(worktree_path)
    workspace = worktree / ".agent-workspace"
    prompt = workspace / "worker-prompt.md"
    if not prompt.is_file():
        raise MemoryHandoffError("restricted worker prompt is missing")
    forbidden = {
        key: value for key, value in os.environ.items()
        if _control_credential_key(key) and isinstance(value, str) and value
    }
    if any(_control_credential_key(key) for key in environment):
        raise MemoryHandoffError("restricted worker environment has product control authority")
    if any(value in environment.values() for value in forbidden.values()):
        raise MemoryHandoffError("restricted worker environment aliases product control authority")
    _task_credential_keys(task_card)
    provider_id = invocation.get("provider", {}).get("id")
    allowed_provider = _PROVIDER_CREDENTIAL_KEYS.get(provider_id, frozenset())
    if any(
        _credential_key(key) and key not in allowed_provider
        for key in environment
    ):
        raise MemoryHandoffError("restricted worker environment has an undeclared credential")
    artifacts = [
        prompt,
        workspace / "invocation.json",
        worktree / ".codex" / "config.toml",
        worktree / ".codex" / "mcp.json",
        worktree / ".claude" / "settings.json",
        worktree / ".claude" / "settings.local.json",
        worktree / ".qwen" / "settings.json",
    ]
    if provider_id == "codex":
        artifacts.append(
            Path(environment.get("CODEX_HOME") or Path.home() / ".codex") / "config.toml"
        )
    elif provider_id == "claude-code":
        artifacts.append(
            Path(environment.get("CLAUDE_CONFIG_DIR") or Path.home() / ".claude")
            / "settings.json"
        )
    elif provider_id == "qwen-code":
        artifacts.append(Path.home() / ".qwen" / "settings.json")
    for artifact in artifacts:
        if not artifact.is_file():
            continue
        try:
            contents = artifact.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise MemoryHandoffError(
                f"restricted worker material is unreadable: {artifact.name}"
            ) from exc
        configured_control = any(
            _control_credential_key(match.group(1))
            for match in re.finditer(
                r"(?im)[\"']?([A-Za-z][A-Za-z0-9_-]+)[\"']?\s*[:=]",
                contents,
            )
        )
        try:
            config = (
                tomllib.loads(contents)
                if artifact.suffix == ".toml"
                else json.loads(contents) if artifact.suffix == ".json" else None
            )
        except (ValueError, TypeError) as exc:
            raise MemoryHandoffError(
                f"restricted worker configuration is invalid: {artifact.name}"
            ) from exc
        tool_authority = _control_tool_config(config)
        if (
            any(key in contents or value in contents for key, value in forbidden.items())
            or configured_control
            or (artifact != prompt and tool_authority)
        ):
            raise MemoryHandoffError(
                f"restricted worker material contains product control authority: {artifact.name}"
            )
    if invocation.get("env") != {}:
        raise MemoryHandoffError("restricted invocation declares an unexpected environment")


def _control_tool_config(config: Any) -> bool:
    """Recognize control actions in a provider's external-tool settings."""

    if not isinstance(config, Mapping):
        return False
    for key, value in config.items():
        name = str(key).lower()
        if "mcp" in name or name == "tools":
            rendered = json.dumps(value, sort_keys=True).lower()
            if any(word.lower() in rendered for word in _CONTROL_AUTHORITY_WORDS):
                return True
        if _control_tool_config(value):
            return True
    return False


def dispatch(
    *,
    worktree_path: str | Path,
    envelope: Mapping[str, Any],
    launcher: Callable[[Mapping[str, Any]], Mapping[str, Any] | None],
) -> dict[str, Any]:
    """Record intent and exact observed invocation around the existing launcher."""

    memory_store, memory_runtime = _open_runtime(worktree_path)
    try:
        return memory_runtime.dispatch(envelope, launcher)
    finally:
        memory_store.close()



def record_dispatch_intent(
    *, worktree_path: str | Path, envelope: Mapping[str, Any],
    supersedes_rejected_attempt_id: str | None = None,
) -> dict[str, Any]:
    """Record the exact dispatch intent before the existing launcher runs."""

    memory_store, memory_runtime = _open_runtime(worktree_path)
    try:
        existing = _dispatch_operation(memory_store, envelope)
        if existing is not None:
            raise MemoryHandoffError(
                f"{existing['status']} dispatch intent already exists; reconcile exact native ownership"
            )
        intent = memory_runtime.record_dispatch_intent(
            envelope, supersedes_rejected_attempt_id=supersedes_rejected_attempt_id,
        )
        confirmed = _dispatch_operation(memory_store, envelope)
        if confirmed is None or confirmed["operation_id"] != intent["operation_id"] or confirmed["status"] != "pending":
            raise MemoryHandoffError("dispatch intent was not durably confirmed")
        return confirmed
    except MemoryHandoffError:
        raise
    except Exception as exc:
        raise MemoryHandoffError(f"cannot record dispatch intent: {exc}") from exc
    finally:
        memory_store.close()


def record_native_review(
    *, worktree_path: str | Path, evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """Submit the complete retained native bundle to the domain transaction."""

    memory_store, memory_runtime = _open_runtime(worktree_path)
    try:
        if evidence["acceptance"]["approval"] == "REJECTED":
            return memory_runtime.record_rejected_native_attempt(evidence)
        return memory_runtime.record_terminal_outcome(evidence)
    except Exception as exc:
        raise MemoryHandoffError(f"cannot record native review: {exc}") from exc
    finally:
        memory_store.close()


def get_rejected_native_attempt(
    *, worktree_path: str | Path, rejected_attempt_id: str,
) -> dict[str, Any]:
    """Read the domain's exact rejected attempt for handoff verification."""
    memory_store, _ = _open_runtime(worktree_path)
    try:
        return memory_store.get_rejected_native_attempt(rejected_attempt_id)
    except Exception as exc:
        raise MemoryHandoffError(f"cannot read rejected native attempt: {exc}") from exc
    finally:
        memory_store.close()


def supersession_id_for_launch(
    *, worktree_path: str | Path, lane: Mapping[str, Any],
    envelope: Mapping[str, Any],
) -> str | None:
    """Verify lane-owned provenance and return only the domain-issued ID."""
    handoff = lane.get("native_supersession")
    if handoff is None:
        return None
    if not isinstance(handoff, Mapping) or set(handoff) != {
        "rejected_attempt_id", "run_id", "decision_id", "operation_id", "evidence_digest",
    }:
        raise MemoryHandoffError("native supersession handoff is malformed")
    rejected_id = handoff["rejected_attempt_id"]
    if not isinstance(rejected_id, str) or not rejected_id:
        raise MemoryHandoffError("native supersession has no returned rejected attempt ID")
    attempt = get_rejected_native_attempt(
        worktree_path=worktree_path, rejected_attempt_id=rejected_id,
    )
    evidence = attempt.get("terminal_evidence")
    dispatch = evidence.get("dispatch") if isinstance(evidence, Mapping) else None
    prior = dispatch.get("envelope") if isinstance(dispatch, Mapping) else None
    acceptance = evidence.get("acceptance") if isinstance(evidence, Mapping) else None
    if (
        attempt.get("content_hash") != content_hash(attempt)
        or not isinstance(prior, Mapping)
        or evidence.get("content_hash") != attempt.get("evidence_digest")
        or not isinstance(acceptance, Mapping)
        or acceptance.get("approval") != "REJECTED"
        or prior.get("decision_id") != attempt.get("decision_id")
        or prior.get("run_id") != attempt.get("run_id")
    ):
        raise MemoryHandoffError("native supersession lacks exact durable rejected-review provenance")
    for field in ("rejected_attempt_id", "run_id", "decision_id", "operation_id", "evidence_digest"):
        if attempt.get(field) != handoff[field]:
            raise MemoryHandoffError(f"native supersession {field} differs from domain authority")
    if attempt["decision_id"] != envelope["decision_id"] or attempt["run_id"] == envelope["run_id"]:
        raise MemoryHandoffError("native supersession is for a different decision or run")
    for field in (
        "task_card_digest", "objective_id", "plan_id", "plan_digest", "base_commit",
        "worktree_path", "route", "recipient", "lane_id", "checkpoint",
        "configuration_digest",
    ):
        if prior.get(field) != envelope.get(field):
            raise MemoryHandoffError(f"native supersession {field} changed from rejected run")
    if prior.get("final_context_id") == envelope.get("final_context_id"):
        raise MemoryHandoffError("native supersession final_context_id was reused")
    return rejected_id


def _dispatch_operation(memory_store: Any, envelope: Mapping[str, Any]) -> dict[str, Any] | None:
    """Find the one intent for this run; reject changed decisions/envelopes.

    Lane 1's current public selector is decision-scoped.  Read the existing
    operation table for a run conflict until its joined run selector lands;
    this creates no second receipt or state owner.
    """

    from memory_harness import contracts

    expected = contracts.make_operation(kind="dispatch", envelope=envelope)
    rows = memory_store._require_connection().execute(
        "SELECT operation_id FROM operations WHERE kind = 'dispatch' AND run_id = ?",
        (envelope["run_id"],),
    ).fetchall()
    if any(row["operation_id"] != expected["operation_id"] for row in rows):
        raise MemoryHandoffError(
            "conflicting dispatch ownership exists for this lane and run"
        )
    matches = [
        operation for operation in memory_store.list_operations(envelope["decision_id"])
        if operation["kind"] == "dispatch" and operation["run_id"] == envelope["run_id"]
    ]
    if any(operation["operation_id"] != expected["operation_id"] for operation in matches):
        raise MemoryHandoffError(
            "conflicting dispatch ownership exists for this decision and run"
        )
    return matches[0] if matches else None


def get_dispatch_operation(
    *, worktree_path: str | Path, envelope: Mapping[str, Any]
) -> dict[str, Any] | None:
    memory_store, _ = _open_runtime(worktree_path)
    try:
        return _dispatch_operation(memory_store, envelope)
    except MemoryHandoffError:
        raise
    except Exception as exc:
        raise MemoryHandoffError(f"cannot read dispatch intent: {exc}") from exc
    finally:
        memory_store.close()


def get_dispatch_decision(
    *, worktree_path: str | Path, envelope: Mapping[str, Any]
) -> dict[str, Any]:
    """Read the exact durable decision in the existing optional store.

    The store exposes a SQL row without its contract schema. On same-decision
    preparation replay it updates the hash and update time while keeping the
    first insertion time. The latest canonical decision uses that update time.
    """
    from memory_harness import contracts

    memory_store, _ = _open_runtime(worktree_path)
    try:
        row = memory_store.get_decision(str(envelope["decision_id"]))
        decision = {
            "schema": "memory-decision/v1",
            **{field: row[field] for field in (
                "decision_id", "task_card_digest", "objective_id", "route",
                "plan_id", "plan_state", "plan_digest", "strategy",
                "configuration", "configuration_digest", "state",
                "content_hash",
            )},
            "created_at": row["updated_at"],
        }
        contracts.validate_decision(decision)
        return decision
    except Exception as exc:
        raise MemoryHandoffError(f"cannot read exact dispatch decision: {exc}") from exc
    finally:
        memory_store.close()


def native_observation(
    *, envelope: Mapping[str, Any], context: Mapping[str, Any],
    controller_identity: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind one observed controller incarnation to the accepted dispatch."""

    pid = controller_identity.get("pid")
    creation = controller_identity.get("creation_time")
    if not isinstance(pid, int) or pid <= 0 or not isinstance(creation, str) or not creation:
        raise MemoryHandoffError("observed controller has no exact process identity")
    return {
        **dispatch_binding(envelope=envelope, context=context),
        "invocation_id": f"controller:{pid}:{creation}",
        "pid": pid,
        "creation_time": creation,
    }


def dispatch_binding(
    *, envelope: Mapping[str, Any], context: Mapping[str, Any]
) -> dict[str, Any]:
    """Identity the controller invocation must carry before native spawn."""

    context_id = context.get("context_id")
    context_digest = context.get("content_hash")
    if not isinstance(context_id, str) or not context_id or not isinstance(context_digest, str) or not context_digest:
        raise MemoryHandoffError("dispatch has no exact final context")
    return {
        "task_card_digest": envelope["task_card_digest"],
        "decision_id": envelope["decision_id"],
        "plan_id": envelope["plan_id"],
        "plan_digest": envelope["plan_digest"],
        "context_id": context_id,
        "context_digest": context_digest,
        "envelope_digest": envelope["content_hash"],
        "lane_id": envelope["lane_id"],
        "run_id": envelope["run_id"],
        "base_commit": envelope["base_commit"],
        "route": envelope["route"],
        "configuration_digest": envelope["configuration_digest"],
    }


def record_observed_invocation(
    *,
    worktree_path: str | Path,
    envelope: Mapping[str, Any],
    observed_invocation: Mapping[str, Any],
) -> dict[str, Any]:
    """Record the exact observed harness invocation after launch."""

    from memory_harness import contracts

    memory_store, _ = _open_runtime(worktree_path)
    try:
        context = load_final_context(worktree_path=worktree_path, envelope=envelope)
        expected = native_observation(
            envelope=envelope,
            context=context,
            controller_identity=observed_invocation,
        )
        if dict(observed_invocation) != expected:
            raise MemoryHandoffError("observed native invocation does not match the exact dispatch")
        existing = _dispatch_operation(memory_store, envelope)
        if existing is None:
            raise MemoryHandoffError("observed invocation has no durable dispatch intent")
        if existing["status"] == "delivered":
            if existing["observed_invocation"] != expected:
                raise MemoryHandoffError("a conflicting native invocation already owns this dispatch")
            return existing
        operation = contracts.make_operation(
            kind="dispatch",
            envelope=envelope,
            status="delivered",
            observed_invocation=observed_invocation,
        )
        if existing["status"] == "ambiguous":
            return _memory_module("runtime").MemoryRuntime(memory_store).reconcile_ambiguous_dispatch(
                envelope, observed_invocation
            )
        return memory_store.record_operation(operation)
    finally:
        memory_store.close()


def record_ambiguous_dispatch(
    *, worktree_path: str | Path, envelope: Mapping[str, Any]
) -> dict[str, Any]:
    """Record an ambiguous launch acknowledgement without duplicating work."""

    from memory_harness import contracts

    memory_store, _ = _open_runtime(worktree_path)
    try:
        operation = contracts.make_operation(
            kind="dispatch",
            envelope=envelope,
            status="ambiguous",
        )
        return memory_store.record_operation(operation)
    finally:
        memory_store.close()


def omit_optional_content(
    *, worktree_path: str | Path, envelope: Mapping[str, Any], item_id: str
) -> dict[str, Any]:
    """Omit legacy optional content; enriched finalization owns its own trace."""

    persisted = load_envelope(worktree_path)
    final_schema = _memory_module("contracts").FINAL_ENVELOPE_SCHEMA
    if envelope.get("schema") == final_schema or (
        persisted is not None and persisted.get("schema") == final_schema
    ):
        raise MemoryHandoffError(
            "final freshness omission for enriched context must run through the "
            "domain finalizer before publishing a new handoff"
        )

    memory_store, memory_runtime = _open_runtime(worktree_path)
    try:
        revised = memory_runtime.omit_optional_content(envelope, item_id)
    finally:
        memory_store.close()
    _write_envelope(worktree_path, revised)
    return revised


__all__ = [
    "LaneMemory",
    "MemoryHandoffError",
    "PendingPlanError",
    "dispatch",
    "dispatch_binding",
    "enabled_handoff_state",
    "finalize_envelope",
    "handoff_from_task_card",
    "handoff_plan",
    "handoff_plan_state",
    "get_dispatch_operation",
    "get_rejected_native_attempt",
    "lane_handoff_state",
    "prepare_lane_memory",
    "load_envelope",
    "load_final_context",
    "memory_paths",
    "native_observation",
    "omit_optional_content",
    "plan_state_summary",
    "prepare_bootstrap_envelope",
    "prepare_resume_envelope",
    "record_ambiguous_dispatch",
    "record_dispatch_intent",
    "record_native_review",
    "supersession_id_for_launch",
    "record_observed_invocation",
    "redact_control_diagnostic",
    "require_accepted_handoff",
    "validate_envelope_for_launch",
    "validate_final_context_for_launch",
    "validate_resume_handoff",
    "validate_task_card",
    "validate_worker_material",
    "worker_environment",
]
