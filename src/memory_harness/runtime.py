"""Runtime coordination for the smallest Stage-A vertical slice."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import Any, Callable, Mapping

from . import contracts
from .config import MemoryConfig, resolve_config
from .privacy import PrivacyPolicy, guard_mandatory, sanitize_optional, worker_bound_finding
from .store import MemoryStore, StoreError


class RuntimeError(ValueError):
    """A Stage-A runtime invariant was violated."""


class DispatchAmbiguityError(RuntimeError):
    """A launch acknowledgement was ambiguous and cannot be safely duplicated."""


class PlanNotAcceptedError(RuntimeError):
    """A plan that is not ROOT-accepted was treated as execution authority."""


@dataclass(frozen=True)
class PreparedMemory:
    decision: dict[str, Any]
    envelope: dict[str, Any] | None


class MemoryRuntime:
    """Coordinate exact preparation, dispatch, omission, and outcomes."""

    def __init__(
        self,
        store: MemoryStore,
        *,
        privacy_policy: PrivacyPolicy | None = None,
        config: MemoryConfig | None = None,
    ) -> None:
        self.store = store
        self.privacy_policy = privacy_policy or PrivacyPolicy()
        self.config = config or resolve_config()

    def prepare(
        self,
        *,
        plan: Mapping[str, Any],
        task_card: Mapping[str, Any],
        lane_id: str,
        run_id: str,
        worktree_path: str,
        base_commit: str,
        mandatory_content: list[Mapping[str, Any]] | None = None,
        optional_content: list[Mapping[str, Any]] | None = None,
    ) -> PreparedMemory:
        contracts.validate_task_card(task_card)
        contracts.validate_plan(plan)
        if task_card["base_commit"] != base_commit:
            raise RuntimeError("task card base does not match the preparation base")
        mandatory = list(mandatory_content or [])
        optional = list(optional_content or [])
        if plan["state"] == "accepted":
            guard_mandatory({"task": task_card["task"], "plan": plan["content"],
                             "mandatory": mandatory, "configuration": asdict(self.config)}, self.privacy_policy)
        decision = contracts.make_decision(
            task_card,
            plan,
            strategy=self.config.strategy,
            configuration=asdict(self.config),
        )
        self.store.record_decision(decision)

        if plan["state"] != "accepted":
            return PreparedMemory(decision=decision, envelope=None)

        safe_optional = []
        omitted = []
        for item in optional:
            if sanitize_optional(item, self.privacy_policy) is None:
                identifier = item["id"]
                omitted.append(
                    "privacy-" + contracts.sha256_hex(identifier)
                    if worker_bound_finding(identifier, self.privacy_policy) else identifier
                )
            else:
                safe_optional.append(item)
        envelope = contracts.make_envelope(
            task_card=task_card,
            plan=plan,
            decision_id=decision["decision_id"],
            lane_id=lane_id,
            run_id=run_id,
            worktree_path=str(worktree_path),
            base_commit=base_commit,
            mandatory_content=mandatory,
            optional_content=safe_optional,
            omitted_content=omitted,
            strategy=self.config.strategy,
            configuration=asdict(self.config),
            privacy_policy=self.privacy_policy,
        )
        return PreparedMemory(decision=decision, envelope=envelope)

    def dispatch(
        self,
        envelope: Mapping[str, Any],
        launcher: Callable[[Mapping[str, Any]], Mapping[str, Any] | None],
    ) -> dict[str, Any]:
        if not isinstance(envelope, Mapping):
            raise RuntimeError("dispatch envelope must be an object")
        if envelope.get("plan_state") != "accepted":
            raise PlanNotAcceptedError("only a ROOT-accepted plan may dispatch")
        guard_mandatory(envelope, self._dispatch_privacy_policy(envelope))
        operation = contracts.make_operation(
            kind="dispatch",
            envelope=envelope,
            status="pending",
        )
        if envelope.get("schema") == contracts.FINAL_ENVELOPE_SCHEMA:
            existing, created = self.store.create_dispatch_intent(envelope, native_receipt_required=False)
        else:
            existing, created = self.store.create_operation(operation)
        if not created:
            if existing["status"] == "delivered":
                return existing
            raise DispatchAmbiguityError(
                f"{existing['status']} dispatch intent is unresolved; "
                "reconcile before retrying"
            )
        observed = launcher(envelope)
        if observed is None:
            ambiguous = contracts.make_operation(
                kind="dispatch",
                envelope=envelope,
                status="ambiguous",
            )
            self.store.record_operation(ambiguous)
            raise DispatchAmbiguityError(
                "ambiguous launch acknowledgement was lost; reconcile the exact existing invocation before retrying"
            )
        if not isinstance(observed, Mapping):
            raise RuntimeError("observed harness invocation must be an object or null")
        invocation_id = observed.get("invocation_id")
        if not isinstance(invocation_id, str) or not invocation_id:
            raise RuntimeError("observed harness invocation requires an invocation_id")
        delivered = contracts.make_operation(
            kind="dispatch",
            envelope=envelope,
            status="delivered",
            observed_invocation=observed,
        )
        return self.store.record_operation(delivered)

    def record_dispatch_intent(
        self, envelope: Mapping[str, Any], *,
        supersedes_rejected_attempt_id: str | None = None,
    ) -> dict[str, Any]:
        """Claim the exact durable final context before the native launcher runs.

        A replay of a pending or ambiguous intent raises visible ambiguity;
        only the first claim permits native spawn.
        This operation alone never means that a controller was spawned.
        """

        guard_mandatory(envelope, self._dispatch_privacy_policy(envelope))

        if envelope.get("schema") != contracts.FINAL_ENVELOPE_SCHEMA:
            raise RuntimeError("split native dispatch requires a finalized envelope")
        existing, created = self.store.create_dispatch_intent(
            envelope, supersedes_rejected_attempt_id=supersedes_rejected_attempt_id,
        )
        if not created:
            if existing["status"] == "delivered":
                return existing
            raise DispatchAmbiguityError(
                f"{existing['status']} dispatch already exists; "
                "reconcile it before any new launch"
            )
        return existing

    def record_observed_dispatch(
        self, envelope: Mapping[str, Any], native_receipt: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Join a spawned controller's exact native receipt to its prior intent."""
        if envelope.get("schema") == contracts.FINAL_ENVELOPE_SCHEMA:
            self.store._validate_native_receipt(native_receipt)
        operation = contracts.make_operation(
            kind="dispatch", envelope=envelope, status="delivered",
            observed_invocation=native_receipt,
        )
        return self.store.record_operation(operation)

    def mark_dispatch_ambiguous(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        """Mark a lost native acknowledgement; ownership stays reserved."""
        return self.store.record_operation(contracts.make_operation(
            kind="dispatch", envelope=envelope, status="ambiguous",
        ))

    def mark_dispatch_pre_spawn_failed(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        """Close an intent only when native spawn is proven not to have begun."""
        return self.store.record_operation(contracts.make_operation(
            kind="dispatch", envelope=envelope, status="failed_pre_spawn",
        ))

    def abandon_dispatch_intent(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        """Close a pre-dispatch intent with no invocation or outcome."""
        return self.store.record_operation(contracts.make_operation(
            kind="dispatch", envelope=envelope, status="abandoned",
        ))

    def _dispatch_privacy_policy(self, envelope: Mapping[str, Any]) -> PrivacyPolicy:
        """Use only provenance already persisted by the trusted finalizer."""
        context = envelope.get("final_context")
        if envelope.get("schema") != contracts.FINAL_ENVELOPE_SCHEMA or not isinstance(context, Mapping):
            return self.privacy_policy
        context_id = context.get("context_id")
        if not isinstance(context_id, str):
            return self.privacy_policy
        try:
            persisted = self.store.get_final_context(context_id)
        except (StoreError, ValueError):
            return self.privacy_policy
        if persisted != context or envelope.get("final_context_integrity") != persisted.get("integrity"):
            return self.privacy_policy
        hashes = set(self.privacy_policy.trusted_approval_hashes)
        trace = persisted.get("delivery_trace")
        if isinstance(trace, Mapping):
            for selected in trace.get("packed", []):
                if isinstance(selected, Mapping) and isinstance(selected.get("provenance"), Mapping):
                    for key in ("source_owner_approval_digest", "source_owner_compact_approval_digest"):
                        digest = selected["provenance"].get(key)
                        if isinstance(digest, str):
                            hashes.add(digest)
        return replace(self.privacy_policy, trusted_approval_hashes=tuple(sorted(hashes)))

    def reconcile_ambiguous_dispatch(
        self,
        envelope: Mapping[str, Any],
        observed_invocation: Mapping[str, Any],
    ) -> dict[str, Any]:
        if not isinstance(observed_invocation, Mapping):
            raise RuntimeError("observed harness invocation must be an object")
        invocation_id = observed_invocation.get("invocation_id")
        if not isinstance(invocation_id, str) or not invocation_id:
            raise RuntimeError("observed harness invocation requires an invocation_id")
        if envelope.get("schema") == contracts.FINAL_ENVELOPE_SCHEMA:
            self.store._validate_native_receipt(observed_invocation)
        operation = contracts.make_operation(
            kind="dispatch",
            envelope=envelope,
            status="ambiguous",
        )
        try:
            existing = self.store.get_operation(operation["operation_id"])
        except StoreError:
            raise DispatchAmbiguityError("no ambiguous dispatch exists to reconcile")
        if existing["status"] == "delivered":
            return existing
        if existing["status"] != "ambiguous":
            raise DispatchAmbiguityError("dispatch is not in an ambiguous state")
        return self.record_observed_dispatch(envelope, observed_invocation)

    def omit_optional_content(
        self, envelope: Mapping[str, Any], item_id: str
    ) -> dict[str, Any]:
        if not isinstance(envelope, Mapping):
            raise RuntimeError("dispatch envelope must be an object")
        optional = list(envelope.get("optional_content", []))
        if not any(item.get("id") == item_id for item in optional):
            raise RuntimeError(f"optional content item is not packed: {item_id}")
        remaining = [dict(item) for item in optional if item.get("id") != item_id]
        omitted = list(envelope.get("delivery", {}).get("omitted", []))
        if item_id not in omitted:
            omitted.append(item_id)
        revised = dict(envelope)
        revised["optional_content"] = remaining
        revised["optional_digest"] = contracts.sha256_hex(remaining)
        revised["delivery"] = {
            "mandatory": list(envelope.get("delivery", {}).get("mandatory", [])),
            "optional": [item["id"] for item in remaining],
            "omitted": omitted,
        }
        revised["content_hash"] = contracts.content_hash(revised)
        return revised

    def record_outcome(
        self,
        *,
        decision_id: str,
        plan_id: str,
        plan_digest: str,
        status: str,
        evidence_digest: str,
        linked_run_id: str,
    ) -> dict[str, Any]:
        try:
            decision = self.store.get_decision(decision_id)
        except Exception as exc:
            raise RuntimeError(f"outcome decision is not durable: {decision_id}") from exc
        if decision["plan_id"] != plan_id:
            raise RuntimeError("outcome plan does not match its decision")
        if decision["plan_digest"] != plan_digest:
            raise RuntimeError("outcome plan digest does not match its decision")
        matching_dispatches = [
            operation
            for operation in self.store.list_operations(decision_id)
            if operation["kind"] == "dispatch"
            and operation["status"] == "delivered"
            and operation["run_id"] == linked_run_id
        ]
        if not matching_dispatches:
            raise RuntimeError("outcome run has no exact delivered dispatch")
        outcome = contracts.make_outcome(
            decision_id=decision_id,
            plan_id=plan_id,
            plan_digest=plan_digest,
            status=status,
            evidence_digest=evidence_digest,
            linked_run_id=linked_run_id,
            task_card_digest=decision["task_card_digest"],
            objective_id=decision["objective_id"],
        )
        return self.store.record_outcome(outcome)

    def record_terminal_outcome(
        self, native_terminal_evidence: Mapping[str, Any], *,
        supersedes_outcome_id: str | None = None,
    ) -> dict[str, Any]:
        """Fix quality from lane 2's complete retained enhanced-parent bundle.

        Acceptance is stored as a separate fact; a forced acceptance cannot
        turn FAIL, BLOCKED, or terminal UNKNOWN into quality PASS.
        """
        contracts.validate_native_terminal_evidence(native_terminal_evidence)
        evidence = native_terminal_evidence
        decision = evidence["decision"]
        plan = evidence["accepted_plan"]
        outcome = contracts.make_outcome(
            decision_id=decision["decision_id"], plan_id=plan["plan_id"],
            plan_digest=plan["content_hash"],
            status=evidence["review"]["review_outcome"],
            evidence_digest=evidence["content_hash"], linked_run_id=evidence["run_id"],
            task_card_digest=evidence["task_card"]["content_hash"],
            objective_id=evidence["objective_id"],
            observed_at=evidence["review"]["reviewed_at"],
        )
        return self.store.record_terminal_outcome(
            outcome, evidence, supersedes_outcome_id=supersedes_outcome_id,
        )

    def record_rejected_native_attempt(
        self, native_terminal_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Retain one exact ROOT-rejected native run for explicit correction."""
        return self.store.record_rejected_native_attempt(native_terminal_evidence)

    # -- STEP-04 preparation, finalization, and safe dispatch ---------------

    def prepare_with_memory(self, **kwargs: Any) -> Any:
        """Delegate one bounded preparation to the STEP-04 coordinator."""

        from . import preparation
        from .config import resolve_limits

        limits = kwargs.pop("limits", None) or resolve_limits()
        service = preparation.PreparationService(
            store=self.store,
            config=kwargs.pop("config", self.config),
            limits=limits,
            privacy_policy=self.privacy_policy,
            clock=kwargs.pop("clock", None),
            registry=kwargs.pop("registry", None),
        )
        return service.prepare(**kwargs)

    def dispatch_finalized(
        self,
        *,
        envelope: Mapping[str, Any],
        context: Mapping[str, Any] | None,
        task_card: Mapping[str, Any],
        plan: Mapping[str, Any],
        lane_id: str,
        run_id: str,
        worktree_path: str,
        base_commit: str,
        checkpoint: str | None = None,
        execution_role: str | None = None,
        invocation_target: str | None = None,
        recipient: str | None = None,
        launcher: Callable[[Mapping[str, Any]], Mapping[str, Any] | None],
    ) -> dict[str, Any]:
        """Validate against the actual target, then dispatch exactly once."""

        from . import context as context_module

        try:
            if context is None:
                raise contracts.ContractError("finalized dispatch requires its domain-finalized context")
            for field, value in (
                ("checkpoint", checkpoint), ("execution_role", execution_role),
                ("invocation_target", invocation_target), ("recipient", recipient),
            ):
                contracts._require_canonical_identity(value, field)
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
            context_module.validate_final_context(
                context,
                envelope=envelope,
                task_card=task_card,
                plan=plan,
                lane_id=lane_id,
                run_id=run_id,
                base_commit=base_commit,
                worktree_path=str(worktree_path),
                checkpoint=checkpoint,
                execution_role=execution_role,
                invocation_target=invocation_target,
                recipient=recipient,
            )
        except Exception as exc:
            raise RuntimeError(f"finalized dispatch does not match its target: {exc}") from exc
        self.store.record_final_context(context, envelope_digest=envelope["content_hash"])
        return self.dispatch(envelope, launcher)

    def record_apc_child_operation(self, child_operation: Mapping[str, Any]) -> dict[str, Any]:
        return self.store.record_apc_child_operation(child_operation)

    def _try_get_operation(self, operation_id: str) -> dict[str, Any] | None:
        try:
            return self.store.get_operation(operation_id)
        except Exception:
            return None


__all__ = [
    "MemoryRuntime",
    "PreparedMemory",
    "RuntimeError",
    "DispatchAmbiguityError",
    "PlanNotAcceptedError",
]
