"""One bounded preparation for one exact objective, plan, and deadline.

The coordinator resolves every feature, network, route, strategy, stage, and
reserve value *before* any optional call, keeps ROOT acceptance distinct from
proposal completion, and hands one exact finalized context to the existing
product harness.  A late Level 0 admission permanently supersedes the old
packet and permits at most one bounded ordinary re-prepare.
"""

from __future__ import annotations

import json
import math
import re
import time
from dataclasses import asdict, dataclass, fields, replace
from typing import Any, Callable, Iterable, Mapping, Sequence

from . import apc, contracts, context as context_module, harness_bridge, templates
from .config import (
    DEEPER,
    PROBLEM_FOCUSED,
    STANDARD,
    MemoryConfig,
    NetworkResolver,
    PreparationLimits,
    resolve_config,
    resolve_limits,
)
from .privacy import PrivacyPolicy, safe_query_payload, sanitize_text
from .search import BoundedSearch, SearchStore
from .store import PreparationConflictError, StoreError


class PreparationError(RuntimeError):
    """A bounded preparation invariant was violated."""


class MandatoryStateFailure(PreparationError):
    """The exact mandatory state is inconsistent; ROOT recovery is required."""


class PlanAcceptanceError(PreparationError):
    """ROOT acceptance was not established for the exact plan revision."""


@dataclass(frozen=True)
class PreparationOutcome:
    mode: str
    decision: dict[str, Any] | None
    preparation: dict[str, Any] | None
    trace: dict[str, Any] | None
    disposition: dict[str, Any] | None
    plan: dict[str, Any]
    proposal: dict[str, Any] | None = None
    context: dict[str, Any] | None = None
    envelope: dict[str, Any] | None = None
    superseded: dict[str, Any] | None = None
    reason: str = ""

    @property
    def dispatchable(self) -> bool:
        return self.mode == "dispatchable" and self.envelope is not None

    @property
    def selected_candidates(self) -> list[dict[str, Any]]:
        if not self.trace:
            return []
        return [
            candidate
            for candidate in self.trace.get("candidates", [])
            if candidate.get("disposition") == "selected"
        ]


_MASKED_STRATEGIES = {"deeper"}


class PreparationService:
    """Own one logical decision from exact state to a safe dispatch."""

    def __init__(
        self,
        *,
        store: Any | None = None,
        config: MemoryConfig | None = None,
        limits: PreparationLimits | None = None,
        privacy_policy: PrivacyPolicy | None = None,
        clock: Callable[[], float] | None = None,
        registry: Sequence[templates.Template] | None = None,
        network_resolver: NetworkResolver | None = None,
    ) -> None:
        self.store = store
        self.config = config
        self.limits = limits or resolve_limits()
        self.privacy_policy = privacy_policy or PrivacyPolicy()
        self.clock = clock or time.monotonic
        self.registry = (
            tuple(registry) if registry is not None else templates.load_default_templates()
        )
        self.network_resolver = network_resolver or NetworkResolver()
        self.search = BoundedSearch(
            limits=self.limits, privacy_policy=self.privacy_policy, clock=self.clock
        )

    # -- exact state -------------------------------------------------------

    def _resolve(self, requested: Mapping[str, Any] | None) -> MemoryConfig:
        if self.config is not None and requested is None:
            return self.config
        return resolve_config(dict(requested or {}))

    def _captured_config(self, preparation: Mapping[str, Any]) -> MemoryConfig:
        """Continue the exact configuration one preparation captured.

        A restarted service, a retry, and a route correction all continue one
        logical decision, so its fixed strategy and feature gates come from
        the recorded preparation instead of whatever a new service instance
        currently resolves by default.
        """

        recorded = preparation["configuration"]
        return MemoryConfig(
            **{
                field.name: recorded[field.name]
                for field in fields(MemoryConfig)
                if field.name in recorded
            }
        )

    @staticmethod
    def _decision_config(decision: Mapping[str, Any]) -> MemoryConfig:
        """Require a complete fixed policy before using durable state as authority."""

        recorded = decision.get("configuration")
        names = {field.name for field in fields(MemoryConfig)}
        if not isinstance(recorded, Mapping) or set(recorded) != names:
            raise MandatoryStateFailure(
                "durable logical decision configuration is unreadable; recover mandatory state"
            )
        if any(
            not isinstance(recorded[name], bool)
            for name in names - {"strategy", "requested_strategy", "reason"}
        ) or any(
            not isinstance(recorded[name], str) or not recorded[name]
            for name in ("strategy", "requested_strategy", "reason")
        ) or recorded["strategy"] not in (STANDARD, PROBLEM_FOCUSED, DEEPER):
            raise MandatoryStateFailure(
                "durable logical decision configuration is invalid; recover mandatory state"
            )
        if recorded["strategy"] != decision["strategy"]:
            raise MandatoryStateFailure(
                "durable logical decision strategy conflicts with its configuration"
            )
        return MemoryConfig(**recorded)

    @staticmethod
    def _standalone_config(
        decision: Mapping[str, Any], request: Mapping[str, Any] | None = None,
    ) -> MemoryConfig:
        """Fill only absent decision fields without changing its recorded meaning."""

        recorded = decision.get("configuration")
        names = {field.name for field in fields(MemoryConfig)}
        boolean_names = names - {"strategy", "requested_strategy", "reason"}
        if not isinstance(recorded, Mapping) or not set(recorded) <= names:
            raise MandatoryStateFailure(
                "standalone logical decision policy is unreadable; recover mandatory state"
            )
        if (recorded.get("strategy") != decision.get("strategy")
                or recorded["strategy"] not in (STANDARD, PROBLEM_FOCUSED, DEEPER)
                or any(not isinstance(recorded[key], bool) for key in boolean_names & recorded.keys())
                or any(
                    not isinstance(recorded[key], str) or not recorded[key]
                    for key in ("requested_strategy", "reason") if key in recorded
                )):
            raise MandatoryStateFailure(
                "standalone logical decision policy is invalid; recover mandatory state"
            )
        raw = {"strategy": recorded["strategy"]}
        raw.update({key: recorded[key] for key in boolean_names if key in recorded})
        baseline = resolve_config(raw)
        if baseline.strategy != decision["strategy"] or any(
            getattr(baseline, key) != recorded[key]
            for key in boolean_names & recorded.keys()
        ):
            raise MandatoryStateFailure(
                "standalone logical decision strategy or feature gates conflict with "
                "their dependency rules; "
                "recover mandatory state"
            )
        requested_strategy = recorded.get("requested_strategy", decision["strategy"])
        if request and "strategy" in request and request["strategy"] != requested_strategy:
            raise MandatoryStateFailure(
                "explicit strategy policy conflicts with the captured logical decision; "
                "recover mandatory state or start a new decision"
            )
        candidate_raw = (
            {"strategy": recorded["strategy"]}
            if request and "all_features" in request else dict(raw)
        )
        for key in boolean_names | {"all_features"}:
            if request and key in request:
                candidate_raw[key] = request[key]
        candidate = resolve_config(candidate_raw)
        if candidate.strategy != baseline.strategy or any(
            getattr(candidate, key) != getattr(baseline, key)
            for key in boolean_names if key in recorded
        ) or any(
            request[key] != recorded[key]
            for key in ("requested_strategy", "reason")
            if request and key in request and key in recorded
        ):
            raise MandatoryStateFailure(
                "explicit feature policy conflicts with the captured logical decision; "
                "recover mandatory state or start a new decision"
            )
        return replace(
            candidate, requested_strategy=requested_strategy,
            reason=recorded.get("reason", candidate.reason),
        )

    @classmethod
    def _check_decision_preparation(
        cls, decision: Mapping[str, Any], captured: MemoryConfig,
    ) -> None:
        recorded = decision["configuration"]
        missing = {
            field.name: getattr(captured, field.name)
            for field in fields(MemoryConfig)
            if isinstance(getattr(captured, field.name), bool) and field.name not in recorded
        }
        expected = cls._standalone_config(decision, missing)
        if (captured.requested_strategy != expected.requested_strategy
                or captured.strategy not in (decision["strategy"], STANDARD)
                or any(
                    getattr(captured, field.name) != getattr(expected, field.name)
                    for field in fields(MemoryConfig)
                    if isinstance(getattr(captured, field.name), bool)
                )):
            raise MandatoryStateFailure(
                "first preparation conflicts with its immutable logical decision policy; "
                "recover mandatory state"
            )

    @staticmethod
    def _check_explicit_policy(
        request: Mapping[str, Any] | None, resolved: MemoryConfig, captured: MemoryConfig,
    ) -> None:
        if not request:
            return
        if request.get("all_features") is False and not captured.all_off:
            raise MandatoryStateFailure(
                "explicit feature policy conflicts with the captured logical decision; "
                "recover mandatory state or start a new decision"
            )
        if request.get("all_features") is True and any(
            getattr(resolved, field.name) != getattr(captured, field.name)
            for field in fields(MemoryConfig)
            if isinstance(getattr(captured, field.name), bool)
        ):
            raise MandatoryStateFailure(
                "explicit all_features policy conflicts with the captured logical decision; "
                "recover mandatory state or start a new decision"
            )
        requested = asdict(captured)
        for key in requested:
            if key not in request:
                continue
            value = (
                resolved.requested_strategy if key == "strategy"
                else getattr(resolved, key) if isinstance(requested[key], bool)
                else request[key]
            )
            captured_value = (
                captured.requested_strategy if key == "strategy" else requested[key]
            )
            if value != captured_value:
                raise MandatoryStateFailure(
                    f"explicit {key} policy conflicts with the captured logical decision; "
                    "recover mandatory state or start a new decision"
                )

    def _durable_preparation(self, preparation_id: str) -> dict[str, Any] | None:
        """Read one exact durable preparation record.

        `None` is returned only when no store is configured, because then no
        durable authority exists to consult.  With a configured store the
        exact record is required: an unreadable or missing record propagates
        instead of being reported as absence, since a failed read is not proof
        that no prior correction happened.
        """

        if self.store is None:
            return None
        return self.store.get_preparation(preparation_id)

    @staticmethod
    def _plan_state_error(exc: Exception, objective_id: str) -> MandatoryStateFailure:
        return MandatoryStateFailure(
            f"mandatory current-plan state is inconsistent for {objective_id!r}: {exc}"
        )

    @staticmethod
    def _blocked_budget_outcome(
        *, decision: Mapping[str, Any], plan: Mapping[str, Any], reason: str,
        preparation: Mapping[str, Any] | None = None,
    ) -> PreparationOutcome:
        return PreparationOutcome(
            mode="no_memory_continuation",
            decision=dict(decision),
            preparation=dict(preparation) if preparation is not None else None,
            trace=None,
            disposition=None,
            plan=dict(plan),
            reason=f"durable preparation budget blocked optional work: {reason}",
        )

    def _recover_budget(
        self, decision_id: str
    ) -> tuple[tuple[float, float] | None, int]:
        """Read the durable cutoff, current grant, and next attempt once.

        Restart, resume, and route correction all share one logical decision, so
        they must not hand out a fresh deadline.  The tightest recorded cutoff
        and its remaining-plus-spent grant retain elapsed cost without counting
        time removed by a shorter trusted cutoff as time already spent.
        """

        if self.store is None:
            return None, 1
        try:
            prior = self.store.list_preparations(decision_id)
        except Exception as exc:
            raise PreparationError(
                f"durable preparation budget lookup failed for {decision_id}: {exc}"
            ) from exc
        unreadable = f"durable preparation budget is unreadable for {decision_id}"
        if not isinstance(prior, list):
            raise PreparationError(unreadable)
        trusted: list[tuple[int, float, float]] = []
        attempts: list[int] = []
        for record in prior:
            if not isinstance(record, Mapping) or record.get("decision_id") != decision_id:
                raise PreparationError(unreadable)
            attempt = record.get("attempt")
            source = record.get("budget_source")
            spent = record.get("spent_seconds")
            if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
                raise PreparationError(unreadable)
            if (
                isinstance(spent, bool)
                or not isinstance(spent, (int, float))
                or not math.isfinite(spent)
                or spent < 0
            ):
                raise PreparationError(unreadable)
            attempts.append(attempt)
            if source == "unknown_time":
                if (
                    record.get("deadline_monotonic") is not None
                    or record.get("remaining_seconds") is not None
                ):
                    raise PreparationError(unreadable)
                continue
            deadline = record.get("deadline_monotonic")
            remaining = record.get("remaining_seconds")
            if (
                source != "trusted_deadline"
                or isinstance(deadline, bool)
                or not isinstance(deadline, (int, float))
                or not math.isfinite(deadline)
                or isinstance(remaining, bool)
                or not isinstance(remaining, (int, float))
                or not math.isfinite(remaining)
                or remaining < 0
            ):
                raise PreparationError(unreadable)
            grant = float(remaining + spent)
            if not math.isfinite(grant):
                raise PreparationError(unreadable)
            trusted.append((attempt, float(deadline), grant))
        if not trusted:
            return None, max(attempts, default=0) + 1
        _, tightest_deadline, granted_budget = min(
            trusted, key=lambda item: (item[1], item[0])
        )
        return (tightest_deadline, granted_budget), max(attempts) + 1

    def _logical_owner(
        self, identity: Mapping[str, str], request: Mapping[str, Any] | None,
        requested: MemoryConfig, explicit_network_mode: str | None,
        explicit_network_evidence: Mapping[str, Any] | None,
    ) -> tuple[dict[str, Any] | None, MemoryConfig | None, str | None, str | None, dict[str, Any] | None]:
        if self.store is None:
            return None, None, None, None, None
        try:
            decision = self.store.find_logical_decision(identity)
        except Exception as exc:
            raise MandatoryStateFailure(
                f"durable logical decision lookup failed; recover mandatory state: {exc}"
            ) from exc
        if decision is None:
            return None, None, None, None, None
        if any(decision.get(key) != value for key, value in identity.items()):
            raise MandatoryStateFailure(
                "durable logical decision identity conflicts with mandatory state; "
                "recover mandatory state"
            )
        try:
            prior = self.store.list_captured_preparations(decision["decision_id"])
        except Exception as exc:
            raise MandatoryStateFailure(
                f"captured logical decision lookup failed; recover mandatory state: {exc}"
            ) from exc
        if not isinstance(prior, list):
            raise MandatoryStateFailure(
                "captured logical decision preparations are unreadable; recover mandatory state"
            )
        # A valid standalone decision already owns its recorded policy. Only
        # absent fields may be filled for its first preparation; the decision
        # row itself remains immutable. All-off still takes the inherited path.
        if not prior:
            self._standalone_config(decision, request)
            return decision, None, None, None, None
        first = [packet for packet in prior if packet.get("attempt") == 1]
        if len(first) != 1:
            raise MandatoryStateFailure(
                "captured logical decision first preparation is unreadable; "
                "recover mandatory state"
            )
        captured = self._decision_config({
            "configuration": first[0].get("configuration"),
            "strategy": first[0].get("strategy"),
        })
        self._check_decision_preparation(decision, captured)
        normalized_request = (
            requested if not request or "all_features" in request
            else resolve_config({**asdict(captured), **request})
        )
        self._check_explicit_policy(request, normalized_request, captured)
        network_modes = set()
        rich_resolutions: list[dict[str, Any] | None] = []
        for packet in prior:
            if not isinstance(packet, Mapping) or any(
                packet.get(key) != value for key, value in identity.items()
            ) or packet.get("decision_id") != decision["decision_id"]:
                raise MandatoryStateFailure(
                    "captured logical decision preparation identity is inconsistent; "
                    "recover mandatory state"
                )
            packet_config = self._decision_config({
                "configuration": packet.get("configuration"),
                "strategy": packet.get("strategy"),
            })
            if packet.get("requested_strategy") != captured.requested_strategy or any(
                getattr(packet_config, field.name) != getattr(captured, field.name)
                for field in fields(MemoryConfig)
                if isinstance(getattr(captured, field.name), bool)
            ):
                raise MandatoryStateFailure(
                    "captured logical decision feature policy is inconsistent; "
                    "recover mandatory state"
                )
            if (packet_config.strategy != captured.strategy
                    and packet_config.strategy != STANDARD):
                raise MandatoryStateFailure(
                    "captured logical decision strategy is inconsistent; recover mandatory state"
                )
            if packet.get("attempt") == 1 and packet_config != captured:
                raise MandatoryStateFailure(
                    "first preparation conflicts with its captured preparation configuration; "
                    "recover mandatory state"
                )
            mode = packet.get("network_mode")
            if not isinstance(mode, str) or not mode:
                raise MandatoryStateFailure(
                    "captured logical decision network mode is unreadable; recover mandatory state"
                )
            network_modes.add(mode)
            rich_resolutions.append(packet.get("network_resolution"))
        if len(network_modes) > 1:
            raise MandatoryStateFailure(
                "captured logical decision network mode is inconsistent; recover mandatory state"
            )
        network_mode = next(iter(network_modes)) if network_modes else None
        captured_resolution = rich_resolutions[0]
        if any(item != captured_resolution for item in rich_resolutions):
            raise MandatoryStateFailure(
                "captured logical decision network resolution is inconsistent; recover mandatory state"
            )
        requested_mode = (
            captured_resolution["requested_mode"] if captured_resolution is not None
            else network_mode
        )
        if (explicit_network_mode is not None and explicit_network_mode != requested_mode
                or explicit_network_evidence is not None and (
                    captured_resolution is None
                    or dict(explicit_network_evidence) != captured_resolution["input_evidence"]
                )):
            raise MandatoryStateFailure(
                "explicit network policy conflicts with the captured logical decision; "
                "recover mandatory state or start a new decision"
            )
        source = prior[0].get("budget_source") if prior else None
        return decision, captured, network_mode, source, captured_resolution

    # -- preparation -------------------------------------------------------

    def _effective_config(
        self,
        resolved_config: MemoryConfig,
        *,
        failure_context: str | None,
        unknown_time: bool,
    ) -> MemoryConfig:
        """Apply the fixed-strategy gates before any optional call.

        A Problem-focused recipe uses the supplied real failure/recovery
        context and otherwise falls back to Standard under Standard's gates.
        Unknown time admits only the configured cheap fixed pass, so it masks
        Deeper as well.  Only the effective strategy changes; the requested
        strategy stays on the record for lineage.
        """

        strategy = resolved_config.strategy
        if strategy == PROBLEM_FOCUSED and not (failure_context or "").strip():
            return replace(
                resolved_config,
                strategy=STANDARD,
                reason="fallback to standard: problem-focused needs real failure context",
            )
        if strategy == DEEPER and unknown_time:
            return replace(
                resolved_config,
                strategy=STANDARD,
                reason="fallback to standard: unknown time permits only the cheap fixed pass",
            )
        return resolved_config

    def prepare(
        self,
        *,
        task_card: Mapping[str, Any],
        plan: Mapping[str, Any] | None,
        objective_id: str,
        route: str = "ordinary",
        request: Mapping[str, Any] | None = None,
        network_mode: str | None = None,
        network_evidence: Mapping[str, Any] | None = None,
        deadline: float | None = None,
        unknown_time: bool = False,
        failure_context: str | None = None,
        stores: Sequence[SearchStore] = (),
        bindings: Mapping[str, Any] | None = None,
        root_replan: Mapping[str, Any] | None = None,
        apc_binding: Mapping[str, Any] | None = None,
        apc_launcher: Callable[[Mapping[str, Any]], Mapping[str, Any] | None] | None = None,
        apc_cleanup: Mapping[str, Any] | None = None,
        mandatory_content: Iterable[Mapping[str, Any]] = (),
        optional_items: Iterable[Mapping[str, Any]] = (),
        freshness_check: Callable[[Mapping[str, Any]], bool] | None = None,
        lane_id: str | None = None,
        run_id: str | None = None,
        worktree_path: str | None = None,
        base_commit: str | None = None,
        checkpoint: str | None = None,
        execution_role: str | None = None,
        invocation_target: str | None = None,
        recipient: str | None = None,
        finalize: bool = False,
        required_sources: frozenset[tuple[str, str, str]] = frozenset(),
    ) -> PreparationOutcome:
        contracts.validate_task_card(task_card)
        if network_mode is not None or network_evidence is not None:
            # Validate even on the inherited/all-off path and before optional work.
            self.network_resolver.resolve(
                network_mode if network_mode is not None else "normal",
                evidence=network_evidence,
            )
        supplied_config = self._resolve(request)
        try:
            current_plan_state = contracts.classify_current_plan(
                plan, expected_objective_id=objective_id, expected_route=route
            )
        except contracts.PlanStateError as exc:
            raise self._plan_state_error(exc, objective_id) from exc
        # An absent plan is an ordinary fresh-planning state: it keeps its
        # exact identity in the record and never invents an inherited plan.
        plan_of_record = (
            dict(plan)
            if plan is not None
            else templates.fresh_plan(objective_id=objective_id, route=route)
        )
        if plan is None and finalize:
            raise PlanAcceptanceError("finalization needs an accepted exact plan")
        identity = contracts.logical_decision_identity(task_card, plan_of_record)

        now = self.clock()
        if deadline is not None:
            absolute_deadline = float(deadline)
            budget_source = "trusted_deadline"
        elif unknown_time:
            absolute_deadline = now + self.limits.standard_stage_seconds
            budget_source = "unknown_time"
        else:
            absolute_deadline = now + self.limits.default_deadline_seconds
            budget_source = "trusted_deadline"

        explicit_deadline = deadline is not None
        ownership_decision: dict[str, Any] | None = None

        def blocked_budget(
            reason: str, *, decision: Mapping[str, Any] | None = None,
            preparation: Mapping[str, Any] | None = None,
        ) -> PreparationOutcome:
            assert decision is not None or ownership_decision is not None
            outcome = self._blocked_budget_outcome(
                decision=decision or ownership_decision, plan=plan_of_record,
                preparation=preparation, reason=reason,
            )
            if not finalize or current_plan_state != "execution_accepted":
                return outcome
            supplied_optional = [dict(item) for item in optional_items]
            dependent = next((
                item for item in supplied_optional
                if context_module._plan_depends_on(plan_of_record, item)
            ), None)
            if dependent is not None:
                return replace(
                    outcome,
                    reason=(
                        outcome.reason + "; plan-affecting optional guidance "
                        f"{dependent.get('source_id', dependent.get('id', 'unknown'))} revision "
                        f"{dependent.get('revision_id', 'unknown')} blocks finalization; "
                        "ROOT must replan"
                    ),
                )
            if not all((checkpoint, execution_role, invocation_target, recipient)):
                # A failed budget cannot supply the missing dispatch target.
                # A raw plan-affecting claim remains unverified here: it must
                # not become dependency authority, and it cannot turn this
                # incomplete target into a finalized context.
                claimed = any(item.get("plan_affecting") is True for item in supplied_optional)
                detail = (
                    "unverified plan-affecting optional guidance and incomplete finalization target"
                    if claimed else "incomplete finalization target"
                )
                return replace(outcome, reason=f"{outcome.reason}; {detail}")
            # The accepted plan and caller-supplied mandatory state can still
            # be finalized. Record every omitted optional item and the failed
            # memory stage in the existing delivery trace.
            return self._finalize(
                outcome=outcome,
                task_card=task_card,
                plan=plan_of_record,
                lane_id=lane_id,
                run_id=run_id,
                worktree_path=worktree_path,
                base_commit=base_commit,
                checkpoint=checkpoint,
                execution_role=execution_role,
                invocation_target=invocation_target,
                recipient=recipient,
                mandatory_content=mandatory_content,
                optional_items=(),
                omitted=["optional-memory-unavailable", *supplied_optional],
                freshness_check=freshness_check,
                required_sources=required_sources,
            )

        proposed_deadline = absolute_deadline
        proposed_budget_source = budget_source
        proposed_unknown_time = unknown_time
        for _ in range(2):
            durable, captured, captured_network, captured_source, captured_resolution = self._logical_owner(
                identity, request, supplied_config, network_mode, network_evidence
            )
            standalone_policy = (
                self._standalone_config(durable, request)
                if durable is not None and captured is None else None
            )
            # The global all-off service setting can cover absent gates on a
            # strategy-only standalone row, but cannot disable a recorded gate.
            standalone_all_off = (
                standalone_policy is not None and request is None
                and supplied_config.all_off and durable["strategy"] == STANDARD
                and not any(
                    enabled for name, enabled in durable["configuration"].items()
                    if name not in ("strategy", "requested_strategy", "reason")
                )
            )
            if ((durable is None and supplied_config.all_off)
                    or (standalone_policy is not None and standalone_policy.all_off)
                    or standalone_all_off):
                if plan is None:
                    raise MandatoryStateFailure(
                        "all-off preparation still needs its exact plan state"
                    )
                return PreparationOutcome(
                    mode="inherited", decision=None, preparation=None, trace=None,
                    disposition=None, plan=dict(plan),
                    reason="all enhancements are off; the inherited harness path applies",
                )
            if captured is None:
                initial_policy = standalone_policy or supplied_config
                requested_config = self._effective_config(
                    initial_policy, failure_context=failure_context,
                    unknown_time=unknown_time,
                )
                if durable is not None:
                    ownership_decision = durable
                else:
                    ownership_decision = contracts.make_decision(
                        task_card, plan_of_record, strategy=requested_config.strategy,
                        configuration=asdict(requested_config),
                    )
                if self.store is not None and durable is None:
                    try:
                        id_taken = self.store.decision_id_exists(
                            ownership_decision["decision_id"]
                        )
                    except Exception as exc:
                        raise MandatoryStateFailure(
                            f"decision identifier lookup failed; recover mandatory state: {exc}"
                        ) from exc
                    if id_taken:
                        # The accepted identifier omits plan state/digest. Only
                        # a genuinely distinct exact plan needs a new suffix.
                        ownership_decision = contracts.make_decision(
                            task_card, plan_of_record, strategy=requested_config.strategy,
                            configuration=asdict(requested_config),
                            decision_id=contracts.sha256_hex({
                                "domain": "memory-decision-exact/v1",
                                "accepted_id": ownership_decision["decision_id"],
                                "identity": identity,
                            }),
                        )
            else:
                ownership_decision = durable
                assert captured is not None
                requested_config = captured
            network_resolution = (
                captured_resolution if captured_network is not None
                else contracts.network_resolution_record(self.network_resolver.resolve(
                    network_mode if network_mode is not None else "normal",
                    evidence=network_evidence,
                    context={
                        "objective_id": objective_id,
                        "task_card_digest": task_card["content_hash"],
                        "plan_id": plan_of_record["plan_id"],
                        "plan_digest": plan_of_record["content_hash"],
                        "decision_id": ownership_decision["decision_id"],
                        "route": route,
                        "plan_state": plan_of_record["state"],
                    },
                ))
            )
            effective_network_mode = (
                captured_network if captured_network is not None
                else network_resolution["effective_mode"]
            )
            try:
                # One decision owns one deadline. A later trusted cutoff may
                # tighten it, while a conflict retries against the new minimum.
                recovered, next_attempt = self._recover_budget(ownership_decision["decision_id"])
            except PreparationError as exc:
                return blocked_budget(str(exc))
            now = self.clock()  # The durable read itself spends trusted time.
            absolute_deadline = proposed_deadline
            budget_source = (
                "unknown_time" if captured_source == "unknown_time"
                else proposed_budget_source
            )
            unknown_time = captured_source == "unknown_time" or proposed_unknown_time
            resolved_config = requested_config
            if recovered is not None:
                durable_deadline, granted_budget = recovered
                durable_remaining = max(0.0, durable_deadline - now)
                spent_seconds = max(0.0, granted_budget - durable_remaining)
                absolute_deadline = (
                    min(absolute_deadline, durable_deadline)
                    if explicit_deadline else durable_deadline
                )
                budget_source = "trusted_deadline"
                unknown_time = False
            else:
                spent_seconds = 0.0
            remaining = max(0.0, absolute_deadline - now) if not unknown_time else 0.0
            resolved_config, admitted, admission_reason = self._admit_config(
                resolved_config, remaining=remaining, unknown_time=unknown_time
            )
            if unknown_time and next_attempt > 1:
                admitted = False
                admission_reason = (
                    "the logical decision already used its one unknown-time cheap pass"
                )
            stage_allowance = self._stage_allowance(
                resolved_config.strategy, remaining, unknown_time, admitted=admitted
            )
            decision = (
                durable if durable is not None else contracts.make_decision(
                    task_card, plan_of_record, strategy=resolved_config.strategy,
                    configuration=asdict(resolved_config),
                    decision_id=ownership_decision["decision_id"],
                )
            )
            preparation = contracts.make_preparation(
                task_card=task_card,
                decision_id=decision["decision_id"],
                objective_id=objective_id,
                route=route,
                plan_id=plan_of_record["plan_id"],
                plan_digest=plan_of_record["content_hash"],
                plan_state=plan_of_record["state"],
                current_plan_state=current_plan_state,
                strategy=resolved_config.strategy,
                requested_strategy=resolved_config.requested_strategy,
                configuration=asdict(resolved_config),
                network_mode=effective_network_mode,
                network_resolution=network_resolution,
                budget_source=budget_source,
                remaining_seconds=None if unknown_time else remaining,
                deadline_monotonic=None if unknown_time else absolute_deadline,
                execution_reserve_seconds=self.limits.execution_reserve_seconds,
                stage_allowance_seconds=stage_allowance,
                spent_seconds=spent_seconds,
                attempt=next_attempt,
            )
            if self.store is None:
                break
            try:
                preparation = self.store.record_preparation(
                    preparation, first_decision=decision if captured is None else None
                )
            except PreparationConflictError:
                continue
            except StoreError as exc:
                if captured is None:
                    raise MandatoryStateFailure(
                        f"logical decision first-writer recovery failed: {exc}"
                    ) from exc
                return self._blocked_budget_outcome(
                    decision=decision, plan=plan_of_record, reason=str(exc)
                )
            break
        else:
            return self._blocked_budget_outcome(
                decision=ownership_decision, plan=plan_of_record,
                reason="another caller changed the decision budget during the bounded retry",
            )

        accepted_precedence = current_plan_state == "execution_accepted"
        replan = self._validate_root_replan(root_replan)
        # Template selection and adaptation run only for an explicit ROOT
        # replan request.  An accepted plan is preserved, and a pending
        # candidate keeps its exact identity and state until ROOT decides.
        template_selection = replan is not None and not accepted_precedence
        # One canonical sanitized representation of the exact task, objective,
        # route, and real failure context is constructed once and shared: the
        # bounded search compares candidates against it, and plan production
        # recomputes every selected template's trusted comparable score against
        # the same record instead of sweeping the registry a second time.
        objective = self._canonical_objective(
            task_card, objective_id, route, failure_context
        )
        search = self._run_search(
            preparation=preparation,
            objective=objective,
            route=route,
            strategy=resolved_config.strategy,
            stores=stores,
            stage_allowance=stage_allowance,
            accepted_precedence=not template_selection,
            config=resolved_config,
            network_mode=effective_network_mode,
            budget_reason=admission_reason,
        )
        if "budget_error" in search:
            return blocked_budget(
                search["budget_error"], decision=decision, preparation=preparation,
            )

        if accepted_precedence:
            disposition = contracts.make_plan_disposition(
                decision_id=decision["decision_id"],
                objective_id=objective_id,
                route=route,
                branch="preserved_accepted",
                reason="a ROOT-accepted same-objective plan takes precedence over memory",
                root_acceptance={
                    "plan_id": plan["plan_id"],
                    "plan_digest": plan["content_hash"],
                    "accepted_by": plan.get("accepted_by", "ROOT"),
                },
            )
            if self.store is not None:
                self.store.record_plan_disposition(disposition)
            outcome = self._finish_planning(
                mode="planning",
                decision=decision,
                preparation=preparation,
                trace=search["trace"],
                disposition=disposition,
                plan=plan,
                proposal=None,
                reason="accepted plan preserved without template scoring",
            )
        elif current_plan_state == "candidate_review" and not template_selection:
            disposition = contracts.make_plan_disposition(
                decision_id=decision["decision_id"],
                objective_id=objective_id,
                route=route,
                branch="candidate_review",
                reason=(
                    "a pending candidate plan continues its existing ROOT review "
                    "without template selection, adaptation, or replacement"
                ),
                preserved_plan=plan,
            )
            if self.store is not None:
                self.store.record_plan_disposition(disposition)
            outcome = self._finish_planning(
                mode=self._planning_mode(search["trace"]),
                decision=decision,
                preparation=preparation,
                trace=search["trace"],
                disposition=disposition,
                plan=plan,
                proposal=None,
                reason=disposition["reason"],
            )
        elif current_plan_state == "absent":
            fresh = templates.fresh_plan(objective_id=objective_id, route=route)
            disposition = contracts.make_plan_disposition(
                decision_id=decision["decision_id"],
                objective_id=objective_id,
                route=route,
                branch="fresh",
                reason=(
                    "a truly absent current plan starts fresh ROOT planning and "
                    "review; no template selection or adaptation runs"
                ),
                fresh=fresh,
            )
            if self.store is not None:
                self.store.record_plan_disposition(disposition)
            outcome = self._finish_planning(
                mode=self._planning_mode(search["trace"]),
                decision=decision,
                preparation=preparation,
                trace=search["trace"],
                disposition=disposition,
                plan=fresh,
                proposal=None,
                reason=disposition["reason"],
            )
        else:
            disposition, produced_plan, proposal = self._produce_plan(
                ownership_decision_id=ownership_decision["decision_id"],
                decision=decision,
                objective_id=objective_id,
                route=route,
                objective=objective,
                trace=search["trace"],
                resolved_config=resolved_config,
                bindings=bindings,
                apc_binding=apc_binding,
                apc_launcher=apc_launcher,
                apc_cleanup=apc_cleanup,
                deadline=(
                    min(absolute_deadline, search["cutoff"])
                    if search["cutoff"] is not None else absolute_deadline
                ),
                unknown_time=unknown_time,
                root_replan=replan,
            )
            if self.store is not None:
                self.store.record_plan_disposition(disposition)
            mode = (
                "no_optional_memory"
                if search["trace"]["outcome"] == "no_optional_memory"
                else "planning"
            )
            outcome = self._finish_planning(
                mode=mode,
                decision=decision,
                preparation=preparation,
                trace=search["trace"],
                disposition=disposition,
                plan=produced_plan,
                proposal=proposal,
                reason=disposition["reason"],
            )
        if not finalize:
            return outcome
        if outcome.plan["state"] != "accepted":
            raise PlanAcceptanceError(
                "only an exact ROOT-accepted plan revision may be finalized "
                f"for dispatch; the current plan state is {outcome.plan['state']!r}"
            )
        return self._finalize(
            outcome=outcome,
            task_card=task_card,
            plan=outcome.plan,
            lane_id=lane_id,
            run_id=run_id,
            worktree_path=worktree_path,
            base_commit=base_commit,
            checkpoint=checkpoint,
            execution_role=execution_role,
            invocation_target=invocation_target,
            recipient=recipient,
            mandatory_content=mandatory_content,
            optional_items=optional_items,
            freshness_check=freshness_check,
            stores=stores,
            objective=objective,
            required_sources=required_sources,
        )


    # -- ROOT acceptance ---------------------------------------------------

    def accept(
        self,
        *,
        disposition: Mapping[str, Any],
        proposal: Mapping[str, Any],
        accepted_content: Any | None = None,
        accepted_plan_id: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Accept one exact revision; proposal completion is not acceptance."""

        try:
            accepted = contracts.accept_plan(
                proposal,
                accepted_plan_id=accepted_plan_id,
                accepted_content=accepted_content,
            )
        except contracts.ContractError as exc:
            raise PlanAcceptanceError(f"ROOT acceptance failed: {exc}") from exc
        revised = dict(disposition)
        revised["root_acceptance"] = {
            "plan_id": accepted["plan_id"],
            "plan_digest": accepted["content_hash"],
            "accepted_by": "ROOT",
            "revision": accepted["revision"],
            "source_branch": disposition["branch"],
            "revised": accepted_content is not None,
        }
        revised["content_hash"] = contracts.content_hash(revised)
        if self.store is not None:
            self.store.record_plan_disposition(revised)
        return revised, accepted

    def reject(
        self, *, disposition: Mapping[str, Any], reason: str
    ) -> dict[str, Any]:
        """Record a ROOT rejection; the attempt is not a cache hit."""

        revised = dict(disposition)
        attempts = list(revised.get("reuse_attempts", []))
        attempts.append(
            {
                "branch": revised["branch"],
                "status": "rejected",
                "reason": str(reason),
                "rejected_by": "ROOT",
            }
        )
        revised["reuse_attempts"] = attempts
        revised["branch"] = "fresh"
        revised["reason"] = f"ROOT rejected the proposal: {reason}"
        revised["fresh"] = revised.get("fresh") or {
            "steps": [],
            "state": "fresh",
        }
        revised["content_hash"] = contracts.content_hash(revised)
        if self.store is not None:
            self.store.record_plan_disposition(revised)
        return revised

    # -- Level 0 correction -------------------------------------------------

    def apply_level_zero(
        self,
        *,
        preparation: Mapping[str, Any],
        decision: Mapping[str, Any],
        task_card: Mapping[str, Any],
        plan: Mapping[str, Any],
        objective_id: str,
        route: str = "ordinary",
        stores: Sequence[SearchStore] = (),
        failure_context: str | None = None,
    ) -> PreparationOutcome:
        """Permanently supersede a topology packet after a late Level 0."""

        contracts.validate_preparation(preparation)
        if preparation.get("supersedes") or preparation.get("superseded_by"):
            return PreparationOutcome(
                mode="no_memory_continuation",
                decision=dict(decision),
                preparation=dict(preparation),
                trace=None,
                disposition=None,
                plan=dict(plan),
                reason=(
                    "a late Level 0 supersedes only one packet; continue explicitly "
                    "without memory instead of replenishing spent time"
                ),
            )
        # The durable record is the authority across restart: once this exact
        # packet has been superseded, another Level 0 verdict for it must not
        # launch a second ordinary re-prepare or hand out fresh time.  With a
        # configured store the exact durable record is required, and an
        # unreadable or missing record is an unknown durability state rather
        # than proof of no prior correction, so it fails closed into the same
        # explicit continuation without memory.
        durable: dict[str, Any] | None = None
        unreadable = ""
        if self.store is not None:
            try:
                durable = self._durable_preparation(preparation["preparation_id"])
            except Exception as exc:
                unreadable = f"{type(exc).__name__}: {exc}"
            if durable is None:
                return PreparationOutcome(
                    mode="no_memory_continuation",
                    decision=dict(decision),
                    preparation=dict(preparation),
                    trace=None,
                    disposition=None,
                    plan=dict(plan),
                    reason=(
                        "the exact packet's durable record is unreadable or "
                        "missing, so an earlier Level 0 correction cannot be "
                        "ruled out; continue explicitly without memory instead "
                        "of risking a second optional call"
                        + (f" ({unreadable})" if unreadable else "")
                    ),
                )
        if durable is not None and (
            durable.get("superseded_by") or durable.get("status") == "superseded"
        ):
            return PreparationOutcome(
                mode="no_memory_continuation",
                decision=dict(decision),
                preparation=durable,
                trace=None,
                disposition=None,
                plan=dict(plan),
                reason=(
                    "a late Level 0 supersedes only one packet; this exact packet "
                    "is already durably superseded, so continue explicitly without "
                    "memory instead of launching a second re-prepare"
                ),
            )
        packet = durable if durable is not None else preparation
        try:
            recovered, next_attempt = self._recover_budget(decision["decision_id"])
        except PreparationError as exc:
            return self._blocked_budget_outcome(
                decision=decision, plan=plan, preparation=packet, reason=str(exc)
            )
        if self.store is not None and recovered is None and packet["budget_source"] != "unknown_time":
            return self._blocked_budget_outcome(
                decision=decision, plan=plan, preparation=packet,
                reason="the exact decision has no readable durable cutoff",
            )
        replacement_id = contracts.sha256_hex(
            {
                "domain": "memory-preparation/v1",
                "supersedes": preparation["preparation_id"],
                "route_correction": "level-0",
            }
        )
        deadline_monotonic = recovered[0] if recovered is not None else packet.get("deadline_monotonic")
        if isinstance(deadline_monotonic, (int, float)):
            remaining = max(0.0, float(deadline_monotonic) - self.clock())
        else:
            remaining = packet.get("remaining_seconds")
        # The correction recomputes elapsed cost from the decision's tightest
        # durable grant, never from this possibly older packet's allowance.
        prior_spent = float(packet.get("spent_seconds") or 0.0)
        prior_remaining = packet.get("remaining_seconds")
        if recovered is not None:
            granted = recovered[1]
        elif isinstance(prior_remaining, (int, float)) and not isinstance(prior_remaining, bool):
            granted = prior_spent + float(prior_remaining)
        else:
            granted = None
        replacement_config = self._captured_config(packet)
        unknown_time = packet["budget_source"] == "unknown_time"
        remaining_value = None if remaining is None else max(0.0, float(remaining))
        if granted is None:
            spent = prior_spent
            remaining_value = prior_remaining
        else:
            spent = max(0.0, granted - float(remaining_value or 0.0))
        # One logical decision owns one captured configuration: the one
        # bounded ordinary re-prepare continues under the abandoned packet's
        # recorded fixed strategy and gates.  A restarted service's current
        # defaults never replace them; only the remaining-time admission may
        # demote the recipe once.
        replacement_config, admitted, admission_reason = self._admit_config(
            replacement_config,
            remaining=float(remaining_value or 0.0),
            unknown_time=unknown_time,
        )
        if unknown_time and self.store is not None and next_attempt > 1:
            admitted = False
            admission_reason = (
                "the logical decision already used its one unknown-time cheap pass"
            )
        stage_allowance = self._stage_allowance(
            replacement_config.strategy,
            float(remaining_value or 0.0),
            unknown_time,
            admitted=admitted,
        )
        replacement = contracts.make_preparation(
            task_card=task_card,
            decision_id=decision["decision_id"],
            objective_id=objective_id,
            route=route,
            plan_id=plan["plan_id"],
            plan_digest=plan["content_hash"],
            plan_state=plan["state"],
            current_plan_state=packet["current_plan_state"],
            strategy=replacement_config.strategy,
            requested_strategy=replacement_config.requested_strategy,
            configuration=asdict(replacement_config),
            network_mode=packet["network_mode"],
            network_resolution=packet.get("network_resolution"),
            budget_source=packet["budget_source"],
            remaining_seconds=remaining_value,
            deadline_monotonic=deadline_monotonic,
            execution_reserve_seconds=packet["execution_reserve_seconds"],
            stage_allowance_seconds=stage_allowance,
            spent_seconds=spent,
            attempt=next_attempt,
            preparation_id=replacement_id,
            supersedes=preparation["preparation_id"],
            route_correction={"level": 0, "action": "bounded_ordinary_reprepare"},
        )
        old = dict(packet)
        old["status"] = "superseded"
        old["superseded_by"] = replacement_id
        if self.store is not None:
            try:
                replacement = self.store.record_preparation(replacement)
                old = self.store.get_preparation(preparation["preparation_id"])
            except (PreparationConflictError, StoreError) as exc:
                return self._blocked_budget_outcome(
                    decision=decision, plan=plan, preparation=packet, reason=str(exc)
                )
        else:
            old["content_hash"] = contracts.content_hash(old)
        # The one bounded ordinary re-prepare obeys the same admission rule and
        # never replenishes the time already spent on the abandoned packet.
        search = self._run_search(
            preparation=replacement,
            objective=self._canonical_objective(
                task_card, objective_id, route, failure_context
            ),
            route=route,
            strategy=replacement_config.strategy,
            stores=stores,
            stage_allowance=stage_allowance,
            accepted_precedence=False,
            config=replacement_config,
            network_mode=replacement["network_mode"],
            budget_reason=admission_reason,
        )
        if "budget_error" in search:
            return self._blocked_budget_outcome(
                decision=decision, plan=plan, preparation=replacement,
                reason=search["budget_error"],
            )
        disposition = contracts.make_plan_disposition(
            decision_id=decision["decision_id"],
            objective_id=objective_id,
            route=route,
            branch="fresh",
            reason=(
                "the superseded topology packet is permanently non-dispatchable; "
                "one bounded ordinary re-prepare was performed"
            ),
            fresh=(
                dict(plan)
                if plan["state"] == "fresh"
                else templates.fresh_plan(objective_id=objective_id, route=route)
            ),
        )
        if self.store is not None:
            self.store.record_plan_disposition(disposition)
        return PreparationOutcome(
            mode="planning",
            decision=dict(decision),
            preparation=replacement,
            trace=search["trace"],
            disposition=disposition,
            plan=dict(plan),
            superseded=old,
            reason=disposition["reason"],
        )


    # -- internals ---------------------------------------------------------

    def _validate_root_replan(
        self, request: Mapping[str, Any] | None
    ) -> dict[str, Any] | None:
        """Return one explicit ROOT replan request, or `None`.

        Only ROOT may replace or extend a current plan.  Any other replan
        request is a mandatory-state failure rather than a quiet replan.
        """

        if request is None:
            return None
        if not isinstance(request, Mapping):
            raise MandatoryStateFailure("a ROOT replan request must be an object")
        if request.get("requested_by") != "ROOT":
            raise MandatoryStateFailure(
                "only an explicit ROOT replan request may replace a current plan"
            )
        return dict(request)

    @staticmethod
    def _planning_mode(trace: Mapping[str, Any] | None) -> str:
        if trace is None or trace.get("outcome") == "no_optional_memory":
            return "no_optional_memory"
        return "planning"

    def _recipe_fits(self, strategy: str, remaining: float) -> bool:
        """A recipe is admitted only when its full configured maximum and the
        positive execution reserve both fit the trusted remaining time."""

        return remaining >= (
            self.limits.stage_seconds_for(strategy) + self.limits.execution_reserve_seconds
        )

    def _admit_config(
        self,
        resolved_config: MemoryConfig,
        *,
        remaining: float,
        unknown_time: bool,
    ) -> tuple[MemoryConfig, bool, str]:
        """Admit one recipe, demoting at most once to Standard.

        A recipe is never truncated to whatever time happens to remain: it is
        admitted whole or demoted.  When neither the requested recipe nor
        Standard fits, no optional call is made and Standard stays the recorded
        fallback recipe.  Demotion is monotonic and never replenishes budget.
        """

        if unknown_time:
            # Unknown time already masks to the cheap fixed pass, which is the
            # bounded case; the execution reserve does not mask it further.
            return resolved_config, True, ""
        if self._recipe_fits(resolved_config.strategy, remaining):
            return resolved_config, True, ""
        if resolved_config.strategy != STANDARD and self._recipe_fits(STANDARD, remaining):
            return (
                replace(
                    resolved_config,
                    strategy=STANDARD,
                    reason=(
                        "fallback to standard: the requested recipe needs its full "
                        "bound plus the execution reserve"
                    ),
                ),
                True,
                "one bounded demotion to standard: the requested recipe did not fit",
            )
        return (
            replace(
                resolved_config,
                strategy=STANDARD,
                reason=(
                    "no optional memory: neither the requested recipe nor standard "
                    "fits the trusted remaining time"
                ),
            ),
            False,
            (
                "the full configured recipe bound and the positive execution reserve "
                "do not fit the trusted remaining time; no optional call was made"
            ),
        )

    def _stage_allowance(
        self, strategy: str, remaining: float, unknown_time: bool, *, admitted: bool = True
    ) -> float:
        if not admitted:
            return 0.0
        if unknown_time:
            return min(
                self.limits.stage_seconds_for(strategy),
                self.limits.unknown_time_budget_seconds,
            )
        # An admitted recipe receives its full configured maximum.
        return self.limits.stage_seconds_for(strategy)

    def _search_policy_context(
        self, preparation: Mapping[str, Any] | None, route: str,
    ) -> dict[str, str] | None:
        """Attribute a query only to an exact preparation in this MemoryStore."""

        if self.store is None or preparation is None:
            return None
        preparation_id = preparation.get("preparation_id")
        if (not isinstance(preparation_id, str)
                or re.fullmatch(r"[0-9a-f]{64}", preparation_id) is None):
            return None
        try:
            durable = self.store.get_preparation(preparation_id)
        except StoreError:
            return None
        if durable != preparation or durable["route"] != route:
            return None
        return {
            "schema": "memory-search-policy-context/v1",
            "preparation_id": durable["preparation_id"],
            "preparation_digest": durable["content_hash"],
        }

    def _run_search(
        self,
        *,
        preparation: Mapping[str, Any],
        objective: Mapping[str, Any],
        route: str,
        strategy: str,
        stores: Sequence[SearchStore],
        stage_allowance: float,
        accepted_precedence: bool,
        config: MemoryConfig,
        network_mode: str,
        budget_reason: str = "",
    ) -> dict[str, Any]:
        enabled: list[SearchStore] = []
        attempts: list[dict[str, Any]] = []
        policy_context = self._search_policy_context(preparation, route)
        for store in stores:
            flag = {
                "historical_evidence": config.experience_read,
                "procedure": config.atlas_shared_retrieval or config.generated_skill_use,
                "template": config.template_memory,
            }.get(store.kind, False)
            disabled_reason = "the resolved configuration disables this store"
            if store.source_kind == "curated_local_procedure":
                # A local curated/builtin procedure store performs no
                # shared/remote call, so the accepted local trust path stays
                # eligible even when Atlas shared retrieval and generated-skill
                # use are both off, and restricted_local never suppresses it.
                flag = True
            elif store.source_kind == "generated_local_procedure":
                # Generated-origin local guidance is separately gated: with
                # generated_skill_use off the source makes no query at all, so
                # no delivery can be filtered out of work already performed.
                flag = config.generated_skill_use
                disabled_reason = (
                    "generated_skill_use is disabled: a generated-origin local "
                    "procedure source performs no query"
                )
            elif store.source_kind == "everos_generated_skill":
                # The future EverOS generated-skill procedure store performs a
                # real remote call, so it is admitted only while generated-skill
                # use is on: ``atlas_shared_retrieval`` is the shared-retrieval
                # authority for Atlas procedures and never enables this source
                # by itself.
                flag = config.generated_skill_use
                disabled_reason = (
                    "generated_skill_use is disabled: the EverOS generated-skill "
                    "source performs no query"
                )
                if flag and network_mode == "restricted_local":
                    # Restricted-local mode must not begin the shared/remote
                    # task-path call at all; the suppression happens here,
                    # before the call, instead of filtering its output after
                    # work already occurred.
                    flag = False
                    disabled_reason = (
                        "restricted-local mode never begins a shared/remote "
                        "retrieval call"
                    )
            elif flag and store.requires_network:
                # A remote procedure store carries shared-retrieval authority:
                # only atlas_shared_retrieval enables it, never the local
                # generated-skill analogue, so an Atlas store is never queried
                # just because generated_skill_use is true.
                if store.kind == "procedure":
                    flag = config.atlas_shared_retrieval
                if flag and network_mode == "restricted_local":
                    # Restricted-local mode must not begin the shared/remote
                    # task-path call at all; the suppression happens here,
                    # before the call, instead of filtering its output after
                    # work already occurred.
                    flag = False
                    disabled_reason = (
                        "restricted-local mode never begins a shared/remote "
                        "retrieval call"
                    )
            if accepted_precedence and store.kind == "template":
                # Template selection never runs when an accepted or candidate
                # plan has precedence.
                flag = False
            if flag and self.store is not None and store.requires_network and policy_context is None:
                flag = False
                disabled_reason = "network store has no safe durable policy context"
            if not flag:
                attempts.append(
                    {
                        "store_id": store.store_id,
                        "kind": store.kind,
                        "status": "disabled",
                        "candidates": 0,
                        "reason": disabled_reason,
                    }
                )
                continue
            enabled.append(store)
        unknown_time = preparation["budget_source"] == "unknown_time"
        rounds = 1 if unknown_time else self.limits.rounds_for(strategy)
        if enabled and stage_allowance > 0 and self.store is not None:
            # Another connection can tighten the decision's durable minimum
            # after this packet is written. Recheck at the actual optional
            # admission boundary, then charge the read against that minimum.
            try:
                recovered, _ = self._recover_budget(preparation["decision_id"])
            except PreparationError as exc:
                return {"budget_error": str(exc)}
            if recovered is None and not unknown_time:
                return {"budget_error": "the exact decision has no readable durable cutoff"}
        else:
            recovered = None
        if unknown_time:
            stage_allowance = min(
                stage_allowance, self.limits.unknown_time_budget_seconds
            )
        elif stage_allowance > 0:
            # The read itself, durable writes, and objective construction all
            # spend trusted time before any optional store call.
            cutoff = float(preparation["deadline_monotonic"])
            if recovered is not None:
                cutoff = min(cutoff, recovered[0])
            remaining = max(0.0, cutoff - self.clock())
            if remaining < stage_allowance + preparation["execution_reserve_seconds"]:
                stage_allowance = 0.0
                budget_reason = (
                    "the full optional stage and positive execution reserve no longer "
                    "fit the trusted deadline"
                )
        if not enabled or stage_allowance <= 0:
            for store in enabled:
                attempts.append(
                    {
                        "store_id": store.store_id,
                        "kind": store.kind,
                        "status": "unattempted-by-budget",
                        "candidates": 0,
                        "reason": budget_reason
                        or "no remaining time inside the resolved stage allowance",
                    }
                )
            result = self.search.run(
                objective=objective, stores=[], route=route, stage_seconds=0.0, rounds=1
            )
            trace = contracts.make_search_trace(
                preparation_id=preparation["preparation_id"],
                strategy=strategy,
                rounds=0,
                attempts=attempts,
                candidates=[],
                selected_ids=[],
                delivered_ids=[],
                outcome="no_optional_memory",
            )
        else:
            result = self.search.run(
                objective=objective,
                stores=enabled,
                route=route,
                stage_seconds=stage_allowance,
                rounds=rounds,
                policy_context=policy_context,
            )
            trace = contracts.make_search_trace(
                preparation_id=preparation["preparation_id"],
                strategy=strategy,
                rounds=result.rounds,
                attempts=attempts + result.attempts,
                candidates=result.candidates,
                selected_ids=result.delivered,
                delivered_ids=result.delivered,
                outcome=result.outcome,
            )
        if self.store is not None:
            self.store.record_search_trace(trace)
            for candidate in trace["candidates"]:
                self.store.record_search_candidate(
                    preparation["preparation_id"], candidate
                )
        return {
            "trace": trace,
            "selected": list(result.delivered),
            "cutoff": cutoff if not unknown_time and stage_allowance > 0 else None,
        }

    def _unresolved_child_operation(self, decision_id: str) -> dict[str, Any] | None:
        """Return one exact child of this decision that is still unresolved.

        Launch intent, a live child, an ambiguous acknowledgement, and pending
        cleanup all keep ownership unresolved until the exact child is
        reconciled, so nothing may relaunch while one is visible.
        """

        if self.store is None:
            return None
        try:
            operations = self.store.list_apc_child_operations(decision_id)
        except Exception:
            return None
        for operation in reversed(list(operations)):
            if operation.get("status") in contracts.APC_CHILD_UNRESOLVED_STATUSES:
                return dict(operation)
        return None

    def _canonical_objective(
        self,
        task_card: Mapping[str, Any],
        objective_id: str,
        route: str,
        failure_context: str | None,
    ) -> dict[str, Any]:
        """Build the one canonical sanitized representation both phases share.

        The exact declared task text, the exact objective identity, the
        resolved route, and the real failure context are the only inputs, so
        the bounded search and plan production compare candidates against
        literally the same representation instead of two separately
        constructed ones.

        The task, objective, and failure text are sanitized with the active
        privacy policy *before* tokenization, so a configured secret contributes
        no token to the bounded store query and no weight to the trusted reuse
        score: the one sanitized record is what both phases consume.  Only this
        query/scoring text is sanitized; durable records keep the exact
        objective identity.
        """

        task_text = sanitize_text(
            str(task_card.get("task") or "").strip(), self.privacy_policy
        )
        # The objective identity is caller-supplied too, so it is sanitized for
        # this query/scoring text exactly like the task and failure context.
        # The durable record keeps the exact objective id untouched.
        objective_text = sanitize_text(str(objective_id), self.privacy_policy)
        context_text = sanitize_text(failure_context or "", self.privacy_policy)
        parts = [part for part in (task_text, objective_text) if part]
        return templates.objective_representation(
            " ".join(parts) or objective_text or str(objective_id),
            route=route,
            limits=self.limits,
            failure_context=context_text or None,
        )

    def _registry_template(
        self, template_id: str, revision_id: str
    ) -> templates.Template | None:
        """Return the exact explicit registry record for one selected identity.

        Rejoining compares the selected candidate's exact logical id with the
        immutable registry template id and its delivered revision with the
        registry version, so a version bump cannot silently satisfy an older
        selection.
        """

        version_text = revision_id[1:] if revision_id[:1] in {"v", "V"} else revision_id
        for template in self.registry:
            if template.template_id != template_id:
                continue
            if str(template.version) == version_text:
                return template
        return None

    def _rejoin_selected_templates(
        self,
        *,
        objective: Mapping[str, Any],
        route: str,
        trace: Mapping[str, Any] | None,
        attempts: list[dict[str, Any]],
    ) -> tuple[list[tuple[templates.Template, float]], bool]:
        """Rejoin every selected template to the explicit immutable registry.

        Only template candidates this preparation trace actually *selected*
        are considered, in their delivered rank order.  Each one must match
        one explicit registry record by exact logical id, version, and content
        digest; its trusted comparable score is then recomputed from the same
        canonical representation the bounded search used, and the configured
        calibrated thresholds decide the band.  A mismatched, forged, or
        inapplicable record is rejected with its reason and is never replaced
        by a looser registry sweep.  The returned flag is true only when every
        selected candidate rejoined with a trusted comparable score, so the
        fresh-planning reason never blames the registry for a later direct-fill
        or adaptation failure.  The rejoined shortlist is ordered by that
        recomputed trusted score, never by the raw score the store supplied.
        """

        selected = [
            candidate
            for candidate in (trace or {}).get("candidates", [])
            if isinstance(candidate, Mapping)
            and candidate.get("kind") == "template"
            and candidate.get("disposition") == "selected"
        ]
        shortlist: list[tuple[templates.Template, float]] = []
        for candidate in selected:
            logical_id = candidate.get("logical_id")
            revision_id = candidate.get("revision_id")
            if not isinstance(logical_id, str) or not isinstance(revision_id, str):
                continue
            template = self._registry_template(logical_id, revision_id)
            if template is None:
                attempts.append(
                    {
                        "template_id": logical_id,
                        "branch": "shortlist",
                        "status": "rejected",
                        "reason": (
                            "the selected template does not rejoin the explicit "
                            "immutable registry by exact id/version"
                        ),
                    }
                )
                continue
            if candidate.get("payload_digest") != contracts.sha256_hex(
                template.to_record()
            ):
                attempts.append(
                    {
                        "template_id": template.template_id,
                        "branch": "shortlist",
                        "status": "rejected",
                        "reason": (
                            "the selected template content digest does not match "
                            "the immutable registry record"
                        ),
                    }
                )
                continue
            representation = templates.template_representation(
                template, limits=self.limits
            )
            if not template.representation_declared or not templates.representations_comparable(
                objective, representation
            ):
                attempts.append(
                    {
                        "template_id": template.template_id,
                        "branch": "shortlist",
                        "status": "rejected",
                        "reason": (
                            "the selected template representation is not comparable "
                            "with the canonical objective representation"
                        ),
                    }
                )
                continue
            if route not in template.routes:
                attempts.append(
                    {
                        "template_id": template.template_id,
                        "branch": "shortlist",
                        "status": "rejected",
                        "reason": (
                            f"the selected template is not applicable to route {route!r}"
                        ),
                    }
                )
                continue
            score = templates.score_representations(
                templates.projected_objective(objective), representation
            )
            if score < self.limits.near_match_threshold:
                attempts.append(
                    {
                        "template_id": template.template_id,
                        "branch": "shortlist",
                        "status": "rejected",
                        "reason": (
                            "the selected template scores below the configured "
                            "near-match threshold in the canonical representation"
                        ),
                    }
                )
                continue
            shortlist.append((template, score))
        rejoined = len(shortlist) == len(selected) and bool(selected)
        # Only the rejoined selected candidates are ordered here, by the
        # recomputed trusted score.  The delivered trace order can be set by
        # store-supplied raw scores, so it must not decide which selected
        # template is proposed; the rejoin order of equally scored candidates
        # is the deterministic tie.  No unselected registry entry is added.
        shortlist.sort(key=lambda item: -item[1])
        return shortlist, rejoined

    @staticmethod
    def _no_selected_template_reason(trace: Mapping[str, Any] | None) -> str:
        """Explain honestly why no template candidate could be selected."""

        attempts = [
            entry
            for entry in (trace or {}).get("attempts", [])
            if isinstance(entry, Mapping) and entry.get("kind") == "template"
        ]
        if not attempts:
            return (
                "no bounded template store ran in this preparation trace; fresh "
                "ROOT planning applies without template reuse or adaptation"
            )
        statuses = {str(entry.get("status")) for entry in attempts}
        if "unattempted-by-budget" in statuses:
            return (
                "the optional stage admitted no template search budget; fresh ROOT "
                "planning applies without template reuse or adaptation"
            )
        if "disabled" in statuses:
            return (
                "template selection was disabled for this preparation; fresh ROOT "
                "planning applies without template reuse or adaptation"
            )
        return (
            "the bounded eligible shortlist selected no template candidate; fresh "
            "ROOT planning applies without template reuse or adaptation"
        )

    def _fresh_plan_reason(
        self,
        *,
        resolved_config: MemoryConfig,
        trace: Mapping[str, Any] | None,
        shortlist: Sequence[tuple[templates.Template, float]],
        attempts: Sequence[Mapping[str, Any]],
        rejoined: bool,
    ) -> str:
        """Explain honestly why fresh ROOT planning applies.

        A nonempty shortlist is reported first: a selected template did rejoin
        the registry with a trusted comparable score, so a later direct-fill or
        adaptation failure is never misreported as a registry mismatch.
        """

        if not resolved_config.template_memory:
            return (
                "template memory is disabled; no template reuse or adaptation runs "
                "and normal fresh ROOT planning applies"
            )
        if shortlist:
            return (
                "no selected template produced a usable direct fill or permitted "
                "adaptation; normal fresh ROOT planning applies"
            )
        if attempts and not rejoined:
            return (
                "no selected template rejoined the explicit immutable registry with "
                "a trusted comparable score; normal fresh ROOT planning applies"
            )
        return self._no_selected_template_reason(trace)


    def _produce_plan(
        self,
        *,
        ownership_decision_id: str,
        decision: Mapping[str, Any],
        objective_id: str,
        route: str,
        objective: Mapping[str, Any],
        trace: Mapping[str, Any] | None,
        resolved_config: MemoryConfig,
        bindings: Mapping[str, Any] | None,
        apc_binding: Mapping[str, Any] | None,
        apc_launcher: Callable[[Mapping[str, Any]], Mapping[str, Any] | None] | None,
        apc_cleanup: Mapping[str, Any] | None,
        deadline: float,
        unknown_time: bool,
        root_replan: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
        """Produce a bounded reuse proposal from the selected shortlist only.

        BEHAVIOR-01's bounded preparation trace is the only source of template
        candidates: a template the eligible shortlist did not actually select
        is never considered, so a disabled feature, a denied optional stage, a
        missing store, or a filtered candidate cannot be routed around by a
        second registry sweep.  Each selected candidate must rejoin the
        explicit immutable registry by exact id, version, and content digest,
        and its trusted comparable score is recomputed from the same canonical
        representation the search used; the configured calibrated thresholds
        decide the band.  A mismatch is rejected, never downgraded to a looser
        registry fallback.
        """

        reuse_attempts: list[dict[str, Any]] = []
        rejoined = False
        if resolved_config.template_memory:
            shortlist, rejoined = self._rejoin_selected_templates(
                objective=objective,
                route=route,
                trace=trace,
                attempts=reuse_attempts,
            )
        else:
            shortlist = []

        for template, score in shortlist:
            if score < self.limits.direct_fill_threshold:
                continue
            if not bindings:
                reuse_attempts.append(
                    {
                        "template_id": template.template_id,
                        "branch": "direct_fill",
                        "status": "unavailable",
                        "reason": "no trusted typed bindings were supplied",
                    }
                )
                continue
            try:
                proposal = templates.direct_fill_typed(
                    template, bindings, objective_id=objective_id, route=route
                )
            except templates.TemplateError as exc:
                # A typed validation failure keeps every lower-ranked selected
                # eligible candidate available; field trust is never weakened.
                reuse_attempts.append(
                    {
                        "template_id": template.template_id,
                        "branch": "direct_fill",
                        "status": "rejected",
                        "reason": str(exc),
                    }
                )
                continue
            disposition = contracts.make_plan_disposition(
                decision_id=decision["decision_id"],
                objective_id=objective_id,
                route=route,
                branch="direct_fill",
                reason=(
                    "a selected comparable template with complete typed fields "
                    "was directly filled"
                ),
                template={
                    "template_id": template.template_id,
                    "version": template.version,
                    "family": template.family,
                    "digest": contracts.sha256_hex(template.to_record()),
                    "score": score,
                },
                proposal=proposal,
                reuse_attempts=reuse_attempts,
                root_replan=root_replan,
            )
            # A direct fill is the proposal itself; it launches no APC child.
            return disposition, proposal, None

        near = [
            (template, score)
            for template, score in shortlist
            if self.limits.near_match_threshold
            <= score
            < self.limits.direct_fill_threshold
        ]
        adaptation_available = bool(
            resolved_config.apc
            and resolved_config.light_adaptation
            and not unknown_time
            and apc_binding is not None
            and apc_launcher is not None
        )
        if near and adaptation_available:
            template, score = near[0]
            record = template.to_record()
            pending = self._unresolved_child_operation(ownership_decision_id)
            remaining = deadline - self.clock()
            if pending is not None:
                # An earlier child may still be live and its cleanup is not
                # proven.  Refuse to relaunch anything until that exact child is
                # reconciled; unresolved ownership is reported, never hidden
                # behind a budget excuse.
                reuse_attempts.append(
                    {
                        "template_id": template.template_id,
                        "branch": "apc_proposal",
                        "status": "rejected",
                        "reason": (
                            f"{pending['status']} APC child already exists; "
                            "reconcile the exact child before any retry"
                        ),
                    }
                )
            elif remaining <= self.limits.execution_reserve_seconds:
                reuse_attempts.append(
                    {
                        "template_id": template.template_id,
                        "branch": "apc_proposal",
                        "status": "unattempted-by-budget",
                        "reason": "no usable adaptation time remains inside the deadline",
                    }
                )
            else:
                try:
                    request = apc.make_apc_request(
                        template=record,
                        parent_decision_id=decision["decision_id"],
                        parent_objective_id=objective_id,
                        permitted_edits=list(template.allowed_edits),
                        binding=apc_binding,
                    )
                    attempt = harness_bridge.run_apc_child(
                        request=request,
                        template_record=record,
                        launcher=apc_launcher,
                        store=self.store,
                        limits=self.limits,
                        clock=self.clock,
                        deadline=deadline - self.limits.execution_reserve_seconds,
                        cleanup=apc_cleanup,
                    )
                except (apc.APCError, harness_bridge.HarnessBridgeError) as exc:
                    reuse_attempts.append(
                        {
                            "template_id": template.template_id,
                            "branch": "apc_proposal",
                            "status": "rejected",
                            "reason": f"{type(exc).__name__}: {exc}",
                        }
                    )
                else:
                    proposal = attempt.proposal
                    disposition = contracts.make_plan_disposition(
                        decision_id=decision["decision_id"],
                        objective_id=objective_id,
                        route=route,
                        branch="apc_proposal",
                        reason="one bounded drafting child produced a proposed artifact",
                        template={
                            "template_id": template.template_id,
                            "version": template.version,
                            "family": template.family,
                            "digest": contracts.sha256_hex(record),
                            "score": score,
                        },
                        proposal=proposal,
                        apc={
                            "request_digest": attempt.child_operation["request_digest"],
                            "child_operation_id": attempt.child_operation["child_operation_id"],
                            "binding": dict(attempt.child_operation["binding"]),
                            "status": attempt.child_operation["status"],
                        },
                        reuse_attempts=reuse_attempts,
                        root_replan=root_replan,
                    )
                    return disposition, proposal, proposal

        fresh = templates.fresh_plan(objective_id=objective_id, route=route)
        disposition = contracts.make_plan_disposition(
            decision_id=decision["decision_id"],
            objective_id=objective_id,
            route=route,
            branch="fresh",
            reason=self._fresh_plan_reason(
                resolved_config=resolved_config,
                trace=trace,
                shortlist=shortlist,
                attempts=reuse_attempts,
                rejoined=rejoined,
            ),
            fresh=fresh,
            reuse_attempts=reuse_attempts,
            root_replan=root_replan,
        )
        return disposition, fresh, None


    def _finish_planning(
        self,
        *,
        mode: str,
        decision: Mapping[str, Any],
        preparation: Mapping[str, Any],
        trace: Mapping[str, Any] | None,
        disposition: Mapping[str, Any] | None,
        plan: Mapping[str, Any],
        proposal: Mapping[str, Any] | None,
        reason: str,
    ) -> PreparationOutcome:
        return PreparationOutcome(
            mode=mode,
            decision=dict(decision),
            preparation=dict(preparation),
            trace=dict(trace) if trace else None,
            disposition=dict(disposition) if disposition else None,
            plan=dict(plan),
            proposal=dict(proposal) if proposal else None,
            reason=reason,
        )

    def _finalize(
        self,
        *,
        outcome: PreparationOutcome,
        task_card: Mapping[str, Any],
        plan: Mapping[str, Any],
        lane_id: str | None,
        run_id: str | None,
        worktree_path: str | None,
        base_commit: str | None,
        checkpoint: str | None,
        execution_role: str | None,
        invocation_target: str | None,
        recipient: str | None,
        mandatory_content: Iterable[Mapping[str, Any]],
        optional_items: Iterable[Mapping[str, Any]],
        freshness_check: Callable[[Mapping[str, Any]], bool] | None,
        omitted: Iterable[str | Mapping[str, Any]] = (),
        stores: Sequence[SearchStore] = (),
        objective: Mapping[str, Any] | None = None,
        required_sources: frozenset[tuple[str, str, str]] = frozenset(),
    ) -> PreparationOutcome:
        if not all((lane_id, run_id, worktree_path, base_commit)):
            raise PreparationError(
                "finalization requires the exact lane, run, worktree, and base identity"
            )
        for field, value in (
            ("checkpoint", checkpoint), ("execution_role", execution_role),
            ("invocation_target", invocation_target), ("recipient", recipient),
        ):
            contracts._require_canonical_identity(value, field)
        selected = outcome.selected_candidates
        packed_optional: list[dict[str, Any]] = []
        selected_provenance: dict[str, dict[str, Any]] = {}
        for candidate in selected:
            selected_provenance[candidate["candidate_id"]] = {
                key: value for key, value in candidate.items()
                if key not in {
                    "candidate_id", "payload", "payload_digest", "content_hash",
                    "score", "comparable", "disposition", "reasons", "id", "content",
                    "frozen_contract", "compact_representation", "compact_approval",
                }
            }
            packed_item = {
                "id": candidate["candidate_id"],
                "kind": candidate["kind"],
                "origin": candidate["origin"],
                "content": candidate["payload"],
                "revision_id": candidate["revision_id"],
            }
            packed_optional.append(packed_item)
        for item in optional_items:
            packed_optional.append(dict(item))
        source_recheck = self._source_rechecker(
            selected=selected, stores=stores, objective=objective,
            preparation=outcome.preparation, route=plan["route"],
        ) if selected else None
        finalized = context_module.finalize_context(
            task_card=task_card,
            plan=plan,
            decision_id=outcome.decision["decision_id"],
            lane_id=str(lane_id),
            run_id=str(run_id),
            worktree_path=str(worktree_path),
            base_commit=str(base_commit),
            strategy=(outcome.preparation or outcome.decision)["strategy"],
            configuration=(outcome.preparation or outcome.decision)["configuration"],
            checkpoint=str(checkpoint),
            execution_role=str(execution_role),
            invocation_target=str(invocation_target),
            recipient=str(recipient),
            mandatory_content=list(mandatory_content),
            optional_items=packed_optional,
            selected_provenance=selected_provenance,
            omitted=omitted,
            privacy_policy=self.privacy_policy,
            limits=self.limits,
            freshness_check=freshness_check,
            source_owner_recheck=source_recheck,
        )
        if required_sources:
            trace = finalized.context["delivery_trace"]
            delivered_ids = {entry["id"] for entry in trace["context_delivered"]}
            delivered = {
                tuple(entry["provenance"].get(key) for key in (
                    "source_id", "logical_id", "revision_id",
                ))
                for entry in trace["selected"] if entry["id"] in delivered_ids
            }
            missing = required_sources - delivered
            if missing:
                raise PreparationError(
                    "plan-required source identity was not delivered on resume; ROOT must replan: "
                    + ", ".join(sorted(source for source, _, _ in missing))
                )
        if self.store is not None:
            self.store.record_final_context(
                finalized.context, envelope_digest=finalized.envelope["content_hash"]
            )
        return PreparationOutcome(
            mode="dispatchable",
            decision=outcome.decision,
            preparation=outcome.preparation,
            trace=outcome.trace,
            disposition=outcome.disposition,
            plan=plan,
            proposal=outcome.proposal,
            context=finalized.context,
            envelope=finalized.envelope,
            superseded=outcome.superseded,
            reason=(
                outcome.reason + "; mandatory-only context finalized with optional memory omitted"
                if outcome.mode == "no_memory_continuation"
                else "the exact accepted plan was finalized into a dispatchable context"
            ),
        )

    def _source_rechecker(
        self, *, selected: Sequence[Mapping[str, Any]], stores: Sequence[SearchStore],
        objective: Mapping[str, Any] | None, preparation: Mapping[str, Any] | None,
        route: str,
    ) -> Callable[[Mapping[str, Any]], Mapping[str, Any]]:
        """Read selected sources again inside the decision's remaining allowance.

        SearchStore queries are the accepted source adapters: procedure adapters
        repeat exact approval, designation, predicate, and revocation checks.
        The finalizer consumes only a finite observation of that fresh read.
        """

        by_store: dict[str, SearchStore] = {}
        ambiguous_stores: set[str] = set()
        for store in stores:
            if store.store_id in by_store:
                ambiguous_stores.add(store.store_id)
            else:
                by_store[store.store_id] = store
        selected_by_id = {candidate["candidate_id"]: candidate for candidate in selected}
        if preparation is not None and preparation.get("budget_source") == "trusted_deadline":
            cutoff = preparation.get("deadline_monotonic")
            reserve = preparation.get("execution_reserve_seconds")
            if self.store is not None and preparation.get("preparation_id"):
                try:
                    recovered, _ = self._recover_budget(str(preparation["decision_id"]))
                    cutoff = min(float(cutoff), recovered[0]) if recovered is not None else None
                except (PreparationError, TypeError, ValueError):
                    cutoff = None
            if isinstance(cutoff, (int, float)) and isinstance(reserve, (int, float)):
                logical_remaining = max(0.0, float(cutoff) - float(reserve) - self.clock())
            else:
                logical_remaining = 0.0
        elif preparation is not None and preparation.get("budget_source") == "unknown_time":
            # There is no trusted remaining cutoff to spend a second optional
            # slice against. Never replenish the one cheap unknown-time pass.
            logical_remaining = 0.0
        else:
            logical_remaining = 0.0
        real_deadline = time.monotonic() + logical_remaining
        policy_context = self._search_policy_context(preparation, route)
        if objective is not None:
            query_fields = {
                "representation": {key: objective[key] for key in (
                    "model", "dimensions", "metric", "sanitizer_version"
                )},
                "tokens": templates.bounded_token_projection(objective.get("tokens", [])),
                "route": route,
            }
            if policy_context is not None:
                query_fields["policy_context"] = policy_context
            query = safe_query_payload(query_fields, self.privacy_policy)
        else:
            query = None

        def observe(
            store: SearchStore, candidate: Mapping[str, Any], item: Mapping[str, Any],
        ) -> Mapping[str, Any]:
            # Everything that can admit an item runs behind the same real
            # cutoff: query, complete materialization, normalization, exact
            # matching, owner readback, hashing, and the final decision.
            digest = contracts.sha256_hex(item["content"])
            request = {
                "store_id": store.store_id, "source_id": candidate["source_id"],
                "logical_id": candidate["logical_id"],
                "revision_id": candidate["revision_id"],
                "content_digest": digest, "route": route,
            }
            status = "unavailable"
            observed_revision = None
            observed_digest = None
            owner_proof: dict[str, Any] = {}
            if (store.kind != candidate.get("kind")
                    or candidate.get("payload_digest") != digest
                    or candidate.get("revision_id") != item.get("revision_id")):
                return {"recheck": contracts.make_final_source_recheck(
                    item=item, status="ineligible",
                ), "owner_proof": owner_proof}
            if candidate.get("freshness") == "frozen":
                if store.final_proof is not None:
                    returned = store.final_proof(request)
                    if isinstance(returned, Mapping):
                        returned = json.loads(contracts.canonical_json(returned))
                        expected = {
                            "source_id": candidate["source_id"],
                            "revision_id": item["revision_id"],
                            "content_digest": digest,
                        }
                        if returned.get("frozen_contract") == expected:
                            status = "frozen"
                            observed_revision, observed_digest = item["revision_id"], digest
                            owner_proof["frozen_contract"] = expected
                        else:
                            status = "ineligible"
                    else:
                        status = "ineligible"
            else:
                query_copy = json.loads(contracts.canonical_json(query))
                readings = json.loads(contracts.canonical_json(list(store.query(query_copy) or ())))
                status = "ineligible"
                for raw in readings:
                    if not isinstance(raw, Mapping) or raw.get("logical_id") != candidate.get("logical_id"):
                        continue
                    observed_revision = raw.get("revision_id") if isinstance(raw.get("revision_id"), str) else None
                    payload = raw.get("payload")
                    observed_digest = contracts.sha256_hex(payload) if isinstance(payload, Mapping) else None
                    if observed_revision != item["revision_id"] or observed_digest != digest:
                        status = "stale"
                        continue
                    if raw.get("revoked") is True or raw.get("withdrawn") is True:
                        status = "revoked"
                        break
                    normalized, _rejection = self.search._normalize(
                        store, raw, objective=objective, route=route,
                    )
                    if (normalized is None or normalized["disposition"] != "eligible"
                            or normalized["source_id"] != candidate["source_id"]
                            or normalized["kind"] != candidate["kind"]
                            or normalized["scope"] != (candidate.get("scope") or {})
                            or normalized["freshness"] != "live"
                            or (isinstance(candidate.get("representation"), Mapping)
                                and normalized["representation"] != candidate["representation"])
                            or normalized["revision_id"] != item["revision_id"]
                            or normalized["payload_digest"] != digest):
                        status = "ineligible"
                        continue
                    status = "eligible"
                    break
                if status == "eligible" and store.final_proof is not None:
                    returned = store.final_proof(request)
                    if isinstance(returned, Mapping):
                        owner_proof = json.loads(contracts.canonical_json(returned))
            if status in {"eligible", "frozen"} and candidate["kind"] == "procedure":
                content = item["content"]
                representation = owner_proof.get("compact_representation")
                approval = owner_proof.get("compact_approval")
                verified = {key: value for key, value in owner_proof.items() if key == "frozen_contract"}
                if (isinstance(content, Mapping) and isinstance(content.get("procedure"), Mapping)
                        and isinstance(content.get("approval"), Mapping)
                        and owner_proof.get("approval_digest") == content["approval"].get("content_hash")):
                    try:
                        contracts.validate_procedure_approval(
                            content["approval"], procedure=content["procedure"],
                        )
                    except contracts.ContractError:
                        pass
                    else:
                        # A separate source-owner readback, not the query row or
                        # candidate's approval flag, supplies this provenance.
                        verified["approval_digest"] = content["approval"]["content_hash"]
                if (isinstance(content, Mapping) and isinstance(content.get("procedure"), Mapping)
                        and isinstance(content.get("approval"), Mapping)
                        and isinstance(representation, Mapping) and isinstance(approval, Mapping)):
                    try:
                        contracts.validate_procedure_approval(content["approval"], procedure=content["procedure"])
                        contracts.validate_procedure_compact_representation(
                            representation, procedure=content["procedure"],
                        )
                        contracts.validate_procedure_compact_approval(
                            approval, procedure=content["procedure"],
                            full_approval=content["approval"], representation=representation,
                        )
                    except contracts.ContractError:
                        pass
                    else:
                        verified["approval_digest"] = content["approval"]["content_hash"]
                        verified["compact_representation"] = representation
                        verified["compact_approval"] = approval
                owner_proof = verified
            recheck = contracts.make_final_source_recheck(
                item=item, status=status,
                observed_revision_id=observed_revision,
                observed_content_digest=observed_digest,
            )
            return {"recheck": recheck, "owner_proof": owner_proof}

        def recheck(item: Mapping[str, Any]) -> Mapping[str, Any]:
            candidate = selected_by_id.get(item["id"])
            provenance = candidate.get("provenance") if candidate else None
            store_id = provenance[0].get("store_id") if isinstance(provenance, list) and provenance else None
            store = None if store_id in ambiguous_stores else by_store.get(store_id)
            if store is not None and store.requires_network and self.store is not None and policy_context is None:
                return contracts.make_final_source_recheck(item=item, status="unavailable")
            if store is not None and query is not None and time.monotonic() < real_deadline:
                deadline = min(real_deadline, time.monotonic() + self.limits.store_seconds)
                try:
                    observed, bounded = self.search._call_bounded(
                        lambda: observe(store, candidate, item),
                        deadline=deadline, label=f"final-recheck:{store_id}",
                    )
                    if bounded is None:
                        return observed
                except Exception:
                    pass
            return contracts.make_final_source_recheck(item=item, status="unavailable")

        return recheck


__all__ = [
    "MandatoryStateFailure",
    "PlanAcceptanceError",
    "PreparationError",
    "PreparationOutcome",
    "PreparationService",
]
